//! Per-session bridge state and the session's mode/config-option surface.

use std::path::PathBuf;
use std::sync::Arc;

use agent_client_protocol::v1::schema as acp;
use async_channel::Receiver;
use async_lock::Mutex;
use serde_json::{Value, json};

use crate::app_server::AppEvent;

/// Config option ids the bridge publishes.
pub const MODE_CONFIG_ID: &str = "mode";
/// Model selector config id.
pub const MODEL_CONFIG_ID: &str = "model";
/// Reasoning-effort selector config id.
pub const EFFORT_CONFIG_ID: &str = "reasoning_effort";
/// Fast-mode toggle config id.
pub const FAST_MODE_CONFIG_ID: &str = "fast-mode";

/// A session mode: the (approval policy, sandbox policy) preset presented
/// to the client.
#[derive(Debug, Clone)]
pub struct AgentMode {
    /// Mode id sent on the wire.
    pub id: &'static str,
    /// Display name.
    pub name: &'static str,
    /// Display description.
    pub description: &'static str,
    /// App-server `approvalPolicy`.
    pub approval_policy: &'static str,
    /// App-server `approvalsReviewer`.
    pub approvals_reviewer: &'static str,
    /// App-server `sandboxPolicy` JSON, or `sandbox` mode name for the modes
    /// that take a named sandbox.
    pub sandbox_policy: Value,
    /// Named `sandbox` value for `thread/start`.
    pub sandbox_mode: &'static str,
}

/// The workspace-write sandbox payload shared by two modes.
fn workspace_write() -> Value {
    json!({
        "type": "workspaceWrite",
        "writableRoots": [],
        "networkAccess": false,
        "excludeTmpdirEnvVar": false,
        "excludeSlashTmp": false,
    })
}

/// Every mode the bridge advertises, in display order.
#[must_use]
pub fn all_modes() -> Vec<AgentMode> {
    vec![
        AgentMode {
            id: "read-only",
            name: "Read-only",
            description: "Requires approval to edit files and access the internet.",
            approval_policy: "on-request",
            approvals_reviewer: "user",
            sandbox_policy: json!({"type": "readOnly", "networkAccess": false}),
            sandbox_mode: "read-only",
        },
        AgentMode {
            id: "workspace-write",
            name: "Workspace access",
            description: "Edit workspace files; ask before writing outside the workspace or accessing the network.",
            approval_policy: "on-request",
            approvals_reviewer: "user",
            sandbox_policy: workspace_write(),
            sandbox_mode: "workspace-write",
        },
        AgentMode {
            id: "agent",
            name: "Auto review",
            description: "Only ask for actions detected as potentially unsafe",
            approval_policy: "on-request",
            approvals_reviewer: "auto_review",
            sandbox_policy: workspace_write(),
            sandbox_mode: "workspace-write",
        },
        AgentMode {
            id: "agent-full-access",
            name: "Full access",
            description: "Unrestricted access to the internet and any file on your computer",
            approval_policy: "never",
            approvals_reviewer: "user",
            sandbox_policy: json!({"type": "dangerFullAccess"}),
            sandbox_mode: "danger-full-access",
        },
    ]
}

/// The mode applied when the client selects none.
#[must_use]
pub fn default_mode() -> AgentMode {
    AgentMode {
        id: "agent",
        name: "Auto review",
        description: "Only ask for actions detected as potentially unsafe",
        approval_policy: "on-request",
        approvals_reviewer: "auto_review",
        sandbox_policy: workspace_write(),
        sandbox_mode: "workspace-write",
    }
}

/// Look up a mode by wire id.
#[must_use]
pub fn find_mode(id: &str) -> Option<AgentMode> {
    all_modes().into_iter().find(|m| m.id == id)
}

/// A live bridge session: one ACP session ↔ one app-server thread.
pub struct Session {
    /// ACP session id (the app-server thread id).
    pub id: acp::SessionId,
    /// App-server thread id.
    pub thread_id: String,
    /// Working directory of the thread.
    pub cwd: PathBuf,
    /// Per-thread app-server events.
    pub events: Receiver<AppEvent>,
    /// Current mode id.
    pub mode: Mutex<String>,
    /// Model override for the next turn, if set.
    pub model: Mutex<Option<String>>,
    /// Reasoning effort override for the next turn, if set.
    pub effort: Mutex<Option<String>>,
    /// Fast mode flag for the next turn.
    pub fast: Mutex<bool>,
    /// Currently running turn id, when a prompt is in flight.
    pub active_turn: Mutex<Option<String>>,
    /// Delivers `session/cancel` into the prompt loop.
    pub cancel_tx: async_channel::Sender<()>,
    /// Receiver cloned by the prompt loop.
    pub cancel_rx: async_channel::Receiver<()>,
    /// Client-provided MCP servers to configure on the thread.
    pub mcp_servers: Vec<acp::McpServer>,
}

impl Session {
    /// Build a session with its event channel.
    #[must_use]
    pub fn new(
        thread_id: String,
        cwd: PathBuf,
        events: Receiver<AppEvent>,
        mode: String,
        mcp_servers: Vec<acp::McpServer>,
    ) -> Arc<Self> {
        let (cancel_tx, cancel_rx) = async_channel::bounded(1);
        Arc::new(Self {
            id: acp::SessionId::new(thread_id.clone()),
            thread_id,
            cwd,
            events,
            mode: Mutex::new(mode),
            model: Mutex::new(None),
            effort: Mutex::new(None),
            fast: Mutex::new(false),
            active_turn: Mutex::new(None),
            cancel_tx,
            cancel_rx,
            mcp_servers,
        })
    }

    /// The `SessionModeState` to embed in session responses.
    #[must_use]
    pub async fn mode_state(&self) -> acp::SessionModeState {
        acp::SessionModeState::new(
            acp::SessionModeId::new(self.mode.lock().await.clone()),
            all_modes()
                .into_iter()
                .map(|m| acp::SessionMode::new(m.id, m.name).description(m.description))
                .collect(),
        )
    }
}

/// A select-option entry inside a config option.
#[must_use]
pub fn select_option(value: &str, name: &str) -> acp::SessionConfigSelectOption {
    acp::SessionConfigSelectOption::new(
        acp::SessionConfigValueId::new(value.to_string()),
        name.to_string(),
    )
}

/// The `mode` config option.
#[must_use]
pub fn mode_option(current: &str) -> acp::SessionConfigOption {
    acp::SessionConfigOption::new(
        MODE_CONFIG_ID,
        "Mode",
        acp::SessionConfigKind::Select(acp::SessionConfigSelect::new(
            current.to_string(),
            acp::SessionConfigSelectOptions::Ungrouped(
                all_modes()
                    .into_iter()
                    .map(|m| {
                        acp::SessionConfigSelectOption::new(
                            acp::SessionConfigValueId::new(m.id),
                            m.name.to_string(),
                        )
                        .description(m.description)
                    })
                    .collect(),
            ),
        )),
    )
    .description("Approval and sandboxing preset for the session")
    .category(acp::SessionConfigOptionCategory::Mode)
}
