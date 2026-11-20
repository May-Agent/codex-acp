//! Per-session bridge state, the session's mode/config-option surface,
//! and the turn driver.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use aither_acp::domain::ids::ConfigId;
use aither_acp::domain::{
    AvailableCommand, Command, CommandInput, ConfigCategory, ConfigKind, ConfigOption, ConfigValue,
    ConfigValueId, ContentBlock, NoticeSeverity, Open, Patch, PermissionOption,
    PermissionOptionKind, PermissionOutcome, PermissionRequest, PermissionSubject, PlanId,
    ResourceContents, SelectConfig, SelectOption, SelectOptions, SessionId, SessionUpdate,
    StopReason, TextInput, ToolCallId, ToolCallSubject, ToolCallUpdate,
};
use aither_acp::rpc::RpcError;
use aither_acp::server::{
    MessageStream, PendingPrompt, PromptError, Session, SessionContext, Turn,
};
use async_channel::Receiver;
use async_lock::Mutex as AsyncMutex;
use serde_json::{Value, json};
use tracing::debug;

use crate::app_server::{AppEvent, AppServer};
use crate::translate;

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

/// The mutable configuration snapshot of a session.
#[derive(Debug)]
pub struct SessionState {
    /// Current mode id.
    pub mode: String,
    /// Model override for the next turn, if set.
    pub model: Option<String>,
    /// Reasoning effort override for the next turn, if set.
    pub effort: Option<String>,
    /// Fast mode flag for the next turn.
    pub fast: bool,
    /// Cached `model/list` entries `(id, display name)`.
    pub models: Vec<(String, String)>,
}

impl SessionState {
    const fn new(mode: String) -> Self {
        Self {
            mode,
            model: None,
            effort: None,
            fast: false,
            models: Vec::new(),
        }
    }
}

fn select_option(value: &str, name: &str, description: Option<&str>) -> SelectOption {
    SelectOption {
        value: ConfigValueId::from_string(value),
        name: name.to_string(),
        description: description.map(str::to_string),
        meta: None,
    }
}

fn select_config(
    id: &str,
    name: &str,
    description: &str,
    category: ConfigCategory,
    current: &str,
    entries: Vec<SelectOption>,
) -> ConfigOption {
    ConfigOption {
        config_id: ConfigId::from_string(id),
        name: name.to_string(),
        description: Some(description.to_string()),
        category: Some(Open::Known(category)),
        kind: ConfigKind::Select(SelectConfig {
            current_value: ConfigValueId::from_string(current),
            options: SelectOptions::Ungrouped(entries),
        }),
        meta: None,
        extra: BTreeMap::new(),
    }
}

fn config_options_of(state: &SessionState) -> Vec<ConfigOption> {
    let mut options = vec![select_config(
        MODE_CONFIG_ID,
        "Mode",
        "Approval and sandboxing preset for the session",
        ConfigCategory::Mode,
        &state.mode,
        all_modes()
            .iter()
            .map(|m| select_option(m.id, m.name, Some(m.description)))
            .collect(),
    )];
    if !state.models.is_empty() {
        options.push(select_config(
            MODEL_CONFIG_ID,
            "Model",
            "Model used for the next turn",
            ConfigCategory::Model,
            state.model.as_deref().unwrap_or(""),
            state
                .models
                .iter()
                .map(|(id, name)| select_option(id, name, None))
                .collect(),
        ));
    }
    options.push(select_config(
        EFFORT_CONFIG_ID,
        "Reasoning effort",
        "Reasoning effort for the next turn",
        ConfigCategory::ThoughtLevel,
        state.effort.as_deref().unwrap_or(""),
        ["minimal", "low", "medium", "high", "xhigh"]
            .into_iter()
            .map(|e| select_option(e, e, None))
            .collect(),
    ));
    options.push(ConfigOption {
        config_id: ConfigId::from_string(FAST_MODE_CONFIG_ID),
        name: "Fast mode".to_string(),
        description: Some("Prefer speed over cost for the next turn".to_string()),
        category: Some(Open::Known(ConfigCategory::ModelConfig)),
        kind: ConfigKind::Boolean(aither_acp::domain::BooleanConfig {
            current_value: state.fast,
        }),
        meta: None,
        extra: BTreeMap::new(),
    });
    options
}

fn commands() -> Vec<AvailableCommand> {
    let text_input = |hint: &str| {
        Some(Open::Known(CommandInput::Text(TextInput {
            hint: Some(hint.to_string()),
            meta: None,
        })))
    };
    [
        ("compact", "Compact the conversation context", None),
        ("logout", "Sign out of the Codex account", None),
        ("status", "Show the current account", None),
        ("review", "Review uncommitted changes", None),
        (
            "review-branch",
            "Review changes against a base branch",
            text_input("base branch"),
        ),
        ("review-commit", "Review a commit", text_input("commit SHA")),
    ]
    .into_iter()
    .map(|(name, description, input)| {
        AvailableCommand::Command(Command {
            name: name.to_string(),
            description: description.to_string(),
            input,
            meta: None,
            extra: BTreeMap::new(),
        })
    })
    .collect()
}

/// Shared session state behind the cloneable [`CodexSession`] handle.
struct SessionInner {
    /// ACP session id assigned by the framework.
    id: SessionId,
    /// App-server thread id.
    thread_id: String,
    /// Working directory of the thread.
    cwd: PathBuf,
    /// Per-thread app-server events.
    events: Receiver<AppEvent>,
    server: Arc<AppServer>,
    /// The host's session table (for `close`/`delete` bookkeeping and
    /// extension-method lookups).
    sessions: Arc<AsyncMutex<HashMap<String, CodexSession>>>,
    /// Mutable config state.
    state: Mutex<SessionState>,
    /// Currently running app-server turn id, when a prompt is in flight.
    active_turn: AsyncMutex<Option<String>>,
}

/// A live bridge session: one ACP session ↔ one app-server thread.
///
/// The handle clones cheaply so the host can keep one in its session
/// table while the framework owns another.
#[derive(Clone)]
pub struct CodexSession {
    inner: Arc<SessionInner>,
}

impl CodexSession {
    /// Build a session around a live thread subscription.
    #[must_use]
    pub fn new(
        id: SessionId,
        thread_id: String,
        cwd: PathBuf,
        events: Receiver<AppEvent>,
        server: Arc<AppServer>,
        sessions: Arc<AsyncMutex<HashMap<String, Self>>>,
        mode: String,
    ) -> Self {
        Self {
            inner: Arc::new(SessionInner {
                id,
                thread_id,
                cwd,
                events,
                server,
                sessions,
                state: Mutex::new(SessionState::new(mode)),
                active_turn: AsyncMutex::new(None),
            }),
        }
    }

    /// The ACP session id.
    #[must_use]
    pub fn id(&self) -> &SessionId {
        &self.inner.id
    }

    /// The app-server thread id.
    #[must_use]
    pub fn thread_id(&self) -> &str {
        &self.inner.thread_id
    }

    fn state(&self) -> std::sync::MutexGuard<'_, SessionState> {
        self.inner.state.lock().expect("session state poisoned")
    }

    /// Update the cached model list (used by `config_options`).
    pub fn set_models(&self, models: Vec<(String, String)>) {
        self.state().models = models;
    }

    /// Record the thread's initial model/effort.
    pub fn set_model_effort(&self, model: Option<String>, effort: Option<String>) {
        let mut s = self.state();
        s.model = model;
        s.effort = effort;
    }

    /// The currently selected model (the legacy `session/set_model` lane).
    pub fn set_model(&self, model: Option<String>) {
        self.state().model = model;
    }

    /// The active app-server turn id, if a turn is running.
    pub async fn active_turn_id(&self) -> Option<String> {
        self.inner.active_turn.lock().await.clone()
    }

    /// Interrupt the running turn, unsubscribe the thread, and detach
    /// from the host's session table.
    async fn teardown(&self) {
        let active = self.inner.active_turn.lock().await.take();
        if let Some(turn_id) = active {
            let _ = self
                .inner
                .server
                .call::<_, Value>(
                    "turn/interrupt",
                    &json!({"threadId": self.inner.thread_id, "turnId": turn_id}),
                )
                .await;
        }
        let _ = self
            .inner
            .server
            .call::<_, Value>(
                "thread/unsubscribe",
                &json!({"threadId": self.inner.thread_id}),
            )
            .await;
        self.inner.server.unsubscribe(&self.inner.thread_id).await;
        self.inner
            .sessions
            .lock()
            .await
            .remove(self.inner.id.as_str());
    }
}

impl Session for CodexSession {
    fn config_options(&self) -> Vec<ConfigOption> {
        config_options_of(&self.state())
    }

    fn available_commands(&self) -> Vec<AvailableCommand> {
        commands()
    }

    fn set_config(
        &self,
        id: &ConfigId,
        value: ConfigValue,
        _cx: &SessionContext,
    ) -> impl Future<Output = Result<Vec<ConfigOption>, RpcError>> + Send {
        std::future::ready(
            self.set_config_state(id, &value)
                .map(|()| self.config_options()),
        )
    }

    async fn prompt(
        &self,
        prompt: PendingPrompt,
        _cx: &SessionContext,
    ) -> Result<StopReason, PromptError> {
        let content = prompt.content().to_vec();
        let turn = prompt
            .accept()
            .await
            .map_err(|rejected| PromptError::from(rejected.into_inner()))?;
        if let Some(cmd) = slash_command(&content) {
            return self.run_command(&turn, &cmd).await;
        }
        let (mode, model, effort, fast) = {
            let s = self.state();
            (
                find_mode(&s.mode).unwrap_or_else(default_mode),
                s.model.clone(),
                s.effort.clone(),
                s.fast,
            )
        };
        let input = prompt_input(&content);
        let resp: Value = self
            .inner
            .server
            .call(
                "turn/start",
                &json!({
                    "threadId": self.inner.thread_id,
                    "input": input,
                    "cwd": self.inner.cwd.to_string_lossy(),
                    "model": model,
                    "effort": effort,
                    "serviceTierForTurn": fast.then_some("fast"),
                    "approvalPolicy": mode.approval_policy,
                    "approvalsReviewer": mode.approvals_reviewer,
                    "sandboxPolicy": mode.sandbox_policy,
                }),
            )
            .await
            .map_err(|e| PromptError::failed(e.to_string()))?;
        let turn_id = resp
            .get("turn")
            .and_then(|t| t.get("id"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        *self.inner.active_turn.lock().await = Some(turn_id.clone());
        let stop = self.drive(&turn, &turn_id).await;
        *self.inner.active_turn.lock().await = None;
        stop
    }

    async fn close(&self) {
        self.teardown().await;
    }
}

impl CodexSession {
    /// Apply a config change to the session state.
    fn set_config_state(&self, id: &ConfigId, value: &ConfigValue) -> Result<(), RpcError> {
        let mut s = self.state();
        match id.as_str() {
            MODE_CONFIG_ID => {
                let mode = config_id_value(value);
                if find_mode(&mode).is_none() {
                    return Err(RpcError::invalid_params(format!("unknown mode {mode}")));
                }
                s.mode = mode;
            }
            MODEL_CONFIG_ID => {
                s.model = Some(config_id_value(value)).filter(|m| !m.is_empty());
            }
            EFFORT_CONFIG_ID => {
                s.effort = Some(config_id_value(value)).filter(|e| !e.is_empty());
            }
            FAST_MODE_CONFIG_ID => {
                s.fast = match value {
                    ConfigValue::Boolean(b) => b.value,
                    other => {
                        return Err(RpcError::invalid_params(format!(
                            "fast-mode expects a boolean value, got {other:?}"
                        )));
                    }
                };
            }
            other => {
                return Err(RpcError::invalid_params(format!(
                    "unknown config option {other}"
                )));
            }
        }
        Ok(())
    }
}

/// The selected value of a `ConfigValue::Id`, as a string.
fn config_id_value(value: &ConfigValue) -> String {
    match value {
        ConfigValue::Id(v) => v.value.to_string(),
        ConfigValue::Boolean(b) => b.value.to_string(),
        ConfigValue::Custom(_, raw) | ConfigValue::Unknown(_, raw) => raw
            .get("value")
            .and_then(Value::as_str)
            .map_or_else(|| raw.to_string(), str::to_string),
    }
}

/// Map ACP prompt content to app-server `UserInput`s.
fn prompt_input(prompt: &[ContentBlock]) -> Vec<Value> {
    let mut input = Vec::new();
    for block in prompt {
        match block {
            ContentBlock::Text(t) => input.push(json!({"type": "text", "text": t.text})),
            ContentBlock::Image(i) => {
                let data = i.data.clone().or_else(|| i.uri.clone()).unwrap_or_default();
                input.push(json!({
                    "type": "image",
                    "url": format!("data:{};base64,{}", i.mime_type, data),
                }));
            }
            ContentBlock::ResourceLink(l) => {
                if let Some(path) = l.uri.strip_prefix("file://") {
                    input.push(json!({
                        "type": "mention",
                        "name": l.name,
                        "path": path,
                    }));
                }
            }
            ContentBlock::Resource(res) => {
                if let ResourceContents::Text(t) = &res.resource {
                    input.push(json!({"type": "text", "text": t.text}));
                }
            }
            ContentBlock::Audio(a) => input.push(json!({
                "type": "audio",
                "url": format!("data:{};base64,{}", a.mime_type, a.data),
            })),
            ContentBlock::Custom(..) | ContentBlock::Unknown(..) => {}
        }
    }
    input
}

/// Whether the prompt is exactly one slash command.
fn slash_command(prompt: &[ContentBlock]) -> Option<String> {
    if let [ContentBlock::Text(t)] = prompt {
        t.text.trim().strip_prefix('/').map(str::to_string)
    } else {
        None
    }
}

/// Live per-turn emission state.
struct TurnDrive<'a> {
    turn: &'a Turn,
    agent: Option<MessageStream>,
    thought: Option<MessageStream>,
}

impl TurnDrive<'_> {
    fn message(&mut self) -> &MessageStream {
        if self.agent.is_none() {
            self.agent = Some(self.turn.message());
        }
        self.agent.as_ref().expect("agent stream")
    }

    async fn agent_text(&mut self, delta: &str) {
        let _ = self.message().text(delta).await;
    }

    async fn thought_text(&mut self, delta: &str) {
        if self.thought.is_none() {
            self.thought = Some(self.turn.thought());
        }
        let _ = self
            .thought
            .as_ref()
            .expect("thought stream")
            .text(delta)
            .await;
    }
}

#[allow(clippy::large_enum_variant)]
enum EventOrCancel {
    Event(AppEvent),
    Cancelled,
}

impl CodexSession {
    /// Consume session events until the turn ends; translate + forward them.
    async fn drive(&self, turn: &Turn, turn_id: &str) -> Result<StopReason, PromptError> {
        let mut d = TurnDrive {
            turn,
            agent: None,
            thought: None,
        };
        loop {
            // Absorb prompts queued mid-turn (v2 clients prompt freely).
            while let Some(steered) = turn.try_recv_steered() {
                self.steer(turn, steered).await;
            }
            let ev = futures_lite::future::or(
                async { self.inner.events.recv().await.map(EventOrCancel::Event) },
                async {
                    turn.cancelled().await;
                    Ok(EventOrCancel::Cancelled)
                },
            )
            .await
            .map_err(|_| PromptError::failed("app-server event channel closed"))?;
            let ev = match ev {
                EventOrCancel::Cancelled => {
                    let _ = self
                        .inner
                        .server
                        .call::<_, Value>(
                            "turn/interrupt",
                            &json!({"threadId": self.inner.thread_id, "turnId": turn_id}),
                        )
                        .await;
                    return Ok(StopReason::Cancelled);
                }
                EventOrCancel::Event(e) => e,
            };
            match ev {
                AppEvent::Notification(n) => {
                    if let Some(stop) = self.forward(&mut d, &n, turn_id).await {
                        return Ok(stop);
                    }
                }
                AppEvent::Request { id, request } => {
                    self.answer_request(turn, id, request).await;
                }
                AppEvent::Unknown { id: Some(id), .. } => {
                    let _ = self
                        .inner
                        .server
                        .respond_error(&id, -32601, "request unsupported by this bridge")
                        .await;
                }
                AppEvent::Unknown { .. } => {}
            }
        }
    }

    /// Merge a queued prompt into the running turn and forward it to the
    /// app-server's `turn/steer`.
    async fn steer(&self, turn: &Turn, steered: PendingPrompt) {
        let input = prompt_input(steered.content());
        let Some(codex_turn) = self.inner.active_turn.lock().await.clone() else {
            steered
                .reject(RpcError::invalid_params("no active turn to steer"))
                .await;
            return;
        };
        if let Err(e) = turn.merge_prompt(steered).await {
            debug!(%e, "steered prompt already resolved");
            return;
        }
        if let Err(e) = self
            .inner
            .server
            .call::<_, Value>(
                "turn/steer",
                &json!({
                    "threadId": self.inner.thread_id,
                    "expectedTurnId": codex_turn,
                    "input": input,
                }),
            )
            .await
        {
            let _ = turn
                .notice(
                    Open::Known(NoticeSeverity::Warning),
                    "Steering failed",
                    Some(e.to_string()),
                )
                .await;
        }
    }

    /// Returns `Some(stop_reason)` when the turn finished.
    async fn forward(
        &self,
        d: &mut TurnDrive<'_>,
        n: &codex_codes::Notification,
        turn_id: &str,
    ) -> Option<StopReason> {
        use codex_codes::Notification as N;
        // Skip turn-scoped leftovers from another turn.
        if let Some(tid) = n.turn_id()
            && !tid.is_empty()
            && tid != turn_id
        {
            return None;
        }
        match n {
            N::AgentMessageDelta(delta) => d.agent_text(&delta.delta).await,
            N::ReasoningDelta(delta) => d.thought_text(&delta.delta).await,
            N::ReasoningTextDelta(delta) => d.thought_text(&delta.delta).await,
            N::PlanDelta(delta) => d.agent_text(&delta.delta).await,
            N::CmdOutputDelta(_)
            | N::FileChangeOutputDelta(_)
            | N::FileChangePatchUpdated(_)
            | N::ItemStarted(_)
            | N::ItemCompleted(_) => Self::emit_tool(d, n).await,
            N::TurnPlanUpdated(plan) => {
                let v = serde_json::to_value(plan).unwrap_or_default();
                let plan_id = v
                    .get("planId")
                    .or_else(|| v.get("id"))
                    .and_then(Value::as_str)
                    .map_or_else(|| PlanId::from_string("turn-plan"), PlanId::from_string);
                let _ = d.turn.plan(plan_id, translate::plan_entries(&v)).await;
            }
            N::TurnCompleted(n) => {
                let v = serde_json::to_value(&n.turn).unwrap_or_default();
                let completed_id = v.get("id").and_then(Value::as_str).unwrap_or_default();
                if !completed_id.is_empty() && completed_id != turn_id {
                    return None;
                }
                return Some(translate::stop_reason(&v));
            }
            N::ThreadTokenUsageUpdated(n) => {
                let v = serde_json::to_value(&n.token_usage).unwrap_or_default();
                let _ = d.turn.context().usage(translate::usage_update(&v)).await;
            }
            N::Error(e) => {
                notice(d.turn, NoticeSeverity::Error, e).await;
            }
            N::Warning(e) => {
                notice(d.turn, NoticeSeverity::Warning, e).await;
            }
            N::ConfigWarning(e) => {
                notice(d.turn, NoticeSeverity::Warning, e).await;
            }
            N::GuardianWarning(e) => {
                notice(d.turn, NoticeSeverity::Warning, e).await;
            }
            N::ContextCompacted(n) => {
                let v = serde_json::to_value(n).unwrap_or_default();
                let _ = d
                    .turn
                    .notice(
                        Open::Known(NoticeSeverity::Info),
                        "Context compacted",
                        v.get("turnId").and_then(Value::as_str).map(str::to_string),
                    )
                    .await;
            }
            _ => {}
        }
        None
    }

    /// Emit a tool-call update for item lifecycle notifications.
    async fn emit_tool(d: &TurnDrive<'_>, n: &codex_codes::Notification) {
        use codex_codes::Notification as N;
        let update = match n {
            N::CmdOutputDelta(delta) => SessionUpdate::ToolCallContentChunk(
                translate::output_chunk(&delta.item_id, &delta.delta),
            ),
            N::FileChangeOutputDelta(delta) => {
                let v = serde_json::to_value(delta).unwrap_or_default();
                let text = v.get("delta").and_then(Value::as_str).unwrap_or_default();
                SessionUpdate::ToolCallContentChunk(translate::output_chunk(&delta.item_id, text))
            }
            N::FileChangePatchUpdated(delta) => {
                let changes = serde_json::to_value(&delta.changes).unwrap_or_default();
                SessionUpdate::ToolCallUpdate(translate::patch_update(
                    &delta.item_id,
                    changes.as_array().map_or(&[], Vec::as_slice),
                ))
            }
            N::ItemStarted(n) => {
                let v = serde_json::to_value(&n.item).unwrap_or_default();
                let status = aither_acp::domain::ToolCallStatus::InProgress;
                match translate::item_as_tool_call(&v, Some(status)) {
                    Some(u) => SessionUpdate::ToolCallUpdate(u),
                    None => return,
                }
            }
            N::ItemCompleted(n) => {
                let v = serde_json::to_value(&n.item).unwrap_or_default();
                match translate::item_completed_update(&v) {
                    Some(u) => SessionUpdate::ToolCallUpdate(u),
                    None => return,
                }
            }
            _ => return,
        };
        let _ = d.turn.update(update).await;
    }

    /// Forward an app-server approval/elicitation request to the ACP client.
    async fn answer_request(&self, turn: &Turn, id: Value, request: codex_codes::ServerRequest) {
        use codex_codes::ServerRequest as R;
        match request {
            R::CmdExecApproval(p) => {
                let raw = serde_json::to_value(&p).unwrap_or_default();
                let decision = self.ask(turn, "Run command?", &raw).await;
                let _ = self
                    .inner
                    .server
                    .respond(&id, &json!({"decision": decision}))
                    .await;
            }
            R::ExecCommandApproval(p) => {
                let raw = serde_json::to_value(&p).unwrap_or_default();
                let decision = self.ask(turn, "Run command?", &raw).await;
                let _ = self
                    .inner
                    .server
                    .respond(&id, &json!({"decision": decision}))
                    .await;
            }
            R::FileChangeApproval(p) => {
                let raw = serde_json::to_value(&p).unwrap_or_default();
                let decision = self.ask(turn, "Apply file changes?", &raw).await;
                let _ = self
                    .inner
                    .server
                    .respond(&id, &json!({"decision": decision}))
                    .await;
            }
            R::ApplyPatchApproval(p) => {
                let raw = serde_json::to_value(&p).unwrap_or_default();
                let decision = self.ask(turn, "Apply file changes?", &raw).await;
                let _ = self
                    .inner
                    .server
                    .respond(&id, &json!({"decision": decision}))
                    .await;
            }
            R::McpServerElicitationRequest(_) => {
                let _ = self
                    .inner
                    .server
                    .respond(&id, &json!({"action": "decline"}))
                    .await;
            }
            other => {
                debug!(
                    ?other,
                    "answering unsupported server request with a refusal"
                );
                let _ = self
                    .inner
                    .server
                    .respond_error(&id, -32601, "request unsupported by this bridge")
                    .await;
            }
        }
    }

    /// Ask the client for permission; returns the app-server decision.
    async fn ask(&self, turn: &Turn, title: &str, raw: &Value) -> &'static str {
        let options = [
            (
                "allow_always",
                "Always allow",
                PermissionOptionKind::AllowAlways,
            ),
            ("allow", "Allow once", PermissionOptionKind::AllowOnce),
            ("reject", "Reject", PermissionOptionKind::RejectOnce),
            (
                "reject_always",
                "Always reject",
                PermissionOptionKind::RejectAlways,
            ),
        ]
        .into_iter()
        .map(|(id, name, kind)| PermissionOption {
            option_id: aither_acp::domain::PermissionOptionId::from_string(id),
            name: name.to_string(),
            kind: Open::Known(kind),
            meta: None,
            extra: BTreeMap::new(),
        })
        .collect();
        let mut subject = ToolCallUpdate::new(ToolCallId::from_string(
            raw.get("itemId")
                .and_then(Value::as_str)
                .unwrap_or("approval"),
        ));
        subject.title = Patch::Set(title.to_string());
        subject.raw_input = Patch::Set(raw.clone());
        let request = PermissionRequest {
            session_id: self.inner.id.clone(),
            title: title.to_string(),
            description: None,
            subject: Some(Open::Known(PermissionSubject::ToolCall(Box::new(
                ToolCallSubject {
                    tool_call: subject,
                    meta: None,
                    extra: BTreeMap::new(),
                },
            )))),
            options,
            meta: None,
            extra: BTreeMap::new(),
        };
        match turn.confirm(request).await {
            Ok(PermissionOutcome::Selected(sel)) => match sel.option_id.as_str() {
                "allow_always" => "acceptForSession",
                "allow" => "accept",
                "reject" | "reject_always" => "decline",
                _ => "cancel",
            },
            _ => "cancel",
        }
    }

    /// A `/`-command prompt: run the codex-side action and report as updates.
    async fn run_command(&self, turn: &Turn, cmd: &str) -> Result<StopReason, PromptError> {
        let (name, args) = cmd.split_once(' ').unwrap_or((cmd, ""));
        match name {
            "compact" => {
                let _: Value = self
                    .inner
                    .server
                    .call(
                        "thread/compact/start",
                        &json!({"threadId": self.inner.thread_id}),
                    )
                    .await
                    .map_err(|e| PromptError::failed(e.to_string()))?;
                Ok(StopReason::EndTurn)
            }
            "logout" => {
                let _: Value = self
                    .inner
                    .server
                    .call("account/logout", &json!({}))
                    .await
                    .map_err(|e| PromptError::failed(e.to_string()))?;
                Ok(StopReason::EndTurn)
            }
            "status" => {
                let v: Value = self
                    .inner
                    .server
                    .call("account/read", &json!({}))
                    .await
                    .unwrap_or_default();
                let _ = turn.message().text(format!("Account: {v}")).await;
                Ok(StopReason::EndTurn)
            }
            "review" | "review-branch" | "review-commit" => {
                let target = match name {
                    "review-branch" => json!({"type": "baseBranch", "branch": args.trim()}),
                    "review-commit" => json!({"type": "commit", "sha": args.trim()}),
                    _ => json!({"type": "uncommittedChanges"}),
                };
                let resp: Value = self
                    .inner
                    .server
                    .call(
                        "review/start",
                        &json!({"threadId": self.inner.thread_id, "target": target}),
                    )
                    .await
                    .map_err(|e| PromptError::failed(e.to_string()))?;
                let turn_id = resp
                    .get("turn")
                    .and_then(|t| t.get("id"))
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                *self.inner.active_turn.lock().await = Some(turn_id.clone());
                let stop = self.drive(turn, &turn_id).await;
                *self.inner.active_turn.lock().await = None;
                stop
            }
            _ => {
                let _ = turn
                    .message()
                    .text(format!("Unknown command: /{name}"))
                    .await;
                Ok(StopReason::EndTurn)
            }
        }
    }
}

/// Emit a session notice for a codex error/warning payload.
async fn notice(turn: &Turn, severity: NoticeSeverity, e: &(impl serde::Serialize + Sync)) {
    let v = serde_json::to_value(e).unwrap_or_default();
    let _ = turn
        .notice(
            Open::Known(severity),
            v.get("message")
                .or_else(|| v.get("error").and_then(|e| e.get("message")))
                .and_then(Value::as_str)
                .unwrap_or("notification"),
            v.get("additionalDetails")
                .and_then(Value::as_str)
                .map(str::to_string),
        )
        .await;
}
