//! ACP session modes, configuration options (`mode`, `model`,
//! `reasoning_effort`, `fast-mode`, `collaboration_mode`), and advertised slash
//! commands.

use std::collections::BTreeMap;

use aither_acp::domain::ids::ConfigId;
use aither_acp::domain::{
    AvailableCommand, BooleanConfig, Command, CommandInput, ConfigCategory, ConfigKind,
    ConfigOption, ConfigValueId, Open, SelectConfig, SelectOption, SelectOptions, TextInput,
};
use aither_acp::vendor::codex::{SessionModel, SessionModels};
use serde_json::{Value, json};

use crate::error::{CodexAcpError, Result};

/// Config option ID for the session approval/sandbox preset.
pub const MODE_CONFIG_ID: &str = "mode";
/// Config option ID for the active model selector.
pub const MODEL_CONFIG_ID: &str = "model";
/// Config option ID for the reasoning effort selector.
pub const EFFORT_CONFIG_ID: &str = "reasoning_effort";
/// Config option ID for the boolean fast-mode toggle.
pub const FAST_MODE_CONFIG_ID: &str = "fast-mode";
/// Config option ID for the Codex collaboration mode (`default` or `plan`).
pub const COLLABORATION_MODE_CONFIG_ID: &str = "collaboration_mode";

/// Default reasoning effort displayed when none has been configured yet.
pub const DEFAULT_REASONING_EFFORT: &str = "medium";

const STANDARD_EFFORT_LEVELS: &[(&str, &str, &str)] = &[
    (
        "minimal",
        "Minimal",
        "Fastest responses with minimal reasoning",
    ),
    ("low", "Low", "Light reasoning effort"),
    ("medium", "Medium", "Balanced reasoning effort"),
    ("high", "High", "Thorough reasoning for complex tasks"),
    ("xhigh", "Extra High", "Maximum reasoning depth"),
];

/// Canonical ACP session modes mapped onto Codex approval and sandbox policies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AcpMode {
    /// Read-only sandbox with interactive approval prompts.
    ReadOnly,
    /// Workspace-write sandbox with user-reviewed outside-workspace approvals.
    WorkspaceWrite,
    /// Workspace-write sandbox with `auto_review` approvals.
    #[default]
    Agent,
    /// Unrestricted access with `approvalPolicy = "never"`.
    AgentFullAccess,
}

impl AcpMode {
    /// Stable wire identifier for this mode.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReadOnly => "read-only",
            Self::WorkspaceWrite => "workspace-write",
            Self::Agent => "agent",
            Self::AgentFullAccess => "agent-full-access",
        }
    }

    /// Human-readable display label.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::ReadOnly => "Read-only",
            Self::WorkspaceWrite => "Workspace access",
            Self::Agent => "Auto review",
            Self::AgentFullAccess => "Full access",
        }
    }

    /// Human-readable mode description.
    #[must_use]
    pub const fn description(self) -> &'static str {
        match self {
            Self::ReadOnly => "Requires approval to edit files and access the internet.",
            Self::WorkspaceWrite => {
                "Edit workspace files; ask before writing outside the workspace or accessing the network."
            }
            Self::Agent => "Only ask for actions detected as potentially unsafe.",
            Self::AgentFullAccess => {
                "Unrestricted access to the internet and any file on your computer."
            }
        }
    }

    /// All supported [`AcpMode`] variants in display order.
    #[must_use]
    pub const fn all() -> [Self; 4] {
        [
            Self::ReadOnly,
            Self::WorkspaceWrite,
            Self::Agent,
            Self::AgentFullAccess,
        ]
    }

    /// Parse an ACP mode identifier string.
    ///
    /// # Errors
    ///
    /// Returns [`CodexAcpError::InvalidParams`] if `mode_id` is unrecognized.
    pub fn parse(mode_id: &str) -> Result<Self> {
        match mode_id {
            "read-only" => Ok(Self::ReadOnly),
            "workspace-write" => Ok(Self::WorkspaceWrite),
            "agent" => Ok(Self::Agent),
            "agent-full-access" => Ok(Self::AgentFullAccess),
            other => Err(CodexAcpError::InvalidParams(format!(
                "unsupported session mode `{other}`; expected `read-only`, `workspace-write`, `agent`, or `agent-full-access`"
            ))),
        }
    }

    /// App-server `approvalPolicy` wire string for this mode.
    #[must_use]
    pub const fn approval_policy_str(self) -> &'static str {
        match self {
            Self::ReadOnly | Self::WorkspaceWrite | Self::Agent => "on-request",
            Self::AgentFullAccess => "never",
        }
    }

    /// App-server `approvalsReviewer` wire string for this mode.
    #[must_use]
    pub const fn approvals_reviewer_str(self) -> &'static str {
        match self {
            Self::ReadOnly | Self::WorkspaceWrite | Self::AgentFullAccess => "user",
            Self::Agent => "auto_review",
        }
    }

    /// App-server `sandbox` mode wire string for `thread/start`.
    #[must_use]
    pub const fn sandbox_mode_str(self) -> &'static str {
        match self {
            Self::ReadOnly => "read-only",
            Self::WorkspaceWrite | Self::Agent => "workspace-write",
            Self::AgentFullAccess => "danger-full-access",
        }
    }

    /// App-server `sandboxPolicy` JSON object for `thread/start` and `turn/start`.
    #[must_use]
    pub fn sandbox_policy_json(self, writable_roots: &[String]) -> Value {
        match self {
            Self::ReadOnly => json!({
                "type": "readOnly",
                "networkAccess": false,
            }),
            Self::WorkspaceWrite | Self::Agent => json!({
                "type": "workspaceWrite",
                "writableRoots": writable_roots,
                "networkAccess": false,
                "excludeTmpdirEnvVar": false,
                "excludeSlashTmp": false,
            }),
            Self::AgentFullAccess => json!({
                "type": "dangerFullAccess",
            }),
        }
    }
}

/// Codex collaboration mode exposed through the `collaboration_mode` session
/// configuration option.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CollaborationMode {
    /// Standard Codex execution mode.
    #[default]
    Default,
    /// Plan mode: produce a plan before making direct workspace changes.
    Plan,
}

impl CollaborationMode {
    /// Stable wire identifier for this collaboration mode.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::Plan => "plan",
        }
    }

    /// Human-readable display label.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Default => "Default",
            Self::Plan => "Plan",
        }
    }

    /// Human-readable description.
    #[must_use]
    pub const fn description(self) -> &'static str {
        match self {
            Self::Default => "Standard coding mode.",
            Self::Plan => "Plan changes before implementing them.",
        }
    }

    /// All supported [`CollaborationMode`] variants in display order.
    #[must_use]
    pub const fn all() -> [Self; 2] {
        [Self::Default, Self::Plan]
    }

    /// Parse a `collaboration_mode` option value.
    ///
    /// # Errors
    ///
    /// Returns [`CodexAcpError::InvalidParams`] if `raw` is not `"default"` or
    /// `"plan"`.
    pub fn parse(raw: &str) -> Result<Self> {
        match raw.trim() {
            "default" => Ok(Self::Default),
            "plan" => Ok(Self::Plan),
            other => Err(CodexAcpError::InvalidParams(format!(
                "unsupported `collaboration_mode` value `{other}`; expected `default` or `plan`"
            ))),
        }
    }

    /// Build the `codex app-server` `turn/start` `collaborationMode` payload.
    ///
    /// `settings.developer_instructions` is explicitly set to `null` so Codex
    /// applies the built-in instructions for the selected collaboration mode.
    #[must_use]
    pub fn turn_param_json(self, model: Option<&str>, effort: Option<&str>) -> Value {
        json!({
            "mode": self.as_str(),
            "settings": {
                "model": model.unwrap_or_default(),
                "reasoning_effort": effort,
                "developer_instructions": Value::Null,
            },
        })
    }
}

/// Validate a `reasoning_effort` value.
///
/// # Errors
///
/// Returns [`CodexAcpError::InvalidParams`] if `raw` is not one of the
/// supported Codex effort levels.
pub fn validate_reasoning_effort(raw: &str) -> Result<String> {
    let trimmed = raw.trim();
    match trimmed {
        "none" | "minimal" | "low" | "medium" | "high" | "xhigh" => Ok(trimmed.to_string()),
        other => Err(CodexAcpError::InvalidParams(format!(
            "unsupported `reasoning_effort` value `{other}`; expected `none`, `minimal`, `low`, `medium`, `high`, or `xhigh`"
        ))),
    }
}

/// Extract selectable model entries from a serialized `model/list` `data` array.
#[must_use]
pub fn extract_model_entries(models: &[Value]) -> Vec<SessionModel> {
    let mut out = Vec::new();
    for item in models {
        let Some(obj) = item.as_object() else {
            continue;
        };
        let Some(id) = obj
            .get("id")
            .or_else(|| obj.get("model"))
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
        else {
            continue;
        };
        if out.iter().any(|m: &SessionModel| m.model_id == id) {
            continue;
        }
        let name = obj
            .get("displayName")
            .or_else(|| obj.get("name"))
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .unwrap_or(id)
            .to_string();
        let description = obj
            .get("description")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(ToOwned::to_owned);
        out.push(SessionModel {
            model_id: id.to_string(),
            name,
            description,
            extra: BTreeMap::new(),
        });
    }
    out
}

/// Build [`SessionModels`] combining `current_model` and `available`.
#[must_use]
pub fn build_session_models(
    current_model: Option<&str>,
    available: &[SessionModel],
) -> SessionModels {
    let mut list = available.to_vec();
    if let Some(cur) = current_model.filter(|s| !s.is_empty())
        && !list.iter().any(|m| m.model_id == cur)
    {
        list.insert(
            0,
            SessionModel {
                model_id: cur.to_string(),
                name: cur.to_string(),
                description: None,
                extra: BTreeMap::new(),
            },
        );
    }
    SessionModels {
        available_models: list,
        current_model_id: current_model
            .filter(|s| !s.is_empty())
            .map(ToOwned::to_owned),
        extra: BTreeMap::new(),
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
    category: Option<ConfigCategory>,
    current: &str,
    entries: Vec<SelectOption>,
) -> ConfigOption {
    ConfigOption {
        config_id: ConfigId::from_string(id),
        name: name.to_string(),
        description: Some(description.to_string()),
        category: category.map(Open::Known),
        kind: ConfigKind::Select(SelectConfig {
            current_value: ConfigValueId::from_string(current),
            options: SelectOptions::Ungrouped(entries),
        }),
        meta: None,
        extra: BTreeMap::new(),
    }
}

/// Build the full session [`ConfigOption`] list (`mode`, optional `model`,
/// `reasoning_effort`, boolean `fast-mode`, and `collaboration_mode`).
#[must_use]
pub fn build_config_options(
    mode: AcpMode,
    current_model: Option<&str>,
    available_models: &[SessionModel],
    effort: Option<&str>,
    fast_mode: bool,
    collaboration_mode: CollaborationMode,
) -> Vec<ConfigOption> {
    let mode_entries = AcpMode::all()
        .into_iter()
        .map(|m| select_option(m.as_str(), m.name(), Some(m.description())))
        .collect();

    let mut options = vec![select_config(
        MODE_CONFIG_ID,
        "Mode",
        "Approval and sandboxing preset for the session",
        Some(ConfigCategory::Mode),
        mode.as_str(),
        mode_entries,
    )];

    let models = build_session_models(current_model, available_models);
    if !models.available_models.is_empty() {
        let selected = models
            .current_model_id
            .clone()
            .unwrap_or_else(|| models.available_models[0].model_id.clone());
        let select_entries = models
            .available_models
            .into_iter()
            .map(|m| select_option(&m.model_id, &m.name, m.description.as_deref()))
            .collect();
        options.push(select_config(
            MODEL_CONFIG_ID,
            "Model",
            "Model used for the next turn",
            Some(ConfigCategory::Model),
            &selected,
            select_entries,
        ));
    }

    let current_effort = effort
        .filter(|s| !s.is_empty())
        .unwrap_or(DEFAULT_REASONING_EFFORT);
    let mut effort_entries: Vec<SelectOption> = STANDARD_EFFORT_LEVELS
        .iter()
        .map(|(val, name, desc)| select_option(val, name, Some(desc)))
        .collect();
    if !effort_entries
        .iter()
        .any(|e| e.value.as_str() == current_effort)
    {
        effort_entries.insert(0, select_option(current_effort, current_effort, None));
    }
    options.push(select_config(
        EFFORT_CONFIG_ID,
        "Reasoning effort",
        "Reasoning effort for the next turn",
        Some(ConfigCategory::ThoughtLevel),
        current_effort,
        effort_entries,
    ));

    options.push(ConfigOption {
        config_id: ConfigId::from_string(FAST_MODE_CONFIG_ID),
        name: "Fast mode".to_string(),
        description: Some("Prefer speed over cost for the next turn".to_string()),
        category: Some(Open::Known(ConfigCategory::ModelConfig)),
        kind: ConfigKind::Boolean(BooleanConfig {
            current_value: fast_mode,
        }),
        meta: None,
        extra: BTreeMap::new(),
    });

    let collaboration_entries = CollaborationMode::all()
        .into_iter()
        .map(|cm| select_option(cm.as_str(), cm.name(), Some(cm.description())))
        .collect();
    options.push(select_config(
        COLLABORATION_MODE_CONFIG_ID,
        "Collaboration mode",
        "Collaboration preset for upcoming turns",
        None,
        collaboration_mode.as_str(),
        collaboration_entries,
    ));

    options
}

/// Built-in slash commands advertised on every Codex ACP session.
#[must_use]
pub fn build_available_commands() -> Vec<AvailableCommand> {
    let text_input = |hint: &str| {
        Some(Open::Known(CommandInput::Text(TextInput {
            hint: Some(hint.to_string()),
            meta: None,
        })))
    };
    [
        (
            "compact",
            "Compact conversation history to free context window space",
            None,
        ),
        (
            "status",
            "Show current session mode, model, effort, fast mode, and account status",
            None,
        ),
        ("logout", "Sign out of the active Codex account", None),
        ("review", "Review uncommitted workspace changes", None),
        (
            "review-branch",
            "Review changes against a base branch",
            text_input("Base branch name (e.g. main)"),
        ),
        (
            "review-commit",
            "Review a specific commit",
            text_input("Commit SHA"),
        ),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collaboration_mode_parse_and_wire_payload() {
        assert_eq!(
            CollaborationMode::parse("default").unwrap(),
            CollaborationMode::Default
        );
        assert_eq!(
            CollaborationMode::parse(" plan ").unwrap(),
            CollaborationMode::Plan
        );
        assert!(matches!(
            CollaborationMode::parse("invalid"),
            Err(CodexAcpError::InvalidParams(_))
        ));

        let payload = CollaborationMode::Plan.turn_param_json(Some("gpt-5.4"), Some("high"));
        assert_eq!(
            payload,
            json!({
                "mode": "plan",
                "settings": {
                    "model": "gpt-5.4",
                    "reasoning_effort": "high",
                    "developer_instructions": Value::Null,
                },
            })
        );

        let default_payload = CollaborationMode::Default.turn_param_json(None, None);
        assert_eq!(
            default_payload,
            json!({
                "mode": "default",
                "settings": {
                    "model": "",
                    "reasoning_effort": Value::Null,
                    "developer_instructions": Value::Null,
                },
            })
        );
    }

    #[test]
    fn config_options_include_collaboration_mode() {
        let opts = build_config_options(
            AcpMode::ReadOnly,
            Some("gpt-5.4"),
            &[],
            Some("medium"),
            false,
            CollaborationMode::Plan,
        );
        let collab = opts
            .iter()
            .find(|o| o.config_id.as_str() == COLLABORATION_MODE_CONFIG_ID)
            .expect("collaboration_mode config option missing");
        assert!(
            !collab.is_mode(),
            "collaboration_mode must not collide with mode"
        );
        let ConfigKind::Select(select) = &collab.kind else {
            panic!("expected select config kind, got {:?}", collab.kind);
        };
        assert_eq!(select.current_value.as_str(), "plan");
        let SelectOptions::Ungrouped(entries) = &select.options else {
            panic!("expected ungrouped select options");
        };
        let values: Vec<&str> = entries.iter().map(|e| e.value.as_str()).collect();
        assert_eq!(values, vec!["default", "plan"]);
    }
}
