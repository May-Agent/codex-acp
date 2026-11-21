//! Session permission memory and `ServerRequestHandler` implementation.
//!
//! Bridges Codex approvals (`CmdExecApproval`, `ExecCommandApproval`,
//! `FileChangeApproval`, `ApplyPatchApproval`, `PermissionsRequestApproval`),
//! `ToolRequestUserInput`, `McpServerElicitationRequest`, and `ItemToolCall`
//! onto [`aither_acp::server::SessionContext`].

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use aither_acp::domain::ids::ElicitationId;
use aither_acp::domain::{
    ElicitAction, ElicitContentValue, ElicitCreate, ElicitMode, ElicitProperty, ElicitationSchema,
    EnumOption, FormMode, Location, Open, Patch, PermissionOption, PermissionOptionId,
    PermissionOptionKind, PermissionOutcome, PermissionRequest, PermissionSubject, PropertyCommon,
    StringProperty, ToolCallContent, ToolCallId, ToolCallStatus, ToolCallSubject, ToolCallUpdate,
    ToolKind, UrlMode,
};
use codex_codes::{
    ApplyPatchApprovalParams, ApplyPatchApprovalResponse, CommandExecutionApprovalDecision,
    CommandExecutionRequestApprovalParams, CommandExecutionRequestApprovalResponse,
    DynamicToolCallParams, DynamicToolCallResponse, ExecCommandApprovalParams,
    ExecCommandApprovalResponse, FileChangeApprovalDecision, FileChangeRequestApprovalParams,
    FileChangeRequestApprovalResponse, McpServerElicitationRequestParams,
    McpServerElicitationRequestResponse, PermissionsRequestApprovalParams,
    PermissionsRequestApprovalResponse, ReviewDecision, ToolRequestUserInputParams,
    ToolRequestUserInputResponse,
    protocol_generated::types::{
        DynamicToolCallOutputContentItem, GrantedPermissionProfile, McpServerElicitationAction,
        ToolRequestUserInputAnswer,
    },
};
use codex_sdk::{HandlerResult, ServerRequestHandler};
use serde_json::{Value, json};

use crate::convert::diff_content;
use crate::mode::AcpMode;
use crate::session::CodexSession;

const OPT_ALLOW_ONCE: &str = "allow_once";
const OPT_ALLOW_ALWAYS: &str = "allow_always";
const OPT_REJECT_ONCE: &str = "reject_once";
const OPT_REJECT_ALWAYS: &str = "reject_always";

const RULE_COMMAND: &str = "command";
const RULE_FILE_CHANGE: &str = "file_change";
const RULE_PERMISSIONS: &str = "permissions";

/// Normalized resolution of an interactive permission prompt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PermissionResolution {
    /// Approved for this invocation only.
    AllowOnce,
    /// Approved and remembered for the rest of the session.
    AllowAlways,
    /// Rejected for this invocation only.
    RejectOnce,
    /// Rejected and remembered for the rest of the session.
    RejectAlways,
    /// Prompt cancelled or transport closed.
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RememberedRule {
    category: String,
    subject: Option<String>,
    allow: bool,
}

/// Per-session store of `AllowAlways` and `RejectAlways` decisions.
#[derive(Debug, Default)]
pub struct PermissionMemory {
    rules: Mutex<Vec<RememberedRule>>,
}

impl PermissionMemory {
    /// Create an empty [`PermissionMemory`].
    #[must_use]
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Evaluate whether `category` + `subject` matches an existing session rule.
    /// Deny rules take precedence over allow rules.
    pub(crate) fn evaluate(
        &self,
        category: &str,
        subject: Option<&str>,
    ) -> Option<PermissionResolution> {
        let (denied, allowed) = {
            let rules = self.rules.lock().expect("permission memory mutex poisoned");
            let matches_rule = |r: &RememberedRule| {
                r.category == category
                    && match (&r.subject, subject) {
                        (None, _) => true,
                        (Some(pat), Some(sub)) => pat == sub,
                        (Some(_), None) => false,
                    }
            };
            (
                rules.iter().any(|r| !r.allow && matches_rule(r)),
                rules.iter().any(|r| r.allow && matches_rule(r)),
            )
        };

        if denied {
            return Some(PermissionResolution::RejectAlways);
        }
        if allowed {
            return Some(PermissionResolution::AllowAlways);
        }
        None
    }

    /// Persist a session-wide allow or deny rule.
    pub(crate) fn record(&self, category: &str, subject: Option<String>, allow: bool) {
        let mut rules = self.rules.lock().expect("permission memory mutex poisoned");
        let candidate = RememberedRule {
            category: category.to_string(),
            subject,
            allow,
        };
        if !rules.contains(&candidate) {
            rules.push(candidate);
        }
    }
}

#[derive(Debug, Default)]
struct RegistryInner {
    by_session: HashMap<String, CodexSession>,
    by_thread: HashMap<String, CodexSession>,
    known_threads: HashMap<String, String>,
}

/// Shared session registry consulted by both `CodexAgent` and
/// [`BridgeServerRequestHandler`] (including out-of-prompt server requests).
#[derive(Debug, Default)]
pub struct SessionRegistry {
    inner: Mutex<RegistryInner>,
}

impl SessionRegistry {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn insert(&self, session: CodexSession) -> Option<CodexSession> {
        let mut guard = self.inner.lock().expect("session registry poisoned");
        guard.known_threads.insert(
            session.id().as_str().to_string(),
            session.thread_id().to_string(),
        );
        guard
            .by_thread
            .insert(session.thread_id().to_string(), session.clone());
        guard
            .by_session
            .insert(session.id().as_str().to_string(), session)
    }

    pub(crate) fn resolve_thread_id(&self, session_or_thread: &str) -> String {
        let guard = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(live) = guard.by_session.get(session_or_thread) {
            return live.thread_id().to_string();
        }
        if let Some(known) = guard.known_threads.get(session_or_thread) {
            return known.clone();
        }
        drop(guard);
        session_or_thread.to_string()
    }

    pub(crate) fn get_by_session(&self, session_id: &str) -> Option<CodexSession> {
        self.inner
            .lock()
            .expect("session registry poisoned")
            .by_session
            .get(session_id)
            .cloned()
    }

    pub(crate) fn resolve(&self, id_or_thread: &str) -> Option<CodexSession> {
        let guard = self.inner.lock().expect("session registry poisoned");
        if !id_or_thread.is_empty() {
            if let Some(s) = guard.by_thread.get(id_or_thread) {
                return Some(s.clone());
            }
            if let Some(s) = guard.by_session.get(id_or_thread) {
                return Some(s.clone());
            }
        }
        guard.by_session.values().next().cloned()
    }

    pub(crate) fn remove_session(&self, session_id: &str) -> Option<CodexSession> {
        let mut guard = self.inner.lock().expect("session registry poisoned");
        let removed = guard.by_session.remove(session_id);
        if let Some(s) = &removed {
            guard.by_thread.remove(s.thread_id());
        }
        removed
    }

    pub(crate) fn drain_all(&self) -> Vec<CodexSession> {
        let mut guard = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard.by_thread.clear();
        guard.by_session.drain().map(|(_, s)| s).collect()
    }

    pub(crate) fn len(&self) -> usize {
        self.inner.lock().map_or(0, |g| g.by_session.len())
    }
}

fn default_permission_options() -> Vec<PermissionOption> {
    [
        (
            OPT_ALLOW_ONCE,
            "Allow once",
            PermissionOptionKind::AllowOnce,
        ),
        (
            OPT_ALLOW_ALWAYS,
            "Allow for session",
            PermissionOptionKind::AllowAlways,
        ),
        (
            OPT_REJECT_ONCE,
            "Reject once",
            PermissionOptionKind::RejectOnce,
        ),
        (
            OPT_REJECT_ALWAYS,
            "Reject for session",
            PermissionOptionKind::RejectAlways,
        ),
    ]
    .into_iter()
    .map(|(id, name, kind)| PermissionOption {
        option_id: PermissionOptionId::from_string(id),
        name: name.to_string(),
        kind: Open::Known(kind),
        meta: None,
        extra: BTreeMap::new(),
    })
    .collect()
}

/// [`ServerRequestHandler`] bound to [`SessionRegistry`], servicing server-initiated
/// requests both mid-turn and outside active turns.
#[derive(Debug, Clone)]
pub struct BridgeServerRequestHandler {
    registry: Arc<SessionRegistry>,
}

impl BridgeServerRequestHandler {
    pub(crate) const fn new(registry: Arc<SessionRegistry>) -> Self {
        Self { registry }
    }

    /// Prompt the client for permission on `session` (resolved via `thread_id`),
    /// consulting and updating [`PermissionMemory`].
    pub(crate) async fn prompt_permission(
        &self,
        thread_id: &str,
        category: &str,
        subject: Option<String>,
        tool_call: ToolCallUpdate,
    ) -> PermissionResolution {
        let Some(session) = self.registry.resolve(thread_id) else {
            return PermissionResolution::Cancelled;
        };

        if let Some(remembered) = session
            .permission_memory()
            .evaluate(category, subject.as_deref())
        {
            return remembered;
        }
        if session.current_mode() == AcpMode::AgentFullAccess {
            return PermissionResolution::AllowOnce;
        }

        prompt_session_permission(&session, category, subject, tool_call).await
    }
}

/// Prompt a specific [`CodexSession`] for permission and record session-scoped choices.
pub(crate) async fn prompt_session_permission(
    session: &CodexSession,
    category: &str,
    subject: Option<String>,
    tool_call: ToolCallUpdate,
) -> PermissionResolution {
    let title = tool_call
        .title
        .clone()
        .into_option()
        .unwrap_or_else(|| "Permission required".to_string());
    let options = default_permission_options();
    let request = PermissionRequest {
        session_id: session.id().clone(),
        title,
        description: None,
        subject: Some(Open::Known(PermissionSubject::ToolCall(Box::new(
            ToolCallSubject {
                tool_call,
                meta: None,
                extra: BTreeMap::new(),
            },
        )))),
        options: options.clone(),
        meta: None,
        extra: BTreeMap::new(),
    };

    let Ok(outcome) = session.context().request_permission(request).await else {
        return PermissionResolution::Cancelled;
    };

    match outcome {
        PermissionOutcome::Cancelled(_) | PermissionOutcome::Unknown(..) => {
            PermissionResolution::Cancelled
        }
        PermissionOutcome::Custom(..) => PermissionResolution::RejectOnce,
        PermissionOutcome::Selected(selected) => {
            let kind = options
                .iter()
                .find(|o| o.option_id == selected.option_id)
                .and_then(|o| match &o.kind {
                    Open::Known(k) => Some(*k),
                    _ => None,
                });
            match kind {
                Some(PermissionOptionKind::AllowOnce) => PermissionResolution::AllowOnce,
                Some(PermissionOptionKind::AllowAlways) => {
                    session.permission_memory().record(category, subject, true);
                    PermissionResolution::AllowAlways
                }
                Some(PermissionOptionKind::RejectAlways) => {
                    session.permission_memory().record(category, subject, false);
                    PermissionResolution::RejectAlways
                }
                Some(PermissionOptionKind::RejectOnce) | None => PermissionResolution::RejectOnce,
            }
        }
    }
}

fn build_subject_call(
    id: &str,
    title: String,
    kind: ToolKind,
    locations: Vec<Location>,
    content: Vec<ToolCallContent>,
    raw_input: Value,
) -> ToolCallUpdate {
    let mut u = ToolCallUpdate::new(ToolCallId::from_string(id));
    u.title = Patch::Set(title);
    u.kind = Patch::Set(Open::Known(kind));
    u.status = Patch::Set(Open::Known(ToolCallStatus::Pending));
    u.raw_input = Patch::Set(raw_input);
    if !locations.is_empty() {
        u.locations = Patch::Set(locations);
    }
    if !content.is_empty() {
        u.content = Patch::Set(content);
    }
    u
}

impl ServerRequestHandler for BridgeServerRequestHandler {
    async fn command_execution_approval(
        &self,
        params: CommandExecutionRequestApprovalParams,
    ) -> HandlerResult<CommandExecutionRequestApprovalResponse> {
        let raw = serde_json::to_value(&params).unwrap_or_else(|_| json!({}));
        let thread_id = raw
            .get("threadId")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let id = if params.item_id.is_empty() {
            params
                .approval_id
                .clone()
                .unwrap_or_else(|| "cmd-approval".to_string())
        } else {
            params.item_id.clone()
        };
        let title = params
            .command
            .as_deref()
            .filter(|s| !s.is_empty())
            .map(|c| format!("Run `{c}`"))
            .or_else(|| {
                raw.get("reason")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned)
            })
            .unwrap_or_else(|| "Run command?".to_string());
        let locations = raw
            .get("cwd")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(|p| {
                vec![Location {
                    path: PathBuf::from(p),
                    line: None,
                    meta: None,
                }]
            })
            .unwrap_or_default();
        let call = build_subject_call(&id, title, ToolKind::Execute, locations, Vec::new(), raw);

        let resolution = self
            .prompt_permission(&thread_id, RULE_COMMAND, params.command, call)
            .await;
        let decision = match resolution {
            PermissionResolution::AllowOnce => CommandExecutionApprovalDecision::Accept,
            PermissionResolution::AllowAlways => CommandExecutionApprovalDecision::AcceptForSession,
            PermissionResolution::RejectOnce | PermissionResolution::RejectAlways => {
                CommandExecutionApprovalDecision::Decline
            }
            PermissionResolution::Cancelled => CommandExecutionApprovalDecision::Cancel,
        };
        Ok(CommandExecutionRequestApprovalResponse { decision })
    }

    async fn exec_command_approval(
        &self,
        params: ExecCommandApprovalParams,
    ) -> HandlerResult<ExecCommandApprovalResponse> {
        let raw = serde_json::to_value(&params).unwrap_or_else(|_| json!({}));
        let thread_id = raw
            .get("conversationId")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let cmd_str = params.command.join(" ");
        let id = if params.call_id.is_empty() {
            "exec-approval".to_string()
        } else {
            params.call_id.clone()
        };
        let title = if cmd_str.is_empty() {
            params
                .reason
                .clone()
                .unwrap_or_else(|| "Run command?".to_string())
        } else {
            format!("Run `{cmd_str}`")
        };
        let workdir = raw.get("cwd").and_then(Value::as_str).unwrap_or_default();
        let locations = if workdir.is_empty() {
            Vec::new()
        } else {
            vec![Location {
                path: PathBuf::from(workdir),
                line: None,
                meta: None,
            }]
        };
        let call = build_subject_call(&id, title, ToolKind::Execute, locations, Vec::new(), raw);

        let subject = (!cmd_str.is_empty()).then_some(cmd_str);
        let resolution = self
            .prompt_permission(&thread_id, RULE_COMMAND, subject, call)
            .await;
        let decision = to_review_decision(resolution, "Command rejected by user");
        Ok(ExecCommandApprovalResponse { decision })
    }

    async fn file_change_approval(
        &self,
        params: FileChangeRequestApprovalParams,
    ) -> HandlerResult<FileChangeRequestApprovalResponse> {
        let raw = serde_json::to_value(&params).unwrap_or_else(|_| json!({}));
        let id = if params.item_id.is_empty() {
            "file-approval".to_string()
        } else {
            params.item_id.clone()
        };
        let title = params
            .reason
            .clone()
            .or_else(|| {
                params
                    .grant_root
                    .as_deref()
                    .map(|root| format!("Modify files under {root}"))
            })
            .unwrap_or_else(|| "Apply file changes?".to_string());
        let locations = params
            .grant_root
            .as_deref()
            .filter(|s| !s.is_empty())
            .map(|root| {
                vec![Location {
                    path: PathBuf::from(root),
                    line: None,
                    meta: None,
                }]
            })
            .unwrap_or_default();
        let call = build_subject_call(&id, title, ToolKind::Edit, locations, Vec::new(), raw);

        let resolution = self
            .prompt_permission(&params.thread_id, RULE_FILE_CHANGE, params.grant_root, call)
            .await;
        let decision = match resolution {
            PermissionResolution::AllowOnce => FileChangeApprovalDecision::Accept,
            PermissionResolution::AllowAlways => FileChangeApprovalDecision::AcceptForSession,
            PermissionResolution::RejectOnce | PermissionResolution::RejectAlways => {
                FileChangeApprovalDecision::Decline
            }
            PermissionResolution::Cancelled => FileChangeApprovalDecision::Cancel,
        };
        Ok(FileChangeRequestApprovalResponse { decision })
    }

    async fn apply_patch_approval(
        &self,
        params: ApplyPatchApprovalParams,
    ) -> HandlerResult<ApplyPatchApprovalResponse> {
        let raw = serde_json::to_value(&params).unwrap_or_else(|_| json!({}));
        let thread_id = raw
            .get("conversationId")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let id = if params.call_id.is_empty() {
            "patch-approval".to_string()
        } else {
            params.call_id.clone()
        };
        let mut locations = Vec::new();
        let mut change_items = Vec::new();
        for (path_str, change) in &params.file_changes {
            locations.push(Location {
                path: PathBuf::from(path_str),
                line: None,
                meta: None,
            });
            let patch_text = serde_json::to_value(change)
                .ok()
                .and_then(|v| {
                    v.get("unified_diff")
                        .or_else(|| v.get("content"))
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned)
                })
                .unwrap_or_default();
            change_items.push(json!({
                "path": path_str,
                "diff": patch_text,
                "kind": "modify",
            }));
        }
        let diffs = diff_content(&json!({"changes": change_items})).unwrap_or_default();
        let title = params
            .reason
            .clone()
            .or_else(|| {
                params
                    .file_changes
                    .keys()
                    .next()
                    .map(|p| format!("Patch {p}"))
            })
            .unwrap_or_else(|| "Apply file changes?".to_string());
        let call = build_subject_call(&id, title, ToolKind::Edit, locations, diffs, raw);

        let resolution = self
            .prompt_permission(&thread_id, RULE_FILE_CHANGE, params.grant_root, call)
            .await;
        let decision = to_review_decision(resolution, "Patch rejected by user");
        Ok(ApplyPatchApprovalResponse { decision })
    }

    async fn permissions_approval(
        &self,
        params: PermissionsRequestApprovalParams,
    ) -> HandlerResult<PermissionsRequestApprovalResponse> {
        let raw = serde_json::to_value(&params).unwrap_or_else(|_| json!({}));
        let id = if params.item_id.is_empty() {
            "permissions-approval".to_string()
        } else {
            params.item_id.clone()
        };
        let title = params
            .reason
            .clone()
            .unwrap_or_else(|| "Grant requested permissions?".to_string());
        let workdir = raw.get("cwd").and_then(Value::as_str).unwrap_or_default();
        let locations = if workdir.is_empty() {
            Vec::new()
        } else {
            vec![Location {
                path: PathBuf::from(workdir),
                line: None,
                meta: None,
            }]
        };
        let call = build_subject_call(&id, title, ToolKind::Other, locations, Vec::new(), raw);
        let subject = serde_json::to_string(&params.permissions).ok();
        let resolution = self
            .prompt_permission(&params.thread_id, RULE_PERMISSIONS, subject, call)
            .await;

        let granted = match resolution {
            PermissionResolution::AllowOnce | PermissionResolution::AllowAlways => {
                GrantedPermissionProfile {
                    file_system: params.permissions.file_system,
                    network: params.permissions.network,
                }
            }
            PermissionResolution::RejectOnce
            | PermissionResolution::RejectAlways
            | PermissionResolution::Cancelled => GrantedPermissionProfile::default(),
        };
        let scope =
            matches!(resolution, PermissionResolution::AllowAlways).then(|| json!("session"));
        Ok(PermissionsRequestApprovalResponse {
            permissions: granted,
            scope,
            strict_auto_review: None,
        })
    }

    async fn request_user_input(
        &self,
        params: ToolRequestUserInputParams,
    ) -> HandlerResult<ToolRequestUserInputResponse> {
        let Some(session) = self.registry.resolve(&params.thread_id) else {
            return Ok(ToolRequestUserInputResponse::default());
        };
        if params.questions.is_empty() {
            return Ok(ToolRequestUserInputResponse::default());
        }

        let mut properties = BTreeMap::new();
        let mut required = Vec::new();
        for q in &params.questions {
            if q.id.is_empty() {
                continue;
            }
            required.push(q.id.clone());
            let one_of = q.options.as_ref().map(|opts| {
                opts.iter()
                    .map(|o| EnumOption {
                        const_value: o.label.clone(),
                        title: if o.description.is_empty() {
                            o.label.clone()
                        } else {
                            format!("{} — {}", o.label, o.description)
                        },
                        meta: None,
                        extra: BTreeMap::new(),
                    })
                    .collect::<Vec<_>>()
            });
            properties.insert(
                q.id.clone(),
                Open::Known(ElicitProperty::String(StringProperty {
                    common: PropertyCommon {
                        title: (!q.header.is_empty()).then(|| q.header.clone()),
                        description: (!q.question.is_empty()).then(|| q.question.clone()),
                        meta: None,
                        extra: BTreeMap::new(),
                    },
                    min_length: None,
                    max_length: None,
                    format: None,
                    default: None,
                    enum_values: None,
                    one_of: one_of.filter(|v| !v.is_empty()),
                })),
            );
        }

        let message = params
            .questions
            .first()
            .map_or_else(|| "Input requested".to_string(), |q| q.question.clone());
        let create = ElicitCreate {
            mode: Open::Known(ElicitMode::Form(FormMode {
                requested_schema: ElicitationSchema {
                    schema_type: "object".to_string(),
                    title: None,
                    properties,
                    required,
                    description: None,
                    meta: None,
                    extra: BTreeMap::new(),
                },
                meta: None,
                extra: BTreeMap::new(),
            })),
            message,
            meta: None,
            extra: BTreeMap::new(),
        };

        let Some(Ok(res)) = session.context().elicit(create).await else {
            return Ok(ToolRequestUserInputResponse::default());
        };
        let mut answers = BTreeMap::new();
        if let Open::Known(ElicitAction::Accept(accept)) = res.action
            && let Some(content) = accept.content
        {
            for (k, val) in content {
                let list = match val {
                    ElicitContentValue::String(s) => vec![s],
                    ElicitContentValue::Integer(i) => vec![i.to_string()],
                    ElicitContentValue::Number(n) => vec![n.to_string()],
                    ElicitContentValue::Boolean(b) => vec![b.to_string()],
                    ElicitContentValue::Array(arr) => arr,
                };
                answers.insert(k, ToolRequestUserInputAnswer { answers: list });
            }
        }
        Ok(ToolRequestUserInputResponse { answers })
    }

    fn dynamic_tool_call(
        &self,
        params: DynamicToolCallParams,
    ) -> impl std::future::Future<Output = HandlerResult<DynamicToolCallResponse>> + Send {
        std::future::ready(Ok(DynamicToolCallResponse {
            content_items: vec![DynamicToolCallOutputContentItem::InputText {
                text: format!(
                    "dynamic client tool `{}` is not registered on this ACP bridge",
                    params.tool
                ),
            }],
            success: false,
        }))
    }

    async fn mcp_elicitation(
        &self,
        params: McpServerElicitationRequestParams,
    ) -> HandlerResult<McpServerElicitationRequestResponse> {
        match params {
            McpServerElicitationRequestParams::Form {
                _meta: meta,
                message,
                requested_schema,
            } => {
                let raw_schema =
                    serde_json::to_value(&requested_schema).unwrap_or_else(|_| json!({}));
                self.forward_form_elicitation(meta.as_ref(), message, &raw_schema)
                    .await
            }
            McpServerElicitationRequestParams::OpenaiForm {
                _meta: meta,
                message,
                requested_schema,
            }
            | McpServerElicitationRequestParams::OpenAiElicitationForm {
                _meta: meta,
                message,
                requested_schema,
            } => {
                self.forward_form_elicitation(meta.as_ref(), message, &requested_schema)
                    .await
            }
            McpServerElicitationRequestParams::Url {
                _meta: meta,
                elicitation_id,
                message,
                url,
            } => {
                let target = meta_target_id(meta.as_ref()).unwrap_or_default();
                let Some(session) = self.registry.resolve(&target) else {
                    return Ok(decline_elicitation());
                };
                let create = ElicitCreate {
                    mode: Open::Known(ElicitMode::Url(UrlMode {
                        elicitation_id: ElicitationId::from_string(elicitation_id),
                        url,
                        meta: None,
                        extra: BTreeMap::new(),
                    })),
                    message,
                    meta,
                    extra: BTreeMap::new(),
                };
                let Some(Ok(res)) = session.context().elicit(create).await else {
                    return Ok(decline_elicitation());
                };
                Ok(map_elicitation_action(res.action))
            }
        }
    }
}

impl BridgeServerRequestHandler {
    async fn forward_form_elicitation(
        &self,
        meta: Option<&Value>,
        message: String,
        raw_schema: &Value,
    ) -> HandlerResult<McpServerElicitationRequestResponse> {
        let target = meta_target_id(meta).unwrap_or_default();
        let Some(session) = self.registry.resolve(&target) else {
            return Ok(decline_elicitation());
        };
        let schema: ElicitationSchema =
            serde_json::from_value(raw_schema.clone()).unwrap_or_else(|_| ElicitationSchema {
                schema_type: "object".to_string(),
                title: None,
                properties: BTreeMap::new(),
                required: Vec::new(),
                description: None,
                meta: None,
                extra: BTreeMap::new(),
            });
        let create = ElicitCreate {
            mode: Open::Known(ElicitMode::Form(FormMode {
                requested_schema: schema,
                meta: None,
                extra: BTreeMap::new(),
            })),
            message,
            meta: meta.cloned(),
            extra: BTreeMap::new(),
        };
        let Some(Ok(res)) = session.context().elicit(create).await else {
            return Ok(decline_elicitation());
        };
        Ok(map_elicitation_action(res.action))
    }
}

fn meta_target_id(meta: Option<&Value>) -> Option<String> {
    let obj = meta?.as_object()?;
    obj.get("sessionId")
        .or_else(|| obj.get("threadId"))
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(ToOwned::to_owned)
}

const fn decline_elicitation() -> McpServerElicitationRequestResponse {
    McpServerElicitationRequestResponse {
        _meta: None,
        action: McpServerElicitationAction::Decline,
        content: None,
    }
}

fn map_elicitation_action(action: Open<ElicitAction>) -> McpServerElicitationRequestResponse {
    match action {
        Open::Known(ElicitAction::Accept(accept)) => McpServerElicitationRequestResponse {
            _meta: accept.meta,
            action: McpServerElicitationAction::Accept,
            content: accept.content.and_then(|c| serde_json::to_value(c).ok()),
        },
        Open::Known(ElicitAction::Cancel(_)) => McpServerElicitationRequestResponse {
            _meta: None,
            action: McpServerElicitationAction::Cancel,
            content: None,
        },
        Open::Known(ElicitAction::Decline(_)) | Open::Custom(..) | Open::Unknown(..) => {
            decline_elicitation()
        }
    }
}

fn to_review_decision(resolution: PermissionResolution, rejection_msg: &str) -> ReviewDecision {
    match resolution {
        PermissionResolution::AllowOnce => ReviewDecision::Approved,
        PermissionResolution::AllowAlways => ReviewDecision::ApprovedForSession,
        PermissionResolution::RejectOnce | PermissionResolution::RejectAlways => {
            ReviewDecision::Denied {
                rejection: rejection_msg.to_string(),
            }
        }
        PermissionResolution::Cancelled => ReviewDecision::Abort,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permission_memory_prefers_deny_over_allow() {
        let mem = PermissionMemory::new();
        mem.record(RULE_COMMAND, Some("cargo test".to_string()), true);
        assert_eq!(
            mem.evaluate(RULE_COMMAND, Some("cargo test")),
            Some(PermissionResolution::AllowAlways)
        );
        mem.record(RULE_COMMAND, Some("cargo test".to_string()), false);
        assert_eq!(
            mem.evaluate(RULE_COMMAND, Some("cargo test")),
            Some(PermissionResolution::RejectAlways)
        );
    }

    #[test]
    fn approval_decision_mapping_tables() {
        assert_eq!(
            to_review_decision(PermissionResolution::AllowOnce, "no"),
            ReviewDecision::Approved
        );
        assert_eq!(
            to_review_decision(PermissionResolution::AllowAlways, "no"),
            ReviewDecision::ApprovedForSession
        );
        assert_eq!(
            to_review_decision(PermissionResolution::Cancelled, "no"),
            ReviewDecision::Abort
        );
        assert!(matches!(
            to_review_decision(PermissionResolution::RejectOnce, "no"),
            ReviewDecision::Denied { .. }
        ));
    }
}
