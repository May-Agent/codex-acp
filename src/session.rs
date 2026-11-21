//! Per-session ACP state and streaming `ThreadEvent` -> `SessionUpdate` turn loop.

use std::fmt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use aither_acp::domain::ids::ConfigId;
use aither_acp::domain::{
    AvailableCommand, ConfigOption, ConfigValue, ContentBlock, NoticeSeverity, Open, Patch, PlanId,
    SessionId, SessionUpdate, StopReason, ToolCallId, ToolCallStatus, ToolCallUpdate, ToolKind,
};
use aither_acp::rpc::RpcError;
use aither_acp::server::{
    MessageStream, PendingPrompt, PromptError, Session, SessionContext, Turn,
};
use aither_acp::vendor::codex::SessionModel;
use async_channel::Receiver;
use codex_codes::{GetAccountParams, Notification, protocol::methods};
use codex_sdk::{AppServer, METHOD_NOT_FOUND_CODE, SdkError, ThreadEvent};
use futures_lite::future::or;
use serde_json::{Value, json};
use tracing::debug;

use crate::auth::{AuthState, notify_auth_status_cx};
use crate::convert::{
    convert_prompt_blocks, emit_notice, emit_notice_from_payload, item_as_tool_call,
    item_completed_update, output_chunk, patch_update, plan_entries, stop_reason, usage_update,
};
use crate::driver::DriverSlot;
use crate::error::{CodexAcpError, Result};
use crate::mode::{
    AcpMode, DEFAULT_REASONING_EFFORT, EFFORT_CONFIG_ID, FAST_MODE_CONFIG_ID, MODE_CONFIG_ID,
    MODEL_CONFIG_ID, build_available_commands, build_config_options, validate_reasoning_effort,
};
use crate::permission::{
    PermissionMemory, PermissionResolution, SessionRegistry, prompt_session_permission,
};

struct SessionInner {
    id: SessionId,
    thread_id: String,
    cwd: PathBuf,
    additional_directories: Vec<PathBuf>,
    mcp_server_names: Vec<String>,
    cx: SessionContext,
    events: Receiver<ThreadEvent>,
    server: AppServer,
    driver: Arc<DriverSlot>,
    auth: Arc<Mutex<AuthState>>,
    registry: Arc<SessionRegistry>,
    mode: Mutex<AcpMode>,
    model: Mutex<Option<String>>,
    effort: Mutex<Option<String>>,
    fast_mode: AtomicBool,
    available_models: Mutex<Vec<SessionModel>>,
    active_turn: Mutex<Option<String>>,
    permission_memory: Arc<PermissionMemory>,
}

/// A live bridge session: one ACP session <-> one `codex app-server` thread.
#[derive(Clone)]
pub struct CodexSession {
    inner: Arc<SessionInner>,
}

impl fmt::Debug for CodexSession {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CodexSession")
            .field("id", &self.inner.id)
            .field("thread_id", &self.inner.thread_id)
            .field("cwd", &self.inner.cwd)
            .finish_non_exhaustive()
    }
}

impl CodexSession {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        id: SessionId,
        thread_id: String,
        cwd: PathBuf,
        additional_directories: Vec<PathBuf>,
        mcp_server_names: Vec<String>,
        cx: SessionContext,
        events: Receiver<ThreadEvent>,
        server: AppServer,
        driver: Arc<DriverSlot>,
        auth: Arc<Mutex<AuthState>>,
        registry: Arc<SessionRegistry>,
        initial_mode: AcpMode,
        initial_model: Option<String>,
        initial_effort: Option<String>,
        available_models: Vec<SessionModel>,
    ) -> Self {
        Self {
            inner: Arc::new(SessionInner {
                id,
                thread_id,
                cwd,
                additional_directories,
                mcp_server_names,
                cx,
                events,
                server,
                driver,
                auth,
                registry,
                mode: Mutex::new(initial_mode),
                model: Mutex::new(initial_model.filter(|s| !s.trim().is_empty())),
                effort: Mutex::new(initial_effort.filter(|s| !s.trim().is_empty())),
                fast_mode: AtomicBool::new(false),
                available_models: Mutex::new(available_models),
                active_turn: Mutex::new(None),
                permission_memory: Arc::new(PermissionMemory::new()),
            }),
        }
    }

    /// The ACP session identifier.
    #[must_use]
    pub fn id(&self) -> &SessionId {
        &self.inner.id
    }

    /// The `codex app-server` thread identifier.
    #[must_use]
    pub fn thread_id(&self) -> &str {
        &self.inner.thread_id
    }

    /// The [`SessionContext`] bound to this session.
    #[must_use]
    pub fn context(&self) -> &SessionContext {
        &self.inner.cx
    }

    pub(crate) fn permission_memory(&self) -> &Arc<PermissionMemory> {
        &self.inner.permission_memory
    }

    /// Current [`AcpMode`] of this session.
    #[must_use]
    pub fn current_mode(&self) -> AcpMode {
        *self
            .inner
            .mode
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Currently selected model identifier, if any.
    #[must_use]
    pub fn current_model(&self) -> Option<String> {
        self.inner
            .model
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Override the selected model identifier (legacy `session/set_model`).
    pub fn set_model(&self, model: Option<String>) {
        *self
            .inner
            .model
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            model.filter(|s| !s.trim().is_empty());
    }

    /// Currently selected reasoning effort, if any.
    #[must_use]
    pub fn current_effort(&self) -> Option<String> {
        self.inner
            .effort
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Whether fast mode is enabled for upcoming turns.
    #[must_use]
    pub fn fast_mode(&self) -> bool {
        self.inner.fast_mode.load(Ordering::Acquire)
    }

    /// Currently active app-server `turnId`, when a turn is in flight.
    #[must_use]
    pub fn active_turn_id(&self) -> Option<String> {
        self.inner
            .active_turn
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub(crate) fn set_active_turn(&self, turn_id: Option<String>) {
        *self
            .inner
            .active_turn
            .lock()
            .expect("active_turn mutex poisoned") = turn_id;
    }

    pub(crate) fn server(&self) -> &AppServer {
        &self.inner.server
    }

    pub(crate) fn driver(&self) -> &Arc<DriverSlot> {
        &self.inner.driver
    }

    fn writable_roots(&self) -> Vec<String> {
        self.inner
            .additional_directories
            .iter()
            .map(|p| p.to_string_lossy().into_owned())
            .collect()
    }

    fn apply_config_change(&self, id: &ConfigId, value: &ConfigValue) -> Result<()> {
        match id.as_str() {
            MODE_CONFIG_ID => {
                let raw = config_string_value(value)?;
                let parsed = AcpMode::parse(&raw)?;
                *self.inner.mode.lock().expect("session mode mutex poisoned") = parsed;
            }
            MODEL_CONFIG_ID => {
                let raw = config_string_value(value)?;
                let trimmed = raw.trim();
                if trimmed.is_empty() {
                    return Err(CodexAcpError::InvalidParams(
                        "`model` config value must not be empty".to_string(),
                    ));
                }
                self.set_model(Some(trimmed.to_string()));
            }
            EFFORT_CONFIG_ID => {
                let raw = config_string_value(value)?;
                let validated = validate_reasoning_effort(&raw)?;
                *self
                    .inner
                    .effort
                    .lock()
                    .expect("session effort mutex poisoned") = Some(validated);
            }
            FAST_MODE_CONFIG_ID => {
                let enabled = config_bool_value(value)?;
                self.inner.fast_mode.store(enabled, Ordering::Release);
            }
            other => {
                return Err(CodexAcpError::InvalidParams(format!(
                    "unknown config option `{other}`"
                )));
            }
        }
        Ok(())
    }

    /// Tear down the session: interrupt any running turn, unsubscribe on the
    /// app-server, and detach from the registry.
    pub(crate) async fn teardown(&self) {
        let active = self
            .inner
            .active_turn
            .lock()
            .expect("active_turn mutex poisoned")
            .take();
        if !self.inner.server.is_closed() {
            if let Some(turn_id) = active {
                let _ = self
                    .inner
                    .driver
                    .drive_until(self.inner.server.call::<_, Value>(
                        methods::TURN_INTERRUPT,
                        &json!({
                            "threadId": self.inner.thread_id,
                            "turnId": turn_id,
                        }),
                    ))
                    .await;
            }
            let _ = self
                .inner
                .driver
                .drive_until(self.inner.server.call::<_, Value>(
                    methods::THREAD_UNSUBSCRIBE,
                    &json!({"threadId": self.inner.thread_id}),
                ))
                .await;
        }
        self.inner.server.unsubscribe(&self.inner.thread_id);
        self.inner.registry.remove_session(self.inner.id.as_str());
    }
}

fn config_string_value(value: &ConfigValue) -> Result<String> {
    match value {
        ConfigValue::Id(v) => Ok(v.value.to_string()),
        ConfigValue::Boolean(b) => Ok(b.value.to_string()),
        ConfigValue::Custom(_, raw) | ConfigValue::Unknown(_, raw) => raw
            .get("value")
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| {
                CodexAcpError::InvalidParams("expected string config value".to_string())
            }),
    }
}

fn config_bool_value(value: &ConfigValue) -> Result<bool> {
    match value {
        ConfigValue::Boolean(b) => Ok(b.value),
        ConfigValue::Id(v) => match v.value.as_str() {
            "true" | "on" | "1" => Ok(true),
            "false" | "off" | "0" => Ok(false),
            other => Err(CodexAcpError::InvalidParams(format!(
                "fast-mode expects boolean value, got `{other}`"
            ))),
        },
        other => Err(CodexAcpError::InvalidParams(format!(
            "fast-mode expects boolean value, got {other:?}"
        ))),
    }
}

fn slash_command(prompt: &[ContentBlock]) -> Option<String> {
    if let [ContentBlock::Text(t)] = prompt {
        t.text.trim().strip_prefix('/').map(str::to_string)
    } else {
        None
    }
}

struct TurnStreams<'a> {
    turn: &'a Turn,
    agent: Option<MessageStream>,
    thought: Option<MessageStream>,
    streamed_agent_text: bool,
}

impl TurnStreams<'_> {
    async fn agent_text(&mut self, delta: &str) {
        if delta.is_empty() {
            return;
        }
        self.streamed_agent_text = true;
        if self.agent.is_none() {
            self.agent = Some(self.turn.message());
        }
        let _ = self.agent.as_ref().expect("agent stream").text(delta).await;
    }

    async fn thought_text(&mut self, delta: &str) {
        if delta.is_empty() {
            return;
        }
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

impl Session for CodexSession {
    fn config_options(&self) -> Vec<ConfigOption> {
        let models = self
            .inner
            .available_models
            .lock()
            .expect("available_models mutex poisoned")
            .clone();
        build_config_options(
            self.current_mode(),
            self.current_model().as_deref(),
            &models,
            self.current_effort().as_deref(),
            self.fast_mode(),
        )
    }

    fn available_commands(&self) -> Vec<AvailableCommand> {
        build_available_commands()
    }

    async fn set_config(
        &self,
        id: &ConfigId,
        value: ConfigValue,
        cx: &SessionContext,
    ) -> std::result::Result<Vec<ConfigOption>, RpcError> {
        self.apply_config_change(id, &value)
            .map_err(CodexAcpError::into_rpc_error)?;
        if id.as_str() == MODE_CONFIG_ID && cx.protocol_version().is_v1() {
            let _ = cx
                .update(SessionUpdate::CurrentModeUpdate(
                    aither_acp::domain::CurrentModeUpdate {
                        current_mode_id: aither_acp::domain::ConfigValueId::from_string(
                            self.current_mode().as_str(),
                        ),
                        meta: None,
                        extra: std::collections::BTreeMap::new(),
                    },
                ))
                .await;
        }
        Ok(self.config_options())
    }

    async fn prompt(
        &self,
        prompt: PendingPrompt,
        cx: &SessionContext,
    ) -> std::result::Result<StopReason, PromptError> {
        let content = prompt.content().to_vec();
        if let Err(err) = validate_prompt_blocks(&content) {
            let rpc = err.into_rpc_error();
            prompt.reject(rpc.clone()).await;
            return Err(PromptError::from(rpc));
        }

        let turn = prompt
            .accept()
            .await
            .map_err(|rejected| PromptError::from(rejected.into_inner()))?;

        if let Some(cmd) = slash_command(&content) {
            return Box::pin(self.run_command(&turn, cx, &cmd))
                .await
                .map_err(CodexAcpError::into_prompt_error);
        }

        let input = convert_prompt_blocks(&content).map_err(CodexAcpError::into_prompt_error)?;
        let mode = self.current_mode();
        let model = self.current_model();
        let effort = self.current_effort();
        let fast = self.fast_mode();
        let roots = self.writable_roots();

        let start_params = json!({
            "threadId": self.inner.thread_id,
            "input": input,
            "cwd": self.inner.cwd.to_string_lossy(),
            "model": model,
            "effort": effort,
            "serviceTierForTurn": fast.then_some("fast"),
            "approvalPolicy": mode.approval_policy_str(),
            "approvalsReviewer": mode.approvals_reviewer_str(),
            "sandboxPolicy": mode.sandbox_policy_json(&roots),
        });

        let resp: Value = self
            .inner
            .driver
            .drive_until(self.inner.server.call(methods::TURN_START, &start_params))
            .await
            .map_err(|e| CodexAcpError::Sdk(e).into_prompt_error())?;

        let turn_id = resp
            .get("turn")
            .and_then(|t| t.get("id"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();

        self.set_active_turn(Some(turn_id.clone()));
        let res = Box::pin(self.drive_turn(&turn, cx, &turn_id)).await;
        self.set_active_turn(None);
        res.map_err(CodexAcpError::into_prompt_error)
    }

    async fn close(&self) {
        self.teardown().await;
    }
}

fn validate_prompt_blocks(content: &[ContentBlock]) -> Result<()> {
    if slash_command(content).is_some() {
        return Ok(());
    }
    let _ = convert_prompt_blocks(content)?;
    Ok(())
}

impl CodexSession {
    async fn drive_turn(
        &self,
        turn: &Turn,
        cx: &SessionContext,
        turn_id: &str,
    ) -> Result<StopReason> {
        enum Next {
            Steered(Box<PendingPrompt>),
            Event(Option<Box<ThreadEvent>>),
            Cancelled,
        }

        let mut streams = TurnStreams {
            turn,
            agent: None,
            thought: None,
            streamed_agent_text: false,
        };

        self.inner
            .driver
            .drive_until(async {
                loop {
                    while let Some(steered) = turn.try_recv_steered() {
                        self.steer_queued(turn, cx, turn_id, steered).await;
                    }

                    let next = or(
                        std::future::poll_fn(|_| {
                            turn.try_recv_steered()
                                .map_or(std::task::Poll::Pending, |p| {
                                    std::task::Poll::Ready(Next::Steered(Box::new(p)))
                                })
                        }),
                        or(
                            async {
                                Next::Event(self.inner.events.recv().await.ok().map(Box::new))
                            },
                            async {
                                turn.cancelled().await;
                                Next::Cancelled
                            },
                        ),
                    )
                    .await;

                    match next {
                        Next::Steered(steered) => {
                            self.steer_queued(turn, cx, turn_id, *steered).await;
                        }
                        Next::Cancelled => {
                            let _ = self
                                .inner
                                .server
                                .call::<_, Value>(
                                    methods::TURN_INTERRUPT,
                                    &json!({
                                        "threadId": self.inner.thread_id,
                                        "turnId": turn_id,
                                    }),
                                )
                                .await;
                            return Ok(StopReason::Cancelled);
                        }
                        Next::Event(None) => {
                            if turn.is_cancelled() {
                                return Ok(StopReason::Cancelled);
                            }
                            if let Some(exit_err) = self.inner.server.terminal_error() {
                                return Err(CodexAcpError::Sdk(exit_err));
                            }
                            return Err(CodexAcpError::Sdk(SdkError::Closed));
                        }
                        Next::Event(Some(ev)) => {
                            if let Some(ev_turn) = ev.turn_id()
                                && !ev_turn.is_empty()
                                && ev_turn != turn_id
                            {
                                continue;
                            }
                            if let Some(stop) =
                                self.forward_event(&mut streams, cx, turn_id, *ev).await?
                            {
                                return Ok(stop);
                            }
                        }
                    }
                }
            })
            .await
    }

    async fn steer_queued(
        &self,
        turn: &Turn,
        cx: &SessionContext,
        turn_id: &str,
        steered: PendingPrompt,
    ) {
        let input = match convert_prompt_blocks(steered.content()) {
            Ok(inp) => inp,
            Err(err) => {
                steered.reject(err.into_rpc_error()).await;
                return;
            }
        };
        if let Err(err) = turn.merge_prompt(steered).await {
            debug!(%err, "steered prompt already resolved");
            return;
        }
        if let Err(err) = self
            .inner
            .server
            .call::<_, Value>(
                methods::TURN_STEER,
                &json!({
                    "threadId": self.inner.thread_id,
                    "expectedTurnId": turn_id,
                    "input": input,
                }),
            )
            .await
        {
            emit_notice(
                cx,
                NoticeSeverity::Warning,
                "Steering failed",
                Some(err.to_string()),
            )
            .await;
        }
    }

    async fn forward_event(
        &self,
        streams: &mut TurnStreams<'_>,
        cx: &SessionContext,
        turn_id: &str,
        ev: ThreadEvent,
    ) -> Result<Option<StopReason>> {
        match ev {
            ThreadEvent::Notification(n) => self.forward_typed(streams, cx, turn_id, n).await,
            ThreadEvent::RawNotification { method, params } => {
                self.forward_raw(streams, cx, turn_id, &method, &params)
                    .await
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn forward_typed(
        &self,
        streams: &mut TurnStreams<'_>,
        cx: &SessionContext,
        turn_id: &str,
        notification: Notification,
    ) -> Result<Option<StopReason>> {
        use Notification as N;
        match notification {
            N::AgentMessageDelta(d) => streams.agent_text(&d.delta).await,
            N::ReasoningDelta(d) => streams.thought_text(&d.delta).await,
            N::ReasoningTextDelta(d) => streams.thought_text(&d.delta).await,
            N::PlanDelta(d) => streams.agent_text(&d.delta).await,
            N::CmdOutputDelta(d) => {
                let _ = streams
                    .turn
                    .update(SessionUpdate::ToolCallContentChunk(output_chunk(
                        &d.item_id, &d.delta,
                    )))
                    .await;
            }
            N::FileChangeOutputDelta(d) => {
                let _ = streams
                    .turn
                    .update(SessionUpdate::ToolCallContentChunk(output_chunk(
                        &d.item_id, &d.delta,
                    )))
                    .await;
            }
            N::FileChangePatchUpdated(d) => {
                let changes = serde_json::to_value(&d.changes).unwrap_or_default();
                let slice = changes.as_array().map_or(&[][..], Vec::as_slice);
                let _ = streams
                    .turn
                    .update(SessionUpdate::ToolCallUpdate(patch_update(
                        &d.item_id, slice,
                    )))
                    .await;
            }
            N::ItemStarted(n) => {
                let v = serde_json::to_value(&n.item).unwrap_or_default();
                if let Some(u) = item_as_tool_call(&v, Some(ToolCallStatus::InProgress)) {
                    let _ = streams.turn.update(SessionUpdate::ToolCallUpdate(u)).await;
                }
            }
            N::ItemCompleted(n) => {
                let v = serde_json::to_value(&n.item).unwrap_or_default();
                self.emit_completed_item(streams, &v).await;
            }
            N::TurnPlanUpdated(plan) => {
                let v = serde_json::to_value(&plan).unwrap_or_default();
                let plan_id = v
                    .get("planId")
                    .or_else(|| v.get("id"))
                    .and_then(Value::as_str)
                    .map_or_else(|| PlanId::from_string("turn-plan"), PlanId::from_string);
                let _ = streams.turn.plan(plan_id, plan_entries(&v)).await;
            }
            N::ThreadTokenUsageUpdated(n) => {
                let v = serde_json::to_value(&n.token_usage).unwrap_or_default();
                let _ = cx.usage(usage_update(&v)).await;
            }
            N::Error(e) => {
                let v = serde_json::to_value(&e).unwrap_or_default();
                emit_notice_from_payload(cx, NoticeSeverity::Error, &v).await;
            }
            N::Warning(e) => {
                let v = serde_json::to_value(&e).unwrap_or_default();
                emit_notice_from_payload(cx, NoticeSeverity::Warning, &v).await;
            }
            N::ConfigWarning(e) => {
                let v = serde_json::to_value(&e).unwrap_or_default();
                emit_notice_from_payload(cx, NoticeSeverity::Warning, &v).await;
            }
            N::GuardianWarning(e) => {
                let v = serde_json::to_value(&e).unwrap_or_default();
                emit_notice_from_payload(cx, NoticeSeverity::Warning, &v).await;
            }
            N::StrictReviewRequired(n) => {
                self.handle_strict_review_required(cx, &n.turn_id).await;
            }
            N::ContextCompacted(n) => {
                let v = serde_json::to_value(&n).unwrap_or_default();
                emit_notice(
                    cx,
                    NoticeSeverity::Info,
                    "Context compacted",
                    v.get("turnId")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned),
                )
                .await;
            }
            N::TurnCompleted(n) => {
                let v = serde_json::to_value(&n.turn).unwrap_or_default();
                let completed_id = v.get("id").and_then(Value::as_str).unwrap_or_default();
                if !completed_id.is_empty() && completed_id != turn_id {
                    return Ok(None);
                }
                return Ok(Some(stop_reason(&v)));
            }
            N::Unknown { method, params } => {
                let p = params.unwrap_or(Value::Null);
                return self.forward_raw(streams, cx, turn_id, &method, &p).await;
            }
            _ => {}
        }
        Ok(None)
    }

    #[allow(clippy::too_many_lines)]
    async fn forward_raw(
        &self,
        streams: &mut TurnStreams<'_>,
        cx: &SessionContext,
        turn_id: &str,
        method: &str,
        params: &Value,
    ) -> Result<Option<StopReason>> {
        match method {
            "item/agentMessage/delta" => {
                if let Some(delta) = params.get("delta").and_then(Value::as_str) {
                    streams.agent_text(delta).await;
                }
            }
            "item/reasoning/summaryTextDelta" | "item/reasoning/textDelta" => {
                if let Some(delta) = params.get("delta").and_then(Value::as_str) {
                    streams.thought_text(delta).await;
                }
            }
            "item/commandExecution/outputDelta" | "item/fileChange/outputDelta" => {
                let id = params
                    .get("itemId")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let delta = params
                    .get("delta")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                if !id.is_empty() {
                    let _ = streams
                        .turn
                        .update(SessionUpdate::ToolCallContentChunk(output_chunk(id, delta)))
                        .await;
                }
            }
            "item/fileChange/patchUpdated" => {
                let id = params
                    .get("itemId")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let changes = params
                    .get("changes")
                    .and_then(Value::as_array)
                    .map_or(&[][..], Vec::as_slice);
                if !id.is_empty() {
                    let _ = streams
                        .turn
                        .update(SessionUpdate::ToolCallUpdate(patch_update(id, changes)))
                        .await;
                }
            }
            "item/started" => {
                if let Some(item) = params.get("item")
                    && let Some(u) = item_as_tool_call(item, Some(ToolCallStatus::InProgress))
                {
                    let _ = streams.turn.update(SessionUpdate::ToolCallUpdate(u)).await;
                }
            }
            "item/completed" => {
                if let Some(item) = params.get("item") {
                    self.emit_completed_item(streams, item).await;
                }
            }
            "turn/plan/updated" => {
                let plan_id = params
                    .get("planId")
                    .or_else(|| params.get("id"))
                    .and_then(Value::as_str)
                    .map_or_else(|| PlanId::from_string("turn-plan"), PlanId::from_string);
                let _ = streams.turn.plan(plan_id, plan_entries(params)).await;
            }
            "thread/tokenUsage/updated" => {
                let token_usage = params.get("tokenUsage").unwrap_or(params);
                let _ = cx.usage(usage_update(token_usage)).await;
            }
            "error" => emit_notice_from_payload(cx, NoticeSeverity::Error, params).await,
            "warning" | "configWarning" => {
                emit_notice_from_payload(cx, NoticeSeverity::Warning, params).await;
            }
            "autoApprovalReview/strictReviewRequired" => {
                let tid = params
                    .get("turnId")
                    .and_then(Value::as_str)
                    .unwrap_or(turn_id);
                self.handle_strict_review_required(cx, tid).await;
            }
            "thread/compacted" => {
                emit_notice(
                    cx,
                    NoticeSeverity::Info,
                    "Context compacted",
                    params
                        .get("turnId")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned),
                )
                .await;
            }
            "turn/completed" => {
                let turn = params.get("turn").unwrap_or(params);
                let completed_id = turn.get("id").and_then(Value::as_str).unwrap_or_default();
                if !completed_id.is_empty() && completed_id != turn_id {
                    return Ok(None);
                }
                return Ok(Some(stop_reason(turn)));
            }
            _ => {}
        }
        Ok(None)
    }

    async fn handle_strict_review_required(&self, cx: &SessionContext, turn_id: &str) {
        emit_notice(
            cx,
            NoticeSeverity::Warning,
            "Strict review required",
            Some("Auto-review escalated this turn to manual permission approval.".to_string()),
        )
        .await;
        if self.current_mode() == AcpMode::Agent {
            let mut call =
                ToolCallUpdate::new(ToolCallId::from_string(format!("strict-review-{turn_id}")));
            call.title = Patch::Set("Continue after strict review escalation?".to_string());
            call.kind = Patch::Set(Open::Known(ToolKind::Other));
            call.status = Patch::Set(Open::Known(ToolCallStatus::Pending));
            let resolution =
                prompt_session_permission(self, "strict_review", Some(turn_id.to_string()), call)
                    .await;
            if matches!(
                resolution,
                PermissionResolution::RejectOnce
                    | PermissionResolution::RejectAlways
                    | PermissionResolution::Cancelled
            ) {
                let _ = self
                    .inner
                    .server
                    .call::<_, Value>(
                        methods::TURN_INTERRUPT,
                        &json!({
                            "threadId": self.inner.thread_id,
                            "turnId": turn_id,
                        }),
                    )
                    .await;
            }
        }
    }

    async fn emit_completed_item(&self, streams: &mut TurnStreams<'_>, item: &Value) {
        let p = item.get("payload").unwrap_or(item);
        if p.get("type").and_then(Value::as_str) == Some("agentMessage") {
            if !streams.streamed_agent_text
                && let Some(text) = p
                    .get("text")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
            {
                streams.agent_text(text).await;
            }
            return;
        }
        if let Some(u) = item_completed_update(item) {
            let _ = streams.turn.update(SessionUpdate::ToolCallUpdate(u)).await;
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn run_command(&self, turn: &Turn, cx: &SessionContext, cmd: &str) -> Result<StopReason> {
        let (name, args) = cmd.split_once(' ').unwrap_or((cmd, ""));
        match name {
            "compact" => {
                let _: Value = self
                    .inner
                    .driver
                    .drive_until(self.inner.server.call(
                        methods::THREAD_COMPACT_START,
                        &json!({"threadId": self.inner.thread_id}),
                    ))
                    .await?;
                Ok(StopReason::EndTurn)
            }
            "logout" => {
                match self
                    .inner
                    .driver
                    .drive_until(self.inner.server.account_logout())
                    .await
                {
                    Ok(_) => {}
                    Err(SdkError::Remote(rem)) if rem.code == METHOD_NOT_FOUND_CODE => {}
                    Err(err) => return Err(CodexAcpError::Sdk(err)),
                }
                let status = self
                    .inner
                    .auth
                    .lock()
                    .expect("auth mutex poisoned")
                    .set_logged_out();
                notify_auth_status_cx(cx, &status).await?;
                Ok(StopReason::EndTurn)
            }
            "status" => {
                let account_detail = match self
                    .inner
                    .driver
                    .drive_until(self.inner.server.account_read(&GetAccountParams {
                        refresh_token: None,
                    }))
                    .await
                {
                    Ok(resp) => resp
                        .account
                        .and_then(|acc| serde_json::to_string(&acc).ok()),
                    Err(_) => None,
                };
                let report = self.format_status_report(account_detail.as_deref());
                let _ = turn.message().text(report).await;
                Ok(StopReason::EndTurn)
            }
            "review" | "review-branch" | "review-commit" => {
                let trimmed = args.trim();
                let target = match name {
                    "review-branch" => {
                        if trimmed.is_empty() {
                            return Err(CodexAcpError::InvalidParams(
                                "`/review-branch` requires a branch name".to_string(),
                            ));
                        }
                        json!({"type": "baseBranch", "branch": trimmed})
                    }
                    "review-commit" => {
                        if trimmed.is_empty() {
                            return Err(CodexAcpError::InvalidParams(
                                "`/review-commit` requires a commit SHA".to_string(),
                            ));
                        }
                        json!({"type": "commit", "sha": trimmed})
                    }
                    _ => json!({"type": "uncommittedChanges"}),
                };
                let resp: Value = self
                    .inner
                    .driver
                    .drive_until(self.inner.server.call(
                        methods::REVIEW_START,
                        &json!({
                            "threadId": self.inner.thread_id,
                            "target": target,
                        }),
                    ))
                    .await?;
                let turn_id = resp
                    .get("turn")
                    .and_then(|t| t.get("id"))
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                self.set_active_turn(Some(turn_id.clone()));
                let stop = Box::pin(self.drive_turn(turn, cx, &turn_id)).await;
                self.set_active_turn(None);
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

    fn format_status_report(&self, account_detail: Option<&str>) -> String {
        let mode = self.current_mode();
        let model = self
            .current_model()
            .unwrap_or_else(|| "default".to_string());
        let effort = self
            .current_effort()
            .unwrap_or_else(|| DEFAULT_REASONING_EFFORT.to_string());
        let fast = self.fast_mode();
        let auth_status = self
            .inner
            .auth
            .lock()
            .expect("auth mutex poisoned")
            .status();
        let auth_desc = account_detail.map_or_else(
            || {
                auth_status.label.as_deref().map_or_else(
                    || auth_status.kind.as_str().to_string(),
                    |lbl| format!("{} ({lbl})", auth_status.kind.as_str()),
                )
            },
            ToOwned::to_owned,
        );
        let mcp = if self.inner.mcp_server_names.is_empty() {
            "none".to_string()
        } else {
            self.inner.mcp_server_names.join(", ")
        };
        format!(
            "Session: {}\nDirectory: {}\nMode: {}\nModel: {}\nReasoning effort: {}\nFast mode: {}\nAccount: {}\nMCP servers: {}",
            self.inner.id,
            self.inner.cwd.display(),
            mode.as_str(),
            model,
            effort,
            fast,
            auth_desc,
            mcp,
        )
    }
}
