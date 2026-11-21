//! [`CodexAgent`] implementing [`aither_acp::server::SessionHost`] onto [`codex_sdk`].

use std::collections::BTreeMap;
use std::fmt;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use aither_acp::domain::patch::EmptyObject;
use aither_acp::domain::{
    AgentCapabilities, AuthMethod, ContentBlock, ExtMethod, McpCaps, McpServer, PromptCaps,
    SessionCaps, SessionId, SessionInfo, SessionListParams, SessionPage, SessionSetupParams,
};
use aither_acp::ext::steering::{METHOD as STEERING_METHOD, SteeringOutcome};
use aither_acp::rpc::{Peer, RpcError};
use aither_acp::server::{
    AuthConfig, AuthRequest, ReplaySink, ResumeRequest, Server, ServerError, SessionContext,
    SessionHost,
};
use aither_acp::vendor::codex::{
    AUTHENTICATION_LOGOUT_METHOD, AUTHENTICATION_STATUS_METHOD, SET_MODEL_METHOD, SessionModel,
};
use async_channel::{Receiver, Sender};
use codex_codes::{ModelListParams, UserInput, protocol::methods};
use codex_sdk::{AnyServerRequestHandler, AppServer, METHOD_NOT_FOUND_CODE, SdkError};
use serde_json::{Value, json};

use crate::auth::{
    AuthState, apply_capabilities, auth_methods, handle_auth_request, logout_with_server,
    read_legacy_authentication_status,
};
use crate::config::{AppServerConnector, CodexAcpConfig};
use crate::convert::{
    convert_prompt_blocks, extract_mcp_server_names, mcp_servers_to_config_map,
    replay_turns_to_updates, rfc3339_from_timestamp,
};
use crate::driver::DriverSlot;
use crate::error::{CodexAcpError, Result};
use crate::mode::{AcpMode, extract_model_entries};
use crate::permission::{BridgeServerRequestHandler, SessionRegistry};
use crate::session::CodexSession;

const LEGACY_STEERING_METHOD: &str = "_codex/session/steering";

struct SharedServer {
    server: AppServer,
    driver: Arc<DriverSlot>,
}

struct AgentInner {
    config: CodexAcpConfig,
    auth: Arc<Mutex<AuthState>>,
    registry: Arc<SessionRegistry>,
    peer: Mutex<Option<Peer>>,
    shared_server: async_lock::Mutex<Option<Arc<SharedServer>>>,
    pump_tx: Sender<Arc<DriverSlot>>,
    pump_rx: Receiver<Arc<DriverSlot>>,
}

/// ACP [`SessionHost`] bridging `codex app-server` via [`codex_sdk`] across
/// both ACP v1 and ACP v2 connections.
#[derive(Clone)]
pub struct CodexAgent {
    inner: Arc<AgentInner>,
}

impl fmt::Debug for CodexAgent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let count = self.inner.registry.len();
        f.debug_struct("CodexAgent")
            .field("agent_info", &self.inner.config.agent_info)
            .field("live_sessions", &count)
            .finish_non_exhaustive()
    }
}

impl CodexAgent {
    /// Construct a new [`CodexAgent`] from `config`.
    #[must_use]
    pub fn new(config: CodexAcpConfig) -> Self {
        let auth = AuthState::from_spawn(&config.spawn);
        let (pump_tx, pump_rx) = async_channel::unbounded();
        Self {
            inner: Arc::new(AgentInner {
                config,
                auth: Arc::new(Mutex::new(auth)),
                registry: Arc::new(SessionRegistry::new()),
                peer: Mutex::new(None),
                shared_server: async_lock::Mutex::new(None),
                pump_tx,
                pump_rx,
            }),
        }
    }

    /// Attach the connection-level [`Peer`] used for `_auth/status_update`
    /// notifications.
    pub(crate) fn set_peer(&self, peer: Peer) {
        *self.inner.peer.lock().expect("peer mutex poisoned") = Some(peer);
    }

    fn peer(&self) -> Option<Peer> {
        self.inner.peer.lock().expect("peer mutex poisoned").clone()
    }

    /// Advertised authentication methods (`chat-gpt`, `chat-gpt-device-code`,
    /// `api-key`, `gateway`).
    #[must_use]
    pub fn auth_methods(&self) -> Vec<AuthMethod> {
        auth_methods()
    }

    /// Background pump polled alongside `Server::serve` inside [`crate::serve`]
    /// so server-initiated requests arriving between prompts are answered
    /// without head-of-line delay.
    pub(crate) async fn drive_background(&self) {
        while let Ok(slot) = self.inner.pump_rx.recv().await {
            slot.drive_to_completion().await;
        }
        std::future::pending::<()>().await;
    }

    /// Shut down all active sessions and close the underlying `AppServer`.
    pub async fn shutdown_all(&self) {
        self.inner.pump_tx.close();
        let drained = self.inner.registry.drain_all();
        for session in &drained {
            session.teardown().await;
        }
        let shared_opt = self.inner.shared_server.lock().await.take();
        if let Some(shared) = shared_opt {
            shared.server.close();
            shared.driver.drive_to_completion().await;
        }
    }

    /// Build a configured [`aither_acp::Server`] wrapping this host with its
    /// auth, `_`-extension, and legacy method handlers.
    #[must_use]
    pub fn into_server(self) -> Server {
        let info = self.inner.config.agent_info.clone();
        let auth_agent = self.clone();
        let ext_agent = self.clone();
        let unknown_agent = self.clone();

        Server::builder(self.clone(), info)
            .auth(AuthConfig::new(self.auth_methods(), move |req| {
                let agent = auth_agent.clone();
                async move {
                    agent
                        .handle_auth(req)
                        .await
                        .map_err(CodexAcpError::into_rpc_error)
                }
            }))
            .ext(move |method, params| {
                let agent = ext_agent.clone();
                async move {
                    agent
                        .handle_ext(&method, params)
                        .await
                        .map_err(CodexAcpError::into_rpc_error)
                }
            })
            .unknown_handler(Arc::new(move |method: &str, params: Option<Value>| {
                let agent = unknown_agent.clone();
                let method = method.to_string();
                Box::pin(async move {
                    agent
                        .handle_unknown(&method, params)
                        .await
                        .map_err(CodexAcpError::into_rpc_error)
                })
                    as std::pin::Pin<
                        Box<
                            dyn std::future::Future<Output = std::result::Result<Value, RpcError>>
                                + Send,
                        >,
                    >
            }))
            .build()
    }

    /// Handle `auth/login` or `auth/logout` (`authenticate`/`logout` on v1).
    ///
    /// # Errors
    ///
    /// Propagates credential validation or app-server RPC errors.
    pub async fn handle_auth(&self, request: AuthRequest) -> Result<()> {
        let shared = self.ensure_server().await?;
        let peer = self.peer();
        shared
            .driver
            .drive_until(handle_auth_request(
                &shared.server,
                &self.inner.auth,
                &self.inner.config.spawn,
                peer.as_ref(),
                request,
            ))
            .await
    }

    /// Handle `_`-prefixed extension methods (`_session/steering` and
    /// `_codex/session/steering`).
    ///
    /// # Errors
    ///
    /// Returns [`CodexAcpError::InvalidParams`] for malformed steering payloads,
    /// [`CodexAcpError::SessionNotFound`] for unknown sessions, or method-not-found
    /// for unrecognized extension methods.
    pub async fn handle_ext(&self, method: &ExtMethod, params: Option<Value>) -> Result<Value> {
        if method.as_str() == STEERING_METHOD.as_str() || method.as_str() == LEGACY_STEERING_METHOD
        {
            let params = params.ok_or_else(|| {
                CodexAcpError::InvalidParams("missing steering params".to_string())
            })?;
            let session_id = params
                .get("sessionId")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| CodexAcpError::InvalidParams("missing `sessionId`".to_string()))?;
            let session = self
                .inner
                .registry
                .get_by_session(session_id)
                .ok_or_else(|| CodexAcpError::SessionNotFound(session_id.to_string()))?;
            let input = parse_steering_input(&params)?;
            let Some(turn_id) = session.active_turn_id() else {
                return Ok(json!({
                    "outcome": SteeringOutcome::Failed.as_str(),
                }));
            };
            let resp: Value = session
                .driver()
                .drive_until(session.server().call(
                    methods::TURN_STEER,
                    &json!({
                        "threadId": session.thread_id(),
                        "expectedTurnId": turn_id,
                        "input": input,
                    }),
                ))
                .await?;
            return Ok(json!({
                "outcome": SteeringOutcome::Injected.as_str(),
                "turnId": resp.get("turnId").cloned().unwrap_or_else(|| json!(turn_id)),
            }));
        }
        Err(CodexAcpError::Server(ServerError::Rpc(
            RpcError::method_not_found(method.as_str()),
        )))
    }

    /// Handle legacy non-`_` Codex RPCs (`session/set_model`,
    /// `authentication/status`, `authentication/logout`).
    ///
    /// # Errors
    ///
    /// Propagates parameter validation, session lookup, or app-server RPC errors.
    pub async fn handle_unknown(&self, method: &str, params: Option<Value>) -> Result<Value> {
        match method {
            SET_MODEL_METHOD => {
                let params = params.ok_or_else(|| {
                    CodexAcpError::InvalidParams("missing `session/set_model` params".to_string())
                })?;
                let session_id = params
                    .get("sessionId")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| {
                        CodexAcpError::InvalidParams("missing `sessionId`".to_string())
                    })?;
                let model = params
                    .get("modelId")
                    .or_else(|| params.get("model"))
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| CodexAcpError::InvalidParams("missing `modelId`".to_string()))?;
                let session = self
                    .inner
                    .registry
                    .get_by_session(session_id)
                    .ok_or_else(|| CodexAcpError::SessionNotFound(session_id.to_string()))?;
                session.set_model(Some(model.to_string()));
                Ok(json!({}))
            }
            AUTHENTICATION_STATUS_METHOD => {
                let shared = self.ensure_server().await?;
                let status = shared
                    .driver
                    .drive_until(read_legacy_authentication_status(
                        &shared.server,
                        &self.inner.auth,
                    ))
                    .await?;
                Ok(serde_json::to_value(status).unwrap_or_else(|_| json!({})))
            }
            AUTHENTICATION_LOGOUT_METHOD => {
                let shared = self.ensure_server().await?;
                let peer = self.peer();
                shared
                    .driver
                    .drive_until(logout_with_server(
                        &shared.server,
                        &self.inner.auth,
                        peer.as_ref(),
                    ))
                    .await?;
                Ok(json!({}))
            }
            _ => Err(CodexAcpError::Server(ServerError::Rpc(
                RpcError::method_not_found(method),
            ))),
        }
    }

    fn resolve_initial_mode(&self) -> Result<AcpMode> {
        if let Some(mode_id) = &self.inner.config.default_mode {
            return AcpMode::parse(mode_id);
        }
        Ok(AcpMode::default())
    }

    async fn ensure_server(&self) -> Result<Arc<SharedServer>> {
        let mut guard = self.inner.shared_server.lock().await;
        if let Some(existing) = guard.as_ref()
            && !existing.server.is_closed()
        {
            return Ok(Arc::clone(existing));
        }

        let handler = AnyServerRequestHandler::new(BridgeServerRequestHandler::new(Arc::clone(
            &self.inner.registry,
        )));
        let spawn_cfg = self.inner.config.spawn.clone();
        let connected = self
            .inner
            .config
            .connector
            .connect(spawn_cfg.clone(), handler)
            .await?;

        let driver = Arc::new(DriverSlot::new(connected.driver));
        let server = connected.server;
        let _ = self.inner.pump_tx.try_send(Arc::clone(&driver));

        driver.drive_until(server.handshake(&spawn_cfg)).await?;

        let shared = Arc::new(SharedServer { server, driver });
        *guard = Some(Arc::clone(&shared));
        drop(guard);
        Ok(shared)
    }

    fn build_thread_config(&self, mcp_servers: &[McpServer]) -> Value {
        let mut map = mcp_servers_to_config_map(mcp_servers);
        self.inner
            .auth
            .lock()
            .expect("auth mutex poisoned")
            .apply_gateway_to_config(&mut map);
        Value::Object(map)
    }

    async fn fetch_available_models(shared: &SharedServer) -> Vec<SessionModel> {
        match shared
            .driver
            .drive_until(
                shared
                    .server
                    .call::<_, Value>(methods::MODEL_LIST, &ModelListParams::default()),
            )
            .await
        {
            Ok(resp) => resp
                .get("data")
                .and_then(Value::as_array)
                .map(|arr| extract_model_entries(arr))
                .unwrap_or_default(),
            Err(SdkError::Remote(rem)) if rem.code == METHOD_NOT_FOUND_CODE => Vec::new(),
            Err(_) => Vec::new(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn attach_session(
        &self,
        shared: &Arc<SharedServer>,
        cx: &SessionContext,
        session_id: SessionId,
        thread_id: String,
        cwd: PathBuf,
        additional_directories: Vec<PathBuf>,
        mcp_servers: &[McpServer],
        initial_mode: AcpMode,
        initial_model: Option<String>,
        initial_effort: Option<String>,
    ) -> CodexSession {
        let events = shared.server.subscribe(&thread_id);
        let available_models = Self::fetch_available_models(shared).await;
        let mcp_names = extract_mcp_server_names(mcp_servers);
        let session = CodexSession::new(
            session_id,
            thread_id,
            cwd,
            additional_directories,
            mcp_names,
            cx.clone(),
            events,
            shared.server.clone(),
            Arc::clone(&shared.driver),
            Arc::clone(&self.inner.auth),
            Arc::clone(&self.inner.registry),
            initial_mode,
            initial_model,
            initial_effort,
            available_models,
        );
        if let Some(prev) = self.inner.registry.insert(session.clone()) {
            prev.teardown().await;
        }
        session
    }

    async fn replay_thread(
        shared: &SharedServer,
        thread_id: &str,
        replay: &ReplaySink,
    ) -> Result<()> {
        let resp: Value = shared
            .driver
            .drive_until(shared.server.call(
                methods::THREAD_READ,
                &json!({"threadId": thread_id, "includeTurns": true}),
            ))
            .await?;
        if let Some(turns) = resp
            .get("thread")
            .and_then(|t| t.get("turns"))
            .and_then(Value::as_array)
        {
            for update in replay_turns_to_updates(turns) {
                let _ = replay.send(update);
            }
        }
        Ok(())
    }
}

fn parse_steering_input(params: &Value) -> Result<Vec<UserInput>> {
    match params.get("prompt").or_else(|| params.get("message")) {
        Some(Value::String(s)) => {
            if s.is_empty() {
                return Err(CodexAcpError::InvalidParams(
                    "steering message must not be empty".to_string(),
                ));
            }
            Ok(vec![UserInput::Text {
                text: s.clone(),
                text_elements: None,
            }])
        }
        Some(Value::Array(arr)) => {
            let blocks: Vec<ContentBlock> = serde_json::from_value(Value::Array(arr.clone()))
                .map_err(|e| CodexAcpError::InvalidParams(e.to_string()))?;
            convert_prompt_blocks(&blocks)
        }
        _ => Err(CodexAcpError::InvalidParams(
            "steering request requires `prompt` or `message`".to_string(),
        )),
    }
}

fn extract_thread_model_and_effort(resp: &Value) -> (Option<String>, Option<String>) {
    let model = resp
        .get("model")
        .or_else(|| resp.get("thread").and_then(|t| t.get("model")))
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(ToOwned::to_owned);
    let effort = resp
        .get("reasoningEffort")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(ToOwned::to_owned);
    (model, effort)
}

impl SessionHost for CodexAgent {
    type Session = CodexSession;

    fn capabilities(&self) -> AgentCapabilities {
        let mut caps = AgentCapabilities {
            session: Some(SessionCaps {
                prompt: Some(PromptCaps {
                    image: Some(EmptyObject),
                    audio: Some(EmptyObject),
                    embedded_context: Some(EmptyObject),
                    meta: None,
                    extra: BTreeMap::new(),
                }),
                mcp: Some(McpCaps {
                    stdio: Some(EmptyObject),
                    http: Some(EmptyObject),
                    acp: None,
                    meta: None,
                    extra: BTreeMap::from([("sse".to_string(), json!({}))]),
                }),
                delete: Some(EmptyObject),
                fork: Some(EmptyObject),
                options: Some(EmptyObject),
                additional_directories: Some(EmptyObject),
                system_prompt: None,
                archive: None,
                status: None,
                title: None,
                meta: None,
                extra: BTreeMap::from([
                    ("list".to_string(), json!({})),
                    ("resume".to_string(), json!({})),
                    ("close".to_string(), json!({})),
                    ("loadSession".to_string(), json!(true)),
                ]),
            }),
            auth: None,
            providers: None,
            meta: None,
            extra: BTreeMap::new(),
        };
        apply_capabilities(&mut caps);
        caps
    }

    async fn start(&self) -> std::result::Result<(), ServerError> {
        self.ensure_server()
            .await
            .map(|_| ())
            .map_err(CodexAcpError::into_server_error)
    }

    async fn shutdown(&self) {
        self.shutdown_all().await;
    }

    async fn new_session(
        &self,
        params: SessionSetupParams,
        cx: &SessionContext,
    ) -> std::result::Result<Self::Session, ServerError> {
        let run = async {
            let initial_mode = self.resolve_initial_mode()?;
            let shared = self.ensure_server().await?;
            let roots: Vec<String> = params
                .additional_directories
                .iter()
                .map(|p| p.to_string_lossy().into_owned())
                .collect();
            let thread_cfg = self.build_thread_config(&params.mcp_servers);

            let resp: Value = shared
                .driver
                .drive_until(shared.server.call(
                    methods::THREAD_START,
                    &json!({
                        "cwd": params.cwd.to_string_lossy(),
                        "approvalPolicy": initial_mode.approval_policy_str(),
                        "approvalsReviewer": initial_mode.approvals_reviewer_str(),
                        "sandbox": initial_mode.sandbox_mode_str(),
                        "sandboxPolicy": initial_mode.sandbox_policy_json(&roots),
                        "config": thread_cfg,
                    }),
                ))
                .await?;

            let thread_id = resp
                .get("thread")
                .and_then(|t| t.get("id"))
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| {
                    CodexAcpError::Sdk(SdkError::Protocol(
                        "thread/start response missing `thread.id`".to_string(),
                    ))
                })?
                .to_string();
            let (model, effort) = extract_thread_model_and_effort(&resp);

            Ok(self
                .attach_session(
                    &shared,
                    cx,
                    cx.session_id().clone(),
                    thread_id,
                    params.cwd,
                    params.additional_directories,
                    &params.mcp_servers,
                    initial_mode,
                    model,
                    effort,
                )
                .await)
        };
        run.await.map_err(CodexAcpError::into_server_error)
    }

    async fn resume_session(
        &self,
        req: ResumeRequest,
        cx: &SessionContext,
    ) -> std::result::Result<Self::Session, ServerError> {
        let run = async {
            let initial_mode = self.resolve_initial_mode()?;
            let shared = self.ensure_server().await?;
            let target_id = self
                .inner
                .registry
                .resolve_thread_id(req.params.session_id.as_str());
            let thread_cfg = self.build_thread_config(&req.params.setup.mcp_servers);

            let resp: Value = shared
                .driver
                .drive_until(shared.server.call(
                    methods::THREAD_RESUME,
                    &json!({
                        "threadId": target_id,
                        "cwd": req.params.setup.cwd.to_string_lossy(),
                        "approvalPolicy": initial_mode.approval_policy_str(),
                        "approvalsReviewer": initial_mode.approvals_reviewer_str(),
                        "sandbox": initial_mode.sandbox_mode_str(),
                        "config": thread_cfg,
                    }),
                ))
                .await?;

            let thread_id = resp
                .get("thread")
                .and_then(|t| t.get("id"))
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .unwrap_or(&target_id)
                .to_string();
            let (model, effort) = extract_thread_model_and_effort(&resp);

            // Replay history when requested (`session/load` or explicit `replayFrom`),
            // or from `resp.thread.turns` if already returned inline.
            if req.params.replay_from.is_some() {
                if let Some(inline_turns) = resp
                    .get("thread")
                    .and_then(|t| t.get("turns"))
                    .and_then(Value::as_array)
                    .filter(|arr| !arr.is_empty())
                {
                    for update in replay_turns_to_updates(inline_turns) {
                        let _ = req.replay.send(update);
                    }
                } else {
                    let _ = Self::replay_thread(&shared, &thread_id, &req.replay).await;
                }
            }

            Ok(self
                .attach_session(
                    &shared,
                    cx,
                    cx.session_id().clone(),
                    thread_id,
                    req.params.setup.cwd,
                    req.params.setup.additional_directories,
                    &req.params.setup.mcp_servers,
                    initial_mode,
                    model,
                    effort,
                )
                .await)
        };
        run.await.map_err(CodexAcpError::into_server_error)
    }

    async fn fork_session(
        &self,
        req: ResumeRequest,
        cx: &SessionContext,
    ) -> std::result::Result<Self::Session, ServerError> {
        let run = async {
            let initial_mode = self.resolve_initial_mode()?;
            let shared = self.ensure_server().await?;
            let source_thread = self
                .inner
                .registry
                .resolve_thread_id(req.params.session_id.as_str());
            let thread_cfg = self.build_thread_config(&req.params.setup.mcp_servers);

            let resp: Value = shared
                .driver
                .drive_until(shared.server.call(
                    methods::THREAD_FORK,
                    &json!({
                        "threadId": source_thread,
                        "cwd": req.params.setup.cwd.to_string_lossy(),
                        "approvalPolicy": initial_mode.approval_policy_str(),
                        "approvalsReviewer": initial_mode.approvals_reviewer_str(),
                        "sandbox": initial_mode.sandbox_mode_str(),
                        "config": thread_cfg,
                    }),
                ))
                .await?;

            let forked_id = resp
                .get("thread")
                .and_then(|t| t.get("id"))
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| {
                    CodexAcpError::Sdk(SdkError::Protocol(
                        "thread/fork response missing `thread.id`".to_string(),
                    ))
                })?
                .to_string();
            let (model, effort) = extract_thread_model_and_effort(&resp);

            if let Some(inline_turns) = resp
                .get("thread")
                .and_then(|t| t.get("turns"))
                .and_then(Value::as_array)
                .filter(|arr| !arr.is_empty())
            {
                for update in replay_turns_to_updates(inline_turns) {
                    let _ = req.replay.send(update);
                }
            } else {
                let _ = Self::replay_thread(&shared, &forked_id, &req.replay).await;
            }

            Ok(self
                .attach_session(
                    &shared,
                    cx,
                    cx.session_id().clone(),
                    forked_id,
                    req.params.setup.cwd,
                    req.params.setup.additional_directories,
                    &req.params.setup.mcp_servers,
                    initial_mode,
                    model,
                    effort,
                )
                .await)
        };
        run.await.map_err(CodexAcpError::into_server_error)
    }

    async fn list_sessions(
        &self,
        params: SessionListParams,
    ) -> std::result::Result<SessionPage, RpcError> {
        let run = async {
            let shared = self.ensure_server().await?;
            let resp = shared
                .driver
                .drive_until(shared.server.call::<_, codex_codes::ThreadListResponse>(
                    methods::THREAD_LIST,
                    &json!({
                        "cwd": params.cwd.as_ref().map(|p| p.to_string_lossy()),
                        "cursor": params.cursor,
                        "archived": false,
                    }),
                ))
                .await?;
            Ok(SessionPage {
                sessions: resp
                    .data
                    .iter()
                    .map(|t| SessionInfo {
                        session_id: SessionId::from_string(&t.id),
                        cwd: PathBuf::from(t.cwd.as_str().unwrap_or_default()),
                        title: t
                            .name
                            .clone()
                            .or_else(|| (!t.preview.is_empty()).then(|| t.preview.clone())),
                        updated_at: Some(rfc3339_from_timestamp(t.updated_at)),
                        meta: None,
                        extra: BTreeMap::new(),
                    })
                    .collect(),
                next_cursor: resp.next_cursor,
                meta: None,
                extra: BTreeMap::new(),
            })
        };
        run.await.map_err(CodexAcpError::into_rpc_error)
    }

    async fn delete_session(&self, session_id: &SessionId) -> std::result::Result<(), RpcError> {
        let run = async {
            let shared = self.ensure_server().await?;
            let thread_id =
                if let Some(live) = self.inner.registry.remove_session(session_id.as_str()) {
                    let tid = live.thread_id().to_string();
                    live.teardown().await;
                    tid
                } else {
                    self.inner.registry.resolve_thread_id(session_id.as_str())
                };

            shared
                .driver
                .drive_until(
                    shared
                        .server
                        .call::<_, Value>(methods::THREAD_DELETE, &json!({"threadId": thread_id})),
                )
                .await?;
            shared.server.unsubscribe(&thread_id);
            Ok(())
        };
        run.await.map_err(CodexAcpError::into_rpc_error)
    }
}
