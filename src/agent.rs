//! [`CodexAgent`] implementing [`aither_acp::server::SessionHost`] onto [`codex_sdk`].

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
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
use codex_codes::{GetAccountRateLimitsParams, ModelListParams, UserInput, protocol::methods};
use codex_sdk::{AnyServerRequestHandler, AppServer, METHOD_NOT_FOUND_CODE, SdkError};
use serde_json::{Map, Value, json};

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
use crate::mode::{AcpMode, CollaborationMode, extract_model_entries};
use crate::permission::{BridgeServerRequestHandler, SessionRegistry};
use crate::session::CodexSession;

const LEGACY_STEERING_METHOD: &str = "_codex/session/steering";

/// `_`-prefixed ACP extension forwarding `codex app-server`'s
/// `mcpServerStatus/list`.
pub const MCP_STATUS_METHOD: &str = "_codex/mcp_status";
/// `_`-prefixed ACP extension forwarding `codex app-server`'s
/// `account/rateLimits/read`.
pub const RATE_LIMITS_METHOD: &str = "_codex/rate_limits";
/// `_`-prefixed ACP extension forwarding `codex app-server`'s
/// `thread/compact/start`.
pub const COMPACT_METHOD: &str = "_codex/compact";

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

    /// Drive the shared `AppServer` I/O loop in the background so
    /// server-initiated requests (approvals, user-input prompts, MCP
    /// elicitations) arriving between turns are serviced without head-of-line
    /// delay.
    ///
    /// [`crate::serve`] polls this automatically alongside [`Server::serve`].
    /// Hosts embedding [`CodexAgent`] in-process (for example via
    /// [`Self::into_server`] or `aither_acp::Client::builder(...).in_process`)
    /// should spawn or race the returned future alongside the ACP connection;
    /// it holds no strong reference to the agent and resolves cleanly once
    /// [`Self::shutdown_all`] (or [`SessionHost::shutdown`]) finishes or all
    /// [`CodexAgent`] handles are dropped.
    pub fn drive_background(&self) -> impl Future<Output = ()> + Send + 'static + use<> {
        let pump_rx = self.inner.pump_rx.clone();
        async move {
            while let Ok(slot) = pump_rx.recv().await {
                slot.drive_to_completion().await;
            }
        }
    }

    /// Shut down all active sessions and close the underlying `AppServer`.
    pub async fn shutdown_all(&self) {
        let drained = self.inner.registry.drain_all();
        for session in &drained {
            session.teardown().await;
        }
        let shared_opt = self.inner.shared_server.lock().await.take();
        if let Some(shared) = shared_opt {
            shared.server.close();
            shared.driver.drive_to_completion().await;
        }
        self.inner.pump_tx.close();
    }

    /// Build a configured [`aither_acp::Server`] wrapping this host with its
    /// auth, `_`-extension, and legacy method handlers.
    ///
    /// When embedding the returned [`Server`] directly without [`crate::serve`],
    /// obtain [`Self::drive_background`] before consuming `self` and poll or
    /// spawn it alongside the ACP connection so server-initiated requests
    /// arriving between prompts keep progressing.
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

    /// Handle `_`-prefixed extension methods (`_session/steering`,
    /// `_codex/session/steering`, `_codex/mcp_status`, `_codex/rate_limits`,
    /// and `_codex/compact`).
    ///
    /// # Errors
    ///
    /// Returns [`CodexAcpError::InvalidParams`] for malformed payloads,
    /// [`CodexAcpError::SessionNotFound`] for unknown sessions,
    /// [`CodexAcpError::Sdk`] for app-server RPC failures, or method-not-found
    /// for unrecognized extension methods.
    pub async fn handle_ext(&self, method: &ExtMethod, params: Option<Value>) -> Result<Value> {
        match method.as_str() {
            m if m == STEERING_METHOD.as_str() || m == LEGACY_STEERING_METHOD => {
                self.handle_steering(params).await
            }
            MCP_STATUS_METHOD => self.handle_mcp_status(params).await,
            RATE_LIMITS_METHOD => self.handle_rate_limits(params).await,
            COMPACT_METHOD => self.handle_compact(params).await,
            other => Err(CodexAcpError::Server(ServerError::Rpc(
                RpcError::method_not_found(other),
            ))),
        }
    }

    async fn handle_steering(&self, params: Option<Value>) -> Result<Value> {
        let params = params
            .ok_or_else(|| CodexAcpError::InvalidParams("missing steering params".to_string()))?;
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
        Ok(json!({
            "outcome": SteeringOutcome::Injected.as_str(),
            "turnId": resp.get("turnId").cloned().unwrap_or_else(|| json!(turn_id)),
        }))
    }

    async fn handle_mcp_status(&self, params: Option<Value>) -> Result<Value> {
        let mut wire = Map::new();
        match params {
            None | Some(Value::Null) => {}
            Some(Value::Object(map)) => {
                if let Some(val) = map.get("cursor").filter(|v| !v.is_null()) {
                    let cursor = val.as_str().ok_or_else(|| {
                        CodexAcpError::InvalidParams("`cursor` must be a string".to_string())
                    })?;
                    wire.insert("cursor".to_string(), Value::String(cursor.to_string()));
                }
                if let Some(val) = map.get("limit").filter(|v| !v.is_null()) {
                    let raw = val.as_u64().ok_or_else(|| {
                        CodexAcpError::InvalidParams(
                            "`limit` must be a non-negative integer".to_string(),
                        )
                    })?;
                    let limit = u32::try_from(raw).map_err(|_| {
                        CodexAcpError::InvalidParams(
                            "`limit` must fit within a 32-bit unsigned integer".to_string(),
                        )
                    })?;
                    wire.insert("limit".to_string(), json!(limit));
                }
                if let Some(val) = map.get("detail").filter(|v| !v.is_null()) {
                    let detail = val.as_str().ok_or_else(|| {
                        CodexAcpError::InvalidParams("`detail` must be a string".to_string())
                    })?;
                    wire.insert("detail".to_string(), Value::String(detail.to_string()));
                }
                if let Some(val) = map.get("serverName").filter(|v| !v.is_null()) {
                    let name = val.as_str().filter(|s| !s.is_empty()).ok_or_else(|| {
                        CodexAcpError::InvalidParams(
                            "`serverName` must be a non-empty string".to_string(),
                        )
                    })?;
                    wire.insert("serverName".to_string(), Value::String(name.to_string()));
                }
                if let Some(val) = map.get("environmentId").filter(|v| !v.is_null()) {
                    let env_id = val.as_str().filter(|s| !s.is_empty()).ok_or_else(|| {
                        CodexAcpError::InvalidParams(
                            "`environmentId` must be a non-empty string".to_string(),
                        )
                    })?;
                    wire.insert(
                        "environmentId".to_string(),
                        Value::String(env_id.to_string()),
                    );
                }

                let session_thread =
                    if let Some(val) = map.get("sessionId").filter(|v| !v.is_null()) {
                        let sid = val.as_str().filter(|s| !s.is_empty()).ok_or_else(|| {
                            CodexAcpError::InvalidParams(
                                "`sessionId` must be a non-empty string".to_string(),
                            )
                        })?;
                        let session = self
                            .inner
                            .registry
                            .get_by_session(sid)
                            .ok_or_else(|| CodexAcpError::SessionNotFound(sid.to_string()))?;
                        Some(session.thread_id().to_string())
                    } else {
                        None
                    };
                let resolved_thread =
                    if let Some(val) = map.get("threadId").filter(|v| !v.is_null()) {
                        let tid = val.as_str().filter(|s| !s.is_empty()).ok_or_else(|| {
                            CodexAcpError::InvalidParams(
                                "`threadId` must be a non-empty string".to_string(),
                            )
                        })?;
                        Some(self.inner.registry.resolve_thread_id(tid))
                    } else {
                        session_thread
                    };
                if let Some(tid) = resolved_thread {
                    wire.insert("threadId".to_string(), Value::String(tid));
                }
            }
            Some(_) => {
                return Err(CodexAcpError::InvalidParams(format!(
                    "`{MCP_STATUS_METHOD}` params must be a JSON object"
                )));
            }
        }

        let shared = self.ensure_server().await?;
        shared
            .driver
            .drive_until(
                shared
                    .server
                    .call::<_, Value>(methods::MCPSERVERSTATUS_LIST, &Value::Object(wire)),
            )
            .await
            .map_err(CodexAcpError::from)
    }

    async fn handle_rate_limits(&self, params: Option<Value>) -> Result<Value> {
        let wire_params = match params {
            None | Some(Value::Null) => GetAccountRateLimitsParams::default(),
            Some(val @ Value::Object(_)) => {
                serde_json::from_value::<GetAccountRateLimitsParams>(val).map_err(|err| {
                    CodexAcpError::InvalidParams(format!(
                        "invalid `{RATE_LIMITS_METHOD}` params: {err}"
                    ))
                })?
            }
            Some(_) => {
                return Err(CodexAcpError::InvalidParams(format!(
                    "`{RATE_LIMITS_METHOD}` params must be a JSON object"
                )));
            }
        };

        let shared = self.ensure_server().await?;
        shared
            .driver
            .drive_until(
                shared
                    .server
                    .call::<_, Value>(methods::ACCOUNT_RATELIMITS_READ, &wire_params),
            )
            .await
            .map_err(CodexAcpError::from)
    }

    async fn handle_compact(&self, params: Option<Value>) -> Result<Value> {
        let map = match params {
            Some(Value::Object(map)) => map,
            None | Some(Value::Null) => {
                return Err(CodexAcpError::InvalidParams(format!(
                    "missing `{COMPACT_METHOD}` params"
                )));
            }
            Some(_) => {
                return Err(CodexAcpError::InvalidParams(format!(
                    "`{COMPACT_METHOD}` params must be a JSON object"
                )));
            }
        };

        if let Some(val) = map.get("sessionId").filter(|v| !v.is_null()) {
            let session_id = val.as_str().filter(|s| !s.is_empty()).ok_or_else(|| {
                CodexAcpError::InvalidParams("`sessionId` must be a non-empty string".to_string())
            })?;
            let session = self
                .inner
                .registry
                .get_by_session(session_id)
                .ok_or_else(|| CodexAcpError::SessionNotFound(session_id.to_string()))?;
            return session
                .driver()
                .drive_until(session.server().call::<_, Value>(
                    methods::THREAD_COMPACT_START,
                    &json!({"threadId": session.thread_id()}),
                ))
                .await
                .map_err(CodexAcpError::from);
        }

        if let Some(val) = map.get("threadId").filter(|v| !v.is_null()) {
            let raw_tid = val.as_str().filter(|s| !s.is_empty()).ok_or_else(|| {
                CodexAcpError::InvalidParams("`threadId` must be a non-empty string".to_string())
            })?;
            let thread_id = self.inner.registry.resolve_thread_id(raw_tid);
            let shared = self.ensure_server().await?;
            return shared
                .driver
                .drive_until(shared.server.call::<_, Value>(
                    methods::THREAD_COMPACT_START,
                    &json!({"threadId": thread_id}),
                ))
                .await
                .map_err(CodexAcpError::from);
        }

        Err(CodexAcpError::InvalidParams(format!(
            "`{COMPACT_METHOD}` requires `sessionId` or `threadId`"
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
        initial_collaboration_mode: Option<CollaborationMode>,
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
            initial_collaboration_mode,
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

fn extract_thread_collaboration_mode(resp: &Value) -> Option<CollaborationMode> {
    resp.get("collaborationMode")
        .and_then(|cm| cm.get("mode"))
        .and_then(Value::as_str)
        .and_then(|mode| CollaborationMode::parse(mode).ok())
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
            let collaboration_mode = extract_thread_collaboration_mode(&resp);

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
                    collaboration_mode,
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
            let collaboration_mode = extract_thread_collaboration_mode(&resp);

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
                    collaboration_mode,
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
            let collaboration_mode = extract_thread_collaboration_mode(&resp);

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
                    collaboration_mode,
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
