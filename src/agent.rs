//! The `codex-acp` ACP server: a [`SessionHost`] bridging ACP sessions to
//! `codex app-server` threads, plus the [`serve`]/[`serve_stdio`] entry
//! points.
//!
//! [`CodexHost`] owns the one shared [`AppServer`]; every session is a
//! [`CodexSession`] subscribed to a
//! per-thread event channel and driving a turn through
//! [`Session::prompt`](aither_acp::server::Session::prompt).

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;

use aither_acp::domain::ids::AuthMethodId;
use aither_acp::domain::{
    AgentAuth, AgentCapabilities, AuthKind, AuthMethod, McpCaps, Open, PromptCaps, SessionCaps,
    SessionInfo, SessionListParams, SessionPage, SessionSetupParams,
};
use aither_acp::rpc::{RpcError, Transport};
use aither_acp::server::{
    AuthConfig, AuthRequest, ResumeRequest, Server, ServerError, SessionContext, SessionHost,
};
use aither_acp::{Implementation, vendor::codex as vx};
use async_lock::Mutex;
use serde_json::{Value, json};
use tracing::{debug, error, warn};

use crate::app_server::{AppServer, Error};
use crate::session::CodexSession;

/// Configuration for the ACP server.
#[derive(Debug, Clone)]
pub struct CodexAcpConfig {
    /// Spawn configuration for `codex app-server`.
    pub spawn: crate::app_server::SpawnConfig,
    /// Initial ACP session mode id (see [`crate::session::all_modes`]).
    pub default_mode: String,
}

impl Default for CodexAcpConfig {
    fn default() -> Self {
        Self {
            spawn: crate::app_server::SpawnConfig::default(),
            default_mode: crate::session::default_mode().id.to_string(),
        }
    }
}

/// Shared state behind [`CodexHost`].
struct HostInner {
    server: Arc<AppServer>,
    sessions: Arc<Mutex<HashMap<String, CodexSession>>>,
    default_mode: String,
}

/// The ACP session host: implements [`SessionHost`] over a `codex
/// app-server` connection.
#[derive(Clone)]
pub struct CodexHost {
    inner: Arc<HostInner>,
}

impl CodexHost {
    /// A host bridging to `server` with the given default mode.
    #[must_use]
    pub fn new(server: Arc<AppServer>, default_mode: impl Into<String>) -> Self {
        Self {
            inner: Arc::new(HostInner {
                server,
                sessions: Arc::new(Mutex::new(HashMap::new())),
                default_mode: default_mode.into(),
            }),
        }
    }

    /// The wrapped app-server connection.
    #[must_use]
    pub fn server(&self) -> &Arc<AppServer> {
        &self.inner.server
    }

    /// Look up a live session by ACP id.
    pub async fn session(&self, id: &str) -> Option<CodexSession> {
        self.inner.sessions.lock().await.get(id).cloned()
    }

    /// Build and register a session around an existing thread.
    async fn attach(
        &self,
        session_id: aither_acp::domain::SessionId,
        thread_id: String,
        cwd: PathBuf,
        events: async_channel::Receiver<crate::app_server::AppEvent>,
        model: Option<String>,
        effort: Option<String>,
    ) -> CodexSession {
        let session = CodexSession::new(
            session_id.clone(),
            thread_id.clone(),
            cwd,
            events,
            self.inner.server.clone(),
            self.inner.sessions.clone(),
            self.inner.default_mode.clone(),
        );
        session.set_model_effort(model, effort);
        self.refresh_models(&session).await;
        self.inner
            .sessions
            .lock()
            .await
            .insert(session_id.to_string(), session.clone());
        session
    }

    /// Fill the session's cached model list from `model/list`.
    async fn refresh_models(&self, session: &CodexSession) {
        let list = self
            .inner
            .server
            .call::<_, codex_codes::ModelListResponse>(
                "model/list",
                &json!({"includeHidden": false}),
            )
            .await;
        let models = match list {
            Ok(list) => list
                .data
                .iter()
                .map(|m| (m.id.clone(), m.display_name.clone()))
                .collect(),
            Err(e) => {
                warn!(%e, "model/list failed; model selector omitted");
                Vec::new()
            }
        };
        session.set_models(models);
    }

    /// Subscribe to a thread and return its event receiver.
    async fn subscribe(
        &self,
        thread_id: &str,
    ) -> async_channel::Receiver<crate::app_server::AppEvent> {
        self.inner.server.subscribe(thread_id).await
    }

    /// Register an already-fetched session; helper for the resume paths.
    async fn attach_resumed(
        &self,
        params: SessionSetupParams,
        cx: &SessionContext,
        thread_id: String,
    ) -> CodexSession {
        let events = self.subscribe(&thread_id).await;
        self.attach(
            cx.session_id().clone(),
            thread_id,
            params.cwd,
            events,
            None,
            None,
        )
        .await
    }

    /// Replay a thread's item history into the resume/fork sink.
    async fn replay(
        &self,
        thread_id: &str,
        replay: &aither_acp::server::ReplaySink,
    ) -> Result<(), Error> {
        let thread = self
            .inner
            .server
            .call::<_, codex_codes::ThreadReadResponse>(
                "thread/read",
                &json!({"threadId": thread_id}),
            )
            .await?;
        for t in &thread.thread.turns {
            for item in &t.items {
                let v = serde_json::to_value(item).unwrap_or_default();
                for update in replay_update(&v) {
                    let _ = replay.send(update);
                }
            }
        }
        Ok(())
    }
}

/// One thread item → zero or more replayed session updates.
fn replay_update(item: &Value) -> Vec<aither_acp::domain::SessionUpdate> {
    use crate::translate;
    let p = item.get("payload").unwrap_or(item);
    let item_type = p.get("type").and_then(Value::as_str).unwrap_or_default();
    let mut out = Vec::new();
    match item_type {
        "userMessage" => {
            if let Some(text) = p
                .get("content")
                .and_then(Value::as_array)
                .and_then(|a| a.first())
                .and_then(|c| c.get("text"))
                .and_then(Value::as_str)
            {
                out.push(translate::user_chunk(text));
            }
        }
        "agentMessage" => {
            if let Some(text) = p.get("text").and_then(Value::as_str) {
                out.push(translate::agent_chunk(text));
            }
        }
        "reasoning" => {
            if let Some(text) = p
                .get("summary")
                .and_then(Value::as_array)
                .and_then(|a| a.first())
                .and_then(Value::as_str)
            {
                out.push(translate::thought_chunk(text));
            }
        }
        _ => {}
    }
    if let Some(update) = translate::item_as_tool_call(item, None) {
        out.push(aither_acp::domain::SessionUpdate::ToolCallUpdate(update));
    }
    out
}

/// MCP session parameters → app-server `mcpConfig` JSON.
fn mcp_config(params: &SessionSetupParams) -> Value {
    use aither_acp::domain::McpServer;
    let servers: Vec<Value> = params
        .mcp_servers
        .iter()
        .filter_map(|s| match s {
            McpServer::Stdio(s) => Some(json!({
                "type": "stdio",
                "name": s.name,
                "command": s.command,
                "args": s.args,
                "env": s.env.iter().map(|e| json!({"name": e.name, "value": e.value}))
                    .collect::<Vec<_>>(),
            })),
            McpServer::Http(s) => Some(json!({
                "type": "http",
                "name": s.name,
                "url": s.url,
                "headers": s.headers.iter().map(|h| json!({"name": h.name, "value": h.value}))
                    .collect::<Vec<_>>(),
            })),
            // v1 `sse` arrives as Unknown; decode the raw object anyway.
            McpServer::Unknown(tag, raw) if tag == "sse" => Some(json!({
                "type": "sse",
                "name": raw.get("name").cloned().unwrap_or_default(),
                "url": raw.get("url").cloned().unwrap_or_default(),
            })),
            _ => None,
        })
        .collect();
    json!({"servers": servers})
}

impl SessionHost for CodexHost {
    type Session = CodexSession;

    fn capabilities(&self) -> AgentCapabilities {
        AgentCapabilities {
            session: Some(SessionCaps {
                prompt: Some(PromptCaps {
                    image: Some(aither_acp::domain::patch::EmptyObject),
                    audio: None,
                    embedded_context: Some(aither_acp::domain::patch::EmptyObject),
                    meta: None,
                    extra: BTreeMap::default(),
                }),
                mcp: Some(McpCaps {
                    stdio: Some(aither_acp::domain::patch::EmptyObject),
                    http: Some(aither_acp::domain::patch::EmptyObject),
                    acp: None,
                    meta: None,
                    extra: BTreeMap::default(),
                }),
                delete: Some(aither_acp::domain::patch::EmptyObject),
                fork: Some(aither_acp::domain::patch::EmptyObject),
                options: Some(aither_acp::domain::patch::EmptyObject),
                additional_directories: None,
                system_prompt: None,
                archive: None,
                status: None,
                title: None,
                meta: None,
                // The v1 codec reads `list`/`resume`/`close`/`loadSession`
                // out of `extra`.
                extra: [
                    ("list".to_string(), json!({})),
                    ("resume".to_string(), json!({})),
                    ("close".to_string(), json!({})),
                    ("loadSession".to_string(), json!(true)),
                ]
                .into_iter()
                .collect(),
            }),
            auth: Some(json!({"logout": true})),
            providers: None,
            // Extension capabilities: steering is a provider-neutral
            // convention keyed under `_meta.steering`.
            meta: Some(json!({
                aither_acp::ext::steering::CAPABILITY: { "supported": true },
            })),
            extra: BTreeMap::default(),
        }
    }

    async fn new_session(
        &self,
        params: SessionSetupParams,
        cx: &SessionContext,
    ) -> Result<Self::Session, ServerError> {
        let mode = crate::session::default_mode();
        let cwd = params.cwd.clone();
        let resp = self
            .inner
            .server
            .call::<_, codex_codes::ThreadStartResponse>(
                "thread/start",
                &json!({
                    "cwd": cwd.to_string_lossy(),
                    "approvalPolicy": mode.approval_policy,
                    "approvalsReviewer": mode.approvals_reviewer,
                    "sandboxPolicy": mode.sandbox_policy,
                    "sandbox": mode.sandbox_mode,
                    "config": {"mcp_servers": mcp_config(&params)["servers"]},
                }),
            )
            .await
            .map_err(|e| ServerError::from(RpcError::internal_error(e.to_string())))?;
        let thread_id = resp.thread.id.clone();
        let events = self.subscribe(&thread_id).await;
        Ok(self
            .attach(
                cx.session_id().clone(),
                thread_id,
                cwd,
                events,
                resp.thread.model.clone(),
                None,
            )
            .await)
    }

    async fn resume_session(
        &self,
        request: ResumeRequest,
        cx: &SessionContext,
    ) -> Result<Self::Session, ServerError> {
        let err = |e: Error| ServerError::from(RpcError::internal_error(e.to_string()));
        let thread_id = request.params.session_id.to_string();
        let resp = self
            .inner
            .server
            .call::<_, codex_codes::ThreadResumeResponse>(
                "thread/resume",
                &json!({"threadId": thread_id}),
            )
            .await
            .map_err(err)?;
        let session = self
            .attach_resumed(request.params.setup, cx, resp.thread.id.clone())
            .await;
        session.set_model_effort(resp.thread.model.clone(), None);
        let _ = self.replay(&resp.thread.id, &request.replay).await;
        Ok(session)
    }

    async fn fork_session(
        &self,
        request: ResumeRequest,
        cx: &SessionContext,
    ) -> Result<Self::Session, ServerError> {
        let err = |e: Error| ServerError::from(RpcError::internal_error(e.to_string()));
        let thread_id = request.params.session_id.to_string();
        let resp = self
            .inner
            .server
            .call::<_, codex_codes::ThreadForkResponse>(
                "thread/fork",
                &json!({"threadId": thread_id}),
            )
            .await
            .map_err(err)?;
        let session = self
            .attach_resumed(request.params.setup, cx, resp.thread.id.clone())
            .await;
        let _ = self.replay(&resp.thread.id, &request.replay).await;
        Ok(session)
    }

    async fn list_sessions(&self, params: SessionListParams) -> Result<SessionPage, RpcError> {
        let cwd = params.cwd.clone();
        let resp = self
            .inner
            .server
            .call::<_, codex_codes::ThreadListResponse>(
                "thread/list",
                &json!({
                    "cwd": cwd.as_ref().map(|p| p.to_string_lossy()),
                    "cursor": params.cursor,
                    "archived": false,
                }),
            )
            .await
            .map_err(|e| RpcError::internal_error(e.to_string()))?;
        Ok(SessionPage {
            sessions: resp
                .data
                .iter()
                .map(|t| SessionInfo {
                    session_id: t.id.clone().into(),
                    cwd: PathBuf::from(t.cwd.as_str().unwrap_or_default()),
                    title: t
                        .name
                        .clone()
                        .or_else(|| (!t.preview.is_empty()).then(|| t.preview.clone())),
                    updated_at: Some(rfc3339_from_ms(t.updated_at)),
                    meta: None,
                    extra: BTreeMap::default(),
                })
                .collect(),
            next_cursor: resp.next_cursor.clone(),
            meta: None,
            extra: BTreeMap::default(),
        })
    }

    async fn delete_session(
        &self,
        session_id: &aither_acp::domain::SessionId,
    ) -> Result<(), RpcError> {
        // Live sessions know their thread id; stored sessions are keyed by
        // the thread id directly (see `list_sessions`).
        let thread_id = self.session(session_id.as_str()).await.map_or_else(
            || session_id.to_string(),
            |session| session.thread_id().to_string(),
        );
        self.inner
            .server
            .call::<_, Value>("thread/delete", &json!({"threadId": thread_id}))
            .await
            .map_err(|e| RpcError::internal_error(e.to_string()))?;
        let _ = self
            .inner
            .server
            .call::<_, Value>("thread/unsubscribe", &json!({"threadId": thread_id}))
            .await;
        self.inner.server.unsubscribe(&thread_id).await;
        self.inner.sessions.lock().await.remove(session_id.as_str());
        Ok(())
    }
}

/// ISO-8601 UTC timestamp from epoch milliseconds without a date dependency.
fn rfc3339_from_ms(ms: i64) -> String {
    let secs = ms.div_euclid(1000);
    let nanos = u32::try_from(ms.rem_euclid(1000)).unwrap_or_default() * 1_000_000;
    let days = secs.div_euclid(86_400);
    let tod = secs.rem_euclid(86_400);
    let (hour, min, sec) = (tod / 3600, tod % 3600 / 60, tod % 60);
    // Civil-from-days (Howard Hinnant's algorithm).
    let shifted = days + 719_468;
    let era = shifted.div_euclid(146_097);
    let doe = shifted.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let full_year = if month <= 2 { year + 1 } else { year };
    format!("{full_year:04}-{month:02}-{day:02}T{hour:02}:{min:02}:{sec:02}.{nanos:09}Z")
}

/// The Codex auth methods advertised on `initialize`.
fn auth_methods() -> Vec<AuthMethod> {
    [
        (vx::auth::CHAT_GPT, "Sign in with ChatGPT"),
        (vx::auth::API_KEY, "Use an API key"),
    ]
    .into_iter()
    .map(|(id, name)| AuthMethod {
        method_id: AuthMethodId::from_string(id),
        name: name.to_string(),
        description: None,
        kind: Open::Known(AuthKind::Agent(AgentAuth::default())),
        meta: None,
        extra: BTreeMap::default(),
    })
    .collect()
}

/// `auth/login` and `auth/logout` forwarded to the app-server account API.
async fn handle_auth(host: &CodexHost, request: AuthRequest) -> Result<(), RpcError> {
    let server = host.server();
    match request {
        AuthRequest::Login(req) => {
            let _resp: Value = if req.method_id.as_str() == vx::auth::API_KEY {
                let key = req
                    .meta
                    .as_ref()
                    .and_then(Value::as_object)
                    .and_then(|o| {
                        o.get("apiKey")
                            .or_else(|| o.get("api_key"))
                            .or_else(|| o.get("token"))
                    })
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(ToOwned::to_owned)
                    .or_else(|| std::env::var("OPENAI_API_KEY").ok())
                    .ok_or_else(|| {
                        RpcError::invalid_params(
                            "no apiKey in auth/login _meta and OPENAI_API_KEY is not set",
                        )
                    })?;
                server
                    .call(
                        "account/login/start",
                        &json!({"type": "apiKey", "apiKey": key}),
                    )
                    .await
                    .map_err(|e| RpcError::internal_error(e.to_string()))?
            } else {
                let login_type = if req.method_id.as_str() == vx::auth::CHAT_GPT_DEVICE_CODE {
                    "chatgptDeviceCode"
                } else {
                    "chatgpt"
                };
                server
                    .call("account/login/start", &json!({"type": login_type}))
                    .await
                    .map_err(|e| RpcError::internal_error(e.to_string()))?
            };
            // Wait for the login completion notification on the global lane.
            let rx = server.global_events();
            loop {
                match rx.recv().await {
                    Ok(crate::app_server::AppEvent::Notification(
                        codex_codes::Notification::AccountLoginCompleted(_),
                    )) => return Ok(()),
                    Ok(_) => {}
                    Err(e) => {
                        return Err(RpcError::internal_error(format!(
                            "login watcher closed: {e}"
                        )));
                    }
                }
            }
        }
        AuthRequest::Logout(_) => {
            server
                .call::<_, Value>("account/logout", &json!({}))
                .await
                .map_err(|e| RpcError::internal_error(e.to_string()))?;
            Ok(())
        }
    }
}

/// `_`-prefixed extension methods: steering plus goal-control aliases.
async fn handle_ext(
    host: &CodexHost,
    method: &aither_acp::domain::ExtMethod,
    params: Option<Value>,
) -> Result<Value, RpcError> {
    match method.as_str() {
        "_session/steering" | "_codex/session/steering" => {
            let params = params.ok_or_else(|| RpcError::invalid_params("missing params"))?;
            let session_id = params
                .get("sessionId")
                .and_then(Value::as_str)
                .ok_or_else(|| RpcError::invalid_params("missing sessionId"))?;
            let session = host
                .session(session_id)
                .await
                .ok_or_else(|| RpcError::invalid_params(format!("unknown session {session_id}")))?;
            // Both `message` and `prompt` payloads are accepted; `prompt`
            // may be a string or a content-block array.
            let input = match params.get("message").or_else(|| params.get("prompt")) {
                Some(Value::String(s)) => vec![json!({"type": "text", "text": s})],
                Some(Value::Array(blocks)) => {
                    let blocks: Vec<aither_acp::domain::ContentBlock> =
                        serde_json::from_value(Value::Array(blocks.clone()))
                            .map_err(|e| RpcError::invalid_params(e.to_string()))?;
                    blocks
                        .iter()
                        .filter_map(|b| b.as_text())
                        .map(|t| json!({"type": "text", "text": t}))
                        .collect()
                }
                _ => return Err(RpcError::invalid_params("missing prompt/message")),
            };
            let turn_id = session
                .active_turn_id()
                .await
                .ok_or_else(|| RpcError::invalid_params("no active turn to steer"))?;
            let resp: Value = session_server(host)
                .call(
                    "turn/steer",
                    &json!({
                        "threadId": session.thread_id(),
                        "expectedTurnId": turn_id,
                        "input": input,
                    }),
                )
                .await
                .map_err(|e| RpcError::internal_error(e.to_string()))?;
            Ok(json!({
                "outcome": aither_acp::ext::steering::SteeringOutcome::Injected.as_str(),
                "turnId": resp.get("turnId").cloned().unwrap_or_default(),
            }))
        }
        _ => Err(RpcError::method_not_found(method.as_str())),
    }
}

fn session_server(host: &CodexHost) -> &Arc<AppServer> {
    host.server()
}

/// Non-extension methods the standard dispatch does not cover: the Codex
/// legacy model/account surface.
async fn handle_unknown(
    host: &CodexHost,
    method: &str,
    params: Option<Value>,
) -> Result<Value, RpcError> {
    match method {
        vx::SET_MODEL_METHOD => {
            let params = params.ok_or_else(|| RpcError::invalid_params("missing params"))?;
            let session_id = params
                .get("sessionId")
                .and_then(Value::as_str)
                .ok_or_else(|| RpcError::invalid_params("missing sessionId"))?;
            let session = host
                .session(session_id)
                .await
                .ok_or_else(|| RpcError::invalid_params(format!("unknown session {session_id}")))?;
            let model = params
                .get("model")
                .or_else(|| params.get("modelId"))
                .and_then(Value::as_str)
                .map(str::to_string);
            session.set_model(model);
            Ok(json!({}))
        }
        vx::AUTHENTICATION_STATUS_METHOD => {
            let v: Value = host
                .server()
                .call("account/read", &json!({}))
                .await
                .map_err(|e| RpcError::internal_error(e.to_string()))?;
            Ok(json!({"type": "account", "account": v}))
        }
        vx::AUTHENTICATION_LOGOUT_METHOD => {
            host.server()
                .call::<_, Value>("account/logout", &json!({}))
                .await
                .map_err(|e| RpcError::internal_error(e.to_string()))?;
            Ok(json!({}))
        }
        _ => Err(RpcError::method_not_found(method)),
    }
}

/// Serve ACP over `transport`, translating every session onto `server`.
///
/// `drive` must be the [`AppServer`] demultiplexer future returned by
/// [`AppServer::attach`]/[`AppServer::spawn`]; it is polled alongside the
/// ACP connection so
/// app-server traffic keeps flowing for the lifetime of the connection.
///
/// # Errors
///
/// Returns an error when the app-server handshake fails or the demux
/// ends before the handshake completes.
pub async fn serve<T, D>(
    transport: T,
    server: Arc<AppServer>,
    drive: D,
    config: CodexAcpConfig,
) -> Result<(), Error>
where
    T: Transport + 'static,
    D: Future<Output = ()> + Send,
{
    futures_lite::pin!(drive);
    // The handshake needs the demux running to receive responses; race it
    // against the drive future so both keep polling.
    let handshake = futures_lite::future::or(server.handshake(&config.spawn), async {
        drive.as_mut().await;
        Err(Error::Connection(crate::jsonrpc::Error::Closed))
    })
    .await;
    if let Err(e) = handshake {
        error!(%e, "codex app-server handshake failed");
        return Err(e);
    }
    let host = CodexHost::new(server, config.default_mode);
    let info = Implementation::new(env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));
    let ext_host = host.clone();
    let unknown_host = host.clone();
    let auth_host = host.clone();
    let acp = Server::builder(host, info)
        .auth(AuthConfig::new(auth_methods(), move |req| {
            let host = auth_host.clone();
            async move { handle_auth(&host, req).await }
        }))
        .ext(move |method, params| {
            let host = ext_host.clone();
            async move { handle_ext(&host, &method, params).await }
        })
        .unknown_handler(Arc::new(move |method: &str, params: Option<Value>| {
            let host = unknown_host.clone();
            let method = method.to_string();
            Box::pin(async move { handle_unknown(&host, &method, params).await })
                as std::pin::Pin<Box<dyn Future<Output = Result<Value, RpcError>> + Send>>
        }))
        .build();
    let (_peer, serve) = acp.serve(transport);
    futures_lite::future::or(serve, drive).await;
    debug!("ACP connection closed");
    Ok(())
}

/// Serve ACP over stdio, spawning `codex app-server` per `config`.
///
/// # Errors
///
/// Returns an error when the app-server fails to spawn or its handshake
/// fails.
#[cfg(feature = "process")]
pub async fn serve_stdio(config: CodexAcpConfig) -> Result<(), Error> {
    let (server, drive) = AppServer::spawn(&config.spawn).await?;
    serve(aither_acp::rpc::Stdio::new(), server, drive, config).await
}
