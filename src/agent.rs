//! The ACP-facing half: session lifecycle, prompting, permissions, modes,
//! config options and auth, served on any ACP transport.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use agent_client_protocol::v1::schema as acp;
use agent_client_protocol::{self as sdk, Agent, Error as AcpError};
use async_lock::Mutex;
use serde_json::{Value, json};
use tracing::debug;

use crate::app_server::{AppEvent, AppServer, SpawnConfig};
use crate::session::{self, Session};
use crate::translate;

/// Bridge configuration.
#[derive(Debug, Clone, Default)]
pub struct CodexAcpConfig {
    /// How `codex` is spawned.
    pub spawn: SpawnConfig,
    /// Mode applied when the client selects none (`agent`).
    pub default_mode: Option<String>,
}

type Conn = sdk::v1::ConnectionTo<sdk::Client>;

/// Shared bridge state behind the ACP connection.
pub struct Bridge {
    server: Arc<AppServer>,
    sessions: Mutex<HashMap<String, Arc<Session>>>,
    client_caps: Mutex<acp::ClientCapabilities>,
    default_mode: String,
}

impl Bridge {
    async fn session(&self, id: &acp::SessionId) -> Result<Arc<Session>, AcpError> {
        self.sessions
            .lock()
            .await
            .get(id.0.as_ref())
            .cloned()
            .ok_or_else(|| AcpError::invalid_params().data(format!("unknown session {id:?}")))
    }

    async fn config_options(&self, s: &Session) -> Vec<acp::SessionConfigOption> {
        let mode = s.mode.lock().await.clone();
        let model = s.model.lock().await.clone();
        let effort = s.effort.lock().await.clone();
        let fast = *s.fast.lock().await;
        let mut options = vec![session::mode_option(&mode)];
        if let Ok(models) = self
            .server
            .call::<_, codex_codes::ModelListResponse>("model/list", &json!({}))
            .await
        {
            let entries: Vec<_> = models
                .data
                .iter()
                .filter_map(|m| serde_json::to_value(m).ok())
                .filter_map(|v| {
                    let id = v.get("id").and_then(Value::as_str)?;
                    let name = v
                        .get("displayName")
                        .or_else(|| v.get("name"))
                        .and_then(Value::as_str)
                        .unwrap_or(id);
                    Some(session::select_option(id, name))
                })
                .collect();
            if !entries.is_empty() {
                options.push(select_option(
                    MODEL_CONFIG_ID,
                    "Model",
                    "Model used for the next turn",
                    acp::SessionConfigOptionCategory::Model,
                    model.as_deref().unwrap_or(""),
                    entries,
                ));
            }
        }
        options.push(select_option(
            EFFORT_CONFIG_ID,
            "Reasoning effort",
            "Reasoning effort for the next turn",
            acp::SessionConfigOptionCategory::ThoughtLevel,
            effort.as_deref().unwrap_or(""),
            ["minimal", "low", "medium", "high", "xhigh"]
                .into_iter()
                .map(|e| session::select_option(e, e))
                .collect(),
        ));
        options.push(
            acp::SessionConfigOption::new(
                FAST_MODE_CONFIG_ID,
                "Fast mode",
                acp::SessionConfigKind::Boolean(acp::SessionConfigBoolean::new(fast)),
            )
            .description("Prefer speed over cost for the next turn")
            .category(acp::SessionConfigOptionCategory::ModelConfig),
        );
        options
    }
}

use crate::session::{EFFORT_CONFIG_ID, FAST_MODE_CONFIG_ID, MODEL_CONFIG_ID};

fn select_option(
    id: &str,
    name: &str,
    description: &str,
    category: acp::SessionConfigOptionCategory,
    current: &str,
    entries: Vec<acp::SessionConfigSelectOption>,
) -> acp::SessionConfigOption {
    acp::SessionConfigOption::new(
        id.to_string(),
        name.to_string(),
        acp::SessionConfigKind::Select(acp::SessionConfigSelect::new(
            current.to_string(),
            acp::SessionConfigSelectOptions::Ungrouped(entries),
        )),
    )
    .description(description)
    .category(category)
}

fn err(e: impl std::fmt::Display) -> AcpError {
    AcpError::internal_error().data(e.to_string())
}

/// Serve the bridge over a caller-provided ACP transport and an attached
/// app-server.
///
/// `drive` is the pump returned by [`AppServer::attach`]/[`AppServer::spawn`];
/// the connection ends when either side closes.
///
/// # Errors
///
/// Propagates connection and protocol errors.
pub async fn serve<T, D>(
    transport: T,
    server: Arc<AppServer>,
    drive: D,
    config: CodexAcpConfig,
) -> Result<(), AcpError>
where
    T: sdk::ConnectTo<Agent>,
    D: Future<Output = ()> + Send,
{
    let bridge = Arc::new(Bridge {
        server,
        sessions: Mutex::new(HashMap::new()),
        client_caps: Mutex::new(acp::ClientCapabilities::default()),
        default_mode: config.default_mode.unwrap_or_else(|| "agent".to_string()),
    });
    let serve = serve_on(transport, bridge);
    let driven = async move {
        drive.await;
        Ok(())
    };
    futures_lite::future::or(serve, driven).await
}

/// Spawn `codex app-server` and serve the bridge over process stdio — the
/// usual editor-adapter entry point.
///
/// # Errors
///
/// Propagates spawn/handshake and connection errors.
#[cfg(feature = "process")]
pub async fn serve_stdio(config: CodexAcpConfig) -> Result<(), AcpError> {
    let spawn = config.spawn.clone();
    let (server, drive) = AppServer::spawn(&spawn).await.map_err(err)?;
    serve(sdk::Stdio::new(), server, drive, config).await
}

async fn serve_on<T: sdk::ConnectTo<Agent>>(
    transport: T,
    bridge: Arc<Bridge>,
) -> Result<(), AcpError> {
    Agent
        .v1()
        .name("codex-acp")
        .on_receive_request(
            {
                let bridge = bridge.clone();
                async move |req: acp::InitializeRequest, r, _conn| {
                    *bridge.client_caps.lock().await = req.client_capabilities.clone();
                    r.respond(initialize_response(req.protocol_version))
                }
            },
            sdk::on_receive_request!(),
        )
        .on_receive_request(
            {
                let bridge = bridge.clone();
                async move |req: acp::AuthenticateRequest, r, _conn| {
                    authenticate(bridge.clone(), req, r).await
                }
            },
            sdk::on_receive_request!(),
        )
        .on_receive_request(
            {
                let bridge = bridge.clone();
                async move |req: acp::NewSessionRequest, r, _conn| {
                    new_session(bridge.clone(), req, r).await
                }
            },
            sdk::on_receive_request!(),
        )
        .on_receive_request(
            {
                let bridge = bridge.clone();
                async move |req: acp::LoadSessionRequest, r, conn| {
                    load_session(bridge.clone(), req, r, conn).await
                }
            },
            sdk::on_receive_request!(),
        )
        .on_receive_request(
            {
                let bridge = bridge.clone();
                async move |req: acp::ResumeSessionRequest, r, _conn| {
                    resume_session(bridge.clone(), req, r).await
                }
            },
            sdk::on_receive_request!(),
        )
        .on_receive_request(
            {
                let bridge = bridge.clone();
                async move |req: acp::ForkSessionRequest, r, conn| {
                    fork_session(bridge.clone(), req, r, conn).await
                }
            },
            sdk::on_receive_request!(),
        )
        .on_receive_request(
            {
                let bridge = bridge.clone();
                async move |req: acp::ListSessionsRequest, r, _conn| {
                    list_sessions(bridge.clone(), req, r).await
                }
            },
            sdk::on_receive_request!(),
        )
        .on_receive_request(
            {
                let bridge = bridge.clone();
                async move |req: acp::CloseSessionRequest, r, _conn| {
                    close_session(bridge.clone(), req, r).await
                }
            },
            sdk::on_receive_request!(),
        )
        .on_receive_request(
            {
                let bridge = bridge.clone();
                async move |req: acp::DeleteSessionRequest, r, _conn| {
                    delete_session(bridge.clone(), req, r).await
                }
            },
            sdk::on_receive_request!(),
        )
        .on_receive_request(
            {
                let bridge = bridge.clone();
                async move |req: acp::PromptRequest, r, conn| {
                    prompt(bridge.clone(), req, r, conn).await
                }
            },
            sdk::on_receive_request!(),
        )
        .on_receive_request(
            {
                let bridge = bridge.clone();
                async move |req: acp::SetSessionModeRequest, r, _conn| {
                    set_mode(bridge.clone(), req, r).await
                }
            },
            sdk::on_receive_request!(),
        )
        .on_receive_request(
            {
                let bridge = bridge.clone();
                async move |req: acp::SetSessionConfigOptionRequest, r, _conn| {
                    set_config_option(bridge.clone(), req, r).await
                }
            },
            sdk::on_receive_request!(),
        )
        .on_receive_request(
            {
                let bridge = bridge.clone();
                async move |req: sdk::UntypedMessage, r, _conn| {
                    ext_request(bridge.clone(), req, r).await
                }
            },
            sdk::on_receive_request!(),
        )
        .on_receive_notification(
            {
                let bridge = bridge.clone();
                async move |n: acp::CancelNotification, _conn| cancel(bridge.clone(), n).await
            },
            sdk::on_receive_notification!(),
        )
        .connect_to(transport)
        .await
}

fn initialize_response(version: sdk::schema::ProtocolVersion) -> acp::InitializeResponse {
    let caps = acp::AgentCapabilities::new()
        .load_session(true)
        .prompt_capabilities(
            acp::PromptCapabilities::new()
                .image(true)
                .embedded_context(true),
        )
        .mcp_capabilities(acp::McpCapabilities::new().http(true).sse(true))
        .session_capabilities(
            acp::SessionCapabilities::new()
                .list(acp::SessionListCapabilities::new())
                .delete(acp::SessionDeleteCapabilities::new())
                .resume(acp::SessionResumeCapabilities::new())
                .fork(acp::SessionForkCapabilities::new())
                .close(acp::SessionCloseCapabilities::new()),
        );
    acp::InitializeResponse::new(version)
        .agent_capabilities(caps)
        .auth_methods(vec![
            acp::AuthMethod::Agent(
                acp::AuthMethodAgent::new("chat-gpt", "ChatGPT")
                    .description("Sign in with a ChatGPT account"),
            ),
            acp::AuthMethod::Agent(
                acp::AuthMethodAgent::new("api-key", "API key")
                    .description("Use CODEX_API_KEY or OPENAI_API_KEY"),
            ),
        ])
        .agent_info(acp::Implementation::new("codex-acp", env!("CARGO_PKG_VERSION")).title("Codex"))
}

async fn authenticate(
    bridge: Arc<Bridge>,
    req: acp::AuthenticateRequest,
    r: sdk::v1::Responder<acp::AuthenticateResponse>,
) -> Result<(), AcpError> {
    match req.method_id.0.as_ref() {
        "api-key" => {
            let key = std::env::var("CODEX_API_KEY")
                .or_else(|_| std::env::var("OPENAI_API_KEY"))
                .map_err(|_| {
                    AcpError::invalid_params().data("no API key in CODEX_API_KEY/OPENAI_API_KEY")
                })?;
            bridge
                .server
                .call::<_, Value>(
                    "account/login/start",
                    &json!({"type": "apiKey", "apiKey": key}),
                )
                .await
                .map_err(err)?;
        }
        "chat-gpt" | "chat-gpt-device-code" => {
            let resp: Value = bridge
                .server
                .call("account/login/start", &json!({"type": "chatgpt"}))
                .await
                .map_err(err)?;
            let url = resp
                .get("authUrl")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let login_id = resp.get("loginId").cloned().unwrap_or(Value::Null);
            tracing::info!(%url, "chatgpt login url");
            let rx = bridge.server.global_events();
            loop {
                let ev = rx
                    .recv()
                    .await
                    .map_err(|_| AcpError::internal_error().data("app-server closed"))?;
                if let AppEvent::Notification(codex_codes::Notification::AccountLoginCompleted(n)) =
                    ev
                {
                    let raw = serde_json::to_value(&n).unwrap_or_default();
                    if login_id.is_null() || raw.get("loginId") == Some(&login_id) {
                        if raw.get("success").and_then(Value::as_bool).unwrap_or(true) {
                            return r.respond(acp::AuthenticateResponse::new());
                        }
                        return Err(AcpError::internal_error().data("login failed"));
                    }
                }
            }
        }
        other => {
            return Err(AcpError::invalid_params().data(format!("unknown auth method {other}")));
        }
    }
    r.respond(acp::AuthenticateResponse::new())
}

/// Turn client-provided MCP servers into a codex `mcp_servers` config map.
fn mcp_config(servers: &[acp::McpServer]) -> Value {
    let mut map = serde_json::Map::new();
    for s in servers {
        match s {
            acp::McpServer::Stdio(stdio) => {
                let env: serde_json::Map<String, Value> = stdio
                    .env
                    .iter()
                    .map(|e| (e.name.clone(), Value::from(e.value.clone())))
                    .collect();
                map.insert(
                    stdio.name.clone(),
                    json!({"command": stdio.command, "args": stdio.args, "env": env}),
                );
            }
            acp::McpServer::Http(http) => {
                map.insert(http.name.clone(), json!({"url": http.url}));
            }
            acp::McpServer::Sse(sse) => {
                map.insert(sse.name.clone(), json!({"url": sse.url}));
            }
            _ => {}
        }
    }
    json!({"mcp_servers": map})
}

async fn new_session(
    bridge: Arc<Bridge>,
    req: acp::NewSessionRequest,
    r: sdk::v1::Responder<acp::NewSessionResponse>,
) -> Result<(), AcpError> {
    let mode = session::find_mode(&bridge.default_mode).unwrap_or_else(session::default_mode);
    let resp: codex_codes::ThreadStartResponse = bridge
        .server
        .call(
            "thread/start",
            &json!({
                "cwd": req.cwd.to_string_lossy(),
                "approvalPolicy": mode.approval_policy,
                "approvalsReviewer": mode.approvals_reviewer,
                "sandbox": mode.sandbox_mode,
                "sandboxPolicy": mode.sandbox_policy,
                "config": mcp_config(&req.mcp_servers),
            }),
        )
        .await
        .map_err(err)?;
    let thread_id = serde_json::to_value(&resp.thread)
        .ok()
        .and_then(|t| t.get("id").and_then(Value::as_str).map(str::to_string))
        .unwrap_or_default();
    let events = bridge.server.subscribe(thread_id.clone()).await;
    let s = Session::new(
        thread_id,
        req.cwd.clone(),
        events,
        bridge.default_mode.clone(),
        req.mcp_servers.clone(),
    );
    *s.model.lock().await = Some(resp.model.clone());
    *s.effort.lock().await = resp.reasoning_effort.as_ref().map(|e| e.0.clone());
    bridge
        .sessions
        .lock()
        .await
        .insert(s.id.0.to_string(), s.clone());
    let modes = s.mode_state().await;
    let config_options = bridge.config_options(&s).await;
    r.respond(
        acp::NewSessionResponse::new(s.id.clone())
            .modes(modes)
            .config_options(config_options),
    )
}

async fn list_sessions(
    bridge: Arc<Bridge>,
    req: acp::ListSessionsRequest,
    r: sdk::v1::Responder<acp::ListSessionsResponse>,
) -> Result<(), AcpError> {
    let resp: codex_codes::ThreadListResponse = bridge
        .server
        .call(
            "thread/list",
            &json!({
                "cwd": req.cwd.as_ref().map(|p| p.to_string_lossy().to_string()),
                "cursor": req.cursor,
            }),
        )
        .await
        .map_err(err)?;
    let sessions = resp
        .data
        .iter()
        .filter_map(|t| {
            let v = serde_json::to_value(t).ok()?;
            let id = v.get("id").and_then(Value::as_str)?.to_string();
            let cwd = v.get("cwd").and_then(Value::as_str).unwrap_or_default();
            Some(
                acp::SessionInfo::new(id, PathBuf::from(cwd))
                    .title(
                        v.get("name")
                            .or_else(|| v.get("title"))
                            .and_then(Value::as_str)
                            .map(str::to_string),
                    )
                    .updated_at(
                        v.get("updatedAt")
                            .and_then(Value::as_i64)
                            .map(rfc3339_from_ms),
                    ),
            )
        })
        .collect();
    r.respond(acp::ListSessionsResponse::new(sessions).next_cursor(resp.next_cursor))
}

/// ISO-8601 UTC timestamp from epoch milliseconds without a date dependency.
fn rfc3339_from_ms(ms: i64) -> String {
    let secs = ms.div_euclid(1000);
    let days = secs.div_euclid(86400);
    let day_secs = secs.rem_euclid(86400);
    let (y, mo, d) = civil_from_days(days);
    format!(
        "{y:04}-{mo:02}-{d:02}T{:02}:{:02}:{:02}Z",
        day_secs / 3600,
        day_secs % 3600 / 60,
        day_secs % 60
    )
}

/// Howard Hinnant's civil-from-days algorithm.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = u32::try_from(doy - (153 * mp + 2) / 5 + 1).unwrap_or(1);
    let m = u32::try_from(if mp < 10 { mp + 3 } else { mp - 9 }).unwrap_or(1);
    (y + i64::from(m <= 2), m, d)
}

/// `thread/resume` + session registration shared by load/resume.
async fn resume_thread(
    bridge: &Arc<Bridge>,
    thread_id: &str,
    cwd: &std::path::Path,
    mcp_servers: &[acp::McpServer],
) -> Result<Arc<Session>, AcpError> {
    let resp: Value = bridge
        .server
        .call(
            "thread/resume",
            &json!({
                "threadId": thread_id,
                "cwd": cwd.to_string_lossy(),
                "config": mcp_config(mcp_servers),
            }),
        )
        .await
        .map_err(err)?;
    let events = bridge.server.subscribe(thread_id.to_string()).await;
    let s = Session::new(
        thread_id.to_string(),
        cwd.to_path_buf(),
        events,
        bridge.default_mode.clone(),
        mcp_servers.to_vec(),
    );
    *s.model.lock().await = resp
        .get("model")
        .and_then(Value::as_str)
        .map(str::to_string);
    *s.effort.lock().await = resp
        .get("reasoningEffort")
        .and_then(Value::as_str)
        .map(str::to_string);
    bridge
        .sessions
        .lock()
        .await
        .insert(s.id.0.to_string(), s.clone());
    Ok(s)
}

/// Replay stored thread items as session updates.
async fn replay_history(
    bridge: &Arc<Bridge>,
    s: &Session,
    conn: &Conn,
    session_id: &acp::SessionId,
) -> Result<(), AcpError> {
    let thread: Value = bridge
        .server
        .call(
            "thread/read",
            &json!({"threadId": s.thread_id, "includeTurns": true}),
        )
        .await
        .map_err(err)?;
    let turns = thread
        .get("thread")
        .and_then(|t| t.get("turns"))
        .or_else(|| thread.get("turns"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    for turn in &turns {
        for item in turn
            .get("items")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let payload = item.get("payload").unwrap_or(item);
            match payload.get("type").and_then(Value::as_str) {
                Some("userMessage") => {
                    let text = payload
                        .get("content")
                        .and_then(Value::as_array)
                        .map(|c| {
                            c.iter()
                                .filter(|b| b.get("type").and_then(Value::as_str) == Some("text"))
                                .filter_map(|b| b.get("text").and_then(Value::as_str))
                                .collect::<Vec<_>>()
                                .join("\n")
                        })
                        .unwrap_or_default();
                    send_update(
                        conn,
                        session_id,
                        acp::SessionUpdate::UserMessageChunk(acp::ContentChunk::new(
                            acp::ContentBlock::Text(acp::TextContent::new(text)),
                        )),
                    )
                    .await;
                }
                Some("agentMessage") => {
                    let text = payload
                        .get("text")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    send_update(conn, session_id, translate::agent_delta(text)).await;
                }
                Some("reasoning") => {
                    let text = payload
                        .get("summary")
                        .and_then(Value::as_array)
                        .and_then(|s| s.first())
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    send_update(conn, session_id, translate::thought_delta(text)).await;
                }
                Some(_) => {
                    if let Some(u) =
                        translate::item_as_tool_call(payload, acp::ToolCallStatus::Pending)
                    {
                        send_update(conn, session_id, u).await;
                    }
                }
                None => {}
            }
        }
    }
    Ok(())
}

async fn send_update(conn: &Conn, session_id: &acp::SessionId, update: acp::SessionUpdate) {
    let _ = conn.send_notification(acp::SessionNotification::new(session_id.clone(), update));
}

async fn load_session(
    bridge: Arc<Bridge>,
    req: acp::LoadSessionRequest,
    r: sdk::v1::Responder<acp::LoadSessionResponse>,
    conn: Conn,
) -> Result<(), AcpError> {
    let s = resume_thread(
        &bridge,
        req.session_id.0.as_ref(),
        &req.cwd,
        &req.mcp_servers,
    )
    .await?;
    replay_history(&bridge, &s, &conn, &req.session_id).await?;
    let modes = s.mode_state().await;
    let config_options = bridge.config_options(&s).await;
    r.respond(
        acp::LoadSessionResponse::new()
            .modes(modes)
            .config_options(config_options),
    )
}

async fn resume_session(
    bridge: Arc<Bridge>,
    req: acp::ResumeSessionRequest,
    r: sdk::v1::Responder<acp::ResumeSessionResponse>,
) -> Result<(), AcpError> {
    let s = resume_thread(
        &bridge,
        req.session_id.0.as_ref(),
        &req.cwd,
        &req.mcp_servers,
    )
    .await?;
    let modes = s.mode_state().await;
    let config_options = bridge.config_options(&s).await;
    r.respond(
        acp::ResumeSessionResponse::new()
            .modes(modes)
            .config_options(config_options),
    )
}

async fn fork_session(
    bridge: Arc<Bridge>,
    req: acp::ForkSessionRequest,
    r: sdk::v1::Responder<acp::ForkSessionResponse>,
    conn: Conn,
) -> Result<(), AcpError> {
    let resp: Value = bridge
        .server
        .call(
            "thread/fork",
            &json!({
                "threadId": req.session_id.0.as_ref(),
                "config": mcp_config(&req.mcp_servers),
            }),
        )
        .await
        .map_err(err)?;
    let new_id = resp
        .get("thread")
        .and_then(|t| t.get("id"))
        .and_then(Value::as_str)
        .ok_or_else(|| AcpError::internal_error().data("fork returned no thread id"))?
        .to_string();
    let events = bridge.server.subscribe(new_id.clone()).await;
    let s = Session::new(
        new_id.clone(),
        req.cwd.clone(),
        events,
        bridge.default_mode.clone(),
        req.mcp_servers.clone(),
    );
    let acp_id = acp::SessionId::new(new_id.clone());
    bridge.sessions.lock().await.insert(new_id, s.clone());
    replay_history(&bridge, &s, &conn, &acp_id).await?;
    let modes = s.mode_state().await;
    let config_options = bridge.config_options(&s).await;
    r.respond(
        acp::ForkSessionResponse::new(acp_id)
            .modes(modes)
            .config_options(config_options),
    )
}

async fn close_session(
    bridge: Arc<Bridge>,
    req: acp::CloseSessionRequest,
    r: sdk::v1::Responder<acp::CloseSessionResponse>,
) -> Result<(), AcpError> {
    let id = req.session_id.0.to_string();
    let _ = bridge
        .server
        .call::<_, Value>("thread/unsubscribe", &json!({"threadId": id}))
        .await;
    bridge.server.unsubscribe(&id).await;
    bridge.sessions.lock().await.remove(&id);
    r.respond(acp::CloseSessionResponse::new())
}

async fn delete_session(
    bridge: Arc<Bridge>,
    req: acp::DeleteSessionRequest,
    r: sdk::v1::Responder<acp::DeleteSessionResponse>,
) -> Result<(), AcpError> {
    let id = req.session_id.0.to_string();
    bridge
        .server
        .call::<_, Value>("thread/delete", &json!({"threadId": id}))
        .await
        .map_err(err)?;
    bridge.server.unsubscribe(&id).await;
    bridge.sessions.lock().await.remove(&id);
    r.respond(acp::DeleteSessionResponse::new())
}

/// Map ACP prompt content to app-server `UserInput`s.
fn prompt_input(prompt: &[acp::ContentBlock], cwd: &std::path::Path) -> Vec<Value> {
    let mut input = Vec::new();
    for block in prompt {
        match block {
            acp::ContentBlock::Text(t) => input.push(json!({"type": "text", "text": t.text})),
            acp::ContentBlock::Image(i) => input.push(json!({
                "type": "image",
                "url": format!("data:{};base64,{}", i.mime_type, i.data),
            })),
            acp::ContentBlock::ResourceLink(l) => input.push(json!({
                "type": "mention",
                "name": l.name,
                "path": l.uri.strip_prefix("file://").map_or_else(
                    || cwd.join(&l.uri).to_string_lossy().to_string(),
                    str::to_string,
                ),
            })),
            acp::ContentBlock::Resource(res) => {
                if let acp::EmbeddedResourceResource::TextResourceContents(t) = &res.resource {
                    input.push(json!({"type": "text", "text": t.text}));
                }
            }
            acp::ContentBlock::Audio(a) => input.push(json!({
                "type": "audio",
                "data": a.data,
                "format": a.mime_type,
            })),
            _ => {}
        }
    }
    input
}

/// Whether the prompt is exactly one slash command.
fn slash_command(prompt: &[acp::ContentBlock]) -> Option<String> {
    if let [acp::ContentBlock::Text(t)] = prompt {
        t.text.trim().strip_prefix('/').map(str::to_string)
    } else {
        None
    }
}

async fn prompt(
    bridge: Arc<Bridge>,
    req: acp::PromptRequest,
    r: sdk::v1::Responder<acp::PromptResponse>,
    conn: Conn,
) -> Result<(), AcpError> {
    let session = bridge.session(&req.session_id).await?;
    if let Some(cmd) = slash_command(&req.prompt) {
        return run_command(&bridge, &session, &cmd, r, &conn).await;
    }
    let mode = session::find_mode(&session.mode.lock().await).unwrap_or_else(session::default_mode);
    let model = session.model.lock().await.clone();
    let effort = session.effort.lock().await.clone();
    let fast = *session.fast.lock().await;
    let input = prompt_input(&req.prompt, &session.cwd);
    let resp: codex_codes::TurnStartResponse = bridge
        .server
        .call(
            "turn/start",
            &json!({
                "threadId": session.thread_id,
                "input": input,
                "cwd": session.cwd.to_string_lossy(),
                "model": model,
                "effort": effort,
                "serviceTier": fast.then_some("fast"),
                "approvalPolicy": mode.approval_policy,
                "approvalsReviewer": mode.approvals_reviewer,
                "sandboxPolicy": mode.sandbox_policy,
            }),
        )
        .await
        .map_err(err)?;
    let turn_id = resp.turn.id.clone();
    *session.active_turn.lock().await = Some(turn_id);

    let stop = run_turn(&bridge, &session, &conn).await?;
    *session.active_turn.lock().await = None;
    r.respond(acp::PromptResponse::new(stop))
}

enum EventOrCancel {
    Event(AppEvent),
    Cancelled,
}

/// Consume session events until the turn ends; translate + forward them.
async fn run_turn(
    bridge: &Arc<Bridge>,
    session: &Arc<Session>,
    conn: &Conn,
) -> Result<acp::StopReason, AcpError> {
    let turn_id = session.active_turn.lock().await.clone().unwrap_or_default();
    loop {
        let ev = futures_lite::future::or(
            async { session.events.recv().await.map(EventOrCancel::Event) },
            async {
                session
                    .cancel_rx
                    .recv()
                    .await
                    .map(|()| EventOrCancel::Cancelled)
            },
        )
        .await
        .map_err(|_| AcpError::internal_error().data("app-server event channel closed"))?;
        let ev = match ev {
            EventOrCancel::Cancelled => {
                let _ = bridge
                    .server
                    .call::<_, Value>(
                        "turn/interrupt",
                        &json!({"threadId": session.thread_id, "turnId": turn_id}),
                    )
                    .await;
                return Ok(acp::StopReason::Cancelled);
            }
            EventOrCancel::Event(e) => e,
        };
        match ev {
            AppEvent::Notification(n) => {
                if let Some(stop) = forward_notification(&n, session, conn).await? {
                    return Ok(stop);
                }
            }
            AppEvent::Request { id, request } => {
                handle_server_request(bridge, session, id, request, conn).await;
            }
            AppEvent::Unknown { id: Some(id), .. } => {
                let _ = bridge
                    .server
                    .respond_error(&id, -32601, "request unsupported by this bridge")
                    .await;
            }
            AppEvent::Unknown { .. } => {}
        }
    }
}

/// Returns `Some(stop_reason)` when the turn finished.
async fn forward_notification(
    n: &codex_codes::Notification,
    session: &Arc<Session>,
    conn: &Conn,
) -> Result<Option<acp::StopReason>, AcpError> {
    use codex_codes::Notification as N;
    let sid = &session.id;
    match n {
        N::AgentMessageDelta(d) => send_update(conn, sid, translate::agent_delta(&d.delta)).await,
        N::ReasoningDelta(d) => {
            send_update(conn, sid, translate::thought_delta(&d.delta)).await;
        }
        N::ReasoningTextDelta(d) => {
            send_update(conn, sid, translate::thought_delta(&d.delta)).await;
        }
        N::CmdOutputDelta(d) => {
            send_update(conn, sid, translate::output_delta(&d.item_id, &d.delta)).await;
        }
        N::FileChangeOutputDelta(d) => {
            let v = serde_json::to_value(d).unwrap_or_default();
            let delta = v.get("delta").and_then(Value::as_str).unwrap_or_default();
            send_update(conn, sid, translate::output_delta(&d.item_id, delta)).await;
        }
        N::FileChangePatchUpdated(d) => {
            let changes = serde_json::to_value(&d.changes).unwrap_or_default();
            send_update(
                conn,
                sid,
                translate::patch_update(&d.item_id, changes.as_array().map_or(&[], Vec::as_slice)),
            )
            .await;
        }
        N::ItemStarted(n) => {
            let v = serde_json::to_value(&n.item).unwrap_or_default();
            if let Some(u) = translate::item_as_tool_call(&v, acp::ToolCallStatus::InProgress) {
                send_update(conn, sid, u).await;
            }
        }
        N::ItemCompleted(n) => {
            let v = serde_json::to_value(&n.item).unwrap_or_default();
            if let Some(u) = translate::item_completed_update(&v) {
                send_update(conn, sid, u).await;
            }
        }
        N::PlanDelta(d) => send_update(conn, sid, translate::agent_delta(&d.delta)).await,
        N::TurnPlanUpdated(d) => {
            let v = serde_json::to_value(d).unwrap_or_default();
            send_update(conn, sid, plan_update(&v)).await;
        }
        N::TurnCompleted(n) => {
            let v = serde_json::to_value(&n.turn).unwrap_or_default();
            return Ok(Some(translate::stop_reason(&v)));
        }
        N::ThreadTokenUsageUpdated(n) => {
            let v = serde_json::to_value(&n.token_usage).unwrap_or_default();
            send_update(conn, sid, translate::usage_update(&v)).await;
        }
        N::Error(e) => {
            let v = serde_json::to_value(e).unwrap_or_default();
            send_update(conn, sid, notice(acp::NoticeSeverity::Error, &v)).await;
        }
        N::Warning(e) => {
            let v = serde_json::to_value(e).unwrap_or_default();
            send_update(conn, sid, notice(acp::NoticeSeverity::Warning, &v)).await;
        }
        N::ConfigWarning(e) => {
            let v = serde_json::to_value(e).unwrap_or_default();
            send_update(conn, sid, notice(acp::NoticeSeverity::Warning, &v)).await;
        }
        N::GuardianWarning(e) => {
            let v = serde_json::to_value(e).unwrap_or_default();
            send_update(conn, sid, notice(acp::NoticeSeverity::Warning, &v)).await;
        }
        N::ContextCompacted(n) => {
            let v = serde_json::to_value(n).unwrap_or_default();
            send_update(
                conn,
                sid,
                acp::SessionUpdate::Notice(
                    acp::Notice::new(acp::NoticeSeverity::Info, "Context compacted")
                        .description(v.get("turnId").and_then(Value::as_str).map(str::to_string)),
                ),
            )
            .await;
        }
        _ => {}
    }
    Ok(None)
}

fn plan_update(plan: &Value) -> acp::SessionUpdate {
    let entries = plan
        .get("plan")
        .and_then(Value::as_array)
        .or_else(|| plan.get("entries").and_then(Value::as_array))
        .map(|es| {
            es.iter()
                .map(|e| {
                    acp::PlanEntry::new(
                        e.get("step")
                            .or_else(|| e.get("content"))
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_string(),
                        acp::PlanEntryPriority::Medium,
                        match e.get("status").and_then(Value::as_str) {
                            Some("completed") => acp::PlanEntryStatus::Completed,
                            Some("inProgress" | "in_progress") => {
                                acp::PlanEntryStatus::InProgress
                            }
                            _ => acp::PlanEntryStatus::Pending,
                        },
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    acp::SessionUpdate::Plan(acp::Plan::new(entries))
}

fn notice(severity: acp::NoticeSeverity, v: &Value) -> acp::SessionUpdate {
    acp::SessionUpdate::Notice(
        acp::Notice::new(
            severity,
            v.get("message")
                .or_else(|| v.get("error").and_then(|e| e.get("message")))
                .and_then(Value::as_str)
                .unwrap_or("notification")
                .to_string(),
        )
        .description(
            v.get("additionalDetails")
                .and_then(Value::as_str)
                .map(str::to_string),
        ),
    )
}

/// Forward an app-server approval/elicitation request to the ACP client.
async fn handle_server_request(
    bridge: &Arc<Bridge>,
    session: &Arc<Session>,
    id: Value,
    request: codex_codes::ServerRequest,
    conn: &Conn,
) {
    use codex_codes::ServerRequest as R;
    match request {
        R::CmdExecApproval(p) => {
            let raw = serde_json::to_value(&p).unwrap_or_default();
            let decision = ask(conn, &session.id, "Run command?", &raw).await;
            let _ = bridge
                .server
                .respond(
                    &id,
                    &json!({"decision": approval_decision(decision.as_deref())}),
                )
                .await;
        }
        R::ExecCommandApproval(p) => {
            let raw = serde_json::to_value(&p).unwrap_or_default();
            let decision = ask(conn, &session.id, "Run command?", &raw).await;
            let _ = bridge
                .server
                .respond(
                    &id,
                    &json!({"decision": approval_decision(decision.as_deref())}),
                )
                .await;
        }
        R::FileChangeApproval(p) => {
            let raw = serde_json::to_value(&p).unwrap_or_default();
            let decision = ask(conn, &session.id, "Apply file changes?", &raw).await;
            let _ = bridge
                .server
                .respond(
                    &id,
                    &json!({"decision": approval_decision(decision.as_deref())}),
                )
                .await;
        }
        R::ApplyPatchApproval(p) => {
            let raw = serde_json::to_value(&p).unwrap_or_default();
            let decision = ask(conn, &session.id, "Apply file changes?", &raw).await;
            let _ = bridge
                .server
                .respond(
                    &id,
                    &json!({"decision": approval_decision(decision.as_deref())}),
                )
                .await;
        }
        R::McpServerElicitationRequest(_) => {
            let _ = bridge
                .server
                .respond(&id, &json!({"action": "decline"}))
                .await;
        }
        other => {
            debug!(
                ?other,
                "answering unsupported server request with a refusal"
            );
            let _ = bridge
                .server
                .respond_error(&id, -32601, "request unsupported by this bridge")
                .await;
        }
    }
}

/// Map a client permission outcome onto the app-server's `decision` values.
fn approval_decision(option: Option<&str>) -> &'static str {
    match option {
        Some("allow_always") => "acceptForSession",
        Some("allow") => "accept",
        Some("reject" | "reject_always") => "decline",
        _ => "cancel",
    }
}

/// Ask the client for permission; returns the selected option id.
async fn ask(conn: &Conn, session_id: &acp::SessionId, title: &str, raw: &Value) -> Option<String> {
    let options = [
        (
            "allow_always",
            "Always allow",
            acp::PermissionOptionKind::AllowAlways,
        ),
        ("allow", "Allow once", acp::PermissionOptionKind::AllowOnce),
        ("reject", "Reject", acp::PermissionOptionKind::RejectOnce),
        (
            "reject_always",
            "Always reject",
            acp::PermissionOptionKind::RejectAlways,
        ),
    ]
    .into_iter()
    .map(|(id, name, kind)| acp::PermissionOption::new(id, name, kind))
    .collect();
    let tool_call = acp::ToolCallUpdate::new(
        raw.get("itemId")
            .and_then(Value::as_str)
            .unwrap_or("approval")
            .to_string(),
        acp::ToolCallUpdateFields::new()
            .title(title)
            .raw_input(raw.clone()),
    );
    let response = conn
        .send_request(acp::RequestPermissionRequest::new(
            session_id.clone(),
            tool_call,
            options,
        ))
        .block_task()
        .await
        .ok()?;
    match response.outcome {
        acp::RequestPermissionOutcome::Selected(sel) => Some(sel.option_id.0.to_string()),
        _ => None,
    }
}

async fn cancel(bridge: Arc<Bridge>, n: acp::CancelNotification) -> Result<(), AcpError> {
    if let Ok(s) = bridge.session(&n.session_id).await {
        let _ = s.cancel_tx.try_send(());
    }
    Ok(())
}

async fn set_mode(
    bridge: Arc<Bridge>,
    req: acp::SetSessionModeRequest,
    r: sdk::v1::Responder<acp::SetSessionModeResponse>,
) -> Result<(), AcpError> {
    let s = bridge.session(&req.session_id).await?;
    let id = req.mode_id.0.to_string();
    if session::find_mode(&id).is_none() {
        return Err(AcpError::invalid_params().data(format!("unknown mode {id}")));
    }
    *s.mode.lock().await = id;
    r.respond(acp::SetSessionModeResponse::new())
}

async fn set_config_option(
    bridge: Arc<Bridge>,
    req: acp::SetSessionConfigOptionRequest,
    r: sdk::v1::Responder<acp::SetSessionConfigOptionResponse>,
) -> Result<(), AcpError> {
    let s = bridge.session(&req.session_id).await?;
    match req.config_id.0.as_ref() {
        session::MODE_CONFIG_ID => {
            let id = req
                .value
                .as_value_id()
                .map(|v| v.0.to_string())
                .unwrap_or_default();
            if session::find_mode(&id).is_none() {
                return Err(AcpError::invalid_params().data(format!("unknown mode {id}")));
            }
            *s.mode.lock().await = id;
        }
        MODEL_CONFIG_ID => {
            *s.model.lock().await = req.value.as_value_id().map(|v| v.0.to_string());
        }
        EFFORT_CONFIG_ID => {
            *s.effort.lock().await = req.value.as_value_id().map(|v| v.0.to_string());
        }
        FAST_MODE_CONFIG_ID => {
            *s.fast.lock().await = req.value.as_bool().unwrap_or(false);
        }
        other => {
            return Err(AcpError::invalid_params().data(format!("unknown config option {other}")));
        }
    }
    let options = bridge.config_options(&s).await;
    r.respond(acp::SetSessionConfigOptionResponse::new(options))
}

/// `_`-prefixed extension methods, including the codex dialect's legacy names.
async fn ext_request(
    bridge: Arc<Bridge>,
    req: sdk::UntypedMessage,
    r: sdk::v1::Responder<serde_json::Value>,
) -> Result<(), AcpError> {
    let method = req.method.as_ref();
    let params = req.params.clone();
    let session_id = params
        .get("sessionId")
        .and_then(Value::as_str)
        .map(str::to_string);
    let result = match method {
        "session/set_model" => {
            if let Some(sid) = session_id {
                let s = bridge.session(&acp::SessionId::new(sid)).await?;
                *s.model.lock().await = params
                    .get("modelId")
                    .and_then(Value::as_str)
                    .map(str::to_string);
            }
            json!({})
        }
        "_session/steering" | "_codex/session/steering" => {
            if let Some(sid) = session_id {
                let s = bridge.session(&acp::SessionId::new(sid)).await?;
                let text = params
                    .get("message")
                    .or_else(|| params.get("prompt"))
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                if let Some(turn) = s.active_turn.lock().await.clone() {
                    let _: Value = bridge
                        .server
                        .call(
                            "turn/steer",
                            &json!({
                                "threadId": s.thread_id,
                                "expectedTurnId": turn,
                                "input": [{"type": "text", "text": text}],
                            }),
                        )
                        .await
                        .map_err(err)?;
                }
            }
            json!({})
        }
        "authentication/logout" | "logout" => {
            let _: Value = bridge
                .server
                .call("account/logout", &json!({}))
                .await
                .map_err(err)?;
            json!({})
        }
        "authentication/status" => {
            let v: Value = bridge
                .server
                .call("account/read", &json!({}))
                .await
                .map_err(err)?;
            json!({"type": "account", "account": v})
        }
        _ => {
            return Err(AcpError::method_not_found().data(format!("unknown method {method}")));
        }
    };
    r.respond(result)
}

/// A `/`-command prompt: run the codex-side action and report as updates.
async fn run_command(
    bridge: &Arc<Bridge>,
    session: &Arc<Session>,
    cmd: &str,
    r: sdk::v1::Responder<acp::PromptResponse>,
    conn: &Conn,
) -> Result<(), AcpError> {
    let (name, args) = cmd.split_once(' ').unwrap_or((cmd, ""));
    match name {
        "compact" => {
            let _: Value = bridge
                .server
                .call(
                    "thread/compact/start",
                    &json!({"threadId": session.thread_id}),
                )
                .await
                .map_err(err)?;
            r.respond(acp::PromptResponse::new(acp::StopReason::EndTurn))
        }
        "logout" => {
            let _: Value = bridge
                .server
                .call("account/logout", &json!({}))
                .await
                .map_err(err)?;
            r.respond(acp::PromptResponse::new(acp::StopReason::EndTurn))
        }
        "status" => {
            let v: Value = bridge
                .server
                .call("account/read", &json!({}))
                .await
                .unwrap_or_default();
            send_update(
                conn,
                &session.id,
                acp::SessionUpdate::AgentMessageChunk(acp::ContentChunk::new(
                    acp::ContentBlock::Text(acp::TextContent::new(format!("Account: {v}"))),
                )),
            )
            .await;
            r.respond(acp::PromptResponse::new(acp::StopReason::EndTurn))
        }
        "review" | "review-branch" | "review-commit" => {
            let target = match name {
                "review-branch" => json!({"type": "baseBranch", "branch": args.trim()}),
                "review-commit" => json!({"type": "commit", "sha": args.trim()}),
                _ => json!({"type": "uncommittedChanges"}),
            };
            let resp: codex_codes::TurnStartResponse = bridge
                .server
                .call(
                    "review/start",
                    &json!({"threadId": session.thread_id, "target": target}),
                )
                .await
                .map_err(err)?;
            *session.active_turn.lock().await = Some(resp.turn.id);
            let stop = run_turn(bridge, session, conn).await?;
            *session.active_turn.lock().await = None;
            r.respond(acp::PromptResponse::new(stop))
        }
        _ => r.respond(acp::PromptResponse::new(acp::StopReason::EndTurn)),
    }
}
