//! The Codex app-server edge: spawning the process and speaking its
//! ndjson JSON-RPC protocol.

use std::collections::HashMap;
use std::sync::Arc;

use async_channel::{Receiver, Sender};
use async_lock::Mutex;
use futures_lite::{AsyncRead, AsyncWrite};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use thiserror::Error;
use tracing::{debug, warn};

use codex_codes::{ServerMessage, ServerRequest};

use crate::jsonrpc::{Connection, Incoming, RemoteError};

/// Errors the bridge can hit on the app-server side.
#[derive(Debug, Error)]
#[allow(clippy::result_large_err)]
pub enum Error {
    /// Transport-level failure.
    #[error(transparent)]
    Connection(#[from] crate::jsonrpc::Error),
    /// Failed to spawn the `codex` binary.
    #[error("failed to spawn codex: {0}")]
    Spawn(#[source] std::io::Error),
    /// The app-server handshake did not complete.
    #[error("app-server initialize failed: {0}")]
    Initialize(#[source] crate::jsonrpc::Error),
    /// An inbound frame could not be decoded.
    #[error("protocol decode failed: {0}")]
    Decode(#[from] codex_codes::Error),
    /// A payload could not be (de)serialized.
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
}

/// One app-server-originated event after demultiplexing.
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum AppEvent {
    /// A server notification, decoded into the typed enum.
    Notification(codex_codes::Notification),
    /// A server request awaiting our response.
    Request {
        /// The id to answer via [`AppServer::respond`].
        id: Value,
        /// Decoded request payload.
        request: ServerRequest,
    },
    /// The method or params were not modeled; raw frames preserved.
    Unknown {
        /// Method name (or `None` when not a notification).
        method: Option<String>,
        /// Raw params value.
        params: Value,
        /// Request id, when the frame was a request.
        id: Option<Value>,
    },
}

/// How the `codex` binary is launched.
#[derive(Debug, Clone, Default)]
pub struct SpawnConfig {
    /// Explicit path to the `codex` executable. `None` resolves `codex` on
    /// `PATH` (or `CODEX_PATH` in the environment).
    pub codex_path: Option<std::path::PathBuf>,
    /// Extra arguments appended after `app-server` — typically
    /// `-c key=value` config overrides.
    pub args: Vec<String>,
    /// Extra environment variables for the child.
    pub env: Vec<(String, String)>,
    /// Client name/version reported in `initialize`.
    pub client_name: String,
    /// Client version reported in `initialize`.
    pub client_version: String,
    /// Opt into the app-server's experimental API surface.
    pub experimental_api: bool,
}

/// A running app-server connection.
///
/// Events are demultiplexed by thread id: each [`subscribe`](Self::subscribe)
/// receiver gets only the frames for its thread, plus requests the session
/// must answer (approvals). Frames with no thread id go to
/// [`global_events`](Self::global_events).
pub struct AppServer {
    conn: Connection<Box<dyn AsyncWrite + Unpin + Send>>,
    subscribers: Mutex<HashMap<String, Sender<AppEvent>>>,
    global_tx: Sender<AppEvent>,
    global_rx: Receiver<AppEvent>,
    /// Keep-alive handle for the spawned child, when we own it.
    #[cfg(feature = "process")]
    child: Mutex<Option<async_process::Child>>,
}

impl AppServer {
    /// Attach to an already-connected pair of app-server streams.
    ///
    /// The returned future must be polled to pump inbound frames into the
    /// demultiplexer — and must be running for [`Self::handshake`] and any
    /// request to resolve, since replies only arrive through it. When it
    /// finishes the connection is dead.
    pub fn attach<R, W>(
        read: R,
        write: W,
    ) -> (Arc<Self>, impl Future<Output = ()> + Send + use<R, W>)
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let (conn, reader) =
            Connection::new(read, Box::new(write) as Box<dyn AsyncWrite + Unpin + Send>);
        let (global_tx, global_rx) = async_channel::unbounded();
        let server = Arc::new(Self {
            conn,
            subscribers: Mutex::new(HashMap::new()),
            global_tx,
            global_rx,
            #[cfg(feature = "process")]
            child: Mutex::new(None),
        });
        let events = server.conn.events();
        let demux = {
            let server = server.clone();
            async move {
                let consume = async move {
                    while let Ok(msg) = events.recv().await {
                        route(&server, msg).await;
                    }
                };
                futures_lite::future::or(reader, consume).await;
            }
        };
        (server, demux)
    }

    /// Spawn `codex app-server` and attach to it.
    ///
    /// # Errors
    ///
    /// Fails to spawn or to complete the handshake.
    ///
    /// # Panics
    ///
    /// Panics when the child's piped stdio is missing (cannot happen in
    /// practice — it is configured right above).
    #[allow(clippy::result_large_err)]
    #[cfg(feature = "process")]
    pub async fn spawn(
        config: &SpawnConfig,
    ) -> Result<(Arc<Self>, impl Future<Output = ()> + Send + use<>), Error> {
        let path = config
            .codex_path
            .clone()
            .or_else(|| std::env::var_os("CODEX_PATH").map(Into::into))
            .unwrap_or_else(|| "codex".into());
        let mut cmd = async_process::Command::new(path);
        cmd.arg("app-server")
            .args(&config.args)
            .envs(config.env.iter().cloned())
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit());
        let mut child = cmd.spawn().map_err(Error::Spawn)?;
        let stdout = child.stdout.take().expect("stdout piped");
        let stdin = child.stdin.take().expect("stdin piped");
        let (server, demux) = Self::attach(stdout, stdin);
        *server.child.lock().await = Some(child);
        Ok((server, demux))
    }

    /// Perform the `initialize`/`initialized` handshake.
    ///
    /// The drive future returned by [`Self::attach`]/[`Self::spawn`] must be
    /// polled concurrently for this to resolve.
    ///
    /// # Errors
    ///
    /// Fails when the peer refuses or the connection drops mid-handshake.
    #[allow(clippy::result_large_err, clippy::future_not_send)]
    pub async fn handshake(&self, config: &SpawnConfig) -> Result<(), Error> {
        let params = serde_json::json!({
            "clientInfo": {
                "name": config.client_name,
                "version": config.client_version,
            },
            "capabilities": {
                "experimentalApi": config.experimental_api,
            },
        });
        self.conn
            .request("initialize", &params)
            .await
            .map_err(Error::Initialize)?;
        self.conn
            .notify("initialized", &serde_json::json!({}))
            .await
            .map_err(Error::Initialize)?;
        Ok(())
    }

    /// Send a typed request and decode the result into `R`.
    ///
    /// # Errors
    ///
    /// Propagates connection errors and decode failures.
    #[allow(clippy::result_large_err, clippy::future_not_send)]
    pub async fn call<P: Serialize + Sync, R: DeserializeOwned>(
        &self,
        method: &str,
        params: &P,
    ) -> Result<R, Error> {
        let value = self.conn.request(method, params).await?;
        Ok(serde_json::from_value(value)?)
    }

    /// Send a notification.
    ///
    /// # Errors
    ///
    /// Propagates write failures.
    #[allow(clippy::result_large_err, clippy::future_not_send)]
    pub async fn notify<P: Serialize + Sync>(&self, method: &str, params: &P) -> Result<(), Error> {
        self.conn.notify(method, params).await?;
        Ok(())
    }

    /// Answer a server request (approval, elicitation, ...).
    ///
    /// # Errors
    ///
    /// Propagates serialization/write failures.
    #[allow(clippy::result_large_err, clippy::future_not_send)]
    pub async fn respond<P: Serialize + Sync>(&self, id: &Value, result: &P) -> Result<(), Error> {
        self.conn
            .respond(id, Ok(serde_json::to_value(result)?))
            .await?;
        Ok(())
    }

    /// Answer a server request with a JSON-RPC error.
    ///
    /// # Errors
    ///
    /// Propagates write failures.
    #[allow(clippy::result_large_err, clippy::future_not_send)]
    pub async fn respond_error(
        &self,
        id: &Value,
        code: i64,
        message: impl Into<String>,
    ) -> Result<(), Error> {
        self.conn
            .respond(
                id,
                Err(RemoteError {
                    code,
                    message: message.into(),
                    data: None,
                }),
            )
            .await?;
        Ok(())
    }

    /// Subscribe to the events of one thread.
    ///
    /// The thread id is known only after `thread/start` returns, so callers
    /// register it once the session is created. Events arriving before the
    /// subscription may be missed; subscribe right after the call resolves.
    #[allow(clippy::future_not_send)]
    pub async fn subscribe(&self, thread_id: impl Into<String>) -> Receiver<AppEvent> {
        let (tx, rx) = async_channel::unbounded();
        self.subscribers.lock().await.insert(thread_id.into(), tx);
        rx
    }

    /// Drop a thread's subscription.
    #[allow(clippy::future_not_send)]
    pub async fn unsubscribe(&self, thread_id: &str) {
        self.subscribers.lock().await.remove(thread_id);
    }

    /// Events that belong to no thread (account, mcp startup, auth).
    #[must_use]
    pub fn global_events(&self) -> Receiver<AppEvent> {
        self.global_rx.clone()
    }

    /// True when the app-server's stdout has ended.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.conn.is_closed()
    }
}

/// Extract a `threadId` from a params object, if present.
fn thread_id_of(params: &Value) -> Option<&str> {
    params
        .get("threadId")
        .or_else(|| params.get("conversationId"))?
        .as_str()
}

async fn route(server: &AppServer, msg: Incoming) {
    let (event, thread_id) = match msg {
        Incoming::Notification { method, params } => {
            let thread = thread_id_of(&params).map(str::to_string);
            let frame = serde_json::json!({"method": method, "params": params});
            let event = match ServerMessage::from_value(frame.clone()) {
                Ok(ServerMessage::Notification(n)) => AppEvent::Notification(n),
                Ok(_) => AppEvent::Unknown {
                    method: Some(method),
                    params,
                    id: None,
                },
                Err(err) => {
                    debug!(%err, "undecoded notification");
                    AppEvent::Unknown {
                        method: frame
                            .get("method")
                            .and_then(Value::as_str)
                            .map(str::to_string),
                        params: frame.get("params").cloned().unwrap_or(Value::Null),
                        id: None,
                    }
                }
            };
            (event, thread)
        }
        Incoming::Request { id, method, params } => {
            let thread = thread_id_of(&params).map(str::to_string);
            let event = match ServerRequest::from_envelope(&method, Some(params.clone())) {
                Ok(request) => AppEvent::Request { id, request },
                Err(err) => {
                    warn!(%err, %method, "undecoded server request");
                    AppEvent::Unknown {
                        method: Some(method),
                        params,
                        id: Some(id),
                    }
                }
            };
            (event, thread)
        }
    };
    let tx = match thread_id {
        Some(tid) => server.subscribers.lock().await.get(&tid).cloned(),
        None => None,
    };
    if let Some(tx) = tx {
        let _ = tx.send(event).await;
    } else {
        let _ = server.global_tx.send(event).await;
    }
}
