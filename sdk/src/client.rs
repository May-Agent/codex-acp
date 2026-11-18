//! `AppServer` client, child process supervisor, per-thread notification
//! demultiplexer, and typed `codex app-server` RPC helpers.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::future::{Future, poll_fn};
use std::io::ErrorKind;
use std::pin::Pin;
use std::process::ExitStatus;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use std::task::Poll;

use async_channel::{Receiver, Sender};
use codex_codes::{
    GetAccountParams, GetAccountResponse, InitializeParams, InitializeResponse, LoginAccountParams,
    LoginAccountResponse, LogoutAccountResponse, ModelListParams, ModelListResponse, Notification,
    RequestId, ReviewStartParams, ReviewStartResponse, ServerRequest, ThreadCompactStartParams,
    ThreadCompactStartResponse, ThreadDeleteParams, ThreadDeleteResponse, ThreadForkParams,
    ThreadForkResponse, ThreadListParams, ThreadListResponse, ThreadReadParams, ThreadReadResponse,
    ThreadResumeParams, ThreadResumeResponse, ThreadStartParams, ThreadStartResponse,
    ThreadUnsubscribeParams, ThreadUnsubscribeResponse, TurnInterruptParams, TurnInterruptResponse,
    TurnStartParams, TurnStartResponse, TurnSteerParams, TurnSteerResponse, protocol::methods,
};
use futures_lite::future::{or, zip};
use futures_lite::io::{AsyncRead, AsyncWrite};
#[cfg(feature = "process")]
use futures_lite::io::{AsyncReadExt, BufReader};
use futures_lite::stream::Stream;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use tracing::{debug, warn};

use crate::config::SpawnConfig;
use crate::error::{RemoteError, Result, SdkError};
use crate::handler::ServerRequestHandler;
use crate::jsonrpc::{Connection, Incoming};

/// Maximum stderr tail retained in memory (16 KiB) for non-zero exit diagnostics.
#[cfg(feature = "process")]
const MAX_STDERR_TAIL_BYTES: usize = 16 * 1024;

/// Maximum unsubscribed thread backlogs retained before evicting the oldest.
const MAX_EARLY_THREADS: usize = 64;

/// Maximum buffered notifications retained per unsubscribed thread before a
/// caller invokes [`AppServer::subscribe`].
const MAX_EARLY_EVENTS_PER_THREAD: usize = 256;

/// Inbound server notification routed to a thread subscription or
/// [`AppServer::global_events`].
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum ThreadEvent {
    /// Typed notification decoded via [`codex_codes::Notification`] (including
    /// [`Notification::Unknown`] for unmodeled notification methods).
    Notification(Notification),
    /// Notification whose method is modeled by `codex-codes` but whose payload
    /// could not be deserialized into the generated schema.
    RawNotification {
        /// JSON-RPC notification method name.
        method: String,
        /// Raw JSON `params` payload (`Value::Null` when absent).
        params: Value,
    },
}

impl ThreadEvent {
    /// Wire `method` name of this notification.
    #[must_use]
    pub fn method(&self) -> &str {
        match self {
            Self::Notification(n) => n.method(),
            Self::RawNotification { method, .. } => method,
        }
    }

    /// Turn identifier associated with this notification, if turn-scoped.
    #[must_use]
    pub fn turn_id(&self) -> Option<&str> {
        match self {
            Self::Notification(n) => n.turn_id(),
            Self::RawNotification { params, .. } => params
                .get("turnId")
                .or_else(|| params.get("turn").and_then(|t| t.get("id")))
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty()),
        }
    }

    /// Borrow the underlying typed [`Notification`], if decoded.
    #[must_use]
    pub const fn as_notification(&self) -> Option<&Notification> {
        match self {
            Self::Notification(n) => Some(n),
            Self::RawNotification { .. } => None,
        }
    }

    /// Consume `self` and return the underlying typed [`Notification`], if decoded.
    #[must_use]
    pub fn into_notification(self) -> Option<Notification> {
        match self {
            Self::Notification(n) => Some(n),
            Self::RawNotification { .. } => None,
        }
    }
}

/// Scoped per-thread notification subscription that automatically unsubscribes
/// from [`AppServer`] when dropped.
#[derive(Debug)]
pub struct ThreadSubscription {
    server: AppServer,
    thread_id: String,
    rx: Receiver<ThreadEvent>,
}

impl ThreadSubscription {
    /// Subscribed thread identifier.
    #[must_use]
    pub fn thread_id(&self) -> &str {
        &self.thread_id
    }

    /// Underlying [`Receiver`] for this thread's events.
    #[must_use]
    pub const fn receiver(&self) -> &Receiver<ThreadEvent> {
        &self.rx
    }

    /// Receive the next [`ThreadEvent`] for this thread, or `None` when closed.
    pub async fn recv(&self) -> Option<ThreadEvent> {
        self.rx.recv().await.ok()
    }
}

impl Drop for ThreadSubscription {
    fn drop(&mut self) {
        self.server.unsubscribe(&self.thread_id);
    }
}

#[derive(Debug, Clone)]
struct ExitRecord {
    status: Option<ExitStatus>,
    stderr: String,
}

struct DemuxState {
    closed: bool,
    subscribers: HashMap<String, Sender<ThreadEvent>>,
    early_buffers: HashMap<String, VecDeque<ThreadEvent>>,
    early_order: VecDeque<String>,
}

impl DemuxState {
    fn new() -> Self {
        Self {
            closed: false,
            subscribers: HashMap::new(),
            early_buffers: HashMap::new(),
            early_order: VecDeque::new(),
        }
    }

    fn subscribe(&mut self, thread_id: String) -> Receiver<ThreadEvent> {
        let (tx, rx) = async_channel::unbounded();
        if let Some(buffered) = self.early_buffers.remove(&thread_id) {
            self.early_order.retain(|id| id != &thread_id);
            for ev in buffered {
                let _ = tx.try_send(ev);
            }
        }
        if self.closed {
            tx.close();
        } else if let Some(prev) = self.subscribers.insert(thread_id, tx) {
            prev.close();
        }
        rx
    }

    fn unsubscribe(&mut self, thread_id: &str) {
        if let Some(tx) = self.subscribers.remove(thread_id) {
            tx.close();
        }
        if self.early_buffers.remove(thread_id).is_some() {
            self.early_order.retain(|id| id != thread_id);
        }
    }

    fn route_thread_event(&mut self, thread_id: String, event: ThreadEvent) {
        if let Some(tx) = self.subscribers.get(&thread_id) {
            if tx.try_send(event).is_ok() {
                return;
            }
            self.subscribers.remove(&thread_id);
            return;
        }
        if self.closed {
            return;
        }
        if !self.early_buffers.contains_key(&thread_id) {
            while self.early_buffers.len() >= MAX_EARLY_THREADS {
                let Some(oldest) = self.early_order.pop_front() else {
                    break;
                };
                self.early_buffers.remove(&oldest);
            }
            self.early_order.push_back(thread_id.clone());
        }
        let queue = self.early_buffers.entry(thread_id).or_default();
        if queue.len() >= MAX_EARLY_EVENTS_PER_THREAD {
            let _ = queue.pop_front();
        }
        queue.push_back(event);
    }

    fn close_all(&mut self) {
        self.closed = true;
        for (_, tx) in self.subscribers.drain() {
            tx.close();
        }
        self.early_buffers.clear();
        self.early_order.clear();
    }
}

struct Inner {
    conn: Connection<Box<dyn AsyncWrite + Unpin + Send>>,
    demux: StdMutex<DemuxState>,
    global_tx: Sender<ThreadEvent>,
    global_rx: Receiver<ThreadEvent>,
    client_closed: AtomicBool,
    close_signal_tx: Sender<()>,
    has_child: bool,
    exit_record: OnceLock<Option<ExitRecord>>,
    exit_ready_rx: Receiver<()>,
}

impl Inner {
    fn is_client_closed(&self) -> bool {
        self.client_closed.load(Ordering::Acquire)
    }

    fn finish_close(&self) {
        self.conn.mark_closed();
        if let Ok(mut demux) = self.demux.lock() {
            demux.close_all();
        }
        self.global_tx.close();
    }

    fn route_notification(&self, method: String, params: Option<Value>) {
        let thread_id = extract_thread_id(params.as_ref()).map(ToOwned::to_owned);
        let event = match Notification::from_envelope(&method, params.clone()) {
            Ok(notification) => ThreadEvent::Notification(notification),
            Err(err) => {
                debug!(%err, %method, "preserving raw notification after schema mismatch");
                ThreadEvent::RawNotification {
                    method,
                    params: params.unwrap_or(Value::Null),
                }
            }
        };

        if let Some(tid) = thread_id {
            if let Ok(mut demux) = self.demux.lock() {
                demux.route_thread_event(tid, event);
            }
        } else {
            let _ = self.global_tx.try_send(event);
        }
    }

    fn exit_error(&self) -> Option<SdkError> {
        let record = self.exit_record.get()?.as_ref()?;
        Some(SdkError::ProcessExited {
            status: record.status,
            stderr: record.stderr.clone(),
        })
    }
}

/// Cloneable client handle to a running `codex app-server` session.
///
/// Construct via [`AppServer::attach`] over arbitrary [`AsyncRead`]/[`AsyncWrite`]
/// streams or [`AppServer::spawn`] (`process` feature) to launch a supervised
/// `codex app-server` child process. Poll or spawn the companion driver future
/// on your executor so inbound frames and server requests are pumped.
#[derive(Clone)]
pub struct AppServer {
    inner: Arc<Inner>,
}

impl fmt::Debug for AppServer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AppServer")
            .field("closed", &self.is_closed())
            .field("pending_count", &self.pending_count())
            .finish_non_exhaustive()
    }
}

impl AppServer {
    /// Attach to an already-connected pair of `codex app-server` streams.
    ///
    /// Returns `(server, driver)`. Poll or spawn `driver` concurrently so
    /// responses, notifications, and [`ServerRequestHandler`] callbacks are
    /// processed.
    pub fn attach<R, W, H>(
        read: R,
        write: W,
        handler: H,
    ) -> (Self, impl Future<Output = ()> + Send + use<R, W, H>)
    where
        R: AsyncRead + Unpin + Send,
        W: AsyncWrite + Unpin + Send + 'static,
        H: ServerRequestHandler,
    {
        let boxed_write: Box<dyn AsyncWrite + Unpin + Send> = Box::new(write);
        let (conn, reader_fut) = Connection::new_deferred(read, boxed_write);
        let (global_tx, global_rx) = async_channel::unbounded();
        let (close_signal_tx, close_signal_rx) = async_channel::bounded(1);
        let (exit_ready_tx, exit_ready_rx) = async_channel::bounded(1);
        exit_ready_tx.close();

        let inner = Arc::new(Inner {
            conn,
            demux: StdMutex::new(DemuxState::new()),
            global_tx,
            global_rx,
            client_closed: AtomicBool::new(false),
            close_signal_tx,
            has_child: false,
            exit_record: OnceLock::from(None),
            exit_ready_rx,
        });

        let events = inner.conn.events();
        let driver_inner = Arc::clone(&inner);
        let handler = Arc::new(handler);

        let driver = async move {
            let pump = async {
                let read_task = async {
                    if let Err(err) = reader_fut.await {
                        debug!(%err, "attached app-server stream ended with read error");
                    }
                    driver_inner.conn.mark_closed();
                };
                let dispatch_task =
                    drive_events(Arc::clone(&driver_inner), events, Arc::clone(&handler));
                let _ = zip(read_task, dispatch_task).await;
            };

            or(pump, async {
                let _ = close_signal_rx.recv().await;
            })
            .await;

            if driver_inner.is_client_closed() {
                let _ = driver_inner.conn.close().await;
            }
            driver_inner.finish_close();
        };

        (Self { inner }, driver)
    }

    /// Spawn `codex app-server` from `config` and attach an [`AppServer`] to its
    /// piped `stdin`, `stdout`, and `stderr`.
    ///
    /// Up to 16 KiB of trailing `stderr` output is captured asynchronously; if
    /// the process exits with a non-zero status (without an explicit
    /// [`Self::close`]), pending and subsequent RPC calls fail with
    /// [`SdkError::ProcessExited`] carrying the exit status and `stderr` tail.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::Io`] if spawning the `codex` process fails or
    /// [`SdkError::Protocol`] if piped stdio handles are unavailable.
    #[cfg(feature = "process")]
    pub fn spawn<H>(
        config: &SpawnConfig,
        handler: H,
    ) -> Result<(Self, impl Future<Output = ()> + Send + use<H>)>
    where
        H: ServerRequestHandler,
    {
        let program = config.resolve_executable();
        let args = config.build_args();
        debug!(program = %program.display(), ?args, "spawning codex app-server");

        let mut cmd = async_process::Command::new(&program);
        cmd.args(&args)
            .envs(&config.env)
            .stdin(async_process::Stdio::piped())
            .stdout(async_process::Stdio::piped())
            .stderr(async_process::Stdio::piped())
            .kill_on_drop(true);

        if let Some(cwd) = &config.cwd {
            cmd.current_dir(cwd);
        }

        let mut child = cmd.spawn().map_err(SdkError::Io)?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| SdkError::Protocol("spawned child missing piped stdin".to_string()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| SdkError::Protocol("spawned child missing piped stdout".to_string()))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| SdkError::Protocol("spawned child missing piped stderr".to_string()))?;

        let boxed_write: Box<dyn AsyncWrite + Unpin + Send> = Box::new(stdin);
        let (conn, reader_fut) = Connection::new_deferred(stdout, boxed_write);
        let (global_tx, global_rx) = async_channel::unbounded();
        let (close_signal_tx, close_signal_rx) = async_channel::bounded(1);
        let (exit_ready_tx, exit_ready_rx) = async_channel::bounded(1);

        let inner = Arc::new(Inner {
            conn,
            demux: StdMutex::new(DemuxState::new()),
            global_tx,
            global_rx,
            client_closed: AtomicBool::new(false),
            close_signal_tx,
            has_child: true,
            exit_record: OnceLock::new(),
            exit_ready_rx,
        });

        let events = inner.conn.events();
        let driver_inner = Arc::clone(&inner);
        let handler = Arc::new(handler);

        let driver = async move {
            run_spawned_driver(
                &mut child,
                reader_fut,
                events,
                stderr,
                handler,
                &close_signal_rx,
                &driver_inner,
            )
            .await;
            exit_ready_tx.close();
        };

        Ok((Self { inner }, driver))
    }

    /// Request immediate shutdown of the connection (and termination of the
    /// spawned child process, if any).
    ///
    /// Pending RPC calls immediately resolve to [`SdkError::Closed`], event
    /// receivers close, and the companion driver future terminates the direct
    /// child process without waiting on inherited grandchild pipe descriptors.
    pub fn close(&self) {
        self.inner.client_closed.store(true, Ordering::Release);
        self.inner.finish_close();
        self.inner.close_signal_tx.close();
    }

    /// Asynchronously close the connection, flushing and closing the outbound
    /// stream in addition to [`Self::close`].
    ///
    /// # Errors
    ///
    /// Propagates underlying writer close errors, if any.
    pub async fn shutdown(&self) -> Result<()> {
        self.close();
        self.inner.conn.close().await
    }

    /// Returns `true` once the connection has closed.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.inner.conn.is_closed()
    }

    /// Number of currently in-flight outbound JSON-RPC requests.
    #[must_use]
    pub fn pending_count(&self) -> usize {
        self.inner.conn.pending_count()
    }

    /// Return [`SdkError::ProcessExited`] if a spawned child process exited
    /// unexpectedly with a non-zero status.
    #[must_use]
    pub fn terminal_error(&self) -> Option<SdkError> {
        self.inner.exit_error()
    }

    /// Subscribe to notifications scoped to `thread_id` (`threadId`,
    /// `conversationId`, or `thread.id`).
    ///
    /// Any early notifications that arrived for `thread_id` before `subscribe`
    /// was called (for example `thread/started` emitted concurrently with a
    /// `thread/start` response) are delivered at the head of the returned
    /// receiver in wire order.
    #[must_use]
    pub fn subscribe(&self, thread_id: impl Into<String>) -> Receiver<ThreadEvent> {
        let tid = thread_id.into();
        let Ok(mut demux) = self.inner.demux.lock() else {
            let (tx, rx) = async_channel::unbounded();
            tx.close();
            return rx;
        };
        demux.subscribe(tid)
    }

    /// Subscribe to `thread_id` with an RAII [`ThreadSubscription`] guard that
    /// automatically unsubscribes on drop.
    #[must_use]
    pub fn subscribe_scoped(&self, thread_id: impl Into<String>) -> ThreadSubscription {
        let tid = thread_id.into();
        let rx = self.subscribe(tid.clone());
        ThreadSubscription {
            server: self.clone(),
            thread_id: tid,
            rx,
        }
    }

    /// Remove the local notification subscription for `thread_id`.
    pub fn unsubscribe(&self, thread_id: &str) {
        if let Ok(mut demux) = self.inner.demux.lock() {
            demux.unsubscribe(thread_id);
        }
    }

    /// Cloneable receiver for global notifications carrying no thread identifier
    /// (account events, MCP startup updates, configuration warnings, etc.).
    #[must_use]
    pub fn global_events(&self) -> Receiver<ThreadEvent> {
        self.inner.global_rx.clone()
    }

    /// Perform the `initialize` request followed by the `initialized`
    /// notification using metadata from `config`.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::ProcessExited`] if a spawned child process exited
    /// before completing the handshake, or [`SdkError::Initialize`] if the peer
    /// rejected initialization or closed mid-handshake.
    pub async fn handshake(&self, config: &SpawnConfig) -> Result<Value> {
        let params = json!({
            "clientInfo": {
                "name": config.client_name,
                "version": config.client_version,
            },
            "capabilities": {
                "experimentalApi": config.experimental_api,
            },
        });
        let resp = self
            .call::<_, Value>(methods::INITIALIZE, &params)
            .await
            .map_err(wrap_handshake_error)?;
        self.notify(methods::INITIALIZED, &json!({}))
            .await
            .map_err(wrap_handshake_error)?;
        Ok(resp)
    }

    /// Typed `initialize` + `initialized` handshake returning [`InitializeResponse`].
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::ProcessExited`] if a spawned child process exited or
    /// [`SdkError::Initialize`] if initialization failed.
    pub async fn initialize(&self, params: &InitializeParams) -> Result<InitializeResponse> {
        let resp = self
            .call::<_, InitializeResponse>(methods::INITIALIZE, params)
            .await
            .map_err(wrap_handshake_error)?;
        self.notify(methods::INITIALIZED, &json!({}))
            .await
            .map_err(wrap_handshake_error)?;
        Ok(resp)
    }

    /// Send an arbitrary JSON-RPC request (`method`, `params`) and deserialize
    /// the `result` payload into `R`.
    ///
    /// Dropping the returned future cancels the pending correlation entry cleanly.
    ///
    /// # Errors
    ///
    /// Propagates transport, remote JSON-RPC, child-exit, or JSON decode errors.
    pub fn call<P, R>(
        &self,
        method: &str,
        params: &P,
    ) -> impl Future<Output = Result<R>> + Send + use<'_, P, R>
    where
        P: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        let req_fut = self.inner.conn.request(method, params);
        async move {
            let value = match req_fut.await {
                Ok(v) => v,
                Err(err) => return Err(self.upgrade_transport_error(err).await),
            };
            serde_json::from_value(value).map_err(SdkError::Json)
        }
    }

    /// Send a one-way JSON-RPC notification (`method`, `params`).
    ///
    /// # Errors
    ///
    /// Propagates serialization, child-exit, or write errors.
    pub fn notify<P>(
        &self,
        method: &str,
        params: &P,
    ) -> impl Future<Output = Result<()>> + Send + use<'_, P>
    where
        P: Serialize + ?Sized,
    {
        let notify_fut = self.inner.conn.notify(method, params);
        async move {
            match notify_fut.await {
                Ok(()) => Ok(()),
                Err(err) => Err(self.upgrade_transport_error(err).await),
            }
        }
    }

    /// Send a JSON-RPC success response for an inbound server request `id`.
    ///
    /// # Errors
    ///
    /// Propagates serialization, child-exit, or write errors.
    pub fn respond<R>(
        &self,
        id: &RequestId,
        result: &R,
    ) -> impl Future<Output = Result<()>> + Send + use<'_, R>
    where
        R: Serialize + ?Sized,
    {
        let encoded = serde_json::to_value(result).map_err(SdkError::Json);
        let id = id.clone();
        async move {
            let value = encoded?;
            match self.inner.conn.respond(&id, Ok(value)).await {
                Ok(()) => Ok(()),
                Err(err) => Err(self.upgrade_transport_error(err).await),
            }
        }
    }

    /// Send a JSON-RPC error response for an inbound server request `id`.
    ///
    /// # Errors
    ///
    /// Propagates child-exit or write errors.
    pub fn respond_error<M>(
        &self,
        id: &RequestId,
        code: i64,
        message: M,
    ) -> impl Future<Output = Result<()>> + Send + use<'_, M>
    where
        M: Into<String>,
    {
        let err = RemoteError::new(code, message);
        let resp_fut = self.inner.conn.respond(id, Err(err));
        async move {
            match resp_fut.await {
                Ok(()) => Ok(()),
                Err(err) => Err(self.upgrade_transport_error(err).await),
            }
        }
    }

    /// Start a new conversation thread (`thread/start`).
    ///
    /// # Errors
    ///
    /// Propagates RPC or decode errors from [`Self::call`].
    pub async fn thread_start(&self, params: &ThreadStartParams) -> Result<ThreadStartResponse> {
        self.call(methods::THREAD_START, params).await
    }

    /// Resume a persisted conversation thread (`thread/resume`).
    ///
    /// # Errors
    ///
    /// Propagates RPC or decode errors from [`Self::call`].
    pub async fn thread_resume(&self, params: &ThreadResumeParams) -> Result<ThreadResumeResponse> {
        self.call(methods::THREAD_RESUME, params).await
    }

    /// Fork an existing conversation thread (`thread/fork`).
    ///
    /// # Errors
    ///
    /// Propagates RPC or decode errors from [`Self::call`].
    pub async fn thread_fork(&self, params: &ThreadForkParams) -> Result<ThreadForkResponse> {
        self.call(methods::THREAD_FORK, params).await
    }

    /// Read metadata and optional turn history for a thread (`thread/read`).
    ///
    /// # Errors
    ///
    /// Propagates RPC or decode errors from [`Self::call`].
    pub async fn thread_read(&self, params: &ThreadReadParams) -> Result<ThreadReadResponse> {
        self.call(methods::THREAD_READ, params).await
    }

    /// List persisted threads (`thread/list`).
    ///
    /// # Errors
    ///
    /// Propagates RPC or decode errors from [`Self::call`].
    pub async fn thread_list(&self, params: &ThreadListParams) -> Result<ThreadListResponse> {
        self.call(methods::THREAD_LIST, params).await
    }

    /// Permanently delete a thread (`thread/delete`).
    ///
    /// # Errors
    ///
    /// Propagates RPC or decode errors from [`Self::call`].
    pub async fn thread_delete(&self, params: &ThreadDeleteParams) -> Result<ThreadDeleteResponse> {
        let resp = self.call(methods::THREAD_DELETE, params).await?;
        self.unsubscribe(&params.thread_id);
        Ok(resp)
    }

    /// Unsubscribe the app-server connection from a loaded thread
    /// (`thread/unsubscribe`) and remove any local subscription.
    ///
    /// # Errors
    ///
    /// Propagates RPC or decode errors from [`Self::call`].
    pub async fn thread_unsubscribe(
        &self,
        params: &ThreadUnsubscribeParams,
    ) -> Result<ThreadUnsubscribeResponse> {
        let resp = self.call(methods::THREAD_UNSUBSCRIBE, params).await?;
        self.unsubscribe(&params.thread_id);
        Ok(resp)
    }

    /// Trigger context compaction on a thread (`thread/compact/start`).
    ///
    /// # Errors
    ///
    /// Propagates RPC or decode errors from [`Self::call`].
    pub async fn thread_compact_start(
        &self,
        params: &ThreadCompactStartParams,
    ) -> Result<ThreadCompactStartResponse> {
        self.call(methods::THREAD_COMPACT_START, params).await
    }

    /// Start a new turn on an active thread (`turn/start`).
    ///
    /// # Errors
    ///
    /// Propagates RPC or decode errors from [`Self::call`].
    pub async fn turn_start(&self, params: &TurnStartParams) -> Result<TurnStartResponse> {
        self.call(methods::TURN_START, params).await
    }

    /// Interrupt an in-progress turn (`turn/interrupt`).
    ///
    /// # Errors
    ///
    /// Propagates RPC or decode errors from [`Self::call`].
    pub async fn turn_interrupt(
        &self,
        params: &TurnInterruptParams,
    ) -> Result<TurnInterruptResponse> {
        self.call(methods::TURN_INTERRUPT, params).await
    }

    /// Steer a running turn with additional user input (`turn/steer`).
    ///
    /// # Errors
    ///
    /// Propagates RPC or decode errors from [`Self::call`].
    pub async fn turn_steer(&self, params: &TurnSteerParams) -> Result<TurnSteerResponse> {
        self.call(methods::TURN_STEER, params).await
    }

    /// Start a code review turn (`review/start`).
    ///
    /// # Errors
    ///
    /// Propagates RPC or decode errors from [`Self::call`].
    pub async fn review_start(&self, params: &ReviewStartParams) -> Result<ReviewStartResponse> {
        self.call(methods::REVIEW_START, params).await
    }

    /// List models available from the configured model provider (`model/list`).
    ///
    /// # Errors
    ///
    /// Propagates RPC or decode errors from [`Self::call`].
    pub async fn model_list(&self, params: &ModelListParams) -> Result<ModelListResponse> {
        self.call(methods::MODEL_LIST, params).await
    }

    /// Initiate an account login flow (`account/login/start`).
    ///
    /// # Errors
    ///
    /// Propagates RPC or decode errors from [`Self::call`].
    pub async fn account_login_start(
        &self,
        params: &LoginAccountParams,
    ) -> Result<LoginAccountResponse> {
        self.call(methods::ACCOUNT_LOGIN_START, params).await
    }

    /// Log out of the active account (`account/logout`).
    ///
    /// # Errors
    ///
    /// Propagates RPC or decode errors from [`Self::call`].
    pub async fn account_logout(&self) -> Result<LogoutAccountResponse> {
        self.call(methods::ACCOUNT_LOGOUT, &json!({})).await
    }

    /// Read current account authentication status (`account/read`).
    ///
    /// # Errors
    ///
    /// Propagates RPC or decode errors from [`Self::call`].
    pub async fn account_read(&self, params: &GetAccountParams) -> Result<GetAccountResponse> {
        self.call(methods::ACCOUNT_READ, params).await
    }

    async fn upgrade_transport_error(&self, err: SdkError) -> SdkError {
        if !self.inner.has_child || self.inner.is_client_closed() {
            return err;
        }
        let is_pipe_death = match &err {
            SdkError::Closed => true,
            SdkError::Io(io_err) => matches!(
                io_err.kind(),
                ErrorKind::BrokenPipe | ErrorKind::ConnectionReset | ErrorKind::UnexpectedEof
            ),
            _ => false,
        };
        if !is_pipe_death {
            return err;
        }
        let _ = self.inner.exit_ready_rx.recv().await;
        self.inner.exit_error().unwrap_or(err)
    }
}

fn wrap_handshake_error(err: SdkError) -> SdkError {
    if matches!(err, SdkError::ProcessExited { .. }) {
        err
    } else {
        SdkError::Initialize(Box::new(err))
    }
}

fn extract_thread_id(params: Option<&Value>) -> Option<&str> {
    let obj = params?.as_object()?;
    obj.get("threadId")
        .or_else(|| obj.get("conversationId"))
        .or_else(|| obj.get("thread").and_then(|t| t.get("id")))
        .or_else(|| obj.get("reviewThreadId"))
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
}

async fn drive_events<H: ServerRequestHandler>(
    inner: Arc<Inner>,
    events: Receiver<Incoming>,
    handler: Arc<H>,
) {
    let mut events = std::pin::pin!(events);
    let mut in_flight: Vec<Pin<Box<dyn Future<Output = ()> + Send>>> = Vec::new();
    let mut events_done = false;

    poll_fn(|cx| {
        if inner.is_client_closed() {
            return Poll::Ready(());
        }

        while !events_done {
            match events.as_mut().poll_next(cx) {
                Poll::Ready(Some(msg)) => match msg {
                    Incoming::Notification { method, params } => {
                        inner.route_notification(method, params);
                    }
                    Incoming::Request { id, method, params } => {
                        let task_inner = Arc::clone(&inner);
                        let task_handler = Arc::clone(&handler);
                        in_flight.push(Box::pin(async move {
                            dispatch_server_request(task_inner, task_handler, id, method, params)
                                .await;
                        }));
                    }
                },
                Poll::Ready(None) => {
                    events_done = true;
                }
                Poll::Pending => break,
            }
        }

        let mut idx = 0;
        while idx < in_flight.len() {
            if in_flight[idx].as_mut().poll(cx).is_ready() {
                drop(in_flight.swap_remove(idx));
            } else {
                idx += 1;
            }
        }

        if events_done && in_flight.is_empty() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;
}

async fn dispatch_server_request<H: ServerRequestHandler>(
    inner: Arc<Inner>,
    handler: Arc<H>,
    id: RequestId,
    method: String,
    params: Option<Value>,
) {
    let outcome = match ServerRequest::from_envelope(&method, params) {
        Ok(request) => handler.handle_request(request).await,
        Err(err) => {
            warn!(%err, %method, "failed to decode inbound server request parameters");
            Err(RemoteError::invalid_params(format!(
                "invalid params for `{method}`: {err}"
            )))
        }
    };

    if let Err(err) = inner.conn.respond(&id, outcome).await {
        debug!(%err, ?id, "unable to send response for server request");
    }
}

#[cfg(feature = "process")]
async fn run_spawned_driver<ReadFut, H>(
    child: &mut async_process::Child,
    reader_fut: ReadFut,
    events: Receiver<Incoming>,
    stderr: async_process::ChildStderr,
    handler: Arc<H>,
    close_signal_rx: &Receiver<()>,
    inner: &Arc<Inner>,
) where
    ReadFut: Future<Output = Result<()>> + Send,
    H: ServerRequestHandler,
{
    enum Phase {
        ClosedByClient,
        NaturalEnd { read_err: Option<SdkError> },
    }

    let mut stderr_collector = StderrCollector::new(drain_stderr(stderr));
    let phase = {
        let io_pump = async {
            let mut read_err = None;
            let read_task = async {
                if let Err(err) = reader_fut.await {
                    read_err = Some(err);
                }
                inner.conn.events().close();
            };
            let dispatch_task = drive_events(Arc::clone(inner), events, Arc::clone(&handler));
            let _ = zip(read_task, dispatch_task).await;
            Phase::NaturalEnd { read_err }
        };

        let poll_io_and_stderr = async {
            let mut io_pinned = Box::pin(io_pump);
            poll_fn(|cx| {
                // Keep draining stderr concurrently so the child never blocks
                // on a full OS stderr pipe buffer while running.
                if !stderr_collector.is_terminated() {
                    stderr_collector.poll_drive(cx);
                }
                io_pinned.as_mut().poll(cx)
            })
            .await
        };

        or(poll_io_and_stderr, async {
            let _ = close_signal_rx.recv().await;
            Phase::ClosedByClient
        })
        .await
    };

    match phase {
        Phase::ClosedByClient => {
            let _ = inner.conn.close().await;
            let _ = child.kill();
            let _ = child.status().await;
            let _ = inner.exit_record.set(None);
            inner.finish_close();
        }
        Phase::NaturalEnd { read_err } => {
            if inner.is_client_closed() {
                let _ = inner.conn.close().await;
                let _ = child.kill();
                let _ = child.status().await;
                let _ = inner.exit_record.set(None);
                inner.finish_close();
                return;
            }
            if read_err.is_some() {
                let _ = child.kill();
            }

            let stderr_tail = or(stderr_collector.finish(), async {
                let _ = close_signal_rx.recv().await;
                String::new()
            })
            .await;

            if inner.is_client_closed() {
                let _ = child.kill();
                let _ = child.status().await;
                let _ = inner.exit_record.set(None);
                inner.finish_close();
                return;
            }

            let status = child.status().await.ok();
            let record = if status.is_none_or(|s| !s.success()) {
                Some(ExitRecord {
                    status,
                    stderr: stderr_tail,
                })
            } else {
                None
            };
            let _ = inner.exit_record.set(record);
            inner.finish_close();
        }
    }
}

#[cfg(feature = "process")]
struct StderrCollector<F> {
    fut: Option<Pin<Box<F>>>,
    output: Option<String>,
}

#[cfg(feature = "process")]
impl<F: Future<Output = String> + Send> StderrCollector<F> {
    fn new(fut: F) -> Self {
        Self {
            fut: Some(Box::pin(fut)),
            output: None,
        }
    }

    const fn is_terminated(&self) -> bool {
        self.fut.is_none()
    }

    fn poll_drive(&mut self, cx: &mut std::task::Context<'_>) {
        if let Some(fut) = &mut self.fut
            && let Poll::Ready(out) = fut.as_mut().poll(cx)
        {
            self.output = Some(out);
            self.fut = None;
        }
    }

    async fn finish(&mut self) -> String {
        if let Some(out) = self.output.take() {
            return out;
        }
        if let Some(fut) = self.fut.take() {
            return fut.await;
        }
        String::new()
    }
}

#[cfg(feature = "process")]
async fn drain_stderr(stderr: async_process::ChildStderr) -> String {
    let mut reader = BufReader::new(stderr);
    let mut raw = Vec::new();
    let mut chunk = [0_u8; 4096];
    loop {
        match reader.read(&mut chunk).await {
            Ok(0) | Err(_) => break,
            Ok(n) => {
                if let Ok(text) = std::str::from_utf8(&chunk[..n]) {
                    for line in text.lines() {
                        if !line.trim().is_empty() {
                            debug!(target: "codex_sdk::stderr", "{line}");
                        }
                    }
                }
                raw.extend_from_slice(&chunk[..n]);
                if raw.len() > MAX_STDERR_TAIL_BYTES * 2 {
                    let drain_to = raw.len() - MAX_STDERR_TAIL_BYTES;
                    raw.drain(..drain_to);
                }
            }
        }
    }
    if raw.len() > MAX_STDERR_TAIL_BYTES {
        let start = raw.len() - MAX_STDERR_TAIL_BYTES;
        String::from_utf8_lossy(&raw[start..]).into_owned()
    } else {
        String::from_utf8_lossy(&raw).into_owned()
    }
}
