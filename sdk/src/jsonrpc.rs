//! Cancel-safe `codex app-server` NDJSON JSON-RPC duplex connection.
//!
//! Unlike standard JSON-RPC 2.0, `codex app-server` omits the `"jsonrpc": "2.0"`
//! header on every wire message:
//! - Request: `{"id": 1, "method": "...", "params": ...}`
//! - Success response: `{"id": 1, "result": ...}`
//! - Error response: `{"id": 1, "error": {"code": ..., "message": ..., "data": ...}}`
//! - Notification: `{"method": "...", "params": ...}`
//!
//! [`Connection::request`] installs a synchronous `Drop` guard around its
//! pending correlation entry so cancelling/dropping an in-flight request future
//! immediately removes its map entry without leaking memory.

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

use async_channel::{Receiver, Sender};
use async_lock::Mutex as AsyncMutex;
use codex_codes::RequestId;
use futures_lite::io::{AsyncRead, AsyncWrite};
use serde::Serialize;
use serde_json::Value;
use tracing::{debug, warn};

use crate::error::{INTERNAL_ERROR_CODE, RemoteError, Result, SdkError};
use crate::transport::{NdjsonReader, NdjsonWriter};

/// Inbound peer frame that is not a correlated response to an outbound request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Incoming {
    /// Server-initiated request requiring a response correlated by `id`.
    Request {
        /// Request identifier to echo back via [`Connection::respond`].
        id: RequestId,
        /// JSON-RPC method name.
        method: String,
        /// Optional request parameters (`None` when omitted on the wire).
        params: Option<Value>,
    },
    /// Server-initiated notification (`method` + optional `params`, no `id`).
    Notification {
        /// JSON-RPC method name.
        method: String,
        /// Optional notification parameters (`None` when omitted on the wire).
        params: Option<Value>,
    },
}

type PendingSender = Sender<std::result::Result<Value, RemoteError>>;

struct SharedPending {
    closed: AtomicBool,
    map: StdMutex<HashMap<RequestId, PendingSender>>,
}

impl SharedPending {
    fn new() -> Self {
        Self {
            closed: AtomicBool::new(false),
            map: StdMutex::new(HashMap::new()),
        }
    }

    fn insert(&self, id: RequestId, tx: PendingSender) -> bool {
        let Ok(mut map) = self.map.lock() else {
            return false;
        };
        if self.closed.load(Ordering::Acquire) {
            return false;
        }
        map.insert(id, tx);
        true
    }

    fn remove(&self, id: &RequestId) -> Option<PendingSender> {
        self.map.lock().ok()?.remove(id)
    }

    fn drain_and_close(&self) {
        self.closed.store(true, Ordering::Release);
        if let Ok(mut map) = self.map.lock() {
            map.clear();
        }
    }

    fn count(&self) -> usize {
        self.map.lock().map_or(0, |map| map.len())
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

struct PendingGuard<'a> {
    pending: &'a SharedPending,
    id: Option<RequestId>,
}

impl<'a> PendingGuard<'a> {
    const fn new(pending: &'a SharedPending, id: RequestId) -> Self {
        Self {
            pending,
            id: Some(id),
        }
    }

    fn disarm(&mut self) {
        self.id = None;
    }
}

impl Drop for PendingGuard<'_> {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            let _ = self.pending.remove(&id);
        }
    }
}

#[derive(Serialize)]
struct OutboundRequest<'a, P: ?Sized> {
    id: &'a RequestId,
    method: &'a str,
    params: &'a P,
}

#[derive(Serialize)]
struct OutboundNotification<'a, P: ?Sized> {
    method: &'a str,
    params: &'a P,
}

#[derive(Serialize)]
struct OutboundResponse<'a> {
    id: &'a RequestId,
    result: &'a Value,
}

#[derive(Serialize)]
struct OutboundError<'a> {
    id: &'a RequestId,
    error: &'a RemoteError,
}

/// Bidirectional LF-only NDJSON JSON-RPC connection speaking the `codex app-server`
/// wire format (`jsonrpc` field omitted).
pub struct Connection<W> {
    writer: AsyncMutex<NdjsonWriter<W>>,
    pending: Arc<SharedPending>,
    events_tx: Sender<Incoming>,
    events_rx: Receiver<Incoming>,
    next_id: AtomicI64,
}

impl<W> fmt::Debug for Connection<W> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Connection")
            .field("closed", &self.pending.is_closed())
            .field("pending_count", &self.pending.count())
            .finish_non_exhaustive()
    }
}

impl<W: AsyncWrite + Unpin + Send> Connection<W> {
    /// Construct a bidirectional JSON-RPC connection over `read` and `write`.
    ///
    /// Returns `(connection, reader_future)`. Poll or spawn `reader_future` on
    /// any async executor; when the peer closes its stream (EOF) or a fatal read
    /// error occurs, `reader_future` drains all pending requests so they resolve
    /// immediately to [`SdkError::Closed`].
    pub fn new<R: AsyncRead + Unpin + Send>(
        read: R,
        write: W,
    ) -> (Self, impl Future<Output = ()> + Send + use<R, W>) {
        let (conn, reader_fut) = Self::new_deferred(read, write);
        let pending = Arc::clone(&conn.pending);
        let events_tx = conn.events_tx.clone();
        let drive = async move {
            if let Err(err) = reader_fut.await {
                debug!(%err, "JSON-RPC reader terminated with error");
            }
            pending.drain_and_close();
            events_tx.close();
        };
        (conn, drive)
    }

    /// Internal constructor used by `AppServer::spawn` so child exit status and
    /// `stderr` can be collected on reader EOF before waking pending requests.
    pub(crate) fn new_deferred<R: AsyncRead + Unpin + Send>(
        read: R,
        write: W,
    ) -> (Self, impl Future<Output = Result<()>> + Send + use<R, W>) {
        let (events_tx, events_rx) = async_channel::unbounded();
        let pending = Arc::new(SharedPending::new());
        let reader_fut = read_loop(
            NdjsonReader::new(read),
            events_tx.clone(),
            Arc::clone(&pending),
        );
        let conn = Self {
            writer: AsyncMutex::new(NdjsonWriter::new(write)),
            pending,
            events_tx,
            events_rx,
            next_id: AtomicI64::new(1),
        };
        (conn, reader_fut)
    }

    /// Send a JSON-RPC request (`{"id", "method", "params"}`) and await its
    /// correlated result payload.
    ///
    /// Dropping the returned future before the response arrives automatically
    /// removes the correlation entry from the pending map.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::Json`] if `params` cannot be serialized,
    /// [`SdkError::Io`] on write failure, [`SdkError::Remote`] when the peer
    /// replies with an `error` frame, or [`SdkError::Closed`] if the connection
    /// closes before a reply arrives.
    pub fn request<P: Serialize + ?Sized>(
        &self,
        method: &str,
        params: &P,
    ) -> impl Future<Output = Result<Value>> + Send + use<'_, W, P> {
        let id = RequestId::Integer(self.next_id.fetch_add(1, Ordering::Relaxed));
        let serialized = serde_json::to_string(&OutboundRequest {
            id: &id,
            method,
            params,
        })
        .map_err(SdkError::Json);

        async move {
            let line = serialized?;
            let (tx, rx) = async_channel::bounded(1);
            if !self.pending.insert(id.clone(), tx) {
                return Err(SdkError::Closed);
            }
            let mut guard = PendingGuard::new(&self.pending, id);

            let mut writer = self.writer.lock().await;
            writer.write_raw_line(&line).await?;
            drop(writer);

            let outcome = rx.recv().await.map_err(|_| SdkError::Closed)?;
            guard.disarm();
            outcome.map_err(SdkError::Remote)
        }
    }

    /// Send a one-way JSON-RPC notification (`{"method", "params"}`).
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::Json`] if serialization fails, [`SdkError::Closed`]
    /// if the connection is already closed, or [`SdkError::Io`] on write failure.
    pub fn notify<P: Serialize + ?Sized>(
        &self,
        method: &str,
        params: &P,
    ) -> impl Future<Output = Result<()>> + Send + use<'_, W, P> {
        let serialized =
            serde_json::to_string(&OutboundNotification { method, params }).map_err(SdkError::Json);
        async move {
            if self.pending.is_closed() {
                return Err(SdkError::Closed);
            }
            let line = serialized?;
            let mut writer = self.writer.lock().await;
            writer.write_raw_line(&line).await
        }
    }

    /// Send a JSON-RPC response frame (`result` or `error`) for an inbound
    /// [`Incoming::Request`].
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::Json`] if serialization fails, [`SdkError::Closed`]
    /// if the connection is closed, or [`SdkError::Io`] on write failure.
    pub fn respond(
        &self,
        id: &RequestId,
        result: std::result::Result<Value, RemoteError>,
    ) -> impl Future<Output = Result<()>> + Send + use<'_, W> {
        let serialized = match result {
            Ok(val) => serde_json::to_string(&OutboundResponse { id, result: &val }),
            Err(err) => serde_json::to_string(&OutboundError { id, error: &err }),
        }
        .map_err(SdkError::Json);

        async move {
            if self.pending.is_closed() {
                return Err(SdkError::Closed);
            }
            let line = serialized?;
            let mut writer = self.writer.lock().await;
            writer.write_raw_line(&line).await
        }
    }

    /// Cloneable receiver yielding inbound requests and notifications.
    #[must_use]
    pub fn events(&self) -> Receiver<Incoming> {
        self.events_rx.clone()
    }

    /// Number of currently in-flight outbound requests awaiting a response.
    #[must_use]
    pub fn pending_count(&self) -> usize {
        self.pending.count()
    }

    /// Returns `true` once the connection has closed and drained pending calls.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.pending.is_closed()
    }

    /// Synchronously mark the connection closed, draining all pending requests
    /// (waking waiters with [`SdkError::Closed`]) and closing [`Self::events`].
    pub(crate) fn mark_closed(&self) {
        self.pending.drain_and_close();
        self.events_tx.close();
    }

    /// Close the connection: drain all pending requests, close the inbound event
    /// channel, and flush/close the outbound writer.
    ///
    /// # Errors
    ///
    /// Propagates underlying writer flush/close errors.
    pub async fn close(&self) -> Result<()> {
        self.mark_closed();
        let mut writer = self.writer.lock().await;
        writer.close().await
    }
}

async fn read_loop<R: AsyncRead + Unpin + Send>(
    mut reader: NdjsonReader<R>,
    events_tx: Sender<Incoming>,
    pending: Arc<SharedPending>,
) -> Result<()> {
    while let Some(line) = reader.next_line().await? {
        if pending.is_closed() {
            break;
        }
        match serde_json::from_str::<Value>(&line) {
            Ok(value) => dispatch_frame(value, &events_tx, &pending),
            Err(err) => warn!(%err, len = line.len(), "dropping malformed JSON line from peer"),
        }
    }
    Ok(())
}

fn dispatch_frame(value: Value, events_tx: &Sender<Incoming>, pending: &SharedPending) {
    let Value::Object(mut obj) = value else {
        debug!("ignoring non-object JSON-RPC frame");
        return;
    };

    let parsed_id = obj
        .remove("id")
        .and_then(|raw_id| serde_json::from_value::<RequestId>(raw_id).ok());
    let method = obj.remove("method").and_then(|m| match m {
        Value::String(s) => Some(s),
        _ => None,
    });

    match (parsed_id, method) {
        (Some(id), None) => {
            let outcome = if let Some(err_val) = obj.remove("error")
                && !err_val.is_null()
            {
                let code = err_val
                    .get("code")
                    .and_then(Value::as_i64)
                    .unwrap_or(INTERNAL_ERROR_CODE);
                let message = err_val
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown remote error")
                    .to_string();
                let data = err_val.get("data").cloned();
                Err(RemoteError {
                    code,
                    message,
                    data,
                })
            } else {
                Ok(obj.remove("result").unwrap_or(Value::Null))
            };

            if let Some(tx) = pending.remove(&id) {
                let _ = tx.try_send(outcome);
            } else {
                debug!(?id, "ignoring response for unknown or cancelled request id");
            }
        }
        (Some(id), Some(method)) => {
            let params = obj.remove("params");
            let _ = events_tx.try_send(Incoming::Request { id, method, params });
        }
        (None, Some(method)) => {
            let params = obj.remove("params");
            let _ = events_tx.try_send(Incoming::Notification { method, params });
        }
        (None, None) => {
            debug!("ignoring JSON frame without id or method");
        }
    }
}
