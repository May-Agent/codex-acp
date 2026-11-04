//! Newline-delimited JSON-RPC plumbing shared by both protocol edges.
//!
//! Both ACP and the app-server protocol put one JSON value per line. Only
//! `\n` terminates a line: U+2028/U+2029 are legal inside JSON strings and
//! appear unescaped on the wire, so splitting must not treat them as line
//! breaks. `BufRead::lines` already satisfies that contract.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_channel::{Receiver, Sender};
use async_lock::Mutex;
use futures_lite::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use futures_lite::{AsyncRead, AsyncWrite, StreamExt};
use serde::Serialize;
use serde_json::Value;
use thiserror::Error;
use tracing::{debug, warn};

/// A message read from the peer that is not a response.
#[derive(Debug)]
pub enum Incoming {
    /// A request from the peer that expects a response.
    Request {
        /// Correlation id echoed back in the response.
        id: Value,
        /// Method name.
        method: String,
        /// Request parameters (`null` when absent).
        params: Value,
    },
    /// A notification (method + params, no id).
    Notification {
        /// Method name.
        method: String,
        /// Notification parameters (`null` when absent).
        params: Value,
    },
}

/// An error object returned by the peer.
#[derive(Debug, Clone, Error)]
#[error("remote error {code}: {message}")]
pub struct RemoteError {
    /// JSON-RPC error code.
    pub code: i64,
    /// Human-readable message.
    pub message: String,
    /// Optional structured payload.
    pub data: Option<Value>,
}

/// Errors from the connection itself.
#[derive(Debug, Error)]
pub enum Error {
    /// The peer closed the stream or the process died.
    #[error("connection closed")]
    Closed,
    /// Failed to serialize or write a frame.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// The peer returned a JSON-RPC error.
    #[error("{0}")]
    Remote(#[from] RemoteError),
    /// The peer's response could not be decoded into the requested type.
    #[error("malformed payload: {0}")]
    Malformed(#[from] serde_json::Error),
}

type PendingMap = Arc<Mutex<HashMap<String, Sender<Result<Value, RemoteError>>>>>;

/// A bidirectional ndjson JSON-RPC connection.
///
/// [`request`](Self::request) correlates responses by id; inbound requests
/// and notifications arrive on [`events`](Self::events). Writes are
/// serialized so concurrent callers never interleave a frame. The
/// connection drops unanswered requests when the peer's stream ends.
pub struct Connection<W> {
    writer: Mutex<W>,
    pending: PendingMap,
    events: Receiver<Incoming>,
    next_id: AtomicU64,
}

impl<W: AsyncWrite + Unpin> Connection<W> {
    /// Build a connection over `read`/`write` halves.
    ///
    /// The returned future drives the reader; it finishes when the peer's
    /// stream ends, then every pending request resolves to
    /// [`Error::Closed`]. Drive it on your executor.
    pub fn new<R: AsyncRead + Unpin + Send>(
        read: R,
        write: W,
    ) -> (Self, impl Future<Output = ()> + Send) {
        let (events_tx, events_rx) = async_channel::unbounded();
        let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
        let reader = read_lines(BufReader::new(read), events_tx, pending.clone());
        (
            Self {
                writer: Mutex::new(write),
                pending,
                events: events_rx,
                next_id: AtomicU64::new(1),
            },
            reader,
        )
    }

    /// Send a request and await its result payload.
    ///
    /// # Errors
    ///
    /// [`Error::Remote`] when the peer answers with an error object,
    /// [`Error::Closed`] when the connection dies first, [`Error::Io`] on a
    /// write failure.
    pub async fn request<P: Serialize>(&self, method: &str, params: &P) -> Result<Value, Error> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let key = id.to_string();
        let (tx, rx) = async_channel::bounded(1);
        self.pending.lock().await.insert(key.clone(), tx);
        let frame = serde_json::json!({
            "id": id,
            "method": method,
            "params": params,
        });
        if let Err(e) = self.write_frame(&frame).await {
            self.pending.lock().await.remove(&key);
            return Err(e);
        }
        rx.recv()
            .await
            .map_err(|_| Error::Closed)?
            .map_err(Error::Remote)
    }

    /// Send a response frame for an inbound [`Incoming::Request`].
    ///
    /// # Errors
    ///
    /// Propagates write failures.
    pub async fn respond(
        &self,
        id: &Value,
        result: Result<Value, RemoteError>,
    ) -> Result<(), Error> {
        let frame = match result {
            Ok(result) => serde_json::json!({"id": id, "result": result}),
            Err(err) => serde_json::json!({"id": id, "error": {
                "code": err.code,
                "message": err.message,
                "data": err.data,
            }}),
        };
        self.write_frame(&frame).await
    }

    /// Send a notification frame.
    ///
    /// # Errors
    ///
    /// Propagates write failures.
    pub async fn notify<P: Serialize>(&self, method: &str, params: &P) -> Result<(), Error> {
        let frame = serde_json::json!({"method": method, "params": params});
        self.write_frame(&frame).await
    }

    /// Stream of inbound requests and notifications.
    #[must_use]
    pub fn events(&self) -> Receiver<Incoming> {
        self.events.clone()
    }

    /// True once the peer's read side has ended (all pending failed).
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.events.is_closed()
    }

    async fn write_frame(&self, frame: &Value) -> Result<(), Error> {
        let mut line = serde_json::to_vec(frame)?;
        line.push(b'\n');
        let mut writer = self.writer.lock().await;
        writer.write_all(&line).await?;
        writer.flush().await?;
        Ok(())
    }
}

async fn read_lines<R: AsyncBufReadExt + Unpin>(
    reader: R,
    events: Sender<Incoming>,
    pending: PendingMap,
) {
    let mut lines = reader.lines();
    while let Some(line) = lines.next().await {
        let Ok(line) = line else { break };
        let text = line.trim();
        if text.is_empty() {
            continue;
        }
        match serde_json::from_str::<Value>(text) {
            Ok(value) => dispatch(value, &events, &pending).await,
            Err(err) => warn!(%err, len = text.len(), "dropping non-JSON line"),
        }
    }
    for (_, tx) in pending.lock().await.drain() {
        drop(tx);
    }
}

async fn dispatch(value: Value, events: &Sender<Incoming>, pending: &PendingMap) {
    let Some(obj) = value.as_object() else {
        return;
    };
    let id = obj.get("id").cloned();
    match (id, obj.contains_key("method")) {
        (Some(id), false) => {
            let outcome = if let Some(err) = obj.get("error") {
                Err(RemoteError {
                    code: err.get("code").and_then(Value::as_i64).unwrap_or(-32603),
                    message: err
                        .get("message")
                        .and_then(Value::as_str)
                        .unwrap_or("unknown error")
                        .to_string(),
                    data: err.get("data").cloned(),
                })
            } else {
                Ok(obj.get("result").cloned().unwrap_or(Value::Null))
            };
            if let Some(tx) = pending.lock().await.remove(&id.to_string()) {
                let _ = tx.try_send(outcome);
            } else {
                debug!(?id, "dropping response for unknown request");
            }
        }
        (Some(id), true) => {
            let _ = events
                .send(Incoming::Request {
                    id,
                    method: obj["method"].as_str().unwrap_or_default().to_string(),
                    params: obj.get("params").cloned().unwrap_or(Value::Null),
                })
                .await;
        }
        (None, true) => {
            let _ = events
                .send(Incoming::Notification {
                    method: obj["method"].as_str().unwrap_or_default().to_string(),
                    params: obj.get("params").cloned().unwrap_or(Value::Null),
                })
                .await;
        }
        _ => debug!("dropping frame without id or method"),
    }
}
