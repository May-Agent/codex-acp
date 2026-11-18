//! Error types and JSON-RPC remote error representation for `codex-sdk`.

use std::process::ExitStatus;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Standard JSON-RPC 2.0 parse error code (`-32700`).
pub const PARSE_ERROR_CODE: i64 = -32700;
/// Standard JSON-RPC 2.0 invalid request error code (`-32600`).
pub const INVALID_REQUEST_CODE: i64 = -32600;
/// Standard JSON-RPC 2.0 method-not-found error code (`-32601`).
pub const METHOD_NOT_FOUND_CODE: i64 = -32601;
/// Standard JSON-RPC 2.0 invalid-params error code (`-32602`).
pub const INVALID_PARAMS_CODE: i64 = -32602;
/// Standard JSON-RPC 2.0 internal-error code (`-32603`).
pub const INTERNAL_ERROR_CODE: i64 = -32603;

/// Result alias using [`SdkError`].
pub type Result<T> = std::result::Result<T, SdkError>;

/// Structured JSON-RPC error object sent to or received from `codex app-server`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[error("remote error {code}: {message}")]
pub struct RemoteError {
    /// JSON-RPC integer error code.
    pub code: i64,
    /// Human-readable error message.
    pub message: String,
    /// Optional structured error payload.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl RemoteError {
    /// Construct a [`RemoteError`] with no structured `data` payload.
    #[must_use]
    pub fn new(code: i64, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            data: None,
        }
    }

    /// Attach a structured JSON `data` payload to this error.
    #[must_use]
    pub fn with_data(mut self, data: Value) -> Self {
        self.data = Some(data);
        self
    }

    /// Construct a JSON-RPC `-32601` (`Method not found`) error for `method`.
    #[must_use]
    pub fn method_not_found(method: &str) -> Self {
        Self::new(
            METHOD_NOT_FOUND_CODE,
            format!("unsupported method `{method}`"),
        )
    }

    /// Construct a JSON-RPC `-32602` (`Invalid params`) error.
    #[must_use]
    pub fn invalid_params(message: impl Into<String>) -> Self {
        Self::new(INVALID_PARAMS_CODE, message)
    }

    /// Construct a JSON-RPC `-32603` (`Internal error`) error.
    #[must_use]
    pub fn internal_error(message: impl Into<String>) -> Self {
        Self::new(INTERNAL_ERROR_CODE, message)
    }

    /// Attempt to decode [`Self::data`] as a [`codex_codes::ModelProviderError`].
    #[must_use]
    pub fn model_provider_error(&self) -> Option<codex_codes::ModelProviderError> {
        serde_json::from_value(self.data.clone()?).ok()
    }
}

impl From<codex_codes::JsonRpcErrorData> for RemoteError {
    fn from(err: codex_codes::JsonRpcErrorData) -> Self {
        Self {
            code: err.code,
            message: err.message,
            data: err.data,
        }
    }
}

impl From<RemoteError> for codex_codes::JsonRpcErrorData {
    fn from(err: RemoteError) -> Self {
        Self {
            code: err.code,
            message: err.message,
            data: err.data,
        }
    }
}

/// Errors produced by `codex-sdk` transport, JSON-RPC correlation, handshake,
/// and child process supervision.
#[derive(Debug, thiserror::Error)]
pub enum SdkError {
    /// Underlying stream or process I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// JSON serialization or deserialization error.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// Typed `codex-codes` wire message decode error.
    #[error("protocol decode error: {0}")]
    Decode(#[source] Box<codex_codes::Error>),

    /// Framing or protocol contract violation.
    #[error("protocol error: {0}")]
    Protocol(String),

    /// JSON-RPC error returned by the remote peer.
    #[error(transparent)]
    Remote(#[from] RemoteError),

    /// The `initialize` / `initialized` handshake failed.
    #[error("app-server initialize failed: {0}")]
    Initialize(#[source] Box<Self>),

    /// The connection or session closed before the operation completed.
    #[error("app-server connection closed")]
    Closed,

    /// The spawned `codex app-server` child process exited unexpectedly.
    #[error("codex app-server exited ({}){}", format_exit_status(*status), format_stderr(stderr))]
    ProcessExited {
        /// Operating-system exit status, when available.
        status: Option<ExitStatus>,
        /// Captured stderr tail from the child process.
        stderr: String,
    },
}

impl From<codex_codes::Error> for SdkError {
    fn from(err: codex_codes::Error) -> Self {
        Self::Decode(Box::new(err))
    }
}

fn format_exit_status(status: Option<ExitStatus>) -> String {
    status.map_or_else(
        || "unknown status".to_string(),
        |s| {
            s.code()
                .map_or_else(|| s.to_string(), |code| format!("code {code}"))
        },
    )
}

fn format_stderr(stderr: &str) -> String {
    let trimmed = stderr.trim();
    if trimmed.is_empty() {
        String::new()
    } else {
        format!(": {trimmed}")
    }
}
