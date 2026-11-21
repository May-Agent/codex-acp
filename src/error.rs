//! Error types for `codex-acp` and conversions into ACP JSON-RPC errors.

use aither_acp::rpc::{PeerError, RpcError};
use aither_acp::server::{PromptError, ServerError};
use codex_sdk::SdkError;

/// Convenience result alias returning [`CodexAcpError`].
pub type Result<T> = std::result::Result<T, CodexAcpError>;

/// Errors produced by the `codex-acp` agent bridge.
#[derive(Debug, thiserror::Error)]
pub enum CodexAcpError {
    /// Underlying `codex-sdk` transport, process, or remote RPC error.
    #[error(transparent)]
    Sdk(#[from] SdkError),

    /// Underlying `aither-acp` server error.
    #[error(transparent)]
    Server(#[from] ServerError),

    /// Underlying `aither-acp` peer transport error.
    #[error(transparent)]
    Peer(#[from] PeerError),

    /// Referenced ACP session does not exist.
    #[error("session not found: {0}")]
    SessionNotFound(String),

    /// Invalid request parameter supplied by the ACP client.
    #[error("{0}")]
    InvalidParams(String),

    /// Authentication failed or credentials are missing.
    #[error("{0}")]
    Auth(String),
}

impl CodexAcpError {
    /// Convert this error into a protocol [`RpcError`].
    #[must_use]
    pub fn into_rpc_error(self) -> RpcError {
        match self {
            Self::SessionNotFound(id) => {
                RpcError::invalid_params(format!("session not found: {id}"))
            }
            Self::InvalidParams(msg) => RpcError::invalid_params(msg),
            Self::Auth(msg) => RpcError::new(-32000, msg),
            Self::Sdk(SdkError::Remote(remote)) => {
                let mut rpc = RpcError::new(remote.code, remote.message);
                rpc.data = remote.data;
                rpc
            }
            Self::Sdk(err) => RpcError::internal_error(err.to_string()),
            Self::Server(ServerError::Rpc(rpc)) | Self::Peer(PeerError::Rejected(rpc)) => rpc,
            Self::Server(err) => RpcError::internal_error(err.to_string()),
            Self::Peer(err) => RpcError::internal_error(err.to_string()),
        }
    }

    /// Convert this error into a [`ServerError`].
    #[must_use]
    pub fn into_server_error(self) -> ServerError {
        ServerError::Rpc(self.into_rpc_error())
    }

    /// Convert this error into a [`PromptError`].
    #[must_use]
    pub fn into_prompt_error(self) -> PromptError {
        PromptError::from(self.into_rpc_error())
    }
}

impl From<RpcError> for CodexAcpError {
    fn from(rpc: RpcError) -> Self {
        Self::Server(ServerError::Rpc(rpc))
    }
}

impl From<CodexAcpError> for RpcError {
    fn from(err: CodexAcpError) -> Self {
        err.into_rpc_error()
    }
}
