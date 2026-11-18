//! Runtime-agnostic Rust SDK for driving `OpenAI` Codex via its `app-server`
//! newline-delimited JSON-RPC protocol.
//!
//! `codex-sdk` pairs [`codex_codes`]'s `types`-only wire models with an
//! executor-independent LF-only NDJSON transport, cancel-safe JSON-RPC duplex,
//! child process supervisor (`process` feature), concurrent server-request
//! handler, and per-thread notification demultiplexer.
//!
//! # Example
//!
//! ```
//! use std::path::PathBuf;
//! use codex_sdk::SpawnConfig;
//!
//! let config = SpawnConfig::new()
//!     .client_info("my-client", "0.1.0")
//!     .override_config("sandbox_mode", "workspace-write")
//!     .experimental_api(true);
//!
//! assert_eq!(
//!     config.build_args(),
//!     vec!["app-server", "-c", "sandbox_mode=workspace-write"],
//! );
//! assert_eq!(config.codex_path, None::<PathBuf>);
//! ```

pub mod client;
pub mod config;
pub mod error;
pub mod handler;
pub mod jsonrpc;
pub mod transport;

pub use codex_codes;
pub use codex_codes::protocol::methods;
pub use codex_codes::{
    ApplyPatchApprovalParams, ApplyPatchApprovalResponse, AskForApproval, ClientInfo, CodexModel,
    CommandExecutionApprovalDecision, CommandExecutionRequestApprovalParams,
    CommandExecutionRequestApprovalResponse, ExecCommandApprovalParams,
    ExecCommandApprovalResponse, FileChangeApprovalDecision, FileChangeRequestApprovalParams,
    FileChangeRequestApprovalResponse, GetAccountParams, GetAccountResponse,
    InitializeCapabilities, InitializeParams, InitializeResponse, JsonRpcError, JsonRpcErrorData,
    JsonRpcMessage, JsonRpcNotification, JsonRpcRequest, JsonRpcResponse, LoginAccountParams,
    LoginAccountResponse, LogoutAccountResponse, ModelListParams, ModelListResponse, Notification,
    RequestId, ReviewStartParams, ReviewStartResponse, ReviewTarget, SandboxMode, SandboxPolicy,
    ServerMessage, ServerRequest, Thread, ThreadCompactStartParams, ThreadCompactStartResponse,
    ThreadDeleteParams, ThreadDeleteResponse, ThreadForkParams, ThreadForkResponse, ThreadItem,
    ThreadListParams, ThreadListResponse, ThreadReadParams, ThreadReadResponse, ThreadResumeParams,
    ThreadResumeResponse, ThreadStartParams, ThreadStartResponse, ThreadUnsubscribeParams,
    ThreadUnsubscribeResponse, Turn, TurnInterruptParams, TurnInterruptResponse, TurnStartParams,
    TurnStartResponse, TurnStatus, TurnSteerParams, TurnSteerResponse, UserInput,
};

pub use client::{AppServer, ThreadEvent, ThreadSubscription};
pub use config::SpawnConfig;
pub use error::{
    INTERNAL_ERROR_CODE, INVALID_PARAMS_CODE, INVALID_REQUEST_CODE, METHOD_NOT_FOUND_CODE,
    PARSE_ERROR_CODE, RemoteError, Result, SdkError,
};
pub use handler::{
    AnyServerRequestHandler, HandlerResult, NoopServerRequestHandler, ServerRequestHandler,
};
pub use jsonrpc::{Connection, Incoming};
pub use transport::{NdjsonReader, NdjsonTransport, NdjsonWriter};
