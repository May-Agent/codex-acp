//! Agent Client Protocol (ACP v1+v2) server for `OpenAI` Codex, built onto
//! [`aither_acp`]'s dual-stack `Server`/`SessionHost`/`Session` API and
//! [`codex_sdk::AppServer`].
//!
//! # Architecture
//!
//! `codex-acp` bridges ACP JSON-RPC traffic from editors and orchestrators
//! onto `codex app-server`:
//!
//! - [`CodexAgent`] implements [`aither_acp::server::SessionHost`] without
//!   tying library code to any async runtime; each live conversation is a
//!   [`CodexSession`] implementing [`aither_acp::server::Session`].
//! - [`serve`] drives a [`CodexAgent`] over any [`aither_acp::Transport`],
//!   cooperatively polling the shared `AppServer` driver both during turns and
//!   while idle between turns so out-of-prompt server requests (approvals,
//!   elicitations, user-input prompts) are answered without blocking, and
//!   shutting down active sessions and the `AppServer` cleanly when the ACP
//!   connection closes.
//! - `serve_stdio` (`process` feature) runs the server over process `stdin`
//!   and `stdout`.
//! - [`AppServerConnector`] decouples `codex app-server` connection creation so
//!   deterministic tests can attach in-memory mock byte streams via
//!   [`CodexAcpConfig::connector`] while production builds spawn `codex` child
//!   processes (`process` feature, enabled by default).
//!
//! # Example
//!
//! ```no_run
//! # #[cfg(feature = "process")]
//! # async fn run() {
//! use codex_acp::{CodexAcpConfig, serve_stdio};
//!
//! let config = CodexAcpConfig::new().default_mode("workspace-write");
//! let (_peer, server) = serve_stdio(config);
//! server.await;
//! # }
//! ```

#![deny(missing_docs)]

pub mod agent;
pub mod auth;
pub mod config;
pub mod convert;
pub mod driver;
pub mod error;
pub mod mode;
pub mod permission;
pub mod session;

use std::future::Future;

pub use aither_acp;
use aither_acp::rpc::Peer;
pub use codex_sdk;

pub use agent::CodexAgent;
#[cfg(feature = "process")]
pub use config::ProcessAppServerConnector;
pub use config::{AnyAppServerConnector, AppServerConnector, CodexAcpConfig, ConnectedAppServer};
pub use error::{CodexAcpError, Result};
pub use mode::AcpMode;
pub use session::CodexSession;

/// Serve a [`CodexAgent`] configured with `config` over an arbitrary
/// [`aither_acp::Transport`].
///
/// Returns a [`Peer`] handle for agent-initiated notifications and requests
/// alongside the connection future that drives both the ACP transport and the
/// `codex app-server` driver, shutting down active turns and closing the
/// `AppServer` when the ACP connection ends.
pub fn serve<T>(transport: T, config: CodexAcpConfig) -> (Peer, impl Future<Output = ()> + Send)
where
    T: aither_acp::Transport + 'static,
{
    let agent = CodexAgent::new(config);
    let bg_agent = agent.clone();
    let shutdown_agent = agent.clone();
    let server = agent.clone().into_server();
    let (peer, connection) = server.serve(transport);
    agent.set_peer(peer.clone());
    let wrapped = async move {
        futures_lite::future::or(connection, bg_agent.drive_background()).await;
        shutdown_agent.shutdown_all().await;
    };
    (peer, wrapped)
}

/// Serve a [`CodexAgent`] configured with `config` over standard input and
/// output (`process` feature).
#[cfg(feature = "process")]
pub fn serve_stdio(config: CodexAcpConfig) -> (Peer, impl Future<Output = ()> + Send) {
    serve(aither_acp::rpc::Stdio::new(), config)
}
