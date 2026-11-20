//! Serve `OpenAI` Codex as an Agent Client Protocol (ACP) agent.
//!
//! The bridge owns a `codex app-server` child process and translates
//! between two JSON-RPC-over-ndjson protocols:
//!
//! - **ACP** on the client side (the `jsonrpc: "2.0"` field is present).
//! - The Codex **app-server** protocol on the agent side (messages carry
//!   only `id`/`method`/`params`; there is no `jsonrpc` field).
//!
//! The entry point is [`serve`], which takes any transport `aither-acp`
//! can drive, or [`serve_stdio`] for the usual editor-adapter case. Both
//! ACP v1 and v2 clients are served through `aither-acp`'s dual-stack
//! negotiation and translation layer.

#![deny(missing_docs)]
#![allow(missing_debug_implementations)]

pub mod agent;
pub mod app_server;
pub mod jsonrpc;
pub mod session;
pub mod translate;

#[cfg(feature = "process")]
pub use agent::serve_stdio;
pub use agent::{CodexAcpConfig, CodexHost, serve};
pub use aither_acp;
pub use codex_sdk;
