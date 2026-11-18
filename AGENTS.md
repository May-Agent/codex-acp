# AGENTS.md

Workspace hosting two runtime-agnostic Rust crates for driving OpenAI Codex:

- `codex-sdk` (`sdk/`) — standalone client for the `codex app-server` NDJSON JSON-RPC protocol (`jsonrpc` field omitted on the wire; LF-only framing preserving unescaped `U+2028`/`U+2029`).
- `codex-acp` (`src/`) — Agent Client Protocol (ACP) server bridging ACP sessions, prompts, permissions, modes, config options, and slash commands onto `codex app-server`.

## Build & test

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo clippy -p codex-sdk --all-targets --no-default-features -- -D warnings
cargo nextest run --workspace --all-features --no-tests=pass
cargo test --doc --workspace --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
cargo test --workspace --all-features -- --ignored   # live tests against `codex`
```

Live tests require a `codex` executable on `PATH` (or `CODEX_PATH`) and a reachable model provider; they are marked `#[ignore]` by default.

## Layout

- `sdk/src/transport.rs` — cancel-safe LF-only (`\n`) NDJSON reader, writer, and duplex transport.
- `sdk/src/jsonrpc.rs` — cancel-safe app-server JSON-RPC duplex (`Connection`, `Incoming`).
- `sdk/src/config.rs` — `SpawnConfig` (`codex_path` / `CODEX_PATH` resolution, `-c` overrides, args, env, cwd, handshake metadata).
- `sdk/src/handler.rs` — concurrent `ServerRequestHandler` (`AnyServerRequestHandler`, `NoopServerRequestHandler`) for server-initiated approvals and elicitations.
- `sdk/src/client.rs` — `AppServer` (`attach`, `process`-gated `spawn`, `close`, `handshake`, typed RPC helpers, per-`threadId`/`conversationId` demultiplexing).
- `src/` — ACP bridge (`agent`, `session`, `translate`, and protocol glue).

## Conventions

- Runtime-agnostic library code: `futures-lite`, `async-channel`, `async-lock`, and optional `async-process` (`process` feature); never depend on `tokio` or `anyhow` outside dev-dependencies.
- Public async methods and traits return `impl Future<Output = ...> + Send` without `Pin<Box<dyn Future>>` in public signatures or `#[allow(clippy::future_not_send)]` suppressions.
- Errors are typed `thiserror` enums; diagnostics go through `tracing`.
- App-server wire models come from `codex-codes` (`default-features = false, features = ["types"]`).
