# AGENTS.md

Workspace hosting two runtime-agnostic Rust crates for driving OpenAI Codex:

- `codex-sdk` (`sdk/`) — standalone client for the `codex app-server` NDJSON JSON-RPC protocol (`jsonrpc` field omitted on the wire; LF-only framing preserving unescaped `U+2028`/`U+2029`).
- `codex-acp` (`src/`) — dual-stack ACP v1/v2 server bridging `aither-acp` (`Server`, `SessionHost`, `Session`) onto `codex-sdk`.

## Build & test

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo clippy --workspace --all-targets --no-default-features -- -D warnings
cargo nextest run --workspace --all-features --no-tests=pass
cargo nextest run --workspace --no-default-features --no-tests=pass
cargo test --doc --workspace --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
cargo nextest run -p codex-acp --test live --run-ignored only
```

Live tests require a `codex` executable on `PATH` (or `CODEX_ACP_CODEX_PATH` / `CODEX_PATH`) and a reachable model provider (`CODEX_ACP_POOL_URL`, `CODEX_ACP_TEST_MODEL`); they are marked `#[ignore]` by default.

## Layout

- `sdk/src/transport.rs` — cancel-safe LF-only (`\n`) NDJSON reader, writer, and duplex transport.
- `sdk/src/jsonrpc.rs` — cancel-safe app-server JSON-RPC duplex (`Connection`, `Incoming`).
- `sdk/src/config.rs` — `SpawnConfig` (`codex_path` / `CODEX_PATH` resolution, `-c` overrides, args, env, cwd, handshake metadata).
- `sdk/src/handler.rs` — concurrent `ServerRequestHandler` (`AnyServerRequestHandler`, `NoopServerRequestHandler`) for server-initiated approvals and elicitations.
- `sdk/src/client.rs` — `AppServer` (`attach`, `process`-gated `spawn`, `close`, `handshake`, typed RPC helpers, per-`threadId`/`conversationId` demultiplexing).
- `src/agent.rs` — `CodexAgent` (`SessionHost`) managing shared `AppServer` lifecycle, sessions, auth, steering, and legacy RPCs.
- `src/session.rs` — `CodexSession` (`Session`) driving prompt turns, streaming updates, config options, and slash commands.
- `src/permission.rs` — `PermissionMemory`, `SessionRegistry`, and `BridgeServerRequestHandler` bridging approvals, user-input prompts, and MCP elicitations.
- `src/auth.rs` — `AuthState` (`chat-gpt`, `chat-gpt-device-code`, `api-key`, `gateway`, logout, `_auth/status_update`).
- `src/convert.rs` — ACP domain <-> `codex app-server` wire conversions, replay, notice gating, and RFC 3339 timestamps.
- `src/driver.rs` — `DriverSlot` cooperative polling of the shared `AppServer` driver without runtime-specific task spawning.
- `src/mode.rs` — `AcpMode`, session `ConfigOption` builders, and slash command metadata.
- `src/config.rs` — `CodexAcpConfig`, `ConnectedAppServer`, `AppServerConnector`, `AnyAppServerConnector`, and `ProcessAppServerConnector`.

## Conventions

- Library code is runtime-agnostic (`futures-lite`, `async-channel`, `async-lock`, optional `async-process` under `process`) and never depends on `tokio` or `anyhow`.
- Public async methods and traits return `impl Future<Output = ...> + Send`; type-erased wrappers keep boxed futures private.
- Errors are typed `thiserror` enums; diagnostics go through `tracing`.
- App-server wire models come from `codex-codes` (`default-features = false, features = ["types"]`); ACP models come from `aither-acp` (`default-features = false`).
