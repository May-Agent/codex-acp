# codex-acp

A runtime-agnostic Rust workspace serving OpenAI Codex over the
[Agent Client Protocol](https://agentclientprotocol.com) (ACP v1 and v2) by
bridging `aither-acp` onto `codex app-server`:

- **`codex-sdk`** (`sdk/`) — standalone client SDK for `codex app-server`
  (LF-only NDJSON framing preserving unescaped `U+2028`/`U+2029`, JSON-RPC
  correlation without a `jsonrpc` wire field, concurrent `ServerRequestHandler`,
  per-`threadId` event demultiplexing, and optional child process supervision
  behind the `process` feature).
- **`codex-acp`** (`.`) — dual-stack ACP v1/v2 agent server (`CodexAgent`,
  `CodexSession`, `serve`, `serve_stdio`) built onto `aither-acp` and
  `codex-sdk`.

## Features

- Dual-stack ACP v1 and v2 negotiation, capability translation, and state
  updates via `aither-acp`
- Authentication: `chat-gpt`, `chat-gpt-device-code`, `api-key`, custom
  `gateway`, `auth/logout` (`logout` on v1), `_auth/status_update` push
  notifications, and legacy `authentication/{status,logout}` RPCs
- Session lifecycle: `session/{new,load,resume,fork,list,close,delete}`, MCP
  server forwarding (`stdio`, `http`, `sse`), and transcript history replay
- Modes (`read-only`, `workspace-write`, `agent`, `agent-full-access`) and
  config options (`mode`, `model`, `reasoning_effort`, boolean `fast-mode`),
  plus legacy `session/set_model`
- Streaming agent text, reasoning summaries, plans, tool calls (with diffs,
  locations, command/file output chunks, and MCP metadata), token usage, and
  capability-gated notices
- Mid-turn and out-of-prompt approvals (`commandExecution`, `execCommand`,
  `fileChange`, `applyPatch`, `permissions`), `requestUserInput`, MCP form/URL
  elicitations, `strictReviewRequired` escalation, and per-session permission
  memory
- Steering (`_session/steering`, legacy `_codex/session/steering`, and v2
  concurrent prompt merging) and slash commands (`/compact`, `/status`,
  `/logout`, `/review`, `/review-branch`, `/review-commit`)

## Usage

```toml
[dependencies]
codex-acp = { git = "https://github.com/May-Agent/codex-acp", branch = "main" }
```

### Stdio adapter (`process` feature, enabled by default)

```rust,no_run
use codex_acp::{CodexAcpConfig, serve_stdio};

#[tokio::main]
async fn main() {
    let (_peer, server) = serve_stdio(CodexAcpConfig::new());
    server.await;
}
```

### Custom transport or in-memory streams (`default-features = false`)

```rust,no_run
use codex_acp::{CodexAcpConfig, ConnectedAppServer, serve};

# async fn example(
#     acp_transport: impl codex_acp::aither_acp::Transport + 'static,
#     read: impl futures_lite::io::AsyncRead + Unpin + Send + 'static,
#     write: impl futures_lite::io::AsyncWrite + Unpin + Send + 'static,
# ) {
let slot = std::sync::Mutex::new(Some((read, write)));
let config = CodexAcpConfig::new()
    .default_mode("workspace-write")
    .connector(move |_spawn, handler| {
        let (r, w) = slot.lock().unwrap().take().expect("connected once");
        std::future::ready(Ok(ConnectedAppServer::attach(r, w, handler)))
    });

let (_peer, server) = serve(acp_transport, config);
server.await;
# }
```

## Configuration & environment variables

- `CodexAcpConfig::codex_path` / `SpawnConfig::codex_path` — explicit `codex`
  executable path; falls back to `CODEX_PATH` and then `"codex"` on `PATH`.
- `SpawnConfig::override_config` / `SpawnConfig::args` — `-c key=value`
  overrides and extra CLI arguments passed to `codex app-server`.
- `SpawnConfig::env_var` — environment variables (`CODEX_API_KEY`,
  `OPENAI_API_KEY`, etc.) passed to the spawned `codex` process.
- `CodexAcpConfig::default_mode` — initial session mode (`"agent"` by default).

## Development & testing

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo clippy --workspace --all-targets --no-default-features -- -D warnings
cargo nextest run --workspace --all-features --no-tests=pass
cargo nextest run --workspace --no-default-features --no-tests=pass
cargo test --doc --workspace --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
```

To run the ignored live integration test against a local `codex` binary and
provider pool:

```bash
CODEX_ACP_POOL_URL=http://127.0.0.1:64395/v1 CODEX_ACP_TEST_MODEL=gpt-6-luna \
    cargo nextest run -p codex-acp --test live --run-ignored only
```

## License

Licensed under either of [Apache-2.0](LICENSE-APACHE) or [MIT](LICENSE-MIT)
at your option.
