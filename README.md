# codex-acp

A pure-Rust library that serves the [Agent Client Protocol](https://agentclientprotocol.com)
(v1) and bridges every operation onto `codex app-server` — Codex's own
JSON-RPC interface — over newline-delimited JSON.

It is the piece an editor agent spawns (stdio) or embeds (in-process streams)
to drive Codex: sessions, prompts, streaming updates, permissions, modes and
auth, all translated to app-server thread/turn/item traffic.

## Status

Initial implementation. Covered today:

- ndjson JSON-RPC duplex with id-correlated requests, inbound requests and
  notifications, LF-only framing (U+2028/U+2029 preserved inside strings)
- `codex app-server` lifecycle: spawn (feature `process`) or attach to
  existing streams, `initialize` handshake, typed calls, per-`threadId`
  event demultiplexing
- ACP v1 handlers: `initialize`, `authenticate` (api-key, chat-gpt device
  flow), `session/{new,load,resume,fork,list,close,delete}`, `session/prompt`,
  `session/cancel`, `session/set_mode`, `session/set_config_option`,
  `_`-extension methods (`session/set_model`, steering, logout, status)
- session modes mapped to codex approval + sandbox policies
  (`read-only`, `workspace-write`, `agent`, `agent-full-access`)
- config options: `mode`, `model`, `reasoning_effort`, `fast-mode`
- thread items and turn notifications -> `SessionUpdate` (agent text,
  reasoning, plans, tool calls with diffs and output, usage, stop reasons)
- approvals (`commandExecution`, `fileChange`, patch) bridged to
  `session/request_permission`; unsupported server requests answered with a
  refusal
- slash commands: `/compact`, `/status`, `/logout`,
  `/review`, `/review-branch`, `/review-commit`
- in-memory wire tests (request correlation, demux, approval round-trip,
  line-separator framing)

Tracked follow-ups live in the issue tracker (AIR extensions, native
subagent sessions, async tasks, session index/goals, live-pool integration
test).

## Usage

```toml
[dependencies]
codex-acp = { version = "0.1", features = ["process"] }
```

```rust,no_run
use codex_acp::{CodexAcpConfig, serve_stdio};

#[tokio::main]
async fn main() -> Result<(), agent_client_protocol::Error> {
    // Spawns `codex app-server`, performs the handshake and serves ACP on
    // process stdio — the usual editor-adapter entry point.
    serve_stdio(CodexAcpConfig::default()).await
}
```

To embed the bridge on existing streams instead of spawning a child:

```rust,ignore
let (server, drive) = AppServer::attach(read, write);
tokio::spawn(drive);          // or `or`/`race` it against your own loop
server.handshake(&spawn_config).await?;
serve(my_acp_transport, server, drive, CodexAcpConfig::default()).await?;
```

`serve` takes any `ConnectTo<Agent>` transport — `sdk::Stdio` for process
stdio, or `sdk::Lines`/custom transports for in-memory or socket pairs.

## Configuration

`CodexAcpConfig { spawn: SpawnConfig, default_mode: Option<String> }`

- `SpawnConfig::codex_path` / `CODEX_PATH` env — which `codex` to run
- `SpawnConfig::args` — extra args appended to `codex app-server` (e.g.
  `-c model_provider=...`)
- `SpawnConfig::env` — child environment additions
- `SpawnConfig::client_name`/`client_version` — reported at `initialize`
- `default_mode` — mode applied when the client selects none (`agent`)

## License

Licensed under either of [Apache-2.0](LICENSE-APACHE) or [MIT](LICENSE-MIT)
at your option.
