# AGENTS.md

`codex-acp` is a library that serves OpenAI Codex as an Agent Client Protocol
(ACP) agent. It spawns `codex app-server` and translates between ACP
(JSON-RPC over ndjson, `jsonrpc` field present) and the app-server protocol
(JSON-RPC over ndjson, `jsonrpc` field absent — wire messages carry only
`id`/`method`/`params`/`result`/`error`).

## Build & test

```bash
cargo build
cargo test --all-features          # unit + mock app-server wire tests
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --all
cargo test --all-features -- --ignored   # live tests against a real `codex` binary
```

Live tests need a `codex` binary on PATH (or `CODEX_ACP_CODEX_PATH`) and a
reachable model provider; they are `#[ignore]`d by default.

## Layout

- `src/jsonrpc/` — the ndjson JSON-RPC duplex used on both edges (line-split
  on `\n` only; U+2028/U+2029 appear unescaped inside strings on the wire).
- `src/app_server/` — spawning `codex app-server`, the `initialize` handshake,
  typed request helpers, the inbound event stream, and answering server
  requests (approvals, elicitations).
- `src/agent/` — the ACP-facing half: session lifecycle, prompt handling,
  permission bridging, modes, config options, auth methods, slash commands.
- `src/translate/` — app-server notifications → ACP `session/update`
  notifications, and history replay for `session/load`.

## Conventions

- Runtime-agnostic: futures-lite/async-*
  primitives, never `tokio::*` in library code (dev-deps may use tokio).
- Errors are `thiserror` enums; diagnostics go through `tracing`.
- Wire types come from `codex-codes` (generated from the upstream app-server
  JSON schema); ACP types come from `agent-client-protocol`. Hand-write only
  what those crates do not model.
