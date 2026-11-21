//! Live end-to-end integration test driving `codex_acp::serve()` against a real
//! `codex app-server` child process bound to a local provider pool.
//!
//! Ignored by default; run with:
//!
//! ```sh
//! CODEX_ACP_POOL_URL=http://127.0.0.1:64395/v1 CODEX_ACP_TEST_MODEL=gpt-6-luna \
//!     cargo nextest run -p codex-acp --test live --run-ignored only
//! ```
#![cfg(feature = "process")]

use std::net::TcpStream;
use std::time::Duration;

use codex_acp::aither_acp::client::{Client, MessageKind, SessionEvent, TurnOutcome};
use codex_acp::aither_acp::domain::{ContentBlock, Implementation, Open, StopReason};
use codex_acp::aither_acp::rpc;
use codex_acp::codex_sdk::SpawnConfig;
use codex_acp::{CodexAcpConfig, serve};
use futures_lite::StreamExt as _;

fn resolve_pool_url() -> String {
    std::env::var("CODEX_ACP_POOL_URL")
        .or_else(|_| std::env::var("POOL_BASE_URL"))
        .unwrap_or_else(|_| "http://127.0.0.1:64395/v1".to_string())
}

fn resolve_test_model() -> String {
    std::env::var("CODEX_ACP_TEST_MODEL")
        .or_else(|_| std::env::var("POOL_MODEL"))
        .unwrap_or_else(|_| "gpt-6-luna".to_string())
}

fn pool_reachable(base_url: &str) -> bool {
    let trimmed = base_url
        .strip_prefix("http://")
        .or_else(|| base_url.strip_prefix("https://"))
        .unwrap_or(base_url);
    let host_port = trimmed.split('/').next().unwrap_or(trimmed);
    let Ok(addrs) = std::net::ToSocketAddrs::to_socket_addrs(&host_port) else {
        return false;
    };
    for addr in addrs {
        if TcpStream::connect_timeout(&addr, Duration::from_millis(500)).is_ok() {
            return true;
        }
    }
    false
}

fn build_live_config(base_url: &str) -> CodexAcpConfig {
    let catalog = std::env::var("POOL_CATALOG").unwrap_or_else(|_| {
        format!(
            "{}/.config/acpsub/codex-catalog.json",
            std::env::var("HOME").unwrap_or_default()
        )
    });

    let mut spawn = SpawnConfig::new()
        .client_info("codex-acp-live-test", env!("CARGO_PKG_VERSION"))
        .experimental_api(true)
        .override_config("model_provider", "pool")
        .override_config("model_providers.pool.name", "pool")
        .override_config("model_providers.pool.base_url", base_url)
        .override_config("model_providers.pool.wire_api", "responses")
        .override_config("model_providers.pool.env_key", "MIRASIM_POOL_SESSION_TOKEN")
        .override_config("preferred_auth_method", "apikey")
        .env_var("MIRASIM_POOL_SESSION_TOKEN", "mirasim-pool");

    if std::path::Path::new(&catalog).exists() {
        spawn = spawn.override_config("model_catalog_json", catalog);
    }
    if let Ok(bin) = std::env::var("CODEX_ACP_CODEX_PATH").or_else(|_| std::env::var("CODEX_PATH"))
        && !bin.trim().is_empty()
    {
        spawn = spawn.codex_path(bin);
    }

    CodexAcpConfig::new().spawn(spawn).default_mode("read-only")
}

/// Drives `initialize` -> `session/new` -> `set_model` -> `session/prompt`
/// ("Reply with exact word: PONG") -> streamed updates -> clean teardown
/// through `codex_acp::serve()`.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires `codex` in PATH and a reachable local provider pool"]
async fn live_pool_turn_through_serve() {
    let base_url = resolve_pool_url();
    if !pool_reachable(&base_url) {
        tracing::warn!(%base_url, "skipping live test: provider pool endpoint unreachable");
        return;
    }
    let model = resolve_test_model();
    let cfg = build_live_config(&base_url);

    let (client_t, server_t) = rpc::Memory::pair();
    let (_peer, server_fut) = serve(server_t, cfg);
    let server_task = tokio::spawn(server_fut);

    let (client, conn) = Client::builder(Implementation::new("codex-acp-live", "0.1.0"))
        .connect(client_t)
        .await
        .expect("ACP initialize against live codex app-server");
    let conn_task = tokio::spawn(conn);

    let session = client
        .new_session(env!("CARGO_MANIFEST_DIR"))
        .open()
        .await
        .expect("live session/new");
    let _ = session.set_model(model.as_str()).await;

    let mut turn = session
        .prompt("Reply with exact word: PONG")
        .expect("live session/prompt");
    let mut saw_notice_error = false;
    let outcome = tokio::time::timeout(Duration::from_secs(120), async {
        while let Some(ev) = turn.next().await {
            if matches!(ev, SessionEvent::Notice(_)) {
                saw_notice_error = true;
            }
        }
        turn.finish().await
    })
    .await
    .expect("live turn timed out after 120s");

    let streamed_text: String = session
        .state()
        .messages
        .iter()
        .filter(|((kind, _), _)| *kind == MessageKind::Agent)
        .flat_map(|(_, m)| {
            m.content
                .as_ref()
                .into_option()
                .into_iter()
                .flatten()
                .filter_map(ContentBlock::as_text)
        })
        .collect();

    assert!(
        matches!(
            &outcome,
            TurnOutcome::Finished {
                stop_reason: Some(Open::Known(StopReason::EndTurn)),
                ..
            }
        ),
        "unexpected live turn outcome: {outcome:?}"
    );
    assert!(
        !streamed_text.trim().is_empty() || saw_notice_error,
        "live turn completed with neither streamed assistant text nor an upstream error notice"
    );

    session.close().await.expect("session/close");
    client.close().await;
    tokio::time::timeout(Duration::from_secs(5), conn_task)
        .await
        .expect("client connection shutdown")
        .expect("client task panicked");
    tokio::time::timeout(Duration::from_secs(5), server_task)
        .await
        .expect("server shutdown")
        .expect("server task panicked");
}
