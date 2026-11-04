//! Live integration test against a real `codex app-server` bound to a local
//! provider pool. Ignored by default; run with
//!
//! ```sh
//! POOL_BASE_URL=http://127.0.0.1:64395/v1 POOL_MODEL=gpt-6-luna \
//!     cargo test --test live -- --ignored --nocapture
//! ```
#![cfg(feature = "process")]

use std::time::Duration;

use codex_acp::app_server::{AppEvent, AppServer, SpawnConfig};
use codex_codes::{Notification, TurnStatus};
use serde_json::{Value, json};

fn pool_config() -> SpawnConfig {
    let base =
        std::env::var("POOL_BASE_URL").unwrap_or_else(|_| "http://127.0.0.1:64395/v1".into());
    let catalog = std::env::var("POOL_CATALOG").unwrap_or_else(|_| {
        format!(
            "{}/.config/acpsub/codex-catalog.json",
            std::env::var("HOME").unwrap_or_default()
        )
    });
    SpawnConfig {
        args: vec![
            "-c".into(),
            "model_provider=pool".into(),
            "-c".into(),
            "model_providers.pool.name=pool".into(),
            "-c".into(),
            format!("model_providers.pool.base_url={base}"),
            "-c".into(),
            "model_providers.pool.wire_api=responses".into(),
            "-c".into(),
            "model_providers.pool.env_key=MIRASIM_POOL_SESSION_TOKEN".into(),
            "-c".into(),
            "preferred_auth_method=apikey".into(),
            "-c".into(),
            format!("model_catalog_json={catalog}"),
        ],
        env: vec![("MIRASIM_POOL_SESSION_TOKEN".into(), "mirasim-pool".into())],
        client_name: "codex-acp-test".into(),
        client_version: "0".into(),
        experimental_api: true,
        codex_path: None,
    }
}

/// A complete turn through the real app-server: thread/start, turn/start,
/// streamed agent deltas, turn/completed.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires `codex` in PATH and a live provider pool"]
async fn live_pool_turn() {
    let model = std::env::var("POOL_MODEL").unwrap_or_else(|_| "gpt-6-luna".into());
    let cfg = pool_config();
    let (server, drive) = AppServer::spawn(&cfg).await.unwrap();
    tokio::spawn(drive);
    server.handshake(&cfg).await.unwrap();

    let start: Value = server
        .call(
            "thread/start",
            &json!({
                "cwd": env!("CARGO_MANIFEST_DIR"),
                "model": model,
                "approvalPolicy": "never",
                "sandbox": "read-only",
            }),
        )
        .await
        .unwrap();
    let tid = start["thread"]["id"].as_str().unwrap().to_string();
    let rx = server.subscribe(tid.clone()).await;

    server
        .call::<_, Value>(
            "turn/start",
            &json!({
                "threadId": tid,
                "input": [{"type": "text", "text": "Reply with exactly: pong"}],
            }),
        )
        .await
        .unwrap();

    let mut saw_text = false;
    let mut saw_error = false;
    let mut status = "none";
    let mut seen: Vec<String> = Vec::new();
    let collect = async {
        while let Ok(ev) = rx.recv().await {
            seen.push(format!("{ev:?}").chars().take(160).collect());
            match ev {
                AppEvent::Notification(Notification::AgentMessageDelta(d)) => {
                    if !d.delta.trim().is_empty() {
                        saw_text = true;
                    }
                }
                AppEvent::Notification(Notification::ItemCompleted(n)) => {
                    let item = serde_json::to_value(&n.item).unwrap_or_default();
                    let is_agent_msg = item.get("type").and_then(Value::as_str)
                        == Some("agentMessage")
                        && item
                            .get("text")
                            .and_then(Value::as_str)
                            .is_some_and(|t| !t.trim().is_empty());
                    if is_agent_msg {
                        saw_text = true;
                    }
                }
                AppEvent::Notification(Notification::TurnCompleted(n)) => {
                    status = match n.turn.status {
                        TurnStatus::Completed => "completed",
                        _ => "other",
                    };
                    break;
                }
                AppEvent::Notification(Notification::Error(_)) => {
                    saw_error = true;
                }
                AppEvent::Notification(Notification::ThreadStatusChanged(n)) => {
                    if format!("{:?}", n.status).contains("SystemError") {
                        saw_error = true;
                    }
                }
                AppEvent::Request { id, .. } => {
                    let _ = server
                        .respond_error(&id, -32601, "unexpected request")
                        .await;
                }
                _ => {}
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(120), collect)
        .await
        .expect("turn did not complete in 120s");
    for e in &seen {
        eprintln!("EV: {e}");
    }
    // Healthy upstream: a completed turn must deliver agent text. Exhausted
    // upstream (pool quota): codex still completes the turn with an error
    // status and an ErrorNotification — the bridge must surface both.
    if status == "completed" {
        assert!(saw_text, "completed turn delivered no agent text");
    } else {
        assert!(saw_error, "failed turn delivered no error notification");
    }
}
