//! Live integration test against a real `codex app-server` executable.
//!
//! Ignored by default; run with:
//!
//! ```sh
//! cargo test -p codex-sdk --test live -- --ignored --nocapture
//! ```
#![cfg(feature = "process")]

use std::time::Duration;

use codex_sdk::{
    AppServer, NoopServerRequestHandler, Notification, SandboxMode, SpawnConfig, ThreadEvent,
    ThreadStartParams, TurnStartParams, TurnStatus, UserInput,
};

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires `codex` on PATH (or CODEX_PATH) and configured model credentials"]
async fn live_app_server_turn_roundtrip() {
    let cfg = SpawnConfig::new()
        .client_info("codex-sdk-live-test", env!("CARGO_PKG_VERSION"))
        .experimental_api(true);

    let (server, drive) = AppServer::spawn(&cfg, NoopServerRequestHandler).unwrap();
    let drive_task = tokio::spawn(drive);

    server.handshake(&cfg).await.unwrap();

    let started = server
        .thread_start(&ThreadStartParams {
            cwd: Some(env!("CARGO_MANIFEST_DIR").to_string()),
            sandbox: Some(SandboxMode::Read_only),
            ..ThreadStartParams::default()
        })
        .await
        .unwrap();
    let rx = server.subscribe(&started.thread.id);

    let _ = server
        .turn_start(&TurnStartParams {
            thread_id: started.thread.id.clone(),
            input: vec![UserInput::Text {
                text: "Reply with the single word: pong".to_string(),
                text_elements: None,
            }],
            ..TurnStartParams::default()
        })
        .await
        .unwrap();

    let mut saw_text = false;
    let mut saw_error = false;
    let mut completed_ok = false;

    tokio::time::timeout(Duration::from_secs(120), async {
        while let Ok(event) = rx.recv().await {
            match event {
                ThreadEvent::Notification(Notification::AgentMessageDelta(d))
                    if !d.delta.trim().is_empty() =>
                {
                    saw_text = true;
                }
                ThreadEvent::Notification(Notification::Error(_)) => {
                    saw_error = true;
                }
                ThreadEvent::Notification(Notification::TurnCompleted(done)) => {
                    completed_ok = matches!(done.turn.status, TurnStatus::Completed);
                    break;
                }
                _ => {}
            }
        }
    })
    .await
    .expect("live turn did not complete within 120s");

    if completed_ok {
        assert!(
            saw_text,
            "completed turn produced no assistant message delta"
        );
    } else {
        assert!(
            saw_error,
            "unsuccessful turn produced no error notification"
        );
    }

    server.close();
    drive_task.await.unwrap();
}
