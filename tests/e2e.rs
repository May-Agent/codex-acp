//! End-to-end: an ACP client over `rpc::Memory` drives the bridge, which
//! talks to a scripted fake `codex app-server` over in-memory byte pipes.
//! Both the v1 and v2 wire paths are exercised.

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use codex_acp::agent::{CodexAcpConfig, serve};
use codex_acp::aither_acp::rpc::{
    self, Handler, Notification, Peer, Request, RequestContext, Response,
};
use codex_acp::app_server::{AppServer, SpawnConfig};
use futures_lite::Stream as _;
use futures_lite::StreamExt as _;
use futures_lite::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt};
use serde_json::{Value, json};

// --- byte pipes --------------------------------------------------------------

struct ReadEnd {
    rx: Pin<Box<async_channel::Receiver<Vec<u8>>>>,
    buf: VecDeque<u8>,
}
struct WriteEnd {
    tx: async_channel::Sender<Vec<u8>>,
}

impl AsyncRead for ReadEnd {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        if self.buf.is_empty() {
            match self.rx.as_mut().poll_next(cx) {
                Poll::Ready(Some(chunk)) => self.buf.extend(chunk),
                Poll::Ready(None) => return Poll::Ready(Ok(0)),
                Poll::Pending => return Poll::Pending,
            }
        }
        let n = buf.len().min(self.buf.len());
        for (dst, src) in buf.iter_mut().zip(self.buf.drain(..n)) {
            *dst = src;
        }
        Poll::Ready(Ok(n))
    }
}

impl AsyncWrite for WriteEnd {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.tx.try_send(buf.to_vec()) {
            Ok(()) => Poll::Ready(Ok(buf.len())),
            Err(e) if e.is_full() => Poll::Pending,
            Err(_) => Poll::Ready(Err(std::io::ErrorKind::BrokenPipe.into())),
        }
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.tx.close();
        Poll::Ready(Ok(()))
    }
}

/// A duplex byte pipe; returns `(a.read, a.write, b.read, b.write)`.
fn pipe() -> (ReadEnd, WriteEnd, ReadEnd, WriteEnd) {
    let (a_tx, a_rx) = async_channel::unbounded();
    let (b_tx, b_rx) = async_channel::unbounded();
    (
        ReadEnd {
            rx: Box::pin(a_rx),
            buf: VecDeque::new(),
        },
        WriteEnd { tx: b_tx },
        ReadEnd {
            rx: Box::pin(b_rx),
            buf: VecDeque::new(),
        },
        WriteEnd { tx: a_tx },
    )
}

// --- scripted fake app-server ------------------------------------------------

/// The fake codex: answers the handshake + thread/turn calls the bridge
/// issues, and after `turn/start` streams one agent message delta and a
/// `turn/completed`.
async fn fake_codex(mut r: ReadEnd, mut w: WriteEnd) {
    let mut lines = futures_lite::io::BufReader::new(&mut r).lines();
    while let Some(line) = lines.next().await {
        let Ok(line) = line else { break };
        let Ok(v) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        let id = v.get("id").cloned();
        let method = v.get("method").and_then(Value::as_str).unwrap_or_default();
        let (result, after) = match method {
            "initialize" => (json!({"serverInfo": {"name": "fake-codex"}}), vec![]),
            "model/list" => (json!({"data": [], "nextCursor": null}), vec![]),
            "thread/start" => (
                json!({
                    "approvalPolicy": "on-request",
                    "approvalsReviewer": "auto_review",
                    "cwd": "/tmp",
                    "instructionSources": [],
                    "reasoningEffort": null,
                    "serviceTier": null,
                    "thread": {
                        "id": "thread-1",
                        "cwd": "/tmp",
                        "model": "gpt-5",
                        "modelProvider": "openai",
                    },
                }),
                vec![],
            ),
            "turn/start" => (
                json!({"turn": {"id": "turn-1", "status": "inProgress", "items": []}}),
                vec![
                    json!({"method": "item/agentMessage/delta", "params": {
                        "threadId": "thread-1", "turnId": "turn-1",
                        "itemId": "m1", "delta": "hello back",
                    }}),
                    json!({"method": "turn/completed", "params": {
                        "threadId": "thread-1", "turnId": "turn-1",
                        "turn": {"id": "turn-1", "status": "completed", "items": []},
                    }}),
                ],
            ),
            _ => (json!({}), vec![]),
        };
        if let Some(id) = id {
            let resp = json!({"id": id, "result": result});
            let mut line = resp.to_string();
            line.push('\n');
            let _ = w.write_all(line.as_bytes()).await;
        }
        for note in after {
            let mut line = note.to_string();
            line.push('\n');
            let _ = w.write_all(line.as_bytes()).await;
        }
    }
}

// --- client capture ----------------------------------------------------------

/// Records every `session/update` notification the bridge emits.
#[derive(Clone, Default)]
struct Capture {
    updates: Arc<Mutex<Vec<Value>>>,
}

impl Handler for Capture {
    fn request(
        &self,
        req: Request,
        _cx: RequestContext,
    ) -> impl std::future::Future<Output = Response> + Send {
        std::future::ready(Response::error(
            req.id,
            rpc::RpcError::method_not_found(&req.method),
        ))
    }

    async fn notification(&self, notification: Notification) {
        self.updates
            .lock()
            .expect("updates")
            .push(notification.params.unwrap_or_default());
    }
}

async fn wait_updates(capture: &Capture, pred: impl Fn(&[Value]) -> bool) {
    for _ in 0..200 {
        if pred(&capture.updates.lock().expect("updates")) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for updates");
}

/// Wire the bridge to a fake codex and a raw `Peer`; returns the peer, the
/// update capture, and the driver task.
fn wired() -> (Peer, Capture, tokio::task::JoinHandle<()>) {
    let (a_r, a_w, b_r, b_w) = pipe();
    let (server, drive) = AppServer::attach(a_r, a_w);
    tokio::spawn(fake_codex(b_r, b_w));

    let config = CodexAcpConfig {
        spawn: SpawnConfig {
            client_name: "e2e-test".to_string(),
            client_version: "0".to_string(),
            ..SpawnConfig::default()
        },
        default_mode: "agent".to_string(),
    };
    let (client_t, agent_t) = rpc::Memory::pair();
    tokio::spawn(serve(agent_t, server, drive, config));

    let capture = Capture::default();
    let (peer, conn) = rpc::connection(client_t, capture.clone());
    let drive_task = tokio::spawn(conn);
    (peer, capture, drive_task)
}

// --- v1 ----------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn v1_new_prompt_and_stop_reason() {
    let (peer, capture, _drive) = wired();

    let init: Value = peer
        .request(
            "initialize",
            &json!({"protocolVersion": 1, "clientCapabilities": {}}),
        )
        .await
        .expect("initialize");
    assert_eq!(init["protocolVersion"], 1);

    let new: Value = peer
        .request("session/new", &json!({"cwd": "/tmp"}))
        .await
        .expect("session/new");
    let session_id = new["sessionId"].as_str().expect("sessionId").to_string();
    // v1 clients see the mode-state and config options on the response.
    assert!(new.get("modes").is_some() || new.get("configOptions").is_some());

    let result: Value = peer
        .request(
            "session/prompt",
            &json!({
                "sessionId": session_id,
                "prompt": [{"type": "text", "text": "hi"}],
            }),
        )
        .await
        .expect("session/prompt");
    assert_eq!(result["stopReason"], "end_turn");

    wait_updates(&capture, |updates| {
        updates
            .iter()
            .any(|u| u["update"]["sessionUpdate"] == "agent_message_chunk")
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn v1_set_mode_via_config() {
    let (peer, _capture, _drive) = wired();
    peer.request::<Value, Value>(
        "initialize",
        &json!({"protocolVersion": 1, "clientCapabilities": {}}),
    )
    .await
    .expect("initialize");
    let new: Value = peer
        .request("session/new", &json!({"cwd": "/tmp"}))
        .await
        .expect("session/new");
    let session_id = new["sessionId"].as_str().unwrap().to_string();

    // Legacy session/set_mode → config-option lane.
    let _: Value = peer
        .request(
            "session/set_mode",
            &json!({"sessionId": session_id, "modeId": "read-only"}),
        )
        .await
        .expect("session/set_mode");

    // session/set_config_option on the model option.
    let _: Value = peer
        .request(
            "session/set_config_option",
            &json!({
                "sessionId": session_id,
                "configId": "reasoning_effort",
                "value": "high",
            }),
        )
        .await
        .expect("session/set_config_option");
}

// --- v2 ----------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn v2_new_prompt_and_streamed_update() {
    let (peer, capture, _drive) = wired();

    let init: Value = peer
        .request(
            "initialize",
            &json!({"protocolVersion": 2, "capabilities": {}}),
        )
        .await
        .expect("initialize");
    assert_eq!(init["protocolVersion"], 2);

    let new: Value = peer
        .request("session/new", &json!({"cwd": "/tmp"}))
        .await
        .expect("session/new");
    let session_id = new["sessionId"].as_str().expect("sessionId").to_string();
    // v2 setup carries the config options and available commands.
    assert!(new["configOptions"].is_array());

    let result: Value = peer
        .request(
            "session/prompt",
            &json!({
                "sessionId": session_id,
                "prompt": [{"type": "text", "text": "hi"}],
            }),
        )
        .await
        .expect("session/prompt");
    // v2 returns the message id up front; the turn finishes asynchronously.
    assert!(result["messageId"].is_string());

    wait_updates(&capture, |updates| {
        updates
            .iter()
            .any(|u| u["update"]["sessionUpdate"] == "agent_message_chunk")
    })
    .await;
    wait_updates(&capture, |updates| {
        updates.iter().any(|u| {
            u["update"]["sessionUpdate"] == "state_update" && u["update"]["state"] == "idle"
        })
    })
    .await;
}
