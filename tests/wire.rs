//! In-memory wire tests for the ndjson `Connection` and the `AppServer`
//! demultiplexer, driven by a scripted mock app-server peer.

use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};

use codex_acp::app_server::{AppEvent, AppServer, SpawnConfig};
use codex_acp::jsonrpc::{Connection, Incoming};
use futures_lite::io::{AsyncRead, AsyncWrite};
use futures_util::Stream as _;
use serde_json::{Value, json};

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

#[tokio::test(flavor = "multi_thread")]
async fn connection_roundtrip() {
    let (ar, aw, br, bw) = pipe();
    let (client, read_a) = Connection::new(ar, aw);
    let (server, read_b) = Connection::new(br, bw);
    tokio::spawn(read_a);
    tokio::spawn(read_b);

    let events = server.events();
    // The notification goes out first, then the request task; a single
    // consumer sees both frames in wire order.
    client.notify("wave", &json!({"hi": true})).await.unwrap();
    let ping = tokio::spawn(async move { client.request("ping", &json!({"x": 1})).await });
    let mut saw_wave = false;
    for _ in 0..2 {
        match events.recv().await.unwrap() {
            Incoming::Notification { method, .. } => {
                assert_eq!(method, "wave");
                saw_wave = true;
            }
            Incoming::Request { id, method, params } => {
                assert_eq!(method, "ping");
                server
                    .respond(&id, Ok(json!({"pong": params})))
                    .await
                    .unwrap();
            }
        }
    }
    assert!(saw_wave);
    assert_eq!(ping.await.unwrap().unwrap(), json!({"pong": {"x": 1}}));
}

#[tokio::test(flavor = "multi_thread")]
async fn app_server_handshake_and_demux() {
    let (ar, aw, br, bw) = pipe();
    let (peer, peer_read) = Connection::new(br, bw);
    let peer_events = peer.events();
    tokio::spawn(async move {
        tokio::spawn(peer_read);
        while let Ok(msg) = peer_events.recv().await {
            let Incoming::Request { id, method, .. } = msg else {
                continue;
            };
            match method.as_str() {
                "initialize" => {
                    peer.respond(&id, Ok(json!({"codexHome": "/tmp/x"})))
                        .await
                        .unwrap();
                }
                "thread/start" => {
                    peer.respond(
                        &id,
                        Ok(json!({
                            "thread": {"id": "t1", "cliVersion": "0", "createdAt": 0,
                                       "cwd": "/tmp", "ephemeral": false, "source": "cli"},
                            "model": "m", "approvalPolicy": "on-request",
                            "approvalsReviewer": "user", "cwd": "/tmp",
                            "sandbox": {"type": "workspaceWrite"},
                            "modelProvider": "p",
                        })),
                    )
                    .await
                    .unwrap();
                    peer.notify(
                        "item/started",
                        &json!({
                            "threadId": "t1", "turnId": "u1", "startedAtMs": 0,
                            "item": {"type": "commandExecution", "id": "i1",
                                     "command": "ls", "status": "inProgress",
                                     "commandActions": [], "cwd": "/tmp"},
                        }),
                    )
                    .await
                    .unwrap();
                    // An inbound approval request; the bridge must answer it.
                    let resp = peer
                        .request(
                            "item/commandExecution/requestApproval",
                            &json!({"threadId": "t1", "itemId": "i1", "turnId": "u1"}),
                        )
                        .await
                        .unwrap();
                    assert_eq!(resp["decision"], "accept");
                }
                _ => {
                    peer.respond(&id, Ok(json!({}))).await.unwrap();
                }
            }
        }
    });

    let (server, drive) = AppServer::attach(ar, aw);
    tokio::spawn(drive);
    server
        .handshake(&SpawnConfig {
            client_name: "test".into(),
            client_version: "0".into(),
            ..SpawnConfig::default()
        })
        .await
        .unwrap();

    let rx = server.subscribe("t1").await;
    let resp: Value = server
        .call("thread/start", &json!({"cwd": "/tmp"}))
        .await
        .unwrap();
    assert_eq!(resp["thread"]["id"], "t1");
    let first = rx.recv().await.unwrap();
    assert!(matches!(first, AppEvent::Notification(_)), "got {first:?}");
    match rx.recv().await.unwrap() {
        AppEvent::Request { id, .. } => {
            server
                .respond(&id, &json!({"decision": "accept"}))
                .await
                .unwrap();
        }
        other => panic!("expected approval request, got {other:?}"),
    }
}

/// Lines split only on `\n`: U+2028 inside a string is payload, not a break.
#[tokio::test(flavor = "multi_thread")]
async fn unicode_line_separators_do_not_split_frames() {
    let (ar, aw, br, bw) = pipe();
    let (client, read_a) = Connection::new(ar, aw);
    let (server, read_b) = Connection::new(br, bw);
    tokio::spawn(read_a);
    tokio::spawn(read_b);

    client
        .notify("sep", &json!({"text": "a\u{2028}b\u{2029}c"}))
        .await
        .unwrap();
    match server.events().recv().await.unwrap() {
        Incoming::Notification { params, .. } => {
            assert_eq!(params["text"], "a\u{2028}b\u{2029}c");
        }
        other @ Incoming::Request { .. } => panic!("expected notification, got {other:?}"),
    }
}
