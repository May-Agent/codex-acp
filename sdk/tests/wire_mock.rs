//! In-memory mock `codex app-server` wire tests and `process`-gated `/bin/sh`
//! child lifecycle tests for `codex-sdk`.

use std::future::Future;
use std::pin::{Pin, pin};
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll, Waker};

use async_channel::{Receiver, Sender};
use codex_sdk::{
    AnyServerRequestHandler, AppServer, CommandExecutionRequestApprovalParams,
    CommandExecutionRequestApprovalResponse, Connection, FileChangeRequestApprovalParams,
    FileChangeRequestApprovalResponse, GetAccountParams, GetAccountRateLimitsParams, HandlerResult,
    Incoming, ListMcpServerStatusParams, LoginAccountParams, LoginAccountResponse,
    METHOD_NOT_FOUND_CODE, ModelListParams, NdjsonReader, NdjsonWriter, NoopServerRequestHandler,
    Notification, RemoteError, ReviewStartParams, ReviewTarget, SdkError, ServerRequestHandler,
    SpawnConfig, ThreadCompactStartParams, ThreadDeleteParams, ThreadEvent, ThreadForkParams,
    ThreadListParams, ThreadReadParams, ThreadResumeParams, ThreadStartParams,
    ThreadUnsubscribeParams, TurnInterruptParams, TurnStartParams, TurnSteerParams, UserInput,
};
use futures_lite::io::{AsyncRead, AsyncWrite};
use futures_lite::stream::Stream;
use serde_json::{Value, json};

struct PipeRead {
    rx: Pin<Box<Receiver<Vec<u8>>>>,
    buf: Vec<u8>,
    pos: usize,
}

struct PipeWrite {
    tx: Sender<Vec<u8>>,
}

fn duplex_pipes() -> ((PipeRead, PipeWrite), (PipeRead, PipeWrite)) {
    let (a_to_b_tx, a_to_b_rx) = async_channel::unbounded();
    let (b_to_a_tx, b_to_a_rx) = async_channel::unbounded();
    (
        (
            PipeRead {
                rx: Box::pin(b_to_a_rx),
                buf: Vec::new(),
                pos: 0,
            },
            PipeWrite { tx: a_to_b_tx },
        ),
        (
            PipeRead {
                rx: Box::pin(a_to_b_rx),
                buf: Vec::new(),
                pos: 0,
            },
            PipeWrite { tx: b_to_a_tx },
        ),
    )
}

impl AsyncRead for PipeRead {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        out: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = &mut *self;
        if this.pos < this.buf.len() {
            let rem = &this.buf[this.pos..];
            let n = rem.len().min(out.len());
            out[..n].copy_from_slice(&rem[..n]);
            this.pos += n;
            if this.pos == this.buf.len() {
                this.buf.clear();
                this.pos = 0;
            }
            return Poll::Ready(Ok(n));
        }

        match this.rx.as_mut().poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(Ok(0)),
            Poll::Ready(Some(chunk)) => {
                let n = chunk.len().min(out.len());
                out[..n].copy_from_slice(&chunk[..n]);
                if n < chunk.len() {
                    this.buf = chunk;
                    this.pos = n;
                }
                Poll::Ready(Ok(n))
            }
        }
    }
}

impl AsyncWrite for PipeWrite {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        self.tx
            .try_send(buf.to_vec())
            .map(|()| buf.len())
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "pipe closed"))
            .into()
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.tx.close();
        Poll::Ready(Ok(()))
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn typed_mcp_status_and_rate_limits_helpers() {
    let ((client_r, client_w), (server_r, server_w)) = duplex_pipes();
    let (server, drive) = AppServer::attach(client_r, client_w, NoopServerRequestHandler);
    let drive_task = tokio::spawn(drive);

    let mock_server = tokio::spawn(async move {
        let mut reader = NdjsonReader::new(server_r);
        let mut writer = NdjsonWriter::new(server_w);

        let line1 = reader.next_line().await.unwrap().unwrap();
        let req1: Value = serde_json::from_str(&line1).unwrap();
        assert_eq!(req1["method"], "mcpServerStatus/list");
        assert_eq!(req1["params"]["threadId"], "thread-9");
        assert_eq!(req1["params"]["limit"], 5);
        writer
            .send_json(&json!({
                "id": req1["id"],
                "result": {
                    "data": [{
                        "name": "flyco",
                        "authStatus": "unsupported",
                        "tools": {},
                        "resources": [],
                        "resourceTemplates": []
                    }],
                    "nextCursor": "c-next"
                }
            }))
            .await
            .unwrap();

        let line2 = reader.next_line().await.unwrap().unwrap();
        let req2: Value = serde_json::from_str(&line2).unwrap();
        assert_eq!(req2["method"], "account/rateLimits/read");
        assert_eq!(req2["params"], json!({}));
        writer
            .send_json(&json!({
                "id": req2["id"],
                "result": {
                    "accountId": "acct-42",
                    "ordinaryUsageAllowed": true,
                    "rateLimits": {
                        "primary": {"usedPercent": 50, "windowDurationMins": 300}
                    }
                }
            }))
            .await
            .unwrap();
    });

    let mcp = server
        .mcp_server_status_list(&ListMcpServerStatusParams {
            thread_id: Some("thread-9".to_string()),
            limit: Some(5),
            ..ListMcpServerStatusParams::default()
        })
        .await
        .expect("mcp_server_status_list");
    assert_eq!(mcp.data.len(), 1);
    assert_eq!(mcp.data[0].name, "flyco");
    assert_eq!(mcp.next_cursor.as_deref(), Some("c-next"));

    let rates = server
        .account_rate_limits_read(&GetAccountRateLimitsParams::default())
        .await
        .expect("account_rate_limits_read");
    assert_eq!(rates.account_id.as_deref(), Some("acct-42"));
    assert_eq!(rates.ordinary_usage_allowed, Some(true));
    assert_eq!(rates.rate_limits["primary"]["usedPercent"], 50);

    server.close();
    mock_server.await.unwrap();
    drive_task.await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn correlation_out_of_order_and_remote_error_decoding() {
    let ((c_read, c_write), (s_read, s_write)) = duplex_pipes();
    let (client, client_drive) = Connection::new(c_read, c_write);
    let client = Arc::new(client);
    tokio::spawn(client_drive);

    let mut raw_in = NdjsonReader::new(s_read);
    let mut raw_out = NdjsonWriter::new(s_write);

    let peer_task = tokio::spawn(async move {
        let line1 = raw_in.next_line().await.unwrap().unwrap();
        let line2 = raw_in.next_line().await.unwrap().unwrap();
        let line3 = raw_in.next_line().await.unwrap().unwrap();

        // App-server wire format omits `"jsonrpc": "2.0"`.
        assert!(!line1.contains("jsonrpc"));
        assert!(!line2.contains("jsonrpc"));
        assert!(!line3.contains("jsonrpc"));

        let mut reqs: Vec<Value> = [line1, line2, line3]
            .iter()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        // Reply strictly in reverse wire arrival order to exercise out-of-order correlation.
        reqs.reverse();

        for req in reqs {
            let reply = match req["method"].as_str().unwrap() {
                "op/one" => json!({"id": req["id"], "result": {"n": 1}}),
                "op/two" => json!({"id": req["id"], "result": {"n": 2}}),
                "op/err" => json!({
                    "id": req["id"],
                    "error": {
                        "code": -32600,
                        "message": "provider mismatch",
                        "data": {
                            "type": "modelProviderMismatch",
                            "modelProvider": "old-p",
                            "currentModelProvider": "new-p"
                        }
                    }
                }),
                other => panic!("unexpected method {other}"),
            };
            raw_out.send_json(&reply).await.unwrap();
        }
    });

    let c1 = Arc::clone(&client);
    let c2 = Arc::clone(&client);
    let c3 = Arc::clone(&client);
    let f1 = tokio::spawn(async move { c1.request("op/one", &json!({"a": 1})).await });
    let f2 = tokio::spawn(async move { c2.request("op/two", &json!({"a": 2})).await });
    let f3 = tokio::spawn(async move { c3.request("op/err", &json!({})).await });

    assert_eq!(f1.await.unwrap().unwrap(), json!({"n": 1}));
    assert_eq!(f2.await.unwrap().unwrap(), json!({"n": 2}));

    let err = f3.await.unwrap().unwrap_err();
    match err {
        SdkError::Remote(remote) => {
            assert_eq!(remote.code, -32600);
            assert_eq!(remote.message, "provider mismatch");
            assert!(remote.model_provider_error().is_some());
        }
        other => panic!("expected SdkError::Remote, got {other:?}"),
    }

    peer_task.await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn dropped_request_cleans_up_pending_map_and_eof_fails_with_closed() {
    let ((c_read, c_write), (s_read, s_write)) = duplex_pipes();
    let (client, client_drive) = Connection::new(c_read, c_write);
    tokio::spawn(client_drive);

    let mut raw_in = NdjsonReader::new(s_read);
    let mut raw_out = NdjsonWriter::new(s_write);

    // Start a request, poll once so it inserts into `pending` and writes the frame,
    // then drop the future before any response arrives.
    {
        let mut req_fut = pin!(client.request("slow/op", &json!({"k": "v"})));
        let mut cx = Context::from_waker(Waker::noop());
        assert!(matches!(req_fut.as_mut().poll(&mut cx), Poll::Pending));
        assert_eq!(client.pending_count(), 1);
    }
    // `PendingGuard::drop` must immediately clear the leaked entry.
    assert_eq!(client.pending_count(), 0);

    let cancelled_frame: Value = raw_in.recv_json().await.unwrap().unwrap();
    assert_eq!(cancelled_frame["method"], "slow/op");
    // Late response for the dropped request must be harmlessly ignored.
    raw_out
        .send_json(&json!({"id": cancelled_frame["id"], "result": {"late": true}}))
        .await
        .unwrap();

    // Subsequent live request still correlates cleanly.
    let live_client = &client;
    let live_fut = async { live_client.request("fast/op", &json!({})).await };
    let answer_fut = async {
        let req: Value = raw_in.recv_json().await.unwrap().unwrap();
        assert_eq!(req["method"], "fast/op");
        raw_out
            .send_json(&json!({"id": req["id"], "result": {"ok": true}}))
            .await
            .unwrap();
    };
    let (live_res, ()) = futures_lite::future::zip(live_fut, answer_fut).await;
    assert_eq!(live_res.unwrap(), json!({"ok": true}));
    assert_eq!(client.pending_count(), 0);

    // Now drop `raw_out` while two requests are pending: both must resolve to `SdkError::Closed`.
    let pending_a = client.request("wait/a", &json!({}));
    let pending_b = client.request("wait/b", &json!({}));
    let close_peer = async move {
        let _ = raw_in.recv_json::<Value>().await;
        let _ = raw_in.recv_json::<Value>().await;
        drop(raw_out);
    };
    let ((res_a, res_b), ()) =
        futures_lite::future::zip(futures_lite::future::zip(pending_a, pending_b), close_peer)
            .await;

    assert!(matches!(res_a, Err(SdkError::Closed)));
    assert!(matches!(res_b, Err(SdkError::Closed)));
    assert!(client.is_closed());
    assert_eq!(client.pending_count(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn preserves_u2028_u2029_and_rejects_embedded_newlines() {
    let ((c_read, c_write), (s_read, s_write)) = duplex_pipes();
    let (client, client_drive) = Connection::new(c_read, c_write);
    let (server, server_drive) = Connection::new(s_read, s_write);
    tokio::spawn(client_drive);
    tokio::spawn(server_drive);

    let text = "alpha\u{2028}beta\u{2029}gamma";
    client
        .notify("unicode/sep", &json!({"body": text}))
        .await
        .unwrap();
    match server.events().recv().await.unwrap() {
        Incoming::Notification { method, params } => {
            assert_eq!(method, "unicode/sep");
            assert_eq!(params.unwrap()["body"], text);
        }
        other @ Incoming::Request { .. } => panic!("expected notification, got {other:?}"),
    }

    let ((_, bad_write), _) = duplex_pipes();
    let mut writer = NdjsonWriter::new(bad_write);
    let lf_err = writer
        .write_raw_line("{\"method\":\"a\"}\n{\"method\":\"b\"}")
        .await
        .unwrap_err();
    assert!(matches!(lf_err, SdkError::Protocol(_)));
    let cr_err = writer
        .write_raw_line("{\"method\":\"a\"}\r{\"method\":\"b\"}")
        .await
        .unwrap_err();
    assert!(matches!(cr_err, SdkError::Protocol(_)));
}

fn mock_thread_response(tid: &str) -> Value {
    json!({
        "thread": {
            "id": tid,
            "cliVersion": "0.170.0",
            "createdAt": 1,
            "cwd": "/tmp",
            "ephemeral": false,
            "source": "cli"
        },
        "model": "gpt-5",
        "approvalPolicy": "on-request",
        "approvalsReviewer": "user",
        "cwd": "/tmp",
        "sandbox": {"type": "workspaceWrite"},
        "modelProvider": "openai"
    })
}

async fn run_demux_mock_peer(peer: Connection<PipeWrite>) {
    let peer_events = peer.events();
    while let Ok(msg) = peer_events.recv().await {
        let Incoming::Request { id, method, params } = msg else {
            continue;
        };
        match method.as_str() {
            "initialize" => {
                peer.respond(&id, Ok(json!({"codexHome": "/tmp/codex"})))
                    .await
                    .unwrap();
            }
            "thread/start" => {
                // Emit `thread/started` BEFORE the `thread/start` response to exercise
                // early per-thread buffering before `server.subscribe("t-early")` runs.
                peer.notify(
                    "thread/started",
                    &json!({
                        "thread": {
                            "id": "t-early",
                            "cliVersion": "0.170.0",
                            "createdAt": 1,
                            "cwd": "/tmp",
                            "ephemeral": false,
                            "source": "cli"
                        }
                    }),
                )
                .await
                .unwrap();
                peer.notify(
                    "item/agentMessage/delta",
                    &json!({
                        "threadId": "t-early",
                        "turnId": "u-early",
                        "itemId": "i-early",
                        "delta": "buffered-before-subscribe"
                    }),
                )
                .await
                .unwrap();
                peer.respond(&id, Ok(mock_thread_response("t-early")))
                    .await
                    .unwrap();
            }
            "thread/resume" => {
                let tid = params
                    .as_ref()
                    .and_then(|p| p.get("threadId"))
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                peer.notify(
                    "custom/legacyEvent",
                    &json!({"conversationId": tid, "detail": "via-conversation-id"}),
                )
                .await
                .unwrap();
                peer.notify(
                    "account/login/completed",
                    &json!({"loginId": "l-1", "success": true}),
                )
                .await
                .unwrap();
                peer.respond(&id, Ok(mock_thread_response(&tid)))
                    .await
                    .unwrap();
            }
            _ => peer.respond(&id, Ok(json!({}))).await.unwrap(),
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn demux_routes_thread_id_legacy_conversation_id_early_start_and_global_events() {
    let ((c_read, c_write), (s_read, s_write)) = duplex_pipes();
    let (peer, peer_drive) = Connection::new(s_read, s_write);
    tokio::spawn(peer_drive);
    tokio::spawn(run_demux_mock_peer(peer));

    let (server, drive) = AppServer::attach(c_read, c_write, NoopServerRequestHandler);
    tokio::spawn(drive);

    let cfg = SpawnConfig::new().client_info("sdk-test", "0.1.0");
    let init_val = server.handshake(&cfg).await.unwrap();
    assert_eq!(init_val["codexHome"], "/tmp/codex");

    // 1. `thread/start` where notifications arrived BEFORE `subscribe("t-early")`.
    let started = server
        .thread_start(&ThreadStartParams::default())
        .await
        .unwrap();
    assert_eq!(started.thread.id, "t-early");
    let early_rx = server.subscribe(&started.thread.id);

    let ev1 = early_rx.recv().await.unwrap();
    assert!(matches!(
        ev1,
        ThreadEvent::Notification(Notification::ThreadStarted(ref n)) if n.thread.id == "t-early"
    ));
    let ev2 = early_rx.recv().await.unwrap();
    assert!(matches!(
        ev2,
        ThreadEvent::Notification(Notification::AgentMessageDelta(ref d)) if d.delta == "buffered-before-subscribe"
    ));

    // 2. Subscribe BEFORE `thread/resume` and verify legacy `conversationId` routing + `global_events`.
    let global_rx = server.global_events();
    let resume_sub = server.subscribe_scoped("legacy-thread-1");
    let resumed = server
        .thread_resume(&ThreadResumeParams {
            thread_id: "legacy-thread-1".to_string(),
            ..ThreadResumeParams::default()
        })
        .await
        .unwrap();
    assert_eq!(resumed.thread.id, "legacy-thread-1");

    let legacy_ev = resume_sub.recv().await.unwrap();
    match legacy_ev {
        ThreadEvent::Notification(Notification::Unknown { method, params }) => {
            assert_eq!(method, "custom/legacyEvent");
            assert_eq!(params.unwrap()["detail"], "via-conversation-id");
        }
        other => panic!("expected Unknown notification, got {other:?}"),
    }

    let global_ev = global_rx.recv().await.unwrap();
    assert!(matches!(
        global_ev,
        ThreadEvent::Notification(Notification::AccountLoginCompleted(_))
    ));
}

struct TestApprovalHandler {
    gate_rx: Receiver<()>,
    server: Arc<OnceLock<AppServer>>,
}

impl ServerRequestHandler for TestApprovalHandler {
    fn command_execution_approval(
        &self,
        params: CommandExecutionRequestApprovalParams,
    ) -> impl Future<Output = HandlerResult<CommandExecutionRequestApprovalResponse>> + Send {
        let gate_rx = self.gate_rx.clone();
        let server = Arc::clone(&self.server);
        async move {
            if params.item_id == "slow-cmd" {
                let _ = gate_rx.recv().await;
                return Ok(CommandExecutionRequestApprovalResponse::accept_for_session());
            }
            // Exercise re-entrant outbound RPC (`model_list`) from inside an approval callback!
            if let Some(srv) = server.get() {
                let models = srv
                    .model_list(&ModelListParams::default())
                    .await
                    .map_err(|e| RemoteError::internal_error(e.to_string()))?;
                if models.data.is_empty() {
                    return Ok(CommandExecutionRequestApprovalResponse::decline());
                }
            }
            Ok(CommandExecutionRequestApprovalResponse::accept())
        }
    }

    fn file_change_approval(
        &self,
        _params: FileChangeRequestApprovalParams,
    ) -> impl Future<Output = HandlerResult<FileChangeRequestApprovalResponse>> + Send {
        std::future::ready(Ok(FileChangeRequestApprovalResponse::accept()))
    }

    fn unknown_request(
        &self,
        method: String,
        params: Option<Value>,
    ) -> impl Future<Output = HandlerResult<Value>> + Send {
        let reply = if method == "custom/echo" {
            Ok(json!({"echo": params}))
        } else {
            Err(RemoteError::method_not_found(&method))
        };
        std::future::ready(reply)
    }
}

async fn run_approval_mock_peer(peer: Arc<Connection<PipeWrite>>) {
    let peer_events = peer.events();
    while let Ok(msg) = peer_events.recv().await {
        let Incoming::Request { id, method, .. } = msg else {
            continue;
        };
        let reply = match method.as_str() {
            "model/list" => json!({
                "data": [{
                    "id": "gpt-5",
                    "model": "gpt-5",
                    "displayName": "GPT-5",
                    "description": "flagship",
                    "supportedReasoningEfforts": [],
                    "defaultReasoningEffort": "medium",
                    "isDefault": true
                }]
            }),
            "turn/start" => json!({
                "turn": {"id": "turn-1", "items": [], "status": "inProgress"}
            }),
            _ => json!({}),
        };
        peer.respond(&id, Ok(reply)).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_server_requests_out_of_turn_and_mid_turn_without_blocking() {
    let ((c_read, c_write), (s_read, s_write)) = duplex_pipes();
    let (peer, peer_drive) = Connection::new(s_read, s_write);
    let peer = Arc::new(peer);
    tokio::spawn(peer_drive);

    let (gate_tx, gate_rx) = async_channel::bounded::<()>(1);
    let server_slot = Arc::new(OnceLock::new());
    let handler = AnyServerRequestHandler::new(TestApprovalHandler {
        gate_rx,
        server: Arc::clone(&server_slot),
    });

    let (server, drive) = AppServer::attach(c_read, c_write, handler);
    let _ = server_slot.set(server.clone());
    tokio::spawn(drive);
    tokio::spawn(run_approval_mock_peer(Arc::clone(&peer)));

    // 1. Out-of-turn server requests BEFORE any thread or turn is started!
    let out_of_turn_file = peer
        .request(
            "item/fileChange/requestApproval",
            &json!({"threadId": "t-idle", "turnId": "u-0", "itemId": "f-1"}),
        )
        .await
        .unwrap();
    assert_eq!(out_of_turn_file["decision"], "accept");

    let out_of_turn_custom = peer
        .request("custom/echo", &json!({"ping": 42}))
        .await
        .unwrap();
    assert_eq!(out_of_turn_custom, json!({"echo": {"ping": 42}}));

    let unsupported_err = peer
        .request("custom/unknownMethod", &json!({}))
        .await
        .unwrap_err();
    assert!(matches!(
        unsupported_err,
        SdkError::Remote(ref r) if r.code == METHOD_NOT_FOUND_CODE
    ));

    // 2. Mid-turn concurrent requests: `slow-cmd` blocks on `gate_tx`, while `fast-cmd`
    // (which makes a re-entrant `model/list` RPC call!) and streamed notifications complete
    // immediately without waiting for `slow-cmd`.
    let thread_rx = server.subscribe("t-mid");
    let turn_resp = server
        .turn_start(&TurnStartParams {
            thread_id: "t-mid".to_string(),
            input: vec![UserInput::Text {
                text: "run commands".to_string(),
                text_elements: None,
            }],
            ..TurnStartParams::default()
        })
        .await
        .unwrap();
    assert_eq!(turn_resp.turn.id, "turn-1");

    let peer_slow = Arc::clone(&peer);
    let slow_task = tokio::spawn(async move {
        peer_slow
            .request(
                "item/commandExecution/requestApproval",
                &json!({"threadId": "t-mid", "turnId": "turn-1", "itemId": "slow-cmd"}),
            )
            .await
            .unwrap()
    });

    tokio::task::yield_now().await;

    let fast_decision = peer
        .request(
            "item/commandExecution/requestApproval",
            &json!({"threadId": "t-mid", "turnId": "turn-1", "itemId": "fast-cmd"}),
        )
        .await
        .unwrap();
    assert_eq!(fast_decision["decision"], "accept");

    peer.notify(
        "item/agentMessage/delta",
        &json!({
            "threadId": "t-mid",
            "turnId": "turn-1",
            "itemId": "msg-1",
            "delta": "streaming-while-approval-pending"
        }),
    )
    .await
    .unwrap();

    let delta_ev = thread_rx.recv().await.unwrap();
    assert!(matches!(
        delta_ev,
        ThreadEvent::Notification(Notification::AgentMessageDelta(ref d))
            if d.delta == "streaming-while-approval-pending"
    ));

    gate_tx.send(()).await.unwrap();
    let slow_decision = slow_task.await.unwrap();
    assert_eq!(slow_decision["decision"], "acceptForSession");
}

async fn run_typed_methods_mock_peer(peer: Connection<PipeWrite>) {
    let peer_events = peer.events();
    while let Ok(msg) = peer_events.recv().await {
        let Incoming::Request { id, method, .. } = msg else {
            continue;
        };
        let reply = match method.as_str() {
            "thread/fork" => mock_thread_response("t-fork"),
            "thread/read" => json!({
                "thread": {
                    "id": "t-fork",
                    "cliVersion": "0.170.0",
                    "createdAt": 1,
                    "cwd": "/tmp",
                    "ephemeral": false,
                    "source": "cli"
                }
            }),
            "thread/list" => json!({"data": [], "nextCursor": null}),
            "thread/delete" | "thread/compact/start" | "turn/interrupt" | "account/logout" => {
                json!({})
            }
            "thread/unsubscribe" => json!({"status": "unsubscribed"}),
            "turn/steer" => json!({"turnId": "turn-1"}),
            "review/start" => json!({
                "reviewThreadId": "t-fork",
                "turn": {"id": "turn-rev", "items": [], "status": "inProgress"}
            }),
            "account/login/start" => json!({"type": "apiKey"}),
            "account/read" => json!({"account": null, "requiresOpenaiAuth": false}),
            other => panic!("unexpected method {other}"),
        };
        peer.respond(&id, Ok(reply)).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn typed_convenience_methods_roundtrip() {
    let ((c_read, c_write), (s_read, s_write)) = duplex_pipes();
    let (peer, peer_drive) = Connection::new(s_read, s_write);
    tokio::spawn(peer_drive);
    tokio::spawn(run_typed_methods_mock_peer(peer));

    let (server, drive) = AppServer::attach(c_read, c_write, NoopServerRequestHandler);
    tokio::spawn(drive);

    let forked = server
        .thread_fork(&ThreadForkParams {
            thread_id: "t-orig".to_string(),
            ..ThreadForkParams::default()
        })
        .await
        .unwrap();
    assert_eq!(forked.thread.id, "t-fork");

    let read = server
        .thread_read(&ThreadReadParams {
            thread_id: "t-fork".to_string(),
            include_turns: Some(true),
        })
        .await
        .unwrap();
    assert_eq!(read.thread.id, "t-fork");

    let listed = server
        .thread_list(&ThreadListParams::default())
        .await
        .unwrap();
    assert_eq!(listed.data.len(), 0);

    let _ = server
        .thread_compact_start(&ThreadCompactStartParams {
            thread_id: "t-fork".to_string(),
        })
        .await
        .unwrap();

    let _ = server
        .turn_interrupt(&TurnInterruptParams {
            thread_id: "t-fork".to_string(),
            turn_id: "turn-1".to_string(),
        })
        .await
        .unwrap();

    let steered = server
        .turn_steer(&TurnSteerParams {
            thread_id: "t-fork".to_string(),
            expected_turn_id: "turn-1".to_string(),
            input: Vec::new(),
            client_user_message_id: None,
        })
        .await
        .unwrap();
    assert_eq!(steered.turn_id, "turn-1");

    let review = server
        .review_start(&ReviewStartParams {
            thread_id: "t-fork".to_string(),
            target: ReviewTarget::UncommittedChanges,
            delivery: None,
        })
        .await
        .unwrap();
    assert_eq!(review.turn.id, "turn-rev");

    let login = server
        .account_login_start(&LoginAccountParams::ApiKey {
            api_key: "sk-test".to_string(),
        })
        .await
        .unwrap();
    assert_eq!(login, LoginAccountResponse::ApiKey);

    let acct = server
        .account_read(&GetAccountParams::default())
        .await
        .unwrap();
    assert!(!acct.requires_openai_auth);

    let _ = server.account_logout().await.unwrap();

    let unsub = server
        .thread_unsubscribe(&ThreadUnsubscribeParams {
            thread_id: "t-fork".to_string(),
        })
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(unsub.status).unwrap(),
        json!("unsubscribed")
    );

    let _ = server
        .thread_delete(&ThreadDeleteParams {
            thread_id: "t-fork".to_string(),
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn global_events_broadcasts_and_caps_unsubscribed_backlog() {
    let ((c_read, c_write), (s_read, s_write)) = duplex_pipes();
    let (peer, peer_drive) = Connection::new(s_read, s_write);
    tokio::spawn(peer_drive);

    let (server, drive) = AppServer::attach(c_read, c_write, NoopServerRequestHandler);
    tokio::spawn(drive);

    // Emit 300 global events while nobody is subscribed; only the latest 256 must be retained.
    for idx in 0..300_u32 {
        peer.notify(
            "account/login/completed",
            &json!({"loginId": format!("early-{idx}"), "success": true}),
        )
        .await
        .unwrap();
    }
    let _ = peer.request("sync/barrier", &json!({})).await;

    let rx_a = server.global_events();
    let rx_b = server.global_events();

    let mut early_count = 0_usize;
    while rx_a.try_recv().is_ok() {
        early_count += 1;
    }
    assert_eq!(early_count, 256);

    // Broadcast check: both `rx_a` and `rx_b` must receive new global events (no work-stealing).
    peer.notify(
        "account/login/completed",
        &json!({"loginId": "broadcast-1", "success": true}),
    )
    .await
    .unwrap();

    let ev_a = rx_a.recv().await.unwrap();
    let ev_b = rx_b.recv().await.unwrap();
    assert!(matches!(
        ev_a,
        ThreadEvent::Notification(Notification::AccountLoginCompleted(ref n))
            if n.login_id.as_deref() == Some("broadcast-1")
    ));
    assert!(matches!(
        ev_b,
        ThreadEvent::Notification(Notification::AccountLoginCompleted(ref n))
            if n.login_id.as_deref() == Some("broadcast-1")
    ));
}

#[cfg(all(feature = "process", unix))]
mod process_tests {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    use super::*;

    static SANDBOX_SEQ: AtomicU64 = AtomicU64::new(1);

    struct ScriptSandbox {
        dir: PathBuf,
        grandchild_pid_file: PathBuf,
    }

    impl ScriptSandbox {
        fn new() -> Self {
            let seq = SANDBOX_SEQ.fetch_add(1, Ordering::Relaxed);
            let dir = std::env::temp_dir().join(format!(
                "codex-sdk-process-test-{}-{seq}",
                std::process::id()
            ));
            std::fs::create_dir_all(&dir).unwrap();
            let grandchild_pid_file = dir.join("grandchild.pid");
            Self {
                dir,
                grandchild_pid_file,
            }
        }

        fn write_script(&self, name: &str, body: &str) -> PathBuf {
            use std::os::unix::fs::PermissionsExt;

            let path = self.dir.join(name);
            std::fs::write(&path, body).unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
            path
        }
    }

    impl Drop for ScriptSandbox {
        fn drop(&mut self) {
            if let Ok(raw_pid) = std::fs::read_to_string(&self.grandchild_pid_file) {
                let pid = raw_pid.trim();
                if !pid.is_empty() && pid.chars().all(|c| c.is_ascii_digit()) {
                    let _ = std::process::Command::new("kill")
                        .args(["-9", pid])
                        .status();
                }
            }
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn nonzero_child_exit_reports_process_exited_with_stderr() {
        let sandbox = ScriptSandbox::new();
        let crash_script = sandbox.write_script(
            "crash.sh",
            "#!/bin/sh\nprintf 'fatal: invalid model provider config\\n' >&2\nexit 42\n",
        );

        let cfg = SpawnConfig::new().codex_path(&crash_script);
        let (server, drive) = AppServer::spawn(&cfg, NoopServerRequestHandler).unwrap();
        let drive_task = tokio::spawn(drive);

        let err = server.handshake(&cfg).await.unwrap_err();
        match err {
            SdkError::ProcessExited { status, stderr } => {
                assert_eq!(status.and_then(|s| s.code()), Some(42));
                assert!(
                    stderr.contains("fatal: invalid model provider config"),
                    "unexpected stderr: {stderr:?}"
                );
            }
            other => panic!("expected ProcessExited, got {other:?}"),
        }

        drive_task.await.unwrap();
        assert!(matches!(
            server.terminal_error(),
            Some(SdkError::ProcessExited { .. })
        ));

        // Direct `/bin/sh` invocation (`sh app-server` exits 127 immediately) where
        // handshake writes after child exit (`BrokenPipe` upgraded to `ProcessExited`).
        let sh_cfg = SpawnConfig::new().codex_path("/bin/sh");
        let (sh_server, sh_drive) = AppServer::spawn(&sh_cfg, NoopServerRequestHandler).unwrap();
        let sh_call = {
            let srv = sh_server.clone();
            let c = sh_cfg.clone();
            tokio::spawn(async move { srv.handshake(&c).await })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        let sh_drive_task = tokio::spawn(sh_drive);

        let sh_err = sh_call.await.unwrap().unwrap_err();
        assert!(
            matches!(sh_err, SdkError::ProcessExited { status: Some(s), .. } if !s.success()),
            "expected ProcessExited from /bin/sh, got {sh_err:?}"
        );
        sh_drive_task.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn close_kills_spawned_child_without_hanging_on_grandchild_fds() {
        let sandbox = ScriptSandbox::new();

        // 1. Direct `exec sleep 60` child closed right after spawn.
        let sleep_script = sandbox.write_script("sleep.sh", "#!/bin/sh\nexec sleep 60\n");
        let sleep_cfg = SpawnConfig::new().codex_path(&sleep_script);
        let (sleep_srv, sleep_drive) =
            AppServer::spawn(&sleep_cfg, NoopServerRequestHandler).unwrap();
        let sleep_drive_task = tokio::spawn(sleep_drive);
        sleep_srv.close();

        tokio::time::timeout(Duration::from_secs(2), sleep_drive_task)
            .await
            .expect("close() on sleep child must finish promptly")
            .unwrap();
        assert!(sleep_srv.terminal_error().is_none());

        // 2. `/bin/sh` script spawning a background grandchild (`sleep 60 &`) that
        // inherits open `stdout` and `stderr` pipes.
        let ready = serde_json::to_string(&json!({
            "method": "account/login/completed",
            "params": {"loginId": "ready-1", "success": true}
        }))
        .unwrap();
        let grandchild_script = sandbox.write_script(
            "grandchild.sh",
            "#!/bin/sh\nsleep 60 &\nprintf '%s\\n' \"$!\" > \"$MOCK_GC_PID\"\nprintf 'holding-stderr\\n' >&2\nprintf '%s\\n' \"$MOCK_READY\"\nwait\n",
        );

        let gc_cfg = SpawnConfig::new()
            .codex_path(&grandchild_script)
            .env_var("MOCK_GC_PID", sandbox.grandchild_pid_file.to_str().unwrap())
            .env_var("MOCK_READY", ready);
        let (gc_srv, gc_drive) = AppServer::spawn(&gc_cfg, NoopServerRequestHandler).unwrap();
        let global_rx = gc_srv.global_events();
        let gc_drive_task = tokio::spawn(gc_drive);

        let first = tokio::time::timeout(Duration::from_secs(5), global_rx.recv())
            .await
            .expect("timed out waiting for ready notification")
            .unwrap();
        assert!(matches!(
            first,
            ThreadEvent::Notification(Notification::AccountLoginCompleted(_))
        ));

        gc_srv.close();

        tokio::time::timeout(Duration::from_secs(2), async {
            assert!(global_rx.recv().await.is_err());
            gc_drive_task.await.unwrap();
        })
        .await
        .expect("close() must resolve promptly even when grandchild holds stdout/stderr open");
        assert!(gc_srv.terminal_error().is_none());
    }

    struct ReentrantCrashHandler {
        server: Arc<OnceLock<AppServer>>,
        observed_err_tx: Sender<SdkError>,
    }

    impl ServerRequestHandler for ReentrantCrashHandler {
        fn command_execution_approval(
            &self,
            _params: CommandExecutionRequestApprovalParams,
        ) -> impl Future<Output = HandlerResult<CommandExecutionRequestApprovalResponse>> + Send
        {
            let server = Arc::clone(&self.server);
            let observed_err_tx = self.observed_err_tx.clone();
            async move {
                let srv = server.get().expect("server populated");
                match srv.model_list(&ModelListParams::default()).await {
                    Ok(_) => Ok(CommandExecutionRequestApprovalResponse::accept()),
                    Err(err) => {
                        let msg = err.to_string();
                        let _ = observed_err_tx.try_send(err);
                        Err(RemoteError::internal_error(msg))
                    }
                }
            }
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn reentrant_handler_and_surviving_grandchild_stderr_do_not_hang_on_crash() {
        let sandbox = ScriptSandbox::new();
        let req_line = serde_json::to_string(&json!({
            "id": 99,
            "method": "item/commandExecution/requestApproval",
            "params": {"threadId": "t-crash", "turnId": "u-crash", "itemId": "i-crash"}
        }))
        .unwrap();

        // Spawn a background grandchild holding ONLY `stderr` open (`>/dev/null`),
        // emit an inbound server request on `stdout`, write diagnostic text to
        // `stderr`, and exit 19 so `stdout` hits EOF while the handler makes a
        // re-entrant `model_list` RPC call and `stderr` still has an open writer!
        let script = sandbox.write_script(
            "reentrant-crash.sh",
            "#!/bin/sh\nsleep 60 >/dev/null &\nprintf '%s\\n' \"$!\" > \"$MOCK_GC_PID\"\nprintf 'reentrant-crash-stderr\\n' >&2\nprintf '%s\\n' \"$MOCK_REQ\"\nexit 19\n",
        );

        let server_slot = Arc::new(OnceLock::new());
        let (err_tx, err_rx) = async_channel::bounded(1);
        let handler = ReentrantCrashHandler {
            server: Arc::clone(&server_slot),
            observed_err_tx: err_tx,
        };

        let cfg = SpawnConfig::new()
            .codex_path(&script)
            .env_var("MOCK_GC_PID", sandbox.grandchild_pid_file.to_str().unwrap())
            .env_var("MOCK_REQ", req_line);

        let (server, drive) = AppServer::spawn(&cfg, handler).unwrap();
        let _ = server_slot.set(server.clone());
        let drive_task = tokio::spawn(drive);

        let handler_err = tokio::time::timeout(Duration::from_secs(2), async {
            drive_task.await.unwrap();
            err_rx
                .recv()
                .await
                .expect("handler must observe exit error")
        })
        .await
        .expect("driver and re-entrant handler must not deadlock or hang on grandchild stderr");

        match handler_err {
            SdkError::ProcessExited { status, stderr } => {
                assert_eq!(status.and_then(|s| s.code()), Some(19));
                assert!(
                    stderr.contains("reentrant-crash-stderr"),
                    "expected captured stderr before grandchild hang, got {stderr:?}"
                );
            }
            other => panic!("expected ProcessExited in re-entrant handler, got {other:?}"),
        }

        match server.terminal_error() {
            Some(SdkError::ProcessExited { status, stderr }) => {
                assert_eq!(status.and_then(|s| s.code()), Some(19));
                assert!(stderr.contains("reentrant-crash-stderr"));
            }
            other => panic!("expected terminal ProcessExited, got {other:?}"),
        }
    }
}
