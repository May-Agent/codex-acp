//! End-to-end integration tests driving `codex_acp::serve` over both ACP v2
//! (`aither_acp::Client`) and ACP v1 (`protocolVersion: 1` JSON-RPC) against a
//! deterministic in-memory mock `codex app-server`.

use std::collections::{BTreeMap, VecDeque};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use codex_acp::aither_acp::client::{
    Client, ElicitationHandler, MessageKind, NotificationHandler, PermissionHandler, SessionEvent,
    TurnOutcome,
};
use codex_acp::aither_acp::domain::{
    AcceptAction, AudioContent, ConfigValue, ConfigValueBoolean, ConfigValueById, ConfigValueId,
    ContentBlock, ElicitAction, ElicitComplete, ElicitContentValue, ElicitCreate, ElicitMode,
    ElicitResult, EmbeddedResource, ExtMethod, HttpHeader, ImageContent, Implementation, McpServer,
    McpServerHttp, McpServerStdio, NoticeSeverity, Open, PermissionOptionId, PermissionOutcome,
    PermissionRequest, ResourceContents, ResourceLink, SelectedOutcome, StopReason,
    TextResourceContents,
};
use codex_acp::aither_acp::ext::steering::METHOD as STEERING_METHOD;
use codex_acp::aither_acp::rpc::{
    self, Handler, Notification as RpcNotification, Peer, Request as RpcRequest, RequestContext,
    Response as RpcResponse,
};
use codex_acp::{CodexAcpConfig, ConnectedAppServer, serve};
use futures_lite::Stream as _;
use futures_lite::StreamExt as _;
use futures_lite::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt};
use serde_json::{Value, json};

// --- In-memory duplex byte pipes for the mock `codex app-server` -------------

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

fn byte_pipe() -> (ReadEnd, WriteEnd, ReadEnd, WriteEnd) {
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

type InboundResponses = Arc<Mutex<Vec<(Value, Result<Value, Value>)>>>;
type RecordedNotifications = Arc<Mutex<Vec<(String, Option<Value>)>>>;

#[derive(Clone)]
struct MockControl {
    outbound_tx: async_channel::Sender<Value>,
    inbound_calls: Arc<Mutex<Vec<(String, Value)>>>,
    inbound_responses: InboundResponses,
    hold_turns: Arc<Mutex<bool>>,
    released_tx: async_channel::Sender<String>,
}

impl MockControl {
    async fn send_raw(&self, frame: Value) {
        self.outbound_tx
            .send(frame)
            .await
            .expect("mock outbound open");
    }

    fn calls_for(&self, method: &str) -> Vec<Value> {
        self.inbound_calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(m, _)| m == method)
            .map(|(_, p)| p.clone())
            .collect()
    }

    fn set_hold_turns(&self, hold: bool) {
        *self.hold_turns.lock().unwrap() = hold;
    }

    async fn release_turn(&self, turn_id: &str) {
        let _ = self.released_tx.send(turn_id.to_string()).await;
    }

    async fn wait_for_response(&self, target_id: &Value) -> Result<Value, Value> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let found = self
                .inbound_responses
                .lock()
                .unwrap()
                .iter()
                .find(|(id, _)| id == target_id)
                .cloned();
            if let Some((_, res)) = found {
                return res;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for response to {target_id}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

fn spawn_mock_app_server() -> (CodexAcpConfig, MockControl) {
    let (bridge_r, bridge_w, mock_r, mock_w) = byte_pipe();
    let bridge_slot = Arc::new(Mutex::new(Some((bridge_r, bridge_w))));
    let (outbound_tx, outbound_rx) = async_channel::unbounded::<Value>();
    let (released_tx, released_rx) = async_channel::unbounded::<String>();
    let inbound_calls = Arc::new(Mutex::new(Vec::new()));
    let inbound_responses = Arc::new(Mutex::new(Vec::new()));
    let hold_turns = Arc::new(Mutex::new(false));

    let ctrl = MockControl {
        outbound_tx: outbound_tx.clone(),
        inbound_calls: Arc::clone(&inbound_calls),
        inbound_responses: Arc::clone(&inbound_responses),
        hold_turns: Arc::clone(&hold_turns),
        released_tx,
    };

    tokio::spawn(run_mock_loop(
        mock_r,
        mock_w,
        outbound_tx,
        outbound_rx,
        released_rx,
        inbound_calls,
        inbound_responses,
        hold_turns,
    ));

    let cfg = CodexAcpConfig::new().connector(move |_spawn, handler| {
        let (r, w) = bridge_slot
            .lock()
            .unwrap()
            .take()
            .expect("single connection per mock");
        std::future::ready(Ok(ConnectedAppServer::attach(r, w, handler)))
    });
    (cfg, ctrl)
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn run_mock_loop(
    mut r: ReadEnd,
    mut w: WriteEnd,
    outbound_tx: async_channel::Sender<Value>,
    outbound_rx: async_channel::Receiver<Value>,
    released_rx: async_channel::Receiver<String>,
    inbound_calls: Arc<Mutex<Vec<(String, Value)>>>,
    inbound_responses: InboundResponses,
    hold_turns: Arc<Mutex<bool>>,
) {
    let writer_task = tokio::spawn(async move {
        while let Ok(val) = outbound_rx.recv().await {
            let mut line = serde_json::to_string(&val).unwrap();
            line.push('\n');
            if w.write_all(line.as_bytes()).await.is_err() {
                break;
            }
        }
    });

    let mut thread_seq = 0_u32;
    let mut turn_seq = 0_u32;
    let mut lines = futures_lite::io::BufReader::new(&mut r).lines();

    while let Some(Ok(line)) = lines.next().await {
        let Ok(v) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        assert!(
            v.get("jsonrpc").is_none(),
            "codex app-server frames must omit `jsonrpc`: {v}"
        );

        let method = v.get("method").and_then(Value::as_str);
        let id = v.get("id").cloned();

        if method.is_none()
            && let Some(resp_id) = id
        {
            let outcome = v.get("error").map_or_else(
                || Ok(v.get("result").cloned().unwrap_or(Value::Null)),
                |err| Err(err.clone()),
            );
            inbound_responses.lock().unwrap().push((resp_id, outcome));
            continue;
        }

        let method = method.unwrap_or_default().to_string();
        let params = v.get("params").cloned().unwrap_or(Value::Null);
        inbound_calls
            .lock()
            .unwrap()
            .push((method.clone(), params.clone()));

        let Some(req_id) = id else {
            continue;
        };

        match method.as_str() {
            "initialize" => {
                let _ = outbound_tx
                    .send(json!({"id": req_id, "result": {"userAgent": "mock-codex/0.1"}}))
                    .await;
            }
            "model/list" => {
                let _ = outbound_tx
                    .send(json!({
                        "id": req_id,
                        "result": {
                            "data": [
                                {"id": "gpt-5.4", "displayName": "GPT-5.4", "description": "Default flagship"},
                                {"id": "gpt-6-luna", "displayName": "GPT-6 Luna", "description": "Fast test model"}
                            ],
                            "nextCursor": null
                        }
                    }))
                    .await;
            }
            "account/login/start" => {
                let kind = params
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or("apiKey");
                if kind == "apiKey" {
                    let _ = outbound_tx
                        .send(json!({"id": req_id, "result": {"type": "apiKey"}}))
                        .await;
                } else {
                    let _ = outbound_tx
                        .send(json!({
                            "id": req_id,
                            "result": {
                                "type": kind,
                                "loginId": "login-42",
                                "authUrl": "https://auth.openai.com/device"
                            }
                        }))
                        .await;
                    let tx = outbound_tx.clone();
                    tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_millis(15)).await;
                        let _ = tx
                            .send(json!({
                                "method": "account/login/completed",
                                "params": {"loginId": "login-42", "success": true, "error": null}
                            }))
                            .await;
                    });
                }
            }
            "account/read" => {
                let _ = outbound_tx
                    .send(json!({
                        "id": req_id,
                        "result": {
                            "account": {
                                "type": "chatgpt",
                                "email": "lexo@example.com",
                                "planType": "pro"
                            },
                            "requiresOpenaiAuth": false
                        }
                    }))
                    .await;
            }
            "account/logout"
            | "thread/unsubscribe"
            | "thread/delete"
            | "thread/compact/start"
            | "modelProvider/default/set" => {
                let _ = outbound_tx.send(json!({"id": req_id, "result": {}})).await;
            }
            "modelProvider/config/write" => {
                let _ = outbound_tx
                    .send(json!({"id": req_id, "result": {"provider": {"id": "gateway"}}}))
                    .await;
            }
            "thread/start" => {
                thread_seq += 1;
                let tid = format!("thread-{thread_seq}");
                let cwd = params.get("cwd").and_then(Value::as_str).unwrap_or("/tmp");
                let _ = outbound_tx
                    .send(json!({
                        "id": req_id,
                        "result": {
                            "model": "gpt-5.4",
                            "reasoningEffort": "medium",
                            "thread": {"id": tid, "cwd": cwd, "model": "gpt-5.4"}
                        }
                    }))
                    .await;
            }
            "thread/resume" => {
                let tid = params
                    .get("threadId")
                    .and_then(Value::as_str)
                    .unwrap_or("thread-1");
                let cwd = params.get("cwd").and_then(Value::as_str).unwrap_or("/tmp");
                let _ = outbound_tx
                    .send(json!({
                        "id": req_id,
                        "result": {
                            "model": "gpt-5.4",
                            "reasoningEffort": "high",
                            "thread": {"id": tid, "cwd": cwd, "model": "gpt-5.4"}
                        }
                    }))
                    .await;
            }
            "thread/fork" => {
                thread_seq += 1;
                let forked = format!("thread-fork-{thread_seq}");
                let cwd = params.get("cwd").and_then(Value::as_str).unwrap_or("/tmp");
                let _ = outbound_tx
                    .send(json!({
                        "id": req_id,
                        "result": {
                            "model": "gpt-5.4",
                            "reasoningEffort": "medium",
                            "thread": {"id": forked, "cwd": cwd, "model": "gpt-5.4"}
                        }
                    }))
                    .await;
            }
            "thread/read" => {
                let tid = params
                    .get("threadId")
                    .and_then(Value::as_str)
                    .unwrap_or("thread-1");
                let _ = outbound_tx
                    .send(json!({
                        "id": req_id,
                        "result": {
                            "thread": {
                                "id": tid,
                                "turns": [{
                                    "id": "hist-turn-1",
                                    "status": "completed",
                                    "items": [
                                        {"type": "userMessage", "id": "u1", "content": [{"type": "text", "text": "prior question"}]},
                                        {"type": "reasoning", "id": "r1", "summary": ["thinking step"]},
                                        {"type": "commandExecution", "id": "c1", "command": "pwd", "cwd": "/tmp", "status": "completed", "aggregatedOutput": "/tmp\n"},
                                        {"type": "agentMessage", "id": "a1", "text": "prior answer"}
                                    ]
                                }]
                            }
                        }
                    }))
                    .await;
            }
            "thread/list" => {
                let _ = outbound_tx
                    .send(json!({
                        "id": req_id,
                        "result": {
                            "data": [{
                                "id": "thread-listed-1",
                                "cliVersion": "0.1.0",
                                "createdAt": 1_700_000_000_i64,
                                "cwd": "/workspace/demo",
                                "ephemeral": false,
                                "modelProvider": "openai",
                                "name": "Demo Session",
                                "path": null,
                                "preview": "hello",
                                "source": "exec",
                                "status": {"type": "idle"},
                                "turns": [],
                                "updatedAt": 1_700_000_000_i64
                            }],
                            "nextCursor": "cursor-2"
                        }
                    }))
                    .await;
            }
            "review/start" => {
                turn_seq += 1;
                let tid = params
                    .get("threadId")
                    .and_then(Value::as_str)
                    .unwrap_or("thread-1")
                    .to_string();
                let turn_id = format!("review-turn-{turn_seq}");
                let _ = outbound_tx
                    .send(json!({
                        "id": req_id,
                        "result": {"turn": {"id": turn_id, "status": "inProgress", "items": []}}
                    }))
                    .await;
                let tx = outbound_tx.clone();
                tokio::spawn(async move {
                    let _ = tx
                        .send(json!({
                            "method": "item/agentMessage/delta",
                            "params": {"threadId": tid, "turnId": turn_id, "itemId": "rv1", "delta": "Review LGTM"}
                        }))
                        .await;
                    let _ = tx
                        .send(json!({
                            "method": "turn/completed",
                            "params": {"threadId": tid, "turnId": turn_id, "turn": {"id": turn_id, "status": "completed", "items": []}}
                        }))
                        .await;
                });
            }
            "turn/steer" => {
                let tid = params
                    .get("expectedTurnId")
                    .and_then(Value::as_str)
                    .unwrap_or("turn-1");
                let _ = outbound_tx
                    .send(json!({"id": req_id, "result": {"turnId": tid}}))
                    .await;
            }
            "turn/interrupt" => {
                let tid = params
                    .get("threadId")
                    .and_then(Value::as_str)
                    .unwrap_or("thread-1")
                    .to_string();
                let turn_id = params
                    .get("turnId")
                    .and_then(Value::as_str)
                    .unwrap_or("turn-1")
                    .to_string();
                let _ = outbound_tx.send(json!({"id": req_id, "result": {}})).await;
                let _ = outbound_tx
                    .send(json!({
                        "method": "turn/completed",
                        "params": {"threadId": tid, "turnId": turn_id, "turn": {"id": turn_id, "status": "interrupted", "items": []}}
                    }))
                    .await;
            }
            "turn/start" => {
                turn_seq += 1;
                let tid = params
                    .get("threadId")
                    .and_then(Value::as_str)
                    .unwrap_or("thread-1")
                    .to_string();
                let turn_id = format!("turn-{turn_seq}");
                let should_hold = *hold_turns.lock().unwrap();
                let _ = outbound_tx
                    .send(json!({
                        "id": req_id,
                        "result": {"turn": {"id": turn_id, "status": "inProgress", "items": []}}
                    }))
                    .await;

                if should_hold {
                    let tx = outbound_tx.clone();
                    let rel_rx = released_rx.clone();
                    tokio::spawn(async move {
                        while let Ok(target) = rel_rx.recv().await {
                            if target == turn_id {
                                let _ = tx
                                    .send(json!({
                                        "method": "item/agentMessage/delta",
                                        "params": {"threadId": tid, "turnId": turn_id, "itemId": "m-held", "delta": "released response"}
                                    }))
                                    .await;
                                let _ = tx
                                    .send(json!({
                                        "method": "turn/completed",
                                        "params": {"threadId": tid, "turnId": turn_id, "turn": {"id": turn_id, "status": "completed", "items": []}}
                                    }))
                                    .await;
                                break;
                            }
                        }
                    });
                } else {
                    let tx = outbound_tx.clone();
                    tokio::spawn(async move {
                        // Foreign turn delta + foreign completion that MUST be ignored!
                        let _ = tx
                            .send(json!({
                                "method": "item/agentMessage/delta",
                                "params": {"threadId": tid, "turnId": "foreign-turn-999", "itemId": "bad", "delta": "WRONG_TURN_TEXT"}
                            }))
                            .await;
                        let _ = tx
                            .send(json!({
                                "method": "turn/completed",
                                "params": {"threadId": tid, "turnId": "foreign-turn-999", "turn": {"id": "foreign-turn-999", "status": "completed", "items": []}}
                            }))
                            .await;

                        // Mid-turn command approval #1 (client chooses AllowAlways)
                        let _ = tx
                            .send(json!({
                                "id": "srv-approve-1",
                                "method": "item/commandExecution/requestApproval",
                                "params": {
                                    "threadId": tid,
                                    "turnId": turn_id,
                                    "itemId": "cmd-1",
                                    "command": "cargo test",
                                    "cwd": "/workspace"
                                }
                            }))
                            .await;
                        tokio::time::sleep(Duration::from_millis(25)).await;

                        // Mid-turn legacy execCommandApproval for same command -> auto-approved from PermissionMemory!
                        let _ = tx
                            .send(json!({
                                "id": "srv-approve-2",
                                "method": "execCommandApproval",
                                "params": {
                                    "conversationId": tid,
                                    "callId": "cmd-2",
                                    "command": ["cargo", "test"],
                                    "cwd": "/workspace",
                                    "parsedCmd": []
                                }
                            }))
                            .await;
                        tokio::time::sleep(Duration::from_millis(25)).await;

                        // Rich streaming events: reasoning, plan, tool start/output/patch/complete, mcp tool, usage, warning, message
                        let _ = tx
                            .send(json!({
                                "method": "item/reasoning/summaryTextDelta",
                                "params": {"threadId": tid, "turnId": turn_id, "itemId": "r-1", "delta": "analyzing "}
                            }))
                            .await;
                        let _ = tx
                            .send(json!({
                                "method": "turn/plan/updated",
                                "params": {
                                    "threadId": tid,
                                    "turnId": turn_id,
                                    "plan": [{"step": "Inspect files", "status": "inProgress"}]
                                }
                            }))
                            .await;
                        let _ = tx
                            .send(json!({
                                "method": "item/started",
                                "params": {
                                    "threadId": tid,
                                    "turnId": turn_id,
                                    "item": {"type": "commandExecution", "id": "cmd-1", "command": "cargo test", "cwd": "/workspace", "status": "inProgress"}
                                }
                            }))
                            .await;
                        let _ = tx
                            .send(json!({
                                "method": "item/commandExecution/outputDelta",
                                "params": {"threadId": tid, "turnId": turn_id, "itemId": "cmd-1", "delta": "test result: ok\n"}
                            }))
                            .await;
                        let _ = tx
                            .send(json!({
                                "method": "item/completed",
                                "params": {
                                    "threadId": tid,
                                    "turnId": turn_id,
                                    "item": {"type": "commandExecution", "id": "cmd-1", "command": "cargo test", "cwd": "/workspace", "status": "completed", "aggregatedOutput": "test result: ok\n"}
                                }
                            }))
                            .await;
                        let _ = tx
                            .send(json!({
                                "method": "item/started",
                                "params": {
                                    "threadId": tid,
                                    "turnId": turn_id,
                                    "item": {"type": "mcpToolCall", "id": "mcp-1", "server": "docs", "tool": "lookup", "status": "inProgress"}
                                }
                            }))
                            .await;
                        let _ = tx
                            .send(json!({
                                "method": "item/fileChange/patchUpdated",
                                "params": {
                                    "threadId": tid,
                                    "turnId": turn_id,
                                    "itemId": "edit-1",
                                    "changes": [{"path": "/workspace/src/lib.rs", "kind": "modify", "diff": "@@ -1 +1 @@\n-old\n+new\n"}]
                                }
                            }))
                            .await;
                        let _ = tx
                            .send(json!({
                                "method": "thread/tokenUsage/updated",
                                "params": {
                                    "threadId": tid,
                                    "turnId": turn_id,
                                    "tokenUsage": {
                                        "total": {"totalTokens": 512},
                                        "modelContextWindow": 200_000
                                    }
                                }
                            }))
                            .await;
                        let _ = tx
                            .send(json!({
                                "method": "warning",
                                "params": {"threadId": tid, "turnId": turn_id, "message": "Rate limit approaching"}
                            }))
                            .await;
                        let _ = tx
                            .send(json!({
                                "method": "item/agentMessage/delta",
                                "params": {"threadId": tid, "turnId": turn_id, "itemId": "msg-1", "delta": "All checks "}
                            }))
                            .await;
                        let _ = tx
                            .send(json!({
                                "method": "item/agentMessage/delta",
                                "params": {"threadId": tid, "turnId": turn_id, "itemId": "msg-1", "delta": "passed!"}
                            }))
                            .await;
                        let _ = tx
                            .send(json!({
                                "method": "turn/completed",
                                "params": {"threadId": tid, "turnId": turn_id, "turn": {"id": turn_id, "status": "completed", "items": []}}
                            }))
                            .await;
                    });
                }
            }
            other => {
                let _ = outbound_tx
                    .send(json!({
                        "id": req_id,
                        "error": {"code": -32601, "message": format!("method not found: {other}")}
                    }))
                    .await;
            }
        }
    }

    writer_task.abort();
}

// --- Client-side test handlers -----------------------------------------------

#[derive(Clone)]
struct RecordingPermissions {
    prompts: Arc<Mutex<Vec<PermissionRequest>>>,
    selected_option: Arc<Mutex<String>>,
}

impl RecordingPermissions {
    fn new(default_option: &str) -> Self {
        Self {
            prompts: Arc::new(Mutex::new(Vec::new())),
            selected_option: Arc::new(Mutex::new(default_option.to_string())),
        }
    }

    fn set_option(&self, opt: &str) {
        *self.selected_option.lock().unwrap() = opt.to_string();
    }

    fn count(&self) -> usize {
        self.prompts.lock().unwrap().len()
    }
}

impl PermissionHandler for RecordingPermissions {
    fn request(
        &self,
        request: PermissionRequest,
    ) -> impl std::future::Future<Output = PermissionOutcome> + Send {
        self.prompts.lock().unwrap().push(request);
        let chosen = self.selected_option.lock().unwrap().clone();
        std::future::ready(PermissionOutcome::Selected(SelectedOutcome {
            option_id: PermissionOptionId::from_string(chosen),
            meta: None,
        }))
    }
}

#[derive(Clone, Default)]
struct RecordingElicitations {
    requests: Arc<Mutex<Vec<ElicitCreate>>>,
}

impl ElicitationHandler for RecordingElicitations {
    fn create(
        &self,
        request: ElicitCreate,
    ) -> impl std::future::Future<Output = ElicitResult> + Send {
        self.requests.lock().unwrap().push(request.clone());
        let content = match &request.mode {
            Open::Known(ElicitMode::Form(_)) => Some(BTreeMap::from([(
                "q1".to_string(),
                ElicitContentValue::String("chosen-answer".to_string()),
            )])),
            _ => None,
        };
        std::future::ready(ElicitResult {
            action: Open::Known(ElicitAction::Accept(AcceptAction {
                content,
                meta: None,
            })),
            meta: None,
            extra: BTreeMap::new(),
        })
    }

    fn complete(&self, _complete: ElicitComplete) -> impl std::future::Future<Output = ()> + Send {
        std::future::ready(())
    }
}

#[derive(Clone, Default)]
struct RecordingNotifications {
    items: RecordedNotifications,
}

impl NotificationHandler for RecordingNotifications {
    fn notify(&self, note: RpcNotification) -> impl std::future::Future<Output = ()> + Send {
        self.items.lock().unwrap().push((note.method, note.params));
        std::future::ready(())
    }
}

fn assert_end_turn(outcome: &TurnOutcome) {
    assert!(
        matches!(
            outcome,
            TurnOutcome::Finished {
                stop_reason: Some(Open::Known(StopReason::EndTurn)),
                ..
            }
        ),
        "expected EndTurn, got {outcome:?}"
    );
}

fn assert_cancelled(outcome: &TurnOutcome) {
    assert!(
        matches!(
            outcome,
            TurnOutcome::Cancelled
                | TurnOutcome::Finished {
                    stop_reason: Some(Open::Known(StopReason::Cancelled)),
                    ..
                }
        ),
        "expected Cancelled, got {outcome:?}"
    );
}

// --- Test 1: ACP v2 handshake, auth flows, session lifecycle, replay, streaming, permissions

#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn v2_handshake_auth_lifecycle_streaming_and_permission_memory() {
    let (mut cfg, ctrl) = spawn_mock_app_server();
    cfg.spawn
        .env
        .insert("CODEX_API_KEY".into(), "sk-test-123".into());

    let (client_t, server_t) = rpc::Memory::pair();
    let (_server_peer, server_fut) = serve(server_t, cfg);
    let server_task = tokio::spawn(server_fut);

    let perms = RecordingPermissions::new("allow_always");
    let elicits = RecordingElicitations::default();
    let notes = RecordingNotifications::default();

    let (client, conn) = Client::builder(Implementation::new("test-client", "1.0"))
        .permissions(perms.clone())
        .elicitation(elicits.clone())
        .notification(notes.clone())
        .connect(client_t)
        .await
        .expect("v2 connect");
    let conn_task = tokio::spawn(conn);

    assert_eq!(client.protocol_version(), 2);
    let method_ids: Vec<&str> = client
        .auth_methods()
        .iter()
        .map(|m| m.method_id.as_str())
        .collect();
    assert!(method_ids.contains(&"chat-gpt"));
    assert!(method_ids.contains(&"chat-gpt-device-code"));
    assert!(method_ids.contains(&"api-key"));
    assert!(method_ids.contains(&"gateway"));
    assert!(session_cap_check(&client, "list"));
    assert!(session_cap_check(&client, "resume"));
    assert!(session_cap_check(&client, "close"));
    assert!(session_cap_check(&client, "loadSession"));

    // Authenticate with API key -> ChatGPT device code -> Logout
    client.authenticate("api-key").await.expect("api-key login");
    client
        .authenticate("chat-gpt-device-code")
        .await
        .expect("device-code login");
    client.logout(None).await.expect("logout");

    // Verify `_auth/status_update` notifications were delivered
    tokio::time::sleep(Duration::from_millis(20)).await;
    let auth_notes: Vec<Value> = notes
        .items
        .lock()
        .unwrap()
        .iter()
        .filter(|(m, _)| m == "_auth/status_update")
        .filter_map(|(_, p)| p.clone())
        .collect();
    assert!(
        auth_notes.len() >= 3,
        "expected >=3 _auth/status_update notifications, got {auth_notes:?}"
    );
    assert_eq!(auth_notes[0]["authStatus"]["kind"], "api_key");
    assert_eq!(auth_notes[1]["authStatus"]["kind"], "account");
    assert_eq!(
        auth_notes[1]["authStatus"]["account"]["email"],
        "lexo@example.com"
    );
    assert_eq!(auth_notes[2]["authStatus"]["kind"], "none");

    // Create session with stdio + http MCP servers
    let session = client
        .new_session("/workspace")
        .mcp_server(McpServer::Stdio(McpServerStdio {
            name: "local-fs".into(),
            command: "mcp-fs".into(),
            args: vec!["--ro".into()],
            env: Vec::new(),
            meta: None,
            extra: BTreeMap::new(),
        }))
        .mcp_server(McpServer::Http(McpServerHttp {
            name: "remote-docs".into(),
            url: "https://docs.example.com/mcp".into(),
            headers: vec![HttpHeader {
                name: "Authorization".into(),
                value: "Bearer tok".into(),
            }],
            meta: None,
            extra: BTreeMap::new(),
        }))
        .open()
        .await
        .expect("new_session");

    let start_calls = ctrl.calls_for("thread/start");
    assert_eq!(start_calls.len(), 1);
    assert_eq!(
        start_calls[0]["config"]["mcp_servers"]["local-fs"]["command"],
        "mcp-fs"
    );
    assert_eq!(
        start_calls[0]["config"]["mcp_servers"]["remote-docs"]["url"],
        "https://docs.example.com/mcp"
    );

    // Stale cancel while idle must succeed cleanly without breaking the next turn
    session.cancel().await.expect("stale cancel");

    // Send multimodal prompt (Text + inline Image + file:// Image + Audio + Resource + ResourceLink)
    let blocks = vec![
        ContentBlock::text("Run checks"),
        ContentBlock::Image(ImageContent {
            data: Some("aGVsbG8=".into()),
            mime_type: "image/png".into(),
            uri: None,
            annotations: None,
            meta: None,
            extra: BTreeMap::new(),
        }),
        ContentBlock::Image(ImageContent {
            data: None,
            mime_type: "image/png".into(),
            uri: Some("file:///workspace/diagram.png".into()),
            annotations: None,
            meta: None,
            extra: BTreeMap::new(),
        }),
        ContentBlock::Audio(AudioContent {
            data: "YXVkaW8=".into(),
            mime_type: "audio/wav".into(),
            annotations: None,
            meta: None,
            extra: BTreeMap::new(),
        }),
        ContentBlock::Resource(EmbeddedResource {
            resource: ResourceContents::Text(TextResourceContents {
                uri: "file:///workspace/README.md".into(),
                text: "# Readme".into(),
                mime_type: Some("text/markdown".into()),
                meta: None,
            }),
            annotations: None,
            meta: None,
            extra: BTreeMap::new(),
        }),
        ContentBlock::ResourceLink(ResourceLink {
            uri: "file:///workspace/Cargo.toml".into(),
            name: "Cargo.toml".into(),
            title: None,
            description: None,
            mime_type: None,
            size: None,
            icons: Vec::new(),
            annotations: None,
            meta: None,
            extra: BTreeMap::new(),
        }),
    ];

    let mut turn = session.prompt(blocks).expect("prompt");
    let mut events = Vec::new();
    while let Some(ev) = turn.next().await {
        events.push(ev);
    }
    let outcome = turn.finish().await;
    assert_end_turn(&outcome);

    // Verify foreign turn text was filtered out and real streamed text + thought + plan + usage + notice landed
    let state = session.state();
    let agent_texts: String = state
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
    assert_eq!(agent_texts, "All checks passed!");
    assert!(!agent_texts.contains("WRONG_TURN_TEXT"));
    assert!(
        state.usage.as_ref().is_some_and(|u| u.used == 512),
        "usage missing: {:?}",
        state.usage
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, SessionEvent::Notice(n) if n.title == "Rate limit approaching" && n.severity == Open::Known(NoticeSeverity::Warning))),
        "expected warning notice in {events:?}"
    );
    assert!(
        state
            .tool_calls
            .values()
            .any(|tc| tc.tool_call_id.as_str() == "mcp-1"
                && codex_acp::aither_acp::vendor::codex::is_mcp_tool_call(tc.meta.as_ref())),
        "expected mcp-1 tool call with is_mcp_tool_call: {:?}",
        state.tool_calls
    );

    // PermissionMemory: 2 approvals for `cargo test`, only 1 interactive prompt!
    assert_eq!(
        perms.count(),
        1,
        "second `cargo test` approval must be satisfied from PermissionMemory"
    );
    assert_eq!(
        ctrl.wait_for_response(&json!("srv-approve-1"))
            .await
            .unwrap()["decision"],
        "acceptForSession"
    );
    assert_eq!(
        ctrl.wait_for_response(&json!("srv-approve-2"))
            .await
            .unwrap()["decision"],
        "approved_for_session"
    );

    // Session list, resume (with replay), fork (with replay), close, delete
    let page = client.sessions(None).await.expect("list_sessions");
    assert_eq!(page.sessions.len(), 1);
    assert_eq!(
        page.sessions[0].updated_at.as_deref(),
        Some("2023-11-14T22:13:20Z")
    );
    assert_eq!(page.next_cursor.as_deref(), Some("cursor-2"));

    let forked = client
        .fork_session(session.id(), "/workspace")
        .await
        .expect("fork_session");
    assert_ne!(forked.id(), session.id());
    assert!(
        forked
            .state()
            .tool_calls
            .values()
            .any(|tc| tc.tool_call_id.as_str() == "c1"),
        "fork should replay historical tool calls"
    );

    let original_id = session.id().clone();
    session.close().await.expect("close session");

    let resumed = client
        .resume_session(original_id, "/workspace")
        .open()
        .await
        .expect("resume_session");
    assert!(
        resumed.state().messages.values().any(|m| {
            m.content
                .as_ref()
                .into_option()
                .is_some_and(|blocks| blocks.iter().any(|b| b.as_text() == Some("prior answer")))
        }),
        "resume should replay thread history: {:?}",
        resumed.state().messages
    );

    resumed.close().await.expect("close resumed session");
    client
        .delete_session(forked.id())
        .await
        .expect("delete session");
    assert_ne!(ctrl.calls_for("thread/unsubscribe"), [] as [Value; 0]);
    assert_ne!(ctrl.calls_for("thread/delete"), [] as [Value; 0]);

    client.close().await;
    let _ = tokio::time::timeout(Duration::from_secs(2), conn_task).await;
    let _ = tokio::time::timeout(Duration::from_secs(2), server_task).await;
}

fn session_cap_check(client: &Client, key: &str) -> bool {
    client
        .agent_capabilities()
        .session
        .as_ref()
        .and_then(|s| s.extra.get(key))
        .is_some_and(|v| v.is_object() || v.as_bool().unwrap_or(false))
}

// --- Test 2: Out-of-prompt approvals, permissions, user-input, elicitations, dynamic tool call, strict review

#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn out_of_prompt_approvals_elicitations_user_input_and_strict_review() {
    let (cfg, ctrl) = spawn_mock_app_server();
    let (client_t, server_t) = rpc::Memory::pair();
    let (_server_peer, server_fut) = serve(server_t, cfg);
    let server_task = tokio::spawn(server_fut);

    let perms = RecordingPermissions::new("allow_once");
    let elicits = RecordingElicitations::default();

    let (client, conn) = Client::builder(Implementation::new("oop-client", "1.0"))
        .permissions(perms.clone())
        .elicitation(elicits.clone())
        .connect(client_t)
        .await
        .expect("connect");
    let conn_task = tokio::spawn(conn);

    let session = client
        .new_session("/workspace")
        .open()
        .await
        .expect("new_session");

    // 1. Out-of-prompt `fileChange/requestApproval` while session is completely idle!
    ctrl.send_raw(json!({
        "id": "oop-file-1",
        "method": "item/fileChange/requestApproval",
        "params": {
            "threadId": "thread-1",
            "turnId": "bg-turn",
            "itemId": "fc-1",
            "reason": "Update config.toml",
            "grantRoot": "/workspace"
        }
    }))
    .await;
    assert_eq!(
        ctrl.wait_for_response(&json!("oop-file-1")).await.unwrap()["decision"],
        "accept"
    );

    // 2. Out-of-prompt `applyPatchApproval`
    ctrl.send_raw(json!({
        "id": "oop-patch-1",
        "method": "applyPatchApproval",
        "params": {
            "conversationId": "thread-1",
            "callId": "patch-1",
            "fileChanges": {
                "/workspace/a.txt": {"type": "add", "content": "hello\n"}
            },
            "reason": "Add a.txt"
        }
    }))
    .await;
    assert_eq!(
        ctrl.wait_for_response(&json!("oop-patch-1")).await.unwrap()["decision"],
        "approved"
    );

    // 3. Out-of-prompt `item/permissions/requestApproval` -> grants requested network/fs profile!
    ctrl.send_raw(json!({
        "id": "oop-perm-1",
        "method": "item/permissions/requestApproval",
        "params": {
            "threadId": "thread-1",
            "turnId": "bg-turn",
            "itemId": "perm-1",
            "cwd": "/workspace",
            "reason": "Allow network access",
            "permissions": {
                "network": {"enabled": true}
            }
        }
    }))
    .await;
    let perm_resp = ctrl.wait_for_response(&json!("oop-perm-1")).await.unwrap();
    assert_eq!(perm_resp["permissions"]["network"]["enabled"], true);

    // 4. Out-of-prompt `item/tool/requestUserInput` -> bridged through form elicitation!
    ctrl.send_raw(json!({
        "id": "oop-input-1",
        "method": "item/tool/requestUserInput",
        "params": {
            "threadId": "thread-1",
            "turnId": "bg-turn",
            "itemId": "ask-1",
            "questions": [{
                "id": "q1",
                "header": "Target",
                "question": "Which target?",
                "options": [{"label": "chosen-answer", "description": "Default target"}]
            }]
        }
    }))
    .await;
    let input_resp = ctrl.wait_for_response(&json!("oop-input-1")).await.unwrap();
    assert_eq!(
        input_resp["answers"]["q1"]["answers"],
        json!(["chosen-answer"])
    );

    // 5. Out-of-prompt MCP form + URL elicitations
    ctrl.send_raw(json!({
        "id": "oop-elicit-form",
        "method": "mcpServer/elicitation/request",
        "params": {
            "_meta": {"threadId": "thread-1"},
            "mode": "form",
            "message": "Provide API parameter",
            "requestedSchema": {
                "type": "object",
                "properties": {"q1": {"type": "string"}},
                "required": ["q1"]
            }
        }
    }))
    .await;
    let form_resp = ctrl
        .wait_for_response(&json!("oop-elicit-form"))
        .await
        .unwrap();
    assert_eq!(form_resp["action"], "accept");
    assert_eq!(form_resp["content"]["q1"], "chosen-answer");

    ctrl.send_raw(json!({
        "id": "oop-elicit-url",
        "method": "mcpServer/elicitation/request",
        "params": {
            "_meta": {"sessionId": session.id().as_str()},
            "mode": "url",
            "elicitationId": "elicit-url-1",
            "message": "Complete OAuth sign-in",
            "url": "https://example.com/oauth"
        }
    }))
    .await;
    let url_resp = ctrl
        .wait_for_response(&json!("oop-elicit-url"))
        .await
        .unwrap();
    assert_eq!(url_resp["action"], "accept");

    // 6. Out-of-prompt `item/tool/call` returns clean structured failure rather than hanging
    ctrl.send_raw(json!({
        "id": "oop-dyn-1",
        "method": "item/tool/call",
        "params": {
            "threadId": "thread-1",
            "turnId": "bg-turn",
            "callId": "dyn-1",
            "tool": "custom_browser",
            "arguments": {}
        }
    }))
    .await;
    let dyn_resp = ctrl.wait_for_response(&json!("oop-dyn-1")).await.unwrap();
    assert_eq!(dyn_resp["success"], false);

    // 7. Unknown server-initiated method returns -32601 cleanly
    ctrl.send_raw(json!({
        "id": "oop-unknown-1",
        "method": "vendor/nonexistentMethod",
        "params": {}
    }))
    .await;
    let err = ctrl
        .wait_for_response(&json!("oop-unknown-1"))
        .await
        .expect_err("must reject unknown server method");
    assert_eq!(err["code"], -32601);

    // 8. `autoApprovalReview/strictReviewRequired` in `agent` mode prompts permission and interrupts on reject!
    ctrl.set_hold_turns(true);
    perms.set_option("reject_once");
    let mut turn_fut = session.prompt("trigger strict review").expect("prompt");
    let strict_task = tokio::spawn(async move { turn_fut.finish().await });
    tokio::time::sleep(Duration::from_millis(30)).await;
    ctrl.send_raw(json!({
        "method": "autoApprovalReview/strictReviewRequired",
        "params": {
            "threadId": "thread-1",
            "turnId": "turn-1",
            "reviewRequestId": "rev-1"
        }
    }))
    .await;
    let outcome = tokio::time::timeout(Duration::from_secs(5), strict_task)
        .await
        .expect("strict review rejection should interrupt turn")
        .unwrap();
    assert_cancelled(&outcome);

    client.close().await;
    let _ = tokio::time::timeout(Duration::from_secs(2), conn_task).await;
    let _ = tokio::time::timeout(Duration::from_secs(2), server_task).await;
}

// --- Test 3: Modes, config options, fast-mode, steering (v2 + ext + legacy), slash commands, cancel & shutdown

#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn modes_configs_steering_slash_commands_cancel_and_disconnect_shutdown() {
    let (cfg, ctrl) = spawn_mock_app_server();
    let (client_t, server_t) = rpc::Memory::pair();
    let (_server_peer, server_fut) = serve(server_t, cfg);
    let server_task = tokio::spawn(server_fut);

    let notes = RecordingNotifications::default();
    let (client, conn) = Client::builder(Implementation::new("cfg-client", "1.0"))
        .notification(notes.clone())
        .connect(client_t)
        .await
        .expect("connect");
    let conn_task = tokio::spawn(conn);

    let session = client
        .new_session("/workspace")
        .open()
        .await
        .expect("new_session");

    // Idle `_session/steering` returns `outcome: "failed"`
    let steer_method = STEERING_METHOD;
    let idle_steer = client
        .ext_request(
            &steer_method,
            &json!({"sessionId": session.id().as_str(), "message": "too early"}),
        )
        .await
        .expect("idle steering");
    assert_eq!(idle_steer["outcome"], "failed");

    // Configure mode, model, reasoning_effort, and boolean fast-mode
    session.set_mode("workspace-write").await.expect("set_mode");
    session
        .set_config(
            "reasoning_effort",
            ConfigValue::Id(ConfigValueById {
                value: ConfigValueId::from_string("xhigh"),
            }),
        )
        .await
        .expect("set effort");
    session
        .set_config(
            "fast-mode",
            ConfigValue::Boolean(ConfigValueBoolean { value: true }),
        )
        .await
        .expect("set fast-mode");
    session.set_model("gpt-6-luna").await.expect("set_model");

    // Slash commands: /status, /compact, /review, /review-branch, /review-commit, /unknown, /logout
    let status_out = session.prompt_run("/status").await.expect("/status");
    assert_end_turn(&status_out);
    let status_text: String = session
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
        status_text.contains("Mode: workspace-write"),
        "{status_text}"
    );
    assert!(status_text.contains("Model: gpt-6-luna"), "{status_text}");
    assert!(
        status_text.contains("Reasoning effort: xhigh"),
        "{status_text}"
    );
    assert!(status_text.contains("Fast mode: true"), "{status_text}");

    assert_end_turn(&session.prompt_run("/compact").await.unwrap());
    assert_eq!(ctrl.calls_for("thread/compact/start").len(), 1);

    assert_end_turn(&session.prompt_run("/review").await.unwrap());
    assert_end_turn(&session.prompt_run("/review-branch main").await.unwrap());
    assert_end_turn(&session.prompt_run("/review-commit deadbeef").await.unwrap());
    let review_calls = ctrl.calls_for("review/start");
    assert_eq!(review_calls.len(), 3);
    assert_eq!(review_calls[0]["target"]["type"], "uncommittedChanges");
    assert_eq!(review_calls[1]["target"]["branch"], "main");
    assert_eq!(review_calls[2]["target"]["sha"], "deadbeef");

    assert_end_turn(&session.prompt_run("/does-not-exist").await.unwrap());
    assert_end_turn(&session.prompt_run("/logout").await.unwrap());
    assert_ne!(ctrl.calls_for("account/logout"), [] as [Value; 0]);

    // Start a held turn to verify turn/start parameters AND all three steering lanes!
    ctrl.set_hold_turns(true);
    let mut held_turn = session.prompt("long task").expect("start held turn");
    let held_task = tokio::spawn(async move { held_turn.finish().await });
    tokio::time::sleep(Duration::from_millis(30)).await;

    let turn_starts = ctrl.calls_for("turn/start");
    assert_eq!(turn_starts.len(), 1);
    assert_eq!(turn_starts[0]["model"], "gpt-6-luna");
    assert_eq!(turn_starts[0]["effort"], "xhigh");
    assert_eq!(turn_starts[0]["serviceTierForTurn"], "fast");
    assert_eq!(turn_starts[0]["approvalsReviewer"], "user");

    // 1. Standard `_session/steering`
    let active_steer = client
        .ext_request(
            &steer_method,
            &json!({"sessionId": session.id().as_str(), "message": "steer via ext"}),
        )
        .await
        .expect("active ext steer");
    assert_eq!(active_steer["outcome"], "injected");

    // 2. Legacy `_codex/session/steering`
    let legacy_steer_method = ExtMethod::new("_codex/session/steering");
    let legacy_steer = client
        .ext_request(
            &legacy_steer_method,
            &json!({"sessionId": session.id().as_str(), "prompt": [{"type": "text", "text": "steer via legacy"}]}),
        )
        .await
        .expect("legacy steer");
    assert_eq!(legacy_steer["outcome"], "injected");

    // 3. Native v2 `session.steer(...)` (concurrent `session/prompt` merged on next turn event)
    let mut steered_handle = session
        .steer("steer via v2 concurrent prompt")
        .await
        .unwrap();
    let steered_task = tokio::spawn(async move { steered_handle.finish().await });
    tokio::time::sleep(Duration::from_millis(20)).await;
    ctrl.send_raw(json!({
        "method": "item/reasoning/summaryTextDelta",
        "params": {"threadId": "thread-1", "turnId": "turn-4", "itemId": "r-steer", "delta": "thinking..."}
    }))
    .await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(
        ctrl.calls_for("turn/steer").len(),
        3,
        "all 3 steering paths must dispatch `turn/steer`"
    );

    ctrl.release_turn("turn-4").await;
    assert_end_turn(&held_task.await.unwrap());
    assert_end_turn(&steered_task.await.unwrap());

    // Mid-turn `session.cancel()` interrupts active turn and returns `Cancelled`
    let mut cancelled_turn = session.prompt("cancel me").expect("cancel turn");
    let cancelled_task = tokio::spawn(async move { cancelled_turn.finish().await });
    tokio::time::sleep(Duration::from_millis(30)).await;
    session.cancel().await.expect("cancel");
    assert_cancelled(&cancelled_task.await.unwrap());

    // Disconnect while another turn is active -> `shutdown_all` must send `turn/interrupt`!
    let interrupt_before = ctrl.calls_for("turn/interrupt").len();
    let mut dangling = session
        .prompt("interrupted on disconnect")
        .expect("dangling");
    let _dangling_task = tokio::spawn(async move { dangling.finish().await });
    tokio::time::sleep(Duration::from_millis(30)).await;

    client.close().await;
    drop(client);
    drop(session);
    let _ = tokio::time::timeout(Duration::from_secs(2), conn_task).await;
    let _ = tokio::time::timeout(Duration::from_secs(2), server_task).await;

    assert!(
        ctrl.calls_for("turn/interrupt").len() > interrupt_before,
        "closing ACP connection during an active turn must send `turn/interrupt`"
    );
}

// --- Test 4: ACP v1 wire compatibility, gateway auth, legacy RPCs, and notice capability gating

struct V1RawSink {
    updates: Arc<Mutex<Vec<Value>>>,
}

impl Handler for V1RawSink {
    fn request(
        &self,
        req: RpcRequest,
        _cx: RequestContext,
    ) -> impl std::future::Future<Output = RpcResponse> + Send {
        if req.method == "session/request_permission" {
            return std::future::ready(RpcResponse::success(
                req.id,
                json!({"outcome": {"outcome": "selected", "optionId": "allow_once"}}),
            ));
        }
        std::future::ready(RpcResponse::error(
            req.id,
            codex_acp::aither_acp::RpcError::method_not_found(&req.method),
        ))
    }

    fn notification(&self, note: RpcNotification) -> impl std::future::Future<Output = ()> + Send {
        if note.method == "session/update"
            && let Some(p) = note.params
        {
            self.updates.lock().unwrap().push(p);
        }
        std::future::ready(())
    }
}

#[allow(clippy::too_many_lines)]
async fn run_v1_session(advertise_notices: bool) -> (Vec<Value>, Peer, MockControl) {
    let (cfg, ctrl) = spawn_mock_app_server();
    let (client_t, server_t) = rpc::Memory::pair();
    let (_server_peer, server_fut) = serve(server_t, cfg);
    tokio::spawn(server_fut);

    let updates = Arc::new(Mutex::new(Vec::new()));
    let (peer, client_conn) = rpc::connection(
        client_t,
        V1RawSink {
            updates: Arc::clone(&updates),
        },
    );
    tokio::spawn(client_conn);

    let caps = if advertise_notices {
        json!({"session": {"notices": {}}})
    } else {
        json!({})
    };
    let init: Value = peer
        .request(
            "initialize",
            &json!({
                "protocolVersion": 1,
                "clientCapabilities": caps,
                "clientInfo": {"name": "v1-editor", "version": "1.0"}
            }),
        )
        .await
        .expect("v1 initialize");
    assert_eq!(init["protocolVersion"], 1);
    assert_eq!(init["agentCapabilities"]["loadSession"], true);

    // Gateway auth over v1 `authenticate`
    let _: Value = peer
        .request(
            "authenticate",
            &json!({
                "methodId": "gateway",
                "_meta": {
                    "gateway": {
                        "baseUrl": "https://gw.example.com/v1",
                        "providerName": "CorpGW",
                        "headers": {"X-Team": "infra"}
                    }
                }
            }),
        )
        .await
        .expect("v1 gateway authenticate");

    // Legacy `authentication/status` reflects gateway!
    let legacy_status: Value = peer
        .request("authentication/status", &json!({}))
        .await
        .expect("legacy authentication/status");
    assert_eq!(legacy_status["type"], "gateway");
    assert_eq!(legacy_status["name"], "CorpGW");

    // `session/new` propagates gateway config into `thread/start`
    let new_res: Value = peer
        .request(
            "session/new",
            &json!({
                "cwd": "/workspace",
                "mcpServers": [{
                    "type": "sse",
                    "name": "sse-srv",
                    "url": "https://sse.example.com/events",
                    "headers": [{"name": "X-Key", "value": "123"}]
                }]
            }),
        )
        .await
        .expect("v1 session/new");
    let sid = new_res["sessionId"].as_str().unwrap().to_string();

    let starts = ctrl.calls_for("thread/start");
    assert_eq!(starts[0]["config"]["model_provider"], "gateway");
    assert_eq!(
        starts[0]["config"]["model_providers"]["gateway"]["base_url"],
        "https://gw.example.com/v1"
    );
    assert_eq!(
        starts[0]["config"]["mcp_servers"]["sse-srv"]["url"],
        "https://sse.example.com/events"
    );

    // Legacy `session/set_model` + v1 `session/set_mode`
    let _: Value = peer
        .request(
            "session/set_model",
            &json!({"sessionId": sid, "modelId": "gpt-6-luna"}),
        )
        .await
        .expect("legacy session/set_model");
    let _: Value = peer
        .request(
            "session/set_mode",
            &json!({"sessionId": sid, "modeId": "read-only"}),
        )
        .await
        .expect("v1 session/set_mode");

    let prompt_res: Value = peer
        .request(
            "session/prompt",
            &json!({
                "sessionId": sid,
                "prompt": [{"type": "text", "text": "hello v1"}]
            }),
        )
        .await
        .expect("v1 session/prompt");
    assert_eq!(prompt_res["stopReason"], "end_turn");

    // Legacy `authentication/logout`
    let _: Value = peer
        .request("authentication/logout", &json!({}))
        .await
        .expect("legacy authentication/logout");

    let collected = updates.lock().unwrap().clone();
    (collected, peer, ctrl)
}

#[tokio::test(flavor = "multi_thread")]
async fn v1_gateway_legacy_methods_and_capability_gated_notices() {
    // 1. Plain v1 client (no `notices` capability) must NOT receive `notice` updates!
    let (plain_updates, plain_peer, plain_ctrl) = run_v1_session(false).await;
    assert!(
        plain_updates
            .iter()
            .any(|u| u["update"]["sessionUpdate"] == "current_mode_update"),
        "v1 set_mode must emit current_mode_update: {plain_updates:?}"
    );
    assert!(
        !plain_updates
            .iter()
            .any(|u| u["update"]["sessionUpdate"] == "notice"),
        "plain v1 client without notices cap must not receive notice updates: {plain_updates:?}"
    );
    assert_eq!(plain_ctrl.calls_for("turn/start")[0]["model"], "gpt-6-luna");
    plain_peer.close();

    // 2. Capability-advertising v1 client (`session.notices: {}`) DOES receive `notice` updates!
    let (cap_updates, cap_peer, _cap_ctrl) = run_v1_session(true).await;
    assert!(
        cap_updates.iter().any(|u| {
            u["update"]["sessionUpdate"] == "notice"
                && u["update"]["title"] == "Rate limit approaching"
        }),
        "v1 client advertising notices capability must receive notice updates: {cap_updates:?}"
    );
    cap_peer.close();
}
