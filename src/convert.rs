//! Conversions between ACP domain types and `codex app-server` wire models.
//!
//! Covers MCP server config, prompt `ContentBlock` -> `UserInput`, thread items
//! and streaming notifications -> `SessionUpdate`, history replay, and timestamps.

use std::collections::BTreeMap;
use std::path::PathBuf;

use aither_acp::domain::patch::EmptyObject;
use aither_acp::domain::{
    ClientCapabilities, Content, ContentBlock, ContentChunk, Diff, DiffChange, DiffOperation,
    DiffPatch, DiffPatchFormat, DiffPathChange, DiffPathPairChange, ImageContent, Location,
    McpServer, MessageId, NoticeSeverity, Open, Patch, PlanEntry, PlanEntryPriority,
    PlanEntryStatus, ResourceContents, SessionUpdate, StopReason, ToolCallCaps, ToolCallContent,
    ToolCallContentChunk, ToolCallId, ToolCallStatus, ToolCallUpdate, ToolKind, UsageUpdate,
};
use aither_acp::server::SessionContext;
use aither_acp::vendor::codex::IS_MCP_TOOL_CALL;
use codex_codes::{
    UserInput,
    protocol_generated::types::{UserInputAudioReference, UserInputImageReference},
};
use serde_json::{Value, json};

use crate::error::{CodexAcpError, Result};

const TOOL_ITEMS: &[&str] = &[
    "commandExecution",
    "fileChange",
    "mcpToolCall",
    "dynamicToolCall",
    "webSearch",
    "imageGeneration",
    "collabAgentToolCall",
    "subAgentActivity",
    "functionCallOutput",
    "imageView",
];

/// Convert client-supplied ACP [`McpServer`] specs into a Codex `mcp_servers`
/// config object (`{"mcp_servers": { ... }}`).
#[must_use]
pub fn mcp_servers_to_config_map(servers: &[McpServer]) -> serde_json::Map<String, Value> {
    let mut mcp_map = serde_json::Map::new();
    for server in servers {
        match server {
            McpServer::Stdio(stdio) => {
                let env: serde_json::Map<String, Value> = stdio
                    .env
                    .iter()
                    .map(|e| (e.name.clone(), Value::String(e.value.clone())))
                    .collect();
                mcp_map.insert(
                    stdio.name.clone(),
                    json!({
                        "command": stdio.command,
                        "args": stdio.args,
                        "env": env,
                    }),
                );
            }
            McpServer::Http(http) => {
                let headers: serde_json::Map<String, Value> = http
                    .headers
                    .iter()
                    .map(|h| (h.name.clone(), Value::String(h.value.clone())))
                    .collect();
                mcp_map.insert(
                    http.name.clone(),
                    json!({
                        "url": http.url,
                        "headers": headers,
                    }),
                );
            }
            McpServer::Unknown(tag, raw) if tag == "sse" => {
                if let Some(name) = raw
                    .get("name")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                {
                    let url = raw.get("url").and_then(Value::as_str).unwrap_or_default();
                    let headers = extract_raw_headers(raw.get("headers"));
                    mcp_map.insert(
                        name.to_string(),
                        json!({
                            "url": url,
                            "headers": headers,
                        }),
                    );
                }
            }
            _ => {}
        }
    }
    let mut root = serde_json::Map::new();
    root.insert("mcp_servers".to_string(), Value::Object(mcp_map));
    root
}

fn extract_raw_headers(headers: Option<&Value>) -> serde_json::Map<String, Value> {
    let mut out = serde_json::Map::new();
    match headers {
        Some(Value::Array(arr)) => {
            for item in arr {
                if let (Some(name), Some(val)) = (
                    item.get("name").and_then(Value::as_str),
                    item.get("value").and_then(Value::as_str),
                ) {
                    out.insert(name.to_string(), Value::String(val.to_string()));
                }
            }
        }
        Some(Value::Object(map)) => {
            for (k, v) in map {
                if let Some(val) = v.as_str() {
                    out.insert(k.clone(), Value::String(val.to_string()));
                }
            }
        }
        _ => {}
    }
    out
}

/// Extract MCP server names from client-supplied [`McpServer`] specs.
#[must_use]
pub fn extract_mcp_server_names(servers: &[McpServer]) -> Vec<String> {
    servers
        .iter()
        .filter_map(|s| match s {
            McpServer::Stdio(stdio) => Some(stdio.name.clone()),
            McpServer::Http(http) => Some(http.name.clone()),
            McpServer::Unknown(tag, raw) if tag == "sse" => raw
                .get("name")
                .and_then(Value::as_str)
                .filter(|n| !n.is_empty())
                .map(ToOwned::to_owned),
            _ => None,
        })
        .collect()
}

/// Convert ACP prompt [`ContentBlock`]s into `codex app-server` [`UserInput`] items.
///
/// # Errors
///
/// Returns [`CodexAcpError::InvalidParams`] if `blocks` is empty or an `Image`
/// block omits both `data` and `uri`.
pub fn convert_prompt_blocks(blocks: &[ContentBlock]) -> Result<Vec<UserInput>> {
    if blocks.is_empty() {
        return Err(CodexAcpError::InvalidParams(
            "prompt must contain at least one content block".to_string(),
        ));
    }

    let mut out = Vec::with_capacity(blocks.len());
    for block in blocks {
        match block {
            ContentBlock::Text(text) => {
                out.push(UserInput::Text {
                    text: text.text.clone(),
                    text_elements: None,
                });
            }
            ContentBlock::Image(img) => {
                out.push(convert_image_input(img)?);
            }
            ContentBlock::Audio(audio) => {
                out.push(UserInput::Audio {
                    audio: UserInputAudioReference::Inline {
                        url: format!("data:{};base64,{}", audio.mime_type, audio.data),
                    },
                });
            }
            ContentBlock::Resource(res) => match &res.resource {
                ResourceContents::Text(t) => {
                    out.push(UserInput::Text {
                        text: t.text.clone(),
                        text_elements: None,
                    });
                }
                ResourceContents::Blob(b) => {
                    let mime = b.mime_type.as_deref().unwrap_or("application/octet-stream");
                    if mime.starts_with("image/") {
                        out.push(UserInput::Image {
                            detail: None,
                            image: UserInputImageReference::Inline {
                                url: format!("data:{mime};base64,{}", b.blob),
                            },
                        });
                    } else if mime.starts_with("audio/") {
                        out.push(UserInput::Audio {
                            audio: UserInputAudioReference::Inline {
                                url: format!("data:{mime};base64,{}", b.blob),
                            },
                        });
                    } else {
                        out.push(UserInput::Text {
                            text: format!("[Embedded resource: {} ({mime})]", b.uri),
                            text_elements: None,
                        });
                    }
                }
            },
            ContentBlock::ResourceLink(link) => {
                if let Some(path) = link.uri.strip_prefix("file://") {
                    out.push(UserInput::Mention {
                        name: link.name.clone(),
                        path: path.to_string(),
                    });
                } else {
                    out.push(UserInput::Text {
                        text: format!("[Resource link: {} ({})]", link.name, link.uri),
                        text_elements: None,
                    });
                }
            }
            ContentBlock::Custom(_, raw) | ContentBlock::Unknown(_, raw) => {
                if let Some(text) = raw.get("text").and_then(Value::as_str) {
                    out.push(UserInput::Text {
                        text: text.to_string(),
                        text_elements: None,
                    });
                }
            }
        }
    }
    if out.is_empty() {
        return Err(CodexAcpError::InvalidParams(
            "prompt contained no supported content blocks".to_string(),
        ));
    }
    Ok(out)
}

fn convert_image_input(img: &ImageContent) -> Result<UserInput> {
    if let Some(data) = img.data.as_deref().filter(|d| !d.is_empty()) {
        return Ok(UserInput::Image {
            detail: None,
            image: UserInputImageReference::Inline {
                url: format!("data:{};base64,{data}", img.mime_type),
            },
        });
    }
    if let Some(uri) = img.uri.as_deref().filter(|u| !u.is_empty()) {
        if let Some(local_path) = uri.strip_prefix("file://") {
            return Ok(UserInput::LocalImage {
                detail: None,
                path: local_path.to_string(),
            });
        }
        return Ok(UserInput::Image {
            detail: None,
            image: UserInputImageReference::Inline {
                url: uri.to_string(),
            },
        });
    }
    Err(CodexAcpError::InvalidParams(
        "image content block requires non-empty `data` or `uri`".to_string(),
    ))
}

/// Whether a v1 client advertised support for `notice` session updates.
#[must_use]
pub fn client_supports_notices(caps: &ClientCapabilities) -> bool {
    let session_notices = caps
        .extra
        .get("session")
        .and_then(|s| s.get("notices"))
        .is_some_and(|v| v.is_object() || v.as_bool() == Some(true));
    let top_notices = caps
        .extra
        .get("notices")
        .is_some_and(|v| v.is_object() || v.as_bool() == Some(true));
    let meta_notices = caps
        .meta
        .as_ref()
        .and_then(|m| m.get("notices"))
        .is_some_and(|v| v.is_object() || v.as_bool() == Some(true));
    session_notices || top_notices || meta_notices
}

/// Emit a session notice on `cx`: native `Notice` on v2, capability-gated
/// `notice` payload on v1.
pub async fn emit_notice(
    cx: &SessionContext,
    severity: NoticeSeverity,
    title: impl Into<String>,
    description: Option<String>,
) {
    let title = title.into();
    if cx.protocol_version().is_v1() {
        if client_supports_notices(cx.client_caps()) {
            let sev_str = match severity {
                NoticeSeverity::Info => "info",
                NoticeSeverity::Warning => "warning",
                NoticeSeverity::Error => "error",
            };
            let mut obj = serde_json::Map::new();
            obj.insert("sessionUpdate".to_string(), json!("notice"));
            obj.insert("severity".to_string(), json!(sev_str));
            obj.insert("title".to_string(), json!(title));
            if let Some(desc) = description.filter(|s| !s.is_empty()) {
                obj.insert("description".to_string(), json!(desc));
            }
            let _ = cx
                .update(SessionUpdate::Unknown(
                    "notice".to_string(),
                    Value::Object(obj),
                ))
                .await;
        }
    } else {
        let _ = cx
            .update(SessionUpdate::Notice(aither_acp::domain::Notice {
                severity: Open::Known(severity),
                title,
                description: description.filter(|s| !s.is_empty()),
                meta: None,
                extra: BTreeMap::new(),
            }))
            .await;
    }
}

/// Emit a notice extracted from an app-server error/warning notification payload.
pub async fn emit_notice_from_payload(
    cx: &SessionContext,
    severity: NoticeSeverity,
    payload: &Value,
) {
    let title = payload
        .get("message")
        .or_else(|| payload.get("error").and_then(|e| e.get("message")))
        .or_else(|| payload.get("summary"))
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .unwrap_or("Notification");
    let description = payload
        .get("additionalDetails")
        .or_else(|| payload.get("details"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    emit_notice(cx, severity, title, description).await;
}

fn status_of(s: &str) -> ToolCallStatus {
    match s {
        "completed" => ToolCallStatus::Completed,
        "failed" | "declined" => ToolCallStatus::Failed,
        "inProgress" => ToolCallStatus::InProgress,
        _ => ToolCallStatus::Pending,
    }
}

fn kind_of(item_type: &str) -> ToolKind {
    match item_type {
        "commandExecution" => ToolKind::Execute,
        "fileChange" => ToolKind::Edit,
        "webSearch" => ToolKind::Search,
        "imageView" => ToolKind::Fetch,
        _ => ToolKind::Other,
    }
}

fn payload(item: &Value) -> &Value {
    item.get("payload").unwrap_or(item)
}

fn item_id(item: &Value) -> Option<String> {
    payload(item)
        .get("id")
        .or_else(|| item.get("id"))
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

fn title_of(item_type: &str, p: &Value) -> String {
    match item_type {
        "commandExecution" => p
            .get("command")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .unwrap_or("Command")
            .to_string(),
        "fileChange" => p
            .get("changes")
            .and_then(Value::as_array)
            .and_then(|a| a.first())
            .and_then(|c| c.get("path"))
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map_or_else(|| "File change".to_string(), |path| format!("Edit {path}")),
        "mcpToolCall" => format!(
            "{}:{}",
            p.get("server").and_then(Value::as_str).unwrap_or("mcp"),
            p.get("tool").and_then(Value::as_str).unwrap_or("tool")
        ),
        "webSearch" => format!(
            "Search: {}",
            p.get("query").and_then(Value::as_str).unwrap_or("")
        ),
        "dynamicToolCall" | "functionCallOutput" => p
            .get("tool")
            .or_else(|| p.get("name"))
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .unwrap_or("Tool call")
            .to_string(),
        _ => item_type.to_string(),
    }
}

fn name_of(item_type: &str, p: &Value) -> String {
    match item_type {
        "mcpToolCall" | "dynamicToolCall" | "functionCallOutput" => p
            .get("tool")
            .or_else(|| p.get("name"))
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .unwrap_or(item_type)
            .to_string(),
        _ => item_type.to_string(),
    }
}

fn locations_of(item_type: &str, p: &Value) -> Vec<Location> {
    let mut out = Vec::new();
    match item_type {
        "commandExecution" => {
            if let Some(cwd) = p
                .get("cwd")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
            {
                out.push(Location {
                    path: PathBuf::from(cwd),
                    line: None,
                    meta: None,
                });
            }
        }
        "fileChange" => {
            if let Some(changes) = p.get("changes").and_then(Value::as_array) {
                for path in changes
                    .iter()
                    .filter_map(|c| c.get("path").and_then(Value::as_str))
                    .filter(|s| !s.is_empty())
                {
                    out.push(Location {
                        path: PathBuf::from(path),
                        line: None,
                        meta: None,
                    });
                }
            }
        }
        _ => {}
    }
    out
}

fn operation_of(c: &Value) -> DiffOperation {
    let path = || DiffPathChange {
        path: PathBuf::from(c.get("path").and_then(Value::as_str).unwrap_or_default()),
    };
    match c
        .get("kind")
        .or_else(|| c.get("type"))
        .or_else(|| c.get("operation"))
        .and_then(Value::as_str)
    {
        Some("add" | "create" | "created") => DiffOperation::Add(path()),
        Some("delete" | "deleted" | "remove") => DiffOperation::Delete(path()),
        Some("move" | "rename" | "renamed") => DiffOperation::Move(DiffPathPairChange {
            old_path: PathBuf::from(
                c.get("oldPath")
                    .or_else(|| c.get("old_path"))
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
            ),
            path: PathBuf::from(c.get("path").and_then(Value::as_str).unwrap_or_default()),
        }),
        _ => DiffOperation::Modify(path()),
    }
}

/// Convert a Codex `changes` array into ACP [`ToolCallContent::Diff`] items.
#[must_use]
pub fn diff_content(p: &Value) -> Option<Vec<ToolCallContent>> {
    let changes = p.get("changes")?.as_array()?;
    let items: Vec<ToolCallContent> = changes
        .iter()
        .filter(|c| {
            c.get("path")
                .and_then(Value::as_str)
                .is_some_and(|s| !s.is_empty())
        })
        .map(|c| {
            let text = c
                .get("diff")
                .or_else(|| c.get("patch"))
                .or_else(|| c.get("newText"))
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            ToolCallContent::Diff(Diff {
                changes: vec![DiffChange {
                    operation: Open::Known(operation_of(c)),
                    file_type: None,
                    mime_type: None,
                    meta: None,
                    extra: BTreeMap::new(),
                }],
                patch: Some(DiffPatch {
                    format: Open::Known(DiffPatchFormat::GitPatch),
                    text,
                    meta: None,
                }),
                meta: None,
                extra: BTreeMap::new(),
            })
        })
        .collect();
    (!items.is_empty()).then_some(items)
}

/// Translate a started or historical thread item into a [`ToolCallUpdate`] upsert,
/// returning `None` when the item is not tool-shaped.
#[must_use]
pub fn item_as_tool_call(item: &Value, status: Option<ToolCallStatus>) -> Option<ToolCallUpdate> {
    let p = payload(item);
    let item_type = p.get("type")?.as_str()?;
    if !TOOL_ITEMS.contains(&item_type) {
        return None;
    }
    let status = status.unwrap_or_else(|| {
        p.get("status")
            .and_then(Value::as_str)
            .map_or(ToolCallStatus::Pending, status_of)
    });
    let mut update = ToolCallUpdate::new(ToolCallId::from_string(item_id(item)?));
    update.name = Patch::Set(name_of(item_type, p));
    update.title = Patch::Set(title_of(item_type, p));
    update.kind = Patch::Set(Open::Known(kind_of(item_type)));
    update.status = Patch::Set(Open::Known(status));
    update.raw_input = Patch::Set(p.clone());
    let locations = locations_of(item_type, p);
    if !locations.is_empty() {
        update.locations = Patch::Set(locations);
    }
    if let Some(content) = diff_content(p) {
        update.content = Patch::Set(content);
    }
    if item_type == "mcpToolCall" {
        update.meta = Patch::Set(json!({ IS_MCP_TOOL_CALL: true }));
    }
    if item_type == "commandExecution" && status == ToolCallStatus::InProgress {
        update.capabilities = Patch::Set(ToolCallCaps {
            cancel: Some(EmptyObject),
            meta: None,
            extra: BTreeMap::new(),
        });
    }
    Some(update)
}

/// Translate a completed thread item into its terminal [`ToolCallUpdate`].
#[must_use]
pub fn item_completed_update(item: &Value) -> Option<ToolCallUpdate> {
    let p = payload(item);
    let item_type = p.get("type").and_then(Value::as_str)?;
    if !TOOL_ITEMS.contains(&item_type) {
        return None;
    }
    let mut update = ToolCallUpdate::new(ToolCallId::from_string(item_id(item)?));
    let status = p
        .get("status")
        .and_then(Value::as_str)
        .map_or(ToolCallStatus::Completed, status_of);
    update.status = Patch::Set(Open::Known(status));
    if item_type == "mcpToolCall" {
        update.meta = Patch::Set(json!({ IS_MCP_TOOL_CALL: true }));
    }
    let raw_output = p
        .get("aggregatedOutput")
        .or_else(|| p.get("output"))
        .or_else(|| p.get("result"))
        .cloned();
    let mut content = diff_content(p).unwrap_or_default();
    if let Some(out_text) = raw_output.as_ref().and_then(Value::as_str)
        && !out_text.is_empty()
    {
        content.push(ToolCallContent::Content(Content {
            content: ContentBlock::text(out_text),
            meta: None,
            extra: BTreeMap::new(),
        }));
    }
    if !content.is_empty() {
        update.content = Patch::Set(content);
    }
    if let Some(out) = raw_output {
        update.raw_output = Patch::Set(out);
    }
    Some(update)
}

/// Translate a command/file output delta into a [`ToolCallContentChunk`] appending text.
#[must_use]
pub fn output_chunk(item_id: &str, delta: &str) -> ToolCallContentChunk {
    ToolCallContentChunk {
        tool_call_id: ToolCallId::from_string(item_id),
        content: ToolCallContent::Content(Content {
            content: ContentBlock::text(delta),
            meta: None,
            extra: BTreeMap::new(),
        }),
        meta: None,
        extra: BTreeMap::new(),
    }
}

/// Translate `item/fileChange/patchUpdated` into a [`ToolCallUpdate`] carrying diffs.
#[must_use]
pub fn patch_update(item_id: &str, changes: &[Value]) -> ToolCallUpdate {
    let mut update = ToolCallUpdate::new(ToolCallId::from_string(item_id));
    let wrapper = json!({"changes": changes});
    if let Some(content) = diff_content(&wrapper) {
        update.content = Patch::Set(content);
    }
    let locations = locations_of("fileChange", &wrapper);
    if !locations.is_empty() {
        update.locations = Patch::Set(locations);
    }
    update
}

/// Translate a `turn/plan/updated` payload into ACP [`PlanEntry`] items.
#[must_use]
pub fn plan_entries(plan: &Value) -> Vec<PlanEntry> {
    plan.get("plan")
        .and_then(Value::as_array)
        .or_else(|| plan.get("entries").and_then(Value::as_array))
        .map(|es| {
            es.iter()
                .map(|e| PlanEntry {
                    content: e
                        .get("step")
                        .or_else(|| e.get("content"))
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                    priority: Some(Open::Known(PlanEntryPriority::Medium)),
                    status: Some(Open::Known(match e.get("status").and_then(Value::as_str) {
                        Some("completed") => PlanEntryStatus::Completed,
                        Some("inProgress" | "in_progress") => PlanEntryStatus::InProgress,
                        _ => PlanEntryStatus::Pending,
                    })),
                    meta: None,
                    extra: BTreeMap::new(),
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Translate `turn/completed` into an ACP [`StopReason`].
#[must_use]
pub fn stop_reason(turn: &Value) -> StopReason {
    match turn.get("status").and_then(Value::as_str) {
        Some("interrupted") => StopReason::Cancelled,
        _ => StopReason::EndTurn,
    }
}

/// Translate `thread/tokenUsage/updated` into an ACP [`UsageUpdate`].
#[must_use]
pub fn usage_update(token_usage: &Value) -> UsageUpdate {
    let used = token_usage
        .get("total")
        .and_then(|t| t.get("totalTokens"))
        .or_else(|| token_usage.get("totalTokens"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let size = token_usage
        .get("modelContextWindow")
        .or_else(|| token_usage.get("contextWindow"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    UsageUpdate {
        used,
        size: Some(size),
        cost: None,
        meta: None,
        extra: BTreeMap::new(),
    }
}

/// Build a replayed user message chunk.
#[must_use]
pub fn user_chunk(text: impl Into<String>) -> SessionUpdate {
    SessionUpdate::UserMessageChunk(ContentChunk::new(
        MessageId::new(),
        ContentBlock::text(text.into()),
    ))
}

/// Build a replayed agent message chunk.
#[must_use]
pub fn agent_chunk(text: impl Into<String>) -> SessionUpdate {
    SessionUpdate::AgentMessageChunk(ContentChunk::new(
        MessageId::new(),
        ContentBlock::text(text.into()),
    ))
}

/// Build a replayed reasoning summary chunk.
#[must_use]
pub fn thought_chunk(text: impl Into<String>) -> SessionUpdate {
    SessionUpdate::AgentThoughtChunk(ContentChunk::new(
        MessageId::new(),
        ContentBlock::text(text.into()),
    ))
}

/// Convert a serialized thread `turns` array (`thread/read`) into replayed [`SessionUpdate`]s.
#[must_use]
pub fn replay_turns_to_updates(turns: &[Value]) -> Vec<SessionUpdate> {
    let mut updates = Vec::new();
    for turn in turns {
        let Some(items) = turn.get("items").and_then(Value::as_array) else {
            continue;
        };
        for item in items {
            updates.extend(replay_item_updates(item));
        }
    }
    updates
}

fn replay_item_updates(item: &Value) -> Vec<SessionUpdate> {
    let p = payload(item);
    let item_type = p.get("type").and_then(Value::as_str).unwrap_or_default();
    let mut out = Vec::new();
    match item_type {
        "userMessage" => {
            if let Some(arr) = p.get("content").and_then(Value::as_array) {
                for entry in arr {
                    if let Some(text) = entry
                        .get("text")
                        .and_then(Value::as_str)
                        .filter(|s| !s.is_empty())
                    {
                        out.push(user_chunk(text));
                    }
                }
            } else if let Some(text) = p
                .get("text")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
            {
                out.push(user_chunk(text));
            }
        }
        "agentMessage" => {
            if let Some(text) = p
                .get("text")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
            {
                out.push(agent_chunk(text));
            }
        }
        "reasoning" => {
            if let Some(arr) = p.get("summary").and_then(Value::as_array) {
                for entry in arr {
                    if let Some(text) = entry.as_str().filter(|s| !s.is_empty()) {
                        out.push(thought_chunk(text));
                    }
                }
            }
        }
        _ => {}
    }
    if let Some(mut update) = item_as_tool_call(item, None) {
        if let Some(done) = item_completed_update(item) {
            if !done.content.is_unchanged() {
                update.content = done.content;
            }
            if !done.raw_output.is_unchanged() {
                update.raw_output = done.raw_output;
            }
            update.status = done.status;
        }
        out.push(SessionUpdate::ToolCallUpdate(update));
    }
    out
}

/// Format an epoch timestamp (seconds or milliseconds) as RFC 3339 UTC.
///
/// Values `< 100_000_000_000` (before year 5138 in seconds) are treated as
/// Unix seconds; larger values are treated as Unix milliseconds.
#[must_use]
pub fn rfc3339_from_timestamp(ts: i64) -> String {
    let ms = if ts.abs() < 100_000_000_000 {
        ts.saturating_mul(1000)
    } else {
        ts
    };
    let secs = ms.div_euclid(1000);
    let nanos = u32::try_from(ms.rem_euclid(1000)).unwrap_or_default() * 1_000_000;
    let days = secs.div_euclid(86_400);
    let tod = secs.rem_euclid(86_400);
    let (hour, min, sec) = (tod / 3600, tod % 3600 / 60, tod % 60);
    // Howard Hinnant's civil_from_days algorithm (epoch 1970-01-01 -> 0000-03-01).
    let shifted = days + 719_468;
    let era = shifted.div_euclid(146_097);
    let doe = shifted.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let full_year = if month <= 2 { year + 1 } else { year };
    if nanos == 0 {
        format!("{full_year:04}-{month:02}-{day:02}T{hour:02}:{min:02}:{sec:02}Z")
    } else {
        format!("{full_year:04}-{month:02}-{day:02}T{hour:02}:{min:02}:{sec:02}.{nanos:09}Z")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn usage_reads_total_total_tokens() {
        let usage = json!({
            "last": {"inputTokens": 3, "outputTokens": 1, "totalTokens": 4},
            "total": {"inputTokens": 10, "outputTokens": 2, "totalTokens": 12},
            "modelContextWindow": 272_000,
        });
        let u = usage_update(&usage);
        assert_eq!(u.used, 12);
        assert_eq!(u.size, Some(272_000));
    }

    #[test]
    fn completed_non_tool_items_emit_nothing() {
        for item in [
            json!({"type": "agentMessage", "id": "m1", "text": "hi"}),
            json!({"type": "reasoning", "id": "r1", "summary": ["s"]}),
            json!({"type": "userMessage", "id": "u1", "content": []}),
            json!({"type": "plan", "id": "p1", "text": "plan"}),
        ] {
            assert!(item_completed_update(&item).is_none(), "{item}");
            assert!(item_as_tool_call(&item, Some(ToolCallStatus::Completed)).is_none());
        }
    }

    #[test]
    fn completed_tool_item_maps_status_and_output() {
        let item = json!({
            "type": "commandExecution", "id": "e1", "command": "ls",
            "status": "completed", "aggregatedOutput": "out",
            "commandActions": [], "cwd": "/tmp",
        });
        let u = item_completed_update(&item).expect("tool call update");
        assert_eq!(u.tool_call_id.as_str(), "e1");
        assert!(matches!(
            u.status,
            Patch::Set(Open::Known(ToolCallStatus::Completed))
        ));
        assert_eq!(u.raw_output, Patch::Set(json!("out")));
    }

    #[test]
    fn mcp_tool_item_sets_vendor_flag() {
        let item = json!({
            "type": "mcpToolCall", "id": "m1", "server": "docs", "tool": "search",
            "status": "inProgress",
        });
        let u = item_as_tool_call(&item, Some(ToolCallStatus::InProgress)).expect("mcp tool");
        assert_eq!(u.meta, Patch::Set(json!({ IS_MCP_TOOL_CALL: true })));
    }

    #[test]
    fn stop_reason_mapping() {
        assert_eq!(
            stop_reason(&json!({"status": "interrupted"})),
            StopReason::Cancelled
        );
        assert_eq!(
            stop_reason(&json!({"status": "failed"})),
            StopReason::EndTurn
        );
        assert_eq!(
            stop_reason(&json!({"status": "completed"})),
            StopReason::EndTurn
        );
    }

    #[test]
    fn rfc3339_handles_seconds_and_milliseconds() {
        assert_eq!(rfc3339_from_timestamp(0), "1970-01-01T00:00:00Z");
        assert_eq!(
            rfc3339_from_timestamp(1_700_000_000),
            "2023-11-14T22:13:20Z"
        );
        assert_eq!(
            rfc3339_from_timestamp(1_700_000_000_500),
            "2023-11-14T22:13:20.500000000Z"
        );
    }
}
