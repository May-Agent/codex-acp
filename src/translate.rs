//! App-server thread items and notifications → ACP `SessionUpdate`s.
//!
//! Items are translated from their JSON form so the mapping stays robust
//! across app-server versions: unknown fields pass through untouched and
//! unknown item kinds degrade to a generic tool call instead of an error.

use agent_client_protocol::v1::schema as acp;
use serde_json::Value;

fn text_block(text: impl Into<String>) -> acp::ContentBlock {
    acp::ContentBlock::Text(acp::TextContent::new(text.into()))
}

fn status_of(s: &str) -> acp::ToolCallStatus {
    match s {
        "completed" => acp::ToolCallStatus::Completed,
        "failed" | "declined" => acp::ToolCallStatus::Failed,
        "inProgress" => acp::ToolCallStatus::InProgress,
        _ => acp::ToolCallStatus::Pending,
    }
}

fn kind_of(item_type: &str) -> acp::ToolKind {
    match item_type {
        "commandExecution" => acp::ToolKind::Execute,
        "fileChange" => acp::ToolKind::Edit,
        "webSearch" | "imageView" => acp::ToolKind::Fetch,
        _ => acp::ToolKind::Other,
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
        .map(str::to_string)
}

fn title_of(item_type: &str, payload: &Value) -> String {
    match item_type {
        "commandExecution" => payload
            .get("command")
            .and_then(Value::as_str)
            .unwrap_or("Command")
            .to_string(),
        "fileChange" => payload
            .get("changes")
            .and_then(Value::as_array)
            .and_then(|a| a.first())
            .and_then(|c| c.get("path"))
            .and_then(Value::as_str)
            .map_or_else(|| "File change".to_string(), |p| format!("Edit {p}")),
        "mcpToolCall" => format!(
            "{}:{}",
            payload
                .get("server")
                .and_then(Value::as_str)
                .unwrap_or("mcp"),
            payload
                .get("tool")
                .and_then(Value::as_str)
                .unwrap_or("tool")
        ),
        "webSearch" => format!(
            "Search: {}",
            payload.get("query").and_then(Value::as_str).unwrap_or("")
        ),
        "dynamicToolCall" | "functionCallOutput" => payload
            .get("tool")
            .or_else(|| payload.get("name"))
            .and_then(Value::as_str)
            .unwrap_or("Tool call")
            .to_string(),
        _ => item_type.to_string(),
    }
}

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

/// A started or historical thread item → a `ToolCall` update, or `None`
/// when the item is not tool-shaped.
#[must_use]
pub fn item_as_tool_call(item: &Value, status: acp::ToolCallStatus) -> Option<acp::SessionUpdate> {
    let p = payload(item);
    let item_type = p.get("type")?.as_str()?;
    if !TOOL_ITEMS.contains(&item_type) {
        return None;
    }
    let status = if status == acp::ToolCallStatus::Pending {
        p.get("status")
            .and_then(Value::as_str)
            .map_or(status, status_of)
    } else {
        status
    };
    let mut call = acp::ToolCall::new(item_id(item)?, title_of(item_type, p))
        .kind(kind_of(item_type))
        .status(status);
    if let Some(content) = diff_content(p) {
        call = call.content(content);
    }
    Some(acp::SessionUpdate::ToolCall(call))
}

fn diff_content(p: &Value) -> Option<Vec<acp::ToolCallContent>> {
    let changes = p.get("changes")?.as_array()?;
    Some(
        changes
            .iter()
            .filter_map(|c| {
                let path = c.get("path").and_then(Value::as_str)?;
                Some(acp::ToolCallContent::Diff(acp::Diff::new(
                    std::path::PathBuf::from(path),
                    c.get("diff").and_then(Value::as_str).unwrap_or_default(),
                )))
            })
            .collect(),
    )
}

/// A completed thread item → its terminal `ToolCallUpdate`.
#[must_use]
pub fn item_completed_update(item: &Value) -> Option<acp::SessionUpdate> {
    let p = payload(item);
    if !TOOL_ITEMS.contains(&p.get("type").and_then(Value::as_str)?) {
        return None;
    }
    let id = item_id(item)?;
    let status = p
        .get("status")
        .and_then(Value::as_str)
        .map_or(acp::ToolCallStatus::Completed, status_of);
    let mut fields = acp::ToolCallUpdateFields::new().status(status);
    if let Some(content) = diff_content(p) {
        fields = fields.content(content);
    }
    if let Some(out) = p
        .get("aggregatedOutput")
        .or_else(|| p.get("output"))
        .or_else(|| p.get("result"))
        .cloned()
    {
        fields = fields.raw_output(out);
    }
    Some(acp::SessionUpdate::ToolCallUpdate(
        acp::ToolCallUpdate::new(id, fields),
    ))
}

/// `item/agentMessage/delta` → `AgentMessageChunk`.
#[must_use]
pub fn agent_delta(delta: &str) -> acp::SessionUpdate {
    acp::SessionUpdate::AgentMessageChunk(acp::ContentChunk::new(text_block(delta)))
}

/// `item/reasoning/*Delta` → `AgentThoughtChunk`.
#[must_use]
pub fn thought_delta(delta: &str) -> acp::SessionUpdate {
    acp::SessionUpdate::AgentThoughtChunk(acp::ContentChunk::new(text_block(delta)))
}

/// A command/file output delta → `ToolCallUpdate` appending text content.
#[must_use]
pub fn output_delta(item_id: &str, delta: &str) -> acp::SessionUpdate {
    let fields = acp::ToolCallUpdateFields::new().content(vec![acp::ToolCallContent::Content(
        acp::Content::new(text_block(delta)),
    )]);
    acp::SessionUpdate::ToolCallUpdate(acp::ToolCallUpdate::new(item_id.to_string(), fields))
}

/// `item/fileChange/patchUpdated` → `ToolCallUpdate` carrying the diff.
#[must_use]
pub fn patch_update(item_id: &str, changes: &[Value]) -> acp::SessionUpdate {
    let content: Vec<_> = changes
        .iter()
        .filter_map(|c| {
            let path = c.get("path").and_then(Value::as_str)?;
            Some(acp::ToolCallContent::Diff(acp::Diff::new(
                path,
                c.get("diff").and_then(Value::as_str).unwrap_or_default(),
            )))
        })
        .collect();
    let fields = acp::ToolCallUpdateFields::new().content(content);
    acp::SessionUpdate::ToolCallUpdate(acp::ToolCallUpdate::new(item_id.to_string(), fields))
}

/// `turn/completed` → ACP stop reason.
#[must_use]
pub fn stop_reason(turn: &Value) -> acp::StopReason {
    match turn.get("status").and_then(Value::as_str) {
        Some("interrupted") => acp::StopReason::Cancelled,
        _ => acp::StopReason::EndTurn,
    }
}

/// `thread/tokenUsage/updated` → `UsageUpdate`.
#[must_use]
pub fn usage_update(token_usage: &Value) -> acp::SessionUpdate {
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
    acp::SessionUpdate::UsageUpdate(acp::UsageUpdate::new(used, size))
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_client_protocol::v1::schema::{SessionUpdate, ToolCallStatus, ToolKind};

    #[test]
    fn usage_reads_total_total_tokens() {
        let usage = serde_json::json!({
            "last": {"inputTokens": 3, "outputTokens": 1, "totalTokens": 4},
            "total": {"inputTokens": 10, "outputTokens": 2, "totalTokens": 12},
            "modelContextWindow": 272_000,
        });
        let SessionUpdate::UsageUpdate(u) = usage_update(&usage) else {
            panic!("expected usage update")
        };
        assert_eq!(u.used, 12);
        assert_eq!(u.size, 272_000);
    }

    #[test]
    fn completed_non_tool_items_emit_nothing() {
        for item in [
            serde_json::json!({"type": "agentMessage", "id": "m1", "text": "hi"}),
            serde_json::json!({"type": "reasoning", "id": "r1", "summary": ["s"]}),
            serde_json::json!({"type": "userMessage", "id": "u1", "content": []}),
            serde_json::json!({"type": "plan", "id": "p1", "text": "plan"}),
        ] {
            assert!(item_completed_update(&item).is_none(), "{item}");
            assert!(item_as_tool_call(&item, ToolCallStatus::Completed).is_none());
        }
    }

    #[test]
    fn completed_tool_item_maps_status_and_output() {
        let item = serde_json::json!({
            "type": "commandExecution", "id": "e1", "command": "ls",
            "status": "completed", "aggregatedOutput": "out",
            "commandActions": [], "cwd": "/tmp",
        });
        let Some(SessionUpdate::ToolCallUpdate(u)) = item_completed_update(&item) else {
            panic!("expected tool call update")
        };
        assert_eq!(u.tool_call_id.0.as_ref(), "e1");
        assert_eq!(u.fields.status, Some(ToolCallStatus::Completed));
        assert_eq!(u.fields.raw_output, Some(serde_json::json!("out")));
    }

    #[test]
    fn tool_item_kinds_and_status() {
        let started = serde_json::json!({
            "type": "fileChange", "id": "f1", "changes": [], "status": "inProgress",
        });
        let Some(SessionUpdate::ToolCall(c)) =
            item_as_tool_call(&started, ToolCallStatus::InProgress)
        else {
            panic!("expected tool call")
        };
        assert_eq!(c.kind, ToolKind::Edit);
        assert_eq!(c.status, ToolCallStatus::InProgress);
    }

    #[test]
    fn stop_reason_mapping() {
        assert_eq!(
            stop_reason(&serde_json::json!({"status": "interrupted"})),
            acp::StopReason::Cancelled
        );
        assert_eq!(
            stop_reason(&serde_json::json!({"status": "failed"})),
            acp::StopReason::EndTurn
        );
        assert_eq!(
            stop_reason(&serde_json::json!({"status": "completed"})),
            acp::StopReason::EndTurn
        );
    }
}
