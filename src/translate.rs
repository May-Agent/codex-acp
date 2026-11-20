//! App-server thread items and notifications → ACP domain updates.
//!
//! Items are translated from their JSON form so the mapping stays robust
//! across app-server versions: unknown fields pass through untouched and
//! unknown item kinds degrade to a generic tool call instead of an error.

use std::collections::BTreeMap;
use std::path::PathBuf;

use aither_acp::domain::patch::EmptyObject;
use aither_acp::domain::{
    Content, Diff, DiffChange, DiffOperation, DiffPatch, DiffPatchFormat, DiffPathChange, Location,
    Open, Patch, PlanEntry, PlanEntryPriority, PlanEntryStatus, SessionUpdate, StopReason,
    ToolCallCaps, ToolCallContent, ToolCallContentChunk, ToolCallId, ToolCallStatus,
    ToolCallUpdate, ToolKind, UsageUpdate,
};
use serde_json::Value;

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
        "webSearch" | "imageView" => ToolKind::Fetch,
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

/// The programmatic name reported for an item kind.
fn name_of(item_type: &str, payload: &Value) -> String {
    match item_type {
        "mcpToolCall" | "dynamicToolCall" | "functionCallOutput" => payload
            .get("tool")
            .or_else(|| payload.get("name"))
            .and_then(Value::as_str)
            .unwrap_or(item_type)
            .to_string(),
        _ => item_type.to_string(),
    }
}

/// Locations a tool item touches, when derivable.
fn locations_of(item_type: &str, payload: &Value) -> Vec<Location> {
    let mut out = Vec::new();
    match item_type {
        "commandExecution" => {
            if let Some(cwd) = payload.get("cwd").and_then(Value::as_str) {
                out.push(Location {
                    path: PathBuf::from(cwd),
                    line: None,
                    meta: None,
                });
            }
        }
        "fileChange" => {
            if let Some(changes) = payload.get("changes").and_then(Value::as_array) {
                for path in changes
                    .iter()
                    .filter_map(|c| c.get("path").and_then(Value::as_str))
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

/// A started or historical thread item → a `ToolCallUpdate` upsert, or
/// `None` when the item is not tool-shaped.
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
    // Commands may be interrupted mid-flight; mark them cancellable.
    if item_type == "commandExecution" && status == ToolCallStatus::InProgress {
        update.capabilities = Patch::Set(ToolCallCaps {
            cancel: Some(EmptyObject),
            meta: None,
            extra: BTreeMap::new(),
        });
    }
    Some(update)
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

use aither_acp::domain::DiffPathPairChange;

/// The codex `changes` array → one `diff` content item per file change.
fn diff_content(p: &Value) -> Option<Vec<ToolCallContent>> {
    let changes = p.get("changes")?.as_array()?;
    Some(
        changes
            .iter()
            .filter(|c| c.get("path").and_then(Value::as_str).is_some())
            .map(|c| {
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
                        text: c
                            .get("diff")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_string(),
                        meta: None,
                    }),
                    meta: None,
                    extra: BTreeMap::new(),
                })
            })
            .collect(),
    )
}

/// A completed thread item → its terminal `ToolCallUpdate`.
#[must_use]
pub fn item_completed_update(item: &Value) -> Option<ToolCallUpdate> {
    let p = payload(item);
    if !TOOL_ITEMS.contains(&p.get("type").and_then(Value::as_str)?) {
        return None;
    }
    let mut update = ToolCallUpdate::new(ToolCallId::from_string(item_id(item)?));
    let status = p
        .get("status")
        .and_then(Value::as_str)
        .map_or(ToolCallStatus::Completed, status_of);
    update.status = Patch::Set(Open::Known(status));
    if let Some(content) = diff_content(p) {
        update.content = Patch::Set(content);
    }
    if let Some(out) = p
        .get("aggregatedOutput")
        .or_else(|| p.get("output"))
        .or_else(|| p.get("result"))
        .cloned()
    {
        update.raw_output = Patch::Set(out);
    }
    Some(update)
}

/// A command/file output delta → `ToolCallContentChunk` appending text.
#[must_use]
pub fn output_chunk(item_id: &str, delta: &str) -> ToolCallContentChunk {
    ToolCallContentChunk {
        tool_call_id: ToolCallId::from_string(item_id),
        content: ToolCallContent::Content(Content {
            content: aither_acp::domain::ContentBlock::text(delta),
            meta: None,
            extra: BTreeMap::new(),
        }),
        meta: None,
        extra: BTreeMap::new(),
    }
}

/// `item/fileChange/patchUpdated` → `ToolCallUpdate` carrying the diff.
#[must_use]
pub fn patch_update(item_id: &str, changes: &[Value]) -> ToolCallUpdate {
    let mut update = ToolCallUpdate::new(ToolCallId::from_string(item_id));
    if let Some(content) = diff_content(&serde_json::json!({"changes": changes})) {
        update.content = Patch::Set(content);
    }
    update
}

/// A `turn/plan/updated` payload → plan entries (content/status/priority).
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

/// `turn/completed` → ACP stop reason.
#[must_use]
pub fn stop_reason(turn: &Value) -> StopReason {
    match turn.get("status").and_then(Value::as_str) {
        Some("interrupted") => StopReason::Cancelled,
        _ => StopReason::EndTurn,
    }
}

/// `thread/tokenUsage/updated` → `UsageUpdate`.
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

/// A replayed user message → `UserMessageChunk`.
#[must_use]
pub fn user_chunk(text: impl Into<String>) -> SessionUpdate {
    SessionUpdate::UserMessageChunk(aither_acp::domain::ContentChunk::new(
        aither_acp::domain::MessageId::new(),
        aither_acp::domain::ContentBlock::text(text.into()),
    ))
}

/// A replayed agent message → `AgentMessageChunk`.
#[must_use]
pub fn agent_chunk(text: impl Into<String>) -> SessionUpdate {
    SessionUpdate::AgentMessageChunk(aither_acp::domain::ContentChunk::new(
        aither_acp::domain::MessageId::new(),
        aither_acp::domain::ContentBlock::text(text.into()),
    ))
}

/// A replayed reasoning summary → `AgentThoughtChunk`.
#[must_use]
pub fn thought_chunk(text: impl Into<String>) -> SessionUpdate {
    SessionUpdate::AgentThoughtChunk(aither_acp::domain::ContentChunk::new(
        aither_acp::domain::MessageId::new(),
        aither_acp::domain::ContentBlock::text(text.into()),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use aither_acp::domain::{SessionUpdate as U, ToolCallStatus};

    #[test]
    fn usage_reads_total_total_tokens() {
        let usage = serde_json::json!({
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
            serde_json::json!({"type": "agentMessage", "id": "m1", "text": "hi"}),
            serde_json::json!({"type": "reasoning", "id": "r1", "summary": ["s"]}),
            serde_json::json!({"type": "userMessage", "id": "u1", "content": []}),
            serde_json::json!({"type": "plan", "id": "p1", "text": "plan"}),
        ] {
            assert!(item_completed_update(&item).is_none(), "{item}");
            assert!(item_as_tool_call(&item, Some(ToolCallStatus::Completed)).is_none());
        }
    }

    #[test]
    fn completed_tool_item_maps_status_and_output() {
        let item = serde_json::json!({
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
        assert_eq!(u.raw_output, Patch::Set(serde_json::json!("out")));
    }

    #[test]
    fn tool_item_kinds_and_status() {
        let started = serde_json::json!({
            "type": "fileChange", "id": "f1", "changes": [], "status": "inProgress",
        });
        let c = item_as_tool_call(&started, Some(ToolCallStatus::InProgress)).expect("tool call");
        assert!(matches!(c.kind, Patch::Set(Open::Known(ToolKind::Edit))));
        assert!(matches!(
            c.status,
            Patch::Set(Open::Known(ToolCallStatus::InProgress))
        ));
    }

    #[test]
    fn stop_reason_mapping() {
        assert_eq!(
            stop_reason(&serde_json::json!({"status": "interrupted"})),
            StopReason::Cancelled
        );
        assert_eq!(
            stop_reason(&serde_json::json!({"status": "failed"})),
            StopReason::EndTurn
        );
        assert_eq!(
            stop_reason(&serde_json::json!({"status": "completed"})),
            StopReason::EndTurn
        );
    }

    #[test]
    fn patch_update_carries_diffs() {
        let u = patch_update(
            "f1",
            &[serde_json::json!({"path": "/a.rs", "diff": "@@ -1 +1 @@", "kind": "update"})],
        );
        let Patch::Set(ref content) = u.content else {
            panic!("expected content patch")
        };
        assert_eq!(content.len(), 1);
        let _ = U::ToolCallUpdate(u);
    }
}
