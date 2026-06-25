//! Conductor: an outbound websocket client that lets a DBOS conductor / cloud control plane manage
//! and observe a running app. Ports the Go `conductor.go` connection loop + message dispatch.
//!
//! The app connects out to the conductor; the conductor sends JSON request messages
//! (`{type, request_id, ...}`) and the app replies (`{type, request_id, error_message?, ...}`),
//! mapping each request onto the existing workflow-management operations.

use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

use super::recovery::recover_pending_workflows;
use super::DbosInner;
use crate::db::management::{
    self, ForkWorkflowInput, ListWorkflowsFilter, StepInfo, WorkflowStatus, INTERNAL_QUEUE_NAME,
};
use crate::db::status::WorkflowStatusType;
use crate::error::Result;

const PING_INTERVAL: Duration = Duration::from_secs(20);
const READ_TIMEOUT: Duration = Duration::from_secs(30);
const INITIAL_RECONNECT_WAIT: Duration = Duration::from_secs(1);
const MAX_RECONNECT_WAIT: Duration = Duration::from_secs(30);
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_DOMAIN: &str = "cloud.dbos.dev";
const DBOS_VERSION: &str = env!("CARGO_PKG_VERSION");

pub(crate) struct ConductorConfig {
    /// Base URL, e.g. `wss://cloud.dbos.dev/conductor/v1alpha1`.
    pub url: String,
    pub api_key: String,
    pub app_name: String,
    pub executor_metadata: Option<serde_json::Map<String, Value>>,
}

/// Build a conductor config if enabled (an API key is present). The URL defaults from `DBOS_DOMAIN`.
pub(crate) fn config_from(
    api_key: Option<String>,
    url: Option<String>,
    app_name: &str,
    metadata: Option<serde_json::Map<String, Value>>,
) -> Option<ConductorConfig> {
    let api_key = api_key.filter(|k| !k.is_empty())?;
    let url = url.filter(|u| !u.is_empty()).unwrap_or_else(|| {
        let domain = std::env::var("DBOS_DOMAIN")
            .ok()
            .filter(|d| !d.is_empty())
            .unwrap_or_else(|| DEFAULT_DOMAIN.to_string());
        format!("wss://{domain}/conductor/v1alpha1")
    });
    Some(ConductorConfig {
        url,
        api_key,
        app_name: app_name.to_string(),
        executor_metadata: metadata,
    })
}

/// Percent-encode a single URL path segment (the app name / api key).
fn encode_segment(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn websocket_url(config: &ConductorConfig) -> String {
    let base = config.url.trim_end_matches('/');
    format!(
        "{base}/websocket/{}/{}",
        encode_segment(&config.app_name),
        encode_segment(&config.api_key)
    )
}

/// Install a process-default rustls crypto provider once, so `wss://` connections don't panic.
fn ensure_crypto_provider() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// The conductor client: connect, serve messages, and reconnect with backoff until cancelled.
pub(crate) async fn run_conductor(
    inner: Arc<DbosInner>,
    config: ConductorConfig,
    token: CancellationToken,
) {
    ensure_crypto_provider();
    let ws_url = websocket_url(&config);
    let mut wait = INITIAL_RECONNECT_WAIT;
    loop {
        if token.is_cancelled() {
            return;
        }
        match connect_and_serve(&inner, &config, &ws_url, &token).await {
            Ok(()) => return, // cancelled
            Err(e) => tracing::warn!(error = %e, "conductor disconnected; reconnecting"),
        }
        if token.is_cancelled() {
            return;
        }
        let jitter = 0.5 + rand::random::<f64>();
        tokio::select! {
            _ = token.cancelled() => return,
            _ = tokio::time::sleep(wait.mul_f64(jitter)) => {}
        }
        wait = (wait * 2).min(MAX_RECONNECT_WAIT);
    }
}

/// Returns `Ok(())` only on cancellation; any connection problem returns `Err` to trigger reconnect.
async fn connect_and_serve(
    inner: &Arc<DbosInner>,
    config: &ConductorConfig,
    ws_url: &str,
    token: &CancellationToken,
) -> std::result::Result<(), String> {
    let (ws_stream, _) =
        tokio::time::timeout(HANDSHAKE_TIMEOUT, tokio_tungstenite::connect_async(ws_url))
            .await
            .map_err(|_| "handshake timeout".to_string())?
            .map_err(|e| format!("connect failed: {e}"))?;
    tracing::info!(app = %config.app_name, "conductor connected");

    let (mut sink, mut stream) = ws_stream.split();
    let mut ping = tokio::time::interval(PING_INTERVAL);
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = token.cancelled() => {
                let _ = sink.send(Message::Close(None)).await;
                return Ok(());
            }
            _ = ping.tick() => {
                if sink.send(Message::Ping(Vec::new())).await.is_err() {
                    return Err("ping write failed".into());
                }
            }
            msg = tokio::time::timeout(READ_TIMEOUT, stream.next()) => {
                let msg = match msg {
                    Err(_) => return Err("read timeout".into()),
                    Ok(None) => return Err("stream ended".into()),
                    Ok(Some(Err(e))) => return Err(format!("read error: {e}")),
                    Ok(Some(Ok(m))) => m,
                };
                match msg {
                    Message::Text(text) => {
                        if let Some(reply) = handle_message(inner, config.executor_metadata.as_ref(), &text).await {
                            if sink.send(Message::Text(reply)).await.is_err() {
                                return Err("response write failed".into());
                            }
                        }
                    }
                    Message::Ping(p) => {
                        let _ = sink.send(Message::Pong(p)).await;
                    }
                    Message::Close(_) => return Err("server closed".into()),
                    _ => {} // Pong / Binary: ignore
                }
            }
        }
    }
}

/// Dispatch one request message and produce a JSON response string (or `None` if unparseable).
async fn handle_message(
    inner: &Arc<DbosInner>,
    executor_metadata: Option<&serde_json::Map<String, Value>>,
    text: &str,
) -> Option<String> {
    let msg: Value = serde_json::from_str(text).ok()?;
    let msg_type = msg
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let request_id = msg
        .get("request_id")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let mut resp = json!({ "type": msg_type, "request_id": request_id });

    match msg_type.as_str() {
        "executor_info" => {
            resp["executor_id"] = json!(inner.executor_id);
            resp["application_version"] = json!(inner.application_version);
            resp["language"] = json!("rust");
            resp["dbos_version"] = json!(DBOS_VERSION);
            if let Ok(h) = std::env::var("HOSTNAME") {
                resp["hostname"] = json!(h);
            }
            if let Some(meta) = executor_metadata {
                resp["executor_metadata"] = Value::Object(meta.clone());
            }
        }
        "recovery" => {
            let execs = string_list(msg.get("executor_ids"));
            let r = recover_pending_workflows(inner.clone(), &execs)
                .await
                .map(|_| ());
            set_success(&mut resp, r);
        }
        "cancel" => {
            let ids = coalesce_ids(&msg);
            let r = management::cancel_workflows(&inner.pool, &inner.schema, &ids)
                .await
                .map(|_| ());
            set_success(&mut resp, r);
        }
        "resume" => {
            let ids = coalesce_ids(&msg);
            let queue = msg
                .get("queue_name")
                .and_then(Value::as_str)
                .unwrap_or(INTERNAL_QUEUE_NAME);
            let r = management::resume_workflows(&inner.pool, &inner.schema, &ids, queue)
                .await
                .map(|_| ());
            set_success(&mut resp, r);
        }
        "list_workflows" => {
            let filter = parse_list_filter(msg.get("body"), false);
            set_output(&mut resp, list_to_wire(inner, filter).await);
        }
        "list_queued_workflows" => {
            let filter = parse_list_filter(msg.get("body"), true);
            set_output(&mut resp, list_to_wire(inner, filter).await);
        }
        "get_workflow" => {
            let id = msg
                .get("workflow_id")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let filter = ListWorkflowsFilter {
                workflow_ids: Some(vec![id]),
                ..Default::default()
            };
            match management::list_workflows(&inner.pool, &inner.schema, filter).await {
                Ok(wfs) => {
                    resp["output"] = wfs.first().map(workflow_to_wire).unwrap_or(Value::Null);
                }
                Err(e) => resp["error_message"] = json!(e.to_string()),
            }
        }
        "list_steps" => {
            let id = msg.get("workflow_id").and_then(Value::as_str).unwrap_or("");
            match management::get_workflow_steps(&inner.pool, &inner.schema, id).await {
                Ok(steps) => {
                    resp["output"] = json!(steps.iter().map(step_to_wire).collect::<Vec<_>>());
                }
                Err(e) => resp["error_message"] = json!(e.to_string()),
            }
        }
        "fork_workflow" => {
            let body = msg.get("body").cloned().unwrap_or(Value::Null);
            let input = ForkWorkflowInput {
                original_workflow_id: body
                    .get("workflow_id")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                start_step: body.get("start_step").and_then(Value::as_i64).unwrap_or(0) as i32,
                forked_workflow_id: body
                    .get("new_workflow_id")
                    .and_then(Value::as_str)
                    .map(String::from),
                application_version: body
                    .get("application_version")
                    .and_then(Value::as_str)
                    .map(String::from),
                queue_name: body
                    .get("queue_name")
                    .and_then(Value::as_str)
                    .map(String::from),
                queue_partition_key: body
                    .get("queue_partition_key")
                    .and_then(Value::as_str)
                    .map(String::from),
            };
            match management::fork_workflow(&inner.pool, &inner.schema, input).await {
                Ok(new_id) => resp["new_workflow_id"] = json!(new_id),
                Err(e) => resp["error_message"] = json!(e.to_string()),
            }
        }
        "exist_pending_workflows" => {
            let exec = msg
                .get("executor_id")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let ver = msg
                .get("application_version")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let filter = ListWorkflowsFilter {
                status: Some(vec![WorkflowStatusType::Pending]),
                executor_ids: Some(vec![exec]),
                application_version: Some(ver),
                limit: Some(1),
                ..Default::default()
            };
            match management::list_workflows(&inner.pool, &inner.schema, filter).await {
                Ok(wfs) => resp["exist"] = json!(!wfs.is_empty()),
                Err(e) => resp["error_message"] = json!(e.to_string()),
            }
        }
        "retention" => {
            let body = msg.get("body").cloned().unwrap_or(Value::Null);
            let gc_cutoff = body.get("gc_cutoff_epoch_ms").and_then(Value::as_i64);
            let gc_rows = body.get("gc_rows_threshold").and_then(Value::as_i64);
            let timeout_cutoff = body.get("timeout_cutoff_epoch_ms").and_then(Value::as_i64);
            let mut r: Result<()> = Ok(());
            if gc_cutoff.is_some() || gc_rows.is_some() {
                r = management::garbage_collect(&inner.pool, &inner.schema, gc_cutoff, gc_rows)
                    .await;
            }
            if r.is_ok() {
                if let Some(tc) = timeout_cutoff {
                    r = management::cancel_all_before(&inner.pool, &inner.schema, tc).await;
                }
            }
            set_success(&mut resp, r);
        }
        "delete" => {
            let ids = coalesce_ids(&msg);
            let r = management::delete_workflows(&inner.pool, &inner.schema, &ids).await;
            set_success(&mut resp, r);
        }
        _ => {
            resp["error_message"] = json!("Unknown message type");
        }
    }

    serde_json::to_string(&resp).ok()
}

async fn list_to_wire(inner: &Arc<DbosInner>, filter: ListWorkflowsFilter) -> Result<Value> {
    let wfs = management::list_workflows(&inner.pool, &inner.schema, filter).await?;
    Ok(json!(wfs.iter().map(workflow_to_wire).collect::<Vec<_>>()))
}

fn set_output(resp: &mut Value, r: Result<Value>) {
    match r {
        Ok(v) => resp["output"] = v,
        Err(e) => resp["error_message"] = json!(e.to_string()),
    }
}

fn set_success(resp: &mut Value, r: Result<()>) {
    match r {
        Ok(()) => resp["success"] = json!(true),
        Err(e) => {
            resp["success"] = json!(false);
            resp["error_message"] = json!(e.to_string());
        }
    }
}

// ---- request parsing helpers ------------------------------------------------------------------

/// A field that may be a single string or an array of strings (`stringOrList`).
fn string_list(v: Option<&Value>) -> Vec<String> {
    match v {
        Some(Value::String(s)) => vec![s.clone()],
        Some(Value::Array(a)) => a
            .iter()
            .filter_map(|x| x.as_str().map(String::from))
            .collect(),
        _ => Vec::new(),
    }
}

fn array_or_none(v: Option<&Value>) -> Option<Vec<String>> {
    let list = string_list(v);
    if list.is_empty() {
        None
    } else {
        Some(list)
    }
}

fn first_string(v: Option<&Value>) -> Option<String> {
    string_list(v).into_iter().next()
}

fn status_list(v: Option<&Value>) -> Option<Vec<WorkflowStatusType>> {
    let names = string_list(v);
    if names.is_empty() {
        return None;
    }
    Some(
        names
            .iter()
            .filter_map(|s| WorkflowStatusType::from_str(s))
            .collect(),
    )
}

fn iso_to_ms(v: Option<&Value>) -> Option<i64> {
    let s = v?.as_str()?;
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.timestamp_millis())
}

/// `workflow_ids` if non-empty, else `[workflow_id]`.
fn coalesce_ids(msg: &Value) -> Vec<String> {
    match array_or_none(msg.get("workflow_ids")) {
        Some(v) => v,
        None => msg
            .get("workflow_id")
            .and_then(Value::as_str)
            .map(|s| vec![s.to_string()])
            .unwrap_or_default(),
    }
}

fn parse_list_filter(body: Option<&Value>, queues_only: bool) -> ListWorkflowsFilter {
    let b = body.cloned().unwrap_or(Value::Null);
    ListWorkflowsFilter {
        workflow_ids: array_or_none(b.get("workflow_uuids")),
        status: status_list(b.get("status")),
        workflow_name: first_string(b.get("workflow_name")),
        queue_name: first_string(b.get("queue_name")),
        queues_only: queues_only
            || b.get("queues_only")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        application_version: first_string(b.get("application_version")),
        executor_ids: array_or_none(b.get("executor_id")),
        start_time: iso_to_ms(b.get("start_time")),
        end_time: iso_to_ms(b.get("end_time")),
        limit: b.get("limit").and_then(Value::as_i64),
        offset: b.get("offset").and_then(Value::as_i64),
        sort_desc: b.get("sort_desc").and_then(Value::as_bool).unwrap_or(false),
    }
}

// ---- response wire shapes ---------------------------------------------------------------------

/// The conductor's PascalCase workflow body, with epoch-ms timestamps as decimal strings and
/// input/output as already-serialized JSON strings.
fn workflow_to_wire(w: &WorkflowStatus) -> Value {
    let mut o = serde_json::Map::new();
    o.insert("WorkflowUUID".into(), json!(w.id));
    o.insert("Status".into(), json!(w.status.as_str()));
    o.insert("WorkflowName".into(), json!(w.name));
    o.insert("CreatedAt".into(), json!(w.created_at.to_string()));
    o.insert("UpdatedAt".into(), json!(w.updated_at.to_string()));
    o.insert("Priority".into(), json!(w.priority.to_string()));
    o.insert("WasForkedFrom".into(), json!(w.was_forked_from));
    let insert_opt = |o: &mut serde_json::Map<String, Value>, k: &str, v: Option<&String>| {
        if let Some(v) = v {
            o.insert(k.into(), json!(v));
        }
    };
    insert_opt(&mut o, "QueueName", w.queue_name.as_ref());
    insert_opt(&mut o, "ExecutorID", w.executor_id.as_ref());
    insert_opt(&mut o, "ApplicationVersion", w.application_version.as_ref());
    insert_opt(&mut o, "ParentWorkflowID", w.parent_workflow_id.as_ref());
    insert_opt(&mut o, "ForkedFrom", w.forked_from.as_ref());
    insert_opt(&mut o, "DeduplicationID", w.deduplication_id.as_ref());
    insert_opt(&mut o, "WorkflowConfigName", w.config_name.as_ref());
    insert_opt(&mut o, "Error", w.error.as_ref());
    if let Some(c) = w.completed_at {
        o.insert("CompletedAt".into(), json!(c.to_string()));
    }
    if let Some(i) = &w.input {
        o.insert("Input".into(), json!(i.to_string()));
    }
    if let Some(out) = &w.output {
        o.insert("Output".into(), json!(out.to_string()));
    }
    // DequeuedAt is the started time, only meaningful while the workflow is PENDING.
    if w.status == WorkflowStatusType::Pending {
        if let Some(s) = w.started_at {
            o.insert("DequeuedAt".into(), json!(s.to_string()));
        }
    }
    Value::Object(o)
}

fn step_to_wire(s: &StepInfo) -> Value {
    let mut o = serde_json::Map::new();
    o.insert("function_id".into(), json!(s.step_id));
    o.insert("function_name".into(), json!(s.step_name));
    if let Some(out) = &s.output {
        o.insert("output".into(), json!(out.to_string()));
    }
    if let Some(e) = &s.error {
        o.insert("error".into(), json!(e));
    }
    if let Some(c) = &s.child_workflow_id {
        o.insert("child_workflow_id".into(), json!(c));
    }
    if let Some(t) = s.started_at {
        o.insert("started_at_epoch_ms".into(), json!(t.to_string()));
    }
    if let Some(t) = s.completed_at {
        o.insert("completed_at_epoch_ms".into(), json!(t.to_string()));
    }
    Value::Object(o)
}
