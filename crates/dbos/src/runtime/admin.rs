//! The admin HTTP server: a small axum app (default port 3001) exposing health, recovery, and
//! workflow-management endpoints for orchestrators / the DBOS console. Ports Go `admin_server.go`.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;

use super::recovery::recover_pending_workflows;
use super::DbosInner;
use super::WorkflowQueue;
use crate::db::management::{
    self, ForkWorkflowInput, ListWorkflowsFilter, StepInfo, WorkflowStatus, INTERNAL_QUEUE_NAME,
};
use crate::db::status::WorkflowStatusType;

type S = State<Arc<DbosInner>>;

/// Bind and serve the admin app until `token` is cancelled.
pub(crate) async fn run_admin_server(inner: Arc<DbosInner>, port: u16, token: CancellationToken) {
    let app = router(inner);
    let addr = format!("0.0.0.0:{port}");
    let listener = match tokio::net::TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!(error = %e, %addr, "admin server failed to bind");
            return;
        }
    };
    tracing::info!(%addr, "DBOS admin server listening");
    let _ = axum::serve(listener, app)
        .with_graceful_shutdown(async move { token.cancelled().await })
        .await;
}

fn router(inner: Arc<DbosInner>) -> Router {
    Router::new()
        .route("/dbos-healthz", get(healthz))
        .route("/dbos-workflow-recovery", post(recovery))
        .route("/deactivate", get(deactivate))
        .route("/conductor", get(conductor_probe))
        .route("/dbos-workflow-queues-metadata", get(queues_metadata))
        .route("/dbos-garbage-collect", post(garbage_collect))
        .route("/dbos-global-timeout", post(global_timeout))
        .route("/workflows", post(list_workflows))
        .route("/workflows/:id", get(get_workflow))
        .route("/workflows/:id/steps", get(list_steps))
        .route("/workflows/:id/cancel", post(cancel))
        .route("/workflows/:id/resume", post(resume))
        .route("/workflows/:id/fork", post(fork))
        .with_state(inner)
}

async fn healthz() -> impl IntoResponse {
    Json(json!({ "status": "healthy" }))
}

async fn conductor_probe() -> impl IntoResponse {
    Json(json!({ "status": true }))
}

async fn deactivate(State(inner): S) -> impl IntoResponse {
    if !inner.deactivated.swap(true, Ordering::SeqCst) {
        inner.scheduler_cancel.cancel();
        tracing::info!("DBOS deactivated: stopping schedulers");
    }
    (StatusCode::OK, "deactivated")
}

async fn recovery(State(inner): S, body: String) -> impl IntoResponse {
    let executor_ids: Vec<String> = match serde_json::from_str(&body) {
        Ok(v) => v,
        Err(_) => {
            return (StatusCode::BAD_REQUEST, "Invalid JSON body".to_string()).into_response()
        }
    };
    match recover_pending_workflows(inner.clone(), &executor_ids).await {
        Ok(ids) => (StatusCode::OK, Json(json!(ids))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Recovery failed: {e}"),
        )
            .into_response(),
    }
}

async fn queues_metadata(State(inner): S) -> impl IntoResponse {
    let arr: Vec<Value> = inner.queues.values().map(queue_to_wire).collect();
    Json(json!(arr))
}

#[derive(serde::Deserialize, Default)]
struct RetentionReq {
    cutoff_epoch_timestamp_ms: Option<i64>,
    rows_threshold: Option<i64>,
}

async fn garbage_collect(State(inner): S, body: String) -> impl IntoResponse {
    let req: RetentionReq = match parse_or_default(&body) {
        Ok(r) => r,
        Err(_) => {
            return (StatusCode::BAD_REQUEST, "Invalid JSON body".to_string()).into_response()
        }
    };
    match management::garbage_collect(
        &inner.pool,
        &inner.schema,
        req.cutoff_epoch_timestamp_ms,
        req.rows_threshold,
    )
    .await
    {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("GC failed: {e}")).into_response(),
    }
}

#[derive(serde::Deserialize)]
struct GlobalTimeoutReq {
    cutoff_epoch_timestamp_ms: i64,
}

async fn global_timeout(State(inner): S, body: String) -> impl IntoResponse {
    let req: GlobalTimeoutReq = match serde_json::from_str(&body) {
        Ok(r) => r,
        Err(_) => {
            return (StatusCode::BAD_REQUEST, "Invalid JSON body".to_string()).into_response()
        }
    };
    match management::cancel_all_before(&inner.pool, &inner.schema, req.cutoff_epoch_timestamp_ms)
        .await
    {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Global timeout failed: {e}"),
        )
            .into_response(),
    }
}

async fn list_workflows(State(inner): S, body: String) -> impl IntoResponse {
    let filter = match parse_filter(&body, false) {
        Ok(f) => f,
        Err(_) => {
            return (StatusCode::BAD_REQUEST, "Invalid JSON input".to_string()).into_response()
        }
    };
    match management::list_workflows(&inner.pool, &inner.schema, filter).await {
        Ok(wfs) => {
            Json(json!(wfs.iter().map(workflow_to_wire).collect::<Vec<_>>())).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn get_workflow(State(inner): S, Path(id): Path<String>) -> impl IntoResponse {
    let filter = ListWorkflowsFilter {
        workflow_ids: Some(vec![id]),
        ..Default::default()
    };
    match management::list_workflows(&inner.pool, &inner.schema, filter).await {
        Ok(wfs) => match wfs.first() {
            Some(w) => Json(workflow_to_wire(w)).into_response(),
            None => (StatusCode::NOT_FOUND, "Workflow not found".to_string()).into_response(),
        },
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn list_steps(State(inner): S, Path(id): Path<String>) -> impl IntoResponse {
    match management::get_workflow_steps(&inner.pool, &inner.schema, &id).await {
        Ok(steps) => {
            Json(json!(steps.iter().map(step_to_wire).collect::<Vec<_>>())).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn cancel(State(inner): S, Path(id): Path<String>) -> impl IntoResponse {
    match management::cancel_workflows(&inner.pool, &inner.schema, &[id]).await {
        Ok(_) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn resume(State(inner): S, Path(id): Path<String>) -> impl IntoResponse {
    match management::resume_workflows(&inner.pool, &inner.schema, &[id], INTERNAL_QUEUE_NAME).await
    {
        Ok(_) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

#[derive(serde::Deserialize, Default)]
struct ForkReq {
    start_step: Option<i32>,
    new_workflow_id: Option<String>,
    application_version: Option<String>,
}

async fn fork(State(inner): S, Path(id): Path<String>, body: String) -> impl IntoResponse {
    let req: ForkReq = match parse_or_default(&body) {
        Ok(r) => r,
        Err(_) => {
            return (StatusCode::BAD_REQUEST, "Invalid JSON input".to_string()).into_response()
        }
    };
    let input = ForkWorkflowInput {
        original_workflow_id: id,
        start_step: req.start_step.unwrap_or(0),
        forked_workflow_id: req.new_workflow_id,
        application_version: req.application_version,
        ..Default::default()
    };
    match management::fork_workflow(&inner.pool, &inner.schema, input).await {
        Ok(new_id) => Json(json!({ "workflow_id": new_id })).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

// ---- helpers ----------------------------------------------------------------------------------

/// Parse a body, treating an empty body as the type's default.
fn parse_or_default<T: serde::de::DeserializeOwned + Default>(body: &str) -> Result<T, ()> {
    if body.trim().is_empty() {
        Ok(T::default())
    } else {
        serde_json::from_str(body).map_err(|_| ())
    }
}

fn parse_filter(body: &str, queues_only: bool) -> Result<ListWorkflowsFilter, ()> {
    let v: Value = if body.trim().is_empty() {
        json!({})
    } else {
        serde_json::from_str(body).map_err(|_| ())?
    };
    let iso = |k: &str| {
        v.get(k)
            .and_then(Value::as_str)
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
            .map(|d| d.timestamp_millis())
    };
    let status = v
        .get("status")
        .and_then(Value::as_str)
        .and_then(WorkflowStatusType::from_str)
        .map(|s| vec![s]);
    let status = if queues_only && status.is_none() {
        Some(vec![
            WorkflowStatusType::Enqueued,
            WorkflowStatusType::Pending,
            WorkflowStatusType::Delayed,
        ])
    } else {
        status
    };
    Ok(ListWorkflowsFilter {
        workflow_ids: v.get("workflow_uuids").and_then(Value::as_array).map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(String::from))
                .collect()
        }),
        status,
        workflow_name: v
            .get("workflow_name")
            .and_then(Value::as_str)
            .map(String::from),
        queue_name: v
            .get("queue_name")
            .and_then(Value::as_str)
            .map(String::from),
        queues_only,
        application_version: v
            .get("application_version")
            .and_then(Value::as_str)
            .map(String::from),
        executor_ids: None,
        start_time: iso("start_time"),
        end_time: iso("end_time"),
        limit: v.get("limit").and_then(Value::as_i64),
        offset: v.get("offset").and_then(Value::as_i64),
        sort_desc: v.get("sort_desc").and_then(Value::as_bool).unwrap_or(false),
    })
}

fn queue_to_wire(q: &WorkflowQueue) -> Value {
    let mut o = serde_json::Map::new();
    o.insert("name".into(), json!(q.name));
    if let Some(c) = q.global_concurrency {
        o.insert("concurrency".into(), json!(c));
    }
    if let Some(w) = q.worker_concurrency {
        o.insert("workerConcurrency".into(), json!(w));
    }
    if q.priority_enabled {
        o.insert("priorityEnabled".into(), json!(true));
    }
    if let Some(r) = &q.rate_limit {
        o.insert(
            "rateLimit".into(),
            json!({ "Limit": r.limit, "Period": r.period.as_secs_f64() }),
        );
    }
    o.insert(
        "maxTasksPerIteration".into(),
        json!(q.max_tasks_per_iteration),
    );
    if q.partition_queue {
        o.insert("partitionQueue".into(), json!(true));
    }
    Value::Object(o)
}

/// PascalCase workflow body with epoch-ms timestamps as strings and input/output as JSON strings —
/// the shape the DBOS console expects from the HTTP admin server.
fn workflow_to_wire(w: &WorkflowStatus) -> Value {
    let mut o = serde_json::Map::new();
    o.insert("WorkflowUUID".into(), json!(w.id));
    o.insert("Status".into(), json!(w.status.as_str()));
    o.insert("WorkflowName".into(), json!(w.name));
    o.insert("Attempts".into(), json!(w.recovery_attempts));
    o.insert("Priority".into(), json!(w.priority));
    o.insert("CreatedAt".into(), json!(w.created_at.to_string()));
    o.insert("UpdatedAt".into(), json!(w.updated_at.to_string()));
    let put = |o: &mut serde_json::Map<String, Value>, k: &str, v: Option<&String>| {
        if let Some(v) = v {
            o.insert(k.into(), json!(v));
        }
    };
    put(&mut o, "QueueName", w.queue_name.as_ref());
    put(&mut o, "ExecutorID", w.executor_id.as_ref());
    put(&mut o, "ApplicationVersion", w.application_version.as_ref());
    put(&mut o, "DeduplicationID", w.deduplication_id.as_ref());
    put(&mut o, "ParentWorkflowID", w.parent_workflow_id.as_ref());
    // Input / Output as already-serialized JSON strings (e.g. 42 -> "42").
    o.insert(
        "Input".into(),
        json!(w.input.as_ref().map(|v| v.to_string()).unwrap_or_default()),
    );
    o.insert(
        "Output".into(),
        json!(w.output.as_ref().map(|v| v.to_string()).unwrap_or_default()),
    );
    o.insert(
        "Error".into(),
        json!(w
            .error
            .as_ref()
            .map(|e| serde_json::to_string(e).unwrap_or_default())
            .unwrap_or_default()),
    );
    if let Some(c) = w.completed_at {
        o.insert("CompletedAt".into(), json!(c.to_string()));
    }
    if w.status == WorkflowStatusType::Pending {
        if let Some(s) = w.started_at {
            o.insert("StartedAt".into(), json!(s.to_string()));
        }
    }
    Value::Object(o)
}

/// snake_case step body with epoch-ms timestamps as numbers (present only when set).
fn step_to_wire(s: &StepInfo) -> Value {
    let mut o = serde_json::Map::new();
    o.insert("function_id".into(), json!(s.step_id));
    o.insert("function_name".into(), json!(s.step_name));
    o.insert(
        "output".into(),
        json!(s.output.as_ref().map(|v| v.to_string()).unwrap_or_default()),
    );
    if let Some(c) = &s.child_workflow_id {
        o.insert("child_workflow_id".into(), json!(c));
    }
    if let Some(t) = s.started_at {
        o.insert("started_at_epoch_ms".into(), json!(t));
    }
    if let Some(t) = s.completed_at {
        o.insert("completed_at_epoch_ms".into(), json!(t));
    }
    if let Some(e) = &s.error {
        o.insert(
            "error".into(),
            json!(serde_json::to_string(e).unwrap_or_default()),
        );
    }
    Value::Object(o)
}
