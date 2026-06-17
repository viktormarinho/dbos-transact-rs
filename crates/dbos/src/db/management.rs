//! Workflow-management operations: list, steps, cancel, resume, fork, garbage-collect. Ports the
//! Go `listWorkflows` / `getWorkflowSteps` / `cancelWorkflows` / `resumeWorkflows` /
//! `forkWorkflow` / `garbageCollectWorkflows`.

use sqlx::{PgPool, Postgres, QueryBuilder, Row};

use super::status::WorkflowStatusType;
use super::{now_epoch_ms, schema_prefix};
use crate::error::{DbosError, Result};
use crate::serialize::{decode_input, decode_value};

/// The default queue resumed workflows are placed on.
pub const INTERNAL_QUEUE_NAME: &str = "_dbos_internal_queue";

/// A workflow's status row (as returned by [`list_workflows`]).
#[derive(Debug, Clone)]
pub struct WorkflowStatus {
    pub id: String,
    pub status: WorkflowStatusType,
    pub name: String,
    pub queue_name: Option<String>,
    pub executor_id: Option<String>,
    pub application_version: Option<String>,
    pub recovery_attempts: i64,
    pub created_at: i64,
    pub updated_at: i64,
    pub completed_at: Option<i64>,
    pub started_at: Option<i64>,
    pub priority: i32,
    pub deduplication_id: Option<String>,
    pub parent_workflow_id: Option<String>,
    pub forked_from: Option<String>,
    pub was_forked_from: bool,
    pub config_name: Option<String>,
    /// The workflow input, decoded (the first positional argument).
    pub input: Option<serde_json::Value>,
    /// The workflow output, decoded (only set for successful workflows).
    pub output: Option<serde_json::Value>,
    /// The workflow error message (only set for failed workflows).
    pub error: Option<String>,
}

/// A recorded step (as returned by [`get_workflow_steps`]).
#[derive(Debug, Clone)]
pub struct StepInfo {
    pub step_id: i32,
    pub step_name: String,
    pub output: Option<serde_json::Value>,
    pub error: Option<String>,
    pub child_workflow_id: Option<String>,
    pub started_at: Option<i64>,
    pub completed_at: Option<i64>,
}

/// Filters for [`list_workflows`]. All set filters are AND-ed.
#[derive(Debug, Default, Clone)]
pub struct ListWorkflowsFilter {
    pub workflow_ids: Option<Vec<String>>,
    pub status: Option<Vec<WorkflowStatusType>>,
    pub workflow_name: Option<String>,
    pub queue_name: Option<String>,
    /// Only workflows that are on a queue.
    pub queues_only: bool,
    pub application_version: Option<String>,
    /// `created_at >= start_time` (epoch ms).
    pub start_time: Option<i64>,
    /// `created_at <= end_time` (epoch ms).
    pub end_time: Option<i64>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
    /// Sort by `created_at` descending instead of ascending.
    pub sort_desc: bool,
}

/// Input to [`fork_workflow`].
#[derive(Debug, Clone, Default)]
pub struct ForkWorkflowInput {
    pub original_workflow_id: String,
    pub forked_workflow_id: Option<String>,
    /// Steps with `function_id < start_step` are copied; the fork resumes from `start_step`.
    pub start_step: i32,
    pub application_version: Option<String>,
    pub queue_name: Option<String>,
    pub queue_partition_key: Option<String>,
}

fn decode_value_opt(s: Option<&str>, ser: Option<&str>) -> Option<serde_json::Value> {
    s?;
    decode_value::<serde_json::Value>(s, ser).ok()
}

fn decode_input_opt(s: Option<&str>, ser: Option<&str>) -> Option<serde_json::Value> {
    s?;
    decode_input::<serde_json::Value>(s, ser).ok()
}

pub async fn list_workflows(
    pool: &PgPool,
    schema: &str,
    filter: ListWorkflowsFilter,
) -> Result<Vec<WorkflowStatus>> {
    let prefix = schema_prefix(schema);

    // `queues_only` with no status defaults to the live queued states.
    let status = match (filter.queues_only, &filter.status) {
        (true, None) => Some(vec![
            WorkflowStatusType::Enqueued,
            WorkflowStatusType::Pending,
            WorkflowStatusType::Delayed,
        ]),
        _ => filter.status.clone(),
    };

    let mut qb = QueryBuilder::<Postgres>::new(format!(
        "SELECT workflow_uuid, status, name, queue_name, executor_id, application_version,
                recovery_attempts, created_at, updated_at, completed_at, started_at_epoch_ms,
                priority, deduplication_id, parent_workflow_id, forked_from, was_forked_from,
                config_name, serialization, inputs, output, error
         FROM {prefix}workflow_status"
    ));

    let mut started = false;
    let mut connect = |qb: &mut QueryBuilder<Postgres>| {
        qb.push(if started { " AND " } else { " WHERE " });
        started = true;
    };

    if let Some(ids) = &filter.workflow_ids {
        connect(&mut qb);
        qb.push("workflow_uuid = ANY(").push_bind(ids.clone()).push(")");
    }
    if let Some(st) = &status {
        if !st.is_empty() {
            let strs: Vec<String> = st.iter().map(|s| s.as_str().to_string()).collect();
            connect(&mut qb);
            qb.push("status = ANY(").push_bind(strs).push(")");
        }
    }
    if let Some(name) = &filter.workflow_name {
        connect(&mut qb);
        qb.push("name = ").push_bind(name.clone());
    }
    if filter.queues_only {
        connect(&mut qb);
        qb.push("queue_name IS NOT NULL");
    } else if let Some(q) = &filter.queue_name {
        connect(&mut qb);
        qb.push("queue_name = ").push_bind(q.clone());
    }
    if let Some(v) = &filter.application_version {
        connect(&mut qb);
        qb.push("application_version = ").push_bind(v.clone());
    }
    if let Some(t) = filter.start_time {
        connect(&mut qb);
        qb.push("created_at >= ").push_bind(t);
    }
    if let Some(t) = filter.end_time {
        connect(&mut qb);
        qb.push("created_at <= ").push_bind(t);
    }

    qb.push(if filter.sort_desc {
        " ORDER BY created_at DESC"
    } else {
        " ORDER BY created_at ASC"
    });
    if let Some(l) = filter.limit {
        qb.push(" LIMIT ").push_bind(l);
    }
    if let Some(o) = filter.offset {
        qb.push(" OFFSET ").push_bind(o);
    }

    let rows = qb.build().fetch_all(pool).await?;
    let mut out = Vec::with_capacity(rows.len());
    for r in rows {
        let serialization: Option<String> = r.try_get("serialization")?;
        let inputs: Option<String> = r.try_get("inputs")?;
        let output: Option<String> = r.try_get("output")?;
        out.push(WorkflowStatus {
            id: r.try_get("workflow_uuid")?,
            status: WorkflowStatusType::from_str(&r.try_get::<String, _>("status")?)
                .unwrap_or(WorkflowStatusType::Pending),
            name: r.try_get("name")?,
            queue_name: r.try_get("queue_name")?,
            executor_id: r.try_get("executor_id")?,
            application_version: r.try_get("application_version")?,
            recovery_attempts: r.try_get::<Option<i64>, _>("recovery_attempts")?.unwrap_or(0),
            created_at: r.try_get("created_at")?,
            updated_at: r.try_get("updated_at")?,
            completed_at: r.try_get("completed_at")?,
            started_at: r.try_get("started_at_epoch_ms")?,
            priority: r.try_get::<Option<i32>, _>("priority")?.unwrap_or(0),
            deduplication_id: r.try_get("deduplication_id")?,
            parent_workflow_id: r.try_get("parent_workflow_id")?,
            forked_from: r.try_get("forked_from")?,
            was_forked_from: r.try_get::<Option<bool>, _>("was_forked_from")?.unwrap_or(false),
            config_name: r.try_get("config_name")?,
            input: decode_input_opt(inputs.as_deref(), serialization.as_deref()),
            output: decode_value_opt(output.as_deref(), serialization.as_deref()),
            error: r.try_get("error")?,
        });
    }
    Ok(out)
}

type StepRow = (
    i32,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<i64>,
    Option<i64>,
    Option<String>,
);

pub async fn get_workflow_steps(
    pool: &PgPool,
    schema: &str,
    workflow_id: &str,
) -> Result<Vec<StepInfo>> {
    let prefix = schema_prefix(schema);
    let rows: Vec<StepRow> = sqlx::query_as(&format!(
        "SELECT function_id, function_name, output, error, child_workflow_id,
                started_at_epoch_ms, completed_at_epoch_ms, serialization
         FROM {prefix}operation_outputs WHERE workflow_uuid = $1 ORDER BY function_id ASC"
    ))
    .bind(workflow_id)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(
            |(step_id, step_name, output, error, child, started, completed, ser)| StepInfo {
                step_id,
                step_name,
                output: decode_value_opt(output.as_deref(), ser.as_deref()),
                error,
                child_workflow_id: child,
                started_at: started,
                completed_at: completed,
            },
        )
        .collect())
}

/// Cancel the given workflows (non-terminal ones move to `CANCELLED`). Returns the ids that exist.
pub async fn cancel_workflows(pool: &PgPool, schema: &str, ids: &[String]) -> Result<Vec<String>> {
    let prefix = schema_prefix(schema);
    let now = now_epoch_ms();
    let found: Vec<String> = sqlx::query_scalar(&format!(
        "WITH existing AS (
            SELECT workflow_uuid FROM {prefix}workflow_status WHERE workflow_uuid = ANY($3)
        ), updated AS (
            UPDATE {prefix}workflow_status
            SET status = $1, updated_at = $2, completed_at = $2, started_at_epoch_ms = NULL,
                queue_name = NULL, deduplication_id = NULL
            WHERE workflow_uuid = ANY($3) AND status NOT IN ($4, $5, $6)
            RETURNING workflow_uuid
        )
        SELECT workflow_uuid FROM existing"
    ))
    .bind(WorkflowStatusType::Cancelled.as_str())
    .bind(now)
    .bind(ids.to_vec())
    .bind(WorkflowStatusType::Success.as_str())
    .bind(WorkflowStatusType::Error.as_str())
    .bind(WorkflowStatusType::Cancelled.as_str())
    .fetch_all(pool)
    .await?;
    Ok(found)
}

/// Resume the given workflows: reset them to `ENQUEUED` on `queue_name` so a runner re-executes
/// them. Already-`SUCCESS`/`ERROR` workflows are left alone. Returns the ids that exist.
pub async fn resume_workflows(
    pool: &PgPool,
    schema: &str,
    ids: &[String],
    queue_name: &str,
) -> Result<Vec<String>> {
    let prefix = schema_prefix(schema);
    let now = now_epoch_ms();
    let found: Vec<String> = sqlx::query_scalar(&format!(
        "WITH existing AS (
            SELECT workflow_uuid FROM {prefix}workflow_status WHERE workflow_uuid = ANY($5)
        ), updated AS (
            UPDATE {prefix}workflow_status
            SET status = $1, queue_name = $2, recovery_attempts = $3,
                workflow_deadline_epoch_ms = NULL, deduplication_id = NULL,
                started_at_epoch_ms = NULL, updated_at = $4, completed_at = NULL
            WHERE workflow_uuid = ANY($5) AND status NOT IN ($6, $7)
            RETURNING workflow_uuid
        )
        SELECT workflow_uuid FROM existing"
    ))
    .bind(WorkflowStatusType::Enqueued.as_str())
    .bind(queue_name)
    .bind(0_i64)
    .bind(now)
    .bind(ids.to_vec())
    .bind(WorkflowStatusType::Success.as_str())
    .bind(WorkflowStatusType::Error.as_str())
    .fetch_all(pool)
    .await?;
    Ok(found)
}

/// Fork a workflow: create a new `ENQUEUED` workflow copying the original's input (and steps below
/// `start_step`), so it re-runs from `start_step`. Returns the new workflow id.
pub async fn fork_workflow(pool: &PgPool, schema: &str, input: ForkWorkflowInput) -> Result<String> {
    let prefix = schema_prefix(schema);
    let new_id = input
        .forked_workflow_id
        .clone()
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let queue_name = input.queue_name.as_deref().unwrap_or(INTERNAL_QUEUE_NAME);
    let now = now_epoch_ms();

    // Read the original's copyable fields: (name, authenticated_user, assumed_role,
    // authenticated_roles, application_id, inputs, serialization, class_name, config_name).
    type OriginalRow = (
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
    );
    let original: Option<OriginalRow> = sqlx::query_as(&format!(
        "SELECT name, authenticated_user, assumed_role, authenticated_roles, application_id,
                inputs, serialization, class_name, config_name
         FROM {prefix}workflow_status WHERE workflow_uuid = $1"
    ))
    .bind(&input.original_workflow_id)
    .fetch_optional(pool)
    .await?;
    let (name, auth_user, assumed_role, auth_roles, app_id, inputs, serialization, class_name, config_name) =
        original.ok_or_else(|| DbosError::non_existent_workflow(&input.original_workflow_id))?;
    let app_version = input.application_version.clone();

    let mut tx = pool.begin().await?;

    sqlx::query(&format!(
        "INSERT INTO {prefix}workflow_status (
            workflow_uuid, status, name, authenticated_user, assumed_role, authenticated_roles,
            application_version, application_id, queue_name, queue_partition_key, inputs,
            created_at, updated_at, recovery_attempts, forked_from, serialization, class_name,
            config_name
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$12,$13,$14,$15,$16,$17)"
    ))
    .bind(&new_id)
    .bind(WorkflowStatusType::Enqueued.as_str())
    .bind(&name)
    .bind(&auth_user)
    .bind(&assumed_role)
    .bind(&auth_roles)
    .bind(&app_version)
    .bind(&app_id)
    .bind(queue_name)
    .bind(&input.queue_partition_key)
    .bind(&inputs)
    .bind(now)
    .bind(0_i64)
    .bind(&input.original_workflow_id)
    .bind(&serialization)
    .bind(&class_name)
    .bind(&config_name)
    .execute(&mut *tx)
    .await?;

    sqlx::query(&format!(
        "UPDATE {prefix}workflow_status SET was_forked_from = TRUE WHERE workflow_uuid = $1"
    ))
    .bind(&input.original_workflow_id)
    .execute(&mut *tx)
    .await?;

    if input.start_step > 0 {
        // NOTE: unlike Go (whose native format is base64 DBOS_JSON, matching the NULL→DBOS_JSON
        // decode default), we MUST copy `serialization` — our default is portable_json, so a step
        // copied with a NULL serialization would be mis-decoded as base64 on replay.
        sqlx::query(&format!(
            "INSERT INTO {prefix}operation_outputs
                (workflow_uuid, function_id, output, error, function_name, child_workflow_id,
                 started_at_epoch_ms, completed_at_epoch_ms, serialization)
             SELECT $1, function_id, output, error, function_name, child_workflow_id,
                    started_at_epoch_ms, completed_at_epoch_ms, serialization
             FROM {prefix}operation_outputs WHERE workflow_uuid = $2 AND function_id < $3"
        ))
        .bind(&new_id)
        .bind(&input.original_workflow_id)
        .bind(input.start_step)
        .execute(&mut *tx)
        .await?;

        sqlx::query(&format!(
            "INSERT INTO {prefix}workflow_events_history (workflow_uuid, function_id, key, value, serialization)
             SELECT $1, function_id, key, value, serialization
             FROM {prefix}workflow_events_history WHERE workflow_uuid = $2 AND function_id < $3"
        ))
        .bind(&new_id)
        .bind(&input.original_workflow_id)
        .bind(input.start_step)
        .execute(&mut *tx)
        .await?;

        sqlx::query(&format!(
            "INSERT INTO {prefix}workflow_events (workflow_uuid, key, value, serialization)
             SELECT $1, h.key, h.value, h.serialization
             FROM {prefix}workflow_events_history h
             INNER JOIN (
                 SELECT key, MAX(function_id) AS max_fid
                 FROM {prefix}workflow_events_history
                 WHERE workflow_uuid = $2 AND function_id < $3 GROUP BY key
             ) latest ON h.key = latest.key AND h.function_id = latest.max_fid
             WHERE h.workflow_uuid = $2 AND h.function_id < $3"
        ))
        .bind(&new_id)
        .bind(&input.original_workflow_id)
        .bind(input.start_step)
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;
    Ok(new_id)
}

/// Delete terminal workflows older than the cutoff (or beyond the row threshold). Never deletes
/// `PENDING`/`ENQUEUED`/`DELAYED`. The effective cutoff is the more recent of the two bounds.
pub async fn garbage_collect(
    pool: &PgPool,
    schema: &str,
    cutoff_epoch_ms: Option<i64>,
    rows_threshold: Option<i64>,
) -> Result<()> {
    let prefix = schema_prefix(schema);
    let mut cutoff = cutoff_epoch_ms;

    if let Some(threshold) = rows_threshold {
        if threshold > 0 {
            let rows_cutoff: Option<i64> = sqlx::query_scalar(&format!(
                "SELECT created_at FROM {prefix}workflow_status
                 ORDER BY created_at DESC LIMIT 1 OFFSET $1"
            ))
            .bind(threshold - 1)
            .fetch_optional(pool)
            .await?;
            if let Some(rc) = rows_cutoff {
                cutoff = Some(cutoff.map_or(rc, |c| c.max(rc)));
            }
        }
    }

    let Some(cutoff) = cutoff else {
        return Ok(()); // nothing to do
    };

    sqlx::query(&format!(
        "DELETE FROM {prefix}workflow_status
         WHERE created_at < $1 AND status NOT IN ($2, $3, $4)"
    ))
    .bind(cutoff)
    .bind(WorkflowStatusType::Pending.as_str())
    .bind(WorkflowStatusType::Enqueued.as_str())
    .bind(WorkflowStatusType::Delayed.as_str())
    .execute(pool)
    .await?;
    Ok(())
}
