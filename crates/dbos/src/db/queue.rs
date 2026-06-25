//! Durable-queue SQL: the dequeue algorithm, delayed→enqueued promotion, queue-assignment clearing
//! (for recovery), and partition discovery. Ports Go `dequeueWorkflows` /
//! `transitionDelayedWorkflows` / `clearQueueAssignment` / `getQueuePartitions`.

use std::time::Duration;

use sqlx::PgPool;

use super::status::WorkflowStatusType;
use super::{now_epoch_ms, schema_prefix};
use crate::error::Result;

/// Promote every `DELAYED` workflow whose delay has elapsed to `ENQUEUED`. Runs (autocommit) at the
/// top of each poll iteration. Matches Go: this is global (not per-queue).
pub async fn transition_delayed_workflows(pool: &PgPool, schema: &str) -> Result<()> {
    let prefix = schema_prefix(schema);
    sqlx::query(&format!(
        "UPDATE {prefix}workflow_status SET status = $1
         WHERE status = $2 AND delay_until_epoch_ms <= $3"
    ))
    .bind(WorkflowStatusType::Enqueued.as_str())
    .bind(WorkflowStatusType::Delayed.as_str())
    .bind(now_epoch_ms())
    .execute(pool)
    .await?;
    Ok(())
}

/// On recovery, push a queued workflow that was claimed (`PENDING`) but never finished back to
/// `ENQUEUED` so a runner reclaims it. Returns true if a row was reset.
pub async fn clear_queue_assignment(
    pool: &PgPool,
    schema: &str,
    workflow_id: &str,
) -> Result<bool> {
    let prefix = schema_prefix(schema);
    let affected = sqlx::query(&format!(
        "UPDATE {prefix}workflow_status
         SET status = $1, started_at_epoch_ms = NULL
         WHERE workflow_uuid = $2 AND queue_name IS NOT NULL AND status = $3"
    ))
    .bind(WorkflowStatusType::Enqueued.as_str())
    .bind(workflow_id)
    .bind(WorkflowStatusType::Pending.as_str())
    .execute(pool)
    .await?
    .rows_affected();
    Ok(affected > 0)
}

/// Distinct partition keys with `ENQUEUED` work, for a partitioned queue.
pub async fn get_queue_partitions(
    pool: &PgPool,
    schema: &str,
    queue_name: &str,
) -> Result<Vec<String>> {
    let prefix = schema_prefix(schema);
    let keys: Vec<String> = sqlx::query_scalar(&format!(
        "SELECT DISTINCT queue_partition_key FROM {prefix}workflow_status
         WHERE queue_name = $1 AND status = $2 AND queue_partition_key IS NOT NULL"
    ))
    .bind(queue_name)
    .bind(WorkflowStatusType::Enqueued.as_str())
    .fetch_all(pool)
    .await?;
    Ok(keys)
}

/// Inputs to one dequeue iteration for a (queue, partition).
pub struct DequeueInput<'a> {
    pub queue_name: &'a str,
    pub executor_id: &'a str,
    pub application_version: &'a str,
    pub partition_key: Option<&'a str>,
    /// In-memory count of this executor's running workflows for the (queue, partition).
    pub local_running_count: usize,
    pub worker_concurrency: Option<u32>,
    pub global_concurrency: Option<u32>,
    pub max_tasks_per_iteration: u32,
    pub rate_limit: Option<(u32, Duration)>,
}

/// A workflow claimed by a dequeue iteration (now `PENDING`, ready to run on this executor).
#[derive(Debug)]
pub struct DequeuedWorkflow {
    pub id: String,
    pub name: String,
    pub inputs: Option<String>,
    pub serialization: Option<String>,
    pub config_name: Option<String>,
}

type ClaimRow = (String, Option<String>, Option<String>, Option<String>);

/// Atomically claim up to `maxTasks` `ENQUEUED` workflows for the queue, flipping them to
/// `PENDING`. Enforces rate limiting and global/worker concurrency, and uses `SKIP LOCKED` (no
/// global concurrency) or `NOWAIT` (global concurrency) per the reference. Commits only if at least
/// one workflow was claimed.
pub async fn dequeue_workflows(
    pool: &PgPool,
    schema: &str,
    input: DequeueInput<'_>,
) -> Result<Vec<DequeuedWorkflow>> {
    let prefix = schema_prefix(schema);
    let enqueued = WorkflowStatusType::Enqueued.as_str();
    let delayed = WorkflowStatusType::Delayed.as_str();
    let pending = WorkflowStatusType::Pending.as_str();

    // Snapshot isolation is only needed when global concurrency or rate limiting must see a
    // consistent count; otherwise read-committed suffices (worker concurrency is in-memory).
    let snapshot = input.global_concurrency.is_some() || input.rate_limit.is_some();
    let mut tx = pool.begin().await?;
    if snapshot {
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .execute(&mut *tx)
            .await?;
    }

    // 1) Rate-limit gate: count workflows actually started within the trailing window.
    let mut num_recent: i64 = 0;
    if let Some((limit, period)) = input.rate_limit {
        let cutoff = now_epoch_ms() - period.as_millis() as i64;
        let sql = format!(
            "SELECT COUNT(*) FROM {prefix}workflow_status
             WHERE queue_name = $1 AND rate_limited = TRUE AND status NOT IN ($2, $3)
               AND started_at_epoch_ms > $4{}",
            partition_clause(input.partition_key.is_some(), 5)
        );
        let mut q = sqlx::query_scalar::<_, i64>(&sql)
            .bind(input.queue_name)
            .bind(enqueued)
            .bind(delayed)
            .bind(cutoff);
        if let Some(pk) = input.partition_key {
            q = q.bind(pk);
        }
        num_recent = q.fetch_one(&mut *tx).await?;
        if num_recent >= limit as i64 {
            return Ok(Vec::new());
        }
    }

    // 2) How many tasks may we claim this iteration.
    let mut max_tasks = input.max_tasks_per_iteration as i64;
    if let Some(wc) = input.worker_concurrency {
        max_tasks = (wc as i64 - input.local_running_count as i64).max(0);
    }
    if let Some(gc) = input.global_concurrency {
        let sql = format!(
            "SELECT COUNT(*) FROM {prefix}workflow_status
             WHERE queue_name = $1 AND status = $2{}",
            partition_clause(input.partition_key.is_some(), 3)
        );
        let mut q = sqlx::query_scalar::<_, i64>(&sql)
            .bind(input.queue_name)
            .bind(pending);
        if let Some(pk) = input.partition_key {
            q = q.bind(pk);
        }
        let global_count: i64 = q.fetch_one(&mut *tx).await?;
        let available = (gc as i64 - global_count).max(0);
        max_tasks = max_tasks.min(available);
    }
    if max_tasks <= 0 {
        return Ok(Vec::new());
    }

    // 3) Select candidate ids in priority/FIFO order, locking them.
    let lock = if input.global_concurrency.is_none() {
        "FOR UPDATE SKIP LOCKED"
    } else {
        "FOR UPDATE NOWAIT"
    };
    let select_sql = format!(
        "SELECT workflow_uuid FROM {prefix}workflow_status
         WHERE queue_name = $1 AND status = $2
           AND (application_version = $3 OR application_version IS NULL){}
         ORDER BY priority ASC, created_at ASC
         {lock} LIMIT {max_tasks}",
        partition_clause(input.partition_key.is_some(), 4)
    );
    let mut q = sqlx::query_scalar::<_, String>(&select_sql)
        .bind(input.queue_name)
        .bind(enqueued)
        .bind(input.application_version);
    if let Some(pk) = input.partition_key {
        q = q.bind(pk);
    }
    let candidate_ids: Vec<String> = q.fetch_all(&mut *tx).await?;

    // 4) Claim each: ENQUEUED -> PENDING, set executor/started_at/rate_limited, materialize deadline.
    let now = now_epoch_ms();
    let rate_limited = input.rate_limit.is_some();
    let claim_sql = format!(
        "UPDATE {prefix}workflow_status
         SET status = $1, application_version = $2, executor_id = $3, started_at_epoch_ms = $4,
             rate_limited = $5,
             workflow_deadline_epoch_ms = CASE
                 WHEN workflow_timeout_ms IS NOT NULL AND workflow_deadline_epoch_ms IS NULL
                 THEN $4 + workflow_timeout_ms ELSE workflow_deadline_epoch_ms END
         WHERE workflow_uuid = $6 AND status = $7
         RETURNING name, inputs, serialization, config_name"
    );
    let mut claimed = Vec::new();
    for id in candidate_ids {
        if let Some((limit, _)) = input.rate_limit {
            if claimed.len() as i64 + num_recent >= limit as i64 {
                break;
            }
        }
        let row: Option<ClaimRow> = sqlx::query_as(&claim_sql)
            .bind(pending)
            .bind(input.application_version)
            .bind(input.executor_id)
            .bind(now)
            .bind(rate_limited)
            .bind(&id)
            .bind(enqueued)
            .fetch_optional(&mut *tx)
            .await?;
        if let Some((name, inputs, serialization, config_name)) = row {
            claimed.push(DequeuedWorkflow {
                id,
                name,
                inputs,
                serialization,
                config_name,
            });
        }
        // A no-row result means another executor claimed it first — skip it.
    }

    if !claimed.is_empty() {
        tx.commit().await?;
    }
    // Empty iterations roll back (the tx is dropped), avoiding WAL bloat.
    Ok(claimed)
}

/// `" AND queue_partition_key = $N"` when partitioned, else empty.
fn partition_clause(partitioned: bool, n: usize) -> String {
    if partitioned {
        format!(" AND queue_partition_key = ${n}")
    } else {
        String::new()
    }
}
