//! `workflow_status` operations: insert (with idempotency + dead-letter), outcome recording, and
//! result polling. Ports the Go `insertWorkflowStatus` / `updateWorkflowOutcome` /
//! `awaitWorkflowResult`.

use std::time::Duration;

use sqlx::PgPool;

use super::{now_epoch_ms, schema_prefix};
use crate::error::{DbosError, Result};

/// `RETURNING` columns of [`insert_workflow_status`]: (recovery_attempts, status, name, queue_name,
/// queue_partition_key, workflow_timeout_ms, workflow_deadline_epoch_ms, owner_xid).
type InsertReturningRow = (
    i64,
    String,
    String,
    Option<String>,
    Option<String>,
    Option<i64>,
    Option<i64>,
    String,
);

/// A row polled by [`await_workflow_result`]: (status, output, error, recovery_attempts, serialization).
type AwaitRow = (String, Option<String>, Option<String>, i64, Option<String>);

/// The `workflow_status.status` value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkflowStatusType {
    Pending,
    Enqueued,
    Delayed,
    Success,
    Error,
    Cancelled,
    MaxRecoveryAttemptsExceeded,
}

impl WorkflowStatusType {
    pub fn as_str(self) -> &'static str {
        match self {
            WorkflowStatusType::Pending => "PENDING",
            WorkflowStatusType::Enqueued => "ENQUEUED",
            WorkflowStatusType::Delayed => "DELAYED",
            WorkflowStatusType::Success => "SUCCESS",
            WorkflowStatusType::Error => "ERROR",
            WorkflowStatusType::Cancelled => "CANCELLED",
            WorkflowStatusType::MaxRecoveryAttemptsExceeded => "MAX_RECOVERY_ATTEMPTS_EXCEEDED",
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Option<Self> {
        Some(match s {
            "PENDING" => WorkflowStatusType::Pending,
            "ENQUEUED" => WorkflowStatusType::Enqueued,
            "DELAYED" => WorkflowStatusType::Delayed,
            "SUCCESS" => WorkflowStatusType::Success,
            "ERROR" => WorkflowStatusType::Error,
            "CANCELLED" => WorkflowStatusType::Cancelled,
            "MAX_RECOVERY_ATTEMPTS_EXCEEDED" => WorkflowStatusType::MaxRecoveryAttemptsExceeded,
            _ => return None,
        })
    }

    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            WorkflowStatusType::Success
                | WorkflowStatusType::Error
                | WorkflowStatusType::Cancelled
                | WorkflowStatusType::MaxRecoveryAttemptsExceeded
        )
    }
}

/// Fields written by [`insert_workflow_status`].
#[derive(Debug, Default)]
pub struct InsertWorkflowInput {
    pub workflow_id: String,
    pub status: String,
    pub name: String,
    pub queue_name: Option<String>,
    pub authenticated_user: Option<String>,
    pub assumed_role: Option<String>,
    pub authenticated_roles: Option<String>,
    pub executor_id: String,
    pub application_version: Option<String>,
    pub application_id: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    /// The `recovery_attempts` literal stored on a fresh insert: 1 normally, 0 for ENQUEUED/DELAYED.
    pub recovery_attempts: i64,
    pub workflow_timeout_ms: Option<i64>,
    pub workflow_deadline_epoch_ms: Option<i64>,
    pub inputs: Option<String>,
    pub deduplication_id: Option<String>,
    pub priority: i32,
    pub queue_partition_key: Option<String>,
    pub owner_xid: String,
    pub parent_workflow_id: Option<String>,
    pub class_name: Option<String>,
    pub config_name: Option<String>,
    pub serialization: Option<String>,
    pub delay_until_epoch_ms: Option<i64>,
    /// `recovery_attempts` increment applied on conflict (1 only for dequeue/recovery, else 0).
    pub increment: i64,
}

/// Outcome of [`insert_workflow_status`] — the (possibly pre-existing) row's state, used to decide
/// idempotency.
#[derive(Debug)]
pub struct InsertResult {
    pub recovery_attempts: i64,
    pub status: String,
    pub name: String,
    pub queue_name: Option<String>,
    pub owner_xid: String,
    /// True if this insert tripped the dead-letter threshold and moved the row to
    /// `MAX_RECOVERY_ATTEMPTS_EXCEEDED` (the caller should commit and surface a DLQ error).
    pub dlq_triggered: bool,
}

/// Insert (or, on conflict, touch) a workflow status row inside `tx`. Returns the resulting row
/// state. `max_retries` drives the dead-letter check (`attempts > max_retries + 1`).
pub async fn insert_workflow_status(
    tx: &mut sqlx::PgConnection,
    schema: &str,
    input: &InsertWorkflowInput,
    max_retries: i64,
) -> Result<InsertResult> {
    let prefix = schema_prefix(schema);
    let sql = format!(
        "INSERT INTO {prefix}workflow_status (
            workflow_uuid, status, name, queue_name, authenticated_user, assumed_role,
            authenticated_roles, executor_id, application_version, application_id, created_at,
            recovery_attempts, updated_at, workflow_timeout_ms, workflow_deadline_epoch_ms, inputs,
            deduplication_id, priority, queue_partition_key, owner_xid, parent_workflow_id,
            class_name, config_name, serialization, delay_until_epoch_ms
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25)
        ON CONFLICT (workflow_uuid) DO UPDATE SET
            recovery_attempts = CASE
                WHEN EXCLUDED.status NOT IN ($26, $27)
                    THEN workflow_status.recovery_attempts + $28
                ELSE workflow_status.recovery_attempts
            END,
            updated_at = EXCLUDED.updated_at,
            executor_id = CASE
                WHEN EXCLUDED.status IN ($26, $27) THEN workflow_status.executor_id
                ELSE EXCLUDED.executor_id
            END
        RETURNING recovery_attempts, status, name, queue_name, queue_partition_key,
                  workflow_timeout_ms, workflow_deadline_epoch_ms, owner_xid"
    );

    let row: InsertReturningRow = sqlx::query_as(&sql)
        .bind(&input.workflow_id)
        .bind(&input.status)
        .bind(&input.name)
        .bind(&input.queue_name)
        .bind(&input.authenticated_user)
        .bind(&input.assumed_role)
        .bind(&input.authenticated_roles)
        .bind(&input.executor_id)
        .bind(&input.application_version)
        .bind(&input.application_id)
        .bind(input.created_at)
        .bind(input.recovery_attempts)
        .bind(input.updated_at)
        .bind(input.workflow_timeout_ms)
        .bind(input.workflow_deadline_epoch_ms)
        .bind(&input.inputs)
        .bind(&input.deduplication_id)
        .bind(input.priority)
        .bind(&input.queue_partition_key)
        .bind(&input.owner_xid)
        .bind(&input.parent_workflow_id)
        .bind(&input.class_name)
        .bind(&input.config_name)
        .bind(&input.serialization)
        .bind(input.delay_until_epoch_ms)
        .bind(WorkflowStatusType::Enqueued.as_str())
        .bind(WorkflowStatusType::Delayed.as_str())
        .bind(input.increment)
        .fetch_one(&mut *tx)
        .await?;

    let (attempts, status, name, queue_name, _qpk, _timeout, _deadline, owner_xid) = row;

    // Same id, different workflow name → conflict.
    if !input.name.is_empty() && name != input.name {
        return Err(DbosError::conflicting_workflow(
            &input.workflow_id,
            format!("it already exists with a different name ({name})"),
        ));
    }
    // Same id, different queue → conflict.
    if let (Some(want), Some(have)) = (&input.queue_name, &queue_name) {
        if want != have {
            return Err(DbosError::conflicting_workflow(
                &input.workflow_id,
                format!("it already exists in a different queue ({have})"),
            ));
        }
    }

    // Dead-letter: a non-terminal row whose attempts exceed the limit moves to the DLQ.
    let mut dlq_triggered = false;
    if status != WorkflowStatusType::Success.as_str()
        && status != WorkflowStatusType::Error.as_str()
        && max_retries > 0
        && attempts > max_retries + 1
    {
        let upd = format!(
            "UPDATE {prefix}workflow_status
             SET status = $1, deduplication_id = NULL, started_at_epoch_ms = NULL, queue_name = NULL
             WHERE workflow_uuid = $2 AND status = $3"
        );
        sqlx::query(&upd)
            .bind(WorkflowStatusType::MaxRecoveryAttemptsExceeded.as_str())
            .bind(&input.workflow_id)
            .bind(WorkflowStatusType::Pending.as_str())
            .execute(&mut *tx)
            .await?;
        dlq_triggered = true;
    }

    Ok(InsertResult {
        recovery_attempts: attempts,
        status,
        name,
        queue_name,
        owner_xid,
        dlq_triggered,
    })
}

/// Record a workflow's terminal outcome. The `WHERE … AND NOT (status = CANCELLED AND new IN
/// (SUCCESS, ERROR))` guard preserves a cancellation that raced with normal completion.
pub async fn update_workflow_outcome(
    pool: &PgPool,
    schema: &str,
    workflow_id: &str,
    status: WorkflowStatusType,
    output: Option<&str>,
    error: Option<&str>,
) -> Result<()> {
    let prefix = schema_prefix(schema);
    let now = now_epoch_ms();
    let sql = format!(
        "UPDATE {prefix}workflow_status
         SET status = $1, output = $2, error = $3, updated_at = $4, completed_at = $4,
             deduplication_id = NULL
         WHERE workflow_uuid = $5 AND NOT (status = $6 AND CAST($1 AS TEXT) IN ($7, $8))"
    );
    sqlx::query(&sql)
        .bind(status.as_str())
        .bind(output)
        .bind(error)
        .bind(now)
        .bind(workflow_id)
        .bind(WorkflowStatusType::Cancelled.as_str())
        .bind(WorkflowStatusType::Success.as_str())
        .bind(WorkflowStatusType::Error.as_str())
        .execute(pool)
        .await?;
    Ok(())
}

/// Terminal outcome read by [`await_workflow_result`].
#[derive(Debug)]
pub enum AwaitOutcome {
    Success {
        output: Option<String>,
        serialization: Option<String>,
    },
    Error {
        error: Option<String>,
        serialization: Option<String>,
    },
    Cancelled,
    DeadLetter {
        recovery_attempts: i64,
    },
}

/// Poll `workflow_status` until `workflow_id` reaches a terminal state. Sleeps `poll_interval`
/// between checks (including when the row does not yet exist).
pub async fn await_workflow_result(
    pool: &PgPool,
    schema: &str,
    workflow_id: &str,
    poll_interval: Duration,
) -> Result<AwaitOutcome> {
    let prefix = schema_prefix(schema);
    let sql = format!(
        "SELECT status, output, error, recovery_attempts, serialization
         FROM {prefix}workflow_status WHERE workflow_uuid = $1"
    );
    loop {
        let row: Option<AwaitRow> = sqlx::query_as(&sql)
            .bind(workflow_id)
            .fetch_optional(pool)
            .await?;
        match row {
            None => tokio::time::sleep(poll_interval).await,
            Some((status, output, error, attempts, serialization)) => {
                match WorkflowStatusType::from_str(&status) {
                    Some(WorkflowStatusType::Success) => {
                        return Ok(AwaitOutcome::Success {
                            output,
                            serialization,
                        })
                    }
                    Some(WorkflowStatusType::Error) => {
                        return Ok(AwaitOutcome::Error {
                            error,
                            serialization,
                        })
                    }
                    Some(WorkflowStatusType::Cancelled) => return Ok(AwaitOutcome::Cancelled),
                    Some(WorkflowStatusType::MaxRecoveryAttemptsExceeded) => {
                        return Ok(AwaitOutcome::DeadLetter {
                            recovery_attempts: attempts,
                        })
                    }
                    // PENDING / ENQUEUED / DELAYED / unknown → keep waiting.
                    _ => tokio::time::sleep(poll_interval).await,
                }
            }
        }
    }
}

/// Read a workflow's current status string, if the row exists.
pub async fn get_status(pool: &PgPool, schema: &str, workflow_id: &str) -> Result<Option<String>> {
    let prefix = schema_prefix(schema);
    let status: Option<String> = sqlx::query_scalar(&format!(
        "SELECT status FROM {prefix}workflow_status WHERE workflow_uuid = $1"
    ))
    .bind(workflow_id)
    .fetch_optional(pool)
    .await?;
    Ok(status)
}
