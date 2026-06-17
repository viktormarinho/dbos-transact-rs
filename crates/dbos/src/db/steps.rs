//! `operation_outputs` operations: step memoization, the determinism check, and child-workflow
//! recording. Ports the Go `checkOperationExecution` / `recordOperationResult` /
//! `checkChildWorkflow` / `recordChildWorkflow`.

use sqlx::PgPool;

use super::{now_epoch_ms, schema_prefix};
use crate::error::{is_unique_violation, DbosError, Result};

/// A previously recorded step result (replay).
#[derive(Debug)]
pub struct RecordedOperation {
    pub output: Option<String>,
    pub error: Option<String>,
    pub serialization: Option<String>,
}

/// Look up a step's recorded result. Returns `None` if the step has not run yet (the caller should
/// execute it). Errors if the workflow does not exist, is cancelled, or a different step name was
/// recorded at this `function_id` (non-determinism).
pub async fn check_operation_execution(
    pool: &PgPool,
    schema: &str,
    workflow_id: &str,
    function_id: i32,
    expected_step_name: &str,
) -> Result<Option<RecordedOperation>> {
    let prefix = schema_prefix(schema);

    // Status guard: the workflow must exist and not be cancelled.
    let status: Option<String> = sqlx::query_scalar(&format!(
        "SELECT status FROM {prefix}workflow_status WHERE workflow_uuid = $1"
    ))
    .bind(workflow_id)
    .fetch_optional(pool)
    .await?;
    match status.as_deref() {
        None => return Err(DbosError::non_existent_workflow(workflow_id)),
        Some("CANCELLED") => return Err(DbosError::workflow_cancelled(workflow_id)),
        _ => {}
    }

    // (output, error, function_name, serialization)
    type OperationRow = (Option<String>, Option<String>, String, Option<String>);
    let row: Option<OperationRow> = sqlx::query_as(&format!(
        "SELECT output, error, function_name, serialization
         FROM {prefix}operation_outputs WHERE workflow_uuid = $1 AND function_id = $2"
    ))
    .bind(workflow_id)
    .bind(function_id)
    .fetch_optional(pool)
    .await?;

    match row {
        None => Ok(None),
        Some((output, error, recorded_name, serialization)) => {
            if recorded_name != expected_step_name {
                return Err(DbosError::unexpected_step(
                    workflow_id,
                    function_id,
                    expected_step_name,
                    recorded_name,
                ));
            }
            Ok(Some(RecordedOperation {
                output,
                error,
                serialization,
            }))
        }
    }
}

/// Record a completed operation (step result, or a child-workflow mapping). A unique-violation on
/// `(workflow_uuid, function_id)` means the same step id was recorded twice — a non-deterministic
/// re-execution.
#[allow(clippy::too_many_arguments)]
pub async fn record_operation_result<'e, E: sqlx::PgExecutor<'e>>(
    exec: E,
    schema: &str,
    workflow_id: &str,
    function_id: i32,
    function_name: &str,
    output: Option<&str>,
    error: Option<&str>,
    child_workflow_id: Option<&str>,
    serialization: Option<&str>,
) -> Result<()> {
    let prefix = schema_prefix(schema);
    let now = now_epoch_ms();
    let sql = format!(
        "INSERT INTO {prefix}operation_outputs
            (workflow_uuid, function_id, function_name, output, error, child_workflow_id,
             started_at_epoch_ms, completed_at_epoch_ms, serialization)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"
    );
    let res = sqlx::query(&sql)
        .bind(workflow_id)
        .bind(function_id)
        .bind(function_name)
        .bind(output)
        .bind(error)
        .bind(child_workflow_id)
        .bind(now)
        .bind(now)
        .bind(serialization)
        .execute(exec)
        .await;
    match res {
        Ok(_) => Ok(()),
        Err(e) if is_unique_violation(&e) => Err(DbosError::conflicting_id(workflow_id)),
        Err(e) => Err(e.into()),
    }
}

/// Record the parent→child mapping at the reserved step id, inside the parent's insert transaction.
pub async fn record_child_workflow(
    tx: &mut sqlx::PgConnection,
    schema: &str,
    parent_id: &str,
    function_id: i32,
    function_name: &str,
    child_id: &str,
) -> Result<()> {
    record_operation_result(
        &mut *tx,
        schema,
        parent_id,
        function_id,
        function_name,
        None,
        None,
        Some(child_id),
        None,
    )
    .await
}

/// Look up the child workflow id recorded at `(parent_id, function_id)`, if any (replay
/// short-circuit).
pub async fn check_child_workflow(
    pool: &PgPool,
    schema: &str,
    parent_id: &str,
    function_id: i32,
) -> Result<Option<String>> {
    let prefix = schema_prefix(schema);
    // Outer Option: row present? Inner Option: column non-null (steps have a NULL child id).
    let child: Option<Option<String>> = sqlx::query_scalar(&format!(
        "SELECT child_workflow_id FROM {prefix}operation_outputs
         WHERE workflow_uuid = $1 AND function_id = $2"
    ))
    .bind(parent_id)
    .bind(function_id)
    .fetch_optional(pool)
    .await?;
    Ok(child.flatten())
}
