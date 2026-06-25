//! Notifications (`send`/`recv`) and events (`set_event`/`get_event`) SQL. Ports the Go
//! `send`/`recv`/`setEvent`/`getEvent` system-database operations.

use sqlx::PgPool;

use super::{now_epoch_ms, schema_prefix};
use crate::error::{is_foreign_key_violation, DbosError, Result};

/// Sentinel stored in `notifications.topic` for an empty/default topic (never SQL NULL).
pub const NULL_TOPIC: &str = "__null__topic__";
pub const NOTIFICATIONS_CHANNEL: &str = "dbos_notifications_channel";
pub const WORKFLOW_EVENTS_CHANNEL: &str = "dbos_workflow_events_channel";

/// Insert a message into a destination workflow's mailbox. A missing destination (FK violation)
/// surfaces as [`DbosError::non_existent_workflow`].
pub async fn insert_notification(
    tx: &mut sqlx::PgConnection,
    schema: &str,
    destination_id: &str,
    topic: &str,
    message: &str,
    serialization: &str,
) -> Result<()> {
    let prefix = schema_prefix(schema);
    let res = sqlx::query(&format!(
        "INSERT INTO {prefix}notifications
            (destination_uuid, topic, message, serialization, message_uuid, created_at_epoch_ms)
         VALUES ($1, $2, $3, $4, $5, $6)"
    ))
    .bind(destination_id)
    .bind(topic)
    .bind(message)
    .bind(serialization)
    .bind(uuid::Uuid::new_v4().to_string())
    .bind(now_epoch_ms())
    .execute(&mut *tx)
    .await;
    match res {
        Ok(_) => Ok(()),
        Err(e) if is_foreign_key_violation(&e) => {
            Err(DbosError::non_existent_workflow(destination_id))
        }
        Err(e) => Err(e.into()),
    }
}

/// Is there an unconsumed message for `(destination, topic)`?
pub async fn notification_exists_unconsumed(
    pool: &PgPool,
    schema: &str,
    destination_id: &str,
    topic: &str,
) -> Result<bool> {
    let prefix = schema_prefix(schema);
    let exists: bool = sqlx::query_scalar(&format!(
        "SELECT EXISTS(SELECT 1 FROM {prefix}notifications
         WHERE destination_uuid = $1 AND topic = $2 AND consumed = false)"
    ))
    .bind(destination_id)
    .bind(topic)
    .fetch_one(pool)
    .await?;
    Ok(exists)
}

/// Consume the oldest unconsumed message for `(destination, topic)` (FIFO, exactly one), returning
/// its `(message, serialization)`.
pub async fn consume_oldest_notification(
    tx: &mut sqlx::PgConnection,
    schema: &str,
    destination_id: &str,
    topic: &str,
) -> Result<Option<(String, Option<String>)>> {
    let prefix = schema_prefix(schema);
    // Target a single message_uuid: created_at can collide within a millisecond.
    let row: Option<(String, Option<String>)> = sqlx::query_as(&format!(
        "WITH oldest_entry AS (
            SELECT message_uuid FROM {prefix}notifications
            WHERE destination_uuid = $1 AND topic = $2 AND consumed = false
            ORDER BY created_at_epoch_ms ASC LIMIT 1
        )
        UPDATE {prefix}notifications SET consumed = true
        WHERE message_uuid = (SELECT message_uuid FROM oldest_entry)
        RETURNING message, serialization"
    ))
    .bind(destination_id)
    .bind(topic)
    .fetch_optional(&mut *tx)
    .await?;
    Ok(row)
}

/// Upsert a workflow's current event value, and append to the per-step history.
pub async fn upsert_workflow_event(
    tx: &mut sqlx::PgConnection,
    schema: &str,
    workflow_id: &str,
    key: &str,
    value: &str,
    serialization: &str,
) -> Result<()> {
    let prefix = schema_prefix(schema);
    sqlx::query(&format!(
        "INSERT INTO {prefix}workflow_events (workflow_uuid, key, value, serialization)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (workflow_uuid, key)
         DO UPDATE SET value = EXCLUDED.value, serialization = EXCLUDED.serialization"
    ))
    .bind(workflow_id)
    .bind(key)
    .bind(value)
    .bind(serialization)
    .execute(&mut *tx)
    .await?;
    Ok(())
}

pub async fn insert_workflow_event_history(
    tx: &mut sqlx::PgConnection,
    schema: &str,
    workflow_id: &str,
    function_id: i32,
    key: &str,
    value: &str,
    serialization: &str,
) -> Result<()> {
    let prefix = schema_prefix(schema);
    sqlx::query(&format!(
        "INSERT INTO {prefix}workflow_events_history
            (workflow_uuid, function_id, key, value, serialization)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (workflow_uuid, function_id, key)
         DO UPDATE SET value = EXCLUDED.value, serialization = EXCLUDED.serialization"
    ))
    .bind(workflow_id)
    .bind(function_id)
    .bind(key)
    .bind(value)
    .bind(serialization)
    .execute(&mut *tx)
    .await?;
    Ok(())
}

/// Read a workflow's current event value for `key`.
pub async fn select_workflow_event(
    pool: &PgPool,
    schema: &str,
    target_workflow_id: &str,
    key: &str,
) -> Result<Option<(String, Option<String>)>> {
    let prefix = schema_prefix(schema);
    let row: Option<(String, Option<String>)> = sqlx::query_as(&format!(
        "SELECT value, serialization FROM {prefix}workflow_events
         WHERE workflow_uuid = $1 AND key = $2"
    ))
    .bind(target_workflow_id)
    .bind(key)
    .fetch_optional(pool)
    .await?;
    Ok(row)
}
