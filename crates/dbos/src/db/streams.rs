//! Durable streams: an append-only, offset-ordered log a workflow publishes under a key, read
//! (live) by consumers. Ports the Go/Python `writeStream` / `readStream`.

use std::time::Duration;

use serde::de::DeserializeOwned;
use sqlx::PgPool;

use super::schema_prefix;
use super::status::get_status;
use crate::error::{DbosError, Result};
use crate::serialize::decode_value;

/// Stored (raw, unserialized) in `streams.value` to mark a stream closed. Cannot collide with a
/// user value: user values are always JSON-encoded, so the bare unquoted string never matches.
pub const STREAM_CLOSED_SENTINEL: &str = "__DBOS_STREAM_CLOSED__";
pub const STREAMS_CHANNEL: &str = "dbos_streams_channel";

const POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Append an entry to a stream, at the next per-key offset (`MAX(offset)+1`). Refuses to write to a
/// closed stream. `function_id` is the producing step id (for durability), independent of `offset`.
pub async fn write_stream_entry(
    tx: &mut sqlx::PgConnection,
    schema: &str,
    workflow_id: &str,
    function_id: i32,
    key: &str,
    value: &str,
    serialization: Option<&str>,
) -> Result<()> {
    let prefix = schema_prefix(schema);

    // Closed guard: a sentinel for this (workflow, key) means the stream is closed.
    let closed: Option<i32> = sqlx::query_scalar(&format!(
        "SELECT 1 FROM {prefix}streams WHERE workflow_uuid = $1 AND key = $2 AND value = $3 LIMIT 1"
    ))
    .bind(workflow_id)
    .bind(key)
    .bind(STREAM_CLOSED_SENTINEL)
    .fetch_optional(&mut *tx)
    .await?;
    if closed.is_some() {
        return Err(DbosError::other(format!("stream '{key}' is already closed")));
    }

    sqlx::query(&format!(
        "INSERT INTO {prefix}streams (workflow_uuid, key, value, \"offset\", function_id, serialization)
         SELECT $1, $2, $3,
                COALESCE((SELECT MAX(\"offset\") FROM {prefix}streams WHERE workflow_uuid = $1 AND key = $2), -1) + 1,
                $4, $5"
    ))
    .bind(workflow_id)
    .bind(key)
    .bind(value)
    .bind(function_id)
    .bind(serialization)
    .execute(&mut *tx)
    .await?;
    Ok(())
}

/// A raw stream entry before decoding.
pub struct StreamEntry {
    pub value: String,
    pub offset: i32,
    pub serialization: Option<String>,
}

/// Read all stream entries at or after `from_offset`, in order. The bool is true if the closing
/// sentinel was seen (the sentinel itself is not returned).
pub async fn read_stream_entries(
    pool: &PgPool,
    schema: &str,
    workflow_id: &str,
    key: &str,
    from_offset: i32,
) -> Result<(Vec<StreamEntry>, bool)> {
    let prefix = schema_prefix(schema);
    let rows: Vec<(String, i32, Option<String>)> = sqlx::query_as(&format!(
        "SELECT value, \"offset\", serialization FROM {prefix}streams
         WHERE workflow_uuid = $1 AND key = $2 AND \"offset\" >= $3 ORDER BY \"offset\" ASC"
    ))
    .bind(workflow_id)
    .bind(key)
    .bind(from_offset)
    .fetch_all(pool)
    .await?;

    let mut entries = Vec::new();
    let mut closed = false;
    for (value, offset, serialization) in rows {
        if value == STREAM_CLOSED_SENTINEL {
            closed = true;
            break;
        }
        entries.push(StreamEntry {
            value,
            offset,
            serialization,
        });
    }
    Ok((entries, closed))
}

/// Read a stream into a vector, blocking until it is closed (sentinel) or the producing workflow
/// becomes inactive (status not `PENDING`/`ENQUEUED`). Returns `(values, closed)`. With
/// `snapshot`, returns the currently-available values without blocking. `notify`, if given, is
/// woken by the LISTEN/NOTIFY listener when new entries arrive.
pub async fn read_stream_blocking<T: DeserializeOwned>(
    pool: &PgPool,
    schema: &str,
    notify: Option<&tokio::sync::Notify>,
    workflow_id: &str,
    key: &str,
    from_offset: i32,
    snapshot: bool,
) -> Result<(Vec<T>, bool)> {
    let mut current = from_offset;
    let mut values = Vec::new();
    loop {
        let notified = notify.map(|n| n.notified());
        let (entries, closed) = read_stream_entries(pool, schema, workflow_id, key, current).await?;
        let got = !entries.is_empty();
        for e in entries {
            values.push(decode_value::<T>(Some(&e.value), e.serialization.as_deref())?);
            current = e.offset + 1;
        }
        if closed {
            return Ok((values, true));
        }
        if snapshot {
            return Ok((values, false));
        }
        // An inactive producer means no more values will come — treat as closed.
        match get_status(pool, schema, workflow_id).await? {
            None => return Err(DbosError::non_existent_workflow(workflow_id)),
            Some(s) if s != "PENDING" && s != "ENQUEUED" => return Ok((values, true)),
            _ => {}
        }
        if !got {
            match notified {
                Some(n) => {
                    tokio::select! {
                        _ = n => {}
                        _ = tokio::time::sleep(POLL_INTERVAL) => {}
                    }
                }
                None => tokio::time::sleep(POLL_INTERVAL).await,
            }
        }
    }
}
