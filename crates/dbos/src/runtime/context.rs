//! The per-workflow execution context, threaded explicitly into steps and child workflows
//! (Go-style — no task-locals), keeping step/child boundaries deterministic.

use std::future::Future;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde::{de::DeserializeOwned, Serialize};
use tracing::Instrument;

use super::listener::WaiterGuard;
use super::step::{decoded_step_error, execute_step_with_retry, StepOptions};
use super::workflow::{start_workflow, WorkflowHandle};
use super::{DbosInner, WorkflowOptions};
use crate::db::notifications::{
    consume_oldest_notification, insert_notification, insert_workflow_event_history,
    notification_exists_unconsumed, select_workflow_event, upsert_workflow_event, NULL_TOPIC,
};
use crate::db::now_epoch_ms;
use crate::db::streams::{write_stream_entry, STREAM_CLOSED_SENTINEL};
use crate::db::steps::{
    check_child_workflow, check_operation_execution, record_operation_result, RecordedOperation,
};
use crate::error::{DbosError, DbosErrorCode, Result};
use crate::serialize::{decode_value, encode_input, encode_value, serialize_workflow_error, Format};

/// The authenticated identity carried by a workflow and inherited by its children.
#[derive(Clone, Default)]
pub(crate) struct AuthIdentity {
    pub user: Option<String>,
    pub role: Option<String>,
    pub roles: Vec<String>,
}

/// Per-running-workflow state, shared (via `Arc`) between the workflow body and its steps/children.
pub(crate) struct WorkflowState {
    pub workflow_id: String,
    /// 0-based step counter, pre-incremented (`-1` initial → first id `0`). Shared by steps,
    /// children, and durable `get_result` so replay matches by `(workflow_uuid, function_id)`.
    step_id: AtomicI32,
    pub auth: AuthIdentity,
}

impl WorkflowState {
    pub fn new(workflow_id: String, auth: AuthIdentity) -> Self {
        WorkflowState {
            workflow_id,
            step_id: AtomicI32::new(-1),
            auth,
        }
    }

    pub fn next_step_id(&self) -> i32 {
        self.step_id.fetch_add(1, Ordering::SeqCst) + 1
    }
}

/// Handle passed to a workflow body and to step closures. Cheap to clone.
#[derive(Clone)]
pub struct WorkflowContext {
    pub(crate) inner: Arc<DbosInner>,
    pub(crate) state: Arc<WorkflowState>,
    /// True inside a step body: a nested `run_step`/`run_workflow` runs inline (no new step id).
    pub(crate) within_step: bool,
}

impl WorkflowContext {
    /// The current workflow's id.
    pub fn workflow_id(&self) -> &str {
        &self.state.workflow_id
    }

    /// Run a durable step. The result is checkpointed on `(workflow_uuid, step_id)`; on replay the
    /// recorded value is returned without re-running. No retries.
    ///
    /// The closure is `FnOnce`, so it may freely move captured values into its future. For retries
    /// use [`run_step_with`](Self::run_step_with) (whose closure is `Fn` and must clone captures).
    pub async fn run_step<R, F, Fut>(&self, name: &str, f: F) -> Result<R>
    where
        R: Serialize + DeserializeOwned + Send,
        F: FnOnce(WorkflowContext) -> Fut + Send,
        Fut: Future<Output = Result<R>> + Send,
    {
        // A step nested inside another step runs inline and counts as part of the enclosing step.
        if self.within_step {
            return f(self.clone()).await;
        }
        let step_id = self.state.next_step_id();
        if let Some(replay) = self.replay_step::<R>(step_id, name).await? {
            return replay;
        }
        let result = f(self.step_ctx()).instrument(self.step_span(name)).await;
        self.record_step(step_id, name, &result).await?;
        result
    }

    /// Like [`run_step`](Self::run_step) but with explicit retry configuration. Because the body may
    /// run several times, the closure is `Fn` and must clone any captured values it needs.
    pub async fn run_step_with<R, F, Fut>(&self, name: &str, opts: StepOptions, f: F) -> Result<R>
    where
        R: Serialize + DeserializeOwned + Send,
        F: Fn(WorkflowContext) -> Fut + Send,
        Fut: Future<Output = Result<R>> + Send,
    {
        if self.within_step {
            return f(self.clone()).await;
        }
        let step_id = self.state.next_step_id();
        if let Some(replay) = self.replay_step::<R>(step_id, name).await? {
            return replay;
        }
        let step_ctx = self.step_ctx();
        let result = execute_step_with_retry(&opts, name, &self.state.workflow_id, move || {
            f(step_ctx.clone())
        })
        .instrument(self.step_span(name))
        .await;
        self.record_step(step_id, name, &result).await?;
        result
    }

    /// A tracing span for a step (a child of the active workflow span).
    fn step_span(&self, name: &str) -> tracing::Span {
        tracing::info_span!(
            "dbos.step",
            "otel.name" = %name,
            operationUUID = %self.state.workflow_id,
            operationType = "step",
            operationName = %name,
        )
    }

    /// A child context marking that we are inside a step body.
    fn step_ctx(&self) -> WorkflowContext {
        WorkflowContext {
            inner: self.inner.clone(),
            state: self.state.clone(),
            within_step: true,
        }
    }

    /// If this step already ran, return its recorded result (`Ok`/`Err`); otherwise `None`.
    async fn replay_step<R: DeserializeOwned>(
        &self,
        step_id: i32,
        name: &str,
    ) -> Result<Option<Result<R>>> {
        match check_operation_execution(
            &self.inner.pool,
            &self.inner.schema,
            &self.state.workflow_id,
            step_id,
            name,
        )
        .await?
        {
            None => Ok(None),
            Some(rec) => {
                if let Some(err) = rec.error {
                    return Ok(Some(Err(decoded_step_error(
                        &err,
                        rec.serialization.as_deref(),
                    ))));
                }
                let value =
                    decode_value::<R>(rec.output.as_deref(), rec.serialization.as_deref())?;
                Ok(Some(Ok(value)))
            }
        }
    }

    /// Checkpoint a step's outcome to `operation_outputs`.
    async fn record_step<R: Serialize>(
        &self,
        step_id: i32,
        name: &str,
        result: &Result<R>,
    ) -> Result<()> {
        let fmt = Format::Portable;
        let (output, error) = match result {
            Ok(value) => (Some(encode_value(value, fmt)?), None),
            Err(e) => (None, Some(serialize_workflow_error(&e.to_string(), None, fmt))),
        };
        record_operation_result(
            &self.inner.pool,
            &self.inner.schema,
            &self.state.workflow_id,
            step_id,
            name,
            output.as_deref(),
            error.as_deref(),
            None,
            Some(fmt.name()),
        )
        .await
    }

    /// Start a child workflow. Its id defaults to `{parent}-{step_id}`; the mapping is recorded so
    /// a replaying parent resolves to the same child.
    pub async fn run_workflow<P, R>(
        &self,
        name: &str,
        input: P,
        opts: WorkflowOptions,
    ) -> Result<WorkflowHandle<R>>
    where
        P: Serialize + Send,
    {
        if self.within_step {
            return Err(DbosError::step_execution(
                &self.state.workflow_id,
                name,
                "cannot start a child workflow from within a step",
            ));
        }
        let step_id = self.state.next_step_id();

        // Replay short-circuit: if a child was already recorded here, return a handle to it.
        if let Some(child_id) = check_child_workflow(
            &self.inner.pool,
            &self.inner.schema,
            &self.state.workflow_id,
            step_id,
        )
        .await?
        {
            return Ok(WorkflowHandle::polling_from_inner(child_id, &self.inner));
        }

        let encoded = encode_input(&input, Format::Portable)?;
        start_workflow::<R>(
            self.inner.clone(),
            name,
            Some(encoded),
            opts,
            Some((self.state.clone(), step_id)),
            Format::Portable,
            false,
        )
        .await
    }

    /// Send a message to another workflow's mailbox. A durable step (sent exactly once across
    /// replays). The destination workflow must exist.
    pub async fn send<M: Serialize>(
        &self,
        destination_id: &str,
        message: M,
        topic: Option<&str>,
    ) -> Result<()> {
        if self.within_step {
            return Err(DbosError::step_execution(
                &self.state.workflow_id,
                "DBOS.send",
                "cannot call Send within a step",
            ));
        }
        let step_id = self.state.next_step_id();
        if check_operation_execution(
            &self.inner.pool,
            &self.inner.schema,
            &self.state.workflow_id,
            step_id,
            "DBOS.send",
        )
        .await?
        .is_some()
        {
            return Ok(());
        }
        let topic = topic.filter(|t| !t.is_empty()).unwrap_or(NULL_TOPIC);
        let encoded = encode_value(&message, Format::Portable)?;
        let unit = encode_value(&(), Format::Portable)?;
        let mut tx = self.inner.pool.begin().await?;
        insert_notification(
            &mut tx,
            &self.inner.schema,
            destination_id,
            topic,
            &encoded,
            Format::Portable.name(),
        )
        .await?;
        record_operation_result(
            &mut *tx,
            &self.inner.schema,
            &self.state.workflow_id,
            step_id,
            "DBOS.send",
            Some(&unit),
            None,
            None,
            Some(Format::Portable.name()),
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    /// Wait for a message on `topic` (default topic if `None`), up to `timeout`. Returns `Ok(None)`
    /// on timeout. Only one `recv` may be active per `(workflow, topic)` at a time. The wait is
    /// durable: it survives a restart and resumes for only the remaining time.
    pub async fn recv<M: DeserializeOwned>(
        &self,
        topic: Option<&str>,
        timeout: Duration,
    ) -> Result<Option<M>> {
        if self.within_step {
            return Err(DbosError::step_execution(
                &self.state.workflow_id,
                "DBOS.recv",
                "cannot call Recv within a step",
            ));
        }
        let step_id = self.state.next_step_id();
        let sleep_step_id = self.state.next_step_id();
        if let Some(rec) = check_operation_execution(
            &self.inner.pool,
            &self.inner.schema,
            &self.state.workflow_id,
            step_id,
            "DBOS.recv",
        )
        .await?
        {
            return decode_optional::<M>(&rec);
        }

        let topic = topic.filter(|t| !t.is_empty()).unwrap_or(NULL_TOPIC);
        let payload = format!("{}::{}", self.state.workflow_id, topic);
        let notify = Arc::new(tokio::sync::Notify::new());
        {
            use dashmap::mapref::entry::Entry;
            match self.inner.notifications_waiters.entry(payload.clone()) {
                Entry::Occupied(_) => {
                    return Err(DbosError::conflicting_id(&self.state.workflow_id))
                }
                Entry::Vacant(v) => {
                    v.insert(notify.clone());
                }
            }
        }
        let _guard = WaiterGuard::new(&self.inner.notifications_waiters, payload);

        let remaining = self.record_sleep(sleep_step_id, timeout, true).await?;
        let deadline = tokio::time::Instant::now() + remaining;
        loop {
            let notified = notify.notified();
            if notification_exists_unconsumed(
                &self.inner.pool,
                &self.inner.schema,
                &self.state.workflow_id,
                topic,
            )
            .await?
            {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                break;
            }
            tokio::select! {
                _ = notified => {}
                _ = tokio::time::sleep_until(deadline) => {}
            }
        }

        let mut tx = self.inner.pool.begin().await?;
        let consumed = consume_oldest_notification(
            &mut tx,
            &self.inner.schema,
            &self.state.workflow_id,
            topic,
        )
        .await?;
        {
            let output = consumed.as_ref().map(|(m, _)| m.as_str());
            let serialization = consumed.as_ref().and_then(|(_, s)| s.as_deref());
            record_operation_result(
                &mut *tx,
                &self.inner.schema,
                &self.state.workflow_id,
                step_id,
                "DBOS.recv",
                output,
                None,
                None,
                Some(serialization.unwrap_or(Format::Portable.name())),
            )
            .await?;
        }
        tx.commit().await?;

        match consumed {
            Some((msg, ser)) => Ok(Some(decode_value::<M>(
                Some(&msg),
                ser.as_deref().or(Some(Format::Portable.name())),
            )?)),
            None => Ok(None),
        }
    }

    /// Publish a key/value event from this workflow (readable by [`get_event`](Self::get_event)).
    /// A durable step.
    pub async fn set_event<V: Serialize>(&self, key: &str, value: V) -> Result<()> {
        if self.within_step {
            return Err(DbosError::step_execution(
                &self.state.workflow_id,
                "DBOS.setEvent",
                "cannot call SetEvent within a step",
            ));
        }
        let step_id = self.state.next_step_id();
        if check_operation_execution(
            &self.inner.pool,
            &self.inner.schema,
            &self.state.workflow_id,
            step_id,
            "DBOS.setEvent",
        )
        .await?
        .is_some()
        {
            return Ok(());
        }
        let encoded = encode_value(&value, Format::Portable)?;
        let unit = encode_value(&(), Format::Portable)?;
        let mut tx = self.inner.pool.begin().await?;
        upsert_workflow_event(
            &mut tx,
            &self.inner.schema,
            &self.state.workflow_id,
            key,
            &encoded,
            Format::Portable.name(),
        )
        .await?;
        insert_workflow_event_history(
            &mut tx,
            &self.inner.schema,
            &self.state.workflow_id,
            step_id,
            key,
            &encoded,
            Format::Portable.name(),
        )
        .await?;
        record_operation_result(
            &mut *tx,
            &self.inner.schema,
            &self.state.workflow_id,
            step_id,
            "DBOS.setEvent",
            Some(&unit),
            None,
            None,
            Some(Format::Portable.name()),
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    /// Read another workflow's event for `key`, waiting up to `timeout` for it to be set. Returns
    /// `Ok(None)` on timeout. Durable when called inside a workflow.
    pub async fn get_event<V: DeserializeOwned>(
        &self,
        target_workflow_id: &str,
        key: &str,
        timeout: Duration,
    ) -> Result<Option<V>> {
        if self.within_step {
            return Err(DbosError::step_execution(
                &self.state.workflow_id,
                "DBOS.getEvent",
                "cannot call GetEvent within a step",
            ));
        }
        let step_id = self.state.next_step_id();
        let sleep_step_id = self.state.next_step_id();
        if let Some(rec) = check_operation_execution(
            &self.inner.pool,
            &self.inner.schema,
            &self.state.workflow_id,
            step_id,
            "DBOS.getEvent",
        )
        .await?
        {
            return decode_optional::<V>(&rec);
        }

        let payload = format!("{target_workflow_id}::{key}");
        let notify = self
            .inner
            .events_waiters
            .entry(payload.clone())
            .or_insert_with(|| Arc::new(tokio::sync::Notify::new()))
            .clone();
        let _guard = WaiterGuard::new(&self.inner.events_waiters, payload);

        let remaining = self.record_sleep(sleep_step_id, timeout, true).await?;
        let deadline = tokio::time::Instant::now() + remaining;
        let mut found: Option<(String, Option<String>)> = None;
        loop {
            let notified = notify.notified();
            if let Some(ev) =
                select_workflow_event(&self.inner.pool, &self.inner.schema, target_workflow_id, key)
                    .await?
            {
                found = Some(ev);
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                break;
            }
            tokio::select! {
                _ = notified => {}
                _ = tokio::time::sleep_until(deadline) => {}
            }
        }

        {
            let output = found.as_ref().map(|(v, _)| v.as_str());
            let serialization = found.as_ref().and_then(|(_, s)| s.as_deref());
            record_operation_result(
                &self.inner.pool,
                &self.inner.schema,
                &self.state.workflow_id,
                step_id,
                "DBOS.getEvent",
                output,
                None,
                None,
                Some(serialization.unwrap_or(Format::Portable.name())),
            )
            .await?;
        }
        match found {
            Some((v, ser)) => Ok(Some(decode_value::<V>(
                Some(&v),
                ser.as_deref().or(Some(Format::Portable.name())),
            )?)),
            None => Ok(None),
        }
    }

    /// Append a value to a durable, append-only stream under `key` (a durable step, written exactly
    /// once across replays). Consumers read it live with [`Dbos::read_stream`](super::Dbos::read_stream).
    /// Errors if the stream is already closed.
    pub async fn write_stream<V: Serialize>(&self, key: &str, value: V) -> Result<()> {
        let encoded = encode_value(&value, Format::Portable)?;
        self.write_stream_raw(key, "DBOS.writeStream", &encoded, Some(Format::Portable.name()))
            .await
    }

    /// Close a stream, so readers stop once they reach this point.
    pub async fn close_stream(&self, key: &str) -> Result<()> {
        self.write_stream_raw(key, "DBOS.closeStream", STREAM_CLOSED_SENTINEL, None)
            .await
    }

    async fn write_stream_raw(
        &self,
        key: &str,
        step_name: &str,
        value: &str,
        serialization: Option<&str>,
    ) -> Result<()> {
        if self.within_step {
            return Err(DbosError::step_execution(
                &self.state.workflow_id,
                step_name,
                "cannot write to a stream within a step",
            ));
        }
        let step_id = self.state.next_step_id();
        if check_operation_execution(
            &self.inner.pool,
            &self.inner.schema,
            &self.state.workflow_id,
            step_id,
            step_name,
        )
        .await?
        .is_some()
        {
            return Ok(());
        }
        let unit = encode_value(&(), Format::Portable)?;
        let mut tx = self.inner.pool.begin().await?;
        write_stream_entry(
            &mut tx,
            &self.inner.schema,
            &self.state.workflow_id,
            step_id,
            key,
            value,
            serialization,
        )
        .await?;
        record_operation_result(
            &mut *tx,
            &self.inner.schema,
            &self.state.workflow_id,
            step_id,
            step_name,
            Some(&unit),
            None,
            None,
            Some(Format::Portable.name()),
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    /// Durably sleep for `duration`. The wake-up deadline is checkpointed, so after a crash the
    /// workflow resumes and waits only the remaining time (even across days). Returns the duration
    /// actually waited.
    pub async fn sleep(&self, duration: Duration) -> Result<Duration> {
        if self.within_step {
            return Err(DbosError::step_execution(
                &self.state.workflow_id,
                "DBOS.sleep",
                "cannot call Sleep within a step",
            ));
        }
        let step_id = self.state.next_step_id();
        self.record_sleep(step_id, duration, false).await
    }

    /// Durable sleep machinery: record the absolute wake-up deadline as a `DBOS.sleep` step so a
    /// restart waits only the remaining time. Returns the remaining duration; with `skip_sleep` it
    /// does not block (used by `recv`/`get_event` timeouts, which do their own waiting).
    pub(crate) async fn record_sleep(
        &self,
        step_id: i32,
        duration: Duration,
        skip_sleep: bool,
    ) -> Result<Duration> {
        let end_ms = if let Some(rec) = check_operation_execution(
            &self.inner.pool,
            &self.inner.schema,
            &self.state.workflow_id,
            step_id,
            "DBOS.sleep",
        )
        .await?
        {
            decode_value::<i64>(rec.output.as_deref(), rec.serialization.as_deref())?
        } else {
            let end = now_epoch_ms() + duration.as_millis() as i64;
            let output = encode_value(&end, Format::Portable)?;
            match record_operation_result(
                &self.inner.pool,
                &self.inner.schema,
                &self.state.workflow_id,
                step_id,
                "DBOS.sleep",
                Some(&output),
                None,
                None,
                Some(Format::Portable.name()),
            )
            .await
            {
                Ok(()) => end,
                // Another process recorded the deadline first — read it back.
                Err(e) if e.is(DbosErrorCode::ConflictingId) => {
                    let rec = check_operation_execution(
                        &self.inner.pool,
                        &self.inner.schema,
                        &self.state.workflow_id,
                        step_id,
                        "DBOS.sleep",
                    )
                    .await?
                    .ok_or(e)?;
                    decode_value::<i64>(rec.output.as_deref(), rec.serialization.as_deref())?
                }
                Err(e) => return Err(e),
            }
        };
        let remaining = Duration::from_millis((end_ms - now_epoch_ms()).max(0) as u64);
        if !skip_sleep {
            tokio::time::sleep(remaining).await;
        }
        Ok(remaining)
    }
}

/// Decode a recorded `recv`/`get_event` result: an error → `Err`, a NULL output (timeout) →
/// `Ok(None)`, a value → `Ok(Some(..))`.
fn decode_optional<M: DeserializeOwned>(rec: &RecordedOperation) -> Result<Option<M>> {
    if let Some(err) = &rec.error {
        return Err(decoded_step_error(err, rec.serialization.as_deref()));
    }
    match &rec.output {
        None => Ok(None),
        Some(out) => Ok(Some(decode_value::<M>(Some(out), rec.serialization.as_deref())?)),
    }
}
