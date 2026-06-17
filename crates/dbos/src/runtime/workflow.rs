//! `start_workflow` (the direct-execution lifecycle) and the `WorkflowHandle`.

use std::marker::PhantomData;
use std::sync::Arc;

use serde::de::DeserializeOwned;
use tokio::sync::{oneshot, Mutex};

use super::context::{AuthIdentity, WorkflowContext, WorkflowState};
use super::{DbosInner, WorkflowOptions};
use crate::db::now_epoch_ms;
use crate::db::status::{
    await_workflow_result, get_status, insert_workflow_status, update_workflow_outcome,
    AwaitOutcome, InsertWorkflowInput, WorkflowStatusType,
};
use crate::db::steps::record_child_workflow;
use crate::error::{DbosError, Result};
use crate::serialize::{
    decode_value, deserialize_workflow_error, serialize_workflow_error, DecodedError, Format,
};

/// A handle to a running or completed workflow. Obtain a typed result with
/// [`get_result`](Self::get_result).
pub struct WorkflowHandle<R> {
    id: String,
    inner: Arc<DbosInner>,
    kind: HandleKind,
    _pd: PhantomData<fn() -> R>,
}

enum HandleKind {
    /// Returned by the executor that ran the body; backed by an in-process channel.
    Owned(Mutex<Option<oneshot::Receiver<Result<String>>>>),
    /// Returned for idempotent/child/recovery/enqueue paths; reads the result from the DB.
    Polling,
}

impl<R> WorkflowHandle<R> {
    pub(crate) fn polling(id: String, inner: Arc<DbosInner>) -> Self {
        WorkflowHandle {
            id,
            inner,
            kind: HandleKind::Polling,
            _pd: PhantomData,
        }
    }

    pub(crate) fn owned(
        id: String,
        inner: Arc<DbosInner>,
        rx: oneshot::Receiver<Result<String>>,
    ) -> Self {
        WorkflowHandle {
            id,
            inner,
            kind: HandleKind::Owned(Mutex::new(Some(rx))),
            _pd: PhantomData,
        }
    }

    pub fn workflow_id(&self) -> &str {
        &self.id
    }
}

impl<R: DeserializeOwned> WorkflowHandle<R> {
    /// Await the workflow's result. For an owned handle this waits on the running task; otherwise
    /// it polls the database. A workflow error surfaces as a [`DbosError`].
    pub async fn get_result(&self) -> Result<R> {
        if let HandleKind::Owned(rx_slot) = &self.kind {
            let rx = rx_slot.lock().await.take();
            if let Some(rx) = rx {
                match rx.await {
                    Ok(Ok(encoded)) => {
                        return decode_value::<R>(Some(&encoded), Some(Format::Portable.name()))
                    }
                    Ok(Err(e)) => return Err(e),
                    // Sender dropped (e.g. the task was cancelled at shutdown) — fall back to poll.
                    Err(_) => {}
                }
            }
        }

        let outcome = await_workflow_result(
            &self.inner.pool,
            &self.inner.schema,
            &self.id,
            self.inner.poll_interval,
        )
        .await?;
        match outcome {
            AwaitOutcome::Success {
                output,
                serialization,
            } => decode_value::<R>(output.as_deref(), serialization.as_deref()),
            AwaitOutcome::Error {
                error,
                serialization,
            } => Err(
                match deserialize_workflow_error(error.as_deref(), serialization.as_deref()) {
                    Some(DecodedError::Plain(s)) => DbosError::other(s),
                    Some(DecodedError::Portable(pe)) => DbosError::other(pe.message),
                    None => DbosError::other("workflow failed"),
                },
            ),
            AwaitOutcome::Cancelled => Err(DbosError::awaited_workflow_cancelled(&self.id)),
            AwaitOutcome::DeadLetter { recovery_attempts } => {
                Err(DbosError::dead_letter_queue(&self.id, recovery_attempts - 2))
            }
        }
    }

    /// Read the workflow's current status without blocking.
    pub async fn get_status(&self) -> Result<Option<WorkflowStatusType>> {
        let s = get_status(&self.inner.pool, &self.inner.schema, &self.id).await?;
        Ok(s.and_then(|s| WorkflowStatusType::from_str(&s)))
    }
}

/// Insert the workflow status, decide idempotency, and (unless skipped) spawn the body — recording
/// its outcome when it finishes. Returns an owned handle if this call runs the body, else a polling
/// handle. Handles both top-level (`parent == None`) and child workflows.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn start_workflow<R>(
    inner: Arc<DbosInner>,
    name: &str,
    encoded_input: Option<String>,
    opts: WorkflowOptions,
    parent: Option<(Arc<WorkflowState>, i32)>,
    fmt: Format,
    is_recovery: bool,
) -> Result<WorkflowHandle<R>> {
    let (handler, max_retries, class_name, config_name) = {
        let entry = inner
            .registry
            .get(name)
            .ok_or_else(|| DbosError::other(format!("workflow {name} is not registered")))?;
        (
            entry.handler.clone(),
            entry.max_retries,
            entry.class_name.clone(),
            entry.config_name.clone(),
        )
    };

    let workflow_id = if let Some(id) = opts.workflow_id.clone() {
        id
    } else if let Some((pstate, step_id)) = &parent {
        format!("{}-{}", pstate.workflow_id, step_id)
    } else {
        uuid::Uuid::new_v4().to_string()
    };

    let now = now_epoch_ms();
    let owner_xid = uuid::Uuid::new_v4().to_string();
    let app_version = opts
        .application_version
        .clone()
        .unwrap_or_else(|| inner.application_version.clone());
    let auth = resolve_auth(&opts, parent.as_ref());

    let input = InsertWorkflowInput {
        workflow_id: workflow_id.clone(),
        status: WorkflowStatusType::Pending.as_str().to_string(),
        name: name.to_string(),
        executor_id: inner.executor_id.clone(),
        application_version: Some(app_version),
        application_id: inner.application_id.clone(),
        created_at: now,
        updated_at: now,
        recovery_attempts: 1,
        inputs: encoded_input.clone(),
        priority: 0,
        owner_xid: owner_xid.clone(),
        parent_workflow_id: parent.as_ref().map(|(p, _)| p.workflow_id.clone()),
        class_name,
        config_name,
        serialization: Some(fmt.name().to_string()),
        authenticated_user: auth.user.clone(),
        assumed_role: auth.role.clone(),
        authenticated_roles: if auth.roles.is_empty() {
            None
        } else {
            serde_json::to_string(&auth.roles).ok()
        },
        // On recovery, bump recovery_attempts via the ON CONFLICT path; a fresh run does not.
        increment: if is_recovery { 1 } else { 0 },
        ..Default::default()
    };

    let mut tx = inner.pool.begin().await?;
    let res = insert_workflow_status(&mut tx, &inner.schema, &input, max_retries).await?;
    if res.dlq_triggered {
        tx.commit().await?;
        return Err(DbosError::dead_letter_queue(&workflow_id, max_retries));
    }
    if let Some((pstate, step_id)) = &parent {
        record_child_workflow(
            &mut tx,
            &inner.schema,
            &pstate.workflow_id,
            *step_id,
            name,
            &workflow_id,
        )
        .await?;
    }

    // Idempotency: don't re-run a workflow that is already terminal, or (unless we are recovering)
    // one that another executor owns.
    let should_skip = res.status == WorkflowStatusType::Success.as_str()
        || res.status == WorkflowStatusType::Error.as_str()
        || (!is_recovery && res.owner_xid.as_deref() != Some(owner_xid.as_str()));
    tx.commit().await?;

    if should_skip {
        return Ok(WorkflowHandle::polling(workflow_id, inner));
    }

    // Run the body in a tracked task; record its outcome and forward it to the owned handle.
    let state = Arc::new(WorkflowState::new(workflow_id.clone(), auth));
    let wf_ctx = WorkflowContext {
        inner: inner.clone(),
        state,
        within_step: false,
    };
    let (tx_res, rx) = oneshot::channel();
    let pool = inner.pool.clone();
    let schema = inner.schema.clone();
    let id_for_task = workflow_id.clone();
    inner.workflow_tasks.spawn(async move {
        let result = handler(wf_ctx, encoded_input, fmt).await;
        let (status, output, err_str) = match &result {
            Ok(out) => (WorkflowStatusType::Success, Some(out.clone()), None),
            Err(e) => (
                WorkflowStatusType::Error,
                None,
                Some(serialize_workflow_error(&e.to_string(), None, fmt)),
            ),
        };
        let _ = update_workflow_outcome(
            &pool,
            &schema,
            &id_for_task,
            status,
            output.as_deref(),
            err_str.as_deref(),
        )
        .await;
        let _ = tx_res.send(result);
    });

    Ok(WorkflowHandle::owned(workflow_id, inner, rx))
}

fn resolve_auth(opts: &WorkflowOptions, parent: Option<&(Arc<WorkflowState>, i32)>) -> AuthIdentity {
    let pauth = parent.map(|(p, _)| &p.auth);
    AuthIdentity {
        user: opts
            .authenticated_user
            .clone()
            .or_else(|| pauth.and_then(|a| a.user.clone())),
        role: opts
            .assumed_role
            .clone()
            .or_else(|| pauth.and_then(|a| a.role.clone())),
        roles: opts
            .authenticated_roles
            .clone()
            .unwrap_or_else(|| pauth.map(|a| a.roles.clone()).unwrap_or_default()),
    }
}
