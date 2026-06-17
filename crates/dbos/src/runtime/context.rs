//! The per-workflow execution context, threaded explicitly into steps and child workflows
//! (Go-style — no task-locals), keeping step/child boundaries deterministic.

use std::future::Future;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;

use serde::{de::DeserializeOwned, Serialize};

use super::step::{decoded_step_error, execute_step_with_retry, StepOptions};
use super::workflow::{start_workflow, WorkflowHandle};
use super::{DbosInner, WorkflowOptions};
use crate::db::steps::{check_child_workflow, check_operation_execution, record_operation_result};
use crate::error::{DbosError, Result};
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
        let result = f(self.step_ctx()).await;
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
        let result =
            execute_step_with_retry(&opts, name, &self.state.workflow_id, move || {
                f(step_ctx.clone())
            })
            .await;
        self.record_step(step_id, name, &result).await?;
        result
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
            return Ok(WorkflowHandle::polling(child_id, self.inner.clone()));
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
}
