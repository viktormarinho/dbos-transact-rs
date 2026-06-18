//! The DBOS runtime: building, launching, and running durable workflows.

mod conductor;
mod context;
mod listener;
mod queue;
mod recovery;
mod registry;
mod scheduler;
mod step;
mod workflow;

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use serde::{de::DeserializeOwned, Serialize};
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::config::{process_config, Config};
use crate::db::management::{
    self, ForkWorkflowInput, ListWorkflowsFilter, StepInfo, WorkflowStatus, INTERNAL_QUEUE_NAME,
};
use crate::db::{connect, run_migrations};
use crate::error::{DbosError, Result};
use crate::serialize::{decode_input, encode_input, encode_value, Format};
use crate::BoxFuture;

use listener::{run_listener, WaiterGuard, WaiterRegistry};
use queue::{run_queue, WorkerCounts};
use recovery::recover_pending_workflows;
use registry::{ErasedWorkflow, Registry, RegistryEntry};
use scheduler::run_scheduler;
use workflow::{enqueue_workflow, start_workflow};

pub use context::WorkflowContext;
pub use queue::{RateLimiter, WorkflowQueue};
pub use scheduler::ScheduledWorkflowInput;
pub use step::StepOptions;
pub use workflow::WorkflowHandle;

pub use crate::db::status::WorkflowStatusType;

/// The default per-workflow recovery limit (`_DEFAULT_MAX_RECOVERY_ATTEMPTS`).
const DEFAULT_MAX_RECOVERY_ATTEMPTS: i64 = 100;

/// Registration-time options for a workflow.
#[derive(Debug, Clone)]
pub struct RegistrationOptions {
    /// Maximum number of recovery attempts before the workflow is moved to the dead-letter queue.
    /// `-1` means unlimited. Defaults to 100.
    pub max_retries: i64,
}

impl Default for RegistrationOptions {
    fn default() -> Self {
        RegistrationOptions {
            max_retries: DEFAULT_MAX_RECOVERY_ATTEMPTS,
        }
    }
}

/// Options for starting a workflow.
#[derive(Debug, Default, Clone)]
pub struct WorkflowOptions {
    /// Set an explicit workflow id (for exactly-once / idempotent invocation). Defaults to a UUIDv4.
    pub workflow_id: Option<String>,
    /// Override the application version recorded for this workflow.
    pub application_version: Option<String>,
    pub authenticated_user: Option<String>,
    pub assumed_role: Option<String>,
    pub authenticated_roles: Option<Vec<String>>,
}

/// Options for enqueueing a workflow onto a durable queue.
#[derive(Debug, Default, Clone)]
pub struct EnqueueOptions {
    /// Explicit workflow id (defaults to a UUIDv4).
    pub workflow_id: Option<String>,
    /// Deduplication key — at most one live workflow per `(queue, deduplication_id)`.
    pub deduplication_id: Option<String>,
    /// Priority (lower runs first; default 0). Only meaningful on a priority-enabled queue.
    pub priority: Option<i32>,
    /// Partition key (for a partitioned queue).
    pub queue_partition_key: Option<String>,
    /// Delay before the workflow becomes eligible to run (status `DELAYED` until then).
    pub delay: Option<std::time::Duration>,
    /// Override the application version.
    pub application_version: Option<String>,
}

pub(crate) struct DbosInner {
    pub pool: PgPool,
    pub schema: String,
    pub executor_id: String,
    pub application_version: String,
    pub application_id: Option<String>,
    pub registry: Arc<Registry>,
    pub workflow_tasks: TaskTracker,
    pub cancel: CancellationToken,
    pub poll_interval: Duration,
    /// In-memory per-(queue, partition) running counts for worker concurrency.
    pub worker_counts: WorkerCounts,
    /// Waiters for `recv` (keyed `"dest::topic"`) and `get_event` (keyed `"workflow::key"`).
    pub notifications_waiters: WaiterRegistry,
    pub events_waiters: WaiterRegistry,
    /// Waiters for stream readers (keyed `"workflow::key"`).
    pub streams_waiters: WaiterRegistry,
}

/// A launched DBOS runtime handle. Cheap to clone.
#[derive(Clone)]
pub struct Dbos {
    inner: Arc<DbosInner>,
}

impl Dbos {
    /// Start configuring a runtime.
    pub fn builder(config: Config) -> DbosBuilder {
        DbosBuilder::new(config)
    }

    /// Start a registered workflow and return a handle to its result.
    pub async fn run_workflow<P, R>(
        &self,
        name: &str,
        input: P,
        opts: WorkflowOptions,
    ) -> Result<WorkflowHandle<R>>
    where
        P: Serialize + Send,
    {
        let encoded = encode_input(&input, Format::Portable)?;
        start_workflow::<R>(
            self.inner.clone(),
            name,
            Some(encoded),
            opts,
            None,
            Format::Portable,
            false,
        )
        .await
    }

    /// Enqueue a registered workflow onto a durable queue. A background runner picks it up subject
    /// to the queue's concurrency / rate / priority controls. Returns a polling handle.
    pub async fn enqueue<P, R>(
        &self,
        queue_name: &str,
        workflow_name: &str,
        input: P,
        opts: EnqueueOptions,
    ) -> Result<WorkflowHandle<R>>
    where
        P: Serialize + Send,
    {
        let encoded = encode_input(&input, Format::Portable)?;
        enqueue_workflow::<R>(self.inner.clone(), queue_name, workflow_name, Some(encoded), opts).await
    }

    /// Get a polling handle to an existing workflow by id.
    pub fn retrieve_workflow<R>(&self, workflow_id: &str) -> WorkflowHandle<R> {
        WorkflowHandle::polling_from_inner(workflow_id.to_string(), &self.inner)
    }

    /// Re-run this executor's `PENDING` workflows (also run automatically at [`launch`]). Returns
    /// the ids that were re-launched.
    pub async fn recover_pending_workflows(&self) -> Result<Vec<String>> {
        let executor = self.inner.executor_id.clone();
        recover_pending_workflows(self.inner.clone(), &[executor]).await
    }

    // ---- Workflow management ----------------------------------------------------------------

    /// List workflows matching `filter`.
    pub async fn list_workflows(&self, filter: ListWorkflowsFilter) -> Result<Vec<WorkflowStatus>> {
        management::list_workflows(&self.inner.pool, &self.inner.schema, filter).await
    }

    /// List a workflow's recorded steps, in order.
    pub async fn list_workflow_steps(&self, workflow_id: &str) -> Result<Vec<StepInfo>> {
        management::get_workflow_steps(&self.inner.pool, &self.inner.schema, workflow_id).await
    }

    /// Cancel a workflow (move it to `CANCELLED`). Errors if it does not exist.
    pub async fn cancel_workflow(&self, workflow_id: &str) -> Result<()> {
        let ids = [workflow_id.to_string()];
        let found = management::cancel_workflows(&self.inner.pool, &self.inner.schema, &ids).await?;
        if found.is_empty() {
            return Err(DbosError::non_existent_workflow(workflow_id));
        }
        Ok(())
    }

    /// Cancel multiple workflows; missing/terminal ones are skipped. Returns the ids that existed.
    pub async fn cancel_workflows(&self, workflow_ids: &[String]) -> Result<Vec<String>> {
        management::cancel_workflows(&self.inner.pool, &self.inner.schema, workflow_ids).await
    }

    /// Resume a workflow: re-enqueue it so a runner re-executes it. Errors if it does not exist.
    pub async fn resume_workflow<R>(&self, workflow_id: &str) -> Result<WorkflowHandle<R>> {
        let ids = [workflow_id.to_string()];
        let found = management::resume_workflows(
            &self.inner.pool,
            &self.inner.schema,
            &ids,
            INTERNAL_QUEUE_NAME,
        )
        .await?;
        if found.is_empty() {
            return Err(DbosError::non_existent_workflow(workflow_id));
        }
        Ok(self.retrieve_workflow(workflow_id))
    }

    /// Fork a workflow from `start_step`, copying earlier steps. Returns a handle to the new
    /// (enqueued) workflow.
    pub async fn fork_workflow<R>(&self, input: ForkWorkflowInput) -> Result<WorkflowHandle<R>> {
        let new_id = management::fork_workflow(&self.inner.pool, &self.inner.schema, input).await?;
        Ok(self.retrieve_workflow(&new_id))
    }

    /// Garbage-collect old terminal workflows (never `PENDING`/`ENQUEUED`/`DELAYED`).
    pub async fn garbage_collect(
        &self,
        cutoff_epoch_ms: Option<i64>,
        rows_threshold: Option<i64>,
    ) -> Result<()> {
        management::garbage_collect(
            &self.inner.pool,
            &self.inner.schema,
            cutoff_epoch_ms,
            rows_threshold,
        )
        .await
    }

    // ---- Streams ----------------------------------------------------------------------------

    /// Read a durable stream into a vector, blocking until it is closed or its producing workflow
    /// finishes. Returns `(values, closed)`.
    pub async fn read_stream<T: DeserializeOwned>(
        &self,
        workflow_id: &str,
        key: &str,
    ) -> Result<(Vec<T>, bool)> {
        self.read_stream_from(workflow_id, key, 0, false).await
    }

    /// Read whatever is currently in a stream from `from_offset`, without blocking.
    pub async fn read_stream_snapshot<T: DeserializeOwned>(
        &self,
        workflow_id: &str,
        key: &str,
        from_offset: i32,
    ) -> Result<(Vec<T>, bool)> {
        self.read_stream_from(workflow_id, key, from_offset, true).await
    }

    async fn read_stream_from<T: DeserializeOwned>(
        &self,
        workflow_id: &str,
        key: &str,
        from_offset: i32,
        snapshot: bool,
    ) -> Result<(Vec<T>, bool)> {
        let payload = format!("{workflow_id}::{key}");
        let notify = self
            .inner
            .streams_waiters
            .entry(payload.clone())
            .or_insert_with(|| Arc::new(tokio::sync::Notify::new()))
            .clone();
        let _guard = WaiterGuard::new(&self.inner.streams_waiters, payload);
        crate::db::streams::read_stream_blocking::<T>(
            &self.inner.pool,
            &self.inner.schema,
            Some(&notify),
            workflow_id,
            key,
            from_offset,
            snapshot,
        )
        .await
    }

    /// Read a durable stream live: returns a receiver yielding each value as it is produced. The
    /// channel closes when the stream closes or the producing workflow finishes.
    pub fn read_stream_async<T: DeserializeOwned + Send + 'static>(
        &self,
        workflow_id: &str,
        key: &str,
    ) -> tokio::sync::mpsc::Receiver<Result<T>> {
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        let inner = self.inner.clone();
        let wf = workflow_id.to_string();
        let key = key.to_string();
        self.inner.workflow_tasks.spawn(async move {
            let payload = format!("{wf}::{key}");
            let notify = inner
                .streams_waiters
                .entry(payload.clone())
                .or_insert_with(|| Arc::new(tokio::sync::Notify::new()))
                .clone();
            let _guard = WaiterGuard::new(&inner.streams_waiters, payload);
            let mut current = 0i32;
            loop {
                let notified = notify.notified();
                let (entries, closed) = match crate::db::streams::read_stream_entries(
                    &inner.pool,
                    &inner.schema,
                    &wf,
                    &key,
                    current,
                )
                .await
                {
                    Ok(x) => x,
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                };
                let got = !entries.is_empty();
                for e in entries {
                    match crate::serialize::decode_value::<T>(
                        Some(&e.value),
                        e.serialization.as_deref(),
                    ) {
                        Ok(v) => {
                            current = e.offset + 1;
                            if tx.send(Ok(v)).await.is_err() {
                                return; // receiver dropped
                            }
                        }
                        Err(e) => {
                            let _ = tx.send(Err(e)).await;
                            return;
                        }
                    }
                }
                if closed {
                    return;
                }
                match crate::db::status::get_status(&inner.pool, &inner.schema, &wf).await {
                    Ok(None) => {
                        let _ = tx.send(Err(DbosError::non_existent_workflow(&wf))).await;
                        return;
                    }
                    Ok(Some(s)) if s != "PENDING" && s != "ENQUEUED" => return,
                    Ok(_) => {}
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                }
                if !got {
                    tokio::select! {
                        _ = notified => {}
                        _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                    }
                }
            }
        });
        rx
    }

    /// Stop background tasks and drain in-flight workflows (up to `timeout`), then close the pool.
    pub async fn shutdown(self, timeout: Duration) {
        self.inner.cancel.cancel();
        self.inner.workflow_tasks.close();
        let _ = tokio::time::timeout(timeout, self.inner.workflow_tasks.wait()).await;
        self.inner.pool.close().await;
    }
}

/// Builder for a [`Dbos`] runtime. Register all workflows, then [`launch`](DbosBuilder::launch).
pub struct DbosBuilder {
    config: Config,
    registry: HashMap<String, RegistryEntry>,
    queues: HashMap<String, WorkflowQueue>,
    schedules: Vec<(String, String)>,
    registration_error: Option<DbosError>,
}

impl DbosBuilder {
    pub fn new(config: Config) -> Self {
        DbosBuilder {
            config,
            registry: HashMap::new(),
            queues: HashMap::new(),
            schedules: Vec::new(),
            registration_error: None,
        }
    }

    /// Register a workflow to run on a cron schedule. The workflow receives a
    /// [`ScheduledWorkflowInput`] with the tick it is firing for. Supports 5-field cron and
    /// 6-field (seconds-first) cron.
    pub fn register_scheduled<F, Fut>(mut self, name: &str, cron: &str, f: F) -> Self
    where
        F: Fn(WorkflowContext, ScheduledWorkflowInput) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.schedules.push((name.to_string(), cron.to_string()));
        self.register_workflow(name, f)
    }

    /// Register a durable queue. A background runner is started for it at [`launch`](Self::launch).
    pub fn register_queue(mut self, queue: WorkflowQueue) -> Self {
        if self.queues.contains_key(&queue.name) {
            if self.registration_error.is_none() {
                self.registration_error = Some(DbosError::conflicting_registration(&queue.name));
            }
            return self;
        }
        self.queues.insert(queue.name.clone(), queue);
        self
    }

    /// Register a durable workflow under an explicit `name` (Rust has no reflection-derived name).
    /// The concrete input/output types are captured here so recovery can decode stored inputs.
    pub fn register_workflow<P, R, F, Fut>(self, name: &str, f: F) -> Self
    where
        P: DeserializeOwned + Serialize + Send + 'static,
        R: Serialize + DeserializeOwned + Send + 'static,
        F: Fn(WorkflowContext, P) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R>> + Send + 'static,
    {
        self.register_workflow_with_options(name, RegistrationOptions::default(), f)
    }

    /// Register a workflow with explicit [`RegistrationOptions`] (e.g. a custom `max_retries`).
    pub fn register_workflow_with_options<P, R, F, Fut>(
        mut self,
        name: &str,
        opts: RegistrationOptions,
        f: F,
    ) -> Self
    where
        P: DeserializeOwned + Serialize + Send + 'static,
        R: Serialize + DeserializeOwned + Send + 'static,
        F: Fn(WorkflowContext, P) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R>> + Send + 'static,
    {
        if self.registry.contains_key(name) {
            if self.registration_error.is_none() {
                self.registration_error = Some(DbosError::conflicting_registration(name));
            }
            return self;
        }
        let f = Arc::new(f);
        let handler: ErasedWorkflow = Arc::new(move |ctx, input, fmt| {
            let f = f.clone();
            let fut: BoxFuture<'static, Result<String>> = Box::pin(async move {
                let p: P = decode_input(input.as_deref(), Some(fmt.name()))?;
                let r: R = f(ctx, p).await?;
                encode_value(&r, fmt)
            });
            fut
        });
        self.registry.insert(
            name.to_string(),
            RegistryEntry {
                handler,
                max_retries: opts.max_retries,
                name: name.to_string(),
                class_name: None,
                config_name: None,
            },
        );
        self
    }

    /// Validate config, connect, migrate the system database, and start the runtime.
    pub async fn launch(mut self) -> Result<Dbos> {
        if let Some(e) = self.registration_error {
            return Err(e);
        }
        let pc = process_config(self.config)?;
        let conductor_cfg = conductor::config_from(
            pc.conductor_api_key.clone(),
            pc.conductor_url.clone(),
            &pc.app_name,
            pc.conductor_executor_metadata.clone(),
        );
        let pool = match &pc.system_db_pool {
            Some(p) => p.clone(),
            None => connect(pc.database_url.as_deref().unwrap_or_default(), 20).await?,
        };
        run_migrations(&pool, &pc.database_schema).await?;

        let inner = Arc::new(DbosInner {
            pool,
            schema: pc.database_schema,
            executor_id: pc.executor_id,
            application_version: pc.application_version,
            application_id: if pc.application_id.is_empty() {
                None
            } else {
                Some(pc.application_id)
            },
            registry: Arc::new(Registry(self.registry)),
            workflow_tasks: TaskTracker::new(),
            cancel: CancellationToken::new(),
            poll_interval: Duration::from_secs(1),
            worker_counts: WorkerCounts::new(),
            notifications_waiters: WaiterRegistry::new(),
            events_waiters: WaiterRegistry::new(),
            streams_waiters: WaiterRegistry::new(),
        });

        // Start the LISTEN/NOTIFY listener that wakes recv/get_event waiters.
        {
            let listener_inner = inner.clone();
            let token = inner.cancel.clone();
            inner
                .workflow_tasks
                .spawn(async move { run_listener(listener_inner, token).await });
        }

        // Recover this executor's interrupted workflows (re-enqueues queued ones).
        let executor = inner.executor_id.clone();
        recover_pending_workflows(inner.clone(), &[executor]).await?;

        // The internal queue always has a runner (resumed/forked workflows land here).
        self.queues
            .entry(INTERNAL_QUEUE_NAME.to_string())
            .or_insert_with(|| WorkflowQueue::new(INTERNAL_QUEUE_NAME));

        // Start a background runner per registered queue.
        for queue in self.queues.into_values() {
            let runner_inner = inner.clone();
            let token = inner.cancel.clone();
            inner
                .workflow_tasks
                .spawn(async move { run_queue(runner_inner, queue, token).await });
        }

        // Start a cron loop per scheduled workflow.
        for (name, cron) in self.schedules {
            let sched_inner = inner.clone();
            let token = inner.cancel.clone();
            inner
                .workflow_tasks
                .spawn(async move { run_scheduler(sched_inner, name, cron, token).await });
        }

        // Connect to the conductor control plane, if configured.
        if let Some(cc) = conductor_cfg {
            let cond_inner = inner.clone();
            let token = inner.cancel.clone();
            inner
                .workflow_tasks
                .spawn(async move { conductor::run_conductor(cond_inner, cc, token).await });
        }

        Ok(Dbos { inner })
    }
}
