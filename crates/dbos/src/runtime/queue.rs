//! Durable queues: the [`WorkflowQueue`] config and the per-queue background runner that dequeues
//! and dispatches workflows. Ports Go `queue.go`'s runner.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use super::context::{AuthIdentity, WorkflowContext, WorkflowState};
use super::DbosInner;
use crate::db::queue::{
    dequeue_workflows, get_queue_partitions, transition_delayed_workflows, DequeueInput,
    DequeuedWorkflow,
};
use crate::db::status::{update_workflow_outcome, WorkflowStatusType};
use crate::serialize::{serialize_workflow_error, Format};

const DEFAULT_MAX_TASKS_PER_ITERATION: u32 = 100;
const DEFAULT_BASE_POLLING: Duration = Duration::from_secs(1);
const DEFAULT_MAX_POLLING: Duration = Duration::from_secs(120);

/// A rate limit: at most `limit` workflows may *start* per `period`.
#[derive(Debug, Clone)]
pub struct RateLimiter {
    pub limit: u32,
    pub period: Duration,
}

/// A durable queue. Enqueued workflows are picked up by a background runner subject to the
/// configured concurrency / rate / priority controls.
#[derive(Debug, Clone)]
pub struct WorkflowQueue {
    pub name: String,
    /// Max concurrent workflows per executor (`None` = unlimited).
    pub worker_concurrency: Option<u32>,
    /// Max concurrent (`PENDING`) workflows across all executors (`None` = unlimited).
    pub global_concurrency: Option<u32>,
    pub priority_enabled: bool,
    pub rate_limit: Option<RateLimiter>,
    pub max_tasks_per_iteration: u32,
    pub partition_queue: bool,
    pub base_polling: Duration,
    pub max_polling: Duration,
}

impl WorkflowQueue {
    pub fn new(name: impl Into<String>) -> Self {
        WorkflowQueue {
            name: name.into(),
            worker_concurrency: None,
            global_concurrency: None,
            priority_enabled: false,
            rate_limit: None,
            max_tasks_per_iteration: DEFAULT_MAX_TASKS_PER_ITERATION,
            partition_queue: false,
            base_polling: DEFAULT_BASE_POLLING,
            max_polling: DEFAULT_MAX_POLLING,
        }
    }

    pub fn worker_concurrency(mut self, n: u32) -> Self {
        self.worker_concurrency = Some(n);
        self
    }
    pub fn global_concurrency(mut self, n: u32) -> Self {
        self.global_concurrency = Some(n);
        self
    }
    pub fn priority_enabled(mut self) -> Self {
        self.priority_enabled = true;
        self
    }
    pub fn rate_limit(mut self, limit: u32, period: Duration) -> Self {
        self.rate_limit = Some(RateLimiter { limit, period });
        self
    }
    pub fn partitioned(mut self) -> Self {
        self.partition_queue = true;
        self
    }
    pub fn max_tasks_per_iteration(mut self, n: u32) -> Self {
        self.max_tasks_per_iteration = n;
        self
    }
    pub fn base_polling_interval(mut self, d: Duration) -> Self {
        self.base_polling = d;
        self
    }
    pub fn max_polling_interval(mut self, d: Duration) -> Self {
        self.max_polling = d;
        self
    }
}

/// In-memory per-(queue, partition) running counts, used for worker-concurrency without a DB query.
pub(crate) type WorkerCounts = dashmap::DashMap<(String, String), AtomicUsize>;

fn count_key(queue: &str, partition: Option<&str>) -> (String, String) {
    (queue.to_string(), partition.unwrap_or("").to_string())
}

fn worker_count(inner: &DbosInner, queue: &str, partition: Option<&str>) -> usize {
    inner
        .worker_counts
        .get(&count_key(queue, partition))
        .map(|c| c.load(Ordering::SeqCst))
        .unwrap_or(0)
}

fn inc_worker(inner: &DbosInner, key: &(String, String)) {
    inner
        .worker_counts
        .entry(key.clone())
        .or_insert_with(|| AtomicUsize::new(0))
        .fetch_add(1, Ordering::SeqCst);
}

fn dec_worker(inner: &DbosInner, key: &(String, String)) {
    if let Some(c) = inner.worker_counts.get(key) {
        c.fetch_sub(1, Ordering::SeqCst);
    }
}

/// The per-queue poll loop: promote delayed work, dequeue per partition, dispatch, and adapt the
/// polling interval. Stops when `token` is cancelled.
pub(crate) async fn run_queue(
    inner: Arc<DbosInner>,
    queue: WorkflowQueue,
    token: CancellationToken,
) {
    let mut interval = queue.base_polling;
    loop {
        let jitter = 0.95 + rand::random::<f64>() * 0.10;
        tokio::select! {
            _ = token.cancelled() => break,
            _ = tokio::time::sleep(interval.mul_f64(jitter)) => {}
        }

        if let Err(e) = transition_delayed_workflows(&inner.pool, &inner.schema).await {
            tracing::warn!(queue = %queue.name, error = %e, "transition_delayed_workflows failed");
        }

        let partitions: Vec<Option<String>> = if queue.partition_queue {
            match get_queue_partitions(&inner.pool, &inner.schema, &queue.name).await {
                Ok(keys) => keys.into_iter().map(Some).collect(),
                Err(e) => {
                    tracing::warn!(queue = %queue.name, error = %e, "get_queue_partitions failed");
                    interval = (interval.mul_f64(2.0)).min(queue.max_polling);
                    continue;
                }
            }
        } else {
            vec![None]
        };

        let mut had_backoff = false;
        for partition in partitions {
            let local = worker_count(&inner, &queue.name, partition.as_deref());
            let dq = DequeueInput {
                queue_name: &queue.name,
                executor_id: &inner.executor_id,
                application_version: &inner.application_version,
                partition_key: partition.as_deref(),
                local_running_count: local,
                worker_concurrency: queue.worker_concurrency,
                global_concurrency: queue.global_concurrency,
                max_tasks_per_iteration: queue.max_tasks_per_iteration,
                rate_limit: queue.rate_limit.as_ref().map(|r| (r.limit, r.period)),
            };
            match dequeue_workflows(&inner.pool, &inner.schema, dq).await {
                Ok(workflows) => {
                    for wf in workflows {
                        dispatch_dequeued(inner.clone(), queue.name.clone(), partition.clone(), wf);
                    }
                }
                Err(e) if e.is_db_contention() => had_backoff = true,
                Err(e) => tracing::error!(queue = %queue.name, error = %e, "dequeue failed"),
            }
        }

        interval = if had_backoff {
            (interval.mul_f64(2.0)).min(queue.max_polling)
        } else {
            (interval.mul_f64(0.9)).max(queue.base_polling)
        };
    }
}

/// Run a claimed (already `PENDING`) workflow's body and record its outcome, maintaining the
/// worker-concurrency counter.
fn dispatch_dequeued(
    inner: Arc<DbosInner>,
    queue_name: String,
    partition: Option<String>,
    wf: DequeuedWorkflow,
) {
    let handler = match inner.registry.get(&wf.name) {
        Some(e) => e.handler.clone(),
        None => {
            tracing::error!(name = %wf.name, workflow_id = %wf.id, "dequeued workflow not registered");
            return;
        }
    };

    let key = count_key(&queue_name, partition.as_deref());
    inc_worker(&inner, &key);

    let fmt = Format::from_name(wf.serialization.as_deref());
    let state = Arc::new(WorkflowState::new(wf.id.clone(), AuthIdentity::default()));
    let ctx = WorkflowContext {
        inner: inner.clone(),
        state,
        within_step: false,
    };
    let inner2 = inner.clone();
    let id = wf.id.clone();
    let input = wf.inputs;
    let span_name = wf.name.clone();
    let executor = inner.executor_id.clone();
    let app_ver = inner.application_version.clone();
    inner.workflow_tasks.spawn(async move {
        let span = tracing::info_span!(
            "dbos.workflow",
            "otel.name" = %span_name,
            "otel.status_code" = tracing::field::Empty,
            operationUUID = %id,
            operationType = "workflow",
            operationName = %span_name,
            executorID = %executor,
            applicationVersion = %app_ver,
            "dbos.queue.name" = %queue_name,
        );
        let result = handler(ctx, input, fmt).instrument(span.clone()).await;
        span.record(
            "otel.status_code",
            if result.is_ok() { "OK" } else { "ERROR" },
        );
        let (status, output, err_str) = match &result {
            Ok(out) => (WorkflowStatusType::Success, Some(out.clone()), None),
            Err(e) => (
                WorkflowStatusType::Error,
                None,
                Some(serialize_workflow_error(&e.to_string(), None, fmt)),
            ),
        };
        let _ = update_workflow_outcome(
            &inner2.pool,
            &inner2.schema,
            &id,
            status,
            output.as_deref(),
            err_str.as_deref(),
        )
        .await;
        dec_worker(&inner2, &key);
    });
}
