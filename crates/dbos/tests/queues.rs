//! M4 acceptance tests: durable queues against a live Postgres.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use dbos::{
    Config, Dbos, DbosError, DbosErrorCode, EnqueueOptions, WorkflowContext, WorkflowQueue,
    WorkflowStatusType,
};
use sqlx::PgPool;

fn test_url() -> String {
    std::env::var("DBOS_SYSTEM_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:dbos@localhost:5439/dbos".to_string())
}

fn unique_schema(prefix: &str) -> String {
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    format!("test_{prefix}_{}_{n}", std::process::id())
}

fn config(schema: &str) -> Config {
    Config {
        app_name: "test".to_string(),
        database_url: Some(test_url()),
        database_schema: Some(schema.to_string()),
        ..Default::default()
    }
}

async fn pool() -> PgPool {
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&test_url())
        .await
        .unwrap()
}

fn fast_queue(name: &str) -> WorkflowQueue {
    WorkflowQueue::new(name).base_polling_interval(Duration::from_millis(10))
}

#[tokio::test]
async fn enqueue_runs_and_returns_results() {
    let schema = unique_schema("q_basic");
    let dbos = Dbos::builder(config(&schema))
        .register_workflow("double", |_ctx: WorkflowContext, n: i64| async move {
            Ok::<_, DbosError>(n * 2)
        })
        .register_queue(fast_queue("q"))
        .launch()
        .await
        .unwrap();

    let mut handles = Vec::new();
    for i in 0..5 {
        handles.push(
            dbos.enqueue::<_, i64>("q", "double", i, EnqueueOptions::default())
                .await
                .unwrap(),
        );
    }
    for (i, h) in handles.iter().enumerate() {
        assert_eq!(h.get_result().await.unwrap(), (i as i64) * 2);
    }
    dbos.shutdown(Duration::from_secs(2)).await;
}

/// Build a workflow that tracks peak concurrency via shared atomics.
fn concurrency_tracker(
    running: Arc<AtomicUsize>,
    max_seen: Arc<AtomicUsize>,
) -> impl Fn(WorkflowContext, ()) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), DbosError>> + Send>>
       + Send
       + Sync
       + 'static {
    move |_ctx, _: ()| {
        let running = running.clone();
        let max_seen = max_seen.clone();
        Box::pin(async move {
            let cur = running.fetch_add(1, Ordering::SeqCst) + 1;
            max_seen.fetch_max(cur, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(60)).await;
            running.fetch_sub(1, Ordering::SeqCst);
            Ok::<_, DbosError>(())
        })
    }
}

#[tokio::test]
async fn global_concurrency_limits_parallelism() {
    let schema = unique_schema("q_gc");
    let running = Arc::new(AtomicUsize::new(0));
    let max_seen = Arc::new(AtomicUsize::new(0));
    let dbos = Dbos::builder(config(&schema))
        .register_workflow("slow", concurrency_tracker(running.clone(), max_seen.clone()))
        .register_queue(fast_queue("q").global_concurrency(1))
        .launch()
        .await
        .unwrap();

    let mut handles = Vec::new();
    for _ in 0..6 {
        handles.push(
            dbos.enqueue::<_, ()>("q", "slow", (), EnqueueOptions::default())
                .await
                .unwrap(),
        );
    }
    for h in &handles {
        h.get_result().await.unwrap();
    }
    assert_eq!(max_seen.load(Ordering::SeqCst), 1, "global concurrency 1 serializes");
    dbos.shutdown(Duration::from_secs(3)).await;
}

#[tokio::test]
async fn worker_concurrency_limits_parallelism() {
    let schema = unique_schema("q_wc");
    let running = Arc::new(AtomicUsize::new(0));
    let max_seen = Arc::new(AtomicUsize::new(0));
    let dbos = Dbos::builder(config(&schema))
        .register_workflow("slow", concurrency_tracker(running.clone(), max_seen.clone()))
        .register_queue(fast_queue("q").worker_concurrency(2))
        .launch()
        .await
        .unwrap();

    let mut handles = Vec::new();
    for _ in 0..6 {
        handles.push(
            dbos.enqueue::<_, ()>("q", "slow", (), EnqueueOptions::default())
                .await
                .unwrap(),
        );
    }
    for h in &handles {
        h.get_result().await.unwrap();
    }
    assert_eq!(max_seen.load(Ordering::SeqCst), 2, "worker concurrency caps at 2");
    dbos.shutdown(Duration::from_secs(3)).await;
}

#[tokio::test]
async fn priority_ordering() {
    let schema = unique_schema("q_prio");
    let order = Arc::new(Mutex::new(Vec::<i64>::new()));
    let order2 = order.clone();
    let dbos = Dbos::builder(config(&schema))
        .register_workflow("record", move |_ctx: WorkflowContext, id: i64| {
            let order = order2.clone();
            async move {
                order.lock().unwrap().push(id);
                Ok::<_, DbosError>(())
            }
        })
        // global concurrency 1 → exactly one runs at a time, in priority order.
        .register_queue(
            WorkflowQueue::new("q")
                .global_concurrency(1)
                .priority_enabled()
                .base_polling_interval(Duration::from_millis(40)),
        )
        .launch()
        .await
        .unwrap();

    // Enqueue: 0 (default pri 0), 1..5 (pri 1..5), 6 & 7 (default pri 0).
    let pris: [(i64, Option<i32>); 8] = [
        (0, None),
        (1, Some(1)),
        (2, Some(2)),
        (3, Some(3)),
        (4, Some(4)),
        (5, Some(5)),
        (6, None),
        (7, None),
    ];
    let mut handles = Vec::new();
    for (id, pri) in pris {
        handles.push(
            dbos.enqueue::<_, ()>(
                "q",
                "record",
                id,
                EnqueueOptions {
                    priority: pri,
                    ..Default::default()
                },
            )
            .await
            .unwrap(),
        );
    }
    for h in &handles {
        h.get_result().await.unwrap();
    }
    // priority ASC, then FIFO by created_at.
    assert_eq!(*order.lock().unwrap(), vec![0, 6, 7, 1, 2, 3, 4, 5]);
    dbos.shutdown(Duration::from_secs(3)).await;
}

#[tokio::test]
async fn deduplication_rejects_duplicate() {
    let schema = unique_schema("q_dedup");
    let dbos = Dbos::builder(config(&schema))
        .register_workflow("noop", |_ctx: WorkflowContext, _: ()| async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok::<_, DbosError>(())
        })
        // Default 1s polling keeps the first workflow ENQUEUED long enough to clash.
        .register_queue(WorkflowQueue::new("q"))
        .launch()
        .await
        .unwrap();

    let opts = EnqueueOptions {
        deduplication_id: Some("dup".to_string()),
        ..Default::default()
    };
    let h1 = dbos
        .enqueue::<_, ()>("q", "noop", (), opts.clone())
        .await
        .unwrap();
    let err = dbos
        .enqueue::<_, ()>("q", "noop", (), opts)
        .await
        .unwrap_err();
    assert_eq!(
        err.code(),
        DbosErrorCode::QueueDeduplicated as i32,
        "got: {err}"
    );
    h1.get_result().await.unwrap();
    dbos.shutdown(Duration::from_secs(3)).await;
}

#[tokio::test]
async fn delayed_execution() {
    let schema = unique_schema("q_delay");
    let dbos = Dbos::builder(config(&schema))
        .register_workflow("echo", |_ctx: WorkflowContext, n: i64| async move {
            Ok::<_, DbosError>(n)
        })
        .register_queue(fast_queue("q"))
        .launch()
        .await
        .unwrap();

    let start = Instant::now();
    let h = dbos
        .enqueue::<_, i64>(
            "q",
            "echo",
            7,
            EnqueueOptions {
                delay: Some(Duration::from_millis(300)),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(h.get_status().await.unwrap(), Some(WorkflowStatusType::Delayed));
    assert_eq!(h.get_result().await.unwrap(), 7);
    assert!(
        start.elapsed() >= Duration::from_millis(250),
        "ran before its delay elapsed: {:?}",
        start.elapsed()
    );
    dbos.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test]
async fn rate_limiter_throttles_into_waves() {
    let schema = unique_schema("q_rate");
    let starts = Arc::new(Mutex::new(Vec::<Instant>::new()));
    let starts2 = starts.clone();
    let dbos = Dbos::builder(config(&schema))
        .register_workflow("tick", move |_ctx: WorkflowContext, _: ()| {
            let starts = starts2.clone();
            async move {
                starts.lock().unwrap().push(Instant::now());
                Ok::<_, DbosError>(())
            }
        })
        // At most 2 starts per 400ms.
        .register_queue(fast_queue("q").rate_limit(2, Duration::from_millis(400)))
        .launch()
        .await
        .unwrap();

    let mut handles = Vec::new();
    for _ in 0..4 {
        handles.push(
            dbos.enqueue::<_, ()>("q", "tick", (), EnqueueOptions::default())
                .await
                .unwrap(),
        );
    }
    for h in &handles {
        h.get_result().await.unwrap();
    }
    let mut times = starts.lock().unwrap().clone();
    times.sort();
    assert_eq!(times.len(), 4);
    // The third start (second wave) is throttled ~one period after the first.
    let gap = times[2].duration_since(times[0]);
    assert!(
        gap >= Duration::from_millis(350),
        "second wave was not throttled: {gap:?}"
    );
    dbos.shutdown(Duration::from_secs(3)).await;
}

#[tokio::test]
async fn queue_recovery_reenqueues_claimed_workflow() {
    let schema = unique_schema("q_recover");
    let ran = Arc::new(AtomicUsize::new(0));
    let ran2 = ran.clone();
    let dbos = Dbos::builder(config(&schema))
        .register_workflow("qtask", move |_ctx: WorkflowContext, _: ()| {
            let ran = ran2.clone();
            async move {
                ran.fetch_add(1, Ordering::SeqCst);
                Ok::<_, DbosError>("done".to_string())
            }
        })
        .register_queue(fast_queue("q"))
        .launch()
        .await
        .unwrap();

    // Run one workflow to completion to learn this executor's id + version.
    let done = dbos
        .enqueue::<_, String>("q", "qtask", (), EnqueueOptions::default())
        .await
        .unwrap();
    assert_eq!(done.get_result().await.unwrap(), "done");
    let (executor, appver): (String, String) = sqlx::query_as(&format!(
        "SELECT executor_id, application_version FROM \"{schema}\".workflow_status
         WHERE workflow_uuid = $1"
    ))
    .bind(done.workflow_id())
    .fetch_one(&pool().await)
    .await
    .unwrap();

    // Forge a queued workflow stuck in PENDING (as if a runner claimed it then crashed).
    let stuck = "stuck-task";
    sqlx::query(&format!(
        "INSERT INTO \"{schema}\".workflow_status
            (workflow_uuid, status, name, queue_name, executor_id, application_version,
             recovery_attempts, serialization, inputs, started_at_epoch_ms)
         VALUES ($1, 'PENDING', 'qtask', 'q', $2, $3, 1, 'portable_json',
                 '{{\"positionalArgs\":[null],\"namedArgs\":{{}}}}', 1)"
    ))
    .bind(stuck)
    .bind(&executor)
    .bind(&appver)
    .execute(&pool().await)
    .await
    .unwrap();

    // Recovery re-enqueues it (PENDING→ENQUEUED); the runner then picks it up and runs it.
    dbos.recover_pending_workflows().await.unwrap();
    let result = dbos
        .retrieve_workflow::<String>(stuck)
        .get_result()
        .await
        .unwrap();
    assert_eq!(result, "done");
    assert_eq!(ran.load(Ordering::SeqCst), 2, "the stuck workflow was run once after recovery");
    dbos.shutdown(Duration::from_secs(3)).await;
}
