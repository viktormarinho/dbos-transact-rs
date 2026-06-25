//! Group B.7: two executors racing the dequeue of a single queued workflow → exactly one runs it.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use dbos::{Config, Dbos, DbosError, EnqueueOptions, WorkflowContext, WorkflowQueue};

fn test_url() -> String {
    std::env::var("DBOS_SYSTEM_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:dbos@localhost:5439/dbos".to_string())
}

fn unique_schema(prefix: &str) -> String {
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    format!("test_{prefix}_{}_{n}", std::process::id())
}

async fn launch_executor(schema: &str, executor_id: &str, runs: Arc<AtomicU64>) -> Dbos {
    Dbos::builder(Config {
        app_name: "test".to_string(),
        database_url: Some(test_url()),
        database_schema: Some(schema.to_string()),
        executor_id: Some(executor_id.to_string()),
        ..Default::default()
    })
    .register_workflow("task", move |_: WorkflowContext, n: i64| {
        let runs = runs.clone();
        async move {
            runs.fetch_add(1, Ordering::SeqCst);
            Ok::<_, DbosError>(n * 2)
        }
    })
    .register_queue(WorkflowQueue::new("q").base_polling_interval(Duration::from_millis(10)))
    .launch()
    .await
    .unwrap()
}

#[tokio::test]
async fn two_executors_run_one_queued_workflow_exactly_once() {
    let schema = unique_schema("twoexec");
    let runs = Arc::new(AtomicU64::new(0));

    // Two executors on the SAME schema + queue, both with a runner polling for work.
    let a = launch_executor(&schema, "exec-a", runs.clone()).await;
    let b = launch_executor(&schema, "exec-b", runs.clone()).await;

    // Enqueue a single workflow; whichever runner claims the row first runs it.
    let h = a
        .enqueue::<_, i64>(
            "q",
            "task",
            21,
            EnqueueOptions { workflow_id: Some("queued-1".into()), ..Default::default() },
        )
        .await
        .unwrap();
    assert_eq!(h.get_result().await.unwrap(), 42);

    // Give the losing runner several poll cycles to (not) double-execute it.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        runs.load(Ordering::SeqCst),
        1,
        "exactly one executor ran the queued workflow"
    );

    a.shutdown(Duration::from_secs(2)).await;
    b.shutdown(Duration::from_secs(2)).await;
}
