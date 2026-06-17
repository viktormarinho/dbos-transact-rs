//! M6 acceptance tests: durable sleep + cron scheduler against a live Postgres.

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use dbos::{
    Config, Dbos, DbosBuilder, DbosError, ScheduledWorkflowInput, WorkflowContext, WorkflowOptions,
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

async fn launch(schema: &str, build: impl FnOnce(DbosBuilder) -> DbosBuilder) -> Dbos {
    build(Dbos::builder(config(schema)))
        .launch()
        .await
        .expect("launch")
}

async fn pool() -> PgPool {
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&test_url())
        .await
        .unwrap()
}

#[tokio::test]
async fn durable_sleep_waits() {
    let schema = unique_schema("sleep");
    let dbos = launch(&schema, |b| {
        b.register_workflow("napper", |ctx: WorkflowContext, _: ()| async move {
            ctx.sleep(Duration::from_millis(300)).await?;
            Ok::<_, DbosError>("awake".to_string())
        })
    })
    .await;

    let start = Instant::now();
    let h = dbos
        .run_workflow::<_, String>("napper", (), WorkflowOptions { workflow_id: Some("nap-1".into()), ..Default::default() })
        .await
        .unwrap();
    assert_eq!(h.get_result().await.unwrap(), "awake");
    assert!(
        start.elapsed() >= Duration::from_millis(250),
        "did not actually sleep: {:?}",
        start.elapsed()
    );

    // The sleep is recorded as a DBOS.sleep step.
    let n: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*) FROM \"{schema}\".operation_outputs
         WHERE workflow_uuid = 'nap-1' AND function_name = 'DBOS.sleep'"
    ))
    .fetch_one(&pool().await)
    .await
    .unwrap();
    assert_eq!(n, 1, "sleep recorded exactly one DBOS.sleep step");
    dbos.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test]
async fn cron_scheduler_fires_each_tick() {
    let schema = unique_schema("cron");
    let count = Arc::new(AtomicU64::new(0));
    let ticks = Arc::new(Mutex::new(Vec::<i64>::new()));
    let count2 = count.clone();
    let ticks2 = ticks.clone();

    let dbos = launch(&schema, move |b| {
        b.register_scheduled("every_second", "* * * * * *", move |_ctx: WorkflowContext, input: ScheduledWorkflowInput| {
            let count = count2.clone();
            let ticks = ticks2.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                ticks.lock().unwrap().push(input.scheduled_time.timestamp());
                Ok::<_, DbosError>(())
            }
        })
    })
    .await;

    tokio::time::sleep(Duration::from_millis(3200)).await;
    dbos.shutdown(Duration::from_secs(2)).await;

    let fired = count.load(Ordering::SeqCst);
    assert!(fired >= 2, "expected the schedule to fire at least twice, got {fired}");

    // Each tick has a distinct scheduled time (exactly-once per tick).
    let t = ticks.lock().unwrap().clone();
    let distinct: HashSet<i64> = t.iter().copied().collect();
    assert_eq!(distinct.len(), t.len(), "each scheduled tick is distinct: {t:?}");
}
