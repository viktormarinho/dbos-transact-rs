//! M3 acceptance tests: crash recovery + dead-letter queue against a live Postgres.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use dbos::{
    Config, Dbos, DbosBuilder, DbosError, DbosErrorCode, RegistrationOptions, WorkflowContext,
    WorkflowOptions,
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

async fn recovery_attempts(schema: &str, id: &str) -> i64 {
    sqlx::query_scalar::<_, i64>(&format!(
        "SELECT recovery_attempts FROM \"{schema}\".workflow_status WHERE workflow_uuid = $1"
    ))
    .bind(id)
    .fetch_one(&pool().await)
    .await
    .unwrap()
}

async fn workflow_status(schema: &str, id: &str) -> String {
    sqlx::query_scalar::<_, String>(&format!(
        "SELECT status FROM \"{schema}\".workflow_status WHERE workflow_uuid = $1"
    ))
    .bind(id)
    .fetch_one(&pool().await)
    .await
    .unwrap()
}

/// Wait until a named step has been checkpointed for a workflow (up to ~3s).
async fn wait_for_step(schema: &str, id: &str, step: &str) {
    let p = pool().await;
    for _ in 0..60 {
        let n: i64 = sqlx::query_scalar(&format!(
            "SELECT count(*) FROM \"{schema}\".operation_outputs
             WHERE workflow_uuid = $1 AND function_name = $2"
        ))
        .bind(id)
        .bind(step)
        .fetch_one(&p)
        .await
        .unwrap();
        if n > 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("step {step} for {id} was never recorded");
}

#[tokio::test]
async fn crash_recovery_replays_steps() {
    let schema = unique_schema("recover");
    let side_effects = Arc::new(AtomicU64::new(0));
    // false → the first execution hangs (simulating a crash); true → recovery completes.
    let release = Arc::new(AtomicBool::new(false));

    // A fresh registration (with its own Arc clones) for each "process".
    let make_build = |se: Arc<AtomicU64>, rel: Arc<AtomicBool>| {
        move |b: DbosBuilder| {
            b.register_workflow("recoverable", move |ctx: WorkflowContext, _: ()| {
                let se = se.clone();
                let rel = rel.clone();
                async move {
                    // Durable step with an observable side effect: must run exactly once.
                    let se2 = se.clone();
                    ctx.run_step("side_effect", move |_| async move {
                        se2.fetch_add(1, Ordering::SeqCst);
                        Ok::<_, DbosError>(())
                    })
                    .await?;
                    if !rel.load(Ordering::SeqCst) {
                        std::future::pending::<()>().await; // "crash": never completes
                    }
                    Ok::<_, DbosError>("done".to_string())
                }
            })
        }
    };

    // Process 1: start the workflow; it records the step, then hangs (stays PENDING).
    let dbos1 = launch(&schema, make_build(side_effects.clone(), release.clone())).await;
    let wf_id = "recover-target".to_string();
    let _ = dbos1
        .run_workflow::<_, String>(
            "recoverable",
            (),
            WorkflowOptions {
                workflow_id: Some(wf_id.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    wait_for_step(&schema, &wf_id, "side_effect").await;
    dbos1.shutdown(Duration::from_millis(100)).await; // "crash" — drop without completing

    // Process 2: allow completion and re-launch. Recovery runs on launch.
    release.store(true, Ordering::SeqCst);
    let dbos2 = launch(&schema, make_build(side_effects.clone(), release.clone())).await;
    let result = dbos2
        .retrieve_workflow::<String>(&wf_id)
        .get_result()
        .await
        .unwrap();

    assert_eq!(result, "done");
    assert_eq!(
        side_effects.load(Ordering::SeqCst),
        1,
        "the step side effect ran once (replayed, not re-executed, on recovery)"
    );
    assert_eq!(recovery_attempts(&schema, &wf_id).await, 2);
    dbos2.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test]
async fn dead_letter_queue_after_max_retries() {
    let schema = unique_schema("dlq");
    let dbos = launch(&schema, |b| {
        b.register_workflow_with_options(
            "dlq_wf",
            RegistrationOptions { max_retries: 2 },
            |_ctx: WorkflowContext, _: ()| async move { Ok::<_, DbosError>("ok".to_string()) },
        )
    })
    .await;

    // Run once to completion to learn this executor's id + application version.
    let done = dbos
        .run_workflow::<_, String>("dlq_wf", (), WorkflowOptions::default())
        .await
        .unwrap();
    assert_eq!(done.get_result().await.unwrap(), "ok");
    let (executor, appver): (String, String) = sqlx::query_as(&format!(
        "SELECT executor_id, application_version FROM \"{schema}\".workflow_status
         WHERE workflow_uuid = $1"
    ))
    .bind(done.workflow_id())
    .fetch_one(&pool().await)
    .await
    .unwrap();

    // Forge a PENDING row whose recovery_attempts already sit at the limit (max_retries + 1).
    let dlq_id = "dlq-target";
    sqlx::query(&format!(
        "INSERT INTO \"{schema}\".workflow_status
            (workflow_uuid, status, name, executor_id, application_version, recovery_attempts,
             serialization, inputs)
         VALUES ($1, 'PENDING', 'dlq_wf', $2, $3, 3, 'portable_json', 'null')"
    ))
    .bind(dlq_id)
    .bind(&executor)
    .bind(&appver)
    .execute(&pool().await)
    .await
    .unwrap();

    // Recovery re-runs it → attempts 3→4 > max(2)+1 → dead-letter.
    dbos.recover_pending_workflows().await.unwrap();

    assert_eq!(
        workflow_status(&schema, dlq_id).await,
        "MAX_RECOVERY_ATTEMPTS_EXCEEDED"
    );
    let err = dbos
        .retrieve_workflow::<String>(dlq_id)
        .get_result()
        .await
        .unwrap_err();
    assert_eq!(
        err.code(),
        DbosErrorCode::DeadLetterQueue as i32,
        "got: {err}"
    );
    dbos.shutdown(Duration::from_secs(2)).await;
}
