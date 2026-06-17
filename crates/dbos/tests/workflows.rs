//! M2 acceptance tests: durable workflows + steps against a live Postgres.
//!
//! Each test runs in its own schema so they can run in parallel. Set `DBOS_SYSTEM_DATABASE_URL`
//! to point at a different Postgres (defaults to the local Docker container on port 5439).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use dbos::{Config, Dbos, DbosBuilder, DbosErrorCode, StepOptions, WorkflowContext, WorkflowOptions};

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

/// Query the recorded steps for a workflow, ordered by function id.
async fn recorded_steps(schema: &str, workflow_id: &str) -> Vec<(i32, String)> {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&test_url())
        .await
        .unwrap();
    let rows: Vec<(i32, String)> = sqlx::query_as(&format!(
        "SELECT function_id, function_name FROM \"{schema}\".operation_outputs
         WHERE workflow_uuid = $1 ORDER BY function_id"
    ))
    .bind(workflow_id)
    .fetch_all(&pool)
    .await
    .unwrap();
    rows
}

#[tokio::test]
async fn basic_workflow_with_steps() {
    let schema = unique_schema("basic");
    let dbos = launch(&schema, |b| {
        b.register_workflow("greet", |ctx: WorkflowContext, name: String| async move {
            let a = ctx
                .run_step("first", |_| async { Ok("a".to_string()) })
                .await?;
            let b = ctx
                .run_step("second", |_| async { Ok("b".to_string()) })
                .await?;
            Ok::<_, dbos::DbosError>(format!("{name}-{a}{b}"))
        })
    })
    .await;

    let handle = dbos
        .run_workflow::<_, String>("greet", "x".to_string(), WorkflowOptions::default())
        .await
        .unwrap();
    assert_eq!(handle.get_result().await.unwrap(), "x-ab");

    // Steps are recorded with 0-based ids and the exact names given.
    let steps = recorded_steps(&schema, handle.workflow_id()).await;
    assert_eq!(
        steps,
        vec![(0, "first".to_string()), (1, "second".to_string())]
    );

    dbos.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test]
async fn same_workflow_id_runs_body_once() {
    let schema = unique_schema("idem");
    let body_runs = Arc::new(AtomicU64::new(0));
    let counter = body_runs.clone();

    let dbos = launch(&schema, move |b| {
        b.register_workflow("count", move |ctx: WorkflowContext, _: ()| {
            let counter = counter.clone();
            async move {
                // Increment OUTSIDE a step, so a second body execution would be observable.
                counter.fetch_add(1, Ordering::SeqCst);
                ctx.run_step("noop", |_| async { Ok::<_, dbos::DbosError>(()) })
                    .await?;
                Ok::<_, dbos::DbosError>("done".to_string())
            }
        })
    })
    .await;

    let opts = WorkflowOptions {
        workflow_id: Some("fixed-id".to_string()),
        ..Default::default()
    };
    let h1 = dbos
        .run_workflow::<_, String>("count", (), opts.clone())
        .await
        .unwrap();
    assert_eq!(h1.get_result().await.unwrap(), "done");

    // Second call with the same id must NOT re-run the body — it polls for the existing result.
    let h2 = dbos
        .run_workflow::<_, String>("count", (), opts)
        .await
        .unwrap();
    assert_eq!(h2.get_result().await.unwrap(), "done");

    assert_eq!(body_runs.load(Ordering::SeqCst), 1, "body ran exactly once");
    dbos.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test]
async fn step_retries_then_succeeds() {
    let schema = unique_schema("retry_ok");
    let attempts = Arc::new(AtomicU64::new(0));
    let counter = attempts.clone();

    let dbos = launch(&schema, move |b| {
        b.register_workflow("flaky", move |ctx: WorkflowContext, _: ()| {
            let counter = counter.clone();
            async move {
                ctx.run_step_with(
                    "flaky_step",
                    StepOptions {
                        max_retries: 3,
                        base_interval: Some(Duration::from_millis(1)),
                        ..Default::default()
                    },
                    move |_| {
                        let counter = counter.clone();
                        async move {
                            let n = counter.fetch_add(1, Ordering::SeqCst);
                            if n < 2 {
                                Err(dbos::DbosError::other("transient"))
                            } else {
                                Ok::<_, dbos::DbosError>(n)
                            }
                        }
                    },
                )
                .await
            }
        })
    })
    .await;

    let handle = dbos
        .run_workflow::<_, u64>("flaky", (), WorkflowOptions::default())
        .await
        .unwrap();
    assert_eq!(handle.get_result().await.unwrap(), 2);
    assert_eq!(attempts.load(Ordering::SeqCst), 3, "1 initial + 2 retries");
    dbos.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test]
async fn step_exhausts_retries() {
    let schema = unique_schema("retry_fail");
    let dbos = launch(&schema, |b| {
        b.register_workflow("always_fails", |ctx: WorkflowContext, _: ()| async move {
            ctx.run_step_with::<(), _, _>(
                "doomed",
                StepOptions {
                    max_retries: 2,
                    base_interval: Some(Duration::from_millis(1)),
                    ..Default::default()
                },
                |_| async { Err(dbos::DbosError::other("always")) },
            )
            .await
        })
    })
    .await;

    let handle = dbos
        .run_workflow::<_, ()>("always_fails", (), WorkflowOptions::default())
        .await
        .unwrap();
    let err = handle.get_result().await.unwrap_err();
    assert!(
        err.to_string().contains("exceeded its maximum of 2 retries"),
        "unexpected error: {err}"
    );
    dbos.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test]
async fn child_workflow() {
    let schema = unique_schema("child");
    let dbos = launch(&schema, |b| {
        b.register_workflow("double", |_ctx: WorkflowContext, n: i64| async move {
            Ok::<_, dbos::DbosError>(n * 2)
        })
        .register_workflow("parent", |ctx: WorkflowContext, n: i64| async move {
            let child = ctx
                .run_workflow::<_, i64>("double", n, WorkflowOptions::default())
                .await?;
            let doubled = child.get_result().await?;
            // Child id is derived from the parent + step id.
            assert_eq!(child.workflow_id(), format!("{}-0", ctx.workflow_id()));
            Ok::<_, dbos::DbosError>(doubled + 1)
        })
    })
    .await;

    let handle = dbos
        .run_workflow::<_, i64>("parent", 21, WorkflowOptions::default())
        .await
        .unwrap();
    assert_eq!(handle.get_result().await.unwrap(), 43);
    dbos.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test]
async fn workflow_error_propagates() {
    let schema = unique_schema("err");
    let dbos = launch(&schema, |b| {
        b.register_workflow("boom", |_ctx: WorkflowContext, _: ()| async move {
            Err::<(), _>(dbos::DbosError::other("kaboom"))
        })
    })
    .await;

    // Owned handle: the real error propagates.
    let handle = dbos
        .run_workflow::<_, ()>("boom", (), WorkflowOptions::default())
        .await
        .unwrap();
    let err = handle.get_result().await.unwrap_err();
    assert!(err.to_string().contains("kaboom"), "got: {err}");

    // Polling handle (retrieve by id): the stored error is reconstructed.
    let polled = dbos.retrieve_workflow::<()>(handle.workflow_id());
    let err2 = polled.get_result().await.unwrap_err();
    assert!(err2.to_string().contains("kaboom"), "got: {err2}");
    dbos.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test]
async fn conflicting_registration_is_rejected() {
    let schema = unique_schema("dupe");
    let result = Dbos::builder(config(&schema))
        .register_workflow("w", |_: WorkflowContext, _: ()| async move {
            Ok::<_, dbos::DbosError>(())
        })
        .register_workflow("w", |_: WorkflowContext, _: ()| async move {
            Ok::<_, dbos::DbosError>(())
        })
        .launch()
        .await;
    let err = result.err().expect("should fail");
    assert_eq!(err.code(), DbosErrorCode::ConflictingRegistration as i32);
}
