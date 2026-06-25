//! Group A.1 + C.8: cross-SDK behavior on a shared system database — migration-version
//! coordination and the recovery gating contract (executor id + application version).

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use dbos::{Config, Dbos, DbosBuilder, DbosError, WorkflowContext, WorkflowOptions};
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
        .max_connections(2)
        .connect(&test_url())
        .await
        .unwrap()
}

/// Forge a PENDING workflow row (as if interrupted), with a valid unit-input envelope.
async fn forge_pending(schema: &str, id: &str, name: &str, executor: &str, app_version: &str) {
    sqlx::query(&format!(
        "INSERT INTO \"{schema}\".workflow_status
            (workflow_uuid, status, name, executor_id, application_version, recovery_attempts,
             serialization, inputs)
         VALUES ($1, 'PENDING', $2, $3, $4, 1, 'portable_json',
                 '{{\"positionalArgs\":[null],\"namedArgs\":{{}}}}')"
    ))
    .bind(id)
    .bind(name)
    .bind(executor)
    .bind(app_version)
    .execute(&pool().await)
    .await
    .unwrap();
}

#[tokio::test]
async fn does_not_migrate_a_ts_migrated_schema() {
    // Simulate a DB migrated by the TS SDK 4.21.6: the same final schema, but stamped with TS's
    // positional version (63) rather than this crate's numbered SCHEMA_VERSION (40). (Schema
    // equivalence is a documented premise; this exercises the version-coordination code path.)
    let schema = unique_schema("tsmig");
    let p = pool().await;
    dbos::db::run_migrations(&p, &schema).await.unwrap();
    sqlx::query(&format!(
        "UPDATE \"{schema}\".dbos_migrations SET version = 63"
    ))
    .execute(&p)
    .await
    .unwrap();

    // The port must NOT run its numbered migrations on a higher-versioned (TS) DB.
    assert!(
        !dbos::db::should_migrate(&p, &schema).await.unwrap(),
        "version 63 >= 40 → no migration"
    );

    // `attributes` (migration 40 here; identical DDL in TS 4.21.6) exists.
    let has_attributes: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM information_schema.columns
         WHERE table_schema = $1 AND table_name = 'workflow_status' AND column_name = 'attributes')",
    )
    .bind(&schema)
    .fetch_one(&p)
    .await
    .unwrap();
    assert!(has_attributes, "attributes column present");

    // Launch (which always calls run_migrations) leaves the TS version untouched and operates.
    let dbos = launch(&schema, |b| {
        b.register_workflow("echo", |_: WorkflowContext, n: i64| async move {
            Ok::<_, DbosError>(n)
        })
    })
    .await;
    let version: i64 =
        sqlx::query_scalar(&format!("SELECT version FROM \"{schema}\".dbos_migrations"))
            .fetch_one(&p)
            .await
            .unwrap();
    assert_eq!(
        version, 63,
        "the port did not run its migrations over the TS schema"
    );

    let h = dbos
        .run_workflow::<_, i64>(
            "echo",
            5,
            WorkflowOptions {
                workflow_id: Some("ts-wf".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(h.get_result().await.unwrap(), 5);
    dbos.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test]
async fn recovery_gated_by_executor_id_and_application_version() {
    let schema = unique_schema("recgate");
    let dbos = launch(&schema, |b| {
        b.register_workflow("noop", |_: WorkflowContext, _: ()| async move {
            Ok::<_, DbosError>("done".to_string())
        })
    })
    .await;

    // Learn this executor's id + application version from a completed run.
    let done = dbos
        .run_workflow::<_, String>("noop", (), WorkflowOptions::default())
        .await
        .unwrap();
    done.get_result().await.unwrap();
    let (executor, app_version): (String, String) = sqlx::query_as(&format!(
        "SELECT executor_id, application_version FROM \"{schema}\".workflow_status WHERE workflow_uuid = $1"
    ))
    .bind(done.workflow_id())
    .fetch_one(&pool().await)
    .await
    .unwrap();

    forge_pending(&schema, "same", "noop", &executor, &app_version).await;
    forge_pending(&schema, "diff_ver", "noop", &executor, "OTHER_APP_VERSION").await;
    forge_pending(&schema, "diff_exec", "noop", "ts-executor-1", &app_version).await;

    // Self-recovery picks up only the same-executor + same-version workflow.
    let recovered = dbos.recover_pending_workflows().await.unwrap();
    assert!(recovered.contains(&"same".to_string()));
    assert!(
        !recovered.contains(&"diff_ver".to_string()),
        "a different application_version is NOT recovered"
    );
    assert!(
        !recovered.contains(&"diff_exec".to_string()),
        "a different executor_id is NOT recovered automatically"
    );

    // Adoption path: recovering FOR the foreign (TS) executor id — same app version — adopts it.
    let adopted = dbos
        .recover_workflows(&["ts-executor-1".to_string()])
        .await
        .unwrap();
    assert!(
        adopted.contains(&"diff_exec".to_string()),
        "a TS-orphaned workflow is adopted when its executor id is supplied"
    );
    assert_eq!(
        dbos.retrieve_workflow::<String>("diff_exec")
            .get_result()
            .await
            .unwrap(),
        "done"
    );

    dbos.shutdown(Duration::from_secs(3)).await;
}
