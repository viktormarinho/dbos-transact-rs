//! M9 acceptance tests: durable streams against a live Postgres.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use dbos::{Config, Dbos, DbosBuilder, DbosError, WorkflowContext, WorkflowOptions};

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

fn with_id(id: &str) -> WorkflowOptions {
    WorkflowOptions {
        workflow_id: Some(id.to_string()),
        ..Default::default()
    }
}

#[tokio::test]
async fn produce_and_consume_with_close() {
    let schema = unique_schema("stream_pc");
    let dbos = launch(&schema, |b| {
        b.register_workflow("producer", |ctx: WorkflowContext, _: ()| async move {
            for i in 0..3i64 {
                ctx.write_stream("feed", i).await?;
            }
            ctx.close_stream("feed").await?;
            Ok::<_, DbosError>(())
        })
    })
    .await;

    let h = dbos
        .run_workflow::<_, ()>("producer", (), with_id("prod-1"))
        .await
        .unwrap();
    let (values, closed): (Vec<i64>, bool) = dbos.read_stream("prod-1", "feed").await.unwrap();
    assert_eq!(values, vec![0, 1, 2]);
    assert!(closed, "stream was explicitly closed");
    h.get_result().await.unwrap();
    dbos.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test]
async fn stream_closes_when_producer_finishes() {
    let schema = unique_schema("stream_fin");
    let dbos = launch(&schema, |b| {
        // Writes values but never calls close_stream.
        b.register_workflow("producer", |ctx: WorkflowContext, _: ()| async move {
            for i in 0..3i64 {
                ctx.write_stream("feed", i).await?;
            }
            Ok::<_, DbosError>("done".to_string())
        })
    })
    .await;

    let h = dbos
        .run_workflow::<_, String>("producer", (), with_id("prod-2"))
        .await
        .unwrap();
    h.get_result().await.unwrap();

    // No sentinel, but the producer is finished → the reader sees closed=true.
    let (values, closed): (Vec<i64>, bool) = dbos.read_stream("prod-2", "feed").await.unwrap();
    assert_eq!(values, vec![0, 1, 2]);
    assert!(closed, "an inactive producer closes the stream");
    dbos.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test]
async fn read_stream_async_live() {
    let schema = unique_schema("stream_async");
    let dbos = launch(&schema, |b| {
        b.register_workflow("producer", |ctx: WorkflowContext, _: ()| async move {
            for i in 0..5i64 {
                ctx.write_stream("feed", i).await?;
                tokio::time::sleep(Duration::from_millis(15)).await;
            }
            ctx.close_stream("feed").await?;
            Ok::<_, DbosError>(())
        })
    })
    .await;

    dbos.run_workflow::<_, ()>("producer", (), with_id("prod-3"))
        .await
        .unwrap();

    // Consume values live as they're produced; the channel closes when the stream closes.
    let mut rx = dbos.read_stream_async::<i64>("prod-3", "feed");
    let mut got = Vec::new();
    while let Some(item) = rx.recv().await {
        got.push(item.unwrap());
    }
    assert_eq!(got, vec![0, 1, 2, 3, 4]);
    dbos.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test]
async fn writing_to_closed_stream_errors() {
    let schema = unique_schema("stream_closed");
    let dbos = launch(&schema, |b| {
        b.register_workflow("bad", |ctx: WorkflowContext, _: ()| async move {
            ctx.write_stream("s", 1i64).await?;
            ctx.close_stream("s").await?;
            ctx.write_stream("s", 2i64).await?; // should error: already closed
            Ok::<_, DbosError>(())
        })
    })
    .await;

    let h = dbos
        .run_workflow::<_, ()>("bad", (), WorkflowOptions::default())
        .await
        .unwrap();
    let err = h.get_result().await.unwrap_err();
    assert!(err.to_string().contains("already closed"), "got: {err}");
    dbos.shutdown(Duration::from_secs(2)).await;
}
