//! M5 acceptance tests: notifications (send/recv) and events (set/get) against a live Postgres.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use dbos::{Config, Dbos, DbosBuilder, DbosErrorCode, WorkflowContext, WorkflowOptions};

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
async fn send_and_recv() {
    let schema = unique_schema("notif_sr");
    let dbos = launch(&schema, |b| {
        b.register_workflow("receiver", |ctx: WorkflowContext, _: ()| async move {
            ctx.recv::<String>(None, Duration::from_secs(5)).await
        })
        .register_workflow("sender", |ctx: WorkflowContext, dest: String| async move {
            ctx.send(&dest, "hello".to_string(), None).await
        })
    })
    .await;

    // The receiver blocks on recv until the sender delivers.
    let receiver = dbos
        .run_workflow::<_, Option<String>>("receiver", (), with_id("recv-1"))
        .await
        .unwrap();
    dbos.run_workflow::<_, ()>("sender", "recv-1".to_string(), WorkflowOptions::default())
        .await
        .unwrap()
        .get_result()
        .await
        .unwrap();

    assert_eq!(receiver.get_result().await.unwrap(), Some("hello".to_string()));
    dbos.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test]
async fn set_and_get_event() {
    let schema = unique_schema("notif_ev");
    let dbos = launch(&schema, |b| {
        b.register_workflow("publisher", |ctx: WorkflowContext, _: ()| async move {
            ctx.set_event("status", "ready".to_string()).await
        })
        .register_workflow("reader", |ctx: WorkflowContext, target: String| async move {
            ctx.get_event::<String>(&target, "status", Duration::from_secs(5)).await
        })
    })
    .await;

    dbos.run_workflow::<_, ()>("publisher", (), with_id("pub-1"))
        .await
        .unwrap()
        .get_result()
        .await
        .unwrap();

    let read = dbos
        .run_workflow::<_, Option<String>>("reader", "pub-1".to_string(), WorkflowOptions::default())
        .await
        .unwrap();
    assert_eq!(read.get_result().await.unwrap(), Some("ready".to_string()));
    dbos.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test]
async fn get_event_waits_for_later_set() {
    let schema = unique_schema("notif_evwait");
    let dbos = launch(&schema, |b| {
        b.register_workflow("publisher", |ctx: WorkflowContext, _: ()| async move {
            // Set the event only after a delay, so the reader must wait for it.
            tokio::time::sleep(Duration::from_millis(300)).await;
            ctx.set_event("k", 99i64).await
        })
        .register_workflow("reader", |ctx: WorkflowContext, target: String| async move {
            ctx.get_event::<i64>(&target, "k", Duration::from_secs(5)).await
        })
    })
    .await;

    let publisher = dbos
        .run_workflow::<_, ()>("publisher", (), with_id("pub-2"))
        .await
        .unwrap();
    let reader = dbos
        .run_workflow::<_, Option<i64>>("reader", "pub-2".to_string(), WorkflowOptions::default())
        .await
        .unwrap();

    assert_eq!(reader.get_result().await.unwrap(), Some(99));
    publisher.get_result().await.unwrap();
    dbos.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test]
async fn recv_times_out() {
    let schema = unique_schema("notif_rto");
    let dbos = launch(&schema, |b| {
        b.register_workflow("waiter", |ctx: WorkflowContext, _: ()| async move {
            ctx.recv::<String>(None, Duration::from_millis(200)).await
        })
    })
    .await;

    let h = dbos
        .run_workflow::<_, Option<String>>("waiter", (), WorkflowOptions::default())
        .await
        .unwrap();
    assert_eq!(h.get_result().await.unwrap(), None, "recv returns None on timeout");
    dbos.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test]
async fn get_event_times_out() {
    let schema = unique_schema("notif_eto");
    let dbos = launch(&schema, |b| {
        b.register_workflow("reader", |ctx: WorkflowContext, _: ()| async move {
            ctx.get_event::<String>("nobody", "missing", Duration::from_millis(200))
                .await
        })
    })
    .await;

    let h = dbos
        .run_workflow::<_, Option<String>>("reader", (), WorkflowOptions::default())
        .await
        .unwrap();
    assert_eq!(h.get_result().await.unwrap(), None, "get_event returns None on timeout");
    dbos.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test]
async fn send_to_nonexistent_workflow_errors() {
    let schema = unique_schema("notif_bad");
    let dbos = launch(&schema, |b| {
        b.register_workflow("bad_sender", |ctx: WorkflowContext, _: ()| async move {
            ctx.send("does-not-exist", "x".to_string(), None).await
        })
    })
    .await;

    let h = dbos
        .run_workflow::<_, ()>("bad_sender", (), WorkflowOptions::default())
        .await
        .unwrap();
    let err = h.get_result().await.unwrap_err();
    assert_eq!(
        err.code(),
        DbosErrorCode::NonExistentWorkflow as i32,
        "got: {err}"
    );
    dbos.shutdown(Duration::from_secs(2)).await;
}
