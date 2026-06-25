//! M7 acceptance tests: workflow management + external client against a live Postgres.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use dbos::{
    Client, Config, Dbos, DbosBuilder, DbosError, DbosErrorCode, EnqueueOptions, ForkWorkflowInput,
    ListWorkflowsFilter, WorkflowContext, WorkflowOptions, WorkflowQueue, WorkflowStatusType,
};

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

fn ids(filter_ids: &[&str]) -> ListWorkflowsFilter {
    ListWorkflowsFilter {
        workflow_ids: Some(filter_ids.iter().map(|s| s.to_string()).collect()),
        ..Default::default()
    }
}

#[tokio::test]
async fn list_workflows_and_steps() {
    let schema = unique_schema("mgmt_list");
    let dbos = launch(&schema, |b| {
        b.register_workflow("stepper", |ctx: WorkflowContext, _: ()| async move {
            ctx.run_step("s1", |_| async { Ok::<_, DbosError>(1) })
                .await?;
            ctx.run_step("s2", |_| async { Ok::<_, DbosError>(2) })
                .await?;
            Ok::<_, DbosError>(3)
        })
    })
    .await;

    let h = dbos
        .run_workflow::<_, i64>(
            "stepper",
            (),
            WorkflowOptions {
                workflow_id: Some("list-target".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(h.get_result().await.unwrap(), 3);

    let listed = dbos.list_workflows(ids(&["list-target"])).await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, "list-target");
    assert_eq!(listed[0].name, "stepper");
    assert_eq!(listed[0].status, WorkflowStatusType::Success);
    assert_eq!(listed[0].output, Some(serde_json::json!(3)));

    let steps = dbos.list_workflow_steps("list-target").await.unwrap();
    let names: Vec<(i32, &str)> = steps
        .iter()
        .map(|s| (s.step_id, s.step_name.as_str()))
        .collect();
    assert_eq!(names, vec![(0, "s1"), (1, "s2")]);

    dbos.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test]
async fn cancel_then_resume() {
    let schema = unique_schema("mgmt_cr");
    let dbos = launch(&schema, |b| {
        b.register_workflow("task", |_ctx: WorkflowContext, _: ()| async move {
            Ok::<_, DbosError>("done".to_string())
        })
        // 1s polling: the workflow stays ENQUEUED long enough to cancel before it runs.
        .register_queue(WorkflowQueue::new("q"))
    })
    .await;

    let h = dbos
        .enqueue::<_, String>(
            "q",
            "task",
            (),
            EnqueueOptions {
                workflow_id: Some("cr-1".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    dbos.cancel_workflow("cr-1").await.unwrap();
    assert_eq!(
        h.get_status().await.unwrap(),
        Some(WorkflowStatusType::Cancelled)
    );

    // Resume re-enqueues onto the internal queue, whose runner executes it to completion.
    let resumed: dbos::WorkflowHandle<String> = dbos.resume_workflow("cr-1").await.unwrap();
    assert_eq!(resumed.get_result().await.unwrap(), "done");

    // Cancelling a non-existent workflow errors.
    let err = dbos.cancel_workflow("nope").await.unwrap_err();
    assert_eq!(err.code(), DbosErrorCode::NonExistentWorkflow as i32);

    dbos.shutdown(Duration::from_secs(3)).await;
}

#[tokio::test]
async fn fork_replays_copied_steps() {
    let schema = unique_schema("mgmt_fork");
    let a = Arc::new(AtomicU64::new(0));
    let b = Arc::new(AtomicU64::new(0));
    let a2 = a.clone();
    let b2 = b.clone();

    let dbos = launch(&schema, move |bld| {
        bld.register_workflow("forkable", move |ctx: WorkflowContext, _: ()| {
            let a = a2.clone();
            let b = b2.clone();
            async move {
                let av = ctx
                    .run_step("a", {
                        let a = a.clone();
                        move |_| async move {
                            Ok::<_, DbosError>(a.fetch_add(1, Ordering::SeqCst) as i64 + 1)
                        }
                    })
                    .await?;
                let bv = ctx
                    .run_step("b", {
                        let b = b.clone();
                        move |_| async move {
                            Ok::<_, DbosError>(b.fetch_add(1, Ordering::SeqCst) as i64 + 1)
                        }
                    })
                    .await?;
                Ok::<_, DbosError>(av * 10 + bv)
            }
        })
    })
    .await;

    let h = dbos
        .run_workflow::<_, i64>(
            "forkable",
            (),
            WorkflowOptions {
                workflow_id: Some("fork-src".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(h.get_result().await.unwrap(), 11); // a=1, b=1
    assert_eq!(a.load(Ordering::SeqCst), 1);
    assert_eq!(b.load(Ordering::SeqCst), 1);

    // Fork from step 1: step "a" (id 0) is copied/replayed; step "b" re-runs.
    let forked: dbos::WorkflowHandle<i64> = dbos
        .fork_workflow(ForkWorkflowInput {
            original_workflow_id: "fork-src".to_string(),
            start_step: 1,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(
        forked.get_result().await.unwrap(),
        12,
        "a replayed (1), b re-ran (2)"
    );
    assert_eq!(
        a.load(Ordering::SeqCst),
        1,
        "step a was replayed, not re-executed"
    );
    assert_eq!(b.load(Ordering::SeqCst), 2, "step b re-ran once more");

    dbos.shutdown(Duration::from_secs(3)).await;
}

#[tokio::test]
async fn external_client_enqueue_and_manage() {
    let schema = unique_schema("mgmt_client");
    // A running server with the workflow registered + a fast queue runner.
    let dbos = launch(&schema, |b| {
        b.register_workflow("double", |_ctx: WorkflowContext, n: i64| async move {
            Ok::<_, DbosError>(n * 2)
        })
        .register_queue(WorkflowQueue::new("q").base_polling_interval(Duration::from_millis(10)))
    })
    .await;

    // An external client connects to the same system database (no migrations, no tasks).
    let client = Client::connect(&test_url(), &schema).await.unwrap();
    let handle = client
        .enqueue(
            "q",
            "double",
            21,
            EnqueueOptions {
                workflow_id: Some("client-wf".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(handle.get_result().await.unwrap(), serde_json::json!(42));

    let listed = client.list_workflows(ids(&["client-wf"])).await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].status, WorkflowStatusType::Success);

    client.shutdown().await;
    dbos.shutdown(Duration::from_secs(2)).await;
}
