//! M11 acceptance test: the admin HTTP server.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use dbos::{Config, Dbos, DbosError, WorkflowContext, WorkflowOptions, WorkflowQueue};
use serde_json::{json, Value};

fn test_url() -> String {
    std::env::var("DBOS_SYSTEM_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:dbos@localhost:5439/dbos".to_string())
}

fn unique_schema(prefix: &str) -> String {
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    format!("test_{prefix}_{}_{n}", std::process::id())
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// GET a URL as JSON, retrying briefly while the server binds.
async fn get_json(client: &reqwest::Client, url: &str) -> Value {
    for _ in 0..40 {
        if let Ok(resp) = client.get(url).send().await {
            return resp.json().await.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("admin server never came up: {url}");
}

#[tokio::test]
async fn admin_server_serves_endpoints() {
    let schema = unique_schema("admin");
    let port = free_port();
    let dbos = Dbos::builder(Config {
        app_name: "test".to_string(),
        database_url: Some(test_url()),
        database_schema: Some(schema.clone()),
        admin_server: true,
        admin_server_port: Some(port),
        ..Default::default()
    })
    .register_workflow("echo", |ctx: WorkflowContext, n: i64| async move {
        ctx.run_step("the_step", |_| async { Ok::<_, DbosError>(()) }).await?;
        Ok::<_, DbosError>(n)
    })
    .register_queue(WorkflowQueue::new("q").worker_concurrency(3))
    .launch()
    .await
    .unwrap();

    let h = dbos
        .run_workflow::<_, i64>("echo", 42i64, WorkflowOptions { workflow_id: Some("wf-1".into()), ..Default::default() })
        .await
        .unwrap();
    assert_eq!(h.get_result().await.unwrap(), 42);

    let base = format!("http://127.0.0.1:{port}");
    // Fresh connection per request: avoids reqwest reusing a keep-alive socket hyper has closed.
    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(0)
        .build()
        .unwrap();

    // Health
    let health = get_json(&client, &format!("{base}/dbos-healthz")).await;
    assert_eq!(health["status"], "healthy");

    // List workflows (PascalCase wire shape; Output is a serialized JSON string)
    let wfs: Value = client
        .post(format!("{base}/workflows"))
        .body(json!({ "workflow_uuids": ["wf-1"] }).to_string())
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let arr = wfs.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["WorkflowUUID"], "wf-1");
    assert_eq!(arr[0]["WorkflowName"], "echo");
    assert_eq!(arr[0]["Status"], "SUCCESS");
    assert_eq!(arr[0]["Output"], "42");

    // Get one workflow
    let wf: Value = client.get(format!("{base}/workflows/wf-1")).send().await.unwrap().json().await.unwrap();
    assert_eq!(wf["WorkflowName"], "echo");

    // List steps (snake_case)
    let steps: Value = client.get(format!("{base}/workflows/wf-1/steps")).send().await.unwrap().json().await.unwrap();
    assert_eq!(steps.as_array().unwrap().len(), 1);
    assert_eq!(steps[0]["function_name"], "the_step");

    // Queues metadata includes our queue
    let queues: Value = client.get(format!("{base}/dbos-workflow-queues-metadata")).send().await.unwrap().json().await.unwrap();
    let names: Vec<&str> = queues.as_array().unwrap().iter().map(|q| q["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"q"), "queues: {queues}");

    // Recovery returns an array
    let rec: Value = client
        .post(format!("{base}/dbos-workflow-recovery"))
        .body(json!(["nonexistent-executor"]).to_string())
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(rec.is_array());

    // Cancel a workflow → 204
    let resp = client.post(format!("{base}/workflows/wf-1/cancel")).send().await.unwrap();
    assert_eq!(resp.status(), 204);

    // 404 for a missing workflow
    let resp = client.get(format!("{base}/workflows/does-not-exist")).send().await.unwrap();
    assert_eq!(resp.status(), 404);

    // Deactivate
    let de = client.get(format!("{base}/deactivate")).send().await.unwrap();
    assert_eq!(de.text().await.unwrap(), "deactivated");

    dbos.shutdown(Duration::from_secs(2)).await;
}
