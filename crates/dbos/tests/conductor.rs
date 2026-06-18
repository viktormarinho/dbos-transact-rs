//! M10 acceptance test: the Conductor websocket client, driven by a mock conductor server.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use dbos::{Config, Dbos, DbosError, WorkflowContext, WorkflowOptions};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

fn test_url() -> String {
    std::env::var("DBOS_SYSTEM_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:dbos@localhost:5439/dbos".to_string())
}

fn unique_schema(prefix: &str) -> String {
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    format!("test_{prefix}_{}_{n}", std::process::id())
}

/// Read the next JSON text frame, skipping ping/pong.
async fn read_json(ws: &mut WebSocketStream<TcpStream>) -> Value {
    loop {
        match ws.next().await.unwrap().unwrap() {
            Message::Text(t) => return serde_json::from_str(&t).unwrap(),
            Message::Ping(_) | Message::Pong(_) => continue,
            other => panic!("unexpected frame: {other:?}"),
        }
    }
}

async fn request(ws: &mut WebSocketStream<TcpStream>, msg: Value) -> Value {
    ws.send(Message::Text(msg.to_string())).await.unwrap();
    read_json(ws).await
}

#[tokio::test]
async fn conductor_serves_management_requests() {
    let schema = unique_schema("conductor");

    // A mock conductor server.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel::<()>();

    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();

        // executor_info
        let resp = request(&mut ws, json!({"type":"executor_info","request_id":"r1"})).await;
        assert_eq!(resp["type"], "executor_info");
        assert_eq!(resp["request_id"], "r1");
        assert_eq!(resp["language"], "rust");
        assert!(resp["executor_id"].as_str().is_some());
        assert!(resp["application_version"].as_str().is_some());

        // Wait until the workflow exists.
        ready_rx.await.unwrap();

        // list_workflows for wf-1
        let resp = request(
            &mut ws,
            json!({"type":"list_workflows","request_id":"r2","body":{"workflow_uuids":["wf-1"]}}),
        )
        .await;
        let out = resp["output"].as_array().expect("output array");
        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["WorkflowUUID"], "wf-1");
        assert_eq!(out[0]["WorkflowName"], "echo");
        assert_eq!(out[0]["Status"], "SUCCESS");
        // Output is a serialized JSON string of the result (42).
        assert_eq!(out[0]["Output"], "42");

        // list_steps for wf-1
        let resp = request(
            &mut ws,
            json!({"type":"list_steps","request_id":"r3","workflow_id":"wf-1"}),
        )
        .await;
        let steps = resp["output"].as_array().expect("steps array");
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0]["function_name"], "the_step");

        // cancel wf-1
        let resp = request(
            &mut ws,
            json!({"type":"cancel","request_id":"r4","workflow_id":"wf-1"}),
        )
        .await;
        assert_eq!(resp["success"], true);

        // unknown type → graceful error
        let resp = request(&mut ws, json!({"type":"bogus","request_id":"r5"})).await;
        assert_eq!(resp["error_message"], "Unknown message type");
    });

    let dbos = Dbos::builder(Config {
        app_name: "test".to_string(),
        database_url: Some(test_url()),
        database_schema: Some(schema.clone()),
        conductor_url: Some(format!("ws://127.0.0.1:{port}/conductor/v1alpha1")),
        conductor_api_key: Some("testkey".to_string()),
        ..Default::default()
    })
    .register_workflow("echo", |ctx: WorkflowContext, n: i64| async move {
        ctx.run_step("the_step", |_| async { Ok::<_, DbosError>(()) }).await?;
        Ok::<_, DbosError>(n)
    })
    .launch()
    .await
    .unwrap();

    // Create wf-1, then let the server proceed.
    let h = dbos
        .run_workflow::<_, i64>("echo", 42i64, WorkflowOptions { workflow_id: Some("wf-1".into()), ..Default::default() })
        .await
        .unwrap();
    assert_eq!(h.get_result().await.unwrap(), 42);
    ready_tx.send(()).unwrap();

    // Propagate any assertion failure from the server task.
    tokio::time::timeout(Duration::from_secs(10), server)
        .await
        .expect("conductor exchange timed out")
        .expect("server task panicked");

    dbos.shutdown(Duration::from_secs(2)).await;
}
