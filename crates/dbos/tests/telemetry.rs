//! M12 acceptance test: workflow/step `tracing` spans carry the DBOS OpenTelemetry attributes.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dbos::{Config, Dbos, DbosError, WorkflowContext, WorkflowOptions};
use tracing::field::{Field, Visit};
use tracing::span::Attributes;
use tracing::Subscriber;
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::prelude::*;
use tracing_subscriber::registry::LookupSpan;

#[derive(Clone)]
struct CapturedSpan {
    name: String,
    fields: HashMap<String, String>,
}

#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<CapturedSpan>>>);

impl<S: Subscriber + for<'a> LookupSpan<'a>> Layer<S> for Capture {
    fn on_new_span(&self, attrs: &Attributes<'_>, _id: &tracing::Id, _ctx: Context<'_, S>) {
        let mut fields = HashMap::new();
        struct V<'a>(&'a mut HashMap<String, String>);
        impl Visit for V<'_> {
            fn record_str(&mut self, field: &Field, value: &str) {
                self.0.insert(field.name().to_string(), value.to_string());
            }
            fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
                self.0
                    .insert(field.name().to_string(), format!("{value:?}"));
            }
        }
        attrs.record(&mut V(&mut fields));
        self.0.lock().unwrap().push(CapturedSpan {
            name: attrs.metadata().name().to_string(),
            fields,
        });
    }
}

fn test_url() -> String {
    std::env::var("DBOS_SYSTEM_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:dbos@localhost:5439/dbos".to_string())
}

fn unique_schema(prefix: &str) -> String {
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    format!("test_{prefix}_{}_{n}", std::process::id())
}

#[tokio::test]
async fn workflow_and_step_spans_carry_dbos_attributes() {
    let capture = Capture::default();
    tracing_subscriber::registry().with(capture.clone()).init();

    let schema = unique_schema("otel");
    let dbos = Dbos::builder(Config {
        app_name: "test".to_string(),
        database_url: Some(test_url()),
        database_schema: Some(schema),
        ..Default::default()
    })
    .register_workflow("echo", |ctx: WorkflowContext, n: i64| async move {
        ctx.run_step("the_step", |_| async { Ok::<_, DbosError>(()) })
            .await?;
        Ok::<_, DbosError>(n)
    })
    .launch()
    .await
    .unwrap();

    let h = dbos
        .run_workflow::<_, i64>(
            "echo",
            7i64,
            WorkflowOptions {
                workflow_id: Some("otel-wf".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(h.get_result().await.unwrap(), 7);

    let spans = capture.0.lock().unwrap().clone();
    let find = |name: &str| spans.iter().find(|s| s.name == name).cloned();

    let wf = find("dbos.workflow").expect("workflow span");
    assert_eq!(
        wf.fields.get("operationType").map(String::as_str),
        Some("workflow")
    );
    assert!(wf.fields.get("otel.name").unwrap().contains("echo"));
    assert!(wf.fields.get("operationUUID").unwrap().contains("otel-wf"));

    let step = find("dbos.step").expect("step span");
    assert_eq!(
        step.fields.get("operationType").map(String::as_str),
        Some("step")
    );
    assert!(step.fields.get("otel.name").unwrap().contains("the_step"));
    assert!(step
        .fields
        .get("operationUUID")
        .unwrap()
        .contains("otel-wf"));

    dbos.shutdown(Duration::from_secs(2)).await;
}
