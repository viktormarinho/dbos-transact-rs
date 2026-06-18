//! Optional OpenTelemetry OTLP trace export (feature `telemetry`).
//!
//! Workflow and step spans are emitted unconditionally via the `tracing` crate (so they flow into
//! whatever subscriber your application installs). This module adds a built-in OTLP/HTTP exporter
//! so DBOS can ship those spans to a collector with one call.
//!
//! It is **opt-in** and **first-one-wins**: call [`init`] with an endpoint to enable it. `init` uses
//! `try_init`, so if your application already installed a global `tracing` subscriber, that one is
//! left in place (DBOS spans still flow into it).
//!
//! ```no_run
//! let _guard = dbos::telemetry::init("my-app", "http://localhost:4318/v1/traces");
//! // ... build and launch Dbos; spans now export over OTLP until `_guard` drops.
//! ```

use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig as _;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

/// Owns the tracer provider; flushes pending spans and shuts it down on drop.
pub struct TelemetryGuard {
    provider: SdkTracerProvider,
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        let _ = self.provider.shutdown();
    }
}

/// Initialize OTLP/HTTP trace export to `traces_endpoint`, tagging every span with
/// `service.name = app_name`, and install a `tracing` subscriber that exports DBOS's workflow and
/// step spans. Returns a guard that flushes on drop.
pub fn init(app_name: &str, traces_endpoint: &str) -> TelemetryGuard {
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_endpoint(traces_endpoint)
        .build()
        .expect("build OTLP span exporter");

    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(
            opentelemetry_sdk::Resource::builder()
                .with_service_name(app_name.to_string())
                .build(),
        )
        .build();

    let tracer = provider.tracer("dbos-tracer");
    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    // first-one-wins: leave a pre-existing global subscriber in place.
    let _ = tracing_subscriber::registry()
        .with(filter)
        .with(otel_layer)
        .try_init();

    opentelemetry::global::set_tracer_provider(provider.clone());
    TelemetryGuard { provider }
}
