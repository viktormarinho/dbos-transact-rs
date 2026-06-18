<div align="center">

# DBOS Transact for Rust

**Lightweight durable workflow orchestration on Postgres — for Rust.**

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

</div>

---

DBOS provides lightweight **durable workflows** backed by Postgres. Register ordinary `async`
functions as durable workflows and steps, and DBOS checkpoints their progress in Postgres so they
**recover exactly where they left off** after any crash, restart, or failure — no external
orchestrator required.

This is a from-scratch Rust implementation of [DBOS Transact](https://github.com/dbos-inc), a port
of the official [Go](https://github.com/dbos-inc/dbos-transact-golang),
[Python](https://github.com/dbos-inc/dbos-transact-py), and
[TypeScript](https://github.com/dbos-inc/dbos-transact-ts) SDKs. It is **wire-compatible with the
DBOS system-database schema**, so the same database can be inspected by DBOS tooling.

> **Status:** the durable-execution core is complete and tested (durable workflows & steps, crash
> recovery, durable queues, notifications/events, durable sleep & cron, an external client, and
> workflow management). See [Feature status](#feature-status). Admin server, the cloud Conductor
> protocol, telemetry, and streams are planned.

## Example

```rust
use dbos::{Config, Dbos, WorkflowContext, WorkflowOptions, WorkflowQueue, StepOptions};

// A workflow is a plain async function. Each `run_step` is checkpointed: on recovery, completed
// steps are replayed from Postgres instead of re-executed.
async fn checkout(ctx: WorkflowContext, order: Order) -> dbos::Result<Receipt> {
    let auth = ctx.run_step("authorize", |_| async move { authorize(order.card).await }).await?;
    ctx.set_event("status", "authorized").await?;                    // publish progress
    let capture = ctx.run_step_with("capture", StepOptions { max_retries: 3, ..Default::default() },
        move |_| capture(auth.clone())).await?;                      // retried with backoff
    Ok(Receipt { id: capture.id })
}

#[tokio::main]
async fn main() -> dbos::Result<()> {
    let dbos = Dbos::builder(Config::new("shop", "postgres://localhost/dbos"))
        .register_workflow("checkout", checkout)
        .register_queue(WorkflowQueue::new("payments").worker_concurrency(4))
        .launch().await?;                  // connects, migrates, recovers interrupted workflows

    // Run directly, or enqueue onto a durable queue:
    let handle = dbos.enqueue::<_, Receipt>("payments", "checkout", order, Default::default()).await?;
    let receipt = handle.get_result().await?;

    dbos.shutdown(std::time::Duration::from_secs(5)).await;
    Ok(())
}
```

## Features

- **💾 Durable workflows** — register `async` functions; their steps are checkpointed to Postgres
  and replayed on recovery. Idempotent invocation by workflow id, child workflows, configurable
  step retries.
- **🔁 Crash recovery** — on launch, interrupted workflows resume from their last completed step.
  A dead-letter queue catches workflows that exceed their recovery limit.
- **📒 Durable queues** — enqueue workflows with global/per-worker concurrency limits, rate
  limiting, priority, deduplication, partitions, and delayed execution.
- **📫 Notifications & events** — exactly-once `send`/`recv` mailboxes and `set_event`/`get_event`,
  woken instantly via Postgres `LISTEN`/`NOTIFY` (with a polling safety net), with durable timeouts.
- **📅 Durable sleep & cron** — sleep for seconds or weeks through restarts; schedule workflows with
  cron, exactly once per tick across the fleet.
- **🌊 Durable streams** — a workflow publishes an append-only, ordered log under a key
  (`write_stream`/`close_stream`); consumers read it live (`read_stream`/`read_stream_async`),
  woken via LISTEN/NOTIFY. Great for progress feeds, LLM token streaming, or incremental output.
- **🛠 Management & client** — list/cancel/resume/fork/garbage-collect workflows; a thin `Client`
  lets external apps enqueue and manage workflows without running a full runtime.

Workflows are **decorator-free** — plain async functions registered by name — mirroring the Go
SDK's ergonomics, which maps cleanly to Rust.

## Getting started

Add the crate (from a local path or git for now):

```toml
[dependencies]
dbos = { git = "https://github.com/viktormarinho/dbos-transact-rs" }
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
```

You need a Postgres database. By default the `portable_json` serialization format is used, making
DBOS-written payloads cross-language readable; a reader for Go's `DBOS_JSON` is included.

## Feature status

| Area | Status |
|------|--------|
| Config, errors, serialization (`portable_json` + `DBOS_JSON` reader) | ✅ |
| System-DB schema + migration runner (Postgres, schema v40) | ✅ |
| Durable workflows + steps (memoization, retries, child workflows) | ✅ |
| Crash recovery + dead-letter queue | ✅ |
| Durable queues (concurrency, rate-limit, priority, dedup, delay, partitions) | ✅ |
| Notifications (`send`/`recv`) & events (`set`/`get`) via LISTEN/NOTIFY | ✅ |
| Durable sleep + cron scheduler | ✅ |
| External client + workflow management (list/cancel/resume/fork/GC) | ✅ |
| `js_superjson` reader — read an existing TypeScript-DBOS database | ✅ |
| Durable streams (`write_stream`/`read_stream`/`read_stream_async`) | ✅ |
| Admin HTTP server, cloud Conductor, OpenTelemetry, scheduler backfill | ⏳ planned |

See [`ROADMAP.md`](ROADMAP.md) and [`DESIGN.md`](DESIGN.md) for the full plan and architecture.

## Development

The crate lives in `crates/dbos`. Integration tests require a Postgres; the test harness creates a
unique schema per test so they run in parallel.

```sh
# Start a throwaway Postgres (matches the default test URL):
docker run -d --name dbos-test-pg -e POSTGRES_PASSWORD=dbos -e POSTGRES_USER=postgres \
  -e POSTGRES_DB=dbos -p 5439:5432 postgres:16

cargo test          # override the DB with DBOS_SYSTEM_DATABASE_URL
cargo clippy --all-targets
```

## License

MIT
