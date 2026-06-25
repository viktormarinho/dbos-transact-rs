# DESIGN.md — DBOS Transact for Rust (`dbos` crate)

## 0. Goal & Philosophy

A from-scratch async-Rust implementation of DBOS Transact: durable, crash-resilient
workflow execution backed by Postgres. The Go reference
(`.context/ref/go/dbos`) is the **primary** architectural model — explicit registration,
explicit error handling, no decorators, plain async functions registered by name.
Python and TypeScript references are consulted for exact semantics where Go is terse
(notification listener polling, croniter edge cases, exception fidelity).

**Hard invariants** (wire-compatibility with the DBOS ecosystem):

1. The system-DB schema (table/column/index/function/trigger names) **must match the Go
   reference exactly** (final migration version **40**). DBOS admin server, Conductor, and
   cross-language clients read these tables; any divergence breaks tooling.
2. Embed the reference SQL migration files (`include_str!`) and port the runner.
3. User API mirrors Go ergonomics: `register_workflow` + `run_workflow` + `run_step` +
   `WorkflowHandle` + `Queue` + `send`/`recv`/`set_event`/`get_event` + `sleep`.
4. Recovery dispatches by **stored name** (+ `config_name`), decoding **stored serialized
   inputs** into the type captured at registration — no decorators, no reflection.

**Scope decision:** target **Postgres only** for v1 (sqlx::Postgres, wire-compatible with
CockroachDB). Keep a `Dialect` seam so SQLite/CRDB can be added later, but do not implement
them in v1. The schema we install is the **Go** schema (stop at migration 40; no
`schedule_name` column — that is Python's migration 41).

---

## 1. Crate & Module Layout

```
dbos/
  Cargo.toml
  src/
    lib.rs                 // pub re-exports; crate docs
    error.rs               // DbosError (thiserror), error codes, SQLSTATE classifiers
    config.rs              // Config + builder, env overrides, app-version/executor-id
    serialize/
      mod.rs               // Serializer trait, resolve_decoder, nil markers
      json.rs              // DBOS_JSON = base64(serde_json)
      portable.rs          // portable_json: raw compact JSON + {positionalArgs,namedArgs}
      error.rs             // PortableWorkflowError, serialize/deserialize_workflow_error
    db/
      mod.rs               // SystemDatabase: PgPool wrapper, all SQL ops
      migrate.rs           // migration runner, Migration table, sanitize_ident
      migrations/          // the embedded .sql files (copied from Go ref, verbatim)
      status.rs            // insert_workflow_status, update_outcome, await_result, list
      steps.rs             // check_operation_execution, record_operation_result, child
      notifications.rs     // send/recv SQL, get/set event SQL, streams SQL
      listener.rs          // PgListener task + polling fallback + WaiterRegistry
    runtime/
      mod.rs               // Dbos handle, DbosInner, launch/shutdown
      context.rs           // DbosContext, WorkflowContext, WorkflowState, step-id counter
      registry.rs          // WorkflowRegistry, ErasedWorkflow, register_workflow
      workflow.rs          // run_workflow lifecycle, handles, child workflows
      step.rs              // run_step retry-with-backoff
      recovery.rs          // recover_pending_workflows
    queue/
      mod.rs               // WorkflowQueue, RateLimiter, registry
      runner.rs            // per-queue poll loop, dequeue, transition_delayed
    scheduler/
      mod.rs               // cron parsing wrapper, schedule CRUD, reconciler
      cron.rs              // 6-field seconds-first cron (wraps `cron` crate / saffron)
    client.rs              // external Client (enqueue/manage, no runtime tasks)
    admin/                 // (later) axum admin server
    conductor/             // (later, feature-gated) websocket client
```

Public surface lives in `lib.rs` (see API sketch). Everything in `db/` is `pub(crate)`.

---

## 2. Core Types & Ownership

```
Dbos                 // owned handle returned by DbosBuilder::launch()
  └─ Arc<DbosInner>
       ├─ pool: PgPool
       ├─ schema: String                 // default "dbos"
       ├─ executor_id: String            // DBOS__VMID | config | "local" | uuid (conductor)
       ├─ application_version: String     // DBOS__APPVERSION | config | binary sha256
       ├─ application_id: Option<String>  // DBOS__APPID
       ├─ serializer: Arc<dyn Serializer> // default DBOS_JSON
       ├─ registry: Arc<WorkflowRegistry>
       ├─ queues: Arc<HashMap<String, WorkflowQueue>>
       ├─ active_workflows: DashMap<String, ActiveEntry>   // ownership + worker-cc count
       ├─ workflow_tasks: TaskTracker     // in-flight workflow JoinHandles (drain on shutdown)
       ├─ notif: Arc<WaiterRegistry>      // send/recv/event/stream waiters
       ├─ cancel: CancellationToken       // root shutdown signal
       └─ launched: AtomicBool
```

`DbosContext` is a cheap clone (`Arc<DbosInner>` + optional `Arc<WorkflowState>`). It is
threaded **explicitly** into steps and child workflows (Go-style), not via task-locals — this
keeps the data flow visible and makes child/step boundaries unambiguous.

`WorkflowState` (per running workflow):
```
struct WorkflowState {
    workflow_id: String,
    step_id: AtomicI32,        // init -1; next_step_id() = fetch_add(1)+1  => first op = 0
    within_step: bool,
    is_portable: bool,
    auth: AuthIdentity,        // user / role / roles, inherited by children
    cancel: CancellationToken, // child token; fired on timeout/cancel
}
```

**Step-id determinism (critical):** `next_step_id()` must be called **synchronously
before any `.await`** that could interleave (matches Py/TS `functionIDGetIncrement`). For
parallel children/joins, reserve ids before spawning tasks.

---

## 3. System Database & Migrations

### 3.1 Schema

The final schema is reproduced verbatim in `schema_summary` (DDL). Notes:

- All `*_epoch_ms` / `created_at` / `updated_at` columns are `BIGINT` = ms since epoch → `i64`.
- `priority` is `INTEGER` (i32); `recovery_attempts` is `BIGINT` (i64); `class_name`/`config_name`
  are `VARCHAR(255)`.
- `streams."offset"` is a reserved word — **always double-quote it** in every query.
- `attributes` is `JSONB` (`serde_json::Value`) with a GIN partial index.
- The single-row `dbos_migrations(version BIGINT PK)` records the highest applied migration.

### 3.2 Migration runner (port of `system_database.go` runner)

Do **not** use `sqlx::migrate!` — it cannot do schema-name parameterization or the
online/autocommit split. Instead:

- `include_str!` each `migrations/NN_*.sql` into a `const`.
- Build `Vec<Migration { version: i64, render: fn(&SchemaCtx) -> String, online: bool }>`
  mirroring `buildMigrations`. `render` fills `%s`/`{}` slots with `sanitize_ident(schema)`
  (double-quote, double embedded quotes — the pgx `Identifier.Sanitize` equivalent), **except**
  migration 10's catalog comparison literal `n.nspname = '<raw>'` which uses the **raw**
  unquoted schema string.
- **Online migrations** (versions `{22,23,24,25,26,27,29,30,31,32,34,35,37}`) contain
  `CREATE/DROP INDEX CONCURRENTLY` and MUST run **outside any transaction** (autocommit);
  the version bump is a **separate** round-trip. Before the first online migration, run
  `cleanup_invalid_indexes` (drop `NOT indisvalid` indexes left by a crashed prior run).
- **Catalog (non-online) migrations** run the SQL **and** the version bump in **one
  transaction**.
- Multi-statement migration files (1, 5, 11, 12, 36, 38, 39, 40) cannot go through a prepared
  `sqlx::query`. Use `sqlx::raw_sql(&body).execute(&mut *tx)` (simple-query protocol) for the
  in-tx path and `sqlx::raw_sql(&sql).execute(&pool)` for the autocommit path.
- Migration 1 appends the listen/notify block (`1_initial_dbos_schema_listen_notify.sql`)
  for Postgres. Migrations 14/38 install the `enqueue_workflow`/`send_message` plpgsql
  functions verbatim — keep them for external-SQL-client compatibility even though the Rust
  SDK inserts rows directly.
- Version bump: `INSERT` when `last_applied == 0`, else `UPDATE` (single-row table, no WHERE).
- Wrap `run_migrations` in `retry_with_backoff` (base 1s, factor 2, cap 120s, max 10) for
  transient connection errors.
- `should_migrate(pool, schema)`: true if schema missing OR `dbos_migrations` missing OR
  recorded version < latest. Fast early-exit otherwise.

CockroachDB branches (`is_cockroach`, detected via `crdb_version` server param) are stubbed
behind the `Dialect` trait but not implemented in v1.

---

## 4. Serialization & Error Taxonomy

### 4.1 Serializer

```
trait Serializer: Send + Sync {
    fn name(&self) -> &str;                                   // "DBOS_JSON" | "portable_json" | custom
    fn encode(&self, v: &serde_json::Value) -> Result<Option<String>>; // None for nil marker
    fn decode(&self, s: Option<&str>) -> Result<serde_json::Value>;
}
```

The persistence boundary is `serde_json::Value` (Rust has no `any`/reflection like Go's
`Serializer[T]`); each registered workflow owns the concrete `serde` (de)serialize.

Built-ins:
- **`DBOS_JSON`** (default, native): `encode = base64(serde_json::to_vec)`, `decode =
  serde_json::from_slice(base64_decode)`. nil → literal marker `"__DBOS_NIL"`. **Note: this
  is base64(JSON), not raw JSON** — match Go for wire-compat.
- **`portable_json`** (cross-language): compact `serde_json::to_string` (no base64). Inputs
  wrapped in `{"positionalArgs":[...],"namedArgs":{...}}`; errors in
  `{"name","message","code?","data?"}`. nil → literal `"null"`. Dates → RFC3339-Z; big ints →
  base-10 string; non-string map keys → error.

`resolve_decoder(stored_name, custom)`: `"portable_json"` → portable; matches custom name →
custom; `""` or `"DBOS_JSON"` → json; else error. **Always dispatch on the per-row
`serialization` column, never on the live context default.**

`safe_decode_*` variants (for `list_workflows`, introspection) log a warning and return the
raw string on failure instead of erroring.

### 4.2 Error type

Single `thiserror` enum with **Go's numeric codes** (iota+1, primary target):

```
ConflictingId=1, Initialization=2, NonExistentWorkflow=3, ConflictingWorkflow=4,
WorkflowCancelled=5, UnexpectedStep=6, AwaitedWorkflowCancelled=7, ConflictingRegistration=8,
WorkflowUnexpectedType=9, WorkflowExecution=10, StepExecution=11, DeadLetterQueue=12,
MaxStepRetriesExceeded=13, QueueDeduplicated=14, PatchingNotEnabled=15, Timeout=16,
NoApplicationVersions=17
```

`Display` = `"DBOS Error {code}: {message}"`. Provide `fn code(&self) -> i32` and `is_code(c)`
matching Go's `errors.Is`-on-code semantics. Variants carry context fields (workflow_id,
destination_id, step_name, queue_name, dedup_id, max_retries, expected/recorded name).

SQLSTATE classifiers (on `sqlx::Error::Database` → `PgDatabaseError.code()`):
- `23505` on dedup partial index → `QueueDeduplicated`; on `operation_outputs` PK →
  `ConflictingId`.
- `23503` on notifications FK → `NonExistentWorkflow(dest)`.
- `40001` (serialization_failure) / `55P03` (lock_not_available) → `is_contention()` → queue
  backoff.

**Native error fidelity caveat (match Go):** for `DBOS_JSON`, the error column stores only
`err.to_string()`; on recovery/await it deserializes to a plain `PlainError(String)` — the
original variant/code is **lost** across the DB. Only `portable_json` round-trips
`{name,message,code,data}` into a `PortableWorkflowError`. Do not attempt to downcast a
native-format DB error back to `DbosError`.

---

## 5. Registration & Recovery Without Decorators (the central design)

Mirror Go's two-map registry exactly.

```
type ErasedFut = Pin<Box<dyn Future<Output = Result<serde_json::Value, DbosError>> + Send>>;
trait ErasedWorkflow: Send + Sync {
    fn run_encoded(&self, ctx: DbosContext, encoded_input: Option<String>,
                   serialization: String, opts: RunOpts) -> ErasedFut;
}

struct RegistryEntry {
    fqn: String, name: String,
    class_name: Option<String>, config_name: Option<String>,  // Option<String>: None=unset, Some("")=default instance
    max_retries: i64,                                          // default 100
    cron: Option<String>,
    handler: Arc<dyn ErasedWorkflow>,
}

struct WorkflowRegistry {
    by_fqn: DashMap<String, RegistryEntry>,
    name_to_fqn: DashMap<String, String>,   // recorded name (or "name/config") -> fqn
}
```

`register_workflow::<P, R, F, Fut>(builder, name, f)`:
- `P: DeserializeOwned + Serialize + Send + 'static`, `R: Serialize + Send + 'static`,
  `F: Fn(WorkflowContext, P) -> Fut + Send + Sync + 'static`,
  `Fut: Future<Output = Result<R, DbosError>> + Send`.
- Builds a concrete `ErasedWorkflow` whose `run_encoded`: (1) decodes `encoded_input` per
  `serialization` — `portable_json` → unwrap `positionalArgs[0]` into `P`; else
  `resolve_decoder` → `P` — into the statically-known `P`; (2) calls `f`; (3) serializes `R`
  back to `Value`. This captures `P`/`R` at registration, so recovery re-materializes the
  correct type from the opaque DB string with no reflection.
- **Rust has no `runtime.FuncForPC`**, so registration takes an **explicit `name: &str`** (the
  durable `workflow_status.name`). Configured instances register under
  `format!("{name}/{config}")` (`instance_qualified_name`).
- Panics (or errs in builder) on duplicate fqn/name (`ConflictingRegistration`) or
  registration after `launch()`.

`recover_pending_workflows(ctx, executor_ids)` (port `recovery.go`):
- `list_workflows(status=[PENDING], executor_ids, application_version=[ctx.app_version] if set,
  load_input=true)`.
- For each row: if `queue_name` non-empty → `clear_queue_assignment` (PENDING→ENQUEUED, null
  `started_at`); push a polling handle if cleared; **do not re-run directly**.
- Else `lookup = instance_qualified_name(name, config_name)`; `name_to_fqn[lookup]` →
  `by_fqn[fqn]` → entry; call
  `entry.handler.run_encoded(ctx_with(WorkflowId(id), is_recovery, auth), row.inputs,
  row.serialization)`. Auth identity re-attached so recovered children inherit it.
- Missing registry entry → log + skip (non-fatal), matching Go.
- Input deserialization failure → immediately mark workflow ERROR (else it loops PENDING
  forever).

---

## 6. Workflow & Step Execution Engine

### 6.1 run_workflow lifecycle (port `(*dbosContext).RunWorkflow`)

1. Resolve registry entry by name → `max_retries`, `class_name`, `config_name`.
2. Validate option combinations (queue/delay/partition/dedup rules — see §7).
3. Detect child workflow (parent `WorkflowState` present): forbid spawning from within a step;
   advance parent `step_id`; inherit auth; generate id `format!("{parent}-{step_id}")` when not
   provided, else `uuid`. For a child, first `check_child_workflow(parent, step_id)` — if a
   `child_workflow_id` is already recorded, return a polling handle (replay short-circuit).
4. Compute status (PENDING for direct, ENQUEUED/DELAYED for queued), `delay_until`, deadline
   (from ctx timeout, only for non-queued; queued deadlines are computed at dequeue).
5. Serialize input (skip if already encoded e.g. recovery; portable → envelope).
6. One transaction: `insert_workflow_status` (UPSERT) + (child) `record_child_workflow`.
7. `should_skip` decision → if enqueued OR already terminal OR another executor's `owner_xid`
   present (and not dequeue/recovery) OR already active locally → commit + return polling
   handle. Else commit, build `WorkflowState`, spawn the body via `tokio::spawn` (held in
   `workflow_tasks` so it survives parent drop, per Python #710 fix), and on completion
   `update_workflow_outcome` (SUCCESS/ERROR/CANCELLED) + send via `oneshot`.
8. Deadline: a sibling `tokio::select!`/`timeout` task cancels the workflow row (status
   CANCELLED) at the deadline, racing the body's return. Cancellation wins (terminal CANCELLED
   cannot transition to SUCCESS/ERROR — enforced in `update_workflow_outcome`'s WHERE NOT
   clause).
9. **ID-conflict path:** if the body insert hits `ConflictingId`, await `await_workflow_result`
   and decode the recorded (still-encoded) result into `R`.

### 6.2 insert_workflow_status UPSERT (port `system_database.go:933`, verbatim SQL)

`recovery_attempts` = 1 for PENDING insert, 0 for ENQUEUED/DELAYED. `ON CONFLICT
(workflow_uuid) DO UPDATE`:
```
recovery_attempts = CASE WHEN EXCLUDED.status NOT IN ('ENQUEUED','DELAYED')
                         THEN recovery_attempts + $increment ELSE recovery_attempts END,
updated_at = EXCLUDED.updated_at,
executor_id = CASE WHEN EXCLUDED.status IN ('ENQUEUED','DELAYED') THEN existing.executor_id
                   ELSE EXCLUDED.executor_id END
RETURNING recovery_attempts, status, name, queue_name, ..., owner_xid
```
`$increment` = `(is_dequeue || is_recovery) as i64`. After return: validate name/queue match
(`ConflictingWorkflow` on mismatch). **DLQ check** (port `:1121`): if status NOT IN
(SUCCESS,ERROR) AND `max_retries > 0` AND `recovery_attempts > max_retries + 1` → UPDATE to
`MAX_RECOVERY_ATTEMPTS_EXCEEDED` (clear dedup/started_at/queue_name) WHERE status='PENDING',
**commit**, return `DeadLetterQueue`. `owner_xid` = fresh uuid per attempt; `should_execute`
compares returned vs generated.

### 6.3 run_step (port `RunAsStep` + `executeStepWithRetry`)

```
run_step<R, F: Fn() -> Fut>(ctx, opts: StepOpts, f) -> Result<R, DbosError>
```
- Requires a workflow context; if `within_step` already, just `f().await` (no nesting, 1 step).
- Allocate `step_id`; `check_operation_execution(wfid, step_id, name)`:
  - row exists → decode recorded output/err and return (memoized replay); also surfaces
    `WorkflowCancelled` if status CANCELLED and `UnexpectedStep` on `function_name` mismatch.
  - none → run body with retry: `max_retries` (default **0** = no retries, Go semantics),
    `base_interval` 100ms, `max_interval` 5s, `backoff_factor` 2.0, optional `retry_predicate`.
    Delay = `base` for attempt 1, else `min(base*factor^(n-1), max)`; abort on
    `ctx.cancel.cancelled()`. On final failure → `MaxStepRetriesExceeded` (joined errors).
- **Record the FINAL result** (success OR `MaxStepRetriesExceeded`) into `operation_outputs`, so
  the step is **never** re-run on workflow replay. **Cancelled steps
  (`WorkflowCancelled`) are NOT recorded** (re-run on resume).

### 6.4 Handles & GetResult

`WorkflowHandle<R>` enum: `Owned(oneshot::Receiver<Outcome>)` (executor owns the run) or
`Polling { id }` (any observer). `get_result()`:
- Owned: select on the oneshot vs ctx cancel vs optional timeout. Second call errors (channel
  closed).
- Polling: `await_workflow_result` poll loop (SELECT status/output/error every `poll_interval`),
  decode into `R`. CANCELLED → `AwaitedWorkflowCancelled`; MAX_RECOVERY_ATTEMPTS_EXCEEDED →
  `DeadLetterQueue(attempts-2)`.
- **Inside a workflow**, GetResult is itself a recorded step named `"DBOS.getResult"` (check/record
  at next step id) so the parent's replay is deterministic.

Built-in step names (exact, load-bearing for replay): `DBOS.send`, `DBOS.recv`, `DBOS.setEvent`,
`DBOS.getEvent`, `DBOS.sleep`, `DBOS.getResult`, `DBOS.writeStream`, `DBOS.closeStream`, plus the
child-enqueue step named after the **child workflow's registered name**.

---

## 7. Durable Queues

### 7.1 Types

```
struct WorkflowQueue {
    name: String,
    worker_concurrency: Option<u32>,    // per-executor cap (in-memory count)
    global_concurrency: Option<u32>,    // cross-executor cap (DB PENDING count); json "concurrency"
    priority_enabled: bool,
    rate_limit: Option<RateLimiter>,    // { limit: u32, period: Duration }
    max_tasks_per_iteration: u32,       // default 100
    partition_queue: bool,
    base_polling: Duration,             // default 1s
    max_polling: Duration,              // default 120s
}
const INTERNAL_QUEUE_NAME = "_dbos_internal_queue";   // always listened
```
Validate at registration: `worker_concurrency <= global_concurrency`, positive polling,
limiter has both fields. Register before launch; conflict → `ConflictingRegistration`.

### 7.2 Enqueue

Validate: delay→queue, partition_key→queue, partition_key XOR dedup_id, dedup_policy→queue+dedup_id,
queue exists, `partition_queue ↔ partition_key` consistency. Status DELAYED (with
`delay_until_epoch_ms`) iff delay else ENQUEUED. recovery_attempts=0, priority=`priority.unwrap_or(0)`.
Single tx: `insert_workflow_status` (+ child link). Return polling handle (no task spawned).
Dedup: `reject` → `QueueDeduplicated`; `return-existing` → retry loop: on conflict
`SELECT workflow_uuid WHERE queue_name=$1 AND deduplication_id=$2`, return existing (retry if
slot freed).

### 7.3 Dequeue (port `dequeueWorkflows`, the exactly-once core)

- `snapshot = global_concurrency.is_some() || rate_limit.is_some()`. Begin tx with
  `SET TRANSACTION ISOLATION LEVEL REPEATABLE READ` if snapshot, else READ COMMITTED.
- **Rate gate:** `COUNT(*) WHERE queue_name=$q AND rate_limited=TRUE AND status NOT IN
  ('ENQUEUED','DELAYED') AND started_at_epoch_ms > now-period [AND partition]`; if ≥ limit →
  return [].
- **maxTasks:** start at `max_tasks_per_iteration`; if `worker_concurrency` →
  `max(worker_cc - local_running_count, 0)` (in-memory count, no DB query); if
  `global_concurrency` → `min(maxTasks, max(global_cc - pending_count, 0))`. ≤0 → return.
- **Candidate select:**
  `SELECT workflow_uuid FROM workflow_status WHERE queue_name=$q AND status='ENQUEUED' AND
  (application_version=$v OR application_version IS NULL) [AND queue_partition_key=$k]
  ORDER BY priority ASC, created_at ASC <lock> LIMIT maxTasks` where `<lock>` =
  `FOR UPDATE SKIP LOCKED` if no global concurrency, else `FOR UPDATE NOWAIT`.
- **Claim** each (stop early if `claimed + num_recent >= limit`):
  `UPDATE ... SET status='PENDING', application_version=$v, executor_id=$e,
  started_at_epoch_ms=now, rate_limited=(rate_limit.is_some()),
  workflow_deadline_epoch_ms = CASE WHEN timeout set AND deadline NULL THEN now+timeout ELSE
  deadline END WHERE workflow_uuid=$id AND status='ENQUEUED' RETURNING name, inputs,
  serialization, config_name`. Only rows that transitioned (RETURNING row present; treat
  `RowNotFound` as lost-race-skip) are claimed — **the exactly-once guard**.
- Commit **only if ≥1 claimed** (avoid empty-commit WAL/XID churn); else rollback. Contention
  (`is_contention`) → backoff.

### 7.4 Poll loop & supporting ops

Per-queue tokio task: `select!{ cancelled => return, sleep(current*jitter[0.95,1.05]) => {} }`;
`transition_delayed_workflows` (`UPDATE ... SET status='ENQUEUED' WHERE status='DELAYED' AND
delay_until_epoch_ms <= now`); if partitioned, `get_queue_partitions` (DISTINCT non-null
partition keys with ENQUEUED work) and run dequeue **per partition** (limits are per-partition);
dispatch each claimed wf via `registry[name(+config)].handler.run_encoded(ctx, inputs,
serialization, is_dequeue)` (log+continue per-wf); adjust `current = if backoff
{ min(current*2, max) } else { max(current*0.9, base) }`. Worker-cc count maintained via
`DashMap<(queue, partition), AtomicUsize>` updated on workflow start/terminal.

---

## 8. Notifications, Events & Streams

### 8.1 Primitives

- **Send/Recv** — FIFO mailbox in `notifications`, keyed by `(destination_uuid, topic)`. Empty
  topic stored as sentinel `"__null__topic__"`. Recv consumes the **oldest unconsumed** row
  (`ORDER BY created_at_epoch_ms ASC LIMIT 1`, pinned by `message_uuid` in the UPDATE so exactly
  one row is consumed even on same-ms ties), sets `consumed=true` (rows retained, GC'd by FK
  cascade). Only **one active Recv per (wf, topic)** → second registration =
  `ConflictingId`.
- **SetEvent/GetEvent** — latest value in `workflow_events` (upsert on `(workflow_uuid,key)`),
  plus append-only `workflow_events_history` keyed by `function_id` (for fork). GetEvent returns
  the latest; concurrent GetEvent waiters for the same key **share** one waiter.
- **Streams** — append-only, 0-based `"offset"` per `(workflow_uuid, key)`. `WriteStream` appends
  at `COALESCE(MAX("offset"),-1)+1` (retry on PK 23505 race); `CloseStream` writes sentinel
  `"__DBOS_STREAM_CLOSED__"`. `ReadStream` tails from an offset, stops on sentinel **or** when the
  producing workflow leaves PENDING/ENQUEUED (do one **final drain** first). Snapshot read returns
  after draining without blocking for close.

### 8.2 OAOO / step recording

Inside a workflow, Send/Recv/SetEvent/GetEvent/WriteStream/CloseStream consume deterministic step
id(s) and record into `operation_outputs`. **Recv and GetEvent each allocate TWO step ids in
order: the op-result id, THEN an internal durable-sleep id** (the timeout). The DB mutation +
`operation_outputs` insert run in the **same transaction**. Send/Sleep/WriteStream are forbidden
inside a step and outside a workflow (except Send and GetEvent may be called **outside** a workflow
— non-durable). FK violation on send → `NonExistentWorkflow(dest)`.

### 8.3 Blocking, wakeup, durable timeout

`WaiterRegistry` = three `DashMap<String, Arc<Notify>>` (notifications / events / streams) keyed by
payload `"{id}::{topic_or_key}"`. Wait pattern (lost-wakeup-safe): **register waiter FIRST**, then
probe DB, then `select!{ notify.notified(), sleep(min(deadline-now, recheck)), cancel.cancelled() }`,
re-probe on each wake. Reconnect → force re-poll all waiters.

`PgListener` task LISTENs on all three channels; on `NOTIFY` parse `"id::key"` and
`notify_waiters()` the matching waiter. Keep a slow polling safety net (1s) for dropped notifies and
for stream readers (which may be non-workflow processes). Cockroach/SQLite use polling only.

**Durable sleep** (`record_sleep`): first run records absolute `end_time = now + duration` as the
output of a `DBOS.sleep` step; on replay reads it back and sleeps `max(0, end - now)`. `skip_sleep`
records/reads the deadline without blocking (used by Recv/GetEvent timeouts). Unique-violation on the
record is swallowed (a racing recorder won).

---

## 9. Crash Recovery & Executor Lifecycle

**Recovery scoping (critical):** only PENDING rows where `executor_id == self.executor_id` AND
`application_version == self.application_version` are recovered. This makes rolling deploys safe.

**Launch** (idempotent via `launched` CAS), in order:
1. Run migrations; compute `application_version` if empty (binary SHA-256 of `current_exe()` +
   crate version appended); regenerate `executor_id` as uuid if conductor enabled.
2. `create_application_version` + warn if not latest.
3. Spawn queue runner tasks, scheduler reconciler, notification listener (tracked in
   `TaskTracker`).
4. Ensure internal queue exists.
5. `recover_pending_workflows(self, &[executor_id])` — awaited synchronously (block launch, map
   errors to Initialization). Since Rust registration is all-before-launch, "handler not found"
   is a logged skip.

**Shutdown** (`shutdown(timeout)`):
1. `cancel.cancel()`.
2. `timeout(t, queue_runner_done)`; stop scheduler/conductor/admin with per-step timeouts.
3. **After producers stop**, `timeout(t, workflow_tasks.wait())` to drain in-flight workflows
   (avoid the Add-vs-Wait race Go documents).
4. `pool.close().await`.

**Dead-letter:** threshold `recovery_attempts > max_recovery_attempts + 1` (note the `+1`;
default `max_recovery_attempts = 100`; 0 disables). Transition committed in its own tx before
returning the error; guarded by `AND status='PENDING'`. `resume_workflow` clears DLQ status and
re-enqueues.

---

## 10. Scheduler (cron) & Durable Sleep

Two paths:
- **Static** `register_workflow(..., WithSchedule(cron))`: in-process cron entry; fires
  `run_workflow` on the internal queue. No DB row.
- **DB-backed**: `workflow_schedules` rows + a reconciler task polling every 30s (fast-poll 1s
  for the first 60s after launch). Reconciler diffs DB schedules against installed tokio tasks
  keyed by **`schedule_id`** (so a re-applied schedule with a new uuid restarts the loop);
  ACTIVE→install (+ optional backfill), missing/PAUSED/replaced→cancel.

Per-tick firing: jitter `[0, min(interval/10, 10s))`; wfID `format!("sched-{name}-{tick.to_rfc3339()}")`
(**canonical RFC3339 seconds** for both the existence check and enqueue — pin one format);
best-effort existence pre-check (perf only; correctness via UNIQUE `workflow_uuid`); enqueue
ENQUEUED with `(scheduled_time, context)` input, the schedule's queue (or internal), and the
**latest** application version; update `last_fired_at`. Manual triggers use `-trigger-` infix.

Cron: 6-field **seconds-first** with `@descriptors`, ranges/steps/lists/names, optional `CRON_TZ`/
tz arg (chrono-tz; empty → UTC). Wrap the `cron`/`saffron` crate; verify seconds-first support or
port croniter.

---

## 11. External Client & Workflow Management

`Client` = `PgPool` + serializer, **no runtime tasks** (no listener/queue runner/scheduler/recovery),
**no migrations**. Methods: `enqueue` (by name, returns polling handle), `send`, `get_event`,
`retrieve_workflow`, `list_workflows`/`list_queued_workflows` (default `load_input=load_output=false`),
`list_workflow_steps`, `cancel_workflow(s)`, `resume_workflow(s)`, `fork_workflow`,
`delete_workflows`, `set_workflow_delay`, `garbage_collect`, application-version ops.

Bulk lifecycle ops use Postgres data-modifying CTEs (`WITH existing AS (SELECT ... WHERE
workflow_uuid = ANY($1)), updated AS (UPDATE ... RETURNING ...) SELECT ... FROM existing`), binding
ids as a single `&[String]` (`= ANY($n)`). Singular wrappers map empty → `NonExistentWorkflow`; plural
silently skip missing. State guards: cancel skips SUCCESS/ERROR/CANCELLED and clears
queue/dedup/started_at + sets completed_at; resume skips SUCCESS/ERROR, sets recovery_attempts=0,
clears deadline/dedup/started_at/completed_at, re-enqueues onto internal queue (or override);
`set_workflow_delay` only touches DELAYED rows (silent no-op otherwise). Fork copies
operation_outputs + workflow_events_history + latest workflow_events + streams for
`function_id < start_step`, sets `forked_from` on the new row and `was_forked_from=TRUE` on the
original. GC never deletes PENDING/ENQUEUED/DELAYED; with both `rows_threshold` and `cutoff` take the
more restrictive bound.

`config_name` is `Option<String>` with three meaningful states (None=unset, Some("")=default
instance, Some(name)=named) — preserve for the recovery lookup key.

---

## 12. Admin Server, Conductor, Telemetry (later milestones)

- **Admin server** (axum, default port 3001, default **OFF** matching Go): `GET /dbos-healthz`,
  `POST /dbos-workflow-recovery`, `GET /deactivate`, `GET /dbos-workflow-queues-metadata`,
  `POST /dbos-garbage-collect` (no-op TODO, 204), `POST /dbos-global-timeout`, `POST /workflows`,
  `POST /queues`, `GET /workflows/:id`, `GET /workflows/:id/steps`, `POST /workflows/:id/{cancel,
  resume,fork}`, `GET /conductor`. **Timestamps in workflow list/get responses are epoch-millisecond
  STRINGS** (the console requires this); priority always emitted even when 0; Input/Output passed
  through as already-serialized JSON strings; steps endpoint uses **integer** epoch-ms.
- **Conductor** (feature-gated, deferred): tokio-tungstenite reconnecting WS client; serde
  tagged-enum protocol; single writer task (serializes all writes); ping 20s / read-timeout 30s;
  backoff x2 cap 30s with jitter. Most handlers delegate to the same runtime/admin ops. v1 can stub
  to `executor_info` + `recovery` + list/cancel/resume.
- **Telemetry**: `tracing` spans around workflow/step/queue ops; OTLP export deferred.
- **Debouncer / Kafka / streams-async**: later optional milestones.

---

## 13. Testing Strategy

Port the **Go integration tests** as the Rust acceptance suite (Go is authoritative). Use
`#[tokio::test(flavor = "multi_thread")]`. Replace Go's `Event`/`sync.Cond` handshakes with
`tokio::sync::Notify`; `require.Eventually` with an `eventually(timeout, interval, async)` helper;
`goleak` with a pool-drain + task-tracker assertion. The `setup_dbos` fixture cleans the DB (DELETE
`workflow_status` CASCADE if at latest version, else drop+recreate) and uses a **unique schema per
test** (`dbos_test_<uuid>`) so `cargo test` parallelism is safe.

**Drop** (do not port) the Go-runtime-specific tests: FQN-from-function-pointer collisions, goleak,
value-vs-pointer-receiver method-value tests, and the Python framework-integration tests
(FastAPI/Flask/Kafka/SQLAlchemy/decorator). Replace FQN-collision with "two distinct closures under
the same name conflict at registration."

The canonical durability test pattern (repeated ~20×): run to completion → `set_workflow_status_pending(id)`
→ `recover_pending_workflows([executor])` → assert identical result, identical recorded step count
(steps replayed, not re-executed), and `recovery_attempts` bumped by 1.

---

## 14. Cross-SDK compatibility (shared system database)

This port is **wire-compatible with the official SDKs on a shared Postgres system database** at the
schema/metadata level (same table and column names). The boundaries below are proven by tests
(`tests/cross_sdk_serialization.rs`, `tests/cross_sdk_interop.rs`) or documented as unsupported.

### Proven compatible
- **Reading TS-written values.** The `js_superjson` reader decodes real `@dbos-inc/dbos-sdk` 4.21.x
  bytes — both the modern SuperJSON envelope (`{json, meta, __dbos_serializer:"superjson"}`) and the
  legacy `DBOSJSON` format (`{dbos_type:"dbos_Date"|"dbos_BigInt"}` wrappers). Fixtures are generated
  by `tests/fixtures/gen_ts_fixtures.mjs` using the same `superjson` library and committed as static
  dumps (regenerable on SDK bumps), so CI needs no Node.
- **Rust → TS values.** This crate writes `portable_json` (plain compact JSON) by default, which is
  exactly what TS's `DBOSPortableJSON.parse` (a `JSON.parse`) reads. No conversion needed.
- **Reading Go-written values.** Go's `DBOS_JSON` (base64 JSON) is read directly; Rust can also write
  it for Go interop.
- **Migration-version coordination.** A database migrated by the TS SDK ends at TS's positional
  version (**63** at 4.21.6); this crate ends at its numbered `SCHEMA_VERSION` (**40**). Because the
  runner only applies migrations with `version > current`, pointing this crate at a TS-migrated DB
  runs **no** migrations (`should_migrate → false`) and leaves the TS version untouched — verified
  against a version-63 schema. (Schema *equivalence* between the two — including the `attributes`
  column, identical DDL at 4.21.6 — is taken as a premise; the test exercises the coordination code
  path, not byte-equal schema diffing.)

### Lossy (documented lowerings when reading foreign rows)
Rich JS types have no exact Rust/JSON equivalent and are lowered when read: **Date → ISO-8601
string**, **BigInt → JSON number** (values beyond `i64` are out of range), **Map → object**
(string keys), **Set → array**, **`undefined` → `null`**. Round-tripping such a value back to TS
yields the lowered form, not the original JS type.

### Fallback for `serialization = NULL` rows
Older rows (pre the `serialization` column) and some cross-SDK rows have `serialization = NULL`. The
origin SDK is then unknown, so decoding is **best-effort**: SuperJSON / plain JSON / legacy DBOSJSON
is tried first, then Go base64 `DBOS_JSON`. This deliberately does **not** assume base64 (a TS-origin
NULL row is plain JSON; base64-decoding it would corrupt data — a fixed latent bug). A TS-native
*input* stored under `NULL` as a bare positional-args array is the one shape that still needs an
explicit `js_superjson`/portable hint to unwrap; document it if you mix SDKs on inputs.

### Explicitly unsupported
- **Python `py_pickle`.** Python's default serializer is base64-pickle, which is not portable to
  Rust. Re-serialize such rows to `portable_json` on the Python side before a Rust app reads them.
- **Running the TS migrator *after* this crate migrated to version 40 (reverse direction).** The two
  schemes' version integers do **not** correspond (Rust's `40` ≠ TS's `40`th migration). A TS
  migrator seeing `dbos_migrations.version = 40` would replay its entries `41..63`, which are
  different operations than this crate ran, relying on TS swallowing `already exists` errors. Treat
  this ordering as unsupported: pick **one** SDK to own migrations, or start the database fresh with
  the SDK that will migrate it. The safe direction (TS migrates → Rust reads/operates without
  migrating) is supported.

### Recovery contract (executor identity & application version)
Crash recovery is gated by `status = PENDING AND executor_id = ANY(<set>) AND application_version =
<current>`. Consequences, all tested:
- A PENDING workflow at a **different `application_version`** is **not** recovered.
- A workflow owned by a **different `executor_id`** is **not** recovered unless that id is in the
  recovery set.
- A Rust executor **can adopt** a workflow orphaned by a TS/Go executor on the shared DB via
  `Dbos::recover_workflows([foreign_executor_id])`, **provided the `application_version` matches**.

**Therefore mutual TS↔Rust recovery is not automatic** — it requires coordinating `executor_id`
(supplying the foreign id to the recovery call) **and** `application_version` (the orphaned workflow's
version must equal the recovering executor's). This is by design: it prevents an executor running
code at version X from resuming a workflow checkpointed by incompatible code at version Y.
