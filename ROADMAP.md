# ROADMAP.md — DBOS Transact Rust port

Milestones are ordered by dependency. Each is small enough to implement and test
independently. Per milestone: **Scope**, **Deliverables**, **Reference tests to port**,
**Acceptance criteria**. Postgres-only for v1 (a live Postgres is required for the integration
suite; use a unique schema per test for parallelism).

---

## M0 — Scaffold, Config, Errors, Serialization

**Scope.** Crate skeleton, dependency wiring, `Config` + builder, `DbosError` taxonomy, the
`Serializer` trait + two built-ins. No DB engine yet beyond a `PgPool` connect + ping.

**Deliverables.**
- `Cargo.toml` (see recommended deps); `lib.rs` re-exports.
- `error.rs`: `DbosError` enum with Go's numeric codes, `code()`, `is_code()`, SQLSTATE
  classifiers (`is_unique_violation`/`is_foreign_key_violation`/`is_contention`).
- `config.rs`: `Config` + `ConfigBuilder`; env overrides (`DBOS__APPVERSION`, `DBOS__VMID`,
  `DBOS__APPID`); defaults (schema "dbos", admin port 3001, executor "local", app-version =
  binary sha256 + crate version); dialect detection from URL scheme (postgres/postgresql +
  libpq key=value heuristic); password masking helper.
- `serialize/`: `Serializer` trait, `DBOS_JSON` (base64+json, `__DBOS_NIL`), `portable_json`
  (raw compact json, `null`, `{positionalArgs,namedArgs}` + `{name,message,code?,data?}`
  envelopes, RFC3339-Z dates), `resolve_decoder`, `serialize/deserialize_workflow_error`,
  `PortableWorkflowError`, `safe_decode_*`.

**Reference tests to port.**
- `dbos_test.go`: `TestConfig` (env/config precedence, missing-field errors, custom metadata,
  password masking), `TestDetectDialect`, `TestPostgresConnectionStringForms`.
- `serialization_test.go`: `TestSerializer` round-trip subset for the type matrix (struct,
  nil ptr, int, string, `*int`, `**int`, slices, arrays, `[]byte`, maps, named types),
  `TestPortableWorkflowError`, nil-marker handling.

**Acceptance.** Config validation matches Go error strings ("missing required config field:
appName", "one of databaseURL..."); every type in the matrix round-trips through encode→decode
for both formats; native error → plain string, portable error → `{name,message,code,data}`;
`code` accepts number OR string; masking hides credentials in both URL and key=value forms.

---

## M1 — System DB schema & migration runner

**Scope.** Embed all 40 Go migration `.sql` files; port the runner with the online/autocommit
split and schema-name parameterization. No workflow logic yet.

**Deliverables.**
- `db/migrations/*.sql` copied verbatim from the Go ref (incl. `1_*_listen_notify.sql`,
  `14_*`/`38_*` plpgsql functions, `10_*_cockroach` variants kept but unused in v1).
- `db/migrate.rs`: `sanitize_ident`, `Migration { version, render, online }` vec mirroring
  `buildMigrations`, `should_migrate`, `cleanup_invalid_indexes`, `run_migrations` (Tx1 schema +
  `dbos_migrations`; online → autocommit + separate version bump; catalog → one tx; multi-statement
  via `sqlx::raw_sql`; INSERT-vs-UPDATE version bump), `retry_with_backoff` wrapper.
- `Dialect` trait (Postgres impl; CRDB/SQLite stubbed).

**Reference tests to port.**
- `migration_test.go`: `TestShouldMigrate`, `TestOnlineMigrationsAreIdempotent`,
  `TestVersionNotBumpedOnMigrationFailure`, `TestRunnerResumesAfterInvalidIndex`.
- `dbos_test.go`: schema-version assertion (PG version == 40, exactly one `dbos_migrations` row),
  `TestCustomSystemDBSchema` (tables created in a custom schema).

**Acceptance.** Fresh DB → final schema matches `schema_summary` DDL exactly (table/column/index/
function/trigger names verified via `information_schema`/`pg_indexes`/`pg_proc`); re-run is a no-op;
a failed migration does not bump the version; an injected INVALID index is cleaned and rebuilt;
custom schema name works; the `enqueue_workflow`/`send_message` functions exist and are
search_path-pinned. Final version == 40, no `schedule_name` column.

---

## M2 — Core workflow + steps (direct execution, no recovery/queues)

**Scope.** Registry, `DbosContext`/`WorkflowState`, `run_workflow` (direct PENDING path only),
`run_step` with retry/backoff, handles, child workflows, the durable status/step SQL.

**Deliverables.**
- `runtime/registry.rs`: `WorkflowRegistry`, `ErasedWorkflow`, `register_workflow<P,R,F>`
  (explicit name, instance-qualified key, duplicate/after-launch errors).
- `runtime/context.rs`: `WorkflowState`, step-id counter (init -1, pre-increment), auth identity.
- `db/status.rs`: `insert_workflow_status` UPSERT (verbatim CASE on recovery_attempts/executor_id),
  `update_workflow_outcome` (CANCELLED guard), `await_workflow_result`, `list_workflows` (filter
  matrix).
- `db/steps.rs`: `check_operation_execution` (presence + cancelled + unexpected-step), 
  `record_operation_result`, `record_child_workflow`, `check_child_workflow`.
- `runtime/workflow.rs`: `run_workflow` (insert → spawn → outcome), `WorkflowHandle<R>`, owner_xid /
  `should_skip`, ID-conflict await path, child id `{parent}-{step_id}` + replay short-circuit,
  `DBOS.getResult` step.
- `runtime/step.rs`: `run_step` retry loop (defaults: max_retries 0, base 100ms, max 5s, factor 2),
  cancelled-step-not-recorded rule.

**Reference tests to port.**
- `workflows_test.go`: `TestWorkflowsRegistration` (double-reg/after-launch errors), `TestSteps`
  (retries/backoff/predicate/step-names, step-within-step = 1 step), `TestChildWorkflow`,
  `TestWorkflowIdempotency`, `TestNoConcurrentWorkflowSameID`, `TestWorkflowHandles`,
  `TestSpecialSteps` (8 built-ins in order), `TestRunWorkflowProvidedVsRegisteredDivergence`.
- `system_database_test.go`: `TestBackoffWithJitter`.

**Acceptance.** Same workflow id runs the body exactly once and second `run_workflow` returns a
polling handle; steps get 0-based sequential ids and exact built-in names; step retries =
N+1 attempts then `MaxStepRetriesExceeded` (joined errors) and the final error is recorded;
`get_result` is single-shot for Owned handles; child id = `{parent}-{step_id}`, child-from-step
rejected; unexpected step name on existing function_id → `UnexpectedStep`; different fn same id →
`ConflictingWorkflow`.

---

## M3 — Crash recovery & dead-letter queue

**Scope.** `recover_pending_workflows`, executor/app-version scoping, recovery_attempts bumping,
DLQ transition, launch/shutdown lifecycle (without queues/scheduler yet).

**Deliverables.**
- `runtime/recovery.rs`: list PENDING by executor+app-version (load_input), queue rows →
  `clear_queue_assignment` + polling handle, direct rows → `run_encoded` with is_recovery + auth;
  input-deser-failure → ERROR; missing handler → log+skip.
- DLQ check wired into `insert_workflow_status` (threshold `> max_retries+1`, own tx, commit
  before erroring).
- `runtime/mod.rs`: `launch` (migrate → app-version → recover[executor] → set launched) and
  `shutdown` (cancel → drain `workflow_tasks` → close pool); `TaskTracker`, `CancellationToken`.

**Reference tests to port.**
- `workflows_test.go`: `TestWorkflowRecovery` (Attempts=2 after one recovery, identical result/steps),
  `TestWorkflowDeadLetterQueue` (DLQ after >max+1, `WithMaxRetries(-1)` infinite, resume clears,
  completed never DLQ), `TestAuthPropagation` (parent→child, survives recovery).
- `dbos_test.go`: `TestContext` (Launch/Shutdown, recovery on launch).

**Acceptance.** The canonical pattern (run → set PENDING → recover) yields identical result, identical
step count (steps replayed not re-run), recovery_attempts +1; DLQ at `attempts > max+1` flips status
to `MAX_RECOVERY_ATTEMPTS_EXCEEDED` and `get_result` raises `DeadLetterQueue`; resume clears it;
recovery is scoped to matching executor_id AND application_version; no leaked tasks/connections after
shutdown.

---

## M4 — Durable queues

**Scope.** `WorkflowQueue` registry, enqueue path, per-queue poll loop, dequeue (exactly-once),
concurrency (worker/global), rate limiting, priority, deduplication, partitions, delayed execution,
queue recovery (re-enqueue).

**Deliverables.**
- `queue/mod.rs`: queue types, `RateLimiter`, validation, registry, `INTERNAL_QUEUE_NAME`.
- `queue/runner.rs`: poll loop (jitter, backoff base→max), `transition_delayed_workflows`,
  `get_queue_partitions`, `dequeue_workflows` (isolation/lock selection, rate gate, maxTasks math,
  conditional-UPDATE claim, commit-only-if-claimed), dispatch via registry.
- worker-cc in-memory counter (`DashMap<(queue,partition), AtomicUsize>`).
- enqueue validation (delay/partition/dedup/priority rules) + dedup return-existing retry loop;
  deadline-at-dequeue.

**Reference tests to port.**
- `queues_test.go`: `TestWorkflowQueues`, `TestQueueRecovery`, `TestGlobalConcurrency`,
  `TestWorkerConcurrency` (2-executor), `TestQueueRateLimiter`, `TestQueueTimeouts`
  (`TimeoutOnlySetOnDequeue`), `TestPriorityQueue`, `TestListQueuedWorkflows`,
  `TestPartitionedQueues`, `TestNewQueueRunner`, `TestQueuePollingIntervals`, `TestListenQueues`,
  `TestDelayedExecution`.
- `debouncer_test.go` dedup/return-existing subset (the debouncer itself is later).

**Acceptance.** Status flows ENQUEUED→PENDING→SUCCESS; global cc=1 serializes; worker cc limits
per-executor across two contexts; rate limiter executes in waves of `limit` per `period`; priority
ascending with FIFO tie-break (order `[0,6,7,1,2,3,4,5]`); partitioned limits are per-partition and
partition-key validation errors fire; timeout applied at dequeue (long ENQUEUED wait still succeeds);
dedup reject → `QueueDeduplicated`, return-existing attaches; delay → DELAYED then transition; queue
recovery re-enqueues (does not re-run directly); `queue_entries_are_cleaned_up` holds after each test.

---

## M5 — Notifications & Events (Send/Recv, Set/GetEvent)

**Scope.** Send/Recv mailbox, Set/GetEvent, the LISTEN/NOTIFY listener + polling fallback +
`WaiterRegistry`, durable timeouts (the second step-id reservation). Streams deferred to M9.

**Deliverables.**
- `db/notifications.rs`: send (tx step + plain), recv (two step ids, single-consumer pin, durable
  timeout), set_event (upsert + history), get_event (inside/outside, two step ids inside).
- `db/listener.rs`: `WaiterRegistry` (DashMap of `Arc<Notify>`), `PgListener` task on
  `dbos_notifications_channel`/`dbos_workflow_events_channel`, force-re-poll on reconnect, 1s polling
  safety net, register-before-probe wait helper.
- `record_sleep` durable-deadline machinery (shared with sleep in M6).

**Reference tests to port.**
- `workflows_test.go`: `TestSendRecv` (3 sends → 3 steps, FIFO, single-consumer conflict),
  `TestSetGetEvent` (timeout text, cross-workflow), recv/get_event timeout-through-restart.
- `dbos_test.go`: `TestCustomSystemDBSchema` Send/Recv + SetEvent across a custom schema.
- Cross-check Python `test_send*`/`test_event*` for listener-vs-polling and same-ms consumption.

**Acceptance.** Recv consumes exactly one of N same-ms messages and marks `consumed`; second
concurrent recv on a topic → `ConflictingId`; recv/get_event timeout is durable across a simulated
recovery (only remaining time waited); GetEvent timeout error text exact (`no event found for key
'<k>' within <dur>`); send to non-existent dest → `NonExistentWorkflow`; cross-instance NOTIFY wakes a
waiter; send forbidden inside a step.

---

## M6 — Durable Sleep & Scheduler (cron)

**Scope.** Public `sleep` (durable, replay-stable), 6-field seconds-first cron, schedule CRUD,
reconciler, backfill, trigger, timezone.

**Deliverables.**
- `runtime/step.rs`/sleep: `sleep(ctx, dur)` (inside-workflow, not-in-step guards) over the
  `record_sleep` deadline machinery from M5.
- `scheduler/cron.rs`: seconds-first cron parser/`next_after` (wrap `cron`/`saffron`; verify
  seconds-first or port croniter), `CRON_TZ`/tz via chrono-tz.
- `scheduler/mod.rs`: `workflow_schedules` CRUD, `apply_schedules` (delete+insert with fresh uuid),
  reconciler task (30s, fast-poll 1s first 60s; key by schedule_id), per-schedule tokio loop
  (jitter, existence check, enqueue at latest app-version, `last_fired_at`), `backfill_schedule`,
  `trigger_schedule` (`-trigger-` infix). Static `WithSchedule` path.

**Reference tests to port.**
- `workflows_test.go`: `TestSleep` (remaining sleep replayed, `DBOS.sleep` step), scheduler-stop-halts-firing.
- `schedule_test.go`: `TestScheduleCRUD`, `TestApplySchedules` (+invalid-signature/atomicity),
  `TestScheduleCronValidation`, `TestBackfillSchedule` (+`Recovery`), `TestTriggerSchedule`,
  `TestScheduleWithOptions`, `TestAutomaticBackfillOnRestart`, `TestScheduleCronTimezone`,
  `TestScheduledWorkflowIDUsesCustomName`.
- Cross-check Python `test_croniter.py` for cron edge cases.

**Acceptance.** Sleep deadline fixed at first run; recovery sleeps only the remainder (elapsed <
original); exactly one workflow per (schedule, tick) fleet-wide (UNIQUE id, not the pre-check);
backfill is idempotent (same ids, no Attempts bump) with correct exclusive boundaries; reconciler
installs/removes/replaces on schedule_id change; tz honored; scheduled ids use the **registered name**
and RFC3339 ticks; invalid cron in `apply_schedules` persists nothing.

---

## M7 — External Client & Workflow Management

**Scope.** `Client` (connect, no tasks/migrations) + management ops (cancel/resume/fork/delete/
set-delay/GC/list/steps/app-versions), usable both standalone and embedded.

**Deliverables.**
- `client.rs`: `Client::connect`, `enqueue<I,O>`, `send`, `get_event`, `retrieve_workflow`,
  `list_workflows`/`list_queued_workflows` (default load false), `list_workflow_steps`,
  `cancel_workflow(s)`, `resume_workflow(s)`, `fork_workflow`, `delete_workflows`,
  `set_workflow_delay`, `garbage_collect`, app-version ops.
- Bulk ops as data-modifying CTEs with `= ANY($1)`; singular vs plural existence semantics; state
  guards; fork copy (steps + events-history + latest events + streams `< start_step`,
  `was_forked_from`); GC (never PENDING/ENQUEUED/DELAYED, more-restrictive bound).

**Reference tests to port.**
- `client_test.go`: `TestClientEnqueue` (all opts incl configured-instance routing, timeout,
  priority, dedup, partition), `TestCancelResume`, `TestDeleteWorkflow`, `TestForkWorkflow`
  (fork at every step + events-history verify + `WasForkedFrom` filter), `TestListWorkflows`,
  `TestGetWorkflowSteps`, `TestClientEnqueueDelay`, `TestClientApplicationVersions`.
- `pgsql_client_test.go`: client `Send` (with/without topic, idempotent), CTE bulk ops.
- `workflows_test.go`: `TestCancelWorkflows`, `TestResumeWorkflows`, `TestGarbageCollect`,
  `TestCancelAllBefore`.

**Acceptance.** Enqueue by name routes to the right registered instance via config_name; client
returns polling handles whose results resolve once a server executes them; cancel/resume field resets
match the reference exactly; singular missing → `NonExistentWorkflow`, plural skips; fork at each step
reproduces the final result with correct counter math and copies events history; GC preserves
in-flight and applies the more-restrictive cutoff; `config_name` None/Some("")/Some(x) preserved.

---

## M8 — Serialization conformance & cross-language interop

**Scope.** Exhaustive serialization round-trips through **every** persistence path; custom
serializers; portable interop byte-compat with Python/TS fixtures.

**Deliverables.**
- `testAllSerializationPaths` equivalent: each input type through workflow input/output, step output,
  send/recv, set/get_event, stream, and recovery.
- Pluggable serializer registration (custom + portable per-operation options).
- Portable interop fixtures cross-checked against Python/TS byte output.

**Reference tests to port.**
- `serialization_test.go`: full `TestSerializer` matrix, `TestClientCustomSerializer`,
  `TestPortableInterop`, `TestPortablePerOperationOptions`, `TestDirectRunPortableWorkflow`,
  `TestPortableWorkflowError`.

**Acceptance.** Every type round-trips through all paths including recovery; custom serializer name
recorded and used per-row; portable bytes match the reference envelope for inputs/errors; named args
rejected for native/custom; recovery decodes stored inputs of all formats.

---

## M9 — Streams

**Scope.** WriteStream/CloseStream/ReadStream (+async/snapshot), workflow-inactive termination,
streams LISTEN/NOTIFY (migration 39 trigger) + polling, fork stream-copy.

**Deliverables.**
- `db/notifications.rs` streams: write (offset = MAX+1, PK-race retry, closed guard), close (sentinel),
  `read_stream` collecting + `read_stream_async` (`impl Stream`/mpsc, terminates on consumer cancel),
  snapshot mode, final-drain on workflow-inactive.
- `dbos_streams_channel` listener + 1s polling fallback (readers may be non-workflow processes).

**Reference tests to port.**
- `workflows_test.go`: `TestStreams` (read/write/close/snapshot/async/recovery/fork/leak).
- `client_test.go`: `TestClientReadStream` (+AsyncGoroutineLeak).

**Acceptance.** Append-only 0-based offsets; write-after-close errors (`stream '<key>' is already
closed`); auto-close on workflow termination with final drain; snapshot returns without blocking;
async reader task exits within 5s of consumer cancel (no leak); recovery replays writes once each;
fork copies stream history `< start_step`.

---

## M10 — Admin HTTP server

**Scope.** axum admin server (off by default) with the full endpoint set and the exact console wire
shapes.

**Deliverables.** `admin/` routes (§12), graceful shutdown, deactivate (one-shot CAS, stops
scheduler only), `to_list_workflow_response` (PascalCase keys, epoch-ms **strings**, priority always
emitted, Input/Output pass-through), steps endpoint (integer epoch-ms).

**Reference tests to port.** `admin_server_test.go`: `TestAdminServer` (off-by-default, health,
recovery body/400, queues-metadata incl internal queue with nil limits, list/list-queued/steps,
deactivate).

**Acceptance.** Off unless enabled; healthz/recovery/queues/list/steps/deactivate match Go response
shapes and status codes; timestamps are epoch-ms strings in list/get and integers in steps; bad
recovery JSON → 400.

---

## M11 — Telemetry, Debouncer, Conductor, Kafka (optional, parallelizable)

**Scope.** Lower-priority management/observability surfaces, each independently shippable.

**Deliverables (split into sub-milestones).**
- **M11a Telemetry**: `tracing` spans on workflow/step/queue/recovery; optional OTLP exporter.
- **M11b Debouncer**: `NewDebouncer` coalescing within a window, latest-input-wins, per-key
  independence (`debouncer.go`).
- **M11c Conductor** (feature-gated): tokio-tungstenite reconnecting client, tagged-enum protocol,
  single-writer task, ping/timeout/backoff; handlers delegating to runtime/admin ops; `StringOrList`.
- **M11d Application versions & metrics**: `get_metrics` aggregation (workflow_count/step_count),
  app-version create/list/set-latest.
- **M11e Patching**: `Patch`/`DeprecatePatch` recorded as `DBOS.patch-<name>` steps,
  `PatchingNotEnabled` guard, fork-across-patch determinism.

**Reference tests to port.** `debouncer_test.go`, `conductor_test.go`,
`conductor_protocol_test.go` (`StringOrList`), `metrics_test.go`, `application_versions_test.go`,
patching cases in `workflows_test.go`.

**Acceptance.** Each sub-surface matches its reference behavior; conductor reconnects on
disconnect/binary/close/ping-timeout and replies with correlated `request_id` + in-band
`error_message`; debouncer coalesces and the latest input wins; metrics aggregate by name; patching
records a `DBOS.patch-*` step only when active.
