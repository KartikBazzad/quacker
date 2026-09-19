# Roadmap

Status: **v1.0–v1.4 shipped** — durable execution, DAG visualizer, debug
logger, worker labels, OTel tracing, Postgres with multi-instance leases, the
perf pass, lifecycle-hook plugins, a pluggable payload codec, and a public
storage-driver contract with a MySQL/MariaDB driver. The v1.0 stability gates
(semver policy, chaos, fuzzing, pkg.go.dev examples) are complete; release tags
wait on a git remote.

- v0.1 shipped: tasks, retries, timeouts, queues, priorities, DAG workflows,
  cron, delayed runs, cancel, graceful shutdown, File persistence + recovery,
  non-blocking introspection, subscribe, task logs, benchmarks.
- v0.2 P0 shipped: schema migrations, single-writer kernel-lock guard, WAL
  hygiene.
- v0.2 P1 shipped: per-key concurrency, rate limiting, middleware,
  retention/purge, metrics callback, log-sink ownership.
- v0.2 P2 shipped: sub-second `@every`, persistent cron/event arming,
  in-process events, CI + MIT license.

Each iteration below is independently shippable; priorities are the
recommended order. Items marked ⚖ are decision points — see the end.

---

## v0.2 — hardening & control (✅ DONE: P0 + P1 + P2)

Goal: make v0.1 safe to run long-lived and multi-instance, and close the
biggest feature gaps vs Hatchet for single-process users.

### P0 — correctness patch (✅ DONE — shipped as part of v0.1 pre-release)

The pre-release review's fixes landed, each with a regression test in
`fixes_test.go`: engine-scoped log sink (multi-instance log routing),
`StepFromContext`/`RunIDFromContext` dead-accessor fix, bounded waiters,
park-without-attempt-consumption, queue-concurrency race, caller-ctx
enqueues, cancel/claim race halt-tombstones, sibling-context cancellation on
run failure, step-resurrection guards, real default jitter, sealed log
channel (no post-Close panic), and the enqueue-during-Close TOCTOU guard.
See DESIGN_NOTES.md for the full list.

Acceptance: a test that runs two engines concurrently and asserts each run's
logs appear only in its own instance's store.

### P0 — storage foundation (✅ DONE)

| Item | Design sketch |
|---|---|
| ✅ Schema migrations | `schema_migrations(version INT PRIMARY KEY, applied_at)` + ordered migration list; `CREATE TABLE IF NOT EXISTS` stops being the whole story. **Prerequisite for every later schema change** (keyed concurrency, parent runs, retention columns). |
| ✅ Single-writer guard for `File` mode | Exclusive non-blocking kernel advisory lock (`flock`/`LockFileEx`) on a permanent `<path>.quacker.lock` sidecar, held for the store's lifetime — a second engine on the same path fails fast with a clear error instead of corrupting assumptions, and the lock releases automatically on process death. |
| ✅ WAL hygiene | Periodic `PRAGMA wal_checkpoint(PASSIVE)` on a timer plus a `TRUNCATE` checkpoint on close; cap WAL growth for long-running File deployments. |

As built: migrations apply per-version in one transaction (v0.1 file DBs
baseline at 1); the guard is a kernel advisory lock on a permanent
sidecar (no stale locks, no reclamation TOCTOU — the kernel releases it
on process death, even SIGKILL) plus an in-process registry for
same-process exclusion; checkpoints are `PASSIVE` every
`WithCheckpointInterval` (default 60s, clamped to ≥10ms) plus a
bounded-timeout `TRUNCATE` on File-mode close whose failure is reported
to the caller.

### P1 — concurrency control (flagship v0.2 features) (✅ DONE)

| Item | Design sketch |
|---|---|
| ✅ Per-key concurrency | `quacker.WithKey(func(in I) string)` on a task; key persisted on `runs`/`steps` (new column + index, needs migrations). Engine keeps per-key semaphores in memory; scheduler claims a step only when its key has a free slot. Strategies: `N concurrent per key` (v0.2) and `strict order per key` (serialize; v0.3). |
| ✅ Rate limiting | Token bucket per queue (and optionally per key): scheduler delays claims when the bucket is empty instead of claiming. Config: `quacker.WithRate("emails", 50, time.Minute)`. |

As built (deviating from the sketch where the sketch was weaker):
per-key gating counts persisted `RUNNING` rows sharing the step's
`concurrency_key` at claim time — no in-memory semaphores to leak on
park/cancel/interrupt, and File-mode restarts inherit the right count.
`WithKeyConcurrency(n)` gives N-per-key (default 1); strict (ordered)
per-key execution remains future work. Rate limiting is a sliding window over persisted `claimed_at`
timestamps inside the claim transaction — `WithRate(queue, n, per)` /
`SetRateLimit` — which beats a token bucket on the acceptance test (no 2N
boundary bursts) and survives restarts. Keys are visible in `Execution`
(run + step level); schema migration v2 adds `concurrency_key`,
`key_limit`, `claimed_at` + indexes.

Acceptance: tests asserting max-concurrent-per-key under load; rate-limited
queue never exceeds N starts per window; both observable in `Execution`
(key visible in snapshots).

### P1 — operations (✅ DONE)

| Item | Design sketch |
|---|---|
| ✅ Middleware hooks | Engine-wide `WithMiddleware` / `q.Use` plus per-task `Wrap`, wrapping task execution: timing, custom metrics, error taxonomies, per-task tracing. |
| ✅ Retention / purge | `q.Purge(PurgeOptions{OlderThan, Statuses, KeepLogs, BatchSize})` deletes terminal runs, steps, and (by default) logs in batches, plus `WithRetention` for a scheduled policy. |
| ✅ Metrics callback | `WithMetricsFunc(func(*Metrics))` invoked on `WithMetricsInterval` — one-liner for Prometheus users until OTel lands. |
| ✅ Log ownership | Task logs default to a sink (engine slog) instead of SQLite; `WithLogStorage(true)` restores built-in persistence + `q.Logs`. |

As built (deviating from the sketch where it was weaker):
middleware composes `global → per-task → body` inside the existing
panic-recover, so a middleware panic fails the step like a task panic, and
`StepContext` gained `Task` (the registered function) so tracing can tell a
named workflow step from its task. Retention is *storage-agnostic*, not
File-only: a long-lived `Memory`/`Ephemeral` daemon grows RAM or its temp WAL
until Close, so `q.Purge` bounds growth in every mode. Purge only ever
touches terminal runs, never one whose step is still `RUNNING` (closing the
cancel-window race where a run turns terminal before its step records
`CANCELLED`), and deletes steps/runs explicitly rather than trusting the
FK cascade. The field is `KeepLogs` (zero value = delete, the safe default)
rather than the sketch's `IncludeLogs`; an orphan-log sweep cleans up logs
left by an earlier `KeepLogs` purge. Schema migration v3 adds
`idx_runs_purge (status, completed_at)`. Logs became a sink chain: engine
slog by default, custom via `WithTaskLogSink`/`WithLogSink`, and opt-in
SQLite persistence, so the engine stops growing log data it never reads.

Acceptance: middleware ordering/panic/retry tests; purge cutoff, batching,
`RUNNING`-guard, keep-logs + orphan-sweep, and auto-retention tests; metrics
interval, panic-recovery, and stop-on-close tests. All pass under `-race`.

### P2 — triggers & ergonomics (✅ DONE)

| Item | Design sketch |
|---|---|
| ✅ Sub-second `@every` | `@every <d>` is parsed by the engine itself and scheduled with a tiny fixed-delay `Schedule`, bypassing the parser's 1s floor. |
| ✅ Persistent cron arming | `Start()` loads `crons` rows into a pending set; each arms automatically when the app `Register`s its target task. File-mode apps need only `Register`, not re-`Cron`. |
| ✅ In-process events | `q.Emit(ctx, "order.placed", payload)` persists the event and fans out to tasks bound via `quacker.On("order.placed", task)` (schema v4). |
| ✅ CI + license | GitHub Actions (gofmt, vet, test, `-race` on ubuntu + macos via `go-version-file`), MIT LICENSE. |

As built (deviating from the sketch where it was weaker):
the sketch said to construct `cron.ConstantDelaySchedule(d)` for sub-second
intervals, but its `Next` subtracts `t.Nanosecond()` and computes wrong (even
backwards) next times below one second — the engine defines its own
fixed-delay schedule instead, and `cronLoop` now sleeps until the earliest
armed cron (capped at 200ms) rather than ticking on a fixed interval, so
sub-second schedules fire accurately without a busy loop. Persistent arming
uses resume-cadence semantics: a stored next time still in the future is
kept, a missed one is recomputed from now (skip missed, no catch-up burst).
Event delivery is **best-effort and in-process**: the event is persisted for
audit (`q.Events`) and then one run is enqueued per binding, each receiving
the payload as input; durable at-least-once event waits were deliberately left
to v0.3 (shipped in slice B). Migration v4 adds `events` and `event_subscriptions`; event bindings
re-arm on `Register` exactly like crons. To keep the new events table from
becoming an unbounded-growth hole, `q.Purge`/`WithRetention` now age events
out with the same cutoff (`PurgeResult.Events`).

Acceptance: sub-second cron fires repeatedly; a cron persisted by one process
fires after a reopen with only `Register`; emit dispatches to every bound
task with the payload, unbound emits still persist, `Off` unbinds; persisted
bindings re-arm; retention purges events. All pass under `-race`.
Semver tags remain pending a git remote (none is configured).

---

## v0.3 — durable execution & visibility

### Slice A — substrate + durable sleep (✅ DONE)

As built (deviating from the sketch's "persisted phase + resume handle"):
durable execution is **journaled replay**, not a phase handle, so tasks stay
plain Go functions. Migration v5 adds a `SUSPENDED` step state (`resume_at`,
`wait_kind`, `wait_event`) and a `step_journal` table. Each durable call
appends a journal entry; on resume the task re-runs from the top and replays
entries by index. Suspension unwinds by an internal panic, caught by the
executor's recover boundary — a task cannot swallow it by ignoring an error.
`ClaimDue` gained a second arm (`SUSPENDED AND resume_at<=now`); a resume
stamps `claimed_at` (start budget) but not `attempts`, and the per-attempt
timeout covers active execution only. `SleepDurable(ctx, d)` releases the
queue and per-key slots; `RunOnce(key, fn)` memoizes side effects across
replays (exactly-once on success, re-runs on failure). A replay-time seatbelt
(`ErrJournalMisaligned`) fails loudly when replayed calls do not match the
journal's kind/key/event instead of resuming with wrong data.

- ✅ Substrate: `SUSPENDED`, journal, resume claim arm, cancellation/status
  plumbing, per-segment timeout.
- ✅ `SleepDurable`.
- ✅ `RunOnce` (required for replay-safe side effects).

### Slice B — durable event waits (✅ DONE)

`WaitFor[T](ctx, "payment.received", timeout)` appends a wait entry and
suspends (resume_at = deadline, or 0 for event-only). `Emit` runs
`DeliverEvent`: one transaction inserts the event and flips every undone
matching wait to done with the payload, then sets those steps QUEUED. Timeout
writes `timed_out=1, done=1` conditionally and returns `ErrWaitTimeout`;
delivery only touches `done=0` entries, so a late emit cannot resurrect a
consumed wait. Waits are subscription-style — an event emitted before the
wait registers does not count. As built, the suspension transition and the
journal insert are one transaction (`SuspendWithJournal`), closing the race
where an emit between "append entry" and "set SUSPENDED" would be lost.

`q.Emit` now does both: durable `WaitFor` wakeups (atomic) and `On`-binding
fan-out; it returns the number of binding runs enqueued.

### Slice C — child runs (✅ DONE)

Migration v6 adds `runs.parent_id` + an index. `EnqueueChild` /
`EnqueueWorkflowChild` enqueue a child of the run currently executing
(`RunIDFromContext`), and `Execution.Children`, `RunSummary.ParentID`, and
`RunFilter.ParentID` expose the lineage. As built: lineage only — no implicit
join, a failed child does not fail the parent, and there is no foreign key,
so a purged parent leaves its children with a dangling id (documented).
Children are ordinary runs and are aged/purged independently.

### Slice D — visibility & perf (✅ DONE)

- ✅ **DAG Visualizer** (`q.DAG`/`q.DAGJSON`/`q.DAGSVG`): a run's step graph
  with the **current state** of every node, as a structured model, indented
  JSON, or a standalone status-coloured SVG (dependency-level layout,
  dependency-free renderer). See `examples/dagsvg`.
- ✅ **Embedded debug logger**: `q.DebugLogs()` streams engine activity
  (claims, retries, suspensions, completions) plus anything logged via
  `q.DebugLogger()`, bounded and drop-on-full; the engine never serves HTTP.
- ✅ **Perf**: `quacker.EnqueueBatch` inserts many runs in one transaction;
  `ClaimDueMulti` claims across every queue in a single transaction per tick
  (was one per queue); parallel `-cpu` benchmarks added. Measured roughly
  ~58µs/run batched vs per-call enqueue, and the single-writer contention
  curve is visible with `-cpu 1,4,10` (see BENCHMARKS.md).

---

## v0.4 — routing & observability (✅ DONE)

- ✅ **Worker labels**: `quacker.WithLabels(...)` on a task and
  `WithWorkerLabels(...)` on an engine; the scheduler claims a step only when
  its labels are a subset of the engine's. Migration v8 adds `steps.labels`,
  and `ClaimDueMulti` adds a `json_each` subset gate to the claim SELECT.
  Routes work to capable workers in-process and is the building block for
  cross-process routing.
- ✅ **OTel tracing**: `WithTracerProvider` emits `quacker.enqueue`,
  `quacker.step`, and `quacker.emit` spans. The step span is a new root
  **linked** to the enqueue span; the W3C traceparent is persisted on the run
  (migration v9) so the link survives a restart. API-only dependency — the
  caller supplies the SDK/exporter; tracing off is allocation-free.
- ✅ **Postgres / multi-instance**: store dialect seam + `postgres/` driver,
  worker identity and step leases (heartbeat + reaper), claim/run locks, and
  cron single-fire — shipped under v1.1 below.

---

## v1.0 — stability (✅ DONE — release tags pending a remote)

- ✅ **API freeze + semver discipline** documented in
  [STABILITY.md](STABILITY.md): the public surface, storage-format
  compatibility, and the extension points. Release tags can't be published
  until a git remote exists.
- ✅ **Chaos suite for File mode** (`chaos_test.go`): a child process enqueues a
  workload, is SIGKILLed mid-execution, and the parent reopens the database and
  asserts every run recovers to SUCCEEDED (at-least-once, no lost terminal
  states). Best-effort random kill points per iteration rather than an
  exhaustive hook at every write site — a deliberate simplification.
- ✅ **Fuzzing** for the DAG validator and the cron parser
  (`internal/engine/fuzz_test.go`), including the `@every` sub-second path.
- ✅ **pkg.go.dev examples** in `example_test.go` (basic task, workflow +
  `DepOutput`, durable sleep/`RunOnce`, plugin), plus the docs site.

---

## v1.1 - Database Support (✅ DONE)

- ✅ **PostgreSQL backend**: a dialect seam in `internal/store` (backend
  registry, `?`→`$n` rebind, per-dialect migrations and DDL) plus a `postgres/`
  driver package registered by blank import, so pgx is only built when used.
  Postgres uses `BIGINT` throughout (unix nanos overflow `INTEGER`), identity
  columns, and a `jsonb` label gate; migrations are serialized with an
  advisory lock. Env-gated integration tests run in CI against a Postgres
  service.
- ✅ Migration scripts for PostgreSQL (a per-dialect migration list).
- ✅ **Multi-instance (leases)**: `worker_id` + `lease_expires_at` on steps; a
  heartbeat extends leases and a leaderless reaper re-queues expired ones
  (replacing boot-only recovery, which is disabled for Postgres). `ClaimDueMulti`
  takes a `pg_advisory_xact_lock` so the counting gates serialize cluster-wide;
  `CompleteStep`/`FinalFailStep`/`CancelRun` take `SELECT … FOR UPDATE` on the
  run; and crons fire once per occurrence via a `next_at` CAS fused with the
  enqueue. A two-engine Postgres test shares one database in CI.
- ✅ **Reuse existing connection**: `PostgresWithDB(*sql.DB)` hands quacker a
  pool it does not own. It runs migrations on the pool but never closes it, so
  the caller's pool outlives `Close`. SQLite rejects it (its single-writer +
  reader pool layout is not a caller-owned shape).

## v1.2 - Performance Optimizations (✅ DONE)

- ✅ **DAG completion without full step scans.** `CompleteStep` now reads only
  the direct dependents of the step that finished and their dependencies'
  statuses (indexed by `(run_id, status)` and `(run_id, name)`), and probes
  for remaining work with a `LIMIT 1` existence check — no full-row load and no
  input/output blobs on the hot path. A chain of 800 steps dropped from ~2.57s
  to ~0.24s per run (~10×; BENCHMARKS.md).
- ✅ **Wide-DAG benchmark** (`BenchmarkWideDAGComplete`).
- ✅ **Horizontal scaling** via the Postgres multi-instance work (v1.1);
  optional cross-node `LISTEN/NOTIFY` wakeups remain a follow-up.
- ✅ `PostgresWithDB(*sql.DB)` connection reuse (the last v1.1 item).


## v1.3 - Advanced Features (✅ DONE)

Compile-time plugins, not dynamic `plugin` loading or out-of-process servers —
the same interface + explicit registration model the Postgres driver uses, so
it stays type-safe, cross-platform, and single-binary.

- ✅ **Lifecycle hooks.** `Open(WithPlugin(p))`; a plugin returns `Hooks` (a
  struct of optional callbacks) for enqueue, step, run-finished, and emit.
  `Before*` callbacks run in registration order and may veto (a `BeforeStep`
  veto fails the step immediately, without retries); `After*` callbacks run in
  reverse, are observe-only, and recover panics. Plugins are explicit
  instances (no global registry) and must be concurrency-safe.
- ✅ **Codec.** `WithCodec` replaces `encoding/json` for user payloads (task
  input/output, `DepOutput`, event payloads, and durable `RunOnce`/`WaitFor`
  values); schema structures (`depends_on`, `labels`) stay JSON because the SQL
  gates read them. Engine-wide, default unchanged, same codec across restarts.
- ⛔ **Custom storage backends — not planned *in v1.3*.** We chose not to ship
  a backend-agnostic `Store` interface (~39 operations whose transactional
  correctness would move onto every third-party driver). **Superseded by v1.4**,
  which publishes the SQL-dialect seam instead; see DESIGN_NOTES §34.

Design input from Jev (semantic judgments, not tests): After* ordering
(reverse, 0.91), After* error handling (recover+log, 0.99), and surface shape
(a single `Hooks` struct over capability interfaces, 0.72). Trust model:
in-process plugins are trusted code; no sandboxing.


## v1.4 - Storage drivers (✅ DONE)

Publish the SQL-dialect seam and prove it with a second third-party-style
driver. The engine keeps every transactional guarantee; a driver only
translates SQL. See [DRIVERS.md](DRIVERS.md).

- ✅ **Public driver contract** (`quacker/driver`): `Backend`, `Config`,
  `Migration`, a name registry (`RegisterBackend`), and `quacker.Driver(name,
  dsn)`. An out-of-tree package can implement it.
- ✅ **MySQL/MariaDB driver** (`mysql/`), multi-instance, tested against a real
  MySQL 8 server in CI: tasks, workflows, durable wait/resume, two engines on
  one database, label routing, purge, and `WithDB`.
- ✅ **Reuse an existing pool**: `Storage.WithDB(*sql.DB)` and
  `PostgresWithDB` (connection reuse).
- ✅ **Portability fixes** the MySQL driver forced into the shared query layer:
  `wkey` rename, `driver.KeyGate`, `driver.UpsertSQL`, `driver.MigrateLocker`,
  select-then-update instead of `UPDATE ... RETURNING`, and derived-table
  `LIMIT` subqueries.

## Backlog
- Pause and Resume Jobs/workflows
- Custom storage backends

## ⚖ Open decisions (input welcome, defaults chosen)

1. **Debug logger**: planned as `q.DebugLogger()`, a method returning a
   channel-backed logger the user consumes; the engine stays HTTP-free.
2. **External events**: in-process emit/listen shipped (v0.2 P2). Webhook or
   external-event ingestion would change the schema — flag it before it lands.
3. **Multi-instance**: shipped — dialect seam, worker leases (heartbeat +
   reaper), claim/run locks, and cron single-fire. Optional follow-ups:
   `LISTEN/NOTIFY` wakeups remain (`WithDB` connection reuse shipped).
4. **License/tags**: MIT is in place (v0.2 P2); semver tags are pending a git
   remote.
