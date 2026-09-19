# Roadmap

Status: **v0.3 in progress** — v0.2 is fully shipped; durable execution is
complete (slices A/B/C: substrate + durable sleep, durable event waits, child
runs), the DAG visualizer, and the embedded debug logger. Remaining v0.3:
perf. Then worker labels, OTel, and the Postgres/multi-instance epic.

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

### Slice D — visibility & perf (in progress)

- ✅ **DAG Visualizer** (`q.DAG`/`q.DAGJSON`/`q.DAGSVG`): a run's step graph
  with the **current state** of every node, as a structured model, indented
  JSON, or a standalone status-coloured SVG (dependency-level layout,
  dependency-free renderer). See `examples/dagsvg`.
- ✅ **Embedded debug logger**: `q.DebugLogs()` streams engine activity
  (claims, retries, suspensions, completions) plus anything logged via
  `q.DebugLogger()`, bounded and drop-on-full; the engine never serves HTTP.
- **Perf**: batch enqueue, multi-queue claim batching in one transaction,
  `-cpu` parallel benchmarks.

---

## v1.0 — stability

- API freeze + semver discipline.
- Chaos suite for File mode: injected process kills at every write site,
  verifying recovery re-queues exactly-once-ish (at-least-once, no lost
  terminal states).
- Fuzzing for the DAG validator and cron parsing.
- Docs site / pkg.go.dev examples baked into `example_test.go`.

---

## v1.1 - Database Support
- Support for PostgreSQL
- Migration scripts for PostgreSQL
- Reuse existing connection

## v1.2 - Performance Optimizations
- Optimize for high throughput and low latency
- Add support for horizontal scaling


## v1.3 - Advanced Features
- Plugin system for extending functionality
- Support for custom storage backends

## ⚖ Open decisions (input welcome, defaults chosen)

1. **Debug logger**: planned as `q.DebugLogger()`, a method returning a
   channel-backed logger the user consumes; the engine stays HTTP-free.
2. **External events**: in-process emit/listen shipped (v0.2 P2). Webhook or
   external-event ingestion would change the schema — flag it before it lands.
3. **Multi-instance**: the Postgres epic needs a store interface plus worker
   identity and step leases, because boot recovery currently re-queues every
   RUNNING row — correct for one process, unsafe for a cluster.
4. **License/tags**: MIT is in place (v0.2 P2); semver tags are pending a git
   remote.
