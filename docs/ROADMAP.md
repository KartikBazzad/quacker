# Roadmap

Status: **v0.2 in progress** — P0 (hardening) and P1 (concurrency control +
operations) are shipped; P2 (triggers & ergonomics) is next.

- v0.1 shipped: tasks, retries, timeouts, queues, priorities, DAG workflows,
  cron, delayed runs, cancel, graceful shutdown, File persistence + recovery,
  non-blocking introspection, subscribe, task logs, benchmarks.
- v0.2 P0 shipped: schema migrations, single-writer kernel-lock guard, WAL
  hygiene.
- v0.2 P1 shipped: per-key concurrency, rate limiting, middleware,
  retention/purge, metrics callback, log-sink ownership.

Each iteration below is independently shippable; priorities are the
recommended order. Items marked ⚖ are decision points — see the end.

---

## v0.2 — hardening & control (in progress: P0 + P1 ✅, P2 next)

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
`WithKeyConcurrency(n)` gives N-per-key (default 1); strict-order-per-key
stays v0.3. Rate limiting is a sliding window over persisted `claimed_at`
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

### P2 — triggers & ergonomics

| Item | Design sketch |
|---|---|
| Sub-second `@every` | Parse `@every <d>` ourselves and construct `cron.ConstantDelaySchedule(d)` directly — bypasses the parser's 1s floor. |
| Persistent cron arming | On `Start()`, load `crons` rows; when the app later `Register`s the target task, arm the schedule automatically. File-mode apps then only need `Register`, not re-`Cron`. |
| In-process events | `q.Emit(ctx, "order.placed", payload)`; tasks subscribe via `quacker.On("order.placed", task)` — stored in a table (needs migrations), fan-out on emit. |
| CI + license | GitHub Actions (fmt, vet, test, `-race` on mac/linux), MIT LICENSE, semver tags. |

---

## v0.3 — durable execution & visibility

- **Durable sleep**: `quacker.SleepDurable(ctx, 2*time.Hour)` checkpoints the
  step (persisted "phase" + input), returns without failing, and the engine
  resumes the continuation later — the state machine gains a
  `SUSPENDED(step_phase)` state. Requires a resumable-task protocol
  (`StepFunc` receiving a resume handle) — biggest design item on the board.
- **Event waits**: durable `WaitFor(ctx, "payment.received", timeout)` built
  on the same suspension mechanism.
- **Child runs**: enqueue from inside a task with `runs.parent_id` for
  lineage; `Execution` exposes children.
- **Embedded debug logger**: `q.DebugLogger()` returns a logger that writes to a channel, which can be consumed by the user.
  - "We dont want the quacker to serve http. so a method can return debug logs"
- **DAG Visualizer**: `q.DAGJSON()` returns a JSON representation of the DAG, which can be consumed by the user.
  - "We need this JSON to return Current State of the Entire DAG"
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

## ⚖ Decision points (input welcome, defaults chosen)

1. **v0.2 flagship**: plan assumes per-key concurrency + rate limiting are
   the headline features, with hardening first. If you'd rather ship
   durable sleep early (it's the sexiest Hatchet-parity feature but the
   biggest design lift), v0.2 and v0.3 can swap.
2. **Debug logger**: embedded debug logger is planned; it will be a method on the quacker instance that returns a logger that writes to a channel, which can be consumed by the user.
3. **Events**: in-process emit/listen is planned; if you want webhook or
   external-event ingestion, that changes the schema — flag it before the
   migrations land.
4. **License**: MIT assumed for the repo; say the word before the first tag.
