# Architecture

How quacker works inside one process. For user-facing docs see the
[README](../README.md); for the reasoning behind these choices see
[DESIGN_NOTES.md](DESIGN_NOTES.md).

## Component map

```
┌─────────────────────────────────────────────────────────────────────┐
│ package quacker (public API)                                        │
│   Open/Close · Task[I,O] · Workflow[I] · RunHandle[O]               │
│   Enqueue/EnqueueWorkflow/Cron · Execution/Runs/Metrics/Logs        │
│   Subscribe · Cancel                                                │
│   Use/Wrap (middleware) · Purge/WithRetention · WithMetricsFunc     │
│   WithTaskLogSink/WithLogSink · WithLogStorage · DAGJSON/DAGSVG     │
└──────────────┬─────────────────────────────┬────────────────────────┘
               │ writes + queries            │ JSON adapter
┌──────────────▼──────────────┐   ┌──────────▼───────────────────────┐
│ internal/engine             │   │ internal/store (SQLite)          │
│  scheduler loop (50ms tick  │──▶│  1 writer connection             │
│  + wake channel)            │   │  4 read-only connections         │
│  per-queue worker pools     │   │  keeper pool (memory mode)       │
│  retries · DAG              │   │  runs / steps / crons / logs     │
│  cron + events triggers     │   │  events / event_subscriptions    │
│  middleware chain           │   │  schema_migrations               │
│  cancel · graceful close    │   └──────────────────────────────────┘
│  metrics + retention loops  │
│  log sink chain / flusher   │
└──────────────┬──────────────┘
        ┌──────▼──────┐
        │ internal/bus│  push events for Subscribe
        └─────────────┘
```

Everything is in-process. There is no server, no network, no serialization
boundary except JSON payloads stored in SQLite.

## Storage internals

Three modes (`internal/store.Config`):

| Mode | DSN | Journal | Durability |
|---|---|---|---|
| `Memory` | `file:quacker-<pid>-<n>?mode=memory&cache=shared` | MEMORY (WAL unavailable to `:memory:`) | none |
| `Ephemeral` (default) | temp file | WAL, `synchronous=OFF` | deleted on Close |
| `File` | user path | WAL, `synchronous=NORMAL` | durable |
| `Postgres` | user DSN (pgx) | MVCC | durable, networked |

Connection topology:

- **Writer**: one connection (`MaxOpenConns(1)`), `BEGIN IMMEDIATE`
  transactions (`_txlock=immediate` on file DSNs). SQLite allows one writer;
  giving it a dedicated connection removes all writer-vs-writer `SQLITE_BUSY`
  handling from our code.
- **Reader pool**: up to 4 connections opened with `query_only(1)`. All
  introspection (`Execution`, `Runs`, `Metrics`, `Logs`, `KnownQueues`) reads
  here. Under WAL, readers never block the writer and vice versa — this is
  the mechanism behind "inspect state without interrupting execution".
- **Keeper pool** (memory mode only): an unused single connection whose only
  job is to keep the shared-cache in-memory database alive. Named
  `:memory:` databases exist only while at least one connection is open.
  The keeper is its own pool so it can never starve the writer (borrowing
  from the 1-slot writer pool deadlocks — see DESIGN_NOTES).

Pragmas on every connection: `busy_timeout=10000`, `foreign_keys=1`.

File mode additionally serializes *engines*, not just writers: `Open`
takes an exclusive non-blocking kernel advisory lock (`flock`/`LockFileEx`)
on a permanent `<path>.quacker.lock` sidecar before any connection touches
the file, and holds it for the store's lifetime. The kernel releases the
lock on process death — no stale locks, no reclamation. A sidecar (not the
DB file) is locked because flock-on-NFS degrades to whole-file `fcntl`
locks that would collide with SQLite's own byte-range locks; the sidecar's
pid/host/time content is diagnostic only. An in-process registry provides
same-process exclusion on every platform. `Close` unlocks and closes it.

WAL modes run a goroutine issuing `PRAGMA wal_checkpoint(PASSIVE)` every
`Config.CheckpointInterval` (default 60s) — never blocking, capping WAL
growth whenever readers are momentarily idle — plus a best-effort
`wal_checkpoint(TRUNCATE)` at `Close`.

### Backends

The query layer is dialect-neutral (it writes `?` placeholders); a registered
`Backend` supplies the connection setup, placeholder rebinding, DDL, and the
dialect-only operations. SQLite is built in (`sqliteBackend`); Postgres lives
in the public `postgres/` package and registers itself from `init`, so
importing it (`_ "…/postgres"`) is what pulls in pgx — builds that stay on
SQLite never compile it.

- `Rebind` rewrites `?` to `$n` for Postgres (skipping quoted literals and
  comments), memoized per query.
- Each backend has its own `Migrations()` list; version numbers are
  per-dialect (a database is never both). Postgres DDL uses `BIGINT` for every
  timestamp/counter (unix nanos overflow `INTEGER`), identity columns, and a
  `jsonb` label gate; `MigrateLock` takes a `pg_advisory_xact_lock` so
  concurrent boots serialize DDL.
- The backend also supplies the multi-instance hooks: `ClaimLock` (advisory
  lock on a claim batch), `RunLock` (`FOR UPDATE` on a run), and
  `SupportsLeases` (false for SQLite). SQLite implements them as no-ops.
- The engine no longer touches the raw pools: the shutdown sweep is
  `Store.InterruptAll`, and `Store.Write`/`Read` are used only by tests.

### Multi-instance (Postgres)

Several engines may share one Postgres database. SQLite keeps its
single-process behavior; leases are enabled only where `Backend.SupportsLeases`
is true.

- **Leases.** A claim stamps `worker_id` and `lease_expires_at`. A heartbeat
  loop (every `LeaseTTL/3`) extends this worker's RUNNING steps; a reaper loop
  re-queues (`QUEUED`, lease cleared) any RUNNING step whose lease expired,
  leaderlessly — the `status='RUNNING'` guard means one reaper wins each row.
  Postgres boot recovery is disabled (`RecoverOnBoot` false): re-queuing every
  RUNNING row would steal a peer's work, so the reaper replaces it.
- **Claim serialization.** `ClaimDueMulti` takes a `pg_advisory_xact_lock`, so
  the per-key and rate gates are evaluated one batch at a time across the
  cluster, exactly as SQLite's single writer does.
- **Per-run decisions.** `CompleteStep`, `FinalFailStep`, and `CancelRun`
  take `SELECT … FOR UPDATE` on the run, so two nodes finishing sibling steps
  of one run can't both compute the terminal transition.
- **Cron single-fire.** Firing CAS-es `next_at` and inserts the run in one
  transaction, so exactly one node enqueues each occurrence (at-most-once: a
  crash rolls both back and retries).
- **Shutdown.** `InterruptAll(workerID, …)` sweeps only this worker's RUNNING
  steps and converges the runs it leaves with no active step; a run another
  worker is executing is left alone.

### Schema

```
runs(id, workflow, kind, status, queue, priority, input, output, error,
     attempts, max_attempts, run_at, created_at, started_at, completed_at,
     concurrency_key, parent_id, trace_parent)
steps(id, run_id→runs, name, task, ord, status, depends_on, queue, priority,
      input, output, error, attempts, max_attempts, timeout_ns,
      run_at, created_at, started_at, completed_at,
      concurrency_key, key_limit, claimed_at,
      resume_at, wait_kind, wait_event, labels)
step_journal(step_id→steps, idx, kind, key, event, wake_at, deadline,
             payload, result, err, done, timed_out, PK(step_id, idx))
crons(id, name UNIQUE, spec, task, input, next_at, created_at)
events(seq AUTOINCREMENT, name, payload, created_at)
event_subscriptions(id PK, event, task, created_at, UNIQUE(event, task))
logs(seq AUTOINCREMENT, run_id, step, at, level, message)
schema_migrations(version PRIMARY KEY, applied_at)
```

Schema changes ship as an ordered migration list; `migrate` applies each
pending entry in one transaction together with its `schema_migrations`
version row. Migration 1 is all `CREATE TABLE IF NOT EXISTS`, so a v0.1
File database (tables present, no version row) baselines at version 1.
Migration 3 adds `idx_runs_purge (status, completed_at)` for retention;
migration 4 adds `events` (best-effort emit audit) and
`event_subscriptions` (event→task bindings, unique per pair); migration 5
adds the durable-execution columns on `steps` and the `step_journal` table
(entries cascade with their step); migration 6 adds `runs.parent_id` +
`idx_runs_parent` for child-run lineage (no foreign key — a purged parent may
leave a dangling id); migration 7 rewrites `steps.depends_on` from
comma-joined text to a JSON array; migration 8 adds `steps.labels` (JSON
array) for worker-label routing; migration 9 adds `runs.trace_parent` (the
W3C traceparent captured at enqueue). Postgres has a single initial migration
with the same columns in dialect types (BIGINT throughout, identity `seq`,
BYTEA, BOOLEAN journal flags, JSON kept as TEXT).

The `logs` table still exists, but is only written when
`WithLogStorage(true)` is set: by default task logs go to a sink (engine slog
unless `WithTaskLogSink`/`WithLogSink` overrides it) and `q.Logs` returns
empty.

Every task execution is a **step** row; a single-task run has exactly one.
`steps.name` is the DAG identity, `steps.task` is the registered function to
run (they differ for named workflow steps). `depends_on` is a JSON array of
step names (`["charge","ship"]`), so a name containing any delimiter is safe;
migration 7 converted it from the earlier comma-joined form. Timestamps are
unix nanoseconds (`0` = unset).

Concurrency control columns (migration 2): `concurrency_key` is computed
from the task's `WithKey` extractor at enqueue — steps sharing a key are
capped at `key_limit` simultaneous `RUNNING` rows across all queues, checked
as a correlated count inside the claim gate (see "Claim" below).
`claimed_at` is stamped on *every* claim so a queue's `WithRate` sliding
window can be counted from durable state; `started_at` keeps first-start
semantics.

## Lifecycle of a run

```
QUEUED ──claim──▶ RUNNING ──success──▶ SUCCEEDED
   ▲                │ │  ▲
   │  retry         │ │  │ resume (resume_at <= now)
   └────────────────┘ │  │
                      │  └── SUSPENDED ──(durable sleep/wait, no slot held)
                      │ final failure (attempts == max_attempts)
                      ▼
                    FAILED
BLOCKED ──all deps SUCCEEDED──▶ QUEUED      (workflow steps)
any active state ──Cancel()──▶ CANCELLED    (running ctx cancelled)
RUNNING/QUEUED ──Close() drain timeout──▶ INTERRUPTED
```

1. **Enqueue** validates the DAG (unique names, deps exist, acyclic via
   Kahn's), inserts the run + steps in one write transaction, registers a
   `Waiter`, and signals the scheduler's wake channel. `EnqueueBatch` does the
   same for many runs in one transaction — all-or-nothing, one wake.
2. **Claim**: the scheduler loop (50ms tick + wake) computes each queue's
   free slots (`concurrency - inFlight`), then calls `ClaimDueMulti`, which
   claims across **all** queues in a single immediate transaction (one tx per
   tick, not one per queue). Per queue it claims due steps — either
   `status='QUEUED' AND run_at <= now` or `status='SUSPENDED' AND
   resume_at > 0 AND resume_at <= now` (a durable resume), ordered
   `priority DESC, run_at ASC` — with `UPDATE steps ... WHERE id=? AND
   status=...` and a rows-affected check, plus `QUEUED→RUNNING` on the parent
   run for fresh claims. A resume stamps `claimed_at` but not `attempts`. Two
   extra gates apply inside the same transaction — a
   **per-key gate** (`RUNNING` count sharing the step's `concurrency_key`
   must be below `key_limit`, re-checked per UPDATE so a batch of same-key
   candidates can't over-claim), a **rate gate** (when the queue has a rate
   limit, the batch is capped at the sliding window's remaining budget,
   counted over `claimed_at`), and a **label gate** (a step is claimable only
   when its `labels` are a subset of the engine's worker labels, checked with
   a `json_each` subquery). Saturated steps simply stay QUEUED. Claims
   are issued only by the single scheduler goroutine, so there is no
   cross-goroutine double-claim to defend against; the guarded UPDATE is
   belt-and-braces.
3. **Execute**: one goroutine per claimed step, `context` with the step's
   timeout, panics recovered with a stack trace. Dependency outputs are
   loaded before the task runs and exposed via `DepOutput`. Task logs go
   through a `slog` handler onto a buffered channel.
4. **Record outcome** (all transitions in one write transaction):
   - success → `CompleteStep`: step SUCCEEDED, then unblock the **direct
     dependents** of that step and check — with a `LIMIT 1` existence probe,
     not a full scan — whether any step remains active; if not, the run goes
     terminal with the output of the last-ordered step. Indexes
     `(run_id, status)` and `(run_id, name)` keep this off the full-step path
     (DESIGN_NOTES §31).
   - retryable failure → step back to `QUEUED` with `run_at = now + backoff`.
   - exhausted failure → `FinalFailStep`: step FAILED, siblings CANCELLED,
     run FAILED (one transaction, guarded against overwriting a run that is
     already terminal).
5. **Completion** releases the waiter (`Result`), and publishes transition
   events to the bus.

### Cancellation

`Cancel(runID)` runs one transaction: run → CANCELLED, QUEUED/BLOCKED steps →
CANCELLED, and collects still-RUNNING step IDs; then it cancels their
contexts and finishes waiters. The executing goroutine observes its context
and records its own step CANCELLED (asynchronously — a snapshot taken
mid-cancel can truthfully show `CANCELLED` run with `RUNNING` step). Halt
tombstones cover the claim→registration window: a Cancel/failure landing
before the executor registers its cancel func leaves a tombstone in
`haltSteps`, which the executor checks (and clears) atomically at
registration so the step never starts the task's side effects.

### Shutdown

`Close(ctx)` stops the loops, waits for in-flight executions until ctx's
deadline (default 30s), sweeps any still-RUNNING rows to INTERRUPTED (in
leases mode, only this worker's — see "Multi-instance"), closes the log
channel after a final flush, and releases waiters with `ErrRunInterrupted`. On `File` storage, the next `Open` re-queues
RUNNING/INTERRUPTED work (`RecoverRunningOnBoot`) and `Start()` re-creates
the queue registry from `SELECT DISTINCT queue FROM steps`. Claims whose
task isn't registered in the new process (work resumed before `Register`
runs) are parked via `ParkStep` and requeued every ~5s without consuming
retry attempts.

## Event bus & logs

- `internal/bus` keeps a subscriber map; `Publish` is non-blocking (slow
  subscribers drop events; snapshots are authoritative). Cancelling a
  subscription closes its channel.
- Task logs flow: `TaskLogger(ctx)` → handler → the engine's sink chain. The
  base sink is the engine logger, unless `WithTaskLogSink` supplies one or
  middleware installs a per-step sink via `WithLogSink(ctx, fn)`. When
  `WithLogStorage(true)` is set, the SQLite sink is appended: a 4096-buffer
  channel (drop-on-full with a shutdown warning) → batched inserts (64 rows /
  50ms) into `logs`.
- Middleware (global `Use`/`WithMiddleware` + per-task `Wrap`) composes
  `global → per-task → body` inside the executor's panic-recover, per attempt.
- The engine logger is fanned out to a bounded debug stream (`q.DebugLogs`,
  distinct from task logs): the engine's own records plus anything via
  `q.DebugLogger()`, drop-on-full and closed at `Close`, so the engine can be
  observed without serving HTTP.
- When a `TracerProvider` is supplied, the engine emits OTel spans for
  enqueue, step execution, and emit. The step span is a new root *linked* to
  the enqueue span; the enqueue span's W3C traceparent is stored on the run so
  the link survives a restart. The library imports only the OTel API and skips
  all span work when tracing is off.
- `loopWG` also owns two optional loops: the metrics push
  (`WithMetricsFunc`/`WithMetricsInterval`) and the retention purge
  (`WithRetention`), both joined before the store closes.

## Triggers (cron & events)

Cron and event bindings are persisted and share one arming model. `Start`
loads `crons` and `event_subscriptions` into **pending** maps; nothing is
armed until `RegisterTask` sees the target task, at which point it moves the
matching entries into the live maps (crons resume cadence: keep a future
`next`, recompute a missed one). `Enqueue` routes through `RegisterTask`, so
supplying a def arms its triggers too.

- **Cron**: `cronLoop` sleeps until the earliest armed `next` (capped 200ms,
  floored 1ms) rather than a fixed tick. `@every <d>` uses an engine-local
  fixed-delay schedule because `cron.ConstantDelaySchedule.Next` is wrong
  below one second.
- **Events**: `Emit` writes an `events` row, then enqueues one run per armed
  binding for the name, passing the payload as input. Delivery is
  best-effort in-process; `q.Events` reads the audit rows and
  `WithRetention`/`q.Purge` ages them out with the same cutoff.

## Durable execution

`SleepDurable`/`RunOnce`/`WaitFor` implement journaled replay.
`step_journal` holds one row per durable call in call order. On each
invocation the executor loads the journal into the step context; a cursor
replays entries by index, validating `kind` (and key/event) as it goes. A
satisfied entry returns; an unsatisfied one is recorded and the task unwinds
via an internal `suspendSignal` panic, which the executor recognizes and
publishes as `SUSPENDED`. The helper records the suspension itself
(`SuspendWithJournal`) atomically with the journal entry, so a visible wait
entry always means a suspended step — this closes an emit-vs-suspend race.
`resume_at` is a sleep's wake or a wait's timeout (0 = event-only). Because
`SUSPENDED` is not `RUNNING`, the step holds no queue or per-key slot, and the
DB-count key gate frees it automatically.

`Emit` calls `DeliverEvent`, one transaction that inserts the event and flips
every undone matching wait to done with the payload before setting those steps
QUEUED; timeouts claim their entry with a conditional
`UPDATE ... WHERE done=0`, so an emit that commits first wins and a
post-timeout emit is inert.

`RunOnce` never suspends: it appends an undone entry, runs `fn`, and on
success memoizes the result. On `fn` error it leaves the entry undone so a
retry re-invokes `fn`. Replay correctness depends on the durable-call
sequence matching the journal; `ErrJournalMisaligned` makes a mismatch a hard
step failure. The contract — code before an await re-executes, and don't
`recover()` over a helper (tasks or middleware) — is documented in
DESIGN_NOTES §19–20.

## Child runs

`EnqueueChild` reads the executing run from the step context and sets the new
run's `parent_id`; `Execution` loads a run's children by that column. There is
no foreign key and no implicit join — children are ordinary runs with
independent retries, timeouts, suspension, and retention, and a failed child
does not fail the parent. See DESIGN_NOTES §22.

## Plugins

Compile-time extension via `WithPlugin(p)`: a plugin returns a `Hooks` struct
of optional callbacks, and the engine invokes them at four points — enqueue
(`EnqueueBatch`), step (`execute`, around the middleware chain), run-finished
(`finishRun`), and emit (`Emit`). `Before*` run in registration order and may
veto (a `BeforeStep` error fails the step immediately, no retries); `After*` run
in reverse and are observe-only, with panics recovered and logged. There is no
global registry and no dynamic loading; plugins are trusted in-process code and
must be concurrency-safe. See DESIGN_NOTES §32.

## Testing strategy

- Behavioral unit/integration tests in the root package (success, retries,
  panics, timeouts, concurrency caps, priority order, DAG ordering and
  failure, cancel, delayed runs, cron, logs, filters, middleware
  ordering/panic/retry, log-sink routing, metrics push, purge/retention,
  sub-second and persistent cron, event emit/On/Off/arming, durable sleep and
  RunOnce replay, journal-misalignment failure, cancel-a-sleeper,
  restart-mid-sleep, durable WaitFor delivery/broadcast/timeout/restart and
  subscription semantics, child-run lineage/independence, DAG JSON/SVG and
  XML-escaping, debug-log capture/close, recovery of suspended steps,
  batch enqueue, multi-queue claim, worker-label routing, OTel spans including
  the cross-restart link, and plugin hooks (ordering, veto, panic recovery,
  per-attempt, concurrency).
- Purge edge cases (terminal-only, `Before<=0`, `RUNNING`-step guard,
  keep-logs + orphan sweep, batching), the migration-v3 index, the
  migration-v4 event tables, and the migration-v5 journal/resume-claim path
  live in `internal/store`.
- Postgres integration tests live in `postgres/` and skip unless
  `QUACKER_TEST_POSTGRES_DSN` is set; CI runs them on ubuntu against a
  `postgres` service container (the SQLite suite runs on ubuntu and macos).
- CI runs gofmt/vet/test/`-race` on ubuntu and macos
  (`.github/workflows/ci.yml`); the repo is MIT-licensed.
- `TestIntrospectionUnderLoad`: 8 readers hammer snapshots while 200 runs
  execute — the non-blocking guarantee, run under `-race`.
- File-mode recovery tests: drain-timeout close → requeue (or fail) →
  complete on reopen; queued work surviving restarts.
- Benchmarks: enqueue, end-to-end throughput, snapshot-under-load (see
  [BENCHMARKS.md](BENCHMARKS.md)).
