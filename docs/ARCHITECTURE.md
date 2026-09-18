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
│   WithTaskLogSink/WithLogSink · WithLogStorage                      │
└──────────────┬─────────────────────────────┬────────────────────────┘
               │ writes + queries            │ JSON adapter
┌──────────────▼──────────────┐   ┌──────────▼───────────────────────┐
│ internal/engine             │   │ internal/store (SQLite)          │
│  scheduler loop (50ms tick  │──▶│  1 writer connection             │
│  + wake channel)            │   │  4 read-only connections         │
│  per-queue worker pools     │   │  keeper pool (memory mode)       │
│  retries · DAG · cron       │   │  runs / steps / crons / logs     │
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

### Schema

```
runs(id, workflow, kind, status, queue, priority, input, output, error,
     attempts, max_attempts, run_at, created_at, started_at, completed_at,
     concurrency_key)
steps(id, run_id→runs, name, task, ord, status, depends_on, queue, priority,
      input, output, error, attempts, max_attempts, timeout_ns,
      run_at, created_at, started_at, completed_at,
      concurrency_key, key_limit, claimed_at)
crons(id, name UNIQUE, spec, task, input, next_at, created_at)
logs(seq AUTOINCREMENT, run_id, step, at, level, message)
schema_migrations(version PRIMARY KEY, applied_at)
```

Schema changes ship as an ordered migration list; `migrate` applies each
pending entry in one transaction together with its `schema_migrations`
version row. Migration 1 is all `CREATE TABLE IF NOT EXISTS`, so a v0.1
File database (tables present, no version row) baselines at version 1.
Migration 3 adds `idx_runs_purge (status, completed_at)` for retention.

The `logs` table still exists, but is only written when
`WithLogStorage(true)` is set: by default task logs go to a sink (engine slog
unless `WithTaskLogSink`/`WithLogSink` overrides it) and `q.Logs` returns
empty.

Every task execution is a **step** row; a single-task run has exactly one.
`steps.name` is the DAG identity, `steps.task` is the registered function to
run (they differ for named workflow steps). `depends_on` is a comma-separated
list of step names. Timestamps are unix nanoseconds (`0` = unset).

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
   ▲                │ │
   │  retry         │ │ final failure (attempts == max_attempts)
   └────────────────┘ ▼
                     FAILED
BLOCKED ──all deps SUCCEEDED──▶ QUEUED      (workflow steps)
any active state ──Cancel()──▶ CANCELLED    (running ctx cancelled)
RUNNING/QUEUED ──Close() drain timeout──▶ INTERRUPTED
```

1. **Enqueue** validates the DAG (unique names, deps exist, acyclic via
   Kahn's), inserts the run + steps in one write transaction, registers a
   `Waiter`, and signals the scheduler's wake channel.
2. **Claim**: the scheduler loop (50ms tick + wake) checks each queue's free
   slots (`concurrency - inFlight`), then claims due steps
   (`status='QUEUED' AND run_at <= now`, ordered `priority DESC, run_at ASC`)
   in one immediate transaction: `UPDATE steps ... WHERE id=? AND
   status='QUEUED'` with a rows-affected check, plus `QUEUED→RUNNING` on the
   parent run. Two extra gates apply inside the same transaction — a
   **per-key gate** (`RUNNING` count sharing the step's `concurrency_key`
   must be below `key_limit`, re-checked per UPDATE so a batch of same-key
   candidates can't over-claim) and a **rate gate** (when the queue has a
   rate limit, the batch is capped at the sliding window's remaining budget,
   counted over `claimed_at`). Saturated steps simply stay QUEUED. Claims
   are issued only by the single scheduler goroutine, so there is no
   cross-goroutine double-claim to defend against; the guarded UPDATE is
   belt-and-braces.
3. **Execute**: one goroutine per claimed step, `context` with the step's
   timeout, panics recovered with a stack trace. Dependency outputs are
   loaded before the task runs and exposed via `DepOutput`. Task logs go
   through a `slog` handler onto a buffered channel.
4. **Record outcome** (all transitions in one write transaction):
   - success → `CompleteStep`: step SUCCEEDED, then unblock newly ready
     dependents, then — computed from the full step set inside the same
     transaction — if no step remains QUEUED/RUNNING/BLOCKED, the run goes
     terminal with the output of the last-ordered step.
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
deadline (default 30s), sweeps any still-RUNNING rows to INTERRUPTED, closes
the log channel after a final flush, and releases waiters with
`ErrRunInterrupted`. On `File` storage, the next `Open` re-queues
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
- `loopWG` also owns two optional loops: the metrics push
  (`WithMetricsFunc`/`WithMetricsInterval`) and the retention purge
  (`WithRetention`), both joined before the store closes.

## Testing strategy

- Behavioral unit/integration tests in the root package (success, retries,
  panics, timeouts, concurrency caps, priority order, DAG ordering and
  failure, cancel, delayed runs, cron, logs, filters, middleware
  ordering/panic/retry, log-sink routing, metrics push, purge/retention).
- Purge edge cases (terminal-only, `Before<=0`, `RUNNING`-step guard,
  keep-logs + orphan sweep, batching) and the migration-v3 index live in
  `internal/store`.
- `TestIntrospectionUnderLoad`: 8 readers hammer snapshots while 200 runs
  execute — the non-blocking guarantee, run under `-race`.
- File-mode recovery tests: drain-timeout close → requeue (or fail) →
  complete on reopen; queued work surviving restarts.
- Benchmarks: enqueue, end-to-end throughput, snapshot-under-load (see
  [BENCHMARKS.md](BENCHMARKS.md)).
