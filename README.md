# quacker 🦆

An **embeddable task & workflow orchestration engine** for Go — a single-process
alternative to [Hatchet](https://github.com/hatchet-dev/hatchet). No server, no
Postgres, no workers to deploy: state lives in SQLite (in-memory by default),
tasks are plain Go functions, and **execution state is readable at any moment
without pausing execution**.

```go
q, _ := quacker.Open() // ephemeral in-memory state

charge := quacker.NewTask("orders.charge", func(ctx context.Context, o Order) (Receipt, error) {
    return chargeCard(ctx, o)
})

h, _ := quacker.Enqueue(ctx, q, charge, Order{ID: "o-1"})
receipt, _ := h.Result(ctx)

// Any time, from anywhere in your process:
snap, _ := q.Execution(ctx, h.RunID()) // status, attempts, step DAG, timings
events, stop := q.Subscribe(h.RunID()) // live transition stream
```

## Why quacker

| | Hatchet | quacker |
|---|---|---|
| Deployment | Server + Postgres (+ Docker) | One `go get`, embedded in your binary |
| State | Postgres | SQLite (pure Go driver, no CGo) |
| Introspection | Web UI / API over HTTP | In-process snapshots & streams, zero network |
| Scope | Distributed fleet | One process, all cores |

Use quacker when your app *is* the worker: background jobs, pipelines, agent
orchestration, fan-out/fan-in — anything where "spin up a queueing platform"
is the wrong amount of infrastructure.

## Features (v0.1)

- **Typed tasks** — `NewTask[I, O]` with JSON-serialized payloads
- **Retries** — attempt counts, exponential/constant backoff with jitter
- **Timeouts** — per-attempt, via `context`
- **Queues** — named queues with independent concurrency limits and priorities
- **DAG workflows** — steps with dependencies, upstream outputs via `DepOutput`
- **Cron & delayed runs** — cron specs (`"@daily"`, `"0 9 * * 1-5"`, `"@every 1s"`)
  and `quacker.WithDelay`
- **Cancellation** — instant, propagated to running task contexts
- **Non-blocking introspection** — `Execution`, `Runs`, `Metrics`, `Logs`,
  `Subscribe` — never pause or slow the workers
- **Graceful shutdown** — drains in-flight work, marks stragglers, optional
  recovery on restart (File storage)

## Storage modes

```go
quacker.Open(quacker.WithStorage(quacker.Memory()))    // pure :memory: SQLite
quacker.Open(quacker.WithStorage(quacker.Ephemeral())) // default; temp file, WAL, deleted on Close
quacker.Open(quacker.WithStorage(quacker.File("state.db").RecoverRunningOnBoot(true)))
```

- **`Memory`** — nothing touches the filesystem. Note: SQLite `:memory:`
  databases cannot use WAL, so reads can briefly wait on an in-flight write
  (absorbed by `busy_timeout`).
- **`Ephemeral`** (default) — WAL-backed temp file with `synchronous=OFF`,
  deleted on `Close`. Same zero-durability semantics as memory, but readers
  are truly lock-free versus the write path.
- **`File`** — survives restarts. Runs interrupted by a previous process are
  re-queued on boot (or marked failed with `RecoverRunningOnBoot(false)`).
  Opening a `File` path takes an exclusive kernel advisory lock
  (`flock`/`LockFileEx`) on a `<path>.quacker.lock` sidecar file, so a
  second engine on the same path fails fast — and the lock is released
  automatically when the process dies, even on SIGKILL. On platforms
  without kernel advisory locks (`!unix && !windows`), only same-process
  exclusion holds.

WAL modes (`Ephemeral`, `File`) run a passive `wal_checkpoint` every 60s
(`WithCheckpointInterval`) and a truncating checkpoint on `Close`, so a
clean shutdown leaves the `-wal` file empty (usually deleted).

How non-blocking introspection works: the store keeps a **single writer
connection** and a **separate read-only pool**. In WAL mode readers never
block the writer, so `Execution()`/`Metrics()` can run in the middle of a
thousand concurrent task transitions without adding contention. A push-based
event bus (`Subscribe`) covers hot-path observation without touching SQLite
at all; snapshots remain the authoritative view.

## Workflows (DAGs)

```go
ship := quacker.NewTask("ship", func(ctx context.Context, o Order) (Shipment, error) {
    rec, err := quacker.DepOutput[Receipt](ctx, "charge") // upstream output
    ...
})

wf := quacker.NewWorkflow[Order]("fulfill",
    quacker.Step("charge", charge),
    quacker.Step("ship", ship, "charge"),           // runs after charge
    quacker.Step("notify", notify, "charge", "ship"),
)

h, _ := quacker.EnqueueWorkflow[Shipment](ctx, q, wf, order) // Result = last step's output
```

Steps start the moment their dependencies succeed. If a step exhausts its
retries, the run fails and remaining steps are cancelled. Task logs written
inside steps are persisted and readable via `q.Logs(ctx, runID, limit)`.

## Cancellation & shutdown

```go
q.Cancel(h.RunID())        // queued steps → CANCELLED, running contexts cancelled
// Result returns quacker.ErrRunCancelled

q.Close(ctx)               // stop claiming, drain in-flight (30s default deadline),
// Result returns quacker.ErrRunInterrupted if the deadline expires
```

## Observability

```go
snap, _ := q.Execution(ctx, h.RunID())          // full snapshot (JSON-tagged)
runs, _ := q.Runs(ctx, quacker.RunFilter{Status: quacker.StatusRunning})
m, _ := q.Metrics(ctx)                          // counts by status, queue depths
logs, _ := q.Logs(ctx, h.RunID(), 100)          // TaskLogger output
events, stop := q.Subscribe("")                 // "" = all runs
```

All snapshot types marshal to JSON, so exposing them over HTTP is trivial.

## Task logs inside tasks

```go
task := quacker.NewTask("import", func(ctx context.Context, in Input) (Output, error) {
    log := quacker.TaskLogger(ctx)
    log.Info("starting batch", "size", len(in.Rows))
    ...
})
```

Lines are batched (~50ms) into the store and readable via `Logs`. On
overflow they are dropped rather than slowing your task; a warning is logged
at shutdown.

## Notes & semantics

- **Task names are part of the storage format.** Renaming a task orphans its
  in-flight runs.
- **Re-registering a task name replaces its function.** `Enqueue`,
  `Register`, and `Cron` all overwrite the registered def for that name;
  in-flight runs execute the currently registered def at claim time.
- **Crons are in-memory.** Re-register them at startup
  (`quacker.Cron(...)`); with File storage, register tasks with
  `quacker.Register` before work resumed from a previous process can execute.
  Unregistered tasks are retried every 5s until registered.
- **`@every` intervals round up to 1 second** (cron parser limitation).
- Tasks should honor `ctx` — timeouts and cancellation are cooperative.
- One engine per process assumes a single writer to a given `File` path —
  enforced by the `.quacker.lock` kernel-lock guard: a second engine on
  the same path fails fast at `Open` instead of corrupting assumptions.

## Not in v0.1

Distributed workers across processes, rate limiting, per-key concurrency
strategies, durable pause/resume (durable sleep), worker labels/affinity, a
web UI, OpenTelemetry.

## Documentation

- [Architecture](docs/ARCHITECTURE.md) — how the engine, store, and event bus fit together
- [Design notes](docs/DESIGN_NOTES.md) — key decisions and lessons from the build
- [Benchmarks](docs/BENCHMARKS.md) — numbers, methodology, how to reproduce
- [Roadmap](docs/ROADMAP.md) — v0.2 hardening & control, v0.3 durable execution, known issues

## Examples

 runnable programs live in [`examples/`](examples/):
[`simple`](examples/simple/main.go) · [`dag`](examples/dag/main.go) ·
[`cron`](examples/cron/main.go) · [`introspect`](examples/introspect/main.go)

## Development

```sh
go test ./...          # unit + integration suite
go test -race ./...    # includes an introspection-under-load stress test
go vet ./... && gofmt -l .
```
