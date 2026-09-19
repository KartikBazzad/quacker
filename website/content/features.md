# Features

quacker is a single-process alternative to a workflow server: durable state,
retries, schedules, and a DAG engine, all inside your Go binary. Everything
below is in the box — no broker, no sidecar, no control plane.

## Tasks and execution

Tasks are plain Go functions with generic, typed inputs and outputs; the input
marshals to JSON, the output decodes back.

```go
type In  struct{ Name string }
type Out struct{ Greeting string }

greet := quacker.NewTask("greet", func(ctx context.Context, in In) (Out, error) {
    return Out{Greeting: "hi " + in.Name}, nil
}, quacker.Retries(3), quacker.Timeout(30*time.Second), quacker.Queue("email"))

h, _ := quacker.Enqueue(ctx, q, greet, In{Name: "ada"})
out, _ := h.Result(ctx)   // blocks until terminal; decodes Out
```

- **Retries with backoff** — `Retries(n)` and `BackoffPolicy(...)` (exponential
  from 500ms, capped at 30s, 10% jitter by default).
- **Per-attempt timeouts** — `Timeout(d)`; a timeout is a normal failure and
  consumes an attempt.
- **Queues and priorities** — `Queue(name)` routes to a named queue with its
  own concurrency; `Enqueue(..., WithPriority(n))` orders claims.
- **Batching** — `EnqueueBatch` inserts many runs in one transaction.
- **Async handles** — `Enqueue` returns immediately; `*RunHandle[O]` exposes
  `RunID()`, `Result(ctx)`, and `Wait(ctx)`. A handle can be held in memory and
  discarded; `Result` re-attaches to the persisted run after a restart.

## Workflows (DAGs)

Compose tasks into a directed acyclic graph with typed dependency outputs.

```go
charge := quacker.NewTask("charge", func(ctx context.Context, o Order) (Receipt, error) { … })
ship := quacker.NewTask("ship", func(ctx context.Context, o Order) (string, error) {
    rcpt, err := quacker.DepOutput[Receipt](ctx, "charge")
    if err != nil { return "", err }
    return "shipped " + rcpt.ID, nil
})

wf := quacker.NewWorkflow[Order]("fulfill",
    quacker.Step("charge", charge),
    quacker.Step("ship", ship, "charge"),   // depends on charge
)
h, _ := quacker.EnqueueWorkflow[Order](ctx, q, wf, order)
```

- **`DepOutput[T]`** reads an upstream step's result inside a downstream task.
- **Live introspection** — `q.DAG(ctx, runID)` returns the graph, `DAGJSON` a
  serializable form, and **`DAGSVG`** a rendered diagram at any point during a
  run.
- **Child runs** — `EnqueueChild`/`EnqueueWorkflowChild` start a run from
  inside another and record the lineage; `q.Children(ctx, runID)` lists them.

## Durable execution

Long waits don't hold a worker slot, and side effects happen once even when a
task is replayed.

```go
res, err := quacker.RunOnce(ctx, "reserve-"+orderID, func() (int, error) {
    return inventory.Reserve(orderID)          // runs exactly once
})
if err != nil { return err }
if err := quacker.SleepDurable(ctx, 24*time.Hour); err != nil { return err }
msg, err := quacker.WaitFor[string](ctx, "payment.settled", 30*time.Minute)
```

- **`SleepDurable`** suspends the step (status `SUSPENDED`) and resumes it when
  due — a `time.Sleep` that survives process restarts.
- **`WaitFor[T]`** blocks on an in-process event with a timeout.
- **`RunOnce`** memoizes a side effect in a per-step journal so a replayed task
  doesn't charge the card twice.
- The replay is journaled, so a task can crash mid-way and resume from the
  first unfinished await.

## Concurrency and rate limits

Limits are enforced in the claim transaction, so they hold across restarts and
across processes sharing a database.

- **Queue concurrency** — `WithQueue("email", 5)` caps a queue's in-flight
  steps.
- **Per-key serialization** — `WithKey` extracts a key from the input and
  `WithKeyConcurrency(n)` caps how many runs with that key execute at once
  (default 1). A due step whose key is saturated stays `QUEUED`.
- **Rate windows** — `WithRate("api", 100, time.Minute)` allows at most `n`
  starts per sliding window, counted over claim timestamps.
- **Worker labels** — `WithLabels("gpu")` on a task and
  `WithWorkerLabels("gpu")` on an engine route work only to capable workers.

## Job control

- **Unique jobs** — `WithUnique(func(in) string)` (or `WithUniqueKey`) makes a
  run unique per key while it is non-terminal. A collision resolves by
  `UniqueConflict`: `UniqueReuse` (default, returns the live run's handle),
  `UniqueError` (`ErrDuplicateJob`), or `UniqueReplace` (cancel the live run).
- **Snooze** — `q.Snooze(ctx, runID, until)` / `SnoozeFor` pushes a non-running
  run's start time forward; a run with a RUNNING step returns `ErrRunRunning`.
- **Pause queues** — `q.PauseQueue(ctx, name)` stops claims from a queue
  (running steps finish; new work queues) until `ResumeQueue`. Persisted, so
  every engine observes it.
- **Pause runs** — `q.PauseRun(ctx, runID)` interrupts a running step and holds
  the run; `ResumeRun` replays it. Durable tasks resume cleanly from their
  journal; non-durable steps re-run from the top.
- **Sequences** — `WithSequence(func(in) string)` runs jobs sharing a key
  strictly one-at-a-time in insertion order (parallel across keys), with
  head-of-line blocking across retries.
- **Dead-letter queue** — `WithDeadLetter()` opts a task in; exhausted failures
  appear in `q.DeadLetters`, and `RetryDeadLetter` reopens one in place.
- **Per-queue retention** — call `WithRetention` with a `Queue` to purge queues
  on different cutoffs.
- **Transactional enqueue** — `EnqueueTx(ctx, tx, q, task, in)` inserts a run
  on your own `*sql.Tx` (Postgres/MySQL), atomic with your business writes —
  the outbox pattern.
- **Encrypted payloads** — `WithPayloadKey(key)` encrypts every user payload
  at rest with AES-256-GCM; decrypt transparently at execution, or read
  ciphertext from introspection.
- **Ephemeral runs** — `WithEphemeral()` keeps a run's state only while it is
  live: deleted on completion and discarded on restart, so there is no history
  to retain.

```go
task := quacker.NewTask("sync", syncFn,
    quacker.WithUnique(func(in SyncInput) string { return in.Account }),
    quacker.WithUniqueConflict(quacker.UniqueReuse))

q.PauseQueue(ctx, "sync")           // hold the whole queue
q.SnoozeFor(ctx, runID, time.Hour)  // or push one run forward
q.PauseRun(ctx, runID)              // or pause a single run
```

## Triggers and scheduling

- **Cron** — `quacker.Cron(q, "nightly", "0 2 * * *", task, input)`; standard
  5-field specs, descriptors (`@hourly`, `@daily`, …), and `@every <dur>`
  including sub-second (`@every 250ms`). Crons persist and re-arm on restart,
  and fire once per occurrence even with several engines on one database.
- **Events** — `quacker.On(q, "user.created", task)` binds an event to a task;
  `q.Emit(ctx, "user.created", payload)` fans out one run per binding, and
  durable `WaitFor` waiters are woken in the same transaction.
- **Children** — run a task or workflow from inside another, with lineage.

## Storage and durability

| Storage | Persistence | Scope |
|---|---|---|
| `Memory()` | none | tests, single-shot runs |
| `Ephemeral()` | temp file, deleted on close | default |
| `File(path)` | survives restarts | single process, durable |
| `Postgres(dsn)` | networked | **multi-instance**, leases |
| `Driver("mysql", dsn)` | networked | **multi-instance**, leases |

- **Restart recovery** — a `File` engine re-queues (or fails, per
  `RecoverRunningOnBoot`) work left in flight by a previous process.
- **Multi-instance** — Postgres and MySQL give each engine a worker id; claimed
  steps carry a lease extended by a heartbeat, and a leaderless reaper
  re-queues a crashed node's expired work. Claim batches and per-run completion
  decisions are serialized with locks.
- **Pluggable drivers** — new SQL databases are a
  [storage driver](storage-drivers.html) you can implement out-of-tree;
  `Storage.WithDB` reuses a pool you already own.

## Observability and operations

- **Middleware** — `WithMiddleware` engine-wide, `Wrap` per task; before/after
  hooks via [plugins](plugins.html).
- **Logging** — task logs to a sink (`WithTaskLogSink`) and optionally to
  storage (`WithLogStorage(true)`, read with `q.Logs`); a live **debug stream**
  for development.
- **Metrics** — `WithMetricsFunc` receives periodic snapshots; `q.Metrics(ctx)`
  reads on demand.
- **Tracing** — `WithTracerProvider` emits an OTel span per enqueue, step, and
  emit, linked across restarts via the persisted W3C traceparent.
- **Retention** — `WithRetention` purges terminal runs on an interval;
  `q.Purge(ctx, ...)` deletes by age and status.
- **Introspection** — `q.Execution`, `q.Runs`, `q.Crons`, `q.Children` read
  state at any moment without pausing execution.

## Extensibility

- **Plugins / lifecycle hooks** — enqueue, step, run-finished, and emit
  callbacks; `Before*` may veto. Compile-time and explicit.
- **Payload codec** — `WithCodec` replaces JSON for user payloads engine-wide.
- **Storage drivers** — `driver.Backend` for new SQL dialects.
- **Middleware** — composable handlers around every task body.

## Reliability

- **At-least-once with no lost terminal states** — the claim path is one
  transaction; a completion either lands or is retried.
- **Crash safety** — File-mode SIGKILL recovery is covered by a chaos suite;
  the DAG validator and cron parser are fuzzed.
- **Same guarantees across databases** — SQLite, Postgres, and MySQL run the
  same query layer and integration tests.

See [Use cases](use-cases.html) for how teams put these together, or start with
[Getting started](getting-started.html).
