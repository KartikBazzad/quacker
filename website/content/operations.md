# Operations

The operational surface: middleware, logs, retention, metrics, tracing, and
introspection — everything needed to run quacker inside a long-lived
process.

## Middleware

Wrap every task attempt with cross-cutting behavior — auth checks, timing,
error translation, context enrichment:

```go
func timing(next quacker.Handler) quacker.Handler {
    return func(ctx context.Context) (any, error) {
        start := time.Now()
        out, err := next(ctx)
        sc, _ := quacker.StepFromContext(ctx)
        log.Printf("%s attempt %d took %s", sc.Task, sc.Attempt, time.Since(start))
        return out, err
    }
}

q, _ := quacker.Open(quacker.WithMiddleware(timing)) // global
q.Use(logging)                                       // runtime, copy-on-write
task := quacker.NewTask("t", fn, quacker.Wrap(auth)) // per-task
```

Order is **global → per-task → task body**, inside the executor's panic
boundary, applied per attempt. A middleware that returns without calling
`next` short-circuits the attempt.

> Middleware must not `recover()` over `next()` — it would swallow the
> durable suspension panic. See the
> [determinism contract](durable.html#the-determinism-contract).

## Task logs

`TaskLogger(ctx)` returns a `*slog.Logger` that fans out to a **sink
chain**:

```go
q, _ := quacker.Open(
    quacker.WithTaskLogSink(func(e quacker.LogEntry) { ship(e) }), // custom sink
    quacker.WithLogStorage(true),                                   // + SQLite (q.Logs)
)

quacker.WithLogSink(ctx, fn) // per-step redirect — usable inside middleware
```

- Default: engine's `slog` logger. `WithLogStorage(true)` re-adds the
  SQLite sink so `q.Logs(ctx, runID, limit)` works — off by default so the
  engine doesn't grow data you never read.
- `LogEntry`: `RunID`, `Step`, `At`, `Level`, `Message`.

## Retention & purge

Terminal runs are deleted explicitly (steps, then runs, then orphaned
logs):

```go
res, err := q.Purge(ctx, quacker.PurgeOptions{
    OlderThan: 7 * 24 * time.Hour,
    Statuses:  []string{quacker.StatusSucceeded}, // default: all terminal
    KeepLogs:  false,                            // keep them if true
    BatchSize: 500,
})
// res.Runs / res.Steps / res.Logs / res.Events counts deleted

// or automatically on an interval:
q, _ := quacker.Open(quacker.WithRetention(quacker.RetentionPolicy{
    OlderThan: 24 * time.Hour, Interval: time.Hour,
}))
```

- Terminal-only: a run with active steps is skipped (`ErrNonTerminalPurge`
  if nothing is purgeable); `OlderThan` must be positive.
- A `NOT EXISTS (RUNNING)` guard inside the purge transaction closes the
  cancel-window race.
- Events age out with the same cutoff (`res.Events`); children are purged
  independently of parents.

## Metrics

```go
q, _ := quacker.Open(quacker.WithMetricsFunc(func(m *quacker.Metrics) {
    publish(m.Queues["default"].Running)
}), quacker.WithMetricsInterval(5*time.Second))
```

`q.Metrics(ctx)` snapshots on demand; the callback fires on an interval
(default 15s, min 100ms), reads via the read pool, recovers panics, and
stops on `Close`. `Metrics` carries per-queue `QueueStats` plus run counts
by status.

## OpenTelemetry

```go
q, _ := quacker.Open(quacker.WithTracerProvider(tp)) // API only — you supply the SDK
```

Spans cover enqueue, step execution, and emit, carrying run/step/task/
attempt/queue attributes; the traceparent persists across suspensions so a
resumed step continues the same trace.

## Introspection

```go
snap, _ := q.Execution(ctx, runID)   // run + steps + children
runs, _ := q.Runs(ctx, quacker.RunFilter{Status: "FAILED", ParentID: pid})
evs, _ := q.Subscribe(runID)         // live transition events
recs := q.DebugLogs()                // engine debug stream (claims, wakes…)
dbg := q.DebugLogger()               // join your lines to that stream
dag, _ := q.DAG(ctx, runID)          // step graph with live statuses
```

`Subscribe` returns a channel of `Event` (status transitions) — non-blocking;
`DebugLogs` is a bounded, drop-on-lag stream for observing the engine
without serving HTTP.

## Shutdown

```go
q.Close(ctx) // stop claiming → drain in-flight → join loops → checkpoint → close
```

- Steps honouring `ctx` see cancellation; unclaimed work becomes
  `INTERRUPTED` and is recovered next `Open`.
- Suspended work is untouched — it resumes where it left off.
- Everything is joined before the store closes: scheduler, cron, metrics,
  retention, and the log flusher.
