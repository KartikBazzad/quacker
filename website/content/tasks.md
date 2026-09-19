# Tasks

A task is a named, typed unit of work — a plain Go function with a JSON input
and output. Create once with `NewTask`; use the value to enqueue runs,
register for recovery, bind events, and attach crons.

```go
task := quacker.NewTask("orders.charge", func(ctx context.Context, in In) (Out, error) {
    // TaskLogger(ctx), SleepDurable, WaitFor, RunOnce, EnqueueChild,
    // StepFromContext/RunIDFromContext, DepOutput are all available here.
    return charge(in)
},
    quacker.Queue("billing"),
    quacker.Retries(3),
    quacker.BackoffPolicy(quacker.Exponential(2*time.Second)),
    quacker.Timeout(30*time.Second),
    quacker.Priority(10),
)
```

## Options

| Option | Effect |
|---|---|
| `Queue(name)` | Route the task's runs to a named queue (default `"default"`). |
| `Retries(n)` | Retry a failed attempt up to `n` times (`maxAttempts = n+1`). |
| `BackoffPolicy(b)` | Delay between attempts — `Exponential(d)`, `Constant(d)`, or a custom `Backoff`. |
| `Timeout(d)` | Per-attempt deadline; the task context is cancelled at `d`. |
| `Priority(n)` | Higher runs claim first within a queue. |
| `WithKey(fn)` / `WithKeyConcurrency(n)` | Per-key concurrency — see [Concurrency](concurrency.html). |
| `WithLabels(...)` | Require worker labels — only engines with covering labels claim the task. |
| `Wrap(mw...)` | Per-task middleware — see [Operations](operations.html). |

`Backoff`: `Exponential` doubles per attempt with 10% jitter; `Constant`
waits a fixed delay; a zero `Backoff` uses library defaults.

## Enqueue options

```go
h, _ := quacker.Enqueue(ctx, q, task, in,
    quacker.WithDelay(5*time.Minute),      // start at least d from now
    quacker.WithRunAt(tomorrow9am),        // start no earlier than t
    quacker.WithPriority(10),              // run-level priority
)
```

## The handle

```go
h.RunID()          // run id for Execution/Logs/Subscribe/DAG
h.Done()           // closed when the run reaches a terminal status
out, err := h.Result(ctx) // blocks for the terminal output, decodes O
```

`Result` returns the decoded output on `SUCCEEDED`, the run's error on
`FAILED`/`INTERRUPTED`, and `ErrNotFound`-style errors for cancelled runs
(you can also check `q.Execution` for the exact status).

## Task context

Inside a task the context carries step identity and helpers:

```go
sc, ok := quacker.StepFromContext(ctx) // RunID, Step, Task, Attempt
id := quacker.RunIDFromContext(ctx)    // just the run id
log := quacker.TaskLogger(ctx)         // *slog.Logger to the log sinks
```

`ctx` is cancelled on `q.Cancel(runID)`, on `q.Close` drain, and at the
attempt `Timeout`. Honor it for fast cancellation.

## Queues

Queues are independent schedulers — each has its own concurrency:

```go
q, _ := quacker.Open(quacker.WithQueue("billing", 4)) // 4 concurrent steps
q.SetQueue("billing", 8)                              // adjust at runtime
```

A queue with concurrency `N` runs at most `N` steps at a time; due work
beyond that stays `QUEUED`. See [Concurrency](concurrency.html) for
rate limits and per-key serialization.
