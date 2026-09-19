# Use cases

quacker fits when you want durable background execution **inside a Go
program** — no broker to run, no service to deploy, no YAML to publish. If your
work is "do this later, retry it, schedule it, or run these steps in order",
quacker keeps that state in the same database your app already uses.

Below are the shapes teams reach for most. Each one works with the built-in
SQLite storage, and scales out on Postgres or MySQL without code changes.

## Background jobs in an existing service

Replace ad-hoc `go func()` calls and in-memory queues with work that survives a
restart and reports its own status.

```go
q, _ := quacker.Open(quacker.WithStorage(quacker.File("jobs.db")))
defer q.Close(ctx)

send := quacker.NewTask("send-email", sendEmail,
    quacker.Retries(5), quacker.Timeout(10*time.Second), quacker.Queue("email"))
quacker.WithQueue("email", 4)   // at most 4 in flight
```

Why it fits: retries, timeouts, and concurrency are declarative; a deploy no
longer drops in-flight work, because a `File` engine re-queues it on the next
`Open`.

## User-triggered async work

An HTTP handler enqueues a run and returns immediately; a status endpoint reads
the run's state. The work outlives the request and the process.

```go
func handleExport(w http.ResponseWriter, r *http.Request) {
    h, _ := quacker.Enqueue(ctx, q, exportTask, ExportInput{
        UserID: userID, Format: "csv",
    })
    json.NewEncoder(w).Encode(map[string]string{"run": h.RunID()})
}

func handleStatus(w http.ResponseWriter, r *http.Request) {
    snap, _ := q.Execution(ctx, chi.URLParam(r, "run"))
    json.NewEncoder(w).Encode(snap)   // status, steps, timings
}
```

Why it fits: `Enqueue` is transactionally durable before you reply, so a crash
between the response and the work loses nothing; `Execution` gives you a
progress model without building one.

## Scheduled and recurring jobs

Drop in-process timers and separate cron containers. Trigger specs are
persisted with the runs.

```go
quacker.Cron(q, "nightly-rollup", "0 2 * * *", rollup, RollupInput{})
quacker.Cron(q, "pulse", "@every 250ms", ping, PingInput{})   // sub-second
```

Why it fits: with several engines on one database, each occurrence fires once —
no distributed lock or "leader" election to write.

## ETL and fan-out pipelines

Fan out a batch, then reduce once every shard is done, using a DAG.

```go
parts := shardKeys(keys, 64)
hs, _ := quacker.EnqueueBatch(ctx, q, extractShard, parts)   // one transaction
// ... or express the whole pipeline as a workflow with Step deps + DepOutput.
```

Why it fits: `EnqueueBatch` is a single insert, per-key limits keep you inside a
downstream API's budget, and `DAGSVG` renders where a run is stuck.

## Durable side effects and sagas

Charge once, wait a day, then continue — without holding a worker or
double-charging on a retry.

```go
charge, _ := quacker.RunOnce(ctx, "charge-"+order, func() (Charge, error) {
    return payments.Capture(order)
})
if err := quacker.SleepDurable(ctx, 24*time.Hour); err != nil { return err }
settled, _ := quacker.WaitFor[Settlement](ctx, "payment.settled", 30*time.Minute)
```

Why it fits: `RunOnce` memoizes the effect in the step journal, so a replayed
task is safe; `SleepDurable` and `WaitFor` suspend without consuming a slot or
a retry.

## Per-tenant fairness and rate limits

Keep one noisy tenant (or one upstream) from starving everything else.

```go
sync := quacker.NewTask("sync-org", syncOrg,
    quacker.WithKey(func(in SyncInput) string { return in.OrgID }),
    quacker.WithKeyConcurrency(1),           // one sync per org at a time
    quacker.Queue("sync"))

// engine-wide sliding-window cap on a queue:
quacker.WithRate("sync", 200, time.Minute)
```

Why it fits: the gate counts RUNNING rows at claim time, so limits survive
restarts and are correct across processes — no in-memory semaphore to drift.

## Multi-instance workers

Run the same binary on many nodes; each engine claims its share and recovers
crashed peers.

```go
import _ "github.com/kartikbazzad/quacker/postgres"

q, _ := quacker.Open(
    quacker.WithStorage(quacker.Postgres(dsn)),
    quacker.WithWorkerID(hostname),
    quacker.WithLeaseTTL(30*time.Second),
)
```

Why it fits: leases + heartbeat + a leaderless reaper mean a killed node's work
is picked up automatically; `WithWorkerLabels` routes specialized work (e.g.
`gpu`) to the nodes that can do it.

## Workflow engine embedded in a product

Ship orchestration as a feature, not a dependency: tests use `Memory()`,
development uses `Ephemeral()`, production uses `File()` or Postgres.

```go
func newEngine(env string) *quacker.Quacker {
    switch env {
    case "test": return must(quacker.Open(quacker.WithStorage(quacker.Memory())))
    case "dev":  return must(quacker.Open(quacker.WithStorage(quacker.Ephemeral())))
    default:     return must(quacker.Open(quacker.WithStorage(quacker.File("state.db"))))
    }
}
```

Why it fits: one process, one writer connection, zero infrastructure — the
same code runs everywhere, and `Memory`/`Ephemeral` make tests fast and
self-cleaning.

## Where quacker is a good fit — and where it isn't

| Good fit | Not a fit |
|---|---|
| Work that must be durable, retried, or scheduled | Sub-millisecond request fan-out you'd just do inline |
| Orchestration embedded in a Go binary | Polyglot fleets needing a language-agnostic server |
| Single-process now, multi-instance later | Petabyte-scale dataflow / stream processing |
| Per-key and per-queue fairness | Cross-region active-active with strict global ordering |
| Teams that don't want to operate a broker | Workflows whose steps must run in other runtimes |

If you're unsure, start with [Getting started](getting-started.html), then read
[Features](features.html) for the full surface.
