# Getting started

## Install

```sh
go get github.com/kartikbazzad/quacker
```

Requires Go with generics support. SQLite runs in-process via `modernc.org/sqlite` — no CGo, no external service.

## Open an engine

```go
q, err := quacker.Open()                        // ephemeral in-memory state
q, err := quacker.Open(quacker.WithStorage(quacker.File("state.db"))) // durable
if err != nil { log.Fatal(err) }
defer q.Close(ctx)
```

Storage modes:

| Storage | Persistence | Use for |
|---|---|---|
| `quacker.Memory()` | none (in-process) | tests, single-shot runs |
| `quacker.Ephemeral()` | temp DB, deleted on close | default; faster than File |
| `quacker.File(path)` | survives restarts | production single-process |

With `File`, work in flight during a crash is recovered on the next `Open` — re-queued by default, or marked failed via `quacker.RecoverRunningOnBoot(false)`.

## Define and run a task

Tasks are generic typed functions — the input marshals to JSON, the output decodes back:

```go
type In  struct{ Name string }
type Out struct{ Greeting string }

greet := quacker.NewTask("greet", func(ctx context.Context, in In) (Out, error) {
    return Out{Greeting: "hi " + in.Name}, nil
})

h, err := quacker.Enqueue(ctx, q, greet, In{Name: "ada"})
if err != nil { log.Fatal(err) }

out, err := h.Result(ctx)   // blocks until the run is terminal
// out.Greeting == "hi ada"
```

`Enqueue` returns a `*RunHandle[O]` immediately — runs execute asynchronously on the queue's workers. `h.Result(ctx)` waits; `h.RunID()` gives the run id for introspection.

## Many runs at once

```go
hs, err := quacker.EnqueueBatch(ctx, q, greet, []In{{"a"}, {"b"}, {"c"}})
// one transaction, one handle per input
```

## Register for restart recovery

A `File`-mode engine can claim a persisted run before your code re-registered its task. Register tasks at startup so recovered runs execute:

```go
quacker.Register(q, greet) // also happens automatically on Enqueue/Cron
```

Unregistered-but-persisted steps park briefly instead of failing.

## Close

```go
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()
q.Close(ctx) // stops claiming, drains in-flight work, checkpoints WAL
```

`Close` stops the scheduler, waits for running steps (bounded), seals the log sinks, joins the cron/metrics/retention loops, and closes the store. Runs never claimed become `INTERRUPTED` and are recovered on the next `Open`.

## Where next

- [Tasks](tasks.html) — retries, timeouts, queues, priorities, keys, labels
- [Workflows](workflows.html) — DAG steps and dependency outputs
- [Durable execution](durable.html) — `SleepDurable`, `WaitFor`, `RunOnce`
