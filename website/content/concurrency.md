# Concurrency & rate limits

Three independent gates shape how much work runs at once. All are enforced
inside the claim transaction — they hold under crash/restart and can't leak
permits.

## Queue concurrency

Each queue runs at most `N` steps simultaneously:

```go
q, _ := quacker.Open(quacker.WithQueue("billing", 4)) // declared at open
q.SetQueue("billing", 8)                              // or adjusted at runtime
```

`QueueStats` (via `q.Metrics`) reports `Running`, `Queued`, `Blocked`,
`Suspended`, and `RateLimited` counts per queue.

## Per-key concurrency

Serialize work by a key derived from the input — e.g. one active run per
customer — without dedicating a queue per key:

```go
sync := quacker.NewTask("acct.sync", syncAcct,
    quacker.WithKey(func(in Acct) string { return in.ID }),
    quacker.WithKeyConcurrency(1), // default is already 1
)
```

- The key is computed once at enqueue and persisted on the step.
- The claim gate counts `RUNNING` steps sharing the key — saturated keys
  stay `QUEUED` rather than crowding out other work.
- `SUSPENDED` steps release their key (a sleeping step doesn't hold it) and
  re-acquire on resume.
- Keyless steps (`WithKey` unset) are never gated.

## Rate limiting

Cap how many steps a queue may **start** within a sliding window — the gate
counts persisted claim timestamps, so bursts like "2N in adjacent windows"
can't happen:

```go
q, _ := quacker.Open(quacker.WithRate("api", 10, time.Second))
q.SetRateLimit("api", 100, time.Minute) // runtime adjustment
```

- Claims beyond `n` in the last `per` stay `QUEUED` and count toward
  `RateLimited` in stats.
- Retries count as starts (each attempt stamps `claimed_at`); parked claims
  hand the token back.
- Durable resumes stamp `claimed_at` too — a sleeper stampede can't slip
  past the window.

## Worker labels

Route work to specific engines — e.g. a GPU-bound task only runs where the
hardware exists:

```go
ml := quacker.NewTask("train", trainModel, quacker.WithLabels("gpu", "us-west"))

// somewhere with a GPU:
q, _ := quacker.Open(quacker.WithWorkerLabels("gpu", "us-west", "ssd"))
```

- A task's labels must be a **subset** of the engine's worker labels; extra
  engine labels are fine.
- No labels on the task means any engine claims it.
- With no matching engine, runs stay `QUEUED` (they don't fail — they wait).

## Ordering recap

Claim order is `priority DESC, created_at ASC` within a queue, filtered by:
due time (`run_at`), per-key availability, rate-window budget, queue
capacity, worker labels, and dependency unblock — all evaluated in one
transaction per batch.
