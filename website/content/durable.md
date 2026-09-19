# Durable execution

Durable execution lets a task **suspend** — for minutes or days — without
holding a worker slot, a queue slot, or a per-key slot, then resume where it
left off. With `File` storage a suspended run survives process restarts.

Tasks stay plain Go functions. The mechanism is **journaled replay**: each
durable helper writes one journal entry; on resume the task re-executes from
the top, completed entries replay their stored results, and the first
unresolved entry suspends again.

## SleepDurable

```go
if err := quacker.SleepDurable(ctx, 2*time.Hour); err != nil {
    return "", err
}
```

- First call persists `{kind: sleep, wake_at}` and marks the step `SUSPENDED`.
- `SUSPENDED` frees the queue and per-key slots and is **not** charged an
  attempt — a 2-hour sleep inside a 3-retry task still has all retries left.
- `d <= 0` consumes a journal slot and returns immediately (alignment).
- The attempt `Timeout` applies per active segment — the suspended duration
  does not count against it.
- `q.Cancel(runID)` cancels a sleeping run.

## WaitFor

```go
payment, err := quacker.WaitFor[Payment](ctx, "payment.received", 24*time.Hour)
if err != nil {
    if errors.Is(err, quacker.ErrWaitTimeout) { /* give up */ }
    return "", err
}
```

- Suspends until `q.Emit(ctx, "payment.received", payload)` delivers the
  payload — decoded into `T` (`WaitFor[struct{}]` for signal-only waits).
- `timeout <= 0` waits indefinitely; the step has no self-wake time and only
  an `Emit` resumes it.
- One `Emit` wakes **every** matching waiter (broadcast) and still triggers
  `On`-bound tasks — in a single transaction.
- Waits are subscription-style: an event emitted *before* the wait registers
  does not count.
- Timeout vs delivery is decided atomically — a late `Emit` cannot resurrect
  a consumed wait, and a payload delivered at the deadline wins over the
  timeout.

## RunOnce

Replay re-executes the task body, so side effects before a suspension point
would otherwise repeat. `RunOnce` memoizes a result in the journal:

```go
receipt, err := quacker.RunOnce(ctx, "reserve", func() (Receipt, error) {
    return chargeCard(order) // real side effect — must not repeat
})
```

- On success the result is stored; every replay returns the stored value.
- On error nothing is memoized — the step fails/retries and `fn` runs again.
  Exactly-once on success, at-least-once on failure: keep `fn` idempotent.
- `key` must be unique within the task — it is checked on replay.
- `RunOnce` never suspends.

## The determinism contract

Replay relies on durable helper calls occurring in the **same order and
count** on every invocation. That means:

- **Do not branch around durable calls** on data that can differ between
  invocations — `if x { SleepDurable(...) }` misaligns the journal when `x`
  changes.
- **Do not `recover()` around durable helpers** (and middleware must not
  recover over `next()`) — suspension is an internal panic; swallowing it
  corrupts state. This is why suspension is a panic, not an error return.
- **Code changes while runs are suspended** can misalign the journal. The
  seatbelt: replay validates entry kind, `WaitFor` event, and `RunOnce` key —
  a mismatch fails loudly with `ErrJournalMisaligned` instead of returning
  wrong data. Same-shaped-but-different calls are the known limitation
  (Temporal-style workflow versioning would be needed to catch them).

## Status plumbing

`SUSPENDED` appears in `Execution` snapshots (`Steps[i].Status` ==
`StatusSuspended`) along with `WaitEvent` and `ResumeAt`; `QueueStats`
gains a `Suspended` bucket. Suspended steps hold their journal — retries
replay in call order and resume mid-sequence.

## Example

`examples/durable` shows the full flow: `RunOnce` a side effect,
`SleepDurable`, then `WaitFor` a payment event.

```sh
go run ./examples/durable
# while sleeping: SUSPENDED
# reserved o-1 -> shipped (paid-42)
```
