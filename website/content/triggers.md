# Triggers: cron, events, children

Three ways to start work beyond direct `Enqueue`: schedules, in-process
events, and fan-out from inside a task.

## Cron

```go
err := quacker.Cron(q, "nightly", "0 2 * * *", rollupTask, Input{})
err = quacker.Cron(q, "pulse", "@every 250ms", pingTask, Input{}) // sub-second
```

- Standard 5-field specs plus descriptors (`@hourly`, `@daily`, …) and
  `@every <dur>` — including durations below one second.
- Crons **persist** (File storage): after a restart they re-arm as soon as
  the task is registered — the app only needs `quacker.Register`, not
  re-`Cron`.
- Missed fire times use resume cadence: kept if still in the future,
  recomputed from now if missed — no catch-up burst.
- `q.Crons()` lists armed crons; `q.RemoveCron(name)` deletes one;
  re-`Cron` on an existing name replaces it.

## Events

```go
quacker.On(q, "order.placed", fulfillTask)  // bind a task to an event
n, err := q.Emit(ctx, "order.placed", order) // persist + fan out
quacker.Off(q, "order.placed", fulfillTask)  // unbind
```

- `Emit` persists the event (audit via `q.Events`) and enqueues one run per
  binding, passing the payload as the run's input.
- Delivery is **best-effort and in-process** — bindings live on this engine.
- Bindings persist and re-arm on `Register`, like crons.
- For durable, at-least-once waiting use `WaitFor` inside a task — see
  [Durable execution](durable.html). One `Emit` does both: wakes `WaitFor`
  waiters and fans out to `On` bindings.

## Child runs

```go
parent := quacker.NewTask("import", func(ctx context.Context, b Batch) (Summary, error) {
    for _, row := range b.Rows {
        if _, err := quacker.EnqueueChild(ctx, q, processRow, row); err != nil {
            return Summary{}, err
        }
    }
    return Summary{Queued: len(b.Rows)}, nil
})

snap, _ := q.Execution(ctx, parentRunID) // snap.Children lists them
```

- `EnqueueChild` / `EnqueueWorkflowChild` set `runs.parent_id` from the
  executing run (`RunIDFromContext`) — they error outside a task.
- Children are **ordinary runs**: independent retries, timeouts, queues,
  suspension, and retention.
- **No implicit join** — the parent can complete while children run, and a
  failed child does not fail the parent.
- `RunFilter{ParentID: ...}` lists a run's children via `q.Runs`.
- Lineage is informational: purging a parent leaves children (with a
  dangling `ParentID`) intact — no cascade.
- Each child records the step that spawned it (`ParentStep`), so
  `q.DAGTree` / `DAGTreeJSON` / `DAGTreeSVG` draw the parent plus its child
  runs, with each child attached to its spawning step and the parent run and
  each step's fan-out boxed and labelled as groups (also in the tree JSON). `DAG`/`DAGSVG`
  show only one run's own steps.

> **Replay caveat:** in a durable task, `EnqueueChild` before a
> `SleepDurable`/`WaitFor` re-enqueues a duplicate child on every replay.
> Wrap it: `quacker.RunOnce(ctx, "fanout", func() (string, error) { ... })`.

## Which trigger when

| Need | Use |
|---|---|
| Run on a schedule | `Cron` |
| React to something in-process | `Emit` + `On` |
| A task that waits for an event | `WaitFor` (durable) |
| Fan out a batch from inside a run | `EnqueueChild` |
