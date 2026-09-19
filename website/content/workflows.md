# Workflows & DAGs

A workflow is a DAG of named steps over one shared input type. Steps run as
their dependencies finish — the engine persists outputs, orders execution,
and reports the run's final output from the last declared step.

```go
type Order struct{ ID string }

ship := quacker.NewWorkflow[Order]("orders.ship",
    quacker.Step("reserve", reserveTask),                    // no deps: runs first
    quacker.Step("pack", packTask, "reserve"),               // after reserve
    quacker.Step("label", labelTask, "pack", "reserve"),     // fan-in
    quacker.Step("notify", notifyTask, "label"),
)
h, _ := quacker.EnqueueWorkflow[Receipt](ctx, q, ship, order)
receipt, err := h.Result(ctx)   // output of the last declared step
```

## Step dependencies

`Step(name, task, deps...)` declares a step and the names it waits on:

- A step is `BLOCKED` until every dependency is `SUCCEEDED`.
- Independent steps are claimed in parallel (bounded by the queue).
- Steps may use different tasks — the task type is free per step; only the
  workflow input `I` is shared.

## Reading dependency outputs

Every step receives the workflow input; upstream outputs come from
`DepOutput[T]`:

```go
label := quacker.NewTask("label", func(ctx context.Context, in Order) (Label, error) {
    pack, err := quacker.DepOutput[Pack](ctx, "pack")
    if err != nil { return Label{}, err }
    return printLabel(pack.Box, in.ID), nil
})
```

`DepOutput` decodes the named step's stored output; a missing or
undecodable output fails the call. Inside a workflow step,
`StepFromContext` reports both the step name and its task name
(`sc.Step` vs `sc.Task`).

## Failure semantics

- A step exhausting retries marks it `FAILED`; the run fails and remaining
  steps become `CANCELLED`.
- Steps that never ran hold their dependencies' outputs for a retry of the
  run itself.
- A failed run publishes which steps were cancelled in the transition
  event.

## DAG introspection

The live graph — every step's current status — is queryable:

```go
dag, _ := q.DAG(ctx, runID)        // nodes + edges + statuses
json, _ := q.DAGJSON(ctx, runID)   // same graph as JSON
svg, _ := q.DAGSVG(ctx, runID)     // standalone SVG document
```

`DAG` returns `DAGNode`s (`Name`, `Task`, `Status`, `DurationMs`, `Err`) and
`DAGEdge`s (`From`, `To`). `DAGJSON` is machine-consumable state for
dashboards; `DAGSVG` renders an embeddable picture (see
`examples/dagsvg`, which writes `dag.svg`).

## Limits

- One output per run: `Result` decodes the last declared step's output —
  order steps so the terminal result is last.
- Workflow input fans out to every step — keep it small; pass bulk data by
  reference (ids, keys) rather than embedding it.
- Retries and timeouts are per task, not per workflow.

## Define the name once

`Task.ID()` returns the task's identity (its name). Use it for the step name
and for dependencies instead of repeating a string literal that can drift or be
misspelled:

```go
charge := quacker.NewTask("charge", chargeFn)
ship := quacker.NewTask("ship", shipFn)

wf := quacker.NewWorkflow[Order]("fulfill",
    quacker.Step(charge.ID(), charge),
    quacker.Step(ship.ID(), ship, charge.ID()), // not the literal "charge"
)
```

The step name is the string that `DepOutput`, node labels, and dependencies
use; naming the step `charge.ID()` keeps the task and its step in sync. (If you
name a step differently, reference it by that step name.)
