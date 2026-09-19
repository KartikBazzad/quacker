# Plugins

Plugins extend the engine with **lifecycle hooks** — callbacks around enqueue,
step execution, run completion, and event emission. They are **compile-time**:
a Go type passed to `Open`, not a dynamically loaded `.so` or an out-of-process
server. That keeps quacker type-safe, cross-platform, and a single binary.

## A plugin

```go
type auditor struct{}

func (auditor) Name() string { return "audit" }

func (auditor) Hooks() quacker.Hooks {
    return quacker.Hooks{
        BeforeStep: func(ctx context.Context, s quacker.StepInfo) (context.Context, error) {
            if blocked(s) {
                return ctx, fmt.Errorf("rejected %s", s.RunID) // veto
            }
            return ctx, nil
        },
        OnRunFinished: func(ctx context.Context, r quacker.RunInfo) { record(r) },
    }
}

q, _ := quacker.Open(quacker.WithPlugin(auditor{}))
```

Set only the fields you need; nil callbacks are skipped. `Hooks` has:

| Callback | When | Can veto? |
|---|---|---|
| `BeforeEnqueue` / `AfterEnqueue` | around `EnqueueBatch` | yes |
| `BeforeStep` / `AfterStep` | around the task body | `BeforeStep` yes |
| `OnRunFinished` | when a run reaches a terminal status | no |
| `BeforeEmit` / `AfterEmit` | around `Emit` | `BeforeEmit` yes |

The info types — `StepInfo`, `EnqueueInfo`, `RunInfo`, `EmitInfo` — carry ids,
status, queue, attempt, key, and labels; no internal types leak.

## Ordering and semantics

- **`Before*` run in registration order**, and multiple plugins chain: each can
  annotate the `context.Context` it passes on.
- **`After*` run in reverse order** (like nested decorators unwinding).
- **`Before*` may veto** by returning an error. A `BeforeStep` veto fails the
  step **immediately, with no retries** (`FinalFailStep`: the step is FAILED,
  siblings are cancelled, the run fails). Use this for admission control,
  quotas, or rejecting work that can't succeed.
- **`After*` are observe-only.** They cannot change an outcome, and their
  panics are recovered and logged — a misbehaving observer never breaks a run.
- Hooks run **once per attempt**, for workflows and for runs recovered from a
  previous process. A task that isn't registered yet (File-mode restart) is
  parked without invoking hooks.

## Relationship to middleware and events

| | Scope | Can change flow? |
|---|---|---|
| **Plugins / hooks** | engine transitions (enqueue, step, run, emit) | yes — `Before*` veto |
| **Middleware** | the task body, per task or engine-wide | no (it decorates, but can short-circuit) |
| **`Subscribe`** | live status transitions, post-hoc | no |

Reach for middleware to wrap a task's body (timing, tracing, error taxonomies);
reach for plugins to gate or observe engine-level activity.

## Trust and concurrency

In-process hooks are **trusted code** — there is no sandbox. The engine invokes
hooks concurrently for parallel steps, so a plugin must be **safe for
concurrent use** (guard shared state). Panics in `Before*` are converted to the
veto error; panics in `After*` are recovered and logged.
