# Design notes & lessons from v0.1

Decisions worth remembering, and bugs caught during the build that the
current design guards against. Written down so v0.2 doesn't relearn them.

## Decisions

### 1. WAL-backed temp file is the default "in-memory" storage

Pure `:memory:` SQLite cannot enable WAL (WAL needs a shared-memory file), so
reads contend with writes through rollback-journal locks. Since the headline
feature is *reading state without interrupting execution*, the default is
`Ephemeral`: a temp file with `journal_mode=WAL, synchronous=OFF`, deleted on
Close. Same zero-durability semantics, truly non-blocking readers.
`Memory()` still exists for strict no-filesystem requirements; the tradeoff
is documented in the README.

### 2. One writer connection, separate read pool

SQLite permits a single writer. Rather than handling `SQLITE_BUSY` between
our own writers, all mutations share one pooled connection (with
`_txlock=immediate` so transactions never start as read-then-upgrade).
Introspection uses a `query_only` pool that, under WAL, never blocks the
writer. `busy_timeout=10s` is a safety net, not a mechanism.

### 3. The keeper connection gets its own pool

A shared-cache named `:memory:` database lives only while ≥1 connection is
open, so v0.1 first pinned a keeper connection *from the write pool* — which
has `MaxOpenConns(1)`. That deadlocked the very first migration (keeper holds
the only slot; `migrate` waits forever; all tests hang at Open). The keeper
now has a dedicated unused pool. Lesson: in-process "pin the DB" tricks must
not borrow from capacity you also need for work.

### 4. DAG progression is computed inside the completing transaction

When a step succeeds, `CompleteStep` (one write transaction) updates the
step, loads all sibling steps, unblocks dependents whose deps are all
SUCCEEDED, and — if no step remains active — moves the run terminal. Two
steps of the same run finishing concurrently can't race the completion
decision because both transactions serialize on the single writer. The engine
only reacts to what the committed transaction reports (publish events, wake
scheduler).

### 5. Steps have a name and a task — they are different columns

First implementation looked up the executing function by `steps.name`. That
works for single tasks (step name = task name) and silently broke named
workflow steps: `Step("audit", processTask)` claims a step named "audit" and
searched the registry for a task named "audit" — park, retry every 5s, never
succeed. The runnable `examples/introspect` caught it. Schema now stores
`steps.task` (registry key) separately from `steps.name` (DAG identity).

### 6. Claiming is single-threaded by construction

One scheduler goroutine issues all claims, so no double-claim defense is
needed between workers; each queue just tracks `inFlight` atomically. The
guarded `UPDATE ... WHERE status='QUEUED'` remains as a cheap invariant.

### 7. Guard every terminal-state write

Run-level final updates are conditioned on
`status NOT IN ('SUCCEEDED','FAILED','CANCELLED','INTERRUPTED')`. This makes
`Cancel` (which writes CANCELLED from the caller's goroutine) race-free
against executors recording their own outcome, without any locking between
them.

### 8. The single-writer guard is a kernel advisory lock, not a lockfile

A probe transaction can prove a database is writable, but only while held
— keeping exclusion would mean holding an exclusive txn for the process
lifetime, which blocks checkpoints and every other tool that touches the
file. Instead `Open` takes an exclusive non-blocking kernel advisory lock
(`flock` on unix, `LockFileEx` on Windows, `fcntl(F_SETLK)` on unixes
without `flock`) on a permanent sidecar file, `<path>.quacker.lock`, held
open for the store's lifetime.

The first implementation was a pid-recording lockfile created by
tmp-file + `link(2)`. It was replaced because the advisory lock is
correct by construction: acquisition is atomic (the kernel serializes
opens), the lock is released automatically when the holder dies — even
via SIGKILL — so there is no stale lock to reclaim and no
check-then-remove TOCTOU where two processes racing to reclaim the same
stale lock could both end up owning it. The sidecar's content
(pid/host/time) is now diagnostic only, written best-effort through the
already-open fd after the lock lands; an empty or stale file means
nothing.

Why a sidecar rather than locking the database file itself: on NFS,
`flock(2)` is emulated as a whole-file `fcntl` lock, which would collide
with SQLite's own `fcntl` byte-range locks on the same inode — every
write transaction would hit `SQLITE_BUSY`. SQLite never touches the
sidecar, so the lock is conflict-free. The sidecar is also never deleted:
unlinking a file another process holds open lets a new inode be created
for the path, and two inodes mean two independent locks — a double-owner
split-brain.

An in-process registry (`sync.Map` keyed by the absolute lock path) sits
in front of the OS call so a second `Open` of the same path in the same
process fails deterministically everywhere — POSIX `fcntl` locks are
per-process and would not self-conflict.

Caveats:

- The filesystem must support advisory locking (on NFS that means the
  server runs lockd); unsupported filesystems fail `Open` with a clear
  error rather than silently unguarding the database.
- On `!unix && !windows` platforms (and hurd, which ships neither
  `flock` nor `fcntl` locking in x/sys) there is no kernel lock at all:
  only same-process exclusion holds. File mode there is best-effort.
- Hardlink or symlink aliases to the same database inode get different
  sidecar paths and therefore different locks — a documented limitation;
  the guard covers the *path*, not the inode.

Upgrade note: binaries running the previous pid-file design hold no
kernel lock, so every old-binary process on a database path must be
stopped before a new-binary process opens it — the old lockfile's
content is invisible to `flock`, and the old release path could unlink
the sidecar out from under the new lock.

### 9. Checkpoints are PASSIVE on the timer, TRUNCATE only at close

A periodic `wal_checkpoint(TRUNCATE)` waits on reader snapshots (up to
busy_timeout) — the wrong thing to run on a hot path whose selling point
is non-blocking reads. `PASSIVE` checkpoints whatever is safely
checkpointable and returns immediately, so the timer caps WAL growth
whenever readers are momentarily idle. At `Close` no readers remain, so
one best-effort TRUNCATE leaves an empty (usually deleted) `-wal` file.

### 10. Per-key concurrency is a claim-time row count, not a semaphore

The roadmap sketch said "per-key semaphores in memory". The shipped design
keeps the semaphore in the database instead: each step persists its
`concurrency_key` (computed once at enqueue) and `key_limit`, and the
claim gate counts `RUNNING` rows sharing the key:

```sql
(concurrency_key = '' OR key_limit <= 0 OR
  (SELECT COUNT(*) FROM steps r
    WHERE r.concurrency_key = steps.concurrency_key
      AND r.status = 'RUNNING') < steps.key_limit)
```

The gate appears twice in `ClaimDue`: in the candidate `SELECT` so
saturated keys can't crowd the `LIMIT` with unclaimable rows, and in the
per-step `UPDATE` where it is re-evaluated against the transaction's own
earlier claims — the latter is what makes a batch of same-key candidates
safe (the first claims, the rest see the fresh RUNNING row and skip).

Why not the in-memory sketch: every terminal path — success, retry, fail,
cancel, halt-tombstone, interrupt sweep, park — would owe a decrement, and
any miss leaks a slot forever. Counting RUNNING rows is self-maintaining:
parked, cancelled, and interrupted steps free their key automatically, and
a File-mode restart inherits the right count from recovered state instead
of rebuilding a semaphore from nothing. The cost is one correlated count
per claim batch on an indexed column — trivial at single-writer scale.

### 11. Rate limiting is a sliding window over persisted claim times

Two designs were on the table: an in-memory token bucket, and counting
claim timestamps in the database. The token bucket loses on both
correctness axes that matter here:

- **Boundary bursts**: a full bucket fires N at t=0, refills, and fires N
  again at t=window — up to 2N starts in barely more than one window. The
  stated acceptance ("never exceeds N starts per window") is the
  sliding-window semantics, not token-bucket semantics.
- **Restart amnesia**: a bucket resets at Open, so a File-mode process
  restarted mid-window gets a fresh burst. `claimed_at` is persisted, so
  the window is real: `ClaimDue` counts
  `WHERE queue=? AND claimed_at >= now - window` inside the claim
  transaction and caps the batch at the remaining budget.

`claimed_at` is stamped on every QUEUED→RUNNING — deliberately *not*
`started_at`, which keeps first-start semantics for introspection and
would let retried steps sneak past the window (a retry is a start).
`ParkStep` clears `claimed_at` for the same reason it returns the attempt:
a parked claim never ran, so it shouldn't spend rate budget either.

### 12. Middleware wraps the task body, inside the panic guard

Middleware is a `func(engine.Handler) engine.Handler` decorator composed
`global → per-task → body`. Two choices matter:

- It is applied **inside** the executor's existing `recover`, not around the
  whole step lifecycle, so a panic in middleware fails the step with a stack
  trace exactly like a task panic — one recovery path, not two.
- The chain reads the global slice copy-on-write under `RLock` and composes
  **outside** the lock. Composing while holding the lock would let a
  middleware constructor that calls `Use` deadlock; mutating in place would
  race a concurrent executor. `Use` therefore always replaces the slice.

Middleware runs per attempt, which is what makes timing and retry
observability correct. `StepContext` gained `Task` (the registered function)
alongside `Step` (the DAG identity), because a named workflow step's tracing
label must be the workflow step while its function is a different task.

### 13. Task logs are a sink chain; SQLite persistence is opt-in

The engine used to own task logs: `TaskLogger` → buffered channel → batched
SQLite inserts. That made the engine the log store, which is the wrong job
for an embedded library and the biggest source of growth for a long-lived
daemon. Now `TaskLogger` writes to a sink chain:

- default: the engine's `slog.Logger` (nothing is silently lost);
- `WithTaskLogSink`: a custom base sink, so callers own retention, sharding,
  and export;
- `WithLogStorage(true)`: the original SQLite sink is *appended*, keeping
  `q.Logs` working for callers who want built-in querying;
- `WithLogSink(ctx, fn)` (used from middleware): redirects lines for a single
  step/ctx.

The context-scoped override is what makes "let the user store logs" possible
at all: a middleware wraps the body, but `TaskLogger` resolves its sink from
the context, so the sink must be injectable there. Sinks run synchronously in
the task goroutine — a deliberate contract that callers buffer a slow
destination themselves, rather than the engine hiding latency or dropping
lines. One consequence: a step's logs are no longer necessarily in SQLite,
so `q.Logs` returning empty is expected unless storage is enabled.

### 14. Purge invariants: terminal-only, no RUNNING step, explicit deletes

`PurgeRuns` is deliberately conservative:

- **Only terminal statuses are ever eligible.** A non-terminal status in the
  filter is an error, not a silent skip, so a typo can't delete live work.
- **`NOT EXISTS (step RUNNING)`.** `Cancel` makes a run `CANCELLED` while the
  executing goroutine is still recording its own step `CANCELLED`; without
  this guard a purge could delete the run out from under that write, turning
  a benign no-op `UPDATE` into a spurious "record failure" log. Deferring the
  purge until no step is RUNNING closes the window deterministically.
- **Steps and runs are deleted explicitly**, in order, rather than relying on
  `ON DELETE CASCADE`. The cascade is enabled, but retention is exactly the
  feature where being wrong is unrecoverable, so it does not depend on a
  pragma that some other connection or future DSN change could drop.
- **Batching and `Before > 0`.** Large deletes are chunked transactions so
  the single writer is never monopolized, and a zero cutoff (which would
  match every finished run) is refused.

The policy is storage-agnostic on purpose: `Memory`/`Ephemeral` daemons grow
RAM or a temp WAL file for the process's lifetime, and pausing to think "but
that's not durable" misses that the growth is real within a single run.

### 15. The metrics callback is an interval push, not a transition hook

`WithMetricsFunc` is a timer in the engine's `loopWG`, not a subscriber on
the transition bus. A per-transition hook fires at the bus's drop-on-overflow
cadence and couples callback cost to the hot path; Prometheus-style scrapers
want a steady sample, and the store is the source of truth. The loop snapshots
via the read pool (never blocking the writer in WAL mode), recovers callback
panics so a bad exporter can't take down the engine, and is joined by
`loopWG.Wait` before the store closes — the same shutdown discipline as the
scheduler and cron loops.

### 16. Sub-second cron uses a custom schedule, not ConstantDelaySchedule

The roadmap sketch said to build sub-second `@every` with
`cron.ConstantDelaySchedule{Duration}`. That type is unusable below one
second: its `Next` computes `t.Add(Delay - t.Nanosecond())`, so whenever the
current nanosecond field exceeds the delay the result moves *backwards*
(e.g. at `.800s` with a 500ms delay, `Next` returns a time 300ms in the
past). `cron.Every` hides this by rounding up to 1s. The engine instead
parses `"@every <d>"` itself (validating `d > 0`) and schedules it with a
three-line `everySchedule{d}` whose `Next` is exactly `t.Add(d)`.

The second half of the fix is the loop: the old `cronLoop` ticked every
200ms, too coarse for sub-second intervals. It now sleeps until the earliest
armed cron's `next` (capped at 200ms, floored at 1ms), so a 40ms schedule
fires on time without spinning. The cap preserves the old wakeup character
when only minute-scale crons exist. Lesson: a library helper can look
"equivalent" while being structurally wrong for the case you need — read its
`Next`, don't trust the name.

### 17. Persisted triggers arm on task registration, resuming cadence

Crons were persisted for introspection but never loaded, and event bindings
did not exist. Both now load on `Start` into **pending** maps (parsed but not
armed), and `RegisterTask` is the single arming point: it moves every pending
trigger whose target task just registered into the live maps. This makes
`Register` the only startup call a File-mode app needs — the documented
in-memory-crons/re-`Cron` dance is gone. `Enqueue` now routes through
`RegisterTask` too, so an enqueue that supplies a task definition arms
matching triggers as well.

Missed-fire policy is **resume cadence, skip missed**: if the stored `next`
is still in the future it is kept (so a restart doesn't shift a @daily's
phase), otherwise it is recomputed from now. Catching up every missed slot
would let a multi-day outage stampede the queue on boot; for schedules, one
fire after recovery is the useful behavior. `RegisterCron` deletes any
same-name pending entry before arming, so re-`Cron` at startup replaces
rather than duplicates.

### 18. Events are best-effort dispatch with a bounded audit log

In-process events are deliberately *not* durable delivery. `Emit` persists
the event row (for `q.Events` introspection) and then enqueues one run per
armed binding, each with the payload as input. A crash between the persist
and the enqueues can lose those dispatches — this is documented rather than
hidden; durable at-least-once `WaitFor` waits, added later (v0.3 slice B),
build on the journal/subscription machinery rather than this best-effort
fan-out. Bindings
themselves are persisted (`event_subscriptions`, unique per event+task) so
they re-arm like crons, and `Off` deletes both the in-memory and stored
binding.

Persisted events would otherwise be a fresh unbounded-growth hole of exactly
the kind retention was built to close, so `PurgeRuns` now also deletes
`events` older than its cutoff, in batches, and `PurgeResult` reports
`Events`. Subscriptions are never purged. The alternative — a separate
event-retention knob — was rejected as more surface for the same outcome.

### 19. Durable execution is journaled replay, not a resume handle

The roadmap's sketch proposed a "resumable-task protocol (`StepFunc` receiving
a resume handle)". Making `SleepDurable(ctx, d)` transparent — the API people
actually want — requires the task to resume in the middle of itself, and Go
has no continuations. Rather than introduce a second, phase-switch task style,
the engine keeps tasks as plain functions and gives them a **journal**:
migration v5 adds `step_journal`, every durable call appends an entry in call
order, and on each invocation a per-step cursor replays entries by index.
Satisfied entries return immediately; an unsatisfied one is persisted and the
task is unwound.

Three decisions make that work:

- **Suspension unwinds by panic, not error.** A helper that returned an error
  could be ignored (or logged) by a task, which would then keep running past
  its suspension point with no durable record. A private `suspendSignal`
  panic is caught by the executor's existing recover boundary, which checks
  for it before the generic panic-to-error conversion. The cost is a real
  contract: `recover()` around a durable helper — in a task *or middleware* —
  swallows the suspension. This is documented, not defended against.
- **Resumes reuse the claim path.** `ClaimDue` gained a second arm,
  `status='SUSPENDED' AND resume_at>0 AND resume_at<=now`. A sleep's wake and
  a wait's timeout are both just `resume_at`, so no separate sweeper or timer
  is needed: a SUSPENDED step is not RUNNING, so it holds neither a queue slot
  nor a per-key slot, and the existing DB-count key gate frees it for free.
- **A resume is a start for rate limiting but not an attempt.** It stamps
  `claimed_at` (so a mass-wake of sleepers can't slip past an N-per-window
  cap by consuming batch slots without window budget) but leaves `attempts`
  and `started_at` alone. The per-attempt timeout is applied per active
  segment, so a sleep longer than the timeout is fine.

`RunOnce` is part of the same slice, not a follow-up: any side effect before a
suspension re-executes on replay, so shipping sleep without it would ship a
feature whose canonical use is broken. It exercises the complete-without-
suspend journal path too. Its error semantics are deliberate — on `fn` error
the entry is left undone, so a retry re-invokes `fn`: exactly-once on
success, at-least-once on failure, with idempotency left to the caller.

### 20. A replay seatbelt turns silent corruption into a loud failure

Journaled replay's known hazard is a task whose durable-call shape changes
between invocations (data-dependent branches, or deploying new code while
runs are suspended). Silent misalignment is far worse than a failed step, so
each replayed entry is validated against the call: the entry `kind` must
match, a wait must carry the same `event`, and a `RunOnce` the same `key`.
A mismatch fails the step with `ErrJournalMisaligned` instead of returning
wrong-typed or wrong-keyed data. This catches the dominant modes — reordering
or removing different-kind calls, kind swaps, key swaps. It deliberately does
not catch same-kind identical-parameter reordering (semantically inert) or
changed-code-same-shape, which is `StepVersion` territory: a versioning
protocol, not a guard, and deferred with the limitation documented.

### 21. Event-wait delivery is atomic, and cancellation of a wait is one transaction

Two races had to be closed for `WaitFor`, and both come down to writing the
right things in one transaction on the single writer:

- **Suspend-vs-deliver.** The first cut appended the wait entry and *then*
  suspended the step. An `Emit` landing between those two writes would set
  the entry done, but the executor would then suspend the step with
  `resume_at=0` (event-only), so nothing would ever claim it — the event was
  lost. `SuspendWithJournal` now sets the step SUSPENDED and inserts the
  journal entry in one transaction, so a visible wait entry always means a
  suspended step. `Emit` (`DeliverEvent`) likewise inserts the event and
  wakes every matching undone wait in one transaction.
- **Timeout-vs-deliver.** A timeout is not just "wake and check": if an emit
  commits first, the timeout must lose. `TimeoutWait` does a conditional
  `UPDATE ... SET timed_out=1, done=1 WHERE ... AND done=0`; `rows affected
  == 0` means the emit won, so `WaitFor` re-reads the entry and returns the
  payload. `DeliverEvent` only touches `done=0` entries, so a post-timeout
  emit is a no-op on that wait. Without this, a late emit could flip a
  consumed wait back to delivered, and a retry would replay the payload
  instead of the timeout — cross-attempt misalignment.

The same principle as the parent-run failure path (§4): decide and record in
one transaction, and let later actors observe the committed result.

### 22. Child runs are lineage, deliberately without a foreign key or a join

`EnqueueChild` sets `runs.parent_id` from the executing step's context. Two
choices are deliberate:

- **No foreign key.** `parent_id REFERENCES runs(id)` would couple a run's
  lifetime to its parent's, so purging a parent would either cascade-delete
  children (losing live work) or fail on the constraint. Retention ages runs
  by completion time independently, so lineage is informational and a purged
  parent can leave a dangling id. `Execution.Children` simply queries by
  `parent_id`.
- **No implicit join.** A parent does not block on its children; the two run
  on the same engine with independent retries, timeouts, and terminal states.
  That keeps child runs composable with everything else (a child can itself
  suspend, fan out, or wait on events), and a failed child leaves the parent
  successful. A join, if wanted later, is expressible on top of the durable
  substrate — a parent can `WaitFor` a completion event, or poll descendants
  — rather than being baked into the enqueue path.

### 23. The DAG visualizer renders SVG itself, from stored state

`q.DAGJSON`/`q.DAGSVG` are per-run and read the run's stored `steps` (name,
`depends_on`, status, attempts), so they show the *current state* and work for
recovered or purged-adjacent runs, not just freshly registered workflows.
Rendering SVG in-process (dependency-level longest-path layout, one box per
step, bezier edges) avoids a Graphviz/cgo dependency and keeps the library's
"one `go get`, embeddable" promise; labels are XML-escaped and long names
truncated, and the layout is deterministic for a given graph. The alternative
— shelling out to `dot` — would have broken the no-external-binary property
for a fairly small amount of drawing code.

### 24. Step dependencies are a JSON array, not a delimited string

`steps.depends_on` started as a comma-joined `TEXT`, with `splitDeps`/`joinDeps`
as the codec. That couples the storage format to a delimiter that step names
were never forbidden from containing, so a step named `"a,b"` silently became
two dependencies. SQLite has no array type, so the fix is a JSON array
(`["a","b"]`) written and read by `encoding/json`; migration 7 rewrites legacy
rows with a `replace`-based SQL expression, and `splitDeps` still accepts the
old comma form as a safety net. The public `Step(name, task, deps ...string)`
signature is unchanged — variants already passed as `[]string` with `deps...`,
and snapshots (`StepState.Deps`, `DAGNode.Deps`) were arrays all along.

### 25. Debug logs are a fan-out stream, not a server

The engine's own diagnostics were only visible through the `*slog.Logger`
passed to `WithLogger`, which is fine when that logger is stdout but awkward
when a user wants to inspect engine activity programmatically. `q.DebugLogs`
adds a bounded, drop-on-full channel, and `New` wraps the engine logger in a
`fanoutHandler` so every engine record reaches both the user's handler and the
stream. `q.DebugLogger()` returns a logger writing to the same stream, so the
app can interleave its own lines. This keeps the "no server" promise — the
user pulls records and forwards them wherever — while the task-log sink chain
(§13) stays a separate concern: task logs are the app's output, debug records
are the engine's narration. `debugSend` checks a mutex-guarded closed flag, so
a log emitted after `Close` drops instead of panicking on a closed channel.

### 26. Batching amortizes the single writer without changing semantics

Two hot paths paid a transaction per item: enqueue (one `CreateRun` per run)
and claim (one `ClaimDue` per queue). Both now have batched forms —
`CreateRuns`/`EnqueueBatch` (N runs, one tx) and `ClaimDueMulti` (all queues,
one tx per tick). The semantics are intentionally unchanged:

- **All-or-nothing enqueue.** A batch validates and inserts together; a bad
  input or DAG leaves zero runs, matching the caller's expectation that a
  batch either lands or doesn't. Waiters are registered under one lock and
  removed together on failure, so a partial batch can't strand a waiter.
- **Claims stay guarded and gated per queue.** `ClaimDueMulti` loops the same
  `claimQueueTx` used by single-queue `ClaimDue` (which is now a one-element
  wrapper), so the key gate, the rate-window budget, the resume arm, and the
  per-step rows-affected check are byte-for-byte the same. Batching moves the
  transaction boundary, not the admission rules.

The trade is explicit: SQLite allows one writer, so parallel producers
serialize and per-op latency rises with `-cpu` (see BENCHMARKS.md). Batching
is the lever — amortize N runs over one commit rather than pretending the
writer parallelizes.

### 27. Worker labels are a subset gate on the claim, not a queue per worker

A task can require labels (`WithLabels("gpu")`) and an engine advertises a set
(`WithWorkerLabels("gpu","linux")`); the scheduler claims a step only when the
step's labels are a subset of the engine's. Two consequences are deliberate:

- **Subset, not equality.** An engine with extra labels still runs a task that
  needs fewer, so a beefier worker isn't wasted. A task with no labels is
  universal; an engine with no labels runs only unlabeled tasks (the safe
  default — an untagged worker doesn't accidentally pick up specialized work).
- **One column, not a queue matrix.** Encoding routing as a queue per
  worker-class would blow up combinatorially and lose the "any worker that
  can" semantics. Storing labels as a JSON array on the step and testing
  membership with a `json_each` subquery in the claim SELECT keeps it to one
  predicate, evaluated inside the same transaction as the other gates.

This is the in-process half of routing. Cross-process routing (the Postgres
epic) reuses the same predicate — each node's claim is filtered by its own
label set — but additionally needs worker identity and leases so a node that
dies mid-step doesn't strand it.

### 28. Tracing is an async link, not a parent, and the propagator is explicit

The engine emits OTel spans only when a `TracerProvider` is supplied (the
library imports just the `otel` API; the SDK/exporter is the caller's). Three
choices matter:

- **The step span is a new root that *links* to the enqueue span** rather than
  being parented to it. Enqueue and execution are separated by an arbitrary
  delay, possibly another process or a previous boot; parenting would produce
  a misleading multi-minute "span" covering the wait. Links are the standard
  async-trace shape and keep the causal edge without the fake duration.
- **The producer traceparent is persisted on the run** (migration 9). An
  in-memory map would lose the link on restart and leak entries; persisting
  one string column makes the link durable and inspectable
  (`Execution().TraceParent`).
- **The W3C `TraceContext` propagator is used directly, not
  `otel.GetTextMapPropagator()`.** The global propagator defaults to a no-op,
  so the first cut silently produced empty traceparents and no links — a bug
  the tracing test caught immediately. Using the concrete propagator makes the
  stored format self-contained regardless of global setup.

When tracing is off, `Engine.tracing` is false and every helper returns
before touching attributes, so the disabled path allocates nothing on the
claim/enqueue hot paths.

### 29. Postgres is a dialect seam, not a second query layer

Adding a second database could have meant duplicating ~1000 lines of SQL or an
interface with two full implementations. Instead the query layer stays single
and dialect-neutral: it writes `?` placeholders and calls a registered
`Backend` for the pieces that differ — connection setup, `Rebind`
(`?`→`$n`, memoized, quote/comment-aware), `Migrations`, `MigrateLock`,
`SupportsCheckpoint`, `RecoverOnBoot`, and the `LabelGate` SQL. The driver
lives in the public `postgres/` package and registers from `init`, so pgx is
compiled only when a build imports it (the database/sql driver pattern) — the
core and SQLite-only users never see it.

Running against a real Postgres immediately caught two bugs a WAL-only test
suite could not:

- **`CASE … ELSE 0 END` pinned the parameter type to int4.** In
  `recoverInterrupted`, a unix-nanos parameter inside a `CASE` with an integer
  `ELSE` made Postgres infer 32-bit `INTEGER`, overflowing on every recovery.
  Rewriting `keep`/`!keep` as distinct statements (rather than a `CASE`) is
  both clearer and type-safe. The general lesson: `?` hides type inference, so
  any parameter whose value is 64-bit must sit in a context that infers a
  64-bit type.
- **Boolean columns need boolean literals.** `done=1`/`done=0` are fine for
  SQLite's INTEGER storage but error on Postgres `BOOLEAN`; `TRUE`/`FALSE`
  work on both (SQLite understands them as 1/0).

The backend is single-instance by construction for now: `recoverInterrupted`
re-queues all RUNNING rows on boot and `InterruptAll` sweeps them all on
shutdown, which is only safe when one engine owns the database. That is
exactly what the multi-instance phase (§Phase 2: worker leases, claim advisory
lock, `FOR UPDATE` run locks, cron CAS) replaces.

### 30. Multi-instance correctness is leases plus three locks, added only where needed

Everything in the engine assumed one process: `recoverInterrupted` re-queued
every RUNNING row on boot, `InterruptAll` swept them all on shutdown, and the
claim/DAG gates were only correct because the single writer serialized them.
Making several engines safe against one Postgres database adds four things,
each scoped to the networked backend so SQLite is untouched:

- **Step leases** (`worker_id`, `lease_expires_at`). A claim takes a lease; a
  heartbeat extends this worker's in-flight steps; a leaderless reaper
  re-queues expired ones. Reaping is guarded on `status='RUNNING'`, so
  concurrent reapers can't double-requeue. Postgres disables boot recovery
  entirely (`RecoverOnBoot` false) — the boot requeue that is correct for one
  process would, in a cluster, steal a peer's live work. This is the single
  most important switch: recovery becomes continuous and lease-driven instead
  of "assume everything RUNNING is dead".
- **A global claim advisory lock.** Claims already take one transaction per
  tick; adding a `pg_advisory_xact_lock` at its start makes the counting gates
  (per-key, rate window) serialize across nodes exactly as the single writer
  does locally, with no change to the gates themselves. The alternative —
  `SELECT … FOR UPDATE SKIP LOCKED` plus per-key advisory locks — buys claim
  parallelism at a large complexity cost; claims are short, so the simple lock
  is the right first move.
- **Per-run `FOR UPDATE`.** `CompleteStep`/`FinalFailStep`/`CancelRun` read a
  run and its steps then decide the terminal transition; two nodes finishing
  sibling steps could otherwise both see "no active steps" and race. Locking
  the run row at the start of the transaction serializes the decision.
- **Cron CAS.** Each engine arms crons in memory; without coordination N nodes
  fire each occurrence N times. CAS-ing `next_at` *in the same transaction as
  the enqueue* makes firing at-most-once across the fleet (a crash rolls both
  back and retries), with no leader election.

Shutdown and leases interact deliberately: `InterruptAll(workerID, …)` sweeps
only the closing worker's RUNNING steps and converges the runs it leaves with
no active step, so a graceful restart of one node never interrupts another's
in-flight work.

### 31. DAG completion reads only the dependents of the step that finished

`CompleteStep` advanced the DAG by loading *every* step row of the run — via
`stepCols`, so each row carried its `input` and `output` blobs — then scanning
in Go for BLOCKED steps whose deps had all succeeded and for any remaining
active step. That is O(steps) per completion and O(steps²) for a wide DAG; a
chain of 800 steps took ~2.6s to finish.

The fix is to do in SQL exactly what the decision needs:

- read only the **direct dependents** of the step that just finished
  (`BlockedDependentsSQL`, a dialect snippet over `depends_on`), not every
  blocked step;
- look up only those dependents' **dependencies' statuses**;
- probe for remaining work with `SELECT 1 … LIMIT 1` rather than counting rows;
- fetch the run output with one `ORDER BY ord DESC LIMIT 1` only when the run
  actually terminates.

Two indexes make it stick: `(run_id, status)` so BLOCKED steps and the
terminal probe are found directly, and `(run_id, name)` so dependency statuses
are point lookups. The 800-step chain fell to ~0.24s (~10×).

Two lessons from getting there:

- **The first attempt was worse, not better.** Expressing the unblock as one
  `UPDATE … WHERE NOT EXISTS (json_each(depends_on) …)` looked elegant but on
  SQLite the correlated subquery was re-evaluated per candidate row and turned
  an O(steps) scan into something closer to O(steps³) — 28s for the same 800
  steps. Portable-looking SQL still has to be checked against the query
  planner on each dialect.
- **Benchmark methodology bit twice.** A shared store across benchmark
  iterations accumulates rows from every iteration, so per-op cost drifts up
  with `b.N` and looks like an algorithmic problem that isn't there; the
  benchmark now uses a fresh store per iteration with setup outside the timer.
  Measure the thing you changed, in isolation.

### 32. Plugins are compile-time and explicit; hooks are a struct, not interfaces

The plugin system is deliberately the Go idiom — interfaces + registration —
not `-buildmode=plugin` (platform-locked, toolchain-hash-fragile) or an
out-of-process server (contradicts "embeddable, no deploy"). A plugin is a
`Plugin` (name + `Hooks()`) passed to `Open(WithPlugin(p))`, so there is no
global registry, no init-order coupling, and it is trivial to test. The public
surface is a **single `Hooks` struct of optional function fields** rather than
a family of capability interfaces; Jev rated the struct higher (0.72) once the
minimal-surface constraint was explicit, and it keeps docs and registration to
one type.

Semantics chosen for least surprise:

- `Before*` run in registration order and may veto; `After*` run in **reverse**
  (Jev 0.91) so they pair like nested decorators.
- `After*` are **observe-only** — they cannot change an already-recorded
  outcome, and their panics are recovered and logged (Jev 0.99). Only `Before*`
  can influence flow, by returning an error.
- A `BeforeStep` veto is an **immediate FAILED with no retries**: a deterministic
  rejection shouldn't burn the retry budget, and a transient condition is the
  hook's to allow. This reuses `FinalFailStep` (siblings cancelled, run failed).
- Hooks are engine-level, complementing per-task middleware (which wraps the
  body) and `Subscribe` (a post-hoc, drop-on-overflow transition stream).
  Order is hook → middleware → body → middleware → hook.
- Hooks run per attempt, for workflows and recovered runs; the unregistered-task
  park path is skipped. In-process plugins are trusted code.

### 33. The codec covers user payloads, never the schema's own JSON

`WithCodec` swaps the encoder for the data tasks move around — task input and
output, `DepOutput`, event payloads, and durable `RunOnce`/`WaitFor` values —
but deliberately **not** `steps.depends_on` and `steps.labels`. Those two are
read by SQL in the claim gate (`json_each`/`jsonb_array_elements_text`), so
letting a codec change them would couple scheduling to the wire format. Keeping
them JSON means a custom codec has zero effect on dependencies, labels,
routing, or concurrency — only on how payloads are (de)serialized.

Two consequences worth stating:

- **The codec lives where the values are decoded.** Task bodies and `DepOutput`
  read it from the step context (`CodecFromContext`); `RunOnce`/`WaitFor` use
  their step state's engine; `Result` carries the engine's codec on the
  `RunHandle`; enqueue/emit use the engine directly. That is why the key
  extractor (`WithKey`) takes the codec too: it decodes the input at enqueue
  time, and a custom codec would otherwise silently leave every run unkeyed.
- **One codec per database, held across restarts.** Durable journal values and
  dependency outputs are stored in whatever the codec produced and decoded by a
  later process, so switching codecs mid-flight (or between two nodes) would
  mis-decode stored bytes. This is documented on `WithCodec` rather than
  guarded, because pinning the codec name in the schema would be a heavier
  commitment than the feature warrants.

Default is `JSONCodec` (a thin `encoding/json` wrapper), so existing data and
behavior are byte-identical.

### 34. Storage stays first-party; there is no public backend contract

We considered shipping a public plugin point for storage and decided against
it. The two shapes both fail:

- **Publish `Backend` (the SQL-dialect seam).** It is not a storage contract at
  all — it is SQL internals: `Rebind` (`?`→`$n`), per-dialect DDL, the
  `json_each`/`jsonb_array_elements_text` label gate, advisory/`FOR UPDATE`
  locks. Exposing it freezes those internals and still only lets someone write
  *another SQL database*, not a different store.
- **A backend-agnostic `Store` interface.** The engine plus public API drive
  ~39 operations, and the transactional correctness lives *inside* them —
  claim key/rate/label gating, DAG completion, atomic event delivery, cron CAS,
  and leases. A third-party driver would have to reimplement all of it with
  identical guarantees, and we cannot enforce that without a conformance suite
  we'd have to build and maintain.

For an embedded, single-binary library the value doesn't justify a large frozen
surface plus an unverifiable correctness contract on someone else's code.
Storage is first-party: SQLite (Memory/Ephemeral/File) and Postgres. Adding a
new SQL database is a normal in-repo change — implement a `Backend` and its
migrations, and let the existing query layer and tests cover it (the Postgres
driver is the template). v1.3's extension story is the compile-time plugin
hooks (§32) and the payload codec (§33), which extend behavior without moving
the durability guarantees out of the engine.

## Lessons (bugs the tests caught)

- **A transaction that isn't committed is a rollback.** `CancelRun` returned
  `rows.Err()` as the function's error and never called `tx.Commit()`; the
  deferred rollback undid the CANCELLED status, and the executor then saw a
  still-RUNNING run and marked it FAILED. Fixed by returning `tx.Commit()`
  and auditing every transaction function for a commit path.
- **INSERT column/value mismatch fails at first execution, not compile
  time.** The runs/steps INSERTs were rewritten several times (adding
  columns); placeholder counts drifted twice. Tests fail within milliseconds,
  but a `CREATE TABLE`-adjacent schema test that inserts through the real
  query path is what makes this visible immediately.
- **Retry loops need a "not registered in this process" park state.** With
  File storage, a restarted engine claims work before the app has registered
  task functions. Failing those runs outright would destroy valid work;
  instead the step is parked for 5s and retried until registered.

## Known v0.1 defects (fixed before first release)

All of the following were found by a pre-release review, fixed, and covered
by regression tests in `fixes_test.go`:

- **Task-log sink was a package-level global** — two `Quacker` instances in
  one process misrouted task logs into the other engine's database. The sink
  is now engine-scoped (carried through the step context).
- **`StepFromContext`/`RunIDFromContext` could never succeed** — the context
  stores a `*stepState` but the accessor asserted the value type
  `StepContext`, so both public helpers silently returned false/"" forever.
  Now asserted correctly and covered by a test.
- **`e.waiters` grew one entry per run, forever** — every cron firing leaked
  a waiter. Waiters are now deleted on completion, cancellation, and close.
- **Park-retry consumed the retry budget** — a claimed step whose task wasn't
  registered (File-mode restart before `Register`) burned a real attempt per
  5s park cycle. `ParkStep` now hands the attempt back; parking never runs
  the task or spends retries.
- **`queueState.concurrency` data race** — read outside the mutex by the
  scheduler tick while `SetQueue` wrote it; now atomic.
- **Enqueue ignored the caller's context** — a cancelled context still
  enqueued. The caller ctx now governs the insert.
- **Run failure didn't cancel sibling contexts; steps could resurrect.**
  `FinalFailStep` marked RUNNING siblings CANCELLED in the DB but left their
  goroutines running; and `CompleteStep` would happily flip a CANCELLED step
  to SUCCEEDED. Now: the engine cancels sibling contexts (with halt
  tombstones for the claim/cancel race window), `CompleteStep` refuses to
  touch steps of a terminal run (and converges the racing step to
  CANCELLED), and the executor treats any terminal run as "record CANCELLED,
  do not retry".
- **Cancel raced claim→registration** — a cancel landing in the window
  before the executor registered its cancel func never interrupted the task.
  Fixed with an atomically-checked halt-tombstone map.
- **Documented default jitter was never applied** — `Exponential` (and the
  implicit default backoff) now set 10% jitter explicitly; `Constant` stays
  deterministic; a zero-value `Backoff` remains "library defaults".
- **Post-`Close` panic vector** — a task outliving the drain deadline could
  send on the closed log channel (send-on-closed panics even inside
  `select`). The channel is now sealed under a mutex before closing.
- **Enqueue-during-Close TOCTOU** — a waiter registered after the sweep was
  never released, hanging `Result`. The closing check and waiter registration
  now happen under one lock. Note: a run accepted just before Close can
  still end INTERRUPTED (it may not have been claimed) — that is the
  documented drain contract.
- **Dead code removed**: `InterruptRun`, `DueCrons`, `ClaimCron`, `ListCrons`
  (the engine drives shutdown sweeps and crons internally). Stale doc
  reference to a nonexistent `quacker.After` fixed; `CronInfo` gained JSON
  tags; `GetStepOutputs` moved to the read pool; `logHandler.WithAttrs` no
  longer aliases its parent's slice; `LogValuer` values are resolved;
  `go.mod` `// indirect` markers corrected.

## Lesson: shared-cache read transactions can deadlock the writer

The first fix for "Execution is not one consistent snapshot" wrapped the
run+steps reads in a single read transaction. Under concurrent snapshots in
Memory mode this deadlocked the writer's claims instantly with
`SQLITE_LOCKED_SHAREDCACHE` ("database is deadlocked") — an error
`busy_timeout` deliberately does **not** retry. This is the sharp edge of
the ":memory: cannot WAL" caveat. The final design needs no transaction:
**read the run row first, then the steps.** Step statuses only advance
forward, so a terminal run observed at time T can never be paired with steps
read at T+ε that are less terminal — the invariant that matters
("terminal run ⇒ terminal steps") holds with plain ordered reads, and there
is nothing for the writer to collide with.

## Other behaviors worth knowing

- Cron `@every` supports sub-second intervals: the engine parses it and uses
  its own fixed-delay schedule, because `robfig/cron`'s
  `ConstantDelaySchedule.Next` is wrong below one second (see §16).
- Crons and event subscriptions persist and re-arm automatically the moment
  their target task is registered; a File-mode app only needs `Register` at
  startup (see §17).
- Timeouts and cancellation are cooperative: tasks that ignore `ctx` are
  only stopped at the drain deadline, recorded as INTERRUPTED.
- `logs` has no foreign key to `runs`: they are only persisted when
  `WithLogStorage(true)` is set, and are deleted explicitly by retention
  (which also runs an orphan sweep) rather than by cascade (see §13, §14).
- The DAG visualizer (`q.DAG`/`q.DAGSVG`) lays nodes out by dependency level
  and renders SVG itself, so there is no Graphviz or browser dependency; the
  layout is derived from the stored `depends_on` edges and current step
  states, not from a registered workflow definition.
