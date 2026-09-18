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

- Cron `@every` durations are rounded up to 1 second by the cron parser
  (`robfig/cron` `Every()` floors at 1s). v0.2 will construct
  `ConstantDelaySchedule` directly to support sub-second intervals.
- Crons live in the `crons` table for introspection but are driven from an
  in-memory map — after a restart the app re-registers them. v0.2 will arm
  DB-persisted crons automatically when their task is registered.
- Timeouts and cancellation are cooperative: tasks that ignore `ctx` are
  only stopped at the drain deadline, recorded as INTERRUPTED.
- `logs` has no foreign key to `runs` (deliberate: logs flush in batches and
  may outlive nothing in particular; cascade deletes aren't needed since
  v0.1 never deletes runs — retention arrives in v0.2).
