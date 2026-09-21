# Benchmarks

Numbers from the benchmark suite (`bench_test.go`, `internal/store/bench_test.go`).
All values are single-process, embedded SQLite — there is no network anywhere
in the loop.

## Results

Machine: **Apple M4, 10 cores** (darwin/arm64), Go 1.26.5, `modernc.org/sqlite`
(pure Go), storage noted per benchmark. Re-measured after v1.9.

```
BenchmarkEnqueueRun-10        426,655 ns/op  ~2,300 enqueues/s (contended)
BenchmarkThroughput-10        458,678 ns/op  ~2,180 runs/s (enqueue→execute→Result)
BenchmarkSaturatedThroughput  ~1,300,000,000 ns/op for n=5000  ~3,800 runs/s
```

The saturated figure was ~2,500 runs/s before the claim index in "Claim
candidates" below; that one index lifted it ~1.5× by removing the claim
query's full due-set sort.

- **`BenchmarkEnqueueRun`** is `Enqueue` in a loop while the scheduler and
  workers drain the same database, so it is a contended enqueue+execute figure,
  not clean insert latency.
- **`BenchmarkThroughput`** awaits each run before enqueuing the next, so it is
  per-job round-trip latency (~460 µs), *not* saturated throughput.
- **`BenchmarkSaturatedThroughput`** (Ephemeral/WAL, n=5000) is the real
  ceiling: enqueue a batch, then wait for all runs. It is the number to compare
  against a queue's "jobs/sec". It scales with queue concurrency — ~3,800
  runs/s at 64 workers, ~4,050 at 128, ~4,400 at 256 (`QUACKER_SAT_WORKERS`) —
  because more in-flight work lets the batched completer form larger batches.
  (Before the claim index these points were ~2,540 / ~3,230 / ~3,790.) At n=1000
  it is ~2,460 runs/s (less index depth); on `Memory()` (no WAL) it collapses to
  ~440 runs/s — use `Ephemeral`/`File` for throughput benchmarks, never
  `Memory`.

## Producer throughput (insert-only)

The store layer isolates the insert path (no worker activity). `EnqueueBatch`
inserts N runs in one transaction; per-run cost falls with batch size.

SQLite, `ModeEphemeral` (`BenchmarkCreateRunsBatch`, n=100 ≈ 25,700 inserts/s):

```
n=1     115,240 ns/op   ~115 µs/run   ~8,700/s
n=10    445,873 ns/op    ~45 µs/run  ~22,400/s
n=100 3,893,511 ns/op    ~39 µs/run  ~25,700/s
```

Postgres (`BenchmarkPostgresEnqueueBatch`, far-future runs so nothing executes
— insert-only). Runs and steps are written with multi-row INSERTs, chunked by
the backend's `InsertBatchRows` (200 for Postgres/MySQL, 25 for modernc SQLite,
which slows down on large VALUES lists). Measured locally against a Postgres 16
container, old per-row vs new multi-row:

```
        per-row (old)        multi-row (new)
n=1      1,004,298 ns/op     1,152,660 ns/op   ~1.0 ms/run
n=10     4,473,823 ns/op     1,174,792 ns/op   ~3.8x
n=100   26,362,766 ns/op     4,545,892 ns/op   ~5.8x
n=1000 280,775,772 ns/op    37,723,296 ns/op   ~7.4x
```

MySQL gains more (its per-statement cost is higher), measured with
`BenchmarkMySQLEnqueueBatch`:

```
        per-row (old)        multi-row (new)
n=1     33,145,236 ns/op     8,006,377 ns/op   ~4.1x
n=10   211,223,842 ns/op    17,527,225 ns/op  ~12x
n=100 1,255,097,403 ns/op   59,561,431 ns/op  ~21x
n=1000 1,964,487,928 ns/op 446,601,799 ns/op  ~4.4x
```

Batch size is per-dialect because it is not monotonic on SQLite: a sweep of
`BenchmarkZZBatchSize`-style runs showed 25 rows best for modernc SQLite (a
200-row batch roughly halved saturated throughput), while networked backends
want the round-trip reduction of a large batch.

Concurrent producers use the pool and scale
(`BenchmarkPostgresEnqueueParallel`, 100 runs/op):

```
-cpu 1    24.55 ms/op  ~4,070/s
-cpu 4    11.56 ms/op  ~8,650/s
-cpu 10    8.65 ms/op ~11,560/s
```

## Comparison to River

River reports **~46,000 jobs/sec on a commodity MacBook Air (M2)** from its
batch job completer — that is **work/completion** throughput (it batches
completions and excludes insertion), on Postgres, across many workers.

quacker on Postgres, saturated end-to-end (insert + execute + complete, one
transaction per completion, single writer):

| | River | quacker |
|---|---|---|
| Units | work/s (batch completion) | jobs/s (insert + work + persist) |
| Storage | Postgres, many connections | SQLite (embedded) or Postgres |
| Machine | M2 Air, 8 cores | M4, 10 cores |
| Figure | ~46,000/s | ~3,800/s (Ephemeral, 64 workers; ~4,400 at 256) |

So River is roughly **12× higher** at completion throughput. The reasons are
structural, not a constant factor:

- **Batched completion, but still per-row queries.** quacker coalesces step
  successes into one transaction per flush (`CompleteSteps`, v1.10), which
  lifted saturated throughput ~1.4× and made it scale with worker count; River
  additionally collapses the row updates into one statement
  (`UPDATE … WHERE id = ANY`), which quacker has not done. quacker has since cut
  the per-completion statement count (see "Completion" below).
- **Batch insert.** quacker now issues multi-row INSERTs (see "Producer
  throughput"); River's `InsertManyFast` uses Postgres `COPY`, which is still
  cheaper for very large batches.
- **Concurrency.** River fans work across pooled connections; SQLite permits a
  single writer (quacker's design trade for embedding).

What quacker buys for that: zero infrastructure (no Postgres, no broker), pure
Go SQLite, one process, and the same code path across SQLite/Postgres/MySQL.
The doc's framing stands — compare intent, not absolutes.

## Batching & parallelism (v0.3)

`EnqueueBatch` inserts N runs in one transaction, and `ClaimDueMulti` claims
across every queue in one transaction per scheduler tick (was one per queue).
The insert win, isolated at the store layer so background execution doesn't
pollute it (`BenchmarkCreateRunsBatch`, `ModeEphemeral`):

See "Producer throughput" above for current numbers: a 100-run batch is ~3×
cheaper per run than one run per transaction — the fixed commit cost is
amortized. The scheduler wakes once per batch.

Parallel producers do **not** scale linearly: SQLite allows one writer, so
`BenchmarkEnqueueParallel` shows per-op latency *rising* with `-cpu` as
producers serialize on the writer. That is the intended trade — embeddable
and dependency-free over write parallelism — and the reason `EnqueueBatch`
exists for fan-out.

## DAG completion (v1.2)

Finishing a step advances its DAG: unblock newly-ready dependents, then
terminate the run once no step is active. The original `CompleteStep` loaded
every step row — including `input`/`output` blobs — on each completion, making
a wide DAG O(steps²). It now reads only the direct dependents of the step that
finished plus their dependencies' statuses (indexes `(run_id, status)` and
`(run_id, name)`), and checks for remaining work with a `LIMIT 1` existence
probe. `BenchmarkWideDAGComplete` (a chain where each completion unblocks the
next) went from:

```
n=800   before ~2.57 s/run        (full-row scan per completion)
n=800   after  ~0.24 s/run        (~10x)
```

with completion cost now roughly flat per step instead of growing with the run
size.

## Completion statement count

A completion used to issue eight statements per step (read the run, update the
step, read its name, probe for BLOCKED dependents, check for remaining active
steps, read a failed sibling's error, read the last step's output, update the
run). Two changes cut that:

- The step name now travels on the `Completion` (the executor already knows it),
  and the run's outcome and last output are read in one query, so a DAG run
  completion drops from eight statements to six.
- A run whose **only** step just finished takes a fast path: it is terminal,
  has no dependents to unblock, no failed sibling, and its own output is the
  run output. `CompleteSteps` classifies the batch with one `GROUP BY` query, so
  the common one-task-per-run shape needs only three statements.

`BenchmarkPostgresComplete` completes single-step runs in batches of 100 (no
engine, so it isolates the completion statements) against a local Postgres 16
container:

```
before   112,977,966 ns/op   (~1.13 ms/completion, 8 statements)
after     52,878,324 ns/op   (~0.53 ms/completion, 3 statements)   ~2.1x
```

The full collapse River does — one bulk `UPDATE … WHERE id = ANY` for the whole
batch — remains the next step; it needs `RETURNING` (Postgres/SQLite) with a
separate MySQL path.

## Claim candidates (queue scan)

The scheduler's candidate read (`claimCandidatesSQL`) was the largest single
cost in a saturated run: CPU-profiling `BenchmarkSaturatedThroughput` put
`ClaimDueMulti`/`claimQueueTx` at ~30% of samples, versus ~2% for completion and
~3% for insert. `EXPLAIN QUERY PLAN` showed why: the query's two arms
(QUEUED/`run_at`, SUSPENDED/`resume_at`) were indexed, but the
`ORDER BY priority DESC, run_at, ord` forced a `USE TEMP B-TREE FOR ORDER BY`,
so every tick gathered *all* due steps, ran the correlated concurrency/sequence/
label gates on each, sorted them, and then took `LIMIT 64`.
`BenchmarkClaimCandidates` isolates the read at n=5000:

```
no new index                 ~9,840,000 ns/op
(queue, status, run_at)      ~9,950,000 ns/op   (no help: still must sort)
(queue, priority DESC, run_at, ord)  ~390,000 ns/op   (~25x)
```

The index whose column order matches the `ORDER BY` lets the scan walk it in
claim order and stop at `LIMIT`; the plan becomes a single
`SEARCH steps USING INDEX idx_steps_queue_claim (queue=?)` with no temp B-tree.
It is now flat in queue depth (~0.38 ms at n=500 and n=5000). Adding it lifts
saturated throughput ~1.5× (see Results) at the cost of ~5% on the batch-insert
path. Migration22 adds it for SQLite, Postgres, and MySQL.

## Idle scheduler (empty claim transactions)

The scheduler used to open a claim transaction every poll interval even when
nothing was due — with the 50ms default that is 20 empty transactions/second
per process, and far more in tests that poll at 1ms. It now sleeps instead:
`Store.NextDue` returns the earliest scheduled QUEUED `run_at` or SUSPENDED
`resume_at`, and the scheduler waits until then rather than polling. Any
mutation that makes work claimable — enqueue, retry, resume, completion,
suspend, park — wakes it. If work is already due but was not claimable (held by
a concurrency/sequence/label gate, or inserted by `EnqueueTx` after the claim)
it re-checks at the poll interval instead of sleeping; only genuinely future
work sleeps longer. The sleep is capped (one minute for SQLite, one second for
Postgres/MySQL, whose `EnqueueTx` inserts on a caller-owned transaction the
engine cannot observe at commit).

While runs are in flight the loop paces claims at the poll interval. Wakes are
split: an *urgent* wake (an enqueue, an unblocked dependent, a resume, a retry)
is honored immediately, so a workflow's next step is claimed without waiting a
poll; a plain *slot-free* wake is ignored while busy, since waking per
completion would open a claim transaction each and contend with the single
writer. That is why `BenchmarkSaturatedThroughput` now also reports
`claimtx/op` — for n=5000 it is ~100, i.e. the whole run is claimed in ~100
transactions rather than one per completion. A rate-limited queue's steps stay
due, so the scheduler re-checks it at the poll interval rather than sleeping.

Covered by `TestNextDue` (store), `TestScheduledRunFiresAtDueTime` (a run
scheduled 60ms out fires even with a one-hour poll interval), and
`TestIdleSchedulerStopsClaiming` (at a 1ms poll an idle engine opens at most a
handful of claim transactions in 200ms, versus ~200 before).

## Reproduce

```sh
go test -run XXX -bench . -benchtime 2000x ./...
go test -run XXX -bench 'BenchmarkSaturatedThroughput' -benchtime 1x .
go test -run XXX -bench 'BenchmarkEnqueueParallel' -benchtime 2000x -cpu 1,4,10 .
go test -run XXX -bench 'BenchmarkCreateRunsBatch' -benchtime 300x ./internal/store/
go test -run XXX -bench 'BenchmarkWideDAGComplete' -benchtime 200x ./internal/store/
go test -run XXX -bench 'BenchmarkClaimCandidates' -benchtime 200x ./internal/store/

# Postgres producer throughput (needs a server):
QUACKER_TEST_POSTGRES_DSN=... \
  go test -run XXX -bench 'BenchmarkPostgresEnqueue' -benchtime 50x -cpu 1,4,10 ./postgres/
QUACKER_TEST_POSTGRES_DSN=... \
  go test -run XXX -bench 'BenchmarkPostgresComplete' -benchtime 50x ./postgres/
QUACKER_TEST_MYSQL_DSN=... \
  go test -run XXX -bench 'BenchmarkMySQLEnqueueBatch' -benchtime 50x ./mysql/
```

Drop `-benchtime` for quicker runs; raise it for stable numbers. Run with
`-benchmem` to see allocation counts. Storage mode can be swapped in
`bench_test.go` (`Memory()` → `Ephemeral()` → `File(path)`) to compare
backends; `File` on an SSD should be close to `Ephemeral` since
`synchronous=NORMAL` only fsyncs on WAL checkpoints.

## What's not yet measured (follow-up candidates)

- `Ephemeral` vs `Memory` vs `File` delta (expect: memory slightly faster per
  write, at the cost of reader/writer lock contention).
- Cost of a durable suspend/resume round-trip vs a plain retry.
- Wide-DAG claim cost as the step count grows (the batch is one tx per tick).
