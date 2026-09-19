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
BenchmarkSaturatedThroughput  2,713,627,250 ns/op for n=5000  ~1,840 runs/s
```

- **`BenchmarkEnqueueRun`** is `Enqueue` in a loop while the scheduler and
  workers drain the same database, so it is a contended enqueue+execute figure,
  not clean insert latency.
- **`BenchmarkThroughput`** awaits each run before enqueuing the next, so it is
  per-job round-trip latency (~460 µs), *not* saturated throughput.
- **`BenchmarkSaturatedThroughput`** (Ephemeral/WAL, 64 workers, n=5000) is the
  real ceiling: enqueue a batch, then wait for all runs. It is the number to
  compare against a queue's "jobs/sec". At n=1000 it is ~2,460 runs/s (less
  index depth); on `Memory()` (no WAL) it collapses to ~440 runs/s — use
  `Ephemeral`/`File` for throughput benchmarks, never `Memory`.

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
— insert-only; each run is two statements, one `runs` and one `steps`):

```
n=1      765,653 ns/op   ~766 µs/run   ~1,300/s
n=10   2,476,807 ns/op   ~248 µs/run   ~4,000/s
n=100 24,137,270 ns/op   ~241 µs/run   ~4,100/s
n=1000 254,776,831 ns/op ~255 µs/run   ~3,900/s
```

Postgres is round-trip bound: two `database/sql` Execs per run (no multi-row
INSERT or `COPY`). Concurrent producers use the pool and scale
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
| Figure | ~46,000/s | ~1,800/s (Ephemeral, 64 workers) |

So River is roughly **20–25× higher** at completion throughput. The reasons are
structural, not a constant factor:

- **One completion transaction per run.** River completes jobs in bulk
  (`UPDATE … WHERE id = ANY`); quacker runs `CompleteStep` per run on SQLite's
  single writer. This is the dominant cost.
- **Batch insert via `COPY`.** River's `InsertManyFast` uses Postgres `COPY`;
  quacker issues one Exec per run for `runs` and one for `steps`.
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

## Reproduce

```sh
go test -run XXX -bench . -benchtime 2000x ./...
go test -run XXX -bench 'BenchmarkSaturatedThroughput' -benchtime 1x .
go test -run XXX -bench 'BenchmarkEnqueueParallel' -benchtime 2000x -cpu 1,4,10 .
go test -run XXX -bench 'BenchmarkCreateRunsBatch' -benchtime 300x ./internal/store/
go test -run XXX -bench 'BenchmarkWideDAGComplete' -benchtime 200x ./internal/store/

# Postgres producer throughput (needs a server):
QUACKER_TEST_POSTGRES_DSN=... \
  go test -run XXX -bench 'BenchmarkPostgresEnqueue' -benchtime 50x -cpu 1,4,10 ./postgres/
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
