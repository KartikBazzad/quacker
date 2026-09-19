# Benchmarks

Numbers from the benchmark suite (`bench_test.go`, `internal/store/bench_test.go`).
All values are single-process, embedded SQLite — there is no network anywhere
in the loop.

## Results

Machine: Apple M-series (darwin/arm64), Go 1.26.5, `modernc.org/sqlite`
v1.59.0 (pure Go driver), storage `Memory()`.

```
BenchmarkEnqueueRun-10               171,704 ns/op   (~5,800 enqueues/s)
BenchmarkThroughput-10               186,430 ns/op   (~5,400 runs/s end-to-end*)
BenchmarkExecutionSnapshot-10         34,088 ns/op   (~29,000 snapshots/s**)
```

Numbers re-measured after the pre-release correctness fixes (statistically
unchanged from the initial run: enqueue 163→172µs, throughput 172→186µs,
snapshots 33.4→34.1µs — within run-to-run noise).

\* enqueue → execute on a worker → await `Result`.
\** while the engine is concurrently enqueuing and executing a 20,000-run
write load against the same database.

## Interpretation

- **End-to-end throughput ~5–6k runs/s** with JSON (de)serialization on both
  sides, full state persistence, and per-run transactions. The engine is far
  from the bottleneck; the JSON adapter and SQLite inserts dominate.
- **Snapshot reads cost ~33µs during sustained write load** — the
  "introspection never pauses execution" guarantee in numbers. Readers go
  through the WAL read pool and are untouched by the writer's transaction
  stream.
- Compare intent, not absolutes: Hatchet's published ~10k tasks/s figure is a
  distributed system over Postgres and HTTP; quacker trades distribution for
  being ~free to embed.

## Batching & parallelism (v0.3)

`EnqueueBatch` inserts N runs in one transaction, and `ClaimDueMulti` claims
across every queue in one transaction per scheduler tick (was one per queue).
The insert win, isolated at the store layer so background execution doesn't
pollute it (`BenchmarkCreateRunsBatch`, `ModeEphemeral`):

```
BenchmarkCreateRunsBatch/n=1-10      78,816 ns/op   (~79 µs/run)
BenchmarkCreateRunsBatch/n=10-10    356,588 ns/op   (~36 µs/run)
BenchmarkCreateRunsBatch/n=100-10  3,183,910 ns/op  (~32 µs/run)
```

A 100-run batch is ~2.5× cheaper per run than one run per transaction — the
fixed commit cost is amortized. The scheduler wakes once per batch.

Parallel producers do **not** scale linearly: SQLite allows one writer, so
`BenchmarkEnqueueParallel` shows per-op latency *rising* with `-cpu` as
producers serialize on the writer. That is the intended trade — embeddable
and dependency-free over write parallelism — and the reason `EnqueueBatch`
exists for fan-out.

## Reproduce

```sh
go test -run XXX -bench . -benchtime 3000x ./...
go test -run XXX -bench 'BenchmarkEnqueueParallel' -benchtime 3000x -cpu 1,4,10 .
go test -run XXX -bench 'BenchmarkCreateRunsBatch' -benchtime 3000x ./internal/store/
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
