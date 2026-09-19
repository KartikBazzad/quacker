# Benchmarks

Numbers from the v0.1 benchmark suite (`bench_test.go`). All values are
single-process, embedded SQLite — there is no network anywhere in the loop.

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

## Reproduce

```sh
go test -run XXX -bench . -benchtime 3000x ./...
```

Drop `-benchtime` for quicker runs; raise it for stable numbers. Run with
`-benchmem` to see allocation counts. Storage mode can be swapped in
`bench_test.go` (`Memory()` → `Ephemeral()` → `File(path)`) to compare
backends; `File` on an SSD should be close to `Ephemeral` since
`synchronous=NORMAL` only fsyncs on WAL checkpoints.

## What's not yet measured (follow-up candidates)

- Parallel benchmark (`-cpu 1,4,10`) — scheduler contention above 1 producer.
- `Ephemeral` vs `Memory` delta (expect: memory slightly faster per write,
  at the cost of reader/writer lock contention).
- Step fan-out cost for wide DAGs (claim loop batches per queue).
