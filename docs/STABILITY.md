# Stability & versioning

What quacker promises about its API, its storage format, and its releases.

## Versioning

quacker follows [Semantic Versioning](https://semver.org): `MAJOR.MINOR.PATCH`.

- **Before a `v1.0` tag**, the public API may change between minor versions;
  changes are called out in the roadmap and design notes.
- **From `v1.0`**, a breaking change to the public API requires a major bump;
  additive, backward-compatible changes are minor; fixes are patch.

No git remote or release tags are configured yet, so nothing is published to a
module proxy — the commit history is the record of record. Tags will start at
`v1.0.0` once a remote exists and the API is frozen.

## Public API surface

The stable surface is the **exported identifiers of `package quacker` and the
`postgres` driver package**. Everything under `internal/` (`internal/store`,
`internal/engine`, `internal/bus`) is implementation detail and may change at
any time.

- New options, hooks, fields, and methods are additive and non-breaking.
- Removing or changing the signature of an exported identifier is breaking.
- When something is superseded it is first deprecated in a doc comment before
  removal in the next major.

## Storage format

- **Schema versions** are tracked in the `schema_migrations` table. Each
  version applies in its own transaction, and a database written by a newer
  build is refused rather than run through older assumptions (`Open` fails with
  a clear "schema version is newer" error).
- **Task names are part of the storage format.** Renaming a task orphans its
  in-flight runs; this is documented on `NewTask`.
- **A payload codec must be consistent across restarts** for a given database
  (durable journal values and cached dependency outputs are decoded by the
  process that encoded them). Schema structures (`depends_on`, `labels`) are
  always JSON regardless of codec.
- Postgres keeps its own migration list; version numbers are per-dialect.

Because storage state is the contract with future versions, it changes more
conservatively than the Go API.

## Extension points

These are stable interfaces intended for third-party use:

- **Middleware** (`Handler`, `Middleware`, `WithMiddleware`, `Wrap`).
- **Plugins / lifecycle hooks** (`Plugin`, `Hooks`, `WithPlugin`).
- **Payload codec** (`Codec`, `WithCodec`).
- **Observability**: `WithTaskLogSink`/`WithLogSink`, `WithMetricsFunc`,
  `WithTracerProvider`.
- **Storage drivers** (`driver.Backend`, `driver.RegisterBackend`,
  `quacker.Driver`). A driver is a SQL dialect — it translates connection
  setup, placeholders, DDL, and a handful of dialect predicates, but never
  owns transactional behavior. SQLite, Postgres, and MySQL/MariaDB ship in this
  module; see [DRIVERS.md](DRIVERS.md) for the contract and portability rules.

Plugins are compile-time and trusted in-process code — there is no sandbox.

## Verification guarantees

- `go test ./...` and `go test -race ./...` (SQLite) on Linux and macOS in CI.
- Postgres and MySQL integration tests against real servers in CI.
- Fuzzing for the DAG validator and the cron parser (`internal/engine`).
- A File-mode chaos suite that SIGKILLs a child mid-execution and asserts
  at-least-once recovery (no lost terminal states).
