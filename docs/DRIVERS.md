# Storage drivers

quacker persists through a **SQL-dialect driver**. Three ship in this module:

| Driver | Select with | Notes |
| --- | --- | --- |
| `sqlite` (built in) | `Memory()`, `Ephemeral()`, `File(path)` | Single process, no leases |
| `postgres` | `Postgres(dsn)` or `Driver("postgres", dsn)` | Multi-instance, pgx |
| `mysql` | `Driver("mysql", dsn)` | Multi-instance, go-sql-driver |

```go
import (
    "github.com/kartikbazzad/quacker"
    _ "github.com/kartikbazzad/quacker/postgres"
    _ "github.com/kartikbazzad/quacker/mysql"
)

q, _ := quacker.Open(quacker.WithStorage(quacker.Driver("mysql", dsn)))
```

Use `Storage.WithDB(*sql.DB)` (or `PostgresWithDB`) to hand quacker a pool you
already own; quacker runs its migrations on it but never closes it.

## What a driver is — and is not

A driver supplies **connection setup, placeholder syntax, DDL/migrations, and
the few SQL constructs that differ between databases**. It does *not*
reimplement quacker's transactional behavior. Claim key/rate/label gating, DAG
completion, atomic event delivery, step leases, and cron single-fire all live
in one query layer that drives every dialect through `driver.Backend`.

That is the whole point of the seam: a new SQL database is a translation, not
a re-implementation of the engine's correctness.

## The contract

`driver.Backend` (see the package docs and `postgres/` + `mysql/` as the
reference implementations):

| Method | Purpose |
| --- | --- |
| `Name()` | Registry key, e.g. `"mysql"` |
| `OpenPools` | Dial (or reuse `Config.DB`); return writer + reader pools and cleanup |
| `Rebind` | Rewrite `?` placeholders (`$n` for Postgres, identity otherwise) |
| `Migrations` | Ordered DDL, version numbers per-dialect |
| `MigrateLock` | Per-transaction migration lock (no-op or advisory) |
| `ClaimLock` | Serialize claim transactions across processes |
| `RunLock` | Lock a run row for DAG-mutating transactions |
| `SupportsLeases` / `SupportsCheckpoint` / `RecoverOnBoot` | Capability flags |
| `KeyGate` | Per-key concurrency predicate (see below) |
| `LabelGate` | Worker-label subset predicate |
| `BlockedDependentsSQL` | Direct dependents of a finished step |
| `UpsertSQL` | `ON CONFLICT` vs `ON DUPLICATE KEY` |

`driver.Config` carries `DSN`, an optional caller-owned `DB`, `Driver`, and the
built-in SQLite `Mode`/`Path`; other fields are informational to a custom
driver.

### Optional interfaces

- **`driver.MigrateLocker`** — for session-scoped migration locks. MySQL's
  `GET_LOCK` is released at session end, not transaction end, so the driver
  acquires it for the whole migration run and releases it explicitly.
  Drivers that don't implement it keep the per-transaction `MigrateLock`.
- **`driver.UniqueViolationer`** — reports whether an error is a unique
  constraint violation, so unique-job inserts can detect a collision
  (`driver.IsUniqueViolation` wraps it with a text-match fallback). SQLite
  matches the constraint message, Postgres SQLSTATE `23505`, MySQL error
  `1062`. A driver that omits it still works via the fallback.

## Portability rules

The shared query layer is written to run on SQLite, Postgres, and MySQL at
once. A driver must satisfy these:

- **64-bit integers.** Unix-nanosecond timestamps overflow 32-bit `INTEGER`;
  use `BIGINT`.
- **No reserved words in shared column names.** The durable journal's key
  column is `wkey`, not `key`, because `KEY` is reserved on MySQL.
- **`?` placeholders everywhere**, with `Rebind` translating if needed.
- **DDL must match the query layer's column names** (types are the driver's
  choice). `postgres/schema.go` and `mysql/schema.go` are the reference.
- **Self-referencing subqueries.** Postgres/SQLite accept the correlated
  `KeyGate` from `driver.CorrelatedKeyGate()`; MySQL does not (error 1093) and
  routes the count through a derived table that the optimizer merges back into
  a covering index lookup. If your dialect rejects reading the table being
  updated, wrap the source in a derived table the same way; keep it
  merge-friendly (no `GROUP BY`) so it does not materialize.
- **Migrations are split on `;`** and each statement runs separately, so a
  migration script must not contain a semicolon inside a string literal.

## Multi-instance

Drivers that report `SupportsLeases() == true` (Postgres, MySQL) must
implement cross-process serialization: `ClaimLock` for the counting claim
gates, `RunLock` for per-run DAG decisions, and an advisory/session lock for
migrations. With those, several engines safely share one database; crashed
workers are recovered by the lease reaper rather than boot recovery
(`RecoverOnBoot` returns false).

## Writing one

1. Implement `driver.Backend` in your own package.
2. Register it from `init`: `func init() { driver.RegisterBackend(myBackend{}) }`.
3. Users import your package for its side effect and select it with
   `quacker.Driver("<name>", dsn)`.

There is no separate conformance suite yet; `postgres/postgres_test.go` and
`mysql/mysql_test.go` are the template — task + workflow, durable wait/resume,
two engines sharing one database, label routing, purge, and `WithDB`.
