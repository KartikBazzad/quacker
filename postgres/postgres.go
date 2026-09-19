// Package postgres registers the PostgreSQL storage backend for quacker.
//
// Import it for its side effect before opening Postgres storage:
//
//	import (
//	    "github.com/kartikbazzad/quacker"
//	    _ "github.com/kartikbazzad/quacker/postgres"
//	)
//
//	q, err := quacker.Open(quacker.WithStorage(quacker.Postgres(dsn)))
//
// The driver is single-instance for now: boot recovery and graceful shutdown
// assume one engine owns the database. Multi-instance (step leases, cross-node
// claim locking) is a later phase.
package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver

	"github.com/kartikbazzad/quacker/internal/store"
)

func init() { store.RegisterBackend(pgBackend{}) }

type pgBackend struct{}

func (pgBackend) Name() string { return "postgres" }

func (pgBackend) Rebind(q string) string {
	if !strings.ContainsRune(q, '?') {
		return q
	}
	if v, ok := rebindCache.Load(q); ok {
		return v.(string)
	}
	out := rebind(q)
	rebindCache.Store(q, out)
	return out
}

// rebindCache memoizes rebindings; the query set is small and fixed, so the
// cache stays bounded.
var rebindCache sync.Map

func (pgBackend) Migrations() []store.Migration {
	return []store.Migration{
		{Version: 1, SQL: pgSchema},
		{Version: 2, SQL: pgMigration2},
		{Version: 3, SQL: pgMigration3},
	}
}

// Advisory-lock keys; arbitrary but stable constants.
const (
	migrateLockKey = 0x71756163_6b657200 // migrations
	claimLockKey   = 0x71756163_6b657201 // claim batches
)

// MigrateLock takes a transaction-scoped advisory lock so two nodes booting at
// once serialize migrations instead of racing DDL. Released when the tx ends.
func (pgBackend) MigrateLock(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, int64(migrateLockKey))
	return err
}

// ClaimLock serializes claim transactions cluster-wide, so the per-key and
// rate gates (which count rows) are evaluated one batch at a time exactly as
// they are under SQLite's single writer.
func (pgBackend) ClaimLock(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, int64(claimLockKey))
	return err
}

// RunLock locks a run row so concurrent step completions of the same run
// serialize their terminal decision.
func (pgBackend) RunLock(ctx context.Context, tx *sql.Tx, runID string) error {
	_, err := tx.ExecContext(ctx, `SELECT 1 FROM runs WHERE id=$1 FOR UPDATE`, runID)
	return err
}

// SupportsLeases is true for Postgres: several engines share the database, so
// worker identity + leases are how crashed nodes' work is recovered.
func (pgBackend) SupportsLeases() bool { return true }

func (pgBackend) SupportsCheckpoint(store.Config) bool { return false }

// RecoverOnBoot is false: boot recovery that re-queues every RUNNING row would
// steal a peer node's in-flight work. The lease reaper recovers a crashed
// node's steps instead.
func (pgBackend) RecoverOnBoot(store.Config) bool { return false }

// LabelGate: labels are stored as JSON text; cast to jsonb and test subset
// membership with jsonb_array_elements_text.
func (pgBackend) LabelGate() string {
	return `NOT EXISTS (
	SELECT 1 FROM jsonb_array_elements_text(steps.labels::jsonb) AS l(value)
	WHERE l.value NOT IN (SELECT value FROM jsonb_array_elements_text(?::jsonb))
)`
}

func (pgBackend) BlockedDependentsSQL() string {
	return `SELECT id, name, depends_on FROM steps
		WHERE run_id=? AND status=?
		  AND EXISTS (SELECT 1 FROM jsonb_array_elements_text(steps.depends_on::jsonb) AS d(value) WHERE d.value=?)`
}

func (pgBackend) OpenPools(ctx context.Context, cfg store.Config) (w, r *sql.DB, cleanup func() error, err error) {
	// Reuse a caller-owned pool: Postgres uses one pool for both reads and
	// writes, so it can be shared directly. Migrations still run against it,
	// but cleanup must leave it open for the caller.
	if cfg.DB != nil {
		if err := cfg.DB.PingContext(ctx); err != nil {
			return nil, nil, nil, fmt.Errorf("quacker: external postgres pool: %w", err)
		}
		return cfg.DB, cfg.DB, nil, nil
	}
	if cfg.DSN == "" {
		return nil, nil, nil, fmt.Errorf("quacker: storage.Postgres requires a DSN or an *sql.DB")
	}
	db, err := sql.Open("pgx", cfg.DSN)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("quacker: open postgres: %w", err)
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, nil, nil, fmt.Errorf("quacker: ping postgres: %w", err)
	}
	// Postgres MVCC needs no query_only reader pool; one pool serves both.
	return db, db, db.Close, nil
}

// rebind rewrites '?' placeholders to Postgres' '$n', skipping question marks
// inside single-quoted string literals, double-quoted identifiers, and line
// comments.
func rebind(q string) string {
	var b strings.Builder
	b.Grow(len(q) + 8)
	n := 0
	for i := 0; i < len(q); i++ {
		c := q[i]
		switch c {
		case '\'':
			// Single-quoted literal; '' escapes a quote.
			b.WriteByte(c)
			for i++; i < len(q); i++ {
				b.WriteByte(q[i])
				if q[i] == '\'' {
					if i+1 < len(q) && q[i+1] == '\'' {
						b.WriteByte(q[i+1])
						i++
						continue
					}
					break
				}
			}
		case '"':
			b.WriteByte(c)
			for i++; i < len(q); i++ {
				b.WriteByte(q[i])
				if q[i] == '"' {
					break
				}
			}
		case '-':
			if i+1 < len(q) && q[i+1] == '-' {
				for i < len(q) && q[i] != '\n' {
					b.WriteByte(q[i])
					i++
				}
				if i < len(q) {
					b.WriteByte('\n')
				}
			} else {
				b.WriteByte(c)
			}
		case '?':
			n++
			b.WriteByte('$')
			b.WriteString(itoa(n))
		default:
			b.WriteByte(c)
		}
	}
	return b.String()
}

func itoa(n int) string {
	if n < 10 {
		return string(rune('0' + n))
	}
	var buf [8]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
