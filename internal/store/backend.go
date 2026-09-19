package store

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/kartikbazzad/quacker/driver"
)

// backendFor selects the driver for cfg: an explicit Config.Driver wins,
// otherwise the built-in SQLite modes map to "sqlite".
func backendFor(cfg driver.Config) (driver.Backend, error) {
	name := cfg.Driver
	if name == "" {
		name = "sqlite"
	}
	b, ok := driver.Lookup(name)
	if !ok {
		return nil, fmt.Errorf("quacker: storage driver %q is not registered; import its driver package (e.g. _ %q)",
			name, "github.com/kartikbazzad/quacker/"+name)
	}
	return b, nil
}

// dbConn is a *sql.DB whose statements are rebound for the dialect, so the
// query layer keeps using '?' placeholders everywhere.
type dbConn struct {
	*sql.DB
	be driver.Backend
}

func (c *dbConn) ExecContext(ctx context.Context, q string, args ...any) (sql.Result, error) {
	return c.DB.ExecContext(ctx, c.be.Rebind(q), args...)
}

func (c *dbConn) QueryContext(ctx context.Context, q string, args ...any) (*sql.Rows, error) {
	return c.DB.QueryContext(ctx, c.be.Rebind(q), args...)
}

func (c *dbConn) QueryRowContext(ctx context.Context, q string, args ...any) *sql.Row {
	return c.DB.QueryRowContext(ctx, c.be.Rebind(q), args...)
}

// txn is a *sql.Tx whose statements are rebound for the dialect.
type txn struct {
	*sql.Tx
	be driver.Backend
}

func (t *txn) exec(ctx context.Context, q string, args ...any) (sql.Result, error) {
	return t.Tx.ExecContext(ctx, t.be.Rebind(q), args...)
}

func (t *txn) query(ctx context.Context, q string, args ...any) (*sql.Rows, error) {
	return t.Tx.QueryContext(ctx, t.be.Rebind(q), args...)
}

func (t *txn) queryRow(ctx context.Context, q string, args ...any) *sql.Row {
	return t.Tx.QueryRowContext(ctx, t.be.Rebind(q), args...)
}
