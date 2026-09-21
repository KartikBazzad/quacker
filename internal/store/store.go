// Package store owns all persistence for quacker.
//
// The SQLite default keeps two connection pools over one database: a single
// writer connection (SQLite permits only one writer at a time) and a pool of
// read-only connections. In WAL mode readers never block the writer, so
// introspection queries (Execution, Runs, Metrics, Logs) can run at any time
// without stalling task execution.
//
// The query layer is dialect-neutral (it uses '?' placeholders); a registered
// Backend supplies the connection setup, placeholder rebinding, DDL, and the
// few dialect-only operations. SQLite is built in; Postgres is available by
// importing its driver package.
package store

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kartikbazzad/quacker/driver"
)

const (
	// defaultCheckpointInterval bounds WAL growth when CheckpointInterval
	// is unset.
	defaultCheckpointInterval = 60 * time.Second
	// minCheckpointInterval keeps a hot-ticker configuration from turning
	// the checkpoint loop into a spin; smaller values are clamped up.
	minCheckpointInterval = 10 * time.Millisecond
)

// Store wraps a database. It is safe for concurrent use.
type Store struct {
	be       driver.Backend
	keyGate  string  // backend's per-key concurrency predicate, built once
	seqGate  string  // backend's per-sequence ordering predicate, built once
	keysGate string  // backend's extra-keys predicate, built once
	write    *dbConn // all mutations go through here
	read     *dbConn // introspection queries
	cleanup  func() error
	// ckptCancel/ckptWG drive the WAL checkpoint loop (SQLite WAL modes only).
	ckptCancel context.CancelFunc
	ckptWG     sync.WaitGroup
	// ckptCount ticks once per checkpoint attempt; tests read it to prove
	// the loop runs without waiting a full interval.
	ckptCount atomic.Int64
	// claimTxs counts claim transactions opened (diagnostic: the scheduler
	// should stop opening them when nothing is due).
	claimTxs atomic.Int64
	closed   atomic.Bool
}

// Open opens (and migrates) the database described by cfg.
func Open(cfg driver.Config) (*Store, error) {
	be, err := backendFor(cfg)
	if err != nil {
		return nil, err
	}
	if err := driver.ValidateMigrations(be.Migrations()); err != nil {
		return nil, fmt.Errorf("quacker: driver %q: %w", be.Name(), err)
	}
	ctx := context.Background()
	write, read, cleanup, err := be.OpenPools(ctx, cfg)
	if err != nil {
		return nil, err
	}
	s := &Store{
		be:       be,
		keyGate:  be.KeyGate(),
		seqGate:  be.SequenceGate(),
		keysGate: be.KeysGate(),
		write:    &dbConn{DB: write, be: be},
		read:     &dbConn{DB: read, be: be},
		cleanup:  cleanup,
	}
	if err := s.migrate(ctx); err != nil {
		s.Close()
		return nil, err
	}
	if be.RecoverOnBoot(cfg) {
		if err := s.recoverInterrupted(ctx, cfg.RecoverRunningOnBoot); err != nil {
			s.Close()
			return nil, err
		}
	}
	if be.SupportsCheckpoint(cfg) {
		interval := cfg.CheckpointInterval
		if interval <= 0 {
			interval = defaultCheckpointInterval
		}
		if interval < minCheckpointInterval {
			interval = minCheckpointInterval
		}
		cc, cancel := context.WithCancel(context.Background())
		s.ckptCancel = cancel
		s.ckptWG.Add(1)
		go s.checkpointLoop(cc, interval)
	}
	return s, nil
}

// Write returns the writer pool. All mutations must use it.
func (s *Store) Write() *dbConn { return s.write }

// Read returns the read-only pool. Introspection queries must use it; in WAL
// mode its queries never block (or get blocked by) the write path.
func (s *Store) Read() *dbConn { return s.read }

// beginTx opens a write transaction whose statements are rebound for the
// dialect.
func (s *Store) beginTx(ctx context.Context) (*txn, error) {
	t, err := s.write.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &txn{Tx: t, be: s.be}, nil
}

// SupportsLeases reports whether the backend is multi-instance and uses step
// leases (Postgres), as opposed to single-process SQLite.
func (s *Store) SupportsLeases() bool { return s.be.SupportsLeases() }

// SupportsExternalTx reports whether the backend can enqueue on a caller-owned
// *sql.Tx. SQLite cannot: quacker owns its single writer connection and the
// file lock, so a caller transaction on the same file is not safe.
func (s *Store) SupportsExternalTx() bool { return s.be.Name() != "sqlite" }

// InterruptAll marks in-flight work INTERRUPTED (shutdown sweep). With an
// empty workerID (single-process SQLite) every RUNNING row is swept. With a
// workerID (leases mode) only that worker's RUNNING steps are swept, and the
// runs they leave with no active step are converged to INTERRUPTED — a run
// another worker is still executing is left alone.
func (s *Store) InterruptAll(ctx context.Context, workerID string, now int64) error {
	if workerID == "" {
		tx, err := s.beginTx(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		// Ephemeral runs leave no history: delete those in flight rather than
		// marking them INTERRUPTED.
		if rows, err := tx.query(ctx, `SELECT id FROM runs WHERE status=? AND ephemeral=1`, StatusRunning); err != nil {
			return err
		} else {
			var ids []string
			for rows.Next() {
				var id string
				if err := rows.Scan(&id); err != nil {
					rows.Close()
					return err
				}
				ids = append(ids, id)
			}
			rows.Close()
			if err := rows.Err(); err != nil {
				return err
			}
			for _, id := range ids {
				if err := deleteEphemeralRunTx(ctx, tx, id); err != nil {
					return err
				}
			}
		}
		if _, err := tx.exec(ctx, `UPDATE steps SET status=?, completed_at=? WHERE status=?`,
			StatusInterrupted, now, StatusRunning); err != nil {
			return err
		}
		if _, err := tx.exec(ctx, `UPDATE runs SET status=?, completed_at=?, unique_key=NULL WHERE status=?`,
			StatusInterrupted, now, StatusRunning); err != nil {
			return err
		}
		return tx.Commit()
	}

	tx, err := s.beginTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	rows, err := tx.query(ctx, `SELECT DISTINCT s.run_id, r.ephemeral
		FROM steps s JOIN runs r ON r.id = s.run_id
		WHERE s.status=? AND s.worker_id=?`, StatusRunning, workerID)
	if err != nil {
		return err
	}
	var runIDs []string
	for rows.Next() {
		var id string
		var ephemeral int64
		if err := rows.Scan(&id, &ephemeral); err != nil {
			rows.Close()
			return err
		}
		if ephemeral != 0 {
			if err := deleteEphemeralRunTx(ctx, tx, id); err != nil {
				rows.Close()
				return err
			}
			continue
		}
		runIDs = append(runIDs, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	if _, err := tx.exec(ctx, `UPDATE steps SET status=?, completed_at=?, worker_id='', lease_expires_at=0
		WHERE status=? AND worker_id=?`, StatusInterrupted, now, StatusRunning, workerID); err != nil {
		return err
	}
	if len(runIDs) > 0 {
		args := []any{StatusInterrupted, now, StatusRunning}
		args = append(args, argsAny(runIDs)...)
		if _, err := tx.exec(ctx, `UPDATE runs SET status=?, completed_at=?, unique_key=NULL WHERE status=? AND id IN (`+
			placeholders(len(runIDs))+`) AND NOT EXISTS (
				SELECT 1 FROM steps s WHERE s.run_id=runs.id AND s.status IN ('QUEUED','RUNNING','BLOCKED','SUSPENDED'))`,
			args...); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// HeartbeatWorker extends the lease of every RUNNING step owned by workerID.
func (s *Store) HeartbeatWorker(ctx context.Context, workerID string, leaseUntil int64) error {
	_, err := s.write.ExecContext(ctx,
		`UPDATE steps SET lease_expires_at=? WHERE worker_id=? AND status=?`,
		leaseUntil, workerID, StatusRunning)
	return err
}

// ReapExpired re-queues up to limit RUNNING steps whose lease has expired (a
// crashed worker), returning how many were requeued. Leaderless: the
// status='RUNNING' guard means at most one reaper wins each row.
func (s *Store) ReapExpired(ctx context.Context, now int64, limit int) (int64, error) {
	if limit <= 0 {
		limit = 100
	}
	tx, err := s.beginTx(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	// The extra SELECT layer is required by MySQL, which rejects LIMIT in a
	// subquery of the same table being modified (error 1093); a derived table
	// is materialized first. It is valid on every dialect.
	res, err := tx.exec(ctx, `UPDATE steps SET
		status=?, run_at=?, worker_id='', lease_expires_at=0
		WHERE id IN (
			SELECT id FROM (
				SELECT s.id AS id FROM steps s JOIN runs r ON r.id=s.run_id
				WHERE s.status=? AND s.lease_expires_at > 0 AND s.lease_expires_at < ?
				  AND r.status NOT IN ('SUCCEEDED','FAILED','CANCELLED','INTERRUPTED')
				LIMIT ?) AS reap)`,
		StatusQueued, now, StatusRunning, now, limit)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, tx.Commit()
}

// checkpointLoop runs a PASSIVE wal_checkpoint on the writer every d.
// PASSIVE never waits on the writer or readers — it checkpoints whatever
// is safely checkpointable and returns — so the timer can sit on the hot
// path and still cap WAL growth whenever readers are momentarily idle.
func (s *Store) checkpointLoop(ctx context.Context, d time.Duration) {
	defer s.ckptWG.Done()
	t := time.NewTicker(d)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			// ExecContext lets Close interrupt an Exec queued behind a
			// long write tx on the single-conn pool.
			_, _ = s.write.ExecContext(ctx, "PRAGMA wal_checkpoint(PASSIVE)")
			s.ckptCount.Add(1)
		}
	}
}

// Close stops the checkpoint loop and releases the pools (and, for SQLite,
// the file lock and any ephemeral file).
func (s *Store) Close() error {
	if s == nil || s.closed.Swap(true) {
		return nil
	}
	if s.ckptCancel != nil {
		s.ckptCancel()
		s.ckptWG.Wait()
	}
	if s.cleanup == nil {
		return nil
	}
	return s.cleanup()
}
