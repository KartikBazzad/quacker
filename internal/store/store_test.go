package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestMigrateBaselinesLegacyShape: a v0.1 database — real tables but no
// schema_migrations — baselines to version 1 on Open rather than failing
// or re-running DDL destructively.
func TestMigrateBaselinesLegacyShape(t *testing.T) {
	p := filepath.Join(t.TempDir(), "legacy.db")
	db, err := sql.Open("sqlite", "file:"+p)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(schema); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	s, err := Open(Config{Mode: ModeFile, Path: p})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var v int
	if err := s.Read().QueryRow(
		`SELECT COALESCE(MAX(version),0) FROM schema_migrations`).Scan(&v); err != nil {
		t.Fatal(err)
	}
	if want := migrations[len(migrations)-1].version; v != want {
		t.Fatalf("baseline schema version = %d, want %d", v, want)
	}
}

// TestMigrationV2Columns: migration 2 adds the concurrency_key, key_limit,
// and claimed_at columns — and a v1-shaped row survives the upgrade with
// defaults intact, so it claims like any unkeyed step.
func TestMigrationV2Columns(t *testing.T) {
	p := filepath.Join(t.TempDir(), "v1.db")
	db, err := sql.Open("sqlite", "file:"+p)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(schema); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO runs
		(id, workflow, kind, status, queue, priority, input, output, error, attempts, max_attempts, run_at, created_at, started_at, completed_at)
		VALUES ('r1', 'w', 'task', 'QUEUED', 'q', 0, NULL, NULL, '', 0, 1, 0, 0, 0, 0)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO steps
		(id, run_id, name, task, ord, status, depends_on, queue, priority, input, output, error, attempts, max_attempts, timeout_ns, run_at, created_at, started_at, completed_at)
		VALUES ('r1/s', 'r1', 's', 't', 0, 'QUEUED', '', 'q', 0, NULL, NULL, '', 0, 1, 0, 0, 0, 0, 0)`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	s, err := Open(Config{Mode: ModeFile, Path: p})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	// The upgraded step carries the new columns (all defaults: unkeyed,
	// unlimited, never claimed) and claims normally.
	var key string
	var keyLimit, claimedAt int64
	if err := s.Read().QueryRow(
		`SELECT concurrency_key, key_limit, claimed_at FROM steps WHERE id='r1/s'`).
		Scan(&key, &keyLimit, &claimedAt); err != nil {
		t.Fatalf("select v2 columns: %v", err)
	}
	if key != "" || keyLimit != 0 || claimedAt != 0 {
		t.Fatalf("v2 defaults = (%q,%d,%d), want ('',0,0)", key, keyLimit, claimedAt)
	}
	claims, err := s.ClaimDue(context.Background(), "q", 10, 1, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(claims) != 1 || claims[0].Step.ID != "r1/s" {
		t.Fatalf("claims = %+v, want the legacy step", claims)
	}
}

// TestCheckpointLoopTicks: the periodic PASSIVE checkpoint actually
// fires — ckptCount increments once per tick, so a 10ms interval must
// produce at least one tick well inside 3s.
func TestCheckpointLoopTicks(t *testing.T) {
	s, err := Open(Config{Mode: ModeEphemeral, CheckpointInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	// Close even when the deadline check fails, so a timeout doesn't leak
	// the ephemeral store's temp files.
	defer s.Close()
	// Push writes so ticks have something to checkpoint and to prove the
	// loop isn't starved behind write traffic.
	for i := 0; i < 5; i++ {
		if _, err := s.Write().Exec(
			`INSERT INTO runs (id, workflow, kind, status, run_at, created_at)
			 VALUES (?, 'w', 'task', 'QUEUED', 0, 0)`, fmt.Sprintf("r%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(3 * time.Second)
	for s.ckptCount.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("checkpoint loop never ticked within 3s")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestFileDSNRejectsMetachars: a File path containing DSN metacharacters
// or control runes is rejected before MkdirAll — otherwise "a.db?x=1"
// would make SQLite open a different file than the .quacker.lock sidecar
// guards, or inject DSN pragmas. URI-decoding ("%2f"), a URI authority
// ("//host"), and a ":memory:" spelling are the same class of bug.
func TestFileDSNRejectsMetachars(t *testing.T) {
	for _, p := range []string{
		"a.db?journal_mode=DELETE",
		"a.db&_pragma=synchronous(0)",
		"a.db#fragment",
		"a\x01.db",
		"dir/a%2fb.db",
		"//localhost/dir/x.db",
		":memory:",
		"./:memory:",
	} {
		if _, err := Open(Config{Mode: ModeFile, Path: p}); err == nil ||
			!strings.Contains(err.Error(), "invalid path") {
			t.Fatalf("Open(%q) err = %v, want an invalid-path error", p, err)
		}
	}
}

// TestMigrationV3Index: migration 3 creates the retention index.
func TestMigrationV3Index(t *testing.T) {
	s, err := Open(Config{Mode: ModeEphemeral})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var n int
	if err := s.Read().QueryRow(
		`SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_runs_purge'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("idx_runs_purge count = %d, want 1", n)
	}
}

// TestMigrationV4Tables: migration 4 creates the events tables.
func TestMigrationV4Tables(t *testing.T) {
	s, err := Open(Config{Mode: ModeEphemeral})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, name := range []string{"events", "event_subscriptions"} {
		var n int
		if err := s.Read().QueryRow(
			`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, name).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Fatalf("table %s count = %d, want 1", name, n)
		}
	}
}

// TestMigrationV5Journal: migration 5 adds the durable-execution columns and
// the journal table.
func TestMigrationV5Journal(t *testing.T) {
	s, err := Open(Config{Mode: ModeEphemeral})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var n int
	if err := s.Read().QueryRow(
		`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='step_journal'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("step_journal table count = %d, want 1", n)
	}
}

// TestJournalRoundTripAndResumeClaim: the journal persists in call order, and
// a SUSPENDED step is claimed again by the resume arm without a new attempt.
func TestJournalRoundTripAndResumeClaim(t *testing.T) {
	s, err := Open(Config{Mode: ModeEphemeral})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	now := nowUnix()
	run := &Run{ID: "r", Workflow: "w", Kind: KindTask, Status: StatusQueued, Queue: "q", RunAt: now, CreatedAt: now, MaxAttempts: 1}
	step := &Step{ID: "r/s", RunID: "r", Name: "s", Task: "t", Ord: 0, Status: StatusQueued, Queue: "q", RunAt: now, CreatedAt: now, MaxAttempts: 1}
	if err := s.CreateRun(ctx, run, []*Step{step}); err != nil {
		t.Fatal(err)
	}
	claims, err := s.ClaimDue(ctx, "q", 1, now, 0, 0)
	if err != nil || len(claims) != 1 || claims[0].Resumed {
		t.Fatalf("first claim = %+v err=%v, want one fresh claim", claims, err)
	}

	if err := s.AppendJournal(ctx, &JournalEntry{StepID: "r/s", Idx: 0, Kind: JournalSleep, WakeAt: now + 1}); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendJournal(ctx, &JournalEntry{StepID: "r/s", Idx: 1, Kind: JournalOnce, Key: "k", Done: true, Result: []byte(`1`)}); err != nil {
		t.Fatal(err)
	}
	journal, err := s.LoadJournal(ctx, "r/s")
	if err != nil || len(journal) != 2 || journal[0].Kind != JournalSleep || journal[1].Kind != JournalOnce || !journal[1].Done {
		t.Fatalf("journal = %+v err=%v", journal, err)
	}

	// An event-only wait (resume_at=0) must not be claimed.
	if err := s.SuspendStep(ctx, "r/s", "wait", "go", 0, now); err != nil {
		t.Fatal(err)
	}
	if c, _ := s.ClaimDue(ctx, "q", 1, now, 0, 0); len(c) != 0 {
		t.Fatalf("event-only wait was claimed: %+v", c)
	}

	// A due resume is claimed, stamps claimed_at, and does not add an attempt.
	if _, err := s.Write().ExecContext(ctx, `UPDATE steps SET status=? WHERE id='r/s'`, StatusRunning); err != nil {
		t.Fatal(err)
	}
	if err := s.SuspendStep(ctx, "r/s", "sleep", "", now-1, now); err != nil {
		t.Fatal(err)
	}
	claims, err = s.ClaimDue(ctx, "q", 1, now, 0, 0)
	if err != nil || len(claims) != 1 {
		t.Fatalf("resume claim = %+v err=%v", claims, err)
	}
	if !claims[0].Resumed || claims[0].Step.Attempts != 1 || claims[0].Step.ClaimedAt != now {
		t.Fatalf("resumed claim = %+v, want resumed, attempts=1, claimed_at=now", claims[0])
	}
}

// TestDepsJSONRoundTrip: dependencies round-trip through a JSON array, so a
// step name containing the old comma delimiter survives.
func TestDepsJSONRoundTrip(t *testing.T) {
	for _, deps := range [][]string{
		nil,
		{},
		{"charge"},
		{"charge", "ship"},
		{"a,b", "c\"d"},
	} {
		got := splitDeps(joinDeps(deps))
		if len(got) != len(deps) {
			t.Fatalf("round trip %v -> %v", deps, got)
		}
		for i := range deps {
			if got[i] != deps[i] {
				t.Fatalf("round trip %v -> %v", deps, got)
			}
		}
	}
	// Legacy comma form still decodes.
	if got := splitDeps("a,b"); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("legacy decode = %v", got)
	}
}

// TestMigrationV7ConvertsCommaDeps: a v6 database with comma-joined
// depends_on is converted to JSON on Open.
func TestMigrationV7ConvertsCommaDeps(t *testing.T) {
	p := filepath.Join(t.TempDir(), "v6.db")
	db, err := sql.Open("sqlite", "file:"+p)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(schema); err != nil {
		t.Fatal(err)
	}
	for _, m := range []string{migration2, migration3, migration4, migration5, migration6} {
		if _, err := db.Exec(m); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, applied_at INTEGER NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	for v := 1; v <= 6; v++ {
		if _, err := db.Exec(`INSERT INTO schema_migrations (version, applied_at) VALUES (?, 0)`, v); err != nil {
			t.Fatal(err)
		}
	}
	now := nowUnix()
	if _, err := db.Exec(`INSERT INTO runs
		(id, workflow, kind, status, queue, priority, input, output, error, attempts, max_attempts, run_at, created_at, started_at, completed_at, concurrency_key, parent_id)
		VALUES ('r','w','task','QUEUED','q',0,NULL,NULL,'',0,1,?,?,0,0,'','')`, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO steps
		(id, run_id, name, task, ord, status, depends_on, queue, priority, input, output, error, attempts, max_attempts, timeout_ns, run_at, created_at, started_at, completed_at, concurrency_key, key_limit, claimed_at, resume_at, wait_kind, wait_event)
		VALUES ('r/s','r','s','t',0,'QUEUED','a,b','q',0,NULL,NULL,'',0,1,0,?,?,0,0,'',0,0,0,'','')`, now, now); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	s, err := Open(Config{Mode: ModeFile, Path: p})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var deps string
	if err := s.Read().QueryRow(`SELECT depends_on FROM steps WHERE id='r/s'`).Scan(&deps); err != nil {
		t.Fatal(err)
	}
	if deps != `["a","b"]` {
		t.Fatalf("depends_on = %q, want [\"a\",\"b\"]", deps)
	}
}

// TestMigrationV6ParentAndListChildren: migration 6 adds parent_id, and
// ListChildren/ListRuns(parent) find a run's children.
func TestMigrationV6ParentAndListChildren(t *testing.T) {
	s, err := Open(Config{Mode: ModeEphemeral})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	now := nowUnix()
	mk := func(id, parent string) {
		run := &Run{ID: id, Workflow: "w", Kind: KindTask, Status: StatusQueued, Queue: "q", RunAt: now, CreatedAt: now, MaxAttempts: 1, ParentID: parent}
		step := &Step{ID: id + "/s", RunID: id, Name: "s", Task: "t", Ord: 0, Status: StatusQueued, Queue: "q", RunAt: now, CreatedAt: now, MaxAttempts: 1}
		if err := s.CreateRun(ctx, run, []*Step{step}); err != nil {
			t.Fatal(err)
		}
	}
	mk("p", "")
	mk("c1", "p")
	mk("c2", "p")
	kids, err := s.ListChildren(ctx, "p")
	if err != nil || len(kids) != 2 {
		t.Fatalf("children = %+v err=%v, want 2", kids, err)
	}
	if kids[0].ParentID != "p" {
		t.Fatalf("child parent = %q, want p", kids[0].ParentID)
	}
	rs, err := s.ListRuns(ctx, Filter{ParentID: "p"})
	if err != nil || len(rs) != 2 {
		t.Fatalf("filtered runs = %+v err=%v, want 2", rs, err)
	}
}

// TestDeliverEventWakesWaitersAndRespectsTimeout: DeliverEvent wakes undone
// waits and sets them QUEUED, but never resurrects one already timed out.
func TestDeliverEventWakesWaitersAndRespectsTimeout(t *testing.T) {
	s, err := Open(Config{Mode: ModeEphemeral})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	now := nowUnix()
	mk := func(id string) {
		run := &Run{ID: "run-" + id, Workflow: "w", Kind: KindTask, Status: StatusQueued, Queue: "q", RunAt: now, CreatedAt: now, MaxAttempts: 1}
		step := &Step{ID: "run-" + id + "/s", RunID: "run-" + id, Name: "s", Task: "t", Ord: 0, Status: StatusQueued, Queue: "q", RunAt: now, CreatedAt: now, MaxAttempts: 1}
		if err := s.CreateRun(ctx, run, []*Step{step}); err != nil {
			t.Fatal(err)
		}
		if c, err := s.ClaimDue(ctx, "q", 1, now, 0, 0); err != nil || len(c) != 1 {
			t.Fatalf("claim %s: %+v err=%v", id, c, err)
		}
	}
	mk("a") // will be woken
	mk("b") // will have timed out
	if err := s.SuspendWithJournal(ctx, &JournalEntry{StepID: "run-a/s", Idx: 0, Kind: JournalWait, Event: "e"}, "wait", "e", 0, now); err != nil {
		t.Fatal(err)
	}
	if err := s.SuspendWithJournal(ctx, &JournalEntry{StepID: "run-b/s", Idx: 0, Kind: JournalWait, Event: "e", Deadline: now - 1}, "wait", "e", now-1, now); err != nil {
		t.Fatal(err)
	}
	if won, err := s.TimeoutWait(ctx, "run-b/s", 0); err != nil || !won {
		t.Fatalf("TimeoutWait = %v err=%v, want true", won, err)
	}
	n, err := s.DeliverEvent(ctx, "e", []byte(`"p"`), now)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("woken = %d, want 1 (the timed-out wait must not resurrect)", n)
	}
	var statusA, statusB string
	if err := s.Read().QueryRow(`SELECT status FROM steps WHERE id='run-a/s'`).Scan(&statusA); err != nil {
		t.Fatal(err)
	}
	if err := s.Read().QueryRow(`SELECT status FROM steps WHERE id='run-b/s'`).Scan(&statusB); err != nil {
		t.Fatal(err)
	}
	if statusA != StatusQueued || statusB != StatusSuspended {
		t.Fatalf("statuses = %q/%q, want QUEUED/SUSPENDED", statusA, statusB)
	}
}

// TestPurgeRejectsBadOptions: a non-terminal status or a zero cutoff is
// refused, so a purge can never touch live work by accident.
func TestPurgeRejectsBadOptions(t *testing.T) {
	s, err := Open(Config{Mode: ModeEphemeral})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	if _, err := s.PurgeRuns(ctx, PurgeOptions{Before: 0}); err == nil {
		t.Fatal("expected error for Before <= 0")
	}
	if _, err := s.PurgeRuns(ctx, PurgeOptions{Before: 1, Statuses: []string{StatusRunning}}); !errors.Is(err, ErrNonTerminalPurge) {
		t.Fatalf("err = %v, want ErrNonTerminalPurge", err)
	}
}

// TestPurgeSkipsRunWithRunningStep: a terminal run whose step is still
// RUNNING is not eligible (the cancel-window guard); once the step settles it
// is.
func TestPurgeSkipsRunWithRunningStep(t *testing.T) {
	s, err := Open(Config{Mode: ModeEphemeral})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	now := nowUnix()
	if _, err := s.Write().ExecContext(ctx, `INSERT INTO runs
		(id, workflow, kind, status, queue, run_at, created_at, completed_at)
		VALUES ('r1','w','task','CANCELLED','q',?,?,?)`, now, now, now-1_000_000); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write().ExecContext(ctx, `INSERT INTO steps
		(id, run_id, name, task, ord, status, depends_on, queue, run_at, created_at)
		VALUES ('r1/s','r1','s','t',0,'RUNNING','','q',?,?)`, now, now); err != nil {
		t.Fatal(err)
	}
	res, err := s.PurgeRuns(ctx, PurgeOptions{Before: now})
	if err != nil {
		t.Fatal(err)
	}
	if res.Runs != 0 {
		t.Fatalf("purged %d runs with a RUNNING step, want 0", res.Runs)
	}
	if _, err := s.Write().ExecContext(ctx,
		`UPDATE steps SET status='CANCELLED' WHERE id='r1/s'`); err != nil {
		t.Fatal(err)
	}
	res, err = s.PurgeRuns(ctx, PurgeOptions{Before: now})
	if err != nil {
		t.Fatal(err)
	}
	if res.Runs != 1 || res.Steps != 1 {
		t.Fatalf("purge after settle = %+v, want 1 run 1 step", res)
	}
}
