package store

import (
	"database/sql"
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
	if v != 1 {
		t.Fatalf("baseline schema version = %d, want 1", v)
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
