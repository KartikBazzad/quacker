package driver

import (
	"context"
	"database/sql"
	"strings"
	"testing"
)

func TestValidateMigrations(t *testing.T) {
	if err := ValidateMigrations([]Migration{{Version: 1}, {Version: 2}}); err != nil {
		t.Fatalf("ordered list rejected: %v", err)
	}
	for name, ms := range map[string][]Migration{
		"empty":      nil,
		"duplicate":  {{Version: 1}, {Version: 1}},
		"descending": {{Version: 2}, {Version: 1}},
	} {
		if err := ValidateMigrations(ms); err == nil {
			t.Fatalf("%s: expected error", name)
		}
	}
}

func TestOnConflictUpsert(t *testing.T) {
	got := OnConflictUpsert("crons",
		[]string{"id", "name", "spec"},
		[]string{"name"},
		[]string{"spec"})
	want := `INSERT INTO crons (id, name, spec) VALUES (?,?,?) ON CONFLICT (name) DO UPDATE SET spec=excluded.spec`
	if got != want {
		t.Fatalf("got  %q\nwant %q", got, want)
	}
}

func TestDuplicateKeyUpsert(t *testing.T) {
	got := DuplicateKeyUpsert("crons",
		[]string{"id", "name", "spec"},
		[]string{"spec"})
	if !strings.Contains(got, "ON DUPLICATE KEY UPDATE spec=VALUES(spec)") {
		t.Fatalf("got %q", got)
	}
}

type fakeBackend struct{}

func (fakeBackend) Name() string { return "fake-test" }
func (fakeBackend) OpenPools(ctx context.Context, cfg Config) (write, read *sql.DB, cleanup func() error, err error) {
	return nil, nil, nil, nil
}
func (fakeBackend) Rebind(q string) string                         { return q }
func (fakeBackend) Migrations() []Migration                        { return []Migration{{Version: 1, SQL: "SELECT 1"}} }
func (fakeBackend) MigrateLock(context.Context, *sql.Tx) error     { return nil }
func (fakeBackend) ClaimLock(context.Context, *sql.Tx) error       { return nil }
func (fakeBackend) RunLock(context.Context, *sql.Tx, string) error { return nil }
func (fakeBackend) SupportsLeases() bool                           { return false }
func (fakeBackend) SupportsCheckpoint(Config) bool                 { return false }
func (fakeBackend) RecoverOnBoot(Config) bool                      { return false }
func (fakeBackend) KeyGate() string                                { return CorrelatedKeyGate() }
func (fakeBackend) LabelGate() string                              { return "1=1" }
func (fakeBackend) BlockedDependentsSQL() string                   { return "SELECT 1" }
func (fakeBackend) UpsertSQL(table string, insertCols, conflictCols, updateCols []string) string {
	return OnConflictUpsert(table, insertCols, conflictCols, updateCols)
}

func TestRegisterAndLookup(t *testing.T) {
	RegisterBackend(fakeBackend{})
	b, ok := Lookup("fake-test")
	if !ok || b.Name() != "fake-test" {
		t.Fatalf("lookup = %v %v", b, ok)
	}
	if _, ok := Lookup("nope"); ok {
		t.Fatal("unexpected lookup hit")
	}
	var _ Backend = fakeBackend{}
}
