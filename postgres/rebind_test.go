package postgres

import "testing"

func TestRebind(t *testing.T) {
	cases := []struct{ in, want string }{
		{"SELECT 1", "SELECT 1"},
		{"SELECT * FROM t WHERE a=? AND b=?", "SELECT * FROM t WHERE a=$1 AND b=$2"},
		{"UPDATE t SET x=? WHERE id=? AND status=?", "UPDATE t SET x=$1 WHERE id=$2 AND status=$3"},
		{"SELECT 'a?b', ? FROM t", "SELECT 'a?b', $1 FROM t"},
		{"SELECT 'it''s ?', ?", "SELECT 'it''s ?', $1"},
		{`SELECT "we?ird", ? FROM t`, `SELECT "we?ird", $1 FROM t`},
		{"SELECT ? -- trailing ? comment\nWHERE x=?", "SELECT $1 -- trailing ? comment\nWHERE x=$2"},
		{"SELECT ? FROM t WHERE n IN (?,?,?)", "SELECT $1 FROM t WHERE n IN ($2,$3,$4)"},
	}
	for _, c := range cases {
		if got := rebind(c.in); got != c.want {
			t.Errorf("rebind(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}
