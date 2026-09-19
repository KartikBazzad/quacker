package engine

import (
	"strings"
	"testing"
	"time"
)

// FuzzParseCron checks that no spec panics the parser and that a parsed
// "@every" schedule always advances time (the bug class fixed by the custom
// fixed-delay schedule).
func FuzzParseCron(f *testing.F) {
	for _, s := range []string{
		"@every 1s", "@every 250ms", "@every 1h30m", "@daily", "@midnight",
		"0 9 * * 1-5", "*/5 * * * *", "@every 0s", "@every -1s", "@every bogus",
		"", "@every ", "@yearly", "60 * * * *", "* * * * * *",
	} {
		f.Add(s)
	}
	now := time.Unix(1_700_000_000, 0)
	f.Fuzz(func(t *testing.T, spec string) {
		sched, err := parseCron(spec)
		if err != nil {
			return
		}
		next := sched.s.Next(now)
		if strings.HasPrefix(spec, "@every ") {
			// A valid @every is strictly positive, so next must be after now.
			if !next.After(now) {
				t.Fatalf("parseCron(%q).Next = %v, want after %v", spec, next, now)
			}
		}
		// Any other parsed spec must also produce a next time without panic.
		_ = sched.s.Next(next)
	})
}

// FuzzValidateDAG checks the DAG validator never panics or hangs on arbitrary
// names/dependencies (including cycles and duplicates).
func FuzzValidateDAG(f *testing.F) {
	f.Add("a,b,c", "a,b")
	f.Add("a", "")
	f.Add("a,b", "b,a")
	f.Add("", "")
	f.Add("a,a", "a")
	f.Add("a,b,c", "c")
	f.Fuzz(func(t *testing.T, namesCSV, depsCSV string) {
		names := splitCSV(namesCSV)
		if len(names) == 0 {
			return
		}
		deps := splitCSV(depsCSV)
		steps := make([]StepReq, len(names))
		for i, n := range names {
			steps[i] = StepReq{Name: n, Deps: deps}
		}
		_ = validateDAG(steps) // must return, never panic or hang
	})
}

func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(s, ",")
}
