package quacker

import (
	"context"
	"strings"
	"testing"
	"time"
)

func containsAll(xs []string, want ...string) bool {
	set := map[string]bool{}
	for _, x := range xs {
		set[x] = true
	}
	for _, w := range want {
		if !set[w] {
			return false
		}
	}
	return true
}

func hasEdge(d *DAG, from, to string) bool {
	for _, e := range d.Edges {
		if e.From == from && e.To == to {
			return true
		}
	}
	return false
}

// TestGroupWorkflow: group deps gate a whole group and expand to member steps;
// the DAG shows group boxes and group-to-group edges instead of the expanded
// cross-group step mesh, while intra-group step edges remain.
func TestGroupWorkflow(t *testing.T) {
	q := newTestQ(t, WithQueue("default", 16))
	ctx := context.Background()

	extract := NewGroup[greetIn]("extract",
		Step("e1", dullTask("e1-fn")),
		Step("e2", dullTask("e2-fn")),
	)
	transform := NewGroup[greetIn]("transform",
		Step("t1", dullTask("t1-fn")),
		Step("t2", dullTask("t2-fn"), "t1"), // intra-group dependency
	).After("extract")
	load := NewGroup[greetIn]("load",
		Step("l1", dullTask("l1-fn")),
	).After("transform")

	wf := NewGroupWorkflow[greetIn]("grouped", extract, transform, load)
	h, err := EnqueueWorkflow[greetOut](ctx, q, wf, greetIn{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Result(ctx); err != nil {
		t.Fatal(err)
	}

	ex, err := q.Execution(ctx, h.RunID())
	if err != nil {
		t.Fatal(err)
	}
	if len(ex.Groups) != 3 {
		t.Fatalf("groups = %d, want 3 (%+v)", len(ex.Groups), ex.Groups)
	}
	wantDeps := map[string][]string{"extract": nil, "transform": {"extract"}, "load": {"transform"}}
	for _, g := range ex.Groups {
		got := g.Deps
		want := wantDeps[g.Name]
		if len(got) != len(want) || (len(want) == 1 && got[0] != want[0]) {
			t.Fatalf("group %q deps = %v, want %v", g.Name, got, want)
		}
	}

	// Group deps are expanded onto every member step at enqueue.
	deps := map[string][]string{}
	started, ended := map[string]time.Time{}, map[string]time.Time{}
	for _, s := range ex.Steps {
		deps[s.Name] = s.Deps
		started[s.Name], ended[s.Name] = s.StartedAt, s.CompletedAt
	}
	if !containsAll(deps["t1"], "e1", "e2") {
		t.Fatalf("t1 deps = %v, want to include e1,e2", deps["t1"])
	}
	if !containsAll(deps["t2"], "t1", "e1", "e2") {
		t.Fatalf("t2 deps = %v, want t1 + extract members", deps["t2"])
	}
	if !containsAll(deps["l1"], "t1", "t2") {
		t.Fatalf("l1 deps = %v, want transform members", deps["l1"])
	}

	// The whole transform group starts only after the extract group finished.
	var extractEnd, transformStart time.Time
	for _, n := range []string{"e1", "e2"} {
		if ended[n].After(extractEnd) {
			extractEnd = ended[n]
		}
	}
	for _, n := range []string{"t1", "t2"} {
		if transformStart.IsZero() || started[n].Before(transformStart) {
			transformStart = started[n]
		}
	}
	if transformStart.Before(extractEnd) {
		t.Fatalf("transform started %v before extract ended %v", transformStart, extractEnd)
	}

	// The DAG view: member nodes carry their group, group deps are group
	// edges, the intra-group edge survives, and the expanded cross-group step
	// edges are hidden.
	d, err := q.DAG(ctx, h.RunID())
	if err != nil {
		t.Fatal(err)
	}
	groupOf := map[string]string{}
	for _, n := range d.Nodes {
		groupOf[n.Name] = n.Group
	}
	if groupOf["t1"] != "transform" || groupOf["l1"] != "load" {
		t.Fatalf("node groups = %v", groupOf)
	}
	if len(d.Groups) != 3 {
		t.Fatalf("DAG groups = %d, want 3", len(d.Groups))
	}
	if !hasEdge(d, "extract", "transform") || !hasEdge(d, "transform", "load") {
		t.Fatalf("missing group edges (edges: %+v)", d.Edges)
	}
	if !hasEdge(d, "t1", "t2") {
		t.Fatalf("missing intra-group edge t1→t2 (edges: %+v)", d.Edges)
	}
	if hasEdge(d, "e1", "t1") || hasEdge(d, "t1", "l1") {
		t.Fatalf("cross-group step edges should be hidden (edges: %+v)", d.Edges)
	}

	// The SVG renders the group boxes and group edges.
	svg, err := q.DAGSVG(ctx, h.RunID())
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"extract", "transform", "load", "t1", "t2"} {
		if !strings.Contains(string(svg), want) {
			t.Fatalf("svg missing %q", want)
		}
	}
}

// TestGroupWorkflowUnknownDep: an After naming an undeclared group fails the
// enqueue instead of silently dropping the dependency.
func TestGroupWorkflowUnknownDep(t *testing.T) {
	q := newTestQ(t)
	wf := NewGroupWorkflow[greetIn]("bad",
		NewGroup[greetIn]("a", Step("s", dullTask("s-fn"))).After("nope"),
	)
	_, err := EnqueueWorkflow[greetOut](context.Background(), q, wf, greetIn{})
	if err == nil || !strings.Contains(err.Error(), "unknown group") {
		t.Fatalf("err = %v, want unknown-group error", err)
	}
}
