package quacker

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

// TestDAGTreeIncludesChildren: DAGTree adds each child run's steps, namespaced,
// and attaches them to the step that spawned them.
func TestDAGTreeIncludesChildren(t *testing.T) {
	q := newTestQ(t, WithQueue("default", 16))
	ctx := context.Background()

	childJob := NewTask("tree.child", func(ctx context.Context, in int) (int, error) {
		return in + 1, nil
	})
	childWF := NewWorkflow[int]("tree.child.wf", Step("job", childJob))

	plan := NewTask("tree.plan", func(ctx context.Context, in int) (int, error) { return in, nil })
	fan := NewTask("tree.fan", func(ctx context.Context, in int) (int, error) {
		hs := make([]*RunHandle[int], 3)
		for i := range hs {
			h, err := EnqueueWorkflowChild[int](ctx, q, childWF, i)
			if err != nil {
				return 0, err
			}
			hs[i] = h
		}
		total := 0
		for _, h := range hs {
			v, err := h.Result(ctx)
			if err != nil {
				return 0, err
			}
			total += v
		}
		return total, nil
	})
	wf := NewWorkflow[int]("tree.parent",
		Step("plan", plan),
		Step("fan", fan, "plan"),
	)
	h, err := EnqueueWorkflow[int](ctx, q, wf, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Result(ctx); err != nil {
		t.Fatal(err)
	}

	tree, err := q.DAGTree(ctx, h.RunID())
	if err != nil {
		t.Fatal(err)
	}
	if tree.RunID != h.RunID() || tree.Status != StatusSucceeded {
		t.Fatalf("tree root = %s/%s", tree.RunID, tree.Status)
	}

	var childSteps int
	for _, n := range tree.Nodes {
		if strings.HasSuffix(n.Name, "/job") {
			childSteps++
		}
	}
	if childSteps != 3 {
		t.Fatalf("child steps in tree = %d, want 3 (nodes: %v)", childSteps, nodeNames(tree))
	}

	// Each child is attached to the spawning step "fan", both as an edge and
	// as a dependency (so the level layout places it after fan, not in the
	// root column).
	spawnEdges := 0
	for _, e := range tree.Edges {
		if e.From == "fan" && strings.HasSuffix(e.To, "/job") {
			spawnEdges++
		}
	}
	if spawnEdges != 3 {
		t.Fatalf("spawn edges from fan = %d, want 3 (edges: %v)", spawnEdges, tree.Edges)
	}
	for _, n := range tree.Nodes {
		if strings.HasSuffix(n.Name, "/job") {
			if len(n.Deps) != 1 || n.Deps[0] != "fan" {
				t.Fatalf("child node %s deps = %v, want [fan]", n.Name, n.Deps)
			}
		}
	}

	// The plain DAG (one run) does NOT include the children.
	plain, err := q.DAG(ctx, h.RunID())
	if err != nil {
		t.Fatal(err)
	}
	if len(plain.Nodes) != 2 {
		t.Fatalf("plain DAG nodes = %d, want 2", len(plain.Nodes))
	}

	// JSON and SVG render the tree.
	js, err := q.DAGTreeJSON(ctx, h.RunID())
	if err != nil {
		t.Fatal(err)
	}
	var decoded DAG
	if err := json.Unmarshal(js, &decoded); err != nil {
		t.Fatalf("tree JSON invalid: %v", err)
	}
	if len(decoded.Nodes) != len(tree.Nodes) {
		t.Fatalf("decoded nodes = %d, tree nodes = %d", len(decoded.Nodes), len(tree.Nodes))
	}
	svg, err := q.DAGTreeSVG(ctx, h.RunID())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(svg), "fan") || !strings.Contains(string(svg), "/job") {
		t.Fatal("tree SVG missing parent step or child step")
	}
}

func nodeNames(d *DAG) []string {
	names := make([]string, len(d.Nodes))
	for i, n := range d.Nodes {
		names[i] = n.Name
	}
	return names
}
