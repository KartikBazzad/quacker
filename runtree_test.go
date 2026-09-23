package quacker

import (
	"context"
	json "github.com/goccy/go-json"
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

	// The spawning step "fan" is contracted into the group it spawns: it is
	// not a node, and its dependency ("plan") becomes a group-level edge.
	for _, n := range tree.Nodes {
		if n.Name == "fan" {
			t.Fatalf("spawner step should be contracted into its group (nodes: %v)", nodeNames(tree))
		}
	}
	for _, n := range tree.Nodes {
		if strings.HasSuffix(n.Name, "/job") && len(n.Deps) != 0 {
			t.Fatalf("child node %s deps = %v, want none (group carries the dep)", n.Name, n.Deps)
		}
	}

	// Only a spawner's child runs form a group: ONE group for the "fan"
	// fan-out (all 3 children), and no box for the root run's own steps.
	if len(tree.Groups) != 1 {
		t.Fatalf("groups = %d, want 1 (fan-out only) (%v)", len(tree.Groups), tree.Groups)
	}
	if got := tree.Groups[0].Label; got != "fan → 3 child runs" {
		t.Fatalf("fan-out group label = %q, want %q", got, "fan → 3 child runs")
	}
	grp := tree.Groups[0]
	if len(grp.Deps) != 1 || grp.Deps[0] != "plan" {
		t.Fatalf("group deps = %v, want [plan]", grp.Deps)
	}
	groupEdges := 0
	for _, e := range tree.Edges {
		if e.From == "plan" && e.To == grp.Name {
			groupEdges++
		}
	}
	if groupEdges != 1 {
		t.Fatalf("plan → group edges = %d, want 1 (edges: %v)", groupEdges, tree.Edges)
	}
	for _, n := range tree.Nodes {
		if strings.HasSuffix(n.Name, "/job") {
			if n.Group != grp.Name {
				t.Fatalf("child node %s group = %q, want the fan group", n.Name, n.Group)
			}
		} else if n.Group != "" {
			t.Fatalf("root node %s group = %q, want none", n.Name, n.Group)
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
	if !strings.Contains(string(svg), "fan → 3 child runs") {
		t.Fatal("tree SVG missing the fan-out group label")
	}

	// The rendered group box must wrap only the child-run nodes, never the
	// root steps — the bounding-box-over-global-layout regression.
	nodeW := dagNodeWidth(tree.Nodes)
	pos, boxes, _, _ := layoutDAG(tree, nodeW)
	bx, ok := boxes[tree.Groups[0].Name]
	if !ok {
		t.Fatal("no box for the fan-out group")
	}
	for _, n := range tree.Nodes {
		p := pos[n.Name]
		cx, cy := p[0]+nodeW/2, p[1]+dagNodeH/2
		inside := cx >= bx.x && cx <= bx.x+bx.w && cy >= bx.y && cy <= bx.y+bx.h
		if want := n.Group == tree.Groups[0].Name; inside != want {
			t.Fatalf("node %s inside its group box = %v, want %v", n.Name, inside, want)
		}
	}
}

func nodeNames(d *DAG) []string {
	names := make([]string, len(d.Nodes))
	for i, n := range d.Nodes {
		names[i] = n.Name
	}
	return names
}

// TestDAGSVGGroupsAreIsolated: a group's box must contain only its own nodes
// and must not overlap a sibling group's box. This is the regression where
// boxes were min/max bounds over globally layered members, so a box could
// enclose nodes belonging to another group.
func TestDAGSVGGroupsAreIsolated(t *testing.T) {
	d := &DAG{
		RunID: "root", Workflow: "wf", Status: StatusSucceeded,
		Groups: []DAGGroup{
			{Name: "root/extract", Label: "extract → 1 child runs"},
			{Name: "root/load", Label: "load → 1 child runs"},
		},
		Nodes: []DAGNode{
			{Name: "extract", Group: ""},
			{Name: "load", Group: ""},
			{Name: "c1/extract_a", Group: "root/extract"},
			{Name: "c1/extract_b", Group: "root/extract"},
			{Name: "c1/extract_join", Group: "root/extract"},
			{Name: "c2/load_a", Group: "root/load"},
			{Name: "c2/load_b", Group: "root/load"},
		},
		Edges: []DAGEdge{
			{From: "extract", To: "c1/extract_a"},
			{From: "c1/extract_a", To: "c1/extract_join"},
			{From: "c1/extract_b", To: "c1/extract_join"},
			{From: "extract", To: "load"},
			{From: "load", To: "c2/load_a"},
			{From: "c2/load_a", To: "c2/load_b"},
		},
	}
	nodeW := dagNodeWidth(d.Nodes)
	pos, boxes, _, _ := layoutDAG(d, nodeW)

	if len(boxes) != len(d.Groups) {
		t.Fatalf("boxes = %d, want %d", len(boxes), len(d.Groups))
	}
	inside := func(r svgRect, name string) bool {
		p := pos[name]
		cx, cy := p[0]+nodeW/2, p[1]+dagNodeH/2
		return cx >= r.x && cx <= r.x+r.w && cy >= r.y && cy <= r.y+r.h
	}
	for _, g := range d.Groups {
		bx := boxes[g.Name]
		for _, n := range d.Nodes {
			if got, want := inside(bx, n.Name), n.Group == g.Name; got != want {
				t.Errorf("node %q inside box %q = %v, want %v (box=%+v pos=%v)",
					n.Name, g.Name, got, want, bx, pos[n.Name])
			}
		}
	}
	for i := 0; i < len(d.Groups); i++ {
		for j := i + 1; j < len(d.Groups); j++ {
			if a, b := boxes[d.Groups[i].Name], boxes[d.Groups[j].Name]; rectsOverlap(a, b) {
				t.Errorf("group boxes overlap: %q=%+v %q=%+v", d.Groups[i].Name, a, d.Groups[j].Name, b)
			}
		}
	}
}

func rectsOverlap(a, b svgRect) bool {
	return a.x < b.x+b.w && b.x < a.x+a.w && a.y < b.y+b.h && b.y < a.y+a.h
}
