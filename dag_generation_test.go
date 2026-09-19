package quacker

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"testing"
)

// dagOut is the output of every generated DAG node: its name plus a fold of its
// dependencies' outputs, so data propagation along every edge is verifiable.
type dagOut struct {
	Name string `json:"name"`
	Sum  string `json:"sum"`
}

func dagSum(name string, deps []string, dep map[string]string) string {
	parts := make([]string, 0, len(deps))
	for _, d := range deps {
		parts = append(parts, d+"="+dep[d])
	}
	sort.Strings(parts)
	return name + "[" + strings.Join(parts, ",") + "]"
}

// dagRecorder records the execution order and flags any node that entered
// before one of its dependencies, the topological-order guarantee.
type dagRecorder struct {
	mu         sync.Mutex
	order      []string
	ran        map[string]int
	violations []string
}

func newDAGRecorder() *dagRecorder { return &dagRecorder{ran: map[string]int{}} }

func (r *dagRecorder) enter(name string, deps []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, d := range deps {
		if r.ran[d] == 0 {
			r.violations = append(r.violations, name+" started before "+d)
		}
	}
	r.order = append(r.order, name)
	r.ran[name]++
}

// nodeSpec is one generated DAG node: a name, its dependencies, and a kind
// ("root", "series", "parallel", "join", "child").
type nodeSpec struct {
	name string
	deps []string
	kind string
}

func dagTask(prefix string, spec nodeSpec, rec *dagRecorder) *Task[string, dagOut] {
	return NewTask(prefix+"."+spec.name, func(ctx context.Context, in string) (dagOut, error) {
		rec.enter(spec.name, spec.deps)
		sums := make(map[string]string, len(spec.deps))
		for _, d := range spec.deps {
			o, err := DepOutput[dagOut](ctx, d)
			if err != nil {
				return dagOut{}, fmt.Errorf("node %s: dep %s: %w", spec.name, d, err)
			}
			sums[d] = o.Sum
		}
		return dagOut{Name: spec.name, Sum: dagSum(spec.name, spec.deps, sums)}, nil
	})
}

// buildSpecWorkflow declares every spec as a step, using override for the node
// named overrideName (the child-spawning node, which needs the engine).
func buildSpecWorkflow(prefix, wfName string, specs []nodeSpec, rec *dagRecorder, overrideName string, override *Task[string, dagOut]) *Workflow[string] {
	steps := make([]StepDef[string], 0, len(specs))
	for _, s := range specs {
		def := dagTask(prefix, s, rec)
		if s.name == overrideName {
			def = override
		}
		steps = append(steps, Step(s.name, def, s.deps...))
	}
	return NewWorkflow[string](wfName, steps...)
}

// expectedSums folds the DAG the same way the tasks do; specs must be in
// topological (declaration) order so each node's deps are already computed.
func expectedSums(specs []nodeSpec) map[string]string {
	sums := map[string]string{}
	for _, s := range specs {
		sums[s.name] = dagSum(s.name, s.deps, sums)
	}
	return sums
}

// verifyDAG runs the shared assertions: success, one run per node, topological
// order, propagated output, and the introspected graph shape.
func verifyDAG(t *testing.T, q *Quacker, h *RunHandle[dagOut], specs []nodeSpec, rec *dagRecorder) {
	t.Helper()
	ctx := context.Background()
	sums := expectedSums(specs)
	last := specs[len(specs)-1].name

	out, err := h.Result(ctx)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if out.Name != last || out.Sum != sums[last] {
		t.Fatalf("run output = %+v, want %s", out, sums[last])
	}

	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.violations) != 0 {
		t.Fatalf("dependency order violated: %v", rec.violations)
	}
	if len(rec.order) != len(specs) {
		t.Fatalf("ran %d nodes, want %d", len(rec.order), len(specs))
	}
	for _, s := range specs {
		if rec.ran[s.name] != 1 {
			t.Fatalf("node %s ran %d times, want once", s.name, rec.ran[s.name])
		}
	}
	pos := make(map[string]int, len(rec.order))
	for i, n := range rec.order {
		pos[n] = i
	}
	for _, s := range specs {
		for _, d := range s.deps {
			if pos[d] >= pos[s.name] {
				t.Fatalf("node %s ran before dep %s", s.name, d)
			}
		}
	}

	// The introspected graph matches the declaration.
	dag, err := q.DAG(ctx, h.RunID())
	if err != nil {
		t.Fatal(err)
	}
	if len(dag.Nodes) != len(specs) {
		t.Fatalf("DAG has %d nodes, want %d", len(dag.Nodes), len(specs))
	}
	wantEdges := 0
	byName := make(map[string]DAGNode, len(dag.Nodes))
	for _, n := range dag.Nodes {
		byName[n.Name] = n
	}
	for _, s := range specs {
		wantEdges += len(s.deps)
		n, ok := byName[s.name]
		if !ok {
			t.Fatalf("DAG missing node %s", s.name)
		}
		if n.Status != StatusSucceeded {
			t.Fatalf("node %s status = %s, want SUCCEEDED", s.name, n.Status)
		}
		if !equalSorted(n.Deps, s.deps) {
			t.Fatalf("node %s deps = %v, want %v", s.name, n.Deps, s.deps)
		}
	}
	if len(dag.Edges) != wantEdges {
		t.Fatalf("DAG has %d edges, want %d", len(dag.Edges), wantEdges)
	}
}

func equalSorted(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	as, bs := append([]string(nil), a...), append([]string(nil), b...)
	sort.Strings(as)
	sort.Strings(bs)
	for i := range as {
		if as[i] != bs[i] {
			return false
		}
	}
	return true
}

// TestDAGComplexTopology builds one DAG exercising every node type: a series
// chain, a parallel fan-out/fan-in, a second independent chain, a node that
// spawns and awaits child runs, and a final join across all three branches.
//
//	start ─ prep ─┬─ fan ─┬─ w0 ─┐
//	              │       ├─ w1 ─┤
//	              │       ├─ w2 ─┼─ gather ─┐
//	              │       └─ w3 ─┘          │
//	              └─ spawn(3 children) ─────┼─ done
//	start ─ chain1 ─ chain2 ────────────────┘
func TestDAGComplexTopology(t *testing.T) {
	q := newTestQ(t, WithQueue("default", 16))
	ctx := context.Background()
	rec := newDAGRecorder()
	const prefix = "dagcomplex"

	child := NewTask(prefix+".child", func(ctx context.Context, in int) (int, error) {
		return in * 2, nil
	})
	spawn := NewTask(prefix+".spawn", func(ctx context.Context, in string) (dagOut, error) {
		rec.enter("spawn", []string{"prep"})
		o, err := DepOutput[dagOut](ctx, "prep")
		if err != nil {
			return dagOut{}, err
		}
		hs := make([]*RunHandle[int], 3)
		for i := range hs {
			h, err := EnqueueChild(ctx, q, child, i)
			if err != nil {
				return dagOut{}, err
			}
			hs[i] = h
		}
		total := 0
		for _, h := range hs {
			v, err := h.Result(ctx)
			if err != nil {
				return dagOut{}, err
			}
			total += v
		}
		if total != 6 { // 0+2+4
			return dagOut{}, fmt.Errorf("child total = %d, want 6", total)
		}
		return dagOut{Name: "spawn", Sum: dagSum("spawn", []string{"prep"}, map[string]string{"prep": o.Sum})}, nil
	})

	specs := []nodeSpec{
		{name: "start", kind: "root"},
		{name: "prep", deps: []string{"start"}, kind: "series"},
		{name: "chain1", deps: []string{"start"}, kind: "series"},
		{name: "chain2", deps: []string{"chain1"}, kind: "series"},
		{name: "fan", deps: []string{"prep"}, kind: "parallel"},
		{name: "w0", deps: []string{"fan"}, kind: "parallel"},
		{name: "w1", deps: []string{"fan"}, kind: "parallel"},
		{name: "w2", deps: []string{"fan"}, kind: "parallel"},
		{name: "w3", deps: []string{"fan"}, kind: "parallel"},
		{name: "gather", deps: []string{"w0", "w1", "w2", "w3"}, kind: "join"},
		{name: "spawn", deps: []string{"prep"}, kind: "child"},
		{name: "done", deps: []string{"gather", "chain2", "spawn"}, kind: "join"},
	}

	wf := buildSpecWorkflow(prefix, "complex", specs, rec, "spawn", spawn)
	h, err := EnqueueWorkflow[dagOut](ctx, q, wf, "root-input")
	if err != nil {
		t.Fatal(err)
	}
	verifyDAG(t, q, h, specs, rec)

	snap, err := q.Execution(ctx, h.RunID())
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Children) != 3 {
		t.Fatalf("run has %d child runs, want 3", len(snap.Children))
	}
	for _, c := range snap.Children {
		if c.ParentID != h.RunID() {
			t.Fatalf("child %s parent = %s, want %s", c.RunID, c.ParentID, h.RunID())
		}
		if c.Status != StatusSucceeded {
			t.Fatalf("child %s status = %s, want SUCCEEDED", c.RunID, c.Status)
		}
	}
}

// TestDAGGeneratedMixed generates several DAGs from seeds, each a random mix of
// series chains, parallel fan-out/fan-in blocks, and diamonds, and verifies
// every one.
func TestDAGGeneratedMixed(t *testing.T) {
	for _, seed := range []int64{1, 7, 42, 1337, 90210} {
		seed := seed
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			q := newTestQ(t, WithQueue("default", 16))
			rec := newDAGRecorder()
			specs := generateDAG(seed, 8)
			prefix := fmt.Sprintf("daggen%d", seed)
			wf := buildSpecWorkflow(prefix, prefix, specs, rec, "", nil)
			h, err := EnqueueWorkflow[dagOut](context.Background(), q, wf, "in")
			if err != nil {
				t.Fatal(err)
			}
			verifyDAG(t, q, h, specs, rec)
		})
	}
}

// generateDAG builds a topological list of nodes by composing segments. Each
// segment is a series chain, a parallel fan-out/fan-in, or a diamond; the next
// segment's roots depend on the previous segment's tail, so the whole graph is
// connected and strictly ordered.
func generateDAG(seed int64, segments int) []nodeSpec {
	r := rand.New(rand.NewSource(seed))
	var specs []nodeSpec
	tail := "" // last node of the previous segment ("" for the first)

	add := func(name string, deps []string, kind string) {
		specs = append(specs, nodeSpec{name: name, deps: deps, kind: kind})
	}

	for seg := 0; seg < segments; seg++ {
		var roots []string
		if tail != "" {
			roots = []string{tail}
		}
		switch r.Intn(3) {
		case 0: // series: chain of length 2..5
			length := 2 + r.Intn(4)
			prev := roots
			for i := 0; i < length; i++ {
				name := fmt.Sprintf("s%d_%d", seg, i)
				add(name, prev, "series")
				prev = []string{name}
			}
			tail = prev[0]
		case 1: // parallel: a fork into W workers joined by a gather
			fork := fmt.Sprintf("p%d_fork", seg)
			add(fork, roots, "parallel")
			width := 2 + r.Intn(4)
			workers := make([]string, 0, width)
			for i := 0; i < width; i++ {
				w := fmt.Sprintf("p%d_w%d", seg, i)
				add(w, []string{fork}, "parallel")
				workers = append(workers, w)
			}
			join := fmt.Sprintf("p%d_join", seg)
			add(join, workers, "join")
			tail = join
		default: // diamond: split into two, rejoin
			split := fmt.Sprintf("d%d_split", seg)
			add(split, roots, "parallel")
			left := fmt.Sprintf("d%d_left", seg)
			right := fmt.Sprintf("d%d_right", seg)
			add(left, []string{split}, "parallel")
			add(right, []string{split}, "parallel")
			merge := fmt.Sprintf("d%d_merge", seg)
			add(merge, []string{left, right}, "join")
			tail = merge
		}
		// Occasionally add a leaf that depends on two earlier nodes, deepening
		// the graph and creating an extra fan-in.
		if r.Intn(3) == 0 && len(specs) > 2 {
			a := specs[r.Intn(len(specs))].name
			b := specs[r.Intn(len(specs))].name
			if a != b {
				extra := fmt.Sprintf("x%d", seg)
				add(extra, []string{a, b}, "join")
				tail = extra
			}
		}
	}
	return specs
}

// TestGeneratedDAGRejectsInvalid: a generated-but-invalid graph (cycle or
// duplicate step name) is refused at enqueue.
func TestGeneratedDAGRejectsInvalid(t *testing.T) {
	q := newTestQ(t)
	ctx := context.Background()
	mk := func(name string) *Task[string, dagOut] {
		return NewTask("daginvalid."+name, func(ctx context.Context, in string) (dagOut, error) {
			return dagOut{Name: name, Sum: name}, nil
		})
	}

	cycle := NewWorkflow[string]("cyc",
		Step("a", mk("a"), "b"),
		Step("b", mk("b"), "a"),
	)
	if _, err := EnqueueWorkflow[dagOut](ctx, q, cycle, "x"); err == nil {
		t.Fatal("cyclic workflow accepted")
	}

	dup := NewWorkflow[string]("dup",
		Step("a", mk("a")),
		Step("a", mk("a")),
	)
	if _, err := EnqueueWorkflow[dagOut](ctx, q, dup, "x"); err == nil {
		t.Fatal("duplicate step name accepted")
	}

	missing := NewWorkflow[string]("missing", Step("a", mk("a"), "ghost"))
	if _, err := EnqueueWorkflow[dagOut](ctx, q, missing, "x"); err == nil {
		t.Fatal("dependency on an unknown step accepted")
	}
}

// TestDAGGeneratedLarge exercises a large generated DAG end to end.
func TestDAGGeneratedLarge(t *testing.T) {
	q := newTestQ(t, WithQueue("default", 32))
	rec := newDAGRecorder()
	specs := generateDAG(2026, 40)
	if len(specs) < 100 {
		t.Fatalf("generator produced only %d nodes", len(specs))
	}
	wf := buildSpecWorkflow("daglarge", "daglarge", specs, rec, "", nil)
	h, err := EnqueueWorkflow[dagOut](context.Background(), q, wf, "in")
	if err != nil {
		t.Fatal(err)
	}
	verifyDAG(t, q, h, specs, rec)
}
