package quacker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ELT-shaped DAGs: a plan fans out to per-shard extracts, each shard is
// transformed in series, the shards fan in to a stage step, then a load and a
// set of per-partition child workflows run, and a final report joins them.
// These tests assert what a pipeline actually needs: correct data flow along
// every edge, fan-out parallelism, fan-in gating, child-run lineage, and
// failure/retry behavior.

type eltPlan struct {
	Shards int `json:"shards"`
}
type eltExtract struct {
	Shard int `json:"shard"`
	Rows  int `json:"rows"`
}
type eltTransform struct {
	Shard int `json:"shard"`
	Rows  int `json:"rows"`
}
type eltStage struct {
	Rows int `json:"rows"`
}
type eltLoad struct {
	Rows int `json:"rows"`
}
type eltReport struct {
	Rows       int `json:"rows"`
	Partitions int `json:"partitions"`
}

// assertDAGOrder checks the recorder saw every node exactly once, with no
// dependency violated, and (given deps) that every dep ran first.
func assertDAGOrder(t *testing.T, rec *dagRecorder, deps map[string][]string) {
	t.Helper()
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.violations) != 0 {
		t.Fatalf("dependency order violated: %v", rec.violations)
	}
	for name := range deps {
		if rec.ran[name] != 1 {
			t.Fatalf("node %s ran %d times, want once", name, rec.ran[name])
		}
	}
	if len(rec.order) != len(deps) {
		t.Fatalf("ran %d nodes, want %d: %v", len(rec.order), len(deps), rec.order)
	}
	pos := make(map[string]int, len(rec.order))
	for i, n := range rec.order {
		pos[n] = i
	}
	for name, ds := range deps {
		for _, d := range ds {
			if pos[d] >= pos[name] {
				t.Fatalf("node %s ran before dep %s", name, d)
			}
		}
	}
}

// TestELTPipeline builds a full extract→transform→stage→load pipeline with a
// per-shard transform chain, a fan-in, and per-partition child workflows.
func TestELTPipeline(t *testing.T) {
	const shards = 8
	q := newTestQ(t, WithQueue("default", 32))
	ctx := context.Background()
	rec := newDAGRecorder()

	// Track extract concurrency to prove the fan-out runs in parallel.
	var inFlight int32
	var maxInFlight int
	var mu sync.Mutex
	enter := func() {
		n := int(atomic.AddInt32(&inFlight, 1))
		mu.Lock()
		if n > maxInFlight {
			maxInFlight = n
		}
		mu.Unlock()
	}
	leave := func() { atomic.AddInt32(&inFlight, -1) }

	plan := NewTask("elt.plan", func(ctx context.Context, in int) (eltPlan, error) {
		rec.enter("plan", nil)
		return eltPlan{Shards: in}, nil
	})

	deps := map[string][]string{"plan": nil}
	steps := []StepDef[int]{Step("plan", plan)}

	for i := 0; i < shards; i++ {
		i := i
		name := fmt.Sprintf("extract_%d", i)
		deps[name] = []string{"plan"}
		ext := NewTask("elt."+name, func(ctx context.Context, in int) (eltExtract, error) {
			rec.enter(name, []string{"plan"})
			enter()
			time.Sleep(5 * time.Millisecond) // make overlap observable
			leave()
			return eltExtract{Shard: i, Rows: (i + 1) * 10}, nil
		})
		steps = append(steps, Step(name, ext, "plan"))

		tname := fmt.Sprintf("transform_%d", i)
		deps[tname] = []string{name}
		tr := NewTask("elt."+tname, func(ctx context.Context, in int) (eltTransform, error) {
			rec.enter(tname, []string{name})
			up, err := DepOutput[eltExtract](ctx, name)
			if err != nil {
				return eltTransform{}, err
			}
			if up.Shard != i {
				return eltTransform{}, fmt.Errorf("shard mismatch: got %d want %d", up.Shard, i)
			}
			return eltTransform{Shard: up.Shard, Rows: up.Rows + 1}, nil
		})
		steps = append(steps, Step(tname, tr, name))
	}

	var transformNames []string
	for i := range shards {
		transformNames = append(transformNames, fmt.Sprintf("transform_%d", i))
	}
	// stage fans in every transform.
	deps["stage"] = append([]string(nil), transformNames...)
	stage := NewTask("elt.stage", func(ctx context.Context, in int) (eltStage, error) {
		rec.enter("stage", transformNames)
		total := 0
		for i := 0; i < shards; i++ {
			up, err := DepOutput[eltTransform](ctx, fmt.Sprintf("transform_%d", i))
			if err != nil {
				return eltStage{}, err
			}
			total += up.Rows
		}
		return eltStage{Rows: total}, nil
	})
	steps = append(steps, Step("stage", stage, transformNames...))

	deps["load"] = []string{"stage"}
	load := NewTask("elt.load", func(ctx context.Context, in int) (eltLoad, error) {
		rec.enter("load", []string{"stage"})
		up, err := DepOutput[eltStage](ctx, "stage")
		if err != nil {
			return eltLoad{}, err
		}
		return eltLoad{Rows: up.Rows}, nil
	})
	steps = append(steps, Step("load", load, "stage"))

	// Per-partition child workflows, spawned from inside a step and awaited.
	childTask := NewTask("elt.partitionjob", func(ctx context.Context, in int) (int, error) {
		return in + 1, nil
	})
	childWF := NewWorkflow[int]("elt.partition-jobs", Step("job", childTask))

	deps["partition"] = []string{"stage"}
	partition := NewTask("elt.partition", func(ctx context.Context, in int) (int, error) {
		rec.enter("partition", []string{"stage"})
		if _, err := DepOutput[eltStage](ctx, "stage"); err != nil {
			return 0, err
		}
		hs := make([]*RunHandle[int], shards)
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
	steps = append(steps, Step("partition", partition, "stage"))

	deps["report"] = []string{"load", "partition"}
	report := NewTask("elt.report", func(ctx context.Context, in int) (eltReport, error) {
		rec.enter("report", []string{"load", "partition"})
		l, err := DepOutput[eltLoad](ctx, "load")
		if err != nil {
			return eltReport{}, err
		}
		p, err := DepOutput[int](ctx, "partition")
		if err != nil {
			return eltReport{}, err
		}
		return eltReport{Rows: l.Rows, Partitions: p}, nil
	})
	steps = append(steps, Step("report", report, "load", "partition"))

	wf := NewWorkflow[int]("elt.pipeline", steps...)
	h, err := EnqueueWorkflow[eltReport](ctx, q, wf, shards)
	if err != nil {
		t.Fatal(err)
	}
	out, err := h.Result(ctx)
	if err != nil {
		t.Fatalf("pipeline failed: %v", err)
	}

	// Data flow: extract_i = (i+1)*10 rows, transform adds 1, stage sums.
	wantStage := 0
	for i := 0; i < shards; i++ {
		wantStage += (i+1)*10 + 1
	}
	wantPartitions := 0
	for i := 0; i < shards; i++ {
		wantPartitions += i + 1
	}
	if out.Rows != wantStage || out.Partitions != wantPartitions {
		t.Fatalf("report = %+v, want rows=%d partitions=%d", out, wantStage, wantPartitions)
	}

	assertDAGOrder(t, rec, deps)

	if maxInFlight < shards/2 {
		t.Fatalf("extracts ran with max concurrency %d, want at least %d (fan-out not parallel)", maxInFlight, shards/2)
	}

	// Introspected graph shape.
	dag, err := q.DAG(ctx, h.RunID())
	if err != nil {
		t.Fatal(err)
	}
	if len(dag.Nodes) != 2*shards+5 { // plan + extract + transform + stage + load + partition + report
		t.Fatalf("DAG nodes = %d, want %d", len(dag.Nodes), 2*shards+5)
	}
	wantEdges := shards + shards + shards + 1 + 1 + 2
	if len(dag.Edges) != wantEdges {
		t.Fatalf("DAG edges = %d, want %d", len(dag.Edges), wantEdges)
	}

	// Child-workflow lineage: one child workflow run per shard.
	snap, err := q.Execution(ctx, h.RunID())
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Children) != shards {
		t.Fatalf("children = %d, want %d", len(snap.Children), shards)
	}
	for _, c := range snap.Children {
		if c.ParentID != h.RunID() || c.Status != StatusSucceeded {
			t.Fatalf("child %s parent=%s status=%s", c.RunID, c.ParentID, c.Status)
		}
		if c.Kind != KindWorkflow {
			t.Fatalf("child %s kind = %s, want workflow", c.RunID, c.Kind)
		}
	}
}

// TestELTFailureCancelsRemaining: a shard that fails permanently fails the run
// and its untouched dependents are cancelled rather than run.
func TestELTFailureCancelsRemaining(t *testing.T) {
	const shards = 4
	const badShard = 2
	q := newTestQ(t, WithQueue("default", 16))
	ctx := context.Background()
	rec := newDAGRecorder()

	plan := NewTask("eltf.plan", func(ctx context.Context, in int) (eltPlan, error) {
		rec.enter("plan", nil)
		return eltPlan{Shards: in}, nil
	})
	steps := []StepDef[int]{Step("plan", plan)}

	var transforms []string
	for i := 0; i < shards; i++ {
		i := i
		ename := fmt.Sprintf("extract_%d", i)
		ext := NewTask("eltf."+ename, func(ctx context.Context, in int) (eltExtract, error) {
			rec.enter(ename, []string{"plan"})
			if i == badShard {
				return eltExtract{}, errors.New("extract boom")
			}
			return eltExtract{Shard: i, Rows: 10}, nil
		})
		steps = append(steps, Step(ename, ext, "plan"))

		tname := fmt.Sprintf("transform_%d", i)
		transforms = append(transforms, tname)
		tr := NewTask("eltf."+tname, func(ctx context.Context, in int) (eltTransform, error) {
			rec.enter(tname, []string{ename})
			up, err := DepOutput[eltExtract](ctx, ename)
			if err != nil {
				return eltTransform{}, err
			}
			return eltTransform{Shard: up.Shard, Rows: up.Rows}, nil
		})
		steps = append(steps, Step(tname, tr, ename))
	}
	stage := NewTask("eltf.stage", func(ctx context.Context, in int) (eltStage, error) {
		rec.enter("stage", transforms)
		return eltStage{}, nil
	})
	steps = append(steps, Step("stage", stage, transforms...))

	wf := NewWorkflow[int]("eltf.pipeline", steps...)
	h, err := EnqueueWorkflow[eltStage](ctx, q, wf, shards)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Result(ctx); err == nil {
		t.Fatal("expected the pipeline to fail")
	}

	snap, err := q.Execution(ctx, h.RunID())
	if err != nil {
		t.Fatal(err)
	}
	if snap.Status != StatusFailed {
		t.Fatalf("run status = %s, want FAILED", snap.Status)
	}
	byName := map[string]StepState{}
	for _, s := range snap.Steps {
		byName[s.Name] = s
	}
	if got := byName[fmt.Sprintf("extract_%d", badShard)].Status; got != StatusFailed {
		t.Fatalf("bad extract = %s, want FAILED", got)
	}
	// The bad shard's transform and the fan-in never ran: CANCELLED.
	if got := byName[fmt.Sprintf("transform_%d", badShard)].Status; got != StatusCancelled {
		t.Fatalf("dependent transform = %s, want CANCELLED", got)
	}
	if got := byName["stage"].Status; got != StatusCancelled {
		t.Fatalf("stage = %s, want CANCELLED", got)
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if rec.ran["stage"] != 0 {
		t.Fatal("stage ran despite a failed upstream shard")
	}
}

// TestELTRetryThenSuccess: a transient shard failure is retried and the
// pipeline still succeeds.
func TestELTRetryThenSuccess(t *testing.T) {
	q := newTestQ(t, WithQueue("default", 16))
	ctx := context.Background()
	rec := newDAGRecorder()
	var calls int32

	plan := NewTask("eltr.plan", func(ctx context.Context, in int) (eltPlan, error) {
		rec.enter("plan", nil)
		return eltPlan{Shards: 1}, nil
	})
	extract := NewTask("eltr.extract", func(ctx context.Context, in int) (eltExtract, error) {
		rec.enter("extract", []string{"plan"})
		if atomic.AddInt32(&calls, 1) == 1 {
			return eltExtract{}, errors.New("transient")
		}
		return eltExtract{Shard: 0, Rows: 42}, nil
	}, Retries(2), BackoffPolicy(Constant(time.Millisecond)))
	load := NewTask("eltr.load", func(ctx context.Context, in int) (eltLoad, error) {
		rec.enter("load", []string{"extract"})
		up, err := DepOutput[eltExtract](ctx, "extract")
		if err != nil {
			return eltLoad{}, err
		}
		return eltLoad{Rows: up.Rows}, nil
	})
	wf := NewWorkflow[int]("eltr.pipeline",
		Step("plan", plan),
		Step("extract", extract, "plan"),
		Step("load", load, "extract"),
	)
	h, err := EnqueueWorkflow[eltLoad](ctx, q, wf, 1)
	if err != nil {
		t.Fatal(err)
	}
	out, err := h.Result(ctx)
	if err != nil {
		t.Fatalf("pipeline failed: %v", err)
	}
	if out.Rows != 42 {
		t.Fatalf("load rows = %d, want 42", out.Rows)
	}
	snap, _ := q.Execution(ctx, h.RunID())
	for _, s := range snap.Steps {
		if s.Name == "extract" {
			if s.Status != StatusSucceeded || s.Attempts != 2 {
				t.Fatalf("extract = %s attempts=%d, want SUCCEEDED/2", s.Status, s.Attempts)
			}
		}
	}
}

// TestELTWideFanInDeepChain: a wide fan-in over many parallel shards plus a long
// series chain in one run — the shape of a large ELT graph.
func TestELTWideFanInDeepChain(t *testing.T) {
	const (
		wide = 200
		deep = 100
	)
	q := newTestQ(t, WithQueue("default", 64))
	ctx := context.Background()
	rec := newDAGRecorder()

	// A deep chain c0 -> c1 -> ... -> c{deep-1}.
	steps := []StepDef[int]{}
	deps := map[string][]string{}
	prev := ""
	for i := 0; i < deep; i++ {
		name := fmt.Sprintf("c%d", i)
		var d []string
		if prev != "" {
			d = []string{prev}
		}
		i := i
		task := NewTask("eltw."+name, func(ctx context.Context, in int) (eltTransform, error) {
			rec.enter(name, d)
			return eltTransform{Shard: i, Rows: i}, nil
		})
		steps = append(steps, Step(name, task, d...))
		deps[name] = d
		prev = name
	}
	// A wide fan-in: every shard depends on the chain tip.
	shards := make([]string, wide)
	for i := 0; i < wide; i++ {
		name := fmt.Sprintf("w%d", i)
		shards[i] = name
		task := NewTask("eltw."+name, func(ctx context.Context, in int) (eltExtract, error) {
			rec.enter(name, []string{prev})
			return eltExtract{Shard: i, Rows: 1}, nil
		})
		steps = append(steps, Step(name, task, prev))
		deps[name] = []string{prev}
	}
	// A final join over the whole fan-in.
	join := NewTask("eltw.join", func(ctx context.Context, in int) (eltStage, error) {
		rec.enter("join", shards)
		total := 0
		for i := 0; i < wide; i++ {
			up, err := DepOutput[eltExtract](ctx, fmt.Sprintf("w%d", i))
			if err != nil {
				return eltStage{}, err
			}
			total += up.Rows
		}
		return eltStage{Rows: total}, nil
	})
	steps = append(steps, Step("join", join, shards...))
	deps["join"] = append([]string(nil), shards...)

	wf := NewWorkflow[int]("eltw.pipeline", steps...)
	h, err := EnqueueWorkflow[eltStage](ctx, q, wf, 0)
	if err != nil {
		t.Fatal(err)
	}
	out, err := h.Result(ctx)
	if err != nil {
		t.Fatalf("wide/deep pipeline failed: %v", err)
	}
	if out.Rows != wide {
		t.Fatalf("join rows = %d, want %d", out.Rows, wide)
	}
	assertDAGOrder(t, rec, deps)
}
