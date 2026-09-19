package quacker

import (
	"context"
	"strings"
	"testing"
)

// TestStepOnTaskDeps: a step can declare its dependency by passing the task,
// which resolves to the step that runs it.
func TestStepOnTaskDeps(t *testing.T) {
	q := newTestQ(t)
	ctx := context.Background()
	charge := NewTask("ref.charge", func(ctx context.Context, in string) (string, error) {
		return "charged:" + in, nil
	})
	ship := NewTask("ref.ship", func(ctx context.Context, in string) (string, error) {
		got, err := DepOutput[string](ctx, "charge")
		if err != nil {
			return "", err
		}
		return "shipped(" + got + ")", nil
	})
	notify := NewTask("ref.notify", func(ctx context.Context, in string) (string, error) {
		got, err := DepOutput[string](ctx, "ship")
		if err != nil {
			return "", err
		}
		return "notified(" + got + ")", nil
	})

	// charge by name, ship depends on the charge *Task, notify on the ship *Task.
	wf := NewWorkflow[string]("ref.flow",
		Step("charge", charge),
		StepOn("ship", ship, charge),
		StepOn("notify", notify, ship),
	)
	h, err := EnqueueWorkflow[string](ctx, q, wf, "o-1")
	if err != nil {
		t.Fatal(err)
	}
	out, err := h.Result(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if out != "notified(shipped(charged:o-1))" {
		t.Fatalf("out = %q", out)
	}
	// The DAG records the resolved step names as edges.
	dag, err := q.DAG(ctx, h.RunID())
	if err != nil {
		t.Fatal(err)
	}
	edges := map[string]string{}
	for _, e := range dag.Edges {
		edges[e.To] = e.From
	}
	if edges["ship"] != "charge" || edges["notify"] != "ship" {
		t.Fatalf("resolved edges = %v", edges)
	}
}

// TestStepOnDependencyErrors: unknown, ambiguous, and unsupported task
// dependencies are reported at enqueue.
func TestStepOnDependencyErrors(t *testing.T) {
	q := newTestQ(t)
	ctx := context.Background()

	a := NewTask("refa", func(ctx context.Context, in string) (string, error) { return in, nil })
	b := NewTask("refb", func(ctx context.Context, in string) (string, error) { return in, nil })
	notInWF := NewTask("refnotin", func(ctx context.Context, in string) (string, error) { return in, nil })

	// Unknown: the dependency task is not a step of the workflow.
	wf := NewWorkflow[string]("ref.unknown", Step("a", a), StepOn("b", b, notInWF))
	if _, err := EnqueueWorkflow[string](ctx, q, wf, "x"); err == nil || !strings.Contains(err.Error(), "not a step") {
		t.Fatalf("unknown dep err = %v", err)
	}

	// Ambiguous: the same task backs two steps, so a task reference cannot pick
	// one; depend by step name instead.
	shared := NewTask("refshared", func(ctx context.Context, in string) (string, error) { return in, nil })
	wf2 := NewWorkflow[string]("ref.ambiguous",
		Step("s1", shared),
		Step("s2", shared),
		StepOn("join", a, shared),
	)
	if _, err := EnqueueWorkflow[string](ctx, q, wf2, "x"); err == nil || !strings.Contains(err.Error(), "runs 2 steps") {
		t.Fatalf("ambiguous dep err = %v", err)
	}

	// Unsupported: an int is neither a step name nor a task.
	wf3 := NewWorkflow[string]("ref.bad", StepOn("x", a, 42))
	if _, err := EnqueueWorkflow[string](ctx, q, wf3, "x"); err == nil || !strings.Contains(err.Error(), "unsupported dependency") {
		t.Fatalf("unsupported dep err = %v", err)
	}
}

// TestStepOnMixedDeps: string step names and task references can be mixed.
func TestStepOnMixedDeps(t *testing.T) {
	q := newTestQ(t)
	ctx := context.Background()
	a := NewTask("mix.a", func(ctx context.Context, in string) (string, error) { return "a", nil })
	b := NewTask("mix.b", func(ctx context.Context, in string) (string, error) { return "b", nil })
	c := NewTask("mix.c", func(ctx context.Context, in string) (string, error) {
		x, err := DepOutput[string](ctx, "a")
		if err != nil {
			return "", err
		}
		y, err := DepOutput[string](ctx, "b")
		if err != nil {
			return "", err
		}
		return x + y, nil
	})
	wf := NewWorkflow[string]("mix.flow",
		Step("a", a),
		Step("b", b),
		StepOn("c", c, "a", b), // one by name, one by task
	)
	h, err := EnqueueWorkflow[string](ctx, q, wf, "in")
	if err != nil {
		t.Fatal(err)
	}
	if out, err := h.Result(ctx); err != nil || out != "ab" {
		t.Fatalf("out=%q err=%v", out, err)
	}
}

// TestTaskID: a task's ID is its stable identity (its name).
func TestTaskID(t *testing.T) {
	task := NewTask("id.check", func(ctx context.Context, in string) (string, error) { return in, nil })
	if task.ID() != task.Name() || task.ID() != "id.check" {
		t.Fatalf("ID=%q Name=%q", task.ID(), task.Name())
	}
}
