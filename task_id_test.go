package quacker

import (
	"context"
	"testing"
)

// TestTaskID: a task's ID is its name, so a name is defined once and referenced
// wherever a string would otherwise be repeated.
func TestTaskID(t *testing.T) {
	task := NewTask("id.check", func(ctx context.Context, in string) (string, error) { return in, nil })
	if task.ID() != "id.check" || task.ID() != task.Name() {
		t.Fatalf("ID=%q Name=%q", task.ID(), task.Name())
	}
}

// TestTaskIDAsDependency uses Task.ID() as the step dependency instead of a
// hardcoded, misspellable string.
func TestTaskIDAsDependency(t *testing.T) {
	q := newTestQ(t)
	ctx := context.Background()

	charge := NewTask("charge", func(ctx context.Context, in string) (string, error) {
		return "charged:" + in, nil
	})
	ship := NewTask("ship", func(ctx context.Context, in string) (string, error) {
		got, err := DepOutput[string](ctx, charge.ID())
		if err != nil {
			return "", err
		}
		return "shipped(" + got + ")", nil
	})
	notify := NewTask("notify", func(ctx context.Context, in string) (string, error) {
		got, err := DepOutput[string](ctx, ship.ID())
		if err != nil {
			return "", err
		}
		return "notified(" + got + ")", nil
	})

	wf := NewWorkflow[string]("id.flow",
		Step(charge.ID(), charge),
		Step(ship.ID(), ship, charge.ID()),
		Step(notify.ID(), notify, ship.ID()),
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

	dag, err := q.DAG(ctx, h.RunID())
	if err != nil {
		t.Fatal(err)
	}
	edges := map[string]string{}
	for _, e := range dag.Edges {
		edges[e.To] = e.From
	}
	if edges["ship"] != "charge" || edges["notify"] != "ship" {
		t.Fatalf("edges = %v", edges)
	}
}
