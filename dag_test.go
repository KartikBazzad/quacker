package quacker

import (
	"context"
	"encoding/xml"
	"errors"
	json "github.com/goccy/go-json"
	"strings"
	"testing"
	"time"
)

func dullTask(name string) *Task[greetIn, greetOut] {
	return NewTask(name, func(ctx context.Context, in greetIn) (greetOut, error) {
		return greetOut{}, nil
	})
}

func runWorkflow(t *testing.T, q *Quacker) *RunHandle[greetOut] {
	t.Helper()
	wf := NewWorkflow[greetIn]("fulfill",
		Step("charge", dullTask("charge-fn")),
		Step("ship", dullTask("ship-fn"), "charge"),
		Step("notify", dullTask("notify-fn"), "charge", "ship"),
	)
	h, err := EnqueueWorkflow[greetOut](context.Background(), q, wf, greetIn{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Result(context.Background()); err != nil {
		t.Fatal(err)
	}
	return h
}

// TestDAGJSON: the JSON snapshot carries every node, edge, and current state.
func TestDAGJSON(t *testing.T) {
	q := newTestQ(t)
	h := runWorkflow(t, q)

	raw, err := q.DAGJSON(context.Background(), h.RunID())
	if err != nil {
		t.Fatal(err)
	}
	var d DAG
	if err := json.Unmarshal(raw, &d); err != nil {
		t.Fatal(err)
	}
	if d.RunID != h.RunID() || d.Workflow != "fulfill" || d.Status != StatusSucceeded {
		t.Fatalf("dag header = %+v", d)
	}
	if len(d.Nodes) != 3 || len(d.Edges) != 3 {
		t.Fatalf("nodes=%d edges=%d, want 3/3", len(d.Nodes), len(d.Edges))
	}
	if d.Nodes[0].Name != "charge" || d.Nodes[0].Task != "charge-fn" {
		t.Fatalf("node 0 = %+v", d.Nodes[0])
	}
	for _, n := range d.Nodes {
		if n.Status != StatusSucceeded {
			t.Fatalf("node %s status = %s, want SUCCEEDED", n.Name, n.Status)
		}
	}
	// Dependency order: charge is a root, notify is last.
	if lv := dagLevels(&d); lv["charge"] != 0 || lv["ship"] != 1 || lv["notify"] != 2 {
		t.Fatalf("levels = %+v, want charge:0 ship:1 notify:2", lv)
	}
}

// TestWorkflowStepNameWithComma: a step name containing the old delimiter is
// preserved end-to-end now that deps are a JSON array.
func TestWorkflowStepNameWithComma(t *testing.T) {
	q := newTestQ(t)
	ran := false
	dependent := NewTask("after-fn", func(ctx context.Context, in greetIn) (greetOut, error) {
		ran = true
		return greetOut{}, nil
	})
	wf := NewWorkflow[greetIn]("comma",
		Step("a,b", dullTask("comma-fn")),
		Step("after", dependent, "a,b"),
	)
	h, err := EnqueueWorkflow[greetOut](context.Background(), q, wf, greetIn{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Result(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !ran {
		t.Fatal("dependent on a comma-named step never ran")
	}
	d, err := q.DAG(context.Background(), h.RunID())
	if err != nil {
		t.Fatal(err)
	}
	if len(d.Edges) != 1 || d.Edges[0].From != "a,b" || d.Edges[0].To != "after" {
		t.Fatalf("edges = %+v, want a,b -> after", d.Edges)
	}
}

// TestDAGSVG: the SVG renders, is well-formed XML, and labels the nodes.
func TestDAGSVG(t *testing.T) {
	q := newTestQ(t)
	h := runWorkflow(t, q)

	svg, err := q.DAGSVG(context.Background(), h.RunID())
	if err != nil {
		t.Fatal(err)
	}
	s := string(svg)
	if !strings.HasPrefix(s, "<svg") || !strings.Contains(s, "</svg>") {
		t.Fatalf("not an svg document: %.60q", s)
	}
	for _, want := range []string{"charge", "ship", "notify", "charge-fn", "qarrow", "SUCCEEDED"} {
		if !strings.Contains(s, want) {
			t.Fatalf("svg missing %q", want)
		}
	}
	var anyDoc struct {
		XMLName xml.Name
	}
	if err := xml.Unmarshal(svg, &anyDoc); err != nil {
		t.Fatalf("svg is not well-formed XML: %v", err)
	}
	if anyDoc.XMLName.Local != "svg" {
		t.Fatalf("root element = %q", anyDoc.XMLName.Local)
	}
}

// TestDAGSVGEscapesLabels: XML-sensitive characters in a step name are escaped.
func TestDAGSVGEscapesLabels(t *testing.T) {
	q := newTestQ(t)
	wf := NewWorkflow[greetIn]("esc",
		Step("a&b<c>", dullTask("esc-fn")),
	)
	h, err := EnqueueWorkflow[greetOut](context.Background(), q, wf, greetIn{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Result(context.Background()); err != nil {
		t.Fatal(err)
	}
	svg, err := q.DAGSVG(context.Background(), h.RunID())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(svg), "a&amp;b&lt;c&gt;") {
		t.Fatalf("label not escaped: %s", svg)
	}
}

// TestDAGReflectsState: a suspended step shows SUSPENDED in the snapshot.
func TestDAGReflectsState(t *testing.T) {
	q := newTestQ(t)
	task := NewTask("dag-wait", func(ctx context.Context, in greetIn) (greetOut, error) {
		_, err := WaitFor[greetIn](ctx, "dag.go", 5*time.Second)
		return greetOut{}, err
	})
	h, _ := Enqueue(context.Background(), q, task, greetIn{})
	waitSuspended(t, q, h.RunID())

	d, err := q.DAG(context.Background(), h.RunID())
	if err != nil {
		t.Fatal(err)
	}
	if len(d.Nodes) != 1 || d.Nodes[0].Status != StatusSuspended {
		t.Fatalf("dag node = %+v, want one SUSPENDED", d.Nodes)
	}
	if _, err := q.Emit(context.Background(), "dag.go", greetIn{}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Result(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// TestDAGSingleTaskAndUnknown: a single-task run is one node with no edges,
// and an unknown run is ErrNotFound.
func TestDAGSingleTaskAndUnknown(t *testing.T) {
	q := newTestQ(t)
	task := dullTask("solo")
	h, _ := Enqueue(context.Background(), q, task, greetIn{})
	if _, err := h.Result(context.Background()); err != nil {
		t.Fatal(err)
	}
	d, err := q.DAG(context.Background(), h.RunID())
	if err != nil {
		t.Fatal(err)
	}
	if len(d.Nodes) != 1 || len(d.Edges) != 0 || d.Nodes[0].Name != "solo" {
		t.Fatalf("single-task dag = %+v", d)
	}
	if _, err := q.DAG(context.Background(), "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown run err = %v, want ErrNotFound", err)
	}
}
