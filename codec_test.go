package quacker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"
)

// taggedCodec proves the engine actually routes payloads through the codec: it
// prefixes every encoding and rejects anything without the tag on decode.
type taggedCodec struct{}

func (taggedCodec) Name() string { return "tagged" }

func (taggedCodec) Marshal(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return append([]byte("TAG1"), b...), nil
}

func (taggedCodec) Unmarshal(data []byte, v any) error {
	if !bytes.HasPrefix(data, []byte("TAG1")) {
		return errors.New("taggedCodec: missing tag")
	}
	return json.Unmarshal(data[4:], v)
}

// TestCustomCodecWorkflowAndDeps: a custom codec round-trips task input/output
// and DepOutput across a DAG.
func TestCustomCodecWorkflowAndDeps(t *testing.T) {
	q := newTestQ(t, WithCodec(taggedCodec{}))
	charge := NewTask("cc.charge", func(ctx context.Context, in greetIn) (greetOut, error) {
		return greetOut{Greeting: "hi " + in.Name}, nil
	})
	ship := NewTask("cc.ship", func(ctx context.Context, in greetIn) (greetOut, error) {
		rec, err := DepOutput[greetOut](ctx, "charge")
		if err != nil {
			return greetOut{}, err
		}
		return greetOut{Greeting: "ship " + rec.Greeting}, nil
	})
	wf := NewWorkflow[greetIn]("cc.flow",
		Step("charge", charge),
		Step("ship", ship, "charge"),
	)
	h, err := EnqueueWorkflow[greetOut](context.Background(), q, wf, greetIn{Name: "a"})
	if err != nil {
		t.Fatal(err)
	}
	out, err := h.Result(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if out.Greeting != "ship hi a" {
		t.Fatalf("output = %q, want %q", out.Greeting, "ship hi a")
	}
}

// TestCustomCodecEventsAndDurable: event payloads and durable journal values
// use the codec too.
func TestCustomCodecEventsAndDurable(t *testing.T) {
	q := newTestQ(t, WithCodec(taggedCodec{}))
	ctx := context.Background()
	task := NewTask("cc.durable", func(ctx context.Context, in greetIn) (greetOut, error) {
		res, err := RunOnce(ctx, "once", func() (string, error) { return "res", nil })
		if err != nil {
			return greetOut{}, err
		}
		p, err := WaitFor[greetIn](ctx, "cc.go", 5*time.Second)
		if err != nil {
			return greetOut{}, err
		}
		return greetOut{Greeting: res + " " + p.Name}, nil
	})
	h, err := Enqueue(ctx, q, task, greetIn{})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 3*time.Second, func() bool {
		s, err := q.Execution(ctx, h.RunID())
		return err == nil && len(s.Steps) == 1 && s.Steps[0].WaitEvent == "cc.go"
	})
	if _, err := q.Emit(ctx, "cc.go", greetIn{Name: "e"}); err != nil {
		t.Fatal(err)
	}
	out, err := h.Result(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if out.Greeting != "res e" {
		t.Fatalf("output = %q, want %q", out.Greeting, "res e")
	}
}

// TestCustomCodecKey: WithKey decodes through the codec.
func TestCustomCodecKey(t *testing.T) {
	q := newTestQ(t, WithCodec(taggedCodec{}))
	var mu sync.Mutex
	inFlight, maxInFlight := 0, 0
	task := NewTask("cc.key", func(ctx context.Context, in greetIn) (greetOut, error) {
		mu.Lock()
		inFlight++
		if inFlight > maxInFlight {
			maxInFlight = inFlight
		}
		mu.Unlock()
		time.Sleep(20 * time.Millisecond)
		mu.Lock()
		inFlight--
		mu.Unlock()
		return greetOut{}, nil
	}, WithKey(func(in greetIn) string { return in.Name }))

	var hs []*RunHandle[greetOut]
	for i := 0; i < 3; i++ {
		h, err := Enqueue(context.Background(), q, task, greetIn{Name: "same"})
		if err != nil {
			t.Fatal(err)
		}
		hs = append(hs, h)
	}
	for _, h := range hs {
		if _, err := h.Result(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if maxInFlight != 1 {
		t.Fatalf("max in-flight = %d, want 1 (per-key serialization under a custom codec)", maxInFlight)
	}
}

// TestDefaultCodec: no WithCodec means JSON.
func TestDefaultCodec(t *testing.T) {
	if name := (JSONCodec{}).Name(); name != "json" {
		t.Fatalf("default codec name = %q, want json", name)
	}
}
