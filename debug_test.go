package quacker

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestDebugLogsCaptureEngineActivity(t *testing.T) {
	q := newTestQ(t)
	var mu sync.Mutex
	var msgs []string
	go func() {
		for rec := range q.DebugLogs() {
			mu.Lock()
			msgs = append(msgs, rec.Message)
			mu.Unlock()
		}
	}()
	task := NewTask("dbg", func(ctx context.Context, in greetIn) (greetOut, error) {
		return greetOut{}, nil
	})
	h, err := Enqueue(context.Background(), q, task, greetIn{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Result(context.Background()); err != nil {
		t.Fatal(err)
	}
	has := func(want string) bool {
		mu.Lock()
		defer mu.Unlock()
		for _, m := range msgs {
			if m == want {
				return true
			}
		}
		return false
	}
	waitFor(t, 3*time.Second, func() bool {
		return has("quacker: claimed step") && has("quacker: run finished")
	})
}

func TestDebugLoggerUserLines(t *testing.T) {
	q := newTestQ(t)
	got := make(chan DebugRecord, 1)
	go func() {
		for rec := range q.DebugLogs() {
			if rec.Message == "hello from app" {
				got <- rec
			}
		}
	}()
	q.DebugLogger().Info("hello from app", "k", "v")
	select {
	case rec := <-got:
		if rec.Level != "INFO" || rec.Attrs["k"] != "v" {
			t.Fatalf("record = %+v", rec)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("DebugLogger line never reached the debug stream")
	}
}

func TestDebugLogsCloseOnClose(t *testing.T) {
	q, err := Open(WithStorage(Memory()), WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	closed := make(chan struct{})
	go func() {
		for range q.DebugLogs() {
		}
		close(closed)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := q.Close(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("DebugLogs did not close on Close")
	}
}
