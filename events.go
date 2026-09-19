package quacker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// On binds an event name to a task: every Emit of that event enqueues one run
// of t with the event payload as the task input. The binding is persisted, so
// with File storage it re-arms on the next Open once the task is registered
// (typically via Register). Registering the same (event, task) pair again is
// idempotent and refreshes the task definition.
func On[I any, O any](q *Quacker, event string, t *Task[I, O]) error {
	return q.eng.RegisterEvent(event, t.toDef())
}

// Off removes an event→task binding. Removing an unknown binding is a no-op.
func Off[I any, O any](q *Quacker, event string, t *Task[I, O]) error {
	return q.eng.RemoveEvent(event, t.name)
}

// Emit persists an event and dispatches it to every bound task, one run each,
// returning how many runs were enqueued. payload is marshaled as JSON (a
// json.RawMessage is used as-is) and becomes each task's input; a payload
// that does not decode to a bound task's input type fails that run at
// execution, like any other input. Delivery is best-effort and in-process:
// unbound events still persist, but a crash between the persist and the
// enqueues can lose those dispatches.
func (q *Quacker) Emit(ctx context.Context, event string, payload any) (int, error) {
	raw, err := q.marshalEventPayload(payload)
	if err != nil {
		return 0, err
	}
	return q.eng.Emit(ctx, event, raw)
}

func (q *Quacker) marshalEventPayload(payload any) (json.RawMessage, error) {
	b, err := q.eng.Codec().Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("quacker: marshal event payload: %w", err)
	}
	return b, nil
}

// EventRecord is one persisted emit, returned by Events.
type EventRecord struct {
	Name    string          `json:"name"`
	Payload json.RawMessage `json:"payload,omitempty"`
	At      time.Time       `json:"at"`
}

// Events lists up to limit recent emitted events, newest first.
func (q *Quacker) Events(ctx context.Context, limit int) ([]EventRecord, error) {
	evs, err := q.st.ListEvents(ctx, limit)
	if err != nil {
		return nil, err
	}
	out := make([]EventRecord, 0, len(evs))
	for _, e := range evs {
		out = append(out, EventRecord{Name: e.Name, Payload: e.Payload, At: unixToTime(e.CreatedAt)})
	}
	return out, nil
}
