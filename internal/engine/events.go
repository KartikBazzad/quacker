package engine

import (
	"context"
	"encoding/json"
	"errors"

	"go.opentelemetry.io/otel/attribute"

	"github.com/kartikbazzad/quacker/internal/store"
)

// RegisterEvent binds an event name to a task: every Emit of that event
// enqueues one run of def with the event payload as input. The binding is
// persisted, so with File storage it re-arms on the next Start once the task
// is registered. Re-registering the same (event, task) is idempotent and
// refreshes the task definition.
func (e *Engine) RegisterEvent(event string, def *TaskDef) error {
	if event == "" {
		return errors.New("quacker: event name required")
	}
	if def == nil {
		return ErrUnknownTask
	}
	e.RegisterTask(def)
	e.mu.Lock()
	updated := false
	for _, s := range e.eventSubs[event] {
		if s.task == def.Name {
			s.def = def
			updated = true
			break
		}
	}
	if !updated {
		e.eventSubs[event] = append(e.eventSubs[event], &eventSub{event: event, task: def.Name, def: def})
	}
	e.mu.Unlock()
	return e.st.UpsertEventSub(context.Background(), &store.EventSub{
		Event: event, Task: def.Name, CreatedAt: e.now().UnixNano(),
	})
}

// RemoveEvent deletes an event→task binding, armed or pending.
func (e *Engine) RemoveEvent(event, task string) error {
	e.mu.Lock()
	subs := e.eventSubs[event]
	for i, s := range subs {
		if s.task == task {
			e.eventSubs[event] = append(subs[:i], subs[i+1:]...)
			break
		}
	}
	pending := e.pendingSubs[task]
	for i, s := range pending {
		if s.event == event {
			e.pendingSubs[task] = append(pending[:i], pending[i+1:]...)
			break
		}
	}
	if len(e.pendingSubs[task]) == 0 {
		delete(e.pendingSubs, task)
	}
	if len(e.eventSubs[event]) == 0 {
		delete(e.eventSubs, event)
	}
	e.mu.Unlock()
	return e.st.DeleteEventSub(context.Background(), event, task)
}

// Emit records an event and dispatches it: it persists the event and wakes
// every armed durable WaitFor on that event atomically, then enqueues one run
// per armed On binding. The enqueue half is best-effort in-process: a crash
// after the atomic wake but before the enqueues loses those run bindings
// (unbound events and event waits still deliver). It returns how many On
// binding runs were enqueued.
func (e *Engine) Emit(ctx context.Context, name string, payload json.RawMessage) (int, error) {
	if name == "" {
		return 0, errors.New("quacker: event name required")
	}
	if ctx == nil {
		ctx = e.ctx
	}
	ctx, span := e.startSpan(ctx, "quacker.emit", attribute.String("quacker.event", name))
	woken, err := e.st.DeliverEvent(ctx, name, payload, e.now().UnixNano())
	endSpan(span, err)
	if err != nil {
		return 0, err
	}
	if woken > 0 {
		e.wakeScheduler()
	}
	e.mu.RLock()
	subs := append([]*eventSub(nil), e.eventSubs[name]...)
	e.mu.RUnlock()

	n := 0
	var firstErr error
	for _, s := range subs {
		def := e.taskDef(s.task) // freshest registered definition
		if def == nil {
			continue
		}
		if _, err := e.Enqueue(ctx, &EnqueueRequest{
			Workflow: s.task,
			Kind:     store.KindTask,
			Input:    payload,
			Queue:    def.Queue,
			Steps:    []StepReq{{Name: def.Name, Def: def}},
		}); err != nil {
			if err == ErrClosed {
				return n, err
			}
			e.log.Error("quacker: event enqueue failed", "event", name, "task", s.task, "err", err)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		n++
	}
	return n, firstErr
}

// loadPersistedSubs moves every stored subscription into the pending map.
// Nothing is armed until its task is registered.
func (e *Engine) loadPersistedSubs() {
	rows, err := e.st.ListEventSubs(context.Background())
	if err != nil {
		e.log.Error("quacker: load event subscriptions", "err", err)
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, s := range rows {
		e.pendingSubs[s.Task] = append(e.pendingSubs[s.Task], &eventSub{event: s.Event, task: s.Task})
	}
}

// takePendingSubsLocked arms the subscriptions whose task just registered.
// Called with e.mu held.
func (e *Engine) takePendingSubsLocked(task string) []*eventSub {
	pending := e.pendingSubs[task]
	if len(pending) == 0 {
		return nil
	}
	delete(e.pendingSubs, task)
	for _, s := range pending {
		s.def = e.tasks[task]
		e.eventSubs[s.event] = append(e.eventSubs[s.event], s)
	}
	return pending
}

func (e *Engine) logArmedTriggers(task string, crons, subs int) {
	if crons == 0 && subs == 0 {
		return
	}
	e.log.Info("quacker: armed persisted triggers for task", "task", task, "crons", crons, "events", subs)
}
