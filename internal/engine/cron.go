package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	cronlib "github.com/robfig/cron/v3"

	"github.com/kartikbazzad/quacker/internal/store"
)

type cronSchedule struct{ s cronlib.Schedule }

func parseCron(spec string) (cronSchedule, error) {
	s, err := cronlib.ParseStandard(spec)
	if err != nil {
		return cronSchedule{}, fmt.Errorf("quacker: invalid cron spec %q: %w", spec, err)
	}
	return cronSchedule{s}, nil
}

// RegisterCron registers (or replaces) a cron trigger that enqueues task def
// with input on the schedule. Crons live in memory: after a restart the
// application must re-register them (typically at startup).
func (e *Engine) RegisterCron(name, spec string, def *TaskDef, input json.RawMessage) error {
	if name == "" {
		return fmt.Errorf("quacker: cron name required")
	}
	sched, err := parseCron(spec)
	if err != nil {
		return err
	}
	if def == nil {
		return ErrUnknownTask
	}
	e.RegisterTask(def)
	now := e.now()
	entry := &cronEntry{
		spec:  spec,
		task:  def.Name,
		sched: sched,
		input: input,
		next:  sched.s.Next(now),
	}
	e.mu.Lock()
	e.crons[name] = entry
	e.mu.Unlock()

	// Persist for introspection (q.Crons) and next-fire continuity across
	// restarts is handled by re-registration.
	_ = e.st.UpsertCron(context.Background(), &store.Cron{
		Name: name, Spec: spec, Task: def.Name, Input: input,
		NextAt: entry.next.UnixNano(), CreatedAt: now.UnixNano(),
	})
	return nil
}

// RemoveCron deletes a cron trigger.
func (e *Engine) RemoveCron(name string) error {
	e.mu.Lock()
	delete(e.crons, name)
	e.mu.Unlock()
	return e.st.DeleteCron(context.Background(), name)
}

// Crons lists registered cron triggers with their next fire time.
func (e *Engine) Crons() []CronInfo {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]CronInfo, 0, len(e.crons))
	for name, c := range e.crons {
		out = append(out, CronInfo{Name: name, Spec: c.spec, Task: c.task, Next: c.next})
	}
	return out
}

func (e *Engine) cronLoop() {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			e.cronTick()
		}
	}
}

func (e *Engine) cronTick() {
	if e.closing.Load() || e.ctx.Err() != nil {
		return
	}
	now := e.now()
	e.mu.Lock()
	var due []string
	for name, c := range e.crons {
		if !c.next.After(now) {
			due = append(due, name)
		}
	}
	e.mu.Unlock()

	for _, name := range due {
		e.mu.Lock()
		entry := e.crons[name]
		e.mu.Unlock()
		if entry == nil {
			continue
		}
		def := e.taskDef(entry.task)
		if def == nil {
			// Task not (yet) registered; retry shortly.
			e.mu.Lock()
			entry.next = now.Add(5 * time.Second)
			e.mu.Unlock()
			e.log.Warn("quacker: cron skipped: task not registered", "cron", name, "task", entry.task)
			continue
		}
		if _, err := e.Enqueue(e.ctx, &EnqueueRequest{
			Workflow: entry.task,
			Kind:     store.KindTask,
			Input:    entry.input,
			Queue:    def.Queue,
			Steps:    []StepReq{{Name: def.Name, Def: def}},
		}); err != nil && err != ErrClosed {
			e.log.Error("quacker: cron enqueue failed", "cron", name, "err", err)
		}
		next := entry.sched.s.Next(now)
		e.mu.Lock()
		entry.next = next
		e.mu.Unlock()
		_ = e.st.UpsertCron(context.Background(), &store.Cron{
			Name: name, Spec: entry.spec, Task: entry.task, Input: entry.input,
			NextAt: next.UnixNano(),
		})
	}
}
