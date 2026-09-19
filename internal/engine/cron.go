package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	cronlib "github.com/robfig/cron/v3"

	"github.com/kartikbazzad/quacker/internal/store"
)

// everySchedule is a fixed-delay schedule used for "@every <d>". It exists
// because cron.ConstantDelaySchedule.Next subtracts t.Nanosecond(), which
// computes wrong (even backwards) next times for delays below one second.
type everySchedule struct{ d time.Duration }

func (s everySchedule) Next(t time.Time) time.Time { return t.Add(s.d) }

type cronSchedule struct{ s cronlib.Schedule }

// parseCron parses a spec. "@every <duration>" is handled here so sub-second
// intervals work; everything else goes through the standard parser.
func parseCron(spec string) (cronSchedule, error) {
	const every = "@every "
	if strings.HasPrefix(spec, every) {
		d, err := time.ParseDuration(strings.TrimSpace(spec[len(every):]))
		if err != nil {
			return cronSchedule{}, fmt.Errorf("quacker: invalid @every duration in %q: %w", spec, err)
		}
		if d <= 0 {
			return cronSchedule{}, fmt.Errorf("quacker: @every duration must be positive in %q", spec)
		}
		return cronSchedule{everySchedule{d}}, nil
	}
	s, err := cronlib.ParseStandard(spec)
	if err != nil {
		return cronSchedule{}, fmt.Errorf("quacker: invalid cron spec %q: %w", spec, err)
	}
	return cronSchedule{s}, nil
}

// RegisterCron registers (or replaces) a cron trigger that enqueues task def
// with input on the schedule. The trigger is persisted, so with File storage
// it is re-armed on the next Start as soon as the task is registered.
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
		name:  name,
		spec:  spec,
		task:  def.Name,
		sched: sched,
		input: input,
		next:  sched.s.Next(now),
	}
	e.mu.Lock()
	delete(e.pendingCrons, name) // a fresh registration supersedes a loaded one
	e.crons[name] = entry
	e.mu.Unlock()

	_ = e.st.UpsertCron(context.Background(), &store.Cron{
		Name: name, Spec: spec, Task: def.Name, Input: input,
		NextAt: entry.next.UnixNano(), CreatedAt: now.UnixNano(),
	})
	return nil
}

// RemoveCron deletes a cron trigger, whether armed or still pending.
func (e *Engine) RemoveCron(name string) error {
	e.mu.Lock()
	delete(e.crons, name)
	delete(e.pendingCrons, name)
	e.mu.Unlock()
	return e.st.DeleteCron(context.Background(), name)
}

// Crons lists registered cron triggers with their next fire time. Crons
// loaded from the store but not yet armed (their task is not registered in
// this process) are included with their persisted next time.
func (e *Engine) Crons() []CronInfo {
	e.mu.Lock()
	out := make([]CronInfo, 0, len(e.crons)+len(e.pendingCrons))
	for name, c := range e.crons {
		out = append(out, CronInfo{Name: name, Spec: c.spec, Task: c.task, Next: c.next})
	}
	for name, c := range e.pendingCrons {
		out = append(out, CronInfo{Name: name, Spec: c.spec, Task: c.task, Next: c.next})
	}
	e.mu.Unlock()
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// loadPersistedCrons moves every stored cron into the pending map. Nothing is
// armed until its task is registered.
func (e *Engine) loadPersistedCrons() {
	rows, err := e.st.ListCrons(context.Background())
	if err != nil {
		e.log.Error("quacker: load crons", "err", err)
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, c := range rows {
		if _, exists := e.crons[c.Name]; exists {
			continue
		}
		sched, err := parseCron(c.Spec)
		if err != nil {
			e.log.Warn("quacker: skipping unparseable persisted cron", "cron", c.Name, "spec", c.Spec, "err", err)
			continue
		}
		next := time.Unix(0, c.NextAt)
		if c.NextAt <= 0 {
			next = time.Time{}
		}
		e.pendingCrons[c.Name] = &cronEntry{
			name: c.Name, spec: c.Spec, task: c.Task, sched: sched, input: c.Input, next: next,
		}
	}
}

// takePendingCronsLocked arms the pending crons whose task just registered,
// resuming the stored cadence: a next time still in the future is kept, and a
// missed one is recomputed from now (skip missed, no catch-up burst). Called
// with e.mu held.
func (e *Engine) takePendingCronsLocked(task string) []*cronEntry {
	var armed []*cronEntry
	now := e.now()
	for name, c := range e.pendingCrons {
		if c.task != task {
			continue
		}
		if c.next.IsZero() || !c.next.After(now) {
			c.next = c.sched.s.Next(now)
		}
		e.crons[name] = c
		armed = append(armed, c)
		delete(e.pendingCrons, name)
	}
	return armed
}

// persistArmedCrons writes the recomputed next times back (best-effort).
func (e *Engine) persistArmedCrons(crons []*cronEntry) {
	for _, c := range crons {
		_ = e.st.UpsertCron(context.Background(), &store.Cron{
			Name: c.name, Spec: c.spec, Task: c.task, Input: c.input, NextAt: c.next.UnixNano(),
		})
	}
}

const (
	cronMaxWait = 200 * time.Millisecond
	cronMinWait = time.Millisecond
)

// cronLoop fires due crons. It waits until the earliest armed cron is due
// (capped at 200ms) rather than ticking on a fixed interval, so sub-second
// schedules fire accurately without a busy loop.
func (e *Engine) cronLoop() {
	timer := time.NewTimer(cronMaxWait)
	defer timer.Stop()
	for {
		select {
		case <-e.ctx.Done():
			return
		case <-timer.C:
			e.cronTick()
			timer.Reset(e.nextCronWait())
		}
	}
}

func (e *Engine) nextCronWait() time.Duration {
	now := e.now()
	wait := cronMaxWait
	e.mu.Lock()
	for _, c := range e.crons {
		if d := c.next.Sub(now); d < wait {
			wait = d
		}
	}
	e.mu.Unlock()
	if wait < cronMinWait {
		wait = cronMinWait
	}
	return wait
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
