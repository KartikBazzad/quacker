package engine

import (
	"context"
	"fmt"
)

// StepInfo describes a step about to run (or that just ran), with no internal
// store types leaked.
type StepInfo struct {
	RunID   string
	Step    string
	Task    string
	Queue   string
	Attempt int
	Key     string
	Labels  []string
}

// EnqueueInfo describes a run about to be enqueued.
type EnqueueInfo struct {
	Workflow string
	Kind     string
	Queue    string
	Priority int64
}

// RunInfo describes a run that reached a terminal status.
type RunInfo struct {
	RunID    string
	Workflow string
	Status   string
	Error    string
}

// EmitInfo describes an event about to be emitted.
type EmitInfo struct{ Event string }

// Hooks is the set of engine lifecycle callbacks a plugin may provide. Only
// non-nil fields are invoked. Before* callbacks run in registration order and
// may return an error to veto; After* callbacks are observe-only, run in
// reverse order, and never change the outcome (their panics are recovered and
// logged).
type Hooks struct {
	BeforeEnqueue func(ctx context.Context, e EnqueueInfo) (context.Context, error)
	AfterEnqueue  func(ctx context.Context, e EnqueueInfo, err error)
	BeforeStep    func(ctx context.Context, s StepInfo) (context.Context, error)
	AfterStep     func(ctx context.Context, s StepInfo, err error)
	OnRunFinished func(ctx context.Context, r RunInfo)
	BeforeEmit    func(ctx context.Context, e EmitInfo) (context.Context, error)
	AfterEmit     func(ctx context.Context, e EmitInfo, dispatched int, err error)
}

// callBefore runs a Before* hook with panic recovery, converting a panic into
// the veto error and preserving the incoming context.
func callBefore(ctx context.Context, f func() (context.Context, error)) (out context.Context, err error) {
	defer func() {
		if r := recover(); r != nil {
			out, err = ctx, fmt.Errorf("quacker: plugin hook panic: %v", r)
		}
	}()
	return f()
}

func (e *Engine) safeAfter(name string, f func()) {
	defer func() {
		if r := recover(); r != nil {
			e.log.Error("quacker: plugin hook panic (recovered)", "hook", name, "panic", fmt.Sprint(r))
		}
	}()
	f()
}

func (e *Engine) beforeEnqueue(ctx context.Context, info EnqueueInfo) (context.Context, error) {
	for _, h := range e.hookGroups {
		f := h.BeforeEnqueue
		if f == nil {
			continue
		}
		var err error
		ctx, err = callBefore(ctx, func() (context.Context, error) { return f(ctx, info) })
		if err != nil {
			return ctx, err
		}
	}
	return ctx, nil
}

func (e *Engine) afterEnqueue(ctx context.Context, info EnqueueInfo, err error) {
	for i := len(e.hookGroups) - 1; i >= 0; i-- {
		if f := e.hookGroups[i].AfterEnqueue; f != nil {
			e.safeAfter("AfterEnqueue", func() { f(ctx, info, err) })
		}
	}
}

func (e *Engine) beforeStep(ctx context.Context, info StepInfo) (context.Context, error) {
	for _, h := range e.hookGroups {
		f := h.BeforeStep
		if f == nil {
			continue
		}
		var err error
		ctx, err = callBefore(ctx, func() (context.Context, error) { return f(ctx, info) })
		if err != nil {
			return ctx, err
		}
	}
	return ctx, nil
}

func (e *Engine) afterStep(ctx context.Context, info StepInfo, err error) {
	for i := len(e.hookGroups) - 1; i >= 0; i-- {
		if f := e.hookGroups[i].AfterStep; f != nil {
			e.safeAfter("AfterStep", func() { f(ctx, info, err) })
		}
	}
}

func (e *Engine) onRunFinished(ctx context.Context, info RunInfo) {
	for i := len(e.hookGroups) - 1; i >= 0; i-- {
		if f := e.hookGroups[i].OnRunFinished; f != nil {
			e.safeAfter("OnRunFinished", func() { f(ctx, info) })
		}
	}
}

func (e *Engine) beforeEmit(ctx context.Context, info EmitInfo) (context.Context, error) {
	for _, h := range e.hookGroups {
		f := h.BeforeEmit
		if f == nil {
			continue
		}
		var err error
		ctx, err = callBefore(ctx, func() (context.Context, error) { return f(ctx, info) })
		if err != nil {
			return ctx, err
		}
	}
	return ctx, nil
}

func (e *Engine) afterEmit(ctx context.Context, info EmitInfo, dispatched int, err error) {
	for i := len(e.hookGroups) - 1; i >= 0; i-- {
		if f := e.hookGroups[i].AfterEmit; f != nil {
			e.safeAfter("AfterEmit", func() { f(ctx, info, dispatched, err) })
		}
	}
}
