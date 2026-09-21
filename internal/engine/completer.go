package engine

import (
	"context"

	"github.com/kartikbazzad/quacker/internal/store"
)

// completionReq is one step success awaiting persistence, plus the step name
// needed to publish the transition.
type completionReq struct {
	comp store.Completion
	name string
}

// submitCompletion hands a step success to the batched completer. Once the
// completer has stopped (engine shutdown), it writes through directly so a
// late finisher is never dropped.
func (e *Engine) submitCompletion(c completionReq) {
	e.cmplMu.Lock()
	if e.cmplStopped {
		e.cmplMu.Unlock()
		e.completeDirect(c)
		return
	}
	e.cmplPending = append(e.cmplPending, c)
	e.cmplMu.Unlock()
	select {
	case e.cmplWake <- struct{}{}:
	default: // a wake is already pending; the completer will drain everything
	}
}

// completerLoop batches step completions into one transaction per flush. It is
// not part of loopWG: it only stops on cmplStop, which Close signals after all
// executions have finished.
func (e *Engine) completerLoop() {
	for {
		select {
		case <-e.cmplWake:
			e.flushCompletions()
		case <-e.cmplStop:
			e.cmplMu.Lock()
			e.cmplStopped = true
			e.cmplMu.Unlock()
			e.flushCompletions() // drain anything that raced the stop
			close(e.cmplDone)
			return
		}
	}
}

// stopCompleter flips submits to direct writes, drains pending completions, and
// waits for the completer to exit. Safe to call more than once.
func (e *Engine) stopCompleter() {
	if !e.cmplStarted.Load() {
		return
	}
	select {
	case <-e.cmplStop:
		return // already stopping/stopped
	default:
	}
	close(e.cmplStop)
	<-e.cmplDone
}

// flushCompletions records every pending completion in one transaction, then
// does the publish/finish work for each. It uses a background context so
// completions land even while the engine is shutting down.
func (e *Engine) flushCompletions() {
	e.cmplMu.Lock()
	if len(e.cmplPending) == 0 {
		e.cmplMu.Unlock()
		return
	}
	batch := e.cmplPending
	e.cmplPending = nil
	e.cmplMu.Unlock()

	ctx := context.Background()
	comps := make([]store.Completion, len(batch))
	for i, c := range batch {
		comps[i] = c.comp
		comps[i].Name = c.name
	}
	results, err := e.st.CompleteSteps(ctx, comps)
	if err != nil {
		// A batch is atomic; on failure fall back to one-by-one so a single
		// bad completion does not drop the rest.
		for _, c := range batch {
			e.completeDirect(c)
		}
		return
	}
	for i, c := range batch {
		e.afterComplete(c, results[i])
	}
}

// completeDirect records one completion outside the batcher.
func (e *Engine) completeDirect(c completionReq) {
	res, err := e.st.CompleteStep(context.Background(), c.comp.StepID, c.comp.RunID, c.name, c.comp.Output, c.comp.Now)
	if err != nil {
		e.log.Error("quacker: record success", "run", c.comp.RunID, "step", c.name, "err", err)
		return
	}
	e.afterComplete(c, res)
}

// afterComplete publishes a step's success and advances its run — the work the
// executor previously did inline after CompleteStep.
func (e *Engine) afterComplete(c completionReq, res store.CompleteResult) {
	if !res.StepDone {
		// Another path already made the step terminal; publish that instead.
		e.publish(c.comp.RunID, c.name, store.StatusRunning, store.StatusCancelled, "", c.comp.Now)
		e.wakeSlot()
		return
	}
	e.publish(c.comp.RunID, c.name, store.StatusRunning, store.StatusSucceeded, "", c.comp.Now)
	for _, name := range res.ReadySteps {
		e.publish(c.comp.RunID, name, store.StatusBlocked, store.StatusQueued, "", c.comp.Now)
	}
	if res.RunTerminal {
		e.finishRun(c.comp.RunID, res.RunStatus, res.RunOutput, res.RunError, c.comp.Now)
	}
	// Wake after the completion is persisted. Unblocked dependents are urgent
	// (a workflow's next step must be claimed now); a plain freed slot is not
	// (the scheduler paces those while busy).
	if len(res.ReadySteps) > 0 {
		e.wakeScheduler()
	} else {
		e.wakeSlot()
	}
}
