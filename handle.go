package quacker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/kartikbazzad/quacker/internal/engine"
	"github.com/kartikbazzad/quacker/internal/store"
)

// RunError is returned by RunHandle.Result when a run fails.
type RunError struct {
	RunID   string
	Status  Status
	Message string
}

func (e *RunError) Error() string {
	return fmt.Sprintf("quacker: run %s failed: %s", e.RunID, e.Message)
}

// Sentinel errors surfaced by Result and Cancel.
var (
	// ErrRunCancelled is returned by Result when the run was cancelled.
	ErrRunCancelled = engine.ErrRunCancelled
	// ErrRunInterrupted is returned by Result when the engine shut down
	// before the run finished.
	ErrRunInterrupted = engine.ErrRunInterrupted
	// ErrNotFound is returned by Execution for unknown run IDs.
	ErrNotFound = store.ErrNotFound
	// ErrClosed is returned by enqueue calls after Close has begun.
	ErrClosed = engine.ErrClosed
)

// RunHandle tracks an enqueued run.
type RunHandle[O any] struct {
	runID string
	w     *engine.Waiter
}

// RunID returns the run's stable identifier.
func (h *RunHandle[O]) RunID() string { return h.runID }

// Done returns a channel closed when the run reaches a terminal status.
func (h *RunHandle[O]) Done() <-chan struct{} { return h.w.Done() }

// Result waits for the run to finish and decodes its output. A failed run
// returns a *RunError; a cancelled run returns ErrRunCancelled.
func (h *RunHandle[O]) Result(ctx context.Context) (O, error) {
	var out O
	status, raw, err := h.w.Wait(ctx)
	if err != nil {
		var re *engine.RunError
		if errors.As(err, &re) {
			return out, &RunError{RunID: re.RunID, Status: StatusFailed, Message: re.Msg}
		}
		return out, err
	}
	if status == store.StatusSucceeded && len(raw) > 0 {
		if err := json.Unmarshal(raw, &out); err != nil {
			return out, fmt.Errorf("quacker: decode output of run %s: %w", h.runID, err)
		}
	}
	return out, nil
}
