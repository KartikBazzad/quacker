package quacker

import "github.com/kartikbazzad/quacker/internal/engine"

// StepInfo describes a step about to run (or that just ran).
type StepInfo = engine.StepInfo

// EnqueueInfo describes a run about to be enqueued.
type EnqueueInfo = engine.EnqueueInfo

// RunInfo describes a run that reached a terminal status.
type RunInfo = engine.RunInfo

// EmitInfo describes an event about to be emitted.
type EmitInfo = engine.EmitInfo

// Hooks is the set of engine lifecycle callbacks a plugin may provide; only
// the non-nil fields are invoked. Before* hooks run in registration order and
// may return an error to veto; After* hooks are observe-only, run in reverse
// order, and never change the outcome (their panics are recovered and logged).
//
// A step veto (BeforeStep returning an error) fails the step immediately with
// no retries. Hooks are invoked concurrently for parallel steps, so a plugin
// must be safe for concurrent use. In-process plugins are trusted code.
type Hooks = engine.Hooks

// Plugin contributes lifecycle hooks to an engine.
type Plugin interface {
	// Name identifies the plugin (for logging/diagnostics).
	Name() string
	// Hooks returns the callbacks to install; leave fields nil to ignore them.
	Hooks() Hooks
}

// WithPlugin registers a plugin's hooks with the engine. Plugins are invoked
// in registration order (Before* forward, After* reverse). It is the
// compile-time extension point: implement Plugin in your own package and pass
// it here — no dynamic loading, no out-of-process server.
func WithPlugin(p Plugin) Option {
	return func(c *config) {
		if p == nil {
			return
		}
		c.plugins = append(c.plugins, p.Hooks())
	}
}
