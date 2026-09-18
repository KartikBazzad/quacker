// Package bus is a tiny in-process pub/sub for run state transitions.
package bus

import "sync"

// Event is a single status transition.
type Event struct {
	RunID string
	Step  string // empty for run-level transitions
	From  string
	To    string
	At    int64 // unix nanos
	Error string
}

// Bus delivers events to subscribers. Slow subscribers drop events rather
// than block publishers (the store remains the source of truth; a snapshot
// via Execution always reflects reality).
type Bus struct {
	mu   sync.RWMutex
	subs map[*sub]struct{}
}

type sub struct {
	runID string // empty = all events
	ch    chan Event
}

func New() *Bus {
	return &Bus{subs: map[*sub]struct{}{}}
}

// Subscribe returns a channel receiving events for runID (empty string = all
// runs) and a cancel function that releases the subscription and closes the
// channel. The channel is buffered; on overflow events are dropped, never
// blocking the engine.
func (b *Bus) Subscribe(runID string, buffer int) (<-chan Event, func()) {
	if buffer <= 0 {
		buffer = 256
	}
	s := &sub{runID: runID, ch: make(chan Event, buffer)}
	b.mu.Lock()
	b.subs[s] = struct{}{}
	b.mu.Unlock()
	cancel := func() {
		b.mu.Lock()
		_, existed := b.subs[s]
		delete(b.subs, s)
		b.mu.Unlock()
		if existed {
			// No publish can be in flight here: Publish takes the read lock.
			close(s.ch)
		}
	}
	return s.ch, cancel
}

// Publish delivers ev to all matching subscribers without blocking.
func (b *Bus) Publish(ev Event) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for s := range b.subs {
		if s.runID != "" && s.runID != ev.RunID {
			continue
		}
		select {
		case s.ch <- ev:
		default: // slow subscriber: drop
		}
	}
}
