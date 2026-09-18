package engine

import (
	"math/rand/v2"
	"time"
)

// Backoff configures retry delays. Zero fields fall back to the defaults
// documented per field.
type Backoff struct {
	// Base is the delay before the first retry. Default 500ms.
	Base time.Duration
	// Factor is the exponential growth multiplier. Default 2.
	Factor float64
	// Max caps the delay. Default 30s.
	Max time.Duration
	// Jitter in [0,1] randomizes the delay upward by up to that fraction to
	// avoid retry thundering herds. Zero means no jitter; the 0.1 default is
	// applied at the task layer (quacker.Exponential/defaultBackoff).
	Jitter float64
}

func (b Backoff) withDefaults() Backoff {
	if b.Base <= 0 {
		b.Base = 500 * time.Millisecond
	}
	if b.Factor <= 0 {
		b.Factor = 2
	}
	if b.Max <= 0 {
		b.Max = 30 * time.Second
	}
	if b.Jitter < 0 {
		b.Jitter = 0
	}
	return b
}

// Delay returns the wait before retry number attempt (1-based).
func (b Backoff) Delay(attempt int, now time.Time) time.Duration {
	b = b.withDefaults()
	if attempt < 1 {
		attempt = 1
	}
	d := b.Base
	for i := 1; i < attempt; i++ {
		d = time.Duration(float64(d) * b.Factor)
		if d >= b.Max {
			d = b.Max
			break
		}
	}
	if d > b.Max {
		d = b.Max
	}
	if b.Jitter > 0 {
		d = time.Duration(float64(d) * (1 + b.Jitter*rand.Float64()))
	}
	return d
}
