// Package backoff computes how long to wait before retrying a failed job.
package backoff

import (
	"math/rand/v2"
	"time"
)

// Exponential doubles the delay after each failed attempt, up to Max:
//
//	attempt 1 -> Base, attempt 2 -> 2*Base, attempt 3 -> 4*Base, ...
//
// Jitter randomizes each delay so that many jobs which failed at the same
// moment (for example, because a database was briefly down) do not all
// retry at the same moment and cause the same outage again.
type Exponential struct {
	Base   time.Duration
	Max    time.Duration
	Jitter bool

	// Rand returns a number in [0, 1). Nil means math/rand/v2.Float64.
	// Tests replace it to get predictable delays.
	Rand func() float64
}

// Default is the policy the worker uses: 1s, 2s, 4s, ... capped at 1h, with jitter.
var Default = Exponential{Base: time.Second, Max: time.Hour, Jitter: true}

// Delay returns the wait before the next run, given how many attempts
// have already failed (1 for the first failure).
func (e Exponential) Delay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}

	d := e.Base
	for i := 1; i < attempt; i++ {
		d *= 2
		// Stop as soon as we reach the cap. This also prevents the
		// duration from overflowing when attempt is very large.
		if d >= e.Max || d <= 0 {
			d = e.Max
			break
		}
	}
	if d > e.Max {
		d = e.Max
	}

	if !e.Jitter {
		return d
	}
	// "Equal jitter": half fixed, half random. The delay is always at
	// least d/2, so retries never happen immediately.
	r := e.Rand
	if r == nil {
		r = rand.Float64
	}
	half := d / 2
	return half + time.Duration(r()*float64(d-half))
}
