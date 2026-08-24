package goqueue

import (
	"math"
	"time"
)

// RetryBackoff controls the delay inserted between retry attempts. After a
// failed attempt the job is re-scheduled to run again after the computed
// delay, so repeated failures spread out instead of hammering the backend.
//
// The zero value is usable: Delay always returns 0, which preserves the old
// "re-queue immediately" behavior.
type RetryBackoff struct {
	// InitialInterval is the delay before the first retry.
	InitialInterval time.Duration
	// MaxInterval caps the delay growth. Zero means no cap.
	MaxInterval time.Duration
	// Multiplier grows the interval geometrically per retry (e.g. 2.0
	// doubles the delay). Values <= 1 act as a fixed interval.
	Multiplier float64
}

// DefaultRetryBackoff returns the recommended schedule: 100ms, 200ms,
// 400ms, ... doubling each retry, capped at 30s. The same defaults used by
// asynq, so behavior is familiar to users migrating.
func DefaultRetryBackoff() RetryBackoff {
	return RetryBackoff{
		InitialInterval: 100 * time.Millisecond,
		MaxInterval:     30 * time.Second,
		Multiplier:      2.0,
	}
}

// Delay returns how long to wait before the n-th retry (1-based: the first
// retry waits InitialInterval). retry values <= 0 are treated as 1. The
// result never exceeds MaxInterval (when set) and is deterministic — no
// jitter — so tests and backoff plots are reproducible.
func (b RetryBackoff) Delay(retry int) time.Duration {
	if retry < 1 {
		retry = 1
	}
	interval := float64(b.InitialInterval)
	if interval <= 0 {
		return 0
	}
	multiplier := b.Multiplier
	if multiplier <= 1 {
		multiplier = 1
	}
	interval *= math.Pow(multiplier, float64(retry-1))
	if max := float64(b.MaxInterval); max > 0 && interval > max {
		interval = max
	}
	return time.Duration(interval)
}
