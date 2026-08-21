package goqueue

import (
	"time"
)

// Defaults applied when options are left at their zero value.
const (
	// DefaultMaxRetry is applied to jobs whose MaxRetry is 0.
	DefaultMaxRetry = 3
	// DefaultWorkers is the worker pool size when WithWorkers is not set.
	DefaultWorkers = 4
	// DefaultPollInterval is how often a worker re-checks an empty queue
	// when using backends that cannot block efficiently.
	DefaultPollInterval = 50 * time.Millisecond
)

// Config holds the resolved Client configuration.
type Config struct {
	Queue        Queue
	Workers      int
	PollInterval time.Duration
	// RetryBackoff is the exponential backoff schedule applied between
	// retries. Defaults to DefaultRetryBackoff.
	RetryBackoff RetryBackoff
	// Hooks holds the optional job-lifecycle callbacks (see hooks.go).
	// Any nil callback is simply not fired.
	Hooks Hooks
	// OnDead is the legacy dead-letter callback. Deprecated: use
	// Hooks.OnDead via WithHooks. When both are set, Hooks.OnDead wins.
	OnDead func(JobInfo)
	// now is the clock used by time-dependent features (scheduler). Defaults
	// to time.Now. Mostly for tests.
	now func() time.Time
}

// Option configures a Client.
type Option func(*Config)

// WithQueue overrides the default in-memory backend. This is how callers
// plug in persistent backends (SQLite, Redis) provided as subpackages.
func WithQueue(q Queue) Option {
	return func(c *Config) { c.Queue = q }
}

// WithWorkers sets the number of concurrent handler goroutines.
func WithWorkers(n int) Option {
	return func(c *Config) {
		if n > 0 {
			c.Workers = n
		}
	}
}

// WithPollInterval sets how often workers re-check the queue when it is
// empty. Lower values reduce latency at the cost of CPU.
func WithPollInterval(d time.Duration) Option {
	return func(c *Config) {
		if d > 0 {
			c.PollInterval = d
		}
	}
}

// WithOnDead registers a callback invoked when a job is moved to the
// dead-letter set. The callback receives the full JobInfo snapshot (see
// Config.OnDead).
func WithOnDead(fn func(JobInfo)) Option {
	return func(c *Config) { c.OnDead = fn }
}

// WithRetryBackoff sets the exponential backoff schedule used between
// retry attempts. Leave unset to use DefaultRetryBackoff.
func WithRetryBackoff(b RetryBackoff) Option {
	return func(c *Config) { c.RetryBackoff = b }
}

// WithClock overrides the scheduler's clock. Leave unset to use time.Now.
// Provided for deterministic tests of scheduled tasks.
func WithClock(now func() time.Time) Option {
	return func(c *Config) {
		if now != nil {
			c.now = now
		}
	}
}
