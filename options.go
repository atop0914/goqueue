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
	// OnDead is invoked (synchronously, from the worker goroutine) when a
	// job exhausts its retries and moves to the DLQ. It may be nil.
	OnDead func(JobInfo)
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
// dead-letter set.
func WithOnDead(fn func(JobInfo)) Option {
	return func(c *Config) { c.OnDead = fn }
}
