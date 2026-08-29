package goqueue

import (
	"context"
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
	// MaxConcurrency caps the number of handler invocations running at the
	// same time across the whole pool. Zero (the default) means unlimited:
	// every worker may run its handler when it dequeues a job. When set to
	// a value smaller than Workers, the surplus workers block on the
	// semaphore until a slot frees up.
	MaxConcurrency int
	// DrainOnShutdown makes Shutdown process every job that is already
	// enqueued (including delayed ones) before returning, instead of the
	// default behavior of only waiting for the handlers currently running
	// and leaving the rest pending for the next Start.
	DrainOnShutdown bool
	// ContextDecorator, when non-nil, wraps the context handed to every
	// handler invocation. Set via WithContextDecorator; see that function
	// for details.
	ContextDecorator func(ctx context.Context) context.Context
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

// WithMaxConcurrency caps how many handler invocations may run at the same
// time across the whole worker pool. It is useful when handlers contend for
// a limited external resource (a database connection pool, an upstream API
// quota, ...) even though workers themselves are cheap goroutines. Zero
// means unlimited; a value below WithWorkers makes the surplus workers wait
// for a free slot before running a handler.
func WithMaxConcurrency(n int) Option {
	return func(c *Config) {
		if n > 0 {
			c.MaxConcurrency = n
		}
	}
}

// WithDrainOnShutdown switches Shutdown into drain mode: instead of leaving
// enqueued jobs for the next Start, Shutdown keeps the workers consuming the
// queue until every pending job (including delayed ones) has been processed,
// and only then returns. Combined with an at-least-once backend this gives a
// "finish what was submitted before we stop" guarantee. Note that jobs
// enqueued concurrently while draining are not part of the guarantee.
func WithDrainOnShutdown(drain bool) Option {
	return func(c *Config) {
		if drain {
			c.DrainOnShutdown = true
		}
	}
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

// WithContextDecorator registers a decorator applied to the context handed
// to every handler invocation, after the per-attempt timeout and job-ID
// values are attached. It is the integration point for observability
// packages that propagate tracing context into handlers (see
// obs/tracing.WithTracing) without the core depending on any
// observability library. Nil or nil-returning decorators are ignored.
func WithContextDecorator(dec func(ctx context.Context) context.Context) Option {
	return func(c *Config) { c.ContextDecorator = dec }
}
