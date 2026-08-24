package goqueue

import "time"

// Hooks is the collection of optional job-lifecycle callbacks. A Client
// with no hooks set behaves exactly like a Client with an empty Hooks.
//
// All callbacks receive a full JobInfo snapshot (ID, Type, State, Attempts,
// MaxRetry, Priority, LastError, EnqueuedAt, DeadAt). They are invoked
// synchronously on the goroutine that owns the lifecycle event:
//
//   - OnEnqueue: the caller's goroutine, right after the job is durably
//     enqueued (State=StatePending, Attempts=0, EnqueuedAt set).
//   - OnSuccess: the worker goroutine, after the job is acked.
//   - OnFailure: the worker goroutine, after every failed attempt
//     (State=StateFailed, LastError set).
//   - OnRetry: the worker goroutine, after a failed attempt that will be
//     retried (fired right after OnFailure).
//   - OnDead: the worker goroutine, after a failed attempt exhausts
//     MaxRetry and the job moves to the DLQ (State=StateDead, DeadAt set,
//     fired right after OnFailure).
//
// Hooks must be safe for concurrent use: OnEnqueue may be called from
// multiple producers, the On*/On*/On*/On* worker hooks from multiple worker
// goroutines. They should not block: a slow hook stalls the worker that
// fired it (or the producer that enqueued).
type Hooks struct {
	// OnEnqueue fires when a job has been enqueued successfully.
	OnEnqueue func(JobInfo)
	// OnSuccess fires when a job finished without error.
	OnSuccess func(JobInfo)
	// OnFailure fires after every failed handler attempt (retriable or not).
	OnFailure func(JobInfo)
	// OnRetry fires after a failed attempt that will be retried. Fired after
	// OnFailure for the same attempt.
	OnRetry func(JobInfo)
	// OnDead fires when a job exhausted its retries and moved to the DLQ.
	// Fired after OnFailure for the final attempt.
	OnDead func(JobInfo)
}

// WithHooks registers the lifecycle hook callbacks. Hooks are optional; any
// nil callback is simply not fired. This replaces the older WithOnDead
// option (which is kept for backward compatibility and is still honored).
func WithHooks(h Hooks) Option {
	return func(c *Config) { c.Hooks = h }
}

// jobInfo constructs the JobInfo snapshot a hook receives from a dequeued
// job. state/lastErr/deadAt describe the event being reported.
func (dj *DequeuedJob) jobInfo(state JobState, lastErr string, deadAt time.Time) JobInfo {
	return JobInfo{
		ID:         dj.ID,
		Type:       dj.Type,
		State:      state,
		Attempts:   dj.Attempt,
		MaxRetry:   dj.MaxRetry,
		Priority:   dj.Priority,
		LastError:  lastErr,
		EnqueuedAt: dj.EnqueuedAt,
		DeadAt:     deadAt,
	}
}
