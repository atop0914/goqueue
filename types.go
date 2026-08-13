package goqueue

import (
	"context"
	"time"
)

// Handler processes a single job payload. Implementations must be safe for
// concurrent use: a handler may be invoked from multiple worker goroutines.
type Handler func(ctx context.Context, payload []byte) error

// Job describes a unit of work to be enqueued.
//
// The zero value is not usable: Type must be set and a matching handler must
// be registered on the Client before the job is enqueued.
type Job struct {
	// ID is an optional unique identifier. When empty, the queue generates
	// one (ULID). Unique jobs (Day 3) reuse this field for dedup.
	ID string

	// Type selects the registered handler. Required.
	Type string

	// Payload is the opaque, caller-encoded job data. Handlers receive it
	// verbatim, so the encoding (JSON, msgpack, ...) is the caller's choice.
	Payload []byte

	// Priority ranks the job within the queue. Higher values are dequeued
	// first. Defaults to 0.
	Priority int

	// MaxRetry is the number of additional attempts allowed after the first
	// run. Defaults to 3 (i.e. up to 4 total attempts). Set to -1 to disable
	// retries entirely.
	MaxRetry int

	// Timeout bounds a single handler invocation. Zero means no timeout.
	Timeout time.Duration

	// RunAfter defers execution until this time. Zero value means "run now".
	RunAfter time.Time
}

// JobState is the lifecycle state of a job.
type JobState int

const (
	// StatePending is enqueued and waiting for a worker.
	StatePending JobState = iota
	// StateRunning is currently being processed by a worker.
	StateRunning
	// StateSucceeded finished without error.
	StateSucceeded
	// StateFailed finished with an error and will be retried.
	StateFailed
	// StateDead exhausted all retry attempts and moved to the DLQ.
	StateDead
)

func (s JobState) String() string {
	switch s {
	case StatePending:
		return "pending"
	case StateRunning:
		return "running"
	case StateSucceeded:
		return "succeeded"
	case StateFailed:
		return "failed"
	case StateDead:
		return "dead"
	default:
		return "unknown"
	}
}

// JobInfo is the snapshot returned by Enqueue and used for inspection.
type JobInfo struct {
	ID         string
	Type       string
	State      JobState
	Attempts   int
	MaxRetry   int
	Priority   int
	LastError  string
	EnqueuedAt time.Time
}

// jobRecord is the internal representation stored in a Queue backend.
type jobRecord struct {
	job        Job
	seq        uint64 // insertion order, for FIFO tie-breaking
	attempts   int    // completed attempts so far
	state      JobState
	lastError  string
	enqueuedAt time.Time
}
