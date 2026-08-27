package goqueue

import (
	"context"
	"errors"
	"time"
)

// Queue is the storage backend contract. Implementations must be safe for
// concurrent use by multiple producers and workers.
//
// The lifecycle of a job through the queue is:
//
//	Enqueue -> Dequeue -> (Ack | Nack)
//
// A worker that receives a job must eventually call exactly one of Ack or
// Nack. Nack with a retryable error re-queues the job (respecting MaxRetry);
// Nack with retryable=false moves it straight to the dead-letter set.
type Queue interface {
	// Enqueue inserts a job and returns its ID. The ID is generated when
	// job.ID is empty.
	Enqueue(ctx context.Context, job Job) (string, error)

	// Dequeue blocks until a job is ready to run (RunAfter elapsed) or the
	// context is canceled. It returns ErrQueueClosed when the queue has been
	// closed and drained.
	Dequeue(ctx context.Context) (*DequeuedJob, error)

	// Ack marks a dequeued job as successfully processed.
	Ack(ctx context.Context, id string) error

	// Nack reports a failed attempt. When retryable is true and the job still
	// has attempts left it is re-queued to run again after delay; otherwise it
	// moves to the dead-letter set.
	//
	// delay is the backoff interval chosen by the caller (e.g. an exponential
	// schedule from RetryBackoff). Backends must honor it by keeping the job
	// invisible until it elapses — for example by scheduling RunAfter = now +
	// delay and reusing the delayed-job machinery. It is ignored when the job
	// goes to the DLQ.
	Nack(ctx context.Context, id string, err error, retryable bool, delay time.Duration) error

	// Dead returns the current dead-letter jobs (attempts exhausted).
	Dead() []JobInfo

	// Len returns the number of pending (not yet dequeued) jobs.
	Len() int

	// Close releases resources and unblocks pending Dequeue calls with
	// ErrQueueClosed.
	Close() error
}

// LenAwareQueue is an optional interface for backends whose Len honors a
// context. The client's drain check uses it when available so that a
// contended embedded backend (e.g. SQLite) cannot wedge Shutdown on an
// unbounded internal wait: a backend that cannot answer in time may report
// "not drained" and let the caller retry.
type LenAwareQueue interface {
	LenContext(ctx context.Context) int
}

// DequeuedJob is a job handed to a worker. The worker must Ack or Nack it.
type DequeuedJob struct {
	ID         string
	Type       string
	Payload    []byte
	Priority   int
	Attempt    int // 1-based: first execution is attempt 1
	MaxRetry   int
	Timeout    time.Duration // per-attempt handler timeout, 0 = none
	DequeuedAt time.Time
	// EnqueuedAt is the job's original enqueue time. It is exposed so
	// callbacks (e.g. OnDead) can build a full JobInfo snapshot.
	EnqueuedAt time.Time
}

// Sentinel errors.
var (
	// ErrQueueClosed is returned by Dequeue once the queue is closed and all
	// pending jobs have been drained.
	ErrQueueClosed = errors.New("goqueue: queue is closed")
	// ErrUnknownType is returned by Enqueue when the job type has no handler.
	ErrUnknownType = errors.New("goqueue: unknown job type")
	// ErrJobNotFound is returned when ack/nacking a job the queue no longer
	// tracks (e.g. already acked).
	ErrJobNotFound = errors.New("goqueue: job not found")
	// ErrJobExists is returned by Enqueue for a unique job whose UniqueKey is
	// already held by a pending or running job.
	ErrJobExists = errors.New("goqueue: unique job already in flight")
)
