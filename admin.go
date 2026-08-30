package goqueue

import (
	"context"
	"errors"
	"fmt"
)

// Admin operations. Every capability is an optional interface a backend may
// implement; the Client methods discover support at runtime via type
// assertion, exactly like LenAwareQueue. This keeps the core Queue contract
// small while letting memory, SQLite and Redis expose operations without
// breaking custom third-party backends.
var (
	// ErrQueuePaused is returned by Dequeue... it is not: while paused
	// Dequeue blocks, it does not fail. The sentinel exists so backends can
	// report a pause-related misuse uniformly (e.g. Pause after Close).
	ErrQueuePaused = errors.New("goqueue: queue is paused")

	// ErrAdminUnsupported is returned (wrapped) by Client admin methods when
	// the configured backend does not implement the matching optional admin
	// interface. Check with errors.Is.
	ErrAdminUnsupported = errors.New("goqueue: backend does not support admin operation")
)

// Pauser is the pause/resume capability of a backend. While paused, Dequeue
// stops handing out jobs: callers block until Resume, Close, or their own
// context is canceled. Jobs already dequeued (running) are unaffected; jobs
// still queued are retained verbatim — priorities, delays and retry
// schedules all survive a pause/resume cycle.
type Pauser interface {
	// Pause stops job delivery. Idempotent. Returns ErrQueueClosed after
	// Close.
	Pause() error
	// Resume lifts the pause and wakes blocked Dequeue callers. Idempotent.
	Resume()
	// IsPaused reports whether the queue is currently paused.
	IsPaused() bool
}

// Purger is the purge capability: drop pending jobs.
type Purger interface {
	// Purge removes pending jobs (waiting, delayed and awaiting-retry) and
	// returns how many were dropped. Running jobs are never touched. With
	// dead set, the dead-letter set is dropped as well and the returned
	// count includes it. Idempotent: purging an empty queue returns 0.
	Purge(ctx context.Context, dead bool) (int, error)
}

// DeadRequeuer is the dead-letter requeue capability.
type DeadRequeuer interface {
	// RequeueDead moves every dead-letter job back into the queue and
	// returns how many were requeued. Per-job semantics (documented on
	// RequeueDeadJob): attempts reset, errors cleared, due immediately.
	// Idempotent: an empty DLQ yields 0.
	RequeueDead(ctx context.Context) (int, error)

	// RequeueDeadJob requeues one dead job by ID. Semantics, identical
	// across backends:
	//
	//   - attempts reset to zero, so the job gets its full retry budget
	//     again (one run plus MaxRetry retries);
	//   - last_error and dead_at are cleared;
	//   - the job becomes due immediately;
	//   - a job's UniqueKey (if any) is re-claimed; when another in-flight
	//     job holds the same key, the dead job is left untouched and
	//     ErrJobExists is returned;
	//   - unknown or non-dead IDs return ErrJobNotFound.
	RequeueDeadJob(ctx context.Context, id string) error
}

// AdminQueue groups all admin capabilities. A backend implements it as a
// whole (all three built-in backends do); the Client nevertheless checks
// each capability independently so partial implementations keep working for
// the operations they do provide.
type AdminQueue interface {
	Pauser
	Purger
	DeadRequeuer
}

func errAdminUnsupported(op string) error {
	return fmt.Errorf("%w: %s", ErrAdminUnsupported, op)
}

// ---- Client-level admin API ----

// Pause stops job delivery for the whole client: workers block inside
// Dequeue until Resume. Enqueue keeps working — producers can keep
// submitting while the queue is paused, which is what makes pause useful for
// maintenance windows (let in-flight work finish, apply a fix, resume).
// Requires the backend to implement Pauser; otherwise ErrAdminUnsupported.
func (c *Client) Pause() error {
	p, ok := c.cfg.Queue.(Pauser)
	if !ok {
		return errAdminUnsupported("pause")
	}
	return p.Pause()
}

// Resume lifts a pause. It is a no-op on backends without the Pauser
// capability (and on a running queue).
func (c *Client) Resume() {
	if p, ok := c.cfg.Queue.(Pauser); ok {
		p.Resume()
	}
}

// IsPaused reports whether the queue is currently paused. Backends without
// the Pauser capability are never paused.
func (c *Client) IsPaused() bool {
	p, ok := c.cfg.Queue.(Pauser)
	return ok && p.IsPaused()
}

// Purge drops pending jobs and returns how many were removed. With dead set,
// the dead-letter set is dropped too and counted. Running jobs always
// finish. Idempotent. Requires Purger; otherwise ErrAdminUnsupported.
func (c *Client) Purge(ctx context.Context, dead bool) (int, error) {
	p, ok := c.cfg.Queue.(Purger)
	if !ok {
		return 0, errAdminUnsupported("purge")
	}
	return p.Purge(ctx, dead)
}

// RequeueDead moves every dead-letter job back into the queue (attempts
// reset, due immediately) and returns the count. See DeadRequeuer for the
// exact per-job semantics. Requires DeadRequeuer; otherwise
// ErrAdminUnsupported.
func (c *Client) RequeueDead(ctx context.Context) (int, error) {
	d, ok := c.cfg.Queue.(DeadRequeuer)
	if !ok {
		return 0, errAdminUnsupported("requeue-dead")
	}
	return d.RequeueDead(ctx)
}

// RequeueDeadJob requeues a single dead-letter job by ID. Use it when
// operators cherry-pick from the DLQ instead of flushing it wholesale.
// Requires DeadRequeuer; otherwise ErrAdminUnsupported.
func (c *Client) RequeueDeadJob(ctx context.Context, id string) error {
	d, ok := c.cfg.Queue.(DeadRequeuer)
	if !ok {
		return errAdminUnsupported("requeue-dead-job")
	}
	return d.RequeueDeadJob(ctx, id)
}

// Compile-time check: the in-memory backend implements the full admin
// surface. The SQLite and Redis backends assert in their own packages.
var (
	_ AdminQueue = (*InMemoryQueue)(nil)
	_ Pauser     = (*Client)(nil) // Client delegates; silence unused warnings via the interface
)
