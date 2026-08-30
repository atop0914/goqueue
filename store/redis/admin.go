package redis

import (
	"context"
	"fmt"
	"time"

	goqueue "github.com/atop0914/goqueue"
)

// ---- admin operations for the Redis Store ----

// Pause implements goqueue.Pauser: claims stop until Resume. The flag is a
// local runtime concern (each process manages its own consumption), so it is
// NOT shared through Redis — pausing one consumer does not pause others.
// Pending data is untouched. Idempotent; returns goqueue.ErrQueueClosed
// after Close.
func (s *Store) Pause() error {
	if s.closed.Load() {
		return goqueue.ErrQueueClosed
	}
	s.paused.Store(true)
	s.signal()
	return nil
}

// Resume implements goqueue.Pauser: local pollers wake and claims continue.
// Idempotent.
func (s *Store) Resume() {
	if !s.paused.CompareAndSwap(true, false) {
		return
	}
	s.signal()
}

// IsPaused implements goqueue.Pauser.
func (s *Store) IsPaused() bool { return s.paused.Load() }

// Purge implements goqueue.Purger: drop every ready job (waiting, delayed
// and awaiting-retry — all live in the ready ZSET) and optionally the dead
// ZSET, atomically in one Lua script. Running jobs are never touched. Unique
// keys of purged jobs are released by scanning the job hashes before the
// delete; keys of running jobs stay held. Returns the number of jobs
// removed. Idempotent.
func (s *Store) Purge(ctx context.Context, dead bool) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()
	n, err := purgeScript.Run(ctx, s.client,
		[]string{s.kReady(), s.kDead(), s.kUnique()},
		s.kJobPrefix(), deadInt(dead),
	).Int()
	if err != nil {
		return 0, fmt.Errorf("goqueue/redis: purge: %w", err)
	}
	return n, nil
}

// RequeueDead implements goqueue.DeadRequeuer: requeue every dead job via
// the single-job path (the same script used by cherry-pick requeues), so
// per-job semantics — including contested unique keys — are identical in
// both forms. Returns the number of jobs requeued.
func (s *Store) RequeueDead(ctx context.Context) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()
	ids, err := s.client.ZRange(ctx, s.kDead(), 0, -1).Result()
	if err != nil {
		return 0, fmt.Errorf("goqueue/redis: requeue-dead scan: %w", err)
	}
	n := 0
	for _, id := range ids {
		if s.RequeueDeadJob(ctx, id) == nil {
			n++
		}
	}
	return n, nil
}

// RequeueDeadJob implements the single-job form of goqueue.DeadRequeuer: one
// Lua script atomically moves a dead job back to the ready queue — attempts
// reset, last_error/dead_at cleared, due immediately (score base 0), unique
// key re-claimed. Semantics match the other backends:
//
//   - unknown or non-dead IDs return goqueue.ErrJobNotFound;
//   - a unique key held by another in-flight job leaves the dead job
//     untouched and returns goqueue.ErrJobExists.
func (s *Store) RequeueDeadJob(ctx context.Context, id string) error {
	ctx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()
	nowMs := time.Now().UnixMilli()
	n, err := requeueDeadScript.Run(ctx, s.client,
		[]string{s.kDead(), s.kReady(), s.kJob(id), s.kUnique()},
		id, nowMs,
	).Int()
	if err != nil {
		return fmt.Errorf("goqueue/redis: requeue-dead-job: %w", err)
	}
	switch n {
	case 0:
		return goqueue.ErrJobNotFound
	case 2:
		return goqueue.ErrJobExists
	default:
		s.signal()
		return nil
	}
}

// Compile-time check: the Redis store implements the full admin surface.
var _ goqueue.AdminQueue = (*Store)(nil)

// deadInt renders the dead-flag Lua argument ("1"/"0").
func deadInt(b bool) string {
	if b {
		return "1"
	}
	return "0"
}
