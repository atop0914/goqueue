package goqueue

import (
	"container/heap"
	"context"
	"time"
)

// ---- admin operations for InMemoryQueue ----

// Pause implements Pauser. After Pause, Dequeue stops delivering jobs:
// waiters block until Resume, Close, or their context is canceled. Pending
// jobs, priorities, delays and retry schedules are retained verbatim.
// Idempotent; returns ErrQueueClosed after Close.
func (q *InMemoryQueue) Pause() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ErrQueueClosed
	}
	q.paused = true
	q.signal()
	return nil
}

// Resume implements Pauser. It wakes every Dequeue blocked on the pause;
// delivery continues exactly where it stopped. Idempotent.
func (q *InMemoryQueue) Resume() {
	q.mu.Lock()
	if !q.paused {
		q.mu.Unlock()
		return
	}
	q.paused = false
	q.mu.Unlock()
	q.signal()
}

// IsPaused implements Pauser.
func (q *InMemoryQueue) IsPaused() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.paused
}

// Purge implements Purger: drop every pending job (waiting, delayed and
// awaiting-retry) and optionally the dead-letter set. Running jobs are never
// touched — they are in q.inflight, not in the heap. Unique keys held by
// purged jobs are released so the same work can be re-enqueued; keys held by
// running jobs stay held. Returns the number of jobs removed. Idempotent.
func (q *InMemoryQueue) Purge(_ context.Context, dead bool) (int, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	n := q.heap.Len()
	q.heap = nil
	for k, id := range q.unique {
		if _, running := q.inflight[id]; !running {
			delete(q.unique, k)
		}
	}
	if dead {
		n += len(q.dead)
		q.dead = make(map[string]*jobRecord)
	}
	q.signal()
	return n, nil
}

// RequeueDead implements DeadRequeuer: requeue every dead job (see
// RequeueDeadJob for per-job semantics) and return the count. Jobs skipped
// because their unique key is contested stay dead and are not counted.
func (q *InMemoryQueue) RequeueDead(ctx context.Context) (int, error) {
	q.mu.Lock()
	ids := make([]string, 0, len(q.dead))
	for _, rec := range q.dead {
		ids = append(ids, rec.job.ID)
	}
	q.mu.Unlock()
	n := 0
	for _, id := range ids {
		if q.RequeueDeadJob(ctx, id) == nil {
			n++
		}
	}
	return n, nil
}

// RequeueDeadJob implements the single-job form of DeadRequeuer: move one
// dead job back into the pending heap with attempts reset, last_error and
// dead_at cleared, and RunAfter zeroed (due immediately). The unique key (if
// any) is re-claimed; when another in-flight job holds the same key the dead
// job is left untouched and ErrJobExists is returned. Unknown or non-dead
// IDs return ErrJobNotFound.
func (q *InMemoryQueue) RequeueDeadJob(_ context.Context, id string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	rec, ok := q.dead[id]
	if !ok {
		return ErrJobNotFound
	}
	if rec.job.UniqueKey != "" {
		if holder, held := q.unique[rec.job.UniqueKey]; held && holder != rec.job.ID {
			return ErrJobExists
		}
		q.unique[rec.job.UniqueKey] = rec.job.ID
	}
	delete(q.dead, id)
	rec.attempts = 0
	rec.state = StatePending
	rec.lastError = ""
	rec.deadAt = time.Time{}
	rec.job.RunAfter = time.Time{}
	rec.seq = q.nextSeq()
	heap.Push(&q.heap, rec)
	q.signal()
	return nil
}
