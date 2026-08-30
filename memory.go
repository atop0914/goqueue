package goqueue

import (
	"container/heap"
	"context"
	"sort"
	"sync"
	"time"
)

// InMemoryQueue is a concurrency-safe, heap-backed Queue that supports
// priorities and delayed execution. It is the default backend and has zero
// external dependencies.
//
// Scheduling order: jobs are sorted by (readyAt, -priority, seq), so the
// highest-priority job whose RunAfter has elapsed and that was enqueued
// earliest is dequeued first.
type InMemoryQueue struct {
	mu       sync.Mutex
	heap     jobHeap
	inflight map[string]*jobRecord // dequeued, awaiting Ack/Nack
	dead     map[string]*jobRecord
	unique   map[string]string // UniqueKey -> job ID while that key is held
	seq      uint64
	closed   bool
	// paused stops Dequeue from delivering jobs. Pending jobs are retained
	// verbatim; blocked callers wait until Resume, Close, or ctx cancel.
	paused bool
	notify chan struct{}
	now    func() time.Time
}

// NewInMemoryQueue creates an empty in-memory queue.
func NewInMemoryQueue() *InMemoryQueue {
	return &InMemoryQueue{
		inflight: make(map[string]*jobRecord),
		dead:     make(map[string]*jobRecord),
		unique:   make(map[string]string),
		notify:   make(chan struct{}, 1),
		now:      time.Now,
	}
}

// Enqueue implements Queue.
func (q *InMemoryQueue) Enqueue(_ context.Context, job Job) (string, error) {
	rec := &jobRecord{
		job:        job,
		state:      StatePending,
		enqueuedAt: q.now(),
	}
	if rec.job.ID == "" {
		rec.job.ID = newID()
	}
	if rec.job.MaxRetry == 0 {
		rec.job.MaxRetry = DefaultMaxRetry
	}

	q.mu.Lock()
	// seq must be assigned under the lock: it is shared across concurrent
	// Enqueue calls and ordering correctness depends on it.
	rec.seq = q.nextSeq()
	if q.closed {
		q.mu.Unlock()
		return "", ErrQueueClosed
	}
	// Unique jobs: reject if the key is already held by a pending or running
	// job. The key stays held across retries and is only released on Ack
	// (success) or when the job moves to the DLQ.
	if rec.job.UniqueKey != "" {
		if _, dup := q.unique[rec.job.UniqueKey]; dup {
			q.mu.Unlock()
			return "", ErrJobExists
		}
		q.unique[rec.job.UniqueKey] = rec.job.ID
	}
	heap.Push(&q.heap, rec)
	q.mu.Unlock()
	q.signal()
	return rec.job.ID, nil
}

// Dequeue implements Queue. It blocks until a job is ready to run or the
// context is canceled, and returns ErrQueueClosed once the queue is closed
// and drained.
func (q *InMemoryQueue) Dequeue(ctx context.Context) (*DequeuedJob, error) {
	for {
		q.mu.Lock()
		if q.closed && q.heap.Len() == 0 {
			q.mu.Unlock()
			return nil, ErrQueueClosed
		}
		// While paused, nothing is delivered — not even after Close (a
		// paused queue still holds its jobs for later). Waiters block here
		// until Resume lifts the pause.
		if q.paused {
			q.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-q.notify:
			}
			continue
		}
		if q.heap.Len() > 0 {
			top := q.heap[0]
			wait := time.Until(top.job.RunAfter)
			if wait <= 0 {
				heap.Pop(&q.heap)
				top.state = StateRunning
				top.attempts++
				q.inflight[top.job.ID] = top
				dj := &DequeuedJob{
					ID:         top.job.ID,
					Type:       top.job.Type,
					Payload:    top.job.Payload,
					Priority:   top.job.Priority,
					Attempt:    top.attempts,
					MaxRetry:   top.job.MaxRetry,
					Timeout:    top.job.Timeout,
					DequeuedAt: q.now(),
					EnqueuedAt: top.enqueuedAt,
				}
				q.mu.Unlock()
				return dj, nil
			}
			q.mu.Unlock()
			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
				continue // re-check: maybe a higher-priority job arrived
			case <-q.notify:
				timer.Stop()
				continue
			}
		}
		q.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-q.notify:
		}
	}
}

// Ack implements Queue.
func (q *InMemoryQueue) Ack(_ context.Context, id string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	rec, ok := q.inflight[id]
	if !ok {
		return ErrJobNotFound
	}
	delete(q.inflight, id)
	rec.state = StateSucceeded
	rec.lastError = ""
	q.releaseUnique(rec)
	return nil
}

// Nack implements Queue. When retryable is true and the job still has
// attempts left it is re-queued to run again after delay (the retry is
// scheduled via RunAfter, reusing the delayed-job machinery); otherwise it
// moves to the DLQ.
func (q *InMemoryQueue) Nack(_ context.Context, id string, err error, retryable bool, delay time.Duration) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	rec, ok := q.inflight[id]
	if !ok {
		return ErrJobNotFound
	}
	delete(q.inflight, id)
	rec.lastError = ""
	if err != nil {
		rec.lastError = err.Error()
	}
	if retryable && rec.attempts <= rec.job.MaxRetry {
		rec.state = StateFailed
		if delay > 0 {
			rec.job.RunAfter = q.now().Add(delay)
		}
		// Unique key stays held across retries.
		heap.Push(&q.heap, rec)
		q.signal()
		return nil
	}
	// Retries exhausted or explicitly non-retryable: move to DLQ.
	rec.state = StateDead
	rec.deadAt = q.now()
	q.dead[id] = rec
	q.releaseUnique(rec)
	return nil
}

// Dead implements Queue. Results are sorted by death time ascending (the
// earliest-died job first), so the DLQ reads as a timeline.
func (q *InMemoryQueue) Dead() []JobInfo {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]JobInfo, 0, len(q.dead))
	for _, rec := range q.dead {
		out = append(out, rec.info())
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].DeadAt.Equal(out[j].DeadAt) {
			return out[i].DeadAt.Before(out[j].DeadAt)
		}
		return out[i].ID < out[j].ID
	})
	return out
}

// Len implements Queue.
func (q *InMemoryQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.heap.Len()
}

// Close implements Queue.
func (q *InMemoryQueue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil
	}
	q.closed = true
	q.signal()
	return nil
}

func (q *InMemoryQueue) nextSeq() uint64 {
	q.seq++
	return q.seq
}

// releaseUnique frees the job's UniqueKey (if any) so a later job with the
// same key may be enqueued. Called only when the job leaves the active set:
// on Ack (success) or when it moves to the DLQ. It must be called with q.mu
// held.
func (q *InMemoryQueue) releaseUnique(rec *jobRecord) {
	if rec.job.UniqueKey == "" {
		return
	}
	if q.unique[rec.job.UniqueKey] == rec.job.ID {
		delete(q.unique, rec.job.UniqueKey)
	}
}

func (q *InMemoryQueue) signal() {
	select {
	case q.notify <- struct{}{}:
	default:
	}
}

func (r *jobRecord) info() JobInfo {
	return JobInfo{
		ID:         r.job.ID,
		Type:       r.job.Type,
		State:      r.state,
		Attempts:   r.attempts,
		MaxRetry:   r.job.MaxRetry,
		Priority:   r.job.Priority,
		LastError:  r.lastError,
		EnqueuedAt: r.enqueuedAt,
		DeadAt:     r.deadAt,
	}
}

// ---- priority heap ----

// jobHeap orders records by (readyAt asc, priority desc, seq asc).
type jobHeap []*jobRecord

func (h jobHeap) Len() int { return len(h) }

func (h jobHeap) Less(i, j int) bool {
	a, b := h[i], h[j]
	if !a.job.RunAfter.Equal(b.job.RunAfter) {
		return a.job.RunAfter.Before(b.job.RunAfter)
	}
	if a.job.Priority != b.job.Priority {
		return a.job.Priority > b.job.Priority
	}
	return a.seq < b.seq
}

func (h jobHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *jobHeap) Push(x any) { *h = append(*h, x.(*jobRecord)) }

func (h *jobHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return item
}
