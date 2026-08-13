package goqueue

import (
	"container/heap"
	"context"
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
	seq      uint64
	closed   bool
	notify   chan struct{}
	now      func() time.Time
}

// NewInMemoryQueue creates an empty in-memory queue.
func NewInMemoryQueue() *InMemoryQueue {
	return &InMemoryQueue{
		inflight: make(map[string]*jobRecord),
		dead:     make(map[string]*jobRecord),
		notify:   make(chan struct{}, 1),
		now:      time.Now,
	}
}

// Enqueue implements Queue.
func (q *InMemoryQueue) Enqueue(_ context.Context, job Job) (string, error) {
	rec := &jobRecord{
		job:        job,
		seq:        q.nextSeq(),
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
	if q.closed {
		q.mu.Unlock()
		return "", ErrQueueClosed
	}
	heap.Push(&q.heap, rec)
	q.mu.Unlock()
	q.signal()
	return rec.job.ID, nil
}

// Dequeue implements Queue. It blocks until a job is ready to run or the
// context is cancelled, and returns ErrQueueClosed once the queue is closed
// and drained.
func (q *InMemoryQueue) Dequeue(ctx context.Context) (*DequeuedJob, error) {
	for {
		q.mu.Lock()
		if q.closed && q.heap.Len() == 0 {
			q.mu.Unlock()
			return nil, ErrQueueClosed
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
	return nil
}

// Nack implements Queue. When retryable is true and the job still has
// attempts left it is re-queued immediately; otherwise it moves to the DLQ.
func (q *InMemoryQueue) Nack(_ context.Context, id string, err error, retryable bool) error {
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
		heap.Push(&q.heap, rec)
		q.signal()
		return nil
	}
	// Retries exhausted or explicitly non-retryable: move to DLQ.
	rec.state = StateDead
	q.dead[id] = rec
	return nil
}

// Dead implements Queue.
func (q *InMemoryQueue) Dead() []JobInfo {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]JobInfo, 0, len(q.dead))
	for _, rec := range q.dead {
		out = append(out, rec.info())
	}
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
