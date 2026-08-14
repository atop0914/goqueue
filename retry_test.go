package goqueue

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRetryBackoffDelay(t *testing.T) {
	b := RetryBackoff{
		InitialInterval: 100 * time.Millisecond,
		MaxInterval:     time.Second,
		Multiplier:      2.0,
	}
	cases := []struct {
		retry int
		want  time.Duration
	}{
		{1, 100 * time.Millisecond},
		{2, 200 * time.Millisecond},
		{3, 400 * time.Millisecond},
		{4, 800 * time.Millisecond},
		{5, time.Second},             // capped at MaxInterval
		{6, time.Second},             // stays capped
		{0, 100 * time.Millisecond},  // clamped to 1
		{-3, 100 * time.Millisecond}, // clamped to 1
	}
	for _, tc := range cases {
		if got := b.Delay(tc.retry); got != tc.want {
			t.Errorf("Delay(%d) = %v, want %v", tc.retry, got, tc.want)
		}
	}
}

func TestRetryBackoffZeroValue(t *testing.T) {
	// A zero-value backoff means "re-queue immediately" (old behaviour),
	// never an infinite or negative delay.
	var b RetryBackoff
	if got := b.Delay(1); got != 0 {
		t.Fatalf("zero-value Delay(1) = %v, want 0", got)
	}
	if got := b.Delay(10); got != 0 {
		t.Fatalf("zero-value Delay(10) = %v, want 0", got)
	}
}

func TestRetryBackoffFixedInterval(t *testing.T) {
	// Multiplier <= 1 behaves as a fixed interval.
	b := RetryBackoff{InitialInterval: 50 * time.Millisecond, Multiplier: 1.0}
	for retry := 1; retry <= 5; retry++ {
		if got := b.Delay(retry); got != 50*time.Millisecond {
			t.Fatalf("Delay(%d) = %v, want 50ms (fixed)", retry, got)
		}
	}
}

// TestNackBackoffReschedulesRunAfter verifies the queue layer: Nack with a
// delay re-queues the job with RunAfter = now + delay, reusing the delayed
// job machinery instead of re-queueing it immediately.
func TestNackBackoffReschedulesRunAfter(t *testing.T) {
	q := NewInMemoryQueue()
	defer q.Close()

	base := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	cur := base
	q.now = func() time.Time { return cur }

	ctx := context.Background()
	if _, err := q.Enqueue(ctx, Job{Type: "t", MaxRetry: 3}); err != nil {
		t.Fatal(err)
	}
	dj, err := q.Dequeue(ctx)
	if err != nil {
		t.Fatal(err)
	}

	cur = base.Add(time.Minute) // clock moves forward
	if err := q.Nack(ctx, dj.ID, errors.New("boom"), true, 5*time.Second); err != nil {
		t.Fatal(err)
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	if q.heap.Len() != 1 {
		t.Fatalf("heap len = %d, want 1", q.heap.Len())
	}
	got := q.heap[0].job.RunAfter
	want := base.Add(time.Minute).Add(5 * time.Second)
	if !got.Equal(want) {
		t.Fatalf("RunAfter = %v, want %v (now + delay)", got, want)
	}
}

// TestRetryBackoffMultiRoundTiming drives a full retry cycle through the
// queue with real timers: each failed attempt must be invisible for its
// backoff delay before it can be dequeued again.
func TestRetryBackoffMultiRoundTiming(t *testing.T) {
	q := NewInMemoryQueue()
	defer q.Close()

	ctx := context.Background()
	if _, err := q.Enqueue(ctx, Job{Type: "t", MaxRetry: 2}); err != nil {
		t.Fatal(err)
	}

	// Round 1: fail, retry after 50ms.
	dj, err := q.Dequeue(ctx)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err := q.Nack(ctx, dj.ID, errors.New("boom"), true, 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	dj, err = q.Dequeue(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed < 40*time.Millisecond {
		t.Fatalf("retry 1 surfaced after %v, want >= 40ms (50ms backoff)", elapsed)
	}

	// Round 2: fail, retry after 100ms.
	start = time.Now()
	if err := q.Nack(ctx, dj.ID, errors.New("boom"), true, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	dj, err = q.Dequeue(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed < 80*time.Millisecond {
		t.Fatalf("retry 2 surfaced after %v, want >= 80ms (100ms backoff)", elapsed)
	}

	// Round 3: attempts exhausted -> dead, nothing left to dequeue.
	if err := q.Nack(ctx, dj.ID, errors.New("boom"), false, 0); err != nil {
		t.Fatal(err)
	}
	if got := len(q.Dead()); got != 1 {
		t.Fatalf("Dead() = %d, want 1", got)
	}
	if got := q.Len(); got != 0 {
		t.Fatalf("Len = %d, want 0", got)
	}
}

// TestDeadSortedByDeathTime verifies Dead() returns jobs in the order they
// died (earliest first), using a fake clock for exact death timestamps.
func TestDeadSortedByDeathTime(t *testing.T) {
	q := NewInMemoryQueue()
	defer q.Close()

	base := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	cur := base
	q.now = func() time.Time { return cur }

	ctx := context.Background()
	var ids []string
	for i := 0; i < 3; i++ {
		id, err := q.Enqueue(ctx, Job{Type: "t", MaxRetry: 1})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
		dj, err := q.Dequeue(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := q.Nack(ctx, dj.ID, errors.New("boom"), false, 0); err != nil {
			t.Fatal(err)
		}
		cur = cur.Add(time.Minute) // next job dies a minute later
	}

	dead := q.Dead()
	if len(dead) != 3 {
		t.Fatalf("Dead() = %d, want 3", len(dead))
	}
	for i, want := range ids {
		if dead[i].ID != want {
			t.Fatalf("dead[%d].ID = %s, want %s (chronological)", i, dead[i].ID, want)
		}
	}
	for i := 1; i < len(dead); i++ {
		if !dead[i].DeadAt.After(dead[i-1].DeadAt) {
			t.Fatalf("dead[%d].DeadAt = %v not after dead[%d].DeadAt = %v",
				i, dead[i].DeadAt, i-1, dead[i-1].DeadAt)
		}
	}
}

func TestDeadIncludesDeadAt(t *testing.T) {
	q := NewInMemoryQueue()
	defer q.Close()

	ctx := context.Background()
	mustEnqueue(t, q, ctx, Job{Type: "t", MaxRetry: 1})
	dj, err := q.Dequeue(ctx)
	if err != nil {
		t.Fatal(err)
	}
	before := time.Now()
	if err := q.Nack(ctx, dj.ID, errors.New("boom"), false, 0); err != nil {
		t.Fatal(err)
	}
	after := time.Now()

	dead := q.Dead()
	if len(dead) != 1 {
		t.Fatalf("Dead() = %d, want 1", len(dead))
	}
	if dead[0].DeadAt.IsZero() {
		t.Fatal("DeadAt is zero, want the death timestamp")
	}
	if dead[0].DeadAt.Before(before) || dead[0].DeadAt.After(after) {
		t.Fatalf("DeadAt = %v out of [before=%v, after=%v]", dead[0].DeadAt, before, after)
	}
}

// TestOnDeadFullJobInfo verifies the OnDead callback receives a complete
// JobInfo snapshot, including the fields that used to be missing
// (Priority, EnqueuedAt, DeadAt).
func TestOnDeadFullJobInfo(t *testing.T) {
	var got atomic.Value
	c := New(WithWorkers(1), WithOnDead(func(info JobInfo) {
		got.Store(info)
	}))
	defer c.Shutdown(context.Background())

	c.Register("fail", func(ctx context.Context, payload []byte) error {
		return errors.New("nope")
	})
	c.Start()

	enqueuedBefore := time.Now()
	id, err := c.Enqueue(context.Background(), Job{
		Type: "fail", MaxRetry: 1, Priority: 7, Payload: []byte("p"),
	})
	if err != nil {
		t.Fatal(err)
	}

	waitFor(t, 2*time.Second, func() bool { return got.Load() != nil })

	info := got.Load().(JobInfo)
	if info.ID != id {
		t.Fatalf("ID = %s, want %s", info.ID, id)
	}
	if info.Type != "fail" {
		t.Fatalf("Type = %q, want %q", info.Type, "fail")
	}
	if info.State != StateDead {
		t.Fatalf("State = %v, want dead", info.State)
	}
	if info.Attempts != 2 {
		t.Fatalf("Attempts = %d, want 2 (1 run + 1 retry)", info.Attempts)
	}
	if info.MaxRetry != 1 {
		t.Fatalf("MaxRetry = %d, want 1", info.MaxRetry)
	}
	if info.Priority != 7 {
		t.Fatalf("Priority = %d, want 7", info.Priority)
	}
	if info.LastError != "nope" {
		t.Fatalf("LastError = %q, want %q", info.LastError, "nope")
	}
	if info.EnqueuedAt.IsZero() || info.EnqueuedAt.Before(enqueuedBefore) {
		t.Fatalf("EnqueuedAt = %v, want >= %v", info.EnqueuedAt, enqueuedBefore)
	}
	if info.DeadAt.IsZero() {
		t.Fatal("DeadAt is zero, want the death timestamp")
	}
}

// TestClientBackoffIntegration runs the whole pipeline and checks the
// retries actually spread out: attempt gaps must match the configured
// exponential schedule (60ms then 120ms).
func TestClientBackoffIntegration(t *testing.T) {
	c := New(WithWorkers(1), WithRetryBackoff(RetryBackoff{
		InitialInterval: 60 * time.Millisecond,
		MaxInterval:     200 * time.Millisecond,
		Multiplier:      2.0,
	}))
	defer c.Shutdown(context.Background())

	var attempts atomic.Int32
	var timesMu sync.Mutex
	var attemptTimes []time.Time
	c.Register("flaky", func(ctx context.Context, payload []byte) error {
		timesMu.Lock()
		attemptTimes = append(attemptTimes, time.Now())
		timesMu.Unlock()
		if attempts.Add(1) < 3 {
			return errors.New("not yet")
		}
		return nil
	})
	c.Start()

	if _, err := c.Enqueue(context.Background(), Job{Type: "flaky", MaxRetry: 5}); err != nil {
		t.Fatal(err)
	}

	waitFor(t, 3*time.Second, func() bool { return attempts.Load() == 3 })

	timesMu.Lock()
	defer timesMu.Unlock()
	if len(attemptTimes) != 3 {
		t.Fatalf("attempts recorded = %d, want 3", len(attemptTimes))
	}
	gap1 := attemptTimes[1].Sub(attemptTimes[0])
	gap2 := attemptTimes[2].Sub(attemptTimes[1])
	if gap1 < 50*time.Millisecond {
		t.Fatalf("gap between attempt 1 and 2 = %v, want >= 50ms (60ms backoff)", gap1)
	}
	if gap2 < 110*time.Millisecond {
		t.Fatalf("gap between attempt 2 and 3 = %v, want >= 110ms (120ms backoff)", gap2)
	}
	if gap2 < gap1 {
		t.Fatalf("gap2 = %v should exceed gap1 = %v (exponential growth)", gap2, gap1)
	}
}
