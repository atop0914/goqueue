package goqueue

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// TestMaxConcurrency verifies the WithMaxConcurrency semaphore: no matter how
// many workers are running, at most N handlers execute at the same time, and
// the cap is actually reached under load (i.e. the pool still runs handlers
// concurrently up to the limit).
func TestMaxConcurrency(t *testing.T) {
	const concurrency = 2
	c := New(WithWorkers(8), WithMaxConcurrency(concurrency))
	defer c.Shutdown(context.Background())

	var inflight atomic.Int64
	var maxInflight atomic.Int64
	var runs atomic.Int64
	c.Register("work", func(ctx context.Context, payload []byte) error {
		n := inflight.Add(1)
		for {
			m := maxInflight.Load()
			if n <= m || maxInflight.CompareAndSwap(m, n) {
				break
			}
		}
		runs.Add(1)
		time.Sleep(20 * time.Millisecond)
		inflight.Add(-1)
		return nil
	})
	c.Start()

	const total = 32
	for i := 0; i < total; i++ {
		if _, err := c.Enqueue(context.Background(), Job{Type: "work"}); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, 5*time.Second, func() bool { return runs.Load() == total })

	if got := maxInflight.Load(); got > concurrency {
		t.Fatalf("max concurrent handlers = %d, want <= %d", got, concurrency)
	}
	if got := maxInflight.Load(); got < 2 {
		t.Fatalf("max concurrent handlers = %d, want >= 2 (semaphore must not serialize everything)", got)
	}
}

// TestDrainOnShutdown verifies drain mode: Shutdown keeps the workers running
// until the whole backlog is processed, then returns with an empty queue.
func TestDrainOnShutdown(t *testing.T) {
	c := New(WithWorkers(3), WithDrainOnShutdown(true))

	var runs atomic.Int32
	c.Register("work", func(ctx context.Context, payload []byte) error {
		runs.Add(1)
		time.Sleep(5 * time.Millisecond)
		return nil
	})
	c.Start()

	const total = 50
	for i := 0; i < total; i++ {
		if _, err := c.Enqueue(context.Background(), Job{Type: "work"}); err != nil {
			t.Fatal(err)
		}
	}

	if err := c.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := runs.Load(); got != total {
		t.Fatalf("processed = %d, want %d (drain must finish the whole backlog)", got, total)
	}
	if got := c.Queue().Len(); got != 0 {
		t.Fatalf("queue length = %d, want 0 after drain", got)
	}
}

// TestDrainWaitsForDelayedJobs verifies that drain mode also covers jobs whose
// RunAfter is in the future: Shutdown must not return until they are done.
func TestDrainWaitsForDelayedJobs(t *testing.T) {
	c := New(WithWorkers(2), WithDrainOnShutdown(true))

	var runs atomic.Int32
	c.Register("work", func(ctx context.Context, payload []byte) error {
		runs.Add(1)
		return nil
	})
	c.Start()

	if _, err := c.Enqueue(context.Background(), Job{
		Type:     "work",
		RunAfter: time.Now().Add(200 * time.Millisecond),
	}); err != nil {
		t.Fatal(err)
	}

	if err := c.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := runs.Load(); got != 1 {
		t.Fatalf("processed = %d, want 1 (drain must wait for delayed jobs)", got)
	}
}

// TestShutdownLeavesPendingJobs is the regression guard for the default
// (non-drain) mode: Shutdown only waits for the handler currently running and
// leaves the rest of the backlog pending for the next Start.
func TestShutdownLeavesPendingJobs(t *testing.T) {
	c := New(WithWorkers(1))

	var runs atomic.Int32
	started := make(chan struct{})
	c.Register("work", func(ctx context.Context, payload []byte) error {
		runs.Add(1)
		select {
		case <-started: // already signalled
		default:
			close(started)
		}
		time.Sleep(50 * time.Millisecond)
		return nil
	})
	c.Start()

	const total = 10
	for i := 0; i < total; i++ {
		if _, err := c.Enqueue(context.Background(), Job{Type: "work"}); err != nil {
			t.Fatal(err)
		}
	}
	<-started // the single worker has picked up exactly one job

	if err := c.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := runs.Load(); got != 1 {
		t.Fatalf("processed = %d, want 1 (default shutdown only waits for the running handler)", got)
	}
	if got := c.Queue().Len(); got != total-1 {
		t.Fatalf("queue length = %d, want %d (pending jobs survive a non-drain shutdown)", got, total-1)
	}
}

// TestDrainShutdownRespectsContext verifies that a drain-mode Shutdown still
// honours its context: a handler that never returns must not block Shutdown
// forever — it returns ctx.Err() and the worker finishes on its own schedule.
func TestDrainShutdownRespectsContext(t *testing.T) {
	c := New(WithWorkers(1), WithDrainOnShutdown(true))

	release := make(chan struct{})
	c.Register("block", func(ctx context.Context, payload []byte) error {
		<-release
		return nil
	})
	c.Start()

	if _, err := c.Enqueue(context.Background(), Job{Type: "block"}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := c.Shutdown(ctx); err == nil {
		t.Fatal("Shutdown should time out while the handler is stuck")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("Shutdown took %v, want ~the context timeout", elapsed)
	}

	close(release) // let the worker finish and exit on its own
}

// TestPanicJobGoesToDeadAndWorkerSurvives verifies the panic-recovery path
// end to end: a handler that panics on every attempt exhausts its retries and
// lands in the DLQ, the worker goroutine survives, and the same pool keeps
// processing normal jobs afterwards.
func TestPanicJobGoesToDeadAndWorkerSurvives(t *testing.T) {
	var dead atomic.Int32
	var succeeded atomic.Int32
	c := New(WithWorkers(1), WithOnDead(func(info JobInfo) {
		dead.Add(1)
	}))
	defer c.Shutdown(context.Background())

	c.Register("boom", func(ctx context.Context, payload []byte) error {
		panic("handler panics every time")
	})
	c.Register("ok", func(ctx context.Context, payload []byte) error {
		succeeded.Add(1)
		return nil
	})
	c.Start()

	if _, err := c.Enqueue(context.Background(), Job{Type: "boom", MaxRetry: 2}); err != nil {
		t.Fatal(err)
	}
	// 1 run + 2 retries, all panicking, then the job dies.
	waitFor(t, 3*time.Second, func() bool { return dead.Load() == 1 })
	if got := len(c.Queue().Dead()); got != 1 {
		t.Fatalf("dead count = %d, want 1", got)
	}

	// The worker must still be alive and process fresh jobs.
	if _, err := c.Enqueue(context.Background(), Job{Type: "ok"}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool { return succeeded.Load() == 1 })
}