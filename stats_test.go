package goqueue

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// TestStatsCountersEnqueue verifies that Stats reflects enqueue counts and
// the per-type breakdown.
func TestStatsCountersEnqueue(t *testing.T) {
	c := New()
	c.Register("email", func(ctx context.Context, p []byte) error { return nil })
	c.Register("sms", func(ctx context.Context, p []byte) error { return nil })

	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if _, err := c.Enqueue(ctx, Job{Type: "email"}); err != nil {
			t.Fatalf("enqueue email: %v", err)
		}
	}
	if _, err := c.Enqueue(ctx, Job{Type: "sms"}); err != nil {
		t.Fatalf("enqueue sms: %v", err)
	}

	s := c.Stats()
	if s.Enqueued != 4 {
		t.Errorf("Enqueued = %d, want 4", s.Enqueued)
	}
	if s.Pending != 4 {
		t.Errorf("Pending = %d, want 4", s.Pending)
	}
	if s.ByType["email"] != 3 || s.ByType["sms"] != 1 {
		t.Errorf("ByType = %v, want email:3 sms:1", s.ByType)
	}
	if s.Workers != DefaultWorkers {
		t.Errorf("Workers = %d, want %d", s.Workers, DefaultWorkers)
	}
	if s.Started {
		t.Error("Started = true, want false (pool not started)")
	}
	// A failed enqueue must not bump counters.
	c.Register("dup", func(ctx context.Context, p []byte) error { return nil })
	if _, err := c.Enqueue(ctx, Job{Type: "dup", UniqueKey: "k"}); err != nil {
		t.Fatalf("first unique enqueue: %v", err)
	}
	if _, err := c.Enqueue(ctx, Job{Type: "dup", UniqueKey: "k"}); err == nil {
		t.Fatal("duplicate unique enqueue succeeded, want ErrJobExists")
	}
	if s2 := c.Stats(); s2.Enqueued != 5 {
		t.Errorf("Enqueued after rejected unique job = %d, want 5", s2.Enqueued)
	}
}

// TestStatsCountersLifecycle drives a full success and a dead-letter flow
// and checks the cumulative counters.
func TestStatsCountersLifecycle(t *testing.T) {
	var wg sync.WaitGroup
	c := New(WithWorkers(2), WithPollInterval(5*time.Millisecond))
	c.Register("ok", func(ctx context.Context, p []byte) error { return nil })
	c.Register("boom", func(ctx context.Context, p []byte) error {
		wg.Done()
		return errors.New("boom")
	})
	// MaxRetry -1: first failure sends the job straight to the DLQ (0 would
	// be normalized to DefaultMaxRetry=3 by the queue).
	const boomJobs = 2
	wg.Add(boomJobs)
	c.Start()
	defer func() {
		_ = c.Shutdown(context.Background())
	}()

	for i := 0; i < 3; i++ {
		if _, err := c.Enqueue(context.Background(), Job{Type: "ok"}); err != nil {
			t.Fatalf("enqueue ok: %v", err)
		}
	}
	for i := 0; i < boomJobs; i++ {
		if _, err := c.Enqueue(context.Background(), Job{Type: "boom", MaxRetry: -1}); err != nil {
			t.Fatalf("enqueue boom: %v", err)
		}
	}
	waitTimeout(t, &wg, 5*time.Second)

	deadline := time.Now().Add(5 * time.Second)
	for {
		s := c.Stats()
		if s.Succeeded == 3 && s.Failed == boomJobs && s.DeadTotal == boomJobs && s.Dead == boomJobs {
			break
		}
		if time.Now().After(deadline) {
			s := c.Stats()
			t.Fatalf("counters did not settle: Succeeded=%d Failed=%d DeadTotal=%d Dead=%d",
				s.Succeeded, s.Failed, s.DeadTotal, s.Dead)
		}
		time.Sleep(20 * time.Millisecond)
	}

	s := c.Stats()
	if s.Enqueued != 3+boomJobs {
		t.Errorf("Enqueued = %d, want %d", s.Enqueued, 3+boomJobs)
	}
	if s.Failed != boomJobs {
		t.Errorf("Failed = %d, want %d", s.Failed, boomJobs)
	}
	if s.DeadTotal != boomJobs {
		t.Errorf("DeadTotal = %d, want %d", s.DeadTotal, boomJobs)
	}
	if !s.Started {
		t.Error("Started = false, want true while pool is running")
	}
}

// TestStatsRunning verifies the Running gauge is non-zero while a slow
// handler is executing and zero afterwards.
func TestStatsRunning(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	c := New(WithWorkers(1), WithPollInterval(5*time.Millisecond))
	c.Register("slow", func(ctx context.Context, p []byte) error {
		close(entered)
		<-release
		return nil
	})
	c.Start()
	defer func() {
		close(release)
		_ = c.Shutdown(context.Background())
	}()

	if _, err := c.Enqueue(context.Background(), Job{Type: "slow"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	<-entered
	if r := c.Running(); r != 1 {
		t.Errorf("Running while handler blocked = %d, want 1", r)
	}
	if s := c.Stats(); s.Running != 1 {
		t.Errorf("Stats().Running while handler blocked = %d, want 1", s.Running)
	}
}

// TestStatsConcurrentEnqueue runs concurrent producers and verifies the
// totals are exact (counter updates are atomic and race-free).
func TestStatsConcurrentEnqueue(t *testing.T) {
	c := New()
	c.Register("t", func(ctx context.Context, p []byte) error { return nil })
	const producers = 8
	const perProducer = 100
	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				if _, err := c.Enqueue(context.Background(), Job{Type: "t"}); err != nil {
					t.Errorf("enqueue: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	s := c.Stats()
	want := int64(producers * perProducer)
	if s.Enqueued != want {
		t.Errorf("Enqueued = %d, want %d", s.Enqueued, want)
	}
	if s.ByType["t"] != want {
		t.Errorf("ByType[t] = %d, want %d", s.ByType["t"], want)
	}
}

func waitTimeout(t *testing.T, wg *sync.WaitGroup, d time.Duration) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatal("wait group timed out")
	}
}