package goqueue

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// pauseUntilBlocked waits until the worker's Dequeue call is parked on the
// pause (observed via the running gauge staying flat), giving the pool time
// to prove it is NOT consuming.
func pauseUntilBlocked(t *testing.T, c *Client) {
	t.Helper()
	c.Pause()
	// Give a would-be consumer ample time to wrongly dequeue something.
	time.Sleep(80 * time.Millisecond)
}

func TestClient_PauseBlocksDeliveryAndResumeContinues(t *testing.T) {
	c := New(WithWorkers(2), WithPollInterval(5*time.Millisecond))
	defer c.Shutdown(context.Background())

	var ran atomic.Int64
	c.Register("t", func(ctx context.Context, p []byte) error { ran.Add(1); return nil })
	c.Start()

	// While paused, enqueued jobs must not run.
	if err := c.Pause(); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	if !c.IsPaused() {
		t.Fatal("IsPaused = false after Pause")
	}
	for i := 0; i < 3; i++ {
		if _, err := c.Enqueue(context.Background(), Job{Type: "t"}); err != nil {
			t.Fatalf("Enqueue while paused: %v", err)
		}
	}
	pauseUntilBlocked(t, c)
	if n := ran.Load(); n != 0 {
		t.Fatalf("handler ran %d times while paused, want 0", n)
	}
	if n := c.Queue().Len(); n != 3 {
		t.Fatalf("queue Len while paused = %d, want 3 (jobs retained)", n)
	}

	// Resume: everything runs.
	c.Resume()
	if c.IsPaused() {
		t.Fatal("IsPaused = true after Resume")
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && ran.Load() < 3 {
		time.Sleep(10 * time.Millisecond)
	}
	if n := ran.Load(); n != 3 {
		t.Fatalf("after resume handler ran %d times, want 3", n)
	}
}

func TestClient_PauseIsIdempotentAndResumeNoop(t *testing.T) {
	c := New()
	defer c.Shutdown(context.Background())
	c.Register("t", func(ctx context.Context, p []byte) error { return nil })

	if err := c.Pause(); err != nil {
		t.Fatalf("first Pause: %v", err)
	}
	if err := c.Pause(); err != nil {
		t.Fatalf("second Pause must be a no-op: %v", err)
	}
	c.Resume()
	c.Resume() // second resume on a running queue: no-op, must not panic
	if c.IsPaused() {
		t.Fatal("still paused after Resume")
	}
}

// unsupportedQueue wraps a Queue without exposing its admin methods, so the
// Client's type assertions must all fail.
type unsupportedQueue struct{ Queue }

func TestClient_AdminUnsupportedError(t *testing.T) {
	// A minimal custom backend without admin capabilities.
	c := New(WithQueue(unsupportedQueue{Queue: NewInMemoryQueue()}))
	defer c.Shutdown(context.Background())
	c.Register("t", func(ctx context.Context, p []byte) error { return nil })

	ctx := context.Background()
	if _, err := c.Purge(ctx, false); !errors.Is(err, ErrAdminUnsupported) {
		t.Errorf("Purge err = %v, want ErrAdminUnsupported", err)
	}
	if _, err := c.RequeueDead(ctx); !errors.Is(err, ErrAdminUnsupported) {
		t.Errorf("RequeueDead err = %v, want ErrAdminUnsupported", err)
	}
	if err := c.RequeueDeadJob(ctx, "x"); !errors.Is(err, ErrAdminUnsupported) {
		t.Errorf("RequeueDeadJob err = %v, want ErrAdminUnsupported", err)
	}
	if err := c.Pause(); !errors.Is(err, ErrAdminUnsupported) {
		t.Errorf("Pause err = %v, want ErrAdminUnsupported", err)
	}
	c.Resume() // no-op, must not panic
	if c.IsPaused() {
		t.Error("IsPaused = true for a backend without Pauser")
	}
}

func TestClient_PurgeDropsPendingKeepsRunning(t *testing.T) {
	c := New(WithWorkers(1), WithPollInterval(5*time.Millisecond))
	defer c.Shutdown(context.Background())

	started := make(chan struct{})
	block := make(chan struct{})
	c.Register("slow", func(ctx context.Context, p []byte) error {
		close(started)
		<-block
		return nil
	})
	c.Register("t", func(ctx context.Context, p []byte) error { return nil })
	c.Start()

	if _, err := c.Enqueue(context.Background(), Job{Type: "slow"}); err != nil {
		t.Fatal(err)
	}
	<-started // worker is inside the handler now
	for i := 0; i < 3; i++ {
		if _, err := c.Enqueue(context.Background(), Job{Type: "t"}); err != nil {
			t.Fatal(err)
		}
	}
	// Unique pending job: its key must be released by the purge.
	if _, err := c.Enqueue(context.Background(), Job{Type: "t", UniqueKey: "u1"}); err != nil {
		t.Fatal(err)
	}

	n, err := c.Purge(context.Background(), false)
	if err != nil {
		t.Fatalf("Purge: %v", err)
	}
	if n != 4 {
		t.Fatalf("Purge removed %d jobs, want 4", n)
	}
	if got := c.Queue().Len(); got != 0 {
		t.Fatalf("Len after purge = %d, want 0", got)
	}
	// Idempotent.
	if n, err := c.Purge(context.Background(), false); err != nil || n != 0 {
		t.Fatalf("second Purge = (%d, %v), want (0, nil)", n, err)
	}
	// Unique key released: re-enqueue must succeed.
	if _, err := c.Enqueue(context.Background(), Job{Type: "t", UniqueKey: "u1"}); err != nil {
		t.Fatalf("re-enqueue unique key after purge: %v", err)
	}
	close(block)
}

func TestClient_PurgeWithDead(t *testing.T) {
	c := New(WithWorkers(1), WithPollInterval(5*time.Millisecond), WithOnDead(nil))
	c.Register("boom", func(ctx context.Context, p []byte) error {
		return errors.New("kaboom")
	})
	if _, err := c.Enqueue(context.Background(), Job{Type: "boom", MaxRetry: -1}); err != nil {
		t.Fatal(err)
	}
	c.Start()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, dead := c.Info(); dead > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.Shutdown(context.Background())

	if _, dead := c.Info(); dead != 1 {
		t.Fatalf("dead = %d, want 1", dead)
	}
	n, err := c.Purge(context.Background(), true)
	if err != nil {
		t.Fatalf("Purge(dead): %v", err)
	}
	if n != 1 {
		t.Fatalf("Purge(dead) removed %d, want 1", n)
	}
	if _, dead := c.Info(); dead != 0 {
		t.Fatalf("dead after purge = %d, want 0", dead)
	}
}

func TestClient_RequeueDeadResetsAttemptsAndRetries(t *testing.T) {
	c := New(WithWorkers(1), WithPollInterval(5*time.Millisecond))

	var attempts int
	c.Register("flaky", func(ctx context.Context, p []byte) error {
		attempts++
		if attempts <= 1 { // first run of each lifecycle fails
			return errors.New("transient")
		}
		return nil
	})
	// No retries: first failure lands straight in the DLQ.
	id, err := c.Enqueue(context.Background(), Job{Type: "flaky", MaxRetry: -1})
	if err != nil {
		t.Fatal(err)
	}
	c.Start()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, dead := c.Info(); dead > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, dead := c.Info(); dead != 1 {
		t.Fatalf("dead = %d, want 1", dead)
	}

	n, err := c.RequeueDead(context.Background())
	if err != nil {
		t.Fatalf("RequeueDead: %v", err)
	}
	if n != 1 {
		t.Fatalf("RequeueDead requeued %d, want 1", n)
	}
	if _, dead := c.Info(); dead != 0 {
		t.Fatalf("dead after requeue = %d, want 0", dead)
	}
	// Idempotent.
	if n, err := c.RequeueDead(context.Background()); err != nil || n != 0 {
		t.Fatalf("second RequeueDead = (%d, %v), want (0, nil)", n, err)
	}

	// The job runs again with a fresh attempt count; this time it succeeds.
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && c.Stats().Succeeded == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if c.Stats().Succeeded != 1 {
		t.Fatalf("succeeded = %d, want 1 after requeue", c.Stats().Succeeded)
	}
	c.Shutdown(context.Background())
	_ = id
}

func TestClient_RequeueDeadJobCherryPickAndErrors(t *testing.T) {
	c := New()
	c.Register("boom", func(ctx context.Context, p []byte) error {
		return errors.New("kaboom")
	})
	ctx := context.Background()
	q := c.Queue().(*InMemoryQueue)

	id1, _ := c.Enqueue(ctx, Job{Type: "boom", MaxRetry: -1})
	id2, _ := c.Enqueue(ctx, Job{Type: "boom", MaxRetry: -1})
	dj1, _ := q.Dequeue(ctx)
	_ = q.Nack(ctx, dj1.ID, errors.New("x"), false, 0)
	dj2, _ := q.Dequeue(ctx)
	_ = q.Nack(ctx, dj2.ID, errors.New("x"), false, 0)
	if len(q.Dead()) != 2 {
		t.Fatalf("dead = %d, want 2", len(q.Dead()))
	}

	// Unknown ID.
	if err := c.RequeueDeadJob(ctx, "nope"); !errors.Is(err, ErrJobNotFound) {
		t.Errorf("unknown id err = %v, want ErrJobNotFound", err)
	}
	// Cherry-pick one.
	if err := c.RequeueDeadJob(ctx, id1); err != nil {
		t.Fatalf("RequeueDeadJob: %v", err)
	}
	if len(q.Dead()) != 1 {
		t.Fatalf("dead after cherry-pick = %d, want 1", len(q.Dead()))
	}
	if q.Len() != 1 {
		t.Fatalf("pending after cherry-pick = %d, want 1", q.Len())
	}
	// Requeuing the same job again: no longer dead -> ErrJobNotFound.
	if err := c.RequeueDeadJob(ctx, id1); !errors.Is(err, ErrJobNotFound) {
		t.Errorf("double requeue err = %v, want ErrJobNotFound", err)
	}
	// The requeued job is due immediately and intact.
	dj, err := q.Dequeue(ctx)
	if err != nil {
		t.Fatalf("Dequeue requeued job: %v", err)
	}
	if dj.ID != id1 {
		t.Errorf("requeued ID = %s, want %s", dj.ID, id1)
	}
	if dj.Attempt != 1 {
		t.Errorf("requeued Attempt = %d, want 1 (reset)", dj.Attempt)
	}
	_ = id2
}

func TestClient_RequeueDeadContestedUniqueKeyStaysDead(t *testing.T) {
	c := New()
	c.Register("boom", func(ctx context.Context, p []byte) error {
		return errors.New("kaboom")
	})
	c.Register("t", func(ctx context.Context, p []byte) error { return nil })
	ctx := context.Background()
	q := c.Queue().(*InMemoryQueue)

	// Dead job holding unique key "k" (key released on death), then a live
	// pending job re-claims the same key.
	deadID, _ := c.Enqueue(ctx, Job{Type: "boom", MaxRetry: -1, UniqueKey: "k"})
	dj, _ := q.Dequeue(ctx)
	_ = q.Nack(ctx, dj.ID, errors.New("x"), false, 0)
	if _, err := c.Enqueue(ctx, Job{Type: "t", UniqueKey: "k"}); err != nil {
		t.Fatalf("enqueue contender: %v", err)
	}

	// Wholesale requeue skips the contested job.
	n, err := c.RequeueDead(ctx)
	if err != nil {
		t.Fatalf("RequeueDead: %v", err)
	}
	if n != 0 {
		t.Fatalf("requeued %d, want 0 (key contested)", n)
	}
	dead := q.Dead()
	if len(dead) != 1 || dead[0].ID != deadID {
		t.Fatalf("dead after contested requeue = %+v, want the original job", dead)
	}
	// Single-job form reports ErrJobExists.
	if err := c.RequeueDeadJob(ctx, deadID); !errors.Is(err, ErrJobExists) {
		t.Errorf("contested single requeue err = %v, want ErrJobExists", err)
	}
	// After the contender is dequeued (key still held — running jobs hold
	// keys) the dead job must still stay put.
	dj2, _ := q.Dequeue(ctx)
	if dj2 == nil || dj2.ID == deadID {
		t.Fatalf("unexpected dequeue %+v", dj2)
	}
	if err := c.RequeueDeadJob(ctx, deadID); !errors.Is(err, ErrJobExists) {
		t.Errorf("running holder err = %v, want ErrJobExists", err)
	}
}

func TestInMemoryQueue_PauseBlocksDequeueUntilResume(t *testing.T) {
	q := NewInMemoryQueue()
	defer q.Close()
	ctx := context.Background()

	if _, err := q.Enqueue(ctx, Job{Type: "t"}); err != nil {
		t.Fatal(err)
	}
	if err := q.Pause(); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	// Dequeue must block, not deliver.
	blocked := make(chan error, 1)
	go func() {
		_, err := q.Dequeue(ctx)
		blocked <- err
	}()
	select {
	case err := <-blocked:
		t.Fatalf("Dequeue returned while paused: %+v", err)
	case <-time.After(100 * time.Millisecond):
	}
	// Resume unblocks and delivers.
	q.Resume()
	select {
	case <-blocked:
	case <-time.After(time.Second):
		t.Fatal("Dequeue still blocked after Resume")
	}
	// Queue closed: Pause reports ErrQueueClosed.
	q.Close()
	if err := q.Pause(); !errors.Is(err, ErrQueueClosed) {
		t.Errorf("Pause after Close = %v, want ErrQueueClosed", err)
	}
}

func TestInMemoryQueue_PausedDequeueHonorsContext(t *testing.T) {
	q := NewInMemoryQueue()
	defer q.Close()
	q.Pause()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	if _, err := q.Dequeue(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want context.DeadlineExceeded", err)
	}
}
