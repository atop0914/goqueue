package goqueue

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestRegisterAndProcess(t *testing.T) {
	c := New(WithWorkers(2))
	defer c.Shutdown(context.Background())

	var got atomic.Value
	c.Register("echo", func(ctx context.Context, payload []byte) error {
		got.Store(string(payload))
		return nil
	})
	c.Start()

	id, err := c.Enqueue(context.Background(), Job{Type: "echo", Payload: []byte("hello")})
	if err != nil {
		t.Fatal(err)
	}
	if id == "" {
		t.Fatal("enqueue returned empty id")
	}

	waitFor(t, 2*time.Second, func() bool {
		v := got.Load()
		return v != nil && v.(string) == "hello"
	})
}

func TestEnqueueUnknownType(t *testing.T) {
	c := New()
	defer c.Shutdown(context.Background())
	c.Register("known", func(ctx context.Context, payload []byte) error { return nil })

	if _, err := c.Enqueue(context.Background(), Job{Type: "nope"}); !errors.Is(err, ErrUnknownType) {
		t.Fatalf("Enqueue = %v, want ErrUnknownType", err)
	}
}

func TestRetryUntilSuccess(t *testing.T) {
	c := New(WithWorkers(1))
	defer c.Shutdown(context.Background())

	var attempts atomic.Int32
	c.Register("flaky", func(ctx context.Context, payload []byte) error {
		n := attempts.Add(1)
		if n < 3 {
			return errors.New("not yet")
		}
		return nil
	})
	c.Start()

	_, err := c.Enqueue(context.Background(), Job{Type: "flaky", MaxRetry: 5})
	if err != nil {
		t.Fatal(err)
	}

	waitFor(t, 2*time.Second, func() bool { return attempts.Load() == 3 })
}

func TestMaxRetryMovesToDead(t *testing.T) {
	var dead atomic.Int32
	c := New(WithWorkers(1), WithOnDead(func(info JobInfo) {
		dead.Add(1)
	}))
	defer c.Shutdown(context.Background())

	var attempts atomic.Int32
	c.Register("fail", func(ctx context.Context, payload []byte) error {
		attempts.Add(1)
		return errors.New("always fails")
	})
	c.Start()

	_, err := c.Enqueue(context.Background(), Job{Type: "fail", MaxRetry: 2})
	if err != nil {
		t.Fatal(err)
	}

	waitFor(t, 2*time.Second, func() bool { return dead.Load() == 1 })
	if got := attempts.Load(); got != 3 {
		t.Fatalf("attempts = %d, want 3 (1 run + 2 retries)", got)
	}
	if got := len(c.Queue().Dead()); got != 1 {
		t.Fatalf("Dead() = %d, want 1", got)
	}
}

func TestPanicRecovery(t *testing.T) {
	c := New(WithWorkers(1))
	defer c.Shutdown(context.Background())

	var attempts atomic.Int32
	c.Register("panic", func(ctx context.Context, payload []byte) error {
		if attempts.Add(1) == 1 {
			panic("handler blew up")
		}
		return nil
	})
	c.Start()

	_, err := c.Enqueue(context.Background(), Job{Type: "panic", MaxRetry: 2})
	if err != nil {
		t.Fatal(err)
	}

	// The panicking run must not kill the worker: a retry succeeds.
	waitFor(t, 2*time.Second, func() bool { return attempts.Load() == 2 })
}

func TestHandlerTimeout(t *testing.T) {
	c := New(WithWorkers(1))
	defer c.Shutdown(context.Background())

	var attempts atomic.Int32
	c.Register("slow", func(ctx context.Context, payload []byte) error {
		attempts.Add(1)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
			return nil
		}
	})
	c.Start()

	_, err := c.Enqueue(context.Background(), Job{Type: "slow", MaxRetry: 1, Timeout: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}

	// Attempt 1 times out, attempt 2 also times out -> dead after 2 runs.
	waitFor(t, 2*time.Second, func() bool { return len(c.Queue().Dead()) == 1 })
	if got := attempts.Load(); got != 2 {
		t.Fatalf("attempts = %d, want 2", got)
	}
}

func TestGracefulShutdown(t *testing.T) {
	c := New(WithWorkers(2))

	started := make(chan struct{})
	release := make(chan struct{})
	c.Register("block", func(ctx context.Context, payload []byte) error {
		close(started)
		<-release // hold the worker open until Shutdown is called
		return nil
	})
	c.Start()

	if _, err := c.Enqueue(context.Background(), Job{Type: "block"}); err != nil {
		t.Fatal(err)
	}
	<-started

	done := make(chan struct{})
	go func() {
		c.Shutdown(context.Background())
		close(done)
	}()
	// Shutdown must block while the handler is running...
	select {
	case <-done:
		t.Fatal("Shutdown returned while handler was still running")
	case <-time.After(50 * time.Millisecond):
	}
	// ...and return once it finishes.
	close(release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown did not return after handler finished")
	}
}

func TestMultipleWorkersConcurrent(t *testing.T) {
	c := New(WithWorkers(8))
	defer c.Shutdown(context.Background())

	var inflight atomic.Int32
	var maxInflight atomic.Int32
	c.Register("work", func(ctx context.Context, payload []byte) error {
		n := inflight.Add(1)
		for {
			m := maxInflight.Load()
			if n <= m || maxInflight.CompareAndSwap(m, n) {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
		inflight.Add(-1)
		return nil
	})
	c.Start()

	for i := 0; i < 64; i++ {
		if _, err := c.Enqueue(context.Background(), Job{Type: "work"}); err != nil {
			t.Fatal(err)
		}
	}

	waitFor(t, 3*time.Second, func() bool { return c.Queue().Len() == 0 })
	if got := maxInflight.Load(); got < 2 {
		t.Fatalf("max inflight = %d, want >= 2 (workers should run concurrently)", got)
	}
	if got := maxInflight.Load(); got > 8 {
		t.Fatalf("max inflight = %d, want <= 8", got)
	}
}

func TestStartIdempotentAndWorkerCount(t *testing.T) {
	c := New(WithWorkers(4))
	defer c.Shutdown(context.Background())

	var runs atomic.Int32
	c.Register("t", func(ctx context.Context, payload []byte) error {
		runs.Add(1)
		time.Sleep(20 * time.Millisecond)
		return nil
	})
	c.Start()
	c.Start() // second Start must not spawn more workers

	for i := 0; i < 16; i++ {
		if _, err := c.Enqueue(context.Background(), Job{Type: "t"}); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, 3*time.Second, func() bool { return runs.Load() == 16 })
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}
