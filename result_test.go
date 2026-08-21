package goqueue

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestResult_Success(t *testing.T) {
	c := New(WithWorkers(1))
	defer c.Shutdown(context.Background())

	RegisterWithResult(c, "sum", func(ctx context.Context, payload []byte) (int, error) {
		a := int(payload[0] - '0')
		b := int(payload[1] - '0')
		return a + b, nil
	})
	c.Start()

	id, err := c.Enqueue(context.Background(), Job{Type: "sum", Payload: []byte("23")})
	if err != nil {
		t.Fatal(err)
	}

	got, err := Task[int](c, id).Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got != 5 {
		t.Fatalf("result = %d, want 5", got)
	}
}

func TestResult_HandlerError(t *testing.T) {
	c := New(WithWorkers(1))
	defer c.Shutdown(context.Background())

	RegisterWithResult(c, "div", func(ctx context.Context, payload []byte) (float64, error) {
		return 0, errors.New("division by zero")
	})
	c.Start()

	id, err := c.Enqueue(context.Background(), Job{Type: "div", MaxRetry: 0})
	if err != nil {
		t.Fatal(err)
	}

	_, err = Task[float64](c, id).Get(context.Background())
	if err == nil || err.Error() != "division by zero" {
		t.Fatalf("Get err = %v, want handler error", err)
	}
}

func TestResult_RetryThenSuccessPublishesFinalValue(t *testing.T) {
	c := New(WithWorkers(1))
	defer c.Shutdown(context.Background())

	var attempts atomic.Int32
	RegisterWithResult(c, "flaky", func(ctx context.Context, payload []byte) (string, error) {
		if attempts.Add(1) == 1 {
			return "", errors.New("first try fails")
		}
		return "second-try-ok", nil
	})
	c.Start()

	id, err := c.Enqueue(context.Background(), Job{Type: "flaky", MaxRetry: 2})
	if err != nil {
		t.Fatal(err)
	}

	// Use a short poll: the first attempt publishes an error result, then the
	// retry overwrites it with the success value. Get should eventually see
	// the final value.
	deadline := time.Now().Add(3 * time.Second)
	for {
		v, _, _ := Task[string](c, id).TryGet()
		if v == "second-try-ok" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("final result not observed, last (v=%q err=%v)", v, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if attempts.Load() != 2 {
		t.Fatalf("attempts = %d, want 2", attempts.Load())
	}
}

func TestResult_GetTimeout(t *testing.T) {
	c := New(WithWorkers(1))
	defer c.Shutdown(context.Background())

	RegisterWithResult(c, "slow", func(ctx context.Context, payload []byte) (int, error) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(500 * time.Millisecond):
			return 42, nil
		}
	})
	c.Start()

	id, err := c.Enqueue(context.Background(), Job{Type: "slow"})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err = Task[int](c, id).Get(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Get err = %v, want DeadlineExceeded", err)
	}
}

func TestResult_TryGetNotReadyThenReady(t *testing.T) {
	c := New(WithWorkers(1))
	defer c.Shutdown(context.Background())

	started := make(chan struct{})
	release := make(chan struct{})
	RegisterWithResult(c, "gate", func(ctx context.Context, payload []byte) (int, error) {
		close(started)
		<-release
		return 99, nil
	})
	c.Start()

	id, err := c.Enqueue(context.Background(), Job{Type: "gate"})
	if err != nil {
		t.Fatal(err)
	}
	<-started

	h := Task[int](c, id)
	if _, _, ok := h.TryGet(); ok {
		t.Fatal("TryGet ok=true before handler finished")
	}
	if h.Done() {
		t.Fatal("Done()=true before handler finished")
	}

	close(release)
	got, err := h.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got != 99 {
		t.Fatalf("result = %d, want 99", got)
	}
	if !h.Done() {
		t.Fatal("Done()=false after completion")
	}
}

func TestResult_DoneBeforeEnqueue(t *testing.T) {
	// The handle must be usable before the job even exists.
	c := New(WithWorkers(1))
	defer c.Shutdown(context.Background())

	RegisterWithResult(c, "t", func(ctx context.Context, payload []byte) (string, error) {
		return "hello", nil
	})
	id := "pre-known-id"
	h := Task[string](c, id)
	if h.Done() {
		t.Fatal("Done()=true for unknown job")
	}

	c.Start()
	sid, err := c.Enqueue(context.Background(), Job{ID: id, Type: "t"})
	if err != nil {
		t.Fatal(err)
	}
	if sid != id {
		t.Fatalf("enqueue id = %q, want %q", sid, id)
	}

	got, err := h.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got != "hello" {
		t.Fatalf("result = %q, want hello", got)
	}
}

func TestResult_PanicPublishesErrorAndRetries(t *testing.T) {
	c := New(WithWorkers(1))
	defer c.Shutdown(context.Background())

	var attempts atomic.Int32
	RegisterWithResult(c, "panic", func(ctx context.Context, payload []byte) (int, error) {
		if attempts.Add(1) == 1 {
			panic("boom")
		}
		return 7, nil
	})
	c.Start()

	id, err := c.Enqueue(context.Background(), Job{Type: "panic", MaxRetry: 2})
	if err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for {
		v, _, _ := Task[int](c, id).TryGet()
		if v == 7 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("final result not observed, last (v=%d)", v)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if attempts.Load() != 2 {
		t.Fatalf("attempts = %d, want 2", attempts.Load())
	}
}

func TestResult_TypeMismatch(t *testing.T) {
	c := New(WithWorkers(1))
	defer c.Shutdown(context.Background())

	RegisterWithResult(c, "int", func(ctx context.Context, payload []byte) (int, error) {
		return 1, nil
	})
	c.Start()

	id, err := c.Enqueue(context.Background(), Job{Type: "int"})
	if err != nil {
		t.Fatal(err)
	}

	_, err = Task[string](c, id).Get(context.Background())
	if err == nil {
		t.Fatal("expected type mismatch error")
	}
}

func TestResult_ConcurrentAccess(t *testing.T) {
	const n = 50
	c := New(WithWorkers(8))
	defer c.Shutdown(context.Background())

	RegisterWithResult(c, "sq", func(ctx context.Context, payload []byte) (int, error) {
		v, _ := strconv.Atoi(string(payload))
		return v * v, nil
	})
	c.Start()

	ids := make([]string, n)
	for i := 0; i < n; i++ {
		id, err := c.Enqueue(context.Background(), Job{Type: "sq", Payload: []byte(strconv.Itoa(i))})
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = id
	}

	var wg sync.WaitGroup
	errCh := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			got, err := Task[int](c, ids[i]).Get(context.Background())
			if err != nil {
				errCh <- err
				return
			}
			want := i * i
			if got != want {
				errCh <- fmt.Errorf("job %d: got %d, want %d", i, got, want)
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}

func TestResult_DeadJobStoresFinalError(t *testing.T) {
	c := New(WithWorkers(1))
	defer c.Shutdown(context.Background())

	RegisterWithResult(c, "always", func(ctx context.Context, payload []byte) (int, error) {
		return 1, errors.New("always fails")
	})
	c.Start()

	id, err := c.Enqueue(context.Background(), Job{Type: "always", MaxRetry: 1})
	if err != nil {
		t.Fatal(err)
	}

	// MaxRetry=1: 2 attempts, both fail, job dies. The error result is
	// published when the handler returns, which is slightly before the job
	// record moves into the DLQ — so wait for the DLQ state first, then
	// assert on the stored error.
	deadline := time.Now().Add(3 * time.Second)
	for len(c.Queue().Dead()) != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("dead jobs = %d, want 1", len(c.Queue().Dead()))
		}
		time.Sleep(10 * time.Millisecond)
	}
	_, err, _ = Task[int](c, id).TryGet()
	if err == nil || err.Error() != "always fails" {
		t.Fatalf("stored error = %v, want always fails", err)
	}
}

func TestResult_ResultIsolation(t *testing.T) {
	// Different jobs with different result types must not interfere.
	c := New(WithWorkers(2))
	defer c.Shutdown(context.Background())

	RegisterWithResult(c, "str", func(ctx context.Context, payload []byte) (string, error) {
		return "s:" + string(payload), nil
	})
	RegisterWithResult(c, "num", func(ctx context.Context, payload []byte) (int, error) {
		v, _ := strconv.Atoi(string(payload))
		return v * 10, nil
	})
	c.Start()

	sid, err := c.Enqueue(context.Background(), Job{Type: "str", Payload: []byte("a")})
	if err != nil {
		t.Fatal(err)
	}
	nid, err := c.Enqueue(context.Background(), Job{Type: "num", Payload: []byte("3")})
	if err != nil {
		t.Fatal(err)
	}

	if s, err := Task[string](c, sid).Get(context.Background()); err != nil || s != "s:a" {
		t.Fatalf("string result = %q, %v", s, err)
	}
	if n, err := Task[int](c, nid).Get(context.Background()); err != nil || n != 30 {
		t.Fatalf("int result = %d, %v", n, err)
	}
}