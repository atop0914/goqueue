package goqueue

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestEnqueueDequeueBasic(t *testing.T) {
	q := NewInMemoryQueue()
	defer q.Close()

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if _, err := q.Enqueue(ctx, Job{Type: "t", Payload: []byte{byte(i)}}); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}
	if got := q.Len(); got != 5 {
		t.Fatalf("Len = %d, want 5", got)
	}
	for i := 0; i < 5; i++ {
		dj, err := q.Dequeue(ctx)
		if err != nil {
			t.Fatalf("dequeue %d: %v", i, err)
		}
		if dj.Payload[0] != byte(i) {
			t.Fatalf("payload = %d, want %d (FIFO order)", dj.Payload[0], i)
		}
		if dj.Attempt != 1 {
			t.Fatalf("Attempt = %d, want 1", dj.Attempt)
		}
		if err := q.Ack(ctx, dj.ID); err != nil {
			t.Fatalf("ack %d: %v", i, err)
		}
	}
	if got := q.Len(); got != 0 {
		t.Fatalf("Len after ack = %d, want 0", got)
	}
}

func TestPriorityOrdering(t *testing.T) {
	q := NewInMemoryQueue()
	defer q.Close()

	ctx := context.Background()
	// Enqueue low first, then high: priority must override FIFO.
	mustEnqueue(t, q, ctx, Job{Type: "t", Payload: []byte("low")})
	mustEnqueue(t, q, ctx, Job{Type: "t", Payload: []byte("high"), Priority: 10})
	mustEnqueue(t, q, ctx, Job{Type: "t", Payload: []byte("mid"), Priority: 5})

	got := []string{}
	for i := 0; i < 3; i++ {
		dj, err := q.Dequeue(ctx)
		if err != nil {
			t.Fatalf("dequeue %d: %v", i, err)
		}
		got = append(got, string(dj.Payload))
	}
	want := []string{"high", "mid", "low"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order = %v, want %v", got, want)
		}
	}
}

func TestDelayedJob(t *testing.T) {
	q := NewInMemoryQueue()
	defer q.Close()

	ctx := context.Background()
	mustEnqueue(t, q, ctx, Job{Type: "t", Payload: []byte("later"), RunAfter: time.Now().Add(80 * time.Millisecond)})
	mustEnqueue(t, q, ctx, Job{Type: "t", Payload: []byte("now")})

	// The immediate job must come out first even though it was enqueued last.
	dj, err := q.Dequeue(ctx)
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if string(dj.Payload) != "now" {
		t.Fatalf("first = %q, want %q", dj.Payload, "now")
	}

	start := time.Now()
	dj, err = q.Dequeue(ctx)
	if err != nil {
		t.Fatalf("dequeue delayed: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 60*time.Millisecond {
		t.Fatalf("delayed job returned after %v, want >= 60ms", elapsed)
	}
	if string(dj.Payload) != "later" {
		t.Fatalf("second = %q, want %q", dj.Payload, "later")
	}
}

func TestNackRetryThenDead(t *testing.T) {
	q := NewInMemoryQueue()
	defer q.Close()

	ctx := context.Background()
	id, err := q.Enqueue(ctx, Job{Type: "t", MaxRetry: 2})
	if err != nil {
		t.Fatal(err)
	}

	sentinel := errors.New("boom")
	for attempt := 1; attempt <= 3; attempt++ {
		dj, err := q.Dequeue(ctx)
		if err != nil {
			t.Fatalf("dequeue attempt %d: %v", attempt, err)
		}
		if dj.ID != id {
			t.Fatalf("got job %s, want %s", dj.ID, id)
		}
		if dj.Attempt != attempt {
			t.Fatalf("Attempt = %d, want %d", dj.Attempt, attempt)
		}
		if err := q.Nack(ctx, dj.ID, sentinel, true); err != nil {
			t.Fatalf("nack attempt %d: %v", attempt, err)
		}
	}

	if dead := q.Dead(); len(dead) != 1 {
		t.Fatalf("Dead() = %d jobs, want 1", len(dead))
	} else {
		if dead[0].State != StateDead || dead[0].LastError != "boom" {
			t.Fatalf("dead job info = %+v", dead[0])
		}
		if dead[0].Attempts != 3 {
			t.Fatalf("dead attempts = %d, want 3", dead[0].Attempts)
		}
	}
}

func TestNackNonRetryableGoesToDead(t *testing.T) {
	q := NewInMemoryQueue()
	defer q.Close()

	ctx := context.Background()
	mustEnqueue(t, q, ctx, Job{Type: "t", MaxRetry: 5})

	dj, err := q.Dequeue(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Nack(ctx, dj.ID, errors.New("permanent"), false); err != nil {
		t.Fatal(err)
	}
	if dead := q.Dead(); len(dead) != 1 {
		t.Fatalf("Dead() = %d, want 1", len(dead))
	}
	if q.Len() != 0 {
		t.Fatalf("Len = %d, want 0", q.Len())
	}
}

func TestAckUnknownJob(t *testing.T) {
	q := NewInMemoryQueue()
	defer q.Close()
	if err := q.Ack(context.Background(), "nope"); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("Ack = %v, want ErrJobNotFound", err)
	}
}

func TestDequeueContextCancel(t *testing.T) {
	q := NewInMemoryQueue()
	defer q.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := q.Dequeue(ctx); err == nil {
		t.Fatal("Dequeue with cancelled ctx: want error")
	}
}

func TestDequeueAfterClose(t *testing.T) {
	q := NewInMemoryQueue()
	q.Close()
	if _, err := q.Dequeue(context.Background()); !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("Dequeue = %v, want ErrQueueClosed", err)
	}
}

func TestEnqueueAfterClose(t *testing.T) {
	q := NewInMemoryQueue()
	q.Close()
	if _, err := q.Enqueue(context.Background(), Job{Type: "t"}); !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("Enqueue = %v, want ErrQueueClosed", err)
	}
}

func TestCloseUnblocksDequeue(t *testing.T) {
	q := NewInMemoryQueue()
	done := make(chan error, 1)
	go func() {
		_, err := q.Dequeue(context.Background())
		done <- err
	}()
	time.Sleep(20 * time.Millisecond)
	q.Close()
	select {
	case err := <-done:
		if !errors.Is(err, ErrQueueClosed) {
			t.Fatalf("Dequeue = %v, want ErrQueueClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Dequeue not unblocked by Close")
	}
}

func TestConcurrentEnqueueDequeue(t *testing.T) {
	q := NewInMemoryQueue()
	defer q.Close()
	ctx := context.Background()

	const total = 2000
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := 0; i < total; i++ {
			if _, err := q.Enqueue(ctx, Job{Type: "t"}); err != nil {
				t.Errorf("enqueue: %v", err)
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < total; i++ {
			dj, err := q.Dequeue(ctx)
			if err != nil {
				t.Errorf("dequeue: %v", err)
				return
			}
			_ = q.Ack(ctx, dj.ID)
		}
	}()

	wg.Wait()
	if got := q.Len(); got != 0 {
		t.Fatalf("Len = %d, want 0", got)
	}
}

func mustEnqueue(t *testing.T, q Queue, ctx context.Context, job Job) {
	t.Helper()
	if _, err := q.Enqueue(ctx, job); err != nil {
		t.Fatalf("enqueue %+v: %v", job, err)
	}
}
