package goqueue

import (
	"context"
	"errors"
	"testing"
)

func TestUniqueJobRejectsDuplicateKey(t *testing.T) {
	q := NewInMemoryQueue()
	defer q.Close()

	id1, err := q.Enqueue(context.Background(), Job{Type: "t", UniqueKey: "cache:user:1"})
	if err != nil {
		t.Fatalf("first enqueue: %v", err)
	}
	if id1 == "" {
		t.Fatal("first enqueue returned empty id")
	}
	// A second job with the same key while the first is pending must fail.
	if _, err := q.Enqueue(context.Background(), Job{Type: "t", UniqueKey: "cache:user:1"}); !errors.Is(err, ErrJobExists) {
		t.Fatalf("duplicate enqueue = %v, want ErrJobExists", err)
	}
	// A different key is allowed.
	if _, err := q.Enqueue(context.Background(), Job{Type: "t", UniqueKey: "cache:user:2"}); err != nil {
		t.Fatalf("different key enqueue: %v", err)
	}
}

func TestUniqueJobReleasedOnAck(t *testing.T) {
	q := NewInMemoryQueue()
	defer q.Close()

	key := "email:daily"
	if _, err := q.Enqueue(context.Background(), Job{Type: "t", UniqueKey: key}); err != nil {
		t.Fatal(err)
	}
	dj, err := q.Dequeue(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// While running, the key is still held.
	if _, err := q.Enqueue(context.Background(), Job{Type: "t", UniqueKey: key}); !errors.Is(err, ErrJobExists) {
		t.Fatalf("enqueue while running = %v, want ErrJobExists", err)
	}
	if err := q.Ack(context.Background(), dj.ID); err != nil {
		t.Fatal(err)
	}
	// After Ack the key is released.
	if _, err := q.Enqueue(context.Background(), Job{Type: "t", UniqueKey: key}); err != nil {
		t.Fatalf("enqueue after ack: %v", err)
	}
}

func TestUniqueJobReleasedOnDead(t *testing.T) {
	q := NewInMemoryQueue()
	defer q.Close()

	key := "dead:job"
	dj := enqueueAndDequeue(t, q, Job{Type: "t", UniqueKey: key, MaxRetry: -1})
	// Exhaust retries -> move to DLQ, releasing the key.
	err := q.Nack(context.Background(), dj.ID, errors.New("boom"), false, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := q.Enqueue(context.Background(), Job{Type: "t", UniqueKey: key}); err != nil {
		t.Fatalf("enqueue after dead: %v", err)
	}
}

func TestUniqueKeyHeldAcrossRetry(t *testing.T) {
	q := NewInMemoryQueue()
	defer q.Close()

	key := "retry:key"
	dj := enqueueAndDequeue(t, q, Job{Type: "t", UniqueKey: key, MaxRetry: 3})
	// Retryable nack keeps the key held (job goes back to pending).
	if err := q.Nack(context.Background(), dj.ID, errors.New("transient"), true, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Enqueue(context.Background(), Job{Type: "t", UniqueKey: key}); !errors.Is(err, ErrJobExists) {
		t.Fatalf("enqueue during retry = %v, want ErrJobExists", err)
	}
}

func enqueueAndDequeue(t *testing.T, q Queue, job Job) *DequeuedJob {
	t.Helper()
	if _, err := q.Enqueue(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	dj, err := q.Dequeue(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return dj
}

func TestUniqueJobConcurrentSingleWinner(t *testing.T) {
	q := NewInMemoryQueue()
	defer q.Close()

	const n = 32
	done := make(chan error, n)
	for i := 0; i < n; i++ {
		go func() {
			key := "contended"
			_, err := q.Enqueue(context.Background(), Job{Type: "t", UniqueKey: key})
			if err != nil && !errors.Is(err, ErrJobExists) {
				done <- err
				return
			}
			done <- nil
		}()
	}
	for i := 0; i < n; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	// Exactly one job should end up in the queue (Len == 1); every concurrent
	// enqueue with the same key either won the slot or was rejected as a dup.
	if q.Len() != 1 {
		t.Fatalf("Len = %d, want 1 (only one unique winner enqueued)", q.Len())
	}
}
