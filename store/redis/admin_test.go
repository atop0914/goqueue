package redis

import (
	"context"
	"errors"
	"testing"
	"time"

	goqueue "github.com/atop0914/goqueue"
)

func TestRedis_PauseBlocksClaimAndResumeContinues(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	if _, err := st.Enqueue(ctx, goqueue.Job{Type: "t"}); err != nil {
		t.Fatal(err)
	}
	if err := st.Pause(); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	if !st.IsPaused() {
		t.Fatal("IsPaused = false after Pause")
	}
	blocked := make(chan error, 1)
	go func() {
		_, err := st.Dequeue(ctx)
		blocked <- err
	}()
	select {
	case err := <-blocked:
		t.Fatalf("Dequeue returned while paused: %+v", err)
	case <-time.After(120 * time.Millisecond):
	}
	// Resume: the retained job is delivered to the single waiter.
	st.Resume()
	select {
	case err := <-blocked:
		if err != nil {
			t.Fatalf("unblocked Dequeue failed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Dequeue still blocked after Resume")
	}
}

func TestRedis_PurgeDropsReadyAndOptionalDead(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		if _, err := st.Enqueue(ctx, goqueue.Job{Type: "t"}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := st.Enqueue(ctx, goqueue.Job{Type: "t", RunAfter: time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Enqueue(ctx, goqueue.Job{Type: "t", UniqueKey: "k1"}); err != nil {
		t.Fatal(err)
	}
	if got := st.Len(); got != 4 {
		t.Fatalf("Len before purge = %d, want 4", got)
	}

	n, err := st.Purge(ctx, false)
	if err != nil {
		t.Fatalf("Purge: %v", err)
	}
	if n != 4 {
		t.Fatalf("Purge removed %d, want 4", n)
	}
	if got := st.Len(); got != 0 {
		t.Fatalf("Len after purge = %d, want 0", got)
	}
	if n, err := st.Purge(ctx, false); err != nil || n != 0 {
		t.Fatalf("second Purge = (%d, %v), want (0, nil)", n, err)
	}
	// Unique key released.
	if _, err := st.Enqueue(ctx, goqueue.Job{Type: "t", UniqueKey: "k1"}); err != nil {
		t.Fatalf("re-enqueue unique after purge: %v", err)
	}
}

func TestRedis_RequeueDeadResetsAndRequeues(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	id, err := st.Enqueue(ctx, goqueue.Job{Type: "t", MaxRetry: -1, UniqueKey: "rk"})
	if err != nil {
		t.Fatal(err)
	}
	dj, err := st.Dequeue(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if dj.ID != id {
		t.Fatalf("got %s want %s", dj.ID, id)
	}
	if err := st.Nack(ctx, id, errors.New("boom"), false, 0); err != nil {
		t.Fatal(err)
	}
	if len(st.Dead()) != 1 {
		t.Fatalf("dead = %d, want 1", len(st.Dead()))
	}

	if err := st.RequeueDeadJob(ctx, id); err != nil {
		t.Fatalf("RequeueDeadJob: %v", err)
	}
	if len(st.Dead()) != 0 {
		t.Fatalf("dead after requeue = %d, want 0", len(st.Dead()))
	}
	if err := st.RequeueDeadJob(ctx, "nope"); !errors.Is(err, goqueue.ErrJobNotFound) {
		t.Errorf("unknown id err = %v, want ErrJobNotFound", err)
	}
	dj2, err := st.Dequeue(ctx)
	if err != nil {
		t.Fatalf("Dequeue requeued: %v", err)
	}
	if dj2.ID != id || dj2.Attempt != 1 {
		t.Fatalf("requeued job = (%s, attempt %d), want (%s, 1)", dj2.ID, dj2.Attempt, id)
	}
	// Kill again; wholesale requeue.
	if err := st.Nack(ctx, id, errors.New("boom2"), false, 0); err != nil {
		t.Fatal(err)
	}
	n, err := st.RequeueDead(ctx)
	if err != nil {
		t.Fatalf("RequeueDead: %v", err)
	}
	if n != 1 {
		t.Fatalf("RequeueDead requeued %d, want 1", n)
	}
	if n, err := st.RequeueDead(ctx); err != nil || n != 0 {
		t.Fatalf("idempotent RequeueDead = (%d, %v), want (0, nil)", n, err)
	}
}

func TestRedis_RequeueDeadContestedUniqueKeyStaysDead(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	deadID, err := st.Enqueue(ctx, goqueue.Job{Type: "t", MaxRetry: -1, UniqueKey: "k"})
	if err != nil {
		t.Fatal(err)
	}
	dj, err := st.Dequeue(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Nack(ctx, dj.ID, errors.New("x"), false, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Enqueue(ctx, goqueue.Job{Type: "t", UniqueKey: "k"}); err != nil {
		t.Fatal(err)
	}

	if err := st.RequeueDeadJob(ctx, deadID); !errors.Is(err, goqueue.ErrJobExists) {
		t.Fatalf("contested requeue err = %v, want ErrJobExists", err)
	}
	if len(st.Dead()) != 1 {
		t.Fatalf("dead after contested requeue = %d, want 1", len(st.Dead()))
	}
	if n, err := st.RequeueDead(ctx); err != nil || n != 0 {
		t.Fatalf("wholesale contested = (%d, %v), want (0, nil)", n, err)
	}
}
