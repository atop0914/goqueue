package redis

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	goqueue "github.com/atop0914/goqueue"
)

// openTestStore starts a fresh miniredis (an in-process Redis server — no
// Docker, no external daemon) and returns a Store connected to it. Unlike
// the SQLite backend there is no embedded-engine flake to guard against, so
// nothing here skips under -short: the whole suite is CI-safe by design.
func openTestStore(t *testing.T) *Store {
	t.Helper()
	mr := miniredis.RunT(t)
	st, err := Open(mr.Addr())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = st.CloseClient() }) // idempotent
	return st
}

func TestRedis_EnqueueDequeueOrder(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	// Interleave priorities: same RunAfter (zero => immediate), so priority
	// DESC with seq ASC tie-break decides the order.
	for _, p := range []int{1, 5, 3, 5} {
		job := goqueue.Job{Type: "t", Payload: []byte(fmt.Sprintf("p%d", p))}
		job.Priority = p
		if _, err := st.Enqueue(ctx, job); err != nil {
			t.Fatalf("Enqueue prio=%d: %v", p, err)
		}
	}
	// Zero-priority job enqueued last must come out behind earlier seq.
	if _, err := st.Enqueue(ctx, goqueue.Job{Type: "t"}); err != nil {
		t.Fatal(err)
	}

	want := []string{"5", "5", "3", "1", "0"}
	for i, w := range want {
		dj, err := st.Dequeue(ctx)
		if err != nil {
			t.Fatalf("Dequeue #%d: %v", i, err)
		}
		if got := fmt.Sprintf("%d", dj.Priority); got != w {
			t.Errorf("dequeue #%d priority = %s, want %s", i, got, w)
		}
		if dj.Attempt != 1 {
			t.Errorf("dequeue #%d attempt = %d, want 1", i, dj.Attempt)
		}
		if dj.MaxRetry != goqueue.DefaultMaxRetry {
			t.Errorf("default MaxRetry = %d, want %d", dj.MaxRetry, goqueue.DefaultMaxRetry)
		}
		if dj.EnqueuedAt.IsZero() {
			t.Error("EnqueuedAt not populated")
		}
	}
	if n := st.Len(); n != 0 {
		t.Errorf("Len = %d, want 0", n)
	}
}

func TestRedis_DelayedJobNotClaimedEarly(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	start := time.Now()
	job := goqueue.Job{Type: "t", RunAfter: start.Add(400 * time.Millisecond)}
	if _, err := st.Enqueue(ctx, job); err != nil {
		t.Fatal(err)
	}
	if n := st.Len(); n != 1 {
		t.Fatalf("Len = %d, want 1 (delayed jobs counted as pending)", n)
	}
	dctx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
	defer cancel()
	if _, err := st.Dequeue(dctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("early Dequeue err = %v, want DeadlineExceeded", err)
	}
	if _, err := st.Dequeue(ctx); err != nil {
		t.Fatalf("due Dequeue: %v", err)
	}
	if el := time.Since(start); el < 350*time.Millisecond {
		t.Errorf("job handed out after %v, expected >= ~400ms delay", el)
	}
}

func TestRedis_AckAndNotFound(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	id, err := st.Enqueue(ctx, goqueue.Job{Type: "t"})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Ack(ctx, id); !errors.Is(err, ErrJobNotFound) {
		t.Errorf("Ack before Dequeue = %v, want ErrJobNotFound", err)
	}
	dj, err := st.Dequeue(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Ack(ctx, dj.ID); err != nil {
		t.Errorf("Ack: %v", err)
	}
	if err := st.Ack(ctx, dj.ID); !errors.Is(err, ErrJobNotFound) {
		t.Errorf("double Ack = %v, want ErrJobNotFound", err)
	}
	if _, _, succ, _ := st.Stats(); succ != 1 {
		t.Errorf("Stats succeeded = %d, want 1", succ)
	}
}

func TestRedis_RetryThenDeadLetter(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	id, err := st.Enqueue(ctx, goqueue.Job{Type: "t", MaxRetry: 1})
	if err != nil {
		t.Fatal(err)
	}
	boom := errors.New("boom")
	// Attempt 1: retryable failure with retry budget left -> back to ready.
	dj, err := st.Dequeue(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if dj.ID != id || dj.Attempt != 1 {
		t.Fatalf("got %s attempt %d", dj.ID, dj.Attempt)
	}
	if err := st.Nack(ctx, id, boom, true, 20*time.Millisecond); err != nil {
		t.Fatalf("Nack#1: %v", err)
	}
	time.Sleep(30 * time.Millisecond) // let the backoff elapse

	// Attempt 2: budget exhausted -> DLQ.
	dj, err = st.Dequeue(ctx)
	if err != nil {
		t.Fatalf("retry Dequeue: %v", err)
	}
	if dj.Attempt != 2 {
		t.Errorf("retry attempt = %d, want 2", dj.Attempt)
	}
	if err := st.Nack(ctx, id, boom, true, 0); err != nil {
		t.Fatalf("Nack#2: %v", err)
	}

	dead := st.Dead()
	if len(dead) != 1 {
		t.Fatalf("Dead = %d entries, want 1", len(dead))
	}
	info := dead[0]
	if info.State != goqueue.StateDead || info.LastError != "boom" || info.Attempts != 2 {
		t.Errorf("dead info = %+v", info)
	}
	if info.DeadAt.IsZero() {
		t.Error("DeadAt not set")
	}
}

func TestRedis_NackNonRetryableGoesStraightToDLQ(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	if _, err := st.Enqueue(ctx, goqueue.Job{Type: "t"}); err != nil { // full retry budget
		t.Fatal(err)
	}
	dj, err := st.Dequeue(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Nack(ctx, dj.ID, errors.New("poison"), false, 0); err != nil {
		t.Fatal(err)
	}
	if dead := st.Dead(); len(dead) != 1 {
		t.Fatalf("Dead = %d entries, want 1 (non-retryable skips retries)", len(dead))
	}
}

// TestRedis_PriorityClamping documents the score-packing limit: priorities
// beyond ±499 are clamped (not an error), so a huge priority still outranks
// defaults but ties among clamped values fall back to insertion order.
func TestRedis_PriorityClamping(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	// a and b both clamp to +499 -> seq decides (a first). c stays 0.
	for _, tag := range []string{"a", "b", "c"} {
		prio := 0
		switch tag {
		case "a":
			prio = 5000
		case "b":
			prio = 499
		}
		if _, err := st.Enqueue(ctx, goqueue.Job{Type: "t", Priority: prio, Payload: []byte(tag)}); err != nil {
			t.Fatalf("Enqueue %s: %v", tag, err)
		}
	}
	want := []string{"a", "b", "c"}
	for i, w := range want {
		dj, err := st.Dequeue(ctx)
		if err != nil {
			t.Fatalf("Dequeue #%d: %v", i, err)
		}
		if string(dj.Payload) != w {
			t.Errorf("dequeue #%d = %q, want %q", i, dj.Payload, w)
		}
	}
}

func TestRedis_UniqueJobs(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	if _, err := st.Enqueue(ctx, goqueue.Job{Type: "t", UniqueKey: "k1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Enqueue(ctx, goqueue.Job{Type: "t", UniqueKey: "k1"}); !errors.Is(err, ErrJobExists) {
		t.Errorf("duplicate unique Enqueue = %v, want ErrJobExists", err)
	}

	// Key stays held across retries...
	dj, err := st.Dequeue(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Nack(ctx, dj.ID, nil, true, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Enqueue(ctx, goqueue.Job{Type: "t", UniqueKey: "k1"}); !errors.Is(err, ErrJobExists) {
		t.Errorf("enqueue while retrying = %v, want ErrJobExists", err)
	}
	// Pick the retrying job back up and kill it for good: the key must be
	// released once the job lands in the DLQ.
	dj2, err := st.Dequeue(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if dj2.ID != dj.ID {
		t.Fatalf("retry dequeue gave %s, want %s", dj2.ID, dj.ID)
	}
	if err := st.Nack(ctx, dj.ID, errors.New("give up"), false, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Enqueue(ctx, goqueue.Job{Type: "t", UniqueKey: "k1"}); err != nil {
		t.Errorf("re-enqueue after DLQ: %v", err)
	}
}

func TestRedis_ConcurrentProducersConsumersNoDoubleClaim(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	const (
		producers = 8
		jobsEach  = 25
		consumers = 6
	)
	var enq sync.WaitGroup
	for p := 0; p < producers; p++ {
		enq.Add(1)
		go func(p int) {
			defer enq.Done()
			for j := 0; j < jobsEach; j++ {
				if _, err := st.Enqueue(ctx, goqueue.Job{
					Type:    "t",
					Payload: []byte(fmt.Sprintf("p%d-j%d", p, j)),
				}); err != nil {
					t.Errorf("Enqueue: %v", err)
					return
				}
			}
		}(p)
	}

	var processed atomic.Int64
	done := make(chan struct{})
	var closeOnce sync.Once
	var cons sync.WaitGroup
	for c := 0; c < consumers; c++ {
		cons.Add(1)
		go func() {
			defer cons.Done()
			for {
				cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
				dj, err := st.Dequeue(cctx)
				cancel()
				if err != nil {
					closeOnce.Do(func() { close(done) }) // queue drained or timed out
					return
				}
				if err := st.Ack(ctx, dj.ID); err != nil {
					t.Errorf("Ack %s: %v", dj.ID, err)
				}
				processed.Add(1)
			}
		}()
	}

	enq.Wait()
	<-done
	cons.Wait()

	if got := processed.Load(); got != producers*jobsEach {
		t.Errorf("processed = %d, want %d", got, int64(producers*jobsEach))
	}
	if n := st.Len(); n != 0 {
		t.Errorf("Len = %d, want 0", n)
	}
	if _, running, _, dead := st.Stats(); running+dead != 0 {
		t.Errorf("leftover running/dead entries: running=%d dead=%d", running, dead)
	}
}

func TestRedis_CloseSemantics(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := st.Dequeue(ctx); !errors.Is(err, ErrQueueClosed) {
		t.Errorf("Dequeue after Close = %v, want ErrQueueClosed", err)
	}
	// Persistence-first design: Enqueue still works while the connection is
	// open, data survives for a future session.
	if _, err := st.Enqueue(ctx, goqueue.Job{Type: "t"}); err != nil {
		t.Errorf("Enqueue after Close: %v", err)
	}
	if err := st.CloseClient(); err != nil {
		t.Errorf("CloseClient: %v", err)
	}
	if err := st.CloseClient(); err != nil { // idempotent
		t.Errorf("second CloseClient: %v", err)
	}
	if _, err := st.Enqueue(ctx, goqueue.Job{Type: "t"}); err == nil {
		t.Error("Enqueue after CloseClient unexpectedly succeeded")
	}
}

// TestRedis_CrashRecoveryRequeuesRunningJobs simulates a process crash by
// dropping the client while jobs sit in the running set; the next Open must
// hand them out again with attempt counts preserved.
func TestRedis_CrashRecoveryRequeuesRunningJobs(t *testing.T) {
	mr := miniredis.RunT(t)
	ctx := context.Background()

	// Session 1: enqueue two jobs, dequeue both (state=running), then "crash".
	st1, err := Open(mr.Addr())
	if err != nil {
		t.Fatal(err)
	}
	for _, tag := range []string{"a", "b"} {
		if _, err := st1.Enqueue(ctx, goqueue.Job{Type: "t", Payload: []byte(tag)}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 2; i++ {
		if _, err := st1.Dequeue(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if _, running, _, _ := st1.Stats(); running != 2 {
		t.Fatalf("pre-crash running = %d, want 2", running)
	}
	// Simulated hard kill: no Ack/Nack, just drop the connection. The server
	// (mr) keeps all keys — exactly like a real Redis surviving a client.
	if err := st1.CloseClient(); err != nil {
		t.Fatal(err)
	}

	// Session 2: both jobs must be back in the ready queue, attempts kept.
	st2, err := Open(mr.Addr())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = st2.CloseClient() })
	if n := st2.Len(); n != 2 {
		t.Fatalf("post-crash Len = %d, want 2", n)
	}
	for i := 0; i < 2; i++ {
		dj, err := st2.Dequeue(ctx)
		if err != nil {
			t.Fatalf("recovered Dequeue #%d: %v", i, err)
		}
		if dj.Attempt != 2 {
			t.Errorf("recovered job attempt = %d, want 2 (first attempt counted before crash)", dj.Attempt)
		}
		if err := st2.Ack(ctx, dj.ID); err != nil {
			t.Errorf("recovered Ack: %v", err)
		}
	}
	if _, running, succ, _ := st2.Stats(); running != 0 || succ != 2 {
		t.Errorf("final stats: running=%d succeeded=%d, want 0/2", running, succ)
	}
}

// TestRedis_UniqueKeySurvivesCrashUntilAck mirrors the SQLite recovery-edge
// guarantee: a unique key held by a job in flight is not freed by a crash —
// only a real Ack or DLQ release releases it.
func TestRedis_UniqueKeySurvivesCrashUntilAck(t *testing.T) {
	mr := miniredis.RunT(t)
	ctx := context.Background()

	st1, err := Open(mr.Addr())
	if err != nil {
		t.Fatal(err)
	}
	id, err := st1.Enqueue(ctx, goqueue.Job{Type: "t", UniqueKey: "k1"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st1.Dequeue(ctx); err != nil {
		t.Fatal(err)
	}
	if err := st1.CloseClient(); err != nil { // crash mid-flight
		t.Fatal(err)
	}

	st2, err := Open(mr.Addr()) // recovery requeues the running job
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st2.CloseClient() })
	if _, err := st2.Enqueue(ctx, goqueue.Job{Type: "t", UniqueKey: "k1"}); !errors.Is(err, ErrJobExists) {
		t.Errorf("enqueue after crash = %v, want ErrJobExists (key must stay held)", err)
	}
	dj, err := st2.Dequeue(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if dj.ID != id || dj.Attempt != 2 {
		t.Fatalf("recovered %s attempt %d, want %s attempt 2", dj.ID, dj.Attempt, id)
	}
	if err := st2.Ack(ctx, id); err != nil {
		t.Fatal(err)
	}
	if _, err := st2.Enqueue(ctx, goqueue.Job{Type: "t", UniqueKey: "k1"}); err != nil {
		t.Errorf("re-enqueue after Ack: %v", err)
	}
}

// TestRedis_MultiQueueIsolation verifies the namespace option: two queues
// sharing one Redis server never see each other's jobs.
func TestRedis_MultiQueueIsolation(t *testing.T) {
	mr := miniredis.RunT(t)
	ctx := context.Background()
	open := func(name string) *Store {
		t.Helper()
		st, err := Open(mr.Addr(), WithQueue(name))
		if err != nil {
			t.Fatalf("Open %s: %v", name, err)
		}
		t.Cleanup(func() { _ = st.CloseClient() })
		return st
	}
	orders := open("orders")
	emails := open("emails")

	id, err := orders.Enqueue(ctx, goqueue.Job{Type: "charge", Payload: []byte("o-1")})
	if err != nil {
		t.Fatal(err)
	}
	if n := emails.Len(); n != 0 {
		t.Errorf("emails Len = %d, want 0", n)
	}
	dctx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
	defer cancel()
	if _, err := emails.Dequeue(dctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("cross-queue Dequeue err = %v, want DeadlineExceeded", err)
	}
	dj, err := orders.Dequeue(ctx)
	if err != nil || dj.ID != id {
		t.Errorf("orders Dequeue = (%s, %v), want (%s, nil)", dj.ID, err, id)
	}
}

// TestRedis_DequeueWakesOnArrival proves an idle Dequeue does not miss a job
// that shows up after it started waiting (notify wake + poll both covered).
func TestRedis_DequeueWakesOnArrival(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	type result struct {
		dj  *goqueue.DequeuedJob
		err error
	}
	ch := make(chan result, 1)
	go func() {
		wctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		dj, err := st.Dequeue(wctx)
		ch <- result{dj, err}
	}()

	time.Sleep(100 * time.Millisecond) // let the consumer go idle first
	if _, err := st.Enqueue(ctx, goqueue.Job{Type: "t", Payload: []byte("late")}); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("Dequeue: %v", r.err)
		}
		if string(r.dj.Payload) != "late" {
			t.Errorf("payload = %q, want %q", r.dj.Payload, "late")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Dequeue did not wake on arrival")
	}
}

// TestRedis_LenContextCanceled documents the drain-probe contract: an
// already-canceled context yields the conservative "work may remain" answer.
func TestRedis_LenContextCanceled(t *testing.T) {
	st := openTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if n := st.LenContext(ctx); n != 1 {
		t.Errorf("LenContext(canceled) = %d, want 1 (conservative)", n)
	}
}

// TestRedis_ClientIntegration drives the root goqueue.Client against this
// backend: handlers, retries, dead-letter callback and graceful shutdown.
func TestRedis_ClientIntegration(t *testing.T) {
	st := openTestStore(t)

	var deadIDs sync.Map
	client := goqueue.New(
		goqueue.WithQueue(st),
		goqueue.WithWorkers(4),
		goqueue.WithDrainOnShutdown(true),
		goqueue.WithOnDead(func(info goqueue.JobInfo) {
			deadIDs.Store(info.ID, struct{}{})
		}),
	)
	client.Register("flaky", func(_ context.Context, payload []byte) error {
		var v int
		fmt.Sscanf(string(payload), "%d", &v)
		if v < 2 { // succeed on the third attempt
			return errors.New("transient")
		}
		return nil
	})
	client.Register("poison", func(_ context.Context, _ []byte) error {
		return errors.New("always fails")
	})

	ctx := context.Background()
	flakyID, err := client.Enqueue(ctx, goqueue.Job{Type: "flaky", Payload: []byte("2")})
	if err != nil {
		t.Fatal(err)
	}
	poisonID, err := client.Enqueue(ctx, goqueue.Job{Type: "poison"})
	if err != nil {
		t.Fatal(err)
	}
	normalID, err := client.Enqueue(ctx, goqueue.Job{Type: "flaky", Payload: []byte("9")})
	if err != nil {
		t.Fatal(err)
	}

	client.Start()
	shCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := client.Shutdown(shCtx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	dead := st.Dead()
	if len(dead) != 1 || dead[0].ID != poisonID {
		t.Errorf("DLQ = %+v, want only poison job %s", dead, poisonID)
	}
	if _, hit := deadIDs.Load(poisonID); !hit {
		t.Error("OnDead was not fired for the poisoned job")
	}
	for _, id := range []string{flakyID, normalID} {
		if _, hit := deadIDs.Load(id); hit {
			t.Errorf("job %s unexpectedly died", id)
		}
	}
	if p, r, s, d := st.Stats(); p != 0 || r != 0 || d != 1 || s != 2 {
		t.Errorf("stats after shutdown: pending=%d running=%d succeeded=%d dead=%d, want 0/0/2/1", p, r, s, d)
	}
}
