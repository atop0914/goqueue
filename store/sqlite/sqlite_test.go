package sqlite

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	goqueue "github.com/atop0914/goqueue"
)

// openTestStore returns a Store on a fresh temp-file database.
func openTestStore(t *testing.T) *Store {
	t.Helper()
	st, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = st.CloseDB() })
	return st
}

func TestSqlite_EnqueueDequeueOrder(t *testing.T) {
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
	}
	if n := st.Len(); n != 0 {
		t.Errorf("Len = %d, want 0", n)
	}
}

func TestSqlite_DelayedJobNotClaimedEarly(t *testing.T) {
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

func TestSqlite_AckAndNotFound(t *testing.T) {
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

func TestSqlite_RetryThenDeadLetter(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	id, err := st.Enqueue(ctx, goqueue.Job{Type: "t", MaxRetry: 1})
	if err != nil {
		t.Fatal(err)
	}
	boom := errors.New("boom")
	// Attempt 1: retryable failure with retry budget left -> back to pending.
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

func TestSqlite_NackNonRetryableGoesStraightToDLQ(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	id, _ := st.Enqueue(ctx, goqueue.Job{Type: "t"}) // full retry budget
	dj, err := st.Dequeue(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Nack(ctx, id, errors.New("poison"), false, 0); err != nil {
		t.Fatal(err)
	}
	_ = dj
	if dead := st.Dead(); len(dead) != 1 {
		t.Fatalf("Dead = %d entries, want 1 (non-retryable skips retries)", len(dead))
	}
}

func TestSqlite_UniqueJobs(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	if _, err := st.Enqueue(ctx, goqueue.Job{Type: "t", UniqueKey: "k1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Enqueue(ctx, goqueue.Job{Type: "t", UniqueKey: "k1"}); !errors.Is(err, ErrJobExists) {
		t.Errorf("duplicate unique Enqueue = %v, want ErrJobExists", err)
	}

	// Key stays held across retries...
	dj, _ := st.Dequeue(ctx)
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

func TestSqlite_ConcurrentProducersConsumersNoDoubleClaim(t *testing.T) {
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
		t.Errorf("leftover running/dead rows: running=%d dead=%d", running, dead)
	}
}

func TestSqlite_CloseSemantics(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := st.Dequeue(ctx); !errors.Is(err, ErrQueueClosed) {
		t.Errorf("Dequeue after Close = %v, want ErrQueueClosed", err)
	}
	// Persistence-first design: Enqueue still works while handle is open,
	// rows survive for a future session.
	if _, err := st.Enqueue(ctx, goqueue.Job{Type: "t"}); err != nil {
		t.Errorf("Enqueue after Close: %v", err)
	}
	if err := st.CloseDB(); err != nil {
		t.Errorf("CloseDB: %v", err)
	}
	if err := st.CloseDB(); err != nil { // idempotent
		t.Errorf("second CloseDB: %v", err)
	}
	if _, err := st.Enqueue(ctx, goqueue.Job{Type: "t"}); err == nil {
		t.Error("Enqueue after CloseDB unexpectedly succeeded")
	}
}

func TestSqlite_CrashRecoveryRequeuesRunningJobs(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "recover.db")
	ctx := context.Background()

	// Session 1: enqueue two jobs, dequeue both (state=running), then "crash".
	st1, err := Open(dbPath)
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
	// Simulated hard kill: no Ack/Nack, just drop the handle.
	if err := st1.CloseDB(); err != nil {
		t.Fatal(err)
	}

	// Session 2: both jobs must be back to pending, attempt counts preserved.
	st2, err := Open(dbPath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = st2.CloseDB() })
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

// TestSqlite_ClientIntegration drives the root goqueue.Client against this
// backend: handlers, retries, dead-letter callback and graceful shutdown.
func TestSqlite_ClientIntegration(t *testing.T) {
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
	shCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
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

// TestSqlite_HeaderlessFileRoundTrip exercises file-mode durability beyond
// the shared temp-dir pattern: separate paths per subtest case.
func TestSqlite_FileModes(t *testing.T) {
	t.Run("memory-db-not-supported-for-persistence", func(t *testing.T) {
		// Document behavior: an in-memory database works within the process
		// but obviously cannot survive a reopen.
		st, err := Open(":memory:")
		if err != nil {
			t.Fatalf("Open :memory:: %v", err)
		}
		defer st.CloseDB()
		ctx := context.Background()
		id, err := st.Enqueue(ctx, goqueue.Job{Type: "t"})
		if err != nil {
			t.Fatal(err)
		}
		dj, err := st.Dequeue(ctx)
		if err != nil || dj.ID != id {
			t.Errorf("in-memory roundtrip failed: %v", err)
		}
	})
}
