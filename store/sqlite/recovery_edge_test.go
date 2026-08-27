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

// The tests in this file probe the durability edges of the SQLite backend:
// crash recovery against retry budgets / delayed rows / unique keys / repeat
// crashes, the WAL concurrency contract under a mixed workload, and the
// full enqueued→crash→reopen→process→ack round trip.

// openTestStoreAt returns a Store on a caller-chosen file path so crash
// round trips can reopen the very same database across simulated processes.
func openTestStoreAt(tb testing.TB, path string) *Store {
	tb.Helper()
	st, err := Open(path)
	if err != nil {
		tb.Fatalf("Open(%s): %v", path, err)
	}
	tb.Cleanup(func() { _ = st.CloseDB() })
	return st
}

// TestSqlite_RedeliveredExhaustedJobGoesToDLQ pins the recovery-vs-retry
// budget boundary. Design decision being locked in: Open()'s running→pending
// sweep deliberately does NOT evaluate retry budgets — a crash mid-handler is
// indistinguishable from a fresh dispatch, so the final delivery always gets
// redelivered (at-least-once), and safety comes from the delivery side: a
// redelivered job whose attempts now exceed MaxRetry lands in the DLQ at its
// very next Nack, even if the caller mislabels it retryable. Poison jobs
// therefore cannot loop forever across restarts.
//
// Setup drives everything through the public API: MaxRetry=1 grants two
// deliveries; the first failure burns into a scheduled retry, the SECOND
// delivery (the last fair one) dies with the process, and the post-crash
// attempt runs past the budget.
func TestSqlite_RedeliveredExhaustedJobGoesToDLQ(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "exhausted.db")
	ctx := context.Background()

	st1, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	id, err := st1.Enqueue(ctx, goqueue.Job{Type: "poison", MaxRetry: 1})
	if err != nil {
		t.Fatal(err)
	}
	// Delivery 1: fail retryably -> scheduled retry (budget spent on it).
	dj, err := st1.Dequeue(ctx)
	if err != nil || dj.ID != id || dj.Attempt != 1 {
		t.Fatalf("d1 = %+v, %v", dj, err)
	}
	if err := st1.Nack(ctx, id, errors.New("boom"), true, 0); err != nil {
		t.Fatalf("Nack#1: %v", err)
	}
	// Delivery 2: the final fair one — killed mid-flight by the crash.
	dj2, err := st1.Dequeue(ctx)
	if err != nil || dj2.ID != id || dj2.Attempt != 2 {
		t.Fatalf("d2 = %+v, %v", dj2, err)
	}
	if _, running, _, _ := st1.Stats(); running != 1 {
		t.Fatalf("pre-crash running = %d, want 1", running)
	}
	if err := st1.CloseDB(); err != nil {
		t.Fatal(err)
	}

	st2, err := Open(dbPath) // crash recovery sweeps running→pending
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = st2.CloseDB() })

	// Delivery 3: past the budget (attempts will hit 3 > MaxRetry 1),
	// yet redelivered — that is the at-least-once guarantee working.
	dj3, err := st2.Dequeue(ctx)
	if err != nil {
		t.Fatalf("redelivered Dequeue: %v", err)
	}
	if dj3.ID != id || dj3.Attempt != 3 {
		t.Fatalf("d3 = %+v, want %s attempt 3", dj3, id)
	}
	// Even a "retryable" classification must respect the burned budget:
	// DLQ, never another retry slot.
	if err := st2.Nack(ctx, id, errors.New("boom"), true, 0); err != nil {
		t.Fatalf("Nack after recovery: %v", err)
	}

	dead := st2.Dead()
	if len(dead) != 1 {
		t.Fatalf("Dead = %d entries, want 1 (budget exhausted at redelivery)", len(dead))
	}
	info := dead[0]
	if info.ID != id || info.State != goqueue.StateDead ||
		info.Attempts != 3 || info.LastError != "boom" {
		t.Errorf("dead info = %+v", info)
	}
	if info.DeadAt.IsZero() {
		t.Error("DeadAt not set")
	}
	if p, r, s, d := st2.Stats(); p != 0 || r != 0 || s != 0 || d != 1 {
		t.Errorf("stats = p%d/r%d/s%d/d%d, want 0/0/0/1", p, r, s, d)
	}
	if n := st2.Len(); n != 0 {
		t.Errorf("Len = %d, want 0", n)
	}
	// And nothing may be left to deliver: the poison loop terminates.
	dctx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
	defer cancel()
	if _, err := st2.Dequeue(dctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("post-DLQ Dequeue err = %v, want DeadlineExceeded (loop terminated)", err)
	}
}

// TestSqlite_RecoveryLeavesDelayedAndTerminalRowsUntouched proves Open()'s
// recovery sweep only requeues rows it must: a future-dated pending row keeps
// its delay (and fires on time after the restart), while succeeded and dead
// rows are neither resurrected nor mutated. Their dead_at/last_error metadata
// survives the process boundary intact.
func TestSqlite_RecoveryLeavesDelayedAndTerminalRowsUntouched(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "untouched.db")
	ctx := context.Background()

	st1, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	delayBase := time.Now()
	if _, err := st1.Enqueue(ctx, goqueue.Job{
		Type:     "t",
		RunAfter: delayBase.Add(900 * time.Millisecond),
	}); err != nil {
		t.Fatal(err)
	}
	okID, err := st1.Enqueue(ctx, goqueue.Job{Type: "done"})
	if err != nil {
		t.Fatal(err)
	}
	dj, err := st1.Dequeue(ctx)
	if err != nil || dj.ID != okID {
		t.Fatalf("dequeue ok job: dj=%+v err=%v", dj, err)
	}
	if err := st1.Ack(ctx, okID); err != nil {
		t.Fatal(err)
	}
	deadID, err := st1.Enqueue(ctx, goqueue.Job{Type: "fatal"})
	if err != nil {
		t.Fatal(err)
	}
	dj2, err := st1.Dequeue(ctx)
	if err != nil || dj2.ID != deadID {
		t.Fatalf("dequeue fatal job: dj=%+v err=%v", dj2, err)
	}
	if err := st1.Nack(ctx, deadID, errors.New("fatal: bad payload"), false, 0); err != nil {
		t.Fatal(err)
	}
	preDead := st1.Dead()
	if len(preDead) != 1 || preDead[0].ID != deadID || preDead[0].DeadAt.IsZero() {
		t.Fatalf("pre-crash DLQ wrong: %+v", preDead)
	}
	// No graceful teardown — kill the process with all three rows persisted.
	if err := st1.CloseDB(); err != nil {
		t.Fatal(err)
	}

	st2, err := Open(dbPath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = st2.CloseDB() })

	// Terminal rows are immutable across restarts.
	if p, r, s, d := st2.Stats(); p != 1 || r != 0 || s != 1 || d != 1 {
		t.Errorf("post-crash stats = p%d/r%d/s%d/d%d, want 1/0/1/1", p, r, s, d)
	}
	got := st2.Dead()
	if len(got) != 1 || got[0].ID != deadID ||
		got[0].LastError != "fatal: bad payload" || got[0].DeadAt.IsZero() {
		t.Errorf("DLQ not preserved: %+v", got)
	}

	// The delayed row kept its future run_after: not claimable right away.
	fastCtx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
	defer cancel()
	if _, err := st2.Dequeue(fastCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("early Dequeue err = %v, want DeadlineExceeded (delay preserved)", err)
	}
	// ...but it does fire once due, proving delay metadata survived.
	dj3, err := st2.Dequeue(ctx)
	if err != nil {
		t.Fatalf("delayed job after restart: %v", err)
	}
	if dj3.Type != "t" {
		t.Errorf("recovered delayed job type = %s, want t", dj3.Type)
	}
	if el := time.Since(delayBase); el < 850*time.Millisecond {
		t.Errorf("delayed job delivered after %v, expected ~900ms", el)
	}
}

// TestSqlite_UniqueKeySurvivesCrashUntilAck ensures uniqueness is durable:
// a crash while the keyed job is in flight keeps the key held on the
// recovered row (duplicates still rejected), and only a real Ack — or the
// DLQ — releases it.
func TestSqlite_UniqueKeySurvivesCrashUntilAck(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "unique.db")
	ctx := context.Background()

	st1, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	id, err := st1.Enqueue(ctx, goqueue.Job{Type: "t", UniqueKey: "ledger-daily"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st1.Dequeue(ctx); err != nil {
		t.Fatal(err)
	}
	if err := st1.CloseDB(); err != nil { // crash mid-flight
		t.Fatal(err)
	}

	st2, err := Open(dbPath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = st2.CloseDB() })

	// Key held by the recovered row: duplicate enqueue must be refused.
	if _, err := st2.Enqueue(ctx, goqueue.Job{Type: "t", UniqueKey: "ledger-daily"}); !errors.Is(err, ErrJobExists) {
		t.Errorf("enqueue while recovered = %v, want ErrJobExists", err)
	}
	// Process the original to completion — only now is the key released.
	dj, err := st2.Dequeue(ctx)
	if err != nil || dj.ID != id {
		t.Fatalf("recover keyed job: dj=%+v err=%v", dj, err)
	}
	if err := st2.Ack(ctx, id); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if _, err := st2.Enqueue(ctx, goqueue.Job{Type: "t", UniqueKey: "ledger-daily"}); err != nil {
		t.Errorf("re-enqueue after release: %v", err)
	}
}

// TestSqlite_MultiCrashAttemptsAccumulate replays the worst-case ops story:
// a worker keeps dying mid-handler. Every crash/restart cycle must preserve
// the attempt series so backoff accounting continues — nothing resets to
// zero, and the eventual handler run observes the true count.
func TestSqlite_MultiCrashAttemptsAccumulate(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "multicrash.db")
	const crashes = 3
	ctx := context.Background()

	var id string
	for cycle := 0; cycle < crashes; cycle++ {
		st, err := Open(dbPath)
		if err != nil {
			t.Fatalf("cycle %d open: %v", cycle, err)
		}
		if cycle == 0 {
			var e error
			id, e = st.Enqueue(ctx, goqueue.Job{Type: "t"})
			if e != nil {
				t.Fatal(e)
			}
		}
		dj, err := st.Dequeue(ctx)
		if err != nil {
			t.Fatalf("cycle %d dequeue: %v", cycle, err)
		}
		if dj.ID != id {
			t.Fatalf("cycle %d got %s, want %s", cycle, dj.ID, id)
		}
		if want := cycle + 1; dj.Attempt != want {
			t.Errorf("cycle %d attempt = %d, want %d", cycle, dj.Attempt, want)
		}
		if err := st.CloseDB(); err != nil { // die mid-handler again
			t.Fatalf("cycle %d close: %v", cycle, err)
		}
	}

	// Final incarnation finally finishes the job.
	stFinal := openTestStoreAt(t, dbPath)
	dj, err := stFinal.Dequeue(ctx)
	if err != nil {
		t.Fatalf("final dequeue: %v", err)
	}
	if dj.Attempt != crashes+1 {
		t.Errorf("final attempt = %d, want %d", dj.Attempt, crashes+1)
	}
	if err := stFinal.Ack(ctx, id); err != nil {
		t.Errorf("final Ack: %v", err)
	}
	if p, r, s, d := stFinal.Stats(); p != 0 || r != 0 || s != 1 || d != 0 {
		t.Errorf("final stats = p%d/r%d/s%d/d%d, want 0/0/1/0", p, r, s, d)
	}
}

// TestSqlite_MixedWALConcurrencyConverges hammers one Store with every row
// shape at once — immediate, delayed, prioritized, unique-keyed, retried —
// from concurrent producers while independent consumers claim/ack/nack, all
// through the single serialized connection WAL gives us. Convergence is
// verified with an exact ledger: accepted = enqueued-minus-rejected,
// every accepted ID terminates exactly once (LoadOrStore catches duplicate
// terminals), and the final state census is pending0/running0/dead0.
func TestSqlite_MixedWALConcurrencyConverges(t *testing.T) {
	st := openTestStoreAt(t, filepath.Join(t.TempDir(), "mixed.db"))
	ctx := context.Background()

	const (
		producers   = 8
		jobsEach    = 30 // 240 total attempts to enqueue
		consumers   = 6
		delayEvery  = 11 // every 11th job delayed 80ms
		flakyEvery  = 5  // every 5th job fails its first attempt
		uniqEvery   = 7  // every 7th job races on one shared unique key
		hurryEvery  = 13 // every 13th job is high priority
		pollTick    = 20 * time.Millisecond
		settleLimit = 45 * time.Second
	)
	total := producers * jobsEach

	var (
		enqueuedIDs []string
		idMu        sync.Mutex
		rejected    atomic.Int64
	)

	var prodWG sync.WaitGroup
	for p := 0; p < producers; p++ {
		prodWG.Add(1)
		go func(p int) {
			defer prodWG.Done()
			for j := 0; j < jobsEach; j++ {
				job := goqueue.Job{Type: "t", Payload: []byte(fmt.Sprintf("p%d-j%d", p, j))}
				switch {
				case j%flakyEvery == 0:
					job.Type = "flaky"
				case j%uniqEvery == 0:
					job.UniqueKey = "one-slot"
				case j%hurryEvery == 0:
					job.Priority = 9
				}
				if j%delayEvery == 0 {
					job.RunAfter = time.Now().Add(80 * time.Millisecond)
				}
				id, err := st.Enqueue(ctx, job)
				if err != nil {
					if errors.Is(err, ErrJobExists) {
						rejected.Add(1) // lost the one unique slot: expected
						continue
					}
					t.Errorf("Enqueue: %v", err)
					return
				}
				idMu.Lock()
				enqueuedIDs = append(enqueuedIDs, id)
				idMu.Unlock()
			}
		}(p)
	}

	var (
		terminals sync.Map // id -> struct{}, LoadOrStore guards dup delivery
		termCount atomic.Int64
	)
	var consWG sync.WaitGroup
	for c := 0; c < consumers; c++ {
		consWG.Add(1)
		go func() {
			defer consWG.Done()
			for {
				cctx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
				dj, err := st.Dequeue(cctx)
				cancel()
				if err != nil {
					if errors.Is(err, ErrQueueClosed) {
						return // store closed by main: consumer retires
					}
					if errors.Is(err, context.DeadlineExceeded) {
						continue // idle window elapsed: keep polling
					}
					t.Errorf("Dequeue: %v", err)
					return
				}
				if dj.Type == "flaky" && dj.Attempt == 1 {
					if err := st.Nack(context.Background(), dj.ID, errors.New("transient"), true, 5*time.Millisecond); err != nil {
						t.Errorf("Nack %s: %v", dj.ID, err)
						return
					}
					continue // will be redelivered and then acked
				}
				if err := st.Ack(context.Background(), dj.ID); err != nil {
					t.Errorf("Ack %s: %v", dj.ID, err)
					return
				}
				if _, loaded := terminals.LoadOrStore(dj.ID, struct{}{}); loaded {
					t.Errorf("job %s terminated twice — double delivery!", dj.ID)
					return
				}
				termCount.Add(1)
			}
		}()
	}

	prodWG.Wait()

	// Settle: wait until every accepted job reached a terminal state.
	accepted := 0
	deadline := time.Now().Add(settleLimit)
	for time.Now().Before(deadline) {
		idMu.Lock()
		accepted = len(enqueuedIDs)
		idMu.Unlock()
		if int(termCount.Load()) >= accepted && accepted > 0 {
			break
		}
		time.Sleep(pollTick)
	}
	idMu.Lock()
	accepted = len(enqueuedIDs)
	idMu.Unlock()

	// Settle finished above: every accepted job has terminated, so we
	// can stop the world and let idle consumers retire on ErrQueueClosed.
	st.Close()
	consWG.Wait()

	rej := rejected.Load()
	if rej == 0 {
		t.Error("unique-key path never exercised (no rejections seen)")
	}
	if accepted+int(rej) != total {
		t.Errorf("ledger broken: accepted=%d rejected=%d, want sum=%d",
			accepted, rej, total)
	}
	if got := int(termCount.Load()); got != accepted {
		t.Errorf("terminals = %d, want %d (every accepted job exactly once)",
			got, accepted)
	}
	p, r, s, d := st.Stats()
	if p != 0 || r != 0 || d != 0 || s != accepted {
		t.Errorf("final census = p%d/r%d/s%d/d%d, want 0/0/%d/0", p, r, s, d, accepted)
	}
}
