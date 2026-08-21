package goqueue

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// hookRecorder appends every fired hook event to a slice for order
// assertions. Safe for concurrent use.
type hookRecorder struct {
	mu   sync.Mutex
	evts []string
}

func (r *hookRecorder) add(evt string) {
	r.mu.Lock()
	r.evts = append(r.evts, evt)
	r.mu.Unlock()
}

func (r *hookRecorder) events() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.evts))
	copy(out, r.evts)
	return out
}

func allHooks(r *hookRecorder, cb func(string, JobInfo)) Hooks {
	return Hooks{
		OnEnqueue: func(i JobInfo) { cb("enqueue", i) },
		OnSuccess: func(i JobInfo) { cb("success", i) },
		OnFailure: func(i JobInfo) { cb("failure", i) },
		OnRetry:   func(i JobInfo) { cb("retry", i) },
		OnDead:    func(i JobInfo) { cb("dead", i) },
	}
}

func TestHooks_SuccessOrder(t *testing.T) {
	rec := &hookRecorder{}
	c := New(WithWorkers(1), WithHooks(allHooks(rec, func(evt string, _ JobInfo) {
		rec.add(evt)
	})))
	defer c.Shutdown(context.Background())

	c.Register("ok", func(ctx context.Context, payload []byte) error { return nil })
	c.Start()

	var got atomic.Value
	c.Register("ok2", nil) // placeholder not used

	if _, err := c.Enqueue(context.Background(), Job{Type: "ok", Payload: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool {
		return len(rec.events()) >= 2
	})

	evts := rec.events()
	if len(evts) < 2 {
		t.Fatalf("events = %v, want [enqueue success]", evts)
	}
	if evts[0] != "enqueue" || evts[1] != "success" {
		t.Fatalf("events = %v, want [enqueue success]", evts)
	}
	if len(evts) != 2 {
		t.Fatalf("events = %v, want exactly [enqueue success]", evts)
	}
	_ = got
}

func TestHooks_EnqueueSnapshot(t *testing.T) {
	rec := &hookRecorder{}
	c := New(WithHooks(allHooks(rec, func(evt string, i JobInfo) {
		if evt == "enqueue" {
			rec.add("enqueue")
		}
	})))
	_ = rec // unused, validated below via closure capture

	// Rebuild with a real capture of the JobInfo
	var mu sync.Mutex
	var snap JobInfo
	c2 := New(WithHooks(Hooks{
		OnEnqueue: func(i JobInfo) {
			mu.Lock()
			snap = i
			mu.Unlock()
		},
	}))
	defer c2.Shutdown(context.Background())

	c2.Register("t", func(ctx context.Context, payload []byte) error { return nil })

	id, err := c2.Enqueue(context.Background(), Job{Type: "t", Priority: 5, MaxRetry: 7, Payload: []byte("p")})
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if snap.ID != id {
		t.Fatalf("snap.ID = %q, want %q", snap.ID, id)
	}
	if snap.Type != "t" || snap.State != StatePending {
		t.Fatalf("snap = %+v, want type=t state=pending", snap)
	}
	if snap.Attempts != 0 {
		t.Fatalf("snap.Attempts = %d, want 0", snap.Attempts)
	}
	if snap.MaxRetry != 7 || snap.Priority != 5 {
		t.Fatalf("snap.MaxRetry=%d Priority=%d, want 7/5", snap.MaxRetry, snap.Priority)
	}
	if snap.EnqueuedAt.IsZero() {
		t.Fatal("snap.EnqueuedAt is zero")
	}
	c.mu.RLock()
	_ = c.types
	c.mu.RUnlock()
}

func TestHooks_FailureRetryDeadOrder(t *testing.T) {
	rec := &hookRecorder{}
	c := New(WithWorkers(1), WithHooks(allHooks(rec, func(evt string, _ JobInfo) {
		rec.add(evt)
	})))
	defer c.Shutdown(context.Background())

	c.Register("fail", func(ctx context.Context, payload []byte) error {
		return errors.New("boom")
	})
	c.Start()

	if _, err := c.Enqueue(context.Background(), Job{Type: "fail", MaxRetry: 1}); err != nil {
		t.Fatal(err)
	}
	// expect: enqueue, failure, retry, failure, dead (2 attempts: 1st retried, 2nd dead)
	waitFor(t, 2*time.Second, func() bool {
		return len(rec.events()) >= 5
	})

	evts := rec.events()
	want := []string{"enqueue", "failure", "retry", "failure", "dead"}
	if len(evts) != len(want) {
		t.Fatalf("events = %v, want %v", evts, want)
	}
	for i := range want {
		if evts[i] != want[i] {
			t.Fatalf("events[%d] = %q, want %q (full: %v)", i, evts[i], want[i], evts)
		}
	}
}

func TestHooks_RetryThenSuccess(t *testing.T) {
	rec := &hookRecorder{}
	c := New(WithWorkers(1), WithHooks(allHooks(rec, func(evt string, _ JobInfo) {
		rec.add(evt)
	})))
	defer c.Shutdown(context.Background())

	var attempts atomic.Int32
	c.Register("flaky", func(ctx context.Context, payload []byte) error {
		if attempts.Add(1) == 1 {
			return errors.New("first try fails")
		}
		return nil
	})
	c.Start()

	if _, err := c.Enqueue(context.Background(), Job{Type: "flaky", MaxRetry: 3}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool {
		return len(rec.events()) >= 4
	})

	evts := rec.events()
	want := []string{"enqueue", "failure", "retry", "success"}
	for i, w := range want {
		if evts[i] != w {
			t.Fatalf("events[%d] = %q, want %q (full: %v)", i, evts[i], w, evts)
		}
	}
}

func TestHooks_DeadSnapshot(t *testing.T) {
	var mu sync.Mutex
	var deadInfo JobInfo
	c := New(WithWorkers(1), WithHooks(Hooks{
		OnDead: func(i JobInfo) {
			mu.Lock()
			deadInfo = i
			mu.Unlock()
		},
	}))
	defer c.Shutdown(context.Background())

	c.Register("fail", func(ctx context.Context, payload []byte) error {
		return errors.New("always fails")
	})
	c.Start()

	id, err := c.Enqueue(context.Background(), Job{Type: "fail", MaxRetry: 1, Priority: 3, Payload: []byte("p")})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return deadInfo.ID != ""
	})

	mu.Lock()
	defer mu.Unlock()
	if deadInfo.ID != id || deadInfo.Type != "fail" {
		t.Fatalf("deadInfo = %+v, want id=%q type=fail", deadInfo, id)
	}
	if deadInfo.State != StateDead || deadInfo.Attempts != 2 {
		t.Fatalf("deadInfo state=%v attempts=%d, want dead/2", deadInfo.State, deadInfo.Attempts)
	}
	if deadInfo.MaxRetry != 1 || deadInfo.Priority != 3 {
		t.Fatalf("deadInfo MaxRetry=%d Priority=%d, want 1/3", deadInfo.MaxRetry, deadInfo.Priority)
	}
	if deadInfo.LastError == "" {
		t.Fatal("deadInfo.LastError empty")
	}
	if deadInfo.DeadAt.IsZero() {
		t.Fatal("deadInfo.DeadAt zero")
	}
	if deadInfo.EnqueuedAt.IsZero() {
		t.Fatal("deadInfo.EnqueuedAt zero")
	}
}

func TestHooks_LegacyOnDeadStillWorks(t *testing.T) {
	var got atomic.Int32
	c := New(WithWorkers(1), WithOnDead(func(info JobInfo) {
		got.Add(1)
	}))
	defer c.Shutdown(context.Background())

	c.Register("fail", func(ctx context.Context, payload []byte) error {
		return errors.New("nope")
	})
	c.Start()

	if _, err := c.Enqueue(context.Background(), Job{Type: "fail", MaxRetry: 0}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool { return got.Load() == 1 })
}

func TestHooks_NewOnDeadWinsOverLegacy(t *testing.T) {
	var newCall, legacyCall atomic.Int32
	c := New(WithWorkers(1),
		WithOnDead(func(info JobInfo) { legacyCall.Add(1) }),
		WithHooks(Hooks{OnDead: func(info JobInfo) { newCall.Add(1) }}),
	)
	defer c.Shutdown(context.Background())

	c.Register("fail", func(ctx context.Context, payload []byte) error {
		return errors.New("nope")
	})
	c.Start()

	if _, err := c.Enqueue(context.Background(), Job{Type: "fail", MaxRetry: 0}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool { return newCall.Load() == 1 })
	if legacyCall.Load() != 0 {
		t.Fatalf("legacy OnDead fired %d times, want 0 (Hooks.OnDead should win)", legacyCall.Load())
	}
}

func TestHooks_ConcurrentJobs(t *testing.T) {
	const n = 50
	var enq, succ atomic.Int32
	var fail, retry, dead atomic.Int32

	c := New(WithWorkers(4), WithHooks(Hooks{
		OnEnqueue: func(info JobInfo) { enq.Add(1) },
		OnSuccess: func(info JobInfo) { succ.Add(1) },
		OnFailure: func(info JobInfo) { fail.Add(1) },
		OnRetry:   func(info JobInfo) { retry.Add(1) },
		OnDead:    func(info JobInfo) { dead.Add(1) },
	}))
	defer c.Shutdown(context.Background())

	c.Register("ok", func(ctx context.Context, payload []byte) error { return nil })
	c.Register("bad", func(ctx context.Context, payload []byte) error { return errors.New("bad") })
	c.Start()

	ctx := context.Background()
	for i := 0; i < n; i++ {
		typ := "ok"
		if i%3 == 0 {
			typ = "bad"
		}
		if _, err := c.Enqueue(ctx, Job{Type: typ, MaxRetry: 1}); err != nil {
			t.Fatal(err)
		}
	}

	waitFor(t, 3*time.Second, func() bool {
		return enq.Load() == n && succ.Load()+fail.Load() >= n/2+1 &&
			dead.Load() >= 1
	})

	// Total invocations must be consistent: every enqueued job either
	// succeeded (1 success, 0 further events) or died (failures = attempts).
	if enq.Load() != n {
		t.Fatalf("enqueue events = %d, want %d", enq.Load(), n)
	}
	if succ.Load() != 33 && succ.Load() != 34 {
		t.Fatalf("success events = %d, want 33 or 34", succ.Load())
	}
	if retry.Load() != fail.Load()-dead.Load() {
		t.Fatalf("retry=%d fail=%d dead=%d inconsistent", retry.Load(), fail.Load(), dead.Load())
	}
	if succ.Load()+dead.Load() != n {
		t.Fatalf("succ+dead = %d, want %d", succ.Load()+dead.Load(), n)
	}
}