package goqueue

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// collector gathers JobInfo snapshots per event slot for CombineHooks tests.
type collector struct {
	mu    sync.Mutex
	byEvt map[string][]JobInfo
}

func (c *collector) hooks() Hooks {
	add := func(evt string) func(JobInfo) {
		return func(i JobInfo) {
			c.mu.Lock()
			c.byEvt[evt] = append(c.byEvt[evt], i)
			c.mu.Unlock()
		}
	}
	return Hooks{OnEnqueue: add("enqueue"), OnSuccess: add("success"), OnFailure: add("failure"), OnRetry: add("retry"), OnDead: add("dead")}
}

func (c *collector) count(evt string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.byEvt[evt])
}

func TestCombineHooks_FansOutInOrder(t *testing.T) {
	var trace []string
	var tmu sync.Mutex
	mark := func(name string) func(JobInfo) {
		return func(JobInfo) { tmu.Lock(); trace = append(trace, name); tmu.Unlock() }
	}
	hA := Hooks{OnEnqueue: mark("A"), OnSuccess: mark("A")}
	hB := Hooks{OnEnqueue: mark("B"), OnSuccess: mark("B")}
	hEmpty := Hooks{} // all nil slots must be skipped silently

	merged := CombineHooks(hA, hEmpty, hB)
	c := New(WithWorkers(1), WithHooks(merged))
	defer c.Shutdown(context.Background())
	c.Register("ok", func(ctx context.Context, payload []byte) error { return nil })
	c.Start()
	if _, err := c.Enqueue(context.Background(), Job{Type: "ok"}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool { return c.Stats().Succeeded == 1 })

	tmu.Lock()
	defer tmu.Unlock()
	want := []string{"A", "B", "A", "B"} // enqueue pair, then success pair
	if len(trace) != len(want) {
		t.Fatalf("trace = %v, want %v", trace, want)
	}
	for i := range want {
		if trace[i] != want[i] {
			t.Fatalf("trace = %v, want %v", trace, want)
		}
	}
}

func TestCombineHooks_NilSlotsSkipped(t *testing.T) {
	var col collector
	col.byEvt = map[string][]JobInfo{}
	enqOnly := Hooks{OnEnqueue: func(i JobInfo) { col.hooks().OnEnqueue(i) }}
	merged := CombineHooks(enqOnly)
	c := New(WithWorkers(1), WithHooks(merged))
	defer c.Shutdown(context.Background())
	c.Register("flaky", func(ctx context.Context, payload []byte) error {
		return errBoomJob
	})
	c.Start()
	if _, err := c.Enqueue(context.Background(), Job{Type: "flaky", MaxRetry: 0}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool { return c.Stats().DeadTotal == 1 })
	if got := col.count("enqueue"); got != 1 {
		t.Fatalf("enqueue events = %d, want 1", got)
	}
	// The nil success/failure/retry/dead slots must not panic when merged
	// callbacks fire for those events (they are no-ops) - reaching here
	// without a panic is the assertion.
}

func TestWithContextDecorator_ReachesHandler(t *testing.T) {
	type ctxKey struct{}
	seen := make(chan string, 1)
	dec := func(ctx context.Context) context.Context {
		return context.WithValue(ctx, ctxKey{}, "decorated")
	}
	c := New(WithWorkers(1), WithContextDecorator(dec))
	defer c.Shutdown(context.Background())
	c.Register("probe", func(ctx context.Context, payload []byte) error {
		select {
		case seen <- ctx.Value(ctxKey{}).(string):
		default:
		}
		return nil
	})
	c.Start()
	if _, err := c.Enqueue(context.Background(), Job{Type: "probe"}); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-seen:
		if got != "decorated" {
			t.Fatalf("handler saw %q, want %q", got, "decorated")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("handler never observed the decorated context")
	}
}

func TestWithContextDecorator_NilDecoratorsTolerated(t *testing.T) {
	nilDec := func(ctx context.Context) context.Context { return nil }
	c := New(WithWorkers(1), WithContextDecorator(nil), WithContextDecorator(nilDec))
	defer c.Shutdown(context.Background())
	c.Register("ok", func(ctx context.Context, payload []byte) error { return nil })
	c.Start()
	if _, err := c.Enqueue(context.Background(), Job{Type: "ok"}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool { return c.Stats().Succeeded == 1 })
}

var errBoomJob = errors.New("boom")
