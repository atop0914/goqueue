package tracing

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/atop0914/goqueue"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"sync/atomic"
)

// setupTracer swaps in a synchronous exporter-backed tracer provider for
// the duration of the test and restores the previous one afterwards.
func setupTracer(t *testing.T) *tracetest.InMemoryExporter {
	t.Helper()
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	orig := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		otel.SetTracerProvider(orig)
	})
	return exporter
}

// wantSpans polls until the exporter holds at least n spans.
func wantSpans(t *testing.T, exp *tracetest.InMemoryExporter, n int) []tracetest.SpanStub {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if got := exp.GetSpans(); len(got) >= n {
			return got
		}
		time.Sleep(5 * time.Millisecond)
	}
	return exp.GetSpans()
}

func attrMap(attrs []attribute.KeyValue) map[string]attribute.Value {
	m := make(map[string]attribute.Value, len(attrs))
	for _, a := range attrs {
		m[string(a.Key)] = a.Value
	}
	return m
}

func TestTracer_SuccessSpan(t *testing.T) {
	exp := setupTracer(t)
	tr := NewTracer(Options{ServiceName: "test-svc"})
	reg := NewRegistry()
	defer reg.Close()

	c := goqueue.New(goqueue.WithWorkers(1), goqueue.WithHooks(tr.Hooks(reg)))
	defer c.Shutdown(context.Background())
	c.Register("ok", func(ctx context.Context, payload []byte) error { return nil })
	c.Start()
	if _, err := c.Enqueue(context.Background(), goqueue.Job{Type: "ok"}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool { return c.Stats().Succeeded == 1 })

	spans := wantSpans(t, exp, 1)
	if len(spans) != 1 {
		t.Fatalf("got %d spans, want 1", len(spans))
	}
	sp := spans[0]
	if sp.Name != "goqueue.process" {
		t.Errorf("span name = %q, want goqueue.process", sp.Name)
	}
	if sp.Status.Code != codes.Ok {
		t.Errorf("status = %v, want Ok", sp.Status)
	}
	m := attrMap(sp.Attributes)
	if v := m["goqueue.job.type"]; v.AsString() != "ok" {
		t.Errorf("job.type = %v, want ok", v)
	}
	if v := m["goqueue.job.attempt"]; v.AsInt64() != 1 {
		t.Errorf("job.attempt = %v, want 1", v)
	}
	if v := m["service.name"]; v.AsString() != "test-svc" {
		t.Errorf("service.name = %v, want test-svc", v)
	}
	if v := m["messaging.system"]; v.AsString() != "goqueue" {
		t.Errorf("messaging.system = %v, want goqueue", v)
	}
}

func TestTracer_RetryThenDeadSpan(t *testing.T) {
	exp := setupTracer(t)
	tr := NewTracer(Options{})
	reg := NewRegistry()
	defer reg.Close()

	c := goqueue.New(
		goqueue.WithWorkers(1),
		goqueue.WithHooks(tr.Hooks(reg)),
		goqueue.WithRetryBackoff(goqueue.RetryBackoff{InitialInterval: 5 * time.Millisecond}),
	)
	defer c.Shutdown(context.Background())
	c.Register("flaky", func(ctx context.Context, payload []byte) error { return errors.New("always") })
	c.Start()
	if _, err := c.Enqueue(context.Background(), goqueue.Job{Type: "flaky", MaxRetry: 2}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool { return c.Stats().DeadTotal == 1 })

	spans := wantSpans(t, exp, 1)
	if len(spans) != 1 {
		t.Fatalf("got %d spans, want 1 (retries must not fork extra spans)", len(spans))
	}
	sp := spans[0]
	if sp.Status.Code != codes.Error {
		t.Errorf("status = %v, want Error", sp.Status)
	}
	m := attrMap(sp.Attributes)
	if v := m["goqueue.job.attempt"]; v.AsInt64() != 3 {
		t.Errorf("final attempt = %v, want 3", v)
	}
	var retries, dead, errs int
	for _, ev := range sp.Events {
		switch ev.Name {
		case "goqueue.retry":
			retries++
		case "goqueue.dead_letter":
			dead++
		}
		if ev.Name == "exception" {
			errs++
		}
	}
	if retries != 2 || dead != 1 || errs != 3 {
		t.Errorf("events = %d retries / %d dead / %d errors, want 2/1/3", retries, dead, errs)
	}
}

func TestTracer_EnqueueSpanReachesHandler(t *testing.T) {
	exp := setupTracer(t)
	tr := NewTracer(Options{})
	reg := NewRegistry()
	defer reg.Close()

	seen := make(chan trace.Span, 1)
	decorated := tr.ContextDecorator(reg)
	c := goqueue.New(
		goqueue.WithWorkers(1),
		goqueue.WithHooks(tr.Hooks(reg)),
		goqueue.WithContextDecorator(decorated),
	)
	defer c.Shutdown(context.Background())
	c.Register("probe", func(ctx context.Context, payload []byte) error {
		if sp := FromJobContext(ctx); sp != nil {
			select {
			case seen <- sp:
			default:
			}
		}
		return nil
	})
	c.Start()

	ctx, enqueueSpan := tr.StartJobSpan(reg, context.Background(), "probe", "job-42")
	if _, err := c.Enqueue(ctx, goqueue.Job{ID: "job-42", Type: "probe"}); err != nil {
		t.Fatal(err)
	}
	select {
	case sp := <-seen:
		if !sp.SpanContext().Equal(enqueueSpan.SpanContext()) {
			t.Error("handler saw a different span context than the enqueue span")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("handler never observed the job span")
	}
	enqueueSpan.End()
	waitFor(t, 2*time.Second, func() bool { return c.Stats().Succeeded == 1 })
	spans := wantSpans(t, exp, 2)
	if len(spans) < 2 {
		t.Fatalf("got %d spans, want at least 2 (process + enqueue)", len(spans))
	}
}

func TestTracer_UntrackedJobsAreNoops(t *testing.T) {
	exp := setupTracer(t)
	tr := NewTracer(Options{})
	reg := NewRegistry()
	defer reg.Close()

	h := tr.Hooks(reg)
	// Worker-side events for a job the tracer never saw must not panic or
	// emit anything.
	h.OnSuccess(goqueue.JobInfo{ID: "ghost", Type: "ok"})
	h.OnFailure(goqueue.JobInfo{ID: "ghost", Type: "ok"})
	h.OnRetry(goqueue.JobInfo{ID: "ghost", Type: "ok"})
	h.OnDead(goqueue.JobInfo{ID: "ghost", Type: "ok", LastError: "boom"})
	if got := len(exp.GetSpans()); got != 0 {
		t.Fatalf("got %d spans for untracked jobs, want 0", got)
	}

	// The reaper must eventually forget abandoned jobs.
	h.OnEnqueue(goqueue.JobInfo{ID: "abandoned", Type: "ok"})
	reg.nowFunc = func() time.Time { return time.Now().Add(2 * time.Hour) }
	reg.reap()
	h.OnSuccess(goqueue.JobInfo{ID: "abandoned", Type: "ok"}) // no-op now
	if got := len(exp.GetSpans()); got != 0 {
		t.Fatalf("got %d spans after reap, want 0", got)
	}
}

func TestTracer_HooksCombineWithUserHooks(t *testing.T) {
	exp := setupTracer(t)
	tr := NewTracer(Options{})
	reg := NewRegistry()
	defer reg.Close()

	var userHits atomic.Int64
	user := goqueue.Hooks{OnSuccess: func(i goqueue.JobInfo) { userHits.Add(1) }}
	merged := goqueue.CombineHooks(user, tr.Hooks(reg))

	c := goqueue.New(goqueue.WithWorkers(1), goqueue.WithHooks(merged))
	defer c.Shutdown(context.Background())
	c.Register("ok", func(ctx context.Context, payload []byte) error { return nil })
	c.Start()
	if _, err := c.Enqueue(context.Background(), goqueue.Job{Type: "ok"}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool { return c.Stats().Succeeded == 1 })
	if got := userHits.Load(); got != 1 {
		t.Fatalf("user hook fired %d times, want 1", got)
	}
	wantSpans(t, exp, 1)
	if got := len(exp.GetSpans()); got != 1 {
		t.Fatalf("got %d spans, want 1", got)
	}
}

// waitFor polls until cond turns true or the timeout elapses (local helper
// to keep the module self-contained).
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	cond()
}
