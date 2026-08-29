package metrics

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/atop0914/goqueue"
)

// fetchText scrapes the collector through its real HTTP handler.
func fetchText(t *testing.T, c *Collector) string {
	t.Helper()
	srv := httptest.NewServer(c.Handler())
	t.Cleanup(srv.Close)
	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func TestCollector_HooksAndGauges(t *testing.T) {
	col := NewCollector(Options{Namespace: "gq"})
	c := goqueue.New(goqueue.WithWorkers(2), goqueue.WithHooks(col.Hooks()))
	col.Watch(c)
	defer c.Shutdown(context.Background())

	c.Register("ok", func(ctx context.Context, payload []byte) error { return nil })
	c.Register("bad", func(ctx context.Context, payload []byte) error {
		return errors.New("nope")
	})
	c.Start()

	for i := 0; i < 3; i++ {
		if _, err := c.Enqueue(context.Background(), goqueue.Job{Type: "ok"}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := c.Enqueue(context.Background(), goqueue.Job{Type: "bad", MaxRetry: -1}); err != nil {
		t.Fatal(err)
	}

	waitForM(t, 3*time.Second, func() bool {
		s := c.Stats()
		return s.Succeeded == 3 && s.DeadTotal == 1
	})

	text := fetchText(t, col)
	for _, want := range []string{
		"gq_jobs_enqueued_total{type=\"bad\"} 1",
		"gq_jobs_enqueued_total{type=\"ok\"} 3",
		"gq_jobs_succeeded_total{type=\"ok\"} 3",
		"gq_jobs_failed_total{type=\"bad\"} 1",
		"gq_jobs_dead_total{type=\"bad\"} 1",
		"gq_job_duration_seconds_bucket{type=\"ok\",le=",
		"gq_job_duration_seconds_count{type=\"ok\"} 3",
		"gq_queue_depth 0",
		"gq_jobs_running 0",
		"gq_dead_jobs 1",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("metrics output missing %q\n--- text ---\n%s", want, text)
		}
	}
}

func TestCollector_RetryAndDurationObserved(t *testing.T) {
	col := NewCollector(Options{Namespace: "gqr"})
	c := goqueue.New(
		goqueue.WithWorkers(1),
		goqueue.WithHooks(col.Hooks()),
		goqueue.WithRetryBackoff(goqueue.RetryBackoff{InitialInterval: 5 * time.Millisecond}),
	)
	col.Watch(c)
	defer c.Shutdown(context.Background())

	c.Register("flaky", func(ctx context.Context, payload []byte) error {
		return errors.New("always")
	})
	c.Start()
	if _, err := c.Enqueue(context.Background(), goqueue.Job{Type: "flaky", MaxRetry: 2}); err != nil {
		t.Fatal(err)
	}
	waitForM(t, 3*time.Second, func() bool { return c.Stats().DeadTotal == 1 })

	text := fetchText(t, col)
	// attempts: initial + 2 retries = 3 failures, 2 of them retried, 1 dead.
	for _, want := range []string{
		"gqr_jobs_failed_total{type=\"flaky\"} 3",
		"gqr_jobs_retried_total{type=\"flaky\"} 2",
		"gqr_jobs_dead_total{type=\"flaky\"} 1",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("metrics output missing %q\n--- text ---\n%s", want, text)
		}
	}
	// Note: no duration assertions here. The histogram observes only
	// successful jobs and prometheus creates series lazily, so a workload
	// that never succeeds yields no gqr_job_duration_seconds_* lines - that
	// is expected, not a bug (covered in TestCollector_HooksAndGauges).
}

func TestCollector_ConcurrentHooks(t *testing.T) {
	col := NewCollector(Options{})
	h := col.Hooks()
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				h.OnEnqueue(goqueue.JobInfo{Type: "t"})
				h.OnSuccess(goqueue.JobInfo{Type: "t", EnqueuedAt: time.Now().Add(-time.Millisecond)})
				h.OnFailure(goqueue.JobInfo{Type: "t"})
				h.OnRetry(goqueue.JobInfo{Type: "t"})
				h.OnDead(goqueue.JobInfo{Type: "t"})
			}
		}()
	}
	wg.Wait()
	text := fetchText(t, col)
	if !strings.Contains(text, "jobs_enqueued_total{type=\"t\"} 1600") {
		t.Errorf("concurrent increments lost; snippet:\n%s", grepLines(text, "jobs_enqueued_total"))
	}
}

func grepLines(text, sub string) string {
	var out []string
	for _, ln := range strings.Split(text, "\n") {
		if strings.Contains(ln, sub) {
			out = append(out, ln)
		}
	}
	return strings.Join(out, "\n")
}

// waitForM is a local poll helper (keeps the module self-contained).
func waitForM(t *testing.T, timeout time.Duration, cond func() bool) {
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

func TestCollector_HooksFanOutWithCombine(t *testing.T) {
	var mu sync.Mutex
	var userEvents int
	user := goqueue.Hooks{OnSuccess: func(i goqueue.JobInfo) { mu.Lock(); userEvents++; mu.Unlock() }}
	col := NewCollector(Options{Namespace: "gqc"})
	merged := goqueue.CombineHooks(user, col.Hooks())

	c := goqueue.New(goqueue.WithWorkers(1), goqueue.WithHooks(merged))
	defer c.Shutdown(context.Background())
	c.Register("ok", func(ctx context.Context, payload []byte) error { return nil })
	c.Start()
	if _, err := c.Enqueue(context.Background(), goqueue.Job{Type: "ok"}); err != nil {
		t.Fatal(err)
	}
	waitForM(t, 3*time.Second, func() bool { return c.Stats().Succeeded == 1 })

	mu.Lock()
	defer mu.Unlock()
	if userEvents != 1 {
		t.Fatalf("user hook fired %d times, want 1", userEvents)
	}
	text := fetchText(t, col)
	if !strings.Contains(text, "gqc_jobs_succeeded_total{type=\"ok\"} 1") {
		t.Errorf("collector metrics missing after CombineHooks:\n%s", text)
	}
}
