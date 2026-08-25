package dashboard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/atop0914/goqueue"
)

// newTestClient returns a Client with an "ok" handler (immediate success)
// and a "boom" handler (immediate failure, retries disabled so it lands in
// the DLQ on the first attempt).
func newTestClient(t *testing.T) *goqueue.Client {
	t.Helper()
	c := goqueue.New(goqueue.WithWorkers(2), goqueue.WithPollInterval(5*time.Millisecond))
	c.Register("ok", func(ctx context.Context, p []byte) error { return nil })
	c.Register("boom", func(ctx context.Context, p []byte) error { return errors.New("kaboom") })
	return c
}

func serve(d *Dashboard) *httptest.Server {
	return httptest.NewServer(d)
}

func get(t *testing.T, srv *httptest.Server, path string) (*http.Response, string) {
	t.Helper()
	resp, err := srv.Client().Get(srv.URL + path)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer resp.Body.Close()
	var buf strings.Builder
	buf.Grow(4096)
	b := make([]byte, 4096)
	for {
		n, err := resp.Body.Read(b)
		buf.Write(b[:n])
		if err != nil {
			break
		}
	}
	return resp, buf.String()
}

// waitFor polls cond until it returns true or the deadline passes.
func waitFor(t *testing.T, d time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", msg)
}

func TestHealthzLiveness(t *testing.T) {
	d := New(newTestClient(t))
	srv := serve(d)
	defer srv.Close()

	resp, body := get(t, srv, "/healthz")
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if strings.TrimSpace(body) != "ok" {
		t.Errorf("body = %q, want \"ok\"", body)
	}
}

func TestReadinessDefault(t *testing.T) {
	d := New(newTestClient(t))
	srv := serve(d)
	defer srv.Close()

	resp, body := get(t, srv, "/healthz/ready")
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if strings.TrimSpace(body) != "ready" {
		t.Errorf("body = %q, want \"ready\"", body)
	}
}

func TestReadinessCustomCheck(t *testing.T) {
	d := New(newTestClient(t), WithReadyCheck(func() error {
		return errors.New("db unreachable")
	}))
	srv := serve(d)
	defer srv.Close()

	resp, body := get(t, srv, "/healthz/ready")
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", resp.StatusCode)
	}
	if !strings.Contains(body, "db unreachable") {
		t.Errorf("body = %q, want it to mention the check error", body)
	}
}

func TestAPIStatus(t *testing.T) {
	c := newTestClient(t)
	d := New(c)
	srv := serve(d)
	defer srv.Close()

	for i := 0; i < 3; i++ {
		if _, err := c.Enqueue(context.Background(), goqueue.Job{Type: "ok"}); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}

	resp, body := get(t, srv, "/api/status")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	var st statusResponse
	if err := json.Unmarshal([]byte(body), &st); err != nil {
		t.Fatalf("unmarshal status: %v (body=%q)", err, body)
	}
	if st.Pending != 3 {
		t.Errorf("pending = %d, want 3", st.Pending)
	}
	if st.Running != 0 {
		t.Errorf("running = %d, want 0 (workers not started)", st.Running)
	}
	if st.Workers != 2 {
		t.Errorf("workers = %d, want 2", st.Workers)
	}
	if st.Started {
		t.Error("started = true, want false")
	}
	if st.UptimeSeconds < 0 {
		t.Errorf("uptime_seconds = %d, want >= 0", st.UptimeSeconds)
	}
}

func TestAPIStatsFullLifecycle(t *testing.T) {
	c := newTestClient(t)
	d := New(c)
	srv := serve(d)
	defer srv.Close()

	c.Start()
	defer func() { _ = c.Shutdown(context.Background()) }()

	for i := 0; i < 2; i++ {
		if _, err := c.Enqueue(context.Background(), goqueue.Job{Type: "ok"}); err != nil {
			t.Fatalf("enqueue ok: %v", err)
		}
	}
	if _, err := c.Enqueue(context.Background(), goqueue.Job{Type: "boom", MaxRetry: -1}); err != nil {
		t.Fatalf("enqueue boom: %v", err)
	}

	waitFor(t, 5*time.Second, func() bool {
		return c.Stats().Succeeded == 2 && c.Stats().DeadTotal == 1
	}, "2 successes + 1 dead letter")

	resp, body := get(t, srv, "/api/stats")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var st statsResponse
	if err := json.Unmarshal([]byte(body), &st); err != nil {
		t.Fatalf("unmarshal stats: %v", err)
	}
	if st.Enqueued != 3 {
		t.Errorf("enqueued = %d, want 3", st.Enqueued)
	}
	if st.Succeeded != 2 {
		t.Errorf("succeeded = %d, want 2", st.Succeeded)
	}
	if st.Failed != 1 {
		t.Errorf("failed = %d, want 1", st.Failed)
	}
	if st.DeadTotal != 1 {
		t.Errorf("dead_total = %d, want 1", st.DeadTotal)
	}
	if st.ByType["ok"] != 2 || st.ByType["boom"] != 1 {
		t.Errorf("by_type = %v, want ok:2 boom:1", st.ByType)
	}
}

func TestAPIJobsDeadLetters(t *testing.T) {
	c := newTestClient(t)
	d := New(c)
	srv := serve(d)
	defer srv.Close()

	c.Start()
	defer func() { _ = c.Shutdown(context.Background()) }()
	if _, err := c.Enqueue(context.Background(), goqueue.Job{Type: "boom", MaxRetry: -1}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	waitFor(t, 5*time.Second, func() bool { return c.Stats().DeadTotal == 1 }, "dead letter")

	resp, body := get(t, srv, "/api/jobs")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var payload struct {
		Dead []deadJobDTO `json:"dead"`
	}
	if err := json.Unmarshal([]byte(body), &payload); err != nil {
		t.Fatalf("unmarshal jobs: %v", err)
	}
	if len(payload.Dead) != 1 {
		t.Fatalf("dead jobs = %d, want 1", len(payload.Dead))
	}
	j := payload.Dead[0]
	if j.Type != "boom" {
		t.Errorf("type = %q, want boom", j.Type)
	}
	if j.State != "dead" {
		t.Errorf("state = %q, want dead", j.State)
	}
	if j.Attempts != 1 {
		t.Errorf("attempts = %d, want 1", j.Attempts)
	}
	if j.MaxRetry != -1 {
		t.Errorf("max_retry = %d, want -1", j.MaxRetry)
	}
	if !strings.Contains(j.LastError, "kaboom") {
		t.Errorf("last_error = %q, want it to contain kaboom", j.LastError)
	}
	if j.DeadAt.IsZero() {
		t.Error("dead_at is zero, want the death timestamp")
	}
	if j.ID == "" {
		t.Error("id is empty, want the job id")
	}
}

func TestAPIJobsOrderAndCap(t *testing.T) {
	c := newTestClient(t)
	d := New(c, WithMaxDeadJobs(2))
	srv := serve(d)
	defer srv.Close()

	c.Start()
	defer func() { _ = c.Shutdown(context.Background()) }()
	// Three distinct ids so we can tell the newest apart.
	for i := 0; i < 3; i++ {
		if _, err := c.Enqueue(context.Background(), goqueue.Job{
			ID:       fmt.Sprintf("dead-%d", i),
			Type:     "boom",
			MaxRetry: -1,
		}); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}
	waitFor(t, 5*time.Second, func() bool {
		return c.Stats().DeadTotal == 3
	}, "three dead letters")

	_, body := get(t, srv, "/api/jobs")
	var payload struct {
		Dead []deadJobDTO `json:"dead"`
	}
	if err := json.Unmarshal([]byte(body), &payload); err != nil {
		t.Fatalf("unmarshal jobs: %v", err)
	}
	if len(payload.Dead) != 2 {
		t.Fatalf("dead jobs = %d, want 2 (capped)", len(payload.Dead))
	}
	// Newest first: dead-2 died after dead-1.
	if payload.Dead[0].ID != "dead-2" {
		t.Errorf("newest dead job = %q, want dead-2", payload.Dead[0].ID)
	}
	if payload.Dead[1].ID != "dead-1" {
		t.Errorf("second dead job = %q, want dead-1", payload.Dead[1].ID)
	}
}

func TestOverviewHTML(t *testing.T) {
	c := newTestClient(t)
	d := New(c, WithTitle("Job Runner"))
	srv := serve(d)
	defer srv.Close()

	if _, err := c.Enqueue(context.Background(), goqueue.Job{Type: "ok"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	resp, body := get(t, srv, "/")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Errorf("Content-Type = %q, want text/html", ct)
	}
	for _, want := range []string{"Job Runner", "c-pending", "id=\"t-enqueued\"", "Dead-letter queue"} {
		if !strings.Contains(body, want) {
			t.Errorf("overview HTML missing %q", want)
		}
	}
	// The initial server-side render must show the enqueued count in the
	// pending card: <div class="num" id="c-pending">1</div>
	if !strings.Contains(body, "c-pending\">1<") {
		t.Errorf("overview should render pending=1 in the pending card, got body:\n%s", body)
	}
}

func TestOverviewShowsDeadLetterRow(t *testing.T) {
	c := newTestClient(t)
	d := New(c)
	srv := serve(d)
	defer srv.Close()

	c.Start()
	defer func() { _ = c.Shutdown(context.Background()) }()
	if _, err := c.Enqueue(context.Background(), goqueue.Job{Type: "boom", MaxRetry: -1}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	waitFor(t, 5*time.Second, func() bool { return c.Stats().DeadTotal == 1 }, "dead letter")

	_, body := get(t, srv, "/")
	if !strings.Contains(body, "kaboom") {
		t.Errorf("overview should render the dead-letter error, got body:\n%s", body)
	}
	if !strings.Contains(body, "boom") {
		t.Errorf("overview should render the dead job type")
	}
}

func TestUnknownPath404(t *testing.T) {
	d := New(newTestClient(t))
	srv := serve(d)
	defer srv.Close()

	resp, _ := get(t, srv, "/nope")
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
}

// TestConcurrentScrapes hammers the endpoints from many goroutines while
// jobs flow through the client, to prove the dashboard is race-free and
// never panics under load.
func TestConcurrentScrapes(t *testing.T) {
	c := newTestClient(t)
	d := New(c)
	srv := serve(d)
	defer srv.Close()

	c.Start()
	defer func() { _ = c.Shutdown(context.Background()) }()

	stop := make(chan struct{})
	var wg sync.WaitGroup
	// Producer: keep enqueueing jobs while scrapers run.
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			typ := "ok"
			if i%5 == 0 {
				typ = "boom"
			}
			_, _ = c.Enqueue(context.Background(), goqueue.Job{Type: typ, MaxRetry: -1})
			i++
		}
	}()
	// Scrapers.
	paths := []string{"/", "/api/status", "/api/stats", "/api/jobs", "/healthz", "/healthz/ready"}
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 30; j++ {
				p := paths[j%len(paths)]
				resp, err := srv.Client().Get(srv.URL + p)
				if err != nil {
					t.Errorf("GET %s: %v", p, err)
					return
				}
				_, _ = ioReadAll(resp)
				resp.Body.Close()
			}
		}()
	}
	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()
}

func ioReadAll(resp *http.Response) ([]byte, error) {
	var buf []byte
	b := make([]byte, 2048)
	for {
		n, err := resp.Body.Read(b)
		buf = append(buf, b[:n]...)
		if err != nil {
			return buf, err
		}
	}
}