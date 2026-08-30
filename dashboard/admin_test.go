package dashboard

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/atop0914/goqueue"
)

// atomicCounter is a tiny race-safe counter for handler invocations.
type atomicCounter struct{ n atomic.Int64 }

func (c *atomicCounter) add(d int64) { c.n.Add(d) }
func (c *atomicCounter) load() int64 { return c.n.Load() }

// post issues a POST against the test server and returns status + decoded
// envelope.
func post(t *testing.T, srv *httptest.Server, path, body string) (int, map[string]any) {
	t.Helper()
	var resp *http.Response
	var err error
	if body == "" {
		resp, err = srv.Client().Post(srv.URL+path, "application/json", nil)
	} else {
		resp, err = srv.Client().Post(srv.URL+path, "application/json", strings.NewReader(body))
	}
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	defer resp.Body.Close()
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("POST %s: decode: %v", path, err)
	}
	return resp.StatusCode, out
}

func TestDashboard_AdminPauseResume(t *testing.T) {
	c := newTestClient(t)
	defer c.Shutdown(context.Background())
	srv := serve(New(c))
	defer srv.Close()

	// GET is rejected with 405.
	resp, _ := get(t, srv, "/api/admin/pause")
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("GET pause status = %d, want 405", resp.StatusCode)
	}

	code, env := post(t, srv, "/api/admin/pause", "")
	if code != http.StatusOK || env["ok"] != true {
		t.Fatalf("pause = (%d, %v), want 200 ok", code, env)
	}
	if !c.IsPaused() {
		t.Fatal("client not paused after /api/admin/pause")
	}

	code, env = post(t, srv, "/api/admin/resume", "")
	if code != http.StatusOK || env["ok"] != true {
		t.Fatalf("resume = (%d, %v), want 200 ok", code, env)
	}
	if c.IsPaused() {
		t.Fatal("client still paused after /api/admin/resume")
	}
}

func TestDashboard_AdminPauseGatesDelivery(t *testing.T) {
	c := goqueue.New(goqueue.WithWorkers(2), goqueue.WithPollInterval(5*time.Millisecond))
	defer c.Shutdown(context.Background())
	var ran atomicCounter
	c.Register("ok", func(ctx context.Context, p []byte) error { ran.add(1); return nil })
	srv := serve(New(c))
	defer srv.Close()
	c.Start()

	if code, _ := post(t, srv, "/api/admin/pause", ""); code != http.StatusOK {
		t.Fatal("pause failed")
	}
	for i := 0; i < 3; i++ {
		if _, err := c.Enqueue(context.Background(), goqueue.Job{Type: "ok"}); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(100 * time.Millisecond)
	if n := ran.load(); n != 0 {
		t.Fatalf("handler ran %d times while paused via dashboard, want 0", n)
	}
	if code, _ := post(t, srv, "/api/admin/resume", ""); code != http.StatusOK {
		t.Fatal("resume failed")
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && ran.load() < 3 {
		time.Sleep(10 * time.Millisecond)
	}
	if n := ran.load(); n != 3 {
		t.Fatalf("after resume ran %d, want 3", n)
	}
}

func TestDashboard_AdminPurgeAndRequeue(t *testing.T) {
	c := newTestClient(t)
	defer c.Shutdown(context.Background())
	srv := serve(New(c))
	defer srv.Close()
	ctx := context.Background()

	// Drive the queue directly (no worker pool) to stage a dead letter.
	q := c.Queue().(*goqueue.InMemoryQueue)
	id, err := c.Enqueue(ctx, goqueue.Job{Type: "boom", MaxRetry: -1})
	if err != nil {
		t.Fatal(err)
	}
	dj, err := q.Dequeue(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Nack(ctx, dj.ID, errors.New("kaboom"), false, 0); err != nil {
		t.Fatal(err)
	}
	if _, dead := c.Info(); dead != 1 {
		t.Fatalf("dead = %d, want 1", dead)
	}

	// Requeue via endpoint.
	code, env := post(t, srv, "/api/admin/requeue-dead", "")
	if code != http.StatusOK || env["ok"] != true {
		t.Fatalf("requeue-dead = (%d, %v), want 200 ok", code, env)
	}
	if n, _ := env["requeued"].(float64); n != 1 {
		t.Fatalf("requeued = %v, want 1", env["requeued"])
	}
	if _, dead := c.Info(); dead != 0 {
		t.Fatalf("dead after requeue = %d, want 0", dead)
	}
	if got := q.Len(); got != 1 {
		t.Fatalf("pending after requeue = %d, want 1", got)
	}

	// Purge including the DLQ (empty here — the requeued job is pending).
	code, env = post(t, srv, "/api/admin/purge", `{"dead":true}`)
	if code != http.StatusOK || env["ok"] != true {
		t.Fatalf("purge = (%d, %v), want 200 ok", code, env)
	}
	if n, _ := env["purged"].(float64); n != 1 {
		t.Fatalf("purged = %v, want 1 (the requeued pending job)", env["purged"])
	}
	if p, dead := c.Info(); p != 0 || dead != 0 {
		t.Fatalf("after purge pending=%d dead=%d, want 0/0", p, dead)
	}
	// Idempotent.
	code, env = post(t, srv, "/api/admin/purge", `{"dead":true}`)
	if n, _ := env["purged"].(float64); code != http.StatusOK || n != 0 {
		t.Fatalf("second purge = (%d, %v), want 200/0", code, env)
	}
	_ = id
}

func TestDashboard_AdminRequeueSingleJobErrors(t *testing.T) {
	c := newTestClient(t)
	defer c.Shutdown(context.Background())
	srv := serve(New(c))
	defer srv.Close()

	// Unknown job ID -> 404 with ok:false.
	code, env := post(t, srv, "/api/admin/requeue-dead", `{"id":"does-not-exist"}`)
	if code != http.StatusNotFound || env["ok"] != false {
		t.Fatalf("unknown id = (%d, %v), want 404 ok:false", code, env)
	}
	if msg, _ := env["error"].(string); msg == "" {
		t.Fatal("error message missing")
	}
}
