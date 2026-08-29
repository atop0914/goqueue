// Package metrics exposes goqueue job-lifecycle and queue-depth metrics in
// the Prometheus text format. It is an optional extension: the goqueue core
// has zero external dependencies, and this package (like every obs/*
// package) lives in its own Go module so importers who do not need
// Prometheus never pull client_golang.
//
// Integration is two-sided and mirrors the dashboard package's philosophy:
//
//	col := metrics.NewCollector(metrics.Options{Namespace: "goqueue"})
//	client := goqueue.New(goqueue.WithHooks(col.Hooks()))
//	col.Watch(client) // queue-depth gauges sampled from client.Stats()
//	http.Handle("/metrics", col.Handler())
//
// Hooks() returns the lifecycle callbacks that increment the event
// counters and observe durations; Watch(client) registers pull-based gauge
// functions that read client.Stats() on every scrape. Both are optional:
// wiring only the hooks yields event counters, wiring only Watch yields
// queue gauges.
package metrics

import (
	"net/http"
	"time"

	"github.com/atop0914/goqueue"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// DefaultBuckets is the end-to-end job duration histogram buckets used when
// Options.DurationBuckets is empty. They span the sub-millisecond in-memory
// regime up to minute-long handler executions.
var DefaultBuckets = []float64{0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60}

// Options tunes the metric names and histogram shape of a Collector.
type Options struct {
	// Namespace prefixes every metric name ("goqueue_jobs_enqueued_total"
	// with the default). Empty yields bare names ("jobs_enqueued_total").
	Namespace string

	// DurationBuckets overrides the end-to-end job duration histogram
	// buckets (seconds). Empty selects DefaultBuckets.
	DurationBuckets []float64
}

// Collector turns goqueue lifecycle hooks into Prometheus metrics and
// samples a Client's Stats into gauges on every scrape. All metric
// operations are concurrency-safe (they are prometheus primitives); the
// hooks are safe to fire from any number of producer and worker goroutines.
type Collector struct {
	reg       *prometheus.Registry
	ns        string
	buckets   []float64
	enqueued  *prometheus.CounterVec
	succeeded *prometheus.CounterVec
	failed    *prometheus.CounterVec
	retried   *prometheus.CounterVec
	dead      *prometheus.CounterVec
	duration  *prometheus.HistogramVec
}

// NewCollector builds a Collector with its own private registry. Use
// Registry to register it with a custom gatherer, or Handler to serve it.
func NewCollector(opts Options) *Collector {
	ns := opts.Namespace
	buckets := opts.DurationBuckets
	if len(buckets) == 0 {
		buckets = DefaultBuckets
	}
	c := &Collector{
		reg:       prometheus.NewRegistry(),
		ns:        ns,
		buckets:   buckets,
		enqueued:  prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "jobs_enqueued_total", Help: "Jobs enqueued since process start."}, []string{"type"}),
		succeeded: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "jobs_succeeded_total", Help: "Jobs that finished without error."}, []string{"type"}),
		failed:    prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "jobs_failed_total", Help: "Failed handler attempts (retriable or not)."}, []string{"type"}),
		retried:   prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "jobs_retried_total", Help: "Failed attempts that will be retried."}, []string{"type"}),
		dead:      prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "jobs_dead_total", Help: "Jobs that exhausted their retries and moved to the DLQ."}, []string{"type"}),
		duration:  prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: ns, Name: "job_duration_seconds", Help: "End-to-end job latency from enqueue to successful completion.", Buckets: buckets}, []string{"type"}),
	}
	c.reg.MustRegister(c.enqueued, c.succeeded, c.failed, c.retried, c.dead, c.duration)
	return c
}

// Hooks returns the goqueue lifecycle callbacks that update the event
// metrics. Wire them into the client at construction time:
//
//	client := goqueue.New(goqueue.WithHooks(col.Hooks()))
//
// The OnSuccess callback observes the end-to-end duration using the job's
// EnqueuedAt timestamp; jobs enqueued before a process restart would skew
// the histogram, which is the accepted trade-off for hook-based
// instrumentation (no core state is needed).
func (c *Collector) Hooks() goqueue.Hooks {
	return goqueue.Hooks{
		OnEnqueue: func(i goqueue.JobInfo) { c.enqueued.WithLabelValues(i.Type).Inc() },
		OnSuccess: func(i goqueue.JobInfo) {
			c.succeeded.WithLabelValues(i.Type).Inc()
			if !i.EnqueuedAt.IsZero() {
				c.duration.WithLabelValues(i.Type).Observe(time.Since(i.EnqueuedAt).Seconds())
			}
		},
		OnFailure: func(i goqueue.JobInfo) { c.failed.WithLabelValues(i.Type).Inc() },
		OnRetry:   func(i goqueue.JobInfo) { c.retried.WithLabelValues(i.Type).Inc() },
		OnDead:    func(i goqueue.JobInfo) { c.dead.WithLabelValues(i.Type).Inc() },
	}
}

// Watch registers pull-based gauges backed by client.Stats(): queue depth
// (pending jobs), in-flight (running) jobs and the current dead-letter set
// size. They are sampled on every scrape, so no polling goroutine is
// needed. Watch may be called at most once per Collector.
func (c *Collector) Watch(client *goqueue.Client) {
	c.reg.MustRegister(
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Namespace: c.ns, Name: "queue_depth", Help: "Jobs currently waiting in the queue."}, func() float64 { return float64(client.Stats().Pending) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Namespace: c.ns, Name: "jobs_running", Help: "Jobs currently being processed by workers."}, func() float64 { return float64(client.Stats().Running) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Namespace: c.ns, Name: "dead_jobs", Help: "Jobs currently in the dead-letter set."}, func() float64 { return float64(client.Stats().Dead) }),
	)
}

// Handler serves this collector's registry in the Prometheus text format.
// Mount it on your own mux: http.Handle("/metrics", col.Handler()).
func (c *Collector) Handler() http.Handler {
	return promhttp.HandlerFor(c.reg, promhttp.HandlerOpts{})
}

// Registry exposes the underlying registry for callers integrating with an
// existing promhttp gatherer or a multi-registry setup.
func (c *Collector) Registry() *prometheus.Registry { return c.reg }
