package redis

import (
	"context"
	"fmt"
	"testing"

	"github.com/alicebob/miniredis/v2"

	goqueue "github.com/atop0914/goqueue"
)

// Baseline throughput for the Redis backend, measured against the same
// operations as the memory/SQLite benchmarks so the three backends can be
// compared head-to-head (see README performance table). miniredis is an
// in-process server — no network stack involved — so absolute numbers here
// are a Lua-script cost estimate, not wire-latency; against a real Redis the
// per-op cost is dominated by the loopback round-trip instead. Run with:
//
//	go test -bench=. -benchmem -run='^$' ./store/redis
func openBenchStore(b *testing.B) *Store {
	b.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(mr.Close)
	st, err := Open(mr.Addr())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = st.CloseClient() })
	return st
}

func BenchmarkRedisEnqueue(b *testing.B) {
	st := openBenchStore(b)
	ctx := context.Background()
	job := goqueue.Job{Type: "email", Payload: []byte("baseline payload")}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := st.Enqueue(ctx, job); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRedisDequeueAck(b *testing.B) {
	st := openBenchStore(b)
	ctx := context.Background()
	job := goqueue.Job{Type: "email", Payload: []byte("baseline payload")}
	// Pre-load everything so the loop only measures Dequeue+Ack.
	for i := 0; i < b.N; i++ {
		if _, err := st.Enqueue(ctx, job); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dj, err := st.Dequeue(ctx)
		if err != nil {
			b.Fatal(err)
		}
		if err := st.Ack(ctx, dj.ID); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRedisEnqueueParallel(b *testing.B) {
	st := openBenchStore(b)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for i := 0; pb.Next(); i++ {
			if _, err := st.Enqueue(ctx, goqueue.Job{
				Type:    "email",
				Payload: []byte(fmt.Sprintf("p-%d", i)),
			}); err != nil {
				b.Error(err)
				return
			}
		}
	})
}

func BenchmarkRedisFullCycleParallel(b *testing.B) {
	st := openBenchStore(b)
	ctx := context.Background()
	// Keep the store primed so parallel claimers never starve on empty
	// queue polls; leftover jobs at the end are fine — this is throughput,
	// not a correctness test.
	primed := make(chan struct{})
	go func() {
		for i := 0; i < 4096; i++ {
			if _, err := st.Enqueue(ctx, goqueue.Job{Type: "t"}); err != nil {
				break
			}
		}
		close(primed)
	}()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			dj, err := st.Dequeue(ctx)
			if err != nil {
				b.Error(err)
				return
			}
			if err := st.Ack(ctx, dj.ID); err != nil {
				b.Error(err)
				return
			}
		}
	})
	b.StopTimer()
	<-primed
}
