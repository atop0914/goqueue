package sqlite

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	goqueue "github.com/atop0914/goqueue"
)

// Baseline throughput for the durable backend, measured against the same
// operations as the memory benchmarks in the root module so the two
// backends can be compared head-to-head (see README performance table).
// File-mode databases under -race are roughly an order of magnitude slower
// than the numbers CI reports without it; run with:
//
//	go test -bench=. -benchmem -run=^$ ./store/sqlite
func BenchmarkSqliteEnqueue(b *testing.B) {
	st := openTestStoreAt(b, filepath.Join(b.TempDir(), "bench.db"))
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

func BenchmarkSqliteDequeueAck(b *testing.B) {
	st := openTestStoreAt(b, filepath.Join(b.TempDir(), "bench.db"))
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

func BenchmarkSqliteEnqueueParallel(b *testing.B) {
	st := openTestStoreAt(b, filepath.Join(b.TempDir(), "bench.db"))
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

func BenchmarkSqliteFullCycleParallel(b *testing.B) {
	st := openTestStoreAt(b, filepath.Join(b.TempDir(), "bench.db"))
	ctx := context.Background()
	// Keep the store primed so parallel claimers never starve on empty
	// queue polls; leftover rows at the end are fine — this is throughput,
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
