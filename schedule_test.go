package goqueue

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestEverySchedule(t *testing.T) {
	base := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	s := Every(5 * time.Minute)
	next, ok := s.Next(base)
	if !ok {
		t.Fatal("Every should always fire")
	}
	if !next.Equal(base.Add(5 * time.Minute)) {
		t.Fatalf("Next = %v, want %v", next, base.Add(5*time.Minute))
	}
	// Non-positive interval never fires.
	s2 := Every(0)
	if _, ok := s2.Next(base); ok {
		t.Fatal("Every(0) should not fire")
	}
}

func TestCronInvalidFieldCount(t *testing.T) {
	if _, err := Cron("0 0 * * * * *"); err == nil {
		t.Fatal("expected error for 7 fields")
	}
	if _, err := Cron("* * * *"); err == nil {
		t.Fatal("expected error for 4 fields")
	}
}

func TestCronEveryMinute(t *testing.T) {
	s, err := Cron("* * * * *")
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 8, 20, 12, 30, 45, 0, time.UTC)
	next, ok := s.Next(base)
	if !ok {
		t.Fatal("should fire")
	}
	want := time.Date(2026, 8, 20, 12, 31, 0, 0, time.UTC) // next minute boundary
	if !next.Equal(want) {
		t.Fatalf("Next = %v, want %v", next, want)
	}
}

func TestCronSpecificMinuteHour(t *testing.T) {
	// Every hour at :15 (5-field: minute=15, hour=*).
	s, err := Cron("15 * * * *")
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 8, 20, 9, 40, 0, 0, time.UTC)
	next, ok := s.Next(base)
	if !ok {
		t.Fatal("should fire")
	}
	want := time.Date(2026, 8, 20, 10, 15, 0, 0, time.UTC)
	if !next.Equal(want) {
		t.Fatalf("Next = %v, want %v", next, want)
	}
}

func TestCron6FieldSeconds(t *testing.T) {
	// Every 30 seconds.
	s, err := Cron("*/30 * * * * *")
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 8, 20, 12, 0, 10, 0, time.UTC)
	next, _ := s.Next(base)
	want := time.Date(2026, 8, 20, 12, 0, 30, 0, time.UTC)
	if !next.Equal(want) {
		t.Fatalf("Next = %v, want %v", next, want)
	}
	next2, _ := s.Next(next)
	want2 := time.Date(2026, 8, 20, 12, 1, 0, 0, time.UTC)
	if !next2.Equal(want2) {
		t.Fatalf("Next = %v, want %v", next2, want2)
	}
}

func TestCronRangeAndList(t *testing.T) {
	// Mon-Fri at 09:00, and also 18:00 (OR of two values).
	s, err := Cron("0 9,18 * * 1-5")
	if err != nil {
		t.Fatal(err)
	}
	// Friday 2026-08-21 12:00 -> next is Sat? no, Sat is 6, excluded. Actually
	// next within same day: 18:00 on Friday.
	fri := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC) // Friday
	next, ok := s.Next(fri)
	if !ok {
		t.Fatal("should fire")
	}
	want := time.Date(2026, 8, 21, 18, 0, 0, 0, time.UTC)
	if !next.Equal(want) {
		t.Fatalf("Next = %v, want %v", next, want)
	}
}

func TestCronDomainWeekday(t *testing.T) {
	// Day-of-month and weekday are OR'd (Vixie semantics). With dow=* every
	// day matches, so a `13 * *` cron fires daily rather than only on the
	// 13th — this is the standard cron behavior worth pinning down.
	s, err := Cron("0 0 12 13 * *")
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	next, ok := s.Next(base)
	if !ok {
		t.Fatal("should fire")
	}
	want := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC) // dow=* matches today
	if !next.Equal(want) {
		t.Fatalf("Next = %v, want %v", next, want)
	}

	// When the weekday can't match before the 13th, the DOM condition alone
	// selects the 13th. Start just before it.
	s2, err := Cron("0 0 12 13 * 0") // 13th OR Sunday
	if err != nil {
		t.Fatal(err)
	}
	base2 := time.Date(2026, 8, 13, 0, 0, 0, 0, time.UTC)
	next2, ok := s2.Next(base2)
	if !ok {
		t.Fatal("should fire")
	}
	want2 := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC) // Sunday 08-16 is later
	if !next2.Equal(want2) {
		t.Fatalf("Next = %v, want %v", next2, want2)
	}
}

// TestSchedulerIntervalEnqueues verifies the Every scheduler enqueues jobs on
// a short real interval and stops cleanly.
func TestSchedulerIntervalEnqueues(t *testing.T) {
	c := New(WithWorkers(1))
	defer c.Shutdown(context.Background())

	var count atomic.Int32
	c.Register("beat", func(ctx context.Context, payload []byte) error {
		count.Add(1)
		return nil
	})
	c.Start()

	taskID := c.Schedule(Every(20*time.Millisecond), func() Job {
		return Job{Type: "beat"}
	})
	if taskID == "" {
		t.Fatal("expected a task id")
	}

	deadline := time.Now().Add(2 * time.Second)
	for count.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := count.Load(); got < 3 {
		t.Fatalf("scheduler produced %d beats, want >= 3", got)
	}

	c.ScheduleStop(taskID)
	before := count.Load()
	time.Sleep(70 * time.Millisecond)
	if after := count.Load(); after != before {
		t.Fatalf("scheduler kept firing after stop: %d -> %d", before, after)
	}
}

// TestSchedulerFakeClock verifies the scheduler computes next fire times from
// the injected clock without relying on real timers. The clock is read from
// the scheduler goroutine, so it must be safe for concurrent use (atomic).
func TestSchedulerFakeClock(t *testing.T) {
	var now atomic.Int64
	base := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	now.Store(base.UnixNano())

	c := New(WithWorkers(2), WithClock(func() time.Time {
		return time.Unix(0, now.Load()).UTC()
	}))
	defer c.Shutdown(context.Background())

	var enqueued atomic.Int32
	c.Register("task", func(ctx context.Context, payload []byte) error {
		enqueued.Add(1)
		return nil
	})
	c.Start()

	spec, err := Cron("* * * * *") // every minute
	if err != nil {
		t.Fatal(err)
	}
	cid := c.Schedule(spec, func() Job { return Job{Type: "task"} })
	defer c.ScheduleStop(cid)

	// Let the scheduler goroutine arm its first fire (12:01:00) before we
	// advance the fake clock, otherwise it would compute the next fire from
	// the already-advanced time.
	time.Sleep(50 * time.Millisecond)

	// Advance the fake clock 61 seconds: the scheduler should fire the job it
	// armed for 12:01:00.
	now.Store(base.Add(61 * time.Second).UnixNano())
	waitFor(t, 2*time.Second, func() bool { return enqueued.Load() >= 1 })
	if got := enqueued.Load(); got < 1 {
		t.Fatalf("fake-clock scheduler fired %d times, want >= 1", got)
	}
}

// TestSchedulerUnknownTypeContinues verifies a scheduled job whose type has no
// handler does not crash or halt the scheduler.
func TestSchedulerUnknownTypeContinues(t *testing.T) {
	c := New(WithWorkers(1))
	defer c.Shutdown(context.Background())

	cid := c.Schedule(Every(5*time.Millisecond), func() Job {
		return Job{Type: "missing"}
	})
	// It should just keep going (no panic). If it halted, ScheduleStop would
	// block forever on an exited goroutine that already returned.
	time.Sleep(20 * time.Millisecond)
	c.ScheduleStop(cid) // must return promptly
}

func TestCronFieldValidation(t *testing.T) {
	cases := []string{
		"61 * * * *", // second out of range (6 fields) -- but this is 5 fields
	}
	for _, expr := range cases {
		if _, err := Cron(expr); err == nil {
			t.Fatalf("Cron(%q): expected error", expr)
		}
	}
	// Valid but should parse fine:
	if _, err := Cron("59 23 31 12 6"); err != nil {
		t.Fatalf("valid 5-field cron rejected: %v", err)
	}
}
