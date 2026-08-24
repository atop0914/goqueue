package goqueue

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Schedule describes a recurring trigger. Next returns the first trigger
// time strictly after the given instant, or (zero, false) when the schedule
// will never fire again.
type Schedule interface {
	Next(after time.Time) (time.Time, bool)
}

// Every returns an interval-based schedule that fires once per duration,
// anchored to the moment it is evaluated. A non-positive duration never
// fires.
func Every(d time.Duration) Schedule { return everySchedule{d} }

type everySchedule struct{ interval time.Duration }

func (e everySchedule) Next(after time.Time) (time.Time, bool) {
	if e.interval <= 0 {
		return time.Time{}, false
	}
	return after.Add(e.interval), true
}

// Cron parses a cron expression (5 or 6 fields, space-separated) and returns
// the matching Schedule:
//
//	6 fields: second(0-59) minute(0-59) hour(0-23) day-of-month(1-31) month(1-12) weekday(0-6, Sun=0)
//	5 fields: the same with seconds fixed to 0 (minute resolution).
//
// Each field accepts `*`, a single value, a range `a-b`, a step `*/n` or
// `a-b/n`, or a comma-separated list of any of those. Day-of-month and
// weekday are combined with OR semantics (like Vixie cron): a day matches if
// either field matches.
func Cron(expr string) (Schedule, error) {
	fields := strings.Fields(strings.TrimSpace(expr))
	if len(fields) != 5 && len(fields) != 6 {
		return nil, fmt.Errorf("goqueue: cron expects 5 or 6 fields, got %d", len(fields))
	}
	if len(fields) == 5 {
		fields = append([]string{"0"}, fields...)
	}
	sec, err := parseField(fields[0], 0, 59)
	if err != nil {
		return nil, fmt.Errorf("goqueue: cron second: %w", err)
	}
	min, err := parseField(fields[1], 0, 59)
	if err != nil {
		return nil, fmt.Errorf("goqueue: cron minute: %w", err)
	}
	hour, err := parseField(fields[2], 0, 23)
	if err != nil {
		return nil, fmt.Errorf("goqueue: cron hour: %w", err)
	}
	dom, err := parseField(fields[3], 1, 31)
	if err != nil {
		return nil, fmt.Errorf("goqueue: cron day-of-month: %w", err)
	}
	mon, err := parseField(fields[4], 1, 12)
	if err != nil {
		return nil, fmt.Errorf("goqueue: cron month: %w", err)
	}
	dow, err := parseField(fields[5], 0, 6)
	if err != nil {
		return nil, fmt.Errorf("goqueue: cron weekday: %w", err)
	}
	return cronSchedule{sec: sec, min: min, hour: hour, dom: dom, mon: mon, dow: dow}, nil
}

// bitField stores the accepted values of one cron field as a bit set.
type bitField [2]uint64

func (b bitField) has(v int) bool {
	if v < 0 || v > 127 {
		return false
	}
	return b[v/64]&(uint64(1)<<uint(v%64)) != 0
}

func (b *bitField) set(v int) { b[v/64] |= uint64(1) << uint(v%64) }

type cronSchedule struct {
	sec, min, hour, dom, mon, dow bitField
}

// Next returns the first matching time strictly after `after`, scanning
// second-by-second up to a two-year horizon (the standard lookahead used by
// cron implementations). A schedule that cannot match within the horizon
// returns (zero, false).
func (cs cronSchedule) Next(after time.Time) (time.Time, bool) {
	candidate := after.Truncate(time.Second).Add(time.Second)
	horizon := after.AddDate(2, 0, 0)
	for !candidate.After(horizon) {
		if cs.match(candidate) {
			return candidate, true
		}
		candidate = candidate.Add(time.Second)
	}
	return time.Time{}, false
}

func (cs cronSchedule) match(t time.Time) bool {
	// Day-of-month and weekday are OR'd (Vixie cron semantics): a day
	// matches if either the day-of-month or the weekday field matches.
	if !cs.dom.has(t.Day()) && !cs.dow.has(int(t.Weekday())) {
		return false
	}
	return cs.sec.has(t.Second()) &&
		cs.min.has(t.Minute()) &&
		cs.hour.has(t.Hour()) &&
		cs.mon.has(int(t.Month()))
}

// parseField parses one cron field into a bit set.
func parseField(field string, min, max int) (bitField, error) {
	var out bitField
	// '*' means every allowed value.
	if field == "*" {
		for v := min; v <= max; v++ {
			out.set(v)
		}
		return out, nil
	}
	for _, part := range strings.Split(field, ",") {
		if err := parseRange(part, min, max, &out); err != nil {
			return out, err
		}
	}
	return out, nil
}

// parseRange parses a single element: value, range, or step form.
func parseRange(s string, min, max int, out *bitField) error {
	base, step, isStep := strings.Cut(s, "/")
	if isStep {
		n, err := strconv.Atoi(step)
		if err != nil || n <= 0 {
			return fmt.Errorf("invalid step %q", step)
		}
		lo, hi := min, max
		if base != "*" {
			var err error
			lo, hi, err = parseBounds(base, min, max)
			if err != nil {
				return err
			}
		}
		for v := lo; v <= hi; v += n {
			out.set(v)
		}
		return nil
	}
	lo, hi, err := parseBounds(s, min, max)
	if err != nil {
		return err
	}
	for v := lo; v <= hi; v++ {
		out.set(v)
	}
	return nil
}

func parseBounds(s string, min, max int) (int, int, error) {
	if lo, hi, ok := strings.Cut(s, "-"); ok {
		a, err1 := strconv.Atoi(lo)
		b, err2 := strconv.Atoi(hi)
		if err1 != nil || err2 != nil || a < min || b > max || a > b {
			return 0, 0, fmt.Errorf("invalid range %q", s)
		}
		return a, b, nil
	}
	v, err := strconv.Atoi(s)
	if err != nil || v < min || v > max {
		return 0, 0, fmt.Errorf("invalid value %q", s)
	}
	return v, v, nil
}
