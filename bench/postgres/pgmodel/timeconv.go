package pgmodel

import (
	"math"
	"strconv"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
)

// An instant is stored as the Unix nanoseconds an int8range holds. An engine stores
// nothing outside [0, math.MaxInt64], which is engine.MinEventTime to
// engine.MaxEventTime.

// ToNanos converts a stored time: the event time of a valid record, which
// [engine.Record.Validate] has already placed in range. It does not check.
func ToNanos(t time.Time) int64 { return t.UnixNano() }

// FromNanos is the time of a stored value, in UTC.
func FromNanos(ns int64) time.Time { return time.Unix(0, ns).UTC() }

// ClampInstant converts the instant of a read. The engine contract allows any time:
// before engine.MinEventTime nothing exists, which is ok false (no query is needed);
// after engine.MaxEventTime the answer is the state after every record, which is
// math.MaxInt64. The comparison is made on the time.Time, because converting an
// instant far outside the range with UnixNano overflows.
func ClampInstant(t time.Time) (ns int64, ok bool) {
	switch {
	case t.Before(engine.MinEventTime):
		return 0, false
	case t.After(engine.MaxEventTime):
		return math.MaxInt64, true
	}
	return t.UnixNano(), true
}

// Bounds is the half-open range [Lo, Hi) a window reads, in nanoseconds. Unbounded
// says Hi is past the representable range, so every stored record from Lo on is in
// it; Empty says no record can be.
type Bounds struct {
	Lo, Hi    int64
	Unbounded bool
	Empty     bool
}

// ClampWindow converts a window's bounds. A from before the range reads from the
// start of it; a from after the range, or an interval with to <= from, is empty; a
// to after the range is unbounded, so that a record at engine.MaxEventTime, which is
// before to, is in it.
func ClampWindow(from, to time.Time) Bounds {
	if !from.Before(to) || from.After(engine.MaxEventTime) || !to.After(engine.MinEventTime) {
		return Bounds{Empty: true}
	}
	b := Bounds{}
	if from.After(engine.MinEventTime) {
		b.Lo = from.UnixNano()
	}
	if to.After(engine.MaxEventTime) {
		b.Hi, b.Unbounded = math.MaxInt64, true
	} else {
		b.Hi = to.UnixNano()
	}
	return b
}

// Contains reports whether an event time in nanoseconds is in the window.
func (b Bounds) Contains(ns int64) bool {
	if b.Empty || ns < b.Lo {
		return false
	}
	return b.Unbounded || ns < b.Hi
}

func itoa(v int64) string { return strconv.FormatInt(v, 10) }
