package xtmodel

import (
	"math"
	"strconv"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
)

// Instant is a point on either time axis, in Unix nanoseconds, held unsigned so that
// two values fit above every instant a record can have: [Forever], the end of time,
// and the instant just after the last representable one ([PastEnd]), at which a read
// sees the state after every record. An engine stores nothing before the epoch or
// after math.MaxInt64 nanoseconds (engine.MinEventTime to engine.MaxEventTime).
type Instant uint64

const (
	// Forever is the end of time: the valid-time upper bound of a document that is
	// still present, and the upper bound of a delete that has no next assertion to
	// stop at. It is above every event time and every deadline, so a segment that
	// ends at the last representable instant is not mistaken for one that never
	// ends.
	Forever Instant = math.MaxUint64
	// PastEnd is the instant of a read after engine.MaxEventTime: later than the end
	// of every closed segment, earlier than [Forever].
	PastEnd Instant = Instant(math.MaxInt64) + 1
)

// instantOf converts a stored time: the event time of a valid record, which
// [engine.Record.Validate] has already placed in range. It does not check.
func instantOf(t time.Time) Instant { return Instant(t.UnixNano()) }

func (i Instant) String() string {
	switch i {
	case Forever:
		return "forever"
	case PastEnd:
		return "past-end"
	}
	return strconv.FormatUint(uint64(i), 10)
}

// ClampInstant converts the instant of a read. The engine contract allows any time:
// before engine.MinEventTime nothing exists, which is ok false (no query is needed);
// after engine.MaxEventTime the answer is the state after every record, which is
// [PastEnd]. The comparison is made on the time.Time, because converting an instant
// far outside the range with UnixNano overflows.
func ClampInstant(t time.Time) (at Instant, ok bool) {
	switch {
	case t.Before(engine.MinEventTime):
		return 0, false
	case t.After(engine.MaxEventTime):
		return PastEnd, true
	}
	return instantOf(t), true
}

// Bounds is the half-open range [Lo, Hi) of event times a window reads, in
// nanoseconds. Unbounded says Hi is past the representable range, so every stored
// record from Lo on is in it; Empty says no record can be.
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

// The system time axis. A batch is one transaction, and its system time is a
// function of its ordinal alone: the n-th committed batch (n from 1) is at
// [BaseSystemTime] plus n microseconds. The sequence number the batch ends at
// cannot be the clock, because the engine contract allows sequence numbers near
// 2^63, which overflow a timestamp; an ordinal cannot, and it is strictly increasing,
// which a transaction's system time must be.
//
// Unverified against XTDB 2.1: that a transaction may be given a system time earlier
// than the server's clock. The server adapter tests it first; if the server refuses,
// the base becomes the server's clock at the first batch, recorded, with the same
// offsets.

// BaseSystemTime is the system time of the zeroth batch: 2000-01-01T00:00:00Z.
var BaseSystemTime = instantOf(time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC))

// SystemTime is the system time of the n-th committed batch, n >= 1.
func SystemTime(n uint64) Instant { return BaseSystemTime + Instant(n)*Instant(time.Microsecond) }

// wholeMicroseconds reports whether a finite boundary is on a whole microsecond,
// the resolution XTDB's temporal columns are taken to have (unverified; the server
// adapter tests it). [Forever] is a boundary no timestamp has to hold.
func wholeMicroseconds(i Instant) bool {
	return i == Forever || i%Instant(time.Microsecond) == 0
}
