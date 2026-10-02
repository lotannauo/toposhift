package pebblekv

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// Where says how an instant lies against the range a store can represent.
type Where int8

const (
	// Before is an instant earlier than [engine.MinEventTime].
	Before Where = -1
	// Inside is an instant a store can hold.
	Inside Where = 0
	// After is an instant later than [engine.MaxEventTime].
	After Where = 1
)

// Locate converts an instant to Unix nanoseconds, or says it lies outside the
// representable range, with the nearest end as the number: 0 before it and
// MaxInt64 after it. Calling UnixNano on such an instant is undefined, so every
// read goes through this.
func Locate(t time.Time) (ns int64, where Where) {
	switch {
	case t.Before(engine.MinEventTime):
		return 0, Before
	case t.After(engine.MaxEventTime):
		return math.MaxInt64, After
	}
	return t.UnixNano(), Inside
}

// Time is the instant a number of Unix nanoseconds stands for, in UTC.
func Time(ns int64) time.Time { return time.Unix(0, ns).UTC() }

// ValueFormat is the version byte that starts every value. A change to the
// layout of a value needs a new one, the golden vectors, and a migration.
const ValueFormat = 1

// ErrValue marks bytes that are not a value this package wrote.
var ErrValue = errors.New("malformed value")

// Value is what a layout stores for one version of one subject, apart from what
// its key already says (the layer, the subject, the producer and the event
// time).
//
// The encoding is one format byte, one flags byte (the kind in the low two bits,
// then a bit saying a Through follows), the sequence number, the TTL and the
// Through (if flagged) as unsigned varints, and the payload as the rest. An
// explicit flag, not a zero sentinel, says whether there is a Through: a run
// whose Through equals its event time is not a single observation.
type Value struct {
	Seq        uint64
	Kind       lifecycle.Kind
	TTL        time.Duration
	HasThrough bool
	// Through is Unix nanoseconds, meaningful only if HasThrough.
	Through int64
	Payload []byte
}

const (
	flagKindMask   = 0b011
	flagHasThrough = 0b100
)

// FromRecord is the value of a record.
func FromRecord(r engine.Record) Value {
	v := Value{Seq: r.Seq, Kind: r.Kind, TTL: r.TTL, Payload: r.Payload}
	if !r.Through.IsZero() {
		v.HasThrough, v.Through = true, r.Through.UnixNano()
	}
	return v
}

// Append appends the encoding of v to dst. The kind must be Observe or Delete,
// and the TTL and Through must not be negative: a record that passed
// [engine.Record.Validate] satisfies all three.
func (v Value) Append(dst []byte) []byte {
	flags := byte(v.Kind)
	if v.HasThrough {
		flags |= flagHasThrough
	}
	dst = append(dst, ValueFormat, flags)
	dst = binary.AppendUvarint(dst, v.Seq)
	dst = binary.AppendUvarint(dst, uint64(v.TTL))
	if v.HasThrough {
		dst = binary.AppendUvarint(dst, uint64(v.Through))
	}
	return append(dst, v.Payload...)
}

// DecodeValue reads a value written by [Value.Append]. The payload aliases b.
func DecodeValue(b []byte) (Value, error) {
	fail := func(why string) (Value, error) { return Value{}, fmt.Errorf("%s: %w", why, ErrValue) }
	if len(b) < 2 {
		return fail("shorter than its header")
	}
	if b[0] != ValueFormat {
		return fail(fmt.Sprintf("format %d, want %d", b[0], ValueFormat))
	}
	flags := b[1]
	var v Value
	v.Kind = lifecycle.Kind(flags & flagKindMask)
	if v.Kind != lifecycle.Observe && v.Kind != lifecycle.Delete {
		return fail(fmt.Sprintf("kind %d", v.Kind))
	}
	if flags&^(flagKindMask|flagHasThrough) != 0 {
		return fail(fmt.Sprintf("unknown flag bits %#b", flags))
	}
	v.HasThrough = flags&flagHasThrough != 0
	rest := b[2:]
	next := func() (uint64, bool) {
		n, w := binary.Uvarint(rest)
		if w <= 0 {
			return 0, false
		}
		rest = rest[w:]
		return n, true
	}
	var ok bool
	if v.Seq, ok = next(); !ok {
		return fail("sequence number")
	}
	ttl, ok := next()
	if !ok || ttl > math.MaxInt64 {
		return fail("TTL")
	}
	v.TTL = time.Duration(ttl)
	if v.HasThrough {
		through, ok := next()
		if !ok || through > math.MaxInt64 {
			return fail("through")
		}
		v.Through = int64(through)
	}
	v.Payload = rest
	return v, nil
}

// Holds reports whether an Observe made at event time eventNs is still the
// producer's live reference at t, assuming t is not before it and the producer
// has asserted nothing since: no TTL means it holds until replaced, and a TTL
// means until the last observation plus the TTL, the deadline. A Delete holds
// nothing. This is the lifecycle specification's rule for one producer's
// segment ([lifecycle.Fold]), and the property test checks it against the fold.
//
// An instant after the representable range is passed as [math.MaxInt64], which
// no deadline exceeds (they are validated to fit), so nothing with a deadline
// holds there.
func (v Value) Holds(eventNs, t int64) bool {
	if v.Kind != lifecycle.Observe {
		return false
	}
	if v.TTL == 0 {
		return true
	}
	last := eventNs
	if v.HasThrough && v.Through > last {
		last = v.Through
	}
	if last > math.MaxInt64-int64(v.TTL) { // a corrupt value; Validate keeps this out
		return true
	}
	return t < last+int64(v.TTL)
}
