package doltmodel

import (
	"bytes"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/workload"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// SchemaVersion is the version of the tables' layout and encodings. It is stored in the
// meta row, and a change to any encoding in this file is a new version.
const SchemaVersion = 1

// EncodeFingerprint is the VARBINARY a fingerprint is stored as: its type name, a zero
// byte, then its 16 hash bytes. Comparing two encodings bytewise gives
// engine.CompareFingerprints: the type decides first, a type that is a prefix of another
// sorts before it because the zero byte is below every byte a type name holds, and the
// hashes follow. A column of these needs no collation and no sort in the client, which
// the text form (String) would: "k8s:..." sorts after "k8s.pod:..." as text, and before
// it by the contract.
func EncodeFingerprint(fp identity.Fingerprint) []byte {
	typ, hash := string(fp.Type()), fp.Hash()
	b := make([]byte, 0, len(typ)+1+len(hash))
	b = append(b, typ...)
	b = append(b, 0)
	return append(b, hash[:]...)
}

// DecodeFingerprint is the inverse of [EncodeFingerprint].
func DecodeFingerprint(b []byte) (identity.Fingerprint, error) {
	n := len(b) - identity.FingerprintBytes - 1
	if n < 1 || b[n] != 0 {
		return identity.Fingerprint{}, fmt.Errorf("doltmodel: %d bytes are not an encoded fingerprint", len(b))
	}
	return identity.FingerprintFromHash(catalog.EntityType(b[:n]), [identity.FingerprintBytes]byte(b[n+1:]))
}

// CompareEncoded orders two encoded fingerprints the way the database orders the column,
// bytewise, which is the order of engine.CompareFingerprints.
func CompareEncoded(a, b []byte) int { return bytes.Compare(a, b) }

// SortFingerprints orders fingerprints by the contract's order, in the client, as every
// result a database column of them is sorted is to be checked. It never orders by the
// text form.
func SortFingerprints(fps []identity.Fingerprint) { slices.SortFunc(fps, engine.CompareFingerprints) }

// EncodeSeq is the BIGINT an ingest sequence number is stored as: the uint64 with its
// sign bit flipped, read as an int64. It keeps the order of the uint64 (a database
// compares BIGINT as signed) over the whole range, which conformance reaches near 2^63
// and up to 2^64-1, where a plain int64 conversion would put the top half before the
// bottom.
func EncodeSeq(seq uint64) int64 { return int64(seq ^ 1<<63) }

// DecodeSeq is the inverse of [EncodeSeq].
func DecodeSeq(v int64) uint64 { return uint64(v) ^ 1<<63 }

// An instant is stored as Unix nanoseconds in a BIGINT, never as a DATETIME, which
// keeps microseconds and so would merge two instants a nanosecond apart. An engine
// stores nothing outside [0, math.MaxInt64], which is engine.MinEventTime to
// engine.MaxEventTime.

// Nanos converts a stored time: the event time of a valid record, which
// [engine.Record.Validate] has already placed in range. It does not check.
func Nanos(t time.Time) int64 { return t.UnixNano() }

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

// The kind column is one character: it is what a read filters on, and what the state
// path's predicate names.
const (
	// KindObserve is stored for an Observe.
	KindObserve = "o"
	// KindDelete is stored for a Delete.
	KindDelete = "d"
)

// EncodeKind is the stored form of a kind.
func EncodeKind(k lifecycle.Kind) string {
	if k == lifecycle.Delete {
		return KindDelete
	}
	return KindObserve
}

// RegisteredPayloadCapacity is the size in bytes of the payload column the baseline is
// registered with. It is the largest payload over every input the baseline reads, plus the
// padding the plans add to every payload, rounded up to a multiple of 256 bytes
// ([CapacityFor]). The plans' payloads are at most 96 bytes with no padding, which gives 256.
// The Alibaba placement file is the other input: its largest payload is counted by the pass
// that reads it, and the constant is raised before registration if that is larger. A record
// over the capacity is refused with [engine.ErrInvalid] and counted as a capability row
// ([Counts].PayloadRefusals), which must be zero on every registered input.
const RegisteredPayloadCapacity = 256

// CapacityFor is the payload capacity for inputs whose largest payload is maxPayload bytes,
// with pad bytes of padding added to every payload: the sum, rounded up to a multiple of 256.
func CapacityFor(maxPayload, pad int) int { return (maxPayload + pad + 255) / 256 * 256 }

// WorkloadPayload is the largest payload a workload draws, with its padding.
func WorkloadPayload(w workload.Config) int { return w.PayloadMax + w.PayloadPad }
