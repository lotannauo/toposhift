package pebblelog

import (
	"encoding/binary"
	"fmt"
	"slices"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
)

// FoldVersion stands for the version of the fold logic a checkpoint was built
// with. A reader uses only a checkpoint of its own version; a change to the fold
// that could change an entry bumps it, and every older checkpoint is then skipped
// and rebuilt. (The lifecycle package has no version of its own yet; this is the
// stamp the product's store will take from it.)
const FoldVersion = 1

// stampFormat is the first byte of a baseline's or a checkpoint's value.
const stampFormat = 1

// Entry is one reference a baseline or a checkpoint carries: an Observe that was
// alive at the instant the stamp stands for, with its own event time, TTL and
// Through, so its deadline is known without its record.
type Entry struct {
	// Ref is the reference, in the form of [appendRef].
	Ref     []byte
	EventNs int64
	// Value is the record's value without its payload.
	Value pebblekv.Value
}

// Stamp is the content of a baseline (kind 2) or a checkpoint (kind 1).
//
// For a checkpoint, Through is B, the last sequence number committed when it was
// built, and W is the largest Seq among every record it depends on. For the
// baseline both are L, the last sequence number at the retention that wrote it,
// and Horizon is that retention's horizon, which is its key's instant except
// past the end of the representable range, where the key is at the last instant:
// the retention is a store-authored fact, and this is what stamps it.
type Stamp struct {
	Kind        byte
	FoldVersion uint32
	Through     uint64
	W           uint64
	Horizon     time.Time
	Entries     []Entry
}

// appendStamp appends the encoding of s: format, kind, fold version, B, W, the
// horizon (a length and its binary form, empty for a checkpoint), then the
// entries sorted by reference, each its reference, its event time and its value,
// all length-prefixed.
func appendStamp(dst []byte, s Stamp) ([]byte, error) {
	dst = append(dst, stampFormat, s.Kind)
	dst = binary.AppendUvarint(dst, uint64(s.FoldVersion))
	dst = binary.BigEndian.AppendUint64(dst, s.Through)
	dst = binary.BigEndian.AppendUint64(dst, s.W)
	var h []byte
	if !s.Horizon.IsZero() {
		var err error
		if h, err = s.Horizon.MarshalBinary(); err != nil {
			return nil, err
		}
	}
	dst = binary.AppendUvarint(dst, uint64(len(h)))
	dst = append(dst, h...)
	entries := slices.Clone(s.Entries)
	slices.SortFunc(entries, func(a, b Entry) int { return slices.Compare(a.Ref, b.Ref) })
	dst = binary.AppendUvarint(dst, uint64(len(entries)))
	for _, e := range entries {
		dst = binary.AppendUvarint(dst, uint64(len(e.Ref)))
		dst = append(dst, e.Ref...)
		dst = binary.AppendUvarint(dst, uint64(e.EventNs))
		v := e.Value
		v.Payload = nil
		enc := v.Append(nil)
		dst = binary.AppendUvarint(dst, uint64(len(enc)))
		dst = append(dst, enc...)
	}
	return dst, nil
}

// decodeStamp reads a stamp. The entries' references alias b.
func decodeStamp(b []byte) (Stamp, error) {
	fail := func(why string) (Stamp, error) { return Stamp{}, fmt.Errorf("stamp %s: %w", why, pebblekv.ErrValue) }
	if len(b) < 2 || b[0] != stampFormat {
		return fail("header")
	}
	s := Stamp{Kind: b[1]}
	rest := b[2:]
	uv := func() (uint64, bool) {
		n, w := binary.Uvarint(rest)
		if w <= 0 {
			return 0, false
		}
		rest = rest[w:]
		return n, true
	}
	take := func(n uint64) ([]byte, bool) {
		if n > uint64(len(rest)) {
			return nil, false
		}
		out := rest[:n]
		rest = rest[n:]
		return out, true
	}
	fv, ok := uv()
	if !ok || fv > 1<<32-1 {
		return fail("fold version")
	}
	s.FoldVersion = uint32(fv)
	nums, ok := take(16)
	if !ok {
		return fail("sequence numbers")
	}
	s.Through, s.W = binary.BigEndian.Uint64(nums), binary.BigEndian.Uint64(nums[8:])
	hn, ok := uv()
	if !ok {
		return fail("horizon length")
	}
	hb, ok := take(hn)
	if !ok {
		return fail("horizon")
	}
	if len(hb) > 0 {
		if err := s.Horizon.UnmarshalBinary(hb); err != nil {
			return fail("horizon form")
		}
	}
	count, ok := uv()
	if !ok || count > uint64(len(rest)) {
		return fail("entry count")
	}
	s.Entries = make([]Entry, 0, count)
	for range count {
		rn, ok := uv()
		if !ok {
			return fail("entry reference length")
		}
		ref, ok := take(rn)
		if !ok {
			return fail("entry reference")
		}
		ns, ok := uv()
		if !ok || ns > 1<<63-1 {
			return fail("entry event time")
		}
		vn, ok := uv()
		if !ok {
			return fail("entry value length")
		}
		vb, ok := take(vn)
		if !ok {
			return fail("entry value")
		}
		v, err := pebblekv.DecodeValue(vb)
		if err != nil {
			return Stamp{}, err
		}
		s.Entries = append(s.Entries, Entry{Ref: ref, EventNs: int64(ns), Value: v})
	}
	if len(rest) != 0 {
		return fail("trailing bytes")
	}
	return s, nil
}

// entryOf is the entry a record contributes: the record without its payload.
func entryOf(ref []byte, ns int64, v pebblekv.Value) Entry {
	v.Payload = nil
	return Entry{Ref: slices.Clone(ref), EventNs: ns, Value: v}
}
