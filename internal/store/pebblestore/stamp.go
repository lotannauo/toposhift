package pebblestore

import (
	"encoding/binary"
	"fmt"
	"slices"
	"time"

	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store/pebblekv"
)

// foldVersion stands for the version of the fold logic a checkpoint was built
// with. A reader uses only a checkpoint of its own version; a change to the fold
// that could change an entry bumps it, and every older checkpoint is then skipped
// and rebuilt.
const foldVersion = 1

// stampFormat is the first byte of a baseline's or a checkpoint's value, for a
// stamp with no boot state; stampFormatBoots is the first byte of a baseline that
// carries one (see [bootState]). A reader takes both, and refuses any other.
const (
	stampFormat      = 1
	stampFormatBoots = 2
)

// entry is one reference a baseline or a checkpoint carries: an Observe that was
// alive at the instant the stamp stands for, with its own event time, TTL and
// Through, so its deadline is known without its record.
type entry struct {
	// ref is the reference, in the form of [appendRef].
	ref     []byte
	eventNs int64
	// value is the record's value without its payload.
	value pebblekv.Value
}

// stamp is the content of a baseline (kind 2) or a checkpoint (kind 1).
//
// For a checkpoint, through is B, the last sequence number committed when it was
// built, and w is the largest Seq among every record it depends on. For the
// baseline both are L, the last sequence number at the retention that wrote it,
// and horizon is that retention's horizon, which is its key's instant except
// past the end of the representable range, where the key is at the last instant:
// the retention is a store-authored fact, and this is what stamps it.
type stamp struct {
	kind        byte
	foldVersion uint32
	through     uint64
	w           uint64
	horizon     time.Time
	entries     []entry
	// boots is the boot fold's state at the horizon, for the baseline of an entity
	// prefix with boots under a boot key, and nil otherwise. It makes the stamp
	// one of format 2.
	boots *bootState
}

// bootState is the boot fold's state at a baseline's horizon: what a fold of the
// entity's whole history needs of the records the baseline stands for, which the
// entries, holding only the references alive at the horizon, do not keep.
//
// boots and collision are a [lifecycle.BootTracker]'s, once it has taken every
// sighting before the horizon of the records discarded so far, in order. carried
// are the sightings at or after the horizon of those records, which are the last
// instants of runs that began before it, in the order of
// [lifecycle.CompareSightings] and without repeats. A read seeds a tracker with
// boots and collision and folds carried together with the sightings of the
// records that stay: the fold of the whole history, from the horizon on.
type bootState struct {
	boots     []lifecycle.Boot
	collision *lifecycle.CloneCollisionError
	carried   []lifecycle.Sighting
}

// appendStamp appends the encoding of s: format, kind, fold version, B, W, the
// horizon (a length and its binary form, empty for a checkpoint), then the
// entries sorted by reference, each its reference, its event time and its value,
// all length-prefixed. An entry keeps the boot and the event-time basis of its
// value, and drops its payload.
//
// A stamp with a boot state is format 2: format 1's encoding with the leading byte
// 2, followed by the state (see appendBootState). Only a baseline with at least
// one boot has one.
func appendStamp(dst []byte, s stamp) ([]byte, error) {
	format := byte(stampFormat)
	if s.boots != nil {
		if s.kind != kindBaseline || len(s.boots.boots) == 0 {
			return nil, fmt.Errorf("pebblestore: a boot state on a stamp of kind %d with %d boots", s.kind, len(s.boots.boots))
		}
		format = stampFormatBoots
	}
	dst = append(dst, format, s.kind)
	dst = binary.AppendUvarint(dst, uint64(s.foldVersion))
	dst = binary.BigEndian.AppendUint64(dst, s.through)
	dst = binary.BigEndian.AppendUint64(dst, s.w)
	var h []byte
	if !s.horizon.IsZero() {
		var err error
		if h, err = s.horizon.MarshalBinary(); err != nil {
			return nil, err
		}
	}
	dst = binary.AppendUvarint(dst, uint64(len(h)))
	dst = append(dst, h...)
	entries := slices.Clone(s.entries)
	slices.SortFunc(entries, func(a, b entry) int { return slices.Compare(a.ref, b.ref) })
	dst = binary.AppendUvarint(dst, uint64(len(entries)))
	for _, e := range entries {
		dst = binary.AppendUvarint(dst, uint64(len(e.ref)))
		dst = append(dst, e.ref...)
		dst = binary.AppendUvarint(dst, uint64(e.eventNs))
		v := e.value
		v.Payload = nil
		enc := v.Append(nil)
		dst = binary.AppendUvarint(dst, uint64(len(enc)))
		dst = append(dst, enc...)
	}
	if s.boots != nil {
		return appendBootState(dst, s.boots)
	}
	return dst, nil
}

// appendBootState appends the boot state: the boots in order of first
// appearance, a count and then each its ID's length and the ID, the instant it
// was first seen and the one it was last seen; then a byte that is 1 if there is a
// collision and 0 if not, and the collision, the stale boot's length and ID, the
// instant it was observed, the newer boot's length and ID, and the instant the
// newer one first appeared; then the carried sightings, a count and each its
// instant, its ID's length and the ID, sorted and without repeats. Every number is
// a uvarint, and every instant in Unix nanoseconds.
func appendBootState(dst []byte, b *bootState) ([]byte, error) {
	instant := func(t time.Time) (uint64, error) {
		ns, where := pebblekv.Locate(t)
		if where != pebblekv.Inside {
			return 0, fmt.Errorf("pebblestore: a boot instant %s outside the representable range", t)
		}
		return uint64(ns), nil
	}
	str := func(dst []byte, s string) []byte {
		dst = binary.AppendUvarint(dst, uint64(len(s)))
		return append(dst, s...)
	}
	dst = binary.AppendUvarint(dst, uint64(len(b.boots)))
	for _, bt := range b.boots {
		first, err := instant(bt.FirstSeen)
		if err != nil {
			return nil, err
		}
		last, err := instant(bt.LastSeen)
		if err != nil {
			return nil, err
		}
		dst = str(dst, bt.ID)
		dst = binary.AppendUvarint(dst, first)
		dst = binary.AppendUvarint(dst, last)
	}
	if c := b.collision; c != nil {
		observed, err := instant(c.ObservedAt)
		if err != nil {
			return nil, err
		}
		newer, err := instant(c.NewerFirstSeen)
		if err != nil {
			return nil, err
		}
		dst = append(dst, 1)
		dst = str(dst, c.StaleBoot)
		dst = binary.AppendUvarint(dst, observed)
		dst = str(dst, c.NewerBoot)
		dst = binary.AppendUvarint(dst, newer)
	} else {
		dst = append(dst, 0)
	}
	carried := slices.Clone(b.carried)
	slices.SortFunc(carried, lifecycle.CompareSightings)
	carried = slices.CompactFunc(carried, func(x, y lifecycle.Sighting) bool { return lifecycle.CompareSightings(x, y) == 0 })
	dst = binary.AppendUvarint(dst, uint64(len(carried)))
	for _, sg := range carried {
		at, err := instant(sg.At)
		if err != nil {
			return nil, err
		}
		dst = binary.AppendUvarint(dst, at)
		dst = str(dst, sg.ID)
	}
	return dst, nil
}

// decodeStamp reads a stamp of format 1 or 2. The entries' references alias b.
// A stamp of format 2 must be a baseline with at least one boot, whose boot state
// is in the form appendBootState writes, with every boot instant before the
// horizon and every carried one at or after it; anything else is refused.
func decodeStamp(b []byte) (stamp, error) {
	fail := func(why string) (stamp, error) { return stamp{}, fmt.Errorf("stamp %s: %w", why, pebblekv.ErrValue) }
	if len(b) < 2 || (b[0] != stampFormat && b[0] != stampFormatBoots) {
		return fail("header")
	}
	s := stamp{kind: b[1]}
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
	s.foldVersion = uint32(fv)
	nums, ok := take(16)
	if !ok {
		return fail("sequence numbers")
	}
	s.through, s.w = binary.BigEndian.Uint64(nums), binary.BigEndian.Uint64(nums[8:])
	hn, ok := uv()
	if !ok {
		return fail("horizon length")
	}
	hb, ok := take(hn)
	if !ok {
		return fail("horizon")
	}
	if len(hb) > 0 {
		if err := s.horizon.UnmarshalBinary(hb); err != nil {
			return fail("horizon form")
		}
	}
	count, ok := uv()
	if !ok || count > uint64(len(rest)) {
		return fail("entry count")
	}
	s.entries = make([]entry, 0, count)
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
			return stamp{}, err
		}
		s.entries = append(s.entries, entry{ref: ref, eventNs: int64(ns), value: v})
	}
	if b[0] == stampFormatBoots {
		if s.kind != kindBaseline {
			return fail("of format 2 that is not a baseline")
		}
		var why string
		s.boots, rest, why = decodeBootState(rest, s.horizon)
		if why != "" {
			return fail(why)
		}
	}
	if len(rest) != 0 {
		return fail("trailing bytes")
	}
	return s, nil
}

// decodeBootState reads the boot state appendBootState wrote, of a baseline at
// horizon, and returns what follows it, or why it cannot. The IDs are copies.
func decodeBootState(b []byte, horizon time.Time) (_ *bootState, rest []byte, why string) {
	rest = b
	uv := func() (uint64, bool) {
		n, w := binary.Uvarint(rest)
		if w <= 0 {
			return 0, false
		}
		rest = rest[w:]
		return n, true
	}
	instant := func() (time.Time, bool) {
		n, ok := uv()
		if !ok || n > 1<<63-1 {
			return time.Time{}, false
		}
		return pebblekv.Time(int64(n)), true
	}
	str := func() (string, bool) {
		n, ok := uv()
		if !ok || n == 0 || n > uint64(len(rest)) {
			return "", false
		}
		s := string(rest[:n])
		rest = rest[n:]
		return s, true
	}
	st := &bootState{}
	count, ok := uv()
	if !ok || count == 0 || count > uint64(len(rest)) {
		return nil, nil, "boot count"
	}
	st.boots = make([]lifecycle.Boot, 0, count)
	for range count {
		var bt lifecycle.Boot
		var ok1, ok2, ok3 bool
		bt.ID, ok1 = str()
		bt.FirstSeen, ok2 = instant()
		bt.LastSeen, ok3 = instant()
		if !ok1 || !ok2 || !ok3 {
			return nil, nil, "boot"
		}
		if !bt.LastSeen.Before(horizon) {
			return nil, nil, "boot seen at or after the horizon"
		}
		st.boots = append(st.boots, bt)
	}
	if len(rest) == 0 {
		return nil, nil, "collision flag"
	}
	flag := rest[0]
	rest = rest[1:]
	switch flag {
	case 0:
	case 1:
		var c lifecycle.CloneCollisionError
		var ok1, ok2, ok3, ok4 bool
		c.StaleBoot, ok1 = str()
		c.ObservedAt, ok2 = instant()
		c.NewerBoot, ok3 = str()
		c.NewerFirstSeen, ok4 = instant()
		if !ok1 || !ok2 || !ok3 || !ok4 {
			return nil, nil, "collision"
		}
		if !c.ObservedAt.Before(horizon) {
			return nil, nil, "collision at or after the horizon"
		}
		st.collision = &c
	default:
		return nil, nil, "collision flag"
	}
	// The boots and the collision are checked as a tracker checks a seed.
	if err := lifecycle.NewBootTracker(lifecycle.Policy{}).Seed(st.boots, st.collision); err != nil {
		return nil, nil, "boot state: " + err.Error()
	}
	count, ok = uv()
	if !ok || count > uint64(len(rest)) {
		return nil, nil, "carried count"
	}
	if count > 0 {
		st.carried = make([]lifecycle.Sighting, 0, count)
	}
	for range count {
		var sg lifecycle.Sighting
		var ok1, ok2 bool
		sg.At, ok1 = instant()
		sg.ID, ok2 = str()
		if !ok1 || !ok2 {
			return nil, nil, "carried sighting"
		}
		if sg.At.Before(horizon) {
			return nil, nil, "carried sighting before the horizon"
		}
		if n := len(st.carried); n > 0 && lifecycle.CompareSightings(st.carried[n-1], sg) >= 0 {
			return nil, nil, "carried sightings out of order"
		}
		st.carried = append(st.carried, sg)
	}
	return st, rest, ""
}

// entryOf is the entry a record contributes: the record without its payload.
func entryOf(ref []byte, ns int64, v pebblekv.Value) entry {
	v.Payload = nil
	return entry{ref: slices.Clone(ref), eventNs: ns, value: v}
}
