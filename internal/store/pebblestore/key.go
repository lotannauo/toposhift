package pebblestore

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/pebblekv"
)

// A key is
//
//	[layer:1][fingerprint:18][dir:1]        the prefix: one entity, one layer, one direction
//	[^ns:8][^seq:8][kind:1]                 the suffix
//
// where dir is 1 for the forward edges of the entity, 2 for the reverse ones, and
// 0 for the existence of the entity itself, and ns is the event time in Unix
// nanoseconds (0 to MaxInt64). Both numbers are stored inverted and big endian,
// so under a bytewise comparer the keys of one prefix run newest event time first
// and, within an instant, highest sequence number first.
//
// kind is 0 for a record, 1 for an ordinary checkpoint and 2 for the retention
// baseline. A checkpoint or a baseline has no sequence number: its seq slot is
// all 0xFF, which is what ^0 is, and 0 is never a valid Seq, so it sorts after
// every record at its instant. At one instant a checkpoint sorts before the
// baseline, by kind. A Seq equal to [store.Latest] would invert to zero, which is
// why no record may carry it.
const (
	prefixLen = 1 + pebblekv.FingerprintKeyLen + 1
	suffixLen = 8 + 8 + 1
	keyLen    = prefixLen + suffixLen

	dirEntity = 0

	kindRecord     = 0
	kindCheckpoint = 1
	kindBaseline   = 2

	// metaLead starts a meta key. Data keys start with a layer, 1 to 4.
	metaLead = 0x00
)

// The meta keys this layout writes, besides the ones pebblekv names.
const (
	// metaCheckpoints says a checkpoint may be in the database: it is the byte 1,
	// written in the commit of the first checkpoint, and never removed. A version of
	// this package that does not invalidate checkpoints refuses a database that has
	// it.
	metaCheckpoints = "checkpoints"
	// metaBootKey is the boot key the database's lifecycle policy was created with.
	metaBootKey = "bootKey"
	// metaPebble is the Pebble version and format major version the database was
	// created with.
	metaPebble = "pebble"
	// metaRetain is the marker of a retention that has begun and not finished: see
	// [retainMarker]. It is absent when no retention is unfinished.
	metaRetain = "retain"
	// metaHorizonLast is the horizon [Store.Horizon] returns: the one of the layer
	// retained most recently, stored the way a layer's horizon is (the instant, then
	// the Seq). Every Retain that moves a layer writes it in the commit of the
	// horizons. It is a hint, trusted only while it agrees with the horizons stored
	// beside it: see [lastHorizon].
	metaHorizonLast = pebblekv.MetaHorizon + "/last"
)

func metaKey(name string) []byte { return append([]byte{metaLead}, name...) }

// invert is the stored form of a time or a sequence number.
func invert(x uint64) uint64 { return ^x }

func appendSuffix(dst []byte, ns int64, seq uint64, kind byte) []byte {
	dst = binary.BigEndian.AppendUint64(dst, invert(uint64(ns)))
	dst = binary.BigEndian.AppendUint64(dst, invert(seq))
	return append(dst, kind)
}

// recordKey is the key of a record in prefix.
func recordKey(prefix []byte, ns int64, seq uint64) []byte {
	return appendSuffix(append([]byte(nil), prefix...), ns, seq, kindRecord)
}

// stampKey is the key of a checkpoint or the baseline at instant ns.
func stampKey(prefix []byte, ns int64, kind byte) []byte {
	return appendSuffix(append([]byte(nil), prefix...), ns, 0, kind)
}

// seekKey is the first key of the prefix that is at or after instant ns in
// time, which is the key to seek to for "event time at or before ns": every
// record at ns itself, whatever its Seq, is at or after it, because the rest of
// the suffix is the smallest it can be. A range delete from here to the end of
// the prefix removes exactly the keys with event time at or before ns.
func seekKey(prefix []byte, ns int64) []byte {
	k := append([]byte(nil), prefix...)
	k = binary.BigEndian.AppendUint64(k, invert(uint64(ns)))
	return append(k, make([]byte, 9)...)
}

// prefixSucc is the first key after every key of a data prefix. A data prefix
// ends in a direction byte, 0 to 2, so the increment never carries.
func prefixSucc(prefix []byte) []byte {
	next := append([]byte(nil), prefix...)
	for i := len(next) - 1; i >= 0; i-- {
		if next[i]++; next[i] != 0 {
			return next
		}
	}
	panic("pebblestore: a prefix of 0xFF bytes has no successor")
}

func prefixBounds(prefix []byte) (lo, hi []byte) {
	return append([]byte(nil), prefix...), prefixSucc(prefix)
}

// dataBounds are the bounds of every data key, all layers: after the meta keys,
// up to the byte after the last layer.
func dataBounds() (lo, hi []byte) {
	return []byte{pebblekv.LayerByte(catalog.L0)}, []byte{pebblekv.LayerByte(catalog.L3) + 1}
}

// parseKey reads a data key. It refuses one whose kind and sequence slot
// disagree: a record always has a Seq, a checkpoint or the baseline never does.
func parseKey(key []byte) (prefix []byte, ns int64, seq uint64, kind byte, err error) {
	if len(key) != keyLen {
		return nil, 0, 0, 0, fmt.Errorf("pebblestore: key %x is %d bytes, want %d", key, len(key), keyLen)
	}
	prefix = key[:prefixLen]
	inv := binary.BigEndian.Uint64(key[prefixLen:])
	if ns = int64(invert(inv)); ns < 0 {
		return nil, 0, 0, 0, fmt.Errorf("pebblestore: key %x has a negative event time", key)
	}
	seq = invert(binary.BigEndian.Uint64(key[prefixLen+8:]))
	kind = key[keyLen-1]
	switch kind {
	case kindRecord:
		if seq == 0 {
			return nil, 0, 0, 0, fmt.Errorf("pebblestore: record key %x has no sequence number", key)
		}
	case kindCheckpoint, kindBaseline:
		if seq != 0 {
			return nil, 0, 0, 0, fmt.Errorf("pebblestore: key %x of kind %d has a sequence number", key, kind)
		}
	default:
		return nil, 0, 0, 0, fmt.Errorf("pebblestore: key %x has kind %d", key, kind)
	}
	return prefix, ns, seq, kind, nil
}

func hasPrefix(key, prefix []byte) bool { return bytes.HasPrefix(key, prefix) }

// appendPrefix appends the prefix of one read.
func (s *Store) appendPrefix(dst []byte, layer catalog.Layer, fp identity.Fingerprint, dir byte) ([]byte, error) {
	dst = append(dst, pebblekv.LayerByte(layer))
	dst, err := s.ids.AppendFingerprint(dst, fp)
	if err != nil {
		return nil, err
	}
	return append(dst, dir), nil
}

// prefixOf is the prefix of a read, or ok false if the entity's type is not one
// the catalog knows: nothing can be stored for it, so a read of it is empty.
func (s *Store) prefixOf(layer catalog.Layer, fp identity.Fingerprint, dir byte) (prefix []byte, ok bool) {
	prefix, err := s.appendPrefix(make([]byte, 0, prefixLen), layer, fp, dir)
	return prefix, err == nil
}

// side is one stored copy of a record: the prefix it lives under and the
// reference (peer, relation, producer) it is filed by within it.
type side struct {
	prefix []byte
	ref    []byte
}

// sidesOf builds the copies a record is stored as: one for an entity, and for an
// edge one under the source's forward prefix and one under the target's reverse
// prefix.
func (s *Store) sidesOf(r store.Record) ([]side, error) {
	switch r.Subject.Kind {
	case store.SubjectEntity:
		p, err := s.appendPrefix(make([]byte, 0, prefixLen), r.Layer, r.Subject.A, dirEntity)
		if err != nil {
			return nil, err
		}
		return []side{{p, appendRef(nil, nil, nil, r.Producer)}}, nil
	case store.SubjectEdge:
		out := make([]side, 0, 2)
		for _, e := range [...]struct {
			owner, peer identity.Fingerprint
			dir         store.Direction
		}{
			{r.Subject.A, r.Subject.B, store.Forward},
			{r.Subject.B, r.Subject.A, store.Reverse},
		} {
			p, err := s.appendPrefix(make([]byte, 0, prefixLen), r.Layer, e.owner, byte(e.dir))
			if err != nil {
				return nil, err
			}
			peer, err := s.ids.AppendFingerprint(nil, e.peer)
			if err != nil {
				return nil, err
			}
			rel, err := s.ids.AppendRelation(nil, r.Subject.Relation)
			if err != nil {
				return nil, err
			}
			out = append(out, side{p, appendRef(nil, peer, rel, r.Producer)})
		}
		return out, nil
	}
	return nil, fmt.Errorf("subject kind %d: %w", r.Subject.Kind, store.ErrInvalid)
}

// The phases of a retention, as the marker records them.
const (
	// phaseRewrite is the rewriting of the prefixes, from the marker's resume key.
	phaseRewrite = 1
	// phaseSettle is the wait after every prefix is rewritten: only the settling, if
	// the store settles, and the removal of the marker remain.
	phaseSettle = 2
)

// retainFormat is the first byte of the marker's value.
const retainFormat = 1

// retainLayer is what the marker says of one layer the retention moves: its new
// horizon and the last sequence number L at the instant the horizon was published.
type retainLayer struct {
	layer   catalog.Layer
	horizon time.Time
	last    uint64
}

// retainMarker is the content of the meta key "retain": a retention that has
// published its horizons and not yet finished its work, written in the same
// synced commit as the horizons it stands for. It is derived: it says nothing
// about the history, only how far this store has got in cleaning it up, and it is
// absent when the store is idle.
//
// The value is
//
//	0x01 | uvarint generation | uvarint layers
//	     | per layer: layer byte, uvarint length and binary form of the horizon, L as 8 bytes big endian
//	     | uvarint length and bytes of the resume key | phase byte
//
// the layers in ascending order. While the phase is [phaseRewrite], resume is the
// first key not yet looked at: every prefix before it has been rewritten, and none
// at or after it has. In [phaseSettle] it is empty.
type retainMarker struct {
	generation uint64
	layers     []retainLayer
	resume     []byte
	phase      byte
}

// check says whether m is one the encoding can carry and a store writes.
func (m retainMarker) check() error {
	if m.generation == 0 {
		return fmt.Errorf("generation 0")
	}
	if len(m.layers) == 0 || len(m.layers) > layers {
		return fmt.Errorf("%d layers", len(m.layers))
	}
	for i, l := range m.layers {
		if _, ok := pebblekv.LayerFromByte(pebblekv.LayerByte(l.layer)); !ok {
			return fmt.Errorf("layer %d is not one", l.layer)
		}
		if i > 0 && l.layer <= m.layers[i-1].layer {
			return fmt.Errorf("layer %d does not follow layer %d", l.layer, m.layers[i-1].layer)
		}
	}
	if m.phase != phaseRewrite && m.phase != phaseSettle {
		return fmt.Errorf("phase %d", m.phase)
	}
	return nil
}

// appendRetainMarker appends the encoding of m.
func appendRetainMarker(dst []byte, m retainMarker) ([]byte, error) {
	if err := m.check(); err != nil {
		return nil, fmt.Errorf("pebblestore: retention marker with %w", err)
	}
	dst = append(dst, retainFormat)
	dst = binary.AppendUvarint(dst, m.generation)
	dst = binary.AppendUvarint(dst, uint64(len(m.layers)))
	for _, l := range m.layers {
		h, err := l.horizon.MarshalBinary()
		if err != nil {
			return nil, fmt.Errorf("pebblestore: retention marker horizon: %w", err)
		}
		dst = append(dst, pebblekv.LayerByte(l.layer))
		dst = binary.AppendUvarint(dst, uint64(len(h)))
		dst = append(dst, h...)
		dst = binary.BigEndian.AppendUint64(dst, l.last)
	}
	dst = binary.AppendUvarint(dst, uint64(len(m.resume)))
	dst = append(dst, m.resume...)
	return append(dst, m.phase), nil
}

// decodeRetainMarker reads a marker written by [appendRetainMarker]. It refuses
// what that never writes: another format byte, a phase that is not one, layers out
// of order, a number that is not in its shortest form, and bytes after the end.
// The resume key aliases b.
func decodeRetainMarker(b []byte) (retainMarker, error) {
	fail := func(why string) (retainMarker, error) {
		return retainMarker{}, fmt.Errorf("retention marker %s: %w", why, pebblekv.ErrValue)
	}
	if len(b) == 0 || b[0] != retainFormat {
		return fail("header")
	}
	rest := b[1:]
	uv := func() (uint64, bool) {
		n, w := binary.Uvarint(rest)
		if w <= 0 || w != uvarintLen(n) {
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
	var m retainMarker
	var ok bool
	if m.generation, ok = uv(); !ok {
		return fail("generation")
	}
	count, ok := uv()
	if !ok || count == 0 || count > layers {
		return fail("layer count")
	}
	for range count {
		lb, ok := take(1)
		if !ok {
			return fail("layer")
		}
		layer, known := pebblekv.LayerFromByte(lb[0])
		if !known {
			return fail("layer")
		}
		hn, ok := uv()
		if !ok {
			return fail("horizon length")
		}
		hb, ok := take(hn)
		if !ok {
			return fail("horizon")
		}
		var h time.Time
		if err := h.UnmarshalBinary(hb); err != nil {
			return fail("horizon form")
		}
		nb, ok := take(8)
		if !ok {
			return fail("sequence number")
		}
		m.layers = append(m.layers, retainLayer{layer: layer, horizon: h, last: binary.BigEndian.Uint64(nb)})
	}
	rn, ok := uv()
	if !ok {
		return fail("resume key length")
	}
	if m.resume, ok = take(rn); !ok {
		return fail("resume key")
	}
	ph, ok := take(1)
	if !ok {
		return fail("phase")
	}
	m.phase = ph[0]
	if len(rest) != 0 {
		return fail("trailing bytes")
	}
	if err := m.check(); err != nil {
		return fail(err.Error())
	}
	return m, nil
}

// uvarintLen is the length of the shortest encoding of n.
func uvarintLen(n uint64) int {
	return len(binary.AppendUvarint(make([]byte, 0, binary.MaxVarintLen64), n))
}
