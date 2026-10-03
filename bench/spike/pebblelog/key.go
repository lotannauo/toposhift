package pebblelog

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
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
// baseline, by kind.
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

	// formatTag identifies the layout and its encodings in a database's meta.
	formatTag = "toposhift/bench/log;key=1;record=1;stamp=1"
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
	panic("pebblelog: a prefix of 0xFF bytes has no successor")
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
		return nil, 0, 0, 0, fmt.Errorf("pebblelog: key %x is %d bytes, want %d", key, len(key), keyLen)
	}
	prefix = key[:prefixLen]
	inv := binary.BigEndian.Uint64(key[prefixLen:])
	if ns = int64(invert(inv)); ns < 0 {
		return nil, 0, 0, 0, fmt.Errorf("pebblelog: key %x has a negative event time", key)
	}
	seq = invert(binary.BigEndian.Uint64(key[prefixLen+8:]))
	kind = key[keyLen-1]
	switch kind {
	case kindRecord:
		if seq == 0 {
			return nil, 0, 0, 0, fmt.Errorf("pebblelog: record key %x has no sequence number", key)
		}
	case kindCheckpoint, kindBaseline:
		if seq != 0 {
			return nil, 0, 0, 0, fmt.Errorf("pebblelog: key %x of kind %d has a sequence number", key, kind)
		}
	default:
		return nil, 0, 0, 0, fmt.Errorf("pebblelog: key %x has kind %d", key, kind)
	}
	return prefix, ns, seq, kind, nil
}

func hasPrefix(key, prefix []byte) bool { return bytes.HasPrefix(key, prefix) }

// appendPrefix appends the prefix of one read.
func (e *Engine) appendPrefix(dst []byte, layer catalog.Layer, fp identity.Fingerprint, dir byte) ([]byte, error) {
	dst = append(dst, pebblekv.LayerByte(layer))
	dst, err := e.ids.AppendFingerprint(dst, fp)
	if err != nil {
		return nil, err
	}
	return append(dst, dir), nil
}

// prefixOf is the prefix of a read, or ok false if the entity's type is not one
// the catalog knows: nothing can be stored for it, so a read of it is empty.
func (e *Engine) prefixOf(layer catalog.Layer, fp identity.Fingerprint, dir byte) (prefix []byte, ok bool) {
	prefix, err := e.appendPrefix(make([]byte, 0, prefixLen), layer, fp, dir)
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
func (e *Engine) sidesOf(r engine.Record) ([]side, error) {
	switch r.Subject.Kind {
	case engine.SubjectEntity:
		p, err := e.appendPrefix(make([]byte, 0, prefixLen), r.Layer, r.Subject.A, dirEntity)
		if err != nil {
			return nil, err
		}
		return []side{{p, appendRef(nil, nil, nil, r.Producer)}}, nil
	case engine.SubjectEdge:
		out := make([]side, 0, 2)
		for _, s := range [...]struct {
			owner, peer identity.Fingerprint
			dir         engine.Direction
		}{
			{r.Subject.A, r.Subject.B, engine.Forward},
			{r.Subject.B, r.Subject.A, engine.Reverse},
		} {
			p, err := e.appendPrefix(make([]byte, 0, prefixLen), r.Layer, s.owner, byte(s.dir))
			if err != nil {
				return nil, err
			}
			peer, err := e.ids.AppendFingerprint(nil, s.peer)
			if err != nil {
				return nil, err
			}
			rel, err := e.ids.AppendRelation(nil, r.Subject.Relation)
			if err != nil {
				return nil, err
			}
			out = append(out, side{p, appendRef(nil, peer, rel, r.Producer)})
		}
		return out, nil
	}
	return nil, fmt.Errorf("subject kind %d: %w", r.Subject.Kind, engine.ErrInvalid)
}
