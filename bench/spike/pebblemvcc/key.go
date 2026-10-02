package pebblemvcc

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/cockroachdb/pebble/v2/cockroachkvs"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// The roach key of one stored subject (the part cockroachkvs calls the key, as
// opposed to its version) is
//
//	[layer:1][fingerprint:18][dir:1]                      the prefix of one read
//	[peer fingerprint:18][relation:2]                     edges only
//	[producer: the rest]
//
// where dir is the direction an edge is read in (1 forward, 2 reverse), and 0
// for the existence of the entity itself, which has no peer and no relation.
// Everything a read of one entity in one layer and one direction touches is
// therefore under one prefix. The producer needs no length: it is last, and
// cockroachkvs compares the whole roach key bytewise.
//
// The version of a key is (wall, logical): wall is the event time in Unix
// nanoseconds plus one, and logical is an ordinal that numbers the versions
// written at one instant, from 1. Neither is the sequence number, which is in
// the value; see [Engine] for why.
const (
	prefixLen  = 1 + pebblekv.FingerprintKeyLen + 1
	edgeKeyLen = prefixLen + pebblekv.FingerprintKeyLen + 2

	dirEntity = 0

	// metaLead starts a meta key. Data keys start with a layer, 1 to 4, so meta
	// keys sort before all data and no data prefix bound can include one.
	metaLead = 0x00

	// formatTag identifies the layout and its encodings in a database's meta.
	formatTag = "toposhift/bench/mvcc;key=1;value=1"
)

// wallOf is the wall time of an event time. The +1 keeps wall 0 unused:
// cockroachkvs encodes wall 0 with logical 0 as a bare key with no version,
// which sorts before every versioned key instead of after, so an event at the
// Unix epoch would sort in the wrong place.
func wallOf(ns int64) uint64 { return uint64(ns) + 1 }

// nanosOf is the inverse of wallOf.
func nanosOf(wall uint64) int64 { return int64(wall - 1) }

// metaKey is the engine key of a meta key. It has no version: a bare key.
func metaKey(name string) []byte {
	return cockroachkvs.EncodeKey(nil, append([]byte{metaLead}, name...), nil)
}

// versionKey is the engine key of one version of a roach key. logical 0 is a
// valid encoding but sorts after every other logical at the same wall, so it is
// only ever used as the key just past the versions of an instant.
func versionKey(roach []byte, wall uint64, logical uint32) []byte {
	return cockroachkvs.EncodeMVCCKey(nil, roach, wall, logical)
}

// atOrBefore is the engine key to seek to for the newest version of roach whose
// wall is at or before the given one, whatever its ordinal. The logical part
// must be the largest possible: versions sort newest first, so a seek with a
// smaller logical part skips the versions at that very instant whose ordinal is
// above it. Using 0 here is the bug the mutation sweep plants.
func atOrBefore(roach []byte, wall uint64) []byte {
	return versionKey(roach, wall, math.MaxUint32)
}

// endOfVersions is the first engine key after every version of roach: the bare
// key of the roach key one byte longer, which sorts after anything with a
// version. Range-delete and iterator bounds must be valid engine keys, so they
// are built here rather than by hand.
func endOfVersions(roach []byte) []byte {
	return cockroachkvs.EncodeKey(nil, append(append([]byte(nil), roach...), 0), nil)
}

// prefixBounds are the iterator bounds of a data prefix: the bare key of the
// prefix itself, and the bare key of the next prefix. Both are valid engine keys.
func prefixBounds(prefix []byte) (lo, hi []byte) {
	next := append([]byte(nil), prefix...)
	for i := len(next) - 1; i >= 0; i-- {
		if next[i]++; next[i] != 0 {
			return cockroachkvs.EncodeKey(nil, prefix, nil), cockroachkvs.EncodeKey(nil, next, nil)
		}
	}
	panic("pebblemvcc: a prefix of 0xFF bytes has no successor; data prefixes start with a layer")
}

// dataBounds are the bounds of every data key, all layers: after the meta keys,
// up to the byte after the last layer.
func dataBounds() (lo, hi []byte) {
	return cockroachkvs.EncodeKey(nil, []byte{pebblekv.LayerByte(catalog.L0)}, nil),
		cockroachkvs.EncodeKey(nil, []byte{pebblekv.LayerByte(catalog.L3) + 1}, nil)
}

// split reads an engine key of a data version.
func split(key []byte) (roach []byte, wall uint64, logical uint32, err error) {
	roach, ver, ok := cockroachkvs.DecodeEngineKey(key)
	if !ok {
		return nil, 0, 0, fmt.Errorf("pebblemvcc: %x is not an engine key", key)
	}
	switch len(ver) {
	case 8:
		return roach, binary.BigEndian.Uint64(ver), 0, nil
	case 12:
		return roach, binary.BigEndian.Uint64(ver), binary.BigEndian.Uint32(ver[8:]), nil
	}
	return nil, 0, 0, fmt.Errorf("pebblemvcc: key %x has a %d-byte version, want 8 or 12", key, len(ver))
}

// appendPrefix appends the prefix of one read.
func (e *Engine) appendPrefix(dst []byte, layer catalog.Layer, fp identity.Fingerprint, dir byte) ([]byte, error) {
	dst = append(dst, pebblekv.LayerByte(layer))
	dst, err := e.ids.AppendFingerprint(dst, fp)
	if err != nil {
		return nil, err
	}
	return append(dst, dir), nil
}

// roachKeys builds the roach keys a record is stored under: one for an entity,
// and for an edge one under the source's forward prefix and one under the
// target's reverse prefix.
func (e *Engine) roachKeys(r engine.Record) ([][]byte, error) {
	switch r.Subject.Kind {
	case engine.SubjectEntity:
		k, err := e.appendPrefix(nil, r.Layer, r.Subject.A, dirEntity)
		if err != nil {
			return nil, err
		}
		return [][]byte{append(k, r.Producer...)}, nil
	case engine.SubjectEdge:
		out := make([][]byte, 0, 2)
		for _, side := range [...]struct {
			owner, peer identity.Fingerprint
			dir         engine.Direction
		}{
			{r.Subject.A, r.Subject.B, engine.Forward},
			{r.Subject.B, r.Subject.A, engine.Reverse},
		} {
			k, err := e.appendPrefix(make([]byte, 0, edgeKeyLen+len(r.Producer)), r.Layer, side.owner, byte(side.dir))
			if err != nil {
				return nil, err
			}
			if k, err = e.ids.AppendFingerprint(k, side.peer); err != nil {
				return nil, err
			}
			if k, err = e.ids.AppendRelation(k, r.Subject.Relation); err != nil {
				return nil, err
			}
			out = append(out, append(k, r.Producer...))
		}
		return out, nil
	}
	return nil, fmt.Errorf("subject kind %d: %w", r.Subject.Kind, engine.ErrInvalid)
}

// edgeOf reads the peer, the relation and the producer out of the roach key of
// an edge, as seen from the prefix's own entity.
func (e *Engine) edgeOf(roach []byte) (peer identity.Fingerprint, rel catalog.RelationType, producer lifecycle.Producer, err error) {
	if len(roach) < edgeKeyLen {
		return identity.Fingerprint{}, "", "", fmt.Errorf("pebblemvcc: roach key %x is too short for an edge", roach)
	}
	if peer, err = e.ids.Fingerprint(roach[prefixLen:]); err != nil {
		return identity.Fingerprint{}, "", "", err
	}
	rel, ok := e.ids.Relation(binary.BigEndian.Uint16(roach[prefixLen+pebblekv.FingerprintKeyLen:]))
	if !ok {
		return identity.Fingerprint{}, "", "", fmt.Errorf("pebblemvcc: roach key %x has an unknown relation", roach)
	}
	return peer, rel, lifecycle.Producer(roach[edgeKeyLen:]), nil
}
