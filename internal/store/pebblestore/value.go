package pebblestore

import (
	"encoding/binary"
	"fmt"

	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store/pebblekv"
)

// A reference is what a producer holds about a subject, as seen from the entity
// whose prefix the record is under: for an edge the peer's fingerprint key (18
// bytes), the relation (2 bytes) and then the producer's name to the end; for an
// entity's own existence just the producer's name. The direction in the prefix
// says which. A reference is also the identity within a prefix for "the newest
// record of a reference decides".
const (
	peerLen = pebblekv.FingerprintKeyLen
	relLen  = 2
	// subjectLen is the length of the peer and relation, the part of an edge's
	// reference that names the edge, apart from who holds it.
	subjectLen = peerLen + relLen
)

func appendRef(dst, peer, rel []byte, producer lifecycle.Producer) []byte {
	dst = append(dst, peer...)
	dst = append(dst, rel...)
	return append(dst, producer...)
}

// A record's value is the reference with a length in front, then the codec of
// [pebblekv.Value]. (The Seq is in the key as well as in the value; every reader
// that decodes a record checks they agree.)
func appendRecordValue(dst, ref []byte, v pebblekv.Value) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(ref)))
	dst = append(dst, ref...)
	return v.Append(dst)
}

// decodeRecordValue reads a record's value. The slices alias b.
func decodeRecordValue(dir byte, b []byte) (ref []byte, v pebblekv.Value, err error) {
	n, w := binary.Uvarint(b)
	if w <= 0 || n > uint64(len(b)-w) {
		return nil, pebblekv.Value{}, fmt.Errorf("record reference: %w", pebblekv.ErrValue)
	}
	ref = b[w : w+int(n)]
	if dir != dirEntity && len(ref) <= subjectLen {
		return nil, pebblekv.Value{}, fmt.Errorf("an edge reference of %d bytes: %w", len(ref), pebblekv.ErrValue)
	}
	v, err = pebblekv.DecodeValue(b[w+int(n):])
	return ref, v, err
}
