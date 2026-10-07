package store

import (
	"bytes"
	"cmp"
	"slices"

	"github.com/lotannauo/toposhift/internal/identity"
)

// NeighborsEach answers a batched read by asking read once for each
// fingerprint, in order. It is for a store that has no faster form yet: read
// must be bound to one snapshot by the caller, or the batch is not consistent
// under concurrent writes. It returns the first error read gives, and a nil
// result with it.
func NeighborsEach(fps []identity.Fingerprint, read func(identity.Fingerprint) ([]Neighbor, error)) ([][]Neighbor, error) {
	out := make([][]Neighbor, len(fps))
	for i, fp := range fps {
		ns, err := read(fp)
		if err != nil {
			return nil, err
		}
		out[i] = ns
	}
	return out, nil
}

// SortNeighbors orders neighbors by peer fingerprint then relation, the order
// Neighbors promises.
func SortNeighbors(ns []Neighbor) {
	slices.SortFunc(ns, func(a, b Neighbor) int {
		if c := CompareFingerprints(a.Peer, b.Peer); c != 0 {
			return c
		}
		return cmp.Compare(a.Relation, b.Relation)
	})
}

// CompareFingerprints orders fingerprints by type name then hash. It is the
// order of every sorted result.
func CompareFingerprints(a, b identity.Fingerprint) int {
	if c := cmp.Compare(a.Type(), b.Type()); c != 0 {
		return c
	}
	ha, hb := a.Hash(), b.Hash()
	return bytes.Compare(ha[:], hb[:])
}

// SortRecords orders records by (EventTime, Seq), the order Window promises.
func SortRecords(rs []Record) {
	slices.SortFunc(rs, func(a, b Record) int {
		if c := a.EventTime.Compare(b.EventTime); c != 0 {
			return c
		}
		return cmp.Compare(a.Seq, b.Seq)
	})
}
