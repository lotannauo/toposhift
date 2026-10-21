package duckmodel

import (
	"cmp"
	"fmt"
	"slices"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
)

// Text is the form a fingerprint has in a file: its type, a colon and 32 lowercase hex
// digits.
func Text(fp identity.Fingerprint) string { return fp.String() }

// ParseText is the inverse of [Text]. It is strict: another form is an error.
func ParseText(s string) (identity.Fingerprint, error) { return identity.ParseFingerprint(s) }

// CompareText orders two fingerprints as a database orders their text, byte by byte.
// It is the order a file sorted by a fingerprint column is in, and not the contract's:
// see the package documentation.
func CompareText(a, b identity.Fingerprint) int {
	sa, sb := Text(a), Text(b)
	switch {
	case sa < sb:
		return -1
	case sa > sb:
		return 1
	}
	return 0
}

// PeerText is one row of a neighbor read as a statement returns it: the peer as text
// and the relation.
type PeerText struct {
	Peer     string
	Relation string
}

// NeighborsFromText turns the rows of a neighbor read into the answer the contract
// asks for: each peer is parsed and the list is ordered by peer fingerprint, then
// relation, in Go. Whatever order the rows came in is ignored, because a database's
// text order is not the contract's order. A peer that is not a fingerprint is an error.
func NeighborsFromText(rows []PeerText) ([]engine.Neighbor, error) {
	return neighborsFromText(rows, mutNone)
}

func neighborsFromText(rows []PeerText, mut mutant) ([]engine.Neighbor, error) {
	out := make([]engine.Neighbor, 0, len(rows))
	for _, r := range rows {
		fp, err := ParseText(r.Peer)
		if err != nil {
			return nil, fmt.Errorf("duckmodel: a neighbor row: %w", err)
		}
		out = append(out, engine.Neighbor{Peer: fp, Relation: catalog.RelationType(r.Relation)})
	}
	if mut == mutOrderByText {
		slices.SortStableFunc(out, func(a, b engine.Neighbor) int {
			return cmp.Or(CompareText(a.Peer, b.Peer), cmp.Compare(a.Relation, b.Relation))
		})
		return out, nil
	}
	engine.SortNeighbors(out)
	return out, nil
}
