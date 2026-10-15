package wire

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/query/blastradius"
	"github.com/lotannauo/toposhift/internal/query/coresidency"
	"github.com/lotannauo/toposhift/internal/query/extract"
	"github.com/lotannauo/toposhift/internal/query/graphdiff"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/query/subgraph"
	"github.com/lotannauo/toposhift/internal/store"
)

// The format's version. A decoder refuses any other major version. A minor
// version only adds optional sections, so a decoder of this minor reads a
// document of a later one and skips what it does not know.
const (
	FormatMajor = 1
	FormatMinor = 0
)

// Errors a decoder or an encoder returns. Each is matched with errors.Is.
var (
	// ErrFormat is bytes that are not a document of this format, or are not its
	// canonical encoding: truncated, a count that does not fit, a varint that is
	// not the shortest, an order that is not the order of the format.
	ErrFormat = errors.New("wire: malformed document")
	// ErrVersion is a document of a major version this decoder does not read.
	ErrVersion = errors.New("wire: unsupported format version")
	// ErrReserved is a section or an operation the format reserves and version 1
	// never writes: the rebase section of live mode, and the attr operation.
	ErrReserved = errors.New("wire: reserved by the format")
)

// Document is what the format carries: a temporal subgraph, the query it
// answers echoed back, and the policy it was derived under. It is derived: the
// header says so once.
type Document struct {
	Query    Query
	Policy   Policy
	Subgraph subgraph.Subgraph
}

// QueryKind says which query a document answers. It is a table of the wire's
// own, with frozen values.
type QueryKind uint8

const (
	// QueryScope is a scope subgraph: the scope's entities and the edges one hop
	// out.
	QueryScope QueryKind = 1
	// QueryCoResidents is a co-residency search.
	QueryCoResidents QueryKind = 2
	// QueryExtraction is a two-query-paths extraction.
	QueryExtraction QueryKind = 3
	// QueryBlastRadius is a blast-radius walk.
	QueryBlastRadius QueryKind = 4
)

// String names the kind, and "QueryKind(n)" outside the table.
func (k QueryKind) String() string {
	switch k {
	case QueryScope:
		return "scope"
	case QueryCoResidents:
		return "co_residents"
	case QueryExtraction:
		return "extraction"
	case QueryBlastRadius:
		return "blast_radius"
	}
	return fmt.Sprintf("QueryKind(%d)", uint8(k))
}

// Valid reports whether k is a value of the table.
func (k QueryKind) Valid() bool { return k >= QueryScope && k <= QueryBlastRadius }

// Query is the echo of the request a document answers: what the cache key is
// hashed from, apart from the requested AsOf, which the resolved token in the
// subgraph replaces.
type Query struct {
	Kind QueryKind
	// Seeds are the entities the query started from, in the order the query gave
	// them. They are carried whole, not as dictionary references: the header is
	// readable before the dictionary is.
	Seeds []identity.Fingerprint
	// Options are the query's options and policy values (a depth, a cap, a
	// minimum overlap in nanoseconds, a flag as 0 or 1), ascending by name, each
	// name once.
	Options []Option
	// AsOf is the token the request named, or [store.Latest]. The token the
	// answer was read at is the subgraph's.
	AsOf uint64
}

// Option is one integer option of a query.
type Option struct {
	Name  string
	Value int64
}

// Policy is the policy vector: the versions of every derivation the document
// depends on, so that a change to any of them is a different document.
type Policy struct {
	Interval, CoResidency, Extract, BlastRadius, GraphDiff int
	// CatalogIDs is a digest of the catalog's id set: every entity and relation
	// with its number. See [CatalogDigest].
	CatalogIDs [sha256.Size]byte
	// IdentityEncoding is identity.EncodingVersion.
	IdentityEncoding int
	// Rank is a digest of the store's producer rank, set when names or
	// descriptions are carried and empty otherwise, as they are not in version
	// 1. Zero or [sha256.Size] bytes.
	Rank []byte
}

// CurrentPolicy is the policy vector of this build. The subgraph's own policy
// version is the subgraph's: it is in the document as the builder gave it.
func CurrentPolicy() Policy {
	return Policy{
		Interval: interval.PolicyVersion, CoResidency: coresidency.PolicyVersion, Extract: extract.PolicyVersion,
		BlastRadius: blastradius.PolicyVersion, GraphDiff: graphdiff.PolicyVersion,
		CatalogIDs:       CatalogDigest(catalog.Default()),
		IdentityEncoding: identity.EncodingVersion,
	}
}

// CatalogDigest is the SHA-256 of a catalog's id set: for each entity type in
// ascending id, the byte 'e', the id as a uvarint, the name's length as a uvarint
// and the name; then the same for each relation with 'r'. Adding a type under a
// new number changes it, and so does renumbering one.
func CatalogDigest(c *catalog.Catalog) [sha256.Size]byte {
	h := sha256.New()
	var tmp []byte
	put := func(tag byte, id uint64, name string) {
		tmp = append(tmp[:0], tag)
		tmp = binary.AppendUvarint(tmp, id)
		tmp = binary.AppendUvarint(tmp, uint64(len(name)))
		tmp = append(tmp, name...)
		h.Write(tmp)
	}
	var es []catalog.Entity
	for e := range c.Entities() {
		es = append(es, e)
	}
	slices.SortFunc(es, func(a, b catalog.Entity) int { return int(a.ID()) - int(b.ID()) })
	for _, e := range es {
		put('e', uint64(e.ID()), string(e.Type()))
	}
	var rs []catalog.Relation
	for r := range c.Relations() {
		rs = append(rs, r)
	}
	slices.SortFunc(rs, func(a, b catalog.Relation) int { return int(a.ID()) - int(b.ID()) })
	for _, r := range rs {
		put('r', uint64(r.ID()), string(r.Type()))
	}
	var out [sha256.Size]byte
	h.Sum(out[:0])
	return out
}

// The tags of the sections. A tag below 0x80 names a required section: a
// decoder that does not know it refuses the document. A tag of 0x80 or above
// names an optional one, which a decoder that does not know it skips. Version 1
// writes the required sections 1 to 12, once each, in this order.
const (
	tagHeader       = 0x01
	tagDictionary   = 0x02
	tagElements     = 0x03
	tagKeyframes    = 0x04
	tagDeltas       = 0x05
	tagQuarantine   = 0x06
	tagTruncated    = 0x07
	tagUnattributed = 0x08
	tagUnmapped     = 0x09
	tagSuppressed   = 0x0a
	tagPods         = 0x0b
	tagAnchor       = 0x0c
	lastRequiredTag = tagAnchor
	// tagRebase is reserved for live mode's "rebase from event time t" message.
	// Version 1 never writes it and a decoder refuses it.
	tagRebase = 0x20
	// optionalBit marks an optional section's tag.
	optionalBit = 0x80
)

// The operations of a delta, frozen. A decoder refuses opAttr: descriptions
// inside the window are not in version 1.
const (
	opAppear    = 1
	opDisappear = 2
	opAttr      = 3
)

// magic opens every document.
var magic = [4]byte{'T', 'S', 'G', 'F'}

func invalid(format string, args ...any) error {
	return fmt.Errorf("wire: "+format+": %w", append(args, store.ErrInvalid)...)
}
