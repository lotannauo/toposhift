package xtengine

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/xtdb/xtmodel"
)

// The `_id` layout is XT-plain: the `_id` of every document is a string (a bigint for
// a meta document) that names the document and nothing else, and a read finds a
// subject's documents by the columns it filters on. XT-iid, the layout that gives one
// subject's documents a common prefix of a UUID `_id` so that an `_id` range finds them,
// is not used, because XTDB 2.1.0 does not prune by an `_id` range: see the package
// documentation.

// segID is the `_id` of the segment document of one (subject, producer): the
// fingerprints of the subject, its relation and the producer, joined by '|'. A
// fingerprint ("type:hex") and a relation never contain '|', and the producer, which
// may, is last, so two different documents never have one `_id`. An entity has no
// second fingerprint and no relation, and so two empty fields. The edge's two
// directions are two tables, so a document of seg_f and one of seg_r share their `_id`.
func segID(k xtmodel.Key) string {
	s := k.Subject
	var b strings.Builder
	b.WriteString(s.A.String())
	b.WriteByte('|')
	if s.Kind == engine.SubjectEdge {
		b.WriteString(s.B.String())
		b.WriteByte('|')
		b.WriteString(string(s.Relation))
	} else {
		b.WriteByte('|')
	}
	b.WriteByte('|')
	b.WriteString(string(k.Producer))
	return b.String()
}

// logID is the `_id` of a log document: the record's sequence number, in decimal
// padded to the 20 digits of the largest, so that the ids sort as the sequence numbers
// do. A sequence number is unique, and a log table holds one document per record, so
// the sequence number alone names it.
func logID(seq uint64) string { return fmt.Sprintf("%020d", seq) }

// metaID is the `_id` of a meta document: the ordinal of the batch, a bigint.
func metaID(n uint64) int64 { return int64(n) }

// A sequence number is stored as a bigint with its top bit flipped: the ingest sequence
// is a uint64, and the conformance workloads cross 2^63, which a bigint cannot hold. The
// flip maps the order of uint64 to the order of int64, so a comparison with a stored
// number (seq <= token) is a comparison of bigints, and the largest token, which sees
// every record, is the largest bigint. A sequence number read with a SQL client is
// therefore shifted by 2^63.

func seqToDB(seq uint64) int64 { return int64(seq ^ (1 << 63)) }
func seqFromDB(v int64) uint64 { return uint64(v) ^ (1 << 63) }

// prefixName is what a table prefix must look like: it becomes part of an identifier
// in SQL text, which cannot be a parameter.
var prefixName = regexp.MustCompile(`^[a-z][a-z0-9_]{0,30}$`)

// tableNames are the names of the tables of one engine, in the order of
// [xtmodel.Table].
type tableNames [xtmodel.NumTables]string

// namesFor returns the table names under a prefix ("" for the names of the guide).
func namesFor(prefix string) (tableNames, error) {
	if prefix != "" && !prefixName.MatchString(prefix) {
		return tableNames{}, fmt.Errorf("xtengine: %q is not a usable table prefix: %w", prefix, engine.ErrInvalid)
	}
	var n tableNames
	for t := range n {
		n[t] = prefix + xtmodel.Table(t).String()
	}
	return n, nil
}

func (n tableNames) of(t xtmodel.Table) string { return n[t] }
