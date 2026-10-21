package pgmodel

import (
	"cmp"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"slices"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
)

// EdgeRow is a row of the edges table: an interval of one edge subject.
type EdgeRow struct {
	Src, Dst identity.Fingerprint
	Rel      catalog.RelationType
	Layer    catalog.Layer
	Row
}

// EntityRow is a row of the entities table.
type EntityRow struct {
	Fp    identity.Fingerprint
	Layer catalog.Layer
	Row
}

// Tables are the interval tables as a value, in the order of their primary keys:
// edges by (src, dst, relation, start) and entities by (fp, start), with the
// fingerprints in the order of their encodings.
type Tables struct {
	Edges    []EdgeRow
	Entities []EntityRow
}

// Len is the number of rows.
func (t Tables) Len() int { return len(t.Edges) + len(t.Entities) }

// Tables returns a copy of the interval tables.
func (m *Model) Tables() Tables {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var t Tables
	for _, st := range m.subjects {
		t.add(st.id, st.layer, st.rows)
	}
	t.sort()
	return t
}

func (t *Tables) add(s engine.Subject, layer catalog.Layer, rows []Row) {
	for _, r := range rows {
		switch s.Kind {
		case engine.SubjectEdge:
			t.Edges = append(t.Edges, EdgeRow{Src: s.A, Dst: s.B, Rel: s.Relation, Layer: layer, Row: r})
		case engine.SubjectEntity:
			t.Entities = append(t.Entities, EntityRow{Fp: s.A, Layer: layer, Row: r})
		}
	}
}

func (t *Tables) sort() {
	slices.SortFunc(t.Edges, func(a, b EdgeRow) int {
		return cmp.Or(
			engine.CompareFingerprints(a.Src, b.Src),
			engine.CompareFingerprints(a.Dst, b.Dst),
			cmp.Compare(a.Rel, b.Rel),
			cmp.Compare(a.Start, b.Start),
		)
	})
	slices.SortFunc(t.Entities, func(a, b EntityRow) int {
		return cmp.Or(engine.CompareFingerprints(a.Fp, b.Fp), cmp.Compare(a.Start, b.Start))
	})
}

// Bulk builds the interval tables of a set of records in one pass, the way a bulk
// load does: the records of each subject are sorted and folded once with
// [lifecycle.Fold]. It shares nothing with [Model.Write] but the fold, so a Model
// that was written incrementally, with late events and retentions, is checked against
// it. The records must be valid ([engine.Record.Validate]) and in one layer per
// subject; their order does not matter.
func Bulk(records []engine.Record) (Tables, error) {
	bySubject := make(map[engine.Subject][]rec)
	layers := make(map[engine.Subject]catalog.Layer)
	for _, r := range records {
		bySubject[r.Subject] = append(bySubject[r.Subject], keep(r))
		layers[r.Subject] = r.Layer
	}
	var t Tables
	for s, recs := range bySubject {
		slices.SortFunc(recs, rec.cmp)
		rows, err := foldRows(recs)
		if err != nil {
			return Tables{}, err
		}
		t.add(s, layers[s], rows)
	}
	t.sort()
	return t, nil
}

// Clipped returns the tables as a reader at or after the horizon sees them: each
// row starts no earlier than the horizon, and a row that ends at or before it is
// gone. Retention keeps a row that spans the horizon as it was, with its old start,
// and a bulk load of the retained log starts it later; clipped, they are the same
// row. A provisional row becomes a final one with its deadline as its end.
func (t Tables) Clipped(horizon int64) Tables {
	var out Tables
	for _, e := range t.Edges {
		if r, ok := clip(e.Row, horizon); ok {
			e.Row = r
			out.Edges = append(out.Edges, e)
		}
	}
	for _, e := range t.Entities {
		if r, ok := clip(e.Row, horizon); ok {
			e.Row = r
			out.Entities = append(out.Entities, e)
		}
	}
	return out
}

func clip(r Row, horizon int64) (Row, bool) {
	end, open := r.EffectiveEnd()
	start := max(r.Start, horizon)
	if !open && end <= start {
		return Row{}, false
	}
	if open {
		return Row{Start: start, Open: true}, true
	}
	return Row{Start: start, End: end, EndSource: r.EndSource}, true
}

// Digest is the SHA-256 of the tables as hex, over each row's key columns, its
// effective end and its end source, so two tables with one digest hold the same
// intervals however the rows were made.
func (t Tables) Digest() string {
	h := sha256.New()
	var buf []byte
	u64 := func(v uint64) { buf = binary.BigEndian.AppendUint64(buf, v) }
	bytesOf := func(b []byte) {
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(b)))
		buf = append(buf, b...)
	}
	row := func(layer catalog.Layer, r Row) {
		end, open := r.EffectiveEnd()
		u64(uint64(layer))
		u64(uint64(r.Start))
		if open {
			buf = append(buf, 1)
		} else {
			buf = append(buf, 0)
			u64(uint64(end))
		}
		buf = append(buf, byte(r.EndSource))
	}
	for _, e := range t.Edges {
		buf = append(buf[:0], 'g')
		bytesOf(EncodeFingerprint(e.Src))
		bytesOf(EncodeFingerprint(e.Dst))
		bytesOf([]byte(e.Rel))
		row(e.Layer, e.Row)
		h.Write(buf)
	}
	for _, e := range t.Entities {
		buf = append(buf[:0], 'e')
		bytesOf(EncodeFingerprint(e.Fp))
		row(e.Layer, e.Row)
		h.Write(buf)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// Equal reports whether two tables hold the same rows, ignoring how a provisional
// row spells its end.
func (t Tables) Equal(o Tables) bool { return t.Digest() == o.Digest() }

// FirstDifference describes the first row the tables differ at, or "" if they are
// equal. It is for a failing test's message.
func (t Tables) FirstDifference(o Tables) string {
	for i := 0; i < max(len(t.Edges), len(o.Edges)); i++ {
		switch {
		case i >= len(t.Edges):
			return "edge row only in the second: " + describeEdge(o.Edges[i])
		case i >= len(o.Edges):
			return "edge row only in the first: " + describeEdge(t.Edges[i])
		case describeEdge(t.Edges[i]) != describeEdge(o.Edges[i]):
			return "edge rows differ: " + describeEdge(t.Edges[i]) + " / " + describeEdge(o.Edges[i])
		}
	}
	for i := 0; i < max(len(t.Entities), len(o.Entities)); i++ {
		switch {
		case i >= len(t.Entities):
			return "entity row only in the second: " + describeEntity(o.Entities[i])
		case i >= len(o.Entities):
			return "entity row only in the first: " + describeEntity(t.Entities[i])
		case describeEntity(t.Entities[i]) != describeEntity(o.Entities[i]):
			return "entity rows differ: " + describeEntity(t.Entities[i]) + " / " + describeEntity(o.Entities[i])
		}
	}
	return ""
}

func describeEdge(e EdgeRow) string {
	return e.Src.String() + " -" + string(e.Rel) + "-> " + e.Dst.String() + " " + e.Layer.String() + " " + describeRow(e.Row)
}

func describeEntity(e EntityRow) string {
	return e.Fp.String() + " " + e.Layer.String() + " " + describeRow(e.Row)
}

func describeRow(r Row) string {
	end, open := r.EffectiveEnd()
	s := "[" + itoa(r.Start) + ", "
	if open {
		s += "inf)"
	} else {
		s += itoa(end) + ") " + r.EndSource.String()
	}
	return s
}
