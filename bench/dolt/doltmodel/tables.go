package doltmodel

import (
	"cmp"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"slices"
	"time"

	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// The rows below are the tables as the database stores them, in the encodings of
// encoding.go: fingerprints, relations and producers as bytes (held in strings, which
// compare bytewise), times as Unix nanoseconds, sequence numbers with the sign bit
// flipped, and kinds as one character. A row's key columns come first.

// LogRow is a row of the edge log: one record of one edge, stored once per direction. In
// the forward row A is the edge's source and Peer its target; in the reverse row they
// are swapped. Its key is (Layer, Dir, A, Peer, Rel, Producer, EventNs, Seq).
type LogRow struct {
	Layer, Dir             int
	A, Peer, Rel, Producer string
	EventNs, Seq           int64
	Kind                   string
	TTLNs                  int64
	// HasThrough says ThroughNs is set; a row without it stores NULL.
	HasThrough bool
	ThroughNs  int64
	Payload    string
}

// EntLogRow is a row of the entity log. Its key is (Fp, Producer, EventNs, Seq). The
// layer is the entity's type's, kept as a column of the row, not of the key.
type EntLogRow struct {
	Fp, Producer string
	Layer        int
	EventNs, Seq int64
	Kind         string
	TTLNs        int64
	HasThrough   bool
	ThroughNs    int64
	Payload      string
}

// StateRow is a row of the edge state table: the newest record, by (event time, seq), of
// one producer about one edge, in one direction. Its key is (Layer, Dir, A, Peer, Rel,
// Producer). The deadline is when the record stops holding the edge: NULL (Open) for an
// Observe with no TTL, which holds until the producer's next record; for an Observe with
// a TTL the later of its event time and its Through, plus the TTL; and for a Delete its
// event time, because a delete holds nothing from then on.
type StateRow struct {
	Layer, Dir             int
	A, Peer, Rel, Producer string
	EventNs, Seq           int64
	Kind                   string
	// Open says DeadlineNs is NULL.
	Open       bool
	DeadlineNs int64
}

// EntStateRow is a row of the entity state table, keyed by (Fp, Producer).
type EntStateRow struct {
	Fp, Producer string
	Layer        int
	EventNs, Seq int64
	Kind         string
	Open         bool
	DeadlineNs   int64
}

// Meta is the one row that says where the store is: the highest sequence number
// committed (stored as [EncodeSeq] stores them), the retention horizon in nanoseconds
// (zero before any), whether the horizon is past the last representable instant (a
// horizon in nanoseconds cannot say it), and the schema version.
type Meta struct {
	LastSeq       int64
	HorizonNs     int64
	PastEnd       bool
	SchemaVersion int
}

// Tables are all the tables as a value, each in the order of its primary key.
type Tables struct {
	Meta     Meta
	Log      []LogRow
	EntLog   []EntLogRow
	State    []StateRow
	EntState []EntStateRow
}

// deadlineOf is the deadline a state row stores for a record, see [StateRow].
func deadlineOf(kind lifecycle.Kind, eventNs int64, ttl time.Duration, hasThrough bool, throughNs int64) (ns int64, open bool) {
	switch {
	case kind == lifecycle.Delete:
		return eventNs, false
	case ttl == 0:
		return 0, true
	}
	last := eventNs
	if hasThrough && throughNs > last {
		last = throughNs
	}
	return last + int64(ttl), false
}

func (r LogRow) stateRow() StateRow {
	s := StateRow{Layer: r.Layer, Dir: r.Dir, A: r.A, Peer: r.Peer, Rel: r.Rel, Producer: r.Producer, EventNs: r.EventNs, Seq: r.Seq, Kind: r.Kind}
	s.DeadlineNs, s.Open = deadlineOf(kindOf(r.Kind), r.EventNs, time.Duration(r.TTLNs), r.HasThrough, r.ThroughNs)
	return s
}

func (r EntLogRow) stateRow() EntStateRow {
	s := EntStateRow{Fp: r.Fp, Producer: r.Producer, Layer: r.Layer, EventNs: r.EventNs, Seq: r.Seq, Kind: r.Kind}
	s.DeadlineNs, s.Open = deadlineOf(kindOf(r.Kind), r.EventNs, time.Duration(r.TTLNs), r.HasThrough, r.ThroughNs)
	return s
}

func kindOf(k string) lifecycle.Kind {
	if k == KindDelete {
		return lifecycle.Delete
	}
	return lifecycle.Observe
}

// DeriveState computes the state tables a set of log rows implies: for each key, the
// newest row by (event time, seq). It shares nothing with the model's writer, which keeps
// the state as it goes, so a state table kept incrementally is checked against it. Rows
// that retention deleted from the state are not in the log's derivation: a caller
// compares after dropping them ([Tables.WithoutExpired]).
func DeriveState(log []LogRow, ent []EntLogRow) ([]StateRow, []EntStateRow) {
	type edgeKey struct {
		layer, dir             int
		a, peer, rel, producer string
	}
	edges := make(map[edgeKey]LogRow)
	for _, r := range log {
		k := edgeKey{r.Layer, r.Dir, r.A, r.Peer, r.Rel, r.Producer}
		if old, ok := edges[k]; !ok || cmp.Or(cmp.Compare(r.EventNs, old.EventNs), cmp.Compare(r.Seq, old.Seq)) > 0 {
			edges[k] = r
		}
	}
	type entKey struct{ fp, producer string }
	ents := make(map[entKey]EntLogRow)
	for _, r := range ent {
		k := entKey{r.Fp, r.Producer}
		if old, ok := ents[k]; !ok || cmp.Or(cmp.Compare(r.EventNs, old.EventNs), cmp.Compare(r.Seq, old.Seq)) > 0 {
			ents[k] = r
		}
	}
	var state []StateRow
	for _, r := range edges {
		state = append(state, r.stateRow())
	}
	var entState []EntStateRow
	for _, r := range ents {
		entState = append(entState, r.stateRow())
	}
	sortState(state)
	sortEntState(entState)
	return state, entState
}

// WithoutExpired returns the state rows of t that a retention at the horizon, with last
// the highest sequence number it had committed, would have deleted: none of those that
// were committed by then and whose deadline is at or before the horizon. It is for
// comparing a state table with [DeriveState] of the log: a row that was written after the
// retention, even one already at its deadline, stays.
func WithoutExpired(state []StateRow, ent []EntStateRow, horizonNs int64, lastRetainSeq int64) ([]StateRow, []EntStateRow) {
	// Sequence numbers are compared in their stored form, which keeps the order.
	var s []StateRow
	for _, r := range state {
		if r.Open || r.DeadlineNs > horizonNs || r.Seq > lastRetainSeq {
			s = append(s, r)
		}
	}
	var e []EntStateRow
	for _, r := range ent {
		if r.Open || r.DeadlineNs > horizonNs || r.Seq > lastRetainSeq {
			e = append(e, r)
		}
	}
	return s, e
}

func sortLog(rs []LogRow) {
	slices.SortFunc(rs, func(a, b LogRow) int {
		return cmp.Or(
			cmp.Compare(a.Layer, b.Layer), cmp.Compare(a.Dir, b.Dir),
			cmp.Compare(a.A, b.A), cmp.Compare(a.Peer, b.Peer), cmp.Compare(a.Rel, b.Rel), cmp.Compare(a.Producer, b.Producer),
			cmp.Compare(a.EventNs, b.EventNs), cmp.Compare(a.Seq, b.Seq),
		)
	})
}

func sortEntLog(rs []EntLogRow) {
	slices.SortFunc(rs, func(a, b EntLogRow) int {
		return cmp.Or(cmp.Compare(a.Fp, b.Fp), cmp.Compare(a.Producer, b.Producer), cmp.Compare(a.EventNs, b.EventNs), cmp.Compare(a.Seq, b.Seq))
	})
}

func sortState(rs []StateRow) {
	slices.SortFunc(rs, func(a, b StateRow) int {
		return cmp.Or(
			cmp.Compare(a.Layer, b.Layer), cmp.Compare(a.Dir, b.Dir),
			cmp.Compare(a.A, b.A), cmp.Compare(a.Peer, b.Peer), cmp.Compare(a.Rel, b.Rel), cmp.Compare(a.Producer, b.Producer),
		)
	})
}

func sortEntState(rs []EntStateRow) {
	slices.SortFunc(rs, func(a, b EntStateRow) int {
		return cmp.Or(cmp.Compare(a.Fp, b.Fp), cmp.Compare(a.Producer, b.Producer))
	})
}

// Len is the number of rows, the meta row not counted.
func (t Tables) Len() int { return len(t.Log) + len(t.EntLog) + len(t.State) + len(t.EntState) }

// Digest is the SHA-256 of the tables as hex over every column of every row, in key
// order, so two tables with one digest hold the same rows.
func (t Tables) Digest() string {
	h := sha256.New()
	var buf []byte
	u64 := func(v uint64) { buf = binary.BigEndian.AppendUint64(buf, v) }
	i64 := func(v int64) { u64(uint64(v)) }
	str := func(s string) {
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(s)))
		buf = append(buf, s...)
	}
	flag := func(b bool) {
		if b {
			buf = append(buf, 1)
		} else {
			buf = append(buf, 0)
		}
	}
	flush := func() { h.Write(buf); buf = buf[:0] }

	buf = append(buf, 'm')
	i64(t.Meta.LastSeq)
	i64(t.Meta.HorizonNs)
	flag(t.Meta.PastEnd)
	u64(uint64(t.Meta.SchemaVersion))
	flush()
	for _, r := range t.Log {
		buf = append(buf, 'l')
		u64(uint64(r.Layer))
		u64(uint64(r.Dir))
		str(r.A)
		str(r.Peer)
		str(r.Rel)
		str(r.Producer)
		i64(r.EventNs)
		i64(r.Seq)
		str(r.Kind)
		i64(r.TTLNs)
		flag(r.HasThrough)
		i64(r.ThroughNs)
		str(r.Payload)
		flush()
	}
	for _, r := range t.EntLog {
		buf = append(buf, 'e')
		str(r.Fp)
		str(r.Producer)
		u64(uint64(r.Layer))
		i64(r.EventNs)
		i64(r.Seq)
		str(r.Kind)
		i64(r.TTLNs)
		flag(r.HasThrough)
		i64(r.ThroughNs)
		str(r.Payload)
		flush()
	}
	for _, r := range t.State {
		buf = append(buf, 's')
		u64(uint64(r.Layer))
		u64(uint64(r.Dir))
		str(r.A)
		str(r.Peer)
		str(r.Rel)
		str(r.Producer)
		i64(r.EventNs)
		i64(r.Seq)
		str(r.Kind)
		flag(r.Open)
		i64(r.DeadlineNs)
		flush()
	}
	for _, r := range t.EntState {
		buf = append(buf, 't')
		str(r.Fp)
		str(r.Producer)
		u64(uint64(r.Layer))
		i64(r.EventNs)
		i64(r.Seq)
		str(r.Kind)
		flag(r.Open)
		i64(r.DeadlineNs)
		flush()
	}
	return hex.EncodeToString(h.Sum(nil))
}

// FirstDifference describes the first row the tables differ at, or "" if they are equal.
// It is for a failing test's message.
func (t Tables) FirstDifference(o Tables) string {
	if t.Meta != o.Meta {
		return fmt.Sprintf("meta differs: %+v / %+v", t.Meta, o.Meta)
	}
	if d := firstDifference("log", t.Log, o.Log); d != "" {
		return d
	}
	if d := firstDifference("entity log", t.EntLog, o.EntLog); d != "" {
		return d
	}
	if d := firstDifference("state", t.State, o.State); d != "" {
		return d
	}
	return firstDifference("entity state", t.EntState, o.EntState)
}

func firstDifference[R comparable](name string, a, b []R) string {
	for i := range max(len(a), len(b)) {
		switch {
		case i >= len(a):
			return fmt.Sprintf("%s row only in the second: %#v", name, b[i])
		case i >= len(b):
			return fmt.Sprintf("%s row only in the first: %#v", name, a[i])
		case a[i] != b[i]:
			return fmt.Sprintf("%s rows differ: %#v / %#v", name, a[i], b[i])
		}
	}
	return ""
}
