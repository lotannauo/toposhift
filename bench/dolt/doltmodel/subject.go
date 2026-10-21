package doltmodel

import (
	"slices"
	"sort"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// rec is a record as the log holds it: the columns of a log row, with its times as Unix
// nanoseconds. The subject and its layer are kept once, with the subject, and the
// producer with its log.
type rec struct {
	seq     uint64
	event   int64
	through int64 // meaningful only if hasThrough
	ttl     time.Duration
	payload []byte
	kind    lifecycle.Kind

	hasThrough bool
}

// keep converts a valid record. The payload is copied.
func keep(r engine.Record) rec {
	c := rec{seq: r.Seq, event: Nanos(r.EventTime), ttl: r.TTL, payload: slices.Clone(r.Payload), kind: r.Kind}
	if !r.Through.IsZero() {
		c.through, c.hasThrough = Nanos(r.Through), true
	}
	return c
}

// record is the engine record a log row stands for. The payload is copied, so a caller
// cannot change the log.
func (c rec) record(s engine.Subject, layer catalog.Layer, p lifecycle.Producer) engine.Record {
	r := engine.Record{
		Layer: layer, Subject: s, Producer: p, EventTime: FromNanos(c.event),
		Seq: c.seq, Kind: c.kind, TTL: c.ttl, Payload: slices.Clone(c.payload),
	}
	if c.hasThrough {
		r.Through = FromNanos(c.through)
	}
	return r
}

// cmp orders log rows of one producer by (event time, seq), the order of the log's key.
func (c rec) cmp(o rec) int {
	switch {
	case c.event < o.event:
		return -1
	case c.event > o.event:
		return 1
	case c.seq < o.seq:
		return -1
	case c.seq > o.seq:
		return 1
	}
	return 0
}

// deadline is when the record stops holding the subject, as a state row stores it.
func (c rec) deadline() (ns int64, open bool) {
	return deadlineOf(c.kind, c.event, c.ttl, c.hasThrough, c.through)
}

// stateRow is the part of a record the state table keeps.
type stateRow struct {
	event    int64
	seq      uint64
	kind     lifecycle.Kind
	open     bool
	deadline int64
}

func stateOf(c rec) stateRow {
	s := stateRow{event: c.event, seq: c.seq, kind: c.kind}
	s.deadline, s.open = c.deadline()
	return s
}

// newer reports whether s is the record a state row keeps over o: the later by event
// time, and at one instant by sequence number.
func (s stateRow) newer(o stateRow) bool {
	if s.event != o.event {
		return s.event > o.event
	}
	return s.seq > o.seq
}

// holds reports whether the row's record holds its subject at t: an Observe whose
// deadline is after t, or that has none. A row is read only for an instant at or after
// every event time it could be superseded at, so the next record of its producer is
// never before t.
func (s stateRow) holds(t int64, f *faults) bool {
	if s.kind != lifecycle.Observe {
		return false
	}
	if s.open {
		return true
	}
	if f.deadlineInclusive {
		return s.deadline >= t
	}
	return s.deadline > t
}

// version is a state row as a commit left it: the row, or its absence once retention
// deleted it, from commit ordinal at on.
type version struct {
	at      int
	present bool
	row     stateRow
}

// producer is what the tables hold about one subject from one producer: its log, and
// the history of its state row.
type producer struct {
	name lifecycle.Producer
	// recs is the producer's log of the subject, ascending by (event time, seq).
	recs []rec
	// vers is the state row's history, ascending by commit ordinal. In an arm with no
	// commits it holds the current row alone.
	vers []version

	// The newest record of the batch being written, staged until the batch is
	// complete (staging is the writer's in-memory map of each key's newest record).
	stagedOp  uint64
	staged    stateRow
	hasStaged bool
}

// subject is what the model keeps for one subject: its layer and its producers.
type subject struct {
	id    engine.Subject
	layer catalog.Layer
	prods []*producer
}

func (st *subject) producer(name lifecycle.Producer) *producer {
	for _, p := range st.prods {
		if p.name == name {
			return p
		}
	}
	p := &producer{name: name}
	st.prods = append(st.prods, p)
	return p
}

// mirror is how many rows of the log and of the state a record or a row of this subject
// stands for: an edge is stored once per direction, an entity once.
func (st *subject) mirror() uint64 {
	if st.id.Kind == engine.SubjectEdge {
		return 2
	}
	return 1
}

// insert places a record in the log in key order. Records mostly arrive in order.
func (p *producer) insert(c rec) {
	if n := len(p.recs); n == 0 || p.recs[n-1].cmp(c) < 0 {
		p.recs = append(p.recs, c)
		return
	}
	i := sort.Search(len(p.recs), func(i int) bool { return p.recs[i].cmp(c) > 0 })
	p.recs = slices.Insert(p.recs, i, c)
}

// head is the current state row, if there is one.
func (p *producer) head() (stateRow, bool) {
	if n := len(p.vers); n > 0 && p.vers[n-1].present {
		return p.vers[n-1].row, true
	}
	return stateRow{}, false
}

// stateAt is the state row as commit ordinal ord left it, if there is one: the last
// version at or before it.
func (p *producer) stateAt(ord int) (stateRow, bool) {
	i := sort.Search(len(p.vers), func(i int) bool { return p.vers[i].at > ord })
	if i == 0 || !p.vers[i-1].present {
		return stateRow{}, false
	}
	return p.vers[i-1].row, true
}

// newestAt is the producer's newest record by (event time, seq) among those with an event
// time at or before t and a sequence number at or below token: the row a ROW_NUMBER over
// the producer's log, newest first, numbers 1.
func (p *producer) newestAt(t int64, token uint64, f *faults) (rec, bool) {
	hi := sort.Search(len(p.recs), func(i int) bool { return p.recs[i].event > t })
	j := hi - 1
	for ; j >= 0; j-- {
		if p.recs[j].seq <= token || f.dropSeqFilter {
			break
		}
	}
	if j < 0 {
		return rec{}, false
	}
	if f.tieByEvent {
		// Ordering by event time alone leaves the winner at one instant to chance: the
		// first of them.
		for j > 0 && p.recs[j-1].event == p.recs[j].event && (p.recs[j-1].seq <= token || f.dropSeqFilter) {
			j--
		}
	}
	return p.recs[j], true
}
