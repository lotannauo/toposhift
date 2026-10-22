package xtmodel

import (
	"fmt"
	"slices"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// Table is one of the XTDB tables the baseline writes.
type Table uint8

const (
	// SegF and SegR hold the valid-time segments of edges, one document per edge,
	// producer and direction: SegF is read from the edge's source (forward), SegR
	// from its target (reverse), as toposhift stores both directions.
	SegF Table = iota
	SegR
	// Ent holds the valid-time segments of entities, one document per entity and
	// producer.
	Ent
	// LogF, LogR and LogE hold every record, one document per record (per
	// direction for an edge), so that Window, which returns every stored record, and
	// a read at a token inside a batch have something to read.
	LogF
	LogR
	LogE
	// Meta holds one document per committed batch, the sequence number it ended at.
	Meta

	// NumTables is the number of tables.
	NumTables int = iota
)

var tableNames = [NumTables]string{"seg_f", "seg_r", "ent", "log_f", "log_r", "log_e", "meta"}

func (t Table) String() string {
	if int(t) < len(tableNames) {
		return tableNames[t]
	}
	return fmt.Sprintf("Table(%d)", uint8(t))
}

// segment reports whether the table holds valid-time segments.
func (t Table) segment() bool { return t == SegF || t == SegR || t == Ent }

// logs reports whether the table is a log of records.
func (t Table) logs() bool { return t == LogF || t == LogR || t == LogE }

// Key names a document: the planner's name for an `_id`, before a server adapter
// encodes it. A segment document is one (subject, producer) in one table; a log
// document is one record, named by its sequence number, which is unique; a meta
// document is one batch, named by its ordinal (in Seq).
type Key struct {
	Table    Table
	Subject  engine.Subject     // segment and log documents
	Producer lifecycle.Producer // segment documents
	Seq      uint64             // log documents: the record's Seq; meta: the batch's ordinal
}

// entity is the entity a document is found from: the source of an edge read
// forward, its target read in reverse, or the entity itself.
func (k Key) entity() identity.Fingerprint {
	if k.Table == SegR || k.Table == LogR {
		return k.Subject.B
	}
	return k.Subject.A
}

// Rec is a log document: a record as the log holds it, with its times as Unix
// nanoseconds. The planner makes one per record, and the two documents of an edge
// share it.
type Rec struct {
	Layer      catalog.Layer
	Subject    engine.Subject
	Producer   lifecycle.Producer
	Event      int64
	Seq        uint64
	Kind       lifecycle.Kind
	TTL        time.Duration
	Through    int64 // meaningful only if HasThrough
	HasThrough bool
	Payload    []byte
}

// newRec converts a valid record. The payload is copied, so the caller can reuse
// its buffer.
func newRec(r engine.Record) *Rec {
	c := &Rec{
		Layer: r.Layer, Subject: r.Subject, Producer: r.Producer, Event: r.EventTime.UnixNano(),
		Seq: r.Seq, Kind: r.Kind, TTL: r.TTL, Payload: slices.Clone(r.Payload),
	}
	if !r.Through.IsZero() {
		c.Through, c.HasThrough = r.Through.UnixNano(), true
	}
	return c
}

// OffMicrosecond reports whether a record has a boundary, an event time or a finite
// deadline, that is not a whole microsecond: the records XTDB's temporal columns cannot
// hold, which the planner counts ([Counts].OffMicrosecond) and a server adapter refuses.
// It is the planner's own test, made on one record, so a stream can be counted without a
// planner. The next assertion's event time, the other boundary a segment has, is another
// record's event time.
func OffMicrosecond(r engine.Record) bool {
	c := Rec{Event: r.EventTime.UnixNano(), Kind: r.Kind, TTL: r.TTL}
	if !r.Through.IsZero() {
		c.Through, c.HasThrough = r.Through.UnixNano(), true
	}
	return !wholeMicroseconds(Instant(c.Event)) || !wholeMicroseconds(c.deadline())
}

// Record is the engine record the document stands for. The payload is copied, so a
// caller cannot change the log.
func (c *Rec) Record() engine.Record {
	r := engine.Record{
		Layer: c.Layer, Subject: c.Subject, Producer: c.Producer, EventTime: time.Unix(0, c.Event).UTC(),
		Seq: c.Seq, Kind: c.Kind, TTL: c.TTL, Payload: slices.Clone(c.Payload),
	}
	if c.HasThrough {
		r.Through = time.Unix(0, c.Through).UTC()
	}
	return r
}

// assertion is the lifecycle assertion of the record. The attributes are left out:
// existence does not depend on them under the zero policy.
func (c *Rec) assertion() lifecycle.Assertion {
	a := lifecycle.Assertion{
		Producer: c.Producer, EventTime: time.Unix(0, c.Event).UTC(), Seq: c.Seq,
		Kind: c.Kind, TTL: c.TTL,
	}
	if c.HasThrough {
		a.Through = time.Unix(0, c.Through).UTC()
	}
	return a
}

// deadline is the instant an Observe's reference lapses: the later of its event time
// and Through, plus its TTL, or [Forever] when it has no TTL (only a delete ends it).
// A delete holds nothing, so its end is its own event time.
func (c *Rec) deadline() Instant {
	switch {
	case c.Kind == lifecycle.Delete:
		return Instant(c.Event)
	case c.TTL <= 0:
		return Forever
	}
	last := c.Event
	if c.HasThrough && c.Through > last {
		last = c.Through
	}
	return Instant(last + int64(c.TTL))
}

// OpKind is the kind of a statement in a transaction.
type OpKind uint8

const (
	// Insert writes a document over a valid-time range. Inserting a document that
	// exists overwrites its valid-time portion [From, To) and leaves the rest.
	Insert OpKind = iota + 1
	// DeletePortion is DELETE ... FOR PORTION OF VALID_TIME FROM From TO To: the
	// document is absent from the valid-time portion [From, To) and unchanged
	// elsewhere.
	DeletePortion
)

func (k OpKind) String() string {
	switch k {
	case Insert:
		return "insert"
	case DeletePortion:
		return "delete portion"
	}
	return fmt.Sprintf("OpKind(%d)", uint8(k))
}

// Op is one statement of a transaction.
type Op struct {
	Kind OpKind
	Key  Key
	// From and To are the valid-time range of the statement, From < To. A log or
	// meta insert is over all of valid time (From 0, To Forever): those tables are
	// only read with FOR VALID_TIME ALL.
	From, To Instant
	// Layer is the `layer` column of a segment document.
	Layer catalog.Layer
	// Rec is the document of a log insert.
	Rec *Rec
	// LastSeq is the document of a meta insert: the Seq the batch ended at.
	LastSeq uint64
}

func (o Op) String() string {
	s := fmt.Sprintf("%s %s", o.Kind, o.Key.Table)
	switch {
	case o.Key.Table.segment():
		s += fmt.Sprintf(" %s %q [%s, %s)", describeSubject(o.Key.Subject), o.Key.Producer, o.From, o.To)
	case o.Key.Table.logs():
		s += fmt.Sprintf(" seq %d", o.Key.Seq)
	case o.Key.Table == Meta:
		s += fmt.Sprintf(" batch %d (last seq %d)", o.Key.Seq, o.LastSeq)
	}
	return s
}

func describeSubject(s engine.Subject) string {
	if s.Kind == engine.SubjectEdge {
		return s.A.String() + " -" + string(s.Relation) + "-> " + s.B.String()
	}
	return s.A.String()
}

// Txn is one transaction: the statements of a batch, at the batch's system time.
type Txn struct {
	System Instant
	Ops    []Op
}
