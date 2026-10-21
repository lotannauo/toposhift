package doltengine

import (
	"fmt"
	"time"

	"github.com/lotannauo/toposhift/bench/dolt/doltmodel"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// This file is the writer's planning, and it touches no database: given a batch and what
// the writer remembers, it says which rows go to each table and how the writer's memory
// changes once the batch is committed. The rows of the plan are the model's row types, so
// a test can hold the plan, batch after batch, to the tables of a [doltmodel.Model] that
// was given the same stream, with no server.

// subjectProducer is a state key as the writer remembers it: a subject and one producer.
// An edge's rows in the two directions are one key here, because they change together.
type subjectProducer struct {
	subject  engine.Subject
	producer lifecycle.Producer
}

// stateEntry is what the writer remembers of the state row of a key: which record it holds,
// by (event time, seq), and the deadline at which it stops holding, which retention
// reads. It is the state row less its address.
type stateEntry struct {
	eventNs, seq int64 // seq as stored: [doltmodel.EncodeSeq]
	open         bool
	deadlineNs   int64
}

func (e stateEntry) newerThan(o stateEntry) bool {
	if e.eventNs != o.eventNs {
		return e.eventNs > o.eventNs
	}
	return e.seq > o.seq
}

// writer is what the engine remembers between batches, and is rebuilt from the tables when
// it is opened again. Only the writer's goroutine uses it.
type writer struct {
	payloadCap int
	horizon    int64
	pastEnd    bool
	lastSeq    uint64

	// layers is the layer each subject was first written in.
	layers map[engine.Subject]catalog.Layer
	// newest is the state row of each key the state table holds.
	newest map[subjectProducer]stateEntry

	f *faults
}

func newWriter(f *faults) *writer {
	return &writer{
		payloadCap: doltmodel.RegisteredPayloadCapacity,
		layers:     make(map[engine.Subject]catalog.Layer),
		newest:     make(map[subjectProducer]stateEntry),
		f:          f,
	}
}

// batchPlan is what one batch writes.
type batchPlan struct {
	last uint64

	log      []doltmodel.LogRow
	entLog   []doltmodel.EntLogRow
	state    []doltmodel.StateRow
	entState []doltmodel.EntStateRow

	// Applied to the writer's memory when the batch is committed.
	layers   map[engine.Subject]catalog.Layer
	replaced map[subjectProducer]stateEntry

	// counts are what the batch adds to the writer's counters.
	counts doltmodel.Counts
}

// validate refuses a batch whole, before anything is planned or written, by the rules every
// engine of the spike has: each record is valid, a producer and a payload fit their
// columns, no record is before the retention horizon, sequence numbers rise above the last
// written, and a subject is written in one layer. The first record that breaks a rule
// decides the error. capacityRefused says the refusal was for a payload over the column,
// which is counted.
func (w *writer) validate(batch []engine.Record) (capacityRefused bool, err error) {
	seq := w.lastSeq
	inBatch := make(map[engine.Subject]catalog.Layer)
	for _, r := range batch {
		if err := r.Validate(); err != nil && !w.f.skipValidate {
			return false, err
		}
		if len(r.Producer) > doltmodel.MaxProducerLen && !w.f.ignoreProducerLen {
			return false, fmt.Errorf("record seq %d: producer of %d bytes is over the column's %d: %w", r.Seq, len(r.Producer), doltmodel.MaxProducerLen, engine.ErrInvalid)
		}
		if len(r.Payload) > w.payloadCap && !w.f.ignoreCapacity {
			return true, fmt.Errorf("record seq %d: payload of %d bytes is over the column's %d: %w", r.Seq, len(r.Payload), w.payloadCap, engine.ErrInvalid)
		}
		if !w.f.acceptBeforeHorizon && (w.pastEnd || doltmodel.Nanos(r.EventTime) < w.horizon) {
			return false, fmt.Errorf("record seq %d at %s: %w", r.Seq, r.EventTime.Format(time.RFC3339), engine.ErrBeforeHorizon)
		}
		if r.Seq <= seq && !w.f.acceptStaleSeq {
			return false, fmt.Errorf("record seq %d is not above the last written seq %d: %w", r.Seq, seq, engine.ErrInvalid)
		}
		seq = r.Seq
		known, ok := inBatch[r.Subject]
		if !ok {
			known, ok = w.layers[r.Subject]
		}
		if ok && known != r.Layer && !w.f.acceptLayerConflict {
			return false, fmt.Errorf("record seq %d: subject is stored in layer %s, not %s: %w", r.Seq, known, r.Layer, engine.ErrInvalid)
		}
		inBatch[r.Subject] = r.Layer
	}
	return false, nil
}

// logRows are the rows a record is stored as in the log: an edge in each direction, an
// entity once.
func logRows(r engine.Record) (edges []doltmodel.LogRow, ent *doltmodel.EntLogRow) {
	through := int64(0)
	hasThrough := !r.Through.IsZero()
	if hasThrough {
		through = doltmodel.Nanos(r.Through)
	}
	kind := doltmodel.EncodeKind(r.Kind)
	payload := string(r.Payload)
	if r.Subject.Kind == engine.SubjectEntity {
		return nil, &doltmodel.EntLogRow{
			Fp: string(doltmodel.EncodeFingerprint(r.Subject.A)), Producer: string(r.Producer), Layer: int(r.Layer),
			EventNs: doltmodel.Nanos(r.EventTime), Seq: doltmodel.EncodeSeq(r.Seq), Kind: kind, TTLNs: int64(r.TTL),
			HasThrough: hasThrough, ThroughNs: through, Payload: payload,
		}
	}
	a, b := string(doltmodel.EncodeFingerprint(r.Subject.A)), string(doltmodel.EncodeFingerprint(r.Subject.B))
	row := doltmodel.LogRow{
		Layer: int(r.Layer), Rel: string(r.Subject.Relation), Producer: string(r.Producer),
		EventNs: doltmodel.Nanos(r.EventTime), Seq: doltmodel.EncodeSeq(r.Seq), Kind: kind, TTLNs: int64(r.TTL),
		HasThrough: hasThrough, ThroughNs: through, Payload: payload,
	}
	fwd, rev := row, row
	fwd.Dir, fwd.A, fwd.Peer = int(engine.Forward), a, b
	rev.Dir, rev.A, rev.Peer = int(engine.Reverse), b, a
	return []doltmodel.LogRow{fwd, rev}, nil
}

// staged is the newest record of a key within the batch being planned: the rows it is
// stored as, and its place in the batch.
type staged struct {
	key   subjectProducer
	edges []doltmodel.LogRow
	ent   *doltmodel.EntLogRow
}

func (s staged) entry() (eventNs, seq int64) {
	if s.ent != nil {
		return s.ent.EventNs, s.ent.Seq
	}
	return s.edges[0].EventNs, s.edges[0].Seq
}

// plan validates the batch and says what it writes. A batch that fails validation has no
// plan, and the writer is as it was.
func (w *writer) plan(batch []engine.Record) (p *batchPlan, capacityRefused bool, err error) {
	if capacityRefused, err = w.validate(batch); err != nil {
		return nil, capacityRefused, err
	}
	p = &batchPlan{
		last: batch[len(batch)-1].Seq, layers: make(map[engine.Subject]catalog.Layer), replaced: make(map[subjectProducer]stateEntry),
	}
	p.counts.Records = uint64(len(batch))
	newest := make(map[subjectProducer]*staged)
	var order []*staged
	for _, r := range batch {
		if _, ok := w.layers[r.Subject]; !ok {
			p.layers[r.Subject] = r.Layer
		}
		edges, ent := logRows(r)
		p.log = append(p.log, edges...)
		if ent != nil {
			p.entLog = append(p.entLog, *ent)
		}
		p.counts.LogRows += uint64(max(len(edges), 1))

		// The writer's map of each key's newest record in this batch.
		key := subjectProducer{r.Subject, r.Producer}
		cur := &staged{key: key, edges: edges, ent: ent}
		prev, seen := newest[key]
		if !seen {
			newest[key] = cur
			order = append(order, cur)
			continue
		}
		ce, cs := cur.entry()
		pe, ps := prev.entry()
		if w.f.stateKeepsLast || w.replaces(stateEntry{eventNs: ce, seq: cs}, stateEntry{eventNs: pe, seq: ps}) {
			*prev = *cur
		}
	}
	for _, s := range order {
		var wrote []doltmodel.StateRow
		var wroteEnt *doltmodel.EntStateRow
		if s.ent != nil {
			_, es := doltmodel.DeriveState(nil, []doltmodel.EntLogRow{*s.ent})
			wroteEnt = &es[0]
		} else {
			st, _ := doltmodel.DeriveState(s.edges, nil)
			wrote = st
		}
		entry := entryOf(wrote, wroteEnt)
		if stored, ok := w.newest[s.key]; ok && !w.f.stateKeepsLast && !w.replaces(entry, stored) {
			continue // the stored row is newer: a late record changes no state
		}
		p.state = append(p.state, wrote...)
		if wroteEnt != nil {
			p.entState = append(p.entState, *wroteEnt)
		}
		p.replaced[s.key] = entry
		p.counts.StateWrites += uint64(max(len(wrote), 1))
	}
	return p, false, nil
}

// replaces reports whether a record's state row replaces another: the newer by (event time,
// seq), or with the defect of ordering by event time alone, a later event time only.
func (w *writer) replaces(s, over stateEntry) bool {
	if w.f.tieByEvent {
		return s.eventNs > over.eventNs
	}
	return s.newerThan(over)
}

func entryOf(edges []doltmodel.StateRow, ent *doltmodel.EntStateRow) stateEntry {
	if ent != nil {
		return stateEntry{eventNs: ent.EventNs, seq: ent.Seq, open: ent.Open, deadlineNs: ent.DeadlineNs}
	}
	r := edges[0]
	return stateEntry{eventNs: r.EventNs, seq: r.Seq, open: r.Open, deadlineNs: r.DeadlineNs}
}

// commit applies a committed batch to the writer's memory.
func (w *writer) commit(p *batchPlan) {
	for s, l := range p.layers {
		w.layers[s] = l
	}
	for k, e := range p.replaced {
		w.newest[k] = e
	}
	w.lastSeq = p.last
}

// retention is what a retention at a horizon does to the writer's memory: the state rows
// whose deadline is at or before it are gone.
func (w *writer) expire(h int64) (gone int) {
	for k, e := range w.newest {
		if !e.open && e.deadlineNs <= h || e.open && w.f.retainDropsOpenState {
			delete(w.newest, k)
			gone++
		}
	}
	return gone
}
