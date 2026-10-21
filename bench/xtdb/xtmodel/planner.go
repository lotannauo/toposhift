package xtmodel

import (
	"fmt"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// History is what a planner asks of the log of committed batches when a record is
// late. A server adapter answers it from its log tables; the [Model] from an index
// of its own.
type History interface {
	// Next returns the event time of the earliest assertion by producer about
	// subject that is strictly after the event time given, among the committed
	// batches, and whether there is one.
	Next(s engine.Subject, p lifecycle.Producer, after int64) (event int64, found bool, err error)
}

// segKey is what a segment document is made for: one producer's assertions about
// one subject. An edge's two directions are two tables over the same segments.
type segKey struct {
	subject  engine.Subject
	producer lifecycle.Producer
}

// segState is what the planner remembers of a segKey: the event time of the
// producer's latest assertion about the subject, and where the segment that
// assertion makes ends (its own event time for a delete, which holds nothing).
type segState struct {
	last int64
	end  Instant
}

// Planner turns the batches of a stream into XTDB transactions: statements over the
// valid time of each producer's lifecycle segments, and one log document per record.
//
// # The mapping
//
// Valid time carries, per (subject, producer), the lifecycle segments of
// [lifecycle.Fold]: an Observe at E is present from E until the producer's next
// assertion about the subject at N (N at the end of time if there is none) or until
// its deadline D, whichever comes first; a Delete holds nothing. Two assertions
// of one producer at one instant are one, the later write winning. The subject
// exists at t if some producer's segment contains t, which is what Fold's
// existence intervals say, so a read of valid time is a read of existence.
//
// A record is written by the statements that make its document say exactly that:
//
//   - Observe: insert [E, min(D, N)); and if D is before N, delete [D, N), because
//     what the document held there before (the segment of the assertion before E, or
//     of the record this one overwrites) is no longer true;
//   - Delete: delete [E, N).
//
// A record is in order when E is not before the producer's latest event time on the
// subject, and then N is the end of time and the planner knows, from its memory of
// that latest assertion's end, whether anything is left to delete; an in-order
// Observe is usually one insert. A late record needs N, the next assertion after
// E, from the [History] (the slow path), and its delete is not conditional.
//
// The statements of a batch for one document are merged, in the order of the records'
// Seq, into the fewest that have the same effect ([merge]).
//
// # Memory
//
// The planner remembers one [segState] per (subject, producer) it has planned. That
// memory is complete: a (subject, producer) it does not know has no segment stored. A
// Plan changes nothing until it is Accepted, so a transaction the server refuses leaves
// the planner as it was.
type Planner struct {
	hist  History
	state map[segKey]segState
	mut   mutation
}

// NewPlanner returns a planner that knows nothing, over an empty store whose log h
// answers from.
func NewPlanner(h History) *Planner {
	return &Planner{hist: h, state: make(map[segKey]segState)}
}

// Plan is the transaction of one batch and what planning it learned.
type Plan struct {
	// Txn is the transaction, without its system time, which the committer gives.
	Txn Txn
	// Counts is the statements and records of this batch.
	Counts Counts

	next map[segKey]segState
}

// keyPlan is the statements one batch makes about one segKey.
type keyPlan struct {
	key   segKey
	layer catalog.Layer
	ops   []vop
	// fresh says the planner knew nothing of the segKey before the batch, so the
	// store holds no document for it.
	fresh bool
}

// Plan plans a batch, the ordinal n of the transaction that commits it. The batch
// must be valid ([engine.Record.Validate]) and in ascending Seq, above every Seq
// planned before; the caller checks. Plan changes nothing of the planner.
func (p *Planner) Plan(n uint64, batch []engine.Record) (*Plan, error) {
	if len(batch) == 0 {
		return nil, fmt.Errorf("xtmodel: an empty batch has no transaction")
	}
	var (
		working  = make(map[segKey]segState)
		pending  = make(map[segKey][]int64) // event times written earlier in this batch
		plans    = make(map[segKey]*keyPlan)
		order    []*keyPlan
		logOps   []Op
		counts   = Counts{Batches: 1, Records: uint64(len(batch))}
		requests uint64 // segment statements the records ask for
	)
	for _, r := range batch {
		c := newRec(r)
		sk := segKey{r.Subject, r.Producer}
		kp := plans[sk]
		if kp == nil {
			_, known := p.state[sk]
			kp = &keyPlan{key: sk, layer: r.Layer, fresh: !known}
			plans[sk] = kp
			order = append(order, kp)
		}
		st, known := working[sk]
		if !known {
			st, known = p.state[sk]
		}
		e, d := Instant(c.Event), c.deadline()
		if !wholeMicroseconds(e) || !wholeMicroseconds(d) {
			counts.OffMicrosecond++
		}

		var ops []vop
		next := st
		if !known || c.Event >= st.last {
			counts.InOrder++
			ops, next = p.inOrder(c, st, known, e, d)
		} else {
			counts.Late++
			var err error
			if ops, err = p.late(c, sk, pending[sk], e, d); err != nil {
				return nil, err
			}
		}
		kp.ops = append(kp.ops, ops...)
		requests += uint64(len(ops))
		working[sk] = next
		pending[sk] = append(pending[sk], c.Event)

		switch r.Subject.Kind {
		case engine.SubjectEdge:
			logOps = append(logOps, logOp(LogF, c), logOp(LogR, c))
		default:
			logOps = append(logOps, logOp(LogE, c))
		}
	}

	var ops []Op
	var written uint64
	for _, kp := range order {
		merged := merge(kp.ops, kp.fresh, p.mut == mutMergeOutOfSeqOrder)
		written += uint64(len(merged))
		for _, tbl := range segmentTables(kp.key.subject) {
			for _, m := range merged {
				k := DeletePortion
				if m.insert {
					k = Insert
				}
				ops = append(ops, Op{
					Kind: k, Key: Key{Table: tbl, Subject: kp.key.subject, Producer: kp.key.producer},
					From: m.from, To: m.to, Layer: kp.layer,
				})
			}
		}
	}
	counts.MergedAway = requests - written
	ops = append(ops, logOps...)
	ops = append(ops, Op{
		Kind: Insert, Key: Key{Table: Meta, Seq: n}, From: 0, To: Forever,
		LastSeq: batch[len(batch)-1].Seq,
	})
	for _, o := range ops {
		switch o.Kind {
		case Insert:
			counts.Tables[o.Key.Table].Inserts++
		case DeletePortion:
			counts.Tables[o.Key.Table].PortionDeletes++
		}
	}
	return &Plan{Txn: Txn{Ops: ops}, Counts: counts, next: working}, nil
}

// inOrder plans a record whose event time is not before the producer's latest on the
// subject. Whatever the producer's latest assertion left present from this record's
// event time on is what the statements must replace.
func (p *Planner) inOrder(c *Rec, st segState, known bool, e, d Instant) ([]vop, segState) {
	prevEnd := Instant(0) // nothing stored
	if known {
		prevEnd = st.end
	}
	if p.mut == mutLowerSeqWins && known && c.Event == st.last {
		return nil, st // the earlier record keeps the instant
	}
	var ops []vop
	if c.Kind == lifecycle.Delete {
		if prevEnd > e {
			ops = append(ops, vop{from: e, to: Forever})
		}
		return ops, segState{last: c.Event, end: e}
	}
	ops = append(ops, vop{insert: true, from: e, to: d})
	if prevEnd > d && p.mut != mutKeepLeftover {
		ops = append(ops, vop{from: d, to: Forever})
	}
	return ops, segState{last: c.Event, end: d}
}

// late plans a record whose event time is before the producer's latest on the
// subject: there is a next assertion, and the record is bounded by it.
func (p *Planner) late(c *Rec, sk segKey, pending []int64, e, d Instant) ([]vop, error) {
	n := Forever
	if p.mut != mutLateNextIsEnd {
		ev, found, err := p.hist.Next(sk.subject, sk.producer, c.Event)
		if err != nil {
			return nil, fmt.Errorf("xtmodel: the next assertion after %d: %w", c.Event, err)
		}
		if found {
			n = Instant(ev)
		}
		for _, t := range pending {
			if t > c.Event && Instant(t) < n {
				n = Instant(t)
			}
		}
	}
	if c.Kind == lifecycle.Delete {
		to := n
		if p.mut == mutDeleteToEnd {
			to = Forever
		}
		return []vop{{from: e, to: to}}, nil
	}
	ops := []vop{{insert: true, from: e, to: min(d, n)}}
	if d < n && p.mut != mutKeepLeftover {
		ops = append(ops, vop{from: d, to: n})
	}
	return ops, nil
}

// Accept records that the plan's transaction committed: the planner remembers what
// the batch said. Plans are accepted in the order they were made.
func (p *Planner) Accept(pl *Plan) {
	for k, v := range pl.next {
		p.state[k] = v
	}
}

// restore sets what the planner remembers of one segKey, for a planner rebuilt from
// the log.
func (p *Planner) restore(sk segKey, st segState) { p.state[sk] = st }

func logOp(t Table, c *Rec) Op {
	return Op{Kind: Insert, Key: Key{Table: t, Subject: c.Subject, Producer: c.Producer, Seq: c.Seq}, From: 0, To: Forever, Layer: c.Layer, Rec: c}
}

func segmentTables(s engine.Subject) []Table {
	if s.Kind == engine.SubjectEdge {
		return []Table{SegF, SegR}
	}
	return []Table{Ent}
}

// Rebuilder rebuilds a planner's memory from the records of a log: for each (subject,
// producer), the latest record by (event time, Seq) and where the segment it makes
// ends. A server adapter opened over a database an earlier one left feeds it the
// records of the forward log tables (the edge records of one direction and the entity
// records), in any order, and gets the planner that adapter had.
type Rebuilder struct {
	latest map[segKey]*Rec
}

// NewRebuilder returns a rebuilder that has seen no record.
func NewRebuilder() *Rebuilder { return &Rebuilder{latest: make(map[segKey]*Rec)} }

// Add notes a record. The record must be valid. Its payload, which the planner does
// not use, is not kept.
func (b *Rebuilder) Add(r engine.Record) {
	r.Payload = nil
	b.add(newRec(r))
}

func (b *Rebuilder) add(c *Rec) {
	sk := segKey{c.Subject, c.Producer}
	if prev := b.latest[sk]; prev == nil || (logEntry{rec: prev}).cmp(logEntry{rec: c}) < 0 {
		b.latest[sk] = c
	}
}

// Len is the number of (subject, producer) pairs seen.
func (b *Rebuilder) Len() int { return len(b.latest) }

// Planner returns a planner over the log h that remembers what the records said.
func (b *Rebuilder) Planner(h History) *Planner {
	p := NewPlanner(h)
	for sk, c := range b.latest {
		p.restore(sk, segState{last: c.Event, end: c.deadline()})
	}
	return p
}
