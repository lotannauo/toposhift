package pgmodel

import (
	"container/heap"
	"fmt"
	"math"
	"slices"
	"sort"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// WriterConfig says how a [Writer] keeps rows.
type WriterConfig struct {
	// Naive makes the writer rewrite the range of an interval each time it is extended,
	// where the skilled writer moves the deadline of a provisional row, which no index
	// covers. It is a diagnostic: what the heap-only updates save.
	Naive bool
}

// Loc is where an engine keeps a row, for an engine that addresses a row by its place in
// the table: a Postgres ctid is six bytes. The planner carries it and never reads it.
type Loc [6]byte

// Stored is a stored row and where it is.
type Stored struct {
	Row
	Loc Loc
}

// Kind is the kind of a row change.
type Kind uint8

const (
	// Insert stores New.
	Insert Kind = iota + 1
	// Update replaces Old with New; they have one start.
	Update
	// Delete removes Old.
	Delete
)

// RowChange is one change to the interval rows of a subject.
type RowChange struct {
	Kind Kind
	// Old is the stored row an Update replaces or a Delete removes.
	Old Stored
	// New is the row an Insert stores or an Update leaves.
	New Row
	// Extension marks an update that moves only the deadline of a provisional row. The
	// rows it changes have no index on the deadline, so a server makes it heap-only.
	Extension bool
	// HeapOnly marks an update that leaves the range of the row as it was, so that no
	// index entry changes and a server can make it heap-only (an Extension is one).
	HeapOnly bool
	// Finalizer marks an update that closes a provisional row whose deadline has
	// passed the newest event time written.
	Finalizer bool
}

// SubjectChanges are the changes of one subject's rows from one batch or one step.
type SubjectChanges struct {
	Subject engine.Subject
	Layer   catalog.Layer
	Rows    []RowChange
}

// producerState is what the writer remembers of a producer's latest assertion about a
// subject: enough to tell whether a new Observe can only lengthen what the producer
// has asserted.
type producerState struct {
	name  lifecycle.Producer
	event int64
	kind  lifecycle.Kind
	// end is the end of the producer's latest segment: its deadline. open says an
	// Observe with no TTL, which has no end. Both are meaningful only for an Observe.
	end  int64
	open bool
}

// subjectState is what the writer remembers of a subject between batches, so that a
// record that only extends the subject's last interval needs no read.
type subjectState struct {
	id    engine.Subject
	layer catalog.Layer
	// last is the subject's last stored row, as stored (provisional or not). hasLast is
	// false when the subject has no row. known is false when the writer does not know
	// the last row: a re-fold from an interval that was not the last left no row from
	// there on, and the rows before it are not in memory. A subject whose last row is
	// not known is written through a re-fold.
	last    Stored
	hasLast bool
	known   bool
	prods   []producerState
	// heapKey is the deadline the subject's live entry in the deadline heap is under.
	heapKey int64
	inHeap  bool
}

// noteRecord records a record as its producer's latest assertion about the subject,
// unless the producer has asserted something later. A record of the same instant
// replaces the earlier one, as it has the higher sequence number.
func noteRecord(prods []producerState, c rec) []producerState {
	st := producerState{name: c.producer, event: c.event, kind: c.kind}
	if c.kind == lifecycle.Observe {
		end, open, ok := segEnd(c)
		// A deadline past the last instant has no integer to be kept as: the producer's
		// segment is then taken to be longer than any a record can carry, so that no
		// record extends it on the fast path.
		st.end, st.open = end, open || !ok
	}
	for i := range prods {
		if prods[i].name == c.producer {
			if c.event >= prods[i].event {
				prods[i] = st
			}
			return prods
		}
	}
	return append(prods, st)
}

// segEnd is where the segment an Observe starts ends if nothing later ends it: its
// deadline, or open if it has no TTL. ok is false when the deadline is after the last
// instant an engine can store.
func segEnd(c rec) (end int64, open, ok bool) {
	if c.ttl == 0 {
		return 0, true, true
	}
	last := c.event
	if c.hasThrough && c.through > last {
		last = c.through
	}
	t := FromNanos(last).Add(c.ttl)
	if t.After(engine.MaxEventTime) {
		return 0, false, false
	}
	return ToNanos(t), false, true
}

// Writer plans the row changes of a streaming writer, for the [Model] and for an
// engine over a database, and keeps what the plan needs between batches.
//
// # What a row is
//
// The rows a subject has after a batch are the maximal intervals of its records folded
// with [lifecycle.Fold], each as one row: an interval that has ended, or whose deadline
// is not past the newest event time written, is a final row, and an interval that ends
// by a deadline after the newest event time is a provisional row, which keeps its range
// open and the deadline in its own column, so that moving it forward is an update that
// no index sees. Which form a row has is decided by the stream alone, so a stream
// written in any batches leaves the same rows, and only the last row of a subject can be
// provisional.
//
// # Three ways to keep them
//
// A record that can only lengthen the subject's last interval is the fast path: it needs
// no read, only the writer's memory of the last row and of each producer's latest
// assertion, and costs one insert (a new interval) or one update of the deadline. Any
// other record is the slow path: the stored rows from the interval the batch lands in are
// read, the log from where that interval starts is folded, and the rows that differ are
// replaced (see the package documentation). After each batch the finalizer closes every
// provisional row whose deadline the newest event time has passed.
//
// A Writer is not safe for concurrent use; a plan is made, then committed or abandoned.
type Writer struct {
	cfg     WriterConfig
	horizon int64
	// newest is the latest event time written; math.MinInt64 before any.
	newest int64
	subj   map[engine.Subject]*subjectState
	heap   deadlineHeap
}

// NewWriter returns a writer with nothing written.
func NewWriter(cfg WriterConfig) *Writer {
	return &Writer{cfg: cfg, newest: math.MinInt64, subj: make(map[engine.Subject]*subjectState)}
}

// Horizon is the retention horizon the writer plans for, in Unix nanoseconds.
func (w *Writer) Horizon() int64 { return w.horizon }

type heapEntry struct {
	key int64
	id  engine.Subject
}

type deadlineHeap []heapEntry

func (h deadlineHeap) Len() int           { return len(h) }
func (h deadlineHeap) Less(i, j int) bool { return h[i].key < h[j].key }
func (h deadlineHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *deadlineHeap) Push(x any)        { *h = append(*h, x.(heapEntry)) }
func (h *deadlineHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// schedule makes sure the subject has a live entry in the heap at or before the
// deadline.
func (w *Writer) schedule(st *subjectState, deadline int64) {
	if st.inHeap && st.heapKey <= deadline {
		return
	}
	st.heapKey, st.inHeap = deadline, true
	heap.Push(&w.heap, heapEntry{key: deadline, id: st.id})
}

// canon is the form a stored row has once the newest event time written is newest: a
// final row, a provisional one, or an open one. row has no deadline.
func (w *Writer) canon(r Row, newest int64) Row { return canonical(r, newest, w.cfg.Naive) }

func canonical(r Row, newest int64, naive bool) Row {
	switch {
	case r.Open:
		return OpenRow(r.Start)
	case !naive && r.EndSource == lifecycle.EndLivenessExpiry && r.End > newest:
		return Provisional(r.Start, r.End, r.EndSource)
	}
	return Final(r.Start, r.End, r.EndSource)
}

// logical is a stored row with its deadline, if any, as its end.
func logical(r Row) Row {
	if !r.HasDeadline {
		return r
	}
	return Final(r.Start, r.Deadline, r.EndSource)
}

func sameRange(a, b Row) bool {
	if a.Start != b.Start || a.Open != b.Open {
		return false
	}
	return a.Open || a.End == b.End
}

// classify says what kind of update takes old to new, which have one start.
func classify(old Stored, to Row) RowChange {
	c := RowChange{Kind: Update, Old: old, New: to, HeapOnly: sameRange(old.Row, to)}
	c.Extension = old.HasDeadline && to.HasDeadline && old.Open && to.Open && old.EndSource == to.EndSource && old.Deadline != to.Deadline
	return c
}

// A batch is planned in steps. Each subject of the batch is planned on the fast path if
// every record of it allows, and otherwise needs the stored rows and the log it is
// folded from; [BatchPlan.Slow] says which, [BatchPlan.Resolve] is given them, and
// [BatchPlan.Finish] closes the plan with the finalizer.

type item struct {
	id       engine.Subject
	layer    catalog.Layer
	recs     []rec
	earliest int64
	st       *subjectState
	slow     bool
	resolved bool
	next     subjectState
	changes  []RowChange
	// folded is the number of records a re-fold read.
	folded int
}

// BatchPlan is the plan of one batch.
type BatchPlan struct {
	w      *Writer
	newest int64
	items  []*item
	byID   map[engine.Subject]*item
	// the finalizer's work, once Finish has run
	finished bool
	popped   []heapEntry
	closed   []closedRow
	repush   []*subjectState
	counts   Counts
}

// closedRow is a subject whose provisional row the finalizer closes, and the row it is
// then.
type closedRow struct {
	st   *subjectState
	last Stored
}

// Plan starts the plan of a batch of valid records, in ascending order of sequence
// number, none before the horizon. It refuses a batch in which a subject is in more
// than one layer, or is not in the layer it was stored in. Nothing the writer
// remembers changes until [BatchPlan.Commit].
func (w *Writer) Plan(batch []engine.Record) (*BatchPlan, error) {
	p := &BatchPlan{w: w, newest: w.newest, byID: make(map[engine.Subject]*item)}
	for _, r := range batch {
		it := p.byID[r.Subject]
		if it == nil {
			it = &item{id: r.Subject, layer: r.Layer, earliest: math.MaxInt64, st: w.subj[r.Subject]}
			p.byID[r.Subject] = it
			p.items = append(p.items, it)
			if it.st != nil && it.st.layer != r.Layer {
				return nil, fmt.Errorf("record seq %d: subject is stored in layer %s, not %s: %w", r.Seq, it.st.layer, r.Layer, engine.ErrInvalid)
			}
		}
		if it.layer != r.Layer {
			return nil, fmt.Errorf("record seq %d: subject is stored in layer %s, not %s: %w", r.Seq, it.layer, r.Layer, engine.ErrInvalid)
		}
		c := keep(r)
		it.recs = append(it.recs, c)
		it.earliest = min(it.earliest, c.event)
		p.newest = max(p.newest, c.event)
	}
	for _, it := range p.items {
		w.tryFast(it, p.newest)
	}
	return p, nil
}

// Slow lists the subjects of the plan that need a re-fold, and so the stored rows and
// the log they ask for, in the order they first appear in the batch.
func (p *BatchPlan) Slow() []engine.Subject {
	var out []engine.Subject
	for _, it := range p.items {
		if it.slow {
			out = append(out, it.id)
		}
	}
	return out
}

// Earliest is the earliest event time of the batch's records of a slow subject, the
// time the stored rows and the log are read from (see [SlowStart]).
func (p *BatchPlan) Earliest(s engine.Subject) int64 { return p.byID[s].earliest }

// SlowStart is where a re-fold begins, given the stored rows of the subject from the
// last that starts at or before earliest, ascending (or all its rows, if none does):
// the start of that row, or math.MinInt64 if there is none. The log is read from it.
func SlowStart(old []Stored, earliest int64) int64 {
	if len(old) > 0 && old[0].Start <= earliest {
		return old[0].Start
	}
	return math.MinInt64
}

// work is a subject's rows as a batch's records are applied to them one at a time.
type work struct {
	prods []producerState
	rows  []workRow
}

// workRow is a row in its logical form (no deadline), and the stored row it began as, if
// it did.
type workRow struct {
	Row
	orig *Stored
}

func newRow(start, end int64, open bool) Row {
	if open {
		return OpenRow(start)
	}
	return Final(start, end, lifecycle.EndLivenessExpiry)
}

// tryFast applies the records of a subject on the fast path, and leaves the plan of the
// subject (its changes and its next state) if every one allows; otherwise the subject is
// slow.
func (w *Writer) tryFast(it *item, newest int64) {
	var wk work
	if it.st != nil {
		if !it.st.known {
			it.slow = true
			return
		}
		wk.prods = slices.Clone(it.st.prods)
		if it.st.hasLast {
			orig := it.st.last
			wk.rows = []workRow{{Row: logical(orig.Row), orig: &orig}}
		}
	}
	for _, c := range it.recs {
		if !w.fast(&wk, c) {
			it.slow = true
			return
		}
	}
	it.next = subjectState{id: it.id, layer: it.layer, known: true, prods: wk.prods}
	for i, wr := range wk.rows {
		to := w.canon(wr.Row, newest)
		switch {
		case wr.orig == nil:
			it.changes = append(it.changes, RowChange{Kind: Insert, New: to})
		case to != wr.orig.Row:
			it.changes = append(it.changes, classify(*wr.orig, to))
		}
		if i == len(wk.rows)-1 {
			it.next.hasLast = true
			it.next.last = Stored{Row: to}
			if wr.orig != nil && to == wr.orig.Row {
				it.next.last.Loc = wr.orig.Loc
			}
		}
	}
}

// fast applies one record to a subject's rows if it can only lengthen them. It reports
// whether it did; if not, the work is abandoned.
//
// The record must be an Observe whose deadline is representable, and later than the
// latest assertion of its producer (or at its instant, which it replaces, if that was an
// Observe), whose own segment is no shorter than the one the producer's latest
// assertion has: then the segments the producer has asserted cover everything they
// covered, and the new one is added. The subject's last interval is [s, e): the record
// must not begin before s, and if it begins after e it is an interval of its own; if it
// begins in [s, e] (touching counts) it ends the interval at the later of e and its
// deadline, which is the interval's end with the liveness source if it passes e, and
// leaves it as it was if it does not. A deadline equal to e is left to the re-fold,
// which breaks a tie between sources. A row that has been closed (final) is not
// extended: a record that reaches it reopens it by a re-fold. The extended interval
// cannot meet the one before it, which ended before s, and none follows it.
func (w *Writer) fast(wk *work, c rec) bool {
	if c.kind != lifecycle.Observe {
		return false
	}
	d, open, ok := segEnd(c)
	if !ok {
		return false
	}
	for i := range wk.prods {
		p := &wk.prods[i]
		if p.name != c.producer {
			continue
		}
		if c.event < p.event || c.event == p.event && p.kind != lifecycle.Observe {
			return false
		}
		if p.kind == lifecycle.Observe {
			if p.open && !open || !p.open && !open && d < p.end {
				return false
			}
		}
	}
	if n := len(wk.rows); n == 0 {
		wk.rows = append(wk.rows, workRow{Row: newRow(c.event, d, open)})
	} else {
		cur := &wk.rows[n-1]
		end, curOpen := cur.EffectiveEnd()
		switch {
		case c.event < cur.Start:
			return false
		case !curOpen && c.event > end:
			wk.rows = append(wk.rows, workRow{Row: newRow(c.event, d, open)})
		case cur.orig != nil && !cur.orig.HasDeadline && !cur.orig.Open && !w.cfg.Naive:
			return false
		case curOpen:
			// Nothing that ends can lengthen an interval with no end, and a new open
			// segment is no longer.
		case open:
			cur.Row = OpenRow(cur.Start)
		case d > end:
			cur.End, cur.EndSource = d, lifecycle.EndLivenessExpiry
		case d < end:
		default:
			return false
		}
	}
	wk.prods = noteRecord(wk.prods, c)
	return true
}

// Resolve gives the plan what a slow subject is re-folded from: its stored rows from
// [SlowStart] on, ascending by start and with where each is, and every record of its log
// that is at or after that start, the baselines a retention left included, in any order
// and not including the batch's own records, which the plan has. It folds them, and
// plans the rows that differ.
func (p *BatchPlan) Resolve(s engine.Subject, old []Stored, input []LogRow) error {
	it := p.byID[s]
	if it == nil || !it.slow || it.resolved {
		return fmt.Errorf("pgmodel: the plan has no slow subject %s awaiting a re-fold", s.A)
	}
	w := p.w
	start := SlowStart(old, it.earliest)
	rs := append(recsOf(input), it.recs...)
	slices.SortFunc(rs, rec.cmp)
	folded, err := foldRows(rs)
	if err != nil {
		return err
	}
	var fresh []Row
	for _, r := range folded {
		if r.Start >= start && !r.EndsAtOrBefore(w.horizon) {
			fresh = append(fresh, w.canon(r, p.newest))
		}
	}
	it.changes = diffStored(old, fresh)
	it.folded = len(rs)

	it.next = subjectState{id: it.id, layer: it.layer, known: true}
	if it.st != nil {
		it.next.prods = slices.Clone(it.st.prods)
	}
	for _, c := range it.recs {
		it.next.prods = noteRecord(it.next.prods, c)
	}
	switch {
	case len(fresh) > 0:
		last := fresh[len(fresh)-1]
		it.next.hasLast, it.next.last = true, Stored{Row: last}
		if i := sort.Search(len(old), func(i int) bool { return old[i].Start >= last.Start }); i < len(old) && old[i].Row == last {
			it.next.last.Loc = old[i].Loc
		}
	case start != math.MinInt64:
		it.next.known = false
	}
	it.resolved = true
	return nil
}

// diffStored is the changes that take the stored rows to the rows they are to be, both
// ascending by start.
func diffStored(old []Stored, fresh []Row) []RowChange {
	var out []RowChange
	i, j := 0, 0
	for i < len(old) || j < len(fresh) {
		switch {
		case j == len(fresh) || i < len(old) && old[i].Start < fresh[j].Start:
			out = append(out, RowChange{Kind: Delete, Old: old[i]})
			i++
		case i == len(old) || fresh[j].Start < old[i].Start:
			out = append(out, RowChange{Kind: Insert, New: fresh[j]})
			j++
		default:
			if old[i].Row != fresh[j] {
				out = append(out, classify(old[i], fresh[j]))
			}
			i++
			j++
		}
	}
	return out
}

// Finish closes the plan: it runs the finalizer over the subjects the batch did not
// touch and returns the changes of every subject, the batch's in the order they first
// appear and then the finalizer's. Every slow subject must have been resolved.
func (p *BatchPlan) Finish() ([]SubjectChanges, error) {
	if p.finished {
		return nil, fmt.Errorf("pgmodel: the plan is already finished")
	}
	w := p.w
	var out []SubjectChanges
	for _, it := range p.items {
		if it.slow && !it.resolved {
			return nil, fmt.Errorf("pgmodel: the slow subject %s was not resolved", it.id.A)
		}
		if it.slow {
			p.counts.Refolds++
			p.counts.RecordsFolded += uint64(it.folded)
		} else {
			p.counts.FastSubjects++
		}
		p.count(it.changes)
		if len(it.changes) > 0 {
			out = append(out, SubjectChanges{Subject: it.id, Layer: it.layer, Rows: it.changes})
		}
	}
	// The finalizer: take every entry the newest event time has passed. An entry is live
	// if it is the one its subject's state names; a subject of the batch is planned
	// already.
	seen := make(map[*subjectState]bool)
	for len(w.heap) > 0 && w.heap[0].key <= p.newest {
		e := heap.Pop(&w.heap).(heapEntry)
		p.popped = append(p.popped, e)
		st := w.subj[e.id]
		if st == nil || !st.inHeap || st.heapKey != e.key || seen[st] {
			continue
		}
		seen[st] = true
		if p.byID[e.id] != nil {
			continue
		}
		switch {
		case !st.hasLast || !st.last.HasDeadline:
			// A live entry for a row that is no longer provisional: dropped.
		case st.last.Deadline > p.newest:
			p.repush = append(p.repush, st)
		default:
			to := Final(st.last.Start, st.last.Deadline, st.last.EndSource)
			ch := classify(st.last, to)
			ch.Finalizer = true
			p.closed = append(p.closed, closedRow{st: st, last: Stored{Row: to}})
			p.count([]RowChange{ch})
			out = append(out, SubjectChanges{Subject: st.id, Layer: st.layer, Rows: []RowChange{ch}})
		}
	}
	p.finished = true
	return out, nil
}

func (p *BatchPlan) count(rows []RowChange) {
	for _, c := range rows {
		switch c.Kind {
		case Insert:
			p.counts.RowsInserted++
		case Delete:
			p.counts.RowsDeleted++
		case Update:
			p.counts.RowsUpdated++
			if c.HeapOnly {
				p.counts.RowsHeapOnly++
			}
			if c.Finalizer {
				p.counts.RowsFinalized++
			}
		}
	}
}

// Counts are the plan's counts: the subjects planned each way, the records folded and
// the row changes. Records is not set; the caller knows the batch.
func (p *BatchPlan) Counts() Counts { return p.counts }

// Commit makes the plan what the writer remembers. It is called once the changes it
// returned are stored.
func (p *BatchPlan) Commit() {
	w := p.w
	if !p.finished {
		return
	}
	for _, e := range p.popped {
		if st := w.subj[e.id]; st != nil && st.inHeap && st.heapKey == e.key {
			st.inHeap = false
		}
	}
	for _, c := range p.closed {
		c.st.last = c.last
	}
	for _, st := range p.repush {
		w.schedule(st, st.last.Deadline)
	}
	for _, it := range p.items {
		st := w.subj[it.id]
		if st == nil {
			st = &subjectState{id: it.id}
			w.subj[it.id] = st
		}
		inHeap, key := st.inHeap, st.heapKey
		*st = it.next
		st.inHeap, st.heapKey = inHeap, key
		if st.hasLast && st.last.HasDeadline {
			w.schedule(st, st.last.Deadline)
		}
	}
	w.newest = p.newest
}

// Abort abandons the plan: the writer remembers what it did before it.
func (p *BatchPlan) Abort() {
	if !p.finished {
		return
	}
	for _, e := range p.popped {
		heap.Push(&p.w.heap, e)
	}
	p.popped = nil
	p.finished = false
}

// SetLoc says where the row of a subject that starts at start is, once it is stored. It
// is remembered if that is the subject's last row, the only one the writer addresses
// without reading it.
func (w *Writer) SetLoc(s engine.Subject, start int64, loc Loc) {
	if st := w.subj[s]; st != nil && st.hasLast && st.last.Start == start {
		st.last.Loc = loc
	}
}

// LastRow is the last stored row the writer remembers for a subject.
func (w *Writer) LastRow(s engine.Subject) (Stored, bool) {
	st := w.subj[s]
	if st == nil || !st.hasLast {
		return Stored{}, false
	}
	return st.last, true
}

// EachLast calls f with the last row the writer remembers for each subject that has one,
// in no particular order. It is for a check that what the writer remembers is what is
// stored.
func (w *Writer) EachLast(f func(s engine.Subject, layer catalog.Layer, last Stored)) {
	for _, st := range w.subj {
		if st.hasLast {
			f(st.id, st.layer, st.last)
		}
	}
}

// Located is a row and the subject it is a row of.
type Located struct {
	Subject engine.Subject
	Layer   catalog.Layer
	Stored
}

// RetainPlan is the part of a retention the writer knows: the provisional rows whose
// deadline the horizon has passed, which have to be deleted by their place, because no
// index covers a deadline. The rows that have ended are found by their range.
type RetainPlan struct {
	h       int64
	Deleted []Located
}

// PlanRetain says which of the rows the writer remembers a horizon removes, and which
// of them are provisional.
func (w *Writer) PlanRetain(h int64) RetainPlan {
	p := RetainPlan{h: h}
	for _, st := range w.subj {
		if st.hasLast && st.last.EndsAtOrBefore(h) && st.last.HasDeadline {
			p.Deleted = append(p.Deleted, Located{Subject: st.id, Layer: st.layer, Stored: st.last})
		}
	}
	slices.SortFunc(p.Deleted, func(a, b Located) int { return compareSubjects(a.Subject, b.Subject) })
	return p
}

// CommitRetain moves the horizon and forgets the rows it removed.
func (w *Writer) CommitRetain(h int64) {
	w.horizon = max(w.horizon, h)
	for _, st := range w.subj {
		if st.hasLast && st.last.EndsAtOrBefore(h) {
			st.hasLast = false
			st.last = Stored{}
		}
	}
}

func compareSubjects(a, b engine.Subject) int {
	if c := engine.CompareFingerprints(a.A, b.A); c != 0 {
		return c
	}
	if c := engine.CompareFingerprints(a.B, b.B); c != 0 {
		return c
	}
	switch {
	case a.Relation < b.Relation:
		return -1
	case a.Relation > b.Relation:
		return 1
	}
	return int(a.Kind) - int(b.Kind)
}

// Restore gives the writer what a database holds for a subject it opens: the subject's
// last stored row, if it has one, and the latest record of each of its producers.
func (w *Writer) Restore(s engine.Subject, layer catalog.Layer, last *Stored, latest []LogRow) {
	st := &subjectState{id: s, layer: layer, known: true}
	if last != nil {
		st.hasLast, st.last = true, *last
	}
	for _, l := range latest {
		st.prods = noteRecord(st.prods, l.rec())
	}
	w.subj[s] = st
	if st.hasLast && st.last.HasDeadline {
		w.schedule(st, st.last.Deadline)
	}
}

// SetState gives the writer the horizon and the newest event time of a database it
// opens.
func (w *Writer) SetState(horizon, newest int64) { w.horizon, w.newest = horizon, newest }
