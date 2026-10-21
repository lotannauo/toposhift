package pgmodel

import (
	"fmt"
	"math"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// Counts is what a Model has done, in the units a Postgres writer is charged in. It
// is a function of the stream and the planner, so it is the same on every run and
// every machine.
type Counts struct {
	// Records are the records written to the log.
	Records uint64
	// Refolds are the subjects re-folded, one per subject per batch, and
	// RecordsFolded the records those folds read.
	Refolds, RecordsFolded uint64
	// RowsInserted, RowsUpdated and RowsDeleted are the changes to the interval
	// tables. A row whose start changes is a delete and an insert, because the start
	// is part of the key.
	RowsInserted, RowsUpdated, RowsDeleted uint64
}

// Model is an in-memory engine with the schema and the read semantics of the
// Postgres interval tables (see the package documentation). It implements
// [engine.Engine]. Reads run concurrently with each other; a read sees whole
// committed batches, because Write and Retain hold the lock for their whole
// effect.
type Model struct {
	mu      sync.RWMutex
	lastSeq uint64
	horizon int64 // Unix nanoseconds; records before it are refused
	// pastEnd says the horizon was moved after the last representable instant, so
	// every record, that instant's included, is before it. horizon alone cannot say
	// it: it is an int64.
	pastEnd  bool
	subjects map[engine.Subject]*subject
	incident [3]map[identity.Fingerprint][]*subject // by direction: entity -> its edges
	counts   Counts
}

var _ engine.Engine = (*Model)(nil)

// subject is what the model keeps for one subject: its layer, its log, its
// baselines and its interval rows.
type subject struct {
	id    engine.Subject
	layer catalog.Layer
	// recs is the log of the subject, ascending by (event time, seq).
	recs []rec
	// base are the baselines retention left: for each producer, its last record
	// before the horizon, ascending by producer.
	base []rec
	// rows are the interval rows, ascending by start. The rows of a subject are
	// maximal, so they neither overlap nor touch, and their ends ascend too.
	rows []Row
}

// New returns an empty model.
func New() *Model {
	m := &Model{subjects: make(map[engine.Subject]*subject)}
	m.incident[engine.Forward] = make(map[identity.Fingerprint][]*subject)
	m.incident[engine.Reverse] = make(map[identity.Fingerprint][]*subject)
	return m
}

// open models are what a directory holds: a model has no files, so a directory is
// only a name, and a model opened again under it finds what the last one left.
var open sync.Map

// Open returns the model that dir names, a new one the first time. It is the form a
// conformance factory needs, which closes an engine and opens it again over the same
// directory and requires everything back. Forget releases it.
func Open(dir string) *Model {
	m, _ := open.LoadOrStore(dir, New())
	return m.(*Model)
}

// Forget drops the model Open returned for dir.
func Forget(dir string) { open.Delete(dir) }

// Counts returns what the model has done so far.
func (m *Model) Counts() Counts {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.counts
}

// Write implements [engine.Engine]. The batch is validated whole before any of it is
// applied, so a refused batch changes nothing.
func (m *Model) Write(batch []engine.Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	seq := m.lastSeq
	inBatch := make(map[engine.Subject]catalog.Layer)
	for _, r := range batch {
		if err := r.Validate(); err != nil {
			return err
		}
		if m.pastEnd || ToNanos(r.EventTime) < m.horizon {
			return fmt.Errorf("record seq %d at %s: %w", r.Seq, r.EventTime.Format(time.RFC3339), engine.ErrBeforeHorizon)
		}
		if r.Seq <= seq {
			return fmt.Errorf("record seq %d is not above the last written seq %d: %w", r.Seq, seq, engine.ErrInvalid)
		}
		seq = r.Seq
		known, ok := inBatch[r.Subject]
		if !ok {
			if st := m.subjects[r.Subject]; st != nil {
				known, ok = st.layer, true
			}
		}
		if ok && known != r.Layer {
			return fmt.Errorf("record seq %d: subject is stored in layer %s, not %s: %w", r.Seq, known, r.Layer, engine.ErrInvalid)
		}
		inBatch[r.Subject] = r.Layer
	}

	// Group the records by subject, in the order the subjects first appear. A
	// subject's records stay in Seq order.
	var order []*subject
	fresh := make(map[*subject][]rec)
	for _, r := range batch {
		st := m.subjects[r.Subject]
		if st == nil {
			st = &subject{id: r.Subject, layer: r.Layer}
			m.subjects[r.Subject] = st
			if r.Subject.Kind == engine.SubjectEdge {
				m.incident[engine.Forward][r.Subject.A] = append(m.incident[engine.Forward][r.Subject.A], st)
				m.incident[engine.Reverse][r.Subject.B] = append(m.incident[engine.Reverse][r.Subject.B], st)
			}
		}
		if _, seen := fresh[st]; !seen {
			order = append(order, st)
		}
		fresh[st] = append(fresh[st], keep(r))
	}
	for _, st := range order {
		if err := m.apply(st, fresh[st]); err != nil {
			// The batch was validated, so a fold that fails is a bug in the model,
			// and the model is no longer consistent.
			return fmt.Errorf("pgmodel: applying a batch: %w", err)
		}
	}
	m.lastSeq = seq
	m.counts.Records += uint64(len(batch))
	return nil
}

// apply stores the records of one batch for one subject and brings its rows up to
// date: it re-folds the subject from the start of the last row that begins at or
// before the earliest of the new records, and replaces the rows from there on.
func (m *Model) apply(st *subject, recs []rec) error {
	earliest := recs[0].event
	for _, c := range recs[1:] {
		earliest = min(earliest, c.event)
	}
	for _, c := range recs {
		i := sort.Search(len(st.recs), func(i int) bool { return st.recs[i].cmp(c) > 0 })
		st.recs = slices.Insert(st.recs, i, c)
	}

	// S is the start of the last row that begins at or before the earliest new
	// event; from is its index. With no such row nothing before the new records can
	// be affected, and the whole subject is folded.
	from := sort.Search(len(st.rows), func(i int) bool { return st.rows[i].Start > earliest }) - 1
	var input []rec
	start := int64(math.MinInt64)
	if from >= 0 {
		start = st.rows[from].Start
	} else {
		from = 0
	}
	// The input is every record of the subject, baselines included, at or after S.
	// A record before S cannot matter: the segment it starts or ends is before S,
	// because S begins a maximal interval and so nothing before it overlaps or touches
	// S. Including the baselines that are before S would be wrong, not just wasteful:
	// without the later record of their producer that ended them, they run on.
	for _, c := range st.base {
		if c.event >= start {
			input = append(input, c)
		}
	}
	lo := sort.Search(len(st.recs), func(i int) bool { return st.recs[i].event >= start })
	input = append(input, st.recs[lo:]...)

	fresh, ch, err := refold(st.rows[from:], input, start, m.horizon)
	if err != nil {
		return err
	}
	rows := make([]Row, 0, from+len(fresh))
	rows = append(rows, st.rows[:from]...)
	st.rows = append(rows, fresh...)
	m.counts.Refolds++
	m.counts.RecordsFolded += uint64(len(input))
	m.counts.RowsInserted += uint64(len(ch.Inserted))
	m.counts.RowsUpdated += uint64(len(ch.Updated))
	m.counts.RowsDeleted += uint64(len(ch.Deleted))
	return nil
}

// LastSeq implements [engine.Engine].
func (m *Model) LastSeq() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.lastSeq
}

// liveAt reports whether the subject exists at t, as the scope sees it. The latest
// snapshot is answered from the interval rows; an earlier one has no interval
// table to read, so the log of the subject is folded as the token sees it.
func (m *Model) liveAt(st *subject, t int64, sc engine.Scope) (bool, error) {
	if st.layer != sc.Layer {
		return false, nil
	}
	if sc.AsOf >= m.lastSeq {
		return rowsContain(st.rows, t), nil
	}
	var seen []rec
	for _, c := range st.base {
		if c.seq <= sc.AsOf {
			seen = append(seen, c)
		}
	}
	for _, c := range st.recs {
		if c.seq <= sc.AsOf {
			seen = append(seen, c)
		}
	}
	tl, err := foldTimeline(seen)
	if err != nil {
		return false, err
	}
	return tl.AliveAt(FromNanos(t)), nil
}

// rowsContain finds the row that could contain t, the last that starts at or before
// it, and asks it.
func rowsContain(rows []Row, t int64) bool {
	i := sort.Search(len(rows), func(i int) bool { return rows[i].Start > t })
	return i > 0 && rows[i-1].Contains(t)
}

func (m *Model) neighbors(fp identity.Fingerprint, dir engine.Direction, t int64, sc engine.Scope) ([]engine.Neighbor, error) {
	var out []engine.Neighbor
	for _, st := range m.incident[dir][fp] {
		alive, err := m.liveAt(st, t, sc)
		if err != nil {
			return nil, err
		}
		if !alive {
			continue
		}
		peer := st.id.B
		if dir == engine.Reverse {
			peer = st.id.A
		}
		out = append(out, engine.Neighbor{Peer: peer, Relation: st.id.Relation})
	}
	engine.SortNeighbors(out)
	return out, nil
}

// Neighbors implements [engine.Engine].
func (m *Model) Neighbors(fp identity.Fingerprint, dir engine.Direction, t time.Time, sc engine.Scope) ([]engine.Neighbor, error) {
	if err := sc.Validate(); err != nil {
		return nil, err
	}
	ns, ok := ClampInstant(t)
	if !ok {
		return nil, nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.neighbors(fp, dir, ns, sc)
}

// NeighborsBatch implements [engine.Engine]. One lock covers the whole batch, so all
// the lists are of one snapshot, and an entity with no edges has an empty list.
func (m *Model) NeighborsBatch(fps []identity.Fingerprint, dir engine.Direction, t time.Time, sc engine.Scope) ([][]engine.Neighbor, error) {
	if err := sc.Validate(); err != nil {
		return nil, err
	}
	ns, ok := ClampInstant(t)
	m.mu.RLock()
	defer m.mu.RUnlock()
	return engine.NeighborsEach(fps, func(fp identity.Fingerprint) ([]engine.Neighbor, error) {
		if !ok {
			return nil, nil
		}
		return m.neighbors(fp, dir, ns, sc)
	})
}

// Alive implements [engine.Engine].
func (m *Model) Alive(fp identity.Fingerprint, t time.Time, sc engine.Scope) (bool, error) {
	if err := sc.Validate(); err != nil {
		return false, err
	}
	ns, ok := ClampInstant(t)
	if !ok {
		return false, nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	st := m.subjects[engine.EntitySubject(fp)]
	if st == nil {
		return false, nil
	}
	return m.liveAt(st, ns, sc)
}

// Window implements [engine.Engine]. It reads the log, the table every record is
// kept in, and no interval.
func (m *Model) Window(fp identity.Fingerprint, dir engine.Direction, from, to time.Time, sc engine.Scope) ([]engine.Record, error) {
	if err := sc.Validate(); err != nil {
		return nil, err
	}
	b := ClampWindow(from, to)
	if b.Empty {
		return nil, nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []engine.Record
	for _, st := range m.incident[dir][fp] {
		if st.layer != sc.Layer {
			continue
		}
		lo := sort.Search(len(st.recs), func(i int) bool { return st.recs[i].event >= b.Lo })
		for _, c := range st.recs[lo:] {
			if !b.Contains(c.event) {
				break
			}
			if c.seq <= sc.AsOf {
				out = append(out, c.record(st.id, st.layer))
			}
		}
	}
	engine.SortRecords(out)
	return out, nil
}

// Retain implements [engine.Engine]; see the package documentation. A horizon
// before the range changes nothing. One after the range discards every record and
// refuses every later one, as the reference engine does, though its rows stay as
// they were read at the last representable instant.
func (m *Model) Retain(horizon time.Time) error {
	h := int64(0)
	past := horizon.After(engine.MaxEventTime)
	switch {
	case past:
		h = math.MaxInt64
	case horizon.After(engine.MinEventTime):
		h = ToNanos(horizon)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	changed := false
	if past && !m.pastEnd {
		m.pastEnd, changed = true, true
	}
	if h > m.horizon {
		m.horizon, changed = h, true
	}
	if !changed {
		return nil
	}
	for _, st := range m.subjects {
		cut := sort.Search(len(st.recs), func(i int) bool { return st.recs[i].event >= h })
		if m.pastEnd {
			cut = len(st.recs)
		}
		if cut > 0 {
			// The baseline of a producer is the last of its records before the
			// horizon, among the baseline an earlier retention left and the log.
			last := make(map[lifecycle.Producer]rec)
			for _, c := range slices.Concat(st.base, st.recs[:cut]) {
				if prev, ok := last[c.producer]; !ok || prev.cmp(c) < 0 {
					last[c.producer] = c
				}
			}
			st.base = st.base[:0:0]
			for _, c := range last {
				st.base = append(st.base, c)
			}
			slices.SortFunc(st.base, func(a, b rec) int { return cmpProducers(a.producer, b.producer) })
			st.recs = slices.Clone(st.recs[cut:])
		}
		// The rows that have ended at or before the horizon are a prefix: the rows
		// of a subject end in the order they start.
		gone := sort.Search(len(st.rows), func(i int) bool { return !st.rows[i].EndsAtOrBefore(h) })
		if gone > 0 {
			m.counts.RowsDeleted += uint64(gone)
			st.rows = slices.Clone(st.rows[gone:])
		}
	}
	return nil
}

func cmpProducers(a, b lifecycle.Producer) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// Size implements [engine.Engine]. A model holds nothing on disk; this is the
// bytes of the rows it holds at a fixed nominal size per row, so that a caller that
// asks gets a number that grows with what is stored. It is not a measurement of
// Postgres.
func (m *Model) Size() (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var n int64
	for _, st := range m.subjects {
		for _, c := range slices.Concat(st.base, st.recs) {
			n += int64(len(c.payload)) + 96
		}
		n += int64(len(st.rows)) * 64
	}
	return n, nil
}

// Close implements [engine.Engine]. The model keeps what it holds, for [Open].
func (m *Model) Close() error { return nil }
