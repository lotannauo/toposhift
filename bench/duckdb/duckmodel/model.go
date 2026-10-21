package duckmodel

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
)

// Arm says which files a [Model] stands for.
type Arm uint8

const (
	// ArmRecords is the record files: every read is answered from the records.
	ArmRecords Arm = iota + 1
	// ArmIntervals is the interval file with the record files behind it: a read as of
	// the newest token is answered from the intervals, and every other read from the
	// records.
	ArmIntervals
)

func (a Arm) String() string {
	switch a {
	case ArmRecords:
		return "records"
	case ArmIntervals:
		return "intervals"
	}
	return fmt.Sprintf("Arm(%d)", uint8(a))
}

// Model is an in-memory engine with the read semantics of one arm's files (see the
// package documentation). It implements [engine.Engine]. Reads run concurrently with
// each other; a read sees whole committed batches, because Write and Retain hold the
// lock for their whole effect.
type Model struct {
	arm     Arm
	mut     mutant
	mu      sync.RWMutex
	lastSeq uint64
	horizon int64 // Unix nanoseconds; records before it are refused
	// pastEnd says the horizon was moved after the last representable instant, so
	// every record, that instant's included, is before it. horizon alone cannot say
	// it: it is an int64.
	pastEnd  bool
	subjects map[engine.Subject]*subject
	incident [3]map[identity.Fingerprint][]*subject // by direction: entity -> its edges
}

var _ engine.Engine = (*Model)(nil)

// subject is what the model keeps for one subject: its layer and its rows.
type subject struct {
	id    engine.Subject
	layer catalog.Layer
	// recs are the records of the subject, ascending as a file sorted by producer, event
	// time and Seq has them. After a retention they hold the baselines too.
	recs []rec

	// fold guards ivs and stale, which a read refreshes while others read.
	fold sync.Mutex
	// ivs are the intervals folded from recs, valid unless stale.
	ivs   []Interval
	stale bool
}

// intervals returns the maximal intervals of the subject, folding its records if they
// have changed since the last time. The slice is never changed afterwards.
func (st *subject) intervals(mut mutant) ([]Interval, error) {
	st.fold.Lock()
	defer st.fold.Unlock()
	if st.stale {
		ivs, err := foldIntervals(st.recs, mut)
		if err != nil {
			return nil, err
		}
		st.ivs, st.stale = ivs, false
	}
	return st.ivs, nil
}

// New returns an empty model of an arm's files.
func New(arm Arm) *Model { return newModel(arm, mutNone) }

func newModel(arm Arm, mut mutant) *Model {
	m := &Model{arm: arm, mut: mut, subjects: make(map[engine.Subject]*subject)}
	m.incident[engine.Forward] = make(map[identity.Fingerprint][]*subject)
	m.incident[engine.Reverse] = make(map[identity.Fingerprint][]*subject)
	return m
}

// Arm returns the arm the model stands for.
func (m *Model) Arm() Arm { return m.arm }

type openKey struct {
	dir string
	arm Arm
}

// open models are what a directory holds: a model has no files, so a directory is only
// a name, and a model opened again under it finds what the last one left.
var open sync.Map

// Open returns the model of an arm that dir names, a new one the first time. It is the
// form a conformance factory needs, which closes an engine and opens it again over the
// same directory and requires everything back. Forget releases it.
func Open(dir string, arm Arm) *Model {
	m, _ := open.LoadOrStore(openKey{dir, arm}, New(arm))
	return m.(*Model)
}

// Forget drops the model Open returned for dir and arm.
func Forget(dir string, arm Arm) { open.Delete(openKey{dir, arm}) }

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
		c := keep(r)
		i := sort.Search(len(st.recs), func(i int) bool { return st.recs[i].cmpInFile(c) > 0 })
		st.recs = slices.Insert(st.recs, i, c)
		st.stale = true
	}
	m.lastSeq = seq
	return nil
}

// LastSeq implements [engine.Engine].
func (m *Model) LastSeq() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.lastSeq
}

// instant converts the instant of a read.
func (m *Model) instant(t time.Time) (int64, bool) {
	if m.mut == mutNoClamp {
		return t.UnixNano(), true
	}
	return ClampInstant(t)
}

// holdsAt reports whether the subject holds at t, as the scope sees it. The newest token
// is answered from the intervals in the intervals arm; every other read is answered by
// the read rule over the records.
func (m *Model) holdsAt(st *subject, t int64, sc engine.Scope) (bool, error) {
	if st.layer != sc.Layer {
		return false, nil
	}
	if m.arm == ArmIntervals && sc.AsOf >= m.lastSeq {
		ivs, err := st.intervals(m.mut)
		if err != nil {
			return false, err
		}
		return intervalsContain(ivs, t), nil
	}
	return holds(st.recs, t, sc.AsOf, m.mut), nil
}

func (m *Model) neighbors(fp identity.Fingerprint, dir engine.Direction, t int64, sc engine.Scope) ([]engine.Neighbor, error) {
	var out []engine.Neighbor
	for _, st := range m.incident[dir][fp] {
		alive, err := m.holdsAt(st, t, sc)
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
	ns, ok := m.instant(t)
	if !ok {
		return nil, nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.neighbors(fp, dir, ns, sc)
}

// NeighborsBatch implements [engine.Engine]. One lock covers the whole batch, so all the
// lists are of one snapshot, and an entity with no edges has an empty list.
func (m *Model) NeighborsBatch(fps []identity.Fingerprint, dir engine.Direction, t time.Time, sc engine.Scope) ([][]engine.Neighbor, error) {
	if err := sc.Validate(); err != nil {
		return nil, err
	}
	ns, ok := m.instant(t)
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.mut == mutBatchSorted {
		fps = slices.SortedFunc(slices.Values(fps), engine.CompareFingerprints)
	}
	out, err := engine.NeighborsEach(fps, func(fp identity.Fingerprint) ([]engine.Neighbor, error) {
		if !ok {
			return nil, nil
		}
		return m.neighbors(fp, dir, ns, sc)
	})
	if m.mut == mutBatchDropsEmpty {
		out = slices.DeleteFunc(out, func(l []engine.Neighbor) bool { return len(l) == 0 })
	}
	return out, err
}

// Alive implements [engine.Engine].
func (m *Model) Alive(fp identity.Fingerprint, t time.Time, sc engine.Scope) (bool, error) {
	if err := sc.Validate(); err != nil {
		return false, err
	}
	ns, ok := m.instant(t)
	if !ok {
		return false, nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	st := m.subjects[engine.EntitySubject(fp)]
	if st == nil {
		return false, nil
	}
	return m.holdsAt(st, ns, sc)
}

// Window implements [engine.Engine]. It reads the records, in either arm: an interval
// has lost the records it was made from.
func (m *Model) Window(fp identity.Fingerprint, dir engine.Direction, from, to time.Time, sc engine.Scope) ([]engine.Record, error) {
	if err := sc.Validate(); err != nil {
		return nil, err
	}
	b := ClampWindow(from, to)
	if b.Empty {
		return nil, nil
	}
	if m.mut == mutWindowInclusive && !b.Unbounded {
		b.Hi++
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []engine.Record
	for _, st := range m.incident[dir][fp] {
		if st.layer != sc.Layer {
			continue
		}
		for i, c := range st.recs {
			if !b.Contains(c.event) || c.seq > sc.AsOf {
				continue
			}
			if m.mut == mutWindowDedups && i+1 < len(st.recs) {
				if next := st.recs[i+1]; next.producer == c.producer && next.event == c.event && next.seq <= sc.AsOf {
					continue
				}
			}
			out = append(out, c.record(st.id, st.layer))
		}
	}
	engine.SortRecords(out)
	return out, nil
}

// Retain implements [engine.Engine]; see the package documentation. A horizon before
// the range changes nothing. One after the range discards every record and refuses every
// later one, as the reference engine does, though what stands for them, each producer's
// last record, stays, so a read at the last representable instant still finds the state
// after every record.
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
		kept := retainRecs(st.recs, m.horizon, m.pastEnd, m.mut)
		if len(kept) != len(st.recs) {
			st.recs = kept
			st.stale = true
		}
	}
	return nil
}

// Records returns every record the model holds, baselines included, ascending by Seq.
func (m *Model) Records() []engine.Record {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []engine.Record
	for _, st := range m.subjects {
		for _, c := range st.recs {
			out = append(out, c.record(st.id, st.layer))
		}
	}
	slices.SortFunc(out, func(a, b engine.Record) int { return cmpSeq(a.Seq, b.Seq) })
	return out
}

func cmpSeq(a, b uint64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// Stats is what a model holds, in the units a file is counted in. It is a function of
// the stream, so it is the same on every run and every machine.
type Stats struct {
	// Subjects are the entities and edges that have a record, and Edges the edges
	// among them.
	Subjects, Edges int
	// Records are the rows the record files hold, baselines included.
	Records int
	// Intervals are the rows of the interval file, counted once for each subject (an
	// edge has a row for each direction in the file, see [Model.EdgeIntervals]). It is
	// zero in the records arm, which holds no interval file.
	Intervals int
}

// Stats counts what the model holds, folding every subject whose intervals are stale in
// the intervals arm.
func (m *Model) Stats() (Stats, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var s Stats
	for _, st := range m.subjects {
		s.Subjects++
		if st.id.Kind == engine.SubjectEdge {
			s.Edges++
		}
		s.Records += len(st.recs)
		if m.arm != ArmIntervals {
			continue
		}
		ivs, err := st.intervals(m.mut)
		if err != nil {
			return Stats{}, err
		}
		s.Intervals += len(ivs)
	}
	return s, nil
}

// EdgeIntervals returns the rows of the interval file of edges, as seen from the source
// (Forward) or from the target (Reverse), in the file's order: by layer, then the text of
// the fingerprint they are seen from, then start. After a retention the rows are exact
// for an instant at or after the horizon only.
func (m *Model) EdgeIntervals(dir engine.Direction) ([]IntervalRow, error) {
	if dir != engine.Forward && dir != engine.Reverse {
		return nil, fmt.Errorf("duckmodel: direction %s: %w", dir, engine.ErrInvalid)
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	var rows []IntervalRow
	for _, st := range m.subjects {
		if st.id.Kind != engine.SubjectEdge {
			continue
		}
		ivs, err := st.intervals(m.mut)
		if err != nil {
			return nil, err
		}
		a, peer := Text(st.id.A), Text(st.id.B)
		if dir == engine.Reverse {
			a, peer = peer, a
		}
		for _, iv := range ivs {
			rows = append(rows, IntervalRow{Layer: st.layer, A: a, Peer: peer, Relation: st.id.Relation, Interval: iv})
		}
	}
	sortRows(rows)
	return rows, nil
}

// EntityIntervals returns the rows of the interval file of entities, in the file's order.
func (m *Model) EntityIntervals() ([]IntervalRow, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var rows []IntervalRow
	for _, st := range m.subjects {
		if st.id.Kind != engine.SubjectEntity {
			continue
		}
		ivs, err := st.intervals(m.mut)
		if err != nil {
			return nil, err
		}
		for _, iv := range ivs {
			rows = append(rows, IntervalRow{Layer: st.layer, A: Text(st.id.A), Interval: iv})
		}
	}
	sortRows(rows)
	return rows, nil
}

// Size implements [engine.Engine]. A model holds nothing on disk; this is the bytes of
// the rows it holds at a fixed nominal size per row (and, in the intervals arm, per
// interval), so that a caller that asks gets a number that grows with what is stored.
// It is not a measurement of any file.
func (m *Model) Size() (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var n int64
	for _, st := range m.subjects {
		for _, c := range st.recs {
			n += int64(len(c.payload)) + 96
		}
		if m.arm == ArmIntervals {
			ivs, err := st.intervals(m.mut)
			if err != nil {
				return 0, err
			}
			n += int64(len(ivs)) * 64
		}
	}
	return n, nil
}

// Close implements [engine.Engine]. The model keeps what it holds, for [Open].
func (m *Model) Close() error { return nil }
