package pgmodel

import (
	"fmt"
	"math"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/lotannauo/toposhift/bench/postgres/pgschema"
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
	// FastSubjects are the subjects of a batch planned without a read, from what the
	// writer remembers; Refolds the subjects re-folded from the stored interval the batch
	// lands in, one per subject per batch, and RecordsFolded the records those folds read.
	FastSubjects, Refolds, RecordsFolded uint64
	// RowsInserted, RowsUpdated and RowsDeleted are the changes to the interval
	// tables. A row whose start changes is a delete and an insert, because the start
	// is part of the key.
	RowsInserted, RowsUpdated, RowsDeleted uint64
	// RowsHeapOnly are the updates that leave the range of the row as it was, so that no
	// index entry changes: the ones a server can make heap-only. RowsFinalized are the
	// updates that close a provisional row whose deadline has passed.
	RowsHeapOnly, RowsFinalized uint64
	// Vacuums and Analyzes are the vacuums and the analyzes the schedule of the
	// deterministic regime called for (a table that is due both gets one command and
	// counts in both), and PartitionsDropped the day partitions of the log a retention
	// dropped.
	Vacuums, Analyzes, PartitionsDropped uint64
}

func (c *Counts) add(o Counts) {
	c.Records += o.Records
	c.FastSubjects += o.FastSubjects
	c.Refolds += o.Refolds
	c.RecordsFolded += o.RecordsFolded
	c.RowsInserted += o.RowsInserted
	c.RowsUpdated += o.RowsUpdated
	c.RowsDeleted += o.RowsDeleted
	c.RowsHeapOnly += o.RowsHeapOnly
	c.RowsFinalized += o.RowsFinalized
	c.Vacuums += o.Vacuums
	c.Analyzes += o.Analyzes
	c.PartitionsDropped += o.PartitionsDropped
}

// ModelConfig says which writer a Model models.
type ModelConfig struct {
	WriterConfig
	// Partitioned says the log is partitioned by day, as the streaming arm's is: a
	// retention drops the partitions that are wholly before the horizon, and the vacuum
	// schedule counts each partition as a table. Otherwise the log is one table and a
	// retention deletes from it.
	Partitioned bool
}

// partition is a day partition of the log, as the writer creates them.
type partition struct {
	lo, hi    int64
	unbounded bool
}

// Model is an in-memory engine with the schema and the read semantics of the
// Postgres interval tables (see the package documentation). It implements
// [engine.Engine]. Reads run concurrently with each other; a read sees whole
// committed batches, because Write and Retain hold the lock for their whole
// effect.
type Model struct {
	mu      sync.RWMutex
	cfg     ModelConfig
	lastSeq uint64
	horizon int64 // Unix nanoseconds; records before it are refused
	// pastEnd says the horizon was moved after the last representable instant, so
	// every record, that instant's included, is before it. horizon alone cannot say
	// it: it is an int64.
	pastEnd  bool
	subjects map[engine.Subject]*subject
	incident [3]map[identity.Fingerprint][]*subject // by direction: entity -> its edges
	w        *Writer
	vac      *VacuumSchedule
	parts    map[string]partition
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
	// rows are the interval rows as stored, ascending by start. The rows of a subject
	// are maximal, so they neither overlap nor touch, and their ends ascend too.
	rows []Row
}

// New returns an empty model of the streaming arm.
func New() *Model { return NewWith(ModelConfig{Partitioned: true}) }

// NewWith returns an empty model of the writer cfg describes.
func NewWith(cfg ModelConfig) *Model {
	m := &Model{
		cfg: cfg, subjects: make(map[engine.Subject]*subject), w: NewWriter(cfg.WriterConfig),
		vac: NewVacuumSchedule(), parts: make(map[string]partition),
	}
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

// VacuumRuns is how many times the schedule of the deterministic regime has called for
// each table to be vacuumed.
func (m *Model) VacuumRuns() map[string]int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.vac.Runs()
}

// AnalyzeRuns is how many times the schedule has called for each table to be analyzed.
func (m *Model) AnalyzeRuns() map[string]int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.vac.AnalyzeRuns()
}

// logTable is the table the log row of an event time is in, for the vacuum schedule.
func (m *Model) logTable(ns int64) (string, error) {
	if !m.cfg.Partitioned {
		return "log", nil
	}
	name, lo, hi, unbounded, err := pgschema.DayBounds(ns)
	if err != nil {
		return "", err
	}
	if _, ok := m.parts[name]; !ok {
		m.parts[name] = partition{lo: lo, hi: hi, unbounded: unbounded}
	}
	return name, nil
}

// Write implements [engine.Engine]. The batch is validated whole before any of it is
// applied, so a refused batch changes nothing.
func (m *Model) Write(batch []engine.Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	seq := m.lastSeq
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
	}
	if len(batch) == 0 {
		return nil
	}
	plan, err := m.w.Plan(batch)
	if err != nil {
		return err
	}
	for _, id := range plan.Slow() {
		old, input := m.slowInputs(m.subjects[id], plan.Earliest(id))
		if err := plan.Resolve(id, old, input); err != nil {
			plan.Abort()
			// The batch was validated, so a fold that fails is a bug in the model, and
			// the model is no longer consistent.
			return fmt.Errorf("pgmodel: applying a batch: %w", err)
		}
	}
	changes, err := plan.Finish()
	if err != nil {
		return fmt.Errorf("pgmodel: applying a batch: %w", err)
	}

	// Store the records, in the order the subjects first appear, then the rows.
	logged := make(map[string]int64)
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
		i := sort.Search(len(st.recs), func(i int) bool { return st.recs[i].cmp(c) > 0 })
		st.recs = slices.Insert(st.recs, i, c)
		table, err := m.logTable(c.event)
		if err != nil {
			plan.Abort()
			return err
		}
		logged[table]++
	}
	var edgeRows, entityRows [3]int64 // inserted, updated, deleted
	for _, sc := range changes {
		st := m.subjects[sc.Subject]
		st.rows = applyRows(st.rows, sc.Rows)
		tally := &edgeRows
		if sc.Subject.Kind == engine.SubjectEntity {
			tally = &entityRows
		}
		for _, c := range sc.Rows {
			tally[c.Kind-Insert]++
		}
	}
	for table, n := range logged {
		m.vac.Add(table, n, 0, 0)
	}
	m.vac.Add("edges", edgeRows[0], edgeRows[1], edgeRows[2])
	m.vac.Add("entities", entityRows[0], entityRows[1], entityRows[2])
	m.counts.add(plan.Counts())
	m.counts.Records += uint64(len(batch))
	m.lastSeq = seq
	plan.Commit()
	m.vacuum()
	return nil
}

// vacuum counts the vacuums and the analyzes the schedule calls for.
func (m *Model) vacuum() {
	for _, a := range m.vac.Due() {
		if a.Vacuum {
			m.counts.Vacuums++
		}
		if a.Analyze {
			m.counts.Analyzes++
		}
	}
}

// slowInputs are what a re-fold of a subject reads: its stored rows from the last that
// starts at or before earliest, and every record of its log, baselines included, from
// where that row starts.
func (m *Model) slowInputs(st *subject, earliest int64) ([]Stored, []LogRow) {
	if st == nil {
		return nil, nil
	}
	from := sort.Search(len(st.rows), func(i int) bool { return st.rows[i].Start > earliest }) - 1
	start := int64(math.MinInt64)
	if from >= 0 {
		start = st.rows[from].Start
	} else {
		from = 0
	}
	old := make([]Stored, 0, len(st.rows)-from)
	for _, r := range st.rows[from:] {
		old = append(old, Stored{Row: r})
	}
	// A record before start cannot matter: the segment it starts or ends is before it,
	// because start begins a maximal interval and so nothing before it overlaps or
	// touches it. A baseline before start must be left out, not just because it is not
	// needed: without the later record of its producer that ended it, it would run on.
	var input []LogRow
	for _, c := range st.base {
		if c.event >= start {
			input = append(input, c.logRow())
		}
	}
	lo := sort.Search(len(st.recs), func(i int) bool { return st.recs[i].event >= start })
	for _, c := range st.recs[lo:] {
		input = append(input, c.logRow())
	}
	return old, input
}

// applyRows applies the changes of a subject to its rows, ascending by start.
func applyRows(rows []Row, changes []RowChange) []Row {
	rows = slices.Clone(rows)
	at := func(start int64) int {
		return sort.Search(len(rows), func(i int) bool { return rows[i].Start >= start })
	}
	for _, c := range changes {
		if c.Kind == Delete {
			i := at(c.Old.Start)
			rows = slices.Delete(rows, i, i+1)
		}
	}
	for _, c := range changes {
		switch c.Kind {
		case Update:
			rows[at(c.Old.Start)] = c.New
		case Insert:
			rows = slices.Insert(rows, at(c.New.Start), c.New)
		}
	}
	return rows
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
	var baselines, trimmed int64
	var edgesGone, entitiesGone int64
	removed := make(map[string]int64) // log rows before the horizon, by table
	for _, st := range m.subjects {
		cut := sort.Search(len(st.recs), func(i int) bool { return st.recs[i].event >= h })
		if m.pastEnd {
			cut = len(st.recs)
		}
		if cut > 0 {
			// The baseline of a producer is the last of its records before the
			// horizon, among the baseline an earlier retention left and the log.
			last := make(map[lifecycle.Producer]rec)
			fresh := make(map[lifecycle.Producer]bool)
			for _, c := range st.recs[:cut] {
				fresh[c.producer] = true
			}
			for _, c := range slices.Concat(st.base, st.recs[:cut]) {
				if prev, ok := last[c.producer]; !ok || prev.cmp(c) < 0 {
					last[c.producer] = c
				}
			}
			for _, b := range st.base {
				if fresh[b.producer] {
					trimmed++
				}
			}
			baselines += int64(len(fresh))
			st.base = st.base[:0:0]
			for _, c := range last {
				st.base = append(st.base, c)
			}
			slices.SortFunc(st.base, func(a, b rec) int { return cmpProducers(a.producer, b.producer) })
			for _, c := range st.recs[:cut] {
				table, err := m.logTable(c.event)
				if err != nil {
					return err
				}
				removed[table]++
			}
			st.recs = slices.Clone(st.recs[cut:])
		}
		// The rows that have ended at or before the horizon are a prefix: the rows
		// of a subject end in the order they start.
		gone := sort.Search(len(st.rows), func(i int) bool { return !st.rows[i].EndsAtOrBefore(h) })
		if gone > 0 {
			m.counts.RowsDeleted += uint64(gone)
			if st.id.Kind == engine.SubjectEdge {
				edgesGone += int64(gone)
			} else {
				entitiesGone += int64(gone)
			}
			st.rows = slices.Clone(st.rows[gone:])
		}
	}
	m.w.CommitRetain(h)
	m.vac.Add("edges", 0, 0, edgesGone)
	m.vac.Add("entities", 0, 0, entitiesGone)
	m.vac.Add("log_base", baselines, 0, trimmed)
	// A partition wholly before the horizon is dropped, with its rows; the one the
	// horizon falls in has its earlier rows deleted. An unpartitioned log is deleted from.
	for name, p := range m.parts {
		if m.pastEnd || !p.unbounded && p.hi <= h {
			delete(m.parts, name)
			m.vac.Drop(name)
			m.counts.PartitionsDropped++
			delete(removed, name)
		}
	}
	for table, n := range removed {
		m.vac.Add(table, 0, 0, n)
	}
	m.vacuum()
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
