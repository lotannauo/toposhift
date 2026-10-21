package xtmodel

import (
	"fmt"
	"math"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// ReadCounts is how the reads a Model has answered were answered.
type ReadCounts struct {
	// Native are the entity reads (one per entity of a batched read) answered from the
	// segment or log tables at a system time or at the newest, which is what the
	// baseline's server reads do.
	Native uint64
	// ClientFold are the reads at a token inside a batch, which no system time names:
	// they are answered from the log by folding it in Go.
	ClientFold uint64
}

// Model is an in-memory XTDB for the baseline's tables: the bitemporal store, the
// [Planner] that writes it, and the reads of the baseline's queries over it. It
// implements [engine.Engine]. Reads run concurrently with each other; a read sees
// whole committed batches, because Write holds the lock for a batch's whole effect.
//
// The system time axis is the batch ordinal ([SystemTime]), so a token that ended a
// batch reads at that batch's system time and a token inside a batch (which a
// system time cannot name) is answered by a client fold of the log. The valid time
// axis is each producer's lifecycle segments, so a read at an instant is a read of
// valid time.
type Model struct {
	mu      sync.RWMutex
	img     *image
	pl      *Planner
	lastSeq uint64
	batches uint64            // ordinals handed out: the n of the last committed batch
	tokens  map[uint64]uint64 // the Seq a committed batch ended at, to its ordinal
	counts  Counts

	native, folded atomic.Uint64

	// mut and beforeCommit are for the tests: a deliberate fault, and a hook that can
	// refuse a transaction the way a server can.
	mut          mutation
	beforeCommit func(*Txn) error
}

var _ engine.Engine = (*Model)(nil)

// New returns an empty model.
func New() *Model { return fromImage(newImage()) }

// fromImage is a model over what an image holds: the token of the last batch, the
// ordinal of the next and the map from tokens to ordinals are read from the meta
// documents, and the planner's memory from the log.
func fromImage(img *image) *Model {
	m := &Model{img: img, tokens: make(map[uint64]uint64, len(img.metas))}
	for _, d := range img.metas {
		m.batches = max(m.batches, d.n)
		m.lastSeq = max(m.lastSeq, d.lastSeq)
		m.tokens[d.lastSeq] = d.n
	}
	m.pl = img.planner()
	return m
}

// setMutation switches on a fault in the model and its planner.
func (m *Model) setMutation(x mutation) {
	m.mut = x
	m.pl.mut = x
}

// open holds what each directory keeps: a model has no files, so a directory is only
// a name, and a model opened again under it finds what the last one left.
var open sync.Map

// Open returns a model over what dir holds, which is empty the first time. It is the
// form a conformance factory needs, which closes an engine and opens it again over the
// same directory and requires everything back; the model it returns has rebuilt its
// state from the image, as an engine over a server would. One model per directory may
// be in use at a time. Forget releases the directory.
func Open(dir string) *Model {
	img, ok := open.Load(dir)
	if !ok {
		img, _ = open.LoadOrStore(dir, newImage())
	}
	return fromImage(img.(*image))
}

// Forget drops what Open keeps for dir.
func Forget(dir string) { open.Delete(dir) }

// Counts returns what the planner has planned so far.
func (m *Model) Counts() Counts {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.counts
}

// ReadCounts returns how the reads so far were answered.
func (m *Model) ReadCounts() ReadCounts {
	return ReadCounts{Native: m.native.Load(), ClientFold: m.folded.Load()}
}

// SystemTimeOf returns the system time of the batch that ended at token, and whether a
// committed batch ended there.
func (m *Model) SystemTimeOf(token uint64) (Instant, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	n, ok := m.tokens[token]
	return SystemTime(n), ok
}

// check validates a batch whole, so that a refused batch changes nothing.
func (m *Model) check(batch []engine.Record) error {
	seq := m.lastSeq
	inBatch := make(map[engine.Subject]catalog.Layer)
	for _, r := range batch {
		if err := r.Validate(); err != nil {
			return err
		}
		if m.mut != mutAcceptBeforeHorizon && (m.img.pastEnd || r.EventTime.UnixNano() < m.img.horizon) {
			return fmt.Errorf("record seq %d at %s: %w", r.Seq, r.EventTime.Format(time.RFC3339), engine.ErrBeforeHorizon)
		}
		if r.Seq <= seq {
			return fmt.Errorf("record seq %d is not above the last written seq %d: %w", r.Seq, seq, engine.ErrInvalid)
		}
		seq = r.Seq
		known, ok := inBatch[r.Subject]
		if !ok {
			known, ok = m.img.layers[r.Subject]
		}
		if ok && known != r.Layer {
			return fmt.Errorf("record seq %d: subject is stored in layer %s, not %s: %w", r.Seq, known, r.Layer, engine.ErrInvalid)
		}
		inBatch[r.Subject] = r.Layer
	}
	return nil
}

// Write implements [engine.Engine]: the batch is one transaction, at the system time of
// the next ordinal. The planner and the counters change, and LastSeq rises, only after
// the transaction has committed, so a batch the server refuses uses neither an ordinal
// nor a sequence number.
func (m *Model) Write(batch []engine.Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(batch) == 0 {
		return nil
	}
	if err := m.check(batch); err != nil {
		return err
	}
	n := m.batches + 1
	pl, err := m.pl.Plan(n, batch)
	if err != nil {
		return err
	}
	pl.Txn.System = SystemTime(n)
	if m.mut == mutRepeatSystemTime && m.batches > 0 {
		pl.Txn.System = SystemTime(m.batches)
	}
	last := batch[len(batch)-1].Seq
	if m.mut == mutLastSeqEarly {
		m.lastSeq = last
	}
	if m.beforeCommit != nil {
		if err := m.beforeCommit(&pl.Txn); err != nil {
			return m.refused(n, err)
		}
	}
	if err := m.img.apply(pl.Txn); err != nil {
		// The batch was valid, so a transaction the store refuses is a bug in the
		// planner or the ordinals.
		return m.refused(n, fmt.Errorf("xtmodel: committing a batch: %w", err))
	}
	m.pl.Accept(pl)
	m.batches = n
	m.lastSeq = last
	m.tokens[last] = n
	m.counts.add(pl.Counts)
	return nil
}

func (m *Model) refused(n uint64, err error) error {
	if m.mut == mutFailedBatchTakesOrdinal {
		m.batches = n
	}
	return err
}

// LastSeq implements [engine.Engine].
func (m *Model) LastSeq() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.lastSeq
}

// resolve says how a token is read. A token at or above the newest batch's is the
// newest system time, which takes no clause; a token that ended a batch is that
// batch's system time; any other (inside a batch, or before the first) is not a
// system time, and is answered from the log with fold true.
func (m *Model) resolve(asOf uint64) (v view, fold bool) {
	switch {
	case m.mut == mutNoSystemTime || asOf >= m.lastSeq:
		return view{latest: true}, false
	case m.mut == mutMidBatchBySystemTime:
		i := sort.Search(len(m.img.metas), func(k int) bool { return m.img.metas[k].lastSeq > asOf })
		if i == 0 {
			return view{sys: BaseSystemTime}, false
		}
		return view{sys: m.img.metas[i-1].sys}, false
	}
	if n, ok := m.tokens[asOf]; ok {
		return view{sys: SystemTime(n)}, false
	}
	return view{}, true
}

// layerOK is the `layer = $L` clause.
func (m *Model) layerOK(have, want catalog.Layer) bool {
	return m.mut == mutNoLayerFilter || have == want
}

// neighbors answers one entity's reads: the edges alive at t that touch fp in the
// direction, in the scope.
func (m *Model) neighbors(fp identity.Fingerprint, dir engine.Direction, at time.Time, t Instant, sc engine.Scope) ([]engine.Neighbor, error) {
	v, fold := m.resolve(sc.AsOf)
	var out []engine.Neighbor
	if fold {
		m.folded.Add(1)
		subjects, err := m.foldSubjects(fp, dir, sc, at)
		if err != nil {
			return nil, err
		}
		for _, s := range subjects {
			out = append(out, peerOf(s, dir))
		}
	} else {
		m.native.Add(1)
		tbl := SegF
		if dir == engine.Reverse {
			tbl = SegR
		}
		for _, d := range m.img.incident[tbl][fp] {
			if m.layerOK(d.layer, sc.Layer) && d.at(t, v) {
				out = append(out, peerOf(d.key.Subject, dir))
			}
		}
	}
	// Several producers can make one edge alive.
	engine.SortNeighbors(out)
	return slices.Compact(out), nil
}

func peerOf(s engine.Subject, dir engine.Direction) engine.Neighbor {
	if dir == engine.Reverse {
		return engine.Neighbor{Peer: s.A, Relation: s.Relation}
	}
	return engine.Neighbor{Peer: s.B, Relation: s.Relation}
}

// foldSubjects is the client fold: the edge subjects touching fp that are alive at
// the instant when only the records with Seq at or below the token are visible,
// found by folding each subject's records of the log with the lifecycle
// specification.
func (m *Model) foldSubjects(fp identity.Fingerprint, dir engine.Direction, sc engine.Scope, at time.Time) ([]engine.Subject, error) {
	tbl := LogF
	if dir == engine.Reverse {
		tbl = LogR
	}
	var order []engine.Subject
	by := make(map[engine.Subject][]lifecycle.Assertion)
	for _, e := range m.img.logs[tbl][fp] {
		if !m.layerOK(e.rec.Layer, sc.Layer) || e.rec.Seq > sc.AsOf {
			continue
		}
		if _, seen := by[e.rec.Subject]; !seen {
			order = append(order, e.rec.Subject)
		}
		by[e.rec.Subject] = append(by[e.rec.Subject], e.rec.assertion())
	}
	var alive []engine.Subject
	for _, s := range order {
		tl, err := lifecycle.Fold(by[s], lifecycle.Policy{})
		if err != nil {
			return nil, fmt.Errorf("xtmodel: %w", err)
		}
		if tl.AliveAt(at) {
			alive = append(alive, s)
		}
	}
	return alive, nil
}

// Neighbors implements [engine.Engine].
func (m *Model) Neighbors(fp identity.Fingerprint, dir engine.Direction, at time.Time, sc engine.Scope) ([]engine.Neighbor, error) {
	if err := sc.Validate(); err != nil {
		return nil, err
	}
	t, ok := ClampInstant(at)
	if !ok {
		return nil, nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.neighbors(fp, dir, at, t, sc)
}

// NeighborsBatch implements [engine.Engine]. One lock covers the whole batch, so all
// the lists are of one snapshot, and an entity with no edges has an empty list.
func (m *Model) NeighborsBatch(fps []identity.Fingerprint, dir engine.Direction, at time.Time, sc engine.Scope) ([][]engine.Neighbor, error) {
	if err := sc.Validate(); err != nil {
		return nil, err
	}
	t, ok := ClampInstant(at)
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([][]engine.Neighbor, 0, len(fps))
	for _, fp := range fps {
		var ns []engine.Neighbor
		if ok {
			var err error
			if ns, err = m.neighbors(fp, dir, at, t, sc); err != nil {
				return nil, err
			}
		}
		if len(ns) == 0 && m.mut == mutBatchDropsEmpty {
			continue
		}
		out = append(out, ns)
	}
	return out, nil
}

// Alive implements [engine.Engine].
func (m *Model) Alive(fp identity.Fingerprint, at time.Time, sc engine.Scope) (bool, error) {
	if err := sc.Validate(); err != nil {
		return false, err
	}
	t, ok := ClampInstant(at)
	if !ok {
		return false, nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	v, fold := m.resolve(sc.AsOf)
	if fold {
		m.folded.Add(1)
		var as []lifecycle.Assertion
		for _, e := range m.img.logs[LogE][fp] {
			if m.layerOK(e.rec.Layer, sc.Layer) && e.rec.Seq <= sc.AsOf {
				as = append(as, e.rec.assertion())
			}
		}
		tl, err := lifecycle.Fold(as, lifecycle.Policy{})
		if err != nil {
			return false, fmt.Errorf("xtmodel: %w", err)
		}
		return tl.AliveAt(at), nil
	}
	m.native.Add(1)
	for _, d := range m.img.incident[Ent][fp] {
		if m.layerOK(d.layer, sc.Layer) && d.at(t, v) {
			return true, nil
		}
	}
	return false, nil
}

// Window implements [engine.Engine]. It reads the log tables over all of valid time,
// which hold every record, those a later record at the same instant overwrites in
// effect included.
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
	v, fold := m.resolve(sc.AsOf)
	if fold {
		m.folded.Add(1)
	} else {
		m.native.Add(1)
	}
	tbl := LogF
	if dir == engine.Reverse {
		tbl = LogR
	}
	l := m.img.logs[tbl][fp]
	lo := sort.Search(len(l), func(i int) bool { return l[i].rec.Event >= b.Lo })
	var out []engine.Record
	for _, e := range l[lo:] {
		if !b.Contains(e.rec.Event) {
			break
		}
		if !m.layerOK(e.rec.Layer, sc.Layer) {
			continue
		}
		visible := e.rec.Seq <= sc.AsOf
		if !fold {
			visible = v.latest || e.sys <= v.sys
		}
		if visible {
			out = append(out, e.rec.Record())
		}
	}
	if m.mut == mutWindowDedup {
		out = dedupe(out)
	}
	engine.SortRecords(out)
	return out, nil
}

// dedupe keeps, of the records of one subject and producer at one instant, the last.
func dedupe(rs []engine.Record) []engine.Record {
	type k struct {
		s engine.Subject
		p lifecycle.Producer
		t time.Time
	}
	last := make(map[k]uint64)
	for _, r := range rs {
		key := k{r.Subject, r.Producer, r.EventTime}
		last[key] = max(last[key], r.Seq)
	}
	return slices.DeleteFunc(rs, func(r engine.Record) bool {
		return last[k{r.Subject, r.Producer, r.EventTime}] != r.Seq
	})
}

// Retain implements [engine.Engine]. XTDB's only removal is ERASE, which removes a
// document for all valid and system time, so one document's old valid-time portions
// cannot be dropped while its later ones stay: retention is not mapped. Retain records
// the horizon, from which Write refuses records before it, and frees nothing, which
// the contract allows. A horizon before the range changes nothing; one after the range
// refuses every later record, as the reference engine does.
func (m *Model) Retain(horizon time.Time) error {
	h := int64(0)
	past := horizon.After(engine.MaxEventTime)
	switch {
	case past:
		h = math.MaxInt64
	case horizon.After(engine.MinEventTime):
		h = horizon.UnixNano()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if past {
		m.img.pastEnd = true
	}
	m.img.horizon = max(m.img.horizon, h)
	return nil
}

// Size implements [engine.Engine]. A model holds nothing on disk; this is the bytes
// of the documents it holds at a fixed nominal size each (a log document, its
// payload, a version of a segment document, the closed ones included), so that a
// caller that asks gets a number that grows with what is stored. It is not a
// measurement of XTDB.
func (m *Model) Size() (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var n int64
	for t := range NumTables {
		for _, l := range m.img.logs[t] {
			for _, e := range l {
				n += int64(len(e.rec.Payload)) + 96
			}
		}
	}
	for _, d := range m.img.docs {
		n += int64(len(d.live)+len(d.closed)) * 64
	}
	return n, nil
}

// Close implements [engine.Engine]. The model keeps what it holds, for [Open].
func (m *Model) Close() error { return nil }
