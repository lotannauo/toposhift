package doltmodel

import (
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/identity"
)

// Path is which tables a read is answered from.
type Path uint8

const (
	// PathKey is the design's rule, the same for every read whatever its age. The state
	// table is read as the commit at the token left it (the current state for the newest
	// token). Each state key, a subject in one direction and one producer, is then settled
	// one way or the other. A row whose event time is at or before the instant is the key's
	// newest record at or before it among those the token sees, because the row is the
	// newest of all of them: the row answers. A row whose event time is after the instant
	// says nothing about the instant, and the key is answered from the log. A key with no
	// row holds nothing at any instant from the horizon on. A token that names no commit
	// has no state to read, and every key is answered from the log.
	PathKey Path = iota + 1
	// PathLog answers every key from the log: for each producer the newest record at or
	// before the instant with a sequence number at or below the token, whatever the instant
	// and the token. It is the reference the rule is checked against.
	PathLog
	// PathCommitTime is the diagnostic arm's: the state as the newest commit whose
	// frontier is at or before the instant left it, as if a commit's place in history
	// were an event time. It gives wrong answers when records arrive late, which is what
	// the arm counts.
	PathCommitTime
)

func (p Path) String() string {
	switch p {
	case PathKey:
		return "key"
	case PathLog:
		return "log"
	case PathCommitTime:
		return "commit-time"
	}
	return fmt.Sprintf("Path(%d)", uint8(p))
}

const (
	opNeighbors = iota
	opBatch
	opAlive
	opWindow
	numOps
)

var opNames = [numOps]string{"neighbors", "batch", "alive", "window"}

// How a read was answered, for the counters: from the state as of a commit alone, from the
// state and the log, from the log alone because its token names no commit, from the
// commit-time reading, or (a window) from the log.
const (
	outAsOfAlone = iota
	outNeededLog
	outNoCommit
	outCommitTime
	outWindowLog
	numOutcomes
)

var outcomeNames = [numOutcomes]string{"as-of-alone", "needed-log", "no-commit", "commit-time", "log"}

// readStats counts the reads by kind and outcome, and the keys the rule settled each way.
type readStats struct {
	queries [numOps][numOutcomes]atomic.Uint64
	keys    [numOps][2]atomic.Uint64 // settled by the state row, answered from the log
	// rows are the rows the key ranges of the reads hold: in the state table, and in the
	// log (see [Model.Work]).
	rows [numOps][2]atomic.Uint64
	// windowPrefix is the rows of the subject a window's primary-key range would hold.
	windowPrefix atomic.Uint64
}

// Paths returns what the reads answered so far, by kind, as "kind/what" counts; only those
// that happened.
//
//	kind/as-of-alone   reads (a batched read counts once) answered from the state as of a
//	                   commit alone, with no key sent to the log
//	kind/needed-log    reads that needed the log for some key
//	kind/no-commit     reads whose token names no commit (one inside a batch, or an old
//	                   token in the arm without commits), answered from the log alone; none
//	                   is expected of a plan's reads in the arm with commits
//	kind/keys-state    state keys settled by their row
//	kind/keys-log      state keys answered from the log
//	kind/commit-time   reads of the diagnostic arm
//	window/log         windows
//
// A read at an instant before the range is answered without a query and is not counted.
// All of it is a function of the reads asked, not of timing.
func (m *Model) Paths() map[string]uint64 {
	out := make(map[string]uint64)
	for op := range numOps {
		for o := range numOutcomes {
			if n := m.reads.queries[op][o].Load(); n > 0 {
				out[opNames[op]+"/"+outcomeNames[o]] = n
			}
		}
		for i, name := range [2]string{"keys-state", "keys-log"} {
			if n := m.reads.keys[op][i].Load(); n > 0 {
				out[opNames[op]+"/"+name] = n
			}
		}
	}
	return out
}

// tally is what one read did: the keys settled by a row, the keys sent to the log because
// their row was after the instant, whether the read had no commit to read, and the rows of
// the state and log ranges it reads.
type tally struct {
	state, log         uint64
	noCommit           bool
	stateRows, logRows uint64
}

// Work returns the rows the reads' key ranges hold, by kind, as "kind/rows-state" and
// "kind/rows-log" counts; only those that are not zero. They are derived from the rows the
// model stores, not measured inside a database:
//
//   - rows-state: for each key a read looks at (a subject in the direction asked and one
//     producer) that has a row in the state table as of the read's commit, one row: the
//     state table's range for the subject holds that many;
//   - rows-log: for each key the rule answers from the log, all the key's log rows, which is
//     the range the log is looked up by (the join reaches the log by the key's columns, and
//     the instant and the sequence filter are applied to the rows of the range, so they do
//     not narrow it); for a read whose token names no commit, every log row of every key of
//     the subject, which is the range it scans;
//   - for a window, "window/rows-log": the log rows of the subject whose event time is in the
//     window, which is the range of the index on event time (again before the sequence
//     filter), and "window/rows-prefix": all the log rows of the subject in the direction,
//     which is the range of the primary key's prefix, the one the planner reads a window by
//     when it does not choose the index on event time. Which of the two a window was read by
//     is in the plan the server printed for it, not in the model.
//
// A row of the log is one record in one direction. The diagnostic reading counts none: it is
// never timed. All of it is a function of the stream and the questions, and the explain
// command checks, for the statements of the reads, that the server reads by these ranges and
// does not scan a table.
func (m *Model) Work() map[string]uint64 {
	out := make(map[string]uint64)
	for op := range numOps {
		for i, name := range [2]string{"rows-state", "rows-log"} {
			if n := m.reads.rows[op][i].Load(); n > 0 {
				out[opNames[op]+"/"+name] = n
			}
		}
	}
	if n := m.reads.windowPrefix.Load(); n > 0 {
		out["window/rows-prefix"] = n
	}
	return out
}

// countKeyed records a read answered by the rule.
func (m *Model) countKeyed(op int, ty tally) {
	o := outAsOfAlone
	switch {
	case ty.noCommit:
		o = outNoCommit
	case ty.log > 0:
		o = outNeededLog
	}
	m.reads.queries[op][o].Add(1)
	m.reads.keys[op][0].Add(ty.state)
	m.reads.keys[op][1].Add(ty.log)
	m.reads.rows[op][0].Add(ty.stateRows)
	m.reads.rows[op][1].Add(ty.logRows)
}

// view is what one read is answered from.
type view struct {
	path      Path
	ord       int  // the state's commit ordinal, for a path that reads the state
	haveState bool // whether the token names a state to read
	token     uint64
}

// choose is the view of a read under the rule: the state as of the token's commit if the
// token names one. The caller holds the lock.
func (m *Model) choose(sc engine.Scope, t int64) view {
	ord, frontier, ok := m.snapshotFor(sc.AsOf)
	if ok && m.f.pathPerQuery && t < frontier {
		ok = false // the choice made for the whole read, not for each key
	}
	return view{path: PathKey, ord: ord, haveState: ok, token: sc.AsOf}
}

// force is a view of the path asked for.
func (m *Model) force(p Path, sc engine.Scope, t int64) (view, error) {
	switch p {
	case PathKey:
		return m.choose(sc, t), nil
	case PathLog:
		return view{path: PathLog, token: sc.AsOf}, nil
	}
	return view{}, fmt.Errorf("doltmodel: a read cannot be forced onto the %s path", p)
}

// liveAt reports whether the subject holds at t, as the view sees it: some producer's
// record holds. Every key is looked at, so that the tally does not depend on which holds.
func (m *Model) liveAt(st *subject, t int64, v view, ty *tally) bool {
	live := false
	for _, p := range st.prods {
		var holds bool
		switch v.path {
		case PathLog:
			holds = m.logHolds(p, t, v.token)
		case PathCommitTime:
			row, ok := p.stateAt(v.ord)
			holds = ok && row.holds(t, &m.f)
		default:
			holds = m.keyHolds(p, t, v, ty)
		}
		live = live || holds
	}
	return live
}

// logHolds is the log path for one key.
func (m *Model) logHolds(p *producer, t int64, token uint64) bool {
	c, ok := p.newestAt(t, token, &m.f)
	return ok && stateOf(c).holds(t, &m.f)
}

// keyHolds is the rule for one key.
func (m *Model) keyHolds(p *producer, t int64, v view, ty *tally) bool {
	if !v.haveState {
		ty.noCommit = true
		ty.logRows += uint64(len(p.recs))
		return m.logHolds(p, t, v.token)
	}
	row, ok := p.stateAt(v.ord)
	if !ok {
		return false // no row: nothing holds at any instant from the horizon on
	}
	ty.stateRows++
	settles := row.event <= t
	if m.f.eventBefore {
		settles = row.event < t
	}
	if settles || m.f.useRowAfterT {
		ty.state++
		return row.holds(t, &m.f)
	}
	ty.log++
	ty.logRows += uint64(len(p.recs))
	token := v.token
	if m.f.fallbackNoSeq {
		token = engine.Latest
	}
	return m.logHolds(p, t, token)
}

func (m *Model) neighbors(fp identity.Fingerprint, dir engine.Direction, t int64, sc engine.Scope, v view, ty *tally) []engine.Neighbor {
	var out []engine.Neighbor
	for _, st := range m.incident[dir][fp] {
		if st.layer != sc.Layer || !m.liveAt(st, t, v, ty) {
			continue
		}
		peer := st.id.B
		if dir == engine.Reverse {
			peer = st.id.A
		}
		out = append(out, engine.Neighbor{Peer: peer, Relation: st.id.Relation})
	}
	// The order is the contract's, made here: by the encoding's bytes, never by the
	// text form of a fingerprint.
	engine.SortNeighbors(out)
	return out
}

func (m *Model) alive(fp identity.Fingerprint, t int64, sc engine.Scope, v view, ty *tally) bool {
	st := m.subjects[engine.EntitySubject(fp)]
	return st != nil && st.layer == sc.Layer && m.liveAt(st, t, v, ty)
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
	var ty tally
	out := m.neighbors(fp, dir, ns, sc, m.choose(sc, ns), &ty)
	m.countKeyed(opNeighbors, ty)
	return out, nil
}

// NeighborsVia is Neighbors answered from the path given. It is for checking the rule
// against the log path, and counts nothing.
func (m *Model) NeighborsVia(p Path, fp identity.Fingerprint, dir engine.Direction, t time.Time, sc engine.Scope) ([]engine.Neighbor, error) {
	if err := sc.Validate(); err != nil {
		return nil, err
	}
	ns, ok := ClampInstant(t)
	if !ok {
		return nil, nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	v, err := m.force(p, sc, ns)
	if err != nil {
		return nil, err
	}
	var ty tally
	return m.neighbors(fp, dir, ns, sc, v, &ty), nil
}

// NeighborsBatch implements [engine.Engine]. One lock covers the whole batch and one view
// answers it, so all the lists are of one snapshot, and an entity with no edges has an
// empty list.
func (m *Model) NeighborsBatch(fps []identity.Fingerprint, dir engine.Direction, t time.Time, sc engine.Scope) ([][]engine.Neighbor, error) {
	if err := sc.Validate(); err != nil {
		return nil, err
	}
	ns, ok := ClampInstant(t)
	m.mu.RLock()
	defer m.mu.RUnlock()
	v := m.choose(sc, ns)
	var ty tally
	out, err := engine.NeighborsEach(fps, func(fp identity.Fingerprint) ([]engine.Neighbor, error) {
		if !ok {
			return nil, nil
		}
		return m.neighbors(fp, dir, ns, sc, v, &ty), nil
	})
	if ok {
		m.countKeyed(opBatch, ty)
	}
	if err == nil && m.f.batchDropsEmpty {
		kept := out[:0]
		for _, l := range out {
			if len(l) > 0 {
				kept = append(kept, l)
			}
		}
		out = kept
	}
	return out, err
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
	var ty tally
	out := m.alive(fp, ns, sc, m.choose(sc, ns), &ty)
	m.countKeyed(opAlive, ty)
	return out, nil
}

// AliveVia is Alive answered from the path given, see [Model.NeighborsVia].
func (m *Model) AliveVia(p Path, fp identity.Fingerprint, t time.Time, sc engine.Scope) (bool, error) {
	if err := sc.Validate(); err != nil {
		return false, err
	}
	ns, ok := ClampInstant(t)
	if !ok {
		return false, nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	v, err := m.force(p, sc, ns)
	if err != nil {
		return false, err
	}
	var ty tally
	return m.alive(fp, ns, sc, v, &ty), nil
}

// Window implements [engine.Engine]. It reads the log, the table every record is kept
// in, through the index on event time, and the state tables not at all.
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
	m.reads.queries[opWindow][outWindowLog].Add(1)
	token := sc.AsOf
	if m.f.dropSeqFilter {
		token = engine.Latest
	}
	var out []engine.Record
	var visited, prefix uint64
	defer func() {
		m.reads.rows[opWindow][1].Add(visited)
		m.reads.windowPrefix.Add(prefix)
	}()
	for _, st := range m.incident[dir][fp] {
		if st.layer != sc.Layer {
			continue
		}
		for _, p := range st.prods {
			prefix += uint64(len(p.recs))
			lo := sort.Search(len(p.recs), func(i int) bool { return p.recs[i].event >= b.Lo })
			for i := lo; i < len(p.recs) && b.Contains(p.recs[i].event); i++ {
				visited++
				c := p.recs[i]
				if c.seq > token {
					continue
				}
				if m.f.windowDedup && i+1 < len(p.recs) && p.recs[i+1].event == c.event && p.recs[i+1].seq <= token {
					continue // a record a later one at the same instant overwrites in effect
				}
				out = append(out, c.record(st.id, st.layer, p.name))
			}
		}
	}
	engine.SortRecords(out)
	return out, nil
}
