package pebblestore

import (
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/cockroachdb/pebble/v2"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/pebblekv"
)

// # Checkpoints
//
// A checkpoint at c (kind 1, keyed at c) is a derived summary of one prefix's
// history strictly before c: one entry for each reference whose newest covered
// record is an Observe still alive at c. A read that reaches one uses its entries
// for the references no newer record decided, and stops, instead of walking the
// rest of the history. It is never a fact and is never trusted past what keeps
// it true:
//
//   - W is the largest Seq among every record the checkpoint depends on: the
//     records walked to build it (including those whose entries were omitted,
//     because a Delete that an old token must not see is exactly such a record,
//     and older records a newer one shadows, which only makes W conservative)
//     and, when it was built on an older checkpoint or the baseline, that base's
//     W (the baseline's is L). A read uses it only if its token is at or above W.
//   - A record written later with an event time before c invalidates it: it is
//     deleted in the same commit as the record, in both stored copies of an edge.
//     A record at exactly c does not (it is read before the checkpoint).
//   - It is never written at or below the retention horizon of its layer (one
//     older than a later horizon may remain where a retention did not rewrite its
//     prefix, and is true): there it would sort before the baseline and hide it,
//     since its build walk starts below the baseline's key.
//   - It is written in a second commit that finishes inside the Write that
//     caused it, and an error there is counted, not returned: the records are
//     already committed, and a returned error would make the caller retry
//     sequence numbers it has used. That commit is not synced, even when the
//     record commit is: a checkpoint is derived, and the log is replayed as a
//     prefix, so one can be lost only with everything committed after it, never
//     while a later record that should have deleted it survives (the record and
//     the deletion are one batch).
//
// Losing a checkpoint is harmless, a stale one is the bug. What the writer knows
// of each prefix is kept in memory (prefixState), so it deletes known keys. It is
// read from the database the first time a write (or the instrument's hook)
// touches the prefix after an opening or a failed commit, but only in a database
// that has ever had a checkpoint (a meta key says so), so one that has none is
// never read and, with checkpoints off, nothing is remembered at all. A retention
// does not make it forget where the policy is on and a checkpoint may exist: it
// works out what the read would find from the keys it leaves in each prefix (see
// foldRetained), and when it has done that for every prefix the map is complete,
// and a prefix missing from it is known to be empty without a read (see
// Store.complete). A database opened empty starts complete. Some retentions do
// not work the state out, and then the map is empty and the prefixes are read as
// they are touched: a database that has no checkpoint yet, a policy that is off,
// a horizon after the range of instants, a retention that leaves a layer alone, a
// key that cannot be read, a store that always reads whole prefixes (tests), and a
// retention that stops or fails. A retention
// that changes nothing (a horizon before the first instant a record can have)
// forgets nothing. That read stops at the newest checkpoint (and the newest
// record, if it is older): it is as long as the tail the policy lets grow, not
// the prefix's history. The older checkpoints are looked up only when something
// needs them, by reading back from the oldest instant known down to the one
// needed: a late record (the checkpoints after it), or a checkpoint asked for at
// an older instant. A late record's lookup is as long as the history it is late
// by.

// CheckpointOptions says when checkpoints are written. The zero value writes
// none.
type CheckpointOptions struct {
	On bool
	// KMin is the fewest records written to a prefix since its last checkpoint
	// before another is due. Alpha scales a second condition: another is also not
	// due until the tail is at least Alpha times as many records as the last
	// checkpoint's size is worth (its bytes over the mean record's), so a prefix
	// with a large state is not checkpointed for a handful of records.
	KMin  int
	Alpha float64
	// Lag backs the instant off from the newest record: c is the newest event
	// time in the prefix plus one nanosecond, minus Lag. With no Lag a checkpoint
	// covers everything so far; a Lag keeps a late record from invalidating it.
	Lag time.Duration
}

// DefaultCheckpoints is the policy a store has when [Options.Checkpoints] is nil: on,
// 64 records between checkpoints (or Alpha 2 times the last one's worth, if more),
// and a lag of one nanosecond.
func DefaultCheckpoints() CheckpointOptions {
	return CheckpointOptions{On: true, KMin: 64, Alpha: 2, Lag: time.Nanosecond}
}

// validate refuses a policy with a negative or non-finite number.
func (o CheckpointOptions) validate() error {
	if o.KMin < 0 || o.Alpha < 0 || o.Lag < 0 || math.IsNaN(o.Alpha) || math.IsInf(o.Alpha, 0) {
		return fmt.Errorf("checkpoint options %+v must be finite and not negative: %w", o, store.ErrInvalid)
	}
	return nil
}

// prefixState is what the writer remembers about one prefix. Everything but
// below and the part of ckpts it bounds is exactly what a read of the whole prefix
// would have found when the state was read, and changes as the writer changes the
// prefix after that.
type prefixState struct {
	// ckpts are the instants, ascending, of every checkpoint in the prefix at or
	// after below; whether there are any before below is not known. below is 0
	// once the whole prefix has been read.
	ckpts      []int64
	below      int64
	latest     int64 // newest record's event time, -1 if none
	since      int   // records written since the last checkpoint was written
	sinceBytes int
	lastBytes  int // size of the last checkpoint written
}

// empty says the state is what reading a prefix that holds no record and no
// checkpoint finds.
func (st *prefixState) empty() bool {
	return st.latest < 0 && len(st.ckpts) == 0 && st.since == 0 && st.below == 0
}

// stateFold builds a prefixState from the keys of one prefix in key order, newest
// first. It is the one definition of what the writer learns from a read, shared
// by the read that learns it ([Store.state]) and by the retention that works it
// out from the keys it is about to leave behind, so the two cannot disagree.
//
// The records before the newest checkpoint are its tail; the newest record may be
// older than it. Once both are found nothing older changes what is remembered,
// except the older checkpoints, which are left to lookups (see know). With whole
// set it reads on regardless, to the end of the prefix.
type stateFold struct {
	st      prefixState
	newer   bool // no checkpoint seen yet
	whole   bool
	stopped bool // nothing older can change the state
}

func newStateFold(whole bool) stateFold {
	return stateFold{st: prefixState{latest: -1}, newer: true, whole: whole}
}

// add takes the next key and says whether the fold needs no more.
func (f *stateFold) add(ns int64, kind byte, valueLen int) (done bool) {
	st := &f.st
	switch kind {
	case kindRecord:
		if st.latest < 0 {
			st.latest = ns
		}
		if f.newer {
			st.since++
			st.sinceBytes += valueLen
		}
	case kindCheckpoint:
		if f.newer {
			st.lastBytes = valueLen
		}
		f.newer = false
		st.ckpts = append(st.ckpts, ns)
		st.below = ns
	}
	if !f.newer && st.latest >= 0 && !f.whole {
		f.stopped = true
	}
	return f.stopped
}

// result is the state: its bound is 0 if the whole prefix was read. It reverses
// the list of checkpoints in place, so it is called once, when the fold is done.
func (f *stateFold) result() prefixState {
	st := f.st
	if !f.stopped {
		st.below = 0
	}
	slices.Reverse(st.ckpts) // the key order is newest first
	return st
}

// forget drops what is remembered of one prefix, which is then read again when
// it is next touched. The map is no longer complete: the prefix may hold keys.
func (s *Store) forget(prefix string) {
	delete(s.states, prefix)
	s.complete = false
}

// forgetAll drops everything that is remembered. Nothing is known of any prefix
// afterwards, so the map is not complete.
func (s *Store) forgetAll() {
	s.states = map[string]*prefixState{}
	s.complete = false
}

// layerOfPrefix is the layer a data prefix belongs to.
func layerOfPrefix(prefix []byte) catalog.Layer {
	l, _ := pebblekv.LayerFromByte(prefix[0])
	return l
}

// readPrefixState reads the prefix from its newest key until the fold needs no
// more: what [Store.state] learns of a prefix nothing remembers. unreadable is
// the error of a key that cannot be parsed, which says nothing of the database;
// err is the error of the read itself.
func (s *Store) readPrefixState(prefix []byte) (st prefixState, keys int64, unreadable, err error) {
	lo, hi := prefixBounds(prefix)
	it, err := s.kv.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return prefixState{}, 0, nil, err
	}
	defer func() { _ = it.Close() }()
	f := newStateFold(s.fullStateRead)
	for ok := it.First(); ok; ok = it.Next() {
		keys++
		_, ns, _, kind, perr := parseKey(it.Key())
		if perr != nil {
			return prefixState{}, keys, perr, nil
		}
		if f.add(ns, kind, len(it.Value())) {
			break
		}
	}
	if err := it.Error(); err != nil {
		return prefixState{}, keys, nil, err
	}
	return f.result(), keys, nil, nil
}

func (s *Store) state(prefix []byte) (*prefixState, error) {
	if st, ok := s.states[string(prefix)]; ok {
		return st, nil
	}
	if !s.anyCkpt || s.complete {
		// Either no checkpoint was ever written to this database, so none is under
		// this prefix, or the map is complete (see Store.complete) and a prefix it
		// lacks holds no record and no checkpoint. Either way there is nothing to
		// find by reading it, and the state is the one a read of nothing finds.
		st := &prefixState{latest: -1}
		s.states[string(prefix)] = st
		return st, nil
	}
	s.iterators++
	res, keys, unreadable, err := s.readPrefixState(prefix)
	if err != nil {
		return nil, err
	}
	if unreadable != nil {
		return nil, unreadable
	}
	st := &res
	s.states[string(prefix)] = st
	s.rec.Count("checkpoint.loads", 1)
	s.rec.Count("checkpoint.load_keys", keys)
	return st, nil
}

// foldRetained is what [Store.state] would find in prefix once a retention at
// the horizon hNs has rewritten it, worked out before the rewrite from the keys
// that stay: those at or after hNs, which it reads from the iterator's position,
// the first key of the prefix. A retention that rewrites the prefix deletes the
// checkpoint exactly at hNs, which a prefix it does not rewrite keeps; dropped is
// the state in the first case and kept in the second. parsed is false if a key
// could not be read, in which case nothing is known. keys is how many it read.
//
// Both states are what a read finds only if nothing older than hNs stays in the
// prefix; a prefix the retention keeps whole (see Store.Retain) has its state
// read instead.
func foldRetained(it *pebble.Iterator, prefix []byte, hNs int64) (kept, dropped prefixState, keys int64, parsed bool, err error) {
	keep, drop := newStateFold(false), newStateFold(false)
	for ok := true; ok && hasPrefix(it.Key(), prefix) && (!keep.stopped || !drop.stopped); ok = it.Next() {
		keys++
		_, ns, _, kind, perr := parseKey(it.Key())
		if perr != nil {
			return kept, dropped, keys, false, nil
		}
		if ns < hNs {
			break // all of it is deleted, or there never was any
		}
		n := len(it.Value())
		if !keep.stopped {
			keep.add(ns, kind, n)
		}
		if !drop.stopped && (kind != kindCheckpoint || ns != hNs) {
			drop.add(ns, kind, n)
		}
	}
	if err := it.Error(); err != nil {
		return kept, dropped, keys, false, err
	}
	return keep.result(), drop.result(), keys, true, nil
}

// know makes st.ckpts hold every checkpoint of the prefix at or after lo, by
// reading the prefix from st.below back to lo.
func (s *Store) know(prefix []byte, st *prefixState, lo int64) error {
	lo = max(lo, 0)
	if lo >= st.below {
		return nil
	}
	_, hi := prefixBounds(prefix)
	s.iterators++
	it, err := s.kv.NewIter(&pebble.IterOptions{LowerBound: seekKey(prefix, st.below-1), UpperBound: hi})
	if err != nil {
		return err
	}
	defer func() { _ = it.Close() }()
	var found []int64 // newest first
	var keys int64
	for ok := it.First(); ok; ok = it.Next() {
		_, ns, _, kind, err := parseKey(it.Key())
		if err != nil {
			return err
		}
		keys++
		if ns < lo {
			break
		}
		if kind == kindCheckpoint {
			found = append(found, ns)
		}
	}
	if err := it.Error(); err != nil {
		return err
	}
	slices.Reverse(found)
	st.ckpts = append(found, st.ckpts...)
	st.below = lo
	s.rec.Count("checkpoint.lookups", 1)
	s.rec.Count("checkpoint.load_keys", keys)
	return nil
}

// floor is the instant at or below which no checkpoint is written in a layer: its
// horizon's, or -1 if there is none. After the end of the range every instant is
// at or below the horizon, which all says.
func (s *Store) floor(layer catalog.Layer) (ns int64, all bool) {
	hNs, where := pebblekv.Locate(s.horizonOf(layer).Time)
	switch where {
	case pebblekv.Before:
		return -1, false
	case pebblekv.After:
		return math.MaxInt64, true
	}
	return hNs, false
}

// placeable says whether a checkpoint can be written at c in a layer.
func (s *Store) placeable(layer catalog.Layer, c int64) bool {
	f, all := s.floor(layer)
	return !all && c >= 1 && c > f
}

// due is the policy: whether the prefix has had enough written to it since its
// last checkpoint.
func (o CheckpointOptions) due(st *prefixState) bool {
	if !o.On || st.since == 0 {
		return false
	}
	need := max(o.KMin, 1)
	if o.Alpha > 0 && st.lastBytes > 0 {
		mean := float64(st.sinceBytes) / float64(st.since)
		if n := int(o.Alpha * float64(st.lastBytes) / mean); n > need {
			need = n
		}
	}
	return st.since >= need
}

// at is the instant the policy would checkpoint a prefix at, or false if there
// is none (nothing written, or the instant leaves the range).
func (o CheckpointOptions) at(st *prefixState) (int64, bool) {
	if st.latest < 0 || st.latest == math.MaxInt64 {
		return 0, false
	}
	c := st.latest + 1 - o.Lag.Nanoseconds()
	return c, c >= 1
}

// build is the checkpoint of prefix at c: it walks from the newest record
// strictly before c toward older ones, deciding each reference by the newest
// record, and stops at the nearest older checkpoint of this fold version or at
// the baseline, whose entries it takes for the references nothing newer decided.
func (s *Store) build(prefix []byte, c int64) ([]byte, error) {
	started := s.now()
	dir := prefix[prefixLen-1]
	lo, hi := prefixBounds(prefix)
	s.iterators++
	it, err := s.kv.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return nil, err
	}
	defer func() { _ = it.Close() }()
	decided := map[string]struct{}{}
	var entries []entry
	var w uint64
	var walked int64
	take := func(ref []byte, ns int64, v pebblekv.Value) {
		if _, done := decided[string(ref)]; done {
			return
		}
		decided[string(ref)] = struct{}{}
		if v.Holds(ns, c) {
			entries = append(entries, entryOf(ref, ns, v))
		}
	}
walk:
	for ok := it.SeekGE(seekKey(prefix, c-1)); ok; ok = it.Next() {
		_, ns, seq, kind, err := parseKey(it.Key())
		if err != nil {
			return nil, err
		}
		switch kind {
		case kindRecord:
			walked++
			ref, v, err := decodeRecordValue(dir, it.Value())
			if err != nil {
				return nil, err
			}
			if v.Seq != seq {
				return nil, fmt.Errorf("pebblestore: a record's key has seq %d and its value %d", seq, v.Seq)
			}
			w = max(w, seq)
			take(ref, ns, v)
		case kindCheckpoint, kindBaseline:
			st, err := decodeStamp(it.Value())
			if err != nil {
				return nil, err
			}
			if st.kind != kind {
				return nil, fmt.Errorf("pebblestore: a key of kind %d holds a stamp of kind %d", kind, st.kind)
			}
			if kind == kindCheckpoint && st.foldVersion != foldVersion {
				continue // built by other logic: not a base
			}
			w = max(w, st.w)
			for _, en := range st.entries {
				take(en.ref, en.eventNs, en.value)
			}
			break walk
		}
	}
	if err := it.Error(); err != nil {
		return nil, err
	}
	s.rec.Count("checkpoint.build_records_walked", walked)
	val, err := appendStamp(nil, stamp{kind: kindCheckpoint, foldVersion: foldVersion, through: s.lastSeq.Load(), w: w, entries: entries})
	if s.timed {
		s.rec.Count("checkpoint.build_ns", s.since(started))
	}
	return val, err
}

// applyCheckpoints commits a batch of checkpoints, not synced whatever the
// database's setting: they are derived, and see the notes above. Two things say a
// checkpoint may exist, and they are not the same. anyCkpt, in memory, says "look
// for them", and is set before the commit, not after: Pebble can return an error
// from a commit whose batch is already visible and in the log (a failed sync), and
// if that was the first checkpoint, a store that still believed there were none
// would never read a prefix for them and never delete one a later record makes
// untrue. flagOnDisk says the meta key is in the database, and is set only when a
// commit that contained it returned success: a commit that failed without landing
// leaves it unset, so the next batch carries the key again, and no checkpoint can
// be on disk without it.
func (s *Store) applyCheckpoints(b *pebble.Batch) error {
	if !s.flagOnDisk {
		if err := b.Set(metaKey(metaCheckpoints), []byte{1}, nil); err != nil {
			return err
		}
	}
	s.anyCkpt = true
	if s.beforeCheckpointApply != nil {
		if err := s.beforeCheckpointApply(); err != nil {
			return err // tests: a commit that failed without landing
		}
	}
	err := s.kv.Apply(b, pebble.NoSync)
	if err == nil && s.afterCheckpointApply != nil {
		err = s.afterCheckpointApply() // tests: a commit that failed after landing
	}
	if err == nil {
		s.flagOnDisk = true
	}
	return err
}

// writeCheckpoints builds and writes, in one commit, the checkpoints of the
// prefixes this write touched that are due, in key order. It never returns an
// error: the records are committed, so a failure is counted and the prefixes it
// concerned are forgotten and will be read from the database next time. ph, if
// not nil, receives the time it spent building and committing.
func (s *Store) writeCheckpoints(touched map[string]struct{}, ph *writePhases) {
	if !s.ckpt.On {
		return
	}
	buildStart := s.now()
	keys := make([]string, 0, len(touched))
	for k := range touched {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	b := s.kv.NewBatch()
	defer func() { _ = b.Close() }()
	type made struct {
		prefix string
		c      int64
		size   int
	}
	var done []made
	for _, k := range keys {
		st := s.states[k]
		if st == nil || !s.ckpt.due(st) {
			continue
		}
		if s.policy.BootKey != "" && k[prefixLen-1] == dirEntity {
			// Under a boot key an entity's existence is folded from its whole prefix
			// (see aliveFolded) and no read uses its checkpoints, so the policy places
			// none; the ones that exist (from the instrument's hook) are still deleted
			// when a record makes them untrue.
			continue
		}
		c, ok := s.ckpt.at(st)
		if !ok || !s.placeable(layerOfPrefix([]byte(k)), c) {
			continue
		}
		if err := s.know([]byte(k), st, c); err != nil {
			s.rec.Count("checkpoint.errors", 1)
			s.forget(k)
			continue
		}
		if _, exists := slices.BinarySearch(st.ckpts, c); exists {
			st.since, st.sinceBytes = 0, 0 // already summarized to here, and still true
			continue
		}
		val, err := s.build([]byte(k), c)
		if err != nil {
			s.rec.Count("checkpoint.errors", 1)
			s.forget(k)
			continue
		}
		if err := b.Set(stampKey([]byte(k), c, kindCheckpoint), val, nil); err != nil {
			s.rec.Count("checkpoint.errors", 1)
			continue
		}
		done = append(done, made{k, c, len(val)})
	}
	if ph != nil {
		ph.ckptBuild = s.since(buildStart)
	}
	if len(done) == 0 {
		return
	}
	commitStart := s.now()
	err := s.applyCheckpoints(b)
	if ph != nil {
		ph.ckptCommit = s.since(commitStart)
	}
	if err != nil {
		s.rec.Count("checkpoint.errors", 1)
		s.forgetAll() // what is on disk is unknown
		return
	}
	for _, m := range done {
		st := s.states[m.prefix]
		if st == nil {
			continue
		}
		i, _ := slices.BinarySearch(st.ckpts, m.c)
		st.ckpts = slices.Insert(st.ckpts, i, m.c)
		st.since, st.sinceBytes, st.lastBytes = 0, 0, m.size
	}
	var bytes int64
	for _, m := range done {
		bytes += int64(len(m.prefix) + suffixLen + m.size)
	}
	s.rec.Count("checkpoint.written", int64(len(done)))
	s.rec.Count("checkpoint.bytes_written", bytes)
}

// invalidate deletes, in b, the checkpoints of the prefix that a record at ns
// makes untrue: every one that covers an instant after it. It updates the list in
// memory; a failed commit forgets the state, so the list is reread.
func (s *Store) invalidate(b *pebble.Batch, prefix []byte, st *prefixState, ns int64) error {
	if ns == math.MaxInt64 {
		return nil // nothing is after it
	}
	if err := s.know(prefix, st, ns+1); err != nil {
		return err
	}
	i, _ := slices.BinarySearch(st.ckpts, ns+1) // the first checkpoint with c > ns
	if i == len(st.ckpts) {
		return nil
	}
	for _, c := range st.ckpts[i:] {
		if err := b.Delete(stampKey(prefix, c, kindCheckpoint), nil); err != nil {
			return err
		}
	}
	s.rec.Count("checkpoint.invalidated", int64(len(st.ckpts)-i))
	st.ckpts = st.ckpts[:i]
	return nil
}

// CheckpointEdges writes the checkpoint of an entity's edges in one direction at
// c, if there is none there already, as the policy would have. It refuses an
// instant it cannot place: one at or below the layer's horizon, or outside the
// range of instants. For tests and measurements.
func (i *Instrument) CheckpointEdges(layer catalog.Layer, fp identity.Fingerprint, dir store.Direction, c time.Time) error {
	if dir != store.Forward && dir != store.Reverse {
		return fmt.Errorf("pebblestore: CheckpointEdges: direction %s: %w", dir, store.ErrInvalid)
	}
	prefix, ok := i.s.prefixOf(layer, fp, byte(dir))
	if !ok {
		return fmt.Errorf("pebblestore: CheckpointEdges: entity type %q is not in the catalog: %w", fp.Type(), store.ErrInvalid)
	}
	return i.s.checkpointAt("CheckpointEdges", prefix, c)
}

// CheckpointEntity writes the checkpoint of an entity's own existence at c; see
// [Instrument.CheckpointEdges].
func (i *Instrument) CheckpointEntity(layer catalog.Layer, fp identity.Fingerprint, c time.Time) error {
	prefix, ok := i.s.prefixOf(layer, fp, dirEntity)
	if !ok {
		return fmt.Errorf("pebblestore: CheckpointEntity: entity type %q is not in the catalog: %w", fp.Type(), store.ErrInvalid)
	}
	return i.s.checkpointAt("CheckpointEntity", prefix, c)
}

func (s *Store) checkpointAt(op string, prefix []byte, c time.Time) error {
	if s.closed.Load() {
		return closedError(op)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed != nil {
		return s.failedError(op)
	}
	cNs, where := pebblekv.Locate(c)
	layer := layerOfPrefix(prefix)
	if where != pebblekv.Inside || !s.placeable(layer, cNs) {
		return fmt.Errorf("pebblestore: %s: a checkpoint at %s cannot be placed (horizon %s): %w",
			op, formatTime(c), formatTime(s.horizonOf(layer).Time), store.ErrInvalid)
	}
	st, err := s.state(prefix)
	if err != nil {
		return err
	}
	if err := s.know(prefix, st, cNs); err != nil {
		s.forget(string(prefix))
		return err
	}
	if _, exists := slices.BinarySearch(st.ckpts, cNs); exists {
		return nil
	}
	val, err := s.build(prefix, cNs)
	if err != nil {
		return err
	}
	b := s.kv.NewBatch()
	defer func() { _ = b.Close() }()
	if err := b.Set(stampKey(prefix, cNs, kindCheckpoint), val, nil); err != nil {
		return err
	}
	if err := s.applyCheckpoints(b); err != nil {
		s.forgetAll() // what is on disk is unknown
		return err
	}
	i, _ := slices.BinarySearch(st.ckpts, cNs)
	st.ckpts = slices.Insert(st.ckpts, i, cNs)
	s.rec.Count("checkpoint.written", 1)
	s.rec.Count("checkpoint.bytes_written", int64(len(prefix)+suffixLen+len(val)))
	return nil
}
