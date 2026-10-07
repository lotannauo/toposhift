package pebblelog

import (
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/cockroachdb/pebble/v2"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
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
//   - It is never written at or below the retention horizon (one older than a
//     later horizon may remain where a retention did not rewrite its prefix, and
//     is true): there it would sort
//     before the baseline and hide it, since its build walk starts below the
//     baseline's key.
//   - It is written in a second commit that finishes inside the Write that
//     caused it, and an error there is counted, not returned: the records are
//     already committed, and a returned error would make the caller retry
//     sequence numbers it has used.
//
// Losing a checkpoint is harmless, a stale one is the bug. What the writer knows
// of each prefix is kept in memory (prefixState), so it deletes known keys. It is
// read from the database the first time a write (or the hook) touches the prefix
// after an opening, a retention or a failed commit, but only in a database that
// has ever had a checkpoint (a meta key says so), so one that has none is never
// read and, with checkpoints off, nothing is remembered at all. That read stops at
// the newest checkpoint (and the newest record, if it is older): it is as long as
// the tail the policy lets grow, not the prefix's history. The older checkpoints
// are looked up only when something needs them, by reading back from the oldest
// instant known down to the one needed: a late record (the checkpoints after it),
// or a checkpoint asked for at an older instant. A late record's lookup is as long
// as the history it is late by.

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

func (e *Engine) state(prefix []byte) (*prefixState, error) {
	if st, ok := e.states[string(prefix)]; ok {
		return st, nil
	}
	st := &prefixState{latest: -1}
	if !e.anyCkpt {
		// No checkpoint was ever written to this database, so none is under this
		// prefix, and there is nothing to find by reading it.
		e.states[string(prefix)] = st
		return st, nil
	}
	lo, hi := prefixBounds(prefix)
	it, err := e.kv.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return nil, err
	}
	defer func() { _ = it.Close() }()
	// The keys run newest first. The records before the newest checkpoint are its
	// tail; the newest record may be older than it. Once both are found nothing
	// older changes what is remembered, except the older checkpoints, which are
	// left to lookups (see know).
	newer := true
	var keys int64
	ok := it.First()
	for ; ok; ok = it.Next() {
		keys++
		_, ns, _, kind, err := parseKey(it.Key())
		if err != nil {
			return nil, err
		}
		switch kind {
		case kindRecord:
			if st.latest < 0 {
				st.latest = ns
			}
			if newer {
				st.since++
				st.sinceBytes += len(it.Value())
			}
		case kindCheckpoint:
			if newer {
				st.lastBytes = len(it.Value())
			}
			newer = false
			st.ckpts = append(st.ckpts, ns)
			st.below = ns
		}
		if !newer && st.latest >= 0 && !e.fullStateRead {
			break
		}
	}
	if err := it.Error(); err != nil {
		return nil, err
	}
	if !ok {
		st.below = 0 // the whole prefix was read
	}
	slices.Reverse(st.ckpts) // the key order is newest first
	e.states[string(prefix)] = st
	e.rec.Count("checkpoint.loads", 1)
	e.rec.Count("checkpoint.load_keys", keys)
	return st, nil
}

// know makes st.ckpts hold every checkpoint of the prefix at or after lo, by
// reading the prefix from st.below back to lo.
func (e *Engine) know(prefix []byte, st *prefixState, lo int64) error {
	lo = max(lo, 0)
	if lo >= st.below {
		return nil
	}
	_, hi := prefixBounds(prefix)
	it, err := e.kv.NewIter(&pebble.IterOptions{LowerBound: seekKey(prefix, st.below-1), UpperBound: hi})
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
	e.rec.Count("checkpoint.lookups", 1)
	e.rec.Count("checkpoint.load_keys", keys)
	return nil
}

// floor is the instant at or below which no checkpoint is written: the horizon's,
// or -1 if there is none. After the end of the range every instant is at or
// below the horizon, which all says.
func (e *Engine) floor() (ns int64, all bool) {
	hNs, where := pebblekv.Locate(e.horizon)
	switch where {
	case pebblekv.Before:
		return -1, false
	case pebblekv.After:
		return math.MaxInt64, true
	}
	return hNs, false
}

// placeable says whether a checkpoint can be written at c.
func (e *Engine) placeable(c int64) bool {
	f, all := e.floor()
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
func (e *Engine) build(prefix []byte, c int64) ([]byte, error) {
	dir := prefix[prefixLen-1]
	lo, hi := prefixBounds(prefix)
	it, err := e.kv.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return nil, err
	}
	defer func() { _ = it.Close() }()
	decided := map[string]struct{}{}
	var entries []Entry
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
				return nil, fmt.Errorf("pebblelog: a record's key has seq %d and its value %d", seq, v.Seq)
			}
			w = max(w, seq)
			take(ref, ns, v)
		case kindCheckpoint, kindBaseline:
			st, err := decodeStamp(it.Value())
			if err != nil {
				return nil, err
			}
			if st.Kind != kind {
				return nil, fmt.Errorf("pebblelog: a key of kind %d holds a stamp of kind %d", kind, st.Kind)
			}
			if kind == kindCheckpoint && st.FoldVersion != FoldVersion {
				continue // built by other logic: not a base
			}
			w = max(w, st.W)
			for _, en := range st.Entries {
				take(en.Ref, en.EventNs, en.Value)
			}
			break walk
		}
	}
	if err := it.Error(); err != nil {
		return nil, err
	}
	e.rec.Count("checkpoint.build_records_walked", walked)
	return appendStamp(nil, Stamp{Kind: kindCheckpoint, FoldVersion: FoldVersion, Through: e.lastSeq.Load(), W: w, Entries: entries})
}

// applyCheckpoints commits a batch of checkpoints. Two things say a checkpoint
// may exist, and they are not the same. anyCkpt, in memory, says "look for them",
// and is set before the commit, not after: Pebble can return an error from a
// commit whose batch is already visible and in the log (a failed sync), and if
// that was the first checkpoint, an engine that still believed there were none
// would never read a prefix for them and never delete one a later record makes
// untrue. flagOnDisk says the meta key is in the database, and is set only when a
// commit that contained it returned success: a commit that failed without landing
// leaves it unset, so the next batch carries the key again, and no checkpoint can
// be on disk without it.
func (e *Engine) applyCheckpoints(b *pebble.Batch) error {
	if !e.flagOnDisk {
		if err := b.Set(metaKey(metaCheckpoints), []byte{1}, nil); err != nil {
			return err
		}
	}
	e.anyCkpt = true
	if e.beforeCheckpointApply != nil {
		if err := e.beforeCheckpointApply(); err != nil {
			return err // tests: a commit that failed without landing
		}
	}
	err := e.kv.Apply(b, e.kv.WriteOptions())
	if err == nil && e.afterCheckpointApply != nil {
		err = e.afterCheckpointApply() // tests: a commit that failed after landing
	}
	if err == nil {
		e.flagOnDisk = true
	}
	return err
}

// writeCheckpoints builds and writes, in one commit, the checkpoints of the
// prefixes this write touched that are due, in key order. It never returns an
// error: the records are committed, so a failure is counted and the prefixes it
// concerned are forgotten and will be read from the database next time.
func (e *Engine) writeCheckpoints(touched map[string]struct{}) {
	if !e.ckpt.On {
		return
	}
	keys := make([]string, 0, len(touched))
	for k := range touched {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	b := e.kv.NewBatch()
	defer func() { _ = b.Close() }()
	type made struct {
		prefix string
		c      int64
		size   int
	}
	var done []made
	for _, k := range keys {
		st := e.states[k]
		if st == nil || !e.ckpt.due(st) {
			continue
		}
		c, ok := e.ckpt.at(st)
		if !ok || !e.placeable(c) {
			continue
		}
		if err := e.know([]byte(k), st, c); err != nil {
			e.rec.Count("checkpoint.errors", 1)
			delete(e.states, k)
			continue
		}
		if _, exists := slices.BinarySearch(st.ckpts, c); exists {
			st.since, st.sinceBytes = 0, 0 // already summarized to here, and still true
			continue
		}
		val, err := e.build([]byte(k), c)
		if err != nil {
			e.rec.Count("checkpoint.errors", 1)
			delete(e.states, k)
			continue
		}
		if err := b.Set(stampKey([]byte(k), c, kindCheckpoint), val, nil); err != nil {
			e.rec.Count("checkpoint.errors", 1)
			continue
		}
		done = append(done, made{k, c, len(val)})
	}
	if len(done) == 0 {
		return
	}
	if err := e.applyCheckpoints(b); err != nil {
		e.rec.Count("checkpoint.errors", 1)
		e.states = map[string]*prefixState{} // what is on disk is unknown
		return
	}
	for _, m := range done {
		st := e.states[m.prefix]
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
	e.rec.Count("checkpoint.written", int64(len(done)))
	e.rec.Count("checkpoint.bytes_written", bytes)
}

// invalidate deletes, in b, the checkpoints of the prefix that a record at ns
// makes untrue: every one that covers an instant after it. It updates the list in
// memory; a failed commit forgets the state, so the list is reread.
func (e *Engine) invalidate(b *pebble.Batch, prefix []byte, st *prefixState, ns int64) error {
	if ns == math.MaxInt64 {
		return nil // nothing is after it
	}
	if err := e.know(prefix, st, ns+1); err != nil {
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
	e.rec.Count("checkpoint.invalidated", int64(len(st.ckpts)-i))
	st.ckpts = st.ckpts[:i]
	return nil
}

var _ engine.Checkpointer = (*Engine)(nil)

// CheckpointEdges implements [engine.Checkpointer].
func (e *Engine) CheckpointEdges(layer catalog.Layer, fp identity.Fingerprint, dir engine.Direction, c time.Time) error {
	if err := checkDir(dir); err != nil {
		return err
	}
	prefix, ok := e.prefixOf(layer, fp, byte(dir))
	if !ok {
		return fmt.Errorf("entity type %q is not in the catalog: %w", fp.Type(), engine.ErrInvalid)
	}
	return e.checkpointAt(prefix, c)
}

// CheckpointEntity implements [engine.Checkpointer].
func (e *Engine) CheckpointEntity(layer catalog.Layer, fp identity.Fingerprint, c time.Time) error {
	prefix, ok := e.prefixOf(layer, fp, dirEntity)
	if !ok {
		return fmt.Errorf("entity type %q is not in the catalog: %w", fp.Type(), engine.ErrInvalid)
	}
	return e.checkpointAt(prefix, c)
}

func (e *Engine) checkpointAt(prefix []byte, c time.Time) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	cNs, where := pebblekv.Locate(c)
	if where != pebblekv.Inside || !e.placeable(cNs) {
		return fmt.Errorf("a checkpoint at %s cannot be placed (horizon %s): %w", c.Format(time.RFC3339Nano), e.horizon.Format(time.RFC3339Nano), engine.ErrInvalid)
	}
	st, err := e.state(prefix)
	if err != nil {
		return err
	}
	if err := e.know(prefix, st, cNs); err != nil {
		delete(e.states, string(prefix))
		return err
	}
	if _, exists := slices.BinarySearch(st.ckpts, cNs); exists {
		return nil
	}
	val, err := e.build(prefix, cNs)
	if err != nil {
		return err
	}
	b := e.kv.NewBatch()
	defer func() { _ = b.Close() }()
	if err := b.Set(stampKey(prefix, cNs, kindCheckpoint), val, nil); err != nil {
		return err
	}
	if err := e.applyCheckpoints(b); err != nil {
		e.states = map[string]*prefixState{} // what is on disk is unknown
		return err
	}
	i, _ := slices.BinarySearch(st.ckpts, cNs)
	st.ckpts = slices.Insert(st.ckpts, i, cNs)
	e.rec.Count("checkpoint.written", 1)
	e.rec.Count("checkpoint.bytes_written", int64(len(prefix)+suffixLen+len(val)))
	return nil
}
