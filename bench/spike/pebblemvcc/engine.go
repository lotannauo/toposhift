// Package pebblemvcc is layout M of the storage-engine spike: one Pebble key per
// edge and producer, with event time as the MVCC version, on Pebble's
// cockroachkvs comparer and key schema.
//
// # What is stored
//
// Every record is stored, never overwritten, because a query pinned to an
// earlier snapshot token must still see the versions it would have seen: the
// lifecycle specification's as-known-at view needs each extension of a run, and
// extensions are written at the run's own start time. A version is keyed by
// (roach key, wall, logical), where wall is the event time and logical is an
// ordinal that numbers the versions of one key written at one instant (the first
// is 1). The sequence number is in the value, not the version: cockroachkvs's
// logical field is 32 bits, and the sequence numbers of one heartbeat run that
// is extended at one instant for months span billions, so the low 32 bits of a
// sequence number would wrap and misorder them. The ordinal is the cost of that:
// a write finds the highest ordinal at its instant with one seek.
//
// An edge is stored twice, under its source's forward prefix and its target's
// reverse prefix, payload and all, so either end reads it in one range. An
// entity's existence is stored once, under its own prefix with direction 0.
//
// # How a read works
//
// A read at instant t and token asOf walks the roach keys under one prefix. For
// each it seeks to the newest version at or before t (the seek must use the
// largest logical part, see [atOrBefore]), steps older past versions whose
// sequence number is above asOf, and applies the lifecycle rule to the first it
// finds: an Observe holds until its deadline or the next assertion, a Delete
// holds nothing ([pebblekv.Value.Holds]). A subject is alive if any producer's
// reference holds. Each read, and each batched read, runs on one iterator, so it
// sees whole committed batches and nothing in between.
//
// # Retention
//
// Pebble has no compaction filter, so retention is explicit work: Retain visits
// every key that has versions before the horizon, keeps the newest one only if
// it is still alive at the horizon (it is the baseline the later answers need),
// and range-deletes the rest, keeping every version at or after the horizon.
// It reaches each key with one seek, including keys whose versions are all at or
// after the horizon (with the time filter, blocks wholly after it are skipped),
// so the scan is O(all keys), the range deletes O(keys with old data), and each
// leaves a tombstone until compaction. The measurement stage costs all three.
package pebblemvcc

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/cockroachkvs"
	"github.com/cockroachdb/pebble/v2/sstable"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
)

// Options says how an engine is opened.
type Options struct {
	pebblekv.Config
	// Recorder receives the engine's counts and samples. Nil discards them. The
	// names are "write.versions", "read.versions_stepped", "retain.keys_visited"
	// (keys that had history before the horizon), "retain.seeks" (the seeks made
	// to reach them, one per key whose newest version is at or after the horizon
	// as well), "retain.baselines_kept" and "retain.range_deletes".
	Recorder engine.Recorder

	// retainBatchBytes overrides [defaultRetainBatchBytes], for the test that
	// makes a retention commit in many pieces.
	retainBatchBytes int
}

// Engine is layout M. It implements [engine.Engine] and [engine.Settler].
type Engine struct {
	kv  *pebblekv.KV
	ids *pebblekv.IDs
	rec engine.Recorder

	lastSeq     atomic.Uint64
	retainBytes int

	// mu serializes Write and Retain, which the caller is already required to
	// do; it keeps the horizon coherent if a caller forgets.
	mu      sync.Mutex
	horizon time.Time
}

var (
	_ engine.Engine  = (*Engine)(nil)
	_ engine.Settler = (*Engine)(nil)
)

// defaultRetainBatchBytes is how much a retention accumulates before it commits.
// A retention is correct at every commit: each key is rewritten on its own, and a
// key with its baseline kept answers every instant at or after the horizon as it
// did before.
const defaultRetainBatchBytes = 4 << 20

// Open opens the engine under dir, new or as an earlier one left it: its
// records, its last sequence number and its retention horizon.
func Open(dir string, opts Options) (*Engine, error) {
	kv, err := pebblekv.Open(dir, opts.Config)
	if err != nil {
		return nil, err
	}
	e := &Engine{kv: kv, ids: pebblekv.Default, rec: opts.Recorder, retainBytes: opts.retainBatchBytes}
	if e.retainBytes == 0 {
		e.retainBytes = defaultRetainBatchBytes
	}
	if e.rec == nil {
		e.rec = engine.NopRecorder{}
	}
	fail := func(err error) (*Engine, error) {
		_ = kv.Close()
		return nil, err
	}
	if err := kv.CheckFormat(metaKey(pebblekv.MetaFormat), []byte(formatTag)); err != nil {
		return fail(err)
	}
	raw, err := kv.GetMeta(metaKey(pebblekv.MetaLastSeq))
	if err != nil {
		return fail(err)
	}
	seq, err := pebblekv.DecodeSeq(raw)
	if err != nil {
		return fail(err)
	}
	e.lastSeq.Store(seq)
	if raw, err = kv.GetMeta(metaKey(pebblekv.MetaHorizon)); err != nil {
		return fail(err)
	}
	if e.horizon, err = pebblekv.DecodeHorizon(raw); err != nil {
		return fail(err)
	}
	return e, nil
}

// LastSeq implements [engine.Engine].
func (e *Engine) LastSeq() uint64 { return e.lastSeq.Load() }

// Settle implements [engine.Settler].
func (e *Engine) Settle() error { return e.kv.Settle() }

// Size implements [engine.Engine].
func (e *Engine) Size() (int64, error) { return e.kv.Size() }

// Close implements [engine.Engine].
func (e *Engine) Close() error { return e.kv.Close() }

// put is one version to store.
type put struct {
	roach []byte
	wall  uint64
	value []byte
}

// Write implements [engine.Engine].
func (e *Engine) Write(batch []engine.Record) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Everything that can refuse the batch happens before anything is stored.
	seq := e.lastSeq.Load()
	var puts []put
	for _, r := range batch {
		if err := r.Validate(); err != nil {
			return err
		}
		if r.EventTime.Before(e.horizon) {
			return fmt.Errorf("record seq %d at %s: %w", r.Seq, r.EventTime.Format(time.RFC3339), engine.ErrBeforeHorizon)
		}
		if r.Seq <= seq {
			return fmt.Errorf("record seq %d is not above the last written seq %d: %w", r.Seq, seq, engine.ErrInvalid)
		}
		seq = r.Seq
		roaches, err := e.roachKeys(r)
		if err != nil {
			return fmt.Errorf("record seq %d: %w", r.Seq, err)
		}
		value := pebblekv.FromRecord(r).Append(nil)
		wall := wallOf(r.EventTime.UnixNano())
		for _, roach := range roaches {
			puts = append(puts, put{roach, wall, value})
		}
	}
	if len(puts) == 0 {
		return nil
	}

	// Number each version among those already at its instant: one more than the
	// highest ordinal there, from the stored data and from this batch. A count
	// would be wrong the moment retention removed one.
	it, err := e.kv.NewIter(nil)
	if err != nil {
		return err
	}
	defer func() { _ = it.Close() }()
	inBatch := make(map[string]uint32, len(puts))
	b := e.kv.NewBatch()
	defer func() { _ = b.Close() }()
	for _, p := range puts {
		slot := string(binary.BigEndian.AppendUint64(append([]byte(nil), p.roach...), p.wall))
		ord, seen := inBatch[slot]
		if !seen && it.SeekGE(atOrBefore(p.roach, p.wall)) {
			roach, wall, logical, err := split(it.Key())
			if err != nil {
				return err
			}
			if wall == p.wall && bytes.Equal(roach, p.roach) {
				ord = logical
			}
		}
		if ord == math.MaxUint32 {
			return fmt.Errorf("more than %d versions of one key at one instant: %w", uint32(math.MaxUint32), engine.ErrInvalid)
		}
		ord++
		inBatch[slot] = ord
		if err := b.Set(versionKey(p.roach, p.wall, ord), p.value, nil); err != nil {
			return err
		}
	}
	if err := it.Error(); err != nil {
		return err
	}
	if err := b.Set(metaKey(pebblekv.MetaLastSeq), pebblekv.EncodeSeq(seq), nil); err != nil {
		return err
	}
	if err := e.kv.Apply(b, e.kv.WriteOptions()); err != nil {
		return err
	}
	e.lastSeq.Store(seq)
	e.rec.Count("write.versions", int64(len(puts)))
	return nil
}

// Retain implements [engine.Engine].
func (e *Engine) Retain(horizon time.Time) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !horizon.After(e.horizon) {
		return nil
	}
	// The horizon is committed first, on its own: from then on Write refuses
	// anything older, so nothing can arrive for a key whose past is being
	// collapsed. If the process stops after it, the old history that was not yet
	// collapsed stays: answers are still right (every key keeps what the answers
	// at and after the horizon need, whether or not its past was collapsed), and
	// only a Retain with a later horizon reclaims the space, because one with this
	// horizon is a no-op.
	raw, err := pebblekv.EncodeHorizon(horizon)
	if err != nil {
		return err
	}
	if err := e.kv.Set(metaKey(pebblekv.MetaHorizon), raw, e.kv.WriteOptions()); err != nil {
		return err
	}
	e.horizon = horizon

	hNs, where := pebblekv.Locate(horizon)
	if where == pebblekv.Before || (where == pebblekv.Inside && hNs == 0) {
		return nil // no instant a record can have is before it
	}
	// The wall of the newest instant strictly before the horizon. A horizon after
	// every representable instant is the last of them: nothing is at or after it.
	lastOld := wallOf(hNs) - 1
	if where == pebblekv.After {
		lastOld = wallOf(math.MaxInt64)
	}

	lo, hi := dataBounds()
	opts := &pebble.IterOptions{LowerBound: lo, UpperBound: hi}
	if e.kv.Config().TimeFilter {
		// Blocks whose versions are all at or after the horizon have nothing to
		// remove.
		opts.PointKeyFilters = []sstable.BlockPropertyFilter{cockroachkvs.NewMVCCTimeIntervalFilter(0, lastOld)}
	}
	it, err := e.kv.NewIter(opts)
	if err != nil {
		return err
	}
	defer func() { _ = it.Close() }()

	b := e.kv.NewBatch()
	defer func() { _ = b.Close() }()
	var visited, kept, deletes, seeks int64
	commit := func() error {
		if b.Empty() {
			return nil
		}
		if err := e.kv.Apply(b, e.kv.WriteOptions()); err != nil {
			return err
		}
		b.Reset()
		return nil
	}
	for ok := it.First(); ok; {
		roach, wall, _, err := split(it.Key())
		if err != nil {
			return err
		}
		if wall > lastOld {
			// This key's newest version is at or after the horizon; go to the
			// newest one before it, if it has any.
			seeks++
			ok = it.SeekGE(atOrBefore(roach, lastOld))
			continue
		}
		visited++
		roach = append([]byte(nil), roach...)
		baseline := append([]byte(nil), it.Key()...)
		v, err := pebblekv.DecodeValue(it.Value())
		if err != nil {
			return err
		}
		_, _, logical, _ := split(baseline)
		// Everything older than the newest version before the horizon always
		// goes; that version stays only if it is alive at the horizon, because
		// then answers at and after the horizon still need it.
		start := baseline
		if v.Holds(nanosOf(wall), hNs) {
			start = versionKey(roach, wall, logical-1)
			kept++
		}
		if err := b.DeleteRange(start, endOfVersions(roach), nil); err != nil {
			return err
		}
		deletes++
		if b.Len() >= e.retainBytes {
			if err := commit(); err != nil {
				return err
			}
		}
		ok = it.SeekGE(endOfVersions(roach))
	}
	if err := it.Error(); err != nil {
		return err
	}
	if err := commit(); err != nil {
		return err
	}
	e.rec.Count("retain.keys_visited", visited)
	e.rec.Count("retain.seeks", seeks)
	e.rec.Count("retain.baselines_kept", kept)
	e.rec.Count("retain.range_deletes", deletes)
	return nil
}
