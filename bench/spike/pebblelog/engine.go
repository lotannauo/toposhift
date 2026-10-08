// Package pebblelog is layout L of the storage-engine spike: a log per entity,
// direction and layer, in Pebble's own bytewise key order, with the event time and
// the sequence number inverted in the key so the newest record is first.
//
// # What is stored
//
// Every record is stored, never overwritten, because a query pinned to an earlier
// snapshot token must see the records it would have seen. The key is the prefix
// (layer, entity fingerprint, direction) and then the inverted event time and
// the inverted Seq, so records at one instant are told apart by their Seq alone
// and a write needs no read. The peer, the relation and the producer are in the
// value. An edge is stored twice, under its source's forward prefix and its
// target's reverse prefix, payload and all, and an entity's existence once, under
// its own prefix with direction 0. See [key.go] for the key and [value.go] and
// [stamp.go] for the values.
//
// # How a read works
//
// A read at instant t and token asOf seeks to the newest record at or before t in
// one prefix and walks toward older ones. For each reference (peer, relation,
// producer) the first record the token sees decides it ([pebblekv.Value.Holds]);
// a subject is alive if any producer's reference holds. The walk ends at the
// retention baseline, which stands for everything before the horizon. With no
// checkpoints a read is as long as the history older than t in its prefix.
//
// # Checkpoints
//
// Interleaved checkpoints (see checkpoint.go) are derived summaries of a prefix's
// history before an instant. A read that reaches a usable one (built by this fold
// logic, and depending only on records its token can see) takes its entries for
// the references nothing newer decided and stops. They are written by a policy
// ([CheckpointOptions]), deleted in the same commit as any later record that
// makes them untrue, never written at or below the retention horizon, and never
// written back as facts: losing one is harmless.
//
// # Retention
//
// Retain(h) rewrites each prefix that has history before h in one commit: it
// replays it, writes a baseline at h holding every reference that is still alive
// at h, and range-deletes everything older. The baseline is not derived data: it
// is the only record of the state before h. It is never invalidated, because
// nothing can be written before h, and it is needed whether or not there are
// checkpoints.
package pebblelog

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/v2"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
)

// Options says how an engine is opened. The key schema is Pebble's default and
// there is no time filter: the layout has no use for one (every read is one
// contiguous range of one prefix).
type Options struct {
	pebblekv.Config
	// Recorder receives the engine's counts. Nil discards them. The names are
	// "write.records", "read.records_stepped", "retain.prefixes_visited",
	// "retain.prefixes_replayed", "retain.records_replayed",
	// "retain.baselines_written", "retain.range_deletes", "retain.seeks",
	// "retain.state_keys" (the keys a retention reads to work out the writer's
	// state, when it does; see [Engine.Retain]), "retain.flush_ns",
	// "retain.settle_ns" and "retain.settle_deadline_hits" (the flush and the wait
	// of [pebblekv.Config.SettleRetention], and the waits that reached their
	// deadline), and for
	// checkpoints "checkpoint.written", "checkpoint.bytes_written" (key and value
	// bytes), "checkpoint.invalidated",
	// "checkpoint.errors", "checkpoint.loads" (prefixes whose state was read from
	// the database: not those a retention worked out, nor the new ones a complete
	// map knows to be empty), "checkpoint.lookups",
	// "checkpoint.load_keys" (the keys both read),
	// "checkpoint.build_records_walked", "read.checkpoint_hits",
	// "read.checkpoint_skipped_w" and "read.checkpoint_skipped_version". Every
	// read also samples Pebble's iterator statistics under "read.<op>.<name>" (see
	// [pebblekv.RecordIter]); those are in the same unit for every layout, which
	// "read.records_stepped" is not.
	Recorder engine.Recorder
	// Checkpoints says when interleaved checkpoints are written; the zero value
	// writes none, and the layout then answers every read by replaying.
	Checkpoints CheckpointOptions

	// retainBatchBytes overrides [defaultRetainBatchBytes], and retainStopAfter
	// makes a retention fail after that many commits of its work: both for tests.
	retainBatchBytes int
	retainStopAfter  int
	// afterCheckpointApply runs after a checkpoint commit has landed; an error it
	// returns stands for a commit that failed but is already visible.
	afterCheckpointApply func() error
	// beforeCheckpointApply returns an error in place of a checkpoint commit, and
	// beforeRecordApply in place of a record commit: a commit that failed without
	// landing.
	beforeCheckpointApply func() error
	beforeRecordApply     func() error
	// fullStateRead makes the writer read the whole of a prefix to learn its
	// state, as it once did, and never trust the map to be complete: for tests
	// that compare the two.
	fullStateRead bool
}

// Engine is layout L. It implements [engine.Engine], [engine.Settler] and
// [engine.Checkpointer].
type Engine struct {
	kv  *pebblekv.KV
	ids *pebblekv.IDs
	rec engine.Recorder

	lastSeq     atomic.Uint64
	retainBytes int
	stopAfter   int
	ckpt        CheckpointOptions

	afterCheckpointApply, beforeCheckpointApply, beforeRecordApply func() error
	fullStateRead                                                  bool

	// mu serializes Write and Retain, which the caller is already required to
	// do; it keeps the horizon coherent if a caller forgets.
	mu      sync.Mutex
	horizon time.Time
	// last is how the last Retain spent its time (see [Engine.LastRetain]).
	last retainPhases
	// states is what the writer remembers of each prefix (see checkpoint.go).
	// anyCkpt says a checkpoint may be in the database (one was, or a commit that
	// reported failure may have landed), in which case a prefix first touched is
	// read for the checkpoints it holds. flagOnDisk says the meta key that records
	// it is durably there; until a commit carrying it succeeds, every checkpoint
	// batch carries it (see applyCheckpoints).
	//
	// complete says the map is not only right about the prefixes in it but
	// exhaustive: a prefix it lacks holds no record and no checkpoint, so the
	// writer knows its state (empty) without reading it. It holds from the
	// opening of a database with no data, and from the end of a retention that
	// worked out the state of every prefix in its own pass (see Retain). It does
	// not hold, and the prefixes are read as they are touched, in a database that
	// already holds data when it is opened; after a retention that does not work
	// the state out (no checkpoint in the database yet, the policy off, a horizon
	// outside the range, a key that cannot be read, or a retention that stops or
	// fails); after anything that drops what is remembered (a failed commit or
	// read); and after a write with checkpoints off and none in the database,
	// which stores records and remembers nothing, so the prefixes it leaves are
	// read if a checkpoint is later written.
	//
	// The price is memory: one entry per live prefix (about 150 to 200 bytes: the
	// map slot, the separately allocated state, the key as a string and the
	// checkpoint list), kept for as long as the engine is open, where the map
	// would otherwise hold only the prefixes touched since the last retention.
	states     map[string]*prefixState
	complete   bool
	anyCkpt    bool
	flagOnDisk bool
}

var (
	_ engine.Engine  = (*Engine)(nil)
	_ engine.Settler = (*Engine)(nil)
)

// defaultRetainBatchBytes is how much a retention accumulates before it commits.
// Each prefix's range delete and baseline are always in one commit, and the
// commits fall between prefixes, so the state after any of them is correct for
// every instant at or after the horizon.
const defaultRetainBatchBytes = 4 << 20

var errInjected = errors.New("pebblelog: injected failure")

// retainPhases is how a call of Retain spent its time.
type retainPhases struct {
	work, flush, settle time.Duration
	deadlineHit         bool
}

// LastRetain is how the last call of Retain spent its time: the rewrite up to its last
// commit, the flush and the wait of SettleRetention, and whether the wait reached its
// deadline. All zero before the first Retain; flush and settle are zero when
// SettleRetention is off or the retention returned early, before rewriting anything.
func (e *Engine) LastRetain() (work, flush, settle time.Duration, deadlineHit bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.last.work, e.last.flush, e.last.settle, e.last.deadlineHit
}

// Open opens the engine under dir, new or as an earlier one left it: its
// records, its last sequence number and its retention horizon.
func Open(dir string, opts Options) (*Engine, error) {
	if o := opts.Checkpoints; o.KMin < 0 || o.Alpha < 0 || o.Lag < 0 || math.IsNaN(o.Alpha) || math.IsInf(o.Alpha, 0) {
		return nil, fmt.Errorf("pebblelog: checkpoint options %+v must be finite and not negative", o)
	}
	cfg := opts.Config
	if cfg.Schema == 0 {
		cfg.Schema = pebblekv.SchemaDefault
	}
	kv, err := pebblekv.Open(dir, pebblekv.BytewiseLayout, cfg)
	if err != nil {
		return nil, err
	}
	e := &Engine{kv: kv, ids: pebblekv.Default, rec: opts.Recorder, retainBytes: opts.retainBatchBytes, stopAfter: opts.retainStopAfter, ckpt: opts.Checkpoints, afterCheckpointApply: opts.afterCheckpointApply, beforeCheckpointApply: opts.beforeCheckpointApply, beforeRecordApply: opts.beforeRecordApply, fullStateRead: opts.fullStateRead, states: map[string]*prefixState{}}
	if e.rec == nil {
		e.rec = engine.NopRecorder{}
	}
	if e.retainBytes == 0 {
		e.retainBytes = defaultRetainBatchBytes
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
	if raw, err = kv.GetMeta(metaKey(metaCheckpoints)); err != nil {
		return fail(err)
	}
	e.anyCkpt = raw != nil
	e.flagOnDisk = e.anyCkpt
	// A database that holds no data has no prefix to learn: every one a write
	// meets is new, and there is no need to read it to find that out. (An engine
	// that reads the whole of every prefix, for tests, trusts nothing it has not
	// read.)
	lo, hi := dataBounds()
	it, err := kv.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return fail(err)
	}
	empty := !it.First()
	err = errors.Join(it.Error(), it.Close())
	if err != nil {
		return fail(err)
	}
	e.complete = empty && !e.fullStateRead
	return e, nil
}

// LastSeq implements [engine.Engine].
func (e *Engine) LastSeq() uint64 { return e.lastSeq.Load() }

// Settle implements [engine.Settler].
func (e *Engine) Settle() error { return e.kv.Settle() }

// Size implements [engine.Engine].
func (e *Engine) Size() (int64, error) { return e.kv.Size() }

// Close implements [engine.Engine]. It does not take the lock Write and Retain
// hold, so it must not run while either does: a Retain under SettleRetention can
// hold the database for up to its deadline.
func (e *Engine) Close() error { return e.kv.Close() }

// Write implements [engine.Engine].
func (e *Engine) Write(batch []engine.Record) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Everything that can refuse the batch happens before anything is stored.
	type put struct {
		prefix, key, value []byte
		ns                 int64
	}
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
		sides, err := e.sidesOf(r)
		if err != nil {
			return fmt.Errorf("record seq %d: %w", r.Seq, err)
		}
		v := pebblekv.FromRecord(r)
		ns := r.EventTime.UnixNano()
		for _, s := range sides {
			puts = append(puts, put{s.prefix, recordKey(s.prefix, ns, r.Seq), appendRecordValue(nil, s.ref, v), ns})
		}
	}
	if len(puts) == 0 {
		return nil
	}
	b := e.kv.NewBatch()
	defer func() { _ = b.Close() }()
	// A record with an event time before a checkpoint's makes that checkpoint
	// untrue, so it is deleted in the same commit as the record. If anything fails
	// from here the list in memory may be ahead of the database, and is dropped.
	touched := map[string]struct{}{}
	fail := func(err error) error {
		e.forgetAll()
		return err
	}
	// Nothing is remembered, and nothing can need invalidating, in a database that
	// has no checkpoints and is not writing any.
	track := e.ckpt.On || e.anyCkpt
	if !track {
		e.complete = false // records are about to be stored that the map will not know of
	}
	for _, p := range puts {
		if track {
			st, err := e.state(p.prefix)
			if err != nil {
				return fail(err)
			}
			if err := e.invalidate(b, p.prefix, st, p.ns); err != nil {
				return fail(err)
			}
			st.latest = max(st.latest, p.ns)
			st.since++
			st.sinceBytes += len(p.value)
			touched[string(p.prefix)] = struct{}{}
		}
		if err := b.Set(p.key, p.value, nil); err != nil {
			return fail(err)
		}
	}
	if err := b.Set(metaKey(pebblekv.MetaLastSeq), pebblekv.EncodeSeq(seq), nil); err != nil {
		return fail(err)
	}
	if e.beforeRecordApply != nil {
		if err := e.beforeRecordApply(); err != nil {
			return fail(err) // tests: a commit that failed without landing
		}
	}
	if err := e.kv.Apply(b, e.kv.WriteOptions()); err != nil {
		return fail(err)
	}
	e.lastSeq.Store(seq)
	e.rec.Count("write.records", int64(len(puts)))
	// The second commit: the checkpoints this write made due. It finishes before
	// Write returns, and its failure is counted, never returned.
	e.writeCheckpoints(touched)
	return nil
}

// Retain implements [engine.Engine]. With SettleRetention it returns only when the
// database has settled or the deadline has passed, holding the engine's lock for
// all of that; Close must not be called meanwhile.
func (e *Engine) Retain(horizon time.Time) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.last = retainPhases{}
	// The clock starts after the lock, so a writer's wait is not counted as work. The
	// work ends at the last commit (workEnd), or at the return on any path that does
	// not reach it.
	start := time.Now()
	var workEnd time.Time
	defer func() {
		if workEnd.IsZero() {
			workEnd = time.Now()
		}
		e.last.work = workEnd.Sub(start)
	}()
	if !horizon.After(e.horizon) {
		return nil
	}
	// The horizon is committed first, on its own: from then on Write refuses
	// anything older, so nothing can arrive for a prefix whose past is being
	// collapsed. If the process stops after it, the prefixes not yet rewritten
	// keep their old records, which answers at and after the horizon never
	// depend on being absent, and only a Retain with a later horizon reclaims the
	// space (one with this horizon is a no-op).
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
		return nil // no instant a record can have is before it: nothing changes, and nothing is forgotten
	}
	// What was remembered of each prefix is about to be out of date. It is
	// worked out again below, and stays empty if the retention does not finish.
	e.forgetAll()
	// The newest instant strictly before the horizon, and where the baseline is
	// keyed. After the end of the range the baseline is keyed at the last instant,
	// inside what the range delete covers, so it is written after the delete.
	oldMax, baseNs := hNs-1, hNs
	if where == pebblekv.After {
		oldMax, baseNs = math.MaxInt64, math.MaxInt64
	}

	lo, hi := dataBounds()
	it, err := e.kv.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return err
	}
	closeIter := sync.OnceFunc(func() { _ = it.Close() })
	defer closeIter()
	// What the writer would find if it read each prefix after this retention,
	// worked out as the retention goes by, so that the first write to a prefix
	// need not read it. Not working it out is always correct (the prefix is then
	// read when it is next touched), so it is done only where it pays: with the
	// checkpoint policy on, where a read would look (a checkpoint may be in the
	// database), where a read is the kind that stops at the tail, and where the
	// horizon is inside the range (past it the retention rewrites every prefix in
	// a way the keys it leaves do not describe). The cost is a read of the keys at
	// or after the horizon of each prefix, up to its newest checkpoint, counted as
	// "retain.state_keys".
	derive := e.ckpt.On && e.anyCkpt && !e.fullStateRead && where == pebblekv.Inside
	derived := derive
	var stateKeys int64
	var next map[string]*prefixState
	if derive {
		next = map[string]*prefixState{}
	}
	remember := func(prefix []byte, st *prefixState) {
		if derive && !st.empty() { // an empty state is what a missing prefix means
			c := *st
			next[string(prefix)] = &c
		}
	}
	b := e.kv.NewBatch()
	defer func() { _ = b.Close() }()

	last := e.lastSeq.Load()
	var visited, replayed, records, baselines, deletes, seeks, commits int64
	commit := func() error {
		if b.Empty() {
			return nil
		}
		if err := e.kv.Apply(b, e.kv.WriteOptions()); err != nil {
			return err
		}
		b.Reset()
		if commits++; e.stopAfter > 0 && commits >= int64(e.stopAfter) {
			return errInjected
		}
		return nil
	}
	for ok := it.First(); ok; {
		key := it.Key()
		if len(key) < prefixLen {
			return fmt.Errorf("pebblelog: key %x is shorter than a prefix", key)
		}
		prefix := slices.Clone(key[:prefixLen])
		dir := prefix[prefixLen-1]
		visited++
		// The keys that stay are read first, from the first key of the prefix,
		// where the iterator is.
		var kept, dropped prefixState
		if derive {
			var parsed bool
			var n int64
			kept, dropped, n, parsed, err = foldRetained(it, prefix, hNs)
			if err != nil {
				return err
			}
			stateKeys += n
			if !parsed {
				derive, next = false, nil // a key that cannot be read: learn nothing here
			}
		}
		// Go to the newest record of this prefix strictly before the horizon. The
		// seek may land in a later prefix, which the loop then takes up.
		seeks++
		ok = it.SeekGE(seekKey(prefix, oldMax))
		if !ok || !hasPrefix(it.Key(), prefix) {
			remember(prefix, &kept) // nothing is rewritten here: a checkpoint at the horizon stays
			continue
		}
		remember(prefix, &dropped)
		replayed++
		decided := map[string]struct{}{}
		var entries []Entry
		consider := func(ref []byte, ns int64, v pebblekv.Value) {
			if _, done := decided[string(ref)]; done {
				return
			}
			decided[string(ref)] = struct{}{}
			if v.Holds(ns, hNs) {
				entries = append(entries, entryOf(ref, ns, v))
			}
		}
		for ; ok && hasPrefix(it.Key(), prefix); ok = it.Next() {
			_, ns, seq, kind, err := parseKey(it.Key())
			if err != nil {
				return err
			}
			switch kind {
			case kindRecord:
				records++
				ref, v, err := decodeRecordValue(dir, it.Value())
				if err != nil {
					return err
				}
				if v.Seq != seq {
					return fmt.Errorf("pebblelog: a record's key has seq %d and its value %d", seq, v.Seq)
				}
				consider(ref, ns, v)
			case kindBaseline:
				st, err := decodeStamp(it.Value())
				if err != nil {
					return err
				}
				if st.Kind != kindBaseline {
					return fmt.Errorf("pebblelog: a baseline key holds a stamp of kind %d", st.Kind)
				}
				for _, en := range st.Entries {
					consider(en.Ref, en.EventNs, en.Value)
				}
			}
		}
		if err := it.Error(); err != nil {
			return err
		}
		// Everything older than the horizon goes, the previous baseline included;
		// what is still alive at the horizon is one new baseline, in the same
		// commit and after the delete.
		if err := b.DeleteRange(seekKey(prefix, oldMax), prefixSucc(prefix), nil); err != nil {
			return err
		}
		deletes++
		if e.anyCkpt && where == pebblekv.Inside {
			// A checkpoint exactly at the horizon would sort before the baseline.
			// It is true (and in a prefix with nothing older, or one this
			// retention did not reach, one may remain, harmlessly: there is no
			// baseline for it to hide). The writer never writes one at or below
			// the horizon, and the ones in the prefixes this retention rewrites go.
			if err := b.Delete(stampKey(prefix, hNs, kindCheckpoint), nil); err != nil {
				return err
			}
		}
		if len(entries) > 0 {
			val, err := appendStamp(nil, Stamp{Kind: kindBaseline, FoldVersion: FoldVersion, Through: last, W: last, Horizon: horizon, Entries: entries})
			if err != nil {
				return err
			}
			if err := b.Set(stampKey(prefix, baseNs, kindBaseline), val, nil); err != nil {
				return err
			}
			baselines++
		}
		if b.Len() >= e.retainBytes {
			if err := commit(); err != nil {
				return err
			}
		}
		seeks++
		ok = it.SeekGE(prefixSucc(prefix))
	}
	if err := it.Error(); err != nil {
		return err
	}
	if err := commit(); err != nil {
		return err
	}
	if derive {
		// Only now, with every commit landed, is what was worked out true of the
		// database. Every prefix was visited, so the map is complete.
		e.states, e.complete = next, true
	}
	if derived {
		e.rec.Count("retain.state_keys", stateKeys)
	}
	e.rec.Count("retain.prefixes_visited", visited)
	e.rec.Count("retain.prefixes_replayed", replayed)
	e.rec.Count("retain.records_replayed", records)
	e.rec.Count("retain.baselines_written", baselines)
	e.rec.Count("retain.range_deletes", deletes)
	e.rec.Count("retain.seeks", seeks)
	workEnd = time.Now()
	// The map is true of the committed data, and settling changes no data, so it
	// comes last. The iterator is closed first: it would pin the tables the
	// compactions replace.
	closeIter()
	return e.settleRetention()
}

// settleRetention is the end of every retention that does not return early, under
// SettleRetention, whether or not it deleted anything: see
// [pebblekv.KV.SettleAfterRetention].
func (e *Engine) settleRetention() error {
	cfg := e.kv.Config()
	if !cfg.SettleRetention {
		return nil
	}
	flush, settle, hit, err := e.kv.SettleAfterRetention(cfg.SettleDeadline)
	e.last.flush, e.last.settle, e.last.deadlineHit = flush, settle, hit
	e.rec.Count("retain.flush_ns", flush.Nanoseconds())
	e.rec.Count("retain.settle_ns", settle.Nanoseconds())
	hits := int64(0)
	if hit {
		hits = 1
	}
	e.rec.Count("retain.settle_deadline_hits", hits)
	return err
}
