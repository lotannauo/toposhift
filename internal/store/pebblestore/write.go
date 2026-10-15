package pebblestore

import (
	"context"
	"fmt"
	"slices"

	"github.com/cockroachdb/pebble/v2"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/pebblekv"
)

// Write implements [store.Store]. It checks every record of the batch, in order
// and by the rules, in the order, of the reference store, before it stores any, so
// a refused batch stores nothing and leaves LastSeq unchanged: the store is closed;
// the context is done; the batch is empty (nil); and then, record by record, the
// record is invalid or its producer is longer than [store.MaxProducerLen], its Seq
// is not above the previous one, its Seq is [store.Latest], it is before the
// horizon of its layer ([store.ErrBeforeHorizon]), it puts a subject in two layers
// within the batch, and, for an entity record under a policy that names a boot
// key, the policy cannot fold it. The context is checked again just before the
// commit, and never after it: a context error means nothing was stored.
//
// The batch is one commit, carrying both copies of every edge, the new last
// sequence number and the deletion of every checkpoint a record of the batch makes
// untrue (in both copies of an edge). Then, if the checkpoint policy finds a prefix
// due, a second commit writes the checkpoints; it is not synced, and an error in it
// is counted ("checkpoint.errors"), never returned, because the records are already
// stored. If the first commit reports an error, the store sets LastSeq to what the
// database shows and refuses every later Write and Retain until it is reopened: the
// outcome is final only then, when the log is replayed (see
// [Store.uncertainCommit]). In practice Pebble ends the process on such an error
// (see package pebblekv), so this is the last line of defence.
//
// In a Background store, while a retention's pass is pending, the records of a
// prefix the pass has not yet reached come with the pass's rewrite of that prefix,
// in the same commit, so that the prefix is as the pass would leave it (see
// [Store.Retain]). A Write that fails before its commit leaves the prefix to the
// pass. The exception is a prefix whose state the publication kept, because the
// retention cannot change it: its records come beside the keys the step has yet to
// remove, and the pass runs the step (see the package comment).
//
// A subject stored in a layer other than the one a record names is not detected
// when the other layer was written by an earlier batch: the contract leaves that
// precondition to the caller, because finding out costs a lookup per record.
func (s *Store) Write(ctx context.Context, batch []store.Record) error {
	var ph writePhases
	if s.closed.Load() {
		return closedError("Write")
	}
	if err := ctx.Err(); err != nil {
		return contextError("Write", err)
	}
	if len(batch) == 0 {
		return nil
	}
	// Validating folds an assertion, which costs, and says nothing about the
	// store's state, so it is done before the lock is taken; its verdict on each
	// record is reported at that record's turn below, so the order of the checks
	// is unchanged. Every lock is released by defer: a panic that a caller
	// recovers cannot leave the store locked.
	validateStart := s.now()
	invalid := make([]error, len(batch))
	for i, r := range batch {
		invalid[i] = r.Validate()
		if invalid[i] == nil {
			// The limit on a producer name is the write path's, not Validate's.
			if err := store.WritableProducer(r.Producer); err != nil {
				invalid[i] = fmt.Errorf("record seq %d: %w", r.Seq, err)
			}
		}
		if invalid[i] == nil {
			// The codec is the last defence against a value it cannot encode.
			if err := s.checkValue(pebblekv.FromRecord(r)); err != nil {
				invalid[i] = fmt.Errorf("record seq %d: %w: %w", r.Seq, err, store.ErrInvalid)
			}
		}
	}
	ph.validate = s.since(validateStart)
	// A background chunk of a retention ends early for a write that waits (see
	// [retainPass.yields]); no other caller says it waits.
	s.writersWaiting.Add(1)
	s.mu.Lock()
	s.writersWaiting.Add(-1)
	defer s.mu.Unlock()
	s.flushCounts()
	if s.failed != nil {
		return s.failedError("Write")
	}

	type put struct {
		prefix, key, value []byte
		ns                 int64
	}
	prev := s.lastSeq.Load()
	inBatch := make(map[store.Subject]catalog.Layer)
	puts := make([]put, 0, 2*len(batch))
	for i, r := range batch {
		fail := func(format string, args ...any) error {
			return fmt.Errorf("pebblestore: Write: batch[%d] seq %d: %s: %w", i, r.Seq, fmt.Sprintf(format, args...), store.ErrInvalid)
		}
		if err := invalid[i]; err != nil {
			return fmt.Errorf("pebblestore: Write: batch[%d]: %w", i, err)
		}
		if r.Seq <= prev {
			return fail("seq is not above the previous seq %d", prev)
		}
		if r.Seq == store.Latest {
			return fail("seq %d is the snapshot token Latest, which no record may carry", r.Seq)
		}
		prev = r.Seq
		if h := s.horizonOf(r.Layer); !h.IsZero() && r.EventTime.Before(h.Time) {
			return fmt.Errorf("pebblestore: Write: batch[%d] seq %d: event time %s is before the horizon %s: %w",
				i, r.Seq, formatTime(r.EventTime), formatTime(h.Time), store.ErrBeforeHorizon)
		}
		if known, ok := inBatch[r.Subject]; ok && known != r.Layer {
			return fail("subject is stored in layer %s, not %s", known, r.Layer)
		}
		inBatch[r.Subject] = r.Layer
		if r.Subject.Kind == store.SubjectEntity && s.policy.BootKey != "" {
			if _, err := lifecycle.Fold([]lifecycle.Assertion{r.Assertion()}, s.policy); err != nil {
				return fmt.Errorf("pebblestore: Write: batch[%d] seq %d: the store's policy cannot fold the record: %w: %w",
					i, r.Seq, store.ErrInvalid, err)
			}
		}
		sides, err := s.sidesOf(r)
		if err != nil {
			return fmt.Errorf("pebblestore: Write: batch[%d] seq %d: %w", i, r.Seq, err)
		}
		v := pebblekv.FromRecord(r)
		ns := r.EventTime.UnixNano()
		for _, sd := range sides {
			puts = append(puts, put{sd.prefix, recordKey(sd.prefix, ns, r.Seq), appendRecordValue(nil, sd.ref, v), ns})
		}
	}

	// In a Background store, while a pass is pending, a prefix the pass has not yet
	// reached is rewritten first, in this very batch: the pass's step for it is
	// committed with the records, so that the keys left are the ones the pass would
	// have left, whatever the order of the two. The prefixes are found now, before
	// the batch is made, because the step reads the prefix through the batch, which
	// must then be indexed. A value of true says the prefix has been rewritten in
	// this batch (two puts of one prefix rewrite it once).
	//
	// The exception is a prefix whose state the publication kept, because it is exactly
	// what a read finds and the retention cannot change it (see [Store.keepLayers]):
	// the step is left to the pass, unless the write would make the step needed first
	// (see [Store.stepMustRun]). Such a prefix is read for nothing, its state is the one
	// kept, and the write puts its records beside keys the step has yet to remove.
	track := s.ckpt.On || s.anyCkpt
	var rewrite map[string]bool
	var leave map[string]*putSpan
	if s.pass != nil {
		for _, p := range puts {
			k := string(p.prefix)
			if sp := leave[k]; sp != nil {
				sp.add(p.ns, len(p.value))
				continue
			}
			if _, done := rewrite[k]; done {
				continue
			}
			switch {
			case track && s.pass.leavesStep(p.prefix):
				if leave == nil {
					leave = map[string]*putSpan{}
				}
				leave[k] = newPutSpan(p.ns, len(p.value))
			case s.pass.owes(p.prefix):
				if rewrite == nil {
					rewrite = map[string]bool{}
				}
				rewrite[k] = false
			}
		}
	}
	var b *pebble.Batch
	if len(rewrite) > 0 {
		b = s.kv.NewIndexedBatch()
	} else {
		b = s.kv.NewBatch()
	}
	defer func() { _ = b.Close() }()
	// A record with an event time before a checkpoint's makes that checkpoint
	// untrue, so it is deleted in the same commit as the record. From here on the
	// list of checkpoints in memory may be ahead of the database, and it is dropped
	// unless the Write goes through to its end: on any error, a context that ends
	// before the commit, or a panic that a caller recovers. (After a commit that
	// failed the store is stopped, and a reopening reads the state afresh; the drop
	// matters for the errors that leave it running.)
	finished := false
	defer func() {
		if !finished {
			s.forgetAll()
		}
		s.states.unpinAll() // the states this Write touched were kept from eviction until now
	}()
	s.iterators = 0
	clear(s.invalidated)
	s.invalidated = s.invalidated[:0]
	// Nothing is remembered, and nothing can need invalidating, in a database that
	// has no checkpoints and is not writing any.
	if !track {
		s.lose() // records are about to be stored that the map will not know of
	}
	touched := map[string]struct{}{}
	rewrites := 0
	var deferred, fallbacks map[string]struct{}
	for _, p := range puts {
		if done, owed := rewrite[string(p.prefix)]; owed && !done {
			// Before anything else is done to the prefix: the state, the checkpoints to
			// delete and the record itself all follow what the rewrite leaves.
			if err := s.rewriteForWrite(b, p.prefix, track); err != nil {
				return fmt.Errorf("pebblestore: Write: %w", err)
			}
			rewrite[string(p.prefix)] = true
			rewrites++
		}
		if track {
			started := s.now()
			st, err := s.state(p.prefix)
			ph.state += s.since(started)
			if err != nil {
				return fmt.Errorf("pebblestore: Write: %w", err)
			}
			s.states.pin(string(p.prefix), st)
			if sp := leave[string(p.prefix)]; sp != nil && !sp.decided {
				sp.decided = true
				// The step runs first, before anything else is done to the prefix, if it must;
				// else it is left to the pass.
				_, was := s.pass.deferred[string(p.prefix)]
				if s.stepMustRun(p.prefix, st, sp) {
					if err := s.touchPrefix(b, p.prefix); err != nil {
						return fmt.Errorf("pebblestore: Write: %w", err)
					}
					if fallbacks == nil {
						fallbacks = map[string]struct{}{}
					}
					fallbacks[string(p.prefix)] = struct{}{}
					if !was {
						rewrites++ // written before the pass reached it, as a rewrite
					}
				} else if !was {
					if deferred == nil {
						deferred = map[string]struct{}{}
					}
					deferred[string(p.prefix)] = struct{}{}
				}
			}
			started = s.now()
			err = s.invalidate(b, p.prefix, st, p.ns)
			ph.invalidate += s.since(started)
			if err != nil {
				return fmt.Errorf("pebblestore: Write: %w", err)
			}
			st.latest = max(st.latest, p.ns)
			st.since++
			st.sinceBytes += len(p.value)
			if st.exact {
				// The record is at or after the newest checkpoint, or the invalidation
				// above has cleared exact: a read counts it.
				st.readSince++
				st.readSinceBytes += len(p.value)
			}
			touched[string(p.prefix)] = struct{}{}
		}
		// The prefix is known to hold keys from before the commit, whatever the
		// commit reports: one that reports failure may have landed. (Not before its
		// state is asked for: a prefix this batch creates is one the map knows to
		// hold nothing until now.)
		s.written(p.prefix)
		if err := b.Set(p.key, p.value, nil); err != nil {
			return fmt.Errorf("pebblestore: Write: %w", err)
		}
	}
	if err := b.Set(metaKey(pebblekv.MetaLastSeq), pebblekv.EncodeSeq(prev), nil); err != nil {
		return fmt.Errorf("pebblestore: Write: %w", err)
	}
	// The last look at the context: past this point the batch is committed or not,
	// and a context error would claim it was not.
	if err := ctx.Err(); err != nil {
		return contextError("Write", err)
	}
	started := s.now()
	err := s.commitRecords(b)
	ph.recordCommit = s.since(started)
	if err != nil {
		return s.uncertainCommit("Write", err)
	}
	s.lastSeq.Store(prev)
	s.reportInvalidated()
	s.rec.Count("write.records", int64(len(puts)))
	// The second commit: the checkpoints this write made due. It finishes before
	// Write returns, and its failure is counted, never returned.
	s.writeCheckpoints(touched, &ph)
	// The prefixes are the pass's no more only now that the batch that rewrote them
	// is committed; a Write that failed before this leaves them to the pass.
	for k := range rewrite {
		s.pass.touched[k] = struct{}{}
	}
	for k := range fallbacks {
		s.pass.touched[k] = struct{}{}
	}
	for k := range deferred {
		s.pass.deferred[k] = struct{}{}
	}
	if rewrites > 0 {
		s.rec.Count("retain.touch_rewrites", int64(rewrites))
	}
	if len(deferred) > 0 {
		s.rec.Count("retain.touch_deferred", int64(len(deferred)))
	}
	if len(fallbacks) > 0 {
		s.rec.Count("retain.touch_fallback", int64(len(fallbacks)))
	}
	finished = true
	if s.iterators > 0 {
		s.rec.Count("write.iterators", s.iterators)
	}
	if s.timed {
		s.rec.Sample("write.phase_ns.validate", ph.validate)
		s.rec.Sample("write.phase_ns.state", ph.state)
		s.rec.Sample("write.phase_ns.invalidate", ph.invalidate)
		s.rec.Sample("write.phase_ns.record_commit", ph.recordCommit)
		s.rec.Sample("write.phase_ns.ckpt_build", ph.ckptBuild)
		s.rec.Sample("write.phase_ns.ckpt_commit", ph.ckptCommit)
	}
	return nil
}

// putSpan is what one Write puts in a prefix a write may leave to the pass: the
// earliest and latest event time, how many records and the bytes of their values.
// decided says the Write has settled whether to run the step first.
type putSpan struct {
	min, max int64
	n, bytes int
	decided  bool
}

func newPutSpan(ns int64, valueLen int) *putSpan {
	return &putSpan{min: ns, max: ns, n: 1, bytes: valueLen}
}

func (sp *putSpan) add(ns int64, valueLen int) {
	sp.min, sp.max = min(sp.min, ns), max(sp.max, ns)
	sp.n++
	sp.bytes += valueLen
}

// stepMustRun says the pass's step must run on a prefix whose state is kept or left
// to the pass before this Write puts its records in it, because the records would
// change what the step leaves, or what a checkpoint built after them reads. st is the
// writer's state of the prefix, and sp what the Write puts in it. The step runs when:
//
//   - a record is before the newest checkpoint known: the write deletes that
//     checkpoint, and the state then no longer stands on a checkpoint above the
//     horizon, which is what makes the step no change to what a read finds;
//   - or the records leave the prefix due for a checkpoint that is not already there,
//     with none known between the horizon and it: the walk that builds it would go on
//     into the keys the step removes, and the checkpoint would hold what it finds
//     there, where the step's baseline stands in the synchronous store.
//
// A state with no checkpoint known has nothing to stand on, and the step runs.
func (s *Store) stepMustRun(prefix []byte, st *prefixState, sp *putSpan) bool {
	n := len(st.ckpts)
	if n == 0 || sp.min < st.ckpts[n-1] {
		return true
	}
	after := *st
	after.latest = max(after.latest, sp.max)
	after.since += sp.n
	after.sinceBytes += sp.bytes
	if !s.ckpt.due(&after) {
		return false
	}
	c, ok := s.ckpt.at(&after)
	layer := layerOfPrefix(prefix)
	if !ok || !s.placeable(layer, c) {
		return false
	}
	if _, exists := slices.BinarySearch(st.ckpts, c); exists {
		return false // nothing is built
	}
	hNs := s.pass.lay[int(layer)-int(catalog.L0)].hNs
	for _, k := range st.ckpts {
		if k > hNs && k < c {
			return false // the build stops at a checkpoint above the horizon
		}
	}
	return true
}

// writePhases is where the time of one Write went, in nanoseconds; see [Recorder].
type writePhases struct {
	validate, state, invalidate, recordCommit, ckptBuild, ckptCommit int64
}

// commitRecords commits a batch of records, synced if the database is.
func (s *Store) commitRecords(b *pebble.Batch) error {
	if s.beforeRecordApply != nil {
		if err := s.beforeRecordApply(); err != nil {
			return err // tests: a commit that failed without landing
		}
	}
	if s.beforeApply != nil {
		s.beforeApply(b)
	}
	if err := s.kv.Apply(b, s.kv.WriteOptions()); err != nil {
		return err
	}
	if s.afterRecordApply != nil {
		return s.afterRecordApply() // tests: a commit that landed, reported as failed
	}
	return nil
}

// uncertainCommit handles an error from the commit of a batch. Pebble applies a
// batch to its memtable, and so makes it visible to reads, before it reports that
// the log could not be synced; and a failure to apply it after the log was
// written leaves a batch that is replayed when the database is opened again. So
// an error says neither that the batch landed nor that it did not, and not even
// reading it back settles it: what is visible now may not survive a crash, and
// what is not may come back. The store therefore stops accepting writes and
// retentions at once (reads continue), whatever the error, until it is reopened;
// the log's replay then decides, and the batch is whole or absent. LastSeq is set
// to what the database shows, the best that is known. The caller holds s.mu.
func (s *Store) uncertainCommit(op string, commitErr error) error {
	if err := s.rereadLastSeq(); err != nil {
		s.failed = fmt.Errorf("a commit failed (%w), and the last sequence number cannot be read back (%w): reopen the store", commitErr, err)
	} else {
		s.failed = fmt.Errorf("a commit failed (%w), so whether its batch is stored is decided only when the store is reopened: reopen the store", commitErr)
	}
	return fmt.Errorf("pebblestore: %s: %w", op, s.failed)
}

// rereadLastSeq sets LastSeq to the last sequence number the database holds.
func (s *Store) rereadLastSeq() error {
	if s.rereadFails != nil {
		if err := s.rereadFails(); err != nil {
			return err
		}
	}
	raw, err := s.kv.GetMeta(metaKey(pebblekv.MetaLastSeq))
	if err != nil {
		return err
	}
	seq, err := pebblekv.DecodeSeq(raw)
	if err != nil {
		return err
	}
	s.lastSeq.Store(seq)
	return nil
}
