package pebblestore

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/pebblekv"
)

// Retain implements [store.Store]. It lets the store discard history before
// horizon, in four steps.
//
// First the horizon of every layer it moves is committed on its own, with the
// last sequence number L at that moment, together with a marker that says a
// retention is under way (see [retainMarker]), and published to readers and
// writers: from then on a write older than it, and a read of an instant before it
// or of a token below L, is refused, so nothing can arrive for a prefix whose
// past is being collapsed, and no read can ask for what is being discarded. If the
// process stops after it, the prefixes not yet rewritten keep their old records,
// which no answer at or after the horizon depends on being absent, and the marker
// stays, so that the next Open finishes the work before it returns. A Retain with
// this horizon again is a no-op, whatever the marker says (so a pass that was
// interrupted in this process, by its context or an error, is finished by the next
// Open or by a Retain with a later horizon, which starts the work over from the
// first key, under a new generation of the marker).
//
// Then each prefix that has history before the horizon is rewritten in one commit:
// the prefix is replayed, a baseline at the horizon holds every reference still
// alive at it, and everything older is range-deleted, the previous baseline
// included. The prefixes are taken in key order, in chunks. A chunk ends between
// two prefixes, once its commit holds about 4 MiB or it has been running for 20 ms,
// and is committed, not synced, together with the marker's new resume key, the
// first key of the next prefix: so after any chunk every prefix before the resume
// key is rewritten and none after it has been touched, and a crash that loses the
// chunk loses its resume key with it. Each chunk reads through an iterator of its
// own, opened when it begins, so that it sees what the chunks before it committed.
//
// Last, the marker passes to its settling phase in the commit of the last chunk;
// with Config.SettleRetention the database is flushed and waited on until it is at
// rest, or until the deadline passes (see [pebblekv.KV.SettleAfterRetention]); and
// then the marker is deleted, if it is still this retention's: it carries a
// generation, and a retention that finds another's marker leaves it.
//
// A prefix with a boot in a record it would discard is kept whole when the store's
// policy names a boot key (see the package comment). A Retain that does not move
// any layer's horizon changes nothing and does not raise LastSeq. A Retain moves
// the horizon of each layer that is not kept to the horizon less the layer's
// offset, and only where that is later than the layer's own: the others keep
// theirs, and their data is left alone. It holds the store's lock for as long as
// it runs, settling included, so Close must not be called meanwhile; the lock is
// not released between chunks.
//
// What the writer remembers of each prefix (see checkpoint.go) is out of date once
// a retention begins to rewrite, so it is dropped then. Where the checkpoint policy
// is on and the database may hold a checkpoint, the retention works out what a
// read of each prefix would find from the keys it leaves, and when it has done so
// for every prefix, after its last commit, the writer's memory is complete again
// and the first write to a prefix needs no read. The keys it reads for that are
// counted as "retain.state_keys". A retention that stops or fails leaves the
// memory empty, and the prefixes are read as they are touched; so does one that
// Open finishes.
func (s *Store) Retain(ctx context.Context, horizon time.Time) error {
	if s.closed.Load() {
		return closedError("Retain")
	}
	if err := ctx.Err(); err != nil {
		return contextError("Retain", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed != nil {
		return s.failedError("Retain")
	}
	s.last = retainPhases{}
	// The clock starts after the lock, so a writer's wait is not counted as work.
	// The work ends at the last commit (workEnd), or at the return on any path that
	// does not reach it.
	start := time.Now()
	var workEnd time.Time
	defer func() {
		if workEnd.IsZero() {
			workEnd = time.Now()
		}
		s.last.work = workEnd.Sub(start)
	}()
	horizon = horizon.UTC()
	// The layers whose horizon this moves, and the horizon each is moved to: the
	// horizon less the layer's offset, for each layer that is not kept, and only
	// where that is after the layer's own. A layer whose horizon it would not move
	// keeps its own, and its data is not touched.
	var moved [layers]bool
	var to [layers]time.Time
	anyMoved := false
	for i, cur := range s.horizons.Load() {
		if s.keep[i] {
			continue
		}
		if h := horizon.Add(-s.offsets[i]); h.After(cur.Time) {
			moved[i], to[i], anyMoved = true, h, true
		}
	}
	if !anyMoved {
		return nil
	}
	last := s.lastSeq.Load()
	// No instant a record can have is before a horizon at the start of the range, or
	// before it: a layer moved to one changes nothing, and so is not in the marker.
	// A retention that moves layers only so far leaves no marker.
	var marker *retainMarker
	allInside := true
	for i, mv := range moved {
		if !mv {
			continue
		}
		hNs, where := pebblekv.Locate(to[i])
		if where == pebblekv.Before || (where == pebblekv.Inside && hNs == 0) {
			continue
		}
		allInside = allInside && where == pebblekv.Inside
		if marker == nil {
			lo, _ := dataBounds()
			marker = &retainMarker{generation: s.retainGen + 1, resume: lo, phase: phaseRewrite}
		}
		marker.layers = append(marker.layers, retainLayer{layer: catalog.L0 + catalog.Layer(i), horizon: to[i], last: last})
	}
	if err := s.publishHorizon(to, last, moved, marker); err != nil {
		return err
	}
	if marker == nil {
		return nil // nothing changes, and nothing is forgotten
	}
	s.retainGen = marker.generation
	// What was remembered of each prefix of a layer about to be rewritten is out of
	// date. It is worked out again below, and stays forgotten if the retention does
	// not finish. What is remembered of the other layers is as true as it was.
	wasComplete := s.complete
	s.forgetLayers(marker.layers)

	// What the writer would find if it read each prefix after this retention, worked
	// out as the retention goes by, so that the first write to a prefix need not
	// read it. Not working it out is always correct (the prefix is then read when it
	// is next touched), so it is done only where it pays: with the checkpoint policy
	// on, where a read would look (a checkpoint may be in the database), where a
	// read is the kind that stops at the tail, and where every horizon the pass
	// rewrites to is inside the range (past it the retention rewrites every prefix
	// in a way the keys it leaves do not describe). The prefixes of the layers the
	// pass leaves alone keep what is remembered of them, and the map is complete
	// afterwards if it was before or if the pass rewrote every layer, since it
	// vouches for the layers it rewrites and for no other. The cost is a read of
	// the keys at or after the horizon of each prefix, up to its newest checkpoint,
	// counted as "retain.state_keys".
	derive := s.ckpt.On && s.anyCkpt && !s.fullStateRead && allInside
	p, err := s.newPass(ctx, *marker, derive, s.stopAfter)
	if err != nil {
		return err
	}
	defer p.close()
	p.wasComplete = wasComplete
	err = p.run()
	workEnd = p.workEnd
	return err
}

// resumeRetention finishes the retention the marker found at Open stands for, and
// returns when it is done: from the resume key if the marker is in its rewriting
// phase, or with the settling alone if it is in its last. Nothing is worked out of
// the writer's state, and nothing is remembered: the first write to a prefix reads
// it. It runs before the store is handed out, so nothing else holds the lock.
func (s *Store) resumeRetention(m retainMarker) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.last = retainPhases{}
	start := time.Now()
	s.forgetAll()
	s.rec.Count("retain.resumed", 1)
	p, err := s.newPass(context.Background(), m, false, s.resumeStopAfter)
	if err != nil {
		return err
	}
	defer p.close()
	err = p.run()
	if p.workEnd.IsZero() {
		p.workEnd = time.Now()
	}
	s.last.work = p.workEnd.Sub(start)
	return err
}

// passLayer is what a pass needs to rewrite one layer.
type passLayer struct {
	horizon time.Time
	last    uint64
	hNs     int64
	where   pebblekv.Where
	// oldMax is the newest instant strictly before the horizon, and baseNs where the
	// baseline is keyed. After the end of the range the baseline is keyed at the
	// last instant, inside what the range delete covers, so it is written after the
	// delete.
	oldMax, baseNs int64
}

// retainPass is the work of one generation of the marker: the rewriting of every
// prefix of the layers it moves, in chunks, and what follows. It is built from the
// marker alone, so that the pass Open resumes is the pass that was stopped.
type retainPass struct {
	s   *Store
	ctx context.Context
	// marker is the marker as last committed: its resume key is where the next chunk
	// begins, and its phase says what is left.
	marker    retainMarker
	stopAfter int

	// moved says which layers the pass rewrites, and lay holds what it needs of each
	// (the layers may be moved to different horizons).
	moved [layers]bool
	lay   [layers]passLayer
	boots bool

	// derive says the writer's state is being worked out as the pass goes by; next
	// is what has been worked out; derived says it was meant to be, for the count
	// of the keys read. wasComplete says the writer's map was complete when the
	// retention began, and so is again when the pass ends having worked out the
	// state of the layers it rewrote.
	derive, derived bool
	wasComplete     bool
	next            map[string]*prefixState
	stateKeys       int64

	b       *pebble.Batch
	commits int
	chunks  int64
	workEnd time.Time

	visited, replayed, records, baselines, deletes, seeks, kept, maxPrefixRecords int64
}

// newPass makes the pass the marker stands for, or refuses a marker that does not
// stand for one: a layer moved to a horizon that rewrites nothing, which has no
// place in a marker.
func (s *Store) newPass(ctx context.Context, m retainMarker, derive bool, stopAfter int) (*retainPass, error) {
	p := &retainPass{s: s, ctx: ctx, marker: m, stopAfter: stopAfter, derive: derive, derived: derive, boots: s.policy.BootKey != ""}
	for _, l := range m.layers {
		i := int(l.layer) - int(catalog.L0)
		lp := passLayer{horizon: l.horizon.UTC(), last: l.last}
		lp.hNs, lp.where = pebblekv.Locate(lp.horizon)
		if lp.where == pebblekv.Before || (lp.where == pebblekv.Inside && lp.hNs == 0) {
			return nil, fmt.Errorf("pebblestore: the retention marker has a horizon that rewrites nothing: %w", store.ErrInvalid)
		}
		lp.oldMax, lp.baseNs = lp.hNs-1, lp.hNs
		if lp.where == pebblekv.After {
			lp.oldMax, lp.baseNs = math.MaxInt64, math.MaxInt64
		}
		p.moved[i], p.lay[i] = true, lp
	}
	if p.derive {
		p.next = map[string]*prefixState{}
	}
	p.b = s.kv.NewBatch()
	return p, nil
}

func (p *retainPass) close() { _ = p.b.Close() }

// run does what is left of the pass: the chunks, then the settling, then the
// removal of the marker.
func (p *retainPass) run() error {
	s := p.s
	for p.marker.phase == phaseRewrite {
		if err := p.chunk(); err != nil {
			return err
		}
	}
	if p.derive {
		// Only now, with every commit landed, is what was worked out true of the
		// database. Every prefix of the layers rewritten was visited, so the map is
		// complete if it was before, and also if every layer was rewritten, since
		// then nothing is left that it has not been told of.
		maps.Copy(s.states, p.next)
		s.complete = p.wasComplete || !slices.Contains(p.moved[:], false)
	}
	if p.derived {
		s.rec.Count("retain.state_keys", p.stateKeys)
	}
	s.rec.Count("retain.prefixes_visited", p.visited)
	s.rec.Count("retain.prefixes_replayed", p.replayed)
	s.rec.Count("retain.prefixes_kept_for_boots", p.kept)
	s.rec.Count("retain.records_replayed", p.records)
	s.rec.Count("retain.baselines_written", p.baselines)
	s.rec.Count("retain.range_deletes", p.deletes)
	s.rec.Count("retain.seeks", p.seeks)
	s.rec.Sample("retain.max_prefix_records", p.maxPrefixRecords)
	s.rec.Sample("retain.chunks", p.chunks)
	p.workEnd = time.Now()
	// Settling changes no data, and no iterator is open: it would pin the tables
	// the compactions replace.
	if err := s.settleRetention(); err != nil {
		return err
	}
	return s.releaseMarker(p.marker.generation)
}

// chunk rewrites prefixes from the marker's resume key until its commit holds
// retainBytes or it has run for the chunk time, and commits them with the
// marker's next state. A chunk ends only between prefixes. It reads through an
// iterator of its own, opened here.
func (p *retainPass) chunk() error {
	s := p.s
	begin := time.Now()
	_, hi := dataBounds()
	it, err := s.kv.NewIter(&pebble.IterOptions{LowerBound: p.marker.resume, UpperBound: hi})
	if err != nil {
		return err
	}
	closeIter := sync.OnceFunc(func() { _ = it.Close() })
	defer closeIter()
	var lastPrefix []byte
	exhausted := true
	for ok := it.First(); ok; {
		key := it.Key()
		if len(key) < prefixLen {
			return fmt.Errorf("pebblestore: key %x is shorter than a prefix", key)
		}
		prefix := slices.Clone(key[:prefixLen])
		if l, known := pebblekv.LayerFromByte(prefix[0]); !known || !p.moved[int(l)-int(catalog.L0)] {
			p.seeks++
			ok = it.SeekGE([]byte{prefix[0] + 1}) // the whole layer is left as it is
			continue
		}
		if ok, err = p.rewritePrefix(it, prefix); err != nil {
			return err
		}
		lastPrefix = prefix
		if ok && (p.b.Len() >= s.retainBytes || time.Since(begin) >= s.chunkTime) {
			exhausted = false
			break
		}
	}
	if err := it.Error(); err != nil {
		return err
	}
	// The first key not looked at is the first of whatever follows the last prefix
	// done; once there is nothing left, the rewriting is over.
	next := p.marker
	if exhausted {
		next.resume, next.phase = []byte{}, phaseSettle
	} else {
		next.resume = prefixSucc(lastPrefix)
		// The prefix just done is at or after the resume key, so its successor is past
		// it. A pass that did not move on would never end.
		if bytes.Compare(next.resume, p.marker.resume) <= 0 {
			return fmt.Errorf("pebblestore: a chunk of the retention ended at %x, not past where it began, %x", next.resume, p.marker.resume)
		}
	}
	closeIter()
	return p.commit(next, begin)
}

// commit commits the batch with the marker next in it, not synced: the horizon is
// already published and durable, and a later synced commit keeps this one ordered
// behind, so a crash that loses it loses the marker's move with it.
func (p *retainPass) commit(next retainMarker, begin time.Time) error {
	s := p.s
	raw, err := appendRetainMarker(nil, next)
	if err != nil {
		return err
	}
	if err := p.b.Set(metaKey(metaRetain), raw, nil); err != nil {
		return err
	}
	if err := p.ctx.Err(); err != nil {
		return contextError("Retain", err)
	}
	if err := s.kv.Apply(p.b, pebble.NoSync); err != nil {
		return err
	}
	p.marker = next
	p.b.Reset()
	p.chunks++
	s.rec.Sample("retain.chunk_hold_ns", time.Since(begin).Nanoseconds())
	if s.afterRetainCommit != nil {
		s.afterRetainCommit()
	}
	if p.commits++; p.stopAfter > 0 && p.commits >= p.stopAfter {
		return errInjected
	}
	return nil
}

// remember keeps the state the writer would find in a prefix, if the pass is
// working that out and the state is not empty (an empty state is what a missing
// prefix means).
func (p *retainPass) remember(prefix []byte, st *prefixState) {
	if p.derive && !st.empty() {
		c := *st
		p.next[string(prefix)] = &c
	}
}

// rewritePrefix rewrites the prefix the iterator is at the first key of, into the
// pass's batch: a baseline at the horizon holds what is alive at it, and everything
// older goes. It leaves the iterator at the first key after the prefix, or, when
// nothing in the prefix is older than the horizon, wherever the seek past that
// instant landed, which is also after it; ok is false when no key is left.
func (p *retainPass) rewritePrefix(it *pebble.Iterator, prefix []byte) (ok bool, err error) {
	s, b := p.s, p.b
	lp := &p.lay[int(layerOfPrefix(prefix))-int(catalog.L0)]
	hNs, horizon := lp.hNs, lp.horizon
	dir := prefix[prefixLen-1]
	p.visited++
	// The keys that stay are read first, from the first key of the prefix, where
	// the iterator is.
	var stays, dropped prefixState
	if p.derive {
		var parsed bool
		var n int64
		stays, dropped, n, parsed, err = foldRetained(it, prefix, hNs)
		if err != nil {
			return false, err
		}
		p.stateKeys += n
		if !parsed {
			p.derive, p.next = false, nil // a key that cannot be read: learn nothing here
		}
	}
	// Go to the newest record of this prefix strictly before the horizon. The
	// seek may land in a later prefix, which the loop then takes up.
	p.seeks++
	ok = it.SeekGE(seekKey(prefix, lp.oldMax))
	if !ok || !hasPrefix(it.Key(), prefix) {
		p.remember(prefix, &stays) // nothing is rewritten here: a checkpoint at the horizon stays
		return ok, nil
	}
	p.replayed++
	var prefixRecords int64
	decided := map[string]struct{}{}
	var entries []entry
	bootSeen := false
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
			return false, err
		}
		switch kind {
		case kindRecord:
			p.records++
			prefixRecords++
			ref, v, err := decodeRecordValue(dir, it.Value())
			if err != nil {
				return false, err
			}
			if v.Seq != seq {
				return false, fmt.Errorf("pebblestore: a record's key has seq %d and its value %d", seq, v.Seq)
			}
			bootSeen = bootSeen || v.Boot != ""
			consider(ref, ns, v)
		case kindBaseline:
			st, err := decodeStamp(it.Value())
			if err != nil {
				return false, err
			}
			if st.kind != kindBaseline {
				return false, fmt.Errorf("pebblestore: a baseline key holds a stamp of kind %d", st.kind)
			}
			for _, en := range st.entries {
				consider(en.ref, en.eventNs, en.value)
			}
		}
	}
	if err := it.Error(); err != nil {
		return false, err
	}
	p.maxPrefixRecords = max(p.maxPrefixRecords, prefixRecords)
	if p.boots && dir == dirEntity && bootSeen {
		// A later record of this entity may collide with a boot in what would
		// be discarded, and the baseline keeps no boot history: the prefix
		// stays whole, which the contract allows.
		p.kept++
		// Nothing in it changes, so what the writer would find is what a read of
		// it finds now. Neither of the states worked out from the keys at or after
		// the horizon is that: the checkpoint at the horizon stays, and older
		// checkpoints and records may stay below it.
		if p.derive {
			res, n, unreadable, rerr := s.readPrefixState(prefix)
			if rerr != nil {
				return false, rerr
			}
			p.stateKeys += n
			if unreadable != nil {
				p.derive, p.next = false, nil // a key that cannot be read: learn nothing here
			} else {
				p.remember(prefix, &res)
			}
		}
	} else {
		p.remember(prefix, &dropped)
		// Everything older than the horizon goes, the previous baseline
		// included; what is still alive at the horizon is one new baseline, in
		// the same commit and after the delete.
		if err := b.DeleteRange(seekKey(prefix, lp.oldMax), prefixSucc(prefix), nil); err != nil {
			return false, err
		}
		p.deletes++
		if s.anyCkpt && lp.where == pebblekv.Inside {
			// A checkpoint exactly at the horizon would sort before the
			// baseline. The writer never writes one at or below the horizon,
			// and the ones in the prefixes this retention rewrites go.
			if err := b.Delete(stampKey(prefix, hNs, kindCheckpoint), nil); err != nil {
				return false, err
			}
		}
		if len(entries) > 0 {
			val, err := appendStamp(nil, stamp{kind: kindBaseline, foldVersion: foldVersion, through: lp.last, w: lp.last, horizon: horizon, entries: entries})
			if err != nil {
				return false, err
			}
			if err := b.Set(stampKey(prefix, lp.baseNs, kindBaseline), val, nil); err != nil {
				return false, err
			}
			p.baselines++
		}
	}
	p.seeks++
	return it.SeekGE(prefixSucc(prefix)), nil
}

// releaseMarker deletes the marker, if it is generation gen's: a retention that
// finishes must not delete the marker of one that began after it. It reads the
// marker first for that, and the delete is not synced, like the commits it ends.
func (s *Store) releaseMarker(gen uint64) error {
	raw, err := s.kv.GetMeta(metaKey(metaRetain))
	if err != nil {
		return err
	}
	if raw == nil {
		return nil
	}
	m, err := decodeRetainMarker(raw)
	if err != nil {
		return err
	}
	if m.generation != gen {
		return nil
	}
	b := s.kv.NewBatch()
	defer func() { _ = b.Close() }()
	if err := b.Delete(metaKey(metaRetain), nil); err != nil {
		return err
	}
	return s.kv.Apply(b, pebble.NoSync)
}

// publishHorizon commits the new horizon of the layers moved, and only those, each
// with the sequence number L, and the "horizon/last" key naming the latest of them,
// in a commit of its own and synced if the database is, and then publishes it to
// readers and writers. The marker of the retention that will rewrite below it, if
// there is one, is in the same commit: a horizon that is stored without it would
// leave a restart nothing to finish, and a marker without the horizon would send
// the restart to rewrite below one it does not refuse writes under. The commit
// comes first: a horizon that is published but not stored would be forgotten by a
// restart that then accepts a write below it.
//
// The latest of the horizons moved is the one with the latest instant, the lowest
// layer's among equals, which is what the contract's Horizon returns after a
// Retain that moved several.
func (s *Store) publishHorizon(to [layers]time.Time, last uint64, moved [layers]bool, marker *retainMarker) error {
	b := s.kv.NewBatch()
	defer func() { _ = b.Close() }()
	hs := *s.horizons.Load() // a copy, with the moved layers replaced
	first := -1
	var latest store.Horizon
	for i := range hs {
		if !moved[i] {
			continue
		}
		raw, err := pebblekv.EncodeLayerHorizon(to[i], last)
		if err != nil {
			return err
		}
		if first < 0 {
			first, latest = i, store.Horizon{Time: to[i], Seq: last}
		} else if to[i].After(latest.Time) {
			latest = store.Horizon{Time: to[i], Seq: last}
		}
		hs[i] = store.Horizon{Time: to[i], Seq: last}
		if err := b.Set(metaKey(pebblekv.HorizonMetaName(catalog.L0+catalog.Layer(i))), raw, nil); err != nil {
			return err
		}
	}
	raw, err := pebblekv.EncodeLayerHorizon(latest.Time, latest.Seq)
	if err != nil {
		return err
	}
	if err := b.Set(metaKey(metaHorizonLast), raw, nil); err != nil {
		return err
	}
	if marker != nil {
		raw, err := appendRetainMarker(nil, *marker)
		if err != nil {
			return err
		}
		if err := b.Set(metaKey(metaRetain), raw, nil); err != nil {
			return err
		}
	}
	if err := s.commitHorizon(b); err != nil {
		// Whether the commit is stored is decided only when the store is reopened
		// (see [Store.uncertainCommit]); until then the store stops writing and
		// retaining. The horizon is published if the database shows it, because
		// refusing reads and writes before it is the safe side of not knowing. The
		// commit is whole or absent, so the first layer it moved says which.
		raw, rerr := s.kv.GetMeta(metaKey(pebblekv.HorizonMetaName(catalog.L0 + catalog.Layer(first))))
		if rerr != nil {
			s.failed = fmt.Errorf("a horizon commit failed (%w), and the horizon cannot be read back (%w): reopen the store", err, rerr)
			return s.failedError("Retain")
		}
		if t, seq, derr := pebblekv.DecodeLayerHorizon(raw); derr == nil && t.Equal(to[first]) && seq == last {
			s.horizons.Store(&hs)
			s.lastMoved.Store(&latest)
		}
		s.failed = fmt.Errorf("a horizon commit failed (%w), so whether it is stored is decided only when the store is reopened: reopen the store", err)
		return s.failedError("Retain")
	}
	s.horizons.Store(&hs)
	s.lastMoved.Store(&latest)
	return nil
}

// commitHorizon commits the batch of horizons, synced if the database is.
func (s *Store) commitHorizon(b *pebble.Batch) error {
	if s.beforeHorizonApply != nil {
		if err := s.beforeHorizonApply(); err != nil {
			return err // tests: a commit that failed without landing
		}
	}
	if err := s.kv.Apply(b, s.kv.WriteOptions()); err != nil {
		return err
	}
	if s.afterHorizonApply != nil {
		return s.afterHorizonApply() // tests: a commit that landed, reported as failed
	}
	return nil
}
