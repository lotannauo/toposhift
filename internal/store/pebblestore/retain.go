package pebblestore

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"maps"
	"math"
	"runtime"
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
// A store opened in [Background] mode does the first step under its lock, in
// milliseconds, and then returns: the rewriting is done by the store's own
// goroutine, one chunk at a time, taking the lock for each chunk only (see
// [Store.retainer]). A write that reaches a prefix the pass has not yet rewritten
// rewrites it first, in the same batch (see [Store.rewriteForWrite]), so the keys
// the pass leaves are the keys the synchronous mode leaves, whatever the
// interleaving. A Retain while a pass is pending starts the pass again at the new
// horizons, from the first key, and keeps rewriting the layers of the older pass
// that it does not move. Close stops the pass at a chunk boundary, and the next
// Open resumes it. Background mode does not settle.
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
	if marker != nil && s.retention == Background && s.pass != nil {
		allInside = s.carryOver(marker, allInside)
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
	if s.retention == Background {
		// The rest is the retainer's. The publication is the work this call did.
		workEnd = time.Now()
		return s.startPass(*marker, derive, wasComplete)
	}
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
	closed  bool

	// bg says the pass is the store's background pass, run by the retainer one
	// chunk at a time; the rest is its state, guarded by the store's lock like
	// everything else a write reads. marker.resume is the first key the pass has
	// not reached: a prefix before it is rewritten. touched holds the prefixes at
	// or after it that a write rewrote in its own batch, which the chunks skip.
	// intact says nothing has dropped what the writer remembers since the pass
	// began (see Store.lose): only then may the pass, when it ends, call the map
	// complete.
	bg      bool
	touched map[string]struct{}
	intact  bool

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

func (p *retainPass) close() {
	if !p.closed {
		p.closed = true
		_ = p.b.Close()
	}
}

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
	p.report()
	// Settling changes no data, and no iterator is open: it would pin the tables
	// the compactions replace.
	if err := s.settleRetention(); err != nil {
		return err
	}
	return s.releaseMarker(p.marker.generation)
}

// report counts what the pass did, once it has rewritten everything.
func (p *retainPass) report() {
	s := p.s
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
		if _, done := p.touched[string(prefix)]; done {
			// A write rewrote it, in its own batch. Visiting it would change nothing.
			p.seeks++
			ok = it.SeekGE(prefixSucc(prefix))
			lastPrefix = prefix
			continue
		}
		var res rewriteResult
		if res, err = p.rewritePrefix(it, prefix, p.b, p.derive); err != nil {
			return err
		}
		if res.lost {
			p.derive, p.next = false, nil
		}
		if res.state != nil {
			p.remember(prefix, res.state)
		}
		ok = res.ok
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
	if err := p.commit(next, begin); err != nil {
		return err
	}
	if p.bg {
		p.install()
	}
	return nil
}

// install puts what the chunk just committed worked out of the writer's state in
// the writer's map, for the prefixes no write has rewritten since the pass began
// (the writer holds those, and is newer). It is done in the chunk's own critical
// section, so no write falls between the commit and the install; and it is done
// chunk by chunk, never for the whole pass at its end, because the writes that
// came between the chunks have moved on the prefixes the pass has passed, and a
// copy at the end would put an older state over theirs.
func (p *retainPass) install() {
	if !p.derive {
		return
	}
	for k, st := range p.next {
		if _, done := p.touched[k]; !done {
			p.s.states[k] = st
		}
	}
	p.next = map[string]*prefixState{}
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
	// A background pass is stopped by the retainer, between two chunks: a chunk that
	// has begun is committed, so a Close waits for one chunk at most.
	if err := p.ctx.Err(); err != nil && !p.bg {
		return contextError("Retain", err)
	}
	if err := s.kv.Apply(p.b, pebble.NoSync); err != nil {
		return err
	}
	p.marker = next
	p.b.Reset()
	p.chunks++
	s.rec.Sample("retain.chunk_hold_ns", time.Since(begin).Nanoseconds())
	if s.afterRetainCommit != nil && !p.bg {
		// A background chunk runs the hook after it has released the lock, so that a
		// hook may write.
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

// rewriteResult is what rewritePrefix found. ok is false when no key is left
// after the prefix. state is what the writer would find in the prefix once the
// rewrite is committed, when derive was set and held to the end, and nil
// otherwise; lost says a key could not be read, so that nothing is to be learnt
// from this prefix or from the rest of the pass.
type rewriteResult struct {
	ok    bool
	state *prefixState
	lost  bool
}

// rewritePrefix rewrites the prefix the iterator is at the first key of, into the
// batch b: a baseline at the horizon holds what is alive at it, and everything
// older goes. It leaves the iterator at the first key after the prefix, or, when
// nothing in the prefix is older than the horizon, wherever the seek past that
// instant landed, which is also after it; ok is false when no key is left. With
// derive set it also works out the state the writer would find in the prefix
// afterwards. It writes to b and nothing else of the store: the chunks pass the
// pass's own batch, and a write that reaches a prefix first passes its own.
func (p *retainPass) rewritePrefix(it *pebble.Iterator, prefix []byte, b *pebble.Batch, derive bool) (res rewriteResult, err error) {
	s := p.s
	lp := &p.lay[int(layerOfPrefix(prefix))-int(catalog.L0)]
	hNs, horizon := lp.hNs, lp.horizon
	dir := prefix[prefixLen-1]
	p.visited++
	// The keys that stay are read first, from the first key of the prefix, where
	// the iterator is.
	var stays, dropped prefixState
	if derive {
		var parsed bool
		var n int64
		stays, dropped, n, parsed, err = foldRetained(it, prefix, hNs)
		if err != nil {
			return res, err
		}
		p.stateKeys += n
		if !parsed {
			derive, res.lost = false, true // a key that cannot be read: learn nothing here
		}
	}
	// Go to the newest record of this prefix strictly before the horizon. The
	// seek may land in a later prefix, which the loop then takes up.
	p.seeks++
	ok := it.SeekGE(seekKey(prefix, lp.oldMax))
	res.ok = ok
	if !ok || !hasPrefix(it.Key(), prefix) {
		if derive {
			res.state = &stays // nothing is rewritten here: a checkpoint at the horizon stays
		}
		return res, nil
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
			return res, err
		}
		switch kind {
		case kindRecord:
			p.records++
			prefixRecords++
			ref, v, err := decodeRecordValue(dir, it.Value())
			if err != nil {
				return res, err
			}
			if v.Seq != seq {
				return res, fmt.Errorf("pebblestore: a record's key has seq %d and its value %d", seq, v.Seq)
			}
			bootSeen = bootSeen || v.Boot != ""
			consider(ref, ns, v)
		case kindBaseline:
			st, err := decodeStamp(it.Value())
			if err != nil {
				return res, err
			}
			if st.kind != kindBaseline {
				return res, fmt.Errorf("pebblestore: a baseline key holds a stamp of kind %d", st.kind)
			}
			for _, en := range st.entries {
				consider(en.ref, en.eventNs, en.value)
			}
		}
	}
	if err := it.Error(); err != nil {
		return res, err
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
		if derive {
			read, n, unreadable, rerr := s.readPrefixState(s.kv, prefix)
			if rerr != nil {
				return res, rerr
			}
			p.stateKeys += n
			if unreadable != nil {
				res.lost = true // a key that cannot be read: learn nothing here
			} else {
				res.state = &read
			}
		}
	} else {
		if derive {
			res.state = &dropped
		}
		// Everything older than the horizon goes, the previous baseline
		// included; what is still alive at the horizon is one new baseline, in
		// the same commit and after the delete.
		if err := b.DeleteRange(seekKey(prefix, lp.oldMax), prefixSucc(prefix), nil); err != nil {
			return res, err
		}
		p.deletes++
		if s.anyCkpt && lp.where == pebblekv.Inside {
			// A checkpoint exactly at the horizon would sort before the
			// baseline. The writer never writes one at or below the horizon,
			// and the ones in the prefixes this retention rewrites go.
			if err := b.Delete(stampKey(prefix, hNs, kindCheckpoint), nil); err != nil {
				return res, err
			}
		}
		if len(entries) > 0 {
			val, err := appendStamp(nil, stamp{kind: kindBaseline, foldVersion: foldVersion, through: lp.last, w: lp.last, horizon: horizon, entries: entries})
			if err != nil {
				return res, err
			}
			if err := b.Set(stampKey(prefix, lp.baseNs, kindBaseline), val, nil); err != nil {
				return res, err
			}
			p.baselines++
		}
	}
	p.seeks++
	res.ok = it.SeekGE(prefixSucc(prefix))
	return res, nil
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

// ---------------------------------------------------------------------------
// The background pass.
//
// In Background mode Retain publishes the horizons and the marker, makes a
// retainPass of the marker and leaves it in Store.pass; the retainer goroutine
// runs it, one chunk at a time, each under the store's lock; and a Write that
// reaches a prefix before the pass does rewrites it itself. Everything below is
// called with the lock held unless it says otherwise.

// carryOver adds to the marker of a Retain the layers of the pass still pending
// that this Retain does not move, each with the horizon and last sequence number it
// was given: the new pass starts again from the first key, and the layers it does
// not move are still owed their rewriting. (A layer that a Retain does not move is
// one that is kept or has a longer offset; those are fixed for the life of a store,
// but a store opened again with other options may resume a pass that holds a layer
// the options of this opening would never move.) It returns whether every layer of
// the marker is inside the range.
func (s *Store) carryOver(m *retainMarker, allInside bool) bool {
	for _, old := range s.pass.marker.layers {
		if slices.ContainsFunc(m.layers, func(l retainLayer) bool { return l.layer == old.layer }) {
			continue
		}
		m.layers = append(m.layers, old)
		_, where := pebblekv.Locate(old.horizon)
		allInside = allInside && where == pebblekv.Inside
	}
	slices.SortFunc(m.layers, func(a, b retainLayer) int { return cmp.Compare(a.layer, b.layer) })
	return allInside
}

// newBackgroundPass makes the pass the marker stands for, as the store's
// background pass: nothing is touched yet, and nothing has dropped what the writer
// remembers.
func (s *Store) newBackgroundPass(m retainMarker, derive bool, stopAfter int) (*retainPass, error) {
	p, err := s.newPass(s.ctx, m, derive, stopAfter)
	if err != nil {
		return nil, err
	}
	p.bg, p.touched, p.intact = true, map[string]struct{}{}, true
	return p, nil
}

// startPass makes the pass of a Retain the store's pending pass, in place of an
// older one, and wakes the retainer. The pass starts from the marker's first key,
// with no prefix touched: the prefixes the older pass or the writes rewrote are
// rewritten again at the new horizons.
func (s *Store) startPass(m retainMarker, derive, wasComplete bool) error {
	p, err := s.newBackgroundPass(m, derive, s.stopAfter)
	if err != nil {
		// The marker of the new generation is stored, and the older pass must not go on
		// to write its own marker over it.
		s.dropPass()
		s.failed = fmt.Errorf("the pass of a retention could not be made (%w), so it is finished only when the store is reopened: reopen the store", err)
		s.bgErr = s.failed
		s.releaseWaiters()
		return s.failedError("Retain")
	}
	p.wasComplete = wasComplete
	s.dropPass()
	s.pass = p
	s.expectWaiters()
	s.signal()
	return nil
}

// resumeInBackground makes the pass a marker found at Open stands for the store's
// pending pass, before the store is handed out, so that a write that arrives
// before the first chunk already sees which prefixes are owed. Nothing is worked
// out of the writer's state: the first write to a prefix reads it.
func (s *Store) resumeInBackground(m retainMarker) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.last = retainPhases{}
	s.forgetAll()
	s.rec.Count("retain.resumed", 1)
	p, err := s.newBackgroundPass(m, false, s.resumeStopAfter)
	if err != nil {
		return err
	}
	s.pass = p
	s.expectWaiters()
	return nil
}

// dropPass forgets the pending pass, if there is one, without ending it: its marker
// stays in the database.
func (s *Store) dropPass() {
	if s.pass != nil {
		s.pass.close()
		s.pass = nil
	}
}

// expectWaiters makes ready the channel the callers of WaitRetained wait on.
func (s *Store) expectWaiters() {
	if s.passDone == nil {
		s.passDone = make(chan struct{})
	}
}

// releaseWaiters wakes the callers of WaitRetained: no pass is pending, or none
// will run.
func (s *Store) releaseWaiters() {
	if s.passDone != nil {
		close(s.passDone)
		s.passDone = nil
	}
}

// signal wakes the retainer; a signal sent while it is awake is kept for its next
// wait. It takes no lock.
func (s *Store) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// startRetainer starts the goroutine that runs the passes of a Background store.
// Open does it, once, after the store is made: a pass that Open resumed is waiting
// for it.
func (s *Store) startRetainer() {
	s.retainerDone = make(chan struct{})
	go s.retainer()
	s.signal()
}

// A step of the retainer.
type retainerStep uint8

const (
	stepMore retainerStep = iota // a pass is pending: take its next chunk
	stepIdle                     // nothing is pending: wait to be woken
	stepExit                     // the store is closing, or has failed
)

// retainer is the goroutine of a Background store. It waits to be woken and then
// takes the pending pass one chunk at a time, each under the store's lock and
// none for longer than the chunk, yielding between them so that a writer that
// waits for the lock gets it. It exits when the store's context is cancelled (at
// the next chunk boundary), or when a commit has failed and the store has latched
// the failure.
func (s *Store) retainer() {
	defer func() {
		s.mu.Lock()
		s.releaseWaiters()
		s.mu.Unlock()
		if s.retainerExited != nil {
			close(s.retainerExited)
		}
		close(s.retainerDone)
	}()
	var cur *retainPass
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.wake:
		}
		for {
			if s.beforeRetainChunk != nil && s.pending() {
				s.beforeRetainChunk() // the lock is free: a hook may write
			}
			step, landed := s.retainStep(&cur)
			if landed && s.afterRetainCommit != nil {
				s.afterRetainCommit() // the lock is free: a hook may write
			}
			if step == stepExit {
				return
			}
			if step == stepIdle {
				break
			}
			runtime.Gosched()
		}
	}
}

// pending says a pass is waiting to be run.
func (s *Store) pending() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pass != nil && s.failed == nil
}

// retainStep takes one chunk of the pending pass. cur is the pass the retainer was
// taking: if another generation has taken its place, it is dropped and the newer is
// started from the first key, with nothing of the older installed. landed says a
// chunk was committed (so the hook runs).
func (s *Store) retainStep(cur **retainPass) (step retainerStep, landed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctx.Err() != nil || s.failed != nil {
		return stepExit, false
	}
	if p := *cur; p != nil && (s.pass == nil || s.pass.marker.generation != p.marker.generation) {
		*cur = nil // another Retain published: this pass is not the store's any more
	}
	if *cur == nil {
		*cur = s.pass
	}
	p := *cur
	if p == nil {
		return stepIdle, false
	}
	// A pass resumed from a marker in its settling phase has nothing left to rewrite
	// (its resume key is empty, and a chunk from there would read the meta keys): in
	// Background mode, which does not settle, it only releases its marker.
	var err error
	if p.marker.phase == phaseRewrite {
		before := p.chunks
		err = p.chunk()
		landed = p.chunks > before
	}
	if err == nil && p.marker.phase == phaseSettle {
		err = s.endPass(p)
		*cur = nil
		if err == nil {
			return stepIdle, landed
		}
	}
	if err != nil {
		s.latch(err)
		return stepExit, landed
	}
	return stepMore, landed
}

// endPass ends a pass whose last chunk has been committed and installed, in the
// order that lets WaitRetained say the pass is over: the marker goes, the map is
// called complete if the pass vouches for it, the counts are made, the pass is
// cleared, and the waiters are woken. Background mode does not settle.
func (s *Store) endPass(p *retainPass) error {
	if err := s.releaseMarker(p.marker.generation); err != nil {
		return err
	}
	if p.derive && p.intact {
		// As the synchronous pass ends, but only if nothing has dropped what the
		// writer remembers since it began: a prefix forgotten after the pass passed it
		// would be missing from the map, which would then claim it holds nothing.
		s.complete = p.wasComplete || !slices.Contains(p.moved[:], false)
	}
	p.report()
	s.pass = nil
	p.close()
	s.releaseWaiters()
	return nil
}

// latch stops the background pass for good after an error in a chunk or in its
// end: Write, Retain, WaitRetained and Close report it, reads continue, and
// reopening the store finishes the pass from its marker. What the writer
// remembers is dropped, as for any commit that failed.
func (s *Store) latch(err error) {
	if s.failed == nil {
		_ = s.uncertainCommit("Retain", err)
	}
	s.bgErr = s.failed
	s.forgetAll()
	s.dropPass()
	s.releaseWaiters()
}

// owes says the pass has yet to rewrite the prefix, so that a write to it must:
// the prefix is in a layer the pass rewrites, is at or after the first key the
// pass has not reached, and no write has rewritten it in this generation.
func (p *retainPass) owes(prefix []byte) bool {
	if p.marker.phase != phaseRewrite {
		return false // every prefix is rewritten already
	}
	i := int(layerOfPrefix(prefix)) - int(catalog.L0)
	if i < 0 || i >= layers || !p.moved[i] || bytes.Compare(prefix, p.marker.resume) < 0 {
		return false
	}
	_, done := p.touched[string(prefix)]
	return !done
}

// rewriteForWrite runs the pass's step for a prefix a write is about to put
// records in, into the write's own batch b, and sets what the writer remembers of
// the prefix to what a read of it finds with the batch applied. The batch is
// indexed, so that the read sees the range delete, the checkpoint delete and the
// baseline of the step over what the database holds. The caller marks the prefix
// as touched only when the batch has been committed: a write that fails leaves
// the prefix to the pass, or to a later write.
//
// The step is the pass's own (rewritePrefix), so the keys it leaves are the keys
// the pass would have left, and the later visit of the pass is a seek that finds
// nothing below the horizon. Nothing a write can put is below the horizon of its
// layer, and the step reaches no further than the instant before it.
func (s *Store) rewriteForWrite(b *pebble.Batch, prefix []byte, track bool) error {
	if err := s.touchPrefix(b, prefix); err != nil {
		return err
	}
	if !track {
		return nil // nothing is remembered when checkpoints are off and none exists
	}
	if !s.anyCkpt {
		// What the state lookup finds in a database that has no checkpoint.
		s.states[string(prefix)] = &prefixState{latest: -1}
		return nil
	}
	s.iterators++
	res, keys, unreadable, err := s.readPrefixState(b, prefix)
	if err != nil {
		return err
	}
	if unreadable != nil {
		return unreadable
	}
	s.states[string(prefix)] = &res
	s.rec.Count("checkpoint.loads", 1)
	s.rec.Count("checkpoint.load_keys", keys)
	return nil
}

// touchPrefix is the pass's step on one prefix, into b. The iterator is of the
// database and is closed before it returns, so that the batch can be read and
// written after.
func (s *Store) touchPrefix(b *pebble.Batch, prefix []byte) error {
	lo, hi := prefixBounds(prefix)
	it, err := s.kv.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return err
	}
	defer func() { _ = it.Close() }()
	if !it.First() {
		return it.Error() // the prefix holds nothing: there is nothing to rewrite
	}
	_, err = s.pass.rewritePrefix(it, prefix, b, false)
	return err
}
