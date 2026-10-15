package pebblestore

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"math"
	"runtime"
	"slices"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
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
// two prefixes, once its commit holds about 4 MiB or it has been running for 20 ms
// (the opening of its iterator counted, but for no more than 10 ms), and is
// committed, not synced,
// together with the marker's new resume key, the first key of the next prefix: so
// after any chunk every prefix before the resume key is rewritten and none after it
// has been touched, and a crash that loses the chunk loses its resume key with it.
// Each chunk reads through an iterator of its own, opened when it begins, so that
// it sees what the chunks before it committed.
// Opening it costs in proportion to the range deletions the memtables hold, one per
// prefix rewritten, so a pass flushes once those reach a count, and waits for the
// flush only if they reach twice the count before it has completed (see
// flushBound). A synchronous Retain whose context is cancelled during that wait
// returns the context's error, as it does at a commit; the retention Open finishes
// has no context, and if a flush keeps failing (a persistent write error, which
// Pebble retries for ever) it waits as long, as every write would at the next full
// memtable.
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
// Open resumes it.
//
// Last, the marker passes to its settling phase in the commit of the last chunk;
// with Config.SettleRetention the database is flushed and waited on until it is at
// rest, or until the deadline passes (see [pebblekv.KV.SettleAfterRetention]); and
// then the marker is deleted, if it is still this retention's: it carries a
// generation, and a retention that finds another's marker leaves it. A Background
// store settles differently, so that writers go on: after the last chunk the
// goroutine releases the lock, flushes, and compacts the key ranges of each layer
// the pass moved, one range after another, until they are done or the deadline
// passes (see [pebblekv.KV.CompactRanges] and settleRanges); only then does it take
// the lock again, delete the marker and wake the callers of
// [Instrument.WaitRetained]. A Retain while it settles starts its own pass at once;
// the settle that is running finishes or reaches its deadline, and is not started
// again. Close during the settle stops it between two ranges and leaves the marker
// in its settling phase, so that the next Open settles again.
//
// When the store's policy names a boot key, the baseline of an entity prefix with
// boots also holds the boot fold's state at the horizon (see the package comment),
// so that a later collision is judged against the boots it discards. A Retain
// that does not move any layer's horizon changes nothing and does not raise
// LastSeq. A Retain moves the horizon of each layer that is not kept to the
// horizon less the layer's offset, and only where that is later than the layer's
// own: the others keep theirs, and their data is left alone. It holds the store's
// lock for as long as it runs, settling included, so Close must not be called
// meanwhile; the lock is not released between chunks.
//
// What the writer remembers of each prefix (see checkpoint.go) is out of date once
// a retention begins to rewrite, so it is not used from then on. Where the
// checkpoint policy is on and the database may hold a checkpoint, the retention
// works out what a read of each prefix would find from the keys it leaves, and puts
// it in the states the writer keeps of the prefixes it rewrites, so that the next
// write to one needs no read; it does not add states the writer did not keep. The
// keys it reads for that are counted as "retain.state_keys". A retention that
// stops or fails leaves those states unknown, and the prefixes are read as they are
// touched; so does one that Open finishes. When the rewriting is over, the stored
// prefixes are scanned again for the writer's filter (see prefixstate.go); a scan
// that is running when a retention publishes stops.
func (s *Store) Retain(ctx context.Context, horizon time.Time) error {
	if s.closed.Load() {
		return closedError("Retain")
	}
	if err := ctx.Err(); err != nil {
		return contextError("Retain", err)
	}
	// What the observer is told of this retention is told after the lock is
	// released (deferred first, so it runs last).
	var announce func()
	defer func() {
		if announce != nil {
			announce()
		}
	}()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.flushCounts()
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
	// No scan runs during a pass: one that is running stops and installs nothing,
	// and the pass's own follows its rewriting. The writes from here on go to a
	// delta of their own.
	s.cancelScan()
	s.newDelta()
	var publishedAt time.Time
	if s.retention == Background {
		publishedAt = s.stamp()
	}
	// What was remembered of each prefix of a layer about to be rewritten is out of
	// date. It is worked out again below, for the prefixes the writer keeps, and
	// stays unknown if the retention does not finish. What is remembered of the
	// other layers is as true as it was.
	s.forgetLayers(marker.layers)

	// What the writer would find if it read each prefix after this retention, worked
	// out as the retention goes by, so that the first write to a prefix need not
	// read it. Not working it out is always correct (the prefix is then read when it
	// is next touched), so it is done only where it pays: with the checkpoint policy
	// on, where a read would look (a checkpoint may be in the database), where a
	// read is the kind that stops at the tail, and where every horizon the pass
	// rewrites to is inside the range (past it the retention rewrites every prefix
	// in a way the keys it leaves do not describe). The prefixes of the layers the
	// pass leaves alone keep what is remembered of them. The cost is a read of the
	// keys at or after the horizon of each prefix, up to its newest checkpoint,
	// counted as "retain.state_keys".
	derive := s.ckpt.On && s.anyCkpt && !s.fullStateRead && allInside
	if s.retention == Background {
		// The rest is the retainer's. The publication is the work this call did.
		workEnd = time.Now()
		// A pass that is still rewriting is replaced: its rewriting ends here, as far as
		// the observer is concerned, and it will not be settled.
		var replaced uint64
		if s.pass != nil && s.pass.marker.phase == phaseRewrite {
			replaced = s.pass.marker.generation
		}
		if err := s.startPass(*marker, derive); err != nil {
			return err
		}
		if o := s.observer; o != nil {
			gen := marker.generation
			announce = func() {
				if replaced != 0 {
					o.RewriteEnded(replaced, publishedAt)
				}
				o.Published(gen, publishedAt)
			}
		}
		return nil
	}
	p, err := s.newPass(ctx, *marker, derive, s.stopAfter)
	if err != nil {
		return err
	}
	defer p.close()
	err = p.run()
	workEnd = p.workEnd
	if err == nil {
		s.startScan() // the scan of the stored prefixes that follows the pass
	}
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
	// is what has been worked out, for the prefixes whose state the writer keeps;
	// derived says it was meant to be, for the count of the keys read.
	derive, derived bool
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
	// scanned says the scan that follows its rewriting has been started.
	bg      bool
	touched map[string]struct{}
	scanned bool

	visited, replayed, records, baselines, deletes, seeks, maxPrefixRecords int64
	// counted is how many of deletes the store's count of unflushed range deletions
	// already has (see flushBound).
	counted int64
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
	p.syncDeletes()
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
	// Only now, with every commit landed, is what was worked out true of the
	// database. The states of the prefixes rewritten that it did not work out are
	// dropped, and read when they are next touched.
	p.install()
	s.states.dropStale()
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
// marker's next state. A chunk of the background pass also ends when a write is
// waiting for the lock, once it has done a minimum of work (see
// [retainPass.yields]). A chunk ends only between prefixes. It reads through an
// iterator of its own, opened here.
//
// The chunk's time counts the opening and first positioning of its iterator, but
// no more than half of it: opening an iterator costs time in proportion to the
// range deletions the memtables hold, and if all of that counted against the chunk,
// a pass that had written enough of them would rewrite one prefix per chunk. So a
// chunk works for at least half the chunk time after the opening, and one whose
// opening is cheap holds the lock for about the chunk time plus its commit. The
// time the lock is held, which "retain.chunk_hold_ns" samples, counts the opening
// in full. A synchronous
// pass first bounds those deletions here (see [retainPass.boundFlushes]); in the
// background the retainer does it between chunks, and starts or waits for a flush
// without the lock (see [Store.flushStep]).
func (p *retainPass) chunk() error {
	s := p.s
	if !p.bg {
		if err := p.boundFlushes(); err != nil {
			return err
		}
	}
	s.rec.Sample("retain.unflushed_range_deletes", s.fl.unflushed())
	begin := s.chunkNow()
	_, hi := dataBounds()
	it, err := s.kv.NewIter(&pebble.IterOptions{LowerBound: p.marker.resume, UpperBound: hi})
	if err != nil {
		return err
	}
	closeIter := sync.OnceFunc(func() { _ = it.Close() })
	defer closeIter()
	ok := it.First()
	if s.afterChunkOpen != nil {
		s.afterChunkOpen()
	}
	positioned := s.chunkNow()
	open := positioned.Sub(begin)
	// The chunk's time runs from workStart: the opening is charged against it, up to
	// half the chunk time.
	workStart := positioned.Add(-min(open, s.chunkTime/2))
	var lastPrefix []byte
	var prefixes int64
	exhausted, yielded := true, false
	for ok {
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
		prefixes++
		if res.lost {
			p.derive, p.next = false, nil
		}
		if res.state != nil {
			p.remember(prefix, res.state)
		}
		ok = res.ok
		lastPrefix = prefix
		if s.afterChunkPrefix != nil {
			s.afterChunkPrefix(prefixes)
		}
		if ok && (p.b.Len() >= s.retainBytes || s.chunkNow().Sub(workStart) >= s.chunkTime) {
			exhausted = false
			break
		}
		if ok && p.yields(prefixes, positioned, open) {
			exhausted, yielded = false, true
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
	if err := p.commit(next, begin, open, prefixes); err != nil {
		return err
	}
	if yielded {
		s.rec.Count("retain.chunks_yielded", 1)
	}
	if p.bg {
		p.install()
	}
	return nil
}

// yields says a background chunk ends here, between two prefixes, for a write that
// is waiting for the lock. It does so only once the chunk has rewritten
// s.chunkMinPrefixes prefixes and has worked, since its iterator was positioned,
// at least as long as its opening was charged against its time (open, up to half the
// chunk time), so that however often writers wait, a pass keeps its progress per
// chunk and most of a shortened chunk's hold is work and not the cost of opening
// its iterator. The synchronous pass holds the lock across all its chunks, so ending
// one early would only add commits: it never yields. The bytes and the time still
// end a chunk, so a pass ends whatever the writers do.
func (p *retainPass) yields(prefixes int64, positioned time.Time, open time.Duration) bool {
	s := p.s
	return p.bg && s.writersWaiting.Load() > 0 &&
		prefixes >= s.chunkMinPrefixes &&
		s.chunkNow().Sub(positioned) >= min(open, s.chunkTime/2)
}

// install puts what has been worked out of the writer's state, since the last
// install, in the states the writer keeps (stale, since the pass began, or fresh),
// for the prefixes no write has rewritten since the pass began (the writer holds
// those, and is newer). It adds no state the writer does not keep. A background
// pass does it in each chunk's own critical section, so no write falls between the
// commit and the install, and chunk by chunk, never for the whole pass at its end,
// because the writes that came between the chunks have moved on the prefixes the
// pass has passed, and a copy at the end would put an older state over theirs; the
// synchronous pass does it once, after its last commit.
func (p *retainPass) install() {
	if !p.derive {
		return
	}
	for k, st := range p.next {
		if _, done := p.touched[k]; !done {
			p.s.states.replace(k, st)
		}
	}
	p.next = map[string]*prefixState{}
}

// commit commits the batch with the marker next in it, not synced: the horizon is
// already published and durable, and a later synced commit keeps this one ordered
// behind, so a crash that loses it loses the marker's move with it. begin is when
// the chunk began, before its iterator was opened, open how long the opening and
// the first positioning took, and prefixes how many prefixes it rewrote: they are
// sampled once the commit has landed, in that order and with the time held last.
func (p *retainPass) commit(next retainMarker, begin time.Time, open time.Duration, prefixes int64) error {
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
	if s.beforeApply != nil {
		s.beforeApply(p.b)
	}
	if err := s.kv.Apply(p.b, pebble.NoSync); err != nil {
		return err
	}
	p.syncDeletes()
	s.fl.heldOff = false // a flush put off or not started is tried again
	p.marker = next
	p.b.Reset()
	p.chunks++
	s.rec.Sample("retain.chunk_open_ns", open.Nanoseconds())
	s.rec.Sample("retain.chunk_prefixes", prefixes)
	s.rec.Sample("retain.chunk_hold_ns", s.chunkNow().Sub(begin).Nanoseconds())
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
// working that out and the writer keeps a state of the prefix (stale or not):
// what the pass works out of the others is left, so that the states it installs
// stay within the cache's budget.
func (p *retainPass) remember(prefix []byte, st *prefixState) {
	if p.derive && p.s.states.has(string(prefix)) {
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
	consider := func(ref []byte, ns int64, v pebblekv.Value) {
		if _, done := decided[string(ref)]; done {
			return
		}
		decided[string(ref)] = struct{}{}
		if v.Holds(ns, hNs) {
			entries = append(entries, entryOf(ref, ns, v))
		}
	}
	// Under a boot key, an entity's records that go and the previous baseline's boot
	// state are what the new baseline's boot state is folded from (see bootsAt).
	boots := p.boots && dir == dirEntity
	var discarded []lifecycle.Assertion
	var prev *bootState
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
			if boots {
				discarded = append(discarded, bootAssertionOf(dir, ref, ns, v, s.policy.BootKey))
			}
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
			prev = st.boots
		}
	}
	if err := it.Error(); err != nil {
		return res, err
	}
	p.maxPrefixRecords = max(p.maxPrefixRecords, prefixRecords)
	var bs *bootState
	if boots {
		if bs, err = p.bootsAt(prev, discarded, horizon); err != nil {
			return res, fmt.Errorf("pebblestore: the boots of prefix %x at the horizon: %w", prefix, err)
		}
	}
	if derive {
		res.state = &dropped
	}
	// Everything older than the horizon goes, the previous baseline included; what
	// is still alive at the horizon, and the boot fold's state at it if the prefix
	// has boots, is one new baseline, in the same commit and after the delete.
	if err := b.DeleteRange(seekKey(prefix, lp.oldMax), prefixSucc(prefix), nil); err != nil {
		return res, err
	}
	p.deletes++
	if s.anyCkpt && lp.where == pebblekv.Inside {
		// A checkpoint exactly at the horizon would sort before the baseline. The
		// writer never writes one at or below the horizon, and the ones in the
		// prefixes this retention rewrites go.
		if err := b.Delete(stampKey(prefix, hNs, kindCheckpoint), nil); err != nil {
			return res, err
		}
	}
	if len(entries) > 0 || bs != nil {
		val, err := appendStamp(nil, stamp{kind: kindBaseline, foldVersion: foldVersion, through: lp.last, w: lp.last, horizon: horizon, entries: entries, boots: bs})
		if err != nil {
			return res, err
		}
		if err := b.Set(stampKey(prefix, lp.baseNs, kindBaseline), val, nil); err != nil {
			return res, err
		}
		p.baselines++
	}
	p.seeks++
	res.ok = it.SeekGE(prefixSucc(prefix))
	return res, nil
}

// bootsAt is the boot fold's state at horizon of an entity prefix whose records
// before it go: prev, the previous baseline's (nil if it has none), and then the
// sightings of the records that go and those prev carries, in order. The sightings
// of the records are the fold's own (lifecycle.Sightings: per producer, the record
// with the highest Seq at each event time, and its last instant if it is a run),
// so every record that goes is passed, whether or not it is the newest of its
// reference. Those before the horizon are folded, and those at or after it are
// carried. It is nil, and the baseline is of format 1, if there is no boot.
func (p *retainPass) bootsAt(prev *bootState, discarded []lifecycle.Assertion, horizon time.Time) (*bootState, error) {
	sightings, err := lifecycle.Sightings(discarded, p.s.policy)
	if err != nil {
		return nil, err
	}
	tr, carried, err := foldBoots(p.s.policy, prev, sightings, &horizon)
	if err != nil {
		return nil, err
	}
	bs := &bootState{boots: tr.Boots(), collision: tr.Collision(), carried: carried}
	if len(bs.boots) == 0 {
		if len(carried) > 0 {
			return nil, fmt.Errorf("%d sightings carried past the horizon and no boot before it", len(carried))
		}
		return nil, nil
	}
	return bs, nil
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
	if s.pass.marker.phase == phaseSettle {
		// Its rewriting is over (it is settling, off the lock): nothing is owed.
		return allInside
	}
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
	p.bg, p.touched = true, map[string]struct{}{}
	return p, nil
}

// startPass makes the pass of a Retain the store's pending pass, in place of an
// older one, and wakes the retainer. The pass starts from the marker's first key,
// with no prefix touched: the prefixes the older pass or the writes rewrote are
// rewritten again at the new horizons.
func (s *Store) startPass(m retainMarker, derive bool) error {
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
// waits for the lock gets it; when the rewriting is over it settles the pass
// without the lock (see [Store.settleBackground]). It exits when the store's
// context is cancelled (at the next chunk boundary, between two ranges of a
// settle, or in a wait for a flush), or when a commit has failed and the store has
// latched the failure.
//
// Between two chunks it also keeps the range deletions the memtable holds under a
// bound, without the lock (see [Store.flushStep]). A chunk holds the lock for the
// opening of its iterator, whose cost grows with those deletions and which the
// bound keeps from growing without end, plus the work, until the chunk time (20 ms) has passed with the opening
// counted up to half of it, plus its commit: about the chunk time plus the commit
// when the opening is cheap, and at most the opening plus half the chunk time plus
// the commit when it is not. A chunk may be shorter: it ends at the next prefix once
// a write is waiting for the lock and the chunk has done a minimum of work (see
// [retainPass.yields]), and before the retainer takes the lock again it yields to the
// waiting write (see [Store.yieldToWriters]).
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
		flushed := false // the last step was a flush's, and no chunk has been taken since
		for {
			if s.beforeRetainChunk != nil && !flushed && s.pending() {
				s.beforeRetainChunk() // the lock is free: a hook may write
			}
			s.yieldToWriters()
			step, landed, w := s.retainStep(&cur)
			if flushed = w.flush != nil; flushed {
				// Before the chunk, and without the lock: a flush to start, or one to wait for.
				if s.flushStep(*w.flush) == stepExit {
					return
				}
				continue
			}
			if w.rewriteEnded && s.observer != nil {
				s.observer.RewriteEnded(w.gen, w.rewriteAt)
			}
			if landed && s.afterRetainCommit != nil {
				s.afterRetainCommit() // the lock is free: a hook may write
			}
			if step == stepExit {
				return
			}
			if step == stepIdle {
				break
			}
			if w.settle != nil && s.settleBackground(w.settle) == stepExit {
				return
			}
			runtime.Gosched()
		}
	}
}

// yieldToWriters yields the processor, at most retainYieldSpins times and while a
// write is waiting for the lock, so that the write takes it before the retainer
// takes it again for its next chunk: a goroutine that is running can take a mutex
// that has just been released before the one it woke gets to it. It changes no
// byte, only who takes the lock next, and the bound keeps a stream of writers from
// stalling the retainer. It returns how many times it yielded.
func (s *Store) yieldToWriters() int {
	n := 0
	for n < retainYieldSpins && s.writersWaiting.Load() > 0 {
		runtime.Gosched()
		n++
	}
	return n
}

// pending says a pass is waiting to be run.
func (s *Store) pending() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pass != nil && s.failed == nil
}

// retainWork is what a step of the retainer leaves for it to do once the lock is
// released: tell the observer the rewriting is over, and settle the pass; or, in a
// step that took no chunk, start or wait for a flush before the chunk (see
// [Store.flushStep]).
type retainWork struct {
	gen          uint64
	rewriteEnded bool
	rewriteAt    time.Time
	settle       *settleJob
	flush        *flushAction
}

// settleJob is a pass whose rewriting is over and the key ranges it settles.
type settleJob struct {
	p      *retainPass
	ranges [][2][]byte
}

// settleResult is how long a settle took, if one ran.
type settleResult struct {
	flush, settle time.Duration
	hit, ran      bool
}

// retainStep takes one chunk of the pending pass. cur is the pass the retainer was
// taking: if another generation has taken its place, it is dropped and the newer is
// started from the first key, with nothing of the older installed. landed says a
// chunk was committed (so the hook runs). When the rewriting is over and the pass is
// to be settled off the lock, the work says so and cur is cleared: the pass ends in
// [Store.settleBackground]. A pass with nothing to settle, and no observer to tell,
// ends here.
func (s *Store) retainStep(cur **retainPass) (step retainerStep, landed bool, w retainWork) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctx.Err() != nil || s.failed != nil {
		return stepExit, false, w
	}
	if p := *cur; p != nil && (s.pass == nil || s.pass.marker.generation != p.marker.generation) {
		*cur = nil // another Retain published: this pass is not the store's any more
	}
	if *cur == nil {
		*cur = s.pass
	}
	p := *cur
	if p == nil {
		return stepIdle, false, w
	}
	w.gen = p.marker.generation
	// A pass resumed from a marker in its settling phase has nothing left to rewrite
	// (its resume key is empty, and a chunk from there would read the meta keys): it
	// only settles, and releases its marker.
	var err error
	if p.marker.phase == phaseRewrite {
		// The unflushed range deletions are bounded before the chunk, by the retainer
		// without the lock: a flush is started, or waited for, off it.
		if a := s.flushNeeds(p); !a.none() {
			w.flush = &a
			return stepMore, false, w
		}
		before := p.chunks
		err = p.chunk()
		landed = p.chunks > before
		if err == nil && p.marker.phase == phaseSettle {
			w.rewriteEnded, w.rewriteAt = true, s.stamp()
		}
	}
	if err == nil && p.marker.phase == phaseSettle && !p.scanned {
		s.rewritten(p)
	}
	if err == nil && p.marker.phase == phaseSettle {
		if s.kv.Config().SettleRetention || s.observer != nil {
			w.settle = &settleJob{p: p, ranges: s.settleRangesOf(p)}
			*cur = nil
			return stepMore, landed, w
		}
		err = s.endPass(p, settleResult{})
		*cur = nil
		if err == nil {
			return stepIdle, landed, w
		}
	}
	if err != nil {
		s.latch(err)
		return stepExit, landed, w
	}
	return stepMore, landed, w
}

// settleRangesOf are the key ranges to settle for a pass: those of each layer it
// moved.
func (s *Store) settleRangesOf(p *retainPass) [][2][]byte {
	ids := entityIDs(s.ids)
	var out [][2][]byte
	for i, moved := range p.moved {
		if moved {
			out = append(out, settleRanges(catalog.L0+catalog.Layer(i), ids)...)
		}
	}
	return out
}

// rewritten is the end of a background pass's rewriting, or the start of a pass
// that Open resumed in its settling phase: the states of the prefixes it rewrote
// that it did not work out are dropped, as the synchronous pass drops them after
// its last commit, and the scan of the stored prefixes that follows the pass
// starts, with the lock free once the caller releases it, beside the settle. The
// caller holds the lock.
func (s *Store) rewritten(p *retainPass) {
	p.scanned = true
	s.states.dropStale()
	s.startScan()
}

// settleBackground settles a pass whose rewriting is over, and ends it. It runs on
// the retainer's goroutine without the store's lock and without an iterator open,
// so that writes go on: it compacts the ranges of the layers the pass moved (see
// [pebblekv.KV.CompactRanges]) and then takes the lock to end the pass.
//
// The marker stays in its settling phase until then, so that a Close or a crash
// during the settle leaves it for the next Open, which settles again. A Close stops
// the settle between two ranges: that is not a failure, and the pass is not ended.
// A settle that reaches its deadline is counted and not an error. A Retain during
// the settle replaces the pass: the settle finishes, it is counted, the old pass is
// reported, and the waiters, who now wait for the newer pass, are left alone.
func (s *Store) settleBackground(j *settleJob) retainerStep {
	p := j.p
	gen := p.marker.generation
	var res settleResult
	if cfg := s.kv.Config(); cfg.SettleRetention {
		ctx := s.ctx
		if s.betweenSettleRanges != nil {
			ctx = pebblekv.WithCompactRangesHook(ctx, s.betweenSettleRanges)
		}
		flush, settle, hit, err := s.kv.CompactRanges(ctx, j.ranges, cfg.SettleDeadline)
		if err != nil {
			if s.ctx.Err() != nil && errors.Is(err, context.Canceled) {
				return stepExit // Close: the marker stays for the next Open
			}
			s.mu.Lock()
			s.latch(err)
			s.mu.Unlock()
			return stepExit
		}
		res = settleResult{flush: flush, settle: settle, hit: hit, ran: true}
	}
	at := s.stamp()
	s.mu.Lock()
	defer s.mu.Unlock()
	if res.ran {
		s.countSettle(res.flush, res.settle, res.hit)
	}
	if s.failed != nil {
		return stepExit
	}
	if s.pass == p && s.observer != nil {
		// Told without the lock, and before the waiters are woken: a caller of
		// WaitRetained that returns has been told.
		s.mu.Unlock()
		s.observer.SettleEnded(gen, at)
		s.mu.Lock()
		if s.failed != nil {
			return stepExit
		}
	}
	if s.pass != p {
		// Replaced while it settled.
		p.report()
		p.close()
		return stepMore
	}
	if err := s.endPass(p, res); err != nil {
		s.latch(err)
		return stepExit
	}
	return stepMore
}

// endPass ends a pass whose last chunk has been committed and installed and which
// has settled (if it settles), in the order that lets WaitRetained say the pass is
// over: the marker goes, the counts are made, the pass is cleared, and the waiters
// are woken.
func (s *Store) endPass(p *retainPass, res settleResult) error {
	if err := s.releaseMarker(p.marker.generation); err != nil {
		return err
	}
	p.report()
	if res.ran {
		s.last.flush, s.last.settle, s.last.deadlineHit = res.flush, res.settle, res.hit
	}
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
	s.written(prefix)
	if err := s.touchPrefix(b, prefix); err != nil {
		return err
	}
	if !track {
		return nil // nothing is remembered when checkpoints are off and none exists
	}
	if !s.anyCkpt {
		// What the state lookup finds in a database that has no checkpoint.
		s.states.pin(string(prefix), &prefixState{latest: -1})
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
	s.states.pin(string(prefix), &res)
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

// ---------------------------------------------------------------------------
// The bound on unflushed range deletions.
//
// A pass writes one range deletion per prefix it rewrites, and so does a write that
// rewrites a prefix the pass has not reached. Until the memtable that holds them is
// flushed, the first iterator opened over it after a commit that added some
// rebuilds the memtable's list of them, at a cost that grows with how many it
// holds; a chunk opens one, as does a write that reads a prefix. The store therefore counts the range deletions retention has committed, and
// a pass flushes once that count, since the last flush it knows has completed,
// reaches retainFlushDels: without waiting, through Pebble's AsyncFlush, so that
// the pass goes on while the flush runs. Only if the count reaches twice that
// before the flush has completed does the pass wait for it. So at the start of
// every chunk, after any wait, fewer than 2 × retainFlushDels of them are
// unflushed (the count is an upper bound: Pebble's own flushes, of a full memtable,
// only lower the true number). What opening an iterator costs grows with the range
// deletions in the memtable: after a commit that added some, the first iterator
// rebuilds the memtable's list of them.
//
// The flush is never Pebble's Flush, which waits on a channel with no way out: a
// flush that keeps failing (a persistent write error) is retried by Pebble for
// ever and its channel never closes. Every wait here is a select that a cancelled
// context ends. And it is never started on a closed database, on which AsyncFlush
// panics: the retainer starts it, outside the store's lock, and Close waits for the
// retainer to exit before it closes the database; a synchronous pass starts it
// under the lock, which Close takes before it closes the database.
//
// AsyncFlush holds Pebble's commit mutex while it rotates the memtable and its log
// (a sync of the old log and the creation of a new one), and while it waits on
// Pebble's own write-stall conditions: so while it runs every commit waits, and
// the first writer to wait holds the store's lock. If level 0 had as many
// sublevels as stop writes, that wait would last until compactions caught up, and
// Close, which waits for the retainer, with it. So a flush is put off while level 0
// has at least its stop threshold less retainFlushL0Margin sublevels: it is counted
// as "retain.flush_deferred" and tried again after the next chunk. (The check is
// made just before the flush and can be overtaken; it closes almost all of the
// window.) Only that condition is checked. Pebble's other one, the memtables
// waiting to be flushed reaching MemTableStopWritesThreshold times MemTableSize in
// all, the pass cannot reach alone, since it starts a flush only once it has seen
// the last one complete; but slow flushes under heavy writes, with Pebble's own
// rotations, could reach it, and a flush started then waits with the commit mutex
// held as above.
//
// A flush that cannot be started (AsyncFlush returns an error) is counted as
// "retain.flush_errors" and is not an error of the pass: no commit's outcome is in
// doubt, so the store goes on as it was, and the flush is tried again after the
// next chunk. In this version of Pebble, AsyncFlush returns an error only for a
// database opened read-only, which never runs a pass (a failure to rotate the log
// panics inside Pebble); the path is kept so that an error a later version returns
// is counted, not made a failure of the store.
//
// While flushes are put off or fail to start, the count is not bounded, and only
// the chunk's clock, which leaves at least half the chunk time to work after the
// opening, keeps the pass moving; iterators opened by writes pay for the same
// deletions. The pass never waits for a flush it did not start.

// defaultRetainFlushDels is the count of unflushed range deletions at which a pass
// flushes. It must stay well below what a memtable holds before Pebble flushes it
// on its own, and low enough that opening an iterator over twice as many is a small
// part of a chunk's time. Provisional.
const defaultRetainFlushDels = 32768

// defaultRetainFlushL0Margin is how many sublevels below its stop threshold level 0
// must be for a pass to start a flush (see above): with Pebble's default threshold
// of 12, a flush is put off at 10 sublevels or more. Provisional.
const defaultRetainFlushL0Margin = 2

// flushBound is the store's count of the range deletions retention has committed,
// and of the flush it started to bound them. It is guarded by the store's lock.
type flushBound struct {
	// dels is the range deletions retention has committed since the store was opened
	// (those of a write that rewrote a prefix are counted when it is put in the
	// batch, so one whose commit failed is counted too). flushed is what dels was
	// when the last flush known to have completed was started: every range deletion
	// committed before then has been flushed.
	dels, flushed int64
	// pending is the channel of the flush started and not yet seen to complete, nil if
	// none, and at what dels was when it was started.
	pending <-chan struct{}
	at      int64
	// heldOff says the last flush was put off or could not be started: none is
	// tried again until a chunk has been committed.
	heldOff bool
}

// unflushed is how many range deletions retention has committed since the start
// of the last flush known to have completed: an upper bound of those the memtables
// still hold.
func (f *flushBound) unflushed() int64 { return f.dels - f.flushed }

// syncDeletes adds the range deletions the pass has made since it last did to the
// store's count. The caller holds the lock.
func (p *retainPass) syncDeletes() {
	p.s.fl.dels += p.deletes - p.counted
	p.counted = p.deletes
}

// flushAction is what bounding the unflushed range deletions asks for before a
// chunk: nothing, a flush to start (at is the count it covers), or a wait for the
// flush that is running.
type flushAction struct {
	start bool
	at    int64
	wait  <-chan struct{}
}

func (a flushAction) none() bool { return !a.start && a.wait == nil }

// flushNeeds says what is to be done, before the next chunk of p, to keep the
// unflushed range deletions under the bound. The caller holds the lock.
func (s *Store) flushNeeds(p *retainPass) flushAction {
	p.syncDeletes()
	f := &s.fl
	if f.pending != nil {
		select {
		case <-f.pending:
			f.flushed, f.pending = f.at, nil
		default:
		}
	}
	if s.retainFlushDels <= 0 {
		return flushAction{}
	}
	switch u := f.unflushed(); {
	case f.pending == nil && !f.heldOff && u >= s.retainFlushDels:
		return flushAction{start: true, at: f.dels}
	case f.pending != nil && u >= 2*s.retainFlushDels:
		return flushAction{wait: f.pending}
	}
	return flushAction{}
}

// startFlush starts a flush of the memtable, which will cover the range deletions
// counted up to at, and does not wait for it. A closed store starts none (see
// above). The caller holds the lock in a synchronous pass, and in the retainer does
// not.
func (s *Store) startFlush() (<-chan struct{}, error) {
	if s.closed.Load() {
		return nil, closedError("Retain")
	}
	if s.flushHook != nil {
		return s.flushHook(s.kv.AsyncFlush)
	}
	return s.kv.AsyncFlush()
}

// startFailed records a flush that could not be started (see above). The caller
// holds the lock.
func (s *Store) startFailed() {
	s.fl.heldOff = true
	s.rec.Count("retain.flush_errors", 1)
}

// flushDeferred records a flush put off because level 0 is near its stop threshold
// (see above). The caller holds the lock.
func (s *Store) flushDeferred() {
	s.fl.heldOff = true
	s.rec.Count("retain.flush_deferred", 1)
}

// nearStall says level 0 has so many sublevels that a flush started now could wait
// for compactions with Pebble's commit mutex held (see above). It reads Pebble's
// metrics, and so does not need the store's lock.
func (s *Store) nearStall() bool {
	if s.retainFlushL0Margin < 0 {
		return false
	}
	n, stop := s.l0Sublevels()
	return n >= stop-s.retainFlushL0Margin
}

// started records a flush that startFlush started. The caller holds the lock.
func (s *Store) started(ch <-chan struct{}, at int64) {
	s.fl.pending, s.fl.at = ch, at
	if ch == nil {
		s.fl.flushed, s.fl.pending = at, nil // nothing to wait for
	}
	s.rec.Count("retain.flushes", 1)
}

// boundFlushes keeps the unflushed range deletions under the bound before a chunk of
// a synchronous pass, with the lock held: it starts a flush, and waits for one only
// at twice the count. The wait ends with the pass's context: a Retain whose context
// is cancelled returns its error, as it does at a commit. The pass Open resumes has
// no context to cancel, and so waits as long as the flush takes; a flush that keeps
// failing makes every write wait as long, at the next full memtable.
func (p *retainPass) boundFlushes() error {
	s := p.s
	for {
		a := s.flushNeeds(p)
		switch {
		case a.none():
			return nil
		case a.start:
			if s.nearStall() {
				s.flushDeferred()
				continue
			}
			ch, err := s.startFlush()
			if err != nil {
				if s.closed.Load() {
					return err
				}
				s.startFailed()
				continue
			}
			s.started(ch, a.at)
		default:
			if s.beforeFlushWait != nil {
				s.beforeFlushWait()
			}
			select {
			case <-a.wait:
			case <-p.ctx.Done():
				return contextError("Retain", p.ctx.Err())
			}
		}
	}
}

// flushStep does, on the retainer's goroutine and without the lock, what bounding
// the unflushed range deletions asked of a step (see [Store.retainStep]): it starts
// a flush and records it, or waits for the one that is running. It returns stepExit
// when the store is closing; a flush put off or that could not be started is
// counted, and the retainer goes on to the chunk.
func (s *Store) flushStep(a flushAction) retainerStep {
	if s.ctx.Err() != nil {
		return stepExit
	}
	if a.wait != nil {
		if s.beforeFlushWait != nil {
			s.beforeFlushWait()
		}
		select {
		case <-a.wait:
			return stepMore
		case <-s.ctx.Done():
			return stepExit
		}
	}
	if s.nearStall() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.flushDeferred()
		return stepMore
	}
	ch, err := s.startFlush()
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		if s.closed.Load() {
			return stepExit
		}
		s.startFailed()
		return stepMore
	}
	s.started(ch, a.at)
	return stepMore
}
