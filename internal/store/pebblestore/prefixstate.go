package pebblestore

import (
	"context"
	"fmt"
	"iter"
	"maps"
	"slices"
	"time"

	"github.com/cockroachdb/pebble/v2"

	"github.com/lotannauo/toposhift/internal/store/prefixfilter"
)

// # The writer's map
//
// What the writer knows of the prefixes is in three parts, all under s.mu:
//
//   - A filter of the stored prefixes (prefixfilter), built by a scan of the data
//     keyspace that visits each prefix once (one seek past it), through an iterator
//     of its own and without the lock. A scan runs when a writable store opens over
//     data, and after each retention pass, never during one: a Retain that
//     publishes stops a scan that is running, which then installs nothing. A store
//     opened over an empty database has the empty filter at once. The filter is
//     never stored.
//   - Deltas: exact sets of the prefixes the writes have put keys in. A new one
//     starts when the store opens, at each retention's publication and at each
//     scan's start; a write adds every prefix it puts a key in (its records, the
//     rewrite of a prefix a background pass owes, a checkpoint the instrument's
//     hook places) to the newest, before its commit and whatever the commit
//     reports, since a commit that reports failure may have landed. A delta is
//     dropped only when a scan that began after its last insert installs its
//     filter. A scan starts its delta and then opens its iterator, both under the
//     lock: a prefix committed after the iterator's snapshot is in the delta.
//   - A cache of prefixState, the states touched most recently, bounded by
//     Options.StateCacheBytes with an estimate of each entry's size. A state a
//     Write has touched is pinned until the Write returns.
//
// The map is complete when a filter is installed and every prefix created since
// its scan began is in a live delta. A state the cache lacks is then known without
// a read for a prefix in neither the filter nor a live delta: it holds nothing (the
// empty state is used, not kept). Every other miss reads the prefix ([Store.state]).
// The map is not complete before the first scan has installed a filter, nor after
// anything that drops what the writer remembers (see Store.lose) until a scan that
// began after it installs.
//
// A retention makes the cached states of the layers it rewrites stale (they are
// misses, and read if touched), puts the states it works out into those of them
// still cached, and drops the rest when its rewriting ends; the prefixes it empties
// stay in the filter until the next scan, which costs a read, never a wrong answer.

// stateEntryBytes estimates what one cached state costs besides its checkpoints:
// the map entry and its key, the entry of the list and the state.
const stateEntryBytes = 192

// cacheEntry is one prefix in the cache. st is nil while the entry is stale.
type cacheEntry struct {
	key        string
	st         *prefixState
	prev, next *cacheEntry
	size       int64
	pinned     bool
}

// stateCache is the cache of states, the least recently used evicted first. Its
// counts go to rec; the caller holds s.mu.
type stateCache struct {
	m map[string]*cacheEntry
	// root is the sentinel of the list: root.next is the most recently used entry,
	// root.prev the least.
	root      cacheEntry
	bytes     int64
	budget    int64
	maxStates int // if not zero, at most this many entries (tests and measurements)
	pinned    []*cacheEntry
	stale     int
	rec       Recorder
}

func newStateCache(budget int64, rec Recorder) *stateCache {
	c := &stateCache{m: map[string]*cacheEntry{}, budget: budget, rec: rec}
	c.root.next, c.root.prev = &c.root, &c.root
	return c
}

func entrySize(st *prefixState) int64 {
	if st == nil {
		return stateEntryBytes
	}
	return stateEntryBytes + 8*int64(cap(st.ckpts))
}

func (c *stateCache) unlink(e *cacheEntry) {
	e.prev.next, e.next.prev = e.next, e.prev
	e.prev, e.next = nil, nil
}

func (c *stateCache) front(e *cacheEntry) {
	if e.prev != nil {
		c.unlink(e)
	}
	e.prev, e.next = &c.root, c.root.next
	c.root.next.prev = e
	c.root.next = e
}

func (c *stateCache) addBytes(n int64) {
	if n != 0 {
		c.bytes += n
		c.rec.Count("writer_map.lru_bytes", n)
	}
}

// get is the state of the prefix, made the most recently used; nil if the cache
// does not hold it, or holds it stale.
func (c *stateCache) get(k string) *prefixState {
	e := c.m[k]
	if e == nil || e.st == nil {
		return nil
	}
	c.front(e)
	return e.st
}

// peek is get without making the entry the most recently used.
func (c *stateCache) peek(k string) *prefixState {
	if e := c.m[k]; e != nil {
		return e.st
	}
	return nil
}

// has says the cache holds an entry for the prefix, stale or not.
func (c *stateCache) has(k string) bool {
	_, ok := c.m[k]
	return ok
}

// len is how many states the cache holds, not counting stale entries.
func (c *stateCache) len() int { return len(c.m) - c.stale }

// all yields the states the cache holds, not the stale entries.
func (c *stateCache) all() iter.Seq2[string, *prefixState] {
	return func(yield func(string, *prefixState) bool) {
		for k, e := range c.m {
			if e.st != nil && !yield(k, e.st) {
				return
			}
		}
	}
}

// put makes st the state of the prefix, as the most recently used, and evicts the
// least recently used unpinned entries beyond the budget. It never evicts the
// entry it puts.
func (c *stateCache) put(k string, st *prefixState) *cacheEntry {
	e := c.m[k]
	if e == nil {
		e = &cacheEntry{key: k}
		c.m[k] = e
	} else if e.st == nil {
		c.stale--
	}
	e.st = st
	c.resize(e)
	c.front(e)
	c.trim(e)
	return e
}

// pin makes st the state of the prefix if the cache does not hold it as such, and
// keeps the entry from eviction until unpinAll.
func (c *stateCache) pin(k string, st *prefixState) {
	e := c.m[k]
	if e == nil || e.st != st {
		e = c.put(k, st)
	}
	if !e.pinned {
		e.pinned = true
		c.pinned = append(c.pinned, e)
	}
}

// unpinAll ends the pins of a Write: the sizes of the states it changed are taken
// again, and the cache is brought back within its budget.
func (c *stateCache) unpinAll() {
	for _, e := range c.pinned {
		e.pinned = false
		if c.m[e.key] == e {
			c.resize(e)
		}
	}
	c.pinned = c.pinned[:0]
	c.trim(nil)
}

// replace sets the state of an entry the cache holds, stale or not, without making
// it more recently used or evicting anything: a retention's worked-out state.
func (c *stateCache) replace(k string, st *prefixState) {
	e := c.m[k]
	if e == nil {
		return
	}
	if e.st == nil {
		c.stale--
	}
	e.st = st
	c.resize(e)
}

func (c *stateCache) resize(e *cacheEntry) {
	n := entrySize(e.st)
	c.addBytes(n - e.size)
	e.size = n
}

func (c *stateCache) over() bool {
	return c.bytes > c.budget || (c.maxStates > 0 && len(c.m) > c.maxStates)
}

// trim evicts the least recently used entries, but the pinned ones and keep, while
// the cache is over its budget.
func (c *stateCache) trim(keep *cacheEntry) {
	for e := c.root.prev; e != &c.root && c.over(); {
		prev := e.prev
		if !e.pinned && e != keep {
			c.drop(e)
			c.rec.Count("writer_map.lru_evictions", 1)
		}
		e = prev
	}
}

func (c *stateCache) drop(e *cacheEntry) {
	if e.st == nil {
		c.stale--
	}
	c.unlink(e)
	delete(c.m, e.key)
	c.addBytes(-e.size)
}

// remove drops the entry of the prefix, if there is one.
func (c *stateCache) remove(k string) {
	if e := c.m[k]; e != nil {
		c.drop(e)
	}
}

// clear drops every entry.
func (c *stateCache) clear() {
	for _, e := range c.pinned {
		e.pinned = false
	}
	c.addBytes(-c.bytes)
	c.m = map[string]*cacheEntry{}
	c.root.next, c.root.prev = &c.root, &c.root
	c.pinned, c.stale = c.pinned[:0], 0
}

// staleWhere makes stale every entry whose prefix pick picks.
func (c *stateCache) staleWhere(pick func(string) bool) {
	for k, e := range c.m {
		if e.st != nil && pick(k) {
			e.st = nil
			c.stale++
			c.resize(e)
		}
	}
}

// dropStale drops the stale entries.
func (c *stateCache) dropStale() {
	if c.stale == 0 {
		return
	}
	for _, e := range c.m {
		if e.st == nil {
			c.drop(e)
		}
	}
}

// setCap sets the cap on the number of entries and evicts down to it.
func (c *stateCache) setCap(n int) {
	c.maxStates = n
	c.trim(nil)
}

// prefixKey is a data prefix, the key of a delta.
type prefixKey [prefixLen]byte

// delta is an exact set of the prefixes written since it started.
type delta struct{ set map[prefixKey]struct{} }

// newDelta starts a delta, which the writes then add to. The caller holds s.mu.
func (s *Store) newDelta() *delta {
	d := &delta{set: map[prefixKey]struct{}{}}
	s.deltas = append(s.deltas, d)
	return d
}

// written adds a prefix a write is about to put a key in to the newest delta. The
// caller holds s.mu.
func (s *Store) written(prefix []byte) {
	if len(s.deltas) == 0 || len(prefix) != prefixLen {
		return // a store opened read-only writes nothing and has no delta
	}
	d := s.deltas[len(s.deltas)-1]
	k := prefixKey(prefix)
	if _, ok := d.set[k]; !ok {
		d.set[k] = struct{}{}
		s.rec.Count("writer_map.delta_prefixes", 1)
	}
}

// filterInstalled says a filter of the stored prefixes is installed. The caller
// holds s.mu.
func (s *Store) filterInstalled() bool { return s.filter != nil }

// inFilterOrDelta says the prefix is in the installed filter, as built and not as
// a test hook forces it to answer, or in a live delta. The caller holds s.mu.
func (s *Store) inFilterOrDelta(prefix []byte) bool {
	if s.filter != nil && s.filter.Contains(prefix) {
		return true
	}
	if len(prefix) != prefixLen {
		return false
	}
	k := prefixKey(prefix)
	for _, d := range s.deltas {
		if _, ok := d.set[k]; ok {
			return true
		}
	}
	return false
}

// scanning says the goroutine of a scan has not exited.
func (s *Store) scanning() bool { return s.liveScans.Load() > 0 }

// scanPoint names a point of a scan of the stored prefixes, at which the test hook
// Options.scanHook is called.
type scanPoint uint8

const (
	// scanStarting is before the scan takes the lock to start its delta and open
	// its iterator.
	scanStarting scanPoint = iota
	// scanOpened is after both, with the lock released.
	scanOpened
	// scanHopped is after each prefix the scan visits.
	scanHopped
	// scanBuilt is after the filter is built, before it is installed.
	scanBuilt
)

// scanRun is one scan of the stored prefixes.
type scanRun struct {
	ctx    context.Context
	cancel context.CancelFunc
	// delta is the delta it started, and lost the count of losses (see Store.lose)
	// when it started: the map is complete when it installs only if no loss came
	// after.
	delta *delta
	lost  uint64
}

// openEmpty installs the filter of a database with no data, at once: there is
// nothing to scan, and the map is complete. The caller holds s.mu, or has not
// handed the store out.
func (s *Store) openEmpty() {
	f, err := prefixfilter.Build(func(func([]byte) bool) {})
	if err != nil {
		return // cannot happen for no keys; the map then waits for a scan
	}
	s.installFilter(f, s.deltas[len(s.deltas)-1], s.lost, 0)
}

// startScan starts a scan of the stored prefixes on a goroutine of its own, in
// place of one that is running, which stops and installs nothing. A store that is
// closed, opened read-only or reads whole prefixes (tests) scans nothing. The
// caller holds s.mu.
func (s *Store) startScan() {
	if s.closed.Load() || s.kv.Config().ReadOnly || s.fullStateRead {
		return
	}
	s.cancelScan()
	ctx, cancel := context.WithCancel(s.scanCtx)
	r := &scanRun{ctx: ctx, cancel: cancel}
	s.scan = r
	if s.scanWait == nil {
		s.scanWait = make(chan struct{})
	}
	s.scans.Add(1)
	s.liveScans.Add(1)
	go s.runScan(r)
}

// cancelScan stops the scan that is running, if there is one: it installs nothing,
// and the delta it started stays live. Its goroutine may still be running, and
// Close waits for it. The caller holds s.mu.
func (s *Store) cancelScan() {
	if s.scan != nil {
		s.scan.cancel()
		s.endScan(s.scan)
	}
}

// endScan says the scan is not the store's any more, and wakes the callers of
// WaitScanned if it was. The caller holds s.mu.
func (s *Store) endScan(r *scanRun) {
	if s.scan != r {
		return
	}
	s.scan = nil
	if s.scanWait != nil {
		close(s.scanWait)
		s.scanWait = nil
	}
}

func (s *Store) hook(at scanPoint, hops int, last []byte) {
	if s.scanHook != nil {
		s.scanHook(at, hops, last)
	}
}

// runScan is the goroutine of a scan: it visits every stored prefix once, builds
// the filter of them and installs it, unless it is stopped first.
func (s *Store) runScan(r *scanRun) {
	defer func() {
		r.cancel()
		s.liveScans.Add(-1)
		s.scans.Done()
	}()
	started := time.Now()
	s.hook(scanStarting, 0, nil)
	s.mu.Lock()
	if r.ctx.Err() != nil || s.scan != r {
		s.endScan(r)
		s.mu.Unlock()
		return
	}
	// The delta first, then the iterator: a write adds its prefixes to the newest
	// delta before it commits, so a prefix committed after the iterator's snapshot
	// is in this delta, which lives until a later scan installs.
	r.delta, r.lost = s.newDelta(), s.lost
	lo, hi := dataBounds()
	it, err := s.kv.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	s.mu.Unlock()
	if err != nil {
		s.failScan(r)
		return
	}
	s.hook(scanOpened, 0, nil)
	var hops int
	var scanErr error
	stopped := false
	f, err := prefixfilter.Build(func(yield func([]byte) bool) {
		for ok := it.First(); ok; {
			if r.ctx.Err() != nil {
				stopped = true
				return
			}
			key := it.Key()
			if len(key) < prefixLen {
				scanErr = fmt.Errorf("pebblestore: key %x is shorter than a prefix", key)
				return
			}
			next := prefixSucc(key[:prefixLen])
			var last []byte
			if s.scanHook != nil {
				last = slices.Clone(key[:prefixLen])
			}
			if !yield(key[:prefixLen]) {
				return
			}
			hops++
			s.hook(scanHopped, hops, last)
			ok = it.SeekGE(next)
		}
		if scanErr == nil {
			scanErr = it.Error()
		}
	})
	if cerr := it.Close(); scanErr == nil {
		scanErr = cerr
	}
	if scanErr == nil {
		scanErr = err
	}
	if stopped || r.ctx.Err() != nil {
		s.mu.Lock()
		s.endScan(r)
		s.mu.Unlock()
		return
	}
	if scanErr != nil {
		s.failScan(r)
		return
	}
	s.hook(scanBuilt, hops, nil)
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.ctx.Err() != nil || s.scan != r {
		s.endScan(r)
		return
	}
	s.installFilter(f, r.delta, r.lost, time.Since(started))
	s.endScan(r)
}

// failScan ends a scan that could not read the database: it is counted, nothing is
// installed, and the deltas stay, so the map is as it was.
func (s *Store) failScan(r *scanRun) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.countLater("writer_map.scan_errors", 1)
	s.endScan(r)
}

// installFilter makes f the filter, drops the deltas older than d, the delta of the
// scan that built it, and says the map is complete unless something has been lost
// since the scan began. The caller holds s.mu.
func (s *Store) installFilter(f *prefixfilter.Filter, d *delta, lost uint64, took time.Duration) {
	i := slices.Index(s.deltas, d)
	if i < 0 {
		return // cannot happen: only the install of a later scan drops a delta
	}
	var old int64
	if s.filter != nil {
		old = int64(s.filter.SizeBytes())
	}
	s.filter = f
	s.countLater("writer_map.filter_bytes", int64(f.SizeBytes())-old)
	var dropped int64
	for _, x := range s.deltas[:i] {
		dropped += int64(len(x.set))
	}
	s.countLater("writer_map.delta_prefixes", -dropped)
	s.deltas = slices.Delete(s.deltas, 0, i)
	s.complete = s.lost == lost
	s.countLater("writer_map.scan_ns", took.Nanoseconds())
}

// countLater keeps a count of a scan for the next call that holds the lock on the
// caller's goroutine (see flushCounts), so that a recorder is called at the moments
// it always was, and the counts a caller reads after an operation do not depend on
// when a scan ended. The caller holds s.mu.
func (s *Store) countLater(name string, n int64) {
	if n == 0 {
		return
	}
	if s.laterCounts == nil {
		s.laterCounts = map[string]int64{}
	}
	s.laterCounts[name] += n
}

// flushCounts gives the recorder the counts kept by countLater, in the order of
// their names. The caller holds s.mu.
func (s *Store) flushCounts() {
	for _, name := range slices.Sorted(maps.Keys(s.laterCounts)) {
		s.rec.Count(name, s.laterCounts[name])
	}
	clear(s.laterCounts)
}

// releaseWriterMap gives back what the counts of the writer map say it holds, when
// the store is closed. The caller holds s.mu.
func (s *Store) releaseWriterMap() {
	s.flushCounts()
	s.states.clear()
	if s.filter != nil {
		s.rec.Count("writer_map.filter_bytes", -int64(s.filter.SizeBytes()))
		s.filter = nil
	}
	var n int64
	for _, d := range s.deltas {
		n += int64(len(d.set))
	}
	if n != 0 {
		s.rec.Count("writer_map.delta_prefixes", -n)
	}
	s.deltas = nil
	s.complete = false
}
