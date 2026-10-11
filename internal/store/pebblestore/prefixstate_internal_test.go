package pebblestore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"math/rand"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
	"github.com/lotannauo/toposhift/internal/store/pebblekv"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

// The writer's map (prefixstate.go) takes a prefix it keeps no state of for empty,
// without reading it, only when a filter of the stored prefixes is installed and
// the prefix is in neither that filter nor a delta of the prefixes written since
// the filter's scan began. Such an answer about a prefix that holds a checkpoint is
// a stale checkpoint, and so a wrong read answer. These tests hold the map to one
// claim above all: once a filter is installed, every prefix that holds a key is in
// it or in a live delta; and to the claims that make that claim cheap: a state it
// keeps is what a read of the prefix finds, it keeps no more than its budget, and
// it reads no prefix it can answer without.

// ---------------------------------------------------------------------------
// Helpers.

// cachedStates is every state the writer keeps, by prefix, the stale entries left
// out. The caller holds s.mu.
func cachedStates(s *Store) map[string]*prefixState {
	return maps.Collect(s.states.all())
}

// forgetCached drops the state the writer keeps of a prefix, as an eviction does.
// The caller holds s.mu.
func forgetCached(s *Store, prefix []byte) {
	s.states.remove(string(prefix))
}

// failedStore says the store has stopped writing until it is reopened.
func failedStore(s *Store) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.failed != nil
}

// prefixesWithAnyKey is every prefix that holds a key of any kind: a record, a
// checkpoint or a baseline. (A prefix that holds only a baseline reads as empty,
// so taking it for empty would be harmless; it is held to the claim all the same,
// since the scan sees its key.)
func prefixesWithAnyKey(s *Store) (map[string]struct{}, error) {
	out := map[string]struct{}{}
	lo, hi := dataBounds()
	it, err := s.kv.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return nil, err
	}
	defer func() { _ = it.Close() }()
	for ok := it.First(); ok; ok = it.Next() {
		prefix, _, _, _, err := parseKey(it.Key())
		if err != nil {
			return nil, err
		}
		out[string(prefix)] = struct{}{}
	}
	return out, it.Error()
}

// writerMapMismatch returns the first way the writer's map is wrong, or nil:
//
//   - it is complete and no filter is installed;
//   - a filter is installed and a prefix that holds a key is in neither the filter
//     (as built, not as a test hook forces it to answer) nor a live delta;
//   - it keeps more states than its cap;
//   - a state it keeps is not what a read of the whole prefix finds, as far as cmp
//     compares it.
//
// The caller holds s.mu, and no Write is in progress.
func writerMapMismatch(s *Store, cmp stateCompare) error {
	if s.complete && !s.filterInstalled() {
		return errors.New("the map is complete and no filter of the stored prefixes is installed")
	}
	if s.filterInstalled() {
		held, err := prefixesWithAnyKey(s)
		if err != nil {
			return err
		}
		for _, k := range slices.Sorted(maps.Keys(held)) {
			if !s.inFilterOrDelta([]byte(k)) {
				return fmt.Errorf("the prefix %x holds keys and is in neither the filter nor a live delta (complete %v)", k, s.complete)
			}
		}
	}
	cached := cachedStates(s)
	if c := s.states.maxStates; c > 0 && len(s.states.m) > c {
		return fmt.Errorf("%d states are kept, and the cap is %d", len(s.states.m), c)
	}
	if s.states.bytes > s.states.budget {
		return fmt.Errorf("the states kept take %d bytes, and the budget is %d", s.states.bytes, s.states.budget)
	}
	if !s.anyCkpt {
		// A database without a checkpoint is never read for a state: every prefix is
		// taken to hold nothing until a write says otherwise.
		return nil
	}
	probe := wholeReader(s)
	for _, k := range slices.Sorted(maps.Keys(cached)) {
		b := cached[k]
		a, err := probe.state([]byte(k))
		if err != nil {
			return err
		}
		differs := !slices.Equal(checkpointsFrom(a.ckpts, b.below), b.ckpts)
		if cmp >= compareLatest {
			differs = differs || a.latest != b.latest
		}
		if differs {
			return fmt.Errorf("the writer keeps %+v of the prefix %x, where a whole read finds %+v", *b, k, *a)
		}
	}
	return nil
}

// stateCompare says how much of a state the writer keeps must be what a read of the
// whole prefix finds. The counts since the last checkpoint and its size are never
// compared: a state the writer carries counts the records since the checkpoint it
// last wrote, and keeps counting them when a late record deletes that checkpoint,
// where a read counts back to the newest checkpoint left; and under a lag, or after
// a checkpoint the instrument's hook placed above the newest record, it counts
// records a read does not. The map of every prefix always did so. They decide only
// where the policy places a checkpoint, never whether one is true.
type stateCompare uint8

const (
	// compareCheckpoints compares the checkpoints at or after the state's bound: what
	// keeps every checkpoint true, since a record deletes the ones it knows of. It is
	// all that is exact of a state the writer took for empty, unread, while no
	// checkpoint was in the database (which the store has always done, and does
	// still): a later checkpoint makes the database one whose states are read, and
	// that state's newest record and counts are what was written since.
	compareCheckpoints stateCompare = iota
	// compareLatest adds the newest record.
	compareLatest
)

// requireWriterMap fails the test with what writerMapMismatch finds, comparing the
// newest record and the checkpoints of each state kept: the tests that call it
// write no state before their first checkpoint into a database that holds data.
func requireWriterMap(t *testing.T, s *Store) {
	t.Helper()
	s.mu.Lock()
	err := writerMapMismatch(s, compareLatest)
	s.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
}

// mapState is whether the map is complete and has a filter, read under the lock.
func mapState(s *Store) (complete, installed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.complete, s.filterInstalled()
}

// completeOf says the map is complete, read under the lock: a scan installs from a
// goroutine of its own.
func completeOf(s *Store) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.complete
}

// covered says the prefix is in the filter or a live delta, read under the lock.
func covered(s *Store, prefix []byte) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inFilterOrDelta(prefix)
}

func waitScanned(t *testing.T, s *Store) {
	t.Helper()
	ctx, cancel := context.WithTimeout(bg, 5*time.Minute)
	defer cancel()
	if err := s.Instrument().WaitScanned(ctx); err != nil {
		t.Fatal(err)
	}
}

// scanPos is where a held scan is: the point, the prefixes it has visited and the
// last of them (nil before the first).
type scanPos struct {
	at   scanPoint
	hops int
	last []byte
}

// scanPause holds one scan of the stored prefixes, so that a test can act while the
// scan is open. Armed at a point, its hook blocks the first scan that reaches the
// point (for scanHopped, the hop given or a later one), or that reaches the end of
// its walk first, before its filter is installed, until the test resumes it. The
// test is the only one to act meanwhile: the scan's goroutine is blocked, so what a
// history does while a scan is held does not depend on timing.
type scanPause struct {
	mu      sync.Mutex
	armed   bool
	at      scanPoint
	hop     int
	paused  chan scanPos
	resumed chan struct{}
	let     bool // resumed is closed
}

func newScanPause() *scanPause { return &scanPause{} }

func (p *scanPause) hook(at scanPoint, hops int, last []byte) {
	p.mu.Lock()
	hit := p.armed && ((at == p.at && (at != scanHopped || hops >= p.hop)) || at == scanBuilt)
	if !hit {
		p.mu.Unlock()
		return
	}
	p.armed = false
	paused, resumed := p.paused, p.resumed
	p.mu.Unlock()
	paused <- scanPos{at, hops, slices.Clone(last)}
	<-resumed
}

// arm makes the next scan that reaches the point stop there.
func (p *scanPause) arm(at scanPoint, hop int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.armed, p.at, p.hop = true, at, hop
	p.paused, p.resumed, p.let = make(chan scanPos, 1), make(chan struct{}), false
}

// await returns where the armed scan is held, or false if every scan of the store
// ended (or none was due) without reaching it, in which case it is disarmed.
func (p *scanPause) await(t *testing.T, s *Store) (scanPos, bool) {
	t.Helper()
	p.mu.Lock()
	paused := p.paused
	p.mu.Unlock()
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(bg, 5*time.Minute)
		defer cancel()
		done <- s.Instrument().WaitScanned(ctx)
	}()
	select {
	case pos := <-paused:
		// A test that fails while the scan is held must not leave it held: the store's
		// Close, which the test's cleanup calls, waits for the scan.
		t.Cleanup(p.resume)
		return pos, true
	case <-done:
		select {
		case pos := <-paused:
			t.Cleanup(p.resume)
			return pos, true
		default:
		}
		p.mu.Lock()
		p.armed = false
		p.mu.Unlock()
		return scanPos{}, false
	case <-time.After(5 * time.Minute):
		t.Fatal("the armed scan neither stopped nor ended")
	}
	return scanPos{}, false
}

// abandon leaves the held scan held for good: for a test that has found the store
// closed under it, where letting it go on would read a closed database.
func (p *scanPause) abandon() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.let = true
}

// resume lets the held scan go on; resuming it again does nothing.
func (p *scanPause) resume() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.resumed != nil && !p.let {
		close(p.resumed)
		p.let = true
	}
}

// wmFailures are the commits the next operation makes fail.
type wmFailures struct{ recordBefore, recordAfter, ckptBefore, ckptAfter bool }

func (f *wmFailures) hooks(o *Options) {
	take := func(b *bool) func() error {
		return func() error {
			if *b {
				*b = false
				return errInjected
			}
			return nil
		}
	}
	o.beforeRecordApply = take(&f.recordBefore)
	o.afterRecordApply = take(&f.recordAfter)
	o.beforeCheckpointApply = take(&f.ckptBefore)
	o.afterCheckpointApply = take(&f.ckptAfter)
}

// wmPod and wmNode are distinct entities for i and j up to 255; the edges of pod i
// are stored in layerOfPod(i).
func wmPod(i int) identity.Fingerprint  { return fingerprintOf(catalog.K8sPod, byte(i)) }
func wmNode(j int) identity.Fingerprint { return fingerprintOf(catalog.K8sNode, byte(j)) }

func wmEdge(seq uint64, i, j int, at time.Time, kind lifecycle.Kind) store.Record {
	r := store.Record{
		Layer: layerOfPod(i), Subject: store.EdgeSubject(wmPod(i), wmNode(j), catalog.ScheduledOn), Producer: "p",
		EventTime: at, Seq: seq, Kind: kind,
	}
	if kind == lifecycle.Observe {
		r.Payload = fmt.Appendf(nil, "edge-%d", seq)
	}
	return r
}

func wmEntity(seq uint64, i int, at time.Time) store.Record {
	r := entityRecord(seq, wmPod(i), "p", at)
	r.Payload = fmt.Appendf(nil, "pod-%d", seq)
	return r
}

// wmWorld is a store, the reference beside it and the sequence numbers given so far.
type wmWorld struct {
	t   *testing.T
	s   *Store
	ref *memstore.Store
	seq uint64
}

func newWMWorld(t *testing.T, s *Store) *wmWorld {
	return &wmWorld{t: t, s: s, ref: newRef(t)}
}

// edges writes, in one batch, an edge of each pod i to node i%3 at the instant.
func (w *wmWorld) edges(at time.Time, pods ...int) {
	w.t.Helper()
	var batch []store.Record
	for _, i := range pods {
		w.seq++
		batch = append(batch, wmEdge(w.seq, i, i%3, at, lifecycle.Observe))
	}
	w.write(batch...)
}

func (w *wmWorld) write(batch ...store.Record) {
	w.t.Helper()
	for _, x := range []store.Store{w.s, w.ref} {
		if err := x.Write(bg, cloneAll(batch)); err != nil {
			w.t.Fatal(err)
		}
	}
}

func (w *wmWorld) retain(h time.Time) {
	w.t.Helper()
	for _, x := range []store.Store{w.s, w.ref} {
		if err := x.Retain(bg, h); err != nil {
			w.t.Fatal(err)
		}
	}
	waitRetained(w.t, w.s)
}

// same compares every answer about the pods given and the three nodes with the
// reference's.
func (w *wmWorld) same(pods []int, times ...time.Time) {
	w.t.Helper()
	var fps []identity.Fingerprint
	for _, i := range pods {
		fps = append(fps, wmPod(i))
	}
	for j := range 3 {
		fps = append(fps, wmNode(j))
	}
	if err := asyncAnswers(w.s, w.ref, fps, times, []uint64{store.Latest, w.s.LastSeq()}); err != nil {
		w.t.Fatal(err)
	}
}

// upTo is the pods 0 to n-1.
func upTo(n int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = i
	}
	return out
}

// wmOpen opens a store on fs with the options the test sets and the writer map's
// hooks.
func wmOpen(t *testing.T, fs vfs.FS, o Options) *Store {
	t.Helper()
	o.Tuning, o.FS = pebblekv.TinyTuning(), fs
	if o.Retention == Background {
		o.retainBatchBytes, o.retainChunkTime = 1, time.Hour
	}
	return mustOpen(t, o)
}

// prefixOfEdge is the forward prefix of pod i's edges.
func prefixOfEdge(t *testing.T, s *Store, i int) []byte {
	t.Helper()
	p, ok := s.prefixOf(layerOfPod(i), wmPod(i), byte(store.Forward))
	if !ok {
		t.Fatal("the pod's type is not in the catalog")
	}
	return p
}

var bothModes = []RetentionMode{Synchronous, Background}

func modeName(m RetentionMode) string {
	if m == Background {
		return "background"
	}
	return "synchronous"
}

// ---------------------------------------------------------------------------
// Section 3.2: no false negative, on random histories.

// wmCoverage is what the random histories exercised.
type wmCoverage struct {
	paused, createdDuringScan, behindScan, aheadScan        int64
	owed, passed, touchRewrites                             int64
	cold, evictions, completeSteps                          int64
	landedFailures, closedDuringScan, retainedDuringScan    int64
	hookIntoNew, retentions, reopenedPending, installedEver int64
}

func (c *wmCoverage) add(o wmCoverage) {
	c.paused += o.paused
	c.createdDuringScan += o.createdDuringScan
	c.behindScan += o.behindScan
	c.aheadScan += o.aheadScan
	c.owed += o.owed
	c.passed += o.passed
	c.touchRewrites += o.touchRewrites
	c.cold += o.cold
	c.evictions += o.evictions
	c.completeSteps += o.completeSteps
	c.landedFailures += o.landedFailures
	c.closedDuringScan += o.closedDuringScan
	c.retainedDuringScan += o.retainedDuringScan
	c.hookIntoNew += o.hookIntoNew
	c.retentions += o.retentions
	c.reopenedPending += o.reopenedPending
	c.installedEver += o.installedEver
}

// TestNoPrefixThatHoldsKeysIsTakenForEmpty runs random histories, in both
// retention modes, through a store whose cache holds one, three, eight or the
// default budget of states, and after every batch and every retention (and after
// every write a background pass lets in between its chunks, and every write made
// while a scan is held) holds the writer's map to writerMapMismatch: with a filter
// installed, every prefix that holds a key is in it or in a live delta, which an
// exhaustive scan of the database checks; a complete map has a filter; the cache
// keeps no more than its cap, and every state it keeps is what a whole read finds.
//
// The histories write late records on a nanosecond grid, several records per prefix
// in a batch, new prefixes throughout, checkpoints by the policy (with a lag and a
// size condition, or none) and by the hook (some into prefixes that hold nothing);
// they retain (in the background with writes between the chunks, to prefixes the
// pass owes and ones it has passed, so that writes rewrite prefixes first), reopen
// (sometimes closing a background store with its pass pending, which the next Open
// resumes), make record commits fail before and after landing (the store then stops
// until it is reopened, and the map is checked before that), make checkpoint
// commits fail, and hold scans at each of their points to write behind and ahead of
// the scan, to retain, to place checkpoints, or to close the store. The answers are
// compared with the reference's every few steps.
func TestNoPrefixThatHoldsKeysIsTakenForEmpty(t *testing.T) {
	if storetest.Trimmed() {
		t.Skip("trimmed run: the random histories run in the full tier")
	}
	t.Parallel()
	seeds := 48
	if raceEnabled {
		seeds = 4 // the scans and passes run on goroutines of their own, but the test drives them in turn
	}
	var mu sync.Mutex
	var tot wmCoverage
	t.Run("seeds", func(t *testing.T) {
		for seed := range seeds {
			t.Run(fmt.Sprint(seed), func(t *testing.T) {
				t.Parallel()
				c := runWriterMapHistory(t, int64(seed))
				mu.Lock()
				defer mu.Unlock()
				tot.add(c)
			})
		}
	})
	if t.Failed() {
		return
	}
	t.Logf("%+v", tot)
	if tot.paused == 0 || tot.createdDuringScan == 0 || tot.behindScan == 0 || tot.aheadScan == 0 {
		t.Fatalf("the histories held %d scans and created %d prefixes during one (%d behind it, %d ahead): they do not exercise the scan", tot.paused, tot.createdDuringScan, tot.behindScan, tot.aheadScan)
	}
	if tot.owed == 0 || tot.passed == 0 || tot.touchRewrites == 0 {
		t.Fatalf("the histories wrote %d times to prefixes a pass owed and %d to prefixes it had passed, with %d rewrites by a write: they do not exercise the background pass", tot.owed, tot.passed, tot.touchRewrites)
	}
	if tot.cold == 0 || tot.evictions == 0 || tot.completeSteps == 0 || tot.installedEver == 0 {
		t.Fatalf("the histories read %d prefixes before a first scan, evicted %d states, had a complete map in %d steps and a filter in %d: they do not exercise the map", tot.cold, tot.evictions, tot.completeSteps, tot.installedEver)
	}
	// The race detector's few seeds are for the races; the rarer edges are the plain
	// run's, which takes every seed.
	if !raceEnabled && (tot.landedFailures == 0 || tot.closedDuringScan == 0 || tot.retainedDuringScan == 0 || tot.hookIntoNew == 0 || tot.reopenedPending == 0) {
		t.Fatalf("the histories failed %d commits that landed, closed %d stores and retained %d times during a scan, placed %d checkpoints into prefixes that held nothing and reopened %d pending passes: they do not exercise the edges", tot.landedFailures, tot.closedDuringScan, tot.retainedDuringScan, tot.hookIntoNew, tot.reopenedPending)
	}
}

// chunkWrites are the batches a history writes between the chunks of a background
// pass, each after the chunk count in its slot, from the retainer's goroutine.
type chunkWrites struct {
	slots   []int
	batches [][]store.Record
	next    int
	calls   int
	write   func([]store.Record)
}

func (w *chunkWrites) hook() {
	k := w.calls
	w.calls++
	for w.next < len(w.batches) && w.slots[w.next] <= k {
		w.write(w.batches[w.next])
		w.next++
	}
}

// rest writes the batches the pass ended before.
func (w *chunkWrites) rest() {
	for ; w.next < len(w.batches); w.next++ {
		w.write(w.batches[w.next])
	}
}

func runWriterMapHistory(t *testing.T, seed int64) wmCoverage {
	var cov wmCoverage
	rng := rand.New(rand.NewSource(seed))
	mode := bothModes[int(seed%2)]
	lags := []time.Duration{0, 0, 1, 3}
	ck := CheckpointOptions{On: rng.Intn(6) != 0, KMin: 1 + rng.Intn(3), Alpha: []float64{0, 2}[rng.Intn(2)], Lag: lags[rng.Intn(len(lags))]}
	capN := []int{1, 3, 8, 0}[rng.Intn(4)]
	useHook := rng.Intn(2) == 0
	// unread says the writer may keep a state it took for empty, unread, while no
	// checkpoint was in the database: until the store is reopened over one, or a
	// retention works the states out again, only the checkpoints of a state are
	// compared.
	unread := false
	fs := vfs.NewMem()
	rec := &countRecorder{}
	pause := newScanPause()
	var fail wmFailures
	var chunks atomic.Pointer[chunkWrites]
	errs := &asyncErrs{}
	ref := newRef(t)
	var s *Store
	var log []string
	logf := func(format string, args ...any) { log = append(log, fmt.Sprintf(format, args...)) }
	fatalf := func(format string, args ...any) {
		t.Helper()
		t.Fatalf("seed %d (%s, %+v, cap %d, hook %v): %s\nhistory:\n%v", seed, modeName(mode), ck, capN, useHook, fmt.Sprintf(format, args...), log)
	}
	open := func() {
		o := Options{
			Config: pebblekv.Config{Tuning: pebblekv.TinyTuning(), FS: fs}, Recorder: rec, Checkpoints: policy(ck), Retention: mode,
			retainBatchBytes: 1 + rng.Intn(300), retainChunkTime: time.Hour, scanHook: pause.hook,
			beforeRetainChunk: func() {
				if w := chunks.Load(); w != nil {
					w.hook()
				}
			},
		}
		fail.hooks(&o)
		var err error
		if s, err = Open("db", o); err != nil {
			fatalf("Open: %v", err)
		}
		s.Instrument().CapCachedStates(capN)
		unread = false
	}
	open()
	defer func() {
		pause.resume() // a history that fails while a scan is held lets it go, so that Close returns
		_ = s.Close()
	}()
	check := func(what string) error {
		s.mu.Lock()
		defer s.mu.Unlock()
		if !s.anyCkpt {
			unread = true
		}
		cmp := compareLatest
		if unread {
			cmp = compareCheckpoints
		}
		if err := writerMapMismatch(s, cmp); err != nil {
			return fmt.Errorf("%s: %w", what, err)
		}
		return nil
	}
	mustCheck := func(what string) {
		t.Helper()
		if err := check(what); err != nil {
			fatalf("%v", err)
		}
		complete, installed := mapState(s)
		if complete {
			cov.completeSteps++
		}
		if installed {
			cov.installedEver++
		}
	}

	const pods = 40
	known := 3
	horizon, retained := 0, false
	var seq uint64
	at := func(n int) time.Time { return t0.Add(time.Duration(n)) }
	record := func(fresh bool) store.Record {
		seq++
		if (fresh || rng.Intn(8) == 0) && known < pods {
			known++
		}
		i := rng.Intn(known)
		if fresh {
			i = known - 1
		}
		ns := horizon + rng.Intn(60)
		kind, ttl := lifecycle.Observe, time.Duration(0)
		switch {
		case rng.Intn(4) == 0:
			kind = lifecycle.Delete
		case rng.Intn(4) == 0:
			ttl = time.Duration(1 + rng.Intn(10))
		}
		var r store.Record
		if rng.Intn(4) == 0 {
			r = wmEntity(seq, i, at(ns))
			r.Kind, r.TTL = kind, ttl
			if kind == lifecycle.Delete {
				r.Payload = nil
			}
		} else {
			r = wmEdge(seq, i, rng.Intn(3), at(ns), kind)
			r.TTL = ttl
		}
		r.Producer = lifecycle.Producer([]string{"p", "q"}[rng.Intn(2)])
		return r
	}
	batchOf := func(fresh bool) []store.Record {
		var b []store.Record
		for range 1 + rng.Intn(4) {
			b = append(b, record(fresh && rng.Intn(2) == 0))
		}
		return b
	}
	// puts are the prefixes a batch puts records in.
	puts := func(batch []store.Record) [][]byte {
		var out [][]byte
		for _, r := range batch {
			sides, err := s.sidesOf(r)
			if err != nil {
				fatalf("%v", err)
			}
			for _, sd := range sides {
				out = append(out, sd.prefix)
			}
		}
		return out
	}
	held := func() map[string]struct{} {
		h, err := prefixesWithAnyKey(s)
		if err != nil {
			fatalf("%v", err)
		}
		return h
	}
	reopen := func() {
		if err := s.Close(); err != nil && !errors.Is(err, errInjected) {
			fatalf("Close: %v", err)
		}
		open()
		logf("reopen")
	}
	// write writes a batch from the test's goroutine, with the failures armed.
	write := func(batch []store.Record) {
		t.Helper()
		notLanding := fail.recordBefore
		err := s.Write(bg, cloneAll(batch))
		landed := err == nil
		switch {
		case err == nil:
		case errors.Is(err, errInjected):
			landed = !notLanding // the hook that fired was the one after the commit
			if landed {
				cov.landedFailures++
			}
		default:
			fatalf("Write: %v", err)
		}
		if landed {
			if err := ref.Write(bg, cloneAll(batch)); err != nil {
				fatalf("the reference refused a batch the store took: %v", err)
			}
		}
		logf("write %d records up to seq %d, landed %v, error %v", len(batch), seq, landed, err)
		// The map is checked before anything else, a store whose commit failed
		// included: the prefixes a commit put keys in are known, whatever it reported.
		mustCheck("after a write")
		fail = wmFailures{}
		if err != nil && failedStore(s) {
			reopen()
			waitScanned(t, s)
			mustCheck("after the reopening")
		}
	}
	hook := func() {
		c := horizon - 2 + rng.Intn(65)
		i := rng.Intn(min(known+2, pods))
		var err error
		switch which := rng.Intn(3); which {
		case 1:
			err = s.Instrument().CheckpointEdges(layerOfPod(i), wmNode(rng.Intn(3)), store.Reverse, at(c))
		default:
			if i >= known {
				cov.hookIntoNew++ // a pod nothing was written for
			}
			if which == 0 {
				err = s.Instrument().CheckpointEdges(layerOfPod(i), wmPod(i), store.Forward, at(c))
			} else {
				err = s.Instrument().CheckpointEntity(catalog.L2, wmPod(i), at(c))
			}
		}
		if err != nil && !errors.Is(err, store.ErrInvalid) && !errors.Is(err, errInjected) {
			fatalf("checkpoint: %v", err)
		}
		logf("checkpoint pod %d at %d: %v", i, c, err)
		fail = wmFailures{}
		mustCheck("after a checkpoint by the hook")
	}
	// planChunks makes the writes a background pass lets in between its chunks.
	planChunks := func() *chunkWrites {
		w := &chunkWrites{}
		for range rng.Intn(5) {
			w.slots = append(w.slots, rng.Intn(6))
			w.batches = append(w.batches, batchOf(rng.Intn(3) == 0))
		}
		slices.Sort(w.slots)
		w.write = func(batch []store.Record) {
			// On the retainer's goroutine: nothing here may fail the test itself.
			s.mu.Lock()
			if p := s.pass; p != nil {
				for _, r := range batch {
					sides, _ := s.sidesOf(r)
					for _, sd := range sides {
						if p.owes(sd.prefix) {
							cov.owed++
						} else if bytes.Compare(sd.prefix, p.marker.resume) < 0 {
							cov.passed++
						}
					}
				}
			}
			s.mu.Unlock()
			if err := s.Write(bg, cloneAll(batch)); err != nil {
				errs.add(fmt.Errorf("a write between chunks: %w", err))
				return
			}
			errs.add(ref.Write(bg, cloneAll(batch)))
			errs.add(check("after a write between chunks"))
		}
		return w
	}
	// retainTo moves the horizon. A retention that works the states out (the policy
	// is on and the database holds a checkpoint) leaves only states that are read.
	retainTo := func(h int) {
		horizon, retained = h, true
		cov.retentions++
		s.mu.Lock()
		derives := s.ckpt.On && s.anyCkpt
		s.mu.Unlock()
		if derives {
			unread = false
		}
		if err := s.Retain(bg, at(horizon)); err != nil {
			fatalf("Retain: %v", err)
		}
		if err := ref.Retain(bg, at(horizon)); err != nil {
			fatalf("the reference's Retain: %v", err)
		}
		logf("retain %d (seq %d)", horizon, seq)
	}
	// duringScan acts while a scan is held at pos.
	duringScan := func(pos scanPos) {
		cov.paused++
		logf("scan held at point %d after %d prefixes", pos.at, pos.hops)
		for range 1 + rng.Intn(3) {
			batch := batchOf(true)
			before := held()
			for _, p := range puts(batch) {
				if _, ok := before[string(p)]; ok || pos.at == scanStarting {
					continue
				}
				// A prefix created after the scan's iterator was opened, which the scan
				// cannot see: before or after where the scan has got to.
				cov.createdDuringScan++
				if pos.last != nil && bytes.Compare(p, pos.last) < 0 {
					cov.behindScan++
				} else {
					cov.aheadScan++
				}
			}
			write(batch)
		}
		if useHook && rng.Intn(3) == 0 {
			hook()
		}
		if rng.Intn(4) == 0 {
			// A retention while the scan is held. A background pass then runs with no
			// writes between its chunks; the caller waits for it.
			cov.retainedDuringScan++
			retainTo(horizon + 1 + rng.Intn(10))
			mustCheck("after a retention during a scan")
		}
	}
	// pausePoint is where a scan is held: a point, and for scanHopped a hop.
	pausePoint := func() (scanPoint, int) {
		return []scanPoint{scanStarting, scanOpened, scanHopped, scanHopped, scanBuilt}[rng.Intn(5)], rng.Intn(12)
	}

	for step := range 100 {
		switch op := rng.Intn(20); {
		case op < 11:
			if rng.Intn(20) == 0 {
				fail.recordBefore = true
			} else if rng.Intn(15) == 0 {
				fail.recordAfter = true
			} else if rng.Intn(12) == 0 {
				fail.ckptBefore = rng.Intn(2) == 0
				fail.ckptAfter = !fail.ckptBefore
			}
			write(batchOf(false))
		case op < 13:
			if useHook {
				if rng.Intn(10) == 0 {
					fail.ckptAfter = true
				}
				hook()
			}
		case op < 17:
			nh := horizon + rng.Intn(20)
			if retained && nh <= horizon {
				continue
			}
			armed := rng.Intn(3) == 0
			if armed {
				pause.arm(pausePoint())
			}
			if mode == Background && !armed && rng.Intn(6) == 0 {
				// Closed with the pass pending: the next Open resumes it, and its scan comes
				// when its rewriting is over.
				retainTo(nh)
				reopen()
				cov.reopenedPending++
				waitRetained(t, s)
				waitScanned(t, s)
				mustCheck("after a pass resumed by Open")
				continue
			}
			var w *chunkWrites
			if mode == Background {
				horizon = nh // the writes between the chunks are at or after it
				w = planChunks()
				chunks.Store(w)
			}
			retainTo(nh)
			if w != nil {
				waitRetained(t, s)
				chunks.Store(nil)
				errs.check(t)
				w.rest()
				errs.check(t)
			}
			mustCheck("after a retention")
			if armed {
				if pos, ok := pause.await(t, s); ok {
					duringScan(pos)
					pause.resume()
				}
			}
			waitRetained(t, s)
			waitScanned(t, s)
			mustCheck("after the scan that follows a retention")
		default:
			r := rng.Intn(3)
			if r > 0 {
				pause.arm(pausePoint())
			}
			reopen()
			if r > 0 {
				if pos, ok := pause.await(t, s); ok {
					if r == 1 {
						duringScan(pos)
						pause.resume()
					} else {
						done := make(chan error, 1)
						go func() { done <- s.Close() }()
						pause.resume()
						if err := <-done; err != nil {
							fatalf("Close during a scan: %v", err)
						}
						cov.closedDuringScan++
						if s.scanning() {
							fatalf("Close returned with the scan still running")
						}
						if complete, installed := mapState(s); complete || installed {
							fatalf("a store closed during its first scan has a filter (%v) or a complete map (%v)", installed, complete)
						}
						open()
						logf("closed during the scan, and opened again")
					}
				}
			}
			waitRetained(t, s) // a retention while the scan was held
			waitScanned(t, s)
			mustCheck("after the scan of an opening")
		}
		if step%8 == 7 {
			var fps []identity.Fingerprint
			for i := range known {
				fps = append(fps, wmPod(i))
			}
			for j := range 3 {
				fps = append(fps, wmNode(j))
			}
			h := at(horizon)
			if err := asyncAnswers(s, ref, fps, []time.Time{h, h.Add(1), h.Add(17), h.Add(40), h.Add(80)}, []uint64{store.Latest, s.LastSeq()}); err != nil {
				fatalf("step %d: %v", step, err)
			}
		}
	}
	c := rec.counts()
	cov.touchRewrites = c["retain.touch_rewrites"]
	cov.cold = c["checkpoint.loads_cold"]
	cov.evictions = c["writer_map.lru_evictions"]
	return cov
}

// ---------------------------------------------------------------------------
// Section 3.3: the races, each held at the points where they meet.

// populate writes edges of the pods given, in three rounds, so that the policy of
// one record between checkpoints writes some.
func (w *wmWorld) populate(pods []int, from int) {
	w.t.Helper()
	for round := range 3 {
		w.edges(sec(from+10*round), pods...)
	}
}

// Before its first scan has installed a filter, a store opened over data is not
// complete: a write reads every prefix it meets, counted as a cold load, and so
// finds the checkpoints a late record makes untrue.
func TestTheMapIsNotCompleteBeforeTheFirstScan(t *testing.T) {
	t.Parallel()
	fs := vfs.NewMem()
	pause := newScanPause()
	rec := &countRecorder{}
	o := Options{Recorder: rec, Checkpoints: policy(stress(0)), scanHook: pause.hook}
	s := wmOpen(t, fs, o)
	w := newWMWorld(t, s)
	w.populate([]int{0, 1, 2, 3, 4, 5}, 10)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	pause.arm(scanStarting, 0)
	s = wmOpen(t, fs, o)
	w.s = s
	if _, ok := pause.await(t, s); !ok {
		t.Fatal("opening a database that holds data started no scan of its prefixes")
	}
	if complete, installed := mapState(s); complete || installed {
		t.Fatalf("before its first scan the map is complete (%v) or has a filter (%v)", complete, installed)
	}
	before := rec.counts()
	// A late record before every checkpoint of pod 0's prefixes.
	w.seq++
	w.write(wmEdge(w.seq, 0, 0, sec(5), lifecycle.Delete))
	after := rec.counts()
	if cold := after["checkpoint.loads_cold"] - before["checkpoint.loads_cold"]; cold != 2 {
		t.Errorf("the write read %d of the 2 prefixes it met cold; want both", cold)
	}
	if n := after["checkpoint.loads"] - before["checkpoint.loads"]; n != 0 {
		t.Errorf("a read before the first scan was counted as %d loads", n)
	}
	if err := s.Instrument().VerifyCheckpoints(); err != nil {
		t.Fatal(err)
	}
	w.same(upTo(6), sec(4), sec(15), sec(27), sec(40))
	pause.resume()
	waitScanned(t, s)
	if complete, installed := mapState(s); !complete || !installed {
		t.Fatalf("after the first scan the map is complete = %v, with a filter = %v", complete, installed)
	}
	requireWriterMap(t, s)
}

// A prefix written before a scan, after the filter it is not in, stays known while
// the scan runs, until the scan's own filter holds it; and a prefix written during
// the scan, behind or ahead of it, or between the start of its delta and the opening
// of its iterator, is in a live delta. The scan is held at each of its points, after
// an opening and after a retention, in both modes; while it is held new prefixes and
// late records are written, and the map is checked then and after.
func TestPrefixesWrittenAroundAScanAreKnown(t *testing.T) {
	t.Parallel()
	points := []struct {
		name string
		at   scanPoint
		hop  int
	}{
		{"starting", scanStarting, 0}, {"opened", scanOpened, 0}, {"after 9 prefixes", scanHopped, 9}, {"built", scanBuilt, 0},
	}
	for _, mode := range bothModes {
		for _, trigger := range []string{"opening", "retention"} {
			for _, pt := range points {
				t.Run(fmt.Sprintf("%s/%s/%s", modeName(mode), trigger, pt.name), func(t *testing.T) {
					t.Parallel()
					fs := vfs.NewMem()
					pause := newScanPause()
					o := Options{Checkpoints: policy(stress(0)), Retention: mode, scanHook: pause.hook}
					s := wmOpen(t, fs, o)
					w := newWMWorld(t, s)
					// The pods 8k first; the pods 8k+4, whose edges are in the same layer and
					// sort between theirs, are new later, behind and ahead of where the scan is
					// held.
					var old, fresh []int
					for k := range 30 {
						old = append(old, 8*k)
					}
					for k := range 15 {
						fresh = append(fresh, 8*k+4)
					}
					w.populate(old[:15], 10)
					if trigger == "opening" {
						if err := s.Close(); err != nil {
							t.Fatal(err)
						}
						pause.arm(pt.at, pt.hop)
						s = wmOpen(t, fs, o)
						w.s = s
					} else {
						// A scan installs a filter of the prefixes so far; the next prefixes are
						// written after its scan began, so the filter does not hold them, and the
						// retention's scan is the one held.
						if err := s.Close(); err != nil {
							t.Fatal(err)
						}
						s = wmOpen(t, fs, o)
						w.s = s
						waitScanned(t, s)
						w.populate(old[15:], 40)
						requireWriterMap(t, s)
						pause.arm(pt.at, pt.hop)
						w.retain(sec(15))
					}
					pos, ok := pause.await(t, s)
					if !ok {
						t.Fatal("no scan of the stored prefixes followed")
					}
					// With the scan held, the prefixes written after the last filter's scan
					// began are known by their delta alone.
					requireWriterMap(t, s)
					var behind, ahead int
					for _, i := range fresh {
						p := prefixOfEdge(t, s, i)
						if pos.last != nil && bytes.Compare(p, pos.last) < 0 {
							behind++
						} else {
							ahead++
						}
					}
					w.edges(sec(60), fresh...)
					w.seq++
					w.write(wmEdge(w.seq, old[0], 0, sec(16), lifecycle.Delete)) // late, in a prefix the scan saw
					requireWriterMap(t, s)
					for _, i := range fresh {
						if complete, installed := mapState(s); installed && !covered(s, prefixOfEdge(t, s, i)) {
							t.Fatalf("pod %d's prefix, written while the scan was held, is not known (complete %v)", i, complete)
						}
					}
					pause.resume()
					waitScanned(t, s)
					if complete, installed := mapState(s); !complete || !installed {
						t.Fatalf("after the scan the map is complete = %v, with a filter = %v", complete, installed)
					}
					requireWriterMap(t, s)
					// A late record in a prefix the scan did not see: it is known, and what it
					// holds is read.
					w.seq++
					w.write(wmEdge(w.seq, fresh[0], 0, sec(55), lifecycle.Delete))
					requireWriterMap(t, s)
					if err := s.Instrument().VerifyCheckpoints(); err != nil {
						t.Fatal(err)
					}
					w.same(append(slices.Clone(old), fresh...), sec(16), sec(33), sec(58), sec(70))
					if pt.at == scanHopped && (behind == 0 || ahead == 0) {
						t.Errorf("the new prefixes were %d behind the held scan and %d ahead: the case does not exercise both", behind, ahead)
					}
				})
			}
		}
	}
}

// A retention while a scan is held: the pass runs (in the background, with the old
// scan still held), the scan that follows it installs, and nothing that holds keys is
// unknown at any point.
func TestARetentionDuringAScan(t *testing.T) {
	t.Parallel()
	for _, mode := range bothModes {
		for _, pt := range []scanPoint{scanOpened, scanHopped, scanBuilt} {
			t.Run(fmt.Sprintf("%s/point %d", modeName(mode), pt), func(t *testing.T) {
				t.Parallel()
				fs := vfs.NewMem()
				pause := newScanPause()
				o := Options{Checkpoints: policy(stress(0)), Retention: mode, scanHook: pause.hook}
				s := wmOpen(t, fs, o)
				w := newWMWorld(t, s)
				pods := []int{0, 2, 4, 6, 8, 10, 12, 14, 16, 18}
				w.populate(pods, 10)
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
				pause.arm(pt, 3)
				s = wmOpen(t, fs, o)
				w.s = s
				if _, ok := pause.await(t, s); !ok {
					t.Fatal("opening a database that holds data started no scan of its prefixes")
				}
				w.edges(sec(30), 1, 3, 5)
				w.retain(sec(25)) // in the background, its pass and its own scan run while the old scan is held
				requireWriterMap(t, s)
				w.edges(sec(40), 7, 9, 11)
				requireWriterMap(t, s)
				pause.resume()
				waitScanned(t, s)
				if complete, installed := mapState(s); !complete || !installed {
					t.Fatalf("after the retention's scan the map is complete = %v, with a filter = %v", complete, installed)
				}
				requireWriterMap(t, s)
				w.seq++
				w.write(wmEdge(w.seq, 7, 1, sec(35), lifecycle.Delete))
				if err := s.Instrument().VerifyCheckpoints(); err != nil {
					t.Fatal(err)
				}
				w.same(upTo(20), sec(25), sec(32), sec(45))
			})
		}
	}
}

// In the background the scan that follows a pass starts when the rewriting is over
// and runs beside the settle, which waits for no scan: the scan is held through the
// whole settle, and writes go on meanwhile.
func TestAScanRunsBesideABackgroundSettle(t *testing.T) {
	t.Parallel()
	fs := vfs.NewMem()
	pause := newScanPause()
	o := settleOptions(fs, policy(stress(0)))
	o.scanHook = pause.hook
	var s atomic.Pointer[Store]
	var during, ranges atomic.Int64
	o.betweenSettleRanges = func(int) {
		ranges.Add(1)
		if st := s.Load(); st != nil && st.scanning() {
			during.Add(1)
		}
	}
	st := mustOpen(t, o)
	s.Store(st)
	w := newWMWorld(t, st)
	w.populate([]int{0, 1, 2, 3, 4, 5, 6, 7}, 10)
	pause.arm(scanOpened, 0)
	w.retain(sec(15)) // returns when the settle has ended, with the scan still held
	pos, ok := pause.await(t, st)
	if !ok || pos.at != scanOpened {
		t.Fatalf("the pass started no scan when its rewriting ended (held %v at %d)", ok, pos.at)
	}
	if ranges.Load() == 0 || during.Load() != ranges.Load() {
		t.Fatalf("the scan was running at %d of the %d settled ranges; want all", during.Load(), ranges.Load())
	}
	w.edges(sec(40), 8, 9, 10)
	requireWriterMap(t, st)
	pause.resume()
	waitScanned(t, st)
	if complete, installed := mapState(st); !complete || !installed {
		t.Fatalf("after the scan the map is complete = %v, with a filter = %v", complete, installed)
	}
	requireWriterMap(t, st)
	w.same(upTo(11), sec(15), sec(35), sec(45))
}

// Close stops a scan and waits for it to exit before it closes the database: a
// scan's iterator must not outlive the database. A Close that returned while the
// scan is held would be seen here: the scan is held for a while after Close is
// called, and only then let go. The map stays incomplete.
func TestCloseWaitsForAScan(t *testing.T) {
	t.Parallel()
	for _, mode := range bothModes {
		for _, pt := range []scanPoint{scanStarting, scanOpened, scanHopped, scanBuilt} {
			t.Run(fmt.Sprintf("%s/point %d", modeName(mode), pt), func(t *testing.T) {
				t.Parallel()
				fs := vfs.NewMem()
				pause := newScanPause()
				o := Options{Checkpoints: policy(stress(0)), Retention: mode, scanHook: pause.hook}
				s := wmOpen(t, fs, o)
				w := newWMWorld(t, s)
				w.populate([]int{0, 1, 2, 3, 4, 5}, 10)
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
				pause.arm(pt, 2)
				s = wmOpen(t, fs, o)
				if _, ok := pause.await(t, s); !ok {
					t.Fatal("opening a database that holds data started no scan of its prefixes")
				}
				closed := make(chan error, 1)
				go func() { closed <- s.Close() }()
				select {
				case err := <-closed:
					pause.abandon()
					t.Fatalf("Close returned (%v) while the scan was held", err)
				case <-time.After(200 * time.Millisecond):
				}
				pause.resume()
				if err := <-closed; err != nil {
					t.Fatal(err)
				}
				if s.scanning() {
					t.Fatal("Close returned with the scan still running")
				}
				if complete, installed := mapState(s); complete || installed {
					t.Fatalf("a store closed during its first scan has a filter (%v) or a complete map (%v)", installed, complete)
				}
			})
		}
	}
}

// The scans race the writes and the retentions with nothing held: a store opened over
// many prefixes takes writes of new prefixes, and retentions, while its scan runs. For
// the race detector; the map is checked when the scans have ended.
func TestAScanRacesWritesAndRetentions(t *testing.T) {
	t.Parallel()
	for _, mode := range bothModes {
		t.Run(modeName(mode), func(t *testing.T) {
			t.Parallel()
			fs := vfs.NewMem()
			o := Options{Checkpoints: policy(CheckpointOptions{On: true, KMin: 2}), Retention: mode}
			s := wmOpen(t, fs, o)
			w := newWMWorld(t, s)
			var many []int
			for i := 0; i < 240; i += 2 {
				many = append(many, i)
			}
			w.populate(many, 10)
			for round := range 3 {
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
				s = wmOpen(t, fs, o)
				w.s = s
				for k := range 10 {
					w.edges(sec(50+10*round+k), 1+2*(k+10*round), 1+2*(k+10*round)+60)
				}
				w.retain(sec(20 + 10*round))
				w.edges(sec(90+round), 3, 5, 7)
				waitScanned(t, s)
				requireWriterMap(t, s)
			}
			w.same(upTo(240), sec(50), sec(95), sec(120))
		})
	}
}

// ---------------------------------------------------------------------------
// The delta of a write that fails, and of the instrument's hook.

// A commit that reports failure may have landed: the prefixes it puts keys in are
// in the delta before it, whatever it reports, and the failure ends completeness as
// well. Both are needed, and each is checked. The hook's checkpoint is placed in a
// prefix that holds nothing yet, which only its delta makes known; when its commit
// succeeds, a late record in that prefix must find it and delete it.
func TestAFailedCommitThatLandedLeavesItsPrefixesKnown(t *testing.T) {
	t.Parallel()
	setup := func(t *testing.T) (*wmWorld, *wmFailures) {
		f := &wmFailures{}
		o := Options{Checkpoints: policy(stress(0))}
		f.hooks(&o)
		s := wmOpen(t, vfs.NewMem(), o)
		w := newWMWorld(t, s)
		w.populate([]int{0, 1, 2}, 10) // checkpoints, so that a write would read a prefix it may find them in
		if complete, installed := mapState(s); !complete || !installed {
			t.Fatalf("a store opened empty is complete = %v, with a filter = %v", complete, installed)
		}
		return w, f
	}
	t.Run("a record commit", func(t *testing.T) {
		t.Parallel()
		w, f := setup(t)
		f.recordAfter = true
		w.seq++
		if err := w.s.Write(bg, []store.Record{wmEdge(w.seq, 7, 0, sec(50), lifecycle.Observe)}); !errors.Is(err, errInjected) {
			t.Fatalf("the write returned %v", err)
		}
		sides, err := w.s.sidesOf(wmEdge(w.seq, 7, 0, sec(50), lifecycle.Observe))
		if err != nil {
			t.Fatal(err)
		}
		for _, sd := range sides {
			if !covered(w.s, sd.prefix) {
				t.Errorf("the prefix %x of a commit that landed and reported failure is not known", sd.prefix)
			}
		}
		if complete, _ := mapState(w.s); complete {
			t.Error("the map is complete after a commit that reported failure")
		}
		requireWriterMap(t, w.s)
	})
	t.Run("a hook commit that landed and failed", func(t *testing.T) {
		t.Parallel()
		w, f := setup(t)
		f.ckptAfter = true
		if err := w.s.Instrument().CheckpointEntity(catalog.L2, wmPod(9), sec(40)); !errors.Is(err, errInjected) {
			t.Fatalf("the hook returned %v", err)
		}
		if !covered(w.s, entityPrefix(t, w.s, wmPod(9))) {
			t.Error("the prefix of a hook commit that landed and reported failure is not known")
		}
		if complete, _ := mapState(w.s); complete {
			t.Error("the map is complete after a hook commit that reported failure")
		}
		requireWriterMap(t, w.s)
		w.seq++
		w.write(wmEntity(w.seq, 9, sec(30))) // before the checkpoint, which no longer holds
		if err := w.s.Instrument().VerifyCheckpoints(); err != nil {
			t.Fatal(err)
		}
		requireWriterMap(t, w.s)
	})
	t.Run("a hook commit into a new prefix", func(t *testing.T) {
		t.Parallel()
		w, _ := setup(t)
		if err := w.s.Instrument().CheckpointEntity(catalog.L2, wmPod(9), sec(40)); err != nil {
			t.Fatal(err)
		}
		if !covered(w.s, entityPrefix(t, w.s, wmPod(9))) {
			t.Error("the prefix the hook placed a checkpoint in is not known")
		}
		// Forget what the writer keeps, as an eviction would: only the delta says the
		// prefix holds a checkpoint now.
		w.s.mu.Lock()
		forgetCached(w.s, entityPrefix(t, w.s, wmPod(9)))
		w.s.mu.Unlock()
		w.seq++
		w.write(wmEntity(w.seq, 9, sec(30)))
		if err := w.s.Instrument().VerifyCheckpoints(); err != nil {
			t.Fatal(err)
		}
		requireWriterMap(t, w.s)
		w.same(upTo(10), sec(29), sec(35), sec(45))
	})
}

// ---------------------------------------------------------------------------
// What the map reads, and what it keeps.

// A complete map reads no prefix it can answer without: one that is in neither the
// filter nor a delta is taken for empty with no read. Only the filter's false
// positives (about one key in 256 that was not built in) are read, and the filter is
// deterministic, so the count is fixed.
func TestACompleteMapReadsNoNewPrefix(t *testing.T) {
	t.Parallel()
	fs := vfs.NewMem()
	rec := &countRecorder{}
	o := Options{Recorder: rec, Checkpoints: policy(CheckpointOptions{On: true, KMin: 1000})}
	s := wmOpen(t, fs, o)
	w := newWMWorld(t, s)
	var old []int
	for i := range 120 {
		old = append(old, 2*i)
	}
	w.edges(sec(10), old...)
	if err := s.Instrument().CheckpointEdges(layerOfPod(0), wmPod(0), store.Forward, sec(11)); err != nil {
		t.Fatal(err) // so that the database may hold a checkpoint, and a prefix may need reading
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = wmOpen(t, fs, o)
	w.s = s
	waitScanned(t, s)
	before := rec.counts()
	var fresh []int
	for i := range 60 {
		fresh = append(fresh, 2*i+1) // in layers 1 and 3, which hold nothing yet
	}
	w.edges(sec(20), fresh...)
	after := rec.counts()
	reads := func(name string) int64 { return after[name] - before[name] }
	if n := reads("checkpoint.loads_cold"); n != 0 {
		t.Errorf("%d cold reads after the scan", n)
	}
	// 60 new forward prefixes and 6 new reverse ones, of the three nodes in two
	// layers: none is read but the filter's false positives.
	if n := reads("checkpoint.loads"); n != 0 {
		t.Errorf("%d prefixes read and found to hold keys; want none", n)
	}
	if n := reads("writer_map.false_positive_reads"); n > 3 {
		t.Errorf("%d of the 66 new prefixes were read; want only the filter's false positives", n)
	}
	if complete, installed := mapState(s); !complete || !installed {
		t.Fatalf("the map is complete = %v, with a filter = %v", complete, installed)
	}
	requireWriterMap(t, s)
}

// With the filter made to answer "maybe" for every prefix, a read of a new prefix
// finds nothing: it is a false positive, not a load.
func TestAFalsePositiveIsNotALoad(t *testing.T) {
	t.Parallel()
	fs := vfs.NewMem()
	rec := &countRecorder{}
	o := Options{Recorder: rec, Checkpoints: policy(CheckpointOptions{On: true, KMin: 1000})}
	s := wmOpen(t, fs, o)
	w := newWMWorld(t, s)
	w.edges(sec(10), 0, 2, 4, 6, 8)
	if err := s.Instrument().CheckpointEdges(layerOfPod(0), wmPod(0), store.Forward, sec(11)); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = wmOpen(t, fs, o)
	w.s = s
	waitScanned(t, s)
	s.Instrument().AssumeEveryPrefixMayExist()
	before := rec.counts()
	w.edges(sec(20), 1, 3, 5, 7, 9, 11, 13, 15, 17, 19) // 10 new forward prefixes and 6 new reverse ones
	w.edges(sec(21), 0, 2)                              // 2 old forward prefixes and 2 old reverse ones
	after := rec.counts()
	reads := func(name string) int64 { return after[name] - before[name] }
	if n := reads("writer_map.false_positive_reads"); n != 16 {
		t.Errorf("%d false positive reads; want the 16 new prefixes", n)
	}
	if n := reads("checkpoint.loads"); n != 4 {
		t.Errorf("%d loads; want the 4 old prefixes", n)
	}
	requireWriterMap(t, s)
}

// The empty state of a prefix a complete map knows to be empty is used, not kept.
func TestACertainMissIsNotCached(t *testing.T) {
	t.Parallel()
	rec := &countRecorder{}
	s := wmOpen(t, vfs.NewMem(), Options{Recorder: rec, Checkpoints: policy(stress(0))})
	w := newWMWorld(t, s)
	w.edges(sec(10), 0, 1, 2)
	if complete, installed := mapState(s); !complete || !installed {
		t.Fatalf("the map is complete = %v, with a filter = %v", complete, installed)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	n, bytesBefore := len(cachedStates(s)), rec.counts()["writer_map.lru_bytes"]
	prefix := prefixOfEdge(t, s, 50)
	st, err := s.state(prefix)
	if err != nil {
		t.Fatal(err)
	}
	if !st.empty() {
		t.Fatalf("a new prefix's state is %+v", *st)
	}
	if _, kept := cachedStates(s)[string(prefix)]; kept || len(cachedStates(s)) != n {
		t.Fatalf("the empty state of a prefix known to be empty was kept (%d states, %d before)", len(cachedStates(s)), n)
	}
	if b := rec.counts()["writer_map.lru_bytes"]; b != bytesBefore || b == 0 {
		t.Fatalf("the cache's bytes went from %d to %d", bytesBefore, b)
	}
}

// A state a Write has touched is kept until the Write returns, whatever the budget:
// with room for one state, a batch that puts several records in each of several
// prefixes stores what a store with room for every state stores, byte for byte, and
// counts the same checkpoints.
func TestAStateTheWriteTouchesIsNotEvictedDuringIt(t *testing.T) {
	t.Parallel()
	recs := []*countRecorder{{}, {}}
	var sides []*Store
	for i, n := range []int{1, 0} {
		s := wmOpen(t, vfs.NewMem(), Options{Recorder: recs[i], Checkpoints: policy(stress(0))})
		s.Instrument().CapCachedStates(n)
		sides = append(sides, s)
	}
	var seq uint64
	for round := range 8 {
		var batch []store.Record
		for k := range 6 {
			seq++
			// Pods 0 to 2 twice each, to nodes 0 to 2; a late one in every other round.
			at := sec(10 + 10*round + k)
			if round%2 == 1 && k == 3 {
				at = sec(10*round - 3)
			}
			batch = append(batch, wmEdge(seq, k%3, (k+round)%3, at, lifecycle.Observe))
		}
		for _, s := range sides {
			if err := s.Write(bg, cloneAll(batch)); err != nil {
				t.Fatal(err)
			}
		}
		if a, b := dataBytes(t, sides[0]), dataBytes(t, sides[1]); !slices.Equal(a, b) {
			t.Fatalf("round %d: the store that keeps one state stored other bytes than the one that keeps all", round)
		}
		requireWriterMap(t, sides[0])
	}
	a, b := recs[0].counts(), recs[1].counts()
	if a["checkpoint.written"] != b["checkpoint.written"] || a["checkpoint.invalidated"] != b["checkpoint.invalidated"] {
		t.Fatalf("checkpoints written %d and invalidated %d with one state kept; %d and %d with all", a["checkpoint.written"], a["checkpoint.invalidated"], b["checkpoint.written"], b["checkpoint.invalidated"])
	}
	if a["writer_map.lru_evictions"] == 0 || b["writer_map.lru_evictions"] != 0 {
		t.Fatalf("evictions: %d with one state kept, %d with all; want some and none", a["writer_map.lru_evictions"], b["writer_map.lru_evictions"])
	}
}

// A retention puts the states it works out only into the prefixes the cache already
// holds: it neither grows the cache past its budget nor evicts to make room, and the
// prefixes it holds after are among those it held before.
func TestARetentionInstallsOnlyCachedStates(t *testing.T) {
	t.Parallel()
	for _, mode := range bothModes {
		for _, limit := range []string{"four states", "4 KiB"} {
			t.Run(modeName(mode)+"/"+limit, func(t *testing.T) {
				t.Parallel()
				rec := &countRecorder{}
				o := Options{Recorder: rec, Checkpoints: policy(stress(0)), Retention: mode}
				if limit == "4 KiB" {
					o.StateCacheBytes = 4 << 10
				}
				s := wmOpen(t, vfs.NewMem(), o)
				if limit == "four states" {
					s.Instrument().CapCachedStates(4)
				}
				w := newWMWorld(t, s)
				var pods []int
				for i := range 40 {
					pods = append(pods, i)
				}
				w.populate(pods, 10)
				s.mu.Lock()
				before := maps.Clone(cachedStates(s))
				s.mu.Unlock()
				evicted := rec.counts()["writer_map.lru_evictions"]
				w.retain(sec(25))
				waitScanned(t, s)
				s.mu.Lock()
				after := maps.Clone(cachedStates(s))
				s.mu.Unlock()
				for k := range after {
					if _, ok := before[k]; !ok {
						t.Fatalf("the retention put the state of %x, which was not kept, into the cache", k)
					}
				}
				c := rec.counts()
				if c["writer_map.lru_evictions"] != evicted {
					t.Fatalf("the retention evicted %d states", c["writer_map.lru_evictions"]-evicted)
				}
				switch limit {
				case "four states":
					if len(after) > 4 || len(before) == 0 {
						t.Fatalf("%d states kept before the retention, %d after; the cap is 4", len(before), len(after))
					}
				default:
					if b := c["writer_map.lru_bytes"]; b <= 0 || b > 4<<10 {
						t.Fatalf("the cache holds %d bytes; its budget is %d", b, 4<<10)
					}
				}
				if c["retain.state_keys"] == 0 {
					t.Fatal("the retention worked no state out, so the case shows nothing")
				}
				requireWriterMap(t, s)
				w.same(upTo(40), sec(25), sec(31))
			})
		}
	}
}

// The counts of the writer map are signed, so that their sum over a measurement is
// the current size: the filter's bytes, the prefixes in live deltas and the bytes of
// the states kept; and they return to zero when the store is closed.
func TestTheWriterMapCounters(t *testing.T) {
	t.Parallel()
	fs := vfs.NewMem()
	rec := &countRecorder{}
	o := Options{Recorder: rec, Checkpoints: policy(stress(0))}
	s := wmOpen(t, fs, o)
	w := newWMWorld(t, s)
	w.edges(sec(10), 0, 1, 2, 3, 4) // 5 forward prefixes and 5 reverse ones, in the delta of the opening
	c := rec.counts()
	if c["writer_map.delta_prefixes"] != 10 || c["writer_map.lru_bytes"] <= 0 {
		t.Fatalf("after writing 10 prefixes: %d in deltas, %d bytes of states", c["writer_map.delta_prefixes"], c["writer_map.lru_bytes"])
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	c = rec.counts()
	for _, n := range []string{"writer_map.delta_prefixes", "writer_map.lru_bytes", "writer_map.filter_bytes"} {
		if c[n] != 0 {
			t.Errorf("%s is %d after Close", n, c[n])
		}
	}
	s = wmOpen(t, fs, o)
	w.s = s
	waitScanned(t, s)
	c = rec.counts()
	if c["writer_map.filter_bytes"] <= 0 || c["writer_map.delta_prefixes"] != 0 || c["writer_map.scan_ns"] <= 0 {
		t.Fatalf("after the scan: %d bytes of filter, %d prefixes in deltas, %d ns scanning", c["writer_map.filter_bytes"], c["writer_map.delta_prefixes"], c["writer_map.scan_ns"])
	}
	w.edges(sec(20), 5, 6) // 2 new forward prefixes and 2 new reverse ones
	if c = rec.counts(); c["writer_map.delta_prefixes"] != 4 {
		t.Fatalf("after writing 4 prefixes, %d prefixes in deltas", c["writer_map.delta_prefixes"])
	}
}

func TestStateCacheBytesMustNotBeNegative(t *testing.T) {
	t.Parallel()
	_, err := Open("db", Options{Config: pebblekv.Config{Tuning: pebblekv.TinyTuning(), FS: vfs.NewMem()}, StateCacheBytes: -1})
	if !errors.Is(err, store.ErrInvalid) {
		t.Fatalf("Open with a negative state cache returned %v", err)
	}
}

// A checkpoint the instrument's hook places in a prefix that holds no record is in
// the delta: a complete map does not take that prefix for empty, even when it keeps
// no state of it, and a late record in it deletes the checkpoint.
func TestAHookCheckpointInAPrefixWithNoRecordsIsKnown(t *testing.T) {
	t.Parallel()
	rec := &countRecorder{}
	s := wmOpen(t, vfs.NewMem(), Options{Recorder: rec, Checkpoints: policy(CheckpointOptions{On: true, KMin: 1000})})
	w := newWMWorld(t, s)
	w.edges(sec(10), 0, 2, 4)
	if err := s.Instrument().CheckpointEdges(layerOfPod(9), wmPod(9), store.Forward, sec(30)); err != nil {
		t.Fatal(err)
	}
	prefix := prefixOfEdge(t, s, 9)
	if got := checkpointsOnDisk(t, s)[string(prefix)]; len(got) != 1 {
		t.Fatalf("the prefix holds the checkpoints %v", got)
	}
	s.mu.Lock()
	forgetCached(s, prefix)
	complete := s.complete
	before := rec.counts()["checkpoint.loads"]
	st, err := s.state(prefix)
	s.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if !complete {
		t.Fatal("the map is not complete, so the case shows nothing")
	}
	if !slices.Equal(st.ckpts, []int64{sec(30).UnixNano()}) || rec.counts()["checkpoint.loads"] != before+1 {
		t.Fatalf("a complete map took the prefix of the hook's checkpoint for %+v (loads %d, were %d)", *st, rec.counts()["checkpoint.loads"], before)
	}
	s.mu.Lock()
	forgetCached(s, prefix)
	s.mu.Unlock()
	w.seq++
	w.write(wmEdge(w.seq, 9, 0, sec(20), lifecycle.Observe)) // before the checkpoint, which it makes untrue
	if got := checkpointsOnDisk(t, s)[string(prefix)]; len(got) != 0 {
		t.Fatalf("the late record left the checkpoints %v", got)
	}
	if err := s.Instrument().VerifyCheckpoints(); err != nil {
		t.Fatal(err)
	}
	requireWriterMap(t, s)
	w.same([]int{0, 2, 4, 9}, sec(15), sec(25), sec(35))
}

// A retention makes stale the states the writer keeps of the layers it rewrites: a
// write to such a prefix before the retention works its state out reads it (a
// load), and what it then keeps is what a whole read finds. Directly, and between
// the chunks of a background pass, where the write rewrites the prefix first.
func TestAWriteToAStaleStateReadsIt(t *testing.T) {
	t.Parallel()
	t.Run("forgotten layers", func(t *testing.T) {
		t.Parallel()
		rec := &countRecorder{}
		s := wmOpen(t, vfs.NewMem(), Options{Recorder: rec, Checkpoints: policy(stress(0))})
		w := newWMWorld(t, s)
		w.populate([]int{0, 1, 2, 3}, 10)
		prefix := prefixOfEdge(t, s, 1)
		s.mu.Lock()
		if s.states.peek(string(prefix)) == nil {
			s.mu.Unlock()
			t.Fatal("the writer keeps no state of the prefix")
		}
		s.forgetLayers([]retainLayer{{layer: layerOfPod(1)}})
		stale := s.states.has(string(prefix)) && s.states.peek(string(prefix)) == nil
		s.mu.Unlock()
		if !stale {
			t.Fatal("the state of a prefix of a forgotten layer is not stale")
		}
		before := rec.counts()["checkpoint.loads"]
		w.seq++
		w.write(wmEdge(w.seq, 1, 1, sec(15), lifecycle.Observe)) // late: before the checkpoints of the prefix
		if n := rec.counts()["checkpoint.loads"] - before; n != 2 {
			t.Fatalf("the write read %d prefixes; want its two, whose states were stale", n)
		}
		requireWriterMap(t, s)
		if err := s.Instrument().VerifyCheckpoints(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("between the chunks of a background pass", func(t *testing.T) {
		t.Parallel()
		rec := &countRecorder{}
		o := Options{Recorder: rec, Checkpoints: policy(stress(0)), Retention: Background}
		var s *Store
		var w *wmWorld
		errs := &asyncErrs{}
		var stale bool
		var loads int64
		calls := 0
		o.beforeRetainChunk = func() {
			if calls++; calls != 1 {
				return
			}
			// Before the first chunk: every state of the layers is stale, and pod 2's
			// prefix is owed.
			prefix := prefixOfEdge(t, s, 2)
			s.mu.Lock()
			stale = s.states.has(string(prefix)) && s.states.peek(string(prefix)) == nil && s.pass.owes(prefix)
			s.mu.Unlock()
			before := rec.counts()["checkpoint.loads"]
			r := wmEdge(w.seq+1, 2, 2, sec(26), lifecycle.Observe)
			errs.add(s.Write(bg, []store.Record{r}))
			errs.add(w.ref.Write(bg, []store.Record{r}))
			w.seq++
			loads = rec.counts()["checkpoint.loads"] - before
			s.mu.Lock()
			errs.add(writerMapMismatch(s, compareLatest))
			s.mu.Unlock()
		}
		s = wmOpen(t, vfs.NewMem(), o)
		w = newWMWorld(t, s)
		w.populate([]int{0, 1, 2, 3}, 10)
		w.retain(sec(25))
		errs.check(t)
		if !stale || loads != 2 {
			t.Fatalf("the prefix was stale and owed: %v; the write read %d prefixes, want its two", stale, loads)
		}
		waitScanned(t, s)
		requireWriterMap(t, s)
		w.same([]int{0, 1, 2, 3}, sec(25), sec(27), sec(40))
	})
}

// A scan that a publishing Retain stops installs nothing and leaves its delta live:
// a delta is dropped only when a later scan installs. The prefix written after the
// stopped scan began is known by that delta alone until the pass's own scan holds it.
func TestAScanStoppedByARetentionLeavesItsDeltaLive(t *testing.T) {
	t.Parallel()
	fs := vfs.NewMem()
	pause := newScanPause()
	gate := make(chan struct{})
	o := Options{Checkpoints: policy(stress(0)), Retention: Background, scanHook: pause.hook}
	s := wmOpen(t, fs, o)
	w := newWMWorld(t, s)
	w.populate([]int{0, 2, 4, 6, 8}, 10)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	o.beforeRetainChunk = func() { <-gate } // the pass waits, so that nothing installs meanwhile
	pause.arm(scanHopped, 2)
	s = wmOpen(t, fs, o)
	w.s = s
	if _, ok := pause.await(t, s); !ok {
		t.Fatal("opening a database that holds data started no scan of its prefixes")
	}
	w.edges(sec(40), 9) // in the delta of the held scan
	if err := s.Retain(bg, sec(15)); err != nil {
		t.Fatal(err)
	}
	if err := w.ref.Retain(bg, sec(15)); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	stopped, live := s.scan == nil, s.inFilterOrDelta(prefixOfEdge(t, s, 9))
	installed := s.filter != nil
	s.mu.Unlock()
	if !stopped || installed || !live {
		t.Fatalf("after the publication: the held scan stopped %v, a filter installed %v, the prefix in a live delta %v", stopped, installed, live)
	}
	pause.resume() // the stopped scan exits, and installs nothing
	close(gate)
	waitRetained(t, s)
	waitScanned(t, s)
	if complete, installed := mapState(s); !complete || !installed {
		t.Fatalf("after the pass's scan the map is complete = %v, with a filter = %v", complete, installed)
	}
	requireWriterMap(t, s)
	w.seq++
	w.write(wmEdge(w.seq, 9, 0, sec(30), lifecycle.Delete))
	if err := s.Instrument().VerifyCheckpoints(); err != nil {
		t.Fatal(err)
	}
	w.same([]int{0, 2, 4, 6, 8, 9}, sec(15), sec(35), sec(45))
}
