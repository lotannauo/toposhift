package pebblestore

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/store"
)

// The tests of the bound on the range deletions a retention leaves unflushed, and of
// the chunk's clock, which charges the opening of its iterator up to half the chunk
// time.

// boundPods is how many pods the stream of these tests names: each has an edge to
// its own node before the horizon and one after it, so each edge has two prefixes
// a retention rewrites.
const boundPods = 1500

// boundHorizon is where these tests retain.
var boundHorizon = asyncAt(30)

// boundPod is the i-th pod of the stream.
func boundPod(i int) identity.Fingerprint { return boundEntity(catalog.K8sPod, i) }

// boundNode is the node the i-th pod is scheduled on, its own, so that no prefix
// holds more than a few records and every chunk ends by its size after about ten.
func boundNode(i int) identity.Fingerprint { return boundEntity(catalog.K8sNode, i) }

func boundEntity(typ catalog.EntityType, i int) identity.Fingerprint {
	var h [identity.FingerprintBytes]byte
	binary.BigEndian.PutUint32(h[:4], uint32(i)+1)
	fp, err := identity.FingerprintFromHash(typ, h)
	if err != nil {
		panic(err)
	}
	return fp
}

// boundStream is the stream: every pod's edge before the horizon, then every pod's
// edge after it, in batches of 100.
func boundStream() [][]store.Record {
	var out [][]store.Record
	var batch []store.Record
	seq := uint64(0)
	for _, at := range []int{10, 50} {
		for i := range boundPods {
			seq++
			batch = append(batch, edgeAt(catalog.L0, boundPod(i), boundNode(i), asyncAt(at), seq))
			if len(batch) == 100 {
				out, batch = append(out, batch), nil
			}
		}
	}
	return append(out, batch)
}

// probeRecorder keeps every count and sample, from any goroutine, and passes each
// sample to onSample (outside its own lock) if that is set.
type probeRecorder struct {
	mu       sync.Mutex
	counts   map[string]int64
	samples  map[string][]int64
	onSample func(name string, v int64)
}

func newProbeRecorder() *probeRecorder {
	return &probeRecorder{counts: map[string]int64{}, samples: map[string][]int64{}}
}

func (r *probeRecorder) Count(name string, n int64) {
	r.mu.Lock()
	r.counts[name] += n
	r.mu.Unlock()
}

func (r *probeRecorder) Sample(name string, v int64) {
	r.mu.Lock()
	r.samples[name] = append(r.samples[name], v)
	r.mu.Unlock()
	if r.onSample != nil {
		r.onSample(name, v)
	}
}

func (r *probeRecorder) count(name string) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.counts[name]
}

func (r *probeRecorder) sampled(name string) []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.samples[name])
}

// rangeDeletes is how many range deletions a batch holds.
func rangeDeletes(b *pebble.Batch) int64 {
	var n int64
	r := b.Reader()
	for {
		kind, _, _, ok, err := r.Next()
		if !ok || err != nil {
			return n
		}
		if kind == pebble.InternalKeyKindRangeDelete {
			n++
		}
	}
}

// flushProbe counts, independently of the store's own count, the range deletions
// committed and those flushed, and delays the flushes the store starts: the flush
// itself runs, but the store sees it complete only when it waits for it, so a
// store that never waits never learns that any flush completed. A range deletion
// counts as flushed once the flush started after it has been seen to complete by
// the store. Its count of committed deletions comes from the batches themselves,
// read just before they are applied (beforeApply), and so includes those of the
// writes that rewrite a prefix the pass has not reached.
type flushProbe struct {
	t  *testing.T
	mu sync.Mutex
	// committed is the range deletions in the batches applied, flushed what
	// committed was when the last flush the store saw complete was started, and
	// maxBatch the most range deletions one batch held.
	committed, flushed, maxBatch int64
	pending                      *probeFlush
	// starts is how many flushes the store started, and waits how many times it
	// waited for one.
	starts, waits int
	// never makes the flushes never complete (nor start), and hold makes the store
	// wait without the probe completing them; prompt makes each flush complete
	// before the store is told it has started; onWait runs at every wait.
	never, hold, prompt bool
	onWait              func()
	// beforeStart runs before each flush is started, and kvClosed says the database
	// has been closed; startedLate says a flush was started after that.
	beforeStart func()
	kvClosed    atomic.Bool
	startedLate atomic.Bool
}

// probeFlush is one flush the store started: what committed was then, the flush's
// own channel, and the channel the store waits on.
type probeFlush struct {
	at   int64
	real <-chan struct{}
	seen chan struct{}
}

func (p *flushProbe) beforeApply(b *pebble.Batch) {
	n := rangeDeletes(b)
	p.mu.Lock()
	p.committed += n
	p.maxBatch = max(p.maxBatch, n)
	p.mu.Unlock()
}

func (p *flushProbe) start(flush func() (<-chan struct{}, error)) (<-chan struct{}, error) {
	if p.kvClosed.Load() {
		p.startedLate.Store(true)
	}
	if p.beforeStart != nil {
		p.beforeStart()
	}
	p.mu.Lock()
	at := p.committed
	p.starts++
	p.mu.Unlock()
	var real <-chan struct{}
	if !p.never {
		var err error
		if real, err = flush(); err != nil {
			return nil, err
		}
	}
	f := &probeFlush{at: at, real: real, seen: make(chan struct{})}
	if p.prompt {
		select {
		case <-real:
		case <-time.After(time.Minute):
			p.t.Error("a flush did not complete in a minute")
		}
		p.mu.Lock()
		p.flushed = max(p.flushed, at)
		p.mu.Unlock()
		close(f.seen)
		return f.seen, nil
	}
	p.mu.Lock()
	p.pending = f
	p.mu.Unlock()
	return f.seen, nil
}

func (p *flushProbe) beforeWait() {
	p.mu.Lock()
	p.waits++
	f := p.pending
	p.mu.Unlock()
	if p.onWait != nil {
		p.onWait()
	}
	if p.never || p.hold {
		return
	}
	if f == nil {
		p.t.Error("the store waits for a flush it did not start")
		return
	}
	select {
	case <-f.real:
	case <-time.After(time.Minute):
		p.t.Error("a flush did not complete in a minute")
		return
	}
	p.mu.Lock()
	p.flushed = max(p.flushed, f.at)
	if p.pending == f {
		p.pending = nil
	}
	p.mu.Unlock()
	close(f.seen)
}

// unflushed is the probe's own count of the range deletions not yet flushed.
func (p *flushProbe) unflushed() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.committed - p.flushed
}

func (p *flushProbe) counts() (starts, waits int, maxBatch int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.starts, p.waits, p.maxBatch
}

// boundOptions open a store on fs whose chunks end at 2,000 bytes (about ten
// prefixes) and never by time, which flushes at threshold unflushed range deletions,
// through the probe if there is one. The memtable is large, so that Pebble seldom
// flushes on its own.
func boundOptions(fs vfs.FS, mode RetentionMode, threshold int, probe *flushProbe, rec Recorder) Options {
	o := chunkOptions(fs, nil)
	o.Tuning.MemTableSize = 16 << 20
	o.Retention = mode
	o.retainBatchBytes, o.retainChunkTime = 2000, time.Hour
	o.retainFlushDels = threshold
	o.Recorder = rec
	// Level 0 always reads as clear: with the tiny tuning, the flushes of a pass can
	// pile up sublevels faster than compactions merge them, and a flush put off
	// (which TestAFlushIsPutOffWhileLevelZeroIsNearItsStop covers) leaves the count
	// unbounded, which these tests must not depend on.
	o.l0Sublevels = func() (int, int) { return 0, 12 }
	if probe != nil {
		o.flushHook, o.beforeFlushWait, o.beforeApply = probe.start, probe.beforeWait, probe.beforeApply
	}
	return o
}

// openBound opens a store with the options and writes the stream to it.
func openBound(t *testing.T, o Options) *Store {
	t.Helper()
	s := mustOpen(t, o)
	for _, batch := range boundStream() {
		if err := s.Write(bg, cloneAll(batch)); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

// retainBound retains the store at the horizon and waits for the pass to end.
func retainBound(t *testing.T, s *Store) {
	t.Helper()
	if err := s.Retain(bg, boundHorizon); err != nil {
		t.Fatal(err)
	}
	if s.retention == Background {
		waitRetained(t, s)
	}
}

// dataDigest is the SHA-256 of every data key and value, in key order.
func dataDigest(t *testing.T, s *Store) string {
	t.Helper()
	h := sha256.New()
	for _, kv := range dataBytes(t, s) {
		_, _ = fmt.Fprintf(h, "%d:%s%d:%s", len(kv[0]), kv[0], len(kv[1]), kv[1])
	}
	return hex.EncodeToString(h.Sum(nil))
}

// boundChecker checks, at the start of every chunk, the store's count of unflushed
// range deletions and the probe's, against twice the threshold.
type boundChecker struct {
	threshold int64
	probe     *flushProbe
	mu        sync.Mutex
	starts    int
	worst     [2]int64 // the store's count and the probe's, at their largest
	errs      []string
}

func (c *boundChecker) onSample(name string, v int64) {
	if name != "retain.unflushed_range_deletes" {
		return
	}
	ind := c.probe.unflushed()
	_, _, maxBatch := c.probe.counts()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.starts++
	c.worst = [2]int64{max(c.worst[0], v), max(c.worst[1], ind)}
	if v >= 2*c.threshold && len(c.errs) < 5 {
		c.errs = append(c.errs, fmt.Sprintf("chunk %d starts with %d unflushed range deletions by the store's count, at least twice the threshold %d", c.starts, v, c.threshold))
	}
	if ind >= 2*c.threshold+maxBatch && len(c.errs) < 5 {
		c.errs = append(c.errs, fmt.Sprintf("chunk %d starts with %d range deletions committed since the start of the last flush seen to complete, at least twice the threshold %d plus a chunk's %d", c.starts, ind, c.threshold, maxBatch))
	}
}

func (c *boundChecker) check(t *testing.T, rec *probeRecorder, probe *flushProbe) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, e := range c.errs {
		t.Error(e)
	}
	dels := rec.count("retain.range_deletes")
	flushes := rec.count("retain.flushes")
	starts, waits, maxBatch := probe.counts()
	t.Logf("%d chunks, %d range deletions, %d flushes started, %d waits; the largest counts at a chunk's start %v; a batch held at most %d", c.starts, dels, flushes, waits, c.worst, maxBatch)
	if dels < 4*c.threshold {
		t.Fatalf("only %d range deletions for a threshold of %d: the test proves nothing", dels, c.threshold)
	}
	if want := dels/(2*c.threshold) - 1; flushes < want {
		t.Errorf("%d flushes for %d range deletions, want at least %d", flushes, dels, want)
	}
	if int64(starts) != flushes {
		t.Errorf("the store counts %d flushes and started %d through its hook", flushes, starts)
	}
	if waits == 0 {
		t.Error("the store never waited for a flush: the delay of the flushes was not tested")
	}
}

// At the start of every chunk, after any wait, fewer than twice the threshold of
// range deletions are unflushed, by the store's count and by the probe's, which
// counts them in the batches committed and counts a flush only once the store has
// seen it complete; the pass starts enough flushes for that. In the background,
// synchronously, and in the background with writes during the pass that rewrite
// many of the prefixes it has not reached (their range deletions count too).
func TestAPassKeepsItsUnflushedRangeDeletionsUnderTheBound(t *testing.T) {
	t.Parallel()
	const threshold = 64
	for _, tc := range []struct {
		name    string
		mode    RetentionMode
		touches bool
	}{
		{"background", Background, false},
		{"synchronous", Synchronous, false},
		{"background with writes ahead of the pass", Background, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			probe := &flushProbe{t: t}
			rec := newProbeRecorder()
			c := &boundChecker{threshold: threshold, probe: probe}
			rec.onSample = c.onSample
			o := boundOptions(vfs.NewMem(), tc.mode, threshold, probe, rec)
			var s *Store
			if tc.touches {
				// Before each chunk, edges after the horizon for eight pods, from the last
				// one down: until the pass meets them, each rewrites its prefix first.
				next, seq := boundPods-1, uint64(10*boundPods)
				o.beforeRetainChunk = func() {
					var batch []store.Record
					for range 8 {
						if next < 0 {
							break
						}
						seq++
						batch = append(batch, edgeAt(catalog.L0, boundPod(next), boundNode(next), asyncAt(60), seq))
						next--
					}
					if len(batch) > 0 {
						if err := s.Write(bg, batch); err != nil {
							t.Error(err)
						}
					}
				}
			}
			s = openBound(t, o)
			retainBound(t, s)
			c.check(t, rec, probe)
			if tc.touches {
				if n := rec.count("retain.touch_rewrites"); n < 200 {
					t.Errorf("only %d prefixes were rewritten by writes", n)
				}
			}
		})
	}
}

// The stall of a pass, reproduced without timing: the chunk's clock is the test's,
// and opening a chunk's iterator advances it in proportion to the range deletions
// the probe counts as unflushed, so that the opening alone takes the whole chunk
// time once 200 of them are. Since the opening is charged against the chunk for no
// more than half the chunk time, no chunk but the last ends after one prefix, and
// with the bound the opening never costs more than twice the threshold allows. The
// time the lock is held, which starts before the opening, counts the opening in
// full.
func TestACostlyOpeningDoesNotCollapseAPass(t *testing.T) {
	t.Parallel()
	const threshold = 256
	k := int64(defaultRetainChunkTime) / 200
	probe := &flushProbe{t: t}
	rec := newProbeRecorder()
	o := boundOptions(vfs.NewMem(), Synchronous, threshold, probe, rec)
	o.retainChunkTime = 0 // the default, 20ms of the test's clock
	var now atomic.Int64
	o.chunkNow = func() time.Time { return t0.Add(time.Duration(now.Load())) }
	var injected []int64
	o.afterChunkOpen = func() {
		d := k * probe.unflushed()
		now.Add(d)
		injected = append(injected, d)
	}
	s := openBound(t, o)
	retainBound(t, s)

	prefixes := rec.sampled("retain.chunk_prefixes")
	opens := rec.sampled("retain.chunk_open_ns")
	holds := rec.sampled("retain.chunk_hold_ns")
	if len(prefixes) < 3 || len(opens) != len(prefixes) || len(holds) != len(prefixes) || len(injected) != len(prefixes) {
		t.Fatalf("%d chunks, %d openings, %d holds, %d injected", len(prefixes), len(opens), len(holds), len(injected))
	}
	var total int64
	for _, n := range prefixes {
		total += n
	}
	_, _, maxBatch := probe.counts()
	t.Logf("%d prefixes in %d chunks; prefixes per chunk %v...; the largest opening %d ns", total, len(prefixes), prefixes[:min(len(prefixes), 12)], slices.Max(opens))
	if m := slices.Min(prefixes[1 : len(prefixes)-1]); m <= 1 {
		t.Errorf("a chunk that is neither the first nor the last rewrote %d prefix", m)
	}
	if int64(len(prefixes)) >= total/2 {
		t.Errorf("%d chunks for %d prefixes", len(prefixes), total)
	}
	if limit := k * (2*threshold + maxBatch); slices.Max(opens) > limit {
		t.Errorf("an opening took %d ns, more than the bound allows (%d)", slices.Max(opens), limit)
	}
	if slices.Max(injected) < int64(defaultRetainChunkTime) {
		t.Errorf("no opening took the chunk time (the largest took %d ns): the stall was not reproduced", slices.Max(injected))
	}
	for i := range holds {
		if holds[i] < injected[i] {
			t.Fatalf("chunk %d held the lock for %d ns and its opening alone took %d", i, holds[i], injected[i])
		}
	}
}

// The flush runs on the retainer's goroutine without the store's lock: a writer can
// take the lock while it is started.
func TestABackgroundPassFlushesWithoutTheLock(t *testing.T) {
	t.Parallel()
	probe := &flushProbe{t: t}
	var s *Store
	var held, free atomic.Int32
	probe.beforeStart = func() {
		if s.mu.TryLock() {
			s.mu.Unlock()
			free.Add(1)
		} else {
			held.Add(1)
		}
	}
	s = openBound(t, boundOptions(vfs.NewMem(), Background, 64, probe, nil))
	retainBound(t, s)
	if held.Load() != 0 || free.Load() == 0 {
		t.Errorf("%d flushes were started with the lock held, %d without", held.Load(), free.Load())
	}
}

// The bound changes when the pass flushes, never what it writes: with a low
// threshold, a high one and none, in both modes, the data keys are the same.
func TestTheFlushesChangeNoKeyAPassLeaves(t *testing.T) {
	t.Parallel()
	want := boundReference(t)
	for _, mode := range []RetentionMode{Synchronous, Background} {
		for _, threshold := range []int{64, 1024, -1} {
			t.Run(fmt.Sprintf("mode %d threshold %d", mode, threshold), func(t *testing.T) {
				t.Parallel()
				rec := newProbeRecorder()
				s := openBound(t, boundOptions(vfs.NewMem(), mode, threshold, nil, rec))
				retainBound(t, s)
				if got := dataDigest(t, s); got != want {
					t.Errorf("the data keys hash to %s, and to %s with no flush", got, want)
				}
				if threshold == 64 && rec.count("retain.flushes") == 0 {
					t.Error("no flush was started")
				}
				if threshold < 0 && rec.count("retain.flushes") != 0 {
					t.Errorf("%d flushes with the flushes off", rec.count("retain.flushes"))
				}
			})
		}
	}
}

// boundReference is the digest of the data keys a synchronous pass with no flush
// leaves.
func boundReference(t *testing.T) string {
	t.Helper()
	s := openBound(t, boundOptions(vfs.NewMem(), Synchronous, -1, nil, nil))
	retainBound(t, s)
	return dataDigest(t, s)
}

// closeWithin closes the store, and fails if Close does not return in a minute.
func closeWithin(t *testing.T, s *Store) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- s.Close() }()
	select {
	case err := <-done:
		return err
	case <-time.After(time.Minute):
		t.Fatal("Close did not return in a minute")
		return nil
	}
}

// A Close while the retainer waits for a flush it started returns, and the retainer
// exits; the next Open finishes the pass to the keys of an uninterrupted one. A
// flush that never completes does not hold up Close either.
func TestACloseDuringAWaitForAFlushEndsThePass(t *testing.T) {
	t.Parallel()
	want := boundReference(t)
	for _, never := range []bool{false, true} {
		t.Run(fmt.Sprintf("never completes %v", never), func(t *testing.T) {
			t.Parallel()
			fs := vfs.NewMem()
			waiting := make(chan struct{}, 1)
			probe := &flushProbe{t: t, hold: true, never: never, onWait: func() {
				select {
				case waiting <- struct{}{}:
				default:
				}
			}}
			o := boundOptions(fs, Background, 64, probe, nil)
			exited := make(chan struct{})
			o.retainerExited = exited
			s, err := Open("db", o)
			if err != nil {
				t.Fatal(err)
			}
			for _, batch := range boundStream() {
				if err := s.Write(bg, cloneAll(batch)); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.Retain(bg, boundHorizon); err != nil {
				t.Fatal(err)
			}
			select {
			case <-waiting:
			case <-time.After(time.Minute):
				t.Fatal("the retainer never waited for a flush")
			}
			if err := closeWithin(t, s); err != nil {
				t.Fatal(err)
			}
			requireClosed(t, exited)
			if readMarkerAfterClose(t, fs) == nil {
				t.Fatal("the pass left no marker: it was not stopped in its rewriting")
			}
			r := mustOpen(t, boundOptions(fs, Background, 64, nil, nil))
			waitRetained(t, r)
			if got := dataDigest(t, r); got != want {
				t.Errorf("the pass finished by the next opening leaves keys hashing to %s, an uninterrupted one %s", got, want)
			}
		})
	}
}

// readMarkerAfterClose reads the marker of the database under "db" on fs, opened
// read-only.
func readMarkerAfterClose(t *testing.T, fs vfs.FS) *retainMarker {
	t.Helper()
	o := chunkOptions(fs, nil)
	o.ReadOnly = true
	r := mustOpen(t, o)
	defer func() { _ = r.Close() }()
	return readMarker(t, r)
}

// A synchronous pass that waits for a flush that never completes returns its
// context's error when the context is cancelled, and leaves the store usable: a write
// goes through and Close returns.
func TestASynchronousWaitForAFlushEndsWithItsContext(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(bg)
	defer cancel()
	probe := &flushProbe{t: t, never: true, onWait: cancel}
	s, err := Open("db", boundOptions(vfs.NewMem(), Synchronous, 64, probe, nil))
	if err != nil {
		t.Fatal(err)
	}
	for _, batch := range boundStream() {
		if err := s.Write(bg, cloneAll(batch)); err != nil {
			t.Fatal(err)
		}
	}
	done := make(chan error, 1)
	go func() { done <- s.Retain(ctx, boundHorizon) }()
	select {
	case err = <-done:
	case <-time.After(time.Minute):
		t.Fatal("Retain did not return in a minute after its context was cancelled")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Retain returned %v, want the context's error", err)
	}
	if _, waits, _ := probe.counts(); waits == 0 {
		t.Fatal("the pass never waited for a flush")
	}
	if err := s.Write(bg, []store.Record{edgeAt(catalog.L0, boundPod(0), boundNode(0), asyncAt(70), 1<<20)}); err != nil {
		t.Fatalf("a write after the cancelled Retain: %v", err)
	}
	if err := closeWithin(t, s); err != nil {
		t.Fatal(err)
	}
}

// Close closes the database only after the retainer has exited, and no flush is
// started after that.
func TestCloseClosesTheDatabaseAfterTheRetainerHasExited(t *testing.T) {
	t.Parallel()
	probe := &flushProbe{t: t}
	fs := vfs.NewMem()
	o := boundOptions(fs, Background, 64, probe, nil)
	var s *Store
	var exitedFirst atomic.Bool
	var calls atomic.Int32
	o.beforeKVClose = func() {
		calls.Add(1)
		select {
		case <-s.retainerDone:
			exitedFirst.Store(true)
		default:
		}
		probe.kvClosed.Store(true)
	}
	reached := make(chan struct{})
	var commits atomic.Int32
	o.afterRetainCommit = func() {
		if commits.Add(1) == 40 {
			close(reached)
		}
	}
	s = openBound(t, o)
	if err := s.Retain(bg, boundHorizon); err != nil {
		t.Fatal(err)
	}
	select {
	case <-reached:
	case <-time.After(time.Minute):
		t.Fatal("the pass did not commit 40 chunks")
	}
	if err := closeWithin(t, s); err != nil {
		t.Fatal(err)
	}
	if starts, _, _ := probe.counts(); starts == 0 {
		t.Error("no flush was started before Close")
	}
	if calls.Load() != 1 || !exitedFirst.Load() {
		t.Errorf("the database was closed %d times, the first after the retainer had exited: %v", calls.Load(), exitedFirst.Load())
	}
	if probe.startedLate.Load() {
		t.Error("a flush was started after the database was closed")
	}
}

// The default threshold is the one documented, and a negative one turns the flushes
// off.
func TestTheFlushThresholdDefaults(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		set  int
		want int64
	}{{0, defaultRetainFlushDels}, {100, 100}, {-1, 0}} {
		o := chunkOptions(vfs.NewMem(), nil)
		o.retainFlushDels = tc.set
		s := mustOpen(t, o)
		if s.retainFlushDels != tc.want {
			t.Errorf("retainFlushDels %d opens with %d, want %d", tc.set, s.retainFlushDels, tc.want)
		}
	}
	if defaultRetainFlushDels != 32768 {
		t.Errorf("the default threshold is %d", defaultRetainFlushDels)
	}
}

// A pass learns that a flush has completed without waiting for it: when every flush
// completes before the pass is told it started, the pass never waits, and it starts
// one flush for each threshold's worth of range deletions (and at most a chunk's).
func TestAPassDoesNotWaitForAFlushThatHasCompleted(t *testing.T) {
	t.Parallel()
	const threshold = 64
	for _, mode := range []RetentionMode{Synchronous, Background} {
		t.Run(fmt.Sprintf("mode %d", mode), func(t *testing.T) {
			t.Parallel()
			probe := &flushProbe{t: t, prompt: true}
			rec := newProbeRecorder()
			s := openBound(t, boundOptions(vfs.NewMem(), mode, threshold, probe, rec))
			retainBound(t, s)
			starts, waits, maxBatch := probe.counts()
			dels := rec.count("retain.range_deletes")
			if waits != 0 {
				t.Errorf("the pass waited %d times for flushes that had all completed (%d started)", waits, starts)
			}
			// A flush starts at the first chunk at or past the threshold.
			if want := dels/(threshold+maxBatch) - 1; int64(starts) < want {
				t.Errorf("%d flushes started for %d range deletions, want at least %d", starts, dels, want)
			}
			if m := slices.Max(rec.sampled("retain.unflushed_range_deletes")); m >= 2*threshold {
				t.Errorf("a chunk started with %d unflushed range deletions", m)
			}
		})
	}
}

// A flush that cannot be started is counted and is not an error: the pass goes on,
// tries again after its next chunk, and ends as it would have; the store keeps
// accepting writes.
func TestAFlushThatCannotStartIsTriedAgainAfterTheNextChunk(t *testing.T) {
	t.Parallel()
	want := boundReference(t)
	for _, mode := range []RetentionMode{Synchronous, Background} {
		t.Run(fmt.Sprintf("mode %d", mode), func(t *testing.T) {
			t.Parallel()
			rec := newProbeRecorder()
			o := boundOptions(vfs.NewMem(), mode, 64, nil, rec)
			var calls atomic.Int32
			var chunksAt []int // the chunks committed at each attempt
			var mu sync.Mutex
			o.flushHook = func(flush func() (<-chan struct{}, error)) (<-chan struct{}, error) {
				mu.Lock()
				chunksAt = append(chunksAt, len(rec.sampled("retain.chunk_prefixes")))
				mu.Unlock()
				if calls.Add(1) == 1 {
					return nil, errors.New("the next log file cannot be created")
				}
				return flush()
			}
			s := openBound(t, o)
			retainBound(t, s)
			if n := rec.count("retain.flush_errors"); n != 1 {
				t.Errorf("%d flush errors counted, want 1", n)
			}
			if n := rec.count("retain.flushes"); n < 2 {
				t.Errorf("%d flushes after the one that failed", n)
			}
			mu.Lock()
			if len(chunksAt) < 2 || chunksAt[1] != chunksAt[0]+1 {
				t.Errorf("the attempts were made after %v chunks: the second is not after the next chunk", chunksAt)
			}
			mu.Unlock()
			if got := dataDigest(t, s); got != want {
				t.Errorf("the data keys hash to %s, and to %s with no flush", got, want)
			}
			if err := s.Write(bg, []store.Record{edgeAt(catalog.L0, boundPod(0), boundNode(0), asyncAt(70), 1<<20)}); err != nil {
				t.Fatalf("a write after the failed flush: %v", err)
			}
			if err := s.Retain(bg, asyncAt(40)); err != nil {
				t.Fatalf("a retention after the failed flush: %v", err)
			}
			if mode == Background {
				waitRetained(t, s)
			}
		})
	}
}

// The opening of a chunk's iterator is charged against the chunk's time in full
// when it costs less than half the chunk time, and for half the chunk time when it
// costs more. The test's clock moves by a millisecond each time the store reads it,
// and by the cost of the opening once the iterator is positioned; chunks end by
// time alone (the flushes are off, and the bytes are never reached). Reading the
// clock at the start and after the opening adds one millisecond to the opening, and
// each prefix's check one more, so a chunk whose opening is charged c of the 20 ms
// rewrites ceil((20ms - c) / 1ms) prefixes.
func TestTheOpeningIsChargedUpToHalfTheChunkTime(t *testing.T) {
	t.Parallel()
	const d = time.Millisecond
	for _, tc := range []struct {
		cost time.Duration // what the opening costs beside the clock's own step
		want int64         // prefixes per chunk
	}{
		{0, 19},                     // charged 1ms (the step): 19 prefixes
		{3 * time.Millisecond, 16},  // charged 4ms in full
		{8 * time.Millisecond, 11},  // charged 9ms in full
		{15 * time.Millisecond, 10}, // 16ms, charged 10ms (half the chunk time)
		{60 * time.Millisecond, 10}, // 61ms, charged 10ms
	} {
		t.Run(tc.cost.String(), func(t *testing.T) {
			t.Parallel()
			rec := newProbeRecorder()
			o := boundOptions(vfs.NewMem(), Synchronous, -1, nil, rec)
			o.retainBatchBytes, o.retainChunkTime = 64<<20, 20*time.Millisecond
			var mu sync.Mutex
			now := t0
			o.chunkNow = func() time.Time {
				mu.Lock()
				defer mu.Unlock()
				at := now
				now = now.Add(d)
				return at
			}
			o.afterChunkOpen = func() {
				mu.Lock()
				now = now.Add(tc.cost)
				mu.Unlock()
			}
			s := openBound(t, o)
			retainBound(t, s)
			prefixes := rec.sampled("retain.chunk_prefixes")
			opens := rec.sampled("retain.chunk_open_ns")
			holds := rec.sampled("retain.chunk_hold_ns")
			if len(prefixes) < 3 {
				t.Fatalf("%d chunks", len(prefixes))
			}
			for i, n := range prefixes[:len(prefixes)-1] {
				if n != tc.want {
					t.Fatalf("chunk %d rewrote %d prefixes, want %d (all: %v)", i, n, tc.want, prefixes)
				}
				if opens[i] != int64(tc.cost+d) {
					t.Fatalf("chunk %d sampled an opening of %d ns, want %d", i, opens[i], tc.cost+d)
				}
				if holds[i] < opens[i] {
					t.Fatalf("chunk %d held the lock for %d ns, less than its opening", i, holds[i])
				}
			}
		})
	}
}

// A flush is put off while level 0 is near the sublevel count at which Pebble stops
// writes, because AsyncFlush would wait for compactions with Pebble's commit mutex
// held: it is counted, no flush is started and none is waited for, the pass goes
// on and leaves the keys it would have, and the store keeps accepting writes. Once
// level 0 is clear, the flush is tried again after the next chunk.
func TestAFlushIsPutOffWhileLevelZeroIsNearItsStop(t *testing.T) {
	t.Parallel()
	want := boundReference(t)
	for _, mode := range []RetentionMode{Synchronous, Background} {
		for _, clears := range []bool{false, true} {
			t.Run(fmt.Sprintf("mode %d clears %v", mode, clears), func(t *testing.T) {
				t.Parallel()
				rec := newProbeRecorder()
				o := boundOptions(vfs.NewMem(), mode, 64, nil, rec)
				var mu sync.Mutex
				var reads []int // the chunks committed at each reading of level 0
				spun := false
				o.l0Sublevels = func() (int, int) {
					mu.Lock()
					defer mu.Unlock()
					chunks := len(rec.sampled("retain.chunk_prefixes"))
					reads = append(reads, chunks)
					// A flush put off is tried again only after the next chunk, so level 0
					// is never read three times between two commits. A store that read it
					// again at once would spin here: it fails, and level 0 then reads as
					// clear so that the pass can end.
					if n := len(reads); n >= 3 && reads[n-3] == chunks {
						if !spun {
							spun = true
							t.Errorf("level 0 was read three times with %d chunks committed: a flush put off is tried again before the next chunk", chunks)
						}
						return 0, 12
					}
					if clears && len(reads) > 3 {
						return 9, 12 // below the stop threshold less the margin
					}
					return 10, 12 // at the stop threshold less the margin
				}
				var starts atomic.Int32
				o.flushHook = func(flush func() (<-chan struct{}, error)) (<-chan struct{}, error) {
					starts.Add(1)
					return flush()
				}
				s := openBound(t, o)
				retainBound(t, s)
				deferred, flushes := rec.count("retain.flush_deferred"), rec.count("retain.flushes")
				mu.Lock()
				t.Logf("%d put off, %d flushes, level 0 read at the chunks %v...", deferred, flushes, reads[:min(len(reads), 8)])
				mu.Unlock()
				switch {
				case !clears && (deferred < 5 || flushes != 0 || starts.Load() != 0):
					t.Errorf("with level 0 near its stop: %d put off, %d flushes counted, %d started; want several put off and none started", deferred, flushes, starts.Load())
				case clears && (deferred != 3 || flushes == 0 || int64(starts.Load()) != flushes):
					t.Errorf("with level 0 clear after three readings: %d put off, %d flushes counted, %d started; want 3 put off and flushes after", deferred, flushes, starts.Load())
				}
				mu.Lock()
				for i := 1; i < len(reads) && i < 4; i++ {
					if reads[i] != reads[i-1]+1 {
						t.Errorf("level 0 was read again after %v chunks: a flush put off is not tried again after exactly the next chunk", reads)
						break
					}
				}
				mu.Unlock()
				if got := dataDigest(t, s); got != want {
					t.Errorf("the data keys hash to %s, and to %s with no flush", got, want)
				}
				if err := s.Write(bg, []store.Record{edgeAt(catalog.L0, boundPod(0), boundNode(0), asyncAt(70), 1<<20)}); err != nil {
					t.Fatalf("a write after the flushes put off: %v", err)
				}
			})
		}
	}
}

// The level-0 check is on by default with a margin of two, reads the database's own
// count, and can be turned off.
func TestTheLevelZeroMarginDefaults(t *testing.T) {
	t.Parallel()
	s := mustOpen(t, chunkOptions(vfs.NewMem(), nil))
	if s.retainFlushL0Margin != 2 || s.nearStall() {
		t.Errorf("a new store has a margin of %d and is near a stall: %v", s.retainFlushL0Margin, s.nearStall())
	}
	if n, stop := s.l0Sublevels(); n != 0 || stop != 12 {
		t.Errorf("a new store reads %d sublevels of %d", n, stop)
	}
	o := chunkOptions(vfs.NewMem(), nil)
	o.retainFlushL0Margin = -1
	o.l0Sublevels = func() (int, int) { return 12, 12 }
	if s := mustOpen(t, o); s.nearStall() {
		t.Error("a negative margin still puts a flush off")
	}
}

// With the tiny tuning (but a memtable that the pass does not fill, so that every
// flush is one the pass started), a low threshold and the database's own level 0,
// the flushes of a pass pile up sublevels about as fast as compactions merge them.
// The pass ends with the keys a pass without flushes leaves, and no write stalls
// while it runs: level 0 is read before each flush, and only the pass's flushes add
// to it. Whether a flush is put off depends on how far compactions have got, so the
// count is logged, not asserted (TestAFlushIsPutOffWhileLevelZeroIsNearItsStop
// asserts it with a fake level 0).
func TestAPassOnTheRealLevelZeroNeverStalls(t *testing.T) {
	t.Parallel()
	want := boundReference(t)
	for _, mode := range []RetentionMode{Synchronous, Background} {
		t.Run(fmt.Sprintf("mode %d", mode), func(t *testing.T) {
			t.Parallel()
			rec := newProbeRecorder()
			o := boundOptions(vfs.NewMem(), mode, 64, nil, rec) // the tiny tuning, a 16 MiB memtable
			o.l0Sublevels = nil                                 // the database's own
			var mu sync.Mutex
			var reasons []string
			o.EventListener = &pebble.EventListener{WriteStallBegin: func(i pebble.WriteStallBeginInfo) {
				mu.Lock()
				reasons = append(reasons, i.Reason)
				mu.Unlock()
			}}
			s := openBound(t, o)
			before := s.kv.WriteStalls()
			retainBound(t, s)
			stalls := s.kv.WriteStalls() - before
			deferred, flushes := rec.count("retain.flush_deferred"), rec.count("retain.flushes")
			n, stop := s.kv.L0Sublevels()
			mu.Lock()
			t.Logf("%d flushes, %d put off, %d write stalls during the pass (%d before it; reasons %v); level 0 at the end: %d of %d", flushes, deferred, stalls, before, reasons, n, stop)
			mu.Unlock()
			if flushes == 0 {
				t.Error("no flush was started")
			}
			if stalls != 0 {
				t.Errorf("%d write stalls during the pass", stalls)
			}
			if got := dataDigest(t, s); got != want {
				t.Errorf("the data keys hash to %s, and to %s with no flush", got, want)
			}
		})
	}
}
