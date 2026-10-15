package pebblestore

import (
	"bytes"
	"context"
	"fmt"
	"math/bits"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/lotannauo/toposhift/internal/store"
)

// This file tests the end of a background chunk for a waiting write: the flag a
// Write raises before it takes the lock, the chunk that ends at a prefix boundary
// once it has done its minimum, the retainer's bounded yield, and that a pass
// still ends, with the bytes of a synchronous one, whatever the writers do.

// yieldClock is the clock of a chunk's time, moved by the test.
type yieldClock struct{ ns atomic.Int64 }

func (c *yieldClock) now() time.Time          { return time.Unix(1_000_000, c.ns.Load()) }
func (c *yieldClock) advance(d time.Duration) { c.ns.Add(int64(d)) }

// yieldOptions open a Background store whose chunks end by nothing but a waiting
// write: no limit of bytes, and an hour of time. minPrefixes 0 is the default.
func yieldOptions(fs vfs.FS, minPrefixes int) Options {
	o := chunkOptions(fs, nil)
	o.Retention = Background
	o.retainBatchBytes, o.retainChunkTime = 1<<30, time.Hour
	o.retainMinPrefixes = minPrefixes
	return o
}

// storedPrefixes are the prefixes of the data, in key order.
func storedPrefixes(t *testing.T, s *Store) [][]byte {
	t.Helper()
	var out [][]byte
	for _, kv := range dataBytes(t, s) {
		if p := []byte(kv[0][:prefixLen]); len(out) == 0 || !bytes.Equal(out[len(out)-1], p) {
			out = append(out, p)
		}
	}
	return out
}

func writePre(t *testing.T, w *asyncWorld, stores ...*Store) {
	t.Helper()
	for _, batch := range w.pre {
		for _, s := range stores {
			if err := s.Write(bg, cloneAll(batch)); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func waitRetainedWithin(t *testing.T, s *Store, d time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(bg, d)
	defer cancel()
	if err := s.Instrument().WaitRetained(ctx); err != nil {
		t.Fatalf("the pass did not end within %s: %v", d, err)
	}
}

// chunkLog collects what a pass says of its chunks.
type chunkLog struct {
	prefixes []int64
	yielded  int64
	chunks   int64
}

func logOf(rec *memRecorder) chunkLog {
	var chunks int64
	if got := rec.samples["retain.chunks"]; len(got) == 1 {
		chunks = got[0]
	}
	return chunkLog{prefixes: slices.Clone(rec.samples["retain.chunk_prefixes"]), yielded: rec.Counter("retain.chunks_yielded"), chunks: chunks}
}

// A Write says it waits just before it takes the lock and no longer once it has it;
// no other call says so.
func TestOnlyAWriteSaysItWaitsForTheLock(t *testing.T) {
	t.Parallel()
	w := newAsyncWorld(1)
	s := mustOpen(t, chunkOptions(vfs.NewMem(), nil))
	writePre(t, w, s)
	if n := s.writersWaiting.Load(); n != 0 {
		t.Fatalf("%d writes wait after the writes have returned", n)
	}

	// A call that blocks on the lock, held by the test, and the flag it raises.
	blocked := func(name string, call func() error, want int32) {
		t.Helper()
		s.mu.Lock()
		done := make(chan error, 1)
		go func() { done <- call() }()
		deadline := time.Now().Add(10 * time.Second)
		if want > 0 {
			for s.writersWaiting.Load() != want && time.Now().Before(deadline) {
				runtime.Gosched()
			}
		} else {
			// Nothing to wait for: give the call time to reach the lock.
			time.Sleep(50 * time.Millisecond)
		}
		got := s.writersWaiting.Load()
		select {
		case err := <-done:
			s.mu.Unlock()
			t.Fatalf("%s returned %v while the lock was held", name, err)
		default:
		}
		s.mu.Unlock()
		if err := <-done; err != nil {
			t.Errorf("%s: %v", name, err)
		}
		if got != want {
			t.Errorf("%s: %d writes waiting while it waited for the lock, want %d", name, got, want)
		}
		if n := s.writersWaiting.Load(); n != 0 {
			t.Errorf("%s: %d writes waiting after it returned", name, n)
		}
	}
	rec := w.later[0]
	for i := range rec {
		rec[i].Seq = s.LastSeq() + uint64(i) + 1
	}
	blocked("Write", func() error { return s.Write(bg, cloneAll(rec)) }, 1)
	blocked("Retain", func() error { return s.Retain(bg, w.h) }, 0)
	blocked("Retain again", func() error { return s.Retain(bg, w.h.Add(time.Nanosecond)) }, 0)

	// A write that is refused after the lock leaves the flag down too.
	stale := cloneAll(rec)
	if err := s.Write(bg, stale); err == nil {
		t.Fatal("a batch with a sequence number already used was stored")
	}
	if n := s.writersWaiting.Load(); n != 0 {
		t.Errorf("%d writes wait after a refused write", n)
	}
}

// The retainer yields to a waiting write a bounded number of times.
func TestTheRetainerYieldsToAWaitingWriteAtMostItsBound(t *testing.T) {
	t.Parallel()
	s := mustOpen(t, chunkOptions(vfs.NewMem(), nil))
	if n := s.yieldToWriters(); n != 0 {
		t.Errorf("yielded %d times with no write waiting", n)
	}
	s.writersWaiting.Store(1)
	got := make(chan int, 1)
	go func() { got <- s.yieldToWriters() }()
	select {
	case n := <-got:
		if n != 64 {
			t.Errorf("yielded %d times to a write that never took the lock, want 64", n)
		}
	case <-time.After(30 * time.Second):
		s.writersWaiting.Store(0) // lets the spinning goroutine end
		t.Fatal("the retainer yields without a bound while a write waits")
	}
	s.writersWaiting.Store(0)
}

// A background chunk ends at the first prefix boundary at which a write is waiting
// and the chunk has rewritten its minimum: at the prefix after which the write began
// to wait, or at the minimum if that is later. The marker's resume key is the
// successor of the last prefix done, and the pass leaves the data a synchronous
// one does.
func TestABackgroundChunkEndsForAWaitingWriteAfterItsMinimum(t *testing.T) {
	t.Parallel()
	const minimum = 4
	for _, k := range []int64{1, minimum, 6, 9} {
		t.Run(fmt.Sprintf("a write waits after prefix %d", k), func(t *testing.T) {
			t.Parallel()
			w := newAsyncWorld(2)
			want := max(k, minimum)
			rec := newMemRecorder()
			o := yieldOptions(vfs.NewMem(), minimum)
			// A clock that stands still: the opening costs nothing, so the work minimum
			// is met at once and the prefix minimum is what is tested.
			o.Recorder, o.chunkNow = rec, (&yieldClock{}).now
			var s *Store
			var mu sync.Mutex
			var resumes [][]byte
			o.afterChunkPrefix = func(n int64) {
				if n == k {
					s.writersWaiting.Store(1)
				}
			}
			o.afterRetainCommit = func() {
				s.writersWaiting.Store(0) // the write took the lock
				m := readMarker(t, s)
				mu.Lock()
				defer mu.Unlock()
				if m == nil {
					resumes = append(resumes, nil)
					return
				}
				resumes = append(resumes, slices.Clone(m.resume))
			}
			s = mustOpen(t, o)
			twin := mustOpen(t, chunkOptions(vfs.NewMem(), nil))
			writePre(t, w, s, twin)
			prefixes := storedPrefixes(t, s)
			if len(prefixes) < 4*int(want) {
				t.Fatalf("%d prefixes: too few to cut into chunks of %d", len(prefixes), want)
			}
			if err := twin.Retain(bg, w.h); err != nil {
				t.Fatal(err)
			}
			if err := s.Retain(bg, w.h); err != nil {
				t.Fatal(err)
			}
			waitRetainedWithin(t, s, time.Minute)

			l := logOf(rec)
			if int(l.chunks) != len(l.prefixes) || l.chunks < 3 {
				t.Fatalf("%d chunks, %d samples of their prefixes", l.chunks, len(l.prefixes))
			}
			// The hook of the last chunk may still be running: read the others.
			mu.Lock()
			read := slices.Clone(resumes[:min(len(resumes), len(l.prefixes)-1)])
			mu.Unlock()
			if len(read) != len(l.prefixes)-1 {
				t.Fatalf("%d markers read for %d chunks", len(read), len(l.prefixes))
			}
			var done int64
			for i, n := range l.prefixes {
				done += n
				if i == len(l.prefixes)-1 {
					break
				}
				if n != want {
					t.Errorf("chunk %d rewrote %d prefixes, want %d (the write waited after prefix %d, the minimum is %d)", i+1, n, want, k, minimum)
				}
				if got, wantKey := read[i], prefixSucc(prefixes[done-1]); !bytes.Equal(got, wantKey) {
					t.Errorf("chunk %d: the marker resumes at %x, the successor of the last prefix done is %x", i+1, got, wantKey)
				}
			}
			if done != int64(len(prefixes)) {
				t.Errorf("the chunks rewrote %d prefixes, the data has %d", done, len(prefixes))
			}
			if l.yielded != l.chunks-1 {
				t.Errorf("retain.chunks_yielded %d, want %d: every chunk but the last ended for the write", l.yielded, l.chunks-1)
			}
			if !slices.Equal(dataBytes(t, s), dataBytes(t, twin)) {
				t.Error("the pass left other data than a synchronous one")
			}
			if m := readMarker(t, s); m != nil {
				t.Errorf("the marker %+v is left", m)
			}
		})
	}
}

// A chunk does not end for a write that waits from its first prefix before it has
// rewritten its minimum, nor before its work has lasted as long as its opening was
// charged. The clock of the chunk is the test's: the opening takes 10 ms and each
// prefix a step.
func TestAChunkDoesNotEndForAWriteBeforeItsMinimum(t *testing.T) {
	t.Parallel()
	const minimum = 4
	opening := 10 * time.Millisecond
	for _, c := range []struct {
		name string
		step time.Duration
		want int64
	}{
		{"prefixes of 10 ms: the prefix minimum binds", 10 * time.Millisecond, minimum},
		{"prefixes of 5 ms: the prefix minimum binds", 5 * time.Millisecond, minimum},
		{"prefixes of 3 ms: the prefix minimum binds", 3 * time.Millisecond, minimum},
		{"prefixes of 2 ms: the work minimum binds", 2 * time.Millisecond, 5},
		{"prefixes of 1 ms: the work minimum binds", time.Millisecond, 10},
		{"prefixes of 750 us: the work minimum binds", 750 * time.Microsecond, 14},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			w := newAsyncWorld(3)
			rec := newMemRecorder()
			clock := &yieldClock{}
			o := yieldOptions(vfs.NewMem(), minimum)
			o.Recorder, o.chunkNow = rec, clock.now
			o.afterChunkOpen = func() { clock.advance(opening) }
			o.afterChunkPrefix = func(int64) { clock.advance(c.step) }
			s := mustOpen(t, o)
			writePre(t, w, s)
			prefixes := len(storedPrefixes(t, s))
			if prefixes < 2*int(c.want)+1 {
				t.Fatalf("%d prefixes: too few for chunks of %d", prefixes, c.want)
			}
			s.writersWaiting.Store(1) // from the first prefix of every chunk, to the end
			if err := s.Retain(bg, w.h); err != nil {
				t.Fatal(err)
			}
			waitRetainedWithin(t, s, time.Minute)
			l := logOf(rec)
			for i, n := range l.prefixes[:len(l.prefixes)-1] {
				if n != c.want {
					t.Errorf("chunk %d rewrote %d prefixes, want %d", i+1, n, c.want)
				}
			}
			if last := l.prefixes[len(l.prefixes)-1]; last > c.want || last == 0 {
				t.Errorf("the last chunk rewrote %d prefixes", last)
			}
		})
	}
}

// A pass ends whatever the writers do. With a write waiting for the whole pass, the
// pass takes its chunks at their minimum, none of them but the last shorter, and
// never more chunks than the minimum allows; with writes arriving without end, it
// ends with the bytes a synchronous pass leaves.
func TestAPassEndsWhateverTheWritersDo(t *testing.T) {
	t.Parallel()
	for _, minimum := range []int{1, 2, 4, 7, 0} {
		t.Run(fmt.Sprintf("a write waits throughout, minimum %d", minimum), func(t *testing.T) {
			t.Parallel()
			w := newAsyncWorld(4)
			rec := newMemRecorder()
			o := yieldOptions(vfs.NewMem(), minimum)
			o.Recorder = rec
			s := mustOpen(t, o)
			t.Cleanup(func() { s.writersWaiting.Store(0) }) // so that Close is not kept waiting by a failure
			writePre(t, w, s)
			want := int64(minimum)
			if minimum == 0 {
				want = defaultRetainMinPrefixes
			}
			if s.chunkMinPrefixes != want {
				t.Fatalf("the store's minimum is %d, want %d", s.chunkMinPrefixes, want)
			}
			total := int64(len(storedPrefixes(t, s)))
			s.writersWaiting.Store(1)
			if err := s.Retain(bg, w.h); err != nil {
				t.Fatal(err)
			}
			waitRetainedWithin(t, s, 30*time.Second)
			l := logOf(rec)
			var sum int64
			for i, n := range l.prefixes {
				sum += n
				if i < len(l.prefixes)-1 && n < want {
					t.Errorf("chunk %d rewrote %d prefixes, fewer than the minimum %d", i+1, n, want)
				}
			}
			if sum != total {
				t.Errorf("the chunks rewrote %d prefixes of %d", sum, total)
			}
			if bound := (total + want - 1) / want; l.chunks > bound {
				t.Errorf("%d chunks for %d prefixes at a minimum of %d: at most %d", l.chunks, total, want, bound)
			}
			if m := readMarker(t, s); m != nil {
				t.Errorf("the marker %+v is left", m)
			}
		})
	}

	t.Run("writes arrive without end", func(t *testing.T) {
		t.Parallel()
		w := newAsyncWorld(5)
		rec := newMemRecorder()
		o := yieldOptions(vfs.NewMem(), 1)
		o.Recorder = rec
		s := mustOpen(t, o)
		syn := mustOpen(t, chunkOptions(vfs.NewMem(), nil))
		writePre(t, w, s, syn)
		if err := syn.Retain(bg, w.h); err != nil {
			t.Fatal(err)
		}
		if err := s.Retain(bg, w.h); err != nil {
			t.Fatal(err)
		}
		ended := make(chan struct{})
		go func() {
			defer close(ended)
			ctx, cancel := context.WithTimeout(bg, time.Minute)
			defer cancel()
			_ = s.Instrument().WaitRetained(ctx)
		}()
		// Writers that never stop until the pass has ended, each batch one edge of the
		// world's pods at a later instant than any before it.
		hNs := int(w.h.Sub(w.base))
		var batches [][]store.Record
		seq := s.LastSeq()
	writing:
		for n := range 20000 {
			select {
			case <-ended:
				break writing
			default:
			}
			i := n % len(w.pods)
			seq++
			r := edgeAt(layerOfPod(i), w.pods[i], w.nodes[n%len(w.nodes)], w.at(hNs+10+n), seq)
			if err := s.Write(bg, []store.Record{r}); err != nil {
				t.Fatal(err)
			}
			batches = append(batches, []store.Record{r})
		}
		select {
		case <-ended:
		case <-time.After(time.Minute):
			t.Fatal("the pass did not end while writes kept arriving")
		}
		waitRetainedWithin(t, s, time.Second)
		if m := readMarker(t, s); m != nil {
			t.Errorf("the marker %+v is left", m)
		}
		for _, b := range batches {
			if err := syn.Write(bg, cloneAll(b)); err != nil {
				t.Fatal(err)
			}
		}
		if !slices.Equal(dataBytes(t, s), dataBytes(t, syn)) {
			t.Error("a pass with writes at every chunk left other data than the synchronous one")
		}
		l := logOf(rec)
		t.Logf("%d writes during the pass, %d chunks, %d ended for a write", len(batches), l.chunks, l.yielded)
	})
}

// A synchronous pass holds the lock across its chunks, so a waiting write does not
// shorten any of them; and a background pass with no write waiting takes the chunks
// a synchronous one does, however many writes came before it.
func TestASynchronousPassIgnoresWaitingWrites(t *testing.T) {
	t.Parallel()
	cs := newChunkStream(t, 90*time.Second)
	run := func(t *testing.T, mode RetentionMode, waiting int32) (chunkLog, [][2]string) {
		t.Helper()
		rec := newMemRecorder()
		o := chunkOptions(vfs.NewMem(), nil)
		o.Retention, o.retainBatchBytes, o.retainChunkTime, o.retainMinPrefixes = mode, 3000, time.Hour, 1
		o.Recorder = rec
		s := mustOpen(t, o)
		cs.writeTo(t, s)
		if n := s.writersWaiting.Load(); n != 0 {
			t.Fatalf("%d writes wait after the stream has been written", n)
		}
		s.writersWaiting.Store(waiting)
		if err := s.Retain(bg, cs.h); err != nil {
			t.Fatal(err)
		}
		if mode == Background {
			waitRetainedWithin(t, s, time.Minute)
		}
		return logOf(rec), dataBytes(t, s)
	}
	quiet, quietData := run(t, Synchronous, 0)
	if quiet.chunks < 3 {
		t.Fatalf("%d chunks: too few to tell", quiet.chunks)
	}
	waiting, waitingData := run(t, Synchronous, 1)
	if !slices.Equal(quiet.prefixes, waiting.prefixes) || quiet.chunks != waiting.chunks || waiting.yielded != 0 {
		t.Errorf("a synchronous pass with a write waiting took chunks of %v (%d ended for a write); with none waiting %v", waiting.prefixes, waiting.yielded, quiet.prefixes)
	}
	if !slices.Equal(quietData, waitingData) {
		t.Error("a waiting write changed the data a synchronous pass leaves")
	}
	// A background pass that no write waits for, after a stream of writes, ends its
	// chunks as the synchronous one does.
	idle, idleData := run(t, Background, 0)
	if !slices.Equal(quiet.prefixes, idle.prefixes) || idle.yielded != 0 {
		t.Errorf("a background pass with no write waiting took chunks of %v (%d ended for a write); the synchronous one %v", idle.prefixes, idle.yielded, quiet.prefixes)
	}
	if !slices.Equal(quietData, idleData) {
		t.Error("the background pass left other data than the synchronous one")
	}
	// And with a write waiting throughout, it still leaves the same data, in more chunks.
	held, heldData := run(t, Background, 1)
	if held.yielded == 0 || held.chunks <= quiet.chunks {
		t.Errorf("a background pass with a write waiting took %d chunks (%d ended for a write); the synchronous one %d", held.chunks, held.yielded, quiet.chunks)
	}
	if !slices.Equal(quietData, heldData) {
		t.Error("the background pass with a write waiting left other data than the synchronous one")
	}
}

// A write that waits during a chunk takes the lock before the retainer opens the
// next chunk. The write is made to wait by a hook that starts it inside the chunk,
// and the order is read from the hooks. The test runs on one processor, where the
// woken write is the next goroutine the processor runs once the retainer yields;
// with several, a woken goroutine that another processor is slow to run can lose the
// lock to the retainer, which the mutex corrects by handing it over once the waiter
// has waited a millisecond (see the retainer's bounded yield). The test does not
// run in parallel with others: it sets the number of processors.
func TestAYieldedChunkLetsTheWaitingWriteIn(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))
	runs := 200
	if raceEnabled {
		runs = 20
	}
	var failed atomic.Bool
	for run := range runs {
		if failed.Load() {
			break
		}
		w := newAsyncWorld(int64(run % 8))
		var mu sync.Mutex
		var events []string
		note := func(e string) {
			mu.Lock()
			defer mu.Unlock()
			events = append(events, e)
		}
		o := yieldOptions(vfs.NewMem(), 2)
		var s *Store
		var once sync.Once
		var wrote chan error
		o.afterChunkOpen = func() { note("open") }
		o.afterRetainCommit = func() { note("commit") }
		o.beforeRecordApply = func() error { note("write"); return nil }
		o.afterChunkPrefix = func(n int64) {
			if n != 2 {
				return
			}
			once.Do(func() {
				wrote = make(chan error, 1)
				batch := cloneAll(w.later[0])
				for i := range batch {
					batch[i].Seq = s.LastSeq() + uint64(i) + 1
				}
				go func() { wrote <- s.Write(bg, batch) }()
				deadline := time.Now().Add(10 * time.Second)
				for s.writersWaiting.Load() == 0 && time.Now().Before(deadline) {
					runtime.Gosched()
				}
			})
		}
		s = mustOpen(t, o)
		writePre(t, w, s)
		mu.Lock()
		events = events[:0]
		mu.Unlock()
		if err := s.Retain(bg, w.h); err != nil {
			t.Fatal(err)
		}
		waitRetainedWithin(t, s, time.Minute)
		if err := <-wrote; err != nil {
			t.Fatal(err)
		}
		mu.Lock()
		log := slices.Clone(events)
		mu.Unlock()
		opens, at := 0, -1
		for i, e := range log {
			switch e {
			case "open":
				opens++
			case "write":
				if at < 0 {
					at = i
					if opens != 1 {
						t.Errorf("run %d: the write took the lock after %d chunk openings, want exactly 1 (it waited during the first): %s", run, opens, strings.Join(log, " "))
						failed.Store(true)
					}
				}
			}
		}
		if at < 0 {
			t.Fatalf("run %d: the write never committed: %s", run, strings.Join(log, " "))
		}
		_ = s.Close()
	}
}

// bucketLow is the lower bound of the bucket of a histogram with eight buckets to a
// doubling (exact below 8), the histogram the benchmark gate reads the median of a
// chunk's prefixes from, at the bottom of its bucket.
func bucketLow(n int64) int64 {
	if n < 8 {
		return n
	}
	k := bits.Len64(uint64(n))
	sub := (n >> (k - 4)) & 7
	return (8 + sub) << (k - 4)
}

// The minimum a chunk rewrites before it ends for a waiting write is a bucket edge
// of that histogram at or above 104: chunks of exactly 100 would be read at 96, below
// the 100 the median must reach, and chunks of exactly the minimum must be read at
// the minimum.
func TestTheMinimumOfPrefixesIsABucketEdgeAboveWhatTheGateReads(t *testing.T) {
	t.Parallel()
	if got := bucketLow(100); got != 96 {
		t.Fatalf("100 prefixes are read at %d, the arithmetic here is wrong", got)
	}
	const gateFloor = 100 // the least the median of a judged chunk's prefixes may be
	n := int64(defaultRetainMinPrefixes)
	if low := bucketLow(n); low != n || low < gateFloor {
		t.Errorf("chunks of %d prefixes are read at %d: want a bucket edge of at least %d", n, low, gateFloor)
	}
}
