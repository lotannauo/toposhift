package pebblestore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/pebblekv"
)

// This file tests the settle of a Background pass: the key ranges it compacts, that
// it holds no lock, that WaitRetained waits for it, what a Retain or a Close during
// it does, and what the observer is told.

// ---------------------------------------------------------------------------
// The key ranges of a settle.

// coverage fails the test unless the ranges are consecutive and cover the keys of
// the layer from its first byte to the byte after it exactly: each range's end is
// the next one's start, and every range is not empty.
func coverage(t *testing.T, layer catalog.Layer, ranges [][2][]byte) {
	t.Helper()
	if len(ranges) == 0 {
		t.Fatal("no range")
	}
	if first := ranges[0][0]; !bytes.Equal(first, []byte{byte(layer)}) {
		t.Errorf("the first range starts at %x, not at the layer %x", first, byte(layer))
	}
	if last := ranges[len(ranges)-1][1]; !bytes.Equal(last, []byte{byte(layer) + 1}) {
		t.Errorf("the last range ends at %x, not at the byte after the layer %x", last, byte(layer)+1)
	}
	for i, r := range ranges {
		if bytes.Compare(r[0], r[1]) >= 0 {
			t.Errorf("range %d is empty or upside down: %x to %x", i, r[0], r[1])
		}
		if i > 0 && !bytes.Equal(ranges[i-1][1], r[0]) {
			t.Errorf("range %d starts at %x, and the one before it ends at %x: a gap or an overlap", i, r[0], ranges[i-1][1])
		}
	}
}

// within says in which ranges the span [lo, hi) of keys lies wholly.
func within(ranges [][2][]byte, lo, hi []byte) []int {
	var out []int
	for i, r := range ranges {
		if bytes.Compare(r[0], lo) <= 0 && bytes.Compare(hi, r[1]) <= 0 {
			out = append(out, i)
		}
	}
	return out
}

// holding says in which ranges a key lies.
func holding(ranges [][2][]byte, key []byte) []int {
	var out []int
	for i, r := range ranges {
		if bytes.Compare(r[0], key) <= 0 && bytes.Compare(key, r[1]) < 0 {
			out = append(out, i)
		}
	}
	return out
}

// The ranges of a layer are consecutive, cover it exactly, and are 178 with the
// shipped catalog: 1 below its first type, 16 for each of its 11 types, 1 above.
func TestSettleRangesCoverTheLayerExactly(t *testing.T) {
	t.Parallel()
	ids := entityIDs(pebblekv.Default)
	if len(ids) != 11 {
		t.Fatalf("the catalog has %d entity types, and this test is written for 11 (178 ranges): update it with the catalog", len(ids))
	}
	for layer := catalog.L0; layer <= catalog.L3; layer++ {
		ranges := settleRanges(layer, ids)
		coverage(t, layer, ranges)
		if len(ranges) != 178 {
			t.Errorf("layer %s: %d ranges, want 178", layer, len(ranges))
		}

		// A random key of the layer, with any type id and any digest, is in exactly one
		// range; a key of another layer is in none. The keys are spread over the ranges.
		rng := rand.New(rand.NewSource(int64(layer)))
		used := map[int]struct{}{}
		for range 20000 {
			key := make([]byte, keyLen)
			rng.Read(key)
			key[0] = byte(layer)
			if got := holding(ranges, key); len(got) != 1 {
				t.Fatalf("layer %s: key %x is in ranges %v, want one", layer, key, got)
			}
			// A key of a registered type, which are the keys that are stored.
			id := ids[rng.Intn(len(ids))]
			key[1], key[2] = byte(id>>8), byte(id)
			got := holding(ranges, key)
			if len(got) != 1 {
				t.Fatalf("layer %s: key %x is in ranges %v, want one", layer, key, got)
			}
			used[got[0]] = struct{}{}
			other := slices.Clone(key)
			other[0] = byte(layer) + 1
			if got := holding(ranges, other); len(got) != 0 {
				t.Fatalf("layer %s: the key %x of the next layer is in ranges %v", layer, other, got)
			}
		}
		// 11 types of 16 ranges each: every one of those 176 is reached by 20000 draws.
		if len(used) != 11*16 {
			t.Errorf("layer %s: keys of the registered types fell in %d ranges, want %d", layer, len(used), 11*16)
		}
	}

	t.Run("the keys of a real prefix are in one range", func(t *testing.T) {
		t.Parallel()
		ranges := settleRanges(catalog.L1, ids)
		n := 0
		for _, typ := range pebblekv.Default.EntityTypes() {
			for first := range 256 {
				fp := fingerprintOf(typ, byte(first))
				for dir := byte(0); dir <= 2; dir++ {
					prefix := []byte{byte(catalog.L1)}
					prefix, err := pebblekv.Default.AppendFingerprint(prefix, fp)
					if err != nil {
						t.Fatal(err)
					}
					prefix = append(prefix, dir)
					lo, hi := prefixBounds(prefix)
					if got := within(ranges, lo, hi); len(got) != 1 {
						t.Fatalf("the prefix %x is wholly in ranges %v, want one", prefix, got)
					}
					// What a retention deletes in it: from the first key at an instant to
					// the end of the prefix.
					if got := within(ranges, seekKey(prefix, 1_700_000_000), prefixSucc(prefix)); len(got) != 1 {
						t.Fatalf("the span a retention deletes in %x is wholly in ranges %v, want one", prefix, got)
					}
					n++
				}
			}
		}
		if n != 11*256*3 {
			t.Fatalf("%d prefixes tried", n)
		}
	})

	t.Run("an id list with a gap, a single id, none, and the last id there is", func(t *testing.T) {
		t.Parallel()
		for _, c := range []struct {
			name string
			ids  []catalog.EntityID
			want int
		}{
			{"a gap", []catalog.EntityID{1, 2, 7}, 1 + 16*3 + 1},
			{"unsorted and repeated", []catalog.EntityID{7, 1, 2, 7}, 1 + 16*3 + 1},
			{"one id", []catalog.EntityID{5}, 1 + 16 + 1},
			{"none", nil, 1},
			{"the last id there is", []catalog.EntityID{3, 0xFFFF}, 1 + 16*2},
		} {
			for layer := catalog.L0; layer <= catalog.L3; layer++ {
				ranges := settleRanges(layer, c.ids)
				coverage(t, layer, ranges)
				if len(ranges) != c.want {
					t.Errorf("%s, layer %s: %d ranges, want %d", c.name, layer, len(ranges), c.want)
				}
				// A key of every id from 0 to the last and beyond, at both ends of the
				// digest, is in one range.
				for id := 0; id <= 0xFFFF; id += 251 {
					for _, d := range []byte{0x00, 0x0f, 0x10, 0x7f, 0xf0, 0xff} {
						key := append([]byte{byte(layer), byte(id >> 8), byte(id)}, bytes.Repeat([]byte{d}, 16)...)
						if got := holding(ranges, key); len(got) != 1 {
							t.Fatalf("%s: key %x is in ranges %v, want one", c.name, key, got)
						}
					}
				}
			}
		}
		// The gap's ids 3 to 6 are in the last range of id 2, and the ids above 7 in
		// the edge range.
		ranges := settleRanges(catalog.L1, []catalog.EntityID{1, 2, 7})
		inGap := holding(ranges, append([]byte{byte(catalog.L1), 0, 5}, bytes.Repeat([]byte{0x80}, 16)...))
		lastOf2 := holding(ranges, append([]byte{byte(catalog.L1), 0, 2}, bytes.Repeat([]byte{0xf8}, 16)...))
		if len(inGap) != 1 || !slices.Equal(inGap, lastOf2) {
			t.Errorf("an id in the gap is in ranges %v, the last digit range of the id before it in %v", inGap, lastOf2)
		}
		if got := holding(ranges, []byte{byte(catalog.L1), 0, 8}); len(got) != 1 || got[0] != len(ranges)-1 {
			t.Errorf("an id above the last is in ranges %v, want the last of %d", got, len(ranges))
		}
	})
}

// ---------------------------------------------------------------------------
// Helpers.

// settleOptions are the options of a Background store that settles.
func settleOptions(fs vfs.FS, ck *CheckpointOptions) Options {
	o := bgOptions(fs, ck)
	o.SettleRetention = true
	return o
}

// rangeGate holds the settle of a store in its hook, once, after the n-th range, until
// the test lets it go.
type rangeGate struct {
	n       int
	reached chan struct{}
	open    chan struct{}
	once    sync.Once
	freed   sync.Once
}

func newRangeGate(n int) *rangeGate {
	return &rangeGate{n: n, reached: make(chan struct{}), open: make(chan struct{})}
}

func (g *rangeGate) hook(done int) {
	if done != g.n {
		return
	}
	first := false
	g.once.Do(func() { first = true })
	if first {
		close(g.reached)
		<-g.open
	}
}

func (g *rangeGate) free() { g.freed.Do(func() { close(g.open) }) }

func (g *rangeGate) wait(t *testing.T) {
	t.Helper()
	select {
	case <-g.reached:
	case <-time.After(time.Minute):
		t.Fatal("the settle never reached the range the test holds it at")
	}
}

// phaseEvent is one thing an observer was told.
type phaseEvent struct {
	name string
	gen  uint64
	at   time.Time
}

// eventLog is a RetainObserver that keeps what it is told, and notes any call that
// found the store's lock held.
type eventLog struct {
	mu     sync.Mutex
	events []phaseEvent
	s      atomic.Pointer[Store]
	locked atomic.Bool
}

func (l *eventLog) note(name string, gen uint64, at time.Time) {
	if s := l.s.Load(); s != nil {
		// The lock is taken for a moment by a caller of WaitRetained or LastRetain now
		// and then; one this call itself holds up is held for the whole call.
		free := false
		for range 200 {
			if s.mu.TryLock() {
				s.mu.Unlock()
				free = true
				break
			}
			time.Sleep(time.Millisecond)
		}
		if !free {
			l.locked.Store(true)
		}
	}
	l.mu.Lock()
	l.events = append(l.events, phaseEvent{name, gen, at})
	l.mu.Unlock()
}

func (l *eventLog) Published(gen uint64, at time.Time)    { l.note("published", gen, at) }
func (l *eventLog) RewriteEnded(gen uint64, at time.Time) { l.note("rewrite", gen, at) }
func (l *eventLog) SettleEnded(gen uint64, at time.Time)  { l.note("settle", gen, at) }

// of is the events of one generation, in the order they came.
func (l *eventLog) of(gen uint64) []phaseEvent {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []phaseEvent
	for _, e := range l.events {
		if e.gen == gen {
			out = append(out, e)
		}
	}
	return out
}

// at is the instant of the generation's event with the name, and whether there is one.
func (l *eventLog) at(gen uint64, name string) (time.Time, bool) {
	for _, e := range l.of(gen) {
		if e.name == name {
			return e.at, true
		}
	}
	return time.Time{}, false
}

func (l *eventLog) names(gen uint64) []string {
	var out []string
	for _, e := range l.of(gen) {
		out = append(out, e.name)
	}
	return out
}

// stepClock is a clock that moves a millisecond with each reading.
type stepClock struct{ n atomic.Int64 }

func (c *stepClock) now() time.Time { return t0.Add(time.Duration(c.n.Add(1)) * time.Millisecond) }

// lockedRecorder is a recorder that may be read while a store is running.
type lockedRecorder struct {
	mu       sync.Mutex
	counters map[string]int64
}

func newLockedRecorder() *lockedRecorder { return &lockedRecorder{counters: map[string]int64{}} }

func (r *lockedRecorder) Count(name string, n int64) {
	r.mu.Lock()
	r.counters[name] += n
	r.mu.Unlock()
}
func (r *lockedRecorder) Sample(string, int64) {}
func (r *lockedRecorder) Counter(name string) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.counters[name]
}

// tombstoneTables are the tables that hold range deletions and overlap the layers'
// keys.
func tombstoneTables(t *testing.T, s *Store, layers ...catalog.Layer) []pebble.SSTableInfo {
	t.Helper()
	levels, err := s.kv.SSTables(pebble.WithProperties())
	if err != nil {
		t.Fatal(err)
	}
	var out []pebble.SSTableInfo
	for _, level := range levels {
		for _, tb := range level {
			if tb.Properties == nil || tb.Properties.NumRangeDeletions == 0 {
				continue
			}
			for _, l := range layers {
				lo, hi := []byte{byte(l)}, []byte{byte(l) + 1}
				if bytes.Compare(tb.Smallest.UserKey, hi) < 0 && bytes.Compare(tb.Largest.UserKey, lo) >= 0 {
					out = append(out, tb)
					break
				}
			}
		}
	}
	return out
}

// allLayers are the layers a Retain with no offsets moves.
var allLayers = []catalog.Layer{catalog.L0, catalog.L1, catalog.L2, catalog.L3}

// metaBytes is every meta key and value as stored, in key order.
func metaBytes(t *testing.T, s *Store) [][2]string {
	t.Helper()
	it, err := s.kv.NewIter(&pebble.IterOptions{LowerBound: []byte{metaLead}, UpperBound: []byte{metaLead + 1}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = it.Close() }()
	var out [][2]string
	for ok := it.First(); ok; ok = it.Next() {
		out = append(out, [2]string{string(it.Key()), string(it.Value())})
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	return out
}

// ---------------------------------------------------------------------------
// The settle holds no lock.

// While a settle is held between two ranges, the lock is free, a write goes through,
// and WaitRetained is still waiting; and the settle compacts the ranges of the layers
// the pass moved, and no others.
func TestBackgroundSettleHoldsNoLock(t *testing.T) {
	t.Parallel()
	cs := newChunkStream(t, 90*time.Second)
	for name, keep := range map[string][4]bool{"every layer moved": {}, "one layer kept": {true}} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			gate := newRangeGate(3)
			var s *Store
			var lockFree atomic.Bool
			var total atomic.Int64
			o := settleOptions(vfs.NewMem(), policy(stress(0)))
			o.Keep = keep
			o.betweenSettleRanges = func(done int) {
				total.Store(int64(done))
				if done == 3 {
					// From the settle itself: nothing it needs is held.
					if s.mu.TryLock() {
						s.mu.Unlock()
						lockFree.Store(true)
					}
				}
				gate.hook(done)
			}
			s = mustOpen(t, o)
			t.Cleanup(gate.free)
			cs.writeTo(t, s)
			if err := s.Retain(bg, cs.h); err != nil {
				t.Fatal(err)
			}
			gate.wait(t)
			if !lockFree.Load() {
				t.Error("the lock was held while the settle ran")
			}

			// A write goes through while the settle is held.
			wrote := make(chan error, 1)
			started := time.Now()
			go func() { wrote <- s.Write(bg, cloneAll(cs.later)) }()
			select {
			case err := <-wrote:
				if err != nil {
					t.Fatal(err)
				}
				t.Logf("a write took %s while the settle was held", time.Since(started))
			case <-time.After(10 * time.Second):
				gate.free()
				<-wrote // it goes through once the settle lets go; the store is not closed under it
				t.Fatal("a write did not return while the settle was held between two ranges")
			}
			// And the pass is not over.
			ctx, cancel := context.WithTimeout(bg, 50*time.Millisecond)
			defer cancel()
			if err := s.Instrument().WaitRetained(ctx); !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("WaitRetained = %v while the settle was held, want it to be still waiting", err)
			}
			if m := marked(t, s); m.phase != phaseSettle {
				t.Errorf("the marker is %+v during the settle, want one in its settling phase", m)
			}

			gate.free()
			waitRetained(t, s)
			want := int64(4 * 178)
			if keep[0] {
				want = 3 * 178
			}
			if got := total.Load(); got != want {
				t.Errorf("%d ranges were compacted, want %d (178 for each layer moved)", got, want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// WaitRetained waits for the settle.

func TestWaitRetainedWaitsForTheSettle(t *testing.T) {
	t.Parallel()
	cs := newChunkStream(t, 90*time.Second)
	gate := newRangeGate(2)
	log := &eventLog{}
	rec := newLockedRecorder()
	o := settleOptions(vfs.NewMem(), policy(stress(0)))
	o.Observer, o.Recorder, o.betweenSettleRanges = log, rec, gate.hook
	s := mustOpen(t, o)
	t.Cleanup(gate.free)
	log.s.Store(s)
	cs.writeTo(t, s)
	if err := s.Retain(bg, cs.h); err != nil {
		t.Fatal(err)
	}
	gen := marked(t, s).generation
	gate.wait(t)

	returned := make(chan bool, 1)
	go func() {
		err := s.Instrument().WaitRetained(bg)
		_, ended := log.at(gen, "settle")
		returned <- err == nil && ended
	}()
	select {
	case <-returned:
		gate.free()
		t.Fatal("WaitRetained returned before the settle was over")
	case <-time.After(150 * time.Millisecond):
	}
	if _, ended := log.at(gen, "settle"); ended {
		t.Error("the observer was told the settle ended while it was held")
	}
	if _, rewritten := log.at(gen, "rewrite"); !rewritten {
		t.Error("the observer was not told the rewriting was over before the settle")
	}
	gate.free()
	select {
	case ok := <-returned:
		if !ok {
			t.Error("WaitRetained returned before the observer was told the settle ended, or with an error")
		}
	case <-time.After(time.Minute):
		t.Fatal("WaitRetained never returned")
	}
	work, flush, settle, hit := s.Instrument().LastRetain()
	if work <= 0 || flush <= 0 || settle <= 0 || hit {
		t.Errorf("LastRetain = %s, %s, %s, %v; want the publication, a flush and a settle that took time, and no deadline", work, flush, settle, hit)
	}
	// The settle is counted in a Background store as in a Synchronous one, once, and the
	// counts are the ones LastRetain reports.
	if rec.Counter("retain.flush_ns") != flush.Nanoseconds() || rec.Counter("retain.settle_ns") != settle.Nanoseconds() || rec.Counter("retain.settle_deadline_hits") != 0 {
		t.Errorf("the counters: flush %d ns, settle %d ns, %d deadline hits; LastRetain says %s and %s", rec.Counter("retain.flush_ns"), rec.Counter("retain.settle_ns"), rec.Counter("retain.settle_deadline_hits"), flush, settle)
	}
	if m := readMarker(t, s); m != nil {
		t.Errorf("the marker %+v is left", m)
	}
}

// ---------------------------------------------------------------------------
// What the settle leaves.

// After a background retention has settled, no table of a layer it moved holds a range
// tombstone, which is what the settle is for. The flush is the first thing a settle
// does.
func TestBackgroundSettleLeavesNoTombstonesInMovingLayers(t *testing.T) {
	t.Parallel()
	cs := newChunkStream(t, 90*time.Second)

	run := func(t *testing.T, settle bool) (*Store, *lockedRecorder, int64) {
		o := bgOptions(vfs.NewMem(), policy(stress(0)))
		o.SettleRetention = settle
		rec := newLockedRecorder()
		o.Recorder = rec
		var s *Store
		var quiet, atFirst atomic.Int64
		// When the last chunk has landed and the flushes it brought on are over, nothing
		// is left in flight: any flush after that is the settle's.
		o.afterRetainCommit = func() {
			for range 10000 {
				if s.kv.Metrics().Flush.NumInProgress == 0 {
					break
				}
				time.Sleep(time.Millisecond)
			}
			quiet.Store(s.kv.Metrics().Flush.Count)
		}
		o.betweenSettleRanges = func(done int) {
			if done == 1 {
				atFirst.Store(s.kv.Metrics().Flush.Count)
			}
		}
		s = mustOpen(t, o)
		cs.writeTo(t, s)
		if err := s.Retain(bg, cs.h); err != nil {
			t.Fatal(err)
		}
		waitRetained(t, s)
		return s, rec, atFirst.Load() - quiet.Load()
	}

	// What the same retention leaves without a settle, for the record: the database's
	// own compactions may or may not have removed the tombstones by the time they are
	// counted, so this asserts nothing.
	t.Run("without a settle", func(t *testing.T) {
		t.Parallel()
		s, _, _ := run(t, false)
		if err := s.kv.Flush(); err != nil {
			t.Fatal(err)
		}
		t.Logf("%d tables hold range tombstones after a retention that does not settle", len(tombstoneTables(t, s, allLayers...)))
	})

	t.Run("with a settle", func(t *testing.T) {
		t.Parallel()
		s, rec, flushed := run(t, true)
		if rec.Counter("retain.settle_deadline_hits") == 1 {
			return // the deadline passed first: the tombstones may be left
		}
		if err := s.kv.Flush(); err != nil { // what the pass left in the memtable, if the settle missed it
			t.Fatal(err)
		}
		for _, tb := range tombstoneTables(t, s, allLayers...) {
			t.Errorf("table %d (%x to %x) holds %d range tombstones after the settle", tb.FileNum, tb.Smallest.UserKey, tb.Largest.UserKey, tb.Properties.NumRangeDeletions)
		}
		// The memtable the pass's last commits went to was flushed before the first
		// range was compacted: the first range of L0 holds no key, so no compaction of
		// it would have needed the flush.
		if flushed != 1 {
			t.Errorf("%d flushes ran between the last chunk and the first range compacted, want the settle's own", flushed)
		}
		if rec.Counter("retain.flush_ns") <= 0 {
			t.Error("the flush was not counted")
		}
	})
}

// What a Background settle costs in flushes while a writer keeps writing: Pebble's
// Compact flushes a memtable that overlaps the range it is given, so a settle of 178
// ranges for each layer moved could force a flush at each. This reports, and asserts
// nothing about, how many flushes ran across one settle beside a writer, and how many
// the same writer brings on alone in the same time.
func TestBackgroundSettleFlushesBesideAWriter(t *testing.T) {
	cs := newChunkStream(t, 90*time.Second)
	payload := bytes.Repeat([]byte("x"), 512)
	pod, node := fingerprintOf(catalog.K8sPod, 1), fingerprintOf(catalog.K8sNode, 0x20)

	for _, c := range []struct {
		name  string
		pause time.Duration
	}{{"a writer at about a thousand records a second", time.Millisecond}, {"a writer that never pauses", 0}} {
		t.Run(c.name, func(t *testing.T) {
			if c.pause == 0 && testing.Short() {
				t.Skip("runs for the whole of a settle deadline")
			}
			// writes records until stop is closed, one at a time, after the horizon.
			writer := func(s *Store, stop <-chan struct{}) (done chan int) {
				done = make(chan int, 1)
				go func() {
					n := 0
					for {
						select {
						case <-stop:
							done <- n
							return
						default:
						}
						r := edgeAt(catalog.L2, pod, node, cs.h.Add(time.Duration(n+1)*time.Millisecond), s.LastSeq()+1)
						r.Payload = payload
						if err := s.Write(bg, []store.Record{r}); err != nil {
							done <- -1
							return
						}
						n++
						time.Sleep(c.pause)
					}
				}()
				return done
			}

			o := settleOptions(vfs.NewMem(), policy(stress(0)))
			o.SettleDeadline = 2 * time.Second // a writer that never pauses may not let the settle end
			rec := newLockedRecorder()
			o.Recorder = rec
			var s *Store
			var quiet, ranges atomic.Int64
			o.afterRetainCommit = func() {
				for range 10000 {
					if s.kv.Metrics().Flush.NumInProgress == 0 {
						break
					}
					time.Sleep(time.Millisecond)
				}
				quiet.Store(s.kv.Metrics().Flush.Count)
			}
			o.betweenSettleRanges = func(done int) { ranges.Store(int64(done)) }
			s = mustOpen(t, o)
			cs.writeTo(t, s)
			stop := make(chan struct{})
			if err := s.Retain(bg, cs.h); err != nil {
				t.Fatal(err)
			}
			done := writer(s, stop)
			waitRetained(t, s)
			close(stop)
			written := <-done
			if written < 0 {
				t.Fatal("the writer failed")
			}
			_, flush, settle, hit := s.Instrument().LastRetain()
			delta := s.kv.Metrics().Flush.Count - quiet.Load()
			t.Logf("one settle beside a writer: %d CompactRanges calls done (of %d), deadline reached %v (%d), flush %s, settle %s, %d records written meanwhile (%d bytes of payload), Flush.Count changed by %d",
				ranges.Load(), 4*178, hit, rec.Counter("retain.settle_deadline_hits"), flush, settle, written, written*len(payload), delta)

			// The same writer alone, for as long as the settle took.
			alone := mustOpen(t, bgOptions(vfs.NewMem(), policy(stress(0))))
			cs.writeTo(t, alone)
			if err := alone.Retain(bg, cs.h); err != nil {
				t.Fatal(err)
			}
			waitRetained(t, alone)
			before := alone.kv.Metrics().Flush.Count
			stop2 := make(chan struct{})
			done2 := writer(alone, stop2)
			time.Sleep(flush + settle)
			close(stop2)
			written2 := <-done2
			t.Logf("the same writer alone for %s: %d records written, Flush.Count changed by %d", flush+settle, written2, alone.kv.Metrics().Flush.Count-before)
		})
	}
}

// A settle that reaches its deadline is counted and is not an error: the pass ends,
// the store is not latched, and writes go on.
func TestBackgroundSettleDeadlineIsCountedNotLatched(t *testing.T) {
	t.Parallel()
	cs := newChunkStream(t, 90*time.Second)
	rec := newLockedRecorder()
	o := settleOptions(vfs.NewMem(), policy(stress(0)))
	o.SettleDeadline, o.Recorder = time.Nanosecond, rec
	s := mustOpen(t, o)
	cs.writeTo(t, s)
	if err := s.Retain(bg, cs.h); err != nil {
		t.Fatal(err)
	}
	waitRetained(t, s)
	if got := rec.Counter("retain.settle_deadline_hits"); got != 1 {
		t.Errorf("%d deadline hits, want 1", got)
	}
	if _, _, _, hit := s.Instrument().LastRetain(); !hit {
		t.Error("LastRetain does not say the deadline was reached")
	}
	if m := readMarker(t, s); m != nil {
		t.Errorf("the marker %+v is left", m)
	}
	if err := s.Write(bg, cloneAll(cs.later)); err != nil {
		t.Errorf("Write after a settle that reached its deadline = %v", err)
	}
	if err := s.Close(); err != nil {
		t.Errorf("Close = %v", err)
	}
}

// ---------------------------------------------------------------------------
// A Retain, or a Close, during the settle.

func TestRetainDuringSettle(t *testing.T) {
	t.Parallel()
	cs := newChunkStream(t, 90*time.Second)
	h2 := cs.h.Add(20 * time.Second)
	ck := policy(stress(0))

	// The synchronous store with the same two Retains is what the keys must be.
	syn := syncOptions(vfs.NewMem(), ck)
	syn.SettleRetention = true
	want := mustOpen(t, syn)
	cs.writeTo(t, want)
	for _, h := range []time.Time{cs.h, h2} {
		if err := want.Retain(bg, h); err != nil {
			t.Fatal(err)
		}
	}

	gate := newRangeGate(1)
	log := &eventLog{}
	o := settleOptions(vfs.NewMem(), ck)
	o.Observer, o.betweenSettleRanges = log, gate.hook
	s := mustOpen(t, o)
	t.Cleanup(gate.free)
	log.s.Store(s)
	cs.writeTo(t, s)
	if err := s.Retain(bg, cs.h); err != nil {
		t.Fatal(err)
	}
	first := marked(t, s).generation
	gate.wait(t) // the first pass is settling

	if err := s.Retain(bg, h2); err != nil {
		t.Fatal(err)
	}
	second := marked(t, s)
	if second.generation != first+1 {
		t.Fatalf("the second Retain's marker is generation %d, the first's was %d", second.generation, first)
	}
	waiting := make(chan error, 1)
	go func() {
		err := s.Instrument().WaitRetained(bg)
		_, ended := log.at(second.generation, "settle")
		if err == nil && !ended {
			err = errors.New("WaitRetained returned before the newer pass had settled")
		}
		waiting <- err
	}()
	gate.free()
	select {
	case err := <-waiting:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Minute):
		t.Fatal("WaitRetained never returned")
	}
	if m := readMarker(t, s); m != nil {
		t.Errorf("the marker %+v is left", m)
	}
	if got := s.Horizon(); !got.Time.Equal(h2) {
		t.Errorf("the horizon is %v, want %v", got.Time, h2)
	}
	if !slices.Equal(dataBytes(t, s), dataBytes(t, want)) {
		t.Error("two retentions, the second during the settle of the first, left other data than two synchronous ones")
	}
	if !slices.Equal(metaBytes(t, s), metaBytes(t, want)) {
		t.Error("two retentions, the second during the settle of the first, left other meta keys than two synchronous ones")
	}
	// The first generation was replaced while it settled: it is not told its settle ended.
	if names := log.names(first); !slices.Equal(names, []string{"published", "rewrite"}) {
		t.Errorf("the replaced generation was told %v, want published and rewrite only", names)
	}
	if names := log.names(second.generation); !slices.Equal(names, []string{"published", "rewrite", "settle"}) {
		t.Errorf("the second generation was told %v", names)
	}
}

// Close during a settle stops it between two ranges, is not a failure, and leaves the
// marker in its settling phase: the next Open settles again and releases it, whether
// it is opened in the background or synchronously.
func TestCloseDuringSettleThenReopen(t *testing.T) {
	t.Parallel()
	cs := newChunkStream(t, 90*time.Second)
	ck := policy(stress(0))
	for _, mode := range []RetentionMode{Background, Synchronous} {
		t.Run(map[RetentionMode]string{Background: "reopened in the background", Synchronous: "reopened synchronously"}[mode], func(t *testing.T) {
			t.Parallel()
			fs := vfs.NewMem()
			want := mustOpen(t, syncOptions(vfs.NewMem(), ck))
			cs.writeTo(t, want)
			if err := want.Retain(bg, cs.h); err != nil {
				t.Fatal(err)
			}

			gate := newRangeGate(2)
			o := settleOptions(fs, ck)
			o.betweenSettleRanges = gate.hook
			s, err := Open("db", o)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(gate.free)
			cs.writeTo(t, s)
			if err := s.Retain(bg, cs.h); err != nil {
				t.Fatal(err)
			}
			gate.wait(t)

			closed := make(chan error, 1)
			go func() { closed <- s.Close() }()
			for s.ctx.Err() == nil {
				time.Sleep(time.Millisecond)
			}
			released := time.Now()
			gate.free()
			select {
			case err := <-closed:
				if err != nil {
					t.Errorf("Close = %v: stopping a settle is not a failure", err)
				}
				if took := time.Since(released); took > 5*time.Second {
					t.Errorf("Close took %s after the settle was let go", took)
				}
			case <-time.After(time.Minute):
				t.Fatal("Close did not return")
			}
			if s.failed != nil || s.bgErr != nil {
				t.Errorf("the store is latched: %v, %v", s.failed, s.bgErr)
			}

			// The marker is still there, in its settling phase.
			peek := syncOptions(fs, ck)
			peek.ReadOnly = true
			ro, err := Open("db", peek)
			if err != nil {
				t.Fatal(err)
			}
			m := marked(t, ro)
			if m.phase != phaseSettle {
				t.Errorf("the marker is %+v after Close during the settle, want one in its settling phase", m)
			}
			if err := ro.Close(); err != nil {
				t.Fatal(err)
			}

			rec := newLockedRecorder()
			var settled atomic.Int64
			again := settleOptions(fs, ck)
			again.Retention, again.Recorder = mode, rec
			again.betweenSettleRanges = func(done int) { settled.Store(int64(done)) }
			r := mustOpen(t, again)
			waitRetained(t, r)
			if m := readMarker(t, r); m != nil {
				t.Errorf("the marker %+v is left", m)
			}
			if rec.Counter("retain.resumed") != 1 {
				t.Errorf("retain.resumed = %d", rec.Counter("retain.resumed"))
			}
			if rec.Counter("retain.flush_ns") <= 0 || rec.Counter("retain.settle_ns") <= 0 {
				t.Errorf("the reopened store did not settle: flush %d ns, settle %d ns", rec.Counter("retain.flush_ns"), rec.Counter("retain.settle_ns"))
			}
			if mode == Background && settled.Load() != 4*178 {
				t.Errorf("the reopened store compacted %d ranges, want %d", settled.Load(), 4*178)
			}
			if mode == Synchronous && settled.Load() != 0 {
				t.Errorf("the synchronous reopening compacted %d ranges, want its own settle", settled.Load())
			}
			if !slices.Equal(dataBytes(t, r), dataBytes(t, want)) {
				t.Error("the retention finished by the next opening left other data than an uninterrupted one")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// The data a settle leaves.

// The same streams, retained in the background with a settle and synchronously with
// one, leave byte-identical data and meta keys, before and after writes that follow.
func TestBackgroundAndSynchronousKeyspacesEqual(t *testing.T) {
	t.Parallel()
	for _, seed := range []uint64{1, 2, 3} {
		cs := newSeededStream(t, seed, 90*time.Second)
		for name, ck := range map[string]*CheckpointOptions{"checkpoints on": policy(stress(0)), "checkpoints off": off()} {
			t.Run(fmt.Sprintf("seed %d, %s", seed, name), func(t *testing.T) {
				t.Parallel()
				syn := syncOptions(vfs.NewMem(), ck)
				syn.SettleRetention = true
				a := mustOpen(t, syn)
				b := mustOpen(t, settleOptions(vfs.NewMem(), ck))
				cs.writeTo(t, a, b)
				before := dataBytes(t, a)
				for _, s := range []*Store{a, b} {
					if err := s.Retain(bg, cs.h); err != nil {
						t.Fatal(err)
					}
				}
				waitRetained(t, b)
				if slices.Equal(dataBytes(t, a), before) {
					t.Fatal("the retention changed no key: the comparison below proves nothing")
				}
				if !slices.Equal(dataBytes(t, a), dataBytes(t, b)) {
					t.Fatal("the data keyspace after the retention differs between the modes")
				}
				if !slices.Equal(metaBytes(t, a), metaBytes(t, b)) {
					t.Fatal("the meta keys after the retention differ between the modes")
				}
				cs.writeLater(t, a, b)
				if !slices.Equal(dataBytes(t, a), dataBytes(t, b)) {
					t.Fatal("the data keyspace after later writes differs between the modes")
				}
				if !slices.Equal(metaBytes(t, a), metaBytes(t, b)) {
					t.Fatal("the meta keys after later writes differ between the modes")
				}
				requireWholeReads(t, b)
			})
		}
	}
}

// ---------------------------------------------------------------------------
// The observer.

func TestObserverOrder(t *testing.T) {
	t.Parallel()
	cs := newChunkStream(t, 90*time.Second)
	ck := policy(stress(0))

	for name, settle := range map[string]bool{"with a settle": true, "without a settle": false} {
		t.Run("one retention, "+name, func(t *testing.T) {
			t.Parallel()
			log := &eventLog{}
			clock := &stepClock{}
			o := bgOptions(vfs.NewMem(), ck)
			o.SettleRetention, o.Observer, o.clock = settle, log, clock.now
			s := mustOpen(t, o)
			log.s.Store(s)
			cs.writeTo(t, s)
			if err := s.Retain(bg, cs.h); err != nil {
				t.Fatal(err)
			}
			gen := marked(t, s).generation
			waitRetained(t, s)
			pub, ok1 := log.at(gen, "published")
			rew, ok2 := log.at(gen, "rewrite")
			set, ok3 := log.at(gen, "settle")
			if !ok1 || !ok2 || !ok3 || len(log.of(gen)) != 3 {
				t.Fatalf("the observer was told %v", log.names(gen))
			}
			if !pub.Before(rew) || rew.After(set) {
				t.Errorf("published %v, rewrite ended %v, settle ended %v: want published < rewrite ended <= settle ended", pub, rew, set)
			}
			if log.locked.Load() {
				t.Error("the store's lock was held during a call of the observer")
			}
			// Told before the waiter returned, and nothing is told after.
			if n := len(log.of(gen)); n != 3 {
				t.Errorf("%d calls", n)
			}
		})
	}

	t.Run("a retention replaced while it rewrites", func(t *testing.T) {
		t.Parallel()
		log := &eventLog{}
		clock := &stepClock{}
		pk := newParker(t, 3)
		o := settleOptions(vfs.NewMem(), ck)
		o.Observer, o.clock, o.beforeRetainChunk = log, clock.now, pk.hook
		s := mustOpen(t, o)
		t.Cleanup(pk.release)
		log.s.Store(s)
		cs.writeTo(t, s)
		if err := s.Retain(bg, cs.h); err != nil {
			t.Fatal(err)
		}
		first := marked(t, s).generation
		pk.wait(t) // two chunks are committed, the third is not begun
		if err := s.Retain(bg, cs.h.Add(20*time.Second)); err != nil {
			t.Fatal(err)
		}
		second := marked(t, s).generation
		pk.release()
		waitRetained(t, s)

		if names := log.names(first); !slices.Equal(names, []string{"published", "rewrite"}) {
			t.Fatalf("the replaced generation was told %v, want published and rewrite and no settle", names)
		}
		if names := log.names(second); !slices.Equal(names, []string{"published", "rewrite", "settle"}) {
			t.Fatalf("the second generation was told %v", names)
		}
		oldEnd, _ := log.at(first, "rewrite")
		newPub, _ := log.at(second, "published")
		if !oldEnd.Equal(newPub) {
			t.Errorf("the replaced generation's rewriting ended at %v, the new publication was at %v: want the same instant", oldEnd, newPub)
		}
		oldPub, _ := log.at(first, "published")
		if !oldPub.Before(oldEnd) {
			t.Errorf("published %v, rewrite ended %v", oldPub, oldEnd)
		}
		rew, _ := log.at(second, "rewrite")
		set, _ := log.at(second, "settle")
		if !newPub.Before(rew) || rew.After(set) {
			t.Errorf("second: published %v, rewrite ended %v, settle ended %v", newPub, rew, set)
		}
		if log.locked.Load() {
			t.Error("the store's lock was held during a call of the observer")
		}
	})

	t.Run("a store with no observer reads no clock for it", func(t *testing.T) {
		t.Parallel()
		var reads atomic.Int64
		o := settleOptions(vfs.NewMem(), ck)
		o.clock = func() time.Time { reads.Add(1); return time.Now() }
		s := mustOpen(t, o)
		cs.writeTo(t, s)
		if err := s.Retain(bg, cs.h); err != nil {
			t.Fatal(err)
		}
		waitRetained(t, s)
		if n := reads.Load(); n != 0 {
			t.Errorf("the clock was read %d times by a store with neither an observer nor a recorder", n)
		}
	})
}

// ---------------------------------------------------------------------------
// The Synchronous settle is the one it always was.

func TestSynchronousSettleUnchanged(t *testing.T) {
	t.Parallel()
	cs := newChunkStream(t, 90*time.Second)
	rec := newLockedRecorder()
	var ranges atomic.Int64
	o := syncOptions(vfs.NewMem(), policy(stress(0)))
	o.SettleRetention, o.Recorder = true, rec
	o.betweenSettleRanges = func(int) { ranges.Add(1) }
	s := mustOpen(t, o)
	cs.writeTo(t, s)
	if err := s.Retain(bg, cs.h); err != nil {
		t.Fatal(err)
	}
	if n := ranges.Load(); n != 0 {
		t.Errorf("a synchronous store compacted %d ranges: it waits for the database to be at rest instead", n)
	}
	work, flush, settle, hit := s.Instrument().LastRetain()
	// The wait for rest is at least the second the database must be still for.
	if work <= 0 || flush <= 0 || settle < time.Second || hit {
		t.Errorf("LastRetain = %s, %s, %s, %v; want the work, a flush, a wait of a second or more and no deadline", work, flush, settle, hit)
	}
	if rec.Counter("retain.flush_ns") != flush.Nanoseconds() || rec.Counter("retain.settle_ns") != settle.Nanoseconds() || rec.Counter("retain.settle_deadline_hits") != 0 {
		t.Errorf("the counters are %d, %d and %d, LastRetain says %s and %s", rec.Counter("retain.flush_ns"), rec.Counter("retain.settle_ns"), rec.Counter("retain.settle_deadline_hits"), flush, settle)
	}
	if m := readMarker(t, s); m != nil {
		t.Errorf("the marker %+v is left", m)
	}
}
