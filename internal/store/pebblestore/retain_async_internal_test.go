package pebblestore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
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
)

// bgOptions open a Background store on fs whose chunks end after the first prefix
// that has something to commit.
func bgOptions(fs vfs.FS, ck *CheckpointOptions) Options {
	o := chunkOptions(fs, ck)
	o.Retention = Background
	o.retainBatchBytes, o.retainChunkTime = 1, time.Hour
	return o
}

// syncOptions are the options of the synchronous store a Background one is
// compared with: the same chunks.
func syncOptions(fs vfs.FS, ck *CheckpointOptions) Options {
	o := bgOptions(fs, ck)
	o.Retention = Synchronous
	return o
}

func waitRetained(t *testing.T, s *Store) {
	t.Helper()
	ctx, cancel := context.WithTimeout(bg, 5*time.Minute)
	defer cancel()
	if err := s.Instrument().WaitRetained(ctx); err != nil {
		t.Fatal(err)
	}
}

// A retention in the background leaves the bytes a synchronous one leaves, the same
// writer state and no marker, and the store goes on as the synchronous one does.
func TestABackgroundRetentionLeavesTheBytesOfASynchronousOne(t *testing.T) {
	t.Parallel()
	cs := newChunkStream(t, 90*time.Second)
	for name, ck := range map[string]*CheckpointOptions{"checkpoints on": policy(stress(0)), "checkpoints off": off()} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			syn := mustOpen(t, syncOptions(vfs.NewMem(), ck))
			bgs := mustOpen(t, bgOptions(vfs.NewMem(), ck))
			ref := newRef(t)
			cs.writeTo(t, syn, bgs, ref)
			for _, x := range []store.Store{syn, bgs, ref} {
				if err := x.Retain(bg, cs.h); err != nil {
					t.Fatal(err)
				}
			}
			waitRetained(t, bgs)
			if m := readMarker(t, bgs); m != nil {
				t.Errorf("the marker %+v is left", m)
			}
			if !slices.Equal(dataBytes(t, bgs), dataBytes(t, syn)) {
				t.Fatal("the background retention left other data than the synchronous one")
			}
			if bgs.complete != syn.complete || len(bgs.states) != len(syn.states) {
				t.Errorf("the background pass leaves a map of %d prefixes (complete %v), the synchronous one %d (%v)", len(bgs.states), bgs.complete, len(syn.states), syn.complete)
			}
			if ck.On {
				requireWholeReads(t, bgs)
			}
			compareToReference(t, bgs, ref, cs.entities[:6], cs.h, bgs.LastSeq(), store.Latest)
			cs.writeLater(t, syn, bgs, ref)
			if !slices.Equal(dataBytes(t, bgs), dataBytes(t, syn)) {
				t.Fatal("writes after the background retention left other data than after the synchronous one")
			}
			requireWholeReads(t, bgs)
		})
	}
}

// ---------------------------------------------------------------------------
// Helpers.

// parker holds the retainer at the n-th time it is about to take a chunk (so after
// n-1 chunks are committed) until the test releases it. Used as beforeRetainChunk.
type parker struct {
	n       int
	calls   int
	reached chan struct{}
	open    chan struct{}
	once    sync.Once
}

func newParker(t *testing.T, n int) *parker {
	p := &parker{n: n, reached: make(chan struct{}), open: make(chan struct{})}
	t.Cleanup(p.release)
	return p
}

func (p *parker) hook() {
	p.calls++
	if p.calls == p.n {
		close(p.reached)
		<-p.open
	}
}

func (p *parker) release() { p.once.Do(func() { close(p.open) }) }

func (p *parker) wait(t *testing.T) {
	t.Helper()
	select {
	case <-p.reached:
	case <-time.After(time.Minute):
		t.Fatal("the retainer never reached the point the test holds it at")
	}
}

// edgeAt is an edge record of the pod to the node in the layer, written by producer
// "p" at the instant.
func edgeAt(layer catalog.Layer, pod, node identity.Fingerprint, at time.Time, seq uint64) store.Record {
	return store.Record{
		Layer: layer, Subject: store.EdgeSubject(pod, node, catalog.ScheduledOn), Producer: "p",
		EventTime: at, Seq: seq, Kind: lifecycle.Observe, Payload: fmt.Appendf(nil, "payload-%d", seq),
	}
}

func marked(t *testing.T, s *Store) retainMarker {
	t.Helper()
	m := readMarker(t, s)
	if m == nil {
		t.Fatal("no marker")
	}
	return *m
}

func firstKey() []byte {
	lo, _ := dataBounds()
	return lo
}

// ---------------------------------------------------------------------------
// The writer's memory, and a pass that must not undo what a write did after it.

// The writes between the chunks move the state of the prefixes the pass has passed
// (here a write makes checkpoints in one). A pass that copied what it had worked out
// over the writer's map when it ended would put an older list of checkpoints over
// theirs, and a late record after that would not delete a checkpoint it makes
// untrue: a stale checkpoint, which is a wrong answer. The pass puts what a chunk
// has worked out in the map in the chunk's own critical section instead.
func TestAPassDoesNotPutAnOlderStateOverTheOneAWriterMadeAfterIt(t *testing.T) {
	t.Parallel()
	ck := policy(CheckpointOptions{On: true, KMin: 1})
	h := asyncAt(100)
	pods := []identity.Fingerprint{fingerprintOf(catalog.K8sPod, 0x10), fingerprintOf(catalog.K8sPod, 0x20), fingerprintOf(catalog.K8sPod, 0x30), fingerprintOf(catalog.K8sPod, 0x40)}
	node := fingerprintOf(catalog.K8sNode, 0x80)
	var seq uint64
	next := func(layer catalog.Layer, pod int, at int) store.Record {
		seq++
		return edgeAt(layer, pods[pod], node, asyncAt(at), seq)
	}
	// Pod 0 alone is in the first layer, so its two prefixes are the first the pass takes.
	// It has a record after the horizon already, so that what the pass works out for the
	// prefix it has passed is not empty and is there to be copied.
	var pre [][]store.Record
	for _, at := range []int{10, 30, 103} {
		pre = append(pre, []store.Record{next(catalog.L0, 0, at)})
	}
	for _, at := range []int{5, 15, 25, 35} {
		pre = append(pre, []store.Record{next(catalog.L1, 1, at), next(catalog.L2, 2, at), next(catalog.L3, 3, at)})
	}

	errs := &asyncErrs{}
	rec := newMemRecorder()
	o := bgOptions(vfs.NewMem(), ck)
	o.Recorder = rec
	var s *Store
	var ref *memstore.Store
	calls := 0
	write := func(at int) {
		r := edgeAt(catalog.L0, pods[0], node, asyncAt(at), s.LastSeq()+1)
		errs.add(s.Write(bg, []store.Record{r}))
		errs.add(ref.Write(bg, []store.Record{r}))
	}
	o.beforeRetainChunk = func() {
		if calls++; calls == 2 { // the first chunk is committed, and has passed the prefixes of pod 0
			write(110)
			write(120)
		}
	}
	s = mustOpen(t, o)
	ref = newRef(t)
	for _, batch := range pre {
		for _, x := range []store.Store{s, ref} {
			if err := x.Write(bg, cloneAll(batch)); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, x := range []store.Store{s, ref} {
		if err := x.Retain(bg, h); err != nil {
			t.Fatal(err)
		}
	}
	waitRetained(t, s)
	errs.check(t)
	if calls < 3 {
		t.Fatalf("the retainer took %d chunks: too few for writes between them", calls)
	}
	if rec.Counter("retain.state_keys") == 0 {
		t.Fatal("the pass worked no state out, so there is nothing for it to copy")
	}
	s.mu.Lock()
	err := asyncStateMismatch(s, true)
	s.mu.Unlock()
	if err != nil {
		t.Fatalf("after the pass: %v", err)
	}
	// A late record, before the checkpoints the writes made.
	late := edgeAt(catalog.L0, pods[0], node, asyncAt(106), s.LastSeq()+1)
	for _, x := range []store.Store{s, ref} {
		if err := x.Write(bg, []store.Record{late}); err != nil {
			t.Fatal(err)
		}
	}
	if err := asyncCheckpointsTrue(s); err != nil {
		t.Fatal(err)
	}
	if err := asyncAnswers(s, ref, append(slices.Clone(pods), node), asyncTimes(h), []uint64{store.Latest, s.LastSeq()}); err != nil {
		t.Fatal(err)
	}
}

// ---------------------------------------------------------------------------
// A second Retain during a pass.

// A Retain while a pass is pending starts the pass again at the new horizons from
// the first key, in the generation after it; the older pass installs nothing after
// that, and the result is what two synchronous retentions leave.
func TestASecondRetainDuringAPassStartsItAgainAtTheNewHorizon(t *testing.T) {
	t.Parallel()
	w := newAsyncWorld(7)
	w.later = nil
	h1, h2 := w.h, w.h.Add(40)
	ck := policy(CheckpointOptions{On: true, KMin: 1})

	syn := mustOpen(t, syncOptions(vfs.NewMem(), ck))
	ref := newRef(t)
	pk := newParker(t, 4)
	o := bgOptions(vfs.NewMem(), ck)
	o.beforeRetainChunk = pk.hook
	bgs := mustOpen(t, o)
	// Released before the store is closed (cleanups run last in, first out).
	t.Cleanup(pk.release)
	for _, batch := range w.pre {
		for _, x := range []store.Store{bgs, syn, ref} {
			if err := x.Write(bg, cloneAll(batch)); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, x := range []store.Store{syn, ref} {
		for _, h := range []time.Time{h1, h2} {
			if err := x.Retain(bg, h); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := bgs.Retain(bg, h1); err != nil {
		t.Fatal(err)
	}
	pk.wait(t) // three chunks of the first generation are committed
	first := marked(t, bgs)
	if first.generation != 1 || bytes.Equal(first.resume, firstKey()) {
		t.Fatalf("the marker is %+v: the first pass has not moved", first)
	}
	if err := bgs.Retain(bg, h2); err != nil {
		t.Fatal(err)
	}
	bgs.mu.Lock()
	p := bgs.pass
	gen, resume, touched := p.marker.generation, slices.Clone(p.marker.resume), len(p.touched)
	bgs.mu.Unlock()
	if m := marked(t, bgs); gen != 2 || m.generation != 2 || !bytes.Equal(resume, firstKey()) || !bytes.Equal(m.resume, firstKey()) || touched != 0 {
		t.Fatalf("after the second Retain the pass is generation %d at %x with %d prefixes touched, and the marker is %+v: want generation 2 from the first key", gen, resume, touched, m)
	}
	pk.release()
	waitRetained(t, bgs)
	if m := readMarker(t, bgs); m != nil {
		t.Errorf("the marker %+v is left", m)
	}
	if bgs.retainGen != 2 {
		t.Errorf("the last generation is %d, want 2", bgs.retainGen)
	}
	if d := diffData(t, dataBytes(t, bgs), dataBytes(t, syn), kindRecord, kindCheckpoint, kindBaseline); d != "" {
		t.Fatalf("the data differs from two synchronous retentions': %s", d)
	}
	bgs.mu.Lock()
	err := rememberedStateMismatch(bgs)
	bgs.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if err := asyncAnswers(bgs, ref, w.entities(), asyncTimes(h2), []uint64{store.Latest, bgs.LastSeq()}); err != nil {
		t.Fatal(err)
	}
}

// A layer the older pass was to rewrite and the new Retain does not move is
// rewritten still, at its own horizon. Here a store opens with a layer kept that the
// pass it resumes holds: a Retain that moves the others must carry that layer into
// its marker, and a write to it must rewrite it first.
func TestASecondRetainCarriesOverTheLayersItDoesNotMove(t *testing.T) {
	t.Parallel()
	w := newAsyncWorld(11)
	h1, h2 := w.h, w.h.Add(30)
	var after [][]store.Record
	for _, batch := range w.later {
		var kept []store.Record
		for _, r := range batch {
			if !r.EventTime.Before(h2) {
				kept = append(kept, r)
			}
		}
		if len(kept) > 0 {
			after = append(after, kept)
		}
	}
	w.later = after
	ck := policy(CheckpointOptions{On: true, KMin: 1})
	keepL3 := func(o *Options) { o.Keep[3] = true }

	// The reference: the first retention in full, then the second with the last layer kept.
	fsRef := vfs.NewMem()
	syn := mustOpenFS(t, syncOptions(fsRef, ck))
	for _, batch := range w.pre {
		if err := syn.Write(bg, cloneAll(batch)); err != nil {
			t.Fatal(err)
		}
	}
	if err := syn.Retain(bg, h1); err != nil {
		t.Fatal(err)
	}
	if err := syn.Close(); err != nil {
		t.Fatal(err)
	}
	o := syncOptions(fsRef, ck)
	keepL3(&o)
	syn = mustOpen(t, o)
	if err := syn.Retain(bg, h2); err != nil {
		t.Fatal(err)
	}
	for _, batch := range w.later {
		if err := syn.Write(bg, cloneAll(batch)); err != nil {
			t.Fatal(err)
		}
	}

	// The store: the first retention stopped after two chunks, then reopened in the
	// background with the last layer kept.
	fs := vfs.NewMem()
	stopped := syncOptions(fs, ck)
	stopped.retainStopAfter = 2
	s, err := Open("db", stopped)
	if err != nil {
		t.Fatal(err)
	}
	for _, batch := range w.pre {
		if err := s.Write(bg, cloneAll(batch)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Retain(bg, h1); !errors.Is(err, errInjected) {
		t.Fatalf("Retain = %v, want the injected failure", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	pk := newParker(t, 1)
	bo := bgOptions(fs, ck)
	keepL3(&bo)
	bo.beforeRetainChunk = pk.hook
	bgs := mustOpen(t, bo)
	// Released before the store is closed (cleanups run last in, first out).
	t.Cleanup(pk.release)
	pk.wait(t)
	if m := marked(t, bgs); m.generation != 1 || len(m.layers) != 4 {
		t.Fatalf("the marker Open resumed is %+v: want all four layers of generation 1", m)
	}
	if err := bgs.Retain(bg, h2); err != nil {
		t.Fatal(err)
	}
	m := marked(t, bgs)
	if m.generation != 2 || len(m.layers) != 4 {
		t.Fatalf("the second marker is %+v: want all four layers of generation 2", m)
	}
	for _, l := range m.layers {
		want := h2
		if l.layer == catalog.L3 {
			want = h1 // carried over, not moved
		}
		if !l.horizon.Equal(want) {
			t.Errorf("layer %s is in the marker at %v, want %v", l.layer, l.horizon, want)
		}
	}
	// The writes arrive while the pass is held before its first chunk: those to the
	// layers it rewrites rewrite their prefixes, the last layer's included though it is
	// kept now.
	for _, batch := range w.later {
		if err := bgs.Write(bg, cloneAll(batch)); err != nil {
			t.Fatal(err)
		}
	}
	bgs.mu.Lock()
	var touchedL3 int
	for k := range bgs.pass.touched {
		if layerOfPrefix([]byte(k)) == catalog.L3 {
			touchedL3++
		}
	}
	bgs.mu.Unlock()
	if touchedL3 == 0 {
		t.Error("no write rewrote a prefix of the carried-over layer")
	}
	pk.release()
	waitRetained(t, bgs)
	if m := readMarker(t, bgs); m != nil {
		t.Errorf("the marker %+v is left", m)
	}
	if d := diffData(t, dataBytes(t, bgs), dataBytes(t, syn), kindRecord, kindCheckpoint, kindBaseline); d != "" {
		t.Fatalf("the data differs from the synchronous retentions': %s", d)
	}
}

func mustOpenFS(t *testing.T, o Options) *Store {
	t.Helper()
	s, err := Open("db", o)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// A crash between the publication of the first retention and the second leaves
// the database as one or the other published it: the horizons and the marker are
// one synced commit. Reopened, it resumes the generation it holds and no other.
func TestACrashAroundTheSecondPublicationResumesOneGenerationOnly(t *testing.T) {
	t.Parallel()
	w := newAsyncWorld(13)
	w.later = nil
	h1, h2 := w.h, w.h.Add(40)
	ck := policy(CheckpointOptions{On: true, KMin: 1})

	reference := func(hs ...time.Time) [][2]string {
		s := mustOpen(t, syncOptions(vfs.NewMem(), ck))
		for _, batch := range w.pre {
			if err := s.Write(bg, cloneAll(batch)); err != nil {
				t.Fatal(err)
			}
		}
		for _, h := range hs {
			if err := s.Retain(bg, h); err != nil {
				t.Fatal(err)
			}
		}
		return dataBytes(t, s)
	}
	afterFirst, afterBoth := reference(h1), reference(h1, h2)

	fs := vfs.NewCrashableMem()
	var before, after *vfs.MemFS
	pubs, landed := 0, 0
	pk := newParker(t, 4)
	o := bgOptions(fs, ck)
	o.Sync = true
	o.beforeRetainChunk = pk.hook
	o.beforeHorizonApply = func() error {
		if pubs++; pubs == 2 {
			before = fs.CrashClone(vfs.CrashCloneCfg{})
		}
		return nil
	}
	o.afterHorizonApply = func() error {
		if landed++; landed == 2 {
			after = fs.CrashClone(vfs.CrashCloneCfg{})
		}
		return nil
	}
	s := mustOpen(t, o)
	// Released before the store is closed (cleanups run last in, first out).
	t.Cleanup(pk.release)
	for _, batch := range w.pre {
		if err := s.Write(bg, cloneAll(batch)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Retain(bg, h1); err != nil {
		t.Fatal(err)
	}
	pk.wait(t)
	if err := s.Retain(bg, h2); err != nil {
		t.Fatal(err)
	}
	pk.release()
	waitRetained(t, s)
	if before == nil || after == nil {
		t.Fatal("a publication was not seen")
	}

	for _, c := range []struct {
		name  string
		fs    *vfs.MemFS
		gen   uint64
		h     time.Time
		bytes [][2]string
	}{
		{"before the second publication", before, 1, h1, afterFirst},
		{"after the second publication", after, 2, h2, afterBoth},
	} {
		t.Run(c.name, func(t *testing.T) {
			ro := bgOptions(c.fs, ck)
			ro.Sync = true
			r := mustOpen(t, ro)
			if r.retainGen != c.gen {
				t.Errorf("the reopened store holds generation %d, want %d", r.retainGen, c.gen)
			}
			if got := r.Horizon(); !got.Time.Equal(c.h) {
				t.Errorf("the horizon is %v, want %v", got.Time, c.h)
			}
			waitRetained(t, r)
			if m := readMarker(t, r); m != nil {
				t.Errorf("the marker %+v is left", m)
			}
			if d := diffData(t, dataBytes(t, r), c.bytes, kindRecord, kindBaseline); d != "" {
				t.Errorf("the data differs from the retention's: %s", d)
			}
			if err := asyncCheckpointsTrue(r); err != nil {
				t.Error(err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Close.

// closeWorld is a history with a reference retention, for the tests that stop a
// pass and then finish it on another opening.
type closeWorld struct {
	w     *asyncWorld
	ck    *CheckpointOptions
	want  [][2]string
	chunk int
}

func newCloseWorld(t *testing.T) *closeWorld {
	t.Helper()
	cw := &closeWorld{w: newAsyncWorld(21), ck: policy(CheckpointOptions{On: true, KMin: 1})}
	cw.w.later = nil
	rec := newMemRecorder()
	o := syncOptions(vfs.NewMem(), cw.ck)
	o.Recorder = rec
	s := mustOpen(t, o)
	cw.write(t, s)
	if err := s.Retain(bg, cw.w.h); err != nil {
		t.Fatal(err)
	}
	cw.want, cw.chunk = dataBytes(t, s), int(rec.samples["retain.chunks"][0])
	if cw.chunk < 8 {
		t.Fatalf("%d chunks", cw.chunk)
	}
	return cw
}

func (cw *closeWorld) write(t *testing.T, s *Store) {
	t.Helper()
	for _, batch := range cw.w.pre {
		if err := s.Write(bg, cloneAll(batch)); err != nil {
			t.Fatal(err)
		}
	}
}

// finish opens the database another time, in the background, and requires the pass
// it left to be finished to the bytes of the uninterrupted retention.
func (cw *closeWorld) finish(t *testing.T, fs vfs.FS) {
	t.Helper()
	o := bgOptions(fs, cw.ck)
	r := mustOpen(t, o)
	waitRetained(t, r)
	if m := readMarker(t, r); m != nil {
		t.Errorf("the marker %+v is left", m)
	}
	if d := diffData(t, dataBytes(t, r), cw.want, kindRecord, kindBaseline); d != "" {
		t.Errorf("the pass finished by the next opening leaves other data than an uninterrupted one: %s", d)
	}
	if got := r.Horizon(); !got.Time.Equal(cw.w.h) {
		t.Errorf("the horizon is %v", got.Time)
	}
}

func requireClosed(t *testing.T, exited chan struct{}) {
	t.Helper()
	select {
	case <-exited:
	default:
		t.Fatal("Close returned while the retainer had not exited")
	}
}

// Close stops the pass at a chunk boundary and waits for the goroutine, whether it
// is idle, between two chunks, in a chunk or gone after a failure; a second Close
// returns nil, and the next Open finishes what was left.
func TestCloseStopsTheRetainerAndTheNextOpeningFinishesThePass(t *testing.T) {
	t.Parallel()
	cw := newCloseWorld(t)

	// closeWhile calls Close while the retainer is parked by hold and checks that it
	// waits for it, and that no chunk is taken after.
	closeWhile := func(t *testing.T, name string, park func(o *Options, pk *parker, commits *int)) {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			fs := vfs.NewMem()
			exited := make(chan struct{})
			pk := newParker(t, 3)
			commits := 0
			o := bgOptions(fs, cw.ck)
			o.retainerExited = exited
			park(&o, pk, &commits)
			s, err := Open("db", o)
			if err != nil {
				t.Fatal(err)
			}
			cw.write(t, s)
			if err := s.Retain(bg, cw.w.h); err != nil {
				t.Fatal(err)
			}
			pk.wait(t)
			atPark := commits
			done := make(chan error, 1)
			started := time.Now()
			go func() { done <- s.Close() }()
			select {
			case err := <-done:
				t.Fatalf("Close returned (%v) while the retainer was held in a chunk", err)
			case <-time.After(100 * time.Millisecond):
			}
			pk.release()
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Minute):
				t.Fatal("Close did not return")
			}
			if took := time.Since(started); took > 30*time.Second {
				t.Errorf("Close took %v", took)
			}
			requireClosed(t, exited)
			if commits != atPark {
				t.Errorf("%d chunks were taken after Close, want none", commits-atPark)
			}
			if err := s.Close(); err != nil {
				t.Errorf("a second Close = %v", err)
			}
			cw.finish(t, fs)
		})
	}
	closeWhile(t, "between two chunks", func(o *Options, pk *parker, commits *int) {
		o.beforeRetainChunk = pk.hook
		o.afterRetainCommit = func() { *commits++ }
	})
	closeWhile(t, "in a chunk", func(o *Options, pk *parker, commits *int) {
		// The hook after a commit is the chunk's last act.
		o.afterRetainCommit = func() {
			if *commits++; *commits == 3 {
				pk.calls = pk.n - 1
				pk.hook()
			}
		}
	})

	t.Run("idle", func(t *testing.T) {
		t.Parallel()
		exited := make(chan struct{})
		o := bgOptions(vfs.NewMem(), cw.ck)
		o.retainerExited = exited
		s, err := Open("db", o)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		requireClosed(t, exited)
	})

	t.Run("after a failure", func(t *testing.T) {
		t.Parallel()
		fs := vfs.NewMem()
		exited := make(chan struct{})
		o := bgOptions(fs, cw.ck)
		o.retainerExited = exited
		o.retainStopAfter = 3
		s, err := Open("db", o)
		if err != nil {
			t.Fatal(err)
		}
		cw.write(t, s)
		if err := s.Retain(bg, cw.w.h); err != nil {
			t.Fatal(err)
		}
		select {
		case <-exited:
		case <-time.After(time.Minute):
			t.Fatal("the retainer did not exit after the failure")
		}
		// The failure is latched: it is what Write, Retain and WaitRetained return, and
		// what Close reports after it has closed the database. Reads go on.
		w1 := s.Write(bg, []store.Record{edgeAt(catalog.L2, cw.w.pods[2], cw.w.nodes[0], cw.w.h.Add(5), s.LastSeq()+1)})
		w2 := s.Retain(bg, cw.w.h.Add(100))
		wctx, wcancel := context.WithTimeout(bg, 10*time.Second)
		defer wcancel()
		w3 := s.Instrument().WaitRetained(wctx)
		for name, err := range map[string]error{"Write": w1, "Retain": w2, "WaitRetained": w3} {
			if !errors.Is(err, errInjected) {
				t.Errorf("%s = %v, want the failure that stopped the pass", name, err)
			}
		}
		if _, err := s.Alive(bg, cw.w.pods[2], cw.w.h.Add(5), store.Current(catalog.L2)); err != nil {
			t.Errorf("a read after the failure = %v", err)
		}
		if err := s.Close(); !errors.Is(err, errInjected) {
			t.Errorf("Close = %v, want the failure that stopped the pass", err)
		}
		requireClosed(t, exited)
		cw.finish(t, fs)
	})
}

// ---------------------------------------------------------------------------
// Crash, with writes between the chunks.

// A synced write between two chunks, one that rewrites prefixes the pass has not
// reached, is in the log after the chunks before it and before those after it. A
// crash that keeps the log to some point leaves a database that either mode of
// store, opened on it, finishes to the data the writes and the retention leave. The
// points are in the middle of the pass, and after its last chunk: the chunks are not
// synced, so a sync that follows the last one (here the log's) makes the marker in
// its settling phase durable while the removal of the marker, which is not synced
// either, may not be; a store opened over that marker has only the removal to do.
func TestACrashWithWritesBetweenChunksIsFinishedByEitherMode(t *testing.T) {
	t.Parallel()
	w := newAsyncWorld(17)
	ck := policy(CheckpointOptions{On: true, KMin: 1})
	ref := newRef(t)
	syn := mustOpen(t, syncOptions(vfs.NewMem(), ck))
	for _, batch := range w.pre {
		for _, x := range []store.Store{ref, syn} {
			if err := x.Write(bg, cloneAll(batch)); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, x := range []store.Store{ref, syn} {
		if err := x.Retain(bg, w.h); err != nil {
			t.Fatal(err)
		}
	}
	for _, batch := range w.later[:2] {
		for _, x := range []store.Store{ref, syn} {
			if err := x.Write(bg, cloneAll(batch)); err != nil {
				t.Fatal(err)
			}
		}
	}
	want := dataBytes(t, syn)

	// crash runs a background retention with synced writes at the third and sixth
	// chunk boundaries. With stopAfter zero the disk is taken at the ninth boundary;
	// otherwise the pass fails after that many chunks, and the disk is taken after the
	// log is synced.
	crash := func(t *testing.T, stopAfter int) (crashed *vfs.MemFS, chunks int, touches int64) {
		errs := &asyncErrs{}
		fs := vfs.NewCrashableMem()
		rec := newMemRecorder()
		exited := make(chan struct{})
		o := bgOptions(fs, ck)
		o.Sync, o.Recorder, o.retainStopAfter, o.retainerExited = true, rec, stopAfter, exited
		var s *Store
		calls := 0
		o.beforeRetainChunk = func() {
			switch calls++; calls {
			case 3:
				errs.add(s.Write(bg, cloneAll(w.later[0])))
			case 6:
				errs.add(s.Write(bg, cloneAll(w.later[1])))
			case 9:
				if stopAfter == 0 {
					touches = rec.Counter("retain.touch_rewrites")
					crashed = fs.CrashClone(vfs.CrashCloneCfg{})
				}
			}
		}
		s = mustOpen(t, o)
		for _, batch := range w.pre {
			if err := s.Write(bg, cloneAll(batch)); err != nil {
				t.Fatal(err)
			}
		}
		if err := s.Retain(bg, w.h); err != nil {
			t.Fatal(err)
		}
		if stopAfter == 0 {
			waitRetained(t, s)
			chunks = int(rec.samples["retain.chunks"][0])
		} else {
			select {
			case <-exited:
			case <-time.After(time.Minute):
				t.Fatal("the pass did not stop")
			}
			if err := s.kv.LogData(nil, pebble.Sync); err != nil {
				t.Fatal(err)
			}
			touches = rec.Counter("retain.touch_rewrites")
			crashed = fs.CrashClone(vfs.CrashCloneCfg{})
		}
		errs.check(t)
		if calls < 6 || crashed == nil || touches == 0 {
			t.Fatalf("the pass took %d chunks, the crash was taken (%v), and writes rewrote %d prefixes", calls, crashed != nil, touches)
		}
		return crashed, chunks, touches
	}
	midpass, chunks, _ := crash(t, 0)
	atEnd, _, _ := crash(t, chunks)
	// The second crash is what the test is about: the marker in the settling phase.
	peek := bgOptions(atEnd.CrashClone(vfs.CrashCloneCfg{}), ck)
	peek.ReadOnly = true
	q := mustOpen(t, peek)
	if m := readMarker(t, q); m == nil || m.phase != phaseSettle {
		t.Fatalf("the disk after the last chunk holds the marker %+v, want one in its settling phase", m)
	}

	for point, disk := range map[string]*vfs.MemFS{"in the middle of the pass": midpass, "after the last chunk": atEnd} {
		for name, mode := range map[string]RetentionMode{"background": Background, "synchronous": Synchronous} {
			t.Run(point+"/"+name, func(t *testing.T) {
				t.Parallel()
				ro := bgOptions(disk.CrashClone(vfs.CrashCloneCfg{}), ck)
				ro.Sync, ro.Retention = true, mode
				r := mustOpen(t, ro)
				waitRetained(t, r)
				if m := readMarker(t, r); m != nil {
					t.Errorf("the marker %+v is left", m)
				}
				if d := diffData(t, dataBytes(t, r), want, kindRecord, kindBaseline); d != "" {
					t.Errorf("the data differs from the retention's and the writes': %s", d)
				}
				if err := asyncCheckpointsTrue(r); err != nil {
					t.Error(err)
				}
				if err := asyncAnswers(r, ref, w.entities(), asyncTimes(w.h), []uint64{store.Latest, r.LastSeq()}); err != nil {
					t.Error(err)
				}
			})
		}
	}
}

// A store opened in the background over a marker in its settling phase has nothing
// to rewrite and, as it does not settle, only the marker to remove: the pass ends,
// WaitRetained returns, and a write is no prefix's rewrite, since every prefix is
// rewritten already.
func TestAResumedPassInItsSettlingPhaseOnlyReleasesItsMarker(t *testing.T) {
	t.Parallel()
	cw := newCloseWorld(t)
	fs := vfs.NewMem()
	stopped := syncOptions(fs, cw.ck)
	stopped.retainStopAfter = cw.chunk // the last chunk is committed, the marker is not removed
	s, err := Open("db", stopped)
	if err != nil {
		t.Fatal(err)
	}
	cw.write(t, s)
	if err := s.Retain(bg, cw.w.h); !errors.Is(err, errInjected) {
		t.Fatalf("Retain = %v", err)
	}
	if m := marked(t, s); m.phase != phaseSettle {
		t.Fatalf("the marker is %+v, want one in its settling phase", m)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	rec := newMemRecorder()
	pk := newParker(t, 1)
	o := bgOptions(fs, cw.ck)
	o.Recorder, o.beforeRetainChunk = rec, pk.hook
	r := mustOpen(t, o)
	t.Cleanup(pk.release)
	pk.wait(t)
	// While the pass is pending, a write rewrites no prefix.
	pods := cw.w.pods
	if err := r.Write(bg, []store.Record{edgeAt(catalog.L2, pods[2], cw.w.nodes[0], cw.w.h.Add(5), r.LastSeq()+1)}); err != nil {
		t.Fatal(err)
	}
	if n := rec.Counter("retain.touch_rewrites"); n != 0 {
		t.Errorf("a write rewrote %d prefixes of a pass with nothing left to rewrite", n)
	}
	pk.release()
	waitRetained(t, r)
	if m := readMarker(t, r); m != nil {
		t.Errorf("the marker %+v is left", m)
	}
	if err := r.Write(bg, []store.Record{edgeAt(catalog.L2, pods[2], cw.w.nodes[0], cw.w.h.Add(6), r.LastSeq()+1)}); err != nil {
		t.Fatal(err)
	}
	if n := rec.Counter("retain.touch_rewrites"); n != 0 {
		t.Errorf("%d prefixes were rewritten by writes", n)
	}
	if got := r.Horizon(); !got.Time.Equal(cw.w.h) {
		t.Errorf("the horizon is %v", got.Time)
	}
	if err := r.Close(); err != nil {
		t.Errorf("Close = %v", err)
	}
}

// A write whose commit fails stops the retainer too, since the store refuses further
// work until it is reopened: the pass stays pending with its marker on disk,
// WaitRetained returns the failure, and Close, which reports a retention that
// failed and this one did not, returns no error. The next opening finishes the pass.
func TestAWritesOwnFailureStopsTheRetainerAndLeavesThePassToTheNextOpening(t *testing.T) {
	t.Parallel()
	cw := newCloseWorld(t)
	fs := vfs.NewMem()
	exited := make(chan struct{})
	pk := newParker(t, 3)
	armed := false
	boom := errors.New("injected record commit failure")
	o := bgOptions(fs, cw.ck)
	o.beforeRetainChunk, o.retainerExited = pk.hook, exited
	o.beforeRecordApply = func() error {
		if armed {
			return boom
		}
		return nil
	}
	s, err := Open("db", o)
	if err != nil {
		t.Fatal(err)
	}
	cw.write(t, s)
	if err := s.Retain(bg, cw.w.h); err != nil {
		t.Fatal(err)
	}
	pk.wait(t)
	armed = true
	err = s.Write(bg, []store.Record{edgeAt(catalog.L2, cw.w.pods[2], cw.w.nodes[0], cw.w.h.Add(5), s.LastSeq()+1)})
	if !errors.Is(err, boom) {
		t.Fatalf("Write = %v, want the commit failure", err)
	}
	pk.release()
	select {
	case <-exited:
	case <-time.After(30 * time.Second):
		t.Fatal("the retainer did not stop after a write's commit failed")
	}
	if m := readMarker(t, s); m == nil || m.phase != phaseRewrite {
		t.Errorf("the marker is %+v: the pass should be pending", m)
	}
	ctx, cancel := context.WithTimeout(bg, 10*time.Second)
	defer cancel()
	if err := s.Instrument().WaitRetained(ctx); !errors.Is(err, boom) {
		t.Errorf("WaitRetained = %v, want the commit failure", err)
	}
	if err := s.Close(); err != nil {
		t.Errorf("Close = %v, want no retention error", err)
	}
	cw.finish(t, fs)
}

// ---------------------------------------------------------------------------
// Open with a marker.

// A store opened in the background over a marker has the pass ready before Open
// returns, so a write that arrives before the first chunk rewrites the prefixes the
// pass has not reached, in its own batch, to what the finished retention leaves.
func TestAWriteBeforeTheFirstChunkOfAResumedPassRewritesItsPrefixes(t *testing.T) {
	t.Parallel()
	w := newAsyncWorld(23)
	ck := policy(CheckpointOptions{On: true, KMin: 1})
	batch := w.later[0]

	ref := mustOpen(t, syncOptions(vfs.NewMem(), ck))
	for _, b := range w.pre {
		if err := ref.Write(bg, cloneAll(b)); err != nil {
			t.Fatal(err)
		}
	}
	if err := ref.Retain(bg, w.h); err != nil {
		t.Fatal(err)
	}

	fs := vfs.NewMem()
	stopped := syncOptions(fs, ck)
	stopped.retainStopAfter = 3
	s, err := Open("db", stopped)
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range w.pre {
		if err := s.Write(bg, cloneAll(b)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Retain(bg, w.h); !errors.Is(err, errInjected) {
		t.Fatalf("Retain = %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	rec := newMemRecorder()
	pk := newParker(t, 1)
	o := bgOptions(fs, ck)
	o.Recorder, o.beforeRetainChunk = rec, pk.hook
	r := mustOpen(t, o)
	// Released before the store is closed (cleanups run last in, first out).
	t.Cleanup(pk.release)
	pk.wait(t)
	r.mu.Lock()
	p := r.pass
	if p == nil || p.marker.generation != 1 || p.derive || r.complete || bytes.Equal(p.marker.resume, firstKey()) {
		r.mu.Unlock()
		t.Fatalf("Open left the pass %+v (complete %v)", p, r.complete)
	}
	r.mu.Unlock()
	if rec.Counter("retain.resumed") != 1 {
		t.Errorf("retain.resumed = %d", rec.Counter("retain.resumed"))
	}
	if err := r.Write(bg, cloneAll(batch)); err != nil {
		t.Fatal(err)
	}
	if err := ref.Write(bg, cloneAll(batch)); err != nil {
		t.Fatal(err)
	}
	r.mu.Lock()
	touched := slices.Collect(maps.Keys(r.pass.touched))
	r.mu.Unlock()
	if len(touched) == 0 {
		t.Fatal("the write rewrote no prefix")
	}
	// At this moment, before the pass has taken a chunk, each prefix the write rewrote
	// is as it is in the retained and written reference.
	got, want := byPrefix(t, r), byPrefix(t, ref)
	for _, k := range touched {
		if !slices.Equal(got[k], want[k]) {
			t.Errorf("the prefix %x, rewritten by the write, holds %d keys, and %d in the reference", k, len(got[k]), len(want[k]))
		}
	}
	pk.release()
	waitRetained(t, r)
	if d := diffData(t, dataBytes(t, r), dataBytes(t, ref), kindRecord, kindCheckpoint, kindBaseline); d != "" {
		t.Errorf("the data differs from the reference's: %s", d)
	}
}

// ---------------------------------------------------------------------------
// Layers.

// The prefixes a write rewrites are those of the layers the pass rewrites. In one
// batch that writes to a kept layer, to a layer whose horizon this retention does
// not move and to two layers it does, only the last two are touched.
func TestOnlyThePrefixesOfTheLayersAPassRewritesAreRewrittenByAWrite(t *testing.T) {
	t.Parallel()
	w := newAsyncWorld(29)
	ck := policy(CheckpointOptions{On: true, KMin: 1})
	h1 := w.h
	h2 := h1.Add(60)
	offsets := [4]time.Duration{0, time.Hour, 0, 0}
	keep := [4]bool{true}
	second := func(o *Options) {
		o.Offsets, o.Keep = offsets, keep
	}

	// The first retention moves every layer to h1 (offsets zero, nothing kept); the
	// second, with the first layer kept and the second's horizon an hour back, moves
	// two.
	fs := vfs.NewMem()
	first := bgOptions(fs, ck)
	s := mustOpenFS(t, first)
	for _, b := range w.pre {
		if err := s.Write(bg, cloneAll(b)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Retain(bg, h1); err != nil {
		t.Fatal(err)
	}
	waitRetained(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// The reference: the same two retentions, synchronous, on a file system of its own.
	refFS := vfs.NewMem()
	rs := mustOpenFS(t, syncOptions(refFS, ck))
	for _, b := range w.pre {
		if err := rs.Write(bg, cloneAll(b)); err != nil {
			t.Fatal(err)
		}
	}
	if err := rs.Retain(bg, h1); err != nil {
		t.Fatal(err)
	}
	if err := rs.Close(); err != nil {
		t.Fatal(err)
	}
	ro := syncOptions(refFS, ck)
	second(&ro)
	ref := mustOpen(t, ro)
	if err := ref.Retain(bg, h2); err != nil {
		t.Fatal(err)
	}

	pk := newParker(t, 1)
	bo := bgOptions(fs, ck)
	second(&bo)
	bo.beforeRetainChunk = pk.hook
	r := mustOpen(t, bo)
	// Released before the store is closed (cleanups run last in, first out).
	t.Cleanup(pk.release)
	if err := r.Retain(bg, h2); err != nil {
		t.Fatal(err)
	}
	pk.wait(t)
	r.mu.Lock()
	moving := r.pass.moved
	r.mu.Unlock()
	if moving != [4]bool{false, false, true, true} {
		t.Fatalf("the pass rewrites layers %v, want the last two only", moving)
	}
	var batch []store.Record
	var seq uint64
	for i := range 4 {
		seq++
		at := h2.Add(time.Duration(10 + i))
		batch = append(batch, edgeAt(catalog.L0+catalog.Layer(i), w.pods[i], w.nodes[0], at, r.LastSeq()+seq))
	}
	if err := r.Write(bg, cloneAll(batch)); err != nil {
		t.Fatal(err)
	}
	if err := ref.Write(bg, cloneAll(batch)); err != nil {
		t.Fatal(err)
	}
	r.mu.Lock()
	byLayer := map[catalog.Layer]int{}
	for k := range r.pass.touched {
		byLayer[layerOfPrefix([]byte(k))]++
	}
	r.mu.Unlock()
	if len(byLayer) != 2 || byLayer[catalog.L2] != 2 || byLayer[catalog.L3] != 2 {
		t.Errorf("the write rewrote prefixes by layer %v: want the two prefixes of each of L2 and L3, and none of the kept or unmoved layers", byLayer)
	}
	pk.release()
	waitRetained(t, r)
	if d := diffData(t, dataBytes(t, r), dataBytes(t, ref), kindRecord, kindCheckpoint, kindBaseline); d != "" {
		t.Errorf("the data differs from the synchronous retention's: %s", d)
	}
}

// ---------------------------------------------------------------------------
// The context of Retain.

// The pass runs on the store's own context: cancelling the one Retain was given
// after it returns does not stop it.
func TestCancellingTheContextOfRetainAfterItReturnsDoesNotStopThePass(t *testing.T) {
	t.Parallel()
	cw := newCloseWorld(t)
	pk := newParker(t, 1)
	o := bgOptions(vfs.NewMem(), cw.ck)
	o.beforeRetainChunk = pk.hook
	s := mustOpen(t, o)
	// Released before the store is closed (cleanups run last in, first out).
	t.Cleanup(pk.release)
	cw.write(t, s)
	ctx, cancel := context.WithCancel(bg)
	if err := s.Retain(ctx, cw.w.h); err != nil {
		t.Fatal(err)
	}
	cancel()
	pk.release()
	waitRetained(t, s)
	if m := readMarker(t, s); m != nil {
		t.Errorf("the marker %+v is left", m)
	}
	if d := diffData(t, dataBytes(t, s), cw.want, kindRecord, kindBaseline); d != "" {
		t.Errorf("the pass left other data than an uninterrupted one: %s", d)
	}
}

// ---------------------------------------------------------------------------
// A write that fails after it has rewritten a prefix.

// countdownCtx is a context whose Err reports nothing for the first calls and a
// cancellation after: Write looks at the context when it starts and again just
// before it commits.
type countdownCtx struct {
	context.Context
	left atomic.Int32
}

func (c *countdownCtx) Err() error {
	if c.left.Add(-1) < 0 {
		return context.Canceled
	}
	return nil
}

func countKeys(t *testing.T, s *Store, prefix []byte) int {
	t.Helper()
	lo, hi := prefixBounds(prefix)
	it, err := s.kv.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for ok := it.First(); ok; ok = it.Next() {
		n++
	}
	if err := errors.Join(it.Error(), it.Close()); err != nil {
		t.Fatal(err)
	}
	return n
}

// A prefix is the writer's, and no longer the pass's, only when the batch that
// rewrote it is committed. A write that fails before its commit (here by its
// context, and by a prefix it cannot read) stores nothing, and the prefix is
// rewritten by a later write or by the pass.
func TestAWriteThatFailsBeforeItsCommitLeavesItsPrefixesToThePass(t *testing.T) {
	t.Parallel()
	ck := policy(CheckpointOptions{On: true, KMin: 1})
	setup := func(t *testing.T) (*asyncWorld, *Store, *Store, *parker, *memRecorder) {
		w := newAsyncWorld(51)
		syn := mustOpen(t, syncOptions(vfs.NewMem(), ck))
		pk := newParker(t, 1)
		rec := newMemRecorder()
		o := bgOptions(vfs.NewMem(), ck)
		o.beforeRetainChunk, o.Recorder = pk.hook, rec
		s := mustOpen(t, o)
		// Released before the store is closed (cleanups run last in, first out).
		t.Cleanup(pk.release)
		for _, b := range w.pre {
			for _, x := range []*Store{s, syn} {
				if err := x.Write(bg, cloneAll(b)); err != nil {
					t.Fatal(err)
				}
			}
		}
		for _, x := range []*Store{s, syn} {
			if err := x.Retain(bg, w.h); err != nil {
				t.Fatal(err)
			}
		}
		pk.wait(t)
		return w, s, syn, pk, rec
	}
	touched := func(s *Store) int {
		s.mu.Lock()
		defer s.mu.Unlock()
		return len(s.pass.touched)
	}

	t.Run("by its context", func(t *testing.T) {
		t.Parallel()
		w, s, syn, pk, rec := setup(t)
		batch := w.later[0]
		before, seq := dataBytes(t, s), s.LastSeq()
		ctx := &countdownCtx{Context: bg}
		ctx.left.Store(1) // the first look at it finds nothing, the one before the commit a cancellation
		if err := s.Write(ctx, cloneAll(batch)); !errors.Is(err, context.Canceled) {
			t.Fatalf("Write = %v, want the cancellation", err)
		}
		if n := touched(s); n != 0 || rec.Counter("retain.touch_rewrites") != 0 {
			t.Errorf("a write that stored nothing left %d prefixes touched (%d rewrites counted)", n, rec.Counter("retain.touch_rewrites"))
		}
		if s.LastSeq() != seq || !slices.Equal(dataBytes(t, s), before) {
			t.Error("a write that failed changed the store")
		}
		// A later write rewrites them, and the pass ends where the synchronous one does.
		if err := s.Write(bg, cloneAll(batch)); err != nil {
			t.Fatal(err)
		}
		if touched(s) == 0 {
			t.Error("the write that went through rewrote no prefix")
		}
		if err := syn.Write(bg, cloneAll(batch)); err != nil {
			t.Fatal(err)
		}
		pk.release()
		waitRetained(t, s)
		if d := diffData(t, dataBytes(t, s), dataBytes(t, syn), kindRecord, kindCheckpoint, kindBaseline); d != "" {
			t.Errorf("the data differs from the synchronous store's: %s", d)
		}
	})

	t.Run("by a prefix it cannot read", func(t *testing.T) {
		t.Parallel()
		w, s, _, pk, rec := setup(t)
		// A key too short to be one of the prefix's, before all of its keys.
		var target []byte
		var batch []store.Record
		for _, b := range w.later {
			for _, r := range b {
				sides, err := s.sidesOf(r)
				if err != nil {
					t.Fatal(err)
				}
				s.mu.Lock()
				owed := s.pass.owes(sides[0].prefix)
				s.mu.Unlock()
				if owed && target == nil {
					target, batch = sides[0].prefix, []store.Record{r}
				}
			}
		}
		if target == nil {
			t.Fatal("no record of the world falls in a prefix the pass has to reach")
		}
		set(t, s, append(slices.Clone(target), 0), []byte("x"))
		keys, seq := countKeys(t, s, target), s.LastSeq()
		err := s.Write(bg, cloneAll(batch))
		if err == nil {
			t.Fatal("a write to a prefix with a key that cannot be read went through")
		}
		s.mu.Lock()
		_, marked := s.pass.touched[string(target)]
		s.mu.Unlock()
		if marked || rec.Counter("retain.touch_rewrites") != 0 {
			t.Errorf("the prefix was marked as rewritten (%v) by a write that failed (%d rewrites counted)", marked, rec.Counter("retain.touch_rewrites"))
		}
		if countKeys(t, s, target) != keys || s.LastSeq() != seq {
			t.Error("a write that failed changed the store")
		}
		// The pass goes by that prefix as it does by any, and ends.
		pk.release()
		waitRetained(t, s)
		if m := readMarker(t, s); m != nil {
			t.Errorf("the marker %+v is left", m)
		}
	})
}

// ---------------------------------------------------------------------------
// What the writer remembers during a pass.

// Whatever drops what the writer remembers while a pass is pending (here a failed
// checkpoint commit) means the pass cannot call the map complete when it ends: the
// prefixes it passed before are not in it any more.
func TestAFailureThatDropsTheWritersMapDuringAPassLeavesItIncomplete(t *testing.T) {
	t.Parallel()
	w := newAsyncWorld(53)
	ck := policy(CheckpointOptions{On: true, KMin: 1})
	armed, fired := false, false
	o := bgOptions(vfs.NewMem(), ck)
	var s *Store
	calls := 0
	errs := &asyncErrs{}
	o.beforeRetainChunk = func() {
		if calls++; calls == 5 {
			armed = true
			errs.add(s.Write(bg, cloneAll(w.later[0])))
		}
	}
	o.beforeCheckpointApply = func() error {
		if armed {
			armed, fired = false, true
			return errors.New("injected checkpoint failure")
		}
		return nil
	}
	s = mustOpen(t, o)
	for _, b := range w.pre {
		if err := s.Write(bg, cloneAll(b)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Retain(bg, w.h); err != nil {
		t.Fatal(err)
	}
	waitRetained(t, s)
	errs.check(t)
	if !fired {
		t.Fatal("no checkpoint commit failed")
	}
	if s.complete {
		t.Error("the pass called the writer's map complete after a failure dropped it")
	}
	s.mu.Lock()
	err := asyncStateMismatch(s, false)
	s.mu.Unlock()
	if err != nil {
		t.Error(err)
	}
}

// Everything that drops what the writer remembers tells the pending pass.
func TestWhateverTheWriterForgetsTheBackgroundPassIsToldOf(t *testing.T) {
	t.Parallel()
	s := openMem(t, Options{Retention: Background})
	// The store's retainer reads the pass under the lock, so the test sets it and
	// forgets under the lock too.
	s.mu.Lock()
	defer s.mu.Unlock()
	for name, forget := range map[string]func(){
		"one prefix": func() { s.forget("x") },
		"everything": s.forgetAll,
		"layers":     func() { s.forgetLayers([]retainLayer{{layer: catalog.L1}}) },
	} {
		s.pass = &retainPass{intact: true}
		forget()
		if s.pass.intact {
			t.Errorf("forgetting %s leaves the pass intact", name)
		}
	}
	s.pass = nil
}

// A checkpoint made between two chunks by the instrument, in a prefix the pass has
// not reached, is found by the chunk that does reach it: each chunk reads through
// an iterator opened when it begins, and what it works out is what a read finds.
func TestEachChunkOfABackgroundPassReadsWhatWasCommittedBeforeIt(t *testing.T) {
	t.Parallel()
	w := newAsyncWorld(31)
	w.later = nil
	ck := policy(CheckpointOptions{On: true, KMin: 100000})
	var s *Store
	errs := &asyncErrs{}
	calls := 0
	o := bgOptions(vfs.NewMem(), ck)
	o.beforeRetainChunk = func() {
		if calls++; calls == 2 {
			for i, pod := range w.pods {
				if layerOfPod(i) == catalog.L3 { // the last layer: not reached yet
					errs.add(s.Instrument().CheckpointEdges(layerOfPod(i), pod, store.Forward, w.h.Add(5)))
				}
			}
		}
	}
	s = mustOpen(t, o)
	for _, b := range w.pre {
		if err := s.Write(bg, cloneAll(b)); err != nil {
			t.Fatal(err)
		}
	}
	// The database holds a checkpoint before the retention, so that the pass works the
	// writer's state out.
	if err := s.Instrument().CheckpointEdges(catalog.L1, w.pods[1], store.Forward, w.h.Add(-10)); err != nil {
		t.Fatal(err)
	}
	if err := s.Retain(bg, w.h); err != nil {
		t.Fatal(err)
	}
	waitRetained(t, s)
	errs.check(t)
	if calls < 3 {
		t.Fatalf("the pass took %d chunks", calls)
	}
	s.mu.Lock()
	err := rememberedStateMismatch(s)
	s.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if err := asyncCheckpointsTrue(s); err != nil {
		t.Fatal(err)
	}
}

// ---------------------------------------------------------------------------
// The mode, WaitRetained and the instrument.

func TestTheBackgroundModeIsNotTheDefault(t *testing.T) {
	t.Parallel()
	if got := DefaultOptions().Retention; got != Synchronous {
		t.Errorf("the default retention is %d, want Synchronous", got)
	}
	o := chunkOptions(vfs.NewMem(), nil)
	o.Retention = 3
	if _, err := Open("db", o); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("Open with retention mode 3 = %v, want a refusal", err)
	}
}

func TestWaitRetainedReturnsWhenThePassIsOverOrTheContextEnds(t *testing.T) {
	t.Parallel()
	cw := newCloseWorld(t)

	// A synchronous store has no pass to wait for.
	syn := mustOpen(t, syncOptions(vfs.NewMem(), cw.ck))
	if err := syn.Instrument().WaitRetained(bg); err != nil {
		t.Errorf("WaitRetained on a synchronous store = %v", err)
	}

	pk := newParker(t, 2)
	o := bgOptions(vfs.NewMem(), cw.ck)
	o.beforeRetainChunk = pk.hook
	s := mustOpen(t, o)
	// Released before the store is closed (cleanups run last in, first out).
	t.Cleanup(pk.release)
	if err := s.Instrument().WaitRetained(bg); err != nil {
		t.Errorf("WaitRetained with no pass = %v", err)
	}
	cw.write(t, s)
	if err := s.Retain(bg, cw.w.h); err != nil {
		t.Fatal(err)
	}
	pk.wait(t)
	// The instrument says what Retain itself did: it published the horizons.
	work, flush, settle, hit := s.Instrument().LastRetain()
	if work <= 0 || flush != 0 || settle != 0 || hit {
		t.Errorf("LastRetain = %v, %v, %v, %v: want the time of the publication and no flush or settle", work, flush, settle, hit)
	}
	ctx, cancel := context.WithTimeout(bg, 50*time.Millisecond)
	defer cancel()
	if err := s.Instrument().WaitRetained(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("WaitRetained with a pass held = %v, want the deadline", err)
	}
	pk.release()
	waitRetained(t, s)
	if m := readMarker(t, s); m != nil {
		t.Errorf("WaitRetained returned with the marker %+v still there", m)
	}
	if s.pass != nil {
		t.Error("WaitRetained returned with a pass pending")
	}

	// A store closed with a pass pending says so to the callers waiting on it.
	pk2 := newParker(t, 2)
	o2 := bgOptions(vfs.NewMem(), cw.ck)
	o2.beforeRetainChunk = pk2.hook
	c := mustOpen(t, o2)
	// Released before the store is closed (cleanups run last in, first out).
	t.Cleanup(pk2.release)
	cw.write(t, c)
	if err := c.Retain(bg, cw.w.h); err != nil {
		t.Fatal(err)
	}
	pk2.wait(t)
	waitErr := make(chan error, 1)
	go func() { waitErr <- c.Instrument().WaitRetained(bg) }()
	time.Sleep(20 * time.Millisecond)
	go func() { _ = c.Close() }()
	time.Sleep(20 * time.Millisecond)
	pk2.release()
	select {
	case err := <-waitErr:
		if !errors.Is(err, store.ErrClosed) {
			t.Errorf("WaitRetained in a closed store = %v, want ErrClosed", err)
		}
	case <-time.After(time.Minute):
		t.Fatal("WaitRetained did not return after Close")
	}
}

func TestABackgroundStoreOpenedReadOnlyStartsNoRetainer(t *testing.T) {
	t.Parallel()
	cw := newCloseWorld(t)
	fs := vfs.NewMem()
	stopped := syncOptions(fs, cw.ck)
	stopped.retainStopAfter = 2
	s, err := Open("db", stopped)
	if err != nil {
		t.Fatal(err)
	}
	cw.write(t, s)
	if err := s.Retain(bg, cw.w.h); !errors.Is(err, errInjected) {
		t.Fatal(err)
	}
	before := dataBytes(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	o := bgOptions(fs, cw.ck)
	o.ReadOnly = true
	r := mustOpen(t, o)
	if r.retainerDone != nil || r.pass != nil {
		t.Error("a read-only store started a retainer or made a pass")
	}
	if readMarker(t, r) == nil || !slices.Equal(dataBytes(t, r), before) {
		t.Error("a read-only Open changed the database")
	}
	if err := r.Instrument().WaitRetained(bg); err != nil {
		t.Errorf("WaitRetained = %v", err)
	}
}
