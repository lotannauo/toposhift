package pebblestore

import (
	"encoding/binary"
	"fmt"
	"reflect"
	"slices"
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

// sinceWorld is a pebblestore and the reference fed the same records: a node, the
// pods placed on it, and two producers.
type sinceWorld struct {
	t    *testing.T
	s    *Store
	ref  *memstore.Store
	node identity.Fingerprint
	pods []identity.Fingerprint
	seq  uint64
}

func newSinceWorld(t *testing.T, s *Store) *sinceWorld {
	t.Helper()
	w := &sinceWorld{t: t, s: s, ref: newRef(t), node: fingerprintOf(catalog.K8sNode, 0x10)}
	for i := range 3 {
		w.pods = append(w.pods, fingerprintOf(catalog.K8sPod, byte(0x40+0x10*i)))
	}
	return w
}

// edge is the next record of pod i's placement on the node.
func (w *sinceWorld) edge(pod int, p lifecycle.Producer, at time.Time, kind lifecycle.Kind, ttl time.Duration) store.Record {
	w.seq++
	r := store.Record{
		Layer: catalog.L2, Subject: store.EdgeSubject(w.pods[pod], w.node, catalog.ScheduledOn), Producer: p,
		EventTime: at, Seq: w.seq, Kind: kind, TTL: ttl, EventTimeBasis: store.BasisObserved,
	}
	if kind == lifecycle.Observe {
		r.Payload = fmt.Appendf(nil, "placement %d", w.seq)
	}
	return r
}

func (w *sinceWorld) write(recs ...store.Record) {
	w.t.Helper()
	for _, x := range []store.Store{w.s, w.ref} {
		if err := x.Write(bg, slices.Clone(recs)); err != nil {
			w.t.Fatal(err)
		}
	}
}

func (w *sinceWorld) retain(h time.Time) {
	w.t.Helper()
	for _, x := range []store.Store{w.s, w.ref} {
		if err := x.Retain(bg, h); err != nil {
			w.t.Fatal(err)
		}
	}
}

// same asks both stores and requires the reference's answer, which it returns.
func (w *sinceWorld) same(fps []identity.Fingerprint, dir store.Direction, from, to time.Time, sc store.Scope, o store.SinceOptions) []store.Since {
	w.t.Helper()
	want, err := w.ref.WindowSince(bg, fps, dir, from, to, sc, o)
	if err != nil {
		w.t.Fatal(err)
	}
	got, err := w.s.WindowSince(bg, fps, dir, from, to, sc, o)
	if err != nil {
		w.t.Fatalf("WindowSince(%s, from %s): %v", dir, from.Sub(t0), err)
	}
	if !reflect.DeepEqual(got, want) {
		w.t.Fatalf("WindowSince(%s, from %s, %+v) =\n%+v\nthe reference says\n%+v", dir, from.Sub(t0), o, got, want)
	}
	return want
}

// seedProducers lists an answer's seeds as pod@producer.
func (w *sinceWorld) seedProducers(a store.Since) []string {
	var out []string
	for _, s := range a.Seeds {
		pod := slices.Index(w.pods, s.Record.Subject.A)
		out = append(out, fmt.Sprintf("%d@%s", pod, s.Record.Producer))
	}
	return out
}

// A window that starts at the horizon finds the baseline keyed there: the seed walk
// starts at the baseline's own key, where seeking "at or before from-1ns" would skip
// it and walk into the history the retention deleted. The reference whose deadline
// is the horizon is no seed; the one that holds across it is, without its payload.
func TestTheSeedWalkFindsTheBaselineAtTheHorizon(t *testing.T) {
	t.Parallel()
	rec := newMemRecorder()
	w := newSinceWorld(t, openMem(t, Options{Checkpoints: off(), Recorder: rec}))
	h := t0.Add(3 * time.Minute)
	w.write(
		w.edge(0, "kubelet", t0, lifecycle.Observe, 3*time.Minute), // its deadline is h
		w.edge(1, "k8sobjects", t0, lifecycle.Observe, 0),
		w.edge(2, "k8sobjects", t0.Add(time.Minute), lifecycle.Delete, 0),
	)
	w.write(w.edge(0, "k8sobjects", h, lifecycle.Observe, 0))
	w.retain(h)
	for _, from := range []time.Time{h, h.Add(time.Nanosecond), h.Add(time.Minute)} {
		for _, p := range []bool{false, true} {
			ans := w.same([]identity.Fingerprint{w.node}, store.Reverse, from, from.Add(time.Hour), store.Current(catalog.L2), store.SinceOptions{Payloads: p})
			if got := w.seedProducers(ans[0]); !slices.Equal(got, []string{"1@k8sobjects"}) && from.Equal(h) {
				t.Errorf("the seeds from the horizon are %v; want the reference that holds across it, 1@k8sobjects", got)
			}
		}
	}
	if rec.Counter("read.baseline_entries_decoded") == 0 {
		t.Error("no seed walk decoded the baseline")
	}
}

// A checkpoint at exactly from lists the references alive at from, so it misses one
// whose deadline is from itself, which holds just before from and is a seed. The
// walk steps over it. A checkpoint one nanosecond earlier holds that reference, and
// the walk stops there; with payloads, the payload of a seed it decides is read from
// the seed's record.
func TestTheSeedWalkStepsOverACheckpointAtFrom(t *testing.T) {
	t.Parallel()
	rec := newMemRecorder()
	w := newSinceWorld(t, openMem(t, Options{Checkpoints: off(), Recorder: rec}))
	c := t0.Add(3 * time.Minute)
	w.write(
		w.edge(0, "kubelet", t0, lifecycle.Observe, 3*time.Minute), // its deadline is c
		w.edge(1, "k8sobjects", t0, lifecycle.Observe, 0),
	)
	// Another producer's record at c continues pod 0's placement.
	w.write(w.edge(0, "k8sobjects", c, lifecycle.Observe, 0))
	in := w.s.Instrument()
	if err := in.CheckpointEdges(catalog.L2, w.node, store.Reverse, c); err != nil {
		t.Fatal(err)
	}
	if err := in.CheckpointEdges(catalog.L2, w.pods[0], store.Forward, c.Add(-time.Nanosecond)); err != nil {
		t.Fatal(err)
	}
	for _, p := range []bool{false, true} {
		o := store.SinceOptions{Payloads: p}
		ans := w.same([]identity.Fingerprint{w.node}, store.Reverse, c, c.Add(time.Hour), store.Current(catalog.L2), o)
		if got, want := w.seedProducers(ans[0]), []string{"0@kubelet", "1@k8sobjects"}; !slices.Equal(got, want) {
			t.Fatalf("the seeds at the checkpoint's instant are %v; want %v", got, want)
		}
		steps := rec.Counter("read.seed_payload_steps")
		ans = w.same([]identity.Fingerprint{w.pods[0]}, store.Forward, c, c.Add(time.Hour), store.Current(catalog.L2), o)
		if got, want := w.seedProducers(ans[0]), []string{"0@kubelet"}; !slices.Equal(got, want) {
			t.Fatalf("the seeds after the checkpoint a nanosecond earlier are %v; want %v", got, want)
		}
		if p && rec.Counter("read.seed_payload_steps") != steps+1 {
			t.Errorf("the payload of the seed the checkpoint decided took %d seeks; want 1", rec.Counter("read.seed_payload_steps")-steps)
		}
	}
	// The node's walk started past the checkpoint at c and stepped its two records
	// to the end of the prefix, decoding no stamp; the pod's stopped at its
	// checkpoint without stepping a record.
	stepped, stamps := rec.samples["read.seed_records_stepped"], rec.samples["read.seed_stamps_decoded"]
	if !slices.Equal(stepped, []int64{2, 0, 2, 0}) || !slices.Equal(stamps, []int64{0, 1, 0, 1}) {
		t.Errorf("the seed walks stepped %v records and decoded %v stamps; want [2 0 2 0] and [0 1 0 1]", stepped, stamps)
	}
}

// In a Background store whose pass has not reached a prefix, the records below the
// horizon are still there. The seed walk judges them as the baseline the pass will
// write does, and gives no payload before the horizon, so the answers before and
// after the pass are the same, and the reference's.
func TestTheSeedsDoNotDependOnHowFarARetentionHasGot(t *testing.T) {
	t.Parallel()
	p := newParker(t, 1)
	o := bgOptions(vfs.NewMem(), off())
	o.beforeRetainChunk = p.hook
	w := newSinceWorld(t, mustOpen(t, o))
	t.Cleanup(p.release) // before the store's Close, which waits for the pass
	h := t0.Add(3 * time.Minute)
	w.write(
		w.edge(0, "kubelet", t0, lifecycle.Observe, 10*time.Minute),
		w.edge(1, "kubelet", t0, lifecycle.Observe, 3*time.Minute), // its deadline is h
		w.edge(2, "k8sobjects", t0, lifecycle.Observe, 0),
	)
	w.write(w.edge(2, "k8sobjects", h.Add(time.Minute), lifecycle.Observe, 0))
	w.retain(h)
	p.wait(t)
	ask := func() [][]store.Since {
		var out [][]store.Since
		for _, from := range []time.Time{h, h.Add(30 * time.Second), h.Add(2 * time.Minute)} {
			for _, dir := range []store.Direction{store.Reverse, store.Forward} {
				fps := []identity.Fingerprint{w.node}
				if dir == store.Forward {
					fps = w.pods
				}
				out = append(out, w.same(fps, dir, from, from.Add(time.Hour), store.Current(catalog.L2), store.SinceOptions{Payloads: true}))
			}
		}
		return out
	}
	before := ask()
	if n := countKeys(t, w.s, w.prefix(store.Reverse)); n < 4 {
		t.Fatalf("the node's prefix holds %d keys while the pass is held; want its four records", n)
	}
	p.release()
	waitRetained(t, w.s)
	if after := ask(); !reflect.DeepEqual(before, after) {
		t.Errorf("the answers before the pass\n%+v\ndiffer from those after it\n%+v", before, after)
	}
}

func (w *sinceWorld) prefix(dir store.Direction) []byte {
	p, ok := w.s.prefixOf(catalog.L2, w.node, byte(dir))
	if !ok {
		w.t.Fatal("no prefix")
	}
	return p
}

// A key of a kind no read knows fails the seed walk as it fails every read that
// meets it: the walk uses parseKey, as decide does. Here the window stops at the
// record above the key, and the walk reaches it.
func TestTheSeedWalkFailsOnAKeyOfAnUnknownKind(t *testing.T) {
	t.Parallel()
	w := newSinceWorld(t, openMem(t, Options{Checkpoints: off()}))
	w.write(w.edge(0, "k8sobjects", t0.Add(time.Minute), lifecycle.Observe, 0))
	prefix := w.prefix(store.Reverse)
	key := binary.BigEndian.AppendUint64(slices.Clone(prefix), invert(uint64(t0.UnixNano())))
	key = append(binary.BigEndian.AppendUint64(key, invert(0)), 3)
	if err := w.s.kv.Set(key, []byte{1}, pebble.Sync); err != nil {
		t.Fatal(err)
	}
	from := t0.Add(2 * time.Minute)
	sc := store.Current(catalog.L2)
	if _, err := w.s.Window(bg, w.node, store.Reverse, from, from.Add(time.Hour), sc); err != nil {
		t.Fatalf("the window, which stops above the key, failed: %v", err)
	}
	if _, err := w.s.Neighbors(bg, w.node, store.Reverse, from, sc); err == nil {
		t.Fatal("Neighbors walked over a key of kind 3")
	}
	got, err := w.s.WindowSince(bg, []identity.Fingerprint{w.node}, store.Reverse, from, from.Add(time.Hour), sc, store.SinceOptions{})
	if err == nil || got != nil {
		t.Fatalf("WindowSince over a key of kind 3 = %v, %v; want a failure", got, err)
	}
}

// One call is one iterator, however many fingerprints it reads, so it is one
// snapshot; and each prefix with a known type gets one seed walk.
func TestWindowSinceIsOneIteratorPerCall(t *testing.T) {
	t.Parallel()
	rec := newMemRecorder()
	w := newSinceWorld(t, openMem(t, Options{Recorder: rec}))
	for i := range w.pods {
		w.write(w.edge(i, "k8sobjects", t0.Add(time.Duration(i)*time.Second), lifecycle.Observe, 0))
	}
	fps := append(slices.Clone(w.pods), w.node, w.pods[0])
	for range 3 {
		w.same(fps, store.Forward, t0.Add(time.Minute), t0.Add(time.Hour), store.Current(catalog.L2), store.SinceOptions{Payloads: true})
	}
	if got := rec.Counter("read.since.reads"); got != 3 {
		t.Errorf("three calls opened %d iterators; want 3", got)
	}
	if got := len(rec.samples["read.seed_records_stepped"]); got != 3*len(fps) {
		t.Errorf("three calls of %d fingerprints made %d seed walks; want %d", len(fps), got, 3*len(fps))
	}
}

// sinceFactory opens a store for the WindowSince checks with a checkpoint policy and
// a recorder.
func sinceFactory(ck CheckpointOptions, rec Recorder) storetest.Factory {
	return storetest.Factory{Open: func(_ string, p lifecycle.Policy) (store.Store, error) {
		o := Options{Checkpoints: &ck, Recorder: rec}
		o.Policy = p
		o.Tuning = pebblekv.TinyTuning()
		o.FS = vfs.NewMem()
		return Open("db", o)
	}}
}

// With a checkpoint after every record, or a minute behind the newest, almost every
// seed walk stops at a checkpoint, and a late record or a retention keeps deleting
// and deriving them: the WindowSince checks pass, and the walks did use
// checkpoints, and stepped over one at from, and decoded baselines.
func TestWindowSinceConformsWithACheckpointAfterEveryRecord(t *testing.T) {
	t.Parallel()
	workloads := storetest.Workloads()
	retains := [][]float64{nil, {0.3, 0.7}}
	if raceEnabled || storetest.Trimmed() {
		workloads, retains = workloads[len(workloads)-1:], retains[1:]
	}
	for _, lag := range []time.Duration{0, time.Minute} {
		t.Run(fmt.Sprintf("lag %s", lag), func(t *testing.T) {
			t.Parallel()
			rec := newLockedSampleRecorder()
			f := sinceFactory(stress(lag), rec)
			for _, w := range workloads {
				for _, retain := range retains {
					s, err := f.Open("", w.Policy)
					if err != nil {
						t.Fatal(err)
					}
					err = storetest.CheckSince(s, w, storetest.Options{RetainAt: retain, Entities: 6, Probes: 4})
					_ = s.Close()
					if err != nil {
						t.Fatalf("workload %q retain %v: %v", w.Name, retain, err)
					}
				}
			}
			s, err := f.Open("", lifecycle.Policy{})
			if err != nil {
				t.Fatal(err)
			}
			if err := storetest.CheckSinceContract(s); err != nil {
				t.Fatal(err)
			}
			stamps := rec.sampled("read.seed_stamps_decoded")
			if rec.Counter("read.checkpoint_hits") == 0 || !slices.Contains(stamps, 1) || rec.Counter("read.baseline_entries_decoded") == 0 {
				t.Errorf("the seed walks used %d checkpoints and decoded %d baseline entries", rec.Counter("read.checkpoint_hits"), rec.Counter("read.baseline_entries_decoded"))
			}
		})
	}
}

// lockedSampleRecorder keeps counters and samples, safe for a store's goroutines.
type lockedSampleRecorder struct {
	lockedRecorder
	samples map[string][]int64
}

func newLockedSampleRecorder() *lockedSampleRecorder {
	return &lockedSampleRecorder{lockedRecorder: lockedRecorder{counters: map[string]int64{}}, samples: map[string][]int64{}}
}

func (r *lockedSampleRecorder) Sample(name string, v int64) {
	r.mu.Lock()
	r.samples[name] = append(r.samples[name], v)
	r.mu.Unlock()
}

func (r *lockedSampleRecorder) sampled(name string) []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.samples[name])
}
