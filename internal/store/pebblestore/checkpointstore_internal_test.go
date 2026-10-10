package pebblestore

import (
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
	"github.com/lotannauo/toposhift/internal/store/pebblekv"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

// A nil policy is the default (on, 64, 2, one nanosecond); the way to write none is
// an explicit policy with nothing on; and DefaultOptions carries the default.
func TestTheCheckpointPolicyDefaultsAndItsExplicitOff(t *testing.T) {
	t.Parallel()
	want := CheckpointOptions{On: true, KMin: 64, Alpha: 2, Lag: time.Nanosecond}
	if got := DefaultCheckpoints(); got != want {
		t.Errorf("DefaultCheckpoints = %+v, want %+v", got, want)
	}
	if got := DefaultOptions().Checkpoints; got == nil || *got != want {
		t.Errorf("DefaultOptions().Checkpoints = %v, want %+v", got, want)
	}
	for name, tc := range map[string]struct {
		opts Options
		want CheckpointOptions
	}{
		"nil":      {Options{}, want},
		"explicit": {Options{Checkpoints: policy(CheckpointOptions{On: true, KMin: 3})}, CheckpointOptions{On: true, KMin: 3}},
		"off":      {Options{Checkpoints: off()}, CheckpointOptions{}},
	} {
		s := openMem(t, tc.opts)
		if s.ckpt != tc.want {
			t.Errorf("%s: the store writes checkpoints by %+v, want %+v", name, s.ckpt, tc.want)
		}
	}
	// The store copies the policy: a caller who changes theirs later changes nothing.
	mine := CheckpointOptions{On: true, KMin: 3}
	s := openMem(t, Options{Checkpoints: &mine})
	mine.KMin = 99
	if s.ckpt.KMin != 3 {
		t.Errorf("the store follows its caller's policy: KMin %d", s.ckpt.KMin)
	}
}

// Whether a record commit is synced does not depend on a checkpoint following it:
// the checkpoint is a second commit that is never synced, and the instrument's hook
// is not synced either. The same writes cost the same syncs with the policy on and
// off, and some of them do write checkpoints.
func TestCheckpointCommitsAreNeverSynced(t *testing.T) {
	t.Parallel()
	syncs := func(ck *CheckpointOptions) (writes, hook int64, checkpoints int) {
		fsys := &countingSyncFS{FS: vfs.NewMem()}
		s, err := Open("db", Options{Config: pebblekv.Config{Tuning: pebblekv.TinyTuning(), FS: fsys, Sync: true}, Checkpoints: ck})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = s.Close() }()
		before := fsys.syncs.Load()
		for i := range 6 {
			if err := s.Write(bg, []store.Record{edgeRecord(uint64(i+1), "p", t0.Add(time.Duration(i)*time.Second), lifecycle.Observe, 0)}); err != nil {
				t.Fatal(err)
			}
		}
		writes = fsys.syncs.Load() - before
		before = fsys.syncs.Load()
		if err := s.Instrument().CheckpointEdges(catalog.L2, podFP, store.Forward, t0.Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
		hook = fsys.syncs.Load() - before
		return writes, hook, len(slices.DeleteFunc(dump(t, s), func(x stored) bool { return x.kind != kindCheckpoint }))
	}
	onW, onH, onCkpts := syncs(policy(stress(0)))
	offW, offH, offCkpts := syncs(off())
	if onCkpts < 6 || offCkpts != 1 {
		t.Fatalf("checkpoints on disk: %d with the policy on, %d with it off (only the hook's); want at least 6 and 1", onCkpts, offCkpts)
	}
	if offW < 6 {
		t.Fatalf("six synced Writes cost %d syncs: Sync is not on", offW)
	}
	if onW != offW {
		t.Errorf("six Writes cost %d syncs with checkpoints on and %d with them off: a checkpoint commit is synced", onW, offW)
	}
	if onH != 0 || offH != 0 {
		t.Errorf("the hook's commit cost %d and %d syncs, want none", onH, offH)
	}
}

// A retention that keeps a prefix whole because of a boot in what it would have
// discarded leaves its checkpoints, the one at the horizon included, and what the
// writer remembers of it afterwards is what reading the whole prefix finds. The
// states worked out from the keys at or after the horizon are not that: the
// checkpoint at the horizon is deleted in the one and absent from the other, and
// older keys that stay may hold more.
func TestAPrefixKeptWholeForBootsHasItsOwnStateWorkedOut(t *testing.T) {
	t.Parallel()
	h := t0.Add(10 * time.Minute)
	host := hostObservation(0, t0, "").Subject.A
	rec := newMemRecorder()
	p := newPair(t, Options{Policy: storetest.QuarantinePolicy(), Recorder: rec, Checkpoints: policy(CheckpointOptions{On: true, KMin: 1000})})
	p.write(hostObservation(1, t0, "boot-a"), hostObservation(2, t0.Add(time.Minute), "boot-b"))
	// A checkpoint of the entity's own prefix exactly at the horizon, and another
	// before it.
	for _, c := range []time.Time{t0.Add(5 * time.Minute), h} {
		if err := p.s.Instrument().CheckpointEntity(catalog.L1, host, c); err != nil {
			t.Fatal(err)
		}
	}
	entityPrefix := func() []byte {
		pr, ok := p.s.prefixOf(catalog.L1, host, dirEntity)
		if !ok {
			t.Fatal("no prefix")
		}
		return pr
	}
	p.retain(h)
	if rec.counters["retain.prefixes_kept_for_boots"] != 1 || rec.counters["retain.range_deletes"] != 0 {
		t.Fatalf("the retention did not keep the prefix whole: %v", rec.counters)
	}
	if got := checkpointsOnDisk(t, p.s)[string(entityPrefix())]; !slices.Equal(got, []int64{t0.Add(5 * time.Minute).UnixNano(), h.UnixNano()}) {
		t.Fatalf("the checkpoints on disk after the retention = %v: a prefix kept whole keeps them all", got)
	}
	if !p.s.complete {
		t.Fatal("the map is not complete after a retention that finished")
	}
	requireWholeReads(t, p.s)
	if st := p.s.states[string(entityPrefix())]; st == nil || !slices.Equal(st.ckpts, []int64{t0.Add(5 * time.Minute).UnixNano(), h.UnixNano()}) {
		t.Fatalf("the writer remembers %+v of the prefix; it holds both checkpoints", st)
	}
	// A later record, and one at the horizon's own instant, delete only what they
	// make untrue; the answers are the reference's.
	p.write(hostObservation(3, h.Add(10*time.Minute), "boot-b"), hostObservation(4, h, "boot-b"))
	requireWholeReads(t, p.s)
	sc := store.Current(catalog.L1)
	for _, at := range []time.Time{h, h.Add(time.Minute), h.Add(30 * time.Minute)} {
		got, err := p.s.Alive(bg, host, at, sc)
		want, _ := p.ref.Alive(bg, host, at, sc)
		if err != nil || got != want {
			t.Errorf("Alive at %v = %v, %v; the reference says %v", at, got, err, want)
		}
	}
}

// A retention that leaves a layer alone does not visit its prefixes, so it keeps
// what is remembered of them (a prefix of that layer is still known, and the map is
// as complete as it was), and a late record to the layer it left still deletes the
// checkpoints it makes untrue.
func TestARetentionThatLeavesALayerAloneKeepsWhatIsRememberedOfIt(t *testing.T) {
	t.Parallel()
	early, mid, late := t0.Add(time.Minute), t0.Add(30*time.Minute), t0.Add(time.Hour)
	fs := vfs.NewMem()
	cfg := pebblekv.Config{Tuning: pebblekv.TinyTuning(), FS: fs}
	s, err := Open("db", Options{Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	for l, h := range map[catalog.Layer]store.Horizon{
		catalog.L0: {Time: early}, catalog.L1: {Time: late}, catalog.L2: {Time: early}, catalog.L3: {Time: early},
	} {
		raw, err := pebblekv.EncodeLayerHorizon(h.Time, h.Seq)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.kv.Set(metaKey(pebblekv.HorizonMetaName(l)), raw, pebble.Sync); err != nil {
			t.Fatal(err)
		}
	}
	_ = s.Close()
	rec := newMemRecorder()
	s, err = Open("db", Options{Config: cfg, Recorder: rec, Checkpoints: policy(CheckpointOptions{On: true, KMin: 1000})})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	ref := newRef(t)
	podL1 := fingerprintOf(catalog.K8sPod, 0x41) // a subject is in one layer only
	l1 := func(seq uint64, producer lifecycle.Producer, at time.Time, kind lifecycle.Kind) store.Record {
		r := edgeRecord(seq, producer, at, kind, 0)
		r.Layer, r.Subject = catalog.L1, store.EdgeSubject(podL1, nodeFP, catalog.ScheduledOn)
		return r
	}
	for _, r := range []store.Record{
		l1(1, "p", late.Add(time.Minute), lifecycle.Observe),
		edgeRecord(2, "p", mid.Add(-time.Minute), lifecycle.Observe, 0),
		edgeRecord(3, "q", mid.Add(time.Minute), lifecycle.Observe, 0),
	} {
		for _, x := range []store.Store{s, ref} {
			if err := x.Write(bg, cloneAll([]store.Record{r})); err != nil {
				t.Fatal(err)
			}
		}
	}
	// A checkpoint in each layer, written by the hook: the policy is too slow to.
	if err := s.Instrument().CheckpointEdges(catalog.L1, podL1, store.Forward, late.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := s.Instrument().CheckpointEdges(catalog.L2, podFP, store.Forward, mid.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if !s.complete || !s.anyCkpt {
		t.Fatalf("the writes left complete = %v, anyCkpt = %v", s.complete, s.anyCkpt)
	}
	l1Prefix, _ := s.prefixOf(catalog.L1, podL1, byte(store.Forward))
	if len(checkpointsOnDisk(t, s)[string(l1Prefix)]) != 1 {
		t.Fatal("no checkpoint in L1")
	}
	l1State := s.states[string(l1Prefix)]
	if l1State == nil {
		t.Fatal("the writer does not remember the prefix of L1")
	}
	l1Before := *l1State
	for _, x := range []store.Store{s, ref} {
		if err := x.Retain(bg, mid); err != nil {
			t.Fatal(err)
		}
	}
	if s.LayerHorizon(catalog.L1).Time.Equal(mid) || !s.LayerHorizon(catalog.L2).Time.Equal(mid) {
		t.Fatalf("the retention moved the wrong layers: L1 %v, L2 %v", s.LayerHorizon(catalog.L1), s.LayerHorizon(catalog.L2))
	}
	if got := s.states[string(l1Prefix)]; got != l1State || !reflect.DeepEqual(*got, l1Before) {
		t.Fatalf("the retention changed what is remembered of the layer it left alone: %+v, was %+v", got, l1Before)
	}
	if !s.complete {
		t.Error("the map was complete before a retention that worked out every layer it rewrote, and is not after")
	}
	if rec.counters["retain.state_keys"] == 0 {
		t.Error("a retention that works the state of the layers it rewrites out counted no keys")
	}
	// A late record in L1, before the checkpoint there.
	late1 := l1(4, "p", late.Add(30*time.Second), lifecycle.Delete)
	for _, x := range []store.Store{s, ref} {
		if err := x.Write(bg, cloneAll([]store.Record{late1})); err != nil {
			t.Fatal(err)
		}
	}
	if left := checkpointsOnDisk(t, s)[string(l1Prefix)]; len(left) != 0 {
		t.Errorf("the checkpoints %v, which the late record made untrue, were left in place", left)
	}
	for _, l := range []catalog.Layer{catalog.L1, catalog.L2} {
		at := map[catalog.Layer]time.Time{catalog.L1: late.Add(2 * time.Minute), catalog.L2: mid.Add(2 * time.Minute)}[l]
		sc := store.Current(l)
		for _, dir := range []store.Direction{store.Forward, store.Reverse} {
			e := map[catalog.Layer]identity.Fingerprint{catalog.L1: podL1, catalog.L2: podFP}[l]
			if dir == store.Reverse {
				e = nodeFP
			}
			got, err := s.Neighbors(bg, e, dir, at, sc)
			want, werr := ref.Neighbors(bg, e, dir, at, sc)
			if err != nil || werr != nil || !slices.Equal(got, want) {
				t.Errorf("Neighbors(%s, %s, %v) in %s = %v, %v; the reference says %v, %v", e, dir, at.Sub(t0), l, got, err, want, werr)
			}
		}
	}
}

// checkpointsByDir counts the checkpoints on disk under the entity prefixes (the
// existence of entities) and under the edge prefixes.
func checkpointsByDir(t *testing.T, s *Store) (entity, edge int) {
	t.Helper()
	for prefix, cs := range checkpointsOnDisk(t, s) {
		if prefix[prefixLen-1] == dirEntity {
			entity += len(cs)
		} else {
			edge += len(cs)
		}
	}
	return entity, edge
}

// Under a boot key nothing reads the checkpoints of an entity's own prefix (Alive
// folds the whole prefix), so the policy places none there, and places them on edge
// prefixes as before; with no boot key it places them on both. One that exists is
// still deleted when a record makes it untrue.
func TestNoEntityCheckpointIsPlacedUnderABootKey(t *testing.T) {
	t.Parallel()
	host := hostObservation(0, t0, "").Subject.A
	writeAll := func(p *pair) {
		for i := range 6 {
			p.write(hostObservation(uint64(2*i+1), t0.Add(time.Duration(i)*time.Second), "boot-a"),
				edgeRecord(uint64(2*i+2), "p", t0.Add(time.Duration(i)*time.Second), lifecycle.Observe, 0))
		}
	}
	for name, tc := range map[string]struct {
		policy     lifecycle.Policy
		wantEntity bool
	}{
		"a boot key":  {storetest.QuarantinePolicy(), false},
		"no boot key": {lifecycle.Policy{}, true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			p := newPair(t, Options{Policy: tc.policy, Checkpoints: policy(stress(0))})
			writeAll(p)
			entity, edge := checkpointsByDir(t, p.s)
			if edge == 0 {
				t.Fatal("no checkpoint on an edge prefix: the test shows nothing")
			}
			if (entity > 0) != tc.wantEntity {
				t.Errorf("%d checkpoints on entity prefixes, want some: %v", entity, tc.wantEntity)
			}
		})
	}
	t.Run("one that exists is still deleted", func(t *testing.T) {
		t.Parallel()
		p := newPair(t, Options{Policy: storetest.QuarantinePolicy(), Checkpoints: policy(stress(0))})
		writeAll(p)
		if err := p.s.Instrument().CheckpointEntity(catalog.L1, host, t0.Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
		if entity, _ := checkpointsByDir(t, p.s); entity != 1 {
			t.Fatalf("%d entity checkpoints after the hook, want 1", entity)
		}
		// A record before it, and enough of them to make the policy want a checkpoint.
		for i := range 3 {
			p.write(hostObservation(uint64(20+i), t0.Add(time.Duration(10+i)*time.Second), "boot-a"))
		}
		if entity, _ := checkpointsByDir(t, p.s); entity != 0 {
			t.Errorf("%d entity checkpoints after records that make it untrue, want 0 (and none placed)", entity)
		}
	})
}
