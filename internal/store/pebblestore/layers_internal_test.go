package pebblestore

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/pebblekv"
)

// The value at the key "horizon/last" is a stored format, and what a store reports
// as its Horizon from it and from the layers' horizons is decided by the rule of
// lastHorizon. The vectors in testdata were computed independently of the code.
type horizonLastVectorFile struct {
	Key     string `json:"key"`
	Vectors []struct {
		Name   string `json:"name"`
		Layers []*struct {
			Horizon string `json:"horizon"`
			Seq     uint64 `json:"seq"`
			Encoded string `json:"encoded"`
		} `json:"layers"`
		Last *string `json:"last"`
		Want struct {
			Horizon string `json:"horizon"`
			Seq     uint64 `json:"seq"`
		} `json:"want"`
	} `json:"vectors"`
}

func TestHorizonLastGoldenVectors(t *testing.T) {
	t.Parallel()
	raw, err := os.ReadFile("testdata/meta_horizon_last_v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var f horizonLastVectorFile
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	if got := hex.EncodeToString(metaKey(metaHorizonLast)); got != f.Key {
		t.Errorf("the key of the last horizon is %s, the vectors say %s", got, f.Key)
	}
	if len(f.Vectors) < 12 {
		t.Fatalf("%d vectors", len(f.Vectors))
	}
	for _, v := range f.Vectors {
		t.Run(v.Name, func(t *testing.T) {
			if len(v.Layers) != layers {
				t.Fatalf("%d layers", len(v.Layers))
			}
			var hs [layers]store.Horizon
			for i, l := range v.Layers {
				if l == nil {
					continue // never retained: no key, the zero horizon
				}
				h, err := time.Parse(time.RFC3339Nano, l.Horizon)
				if err != nil {
					t.Fatal(err)
				}
				enc, err := pebblekv.EncodeLayerHorizon(h, l.Seq)
				if err != nil || hex.EncodeToString(enc) != l.Encoded {
					t.Fatalf("layer %d encodes as %x, %v; the vectors say %s", i, enc, err, l.Encoded)
				}
				hs[i] = store.Horizon{Time: h, Seq: l.Seq}
			}
			var last []byte
			if v.Last != nil {
				if last, err = hex.DecodeString(*v.Last); err != nil {
					t.Fatal(err)
				}
				if last == nil {
					last = []byte{} // an empty value is a value, not an absent key
				}
			}
			want, err := time.Parse(time.RFC3339Nano, v.Want.Horizon)
			if err != nil {
				t.Fatal(err)
			}
			if got := lastHorizon(last, hs); !got.Time.Equal(want) || got.Seq != v.Want.Seq {
				t.Errorf("Horizon = {%v, %d}, want {%v, %d}", got.Time, got.Seq, want, v.Want.Seq)
			}
		})
	}
}

// ---------------------------------------------------------------------------

// layerOffsets are the offsets and kept layers the tests below open stores with:
// L0 is kept, L1 is behind by 20 seconds, L2 follows the horizon and L3 is 10
// seconds behind.
var (
	testOffsets = [4]time.Duration{5 * time.Second, 20 * time.Second, 0, 10 * time.Second}
	testKeep    = [4]bool{true, false, false, false}
)

// layerKeys are the data keys of one layer, in key order.
func layerKeys(t *testing.T, s *Store, l catalog.Layer) [][2]string {
	t.Helper()
	var out [][2]string
	for _, kv := range dataBytes(t, s) {
		if kv[0][0] == pebblekv.LayerByte(l) {
			out = append(out, kv)
		}
	}
	return out
}

// prefixesIn counts the prefixes that hold keys in the given layers.
func prefixesIn(t *testing.T, s *Store, ls ...catalog.Layer) int64 {
	t.Helper()
	var n int64
	for p := range byPrefix(t, s) {
		for _, l := range ls {
			if p[0] == pebblekv.LayerByte(l) {
				n++
			}
		}
	}
	return n
}

func layerOptions(fs vfs.FS, offsets [4]time.Duration, keep [4]bool) Options {
	o := chunkOptions(fs, policy(CheckpointOptions{On: true, KMin: 6, Alpha: 1}))
	o.Offsets, o.Keep = offsets, keep
	return o
}

// Every layer a Retain moves is rewritten at its own horizon, and nothing else is:
// the layer's keys are what a store whose layers all share that horizon leaves, and
// a kept layer's are as they were. The commit that publishes the horizons holds the
// horizon of each layer that moved and no other, the last horizon, and a marker that
// names those layers with their own horizons.
func TestEachMovedLayerIsRewrittenAtItsOwnHorizon(t *testing.T) {
	t.Parallel()
	cs := newChunkStream(t, 90*time.Second)
	moving := []catalog.Layer{catalog.L1, catalog.L2, catalog.L3}

	var s *Store
	type commit struct {
		horizons map[catalog.Layer]store.Horizon
		last     store.Horizon
		marker   *retainMarker
	}
	var commits []commit
	rec := newMemRecorder()
	o := layerOptions(vfs.NewMem(), testOffsets, testKeep)
	o.Recorder = rec
	o.afterHorizonApply = func() error {
		c := commit{horizons: map[catalog.Layer]store.Horizon{}, marker: readMarker(t, s)}
		for _, l := range []catalog.Layer{catalog.L0, catalog.L1, catalog.L2, catalog.L3} {
			raw, err := s.kv.GetMeta(metaKey(pebblekv.HorizonMetaName(l)))
			if err != nil {
				t.Error(err)
			}
			if raw == nil {
				continue
			}
			h, seq, err := pebblekv.DecodeLayerHorizon(raw)
			if err != nil {
				t.Error(err)
			}
			c.horizons[l] = store.Horizon{Time: h, Seq: seq}
		}
		raw, err := s.kv.GetMeta(metaKey(metaHorizonLast))
		if err != nil {
			t.Error(err)
		}
		h, seq, err := pebblekv.DecodeLayerHorizon(raw)
		if err != nil || raw == nil {
			t.Errorf("the key of the last horizon was %x, %v", raw, err)
		}
		c.last = store.Horizon{Time: h, Seq: seq}
		commits = append(commits, c)
		return nil
	}
	s = mustOpen(t, o)
	cs.writeTo(t, s)
	last := s.LastSeq()
	before := map[catalog.Layer][][2]string{}
	for _, l := range []catalog.Layer{catalog.L0, catalog.L1, catalog.L2, catalog.L3} {
		before[l] = layerKeys(t, s, l)
	}
	for _, l := range moving {
		if len(before[l]) == 0 {
			t.Fatalf("the stream has no record in layer %s", l)
		}
	}
	holding := prefixesIn(t, s, moving...)
	if err := s.Retain(bg, cs.h); err != nil {
		t.Fatal(err)
	}

	// The commit.
	if len(commits) != 1 {
		t.Fatalf("%d commits of horizons", len(commits))
	}
	c := commits[0]
	want := map[catalog.Layer]store.Horizon{
		catalog.L1: {Time: cs.h.Add(-20 * time.Second), Seq: last},
		catalog.L2: {Time: cs.h, Seq: last},
		catalog.L3: {Time: cs.h.Add(-10 * time.Second), Seq: last},
	}
	if len(c.horizons) != len(want) {
		t.Errorf("the commit holds the horizons %v, want those of layers L1, L2 and L3 only", c.horizons)
	}
	for l, w := range want {
		if got := c.horizons[l]; !sameHorizonValue(got, w) {
			t.Errorf("the commit holds %v for %s, want %v", got, l, w)
		}
	}
	if !sameHorizonValue(c.last, want[catalog.L2]) {
		t.Errorf("the commit holds the last horizon %v, want the latest of the three, %v", c.last, want[catalog.L2])
	}
	if c.marker == nil || len(c.marker.layers) != len(moving) {
		t.Fatalf("the marker with the horizons is %+v", c.marker)
	}
	for i, l := range c.marker.layers {
		if l.layer != moving[i] || !l.horizon.Equal(want[l.layer].Time) || l.last != last {
			t.Errorf("layer %d of the marker is %+v, want %s at %v", i, l, moving[i], want[moving[i]])
		}
	}

	// The state: the horizons, Horizon, and what each layer holds.
	for _, l := range moving {
		if got := s.LayerHorizon(l); !sameHorizonValue(got, want[l]) {
			t.Errorf("LayerHorizon(%s) = %v, want %v", l, got, want[l])
		}
	}
	if got := s.LayerHorizon(catalog.L0); !got.IsZero() {
		t.Errorf("the kept layer has the horizon %v", got)
	}
	if got := s.Horizon(); !sameHorizonValue(got, want[catalog.L2]) {
		t.Errorf("Horizon = %v, want %v", got, want[catalog.L2])
	}
	if !slices.Equal(layerKeys(t, s, catalog.L0), before[catalog.L0]) {
		t.Error("the kept layer was rewritten")
	}
	if got := rec.counters["retain.prefixes_visited"]; got != holding {
		t.Errorf("%d prefixes visited, the moving layers held %d", got, holding)
	}
	for _, l := range moving {
		// A store whose layers all share this layer's horizon leaves this layer the same.
		same := mustOpen(t, chunkOptions(vfs.NewMem(), policy(CheckpointOptions{On: true, KMin: 6, Alpha: 1})))
		cs.writeTo(t, same)
		if err := same.Retain(bg, want[l].Time); err != nil {
			t.Fatal(err)
		}
		if got, wantKeys := layerKeys(t, s, l), layerKeys(t, same, l); !slices.Equal(got, wantKeys) {
			t.Errorf("layer %s holds %d keys that differ from the %d a store retained at %v leaves", l, len(got), len(wantKeys), want[l].Time)
		}
		if slices.Equal(layerKeys(t, s, l), before[l]) {
			t.Errorf("layer %s was not rewritten", l)
		}
	}
	if m := readMarker(t, s); m != nil {
		t.Errorf("the finished retention left the marker %+v", m)
	}

	// A second Retain: the same, and it survives a reopening.
	later := cs.h.Add(30 * time.Second)
	if err := s.Retain(bg, later); err != nil {
		t.Fatal(err)
	}
	for _, l := range moving {
		if got, w := s.LayerHorizon(l).Time, later.Add(-testOffsets[l-catalog.L0]); !got.Equal(w) {
			t.Errorf("after the second Retain LayerHorizon(%s) = %v, want %v", l, got, w)
		}
	}
	if !slices.Equal(layerKeys(t, s, catalog.L0), before[catalog.L0]) {
		t.Error("the second Retain rewrote the kept layer")
	}
	fs := s.kv.Config().FS
	wantHorizon := s.Horizon()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	r := mustOpen(t, layerOptions(fs, [4]time.Duration{}, [4]bool{}))
	if got := r.Horizon(); !sameHorizonValue(got, wantHorizon) || !got.Time.Equal(later) {
		t.Errorf("after a reopening Horizon = %v, want %v", got, wantHorizon)
	}
}

func sameHorizonValue(a, b store.Horizon) bool { return a.Time.Equal(b.Time) && a.Seq == b.Seq }

// A layer a Retain moves to a horizon that rewrites nothing (here, one before the
// first instant) has its horizon published and is not in the marker, so the pass
// does not visit it and the data of that layer is as it was.
func TestAMovedLayerThatRewritesNothingIsNotInTheMarker(t *testing.T) {
	t.Parallel()
	cs := newChunkStream(t, 90*time.Second)
	var s *Store
	var markers []*retainMarker
	rec := newMemRecorder()
	o := layerOptions(vfs.NewMem(), [4]time.Duration{0, 100 * 365 * 24 * time.Hour, 0, 0}, [4]bool{false, false, true, false})
	o.Recorder = rec
	o.afterHorizonApply = func() error { markers = append(markers, readMarker(t, s)); return nil }
	s = mustOpen(t, o)
	cs.writeTo(t, s)
	l1 := layerKeys(t, s, catalog.L1)
	holding := prefixesIn(t, s, catalog.L0, catalog.L3)
	if err := s.Retain(bg, cs.h); err != nil {
		t.Fatal(err)
	}
	if len(markers) != 1 || markers[0] == nil || len(markers[0].layers) != 2 ||
		markers[0].layers[0].layer != catalog.L0 || markers[0].layers[1].layer != catalog.L3 {
		t.Fatalf("the marker names %+v, want layers L0 and L3 only", markers)
	}
	if got, want := s.LayerHorizon(catalog.L1), cs.h.Add(-100*365*24*time.Hour); !got.Time.Equal(want) || got.Seq != s.LastSeq() || !got.Time.Before(store.MinEventTime) {
		t.Errorf("LayerHorizon(L1) = %v, want {%v, %d}, a horizon before the range", got, want, s.LastSeq())
	}
	if !slices.Equal(layerKeys(t, s, catalog.L1), l1) {
		t.Error("a layer moved to a horizon before the range was rewritten")
	}
	if got := rec.counters["retain.prefixes_visited"]; got != holding {
		t.Errorf("%d prefixes visited, layers L0 and L3 held %d", got, holding)
	}
	if got := s.LayerHorizon(catalog.L2); !got.IsZero() {
		t.Errorf("the kept layer has the horizon %v", got)
	}
}

// A retention that stops after some chunks, with a marker that names two of the four
// layers at horizons of their own, is finished by the next Open, and only in those
// layers, whatever offsets and kept layers it is opened with.
func TestAnInterruptedRetentionOfTwoLayersResumesInThoseLayers(t *testing.T) {
	t.Parallel()
	cs := newChunkStream(t, 90*time.Second)
	offsets := [4]time.Duration{0, 15 * time.Second, 0, 0}
	keep := [4]bool{true, false, false, true}
	build := func(fs vfs.FS, stop int, rec Recorder) *Store {
		o := layerOptions(fs, offsets, keep)
		o.retainBatchBytes, o.retainChunkTime, o.retainStopAfter = 1500, time.Hour, stop
		o.Recorder = rec
		s, err := Open("db", o)
		if err != nil {
			t.Fatal(err)
		}
		cs.writeTo(t, s)
		return s
	}
	recWhole := newMemRecorder()
	whole := build(vfs.NewMem(), 0, recWhole)
	untouched := map[catalog.Layer][][2]string{
		catalog.L0: layerKeys(t, whole, catalog.L0), catalog.L3: layerKeys(t, whole, catalog.L3),
	}
	if err := whole.Retain(bg, cs.h); err != nil {
		t.Fatal(err)
	}
	n, want := int(recWhole.samples["retain.chunks"][0]), dataBytes(t, whole)
	wantHorizons := map[catalog.Layer]store.Horizon{}
	for _, l := range []catalog.Layer{catalog.L0, catalog.L1, catalog.L2, catalog.L3} {
		wantHorizons[l] = whole.LayerHorizon(l)
	}
	_ = whole.Close()
	if n < 6 {
		t.Fatalf("%d chunks", n)
	}
	for _, stop := range []int{1, n / 2, n - 1} {
		t.Run(fmt.Sprintf("stopped after chunk %d of %d", stop, n), func(t *testing.T) {
			t.Parallel()
			fs := vfs.NewMem()
			s := build(fs, stop, nil)
			if err := s.Retain(bg, cs.h); !errors.Is(err, errInjected) {
				t.Fatalf("Retain = %v", err)
			}
			m := readMarker(t, s)
			if m == nil || len(m.layers) != 2 || m.layers[0].layer != catalog.L1 || m.layers[1].layer != catalog.L2 ||
				!m.layers[0].horizon.Equal(cs.h.Add(-15*time.Second)) || !m.layers[1].horizon.Equal(cs.h) {
				t.Fatalf("the marker is %+v, want layers L1 and L2 at their own horizons", m)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			// Opened with no offsets and no kept layer: the marker decides what is finished.
			rec := newMemRecorder()
			o := layerOptions(fs, [4]time.Duration{}, [4]bool{})
			o.retainBatchBytes, o.retainChunkTime = 1500, time.Hour
			o.Recorder = rec
			r, err := Open("db", o)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = r.Close() }()
			if rec.counters["retain.resumed"] != 1 || readMarker(t, r) != nil {
				t.Errorf("resumed %d times, marker %+v", rec.counters["retain.resumed"], readMarker(t, r))
			}
			if !slices.Equal(dataBytes(t, r), want) {
				t.Error("the data is not what an uninterrupted retention leaves")
			}
			for _, l := range []catalog.Layer{catalog.L0, catalog.L3} {
				if !slices.Equal(layerKeys(t, r, l), untouched[l]) {
					t.Errorf("the resumed retention changed layer %s", l)
				}
			}
			for l, h := range wantHorizons {
				if got := r.LayerHorizon(l); !sameHorizonValue(got, h) {
					t.Errorf("LayerHorizon(%s) = %v, want %v", l, got, h)
				}
			}
			if got, max := rec.counters["retain.prefixes_visited"], prefixesIn(t, r, catalog.L1, catalog.L2); got > max {
				t.Errorf("the resumed retention visited %d prefixes, layers L1 and L2 hold %d", got, max)
			}
		})
	}
}

// The writer's memory after a Retain that leaves layers alone: the prefixes of those
// layers keep the states they had (the very values), the prefixes of the layers it
// rewrote are what a whole read finds, and the map is complete only if it was.
func TestTheWritersMemoryOfAnUnmovedLayerSurvivesARetain(t *testing.T) {
	t.Parallel()
	cs := newChunkStream(t, 90*time.Second)
	for _, tc := range []struct {
		name         string
		reopen       bool
		wantComplete bool
	}{
		{"a complete map stays complete", false, true},
		{"a map that was not complete is not", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fs := vfs.NewMem()
			rec := newMemRecorder()
			o := layerOptions(fs, testOffsets, testKeep)
			o.Recorder = rec
			s := mustOpen(t, o)
			cs.writeTo(t, s)
			if tc.reopen {
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
				s = mustOpen(t, o)
				if s.complete {
					t.Fatal("a database that holds data opens complete")
				}
				// A write to a prefix of the kept layer, which the store then remembers.
				var l0 []byte
				for p := range byPrefix(t, s) {
					if p[0] == pebblekv.LayerByte(catalog.L0) {
						l0 = []byte(p)
						break
					}
				}
				if _, err := s.state(l0); err != nil {
					t.Fatal(err)
				}
			}
			if !s.anyCkpt {
				t.Fatal("no checkpoint was written")
			}
			unmoved := map[string]*prefixState{}
			saved := map[string]prefixState{}
			for k, st := range s.states {
				if layerOfPrefix([]byte(k)) == catalog.L0 {
					unmoved[k], saved[k] = st, *st
				}
			}
			if len(unmoved) == 0 {
				t.Fatal("the writer remembers nothing of the kept layer")
			}
			if err := s.Retain(bg, cs.h); err != nil {
				t.Fatal(err)
			}
			if s.complete != tc.wantComplete {
				t.Errorf("complete = %v after the Retain, want %v", s.complete, tc.wantComplete)
			}
			for k, st := range unmoved {
				if got := s.states[k]; got != st || !reflect.DeepEqual(*got, saved[k]) {
					t.Errorf("what the writer remembered of the prefix %x of the kept layer changed", k)
				}
			}
			requireWholeReads(t, s)
			if rec.counters["retain.state_keys"] == 0 {
				t.Error("the retention worked out nothing of the layers it rewrote")
			}
			// Some prefix of a rewritten layer is remembered.
			var moved int
			for k := range s.states {
				if layerOfPrefix([]byte(k)) != catalog.L0 {
					moved++
				}
			}
			if moved == 0 {
				t.Error("the writer remembers nothing of the layers the retention rewrote")
			}
		})
	}
}

// A Retain whose horizon is past the range rewrites every prefix in a way the keys
// it leaves do not describe, so it works nothing out: the prefixes of the layers it
// rewrote are forgotten, those of a kept layer are remembered still, and the map is
// no longer complete.
func TestARetainPastTheRangeForgetsOnlyTheLayersItRewrote(t *testing.T) {
	t.Parallel()
	cs := newChunkStream(t, 90*time.Second)
	s := mustOpen(t, layerOptions(vfs.NewMem(), testOffsets, testKeep))
	cs.writeTo(t, s)
	if !s.anyCkpt || !s.complete {
		t.Fatalf("anyCkpt %v, complete %v", s.anyCkpt, s.complete)
	}
	kept := map[string]*prefixState{}
	for k, st := range s.states {
		if layerOfPrefix([]byte(k)) == catalog.L0 {
			kept[k] = st
		}
	}
	if err := s.Retain(bg, store.MaxEventTime.Add(24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if s.complete {
		t.Error("the map is complete after a retention that worked nothing out")
	}
	for k, st := range kept {
		if s.states[k] != st {
			t.Errorf("the prefix %x of the kept layer was forgotten", k)
		}
	}
	for k := range s.states {
		if _, ok := kept[k]; !ok {
			t.Errorf("the prefix %x of a layer that was rewritten is remembered", k)
		}
	}
}

// The key "horizon/last" is the Horizon of a store, as long as it names the horizon
// of a layer that has the highest Seq. A Retain by a binary that does not write it
// leaves it behind, and then the Horizon is derived from the layers; the next Retain
// that moves a layer writes it again.
func TestAStaleHorizonLastIsIgnoredAndHealedByTheNextRetain(t *testing.T) {
	t.Parallel()
	cs := newChunkStream(t, 90*time.Second)
	fs := vfs.NewMem()
	s := mustOpen(t, layerOptions(fs, [4]time.Duration{}, [4]bool{}))
	cs.writeTo(t, s)
	h1, h2, h3 := cs.h, cs.h.Add(10*time.Second), cs.h.Add(20*time.Second)
	if err := s.Retain(bg, h1); err != nil {
		t.Fatal(err)
	}
	stale, err := s.kv.GetMeta(metaKey(metaHorizonLast))
	if err != nil || stale == nil {
		t.Fatalf("no last horizon after a Retain: %x, %v", stale, err)
	}
	cs.writeLater(t, s)
	if err := s.Retain(bg, h2); err != nil {
		t.Fatal(err)
	}
	seq2 := s.LastSeq()
	fresh, err := s.kv.GetMeta(metaKey(metaHorizonLast))
	if err != nil || bytes.Equal(fresh, stale) {
		t.Fatalf("the second Retain left the last horizon %x, %v", fresh, err)
	}
	reopen := func() *Store {
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		s = mustOpen(t, layerOptions(fs, [4]time.Duration{}, [4]bool{}))
		return s
	}
	if got := reopen().Horizon(); !got.Time.Equal(h2) || got.Seq != seq2 {
		t.Errorf("Horizon = %v, want {%v, %d}", got, h2, seq2)
	}
	// A binary that does not write the key moved the layers on, and the key is what
	// the Retain before left.
	set(t, s, metaKey(metaHorizonLast), stale)
	if got := reopen().Horizon(); !got.Time.Equal(h2) || got.Seq != seq2 {
		t.Errorf("with a stale key Horizon = %v, want the one derived from the layers, {%v, %d}", got, h2, seq2)
	}
	// So does a database with no such key at all.
	if err := s.kv.Delete(metaKey(metaHorizonLast), s.kv.WriteOptions()); err != nil {
		t.Fatal(err)
	}
	if got := reopen().Horizon(); !got.Time.Equal(h2) || got.Seq != seq2 {
		t.Errorf("with no key Horizon = %v, want {%v, %d}", got, h2, seq2)
	}
	// The next Retain that moves a layer writes it, and the Horizon follows.
	if err := s.Retain(bg, h3); err != nil {
		t.Fatal(err)
	}
	raw, err := s.kv.GetMeta(metaKey(metaHorizonLast))
	if err != nil {
		t.Fatal(err)
	}
	ht, hseq, err := pebblekv.DecodeLayerHorizon(raw)
	if err != nil || !ht.Equal(h3) || hseq != seq2 {
		t.Errorf("the last horizon after the third Retain is {%v, %d}, %v; want {%v, %d}", ht, hseq, err, h3, seq2)
	}
	if got := reopen().Horizon(); !got.Time.Equal(h3) {
		t.Errorf("after healing Horizon = %v", got)
	}
}

// Two Retains with no write between them, the second of which moves a layer to a
// horizon earlier than the first moved another to, leave the horizon of the second
// as the Horizon, before and after a reopening: it is not derivable from the layers.
func TestHorizonIsTheLayerRetainedMostRecentlyEvenWhenItsHorizonIsEarlier(t *testing.T) {
	t.Parallel()
	cs := newChunkStream(t, 90*time.Second)
	fs := vfs.NewMem()
	// First L1, L2 and L3 are retained at h; then the same store is opened with L0 not
	// kept, and a Retain at an earlier horizon moves L0 alone.
	o := layerOptions(fs, [4]time.Duration{}, [4]bool{true, false, false, false})
	s := mustOpen(t, o)
	cs.writeTo(t, s)
	if err := s.Retain(bg, cs.h); err != nil {
		t.Fatal(err)
	}
	last := s.LastSeq()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = mustOpen(t, layerOptions(fs, [4]time.Duration{}, [4]bool{}))
	early := cs.h.Add(-30 * time.Second)
	if err := s.Retain(bg, early); err != nil {
		t.Fatal(err)
	}
	for l, want := range map[catalog.Layer]time.Time{catalog.L0: early, catalog.L1: cs.h, catalog.L2: cs.h, catalog.L3: cs.h} {
		if got := s.LayerHorizon(l); !got.Time.Equal(want) || got.Seq != last {
			t.Errorf("LayerHorizon(%s) = %v, want {%v, %d}", l, got, want, last)
		}
	}
	check := func(s *Store, when string) {
		if got := s.Horizon(); !got.Time.Equal(early) || got.Seq != last {
			t.Errorf("%s Horizon = %v, want {%v, %d}, the horizon of the layer retained most recently", when, got, early, last)
		}
	}
	check(s, "after the Retain")
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	check(mustOpen(t, layerOptions(fs, [4]time.Duration{}, [4]bool{})), "after a reopening")
}

// A negative offset is refused at Open, whether or not the layer is kept.
func TestOpenRefusesANegativeOffset(t *testing.T) {
	t.Parallel()
	for i := range layers {
		for _, kept := range []bool{false, true} {
			o := layerOptions(vfs.NewMem(), [4]time.Duration{}, [4]bool{})
			o.Offsets[i], o.Keep[i] = -time.Nanosecond, kept
			s, err := Open("db", o)
			if err == nil {
				_ = s.Close()
			}
			if !errors.Is(err, store.ErrInvalid) {
				t.Errorf("Open with an offset of -1ns for layer %d (kept %v) = %v, want ErrInvalid", i, kept, err)
			}
		}
	}
}
