package memstore_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// The tests in this file are for the retention offsets and kept layers of the
// reference, which the conformance suite does not exercise: it opens every store
// with all offsets zero.

// layerTopology has one edge in each layer, each from a pod of its own to one node.
type layerTopology struct {
	pods map[catalog.Layer]identity.Fingerprint
	node identity.Fingerprint
}

var layers = []catalog.Layer{catalog.L0, catalog.L1, catalog.L2, catalog.L3}

func newLayerTopology(t testing.TB) layerTopology {
	lt := layerTopology{pods: map[catalog.Layer]identity.Fingerprint{}, node: fp(t, catalog.K8sNode, catalog.K8sNodeUID, "n1")}
	for _, l := range layers {
		lt.pods[l] = fp(t, catalog.K8sPod, catalog.K8sPodUID, "pod-"+l.String())
	}
	return lt
}

// edge is a record of the layer's edge, observed at base+d.
func (lt layerTopology) edge(l catalog.Layer, seq uint64, d time.Duration) store.Record {
	return store.Record{
		Layer: l, Subject: store.EdgeSubject(lt.pods[l], lt.node, catalog.ScheduledOn), Producer: "k8s",
		EventTime: at(d), Seq: seq, Kind: lifecycle.Observe, Payload: []byte{byte(seq)},
	}
}

// populateLayers writes, with seqs 1 to 4, one record in each layer at base.
func populateLayers(t testing.TB, s *memstore.Store, lt layerTopology) {
	t.Helper()
	for i, l := range layers {
		write(t, s, lt.edge(l, uint64(i+1), 0))
	}
}

// layerArgs are arguments every read accepts, in layer l, at instant d after base
// (for the windows, from d to an hour after it), as of the token asOf.
func layerArgs(lt layerTopology, l catalog.Layer, d time.Duration, asOf uint64) callArgs {
	return callArgs{
		fp: lt.pods[l], fps: []identity.Fingerprint{lt.pods[l], lt.node}, dir: store.Forward,
		t: at(d), to: at(d + time.Hour), sc: store.Scope{Layer: l, AsOf: asOf},
	}
}

// wantRead requires every kind of read to be refused for the horizon, or to be
// answered.
func wantRead(t *testing.T, s *memstore.Store, what string, a callArgs, refused bool) {
	t.Helper()
	for _, r := range reads {
		_, err := r.call(context.Background(), s, a)
		switch {
		case refused && !errors.Is(err, store.ErrBeforeHorizon):
			t.Errorf("%s, %s: err = %v, want ErrBeforeHorizon", r.name, what, err)
		case !refused && err != nil:
			t.Errorf("%s, %s: %v", r.name, what, err)
		}
	}
}

func TestOpenRefusesANegativeOffset(t *testing.T) {
	t.Parallel()
	for i, l := range layers {
		var o memstore.Options
		o.Offsets[i] = -time.Nanosecond
		if _, err := memstore.Open(o); !errors.Is(err, store.ErrInvalid) {
			t.Errorf("Open with a negative offset in layer %s: err = %v, want ErrInvalid", l, err)
		}
	}
	// A negative offset is refused whether or not the layer is kept; a zero or
	// positive one is not.
	var o memstore.Options
	o.Offsets[1], o.Keep[1] = -time.Second, true
	if _, err := memstore.Open(o); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("Open with a negative offset in a kept layer: err = %v, want ErrInvalid", err)
	}
	s := open(t, memstore.Options{Offsets: [4]time.Duration{0, time.Nanosecond, time.Hour, 0}})
	if got := s.LayerHorizon(catalog.L0); !got.IsZero() {
		t.Errorf("a new store has the horizon %v in L0", got)
	}
}

func TestWithoutOffsetsEveryLayerHasTheHorizon(t *testing.T) {
	t.Parallel()
	lt := newLayerTopology(t)
	s := open(t)
	populateLayers(t, s, lt)
	h := at(5 * time.Minute)
	if err := s.Retain(context.Background(), h); err != nil {
		t.Fatal(err)
	}
	want := store.Horizon{Time: h, Seq: 4}
	if got := s.Horizon(); got != want {
		t.Errorf("Horizon() = %+v, want %+v", got, want)
	}
	for _, l := range layers {
		if got := s.LayerHorizon(l); got != want {
			t.Errorf("LayerHorizon(%s) = %+v, want %+v", l, got, want)
		}
	}
	for _, l := range []catalog.Layer{0, catalog.L3 + 1, 255} {
		if got := s.LayerHorizon(l); !got.IsZero() {
			t.Errorf("LayerHorizon(%s) = %+v, want the zero horizon", l, got)
		}
	}
}

// With offsets, a Retain puts each layer's horizon at the instant less its offset.
// The offsets here are 0, 20, 40 and 5 minutes for L0 to L3.
var offsetsOpts = memstore.Options{Offsets: [4]time.Duration{0, 20 * time.Minute, 40 * time.Minute, 5 * time.Minute}}

func TestRetainSetsEachLayersHorizonByItsOffset(t *testing.T) {
	t.Parallel()
	lt := newLayerTopology(t)
	s := open(t, offsetsOpts)
	ctx := context.Background()
	populateLayers(t, s, lt)
	if err := s.Retain(ctx, at(time.Hour)); err != nil {
		t.Fatal(err)
	}
	for l, d := range map[catalog.Layer]time.Duration{
		catalog.L0: time.Hour, catalog.L1: 40 * time.Minute, catalog.L2: 20 * time.Minute, catalog.L3: 55 * time.Minute,
	} {
		want := store.Horizon{Time: at(d), Seq: 4}
		got := s.LayerHorizon(l)
		if got != want || got.Time.Location() != time.UTC {
			t.Errorf("LayerHorizon(%s) = %+v, want %+v in UTC", l, got, want)
		}
	}
	// Horizon is the latest of the horizons the Retain moved, which is L0's.
	if got, want := s.Horizon(), (store.Horizon{Time: at(time.Hour), Seq: 4}); got != want {
		t.Errorf("Horizon() = %+v, want L0's %+v", got, want)
	}

	// A later write, then a later Retain: every layer moves, with the new LastSeq.
	write(t, s, lt.edge(catalog.L2, 5, time.Hour))
	if err := s.Retain(ctx, at(90*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if got, want := s.LayerHorizon(catalog.L2), (store.Horizon{Time: at(50 * time.Minute), Seq: 5}); got != want {
		t.Errorf("LayerHorizon(L2) = %+v after the second Retain, want %+v", got, want)
	}
	if got, want := s.Horizon(), (store.Horizon{Time: at(90 * time.Minute), Seq: 5}); got != want {
		t.Errorf("Horizon() = %+v after the second Retain, want %+v", got, want)
	}
}

func TestEachLayerRefusesReadsAndWritesByItsOwnHorizon(t *testing.T) {
	t.Parallel()
	lt := newLayerTopology(t)
	s := open(t, offsetsOpts)
	ctx := context.Background()
	populateLayers(t, s, lt)
	if err := s.Retain(ctx, at(time.Hour)); err != nil {
		t.Fatal(err)
	}
	horizons := map[catalog.Layer]time.Duration{
		catalog.L0: time.Hour, catalog.L1: 40 * time.Minute, catalog.L2: 20 * time.Minute, catalog.L3: 55 * time.Minute,
	}
	for _, l := range layers {
		d := horizons[l]
		wantRead(t, s, l.String()+" a nanosecond before its horizon", layerArgs(lt, l, d-time.Nanosecond, 4), true)
		wantRead(t, s, l.String()+" at its horizon", layerArgs(lt, l, d, 4), false)
		wantRead(t, s, l.String()+" a token below its horizon's Seq", layerArgs(lt, l, d, 3), true)
		wantRead(t, s, l.String()+" its horizon's Seq", layerArgs(lt, l, d, 4), false)
	}
	// The instant L2 may still read is long gone for L0, and the other way round.
	wantRead(t, s, "L2 at the instant L0 has lost", layerArgs(lt, catalog.L2, 30*time.Minute, 4), false)
	wantRead(t, s, "L0 at the instant L2 can still read", layerArgs(lt, catalog.L0, 30*time.Minute, 4), true)

	// A write is refused by the horizon of the record's own layer.
	seq := uint64(5)
	for _, l := range layers {
		d := horizons[l]
		err := s.Write(ctx, []store.Record{lt.edge(l, seq, d-time.Nanosecond)})
		if !errors.Is(err, store.ErrBeforeHorizon) {
			t.Errorf("a record in %s a nanosecond before its horizon: err = %v, want ErrBeforeHorizon", l, err)
		}
		if got := s.LastSeq(); got != 4 {
			t.Errorf("a record in %s refused for the horizon moved LastSeq to %d", l, got)
		}
	}
	for _, l := range layers {
		if err := s.Write(ctx, []store.Record{lt.edge(l, seq, horizons[l])}); err != nil {
			t.Errorf("a record in %s at its horizon: %v", l, err)
		}
		seq++
	}
	// An instant L0 has lost but L2 has not: accepted in L2, refused in L0, and a
	// batch with both is refused whole.
	if err := s.Write(ctx, []store.Record{lt.edge(catalog.L2, seq, 30*time.Minute)}); err != nil {
		t.Errorf("a record in L2 between the horizons: %v", err)
	}
	seq++
	err := s.Write(ctx, []store.Record{lt.edge(catalog.L2, seq, 31*time.Minute), lt.edge(catalog.L0, seq+1, 31*time.Minute)})
	if !errors.Is(err, store.ErrBeforeHorizon) {
		t.Errorf("a batch with a record in L0 between the horizons: err = %v, want ErrBeforeHorizon", err)
	}
	if got := s.LastSeq(); got != seq-1 {
		t.Errorf("a batch refused for the horizon moved LastSeq to %d, want %d", got, seq-1)
	}
}

func TestAKeptLayerAnswersBeforeAnotherLayersHorizon(t *testing.T) {
	t.Parallel()
	lt := newLayerTopology(t)
	opts := offsetsOpts
	opts.Keep[catalog.L1-catalog.L0] = true
	s := open(t, opts)
	ctx := context.Background()
	populateLayers(t, s, lt)
	if err := s.Retain(ctx, at(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if got := s.LayerHorizon(catalog.L1); !got.IsZero() {
		t.Errorf("LayerHorizon of the kept layer = %+v, want the zero horizon", got)
	}

	// L0's horizon is at the hour, with Seq 4. The kept layer answers the instants
	// before it, and tokens below its Seq, which L0 refuses.
	for _, a := range []struct {
		what string
		d    time.Duration
		asOf uint64
	}{
		{"an instant before L0's horizon", 30 * time.Minute, 4},
		{"the first instant", 0, 4},
		{"token 0", 0, 0},
		{"a token below L0's Seq", time.Hour, 3},
	} {
		wantRead(t, s, "the kept layer, "+a.what, layerArgs(lt, catalog.L1, a.d, a.asOf), false)
	}
	wantRead(t, s, "L0 at an instant before its horizon", layerArgs(lt, catalog.L0, 30*time.Minute, 4), true)
	wantRead(t, s, "L0 under a token below its Seq", layerArgs(lt, catalog.L0, time.Hour, 3), true)

	// Nor does a write in the kept layer meet a horizon.
	if err := s.Write(ctx, []store.Record{lt.edge(catalog.L1, 5, 0)}); err != nil {
		t.Errorf("a record in the kept layer at the first instant: %v", err)
	}
	if err := s.Write(ctx, []store.Record{lt.edge(catalog.L0, 6, 0)}); !errors.Is(err, store.ErrBeforeHorizon) {
		t.Errorf("a record in L0 at the first instant: err = %v, want ErrBeforeHorizon", err)
	}

	// Retaining again leaves it alone, and Horizon is that of a layer that moved.
	if err := s.Retain(ctx, at(2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if got := s.LayerHorizon(catalog.L1); !got.IsZero() {
		t.Errorf("LayerHorizon of the kept layer = %+v after a second Retain, want the zero horizon", got)
	}
	if got, want := s.Horizon(), (store.Horizon{Time: at(2 * time.Hour), Seq: 5}); got != want {
		t.Errorf("Horizon() = %+v, want L0's %+v", got, want)
	}
}

func TestHorizonIsTheLatestHorizonTheRetainMoved(t *testing.T) {
	t.Parallel()
	// With offsets of 0, 20, 40 and 5 minutes, a Retain at the hour puts the
	// horizons at 60, 40, 20 and 55 minutes. A kept layer's does not move, so it
	// is not a candidate.
	for _, tt := range []struct {
		name string
		keep [4]bool
		want time.Duration
	}{
		{"no layer kept: L0's", [4]bool{}, time.Hour},
		{"L0 kept: L3's", [4]bool{true, false, false, false}, 55 * time.Minute},
		{"L0 and L3 kept: L1's", [4]bool{true, false, false, true}, 40 * time.Minute},
		{"only L2 retained: L2's", [4]bool{true, true, false, true}, 20 * time.Minute},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			opts := offsetsOpts
			opts.Keep = tt.keep
			s := open(t, opts)
			if err := s.Retain(context.Background(), at(time.Hour)); err != nil {
				t.Fatal(err)
			}
			if got, want := s.Horizon(), (store.Horizon{Time: at(tt.want)}); got != want {
				t.Errorf("Horizon() = %+v, want %+v", got, want)
			}
		})
	}
}

// Horizon follows the layer retained most recently: a Retain that moves no horizon
// leaves it where it was, and one that moves some makes it the latest of those.
func TestHorizonIsUnchangedByARetainThatMovesNothing(t *testing.T) {
	t.Parallel()
	s := open(t, offsetsOpts)
	ctx := context.Background()
	if err := s.Retain(ctx, at(time.Hour)); err != nil {
		t.Fatal(err)
	}
	want := s.Horizon()
	if err := s.Retain(ctx, at(30*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if got := s.Horizon(); got != want {
		t.Errorf("Horizon() = %+v after a Retain that moves nothing, want %+v", got, want)
	}
}

func TestEveryLayerKeptMeansNoHorizon(t *testing.T) {
	t.Parallel()
	lt := newLayerTopology(t)
	s := open(t, memstore.Options{Keep: [4]bool{true, true, true, true}})
	populateLayers(t, s, lt)
	if err := s.Retain(context.Background(), at(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if got := s.Horizon(); !got.IsZero() {
		t.Errorf("Horizon() = %+v, want the zero horizon", got)
	}
	for _, l := range layers {
		wantRead(t, s, l.String()+" long before the Retain", layerArgs(lt, l, -time.Hour, 0), false)
	}
}

func TestARetainNeverMovesAHorizonBackward(t *testing.T) {
	t.Parallel()
	lt := newLayerTopology(t)
	s := open(t, offsetsOpts)
	ctx := context.Background()
	populateLayers(t, s, lt)
	if err := s.Retain(ctx, at(time.Hour)); err != nil {
		t.Fatal(err)
	}
	var first [4]store.Horizon
	for i, l := range layers {
		first[i] = s.LayerHorizon(l)
	}
	firstHorizon := s.Horizon()
	write(t, s, lt.edge(catalog.L2, 5, time.Hour))

	// An instant that puts every layer's horizon, less its offset, at or before
	// where it is moves none of them, raises no Seq and changes Horizon() not at all.
	for _, d := range []time.Duration{time.Hour, 59 * time.Minute, 30 * time.Minute, 0, -time.Hour} {
		if err := s.Retain(ctx, at(d)); err != nil {
			t.Fatal(err)
		}
		for i, l := range layers {
			if got := s.LayerHorizon(l); got != first[i] {
				t.Errorf("after Retain(%s) LayerHorizon(%s) = %+v, want %+v", at(d).Sub(base), l, got, first[i])
			}
		}
		if got := s.Horizon(); got != firstHorizon {
			t.Errorf("after Retain(%s) Horizon() = %+v, want %+v", at(d).Sub(base), got, firstHorizon)
		}
	}

	// An instant a nanosecond past L0's horizon, whose offset is zero, is past every
	// layer's, so each moves by the same step and takes the new LastSeq.
	if err := s.Retain(ctx, at(time.Hour+time.Nanosecond)); err != nil {
		t.Fatal(err)
	}
	for i, l := range layers {
		got := s.LayerHorizon(l)
		if want := first[i].Time.Add(time.Nanosecond); !got.Time.Equal(want) || got.Seq != 5 {
			t.Errorf("LayerHorizon(%s) = %+v after a Retain a nanosecond later, want %s and Seq 5", l, got, want.Format(time.RFC3339Nano))
		}
	}
}

func TestLayerHorizonsAfterClose(t *testing.T) {
	t.Parallel()
	lt := newLayerTopology(t)
	s, err := memstore.Open(offsetsOpts)
	if err != nil {
		t.Fatal(err)
	}
	populateLayers(t, s, lt)
	if err := s.Retain(context.Background(), at(time.Hour)); err != nil {
		t.Fatal(err)
	}
	var before [4]store.Horizon
	for i, l := range layers {
		before[i] = s.LayerHorizon(l)
		if before[i].IsZero() {
			t.Fatalf("LayerHorizon(%s) is zero after a Retain", l)
		}
	}
	horizon := s.Horizon()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	for i, l := range layers {
		if got := s.LayerHorizon(l); got != before[i] {
			t.Errorf("LayerHorizon(%s) = %+v after Close, want %+v", l, got, before[i])
		}
	}
	if got := s.Horizon(); got != horizon {
		t.Errorf("Horizon() = %+v after Close, want %+v", got, horizon)
	}
}
