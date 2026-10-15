package storetest_test

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

// restoredWorld is a refreshing stream written to a live store, retained ten
// minutes in, and a copy of it a test changes.
type restoredWorld struct {
	recs    []store.Record
	asked   time.Time
	opts    storetest.RestoredOptions
	offsets memstore.Options
}

func newRestoredWorld(t *testing.T) restoredWorld {
	t.Helper()
	c := storetest.Tiny()
	c.Refresh.On = true
	c.LateProbability = 0
	g := mustGenerator(t, c)
	recs := g.All()
	return restoredWorld{
		recs:    recs,
		asked:   c.Start.Add(10 * time.Minute),
		opts:    storetest.RestoredOptions{Entities: g.Entities(), Written: recs, Span: 6 * time.Minute},
		offsets: memstore.Options{Offsets: [4]time.Duration{0, 5 * time.Minute, 0, 0}, Keep: [4]bool{true, false, false, false}},
	}
}

// open writes recs to a new store and retains it to the world's horizon, unless
// retain is false.
func (w restoredWorld) open(t *testing.T, recs []store.Record, retain bool) *memstore.Store {
	t.Helper()
	s, err := memstore.Open(w.offsets)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if err := s.Write(bg, recs); err != nil {
		t.Fatal(err)
	}
	if retain {
		if err := s.Retain(bg, w.asked); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

func TestCompareRestored(t *testing.T) {
	t.Parallel()
	w := newRestoredWorld(t)
	live := w.open(t, w.recs, true)
	h := live.LayerHorizon(catalog.L2).Time

	// What decides the stream's state at the horizon in L2: the last records of
	// each L2 subject at or before it.
	lastBefore := -1
	for i, r := range w.recs {
		if r.Layer == catalog.L2 && !r.EventTime.After(h) {
			lastBefore = i
		}
	}
	if lastBefore < 0 {
		t.Fatal("no L2 record before the horizon")
	}
	firstAfter := slices.IndexFunc(w.recs, func(r store.Record) bool {
		return r.Layer == catalog.L2 && r.EventTime.After(h) && r.Kind == lifecycle.Observe
	})

	tests := []struct {
		name string
		recs func() []store.Record
		// retain retains the restored store; want is the read the first mismatch is
		// in, "" for none.
		retain bool
		want   string
	}{
		{"the same records", func() []store.Record { return w.recs }, true, ""},
		{"every record before the horizon less a TTL dropped", func() []store.Record {
			return slices.DeleteFunc(slices.Clone(w.recs), func(r store.Record) bool {
				return r.Layer == catalog.L2 && r.EventTime.Before(h.Add(-4*time.Minute))
			})
		}, true, ""},
		{"the records deciding the horizon dropped", func() []store.Record {
			return slices.DeleteFunc(slices.Clone(w.recs), func(r store.Record) bool {
				return r.Layer != catalog.L0 && !r.EventTime.After(h)
			})
		}, true, "Alive or Neighbors"},
		{"another payload after the horizon", func() []store.Record {
			out := slices.Clone(w.recs)
			out[firstAfter].Payload = append(slices.Clone(out[firstAfter].Payload), 'x')
			return out
		}, true, "Window or EntityWindow"},
		{"not retained", func() []store.Record { return w.recs }, false, "horizon"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			restored := w.open(t, tt.recs(), tt.retain)
			err := storetest.CompareRestored(live, restored, w.opts)
			if tt.want == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			var mm *storetest.RestoredMismatch
			if !errors.As(err, &mm) || !errors.Is(err, storetest.ErrMismatch) || mm.Read != tt.want {
				t.Fatalf("CompareRestored = %v, want a mismatch in %s", err, tt.want)
			}
			if lh := live.LayerHorizon(mm.Layer).Time; tt.want == "Alive or Neighbors" && (mm.At.Before(lh) || mm.At.After(lh.Add(w.opts.Span))) {
				t.Fatalf("the first mismatch is at %s, %s after the horizon of %s", mm.At, mm.At.Sub(lh), mm.Layer)
			}
		})
	}
}

// A restored store retains at its own last Seq, above the live store's at its
// retention, and the comparison requires the tokens between them to be refused
// there and answered in the live store.
func TestCompareRestoredTokensBetweenTheHorizons(t *testing.T) {
	t.Parallel()
	w := newRestoredWorld(t)
	// The live store retains halfway, the restored one after everything.
	half := len(w.recs) / 2
	live, err := memstore.Open(w.offsets)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = live.Close() }()
	if err := live.Write(bg, w.recs[:half]); err != nil {
		t.Fatal(err)
	}
	if err := live.Retain(bg, w.asked); err != nil {
		t.Fatal(err)
	}
	var after []store.Record
	for _, r := range w.recs[half:] {
		if !r.EventTime.Before(live.LayerHorizon(r.Layer).Time) {
			after = append(after, r)
		}
	}
	if err := live.Write(bg, after); err != nil {
		t.Fatal(err)
	}
	restored := w.open(t, append(slices.Clone(w.recs[:half]), after...), true)
	o := w.opts
	o.Written = append(slices.Clone(w.recs[:half]), after...)
	if live.LayerHorizon(catalog.L2).Seq >= restored.LayerHorizon(catalog.L2).Seq {
		t.Fatal("the restored store's horizon is not above the live store's")
	}
	if err := storetest.CompareRestored(live, restored, o); err != nil {
		t.Fatal(err)
	}
}
