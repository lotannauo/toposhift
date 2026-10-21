package decay_test

import (
	"math/rand/v2"
	"testing"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

func openMem(t tb) *memstore.Store {
	t.Helper()
	st, err := memstore.Open(memstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	return st
}

// For random sightings of edges (cadences around the extension interval and the
// TTL, silences, late arrivals, spans attested by a translator, a coalescer
// that restarts or crashes), written through ingest's coalescer to memstore,
// the bound derived at a random instant and token is never above the true
// silence, which is computed from the raw sightings the token knows of, and
// falls short of it by at most the extension interval.
func TestSoundnessAndTightness(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		d := rapidDrawer{rt}
		w := drawWorld(d, false)
		st := openMem(rt)
		defer func() { _ = st.Close() }()
		r := build(rt, w, []store.Store{st}, nil)
		r.query(rt, d, 8)
	})
}

// The same over worlds from fixed seeds, so that the claims about what the
// generator covers are the same on every run.
func TestSoundnessAndTightnessOnFixedWorlds(t *testing.T) {
	worlds := 400
	if raceEnabled {
		worlds = 80
	}
	var total tally
	for seed := range worlds {
		d := randDrawer{rand.New(rand.NewPCG(uint64(seed), 21))}
		st := openMem(t)
		r := build(t, drawWorld(d, false), []store.Store{st}, nil)
		tl := r.query(t, d, 16)
		total.add(tl)
		_ = st.Close()
	}
	t.Logf("%d worlds, %d queries: %d with evidence, %d with a positive bound (%d of them exactly the true silence less X); the largest shortfall was %.3f X, and %d queries were within a hundredth of X",
		total.worlds, total.queries, total.withEvidence, total.positive, total.exact, total.worst, total.atX)
	if total.withEvidence < total.queries/3 || total.positive < total.queries/20 || total.exact < total.positive/10 || total.worst < 0.9 {
		t.Errorf("the generator is too thin: %+v", total)
	}
}
