package xtmodel

import (
	"fmt"
	"testing"

	"pgregory.net/rapid"
)

// fixedSeeds is how many streams of a fixed seed the deterministic tests run. A claim
// of coverage rests on these, which are the same on every run; the rapid tests widen
// the search and report a failing -rapid.seed.
const fixedSeeds = 60

// seeds is how many fixed streams a test runs: all of them, or under -short (the race
// detector's tier, where each stream costs seconds and none of this starts a goroutine)
// the first few.
func seeds(full int) uint64 {
	if testing.Short() {
		return uint64(min(full, 6))
	}
	return uint64(full)
}

// TestPlannerThenModelEqualsFoldOnFixedStreams writes streams of a fixed seed, with
// late events, shared instants, overridden records, several producers, runs and
// deletes, and retentions at random points, to a model and to the reference engine.
// After every batch the segments the planner wrote are, at the newest system time and
// at the system time of every earlier batch, the existence intervals of
// lifecycle.Fold over the records visible at that batch's token, and every kind of
// read at every instant of interest and at every token is the reference engine's.
func TestPlannerThenModelEqualsFoldOnFixedStreams(t *testing.T) {
	for seed := uint64(1); seed <= seeds(fixedSeeds); seed++ {
		t.Run(fmt.Sprint("seed ", seed), func(t *testing.T) {
			if err := run(New(), genStream(seedDrawer(seed), 25, seed%2 == 0)); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestPlannerThenModelEqualsFold is the same check over streams rapid draws, with
// shrinking.
func TestPlannerThenModelEqualsFold(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		if err := run(New(), genStream(rapidDrawer{t}, 25, true)); err != nil {
			t.Fatal(err)
		}
	})
}
