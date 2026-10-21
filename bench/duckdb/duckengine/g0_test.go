package duckengine

import (
	"context"
	"testing"

	"github.com/lotannauo/toposhift/bench/duckdb/g0"
)

// TestEveryArmAnswersTheTinyPlanExactly is the first gate: each arm's files answer every
// question of the plan as the reference engine did.
func TestEveryArmAnswersTheTinyPlanExactly(t *testing.T) {
	dir, plan, _ := sharedBuild(t)
	for _, arm := range Arms {
		t.Run(string(arm), func(t *testing.T) {
			db := openRead(t, false)
			r, err := OpenReader(context.Background(), db, dir, arm, true)
			mustf(t, err, "opening %s", arm)
			defer r.Close()
			rep, err := g0.Check(context.Background(), plan, r, nil)
			mustf(t, err, "g0")
			if !rep.OK() {
				t.Fatalf("%d of %d answers match; first mismatches: %+v", rep.Matched, rep.Queries, rep.Mismatches)
			}
		})
	}
}
