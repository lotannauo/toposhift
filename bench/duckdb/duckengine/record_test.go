package duckengine

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/runner"
)

// TestRecordProfileFixtures writes the profiles testdata/profile holds, from real reads
// of the tiny plan's files. It does nothing unless DUCKENGINE_RECORD_PROFILES names a
// directory: the fixtures are recorded once and read by the tests, which must not depend
// on DuckDB's timings.
//
//	DUCKENGINE_RECORD_PROFILES=$PWD/testdata/profile go test -run TestRecordProfileFixtures ./duckengine
func TestRecordProfileFixtures(t *testing.T) {
	out := os.Getenv("DUCKENGINE_RECORD_PROFILES")
	if out == "" {
		t.Skip("DUCKENGINE_RECORD_PROFILES is not set")
	}
	dir, plan, _ := sharedBuild(t)
	pick := func(op runner.Op, d engine.Direction, age string) runner.Query {
		for _, q := range plan.Queries {
			if q.Op == op && q.Dir == d && (age == "" || q.Age == age) {
				return q
			}
		}
		t.Fatalf("no %s %s %s", op, d, age)
		return runner.Query{}
	}
	record := func(name string, arm Arm, q runner.Query, times int) {
		db := openRead(t, true)
		r, err := OpenReader(context.Background(), db, dir, arm, false)
		mustf(t, err, "open")
		for i := 0; i < times; i++ {
			if _, err := runner.Ask(r, q); err != nil {
				t.Fatal(err)
			}
			b, err := json.MarshalIndent(r.Trace().Raw, "", " ")
			mustf(t, err, "marshal")
			suffix := ""
			if times > 1 {
				suffix = string(rune('a' + i))
			}
			b = bytes.ReplaceAll(b, []byte(dir), []byte("/build")) // the directory is a temporary one
			mustf(t, os.WriteFile(filepath.Join(out, name+suffix+".json"), append(b, '\n'), 0o644), "write")
		}
	}
	record("neighbors_fwd_a2_", ArmA2, pick(runner.OpNeighbors, engine.Forward, runner.AgeNow), 2)
	record("neighbors_rev_a1", ArmA1, pick(runner.OpNeighbors, engine.Reverse, runner.AgeOldToken), 1)
	record("batch_rev_a2", ArmA2, pick(runner.OpBatch, engine.Reverse, runner.AgeNow), 1)
	record("alive_i", ArmI, pick(runner.OpAlive, engine.Forward, runner.AgeNow), 1)
}
