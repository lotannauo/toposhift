package duckengine

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/spike/workload"
)

// TestMain sets how many random workloads rapid runs, as the spike's own candidates do,
// and removes what the shared fixture made.
func TestMain(m *testing.M) {
	if err := flag.Set("rapid.checks", conformance.RapidChecks("4")); err != nil {
		panic(err)
	}
	code := m.Run()
	if shared.dir != "" {
		os.RemoveAll(shared.dir)
	}
	os.Exit(code)
}

// tinyPlan is a small plan with two retentions, so that its stream has baselines.
func tinyPlan(t testing.TB, retain bool) *runner.Plan {
	t.Helper()
	spec := runner.DefaultSpec(workload.Tiny())
	spec.BatchSize = 64
	spec.MinNonEmpty = 0
	spec.Retentions = nil
	if retain {
		// Two moves of the horizon inside the 20 minutes of the stream.
		spec.Retentions = []runner.Retention{{At: 9 * time.Minute, Keep: 4 * time.Minute}, {At: 15 * time.Minute, Keep: 5 * time.Minute}}
	}
	p, err := runner.MakePlan(context.Background(), spec, nil)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

var shared struct {
	once sync.Once
	dir  string
	plan *runner.Plan
	man  *Manifest
	err  error
}

// sharedBuild is one build of every arm of the tiny plan, made once and only read.
func sharedBuild(t testing.TB) (string, *runner.Plan, *Manifest) {
	t.Helper()
	shared.once.Do(func() {
		shared.plan = tinyPlan(t, true)
		dir, err := os.MkdirTemp("", "duckengine-shared-*")
		if err != nil {
			shared.err = err
			return
		}
		shared.dir = dir
		ctx := context.Background()
		db, err := Open(ctx, Config{Mode: ModeBuild, TempDir: filepath.Join(dir, "tmp")})
		if err != nil {
			shared.err = err
			return
		}
		defer db.Close()
		shared.man, shared.err = Build(ctx, db, filepath.Join(dir, "build"), PlanSource(shared.plan, "tiny"), Arms, BuildOptions{VerifyModel: true})
	})
	if shared.err != nil {
		t.Fatal(shared.err)
	}
	return filepath.Join(shared.dir, "build"), shared.plan, shared.man
}

// openRead opens a connection for reading, with profiling if asked.
func openRead(t testing.TB, profile bool) *DB {
	t.Helper()
	db, err := Open(context.Background(), Config{Mode: ModeRead, TempDir: t.TempDir(), Profile: profile})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func mustf(t testing.TB, err error, format string, args ...any) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", fmt.Sprintf(format, args...), err)
	}
}
