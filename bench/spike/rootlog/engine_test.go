package rootlog_test

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/lotannauo/toposhift/bench/spike/candidates"
	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
	"github.com/lotannauo/toposhift/bench/spike/pebblelog"
	"github.com/lotannauo/toposhift/bench/spike/rootlog"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/spike/workload"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/pebblestore"
)

// The runner reads how a retention spent its time through this interface.
var _ interface {
	LastRetain() (work, flush, settle time.Duration, deadlineHit bool)
} = (*rootlog.Engine)(nil)

func stream(t *testing.T) []engine.Record {
	t.Helper()
	cfg := workload.Tiny()
	cfg.Duration = 5 * time.Minute
	g, err := workload.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return g.All()
}

// An engine describes itself as the spike's engine does, with the settings it was
// opened with, and says what is different: it was built through the root store,
// and which checkpoint policy it has, in the spike's format.
func TestDescribeNamesTheStoreAndThePolicy(t *testing.T) {
	t.Parallel()
	recs := stream(t)
	for name, want := range map[string]string{
		"off":  "off",
		"k8a1": "kmin=8 alpha=1 lag=0s",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			c := settings[name]
			e, err := inMemory(options(c))(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = e.Close() }()
			if err := e.Write(recs); err != nil {
				t.Fatal(err)
			}
			if err := e.(engine.Settler).Settle(); err != nil {
				t.Fatal(err)
			}
			got, err := e.(engine.Describer).Describe()
			if err != nil {
				t.Fatal(err)
			}
			for k, w := range map[string]string{"layout": "L", "store": "root", "checkpoints": want} {
				if got[k] != w {
					t.Errorf("%s = %q, want %q", k, got[k], w)
				}
			}

			// Everything else is what the spike's engine says of the same database.
			ref, err := pebblelog.Open(t.TempDir(), pebblelog.Options{
				Config:      pebblekv.Config{Tuning: pebblekv.TinyTuning(), FS: vfs.NewMem()},
				Checkpoints: pebblelog.CheckpointOptions{On: c.On, KMin: c.KMin, Alpha: c.Alpha, Lag: c.Lag},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = ref.Close() }()
			if err := ref.Write(recs); err != nil {
				t.Fatal(err)
			}
			if err := ref.Settle(); err != nil {
				t.Fatal(err)
			}
			refDesc, err := ref.Describe()
			if err != nil {
				t.Fatal(err)
			}
			delete(got, "store")
			// The options text carries the file system, which differs here, and the
			// bytes recovered at the opening, which are those of a log that was replayed.
			if !slices.Equal(slices.Sorted(maps.Keys(got)), slices.Sorted(maps.Keys(refDesc))) {
				t.Errorf("keys of Describe: %v, the spike's engine has %v", slices.Sorted(maps.Keys(got)), slices.Sorted(maps.Keys(refDesc)))
			}
			for k, w := range refDesc {
				if k != "pebble_options" && got[k] != w {
					t.Errorf("%s = %q, the spike's engine says %q", k, got[k], w)
				}
			}
		})
	}
}

// The store is opened with the lifecycle policy that names no boot key, and that
// is fixed when the database is created: the database opens with the zero policy
// and not with one that names the boot key.
func TestTheStoreIsOpenedWithNoBootKey(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfg := pebblekv.Config{Schema: pebblekv.SchemaDefault, Tuning: pebblekv.TinyTuning()}
	e, err := rootlog.Open(dir, rootlog.Options{Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Write(stream(t)[:10]); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	s, err := pebblestore.Open(dir, pebblestore.Options{Config: cfg, Policy: lifecycle.Policy{}})
	if err != nil {
		t.Fatalf("the database does not open with the zero policy: %v", err)
	}
	_ = s.Close()
	if s, err := pebblestore.Open(dir, pebblestore.Options{Config: cfg, Policy: lifecycle.Policy{BootKey: lifecycle.BootID}}); err == nil {
		_ = s.Close()
		t.Fatal("the database opens with a policy that names the boot key")
	}
}

// An error of the store matches the engine's errors as well as the store's, and a
// plain invalid call is not taken for one before the horizon.
func TestErrorsMatchBothContracts(t *testing.T) {
	t.Parallel()
	recs := stream(t)
	o := options(pebblestore.CheckpointOptions{})
	o.FS = vfs.NewMem()
	e, err := rootlog.Open("db", o) // the refusal is the point
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = e.Close() }()
	if err := e.Write(recs[:20]); err != nil {
		t.Fatal(err)
	}
	if err := e.Write(recs[:1]); !errors.Is(err, engine.ErrInvalid) || errors.Is(err, engine.ErrBeforeHorizon) || !errors.Is(err, store.ErrInvalid) {
		t.Errorf("a repeated Seq: %v", err)
	}
	horizon := recs[19].EventTime.Add(time.Minute)
	if err := e.Retain(horizon); err != nil {
		t.Fatal(err)
	}
	stale := recs[19]
	stale.Seq = recs[19].Seq + 1
	if err := e.Write([]engine.Record{stale}); !errors.Is(err, engine.ErrBeforeHorizon) || !errors.Is(err, store.ErrBeforeHorizon) || !errors.Is(err, engine.ErrInvalid) {
		t.Errorf("a record before the horizon: %v", err)
	}
	if _, err := e.Neighbors(stale.Subject.A, engine.Forward, horizon.Add(-time.Hour), engine.Current(stale.Layer)); !errors.Is(err, engine.ErrBeforeHorizon) {
		t.Errorf("a read before the horizon: %v", err)
	}
}

// timingOrLayout says a counter is not compared between a build through the root
// store and one through the spike's engine: a timing (an "_ns" name), the bytes of
// the blocks a read loaded (they depend on how the database was laid out in
// tables, which the extra keys the root store keeps, and the way a retention
// interleaves with the compactions, change) and the allocations of a read (the
// adapter converts the records it is given and returns).
func timingOrLayout(name string) bool {
	return strings.HasSuffix(name, "_ns") || strings.Contains(name, "block_bytes") || strings.Contains(name, "block_read_ns") ||
		strings.HasPrefix(name, "cold_") || strings.HasPrefix(name, "alloc") || name == "block_loads"
}

// What only the store counts: the phases of a write and the time of building
// checkpoints (timings, or nothing the spike's engine had a name for), what a
// retention does with the prefixes a boot key keeps, and how the store cuts a
// retention into chunks (the spike's engine retained in one piece). The counters
// "retain.resumed" and "retain.superseded" appear only when Open finishes or
// discards a retention an earlier process left, so the tiny run never produces
// them, but a build that crashed and was reopened would.
var onlyTheStore = map[string]bool{
	"write.iterators":                true,
	"checkpoint.build_ns":            true,
	"retain.max_prefix_records":      true,
	"retain.prefixes_kept_for_boots": true,
	"retain.chunks":                  true,
	"retain.chunk_hold_ns":           true,
	"retain.resumed":                 true,
	"retain.superseded":              true,
}

func isOnlyTheStores(name string) bool {
	return onlyTheStore[name] || strings.HasPrefix(name, "write.phase_ns.")
}

func sameCounters(t *testing.T, what string, spike, root map[string]int64, extra func(string) bool) {
	t.Helper()
	for name, v := range spike {
		got, ok := root[name]
		switch {
		case !ok:
			t.Errorf("%s: the root store has no counter %q", what, name)
		case !timingOrLayout(name) && got != v:
			t.Errorf("%s: %s = %d through the root store, %d through the spike's engine", what, name, got, v)
		}
	}
	for name := range root {
		if _, ok := spike[name]; !ok && !extra(name) {
			t.Errorf("%s: the root store has a counter %q the spike's engine has no name for", what, name)
		}
	}
}

// quiet drops Pebble's messages.
type quiet struct{}

func (quiet) Infof(string, ...any)  {}
func (quiet) Errorf(string, ...any) {}
func (quiet) Fatalf(format string, args ...any) {
	panic(fmt.Sprintf("pebble: "+format, args...))
}

// dataDigest is the digest of the data keys of a database a build left under dir.
func dataDigest(t *testing.T, dir string) pebblekv.Digest {
	t.Helper()
	db, err := pebble.Open(filepath.Join(dir, runner.DBDir), &pebble.Options{ReadOnly: true, Logger: quiet{}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	d, err := pebblekv.DigestRange(db, pebblekv.DataLo, pebblekv.DataHi)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// A tiny plan built and read through each of the spike's layout-L engines and
// through the same variants of the root store gives the same stream of keys and
// values, and the same counters of what the engine did and what the reads cost,
// with the extra names the store has and the counters that depend on how the
// tables lie left out.
func TestATinyRunAgreesWithTheSpike(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t) // starts no goroutine of its own; the full tier runs it
	spec := runner.DefaultSpec(workload.Tiny())
	spec.BatchSize = 64
	spec.Retentions = []runner.Retention{{At: 12 * time.Minute, Keep: 6 * time.Minute}}
	spec.MinNonEmpty = 0 // twenty minutes has no day to look back over
	plan, err := runner.MakePlan(context.Background(), spec, nil)
	if err != nil {
		t.Fatal(err)
	}
	clean := runner.Guards{Info: runner.BuildInfo{GoVersion: "test", GOOS: "test", GOARCH: "test", Revision: "r"}}

	type built struct {
		dir string
		m   *runner.Manifest
		r   *runner.Results
	}
	run := func(t *testing.T, name string) built {
		t.Helper()
		v, err := candidates.Lookup(name)
		if err != nil {
			t.Fatal(err)
		}
		dir := runner.CandidateDir(t.TempDir(), v.Name)
		m, err := runner.Build(context.Background(), plan, v, dir, clean, nil)
		if err != nil {
			t.Fatalf("building %s: %v", name, err)
		}
		r, err := runner.Read(context.Background(), plan, v, dir, clean, nil)
		if err != nil {
			t.Fatalf("reading %s: %v", name, err)
		}
		return built{dir, m, r}
	}

	for _, point := range []string{"off", "k64a2l1ns"} {
		t.Run(point, func(t *testing.T) {
			t.Parallel()
			spike, root := run(t, "L/"+point), run(t, "Lroot/"+point)

			if root.m.Candidate != "Lroot/"+point || root.r.Candidate != "Lroot/"+point {
				t.Errorf("candidate names %q and %q", root.m.Candidate, root.r.Candidate)
			}
			if root.m.Layout != spike.m.Layout || root.m.PlanDigest != spike.m.PlanDigest || !reflect.DeepEqual(root.m.Stream, spike.m.Stream) {
				t.Errorf("layout, plan or stream differ: %q %q, %q %q", root.m.Layout, spike.m.Layout, root.m.PlanDigest, spike.m.PlanDigest)
			}
			if !maps.Equal(root.m.Breakdown, spike.m.Breakdown) {
				t.Errorf("breakdown differs:\n root  %v\n spike %v", root.m.Breakdown, spike.m.Breakdown)
			}
			wantDescribe := maps.Clone(spike.m.Describe)
			wantDescribe["store"] = "root"
			// The one key a build through the root store has besides the spike engine's
			// (go_gc is in both): how its retention runs.
			wantDescribe[runner.RetentionModeKey] = "sync"
			for k, v := range wantDescribe {
				if k != "pebble_options" && root.m.Describe[k] != v {
					t.Errorf("Describe[%s] = %q, want %q", k, root.m.Describe[k], v)
				}
			}
			if len(root.m.Describe) != len(wantDescribe) {
				t.Errorf("Describe has %d keys, want %d", len(root.m.Describe), len(wantDescribe))
			}
			sameCounters(t, "build", spike.m.Counters, root.m.Counters, isOnlyTheStores)
			if len(root.m.UncompactedWrong) != 0 || len(root.r.Mismatches) != 0 || len(root.r.Unstable) != 0 {
				t.Errorf("wrong %v, mismatches %v, unstable %v", root.m.UncompactedWrong, root.r.Mismatches, root.r.Unstable)
			}

			if len(root.r.Queries) != len(spike.r.Queries) || len(root.r.Queries) == 0 {
				t.Fatalf("%d queries through the root store, %d through the spike's engine", len(root.r.Queries), len(spike.r.Queries))
			}
			for i, q := range spike.r.Queries {
				got := root.r.Queries[i]
				if got.Digest != q.Digest || got.Size != q.Size {
					t.Errorf("query %d: answer %s (%d) through the root store, %s (%d) through the spike's engine", i, got.Digest, got.Size, q.Digest, q.Size)
				}
				sameCounters(t, plan.Queries[i].Name(), q.Counters, got.Counters, func(string) bool { return false })
			}
			if len(root.m.Uncompacted) != len(spike.m.Uncompacted) {
				t.Fatalf("%d uncompacted reads, want %d", len(root.m.Uncompacted), len(spike.m.Uncompacted))
			}
			for i, u := range spike.m.Uncompacted {
				sameCounters(t, "uncompacted "+u.Query, u.Counters, root.m.Uncompacted[i].Counters, func(string) bool { return false })
			}

			// The data keys: every record, checkpoint and baseline, byte for byte.
			a, b := dataDigest(t, spike.dir), dataDigest(t, root.dir)
			if a != b || a.Records == 0 || a.Baselines == 0 {
				t.Errorf("data keys differ or are missing the kinds a run holds:\n root  %+v\n spike %+v", b, a)
			}
			if point != "off" && a.Checkpoints == 0 {
				t.Errorf("a variant with checkpoints wrote none: %+v", a)
			}
		})
	}
}
