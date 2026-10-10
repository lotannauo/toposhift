package candidates_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/lotannauo/toposhift/bench/spike/candidates"
	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
	"github.com/lotannauo/toposhift/bench/spike/workload"
)

// Each factory call gets a file system of its own: the checks are one-shot.
func factory(v candidates.Variant, o candidates.Options) conformance.Factory {
	return func(dir string) (engine.Engine, error) {
		o := o
		o.FS = vfs.NewMem()
		return v.Open(dir, o)
	}
}

// Policies of layout L that a sweep names besides the default.
var sweepPoints = append([]string{"L/k8a1", "L/k32a0.5", "L/k128a8l1ns", "L/k64a4l2s", "L/k64a4l1h", "L/k64a4l1h30m", "L/k64a2l1ns"}, candidates.SweepGrid()...)

// The variants built through the root module's store. They are found by Lookup
// and are not in All.
var rootPoints = []string{"Lroot/off", "Lroot/k8a1", "Lroot/k64a2l1ns", "Lroot/k64a1l1ns", "Lroot/k64a4l1h30m"}

func everyVariant(t *testing.T) []candidates.Variant {
	t.Helper()
	out := candidates.All()
	for _, name := range append(slices.Clone(sweepPoints), rootPoints...) {
		v, err := candidates.Lookup(name)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, v)
	}
	return out
}

// Every variant a measurement can run passes the part of the conformance test that
// exercises its configuration, at the settings it is measured with (Pebble's
// benchmark tuning, not the tiny one the layouts' own tests use), so a variant is
// never measured that was not checked as it is run.
func TestEveryVariantConformsAtItsMeasuredSettings(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t) // starts no goroutine of its own; the full tier runs it
	for _, v := range everyVariant(t) {
		t.Run(v.Name, func(t *testing.T) {
			t.Parallel()
			conformance.RunPartial(t, factory(v, candidates.Options{}))
			if v.Layout == "L" {
				if err := conformance.CheckCheckpoint(open(t, factory(v, candidates.Options{}))); err != nil {
					t.Fatalf("checkpoint check: %v", err)
				}
			}
			t.Run("measurement", func(t *testing.T) {
				if err := conformance.CheckMeasurement(open(t, factory(v, candidates.Options{}))); err != nil {
					t.Fatal(err)
				}
			})
		})
	}
}

func open(t *testing.T, f conformance.Factory) engine.Engine {
	t.Helper()
	e, err := f(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e
}

// A name finds the variant it names, and only that.
func TestNamesRoundTripAndBadOnesAreRefused(t *testing.T) {
	t.Parallel()
	seen := map[string]bool{}
	for _, v := range everyVariant(t) {
		if seen[v.Name] {
			t.Errorf("two variants are named %q", v.Name)
		}
		seen[v.Name] = true
		got, err := candidates.Lookup(v.Name)
		if err != nil || got.Name != v.Name || got.Layout != v.Layout {
			t.Errorf("Lookup(%q) = %+v, %v", v.Name, got, err)
		}
	}
	for _, bad := range []string{
		"", "M", "M/nonsense", "L", "L/k", "L/k0a4", "L/k64a0", "L/k64a-1", "L/k64a4l-2s", "L/k64a4lforever",
		"L/k064a4",        // a second spelling of L/k64a4
		"L/k64a4.0",       // and another
		"L/k64a4l0s",      // and another: no lag has no suffix
		"L/k64a4l1h30m0s", // a second spelling of L/k64a4l1h30m
		"L/k64a4l90m",     // and another
		"L/k64a4l1h0m",    // a second spelling of L/k64a4l1h
		"X/crdb1", "m/crdb1",
		"Lroot", "Lroot/", "Lroot/off2", "Lroot/L/off", "Lroot/k0a4", "Lroot/k64a0", "Lroot/k064a4", "Lroot/k64a4l0s", "Lroot/k64a4l90m", "Lroot/M/crdb1",
		"Lroot/off+filter", "LROOT/off",
	} {
		if v, err := candidates.Lookup(bad); err == nil {
			t.Errorf("Lookup(%q) = %q, want an error", bad, v.Name)
		}
	}
}

// The variants of the root store are looked up and are not run by default: a
// default measurement is the spike's, unchanged.
func TestRootVariantsAreFoundAndNotInTheDefaultSet(t *testing.T) {
	t.Parallel()
	for _, name := range rootPoints {
		if slices.Contains(candidates.Names(), name) {
			t.Errorf("%s is in the default set", name)
		}
		v, err := candidates.Lookup(name)
		if err != nil || v.Name != name || v.Layout != "L" {
			t.Errorf("Lookup(%q) = %+v, %v", name, v, err)
		}
	}
	for _, v := range candidates.All() {
		if strings.HasPrefix(v.Name, "Lroot/") {
			t.Errorf("the default set holds %s", v.Name)
		}
	}
	for _, name := range []string{"L/off", "L/k64a2l1ns"} {
		root, err := candidates.Lookup("Lroot/" + name[len("L/"):])
		if err != nil {
			t.Fatal(err)
		}
		plain, err := candidates.Lookup(name)
		if err != nil {
			t.Fatal(err)
		}
		if root.Name == plain.Name || root.Layout != plain.Layout {
			t.Errorf("%s and %s: layouts %q and %q", plain.Name, root.Name, plain.Layout, root.Layout)
		}
	}
}

// A variant is what its name says, in the tables it writes and not only in the
// options it was given: the key schema and the collectors are read back from the
// files, and the checkpoint policy from the engine. (Every table also carries
// Pebble's own obsolete-key collector.)
func TestAVariantIsWhatItsNameSays(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t) // starts no goroutine of its own; the full tier runs it
	want := map[string]map[string]string{
		"M/crdb1":          {"layout": "M", "key_schema_in_tables": "crdb1", "collectors_in_tables": "obsolete-key"},
		"M/crdb1+filter":   {"layout": "M", "key_schema_in_tables": "crdb1", "collectors_in_tables": "MVCCTimeInterval,obsolete-key"},
		"M/default":        {"layout": "M", "key_schema_in_tables": "DefaultKeySchema(cockroach_comparator,16)", "collectors_in_tables": "obsolete-key"},
		"M/default+filter": {"layout": "M", "key_schema_in_tables": "DefaultKeySchema(cockroach_comparator,16)", "collectors_in_tables": "MVCCTimeInterval,obsolete-key"},
		"L/off":            {"layout": "L", "checkpoints": "off", "collectors_in_tables": "obsolete-key"},
		"L/k64a4":          {"layout": "L", "checkpoints": "kmin=64 alpha=4 lag=0s"},
		"L/k64a4l1ns":      {"layout": "L", "checkpoints": "kmin=64 alpha=4 lag=1ns"},
		"L/k128a0.5l1ns":   {"layout": "L", "checkpoints": "kmin=128 alpha=0.5 lag=1ns"},
		"L/k64a4l1h30m":    {"layout": "L", "checkpoints": "kmin=64 alpha=4 lag=1h30m0s"},
		// The root store's variants say so, and the others do not.
		"Lroot/off":         {"layout": "L", "store": "root", "checkpoints": "off", "collectors_in_tables": "obsolete-key"},
		"Lroot/k64a2l1ns":   {"layout": "L", "store": "root", "checkpoints": "kmin=64 alpha=2 lag=1ns"},
		"Lroot/k64a4":       {"layout": "L", "store": "root", "checkpoints": "kmin=64 alpha=4 lag=0s"},
		"Lroot/k64a4l1h30m": {"layout": "L", "store": "root", "checkpoints": "kmin=64 alpha=4 lag=1h30m0s"},
	}
	for name, props := range want {
		v, err := candidates.Lookup(name)
		if err != nil {
			t.Fatal(err)
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cfg := workload.Tiny()
			cfg.Duration = 5 * time.Minute
			g, err := workload.New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			o := candidates.Options{Tuning: tiny()}
			e, err := factory(v, o)(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = e.Close() }()
			if err := e.Write(g.All()); err != nil {
				t.Fatal(err)
			}
			if err := e.(engine.Settler).Settle(); err != nil {
				t.Fatal(err)
			}
			got, err := e.(engine.Describer).Describe()
			if err != nil {
				t.Fatal(err)
			}
			for k, w := range props {
				if got[k] != w {
					t.Errorf("%s = %q, want %q (all: %v)", k, got[k], w, got)
				}
			}
			if _, has := props["store"]; !has {
				if s, ok := got["store"]; ok {
					t.Errorf("a variant that is not the root store's says store = %q", s)
				}
			}
		})
	}
}

// What a run chooses reaches the database: the cache size, the commit mode and
// whether Pebble compacts on its own.
func TestOptionsReachTheDatabase(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"L/off", "Lroot/off"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			optionsReachTheDatabase(t, name)
		})
	}
}

func optionsReachTheDatabase(t *testing.T, name string) {
	t.Helper()
	v, err := candidates.Lookup(name)
	if err != nil {
		t.Fatal(err)
	}
	e, err := factory(v, candidates.Options{
		CacheBytes: 12 << 20, Sync: true, DisableAutoCompactions: true, DisableReadCompactions: true,
	})(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = e.Close() }()
	got, err := e.(engine.Describer).Describe()
	if err != nil {
		t.Fatal(err)
	}
	for k, w := range map[string]string{
		"cache_bytes": "12582912", "sync": "true", "auto_compactions": "false", "read_compactions": "false",
		"block_bytes": "32768", // the zero tuning is the benchmark tuning
	} {
		if got[k] != w {
			t.Errorf("%s = %q, want %q", k, got[k], w)
		}
	}
	// And the defaults are the other way round.
	e2, err := factory(v, candidates.Options{})(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = e2.Close() }()
	got, _ = e2.(engine.Describer).Describe()
	if got["sync"] != "false" || got["auto_compactions"] != "true" || got["read_compactions"] != "true" {
		t.Errorf("defaults: %v", got)
	}
}

// SettleRetention reaches the engine of every layout, and is off unless asked.
func TestSettleRetentionReachesEveryLayout(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"L/off", "L/k64a2l1ns", "M/crdb1", "Lroot/off", "Lroot/k64a2l1ns"} {
		v, err := candidates.Lookup(name)
		if err != nil {
			t.Fatal(err)
		}
		for _, asked := range []bool{true, false} {
			e, err := factory(v, candidates.Options{SettleRetention: asked})(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			got, err := e.(engine.Describer).Describe()
			_ = e.Close()
			if err != nil {
				t.Fatal(err)
			}
			if want := fmt.Sprint(asked); got["settle_tombstones"] != want {
				t.Errorf("%s asked %v: settle_tombstones = %q, want %q", name, asked, got["settle_tombstones"], want)
			}
		}
	}
}

// The recorder given to a variant gets its counts, and a variant with checkpoints
// writes them where one without does not.
func TestARecorderSeesWhatTheVariantDoes(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t) // starts no goroutine of its own; the full tier runs it
	for name, wantCheckpoints := range map[string]bool{"L/off": false, "L/k8a1": true, "M/crdb1": false, "Lroot/off": false, "Lroot/k8a1": true} {
		v, err := candidates.Lookup(name)
		if err != nil {
			t.Fatal(err)
		}
		rec := &engine.MemRecorder{}
		e, err := factory(v, candidates.Options{Recorder: rec, Tuning: tiny()})(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		cfg := workload.Tiny()
		cfg.Duration = 10 * time.Minute
		g, err := workload.New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if err := e.Write(g.All()); err != nil {
			t.Fatal(err)
		}
		if err := e.Close(); err != nil {
			t.Fatal(err)
		}
		if got := rec.Counter("write.records") + rec.Counter("write.versions"); got == 0 {
			t.Errorf("%s: the recorder saw no writes", name)
		}
		if got := rec.Counter("checkpoint.written") > 0; got != wantCheckpoints {
			t.Errorf("%s: checkpoints written = %v, want %v", name, got, wantCheckpoints)
		}
	}
}

func TestQuiesceAndCompactAllAreThereThroughTheInterface(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	for _, v := range candidates.All() {
		e := open(t, factory(v, candidates.Options{Tuning: tiny()}))
		q, ok := e.(engine.Quiescer)
		if !ok {
			t.Fatalf("%s is not a Quiescer", v.Name)
		}
		if err := q.Quiesce(ctx); err != nil {
			t.Errorf("%s: %v", v.Name, err)
		}
	}
}

func tiny() pebblekv.Tuning { return pebblekv.TinyTuning() }

// Wrap opens the candidate it wraps, under another name that is not in the
// registry, with whatever the wrapper makes of the engine, and closes the engine
// if the wrapper refuses it.
func TestWrapIsNotACandidate(t *testing.T) {
	t.Parallel()

	base, err := candidates.Lookup("L/off")
	if err != nil {
		t.Fatal(err)
	}
	var seen candidates.Options
	w := candidates.Wrap(base, "L/wrapped", func(e engine.Engine, o candidates.Options) (engine.Engine, error) {
		seen = o
		return e, nil
	})
	if w.Name != "L/wrapped" || w.Layout != "L" {
		t.Errorf("wrapped variant is %s (layout %s)", w.Name, w.Layout)
	}
	e, err := w.Open("db", candidates.Options{FS: vfs.NewMem(), Sync: true})
	if err != nil {
		t.Fatal(err)
	}
	if !seen.Sync {
		t.Error("the wrapper did not get the run's options")
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := candidates.Lookup("L/wrapped"); err == nil {
		t.Error("a wrapped variant is in the registry")
	}

	refused := candidates.Wrap(base, "L/refused", func(engine.Engine, candidates.Options) (engine.Engine, error) {
		return nil, context.Canceled
	})
	fs := vfs.NewMem()
	if _, err := refused.Open("db", candidates.Options{FS: fs}); err == nil {
		t.Error("a refusing wrapper opened an engine")
	}
	// The engine it refused is closed, so the directory can be opened again.
	again, err := base.Open("db", candidates.Options{FS: fs})
	if err != nil {
		t.Fatalf("the refused engine was left open: %v", err)
	}
	_ = again.Close()
}

// The retention mode reaches the variants built through the root store and no other:
// they open in no mode and in sync, and refuse background (which the store does not
// have) and anything else, saying so; every other variant ignores it.
func TestTheRetentionModeIsTheRootVariantsOnly(t *testing.T) {
	t.Parallel()

	for _, name := range []string{"Lroot/off", "Lroot/k64a2l1ns", "L/off", "M/default"} {
		v, err := candidates.Lookup(name)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := v.Root(), strings.HasPrefix(name, "Lroot/"); got != want {
			t.Errorf("%s: Root() is %v, want %v", name, got, want)
		}
		for _, mode := range []string{"", candidates.RetentionSync, candidates.RetentionBackground, "async"} {
			e, err := v.Open("db", candidates.Options{FS: vfs.NewMem(), RetentionMode: mode})
			refused := mode == candidates.RetentionBackground || mode == "async"
			switch {
			case v.Root() && refused:
				if err == nil {
					_ = e.Close()
					t.Errorf("%s in mode %q: opened", name, mode)
				} else if mode == candidates.RetentionBackground && !strings.Contains(err.Error(), "Background: not available on this store") {
					t.Errorf("%s in mode %q: %v, want it to say the mode is not available", name, mode, err)
				}
			case err != nil:
				t.Errorf("%s in mode %q: %v", name, mode, err)
			default:
				_ = e.Close()
			}
		}
	}
	wrapped := candidates.Wrap(candidates.LrootNoCheckpoints(), "wrapped", func(e engine.Engine, _ candidates.Options) (engine.Engine, error) { return e, nil })
	if !wrapped.Root() {
		t.Error("a wrapped root variant is not root")
	}
}
