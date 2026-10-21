package candidates_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/lotannauo/toposhift/bench/spike/candidates"
	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
	"github.com/lotannauo/toposhift/bench/spike/pebblelog"
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
// Policies that put checkpoints back (see pebblelog.CheckpointOptions.Replace): found
// by Lookup, in no default set.
var replacePoints = []string{"L/k64a2l1nsr4", "L/k64a2r4", "L/k8a1l2sr1", "L/k64a4l1h30mr4"}

var rootPoints = []string{"Lroot/off", "Lroot/k8a1", "Lroot/k64a2l1ns", "Lroot/k64a1l1ns", "Lroot/k64a4l1h30m"}

func everyVariant(t *testing.T) []candidates.Variant {
	t.Helper()
	out := candidates.All()
	for _, name := range slices.Concat(sweepPoints, replacePoints, rootPoints) {
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

// A name with a replacement count finds the variant it names, and only that: the
// suffix comes after the lag, only when above zero, and the root store has none.
func TestReplacementNamesParseAndRoundTrip(t *testing.T) {
	t.Parallel()
	for _, name := range replacePoints {
		v, err := candidates.Lookup(name)
		if err != nil || v.Name != name || v.Layout != "L" || v.Root() {
			t.Errorf("Lookup(%q) = %+v, %v", name, v, err)
		}
		if slices.Contains(candidates.Names(), name) || slices.Contains(candidates.SweepGrid(), name) {
			t.Errorf("%s is in a default set", name)
		}
	}
	// A count of zero is the policy without one, under its own name.
	if got := candidates.LCheckpointsReplacing(64, 2, time.Nanosecond, 0).Name; got != "L/k64a2l1ns" {
		t.Errorf("a replacement count of zero is named %q", got)
	}
	if got := candidates.LCheckpointsReplacing(64, 2, time.Nanosecond, 4).Name; got != "L/k64a2l1nsr4" {
		t.Errorf("a replacement count of four is named %q", got)
	}
	for _, name := range []string{"L/k64a4l1h30m", "L/k64a4l1h30mr4"} {
		if _, err := candidates.Lookup(name); err != nil { // a multi-unit lag stops where the suffix begins
			t.Error(err)
		}
	}
	for _, bad := range []string{
		"Lroot/k64a2l1nsr4",                              // the root store has no such option
		"L/k64a2l1nsr0",                                  // no count has no suffix
		"L/k64a2l1nsr04",                                 // a second spelling of L/k64a2l1nsr4
		"L/k64a2l1nsr",                                   // no count
		"L/k64a2l1nsr-1",                                 // not a count
		"L/k64a2r4l1ns",                                  // the lag comes first
		"L/k64a2l1nsr4r4",                                // one suffix
		"L/k64a2l1nsR4", "L/k64a2l1nsx4", "L/k64a2l1ns4", // not the suffix
		"L/off4", "L/offr4", "Lroot/offr4", // the policy off has no count
	} {
		if v, err := candidates.Lookup(bad); err == nil {
			t.Errorf("Lookup(%q) = %q, want an error", bad, v.Name)
		}
	}
	if _, err := candidates.Lookup("Lroot/k64a2l1nsr4"); err == nil || !strings.Contains(err.Error(), "root store") {
		t.Errorf("Lookup of a root store variant with a count: %v, want an error that says the root store has none", err)
	}
}

// Options.TallyPrefixes reaches the engines of layout L and only those: the root
// store's variants do not have the tallies.
func TestTalliesAreKeptByTheSpikesLayoutOnly(t *testing.T) {
	t.Parallel()
	for name, want := range map[string]bool{"L/k64a2l1nsr4": true, "L/k64a2l1ns": true, "L/off": true, "Lroot/k64a2l1ns": false, "M/crdb1": false} {
		v, err := candidates.Lookup(name)
		if err != nil {
			t.Fatal(err)
		}
		for _, tally := range []bool{false, true} {
			e, err := factory(v, candidates.Options{Tuning: tiny(), TallyPrefixes: tally})(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			pt, ok := e.(pebblelog.PrefixTallier)
			got := ok && pt.PrefixTallies() != nil
			if got != (want && tally) {
				t.Errorf("%s with TallyPrefixes %v: tallies kept %v, want %v", name, tally, got, want && tally)
			}
			_ = e.Close()
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
		"L/k64a2l5m":       {"layout": "L", "checkpoints": "kmin=64 alpha=2 lag=5m0s"},
		// Said only when above zero, so that no earlier description changes.
		"L/k64a2l1nsr4": {"layout": "L", "checkpoints": "kmin=64 alpha=2 lag=1ns replace=4"},
		// The root store's variants say so, and the others do not.
		"Lroot/off":         {"layout": "L", "store": "root", "checkpoints": "off", "collectors_in_tables": "obsolete-key"},
		"Lroot/k64a2l1ns":   {"layout": "L", "store": "root", "checkpoints": "kmin=64 alpha=2 lag=1ns"},
		"Lroot/k64a2l5m":    {"layout": "L", "store": "root", "checkpoints": "kmin=64 alpha=2 lag=5m0s"},
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
// they open in no mode, in sync and in background, and refuse anything else, saying
// so; every other variant ignores it.
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
			refused := mode == "async"
			switch {
			case v.Root() && refused:
				if err == nil {
					_ = e.Close()
					t.Errorf("%s in mode %q: opened", name, mode)
				} else if !strings.Contains(err.Error(), `"async" is not`) {
					t.Errorf("%s in mode %q: %v, want it to say the mode is not one there is", name, mode, err)
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

// ServeOptions reaches the variants built through the root store and no other: the
// database an Lroot variant creates with it is refused by one opened without it (the
// boot key is fixed at creation), and the others ignore it. Off, the default, an Lroot
// database opens as it always did.
func TestServeOptionsAreTheRootVariantsOnly(t *testing.T) {
	t.Parallel()

	for _, name := range []string{"Lroot/off", "Lroot/k64a2l1ns", "L/off", "M/default"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			v, err := candidates.Lookup(name)
			if err != nil {
				t.Fatal(err)
			}
			fs := vfs.NewMem()
			open := func(serve bool) (engine.Engine, error) {
				return v.Open("db", candidates.Options{FS: fs, ServeOptions: serve})
			}
			e, err := open(true)
			if err != nil {
				t.Fatal(err)
			}
			if err := e.Close(); err != nil {
				t.Fatal(err)
			}
			e, err = open(false)
			switch {
			case v.Root() && err == nil:
				_ = e.Close()
				t.Errorf("%s: a database created with ServeOptions opens without them", name)
			case !v.Root() && err != nil:
				t.Errorf("%s: ServeOptions changed a variant that is not built through the root store: %v", name, err)
			case err == nil:
				_ = e.Close()
			}
		})
	}
}

// CheckRetentionMode says what the root store can be opened in: no mode, sync and
// background, and nothing else.
func TestCheckRetentionModeAcceptsSyncAndBackground(t *testing.T) {
	t.Parallel()

	for mode, ok := range map[string]bool{"": true, candidates.RetentionSync: true, candidates.RetentionBackground: true, "async": false, "Background": false} {
		if err := candidates.CheckRetentionMode(mode); (err == nil) != ok {
			t.Errorf("CheckRetentionMode(%q) = %v, want ok %v", mode, err, ok)
		}
	}
}

// The mode and the observer reach the store of a root variant: in background the
// observer is told of the publication, the end of the rewrite and the end of the
// settle of a retention, once each and at instants in that order, before WaitRetained
// returns; in sync (and with no mode) it is never told, because the store does all its
// work under its lock. The order the calls arrive in is not checked: the Retain tells
// of the publication after it releases the store's lock, and the store's goroutine may
// end the rewrite, and the settle, before it does (see pebblestore.RetainObserver).
func TestTheObserverReachesTheStoreOfARootVariantInTheBackground(t *testing.T) {
	t.Parallel()

	cfg := workload.Tiny()
	cfg.Duration = 5 * time.Minute
	g, err := workload.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	recs := g.All()
	horizon := recs[len(recs)-1].EventTime.Add(-2 * time.Minute)
	for _, c := range []struct {
		mode string
		told bool
	}{{"", false}, {candidates.RetentionSync, false}, {candidates.RetentionBackground, true}} {
		for _, name := range []string{"Lroot/off", "Lroot/k8a1"} {
			v, err := candidates.Lookup(name)
			if err != nil {
				t.Fatal(err)
			}
			var told []phaseCall
			var mu sync.Mutex
			obs := phaseLog(func(c phaseCall) { mu.Lock(); told = append(told, c); mu.Unlock() })
			e, err := v.Open("db", candidates.Options{FS: vfs.NewMem(), RetentionMode: c.mode, SettleRetention: true, RetainObserver: obs})
			if err != nil {
				t.Fatal(err)
			}
			if err := e.Write(recs); err != nil {
				t.Fatal(err)
			}
			if err := e.Retain(horizon); err != nil {
				t.Fatal(err)
			}
			if w, ok := e.(interface{ WaitRetained(context.Context) error }); !ok {
				t.Errorf("%s in mode %q: cannot be waited for", name, c.mode)
			} else if err := w.WaitRetained(context.Background()); err != nil {
				t.Errorf("%s in mode %q: %v", name, c.mode, err)
			}
			mu.Lock()
			got := slices.Clone(told)
			mu.Unlock()
			if err := toldOfOneRetention(got, c.told); err != nil {
				t.Errorf("%s in mode %q: %v", name, c.mode, err)
			}
			if err := e.Close(); err != nil {
				t.Errorf("%s in mode %q: Close = %v", name, c.mode, err)
			}
		}
	}
}

// phaseCall is one call of a RetainObserver: the phase, the generation and the
// instant.
type phaseCall struct {
	what string
	gen  uint64
	at   time.Time
}

func (c phaseCall) String() string { return fmt.Sprintf("%s of %d", c.what, c.gen) }

// phaseLog is a RetainObserver that passes each call on.
type phaseLog func(phaseCall)

func (f phaseLog) Published(gen uint64, at time.Time)    { f(phaseCall{"published", gen, at}) }
func (f phaseLog) RewriteEnded(gen uint64, at time.Time) { f(phaseCall{"rewrite", gen, at}) }
func (f phaseLog) SettleEnded(gen uint64, at time.Time)  { f(phaseCall{"settle", gen, at}) }

// toldOfOneRetention says what is wrong with the calls an observer got for one
// retention: none if told is false; else one call of each phase, all of one
// generation, at instants in the order of the phases (the instants are read under
// the store's lock, one phase after the other; the arrival order is not promised).
func toldOfOneRetention(got []phaseCall, told bool) error {
	if !told {
		if len(got) != 0 {
			return fmt.Errorf("the observer was told %v, want nothing", got)
		}
		return nil
	}
	at := map[string]time.Time{}
	for _, c := range got {
		if _, twice := at[c.what]; twice || c.gen != got[0].gen {
			return fmt.Errorf("the observer was told %v, want each phase once, of one generation", got)
		}
		at[c.what] = c.at
	}
	pub, ok1 := at["published"]
	rew, ok2 := at["rewrite"]
	set, ok3 := at["settle"]
	if !ok1 || !ok2 || !ok3 || len(got) != 3 {
		return fmt.Errorf("the observer was told %v, want published, rewrite and settle", got)
	}
	if rew.Before(pub) || set.Before(rew) {
		return fmt.Errorf("published at %v, rewrite ended at %v, settle ended at %v: want them in that order", pub, rew, set)
	}
	return nil
}

// External makes a candidate that is not in the registry, under a name that cannot be
// taken for one that is.
func TestExternalMakesACandidateOutsideTheRegistry(t *testing.T) {
	t.Parallel()

	base, err := candidates.Lookup("L/off")
	if err != nil {
		t.Fatal(err)
	}
	var seen candidates.Options
	opened := 0
	open := func(dir string, o candidates.Options) (engine.Engine, error) {
		seen = o
		opened++
		return base.Open(dir, o)
	}

	for _, cgo := range []bool{false, true} {
		v, err := candidates.External("RL/off", "L", cgo, open)
		if err != nil {
			t.Fatal(err)
		}
		if v.Name != "RL/off" || v.Layout != "L" || v.Root() || v.CGO() != cgo {
			t.Errorf("the candidate is %s (layout %s, root %v, cgo %v), want RL/off, L, false, %v", v.Name, v.Layout, v.Root(), v.CGO(), cgo)
		}
		e, err := v.Open("db", candidates.Options{FS: vfs.NewMem(), Sync: true})
		if err != nil {
			t.Fatal(err)
		}
		if !seen.Sync {
			t.Error("the open function did not get the run's options")
		}
		if err := e.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if opened != 2 {
		t.Errorf("the open function ran %d times, want 2", opened)
	}
	if _, err := candidates.Lookup("RL/off"); err == nil {
		t.Error("an external candidate is in the registry")
	}
	if slices.Contains(candidates.Names(), "RL/off") {
		t.Error("an external candidate is in the default set")
	}

	// A layout M candidate is allowed, and a name beside the reserved ones.
	for _, c := range []struct{ name, layout string }{{"RL/k64a2l5m", "L"}, {"RM/crdb1", "M"}, {"Lx/off", "L"}, {"l/off", "L"}, {"ML/off", "L"}, {"Lroot", "L"}, {"M", "M"}} {
		if _, err := candidates.External(c.name, c.layout, true, open); err != nil {
			t.Errorf("%s: %v", c.name, err)
		}
	}
}

// The names of the registry cannot be used by an external candidate, whether
// Lookup finds them or the prefix is only reserved, so that a result never names a
// candidate that is not the one this package makes.
func TestExternalRefusesTheNamesOfTheRegistry(t *testing.T) {
	t.Parallel()

	open := func(string, candidates.Options) (engine.Engine, error) { return nil, context.Canceled }
	names := append(candidates.Names(), "L/k32a2", "L/k128a0.5l1ns", "L/k64a2l1nsr4", "Lroot/off", "Lroot/k64a2l1ns",
		// Reserved by prefix though Lookup does not find them.
		"L/", "L/rocks", "L/off ", "L/k64a2l1nsr4x", "Lroot/", "Lroot/rocks", "Lroot/k64a2l1nsr4", "M/", "M/rocks", "M/crdb2")
	for _, name := range names {
		for _, cgo := range []bool{false, true} {
			if v, err := candidates.External(name, "L", cgo, open); err == nil {
				t.Errorf("%q (cgo %v) was accepted as %+v", name, cgo, v)
			}
		}
	}
	// A name that Lookup resolves is refused for the reason that it resolves, a
	// reserved one for its prefix.
	if _, err := candidates.External("L/off", "L", true, open); err == nil || !strings.Contains(err.Error(), "L/off") {
		t.Errorf("L/off: %v", err)
	}
	if _, err := candidates.External("M/rocks", "L", true, open); err == nil || !strings.Contains(err.Error(), "M/") {
		t.Errorf("M/rocks: %v", err)
	}

	// Nothing else about the call may be left out.
	for name, c := range map[string]struct {
		name, layout string
		open         func(string, candidates.Options) (engine.Engine, error)
	}{
		"no name":      {"", "L", open},
		"no layout":    {"RL/off", "", open},
		"another one":  {"RL/off", "N", open},
		"lower case":   {"RL/off", "l", open},
		"no open func": {"RL/off", "L", nil},
	} {
		if _, err := candidates.External(c.name, c.layout, true, c.open); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// Only External sets the declaration: no candidate of the registry has it, however
// it is found.
func TestNoCandidateOfTheRegistryDeclaresCgo(t *testing.T) {
	t.Parallel()

	for _, name := range append(candidates.Names(), candidates.SweepGrid()...) {
		v, err := candidates.Lookup(name)
		if err != nil {
			t.Fatal(err)
		}
		if v.CGO() {
			t.Errorf("%s declares cgo", name)
		}
	}
	for _, v := range []candidates.Variant{candidates.LNoCheckpoints(), candidates.LCheckpoints(64, 4, 0), candidates.LCheckpointsReplacing(64, 4, 0, 2), candidates.LrootNoCheckpoints(), candidates.LrootCheckpoints(64, 4, 0), {}} {
		if v.CGO() {
			t.Errorf("%q declares cgo", v.Name)
		}
	}
}

// CheckExternalName is the rule External applies, for the guards to apply again to a
// variant whose name changed after it was made.
func TestCheckExternalNameIsExternalsRule(t *testing.T) {
	t.Parallel()

	open := func(string, candidates.Options) (engine.Engine, error) { return nil, context.Canceled }
	for _, name := range append(candidates.Names(), "", "L/", "L/rocks", "Lroot/off", "M/crdb2", "L/k32a2") {
		if err := candidates.CheckExternalName(name); err == nil {
			t.Errorf("%q was accepted", name)
		}
		if _, err := candidates.External(name, "L", true, open); err == nil {
			t.Errorf("External accepted %q", name)
		}
	}
	for _, name := range []string{"RL/off", "RL/k64a2l5m", "X/wrapped", "Lx/off", "l/off", "M"} {
		if err := candidates.CheckExternalName(name); err != nil {
			t.Errorf("%q: %v", name, err)
		}
	}
}
