// Package candidates is the list of storage candidates the spike measures, each
// with the name it goes by in results, so that the variant a measurement runs is
// the variant the conformance test ran. A name says everything that distinguishes
// the candidate:
//
//	M/crdb1             layout M on cockroachkvs's columnar key schema
//	M/crdb1+filter      the same with the MVCC time-interval block filter
//	M/default           layout M on Pebble's default key schema
//	M/default+filter    the same with the filter
//	L/off               layout L (a log per entity) with no checkpoints
//	L/k64a4l1ns         layout L with checkpoints due after 64 records, scaled by 4, at
//	                    an instant 1 ns behind the newest (the design's default)
//	L/k64a4             the same at the newest instant (lag 0): an extension of a run
//	                    at its start deletes every checkpoint after it, so it cannot
//	                    help a prefix whose runs are refreshed
//	L/k64a4l2s          the same with a checkpoint instant 2 s behind the newest
//	L/k64a4l1h30m       the same 1 h 30 min behind: behind every run that can still be
//	                    extended when the coalescer bounds a run's age (RunMaxAge) and
//	                    the bound plus the extension interval is at most the lag
//	Lroot/off           layout L built through the root module's store, with no
//	                    checkpoints
//	Lroot/k64a2l1ns     the same with checkpoints, named as the L variants are, under the
//	                    prefix Lroot/ (any policy of layout L)
//
// The Lroot variants are for repeating what the spike measured on the code the
// product runs. They are found by [Lookup] and are not in [All], so a default
// measurement does not run them; a manifest of one is not compared with a manifest
// of an L variant by the gates.
//
// A lag is written as Go writes a duration, without the units that are zero at its
// end (30m, not 30m0s; 1h, not 1h0m0s).
//
// The settings a runner or a test chooses, not the candidate's own, are in
// [Options].
package candidates

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
	"github.com/lotannauo/toposhift/bench/spike/pebblelog"
	"github.com/lotannauo/toposhift/bench/spike/pebblemvcc"
	"github.com/lotannauo/toposhift/bench/spike/rootlog"
	"github.com/lotannauo/toposhift/internal/store/pebblestore"
)

// Options are the settings of a run, as opposed to the candidate.
type Options struct {
	// Tuning sizes Pebble. The zero value is [pebblekv.BenchTuning].
	Tuning pebblekv.Tuning
	// CacheBytes, if above zero, replaces the tuning's block cache size. A
	// comparison gives every candidate the same absolute size.
	CacheBytes int64
	// FS is the file system; nil is the real one.
	FS vfs.FS
	// Recorder receives the engine's counts and samples; nil discards them.
	Recorder engine.Recorder
	// Sync makes every commit wait for the log to reach the disk.
	Sync bool
	// DisableAutoCompactions and DisableReadCompactions hold the shape of the
	// tables still under a measurement; see [pebblekv.Config].
	DisableAutoCompactions, DisableReadCompactions bool
	// SettleRetention makes every retention of the candidate end only when the
	// database has settled what it wrote: every candidate a measurement builds
	// settles its retentions. See [pebblekv.Config].
	SettleRetention bool
	// ReadOnly opens the existing database for reading only; see
	// [pebblekv.Config].
	ReadOnly bool
}

func (o Options) config() pebblekv.Config {
	t := o.Tuning
	if t == (pebblekv.Tuning{}) {
		t = pebblekv.BenchTuning()
	}
	if o.CacheBytes > 0 {
		t.CacheBytes = o.CacheBytes
	}
	return pebblekv.Config{
		Tuning: t, FS: o.FS, Sync: o.Sync,
		DisableAutoCompactions: o.DisableAutoCompactions, DisableReadCompactions: o.DisableReadCompactions,
		ReadOnly: o.ReadOnly, SettleRetention: o.SettleRetention,
	}
}

// Variant is one candidate.
type Variant struct {
	// Name is the name results go by.
	Name string
	// Layout is "M" or "L".
	Layout string
	open   func(dir string, o Options) (engine.Engine, error)
}

// Open opens the candidate under dir.
func (v Variant) Open(dir string, o Options) (engine.Engine, error) { return v.open(dir, o) }

func layoutM(schema pebblekv.Schema, filter bool) Variant {
	name := "M/" + schema.String()
	if filter {
		name += "+filter"
	}
	return Variant{Name: name, Layout: "M", open: func(dir string, o Options) (engine.Engine, error) {
		cfg := o.config()
		cfg.Schema, cfg.TimeFilter = schema, filter
		return pebblemvcc.Open(dir, pebblemvcc.Options{Config: cfg, Recorder: o.Recorder})
	}}
}

// LNoCheckpoints is layout L with no checkpoints.
func LNoCheckpoints() Variant {
	return Variant{Name: "L/off", Layout: "L", open: func(dir string, o Options) (engine.Engine, error) {
		return pebblelog.Open(dir, pebblelog.Options{Config: o.config(), Recorder: o.Recorder})
	}}
}

// LCheckpoints is layout L with checkpoints due when a prefix has had kMin
// records since its last, scaled by alpha, with the instant lag behind the newest
// (see [pebblelog.CheckpointOptions]).
func LCheckpoints(kMin int, alpha float64, lag time.Duration) Variant {
	return Variant{Name: "L/" + checkpointSuffix(kMin, alpha, lag), Layout: "L", open: func(dir string, o Options) (engine.Engine, error) {
		return pebblelog.Open(dir, pebblelog.Options{
			Config: o.config(), Recorder: o.Recorder,
			Checkpoints: pebblelog.CheckpointOptions{On: true, KMin: kMin, Alpha: alpha, Lag: lag},
		})
	}}
}

// LrootNoCheckpoints is layout L built through the root module's store with no
// checkpoints, stated outright to the store.
func LrootNoCheckpoints() Variant {
	return Variant{Name: "Lroot/off", Layout: "L", open: func(dir string, o Options) (engine.Engine, error) {
		return rootlog.Open(dir, rootlog.Options{Config: o.config(), Recorder: o.Recorder})
	}}
}

// LrootCheckpoints is layout L built through the root module's store with the
// checkpoint policy of [LCheckpoints].
func LrootCheckpoints(kMin int, alpha float64, lag time.Duration) Variant {
	return Variant{Name: "Lroot/" + checkpointSuffix(kMin, alpha, lag), Layout: "L", open: func(dir string, o Options) (engine.Engine, error) {
		return rootlog.Open(dir, rootlog.Options{
			Config: o.config(), Recorder: o.Recorder,
			Checkpoints: pebblestore.CheckpointOptions{On: true, KMin: kMin, Alpha: alpha, Lag: lag},
		})
	}}
}

// checkpointSuffix is the part of a checkpoint variant's name after its prefix:
// k<K>a<alpha>[l<lag>].
func checkpointSuffix(kMin int, alpha float64, lag time.Duration) string {
	name := fmt.Sprintf("k%da%s", kMin, strconv.FormatFloat(alpha, 'g', -1, 64))
	if lag != 0 {
		name += "l" + lagName(lag)
	}
	return name
}

// lagName is the duration as Go writes it, without the zero units at its end.
func lagName(d time.Duration) string {
	s := d.String()
	if t, ok := strings.CutSuffix(s, "m0s"); ok {
		s = t + "m"
	}
	if t, ok := strings.CutSuffix(s, "h0m"); ok {
		s = t + "h"
	}
	return s
}

// Wrap is v with the engine it opens passed through wrap, under another name: a
// variant that is not a candidate, for a test of a runner that needs an engine
// that misbehaves. It is not in [All] and [Lookup] does not find it.
func Wrap(v Variant, name string, wrap func(engine.Engine, Options) (engine.Engine, error)) Variant {
	return Variant{Name: name, Layout: v.Layout, open: func(dir string, o Options) (engine.Engine, error) {
		e, err := v.open(dir, o)
		if err != nil {
			return nil, err
		}
		w, err := wrap(e, o)
		if err != nil {
			_ = e.Close()
			return nil, err
		}
		return w, nil
	}}
}

// All is the set a measurement runs by default: the four layout M variants, layout
// L without checkpoints and layout L with the design's defaults (K_min 64, alpha 4,
// an instant 1 ns behind the newest).
func All() []Variant {
	return []Variant{
		layoutM(pebblekv.SchemaCRDB, false), layoutM(pebblekv.SchemaCRDB, true),
		layoutM(pebblekv.SchemaDefault, false), layoutM(pebblekv.SchemaDefault, true),
		LNoCheckpoints(), LCheckpoints(64, 4, time.Nanosecond),
	}
}

// SweepGrid is the checkpoint policies of the mini-sweep besides the default at a
// lag of 1 ns, which is in [All]: every other combination of K_min 32, 64 and 128
// with alpha 1 and 4 at a lag of 1 ns, and the default policy at lag 0, which shows
// what the lag buys. Every one is conformed with the rest of the registry.
func SweepGrid() []string {
	var out []string
	for _, k := range []int{32, 64, 128} {
		for _, a := range []float64{1, 4} {
			if k == 64 && a == 4 {
				continue
			}
			out = append(out, LCheckpoints(k, a, time.Nanosecond).Name)
		}
	}
	return append(out, LCheckpoints(64, 4, 0).Name)
}

// Names are the names of [All].
func Names() []string {
	var out []string
	for _, v := range All() {
		out = append(out, v.Name)
	}
	return out
}

var checkpointName = regexp.MustCompile(`^(L|Lroot)/k([0-9]+)a([0-9.]+)(?:l(.+))?$`)

// Lookup finds a variant by name. Besides the names in [All] it accepts any
// checkpoint policy of layout L (L/k32a2, L/k128a0.5l1ns), which is how a sweep
// names the points it runs, and the root-store variants (Lroot/off and any
// checkpoint policy under the prefix Lroot/).
func Lookup(name string) (Variant, error) {
	for _, v := range All() {
		if v.Name == name {
			return v, nil
		}
	}
	if name == "Lroot/off" {
		return LrootNoCheckpoints(), nil
	}
	if m := checkpointName.FindStringSubmatch(name); m != nil {
		k, err := strconv.Atoi(m[2])
		alpha, err2 := strconv.ParseFloat(m[3], 64)
		var lag time.Duration
		var err3 error
		if m[4] != "" {
			lag, err3 = time.ParseDuration(m[4])
		}
		if err != nil || err2 != nil || err3 != nil || k < 1 || alpha <= 0 || lag < 0 {
			return Variant{}, fmt.Errorf("candidates: %q is not a checkpoint policy (K at least 1, alpha above 0, lag not negative)", name)
		}
		v := LCheckpoints(k, alpha, lag)
		if m[1] == "Lroot" {
			v = LrootCheckpoints(k, alpha, lag)
		}
		if v.Name != name { // a spelling that is not the canonical one would name the same point twice
			return Variant{}, fmt.Errorf("candidates: write %q as %q", name, v.Name)
		}
		return v, nil
	}
	return Variant{}, fmt.Errorf("candidates: no variant %q (have %s)", name, strings.Join(Names(), ", "))
}
