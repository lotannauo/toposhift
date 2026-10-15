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
//	L/k64a2l1nsr4       the policy of L/k64a2l1ns that, when a write deletes checkpoints
//	                    in a prefix, puts back the newest four, each at its own instant
//	                    (r<N>, after the lag, only when N is above zero; a prototype,
//	                    not in any default set)
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
	"errors"
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
	// RetentionMode is how the root store's retention runs, for the Lroot variants
	// only (every other variant ignores it): "" and [RetentionSync] are the store's
	// Synchronous mode, [RetentionBackground] its Background one, opened with the
	// settle of [Options.SettleRetention] if that is on. See [CheckRetentionMode].
	RetentionMode string
	// RetainObserver, if not nil, is passed to the root store of a Lroot variant, and
	// is told when each retention of a store in the background mode reaches the
	// points that bound its phases (see [pebblestore.RetainObserver]); a Synchronous
	// store never calls it. Every other variant ignores it.
	RetainObserver pebblestore.RetainObserver
	// RetainFlushDels is, for the Lroot variants only (every other variant ignores
	// it), the count of unflushed range deletions at which a retention's pass flushes
	// ([pebblestore.Options.RetainFlushDels]); 0 is the store's default,
	// [pebblestore.DefaultRetainFlushDels]. The store refuses a negative value.
	RetainFlushDels int
	// TallyPrefixes makes a variant of the spike's own layout L (not the root
	// store's) keep per-prefix counts of the checkpoints a record deleted and a
	// write put back, in memory (see [pebblelog.Options]); the others ignore it.
	TallyPrefixes bool
}

// The retention modes a build can ask of the root store.
const (
	// RetentionSync makes a retention do all of its work, settling included, before
	// it returns: the store's Synchronous mode, and the one "" means.
	RetentionSync = "sync"
	// RetentionBackground is the mode in which a retention runs beside the writer: the
	// store's Background mode. A build in it waits for the pass and its settle before
	// it reads anything.
	RetentionBackground = "background"
)

// CheckRetentionMode returns why mode is not one the root store can be opened in, or
// nil for "", [RetentionSync] and [RetentionBackground].
func CheckRetentionMode(mode string) error {
	switch mode {
	case "", RetentionSync, RetentionBackground:
		return nil
	}
	return fmt.Errorf("candidates: retention mode %q is not %q or %q", mode, RetentionSync, RetentionBackground)
}

// storeRetention is the retention mode of the root store that mode names, or the
// error [CheckRetentionMode] gives.
func storeRetention(mode string) (pebblestore.RetentionMode, error) {
	if err := CheckRetentionMode(mode); err != nil {
		return 0, err
	}
	if mode == RetentionBackground {
		return pebblestore.Background, nil
	}
	return pebblestore.Synchronous, nil
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
	// root says the candidate is built through the root module's store, which has a
	// retention mode ([Options.RetentionMode]).
	root bool
	// cgo says the candidate was made by [External] and declares that it needs a binary
	// built with cgo. Nothing else sets it, so a built-in candidate never has it.
	cgo  bool
	open func(dir string, o Options) (engine.Engine, error)
}

// Open opens the candidate under dir.
func (v Variant) Open(dir string, o Options) (engine.Engine, error) { return v.open(dir, o) }

// Root says whether the candidate is built through the root module's store, whose
// retention has a mode.
func (v Variant) Root() bool { return v.root }

// CGO says whether the candidate was made by [External] and declares that it is built
// with cgo, so that a binary built with cgo may be used for its results. No candidate
// of [All] or [Lookup] does.
func (v Variant) CGO() bool { return v.cgo }

// reservedPrefixes are the starts of the names of the candidates this package makes.
var reservedPrefixes = []string{"L/", "Lroot/", "M/"}

// CheckExternalName returns why name is not one an external candidate may go by, or
// nil: it is empty, [Lookup] resolves it, or it begins with L/, Lroot/ or M/, which are
// the names of this package's candidates. An external candidate cannot then be taken
// for one of them in a result.
func CheckExternalName(name string) error {
	if name == "" {
		return errors.New("candidates: an external candidate needs a name")
	}
	if _, err := Lookup(name); err == nil {
		return fmt.Errorf("candidates: %q is the name of a candidate of this package", name)
	}
	for _, p := range reservedPrefixes {
		if strings.HasPrefix(name, p) {
			return fmt.Errorf("candidates: %q begins with %q, which is for the candidates of this package", name, p)
		}
	}
	return nil
}

// External is a candidate defined outside this package, by an engine this package does
// not import: its layout is "M" or "L", and open opens it. cgo declares that it needs
// a binary built with cgo (a native library), which a runner's guards then admit for
// this candidate only. The name is the one results go by, and [CheckExternalName] says
// which are refused.
//
// The declaration is trusted, not checked: the guards cannot tell what open builds.
// They do check the name again ([Variant.Name] is exported, and [Wrap] gives another).
func External(name, layout string, cgo bool, open func(dir string, o Options) (engine.Engine, error)) (Variant, error) {
	if err := CheckExternalName(name); err != nil {
		return Variant{}, err
	}
	if layout != "L" && layout != "M" {
		return Variant{}, fmt.Errorf("candidates: external candidate %q: layout %q is not \"L\" or \"M\"", name, layout)
	}
	if open == nil {
		return Variant{}, fmt.Errorf("candidates: external candidate %q has no open function", name)
	}
	return Variant{Name: name, Layout: layout, cgo: cgo, open: open}, nil
}

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
		return pebblelog.Open(dir, pebblelog.Options{Config: o.config(), Recorder: o.Recorder, TallyPrefixes: o.TallyPrefixes})
	}}
}

// LCheckpoints is layout L with checkpoints due when a prefix has had kMin
// records since its last, scaled by alpha, with the instant lag behind the newest
// (see [pebblelog.CheckpointOptions]).
func LCheckpoints(kMin int, alpha float64, lag time.Duration) Variant {
	return LCheckpointsReplacing(kMin, alpha, lag, 0)
}

// LCheckpointsReplacing is [LCheckpoints] with [pebblelog.CheckpointOptions].Replace
// set: with replace above zero, a write that deletes checkpoints in a prefix puts back
// up to that many, each at its own instant. Zero is [LCheckpoints], by the same name.
func LCheckpointsReplacing(kMin int, alpha float64, lag time.Duration, replace int) Variant {
	return Variant{Name: "L/" + checkpointSuffix(kMin, alpha, lag, replace), Layout: "L", open: func(dir string, o Options) (engine.Engine, error) {
		return pebblelog.Open(dir, pebblelog.Options{
			Config: o.config(), Recorder: o.Recorder, TallyPrefixes: o.TallyPrefixes,
			Checkpoints: pebblelog.CheckpointOptions{On: true, KMin: kMin, Alpha: alpha, Lag: lag, Replace: replace},
		})
	}}
}

// LrootNoCheckpoints is layout L built through the root module's store with no
// checkpoints, stated outright to the store.
func LrootNoCheckpoints() Variant {
	return Variant{Name: "Lroot/off", Layout: "L", root: true, open: func(dir string, o Options) (engine.Engine, error) {
		mode, err := storeRetention(o.RetentionMode)
		if err != nil {
			return nil, err
		}
		return rootlog.Open(dir, rootlog.Options{Config: o.config(), Recorder: o.Recorder, Retention: mode, Observer: o.RetainObserver, RetainFlushDels: o.RetainFlushDels})
	}}
}

// LrootCheckpoints is layout L built through the root module's store with the
// checkpoint policy of [LCheckpoints].
func LrootCheckpoints(kMin int, alpha float64, lag time.Duration) Variant {
	return Variant{Name: "Lroot/" + checkpointSuffix(kMin, alpha, lag, 0), Layout: "L", root: true, open: func(dir string, o Options) (engine.Engine, error) {
		mode, err := storeRetention(o.RetentionMode)
		if err != nil {
			return nil, err
		}
		return rootlog.Open(dir, rootlog.Options{
			Config: o.config(), Recorder: o.Recorder, Retention: mode, Observer: o.RetainObserver, RetainFlushDels: o.RetainFlushDels,
			Checkpoints: pebblestore.CheckpointOptions{On: true, KMin: kMin, Alpha: alpha, Lag: lag},
		})
	}}
}

// checkpointSuffix is the part of a checkpoint variant's name after its prefix:
// k<K>a<alpha>[l<lag>][r<N>]: the lag when it is not zero, and the replacement
// count when it is above zero, so that no earlier name changes.
func checkpointSuffix(kMin int, alpha float64, lag time.Duration, replace int) string {
	name := fmt.Sprintf("k%da%s", kMin, strconv.FormatFloat(alpha, 'g', -1, 64))
	if lag != 0 {
		name += "l" + lagName(lag)
	}
	if replace > 0 {
		name += "r" + strconv.Itoa(replace)
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
	return Variant{Name: name, Layout: v.Layout, root: v.root, cgo: v.cgo, open: func(dir string, o Options) (engine.Engine, error) {
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

// The lag is one or more number-and-unit pieces (1h30m), and no unit has an r in it,
// so the lag stops where the replacement count begins.
var checkpointName = regexp.MustCompile(`^(L|Lroot)/k([0-9]+)a([0-9.]+)(?:l((?:[0-9.]+[a-zµ]+)+))?(?:r([0-9]+))?$`)

// Lookup finds a variant by name. Besides the names in [All] it accepts any
// checkpoint policy of layout L (L/k32a2, L/k128a0.5l1ns, and with a replacement
// count L/k64a2l1nsr4), which is how a sweep names the points it runs, and the
// root-store variants (Lroot/off and any checkpoint policy under the prefix Lroot/,
// without a replacement count).
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
		replace, err4 := 0, error(nil)
		if m[5] != "" {
			replace, err4 = strconv.Atoi(m[5])
		}
		if err != nil || err2 != nil || err3 != nil || err4 != nil || k < 1 || alpha <= 0 || lag < 0 {
			return Variant{}, fmt.Errorf("candidates: %q is not a checkpoint policy (K at least 1, alpha above 0, lag not negative)", name)
		}
		if m[1] == "Lroot" && m[5] != "" {
			return Variant{}, fmt.Errorf("candidates: %q: the root store has no replacement of checkpoints (r<N> is for the L variants)", name)
		}
		v := LCheckpointsReplacing(k, alpha, lag, replace)
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
