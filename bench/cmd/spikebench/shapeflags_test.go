package main

import (
	"flag"
	"slices"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/internal/store/pebblekv"
)

var shapeFlagNames = []string{"-lbase-target-mb", "-target-sizes-mb", "-compaction-concurrency", "-l0-stop-writes", "-disable-delete-only", "-compression", "-l0-compaction-concurrency", "-compaction-debt-concurrency-mb", "-compaction-garbage-fraction"}

// Off by default: no shape reaches the build, and none is passed on to a build step.
func TestShapeFlagsAreOffByDefault(t *testing.T) {
	t.Parallel()
	b := parseBuild(t, false)
	if b.shape() != (pebblekv.Tuning{}) || b.options().Shape != (pebblekv.Tuning{}) {
		t.Errorf("shape %+v, options %+v", b.shape(), b.options().Shape)
	}
	for _, a := range b.args() {
		for _, name := range shapeFlagNames {
			if strings.HasPrefix(a, name) {
				t.Errorf("a default build passes %s on", a)
			}
		}
	}
}

// Each flag reaches the build options, is passed on to the build steps in a form a step
// makes the same choices from, and gives the words the build records.
func TestShapeFlagsReachTheBuild(t *testing.T) {
	t.Parallel()
	const mib = 1 << 20
	for _, c := range []struct {
		args  []string
		shape pebblekv.Tuning
		words string
		pass  []string
	}{
		{[]string{"-lbase-target-mb=4"}, pebblekv.Tuning{LBaseTargetFileSize: 4 * mib}, "lbase_target_file_size=4194304", []string{"-lbase-target-mb=4"}},
		{[]string{"-target-sizes-mb=64, 4,8"}, pebblekv.Tuning{TargetFileSizes: [7]int64{64 * mib, 4 * mib, 8 * mib}}, "target_file_sizes=67108864,4194304,8388608,0,0,0,0", []string{"-target-sizes-mb=64,4,8"}},
		{[]string{"-compaction-concurrency=1, 3"}, pebblekv.Tuning{CompactionConcurrencyLower: 1, CompactionConcurrencyUpper: 3}, "compaction_concurrency=1-3", []string{"-compaction-concurrency=1,3"}},
		{[]string{"-l0-stop-writes=20"}, pebblekv.Tuning{L0StopWritesThreshold: 20}, "l0_stop_writes_threshold=20", []string{"-l0-stop-writes=20"}},
		{[]string{"-disable-delete-only"}, pebblekv.Tuning{DisableDeleteOnlyCompactions: true}, "delete_only_compactions=off", []string{"-disable-delete-only"}},
		{[]string{"-compression=zstd"}, pebblekv.Tuning{Compression: pebblekv.CompressionZstd}, "compression=zstd", []string{"-compression=zstd"}},
		{[]string{"-compression=snappy"}, pebblekv.Tuning{Compression: pebblekv.CompressionSnappy}, "compression=snappy", []string{"-compression=snappy"}},
		{[]string{"-compression=default"}, pebblekv.Tuning{}, "", nil},
	} {
		b := parseBuild(t, false, c.args...)
		if got := b.options().Shape; got != c.shape {
			t.Errorf("%v: the build options say %+v, want %+v", c.args, got, c.shape)
		}
		if got := b.shape().ShapeString(); got != c.words {
			t.Errorf("%v: recorded as %q, want %q", c.args, got, c.words)
		}
		for _, p := range c.pass {
			if !slices.Contains(b.args(), p) {
				t.Errorf("%v: passed on as %v, want %s among them", c.args, b.args(), p)
			}
		}
		if again := parseBuild(t, true, b.args()...); again != b {
			t.Errorf("%v: a step makes %+v, want %+v", c.args, again, b)
		}
	}

	all := parseBuild(t, false, "-target-sizes-mb=64,4", "-compaction-concurrency=2,4", "-l0-stop-writes=16", "-disable-delete-only", "-compression=balanced")
	if again := parseBuild(t, false, all.args()...); again != all || again.options().Shape != all.options().Shape {
		t.Errorf("all together: a step makes %+v, want %+v", again, all)
	}
}

// What cannot be honoured is refused when the flags are read, before anything is built.
func TestShapeFlagsRefuseWhatTheStoreRefuses(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		args []string
		want string
	}{
		{[]string{"-lbase-target-mb=-1"}, "-lbase-target-mb"},
		{[]string{"-lbase-target-mb=2000"}, "-lbase-target-mb"},
		{[]string{"-lbase-target-mb=4", "-target-sizes-mb=64,4"}, "give one"},
		{[]string{"-target-sizes-mb=64,0"}, "-target-sizes-mb"},
		{[]string{"-target-sizes-mb=64,x"}, "-target-sizes-mb"},
		{[]string{"-target-sizes-mb=64,1,1,1,1,1,1,1"}, "8 sizes"},
		{[]string{"-target-sizes-mb=5000"}, "-target-sizes-mb"},
		{[]string{"-target-sizes-mb=32,4"}, "differs from TargetFileSize"},
		{[]string{"-compaction-concurrency=3"}, "-compaction-concurrency"},
		{[]string{"-compaction-concurrency=0,2"}, "-compaction-concurrency"},
		{[]string{"-compaction-concurrency=4,2"}, "-compaction-concurrency"},
		{[]string{"-compaction-concurrency=a,b"}, "-compaction-concurrency"},
		{[]string{"-l0-stop-writes=-1"}, "-l0-stop-writes"},
		{[]string{"-l0-stop-writes=3"}, "below the compaction threshold 4"},
		{[]string{"-l0-stop-writes=6"}, "within 2 sublevels of the compaction threshold 4"},
		{[]string{"-compression=lz4"}, "-compression"},
	} {
		fs := flag.NewFlagSet("t", flag.ContinueOnError)
		var b buildFlags
		b.flags(fs)
		if err := fs.Parse(c.args); err != nil {
			t.Fatalf("%v: %v", c.args, err)
		}
		if err := b.resolve(fs, false); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%v: %v, want a refusal saying %q", c.args, err, c.want)
		}
	}
}

// A run that resumes refuses a candidate built with another shape than its flags say,
// and a manifest without the key is of a build with none.
func TestShapeFlagsAgreeWithTheBuild(t *testing.T) {
	t.Parallel()
	out := t.TempDir()
	dir := runner.CandidateDir(out, "Lroot/k64a2l5m")
	d := map[string]string{runner.RestKey: "false", runner.RetentionModeKey: "background", "store": "root", runner.RetainFlushDelsKey: "32768"}
	writeManifest(t, dir, d)

	none := parseBuild(t, false, "-retention-mode=background")
	if err := none.agreeAll(out); err != nil {
		t.Errorf("a build with no shape, flags with none: %v", err)
	}
	set := parseBuild(t, false, "-retention-mode=background", "-disable-delete-only", "-l0-stop-writes=20")
	if err := set.agreeAll(out); err == nil || !strings.Contains(err.Error(), runner.ShapeKey) {
		t.Errorf("a build with no shape, flags with one: %v", err)
	}
	d[runner.ShapeKey] = set.shape().ShapeString()
	writeManifest(t, dir, d)
	if err := set.agreeAll(out); err != nil {
		t.Errorf("a build with the shape the flags say: %v", err)
	}
	if err := none.agreeAll(out); err == nil || !strings.Contains(err.Error(), runner.ShapeKey) {
		t.Errorf("a build with a shape, flags with none: %v", err)
	}
	other := parseBuild(t, false, "-retention-mode=background", "-disable-delete-only", "-l0-stop-writes=24")
	if err := other.agreeAll(out); err == nil || !strings.Contains(err.Error(), runner.ShapeKey) {
		t.Errorf("a build with another shape: %v", err)
	}
}

// The three signals of compaction concurrency reach the build options and are passed on;
// one written at Pebble's own value is recorded as no shape, and a bad value is refused.
func TestSignalFlagsReachTheBuild(t *testing.T) {
	t.Parallel()
	b := parseBuild(t, false, "-l0-compaction-concurrency=6", "-compaction-debt-concurrency-mb=256", "-compaction-garbage-fraction=0.25")
	want := pebblekv.Tuning{L0CompactionConcurrency: 6, CompactionDebtConcurrency: 256 << 20, CompactionGarbageFraction: 0.25}
	if got := b.options().Shape; got != want {
		t.Errorf("the build options say %+v, want %+v", got, want)
	}
	if got := b.shape().ShapeString(); got != "l0_compaction_concurrency=6 compaction_debt_concurrency=268435456 compaction_garbage_fraction=0.25" {
		t.Errorf("recorded as %q", got)
	}
	for _, p := range []string{"-l0-compaction-concurrency=6", "-compaction-debt-concurrency-mb=256", "-compaction-garbage-fraction=0.25"} {
		if !slices.Contains(b.args(), p) {
			t.Errorf("passed on as %v, want %s among them", b.args(), p)
		}
	}
	if again := parseBuild(t, true, b.args()...); again != b {
		t.Errorf("a step makes %+v, want %+v", again, b)
	}
	own := parseBuild(t, false, "-l0-compaction-concurrency=10", "-compaction-debt-concurrency-mb=1024", "-compaction-garbage-fraction=0.4")
	if got := own.shape().ShapeString(); got != "" {
		t.Errorf("Pebble's own values are recorded as %q", got)
	}
	for _, args := range [][]string{{"-l0-compaction-concurrency=-1"}, {"-compaction-debt-concurrency-mb=-1"}, {"-compaction-garbage-fraction=-0.1"}, {"-compaction-garbage-fraction=1.5"}, {"-compaction-debt-concurrency-mb=1048577"}} {
		fs := flag.NewFlagSet("t", flag.ContinueOnError)
		var b buildFlags
		b.flags(fs)
		if err := fs.Parse(args); err != nil {
			t.Fatal(err)
		}
		if err := b.resolve(fs, false); err == nil || !strings.Contains(err.Error(), strings.SplitN(args[0], "=", 2)[0]) {
			t.Errorf("%v: %v", args, err)
		}
	}
}
