package runner_test

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/bench/spike/candidates"
	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	rootkv "github.com/lotannauo/toposhift/internal/store/pebblekv"
)

// optionLines are the lines of the Pebble options a manifest records, in order, with
// the level a line is under in front of the keys of a level's section ("L3.").
func optionLines(t *testing.T, m *runner.Manifest) []string {
	t.Helper()
	text := m.Describe["pebble_options"]
	if text == "" {
		t.Fatal("the manifest records no Pebble options")
	}
	var out []string
	level := ""
	for _, l := range strings.Split(text, "\n") {
		l = strings.TrimSpace(l)
		if rest, ok := strings.CutPrefix(l, "[Level \""); ok {
			level = "L" + strings.TrimSuffix(rest, "\"]") + "."
			continue
		}
		if l != "" && !strings.HasPrefix(l, "[") {
			out = append(out, level+l)
		}
	}
	return out
}

func has(lines []string, line string) bool {
	for _, l := range lines {
		if l == line {
			return true
		}
	}
	return false
}

// The table shape and compaction settings of a build reach the database of any
// candidate that is built through Pebble, root store or not, and are recorded: the key
// with the words of the settings, and the Pebble options with what they come to. The
// plan's block cache and the tuning's sizes still apply. A build without any records
// no key and Pebble's own defaults.
func TestABuildRecordsItsTableShape(t *testing.T) {
	t.Parallel()

	plan := mustPlan(t, tinySpec())
	const mib = 1 << 20
	shape := pebblekv.Tuning{
		TargetFileSizes:            [7]int64{64 * mib, 4 * mib, 8 * mib},
		CompactionConcurrencyLower: 1, CompactionConcurrencyUpper: 3,
		L0StopWritesThreshold:        20,
		DisableDeleteOnlyCompactions: true,
		Compression:                  mustCompression(t, "zstd"),
	}
	const words = "target_file_sizes=67108864,4194304,8388608,0,0,0,0 compaction_concurrency=1-3 l0_stop_writes_threshold=20 delete_only_compactions=off compression=zstd"
	for _, c := range []struct{ name, mode string }{
		{"Lroot/k64a2l1ns", candidates.RetentionBackground},
		{"L/k64a2l1ns", ""},
	} {
		m, err := runner.BuildWith(context.Background(), plan, lookup(t, c.name), runner.CandidateDir(t.TempDir(), c.name), clean,
			runner.BuildOptions{RetentionMode: c.mode, Shape: shape}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if got := m.Describe[runner.ShapeKey]; got != words {
			t.Errorf("%s: the build records %q, want %q", c.name, got, words)
		}
		lines := optionLines(t, m)
		for _, want := range []string{
			"concurrent_compactions=1", "max_concurrent_compactions=3", "l0_stop_writes_threshold=20",
			"disable_delete_only_compactions=true",
			"L0.compression=ZSTD", "L6.compression=ZSTD",
			"L0.target_file_size=67108864", "L1.target_file_size=4194304", "L2.target_file_size=8388608", "L3.target_file_size=16777216",
		} {
			if !has(lines, want) {
				t.Errorf("%s: the recorded options lack %q", c.name, want)
			}
		}
		if got := m.Describe["target_file_bytes"]; got != "67108864" {
			t.Errorf("%s: the tuning's target is %q, want the bench tuning's 64 MiB", c.name, got)
		}
		if got := m.Describe["cache_bytes"]; got != strconv.FormatInt(plan.CacheBytes, 10) {
			t.Errorf("%s: the block cache is %q, want the plan's", c.name, got)
		}
	}

	// And without a shape: no key, and the options Pebble has by default.
	m, err := runner.BuildWith(context.Background(), plan, lookup(t, "Lroot/k64a2l1ns"), runner.CandidateDir(t.TempDir(), "x"), clean,
		runner.BuildOptions{RetentionMode: candidates.RetentionBackground}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := m.Describe[runner.ShapeKey]; ok {
		t.Errorf("a build without a shape records %q", got)
	}
	lines := optionLines(t, m)
	for _, want := range []string{
		"concurrent_compactions=1", "max_concurrent_compactions=1", "l0_stop_writes_threshold=12",
		"L0.compression=Snappy", "L6.compression=Snappy", "L1.target_file_size=134217728",
	} {
		if !has(lines, want) {
			t.Errorf("a build without a shape lacks %q in its options", want)
		}
	}
	for _, l := range lines {
		if strings.Contains(l, "disable_delete_only") {
			t.Errorf("a build without a shape names %q", l)
		}
	}
}

// A shape the store would refuse is refused before anything is built, with the store's
// message, and leaves no directory.
func TestABuildRefusesAShapeTheStoreRefuses(t *testing.T) {
	t.Parallel()

	plan := mustPlan(t, tinySpec())
	const name = "Lroot/k64a2l1ns"
	for _, c := range []struct {
		label string
		shape pebblekv.Tuning
		want  string
	}{
		{"a negative base-level target", pebblekv.Tuning{LBaseTargetFileSize: -1}, "base-level target"},
		{"a stop threshold below the compaction threshold", pebblekv.Tuning{L0StopWritesThreshold: 3}, "below the compaction threshold 4"},
		{"a lower bound above the upper", pebblekv.Tuning{CompactionConcurrencyLower: 4, CompactionConcurrencyUpper: 2}, "above the upper bound"},
		{"a level-0 size that is not the tuning's", pebblekv.Tuning{TargetFileSizes: [7]int64{32 << 20}}, "differs from TargetFileSize"},
	} {
		dir := runner.CandidateDir(t.TempDir(), name)
		_, err := runner.BuildWith(context.Background(), plan, lookup(t, name), dir, clean,
			runner.BuildOptions{RetentionMode: candidates.RetentionBackground, Shape: c.shape}, nil)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v, want a refusal saying %q", c.label, err, c.want)
		}
		if _, err := os.Stat(dir); err == nil {
			t.Errorf("%s: the build made %s before it was refused", c.label, dir)
		}
	}
}

func mustCompression(t *testing.T, name string) rootkv.Compression {
	t.Helper()
	c, err := rootkv.ParseCompression(name)
	if err != nil {
		t.Fatal(err)
	}
	return c
}
