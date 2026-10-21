package runner_test

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/bench/spike/candidates"
	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
	"github.com/lotannauo/toposhift/bench/spike/runner"
)

// A build through the root store with serve's options records it in the manifest, one
// without records nothing of it, and a candidate that is not built through the root
// store refuses it before anything is built.
func TestABuildWithServesOptionsRecordsThemAndOnlyTheRootStoreTakesThem(t *testing.T) {
	plan := windowOfADayPlan(t)
	ctx := context.Background()
	v := lookup(t, "Lroot/k64a2l1ns")

	dir := runner.CandidateDir(t.TempDir(), v.Name)
	m, err := runner.BuildWith(ctx, plan, v, dir, clean, runner.BuildOptions{ServeOptions: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	// A read opens the database as it was built, from the manifest.
	if _, err := runner.Read(ctx, plan, v, dir, clean, nil); err != nil {
		t.Errorf("reading a database built with serve's options: %v", err)
	}
	if m.Describe[runner.ServeOptionsKey] != "true" {
		t.Errorf("described with %s %q", runner.ServeOptionsKey, m.Describe[runner.ServeOptionsKey])
	}

	m, err = runner.BuildWith(ctx, plan, v, runner.CandidateDir(t.TempDir(), v.Name), clean, runner.BuildOptions{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := m.Describe[runner.ServeOptionsKey]; ok {
		t.Errorf("a build without serve's options is described with %s %q", runner.ServeOptionsKey, got)
	}

	other := lookup(t, "L/k64a2l1ns")
	dir = runner.CandidateDir(t.TempDir(), other.Name)
	if _, err := runner.BuildWith(ctx, plan, other, dir, clean, runner.BuildOptions{ServeOptions: true}, nil); err == nil || !strings.Contains(err.Error(), "root store") {
		t.Errorf("a candidate not built through the root store took serve's options: %v", err)
	}
}

// Serve's options and a table shape are separate settings of one build: a build with
// both records both, its database opens read-only as it was built, and the store gate
// pools its repetitions only with builds that have both alike, refusing a family that
// mixes in a build without either, as it does for each setting alone.
func TestABuildWithServesOptionsAndATableShapeRecordsBothAndPoolsOnlyWithBoth(t *testing.T) {
	t.Parallel()

	plan := mustPlan(t, tinySpec())
	ctx := context.Background()
	v := lookup(t, "Lroot/k64a2l1ns")
	shape := pebblekv.Tuning{DisableDeleteOnlyCompactions: true, Compression: mustCompression(t, "zstd")}
	words := shape.ShapeString()
	if !strings.Contains(words, "compression=zstd") {
		t.Fatalf("the shape's words are %q", words)
	}

	dir := runner.CandidateDir(t.TempDir(), v.Name)
	m, err := runner.BuildWith(ctx, plan, v, dir, clean,
		runner.BuildOptions{RetentionMode: candidates.RetentionBackground, ServeOptions: true, Shape: shape}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := m.Describe[runner.ServeOptionsKey]; got != "true" {
		t.Errorf("described with %s %q, want \"true\"", runner.ServeOptionsKey, got)
	}
	if got := m.Describe[runner.ShapeKey]; got != words {
		t.Errorf("described with %s %q, want %q", runner.ShapeKey, got, words)
	}
	lines := optionLines(t, m)
	for _, want := range []string{"disable_delete_only_compactions=true", "L0.compression=ZSTD"} {
		if !has(lines, want) {
			t.Errorf("the recorded Pebble options lack %q", want)
		}
	}
	// A read opens the database read-only from the manifest, with serve's options, and
	// finds it as it was built.
	if _, err := runner.Read(ctx, plan, v, dir, clean, nil); err != nil {
		t.Errorf("reading a database built with serve's options and a shape: %v", err)
	}

	// Repetitions in the store gate, each with the two settings as this build recorded
	// them or with one of them left out.
	rep := func(n int, drop ...string) *runner.StoreGateBuild {
		return synth(v.Name, func(b *runner.StoreGateBuild) {
			b.Dir += "/rep" + strconv.Itoa(n)
			b.Job = &runner.Job{RunID: "7", RunAttempt: "1", Rep: n}
			b.Manifest.Describe[runner.RetainFlushDelsKey] = "32768"
			for _, k := range []string{runner.ServeOptionsKey, runner.ShapeKey} {
				b.Manifest.Describe[k] = m.Describe[k]
			}
			for _, k := range drop {
				delete(b.Manifest.Describe, k)
			}
		})
	}
	if err := runner.CheckStoreGateSettings([]*runner.StoreGateBuild{rep(1), rep(2)}); err != nil {
		t.Errorf("two repetitions with both: %v", err)
	}
	for _, c := range []struct {
		name string
		drop []string
		want []string
	}{
		{"a repetition without the shape", []string{runner.ShapeKey}, []string{"different pebble_shape", `pebble_shape="none"`, strconv.Quote(words)}},
		{"a repetition without serve's options", []string{runner.ServeOptionsKey}, []string{"different serve_options", `serve_options="true"`, `serve_options="false"`}},
		// Both differ: the first setting of the gate's order that differs is named.
		{"a repetition without either", []string{runner.ServeOptionsKey, runner.ShapeKey}, []string{"different pebble_shape"}},
	} {
		err := runner.CheckStoreGateSettings([]*runner.StoreGateBuild{rep(1), rep(2, c.drop...)})
		if err == nil {
			t.Errorf("%s: pooled", c.name)
			continue
		}
		for _, w := range c.want {
			if !strings.Contains(err.Error(), w) {
				t.Errorf("%s: %v, want it to say %s", c.name, err, w)
			}
		}
	}
}
