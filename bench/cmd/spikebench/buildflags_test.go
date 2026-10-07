package main

import (
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/runner"
)

func parseBuild(t *testing.T, untimed bool, args ...string) buildFlags {
	t.Helper()
	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	var b buildFlags
	b.flags(fs)
	if err := fs.Parse(args); err != nil {
		t.Fatal(err)
	}
	if err := b.resolve(fs, untimed); err != nil {
		t.Fatal(err)
	}
	return b
}

// The rest after each retention is on by default for an untimed build and off for
// a timed one, unless it is given; the canonical layout, the sync of every commit and
// the sampling of the metrics are off unless given. What is passed on to a build step
// says all of them, and makes the same choices whatever the step's own -untimed.
func TestBuildFlagsDefaultsAndWhatIsPassedOn(t *testing.T) {
	t.Parallel()

	for _, c := range []struct {
		untimed bool
		args    []string
		want    buildFlags
	}{
		{true, nil, buildFlags{rest: true}},
		{false, nil, buildFlags{}},
		{true, []string{"-rest-after-retention=false"}, buildFlags{}},
		{false, []string{"-rest-after-retention"}, buildFlags{rest: true}},
		{true, []string{"-canonical-layout"}, buildFlags{rest: true, canonical: true}},
		{false, []string{"-canonical-layout=true", "-rest-after-retention=false"}, buildFlags{canonical: true}},
		{true, []string{"-sync"}, buildFlags{rest: true, sync: true}},
		{false, []string{"-sync"}, buildFlags{sync: true}},
		{false, []string{"-sync=false"}, buildFlags{}},
		{true, []string{"-metrics-every", "30s"}, buildFlags{rest: true, metricsEvery: 30 * time.Second}},
		{false, []string{"-metrics-every=1m30s"}, buildFlags{metricsEvery: 90 * time.Second}},
		{false, []string{"-metrics-every=0"}, buildFlags{}},
		{false, []string{"-sync", "-metrics-every", "30s", "-canonical-layout"}, buildFlags{canonical: true, sync: true, metricsEvery: 30 * time.Second}},
		{true, []string{"-sync", "-metrics-every=1s", "-canonical-layout", "-rest-after-retention=false"}, buildFlags{canonical: true, sync: true, metricsEvery: time.Second}},
	} {
		got := parseBuild(t, c.untimed, c.args...)
		if got != c.want {
			t.Errorf("untimed %v, %v: %+v, want %+v", c.untimed, c.args, got, c.want)
		}
		for _, untimed := range []bool{false, true} {
			if again := parseBuild(t, untimed, got.args()...); again != got {
				t.Errorf("untimed %v, %v: passed on as %v, a step makes %+v", c.untimed, c.args, got.args(), again)
			}
		}
	}
}

// A sampling interval of a second or more, or none, is accepted; one that is
// negative or shorter than a second is refused, naming the flag.
func TestMetricsEveryIsAtLeastASecondOrNone(t *testing.T) {
	t.Parallel()

	for _, c := range []struct {
		arg    string
		accept bool
	}{
		{"0", true},
		{"1s", true},
		{"30s", true},
		{"1h", true},
		{"500ms", false},
		{"999ms", false},
		{"1ns", false},
		{"-1s", false},
		{"-1ms", false},
	} {
		fs := flag.NewFlagSet("t", flag.ContinueOnError)
		var b buildFlags
		b.flags(fs)
		if err := fs.Parse([]string{"-metrics-every=" + c.arg}); err != nil {
			t.Fatal(err)
		}
		err := b.resolve(fs, false)
		switch {
		case c.accept && err != nil:
			t.Errorf("-metrics-every=%s: refused: %v, want accepted", c.arg, err)
		case !c.accept && (err == nil || !strings.Contains(err.Error(), "-metrics-every")):
			t.Errorf("-metrics-every=%s: %v, want a refusal naming -metrics-every", c.arg, err)
		}
	}
}

// A run passes the build flags to every build it starts, and to nothing else.
func TestRunPassesTheBuildFlagsToItsBuilds(t *testing.T) {
	t.Parallel()

	var s steps
	b := buildFlags{canonical: true, sync: true, metricsEvery: 30 * time.Second}
	runCandidates(t.Context(), []string{"A", "B"}, b.forward(s.step), nothingOnDisk)
	want := []string{
		"build -candidate A -rest-after-retention=false -canonical-layout=true -sync=true -metrics-every=30s", "read -candidate A",
		"build -candidate B -rest-after-retention=false -canonical-layout=true -sync=true -metrics-every=30s", "read -candidate B",
	}
	if !slices.Equal(s.calls, want) {
		t.Errorf("calls %v, want %v", s.calls, want)
	}
}

func writeManifest(t *testing.T, dir string, describe map[string]string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(runner.Manifest{Describe: describe})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, runner.ManifestFile), b, 0o644); err != nil {
		t.Fatal(err)
	}
}

// A run that resumes refuses a candidate built otherwise than its flags say, before
// it builds anything; a manifest that does not record the canonical layout is of a
// build without it, and one that does not record the rest agrees with neither.
func TestARunRefusesToResumeOverABuildMadeOtherwise(t *testing.T) {
	t.Parallel()

	out := t.TempDir()
	// every candidate with a manifest counts, not only those a run names
	plain := buildFlags{rest: true}
	if err := plain.agreeAll(out); err != nil {
		t.Errorf("nothing built: %v", err)
	}
	writeManifest(t, runner.CandidateDir(out, "M/crdb1"), map[string]string{runner.RestKey: "true", runner.CanonicalKey: "false"})
	if err := plain.agreeAll(out); err != nil {
		t.Errorf("built alike: %v", err)
	}
	for _, c := range []struct {
		b    buildFlags
		want string
	}{
		{buildFlags{rest: true, canonical: true}, "canonical_layout"},
		{buildFlags{}, "rest_after_retention"},
	} {
		if err := c.b.agreeAll(out); err == nil || !strings.Contains(err.Error(), c.want) || !strings.Contains(err.Error(), "M_crdb1") {
			t.Errorf("%+v over a plain rested build: %v", c.b, err)
		}
	}
	writeManifest(t, runner.CandidateDir(out, "M/crdb1"), map[string]string{runner.RestKey: "true"})
	if err := plain.agreeAll(out); err != nil {
		t.Errorf("a manifest without the canonical key, asked for none: %v", err)
	}
	if err := (buildFlags{rest: true, canonical: true}).agreeAll(out); err == nil {
		t.Error("a manifest without the canonical key, asked for it: accepted")
	}
	writeManifest(t, runner.CandidateDir(out, "M/crdb1"), map[string]string{runner.CanonicalKey: "false"})
	if err := plain.agreeAll(out); err == nil || !strings.Contains(err.Error(), "rest_after_retention") {
		t.Errorf("a manifest without the rest key: %v", err)
	}

	// The sync of every commit and the sampling of the metrics: a manifest that says
	// one is refused by flags that say another, naming it; a manifest without the key
	// is of a build without it.
	for _, c := range []struct {
		name     string
		describe map[string]string
		b        buildFlags
		want     string // the key the refusal names, or "" for none
	}{
		{"synced build, flags without sync", map[string]string{runner.RestKey: "true", runner.SyncKey: "true"}, plain, "sync"},
		{"unsynced build, flags with sync", map[string]string{runner.RestKey: "true", runner.SyncKey: "false"}, buildFlags{rest: true, sync: true}, "sync"},
		{"synced build, flags with sync", map[string]string{runner.RestKey: "true", runner.SyncKey: "true"}, buildFlags{rest: true, sync: true}, ""},
		{"no sync key, flags without sync", map[string]string{runner.RestKey: "true"}, plain, ""},
		{"no sync key, flags with sync", map[string]string{runner.RestKey: "true"}, buildFlags{rest: true, sync: true}, "sync"},
		{"sampled build, flags without sampling", map[string]string{runner.RestKey: "true", runner.MetricsKey: "30s"}, plain, "metrics_every"},
		{"unsampled build, flags with sampling", map[string]string{runner.RestKey: "true", runner.MetricsKey: "0s"}, buildFlags{rest: true, metricsEvery: 30 * time.Second}, "metrics_every"},
		{"sampled build, another interval", map[string]string{runner.RestKey: "true", runner.MetricsKey: "30s"}, buildFlags{rest: true, metricsEvery: time.Minute}, "metrics_every"},
		{"sampled build, flags with its interval", map[string]string{runner.RestKey: "true", runner.MetricsKey: "30s"}, buildFlags{rest: true, metricsEvery: 30 * time.Second}, ""},
		{"no metrics key, flags without sampling", map[string]string{runner.RestKey: "true"}, plain, ""},
		{"no metrics key, flags with sampling", map[string]string{runner.RestKey: "true"}, buildFlags{rest: true, metricsEvery: 30 * time.Second}, "metrics_every"},
	} {
		writeManifest(t, runner.CandidateDir(out, "M/crdb1"), c.describe)
		err := c.b.agreeAll(out)
		switch {
		case c.want == "" && err != nil:
			t.Errorf("%s: refused: %v, want accepted", c.name, err)
		case c.want != "" && (err == nil || !strings.Contains(err.Error(), c.want)):
			t.Errorf("%s: %v, want a refusal naming %s", c.name, err, c.want)
		}
	}
}

// Through the program: a run told to rewrite its tables and not to rest builds every
// candidate so, and the same run asked for the defaults afterwards refuses, before it
// builds anything, to resume over those builds.
func TestARunBuildsItsFamilyAlike(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t) // builds a binary and runs it; the full tier does

	bin := binary(t)
	out := filepath.Join(t.TempDir(), "out")
	if _, stderr, err := run(t, bin, "run", "-untimed", "-preset", "tiny", "-events-per-second", "0.05", "-batch", "40",
		"-candidates", "L/off,M/crdb1", "-canonical-layout", "-rest-after-retention=false", "-out", out); err != nil {
		t.Fatalf("run: %v\n%.2000s", err, stderr)
	}
	for _, c := range []string{"L_off", "M_crdb1"} {
		m, err := runner.LoadManifest(filepath.Join(out, c))
		if err != nil {
			t.Fatal(err)
		}
		if m.Describe[runner.CanonicalKey] != "true" || m.Describe[runner.RestKey] != "false" {
			t.Errorf("%s: canonical_layout %q, rest_after_retention %q", c, m.Describe[runner.CanonicalKey], m.Describe[runner.RestKey])
		}
	}
	if err := os.RemoveAll(filepath.Join(out, "M_crdb1")); err != nil {
		t.Fatal(err)
	}
	_, stderr, err := run(t, bin, "run", "-untimed", "-candidates", "L/off,M/crdb1", "-out", out)
	if err == nil || !strings.Contains(stderr, "canonical_layout") || !strings.Contains(stderr, "a family is built all with or all without it") {
		t.Errorf("a resumed run asking for the defaults over canonical builds: %v\n%.2000s", err, stderr)
	}
	if _, err := os.Stat(filepath.Join(out, "M_crdb1")); err == nil {
		t.Error("the refused run built the candidate it was missing")
	}
}

// A candidate directory that is a link to the directory is checked like one.
func TestAgreeAllFollowsALinkToACandidate(t *testing.T) {
	t.Parallel()

	real := t.TempDir()
	writeManifest(t, real, map[string]string{runner.RestKey: "true", runner.CanonicalKey: "false"})
	out := t.TempDir()
	if err := os.Symlink(real, filepath.Join(out, "M_crdb1")); err != nil {
		t.Skipf("no symbolic links here: %v", err)
	}
	if err := (buildFlags{rest: true}).agreeAll(out); err != nil {
		t.Errorf("a linked candidate built alike: %v", err)
	}
	if err := (buildFlags{rest: true, canonical: true}).agreeAll(out); err == nil || !strings.Contains(err.Error(), "canonical_layout") {
		t.Errorf("a linked candidate built plain, asked for canonical: %v", err)
	}
}
