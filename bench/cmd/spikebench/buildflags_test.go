package main

import (
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

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
	b.resolve(fs, untimed)
	return b
}

// The rest after each retention is on by default for an untimed build and off for
// a timed one, unless it is given; the canonical layout is off unless it is given.
// What is passed on to a build step says both, and makes the same choices whatever
// the step's own -untimed.
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

// A run passes the build flags to every build it starts, and to nothing else.
func TestRunPassesTheBuildFlagsToItsBuilds(t *testing.T) {
	t.Parallel()

	var s steps
	b := buildFlags{canonical: true}
	runCandidates(t.Context(), []string{"A", "B"}, b.forward(s.step), nothingOnDisk)
	want := []string{
		"build -candidate A -rest-after-retention=false -canonical-layout=true", "read -candidate A",
		"build -candidate B -rest-after-retention=false -canonical-layout=true", "read -candidate B",
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
