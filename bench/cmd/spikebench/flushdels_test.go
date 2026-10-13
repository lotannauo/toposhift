package main

import (
	"flag"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/runner"
)

// -retain-flush-dels is for a background build of the root store: with a synchronous
// retention, or none given, or a negative count, it is refused naming the flag; for a
// candidate that is not through the root store it is refused before anything is built;
// it is passed on to the build steps and to the build; and a run refuses to resume over a
// build through the root store made at another threshold, or that recorded none.
func TestRetainFlushDelsFlag(t *testing.T) {
	t.Parallel()

	for _, c := range []struct {
		args []string
		want string
	}{
		{[]string{"-retain-flush-dels=24576", "-retention-mode=background"}, ""},
		{[]string{"-retain-flush-dels=0"}, ""},
		{[]string{"-retain-flush-dels=24576"}, "needs -retention-mode background"},
		{[]string{"-retain-flush-dels=24576", "-retention-mode=sync"}, "needs -retention-mode background"},
		{[]string{"-retain-flush-dels=-1", "-retention-mode=background"}, "-retain-flush-dels -1"},
	} {
		fs := flag.NewFlagSet("t", flag.ContinueOnError)
		var b buildFlags
		b.flags(fs)
		if err := fs.Parse(c.args); err != nil {
			t.Fatal(err)
		}
		err := b.resolve(fs, false)
		switch {
		case c.want == "" && err != nil:
			t.Errorf("%v: refused: %v", c.args, err)
		case c.want != "" && (err == nil || !strings.Contains(err.Error(), c.want)):
			t.Errorf("%v: %v, want a refusal saying %q", c.args, err, c.want)
		}
	}

	b := parseBuild(t, false, "-retain-flush-dels=24576", "-retention-mode=background")
	if got := b.options().RetainFlushDels; got != 24576 {
		t.Errorf("the build options say %d", got)
	}
	if !slices.Contains(b.args(), "-retain-flush-dels=24576") {
		t.Errorf("passed on as %v", b.args())
	}
	if again := parseBuild(t, false, b.args()...); again != b {
		t.Errorf("a step makes %+v, want %+v", again, b)
	}
	if slices.ContainsFunc(parseBuild(t, false).args(), func(a string) bool { return strings.HasPrefix(a, "-retain-flush-dels") }) {
		t.Error("a build at the default passes -retain-flush-dels on")
	}

	if err := b.flushDelsCandidates([]string{"Lroot/off", "Lroot/k64a2l1ns"}); err != nil {
		t.Errorf("the root store's candidates: %v", err)
	}
	if err := b.flushDelsCandidates([]string{"Lroot/off", "L/off"}); err == nil || !strings.Contains(err.Error(), "L/off") {
		t.Errorf("a candidate of the spike's own layout: %v", err)
	}
	if err := parseBuild(t, false).flushDelsCandidates([]string{"L/off"}); err != nil {
		t.Errorf("no threshold: %v", err)
	}

	// a family is built all at one threshold, and a build that recorded none is not taken
	// for one at the default; a build not through the root store has none
	out := t.TempDir()
	root := func(v string) map[string]string {
		d := map[string]string{runner.RestKey: "false", runner.RetentionModeKey: "background", "store": "root"}
		if v != "" {
			d[runner.RetainFlushDelsKey] = v
		}
		return d
	}
	dir := runner.CandidateDir(out, "Lroot/off")
	writeManifest(t, dir, root("32768"))
	if err := b.agreeAll(out); err == nil || !strings.Contains(err.Error(), runner.RetainFlushDelsKey) {
		t.Errorf("a build at the default, flags at 24576: %v", err)
	}
	writeManifest(t, dir, root("24576"))
	if err := b.agreeAll(out); err != nil {
		t.Errorf("a build at the threshold the flags say: %v", err)
	}
	def := parseBuild(t, false, "-retention-mode=background")
	if err := def.agreeAll(out); err == nil {
		t.Error("a build at 24576, flags at the default: agreed")
	}
	writeManifest(t, dir, root(""))
	if err := def.agreeAll(out); err == nil || !strings.Contains(err.Error(), "not recorded") {
		t.Errorf("a build that recorded no threshold, flags at the default: %v", err)
	}
	writeManifest(t, dir, root("32768"))
	if err := def.agreeAll(out); err != nil {
		t.Errorf("a build at the default, flags at the default: %v", err)
	}
	spike := t.TempDir()
	writeManifest(t, runner.CandidateDir(spike, "L/off"), map[string]string{runner.RestKey: "false"})
	if err := parseBuild(t, false).agreeAll(spike); err != nil {
		t.Errorf("a build not through the root store: %v", err)
	}
}

// storegate refuses, with status 3, paced builds in the background without -commit, a
// -commit that is not 40 hexadecimal digits, and builds of another commit; and takes the
// same builds with their own commit.
func TestStoreGateNeedsTheCommitOfPacedBuilds(t *testing.T) {
	t.Parallel()

	var h runner.Histogram
	h.Add(time.Millisecond)
	trace := &runner.GateTrace{
		Mode:        "background",
		PaceStartNs: 1, Windows: []runner.GateWindow{{StartNs: 1, RewriteEndNs: 1e9 + 1, SettleEndNs: 1e9 + 1, ReturnNs: 1}},
		Inside:  runner.GateBatches{Batches: 1, Records: 4000, Commit: h},
		Outside: runner.GateBatches{Batches: 1, Records: 4_000_000, Commit: h},
		WriteNs: 1000e9, KeepNs: int64(30 * 24 * time.Hour), PeakRSS: 1 << 30,
	}
	root := t.TempDir()
	writeGateBuild(t, root, "Lroot/k64a2l1ns", trace, 1<<30)
	var out, errOut strings.Builder
	for _, c := range []struct {
		args []string
		want string
	}{
		{nil, "-commit"},
		{[]string{"-commit", "0123456"}, "not 40 lowercase hexadecimal digits"},
		{[]string{"-commit", strings.Repeat("f", 40)}, "was built from revision"},
	} {
		err := runStoreGate(append(c.args, root), &out, &errOut)
		if exitCode(err) != 3 || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%v: status %d (%v), want 3 saying %q", c.args, exitCode(err), err, c.want)
		}
	}
	out.Reset()
	if err := runStoreGate([]string{"-commit", gateCommit, root}, &out, &errOut); exitCode(err) == 3 {
		t.Errorf("the builds' own commit: %v", err)
	}
	if !strings.Contains(out.String(), "retain_flush_dels") {
		t.Errorf("the report does not give the threshold:\n%s", out.String())
	}
}
