package main

import (
	"slices"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/bench/spike/runner"
)

// -serve-options is for the root store's candidates: it is off by default and passes
// nothing on; given, it reaches the build options and the build steps; it is refused
// for a candidate that is not through the root store before anything is built; and a
// run refuses to resume over a build of the root store made with the other setting.
func TestServeOptionsFlag(t *testing.T) {
	t.Parallel()

	def := parseBuild(t, false)
	if def.options().ServeOptions || slices.Contains(def.args(), "-serve-options") {
		t.Error("a build at the default asks for serve's options")
	}
	b := parseBuild(t, false, "-serve-options")
	if !b.options().ServeOptions {
		t.Error("the build options do not say it")
	}
	if !slices.Contains(b.args(), "-serve-options") {
		t.Errorf("passed on as %v", b.args())
	}
	if again := parseBuild(t, false, b.args()...); again != b {
		t.Errorf("a step makes %+v, want %+v", again, b)
	}

	if err := b.serveCandidates([]string{"Lroot/off", "Lroot/k64a2l1ns"}); err != nil {
		t.Errorf("the root store's candidates: %v", err)
	}
	if err := b.serveCandidates([]string{"Lroot/off", "L/off"}); err == nil || !strings.Contains(err.Error(), "L/off") {
		t.Errorf("a candidate of the spike's own layout: %v", err)
	}
	if err := def.serveCandidates([]string{"L/off"}); err != nil {
		t.Errorf("no option: %v", err)
	}

	// A family is built all with or all without it; a manifest without the key is of a
	// build without; a build not through the root store has none.
	out := t.TempDir()
	dir := runner.CandidateDir(out, "Lroot/off")
	root := func(serve string) map[string]string {
		d := map[string]string{runner.RestKey: "false", "store": "root", runner.RetainFlushDelsKey: "32768"}
		if serve != "" {
			d[runner.ServeOptionsKey] = serve
		}
		return d
	}
	writeManifest(t, dir, root(""))
	if err := def.agreeAll(out); err != nil {
		t.Errorf("a build without, flags without: %v", err)
	}
	if err := b.agreeAll(out); err == nil || !strings.Contains(err.Error(), runner.ServeOptionsKey) {
		t.Errorf("a build without, flags with: %v", err)
	}
	writeManifest(t, dir, root("true"))
	if err := b.agreeAll(out); err != nil {
		t.Errorf("a build with, flags with: %v", err)
	}
	if err := def.agreeAll(out); err == nil || !strings.Contains(err.Error(), runner.ServeOptionsKey) {
		t.Errorf("a build with, flags without: %v", err)
	}
	spike := t.TempDir()
	writeManifest(t, runner.CandidateDir(spike, "L/off"), map[string]string{runner.RestKey: "false"})
	if err := def.agreeAll(spike); err != nil {
		t.Errorf("a build not through the root store: %v", err)
	}
}
