package runner_test

import (
	"context"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/bench/spike/candidates"
	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/runner"
)

// external is a candidate defined outside the registry that opens what L/off opens.
func external(t *testing.T, name string, cgo bool) candidates.Variant {
	t.Helper()
	base := lookup(t, "L/off")
	v, err := candidates.External(name, "L", cgo, func(dir string, o candidates.Options) (engine.Engine, error) {
		return base.Open(dir, o)
	})
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// goodInfo is a build a result may come from but for what a test changes in it.
var goodInfo = runner.BuildInfo{GoVersion: "test", GOOS: "test", GOARCH: "test", Revision: "r"}

// A binary built with cgo is refused for every candidate of the registry, and is
// accepted only for one made by External that declares cgo. Everything else that
// refuses a binary still refuses it for that one.
func TestCheckForAcceptsCgoOnlyForAnExternalCandidateThatDeclaresIt(t *testing.T) {
	t.Parallel()

	withCGO := goodInfo
	withCGO.CGO = true

	builtIn := candidates.All()
	for _, name := range []string{"L/k32a2", "L/k64a2l1nsr4", "Lroot/off", "Lroot/k64a2l1ns"} {
		builtIn = append(builtIn, lookup(t, name))
	}
	builtIn = append(builtIn, candidates.Wrap(lookup(t, "L/off"), "L/wrapped", func(e engine.Engine, _ candidates.Options) (engine.Engine, error) { return e, nil }))
	builtIn = append(builtIn, candidates.Variant{})
	for _, v := range builtIn {
		if v.CGO() {
			t.Errorf("%q declares cgo", v.Name)
		}
		err := runner.Guards{Info: withCGO}.CheckFor(v)
		if err == nil || !strings.Contains(err.Error(), "cgo") {
			t.Errorf("%q with a cgo binary: %v, want a refusal for cgo", v.Name, err)
		}
		if err := (runner.Guards{Info: goodInfo}).CheckFor(v); err != nil {
			t.Errorf("%q with a clean binary: %v", v.Name, err)
		}
	}

	declared, plain := external(t, "X/declared", true), external(t, "X/plain", false)
	if !declared.CGO() || plain.CGO() {
		t.Fatalf("CGO() is %v for the one that declares cgo and %v for the one that does not", declared.CGO(), plain.CGO())
	}
	if err := (runner.Guards{Info: withCGO}).CheckFor(declared); err != nil {
		t.Errorf("an external candidate that declares cgo, with a cgo binary: %v", err)
	}
	if err := (runner.Guards{Info: goodInfo}).CheckFor(declared); err != nil {
		t.Errorf("an external candidate that declares cgo, with a binary without cgo: %v", err)
	}
	if err := (runner.Guards{Info: withCGO}).CheckFor(plain); err == nil || !strings.Contains(err.Error(), "cgo") {
		t.Errorf("an external candidate that does not declare cgo, with a cgo binary: %v, want a refusal for cgo", err)
	}
	if err := (runner.Guards{Info: goodInfo}).CheckFor(plain); err != nil {
		t.Errorf("an external candidate that does not declare cgo, with a clean binary: %v", err)
	}
	// A wrapper of a declared candidate is still that candidate.
	wrapped := candidates.Wrap(declared, "X/wrapped", func(e engine.Engine, _ candidates.Options) (engine.Engine, error) { return e, nil })
	if !wrapped.CGO() {
		t.Error("a wrapper of a candidate that declares cgo does not")
	}
	if err := (runner.Guards{Info: withCGO}).CheckFor(wrapped); err != nil {
		t.Errorf("a wrapper of a candidate that declares cgo, under another external name: %v", err)
	}

	// But under the name of a candidate of the registry, whether by Wrap or by
	// renaming a copy, the declaration does not count: the binary's cgo refuses it.
	passthrough := func(e engine.Engine, _ candidates.Options) (engine.Engine, error) { return e, nil }
	renamed := declared
	renamed.Name = "L/off"
	impostors := []candidates.Variant{
		candidates.Wrap(declared, "L/off", passthrough), candidates.Wrap(declared, "L/k64a4l1ns", passthrough),
		candidates.Wrap(declared, "Lroot/off", passthrough), candidates.Wrap(declared, "M/crdb1", passthrough),
		candidates.Wrap(declared, "M/anything", passthrough), candidates.Wrap(declared, "", passthrough), renamed,
	}
	for _, v := range impostors {
		if !v.CGO() {
			t.Fatalf("%q lost its declaration: the test does not reach the name check", v.Name)
		}
		if err := (runner.Guards{Info: withCGO}).CheckFor(v); err == nil || !strings.Contains(err.Error(), "cgo") {
			t.Errorf("a cgo binary for the declared candidate under the name %q: %v, want a refusal for cgo", v.Name, err)
		}
		if err := (runner.Guards{Info: goodInfo}).CheckFor(v); err != nil {
			t.Errorf("a clean binary for the declared candidate under the name %q: %v", v.Name, err)
		}
	}

	// What else refuses a binary refuses it for every candidate, with cgo or without.
	for name, c := range map[string]struct {
		info runner.BuildInfo
		want string
	}{
		"race":        {runner.BuildInfo{Race: true, Revision: "r"}, "race detector"},
		"invariants":  {runner.BuildInfo{Invariants: true, Revision: "r"}, "invariants"},
		"unoptimized": {runner.BuildInfo{Unoptimized: true, Revision: "r"}, "optimization or inlining off"},
		"dirty":       {runner.BuildInfo{Modified: true, Revision: "r"}, "uncommitted"},
		"no stamp":    {runner.BuildInfo{}, "version control stamp"},
	} {
		for _, cgo := range []bool{false, true} {
			info := c.info
			info.CGO = cgo
			for _, v := range []candidates.Variant{declared, plain, lookup(t, "L/off")} {
				err := runner.Guards{Info: info}.CheckFor(v)
				if err == nil || !strings.Contains(err.Error(), c.want) {
					t.Errorf("%s, cgo %v, %q: %v, want a refusal for %q", name, cgo, v.Name, err, c.want)
				}
				if err := (runner.Guards{Info: info, Untimed: true}).CheckFor(v); err != nil {
					t.Errorf("%s, cgo %v, %q, untimed: %v", name, cgo, v.Name, err)
				}
			}
		}
	}

	// For a candidate that does not declare cgo, CheckFor is Check, down to its words.
	for _, cgo := range []bool{false, true} {
		for _, rest := range []runner.BuildInfo{goodInfo, {}, {Race: true, Invariants: true, Revision: "r"}, {Modified: true, Unoptimized: true}} {
			info := rest
			info.CGO = cgo
			g := runner.Guards{Info: info}
			for _, v := range []candidates.Variant{lookup(t, "L/off"), plain} {
				got, want := g.CheckFor(v), g.Check()
				if (got == nil) != (want == nil) || got != nil && got.Error() != want.Error() {
					t.Errorf("%q, %+v: CheckFor is %v, Check is %v", v.Name, info, got, want)
				}
			}
		}
	}

	// Without the allowance the reason for a binary that is also dirty is both.
	both := runner.BuildInfo{CGO: true, Modified: true, Revision: "r"}
	if err := (runner.Guards{Info: both}).CheckFor(declared); err == nil || strings.Contains(err.Error(), "cgo") || !strings.Contains(err.Error(), "uncommitted") {
		t.Errorf("a dirty cgo binary for a candidate that declares cgo: %v, want the dirty tree only", err)
	}
}

// BuildWith and Read apply the allowance: a cgo binary builds and reads a database
// of an external candidate that declares cgo, and is refused for the others, and
// the build records that it had cgo.
func TestBuildAndReadApplyTheCgoAllowance(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t)

	plan := mustPlan(t, tinySpec())
	cgoGuards := runner.Guards{Info: goodInfo}
	cgoGuards.Info.CGO = true
	ctx := context.Background()
	root := t.TempDir()

	// A candidate of the registry is refused by both.
	builtIn := lookup(t, "L/off")
	if _, err := runner.Build(ctx, plan, builtIn, runner.CandidateDir(root, "builtin"), cgoGuards, nil); err == nil || !strings.Contains(err.Error(), "cgo") {
		t.Errorf("a cgo build of a candidate of the registry: %v", err)
	}
	dir, _, _ := run(t, plan, builtIn)
	if _, err := runner.Read(ctx, plan, builtIn, dir, cgoGuards, nil); err == nil || !strings.Contains(err.Error(), "cgo") {
		t.Errorf("a cgo read of a candidate of the registry: %v", err)
	}

	// So is an external candidate that does not declare cgo.
	plain := external(t, "X/plain", false)
	if _, err := runner.Build(ctx, plan, plain, runner.CandidateDir(root, "plain"), cgoGuards, nil); err == nil || !strings.Contains(err.Error(), "cgo") {
		t.Errorf("a cgo build of an external candidate that does not declare cgo: %v", err)
	}
	plainDir, _, _ := run(t, plan, plain)
	if _, err := runner.Read(ctx, plan, plain, plainDir, cgoGuards, nil); err == nil || !strings.Contains(err.Error(), "cgo") {
		t.Errorf("a cgo read of an external candidate that does not declare cgo: %v", err)
	}

	// One that declares it builds and reads, and the manifest says cgo was on.
	declared := external(t, "X/declared", true)
	declaredDir := runner.CandidateDir(root, declared.Name)
	m, err := runner.Build(ctx, plan, declared, declaredDir, cgoGuards, nil)
	if err != nil {
		t.Fatalf("a cgo build of an external candidate that declares cgo: %v", err)
	}
	if !m.Build.CGO || m.Candidate != "X/declared" {
		t.Errorf("the manifest records %q built with cgo %v", m.Candidate, m.Build.CGO)
	}
	if _, err := runner.Read(ctx, plan, declared, declaredDir, cgoGuards, nil); err != nil {
		t.Errorf("a cgo read of an external candidate that declares cgo: %v", err)
	}

	// Its other guards still hold, in both.
	raceGuards := runner.Guards{Info: cgoGuards.Info}
	raceGuards.Info.Race = true
	if _, err := runner.Build(ctx, plan, declared, runner.CandidateDir(root, "race"), raceGuards, nil); err == nil || !strings.Contains(err.Error(), "race detector") {
		t.Errorf("a race build of an external candidate that declares cgo: %v", err)
	}
	if _, err := runner.Read(ctx, plan, declared, declaredDir, raceGuards, nil); err == nil || !strings.Contains(err.Error(), "race detector") {
		t.Errorf("a race read of an external candidate that declares cgo: %v", err)
	}
}
