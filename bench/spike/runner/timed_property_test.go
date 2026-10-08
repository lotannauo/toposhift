package runner_test

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/bench/spike/runner"
)

var propertyCommits = []time.Duration{1000, 1100, 1200, 1300, 2000, 2600, 5000, 10 * time.Millisecond}

// drawWorld draws a world: one or two windows, the two members of the reference set and
// sometimes the joiner and another policy, one to five repetitions of each on each
// architecture, on one of two CPU models of the architecture, and a counters run of each
// candidate or none.
func drawWorld(t *rapid.T) world {
	windows := rapid.SampledFrom([][]int{{7}, {2, 7}}).Draw(t, "windows")
	cands := []string{cOff, cPolicy}
	if rapid.Bool().Draw(t, "joiner") {
		cands = append(cands, cJoiner)
	}
	if rapid.Bool().Draw(t, "other") {
		cands = append(cands, cOther)
	}
	models := map[string][]string{"aarch64": {armModel, "Test ARM B"}, "x86_64": {x86ModelA, x86ModelB}}
	var w world
	for _, window := range windows {
		for _, cand := range cands {
			if rapid.Bool().Draw(t, fmt.Sprintf("counters %d %s", window, cand)) {
				w.Counters = append(w.Counters, worldCounters{Window: window, Candidate: cand})
			}
			for _, arch := range []string{"aarch64", "x86_64"} {
				reps := rapid.IntRange(1, 5).Draw(t, fmt.Sprintf("reps %d %s %s", window, cand, arch))
				for rep := 1; rep <= reps; rep++ {
					key := fmt.Sprintf("%d %s %s %d", window, cand, arch, rep)
					w.Builds = append(w.Builds, fixtureBuild{
						Window: window, Candidate: cand, Arch: arch, Rep: rep,
						CPU:            rapid.SampledFrom(models[arch]).Draw(t, "cpu "+key),
						Commit:         rapid.SampledFrom(propertyCommits).Draw(t, "commit "+key),
						After:          rapid.SampledFrom(propertyCommits).Draw(t, "after "+key),
						Retain:         time.Duration(rapid.IntRange(1, 90).Draw(t, "retain "+key)) * time.Second,
						BytesPerRecord: int64(rapid.IntRange(10, 100).Draw(t, "bytes "+key)),
						CheckpointPct:  int64(rapid.IntRange(0, 30).Draw(t, "ckpt "+key)),
						Excess:         time.Duration(rapid.IntRange(0, 30).Draw(t, "excess "+key)) * time.Second,
					})
				}
			}
		}
	}
	return w
}

// stopper is what the checks of a property need of a test: both a rapid test and a plain
// one have it. The same checks run on drawn inputs and on the named inputs below, so
// that the claim that they meet every kind of scope does not depend on what is drawn.
type stopper interface {
	Fatalf(format string, args ...any)
}

func checkOrderIndependence(t stopper, in, shuffled runner.TimedInputs) {
	a, b := runner.JudgeTiming(in, verified()), runner.JudgeTiming(shuffled, verified())
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("the reports differ:\n%s\nand\n%s", timingText(a), timingText(b))
	}
	if timingText(a) != timingText(b) {
		t.Fatalf("the texts differ")
	}
}

func TestTimingIsIndependentOfTheOrderOfItsInputs(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		in := drawWorld(t).memory()
		checkOrderIndependence(t, in, runner.TimedInputs{
			Builds:   rapid.Permutation(in.Builds).Draw(t, "builds"),
			Plans:    rapid.Permutation(in.Plans).Draw(t, "plans"),
			Counters: rapid.Permutation(in.Counters).Draw(t, "counters"),
		})
	})
}

// buildsOf are the builds of a candidate on an architecture the verdict was made over.
func buildsOf(rep runner.TimingReport, v runner.TimingVerdict) []runner.TimingBuild {
	var out []runner.TimingBuild
	for _, b := range rep.Builds {
		if b.Window == v.Window && b.Candidate == v.Candidate && b.Arch == v.Arch && (v.Scope == "" || strings.HasPrefix(v.Scope, "pooled over ") || v.Scope == b.CPUModel) {
			out = append(out, b)
		}
	}
	return out
}

func checkMedians(t stopper, w world) {
	rep := w.judge(verified())
	for _, v := range rep.Verdicts {
		if v.NotComparable || v.Arch == runner.ArchBoth {
			continue
		}
		var values []float64
		switch v.Gate {
		case runner.GateG3Commit:
			for _, b := range buildsOf(rep, v) {
				values = append(values, float64(b.CommitP99))
			}
		case runner.GateG4:
			for _, b := range buildsOf(rep, v) {
				values = append(values, b.BytesPerRecord)
			}
		default:
			continue
		}
		if len(values) != v.Reps {
			t.Fatalf("%+v: %d values for %d repetitions", v, len(values), v.Reps)
		}
		if !slices.Contains(values, v.Value) || v.Value < slices.Min(values) || v.Value > slices.Max(values) {
			t.Fatalf("%d days %s %s %s %s: value %v is not among %v", v.Window, v.Candidate, v.Gate, v.Arch, v.Scope, v.Value, values)
		}
	}
}

func TestAMedianIsOneOfTheValues(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) { checkMedians(t, drawWorld(t)) })
}

func checkConjunction(t stopper, w world) {
	rep := w.judge(verified())
	for _, both := range rep.Verdicts {
		if both.Arch != runner.ArchBoth {
			continue
		}
		want, archs := true, map[string]bool{}
		for _, v := range rep.Verdicts {
			if v.Window == both.Window && v.Candidate == both.Candidate && v.Gate == both.Gate && v.Arch != runner.ArchBoth {
				want, archs[v.Arch] = want && v.Pass, true
			}
		}
		if len(archs) == 2 && both.Pass != want {
			t.Fatalf("%d days %s %s: both passes %v, the architectures' conjunction is %v", both.Window, both.Candidate, both.Gate, both.Pass, want)
		}
		if len(archs) < 2 && (both.Pass || both.Judged) {
			t.Fatalf("%d days %s %s: judged on %d architectures: %+v", both.Window, both.Candidate, both.Gate, len(archs), both)
		}
	}
}

func TestBothIsTheConjunctionOfTheArchitectures(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) { checkConjunction(t, drawWorld(t)) })
}

func checkRepeating(t stopper, w world, cand string) {
	doubled := w
	doubled.Builds = slices.Clone(w.Builds)
	for _, b := range w.Builds {
		if b.Candidate == cand {
			b.Run = "200"
			doubled.Builds = append(doubled.Builds, b)
		}
	}
	before, after := w.judge(verified()), doubled.judge(verified())
	type key struct {
		window            int
		gate, arch, scope string
	}
	index := map[key]float64{}
	for _, v := range before.Verdicts {
		if v.Candidate == cand && v.Gate != runner.GateG0 && !v.NotComparable {
			index[key{v.Window, v.Gate, v.Arch, v.Scope}] = v.Value
		}
	}
	for _, v := range after.Verdicts {
		if v.Candidate != cand || v.Gate == runner.GateG0 || v.NotComparable || v.Arch == runner.ArchBoth {
			continue
		}
		if was, ok := index[key{v.Window, v.Gate, v.Arch, v.Scope}]; ok && was != v.Value {
			t.Fatalf("%d days %s %s %s %s: the value was %v and is %v with every build twice", v.Window, cand, v.Gate, v.Arch, v.Scope, was, v.Value)
		}
	}
}

func TestRepeatingEveryBuildOfACandidateChangesNoMedian(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		checkRepeating(t, drawWorld(t), rapid.SampledFrom([]string{cOff, cPolicy}).Draw(t, "candidate"))
	})
}

// Where G2 is judged against the budget it is the largest value of the repetitions, whatever
// order they are in.
func checkLargestStall(t stopper, w world, builds []runner.TimedBuild) {
	rep := w.judge(verified())
	shuffled := w.memory()
	shuffled.Builds = builds
	other := runner.JudgeTiming(shuffled, verified())
	for _, v := range rep.Verdicts {
		if v.Gate != runner.GateG2 || v.Arch == runner.ArchBoth || v.Scope != "" || v.NotComparable {
			continue
		}
		var stalls []int64
		for _, b := range rep.Builds {
			if b.Window == v.Window && b.Candidate == v.Candidate && b.Arch == v.Arch {
				stalls = append(stalls, b.Stall)
			}
		}
		if v.Value != float64(slices.Max(stalls)) {
			t.Fatalf("%d days %s %s: value %v, the stalls are %v", v.Window, v.Candidate, v.Arch, v.Value, stalls)
		}
		if o, ok := verdictIn(other, v); !ok || o.Value != v.Value {
			t.Fatalf("%d days %s %s: value %v and, with the builds in another order, %+v", v.Window, v.Candidate, v.Arch, v.Value, o)
		}
	}
}

func TestG2AgainstTheBudgetIsTheLargestStall(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		w := drawWorld(t)
		checkLargestStall(t, w, rapid.Permutation(w.memory().Builds).Draw(t, "builds"))
	})
}

func verdictIn(rep runner.TimingReport, like runner.TimingVerdict) (runner.TimingVerdict, bool) {
	for _, v := range rep.Verdicts {
		if v.Window == like.Window && v.Candidate == like.Candidate && v.Gate == like.Gate && v.Arch == like.Arch && v.Scope == like.Scope {
			return v, true
		}
	}
	return runner.TimingVerdict{}, false
}

// Every property holds on one named input of each kind of scope, and each input reaches
// its scope: one model, models pooled, a row for each of several models, hardware that
// cannot be compared, and a fallback of G2. Nothing here is drawn, so what the claim
// covers does not change from one run to the next.
func TestThePropertiesHoldOnEveryKindOfScope(t *testing.T) {
	t.Parallel()

	s := time.Second
	models := func(b *fixtureBuild) { // the policy on the second model of x86_64
		if b.Candidate == cPolicy && b.Arch == "x86_64" {
			b.CPU = x86ModelB
		}
	}
	alternating := func(b *fixtureBuild) {
		if b.Arch == "x86_64" && b.Candidate != cJoiner {
			b.CPU = []string{x86ModelA, x86ModelB}[b.Rep-1]
		}
	}
	slow := func(b *fixtureBuild) { b.Retain = 70 * s }
	g3rows := func(rep runner.TimingReport, cand, arch string) []runner.TimingVerdict {
		return rowsFor(rep, 7, cand, runner.GateG3Commit, arch)
	}
	for _, c := range []struct {
		name  string
		world world
		// reaches says whether the report has a row of the kind.
		reaches func(runner.TimingReport) bool
	}{
		{"one model", grid(referenceAndJoiner, []int{7}, 3, nil), func(rep runner.TimingReport) bool {
			r := g3rows(rep, cPolicy, "x86_64")
			return len(r) == 1 && r[0].Scope == x86ModelA
		}},
		{"pooled", grid(referenceAndJoiner, []int{7}, 3, models), func(rep runner.TimingReport) bool {
			r := g3rows(rep, cPolicy, "x86_64")
			return len(r) == 1 && strings.HasPrefix(r[0].Scope, "pooled over ")
		}},
		{"several models", grid(referenceAndJoiner, []int{7}, 2, alternating), func(rep runner.TimingReport) bool {
			r := g3rows(rep, cPolicy, "x86_64")
			return len(r) == 2 && r[0].Scope == x86ModelA && r[1].Scope == x86ModelB
		}},
		{"not comparable", grid(referenceAndJoiner, []int{7}, 1, models), func(rep runner.TimingReport) bool {
			r := g3rows(rep, cPolicy, "x86_64")
			return len(r) == 1 && r[0].NotComparable
		}},
		{"G2 fallback", grid(referenceAndJoiner, []int{7}, 3, slow), func(rep runner.TimingReport) bool {
			return len(rep.Findings) == 2 && strings.Contains(mustRowQuiet(rep, 7, cPolicy, runner.GateG2, "aarch64").LimitText, "no layout of the reference set fits")
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			if !c.reaches(c.world.judge(verified())) {
				t.Fatalf("the input does not reach its scope:\n%s", timingText(c.world.judge(verified())))
			}
			in := c.world.memory()
			reversed := runner.TimedInputs{Builds: slices.Clone(in.Builds), Plans: slices.Clone(in.Plans), Counters: slices.Clone(in.Counters)}
			slices.Reverse(reversed.Builds)
			slices.Reverse(reversed.Plans)
			slices.Reverse(reversed.Counters)
			checkOrderIndependence(t, in, reversed)
			checkMedians(t, c.world)
			checkConjunction(t, c.world)
			for _, cand := range []string{cOff, cPolicy} {
				checkRepeating(t, c.world, cand)
			}
			checkLargestStall(t, c.world, reversed.Builds)
		})
	}
}

// mustRowQuiet is the one verdict of a row, or the zero verdict.
func mustRowQuiet(rep runner.TimingReport, window int, cand, gate, arch string) runner.TimingVerdict {
	if rows := rowsFor(rep, window, cand, gate, arch); len(rows) == 1 {
		return rows[0]
	}
	return runner.TimingVerdict{}
}
