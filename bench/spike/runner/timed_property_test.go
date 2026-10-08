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

func TestTimingIsIndependentOfTheOrderOfItsInputs(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		in := drawWorld(t).memory()
		shuffled := runner.TimedInputs{
			Builds:   rapid.Permutation(in.Builds).Draw(t, "builds"),
			Plans:    rapid.Permutation(in.Plans).Draw(t, "plans"),
			Counters: rapid.Permutation(in.Counters).Draw(t, "counters"),
		}
		a, b := runner.JudgeTiming(in, verified()), runner.JudgeTiming(shuffled, verified())
		if !reflect.DeepEqual(a, b) {
			t.Fatalf("the reports differ:\n%s\nand\n%s", timingText(a), timingText(b))
		}
		if timingText(a) != timingText(b) {
			t.Fatal("the texts differ")
		}
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

func TestAMedianIsOneOfTheValues(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		rep := drawWorld(t).judge(verified())
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
	})
}

func TestBothIsTheConjunctionOfTheArchitectures(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		rep := drawWorld(t).judge(verified())
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
	})
}

func TestRepeatingEveryBuildOfACandidateChangesNoMedian(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		w := drawWorld(t)
		cand := rapid.SampledFrom([]string{cOff, cPolicy}).Draw(t, "candidate")
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
	})
}

// Where G2 is judged against the budget it is the largest value of the repetitions, whatever
// order they are in.
func TestG2AgainstTheBudgetIsTheLargestStall(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		w := drawWorld(t)
		rep := w.judge(verified())
		shuffled := w.memory()
		shuffled.Builds = rapid.Permutation(shuffled.Builds).Draw(t, "builds")
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

// The generators reach every kind of scope: one model, models pooled, a row for each of
// several models, and hardware that cannot be compared, and a fallback of G2.
func TestThePropertyGeneratorsReachEveryScope(t *testing.T) {
	t.Parallel()

	var model, pooled, several, notComparable, fallback int
	rapid.Check(t, func(t *rapid.T) {
		rep := drawWorld(t).judge(verified())
		perKey := map[string]int{}
		for _, v := range rep.Verdicts {
			if v.Gate != runner.GateG3Commit || v.Arch == runner.ArchBoth {
				continue
			}
			switch {
			case v.NotComparable:
				notComparable++
			case strings.HasPrefix(v.Scope, "pooled over "):
				pooled++
			default:
				model++
			}
			perKey[fmt.Sprintf("%d %s %s", v.Window, v.Candidate, v.Arch)]++
		}
		for _, n := range perKey {
			if n > 1 {
				several++
			}
		}
		if len(rep.Findings) > 0 {
			fallback++
		}
	})
	for name, n := range map[string]int{"one model": model, "pooled": pooled, "several models": several, "not comparable": notComparable, "G2 fallback": fallback} {
		if n == 0 {
			t.Errorf("no generated input had a scope of kind %q", name)
		}
	}
}
