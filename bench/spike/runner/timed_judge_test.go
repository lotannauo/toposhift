package runner_test

import (
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/runner"
)

const (
	cOff    = "L/off"
	cPolicy = "L/k64a2l1ns"
	cOther  = "L/k64a1l1ns"
	cJoiner = "M/crdb1"
)

// grid is a world of the candidates at the windows, on both architectures, with reps
// repetitions of each and a counters run of each; set customizes every build.
func grid(cands []string, windows []int, reps int, set func(*fixtureBuild)) world {
	var w world
	for _, window := range windows {
		for _, cand := range cands {
			w.Counters = append(w.Counters, worldCounters{Window: window, Candidate: cand})
			for _, arch := range []string{"aarch64", "x86_64"} {
				for rep := 1; rep <= reps; rep++ {
					b := fixtureBuild{
						Window: window, Candidate: cand, Arch: arch, Rep: rep,
						Commit: 10 * time.Millisecond, After: 5 * time.Millisecond, Retain: 20 * time.Second, BytesPerRecord: 40,
					}
					if set != nil {
						set(&b)
					}
					w.Builds = append(w.Builds, b)
				}
			}
		}
	}
	return w
}

var referenceAndJoiner = []string{cOff, cPolicy, cJoiner}

func (w world) judge(opts runner.TimingOptions) runner.TimingReport {
	return runner.JudgeTiming(w.memory(), opts)
}

func rowsFor(rep runner.TimingReport, window int, cand, gate, arch string) []runner.TimingVerdict {
	var out []runner.TimingVerdict
	for _, v := range rep.Verdicts {
		if v.Window == window && v.Candidate == cand && v.Gate == gate && v.Arch == arch {
			out = append(out, v)
		}
	}
	return out
}

func mustRow(t *testing.T, rep runner.TimingReport, window int, cand, gate, arch string) runner.TimingVerdict {
	t.Helper()
	rows := rowsFor(rep, window, cand, gate, arch)
	if len(rows) != 1 {
		t.Fatalf("%d verdicts for %d days %s %s %s, want 1\n%s", len(rows), window, cand, gate, arch, timingText(rep))
	}
	return rows[0]
}

func TestTimingRefusesInputsThatCannotBeJudgedTogether(t *testing.T) {
	t.Parallel()

	at := func(window int, cand, arch string, rep int) func(fixtureBuild) bool {
		return func(b fixtureBuild) bool {
			return b.Window == window && b.Candidate == cand && b.Arch == arch && b.Rep == rep
		}
	}
	one := at(7, cOff, "x86_64", 1)
	for _, c := range []struct {
		name   string
		world  func(world) world
		inputs func(*runner.TimedInputs)
		opts   func(*runner.TimingOptions)
		want   string // "" for the positive control
	}{
		{name: "unchanged"},
		{name: "two revisions", world: func(w world) world {
			return w.with(one, func(b *fixtureBuild) { b.Revision = strings.Repeat("b", 40) })
		}, want: "builds of two revisions, aaaaaaaaaaaa and bbbbbbbbbbbb"},
		{name: "another executable on one architecture", world: func(w world) world {
			return w.with(one, func(b *fixtureBuild) { b.Executable = strings.Repeat("9", 64) })
		}, want: "on x86_64 the builds were made by different binaries"},
		{name: "another Go version on one architecture", world: func(w world) world {
			return w.with(one, func(b *fixtureBuild) { b.GoVersion = "go1.27.2" })
		}, want: "on x86_64 the builds were made by different binaries"},
		{name: "a sync mixed", world: func(w world) world {
			return w.with(one, func(b *fixtureBuild) { b.Sync = "true" })
		}, want: `built with and without a sync of every commit ("false" and "true"): their timings are not comparable`},
		{name: "a rest mixed", world: func(w world) world {
			return w.with(one, func(b *fixtureBuild) { b.Rest = "true" })
		}, want: "built with and without a rest after each retention"},
		{name: "the canonical layout mixed", world: func(w world) world {
			return w.with(one, func(b *fixtureBuild) { b.Canonical = "true" })
		}, want: "built with and without the canonical layout"},
		{name: "metric sampling mixed", world: func(w world) world {
			return w.with(one, func(b *fixtureBuild) { b.Metrics = "30s" })
		}, want: "built with different metric sampling"},
		{name: "Go memory limits mixed", world: func(w world) world {
			return w.with(one, func(b *fixtureBuild) { b.MemLimit = "11811160064" })
		}, want: `built with different Go memory limits ("none" and "11811160064"): their timings are not comparable`},
		{name: "two Go memory limits mixed", world: func(w world) world {
			w = w.with(func(fixtureBuild) bool { return true }, func(b *fixtureBuild) { b.MemLimit = "11811160064" })
			return w.with(one, func(b *fixtureBuild) { b.MemLimit = "12884901888" })
		}, want: `built with different Go memory limits ("11811160064" and "12884901888")`},
		{name: "settling mixed", world: func(w world) world {
			return w.with(one, func(b *fixtureBuild) { b.Settle = "false" })
		}, want: `built with and without settling a retention's tombstones ("true" and "false")`},
		{name: "settle deadlines mixed", world: func(w world) world {
			return w.with(one, func(b *fixtureBuild) { b.Deadline = "5m0s" })
		}, want: `settled a retention with different deadlines ("2m0s" and "5m0s")`},
		{name: "batches after a retention mixed", world: func(w world) world {
			return w.with(one, func(b *fixtureBuild) { b.PostBatches = "50" })
		}, want: `recorded different numbers of batches after a retention ("100" and "50")`},
		{name: "a Pebble option different", world: func(w world) world {
			return w.with(one, func(b *fixtureBuild) { b.Options = "[Options]\n  max_open_files=2000\n" })
		}, want: "ran with different Pebble options"},
		{name: "two plans at one window", world: func(w world) world {
			return w.with(one, func(b *fixtureBuild) { b.Stream = "another-stream" })
		}, want: "two plans at 7 days"},
		{name: "one plan at two windows", world: func(w world) world {
			return w.with(at(2, cOff, "x86_64", 1), func(b *fixtureBuild) { b.Stream = "stream-R7" })
		}, want: "was built at two windows, 2 and 7 days"},
		{name: "a plan whose stream is not the manifest's", world: func(w world) world {
			return w.with(func(b fixtureBuild) bool { return b.Window == 7 }, func(b *fixtureBuild) { b.ManifestStream = "not-the-plans" })
		}, want: "does not hold the stream the builds were made of"},
		{name: "two families", world: func(w world) world {
			return w.with(one, func(b *fixtureBuild) { b.Family = "ph5m-rmae" })
		}, want: "builds of 2 families, ph5m-rmae and rmae"},
		{name: "two pins windows", world: func(w world) world {
			return w.with(one, func(b *fixtureBuild) { b.PinsWindow = 3 })
		}, want: "the pins were chosen from different windows (2 and 3 days)"},
		{name: "a plan made under other rules", world: func(w world) world {
			w.PlanRules = map[int]string{7: strings.Repeat("f", 64)}
			return w
		}, want: "the plan of 7 days was made under rules ffffffffffff; this binary judges by rules " + currentRulesShort()},
		{name: "the same job twice", inputs: func(in *runner.TimedInputs) {
			in.Builds = append(in.Builds, in.Builds[0])
		}, want: "the same job twice"},
		{name: "a revision that is not an ancestor", opts: func(o *runner.TimingOptions) {
			o.Check = func(string) runner.RevisionCheck { return runner.RevisionCheck{Ancestor: false} }
		}, want: "revision aaaaaaaaaaaa is not an ancestor of origin/main: a timed run is dispatched from main"},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			w := full(3)
			if c.world != nil {
				w = c.world(w)
			}
			in := w.memory()
			if c.inputs != nil {
				c.inputs(&in)
			}
			opts := verified()
			if c.opts != nil {
				c.opts(&opts)
			}
			rep := runner.JudgeTiming(in, opts)
			if c.want == "" {
				if len(rep.Problems) != 0 || !rep.Judged {
					t.Errorf("the unchanged inputs have problems %q (judged %v)", rep.Problems, rep.Judged)
				}
				return
			}
			if !slices.ContainsFunc(rep.Problems, func(p string) bool { return strings.Contains(p, c.want) }) {
				t.Errorf("problems %q do not contain %q", rep.Problems, c.want)
			}
			if rep.Judged {
				t.Error("inputs that cannot be judged together were judged")
			}
			if !strings.Contains(timingText(rep), "THESE TIMINGS ARE NOT TO BE JUDGED TOGETHER") {
				t.Error("the text does not say the timings are not to be judged together")
			}
			for _, v := range rep.Verdicts {
				if v.Judged || v.Reason != "the timings cannot be judged together" {
					t.Errorf("%d days %s %s %s: judged %v, reason %q", v.Window, v.Candidate, v.Gate, v.Arch, v.Judged, v.Reason)
				}
			}
		})
	}
}

func TestTimingRefusesAJoinSetThatDiffersWithinItself(t *testing.T) {
	t.Parallel()

	w := full(3)
	join := grid([]string{cOff, cJoiner}, []int{7}, 3, func(b *fixtureBuild) { b.Run = "200" })
	join.Builds[0].Revision = strings.Repeat("b", 40)
	in := w.memory()
	in.JoinFrom = join.memory().Builds
	rep := runner.JudgeTiming(in, verified())
	if !slices.ContainsFunc(rep.Problems, func(p string) bool { return strings.HasPrefix(p, "the join set: builds of two revisions") }) {
		t.Errorf("problems %q do not name the join set", rep.Problems)
	}
}

func TestTimingAggregatesByTheRule(t *testing.T) {
	t.Parallel()

	ms := time.Millisecond
	perRep := func(ds ...time.Duration) func(*fixtureBuild) {
		return func(b *fixtureBuild) {
			if b.Candidate == cPolicy {
				b.Commit = ds[b.Rep-1]
			}
		}
	}
	// 1024 ns and 2048 ns are a bucket bound apart exactly twice, and 2304 ns is in the
	// next bucket above twice the best.
	if a, b, c := bound(1024), bound(2048), bound(2304); b != 2*a || c <= 2*a {
		t.Fatalf("bounds %d, %d, %d are not in the ratio the tests need", a, b, c)
	}
	commits := func(off, policy time.Duration) func(*fixtureBuild) {
		return func(b *fixtureBuild) {
			switch b.Candidate {
			case cOff:
				b.Commit = off
			case cPolicy:
				b.Commit = policy
			}
		}
	}
	bytes := func(policy int64) func(*fixtureBuild) {
		return func(b *fixtureBuild) {
			if b.Candidate == cPolicy {
				b.BytesPerRecord = policy
			}
		}
	}
	retains := func(ds ...time.Duration) func(*fixtureBuild) {
		return func(b *fixtureBuild) {
			if b.Candidate == cPolicy {
				b.Retain = ds[b.Rep-1]
			}
		}
	}
	for _, c := range []struct {
		name  string
		reps  int
		set   func(*fixtureBuild)
		gate  string
		value float64
		pass  bool
	}{
		{"the median of 10, 11 and 100 ms is 11", 3, perRep(10*ms, 11*ms, 100*ms), runner.GateG3Commit, float64(bound(11 * ms)), true},
		{"the lower median of 10, 11, 12 and 100 ms is 11", 4, perRep(10*ms, 11*ms, 12*ms, 100*ms), runner.GateG3Commit, float64(bound(11 * ms)), true},
		{"the largest retention of 20, 61 and 30 s is 61", 3, retains(20*time.Second, 61*time.Second, 30*time.Second), runner.GateG2, float64(61 * time.Second), false},
		{"exactly twice the best commit passes", 3, commits(1024, 2048), runner.GateG3Commit, float64(bound(2048)), true},
		{"the next bucket above twice the best does not", 3, commits(1024, 2304), runner.GateG3Commit, float64(bound(2304)), false},
		{"exactly twice the best bytes pass", 3, bytes(80), runner.GateG4, 80, true},
		{"above twice the best bytes do not", 3, bytes(81), runner.GateG4, 81, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			rep := grid(referenceAndJoiner, []int{7}, c.reps, c.set).judge(verified())
			if len(rep.Problems) != 0 {
				t.Fatalf("problems %q", rep.Problems)
			}
			for _, arch := range []string{"aarch64", "x86_64"} {
				v := mustRow(t, rep, 7, cPolicy, c.gate, arch)
				if v.Value != c.value || v.Pass != c.pass || v.Reps != c.reps {
					t.Errorf("%s: value %v pass %v of %d, want %v %v of %d (%s, %s)", arch, v.Value, v.Pass, v.Reps, c.value, c.pass, c.reps, v.ValueText, v.LimitText)
				}
				if !v.Judged {
					t.Errorf("%s: not judged: %s", arch, v.Reason)
				}
			}
			both := mustRow(t, rep, 7, cPolicy, c.gate, runner.ArchBoth)
			if both.Pass != c.pass || !both.Judged {
				t.Errorf("both: pass %v judged %v (%s), want pass %v", both.Pass, both.Judged, both.Reason, c.pass)
			}
		})
	}

	t.Run("a candidate that passes on one architecture and is over on the other is over", func(t *testing.T) {
		t.Parallel()
		rep := grid(referenceAndJoiner, []int{7}, 3, func(b *fixtureBuild) {
			if b.Candidate == cPolicy && b.Arch == "x86_64" {
				b.Commit = 100 * ms
			}
		}).judge(verified())
		if v := mustRow(t, rep, 7, cPolicy, runner.GateG3Commit, "aarch64"); !v.Pass || !v.Judged {
			t.Errorf("aarch64: %+v", v)
		}
		if v := mustRow(t, rep, 7, cPolicy, runner.GateG3Commit, "x86_64"); v.Pass || !v.Judged {
			t.Errorf("x86_64: %+v", v)
		}
		both := mustRow(t, rep, 7, cPolicy, runner.GateG3Commit, runner.ArchBoth)
		if both.Pass || !both.Judged || both.ValueText != "aarch64 ok, x86_64 OVER" {
			t.Errorf("both: %+v", both)
		}
		if !strings.Contains(timingText(rep), "OVER (CI timing)") {
			t.Errorf("the text does not show an OVER timing:\n%s", timingText(rep))
		}
	})

	t.Run("a candidate with no build on an architecture is not judged on both", func(t *testing.T) {
		t.Parallel()
		w := grid(referenceAndJoiner, []int{7}, 3, nil).without(func(b fixtureBuild) bool { return b.Candidate == cPolicy && b.Arch == "x86_64" })
		rep := w.judge(verified())
		for _, gate := range []string{runner.GateG2, runner.GateG3Commit, runner.GateG4} {
			both := mustRow(t, rep, 7, cPolicy, gate, runner.ArchBoth)
			if both.Judged || both.Reason != "no builds of "+cPolicy+" on x86_64 at 7 days" || both.Pass {
				t.Errorf("%s both: %+v", gate, both)
			}
			if len(rowsFor(rep, 7, cPolicy, gate, "x86_64")) != 0 {
				t.Errorf("%s: a row on an architecture with no build", gate)
			}
		}
		if rep.Judged {
			t.Error("a report with a candidate missing on an architecture is judged")
		}
	})
}

func TestTimingComparesWithinOneCPUModel(t *testing.T) {
	t.Parallel()

	policyOnB := func(b *fixtureBuild) {
		if b.Candidate == cPolicy && b.Arch == "x86_64" {
			b.CPU = x86ModelB
		}
	}
	t.Run("one model per architecture", func(t *testing.T) {
		t.Parallel()
		rep := grid(referenceAndJoiner, []int{7}, 3, nil).judge(verified())
		v := mustRow(t, rep, 7, cPolicy, runner.GateG3Commit, "x86_64")
		if v.Scope != x86ModelA || !v.Judged || v.NotComparable {
			t.Errorf("%+v", v)
		}
		if !rep.Judged {
			t.Errorf("not judged: %q", rep.NotJudged)
		}
	})
	t.Run("two models and one repetition each are not comparable", func(t *testing.T) {
		t.Parallel()
		rep := grid(referenceAndJoiner, []int{7}, 1, policyOnB).judge(verified())
		v := mustRow(t, rep, 7, cPolicy, runner.GateG3Commit, "x86_64")
		if !v.NotComparable || v.Judged || v.Scope != "" || v.Pass {
			t.Errorf("%+v", v)
		}
		for _, want := range []string{x86ModelA, x86ModelB, "not comparable", cPolicy + " ran on " + x86ModelB, cOff + " on " + x86ModelA} {
			if !strings.Contains(v.LimitText, want) {
				t.Errorf("the limit %q does not say %q", v.LimitText, want)
			}
		}
		if v.Reason != v.LimitText {
			t.Errorf("reason %q, want the limit %q", v.Reason, v.LimitText)
		}
		// The architecture whose builds share a model is compared.
		if a := mustRow(t, rep, 7, cPolicy, runner.GateG3Commit, "aarch64"); a.NotComparable || a.Scope != armModel {
			t.Errorf("aarch64: %+v", a)
		}
		both := mustRow(t, rep, 7, cPolicy, runner.GateG3Commit, runner.ArchBoth)
		if !both.NotComparable || both.Judged || both.Pass {
			t.Errorf("both: %+v", both)
		}
		if !strings.Contains(timingText(rep), "not judged") {
			t.Error("the text does not say the row is not judged")
		}
	})
	t.Run("two models and three repetitions each are pooled", func(t *testing.T) {
		t.Parallel()
		rep := grid(referenceAndJoiner, []int{7}, 3, policyOnB).judge(verified())
		v := mustRow(t, rep, 7, cPolicy, runner.GateG3Commit, "x86_64")
		if v.Scope != "pooled over "+x86ModelA+", "+x86ModelB || !v.Judged || v.NotComparable || v.Reps != 3 {
			t.Errorf("%+v", v)
		}
	})
	t.Run("a model every candidate ran on is compared and the others are left out", func(t *testing.T) {
		t.Parallel()
		// Two repetitions each: the baseline on model A twice, the policy on A and on B.
		rep := grid(referenceAndJoiner, []int{7}, 2, func(b *fixtureBuild) {
			if b.Candidate == cPolicy && b.Arch == "x86_64" && b.Rep == 2 {
				b.CPU = x86ModelB
			}
		}).judge(verified())
		rows := rowsFor(rep, 7, cPolicy, runner.GateG3Commit, "x86_64")
		if len(rows) != 1 || rows[0].Scope != x86ModelA || rows[0].Reps != 1 {
			t.Fatalf("rows %+v, want one in %s with the one repetition on it", rows, x86ModelA)
		}
		if rows[0].Judged || !strings.Contains(rows[0].Reason, "single repetition") {
			t.Errorf("%+v", rows[0])
		}
	})
	t.Run("every model all candidates ran on is compared", func(t *testing.T) {
		t.Parallel()
		// Two repetitions each, on model A and on model B: too few to pool, and every
		// candidate ran on each model, so there is one row for each.
		rep := grid(referenceAndJoiner, []int{7}, 2, func(b *fixtureBuild) {
			if b.Arch == "x86_64" && b.Candidate != cJoiner {
				b.CPU = []string{x86ModelA, x86ModelB}[b.Rep-1]
			}
		}).judge(verified())
		rows := rowsFor(rep, 7, cPolicy, runner.GateG3Commit, "x86_64")
		if len(rows) != 2 || rows[0].Scope != x86ModelA || rows[1].Scope != x86ModelB {
			t.Fatalf("rows %+v, want one in each model", rows)
		}
		both := mustRow(t, rep, 7, cPolicy, runner.GateG3Commit, runner.ArchBoth)
		if !both.Pass {
			t.Errorf("both: %+v", both)
		}
	})
}

func TestTheReferenceSetIsThePreRegisteredOne(t *testing.T) {
	t.Parallel()

	ms := time.Millisecond
	cands := []string{cOff, cPolicy, cOther, cJoiner}
	t.Run("a candidate outside the set sets no standard", func(t *testing.T) {
		t.Parallel()
		rep := grid(cands, []int{7}, 3, func(b *fixtureBuild) {
			switch b.Candidate {
			case cPolicy:
				b.Commit = 15 * ms
			case cOther:
				b.Commit = ms // far below everyone
			}
		}).judge(verified())
		if !slices.Equal(rep.Reference, []string{cOff, cPolicy}) {
			t.Errorf("reference %v", rep.Reference)
		}
		v := mustRow(t, rep, 7, cPolicy, runner.GateG3Commit, "aarch64")
		if !v.Pass || !v.Judged || v.Best != float64(bound(10*ms)) {
			t.Errorf("a candidate within twice the baseline fails against one outside the set: %+v", v)
		}
		if o := mustRow(t, rep, 7, cOther, runner.GateG3Commit, "aarch64"); !o.Pass || !o.Judged || o.Best != float64(bound(10*ms)) {
			t.Errorf("the candidate outside the set is not judged against the best: %+v", o)
		}
	})
	t.Run("a member that fails G0 sets no standard", func(t *testing.T) {
		t.Parallel()
		w := grid(cands, []int{7}, 3, func(b *fixtureBuild) {
			switch b.Candidate {
			case cOff:
				b.Commit = 5 * ms
			case cPolicy:
				b.Commit = 15 * ms
			}
		})
		for i := range w.Counters {
			if w.Counters[i].Candidate == cOff {
				w.Counters[i].Mismatches = []string{"a query"}
			}
		}
		rep := w.judge(verified())
		if g0 := mustRow(t, rep, 7, cOff, runner.GateG0, ""); g0.Pass || !g0.Judged {
			t.Errorf("G0 of the failing member: %+v", g0)
		}
		v := mustRow(t, rep, 7, cPolicy, runner.GateG3Commit, "aarch64")
		if !v.Pass || !v.Judged || v.Best != float64(bound(15*ms)) {
			t.Errorf("the member that answers wrongly set the best: %+v", v)
		}
		if off := mustRow(t, rep, 7, cOff, runner.GateG3Commit, "aarch64"); off.Judged || !strings.Contains(off.Reason, "G0 of L/off failed") {
			t.Errorf("the member that fails G0 is judged on G3: %+v", off)
		}
	})
	t.Run("a member with G0 not established leaves the best open", func(t *testing.T) {
		t.Parallel()
		w := grid(cands, []int{7}, 3, nil)
		w.Counters = slices.DeleteFunc(w.Counters, func(c worldCounters) bool { return c.Candidate == cOff })
		rep := w.judge(verified())
		for _, gate := range []string{runner.GateG3Commit, runner.GateG4} {
			v := mustRow(t, rep, 7, cPolicy, gate, "aarch64")
			if v.Judged || !strings.Contains(v.Reason, "the best is not established: G0 of L/off is not established") {
				t.Errorf("%s: %+v", gate, v)
			}
		}
		// G2 is absolute and does not wait for the best.
		if v := mustRow(t, rep, 7, cPolicy, runner.GateG2, "aarch64"); !v.Judged {
			t.Errorf("G2: %+v", v)
		}
	})
	t.Run("a member that is not built at a window leaves the best open", func(t *testing.T) {
		t.Parallel()
		w := grid(cands, []int{2, 7}, 3, nil).without(func(b fixtureBuild) bool { return b.Window == 2 && b.Candidate == cPolicy })
		rep := w.judge(verified())
		v := mustRow(t, rep, 2, cOff, runner.GateG3Commit, "aarch64")
		if v.Judged || !strings.Contains(v.Reason, cPolicy+" is in the reference set and was not built at 2 days") {
			t.Errorf("%+v", v)
		}
	})
}

func TestTheJoinRule(t *testing.T) {
	t.Parallel()

	// The bounds of the commit histogram: the joiner at 1100 ns is exactly 90% of the
	// baseline at 1200 ns, and below 90% of it at 1300 ns.
	if lo, mid, hi := bound(1100), bound(1200), bound(1300); 10*lo != 9*mid || 10*lo >= 9*hi || lo >= mid || mid >= hi {
		t.Fatalf("bounds %d, %d, %d are not in the ratios the tests need", lo, mid, hi)
	}
	// At 2600 ns the policy is within twice the baseline's 1300 ns and not within twice the joiner's 1100 ns.
	if p, off, j := bound(2600), bound(1300), bound(1100); float64(p) > 2*float64(off) || float64(p) <= 2*float64(j) {
		t.Fatalf("bounds %d, %d, %d are not in the ratios the tests need", p, off, j)
	}
	// commits sets the commit of each candidate at each window and architecture.
	commits := func(off, policy int, joiner func(window int, arch string) int) func(*fixtureBuild) {
		return func(b *fixtureBuild) {
			switch b.Candidate {
			case cOff:
				b.Commit = time.Duration(off)
			case cPolicy:
				b.Commit = time.Duration(policy)
			case cJoiner:
				b.Commit = time.Duration(joiner(b.Window, b.Arch))
			}
		}
	}
	same := func(int, string) int { return 1300 }
	get := func(rep runner.TimingReport) runner.TimingJoin { return rep.Join }

	t.Run("more than 10% below joins at every window", func(t *testing.T) {
		t.Parallel()
		w := grid(referenceAndJoiner, []int{2, 7}, 3, commits(1300, 2600, func(int, string) int { return 1100 }))
		rep := w.judge(verified())
		j := get(rep)
		if !j.Decided || !j.Joins || len(j.Arches) != 2 || !j.Arches[0].Below || j.Arches[0].JoinerP99 != bound(1100) || j.Arches[0].BaselineP99 != bound(1300) {
			t.Fatalf("join %+v", j)
		}
		if !slices.Equal(rep.Reference, referenceAndJoiner) {
			t.Errorf("reference %v", rep.Reference)
		}
		for _, window := range []int{2, 7} {
			v := mustRow(t, rep, window, cPolicy, runner.GateG3Commit, "aarch64")
			if v.Pass || !v.Judged || v.Best != float64(bound(1100)) {
				t.Errorf("%d days: the policy is within twice the baseline and not twice the joiner: %+v", window, v)
			}
		}
		// Against the baseline alone the policy passes.
		rep = grid(referenceAndJoiner, []int{2, 7}, 3, commits(1300, 2600, same)).judge(verified())
		if v := mustRow(t, rep, 7, cPolicy, runner.GateG3Commit, "aarch64"); !v.Pass || get(rep).Joins {
			t.Errorf("the control: %+v, joins %v", v, get(rep).Joins)
		}
	})
	t.Run("exactly 10% below does not join", func(t *testing.T) {
		t.Parallel()
		rep := grid(referenceAndJoiner, []int{2, 7}, 3, commits(1200, 2400, func(int, string) int { return 1100 })).judge(verified())
		if j := get(rep); !j.Decided || j.Joins || !slices.Equal(rep.Reference, []string{cOff, cPolicy}) {
			t.Errorf("join %+v, reference %v", j, rep.Reference)
		}
	})
	t.Run("joining on one architecture is enough", func(t *testing.T) {
		t.Parallel()
		rep := grid(referenceAndJoiner, []int{2, 7}, 3, commits(1300, 2600, func(_ int, arch string) int {
			if arch == "x86_64" {
				return 1100
			}
			return 1300
		})).judge(verified())
		j := get(rep)
		if !j.Joins || j.Arches[0].Below || !j.Arches[1].Below {
			t.Errorf("join %+v", j)
		}
	})
	t.Run("not comparable on one architecture and not below on the other is not decided", func(t *testing.T) {
		t.Parallel()
		rep := grid(referenceAndJoiner, []int{2, 7}, 1, func(b *fixtureBuild) {
			commits(1300, 2600, same)(b)
			if b.Candidate == cJoiner && b.Arch == "x86_64" {
				b.CPU = x86ModelB
			}
		}).judge(verified())
		j := get(rep)
		if j.Decided || j.Joins || !strings.Contains(j.Reason, "not comparable") {
			t.Fatalf("join %+v", j)
		}
		for _, gate := range []string{runner.GateG3Commit, runner.GateG4} {
			for _, v := range rep.Verdicts {
				if v.Gate == gate && v.Arch != "" && (v.Judged || !strings.Contains(v.Reason, "whether M/crdb1 joins is not decided") && !v.NotComparable) {
					t.Errorf("%d days %s %s %s: %+v", v.Window, v.Candidate, v.Gate, v.Arch, v)
				}
			}
		}
		if len(rep.Reference) != 2 {
			t.Errorf("reference %v", rep.Reference)
		}
	})
	t.Run("a joiner without a build at the join window is not decided", func(t *testing.T) {
		t.Parallel()
		w := grid(referenceAndJoiner, []int{2, 7}, 3, nil).without(func(b fixtureBuild) bool { return b.Candidate == cJoiner && b.Window == 7 })
		rep := w.judge(verified())
		if j := get(rep); j.Decided || !strings.Contains(j.Reason, "no builds of M/crdb1 at 7 days on aarch64") {
			t.Errorf("join %+v", j)
		}
	})
	t.Run("qualifying with G0 not established is not decided", func(t *testing.T) {
		t.Parallel()
		w := grid(referenceAndJoiner, []int{2, 7}, 3, commits(1300, 2600, func(int, string) int { return 1100 }))
		w.Counters = slices.DeleteFunc(w.Counters, func(c worldCounters) bool { return c.Candidate == cJoiner })
		rep := w.judge(verified())
		j := get(rep)
		if j.Decided || j.Joins || !strings.Contains(j.Reason, "M/crdb1 qualifies by its commit but its G0 is not established") {
			t.Errorf("join %+v", j)
		}
	})
	t.Run("qualifying with G0 failing does not join", func(t *testing.T) {
		t.Parallel()
		w := grid(referenceAndJoiner, []int{2, 7}, 3, commits(1300, 2600, func(int, string) int { return 1100 }))
		for i := range w.Counters {
			if w.Counters[i].Candidate == cJoiner {
				w.Counters[i].Mismatches = []string{"a query"}
			}
		}
		rep := w.judge(verified())
		if j := get(rep); !j.Decided || j.Joins || !strings.Contains(j.Reason, "its G0 fails") {
			t.Errorf("join %+v", j)
		}
	})
	t.Run("a joined candidate that is not built at a window leaves it open", func(t *testing.T) {
		t.Parallel()
		w := grid(referenceAndJoiner, []int{2, 7}, 3, commits(1300, 2600, func(int, string) int { return 1100 })).
			without(func(b fixtureBuild) bool { return b.Candidate == cJoiner && b.Window == 2 })
		rep := w.judge(verified())
		if !get(rep).Joins {
			t.Fatalf("join %+v", get(rep))
		}
		for _, gate := range []string{runner.GateG3Commit, runner.GateG4} {
			v := mustRow(t, rep, 2, cOff, gate, "aarch64")
			if v.Judged || !strings.Contains(v.Reason, "M/crdb1 is in the reference set and was not built at 2 days") {
				t.Errorf("%s: %+v", gate, v)
			}
		}
		if v := mustRow(t, rep, 7, cOff, runner.GateG3Commit, "aarch64"); !v.Judged {
			t.Errorf("the window it is built at: %+v", v)
		}
	})
	t.Run("the join comes from the join set when one is given", func(t *testing.T) {
		t.Parallel()
		w := grid(referenceAndJoiner, []int{2, 7}, 3, commits(1300, 2600, same))
		join := grid([]string{cOff, cJoiner}, []int{7}, 3, func(b *fixtureBuild) {
			commits(1300, 0, func(int, string) int { return 1100 })(b)
			b.Run, b.Revision = "200", strings.Repeat("b", 40)
		})
		in := w.memory()
		in.JoinFrom = join.memory().Builds
		rep := runner.JudgeTiming(in, verified())
		j := get(rep)
		if !j.Joins || !slices.Equal(j.Runs, []string{"200"}) || j.Revision != strings.Repeat("b", 40) {
			t.Fatalf("join %+v", j)
		}
		if !slices.Equal(rep.Runs, []string{"100"}) || rep.Revision != fixtureRevision {
			t.Errorf("the builds' runs %v and revision %s", rep.Runs, rep.Revision)
		}
		// The join set decides who is in the reference set; the best is taken over the builds
		// being judged, whatever the join set held.
		if !slices.Equal(rep.Reference, referenceAndJoiner) {
			t.Errorf("reference %v", rep.Reference)
		}
		if v := mustRow(t, rep, 7, cPolicy, runner.GateG3Commit, "aarch64"); v.Best != float64(bound(1300)) || !v.Judged {
			t.Errorf("the best was not taken over the builds: %+v", v)
		}
		// The builds alone would not join.
		in.JoinFrom = nil
		if get(runner.JudgeTiming(in, verified())).Joins {
			t.Error("the builds' own joiner joined")
		}
	})
	t.Run("the join is decided at 7 days only", func(t *testing.T) {
		t.Parallel()
		rep := grid(referenceAndJoiner, []int{2, 7}, 3, commits(1300, 2600, func(window int, _ string) int {
			if window == 2 {
				return 1100
			}
			return 1300
		})).judge(verified())
		if j := get(rep); !j.Decided || j.Joins {
			t.Errorf("join %+v", j)
		}
	})
}

func TestTimingLabels(t *testing.T) {
	t.Parallel()

	rep := full(3).judge(verified())
	if !rep.Judged || len(rep.NotJudged) != 0 || len(rep.Problems) != 0 {
		t.Fatalf("judged %v, not judged %q, problems %q", rep.Judged, rep.NotJudged, rep.Problems)
	}
	text := timingText(rep)
	if !strings.Contains(text, "every precondition holds: these are CI timings") || strings.Contains(text, "local timing") || strings.Contains(text, "NOT JUDGED") {
		t.Errorf("the text of a judged report:\n%s", text)
	}
	for _, line := range strings.Split(text, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 || !strings.HasPrefix(fields[0], "L/") && !strings.HasPrefix(fields[0], "M/") {
			continue
		}
		timing := strings.Contains(line, " G2 ") || strings.Contains(line, " G3 ")
		if got := strings.HasSuffix(line, "(CI timing)"); got != timing {
			t.Errorf("a line with a timing %v ends with the label %v: %s", timing, got, line)
		}
		if strings.Contains(line, "(not judged") {
			t.Errorf("a judged line says it is not: %s", line)
		}
	}

	for _, c := range []struct {
		name  string
		world world
		opts  runner.TimingOptions
		want  string
		text  string
	}{
		{"one repetition", full(1), verified(), "single repetition: indicative only", ""},
		{"two repetitions", full(2), verified(), "2 repetitions: indicative only (at least 3 are needed)", ""},
		{"no revision check", full(3), unverified(), "the revision was not checked against origin/main (give -git with a checkout of the repository)", "WARNING: the revision was not checked against origin/main (give -git): these results are unverified"},
		{"a revision check that failed", full(3), runner.TimingOptions{Rules: runner.DefaultRules(), Ref: "origin/main", Check: func(string) runner.RevisionCheck {
			return runner.RevisionCheck{Err: "the revision is not in the checkout"}
		}}, "the revision could not be checked against origin/main: the revision is not in the checkout", "WARNING: the revision could not be checked against origin/main: the revision is not in the checkout"},
		{"every build synced", full(3).with(every, func(b *fixtureBuild) { b.Sync = "true" }), verified(), "these builds synced every commit: G2, G3 and G4 are judged on builds that do not, and a synced build is a separate measurement", "a sync of every commit"},
		{"every build rested", full(3).with(every, func(b *fixtureBuild) { b.Rest = "true" }), verified(), "these builds rested after each retention", "a rest after each retention"},
		{"no build records whether it rested", full(3).with(every, func(b *fixtureBuild) { b.Rest = "-" }), verified(), "these builds do not record whether they rested after each retention", ""},
		{"every build canonical", full(3).with(every, func(b *fixtureBuild) { b.Canonical = "true" }), verified(), "these builds were rewritten into the canonical layout", "rewritten into the canonical layout"},
		{"every build sampled its metrics", full(3).with(every, func(b *fixtureBuild) { b.Metrics = "30s" }), verified(), "these builds sampled their metrics every 30s, which slows a build", "metrics sampled every 30s"},
		{"an untimed manifest", full(3).with(func(b fixtureBuild) bool {
			return b.Window == 7 && b.Candidate == cOff && b.Rep == 1 && b.Arch == "aarch64"
		}, func(b *fixtureBuild) { b.Untimed = true }), verified(), "L/off at 7 days on aarch64 was built by a binary a result may not come from", ""},
		{"a build from a modified tree", full(3).with(every, func(b *fixtureBuild) { b.Modified = true }), verified(), "built from a tree with uncommitted changes", ""},
		{"no build settled a retention", full(3).with(every, func(b *fixtureBuild) { b.Settle = "false" }), verified(), "these builds did not settle a retention's tombstones: rules 5 judge only builds that do", "did not settle"},
		{"no build says it settled", full(3).with(every, func(b *fixtureBuild) { b.Settle = "-" }), verified(), "these builds did not settle a retention's tombstones", ""},
		{"batches after a retention counted differently", full(3).with(every, func(b *fixtureBuild) { b.PostBatches = "50" }), verified(), "did not record the batches after a retention as rules 5 count them (50)", ""},
		{"batches after a retention not recorded in the description", full(3).with(every, func(b *fixtureBuild) { b.PostBatches = "-" }), verified(), "as rules 5 count them (not recorded)", ""},
		{"no batches after a retention recorded", full(3).with(every, func(b *fixtureBuild) { b.NoPost = true }), verified(), "as rules 5 count them (100)", ""},
		{"no plan of the window", func() world { w := full(3); w.NoPlans = map[int]bool{2: true, 7: true}; return w }(), verified(), "no plan of 7 days (", "not among the inputs"},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			rep := c.world.judge(c.opts)
			if rep.Judged {
				t.Fatal("judged")
			}
			if !slices.ContainsFunc(rep.NotJudged, func(r string) bool { return strings.Contains(r, c.want) }) {
				t.Errorf("not judged %q does not hold %q", rep.NotJudged, c.want)
			}
			if !slices.ContainsFunc(rep.Verdicts, func(v runner.TimingVerdict) bool { return !v.Judged && strings.Contains(v.Reason, c.want) }) {
				t.Errorf("no verdict has the reason %q", c.want)
			}
			text := timingText(rep)
			if !strings.Contains(text, "not judged: ") || strings.Contains(text, "every precondition holds") || strings.Contains(text, "local timing") {
				t.Errorf("the text of a report that is not judged:\n%s", text)
			}
			if c.text != "" && !strings.Contains(text, c.text) {
				t.Errorf("the text does not say %q", c.text)
			}
			for _, line := range strings.Split(text, "\n") {
				if strings.HasSuffix(line, "(CI timing)") {
					t.Errorf("a label of CI timing on a report with a precondition missing: %s", line)
				}
			}
		})
	}
}

func TestTimingWithNoBuildsIsNotJudged(t *testing.T) {
	t.Parallel()

	rep := runner.JudgeTiming(runner.TimedInputs{}, verified())
	if rep.Judged || !slices.Equal(rep.NotJudged, []string{"no builds"}) {
		t.Errorf("judged %v, not judged %q", rep.Judged, rep.NotJudged)
	}
	b, err := json.Marshal(rep)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"Problems", "Runs", "Builds", "Verdicts"} {
		if !strings.Contains(string(b), fmt.Sprintf("%q:[]", field)) {
			t.Errorf("%s is not an empty list in %s", field, b)
		}
	}
}

func TestTimingOnAPilotReplica(t *testing.T) {
	t.Parallel()

	// The pilot's problem preconditions are met but for the repetitions: the revision is
	// taken to be checked, so that the reasons below the revision's show.
	rep := replica().judge(verified())
	text := timingText(rep)
	checkGolden(t, timingReplicaGolden, []byte(text))

	if rep.Judged || len(rep.Problems) != 0 || len(rep.Findings) != 0 {
		t.Errorf("judged %v with problems %q and findings %q", rep.Judged, rep.Problems, rep.Findings)
	}
	for _, v := range rep.Verdicts {
		if strings.Contains(v.Gate, "after") || strings.Contains(v.Gate, "first") {
			t.Errorf("a verdict on the first batch after a retention: %+v", v)
		}
	}
	if strings.Contains(text, "G3 first batch") {
		t.Error("the text has a G3 row on the first batch after a retention")
	}
	if v := mustRow(t, rep, 7, cPolicy, runner.GateG3Commit, "aarch64"); v.Pass || v.Judged {
		t.Errorf("L/k64a2l1ns G3 on aarch64: %+v", v)
	}
	if v := mustRow(t, rep, 7, cPolicy, runner.GateG3Commit, "x86_64"); !v.NotComparable || v.Judged {
		t.Errorf("L/k64a2l1ns G3 on x86_64: %+v", v)
	}
	if !rep.Join.Decided || rep.Join.Joins || !slices.Equal(rep.Reference, []string{cOff, cPolicy}) {
		t.Errorf("join %+v, reference %v", rep.Join, rep.Reference)
	}
	if v := mustRow(t, rep, 7, cJoiner, runner.GateG0, ""); v.Judged || v.ValueText != "not established" {
		t.Errorf("G0 of M/crdb1: %+v", v)
	}
	for _, cand := range []string{cOff, cPolicy, cJoiner} {
		for _, arch := range []string{"aarch64", "x86_64", runner.ArchBoth} {
			if v := mustRow(t, rep, 7, cand, runner.GateG4, arch); !v.Pass {
				t.Errorf("G4 of %s on %s: %+v", cand, arch, v)
			}
		}
	}
	for _, want := range []string{
		"single repetition: indicative only", "not comparable", "OVER (not judged: single repetition: indicative only)",
		"M/crdb1 does not join", "G0 of M/crdb1 is not established", "NOT JUDGED:",
		"(version 5, the rules this binary carries)", "settled each retention (deadline 2m0s)",
		"diagnostics at 7 days (they do not decide;", "first batch after a retention, longest (median of 1)", "former rule x2.0 the best 5ms: OVER",
		"retention alone, longest", "settling of a retention, longest", "G2 retention and the slowness after it, aarch64 (largest of 1)",
		"the first 100 batches after it, within 1m0s",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("the text does not say %q", want)
		}
	}
	if strings.Contains(text, "CI timing") && !strings.Contains(text, "timed builds of the bench workflow") {
		t.Error("a label of CI timing in a report that is not judged")
	}
	for _, line := range strings.Split(text, "\n") {
		if strings.HasSuffix(line, "(CI timing)") {
			t.Errorf("a CI timing in the replica: %s", line)
		}
	}
}

// The artifacts read back from files are the ones that were written: the report of a
// replica loaded from disk is the report of the same builds held in memory, wherever
// they were found.
func TestTimingOfArtifactsOnDiskIsTheTimingOfTheBuilds(t *testing.T) {
	t.Parallel()

	w := replica()
	disk := runner.JudgeTiming(w.load(t), verified())
	again := runner.JudgeTiming(w.load(t), verified())
	memory := w.judge(verified())
	a, err := json.Marshal(disk)
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(memory)
	if err != nil {
		t.Fatal(err)
	}
	c, err := json.Marshal(again)
	if err != nil {
		t.Fatal(err)
	}
	if string(a) != string(b) || string(a) != string(c) {
		t.Errorf("the report of the files differs from the report of the builds in memory:\n%s\n%s", a, b)
	}
}

func TestTimingIsDeterministic(t *testing.T) {
	t.Parallel()

	in := replica().memory()
	reverse := in
	reverse.Builds = slices.Clone(in.Builds)
	slices.Reverse(reverse.Builds)
	reverse.Plans = slices.Clone(in.Plans)
	slices.Reverse(reverse.Plans)
	reverse.Counters = slices.Clone(in.Counters)
	slices.Reverse(reverse.Counters)

	var texts, jsons []string
	for _, in := range []runner.TimedInputs{in, in, reverse} {
		rep := runner.JudgeTiming(in, verified())
		texts = append(texts, timingText(rep))
		b, err := json.MarshalIndent(rep, "", " ")
		if err != nil {
			t.Fatal(err)
		}
		jsons = append(jsons, string(b))
	}
	for i := 1; i < 3; i++ {
		if texts[i] != texts[0] {
			t.Errorf("the text of run %d differs", i)
		}
		if jsons[i] != jsons[0] {
			t.Errorf("the JSON of run %d differs", i)
		}
	}
}

func TestTimingNeverChangesTheRules(t *testing.T) {
	t.Parallel()

	encode := func(r runner.Rules) string {
		b, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	digest := func() string {
		d, err := runner.DefaultRules().Digest()
		if err != nil {
			t.Fatal(err)
		}
		return d
	}
	before := digest()
	if !strings.HasPrefix(before, "ea47515f") {
		t.Fatalf("the rules digest is %s", before)
	}
	rules := runner.DefaultRules()
	given := encode(rules)
	opts := verified()
	opts.Rules = rules
	rep := runner.JudgeTiming(full(3).memory(), opts)
	if rep.RulesDigest != before {
		t.Errorf("the report is of rules %s, not %s", rep.RulesDigest, before)
	}
	if after := digest(); after != before {
		t.Errorf("the rules digest changed from %s to %s", before, after)
	}
	if encode(rules) != given {
		t.Error("the rules given were changed")
	}
}

// currentRulesShort is the first twelve characters of the digest of the rules this build
// carries.
func currentRulesShort() string {
	d, err := runner.DefaultRules().Digest()
	if err != nil {
		panic(err)
	}
	return d[:12]
}

// G2 counts the slowness a retention leaves behind: the retention's time plus the excess
// over the median commit of the batches after it. These tests build retentions that are
// short and batches after them that are slow, the case the former rule passed.
func TestTimingG2CountsTheSlownessAfterARetention(t *testing.T) {
	t.Parallel()

	s := time.Second
	perRep := func(excess ...time.Duration) func(*fixtureBuild) {
		return func(b *fixtureBuild) {
			if b.Candidate == cPolicy {
				b.Retain, b.Excess = 20*s, excess[(b.Rep-1)%len(excess)]
			}
		}
	}
	for _, c := range []struct {
		name  string
		set   func(*fixtureBuild)
		value time.Duration
		pass  bool
	}{
		{"a short retention with slow batches after it fails", perRep(50 * s), 70 * s, false},
		{"the largest over the repetitions, not the median", perRep(10*s, 30*s, 20*s), 50 * s, true},
		{"the largest over the repetitions fails when one repetition does", perRep(10*s, 50*s, 20*s), 70 * s, false},
		{"exactly the budget passes", perRep(40 * s), 60 * s, true},
		{"a nanosecond over the budget fails", perRep(40*s + 1), 60*s + 1, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			rep := grid(referenceAndJoiner, []int{7}, 3, c.set).judge(verified())
			for _, arch := range []string{"aarch64", "x86_64"} {
				v := mustRow(t, rep, 7, cPolicy, runner.GateG2, arch)
				if v.Value != float64(c.value) || v.Pass != c.pass || !v.Judged || v.Reps != 3 {
					t.Errorf("%s: value %v pass %v judged %v of %d (%s), want %v %v", arch, v.Value, v.Pass, v.Judged, v.Reps, v.Reason, float64(c.value), c.pass)
				}
			}
			if both := mustRow(t, rep, 7, cPolicy, runner.GateG2, runner.ArchBoth); both.Pass != c.pass {
				t.Errorf("both: %+v", both)
			}
			// The retention alone is within the budget in every case: the former rule passed.
			for _, b := range rep.Builds {
				if b.Candidate == cPolicy && (b.LongestRetention > int64(60*s) || b.Stall < b.LongestRetention) {
					t.Errorf("build %+v: the longest retention is the diagnostic and the stall is at least it", b)
				}
			}
			if len(rep.Findings) != 0 {
				t.Errorf("findings %q where a layout of the reference set fits", rep.Findings)
			}
		})
	}
}

func TestTimingG2AgainstTheBestWhereNoLayoutFits(t *testing.T) {
	t.Parallel()

	s := time.Second
	retains := func(off, policy, other time.Duration, only string) func(*fixtureBuild) {
		return func(b *fixtureBuild) {
			if only != "" && b.Arch != only {
				return
			}
			switch b.Candidate {
			case cOff:
				b.Retain = off
			case cPolicy:
				b.Retain = policy
			case cJoiner:
				b.Retain = other
			}
		}
	}
	t.Run("no member fits: a candidate within twice the best passes and one above fails", func(t *testing.T) {
		t.Parallel()
		for _, c := range []struct {
			policy time.Duration
			pass   bool
		}{{140 * s, true}, {140*s + 1, false}, {70 * s, true}} {
			rep := grid(referenceAndJoiner, []int{7}, 3, retains(70*s, c.policy, 30*s, "")).judge(verified())
			for _, arch := range []string{"aarch64", "x86_64"} {
				v := mustRow(t, rep, 7, cPolicy, runner.GateG2, arch)
				if v.Pass != c.pass || !v.Judged || v.Best != float64(70*s) || v.Limit != float64(140*s) || !strings.Contains(v.LimitText, "(x2.0 the best, 1m10s; no layout of the reference set fits 1m0s)") {
					t.Errorf("%v on %s: %+v", c.policy, arch, v)
				}
			}
			// A candidate within the budget passes whatever the best is.
			if v := mustRow(t, rep, 7, cJoiner, runner.GateG2, "aarch64"); !v.Pass || !v.Judged {
				t.Errorf("a candidate within the budget: %+v", v)
			}
			if len(rep.Findings) != 2 || !strings.HasPrefix(rep.Findings[0], "at 7 days on aarch64 synchronous retention fits 1m0s for no layout of the reference set (best 1m10s): the store must retain asynchronously before it ingests real data; G2 there is judged against the best") {
				t.Errorf("findings %q", rep.Findings)
			}
			if text := timingText(rep); !strings.Contains(text, "FINDING: at 7 days on x86_64 synchronous retention fits") {
				t.Errorf("the text does not give the finding:\n%s", text)
			}
		}
	})
	t.Run("one member within the budget leaves the budget absolute", func(t *testing.T) {
		t.Parallel()
		rep := grid(referenceAndJoiner, []int{7}, 3, retains(70*s, 50*s, 70*s, "")).judge(verified())
		if len(rep.Findings) != 0 {
			t.Errorf("findings %q", rep.Findings)
		}
		for _, cand := range []string{cOff, cJoiner} {
			v := mustRow(t, rep, 7, cand, runner.GateG2, "aarch64")
			if v.Pass || !v.Judged || v.LimitText != "1m0s" {
				t.Errorf("%s over the budget with a layout that fits: %+v", cand, v)
			}
		}
		if v := mustRow(t, rep, 7, cPolicy, runner.GateG2, "aarch64"); !v.Pass {
			t.Errorf("the member that fits: %+v", v)
		}
	})
	t.Run("the fallback is per architecture", func(t *testing.T) {
		t.Parallel()
		w := grid(referenceAndJoiner, []int{7}, 3, func(b *fixtureBuild) {
			if b.Arch == "x86_64" {
				retains(70*s, 130*s, 30*s, "")(b)
			} else {
				retains(20*s, 70*s, 30*s, "")(b)
			}
		})
		rep := w.judge(verified())
		if len(rep.Findings) != 1 || !strings.Contains(rep.Findings[0], "on x86_64") {
			t.Fatalf("findings %q", rep.Findings)
		}
		if v := mustRow(t, rep, 7, cPolicy, runner.GateG2, "aarch64"); v.Pass || v.LimitText != "1m0s" {
			t.Errorf("aarch64 is judged against the budget: %+v", v)
		}
		if v := mustRow(t, rep, 7, cPolicy, runner.GateG2, "x86_64"); !v.Pass || v.Best != float64(70*s) {
			t.Errorf("x86_64 is judged against the best: %+v", v)
		}
	})
	t.Run("members on two CPU models and one repetition are not comparable", func(t *testing.T) {
		t.Parallel()
		w := grid(referenceAndJoiner, []int{7}, 1, func(b *fixtureBuild) {
			retains(70*s, 130*s, 30*s, "")(b)
			if b.Candidate == cPolicy && b.Arch == "x86_64" {
				b.CPU = x86ModelB
			}
		})
		rep := w.judge(verified())
		v := mustRow(t, rep, 7, cPolicy, runner.GateG2, "x86_64")
		if !v.NotComparable || v.Judged || v.Pass || !strings.Contains(v.LimitText, "not comparable") {
			t.Errorf("%+v", v)
		}
		if len(rep.Findings) != 0 {
			t.Errorf("findings %q from members with one repetition", rep.Findings)
		}
	})
	t.Run("a member with G0 not established leaves the fallback open", func(t *testing.T) {
		t.Parallel()
		w := grid(referenceAndJoiner, []int{7}, 3, retains(70*s, 130*s, 30*s, ""))
		w.Counters = slices.DeleteFunc(w.Counters, func(c worldCounters) bool { return c.Candidate == cOff })
		rep := w.judge(verified())
		v := mustRow(t, rep, 7, cPolicy, runner.GateG2, "aarch64")
		if v.Judged || !strings.Contains(v.Reason, "the best is not established: G0 of L/off is not established") {
			t.Errorf("%+v", v)
		}
		// Within the budget it passes either way.
		if v := mustRow(t, rep, 7, cJoiner, runner.GateG2, "aarch64"); !v.Judged || !v.Pass {
			t.Errorf("%+v", v)
		}
	})
}

func TestTimingG3IsTheCommitAlone(t *testing.T) {
	t.Parallel()

	rep := grid(referenceAndJoiner, []int{7}, 3, func(b *fixtureBuild) {
		if b.Candidate == cPolicy {
			b.After = 6 * time.Second // far above L/off's: the former rule failed it
		}
	}).judge(verified())
	for _, v := range rep.Verdicts {
		if strings.Contains(v.Gate, "after") || strings.Contains(v.Gate, "first") {
			t.Errorf("a verdict on the first batch after a retention: %+v", v)
		}
	}
	if text := timingText(rep); strings.Contains(text, "G3 first batch") {
		t.Errorf("the text has a G3 row on the first batch:\n%s", text)
	}
	if v := mustRow(t, rep, 7, cPolicy, runner.GateG3Commit, "aarch64"); !v.Pass || !v.Judged {
		t.Errorf("G3 commit: %+v", v)
	}
	// The slow first batch is G2's first term, and the diagnostic shows the verdict the former rule gave.
	var seen bool
	for _, d := range rep.Diagnostics {
		if d.Candidate == cPolicy && d.Arch == "aarch64" && d.What == "first batch" {
			seen = true
			if d.FormerPass || !strings.Contains(d.Text, "OVER") || d.Value != int64(6*time.Second) || d.Reps != 3 {
				t.Errorf("diagnostic %+v", d)
			}
		}
		if d.Candidate == cOff && d.What == "first batch" && !d.FormerPass {
			t.Errorf("the baseline's own diagnostic is over: %+v", d)
		}
	}
	if !seen {
		t.Error("no diagnostic of the first batch")
	}
	if text := timingText(rep); !strings.Contains(text, "diagnostics at 7 days (they do not decide;") || !strings.Contains(text, "former rule x2.0 the best") {
		t.Errorf("the text has no diagnostics:\n%s", text)
	}
	if v := mustRow(t, rep, 7, cPolicy, runner.GateG2, "aarch64"); v.Value < float64(6*time.Second) || !v.Pass {
		t.Errorf("G2 counts the slow first batch: %+v", v)
	}
}

func TestTimingDiagnosticsShowTheRetentionAndTheSettling(t *testing.T) {
	t.Parallel()

	rep := grid(referenceAndJoiner, []int{7}, 3, func(b *fixtureBuild) {
		if b.Candidate == cPolicy {
			b.SettleTime, b.DeadlineHits = 90*time.Second, 1
		}
	}).judge(verified())
	var settling, alone int
	for _, d := range rep.Diagnostics {
		switch {
		case d.What == "settling" && d.Candidate == cPolicy && d.Arch == "aarch64":
			settling++
			if d.Value != int64(90*time.Second) || d.Reps != 3 || d.Text != "deadline reached 3 times" {
				t.Errorf("settling %+v", d)
			}
		case d.What == "retention alone" && d.Candidate == cPolicy && d.Arch == "aarch64":
			alone++
			if d.Value != int64(20*time.Second) || d.Reps != 3 {
				t.Errorf("retention alone %+v", d)
			}
		}
	}
	if settling != 1 || alone != 1 {
		t.Errorf("%d settling and %d retention-alone diagnostics, want one each", settling, alone)
	}
	for _, b := range rep.Builds {
		if b.Candidate == cPolicy && (b.SettleLongest != int64(90*time.Second) || b.SettleDeadlineHits != 1) {
			t.Errorf("build %+v", b)
		}
	}
}

// L/off's own commit varies over its repetitions, and a comparison with it cannot be
// resolved where it varies by more than a factor of 1.5.
func TestTimingDoesNotDecideWhereTheBaselineIsNoisy(t *testing.T) {
	t.Parallel()

	// Bucket bounds exactly 2:3 apart, and the next bound above the larger one.
	lo, hi, over := bound(1200), bound(1800), bound(1950)
	if 3*lo != 2*hi || float64(over) <= 1.5*float64(lo) {
		t.Fatalf("bounds %d, %d and %d are not in the ratios the tests need", lo, hi, over)
	}
	commits := func(third time.Duration) func(*fixtureBuild) {
		return func(b *fixtureBuild) {
			if b.Candidate == cOff {
				b.Commit = []time.Duration{1200, 1250, third}[b.Rep-1]
			}
		}
	}
	for _, c := range []struct {
		name  string
		third time.Duration
		noise bool
	}{{"exactly 1.5 times is decided", 1800, false}, {"more than 1.5 times is not", 1950, true}} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			rep := grid(referenceAndJoiner, []int{7}, 3, commits(c.third)).judge(verified())
			for _, cand := range []string{cOff, cPolicy} {
				for _, arch := range []string{"aarch64", "x86_64"} {
					v := mustRow(t, rep, 7, cand, runner.GateG3Commit, arch)
					model := map[string]string{"aarch64": armModel, "x86_64": x86ModelA}[arch]
					want := "L/off's own 99th-percentile commit varies by more than x1.5 over its repetitions in " + model + ": the noise is larger than the gate can resolve"
					if c.noise && (v.Judged || v.Reason != want) || !c.noise && !v.Judged {
						t.Errorf("%s on %s: judged %v, reason %q", cand, arch, v.Judged, v.Reason)
					}
				}
			}
			// The other gates do not wait for it.
			if v := mustRow(t, rep, 7, cPolicy, runner.GateG4, "aarch64"); !v.Judged {
				t.Errorf("G4: %+v", v)
			}
			if c.noise && !slices.ContainsFunc(rep.NotJudged, func(r string) bool { return strings.Contains(r, "varies by more than x1.5") }) {
				t.Errorf("not judged %q", rep.NotJudged)
			}
		})
	}
	t.Run("a relative G2 row is not decided either", func(t *testing.T) {
		t.Parallel()
		s := time.Second
		rep := grid(referenceAndJoiner, []int{7}, 3, func(b *fixtureBuild) {
			switch b.Candidate {
			case cOff:
				b.Retain = []time.Duration{70 * s, 80 * s, 110 * s}[b.Rep-1]
			case cPolicy:
				b.Retain = 100 * s
			}
		}).judge(verified())
		v := mustRow(t, rep, 7, cPolicy, runner.GateG2, "aarch64")
		if v.Judged || !strings.Contains(v.Reason, "own retention plus the slowness after it varies by more than x1.5") {
			t.Errorf("%+v", v)
		}
		// An absolute row is not affected.
		if v := mustRow(t, rep, 7, cJoiner, runner.GateG2, "aarch64"); !v.Judged || !v.Pass {
			t.Errorf("%+v", v)
		}
	})
}

// A best that no member has a value for is not a best: the row is not judged, whatever
// its pass says.
func TestTimingWithoutABestIsNotJudged(t *testing.T) {
	t.Parallel()

	for name, set := range map[string]func(*fixtureBuild){
		"no batch was timed":  func(b *fixtureBuild) { b.NoBatches = true },
		"no table has a byte": func(b *fixtureBuild) { b.ZeroBytes = true },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			rep := grid(referenceAndJoiner, []int{7}, 3, set).judge(verified())
			gate := runner.GateG3Commit
			if name == "no table has a byte" {
				gate = runner.GateG4
			}
			want := "the best is not established on aarch64: no member of the reference set has a value"
			for _, cand := range referenceAndJoiner {
				v := mustRow(t, rep, 7, cand, gate, "aarch64")
				if v.Judged || v.Reason != want {
					t.Errorf("%s: %+v", cand, v)
				}
			}
			for _, line := range strings.Split(timingText(rep), "\n") {
				if strings.Contains(line, "OVER (CI timing)") || strings.Contains(line, "no best") && strings.HasSuffix(line, "(CI timing)") {
					t.Errorf("a row without a best is labelled: %s", line)
				}
			}
		})
	}
}

// A join set is judged on the same footing as the builds: the same family, the pins of
// the same window, and a plan of the join window that is among the inputs, made under
// these rules and the builds' own.
func TestTimingRefusesAJoinSetThatCannotBeComparedWithTheBuilds(t *testing.T) {
	t.Parallel()

	for _, c := range []struct {
		name string
		join func(*fixtureBuild)
		plan func(*runner.TimedInputs)
		want string
	}{
		{name: "the same family and plan"},
		{name: "another family", join: func(b *fixtureBuild) { b.Family = "ph5m-rmae" }, want: "the join set: its builds are of family ph5m-rmae and the builds judged are of family rmae"},
		{name: "pins from another window", join: func(b *fixtureBuild) { b.PinsWindow = 3 }, want: "the join set: its pins were chosen from the 3-day window and those of the builds judged from the 2-day window"},
		{name: "a plan of another stream", join: func(b *fixtureBuild) { b.Stream = "another-stream" }, want: "the join set: its plan of 7 days ("},
		{name: "no plan among the inputs", join: func(b *fixtureBuild) { b.Stream = "another-stream" }, plan: func(in *runner.TimedInputs) { in.JoinPlans = nil }, want: "the join set: no plan of 7 days ("},
		{name: "a plan made under other rules", plan: func(in *runner.TimedInputs) {
			for _, p := range in.JoinPlans {
				p.RulesDigest = strings.Repeat("f", 64)
			}
			in.Plans = nil
		}, want: "was made under rules ffffffffffff"},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			in := full(3).memory()
			join := grid([]string{cOff, cJoiner}, []int{7}, 3, func(b *fixtureBuild) {
				b.Run = "200"
				if c.join != nil {
					c.join(b)
				}
			}).memory()
			in.JoinFrom, in.JoinPlans = join.Builds, join.Plans
			if c.plan != nil {
				c.plan(&in)
			}
			rep := runner.JudgeTiming(in, verified())
			if c.want == "" {
				if len(rep.Problems) != 0 || !rep.Judged {
					t.Errorf("problems %q, judged %v", rep.Problems, rep.Judged)
				}
				return
			}
			if !slices.ContainsFunc(rep.Problems, func(p string) bool { return strings.Contains(p, c.want) }) {
				t.Errorf("problems %q do not contain %q", rep.Problems, c.want)
			}
			if rep.Judged {
				t.Error("judged")
			}
		})
	}
}

// The order the builds are given in changes nothing, even where two share a key and
// differ in what they measured.
func TestTimingOrdersBuildsThatShareAJob(t *testing.T) {
	t.Parallel()

	in := full(3).memory()
	clone := in.Builds[0]
	m := *clone.Manifest
	m.Timing.Retains = []int64{1, 2}
	clone.Manifest = &m
	in.Builds = append(in.Builds, clone)
	reversed := in
	reversed.Builds = slices.Clone(in.Builds)
	slices.Reverse(reversed.Builds)
	a, b := runner.JudgeTiming(in, verified()), runner.JudgeTiming(reversed, verified())
	if !reflect.DeepEqual(a.Builds, b.Builds) {
		t.Error("the builds are listed in the order they were given in")
	}
}

// The text states the rules the judgement used, and claims the rules this binary carries
// only when they are.
func TestTimingStatesTheRulesItUsed(t *testing.T) {
	t.Parallel()

	rules := runner.DefaultRules()
	rules.CommitFactor, rules.BytesPerRecordFactor, rules.StallBudgetSeconds = 3, 4, 30
	opts := verified()
	opts.Rules = rules
	text := timingText(full(3).judge(opts))
	for _, want := range []string{"within 30s (or, where no layout of the reference set fits, within x3.0 the best)", "within x3.0 the best median of the reference set", "G4 the median, within x4.0 the best median"} {
		if !strings.Contains(text, want) {
			t.Errorf("the rule line does not say %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, "the rules this binary carries") {
		t.Errorf("rules that are not this binary's are called its own:\n%s", text)
	}
	if !strings.Contains(timingText(full(3).judge(verified())), "the rules this binary carries") {
		t.Error("the rules this binary carries are not called so")
	}
}

// A verdict that could not be made at all says why once, in the bracket after the word.
func TestTimingSaysWhyAVerdictWasNotMadeOnce(t *testing.T) {
	t.Parallel()

	text := timingText(replica().judge(verified()))
	var seen bool
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(line, "M/crdb1") && strings.Contains(line, "G0 answers") {
			seen = true
			if strings.Contains(line, "(not judged:") || !strings.Contains(line, "not judged (G0 of M/crdb1 is not established: no counters run of plan") {
				t.Errorf("the G0 line: %s", line)
			}
		}
	}
	if !seen {
		t.Error("no G0 line of M/crdb1")
	}
}

// A finding is a claim about the engine, made only from inputs that are judged.
func TestTimingMakesNoFindingFromInputsItRefuses(t *testing.T) {
	t.Parallel()

	s := time.Second
	slow := func(b *fixtureBuild) { b.Retain = 70 * s }
	for _, c := range []struct {
		name  string
		world world
		opts  runner.TimingOptions
	}{
		{"the control: judged", grid(referenceAndJoiner, []int{7}, 3, slow), verified()},
		{"builds that did not settle", grid(referenceAndJoiner, []int{7}, 3, func(b *fixtureBuild) { slow(b); b.Settle = "false" }), verified()},
		{"builds that did not record the batches after a retention", grid(referenceAndJoiner, []int{7}, 3, func(b *fixtureBuild) { slow(b); b.NoPost = true }), verified()},
		{"a sync of every commit", grid(referenceAndJoiner, []int{7}, 3, func(b *fixtureBuild) { slow(b); b.Sync = "true" }), verified()},
		{"a single repetition of every member", grid(referenceAndJoiner, []int{7}, 1, slow), verified()},
		{"two repetitions of every member", grid(referenceAndJoiner, []int{7}, 2, slow), verified()},
		{"a join that is not decided", grid(referenceAndJoiner, []int{2, 7}, 3, func(b *fixtureBuild) {
			slow(b)
			if b.Candidate == cJoiner && b.Arch == "x86_64" && b.Window == 7 {
				b.Commit = 20 * time.Millisecond
			}
		}).without(func(b fixtureBuild) bool { return b.Candidate == cJoiner && b.Window == 7 && b.Arch == "aarch64" }), verified()},
		{"a revision not checked", grid(referenceAndJoiner, []int{7}, 3, slow), unverified()},
		{"a plan that is not among the inputs", func() world {
			w := grid(referenceAndJoiner, []int{7}, 3, slow)
			w.NoPlans = map[int]bool{7: true}
			return w
		}(), verified()},
		{"settings that differ", grid(referenceAndJoiner, []int{7}, 3, func(b *fixtureBuild) {
			slow(b)
			if b.Candidate == cOff && b.Rep == 1 {
				b.Rest = "true"
			}
		}), verified()},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			rep := c.world.judge(c.opts)
			control := c.name == "the control: judged"
			if control != (len(rep.Findings) == 2) || !control && len(rep.Findings) != 0 {
				t.Errorf("findings %q", rep.Findings)
			}
			if !control && strings.Contains(timingText(rep), "FINDING") {
				t.Error("the text makes a finding")
			}
			if !control {
				for _, v := range rep.Verdicts {
					if v.Gate == runner.GateG2 && v.Judged && strings.Contains(v.LimitText, "no layout") {
						t.Errorf("a row is judged against the best: %+v", v)
					}
				}
			}
		})
	}
}

// A member with builds and no retention has no value: the comparison with the best
// does not leave it out.
func TestTimingG2DoesNotLeaveOutAMemberWithoutAValue(t *testing.T) {
	t.Parallel()

	s := time.Second
	rep := grid(referenceAndJoiner, []int{7}, 3, func(b *fixtureBuild) {
		switch b.Candidate {
		case cOff:
			b.Retain = 0
		case cPolicy:
			b.Retain = 70 * s
		case cJoiner:
			b.Retain = 130 * s
		}
	}).judge(verified())
	v := mustRow(t, rep, 7, cJoiner, runner.GateG2, "aarch64")
	if v.Judged || !v.NotComparable || !strings.Contains(v.LimitText, "L/off has no value to compare") {
		t.Errorf("%+v", v)
	}
}

// The digest of a plan does not cover the rules it was made under: two plans of one
// digest are both checked.
func TestTimingChecksEveryPlanWhateverItsDigest(t *testing.T) {
	t.Parallel()

	in := full(3).memory()
	old := *in.Plans[0]
	old.RulesDigest = strings.Repeat("e", 64)
	for _, order := range []bool{false, true} {
		plans := append([]*runner.Plan{&old}, in.Plans...)
		if order {
			slices.Reverse(plans)
		}
		in2 := in
		in2.Plans = plans
		rep := runner.JudgeTiming(in2, verified())
		if !slices.ContainsFunc(rep.Problems, func(p string) bool { return strings.Contains(p, "was made under rules eeeeeeeeeeee") }) || rep.Judged {
			t.Errorf("problems %q (judged %v)", rep.Problems, rep.Judged)
		}
	}
}

func TestTimingSaysWhenNoSettleDeadlineWasRecorded(t *testing.T) {
	t.Parallel()

	text := timingText(full(3).with(every, func(b *fixtureBuild) { b.Deadline = "-" }).judge(verified()))
	if !strings.Contains(text, "settled each retention (no deadline recorded)") || strings.Contains(text, "(deadline )") {
		t.Errorf("the builds line:\n%s", text)
	}
}

// Builds that all ran under one Go memory limit are judged as builds without one are,
// and the builds line says the limit; a limit makes no precondition fail.
func TestTimingJudgesBuildsThatAllRanUnderOneGoMemoryLimit(t *testing.T) {
	t.Parallel()

	without := full(3).judge(verified())
	with := full(3).with(every, func(b *fixtureBuild) { b.MemLimit = "11811160064" }).judge(verified())
	if !with.Judged || len(with.Problems) != 0 || len(with.NotJudged) != 0 {
		t.Fatalf("judged %v, problems %q, not judged %q", with.Judged, with.Problems, with.NotJudged)
	}
	if !reflect.DeepEqual(with.Verdicts, without.Verdicts) {
		t.Error("the verdicts of builds under a limit are not those of the same builds without one")
	}
	if got := with.Settings.GoMemoryLimit; got != "11811160064" {
		t.Errorf("the settings hold the limit %q", got)
	}
	if text := timingText(with); !strings.Contains(text, "Go memory limit 11 GiB; binaries") || strings.Contains(text, "no Go memory limit") {
		t.Errorf("the builds line of builds under a limit:\n%s", text)
	}
	if text := timingText(without); !strings.Contains(text, "no Go memory limit; binaries") {
		t.Errorf("the builds line of builds without a limit:\n%s", text)
	}
	if got := without.Settings.GoMemoryLimit; got != "none" {
		t.Errorf("builds without the key have the limit %q, want none", got)
	}
}

// A limit recorded as none is the same as a manifest without the key.
func TestTimingTakesNoneAndNoKeyAlike(t *testing.T) {
	t.Parallel()

	w := full(3).with(func(b fixtureBuild) bool { return b.Candidate == cOff }, func(b *fixtureBuild) { b.MemLimit = "none" })
	if rep := w.judge(verified()); !rep.Judged || len(rep.Problems) != 0 {
		t.Errorf("judged %v, problems %q", rep.Judged, rep.Problems)
	}
}
