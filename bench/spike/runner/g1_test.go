package runner_test

import (
	"math"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/internal/identity"
)

// g1Windows builds the four windows of one candidate over the given queries, with
// the work of a query in a window given by f.
func g1Windows(queries []runner.Query, candidate string, f func(q runner.Query, days int) map[string]int64) []runner.RetainedWindow {
	var out []runner.RetainedWindow
	for _, days := range []int{2, 7, 14, 30} {
		plan := &runner.Plan{Queries: queries}
		res := &runner.Results{Candidate: candidate, Describe: map[string]string{"block_bytes": "32768"}}
		for _, q := range queries {
			res.Queries = append(res.Queries, runner.QueryResult{Counters: f(q, days)})
		}
		out = append(out, runner.RetainedWindow{Days: days, Plan: plan, Candidates: []*runner.Candidate{{Manifest: &runner.Manifest{}, Results: res}}})
	}
	return out
}

func oneEntity() []identity.Fingerprint { return make([]identity.Fingerprint, 1) }

func cellOf(t *testing.T, cells []runner.G1Cell, group, age, counter string) runner.G1Cell {
	t.Helper()
	for _, c := range cells {
		if c.Group == group && c.Age == age && c.Counter == counter {
			return c
		}
	}
	t.Fatalf("no cell %s %s %s in %d cells", group, age, counter, len(cells))
	return runner.G1Cell{}
}

// The budget of a read at the target window is what it must fit, and what it would
// cost if the retention were doubled must fit as well: a read that fits only if it
// stops growing fails when it grows.
func TestG1AReadMustFitItsBudgetAndStillFitWhenTheRetentionDoubles(t *testing.T) {
	t.Parallel()

	rules := runner.DefaultRules()
	// seeks: 8 per entity read and 4 per item of the answer: 28 for one entity and
	// five items.
	qs := []runner.Query{{Group: "node<- hot-records neighbors", Rank: 0, Op: runner.OpNeighbors, Age: runner.AgeNow, Fps: oneEntity(), Size: 5}}
	for name, c := range map[string]struct {
		work       func(days int) int64
		ratio      float64
		growth     float64
		pass       bool
		wantClass  string
		wantGrowth func(float64) bool
	}{
		"flat and under":                {func(int) int64 { return 20 }, 20.0 / 28, 0, true, "bounded", nil},
		"flat and over":                 {func(int) int64 { return 40 }, 40.0 / 28, 0, false, "bounded", nil},
		"under but growing linearly":    {func(d int) int64 { return int64(d) * 2 / 3 }, 20.0 / 28, 1, false, "linear", nil}, // 20 at 30 days
		"under and growing slowly":      {func(d int) int64 { return 10 + int64(math.Sqrt(float64(d))) }, 0, 0, true, "bounded", func(g float64) bool { return g > 0 && g < 0.25 }},
		"at the floor only at 30":       {func(d int) int64 { return map[int]int64{2: 1, 7: 1, 14: 1, 30: 8}[d] }, 8.0 / 28, 0, true, "bounded", nil}, // at the floor: no growth counted
		"flat to 14 days, then growing": {func(d int) int64 { return map[int]int64{2: 10, 7: 10, 14: 10, 30: 20}[d] }, 20.0 / 28, 0.9, false, "bounded", func(g float64) bool { return g > 0.8 && g < 1 }},
	} {
		cells, err := runner.G1(g1Windows(qs, "A", func(_ runner.Query, d int) map[string]int64 {
			return map[string]int64{"read.neighbors.seeks": c.work(d)}
		}), rules)
		if err != nil {
			t.Fatal(err)
		}
		got := cellOf(t, cells, qs[0].Group, runner.AgeNow, "seeks")
		if got.Pass != c.pass || got.Class != c.wantClass {
			t.Errorf("%s: pass %v, class %s; want %v, %s: %+v", name, got.Pass, got.Class, c.pass, c.wantClass, got)
		}
		if c.ratio > 0 && math.Abs(got.Ratio-c.ratio) > 1e-9 {
			t.Errorf("%s: ratio %v, want %v", name, got.Ratio, c.ratio)
		}
		if c.wantGrowth == nil && math.Abs(got.Growth-c.growth) > 0.1 {
			t.Errorf("%s: growth %v, want %v", name, got.Growth, c.growth)
		}
		if c.wantGrowth != nil && !c.wantGrowth(got.Growth) {
			t.Errorf("%s: growth %v", name, got.Growth)
		}
		if want := got.Ratio * math.Pow(rules.G1Headroom, got.Growth); math.Abs(got.Projected-want) > 1e-9 {
			t.Errorf("%s: projected %v, want ratio x headroom^growth = %v", name, got.Projected, want)
		}
		if runner.G1Passes(cells, "A") != c.pass {
			t.Errorf("%s: the candidate's verdict is not its cell's", name)
		}
	}
}

// The population is judged by its worst query, or for the middle one by its median;
// and the budget of a read is per entity and per item of its answer, in blocks of the
// run's block size for the cold bytes.
func TestG1APopulationIsJudgedByItsLargestOrItsMedian(t *testing.T) {
	t.Parallel()

	rules := runner.DefaultRules()
	work := []int64{14, 33, 17} // against a budget of 28: ratios .5, 1.18, .61
	mk := func(group string) []runner.Query {
		var qs []runner.Query
		for i := range work {
			qs = append(qs, runner.Query{Group: group, Rank: i, Op: runner.OpNeighbors, Age: runner.AgeNow, Fps: oneEntity(), Size: 5})
		}
		return qs
	}
	for _, c := range []struct {
		group string
		pass  bool
		stat  string
		worst int
	}{
		{"node<- hot-records neighbors", false, "largest", 1},
		{"node<- hot-extensions neighbors", false, "largest", 1},
		{"node<- median neighbors", true, "median", 2},
	} {
		qs := mk(c.group)
		cells, err := runner.G1(g1Windows(qs, "A", func(q runner.Query, _ int) map[string]int64 {
			return map[string]int64{"read.neighbors.seeks": work[q.Rank]}
		}), rules)
		if err != nil {
			t.Fatal(err)
		}
		got := cellOf(t, cells, c.group, runner.AgeNow, "seeks")
		if got.Pass != c.pass || got.Statistic != c.stat || got.Worst != qs[c.worst].Name() || got.Queries != 3 {
			t.Errorf("%s: pass %v, %s, worst %q over %d queries; want %v, %s, %q over 3", c.group, got.Pass, got.Statistic, got.Worst, got.Queries, c.pass, c.stat, qs[c.worst].Name())
		}
	}

	// Bytes are budgeted in blocks of the run: 6 blocks per entity and one per 64
	// items, so a read of 64 items and one entity may load 7 blocks of 32 KiB.
	qs := []runner.Query{{Group: "node<- hot-records neighbors", Op: runner.OpNeighbors, Age: runner.AgeNow, Fps: oneEntity(), Size: 64}}
	at := func(bytes int64) runner.G1Cell {
		cells, err := runner.G1(g1Windows(qs, "A", func(runner.Query, int) map[string]int64 {
			return map[string]int64{"cold_block_bytes": bytes}
		}), rules)
		if err != nil {
			t.Fatal(err)
		}
		return cellOf(t, cells, qs[0].Group, runner.AgeNow, "cold_block_bytes")
	}
	if c := at(7 * 32768); !c.Pass || math.Abs(c.Ratio-1) > 1e-9 {
		t.Errorf("exactly the budget: %+v", c)
	}
	if c := at(7*32768 + 1); c.Pass {
		t.Errorf("one byte over the budget passes: %+v", c)
	}
}

func TestG1RefusesWindowsItCannotJudge(t *testing.T) {
	t.Parallel()

	rules := runner.DefaultRules()
	qs := []runner.Query{{Group: "node<- hot-records neighbors", Op: runner.OpNeighbors, Age: runner.AgeNow, Fps: oneEntity(), Size: 5}}
	f := func(runner.Query, int) map[string]int64 { return map[string]int64{"read.neighbors.seeks": 1} }
	ws := g1Windows(qs, "A", f)
	if _, err := runner.G1(ws[:3], rules); err == nil {
		t.Error("three windows of four were judged")
	}
	ws[1].Days = 8
	if _, err := runner.G1(ws, rules); err == nil {
		t.Error("windows of 2, 8, 14 and 30 days were judged")
	}
	ws = g1Windows(qs, "A", f)
	ws[2].Candidates = nil
	if _, err := runner.G1(ws, rules); err == nil || !strings.Contains(err.Error(), "no results") {
		t.Errorf("a candidate with no results in a window: %v", err)
	}
	ws = g1Windows(qs, "A", f)
	other := []runner.Query{{Group: "node<- hot-records neighbors", Rank: 7, Op: runner.OpNeighbors, Age: runner.AgeNow, Fps: oneEntity(), Size: 5}}
	ws[0].Plan = &runner.Plan{Queries: other}
	if _, err := runner.G1(ws, rules); err == nil || !strings.Contains(err.Error(), "same pins") {
		t.Errorf("windows asking other questions: %v", err)
	}
}

// Only the instants of the rules are judged: a window read is not.
func TestG1JudgesTheInstantsOfTheRulesOnly(t *testing.T) {
	t.Parallel()

	qs := []runner.Query{
		{Group: "node<- hot-records neighbors", Op: runner.OpNeighbors, Age: runner.AgeNow, Fps: oneEntity(), Size: 5},
		{Group: "node<- hot-records neighbors", Op: runner.OpNeighbors, Age: runner.Age1h, Fps: oneEntity(), Size: 5},
		{Group: "node<- hot-records window", Op: runner.OpWindow, Age: runner.AgeWindow1d, Fps: oneEntity(), Size: 5},
	}
	cells, err := runner.G1(g1Windows(qs, "A", func(runner.Query, int) map[string]int64 {
		return map[string]int64{"read.neighbors.seeks": 1, "read.window.seeks": 1}
	}), runner.DefaultRules())
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range cells {
		if c.Age != runner.AgeNow && c.Age != runner.Age1d {
			t.Errorf("a cell at %s was judged", c.Age)
		}
	}
	if len(cells) != 4 { // one query at now, and four counters
		t.Errorf("%d cells, want 4", len(cells))
	}
}

// The report says whether each candidate passes and prints its cells, the worst
// first, a few or all.
func TestG1TheReportSaysWhoPassesAndWhoDoesNot(t *testing.T) {
	t.Parallel()

	rules := runner.DefaultRules()
	var qs []runner.Query
	for i := 0; i < 4; i++ {
		qs = append(qs, runner.Query{Group: "node<- hot-records neighbors", Rank: i, Op: runner.OpNeighbors, Age: runner.AgeNow, Fps: oneEntity(), Size: 5})
	}
	good := g1Windows(qs, "A", func(runner.Query, int) map[string]int64 { return map[string]int64{"read.neighbors.seeks": 10} })
	bad := g1Windows(qs, "B", func(_ runner.Query, d int) map[string]int64 {
		return map[string]int64{"read.neighbors.seeks": int64(d) * 4}
	})
	for i := range good {
		good[i].Candidates = append(good[i].Candidates, bad[i].Candidates...)
	}
	cells, err := runner.G1(good, rules)
	if err != nil {
		t.Fatal(err)
	}
	var all, few strings.Builder
	runner.WriteG1(&all, cells, rules, 0)
	runner.WriteG1(&few, cells, rules, 2)
	if !strings.Contains(all.String(), "A: PASSES G1") || !strings.Contains(all.String(), "B: FAILS G1 in") {
		t.Errorf("the report does not say who passes:\n%s", all.String())
	}
	if strings.Count(all.String(), "node<- hot-records neighbors now") != 8 || !strings.Contains(few.String(), "more cells") {
		t.Errorf("the cells printed: all %d, limited %d", strings.Count(all.String(), "node<- hot-records neighbors now"), strings.Count(few.String(), "node<- hot-records neighbors now"))
	}
	if !runner.G1Passes(cells, "A") || runner.G1Passes(cells, "B") || runner.G1Passes(cells, "C") {
		t.Error("G1Passes disagrees with the report, or passes a candidate that has no cell")
	}
}

// The budget of a read is per entity it asks about: a batch of ten entities is allowed
// ten times the base.
func TestG1ABatchIsBudgetedPerEntityItReads(t *testing.T) {
	t.Parallel()

	qs := []runner.Query{{Group: "node<- hot-records batch", Op: runner.OpBatch, Age: runner.AgeNow, Fps: make([]identity.Fingerprint, 10), Size: 50}}
	cells, err := runner.G1(g1Windows(qs, "A", func(runner.Query, int) map[string]int64 {
		return map[string]int64{"read.batch.seeks": 280}
	}), runner.DefaultRules())
	if err != nil {
		t.Fatal(err)
	}
	// 8 per entity x 10 + 4 per item x 50 = 280.
	if c := cellOf(t, cells, qs[0].Group, runner.AgeNow, "seeks"); c.Budget != 280 || c.Ratio != 1 || !c.Pass {
		t.Errorf("a batch of ten entities and fifty items: %+v", c)
	}
}

// The median population is judged by its lower median when its queries are even in
// number, and the growth diagnostics are what they say: the slope between the first and
// third windows, and a fit over all four.
func TestG1TheMedianAndTheDiagnostics(t *testing.T) {
	t.Parallel()

	rules := runner.DefaultRules()
	// Four queries of the median population whose projected ratios are in the order of
	// their rank: .25, .5, 1.5 and 2: the lower median is .5 (passes), the upper 1.5.
	work := []int64{7, 14, 42, 56} // against 28
	var qs []runner.Query
	for i := range work {
		qs = append(qs, runner.Query{Group: "node<- median neighbors", Rank: i, Op: runner.OpNeighbors, Age: runner.AgeNow, Fps: oneEntity(), Size: 5})
	}
	cells, err := runner.G1(g1Windows(qs, "A", func(q runner.Query, _ int) map[string]int64 {
		return map[string]int64{"read.neighbors.seeks": work[q.Rank]}
	}), rules)
	if err != nil {
		t.Fatal(err)
	}
	if c := cellOf(t, cells, qs[0].Group, runner.AgeNow, "seeks"); !c.Pass || c.Worst != qs[1].Name() || c.Statistic != "median" {
		t.Errorf("four queries: %+v, want the lower median to be the second", c)
	}

	// Work that is exactly linear in the days has slope one, in the first-to-third window
	// and in the fit, and is "linear"; work that is exactly flat has slope zero and is
	// "bounded".
	one := []runner.Query{{Group: "node<- hot-records neighbors", Op: runner.OpNeighbors, Age: runner.AgeNow, Fps: oneEntity(), Size: 100}}
	linear, err := runner.G1(g1Windows(one, "A", func(_ runner.Query, d int) map[string]int64 {
		return map[string]int64{"read.neighbors.seeks": int64(d) * 100}
	}), rules)
	if err != nil {
		t.Fatal(err)
	}
	if c := cellOf(t, linear, one[0].Group, runner.AgeNow, "seeks"); math.Abs(c.SlopeFirst-1) > 1e-9 || math.Abs(c.Fit-1) > 1e-9 || c.Class != "linear" {
		t.Errorf("linear work: slope %v, fit %v, class %s", c.SlopeFirst, c.Fit, c.Class)
	}
	flat, err := runner.G1(g1Windows(one, "A", func(runner.Query, int) map[string]int64 {
		return map[string]int64{"read.neighbors.seeks": 300}
	}), rules)
	if err != nil {
		t.Fatal(err)
	}
	if c := cellOf(t, flat, one[0].Group, runner.AgeNow, "seeks"); c.SlopeFirst != 0 || c.Fit != 0 || c.Class != "bounded" {
		t.Errorf("flat work: slope %v, fit %v, class %s", c.SlopeFirst, c.Fit, c.Class)
	}
	// A slope in between the thresholds is called so.
	between, err := runner.G1(g1Windows(one, "A", func(_ runner.Query, d int) map[string]int64 {
		return map[string]int64{"read.neighbors.seeks": int64(1000 * math.Pow(float64(d), 0.5))}
	}), rules)
	if err != nil {
		t.Fatal(err)
	}
	if c := cellOf(t, between, one[0].Group, runner.AgeNow, "seeks"); c.Class != "between" {
		t.Errorf("work growing as the square root of the days: slope %v, class %s", c.SlopeFirst, c.Class)
	}
}

// Work that shrinks does not earn a discount: growth is never negative.
func TestG1ShrinkingWorkIsNotCreditedWithNegativeGrowth(t *testing.T) {
	t.Parallel()

	qs := []runner.Query{{Group: "node<- hot-records neighbors", Op: runner.OpNeighbors, Age: runner.AgeNow, Fps: oneEntity(), Size: 5}}
	cells, err := runner.G1(g1Windows(qs, "A", func(_ runner.Query, d int) map[string]int64 {
		return map[string]int64{"read.neighbors.seeks": map[int]int64{2: 400, 7: 400, 14: 80, 30: 40}[d]}
	}), runner.DefaultRules())
	if err != nil {
		t.Fatal(err)
	}
	// 40 against a budget of 28 fails whatever the work was before.
	if c := cellOf(t, cells, qs[0].Group, runner.AgeNow, "seeks"); c.Pass || c.Growth != 0 || c.Projected != c.Ratio {
		t.Errorf("work that shrank to over its budget: %+v", c)
	}
}

// The slope between the first window and the third is the one between them, whatever
// happens in the second; and a slope exactly at a threshold is bounded at the first and
// linear at the second.
func TestG1TheFirstSlopeAndItsThresholds(t *testing.T) {
	t.Parallel()

	rules := runner.DefaultRules()
	one := []runner.Query{{Group: "node<- hot-records neighbors", Op: runner.OpNeighbors, Age: runner.AgeNow, Fps: oneEntity(), Size: 100}}
	run := func(w2, w14 int64, r runner.Rules) runner.G1Cell {
		cells, err := runner.G1(g1Windows(one, "A", func(_ runner.Query, d int) map[string]int64 {
			return map[string]int64{"read.neighbors.seeks": map[int]int64{2: w2, 7: w2, 14: w14, 30: w14}[d]}
		}), r)
		if err != nil {
			t.Fatal(err)
		}
		return cellOf(t, cells, one[0].Group, runner.AgeNow, "seeks")
	}
	// Flat to 7 days, then ten times more at 14: the slope from 2 to 14 sees it, one from 2 to 7 does not.
	if c := run(100, 1000, rules); math.Abs(c.SlopeFirst-math.Log(10)/math.Log(7)) > 1e-9 {
		t.Errorf("slope %v, want ln 10 / ln 7", c.SlopeFirst)
	}
	exact := math.Log(200.0/100.0) / math.Log(14.0/2.0)
	at := rules
	at.BoundedSlope, at.LinearSlope = exact, exact
	if c := run(100, 200, at); c.Class != "bounded" {
		t.Errorf("a slope exactly at the bounded threshold is %s", c.Class)
	}
	below, above := rules, rules
	below.BoundedSlope, below.LinearSlope = exact-1e-6, exact+1e-6
	if c := run(100, 200, below); c.Class != "between" {
		t.Errorf("a slope just above the bounded threshold and below the linear one is %s", c.Class)
	}
	above.BoundedSlope, above.LinearSlope = exact-2e-6, exact
	if c := run(100, 200, above); c.Class != "linear" {
		t.Errorf("a slope exactly at the linear threshold is %s", c.Class)
	}
}

// G1 is judged at the window the rules name as the target.
func TestG1NeedsTheTargetWindowToBeTheLastOne(t *testing.T) {
	t.Parallel()

	qs := []runner.Query{{Group: "node<- hot-records neighbors", Op: runner.OpNeighbors, Age: runner.AgeNow, Fps: oneEntity(), Size: 5}}
	rules := runner.DefaultRules()
	rules.TargetWindow = 14
	if _, err := runner.G1(g1Windows(qs, "A", func(runner.Query, int) map[string]int64 { return map[string]int64{"read.neighbors.seeks": 1} }), rules); err == nil {
		t.Error("windows ending at 30 days were judged against a target of 14")
	}
}

// A read that nothing is allowed to cost, or whose block size is unknown, would pass
// for want of a ratio: it is refused.
func TestG1RefusesABudgetItCannotApply(t *testing.T) {
	t.Parallel()

	qs := []runner.Query{{Group: "node<- hot-records neighbors", Op: runner.OpNeighbors, Age: runner.AgeNow, Fps: oneEntity(), Size: 5}}
	f := func(runner.Query, int) map[string]int64 { return map[string]int64{"read.neighbors.seeks": 1} }

	ws := g1Windows(qs, "A", f)
	for _, w := range ws {
		delete(w.Candidates[0].Results.Describe, "block_bytes")
	}
	if _, err := runner.G1(ws, runner.DefaultRules()); err == nil || !strings.Contains(err.Error(), "block_bytes") {
		t.Errorf("a candidate that does not say its block size: %v", err)
	}

	rules := runner.DefaultRules()
	rules.G1Budget = map[string]runner.Budget{}
	if _, err := runner.G1(g1Windows(qs, "A", f), rules); err == nil || !strings.Contains(err.Error(), "no budget") {
		t.Errorf("a counter with no budget: %v", err)
	}
}

// The block counters of a projected store depend on an index that is not a full
// store's, in either direction: a cell on them is reported and not decided, whether it is
// over or under its budget, and does not count towards the candidate's verdict. The
// cells on the other counters are decided as they were.
func TestG1BlockCountersOfAProjectionAreNotDecided(t *testing.T) {
	t.Parallel()

	qs := []runner.Query{{Group: "node<- hot-records neighbors", Op: runner.OpNeighbors, Age: runner.AgeNow, Fps: oneEntity(), Size: 5}}
	rules := runner.DefaultRules()
	for name, blockLoads := range map[string]int64{ // at every window: under the budget of about 8, or over it
		"block counters under": 1,
		"block counters over":  100,
	} {
		for _, pinned := range []bool{true, false} {
			ws := g1Windows(qs, "A", func(runner.Query, int) map[string]int64 {
				return map[string]int64{"read.neighbors.seeks": 1, "read.neighbors.block_loads": blockLoads, "read.neighbors.cold_block_bytes": blockLoads << 15}
			})
			if pinned {
				for _, w := range ws {
					w.Plan.Spec.Pins = &runner.Pins{}
				}
			}
			cells, err := runner.G1(ws, rules)
			if err != nil {
				t.Fatal(err)
			}
			for _, counter := range rules.G1Counters {
				cell := cellOf(t, cells, qs[0].Group, runner.AgeNow, counter)
				want := pinned && (counter == "block_loads" || counter == "cold_block_bytes")
				if cell.NotDecided != want {
					t.Errorf("%s, pinned %v, %s: not decided %v, want %v", name, pinned, counter, cell.NotDecided, want)
				}
			}
			// Pinned, the block counters do not count and the verdict is the other counters';
			// not pinned, a read over its block budget fails.
			if got, want := runner.G1Passes(cells, "A"), pinned || blockLoads < 8; got != want {
				t.Errorf("%s, pinned %v: passes %v, want %v", name, pinned, got, want)
			}
			var out strings.Builder
			runner.WriteG1(&out, cells, rules, 0)
			if got := strings.Contains(out.String(), "not decided, marked ?"); got != pinned {
				t.Errorf("%s, pinned %v: the verdict says cells are not decided: %v\n%s", name, pinned, got, out.String())
			}
		}
	}
}
