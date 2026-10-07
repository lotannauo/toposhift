package runner_test

import (
	"encoding/json"
	"math"
	"slices"
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

// g1WindowsSized is g1Windows with the size of each query's answer in each window
// given by size.
func g1WindowsSized(queries []runner.Query, candidate string, f func(q runner.Query, days int) map[string]int64, size func(q runner.Query, days int) int) []runner.RetainedWindow {
	ws := g1Windows(queries, candidate, f)
	for _, w := range ws {
		qs := slices.Clone(queries)
		for i := range qs {
			qs[i].Size = size(qs[i], w.Days)
		}
		w.Plan.Queries = qs
	}
	return ws
}

// atEveryInstant is the queries again at every instant G1 judges.
func atEveryInstant(qs []runner.Query) []runner.Query {
	var out []runner.Query
	for _, age := range runner.DefaultRules().G1Instants {
		for _, q := range qs {
			q.Age = age
			out = append(out, q)
		}
	}
	return out
}

// lsSlope is the least-squares slope of ln y on ln days over the four windows, as the
// rules define it, computed here on its own.
func lsSlope(y [4]float64) float64 {
	days := [4]float64{2, 7, 14, 30}
	var mx, my float64
	for i := range days {
		mx += math.Log(days[i]) / 4
		my += math.Log(y[i]) / 4
	}
	var num, den float64
	for i := range days {
		num += (math.Log(days[i]) - mx) * (math.Log(y[i]) - my)
		den += (math.Log(days[i]) - mx) * (math.Log(days[i]) - mx)
	}
	return num / den
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
// stops growing fails when it grows. With one query the population is that query, and
// its growth is the least-squares slope over every window.
func TestG1AReadMustFitItsBudgetAndStillFitWhenTheRetentionDoubles(t *testing.T) {
	t.Parallel()

	rules := runner.DefaultRules()
	// seeks: 8 per entity read and 4 per item of the answer: 48 for one entity and
	// ten items.
	qs := []runner.Query{{Group: "node<- hot-records neighbors", Rank: 0, Op: runner.OpNeighbors, Age: runner.AgeNow, Fps: oneEntity(), Size: 10}}
	for name, c := range map[string]struct {
		work       func(days int) int64
		ratio      float64
		growth     float64
		pass       bool
		wantClass  string
		wantGrowth func(float64) bool
	}{
		"flat and under":             {func(int) int64 { return 20 }, 20.0 / 48, 0, true, "bounded", nil},
		"flat and over":              {func(int) int64 { return 60 }, 60.0 / 48, 0, false, "bounded", nil},
		"under but growing linearly": {func(d int) int64 { return int64(d) }, 30.0 / 48, 1, false, "linear", nil}, // 30 at 30 days, projected 1.25
		"under and growing slowly":   {func(d int) int64 { return 10 + int64(math.Sqrt(float64(d))) }, 0, 0, true, "bounded", func(g float64) bool { return g > 0 && g < 0.25 }},
		"at the floor only at 30":    {func(d int) int64 { return map[int]int64{2: 1, 7: 1, 14: 1, 30: 8}[d] }, 8.0 / 48, 0, true, "bounded", nil}, // at the floor: no growth counted
		// A jump in the last window alone is a quarter of the slope over all four: under
		// the budget it passes, near it it fails.
		"flat to 14 days, then doubling, under": {
			func(d int) int64 { return map[int]int64{2: 10, 7: 10, 14: 10, 30: 20}[d] }, 20.0 / 48, 0, true, "bounded",
			func(g float64) bool { return math.Abs(g-lsSlope([4]float64{10, 10, 10, 20})) < 1e-12 },
		},
		"flat to 14 days, then growing, near": {
			func(d int) int64 { return map[int]int64{2: 20, 7: 20, 14: 20, 30: 46}[d] }, 46.0 / 48, 0, false, "bounded",
			func(g float64) bool { return math.Abs(g-lsSlope([4]float64{20, 20, 20, 46})) < 1e-12 },
		},
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
		if c.ratio > 0 && !(math.Abs(got.Ratio-c.ratio) <= 1e-9) {
			t.Errorf("%s: ratio %v, want %v", name, got.Ratio, c.ratio)
		}
		if c.wantGrowth == nil && !(math.Abs(got.Growth-c.growth) <= 1e-9) {
			t.Errorf("%s: growth %v, want %v", name, got.Growth, c.growth)
		}
		if c.wantGrowth != nil && !c.wantGrowth(got.Growth) {
			t.Errorf("%s: growth %v", name, got.Growth)
		}
		if want := got.Ratio * math.Pow(rules.G1Headroom, got.Growth); !(math.Abs(got.Projected-want) <= 1e-9) {
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
	if c := at(7 * 32768); !c.Pass || !(math.Abs(c.Ratio-1) <= 1e-9) {
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
	// The plans ask only "now": A passes every cell it has and is not shown to pass.
	if !strings.Contains(all.String(), "A: PASSES ONLY THE INSTANTS ITS PLANS ASK: G1 is not shown (its plans do not ask 3h, 9h, 1d, dead)") || !strings.Contains(all.String(), "B: FAILS G1 in") {
		t.Errorf("the report does not say who passes:\n%s", all.String())
	}
	if v := runner.G1Verdicts(cells, rules); len(v) != 2 || v[0].Pass || !slices.Equal(v[0].Missing, []string{runner.Age3h, runner.Age9h, runner.Age1d, runner.AgeDead}) || v[1].Pass || v[1].Failing != 1 {
		t.Errorf("verdicts %+v", v)
	}
	if strings.Count(all.String(), "node<- hot-records neighbors now") != 8 || !strings.Contains(few.String(), "more cells") {
		t.Errorf("the cells printed: all %d, limited %d", strings.Count(all.String(), "node<- hot-records neighbors now"), strings.Count(few.String(), "node<- hot-records neighbors now"))
	}
	if !runner.G1Passes(cells, "A") || runner.G1Passes(cells, "B") || runner.G1Passes(cells, "C") {
		t.Error("G1Passes disagrees with the report, or passes a candidate that has no cell")
	}

	// Asked at every instant of the rules, A passes.
	every := atEveryInstant(qs)
	cells, err = runner.G1(g1Windows(every, "A", func(runner.Query, int) map[string]int64 { return map[string]int64{"read.neighbors.seeks": 10} }), rules)
	if err != nil {
		t.Fatal(err)
	}
	var full strings.Builder
	runner.WriteG1(&full, cells, rules, 0)
	if v := runner.G1Verdicts(cells, rules); len(v) != 1 || !v[0].Pass || len(v[0].Missing) != 0 || !strings.Contains(full.String(), "A: PASSES G1\n") {
		t.Errorf("a candidate under its budget at every instant: %+v\n%s", v, full.String())
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
	if c := cellOf(t, linear, one[0].Group, runner.AgeNow, "seeks"); !(math.Abs(c.SlopeFirst-1) <= 1e-9) || !(math.Abs(c.Fit-1) <= 1e-9) || c.Class != "linear" {
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
	if c := run(100, 1000, rules); !(math.Abs(c.SlopeFirst-math.Log(10)/math.Log(7)) <= 1e-9) {
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
		for _, store := range []string{"projected", "full", "full at pins"} {
			pinned := store == "projected" // only a projection leaves its block counters undecided
			ws := g1Windows(qs, "A", func(runner.Query, int) map[string]int64 {
				return map[string]int64{"read.neighbors.seeks": 1, "read.neighbors.block_loads": blockLoads, "read.neighbors.cold_block_bytes": blockLoads << 15}
			})
			for _, w := range ws {
				switch store {
				case "projected":
					w.Plan.Spec.Pins = &runner.Pins{}
				case "full at pins":
					w.Plan.Spec.Pins, w.Plan.Spec.FullStore = &runner.Pins{}, true
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
					t.Errorf("%s, %s, %s: not decided %v, want %v", name, store, counter, cell.NotDecided, want)
				}
			}
			// Pinned, the block counters do not count and the verdict is the other counters';
			// not pinned, a read over its block budget fails.
			if got, want := runner.G1Passes(cells, "A"), pinned || blockLoads < 8; got != want {
				t.Errorf("%s, %s: passes %v, want %v", name, store, got, want)
			}
			var out strings.Builder
			runner.WriteG1(&out, cells, rules, 0)
			if got := strings.Contains(out.String(), "not decided, marked ?"); got != pinned {
				t.Errorf("%s, %s: the verdict says cells are not decided: %v\n%s", name, store, got, out.String())
			}
		}
	}
}

// population builds the queries of one population with the work of each in each
// window (2, 7, 14 and 30 days) on one counter, every query one entity with an answer
// of size items, and judges it.
func population(t *testing.T, group string, size int, counter, key string, work [][4]int64, rules runner.Rules) runner.G1Cell {
	t.Helper()
	var qs []runner.Query
	for i := range work {
		qs = append(qs, runner.Query{Group: group, Rank: i, Op: runner.OpNeighbors, Age: runner.AgeNow, Fps: oneEntity(), Size: size})
	}
	idx := map[int]int{2: 0, 7: 1, 14: 2, 30: 3}
	cells, err := runner.G1(g1Windows(qs, "A", func(q runner.Query, d int) map[string]int64 {
		return map[string]int64{key: work[q.Rank][idx[d]]}
	}), rules)
	if err != nil {
		t.Fatal(err)
	}
	return cellOf(t, cells, group, runner.AgeNow, counter)
}

// The growth is the population's, not a query's: one query that met a checkpoint by
// chance in the window of 14 days (and so seems to grow tenfold to 30) does not make
// a population whose statistic is flat fail. The slope of each query over the last two
// windows, which the rules used before, would have failed it at 6.8 times the budget.
func TestG1GrowthIsThePopulationsNotThatOfOneQuery(t *testing.T) {
	t.Parallel()

	rules := runner.DefaultRules()
	c := population(t, "node<- hot-records neighbors", 10, "seeks", "read.neighbors.seeks", [][4]int64{
		{40, 40, 4, 40}, // lucky at 14 days
		{30, 30, 30, 30},
		{20, 20, 20, 20},
	}, rules)
	want := lsSlope([4]float64{40.0 / 48, 40.0 / 48, 30.0 / 48, 40.0 / 48})
	if !c.Pass || !(math.Abs(c.Fit-want) <= 1e-12) || c.Growth != 0 || c.Projected != c.Ratio || c.Ratio != 40.0/48 || !strings.Contains(c.Worst, "#0") {
		t.Errorf("%+v; want a pass at 40/48 with fit %v and no growth", c, want)
	}
	if c.QueryFitOf == "" || c.LinearQuery || c.SlopeLast <= 0 {
		t.Errorf("the diagnostics: query fit %v of %q (linear %v), slope over the last two %v", c.QueryFit, c.QueryFitOf, c.LinearQuery, c.SlopeLast)
	}
}

// The statistic fitted is of the budget ratio in each window, whose budget is of the
// answer in that window: a read whose answer doubles and whose work doubles with it
// does not grow, and one whose work doubles with a fixed answer does.
func TestG1GrowthIsOfTheRatioToEachWindowsBudget(t *testing.T) {
	t.Parallel()

	rules := runner.DefaultRules()
	qs := []runner.Query{{Group: "node<- hot-records neighbors", Op: runner.OpNeighbors, Age: runner.AgeNow, Fps: oneEntity()}}
	// An answer of 2d items: a budget of 8 + 8d seeks, and work of three quarters of it.
	cells, err := runner.G1(g1WindowsSized(qs, "A", func(_ runner.Query, d int) map[string]int64 {
		return map[string]int64{"read.neighbors.seeks": int64(6 + 6*d)}
	}, func(_ runner.Query, d int) int { return 2 * d }), rules)
	if err != nil {
		t.Fatal(err)
	}
	c := cellOf(t, cells, qs[0].Group, runner.AgeNow, "seeks")
	if !c.Pass || !(math.Abs(c.Fit) <= 1e-12) || c.Ratio != 0.75 || c.Budget != 248 {
		t.Errorf("work in proportion to the answer: %+v", c)
	}
	// The same work with the answer of the last window throughout grows.
	cells, err = runner.G1(g1WindowsSized(qs, "A", func(_ runner.Query, d int) map[string]int64 {
		return map[string]int64{"read.neighbors.seeks": int64(6 + 6*d)}
	}, func(runner.Query, int) int { return 60 }), rules)
	if err != nil {
		t.Fatal(err)
	}
	if c := cellOf(t, cells, qs[0].Group, runner.AgeNow, "seeks"); c.Pass || !(math.Abs(c.Growth-lsSlope([4]float64{18, 48, 90, 186})) <= 1e-12) {
		t.Errorf("work growing with a fixed answer: %+v", c)
	}
}

// The median population's statistic is the median in each window, whichever query it
// is: here the median query at 30 days is flat, and the population's median grows.
func TestG1TheMedianIsTakenInEachWindow(t *testing.T) {
	t.Parallel()

	rules := runner.DefaultRules()
	c := population(t, "node<- median neighbors", 10, "seeks", "read.neighbors.seeks", [][4]int64{
		{15, 22, 38, 60},
		{45, 45, 45, 45},
		{30, 30, 30, 30},
	}, rules)
	want := lsSlope([4]float64{30.0 / 48, 30.0 / 48, 38.0 / 48, 45.0 / 48})
	if c.Pass || !(math.Abs(c.Fit-want) <= 1e-12) || !strings.Contains(c.Worst, "#1") || c.Ratio != 45.0/48 || c.Statistic != "median" {
		t.Errorf("%+v; want a fail at 45/48 with fit %v from the medians 30, 30, 38, 45", c, want)
	}
}

// The growth counts only when the work of the statistic's query at the target window
// is above the counter's floor.
func TestG1TheFloorIsOnTheStatisticsWorkAtTheTarget(t *testing.T) {
	t.Parallel()

	rules := runner.DefaultRules()
	// seeks: a floor of 8, and a budget of 12 for one entity and one item.
	at := population(t, "node<- hot-records neighbors", 1, "seeks", "read.neighbors.seeks", [][4]int64{{1, 2, 4, 8}}, rules)
	over := population(t, "node<- hot-records neighbors", 1, "seeks", "read.neighbors.seeks", [][4]int64{{1, 2, 4, 9}}, rules)
	if !at.Pass || at.Growth != 0 || at.Fit < 0.5 {
		t.Errorf("work at the floor: %+v", at)
	}
	if over.Pass || over.Growth != over.Fit || over.Fit < 0.5 {
		t.Errorf("work one above the floor: %+v", over)
	}
}

// A window in which the work is zero counts as work of one, in the statistic and in
// choosing the query that is it: work that appears from nothing grows, and is not
// judged on the windows in which it is there alone.
func TestG1WorkFromNothingGrows(t *testing.T) {
	t.Parallel()

	// steps: a floor of 256, and a budget of 128 + 5 x 40 = 328.
	c := population(t, "node<- hot-records neighbors", 40, "internal_steps+checkpoint_entries_decoded+baseline_entries_decoded", "read.neighbors.internal_steps",
		[][4]int64{{0, 0, 0, 300}}, runner.DefaultRules())
	if want := lsSlope([4]float64{1.0 / 328, 1.0 / 328, 1.0 / 328, 300.0 / 328}); c.Pass || !(math.Abs(c.Fit-want) <= 1e-12) || c.Ratio != 300.0/328 {
		t.Errorf("work from nothing to 300 of 328: %+v, want a fit of %v", c, want)
	}

	// Seeks: a query of no work and a budget of 48 is larger, counted as one, than a
	// query of one and a budget of 96.
	qs := []runner.Query{
		{Group: "node<- hot-records neighbors", Rank: 0, Op: runner.OpNeighbors, Age: runner.AgeNow, Fps: oneEntity(), Size: 10},
		{Group: "node<- hot-records neighbors", Rank: 1, Op: runner.OpNeighbors, Age: runner.AgeNow, Fps: oneEntity(), Size: 22},
	}
	cells, err := runner.G1(g1Windows(qs, "A", func(q runner.Query, d int) map[string]int64 {
		if q.Rank == 1 {
			return map[string]int64{"read.neighbors.seeks": 1}
		}
		return map[string]int64{"read.neighbors.seeks": map[int]int64{2: 0, 7: 0, 14: 0, 30: 40}[d]}
	}), runner.DefaultRules())
	if err != nil {
		t.Fatal(err)
	}
	if c, want := cellOf(t, cells, qs[0].Group, runner.AgeNow, "seeks"), lsSlope([4]float64{1.0 / 48, 1.0 / 48, 1.0 / 48, 40.0 / 48}); !(math.Abs(c.Fit-want) <= 1e-12) {
		t.Errorf("no work against one: %+v, want a fit of %v", c, want)
	}
}

// A query whose own work grows at or above the linear slope is flagged, whether or not
// the population's statistic grows, and the flag does not decide; a query at or below
// the floor at the target window is not looked at.
func TestG1AQueryThatGrowsLinearlyIsFlagged(t *testing.T) {
	t.Parallel()

	rules := runner.DefaultRules()
	work := [][4]int64{{2, 7, 14, 30}, {100, 100, 100, 100}} // a budget of 128 for 30 items
	c := population(t, "node<- hot-records neighbors", 30, "seeks", "read.neighbors.seeks", work, rules)
	if !c.Pass || c.Fit != 0 || !c.LinearQuery || !(math.Abs(c.QueryFit-1) <= 1e-12) || !strings.Contains(c.QueryFitOf, "#0") {
		t.Errorf("one query linear under a flat statistic: %+v", c)
	}
	at := rules
	at.LinearSlope = c.QueryFit
	if c := population(t, "node<- hot-records neighbors", 30, "seeks", "read.neighbors.seeks", work, at); !c.LinearQuery {
		t.Errorf("a fit exactly at the linear slope is not flagged: %+v", c)
	}
	above := rules
	above.LinearSlope = math.Nextafter(c.QueryFit, 2)
	if c := population(t, "node<- hot-records neighbors", 30, "seeks", "read.neighbors.seeks", work, above); c.LinearQuery {
		t.Errorf("a fit just under the linear slope is flagged: %+v", c)
	}
	if c := population(t, "node<- hot-records neighbors", 30, "seeks", "read.neighbors.seeks", [][4]int64{{1, 2, 4, 8}, {100, 100, 100, 100}}, rules); c.LinearQuery || strings.Contains(c.QueryFitOf, "#0") {
		t.Errorf("a query at the floor was looked at: %+v", c)
	}
}

// Of two queries with the same ratio at the target window, the statistic is the one
// with more work, wherever it is in the plan.
func TestG1ATieInRatioGoesToTheQueryWithMoreWork(t *testing.T) {
	t.Parallel()

	qs := []runner.Query{
		{Group: "node<- hot-records neighbors", Rank: 0, Op: runner.OpNeighbors, Age: runner.AgeNow, Fps: oneEntity(), Size: 22}, // a budget of 96
		{Group: "node<- hot-records neighbors", Rank: 1, Op: runner.OpNeighbors, Age: runner.AgeNow, Fps: oneEntity(), Size: 10}, // a budget of 48
	}
	cells, err := runner.G1(g1Windows(qs, "A", func(q runner.Query, _ int) map[string]int64 {
		return map[string]int64{"read.neighbors.seeks": map[int]int64{0: 48, 1: 24}[q.Rank]}
	}), runner.DefaultRules())
	if err != nil {
		t.Fatal(err)
	}
	if c := cellOf(t, cells, qs[0].Group, runner.AgeNow, "seeks"); c.Worst != qs[0].Name() || c.Value != 48 || c.Ratio != 0.5 {
		t.Errorf("a tie: %+v", c)
	}
}

// A candidate whose every cell is undecided has no verdict to pass: with nothing decided
// it does not pass, however many instants its plans ask.
func TestG1ACandidateWithNothingDecidedDoesNotPass(t *testing.T) {
	t.Parallel()

	qs := []runner.Query{{Group: "node<- hot-records neighbors", Op: runner.OpNeighbors, Age: runner.AgeNow, Fps: oneEntity(), Size: 5}}
	f := func(runner.Query, int) map[string]int64 {
		return map[string]int64{"read.neighbors.block_loads": 1, "read.neighbors.cold_block_bytes": 1}
	}
	rules := runner.DefaultRules()
	rules.G1Counters = []string{"block_loads", "cold_block_bytes"} // undecided on a projection
	rules.G1Instants = []string{runner.AgeNow}
	ws := g1Windows(qs, "A", f)
	for _, w := range ws {
		w.Plan.Spec.Pins = &runner.Pins{}
	}
	cells, err := runner.G1(ws, rules)
	if err != nil {
		t.Fatal(err)
	}
	vs := runner.G1Verdicts(cells, rules)
	if len(vs) != 1 || vs[0].Decided != 0 || vs[0].NotDecided == 0 || vs[0].Pass || len(vs[0].Missing) != 0 {
		t.Errorf("a candidate with only undecided cells: %+v", vs)
	}
}

// A read that costs nothing has a ratio of nothing: the ratio is the work over the
// budget, and the work counts as at least one only where a slope is taken.
func TestG1AReadThatCostsNothingHasNoRatio(t *testing.T) {
	t.Parallel()

	qs := []runner.Query{{Group: "node<- hot-records neighbors", Op: runner.OpNeighbors, Age: runner.AgeNow, Fps: oneEntity(), Size: 5}}
	cells, err := runner.G1(g1Windows(qs, "A", func(runner.Query, int) map[string]int64 {
		return map[string]int64{"read.neighbors.seeks": 0}
	}), runner.DefaultRules())
	if err != nil {
		t.Fatal(err)
	}
	c := cellOf(t, cells, qs[0].Group, runner.AgeNow, "seeks")
	if c.Ratio != 0 || c.Projected != 0 || c.Growth != 0 || !c.Pass {
		t.Errorf("a read with no work: %+v", c)
	}
}

// Windows that cannot be judged together give a report in which nothing passes, and the
// lists of a report are lists, never null.
func TestAReportOfWindowsThatCannotBeJudgedTogetherPassesNothing(t *testing.T) {
	t.Parallel()

	rules := runner.DefaultRules()
	cells := []runner.G1Cell{{Candidate: "A", Group: "g", Age: runner.AgeNow, Counter: "seeks", Pass: true}}
	rules.G1Instants = []string{runner.AgeNow}
	good := runner.NewG1Report(cells, rules, nil)
	if !good.Judgeable || len(good.Verdicts) != 1 || !good.Verdicts[0].Pass || good.Problems == nil || good.Verdicts[0].Missing == nil {
		t.Errorf("a report of windows that agree: %+v", good)
	}
	bad := runner.NewG1Report(cells, rules, []string{"window of 7 days: another plan"})
	if bad.Judgeable || bad.Verdicts[0].Pass {
		t.Errorf("a report of windows that cannot be judged together has a pass: %+v", bad)
	}
	b, err := json.Marshal(good)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "null") {
		t.Errorf("a list of the report is null: %s", b)
	}
}
