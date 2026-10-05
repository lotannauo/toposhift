package runner_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/bench/spike/runner"
)

// A counter orders two values only if the larger is above its floor and more than
// the tolerance times the smaller, whichever is the larger.
func TestACounterOrdersTwoValuesOnlyBeyondItsFloorAndTolerance(t *testing.T) {
	t.Parallel()

	r := runner.DefaultRules()
	floor := float64(r.CounterFloors["seeks"])
	tol := r.OrderTolerance
	for _, c := range []struct {
		name string
		a, b float64
		want int
	}{
		{"a larger beyond the tolerance", 1000, 500, +1},
		{"b larger beyond the tolerance", 500, 1000, -1},
		{"within the tolerance", 1000, 1000 / (tol - 0.01), 0},
		{"just beyond the tolerance", 1000, 1000 / (tol + 0.01), +1},
		{"equal", 700, 700, 0},
		{"the larger is at the floor", floor, 0, 0},
		{"the larger is above the floor and the other is zero", floor + 1, 0, +1},
		{"both below the floor", floor - 1, 1, 0},
		{"zero both", 0, 0, 0},
	} {
		if got := r.Orders("seeks", c.a, c.b); got != c.want {
			t.Errorf("%s: Orders(%v, %v) = %d, want %d", c.name, c.a, c.b, got, c.want)
		}
		if got := r.Orders("seeks", c.b, c.a); got != -c.want {
			t.Errorf("%s: Orders is not antisymmetric: (%v, %v) = %d, want %d", c.name, c.b, c.a, got, -c.want)
		}
	}
	// A value exactly the tolerance times the other is not beyond it.
	exact := r
	exact.OrderTolerance = 2
	if got := exact.Orders("seeks", 2000, 1000); got != 0 {
		t.Errorf("Orders at exactly the tolerance = %d, want 0", got)
	}
	if got := exact.Orders("seeks", 2001, 1000); got != +1 {
		t.Errorf("Orders just beyond the tolerance = %d, want +1", got)
	}
	// The floor is the counter's own.
	if r.Orders("cold_block_bytes", 10_000, 1000) != 0 || r.Orders("seeks", 10_000, 1000) == 0 {
		t.Error("the floors of two counters were taken for each other")
	}
}

// synthetic builds a plan of cells and candidates whose counters are the numbers
// given, so the verdicts can be checked against arithmetic.
func synthetic(cells []string, queriesPerCell int, counters map[string]map[string][]int64) (*runner.Plan, []*runner.Candidate) {
	plan := &runner.Plan{}
	for _, cell := range cells {
		group, age, _ := strings.Cut(cell, "@")
		for k := 0; k < queriesPerCell; k++ {
			plan.Queries = append(plan.Queries, runner.Query{Group: group, Age: age, Op: runner.OpNeighbors, Rank: k})
		}
	}
	var cs []*runner.Candidate
	for name, byCell := range counters {
		res := &runner.Results{Candidate: name}
		for _, cell := range cells {
			for k := 0; k < queriesPerCell; k++ {
				qr := runner.QueryResult{Counters: map[string]int64{}}
				for counter, vals := range byCell {
					if cell == strings.SplitN(counter, "|", 2)[0] {
						qr.Counters[strings.SplitN(counter, "|", 2)[1]] = vals[k]
					}
				}
				res.Queries = append(res.Queries, qr)
			}
		}
		cs = append(cs, &runner.Candidate{Manifest: &runner.Manifest{}, Results: res})
	}
	return plan, cs
}

func TestVerdictsAreOrderedMixedOrTiedOnTheMeansOfACell(t *testing.T) {
	t.Parallel()

	// Three cells and two candidates, A and B, over two queries each.
	//   c1: A seeks 1000 against B's 10, B steps 5000 against A's 100: mixed
	//   c2: A seeks 1000 against B's 10, steps alike: ordered, B first
	//   c3: all within the tolerance: tied
	// Cell values are means: in c1 A's seeks are 1800 and 200 (mean 1000).
	cells := []string{"g@now", "g@1d", "h@now"}
	plan, cs := synthetic(cells, 2, map[string]map[string][]int64{
		"A": {
			"g@now|read.neighbors.seeks": {1800, 200}, "g@now|read.neighbors.internal_steps": {100, 100},
			"g@1d|read.neighbors.seeks": {1000, 1000}, "g@1d|read.neighbors.internal_steps": {500, 500},
			"h@now|read.neighbors.seeks": {400, 400}, "h@now|read.neighbors.internal_steps": {500, 500},
		},
		"B": {
			"g@now|read.neighbors.seeks": {10, 10}, "g@now|read.neighbors.internal_steps": {5000, 5000},
			"g@1d|read.neighbors.seeks": {10, 10}, "g@1d|read.neighbors.internal_steps": {500, 500},
			"h@now|read.neighbors.seeks": {410, 410}, "h@now|read.neighbors.internal_steps": {505, 505},
		},
	})
	// map iteration made the order of cs arbitrary; the verdicts are in name order
	if cs[0].Results.Candidate > cs[1].Results.Candidate {
		cs[0], cs[1] = cs[1], cs[0]
	}
	rules := runner.DefaultRules()
	vs := runner.Verdicts(plan, cs, rules)
	if len(vs) != 3 {
		t.Fatalf("%d verdicts for three cells and one pair", len(vs))
	}
	by := map[string]runner.Verdict{}
	for _, v := range vs {
		by[v.Cell] = v
	}
	if v := by["g now"]; !v.Mixed() || fmt.Sprint(v.BetterB) != "[seeks]" || fmt.Sprint(v.BetterA) != "[internal_steps+checkpoint_entries_decoded+baseline_entries_decoded]" {
		t.Errorf("g now: %+v, want seeks for B and steps for A", v)
	}
	if v := by["g 1d"]; !v.Ordered() || len(v.BetterA) != 0 || fmt.Sprint(v.BetterB) != "[seeks]" {
		t.Errorf("g 1d: %+v, want ordered in B's favour", v)
	}
	if v := by["h now"]; v.Ordered() || v.Mixed() {
		t.Errorf("h now: %+v, want tied", v)
	}
	// A cell's value is the mean over its queries, not their sum.
	if got := runner.CellMeans(plan, cs, "seeks")["g now"]; len(got) != 2 || got[0]+got[1] != 1000+10 {
		t.Errorf("means of seeks in g now: %v, want 1000 and 10", got)
	}
	mixed := runner.MixedCells(vs)
	if len(mixed) != 1 || mixed[0].Cell != "g now" {
		t.Fatalf("mixed cells %+v, want only g now", mixed)
	}
	d1 := runner.MixedDigest(mixed)
	if d1 == runner.MixedDigest(nil) {
		t.Error("the list of mixed cells has the digest of an empty one")
	}
	// A change in the verdict changes the digest.
	mixed[0].BetterA = append(mixed[0].BetterA, "block_loads")
	if runner.MixedDigest(mixed) == d1 {
		t.Error("the digest does not cover the counters of a mixed cell")
	}
}

// The terms of a rule's counter are Pebble's statistics for the read, a layout's
// own counter, or a counter of the runner's, and a sum of them is their sum.
func TestAVerdictAddsTheTermsOfACounter(t *testing.T) {
	t.Parallel()

	cells := []string{"g@now"}
	plan, cs := synthetic(cells, 1, map[string]map[string][]int64{
		"A": {"g@now|read.neighbors.internal_steps": {100}, "g@now|read.checkpoint_entries_decoded": {1000}, "g@now|read.baseline_entries_decoded": {1000}},
		"B": {"g@now|read.neighbors.internal_steps": {1500}, "g@now|read.checkpoint_entries_decoded": {0}, "g@now|read.baseline_entries_decoded": {0}},
	})
	if cs[0].Results.Candidate > cs[1].Results.Candidate {
		cs[0], cs[1] = cs[1], cs[0]
	}
	// Alone, A's steps (100) are below B's (1500); with the entries it decoded (2100) it is the larger.
	vs := runner.Verdicts(plan, cs, runner.DefaultRules())
	if len(vs) != 1 || !vs[0].Ordered() || len(vs[0].BetterB) != 1 {
		t.Errorf("verdicts %+v, want B ahead on steps plus decoded entries", vs)
	}
	means := runner.CellMeans(plan, cs, "internal_steps+checkpoint_entries_decoded+baseline_entries_decoded")
	if got := means["g now"]; len(got) != 2 || got[0]+got[1] != 2100+1500 {
		t.Errorf("means %v, want 2100 and 1500", got)
	}
}

// A candidate with no result for the queries of a cell is left out of it: its
// missing numbers do not make it the cheapest.
func TestACandidateWithoutResultsIsLeftOutOfACell(t *testing.T) {
	t.Parallel()

	plan, cs := synthetic([]string{"g@now"}, 1, map[string]map[string][]int64{
		"A": {"g@now|read.neighbors.seeks": {1000}},
		"B": {"g@now|read.neighbors.seeks": {10}},
	})
	cs = append(cs, &runner.Candidate{Manifest: &runner.Manifest{}, Results: &runner.Results{Candidate: "C"}})
	for _, v := range runner.Verdicts(plan, cs, runner.DefaultRules()) {
		if (v.A == "C" || v.B == "C") && (v.Ordered() || v.Mixed()) {
			t.Errorf("%+v: a candidate with no results orders a cell", v)
		}
	}
}

// The report says how many cells are ordered, mixed and tied.
func TestTheReportCountsOrderedMixedAndTiedCells(t *testing.T) {
	t.Parallel()

	plan, cs := synthetic([]string{"g@now", "g@1d", "k@1d", "h@now"}, 1, map[string]map[string][]int64{
		"A": {
			"g@now|read.neighbors.seeks": {1000}, "g@now|read.neighbors.internal_steps": {100},
			"g@1d|read.neighbors.seeks": {1000}, "g@1d|read.neighbors.internal_steps": {500},
			"k@1d|read.neighbors.seeks": {1000}, "k@1d|read.neighbors.internal_steps": {500},
			"h@now|read.neighbors.seeks": {400}, "h@now|read.neighbors.internal_steps": {500},
		},
		"B": {
			"g@now|read.neighbors.seeks": {10}, "g@now|read.neighbors.internal_steps": {5000},
			"g@1d|read.neighbors.seeks": {10}, "g@1d|read.neighbors.internal_steps": {500},
			"k@1d|read.neighbors.seeks": {10}, "k@1d|read.neighbors.internal_steps": {500},
			"h@now|read.neighbors.seeks": {410}, "h@now|read.neighbors.internal_steps": {505},
		},
	})
	var out strings.Builder
	runner.Write(&out, plan, cs, true)
	if want := "ordered 2, mixed 1 (counters disagree), tied on counters 1"; !strings.Contains(out.String(), want) {
		t.Errorf("the report lacks %q:\n%s", want, out.String())
	}
	if !strings.Contains(out.String(), "g now: A costs less in internal_steps") {
		t.Errorf("the report does not name the mixed cell:\n%s", out.String())
	}
}

// A counter with no floor would order any two values above zero, so every counter
// the rules order by, and decide the growth of a read on, has one.
func TestEveryCounterOfTheRulesHasAFloor(t *testing.T) {
	t.Parallel()

	r := runner.DefaultRules()
	for _, c := range append(slices.Clone(r.OrderCounters), r.G1Counters...) {
		if r.CounterFloors[c] <= 0 {
			t.Errorf("counter %q has no floor", c)
		}
	}
}

// A read that fills more than half the cache from empty is called out, because its
// cold counts may include a block loaded twice.
func TestTheReportWarnsWhenAColdReadFilledTheCache(t *testing.T) {
	t.Parallel()

	plan, cs := synthetic([]string{"g@now"}, 2, map[string]map[string][]int64{
		"A": {"g@now|cold_cache_bytes": {600 << 10, 100 << 10}},
		"B": {"g@now|cold_cache_bytes": {400 << 10, 100 << 10}},
	})
	plan.CacheBytes = 1 << 20
	var out strings.Builder
	runner.Write(&out, plan, cs, true)
	if !strings.Contains(out.String(), "WARNING: A: 1 queries filled more than half of the 1 MiB cache") {
		t.Errorf("no warning for A:\n%s", out.String())
	}
	if strings.Contains(out.String(), "WARNING: B") {
		t.Errorf("a warning for B, whose largest read filled exactly 40%% of the cache:\n%s", out.String())
	}
}

// A cell is judged by its queries, not by their mean: two prefixes that the same
// counter puts in opposite directions cancel in a mean and are a mixed cell.
func TestACellWhoseQueriesCancelInAMeanIsMixed(t *testing.T) {
	t.Parallel()

	plan, cs := synthetic([]string{"g@now"}, 2, map[string]map[string][]int64{
		"A": {"g@now|read.neighbors.seeks": {1000, 10}},
		"B": {"g@now|read.neighbors.seeks": {10, 1000}},
	})
	if m := runner.CellMeans(plan, cs, "seeks")["g now"]; len(m) != 2 || m[0] != m[1] {
		t.Fatalf("the means %v should be equal: the case shows nothing otherwise", m)
	}
	vs := runner.Verdicts(plan, cs, runner.DefaultRules())
	if len(vs) != 1 || !vs[0].Mixed() || vs[0].Queries != 2 || vs[0].AheadA != 1 || vs[0].AheadB != 1 {
		t.Errorf("verdicts %+v, want one mixed cell of two queries, one each way", vs)
	}
}

// A cell is ordered only if a majority of its queries put the pair in order one way
// and none puts it the other way; fewer is a tie, and one query the other way is a
// mixed cell.
func TestACellIsOrderedByAMajorityOfItsQueries(t *testing.T) {
	t.Parallel()

	for name, c := range map[string]struct {
		a, b                 []int64
		ordered, mixed, tied bool
		first                string
	}{
		"all three put B first":       {[]int64{1000, 1000, 1000}, []int64{10, 10, 10}, true, false, false, "B"},
		"all three put A first":       {[]int64{10, 10, 10}, []int64{1000, 1000, 1000}, true, false, false, "A"},
		"two of three put B first":    {[]int64{1000, 1000, 10}, []int64{10, 10, 10}, true, false, false, "B"},
		"one of three puts B first":   {[]int64{1000, 10, 10}, []int64{10, 10, 10}, false, false, true, ""},
		"two B first and one A first": {[]int64{1000, 1000, 10}, []int64{10, 10, 1000}, false, true, false, ""},
		"none puts either first":      {[]int64{100, 100, 100}, []int64{105, 105, 105}, false, false, true, ""},
	} {
		plan, cs := synthetic([]string{"g@now"}, 3, map[string]map[string][]int64{
			"A": {"g@now|read.neighbors.seeks": c.a},
			"B": {"g@now|read.neighbors.seeks": c.b},
		})
		vs := runner.Verdicts(plan, cs, runner.DefaultRules())
		if len(vs) != 1 {
			t.Fatalf("%s: %d verdicts", name, len(vs))
		}
		if got := vs[0].First(); got != c.first {
			t.Errorf("%s: the cell is ordered towards %q, want %q (A ahead %d, B ahead %d)", name, got, c.first, vs[0].AheadA, vs[0].AheadB)
		}
		if v := vs[0]; v.Ordered() != c.ordered || v.Mixed() != c.mixed || (!v.Ordered() && !v.Mixed()) != c.tied {
			t.Errorf("%s: ordered %v, mixed %v (queries %d, A ahead %d, B ahead %d, mixed %d); want ordered %v, mixed %v, tied %v",
				name, v.Ordered(), v.Mixed(), v.Queries, v.AheadA, v.AheadB, v.MixedQueries, c.ordered, c.mixed, c.tied)
		}
	}
}

// The verdicts, and so the digest of the mixed cells, do not depend on the order
// the candidates are given in.
func TestVerdictsDoNotDependOnTheOrderOfTheCandidates(t *testing.T) {
	t.Parallel()

	plan, cs := synthetic([]string{"g@now", "g@1d"}, 2, map[string]map[string][]int64{
		"A": {"g@now|read.neighbors.seeks": {1000, 1000}, "g@now|read.neighbors.internal_steps": {100, 100}, "g@1d|read.neighbors.seeks": {5, 5}},
		"B": {"g@now|read.neighbors.seeks": {10, 10}, "g@now|read.neighbors.internal_steps": {5000, 5000}, "g@1d|read.neighbors.seeks": {500, 500}},
		"C": {"g@now|read.neighbors.seeks": {400, 400}, "g@now|read.neighbors.internal_steps": {400, 400}, "g@1d|read.neighbors.seeks": {5, 5}},
	})
	rules := runner.DefaultRules()
	want := runner.MixedDigest(runner.MixedCells(runner.Verdicts(plan, cs, rules)))
	for _, perm := range [][]int{{0, 1, 2}, {2, 1, 0}, {1, 2, 0}, {2, 0, 1}} {
		shuffled := []*runner.Candidate{cs[perm[0]], cs[perm[1]], cs[perm[2]]}
		vs := runner.Verdicts(plan, shuffled, rules)
		for _, v := range vs {
			if v.A >= v.B {
				t.Errorf("the pair %s, %s is not in name order", v.A, v.B)
			}
		}
		if got := runner.MixedDigest(runner.MixedCells(vs)); got != want {
			t.Errorf("order %v gives digest %.12s, want %.12s", perm, got, want)
		}
	}
}
