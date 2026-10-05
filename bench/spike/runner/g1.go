package runner

import (
	"fmt"
	"io"
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
)

// RetainedWindow is the runs over one window of retained history: the plan of the
// stream that has always held Days days, and every candidate read from it. The
// plans of the windows ask the same questions of the same pinned prefixes, so a
// query is in each of them under one name.
type RetainedWindow struct {
	Days       int
	Plan       *Plan
	Candidates []*Candidate
}

// G1Cell is how one candidate does on one population of queries (a class of
// prefix, how its members were chosen, the read, at one instant) on one counter of
// G1: the queries it is the statistic of, the worst of them, and what the rule of G1
// makes of it.
type G1Cell struct {
	Candidate, Group, Age, Counter string
	// Statistic says what the cell is the verdict of: "largest" for a population of
	// the busiest prefixes, "median" for the one around the middle.
	Statistic string
	Queries   int
	// Worst is the query whose projected ratio is the statistic of the population.
	Worst string
	// Value is its work at the target window and Budget what the rules allow it;
	// Ratio is their quotient.
	Value, Budget, Ratio float64
	// Growth is its slope over the last two windows (0 when it is at or below the
	// counter's floor) and Projected the ratio times G1Headroom to that power: the
	// ratio if the retention were multiplied by G1Headroom.
	Growth, Projected float64
	// SlopeFirst is its slope between the first window and the third, the
	// diagnostic of bounded and linear growth, Class what the rules call it, and Fit
	// the slope of a least-squares line of the logarithm of its work against that of
	// the window, over every window in which it is above zero.
	SlopeFirst float64
	Class      string
	Fit        float64
	Pass       bool
	// NotDecided says the counter was read from a projected store (see [Pins]) and is a
	// block counter: it depends on the depth of the index, which a projection has less
	// of than a full store, and the two differ in either direction (a projection reads
	// fewer index blocks, and the offset moves the slope as well as the level), so
	// neither a pass nor a fail on it is shown. The cell is reported and does not
	// count towards the candidate's verdict.
	NotDecided bool
}

// blockCounters are the counters that depend on the depth of the index, which a
// projected store has less of than a full one.
var blockCounters = map[string]bool{"block_loads": true, "cold_block_bytes": true}

// G1 is the verdict of G1 for every candidate in every window, cell by cell. A
// candidate passes if every cell of it passes.
func G1(windows []RetainedWindow, r Rules) ([]G1Cell, error) {
	if len(windows) != len(r.Windows) {
		return nil, fmt.Errorf("runner: G1 is judged over %d windows (%v) and there are %d", len(r.Windows), r.Windows, len(windows))
	}
	ws := slices.Clone(windows)
	sort.Slice(ws, func(i, j int) bool { return ws[i].Days < ws[j].Days })
	for i, w := range ws {
		if w.Days != r.Windows[i] {
			return nil, fmt.Errorf("runner: the windows are %v, G1 needs %v", daysOf(ws), r.Windows)
		}
	}
	if len(ws) < 3 {
		return nil, fmt.Errorf("runner: G1 needs at least three windows")
	}
	target := ws[len(ws)-1]
	if target.Days != r.TargetWindow {
		return nil, fmt.Errorf("runner: the last window is %d days, the rules' target is %d", target.Days, r.TargetWindow)
	}

	instants := map[string]bool{}
	for _, a := range r.G1Instants {
		instants[a] = true
	}
	names := map[string]bool{}
	for _, w := range ws {
		for _, c := range w.Candidates {
			names[c.Results.Candidate] = true
		}
	}
	var out []G1Cell
	for _, name := range sortedKeys(names) {
		cands := make([]*Candidate, len(ws))
		for i, w := range ws {
			for _, c := range w.Candidates {
				if c.Results.Candidate == name {
					cands[i] = c
				}
			}
			if cands[i] == nil {
				return nil, fmt.Errorf("runner: %s has no results in the window of %d days", name, w.Days)
			}
		}
		// The queries of every window by name: pinned, so each is in each.
		index := make([]map[string]int, len(ws))
		for i, w := range ws {
			index[i] = map[string]int{}
			for qi, q := range w.Plan.Queries {
				index[i][queryID(q)] = qi
			}
		}
		blockBytes, err := strconv.ParseInt(cands[len(ws)-1].Results.Describe["block_bytes"], 10, 64)
		if err != nil || blockBytes <= 0 {
			return nil, fmt.Errorf("runner: %s does not say its block size (block_bytes %q), which the budget of a read in blocks needs", name, cands[len(ws)-1].Results.Describe["block_bytes"])
		}

		type cellKey struct{ group, age, counter string }
		groups := map[cellKey][]G1Cell{} // one entry per query
		var order []cellKey
		for _, q := range target.Plan.Queries {
			if !instants[q.Age] {
				continue
			}
			for _, counter := range r.G1Counters {
				var work []float64
				for i, w := range ws {
					wi, ok := index[i][queryID(q)]
					if !ok {
						return nil, fmt.Errorf("runner: %s is asked in the window of %d days and not in the one of %d: the plans are not of the same pins", q.Name(), target.Days, w.Days)
					}
					work = append(work, float64(counterValue(cands[i].Results.Queries[wi], w.Plan.Queries[wi], counter)))
				}
				c, err := queryCell(name, q, counter, work, ws, r, blockBytes)
				if err != nil {
					return nil, err
				}
				c.NotDecided = target.Plan.Spec.Pins != nil && blockCounters[counter]
				k := cellKey{q.Group, q.Age, counter}
				if _, ok := groups[k]; !ok {
					order = append(order, k)
				}
				groups[k] = append(groups[k], c)
			}
		}
		for _, k := range order {
			out = append(out, populationCell(groups[k]))
		}
	}
	return out, nil
}

// queryID names a query in every window.
func queryID(q Query) string { return fmt.Sprintf("%s|%d|%s|%s", q.Group, q.Rank, q.Age, q.Op) }

func daysOf(ws []RetainedWindow) []int {
	var out []int
	for _, w := range ws {
		out = append(out, w.Days)
	}
	return out
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// queryCell is the verdict of G1 on one query, from its work in each window.
func queryCell(candidate string, q Query, counter string, work []float64, ws []RetainedWindow, r Rules, blockBytes int64) (G1Cell, error) {
	n := len(ws)
	budget := r.G1Budget[counter].For(len(q.Fps), q.Size, blockBytes)
	if budget <= 0 { // a read that nothing is allowed to cost would pass for want of a ratio
		return G1Cell{}, fmt.Errorf("runner: %s has no budget in the rules for the counter %s", q.Name(), counter)
	}
	c := G1Cell{Candidate: candidate, Group: q.Group, Age: q.Age, Counter: counter, Worst: q.Name(), Value: work[n-1], Budget: budget}
	c.Ratio = c.Value / budget
	slope := func(i, j int) float64 {
		a, b := math.Max(work[i], 1), math.Max(work[j], 1)
		return math.Log(b/a) / math.Log(float64(ws[j].Days)/float64(ws[i].Days))
	}
	if work[n-1] > float64(r.CounterFloors[counter]) {
		c.Growth = math.Max(slope(n-2, n-1), 0)
	}
	c.Projected = c.Ratio * math.Pow(r.G1Headroom, c.Growth)
	if work[2] > float64(r.CounterFloors[counter]) || work[0] > float64(r.CounterFloors[counter]) {
		c.SlopeFirst = slope(0, 2)
	}
	switch {
	case c.SlopeFirst <= r.BoundedSlope:
		c.Class = "bounded"
	case c.SlopeFirst >= r.LinearSlope:
		c.Class = "linear"
	default:
		c.Class = "between"
	}
	c.Fit = logLogFit(work, ws)
	c.Pass = c.Projected <= 1
	return c, nil
}

// logLogFit is the slope of the least-squares line of ln work against ln days,
// over the windows in which the work is above zero, or 0 if fewer than two are.
func logLogFit(work []float64, ws []RetainedWindow) float64 {
	var xs, ys []float64
	for i, w := range work {
		if w > 0 {
			xs, ys = append(xs, math.Log(float64(ws[i].Days))), append(ys, math.Log(w))
		}
	}
	if len(xs) < 2 {
		return 0
	}
	var mx, my float64
	for i := range xs {
		mx, my = mx+xs[i], my+ys[i]
	}
	mx, my = mx/float64(len(xs)), my/float64(len(xs))
	var num, den float64
	for i := range xs {
		num += (xs[i] - mx) * (ys[i] - my)
		den += (xs[i] - mx) * (xs[i] - mx)
	}
	if den == 0 {
		return 0
	}
	return num / den
}

// populationCell is the verdict on a population from the verdicts of its queries:
// the largest projected ratio for a population of the busiest prefixes, the median
// for the one around the middle.
func populationCell(qs []G1Cell) G1Cell {
	sorted := slices.Clone(qs)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].Projected < sorted[j].Projected })
	pick, stat := sorted[len(sorted)-1], "largest"
	if strings.Contains(qs[0].Group, " median ") {
		pick, stat = sorted[(len(sorted)-1)/2], "median"
	}
	pick.Statistic, pick.Queries = stat, len(qs)
	return pick
}

// G1Passes is whether every cell of the candidate that is decided passes, and there is
// one that is.
func G1Passes(cells []G1Cell, candidate string) bool {
	decided := false
	for _, c := range cells {
		if c.Candidate == candidate && !c.NotDecided {
			decided = true
			if !c.Pass {
				return false
			}
		}
	}
	return decided
}

// WriteG1 prints the verdicts: for each candidate whether it passes G1, and its
// cells, the largest projected ratios first (so the failing ones), with a line
// saying what each number is. A limit above zero prints that many cells of each.
func WriteG1(w io.Writer, cells []G1Cell, r Rules, limit int) {
	rulesDigest, _ := r.Digest()
	fmt.Fprintf(w, "G1 (rules %.12s): a read must fit its budget at %d days of retained history, and still fit if that history were multiplied by %.0f.\n", rulesDigest, r.TargetWindow, r.G1Headroom)
	fmt.Fprintln(w, "ratio = work / budget at the target window; growth = slope over the last two windows (0 at or below the counter's floor); projected = ratio x headroom^growth, which must be at most 1;")
	fmt.Fprintln(w, "the slope between the first window and the third, the class it gives (bounded, between, linear) and the fit over every window are diagnostics, and do not decide.")
	fmt.Fprintln(w, "block counters of a projected store depend on the depth of its index, which differs from a full store's in level and in slope: such a cell (?) is reported and not decided, whether it is over or under.")
	names := map[string]bool{}
	for _, c := range cells {
		names[c.Candidate] = true
	}
	for _, name := range sortedKeys(names) {
		var mine []G1Cell
		failing, undecided := 0, 0
		for _, c := range cells {
			if c.Candidate == name {
				mine = append(mine, c)
				if c.NotDecided {
					undecided++
				} else if !c.Pass {
					failing++
				}
			}
		}
		verdict := "PASSES G1"
		if failing > 0 {
			verdict = fmt.Sprintf("FAILS G1 in %d of %d decided cells", failing, len(mine)-undecided)
		}
		if undecided > 0 {
			verdict += fmt.Sprintf(" (%d cells on block counters of a projected store are not decided, marked ?)", undecided)
		}
		fmt.Fprintf(w, "\n%s: %s\n", name, verdict)
		sort.SliceStable(mine, func(i, j int) bool { return mine[i].Projected > mine[j].Projected })
		tw := tabwriter.NewWriter(w, 2, 0, 2, ' ', tabwriter.AlignRight)
		fmt.Fprintln(tw, "cell\tcounter\tstat\tratio\tgrowth\tprojected\tslope w1-w3\tclass\tfit\t")
		for i, c := range mine {
			if limit > 0 && i == limit {
				fmt.Fprintf(tw, "(%d more cells)\t\t\t\t\t\t\t\t\t\n", len(mine)-limit)
				break
			}
			mark := ""
			switch {
			case c.NotDecided:
				mark = " ?"
			case !c.Pass:
				mark = " FAIL"
			}
			fmt.Fprintf(tw, "%s %s\t%s\t%s\t%.2f\t%.2f\t%.2f%s\t%.2f\t%s\t%.2f\t\n", c.Group, c.Age, shortCounter(c.Counter), c.Statistic, c.Ratio, c.Growth, c.Projected, mark, c.SlopeFirst, c.Class, c.Fit)
		}
		_ = tw.Flush()
	}
}

func shortCounter(c string) string {
	if strings.HasPrefix(c, "internal_steps") {
		return "steps+decoded"
	}
	return c
}
