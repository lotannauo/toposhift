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
// G1: the queries it is the statistic of, the one that is the statistic at the
// target window, and what the rule of G1 makes of the population.
type G1Cell struct {
	Candidate, Group, Age, Counter string
	// Statistic says what the cell is the verdict of: "largest" for a population of
	// the busiest prefixes, "median" for the one around the middle.
	Statistic string
	Queries   int
	// Worst is the query that is the population's statistic at the target window.
	Worst string
	// Value is its work at the target window and Budget what the rules allow it;
	// Ratio is their quotient, the population's statistic at the target window.
	Value, Budget, Ratio float64
	// Fit is the slope of a least-squares line through the logarithm of the
	// population's statistic of the ratio in every window against that of the window
	// (each query's work counted as at least 1), and Growth is Fit where it is above
	// zero and the work of Worst at the target window is above the counter's floor,
	// and 0 otherwise. Projected is Ratio times G1Headroom to the Growth: the ratio if
	// the retention were multiplied by G1Headroom.
	Fit, Growth, Projected float64
	// SlopeLast is the slope of the statistic between the last two windows, and
	// SlopeFirst between the first window and the third, the diagnostic of bounded and
	// linear growth, Class what the rules call it. Each is 0 when the work of the
	// statistic's query is at or below the counter's floor in both of its windows.
	SlopeLast, SlopeFirst float64
	Class                 string
	// QueryFit is the largest fit of one query's own work over the windows in which it
	// is above zero, among the queries whose work at the target window is above the
	// counter's floor, and QueryFitOf names that query; LinearQuery says it is at or
	// above LinearSlope. A diagnostic: it does not decide.
	QueryFit    float64
	QueryFitOf  string
	LinearQuery bool
	Pass        bool
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

// g1Query is one query's work and budget in each window, in order of days.
type g1Query struct {
	name         string
	work, budget []float64
}

// G1 is the verdict of G1 for every candidate in every window, population by
// population. A candidate passes if every cell of it passes. An instant of the rules
// that the plans do not ask has no cell: see [G1Missing].
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
	// The queries of every window by name: pinned, so each is in each.
	index := make([]map[string]int, len(ws))
	for i, w := range ws {
		index[i] = map[string]int{}
		for qi, q := range w.Plan.Queries {
			index[i][queryID(q)] = qi
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
		blockBytes, err := strconv.ParseInt(cands[len(ws)-1].Results.Describe["block_bytes"], 10, 64)
		if err != nil || blockBytes <= 0 {
			return nil, fmt.Errorf("runner: %s does not say its block size (block_bytes %q), which the budget of a read in blocks needs", name, cands[len(ws)-1].Results.Describe["block_bytes"])
		}

		type cellKey struct{ group, age, counter string }
		groups := map[cellKey][]g1Query{}
		var order []cellKey
		for _, q := range target.Plan.Queries {
			if !instants[q.Age] {
				continue
			}
			for _, counter := range r.G1Counters {
				gq := g1Query{name: q.Name()}
				for i, w := range ws {
					wi, ok := index[i][queryID(q)]
					if !ok {
						return nil, fmt.Errorf("runner: %s is asked in the window of %d days and not in the one of %d: the plans are not of the same pins", q.Name(), target.Days, w.Days)
					}
					wq := w.Plan.Queries[wi]
					budget := r.G1Budget[counter].For(len(wq.Fps), wq.Size, blockBytes)
					if budget <= 0 { // a read that nothing is allowed to cost would pass for want of a ratio
						return nil, fmt.Errorf("runner: %s has no budget in the rules for the counter %s", q.Name(), counter)
					}
					gq.work = append(gq.work, float64(counterValue(cands[i].Results.Queries[wi], wq, counter)))
					gq.budget = append(gq.budget, budget)
				}
				k := cellKey{q.Group, q.Age, counter}
				if _, ok := groups[k]; !ok {
					order = append(order, k)
				}
				groups[k] = append(groups[k], gq)
			}
		}
		for _, k := range order {
			c := populationCell(groups[k], ws, r, k.counter, strings.Contains(k.group, " median "))
			c.Candidate, c.Group, c.Age, c.Counter = name, k.group, k.age, k.counter
			c.NotDecided = target.Plan.Spec.Projected() && blockCounters[k.counter]
			out = append(out, c)
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

// statistic is the index of the query that is the population's statistic in window
// i, by the ratio of its work (counted as at least 1) to its budget: the largest, or
// the lower median; among queries of equal ratio, the one with more work, then the
// later in the plan.
func statistic(qs []g1Query, i int, median bool) int {
	idx := make([]int, len(qs))
	for j := range idx {
		idx[j] = j
	}
	ratio := func(j int) float64 { return math.Max(qs[j].work[i], 1) / qs[j].budget[i] }
	sort.SliceStable(idx, func(a, b int) bool {
		ra, rb := ratio(idx[a]), ratio(idx[b])
		if ra != rb {
			return ra < rb
		}
		return qs[idx[a]].work[i] < qs[idx[b]].work[i]
	})
	if median {
		return idx[(len(idx)-1)/2]
	}
	return idx[len(idx)-1]
}

// populationCell is the verdict of G1 on one population of queries, from their work
// and budgets in every window (see [Rules]).
func populationCell(qs []g1Query, ws []RetainedWindow, r Rules, counter string, median bool) G1Cell {
	n := len(ws)
	floor := float64(r.CounterFloors[counter])
	c := G1Cell{Statistic: "largest", Queries: len(qs)}
	if median {
		c.Statistic = "median"
	}
	picks := make([]int, n)
	stat := make([]float64, n) // S in each window, with work counted as at least 1
	xs, ys := make([]float64, n), make([]float64, n)
	for i := range ws {
		picks[i] = statistic(qs, i, median)
		q := qs[picks[i]]
		stat[i] = math.Max(q.work[i], 1) / q.budget[i]
		xs[i], ys[i] = math.Log(float64(ws[i].Days)), math.Log(stat[i])
	}
	worst := qs[picks[n-1]]
	c.Worst, c.Value, c.Budget = worst.name, worst.work[n-1], worst.budget[n-1]
	c.Ratio = c.Value / c.Budget
	c.Fit = leastSquaresSlope(xs, ys)
	if worst.work[n-1] > floor {
		c.Growth = math.Max(c.Fit, 0)
	}
	c.Projected = c.Ratio * math.Pow(r.G1Headroom, c.Growth)
	c.Pass = c.Projected <= 1
	slope := func(i, j int) float64 {
		if qs[picks[i]].work[i] <= floor && qs[picks[j]].work[j] <= floor {
			return 0
		}
		return math.Log(stat[j]/stat[i]) / math.Log(float64(ws[j].Days)/float64(ws[i].Days))
	}
	c.SlopeLast, c.SlopeFirst = slope(n-2, n-1), slope(0, 2)
	switch {
	case c.SlopeFirst <= r.BoundedSlope:
		c.Class = "bounded"
	case c.SlopeFirst >= r.LinearSlope:
		c.Class = "linear"
	default:
		c.Class = "between"
	}
	for _, q := range qs {
		if q.work[n-1] <= floor {
			continue
		}
		if f := logLogFit(q.work, ws); c.QueryFitOf == "" || f > c.QueryFit {
			c.QueryFit, c.QueryFitOf = f, q.name
		}
	}
	c.LinearQuery = c.QueryFitOf != "" && c.QueryFit >= r.LinearSlope
	return c
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
	return leastSquaresSlope(xs, ys)
}

// leastSquaresSlope is the slope of the least-squares line of ys against xs, or 0 if
// there are fewer than two points or the xs are all equal.
func leastSquaresSlope(xs, ys []float64) float64 {
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

// G1Passes is whether every cell of the candidate that is decided passes, and there is
// one that is. It does not see an instant that has no cell: see [G1Missing].
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

// G1Missing lists the instants of the rules at which the candidate has no cell: the
// plans did not ask them (a plan made before they were added). A candidate that
// passes every cell it has is not shown to pass G1 while one is missing.
func G1Missing(cells []G1Cell, r Rules, candidate string) []string {
	have := map[string]bool{}
	for _, c := range cells {
		if c.Candidate == candidate {
			have[c.Age] = true
		}
	}
	var out []string
	for _, a := range r.G1Instants {
		if !have[a] {
			out = append(out, a)
		}
	}
	return out
}

// G1Verdict is one candidate's verdict on G1: it passes if every decided cell passes,
// there is one, and no instant of the rules is missing from its plans.
type G1Verdict struct {
	Candidate                    string
	Pass                         bool
	Failing, Decided, NotDecided int
	Missing                      []string
}

// G1Verdicts are the verdicts of every candidate that has a cell, by name.
func G1Verdicts(cells []G1Cell, r Rules) []G1Verdict {
	names := map[string]bool{}
	for _, c := range cells {
		names[c.Candidate] = true
	}
	var out []G1Verdict
	for _, name := range sortedKeys(names) {
		v := G1Verdict{Candidate: name, Missing: G1Missing(cells, r, name)}
		if v.Missing == nil {
			v.Missing = []string{}
		}
		for _, c := range cells {
			switch {
			case c.Candidate != name:
			case c.NotDecided:
				v.NotDecided++
			default:
				v.Decided++
				if !c.Pass {
					v.Failing++
				}
			}
		}
		v.Pass = v.Failing == 0 && v.Decided > 0 && len(v.Missing) == 0
		out = append(out, v)
	}
	return out
}

// G1Report is what the g1 step writes for tools to read (g1.json): the rules, the
// reasons the windows cannot be judged together if there are any, the verdicts and
// every cell.
type G1Report struct {
	RulesDigest  string
	RulesVersion int
	Windows      []int
	// Judgeable is false when the windows cannot be judged together (Problems says why):
	// no verdict of such a report is a pass.
	Judgeable bool
	Problems  []string
	Verdicts  []G1Verdict
	Cells     []G1Cell
}

// NewG1Report gathers a report of the cells under the rules.
func NewG1Report(cells []G1Cell, r Rules, problems []string) G1Report {
	d, _ := r.Digest()
	if problems == nil {
		problems = []string{}
	}
	vs := G1Verdicts(cells, r)
	if len(problems) > 0 {
		for i := range vs {
			vs[i].Pass = false
		}
	}
	return G1Report{RulesDigest: d, RulesVersion: r.Version, Windows: r.Windows, Judgeable: len(problems) == 0, Problems: problems, Verdicts: vs, Cells: cells}
}

// WriteG1 prints the verdicts: for each candidate whether it passes G1, and its
// cells, the largest projected ratios first (so the failing ones), with a line
// saying what each number is. A limit above zero prints that many cells of each.
func WriteG1(w io.Writer, cells []G1Cell, r Rules, limit int) {
	rulesDigest, _ := r.Digest()
	fmt.Fprintf(w, "G1 (rules %.12s, version %d): a read must fit its budget at %d days of retained history, and still fit if that history were multiplied by %.0f.\n", rulesDigest, r.Version, r.TargetWindow, r.G1Headroom)
	fmt.Fprintln(w, "ratio = work / budget of the population's statistic (its largest, or median) at the target window; fit = least-squares slope of ln(statistic of the ratio) on ln(days) over every window;")
	fmt.Fprintln(w, "projected = ratio x headroom^max(fit, 0), with the fit counted as 0 when the statistic's work at the target is at or below the counter's floor; it must be at most 1.")
	fmt.Fprintln(w, "slope w3-w4 and w1-w3 (the statistic's slope between those windows), the class, and the largest fit of one query's own work (! at or above the linear slope) are diagnostics, and do not decide.")
	fmt.Fprintln(w, "block counters of a projected store depend on the depth of its index, which differs from a full store's in level and in slope: such a cell (?) is reported and not decided, whether it is over or under.")
	for _, v := range G1Verdicts(cells, r) {
		verdict := "PASSES G1"
		switch {
		case v.Failing > 0:
			verdict = fmt.Sprintf("FAILS G1 in %d of %d decided cells", v.Failing, v.Decided)
		case len(v.Missing) > 0:
			verdict = "PASSES ONLY THE INSTANTS ITS PLANS ASK: G1 is not shown"
		case v.Decided == 0:
			verdict = "HAS NO DECIDED CELL: G1 is not shown"
		}
		if len(v.Missing) > 0 {
			verdict += fmt.Sprintf(" (its plans do not ask %s)", strings.Join(v.Missing, ", "))
		}
		if v.NotDecided > 0 {
			verdict += fmt.Sprintf(" (%d cells on block counters of a projected store are not decided, marked ?)", v.NotDecided)
		}
		fmt.Fprintf(w, "\n%s: %s\n", v.Candidate, verdict)
		var mine []G1Cell
		for _, c := range cells {
			if c.Candidate == v.Candidate {
				mine = append(mine, c)
			}
		}
		sort.SliceStable(mine, func(i, j int) bool { return mine[i].Projected > mine[j].Projected })
		tw := tabwriter.NewWriter(w, 2, 0, 2, ' ', tabwriter.AlignRight)
		fmt.Fprintln(tw, "cell\tcounter\tstat\tratio\tfit\tprojected\tslope w3-w4\tslope w1-w3\tclass\tquery fit\t")
		for i, c := range mine {
			if limit > 0 && i == limit {
				fmt.Fprintf(tw, "(%d more cells)\t\t\t\t\t\t\t\t\t\t\n", len(mine)-limit)
				break
			}
			mark := ""
			switch {
			case c.NotDecided:
				mark = " ?"
			case !c.Pass:
				mark = " FAIL"
			}
			qf := "-"
			if c.QueryFitOf != "" {
				qf = fmt.Sprintf("%.2f %s", c.QueryFit, rankOf(c.QueryFitOf))
				if c.LinearQuery {
					qf += " !"
				}
			}
			fmt.Fprintf(tw, "%s %s\t%s\t%s\t%.2f\t%.2f\t%.2f%s\t%.2f\t%.2f\t%s\t%s\t\n", c.Group, c.Age, shortCounter(c.Counter), c.Statistic, c.Ratio, c.Fit, c.Projected, mark, c.SlopeLast, c.SlopeFirst, c.Class, qf)
		}
		_ = tw.Flush()
	}
}

// rankOf is the "#rank" of a query's name ("group #rank age").
func rankOf(name string) string {
	f := strings.Fields(name[strings.LastIndex(name, " #")+1:])
	if len(f) == 0 {
		return name
	}
	return f[0]
}

func shortCounter(c string) string {
	if strings.HasPrefix(c, "internal_steps") {
		return "steps+decoded"
	}
	return c
}
