package blastscore

import (
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"slices"
	"sort"
	"strings"

	"github.com/lotannauo/toposhift/internal/query/blastradius"
	"github.com/lotannauo/toposhift/internal/query/coresidency"
	"github.com/lotannauo/toposhift/internal/query/extract"
	"github.com/lotannauo/toposhift/internal/query/interval"
)

// SchemaVersion is the version of the results JSON contract. Version 2 added
// each case's trace coverage (trace_covers_window, trace_overlap_minutes) to its
// diagnostics.
const SchemaVersion = 2

// Meta names what was scored. Both are labels such as a dataset revision and a
// loader version; they are validated to hold no more than that.
type Meta struct {
	Dataset string `json:"dataset"`
	Loader  string `json:"loader"`
}

// labelPattern is what a Meta label may hold.
var labelPattern = regexp.MustCompile(`^[A-Za-z0-9._@+-]{1,64}$`)

// Versions are the policy versions of the queries the scores were made with. A
// change to any of them makes a recorded score stale.
type Versions struct {
	Blastradius int `json:"blastradius"`
	Coresidency int `json:"coresidency"`
	Extract     int `json:"extract"`
	Interval    int `json:"interval"`
}

// CurrentVersions are the versions of the queries this build links.
func CurrentVersions() Versions {
	return Versions{
		Blastradius: blastradius.PolicyVersion, Coresidency: coresidency.PolicyVersion,
		Extract: extract.PolicyVersion, Interval: interval.PolicyVersion,
	}
}

// Rule is the acceptance rule the report was judged by.
type Rule struct {
	Date                 string  `json:"date"`
	GoodRecall           float64 `json:"good_recall"`
	GoodRecallLower      float64 `json:"good_recall_lower"`
	GoodLift             float64 `json:"good_lift"`
	GoodLiftLower        float64 `json:"good_lift_lower"`
	NonInferiorityMargin float64 `json:"non_inferiority_margin"`
	PoorRecall           float64 `json:"poor_recall"`
	PoorLift             float64 `json:"poor_lift"`
	MinCases             int     `json:"min_cases"`
	Resamples            int     `json:"resamples"`
	BootstrapSeed        uint64  `json:"bootstrap_seed"`
}

func currentRule() Rule {
	return Rule{
		Date: AcceptanceDate, GoodRecall: GoodRecall, GoodRecallLower: GoodRecallLower,
		GoodLift: GoodLift, GoodLiftLower: GoodLiftLower, NonInferiorityMargin: NonInferiorityMargin,
		PoorRecall: PoorRecall, PoorLift: PoorLift, MinCases: MinCases,
		Resamples: Resamples, BootstrapSeed: BootstrapSeed,
	}
}

// DatasetCheck is the load check over the dataset: the share of its cases that
// failed to load, with uninstrumented seeds taken out first. A case fails to
// load only if its seed is absent in the window or the facts hold no depends_on
// piece in it. The last three counts are reported and have no effect on Pass.
type DatasetCheck struct {
	Cases                      int `json:"cases"`
	ExcludedUninstrumentedSeed int `json:"excluded_uninstrumented_seed"`
	Counted                    int `json:"counted"`
	LoadFailures               int `json:"load_failures"`
	QuarantinedSeeds           int `json:"quarantined_seeds"`
	UnmappableRoots            int `json:"unmappable_roots"`
	NoWindow                   int `json:"no_window"`
	UnreadableLabels           int `json:"unreadable_labels"`
	MissingObjects             int `json:"missing_objects"`
	// Pass is false when more than a quarter of the counted cases failed to load.
	Pass bool `json:"pass"`
}

// Difference is F1(predictor) - F1(baseline) over a stratum's cases, with its
// interval from the resamples the two share. Resolved says the interval leaves
// out zero; a difference that is not resolved is not a ranking.
type Difference struct {
	F1       Num      `json:"f1_difference"`
	Interval Interval `json:"interval"`
	Resolved bool     `json:"resolved"`
}

// Ceiling is what the union-graph comparator reaches on a stratum's cases. It
// is reported and not judged: if it is below the Good recall, Good is out of
// reach because of direction or data, not because of interval semantics.
type Ceiling struct {
	Recall    Num  `json:"recall"`
	BelowGood bool `json:"below_good_recall"`
}

// Stratum is one level, class and predictor over the cases that fall in it.
type Stratum struct {
	Level     Level     `json:"level"`
	Class     Class     `json:"class"`
	Predictor Predictor `json:"predictor"`
	// Headline marks the one stratum the primary judgement is on: W at service
	// level over call-graph cases.
	Headline bool `json:"headline"`
	// Cases lists the cases scored, by id. Scorable counts those with a truth, and
	// SingleNodeSkips those a co-residency predictor did not score because the
	// cluster has one node.
	Cases           []string `json:"cases"`
	Scorable        int      `json:"scorable"`
	SingleNodeSkips int      `json:"single_node_skips"`
	// Unresolved is the truth that could not be predicted at all (unmappable,
	// unplaced or uninstrumented), out of Truth.
	Unresolved int `json:"unresolved_truth"`
	Truth      int `json:"truth"`
	// All is the pooled scores the rule judges. NoTruncated leaves out the cases a
	// cap cut, and NoUninstrumented the uninstrumented truth. Neither is judged.
	All               Pooled   `json:"all"`
	NoTruncated       Pooled   `json:"no_truncated"`
	NoUninstrumented  Pooled   `json:"no_uninstrumented"`
	PrecisionInterval Interval `json:"precision_interval"`
	RecallInterval    Interval `json:"recall_interval"`
	F1Interval        Interval `json:"f1_interval"`
	// Lift is the micro precision over B_all's, with its interval.
	Lift         Num      `json:"lift"`
	LiftInterval Interval `json:"lift_interval"`
	// VsOneHop and VsAll are the paired differences from the baselines.
	VsOneHop *Difference `json:"vs_b_1hop,omitempty"`
	VsAll    *Difference `json:"vs_b_all,omitempty"`
	Ceiling  *Ceiling    `json:"ceiling,omitempty"`
	// Verdict is set for the predictors the rule judges, with the reasons that
	// bear on it.
	Verdict Verdict  `json:"verdict,omitempty"`
	Reasons []Reason `json:"reasons,omitempty"`
}

// Report is the scored dataset. It is the results contract: it holds pooled
// numbers, and numbers keyed by case id, and nothing else. Its JSON carries no
// entity, pod or node name, no uid, no label text and no timestamp.
type Report struct {
	Schema   int                `json:"schema"`
	Meta     Meta               `json:"meta"`
	Versions Versions           `json:"versions"`
	Rule     Rule               `json:"rule"`
	Dataset  DatasetCheck       `json:"dataset_check"`
	Strata   []Stratum          `json:"strata"`
	Cases    map[string]CaseRun `json:"cases"`
}

// Evaluate pools the runs of every case into strata and judges them. Cases are
// keyed by id, so two runs of one id are an error.
func Evaluate(runs []CaseRun, meta Meta) (Report, error) {
	for _, l := range []string{meta.Dataset, meta.Loader} {
		if !labelPattern.MatchString(l) {
			return Report{}, fmt.Errorf("blastscore: the label %q is not a plain label", l)
		}
	}
	rep := Report{
		Schema: SchemaVersion, Meta: meta, Versions: CurrentVersions(), Rule: currentRule(),
		Cases: map[string]CaseRun{},
	}
	for _, r := range runs {
		if !CaseIDPattern.MatchString(r.ID) {
			return Report{}, fmt.Errorf("blastscore: case id %q does not match %s", r.ID, CaseIDPattern)
		}
		if _, dup := rep.Cases[r.ID]; dup {
			return Report{}, fmt.Errorf("blastscore: case %s appears twice", r.ID)
		}
		rep.Cases[r.ID] = r
	}
	rep.Dataset = datasetCheck(runs)

	ids := make([]string, 0, len(rep.Cases))
	for id := range rep.Cases {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return caseLess(ids[i], ids[j]) })

	type key struct {
		l Level
		c Class
		p Predictor
	}
	byKey := map[key][]string{}
	for _, id := range ids {
		r := rep.Cases[id]
		for _, s := range r.Scores {
			k := key{s.Level, r.Class, s.Predictor}
			byKey[k] = append(byKey[k], id)
		}
		for _, s := range r.Skips {
			if s.Reason == ReasonSingleNodeCase {
				k := key{s.Level, r.Class, s.Predictor}
				if _, ok := byKey[k]; !ok {
					byKey[k] = nil
				}
			}
		}
	}
	keys := make([]key, 0, len(byKey))
	for k := range byKey {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, b := keys[i], keys[j]
		if a.l != b.l {
			return a.l == LevelService
		}
		if a.c != b.c {
			return a.c == ClassCallGraph
		}
		return predictorOrder(a.p) < predictorOrder(b.p)
	})
	for _, k := range keys {
		rep.Strata = append(rep.Strata, rep.stratum(k.l, k.c, k.p, byKey[k]))
	}
	return rep, nil
}

func predictorOrder(p Predictor) int {
	i := slices.Index([]Predictor{PredictorW, PredictorWPod, PredictorC, PredictorCSvc, PredictorWC, PredictorBAll, PredictorB1Hop, PredictorBUnion}, p)
	if i < 0 {
		return math.MaxInt
	}
	return i
}

// caseLess orders case ids by their number.
func caseLess(a, b string) bool {
	var x, y int
	_, _ = fmt.Sscanf(a, "Scenario-%d", &x)
	_, _ = fmt.Sscanf(b, "Scenario-%d", &y)
	return x < y
}

func datasetCheck(runs []CaseRun) DatasetCheck {
	d := DatasetCheck{Cases: len(runs)}
	for _, r := range runs {
		if r.Outcome == OutcomeExcluded && r.Exclusion == ExcludedUninstrumentedSeed {
			d.ExcludedUninstrumentedSeed++
			continue
		}
		d.Counted++
		if r.LoadFailed {
			d.LoadFailures++
		}
		switch {
		case r.Reason == ReasonQuarantinedSeed:
			d.QuarantinedSeeds++
		case r.Exclusion == ExcludedUnmappableRoot:
			d.UnmappableRoots++
		case r.Exclusion == ExcludedNoWindow:
			d.NoWindow++
		case r.Exclusion == ExcludedUnreadableLabels:
			d.UnreadableLabels++
		case r.Exclusion == ExcludedMissingObjects:
			d.MissingObjects++
		}
	}
	d.Pass = d.Counted > 0 && d.LoadFailures*LoadFailureDen <= d.Counted*LoadFailureNum
	return d
}

// scoreOf finds a case's score for a level and predictor.
func (r CaseRun) scoreOf(l Level, p Predictor) (CaseScore, bool) {
	for _, s := range r.Scores {
		if s.Level == l && s.Predictor == p {
			return s, true
		}
	}
	return CaseScore{}, false
}

// stratum pools and judges one stratum over its cases.
func (rep *Report) stratum(l Level, c Class, p Predictor, ids []string) Stratum {
	st := Stratum{Level: l, Class: c, Predictor: p, Headline: l == LevelService && c == ClassCallGraph && p == PredictorW, Cases: ids}
	if st.Cases == nil {
		st.Cases = []string{}
	}
	var xs, bAll, bOne, bUnion []CaseScore
	haveOne := true
	for _, id := range ids {
		r := rep.Cases[id]
		x, _ := r.scoreOf(l, p)
		xs = append(xs, x)
		a, _ := r.scoreOf(l, PredictorBAll)
		bAll = append(bAll, a)
		o, ok := r.scoreOf(l, PredictorB1Hop)
		haveOne = haveOne && ok
		bOne = append(bOne, o)
		u, _ := r.scoreOf(l, PredictorBUnion)
		bUnion = append(bUnion, u)
		st.Truth += x.T
		st.Unresolved += x.Unmappable + x.Unplaced + x.Uninstrumented
		if x.T > 0 {
			st.Scorable++
		}
	}
	for _, id := range rep.sortedIDs() {
		r := rep.Cases[id]
		if r.Class != c {
			continue
		}
		for _, s := range r.Skips {
			if s.Level == l && s.Predictor == p && s.Reason == ReasonSingleNodeCase && s.T > 0 {
				st.SingleNodeSkips++
			}
		}
	}
	st.All, st.NoTruncated, st.NoUninstrumented = pool(xs, VariantAll), pool(xs, VariantNoTruncated), pool(xs, VariantNoUninstrumented)
	pa := pool(bAll, VariantAll)
	st.Lift = Num(lift(st.All.Hits, st.All.N, pa.Hits, pa.N))

	if len(xs) > 0 {
		hasOne := haveOne && p != PredictorB1Hop && p != PredictorBAll && p != PredictorBUnion
		hasAll := p != PredictorBAll
		sum := func(idx []int, ss []CaseScore) (h, n, t int) {
			for _, i := range idx {
				h, n, t = h+ss[i].Hits, n+ss[i].N, t+ss[i].T
			}
			return
		}
		ivs := bootstrap(len(xs), Resamples, BootstrapSeed, func(idx []int) []float64 {
			h, n, t := sum(idx, xs)
			ah, an, _ := sum(idx, bAll)
			oneH, oneN, oneT := sum(idx, bOne)
			fx := f1(h, n, t)
			v := []float64{ratio(h, n), ratio(h, t), fx, lift(h, n, ah, an), math.NaN(), math.NaN()}
			if hasOne {
				v[4] = fx - f1(oneH, oneN, oneT)
			}
			if hasAll {
				v[5] = fx - f1(ah, an, t)
			}
			return v
		})
		st.PrecisionInterval, st.RecallInterval, st.F1Interval, st.LiftInterval = ivs[0], ivs[1], ivs[2], ivs[3]
		if hasOne {
			one := pool(bOne, VariantAll)
			st.VsOneHop = difference(Num(f1(st.All.Hits, st.All.N, st.All.T)-f1(one.Hits, one.N, one.T)), ivs[4])
		}
		if hasAll {
			st.VsAll = difference(Num(f1(st.All.Hits, st.All.N, st.All.T)-f1(pa.Hits, pa.N, pa.T)), ivs[5])
		}
	}
	if l == LevelService && p.judged() {
		var uh int
		for _, u := range bUnion {
			uh += u.Hits
		}
		r := ratio(uh, st.All.T)
		st.Ceiling = &Ceiling{Recall: Num(r), BelowGood: r < GoodRecall}
	}
	if p.judged() {
		st.Verdict, st.Reasons = rep.judge(&st)
	}
	return st
}

func difference(point Num, iv Interval) *Difference {
	d := &Difference{F1: point, Interval: iv}
	d.Resolved = finite(float64(iv.Lower)) && finite(float64(iv.Upper)) && (float64(iv.Lower) > 0 || float64(iv.Upper) < 0)
	return d
}

func (rep *Report) sortedIDs() []string {
	ids := make([]string, 0, len(rep.Cases))
	for id := range rep.Cases {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return caseLess(ids[i], ids[j]) })
	return ids
}

// judge says "cannot say" for every reason the data gives, and otherwise applies
// the rule.
func (rep *Report) judge(st *Stratum) (Verdict, []Reason) {
	var why []Reason
	if !rep.Dataset.Pass {
		why = append(why, ReasonLoadFailures)
	}
	if st.Scorable < MinCases {
		if st.Class == ClassCoResidency && st.Predictor.coresidency() && st.Scorable+st.SingleNodeSkips >= MinCases {
			why = append(why, ReasonSingleNode)
		} else {
			why = append(why, ReasonFewCases)
		}
	}
	if st.Unresolved*UnresolvedTruthDen > st.Truth*UnresolvedTruthNum {
		why = append(why, ReasonUnresolvedTruth)
	}
	if len(why) > 0 {
		return CannotSay, why
	}
	e := Evidence{
		Cases:  st.Scorable,
		Recall: float64(st.All.Recall), RecallLower: float64(st.RecallInterval.Lower),
		Lift: float64(st.Lift), LiftLower: float64(st.LiftInterval.Lower),
	}
	if d := st.VsOneHop; d != nil {
		e.HasComparator = true
		e.DiffF1, e.DiffLower, e.DiffUpper = float64(d.F1), float64(d.Interval.Lower), float64(d.Interval.Upper)
	}
	return Judge(e)
}

// JSON is the results contract: the report as indented JSON.
func (rep Report) JSON() ([]byte, error) { return json.MarshalIndent(rep, "", "  ") }

// ResolveNote is what 35 cases can and cannot resolve. It is an estimate made
// before any measurement, printed with every report so that a result is read
// against it.
const ResolveNote = "What 35 cases resolve (an estimate made before any measurement): " +
	"the headline stratum keeps about 15-25 of the 35 cases; " +
	"at a recall of 0.80 the 95 % interval on micro recall is about +/-0.13 at 25 cases and +/-0.23 at 8, " +
	"so a true 0.80 is called good only with about 20 cases or more; " +
	"differences between two predictors under about 0.12-0.15 cannot be resolved; " +
	"chance-level precision is about 0.11-0.22, so a lift of 1.10 is chance."

// Text renders the report for a person. It prints no timing, because these are
// accuracy numbers.
func (rep Report) Text() string {
	var b strings.Builder
	v := rep.Versions
	fmt.Fprintf(&b, "dataset %s, loader %s; policy versions: blastradius %d, coresidency %d, extract %d, interval %d\n",
		rep.Meta.Dataset, rep.Meta.Loader, v.Blastradius, v.Coresidency, v.Extract, v.Interval)
	fmt.Fprintf(&b, "acceptance rule fixed %s; bootstrap %d resamples, seed %d, paired, 95 %% percentile intervals\n",
		rep.Rule.Date, rep.Rule.Resamples, rep.Rule.BootstrapSeed)
	d := rep.Dataset
	fmt.Fprintf(&b, "load check: %d cases, %d uninstrumented seeds excluded, %d counted, %d failed to load (rule: pass if at most %d/%d): pass=%t\n",
		d.Cases, d.ExcludedUninstrumentedSeed, d.Counted, d.LoadFailures, LoadFailureNum, LoadFailureDen, d.Pass)
	fmt.Fprintf(&b, "reported, not in the rule: %d quarantined seeds, %d unmappable roots, %d without a window, %d with unreadable labels, %d without objects\n",
		d.QuarantinedSeeds, d.UnmappableRoots, d.NoWindow, d.UnreadableLabels, d.MissingObjects)
	fmt.Fprintf(&b, "%s\n", ResolveNote)
	for _, s := range rep.Strata {
		mark := ""
		if s.Headline {
			mark = " [headline]"
		}
		fmt.Fprintf(&b, "\n%s / %s / %s%s: %d cases (%d scorable), n=%d hits=%d truth=%d\n",
			s.Level, s.Class, s.Predictor, mark, len(s.Cases), s.Scorable, s.All.N, s.All.Hits, s.All.T)
		fmt.Fprintf(&b, "  precision %s %s, recall %s %s, F1 %s %s, lift %s %s\n",
			f(s.All.Precision), iv(s.PrecisionInterval), f(s.All.Recall), iv(s.RecallInterval),
			f(s.All.F1), iv(s.F1Interval), f(s.Lift), iv(s.LiftInterval))
		fmt.Fprintf(&b, "  without truncated cases: precision %s recall %s (%d cases); without uninstrumented truth: recall %s\n",
			f(s.NoTruncated.Precision), f(s.NoTruncated.Recall), s.NoTruncated.Cases, f(s.NoUninstrumented.Recall))
		if s.VsOneHop != nil {
			fmt.Fprintf(&b, "  F1 minus B_1hop: %s %s%s\n", f(s.VsOneHop.F1), iv(s.VsOneHop.Interval), resolved(s.VsOneHop))
		}
		if s.VsAll != nil {
			fmt.Fprintf(&b, "  F1 minus B_all: %s %s%s\n", f(s.VsAll.F1), iv(s.VsAll.Interval), resolved(s.VsAll))
		}
		if s.Ceiling != nil {
			fmt.Fprintf(&b, "  ceiling (B_union recall, reported and not judged): %s", f(s.Ceiling.Recall))
			if s.Ceiling.BelowGood {
				fmt.Fprintf(&b, "; below %.2f, so good is out of reach because of direction or data, not interval semantics", GoodRecall)
			}
			fmt.Fprintln(&b)
		}
		if s.Verdict != "" {
			fmt.Fprintf(&b, "  verdict: %s", s.Verdict)
			for _, r := range s.Reasons {
				fmt.Fprintf(&b, " [%s]", r)
			}
			fmt.Fprintln(&b)
		}
	}
	return b.String()
}

func f(n Num) string {
	if !finite(float64(n)) {
		return "undefined"
	}
	return fmt.Sprintf("%.3f", float64(n))
}

func iv(i Interval) string {
	if !finite(float64(i.Lower)) || !finite(float64(i.Upper)) {
		return "[undefined]"
	}
	return fmt.Sprintf("[%.3f, %.3f]", float64(i.Lower), float64(i.Upper))
}

func resolved(d *Difference) string {
	if d.Resolved {
		return ""
	}
	return " (not resolved)"
}
