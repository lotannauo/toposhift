package blastscore

import (
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

var meta = Meta{Dataset: "synthetic", Loader: "test"}

func sc(l Level, p Predictor, n, hits, truth int) CaseScore {
	return CaseScore{Level: l, Predictor: p, N: n, Hits: hits, T: truth}
}

// svcRun is a scored call-graph case with the scores of one W, B_all, B_1hop and
// B_union at service level.
func svcRun(id string, w, all, one, union CaseScore) CaseRun {
	r := CaseRun{ID: id, Class: ClassCallGraph, Outcome: OutcomeScored}
	for _, s := range []CaseScore{w, all, one, union} {
		s.Case = id
		r.Scores = append(r.Scores, s)
	}
	return r
}

func goodRuns(n int) []CaseRun {
	var runs []CaseRun
	for i := 1; i <= n; i++ {
		runs = append(runs, svcRun(fmt.Sprintf("Scenario-%d", i),
			sc(LevelService, PredictorW, 4, 4, 4), // P = R = 1
			sc(LevelService, PredictorBAll, 10, 4, 4),
			sc(LevelService, PredictorB1Hop, 3, 2, 4), // P 2/3, R 1/2, F1 4/7
			sc(LevelService, PredictorBUnion, 4, 4, 4)))
	}
	return runs
}

func stratumOf(t *testing.T, rep Report, l Level, c Class, p Predictor) Stratum {
	t.Helper()
	for _, s := range rep.Strata {
		if s.Level == l && s.Class == c && s.Predictor == p {
			return s
		}
	}
	t.Fatalf("no %s / %s / %s stratum", l, c, p)
	return Stratum{}
}

func evaluate(t *testing.T, runs []CaseRun) Report {
	t.Helper()
	rep, err := Evaluate(runs, meta)
	if err != nil {
		t.Fatal(err)
	}
	return rep
}

func TestSevenCasesCannotSayAndEightAreJudged(t *testing.T) {
	w := stratumOf(t, evaluate(t, goodRuns(7)), LevelService, ClassCallGraph, PredictorW)
	if w.Verdict != CannotSay || len(w.Reasons) != 1 || w.Reasons[0] != ReasonFewCases {
		t.Errorf("7 cases: %s %v", w.Verdict, w.Reasons)
	}
	w = stratumOf(t, evaluate(t, goodRuns(8)), LevelService, ClassCallGraph, PredictorW)
	if w.Verdict != Good || !w.Headline {
		t.Errorf("8 perfect cases: %s %v (headline %t)", w.Verdict, w.Reasons, w.Headline)
	}
	if w.All.Recall != 1 || w.Lift != 2.5 || !approx(float64(w.VsOneHop.F1), 1-4.0/7) {
		t.Errorf("recall %v lift %v difference %v", w.All.Recall, w.Lift, w.VsOneHop.F1)
	}
	if !w.VsOneHop.Resolved || !w.VsAll.Resolved {
		t.Error("a difference of 0.43 on identical cases is resolved")
	}
	// Only the primary stratum is a headline, and a baseline has no verdict.
	rep := evaluate(t, goodRuns(8))
	for _, s := range rep.Strata {
		if s.Headline != (s.Predictor == PredictorW) {
			t.Errorf("%s is headline %t", s.Predictor, s.Headline)
		}
		if (s.Verdict != "") != s.Predictor.judged() {
			t.Errorf("%s has verdict %q", s.Predictor, s.Verdict)
		}
	}
}

func TestPoorWhenRecallIsLowOrTheWalkIsResolvablyWorseThanOneHop(t *testing.T) {
	low := goodRuns(10)
	for i := range low {
		low[i].Scores[0].Hits = 2 // recall 0.5
	}
	if w := stratumOf(t, evaluate(t, low), LevelService, ClassCallGraph, PredictorW); w.Verdict != Poor {
		t.Errorf("recall 0.5: %s", w.Verdict)
	}

	worse := goodRuns(10)
	for i := range worse {
		worse[i].Scores[0] = CaseScore{Case: worse[i].ID, Level: LevelService, Predictor: PredictorW, N: 4, Hits: 3, T: 4} // F1 0.75, recall 0.75
		worse[i].Scores[2] = CaseScore{Case: worse[i].ID, Level: LevelService, Predictor: PredictorB1Hop, N: 4, Hits: 4, T: 4}
	}
	w := stratumOf(t, evaluate(t, worse), LevelService, ClassCallGraph, PredictorW)
	if w.Verdict != Poor || w.VsOneHop.Interval.Upper >= 0 || !w.VsOneHop.Resolved {
		t.Errorf("worse than one hop: %s, difference %v %+v", w.Verdict, w.VsOneHop.F1, w.VsOneHop.Interval)
	}

	mixed := goodRuns(10)
	for i := range mixed {
		mixed[i].Scores[0] = CaseScore{Case: mixed[i].ID, Level: LevelService, Predictor: PredictorW, N: 4, Hits: 3, T: 4} // recall 0.75
	}
	if w := stratumOf(t, evaluate(t, mixed), LevelService, ClassCallGraph, PredictorW); w.Verdict != Mixed {
		t.Errorf("recall 0.75: %s %v", w.Verdict, w.Reasons)
	}
}

// Two predictors of one stratum are scored on the same resamples: a predictor
// that does what B_1hop does, case by case, has a difference of exactly zero in
// every resample, so an interval of exactly [0, 0], and is not resolved,
// though its own recall interval has width.
func TestPairedDifferenceOfTheSameListsIsZero(t *testing.T) {
	var runs []CaseRun
	for i := 1; i <= 12; i++ {
		hits := 1 + i%4
		w := sc(LevelService, PredictorW, 4, hits, 4)
		runs = append(runs, svcRun(fmt.Sprintf("Scenario-%d", i), w,
			sc(LevelService, PredictorBAll, 10, 4, 4),
			sc(LevelService, PredictorB1Hop, 4, hits, 4),
			sc(LevelService, PredictorBUnion, 4, 4, 4)))
	}
	w := stratumOf(t, evaluate(t, runs), LevelService, ClassCallGraph, PredictorW)
	d := w.VsOneHop
	if d.F1 != 0 || d.Interval.Lower != 0 || d.Interval.Upper != 0 || d.Resolved {
		t.Errorf("difference %v %+v resolved %t, want exactly zero and not resolved", d.F1, d.Interval, d.Resolved)
	}
	if w.RecallInterval.Lower == w.RecallInterval.Upper {
		t.Error("the recall interval has no width")
	}
	if !strings.Contains(evaluate(t, runs).Text(), "(not resolved)") {
		t.Error("the text report does not say a difference is not resolved")
	}
}

func TestDatasetLoadRule(t *testing.T) {
	mk := func(excluded, failed, ok int) []CaseRun {
		var runs []CaseRun
		id := 0
		add := func(r CaseRun) { id++; r.ID = fmt.Sprintf("Scenario-%d", id); runs = append(runs, r) }
		for i := 0; i < excluded; i++ {
			add(CaseRun{Outcome: OutcomeExcluded, Exclusion: ExcludedUninstrumentedSeed})
		}
		for i := 0; i < failed; i++ {
			add(CaseRun{Outcome: OutcomeCannotSay, Reason: ReasonSeedAbsent, LoadFailed: true})
		}
		for i := 0; i < ok; i++ {
			add(CaseRun{Outcome: OutcomeScored})
		}
		return runs
	}
	for _, tc := range []struct {
		name                 string
		excluded, failed, ok int
		pass                 bool
	}{
		{"a quarter exactly", 0, 6, 18, true},
		{"just over a quarter", 0, 7, 17, false},
		{"none", 0, 0, 24, true},
		// 8 of 35 would pass (22.9 %) if uninstrumented seeds were counted, and
		// 8 of 26 does not (30.8 %): they are taken out first.
		{"uninstrumented seeds are excluded before the count", 9, 8, 18, false},
		{"and still pass when the rest is enough", 9, 6, 20, true},
		{"no case at all", 0, 0, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := datasetCheck(mk(tc.excluded, tc.failed, tc.ok))
			if d.Pass != tc.pass || d.ExcludedUninstrumentedSeed != tc.excluded || d.LoadFailures != tc.failed {
				t.Errorf("%+v, want pass %t", d, tc.pass)
			}
		})
	}

	// A quarantined seed, an unmappable root and a missing window are reported
	// and never count as load failures: 6 failures among 24 counted cases pass,
	// whatever else is reported, and 7 do not.
	t.Run("reported cases are not load failures", func(t *testing.T) {
		build := func(failed int) []CaseRun {
			runs := mk(0, failed, 0)
			add := func(r CaseRun) { r.ID = fmt.Sprintf("Scenario-%d", 200+len(runs)); runs = append(runs, r) }
			for i := 0; i < 3; i++ {
				add(CaseRun{Outcome: OutcomeCannotSay, Reason: ReasonQuarantinedSeed})
				add(CaseRun{Outcome: OutcomeExcluded, Exclusion: ExcludedUnmappableRoot})
				add(CaseRun{Outcome: OutcomeExcluded, Exclusion: ExcludedNoWindow})
			}
			for len(runs) < 24 {
				add(CaseRun{Outcome: OutcomeScored})
			}
			return runs
		}
		d := datasetCheck(build(6))
		if !d.Pass || d.LoadFailures != 6 || d.QuarantinedSeeds != 3 || d.UnmappableRoots != 3 || d.NoWindow != 3 || d.Counted != 24 {
			t.Errorf("%+v", d)
		}
		if d := datasetCheck(build(7)); d.Pass || d.LoadFailures != 7 || d.Counted != 24 {
			t.Errorf("7 of 24: %+v", d)
		}
	})

	// A dataset that fails the load check leaves every stratum unable to say.
	runs := append(goodRuns(8), mk(0, 12, 0)...)
	for i := range runs[8:] {
		runs[8+i].ID = fmt.Sprintf("Scenario-%d", 100+i)
	}
	w := stratumOf(t, evaluate(t, runs), LevelService, ClassCallGraph, PredictorW)
	if w.Verdict != CannotSay || w.Reasons[0] != ReasonLoadFailures {
		t.Errorf("%s %v", w.Verdict, w.Reasons)
	}
}

func TestUnresolvedTruthRule(t *testing.T) {
	mk := func(unmappable int) Report {
		runs := goodRuns(8) // truth 4 each, 32 in all
		for i := range runs {
			runs[i].Scores[0].T, runs[i].Scores[0].Hits, runs[i].Scores[0].N = 5, 4, 4
		}
		runs[0].Scores[0].Unmappable = min(unmappable, 5)
		runs[1].Scores[0].Unmappable = max(unmappable-5, 0)
		return evaluate(t, runs)
	}
	// 40 truth entities: 8 unresolved is exactly a fifth.
	if w := stratumOf(t, mk(8), LevelService, ClassCallGraph, PredictorW); w.Verdict == CannotSay {
		t.Errorf("20 %% unresolved: %v", w.Reasons)
	}
	w := stratumOf(t, mk(9), LevelService, ClassCallGraph, PredictorW)
	if w.Verdict != CannotSay || w.Reasons[0] != ReasonUnresolvedTruth || w.Unresolved != 9 || w.Truth != 40 {
		t.Errorf("over 20 %% unresolved: %s %v (%d of %d)", w.Verdict, w.Reasons, w.Unresolved, w.Truth)
	}
}

// A co-residency stratum that reaches 8 cases only before the single-node rule
// removes its one-node cases cannot say, and says why.
func TestSingleNodeRule(t *testing.T) {
	mk := func(scored, singleNode int) []CaseRun {
		var runs []CaseRun
		for i := 1; i <= scored+singleNode; i++ {
			r := CaseRun{ID: fmt.Sprintf("Scenario-%d", i), Class: ClassCoResidency, Outcome: OutcomeScored}
			if i <= scored {
				r.Scores = []CaseScore{
					{Case: r.ID, Level: LevelPod, Predictor: PredictorC, N: 3, Hits: 3, T: 3},
					{Case: r.ID, Level: LevelPod, Predictor: PredictorBAll, N: 9, Hits: 3, T: 3},
				}
			} else {
				r.Skips = []Skip{{Level: LevelPod, Predictor: PredictorC, Reason: ReasonSingleNodeCase, T: 3}}
			}
			runs = append(runs, r)
		}
		return runs
	}
	for _, tc := range []struct {
		scored, single int
		verdict        Verdict
		reason         Reason
	}{
		{5, 3, CannotSay, ReasonSingleNode},
		{5, 2, CannotSay, ReasonFewCases},
		{7, 1, CannotSay, ReasonSingleNode},
		{8, 0, Mixed, ReasonNoComparator},
		{8, 5, Mixed, ReasonNoComparator},
		{0, 9, CannotSay, ReasonSingleNode},
	} {
		c := stratumOf(t, evaluate(t, mk(tc.scored, tc.single)), LevelPod, ClassCoResidency, PredictorC)
		if c.Verdict != tc.verdict || c.Reasons[0] != tc.reason || c.SingleNodeSkips != tc.single {
			t.Errorf("%d scored, %d single-node: %s %v (skips %d), want %s %s", tc.scored, tc.single, c.Verdict, c.Reasons, c.SingleNodeSkips, tc.verdict, tc.reason)
		}
	}
}

// A pod-level stratum has no B_1hop: it can be Poor or Mixed and never Good.
func TestNoComparatorIsNeverGood(t *testing.T) {
	var runs []CaseRun
	for i := 1; i <= 10; i++ {
		id := fmt.Sprintf("Scenario-%d", i)
		runs = append(runs, CaseRun{ID: id, Class: ClassCoResidency, Outcome: OutcomeScored, Scores: []CaseScore{
			{Case: id, Level: LevelPod, Predictor: PredictorW, N: 4, Hits: 4, T: 4},
			{Case: id, Level: LevelPod, Predictor: PredictorBAll, N: 10, Hits: 4, T: 4},
		}})
	}
	w := stratumOf(t, evaluate(t, runs), LevelPod, ClassCoResidency, PredictorW)
	if w.Verdict != Mixed || w.VsOneHop != nil || w.Reasons[0] != ReasonNoComparator {
		t.Errorf("%s %v", w.Verdict, w.Reasons)
	}
}

// If the union graph does not reach a recall of 0.80, Good is out of reach for
// reasons of direction or data, and the report says so without moving a
// threshold.
func TestCeiling(t *testing.T) {
	runs := goodRuns(10)
	for i := range runs {
		runs[i].Scores[3].Hits = 2 // B_union recall 0.5
	}
	rep := evaluate(t, runs)
	w := stratumOf(t, rep, LevelService, ClassCallGraph, PredictorW)
	if w.Ceiling == nil || w.Ceiling.Recall != 0.5 || !w.Ceiling.BelowGood {
		t.Errorf("ceiling %+v", w.Ceiling)
	}
	if w.Verdict != Good {
		t.Errorf("the ceiling is reported and not judged: verdict %s", w.Verdict)
	}
	if !strings.Contains(rep.Text(), "good is out of reach because of direction or data") {
		t.Error("the text report does not say Good is out of reach")
	}
	if c := stratumOf(t, evaluate(t, goodRuns(10)), LevelService, ClassCallGraph, PredictorW).Ceiling; c.BelowGood {
		t.Error("a ceiling of 1 is below Good")
	}
}

func TestEvaluateRefusesWhatItCannotKey(t *testing.T) {
	if _, err := Evaluate([]CaseRun{{ID: "Scenario-1"}, {ID: "Scenario-1"}}, meta); err == nil {
		t.Error("a case id twice")
	}
	if _, err := Evaluate([]CaseRun{{ID: "frontend"}}, meta); err == nil {
		t.Error("an entity name as a case id")
	}
	if _, err := Evaluate(nil, Meta{Dataset: "a name with spaces", Loader: "x"}); err == nil {
		t.Error("a free-text label")
	}
}

func TestReportCarriesTheVersionsAndTheResolveNote(t *testing.T) {
	rep := evaluate(t, goodRuns(8))
	if rep.Versions != CurrentVersions() || rep.Versions.Blastradius == 0 || rep.Versions.Interval == 0 {
		t.Errorf("versions %+v", rep.Versions)
	}
	text := rep.Text()
	for _, want := range []string{ResolveNote, "[headline]", "ceiling (B_union recall, reported and not judged)", "acceptance rule fixed 2026-10-15", "seed 20261015"} {
		if !strings.Contains(text, want) {
			t.Errorf("the text report lacks %q", want)
		}
	}
	for _, banned := range []string{"ms", "ops/s", "duration", "latency"} {
		if strings.Contains(strings.ToLower(text), " "+banned) {
			t.Errorf("the text report mentions %q: these are accuracy numbers", banned)
		}
	}
}

// The results contract: the report holds pooled numbers and numbers keyed by
// case id, and no name, uid, label text or timestamp. It is checked three ways:
// by the type, which may hold no free string; by a report made from real runs,
// whose JSON may hold no fingerprint and no date; and by the vocabulary of every
// string in it.
func TestResultsContract(t *testing.T) {
	allowedStrings := map[reflect.Type]bool{
		reflect.TypeFor[Level](): true, reflect.TypeFor[Class](): true, reflect.TypeFor[Predictor](): true,
		reflect.TypeFor[Outcome](): true, reflect.TypeFor[Exclusion](): true, reflect.TypeFor[Reason](): true,
		reflect.TypeFor[Verdict](): true,
	}
	// The labels, the case ids and the date of the rule are the only free strings.
	freeStrings := map[string]bool{
		"blastscore.Meta.Dataset": true, "blastscore.Meta.Loader": true, "blastscore.Rule.Date": true,
	}
	var walk func(path string, ty reflect.Type)
	walk = func(path string, ty reflect.Type) {
		switch ty.Kind() {
		case reflect.String:
			if !allowedStrings[ty] && !freeStrings[path] && path != "blastscore.Stratum.Cases" {
				t.Errorf("%s is a free string", path)
			}
		case reflect.Struct:
			if ty == reflect.TypeFor[time.Time]() || ty == reflect.TypeFor[identity.Fingerprint]() {
				t.Errorf("%s holds a %s", path, ty)
				return
			}
			for i := 0; i < ty.NumField(); i++ {
				f := ty.Field(i)
				if f.Tag.Get("json") == "-" {
					continue
				}
				walk("blastscore."+ty.Name()+"."+f.Name, f.Type)
			}
		case reflect.Slice, reflect.Array, reflect.Pointer:
			if ty.Elem().Kind() == reflect.Uint8 {
				t.Errorf("%s holds bytes", path)
			}
			walk(path, ty.Elem())
		case reflect.Map:
			if ty.Key().Kind() != reflect.String {
				t.Errorf("%s has a map key that is not a case id", path)
			}
			walk(path, ty.Elem())
		}
	}
	walk("Report", reflect.TypeFor[Report]())

	// A report from real runs.
	S, X, Y := svc(1), svc(2), svc(3)
	p1, p2, n1, n2 := pod(1), pod(2), node(1), node(2)
	all := []identity.Fingerprint{S, X, Y, p1, p2, n1, n2}
	var runs []CaseRun
	for i := 1; i <= 3; i++ {
		st, f := world(t, memstore.Options{}, all...)
		f.write(obs(dependsOn(X, S), hm(9, 0)), obs(dependsOn(Y, X), hm(9, 0)), obs(sched(p1, n1), hm(9, 0)), obs(sched(p2, n1), hm(9, 0)))
		c := svcCase(fmt.Sprintf("Scenario-%d", i), S, []identity.Fingerprint{S, X, Y}, X, Y)
		c.SeedPods = []identity.Fingerprint{p1}
		c.Pods = []identity.Fingerprint{p1, p2}
		c.Pod = Truth{Entities: []identity.Fingerprint{p2}}
		c.Nodes = 2
		c.SnapshotTime = hm(10, 5)
		runs = append(runs, runCase(t, st, c, Config{}))
	}
	rep := evaluate(t, runs)
	raw, err := rep.JSON()
	if err != nil {
		t.Fatal(err)
	}
	js := string(raw)
	for _, fp := range all {
		for _, form := range []string{fp.String(), fmt.Sprintf("%x", fp.Hash())} {
			if strings.Contains(js, form) {
				t.Errorf("the results hold %q of a %s", form, fp.Type())
			}
		}
	}
	if m := regexp.MustCompile(`\d{4}-\d{2}-\d{2}`).FindString(strings.ReplaceAll(js, AcceptanceDate, "")); m != "" {
		t.Errorf("the results hold a date: %s", m)
	}
	if regexp.MustCompile(`\d{2}:\d{2}:\d{2}`).MatchString(js) {
		t.Error("the results hold a time of day")
	}

	var doc any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("the results are not JSON: %v", err)
	}
	vocab := map[string]bool{"synthetic": true, "test": true, AcceptanceDate: true}
	for _, set := range [][]string{
		{"service", "pod", "call-graph", "co-residency", "scored", "excluded", "cannot_say", "good", "poor", "mixed"},
		predictorNames(), reasonNames(),
	} {
		for _, s := range set {
			vocab[s] = true
		}
	}
	for id := range rep.Cases {
		vocab[id] = true
	}
	var check func(v any, where string)
	check = func(v any, where string) {
		switch x := v.(type) {
		case string:
			if !vocab[x] {
				t.Errorf("%s holds the string %q, which is not in the vocabulary", where, x)
			}
		case []any:
			for _, e := range x {
				check(e, where)
			}
		case map[string]any:
			for k, e := range x {
				if !regexp.MustCompile(`^[a-z0-9_]+$`).MatchString(k) && !CaseIDPattern.MatchString(k) {
					t.Errorf("%s has the key %q", where, k)
				}
				check(e, where+"."+k)
			}
		}
	}
	check(doc, "results")
	cases := doc.(map[string]any)["cases"].(map[string]any)
	if len(cases) != 3 {
		t.Errorf("%d cases keyed by id, want 3", len(cases))
	}
	// An undefined number is null, not a NaN that cannot be written.
	if !strings.Contains(js, "null") {
		t.Log("no undefined number in this report")
	}
}

func predictorNames() []string {
	return []string{string(PredictorW), string(PredictorWPod), string(PredictorC), string(PredictorCSvc), string(PredictorWC), string(PredictorBAll), string(PredictorB1Hop), string(PredictorBUnion)}
}

func reasonNames() []string {
	var out []string
	for _, r := range []Reason{
		ReasonFewCases, ReasonSingleNode, ReasonUnresolvedTruth, ReasonLoadFailures, ReasonLiftUndefined, ReasonNoComparator,
		ReasonSeedAbsent, ReasonQuarantinedSeed, ReasonNoDependsOn, ReasonSnapshotOutside, ReasonNoSeedPods, ReasonSingleNodeCase,
		Reason(ExcludedUninstrumentedSeed), Reason(ExcludedUnmappableRoot), Reason(ExcludedNoWindow),
	} {
		out = append(out, string(r))
	}
	return out
}
