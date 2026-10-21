package runner_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/internal/store/pebblekv"
)

// Every duration here is a number the test chose to look like one: none is a measurement.

const (
	rtBase = "L/k64a2l5m"
	rtM    = "M/crdb1"
	rtOff  = "L/off"

	rtHot  = "service<- hot-records neighbors"
	rtMed  = "node<- median neighbors"
	rtTiny = "pod-> hot-records neighbors"
)

var rtCommit = strings.Repeat("a", 40)

// rtPlan has three kinds of group: the hottest prefixes (three queries, at two ages), a
// median group of four queries, and a group that a candidate answers in under a
// microsecond.
func rtPlan() *runner.Plan {
	p := makePlan("rt-stream", 1000)
	p.CacheBytes = runner.ReadTimingCacheBytes
	add := func(group string, n int, ages ...string) {
		for _, age := range ages {
			for i := range n {
				p.Queries = append(p.Queries, runner.Query{Group: group, Rank: i, Op: runner.OpNeighbors, Age: age, Expect: "x"})
			}
		}
	}
	add(rtHot, 3, runner.AgeNow, runner.Age1d)
	add(rtMed, 4, runner.AgeNow)
	add(rtTiny, 2, runner.AgeNow)
	return p
}

// rtCandidate is how a candidate's queries compare: the factor on the durations, in
// thousandths.
type rtJobSpec struct {
	cand, arch, cpu string
	rep             int
	// scale is the candidate's durations in per-mille of the standard ones.
	scale int64
	// notWarm are the queries (by index in the plan) that missed the cache.
	notWarm []int
	// exec is the binary's hash, and run the run id; zero values are the defaults.
	exec, run string
}

// rtStd are the durations of the plan's queries at scale 1000, in ns: the hot group at
// "now" is 10, 50 and 20 µs, at 1d 11, 12 and 13 µs; the median group is 10, 40, 20 and
// 30 µs; the group that is fast is 300 and 400 ns.
var rtStd = []int64{10_000, 50_000, 20_000, 11_000, 12_000, 13_000, 10_000, 40_000, 20_000, 30_000, 300, 400}

// rtNs is 31 durations whose median (the 16th of them in order) is m, in a scrambled
// order, with the largest replaced by a huge one, so that the mean, the maximum, the
// first and the last are none of them the median.
func rtNs(m int64) []int64 {
	ns := make([]int64, 31)
	for i := range ns {
		off := int64((i*7)%31 - 15)
		ns[i] = m + off
		if off == 15 {
			ns[i] = m * 1000
		}
	}
	return ns
}

func rtRead(p *runner.Plan, planDigest string, s rtJobSpec) runner.TimedRead {
	if s.exec == "" {
		s.exec = strings.Repeat("1", 64)
	}
	if s.run == "" {
		s.run = "100"
	}
	t := &runner.ReadTimings{
		Version: runner.ReadTimingsVersion, Candidate: s.cand, PlanDigest: planDigest, ManifestDigest: "m",
		Build:      runner.BuildInfo{Executable: s.exec, Revision: rtCommit},
		GOMAXPROCS: 4, NumCPU: 4, GoMemLimit: math.MaxInt64, CacheBytes: runner.ReadTimingCacheBytes,
		Warmup: runner.ReadTimingWarmup, Reps: runner.ReadTimingReps, Readers: runner.ReadTimingReaders, WarmCounter: runner.PebbleWarmCounter,
	}
	for i, q := range p.Queries {
		qt := runner.QueryTiming{Name: q.Name(), Group: q.Group, Age: q.Age, Op: q.Op, Digest: "x", Warm: !slices.Contains(s.notWarm, i)}
		// Each repetition adds a fiftieth of the standard duration, so that the job values differ.
		qt.Ns = rtNs(rtStd[i]*s.scale/1000 + int64(s.rep)*rtStd[i]/50)
		if !qt.Warm {
			qt.WarmDelta = 3
		}
		// A collection ran during the timed asks of query 1 in the odd repetitions.
		if i == 1 {
			qt.GCs = uint32(s.rep % 2)
		}
		t.Queries = append(t.Queries, qt)
	}
	return runner.TimedRead{
		Job: runner.Job{
			SHA: rtCommit, Arch: s.arch, Family: "rmae", Window: 7, Candidate: s.cand, Rep: s.rep, CPUModel: s.cpu,
			RunID: s.run, RunAttempt: "1", BinarySHA256: s.exec, NProc: 4,
		},
		Timings: t,
		Results: &runner.Results{Candidate: s.cand, PlanDigest: planDigest, Describe: map[string]string{"cache_bytes": fmt.Sprint(runner.ReadTimingCacheBytes)}},
	}
}

// rtWorld is five jobs on each of the three candidates, on one architecture and CPU
// model, and the options that judge them.
func rtWorld(t *testing.T) ([]runner.TimedRead, runner.ReadTimingOptions) {
	t.Helper()
	p := rtPlan()
	d, err := p.Digest()
	if err != nil {
		t.Fatal(err)
	}
	var reads []runner.TimedRead
	for _, c := range []struct {
		name  string
		scale int64
	}{{rtBase, 1000}, {rtM, 2000}, {rtOff, 900}} {
		for rep := 1; rep <= 5; rep++ {
			reads = append(reads, rtRead(p, d, rtJobSpec{cand: c.name, arch: "x86_64", cpu: "Test X86 A", rep: rep, scale: c.scale}))
		}
	}
	return reads, runner.ReadTimingOptions{
		Commit: rtCommit, Plans: []*runner.Plan{p}, Base: rtBase, Jobs: 5,
		Primary: []string{rtMed + "@now", rtHot + "@now"},
	}
}

func TestTheReadTimingRulesTextAndItsDigestArePinned(t *testing.T) {
	t.Parallel()

	text := runner.ReadTimingRulesText()
	for _, want := range []string{
		"read timing rules 1\n",
		"report only: the judge decides nothing",
		"the hashing of the answer is not timed",
		"the engine's counter reporting is, with the recorder muted",
		"per query 3 untimed warm-up asks and then 31 timed asks, 1 reader",
		"the block cache is 256 MiB (the product's)",
		"a query that is not warm in a job is left out of its cell for that job and is counted",
		"its value is printed apart and is never counted among the cell's job values",
		"the median of its timed asks",
		"the largest for a group of the hottest prefixes, the lower median for a group around the median",
		"at least 3 counted job values in a cell",
		"or pooled over models when every candidate compared has at least 3 counted job values",
		"never one number",
		"printed as \"< 1 µs\"",
		"below 1000 ns",
		"the same job twice",
		"collections counted",
		"summary line gives how many jobs are outside every scope and their models, without changing the exit status",
		"exit status 3",
		"exit status 2",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("the rules text lacks %q", want)
		}
	}
	// The cache of the judge is the product's, and the one default is the other's.
	if got := pebblekv.BenchTuning().CacheBytes; got != runner.ReadTimingCacheBytes {
		t.Errorf("the product's block cache is %d bytes, the judge's %d", got, runner.ReadTimingCacheBytes)
	}
	const pinned = "e76692c37dda16ce4a1127ae85604120c399e7808f952ab9bcd8779eabfbdcec"
	sum := sha256.Sum256([]byte(text))
	if got := hex.EncodeToString(sum[:]); got != runner.ReadTimingRulesDigest() || got != pinned {
		t.Errorf("the rules text has digest %s, pinned at %s", got, pinned)
	}
	if want := "read timing rules 1 digest " + runner.ReadTimingRulesDigest(); runner.ReadTimingDigestLine() != want {
		t.Errorf("the digest line is %q", runner.ReadTimingDigestLine())
	}
	// Rules v5 is not touched: its digest is the one every citation names.
	v5, err := runner.DefaultRules().Digest()
	if err != nil || !strings.HasPrefix(v5, "ea47515f5ec1") {
		t.Errorf("the decision rules' digest is %s (%v), not ea47515f5ec1…", v5, err)
	}
}

func judge(t *testing.T, reads []runner.TimedRead, o runner.ReadTimingOptions) *runner.ReadTimingReport {
	t.Helper()
	rep, err := runner.JudgeReadTimings(reads, o)
	if err != nil {
		t.Fatal(err)
	}
	return rep
}

func findCell(t *testing.T, a runner.ReadTimingArch, group, age string) runner.ReadTimingCell {
	t.Helper()
	for _, c := range a.Cells {
		if c.Group == group && c.Age == age {
			return c
		}
	}
	t.Fatalf("no cell %s@%s", group, age)
	return runner.ReadTimingCell{}
}

func findCand(t *testing.T, s runner.ReadTimingScope, name string) runner.ReadTimingCandidate {
	t.Helper()
	for _, c := range s.Candidates {
		if c.Name == name {
			return c
		}
	}
	t.Fatalf("no candidate %s", name)
	return runner.ReadTimingCandidate{}
}

func approx(a, b float64) bool { return math.Abs(a-b) <= 1e-9*math.Max(1, math.Abs(b)) }

func TestReadTimingSummary(t *testing.T) {
	t.Parallel()

	reads, o := rtWorld(t)
	rep := judge(t, reads, o)
	var out bytes.Buffer
	runner.WriteReadTimings(&out, rep)
	checkGolden(t, "testdata/readtiming_report.golden", out.Bytes())

	if !strings.HasPrefix(out.String(), runner.ReadTimingDigestLine()+"\n") {
		t.Errorf("the report does not begin with the digest of the rules:\n%.200s", out.String())
	}
	if len(rep.Arches) != 1 || len(rep.NotReported) != 0 {
		t.Fatalf("%d architectures, not reported %v", len(rep.Arches), rep.NotReported)
	}
	a := rep.Arches[0]

	// Primary cells first, in the order given, then the rest in the plan's.
	var order []string
	for _, c := range a.Cells {
		order = append(order, fmt.Sprintf("%s@%s/%v", c.Group, c.Age, c.Primary))
	}
	wantOrder := []string{rtMed + "@now/true", rtHot + "@now/true", rtHot + "@1d/false", rtTiny + "@now/false"}
	if !slices.Equal(order, wantOrder) {
		t.Errorf("the cells are in the order %v, want %v", order, wantOrder)
	}

	// The hottest group: the largest of the queries' medians (50 µs at scale 1000, and a
	// repetition adds 1 µs for its number).
	hot := findCell(t, a, rtHot, "now")
	if hot.Statistic != "largest" || hot.Queries != 3 || len(hot.Scopes) != 1 {
		t.Fatalf("hot cell: %+v", hot)
	}
	base := findCand(t, hot.Scopes[0], rtBase)
	var gotJobs []int64
	for _, j := range base.Jobs {
		gotJobs = append(gotJobs, j.Value.Ns)
	}
	// Query 1's median is 50000 + rep*1000 at scale 1000.
	if want := []int64{51000, 52000, 53000, 54000, 55000}; !slices.Equal(gotJobs, want) {
		t.Errorf("the base's job values are %v, want %v: the largest of the medians, in job order", gotJobs, want)
	}
	if base.Min.Ns != 51000 || base.Median.Ns != 53000 || base.Max.Ns != 55000 {
		t.Errorf("min, median, max = %v %v %v", base.Min, base.Median, base.Max)
	}
	if !approx(base.Spread, 55000.0/51000.0) {
		t.Errorf("the base's spread is %v, want the largest job value over the smallest", base.Spread)
	}
	// M is at twice the durations: its job values are 100000 + rep*1000, and the ratio is
	// the ratio of the medians, bounded by min/max and max/min.
	m := findCand(t, hot.Scopes[0], rtM)
	if m.Min.Ns != 101000 || m.Median.Ns != 103000 || m.Max.Ns != 105000 {
		t.Errorf("M: min, median, max = %v %v %v", m.Min, m.Median, m.Max)
	}
	if !approx(m.Ratio, 103000.0/53000.0) || !approx(m.RatioLow, 101000.0/55000.0) || !approx(m.RatioHigh, 105000.0/51000.0) {
		t.Errorf("M's ratio %v [%v, %v]", m.Ratio, m.RatioLow, m.RatioHigh)
	}
	if base.Ratio != 0 {
		t.Errorf("the base has a ratio to itself: %v", base.Ratio)
	}

	// The median group has four queries, 10, 40, 20 and 30 µs: its statistic is the lower
	// median, 20 µs, and neither the largest nor the upper median.
	med := findCell(t, a, rtMed, "now")
	if med.Statistic != "median" || med.Queries != 4 {
		t.Fatalf("median cell: %+v", med)
	}
	if got := findCand(t, med.Scopes[0], rtBase).Median.Ns; got != 21200 {
		t.Errorf("the median group's value is %d, want the lower median of 10, 20, 30 and 40 µs (21200)", got)
	}

	// The noise is the largest spread of a candidate in a primary cell.
	var worst float64
	for _, c := range a.Cells {
		if c.Primary {
			for _, s := range c.Scopes {
				for _, cand := range s.Candidates {
					worst = math.Max(worst, cand.Spread)
				}
			}
		}
	}
	if worst <= 1 || !approx(rep.Noise, worst) {
		t.Errorf("the run's noise is %v, want %v", rep.Noise, worst)
	}

	// A duration below a microsecond is no number: not in the text, not in a ratio, not
	// in a spread, not in the JSON.
	tiny := findCell(t, a, rtTiny, "now")
	for _, cand := range tiny.Scopes[0].Candidates {
		if !cand.Min.Below || !cand.Median.Below || !cand.Max.Below || cand.Min.Ns != 0 || cand.Median.Ns != 0 {
			t.Errorf("%s: min, median, max = %+v %+v %+v for sub-microsecond reads", cand.Name, cand.Min, cand.Median, cand.Max)
		}
		if cand.Spread != 0 || cand.Ratio != 0 || cand.RatioLow != 0 || (cand.Name != rtBase && !cand.RatioNotFormed) {
			t.Errorf("%s: a spread or ratio was formed from sub-microsecond reads: %+v", cand.Name, cand)
		}
		for _, j := range cand.Jobs {
			if !j.Value.Below || j.Value.Ns != 0 {
				t.Errorf("%s: job value %+v", cand.Name, j.Value)
			}
		}
	}
	b, _ := json.Marshal(tiny)
	if regexp.MustCompile(`"Ns":[1-9]`).Match(b) {
		t.Errorf("a sub-microsecond duration is in the JSON: %s", b)
	}
	text := out.String()
	at := strings.Index(text, "cell "+rtTiny)
	if at < 0 || !strings.Contains(text[at:], "jobs (µs) [< 1 µs") || !strings.Contains(text[at:], "ratio to base not formed (< 1 µs)") {
		t.Errorf("the cell of reads under a microsecond does not say so:\n%s", text)
	}

	// Every candidate of every cell is its job values with min, median and max: never one number.
	for _, l := range strings.Split(text, "\n") {
		complete := strings.Contains(l, "jobs (µs) [") && strings.Contains(l, "min ") && strings.Contains(l, "median ") && strings.Contains(l, "max ")
		if strings.HasPrefix(l, "    ") && !strings.HasPrefix(l, "      ") && !complete {
			t.Errorf("a candidate's line is not its job values with their min, median and max: %q", l)
		}
	}
}

// A query that missed the cache in a job is left out of its cell for that job, the cell
// is partial for that job, and a partial job is neither counted among the values nor
// allowed to flatter the candidate: here the slowest query is the one left out.
func TestReadTimingLeavesOutAndReportsANotWarmQuery(t *testing.T) {
	t.Parallel()

	reads, o := rtWorld(t)
	p := o.Plans[0]
	d, _ := p.Digest()
	for i, r := range reads {
		if r.Job.Candidate == rtM && r.Job.Rep == 2 {
			// Query 1, the largest of the hot group at "now", missed the cache in this job.
			reads[i] = rtRead(p, d, rtJobSpec{cand: rtM, arch: "x86_64", cpu: "Test X86 A", rep: 2, scale: 2000, notWarm: []int{1}})
		}
	}
	rep := judge(t, reads, runner.ReadTimingOptions{Commit: o.Commit, Plans: o.Plans, Base: o.Base, Jobs: 5, Primary: o.Primary})
	hot := findCell(t, rep.Arches[0], rtHot, "now")
	m := findCand(t, hot.Scopes[0], rtM)
	if len(m.Jobs) != 4 || len(m.Partial) != 1 {
		t.Fatalf("M has %d counted jobs and %d partial", len(m.Jobs), len(m.Partial))
	}
	if m.Partial[0].Rep != 2 || m.Partial[0].Warm != 2 || m.Partial[0].Queries != 3 || !m.Partial[0].Partial {
		t.Errorf("the partial job: %+v", m.Partial[0])
	}
	// Its value is the largest of the two queries left (20 and 10 µs doubled), lower than
	// any job that counts: it must not be among the values.
	if m.Partial[0].Value.Ns >= m.Min.Ns {
		t.Errorf("the partial value %d is not below the counted minimum %d: the test does not show a flattering job", m.Partial[0].Value.Ns, m.Min.Ns)
	}
	if m.LeftOut != 1 {
		t.Errorf("queries left out: %d, want 1", m.LeftOut)
	}
	if m.Min.Ns != 101000 || m.Max.Ns != 105000 {
		t.Errorf("min and max %v %v: the partial job counted", m.Min, m.Max)
	}
	var out bytes.Buffer
	runner.WriteReadTimings(&out, rep)
	if !strings.Contains(out.String(), "queries left out 1") || !strings.Contains(out.String(), "partial: rep 2 (2 of 3 warm,") || !strings.Contains(out.String(), "not counted)") {
		t.Errorf("the report does not name the query left out and the partial job:\n%s", out.String())
	}
	// A job with no warm query in a cell has no value in it; it is partial all the same.
	reads[7] = rtRead(p, d, rtJobSpec{cand: rtM, arch: "x86_64", cpu: "Test X86 A", rep: 3, scale: 2000, notWarm: []int{0, 1, 2}})
	rep = judge(t, reads, runner.ReadTimingOptions{Commit: o.Commit, Plans: o.Plans, Base: o.Base, Jobs: 5, Primary: o.Primary})
	m = findCand(t, findCell(t, rep.Arches[0], rtHot, "now").Scopes[0], rtM)
	if len(m.Jobs) != 3 || len(m.Partial) != 2 || m.Partial[1].Warm != 0 {
		t.Errorf("a job with no warm query: %d counted, %+v partial", len(m.Jobs), m.Partial)
	}
	out.Reset()
	runner.WriteReadTimings(&out, rep)
	if !strings.Contains(out.String(), "no warm query") {
		t.Errorf("a job with no warm query is not said to have none:\n%s", out.String())
	}
}

// Each thing that makes the judge decline to judge is declined, with the reason named.
func TestReadTimingRefuses(t *testing.T) {
	t.Parallel()

	_, o0 := rtWorld(t)
	otherPlan := rtPlan()
	otherPlan.Stream.Digest = "another"
	otherDigest, _ := otherPlan.Digest()
	plan64 := rtPlan()
	plan64.CacheBytes = 64 << 20
	plan64Digest, _ := plan64.Digest()

	cases := []struct {
		name   string
		mutate func(reads []runner.TimedRead, o *runner.ReadTimingOptions)
		want   string
	}{
		{"a second commit", func(r []runner.TimedRead, _ *runner.ReadTimingOptions) { r[3].Job.SHA = strings.Repeat("b", 40) }, "the jobs are of the commits"},
		{"a commit that is not the one named", func(_ []runner.TimedRead, o *runner.ReadTimingOptions) { o.Commit = strings.Repeat("c", 40) }, "not only of cccc"},
		{"a commit that is no commit", func(_ []runner.TimedRead, o *runner.ReadTimingOptions) { o.Commit = "main" }, `-commit "main" is not 40`},
		{"the same job twice", func(r []runner.TimedRead, _ *runner.ReadTimingOptions) { r[1].Job.Rep = r[0].Job.Rep }, "the same job twice: L/k64a2l5m rep 1 on x86_64 (run 100 attempt 1)"},
		{"a second run", func(r []runner.TimedRead, _ *runner.ReadTimingOptions) { r[4].Job.RunID = "101" }, "more than one run or attempt"},
		{"a second attempt", func(r []runner.TimedRead, _ *runner.ReadTimingOptions) { r[4].Job.RunAttempt = "2" }, "100 attempt 2"},
		{"two binaries for an architecture", func(r []runner.TimedRead, _ *runner.ReadTimingOptions) {
			r[0].Job.BinarySHA256 = strings.Repeat("9", 64)
		}, "x86_64: more than one binary"},
		{"two plans", func(r []runner.TimedRead, _ *runner.ReadTimingOptions) { r[0].Timings.PlanDigest = otherDigest }, "more than one plan"},
		{"a plan that was not given", func(_ []runner.TimedRead, o *runner.ReadTimingOptions) { o.Plans = []*runner.Plan{otherPlan} }, "is not among the plans given"},
		{"an untimed read", func(r []runner.TimedRead, _ *runner.ReadTimingOptions) { r[2].Timings.Untimed = true }, "an untimed read"},
		{"a cache of 64 MiB in the timings", func(r []runner.TimedRead, _ *runner.ReadTimingOptions) { r[2].Timings.CacheBytes = 64 << 20 }, "read with a block cache of 67108864 bytes, not 256 MiB"},
		{"a cache of 64 MiB in the plan", func(r []runner.TimedRead, o *runner.ReadTimingOptions) {
			for i := range r {
				r[i].Timings.PlanDigest = plan64Digest
			}
			o.Plans = []*runner.Plan{plan64}
		}, "the plan's block cache is 67108864 bytes, not 256 MiB"},
		{"a cache of 64 MiB in the results", func(r []runner.TimedRead, _ *runner.ReadTimingOptions) {
			r[2].Results.Describe["cache_bytes"] = "67108864"
		}, "the results' block cache is"},
		{"30 timed asks", func(r []runner.TimedRead, _ *runner.ReadTimingOptions) {
			r[1].Timings.Reps = 30
			for i := range r[1].Timings.Queries {
				r[1].Timings.Queries[i].Ns = r[1].Timings.Queries[i].Ns[:30]
			}
		}, "30 timed asks and 1 readers, not 3, 31 and 1"},
		{"2 warm-up asks", func(r []runner.TimedRead, _ *runner.ReadTimingOptions) { r[1].Timings.Warmup = 2 }, "2 warm-up"},
		{"2 readers", func(r []runner.TimedRead, _ *runner.ReadTimingOptions) { r[1].Timings.Readers = 2 }, "2 readers"},
		{"a different GOMAXPROCS", func(r []runner.TimedRead, _ *runner.ReadTimingOptions) { r[1].Timings.GOMAXPROCS = 2 }, "different GOMAXPROCS"},
		{"GOGC set", func(r []runner.TimedRead, _ *runner.ReadTimingOptions) { r[1].Timings.GOGC = "off" }, `GOGC was set ("off")`},
		{"a memory limit", func(r []runner.TimedRead, _ *runner.ReadTimingOptions) { r[1].Timings.GoMemLimit = 11 << 30 }, "a memory limit was set"},
		{"a wrong answer in the results", func(r []runner.TimedRead, _ *runner.ReadTimingOptions) { r[1].Results.Mismatches = []string{"q"} }, "differ from the reference engine's"},
		{"a wrong answer in the timings", func(r []runner.TimedRead, _ *runner.ReadTimingOptions) { r[1].Timings.Queries[4].Mismatch = true }, "differs from the reference engine's"},
		{"fewer jobs than asked", func(r []runner.TimedRead, _ *runner.ReadTimingOptions) {}, "has 5 jobs, fewer than the 6 asked for"},
		{"a base that is no candidate", func(_ []runner.TimedRead, o *runner.ReadTimingOptions) { o.Base = "L/none" }, `-base "L/none" is not among the candidates`},
		{"timings that are not the plan's queries", func(r []runner.TimedRead, _ *runner.ReadTimingOptions) { r[1].Timings.Queries[3].Name = "other" }, "the plan's query is"},
		{"too few queries", func(r []runner.TimedRead, _ *runner.ReadTimingOptions) {
			r[1].Timings.Queries = r[1].Timings.Queries[:5]
		}, "timings of 5 queries"},
		{"a count of durations that is not the asks", func(r []runner.TimedRead, _ *runner.ReadTimingOptions) {
			r[1].Timings.Queries[0].Ns = r[1].Timings.Queries[0].Ns[:30]
		}, "has 30 durations, not 31"},
		{"a negative duration", func(r []runner.TimedRead, _ *runner.ReadTimingOptions) { r[1].Timings.Queries[0].Ns[3] = -1 }, "negative duration"},
		{"a primary cell the plan does not have", func(_ []runner.TimedRead, o *runner.ReadTimingOptions) { o.Primary = []string{"nothing@now"} }, `-primary "nothing@now" names no cell`},
		{"no jobs", func(r []runner.TimedRead, _ *runner.ReadTimingOptions) {}, "no timed read found"},
	}
	for _, c := range cases {
		reads, o := rtWorld(t)
		o.Jobs = 5
		switch c.name {
		case "fewer jobs than asked":
			o.Jobs = 6
		case "no jobs":
			reads = nil
		}
		o.Plans = slices.Clone(o0.Plans)
		c.mutate(reads, &o)
		why := runner.CheckReadTimings(reads, o)
		if !slices.ContainsFunc(why, func(s string) bool { return strings.Contains(s, c.want) }) {
			t.Errorf("%s: reasons %q, want one with %q", c.name, why, c.want)
		}
		if _, err := runner.JudgeReadTimings(reads, o); err == nil {
			t.Errorf("%s: judged", c.name)
		} else if ref := (*runner.ReadTimingRefusal)(nil); !errors.As(err, &ref) || len(ref.Reasons) == 0 {
			t.Errorf("%s: err = %T %v, want a refusal", c.name, err, err)
		}
	}
	// And the world that none of them mutates is judged.
	reads, o := rtWorld(t)
	if why := runner.CheckReadTimings(reads, o); len(why) > 0 {
		t.Errorf("a clean world is refused: %q", why)
	}
}

// A cell is not reported where a candidate has fewer than three counted jobs, or where the
// CPU models cannot be compared; it is named, the others are reported.
func TestReadTimingNotReportedCells(t *testing.T) {
	t.Parallel()

	_, o := rtWorld(t)
	p := o.Plans[0]
	d, _ := p.Digest()

	// 1. M has five jobs, three of them partial in the hot cell at "now": two count.
	var reads []runner.TimedRead
	for rep := 1; rep <= 5; rep++ {
		reads = append(reads, rtRead(p, d, rtJobSpec{cand: rtBase, arch: "x86_64", cpu: "Test X86 A", rep: rep, scale: 1000}))
		spec := rtJobSpec{cand: rtM, arch: "x86_64", cpu: "Test X86 A", rep: rep, scale: 2000}
		if rep <= 3 {
			spec.notWarm = []int{1}
		}
		reads = append(reads, rtRead(p, d, spec))
	}
	rep := judge(t, reads, o)
	hot := findCell(t, rep.Arches[0], rtHot, "now")
	if len(hot.Scopes) != 1 || !strings.Contains(hot.Scopes[0].NotReported, "M/crdb1 has 2 counted jobs, fewer than 3") {
		t.Errorf("hot cell scopes: %+v", hot.Scopes)
	}
	if len(rep.NotReported) != 1 || !strings.Contains(rep.NotReported[0], rtHot+" @ now") {
		t.Errorf("not reported: %q", rep.NotReported)
	}
	// The other cells are reported all the same.
	if med := findCell(t, rep.Arches[0], rtMed, "now"); med.Scopes[0].NotReported != "" {
		t.Errorf("an unaffected cell is not reported: %+v", med.Scopes[0])
	}
	var out bytes.Buffer
	runner.WriteReadTimings(&out, rep)
	if !strings.Contains(out.String(), "NOT REPORTED (1):") {
		t.Errorf("the text does not list what is not reported:\n%s", out.String())
	}

	// 2. Two CPU models and two jobs of each candidate: not enough to pool, and one job
	// of a candidate on each model: nothing to compare within a model.
	reads = nil
	for i, cpu := range []string{"Test X86 A", "Test X86 B"} {
		reads = append(reads, rtRead(p, d, rtJobSpec{cand: rtBase, arch: "x86_64", cpu: cpu, rep: i + 1, scale: 1000}))
		reads = append(reads, rtRead(p, d, rtJobSpec{cand: rtM, arch: "x86_64", cpu: cpu, rep: i + 1, scale: 2000}))
	}
	two := runner.ReadTimingOptions{Commit: o.Commit, Plans: o.Plans, Base: o.Base, Jobs: 2, Primary: o.Primary}
	rep = judge(t, reads, two)
	if len(rep.NotReported) == 0 {
		t.Fatal("cells of two jobs on two CPU models are reported")
	}
	for _, s := range rep.Arches[0].Cells[0].Scopes {
		if s.NotReported == "" {
			t.Errorf("scope %q of two jobs is reported", s.Scope)
		}
	}

	// 3. With three jobs of each candidate over two models they are pooled and reported.
	reads = nil
	for rep := 1; rep <= 3; rep++ {
		cpu := []string{"Test X86 A", "Test X86 A", "Test X86 B"}[rep-1]
		reads = append(reads, rtRead(p, d, rtJobSpec{cand: rtBase, arch: "x86_64", cpu: cpu, rep: rep, scale: 1000}))
		reads = append(reads, rtRead(p, d, rtJobSpec{cand: rtM, arch: "x86_64", cpu: cpu, rep: rep, scale: 2000}))
	}
	three := runner.ReadTimingOptions{Commit: o.Commit, Plans: o.Plans, Base: o.Base, Jobs: 3, Primary: o.Primary}
	rep = judge(t, reads, three)
	if len(rep.NotReported) != 0 {
		t.Errorf("not reported: %q", rep.NotReported)
	}
	if got := rep.Arches[0].Cells[0].Scopes[0].Scope; got != "pooled over Test X86 A, Test X86 B" {
		t.Errorf("scope %q", got)
	}
}

// The loader reads the artifacts of the bench workflow and refuses files that are not of
// one job.
func TestLoadReadTimings(t *testing.T) {
	t.Parallel()

	// manifestCache is the block cache the build's manifest describes; "-" leaves the key out.
	good := fmt.Sprint(runner.ReadTimingCacheBytes)
	write := func(t *testing.T, manifestCache string, mutate func(dir string, b *fixtureBuild, tm *runner.ReadTimings, res *runner.Results)) (string, error) {
		t.Helper()
		root := t.TempDir()
		b := fixtureBuild{Window: 7, Candidate: rtBase, Records: 1000}
		fb := b.filled()
		dir := writeArtifact(t, root, b)
		// The manifest of a Pebble build describes its block cache.
		m := fb.manifest()
		if manifestCache != "-" {
			m.Describe["cache_bytes"] = manifestCache
		}
		writeJSON(t, filepath.Join(dir, runner.ManifestFile), m)
		raw, err := os.ReadFile(filepath.Join(dir, runner.ManifestFile))
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(raw)
		md := hex.EncodeToString(sum[:])
		res := &runner.Results{Candidate: rtBase, PlanDigest: fb.planDigest(), ManifestDigest: md, Describe: map[string]string{"cache_bytes": fmt.Sprint(runner.ReadTimingCacheBytes)}}
		tm := &runner.ReadTimings{
			Version: runner.ReadTimingsVersion, Candidate: rtBase, PlanDigest: fb.planDigest(), ManifestDigest: md,
			Build: runner.BuildInfo{Executable: fb.Executable, Revision: fb.Revision}, GoMemLimit: math.MaxInt64,
			CacheBytes: runner.ReadTimingCacheBytes,
		}
		if mutate != nil {
			mutate(dir, &b, tm, res)
		}
		writeJSON(t, filepath.Join(dir, runner.ResultsFile), res)
		writeJSON(t, filepath.Join(dir, runner.TimingsFile), tm)
		reads, err := runner.LoadReadTimings([]string{root})
		if err == nil && (len(reads) != 1 || reads[0].Job.Candidate != rtBase || reads[0].Timings.Candidate != rtBase || reads[0].Results == nil) {
			t.Fatalf("loaded %+v", reads)
		}
		return dir, err
	}
	if _, err := write(t, good, nil); err != nil {
		t.Fatalf("a good artifact: %v", err)
	}
	// The build's manifest at another cache, or without one, is not a build the read belongs to.
	for _, mc := range []string{"67108864", "-"} {
		if _, err := write(t, mc, nil); err == nil || !strings.Contains(err.Error(), "another block cache than the build's") {
			t.Errorf("a manifest with cache %q: err = %v", mc, err)
		}
	}
	for _, c := range []struct {
		name   string
		mutate func(dir string, b *fixtureBuild, tm *runner.ReadTimings, res *runner.Results)
		want   string
	}{
		{"no job file", func(dir string, _ *fixtureBuild, _ *runner.ReadTimings, _ *runner.Results) {
			_ = os.Remove(filepath.Join(dir, runner.JobFile))
		}, "not an artifact of the bench workflow"},
		{"another candidate", func(_ string, _ *fixtureBuild, tm *runner.ReadTimings, _ *runner.Results) { tm.Candidate = rtM }, "candidate"},
		{"another plan", func(_ string, _ *fixtureBuild, tm *runner.ReadTimings, _ *runner.Results) { tm.PlanDigest = "x" }, "not of the plan"},
		{"another manifest", func(_ string, _ *fixtureBuild, tm *runner.ReadTimings, _ *runner.Results) { tm.ManifestDigest = "x" }, "not read from the manifest"},
		{"another executable", func(_ string, _ *fixtureBuild, tm *runner.ReadTimings, _ *runner.Results) {
			tm.Build.Executable = strings.Repeat("7", 64)
		}, "executable"},
		{"another revision", func(_ string, _ *fixtureBuild, tm *runner.ReadTimings, _ *runner.Results) {
			tm.Build.Revision = strings.Repeat("7", 40)
		}, "revision"},
		{"another version", func(_ string, _ *fixtureBuild, tm *runner.ReadTimings, _ *runner.Results) { tm.Version = 2 }, "version 2"},
		{"read at another cache", func(_ string, _ *fixtureBuild, tm *runner.ReadTimings, _ *runner.Results) { tm.CacheBytes = 64 << 20 }, "another block cache than the build's"},
		{"results of another manifest", func(_ string, _ *fixtureBuild, _ *runner.ReadTimings, res *runner.Results) { res.ManifestDigest = "x" }, "another manifest"},
	} {
		if _, err := write(t, good, c.mutate); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: err = %v, want one with %q", c.name, err, c.want)
		}
	}
}

// A job on a CPU model that no reported scope holds is named in the print and the JSON.
func TestReadTimingNamesJobsOutsideEveryReportedScope(t *testing.T) {
	t.Parallel()

	_, o := rtWorld(t)
	p := o.Plans[0]
	d, _ := p.Digest()
	var reads []runner.TimedRead
	for rep := 1; rep <= 3; rep++ {
		reads = append(reads, rtRead(p, d, rtJobSpec{cand: rtBase, arch: "x86_64", cpu: "Test X86 A", rep: rep, scale: 1000}))
		reads = append(reads, rtRead(p, d, rtJobSpec{cand: rtM, arch: "x86_64", cpu: "Test X86 A", rep: rep, scale: 2000}))
	}
	// Two jobs on another model, one of them partial in the hot cell: one job of the base
	// alone, so that no scope for that model holds both candidates.
	reads = append(reads, rtRead(p, d, rtJobSpec{cand: rtBase, arch: "x86_64", cpu: "Test X86 B", rep: 4, scale: 1000, notWarm: []int{1}}))
	three := runner.ReadTimingOptions{Commit: o.Commit, Plans: o.Plans, Base: o.Base, Jobs: 3, Primary: o.Primary}
	rep := judge(t, reads, three)
	hot := findCell(t, rep.Arches[0], rtHot, "now")
	if want := []string{rtBase + " rep 4 on Test X86 B (partial)"}; !slices.Equal(hot.Outside, want) {
		t.Fatalf("outside every reported scope: %q, want %q", hot.Outside, want)
	}
	var out bytes.Buffer
	runner.WriteReadTimings(&out, rep)
	if !strings.Contains(out.String(), "outside every reported scope: "+rtBase+" rep 4 on Test X86 B (partial)") {
		t.Errorf("the print does not name the job:\n%s", out.String())
	}
}

func stripCollections(r *runner.ReadTimingReport) {
	for a := range r.Arches {
		for c := range r.Arches[a].Cells {
			for s := range r.Arches[a].Cells[c].Scopes {
				for k := range r.Arches[a].Cells[c].Scopes[s].Candidates {
					cand := &r.Arches[a].Cells[c].Scopes[s].Candidates[k]
					cand.CollectionsByQuery = nil
					for _, js := range [][]runner.ReadTimingJob{cand.Jobs, cand.Partial} {
						for j := range js {
							js[j].Collections = 0
						}
					}
				}
			}
		}
	}
}

// The collections counted are shown, per job and per query, and are a diagnostic only:
// the same reads with other counts have the same values, ratios, spreads, cells not
// reported and refusals; only the column differs.
func TestReadTimingShowsCollectionsCountedAndUsesThemForNothing(t *testing.T) {
	t.Parallel()

	reads, o := rtWorld(t)
	rep := judge(t, reads, o)
	var out bytes.Buffer
	runner.WriteReadTimings(&out, rep)
	text := out.String()
	// Query 1 of the hot cell is the one with collections, in reps 1, 3 and 5 of each candidate.
	hot := findCell(t, rep.Arches[0], rtHot, "now")
	base := findCand(t, hot.Scopes[0], rtBase)
	var perJob []uint32
	for _, j := range base.Jobs {
		perJob = append(perJob, j.Collections)
	}
	if want := []uint32{1, 0, 1, 0, 1}; !slices.Equal(perJob, want) {
		t.Errorf("collections per job: %v, want %v", perJob, want)
	}
	var q1 runner.ReadTimingQueryGC
	for _, g := range base.CollectionsByQuery {
		if strings.Contains(g.Query, "#1 ") {
			q1 = g
		}
	}
	if !slices.Equal(q1.PerJob, []uint32{1, 0, 1, 0, 1}) || len(base.CollectionsByQuery) != 3 {
		t.Errorf("collections by query: %+v", base.CollectionsByQuery)
	}
	if !strings.Contains(text, "collections counted (diagnostic) per job [1 0 1 0 1]") || !strings.Contains(text, "collections counted, "+rtHot+" #1 now: [1 0 1 0 1]") {
		t.Errorf("the print lacks the collections column:\n%s", text)
	}
	if j, _ := json.Marshal(rep); !strings.Contains(string(j), `"CollectionsByQuery"`) {
		t.Error("the JSON lacks the collections")
	}

	// Another count in every query of every job changes the column and nothing else.
	reads2, o2 := rtWorld(t)
	for _, r := range reads2 {
		for i := range r.Timings.Queries {
			r.Timings.Queries[i].GCs = uint32(50 + 7*i)
		}
	}
	if why := runner.CheckReadTimings(reads2, o2); len(why) > 0 {
		t.Errorf("collections caused a refusal: %q", why)
	}
	rep2 := judge(t, reads2, o2)
	if a, b := rep, rep2; len(a.NotReported) != len(b.NotReported) || a.Noise != b.Noise {
		t.Errorf("collections moved the noise or the cells not reported")
	}
	rep, rep2 = judgeStripped(t, reads, o), judgeStripped(t, reads2, o2)
	ja, _ := json.Marshal(rep)
	jb, _ := json.Marshal(rep2)
	if !bytes.Equal(ja, jb) {
		t.Errorf("collections changed more than their column:\n%s\n%s", ja, jb)
	}
}

func judgeStripped(t *testing.T, reads []runner.TimedRead, o runner.ReadTimingOptions) *runner.ReadTimingReport {
	t.Helper()
	r := judge(t, reads, o)
	stripCollections(r)
	return r
}

// An architecture with jobs outside every scope says how many, and on which models, in one
// line; an architecture with none says nothing; the exit status is not touched.
func TestReadTimingSummarisesJobsOutsideEveryScopePerArchitecture(t *testing.T) {
	t.Parallel()

	_, o := rtWorld(t)
	p := o.Plans[0]
	d, _ := p.Digest()
	arm := strings.Repeat("5", 64)
	var reads []runner.TimedRead
	for rep := 1; rep <= 3; rep++ {
		for _, c := range []struct {
			name  string
			scale int64
		}{{rtBase, 1000}, {rtM, 2000}} {
			reads = append(reads, rtRead(p, d, rtJobSpec{cand: c.name, arch: "x86_64", cpu: "Test X86 A", rep: rep, scale: c.scale}))
			reads = append(reads, rtRead(p, d, rtJobSpec{cand: c.name, arch: "aarch64", cpu: "Test ARM A", rep: rep, scale: c.scale, exec: arm}))
		}
	}
	// Two jobs on two other models, on x86_64 only, partial in the hot cell at "now": they
	// are outside every reported scope there, and in the other cells they are pooled.
	reads = append(reads, rtRead(p, d, rtJobSpec{cand: rtBase, arch: "x86_64", cpu: "Test X86 B", rep: 4, scale: 1000, notWarm: []int{1}}))
	reads = append(reads, rtRead(p, d, rtJobSpec{cand: rtM, arch: "x86_64", cpu: "Test X86 C", rep: 4, scale: 2000, notWarm: []int{1}}))
	three := runner.ReadTimingOptions{Commit: o.Commit, Plans: o.Plans, Base: o.Base, Jobs: 3, Primary: o.Primary}
	rep := judge(t, reads, three)
	if len(rep.NotReported) != 0 {
		t.Fatalf("not reported: %q", rep.NotReported)
	}
	var x86, aarch64 runner.ReadTimingArch
	for _, a := range rep.Arches {
		if a.Arch == "x86_64" {
			x86 = a
		} else {
			aarch64 = a
		}
	}
	if x86.OutsideJobs != 2 || !slices.Equal(x86.OutsideModels, []string{"Test X86 B", "Test X86 C"}) || aarch64.OutsideJobs != 0 {
		t.Errorf("outside: x86_64 %d %v, aarch64 %d", x86.OutsideJobs, x86.OutsideModels, aarch64.OutsideJobs)
	}
	var out bytes.Buffer
	runner.WriteReadTimings(&out, rep)
	text := out.String()
	if strings.Count(text, "jobs outside every scope") != 1 || !strings.Contains(text, "  2 jobs outside every scope (models Test X86 B, Test X86 C)\n") {
		t.Errorf("the summary lines:\n%s", text)
	}
	// The line is under its architecture's header, before the cells.
	at, hdr := strings.Index(text, "jobs outside every scope"), strings.Index(text, "architecture x86_64")
	cells := hdr + strings.Index(text[hdr:], "PRIMARY CELLS")
	if hdr >= at || at >= cells {
		t.Errorf("the line is out of place (%d, %d, %d):\n%s", hdr, at, cells, text)
	}
}
