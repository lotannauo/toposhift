package runner_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/candidates"
	"github.com/lotannauo/toposhift/bench/spike/runner"
)

const (
	gib = 1 << 30
)

func hist(n int, d time.Duration) runner.Histogram {
	var h runner.Histogram
	for range n {
		h.Add(d)
	}
	return h
}

// histOf is a histogram of the durations, one batch each.
func histOf(ds ...time.Duration) runner.Histogram {
	var h runner.Histogram
	for _, d := range ds {
		h.Add(d)
	}
	return h
}

// synth is a build that passes every row: one window of ten seconds in a stream of a
// thousand, 4000 records a second written inside it and about 3000 a second outside,
// 30 days kept, a live heap of 1 GiB and a resident set of 2 GiB, in a mode that
// writes beside the retention (a synchronous build writes none inside).
func synth(cand string, mods ...func(*runner.StoreGateBuild)) *runner.StoreGateBuild {
	b := &runner.StoreGateBuild{
		Dir: "/builds/" + strings.ReplaceAll(cand, "/", "_"),
		Manifest: &runner.Manifest{
			Candidate: cand, Layout: "L", PlanDigest: "plan",
			Build: runner.BuildInfo{GOOS: "linux", GOARCH: "arm64"},
			Describe: map[string]string{
				runner.GoGCKey: "100", runner.GoMemoryLimitKey: "11811160064", runner.RetentionModeKey: "background",
			},
			Gate: &runner.GateTrace{
				Mode:    "background",
				Windows: []runner.GateWindow{{StartNs: 0, RewriteEndNs: 10e9, SettleEndNs: 12e9, ReturnNs: 0}},
				Inside:  runner.GateBatches{Batches: 100, Records: 40000, Commit: hist(100, 30*time.Millisecond)},
				Outside: runner.GateBatches{Batches: 1000, Records: 3_000_000, Commit: hist(1000, 20*time.Millisecond)},
				WriteNs: 1000e9, KeepNs: int64(30 * 24 * time.Hour), PeakRSS: 2 * gib,
				Maxima: map[string]int64{"retain.max_prefix_records": 123, "retain.chunk_hold_ns": int64(40 * time.Millisecond)},
			},
		},
		Metrics: runner.StoreGateMetrics{Lines: 5, HeapLiveSamples: 5, HeapLivePeak: gib},
	}
	for _, m := range mods {
		m(b)
	}
	return b
}

func trace(f func(*runner.GateTrace)) func(*runner.StoreGateBuild) {
	return func(b *runner.StoreGateBuild) { f(b.Manifest.Gate) }
}

// find returns the row of the candidate whose gate starts with q.
func find(t *testing.T, rep *runner.StoreGateReport, cand, q string) runner.StoreGateRow {
	t.Helper()
	for _, r := range rep.Rows {
		if r.Candidate == cand && strings.HasPrefix(r.Gate, q) {
			return r
		}
	}
	t.Fatalf("no row %s %s in %+v", cand, q, rep.Rows)
	return runner.StoreGateRow{}
}

func states(t *testing.T, builds []*runner.StoreGateBuild, cand string) map[string]string {
	t.Helper()
	rep := runner.JudgeStoreGate(builds)
	out := map[string]string{}
	for _, q := range []string{"Q1", "Q2", "Q3", "Q4", "Q5 peak live", "Q5 peak res"} {
		out[q] = find(t, rep, cand, q).State
	}
	return out
}

// A build that is within every limit passes every row; the baseline shows as one.
func TestAStoreWithinEveryLimitPassesEveryRow(t *testing.T) {
	t.Parallel()

	rep := runner.JudgeStoreGate([]*runner.StoreGateBuild{synth("Lroot/off"), synth("Lroot/k64a2l1ns")})
	for _, q := range []string{"Q1", "Q2", "Q3", "Q4", "Q5 peak res"} {
		for _, c := range []string{"Lroot/off", "Lroot/k64a2l1ns"} {
			if r := find(t, rep, c, q); r.State != runner.GateOK {
				t.Errorf("%s %s: %+v, want ok", c, q, r)
			}
		}
	}
	if r := find(t, rep, "Lroot/off", "Q5 peak live"); r.State != runner.GateBaseline {
		t.Errorf("the baseline's heap: %+v", r)
	}
	if r := find(t, rep, "Lroot/k64a2l1ns", "Q5 peak live"); r.State != runner.GateOK {
		t.Errorf("the heap against the baseline: %+v", r)
	}
	if len(rep.NotJudged) != 0 || rep.Over != 0 {
		t.Errorf("not judged %v, over %d", rep.NotJudged, rep.Over)
	}
}

// Q1 is the longest batch inside a window, at most 250 ms: at the limit it passes and
// a nanosecond over it does not, and the longest batch outside the windows is shown as
// the control (a verdict that dropped it would show the store's slowness without what
// the machine does anyway).
func TestQ1IsTheLongestBatchInsideAWindowWithTheControl(t *testing.T) {
	t.Parallel()

	for _, c := range []struct {
		longest time.Duration
		want    string
	}{
		{runner.StoreGateQ1MaxWait - time.Nanosecond, runner.GateOK},
		{runner.StoreGateQ1MaxWait, runner.GateOK},
		{runner.StoreGateQ1MaxWait + time.Nanosecond, runner.GateOver},
		{time.Second, runner.GateOver},
	} {
		b := synth("Lroot/k64a2l1ns", trace(func(g *runner.GateTrace) {
			g.Inside.Commit = histOf(10*time.Millisecond, c.longest)
			g.Outside.Commit = histOf(7*time.Millisecond, 91*time.Millisecond)
		}))
		r := find(t, runner.JudgeStoreGate([]*runner.StoreGateBuild{b}), "Lroot/k64a2l1ns", "Q1")
		if r.State != c.want {
			t.Errorf("longest %s: %+v, want %s", c.longest, r, c.want)
		}
		if !strings.Contains(r.Value, "control") || !strings.Contains(r.Value, "91ms") {
			t.Errorf("longest %s: the value %q does not show the control, the longest outside (91ms)", c.longest, r.Value)
		}
		if !strings.Contains(r.Limit, "250ms") {
			t.Errorf("limit %q", r.Limit)
		}
	}
}

// Q2 is the 99th percentile inside against the larger of twice the one outside and
// 50 ms. Each branch of the maximum decides in a case of its own, so that a judge that
// used only the factor, or only the floor, is told apart: with a quick outside the
// floor is the limit (a factor alone would condemn 30 ms against 2 ms), and with a slow
// outside the factor is (a floor alone would condemn 150 ms against 100 ms). The inside
// is read at the top of its bucket and the outside at the bottom of its: an inside of
// 2.1 times an outside, both in buckets next to the limit, is over, though the tops of
// the buckets (what a quantile of the histogram returns) would let it pass.
func TestQ2IsTheLargerOfTwiceTheOutsideAndTheFloor(t *testing.T) {
	t.Parallel()

	// 100 ms is in the bucket from 92274688 ns to 100663296 ns. Twice a bucket bound is
	// a bucket bound, so the limit, twice the bottom of that bucket, can be met to the
	// bucket: a value in the bucket just below the bound is at it, one above is over.
	const bottom = 92274688
	outside := histOf(100 * time.Millisecond)
	twice := 2 * time.Duration(bottom)
	for _, c := range []struct {
		name            string
		inside, outside runner.Histogram
		want            string
	}{
		{"quick outside, inside under the floor", hist(100, 30*time.Millisecond), hist(1000, 2*time.Millisecond), runner.GateOK},
		{"quick outside, inside over the floor", hist(100, 60*time.Millisecond), hist(1000, 2*time.Millisecond), runner.GateOver},
		{"slow outside, inside over the floor and within twice", hist(100, 150*time.Millisecond), outside, runner.GateOK},
		{"slow outside, inside at twice (the bucket bound)", histOf(twice - time.Nanosecond), outside, runner.GateOK},
		{"slow outside, inside over twice", histOf(twice), outside, runner.GateOver},
		// 95 ms and 199.5 ms are in the buckets [92.27, 100.66) and [184.5, 201.3): the tops
		// are 100.66 and 201.3, which would pass at twice; the values are 2.1 times apart
		{"2.1 times the outside, both in the buckets next to the limit", histOf(199500 * time.Microsecond), histOf(95 * time.Millisecond), runner.GateOver},
		{"1.6 times the outside", histOf(152 * time.Millisecond), histOf(95 * time.Millisecond), runner.GateOK},
		{"slow outside, inside far over", hist(100, time.Second), outside, runner.GateOver},
	} {
		b := synth("Lroot/k64a2l1ns", trace(func(g *runner.GateTrace) { g.Inside.Commit, g.Outside.Commit = c.inside, c.outside }))
		r := find(t, runner.JudgeStoreGate([]*runner.StoreGateBuild{b}), "Lroot/k64a2l1ns", "Q2")
		if r.State != c.want {
			t.Errorf("%s: %+v, want %s", c.name, r, c.want)
		}
	}
}

// Q3 is the longest rewrite, from the publication to the end of the rewrite, within 2 h:
// not to the end of the settle, nor to the return of the call (a rewrite that ended in
// an hour and fifty-nine minutes and was settled for another half hour is within).
func TestQ3IsMeasuredToTheEndOfTheRewrite(t *testing.T) {
	t.Parallel()

	const h = int64(time.Hour)
	// Outside the windows the builder writes 3000 records a second exactly: the stream
	// takes 1000 h, the window is away for its own length, and the records are 3000 times
	// the seconds that remain.
	at := func(rewrite, settle, ret int64, records int64) func(*runner.GateTrace) {
		return func(g *runner.GateTrace) {
			g.Windows = []runner.GateWindow{{StartNs: 0, RewriteEndNs: rewrite, SettleEndNs: settle, ReturnNs: ret}}
			g.WriteNs = 1000 * h
			g.Inside = runner.GateBatches{}
			g.Outside.Records = records
		}
	}
	secondsOutside := func(away int64) int64 { return (1000*h - away) / 1e9 }
	for _, c := range []struct {
		name string
		f    func(*runner.GateTrace)
		want string
	}{
		{"just under", at(2*h-1, 2*h, 2*h, 3000*secondsOutside(2*h)+3000), runner.GateOK},
		{"at the limit", at(2*h, 2*h, 2*h, 3000*secondsOutside(2*h)+3000), runner.GateOK},
		{"just over", at(2*h+1, 2*h+1, 2*h+1, 3000*secondsOutside(2*h+1)+3000), runner.GateOver},
		// the mutant that took the end of the settle, or the return of the call, would say over
		{"rewrite within, settle and call beyond", at(h*119/60, h*150/60, h*150/60, 3000*secondsOutside(h*150/60)+3000), runner.GateOK},
		{"rewrite beyond, call back at once", at(3*h, 3*h, 0, 3000*secondsOutside(3*h)+3000), runner.GateOver},
	} {
		b := synth("Lroot/k64a2l1ns", trace(c.f))
		r := find(t, runner.JudgeStoreGate([]*runner.StoreGateBuild{b}), "Lroot/k64a2l1ns", "Q3")
		if r.State != c.want {
			t.Errorf("%s: %+v, want %s", c.name, r, c.want)
		}
	}
}

// A load below the 3000 records a second the rule is judged at is not judged, even when
// the rewrite is quick (a rewrite is not shown quick by writing slowly beside it), and
// is listed as not judged; a build that keeps fewer than 30 days is shown and not
// judged; one with no batch outside the windows has no load to judge by.
func TestQ3IsNotJudgedAtALowLoadOrFewDaysKept(t *testing.T) {
	t.Parallel()

	const h = int64(time.Hour)
	load := func(records int64) func(*runner.GateTrace) {
		return func(g *runner.GateTrace) {
			g.Windows = []runner.GateWindow{{StartNs: 0, RewriteEndNs: h, SettleEndNs: h, ReturnNs: h}}
			g.WriteNs = 1000 * h
			g.Inside = runner.GateBatches{}
			g.Outside.Records = records
		}
	}
	seconds := (1000*h - h) / 1e9
	for _, c := range []struct {
		name string
		f    func(*runner.GateTrace)
		want string
		why  string
	}{
		{"at the level", load(3000 * seconds), runner.GateOK, ""},
		{"below the level", load(3000*seconds - 1), runner.GateNotJudged, "below the 3000 records/s"},
		{"no records outside", load(0), runner.GateNotJudged, "load outside the windows cannot be worked out"},
		// the builder writes beside a settle as it does anywhere outside a window
		{"the load written in a settle", func(g *runner.GateTrace) { load(0)(g); g.Settling.Records = 3000 * seconds }, runner.GateOK, ""},
		{"29 days kept", func(g *runner.GateTrace) { load(3000 * seconds)(g); g.KeepNs = int64(29 * 24 * time.Hour) }, runner.GateNotJudged, "30 days kept"},
		{"31 days kept", func(g *runner.GateTrace) { load(3000 * seconds)(g); g.KeepNs = int64(31 * 24 * time.Hour) }, runner.GateOK, ""},
	} {
		b := synth("Lroot/k64a2l1ns", trace(c.f))
		rep := runner.JudgeStoreGate([]*runner.StoreGateBuild{b})
		r := find(t, rep, "Lroot/k64a2l1ns", "Q3")
		if r.State != c.want {
			t.Errorf("%s: %+v, want %s", c.name, r, c.want)
		}
		listed := ""
		for _, s := range rep.NotJudged {
			if strings.Contains(s, "Q3") {
				listed = s
			}
		}
		if (c.why == "") != (listed == "") || !strings.Contains(listed, c.why) {
			t.Errorf("%s: the list of what was not judged has %q for Q3, want it to say %q", c.name, listed, c.why)
		}
		if c.want == runner.GateNotJudged && !strings.Contains(r.Value, "records/s") && c.name != "no records outside" {
			t.Errorf("%s: the value %q does not show the load", c.name, r.Value)
		}
	}
}

// The load that Q3 is judged at leaves out what the builder did not spend writing: the
// time it waited for the database to rest, and the time it was away in a retention
// that did not return until it was done.
func TestTheLoadLeavesOutRestsAndTimeAwayInARetention(t *testing.T) {
	t.Parallel()

	const h = int64(time.Hour)
	// 1000 h of stream, 100 h of rest, a window of 1 h whose call returned after 5 h:
	// 895 h of writing.
	b := synth("Lroot/k64a2l1ns", trace(func(g *runner.GateTrace) {
		g.Windows = []runner.GateWindow{{StartNs: 0, RewriteEndNs: h, SettleEndNs: 5 * h, ReturnNs: 5 * h}}
		g.WriteNs, g.RestNs = 1000*h, 100*h
		g.Inside = runner.GateBatches{}
		g.Outside.Records = 3000 * (895 * h / 1e9)
	}))
	r := find(t, runner.JudgeStoreGate([]*runner.StoreGateBuild{b}), "Lroot/k64a2l1ns", "Q3")
	if r.State != runner.GateOK || !strings.Contains(r.Value, "load 3000 records/s") {
		t.Errorf("%+v, want ok at a load of 3000 records/s", r)
	}
}

// Q4 is the records written inside windows over the windows' total time, at least
// 3000 a second: at it passes and a record under does not.
func TestQ4IsTheThroughputInsideWindows(t *testing.T) {
	t.Parallel()

	for _, c := range []struct {
		records int64
		want    string
	}{
		{30000 + 1, runner.GateOK}, {30000, runner.GateOK}, {30000 - 1, runner.GateOver}, {0, runner.GateOver},
	} {
		b := synth("Lroot/k64a2l1ns", trace(func(g *runner.GateTrace) { g.Inside.Records = c.records })) // a window of 10 s
		r := find(t, runner.JudgeStoreGate([]*runner.StoreGateBuild{b}), "Lroot/k64a2l1ns", "Q4")
		if r.State != c.want {
			t.Errorf("%d records inside 10 s: %+v, want %s", c.records, r, c.want)
		}
	}
}

// Q5 is the peak live heap against Lroot/off's on the same plan, plus 512 MB, and the
// peak resident set against 14 GB (decimal: a value between the readings in MB and in
// MiB, or in GB and in GiB, is over), as two rows that do not stand in for each other: a
// build over the heap limit with a small resident set, and one over the ceiling with a
// small heap, are each over on their own row only.
func TestQ5IsTheLiveHeapAgainstTheBaselineAndTheResidentSetAgainstACeiling(t *testing.T) {
	t.Parallel()

	off := synth("Lroot/off")
	for _, c := range []struct {
		name      string
		heap, rss int64
		heapIs    string
		rssIs     string
	}{
		{"both within", gib + 512_000_000, 14_000_000_000, runner.GateOK, runner.GateOK},
		{"heap just over", gib + 512_000_000 + 1, 2 * gib, runner.GateOver, runner.GateOK},
		{"resident set just over", gib, 14_000_000_000 + 1, runner.GateOK, runner.GateOver},
		// 520 MB is more than 512 MB and less than 512 MiB (536.9 MB): a limit computed in MiB
		// would pass it; 14.5 GB is more than 14 GB and less than 14 GiB (15.0 GB)
		{"heap between 512 MB and 512 MiB over the baseline", gib + 520_000_000, 2 * gib, runner.GateOver, runner.GateOK},
		{"resident set between 14 GB and 14 GiB", gib, 14_500_000_000, runner.GateOK, runner.GateOver},
		{"heap far over, resident set small", 8 * gib, gib, runner.GateOver, runner.GateOK},
		{"heap small, resident set far over", gib / 2, 20 * gib, runner.GateOK, runner.GateOver},
	} {
		b := synth("Lroot/k64a2l1ns", trace(func(g *runner.GateTrace) { g.PeakRSS = c.rss }), func(b *runner.StoreGateBuild) { b.Metrics.HeapLivePeak = c.heap })
		rep := runner.JudgeStoreGate([]*runner.StoreGateBuild{off, b})
		if r := find(t, rep, "Lroot/k64a2l1ns", "Q5 peak live"); r.State != c.heapIs {
			t.Errorf("%s: heap %+v, want %s", c.name, r, c.heapIs)
		}
		if r := find(t, rep, "Lroot/k64a2l1ns", "Q5 peak res"); r.State != c.rssIs {
			t.Errorf("%s: resident set %+v, want %s", c.name, r, c.rssIs)
		}
	}
}

// The live heaps of two builds are compared only if they ran under the same GC target
// and the same memory limit, and the target was recorded in both; otherwise the heap row
// is not judged and the list says which setting differs. The resident set, which is
// judged against a ceiling and not against the other build, is unaffected.
func TestBuildsUnderDifferentGoSettingsAreNotComparedOnTheirHeap(t *testing.T) {
	t.Parallel()

	for _, c := range []struct {
		name string
		mod  func(*runner.StoreGateBuild)
		why  string
	}{
		{"another GC target", func(b *runner.StoreGateBuild) { b.Manifest.Describe[runner.GoGCKey] = "50" }, runner.GoGCKey + " differs"},
		{"another memory limit", func(b *runner.StoreGateBuild) { b.Manifest.Describe[runner.GoMemoryLimitKey] = "none" }, runner.GoMemoryLimitKey + " differs"},
		{"a limit against no limit recorded (none)", func(b *runner.StoreGateBuild) { delete(b.Manifest.Describe, runner.GoMemoryLimitKey) }, runner.GoMemoryLimitKey + " differs"},
		{"no GC target recorded", func(b *runner.StoreGateBuild) { delete(b.Manifest.Describe, runner.GoGCKey) }, runner.GoGCKey + " is not recorded"},
	} {
		// the candidate is made far over the heap limit: judged, it would be over
		b := synth("Lroot/k64a2l1ns", c.mod, func(b *runner.StoreGateBuild) { b.Metrics.HeapLivePeak = 9 * gib })
		rep := runner.JudgeStoreGate([]*runner.StoreGateBuild{synth("Lroot/off"), b})
		r := find(t, rep, "Lroot/k64a2l1ns", "Q5 peak live")
		if r.State != runner.GateNotJudged {
			t.Errorf("%s: %+v, want not judged", c.name, r)
		}
		listed := strings.Join(rep.NotJudged, "\n")
		if !strings.Contains(listed, c.why) {
			t.Errorf("%s: the list of what was not judged is %q, want it to say %q", c.name, listed, c.why)
		}
		if rep.Over != 0 {
			t.Errorf("%s: %d rows over", c.name, rep.Over)
		}
		if r := find(t, rep, "Lroot/k64a2l1ns", "Q5 peak res"); r.State != runner.GateOK {
			t.Errorf("%s: the resident set %+v, want ok", c.name, r)
		}
	}

	// a build without live heap samples, and one without a baseline
	rep := runner.JudgeStoreGate([]*runner.StoreGateBuild{synth("Lroot/k64a2l1ns")})
	if r := find(t, rep, "Lroot/k64a2l1ns", "Q5 peak live"); r.State != runner.GateNotJudged || !strings.Contains(strings.Join(rep.NotJudged, "\n"), "no Lroot/off build of the same plan") {
		t.Errorf("without a baseline: %+v, %v", r, rep.NotJudged)
	}
	noSamples := synth("Lroot/k64a2l1ns", func(b *runner.StoreGateBuild) { b.Metrics = runner.StoreGateMetrics{} })
	rep = runner.JudgeStoreGate([]*runner.StoreGateBuild{synth("Lroot/off"), noSamples})
	if r := find(t, rep, "Lroot/k64a2l1ns", "Q5 peak live"); r.State != runner.GateNotJudged || !strings.Contains(strings.Join(rep.NotJudged, "\n"), "-metrics-every") {
		t.Errorf("without samples: %+v, %v", r, rep.NotJudged)
	}
	// another plan, another architecture and another repetition are not the baseline
	other := synth("Lroot/off", func(b *runner.StoreGateBuild) { b.Manifest.PlanDigest = "other" })
	rep = runner.JudgeStoreGate([]*runner.StoreGateBuild{other, synth("Lroot/k64a2l1ns")})
	if r := find(t, rep, "Lroot/k64a2l1ns", "Q5 peak live"); r.State != runner.GateNotJudged {
		t.Errorf("a baseline of another plan: %+v", r)
	}
}

// A build with no batch inside any window has nothing to judge on Q1, Q2 and Q4: they
// are not judged and say why (for a synchronous build, that the writer is blocked for
// the whole window, and for how long), while Q3 and Q5 are judged. A build with no window
// at all, an untimed one, one not through the root store, one of a binary that recorded
// no trace and one whose window never ended are not judged on anything.
func TestWhatCannotBeJudgedIsSaidAndNotGuessed(t *testing.T) {
	t.Parallel()

	sync := synth("Lroot/k64a2l1ns", trace(func(g *runner.GateTrace) {
		g.Mode = "sync"
		g.Inside = runner.GateBatches{}
		g.Windows = []runner.GateWindow{{StartNs: 0, RewriteEndNs: 10e9, SettleEndNs: 12e9, ReturnNs: 13e9}}
	}), func(b *runner.StoreGateBuild) { b.Manifest.Describe[runner.RetentionModeKey] = "sync" })
	rep := runner.JudgeStoreGate([]*runner.StoreGateBuild{synth("Lroot/off"), sync})
	for _, q := range []string{"Q1", "Q2", "Q4"} {
		if r := find(t, rep, "Lroot/k64a2l1ns", q); r.State != runner.GateNotJudged {
			t.Errorf("synchronous %s: %+v, want not judged", q, r)
		}
	}
	if r := find(t, rep, "Lroot/k64a2l1ns", "Q3"); r.State != runner.GateOK {
		t.Errorf("synchronous Q3: %+v", r)
	}
	if rep.Unexpected != 0 {
		t.Errorf("%d rows not judged that the synchronous mode should judge", rep.Unexpected)
	}
	listed := strings.Join(rep.NotJudged, "\n")
	if !strings.Contains(listed, "the writer is blocked for all of it (the longest 13s)") {
		t.Errorf("the list of what was not judged: %s", listed)
	}
	var out strings.Builder
	runner.WriteStoreGate(&out, rep)
	for _, want := range []string{"retention mode: sync, background", "synchronous retention: the writer is blocked", "NOT JUDGED:", "rules digest " + runner.StoreGateRulesDigest()} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("the report lacks %q:\n%s", want, out.String())
		}
	}

	for _, c := range []struct {
		name string
		b    *runner.StoreGateBuild
		why  string
	}{
		{"no window", synth("Lroot/k64a2l1ns", trace(func(g *runner.GateTrace) { g.Windows = nil })), "no retention window"},
		{"untimed", synth("Lroot/k64a2l1ns", func(b *runner.StoreGateBuild) { b.Manifest.Untimed = true }), "untimed"},
		{"not through the root store", synth("L/k64a2l1ns"), "not a build through the root store"},
		{"no trace", synth("Lroot/k64a2l1ns", func(b *runner.StoreGateBuild) { b.Manifest.Gate = nil }), "earlier binary"},
		{"no mode", synth("Lroot/k64a2l1ns", trace(func(g *runner.GateTrace) { g.Mode = "" })), "no retention mode"},
		{"window never ended", synth("Lroot/k64a2l1ns", trace(func(g *runner.GateTrace) { g.Windows[0].RewriteEndNs = 1<<63 - 1 })), "never ended"},
		{"window ends before it starts", synth("Lroot/k64a2l1ns", trace(func(g *runner.GateTrace) { g.Windows[0].StartNs = 11e9 })), "ends before it starts"},
	} {
		rep := runner.JudgeStoreGate([]*runner.StoreGateBuild{c.b})
		if got := states(t, []*runner.StoreGateBuild{c.b}, c.b.Manifest.Candidate); got["Q1"] != runner.GateNotJudged {
			t.Errorf("%s: Q1 is %s", c.name, got["Q1"])
		}
		if !strings.Contains(strings.Join(rep.NotJudged, "\n"), c.why) {
			t.Errorf("%s: the list of what was not judged is %v, want it to say %q", c.name, rep.NotJudged, c.why)
		}
		if rep.Over != 0 {
			t.Errorf("%s: %d rows over", c.name, rep.Over)
		}
		if rep.Unexpected == 0 {
			t.Errorf("%s: nothing was judged, and no row is counted as one that should have been", c.name)
		}
	}
	// Without a window, Q3 is not judged either; and Q5's resident set is judged only for a build that is.
	noWindow := synth("Lroot/k64a2l1ns", trace(func(g *runner.GateTrace) { g.Windows = nil }))
	if got := states(t, []*runner.StoreGateBuild{noWindow}, "Lroot/k64a2l1ns"); got["Q3"] != runner.GateNotJudged || got["Q5 peak res"] != runner.GateOK {
		t.Errorf("a build without a window: %v", got)
	}
}

// The repetitions of a candidate on an architecture are one row, the worst of those
// that could be judged, and the row says so.
func TestRepetitionsAreJudgedByTheWorstOfThem(t *testing.T) {
	t.Parallel()

	rep := func(n int, longest time.Duration) *runner.StoreGateBuild {
		return synth("Lroot/k64a2l1ns", func(b *runner.StoreGateBuild) {
			b.Dir += "/rep" + string(rune('0'+n))
			b.Job = &runner.Job{Rep: n}
		}, trace(func(g *runner.GateTrace) { g.Inside.Commit = histOf(longest) }))
	}
	r := find(t, runner.JudgeStoreGate([]*runner.StoreGateBuild{rep(1, 10*time.Millisecond), rep(2, 300*time.Millisecond), rep(3, 20*time.Millisecond)}), "Lroot/k64a2l1ns", "Q1")
	if r.State != runner.GateOver || !strings.Contains(r.Value, "300ms") || !strings.Contains(r.Value, "worst of 3 judged of 3") {
		t.Errorf("%+v", r)
	}
	// each repetition is compared with the baseline of its own repetition
	offs := []*runner.StoreGateBuild{
		synth("Lroot/off", func(b *runner.StoreGateBuild) { b.Job = &runner.Job{Rep: 1} }),
		synth("Lroot/off", func(b *runner.StoreGateBuild) { b.Job = &runner.Job{Rep: 2}; b.Metrics.HeapLivePeak = 4 * gib }),
	}
	cands := []*runner.StoreGateBuild{
		synth("Lroot/k64a2l1ns", func(b *runner.StoreGateBuild) { b.Job = &runner.Job{Rep: 1}; b.Dir += "/1" }),
		synth("Lroot/k64a2l1ns", func(b *runner.StoreGateBuild) {
			b.Job = &runner.Job{Rep: 2}
			b.Dir += "/2"
			b.Metrics.HeapLivePeak = 4*gib + 512_000_000
		}),
	}
	rr := runner.JudgeStoreGate(append(offs, cands...))
	if r := find(t, rr, "Lroot/k64a2l1ns", "Q5 peak live"); r.State != runner.GateOK {
		t.Errorf("each repetition against the baseline of its own: %+v", r)
	}
	// two baselines of one plan, architecture and repetition: which one is it?
	dup := synth("Lroot/off", func(b *runner.StoreGateBuild) { b.Dir += "/again" })
	rr = runner.JudgeStoreGate([]*runner.StoreGateBuild{synth("Lroot/off"), dup, synth("Lroot/k64a2l1ns")})
	if r := find(t, rr, "Lroot/k64a2l1ns", "Q5 peak live"); r.State != runner.GateNotJudged || !strings.Contains(strings.Join(rr.NotJudged, "\n"), "which is the baseline") {
		t.Errorf("two baselines: %+v %v", r, rr.NotJudged)
	}
}

// The rules the gate states are numbers fixed before anything is measured, in a text with a digest, printed with
// every report; the digest is pinned here, so a change to a threshold or a word of the
// rules fails this test and shows in the diff. It reads nothing of the spike's rules.
func TestTheRulesTextAndItsDigestArePinned(t *testing.T) {
	t.Parallel()

	text := runner.StoreGateRulesText()
	for _, want := range []string{
		"store gate rules 1\n",
		"at most 250ms",
		"max(2x the same outside windows and settles, read as the lower bound of its bucket, 50ms)",
		"with 30 days kept, the longest rewrite (from the publication to the end of the rewrite) is at most 2h0m0s, at a load outside windows of at least 3000 records/s",
		"are at least 3000 records/s",
		"plus 512 MB",
		"the same GOGC, GOMEMLIMIT and metrics interval",
		"MB and GB are 10^6 and 10^9 bytes",
		"lower bound of its bucket",
		"upper bound of its histogram bucket",
		"a batch that overlaps it and no window is settling",
		"plus the time of those batches outside the windows",
		"peak resident set of the build process",
		"is at most 14 GB",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("the rules text lacks %q:\n%s", want, text)
		}
	}
	const pinned = "014496b454e822c11c5b2791fc61a355d5dc4655c81de57a2364c4b67776d6f2"
	if got := runner.StoreGateRulesDigest(); got != pinned {
		t.Errorf("the digest of the rules text is %s, pinned %s: a limit or a word of the rules changed, which is a decision and not a fix of a test\n%s", got, pinned, text)
	}
}

// A real build of a root-store candidate records what the gate judges: the mode, the
// GC target, a window for each retention that rewrote, every batch outside them (a
// synchronous store writes none inside), the time it waited to rest and its resident
// set; the manifest on disk has it, the gate loads it, judges Q3 on what the build did
// and says that Q1, Q2 and Q4 have no batch inside a window to judge; a candidate not
// built through the root store is listed as not judged.
func TestABuildRecordsWhatTheGateJudges(t *testing.T) {
	t.Parallel()

	plan := mustPlan(t, tinySpec())
	out := t.TempDir()
	root := lookup(t, "Lroot/off")
	rootDir := runner.CandidateDir(out, root.Name)
	m, err := runner.BuildWith(context.Background(), plan, root, rootDir, clean, runner.BuildOptions{MetricsEvery: 5 * time.Millisecond, RetentionMode: "sync"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	g := m.Gate
	if g == nil {
		t.Fatal("no gate trace in the manifest")
	}
	if g.Mode != "sync" || m.Describe[runner.RetentionModeKey] != "sync" {
		t.Errorf("mode %q, described %q, want sync", g.Mode, m.Describe[runner.RetentionModeKey])
	}
	if m.Describe[runner.GoGCKey] == "" {
		t.Errorf("%s is not recorded", runner.GoGCKey)
	}
	if n := len(m.Stream.Retentions); len(g.Windows) == 0 || len(g.Windows) > n {
		t.Errorf("%d windows for %d retentions", len(g.Windows), n)
	}
	for i, w := range g.Windows {
		if w.StartNs < 0 || w.RewriteEndNs < w.StartNs || w.SettleEndNs < w.RewriteEndNs || w.ReturnNs < w.SettleEndNs-1e6 || w.RewriteEndNs == 1<<63-1 {
			t.Errorf("window %d is %+v", i, w)
		}
		if i > 0 && w.StartNs < g.Windows[i-1].ReturnNs {
			t.Errorf("window %d starts before the writer was back from window %d", i, i-1)
		}
	}
	if g.Inside.Batches != 0 || g.Outside.Batches != m.Timing.Writes.Count || g.Outside.Records != int64(m.Stream.Records) {
		t.Errorf("inside %+v; outside %d batches and %d records, the build wrote %d and %d", g.Inside.Batches, g.Outside.Batches, g.Outside.Records, m.Timing.Writes.Count, m.Stream.Records)
	}
	if g.WriteNs <= 0 || g.KeepNs <= 0 || g.RestNs < 0 || g.WriteNs < g.RestNs {
		t.Errorf("write %d, keep %d, rest %d", g.WriteNs, g.KeepNs, g.RestNs)
	}
	if runtime.GOOS != "windows" && g.PeakRSS <= 0 {
		t.Errorf("peak resident set %d", g.PeakRSS)
	}
	if _, ok := g.Maxima["retain.chunk_hold_ns"]; !ok {
		t.Errorf("no largest chunk hold in %v", g.Maxima)
	}

	// a candidate that is not built through the root store records no trace
	other := lookup(t, "L/off")
	om, err := runner.BuildWith(context.Background(), plan, other, runner.CandidateDir(out, other.Name), clean, runner.BuildOptions{RetentionMode: "sync"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if om.Gate != nil || om.Describe[runner.RetentionModeKey] != "" {
		t.Errorf("L/off: %+v, described %q: only a root-store build records the gate's trace", om.Gate, om.Describe[runner.RetentionModeKey])
	}

	builds, err := runner.LoadStoreGateBuilds([]string{out})
	if err != nil {
		t.Fatal(err)
	}
	if len(builds) != 2 {
		t.Fatalf("%d builds loaded from %s", len(builds), out)
	}
	rep := runner.JudgeStoreGate(builds)
	for _, q := range []string{"Q1", "Q2", "Q4"} {
		if r := find(t, rep, "Lroot/off", q); r.State != runner.GateNotJudged {
			t.Errorf("%s: %+v, want not judged", q, r)
		}
	}
	if r := find(t, rep, "Lroot/off", "Q3"); r.State == runner.GateOver {
		t.Errorf("Q3: %+v", r)
	}
	if r := find(t, rep, "L/off", "Q1"); r.State != runner.GateNotJudged {
		t.Errorf("L/off Q1: %+v", r)
	}
	listed := strings.Join(rep.NotJudged, "\n")
	for _, want := range []string{"synchronous retention: no batch is written inside a window", "not a build through the root store"} {
		if !strings.Contains(listed, want) {
			t.Errorf("the list of what was not judged lacks %q:\n%s", want, listed)
		}
	}
	var text strings.Builder
	runner.WriteStoreGate(&text, rep)
	if !strings.Contains(text.String(), "Background: not available on this store") {
		t.Errorf("the report does not say the background mode is not available:\n%s", text.String())
	}

	// a directory with no build in it, and a build with a damaged job, are errors
	if _, err := runner.LoadStoreGateBuilds([]string{t.TempDir()}); err == nil {
		t.Error("an empty directory loaded")
	}
	if err := os.WriteFile(filepath.Join(rootDir, runner.JobFile), []byte("{"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := runner.LoadStoreGateBuild(rootDir); err == nil {
		t.Error("a damaged job loaded")
	}
}

// The root store has one retention mode: a build asked for the background one fails
// before it writes anything.
func TestABuildInTheBackgroundModeIsRefused(t *testing.T) {
	t.Parallel()

	plan := mustPlan(t, tinySpec())
	v := lookup(t, "Lroot/off")
	dir := runner.CandidateDir(t.TempDir(), v.Name)
	_, err := runner.BuildWith(context.Background(), plan, v, dir, clean, runner.BuildOptions{RetentionMode: candidates.RetentionBackground}, nil)
	if err == nil || !strings.Contains(err.Error(), "Background: not available on this store") {
		t.Errorf("%v", err)
	}
}

// A manifest with no gate record writes none: the field is left out of the JSON, so
// the manifests of builds that have none are as they were.
func TestAManifestWithoutAGateRecordHasNoGateField(t *testing.T) {
	t.Parallel()

	b, err := json.Marshal(&runner.Manifest{Candidate: "Lroot/off"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "Gate") {
		t.Errorf("a manifest without a trace has %s", b)
	}
}

// Q4 divides the records of the batches inside windows by the windows' time and by the
// time of those batches that lies outside the windows: a big batch over a short window
// does not make a quick writer. Ten seconds of window and 40000 records are 4000 a
// second; with another 3 s of batches outside the window they are 3077, with 4 s 2857.
func TestQ4CountsTheTimeOfABatchThatOnlyTouchesAWindow(t *testing.T) {
	t.Parallel()

	for _, c := range []struct {
		spill time.Duration
		want  string
	}{
		{0, runner.GateOK}, {3 * time.Second, runner.GateOK}, {4 * time.Second, runner.GateOver}, {time.Minute, runner.GateOver},
	} {
		b := synth("Lroot/k64a2l1ns", trace(func(g *runner.GateTrace) { g.Inside.SpillNs = int64(c.spill) }))
		r := find(t, runner.JudgeStoreGate([]*runner.StoreGateBuild{b}), "Lroot/k64a2l1ns", "Q4")
		if r.State != c.want {
			t.Errorf("spill %s: %+v, want %s", c.spill, r, c.want)
		}
	}
}

// The Lroot/off a heap is compared with must itself be a build the gate judges (an
// untimed one, one without the gate's record or one whose window never ended is not),
// and must have sampled at the same interval: the peak of a sample depends on how often
// it is taken.
func TestTheBaselineOfQ5MustBeJudgeableAndSampledAlike(t *testing.T) {
	t.Parallel()

	for _, c := range []struct {
		name string
		off  func(*runner.StoreGateBuild)
		why  string
	}{
		{"an untimed baseline", func(b *runner.StoreGateBuild) { b.Manifest.Untimed = true }, "baseline cannot be used: an untimed build"},
		{"a baseline of an earlier binary", func(b *runner.StoreGateBuild) { b.Manifest.Gate = nil }, "baseline cannot be used: built without the gate's record"},
		{"a baseline sampled at another interval", func(b *runner.StoreGateBuild) { b.Manifest.Describe[runner.MetricsKey] = "30s" }, runner.MetricsKey + " differs"},
	} {
		cand := synth("Lroot/k64a2l1ns", func(b *runner.StoreGateBuild) { b.Metrics.HeapLivePeak = 9 * gib })
		rep := runner.JudgeStoreGate([]*runner.StoreGateBuild{synth("Lroot/off", c.off), cand})
		if r := find(t, rep, "Lroot/k64a2l1ns", "Q5 peak live"); r.State != runner.GateNotJudged {
			t.Errorf("%s: %+v, want not judged", c.name, r)
		}
		if !strings.Contains(strings.Join(rep.NotJudged, "\n"), c.why) {
			t.Errorf("%s: the list of what was not judged is %v, want it to say %q", c.name, rep.NotJudged, c.why)
		}
	}
}

// A row that only some repetitions could be judged on says so: it is marked partial in
// the output, and not only by its figures.
func TestARowJudgedOnSomeRepetitionsIsMarkedPartial(t *testing.T) {
	t.Parallel()

	one := func(n int, mod func(*runner.GateTrace)) *runner.StoreGateBuild {
		return synth("Lroot/k64a2l1ns", func(b *runner.StoreGateBuild) { b.Dir += "/" + string(rune('0'+n)); b.Job = &runner.Job{Rep: n} }, trace(mod))
	}
	rep := runner.JudgeStoreGate([]*runner.StoreGateBuild{
		one(1, func(*runner.GateTrace) {}), one(2, func(g *runner.GateTrace) { g.Windows = nil }), one(3, func(*runner.GateTrace) {}),
	})
	if r := find(t, rep, "Lroot/k64a2l1ns", "Q1"); !r.Partial || r.State != runner.GateOK {
		t.Errorf("%+v", r)
	}
	if r := find(t, rep, "Lroot/k64a2l1ns", "Q5 peak res"); r.Partial {
		t.Errorf("a row judged on every repetition is partial: %+v", r)
	}
	var out strings.Builder
	runner.WriteStoreGate(&out, rep)
	if !strings.Contains(out.String(), "ok (partial)") {
		t.Errorf("the output does not mark the row:\n%s", out.String())
	}
}

// The batches of a settle are reported apart: their count, longest and 99th percentile
// are in the lines that are not judged.
func TestTheBatchesOfASettleAreReportedApart(t *testing.T) {
	t.Parallel()

	b := synth("Lroot/k64a2l1ns", trace(func(g *runner.GateTrace) {
		g.Settling = runner.GateBatches{Batches: 3, Records: 3000, Commit: histOf(time.Millisecond, 2*time.Millisecond, 700*time.Millisecond)}
	}))
	rep := runner.JudgeStoreGate([]*runner.StoreGateBuild{b})
	if len(rep.Info) != 1 || !strings.Contains(rep.Info[0], "settling (inside no window): 3 batches, longest 700ms") {
		t.Errorf("%v", rep.Info)
	}
	// and they are not the control, nor the baseline of Q2
	if r := find(t, rep, "Lroot/k64a2l1ns", "Q1"); !strings.Contains(r.Value, "longest outside windows: 20ms") {
		t.Errorf("the control: %q", r.Value)
	}
}

// Builds that run at once in one process record the same GC target, the one the process
// started with: reading it must not set it, or a build that runs while another reads
// (or while any code holds the target at "off" for a while, as a cold read does) would
// record "off". The target is held at "off" here while two builds run side by side.
// This test is not parallel: it changes the process's setting for its length.
func TestBuildsRunAtOnceRecordTheSameGCTarget(t *testing.T) {
	sample := []metrics.Sample{{Name: "/gc/gogc:percent"}}
	metrics.Read(sample)
	if sample[0].Value.Kind() != metrics.KindUint64 {
		t.Skip("the runtime does not report the GC target")
	}
	want := "off"
	if n := int64(sample[0].Value.Uint64()); n >= 0 {
		want = strconv.FormatInt(n, 10)
	}

	plan := mustPlan(t, tinySpec())
	defer debug.SetGCPercent(debug.SetGCPercent(-1)) // held at "off" until the test ends
	var wg sync.WaitGroup
	got := make([]string, 4)
	for i, name := range []string{"Lroot/off", "L/off", "Lroot/k64a2l1ns", "L/k64a2l1ns"} {
		v := lookup(t, name)
		wg.Add(1)
		go func() {
			defer wg.Done()
			m, err := runner.BuildWith(context.Background(), plan, v, runner.CandidateDir(t.TempDir(), v.Name), clean, runner.BuildOptions{}, nil)
			if err != nil {
				t.Error(err)
				return
			}
			got[i] = m.Describe[runner.GoGCKey]
		}()
	}
	wg.Wait()
	for i, g := range got {
		if g != want {
			t.Errorf("build %d recorded %s %q, want %q (the target the process started with)", i, runner.GoGCKey, g, want)
		}
	}
}
