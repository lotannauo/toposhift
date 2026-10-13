package runner_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/candidates"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
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
				runner.PaceKey: "4500", runner.PaceFromDayKey: "46.5",
			},
			Gate: &runner.GateTrace{
				Mode:    "background",
				Windows: []runner.GateWindow{{StartNs: 0, RewriteEndNs: 10e9, SettleEndNs: 12e9, ReturnNs: 0}},
				Inside:  runner.GateBatches{Batches: 100, Records: 40000, Commit: hist(100, 30*time.Millisecond)},
				Outside: runner.GateBatches{Batches: 1000, Records: 3_000_000, Commit: hist(1000, 20*time.Millisecond)},
				WriteNs: 1000e9, StreamEndNs: 1000e9, KeepNs: int64(30 * 24 * time.Hour), PeakRSS: 2 * gib,
				Maxima: map[string]int64{"retain.max_prefix_records": 123, "retain.chunk_hold_ns": int64(40 * time.Millisecond)},
			},
		},
		Metrics: runner.StoreGateMetrics{Lines: 5, HeapLiveSamples: 5, HeapLivePeak: gib},
	}
	for _, m := range mods {
		m(b)
	}
	startPacingExplicitly(b)
	return b
}

// startPacingExplicitly gives a paced build in the background the start of its pacing, which
// a build the gate judges has: one nanosecond into the build, with every instant of the
// trace moved by that nanosecond, so that the windows, the end of the stream and every
// duration between them are the ones the fixture says. A build that says it was not paced,
// or whose retention is synchronous, or that already has a start, is left as it is.
func startPacingExplicitly(b *runner.StoreGateBuild) {
	g := b.Manifest.Gate
	if g == nil || g.Mode != "background" || g.PaceStartNs != 0 {
		return
	}
	if rate, _ := strconv.Atoi(b.Manifest.Describe[runner.PaceKey]); rate <= 0 {
		return
	}
	open := int64(1<<63 - 1)
	later := func(ns *int64) {
		if *ns != 0 && *ns != open {
			*ns++
		}
	}
	for i := range g.Windows {
		w := &g.Windows[i]
		w.StartNs++
		w.ReturnNs++
		later(&w.RewriteEndNs)
		later(&w.SettleEndNs)
		later(&w.SupersededNs)
	}
	later(&g.WriteNs)
	later(&g.StreamEndNs)
	g.PaceStartNs = 1
}

// synthSync is the same build made with a synchronous retention, which is judged as in
// version 1: a build of the tests of arithmetic that has not changed.
func synthSync(cand string, mods ...func(*runner.StoreGateBuild)) *runner.StoreGateBuild {
	toSync := func(b *runner.StoreGateBuild) {
		b.Manifest.Gate.Mode = "sync"
		b.Manifest.Describe[runner.RetentionModeKey] = "sync"
		delete(b.Manifest.Describe, runner.PaceKey)
		delete(b.Manifest.Describe, runner.PaceFromDayKey)
	}
	return synth(cand, append([]func(*runner.StoreGateBuild){toSync}, mods...)...)
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
// a nanosecond over it does not, and the longest batch outside every window and settle
// is shown as the control (a verdict that dropped it would show the store's slowness
// without what the machine does anyway).
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

// A build over 250 ms is over the limit only if its control, the longest batch outside
// every window and settle, is at most 150 ms: with a higher control, or none, Q1 is not
// judged, and says why. The control never prevents a pass and never relaxes the limit.
// The cases are numbered as the decision lists them, so that each mutant of the rule is
// caught by a case of its own: a control ignored (5, 7), a control that relaxes the limit
// (9), >= against 150 ms (4), a high control that withholds a pass (6), a missing control
// taken for zero (7), < against 250 ms (1).
func TestQ1IsOverOnlyWhereItsControlLeavesRoomToTellNoiseFromAWait(t *testing.T) {
	t.Parallel()

	const none = time.Duration(-1) // no batch outside every window and settle
	ms := time.Millisecond
	for _, c := range []struct {
		n       int
		inside  time.Duration
		control time.Duration
		want    string
		reason  string // what the list of what was not judged says, "" for nothing about Q1
	}{
		{1, 250 * ms, 90 * ms, runner.GateOK, ""},
		{2, 250*ms - 1, 90 * ms, runner.GateOK, ""},
		{3, 250*ms + 1, 90 * ms, runner.GateOver, ""},
		{4, 270 * ms, 150 * ms, runner.GateOver, ""},
		{5, 270 * ms, 150*ms + 1, runner.GateNotJudged, "control 150.000001ms above 150ms"},
		{6, 240 * ms, 180 * ms, runner.GateOK, ""},
		{7, 270 * ms, none, runner.GateNotJudged, "no batch outside every window and settle"},
		{8, 240 * ms, none, runner.GateOK, ""},
		{9, 380 * ms, 300 * ms, runner.GateNotJudged, "control 300ms above 150ms"},
	} {
		b := synth("Lroot/k64a2l1ns", trace(func(g *runner.GateTrace) {
			g.Inside.Commit = histOf(10*ms, c.inside)
			if c.control == none {
				g.Outside = runner.GateBatches{}
			} else {
				g.Outside.Commit = histOf(7*ms, c.control)
			}
		}))
		rep := runner.JudgeStoreGate([]*runner.StoreGateBuild{b})
		r := find(t, rep, "Lroot/k64a2l1ns", "Q1")
		if r.State != c.want {
			t.Errorf("case %d: inside %s, control %s: %+v, want %s", c.n, c.inside, c.control, r, c.want)
		}
		if !strings.Contains(r.Limit, "250ms") || !strings.Contains(r.Value, "control") {
			t.Errorf("case %d: the row %+v does not show the limit and the control", c.n, r)
		}
		listed := ""
		for _, s := range rep.NotJudged {
			if strings.Contains(s, "Q1") {
				listed = s
			}
		}
		if (c.reason == "") != (listed == "") || !strings.Contains(listed, c.reason) {
			t.Errorf("case %d: the list of what was not judged has %q for Q1, want it to say %q", c.n, listed, c.reason)
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
		b := synthSync("Lroot/k64a2l1ns", trace(c.f))
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
		b := synthSync("Lroot/k64a2l1ns", trace(c.f))
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
	b := synthSync("Lroot/k64a2l1ns", trace(func(g *runner.GateTrace) {
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

	syncBuild := func(cand string) *runner.StoreGateBuild {
		return synth(cand, trace(func(g *runner.GateTrace) {
			g.Mode = "sync"
			g.Inside = runner.GateBatches{}
			g.Windows = []runner.GateWindow{{StartNs: 0, RewriteEndNs: 10e9, SettleEndNs: 12e9, ReturnNs: 13e9}}
		}), func(b *runner.StoreGateBuild) { b.Manifest.Describe[runner.RetentionModeKey] = "sync" })
	}
	sync := syncBuild("Lroot/k64a2l1ns")
	rep := runner.JudgeStoreGate([]*runner.StoreGateBuild{syncBuild("Lroot/off"), sync})
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
	for _, want := range []string{"retention mode: sync\n", "synchronous retention: the writer is blocked", "NOT JUDGED:", "rules digest " + runner.StoreGateRulesDigest()} {
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
		{"window never ended", synthSync("Lroot/k64a2l1ns", trace(func(g *runner.GateTrace) { g.Windows[0].RewriteEndNs = 1<<63 - 1 })), "never ended"},
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
			b.Job = &runner.Job{RunID: "1", RunAttempt: "1", Rep: n}
		}, trace(func(g *runner.GateTrace) { g.Inside.Commit = histOf(longest) }))
	}
	r := find(t, runner.JudgeStoreGate([]*runner.StoreGateBuild{rep(1, 10*time.Millisecond), rep(2, 300*time.Millisecond), rep(3, 20*time.Millisecond)}), "Lroot/k64a2l1ns", "Q1")
	if r.State != runner.GateOver || !strings.Contains(r.Value, "300ms") || !strings.Contains(r.Value, "worst of 3 judged of 3") {
		t.Errorf("%+v", r)
	}
	// each repetition is compared with the baseline of its own repetition
	offs := []*runner.StoreGateBuild{
		synth("Lroot/off", func(b *runner.StoreGateBuild) { b.Job = &runner.Job{RunID: "1", RunAttempt: "1", Rep: 1} }),
		synth("Lroot/off", func(b *runner.StoreGateBuild) {
			b.Job = &runner.Job{RunID: "1", RunAttempt: "1", Rep: 2}
			b.Metrics.HeapLivePeak = 4 * gib
		}),
	}
	cands := []*runner.StoreGateBuild{
		synth("Lroot/k64a2l1ns", func(b *runner.StoreGateBuild) {
			b.Job = &runner.Job{RunID: "1", RunAttempt: "1", Rep: 1}
			b.Dir += "/1"
		}),
		synth("Lroot/k64a2l1ns", func(b *runner.StoreGateBuild) {
			b.Job = &runner.Job{RunID: "1", RunAttempt: "1", Rep: 2}
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
		"store gate rules 3\n",
		"at most 250ms",
		"A build over that limit is over it only if its control is at most 150ms; with a control above 150ms, or no batch outside every window and every settle, Q1 is not judged for the build, with the control in the reason,",
		"A control never prevents a pass.",
		"max(2x the same outside windows and settles, read as the lower bound of its bucket, 50ms)",
		"with 30 days kept, the longest rewrite (from the publication to the end of the rewrite) is at most 2h0m0s, at a load outside windows of at least 3000 records/s",
		"are at least 3000 records/s",
		"plus 512 MB",
		"the same GOGC, GOMEMLIMIT and metrics interval, and with the same retention mode, sync setting, pacing rate, pacing start and retention-flush threshold",
		// version 2: the cadence, the pacing, and what is judged of a build in the background
		"Q3 assumes the product calls Retain at most once a day per store, each call moving every layer that is due; its bound is one twelfth of that interval. A shorter product cadence needs a new rule.",
		"`-pace R`: records are due on a fixed wall-clock schedule of R per second from the moment pacing starts; when the writer is behind it writes without sleeping until it is back on schedule, and never runs ahead of it. A build of window W is unpaced for its first D = 2W + 1.5 − ⌈W/2⌉ stream days and paced for the rest (7 days: D = 11.5; 30 days: D = 46.5); any other D is refused. A build whose retention is synchronous is not paced.",
		"Q1, Q2, Q3 and Q4 are judged only on the paced part of a build paced at the pre-registered offered load of 4,500 records/s: its windows that start at or after D (the judged windows) and, for Q1's control, Q2's outside and Q3's load, its batches that start at or after D.",
		"A batch that overlaps an unjudged window, or its settle, is counted in neither the inside nor the outside, and is reported.",
		"A window replaced by a later retention, or still open when the stream ended, ends there, for Q1, Q2 and Q4, and does not refuse the build.",
		"Q3's load is taken over the paced writing time, from D to the end of the stream.",
		"Q3 fails if any rewrite time or any lower bound exceeds its limit, at Q3's load. Otherwise, if any judged pass has only a lower bound, Q3 is not judged. Otherwise it passes.",
		"A window's time for Q4 ends at the end of the stream.",
		"Q5 compares builds with the same retention mode, sync setting, pacing rate, pacing start and retention-flush threshold.",
		"A build whose retention is synchronous is judged as in version 1.",
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
	if runner.StoreGateRulesVersion != 3 {
		t.Errorf("the rules version is %d: version 3 adds the retention-flush threshold to Q5's match, registered before any background build was judged on CI", runner.StoreGateRulesVersion)
	}
	const pinned = "667f1857c82875128eb7524618e41003fa679b02fa3d202111db076130b1761f"
	if got := runner.StoreGateRulesDigest(); got != pinned {
		t.Errorf("the digest of the rules text is %s, pinned %s: a limit or a word of the rules changed, which is a decision and not a fix of a test\n%s", got, pinned, text)
	}
}

// The log line of version 2 stands above the version in the source, word for word.
func TestTheLogOfVersionTwoIsInTheSource(t *testing.T) {
	t.Parallel()

	const log = "Log (2026-10-10, version 2, before any background build was judged by the gate on a measurement): Q3 assumes a retention at most once a day per store. For a build whose retention runs in the background: Q1 to Q4 are judged only on the paced part, from stream day 2W + 1.5 − ⌈W/2⌉, with the writer paced at 4,500 records/s on an open-loop schedule and its commits synced; the rest, including batches in unjudged windows, prints its values for information. Q3 fails when a rewrite, or a lower bound of one (a pass replaced, or unfinished at the end of the stream), exceeds its limit; it is not judged when a judged pass has only a lower bound, and otherwise passes. At 4,500 records/s a stream day lasts minutes of wall time, so Q3 can fail only if the writer stalls for about its limit, and a rewrite that outlasts its stream day is caught by the reopen check before dispatch. Q3's load is taken over the paced writing time, a window's time for Q4 ends at the end of the stream, and Q5 also matches the sync setting, pacing rate and pacing start. A build whose retention is synchronous is judged as in version 1."
	raw, err := os.ReadFile("storegate.go")
	if err != nil {
		t.Fatal(err)
	}
	var words []string
	collecting := false
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(line, "// Log (2026-10-10, version 2"):
			collecting = true
		case collecting && !strings.HasPrefix(line, "//"):
			collecting = false
		}
		if collecting {
			words = append(words, strings.Fields(strings.TrimPrefix(line, "//"))...)
		}
	}
	if got := strings.Join(words, " "); got != log {
		t.Errorf("the log line above StoreGateRulesVersion is\n%s\nwant\n%s", got, log)
	}
	if !strings.Contains(runner.StoreGateQ3Note, "At 4,500 records/s a stream day lasts minutes of wall time, so in the Background arm Q3 can fail only if no retention follows a publication for its whole limit, that is, if the writer stalls for about that long; otherwise it passes or is not judged. A rewrite that outlasts its stream day is caught by the reopen check before dispatch.") {
		t.Errorf("the Q3 note: %s", runner.StoreGateQ3Note)
	}
}

// The log line of version 3 stands above the version in the source, word for word.
func TestTheLogOfVersionThreeIsInTheSource(t *testing.T) {
	t.Parallel()
	const log = "Log (2026-10-13, version 3, before any background build was judged on CI): Q5 and the repetitions of a verdict also match the retention-flush threshold (retain_flush_dels), which a build through the root store records in its manifest; a manifest without it is not recorded, never read as the default. The thresholds 32,768, 24,576 and 16,384 are registered as three distinct configurations, each a 7-day background dispatch of its own from one frozen commit; a dispatch of one rung is not a redispatch of another with a changed configuration. Incomplete builds: A build is complete when it has written its gate trace. A build that stops before that because the build or the store failed (an error returned, a crash, the process killed for memory, a full disk) has failed, and its arm fails. A build that stops before that for another reason (its build step or job reached its time limit, it was cancelled, or its runner was lost with no sign in its logs of memory or disk exhaustion) is incomplete and is not judged; the report lists it with the reason and the evidence. A cancellation counts as incomplete only if its reason is recorded when it is made (for a cancellation the owner did not make, when it is found) and is not drawn from the build's own output; otherwise the arm is not judged and is not redispatched under this rules version. A complete build with a row the report marks over its limit fails its arm, whatever the arm's other builds did. Otherwise an arm with any incomplete pre-registered build is not judged. An arm that is not judged has not passed, and the pre-registered fallback applies: the release ships with retention off by default. The only exception is one redispatch of the whole arm (every build, baselines included, on every architecture), under the same rules version and the same configuration (the same commit, workflow inputs and plans; the runner image is recorded), that is complete and passes. Its verdict replaces the first dispatch's; the first dispatch's complete builds are reported beside it and are not pooled with it. Re-running some of an arm's jobs, in the same workflow run or another, is not a redispatch and is not allowed. Each incomplete build and each redispatch is recorded, with its reason, in the gate's log before the next dispatch. No build or arm is redispatched with a changed configuration under the same rules version. A changed configuration needs a new rules version, whose log lists every earlier dispatch of the arm with its verdict or why it was incomplete; the fallback holds for any release made before an arm under the new version passes. An arm that finished with a criterion not judged has not passed; it may use the same single whole-arm redispatch as an incomplete build, one in total whatever the reason. A reopen on (a) alone leads to one measured change of the settle deadline under a new rules version, logged, and another quiet run; a reopen on (b) or (c) means the fallback holds for this release, and any change goes into a new, logged rules version followed by another quiet run. The retention-chunk criteria, the threshold ladder, the reopen thresholds and the 30-day dispatch condition were written after the void local 30-day reopen build of 2026-10-11/12 and after local observations of the retention's chunks, whose values were seen and are not published. The 25% settle-share criterion is withdrawn because it could not trip at a 2-minute settle deadline. The chunk criteria's bounds come from the chunk clock's caps (chunkTime/2, 1.5 × chunkTime); the local data informed only the rung values. FlushDelayDeleteRange is 0. No threshold was set from a CI measurement. Under version 2 no background arm was dispatched on CI, and the one local 30-day reopen build was void (machine loaded) and was not read."
	raw, err := os.ReadFile("storegate.go")
	if err != nil {
		t.Fatal(err)
	}
	var words []string
	collecting := false
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(line, "// Log (2026-10-13, version 3"):
			collecting = true
		case collecting && !strings.HasPrefix(line, "//"):
			collecting = false
		}
		if collecting {
			words = append(words, strings.Fields(strings.TrimPrefix(line, "//"))...)
		}
	}
	if got := strings.Join(words, " "); got != log {
		t.Errorf("the log line above StoreGateRulesVersion is\n%s\nwant\n%s", got, log)
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
	// What the build recorded of its chunks and its database is reported: the stalls
	// and the sublevels were sampled (the spread of the chunks depends on how many
	// chunks the tiny plan's passes have, beyond the last of each, and may count none).
	for _, want := range []string{"the whole build, not only its judged part (reported, not judged): chunks of all passes pooled, each pass's last left out: ", "; least prefixes in a chunk ", "; flushes: retain.flushes ", "; write stalls ", "; level 0 sublevels: peak "} {
		if !strings.Contains(text.String(), want) {
			t.Errorf("the report lacks %q:\n%s", want, text.String())
		}
	}
	if strings.Contains(text.String(), "write stalls not recorded") || strings.Contains(text.String(), "sublevels: peak not recorded") {
		t.Errorf("a build sampled every 5ms has no stalls or sublevels:\n%s", text.String())
	}
	if len(g.Chunks) != 3 {
		t.Errorf("the build recorded the spread of %d samples of its chunks, want 3: %v", len(g.Chunks), g.Chunks)
	}
	for name, sp := range g.Chunks {
		if sp.Count < 0 || (sp.Count > 0 && (sp.Min > sp.P50 || sp.P50 > sp.P99 || sp.P99 > sp.Max+sp.Max/8)) {
			t.Errorf("the spread of %s is %+v", name, sp)
		}
	}
	if strings.Contains(text.String(), "not available on this store") {
		t.Errorf("the report says a mode is not available, and both are:\n%s", text.String())
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

// The report gives the spread of the samples of the chunks, the write stalls and the
// peak sublevels of level 0 of each build through the root store, for information, and
// says what a build did not record; none of it changes a row.
func TestTheReportGivesTheChunksAndTheStallsForInformation(t *testing.T) {
	t.Parallel()
	recorded := synth("Lroot/off", func(b *runner.StoreGateBuild) {
		b.Manifest.Gate.Chunks = map[string]runner.GateSpread{
			"retain.chunk_prefixes": {Count: 40, Min: 3, P50: 120, P99: 400, Max: 410},
			"retain.chunk_open_ns":  {Count: 40, Min: int64(time.Millisecond), P50: int64(2 * time.Millisecond), P99: int64(5 * time.Millisecond), Max: int64(6 * time.Millisecond)},
			"retain.chunk_hold_ns":  {Count: 40, Min: int64(10 * time.Millisecond), P50: int64(22 * time.Millisecond), P99: int64(29 * time.Millisecond), Max: int64(31 * time.Millisecond)},
		}
		b.Metrics.WriteStalls, b.Metrics.WriteStallsRead, b.Metrics.L0SublevelsPeak = 2, true, 9
		b.Manifest.Counters = map[string]int64{"retain.flushes": 7, "retain.flush_deferred": 3}
	})
	bare := synth("Lroot/off")
	repRecorded, repBare := runner.JudgeStoreGate([]*runner.StoreGateBuild{recorded}), runner.JudgeStoreGate([]*runner.StoreGateBuild{bare})
	info := strings.Join(repRecorded.Info, "\n")
	for _, want := range []string{
		"retain.chunk_prefixes min 3, p50 120, p99 400, max 410 over 40",
		"retain.chunk_open_ns min 1ms, p50 2ms, p99 5ms, max 6ms over 40",
		"retain.chunk_hold_ns min 10ms, p50 22ms, p99 29ms, max 31ms over 40",
		"; least prefixes in a chunk 3; flushes: retain.flushes 7, retain.flush_deferred 3, retain.flush_errors 0; write stalls at least 2, summed over the openings of the database; level 0 sublevels: peak 9 sampled",
	} {
		if !strings.Contains(info, want) {
			t.Errorf("the report lacks %q:\n%s", want, info)
		}
	}
	collapsed := synth("Lroot/off", func(b *runner.StoreGateBuild) {
		b.Manifest.Gate.Chunks = map[string]runner.GateSpread{"retain.chunk_prefixes": {Count: 40, Min: 1, P50: 120, P99: 400, Max: 410}}
	})
	info = strings.Join(runner.JudgeStoreGate([]*runner.StoreGateBuild{collapsed}).Info, "\n")
	if want := "least prefixes in a chunk 1 (a chunk that was not its pass's last rewrote one prefix)"; !strings.Contains(info, want) {
		t.Errorf("the report of a build with a one-prefix chunk lacks %q:\n%s", want, info)
	}
	info = strings.Join(repBare.Info, "\n")
	for _, want := range []string{"retain.chunk_prefixes not recorded", "least prefixes in a chunk not recorded", "flushes: not recorded", "write stalls not recorded", "level 0 sublevels: peak 0 sampled"} {
		if !strings.Contains(info, want) {
			t.Errorf("the report of a build that recorded none lacks %q:\n%s", want, info)
		}
	}
	for _, q := range []string{"Q1", "Q2", "Q3", "Q4", "Q5"} {
		if a, b := find(t, repRecorded, "Lroot/off", q), find(t, repBare, "Lroot/off", q); a.State != b.State {
			t.Errorf("%s is %v with the chunks recorded and %v without", q, a.State, b.State)
		}
	}
	if repRecorded.RulesDigest != repBare.RulesDigest || repRecorded.RulesDigest != runner.StoreGateRulesDigest() {
		t.Error("the rules digest moved")
	}
}

// A real build in the background mode records what the gate judges, as the store
// reports it: the mode, a window with all four instants for each retention that
// rewrote, every window ended, the settle counted (the retention settled), and the
// instant each call returned at the publication or after it and never before. The
// build is read back and judged, and the reason a row is not judged is the smoke
// plan's (no batch inside a window, 30 days not kept, no heap samples), and never
// the mode.
func TestABuildInTheBackgroundModeRecordsAllFourInstants(t *testing.T) {
	t.Parallel()

	plan := mustPlan(t, tinySpec())
	out := t.TempDir()
	v := lookup(t, "Lroot/k64a2l1ns")
	dir := runner.CandidateDir(out, v.Name)
	m, err := runner.BuildWith(context.Background(), plan, v, dir, clean, runner.BuildOptions{MetricsEvery: 5 * time.Millisecond, RetentionMode: candidates.RetentionBackground}, nil)
	if err != nil {
		t.Fatal(err)
	}
	g := m.Gate
	if g == nil {
		t.Fatal("no gate trace in the manifest")
	}
	if g.Mode != "background" || m.Describe[runner.RetentionModeKey] != "background" {
		t.Errorf("mode %q, described %q, want background", g.Mode, m.Describe[runner.RetentionModeKey])
	}
	if n := len(m.Stream.Retentions); len(g.Windows) == 0 || len(g.Windows) > n {
		t.Errorf("%d windows for %d retentions", len(g.Windows), n)
	}
	t.Logf("windows %+v; %d batches inside, %d settling, %d outside", g.Windows, g.Inside.Batches, g.Settling.Batches, g.Outside.Batches)
	for i, w := range g.Windows {
		open := int64(1<<63 - 1)
		if w.StartNs < 0 || w.RewriteEndNs == open || w.SettleEndNs == open {
			t.Errorf("window %d never ended: %+v", i, w)
		}
		if w.RewriteEndNs < w.StartNs || w.SettleEndNs < w.RewriteEndNs {
			t.Errorf("window %d: the phases are not in order: %+v", i, w)
		}
		if w.ReturnNs < w.StartNs || w.ReturnNs-w.StartNs > int64(time.Minute) {
			t.Errorf("window %d: the call returned at %d, the publication was at %d: a background call returns at the publication", i, w.ReturnNs, w.StartNs)
		}
		if i > 0 && w.StartNs < g.Windows[i-1].SettleEndNs {
			t.Errorf("window %d starts before window %d ended: %+v, %+v", i, i-1, w, g.Windows[i-1])
		}
	}
	// The retention settled, off the lock, and the build waited for it.
	if m.Counters["retain.flush_ns"] <= 0 || m.Counters["retain.settle_ns"] <= 0 {
		t.Errorf("the retentions did not settle: flush %d ns, settle %d ns", m.Counters["retain.flush_ns"], m.Counters["retain.settle_ns"])
	}
	if m.Counters["retain.settle_deadline_hits"] != 0 {
		t.Errorf("%d settles reached their deadline", m.Counters["retain.settle_deadline_hits"])
	}
	if g.Inside.Batches+g.Settling.Batches+g.Outside.Batches != m.Timing.Writes.Count {
		t.Errorf("the batches inside, settling and outside are %d, %d and %d, the build wrote %d", g.Inside.Batches, g.Settling.Batches, g.Outside.Batches, m.Timing.Writes.Count)
	}
	if got := len(m.Timing.RetainPhases); got != len(m.Stream.Retentions) {
		t.Errorf("%d retention phases for %d retentions", got, len(m.Stream.Retentions))
	}

	builds, err := runner.LoadStoreGateBuilds([]string{out})
	if err != nil {
		t.Fatal(err)
	}
	rep := runner.JudgeStoreGate(builds)
	for _, s := range rep.NotJudged {
		if strings.Contains(s, "never ended") || strings.Contains(s, "before it starts") || strings.Contains(s, "no retention window") || strings.Contains(s, "no retention mode") || strings.Contains(s, "not available") {
			t.Errorf("not judged: %s", s)
		}
	}
	if find(t, rep, v.Name, "Q3").State == runner.GateOK && g.KeepNs < int64(30*24*time.Hour) {
		t.Errorf("Q3 passed on a build that kept %d ns", g.KeepNs)
	}
}

// backgroundEngine is what the probe below stands for: an engine built through the root
// store, which retains in the background.
type backgroundEngine interface {
	fullEngine
	WaitRetained(ctx context.Context) error
	LastRetain() (work, flush, settle time.Duration, deadlineHit bool)
}

// waitProbe notes whether a pass was waited for before the build read, sized or
// compacted anything, and before it rested after a retention.
type waitProbe struct {
	backgroundEngine
	mu sync.Mutex
	// waited says WaitRetained has been called since the last Retain.
	waited bool
	waits  int
	early  []string
}

func (p *waitProbe) Retain(h time.Time) error {
	p.mu.Lock()
	p.waited = false
	p.mu.Unlock()
	return p.backgroundEngine.Retain(h)
}

func (p *waitProbe) WaitRetained(ctx context.Context) error {
	err := p.backgroundEngine.WaitRetained(ctx)
	p.mu.Lock()
	p.waited = true
	p.waits++
	p.mu.Unlock()
	return err
}

func (p *waitProbe) check(what string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.waited {
		p.early = append(p.early, what)
	}
}

func (p *waitProbe) Quiesce(ctx context.Context) error {
	p.check("Quiesce")
	return p.backgroundEngine.Quiesce(ctx)
}

func (p *waitProbe) CompactAll(ctx context.Context) error {
	p.check("CompactAll")
	return p.backgroundEngine.(engine.Quiescer).CompactAll(ctx)
}

func (p *waitProbe) Stats() map[string]int64 {
	p.check("Stats")
	return p.backgroundEngine.Stats()
}

func (p *waitProbe) Neighbors(fp identity.Fingerprint, dir engine.Direction, t time.Time, s engine.Scope) ([]engine.Neighbor, error) {
	p.check("Neighbors")
	return p.backgroundEngine.Neighbors(fp, dir, t, s)
}

func (p *waitProbe) Describe() (map[string]string, error) {
	p.check("Describe")
	return p.backgroundEngine.Describe()
}

func (p *waitProbe) Breakdown() (map[engine.Part]int64, error) {
	p.check("Breakdown")
	return p.backgroundEngine.Breakdown()
}

func (p *waitProbe) SizeByLayer() (map[catalog.Layer]int64, error) {
	p.check("SizeByLayer")
	return p.backgroundEngine.SizeByLayer()
}

func (p *waitProbe) Size() (int64, error) {
	p.check("Size")
	return p.backgroundEngine.Size()
}

// A build in the background mode waits for the pass, its settle included, before it
// reads, sizes, describes or compacts anything, and, when it rests after a retention,
// before it rests; and its gate record has no window left open.
func TestABuildInTheBackgroundWaitsForThePassBeforeItReadsAnything(t *testing.T) {
	t.Parallel()

	for _, rest := range []bool{false, true} {
		t.Run(fmt.Sprintf("rest after retention %v", rest), func(t *testing.T) {
			t.Parallel()
			plan := mustPlan(t, tinySpec())
			var probe *waitProbe
			v := candidates.Wrap(lookup(t, "Lroot/off"), "Lroot/off", func(e engine.Engine, _ candidates.Options) (engine.Engine, error) {
				// The build opens the database again, read-only, to check what it wrote: the
				// probe is the engine that built.
				p := &waitProbe{backgroundEngine: e.(backgroundEngine)}
				if probe == nil {
					probe = p
				}
				return p, nil
			})
			dir := runner.CandidateDir(t.TempDir(), v.Name)
			m, err := runner.BuildWith(context.Background(), plan, v, dir, clean, runner.BuildOptions{RestAfterRetention: rest, RetentionMode: candidates.RetentionBackground}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(probe.early) != 0 {
				t.Errorf("the build called %v before it had waited for the pass", probe.early)
			}
			if want := 1 + btoi(rest)*len(m.Stream.Retentions); probe.waits != want {
				t.Errorf("the pass was waited for %d times, want %d (once at the end of the stream, and after each retention when the build rests)", probe.waits, want)
			}
			for i, w := range m.Gate.Windows {
				if w.RewriteEndNs == 1<<63-1 || w.SettleEndNs == 1<<63-1 {
					t.Errorf("window %d is still open: %+v", i, w)
				}
			}
		})
	}
}

func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}

// The repetitions of a candidate on an architecture are one verdict: builds of one pool
// that differ in the retention mode, the sync setting, the pacing rate or the day the
// pacing started are refused, naming the setting and each value with its builds.
func TestAPoolMixingSettingsIsRefused(t *testing.T) {
	t.Parallel()

	for _, c := range []struct {
		key   string
		other string
		mod   func(*runner.StoreGateBuild)
	}{
		{runner.RetentionModeKey, "sync", func(b *runner.StoreGateBuild) {
			b.Manifest.Describe[runner.RetentionModeKey] = "sync"
			b.Manifest.Gate.Mode = "sync"
		}},
		{runner.SyncKey, "true", func(b *runner.StoreGateBuild) { b.Manifest.Describe[runner.SyncKey] = "true" }},
		{runner.PaceKey, "4000", func(b *runner.StoreGateBuild) { b.Manifest.Describe[runner.PaceKey] = "4000" }},
		{runner.PaceFromDayKey, "12", func(b *runner.StoreGateBuild) { b.Manifest.Describe[runner.PaceFromDayKey] = "12" }},
	} {
		first := synth("Lroot/k64a2l1ns", func(b *runner.StoreGateBuild) { b.Dir = "/builds/first" })
		second := synth("Lroot/k64a2l1ns", func(b *runner.StoreGateBuild) { b.Dir = "/builds/second" }, c.mod)
		rep := runner.JudgeStoreGate([]*runner.StoreGateBuild{first, second})
		for _, want := range []string{"different " + c.key, fmt.Sprintf("%s=%q", c.key, c.other), "/builds/first", "/builds/second"} {
			if !strings.Contains(rep.Refusal, want) {
				t.Errorf("%s: the refusal is %q, want it to say %q", c.key, rep.Refusal, want)
			}
		}
		if len(rep.Rows) != 0 || rep.Over != 0 || rep.Unexpected != 0 {
			t.Errorf("%s: a refused set has %d rows, %d over, %d unexpected", c.key, len(rep.Rows), rep.Over, rep.Unexpected)
		}
		if err := runner.CheckStoreGateSettings([]*runner.StoreGateBuild{first, second}); err == nil {
			t.Errorf("%s: CheckStoreGateSettings accepted a mix", c.key)
		}
		var text strings.Builder
		runner.WriteStoreGate(&text, rep)
		if !strings.Contains(text.String(), "NOT JUDGED: ") || !strings.Contains(text.String(), c.key) {
			t.Errorf("%s: the report does not say the set was refused:\n%s", c.key, text.String())
		}
	}

	// the same setting in every build, a baseline that differs (its own pool), and a
	// build with no mode (not through the root store) are not a mix
	same := []*runner.StoreGateBuild{synth("Lroot/off"), synth("Lroot/k64a2l1ns", func(b *runner.StoreGateBuild) { b.Dir = "/builds/a" }), synth("Lroot/k64a2l1ns", func(b *runner.StoreGateBuild) { b.Dir = "/builds/b" })}
	if err := runner.CheckStoreGateSettings(same); err != nil {
		t.Errorf("builds of one setting: %v", err)
	}
	syncBase := synth("Lroot/off", func(b *runner.StoreGateBuild) { b.Manifest.Describe[runner.SyncKey] = "true" })
	if err := runner.CheckStoreGateSettings([]*runner.StoreGateBuild{syncBase, synth("Lroot/k64a2l1ns")}); err != nil {
		t.Errorf("a baseline of another setting is its own pool: %v", err)
	}
	plain := synth("L/off", func(b *runner.StoreGateBuild) { b.Manifest.Describe = map[string]string{}; b.Manifest.Gate = nil })
	if err := runner.CheckStoreGateSettings([]*runner.StoreGateBuild{synth("Lroot/off"), plain}); err != nil {
		t.Errorf("a build with no mode made a pool mixed: %v", err)
	}
	// a mode kept only in the gate's record counts as the mode
	noDescription := synth("Lroot/k64a2l1ns", func(b *runner.StoreGateBuild) {
		delete(b.Manifest.Describe, runner.RetentionModeKey)
		b.Manifest.Gate.Mode = "sync"
		b.Dir = "/builds/n"
	})
	if err := runner.CheckStoreGateSettings([]*runner.StoreGateBuild{synth("Lroot/k64a2l1ns"), noDescription}); err == nil {
		t.Error("a build whose mode is only in the gate's record was taken for the other mode")
	}
}

// Q5 compares a build with an Lroot/off baseline of the same retention mode, sync
// setting, pacing rate and pacing start: against one that differs in any of them it is
// not judged, naming the setting, and with a matching baseline in every repetition it is
// judged in all three.
func TestQ5NeedsABaselineOfTheSameSettings(t *testing.T) {
	t.Parallel()

	reps := func(cand string, mods ...func(*runner.StoreGateBuild)) []*runner.StoreGateBuild {
		var out []*runner.StoreGateBuild
		for rep := 1; rep <= 3; rep++ {
			out = append(out, synth(cand, append([]func(*runner.StoreGateBuild){func(b *runner.StoreGateBuild) {
				b.Dir = fmt.Sprintf("/builds/%s_%d", strings.ReplaceAll(cand, "/", "_"), rep)
				b.Job = &runner.Job{RunID: "1", RunAttempt: "1", Rep: rep}
			}}, mods...)...))
		}
		return out
	}
	rep := runner.JudgeStoreGate(append(reps("Lroot/off"), reps("Lroot/k64a2l1ns")...))
	row := find(t, rep, "Lroot/k64a2l1ns", "Q5 peak live")
	if row.State != runner.GateOK || row.Partial {
		t.Errorf("a matching baseline in every repetition: %+v, want judged in all three", row)
	}

	for _, c := range []struct {
		key string
		mod func(*runner.StoreGateBuild)
	}{
		{runner.RetentionModeKey, func(b *runner.StoreGateBuild) {
			b.Manifest.Describe[runner.RetentionModeKey] = "sync"
			b.Manifest.Gate.Mode = "sync"
		}},
		{runner.SyncKey, func(b *runner.StoreGateBuild) { b.Manifest.Describe[runner.SyncKey] = "true" }},
		{runner.PaceKey, func(b *runner.StoreGateBuild) { b.Manifest.Describe[runner.PaceKey] = "4000" }},
		// The candidate's builds do not record a threshold: a baseline that does is not
		// taken for a build at the default.
		{runner.RetainFlushDelsKey, func(b *runner.StoreGateBuild) { b.Manifest.Describe[runner.RetainFlushDelsKey] = "32768" }},
		// A pacing start that differs from the rule's is a refusal of the build itself, so
		// that the baseline cannot be used; the comparison of the start is a unit test.
		{runner.PaceFromDayKey, func(b *runner.StoreGateBuild) { b.Manifest.Describe[runner.PaceFromDayKey] = "12" }},
	} {
		rep := runner.JudgeStoreGate(append(reps("Lroot/off", c.mod), reps("Lroot/k64a2l1ns")...))
		row := find(t, rep, "Lroot/k64a2l1ns", "Q5 peak live")
		if row.State != runner.GateNotJudged {
			t.Errorf("%s: a baseline that differs: %+v, want not judged", c.key, row)
		}
		want := c.key + " differs"
		if c.key == runner.PaceFromDayKey {
			want = "pace from day 12"
		}
		if !strings.Contains(strings.Join(rep.NotJudged, "\n"), want) {
			t.Errorf("%s: not judged %q, want it to say %q", c.key, rep.NotJudged, want)
		}
	}
}

// ---------------------------------------------------------------------------
// Rules version 2: a build in the background, judged on its paced part.

const hr = int64(time.Hour)

// pacedBuild is a build in the background paced at the rule's rate that kept 30 days,
// with the windows given, a stream of 1000 h, and the load of 4500 records/s outside
// them (the batches inside are left out, so that Q3 alone has something to judge).
func pacedBuild(windows []runner.GateWindow, mods ...func(*runner.GateTrace)) *runner.StoreGateBuild {
	return synth("Lroot/k64a2l1ns", trace(func(g *runner.GateTrace) {
		g.Windows = called(windows)
		g.WriteNs, g.StreamEndNs = 1000*hr, 1000*hr
		g.Inside = runner.GateBatches{}
		g.Outside.Records = 4500 * (1000 * hr / 1e9)
		for _, m := range mods {
			m(g)
		}
	}))
}

// called gives each window the return of its call at its publication, as a background
// store's is.
func called(ws []runner.GateWindow) []runner.GateWindow {
	out := slices.Clone(ws)
	for i := range out {
		if out[i].ReturnNs == 0 {
			out[i].ReturnNs = out[i].StartNs
		}
	}
	return out
}

func q3Of(t *testing.T, b *runner.StoreGateBuild) (runner.StoreGateRow, *runner.StoreGateReport) {
	t.Helper()
	rep := runner.JudgeStoreGate([]*runner.StoreGateBuild{b})
	return find(t, rep, b.Manifest.Candidate, "Q3"), rep
}

// Q3 takes the rewrite time of each judged pass that ended, and for one that a later
// retention replaced, or that had not ended when the stream did, the least its rewrite
// took. It fails if any is over the limit; otherwise it is not judged if any pass has only
// a lower bound; otherwise it passes. The bound of a replaced pass runs to the
// publication that replaced it, and that of an unfinished one to the end of the stream,
// and neither to the end of the rewrite that came later.
func TestQ3TakesLowerBoundsOfThePassesThatDidNotFinish(t *testing.T) {
	t.Parallel()

	open := int64(1<<63 - 1)
	end := 1000 * hr
	for _, c := range []struct {
		name    string
		windows []runner.GateWindow
		want    string
		why     string
	}{
		// a replaced pass, its rewrite said to end at 30 minutes (a bound measured to the
		// rewrite's end would be 30 minutes)
		{"a replaced pass over the limit", []runner.GateWindow{{StartNs: 0, RewriteEndNs: hr / 2, SettleEndNs: hr / 2, SupersededNs: 3 * hr}}, runner.GateOver, ""},
		{"a replaced pass under it", []runner.GateWindow{{StartNs: 0, RewriteEndNs: hr / 2, SettleEndNs: hr / 2, SupersededNs: hr}}, runner.GateNotJudged, "only a lower bound"},
		{"a replaced pass at the limit", []runner.GateWindow{{StartNs: 0, RewriteEndNs: hr, SettleEndNs: hr, SupersededNs: 2 * hr}}, runner.GateNotJudged, "only a lower bound"},
		// an unfinished pass, its rewrite ending long after the stream (a bound measured to
		// the end of the wait for the store would be 4 hours)
		{"an unfinished pass over the limit", []runner.GateWindow{{StartNs: end - 3*hr, RewriteEndNs: end + hr, SettleEndNs: end + 2*hr}}, runner.GateOver, ""},
		{"an unfinished pass under it", []runner.GateWindow{{StartNs: end - hr, RewriteEndNs: end + 3*hr, SettleEndNs: end + 4*hr}}, runner.GateNotJudged, "only a lower bound"},
		{"a window open when the stream ended", []runner.GateWindow{{StartNs: end - hr, RewriteEndNs: open, SettleEndNs: open}}, runner.GateNotJudged, "only a lower bound"},
		// a pass that finished over the limit fails Q3 whatever else happened
		{"a pass over the limit and a replaced one", []runner.GateWindow{
			{StartNs: 0, RewriteEndNs: 3 * hr, SettleEndNs: 3 * hr},
			{StartNs: 10 * hr, RewriteEndNs: 10*hr + 600e9, SettleEndNs: 10*hr + 600e9, SupersededNs: 10*hr + 600e9},
		}, runner.GateOver, ""},
		{"a pass over the limit and an unfinished one", []runner.GateWindow{
			{StartNs: 0, RewriteEndNs: 3 * hr, SettleEndNs: 3 * hr},
			{StartNs: end - hr, RewriteEndNs: open, SettleEndNs: open},
		}, runner.GateOver, ""},
		{"passes that finished within the limit and a replaced one", []runner.GateWindow{
			{StartNs: 0, RewriteEndNs: hr, SettleEndNs: hr},
			{StartNs: 10 * hr, RewriteEndNs: 10*hr + 600e9, SettleEndNs: 10*hr + 600e9, SupersededNs: 10*hr + 600e9},
		}, runner.GateNotJudged, "only a lower bound"},
		{"passes that all finished within the limit", []runner.GateWindow{
			{StartNs: 0, RewriteEndNs: hr, SettleEndNs: hr},
			{StartNs: 10 * hr, RewriteEndNs: 11 * hr, SettleEndNs: 11 * hr},
		}, runner.GateOK, ""},
	} {
		row, rep := q3Of(t, pacedBuild(c.windows))
		if row.State != c.want {
			t.Errorf("%s: %+v, want %s", c.name, row, c.want)
		}
		if c.why != "" && !strings.Contains(strings.Join(rep.NotJudged, "\n"), c.why) {
			t.Errorf("%s: not judged %q, want it to say %q", c.name, rep.NotJudged, c.why)
		}
		if strings.Contains(strings.Join(rep.NotJudged, "\n"), "never ended") {
			t.Errorf("%s: a window open at the end of the stream refused the build: %q", c.name, rep.NotJudged)
		}
	}
}

// A window open at the end of the stream does not refuse a build in the background or
// its Lroot/off baseline: Q5 is judged against it.
func TestAWindowOpenAtTheEndOfTheStreamDoesNotRefuseABackgroundBuildOrItsBaseline(t *testing.T) {
	t.Parallel()

	open := int64(1<<63 - 1)
	windows := []runner.GateWindow{{StartNs: 0, RewriteEndNs: hr, SettleEndNs: hr}, {StartNs: 999 * hr, RewriteEndNs: open, SettleEndNs: open}}
	rep := runner.JudgeStoreGate([]*runner.StoreGateBuild{
		synth("Lroot/off", trace(func(g *runner.GateTrace) { g.Windows = called(windows) })),
		synth("Lroot/k64a2l1ns", trace(func(g *runner.GateTrace) { g.Windows = called(windows) })),
	})
	if r := find(t, rep, "Lroot/k64a2l1ns", "Q5 peak live"); r.State != runner.GateOK {
		t.Errorf("Q5 against a baseline with a window open at the end: %+v", r)
	}
	if strings.Contains(strings.Join(rep.NotJudged, "\n"), "never ended") {
		t.Errorf("refused: %q", rep.NotJudged)
	}
}

// A window that a later retention replaced ends at that publication for Q4, and one
// open when the stream ended ends at the end of the stream: neither runs on to the end of
// a rewrite the store finished later.
func TestQ4EndsAWindowWhereItIsReplacedOrWhereTheStreamEnds(t *testing.T) {
	t.Parallel()

	for _, c := range []struct {
		name    string
		records int64
		g       func(*runner.GateTrace)
		want    string
	}{
		// 15000 records over the 5 s to the publication that replaced it: 3000 a second
		{"replaced", 15000, func(g *runner.GateTrace) {
			g.Windows = called([]runner.GateWindow{{StartNs: 0, RewriteEndNs: 20e9, SettleEndNs: 20e9, SupersededNs: 5e9}})
		}, runner.GateOK},
		{"replaced, a record fewer", 14999, func(g *runner.GateTrace) {
			g.Windows = called([]runner.GateWindow{{StartNs: 0, RewriteEndNs: 20e9, SettleEndNs: 20e9, SupersededNs: 5e9}})
		}, runner.GateOver},
		// 30000 records over the 10 s to the end of the stream, 100 s into a rewrite that ends at 200 s
		{"open at the end of the stream", 30000, func(g *runner.GateTrace) {
			g.WriteNs, g.StreamEndNs = 100e9, 100e9
			g.Windows = called([]runner.GateWindow{{StartNs: 90e9, RewriteEndNs: 200e9, SettleEndNs: 300e9}})
		}, runner.GateOK},
		{"open at the end of the stream, a record fewer", 29999, func(g *runner.GateTrace) {
			g.WriteNs, g.StreamEndNs = 100e9, 100e9
			g.Windows = called([]runner.GateWindow{{StartNs: 90e9, RewriteEndNs: 200e9, SettleEndNs: 300e9}})
		}, runner.GateOver},
	} {
		b := synth("Lroot/k64a2l1ns", trace(func(g *runner.GateTrace) {
			g.Inside = runner.GateBatches{Batches: 10, Records: c.records, Commit: hist(10, time.Millisecond)}
			c.g(g)
		}))
		if r := find(t, runner.JudgeStoreGate([]*runner.StoreGateBuild{b}), "Lroot/k64a2l1ns", "Q4"); r.State != c.want {
			t.Errorf("%s: %+v, want %s", c.name, r, c.want)
		}
	}
}

// Q3's load is taken over the paced writing time: the records of the paced batches that
// are not excluded over the time from the pacing's start to the end of the stream, less
// the time of the windows that started before it and their settles. The excluded and
// unpaced batches are not in the records, and the time of a judged window is not out of
// the time.
func TestQ3sLoadIsTheLoadOverThePacedWritingTime(t *testing.T) {
	t.Parallel()

	b := synth("Lroot/k64a2l1ns", trace(func(g *runner.GateTrace) {
		g.PaceStartNs, g.WriteNs, g.StreamEndNs = 100e9, 1100e9, 1100e9
		g.Windows = called([]runner.GateWindow{
			{StartNs: 50e9, RewriteEndNs: 150e9, SettleEndNs: 200e9},  // unjudged: its time after 100 s, 100 s, is out
			{StartNs: 500e9, RewriteEndNs: 600e9, SettleEndNs: 700e9}, // judged: its time stays
		})
		// 4.05 million records over 1100 - 100 - 100 = 900 s is 4500 a second
		g.Inside = runner.GateBatches{Batches: 10, Records: 50_000, Commit: hist(10, time.Millisecond)}
		g.Settling = runner.GateBatches{Batches: 10, Records: 50_000, Commit: hist(10, time.Millisecond)}
		g.Outside = runner.GateBatches{Batches: 1000, Records: 3_950_000, Commit: hist(1000, time.Millisecond)}
		g.Excluded = runner.GateBatches{Batches: 5, Records: 999_999_999, Commit: hist(5, time.Millisecond)}
		g.Unpaced = runner.GateBatches{Batches: 5, Records: 777_777_777, Commit: hist(5, time.Millisecond)}
	}))
	row, rep := q3Of(t, b)
	if row.State != runner.GateOK || !strings.Contains(row.Value, "load 4500 records/s over the paced writing time") {
		t.Errorf("%+v (%q), want ok at a load of 4500 records/s over the paced writing time", row, rep.NotJudged)
	}

	// the time paced writing spent resting is out of the time too
	b = synth("Lroot/k64a2l1ns", trace(func(g *runner.GateTrace) {
		g.PaceStartNs, g.WriteNs, g.StreamEndNs, g.RestNs, g.PacedRestNs = 100e9, 1100e9, 1100e9, 400e9, 100e9
		g.Windows = called([]runner.GateWindow{{StartNs: 500e9, RewriteEndNs: 600e9, SettleEndNs: 700e9}})
		g.Inside = runner.GateBatches{}
		g.Outside = runner.GateBatches{Batches: 1000, Records: 4500 * 900, Commit: hist(1000, time.Millisecond)}
	}))
	if row, _ := q3Of(t, b); row.State != runner.GateOK || !strings.Contains(row.Value, "load 4500 records/s") {
		t.Errorf("with a rest: %+v", row)
	}
}

// A build paced at another rate than the pre-registered 4,500 records/s is reported with
// its values and not judged on any row, and so is a build that was not paced; a pace
// start other than the rule's day for the window refuses the build.
func TestOnlyABuildPacedAtTheRulesRateIsJudged(t *testing.T) {
	t.Parallel()

	for _, c := range []struct {
		name string
		mod  func(*runner.StoreGateBuild)
		why  string
	}{
		{"paced at 4000", func(b *runner.StoreGateBuild) { b.Manifest.Describe[runner.PaceKey] = "4000" }, "paced at 4000 records/s, not at the pre-registered offered load of 4500"},
		{"not paced", func(b *runner.StoreGateBuild) {
			b.Manifest.Describe[runner.PaceKey] = "0"
			delete(b.Manifest.Describe, runner.PaceFromDayKey)
		}, "this one was not paced"},
		{"no pace key at all", func(b *runner.StoreGateBuild) {
			delete(b.Manifest.Describe, runner.PaceKey)
			delete(b.Manifest.Describe, runner.PaceFromDayKey)
		}, "this one was not paced"},
		{"paced from another day", func(b *runner.StoreGateBuild) { b.Manifest.Describe[runner.PaceFromDayKey] = "12" }, "pace from day 12, the rule gives 46.5 for the window of 30 days"},
		{"paced from no recorded day", func(b *runner.StoreGateBuild) { delete(b.Manifest.Describe, runner.PaceFromDayKey) }, "pace from day not recorded, the rule gives 46.5"},
		{"keeping a part of a day", func(b *runner.StoreGateBuild) { b.Manifest.Gate.KeepNs += 3600e9 }, "not a whole number of days"},
	} {
		b := synth("Lroot/k64a2l1ns", c.mod)
		rep := runner.JudgeStoreGate([]*runner.StoreGateBuild{b})
		for _, q := range []string{"Q1", "Q2", "Q3", "Q4", "Q5 peak live", "Q5 peak res"} {
			if r := find(t, rep, "Lroot/k64a2l1ns", q); r.State != runner.GateNotJudged {
				t.Errorf("%s: %s is %+v, want not judged", c.name, q, r)
			}
		}
		if !strings.Contains(strings.Join(rep.NotJudged, "\n"), c.why) {
			t.Errorf("%s: not judged %q, want it to say %q", c.name, rep.NotJudged, c.why)
		}
	}

	// reported with its values: the rate other than the rule's still shows what it did
	rep := runner.JudgeStoreGate([]*runner.StoreGateBuild{synth("Lroot/k64a2l1ns", func(b *runner.StoreGateBuild) { b.Manifest.Describe[runner.PaceKey] = "4000" })})
	if r := find(t, rep, "Lroot/k64a2l1ns", "Q1"); !strings.Contains(r.Value, "30ms") {
		t.Errorf("a build paced at 4000: Q1 shows %q, want its value", r.Value)
	}
	var text strings.Builder
	runner.WriteStoreGate(&text, rep)
	if !strings.Contains(text.String(), "paced at 4000 records/s from day 46.5") {
		t.Errorf("the report does not say how the build was paced:\n%s", text.String())
	}

	// the day for a window of 7 days is 11.5
	seven := func(day string) *runner.StoreGateBuild {
		return synth("Lroot/k64a2l1ns", func(b *runner.StoreGateBuild) {
			b.Manifest.Gate.KeepNs = 7 * 24 * hr
			b.Manifest.Describe[runner.PaceFromDayKey] = day
		})
	}
	if rep := runner.JudgeStoreGate([]*runner.StoreGateBuild{seven("12")}); !strings.Contains(strings.Join(rep.NotJudged, "\n"), "pace from day 12, the rule gives 11.5 for the window of 7 days") {
		t.Errorf("12 for 7 days: %q", rep.NotJudged)
	}
	if rep := runner.JudgeStoreGate([]*runner.StoreGateBuild{seven("11.5")}); strings.Contains(strings.Join(rep.NotJudged, "\n"), "pace from day") {
		t.Errorf("11.5 for 7 days: %q", rep.NotJudged)
	}
}

// A build that says it was paced and whose trace has no start of the pacing is refused:
// it cannot be told which windows are judged. A build that says it was not paced has
// none, and is reported as unpaced.
func TestAPacedBuildWithoutTheStartOfItsPacingIsRefused(t *testing.T) {
	t.Parallel()

	paced := synth("Lroot/k64a2l1ns")
	paced.Manifest.Gate.PaceStartNs = 0
	rep := runner.JudgeStoreGate([]*runner.StoreGateBuild{paced})
	for _, q := range []string{"Q1", "Q2", "Q3", "Q4", "Q5 peak live", "Q5 peak res"} {
		if r := find(t, rep, "Lroot/k64a2l1ns", q); r.State != runner.GateNotJudged {
			t.Errorf("%s of a build that was paced and whose pacing never started: %+v, want not judged", q, r)
		}
	}
	if got := strings.Join(rep.NotJudged, "\n"); !strings.Contains(got, "paced, but the pacing never started") {
		t.Errorf("not judged %q, want it to say the pacing never started", got)
	}
	// its Lroot/off baseline cannot be used either
	base := synth("Lroot/off")
	base.Manifest.Gate.PaceStartNs = 0
	rep = runner.JudgeStoreGate([]*runner.StoreGateBuild{base, synth("Lroot/k64a2l1ns")})
	if r := find(t, rep, "Lroot/k64a2l1ns", "Q5 peak live"); r.State != runner.GateNotJudged {
		t.Errorf("Q5 against a baseline whose pacing never started: %+v", r)
	}

	unpaced := synth("Lroot/k64a2l1ns", func(b *runner.StoreGateBuild) {
		b.Manifest.Describe[runner.PaceKey] = "0"
		delete(b.Manifest.Describe, runner.PaceFromDayKey)
	})
	rep = runner.JudgeStoreGate([]*runner.StoreGateBuild{unpaced})
	got := strings.Join(rep.NotJudged, "\n")
	if strings.Contains(got, "never started") || !strings.Contains(got, "this one was not paced") {
		t.Errorf("a build that was not paced: %q", got)
	}
}

// The report of a build in the background says the Q3 note, how the build was paced, the
// batches it divided into, the number Q2's percentile is taken over, and for each pass
// the time of its rewrite (or the least it took), of its settle, and whether that settle
// reached its deadline, and the passes with only a lower bound.
func TestTheReportOfABackgroundBuildShowsEachPass(t *testing.T) {
	t.Parallel()

	b := synth("Lroot/k64a2l1ns", trace(func(g *runner.GateTrace) {
		g.PaceStartNs = 5e9
		g.Unpaced = runner.GateBatches{Batches: 7}
		g.Excluded = runner.GateBatches{Batches: 3}
		g.Windows = called([]runner.GateWindow{
			{StartNs: 1e9, RewriteEndNs: 3e9, SettleEndNs: 4e9, SettleDeadlineHit: true},    // before the pacing: not judged
			{StartNs: 10e9, RewriteEndNs: 12e9, SettleEndNs: 15e9},                          // settled in time
			{StartNs: 20e9, RewriteEndNs: 22e9, SettleEndNs: 30e9, SettleDeadlineHit: true}, // reached its deadline
			{StartNs: 40e9, RewriteEndNs: 41e9, SettleEndNs: 41e9, SupersededNs: 41e9},      // replaced in its rewrite
			{StartNs: 50e9, RewriteEndNs: 1<<63 - 1, SettleEndNs: 1<<63 - 1},                // not ended when the stream ended
		})
	}))
	rep := runner.JudgeStoreGate([]*runner.StoreGateBuild{b})
	var text strings.Builder
	runner.WriteStoreGate(&text, rep)
	out := text.String()
	for _, want := range []string{
		runner.StoreGateQ3Note,
		"paced at 4500 records/s from day 46.5",
		"7 unpaced, 3 excluded",
		"Q2 is taken over 100 inside batches",
		"pass 1 (before the day the pacing started, not judged): published at 1s, rewrite 2s, settle 1s, settle reached its deadline: yes",
		"pass 2 (judged): published at 10s, rewrite 2s, settle 3s, settle reached its deadline: no",
		"pass 3 (judged): published at 20s, rewrite 2s, settle 8s, settle reached its deadline: yes",
		"pass 4 (judged): published at 40s, rewrite at least 1s (replaced by the next retention), settle none (the pass was not settled), settle reached its deadline: -",
		"pass 5 (judged): published at 50s, rewrite at least 15m50s (not ended when the stream ended)",
		"2 judged passes have only a lower bound of their rewrite: pass 4: at least 1s; pass 5: at least 15m50s",
		"commits synced: false",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("the report lacks %q:\n%s", want, out)
		}
	}
	if r := find(t, rep, "Lroot/k64a2l1ns", "Q2"); !strings.Contains(r.Value, "over 100 inside batches") {
		t.Errorf("Q2 shows %q, want the number of inside batches", r.Value)
	}
	// a report with no build in the background does not say the Q3 note
	var syncText strings.Builder
	runner.WriteStoreGate(&syncText, runner.JudgeStoreGate([]*runner.StoreGateBuild{synthSync("Lroot/off")}))
	if strings.Contains(syncText.String(), runner.StoreGateQ3Note) {
		t.Error("the Q3 note is in a report with no build in the background")
	}
}

// windowOfADayPlan is the plan of a window of one day (a stream of three and a half days
// with a retention a day from the second on) of a workload made light enough to build in
// a test; it is made once.
func windowOfADayPlan(t *testing.T) *runner.Plan {
	t.Helper()
	windowPlanOnce.Do(func() {
		spec := tinySpec()
		w := &spec.Workload
		w.Racks, w.Hosts, w.Pods, w.Services = 2, 3, 12, 3
		w.EventsPerSecond = 0.01
		w.HeartbeatInterval, w.RollupInterval = 20*time.Minute, 20*time.Minute
		windowPlan, windowPlanErr = runner.MakePlan(context.Background(), runner.WindowSpec(spec, 1), nil)
	})
	if windowPlanErr != nil {
		t.Fatal(windowPlanErr)
	}
	return windowPlan
}

var (
	windowPlanOnce sync.Once
	windowPlan     *runner.Plan
	windowPlanErr  error
)

// A build is refused before it writes anything when its pacing cannot be judged: a
// negative rate, a candidate that is not built through the root store, a retention that
// is synchronous (or has no mode, which is the store's own, synchronous), and a plan
// that is not a window plan.
func TestABuildThatCannotBePacedIsRefusedBeforeItWrites(t *testing.T) {
	t.Parallel()

	windowPlan := windowOfADayPlan(t)
	for _, c := range []struct {
		name string
		plan *runner.Plan
		cand string
		opts runner.BuildOptions
		why  string
	}{
		{"a negative rate", windowPlan, "Lroot/off", runner.BuildOptions{RetentionMode: "background", Pace: -1}, "a rate in records per second"},
		{"a candidate not through the root store", windowPlan, "L/off", runner.BuildOptions{RetentionMode: "background", Pace: 4500}, "not built through the root store"},
		{"a synchronous retention", windowPlan, "Lroot/off", runner.BuildOptions{RetentionMode: "sync", Pace: 4500}, "a build whose retention is synchronous is not paced"},
		{"no retention mode", windowPlan, "Lroot/off", runner.BuildOptions{Pace: 4500}, "a build whose retention is synchronous is not paced"},
		{"a plan that is not a window plan", mustPlan(t, tinySpec()), "Lroot/off", runner.BuildOptions{RetentionMode: "background", Pace: 4500}, "not a window plan"},
	} {
		dir := filepath.Join(t.TempDir(), "c")
		_, err := runner.BuildWith(context.Background(), c.plan, lookup(t, c.cand), dir, clean, c.opts, nil)
		if err == nil || !strings.Contains(err.Error(), c.why) {
			t.Errorf("%s: %v, want a refusal saying %q", c.name, err, c.why)
		}
		if _, statErr := os.Stat(dir); statErr == nil {
			t.Errorf("%s: the build made %s before it refused", c.name, dir)
		}
	}
}

// A timed build in the background, paced, of a window of one day (so paced from day 2.5)
// records windows with all four instants, the start of the pacing and the end of the
// stream; the batches before the pacing are unpaced, none of the unpaced is inside or
// outside; its description says how it was paced; and the gate judges or reports every
// row with a reason, none for want of a window. The rate is far above the rule's, so
// that the build is quick and its rows are reported and not judged for that.
func TestAPacedBackgroundBuildRecordsItsPacingAndItsWindows(t *testing.T) {
	t.Parallel()

	plan := windowOfADayPlan(t)
	out := t.TempDir()
	v := lookup(t, "Lroot/k64a2l1ns")
	dir := runner.CandidateDir(out, v.Name)
	m, err := runner.BuildWith(context.Background(), plan, v, dir, clean, runner.BuildOptions{MetricsEvery: 50 * time.Millisecond, RetentionMode: candidates.RetentionBackground, Pace: 200_000}, nil)
	if err != nil {
		t.Fatal(err)
	}
	g := m.Gate
	if m.Untimed {
		t.Fatal("an untimed build")
	}
	if g == nil || g.PaceStartNs <= 0 || g.StreamEndNs <= g.PaceStartNs {
		t.Fatalf("pacing started at %v and the stream ended at %v", g.PaceStartNs, g.StreamEndNs)
	}
	if m.Describe[runner.PaceKey] != "200000" || m.Describe[runner.PaceFromDayKey] != "2.5" {
		t.Errorf("described as paced at %q from day %q, want 200000 from 2.5", m.Describe[runner.PaceKey], m.Describe[runner.PaceFromDayKey])
	}
	// Without the option, nothing of the profiles is written or described.
	if _, err := os.Stat(filepath.Join(dir, runner.ProfilesDir)); !os.IsNotExist(err) {
		t.Errorf("a build without profiles has %s: %v", runner.ProfilesDir, err)
	}
	if v, ok := m.Describe[runner.ProfileWindowsKey]; ok {
		t.Errorf("a build without profiles is described with %s %q", runner.ProfileWindowsKey, v)
	}
	if g.Unpaced.Batches == 0 {
		t.Error("no batch was written before the pacing began")
	}
	if all := g.Unpaced.Batches + g.Excluded.Batches + g.Inside.Batches + g.Settling.Batches + g.Outside.Batches; all != m.Timing.Writes.Count {
		t.Errorf("the batches divide into %d, the build wrote %d", all, m.Timing.Writes.Count)
	}
	if len(g.Windows) != 2 {
		t.Fatalf("%d windows for %d retentions: %+v", len(g.Windows), len(m.Stream.Retentions), g.Windows)
	}
	open := int64(1<<63 - 1)
	for i, w := range g.Windows {
		if w.RewriteEndNs == open || w.SettleEndNs == open || w.RewriteEndNs < w.StartNs || w.SettleEndNs < w.RewriteEndNs {
			t.Errorf("window %d: %+v", i, w)
		}
	}
	if g.Windows[0].StartNs >= g.PaceStartNs {
		t.Errorf("the first retention (day 2) was published at %d, after the pacing began at %d", g.Windows[0].StartNs, g.PaceStartNs)
	}
	if g.Windows[1].StartNs < g.PaceStartNs {
		t.Errorf("the second retention (day 3) was published at %d, before the pacing began at %d", g.Windows[1].StartNs, g.PaceStartNs)
	}
	builds, err := runner.LoadStoreGateBuilds([]string{out})
	if err != nil {
		t.Fatal(err)
	}
	rep := runner.JudgeStoreGate(builds)
	if rep.Refusal != "" || len(rep.Rows) != 6 {
		t.Fatalf("refusal %q, %d rows", rep.Refusal, len(rep.Rows))
	}
	for _, r := range rep.Rows {
		if r.State == runner.GateNotJudged && !strings.Contains(strings.Join(rep.NotJudged, "\n"), "paced at 200000 records/s, not at the pre-registered") {
			t.Errorf("%s is not judged, and the list says nothing of the rate: %q", r.Gate, rep.NotJudged)
		}
	}
	if strings.Contains(strings.Join(rep.NotJudged, "\n"), "no judged window") {
		t.Errorf("the build has a judged window and the gate found none: %q", rep.NotJudged)
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
		return synth("Lroot/k64a2l1ns", func(b *runner.StoreGateBuild) {
			b.Dir += "/" + string(rune('0'+n))
			b.Job = &runner.Job{RunID: "1", RunAttempt: "1", Rep: n}
		}, trace(mod))
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
