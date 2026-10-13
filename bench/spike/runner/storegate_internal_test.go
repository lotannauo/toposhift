package runner

import (
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
)

var epoch = time.Date(2026, 10, 10, 0, 0, 0, 0, time.UTC)

func at(ns int64) time.Time { return epoch.Add(time.Duration(ns)) }

// A batch is inside a window if it overlaps it, its first and last instants included:
// one that ends at the publication, one that starts at the end of the rewrite and one
// that spans the window are inside, and one a nanosecond clear of it on either side is
// outside. A batch inside an open window (a rewrite that has not ended) is inside,
// whatever its length, and so is one that ends after the window opens.
func TestABatchThatTouchesAWindowIsInside(t *testing.T) {
	t.Parallel()

	for _, c := range []struct {
		name       string
		start, dur int64
		inside     bool
	}{
		{"ends a nanosecond before the publication", 50, 49, false},
		{"ends at the publication", 50, 50, true},
		{"starts a nanosecond after the rewrite ends", 201, 10, false},
		{"starts as the rewrite ends", 200, 10, true},
		{"spans the window", 50, 300, true},
		{"within the window", 120, 10, true},
		{"after the settle", 400, 10, false},
	} {
		g := newGateTracker(epoch)
		g.publish(at(100))
		g.rewriteEnded(at(200))
		g.settleEnded(at(200)) // no settle: a batch after the window is outside
		g.returned(at(200))
		g.batch(at(c.start), time.Duration(c.dur), 7)
		if got := g.inside.Batches == 1 && g.inside.Records == 7 && g.outside.Batches == 0; got != c.inside {
			t.Errorf("%s: inside %+v, outside %+v, want inside %v", c.name, g.inside, g.outside, c.inside)
		}
		if g.inside.Batches+g.outside.Batches != 1 || g.inside.Commit.Count+g.outside.Commit.Count != 1 {
			t.Errorf("%s: the batch is counted %d times", c.name, g.inside.Batches+g.outside.Batches)
		}
	}

	g := newGateTracker(epoch)
	g.batch(at(10), 5, 1) // before any window
	g.publish(at(100))
	g.batch(at(90), 11, 1)   // ends after the publication
	g.batch(at(5000), 1, 1)  // in the open window
	g.rewriteEnded(at(6000)) // closes it
	g.settleEnded(at(6000))
	g.batch(at(7000), 1, 1)  // outside
	g.publish(at(8000))      // a second window
	g.rewriteEnded(at(8100)) // closed
	g.settleEnded(at(8100))
	g.batch(at(7990), 20, 1) // ends inside the second
	g.batch(at(9000), 1, 1)  // outside again
	if g.inside.Batches != 3 || g.outside.Batches != 3 {
		t.Errorf("inside %d, outside %d, want 3 and 3", g.inside.Batches, g.outside.Batches)
	}
}

// A retention that did all of its work before it returned makes one window, from the
// call to the end of the rewrite, with the end of the settle and the return of the call
// as they were; one that rewrote nothing makes none; every method accepts no tracker.
func TestASynchronousRetentionMakesOneWindow(t *testing.T) {
	t.Parallel()

	g := newGateTracker(epoch)
	g.syncWindow(at(1000), at(1700), 400, 100, 150)
	g.syncWindow(at(2000), at(2005), 0, 0, 0)
	want := []GateWindow{{StartNs: 1000, RewriteEndNs: 1400, SettleEndNs: 1650, ReturnNs: 1700}}
	if len(g.windows) != 1 || g.windows[0] != want[0] {
		t.Errorf("windows %+v, want %+v", g.windows, want)
	}
	g.rested(3 * time.Second)
	g.rested(time.Second)
	tr := g.trace(at(10_000), "sync", 36*time.Hour, map[string]int64{"x": 1})
	if tr.WriteNs != 10_000 || tr.RestNs != int64(4*time.Second) || tr.KeepNs != int64(36*time.Hour) || tr.Mode != "sync" || len(tr.Windows) != 1 {
		t.Errorf("%+v", tr)
	}
	tr.Windows[0].StartNs = 1
	if g.windows[0].StartNs != 1000 {
		t.Error("the trace shares the tracker's windows")
	}

	var none *gateTracker
	none.publish(at(0))
	none.rewriteEnded(at(0))
	none.settleEnded(at(0))
	none.returned(at(0))
	none.syncWindow(at(0), at(1), 1, 1, 1)
	none.batch(at(0), 1, 1)
	none.rested(1)
}

// The recorder keeps the largest of the samples a retention reports and passes every
// count and sample on.
func TestTheMaxRecorderKeepsTheLargestSamples(t *testing.T) {
	t.Parallel()

	c := NewCapture()
	m := newMaxRecorder(c)
	for _, v := range []int64{5, 90, 7} {
		m.Sample("retain.chunk_hold_ns", v)
	}
	m.Sample("retain.max_prefix_records", 3)
	m.Sample("read.other", 4)
	m.Count("retain.seeks", 2)
	if got := m.maxima(); got["retain.chunk_hold_ns"] != 90 || got["retain.max_prefix_records"] != 3 || len(got) != 2 {
		t.Errorf("maxima %v", got)
	}
	if got := c.Totals(); got["retain.chunk_hold_ns"] != 102 || got["read.other"] != 4 || got["retain.seeks"] != 2 {
		t.Errorf("totals %v: the wrapped recorder did not see everything", got)
	}
}

// The peak live heap and the peak tombstones are read off a metrics file; a file that is
// not there has none, and a line that is not JSON is an error naming it.
func TestTheMetricsFileGivesThePeakLiveHeap(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, MetricsFile)
	if got, err := readStoreGateMetrics(path); err != nil || got != (StoreGateMetrics{}) {
		t.Errorf("no file: %+v, %v", got, err)
	}
	lines := `{"elapsed_ms":1,"phase":"write","batches":1,"retentions":0,"stats":{"tombstones":4},"go":{"total":9,"heap_live":0}}
{"elapsed_ms":2,"phase":"write","batches":2,"retentions":0,"stats":{"tombstones":40},"go":{"total":9,"heap_live":300}}
{"elapsed_ms":3,"phase":"done","batches":2,"retentions":1,"stats":{"tombstones":9},"go":{"total":9,"heap_live":200}}
`
	if err := os.WriteFile(path, []byte(lines), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := readStoreGateMetrics(path)
	if err != nil || got != (StoreGateMetrics{Lines: 3, HeapLiveSamples: 2, HeapLivePeak: 300, TombstonesPeak: 40}) {
		t.Errorf("%+v, %v", got, err)
	}
	if err := os.WriteFile(path, []byte(lines+"not json\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := readStoreGateMetrics(path); err == nil || !strings.Contains(err.Error(), "line 4") {
		t.Errorf("a damaged line: %v", err)
	}
}

// The GC target is recorded as the percent, or off; the process has a peak resident set
// wherever the system says it.
func TestTheGoSettingsAndTheResidentSetAreRead(t *testing.T) {
	t.Parallel()

	for n, want := range map[int]string{100: "100", 50: "50", 0: "0", -1: "off"} {
		if got := gcPercentText(n); got != want {
			t.Errorf("gcPercentText(%d) = %q, want %q", n, got, want)
		}
	}
	if runtime.GOOS == "linux" || runtime.GOOS == "darwin" {
		if n := peakRSS(); n < 1<<20 {
			t.Errorf("peak resident set %d bytes", n)
		}
	}
}

// A batch that overlaps a settle and no window is settling, and is neither inside nor
// outside; one that overlaps a window and a settle is inside. A batch that touches a
// window only in part adds the rest of its span to the inside's spill, once.
func TestABatchInASettleIsCountedApartFromTheOutside(t *testing.T) {
	t.Parallel()

	g := newGateTracker(epoch)
	g.publish(at(100))
	g.rewriteEnded(at(200))
	g.settleEnded(at(300))
	g.batch(at(250), 10, 5)  // in the settle
	g.batch(at(301), 10, 5)  // after it
	g.batch(at(300), 10, 5)  // as it ends
	g.batch(at(150), 100, 7) // from the window into the settle: inside
	if g.settling.Batches != 2 || g.settling.Records != 10 || g.outside.Batches != 1 || g.inside.Batches != 1 {
		t.Errorf("settling %+v, outside %+v, inside %+v", g.settling, g.outside, g.inside)
	}
	if g.settling.Commit.Count != 2 || g.outside.Commit.Count != 1 {
		t.Errorf("the durations are not counted with the batches")
	}
	// the batch from 150 to 250 is inside from 150 to 200: 50 ns of it is outside the window
	if g.inside.SpillNs != 50 {
		t.Errorf("spill %d, want 50", g.inside.SpillNs)
	}

	// a big batch over a short window: all its records, and the rest of its span as spill
	h := newGateTracker(epoch)
	h.publish(at(100))
	h.rewriteEnded(at(200))
	h.settleEnded(at(200))
	h.batch(at(0), 1000, 10)
	if h.inside.Records != 10 || h.inside.SpillNs != 900 {
		t.Errorf("inside %+v, want 10 records and a spill of 900 ns", h.inside)
	}
	// while the rewrite is going on, and after it, before the settle's end is known
	o := newGateTracker(epoch)
	o.publish(at(100))
	o.batch(at(500), 10, 1) // the window is open: inside
	o.rewriteEnded(at(600))
	o.batch(at(700), 10, 1) // the settle's end is not known yet: settling
	if o.inside.Batches != 1 || o.settling.Batches != 1 || o.outside.Batches != 0 {
		t.Errorf("inside %d, settling %d, outside %d", o.inside.Batches, o.settling.Batches, o.outside.Batches)
	}
}

// The bounds of a histogram's bucket hold the value that was counted in it.
func TestABucketHoldsTheValuesItWasCountedFor(t *testing.T) {
	t.Parallel()

	for _, d := range []int64{0, 1, 7, 8, 9, 15, 16, 100, 1000, 12345, 92274688, 100000000, 100663295, 100663296, 199500000, 1 << 40} {
		var h Histogram
		h.Add(time.Duration(d))
		lo, hi := quantileBounds(h, 0.99)
		if d >= 8 && (lo > d || d >= hi) || d < 8 && (lo != d || hi != d) {
			t.Errorf("%d is counted in the bucket [%d, %d)", d, lo, hi)
		}
		if hi != h.Quantile(0.99) {
			t.Errorf("%d: the top of the bucket %d is not the histogram's quantile %d", d, hi, h.Quantile(0.99))
		}
	}
	if lo, hi := quantileBounds(Histogram{}, 0.5); lo != 0 || hi != 0 {
		t.Errorf("no durations: %d, %d", lo, hi)
	}
}

// With many windows the batch is classified as a scan of all of them would classify
// it: inside, settling or outside, and the spill. Random batches of fixed seeds are
// placed around a thousand ordered windows, some with settles that run into the next
// window's publication gap and some without.
func TestTheEarlyExitOfTheWindowScanChangesNothing(t *testing.T) {
	t.Parallel()

	for seed := uint64(1); seed <= 3; seed++ {
		rng := rand.New(rand.NewPCG(seed, 7))
		g := newGateTracker(epoch)
		var ws []GateWindow
		at0 := int64(0)
		for range 1000 {
			start := at0 + 1 + rng.Int64N(1000)
			rewrite := start + rng.Int64N(500)
			settle := rewrite + rng.Int64N(600) // may reach past the next publication's gap, never past it
			ws = append(ws, GateWindow{StartNs: start, RewriteEndNs: rewrite, SettleEndNs: settle, ReturnNs: start})
			at0 = settle
		}
		g.windows = ws
		var inside, settling, outside, spill int64
		for range 20000 {
			s := rng.Int64N(at0 + 1000)
			d := rng.Int64N(300)
			e := s + d
			in, st := false, false
			var overlap int64
			for _, w := range ws { // the scan of all of them
				switch {
				case w.StartNs <= e && s <= w.RewriteEndNs:
					in = true
					overlap += min(e, w.RewriteEndNs) - max(s, w.StartNs)
				case s <= w.SettleEndNs && e >= w.RewriteEndNs:
					st = true
				}
			}
			switch {
			case in:
				inside++
				spill += max(d-overlap, 0)
			case st:
				settling++
			default:
				outside++
			}
			g.batch(at(s), time.Duration(d), 1)
		}
		if g.inside.Batches != inside || g.settling.Batches != settling || g.outside.Batches != outside || g.inside.SpillNs != spill {
			t.Errorf("seed %d: inside %d settling %d outside %d spill %d, a scan of every window gives %d, %d, %d, %d",
				seed, g.inside.Batches, g.settling.Batches, g.outside.Batches, g.inside.SpillNs, inside, settling, outside, spill)
		}
		if inside == 0 || settling == 0 || outside == 0 {
			t.Errorf("seed %d: the batches do not cover all three classes (%d, %d, %d)", seed, inside, settling, outside)
		}
	}
}

// win is a window with the instants given, none replaced and no deadline hit.
func win(start, rewriteEnd, settleEnd, ret int64) GateWindow {
	return GateWindow{StartNs: start, RewriteEndNs: rewriteEnd, SettleEndNs: settleEnd, ReturnNs: ret}
}

// replaced is w, replaced in its rewrite by the publication at ns.
func (w GateWindow) replaced(ns int64) GateWindow { w.SupersededNs = ns; return w }

// The observer of a store that retains in the background gives the tracker the
// windows its generations make, in whatever order the calls arrive in: a retention a
// later publication replaces ends there, in its rewrite (and is marked replaced) or in
// its settle; a phase ends at the earliest instant it is given; a generation the
// tracker was not told the publication of has no window.
func TestTheObserverFeedsTheWindowsWhateverTheOrderOfTheCalls(t *testing.T) {
	t.Parallel()

	const open = gateWindowOpen
	type call struct {
		what string
		gen  uint64
		ns   int64
	}
	p := func(gen uint64, ns int64) call { return call{"published", gen, ns} }
	r := func(gen uint64, ns int64) call { return call{"rewrite", gen, ns} }
	s := func(gen uint64, ns int64) call { return call{"settle", gen, ns} }
	for _, c := range []struct {
		name  string
		calls []call
		want  []GateWindow
	}{
		{"one retention", []call{p(1, 100), r(1, 200), s(1, 300)}, []GateWindow{win(100, 200, 300, 100)}},
		{"a later report of an end does not move it", []call{p(1, 100), r(1, 200), r(1, 260), s(1, 300), s(1, 350)}, []GateWindow{win(100, 200, 300, 100)}},
		{"one that has not ended", []call{p(1, 100), r(1, 200)}, []GateWindow{win(100, 200, open, 100)}},
		{
			// the store's real order: the replaced pass's rewrite ends at the instant of the
			// publication that replaces it
			"replaced while it rewrites",
			[]call{p(1, 100), r(1, 150), p(2, 150), r(2, 300), s(2, 400)},
			[]GateWindow{win(100, 150, 150, 100).replaced(150), win(150, 300, 400, 150)},
		},
		{
			"replaced while it rewrites, the publication arriving first",
			[]call{p(1, 100), p(2, 150), r(1, 150), r(2, 300), s(2, 400)},
			[]GateWindow{win(100, 150, 150, 100).replaced(150), win(150, 300, 400, 150)},
		},
		{
			"replaced while it settles",
			[]call{p(1, 100), r(1, 200), p(2, 250), r(2, 300), s(2, 400)},
			[]GateWindow{win(100, 200, 250, 100), win(250, 300, 400, 250)},
		},
		{
			// the end of the rewrite, told late, was before the publication that replaced the
			// pass: it was its settle that was replaced
			"replaced while it settles, the report of its rewrite arriving late",
			[]call{p(1, 100), p(2, 250), r(1, 200), r(2, 300), s(2, 400)},
			[]GateWindow{win(100, 200, 250, 100), win(250, 300, 400, 250)},
		},
		{
			"replaced while it settles, after its settle had ended, the report arriving late",
			[]call{p(1, 100), r(1, 200), p(2, 250), s(1, 230), r(2, 300), s(2, 400)},
			[]GateWindow{win(100, 200, 230, 100), win(250, 300, 400, 250)},
		},
		{
			"three, each replaced by the next",
			[]call{p(1, 100), p(2, 200), p(3, 300), r(3, 500), s(3, 600)},
			[]GateWindow{win(100, 200, 200, 100).replaced(200), win(200, 300, 300, 200).replaced(300), win(300, 500, 600, 300)},
		},
		{"a generation that was not published here", []call{r(9, 500), s(9, 600), p(10, 700), r(10, 800), s(10, 900)}, []GateWindow{win(700, 800, 900, 700)}},
	} {
		g := newGateTracker(epoch)
		obs := gateObserver{g}
		for _, k := range c.calls {
			switch k.what {
			case "published":
				obs.Published(k.gen, at(k.ns))
			case "rewrite":
				obs.RewriteEnded(k.gen, at(k.ns))
			case "settle":
				obs.SettleEnded(k.gen, at(k.ns))
			}
		}
		if !slices.Equal(g.windows, c.want) {
			t.Errorf("%s: windows %+v, want %+v", c.name, g.windows, c.want)
		}
	}

	var none *gateTracker // a build that is not through the root store has no tracker
	gateObserver{none}.Published(1, at(1))
	gateObserver{none}.RewriteEnded(1, at(2))
	gateObserver{none}.SettleEnded(1, at(3))
	none.settleCounted(1)
}

// The store counts each settle, then tells the observer it ended, from one goroutine;
// the tracker gives each window the count of its own settle: a pass that was replaced as
// it settled counts a settle and is never told it ended, and the pass after it, which
// has none to count, does not take that count. Counts of 1, 0, 1 are matched to the
// three passes they belong to, not to the first three that end.
func TestEachWindowTakesTheDeadlineCountOfItsOwnSettle(t *testing.T) {
	t.Parallel()

	// The interleaving of internal/store/pebblestore/retain.go: a Retain reports
	// RewriteEnded(old) and Published(new) at one instant for a pass it replaces in its
	// rewrite; the retainer reports RewriteEnded, then counts the settle, then reports
	// SettleEnded; a pass replaced as it settles counts its settle after the Retain's
	// Published and is not reported ended.
	g := newGateTracker(epoch)
	obs := gateObserver{g}
	obs.Published(1, at(100))
	obs.RewriteEnded(1, at(200))
	g.settleCounted(1) // pass 1 settles and reaches its deadline
	obs.SettleEnded(1, at(300))

	obs.Published(2, at(400))
	obs.RewriteEnded(2, at(500))
	// A Retain publishes pass 3 while pass 2 settles.
	obs.Published(3, at(550))
	g.settleCounted(1) // pass 2's settle ends after that, reaches its deadline, and is not reported
	// Pass 3's rewrite ends; it has no settle to count (nothing to settle).
	obs.RewriteEnded(3, at(600))
	obs.SettleEnded(3, at(610))

	obs.Published(4, at(700))
	obs.RewriteEnded(4, at(800))
	g.settleCounted(0)
	obs.SettleEnded(4, at(900))

	obs.Published(5, at(1000))
	obs.RewriteEnded(5, at(1100))
	g.settleCounted(1)
	obs.SettleEnded(5, at(1200))

	want := []bool{true, false, false, false, true}
	for i, w := range g.windows {
		if w.SettleDeadlineHit != want[i] {
			t.Errorf("window %d: deadline hit %v, want %v (%+v)", i+1, w.SettleDeadlineHit, want[i], g.windows)
		}
	}
	if len(g.windows) != len(want) {
		t.Fatalf("%d windows", len(g.windows))
	}
}

// The instant a call returned is set on the window that call made, and on none if it
// made none (a retention that moved nothing).
func TestTheReturnOfACallIsSetOnTheWindowItMade(t *testing.T) {
	t.Parallel()

	g := newGateTracker(epoch)
	obs := gateObserver{g}
	n := g.count()
	obs.Published(1, at(100))
	g.returnedSince(n, at(130))
	n = g.count()
	g.returnedSince(n, at(900)) // a call that published nothing
	obs.RewriteEnded(1, at(200))
	obs.SettleEnded(1, at(300))
	if want := []GateWindow{win(100, 200, 300, 130)}; !slices.Equal(g.windows, want) {
		t.Errorf("windows %+v, want %+v", g.windows, want)
	}
	var none *gateTracker
	if none.count() != 0 {
		t.Error("a nil tracker has windows")
	}
	none.returnedSince(0, at(1))
}

// What the tracker makes of a store's report is judged: a build whose batches are in
// every part (inside a window, in a settle, outside) is judged on all six rows, and the
// rules are the ones whose digest was pinned before any build was judged.
func TestTheBackgroundWindowsOfAStoreReportJudgeAllSixRows(t *testing.T) {
	t.Parallel()

	const oneGiB = 1 << 30
	g := newGateTracker(epoch)
	g.setPaced()
	g.startPacing(at(9e8)) // the batches before it are unpaced
	obs := gateObserver{g}
	// Twenty seconds of batches of 100 records every 25 ms (4000 records a second),
	// with a retention published at 1 s that rewrites for 10 s and settles for 2.
	obs.Published(1, at(1e9))
	g.returnedSince(0, at(1e9+1e6))
	obs.RewriteEnded(1, at(11e9))
	obs.SettleEnded(1, at(13e9))
	for ms := int64(0); ms < 20000; ms += 25 {
		g.batch(at(ms*1e6), 20*time.Millisecond, 100)
	}
	trace := g.trace(at(20e9), "background", 30*24*time.Hour, map[string]int64{"retain.max_prefix_records": 12, "retain.chunk_hold_ns": int64(30 * time.Millisecond)})
	trace.PeakRSS = 2 * oneGiB
	if len(trace.Windows) != 1 || trace.Inside.Batches == 0 || trace.Outside.Batches == 0 || trace.Settling.Batches == 0 {
		t.Fatalf("windows %v, inside %d, settling %d, outside %d batches", trace.Windows, trace.Inside.Batches, trace.Settling.Batches, trace.Outside.Batches)
	}
	build := func(cand string) *StoreGateBuild { return pacedBuildOf(cand, trace) }
	rep := JudgeStoreGate([]*StoreGateBuild{build("Lroot/off"), build("Lroot/k64a2l1ns")})
	judged := map[string]int{}
	for _, row := range rep.Rows {
		judged[row.Gate[:2]]++
		if row.State == GateNotJudged || row.State == GateOver {
			t.Errorf("%s %s: %+v, want it judged and within its limit", row.Candidate, row.Gate, row)
		}
	}
	if len(rep.Rows) != 12 || len(rep.NotJudged) != 0 || rep.Over != 0 {
		t.Errorf("%d rows, not judged %v, over %d: want six rows for each of two builds, all judged", len(rep.Rows), rep.NotJudged, rep.Over)
	}
}

// pacedBuildOf is a build in the background, paced at the rule's rate from the day it
// gives for the 30 days it kept, with the trace given.
func pacedBuildOf(cand string, trace *GateTrace) *StoreGateBuild {
	const oneGiB = 1 << 30
	return &StoreGateBuild{
		Dir: "/builds/" + strings.ReplaceAll(cand, "/", "_"),
		Manifest: &Manifest{
			Candidate: cand, Layout: "L", PlanDigest: "plan", Build: BuildInfo{GOOS: "linux", GOARCH: "arm64"},
			Describe: map[string]string{GoGCKey: "100", GoMemoryLimitKey: "11811160064", RetentionModeKey: "background", PaceKey: "4500", PaceFromDayKey: "46.5"},
			Gate:     trace,
		},
		Metrics: StoreGateMetrics{Lines: 5, HeapLiveSamples: 5, HeapLivePeak: oneGiB},
	}
}

// In a paced build a batch that starts before the pacing did is unpaced, and one that
// overlaps a window that started before it, or that window's settle, is excluded; the
// others are inside, settling or outside as they always were. None of the unpaced or
// excluded is inside, settling or outside.
func TestAPacedBuildDividesItsBatchesByThePacingStartAndTheWindowsBefore(t *testing.T) {
	t.Parallel()

	g := newGateTracker(epoch)
	g.setPaced()
	obs := gateObserver{g}
	obs.Published(1, at(50)) // before the pacing: an unjudged window
	g.batch(at(60), 10, 1)   // starts before the pacing does
	g.rested(7)              // a rest before the pacing is not a rest of the paced time
	g.startPacing(at(100))
	g.startPacing(at(120)) // once
	obs.RewriteEnded(1, at(150))
	obs.SettleEnded(1, at(200))
	g.batch(at(110), 5, 2)   // beside the rewrite of the unjudged window
	g.batch(at(170), 5, 3)   // beside its settle
	g.batch(at(140), 100, 5) // spanning both
	g.batch(at(250), 5, 7)   // outside everything
	obs.Published(2, at(300))
	obs.RewriteEnded(2, at(400))
	obs.SettleEnded(2, at(450))
	g.batch(at(320), 5, 11) // inside the judged window
	g.batch(at(420), 5, 13) // in its settle
	g.batch(at(500), 5, 17) // outside
	g.rested(11)
	tr := g.trace(at(600), "background", 30*24*time.Hour, nil)
	if tr.PaceStartNs != 100 || tr.StreamEndNs != 600 {
		t.Errorf("pacing started at %d and the stream ended at %d, want 100 and 600", tr.PaceStartNs, tr.StreamEndNs)
	}
	for _, c := range []struct {
		name             string
		got              GateBatches
		batches, records int64
	}{
		{"unpaced", tr.Unpaced, 1, 1},
		{"excluded", tr.Excluded, 3, 2 + 3 + 5},
		{"inside", tr.Inside, 1, 11},
		{"settling", tr.Settling, 1, 13},
		{"outside", tr.Outside, 2, 7 + 17},
	} {
		if c.got.Batches != c.batches || c.got.Records != c.records {
			t.Errorf("%s: %d batches and %d records, want %d and %d", c.name, c.got.Batches, c.got.Records, c.batches, c.records)
		}
	}
	if tr.RestNs != 18 || tr.PacedRestNs != 11 {
		t.Errorf("rest %d and paced rest %d, want 18 and 11", tr.RestNs, tr.PacedRestNs)
	}

	// a build that is not paced divides its batches as it always did
	plain := newGateTracker(epoch)
	gateObserver{plain}.Published(1, at(50))
	gateObserver{plain}.RewriteEnded(1, at(150))
	gateObserver{plain}.SettleEnded(1, at(200))
	plain.batch(at(60), 10, 1)
	plain.batch(at(170), 5, 1)
	plain.batch(at(300), 5, 1)
	if p := plain.trace(at(400), "background", 0, nil); p.Inside.Batches != 1 || p.Settling.Batches != 1 || p.Outside.Batches != 1 || p.Unpaced.Batches+p.Excluded.Batches != 0 {
		t.Errorf("an unpaced build: %+v %+v %+v unpaced %+v excluded %+v", p.Inside, p.Settling, p.Outside, p.Unpaced, p.Excluded)
	}
}

// Q1's control, Q2's outside and Q3's load come from the paced batches only: a long batch
// before the pacing began, or beside an unjudged window, is neither the control nor the
// outside. Here the build is over the limit with a control within the noise ceiling, which
// a control that held the long batch before the pacing would withhold.
func TestTheControlIsTakenOverThePacedBatchesOnly(t *testing.T) {
	t.Parallel()

	g := newGateTracker(epoch)
	g.setPaced()
	obs := gateObserver{g}
	obs.Published(1, at(5e9)) // an unjudged window
	g.batch(at(1e9), 900*time.Millisecond, 100)
	g.startPacing(at(10e9))
	obs.RewriteEnded(1, at(12e9))
	obs.SettleEnded(1, at(14e9))
	g.batch(at(11e9), 900*time.Millisecond, 100) // beside the unjudged window
	g.batch(at(20e9), 10*time.Millisecond, 100)  // paced, outside
	obs.Published(2, at(30e9))
	obs.RewriteEnded(2, at(40e9))
	obs.SettleEnded(2, at(41e9))
	g.batch(at(35e9), 300*time.Millisecond, 100) // inside the judged window, over the limit
	tr := g.trace(at(60e9), "background", 30*24*time.Hour, nil)
	rows, _ := judgeBuild(pacedBuildOf("Lroot/k64a2l1ns", tr), nil, "")
	if r := rows[gateQ1]; !r.judged || r.pass {
		t.Errorf("Q1: judged %v, pass %v (%s %s), want over the limit and judged with a control of 10ms", r.judged, r.pass, r.value, r.reason)
	}
	if r := rows[gateQ2]; !strings.Contains(r.value, "over 1 inside batches") {
		t.Errorf("Q2: %q", r.value)
	}
}

// sameSettings compares the retention mode, the sync setting, the pacing rate and the
// pacing start, and reads a key a manifest lacks as its default.
func TestQ5ComparesTheModeTheSyncAndThePacing(t *testing.T) {
	t.Parallel()

	base := func() *Manifest {
		return &Manifest{Describe: map[string]string{GoGCKey: "100", RetentionModeKey: "background", PaceKey: "4500", PaceFromDayKey: "46.5"}}
	}
	if why := sameSettings(base(), base()); why != "" {
		t.Fatalf("the same settings: %s", why)
	}
	for _, c := range []struct{ key, value string }{
		{RetentionModeKey, "sync"}, {SyncKey, "true"}, {PaceKey, "4000"}, {PaceFromDayKey, "11.5"},
	} {
		other := base()
		other.Describe[c.key] = c.value
		if why := sameSettings(base(), other); !strings.Contains(why, c.key+" differs") {
			t.Errorf("%s: %q", c.key, why)
		}
	}
	// a missing key is its default
	missing := base()
	delete(missing.Describe, PaceKey)
	delete(missing.Describe, PaceFromDayKey)
	other := base()
	other.Describe[PaceKey], other.Describe[PaceFromDayKey] = "0", ""
	if why := sameSettings(missing, other); why != "" {
		t.Errorf("a key left out against its default: %s", why)
	}
	if why := sameSettings(missing, base()); !strings.Contains(why, PaceKey) {
		t.Errorf("unpaced against paced: %q", why)
	}
	syncOff := base()
	syncOff.Describe[SyncKey] = "false"
	if why := sameSettings(base(), syncOff); why != "" {
		t.Errorf("a sync of false against a key left out: %s", why)
	}
}

// ---------------------------------------------------------------------------
// The pacing in the build.

// stubEngine is the part of an engine the sink of a build writes to, with a count of
// write stalls a test sets.
type stubEngine struct {
	measurable
	last   uint64
	stalls int64
}

func (e *stubEngine) Write(b []engine.Record) error { e.last = b[len(b)-1].Seq; return nil }
func (e *stubEngine) LastSeq() uint64               { return e.last }
func (e *stubEngine) WriteStalls() int64            { return e.stalls }

// day is a batch of one record at the stream day given, from epoch.
func recordAtDay(seq uint64, days float64) engine.Record {
	return engine.Record{Seq: seq, EventTime: epoch.Add(time.Duration(days * float64(24*time.Hour)))}
}

func pacedSink(g *gateTracker, p *pacer) buildSink {
	after := false
	return buildSink{e: &stubEngine{}, t: &Timing{}, afterRetention: &after, gate: g, pace: &paceState{pacer: p, fromDay: 2.5, streamAt: epoch}}
}

// The batches of a day before the one the build is paced from are written as fast as
// the engine takes them; the first batch from that day on starts the schedule, and the
// ones after it wait for it. The gate hears the start of the pacing, and counts the
// batches before it as unpaced.
func TestABuildIsPacedFromTheRulesDay(t *testing.T) {
	t.Parallel()

	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	g := newGateTracker(time.Now())
	g.setPaced()
	sink := pacedSink(g, testPacer(100, clock))
	for i, day := range []float64{0.5, 1, 2.4, 2.6, 2.7, 3} {
		if err := sink.Write([]engine.Record{recordAtDay(uint64(i+1), day)}); err != nil {
			t.Fatal(err)
		}
		switch {
		case i < 3 && (len(clock.sleeps) != 0 || g.paceStartNs != 0):
			t.Fatalf("batch %d at day %v was paced: sleeps %v, pacing started at %d", i+1, day, clock.sleeps, g.paceStartNs)
		case i == 3 && (len(clock.sleeps) != 0 || g.paceStartNs == 0):
			t.Fatalf("the first batch from day 2.5 slept %v, or the pacing did not start (%d)", clock.sleeps, g.paceStartNs)
		}
	}
	// each paced batch after the first is due 10 ms after the one before, on a clock that
	// moves only by the sleeps
	if len(clock.sleeps) != 2 || clock.sleeps[0] != 10*time.Millisecond || clock.sleeps[1] != 10*time.Millisecond {
		t.Errorf("sleeps %v, want 10ms and 10ms", clock.sleeps)
	}
	tr := g.trace(time.Now(), "background", 0, nil)
	if tr.Unpaced.Batches != 3 || tr.Outside.Batches != 3 {
		t.Errorf("%d batches unpaced and %d outside, want 3 and 3", tr.Unpaced.Batches, tr.Outside.Batches)
	}
}

// The stream day of a batch is the day of its last record, which is what the stream is
// driven by: a batch whose first record is early (a coalesced extension) and whose last is
// late switches the pacing on, and one whose first is late and whose last is early does not.
func TestTheStreamDayOfABatchIsThatOfItsLastRecord(t *testing.T) {
	t.Parallel()

	ps := &paceState{fromDay: 2.5, streamAt: epoch}
	if day := ps.streamDay([]engine.Record{recordAtDay(1, 0.1), recordAtDay(2, 3)}); day != 3 {
		t.Errorf("an early first record and a late last: day %v, want 3", day)
	}
	if day := ps.streamDay([]engine.Record{recordAtDay(1, 3), recordAtDay(2, 0.1)}); day > 0.11 {
		t.Errorf("a late first record and an early last: day %v, want 0.1", day)
	}
	g := newGateTracker(time.Now())
	g.setPaced()
	sink := pacedSink(g, testPacer(100, &fakeClock{t: time.Unix(1_700_000_000, 0)}))
	if err := sink.Write([]engine.Record{recordAtDay(1, 3), recordAtDay(2, 0.1)}); err != nil {
		t.Fatal(err)
	}
	if sink.pace.on {
		t.Error("a batch whose last record is on day 0.1 switched the pacing on")
	}
	if err := sink.Write([]engine.Record{recordAtDay(3, 0.1), recordAtDay(4, 3)}); err != nil {
		t.Fatal(err)
	}
	if !sink.pace.on {
		t.Error("a batch whose last record is on day 3 did not switch the pacing on")
	}
}

// timedEngine is an engine whose every write takes a fixed time on a fake clock.
type timedEngine struct {
	stubEngine
	clock *fakeClock
	took  time.Duration
}

func (e *timedEngine) Write(b []engine.Record) error {
	e.clock.t = e.clock.t.Add(e.took)
	return e.stubEngine.Write(b)
}

// The wait for the schedule is not in the time of the batch: with a sleep much longer
// than the engine's write before each batch, the commit the gate and the timings record
// is the engine's write and nothing more. The sink, the engine and the pacer share one
// clock that moves only by what each of them takes, so the times are exact.
func TestThePacersSleepIsNotInTheCommitTime(t *testing.T) {
	t.Parallel()

	const took = 2 * time.Millisecond
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	p := testPacer(10, clock) // a batch of one record is due every 100 ms
	// The epoch is before the first batch: a pacing that starts at the epoch itself is
	// not told apart from one that has not started.
	g := newGateTracker(clock.t.Add(-time.Second))
	g.setPaced()
	sink := pacedSink(g, p)
	sink.e = &timedEngine{clock: clock, took: took}
	sink.now = clock.now
	for i := range 4 {
		if err := sink.Write([]engine.Record{recordAtDay(uint64(i+1), 3)}); err != nil {
			t.Fatal(err)
		}
	}
	// the first batch starts the schedule; each of the others waits for what is left of
	// its 100 ms after the write before it
	if want := 3; len(clock.sleeps) != want {
		t.Fatalf("%d sleeps %v, want %d", len(clock.sleeps), clock.sleeps, want)
	}
	for i, d := range clock.sleeps {
		if d != 100*time.Millisecond-took {
			t.Errorf("sleep %d is %s, want %s", i, d, 100*time.Millisecond-took)
		}
	}
	tr := g.trace(clock.now(), "background", 0, nil)
	if tr.Outside.Batches != 4 {
		t.Fatalf("%+v", tr)
	}
	for name, h := range map[string]Histogram{"the gate's commits": tr.Outside.Commit, "the timings' writes": sink.t.Writes} {
		if h.Count != 4 || h.MaxNs != int64(took) || h.TotalNs != 4*int64(took) {
			t.Errorf("%s: %d of them, longest %dns, together %dns, want 4 of %dns each: they hold the pacer's sleep", name, h.Count, h.MaxNs, h.TotalNs, int64(took))
		}
	}
}

// The recorder of a build keeps the spread of each sample of a chunk over every chunk
// but the last of each pass that ended ("retain.chunks" follows that chunk), and keeps
// the last chunk of a pass that a later one replaced (no "retain.chunks" follows it)
// and the chunk a build ends in.
func TestTheChunksOfAPassAreSpreadWithoutItsLastChunk(t *testing.T) {
	t.Parallel()
	g := newGateTracker(time.Now())
	m := newMaxRecorder(NewCapture())
	m.gate = g
	chunk := func(prefixes int64, open, hold time.Duration) {
		m.Sample("retain.chunk_open_ns", int64(open))
		m.Sample("retain.chunk_prefixes", prefixes)
		m.Sample("retain.chunk_hold_ns", int64(hold))
	}
	// A pass of four chunks, the last short.
	chunk(100, time.Millisecond, 20*time.Millisecond)
	chunk(5, 2*time.Millisecond, 25*time.Millisecond)
	chunk(100, 3*time.Millisecond, 22*time.Millisecond)
	chunk(1, 0, time.Millisecond)
	m.Sample("retain.max_prefix_records", 3)
	m.Sample("retain.chunks", 4)
	// A pass replaced after two chunks, then a pass the build ends in.
	chunk(7, time.Millisecond, 21*time.Millisecond)
	chunk(6, time.Millisecond, 21*time.Millisecond)
	chunk(4, 4*time.Millisecond, 30*time.Millisecond)

	sp := g.trace(time.Now(), "background", 0, m.maxima()).Chunks
	want := map[string]GateSpread{
		"retain.chunk_prefixes": {Count: 6, Min: 4, Max: 100},
		"retain.chunk_open_ns":  {Count: 6, Min: int64(time.Millisecond), Max: int64(4 * time.Millisecond)},
		"retain.chunk_hold_ns":  {Count: 6, Min: int64(20 * time.Millisecond), Max: int64(30 * time.Millisecond)},
	}
	for name, w := range want {
		got, ok := sp[name]
		if !ok {
			t.Errorf("no spread of %s: %v", name, sp)
			continue
		}
		if got.Count != w.Count || got.Min != w.Min || got.Max != w.Max {
			t.Errorf("%s: %d chunks, least %d, largest %d; want %d, %d and %d (the chunks of all passes pooled)", name, got.Count, got.Min, got.Max, w.Count, w.Min, w.Max)
		}
		if got.P50 < got.Min || got.P99 < got.P50 || got.Max < got.Min {
			t.Errorf("%s: least %d, p50 %d, p99 %d", name, got.Min, got.P50, got.P99)
		}
	}
	// Prefixes 4 5 6 7 100 100: the median is 6 (exact below 8), the 99th is 100's bucket.
	if p := sp["retain.chunk_prefixes"]; p.P50 != 6 || p.P99 < 100 || p.P99 > 113 {
		t.Errorf("prefixes p50 %d, p99 %d; want 6 and 100 within an eighth", p.P50, p.P99)
	}
	if len(sp) != 3 {
		t.Errorf("spreads of %d samples: %v", len(sp), sp)
	}

	// A build with no chunk has no spread, and one whose chunks were all the last of
	// their pass has a spread of none.
	empty := newGateTracker(time.Now())
	if c := empty.trace(time.Now(), "sync", 0, nil).Chunks; c != nil {
		t.Errorf("a build without chunks has %v", c)
	}
	lasts := newGateTracker(time.Now())
	m = newMaxRecorder(NewCapture())
	m.gate = lasts
	m.Sample("retain.chunk_open_ns", 1)
	m.Sample("retain.chunk_prefixes", 3)
	m.Sample("retain.chunk_hold_ns", 2)
	m.Sample("retain.chunks", 1)
	if c := lasts.trace(time.Now(), "sync", 0, nil).Chunks; len(c) != 3 || c["retain.chunk_prefixes"].Count != 0 {
		t.Errorf("a build whose only chunk was its pass's last has %v", c)
	}
}

// The gate reads the peak sublevels of level 0 and the write stalls off the metrics
// file, the stalls summed over the openings of the database, and says when a file
// did not count the stalls.
func TestTheGateReadsTheStallsAndTheSublevels(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "metrics.jsonl")
	// Two openings of the database: 3 stalls in the first, then the count starts
	// again and reaches 2.
	lines := `{"elapsed_ms":1,"stats":{"l0_sublevels":2,"write_stalls":0,"tombstones":5}}
{"elapsed_ms":2,"stats":{"l0_sublevels":7,"write_stalls":3}}
{"elapsed_ms":3,"stats":{"l0_sublevels":1,"write_stalls":3}}
{"elapsed_ms":4,"stats":{"l0_sublevels":1,"write_stalls":0}}
{"elapsed_ms":5,"stats":{"l0_sublevels":3,"write_stalls":2}}
`
	if err := os.WriteFile(path, []byte(lines), 0o644); err != nil {
		t.Fatal(err)
	}
	m, err := readStoreGateMetrics(path)
	if err != nil {
		t.Fatal(err)
	}
	if m.L0SublevelsPeak != 7 || m.WriteStalls != 5 || !m.WriteStallsRead || m.TombstonesPeak != 5 {
		t.Errorf("read %+v", m)
	}
	old := filepath.Join(dir, "old.jsonl")
	if err := os.WriteFile(old, []byte(`{"elapsed_ms":1,"stats":{"l0_sublevels":2}}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if m, err = readStoreGateMetrics(old); err != nil || m.WriteStallsRead || m.L0SublevelsPeak != 2 {
		t.Errorf("a file without stalls read as %+v, %v", m, err)
	}
}
