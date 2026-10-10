package runner

import (
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
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
