package runner

import (
	"bytes"
	"strings"
	"testing"
)

func TestReportHelpers(t *testing.T) {
	t.Parallel()

	for n, want := range map[int64]string{0: "0 B", 1023: "1023 B", 1024: "1.0 KiB", 1536: "1.5 KiB", 1<<20 - 1: "1024.0 KiB", 1 << 20: "1.0 MiB", 3 << 29: "1.50 GiB", 1 << 30: "1.00 GiB"} {
		if got := bytesStr(n); got != want {
			t.Errorf("bytesStr(%d) = %q, want %q", n, got, want)
		}
	}
	if got := sumPrefix(map[string]int64{"tables_l0": 2, "tables_l6": 3, "bytes_l0": 100, "x": 7}, "tables_l"); got != 5 {
		t.Errorf("sumPrefix = %d, want 5", got)
	}
	bd := map[string]int64{"L2/observe": 10, "L3/observe": 5, "L2/delete": 7, "L2/payload forward": 3}
	if got := sumKind(bd, "observe"); got != 15 {
		t.Errorf("sumKind(observe) = %d, want 15", got)
	}
	if got := sumKind(bd, "payload forward"); got != 3 {
		t.Errorf("sumKind(payload forward) = %d, want 3", got)
	}
	cs := []*Candidate{{Manifest: &Manifest{Breakdown: bd}}, {Manifest: &Manifest{Breakdown: map[string]int64{"L1/baseline": 1}}}}
	if got := strings.Join(partKinds(cs), ","); got != "baseline,delete,observe,payload forward" {
		t.Errorf("partKinds = %q", got)
	}

	// The options of two builds are compared leaving out only what a candidate is.
	a := "[Options]\n  comparer=x\n  key_schema=y\n  block_size=1\n  table_property_collectors=z\n"
	b := "[Options]\n  comparer=other\n  key_schema=other\n  block_size=1\n"
	if d := optionsDiff(a, b); d != "" {
		t.Errorf("options that differ in the comparer, the key schema and the collectors: %q", d)
	}
	if d := optionsDiff(a, strings.Replace(b, "block_size=1", "block_size=2", 1)); !strings.Contains(d, "block_size=1") || !strings.Contains(d, "block_size=2") {
		t.Errorf("options that differ in the block size: %q", d)
	}
	if d := optionsDiff(a, b+"  extra=1\n"); d == "" {
		t.Error("options with an extra line are the same")
	}
	if d := optionsDiff(a+"  extra=1\n", b); d == "" {
		t.Error("options with a line fewer are the same")
	}
}

// The tables of reads show the mean per query of each row and, in brackets, how
// many times the smallest it is.
func TestReportMetricCells(t *testing.T) {
	t.Parallel()

	q := func(group, age string, op Op) Query { return Query{Group: group, Age: age, Op: op, Rank: 0} }
	plan := &Plan{Queries: []Query{
		q("g", AgeNow, OpNeighbors), q("g", AgeNow, OpNeighbors), q("g", Age1h, OpNeighbors), q("h", AgeWindow1d, OpWindow), q("z", AgeNow, OpAlive), q("h", AgeWindow1h, OpWindow),
	}}
	res := func(steps ...int64) *Results {
		r := &Results{Queries: make([]QueryResult, len(plan.Queries))}
		for i := range plan.Queries {
			r.Queries[i].Counters = map[string]int64{"read." + string(plan.Queries[i].Op) + ".steps": steps[i]}
		}
		return r
	}
	cs := []*Candidate{
		{Manifest: &Manifest{}, Results: func() *Results { r := res(10, 30, 7, 4, 0, 9); r.Candidate = "A"; return r }()},
		{Manifest: &Manifest{}, Results: func() *Results { r := res(20, 20, 70, 4, 0, 9); r.Candidate = "B"; return r }()},
	}
	var short, full bytes.Buffer
	writeMetric(&short, plan, cs, metricSpec{counter: "steps", perOp: true}, false)
	writeMetric(&full, plan, cs, metricSpec{counter: "steps", perOp: true}, true)

	row := func(text, label string) string {
		for _, l := range strings.Split(text, "\n") {
			if strings.HasPrefix(strings.TrimSpace(l), label) {
				return strings.Join(strings.Fields(l), " ")
			}
		}
		return ""
	}
	// Means are 20 and 20 for g at now; 7 and 70 at 1h; 4 and 4 for the window; zero for alive.
	if got, want := row(short.String(), "g now"), "g now 20 (x1.0) 20 (x1.0)"; got != want {
		t.Errorf("row g now = %q, want %q", got, want)
	}
	if got, want := row(full.String(), "g 1h"), "g 1h 7 (x1.0) 70 (x10.0)"; got != want {
		t.Errorf("row g 1h = %q, want %q", got, want)
	}
	if got := row(short.String(), "g 1h"); got != "g 1h 7 (x1.0) 70 (x10.0)" {
		t.Errorf("the short report's row for an hour back: %q", got)
	}
	if got, want := row(short.String(), "h window-1d"), "h window-1d 4 (x1.0) 4 (x1.0)"; got != want {
		t.Errorf("row h window-1d = %q, want %q", got, want)
	}
	if got, want := row(short.String(), "z now"), "z now 0 0"; got != want {
		t.Errorf("row z now = %q, want %q", got, want)
	}
	// An hour's window is in the full report and not in the short one.
	if got := row(short.String(), "h window-1h"); got != "" {
		t.Errorf("the short report has a row for an hour's window: %q", got)
	}
	if got, want := row(full.String(), "h window-1h"), "h window-1h 9 (x1.0) 9 (x1.0)"; got != want {
		t.Errorf("the full report's row for an hour's window = %q, want %q", got, want)
	}
	if got := row(short.String(), "A"); !strings.Contains(got, "B") {
		t.Errorf("the header does not name the candidates: %q", got)
	}
}

// What each candidate holds is shown as the numbers of its manifest say.
func TestReportRowsOfWhatIsHeld(t *testing.T) {
	t.Parallel()

	plan := &Plan{}
	c := &Candidate{
		Results: &Results{Candidate: "X"},
		Manifest: &Manifest{
			Stream:         StreamInfo{Records: 10},
			StatsCompacted: map[string]int64{"live_table_bytes": 1000, "tables_l5": 1, "tables_l6": 2, "tombstones": 9},
			StatsBuilt:     map[string]int64{"bytes_in": 100, "bytes_flushed": 300, "bytes_compacted": 200, "flushes": 4, "compactions": 3},
			SizeByLayer:    map[string]int64{"L2": 2048},
			Breakdown:      map[string]int64{"L2/observe": 5, "L3/observe": 6},
			Counters:       map[string]int64{"retain.seeks": 77},
		},
	}
	var out bytes.Buffer
	Write(&out, plan, []*Candidate{c}, false)
	row := func(label string) string {
		for _, l := range strings.Split(out.String(), "\n") {
			if f := strings.Fields(l); len(f) > 0 && strings.HasPrefix(strings.Join(f, " "), label) {
				return strings.Join(f, " ")
			}
		}
		return ""
	}
	for label, want := range map[string]string{
		"table bytes per record written (those retained away too)": "table bytes per record written (those retained away too) 100",
		"tables":            "tables 3",
		"tombstones left":   "tombstones left 9",
		"L2 table bytes":    "L2 table bytes 2.0 KiB",
		"observe (logical)": "observe (logical) 11 B",
		"write amplification built (depends on when compactions ran)":               "write amplification built (depends on when compactions ran) 5.00",
		"flushes, compactions while building (also depend on when compactions ran)": "flushes, compactions while building (also depend on when compactions ran) 4, 3",
		"retain.seeks": "retain.seeks 77",
	} {
		if got := row(label); got != want {
			t.Errorf("row %q = %q, want %q", label, got, want)
		}
	}
}

// Only what has been seen to drift is allowed to.
func TestOnlyKnownCountersAreAllowedToDrift(t *testing.T) {
	t.Parallel()

	for name, want := range map[string]bool{
		"read.neighbors.block_bytes_cached": true, "read.batch.block_bytes_cached": true, "read.window.block_read_ns": true,
		"read.batch.separated_values":     true,
		"read.neighbors.separated_values": false, "read.window.separated_values": false, "read.alive.separated_values": false,
		"read.batch.steps": false, "read.neighbors.block_bytes": false, "read.neighbors.points": false, "read.checkpoint_hits": false,
	} {
		if got := volatile(name); got != want {
			t.Errorf("volatile(%q) = %v, want %v", name, got, want)
		}
	}
}
