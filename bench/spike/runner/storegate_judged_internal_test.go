package runner

import (
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
)

// The figures of the part of a build the gate judges, which the retention-chunk
// criteria read: the chunks of the judged passes, the stalls and the flushes from the
// start of the pacing to the end of the stream.

// A chunk is in the judged part when its pass was published at or after the start of the
// pacing, whenever the chunk came: a pass published before it is unjudged though its
// chunks run on after it, and the first chunk of a pass that came before the store told
// the tracker of its publication is tagged by its own instant when it is kept. Each pass's
// last chunk is left out, except that of a pass a later retention replaced, and the chunk
// the build ends in is kept. The count over the opening's bound is exact: an opening of
// exactly 10ms is not over it.
func TestTheChunksAreTaggedByThePartOfTheirPass(t *testing.T) {
	t.Parallel()
	g := newGateTracker(epoch)
	g.setPaced()
	var now time.Time
	g.now = func() time.Time { return now }
	m := newMaxRecorder(NewCapture())
	m.gate = g
	obs := gateObserver{g}
	chunk := func(ns, prefixes int64, open, hold time.Duration) {
		now = at(ns)
		m.Sample("retain.chunk_open_ns", int64(open))
		m.Sample("retain.chunk_prefixes", prefixes)
		m.Sample("retain.chunk_hold_ns", int64(hold))
	}
	ms := time.Millisecond

	obs.Published(1, at(10)) // before the pacing
	chunk(20, 50, 20*ms, 40*ms)
	g.startPacing(at(100))
	chunk(120, 60, 20*ms, 40*ms) // after the pacing started, of a pass published before it
	chunk(130, 1, 0, ms)         // its last
	m.Sample("retain.chunks", 3)

	chunk(210, 120, 11*ms, 25*ms) // sampled before the store told of its pass
	obs.Published(2, at(200))
	chunk(220, 130, 10*ms, 31*ms)
	chunk(230, 90, 12*ms, 26*ms)
	chunk(240, 2, ms, 2*ms) // its last
	m.Sample("retain.chunks", 4)

	obs.Published(3, at(300))
	chunk(310, 200, 2*ms, 22*ms) // the last of a pass that the next replaced
	obs.Published(4, at(400))
	chunk(410, 150, 3*ms, 23*ms) // the build ends in this pass

	tr := g.trace(at(500), "background", 30*24*time.Hour, m.maxima())
	for _, c := range []struct {
		name   string
		spread map[string]GateSpread
		count  int64
	}{{"all", tr.Chunks, 7}, {"judged", tr.JudgedChunks, 5}, {"unjudged", tr.UnjudgedChunks, 2}} {
		for _, s := range gateChunkSamples {
			if got := c.spread[s].Count; got != c.count {
				t.Errorf("%s: %s over %d chunks, want %d", c.name, s, got, c.count)
			}
		}
	}
	open, hold, prefixes := tr.JudgedChunks["retain.chunk_open_ns"], tr.JudgedChunks["retain.chunk_hold_ns"], tr.JudgedChunks["retain.chunk_prefixes"]
	if open.Over != 2 || open.Min != int64(2*ms) || open.Max != int64(12*ms) || open.Sum != int64(38*ms) {
		t.Errorf("judged openings %+v: want 2 over 10ms (11ms and 12ms, not 10ms), least 2ms, largest 12ms, sum 38ms", open)
	}
	if u := tr.UnjudgedChunks["retain.chunk_open_ns"]; u.Over != 2 || u.Min != int64(20*ms) {
		t.Errorf("unjudged openings %+v: want both 20ms chunks of the pass before the pacing", u)
	}
	// Prefixes 90 120 130 150 200: the median 130 is in the bucket from 128 to 144.
	if prefixes.P50Low != 128 || prefixes.P50 != 144 || prefixes.Sum != 690 || prefixes.mean() != 138 {
		t.Errorf("judged prefixes %+v: want the median's bucket from 128 to 144, sum 690, mean 138", prefixes)
	}
	// The 99th percentile of the holds is 31ms's bucket, read at its top.
	if hold.P99 <= int64(31*ms) || hold.P99 > int64(31*ms)+int64(31*ms)/8 || hold.P90 < int64(26*ms) {
		t.Errorf("judged holds %+v: want p99 at the top of 31ms's bucket", hold)
	}

	// A build that is not paced is judged on all of it.
	plain := newGateTracker(epoch)
	plain.now = func() time.Time { return now }
	pm := newMaxRecorder(NewCapture())
	pm.gate = plain
	now = at(5)
	pm.Sample("retain.chunk_open_ns", 1)
	pm.Sample("retain.chunk_prefixes", 3)
	pm.Sample("retain.chunk_hold_ns", 2)
	if p := plain.trace(at(10), "sync", 0, nil); p.JudgedChunks["retain.chunk_prefixes"].Count != 1 || p.UnjudgedChunks["retain.chunk_prefixes"].Count != 0 {
		t.Errorf("a build that is not paced: judged %v, unjudged %v", p.JudgedChunks, p.UnjudgedChunks)
	}
}

// The judged part's write stalls are counted from the database's count at the start of
// the pacing, not from its opening, to the end of the stream; its flushes are those
// counted from the start of the pacing to the end of the stream, each of the three
// present, 0 included.
func TestTheJudgedPartCountsItsStallsAndFlushesFromThePacing(t *testing.T) {
	t.Parallel()
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	g := newGateTracker(time.Now())
	g.setPaced()
	m := newMaxRecorder(NewCapture())
	m.gate = g
	sink := pacedSink(g, testPacer(100, clock))
	stub := sink.e.(*stubEngine)

	stub.stalls = 3
	m.Count("retain.flushes", 5) // before the pacing
	m.Count("retain.flush_deferred", 1)
	if err := sink.Write([]engine.Record{recordAtDay(1, 1)}); err != nil {
		t.Fatal(err)
	}
	stub.stalls = 4
	if err := sink.Write([]engine.Record{recordAtDay(2, 3)}); err != nil { // the pacing starts
		t.Fatal(err)
	}
	stub.stalls = 6
	m.Count("retain.flushes", 2)
	if err := sink.Write([]engine.Record{recordAtDay(3, 3)}); err != nil {
		t.Fatal(err)
	}
	g.endStream(6, true)
	m.Count("retain.flush_errors", 1) // after the stream
	tr := g.trace(time.Now(), "background", 0, nil)
	if !tr.StallsRead || tr.StallsAtPaceStart != 4 || tr.StallsAtStreamEnd != 6 {
		t.Errorf("stalls read %v, %d at the start of the pacing and %d at the end of the stream, want 4 and 6", tr.StallsRead, tr.StallsAtPaceStart, tr.StallsAtStreamEnd)
	}
	want := map[string]int64{"retain.flushes": 2, "retain.flush_deferred": 0, "retain.flush_errors": 0}
	if len(tr.JudgedCounts) != 3 || tr.JudgedCounts["retain.flushes"] != 2 || tr.JudgedCounts["retain.flush_deferred"] != 0 || tr.JudgedCounts["retain.flush_errors"] != 0 {
		t.Errorf("judged counts %v, want %v", tr.JudgedCounts, want)
	}

	// A build that is not paced counts from the start of its stream.
	plain := newGateTracker(epoch)
	pm := newMaxRecorder(NewCapture())
	pm.gate = plain
	pm.Count("retain.flushes", 1) // before the stream
	plain.startJudged()
	pm.Count("retain.flush_deferred", 2)
	plain.endStream(7, true)
	if p := plain.trace(at(10), "background", 0, nil); p.JudgedCounts["retain.flush_deferred"] != 2 || p.JudgedCounts["retain.flushes"] != 0 || p.StallsAtPaceStart != 0 || p.StallsAtStreamEnd != 7 {
		t.Errorf("a build that is not paced: counts %v, stalls %d to %d", p.JudgedCounts, p.StallsAtPaceStart, p.StallsAtStreamEnd)
	}
}

// meets is a trace of a build in the background whose judged part meets every criterion:
// 100 judged chunks, 10 of them over the opening's bound (exactly the 10% allowed), a
// 99th percentile hold at 30ms, a median of 100 prefixes at the bottom of its bucket, no
// stall and no flush put off or refused.
func meets() *GateTrace {
	return &GateTrace{
		Mode: "background",
		JudgedChunks: map[string]GateSpread{
			"retain.chunk_open_ns":  {Count: 100, Min: 1, P50: int64(2 * time.Millisecond), P90: int64(9 * time.Millisecond), P99: int64(15 * time.Millisecond), Max: int64(16 * time.Millisecond), Over: 10},
			"retain.chunk_hold_ns":  {Count: 100, Min: 1, P50: int64(20 * time.Millisecond), P99: int64(30 * time.Millisecond), Max: int64(30 * time.Millisecond)},
			"retain.chunk_prefixes": {Count: 100, Min: 1, P50: 104, P50Low: 100, P99: 400, Max: 410},
		},
		JudgedCounts:      map[string]int64{"retain.flushes": 9, "retain.flush_deferred": 0, "retain.flush_errors": 0},
		StallsAtPaceStart: 3, StallsAtStreamEnd: 3, StallsRead: true,
	}
}

// Each criterion is met at its bound and not past it, and the build meets the criteria
// only if it meets all of them. C1 is the exact count over the bound, not a quantile: 11
// of 100 chunks over 10ms is not met even with a 90th percentile below it. A build with no
// judged chunk meets none of C1 to C3, and one that did not record its stalls or flushes
// meets neither C4 nor C5.
func TestTheRetentionChunkCriteria(t *testing.T) {
	t.Parallel()
	var none [gateRows]gateRow
	if ok, parts := retainChunkCriteria(meets(), none); !ok || len(parts) != 6 {
		t.Fatalf("a build that meets them: %v %v", ok, parts)
	}
	for _, c := range []struct {
		name, criterion string
		mod             func(*GateTrace)
	}{
		{"11 of 100 over the bound, p90 below it", "C1 not met", func(g *GateTrace) {
			o := g.JudgedChunks["retain.chunk_open_ns"]
			o.Over = 11
			g.JudgedChunks["retain.chunk_open_ns"] = o
		}},
		{"a p99 hold above 30ms", "C2 not met", func(g *GateTrace) {
			h := g.JudgedChunks["retain.chunk_hold_ns"]
			h.P99 = int64(30*time.Millisecond) + 1
			g.JudgedChunks["retain.chunk_hold_ns"] = h
		}},
		{"a median of 99 prefixes at the bottom of its bucket", "C3 not met", func(g *GateTrace) {
			p := g.JudgedChunks["retain.chunk_prefixes"]
			p.P50Low = 99
			g.JudgedChunks["retain.chunk_prefixes"] = p
		}},
		{"a stall in the judged part", "C4 not met", func(g *GateTrace) { g.StallsAtStreamEnd = 4 }},
		{"stalls not recorded", "C4 not met", func(g *GateTrace) { g.StallsRead = false }},
		{"a flush put off", "C5 not met", func(g *GateTrace) { g.JudgedCounts["retain.flush_deferred"] = 1 }},
		{"a flush refused", "C5 not met", func(g *GateTrace) { g.JudgedCounts["retain.flush_errors"] = 1 }},
		{"flushes not recorded", "C5 not met", func(g *GateTrace) { g.JudgedCounts = nil }},
		{"no judged chunk", "C1 not met", func(g *GateTrace) { g.JudgedChunks = map[string]GateSpread{} }},
	} {
		tr := meets()
		c.mod(tr)
		ok, parts := retainChunkCriteria(tr, none)
		if ok || !strings.Contains(strings.Join(parts, "; "), c.criterion) {
			t.Errorf("%s: met %v, %v; want %q", c.name, ok, parts, c.criterion)
		}
	}
	noChunk := meets()
	noChunk.JudgedChunks = nil
	if _, parts := retainChunkCriteria(noChunk, none); !strings.Contains(parts[1], "C2 not met") || !strings.Contains(parts[2], "C3 not met") {
		t.Errorf("no judged chunk: %v", parts)
	}
	// Q1, Q2 and Q4 each pass or are not judged; one over its limit fails the criteria.
	var rows [gateRows]gateRow
	rows[gateQ1] = gateRow{judged: true, pass: true}
	rows[gateQ2] = gateRow{judged: true, pass: false}
	if ok, parts := retainChunkCriteria(meets(), rows); ok || !strings.Contains(parts[5], "over: Q2") {
		t.Errorf("Q2 over its limit: %v %v", ok, parts)
	}
}

// The builds through the root store are set side by side only at one retention flush
// threshold: a build that records another, or none, is named; a build that is not
// through the root store has no threshold and is left out.
func TestAFamilyIsBuiltAtOneFlushThreshold(t *testing.T) {
	t.Parallel()
	root := func(v string) *Manifest {
		m := &Manifest{Describe: map[string]string{"store": "root"}}
		if v != "" {
			m.Describe[RetainFlushDelsKey] = v
		}
		return m
	}
	spike := &Manifest{Describe: map[string]string{}}
	if why := flushDelsMix([]string{"L/off", "Lroot/off", "Lroot/k64a2l1ns"}, []*Manifest{spike, root("32768"), root("32768")}); why != "" {
		t.Errorf("one threshold: %s", why)
	}
	if why := flushDelsMix([]string{"Lroot/off", "Lroot/k64a2l1ns"}, []*Manifest{root("32768"), root("24576")}); !strings.Contains(why, `retain_flush_dels="24576": Lroot/k64a2l1ns`) || !strings.Contains(why, `retain_flush_dels="32768": Lroot/off`) {
		t.Errorf("two thresholds: %q", why)
	}
	if why := flushDelsMix([]string{"Lroot/off", "Lroot/k64a2l1ns"}, []*Manifest{root(""), root("32768")}); !strings.Contains(why, `"not recorded"`) {
		t.Errorf("one not recorded and the default: %q", why)
	}
}

// The timing judge takes a build at its own default threshold, and one that recorded
// none, and refuses one recorded at another threshold.
func TestTheTimingJudgeRefusesAnotherFlushThreshold(t *testing.T) {
	t.Parallel()
	j := &judge{r: DefaultRules()}
	build := func(v string) *tbuild {
		m := &Manifest{Build: BuildInfo{Revision: "r"}, Describe: map[string]string{}}
		if v != "" {
			m.Describe[RetainFlushDelsKey] = v
		}
		return &tbuild{TimedBuild: TimedBuild{Job: Job{Candidate: "Lroot/k64a2l1ns", Window: 7, Arch: "x86_64"}, Manifest: m}}
	}
	if _, why := j.buildsWhy([]*tbuild{build("24576")}); !strings.Contains(why, "retention flush threshold of 24576") {
		t.Errorf("another threshold: %q", why)
	}
	for _, v := range []string{"32768", ""} {
		if _, why := j.buildsWhy([]*tbuild{build(v)}); strings.Contains(why, "threshold") {
			t.Errorf("%q: %q", v, why)
		}
	}
}
