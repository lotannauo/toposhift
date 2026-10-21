package vmmodel_test

import (
	"errors"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/victoria/internal/fixture"
	"github.com/lotannauo/toposhift/bench/victoria/vmmodel"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// tick is an instant, in nanoseconds, small enough that a brute force can walk them.
func tick(n int64) time.Time { return time.Unix(0, n).UTC() }

func iv(start, end int64) lifecycle.Interval {
	if end < 0 {
		return lifecycle.Interval{Start: tick(start)}
	}
	return lifecycle.Interval{Start: tick(start), End: tick(end)}
}

// bruteBoundary says whether existence changes at some instant x in (t-band, t+band],
// that is, whether it differs between x-1 and x, over instants that are whole ticks.
func bruteBoundary(ivs []lifecycle.Interval, t, band int64) bool {
	alive := func(x int64) bool {
		for _, i := range ivs {
			if tick(x).Before(i.Start) {
				continue
			}
			if i.Open() || tick(x).Before(i.End) {
				return true
			}
		}
		return false
	}
	for x := t - band + 1; x <= t+band; x++ {
		if alive(x) != alive(x-1) {
			return true
		}
	}
	return false
}

// The band is open on the left and closed on the right, a boundary of existence is where
// it changes (the join of two touching intervals is not one), and an open interval has
// no end.
func TestInBandEdges(t *testing.T) {
	const T, B = 100, 10
	cases := []struct {
		name string
		ivs  []lifecycle.Interval
		want bool
	}{
		{"starts at t+B: inside, the right end is included", []lifecycle.Interval{iv(110, -1)}, true},
		{"starts at t+B+1: outside", []lifecycle.Interval{iv(111, -1)}, false},
		{"starts at t-B: outside, the left end is excluded", []lifecycle.Interval{iv(90, -1)}, false},
		{"starts at t-B+1: inside", []lifecycle.Interval{iv(91, -1)}, true},
		{"ends at t+B: inside", []lifecycle.Interval{iv(0, 110)}, true},
		{"ends at t+B+1: outside", []lifecycle.Interval{iv(0, 111)}, false},
		{"ends at t-B: outside", []lifecycle.Interval{iv(0, 90)}, false},
		{"ends at t-B+1: inside", []lifecycle.Interval{iv(0, 91)}, true},
		{"spans the whole band", []lifecycle.Interval{iv(0, 200)}, false},
		{"open and from long ago", []lifecycle.Interval{iv(0, -1)}, false},
		{"entirely inside the band", []lifecycle.Interval{iv(95, 105)}, true},
		{"no existence", nil, false},
		// Two intervals that touch at x are one run: nothing changes at x.
		{"touching inside the band", []lifecycle.Interval{iv(0, 100), iv(100, 200)}, false},
		{"touching, given out of order", []lifecycle.Interval{iv(100, 200), iv(0, 100)}, false},
		{"overlapping inside the band", []lifecycle.Interval{iv(0, 105), iv(95, 200)}, false},
		{"a gap of one tick inside the band", []lifecycle.Interval{iv(0, 100), iv(101, 200)}, true},
		{"touching, then the run ends in the band", []lifecycle.Interval{iv(0, 50), iv(50, 105)}, true},
		{"an open interval swallows what follows", []lifecycle.Interval{iv(0, -1), iv(100, 105)}, false},
		{"an open interval touching a closed one", []lifecycle.Interval{iv(0, 100), iv(100, -1)}, false},
	}
	for _, c := range cases {
		if got := vmmodel.InBand(c.ivs, tick(T), time.Duration(B)); got != c.want {
			t.Errorf("%s: InBand = %v, want %v", c.name, got, c.want)
		}
		if got := bruteBoundary(c.ivs, T, B); got != c.want {
			t.Errorf("%s: the brute force says %v, the case says %v", c.name, got, c.want)
		}
	}
	// InBand does not reorder or change its argument.
	in := []lifecycle.Interval{iv(100, 200), iv(0, 100)}
	vmmodel.InBand(in, tick(T), time.Duration(B))
	if !in[0].Start.Equal(tick(100)) {
		t.Error("InBand sorted its argument")
	}
}

// Against the brute force, for random intervals that overlap and touch, and any instant
// and width.
func TestInBandAgainstBruteForce(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		n := rapid.IntRange(0, 5).Draw(rt, "intervals")
		var ivs []lifecycle.Interval
		prevEnd := int64(0)
		for i := 0; i < n; i++ {
			start := rapid.Int64Range(0, 60).Draw(rt, "start")
			if rapid.IntRange(0, 2).Draw(rt, "touch") == 0 {
				start = prevEnd // touching the one before
			}
			if rapid.IntRange(0, 6).Draw(rt, "open") == 0 {
				ivs = append(ivs, iv(start, -1))
				continue
			}
			end := start + rapid.Int64Range(1, 30).Draw(rt, "length")
			ivs = append(ivs, iv(start, end))
			prevEnd = end
		}
		T := rapid.Int64Range(0, 90).Draw(rt, "t")
		B := rapid.Int64Range(1, 25).Draw(rt, "band")
		if got, want := vmmodel.InBand(ivs, tick(T), time.Duration(B)), bruteBoundary(ivs, T, B); got != want {
			rt.Fatalf("InBand(%v, t=%d, band=%d) = %v, brute force %v", ivs, T, B, got, want)
		}
	})
}

// ---- classification ----

func classWorld(t *testing.T) (*vmmodel.World, *vmmodel.Registry, identity.Fingerprint, identity.Fingerprint) {
	t.Helper()
	reg := registry(t)
	node := fixture.FP(catalog.K8sNode, 1)
	host := fixture.FP(catalog.Host, 2)
	podA, podB, podC := fixture.FP(catalog.K8sPod, 11), fixture.FP(catalog.K8sPod, 12), fixture.FP(catalog.K8sPod, 13)
	hb := func(seq uint64, s engine.Subject, p lifecycle.Producer, ttl time.Duration, at time.Duration) engine.Record {
		layer := catalog.L2
		if s.Relation == catalog.RunsOn {
			layer = catalog.L1
		}
		return engine.Record{Layer: layer, Subject: s, Producer: p, EventTime: t0.Add(at), Seq: seq, Kind: lifecycle.Observe, TTL: ttl}
	}
	h := 3600 * time.Second
	// pod A is placed long ago and for a long time (refreshed every 15 min for 10 hours),
	// pod B is placed at hour 8, and pod C was refreshed once, at hour 5.
	var recs []engine.Record
	seq := uint64(0)
	add := func(r engine.Record) { seq++; r.Seq = seq; recs = append(recs, r) }
	for at := time.Duration(0); at <= 10*h; at += 15 * time.Minute {
		add(hb(0, engine.EdgeSubject(podA, node, catalog.ScheduledOn), "k8s-cluster", h, at))
	}
	for at := 8 * h; at <= 10*h; at += 15 * time.Minute {
		add(hb(0, engine.EdgeSubject(podB, node, catalog.ScheduledOn), "k8s-cluster", h, at))
	}
	add(hb(0, engine.EdgeSubject(podC, node, catalog.ScheduledOn), "k8s-cluster", h, 5*h))
	for at := time.Duration(0); at <= 10*h; at += time.Minute {
		add(hb(0, engine.EntitySubject(node), "node-collector", 240*time.Second, at))
		add(hb(0, engine.EdgeSubject(node, host, catalog.RunsOn), "node-collector", 240*time.Second, at))
	}
	// the records are in event-time order here, which is also arrival order
	w := vmmodel.NewWorld()
	if err := w.Write(sortBySeqKeepingTime(recs)); err != nil {
		t.Fatal(err)
	}
	return w, reg, node, host
}

// sortBySeqKeepingTime renumbers the records in event-time order so Seq ascends.
func sortBySeqKeepingTime(recs []engine.Record) []engine.Record {
	out := make([]engine.Record, len(recs))
	copy(out, recs)
	engine.SortRecords(out)
	for i := range out {
		out[i].Seq = uint64(i + 1)
	}
	return out
}

func query(op runner.Op, layer catalog.Layer, dir engine.Direction, at time.Duration, fps ...identity.Fingerprint) runner.Query {
	return runner.Query{Group: "g", Op: op, Layer: layer, Dir: dir, Fps: fps, Age: "x", At: t0.Add(at), AsOf: engine.Latest}
}

func TestClassifyMarksInstantsNearAChange(t *testing.T) {
	w, reg, node, _ := classWorld(t)
	h := 3600 * time.Second
	cases := []struct {
		name string
		q    runner.Query
		want bool
		band time.Duration
	}{
		// Pod A's edge (3,600 s window) exists from 0 to 11 h. Pod B's starts at 8 h and
		// pod C's is the single refresh at 5 h, existing from 5 h to 6 h.
		{"reverse read of the node at 2 h: only A, far from its ends", query(runner.OpNeighbors, catalog.L2, engine.Reverse, 2*h, node), false, h + time.Millisecond},
		{"at 5 h: pod C's edge starts there", query(runner.OpNeighbors, catalog.L2, engine.Reverse, 5*h, node), true, h + time.Millisecond},
		{"at 5 h 30: C is alive and ends in half an hour, inside the band", query(runner.OpNeighbors, catalog.L2, engine.Reverse, 5*h+30*time.Minute, node), true, h + time.Millisecond},
		{"at 7 h: C ended an hour ago, B starts in an hour: neither is in the answer, A is far", query(runner.OpNeighbors, catalog.L2, engine.Reverse, 7*h, node), false, h + time.Millisecond},
		{"at 8 h 30: B started half an hour ago", query(runner.OpNeighbors, catalog.L2, engine.Reverse, 8*h+30*time.Minute, node), true, h + time.Millisecond},
		{"at 10 h 30: A ends in half an hour", query(runner.OpNeighbors, catalog.L2, engine.Reverse, 10*h+30*time.Minute, node), true, h + time.Millisecond},
		{"a batch is the union of its entities", query(runner.OpBatch, catalog.L2, engine.Reverse, 2*h, node, node), false, h + time.Millisecond},
		// The node exists from 0 to 10 h + 4 min; its band is 240 s.
		{"alive at 5 h: far from its ends", query(runner.OpAlive, catalog.L2, 0, 5*h, node), false, 240*time.Second + time.Millisecond},
		{"alive at 10 h: it ends in four minutes, at the edge of the band", query(runner.OpAlive, catalog.L2, 0, 10*h, node), true, 240*time.Second + time.Millisecond},
		{"alive an hour after the end: long ended", query(runner.OpAlive, catalog.L2, 0, 11*h+10*time.Minute, node), false, 240*time.Second + time.Millisecond},
		{"alive just after its start", query(runner.OpAlive, catalog.L2, 0, 3*time.Second, node), true, 240*time.Second + time.Millisecond},
	}
	for _, c := range cases {
		got, err := w.Classify(reg, c.q, nil)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if got.Boundary != c.want || got.Band != c.band {
			t.Errorf("%s: class %+v, want boundary %v band %s", c.name, got, c.want, c.band)
		}
	}
}

// A subject the store returned that the reference does not have is involved too: an edge
// that ended an hour ago is not in the answer, but if the store returned it, the instant
// is a boundary, and the band is that of the longest window among all the subjects.
func TestClassifyUsesTheSubjectsTheStoreReturned(t *testing.T) {
	w, reg, node, _ := classWorld(t)
	h := 3600 * time.Second
	q := query(runner.OpNeighbors, catalog.L2, engine.Reverse, 7*h, node)
	pre, err := w.Classify(reg, q, nil)
	if err != nil || pre.Boundary {
		t.Fatalf("from the reference alone: %+v, %v", pre, err)
	}
	podC := fixture.FP(catalog.K8sPod, 13)
	returned := vmmodel.NeighborSubjects(node, engine.Reverse, []engine.Neighbor{{Peer: podC, Relation: catalog.ScheduledOn}})
	// C's interval ended at 6 h, which is exactly t-B on the left: outside. Move the read
	// to just inside the band.
	q2 := query(runner.OpNeighbors, catalog.L2, engine.Reverse, 6*h+59*time.Minute, node)
	got, err := w.Classify(reg, q2, returned)
	if err != nil || !got.Boundary {
		t.Errorf("with the returned edge: %+v, %v", got, err)
	}
	if got.Subjects < 2 {
		t.Errorf("involved %d subjects", got.Subjects)
	}
	without, err := w.Classify(reg, q2, nil)
	if err != nil || without.Boundary {
		t.Errorf("without it: %+v, %v", without, err)
	}
	// A returned subject that is nowhere near a change leaves the query as it was.
	far := vmmodel.NeighborSubjects(node, engine.Reverse, []engine.Neighbor{{Peer: fixture.FP(catalog.K8sPod, 11), Relation: catalog.ScheduledOn}})
	got, err = w.Classify(reg, query(runner.OpNeighbors, catalog.L2, engine.Reverse, 2*h, node), far)
	if err != nil || got.Boundary || got.Subjects != 1 {
		t.Errorf("a far subject: %+v, %v", got, err)
	}
}

func TestClassifyRefusesWhatIsNotAnInstantReadOfTheLatest(t *testing.T) {
	w, reg, node, _ := classWorld(t)
	old := query(runner.OpNeighbors, catalog.L2, engine.Reverse, time.Hour, node)
	old.AsOf = 5
	win := query(runner.OpWindow, catalog.L2, engine.Reverse, time.Hour, node)
	for name, q := range map[string]runner.Query{"an old token": old, "a window": win} {
		if _, err := w.Classify(reg, q, nil); !errors.Is(err, vmmodel.ErrUnclassifiable) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
}

func TestReferenceSubjectsOfAnExistenceReadIsTheEntityWhetherOrNotItIsAlive(t *testing.T) {
	w, _, node, _ := classWorld(t)
	got, err := w.ReferenceSubjects(query(runner.OpAlive, catalog.L2, 0, 20*3600*time.Second, node))
	if err != nil || len(got) != 1 || got[0] != engine.EntitySubject(node) {
		t.Errorf("subjects %v, %v", got, err)
	}
}
