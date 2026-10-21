package vmmodel_test

import (
	"errors"
	"slices"
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

// worldOf folds records into a world, numbering them in the order given.
func worldOf(t fataler, recs ...engine.Record) *vmmodel.World {
	t.Helper()
	w := vmmodel.NewWorld()
	out := slices.Clone(recs)
	for i := range out {
		out[i].Seq = uint64(i + 1)
	}
	if err := w.Write(out); err != nil {
		t.Fatal(err)
	}
	return w
}

func edgeRec(pod, node identity.Fingerprint, p lifecycle.Producer, kind lifecycle.Kind, ttl, at time.Duration) engine.Record {
	return engine.Record{Layer: catalog.L2, Subject: engine.EdgeSubject(pod, node, catalog.ScheduledOn), Producer: p, EventTime: t0.Add(at), Kind: kind, TTL: ttl}
}

func nodeRec(node identity.Fingerprint, kind lifecycle.Kind, ttl, at time.Duration) engine.Record {
	return engine.Record{Layer: catalog.L2, Subject: engine.EntitySubject(node), Producer: "node-collector", EventTime: t0.Add(at), Kind: kind, TTL: ttl}
}

func query(op runner.Op, layer catalog.Layer, dir engine.Direction, at time.Duration, fps ...identity.Fingerprint) runner.Query {
	return runner.Query{Group: "g", Op: op, Layer: layer, Dir: dir, Fps: fps, Age: "x", At: t0.Add(at), AsOf: engine.Latest}
}

const hour = 3600 * time.Second

// The band is the slack and nothing else: a window does not widen it. A pod's placement
// held by the pod collector (a window of an hour) exists from 0 to 11 hours, refreshed
// every quarter of an hour for ten: an instant half an hour before its end is far from
// any change, and the instants a millisecond either side of the start and the end are
// the boundary.
func TestClassifyIsBoundaryOnlyWithinTheSlackOfAChange(t *testing.T) {
	node, podA := fixture.FP(catalog.K8sNode, 1), fixture.FP(catalog.K8sPod, 11)
	var recs []engine.Record
	for at := time.Duration(0); at <= 10*hour; at += 15 * time.Minute {
		recs = append(recs, edgeRec(podA, node, "k8s-cluster", lifecycle.Observe, hour, at))
	}
	w := worldOf(t, recs...)
	ms := time.Millisecond
	cases := []struct {
		name string
		at   time.Duration
		want bool
	}{
		{"at 2 h: far from both ends", 2 * hour, false},
		{"at 10 h 30: the window is still open for half an hour, and that is no reason", 10*hour + 30*time.Minute, false},
		{"the start itself", 0, true},
		{"a millisecond before the start: the start is the right end of the band", -ms, true},
		{"two milliseconds before the start", -2 * ms, false},
		{"a millisecond after the start: the start is the left end, excluded", ms, false},
		{"a millisecond before the end", 11*hour - ms, true},
		{"two milliseconds before the end", 11*hour - 2*ms, false},
		{"the end itself", 11 * hour, true},
		{"a millisecond after the end", 11*hour + ms, false},
	}
	for _, c := range cases {
		got, err := w.Classify(w, query(runner.OpNeighbors, catalog.L2, engine.Reverse, c.at, node))
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if got.Boundary() != c.want || (c.want && got.Kind != vmmodel.Boundary) || (!c.want && got.Kind != vmmodel.Compared) {
			t.Errorf("%s: class %+v, want boundary %v", c.name, got, c.want)
		}
	}
	// A batch is the union of its entities, and a repeat adds nothing.
	got, err := w.Classify(w, query(runner.OpBatch, catalog.L2, engine.Reverse, 2*hour, node, node))
	if err != nil || got.Boundary() || got.Subjects != 1 {
		t.Errorf("batch: %+v, %v", got, err)
	}
}

// An existence read involves the entity whether or not it is alive.
func TestClassifyAnExistenceRead(t *testing.T) {
	node := fixture.FP(catalog.K8sNode, 1)
	var recs []engine.Record
	for at := time.Duration(0); at <= 10*hour; at += time.Minute {
		recs = append(recs, nodeRec(node, lifecycle.Observe, 240*time.Second, at))
	}
	w := worldOf(t, recs...)
	end := 10*hour + 240*time.Second
	for _, c := range []struct {
		name string
		at   time.Duration
		want bool
	}{
		{"alive at 5 h", 5 * hour, false},
		{"alive at 10 h: four minutes from its end", 10 * hour, false},
		{"its end", end, true},
		{"an hour after the end: dead, far from a change", end + hour, false},
		{"its start", 0, true},
		{"just after its start", 3 * time.Second, false},
	} {
		got, err := w.Classify(w, query(runner.OpAlive, catalog.L2, 0, c.at, node))
		if err != nil || got.Boundary() != c.want || got.Subjects != 1 {
			t.Errorf("%s: %+v, %v", c.name, got, err)
		}
	}
}

// The reference's stream may end an interval earlier than the store's, and may open a gap
// in the middle of one (the coalescer, a refresh that arrived late): an instant where the
// store's stream has the subject and the reference's does not is a boundary, however far
// from a change, and the store may return that subject.
func TestClassifyTheCoalescedEndGap(t *testing.T) {
	node, podA := fixture.FP(catalog.K8sNode, 1), fixture.FP(catalog.K8sPod, 11)
	ref := worldOf(t, edgeRec(podA, node, "k8s-cluster", lifecycle.Observe, hour, 0)) // [0, 1 h)
	held := worldOf(t,
		edgeRec(podA, node, "k8s-cluster", lifecycle.Observe, hour, 0),
		edgeRec(podA, node, "k8s-cluster", lifecycle.Observe, hour, 15*time.Minute), // [0, 1 h 15)
	)
	for _, c := range []struct {
		name string
		at   time.Duration
		want bool
	}{
		{"both alive", 30 * time.Minute, false},
		{"in the gap: only the store's stream has it", 65 * time.Minute, true},
		{"both dead", 2 * hour, false},
	} {
		got, err := ref.Classify(held, query(runner.OpNeighbors, catalog.L2, engine.Reverse, c.at, node))
		if err != nil || got.Boundary() != c.want {
			t.Errorf("%s: %+v, %v", c.name, got, err)
		}
		// With one view there is no gap.
		if one, err := ref.Classify(ref, query(runner.OpNeighbors, catalog.L2, engine.Reverse, c.at, node)); err != nil || one.Boundary() {
			t.Errorf("%s with one view: %+v, %v", c.name, one, err)
		}
	}
	// The same through an existence read of an entity.
	a := worldOf(t, nodeRec(node, lifecycle.Observe, 240*time.Second, 0))
	b := worldOf(t, nodeRec(node, lifecycle.Observe, 240*time.Second, 0), nodeRec(node, lifecycle.Observe, 240*time.Second, 120*time.Second))
	if got, err := a.Classify(b, query(runner.OpAlive, catalog.L2, 0, 5*time.Minute, node)); err != nil || !got.Boundary() {
		t.Errorf("alive in the gap: %+v, %v", got, err)
	}
}

// Against a brute force: the instant is a boundary exactly when existence changes in
// either view within a millisecond of it, or the store's view has the subject alive and
// the reference's does not. Intervals are on whole milliseconds, made of refreshes that
// overlap and touch, so that merging them is exercised too.
func TestClassifyAgainstBruteForce(t *testing.T) {
	node := fixture.FP(catalog.K8sNode, 1)
	rapid.Check(t, func(rt *rapid.T) {
		draw := func(name string) []engine.Record {
			n := rapid.IntRange(0, 4).Draw(rt, name+" refreshes")
			var recs []engine.Record
			for i := 0; i < n; i++ {
				at := time.Duration(rapid.IntRange(0, 40).Draw(rt, name+" at")) * time.Millisecond
				ttl := time.Duration(rapid.IntRange(1, 15).Draw(rt, name+" ttl")) * time.Millisecond
				recs = append(recs, engine.Record{Layer: catalog.L2, Subject: engine.EntitySubject(node), Producer: "node-collector", EventTime: t0.Add(at), Kind: lifecycle.Observe, TTL: ttl})
			}
			return recs
		}
		refRecs := draw("ref")
		// The store's view holds the reference's records and may have more, which can only
		// lengthen existence, as the coalescer's streams are related.
		held := vmmodel.NewWorld()
		extra := append(slices.Clone(refRecs), draw("store")...)
		for i := range extra {
			extra[i].Seq = uint64(i + 1)
		}
		if err := held.Write(extra); err != nil {
			rt.Fatal(err)
		}
		ref := worldOf(rt, refRecs...)
		at := time.Duration(rapid.IntRange(-2, 60).Draw(rt, "t")) * time.Millisecond
		s := engine.EntitySubject(node)
		alive := func(w *vmmodel.World, x time.Duration) bool {
			ok, err := w.AliveAt(s, t0.Add(x))
			if err != nil {
				rt.Fatal(err)
			}
			return ok
		}
		want := alive(held, at) && !alive(ref, at)
		for _, x := range []time.Duration{at, at + time.Millisecond} {
			want = want || alive(ref, x) != alive(ref, x-time.Millisecond) || alive(held, x) != alive(held, x-time.Millisecond)
		}
		got, err := ref.Classify(held, query(runner.OpAlive, catalog.L2, 0, at, node))
		if err != nil {
			rt.Fatal(err)
		}
		if got.Boundary() != want {
			rt.Fatalf("t=%s ref %v store %v: boundary %v, brute force %v", at, refRecs, extra, got.Boundary(), want)
		}
	})
}

func neighbor(pod identity.Fingerprint) []engine.Neighbor {
	return []engine.Neighbor{{Peer: pod, Relation: catalog.ScheduledOn}}
}

// A subject the store returned that the reference does not have is a read of a deleted
// fact only if a producer deleted it at most its window ago and nothing else differs.
func TestClassifyAnswerReadsADeletedFact(t *testing.T) {
	reg := registry(t)
	node, podA, podB := fixture.FP(catalog.K8sNode, 1), fixture.FP(catalog.K8sPod, 11), fixture.FP(catalog.K8sPod, 12)
	// The kubelet confirmed pod A's placement at 0 with a TTL of 300 s and withdrew it at
	// 100 s. Pod B's was never withdrawn (and is alive until 3,600 s by the pod collector).
	w := worldOf(t,
		edgeRec(podA, node, "kubelet", lifecycle.Observe, 300*time.Second, 0),
		edgeRec(podA, node, "kubelet", lifecycle.Delete, 0, 100*time.Second),
		edgeRec(podB, node, "k8s-cluster", lifecycle.Observe, hour, 0),
	)
	a := vmmodel.NeighborSubjects(node, engine.Reverse, neighbor(podA))
	b := vmmodel.NeighborSubjects(node, engine.Reverse, neighbor(podB))
	both := slices.Concat(a, b)
	sem := vmmodel.Semantics{}
	at := func(d time.Duration) runner.Query {
		return query(runner.OpNeighbors, catalog.L2, engine.Reverse, d, node)
	}
	for _, c := range []struct {
		name     string
		q        runner.Query
		returned []engine.Subject
		sem      vmmodel.Semantics
		want     vmmodel.Kind
	}{
		{"the deleted edge, 50 s after the delete, with pod B", at(150 * time.Second), both, sem, vmmodel.ReadsDeletedFact},
		{"the deleted edge without the live one: a subject of the reference is missing", at(150 * time.Second), a, sem, vmmodel.Compared},
		{"only the live one: the reference's answer", at(150 * time.Second), b, sem, vmmodel.Compared},
		{"the deleted edge just inside the window (299 s after)", at(399 * time.Second), both, sem, vmmodel.ReadsDeletedFact},
		{"the deleted edge a full window after: nothing explains it", at(400 * time.Second), both, sem, vmmodel.Compared},
		{"the deleted edge, but markers end a window: nothing explains it", at(150 * time.Second), both, vmmodel.Semantics{StaleVisible: true}, vmmodel.Compared},
		{"an edge that was never deleted, returned before it exists", at(-time.Hour), b, sem, vmmodel.Compared},
		{"at the delete itself, which is a boundary", at(100 * time.Second), both, sem, vmmodel.Boundary},
	} {
		got, err := w.ClassifyAnswer(reg, w, c.sem, c.q, c.returned)
		if err != nil || got.Kind != c.want {
			t.Errorf("%s: %+v, %v, want %s", c.name, got, err, c.want)
		}
	}

	// An existence read: the entity returned alive after its producer withdrew it.
	n := worldOf(t, nodeRec(node, lifecycle.Observe, 240*time.Second, 0), nodeRec(node, lifecycle.Delete, 0, 100*time.Second))
	ent := []engine.Subject{engine.EntitySubject(node)}
	if got, err := n.ClassifyAnswer(reg, n, sem, query(runner.OpAlive, catalog.L2, 0, 150*time.Second, node), ent); err != nil || got.Kind != vmmodel.ReadsDeletedFact {
		t.Errorf("alive after the withdrawal: %+v, %v", got, err)
	}
	if got, err := n.ClassifyAnswer(reg, n, sem, query(runner.OpAlive, catalog.L2, 0, 150*time.Second, node), nil); err != nil || got.Kind != vmmodel.Compared {
		t.Errorf("not alive, as the reference says: %+v, %v", got, err)
	}
	if got, err := n.ClassifyAnswer(reg, n, sem, query(runner.OpAlive, catalog.L2, 0, 400*time.Second, node), ent); err != nil || got.Kind != vmmodel.Compared {
		t.Errorf("alive a window after the withdrawal: %+v, %v", got, err)
	}
}

func TestClassifyRefusesWhatIsNotAnInstantReadOfTheLatest(t *testing.T) {
	node := fixture.FP(catalog.K8sNode, 1)
	w := worldOf(t, nodeRec(node, lifecycle.Observe, time.Minute, 0))
	old := query(runner.OpNeighbors, catalog.L2, engine.Reverse, time.Hour, node)
	old.AsOf = 5
	win := query(runner.OpWindow, catalog.L2, engine.Reverse, time.Hour, node)
	for name, q := range map[string]runner.Query{"an old token": old, "a window": win} {
		if _, err := w.Classify(w, q); !errors.Is(err, vmmodel.ErrUnclassifiable) {
			t.Errorf("%s: err = %v", name, err)
		}
		if _, err := w.ClassifyAnswer(registry(t), w, vmmodel.Semantics{}, q, nil); !errors.Is(err, vmmodel.ErrUnclassifiable) {
			t.Errorf("%s: ClassifyAnswer err = %v", name, err)
		}
	}
}

func TestAnswerIsTheReferencesAliveSubjects(t *testing.T) {
	node, podA, podB := fixture.FP(catalog.K8sNode, 1), fixture.FP(catalog.K8sPod, 11), fixture.FP(catalog.K8sPod, 12)
	w := worldOf(t,
		edgeRec(podA, node, "k8s-cluster", lifecycle.Observe, hour, 0),
		edgeRec(podB, node, "k8s-cluster", lifecycle.Observe, hour, 2*hour),
		nodeRec(node, lifecycle.Observe, time.Minute, 0),
	)
	reverse := func(at time.Duration) runner.Query {
		return query(runner.OpNeighbors, catalog.L2, engine.Reverse, at, node)
	}
	for _, c := range []struct {
		name string
		q    runner.Query
		want []engine.Subject
	}{
		{"the edge that is alive", reverse(30 * time.Minute), vmmodel.NeighborSubjects(node, engine.Reverse, neighbor(podA))},
		{"nothing between the two", reverse(90 * time.Minute), nil},
		{"the later one", reverse(2*hour + time.Minute), vmmodel.NeighborSubjects(node, engine.Reverse, neighbor(podB))},
		{"a batch with a repeat is a set", query(runner.OpBatch, catalog.L2, engine.Reverse, 30*time.Minute, node, node), vmmodel.NeighborSubjects(node, engine.Reverse, neighbor(podA))},
		{"an entity that is alive", query(runner.OpAlive, catalog.L2, 0, 30*time.Second, node), []engine.Subject{engine.EntitySubject(node)}},
		{"an entity that is not", query(runner.OpAlive, catalog.L2, 0, time.Hour, node), nil},
	} {
		got, err := w.Answer(c.q)
		if err != nil || !slices.Equal(got, c.want) {
			t.Errorf("%s: %v, %v, want %v", c.name, got, err, c.want)
		}
	}
}
