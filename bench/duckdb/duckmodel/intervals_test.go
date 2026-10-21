package duckmodel

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// expectedRows are the rows an interval file holds for a set of records, derived here
// from the lifecycle specification directly, a subject at a time.
func expectedRows(t testingT, written []engine.Record) (fwd, rev, ent []IntervalRow) {
	t.Helper()
	bySubject := map[engine.Subject][]engine.Record{}
	for _, r := range written {
		bySubject[r.Subject] = append(bySubject[r.Subject], r)
	}
	for sub, rs := range bySubject {
		as := make([]lifecycle.Assertion, len(rs))
		for i, r := range rs {
			as[i] = r.Assertion()
		}
		tl, err := lifecycle.Fold(as, lifecycle.Policy{})
		if err != nil {
			t.Fatal(err)
		}
		for _, iv := range tl.Existence() {
			row := IntervalRow{Layer: rs[0].Layer, Relation: sub.Relation, Interval: Interval{Start: iv.Start.UnixNano(), EndSource: iv.EndSource}}
			if iv.Open() {
				row.Open = true
			} else {
				row.End = iv.End.UnixNano()
			}
			switch sub.Kind {
			case engine.SubjectEntity:
				row.A = sub.A.String()
				ent = append(ent, row)
			default:
				f, r := row, row
				f.A, f.Peer = sub.A.String(), sub.B.String()
				r.A, r.Peer = sub.B.String(), sub.A.String()
				fwd, rev = append(fwd, f), append(rev, r)
			}
		}
	}
	// A file's order: layer, the text of the end it is seen from, start, then the peer and relation.
	order := func(a, b IntervalRow) int {
		for _, c := range []int{
			strings.Compare(a.Layer.String(), b.Layer.String()), strings.Compare(a.A, b.A), cmp64(a.Start, b.Start),
			strings.Compare(a.Peer, b.Peer), strings.Compare(string(a.Relation), string(b.Relation)),
		} {
			if c != 0 {
				return c
			}
		}
		return 0
	}
	slices.SortFunc(fwd, order)
	slices.SortFunc(rev, order)
	slices.SortFunc(ent, order)
	return fwd, rev, ent
}

func cmp64(a, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// rowsDiffer returns what differs between a model's interval rows and those derived from
// the records it was given.
func rowsDiffer(t testingT, m *Model, written []engine.Record) string {
	t.Helper()
	fwd, rev, ent := expectedRows(t, written)
	gf, err := m.EdgeIntervals(engine.Forward)
	if err != nil {
		t.Fatal(err)
	}
	gr, err := m.EdgeIntervals(engine.Reverse)
	if err != nil {
		t.Fatal(err)
	}
	ge, err := m.EntityIntervals()
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name      string
		got, want []IntervalRow
	}{{"forward edge rows", gf, fwd}, {"reverse edge rows", gr, rev}, {"entity rows", ge, ent}} {
		if !slices.Equal(c.got, c.want) {
			return c.name
		}
	}
	return ""
}

// TestIntervalRowsAreTheFoldOfEachSubject: on every fixed workload, the rows of the
// interval file, in each direction and for entities, are the maximal intervals the
// lifecycle fold gives each subject, with how each ended, in the file's order.
func TestIntervalRowsAreTheFoldOfEachSubject(t *testing.T) {
	for i, cfg := range conformance.Configs() {
		m := New(ArmIntervals)
		s := stream(t, cfg, 50, nil, []engine.Engine{m})
		if d := rowsDiffer(t, m, s.written); d != "" {
			t.Errorf("config %d: the %s differ from the fold", i, d)
		}
		st, err := m.Stats()
		if err != nil {
			t.Fatal(err)
		}
		fwd, _, ent := expectedRows(t, s.written)
		if st.Intervals != len(fwd)+len(ent) || st.Records != len(s.written) || st.Intervals == 0 || st.Intervals >= st.Records {
			t.Errorf("config %d: stats %+v for %d records and %d+%d intervals", i, st, len(s.written), len(fwd), len(ent))
		}
	}
}

func fixtureEdge(t testingT) engine.Subject {
	t.Helper()
	return engine.EdgeSubject(fp(t, "k8s.pod", 1), fp(t, "k8s.node", 2), catalog.ScheduledOn)
}

func rec1(sub engine.Subject, seq uint64, p lifecycle.Producer, kind lifecycle.Kind, at, ttl int64) engine.Record {
	r := engine.Record{
		Layer: catalog.L2, Subject: sub, Producer: p, EventTime: time.Unix(at, 0).UTC(), Seq: seq, Kind: kind,
		TTL: time.Duration(ttl) * time.Second,
	}
	if kind == lifecycle.Observe {
		r.Payload = []byte{byte(seq)}
	}
	return r
}

// TestIntervalFixtureFaults: two edges with the cases a fold has to get right, where each
// of three faults of the conversion gives other rows. On the first, p observes [10, 15)
// and q observes from 15, so the runs touch and are one interval that ends where q's
// does, by the expiry of q; r observes at 40 and never lapses, so its interval is open.
// On the second, p's delete ends [50, 55) by the producer. The rows are checked against
// the fold, and the faults must not give them.
func TestIntervalFixtureFaults(t *testing.T) {
	sub := fixtureEdge(t)
	other := engine.EdgeSubject(fp(t, "k8s.pod", 3), fp(t, "k8s.node", 2), catalog.ScheduledOn)
	recs := []engine.Record{
		rec1(sub, 1, "p", lifecycle.Observe, 10, 5),
		rec1(sub, 2, "q", lifecycle.Observe, 15, 5),
		rec1(sub, 3, "r", lifecycle.Observe, 40, 0),
		rec1(other, 4, "p", lifecycle.Observe, 50, 10),
		rec1(other, 5, "p", lifecycle.Delete, 55, 0),
	}

	run := func(mut mutant) (*Model, string) {
		m := newModel(ArmIntervals, mut)
		if err := m.Write(cloneRecords(recs)); err != nil {
			t.Fatal(err)
		}
		return m, rowsDiffer(t, m, recs)
	}
	m, d := run(mutNone)
	if d != "" {
		t.Fatalf("the honest model's %s differ from the fold", d)
	}
	rows, err := m.EdgeIntervals(engine.Forward)
	if err != nil {
		t.Fatal(err)
	}
	// The shape the fixture was built for: the merged interval, the open one, the one a
	// delete ended. They are on two edges, so the file's order puts the first edge's first.
	var shape []string
	for _, r := range rows {
		end := "open"
		if !r.Open {
			end = time.Unix(0, r.End).UTC().Format("15:04:05") + " " + r.EndSource.String()
		}
		shape = append(shape, time.Unix(0, r.Start).UTC().Format("15:04:05")+" to "+end)
	}
	want := []string{"00:00:10 to 00:00:20 liveness_expiry", "00:00:40 to open", "00:00:50 to 00:00:55 producer"}
	if !slices.Equal(shape, want) {
		t.Fatalf("rows %q, want %q", shape, want)
	}
	for name, mut := range map[string]mutant{
		"touching intervals left split":    mutIntervalsSplit,
		"an open interval given an end":    mutOpenGetsEnd,
		"an interval's end source dropped": mutEndSourceDropped,
	} {
		if _, d := run(mut); d == "" {
			t.Errorf("%s: the rows still equal the fold", name)
		}
	}
}

// TestIntervalsAreRefoldedWhenRecordsChange: a late record, a delete and a retention
// each change what the interval file answers, and the next read sees it.
func TestIntervalsAreRefoldedWhenRecordsChange(t *testing.T) {
	sub := fixtureEdge(t)
	src, dst := sub.A, sub.B
	m := New(ArmIntervals)
	sc := engine.Current(catalog.L2)
	at := func(s int64) time.Time { return time.Unix(s, 0).UTC() }
	alive := func(s int64) bool {
		t.Helper()
		ns, err := m.Neighbors(src, engine.Forward, at(s), sc)
		if err != nil {
			t.Fatal(err)
		}
		return len(ns) == 1 && ns[0].Peer == dst
	}
	if err := m.Write([]engine.Record{rec1(sub, 1, "p", lifecycle.Observe, 10, 10)}); err != nil {
		t.Fatal(err)
	}
	if !alive(12) || alive(25) {
		t.Fatal("after the first record")
	}
	// A late delete at 11, written second: the edge is no longer alive at 12 now, and
	// the old token still sees it alive.
	if err := m.Write([]engine.Record{rec1(sub, 2, "p", lifecycle.Delete, 11, 0)}); err != nil {
		t.Fatal(err)
	}
	if alive(12) {
		t.Fatal("after a late delete the edge is alive at 12")
	}
	old, err := m.Neighbors(src, engine.Forward, at(12), engine.Scope{Layer: catalog.L2, AsOf: 1})
	if err != nil || len(old) != 1 {
		t.Fatalf("as of the token before the delete: %v, %v (the interval file cannot answer this; the records do)", old, err)
	}
	if err := m.Write([]engine.Record{rec1(sub, 3, "q", lifecycle.Observe, 20, 0)}); err != nil {
		t.Fatal(err)
	}
	if !alive(30) || alive(15) {
		t.Fatal("after a record by another producer")
	}
	if err := m.Retain(at(25)); err != nil {
		t.Fatal(err)
	}
	if !alive(30) || !alive(25) {
		t.Fatal("after a retention the baseline of q must stand for the edge")
	}
}

func TestEdgeIntervalsRefusesAnotherDirection(t *testing.T) {
	if _, err := New(ArmIntervals).EdgeIntervals(engine.Direction(0)); err == nil {
		t.Fatal("a direction that is neither forward nor reverse was accepted")
	}
}
