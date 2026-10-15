package interval_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

// The scenarios fix exact answers. Times are on 2026-01-01 UTC, written hh:mm.

const k8s lifecycle.Producer = "k8sobjects/default"

func hm(h, m int) time.Time {
	return time.Date(2026, 1, 1, h, m, 0, 0, time.UTC)
}

func win(from, to time.Time) interval.Window {
	return interval.Window{From: from, To: to, AsOf: store.Latest}
}

func pc(start, end time.Time, before bool, src lifecycle.EndSource, ps ...lifecycle.Producer) interval.Piece {
	return interval.Piece{Span: interval.Span{Start: start, End: end}, Before: before, EndSource: src, Producers: ps}
}

func elem(sub store.Subject, class interval.Class, transitions int, ps ...interval.Piece) interval.Element {
	return interval.Element{Subject: sub, Layer: layerOf(sub), Pieces: ps, Class: class, Transitions: transitions}
}

func mustEdges(t *testing.T, r *interval.Reader, fp identity.Fingerprint, dir store.Direction, w interval.Window) interval.Result {
	t.Helper()
	res, err := r.Edges(ctx, fp, dir, w)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func mustEntity(t *testing.T, r *interval.Reader, fp identity.Fingerprint, w interval.Window) interval.Result {
	t.Helper()
	res, err := r.Entity(ctx, fp, w)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func check(t *testing.T, what string, got, want []interval.Element) {
	t.Helper()
	if d := diff(got, want); d != "" {
		t.Errorf("%s:\n%s", what, d)
	}
}

func checkSpans(t *testing.T, what string, got []interval.Span, want ...interval.Span) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("%s: got %v, want %v", what, got, want)
		return
	}
	for i := range got {
		if !got[i].Start.Equal(want[i].Start) || !got[i].End.Equal(want[i].End) {
			t.Errorf("%s: got %v, want %v", what, got, want)
			return
		}
	}
}

func subjects(els []interval.Element) []store.Subject {
	out := make([]store.Subject, len(els))
	for i, el := range els {
		out[i] = el.Subject
	}
	return out
}

func checkSubjects(t *testing.T, what string, got []interval.Element, want ...store.Subject) {
	t.Helper()
	if g := subjects(got); !slices.Equal(g, want) {
		t.Errorf("%s: got %v, want %v", what, g, want)
	}
}

// scenarioA: two pods that never coexisted on a node, and one that was there
// throughout.
type scenarioA struct {
	s, a, b, n identity.Fingerprint
	sn, an, bn store.Subject
}

func newScenarioA(t *testing.T, f *feed) scenarioA {
	sc := scenarioA{
		s: mkfp(t, catalog.K8sPod, 1), a: mkfp(t, catalog.K8sPod, 2), b: mkfp(t, catalog.K8sPod, 3),
		n: mkfp(t, catalog.K8sNode, 1),
	}
	sc.sn = edge(sc.s, sc.n, catalog.ScheduledOn)
	sc.an = edge(sc.a, sc.n, catalog.ScheduledOn)
	sc.bn = edge(sc.b, sc.n, catalog.ScheduledOn)
	f.write(
		observe(ent(sc.s), k8s, hm(9, 0), 0), observe(ent(sc.a), k8s, hm(9, 0), 0),
		observe(ent(sc.b), k8s, hm(9, 0), 0), observe(ent(sc.n), k8s, hm(9, 0), 0),
		observe(sc.sn, k8s, hm(9, 0), 0),
		observe(sc.an, k8s, hm(9, 58), 0), deleted(sc.an, k8s, hm(10, 5)),
		observe(sc.bn, k8s, hm(10, 6), 0),
	)
	return sc
}

func (sc scenarioA) want() []interval.Element {
	return []interval.Element{
		elem(sc.sn, interval.ClassStable, 0, pc(hm(10, 0), hm(10, 30), true, lifecycle.EndUnknown, k8s)),
		elem(sc.an, interval.ClassDisappeared, 1, pc(hm(10, 0), hm(10, 5), true, lifecycle.EndProducer, k8s)),
		elem(sc.bn, interval.ClassAppeared, 1, pc(hm(10, 6), hm(10, 30), false, lifecycle.EndUnknown, k8s)),
	}
}

func TestScenarioATwoPodsThatNeverCoexisted(t *testing.T) {
	st := openStore(t, memstore.Options{})
	sc := newScenarioA(t, newFeed(t, st))
	w := win(hm(10, 0), hm(10, 30))
	for _, raw := range []bool{false, true} {
		res := mustEdges(t, interval.NewReader(st, interval.Options{Raw: raw}), sc.n, store.Reverse, w)
		check(t, "Edges(N, Reverse)", res.Elements, sc.want())
		if res.Token != 8 {
			t.Errorf("token %d, want 8", res.Token)
		}
		if len(res.Elements) != 3 {
			t.FailNow()
		}
		s, a, b := interval.Spans(res.Elements[0]), interval.Spans(res.Elements[1]), interval.Spans(res.Elements[2])
		// A union graph would show A and B on N together. They never were.
		checkSpans(t, "Intersect(A, B)", interval.Intersect(a, b))
		sa := interval.Intersect(s, a)
		checkSpans(t, "Intersect(S, A)", sa, interval.Span{Start: hm(10, 0), End: hm(10, 5)})
		if d := interval.Duration(sa); d != 5*time.Minute {
			t.Errorf("Duration(S, A) = %s", d)
		}
		sb := interval.Intersect(s, b)
		checkSpans(t, "Intersect(S, B)", sb, interval.Span{Start: hm(10, 6), End: hm(10, 30)})
		if d := interval.Duration(sb); d != 24*time.Minute {
			t.Errorf("Duration(S, B) = %s", d)
		}
		checkSpans(t, "Along(A, S, B)", interval.Along(a, s, b))
		checkSubjects(t, "Intersection", interval.Intersection(res.Elements), sc.sn)
		checkSubjects(t, "Union", interval.Union(res.Elements), sc.sn, sc.an, sc.bn)
		checkSubjects(t, "At(10:03)", interval.At(res.Elements, hm(10, 3)), sc.sn, sc.an)
		checkSubjects(t, "At(10:05)", interval.At(res.Elements, hm(10, 5)), sc.sn)
		checkSubjects(t, "At(10:05 - 1ns)", interval.At(res.Elements, hm(10, 5).Add(-time.Nanosecond)), sc.sn, sc.an)
		checkSubjects(t, "At(10:06)", interval.At(res.Elements, hm(10, 6)), sc.sn, sc.bn)
		for _, bad := range []time.Time{hm(10, 0).Add(-time.Nanosecond), hm(10, 30)} {
			if _, err := interval.AtChecked(res, bad); !errors.Is(err, store.ErrInvalid) {
				t.Errorf("AtChecked(%s): %v, want ErrInvalid", ts(bad), err)
			}
		}
		if got, err := interval.AtChecked(res, hm(10, 0)); err != nil || len(got) != 2 {
			t.Errorf("AtChecked(10:00) = %d elements, %v; want S and A", len(got), err)
		}
	}
}

func TestScenarioBHandOffAndOneNanosecondGap(t *testing.T) {
	p, n := mkfp(t, catalog.K8sPod, 1), mkfp(t, catalog.K8sNode, 1)
	e := edge(p, n, catalog.ScheduledOn)
	w := win(hm(10, 0), hm(10, 30))
	for _, tc := range []struct {
		name string
		p2At time.Time
		want interval.Element
	}{
		{"seamless", hm(10, 5), elem(e, interval.ClassStable, 0,
			pc(hm(10, 0), hm(10, 30), true, lifecycle.EndUnknown, "p1", "p2"))},
		{"one nanosecond gap", hm(10, 5).Add(time.Nanosecond), elem(e, interval.ClassFlapping, 2,
			pc(hm(10, 0), hm(10, 5), true, lifecycle.EndLivenessExpiry, "p1"),
			pc(hm(10, 5).Add(time.Nanosecond), hm(10, 30), false, lifecycle.EndUnknown, "p2"))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := openStore(t, memstore.Options{})
			newFeed(t, st).write(
				observe(ent(p), k8s, hm(9, 0), 0), observe(ent(n), k8s, hm(9, 0), 0),
				observe(e, "p1", hm(9, 55), 10*time.Minute), observe(e, "p2", tc.p2At, 0),
			)
			for _, raw := range []bool{true, false} {
				res := mustEdges(t, interval.NewReader(st, interval.Options{Raw: raw}), p, store.Forward, w)
				check(t, "Edges(P, Forward)", res.Elements, []interval.Element{tc.want})
			}
		})
	}
}

func TestScenarioCDeleteAndDeadlineAtOneInstant(t *testing.T) {
	p, n := mkfp(t, catalog.K8sPod, 1), mkfp(t, catalog.K8sNode, 1)
	e := edge(p, n, catalog.ScheduledOn)
	st := openStore(t, memstore.Options{})
	newFeed(t, st).write(
		observe(e, "p1", hm(10, 0), 5*time.Minute),
		observe(e, "p2", hm(10, 1), 0), deleted(e, "p2", hm(10, 5)),
	)
	r := interval.NewReader(st, interval.Options{Raw: true})
	piece := pc(hm(10, 0), hm(10, 5), false, lifecycle.EndProducer, "p1", "p2")
	res := mustEdges(t, r, p, store.Forward, win(hm(10, 0), hm(10, 30)))
	check(t, "window from 10:00", res.Elements, []interval.Element{elem(e, interval.ClassDisappeared, 1, piece)})
	res = mustEdges(t, r, p, store.Forward, win(hm(9, 59), hm(10, 30)))
	check(t, "window from 09:59", res.Elements, []interval.Element{elem(e, interval.ClassTransient, 2, piece)})
}

func TestScenarioDLateRecordAtTwoTokens(t *testing.T) {
	p, n := mkfp(t, catalog.K8sPod, 1), mkfp(t, catalog.K8sNode, 1)
	e := edge(p, n, catalog.ScheduledOn)
	st := openStore(t, memstore.Options{})
	newFeed(t, st).each(deleted(e, k8s, hm(10, 10)), observe(e, k8s, hm(10, 0), 0))
	r := interval.NewReader(st, interval.Options{Raw: true})
	w := win(hm(9, 50), hm(10, 30))

	w.AsOf = 1
	res := mustEdges(t, r, p, store.Forward, w)
	check(t, "token 1", res.Elements, nil)
	if res.Token != 1 {
		t.Errorf("token %d, want 1", res.Token)
	}
	w.AsOf = 2
	res = mustEdges(t, r, p, store.Forward, w)
	check(t, "token 2", res.Elements, []interval.Element{
		elem(e, interval.ClassTransient, 2, pc(hm(10, 0), hm(10, 10), false, lifecycle.EndProducer, k8s)),
	})
}

func TestScenarioERunExtensionAtAnOldToken(t *testing.T) {
	p, q, n := mkfp(t, catalog.K8sPod, 1), mkfp(t, catalog.K8sPod, 2), mkfp(t, catalog.K8sNode, 1)
	e := edge(p, n, catalog.ScheduledOn)
	st := openStore(t, memstore.Options{})
	newFeed(t, st).each(
		observe(ent(n), k8s, hm(9, 0), 0),                      // 1
		observe(ent(p), k8s, hm(9, 0), 0),                      // 2
		run(e, "kubelet", hm(10, 0), 2*time.Minute, hm(10, 2)), // 3
		observe(ent(q), k8s, hm(9, 0), 0),                      // 4
		run(e, "kubelet", hm(10, 0), 2*time.Minute, hm(10, 4)), // 5
	)
	for _, raw := range []bool{true, false} {
		r := interval.NewReader(st, interval.Options{Raw: raw})
		for _, tc := range []struct {
			token uint64
			end   time.Time
		}{{3, hm(10, 4)}, {4, hm(10, 4)}, {5, hm(10, 6)}} {
			w := win(hm(10, 0), hm(10, 30))
			w.AsOf = tc.token
			res := mustEdges(t, r, p, store.Forward, w)
			check(t, "token", res.Elements, []interval.Element{
				elem(e, interval.ClassDisappeared, 1, pc(hm(10, 0), tc.end, false, lifecycle.EndLivenessExpiry, "kubelet")),
			})
		}
	}
}

func TestScenarioFLostDeleteAndTheCascade(t *testing.T) {
	p, n := mkfp(t, catalog.K8sPod, 1), mkfp(t, catalog.K8sNode, 1)
	e := edge(p, n, catalog.ScheduledOn)
	w := win(hm(10, 0), hm(11, 0))
	type want struct{ raw, cascade interval.Element }
	for _, tc := range []struct {
		name string
		recs []store.Record
		want want
	}{
		{"lost delete", []store.Record{
			run(ent(p), k8s, hm(10, 0), 15*time.Minute, hm(10, 10)),
			observe(e, k8s, hm(10, 0), 0),
			observe(ent(n), k8s, hm(9, 0), 0),
		}, want{
			elem(e, interval.ClassStable, 0, pc(hm(10, 0), hm(11, 0), false, lifecycle.EndUnknown, k8s)),
			elem(e, interval.ClassDisappeared, 1, pc(hm(10, 0), hm(10, 25), false, lifecycle.EndCascade, k8s)),
		}},
		{"the edge's own source wins a tie", []store.Record{
			run(ent(p), k8s, hm(10, 0), 15*time.Minute, hm(10, 10)),
			observe(e, k8s, hm(10, 0), 0), deleted(e, k8s, hm(10, 25)),
			observe(ent(n), k8s, hm(9, 0), 0),
		}, want{
			elem(e, interval.ClassDisappeared, 1, pc(hm(10, 0), hm(10, 25), false, lifecycle.EndProducer, k8s)),
			elem(e, interval.ClassDisappeared, 1, pc(hm(10, 0), hm(10, 25), false, lifecycle.EndProducer, k8s)),
		}},
		{"an endpoint that begins inside the window", []store.Record{
			observe(e, k8s, hm(9, 0), 0),
			observe(ent(p), k8s, hm(9, 0), 0),
			observe(ent(n), k8s, hm(10, 10), 0),
		}, want{
			elem(e, interval.ClassStable, 0, pc(hm(10, 0), hm(11, 0), true, lifecycle.EndUnknown, k8s)),
			elem(e, interval.ClassAppeared, 1, pc(hm(10, 10), hm(11, 0), false, lifecycle.EndUnknown, k8s)),
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := openStore(t, memstore.Options{})
			newFeed(t, st).write(tc.recs...)
			res := mustEdges(t, interval.NewReader(st, interval.Options{Raw: true}), p, store.Forward, w)
			check(t, "raw", res.Elements, []interval.Element{tc.want.raw})
			res = mustEdges(t, interval.NewReader(st, interval.Options{}), p, store.Forward, w)
			check(t, "cascade", res.Elements, []interval.Element{tc.want.cascade})
			res = mustEdges(t, interval.NewReader(st, interval.Options{}), n, store.Reverse, w)
			check(t, "cascade, read from the node", res.Elements, []interval.Element{tc.want.cascade})
		})
	}
}

func TestScenarioGQuarantine(t *testing.T) {
	inner := openStore(t, memstore.Options{Policy: storetest.QuarantinePolicy()})
	h, n := mkfp(t, catalog.Host, 1), mkfp(t, catalog.K8sNode, 1)
	e := edge(n, h, catalog.RunsOn)
	host := func(boot string, at time.Time) store.Record {
		r := observe(ent(h), "node-collector", at, 15*time.Minute)
		r.Boot = boot
		return r
	}
	newFeed(t, inner).write(
		host("b1", hm(9, 0)), host("b2", hm(9, 5)), host("b1", hm(9, 10)),
		observe(ent(n), k8s, hm(9, 0), 0),
		observe(e, "node-collector", hm(9, 0), 0),
	)
	st := &spy{Store: inner}
	r := interval.NewReader(st, interval.Options{})
	w := win(hm(10, 0), hm(10, 30))

	res := mustEntity(t, r, h, w)
	if len(res.Elements) != 1 {
		t.Fatalf("Entity(H) = %v", describeAll(res.Elements))
	}
	el := res.Elements[0]
	if el.Quarantine == nil || !errors.Is(el.Quarantine, store.ErrQuarantined) ||
		el.Pieces != nil || el.Class != interval.ClassNone || el.Subject != ent(h) || el.Layer != catalog.L1 {
		t.Fatalf("Entity(H) = %v", describeAll(res.Elements))
	}
	q := el.Quarantine
	for _, view := range [][]interval.Element{interval.Union(res.Elements), interval.Intersection(res.Elements), interval.At(res.Elements, hm(10, 1))} {
		if len(view) != 1 || view[0].Quarantine == nil {
			t.Errorf("a view dropped the quarantined entity: %v", describeAll(view))
		}
	}

	want := elem(e, interval.ClassStable, 0, pc(hm(10, 0), hm(10, 30), true, lifecycle.EndUnknown, "node-collector"))
	want.Uncascaded = q
	st.reset()
	res = mustEdges(t, r, n, store.Forward, w)
	check(t, "Edges(N, Forward)", res.Elements, []interval.Element{want})
	wantLog := []string{"Window k8s.node L1", "Window k8s.node L2", "Alive k8s.node L2", "Alive host L1", "EntityWindow k8s.node L2"}
	if !slices.Equal(st.log, wantLog) {
		t.Errorf("reads %q, want %q", st.log, wantLog)
	}
	st.reset()
	res = mustEdges(t, r, h, store.Reverse, w)
	check(t, "Edges(H, Reverse)", res.Elements, []interval.Element{want})
	if st.log[0] != "Window host L1" {
		t.Errorf("reads %q, want the edges read in L1", st.log)
	}
	if u := interval.Union(res.Elements); len(res.Elements) != 1 || len(u) != 1 || !sameSpans(interval.Spans(res.Elements[0]), interval.Spans(u[0])) {
		t.Error("an uncascaded edge is judged by its own pieces in the views")
	}
}

func TestScenarioG2WriterDriftIsLoud(t *testing.T) {
	n, h, p := mkfp(t, catalog.K8sNode, 1), mkfp(t, catalog.Host, 1), mkfp(t, catalog.K8sPod, 1)
	w := win(hm(10, 0), hm(10, 30))
	inLayer := func(r store.Record, l catalog.Layer) store.Record { r.Layer = l; return r }
	for _, tc := range []struct {
		name string
		rec  store.Record
		fp   identity.Fingerprint
		dir  store.Direction
	}{
		{"unregistered relation", inLayer(observe(edge(n, h, "x.unknown"), k8s, hm(9, 0), 0), catalog.L1), n, store.Forward},
		{"derived relation", inLayer(observe(edge(n, h, catalog.SameAs), k8s, hm(9, 0), 0), catalog.L1), n, store.Forward},
		{"types the relation does not allow", observe(edge(p, h, catalog.RunsOn), k8s, hm(9, 0), 0), h, store.Reverse},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := openStore(t, memstore.Options{})
			newFeed(t, st).write(tc.rec)
			for _, raw := range []bool{true, false} {
				_, err := interval.NewReader(st, interval.Options{Raw: raw}).Edges(ctx, tc.fp, tc.dir, w)
				if !errors.Is(err, store.ErrInvalid) || !strings.Contains(err.Error(), tc.rec.Subject.A.String()) ||
					!strings.Contains(err.Error(), "layer L1") {
					t.Errorf("raw %t: %v, want ErrInvalid naming the edge and L1", raw, err)
				}
				if _, _, err := interval.NewReader(st, interval.Options{Raw: raw}).EdgesBatch(ctx, []identity.Fingerprint{tc.fp}, tc.dir, w); !errors.Is(err, store.ErrInvalid) {
					t.Errorf("EdgesBatch: %v, want ErrInvalid", err)
				}
			}
		})
	}
	t.Run("relation stored in another layer", func(t *testing.T) {
		// A runs_on edge written in L2 is invisible to a read of L1, its
		// relation's layer: a process's forward read covers L1 only, so it does
		// not see it. A node's forward read also covers L2 (has_address), finds
		// the edge there, and fails loudly. The writers' tests against the
		// catalog keep either from arising.
		proc := mkfp(t, catalog.Process, 1)
		st := openStore(t, memstore.Options{})
		newFeed(t, st).write(
			inLayer(observe(edge(proc, h, catalog.RunsOn), k8s, hm(9, 0), 0), catalog.L2),
			inLayer(observe(edge(n, h, catalog.RunsOn), k8s, hm(9, 0), 0), catalog.L2),
		)
		res := mustEdges(t, interval.NewReader(st, interval.Options{Raw: true}), proc, store.Forward, w)
		check(t, "Edges(process, Forward)", res.Elements, nil)
		for _, raw := range []bool{true, false} {
			_, err := interval.NewReader(st, interval.Options{Raw: raw}).Edges(ctx, n, store.Forward, w)
			if !errors.Is(err, store.ErrInvalid) || !strings.Contains(err.Error(), n.String()) || !strings.Contains(err.Error(), "layer L1") {
				t.Errorf("raw %t: Edges(N, Forward) = %v, want ErrInvalid naming the edge and L1", raw, err)
			}
		}
	})
}

// TestScenarioHRetention: after a retention a window that starts at or after a
// layer's horizon is read from the seeds the store keeps and gives the answer of
// a store that kept everything; one that starts before it is refused by the
// store, with ErrBeforeHorizon, at the first read of that layer.
func TestScenarioHRetention(t *testing.T) {
	p, n, h := mkfp(t, catalog.K8sPod, 1), mkfp(t, catalog.K8sNode, 1), mkfp(t, catalog.Host, 1)
	recs := []store.Record{
		observe(ent(p), k8s, hm(9, 0), 0), observe(ent(n), k8s, hm(9, 0), 0), observe(ent(h), k8s, hm(9, 0), 0),
		observe(edge(p, n, catalog.ScheduledOn), k8s, hm(9, 0), 0), observe(edge(n, h, catalog.RunsOn), k8s, hm(9, 0), 0),
	}
	w := win(hm(10, 0), hm(10, 30))
	early := win(hm(10, 0).Add(-time.Nanosecond), hm(10, 30))
	refused := func(t *testing.T, err error, layer string) {
		t.Helper()
		if !errors.Is(err, store.ErrBeforeHorizon) || errors.Is(err, interval.ErrRetained) || !strings.Contains(err.Error(), "layer "+layer) {
			t.Fatalf("%v, want ErrBeforeHorizon naming layer %s", err, layer)
		}
	}
	twin := openStore(t, memstore.Options{})
	newFeed(t, twin).write(recs...)
	open := func(t *testing.T, keep [4]bool) *spy {
		inner := openStore(t, memstore.Options{Keep: keep})
		newFeed(t, inner).write(recs...)
		if err := inner.Retain(ctx, hm(10, 0)); err != nil {
			t.Fatal(err)
		}
		return &spy{Store: inner}
	}
	// same requires a read of the retained store to answer as the twin does.
	same := func(t *testing.T, what string, got, want interval.Result, err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("%s: %v", what, err)
		}
		if df := diff(got.Elements, want.Elements); df != "" {
			t.Errorf("%s after the retention, and on a store that kept everything:\n%s", what, df)
		}
	}

	t.Run("every layer retained", func(t *testing.T) {
		st := open(t, [4]bool{})
		for _, raw := range []bool{false, true} {
			r, tw := interval.NewReader(st, interval.Options{Raw: raw}), interval.NewReader(twin, interval.Options{Raw: raw})
			for e := range catalog.Default().Entities() {
				for _, fp := range []identity.Fingerprint{mkfp(t, e.Type(), 9), p, n, h} {
					if fp.Type() != e.Type() {
						continue
					}
					got, err := r.Entity(ctx, fp, w)
					want, _ := tw.Entity(ctx, fp, w)
					same(t, "Entity", got, want, err)
					_, err = r.Entity(ctx, fp, early)
					refused(t, err, e.Layer().String())
					for _, dir := range []store.Direction{store.Forward, store.Reverse} {
						got, err := r.Edges(ctx, fp, dir, w)
						want, _ := tw.Edges(ctx, fp, dir, w)
						same(t, "Edges", got, want, err)
						_, err = r.Edges(ctx, fp, dir, early)
						_, _, berr := r.EdgesBatch(ctx, []identity.Fingerprint{fp, fp}, dir, early)
						layers := interval.EdgeLayers(catalog.Default(), e.Type(), dir)
						if len(layers) == 0 {
							// No layer is read, so nothing is refused.
							if err != nil || berr != nil {
								t.Errorf("%s %s with no edge layer: %v, %v", e.Type(), dir, err, berr)
							}
							continue
						}
						refused(t, err, layers[0].String())
						refused(t, berr, layers[0].String())
					}
				}
			}
		}
		if _, writes := st.counts(); writes != 0 {
			t.Errorf("%d writes; want none", writes)
		}
	})
	t.Run("only L1 retained", func(t *testing.T) {
		st := open(t, [4]bool{true, false, true, true})
		r := interval.NewReader(st, interval.Options{})
		res := mustEdges(t, r, p, store.Forward, early)
		if len(res.Elements) != 1 {
			t.Errorf("Edges(P, Forward) = %v", describeAll(res.Elements))
		}
		got, err := r.Edges(ctx, n, store.Forward, w)
		want, _ := interval.NewReader(twin, interval.Options{}).Edges(ctx, n, store.Forward, w)
		same(t, "Edges(N, Forward)", got, want, err)
		st.reset()
		_, err = r.Edges(ctx, n, store.Forward, early)
		refused(t, err, "L1")
		if !slices.Equal(st.log, []string{"Window k8s.node L1"}) {
			t.Errorf("reads %q; want the refusal at the first read of L1", st.log)
		}
	})
	t.Run("only L2 retained", func(t *testing.T) {
		st := open(t, [4]bool{true, true, false, true})
		// A node's forward edges are stored in L1 and L2 (has_address), so even a
		// raw read of them is refused, at the read of L2, after the one of L1.
		for _, raw := range []bool{true, false} {
			st.reset()
			_, err := interval.NewReader(st, interval.Options{Raw: raw}).Edges(ctx, n, store.Forward, early)
			refused(t, err, "L2")
			if !slices.Equal(st.log, []string{"Window k8s.node L1", "Window k8s.node L2"}) {
				t.Errorf("raw %t: reads %q; want the refusal at the read of L2", raw, st.log)
			}
		}
		got, err := interval.NewReader(st, interval.Options{}).Edges(ctx, n, store.Forward, w)
		want, _ := interval.NewReader(twin, interval.Options{}).Edges(ctx, n, store.Forward, w)
		same(t, "Edges(N, Forward)", got, want, err)
	})
}

func TestScenarioITokenPinnedOnce(t *testing.T) {
	inner := openStore(t, memstore.Options{})
	sc := newScenarioA(t, newFeed(t, inner))
	before := inner.LastSeq()
	st := &spy{Store: inner}
	late := newFeed(t, inner)
	st.onWindow = func(n int) {
		if n == 1 {
			late.write(deleted(sc.sn, k8s, hm(10, 1)))
		}
	}
	res := mustEdges(t, interval.NewReader(st, interval.Options{}), sc.n, store.Reverse, win(hm(10, 0), hm(10, 30)))
	if res.Token != before {
		t.Errorf("token %d, want %d", res.Token, before)
	}
	check(t, "Edges(N, Reverse)", res.Elements, sc.want())
	if inner.LastSeq() != before+1 {
		t.Fatal("the hook did not write")
	}
	if _, writes := st.counts(); writes != 0 {
		t.Errorf("the reader wrote %d times", writes)
	}
}

func TestTokenPinnedUnderConcurrentWrites(t *testing.T) {
	st := openStore(t, memstore.Options{})
	sc := newScenarioA(t, newFeed(t, st))
	done := make(chan struct{})
	go func() {
		defer close(done)
		seq := st.LastSeq()
		for i := range 40 {
			seq++
			r := observe(sc.sn, "w", hm(10, 0).Add(time.Duration(i)*time.Second), time.Second)
			r.Seq = seq
			if err := st.Write(context.Background(), []store.Record{r}); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	r := interval.NewReader(st, interval.Options{})
	w := win(hm(10, 0), hm(10, 30))
	for range 40 {
		res := mustEdges(t, r, sc.n, store.Reverse, w)
		w2 := w
		w2.AsOf = res.Token
		again := mustEdges(t, r, sc.n, store.Reverse, w2)
		check(t, "the answer at its own token", res.Elements, again.Elements)
	}
	<-done
}

func TestScenarioJArguments(t *testing.T) {
	inner := openStore(t, memstore.Options{})
	sc := newScenarioA(t, newFeed(t, inner))
	st := &spy{Store: inner}
	r := interval.NewReader(st, interval.Options{})
	w := win(hm(10, 0), hm(10, 30))
	invalid := func(what string, err error) {
		t.Helper()
		if !errors.Is(err, store.ErrInvalid) {
			t.Errorf("%s: %v, want ErrInvalid", what, err)
		}
	}
	for _, bad := range []interval.Window{win(hm(10, 0), hm(10, 0)), win(hm(10, 1), hm(10, 0))} {
		_, err := r.Entity(ctx, sc.s, bad)
		invalid("Entity, empty window", err)
		_, err = r.Edges(ctx, sc.s, store.Forward, bad)
		invalid("Edges, empty window", err)
		_, _, err = r.EdgesBatch(ctx, []identity.Fingerprint{sc.s}, store.Forward, bad)
		invalid("EdgesBatch, empty window", err)
		// The window is checked first.
		_, err = r.Edges(ctx, identity.Fingerprint{}, 0, bad)
		if err == nil || !strings.Contains(err.Error(), "window") {
			t.Errorf("an empty window with other bad arguments: %v, want the window named", err)
		}
	}
	unknown, err := identity.FingerprintFromHash("x.unknown", [identity.FingerprintBytes]byte{1})
	if err != nil {
		t.Fatal(err)
	}
	for _, dir := range []store.Direction{0, 3} {
		_, err := r.Edges(ctx, sc.s, dir, w)
		invalid("direction", err)
		_, _, err = r.EdgesBatch(ctx, []identity.Fingerprint{sc.s}, dir, w)
		invalid("EdgesBatch direction", err)
	}
	for _, fp := range []identity.Fingerprint{{}, unknown} {
		_, err := r.Entity(ctx, fp, w)
		invalid("Entity fingerprint", err)
		_, err = r.Edges(ctx, fp, store.Forward, w)
		invalid("Edges fingerprint", err)
		_, _, err = r.EdgesBatch(ctx, []identity.Fingerprint{sc.s, fp}, store.Forward, w)
		invalid("EdgesBatch fingerprint", err)
	}
	if reads, _ := st.counts(); reads != 0 {
		t.Errorf("%d reads for invalid arguments: %q", reads, st.log)
	}

	port := mkfp(t, catalog.Port, 1)
	res, err := r.Edges(ctx, port, store.Forward, w)
	if err != nil || res.Elements != nil || res.Token != inner.LastSeq() {
		t.Errorf("Edges(port, Forward) = %v, %v", res, err)
	}
	tok, out, err := r.EdgesBatch(ctx, []identity.Fingerprint{port, port}, store.Reverse, w)
	if err != nil || len(out) != 2 || out[0] != nil || out[1] != nil || tok != inner.LastSeq() {
		t.Errorf("EdgesBatch(ports) = %d, %v, %v", tok, out, err)
	}
	tok, out, err = r.EdgesBatch(ctx, nil, store.Reverse, w)
	if err != nil || len(out) != 0 || tok != inner.LastSeq() {
		t.Errorf("EdgesBatch(nil) = %d, %v, %v", tok, out, err)
	}
	if reads, _ := st.counts(); reads != 0 {
		t.Errorf("%d reads for a type with no edge layer: %q", reads, st.log)
	}

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = r.Edges(cancelled, sc.n, store.Reverse, w)
	_, eerr := r.Entity(cancelled, sc.n, w)
	if !errors.Is(err, context.Canceled) || !errors.Is(eerr, context.Canceled) {
		t.Errorf("cancelled: %v, %v", err, eerr)
	}
	if err := inner.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = r.Edges(ctx, sc.n, store.Reverse, w)
	_, eerr = r.Entity(ctx, sc.n, w)
	if !errors.Is(err, store.ErrClosed) || !errors.Is(eerr, store.ErrClosed) {
		t.Errorf("closed: %v, %v", err, eerr)
	}
}

func TestScenarioKClasses(t *testing.T) {
	w := win(hm(10, 0), hm(10, 30))
	for i, tc := range []struct {
		name string
		recs func(store.Subject) []store.Record
		want func(store.Subject) interval.Element
	}{
		{"none", func(s store.Subject) []store.Record {
			return []store.Record{observe(s, k8s, hm(10, 40), 0)}
		}, func(s store.Subject) interval.Element { return elem(s, interval.ClassNone, 0) }},
		{"stable", func(s store.Subject) []store.Record {
			return []store.Record{observe(s, k8s, hm(9, 0), 0)}
		}, func(s store.Subject) interval.Element {
			return elem(s, interval.ClassStable, 0, pc(hm(10, 0), hm(10, 30), true, lifecycle.EndUnknown, k8s))
		}},
		{"appeared", func(s store.Subject) []store.Record {
			return []store.Record{observe(s, k8s, hm(10, 10), 0)}
		}, func(s store.Subject) interval.Element {
			return elem(s, interval.ClassAppeared, 1, pc(hm(10, 10), hm(10, 30), false, lifecycle.EndUnknown, k8s))
		}},
		{"disappeared", func(s store.Subject) []store.Record {
			return []store.Record{observe(s, k8s, hm(9, 0), 0), deleted(s, k8s, hm(10, 10))}
		}, func(s store.Subject) interval.Element {
			return elem(s, interval.ClassDisappeared, 1, pc(hm(10, 0), hm(10, 10), true, lifecycle.EndProducer, k8s))
		}},
		{"transient", func(s store.Subject) []store.Record {
			return []store.Record{observe(s, k8s, hm(10, 5), 0), deleted(s, k8s, hm(10, 10))}
		}, func(s store.Subject) interval.Element {
			return elem(s, interval.ClassTransient, 2, pc(hm(10, 5), hm(10, 10), false, lifecycle.EndProducer, k8s))
		}},
		{"transient ending one nanosecond before To", func(s store.Subject) []store.Record {
			return []store.Record{observe(s, k8s, hm(10, 0).Add(time.Nanosecond), 0), deleted(s, k8s, hm(10, 30).Add(-time.Nanosecond))}
		}, func(s store.Subject) interval.Element {
			return elem(s, interval.ClassTransient, 2, pc(hm(10, 0).Add(time.Nanosecond), hm(10, 30).Add(-time.Nanosecond), false, lifecycle.EndProducer, k8s))
		}},
		{"flapping, left and returned", func(s store.Subject) []store.Record {
			return []store.Record{observe(s, k8s, hm(9, 0), 0), deleted(s, k8s, hm(10, 10)), observe(s, k8s, hm(10, 20), 0)}
		}, func(s store.Subject) interval.Element {
			return elem(s, interval.ClassFlapping, 2,
				pc(hm(10, 0), hm(10, 10), true, lifecycle.EndProducer, k8s),
				pc(hm(10, 20), hm(10, 30), false, lifecycle.EndUnknown, k8s))
		}},
		{"flapping, neither end", func(s store.Subject) []store.Record {
			return []store.Record{
				observe(s, k8s, hm(10, 1), 0), deleted(s, k8s, hm(10, 5)),
				observe(s, k8s, hm(10, 10), 3*time.Minute),
			}
		}, func(s store.Subject) interval.Element {
			return elem(s, interval.ClassFlapping, 4,
				pc(hm(10, 1), hm(10, 5), false, lifecycle.EndProducer, k8s),
				pc(hm(10, 10), hm(10, 13), false, lifecycle.EndLivenessExpiry, k8s))
		}},
		{"starts at From without Before, to To", func(s store.Subject) []store.Record {
			return []store.Record{observe(s, k8s, hm(10, 0), 0)}
		}, func(s store.Subject) interval.Element {
			return elem(s, interval.ClassStable, 0, pc(hm(10, 0), hm(10, 30), false, lifecycle.EndUnknown, k8s))
		}},
		{"starts at From without Before, ends inside", func(s store.Subject) []store.Record {
			return []store.Record{observe(s, k8s, hm(10, 0), 0), deleted(s, k8s, hm(10, 10))}
		}, func(s store.Subject) interval.Element {
			return elem(s, interval.ClassDisappeared, 1, pc(hm(10, 0), hm(10, 10), false, lifecycle.EndProducer, k8s))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := openStore(t, memstore.Options{})
			pod, node := mkfp(t, catalog.K8sPod, byte(i)), mkfp(t, catalog.K8sNode, 1)
			e := edge(pod, node, catalog.ScheduledOn)
			newFeed(t, st).write(append(tc.recs(ent(pod)), tc.recs(e)...)...)
			res := mustEntity(t, interval.NewReader(st, interval.Options{}), pod, w)
			check(t, "Entity", res.Elements, []interval.Element{tc.want(ent(pod))})
			want := []interval.Element{tc.want(e)}
			if len(want[0].Pieces) == 0 {
				want = nil
			}
			res = mustEdges(t, interval.NewReader(st, interval.Options{Raw: true}), pod, store.Forward, w)
			check(t, "raw Edges", res.Elements, want)
		})
	}
}

func TestScenarioLTwoLayersInOneRead(t *testing.T) {
	s, s2 := mkfp(t, catalog.Service, 1), mkfp(t, catalog.Service, 2)
	si := mkfp(t, catalog.ServiceInstance, 1)
	inst, dep := edge(si, s, catalog.InstanceOf), edge(s2, s, catalog.DependsOn)
	if got := interval.EdgeLayers(catalog.Default(), catalog.Service, store.Reverse); !slices.Equal(got, []catalog.Layer{catalog.L2, catalog.L3}) {
		t.Fatalf("EdgeLayers(service, Reverse) = %v", got)
	}
	inner := openStore(t, memstore.Options{})
	newFeed(t, inner).write(
		observe(ent(s), "traces", hm(9, 0), 0), observe(ent(s2), "traces", hm(9, 0), 0), observe(ent(si), "traces", hm(9, 0), 0),
		observe(inst, "traces", hm(9, 30), 0), observe(dep, "traces", hm(10, 5), 0),
	)
	want := []interval.Element{
		elem(dep, interval.ClassAppeared, 1, pc(hm(10, 5), hm(10, 30), false, lifecycle.EndUnknown, "traces")),
		elem(inst, interval.ClassStable, 0, pc(hm(10, 0), hm(10, 30), true, lifecycle.EndUnknown, "traces")),
	}
	st := &spy{Store: inner}
	late := newFeed(t, inner)
	// The write lands between the two Window calls, the L2 read and the L3 read.
	st.afterWin = func(n int) {
		if n == 1 {
			late.write(deleted(dep, "traces", hm(10, 6)))
		}
	}
	res := mustEdges(t, interval.NewReader(st, interval.Options{}), s, store.Reverse, win(hm(10, 0), hm(10, 30)))
	check(t, "Edges(S, Reverse)", res.Elements, want)
	if st.windows != 2 || inner.LastSeq() != res.Token+1 {
		t.Errorf("%d windows, token %d, last %d; want both layers read at the token before the write", st.windows, res.Token, inner.LastSeq())
	}
}

func TestEdgesBatchSharesOneTokenAndRepeats(t *testing.T) {
	st := openStore(t, memstore.Options{})
	sc := newScenarioA(t, newFeed(t, st))
	r := interval.NewReader(st, interval.Options{})
	w := win(hm(10, 0), hm(10, 30))
	tok, out, err := r.EdgesBatch(ctx, []identity.Fingerprint{sc.n, sc.a, sc.n}, store.Reverse, w)
	if err != nil || tok != st.LastSeq() || len(out) != 3 {
		t.Fatalf("EdgesBatch = %d, %d, %v", tok, len(out), err)
	}
	check(t, "out[0]", out[0], sc.want())
	check(t, "out[2]", out[2], sc.want())
	check(t, "out[1]", out[1], nil)
	out[0][0].Pieces[0].Producers[0] = "changed"
	check(t, "out[2] after a change to out[0]", out[2], sc.want())
	tok, out, err = r.EdgesBatch(ctx, []identity.Fingerprint{sc.a, sc.b}, store.Forward, w)
	if err != nil || len(out) != 2 {
		t.Fatal(err)
	}
	check(t, "A forward", out[0], sc.want()[1:2])
	check(t, "B forward", out[1], sc.want()[2:3])
	_ = tok
}
