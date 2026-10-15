package blastradius_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/blastradius"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

// The Around scenarios fix exact answers, written before the code, on the
// conventions of the Impact scenarios: times are on 2026-01-01 UTC, written
// hh:mm; every entity is observed at 09:00 with no end; the window is
// [10:00, 10:30) with the default policy unless a scenario says otherwise.

func around(t *testing.T, st store.Store, pod identity.Fingerprint, w interval.Window, o blastradius.Options) blastradius.AroundResult {
	t.Helper()
	sp := &spy{Store: st}
	res, err := blastradius.NewWalker(sp).Around(ctx, pod, w, o)
	if err != nil {
		t.Fatal(err)
	}
	if sp.writes != 0 {
		t.Fatalf("%d writes or retentions through the walker's store", sp.writes)
	}
	return res
}

// sdesc is a substrate as one comparable line: the entity, depth, time on it,
// whether it is expanded, and per upward leg its from entity, relation, spans
// and edge.
func sdesc(s blastradius.Substrate) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s depth=%d on=%s %s expanded=%t", s.Entity, s.Depth, spansString(s.On), s.Duration, s.Expanded)
	for _, v := range s.Vias {
		fmt.Fprintf(&b, " <%s %s %s leg=%s>", v.From, v.Relation, spansString(v.Spans), describeLeg(v.Leg))
	}
	return b.String()
}

func sdescs(ss []blastradius.Substrate) []string {
	out := make([]string, len(ss))
	for i, s := range ss {
		out[i] = sdesc(s)
	}
	return out
}

// sbrief is a substrate without its legs' pieces.
func sbrief(s blastradius.Substrate) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s depth=%d on=%s %s expanded=%t", s.Entity, s.Depth, spansString(s.On), s.Duration, s.Expanded)
	for _, v := range s.Vias {
		fmt.Fprintf(&b, " <%s %s %s leg=%s>%s>", v.From, v.Relation, spansString(v.Spans), v.Leg.Subject.A, v.Leg.Subject.B)
	}
	return b.String()
}

func sbriefs(ss []blastradius.Substrate) []string {
	out := make([]string, len(ss))
	for i, s := range ss {
		out[i] = sbrief(s)
	}
	return out
}

// sline is the expected sbrief line of a substrate the pod was on over on,
// through vias.
func sline(fp identity.Fingerprint, depth int, on []interval.Span, expanded bool, vias ...arr) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s depth=%d on=%s %s expanded=%t", fp, depth, spansString(on), duration(on), expanded)
	for _, v := range vias {
		fmt.Fprintf(&b, " <%s %s %s leg=%s>%s>", v.parent, v.rel, spansString(v.spans), v.parent, fp)
	}
	return b.String()
}

// upwardReads is the reads of the upward walk alone: Reads less every
// substrate walk's.
func upwardReads(res blastradius.AroundResult) blastradius.Reads {
	r := res.Reads
	for _, s := range res.Substrates {
		r.Calls -= s.Impact.Reads.Calls
		r.Windows -= s.Impact.Reads.Windows
		r.Alive -= s.Impact.Reads.Alive
		r.EntityWindows -= s.Impact.Reads.EntityWindows
	}
	return r
}

func substrate(t *testing.T, res blastradius.AroundResult, fp identity.Fingerprint) blastradius.Substrate {
	t.Helper()
	for _, s := range res.Substrates {
		if s.Entity == fp {
			return s
		}
	}
	t.Fatalf("%s is no substrate: %v", fp, sbriefs(res.Substrates))
	return blastradius.Substrate{}
}

// sameAround returns "" when two answers agree on everything, the reads of
// each substrate included, and otherwise the first difference.
func sameAround(got, want blastradius.AroundResult) string {
	if got.Token != want.Token || !got.From.Equal(want.From) || !got.To.Equal(want.To) || got.Seed != want.Seed ||
		got.PolicyVersion != want.PolicyVersion || got.IntervalPolicy != want.IntervalPolicy {
		return fmt.Sprintf("header %d %s %s %s %d %d, want %d %s %s %s %d %d",
			got.Token, ts(got.From), ts(got.To), got.Seed, got.PolicyVersion, got.IntervalPolicy,
			want.Token, ts(want.From), ts(want.To), want.Seed, want.PolicyVersion, want.IntervalPolicy)
	}
	if d := diffLines("substrates", sdescs(got.Substrates), sdescs(want.Substrates)); d != "" {
		return d
	}
	for i, g := range got.Substrates {
		w := want.Substrates[i]
		if d := sameResult(g.Impact, w.Impact); d != "" {
			return fmt.Sprintf("Impact(%s): %s", g.Entity, d)
		}
		if g.Impact.Reads != w.Impact.Reads {
			return fmt.Sprintf("Impact(%s): reads %+v, want %+v", g.Entity, g.Impact.Reads, w.Impact.Reads)
		}
	}
	for _, d := range []string{
		diffLines("unattributed", describeUnattributed(got.Unattributed), describeUnattributed(want.Unattributed)),
		diffLines("quarantined", quarantinedSubjects(got.Quarantined), quarantinedSubjects(want.Quarantined)),
	} {
		if d != "" {
			return d
		}
	}
	if fmt.Sprint(got.Truncated) != fmt.Sprint(want.Truncated) {
		return fmt.Sprintf("truncated %+v, want %+v", got.Truncated, want.Truncated)
	}
	if got.Reads != want.Reads {
		return fmt.Sprintf("reads %+v, want %+v", got.Reads, want.Reads)
	}
	return ""
}

var (
	P3, C2, C3 = pod(3), container(2), container(3)
	R1, Z1     = rack(1), zone(1)
)

// scenarioAR is A's shape with fabric: P1 on N1, N1 on H, H in R1 and Z1; P2
// on N1 over [10:10, 10:20); P3 on N2 on H. Every pod has one container.
func scenarioAR(t *testing.T) *memstore.Store {
	st, f := world(t, memstore.Options{}, H, N1, N2, R1, Z1, P1, P2, P3, C1, C2, C3)
	f.write(
		since(locatedIn(H, R1), hm(9, 0)), since(locatedIn(H, Z1), hm(9, 0)),
		since(runsOn(N1, H), hm(9, 0)), since(runsOn(N2, H), hm(9, 0)),
		since(sched(P1, N1), hm(9, 0)),
		since(sched(P2, N1), hm(10, 10)), gone(sched(P2, N1), hm(10, 20)),
		since(sched(P3, N2), hm(9, 0)),
		since(partOf(C1, P1), hm(9, 0)), since(partOf(C2, P2), hm(9, 0)), since(partOf(C3, P3), hm(9, 0)))
	return st
}

var mid = span(hm(10, 10), hm(10, 20))

func TestARAWholeChain(t *testing.T) {
	st := scenarioAR(t)
	res := around(t, st, P1, w30, blastradius.Options{})
	expect(t, "substrates", sbriefs(res.Substrates),
		sline(N1, 1, whole, true, arr{P1, catalog.ScheduledOn, whole}),
		sline(H, 2, whole, true, arr{N1, catalog.RunsOn, whole}),
		sline(R1, 3, whole, false, arr{H, catalog.LocatedIn, whole}),
		sline(Z1, 3, whole, false, arr{H, catalog.LocatedIn, whole}))
	if res.Token != st.LastSeq() || res.Seed != P1 || res.PolicyVersion != 1 || res.IntervalPolicy != interval.PolicyVersion ||
		!res.From.Equal(hm(10, 0)) || !res.To.Equal(hm(10, 30)) {
		t.Errorf("header %+v", res)
	}
	expect(t, "unattributed", describeUnattributed(res.Unattributed))
	expect(t, "quarantined", quarantinedSubjects(res.Quarantined))
	if fmt.Sprint(res.Truncated) != fmt.Sprint(blastradius.Truncation{}) {
		t.Errorf("truncated %+v", res.Truncated)
	}

	// Under the node: the other pod over its time, and its container. The seed
	// pod and its container are absent.
	n := substrate(t, res, N1).Impact
	expect(t, "Impact(N1)", briefs(n.Members),
		mline(P2, 1, mid, interval.ClassTransient, 2, arr{N1, catalog.ScheduledOn, mid}),
		mline(C2, 2, mid, interval.ClassTransient, 2, arr{P2, catalog.PartOf, mid}))
	expectReads(t, n.Reads, 2, 2, 5, 5)
	// Under the host: both nodes, the other two pods and their containers.
	h := substrate(t, res, H).Impact
	expect(t, "Impact(H)", briefs(h.Members),
		mline(N1, 1, whole, interval.ClassStable, 0, arr{H, catalog.RunsOn, whole}),
		mline(N2, 1, whole, interval.ClassStable, 0, arr{H, catalog.RunsOn, whole}),
		mline(P3, 2, whole, interval.ClassStable, 0, arr{N2, catalog.ScheduledOn, whole}),
		mline(P2, 2, mid, interval.ClassTransient, 2, arr{N1, catalog.ScheduledOn, mid}),
		mline(C3, 3, whole, interval.ClassStable, 0, arr{P3, catalog.PartOf, whole}),
		mline(C2, 3, mid, interval.ClassTransient, 2, arr{P2, catalog.PartOf, mid}))
	expectReads(t, h.Reads, 4, 9, 15, 15)
	for _, s := range res.Substrates {
		if s.Expanded && (s.Impact.Token != res.Token || s.Impact.Seed != s.Entity) {
			t.Errorf("%s: token %d, seed %s", s.Entity, s.Impact.Token, s.Impact.Seed)
		}
	}
	// A rack and a zone are listed and not walked: their Impact is the zero
	// Result, and no read was made from them.
	for _, fp := range []identity.Fingerprint{R1, Z1} {
		if s := substrate(t, res, fp); s.Expanded || fmt.Sprintf("%+v", s.Impact) != fmt.Sprintf("%+v", blastradius.Result{}) {
			t.Errorf("%s: expanded %t, impact %+v", fp, s.Expanded, s.Impact)
		}
	}
	expectReads(t, res.Reads, 9, 15, 27, 27)

	// The option walks them too, each as Impact walks it, over the whole window.
	res = around(t, st, P1, w30, blastradius.Options{ExpandFabric: true})
	expect(t, "substrates with the option", sbriefs(res.Substrates),
		sline(N1, 1, whole, true, arr{P1, catalog.ScheduledOn, whole}),
		sline(H, 2, whole, true, arr{N1, catalog.RunsOn, whole}),
		sline(R1, 3, whole, true, arr{H, catalog.LocatedIn, whole}),
		sline(Z1, 3, whole, true, arr{H, catalog.LocatedIn, whole}))
	for _, fp := range []identity.Fingerprint{R1, Z1} {
		s := substrate(t, res, fp)
		expect(t, fmt.Sprintf("Impact(%s)", fp), briefs(s.Impact.Members),
			mline(H, 1, whole, interval.ClassStable, 0, arr{fp, catalog.LocatedIn, whole}),
			mline(N1, 2, whole, interval.ClassStable, 0, arr{H, catalog.RunsOn, whole}),
			mline(N2, 2, whole, interval.ClassStable, 0, arr{H, catalog.RunsOn, whole}),
			mline(P3, 3, whole, interval.ClassStable, 0, arr{N2, catalog.ScheduledOn, whole}),
			mline(P2, 3, mid, interval.ClassTransient, 2, arr{N1, catalog.ScheduledOn, mid}),
			mline(C3, 4, whole, interval.ClassStable, 0, arr{P3, catalog.PartOf, whole}),
			mline(C2, 4, mid, interval.ClassTransient, 2, arr{P2, catalog.PartOf, mid}))
		expectReads(t, s.Impact.Reads, 6, 11, 20, 20)
	}
	expectReads(t, res.Reads, 21, 37, 67, 67)

	// Impact ignores the option.
	if a, b := impact(t, st, H, w30, blastradius.Options{}), impact(t, st, H, w30, blastradius.Options{ExpandFabric: true}); sameResult(a, b) != "" || a.Reads != b.Reads {
		t.Errorf("Impact differs with the option: %s", sameResult(a, b))
	}
}

func TestARBNodeOnTwoHosts(t *testing.T) {
	st := scenarioAR(t)
	newFeed(t, st).write(observe(ent(H2), k8s, hm(9, 0), 0), since(runsOn(N1, H2), hm(10, 12)))
	res := around(t, st, P1, w30, blastradius.Options{})
	first := span(hm(10, 0), hm(10, 12))
	expect(t, "substrates", sbriefs(res.Substrates),
		sline(N1, 1, whole, true, arr{P1, catalog.ScheduledOn, whole}),
		sline(H, 2, first, true, arr{N1, catalog.RunsOn, first}),
		sline(R1, 3, first, false, arr{H, catalog.LocatedIn, first}),
		sline(Z1, 3, first, false, arr{H, catalog.LocatedIn, first}))
	expect(t, "unattributed", describeUnattributed(res.Unattributed),
		uline(N1, catalog.RunsOn, catalog.Host, []identity.Fingerprint{H, H2}, sp(hm(10, 12), hm(10, 30))))
	// The host walk starts over [10:00, 10:12): the node's time on the other
	// host lies outside it, so it is not unattributed there.
	h := substrate(t, res, H).Impact
	expect(t, "Impact(H)", briefs(h.Members),
		mline(N1, 1, first, interval.ClassDisappeared, 1, arr{H, catalog.RunsOn, first}),
		mline(N2, 1, first, interval.ClassDisappeared, 1, arr{H, catalog.RunsOn, first}),
		mline(P3, 2, first, interval.ClassDisappeared, 1, arr{N2, catalog.ScheduledOn, first}),
		mline(P2, 2, span(hm(10, 10), hm(10, 12)), interval.ClassTransient, 2, arr{N1, catalog.ScheduledOn, span(hm(10, 10), hm(10, 12))}),
		mline(C3, 3, first, interval.ClassDisappeared, 1, arr{P3, catalog.PartOf, first}),
		mline(C2, 3, span(hm(10, 10), hm(10, 12)), interval.ClassTransient, 2, arr{P2, catalog.PartOf, span(hm(10, 10), hm(10, 12))}))
	expect(t, "Impact(H) unattributed", describeUnattributed(h.Unattributed))
	// The upward walk reads P1 and N1; N1, H and H2; then H, R1 and Z1. The
	// other host is no substrate, so it is not in the frontier.
	expectReads(t, upwardReads(res), 3, 4, 8, 8)
	for _, s := range res.Substrates {
		if s.Entity == H2 {
			t.Errorf("%s is a substrate", H2)
		}
	}
}

func TestARCPodOnTwoNodes(t *testing.T) {
	st, f := world(t, memstore.Options{}, H, N1, N2, P1)
	f.write(since(runsOn(N1, H), hm(9, 0)), since(runsOn(N2, H), hm(9, 0)),
		since(sched(P1, N1), hm(9, 0)),
		observe(sched(P1, N2), "k8scluster/default", hm(10, 10), 0), gone2(sched(P1, N2), "k8scluster/default", hm(10, 20)))
	res := around(t, st, P1, w30, blastradius.Options{})
	expect(t, "substrates", sbriefs(res.Substrates),
		sline(N1, 1, whole, true, arr{P1, catalog.ScheduledOn, whole}),
		sline(N2, 1, mid, true, arr{P1, catalog.ScheduledOn, mid}),
		sline(H, 2, whole, true, arr{N1, catalog.RunsOn, whole}, arr{N2, catalog.RunsOn, mid}))
	expect(t, "unattributed", describeUnattributed(res.Unattributed))
	// Under each node, the pod is excluded and nothing else is on it; under the
	// host, the two nodes.
	expect(t, "Impact(N1)", briefs(res.Substrates[0].Impact.Members))
	expect(t, "Impact(N2)", briefs(res.Substrates[1].Impact.Members))
	expect(t, "Impact(H)", briefs(res.Substrates[2].Impact.Members),
		mline(N1, 1, whole, interval.ClassStable, 0, arr{H, catalog.RunsOn, whole}),
		mline(N2, 1, whole, interval.ClassStable, 0, arr{H, catalog.RunsOn, whole}))
	expectReads(t, res.Substrates[0].Impact.Reads, 1, 1, 2, 2)
	expectReads(t, res.Substrates[1].Impact.Reads, 1, 1, 2, 2)
	expectReads(t, res.Substrates[2].Impact.Reads, 3, 7, 9, 9)
	expectReads(t, res.Reads, 8, 15, 19, 19)
}

// gone2 is a Delete of an edge by a given producer.
func gone2(sub store.Subject, p lifecycle.Producer, at time.Time) store.Record {
	return deleted(sub, p, at)
}

func TestARDNoLocatedIn(t *testing.T) {
	st := dataA(t)
	res := around(t, st, P1, w30, blastradius.Options{})
	expect(t, "substrates", sbriefs(res.Substrates),
		sline(N1, 1, whole, true, arr{P1, catalog.ScheduledOn, whole}),
		sline(H, 2, whole, true, arr{N1, catalog.RunsOn, whole}))
	// Three calls: the host has a forward edge layer, so its read is made, and
	// it finds no edge. The pod and the node, then the node and the host.
	expectReads(t, upwardReads(res), 3, 4, 4, 4)
}

func TestAREQuarantine(t *testing.T) {
	boot := func(h identity.Fingerprint, id string, at time.Time, ttl time.Duration) store.Record {
		r := observe(ent(h), agent, at, ttl)
		r.Boot = id
		return r
	}
	t.Run("a quarantined host", func(t *testing.T) {
		st, f := world(t, memstore.Options{Policy: storetest.QuarantinePolicy()}, N1, P1)
		f.write(boot(H, "b1", hm(9, 0), 0),
			boot(H2, "b1", hm(9, 0), 15*time.Minute), boot(H2, "b2", hm(9, 5), 15*time.Minute), boot(H2, "b1", hm(9, 10), 15*time.Minute),
			since(runsOn(N1, H), hm(9, 0)), since(runsOn(N1, H2), hm(10, 10)), since(sched(P1, N1), hm(9, 0)))
		res := around(t, st, P1, w30, blastradius.Options{})
		first := span(hm(10, 0), hm(10, 10))
		// The second host is quarantined: its claim is listed, never followed,
		// and still blocks the first host from 10:10.
		expect(t, "substrates", sbriefs(res.Substrates),
			sline(N1, 1, whole, true, arr{P1, catalog.ScheduledOn, whole}),
			sline(H, 2, first, true, arr{N1, catalog.RunsOn, first}))
		expect(t, "unattributed", describeUnattributed(res.Unattributed),
			uline(N1, catalog.RunsOn, catalog.Host, []identity.Fingerprint{H, H2}, sp(hm(10, 10), hm(10, 30))))
		expect(t, "quarantined", quarantinedSubjects(res.Quarantined), quarantinedSubjects([]interval.Element{{Subject: runsOn(N1, H2)}})...)
		// The upward walk reads P1 and N1; then N1, H and H2, which is
		// quarantined and has no EntityWindow; then the host's edges, none.
		expectReads(t, upwardReads(res), 3, 4, 5, 4)
		h := substrate(t, res, H).Impact
		expect(t, "Impact(H)", briefs(h.Members),
			mline(N1, 1, first, interval.ClassDisappeared, 1, arr{H, catalog.RunsOn, first}))
		expect(t, "Impact(H) quarantined", quarantinedSubjects(h.Quarantined), quarantinedSubjects([]interval.Element{{Subject: runsOn(N1, H2)}})...)
	})
	t.Run("the only host is quarantined", func(t *testing.T) {
		st, f := world(t, memstore.Options{Policy: storetest.QuarantinePolicy()}, N1, P1)
		f.write(boot(H, "b1", hm(9, 0), 15*time.Minute), boot(H, "b2", hm(9, 5), 15*time.Minute), boot(H, "b1", hm(9, 10), 15*time.Minute),
			since(runsOn(N1, H), hm(9, 0)), since(sched(P1, N1), hm(9, 0)))
		res := around(t, st, P1, w30, blastradius.Options{})
		expect(t, "substrates", sbriefs(res.Substrates), sline(N1, 1, whole, true, arr{P1, catalog.ScheduledOn, whole}))
		expect(t, "quarantined", quarantinedSubjects(res.Quarantined), quarantinedSubjects([]interval.Element{{Subject: runsOn(N1, H)}})...)
		// P1 and N1, then N1 and H (H quarantined: no EntityWindow); the host is
		// not reached, so its edges are not read: two calls.
		expectReads(t, upwardReads(res), 2, 3, 4, 3)
		n := substrate(t, res, N1).Impact
		expect(t, "Impact(N1)", briefs(n.Members))
		expectReads(t, n.Reads, 1, 1, 2, 2)
	})
	// A seed pod the store reports quarantined; none can be today (only host
	// records may carry a boot id); tested through a wrapper. Such a seed reaches
	// nothing, as a quarantined seed does in Impact (scenario D), and its
	// placement edge is listed.
	t.Run("a quarantined seed pod", func(t *testing.T) {
		inner, f := world(t, memstore.Options{}, N1, H, P1)
		f.write(since(runsOn(N1, H), hm(9, 0)), since(sched(P1, N1), hm(9, 0)))
		st := &quarantiningStore{Store: inner, who: P1}
		res, err := blastradius.NewWalker(st).Around(ctx, P1, w30, blastradius.Options{})
		if err != nil {
			t.Fatal(err)
		}
		expect(t, "substrates", sbriefs(res.Substrates))
		expect(t, "quarantined", quarantinedSubjects(res.Quarantined), quarantinedSubjects([]interval.Element{{Subject: sched(P1, N1)}})...)
		if len(res.Quarantined) != 1 || !errors.Is(res.Quarantined[0].Uncascaded, store.ErrQuarantined) {
			t.Errorf("Quarantined = %v", quarantinedSubjects(res.Quarantined))
		}
		expectReads(t, res.Reads, 1, 1, 2, 1)
	})
}

// quarantiningStore reports one entity quarantined, as a store whose policy
// tracks the entity's clones would.
type quarantiningStore struct {
	store.Store
	who identity.Fingerprint
}

func (q *quarantiningStore) Alive(ctx context.Context, fp identity.Fingerprint, t time.Time, sc store.Scope) (bool, error) {
	if fp == q.who {
		return false, &store.QuarantineError{Entity: fp, Layer: sc.Layer}
	}
	return q.Store.Alive(ctx, fp, t, sc)
}

// scenarioARF is F's shape: N1 on H, no located_in. P1 (the seed) is on N1 over
// [10:10, 10:20), P2 over the whole window, P3 over [10:00, 10:05) only. Every
// pod has one container.
func scenarioARF(t *testing.T) *memstore.Store {
	st, f := world(t, memstore.Options{}, H, N1, P1, P2, P3, C1, C2, C3)
	f.write(since(runsOn(N1, H), hm(9, 0)),
		since(sched(P1, N1), hm(10, 10)), gone(sched(P1, N1), hm(10, 20)),
		since(sched(P2, N1), hm(9, 0)),
		since(sched(P3, N1), hm(9, 0)), gone(sched(P3, N1), hm(10, 5)),
		since(partOf(C1, P1), hm(9, 0)), since(partOf(C2, P2), hm(9, 0)), since(partOf(C3, P3), hm(9, 0)))
	return st
}

func TestARFTheRestriction(t *testing.T) {
	st := scenarioARF(t)
	res := around(t, st, P1, w30, blastradius.Options{})
	expect(t, "substrates", sbriefs(res.Substrates),
		sline(N1, 1, mid, true, arr{P1, catalog.ScheduledOn, mid}),
		sline(H, 2, mid, true, arr{N1, catalog.RunsOn, mid}))
	// The other pod is a member for the pod's time on the node only, and the
	// pod that left before it came is never expanded.
	n := substrate(t, res, N1).Impact
	expect(t, "Impact(N1)", briefs(n.Members),
		mline(P2, 1, mid, interval.ClassTransient, 2, arr{N1, catalog.ScheduledOn, mid}),
		mline(C2, 2, mid, interval.ClassTransient, 2, arr{P2, catalog.PartOf, mid}))
	h := substrate(t, res, H).Impact
	expect(t, "Impact(H)", briefs(h.Members),
		mline(N1, 1, mid, interval.ClassTransient, 2, arr{H, catalog.RunsOn, mid}),
		mline(P2, 2, mid, interval.ClassTransient, 2, arr{N1, catalog.ScheduledOn, mid}),
		mline(C2, 3, mid, interval.ClassTransient, 2, arr{P2, catalog.PartOf, mid}))
	// The upward walk reads P1 and N1, then N1 and H, then H, which has no
	// edge. The node's reverse read reads N1, P1, P2 and P3, then P2's reads P2
	// and its container: P3 has no Window of its own and its container is not
	// read. A walk over the whole window cut afterwards would expand P3 too.
	expectReads(t, n.Reads, 2, 2, 6, 6)
	expectReads(t, h.Reads, 4, 5, 10, 10)
	expectReads(t, res.Reads, 9, 11, 20, 20)
}

func TestARGNothing(t *testing.T) {
	inner := dataA(t)
	st := &spy{Store: inner}
	w := blastradius.NewWalker(st)
	res, err := w.Around(ctx, P2, win(hm(10, 0), hm(10, 5)), blastradius.Options{})
	if err != nil {
		t.Fatal(err)
	}
	// P2 is scheduled only from 10:10: it has no substrate in the window.
	expect(t, "substrates", sbriefs(res.Substrates))
	expectReads(t, res.Reads, 1, 1, 0, 0)
	if res.Token != inner.LastSeq() {
		t.Errorf("token %d, want %d", res.Token, inner.LastSeq())
	}

	unknown, err := identity.FingerprintFromHash("x.unknown", [identity.FingerprintBytes]byte{1})
	if err != nil {
		t.Fatal(err)
	}
	st.calls = 0
	pol := func(f func(*blastradius.Policy)) blastradius.Options {
		p := blastradius.DefaultPolicy()
		f(&p)
		return blastradius.Options{Policy: p}
	}
	for _, tc := range []struct {
		name string
		seed identity.Fingerprint
		w    interval.Window
		o    blastradius.Options
	}{
		{"From == To", P1, win(hm(10, 0), hm(10, 0)), blastradius.Options{}},
		{"From > To", P1, win(hm(10, 1), hm(10, 0)), blastradius.Options{}},
		{"zero seed", identity.Fingerprint{}, w30, blastradius.Options{}},
		{"unknown type", unknown, w30, blastradius.Options{}},
		{"a host", H, w30, blastradius.Options{}},
		{"a node", N1, w30, blastradius.Options{}},
		{"a container", C1, w30, blastradius.Options{}},
		{"a service", S1, w30, blastradius.Options{}},
		{"MaxDepth 0", P1, w30, pol(func(p *blastradius.Policy) { p.MaxDepth = 0 })},
		{"MaxFrontier 0", P1, w30, pol(func(p *blastradius.Policy) { p.MaxFrontier = 0 })},
		{"MaxMembers -1", P1, w30, pol(func(p *blastradius.Policy) { p.MaxMembers = -1 })},
		{"a partial policy", P1, w30, blastradius.Options{Policy: blastradius.Policy{MaxDepth: 3}}},
	} {
		res, err := w.Around(ctx, tc.seed, tc.w, tc.o)
		if !errors.Is(err, store.ErrInvalid) || res.Token != 0 || res.Seed != (identity.Fingerprint{}) || res.Substrates != nil ||
			!strings.HasPrefix(err.Error(), "blastradius: Around: ") {
			t.Errorf("%s: %v", tc.name, err)
		}
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := w.Around(cancelled, P1, w30, blastradius.Options{}); !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled: %v", err)
	}
	if st.calls != 0 {
		t.Errorf("%d store calls for invalid arguments or a cancelled context", st.calls)
	}
	if err := inner.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Around(ctx, P1, w30, blastradius.Options{}); !errors.Is(err, store.ErrClosed) {
		t.Errorf("closed: %v", err)
	}
}

func TestARHOneToken(t *testing.T) {
	inner := scenarioAR(t)
	want := around(t, inner, P1, w30, blastradius.Options{})
	before := inner.LastSeq()
	late := newFeed(t, inner)
	st := &spy{Store: inner, onWindow: func(n int) {
		// The upward walk makes three Window reads, so the fourth is the first
		// of the first substrate's walk.
		if n == 4 {
			late.write(gone(sched(P2, N1), hm(10, 1)))
		}
	}}
	res := around(t, st, P1, w30, blastradius.Options{})
	if inner.LastSeq() != before+1 {
		t.Fatal("the hook did not write")
	}
	if d := sameAround(res, want); d != "" || res.Token != before {
		t.Errorf("token %d, want %d: %s", res.Token, before, d)
	}
	for _, s := range res.Substrates {
		if s.Expanded && s.Impact.Token != res.Token {
			t.Errorf("Impact(%s) at token %d, want %d", s.Entity, s.Impact.Token, res.Token)
		}
	}
	for _, tok := range st.tokens {
		if tok != before {
			t.Fatalf("a read at token %d, want %d: %v", tok, before, st.tokens)
		}
	}
	if st.writes != 0 {
		t.Errorf("%d writes through the walker's store", st.writes)
	}
}

func TestARIRetention(t *testing.T) {
	retained := func(t *testing.T, res blastradius.AroundResult, err error) {
		t.Helper()
		if !errors.Is(err, interval.ErrRetained) || !errors.Is(err, errors.ErrUnsupported) || res.Token != 0 || res.Substrates != nil || res.Seed != (identity.Fingerprint{}) {
			t.Fatalf("%v, %+v; want ErrRetained with a zero result", err, res)
		}
	}
	p1 := observe(ent(P1), k8s, hm(9, 0), 0)
	t.Run("L1 retained", func(t *testing.T) {
		st := scenarioA(t, memstore.Options{Keep: [4]bool{true, false, true, true}}, p1)
		if err := st.Retain(ctx, hm(10, 0)); err != nil {
			t.Fatal(err)
		}
		res, err := blastradius.NewWalker(st).Around(ctx, P1, w30, blastradius.Options{})
		retained(t, res, err)
	})
	t.Run("L2 retained", func(t *testing.T) {
		st := scenarioA(t, memstore.Options{Keep: [4]bool{true, true, false, true}}, p1)
		if err := st.Retain(ctx, hm(10, 0)); err != nil {
			t.Fatal(err)
		}
		reads := &spy{Store: st}
		res, err := blastradius.NewWalker(reads).Around(ctx, P1, w30, blastradius.Options{})
		retained(t, res, err)
		if reads.windows != 0 {
			t.Errorf("%d Window reads, want none: the first call is refused before its first read", reads.windows)
		}
	})
}

func TestARLTheReferenceCell(t *testing.T) {
	nth := func(typ catalog.EntityType, i int) identity.Fingerprint {
		var h [identity.FingerprintBytes]byte
		h[0], h[1] = byte(i>>8), byte(i)
		fp, err := identity.FingerprintFromHash(typ, h)
		if err != nil {
			t.Fatal(err)
		}
		return fp
	}
	N := node(1)
	st, f := world(t, memstore.Options{}, H, N, R1, Z1)
	f.write(since(runsOn(N, H), hm(9, 0)), since(locatedIn(H, R1), hm(9, 0)), since(locatedIn(H, Z1), hm(9, 0)))
	for i := 1; i <= blastradius.BudgetReferencePods; i++ {
		p, c := nth(catalog.K8sPod, i), nth(catalog.Container, i)
		f.write(observe(ent(p), k8s, hm(9, 0), 0), observe(ent(c), k8s, hm(9, 0), 0),
			since(sched(p, N), hm(9, 0)), since(partOf(c, p), hm(9, 0)))
	}
	for i := range 40 {
		p := nth(catalog.K8sPod, 300+i)
		f.write(observe(ent(p), k8s, hm(7, 0), 0), since(sched(p, N), hm(8, 0)), gone(sched(p, N), hm(9, i)))
	}
	seed := nth(catalog.K8sPod, 7)

	res := around(t, st, seed, w30, blastradius.Options{})
	n, h := substrate(t, res, N).Impact, substrate(t, res, H).Impact
	if len(n.Members) != 2*(blastradius.BudgetReferencePods-1) || len(h.Members) != 1+2*(blastradius.BudgetReferencePods-1) {
		t.Errorf("%d members under the node, %d under the host", len(n.Members), len(h.Members))
	}
	for _, m := range append(slices.Clone(n.Members), h.Members...) {
		if m.Entity == seed || m.Entity == nth(catalog.Container, 7) || !sameSpans(m.Reach, whole) {
			t.Fatalf("%s", brief(m))
		}
	}
	// Listing the fabric is free, and the default reads are the upward walk's
	// plus the node's and the host's.
	expectReads(t, n.Reads, 2, 110, 329, 329)
	expectReads(t, h.Reads, 4, 113, 333, 333)
	expectReads(t, res.Reads, 9, 227, 669, 669)
	if res.Reads.Alive > blastradius.BudgetAroundExistence || res.Reads.Windows > blastradius.BudgetAroundWindows {
		t.Errorf("reads %+v over the budget", res.Reads)
	}
	if len(res.Substrates) != 4 {
		t.Errorf("%v", sbriefs(res.Substrates))
	}

	res = around(t, st, seed, w30, blastradius.Options{ExpandFabric: true})
	for _, fp := range []identity.Fingerprint{R1, Z1} {
		expectReads(t, substrate(t, res, fp).Impact.Reads, 6, 115, 338, 338)
	}
	expectReads(t, res.Reads, 21, 457, 1345, 1345)
	if res.Reads.Alive > blastradius.BudgetAroundExpandedExistence || res.Reads.Windows > blastradius.BudgetAroundExpandedWindows {
		t.Errorf("reads %+v over the budget", res.Reads)
	}

	// Impact from the host is what scenario L fixes.
	expectReads(t, impact(t, st, H, w30, blastradius.Options{}).Reads, 4, 114, 335, 335)
}

// checkAround checks an answer against the per-instant oracle: it equals the
// oracle's exactly, the reads equal the budget's formula, and nothing is
// written.
func checkAround(t tb, st store.Store, or *oracle, tok uint64, pod identity.Fingerprint, w interval.Window, depth int, expand bool) blastradius.AroundResult {
	t.Helper()
	sp := &spy{Store: st}
	got, err := blastradius.NewWalker(sp).Around(ctx, pod, w, blastradius.Options{Policy: wide(depth), ExpandFabric: expand})
	if err != nil {
		t.Fatal(err)
	}
	want, reads := or.around(pod, depth, expand)
	want.Token = tok
	want.Reads = reads
	for i := range want.Substrates {
		if want.Substrates[i].Expanded {
			want.Substrates[i].Impact.Token = tok
		}
	}
	if d := sameAround(got, want); d != "" {
		t.Fatalf("pod %s, window [%s, %s), token %d, depth %d, expand %t, against the oracle: %s", pod, ts(w.From), ts(w.To), tok, depth, expand, d)
	}
	if sp.writes != 0 {
		t.Fatalf("%d writes", sp.writes)
	}
	return got
}

func TestAroundAgainstTheOracle(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		c := drawCase(t)
		or, tok := oracleFor(t, c.st, c.recs, c.w)
		expand := rapid.Bool().Draw(t, "expand")
		for _, pod := range c.g.pods {
			checkAround(t, c.st, or, tok, pod, c.w, c.depth, expand)
		}
	})
}

// untruncated says a result lost nothing to a cap or to the depth bound.
func untruncated(res blastradius.Result) bool {
	return len(res.Truncated.Depth) == 0 && len(res.Truncated.Frontier) == 0 && res.Truncated.Members == 0
}

// TestAroundSubstrateTimeAndMemberReach checks that the pod's time on a
// substrate is the pod's reach in Impact from it, whenever that is untruncated,
// and that every member's reach is the union of its arrivals, lies within the
// pod's time on the substrate, and is the member's reach in Impact from the
// substrate cut to that time, for the members the pod's own containers cannot
// be.
func TestAroundSubstrateTimeAndMemberReach(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		c := drawCase(t)
		for _, pod := range c.g.pods {
			res, err := blastradius.NewWalker(c.st).Around(ctx, pod, c.w, blastradius.Options{Policy: wide(c.depth), ExpandFabric: true})
			if err != nil {
				t.Fatal(err)
			}
			w := c.w
			w.AsOf = res.Token
			for _, s := range res.Substrates {
				full := mustImpact(t, c.st, s.Entity, w, c.options())
				if untruncated(full) {
					if got := reachOf(full)[pod]; !sameSpans(got, s.On) {
						t.Fatalf("pod %s on %s: On %s, its reach in Impact is %s", pod, s.Entity, spansString(s.On), spansString(got))
					}
				}
				fullReach := reachOf(full)
				for _, m := range s.Impact.Members {
					var all [][]interval.Span
					for _, a := range m.Arrivals {
						all = append(all, a.Spans)
					}
					if !sameSpans(m.Reach, testUnion(all...)) || !within(m.Reach, s.On) {
						t.Fatalf("pod %s on %s: %s, outside the arrivals or the pod's time", pod, s.Entity, describe(m))
					}
					cut := interval.Intersect(fullReach[m.Entity], s.On)
					if !within(m.Reach, cut) {
						t.Fatalf("pod %s on %s: %s is reached over %s, more than Impact's %s cut to the pod's time", pod, s.Entity, m.Entity, spansString(m.Reach), spansString(cut))
					}
					if typ := m.Entity.Type(); (typ == catalog.K8sNode || typ == catalog.Host || typ == catalog.K8sPod) && !sameSpans(m.Reach, cut) {
						t.Fatalf("pod %s on %s: %s is reached over %s, Impact's cut to the pod's time is %s", pod, s.Entity, m.Entity, spansString(m.Reach), spansString(cut))
					}
					if m.Entity == pod {
						t.Fatalf("the pod %s is a member under %s", pod, s.Entity)
					}
				}
				if len(s.On) == 0 {
					t.Fatalf("%s has no time", s.Entity)
				}
			}
		}
	})
}
