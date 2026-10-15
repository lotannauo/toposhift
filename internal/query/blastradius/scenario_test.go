package blastradius_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/blastradius"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

// The scenarios fix exact answers, written before the code. Times are on
// 2026-01-01 UTC, written hh:mm; every entity is observed at 09:00 with no end,
// and the window is [10:00, 10:30) with the default policy, unless a scenario
// says otherwise.

func hm(h, m int) time.Time { return time.Date(2026, 1, 1, h, m, 0, 0, time.UTC) }

func win(from, to time.Time) interval.Window {
	return interval.Window{From: from, To: to, AsOf: store.Latest}
}

var w30 = win(hm(10, 0), hm(10, 30))

func sp(a, b time.Time) interval.Span { return interval.Span{Start: a, End: b} }

// impact walks from seed through a spy, and fails on an error or on any write:
// nothing derived is written.
func impact(t *testing.T, st store.Store, seed identity.Fingerprint, w interval.Window, o blastradius.Options) blastradius.Result {
	t.Helper()
	sp := &spy{Store: st}
	res, err := blastradius.NewWalker(sp).Impact(ctx, seed, w, o)
	if err != nil {
		t.Fatal(err)
	}
	if sp.writes != 0 {
		t.Fatalf("%d writes or retentions through the walker's store", sp.writes)
	}
	return res
}

func expect(t *testing.T, what string, got []string, want ...string) {
	t.Helper()
	if want == nil {
		want = []string{}
	}
	if d := diffLines(what, got, want); d != "" {
		t.Error(d)
	}
}

func expectReads(t *testing.T, got blastradius.Reads, calls, windows, alive, ew int) {
	t.Helper()
	if want := (blastradius.Reads{Calls: calls, Windows: windows, Alive: alive, EntityWindows: ew}); got != want {
		t.Errorf("reads %+v, want %+v", got, want)
	}
}

// arr is an expected arrival: the parent, the relation and the spans.
type arr struct {
	parent identity.Fingerprint
	rel    catalog.RelationType
	spans  []interval.Span
}

// brief is a member without its legs' pieces: entity, depth, reach, class and
// arrivals.
func brief(m blastradius.Member) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s depth=%d reach=%s %s %s transitions=%d", m.Entity, m.Depth, spansString(m.Reach), m.Duration, m.Class, m.Transitions)
	for _, a := range m.Arrivals {
		fmt.Fprintf(&b, " <%s %s %s %s leg=%s>%s>", a.Parent, a.Relation, a.Propagation, spansString(a.Spans), a.Leg.Subject.A, a.Leg.Subject.B)
	}
	return b.String()
}

func briefs(ms []blastradius.Member) []string {
	out := make([]string, len(ms))
	for i, m := range ms {
		out[i] = brief(m)
	}
	return out
}

// mline is the brief line of a member reached over reach through arrivals.
func mline(fp identity.Fingerprint, depth int, reach []interval.Span, class interval.Class, transitions int, arrivals ...arr) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s depth=%d reach=%s %s %s transitions=%d", fp, depth, spansString(reach), duration(reach), class, transitions)
	for _, a := range arrivals {
		rel, _ := catalog.Default().Relation(a.rel)
		fmt.Fprintf(&b, " <%s %s %s %s leg=%s>%s>", a.parent, a.rel, rel.Propagation(), spansString(a.spans), fp, a.parent)
	}
	return b.String()
}

func span(a, b time.Time) []interval.Span { return []interval.Span{sp(a, b)} }

var whole = span(hm(10, 0), hm(10, 30))

func uline(subject identity.Fingerprint, rel catalog.RelationType, target catalog.EntityType, cands []identity.Fingerprint, ss ...interval.Span) string {
	return fmt.Sprintf("%s %s %s %s %v", subject, rel, target, spansString(ss), cands)
}

// world is a store with entities observed at 09:00 with no end.
func world(t *testing.T, opts memstore.Options, fps ...identity.Fingerprint) (*memstore.Store, *feed) {
	st := openStore(t, opts)
	f := newFeed(t, st)
	var rs []store.Record
	for _, fp := range fps {
		rs = append(rs, observe(ent(fp), k8s, hm(9, 0), 0))
	}
	if len(rs) > 0 {
		f.write(rs...)
	}
	return st, f
}

// since is an edge observed by its relation's producer from at, with no end.
func since(sub store.Subject, at time.Time) store.Record {
	return observe(sub, producerOf(sub.Relation), at, 0)
}

// gone is a Delete of an edge by its relation's producer.
func gone(sub store.Subject, at time.Time) store.Record {
	return deleted(sub, producerOf(sub.Relation), at)
}

var (
	H, H2      = host(1), host(2)
	N1, N2     = node(1), node(2)
	P1, P2     = pod(1), pod(2)
	C1         = container(1)
	S1, S2, S3 = service(1), service(2), service(3)
)

// scenarioA writes a host, two nodes on it, a pod on each (the second only
// over [10:10, 10:20)) and a container of the first pod. p1 is the first pod's
// entity record.
func scenarioA(t *testing.T, opts memstore.Options, p1 store.Record) *memstore.Store {
	st, f := world(t, opts, H, N1, N2, P2, C1)
	f.write(p1,
		since(runsOn(N1, H), hm(9, 0)), since(runsOn(N2, H), hm(9, 0)),
		since(sched(P1, N1), hm(9, 0)),
		since(sched(P2, N2), hm(10, 10)), gone(sched(P2, N2), hm(10, 20)),
		since(partOf(C1, P1), hm(9, 0)))
	return st
}

func dataA(t *testing.T) *memstore.Store {
	return scenarioA(t, memstore.Options{}, observe(ent(P1), k8s, hm(9, 0), 0))
}

func arrivalOf(t *testing.T, res blastradius.Result, y, parent identity.Fingerprint) blastradius.Arrival {
	t.Helper()
	for _, m := range res.Members {
		if m.Entity != y {
			continue
		}
		for _, a := range m.Arrivals {
			if a.Parent == parent {
				return a
			}
		}
	}
	t.Fatalf("no arrival of %s from %s in %v", y, parent, briefs(res.Members))
	return blastradius.Arrival{}
}

// expectLegEnd checks that the leg of y's arrival from parent has one piece,
// ending at end by src.
func expectLegEnd(t *testing.T, res blastradius.Result, y, parent identity.Fingerprint, end time.Time, src lifecycle.EndSource) {
	t.Helper()
	leg := arrivalOf(t, res, y, parent).Leg
	if len(leg.Pieces) != 1 || !leg.Pieces[0].End.Equal(end) || leg.Pieces[0].EndSource != src {
		t.Errorf("the leg %s: %s, want one piece ending at %s by %s", y, describeLeg(leg), ts(end), src)
	}
}

func TestScenarioAHostSubstrate(t *testing.T) {
	st := dataA(t)
	res := impact(t, st, H, w30, blastradius.Options{})
	expect(t, "Impact(H)", briefs(res.Members),
		mline(N1, 1, whole, interval.ClassStable, 0, arr{H, catalog.RunsOn, whole}),
		mline(N2, 1, whole, interval.ClassStable, 0, arr{H, catalog.RunsOn, whole}),
		mline(P1, 2, whole, interval.ClassStable, 0, arr{N1, catalog.ScheduledOn, whole}),
		mline(P2, 2, span(hm(10, 10), hm(10, 20)), interval.ClassTransient, 2, arr{N2, catalog.ScheduledOn, span(hm(10, 10), hm(10, 20))}),
		mline(C1, 3, whole, interval.ClassStable, 0, arr{P1, catalog.PartOf, whole}),
	)
	expectLegEnd(t, res, P2, N2, hm(10, 20), lifecycle.EndProducer)
	if ps := arrivalOf(t, res, P2, N2).Leg.Pieces[0].Producers; !slices.Equal(ps, []lifecycle.Producer{k8s}) {
		t.Errorf("P2's leg producers %v", ps)
	}
	expect(t, "unattributed", describeUnattributed(res.Unattributed))
	expect(t, "quarantined", quarantinedSubjects(res.Quarantined))
	if fmt.Sprint(res.Truncated) != fmt.Sprint(blastradius.Truncation{}) {
		t.Errorf("truncated %+v", res.Truncated)
	}
	expectReads(t, res.Reads, 4, 9, 12, 12)
	if res.Token != st.LastSeq() || res.Seed != H || res.PolicyVersion != 1 || res.IntervalPolicy != interval.PolicyVersion ||
		!res.From.Equal(hm(10, 0)) || !res.To.Equal(hm(10, 30)) {
		t.Errorf("header %+v", res)
	}
	if got := entities(blastradius.Intersection(res.Members)); !slices.Equal(got, []identity.Fingerprint{N1, N2, P1, C1}) {
		t.Errorf("Intersection = %v", got)
	}
	if got := entities(blastradius.At(res.Members, hm(10, 20))); !slices.Equal(got, []identity.Fingerprint{N1, N2, P1, C1}) {
		t.Errorf("At(10:20) = %v", got)
	}
	if got := entities(blastradius.At(res.Members, hm(10, 10))); len(got) != 5 {
		t.Errorf("At(10:10) = %v", got)
	}
	if got := blastradius.Union(res.Members); len(got) != 5 {
		t.Errorf("Union = %v", entities(got))
	}
	if _, err := blastradius.AtChecked(res, hm(10, 30)); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("AtChecked(To) = %v", err)
	}
	if _, err := blastradius.AtChecked(res, hm(10, 0).Add(-time.Nanosecond)); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("AtChecked(From - 1ns) = %v", err)
	}
	if got, err := blastradius.AtChecked(res, hm(10, 0)); err != nil || len(got) != 4 {
		t.Errorf("AtChecked(From) = %v, %v", entities(got), err)
	}
}

func TestScenarioBPathThatNeverExisted(t *testing.T) {
	N, P := node(1), pod(1)
	st, f := world(t, memstore.Options{}, H, N, P)
	f.write(since(runsOn(N, H), hm(9, 0)), gone(runsOn(N, H), hm(10, 10)), since(sched(P, N), hm(10, 15)))
	res := impact(t, st, H, w30, blastradius.Options{})
	expect(t, "Impact(H)", briefs(res.Members),
		mline(N, 1, span(hm(10, 0), hm(10, 10)), interval.ClassDisappeared, 1, arr{H, catalog.RunsOn, span(hm(10, 0), hm(10, 10))}))
	expectLegEnd(t, res, N, H, hm(10, 10), lifecycle.EndProducer)
	expectReads(t, res.Reads, 3, 4, 6, 6)
}

func TestScenarioCNodeBetweenTwoHosts(t *testing.T) {
	N, P := node(1), pod(1)
	st, f := world(t, memstore.Options{}, H, H2, N, P)
	f.write(observe(runsOn(N, H), agent, hm(9, 55), 20*time.Minute), since(runsOn(N, H2), hm(10, 10)), since(sched(P, N), hm(9, 0)))
	both := uline(N, catalog.RunsOn, catalog.Host, []identity.Fingerprint{H, H2}, sp(hm(10, 10), hm(10, 15)))

	res := impact(t, st, H, w30, blastradius.Options{})
	first := span(hm(10, 0), hm(10, 10))
	expect(t, "Impact(H)", briefs(res.Members),
		mline(N, 1, first, interval.ClassDisappeared, 1, arr{H, catalog.RunsOn, first}),
		mline(P, 2, first, interval.ClassDisappeared, 1, arr{N, catalog.ScheduledOn, first}))
	expect(t, "Impact(H) unattributed", describeUnattributed(res.Unattributed), both)
	expectReads(t, res.Reads, 4, 5, 7, 7)

	res = impact(t, st, H2, w30, blastradius.Options{})
	last := span(hm(10, 15), hm(10, 30))
	expect(t, "Impact(H2)", briefs(res.Members),
		mline(N, 1, last, interval.ClassAppeared, 1, arr{H2, catalog.RunsOn, last}),
		mline(P, 2, last, interval.ClassAppeared, 1, arr{N, catalog.ScheduledOn, last}))
	expect(t, "Impact(H2) unattributed", describeUnattributed(res.Unattributed), both)
	expectReads(t, res.Reads, 4, 5, 7, 7)
}

// TestScenarioCClaimOfAQuarantinedTarget is C with the second host quarantined:
// the claim still counts, and is never lifted through.
func TestScenarioCClaimOfAQuarantinedTarget(t *testing.T) {
	N, P := node(1), pod(1)
	st, f := world(t, memstore.Options{Policy: storetest.QuarantinePolicy()}, N, P)
	boot := func(h identity.Fingerprint, id string, at time.Time, ttl time.Duration) store.Record {
		r := observe(ent(h), agent, at, ttl)
		r.Boot = id
		return r
	}
	f.write(boot(H, "b1", hm(9, 0), 0),
		boot(H2, "b1", hm(9, 0), 15*time.Minute), boot(H2, "b2", hm(9, 5), 15*time.Minute), boot(H2, "b1", hm(9, 10), 15*time.Minute),
		since(runsOn(N, H), hm(9, 0)), since(runsOn(N, H2), hm(10, 10)), since(sched(P, N), hm(9, 0)))
	res := impact(t, st, H, w30, blastradius.Options{})
	first := span(hm(10, 0), hm(10, 10))
	expect(t, "Impact(H)", briefs(res.Members),
		mline(N, 1, first, interval.ClassDisappeared, 1, arr{H, catalog.RunsOn, first}),
		mline(P, 2, first, interval.ClassDisappeared, 1, arr{N, catalog.ScheduledOn, first}))
	expect(t, "unattributed", describeUnattributed(res.Unattributed),
		uline(N, catalog.RunsOn, catalog.Host, []identity.Fingerprint{H, H2}, sp(hm(10, 10), hm(10, 30))))
	expect(t, "quarantined", quarantinedSubjects(res.Quarantined), quarantinedSubjects([]interval.Element{{Subject: runsOn(N, H2)}})...)
	expectReads(t, res.Reads, 4, 5, 7, 6)

	res = impact(t, st, H2, w30, blastradius.Options{})
	expect(t, "Impact(H2)", briefs(res.Members))
	expect(t, "Impact(H2) quarantined", quarantinedSubjects(res.Quarantined), quarantinedSubjects([]interval.Element{{Subject: runsOn(N, H2)}})...)
	expectReads(t, res.Reads, 1, 1, 2, 1)
}

func TestScenarioDQuarantinedSeed(t *testing.T) {
	N := node(1)
	st, f := world(t, memstore.Options{Policy: storetest.QuarantinePolicy()}, N)
	boot := func(id string, at time.Time) store.Record {
		r := observe(ent(H), agent, at, 15*time.Minute)
		r.Boot = id
		return r
	}
	f.write(boot("b1", hm(9, 0)), boot("b2", hm(9, 5)), boot("b1", hm(9, 10)), since(runsOn(N, H), hm(9, 0)))
	res := impact(t, st, H, w30, blastradius.Options{})
	expect(t, "Impact(H)", briefs(res.Members))
	if len(res.Quarantined) != 1 || res.Quarantined[0].Subject != runsOn(N, H) || !errors.Is(res.Quarantined[0].Uncascaded, store.ErrQuarantined) {
		t.Errorf("Quarantined = %v", quarantinedSubjects(res.Quarantined))
	}
	expectReads(t, res.Reads, 1, 1, 2, 1)
}

func TestScenarioEDependsOnAndACycle(t *testing.T) {
	cycle := []store.Record{since(dependsOn(S2, S1), hm(9, 0)), since(dependsOn(S3, S2), hm(9, 0)), since(dependsOn(S1, S3), hm(9, 0))}
	want := []string{
		mline(S2, 1, whole, interval.ClassStable, 0, arr{S1, catalog.DependsOn, whole}),
		mline(S3, 2, whole, interval.ClassStable, 0, arr{S2, catalog.DependsOn, whole}),
	}
	t.Run("E", func(t *testing.T) {
		st, f := world(t, memstore.Options{}, S1, S2, S3)
		f.write(cycle...)
		res := impact(t, st, S1, w30, blastradius.Options{})
		expect(t, "Impact(S1)", briefs(res.Members), want...)
		if p := res.Members[0].Arrivals[0].Propagation; p != catalog.PropagateUp {
			t.Errorf("S2's arrival propagates %s", p)
		}
		expectReads(t, res.Reads, 3, 6, 6, 6)
	})
	t.Run("E′", func(t *testing.T) {
		SI := instance(1)
		st, f := world(t, memstore.Options{}, S1, S2, S3, SI)
		f.write(append(cycle, since(instanceOf(SI, S1), hm(9, 0)))...)
		res := impact(t, st, S1, w30, blastradius.Options{})
		expect(t, "Impact(S1)", briefs(res.Members), want...)
		expectReads(t, res.Reads, 3, 6, 7, 7)
	})
}

func TestScenarioFAnInstant(t *testing.T) {
	st := dataA(t)
	at := hm(10, 12)
	w := win(at, at.Add(time.Nanosecond))
	res := impact(t, st, H, w, blastradius.Options{})
	if got := entities(res.Members); !slices.Equal(got, []identity.Fingerprint{N1, N2, P1, P2, C1}) {
		t.Fatalf("members %v", got)
	}
	for _, m := range res.Members {
		if !sameSpans(m.Reach, span(at, at.Add(time.Nanosecond))) || m.Class != interval.ClassStable {
			t.Errorf("%s", brief(m))
		}
	}
}

func TestScenarioGNothingToReach(t *testing.T) {
	st := dataA(t)
	res := impact(t, st, P2, w30, blastradius.Options{})
	expect(t, "Impact(P2)", briefs(res.Members))
	expectReads(t, res.Reads, 1, 1, 0, 0)
	res = impact(t, st, C1, w30, blastradius.Options{})
	expect(t, "Impact(C1)", briefs(res.Members))
	expectReads(t, res.Reads, 1, 0, 0, 0)
	if res.Token != st.LastSeq() {
		t.Errorf("token %d, want %d", res.Token, st.LastSeq())
	}
}

func TestScenarioHTheTokenIsPinnedOnce(t *testing.T) {
	inner := dataA(t)
	want := impact(t, inner, H, w30, blastradius.Options{})
	before := inner.LastSeq()
	late := newFeed(t, inner)
	st := &spy{Store: inner, onWindow: func(n int) {
		// The fourth Window is the depth-2 call's first: one at depth 1, two for
		// the claims.
		if n == 4 {
			late.write(gone(sched(P1, N1), hm(10, 1)))
		}
	}}
	res := impact(t, st, H, w30, blastradius.Options{})
	if inner.LastSeq() != before+1 {
		t.Fatal("the hook did not write")
	}
	if d := sameResult(res, want); d != "" || res.Token != before {
		t.Errorf("token %d, want %d: %s", res.Token, before, d)
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

func TestScenarioIRetention(t *testing.T) {
	// After a retention at the window's start a walk answers as on a store that
	// kept everything, reads included; a window that starts before the horizon
	// is refused by the store, with ErrBeforeHorizon and a zero result.
	p1 := observe(ent(P1), k8s, hm(9, 0), 0)
	for _, tc := range []struct {
		name string
		keep [4]bool
	}{
		{"L1 retained", [4]bool{true, false, true, true}},
		{"L2 retained", [4]bool{true, true, false, true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			twin := scenarioA(t, memstore.Options{}, p1)
			st := scenarioA(t, memstore.Options{Keep: tc.keep}, p1)
			if err := st.Retain(ctx, hm(10, 0)); err != nil {
				t.Fatal(err)
			}
			for _, seed := range []identity.Fingerprint{H, P1} {
				if got, want := impact(t, st, seed, w30, blastradius.Options{}), impact(t, twin, seed, w30, blastradius.Options{}); !reflect.DeepEqual(got, want) {
					t.Errorf("Impact(%s) after the retention:\n%+v\nwant\n%+v", seed, got, want)
				}
			}
			res, err := blastradius.NewWalker(st).Impact(ctx, H, interval.Window{From: hm(9, 59), To: hm(10, 30), AsOf: store.Latest}, blastradius.Options{})
			if !errors.Is(err, store.ErrBeforeHorizon) || res.Token != 0 || res.Members != nil || res.Seed != (identity.Fingerprint{}) {
				t.Fatalf("a window before the horizon: %v, %+v; want ErrBeforeHorizon with a zero result", err, res)
			}
		})
	}
}

func TestScenarioJArguments(t *testing.T) {
	inner := dataA(t)
	st := &spy{Store: inner}
	w := blastradius.NewWalker(st)
	unknown, err := identity.FingerprintFromHash("x.unknown", [identity.FingerprintBytes]byte{1})
	if err != nil {
		t.Fatal(err)
	}
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
		{"From == To", H, win(hm(10, 0), hm(10, 0)), blastradius.Options{}},
		{"From > To", H, win(hm(10, 1), hm(10, 0)), blastradius.Options{}},
		{"zero seed", identity.Fingerprint{}, w30, blastradius.Options{}},
		{"unknown type", unknown, w30, blastradius.Options{}},
		{"MaxDepth 0", H, w30, pol(func(p *blastradius.Policy) { p.MaxDepth = 0 })},
		{"MaxFrontier 0", H, w30, pol(func(p *blastradius.Policy) { p.MaxFrontier = 0 })},
		{"MaxMembers -1", H, w30, pol(func(p *blastradius.Policy) { p.MaxMembers = -1 })},
		{"a partial policy", H, w30, blastradius.Options{Policy: blastradius.Policy{MaxDepth: 3}}},
	} {
		res, err := w.Impact(ctx, tc.seed, tc.w, tc.o)
		if !errors.Is(err, store.ErrInvalid) || res.Token != 0 || res.Seed != (identity.Fingerprint{}) || !strings.HasPrefix(err.Error(), "blastradius: Impact: ") {
			t.Errorf("%s: %v", tc.name, err)
		}
	}
	if st.calls != 0 {
		t.Errorf("%d store calls for invalid arguments", st.calls)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := w.Impact(cancelled, H, w30, blastradius.Options{}); !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled: %v", err)
	}
	if st.calls != 0 {
		t.Errorf("%d store calls for a cancelled context", st.calls)
	}
	if err := inner.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Impact(ctx, H, w30, blastradius.Options{}); !errors.Is(err, store.ErrClosed) {
		t.Errorf("closed: %v", err)
	}
}

func TestScenarioKTheDepthBound(t *testing.T) {
	S4, S5 := service(4), service(5)
	st, f := world(t, memstore.Options{}, S1, S2, S3, S4, S5)
	f.write(since(dependsOn(S2, S1), hm(9, 0)), since(dependsOn(S3, S2), hm(9, 0)),
		since(dependsOn(S4, S3), hm(9, 0)), since(dependsOn(S5, S4), hm(9, 0)))
	depth := func(d int) blastradius.Options {
		p := blastradius.DefaultPolicy()
		p.MaxDepth = d
		return blastradius.Options{Policy: p}
	}
	res := impact(t, st, S1, w30, depth(3))
	expect(t, "MaxDepth 3", briefs(res.Members),
		mline(S2, 1, whole, interval.ClassStable, 0, arr{S1, catalog.DependsOn, whole}),
		mline(S3, 2, whole, interval.ClassStable, 0, arr{S2, catalog.DependsOn, whole}),
		mline(S4, 3, whole, interval.ClassStable, 0, arr{S3, catalog.DependsOn, whole}))
	if !slices.Equal(res.Truncated.Depth, []identity.Fingerprint{S4}) {
		t.Errorf("Truncated.Depth %v, want [%s]", res.Truncated.Depth, S4)
	}
	expectReads(t, res.Reads, 3, 6, 6, 6)

	res = impact(t, st, S1, w30, depth(4))
	if got := entities(res.Members); !slices.Equal(got, []identity.Fingerprint{S2, S3, S4, S5}) || res.Members[3].Depth != 4 {
		t.Errorf("MaxDepth 4: %v", briefs(res.Members))
	}
	if !slices.Equal(res.Truncated.Depth, []identity.Fingerprint{S5}) {
		t.Errorf("MaxDepth 4: Truncated.Depth %v, want [%s]", res.Truncated.Depth, S5)
	}
	expectReads(t, res.Reads, 4, 8, 8, 8)

	res = impact(t, st, S1, w30, blastradius.Options{})
	if len(res.Members) != 4 || res.Truncated.Depth != nil {
		t.Errorf("default: %v, truncated %+v", briefs(res.Members), res.Truncated)
	}
	expectReads(t, res.Reads, 5, 10, 8, 8)
}

func TestScenarioLTheHotHost(t *testing.T) {
	N := node(1)
	nth := func(typ catalog.EntityType, i int) identity.Fingerprint {
		var h [identity.FingerprintBytes]byte
		h[0], h[1] = byte(i>>8), byte(i)
		fp, err := identity.FingerprintFromHash(typ, h)
		if err != nil {
			t.Fatal(err)
		}
		return fp
	}
	st, f := world(t, memstore.Options{}, H, N)
	f.write(since(runsOn(N, H), hm(9, 0)))
	for i := 1; i <= blastradius.BudgetReferencePods; i++ {
		p, c := nth(catalog.K8sPod, i), nth(catalog.Container, i)
		f.write(observe(ent(p), k8s, hm(9, 0), 0), observe(ent(c), k8s, hm(9, 0), 0),
			since(sched(p, N), hm(9, 0)), since(partOf(c, p), hm(9, 0)))
	}
	for i := range 40 {
		p := nth(catalog.K8sPod, 300+i)
		f.write(observe(ent(p), k8s, hm(7, 0), 0), since(sched(p, N), hm(8, 0)), gone(sched(p, N), hm(9, i)))
	}
	res := impact(t, st, H, w30, blastradius.Options{})
	depths := map[int]int{}
	for _, m := range res.Members {
		depths[m.Depth]++
		if !sameSpans(m.Reach, whole) {
			t.Fatalf("%s", brief(m))
		}
	}
	if len(res.Members) != 221 || depths[1] != 1 || depths[2] != 110 || depths[3] != 110 || fmt.Sprint(res.Truncated) != fmt.Sprint(blastradius.Truncation{}) {
		t.Errorf("%d members by depth %v, truncated %+v", len(res.Members), depths, res.Truncated)
	}
	expectReads(t, res.Reads, 4, 114, 335, 335)
	if res.Reads.Alive > blastradius.BudgetReferenceExistence || res.Reads.Windows > blastradius.BudgetReferenceWindows {
		t.Errorf("reads %+v over the budget", res.Reads)
	}
	// To depth 2: the hot node's reverse call adds 1 + 110 existence reads, and
	// none for the 40 pods whose placement ended before the window.
	p := blastradius.DefaultPolicy()
	p.MaxDepth = 2
	expectReads(t, impact(t, st, H, w30, blastradius.Options{Policy: p}).Reads, 3, 4, 115, 115)
}

func TestScenarioMFabricPlacementPerTargetType(t *testing.T) {
	R, R2, Z := rack(1), rack(2), zone(1)
	H1 := H
	data := func(t *testing.T) (*memstore.Store, *feed) {
		st, f := world(t, memstore.Options{}, R, R2, Z, H1, H2, N1, P1)
		f.write(since(locatedIn(H1, R), hm(9, 0)), since(locatedIn(H1, Z), hm(9, 0)), since(locatedIn(H2, R), hm(9, 0)),
			since(runsOn(N1, H1), hm(9, 0)), since(sched(P1, N1), hm(9, 0)))
		return st, f
	}
	t.Run("M", func(t *testing.T) {
		st, _ := data(t)
		res := impact(t, st, R, w30, blastradius.Options{})
		expect(t, "Impact(R)", briefs(res.Members),
			mline(H1, 1, whole, interval.ClassStable, 0, arr{R, catalog.LocatedIn, whole}),
			mline(H2, 1, whole, interval.ClassStable, 0, arr{R, catalog.LocatedIn, whole}),
			mline(N1, 2, whole, interval.ClassStable, 0, arr{H1, catalog.RunsOn, whole}),
			mline(P1, 3, whole, interval.ClassStable, 0, arr{N1, catalog.ScheduledOn, whole}))
		expect(t, "unattributed", describeUnattributed(res.Unattributed))
		expectReads(t, res.Reads, 6, 9, 13, 13)
	})
	t.Run("M′", func(t *testing.T) {
		st, f := data(t)
		f.write(since(locatedIn(H2, R2), hm(10, 10)))
		res := impact(t, st, R, w30, blastradius.Options{})
		first := span(hm(10, 0), hm(10, 10))
		expect(t, "Impact(R)", briefs(res.Members),
			mline(H1, 1, whole, interval.ClassStable, 0, arr{R, catalog.LocatedIn, whole}),
			mline(H2, 1, first, interval.ClassDisappeared, 1, arr{R, catalog.LocatedIn, first}),
			mline(N1, 2, whole, interval.ClassStable, 0, arr{H1, catalog.RunsOn, whole}),
			mline(P1, 3, whole, interval.ClassStable, 0, arr{N1, catalog.ScheduledOn, whole}))
		expect(t, "unattributed", describeUnattributed(res.Unattributed),
			uline(H2, catalog.LocatedIn, catalog.Rack, []identity.Fingerprint{R, R2}, sp(hm(10, 10), hm(10, 30))))
		expectReads(t, res.Reads, 6, 9, 14, 14)
	})
}

func TestScenarioNTheCascadeInsideTheWalk(t *testing.T) {
	st := scenarioA(t, memstore.Options{}, run(ent(P1), k8s, hm(9, 0), 20*time.Minute, hm(10, 0)))
	res := impact(t, st, H, w30, blastradius.Options{})
	cut := span(hm(10, 0), hm(10, 20))
	if got := briefs(res.Members); len(got) != 5 || got[2] != mline(P1, 2, cut, interval.ClassDisappeared, 1, arr{N1, catalog.ScheduledOn, cut}) ||
		got[4] != mline(C1, 3, cut, interval.ClassDisappeared, 1, arr{P1, catalog.PartOf, cut}) {
		t.Errorf("Impact(H):\n  %s", strings.Join(got, "\n  "))
	}
	expectLegEnd(t, res, P1, N1, hm(10, 20), lifecycle.EndCascade)
	expectLegEnd(t, res, C1, P1, hm(10, 20), lifecycle.EndCascade)
}

func TestScenarioOPodOnTwoNodesIsNotChecked(t *testing.T) {
	P := pod(1)
	st, f := world(t, memstore.Options{}, N1, N2, P)
	f.write(observe(sched(P, N1), k8s, hm(9, 0), 0), observe(sched(P, N2), "k8scluster/default", hm(9, 0), 0))
	res := impact(t, st, N1, w30, blastradius.Options{})
	expect(t, "Impact(N1)", briefs(res.Members), mline(P, 1, whole, interval.ClassStable, 0, arr{N1, catalog.ScheduledOn, whole}))
	expect(t, "unattributed", describeUnattributed(res.Unattributed))
}

// TestScenarioPTheWalkReachesServiceInstancesAndStops is A with a service
// instance part of the first pod: the walk reaches the instance from the pod,
// and stops there, because instance_of propagates nothing.
func TestScenarioPTheWalkReachesServiceInstancesAndStops(t *testing.T) {
	S, SI := service(1), instance(1)
	st, f := world(t, memstore.Options{}, H, N1, N2, P1, P2, C1, S, SI)
	f.write(
		since(runsOn(N1, H), hm(9, 0)), since(runsOn(N2, H), hm(9, 0)),
		since(sched(P1, N1), hm(9, 0)),
		since(sched(P2, N2), hm(10, 10)), gone(sched(P2, N2), hm(10, 20)),
		since(partOf(C1, P1), hm(9, 0)),
		since(partOf(SI, P1), hm(9, 0)), since(instanceOf(SI, S), hm(9, 0)))
	res := impact(t, st, H, w30, blastradius.Options{})
	// At depth 3 a container sorts before a service instance by type name.
	expect(t, "Impact(H)", briefs(res.Members),
		mline(N1, 1, whole, interval.ClassStable, 0, arr{H, catalog.RunsOn, whole}),
		mline(N2, 1, whole, interval.ClassStable, 0, arr{H, catalog.RunsOn, whole}),
		mline(P1, 2, whole, interval.ClassStable, 0, arr{N1, catalog.ScheduledOn, whole}),
		mline(P2, 2, span(hm(10, 10), hm(10, 20)), interval.ClassTransient, 2, arr{N2, catalog.ScheduledOn, span(hm(10, 10), hm(10, 20))}),
		mline(C1, 3, whole, interval.ClassStable, 0, arr{P1, catalog.PartOf, whole}),
		mline(SI, 3, whole, interval.ClassStable, 0, arr{P1, catalog.PartOf, whole}),
	)
	if slices.Contains(entities(res.Members), S) {
		t.Errorf("the service %s is a member", S)
	}
	// Depth 3 reads P1, C1 and SI. There is no depth-4 call: neither a
	// container nor a service instance has a reverse edge layer.
	expectReads(t, res.Reads, 4, 9, 13, 13)

	// From the service, the instance_of edge into it is read and counted, and
	// not followed.
	res = impact(t, st, S, w30, blastradius.Options{})
	expect(t, "Impact(S)", briefs(res.Members))
	expectReads(t, res.Reads, 1, 2, 2, 2)
}

// TestScenarioCaps checks the frontier and member caps on fixed data: a rack
// with three hosts, each with one node.
func TestScenarioCaps(t *testing.T) {
	R := rack(1)
	H3, N3 := host(3), node(3)
	st, f := world(t, memstore.Options{}, R, H, H2, H3, N1, N2, N3)
	f.write(since(locatedIn(H, R), hm(9, 0)), since(locatedIn(H2, R), hm(10, 10)), since(locatedIn(H3, R), hm(10, 20)),
		since(runsOn(N1, H), hm(9, 0)), since(runsOn(N2, H2), hm(9, 0)), since(runsOn(N3, H3), hm(9, 0)))
	p := blastradius.DefaultPolicy()
	p.MaxFrontier = 2
	res := impact(t, st, R, w30, blastradius.Options{Policy: p})
	if got := entities(res.Members); !slices.Equal(got, []identity.Fingerprint{H, H2, H3, N1, N2}) {
		t.Errorf("MaxFrontier 2: %v", briefs(res.Members))
	}
	if fmt.Sprint(res.Truncated.Frontier) != fmt.Sprint([]blastradius.FrontierCut{{Depth: 1, Dropped: []identity.Fingerprint{H3}}}) {
		t.Errorf("MaxFrontier 2: %+v", res.Truncated)
	}
	p = blastradius.DefaultPolicy()
	p.MaxMembers = 4
	res = impact(t, st, R, w30, blastradius.Options{Policy: p})
	if got := entities(res.Members); !slices.Equal(got, []identity.Fingerprint{H, H2, H3, N1}) || res.Truncated.Members != 2 {
		t.Errorf("MaxMembers 4: %v, %+v", briefs(res.Members), res.Truncated)
	}
}
