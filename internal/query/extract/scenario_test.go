package extract_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/coresidency"
	"github.com/lotannauo/toposhift/internal/query/extract"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

// The scenarios fix exact answers, written before the code. Times are on
// 2026-01-01 UTC, written hh:mm; every entity is observed at 09:00 with no end,
// and the window is [10:00, 10:30) unless a scenario says otherwise.

func hm(h, m int) time.Time { return time.Date(2026, 1, 1, h, m, 0, 0, time.UTC) }

func win(from, to time.Time) interval.Window {
	return interval.Window{From: from, To: to, AsOf: store.Latest}
}

var w30 = win(hm(10, 0), hm(10, 30))

func sp(a, b time.Time) interval.Span { return interval.Span{Start: a, End: b} }

// world is a store with the entities observed at 09:00 with no end.
func world(t *testing.T, opts memstore.Options, fps ...identity.Fingerprint) (*memstore.Store, *feed) {
	t.Helper()
	st := openStore(t, opts)
	f := newFeed(t, st)
	var rs []store.Record
	for _, fp := range fps {
		rs = append(rs, obs(ent(fp), hm(9, 0)))
	}
	if len(rs) > 0 {
		f.write(rs...)
	}
	return st, f
}

// service writes instance i of s, part of pod p, from 09:00.
func service(f *feed, s, i, p identity.Fingerprint) {
	f.write(obs(instOf(i, s), hm(9, 0)), obs(partOf(i, p), hm(9, 0)))
}

func run(t *testing.T, st store.Store, seed extract.Seed, w interval.Window, o extract.Options) extract.Result {
	t.Helper()
	res, err := extract.NewExtractor(st).Extract(ctx, seed, w, o)
	if err != nil {
		t.Fatal(err)
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

func expectReads(t *testing.T, got coresidency.Reads, calls, windows, alive, ew int) {
	t.Helper()
	if want := (coresidency.Reads{Calls: calls, Windows: windows, Alive: alive, EntityWindows: ew}); got != want {
		t.Errorf("reads %+v, want %+v", got, want)
	}
}

// ps is an expected pair span: its span, Before, and its ends as endString
// writes them by index.
type ps struct {
	s, e   time.Time
	before bool
	ends   []string
}

// pl is an expected pair in describePair's format, by index.
func pl(mech extract.Mechanism, member, peer identity.Fingerprint, class interval.Class, tr int, reason coresidency.Reason,
	cov [2]int, vias []string, contributions int, spans ...ps,
) string {
	var b strings.Builder
	var d time.Duration
	for _, s := range spans {
		d += s.e.Sub(s.s)
	}
	fmt.Fprintf(&b, "%s member=%s peer=%s %s %s transitions=%d reason=%s coverage=%v", mech, member, peer, d, class, tr, reason, cov)
	for _, s := range spans {
		ends := s.ends
		if ends == nil {
			ends = []string{}
		}
		fmt.Fprintf(&b, " {[%s, %s) before=%t ends=%v}", ts(s.s), ts(s.e), s.before, ends)
	}
	slices.Sort(vias)
	fmt.Fprintf(&b, " vias=%v contributions=%d", vias, contributions)
	return b.String()
}

func via(fp identity.Fingerprint, ss ...interval.Span) string {
	return fmt.Sprintf("%s:%s", fp, spansString(ss))
}

func ua(seed, subject identity.Fingerprint, why string, cands []identity.Fingerprint, ss ...interval.Span) string {
	s := "-"
	if !seed.IsZero() {
		s = seed.String()
	}
	return fmt.Sprintf("%s %s %s %s %v", s, subject, why, spansString(ss), cands)
}

var (
	S, T, C, K = svc(1), svc(2), svc(3), svc(4)
	IS, IT, IC = inst(1), inst(2), inst(3)
	PS, PT, PC = pod(1), pod(2), pod(3)
	N, H, H2   = node(1), host(1), host(2)
)

var none = [2]int{}

// scenarioQA writes Q-A: the seed's pod and T's pod on one node of one host, T's
// from 10:10 to 10:20, with a caller C and a callee K.
func scenarioQA(t *testing.T, opts memstore.Options) (*memstore.Store, *feed) {
	st, f := world(t, opts, S, T, C, K, IS, IT, PS, PT, N, H)
	service(f, S, IS, PS)
	service(f, T, IT, PT)
	f.write(obs(sched(PS, N), hm(9, 0)), obs(sched(PT, N), hm(10, 10)), del(sched(PT, N), hm(10, 20)),
		obs(runsOn(N, H), hm(9, 0)), obs(dependsOn(C, S), hm(9, 0)), obs(dependsOn(S, K), hm(9, 0)))
	return st, f
}

var stableDep = "stable [2026-01-01T10:00:00Z, 2026-01-01T10:30:00Z) before=true end=unknown"

func qaMembers(cInstances ...string) []string {
	return []string{
		fmt.Sprintf("%s seed deps=[] instances=[%s]", S, subjectString(instOf(IS, S))),
		fmt.Sprintf("%s caller deps=[%s %s] instances=%v", C, subjectString(dependsOn(C, S)), stableDep, cInstances),
		fmt.Sprintf("%s callee deps=[%s %s] instances=[]", K, subjectString(dependsOn(S, K)), stableDep),
	}
}

func qaPair(member identity.Fingerprint) string {
	return pl(extract.SharedNode, member, T, interval.ClassTransient, 2, coresidency.ReasonNone, none,
		[]string{via(N, sp(hm(10, 10), hm(10, 20)))}, 1,
		ps{hm(10, 10), hm(10, 20), false, []string{"c0:leg5:pod(1,producer)"}})
}

func TestScenarioQAAnchoredMinimal(t *testing.T) {
	st, _ := scenarioQA(t, memstore.Options{})
	res := run(t, st, extract.Anchored{Service: S, Host: H, At: hm(10, 15)}, w30, extract.Options{})
	if res.Anchor != extract.OnHost {
		t.Errorf("anchor %s, want on_host", res.Anchor)
	}
	expect(t, "members", describeMembers(res.Members), qaMembers()...)
	expect(t, "pairs", describePairs(res.Pairs, false), qaPair(S))
	expect(t, "suppressed", describePairs(res.Suppressed, false))
	expect(t, "unattributed", describeUnattributed(res.Unattributed))
	expect(t, "unmapped", describeUnmapped(res.Unmapped))
	if res.Ubiquity != (extract.Ubiquity{Judged: false, FrontierNodes: 1}) {
		t.Errorf("ubiquity %+v", res.Ubiquity)
	}
	if fr := res.PodResults; len(fr) != 1 || fr[0].Seed != PS ||
		!slices.EqualFunc(fr[0].SeedHosts, []coresidency.HostSpans{{Node: N, Host: H, Spans: []interval.Span{sp(hm(10, 0), hm(10, 30))}}},
			func(a, b coresidency.HostSpans) bool {
				return a.Node == b.Node && a.Host == b.Host && sameSpans(a.Spans, b.Spans)
			}) {
		t.Errorf("pod results %v", seedsOf(res))
	}
	expectReads(t, res.Reads, 10, 14, 29, 29)
	expectStoreReads(t, res.StoreReads, 12, 10, 10)
	checkMemo(t, st, extract.Anchored{Service: S, Host: H, At: hm(10, 15)}, w30, extract.Options{})
	if res.Token != st.LastSeq() || res.PolicyVersion != 1 || res.CoResidencyPolicy != coresidency.PolicyVersion ||
		res.IntervalPolicy != interval.PolicyVersion || res.Seed != (extract.Anchored{Service: S, Host: H, At: hm(10, 15)}) {
		t.Errorf("header %+v", res)
	}
	if c := res.Pairs[0].Contributions[0]; c.MemberPod != PS || c.PeerPod != PT || c.MemberInst != IS || c.PeerInst != IT ||
		c.Via != N || c.Entry != 0 || res.PodResults[0].CoResidents[c.Entry].Peer != PT {
		t.Errorf("contribution %+v", c)
	}
}

func TestScenarioQA2ANeighbourOnTheAnchor(t *testing.T) {
	st, f := scenarioQA(t, memstore.Options{})
	f.write(obs(ent(IC), hm(9, 0)), obs(ent(PC), hm(9, 0)))
	service(f, C, IC, PC)
	f.write(obs(sched(PC, N), hm(9, 0)))
	res := run(t, st, extract.Anchored{Service: S, Host: H, At: hm(10, 15)}, w30, extract.Options{})
	if res.Anchor != extract.OnHost || !slices.Equal(seedsOf(res), []identity.Fingerprint{PS, PC}) {
		t.Errorf("anchor %s, seed pods %v", res.Anchor, seedsOf(res))
	}
	expect(t, "members", describeMembers(res.Members), qaMembers(subjectString(instOf(IC, C)))...)
	expect(t, "pairs", describePairs(res.Pairs, false),
		pl(extract.SharedNode, S, C, interval.ClassStable, 0, coresidency.ReasonNone, none,
			[]string{via(N, sp(hm(10, 0), hm(10, 30)))}, 2, ps{hm(10, 0), hm(10, 30), true, nil}),
		qaPair(S),
		qaPair(C))
	expectReads(t, res.Reads, 14, 21, 46, 46)
	expectStoreReads(t, res.StoreReads, 15, 12, 12)
	checkMemo(t, st, extract.Anchored{Service: S, Host: H, At: hm(10, 15)}, w30, extract.Options{})
}

func TestScenarioQA3TheAnchorsNodeOnTwoHosts(t *testing.T) {
	st, f := scenarioQA(t, memstore.Options{})
	f.write(obs(ent(H2), hm(9, 0)), obs(runsOn(N, H2), hm(10, 12)))
	res := run(t, st, extract.Anchored{Service: S, Host: H, At: hm(10, 15)}, w30, extract.Options{})
	if res.Anchor != extract.HostUnattributed {
		t.Errorf("anchor %s, want host_unattributed", res.Anchor)
	}
	expect(t, "pairs", describePairs(res.Pairs, false))
	expect(t, "unattributed", describeUnattributed(res.Unattributed),
		ua(PS, N, "several_hosts", []identity.Fingerprint{H, H2}, sp(hm(10, 12), hm(10, 30))))
	if len(res.PodResults) != 1 || len(res.PodResults[0].SeedHosts) != 1 || !sameSpans(res.PodResults[0].SeedHosts[0].Spans, []interval.Span{sp(hm(10, 0), hm(10, 12))}) {
		t.Errorf("pod results %+v", res.PodResults)
	}
	expectReads(t, res.Reads, 10, 14, 30, 30)
	expectStoreReads(t, res.StoreReads, 12, 11, 11)
	checkMemo(t, st, extract.Anchored{Service: S, Host: H, At: hm(10, 15)}, w30, extract.Options{})
}

func TestScenarioQBAnchoredNotOnTheHost(t *testing.T) {
	st, f := scenarioQA(t, memstore.Options{})
	f.write(del(sched(PS, N), hm(10, 10)))
	res := run(t, st, extract.Anchored{Service: S, Host: H, At: hm(10, 15)}, w30, extract.Options{})
	if res.Anchor != extract.NotOnHost || len(res.PodResults) != 0 {
		t.Errorf("anchor %s, %d finds", res.Anchor, len(res.PodResults))
	}
	expect(t, "members", describeMembers(res.Members), qaMembers()...)
	expect(t, "pairs", describePairs(res.Pairs, false))
	expectReads(t, res.Reads, 6, 7, 15, 15)
	expectStoreReads(t, res.StoreReads, 7, 10, 10)
	checkMemo(t, st, extract.Anchored{Service: S, Host: H, At: hm(10, 15)}, w30, extract.Options{})

	// The anchor's instant decides: at 10:05 the seed's pod is on the host.
	res = run(t, st, extract.Anchored{Service: S, Host: H, At: hm(10, 5)}, w30, extract.Options{})
	if res.Anchor != extract.OnHost {
		t.Errorf("at 10:05: anchor %s, want on_host", res.Anchor)
	}
}

// scenarioQC writes the plan's regression at the service level: A's pod left
// the node at 10:05 and B's came at 10:06.
func scenarioQC(t *testing.T) (*memstore.Store, [3]identity.Fingerprint) {
	A, B := svc(2), svc(3)
	IA, IB, PA, PB := inst(2), inst(3), pod(2), pod(3)
	st, f := world(t, memstore.Options{}, S, A, B, IS, IA, IB, PS, PA, PB, N)
	service(f, S, IS, PS)
	service(f, A, IA, PA)
	service(f, B, IB, PB)
	f.write(obs(dependsOn(S, A), hm(9, 0)), obs(dependsOn(B, S), hm(9, 0)),
		obs(sched(PS, N), hm(9, 0)), obs(sched(PA, N), hm(9, 58)), del(sched(PA, N), hm(10, 5)), obs(sched(PB, N), hm(10, 6)))
	return st, [3]identity.Fingerprint{A, B, PA}
}

func TestScenarioQCNoUnionGraphAtTheServiceLevel(t *testing.T) {
	st, e := scenarioQC(t)
	A, B, PA, PB := e[0], e[1], e[2], pod(3)
	res := run(t, st, extract.Exploratory{Service: S}, w30, extract.Options{})
	expect(t, "members", describeMembers(res.Members),
		fmt.Sprintf("%s seed deps=[] instances=[%s]", S, subjectString(instOf(IS, S))),
		fmt.Sprintf("%s callee deps=[%s %s] instances=[%s]", A, subjectString(dependsOn(S, A)), stableDep, subjectString(instOf(inst(2), A))),
		fmt.Sprintf("%s caller deps=[%s %s] instances=[%s]", B, subjectString(dependsOn(B, S)), stableDep, subjectString(instOf(inst(3), B))))
	expect(t, "pairs", describePairs(res.Pairs, false),
		pl(extract.SharedNode, S, B, interval.ClassAppeared, 1, coresidency.ReasonNone, none,
			[]string{via(N, sp(hm(10, 6), hm(10, 30)))}, 2, ps{hm(10, 6), hm(10, 30), false, nil}),
		pl(extract.SharedNode, S, A, interval.ClassDisappeared, 1, coresidency.ReasonNone, none,
			[]string{via(N, sp(hm(10, 0), hm(10, 5)))}, 2,
			ps{hm(10, 0), hm(10, 5), true, []string{"c0:leg5:pod(1,producer)", "c1:leg4:pod(0,producer)"}}))
	for _, p := range res.Pairs {
		if (p.Member == A && p.Peer == B) || (p.Member == B && p.Peer == A) {
			t.Errorf("a pair of A and B, which never shared the node: %s", describePair(p, false))
		}
	}
	if !slices.Equal(seedsOf(res), []identity.Fingerprint{PS, PA, PB}) {
		t.Errorf("seed pods %v", seedsOf(res))
	}
	expect(t, "unattributed", describeUnattributed(res.Unattributed),
		ua(PS, N, "no_host", nil, sp(hm(10, 0), hm(10, 30))),
		ua(PA, N, "no_host", nil, sp(hm(10, 0), hm(10, 5))),
		ua(PB, N, "no_host", nil, sp(hm(10, 6), hm(10, 30))))
	// S1 2/3, S2 1/2, X1 4/5, X2 3/9, three Finds 4/6 each (no runs_on: hop C
	// reads the node's two forward layers and finds nothing), M1 3/6 (the seed
	// pods' sidecars), no M2.
	expectReads(t, res.Reads, 14, 25, 43, 43)
	expectStoreReads(t, res.StoreReads, 19, 10, 10)
	checkMemo(t, st, extract.Exploratory{Service: S}, w30, extract.Options{})
}

func TestScenarioQDInstancesUnion(t *testing.T) {
	IT1, IT2, PT1, PT2 := inst(2), inst(3), pod(2), pod(3)
	setup := func(t *testing.T, t2 time.Time) *memstore.Store {
		st, f := world(t, memstore.Options{}, S, T, IS, IT1, IT2, PS, PT1, PT2, N, H)
		service(f, S, IS, PS)
		service(f, T, IT1, PT1)
		service(f, T, IT2, PT2)
		f.write(obs(runsOn(N, H), hm(9, 0)), obs(sched(PS, N), hm(9, 0)),
			obs(sched(PT1, N), hm(10, 0)), del(sched(PT1, N), hm(10, 10)),
			obs(sched(PT2, N), t2), del(sched(PT2, N), hm(10, 20)))
		return st
	}
	res := run(t, setup(t, hm(10, 10)), extract.Exploratory{Service: S}, w30, extract.Options{})
	expect(t, "pairs", describePairs(res.Pairs, false),
		pl(extract.SharedNode, S, T, interval.ClassDisappeared, 1, coresidency.ReasonNone, none,
			[]string{via(N, sp(hm(10, 0), hm(10, 20)))}, 2,
			ps{hm(10, 0), hm(10, 20), false, []string{"c1:leg5:pod(1,producer)"}}))

	ns := hm(10, 10).Add(time.Nanosecond)
	res = run(t, setup(t, ns), extract.Exploratory{Service: S}, w30, extract.Options{})
	expect(t, "pairs, a 1 ns gap", describePairs(res.Pairs, false),
		pl(extract.SharedNode, S, T, interval.ClassFlapping, 3, coresidency.ReasonNone, none,
			[]string{via(N, sp(hm(10, 0), hm(10, 10)), sp(ns, hm(10, 20)))}, 2,
			ps{hm(10, 0), hm(10, 10), false, []string{"c0:leg5:pod(1,producer)"}},
			ps{ns, hm(10, 20), false, []string{"c1:leg5:pod(1,producer)"}}))
}

func TestScenarioQEUnmapped(t *testing.T) {
	X := pod(9)
	st, f := world(t, memstore.Options{}, S, IS, PS, X, N, H)
	service(f, S, IS, PS)
	f.write(obs(runsOn(N, H), hm(9, 0)), obs(sched(PS, N), hm(9, 0)), obs(sched(X, N), hm(9, 0)))
	res := run(t, st, extract.Exploratory{Service: S}, w30, extract.Options{})
	expect(t, "pairs", describePairs(res.Pairs, false))
	expect(t, "unmapped", describeUnmapped(res.Unmapped), fmt.Sprintf("%s %s", X, spansString([]interval.Span{sp(hm(10, 0), hm(10, 30))})))

	// A description naming a workload maps nothing.
	r := obs(ent(X), hm(9, 30))
	r.Payload = []byte(`{"owner":{"kind":"ReplicaSet","name":"t-5d8"},"k8s.workload.name":"t"}`)
	newFeed(t, st).write(r)
	res = run(t, st, extract.Exploratory{Service: S}, w30, extract.Options{})
	expect(t, "pairs with a description", describePairs(res.Pairs, false))
	expect(t, "unmapped with a description", describeUnmapped(res.Unmapped), fmt.Sprintf("%s %s", X, spansString([]interval.Span{sp(hm(10, 0), hm(10, 30))})))
}

func TestScenarioQFSharedPod(t *testing.T) {
	Z, instZ := svc(5), inst(5)
	setup := func(t *testing.T, zFrom, zTo time.Time) *memstore.Store {
		st, f := world(t, memstore.Options{}, S, T, Z, IS, IT, instZ, PS, PT, N, H)
		service(f, S, IS, PS)
		service(f, T, IT, PT)
		f.write(obs(instOf(instZ, Z), hm(9, 0)), obs(partOf(instZ, PS), zFrom), obs(runsOn(N, H), hm(9, 0)),
			obs(sched(PS, N), hm(9, 0)), obs(sched(PT, N), hm(9, 0)))
		if zTo.Before(w30.To) {
			f.write(del(partOf(instZ, PS), zTo))
		}
		return st
	}
	tPair := pl(extract.SharedNode, S, T, interval.ClassStable, 0, coresidency.ReasonNone, none,
		[]string{via(N, sp(hm(10, 0), hm(10, 30)))}, 1, ps{hm(10, 0), hm(10, 30), true, nil})
	res := run(t, setup(t, hm(9, 0), w30.To), extract.Exploratory{Service: S}, w30, extract.Options{})
	expect(t, "pairs", describePairs(res.Pairs, false),
		pl(extract.SharedPod, S, Z, interval.ClassStable, 0, coresidency.ReasonNone, none,
			[]string{via(PS, sp(hm(10, 0), hm(10, 30)))}, 1, ps{hm(10, 0), hm(10, 30), true, nil}),
		tPair)
	if c := res.Pairs[0].Contributions[0]; c.Entry != -1 || c.MemberPod != PS || c.PeerPod != PS || c.PeerInst != instZ {
		t.Errorf("shared pod contribution %+v", c)
	}

	// Shorter than the shared node, the shared pod ranks after it: pairs rank
	// by duration first. Its end is the peer instance's part_of (leg 2).
	res = run(t, setup(t, hm(10, 10), hm(10, 20)), extract.Exploratory{Service: S}, w30, extract.Options{})
	expect(t, "pairs, a shorter shared pod", describePairs(res.Pairs, false),
		tPair,
		pl(extract.SharedPod, S, Z, interval.ClassTransient, 2, coresidency.ReasonNone, none,
			[]string{via(PS, sp(hm(10, 10), hm(10, 20)))}, 1, ps{hm(10, 10), hm(10, 20), false, []string{"c0:leg2:producer"}}))

	// The same from an anchor.
	res = run(t, setup(t, hm(9, 0), w30.To), extract.Anchored{Service: S, Host: H, At: hm(10, 0)}, w30, extract.Options{})
	if len(res.Pairs) != 2 || res.Pairs[0].Mechanism != extract.SharedPod || res.Pairs[0].Peer != Z {
		t.Errorf("anchored: %v", describePairs(res.Pairs, false))
	}
}

// scenarioQG writes S with a pod on each of sNodes nodes, D with a pod on each
// of dNodes of them, and R with one pod on the first.
func scenarioQG(t *testing.T, sNodes, dNodes int) *memstore.Store {
	D, R := svc(6), svc(7)
	st, f := world(t, memstore.Options{}, S, D, R)
	add := func(s identity.Fingerprint, n byte, k int) {
		i, p := inst(n), pod(n)
		f.write(obs(ent(i), hm(9, 0)), obs(ent(p), hm(9, 0)))
		service(f, s, i, p)
		f.write(obs(sched(p, node(byte(k))), hm(9, 0)))
	}
	for k := 1; k <= sNodes; k++ {
		f.write(obs(ent(node(byte(k))), hm(9, 0)))
		add(S, byte(10+k), k)
		if k <= dNodes {
			add(D, byte(20+k), k)
		}
	}
	add(R, 31, 1)
	return st
}

func TestScenarioQGUbiquity(t *testing.T) {
	D, R := svc(6), svc(7)
	opts := extract.Options{NodeOnly: true}
	reasons := func(ps []extract.Pair) []string {
		var out []string
		for _, p := range ps {
			out = append(out, fmt.Sprintf("%s %s %v", p.Peer, p.Reason, p.Coverage))
		}
		return out
	}
	for _, tc := range []struct {
		name          string
		s, d          int
		pairs, suppr  []string
		judged        bool
		frontierNodes int
	}{
		{"D on 6 of 6", 6, 6, []string{fmt.Sprintf("%s none [1 6]", R)}, []string{fmt.Sprintf("%s ubiquitous [6 6]", D)}, true, 6},
		{"D on 5 of 6", 6, 5, []string{fmt.Sprintf("%s none [1 6]", R)}, []string{fmt.Sprintf("%s ubiquitous [5 6]", D)}, true, 6},
		{"D on 4 of 6", 6, 4, []string{fmt.Sprintf("%s none [4 6]", D), fmt.Sprintf("%s none [1 6]", R)}, nil, true, 6},
		{"D on 4 of 5, the boundary", 5, 4, []string{fmt.Sprintf("%s none [1 5]", R)}, []string{fmt.Sprintf("%s ubiquitous [4 5]", D)}, true, 5},
		{"S on 4 nodes", 4, 4, []string{fmt.Sprintf("%s none [0 0]", D), fmt.Sprintf("%s none [0 0]", R)}, nil, false, 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := run(t, scenarioQG(t, tc.s, tc.d), extract.Exploratory{Service: S}, w30, opts)
			expect(t, "pairs", reasons(res.Pairs), tc.pairs...)
			expect(t, "suppressed", reasons(res.Suppressed), tc.suppr...)
			if res.Ubiquity != (extract.Ubiquity{Judged: tc.judged, FrontierNodes: tc.frontierNodes}) {
				t.Errorf("ubiquity %+v", res.Ubiquity)
			}
		})
	}

	// Only a shared node covers a node: D beside S through each node's host
	// alone covers nothing.
	t.Run("D only through hosts", func(t *testing.T) {
		st := scenarioQG(t, 5, 0)
		f := newFeed(t, st)
		for k := byte(1); k <= 5; k++ {
			i, p, n2, h := inst(40+k), pod(40+k), node(10+k), host(k)
			f.write(obs(ent(i), hm(9, 0)), obs(ent(p), hm(9, 0)), obs(ent(n2), hm(9, 0)), obs(ent(h), hm(9, 0)),
				obs(runsOn(node(k), h), hm(9, 0)), obs(runsOn(n2, h), hm(9, 0)))
			service(f, D, i, p)
			f.write(obs(sched(p, n2), hm(9, 0)))
		}
		res := run(t, st, extract.Exploratory{Service: S}, w30, extract.Options{})
		var got []string
		for _, p := range res.Pairs {
			got = append(got, fmt.Sprintf("%s %s %s %v", p.Peer, p.Mechanism, p.Reason, p.Coverage))
		}
		expect(t, "pairs", got, fmt.Sprintf("%s shared_node none [1 5]", R), fmt.Sprintf("%s shared_host none [0 5]", D))
		expect(t, "suppressed", reasons(res.Suppressed))
	})

	// Anchored, a frontier of one node is never judged.
	st := scenarioQG(t, 6, 6)
	f := newFeed(t, st)
	f.write(obs(ent(H), hm(9, 0)), obs(runsOn(node(1), H), hm(9, 0)))
	res := run(t, st, extract.Anchored{Service: S, Host: H, At: hm(10, 0)}, w30, extract.Options{})
	if res.Ubiquity.Judged || len(res.Suppressed) != 0 || res.Ubiquity.FrontierNodes != 1 {
		t.Errorf("anchored: ubiquity %+v, suppressed %v", res.Ubiquity, reasons(res.Suppressed))
	}
}

func TestScenarioQHDependencies(t *testing.T) {
	st, f := world(t, memstore.Options{}, S, C)
	f.write(observe(dependsOn(C, S), traces, hm(9, 50), 5*time.Minute), observe(dependsOn(C, S), traces, hm(9, 55), 5*time.Minute),
		observe(dependsOn(C, S), traces, hm(10, 0), 5*time.Minute))
	res := run(t, st, extract.Exploratory{Service: S}, w30, extract.Options{})
	expect(t, "members", describeMembers(res.Members),
		fmt.Sprintf("%s seed deps=[] instances=[]", S),
		fmt.Sprintf("%s caller deps=[%s disappeared [2026-01-01T10:00:00Z, 2026-01-01T10:05:00Z) before=true end=liveness_expiry] instances=[]", C, subjectString(dependsOn(C, S))))
}

func TestScenarioQIOneToken(t *testing.T) {
	// S1 2, S2 1, A1 1, A2 1, A3 2, A4 2: the Find's first Window is the tenth.
	// The Find resolves its own token before that Window, so a write before
	// the ninth also tests that the Find is given the extraction's. The count
	// is of Windows that reach the store: none of the first nine repeats an
	// earlier one, so the memo does not move the tenth.
	for _, at := range []int{10, 9} {
		t.Run(fmt.Sprintf("a write before Window %d", at), func(t *testing.T) { oneToken(t, at) })
	}
}

func oneToken(t *testing.T, window int) {
	inner, _ := scenarioQA(t, memstore.Options{})
	seed := extract.Anchored{Service: S, Host: H, At: hm(10, 15)}
	want := run(t, inner, seed, w30, extract.Options{})
	before := inner.LastSeq()
	late := newFeed(t, inner)
	st := &spy{Store: inner, onWindow: func(n int) {
		if n == window {
			late.write(del(sched(PT, N), hm(10, 12)))
		}
	}}
	res := run(t, st, seed, w30, extract.Options{})
	if inner.LastSeq() != before+1 {
		t.Fatal("the hook did not write")
	}
	if d := diffLines("pairs", describePairs(res.Pairs, true), describePairs(want.Pairs, true)); d != "" || res.Token != before {
		t.Errorf("token %d, want %d: %s", res.Token, before, d)
	}
	for _, fr := range res.PodResults {
		if fr.Token != before {
			t.Errorf("a Find at token %d, want %d", fr.Token, before)
		}
	}
	for _, tok := range st.tokens {
		if tok != before {
			t.Fatalf("a read at token %d, want %d", tok, before)
		}
	}
	if st.writes != 0 {
		t.Errorf("%d writes through the extractor's store", st.writes)
	}
}

func TestScenarioQJRetention(t *testing.T) {
	retained := func(t *testing.T, st store.Store, seed extract.Seed, o extract.Options) {
		t.Helper()
		res, err := extract.NewExtractor(st).Extract(ctx, seed, w30, o)
		if !errors.Is(err, interval.ErrRetained) || !errors.Is(err, errors.ErrUnsupported) || res.Token != 0 || res.Members != nil {
			t.Fatalf("%v, %+v; want ErrRetained with a zero result", err, res)
		}
	}
	anchored := extract.Anchored{Service: S, Host: H, At: hm(10, 15)}
	t.Run("L3 retained", func(t *testing.T) {
		st, _ := scenarioQA(t, memstore.Options{Keep: [4]bool{true, true, true, false}})
		if err := st.Retain(ctx, hm(10, 0)); err != nil {
			t.Fatal(err)
		}
		retained(t, st, extract.Exploratory{Service: S}, extract.Options{NodeOnly: true})
		retained(t, st, anchored, extract.Options{})
	})
	t.Run("L1 retained", func(t *testing.T) {
		st, _ := scenarioQA(t, memstore.Options{Keep: [4]bool{true, false, true, true}})
		if err := st.Retain(ctx, hm(10, 0)); err != nil {
			t.Fatal(err)
		}
		retained(t, st, extract.Exploratory{Service: S}, extract.Options{})
		retained(t, st, anchored, extract.Options{})
		res := run(t, st, extract.Exploratory{Service: S}, w30, extract.Options{NodeOnly: true})
		if len(res.Pairs) != 1 || res.Pairs[0].Peer != T {
			t.Errorf("NodeOnly: %v", describePairs(res.Pairs, false))
		}
		if _, err := extract.NewExtractor(st).Extract(ctx, anchored, w30, extract.Options{NodeOnly: true}); !errors.Is(err, store.ErrInvalid) {
			t.Errorf("anchored NodeOnly: %v", err)
		}
	})
}

func TestScenarioQKArguments(t *testing.T) {
	inner, _ := scenarioQA(t, memstore.Options{})
	st := &spy{Store: inner}
	e := extract.NewExtractor(st)
	over, zeroPods, negative := extract.DefaultPolicy(), extract.DefaultPolicy(), extract.DefaultPolicy()
	over.UbiquityNum = over.UbiquityDen + 1
	zeroPods.MaxSeedPods = 0
	negative.CoResidency.MinOverlap = -time.Nanosecond
	for _, tc := range []struct {
		name string
		seed extract.Seed
		w    interval.Window
		o    extract.Options
	}{
		{"a pod as the service", extract.Exploratory{Service: PS}, w30, extract.Options{}},
		{"a nil seed", nil, w30, extract.Options{}},
		{"a zero seed", extract.Exploratory{}, w30, extract.Options{}},
		{"a seed by pointer", &extract.Exploratory{Service: S}, w30, extract.Options{}},
		{"a node as the host", extract.Anchored{Service: S, Host: N, At: hm(10, 15)}, w30, extract.Options{}},
		{"At equal to To", extract.Anchored{Service: S, Host: H, At: hm(10, 30)}, w30, extract.Options{}},
		{"At before From", extract.Anchored{Service: S, Host: H, At: hm(9, 59)}, w30, extract.Options{}},
		{"anchored and NodeOnly", extract.Anchored{Service: S, Host: H, At: hm(10, 15)}, w30, extract.Options{NodeOnly: true}},
		{"From == To", extract.Exploratory{Service: S}, win(hm(10, 0), hm(10, 0)), extract.Options{}},
		{"From > To", extract.Exploratory{Service: S}, win(hm(10, 1), hm(10, 0)), extract.Options{}},
		{"UbiquityNum > UbiquityDen", extract.Exploratory{Service: S}, w30, extract.Options{Policy: over}},
		{"MaxSeedPods 0", extract.Exploratory{Service: S}, w30, extract.Options{Policy: zeroPods}},
		{"a negative co-residency MinOverlap", extract.Exploratory{Service: S}, w30, extract.Options{Policy: negative}},
	} {
		res, err := e.Extract(ctx, tc.seed, tc.w, tc.o)
		if !errors.Is(err, store.ErrInvalid) || res.Token != 0 || res.Seed != nil {
			t.Errorf("%s: %v", tc.name, err)
		}
	}
	if st.calls != 0 {
		t.Errorf("%d store calls for invalid arguments", st.calls)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := e.Extract(cancelled, extract.Exploratory{Service: S}, w30, extract.Options{}); !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled: %v", err)
	}
	if st.calls != 0 {
		t.Errorf("%d store calls for a cancelled context", st.calls)
	}
}

func TestScenarioQLCaps(t *testing.T) {
	st, f := world(t, memstore.Options{}, S, N)
	for k := byte(1); k <= 5; k++ {
		f.write(obs(ent(inst(k)), hm(9, 0)), obs(ent(pod(k)), hm(9, 0)))
		service(f, S, inst(k), pod(k))
	}
	pol := extract.DefaultPolicy()
	pol.MaxSeedPods = 3
	res := run(t, st, extract.Exploratory{Service: S}, w30, extract.Options{Policy: pol})
	if !slices.Equal(seedsOf(res), []identity.Fingerprint{pod(1), pod(2), pod(3)}) ||
		!slices.Equal(res.Truncated.SeedPods, []identity.Fingerprint{pod(4), pod(5)}) {
		t.Errorf("finds %v, truncated %v", seedsOf(res), res.Truncated.SeedPods)
	}
	pol = extract.DefaultPolicy()
	pol.MaxInstances = 2
	res = run(t, st, extract.Exploratory{Service: S}, w30, extract.Options{Policy: pol})
	if len(res.Members[0].Instances) != 2 || !slices.Equal(res.Truncated.Instances, []identity.Fingerprint{inst(3), inst(4), inst(5)}) {
		t.Errorf("instances %v, truncated %v", describeMembers(res.Members), res.Truncated.Instances)
	}

	// Anchored, every instance used is listed: the cap leaves out inst 3 to 5,
	// but the host side finds inst 3 and 4 on the anchor, and only inst 5,
	// whose pod is elsewhere, stays truncated.
	f.write(obs(ent(H), hm(9, 0)), obs(runsOn(N, H), hm(9, 0)))
	for k := byte(1); k <= 4; k++ {
		f.write(obs(sched(pod(k), N), hm(9, 0)))
	}
	res = run(t, st, extract.Anchored{Service: S, Host: H, At: hm(10, 0)}, w30, extract.Options{Policy: pol})
	if len(res.Members[0].Instances) != 4 || !slices.Equal(res.Truncated.Instances, []identity.Fingerprint{inst(5)}) || len(res.PodResults) != 4 {
		t.Errorf("anchored: instances %v, truncated %v, finds %v", describeMembers(res.Members), res.Truncated.Instances, seedsOf(res))
	}
}

func TestScenarioQMAnInstanceOnTwoPods(t *testing.T) {
	PS1, PS2 := pod(1), pod(4)
	st, f := world(t, memstore.Options{}, S, T, IS, IT, PS1, PS2, PT, N)
	service(f, S, IS, PS1)
	service(f, T, IT, PT)
	f.write(obs(partOf(IS, PS2), hm(10, 10)), del(partOf(IS, PS2), hm(10, 20)),
		obs(sched(PS1, N), hm(9, 0)), obs(sched(PT, N), hm(9, 0)))
	res := run(t, st, extract.Exploratory{Service: S}, w30, extract.Options{NodeOnly: true})
	expect(t, "unattributed", describeUnattributed(res.Unattributed),
		ua(identity.Fingerprint{}, IS, "several_pods", []identity.Fingerprint{PS1, PS2}, sp(hm(10, 10), hm(10, 20))))
	if !slices.Equal(seedsOf(res), []identity.Fingerprint{PS1}) {
		t.Errorf("seed pods %v", seedsOf(res))
	}
	expect(t, "pairs", describePairs(res.Pairs, false),
		pl(extract.SharedNode, S, T, interval.ClassFlapping, 2, coresidency.ReasonNone, none,
			[]string{via(N, sp(hm(10, 0), hm(10, 10)), sp(hm(10, 20), hm(10, 30)))}, 1,
			ps{hm(10, 0), hm(10, 10), true, []string{"c0:leg1:unattributed"}},
			ps{hm(10, 20), hm(10, 30), false, nil}))
}

func TestScenarioQNQuarantinedAnchor(t *testing.T) {
	st, f := world(t, memstore.Options{Policy: storetest.QuarantinePolicy()}, S, T, IS, IT, PS, PT, N)
	boot := func(id string, at time.Time) store.Record {
		r := observe(ent(H), agent, at, 15*time.Minute)
		r.Boot = id
		return r
	}
	f.write(boot("b1", hm(9, 0)), boot("b2", hm(9, 5)), boot("b1", hm(9, 10)))
	service(f, S, IS, PS)
	service(f, T, IT, PT)
	f.write(obs(runsOn(N, H), hm(9, 0)), obs(sched(PS, N), hm(9, 0)), obs(sched(PT, N), hm(9, 0)))
	res := run(t, st, extract.Anchored{Service: S, Host: H, At: hm(10, 15)}, w30, extract.Options{})
	if res.Anchor != extract.NotOnHost || len(res.Quarantined) != 1 || res.Quarantined[0].Subject != runsOn(N, H) ||
		!errors.Is(res.Quarantined[0].Uncascaded, store.ErrQuarantined) {
		t.Errorf("anchor %s, quarantined %v", res.Anchor, res.Quarantined)
	}
	// Exploratory, the Find lists the node as on a quarantined host, and the
	// shared node stands.
	res = run(t, st, extract.Exploratory{Service: S}, w30, extract.Options{})
	if len(res.Pairs) != 1 || res.Pairs[0].Peer != T {
		t.Errorf("exploratory: %v", describePairs(res.Pairs, false))
	}
	expect(t, "unattributed", describeUnattributed(res.Unattributed),
		ua(PS, N, "quarantined_host", []identity.Fingerprint{H}, sp(hm(10, 0), hm(10, 30))))
}

func TestScenarioTwoMembersInOnePodAndThroughAHost(t *testing.T) {
	// S and its callee C run in one pod; T runs on a second node of the host.
	N2 := node(2)
	st, f := world(t, memstore.Options{}, S, C, T, IS, IC, IT, PS, PT, N, N2, H)
	service(f, S, IS, PS)
	service(f, C, IC, PS)
	service(f, T, IT, PT)
	f.write(obs(dependsOn(S, C), hm(9, 0)), obs(runsOn(N, H), hm(9, 0)), obs(runsOn(N2, H), hm(9, 0)),
		obs(sched(PS, N), hm(9, 0)), obs(sched(PT, N2), hm(10, 10)), del(sched(PT, N2), hm(10, 20)))
	res := run(t, st, extract.Exploratory{Service: S}, w30, extract.Options{})
	hostEnd := []string{"c0:leg6:pod(2,producer)", "c1:leg6:pod(2,producer)"}
	expect(t, "pairs", describePairs(res.Pairs, false),
		pl(extract.SharedPod, S, C, interval.ClassStable, 0, coresidency.ReasonNone, none,
			[]string{via(PS, sp(hm(10, 0), hm(10, 30)))}, 2, ps{hm(10, 0), hm(10, 30), true, nil}),
		pl(extract.SharedHost, S, T, interval.ClassTransient, 2, coresidency.ReasonNone, none,
			[]string{via(H, sp(hm(10, 10), hm(10, 20)))}, 1, ps{hm(10, 10), hm(10, 20), false, hostEnd[:1]}),
		pl(extract.SharedHost, C, T, interval.ClassTransient, 2, coresidency.ReasonNone, none,
			[]string{via(H, sp(hm(10, 10), hm(10, 20)))}, 1, ps{hm(10, 10), hm(10, 20), false, hostEnd[:1]}))
	if !slices.Equal(seedsOf(res), []identity.Fingerprint{PS}) {
		t.Errorf("seed pods %v", seedsOf(res))
	}
	if r := res.Reads; r.Calls != 2+1+1+6+1+1 {
		t.Errorf("reads %+v", r)
	}
	_ = lifecycle.EndProducer
	_ = catalog.Host
}

func TestScenarioNoPairOfAServiceWithItself(t *testing.T) {
	IS2, PS2 := inst(2), pod(2)
	st, f := world(t, memstore.Options{}, S, IS, IS2, PS, PS2, N)
	service(f, S, IS, PS)
	service(f, S, IS2, PS2)
	f.write(obs(sched(PS, N), hm(9, 0)), obs(sched(PS2, N), hm(9, 0)))
	res := run(t, st, extract.Exploratory{Service: S}, w30, extract.Options{NodeOnly: true})
	if len(res.PodResults) != 2 || len(res.PodResults[0].CoResidents) != 1 {
		t.Fatalf("finds %v", seedsOf(res))
	}
	expect(t, "pairs", describePairs(res.Pairs, false))
	expect(t, "unmapped", describeUnmapped(res.Unmapped))
}

func TestBudgetConstants(t *testing.T) {
	if extract.BudgetAnchoredExistence != extract.BudgetAnchorMapping+7*coresidency.BudgetReferenceExistence ||
		extract.BudgetExploratoryExistence != extract.DefaultPolicy().MaxSeedPods*extract.BudgetPerSeedPod ||
		extract.BudgetWorkFactor != coresidency.BudgetWorkFactor || extract.BudgetPerSeedPod < 4*coresidency.BudgetReferenceExistence {
		t.Error("the budget constants no longer add up; a change needs a new dated comment")
	}
}

func TestScenarioPairRanking(t *testing.T) {
	// S's pod on N, T's on N, U's on N2, both nodes on H, and Z's instance in
	// S's pod, each over its own span.
	U, Z, IU, instZ, PU, N2 := svc(6), svc(5), inst(6), inst(5), pod(6), node(2)
	setup := func(t *testing.T, tSpan, uSpan, zSpan interval.Span) *memstore.Store {
		st, f := world(t, memstore.Options{}, S, T, U, Z, IS, IT, IU, instZ, PS, PT, PU, N, N2, H)
		service(f, S, IS, PS)
		service(f, T, IT, PT)
		service(f, U, IU, PU)
		f.write(obs(instOf(instZ, Z), hm(9, 0)), obs(runsOn(N, H), hm(9, 0)), obs(runsOn(N2, H), hm(9, 0)), obs(sched(PS, N), hm(9, 0)))
		for _, e := range []struct {
			sub  store.Subject
			span interval.Span
		}{{sched(PT, N), tSpan}, {sched(PU, N2), uSpan}, {partOf(instZ, PS), zSpan}} {
			f.write(obs(e.sub, e.span.Start))
			if e.span.End.Before(w30.To) {
				f.write(del(e.sub, e.span.End))
			}
		}
		return st
	}
	order := func(res extract.Result) []string {
		var out []string
		for _, p := range res.Pairs {
			out = append(out, fmt.Sprintf("%s %s %s", p.Peer, p.Mechanism, p.Duration))
		}
		return out
	}

	t.Run("duration first", func(t *testing.T) {
		// A one-second shared node and a one-second shared pod against a
		// thirty-minute shared host: the host ranks first, and is what MaxPairs 1
		// keeps.
		sec := hm(10, 5).Add(time.Second)
		st := setup(t, sp(hm(10, 5), sec), sp(hm(9, 0), w30.To), sp(hm(10, 5), sec))
		res := run(t, st, extract.Exploratory{Service: S}, w30, extract.Options{})
		expect(t, "pairs", order(res),
			fmt.Sprintf("%s shared_host 30m0s", U), fmt.Sprintf("%s shared_pod 1s", Z), fmt.Sprintf("%s shared_node 1s", T))
		pol := extract.DefaultPolicy()
		pol.MaxPairs = 1
		res = run(t, st, extract.Exploratory{Service: S}, w30, extract.Options{Policy: pol})
		expect(t, "pairs, MaxPairs 1", order(res), fmt.Sprintf("%s shared_host 30m0s", U))
		if res.Truncated.Pairs != 2 {
			t.Errorf("truncated %+v", res.Truncated)
		}
	})

	t.Run("equal durations", func(t *testing.T) {
		// Ten minutes each: the mechanism breaks the tie, shared pod, then shared
		// node, then shared host.
		ten := sp(hm(10, 10), hm(10, 20))
		res := run(t, setup(t, ten, ten, ten), extract.Exploratory{Service: S}, w30, extract.Options{})
		expect(t, "pairs", order(res),
			fmt.Sprintf("%s shared_pod 10m0s", Z), fmt.Sprintf("%s shared_node 10m0s", T), fmt.Sprintf("%s shared_host 10m0s", U))
	})
}

func TestScenarioAMemberIsNeverUbiquitous(t *testing.T) {
	// S on six nodes, its callee D and a stranger E beside every pod of S, R on
	// one node. E is ubiquitous; D, a member, is not judged, and {S, D} stays.
	D, R, E := svc(6), svc(7), svc(8)
	st := scenarioQG(t, 6, 6)
	f := newFeed(t, st)
	f.write(obs(ent(E), hm(9, 0)), obs(dependsOn(S, D), hm(9, 0)))
	for k := byte(1); k <= 6; k++ {
		i, p := inst(50+k), pod(50+k)
		f.write(obs(ent(i), hm(9, 0)), obs(ent(p), hm(9, 0)))
		service(f, E, i, p)
		f.write(obs(sched(p, node(k)), hm(9, 0)))
	}
	res := run(t, st, extract.Exploratory{Service: S}, w30, extract.Options{NodeOnly: true})
	lines := func(ps []extract.Pair) []string {
		var out []string
		for _, p := range ps {
			out = append(out, fmt.Sprintf("%s %s %s %v", p.Member, p.Peer, p.Reason, p.Coverage))
		}
		return out
	}
	expect(t, "pairs", lines(res.Pairs),
		fmt.Sprintf("%s %s none [0 0]", S, D), fmt.Sprintf("%s %s none [1 6]", S, R), fmt.Sprintf("%s %s none [1 6]", D, R))
	expect(t, "suppressed", lines(res.Suppressed),
		fmt.Sprintf("%s %s ubiquitous [6 6]", S, E), fmt.Sprintf("%s %s ubiquitous [6 6]", D, E))
	if res.Ubiquity != (extract.Ubiquity{Judged: true, FrontierNodes: 6}) {
		t.Errorf("ubiquity %+v", res.Ubiquity)
	}
}
