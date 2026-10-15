package coresidency_test

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
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

// The scenarios fix exact answers. Times are on 2026-01-01 UTC, written hh:mm.

func hm(h, m int) time.Time { return time.Date(2026, 1, 1, h, m, 0, 0, time.UTC) }

func win(from, to time.Time) interval.Window {
	return interval.Window{From: from, To: to, AsOf: store.Latest}
}

var w30 = win(hm(10, 0), hm(10, 30))

// ov is an expected overlap: its span, Before, each leg's producers, and its
// ends written as describe writes them.
type ov struct {
	s, e   time.Time
	before bool
	prods  [][]lifecycle.Producer
	ends   []string
}

var (
	sk = []lifecycle.Producer{k8s}
	sa = []lifecycle.Producer{agent}
)

// legs is each leg's producers, in leg order.
func legs(prods ...[]lifecycle.Producer) [][]lifecycle.Producer { return prods }

// line is an expected entry in describe's format.
func line(mech coresidency.Mechanism, peer identity.Fingerprint, nodes []identity.Fingerprint, via identity.Fingerprint,
	legs []store.Subject, d time.Duration, class interval.Class, transitions int, reason coresidency.Reason, ovs ...ov,
) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s peer=%s nodes=%v via=%s legs=[", mech, peer, nodes, via)
	for i, l := range legs {
		if i > 0 {
			b.WriteString(" ")
		}
		fmt.Fprintf(&b, "%s>%s", l.A, l.B)
	}
	fmt.Fprintf(&b, "] %s %s transitions=%d reason=%s", d, class, transitions, reason)
	for _, o := range ovs {
		fmt.Fprintf(&b, " {[%s, %s) before=%t", ts(o.s), ts(o.e), o.before)
		for i, p := range o.prods {
			fmt.Fprintf(&b, " leg%d%v", i, p)
		}
		for _, e := range o.ends {
			fmt.Fprintf(&b, " %s", e)
		}
		b.WriteString("}")
	}
	return b.String()
}

func shared(peer, n identity.Fingerprint, seed identity.Fingerprint, d time.Duration, class interval.Class, tr int, ovs ...ov) string {
	return line(coresidency.SharedNode, peer, []identity.Fingerprint{n}, n,
		[]store.Subject{sched(seed, n), sched(peer, n)}, d, class, tr, coresidency.ReasonNone, ovs...)
}

func viaHost(peer, n1, n2, h, seed identity.Fingerprint, d time.Duration, class interval.Class, tr int, ovs ...ov) string {
	return line(coresidency.SharedHost, peer, []identity.Fingerprint{n1, n2}, h,
		[]store.Subject{sched(seed, n1), runsOn(n1, h), sched(peer, n2), runsOn(n2, h)}, d, class, tr, coresidency.ReasonNone, ovs...)
}

func unattr(subject identity.Fingerprint, why coresidency.UnattributedReason, cands []identity.Fingerprint, ss ...interval.Span) string {
	return fmt.Sprintf("%s %s %s %v", subject, why, spansString(ss), cands)
}

func hostLine(n, h identity.Fingerprint, ss ...interval.Span) string {
	return fmt.Sprintf("%s %s %s", n, h, spansString(ss))
}

func sp(a, b time.Time) interval.Span { return interval.Span{Start: a, End: b} }

func find(t *testing.T, st store.Store, seed identity.Fingerprint, w interval.Window, o coresidency.Options) coresidency.Result {
	t.Helper()
	res, err := coresidency.NewFinder(st).Find(ctx, seed, w, o)
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

func peers(cs []coresidency.CoResident) []identity.Fingerprint {
	var out []identity.Fingerprint
	for _, c := range cs {
		out = append(out, c.Peer)
	}
	return out
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

var S, A, B, N = pod(1), pod(2), pod(3), node(1)

// scenarioA writes the plan's two pods that never coexisted, with A's Delete at
// aDelete.
func scenarioA(t *testing.T, aDelete time.Time) *memstore.Store {
	st, f := world(t, memstore.Options{}, S, A, B, N)
	f.write(
		observe(sched(S, N), k8s, hm(9, 0), 0),
		observe(sched(A, N), k8s, hm(9, 58), 0), deleted(sched(A, N), k8s, aDelete),
		observe(sched(B, N), k8s, hm(10, 6), 0),
	)
	return st
}

func TestScenarioATwoPodsThatNeverCoexisted(t *testing.T) {
	st := scenarioA(t, hm(10, 5))
	res := find(t, st, S, w30, coresidency.Options{})
	expect(t, "Find(S)", describeAll(res.CoResidents),
		shared(B, N, S, 24*time.Minute, interval.ClassAppeared, 1, ov{hm(10, 6), hm(10, 30), false, legs(sk, sk), nil}),
		shared(A, N, S, 5*time.Minute, interval.ClassDisappeared, 1, ov{hm(10, 0), hm(10, 5), true, legs(sk, sk), []string{"end(1,producer)"}}),
	)
	expect(t, "Find(S) suppressed", describeAll(res.Suppressed))
	expect(t, "Find(S) unattributed", describeUnattributed(res.Unattributed), unattr(N, coresidency.NoHost, nil, sp(hm(10, 0), hm(10, 30))))
	expectReads(t, res.Reads, 3, 4, 6, 6)
	if res.PolicyVersion != 1 || res.IntervalPolicy != interval.PolicyVersion || res.Token != st.LastSeq() || res.Seed != S {
		t.Errorf("header %+v", res)
	}
	if got := coresidency.At(res.CoResidents, hm(10, 5)); len(got) != 0 {
		t.Errorf("At(10:05) = %v", peers(got))
	}
	if got := peers(coresidency.At(res.CoResidents, hm(10, 4))); !slices.Equal(got, []identity.Fingerprint{A}) {
		t.Errorf("At(10:04) = %v", got)
	}
	if got := peers(coresidency.At(res.CoResidents, hm(10, 6))); !slices.Equal(got, []identity.Fingerprint{B}) {
		t.Errorf("At(10:06) = %v", got)
	}
	if _, err := coresidency.AtChecked(res, hm(10, 30)); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("AtChecked(To) = %v", err)
	}
	if got := coresidency.Intersection(res.CoResidents); len(got) != 0 {
		t.Errorf("Intersection = %v", peers(got))
	}

	res = find(t, st, A, w30, coresidency.Options{})
	expect(t, "Find(A)", describeAll(res.CoResidents),
		shared(S, N, A, 5*time.Minute, interval.ClassDisappeared, 1, ov{hm(10, 0), hm(10, 5), true, legs(sk, sk), []string{"end(0,producer)"}}))
	expect(t, "Find(A) suppressed", describeAll(res.Suppressed))
	expect(t, "Find(A) unattributed", describeUnattributed(res.Unattributed), unattr(N, coresidency.NoHost, nil, sp(hm(10, 0), hm(10, 5))))

	res = find(t, st, B, w30, coresidency.Options{})
	expect(t, "Find(B)", describeAll(res.CoResidents),
		shared(S, N, B, 24*time.Minute, interval.ClassAppeared, 1, ov{hm(10, 6), hm(10, 30), false, legs(sk, sk), nil}))
}

func TestScenarioATouchingAndTheMinimumOverlap(t *testing.T) {
	t.Run("touching", func(t *testing.T) {
		st := scenarioA(t, hm(10, 6))
		if got := find(t, st, A, w30, coresidency.Options{}); slices.Contains(peers(got.CoResidents), B) || len(got.Suppressed) != 0 {
			t.Errorf("Find(A) = %v, suppressed %v", describeAll(got.CoResidents), describeAll(got.Suppressed))
		}
		if got := find(t, st, B, w30, coresidency.Options{}); slices.Contains(peers(got.CoResidents), A) || len(got.Suppressed) != 0 {
			t.Errorf("Find(B) = %v, suppressed %v", describeAll(got.CoResidents), describeAll(got.Suppressed))
		}
	})
	oneNs := hm(10, 6).Add(time.Nanosecond)
	t.Run("one nanosecond", func(t *testing.T) {
		st := scenarioA(t, oneNs)
		res := find(t, st, A, w30, coresidency.Options{})
		expect(t, "suppressed", describeAll(res.Suppressed), line(coresidency.SharedNode, B, []identity.Fingerprint{N}, N,
			[]store.Subject{sched(A, N), sched(B, N)}, time.Nanosecond, interval.ClassTransient, 2, coresidency.BelowMinOverlap,
			ov{hm(10, 6), oneNs, false, legs(sk, sk), []string{"end(0,producer)"}}))
		if slices.Contains(peers(res.CoResidents), B) {
			t.Error("B is a co-resident")
		}
		pol := coresidency.DefaultPolicy()
		pol.MinOverlap = 0
		res = find(t, st, A, w30, coresidency.Options{Policy: pol})
		if !slices.Contains(peers(res.CoResidents), B) || len(res.Suppressed) != 0 {
			t.Errorf("MinOverlap 0: %v", describeAll(res.CoResidents))
		}
	})
	for _, tc := range []struct {
		end  time.Time
		kept bool
	}{{hm(10, 6).Add(time.Second), true}, {hm(10, 6).Add(time.Second - time.Nanosecond), false}} {
		st := scenarioA(t, tc.end)
		res := find(t, st, A, w30, coresidency.Options{})
		if got := slices.Contains(peers(res.CoResidents), B); got != tc.kept || slices.Contains(peers(res.Suppressed), B) == tc.kept {
			t.Errorf("Delete at %s: kept %t, want %t", ts(tc.end), got, tc.kept)
		}
	}
}

func TestScenarioBOpenWindowEdgesAreNeverSuppressed(t *testing.T) {
	st := scenarioA(t, hm(10, 5))
	half := 500 * time.Millisecond
	res := find(t, st, S, win(hm(10, 0), hm(10, 6).Add(half)), coresidency.Options{})
	expect(t, "short window, B", describeAll(res.CoResidents),
		shared(A, N, S, 5*time.Minute, interval.ClassDisappeared, 1, ov{hm(10, 0), hm(10, 5), true, legs(sk, sk), []string{"end(1,producer)"}}),
		shared(B, N, S, half, interval.ClassAppeared, 1, ov{hm(10, 6), hm(10, 6).Add(half), false, legs(sk, sk), nil}),
	)
	res = find(t, st, S, win(hm(10, 5).Add(-half), hm(10, 30)), coresidency.Options{})
	expect(t, "late window, A", describeAll(res.CoResidents),
		shared(B, N, S, 24*time.Minute, interval.ClassAppeared, 1, ov{hm(10, 6), hm(10, 30), false, legs(sk, sk), nil}),
		shared(A, N, S, half, interval.ClassDisappeared, 1, ov{hm(10, 5).Add(-half), hm(10, 5), true, legs(sk, sk), []string{"end(1,producer)"}}),
	)

	st2, f := world(t, memstore.Options{}, S, A, B, N)
	f.write(
		observe(sched(S, N), k8s, hm(9, 0), 0),
		observe(sched(A, N), k8s, hm(9, 58), 0), deleted(sched(A, N), k8s, hm(10, 5)),
		observe(sched(B, N), k8s, hm(10, 10), 0), deleted(sched(B, N), k8s, hm(10, 10).Add(half)),
	)
	res = find(t, st2, S, w30, coresidency.Options{})
	if !slices.Equal(peers(res.Suppressed), []identity.Fingerprint{B}) || slices.Contains(peers(res.CoResidents), B) {
		t.Errorf("B inside the window: kept %v, suppressed %v", describeAll(res.CoResidents), describeAll(res.Suppressed))
	}
}

func TestScenarioCTheCascadeAndTiesOfEnds(t *testing.T) {
	X, Y := pod(4), pod(5)
	xEnt := run(ent(X), k8s, hm(9, 50), 15*time.Minute, hm(10, 5))
	xLine := shared(X, N, S, 20*time.Minute, interval.ClassDisappeared, 1, ov{hm(10, 0), hm(10, 20), true, legs(sk, sk), []string{"end(1,cascade)"}})
	t.Run("C", func(t *testing.T) {
		st, f := world(t, memstore.Options{}, S, N)
		f.write(xEnt, observe(sched(S, N), k8s, hm(9, 0), 0), observe(sched(X, N), k8s, hm(9, 0), 0))
		res := find(t, st, S, w30, coresidency.Options{})
		expect(t, "Find(S)", describeAll(res.CoResidents), xLine)
	})
	t.Run("C′", func(t *testing.T) {
		st, f := world(t, memstore.Options{}, S, Y)
		f.write(xEnt, run(ent(N), k8s, hm(9, 0), 30*time.Minute, hm(9, 55)),
			observe(sched(S, N), k8s, hm(9, 0), 0), observe(sched(X, N), k8s, hm(9, 0), 0), observe(sched(Y, N), k8s, hm(9, 0), 0))
		res := find(t, st, S, w30, coresidency.Options{})
		if len(res.Placement) != 1 || len(res.Placement[0].Pieces) != 1 || !res.Placement[0].Pieces[0].End.Equal(hm(10, 25)) ||
			res.Placement[0].Pieces[0].EndSource != lifecycle.EndCascade {
			t.Errorf("Placement = %+v", res.Placement)
		}
		expect(t, "Find(S)", describeAll(res.CoResidents),
			shared(Y, N, S, 25*time.Minute, interval.ClassDisappeared, 1, ov{hm(10, 0), hm(10, 25), true, legs(sk, sk), []string{"end(0,cascade)", "end(1,cascade)"}}),
			xLine)
		expect(t, "unattributed", describeUnattributed(res.Unattributed), unattr(N, coresidency.NoHost, nil, sp(hm(10, 0), hm(10, 25))))
	})
}

func TestScenarioDTwoProducersOnOneLeg(t *testing.T) {
	X := pod(4)
	both := []lifecycle.Producer{k8s, "kubelet"}
	for _, tc := range []struct {
		name    string
		kubelet time.Time
		want    string
	}{
		{"seamless", hm(10, 0), shared(X, N, S, 5*time.Minute, interval.ClassDisappeared, 1,
			ov{hm(10, 0), hm(10, 5), true, legs(sk, both), []string{"end(1,liveness_expiry)"}})},
		{"one nanosecond gap", hm(10, 2).Add(time.Nanosecond), shared(X, N, S, 7*time.Minute, interval.ClassFlapping, 3,
			ov{hm(10, 0), hm(10, 2), true, legs(sk, sk), []string{"end(1,producer)"}},
			ov{hm(10, 2).Add(time.Nanosecond), hm(10, 7).Add(time.Nanosecond), false, legs(sk, []lifecycle.Producer{"kubelet"}), []string{"end(1,liveness_expiry)"}})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st, f := world(t, memstore.Options{}, S, X, N)
			f.write(observe(sched(S, N), k8s, hm(9, 0), 0),
				observe(sched(X, N), k8s, hm(9, 0), 0), deleted(sched(X, N), k8s, hm(10, 2)),
				observe(sched(X, N), "kubelet", tc.kubelet, 5*time.Minute))
			expect(t, "Find(S)", describeAll(find(t, st, S, w30, coresidency.Options{}).CoResidents), tc.want)
		})
	}
}

// scenarioE writes one host, two nodes on it and three pods.
func scenarioE(t *testing.T, opts memstore.Options, withRunsOn bool) (*memstore.Store, [6]identity.Fingerprint) {
	H, N1, N2, Z, Y := host(1), node(1), node(2), pod(2), pod(3)
	st, f := world(t, opts, H, N1, N2, S, Z, Y)
	if withRunsOn {
		f.write(observe(runsOn(N1, H), agent, hm(9, 0), 0), observe(runsOn(N2, H), agent, hm(9, 0), 0))
	}
	f.write(observe(sched(S, N1), k8s, hm(9, 0), 0), observe(sched(Z, N1), k8s, hm(9, 0), 0),
		observe(sched(Y, N2), k8s, hm(10, 10), 0), deleted(sched(Y, N2), k8s, hm(10, 20)))
	return st, [6]identity.Fingerprint{H, N1, N2, S, Z, Y}
}

func TestScenarioESharedHostThroughTwoNodes(t *testing.T) {
	st, e := scenarioE(t, memstore.Options{}, true)
	H, N1, N2, Z, Y := e[0], e[1], e[2], e[4], e[5]
	zLine := shared(Z, N1, S, 30*time.Minute, interval.ClassStable, 0, ov{hm(10, 0), hm(10, 30), true, legs(sk, sk), nil})
	res := find(t, st, S, w30, coresidency.Options{})
	expect(t, "Find(S)", describeAll(res.CoResidents), zLine,
		viaHost(Y, N1, N2, H, S, 10*time.Minute, interval.ClassTransient, 2,
			ov{hm(10, 10), hm(10, 20), false, legs(sk, sa, sk, sa), []string{"end(2,producer)"}}))
	expect(t, "unattributed", describeUnattributed(res.Unattributed))
	expect(t, "seed hosts", describeSeedHosts(res.SeedHosts), hostLine(N1, H, sp(hm(10, 0), hm(10, 30))))
	expectReads(t, res.Reads, 6, 8, 14, 14)
	if got := coresidency.Intersection(res.CoResidents); len(got) != 1 || got[0].Peer != Z {
		t.Errorf("Intersection = %v", peers(got))
	}

	st, _ = scenarioE(t, memstore.Options{}, false)
	res = find(t, st, S, w30, coresidency.Options{})
	expect(t, "Find(S) without runs_on", describeAll(res.CoResidents), zLine)
	expect(t, "unattributed", describeUnattributed(res.Unattributed), unattr(N1, coresidency.NoHost, nil, sp(hm(10, 0), hm(10, 30))))
	expect(t, "seed hosts without runs_on", describeSeedHosts(res.SeedHosts))
	expectReads(t, res.Reads, 3, 4, 5, 5)
}

func TestScenarioENodeBetweenTwoHosts(t *testing.T) {
	H, H2, N1, N2, N3 := host(1), host(2), node(1), node(2), node(3)
	Y2, Y := pod(2), pod(3)
	st, f := world(t, memstore.Options{}, H, H2, N1, N2, N3, S, Y, Y2)
	f.write(
		observe(runsOn(N1, H), agent, hm(9, 55), 20*time.Minute), observe(runsOn(N1, H2), agent, hm(10, 10), 0),
		observe(runsOn(N2, H2), agent, hm(9, 0), 0), observe(runsOn(N3, H), agent, hm(9, 0), 0),
		observe(sched(S, N1), k8s, hm(9, 0), 0),
		observe(sched(Y, N2), k8s, hm(10, 5), 0), deleted(sched(Y, N2), k8s, hm(10, 25)),
		observe(sched(Y2, N3), k8s, hm(9, 0), 0),
	)
	res := find(t, st, S, w30, coresidency.Options{})
	expect(t, "Find(S)", describeAll(res.CoResidents),
		viaHost(Y2, N1, N3, H, S, 10*time.Minute, interval.ClassDisappeared, 1,
			ov{hm(10, 0), hm(10, 10), true, legs(sk, sa, sk, sa), []string{"end(1,unattributed)"}}),
		viaHost(Y, N1, N2, H2, S, 10*time.Minute, interval.ClassTransient, 2,
			ov{hm(10, 15), hm(10, 25), false, legs(sk, sa, sk, sa), []string{"end(2,producer)"}}),
	)
	expect(t, "unattributed", describeUnattributed(res.Unattributed),
		unattr(N1, coresidency.SeveralHosts, []identity.Fingerprint{H, H2}, sp(hm(10, 10), hm(10, 15))))
	// The instants with two hosts are in neither entry.
	wantHosts := []string{hostLine(N1, H, sp(hm(10, 0), hm(10, 10))), hostLine(N1, H2, sp(hm(10, 15), hm(10, 30)))}
	expect(t, "seed hosts", describeSeedHosts(res.SeedHosts), wantHosts...)
	expectReads(t, res.Reads, 6, 12, 20, 20)

	// MaxHosts bounds the hosts whose nodes are read, not the seed's hosts.
	pol := coresidency.DefaultPolicy()
	pol.MaxHosts = 1
	capped := find(t, st, S, w30, coresidency.Options{Policy: pol})
	if !slices.Equal(capped.Truncated.Hosts, []identity.Fingerprint{H}) {
		t.Errorf("Truncated.Hosts = %v, want [%s]", capped.Truncated.Hosts, H)
	}
	expect(t, "seed hosts, MaxHosts 1", describeSeedHosts(capped.SeedHosts), wantHosts...)
}

func TestScenarioFQuarantinedHost(t *testing.T) {
	H, N1, N2, Z, Y := host(1), node(1), node(2), pod(2), pod(3)
	st, f := world(t, memstore.Options{Policy: storetest.QuarantinePolicy()}, N1, N2, S, Z, Y)
	boot := func(id string, at time.Time) store.Record {
		r := observe(ent(H), agent, at, 15*time.Minute)
		r.Boot = id
		return r
	}
	f.write(boot("b1", hm(9, 0)), boot("b2", hm(9, 5)), boot("b1", hm(9, 10)),
		observe(runsOn(N1, H), agent, hm(9, 0), 0), observe(runsOn(N2, H), agent, hm(9, 0), 0),
		observe(sched(S, N1), k8s, hm(9, 0), 0), observe(sched(Z, N1), k8s, hm(9, 0), 0), observe(sched(Y, N2), k8s, hm(9, 0), 0))
	res := find(t, st, S, w30, coresidency.Options{})
	expect(t, "Find(S)", describeAll(res.CoResidents),
		shared(Z, N1, S, 30*time.Minute, interval.ClassStable, 0, ov{hm(10, 0), hm(10, 30), true, legs(sk, sk), nil}))
	if len(res.Quarantined) != 1 || res.Quarantined[0].Subject != runsOn(N1, H) || !errors.Is(res.Quarantined[0].Uncascaded, store.ErrQuarantined) {
		t.Errorf("Quarantined = %v", quarantinedSubjects(res.Quarantined))
	}
	expect(t, "unattributed", describeUnattributed(res.Unattributed),
		unattr(N1, coresidency.QuarantinedHost, []identity.Fingerprint{H}, sp(hm(10, 0), hm(10, 30))))
	expect(t, "seed hosts", describeSeedHosts(res.SeedHosts))
	expectReads(t, res.Reads, 3, 4, 7, 6)
}

func TestScenarioGTokenPinnedOnce(t *testing.T) {
	inner := scenarioA(t, hm(10, 5))
	want := find(t, inner, S, w30, coresidency.Options{})
	before := inner.LastSeq()
	late := newFeed(t, inner)
	st := &spy{Store: inner, onWindow: func(n int) {
		if n == 2 {
			late.write(deleted(sched(A, N), k8s, hm(10, 1)))
		}
	}}
	res := find(t, st, S, w30, coresidency.Options{})
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
		t.Errorf("%d writes through the finder's store", st.writes)
	}
}

func TestScenarioHRetention(t *testing.T) {
	retained := func(t *testing.T, res coresidency.Result, err error) {
		t.Helper()
		if !errors.Is(err, interval.ErrRetained) || !errors.Is(err, errors.ErrUnsupported) || res.Token != 0 || res.CoResidents != nil {
			t.Fatalf("%v, %+v; want ErrRetained with a zero result", err, res)
		}
	}
	t.Run("L2 retained", func(t *testing.T) {
		st, _ := scenarioE(t, memstore.Options{Keep: [4]bool{true, true, false, true}}, true)
		if err := st.Retain(ctx, hm(10, 0)); err != nil {
			t.Fatal(err)
		}
		res, err := coresidency.NewFinder(st).Find(ctx, S, w30, coresidency.Options{})
		retained(t, res, err)
	})
	t.Run("L1 retained", func(t *testing.T) {
		st, e := scenarioE(t, memstore.Options{Keep: [4]bool{true, false, true, true}}, true)
		if err := st.Retain(ctx, hm(10, 0)); err != nil {
			t.Fatal(err)
		}
		res, err := coresidency.NewFinder(st).Find(ctx, S, w30, coresidency.Options{})
		retained(t, res, err)
		res = find(t, st, S, w30, coresidency.Options{NodeOnly: true})
		expect(t, "NodeOnly", describeAll(res.CoResidents),
			shared(e[4], e[1], S, 30*time.Minute, interval.ClassStable, 0, ov{hm(10, 0), hm(10, 30), true, legs(sk, sk), nil}))
		expect(t, "NodeOnly unattributed", describeUnattributed(res.Unattributed))
		if res.Reads.Calls != 2 {
			t.Errorf("reads %+v, want 2 calls", res.Reads)
		}
	})
}

func TestScenarioIArguments(t *testing.T) {
	inner := scenarioA(t, hm(10, 5))
	st := &spy{Store: inner}
	f := coresidency.NewFinder(st)
	unknown, err := identity.FingerprintFromHash("x.unknown", [identity.FingerprintBytes]byte{1})
	if err != nil {
		t.Fatal(err)
	}
	negative, zeroVia := coresidency.DefaultPolicy(), coresidency.DefaultPolicy()
	negative.MinOverlap = -time.Nanosecond
	zeroVia.MaxPerVia = 0
	for _, tc := range []struct {
		name string
		seed identity.Fingerprint
		w    interval.Window
		o    coresidency.Options
	}{
		{"From == To", S, win(hm(10, 0), hm(10, 0)), coresidency.Options{}},
		{"From > To", S, win(hm(10, 1), hm(10, 0)), coresidency.Options{}},
		{"zero seed", identity.Fingerprint{}, w30, coresidency.Options{}},
		{"node seed", N, w30, coresidency.Options{}},
		{"unknown type", unknown, w30, coresidency.Options{}},
		{"negative MinOverlap", S, w30, coresidency.Options{Policy: negative}},
		{"MaxPerVia 0", S, w30, coresidency.Options{Policy: zeroVia}},
	} {
		res, err := f.Find(ctx, tc.seed, tc.w, tc.o)
		if !errors.Is(err, store.ErrInvalid) || res.Token != 0 || res.Seed != (identity.Fingerprint{}) {
			t.Errorf("%s: %v", tc.name, err)
		}
	}
	if st.calls != 0 {
		t.Errorf("%d store calls for invalid arguments", st.calls)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := f.Find(cancelled, S, w30, coresidency.Options{}); !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled: %v", err)
	}
	if st.calls != 0 {
		t.Errorf("%d store calls for a cancelled context", st.calls)
	}
	if err := inner.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Find(ctx, S, w30, coresidency.Options{}); !errors.Is(err, store.ErrClosed) {
		t.Errorf("closed: %v", err)
	}
}

func TestScenarioJTheHotNode(t *testing.T) {
	H := host(1)
	podN := func(i int) identity.Fingerprint {
		var h [identity.FingerprintBytes]byte
		h[0], h[1] = byte(i>>8), byte(i)
		fp, err := identity.FingerprintFromHash(catalog.K8sPod, h)
		if err != nil {
			t.Fatal(err)
		}
		return fp
	}
	st, f := world(t, memstore.Options{}, N, H)
	f.write(observe(runsOn(N, H), agent, hm(9, 0), 0))
	seed := podN(1)
	place := func(p identity.Fingerprint, at time.Time, end time.Time) {
		rs := []store.Record{observe(ent(p), k8s, hm(7, 0), 0), observe(sched(p, N), k8s, at, 0)}
		if !end.IsZero() {
			rs = append(rs, deleted(sched(p, N), k8s, end))
		}
		f.write(rs...)
	}
	for i := 1; i <= 110; i++ {
		var end time.Time
		if i > 90 {
			end = hm(10, 5+(i-91))
		}
		place(podN(i), hm(9, 0), end)
	}
	for i := range 20 {
		place(podN(200+i), hm(10, 1+i), time.Time{})
	}
	for i := range 40 {
		place(podN(300+i), hm(8, 0), hm(9, i))
	}
	res := find(t, st, seed, w30, coresidency.Options{NodeOnly: true})
	if len(res.CoResidents) != 129 || len(res.Suppressed) != 0 || fmt.Sprint(res.Truncated) != fmt.Sprint(coresidency.Truncation{}) {
		t.Errorf("%d co-residents, %d suppressed, truncated %+v", len(res.CoResidents), len(res.Suppressed), res.Truncated)
	}
	expectReads(t, res.Reads, 2, 2, 133, 133)
	withHosts := find(t, st, seed, w30, coresidency.Options{})
	if d := diffLines("with hosts", describeAll(withHosts.CoResidents), describeAll(res.CoResidents)); d != "" {
		t.Error(d)
	}
	expectReads(t, withHosts.Reads, 4, 5, 137, 137)
}

func TestScenarioKRankingAndTheBudget(t *testing.T) {
	P1, P2, P3, P4, P5 := pod(11), pod(12), pod(13), pod(14), pod(15)
	st, f := world(t, memstore.Options{}, S, N, P1, P2, P3, P4, P5)
	f.write(observe(sched(S, N), k8s, hm(9, 0), 0))
	for _, p := range []struct {
		fp       identity.Fingerprint
		from, to time.Time
	}{{P1, hm(10, 0), hm(10, 1)}, {P2, hm(10, 0), hm(10, 2)}, {P3, hm(10, 10), hm(10, 13)}, {P4, hm(10, 20), hm(10, 23)}, {P5, hm(10, 0), hm(10, 5)}} {
		f.write(observe(sched(p.fp, N), k8s, p.from, 0), deleted(sched(p.fp, N), k8s, p.to))
	}
	res := find(t, st, S, w30, coresidency.Options{})
	if got := peers(res.CoResidents); !slices.Equal(got, []identity.Fingerprint{P5, P3, P4, P2, P1}) {
		t.Errorf("order %v", got)
	}
	pol := coresidency.DefaultPolicy()
	pol.MaxPerVia = 3
	res = find(t, st, S, w30, coresidency.Options{Policy: pol})
	if got := peers(res.CoResidents); !slices.Equal(got, []identity.Fingerprint{P5, P3, P4}) ||
		fmt.Sprint(res.Truncated.PerVia) != fmt.Sprint([]coresidency.ViaCount{{Via: N, Mechanism: coresidency.SharedNode, Dropped: 2}}) {
		t.Errorf("MaxPerVia 3: %v, %+v", got, res.Truncated)
	}
	pol = coresidency.DefaultPolicy()
	pol.MaxResults = 2
	res = find(t, st, S, w30, coresidency.Options{Policy: pol})
	if got := peers(res.CoResidents); !slices.Equal(got, []identity.Fingerprint{P5, P3}) || res.Truncated.Results != 3 {
		t.Errorf("MaxResults 2: %v, %+v", got, res.Truncated)
	}
}

func TestScenarioLNoPlacementFromADescription(t *testing.T) {
	st := scenarioA(t, hm(10, 5))
	W := pod(9)
	r := observe(ent(W), k8s, hm(9, 0), 0)
	r.Payload = []byte(`{"node_name":"n"}`)
	newFeed(t, st).write(r)
	if got := find(t, st, S, w30, coresidency.Options{}); slices.Contains(peers(got.CoResidents), W) {
		t.Error("W is a co-resident of S")
	}
	res := find(t, st, W, w30, coresidency.Options{})
	if len(res.Placement) != 0 || len(res.CoResidents) != 0 || len(res.Suppressed) != 0 || len(res.Unattributed) != 0 {
		t.Errorf("Find(W) = %+v", res)
	}
	expectReads(t, res.Reads, 1, 1, 0, 0)
}

func TestScenarioMTheSeedOnTwoNodes(t *testing.T) {
	N1, N2, Z, Y := node(1), node(2), pod(2), pod(3)
	st, f := world(t, memstore.Options{}, N1, N2, S, Z, Y)
	f.write(observe(sched(S, N1), k8s, hm(9, 0), 0),
		observe(sched(S, N2), "k8scluster/default", hm(10, 10), 0), deleted(sched(S, N2), "k8scluster/default", hm(10, 20)),
		observe(sched(Z, N1), k8s, hm(9, 0), 0), observe(sched(Y, N2), k8s, hm(9, 0), 0))
	res := find(t, st, S, w30, coresidency.Options{})
	expect(t, "Find(S)", describeAll(res.CoResidents),
		shared(Z, N1, S, 20*time.Minute, interval.ClassFlapping, 2,
			ov{hm(10, 0), hm(10, 10), true, legs(sk, sk), []string{"end(0,unattributed)"}},
			ov{hm(10, 20), hm(10, 30), false, legs(sk, sk), nil}))
	if len(res.Placement) != 2 {
		t.Errorf("Placement has %d edges", len(res.Placement))
	}
	// Ordered by subject: a k8s.node sorts before a k8s.pod.
	expect(t, "unattributed", describeUnattributed(res.Unattributed),
		unattr(N1, coresidency.NoHost, nil, sp(hm(10, 0), hm(10, 10)), sp(hm(10, 20), hm(10, 30))),
		unattr(S, coresidency.SeveralNodes, []identity.Fingerprint{N1, N2}, sp(hm(10, 10), hm(10, 20))))
	expectReads(t, res.Reads, 3, 4, 6, 6)
}

func TestScenarioNSeedHosts(t *testing.T) {
	H, H2, N1, N2 := host(1), host(2), node(1), node(2)

	t.Run("one host, clipped to the window", func(t *testing.T) {
		st, f := world(t, memstore.Options{}, H, N1, S)
		f.write(observe(sched(S, N1), k8s, hm(9, 0), 0),
			observe(runsOn(N1, H), agent, hm(10, 10), 0), deleted(runsOn(N1, H), agent, hm(10, 20)))
		res := find(t, st, S, w30, coresidency.Options{})
		expect(t, "seed hosts", describeSeedHosts(res.SeedHosts), hostLine(N1, H, sp(hm(10, 10), hm(10, 20))))
		expect(t, "unattributed", describeUnattributed(res.Unattributed),
			unattr(N1, coresidency.NoHost, nil, sp(hm(10, 0), hm(10, 10)), sp(hm(10, 20), hm(10, 30))))

		// The seed's own time on the node bounds the spans too.
		st, f = world(t, memstore.Options{}, H, N1, S)
		f.write(observe(runsOn(N1, H), agent, hm(9, 0), 0),
			observe(sched(S, N1), k8s, hm(10, 5), 0), deleted(sched(S, N1), k8s, hm(10, 25)))
		res = find(t, st, S, w30, coresidency.Options{})
		expect(t, "seed hosts, seed on the node 10:05 to 10:25", describeSeedHosts(res.SeedHosts), hostLine(N1, H, sp(hm(10, 5), hm(10, 25))))
	})

	t.Run("no host, and no node", func(t *testing.T) {
		st, f := world(t, memstore.Options{}, N1, S, A)
		f.write(observe(sched(S, N1), k8s, hm(9, 0), 0))
		res := find(t, st, S, w30, coresidency.Options{})
		if res.SeedHosts != nil {
			t.Errorf("seed hosts %v, want none", describeSeedHosts(res.SeedHosts))
		}
		// A pod with no scheduled_on is on no node.
		res = find(t, st, A, w30, coresidency.Options{})
		if res.SeedHosts != nil {
			t.Errorf("seed hosts of a pod on no node: %v", describeSeedHosts(res.SeedHosts))
		}
	})

	t.Run("several hosts at once", func(t *testing.T) {
		st, f := world(t, memstore.Options{}, H, H2, N1, S)
		f.write(observe(sched(S, N1), k8s, hm(9, 0), 0),
			observe(runsOn(N1, H), agent, hm(9, 0), 0), observe(runsOn(N1, H2), agent, hm(9, 0), 0))
		res := find(t, st, S, w30, coresidency.Options{})
		if res.SeedHosts != nil {
			t.Errorf("seed hosts %v, want none", describeSeedHosts(res.SeedHosts))
		}
		expect(t, "unattributed", describeUnattributed(res.Unattributed),
			unattr(N1, coresidency.SeveralHosts, []identity.Fingerprint{H, H2}, sp(hm(10, 0), hm(10, 30))))
	})

	t.Run("node only reads no host", func(t *testing.T) {
		st, f := world(t, memstore.Options{}, H, N1, S)
		f.write(observe(sched(S, N1), k8s, hm(9, 0), 0), observe(runsOn(N1, H), agent, hm(9, 0), 0))
		if res := find(t, st, S, w30, coresidency.Options{NodeOnly: true}); res.SeedHosts != nil {
			t.Errorf("seed hosts %v, want none", describeSeedHosts(res.SeedHosts))
		}
		res := find(t, st, S, w30, coresidency.Options{})
		expect(t, "seed hosts", describeSeedHosts(res.SeedHosts), hostLine(N1, H, sp(hm(10, 0), hm(10, 30))))
	})

	t.Run("two nodes at once leave those instants out", func(t *testing.T) {
		st, f := world(t, memstore.Options{}, H, H2, N1, N2, S)
		f.write(observe(runsOn(N1, H), agent, hm(9, 0), 0), observe(runsOn(N2, H2), agent, hm(9, 0), 0),
			observe(sched(S, N1), k8s, hm(9, 0), 0), deleted(sched(S, N1), k8s, hm(10, 20)),
			observe(sched(S, N2), "k8scluster/default", hm(10, 10), 0))
		res := find(t, st, S, w30, coresidency.Options{})
		expect(t, "seed hosts", describeSeedHosts(res.SeedHosts),
			hostLine(N1, H, sp(hm(10, 0), hm(10, 10))),
			hostLine(N2, H2, sp(hm(10, 20), hm(10, 30))))
		expect(t, "unattributed", describeUnattributed(res.Unattributed),
			unattr(S, coresidency.SeveralNodes, []identity.Fingerprint{N1, N2}, sp(hm(10, 10), hm(10, 20))))
	})

	t.Run("two nodes on one host are two entries, by node", func(t *testing.T) {
		st, f := world(t, memstore.Options{}, H, N1, N2, S)
		f.write(observe(runsOn(N1, H), agent, hm(9, 0), 0), observe(runsOn(N2, H), agent, hm(9, 0), 0),
			observe(sched(S, N2), k8s, hm(9, 0), 0), deleted(sched(S, N2), k8s, hm(10, 10)),
			observe(sched(S, N1), k8s, hm(10, 10), 0))
		res := find(t, st, S, w30, coresidency.Options{})
		expect(t, "seed hosts", describeSeedHosts(res.SeedHosts),
			hostLine(N1, H, sp(hm(10, 10), hm(10, 30))),
			hostLine(N2, H, sp(hm(10, 0), hm(10, 10))))
		expect(t, "unattributed", describeUnattributed(res.Unattributed))
	})
}
