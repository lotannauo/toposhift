package extract_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/query/extract"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// scriptedCluster writes a fixed cluster with the cases the grid draws only
// sometimes: a callee that moves to another node, a caller that becomes a
// callee too, a sidecar, a pod with no instance, an instance on two pods and
// one of two services for a while, a node that moves from one host to another
// and one claiming two hosts at once, a service with a pod on every node, and
// an instance that expires. It returns the records with their Seq.
func scriptedCluster(t *testing.T) (*memstore.Store, []store.Record, []identity.Fingerprint) {
	A, B, D, E := svc(2), svc(3), svc(4), svc(5)
	H1, H2 := host(1), host(2)
	N1, N2, N3, N4 := node(1), node(2), node(3), node(4)
	IS1, IS2, IS3, IA1, IA2, IB1, IE1, IE2 := inst(1), inst(2), inst(3), inst(4), inst(5), inst(6), inst(7), inst(8)
	PS1, PS2, PS3, PA1, PA2, PB1, PE, PX := pod(1), pod(2), pod(3), pod(4), pod(5), pod(6), pod(7), pod(8)
	all := []identity.Fingerprint{S, A, B, D, E, H1, H2, N1, N2, N3, N4, IS1, IS2, IS3, IA1, IA2, IB1, IE1, IE2, PS1, PS2, PS3, PA1, PA2, PB1, PE, PX}
	var ds, pds []identity.Fingerprint
	for k := byte(1); k <= 4; k++ {
		ds, pds = append(ds, inst(20+k)), append(pds, pod(20+k))
	}
	all = append(append(all, ds...), pds...)
	st, f := world(t, memstore.Options{}, all...)
	var recs []store.Record
	w := func(rs ...store.Record) { recs = append(recs, f.write(rs...)...) }
	at := func(m int) time.Time { return hm(10, m) }
	for _, x := range []struct{ i, s, p identity.Fingerprint }{
		{IS1, S, PS1}, {IS2, S, PS2}, {IS3, S, PS1}, {IA1, A, PA1}, {IA2, A, PA2}, {IB1, B, PB1}, {IE1, E, PB1},
	} {
		w(obs(instOf(x.i, x.s), hm(9, 0)), obs(partOf(x.i, x.p), hm(9, 0)))
	}
	nodes := []identity.Fingerprint{N1, N2, N3, N4}
	for k := range 4 {
		w(obs(instOf(ds[k], D), hm(9, 0)), obs(partOf(ds[k], pds[k]), hm(9, 0)), obs(sched(pds[k], nodes[k]), hm(9, 0)))
	}
	w(
		obs(dependsOn(S, A), hm(9, 0)), obs(dependsOn(B, S), hm(9, 0)), obs(dependsOn(A, E), hm(9, 0)), obs(dependsOn(S, B), at(10)),
		obs(runsOn(N1, H1), hm(9, 0)), obs(runsOn(N2, H1), hm(9, 0)), obs(runsOn(N3, H2), hm(9, 0)),
		obs(runsOn(N4, H2), hm(9, 0)), del(runsOn(N4, H2), at(15)), obs(runsOn(N4, H1), at(15)),
		obs(runsOn(N2, H2), at(20)), del(runsOn(N2, H2), at(25)),
		obs(sched(PS1, N1), hm(9, 0)), obs(sched(PS2, N3), at(5)), obs(sched(PS3, N4), hm(9, 0)),
		obs(sched(PA1, N1), hm(9, 0)), del(sched(PA1, N1), at(12)), obs(sched(PA2, N2), at(12)),
		obs(sched(PB1, N3), hm(9, 0)), obs(sched(PX, N2), hm(9, 0)), del(sched(PX, N2), at(20)),
		del(partOf(IS3, PS1), at(18)), obs(partOf(IS3, PS3), at(16)),
		obs(instOf(IA1, E), at(8)), del(instOf(IA1, E), at(9)),
		obs(instOf(IE2, E), hm(9, 0)), observe(partOf(IE2, PE), traces, at(10), 12*time.Minute), obs(sched(PE, N4), at(10)),
	)
	// The entities world wrote are records too.
	return st, allRecords(t, st, recs, all), all
}

// TestScriptedClusterOracleAndPointReads is Q1, Q2 and Q9 on the scripted
// cluster, for three seed services, exploratory with and without hosts and
// anchored at both hosts at several instants.
func TestScriptedClusterOracleAndPointReads(t *testing.T) {
	st, recs, all := scriptedCluster(t)
	tok := st.LastSeq()
	crit := newOracle(recs, tok, w30.From, w30.To, nil).all
	pol := wide()
	pol.UbiquityMinNodes = 2
	pol.UbiquityNum, pol.UbiquityDen = 3, 4
	var seeds []seedCase
	for _, s := range []identity.Fingerprint{S, svc(2), svc(3)} {
		seeds = append(seeds, seedCase{extract.Exploratory{Service: s}, extract.Options{Policy: pol}},
			seedCase{extract.Exploratory{Service: s}, extract.Options{NodeOnly: true, Policy: pol}})
		for _, h := range []identity.Fingerprint{host(1), host(2)} {
			for _, m := range []int{0, 5, 13, 16, 21, 29} {
				seeds = append(seeds, seedCase{extract.Anchored{Service: s, Host: h, At: hm(10, m)}, extract.Options{Policy: pol}})
			}
		}
	}
	states := map[string]int{}
	for _, sc := range seeds {
		checkOracle(t, st, recs, all, sc.seed, w30, sc.o)
		checkPointReads(t, st, sc.seed, []identity.Fingerprint{node(1), node(2), node(3), node(4)}, w30, sc.o, crit)
		res := mustExtract(t, st, sc.seed, w30, sc.o)
		states[res.Anchor.String()]++
		for _, p := range res.Pairs {
			states[p.Mechanism.String()]++
		}
		states["suppressed"] += len(res.Suppressed)
		states["unmapped"] += len(res.Unmapped)
	}
	for _, k := range []string{"on_host", "not_on_host", "host_unattributed", "shared_pod", "shared_node", "shared_host", "suppressed", "unmapped"} {
		if states[k] == 0 {
			t.Errorf("the scripted cluster never gave %s: %v", k, states)
		}
	}
	t.Log(fmt.Sprint(states))
}

// allRecords adds the entity records the scenario helpers wrote to recs.
func allRecords(t *testing.T, st store.Store, recs []store.Record, fps []identity.Fingerprint) []store.Record {
	t.Helper()
	seen := map[uint64]bool{}
	for _, r := range recs {
		seen[r.Seq] = true
	}
	for _, fp := range fps {
		rs, err := st.EntityWindow(ctx, fp, hm(0, 0), hm(23, 0), store.Current(layerOf(ent(fp))))
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range rs {
			if !seen[r.Seq] {
				seen[r.Seq] = true
				recs = append(recs, r)
			}
		}
	}
	return recs
}
