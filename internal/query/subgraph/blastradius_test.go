package subgraph_test

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/query/blastradius"
	"github.com/lotannauo/toposhift/internal/query/subgraph"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

func container(n int) identity.Fingerprint { return mkfp(catalog.Container, n) }

// blastWorld writes a host H with two nodes, a pod on each, a container in the
// first pod:
//
//	P1 on N throughout, C1 part of P1;
//	P2 on N2 from 10 to 20.
func blastWorld(t testing.TB) (*memstore.Store, *feed) {
	st, f := openStore(t, memstore.Options{})
	for _, fp := range []identity.Fingerprint{hostH, nodeN, nodeN2, podP1, podP2, container(1)} {
		f.write(obs(ent(fp), -100))
	}
	f.write(obs(runsOn(nodeN, hostH), -100), obs(runsOn(nodeN2, hostH), -100),
		obs(sched(podP1, nodeN), -100), obs(partOf(container(1), podP1), -100),
		obs(sched(podP2, nodeN2), 10), del(sched(podP2, nodeN2), 20))
	return st, f
}

func impactOf(t testing.TB, st store.Store, seed identity.Fingerprint, o blastradius.Options) blastradius.Result {
	t.Helper()
	res, err := blastradius.NewWalker(st).Impact(ctx, seed, win30(), o)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func blastWant(res blastradius.Result) func(time.Time) map[string]int {
	return func(x time.Time) map[string]int {
		out := map[string]int{}
		for _, m := range blastradius.At(res.Members, x) {
			out[fmt.Sprintf("blast_radius.reach %s %s", res.Seed, m.Entity)]++
		}
		return out
	}
}

// A member is a derived element keyed by the seed and the member, with the rule
// of a reach. A reach has no source of its own and does not begin before the
// window: its end is reach_ended and lists no leg, and its paths are its
// arrivals' legs.
func TestFromBlastRadius(t *testing.T) {
	st, _ := blastWorld(t)
	res := impactOf(t, st, hostH, blastradius.Options{})
	sg, err := subgraph.FromBlastRadius(ctx, st, res, subgraph.ReadPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	checkDerived(t, st, sg, blastWant(res))
	if len(res.Members) != 5 {
		t.Fatalf("members %d", len(res.Members))
	}
	for _, m := range res.Members {
		els := derivedOf(sg, subgraph.RuleReach, hostH, m.Entity)
		if len(els) != 1 {
			t.Fatalf("%s: %v", m.Entity, els)
		}
		el := els[0]
		if el.Subject != subgraph.SubjectDerived || el.Rule != subgraph.RuleReach || el.Key.Detail != "" || len(el.Paths) != len(m.Arrivals) {
			t.Errorf("%s: %+v", m.Entity, el)
		}
		for _, p := range el.Pieces {
			if p.Before || len(p.Ends) != 0 || len(p.Producers) != 0 {
				t.Errorf("%s: piece %+v", m.Entity, p)
			}
		}
		for i, a := range m.Arrivals {
			if got, want := legNames(sg, el.Paths[i]), fmt.Sprintf("%s>%s %s", a.Leg.Subject.A, a.Leg.Subject.B, a.Relation); got != want {
				t.Errorf("%s path %d: %s, want %s", m.Entity, i, got, want)
			}
		}
	}
	// P2: reached [10, 20), ended by the intersection, with no source.
	p2 := derivedOf(sg, subgraph.RuleReach, hostH, podP2)[0]
	expectEqual(t, "P2 reach", pieceSecs(p2), [][2]int{{10, 20}})
	if p2.Pieces[0].EndReason != subgraph.EndReasonReachEnded || p2.Class != subgraph.ClassTransient {
		t.Errorf("P2: %+v", p2)
	}
	// N is reached throughout: no end, and still not Before.
	n := derivedOf(sg, subgraph.RuleReach, hostH, nodeN)[0]
	if n.Class != subgraph.ClassStable || n.Pieces[0].EndReason != subgraph.EndReasonUnknown || n.Pieces[0].Before {
		t.Errorf("N: %+v", n)
	}
	// The seed and the members have existence elements, the legs are elements.
	for _, fp := range []identity.Fingerprint{hostH, nodeN, nodeN2, podP1, podP2, container(1)} {
		if !hasKey(sg, ent(fp)) {
			t.Errorf("no existence element for %s", fp)
		}
	}
	for _, k := range []subgraph.Key{runsOn(nodeN, hostH), sched(podP2, nodeN2), partOf(container(1), podP1)} {
		if !hasKey(sg, k) {
			t.Errorf("no leg %s", k)
		}
	}
	// The seed is not its own member.
	if got := derivedOf(sg, subgraph.RuleReach, hostH, hostH); len(got) != 0 {
		t.Errorf("the seed reaches itself: %v", got)
	}
}

// A node on two hosts is placed on neither over the overlap, which is reported
// as unattributed with the targets claimed, and a reach that ends because
// another claim began ends by the intersection, not by a source.
func TestFromBlastRadiusUnattributed(t *testing.T) {
	st, f := openStore(t, memstore.Options{})
	h2 := host(2)
	for _, fp := range []identity.Fingerprint{hostH, h2, nodeN, podP1} {
		f.write(obs(ent(fp), -100))
	}
	f.write(observe(runsOn(nodeN, hostH), agent, -5, 20*time.Second), obs(runsOn(nodeN, h2), 10), obs(sched(podP1, nodeN), -100))
	res := impactOf(t, st, hostH, blastradius.Options{})
	if len(res.Unattributed) != 1 {
		t.Fatalf("unattributed %+v", res.Unattributed)
	}
	sg, err := subgraph.FromBlastRadius(ctx, st, res, subgraph.ReadPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	checkDerived(t, st, sg, blastWant(res))
	if len(sg.Sections.Unattributed) != 1 {
		t.Fatalf("unattributed %+v", sg.Sections.Unattributed)
	}
	u, w := sg.Sections.Unattributed[0], res.Unattributed[0]
	if u.Subject != w.Subject || u.Reason != subgraph.UnattributedSeveralClaims || u.Relation != catalog.RunsOn || u.Target != catalog.Host ||
		!slices.Equal(u.Candidates, w.Candidates) || len(u.Spans) != len(w.Spans) || !u.SeedPod.IsZero() {
		t.Errorf("unattributed %+v, want %+v", u, w)
	}
	for _, fp := range u.Candidates {
		if _, ok := sg.EntityID(fp); !ok {
			t.Errorf("%s is named by a section and not in the dictionary", fp)
		}
	}
	// N's reach from H ends at 10, where its claim of H2 begins.
	n := derivedOf(sg, subgraph.RuleReach, hostH, nodeN)
	if len(n) != 1 || n[0].Pieces[0].EndReason != subgraph.EndReasonReachEnded || len(n[0].Pieces[0].Ends) != 0 {
		t.Errorf("N: %+v", n)
	}
}

func TestFromBlastRadiusTruncation(t *testing.T) {
	st, f := openStore(t, memstore.Options{})
	s1, s2, s3, s4 := svc(1), svc(2), svc(3), svc(4)
	for _, fp := range []identity.Fingerprint{s1, s2, s3, s4} {
		f.write(obs(ent(fp), -100))
	}
	f.write(obs(dependsOn(s2, s1), -100), obs(dependsOn(s3, s2), -100), obs(dependsOn(s4, s3), -100))
	pol := blastradius.DefaultPolicy()
	pol.MaxDepth = 2
	res := impactOf(t, st, s1, blastradius.Options{Policy: pol})
	if len(res.Truncated.Depth) == 0 {
		t.Fatalf("truncated %+v", res.Truncated)
	}
	sg, err := subgraph.FromBlastRadius(ctx, st, res, subgraph.ReadPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(sg.Sections.Truncated.Depth, res.Truncated.Depth) {
		t.Errorf("truncated %+v, want %+v", sg.Sections.Truncated, res.Truncated)
	}
	for _, fp := range sg.Sections.Truncated.Depth {
		if id, ok := sg.EntityID(fp); !ok || sg.Entities[id].Boundary {
			// The member reached at the limit was reached, and its existence read.
			t.Errorf("%s: in the dictionary %t", fp, ok)
		}
	}

	// A frontier cut and a member cut.
	st, f = openStore(t, memstore.Options{})
	rack1, h1, h2, h3 := mkfp(catalog.Rack, 1), host(1), host(2), host(3)
	for _, fp := range []identity.Fingerprint{rack1, h1, h2, h3} {
		f.write(obs(ent(fp), -100))
	}
	f.write(obs(subgraph.EdgeKey(h1, rack1, catalog.LocatedIn), -100), obs(subgraph.EdgeKey(h2, rack1, catalog.LocatedIn), -100),
		obs(subgraph.EdgeKey(h3, rack1, catalog.LocatedIn), -100))
	for i, node := range []identity.Fingerprint{nodeN, nodeN2, node3()} {
		f.write(obs(ent(node), -100), obs(runsOn(node, []identity.Fingerprint{h1, h2, h3}[i]), -100))
	}
	pol = blastradius.DefaultPolicy()
	pol.MaxFrontier, pol.MaxMembers = 2, 4
	res = impactOf(t, st, rack1, blastradius.Options{Policy: pol})
	if len(res.Truncated.Frontier) == 0 || res.Truncated.Members == 0 {
		t.Fatalf("truncated %+v", res.Truncated)
	}
	sg, err = subgraph.FromBlastRadius(ctx, st, res, subgraph.ReadPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	got := sg.Sections.Truncated
	if got.MembersDropped != res.Truncated.Members || len(got.Frontier) != len(res.Truncated.Frontier) {
		t.Fatalf("truncated %+v, want %+v", got, res.Truncated)
	}
	for i, c := range got.Frontier {
		if c.Depth != res.Truncated.Frontier[i].Depth || !slices.Equal(c.Dropped, res.Truncated.Frontier[i].Dropped) {
			t.Errorf("frontier cut %d: %+v, want %+v", i, c, res.Truncated.Frontier[i])
		}
		for _, fp := range c.Dropped {
			if _, ok := sg.EntityID(fp); !ok {
				t.Errorf("%s is named by a section and not in the dictionary", fp)
			}
		}
	}
	checkDerived(t, st, sg, blastWant(res))
}

func node3() identity.Fingerprint { return node(3) }

// A quarantined seed reaches nothing, its edges are in the static section and
// the walk's quarantined list travels.
func TestFromBlastRadiusQuarantine(t *testing.T) {
	st, f := openStore(t, memstore.Options{Policy: storetest.QuarantinePolicy()})
	boot := func(id string, sec int) store.Record {
		r := observe(ent(hostH), agent, sec, 15*time.Minute)
		r.Boot = id
		return r
	}
	f.write(obs(ent(nodeN), -100), boot("b1", -100), boot("b2", -95), boot("b1", -90), observe(runsOn(nodeN, hostH), agent, -100, 0))
	res := impactOf(t, st, hostH, blastradius.Options{})
	if len(res.Quarantined) != 1 {
		t.Fatalf("quarantined %v", res.Quarantined)
	}
	sg, err := subgraph.FromBlastRadius(ctx, st, res, subgraph.ReadPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	want := []subgraph.Key{ent(hostH), runsOn(nodeN, hostH)}
	slices.SortFunc(want, subgraph.Key.Compare)
	if got := quarantinedKeys(sg); !slices.Equal(got, want) {
		t.Errorf("quarantine section %v, want %v", got, want)
	}
	if len(keysOfDerived(sg)) != 0 || hasKey(sg, ent(hostH)) {
		t.Errorf("elements %v", sortedKeys(sg))
	}
	for _, fp := range []identity.Fingerprint{hostH, nodeN} {
		if id, ok := sg.EntityID(fp); !ok || sg.Entities[id].Boundary {
			t.Errorf("%s: in the dictionary %t", fp, ok)
		}
	}
	checkDerived(t, st, sg, blastWant(res))
}

// The reference cell of the blast-radius package: a host with one node and
// BudgetReferencePods live pods with a container each.
func TestBlastRadiusReferenceCellIsWithinTheReadBudget(t *testing.T) {
	st, f := openStore(t, memstore.Options{})
	f.write(obs(ent(hostH), -100), obs(ent(nodeN), -100), obs(runsOn(nodeN, hostH), -100))
	for i := 1; i <= blastradius.BudgetReferencePods; i++ {
		f.write(obs(ent(pod(i)), -100), obs(ent(container(i)), -100), obs(sched(pod(i), nodeN), -100), obs(partOf(container(i), pod(i)), -100))
	}
	res := impactOf(t, st, hostH, blastradius.Options{})
	cs := &counting{spy: &spy{Store: st}}
	sg, err := subgraph.FromBlastRadius(ctx, cs, res, subgraph.ReadPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	n := 2 + 2*blastradius.BudgetReferencePods
	if cs.alive != n || cs.entityWindows != n || cs.wins != 0 || readEntities(sg) != n {
		t.Errorf("%d Alive, %d EntityWindow, %d Window, %d entities, want %d", cs.alive, cs.entityWindows, cs.wins, readEntities(sg), n)
	}
	if cs.alive > subgraph.BudgetBlastEntities {
		t.Errorf("%d existence reads, over the budget of %d", cs.alive, subgraph.BudgetBlastEntities)
	}
}
