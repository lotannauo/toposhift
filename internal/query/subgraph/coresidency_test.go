package subgraph_test

import (
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/coresidency"
	"github.com/lotannauo/toposhift/internal/query/subgraph"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

// coWorld writes a seed pod P1 on node N from before the window, and on host H
// through N, with:
//
//	P2 on N from 5 to 15 (a producer's delete);
//	P3 on N throughout;
//	P4 on N2, which is on H too, throughout: a shared host;
//	P5 on N from 20 to 22: short.
func coWorld(t testing.TB, opts memstore.Options) (*memstore.Store, *feed) {
	st, f := openStore(t, opts)
	for _, fp := range []identity.Fingerprint{nodeN, nodeN2, hostH, podP1, podP2, podP3, podP4, pod(5)} {
		f.write(obs(ent(fp), -100))
	}
	f.write(
		obs(sched(podP1, nodeN), -100), obs(runsOn(nodeN, hostH), -100), obs(runsOn(nodeN2, hostH), -100),
		obs(sched(podP2, nodeN), 5), del(sched(podP2, nodeN), 15),
		obs(sched(podP3, nodeN), -100),
		obs(sched(podP4, nodeN2), -100),
		obs(sched(pod(5), nodeN), 20), del(sched(pod(5), nodeN), 22),
	)
	return st, f
}

func findCo(t testing.TB, st store.Store, seed identity.Fingerprint, o coresidency.Options) coresidency.Result {
	t.Helper()
	res, err := coresidency.NewFinder(st).Find(ctx, seed, win30(), o)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func coWant(res coresidency.Result) func(time.Time) map[string]int {
	return func(x time.Time) map[string]int {
		out := map[string]int{}
		for _, c := range coresidency.At(res.CoResidents, x) {
			out[fmt.Sprintf("co_residency.%s %s %s", c.Mechanism, res.Seed, c.Peer)]++
		}
		return out
	}
}

// Each co-resident is a derived element keyed by the seed and the peer, with
// the rule of its mechanism, the legs it was intersected from, its overlaps as
// pieces, and the legs that end where an overlap does.
func TestFromCoResidency(t *testing.T) {
	st, _ := coWorld(t, memstore.Options{})
	res := findCo(t, st, podP1, coresidency.Options{})
	sg, err := subgraph.FromCoResidency(ctx, st, res, subgraph.ReadPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if sg.Basis != subgraph.BasisDerived || sg.Token != res.Token || !sg.From.Equal(res.From) || !sg.To.Equal(res.To) {
		t.Errorf("header %s %d %s %s", sg.Basis, sg.Token, sg.From, sg.To)
	}
	checkDerived(t, st, sg, coWant(res))

	node2 := derivedOf(sg, subgraph.RuleCoSharedNode, podP1, podP2)
	if len(node2) != 1 {
		t.Fatalf("elements for P2: %v", node2)
	}
	el := node2[0]
	if el.Subject != subgraph.SubjectDerived || el.Rule != subgraph.RuleCoSharedNode || el.Class != subgraph.ClassTransient || el.Transitions != 2 {
		t.Errorf("P2: subject %s rule %s class %s transitions %d", el.Subject, el.Rule, el.Class, el.Transitions)
	}
	expectEqual(t, "P2 pieces", pieceSecs(el), [][2]int{{5, 15}})
	p := el.Pieces[0]
	if p.EndReason != subgraph.EndReasonReachEnded || p.Before || len(p.Producers) != 0 {
		t.Errorf("P2 piece %+v", p)
	}
	// The overlap ends where the peer's edge to the node does, by a delete.
	if want := []subgraph.LegEnd{{Path: 0, Leg: 1, Reason: subgraph.EndReasonProducer}}; !slices.Equal(p.Ends, want) {
		t.Errorf("P2 ends %+v, want %+v", p.Ends, want)
	}
	if len(el.Paths) != 1 || legNames(sg, el.Paths[0]) != fmt.Sprintf("%s>%s scheduled_on; %s>%s scheduled_on", podP1, nodeN, podP2, nodeN) {
		t.Errorf("P2 legs %s", legNames(sg, el.Paths[0]))
	}
	if want := fmt.Sprintf("via=%s;nodes=%s", nodeN, nodeN); el.Key.Detail != want {
		t.Errorf("detail %q, want %q", el.Key.Detail, want)
	}

	// A shared node held all window begins before it and has no end.
	p3 := derivedOf(sg, subgraph.RuleCoSharedNode, podP1, podP3)[0]
	if p3.Class != subgraph.ClassStable || !p3.Pieces[0].Before || p3.Pieces[0].EndReason != subgraph.EndReasonUnknown || len(p3.Pieces[0].Ends) != 0 {
		t.Errorf("P3 %+v", p3)
	}

	// A shared host has the four legs through the two nodes.
	p4 := derivedOf(sg, subgraph.RuleCoSharedHost, podP1, podP4)
	if len(p4) != 1 || len(p4[0].Paths[0]) != 4 {
		t.Fatalf("P4: %v", p4)
	}
	if want := fmt.Sprintf("%s>%s scheduled_on; %s>%s runs_on; %s>%s scheduled_on; %s>%s runs_on", podP1, nodeN, nodeN, hostH, podP4, nodeN2, nodeN2, hostH); legNames(sg, p4[0].Paths[0]) != want {
		t.Errorf("P4 legs %s", legNames(sg, p4[0].Paths[0]))
	}

	// The seed, the peers, the nodes and the host have their existence from the
	// read at the result's token, and none is a boundary.
	for _, fp := range []identity.Fingerprint{podP1, podP2, podP3, podP4, nodeN, nodeN2, hostH} {
		if !hasKey(sg, ent(fp)) {
			t.Errorf("no existence element for %s", fp)
		}
		if id, ok := sg.EntityID(fp); !ok || sg.Entities[id].Boundary {
			t.Errorf("%s: in the dictionary %t, boundary", fp, ok)
		}
	}
	// The seed's placement and the legs are elements, as the interval package read
	// them.
	for _, k := range []subgraph.Key{sched(podP1, nodeN), sched(podP2, nodeN), runsOn(nodeN, hostH), runsOn(nodeN2, hostH)} {
		if !hasKey(sg, k) {
			t.Errorf("no element %s", k)
		}
	}
	// A pod that shared nothing for long enough is not an element.
	if got := derivedOf(sg, subgraph.RuleCoSharedNode, podP1, pod(5)); len(got) != 1 {
		t.Errorf("P5 (2 s, over the 1 s minimum): %v", got)
	}
}

// Everything a co-residency result reports travels: the suppressed entries with
// their reason, the unattributed placements, the truncated lists.
func TestFromCoResidencySections(t *testing.T) {
	st, f := coWorld(t, memstore.Options{})
	// The seed on a second node for a while.
	f.write(obs(ent(node(9)), -100), obs(sched(podP1, node(9)), 10), del(sched(podP1, node(9)), 12))
	pol := coresidency.DefaultPolicy()
	pol.MinOverlap = 5 * time.Second
	res := findCo(t, st, podP1, coresidency.Options{Policy: pol})
	if len(res.Suppressed) == 0 || len(res.Unattributed) == 0 {
		t.Fatalf("the world gives no suppressed entry or no unattributed placement: %+v", res)
	}
	sg, err := subgraph.FromCoResidency(ctx, st, res, subgraph.ReadPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	checkDerived(t, st, sg, coWant(res))

	// Suppressed: P5 (2 s) with its reason, in no keyframe or delta.
	if len(sg.Sections.Suppressed) != len(res.Suppressed) {
		t.Fatalf("suppressed %d, want %d", len(sg.Sections.Suppressed), len(res.Suppressed))
	}
	for i, s := range sg.Sections.Suppressed {
		want := res.Suppressed[i]
		if s.Key.A != res.Seed || s.Key.B != want.Peer || s.Reason != subgraph.SuppressedBelowMinOverlap || s.Class != subgraph.ClassTransient ||
			s.Transitions != want.Transitions || len(s.Spans) != len(want.Overlaps) {
			t.Errorf("suppressed %d: %+v, want %+v", i, s, want)
		}
		if hasKey(sg, s.Key) {
			t.Errorf("suppressed %s is an element", s.Key)
		}
	}
	expectEqual(t, "suppressed P5 span", sectionSecs(sg.Sections.Suppressed[0].Spans), [][2]int{{20, 22}})

	// Unattributed: every entry, with its reason, spans and candidates.
	if len(sg.Sections.Unattributed) != len(res.Unattributed) {
		t.Fatalf("unattributed %d, want %d", len(sg.Sections.Unattributed), len(res.Unattributed))
	}
	seen := map[subgraph.UnattributedReason]bool{}
	for i, u := range sg.Sections.Unattributed {
		want := res.Unattributed[i]
		if u.Subject != want.Subject || u.Reason.String() != want.Reason.String() || !slices.Equal(u.Candidates, want.Candidates) ||
			len(u.Spans) != len(want.Spans) {
			t.Errorf("unattributed %d: %+v, want %+v", i, u, want)
		}
		for _, s := range u.Spans {
			if s.EndReason != subgraph.EndReasonReachEnded && s.EndReason != subgraph.EndReasonUnknown {
				t.Errorf("unattributed %d span %+v", i, s)
			}
		}
		seen[u.Reason] = true
	}
	if !seen[subgraph.UnattributedSeveralNodes] {
		t.Errorf("reasons %v", seen)
	}
	// Every entity a section names is in the dictionary.
	for _, u := range sg.Sections.Unattributed {
		for _, fp := range append([]identity.Fingerprint{u.Subject}, u.Candidates...) {
			if _, ok := sg.EntityID(fp); !ok {
				t.Errorf("%s is named by a section and not in the dictionary", fp)
			}
		}
	}
}

func TestFromCoResidencyTruncation(t *testing.T) {
	st, f := coWorld(t, memstore.Options{})
	f.write(obs(ent(node(9)), -100), obs(sched(podP1, node(9)), 10), del(sched(podP1, node(9)), 12))
	// Two more nodes of the host, each with a pod.
	for i := 3; i <= 4; i++ {
		f.write(obs(ent(node(i)), -100), obs(ent(pod(10+i)), -100), obs(runsOn(node(i), hostH), -100), obs(sched(pod(10+i), node(i)), -100))
	}
	pol := coresidency.DefaultPolicy()
	pol.MaxNodes, pol.MaxResults = 1, 1
	res := findCo(t, st, podP1, coresidency.Options{Policy: pol})
	tr := res.Truncated
	if len(tr.Nodes) == 0 || tr.Results == 0 {
		t.Fatalf("the world truncates nothing: %+v", tr)
	}
	sg, err := subgraph.FromCoResidency(ctx, st, res, subgraph.ReadPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	got := sg.Sections.Truncated
	if !slices.Equal(got.Nodes, tr.Nodes) || !slices.Equal(got.Hosts, tr.Hosts) || got.Results != tr.Results || got.SuppressedDropped != tr.SuppressedDropped {
		t.Errorf("truncated %+v, want %+v", got, tr)
	}
	for _, fp := range got.Nodes {
		id, ok := sg.EntityID(fp)
		if !ok || !sg.Entities[id].Boundary {
			t.Errorf("the node %s that was not read: in the dictionary %t, boundary %t", fp, ok, ok && sg.Entities[id].Boundary)
		}
	}

	// Per-via cuts, with the mechanism as a wire value.
	pol = coresidency.DefaultPolicy()
	pol.MaxPerVia = 1
	res = findCo(t, st, podP1, coresidency.Options{Policy: pol})
	if len(res.Truncated.PerVia) == 0 {
		t.Fatalf("no per-via cut: %+v", res.Truncated)
	}
	sg, err = subgraph.FromCoResidency(ctx, st, res, subgraph.ReadPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if len(sg.Sections.Truncated.PerVia) != len(res.Truncated.PerVia) {
		t.Fatalf("per via %+v, want %+v", sg.Sections.Truncated.PerVia, res.Truncated.PerVia)
	}
	for i, v := range sg.Sections.Truncated.PerVia {
		w := res.Truncated.PerVia[i]
		if v.Via != w.Via || v.Dropped != w.Dropped || v.Mechanism.String() != w.Mechanism.String() {
			t.Errorf("per via %d: %+v, want %+v", i, v, w)
		}
	}
}

// A quarantined host: its edges are in the static section, in no keyframe or
// delta, and its existence read is the quarantine.
func TestFromCoResidencyQuarantine(t *testing.T) {
	st, f := openStore(t, memstore.Options{Policy: storetest.QuarantinePolicy()})
	boot := func(id string, sec int) store.Record {
		r := observe(ent(hostH), agent, sec, 15*time.Minute)
		r.Boot = id
		return r
	}
	for _, fp := range []identity.Fingerprint{nodeN, nodeN2, podP1, podP2, podP4} {
		f.write(obs(ent(fp), -100))
	}
	f.write(obs(sched(podP1, nodeN), -100), obs(sched(podP2, nodeN), -100), obs(sched(podP4, nodeN2), -100),
		boot("b1", -100), boot("b2", -95), boot("b1", -90),
		observe(runsOn(nodeN, hostH), agent, -100, 0), observe(runsOn(nodeN2, hostH), agent, -100, 0))
	res := findCo(t, st, podP1, coresidency.Options{})
	if len(res.Quarantined) == 0 {
		t.Fatal("the world quarantines nothing")
	}
	sg, err := subgraph.FromCoResidency(ctx, st, res, subgraph.ReadPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	got := quarantinedKeys(sg)
	want := []subgraph.Key{ent(hostH), runsOn(nodeN, hostH)}
	slices.SortFunc(want, subgraph.Key.Compare)
	if !slices.Equal(got, want) {
		t.Errorf("quarantine section %v, want %v", got, want)
	}
	if hasKey(sg, ent(hostH)) {
		t.Error("the quarantined host is an element")
	}
	for _, kf := range sg.Keyframes {
		for _, e := range kf.Elements {
			if k := sg.Elements[e].Key; k.A == hostH || k.B == hostH {
				t.Errorf("a keyframe holds %s", k)
			}
		}
	}
	for _, d := range sg.Deltas {
		if k := sg.Elements[d.Element].Key; k.A == hostH || k.B == hostH {
			t.Errorf("a delta for %s", k)
		}
	}
	for _, e := range sg.Quarantine.Edges {
		if !errors.Is(e.Cause, store.ErrQuarantined) || e.Cause.Entity != hostH || len(e.Pieces) == 0 {
			t.Errorf("uncascaded edge %+v", e)
		}
	}
	// The shared host is not derived through a quarantined host.
	if got := derivedOf(sg, subgraph.RuleCoSharedHost, podP1, podP4); len(got) != 0 {
		t.Errorf("a shared host through a quarantined host: %v", got)
	}
	checkDerived(t, st, sg, coWant(res))
}

// Several paths to one peer are several elements with distinct keys: a peer on
// two other nodes of the seed's host shares the host through each, and a node
// with the seed besides. They differ in the detail of the key alone.
func TestFromCoResidencyKeysAreUnique(t *testing.T) {
	st, f := openStore(t, memstore.Options{})
	n3 := node(3)
	for _, fp := range []identity.Fingerprint{nodeN, nodeN2, n3, hostH, podP1, podP2} {
		f.write(obs(ent(fp), -100))
	}
	f.write(obs(sched(podP1, nodeN), -100), obs(sched(podP2, nodeN), -100), obs(sched(podP2, nodeN2), -100), obs(sched(podP2, n3), -100),
		obs(runsOn(nodeN, hostH), -100), obs(runsOn(nodeN2, hostH), -100), obs(runsOn(n3, hostH), -100))
	res := findCo(t, st, podP1, coresidency.Options{})
	if len(res.CoResidents) != 3 {
		t.Fatalf("the world gives %d co-residents, want 3", len(res.CoResidents))
	}
	sg, err := subgraph.FromCoResidency(ctx, st, res, subgraph.ReadPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	keys := keysOfDerived(sg)
	if len(keys) != 3 {
		t.Fatalf("%d derived elements, want 3", len(keys))
	}
	details := map[string]bool{}
	for _, k := range keys {
		if k.A != podP1 || k.B != podP2 {
			t.Errorf("ends of %s", k)
		}
		if details[k.Rule+k.Detail] {
			t.Errorf("two elements of the rule %s and the detail %q", k.Rule, k.Detail)
		}
		details[k.Rule+k.Detail] = true
	}
	hosts := derivedOf(sg, subgraph.RuleCoSharedHost, podP1, podP2)
	if len(hosts) != 2 || hosts[0].Key.Detail == hosts[1].Key.Detail {
		t.Errorf("shared host elements %+v", hosts)
	}
	for _, el := range hosts {
		want1 := fmt.Sprintf("via=%s;nodes=%s,%s", hostH, nodeN, nodeN2)
		want2 := fmt.Sprintf("via=%s;nodes=%s,%s", hostH, nodeN, n3)
		if d := el.Key.Detail; d != want1 && d != want2 {
			t.Errorf("detail %q", d)
		}
	}
	checkDerived(t, st, sg, coWant(res))
}

func keysOfDerived(sg subgraph.Subgraph) []subgraph.Key {
	var out []subgraph.Key
	for _, el := range sg.Elements {
		if el.Key.Kind == subgraph.KindDerived {
			out = append(out, el.Key)
		}
	}
	return out
}

var _ = catalog.ScheduledOn

// A seed on a node with no host is unattributed there, with its reason and the
// span of its time on the node.
func TestFromCoResidencyNoHost(t *testing.T) {
	st, f := openStore(t, memstore.Options{})
	n8, p6, p7 := node(8), pod(6), pod(7)
	for _, fp := range []identity.Fingerprint{n8, p6, p7} {
		f.write(obs(ent(fp), -100))
	}
	f.write(obs(sched(p6, n8), 3), obs(sched(p7, n8), 5), del(sched(p7, n8), 20))
	res := findCo(t, st, p6, coresidency.Options{})
	sg, err := subgraph.FromCoResidency(ctx, st, res, subgraph.ReadPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if len(sg.Sections.Unattributed) != 1 {
		t.Fatalf("unattributed %+v", sg.Sections.Unattributed)
	}
	u := sg.Sections.Unattributed[0]
	if u.Subject != n8 || u.Reason != subgraph.UnattributedNoHost || u.Reason.String() != "no_host" || !u.SeedPod.IsZero() {
		t.Errorf("unattributed %+v", u)
	}
	expectEqual(t, "spans", sectionSecs(u.Spans), [][2]int{{3, 30}})
	if u.Spans[0].EndReason != subgraph.EndReasonUnknown {
		t.Errorf("a span that reaches To ends with %s", u.Spans[0].EndReason)
	}
	checkDerived(t, st, sg, coWant(res))
}

// The hosts a search did not read are carried, and are boundary entries: they
// were named and not read.
func TestFromCoResidencyHostsTruncation(t *testing.T) {
	st, f := coWorld(t, memstore.Options{})
	h5, n5 := host(5), node(5)
	f.write(obs(ent(h5), -100), obs(ent(n5), -100), obs(runsOn(n5, h5), -100), del(sched(podP1, nodeN), 15), obs(sched(podP1, n5), 15))
	pol := coresidency.DefaultPolicy()
	pol.MaxHosts = 1
	res := findCo(t, st, podP1, coresidency.Options{Policy: pol})
	if len(res.Truncated.Hosts) == 0 {
		t.Fatalf("no host is cut: %+v", res.Truncated)
	}
	sg, err := subgraph.FromCoResidency(ctx, st, res, subgraph.ReadPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(sg.Sections.Truncated.Hosts, res.Truncated.Hosts) {
		t.Errorf("hosts %v, want %v", sg.Sections.Truncated.Hosts, res.Truncated.Hosts)
	}
	for _, fp := range sg.Sections.Truncated.Hosts {
		if id, ok := sg.EntityID(fp); !ok || !sg.Entities[id].Boundary {
			t.Errorf("the host %s that was not read: in the dictionary %t", fp, ok)
		}
	}
	checkDerived(t, st, sg, coWant(res))
}

// A result the wire has no value for is refused, never carried by a guess: a
// leg that ends with no known source, a mechanism outside the table, an entry
// suppressed for no reason, an entry shown that is suppressed.
func TestAdaptersRefuseWhatTheyCannotName(t *testing.T) {
	st, _ := coWorld(t, memstore.Options{})
	pol := coresidency.DefaultPolicy()
	pol.MinOverlap = 5 * time.Second
	base := findCo(t, st, podP1, coresidency.Options{Policy: pol})
	if len(base.Suppressed) == 0 || len(base.CoResidents) < 3 {
		t.Fatalf("the world gives %d co-residents and %d suppressed", len(base.CoResidents), len(base.Suppressed))
	}
	deep := func(res coresidency.Result) coresidency.Result {
		res.CoResidents = slices.Clone(res.CoResidents)
		for i := range res.CoResidents {
			res.CoResidents[i].Overlaps = slices.Clone(res.CoResidents[i].Overlaps)
			for j := range res.CoResidents[i].Overlaps {
				res.CoResidents[i].Overlaps[j].Ends = slices.Clone(res.CoResidents[i].Overlaps[j].Ends)
			}
		}
		res.Suppressed = slices.Clone(res.Suppressed)
		return res
	}
	ended := slices.IndexFunc(base.CoResidents, func(c coresidency.CoResident) bool { return len(c.Overlaps[0].Ends) > 0 })
	if ended < 0 {
		t.Fatal("no overlap ends in the world")
	}
	cases := map[string]func(res *coresidency.Result){
		"a leg that ends with no known source": func(r *coresidency.Result) {
			r.CoResidents[ended].Overlaps[0].Ends[0].Source = lifecycle.EndUnknown
		},
		"a leg that goes on and has a source": func(r *coresidency.Result) {
			r.CoResidents[ended].Overlaps[0].Ends[0].Unattributed = true
		},
		"a source outside the lifecycle's table": func(r *coresidency.Result) {
			r.CoResidents[ended].Overlaps[0].Ends[0].Source = 77
		},
		"an overlap that ends and names no leg": func(r *coresidency.Result) { r.CoResidents[ended].Overlaps[0].Ends = nil },
		"an end of a leg the entry lacks":       func(r *coresidency.Result) { r.CoResidents[ended].Overlaps[0].Ends[0].Leg = 9 },
		"a mechanism outside the table":         func(r *coresidency.Result) { r.CoResidents[0].Mechanism = 9 },
		"no mechanism":                          func(r *coresidency.Result) { r.CoResidents[0].Mechanism = coresidency.MechanismNone },
		"a suppressed entry without a reason":   func(r *coresidency.Result) { r.Suppressed[0].Reason = coresidency.ReasonNone },
		"a shown entry that is suppressed":      func(r *coresidency.Result) { r.CoResidents[0].Reason = coresidency.BelowMinOverlap },
		"a class outside the table":             func(r *coresidency.Result) { r.CoResidents[0].Class = 77 },
		"an unattributed reason outside the table": func(r *coresidency.Result) {
			r.Unattributed = append(slices.Clone(r.Unattributed), coresidency.Unattributed{Subject: nodeN, Reason: 77, Spans: r.CoResidents[0].Spans()})
		},
		"a truncated per-via mechanism outside the table": func(r *coresidency.Result) {
			r.Truncated.PerVia = []coresidency.ViaCount{{Via: nodeN, Mechanism: 9, Dropped: 1}}
		},
		"a leg that is another read": func(r *coresidency.Result) {
			// Two reads of one subject that disagree are an error, never a pick.
			legs := slices.Clone(r.CoResidents[0].Legs)
			legs[0].Pieces = slices.Clone(legs[0].Pieces)
			legs[0].Pieces[0].End = legs[0].Pieces[0].End.Add(-time.Second)
			r.CoResidents[0].Legs = legs
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			res := deep(base)
			mutate(&res)
			sg, err := subgraph.FromCoResidency(ctx, st, res, subgraph.ReadPolicy{})
			if !errors.Is(err, store.ErrInvalid) {
				t.Fatalf("FromCoResidency = %v, want a refusal wrapping ErrInvalid", err)
			}
			if len(sg.Elements) != 0 || len(sg.Entities) != 0 {
				t.Errorf("a partial answer: %d elements", len(sg.Elements))
			}
		})
	}
}
