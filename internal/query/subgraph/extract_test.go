package subgraph_test

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/query/coresidency"
	"github.com/lotannauo/toposhift/internal/query/extract"
	"github.com/lotannauo/toposhift/internal/query/subgraph"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

var (
	svcS, svcT, svcC, svcK, svcU, svcZ = svc(1), svc(2), svc(3), svc(4), svc(5), svc(6)
	instS, instT, instU, instZ         = inst(1), inst(2), inst(5), inst(6)
	podT, podU                         = pod(2), pod(5)
)

// service writes an instance of s, part of pod p, from before the window.
func service(f *feed, s, i, p identity.Fingerprint) {
	f.write(obs(ent(i), -100), obs(instOf(i, s), -100), obs(partOf(i, p), -100))
}

// extractWorld writes the seed service S with a pod on node N of host H, and:
//
//	T with a pod on N from 10 to 20: a shared node, ended by a delete;
//	U with a pod on N2, also on H, throughout: a shared host;
//	Z with an instance part of S's pod from 5 to 22: a shared pod;
//	C a caller of S and K a callee.
func extractWorld(t testing.TB) (*memstore.Store, *feed) {
	st, f := openStore(t, memstore.Options{})
	for _, fp := range []identity.Fingerprint{svcS, svcT, svcU, svcZ, svcC, svcK, podP1, podT, podU, nodeN, nodeN2, hostH} {
		f.write(obs(ent(fp), -100))
	}
	service(f, svcS, instS, podP1)
	service(f, svcT, instT, podT)
	service(f, svcU, instU, podU)
	f.write(obs(ent(instZ), -100), obs(instOf(instZ, svcZ), -100), obs(partOf(instZ, podP1), 5), del(partOf(instZ, podP1), 22),
		obs(sched(podP1, nodeN), -100), obs(sched(podT, nodeN), 10), del(sched(podT, nodeN), 20),
		obs(sched(podU, nodeN2), -100),
		obs(runsOn(nodeN, hostH), -100), obs(runsOn(nodeN2, hostH), -100), del(runsOn(nodeN2, hostH), 26),
		obs(dependsOn(svcC, svcS), -100), obs(dependsOn(svcS, svcK), -100))
	return st, f
}

func runExtract(t testing.TB, st store.Store, seed extract.Seed, o extract.Options) extract.Result {
	t.Helper()
	res, err := extract.NewExtractor(st).Extract(ctx, seed, win30(), o)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func exWant(res extract.Result) func(time.Time) map[string]int {
	return func(x time.Time) map[string]int {
		out := map[string]int{}
		for _, p := range res.Pairs {
			for _, s := range p.Spans {
				if s.Contains(x) {
					out[fmt.Sprintf("pair.%s %s %s", p.Mechanism, p.Member, p.Peer)]++
				}
			}
		}
		return out
	}
}

// A pair is a derived element keyed by its two services; each contribution is a
// path of the four mapping legs and then the legs of its pod-level entry, and
// the legs that end where a span does are listed by contribution and leg, the
// nested pod-level end among them.
func TestFromExtraction(t *testing.T) {
	st, _ := extractWorld(t)
	res := runExtract(t, st, extract.Exploratory{Service: svcS}, extract.Options{})
	sg, err := subgraph.FromExtraction(ctx, st, res, subgraph.ReadPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	checkDerived(t, st, sg, exWant(res))
	if len(res.Pairs) != 3 {
		t.Fatalf("pairs %d", len(res.Pairs))
	}

	// S and T: a shared node [10, 20). The span ends where T's pod leaves the
	// node, which is a leg of the pod-level entry: leg 4 + 1.
	tt := derivedOf(sg, subgraph.RulePairSharedNode, svcS, svcT)
	if len(tt) != 1 || tt[0].Key.Detail != "" {
		t.Fatalf("S-T: %+v", tt)
	}
	expectEqual(t, "S-T pieces", pieceSecs(tt[0]), [][2]int{{10, 20}})
	if want := []subgraph.LegEnd{{Path: 0, Leg: 5, Reason: subgraph.EndReasonProducer}}; !slices.Equal(tt[0].Pieces[0].Ends, want) {
		t.Errorf("S-T ends %+v, want %+v", tt[0].Pieces[0].Ends, want)
	}
	if tt[0].Pieces[0].EndReason != subgraph.EndReasonReachEnded {
		t.Errorf("S-T end reason %s", tt[0].Pieces[0].EndReason)
	}
	if got, want := legNames(sg, tt[0].Paths[0]), fmt.Sprintf(
		"%s>%s instance_of; %s>%s part_of; %s>%s part_of; %s>%s instance_of; %s>%s scheduled_on; %s>%s scheduled_on",
		instS, svcS, instS, podP1, instT, podT, instT, svcT, podP1, nodeN, podT, nodeN); got != want {
		t.Errorf("S-T legs:\n%s\nwant\n%s", got, want)
	}

	// S and Z: a shared pod [5, 22), four legs, ended by a mapping leg (leg 2:
	// the peer instance's part_of).
	zz := derivedOf(sg, subgraph.RulePairSharedPod, svcS, svcZ)
	if len(zz) != 1 || len(zz[0].Paths) != 1 || len(zz[0].Paths[0]) != 4 {
		t.Fatalf("S-Z: %+v", zz)
	}
	expectEqual(t, "S-Z pieces", pieceSecs(zz[0]), [][2]int{{5, 22}})
	if want := []subgraph.LegEnd{{Path: 0, Leg: 2, Reason: subgraph.EndReasonProducer}}; !slices.Equal(zz[0].Pieces[0].Ends, want) {
		t.Errorf("S-Z ends %+v", zz[0].Pieces[0].Ends)
	}

	// S and U: a shared host to 26, six legs in the path (four mapping and the
	// pod-level entry's four make eight).
	uu := derivedOf(sg, subgraph.RulePairSharedHost, svcS, svcU)
	if len(uu) != 1 || len(uu[0].Paths[0]) != 8 {
		t.Fatalf("S-U: %+v", uu)
	}
	expectEqual(t, "S-U pieces", pieceSecs(uu[0]), [][2]int{{0, 26}})
	if want := []subgraph.LegEnd{{Path: 0, Leg: 7, Reason: subgraph.EndReasonProducer}}; !slices.Equal(uu[0].Pieces[0].Ends, want) {
		t.Errorf("S-U ends %+v", uu[0].Pieces[0].Ends)
	}

	// The members' dependency and instance edges, the services, the instances
	// and the pods have elements; the pod-level entries are not elements.
	for _, k := range []subgraph.Key{
		dependsOn(svcC, svcS), dependsOn(svcS, svcK), instOf(instS, svcS), ent(svcS), ent(svcC), ent(instS), ent(podP1), ent(nodeN), ent(hostH),
		partOf(instS, podP1), sched(podT, nodeN),
	} {
		if !hasKey(sg, k) {
			t.Errorf("no element %s", k)
		}
	}
	for _, el := range sg.Elements {
		if el.Key.Kind == subgraph.KindDerived && (el.Rule == subgraph.RuleCoSharedNode || el.Rule == subgraph.RuleCoSharedHost) {
			t.Errorf("a pod-level entry is an element: %s", el.Key)
		}
	}
	if sg.Sections.Anchor != subgraph.AnchorNone || sg.Sections.Ubiquity.Judged {
		t.Errorf("anchor %s, ubiquity %+v", sg.Sections.Anchor, sg.Sections.Ubiquity)
	}
	if len(sg.Sections.Pods) != len(res.PodResults) {
		t.Errorf("pod sections %d, pod results %d", len(sg.Sections.Pods), len(res.PodResults))
	}
}

// A mapping that ends because an instance became part of two pods is a
// mapping leg that ends unattributed; the extraction's own refusal is a section.
func TestFromExtractionUnattributedAndUnmapped(t *testing.T) {
	st, f := openStore(t, memstore.Options{})
	podP9, podX := pod(9), pod(8)
	for _, fp := range []identity.Fingerprint{svcS, svcT, instS, instT, podP1, podP9, podT, podX, nodeN} {
		f.write(obs(ent(fp), -100))
	}
	service(f, svcS, instS, podP1)
	service(f, svcT, instT, podT)
	f.write(obs(partOf(instS, podP9), 10), del(partOf(instS, podP9), 20),
		obs(sched(podP1, nodeN), -100), obs(sched(podT, nodeN), -100), obs(sched(podX, nodeN), -100))
	res := runExtract(t, st, extract.Exploratory{Service: svcS}, extract.Options{NodeOnly: true})
	if len(res.Unattributed) == 0 || len(res.Unmapped) == 0 {
		t.Fatalf("unattributed %v, unmapped %v", res.Unattributed, res.Unmapped)
	}
	sg, err := subgraph.FromExtraction(ctx, st, res, subgraph.ReadPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	checkDerived(t, st, sg, exWant(res))
	if len(sg.Sections.Unattributed) != len(res.Unattributed) || len(sg.Sections.Unmapped) != len(res.Unmapped) {
		t.Fatalf("sections: %d unattributed, %d unmapped", len(sg.Sections.Unattributed), len(sg.Sections.Unmapped))
	}
	u := sg.Sections.Unattributed[0]
	if u.Subject != instS || u.Reason != subgraph.UnattributedSeveralPods || !u.SeedPod.IsZero() || !slices.Equal(u.Candidates, []identity.Fingerprint{podP1, podP9}) {
		t.Errorf("unattributed %+v", u)
	}
	expectEqual(t, "unattributed spans", sectionSecs(u.Spans), [][2]int{{10, 20}})
	if u.Spans[0].EndReason != subgraph.EndReasonReachEnded {
		t.Errorf("unattributed end %s", u.Spans[0].EndReason)
	}
	m := sg.Sections.Unmapped[0]
	if m.Pod != podX {
		t.Errorf("unmapped %+v", m)
	}
	expectEqual(t, "unmapped spans", sectionSecs(m.Spans), [][2]int{{0, 30}})
	// The pair's span ends at the unattributed spans' start with the mapping leg
	// that goes on, unattributed.
	pair := derivedOf(sg, subgraph.RulePairSharedNode, svcS, svcT)
	if len(pair) != 1 {
		t.Fatalf("pairs %v", pair)
	}
	var seen bool
	for _, p := range pair[0].Pieces {
		for _, e := range p.Ends {
			seen = seen || e.Reason == subgraph.EndReasonUnattributed
		}
	}
	if !seen {
		t.Errorf("no leg ends unattributed: %+v", pair[0].Pieces)
	}
}

// A ubiquitous peer's pairs are suppressed, shown with their reason and
// coverage, and are in no keyframe or delta.
func TestFromExtractionSuppressed(t *testing.T) {
	dd, rr := svc(7), svc(8)
	st, f := openStore(t, memstore.Options{})
	f.write(obs(ent(svcS), -100), obs(ent(dd), -100), obs(ent(rr), -100))
	add := func(s identity.Fingerprint, n int, k int) {
		i, p := inst(n), pod(n)
		f.write(obs(ent(i), -100), obs(ent(p), -100))
		service(f, s, i, p)
		f.write(obs(sched(p, node(k)), -100))
	}
	for k := 1; k <= 6; k++ {
		f.write(obs(ent(node(k)), -100))
		add(svcS, 10+k, k)
		add(dd, 20+k, k)
	}
	add(rr, 31, 1)
	res := runExtract(t, st, extract.Exploratory{Service: svcS}, extract.Options{NodeOnly: true})
	if len(res.Suppressed) == 0 || !res.Ubiquity.Judged {
		t.Fatalf("suppressed %d, ubiquity %+v", len(res.Suppressed), res.Ubiquity)
	}
	sg, err := subgraph.FromExtraction(ctx, st, res, subgraph.ReadPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	checkDerived(t, st, sg, exWant(res))
	if len(sg.Sections.Suppressed) != len(res.Suppressed) || !sg.Sections.Ubiquity.Judged || sg.Sections.Ubiquity.FrontierNodes != 6 {
		t.Fatalf("suppressed %d, ubiquity %+v", len(sg.Sections.Suppressed), sg.Sections.Ubiquity)
	}
	for i, s := range sg.Sections.Suppressed {
		w := res.Suppressed[i]
		if s.Reason != subgraph.SuppressedUbiquitous || s.Coverage != w.Coverage || s.Key.A != w.Member || s.Key.B != w.Peer ||
			s.Key.Rule != subgraph.RulePairSharedNode || s.Class != subgraph.ClassStable || len(s.Spans) != len(w.Spans) {
			t.Errorf("suppressed %d: %+v, want %+v", i, s, w)
		}
		if hasKey(sg, s.Key) {
			t.Errorf("suppressed %s is an element", s.Key)
		}
		if s.Coverage[0] < 5 || s.Coverage[1] != 6 {
			t.Errorf("coverage %v", s.Coverage)
		}
	}
	// The peer that is not ubiquitous is an element; the one that is, is not.
	if len(derivedOf(sg, subgraph.RulePairSharedNode, svcS, rr)) != 1 || len(derivedOf(sg, subgraph.RulePairSharedNode, svcS, dd)) != 0 {
		t.Errorf("derived: %v", keysOfDerived(sg))
	}
}

// The extraction's own cuts, and each pod-level search's, travel: the Finds' node
// cuts are in no list of the extraction.
func TestFromExtractionTruncation(t *testing.T) {
	st, f := extractWorld(t)
	for i := 3; i <= 4; i++ {
		f.write(obs(ent(node(i)), -100), obs(ent(pod(10+i)), -100), obs(runsOn(node(i), hostH), -100), obs(sched(pod(10+i), node(i)), -100))
	}
	pol := extract.DefaultPolicy()
	pol.MaxPairs = 1
	res := runExtract(t, st, extract.Exploratory{Service: svcS}, extract.Options{Policy: pol})
	if res.Truncated.Pairs == 0 {
		t.Fatalf("the world cuts too few pairs: %+v", res.Truncated)
	}
	sg, err := subgraph.FromExtraction(ctx, st, res, subgraph.ReadPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if sg.Sections.Truncated.Pairs != res.Truncated.Pairs || len(derivedOf(sg, subgraph.RulePairSharedNode, svcS, svcT))+
		len(derivedOf(sg, subgraph.RulePairSharedPod, svcS, svcZ))+len(derivedOf(sg, subgraph.RulePairSharedHost, svcS, svcU)) != 1 {
		t.Errorf("pairs cut: %+v, %v", sg.Sections.Truncated, keysOfDerived(sg))
	}
	checkDerived(t, st, sg, exWant(res))

	// The searches' own caps are in no list of the extraction.
	pol = extract.DefaultPolicy()
	pol.CoResidency = coresidency.DefaultPolicy()
	pol.CoResidency.MaxNodes, pol.CoResidency.MaxResults = 1, 1
	res = runExtract(t, st, extract.Exploratory{Service: svcS}, extract.Options{Policy: pol})
	tr := res.Truncated
	var podCuts int
	for _, pr := range res.PodResults {
		if len(pr.Truncated.Nodes) > 0 || pr.Truncated.Results > 0 {
			podCuts++
		}
	}
	if podCuts == 0 {
		t.Fatalf("the world cuts too little: %+v", tr)
	}
	sg, err = subgraph.FromExtraction(ctx, st, res, subgraph.ReadPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	got := sg.Sections.Truncated
	if !slices.Equal(got.Members, tr.Members) || !slices.Equal(got.Instances, tr.Instances) || !slices.Equal(got.SeedPods, tr.SeedPods) ||
		got.Pairs != tr.Pairs || got.SuppressedDropped != tr.SuppressedDropped {
		t.Errorf("truncated %+v, want %+v", got, tr)
	}
	if len(sg.Sections.Pods) != len(res.PodResults) {
		t.Fatalf("pod sections %d, results %d", len(sg.Sections.Pods), len(res.PodResults))
	}
	for i, pr := range res.PodResults {
		ps := sg.Sections.Pods[i]
		g, w := ps.Truncated, pr.Truncated
		if ps.Seed != pr.Seed || !slices.Equal(g.Nodes, w.Nodes) || !slices.Equal(g.Hosts, w.Hosts) || g.Results != w.Results ||
			g.SuppressedDropped != w.SuppressedDropped || len(g.PerVia) != len(w.PerVia) {
			t.Errorf("pod section %d: %+v, want %+v", i, ps, pr.Truncated)
		}
	}
	checkDerived(t, st, sg, exWant(res))

	// The members, instances and seed pods the policy left out.
	pol = extract.DefaultPolicy()
	pol.MaxMembers, pol.MaxInstances, pol.MaxSeedPods = 2, 1, 1
	f.write(obs(ent(inst(30)), -100), obs(instOf(inst(30), svcS), -100))
	res = runExtract(t, st, extract.Exploratory{Service: svcS}, extract.Options{Policy: pol})
	tr = res.Truncated
	if len(tr.Members) == 0 || len(tr.Instances) == 0 {
		t.Fatalf("the world cuts too little: %+v", tr)
	}
	sg, err = subgraph.FromExtraction(ctx, st, res, subgraph.ReadPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	got = sg.Sections.Truncated
	if !slices.Equal(got.Members, tr.Members) || !slices.Equal(got.Instances, tr.Instances) || !slices.Equal(got.SeedPods, tr.SeedPods) {
		t.Errorf("truncated %+v, want %+v", got, tr)
	}
	for _, fp := range slices.Concat(got.Members, got.Instances) {
		if id, ok := sg.EntityID(fp); !ok || !sg.Entities[id].Boundary {
			t.Errorf("%s was left out and not read: in the dictionary %t", fp, ok)
		}
	}
	checkDerived(t, st, sg, exWant(res))
}

// Each pod-level search's quarantined edges are in the quarantine section and
// named by its pod section.
func TestFromExtractionQuarantine(t *testing.T) {
	st, f := openStore(t, memstore.Options{Policy: storetest.QuarantinePolicy()})
	boot := func(id string, sec int) store.Record {
		r := observe(ent(hostH), agent, sec, 15*time.Minute)
		r.Boot = id
		return r
	}
	for _, fp := range []identity.Fingerprint{svcS, svcT, podP1, podT, nodeN, nodeN2} {
		f.write(obs(ent(fp), -100))
	}
	service(f, svcS, instS, podP1)
	service(f, svcT, instT, podT)
	f.write(obs(sched(podP1, nodeN), -100), obs(sched(podT, nodeN2), -100),
		boot("b1", -100), boot("b2", -95), boot("b1", -90),
		observe(runsOn(nodeN, hostH), agent, -100, 0), observe(runsOn(nodeN2, hostH), agent, -100, 0))
	res := runExtract(t, st, extract.Exploratory{Service: svcS}, extract.Options{})
	var finds int
	for _, pr := range res.PodResults {
		finds += len(pr.Quarantined)
	}
	if finds == 0 {
		t.Fatalf("no Find reported a quarantined edge: %+v", res.PodResults)
	}
	sg, err := subgraph.FromExtraction(ctx, st, res, subgraph.ReadPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	section := quarantinedKeys(sg)
	var named int
	for i, pr := range res.PodResults {
		ps := sg.Sections.Pods[i]
		if len(ps.Quarantined) != len(pr.Quarantined) {
			t.Fatalf("pod section %d lists %d quarantined edges, the Find %d", i, len(ps.Quarantined), len(pr.Quarantined))
		}
		for j, k := range ps.Quarantined {
			if want := subgraph.KeyOf(pr.Quarantined[j].Subject); k != want {
				t.Errorf("pod section %d quarantined %d: %s, want %s", i, j, k, want)
			}
			if !slices.Contains(section, k) {
				t.Errorf("%s is named by a pod section and not in the quarantine section", k)
			}
			named++
		}
	}
	for _, k := range section {
		if hasKey(sg, k) {
			t.Errorf("%s is quarantined and an element", k)
		}
	}
	for _, kf := range sg.Keyframes {
		for _, e := range kf.Elements {
			if k := sg.Elements[e].Key; k.A == hostH || k.B == hostH {
				t.Errorf("a keyframe holds %s", k)
			}
		}
	}
	checkDerived(t, st, sg, exWant(res))
}

// An anchored extraction carries the anchor state.
func TestFromExtractionAnchor(t *testing.T) {
	st, _ := extractWorld(t)
	for _, tc := range []struct {
		name string
		host identity.Fingerprint
		want subgraph.AnchorState
	}{
		{"on the host", hostH, subgraph.AnchorOnHost},
		{"not on the host", host(9), subgraph.AnchorNotOnHost},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := runExtract(t, st, extract.Anchored{Service: svcS, Host: tc.host, At: at(15)}, extract.Options{})
			sg, err := subgraph.FromExtraction(ctx, st, res, subgraph.ReadPolicy{})
			if err != nil {
				t.Fatal(err)
			}
			if sg.Sections.Anchor != tc.want || sg.Sections.Anchor.String() != res.Anchor.String() {
				t.Errorf("anchor %s, result's %s, want %s", sg.Sections.Anchor, res.Anchor, tc.want)
			}
			checkDerived(t, st, sg, exWant(res))
		})
	}
}

// A pod-level search's own suppressed entries travel in its section, with their
// reason: they are in no list of the extraction.
func TestFromExtractionPodSuppressed(t *testing.T) {
	st, f := extractWorld(t)
	// A pod on the seed's node for 2 s, under the search's 5 s minimum.
	p9 := pod(9)
	f.write(obs(ent(p9), -100), obs(sched(p9, nodeN), 20), del(sched(p9, nodeN), 22))
	pol := extract.DefaultPolicy()
	pol.CoResidency = coresidency.DefaultPolicy()
	pol.CoResidency.MinOverlap = 5 * time.Second
	res := runExtract(t, st, extract.Exploratory{Service: svcS}, extract.Options{Policy: pol})
	var cut int
	for _, pr := range res.PodResults {
		cut += len(pr.Suppressed)
	}
	if cut == 0 {
		t.Fatal("no search suppresses an entry")
	}
	sg, err := subgraph.FromExtraction(ctx, st, res, subgraph.ReadPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	var got int
	for i, pr := range res.PodResults {
		ps := sg.Sections.Pods[i]
		if len(ps.Suppressed) != len(pr.Suppressed) {
			t.Fatalf("pod section %d: %d suppressed, the search %d", i, len(ps.Suppressed), len(pr.Suppressed))
		}
		for j, s := range ps.Suppressed {
			w := pr.Suppressed[j]
			if s.Key.A != pr.Seed || s.Key.B != w.Peer || s.Reason != subgraph.SuppressedBelowMinOverlap || s.Key.Rule != subgraph.RuleCoSharedNode {
				t.Errorf("pod section %d suppressed %d: %+v, want %+v", i, j, s, w)
			}
			if hasKey(sg, s.Key) {
				t.Errorf("suppressed %s is an element", s.Key)
			}
			got++
		}
	}
	if got != cut {
		t.Errorf("%d suppressed entries, want %d", got, cut)
	}
	if len(sg.Sections.Suppressed) != len(res.Suppressed) {
		t.Errorf("the extraction's own suppressed list changed: %d", len(sg.Sections.Suppressed))
	}
	checkDerived(t, st, sg, exWant(res))
}
