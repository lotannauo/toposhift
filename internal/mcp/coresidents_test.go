package mcp

import (
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
)

func coResidentOf(out CoResidentsOutput, peer identity.Fingerprint) *CoResident {
	for i, c := range out.CoResidents {
		if c.Peer.FP == peer {
			return &out.CoResidents[i]
		}
	}
	return nil
}

func reason(r *string) string {
	if r == nil {
		return "<null>"
	}
	return *r
}

// The co-residents of a pod, ranked by time together, each with the legs of its
// path and why each overlap ended.
func TestCoResidentsOfAPod(t *testing.T) {
	w := newWorld(t, rankObjs)
	s := w.source(t, rankObjs, true)
	out, err := s.CoResidents(ctx, CoResidentsInput{Pod: w.p1.String(), From: w10, To: w1030})
	if err != nil {
		t.Fatal(err)
	}
	var order []identity.Fingerprint
	for _, c := range out.CoResidents {
		order = append(order, c.Peer.FP)
	}
	// P7 shares the host for the whole window, then P3 (24 min), P4 (20), P6 (10), P2 (5).
	want := []identity.Fingerprint{w.p7, w.p3, w.p4, w.p6, w.p2}
	if len(order) != len(want) {
		t.Fatalf("co-residents %d, want %d", len(order), len(want))
	}
	for i := range want {
		if order[i] != want[i] {
			t.Errorf("co-resident %d is %s, want %s", i, order[i].String(), want[i].String())
		}
	}

	p7 := coResidentOf(out, w.p7)
	if p7.Mechanism != "shared_host" || p7.Rule != "co_residency.shared_host" || p7.Via.FP != w.h1 || len(p7.Nodes) != 2 || len(p7.Legs) != 4 ||
		p7.Subject != "derived" || p7.Basis != "derived" || p7.Class != "stable" {
		t.Errorf("P7: %+v", p7)
	}
	p3 := coResidentOf(out, w.p3)
	if p3.Mechanism != "shared_node" || p3.Rule != "co_residency.shared_node" || p3.Via.FP != w.n1 || p3.Duration != "24m0s" || p3.Class != "appeared" {
		t.Errorf("P3: %+v", p3)
	}

	// Why each overlap ended, by the leg that ended it.
	ends := func(c *CoResident) []string {
		var out []string
		for _, o := range c.Overlaps {
			out = append(out, reason(o.EndReason))
			for _, e := range o.Ends {
				out = append(out, "leg"+string(rune('0'+e.Leg))+":"+reason(e.EndReason))
			}
		}
		return out
	}
	for _, tc := range []struct {
		peer identity.Fingerprint
		want []string
	}{
		{w.p2, []string{"reach_ended", "leg1:cascade"}},
		{w.p6, []string{"reach_ended", "leg1:liveness_expiry"}},
		{w.p4, []string{"reach_ended", "leg1:producer", "<null>"}},
		{w.p3, []string{"<null>"}},
	} {
		if got := ends(coResidentOf(out, tc.peer)); !equal(got, tc.want) {
			t.Errorf("%s: ends %v, want %v", tc.peer, got, tc.want)
		}
	}

	// A path through the host is not read in node-only mode.
	only, err := s.CoResidents(ctx, CoResidentsInput{Pod: w.p1.String(), From: w10, To: w1030, NodeOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	if coResidentOf(only, w.p7) != nil || len(only.CoResidents) != 4 || !only.NodeOnly {
		t.Errorf("node only: %d co-residents", len(only.CoResidents))
	}
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// A placement that cannot be told is listed as unattributed, with its candidates,
// and a short overlap is suppressed rather than dropped.
func TestCoResidentsUnattributedAndSuppressed(t *testing.T) {
	w := newWorld(t, rankObjs)
	n4 := fpOf(t, catalog.K8sNode, catalog.K8sNodeUID, "node-uid-4") // no host
	a := fpOf(t, catalog.K8sPod, catalog.K8sPodUID, "pod-uid-a")
	b := fpOf(t, catalog.K8sPod, catalog.K8sPodUID, "pod-uid-b")
	two := fpOf(t, catalog.K8sPod, catalog.K8sPodUID, "pod-uid-two") // on two nodes at once
	short := fpOf(t, catalog.K8sPod, catalog.K8sPodUID, "pod-uid-short")
	nine := hm(9, 0)
	w.feed.write(
		observe(ent(n4), objs, nine, 0, `{"name":"worker-4"}`),
		observe(ent(a), objs, nine, 0, `{"name":"a"}`),
		observe(ent(b), objs, nine, 0, `{"name":"b"}`),
		observe(ent(two), objs, nine, 0, `{"name":"two"}`),
		observe(ent(short), objs, nine, 0, `{"name":"short"}`),
		observe(edge(a, n4, catalog.ScheduledOn), objs, nine, 0, `{}`),
		observe(edge(b, n4, catalog.ScheduledOn), objs, nine, 0, `{}`),
		observe(edge(two, n4, catalog.ScheduledOn), objs, nine, 0, `{}`),
		observe(edge(two, w.n2, catalog.ScheduledOn), objs, hm(10, 10), 0, `{}`),
		deleted(edge(two, w.n2, catalog.ScheduledOn), objs, hm(10, 20)),
		observe(edge(short, w.n1, catalog.ScheduledOn), objs, hm(10, 15).Add(200*time.Millisecond), 0, `{}`),
		deleted(edge(short, w.n1, catalog.ScheduledOn), objs, hm(10, 15).Add(700*time.Millisecond)),
	)
	s := w.source(t, rankObjs, true)

	out, err := s.CoResidents(ctx, CoResidentsInput{Pod: a.String(), From: w10, To: w1030})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.CoResidents) != 2 || coResidentOf(out, b).Mechanism != "shared_node" {
		t.Errorf("co-residents %d", len(out.CoResidents))
	}
	var no *UnattributedAt
	for i, u := range out.Unattributed {
		if u.Reason == "no_host" {
			no = &out.Unattributed[i]
		}
	}
	if no == nil || no.Entity.FP != n4 || no.Rule != "unattributed.no_host" || no.Subject != "derived" || no.Basis != "derived" ||
		len(no.Spans) != 1 || no.Spans[0].EndReason != nil {
		t.Errorf("a node with no host: %+v", out.Unattributed)
	}

	two2, err := s.CoResidents(ctx, CoResidentsInput{Pod: two.String(), From: w10, To: w1030})
	if err != nil {
		t.Fatal(err)
	}
	var several *UnattributedAt
	for i, u := range two2.Unattributed {
		if u.Reason == "several_nodes" {
			several = &two2.Unattributed[i]
		}
	}
	if several == nil || several.Entity.FP != two || len(several.Candidates) != 2 || several.Rule != "unattributed.several_nodes" {
		t.Fatalf("a pod on two nodes: %+v", two2.Unattributed)
	}
	// The ambiguity ends inside the window: the value is reach_ended, and the
	// sentence does not point at legs, because an unattributed span lists none.
	if sp := several.Spans[0]; sp.End != "2026-01-01T10:20:00Z" || reason(sp.EndReason) != "reach_ended" || sp.EndSentence != "the ambiguity ended" {
		t.Errorf("the span of an ambiguity: %+v", sp)
	}

	p1, err := s.CoResidents(ctx, CoResidentsInput{Pod: w.p1.String(), From: w10, To: w1030})
	if err != nil {
		t.Fatal(err)
	}
	if len(p1.Suppressed) != 1 || p1.Suppressed[0].Peer.FP != short || p1.Suppressed[0].SuppressedReason != "below_min_overlap" ||
		p1.Counts.Suppressed != 1 || coResidentOf(p1, short) != nil {
		t.Errorf("suppressed: %+v", p1.Suppressed)
	}
	if !contains(p1.Notes, "Suppressed co-residents overlapped for less than 1s inside the window, which event times cannot tell from an artefact of ordering.") {
		t.Errorf("notes %q", p1.Notes)
	}
}

// Only a pod is a seed.
func TestCoResidentsSeedMustBeAPod(t *testing.T) {
	w := newWorld(t, rankObjs)
	s := w.source(t, rankObjs, true)
	_, err := s.CoResidents(ctx, CoResidentsInput{Pod: w.n1.String(), From: w10, To: w1030})
	wantErr(t, "a node as seed", err, "The request was refused: ")
}
