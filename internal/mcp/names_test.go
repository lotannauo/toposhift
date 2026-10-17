package mcp

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
)

func str(p *string) string {
	if p == nil {
		return "<null>"
	}
	return *p
}

// The rank fixture gives each producer a different name string, and only the
// winner's shows, with the producer it came from. Other ranks give another name.
func TestNameIsTheWinnersOnly(t *testing.T) {
	w := newWorld(t, rankObjs)
	ask := func(s *Source) (BlastRadiusOutput, Ref) {
		out, err := s.BlastRadius(ctx, BlastRadiusInput{Entity: w.h1.String(), From: w10, To: w1030})
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range out.Members {
			if m.Entity.FP == w.p1 {
				return out, m.Entity
			}
		}
		t.Fatal("p1 is not a member")
		return out, Ref{}
	}

	objsWins, p1 := ask(w.source(t, rankObjs, true))
	if str(p1.Name) != "web-0" || str(p1.Namespace) != "shop" || p1.NameFrom != string(objs) {
		t.Errorf("under k8sobjects ranked first: %s/%s from %s", str(p1.Namespace), str(p1.Name), p1.NameFrom)
	}
	b, err := json.Marshal(objsWins)
	if err != nil {
		t.Fatal(err)
	}
	for _, loser := range []string{"web-0-cluster", "shop-cluster"} {
		if strings.Contains(string(b), loser) {
			t.Errorf("a losing producer's value %q is in the answer", loser)
		}
	}

	_, p1 = ask(w.source(t, rankClus, true))
	if str(p1.Name) != "web-0-cluster" || str(p1.Namespace) != "shop-cluster" || p1.NameFrom != string(clus) {
		t.Errorf("under k8scluster ranked first: %s/%s from %s", str(p1.Namespace), str(p1.Name), p1.NameFrom)
	}
}

// A pod deleted inside a what_changed window keeps its name, from the window's
// first instant. A name that changed inside the window is the one at its last.
// An entity that was in neither end has a null name and says why.
func TestNameFromEitherEndOfTheWindow(t *testing.T) {
	w := newWorld(t, rankObjs)
	p8 := fpOf(t, catalog.K8sPod, catalog.K8sPodUID, "pod-uid-8")
	w.feed.write(
		observe(ent(p8), objs, hm(10, 10), 0, `{"name":"web-7","namespace":"shop"}`),
		deleted(ent(p8), objs, hm(10, 15)),
		observe(ent(w.n2), objs, hm(10, 21), 0, `{"name":"renamed-worker-2"}`),
	)
	s := w.source(t, rankObjs, true)
	out, err := s.WhatChanged(ctx, WhatChangedInput{
		Scope: []string{w.p2.String(), p8.String(), w.n2.String()}, From: w10, To: w1030,
	})
	if err != nil {
		t.Fatal(err)
	}
	byFP := map[identity.Fingerprint]Ref{}
	for _, r := range out.Scope {
		byFP[r.FP] = r
	}
	if r := byFP[w.p2]; str(r.Name) != "web-1" || r.NameFrom != string(objs) {
		t.Errorf("a pod deleted at 10:05 has the name %s (%s), want web-1 from the window's first instant", str(r.Name), r.NameFrom)
	}
	if r := byFP[w.n2]; str(r.Name) != "renamed-worker-2" {
		t.Errorf("a node renamed at 10:21 has the name %s, want the one at the window's last instant", str(r.Name))
	}
	if r := byFP[p8]; r.Name != nil || r.NameNote != "not present at either end of the window" {
		t.Errorf("a pod that came and went inside the window: name %s, note %q", str(r.Name), r.NameNote)
	}
}

// Hosts are unnamed on purpose, and a name is never taken from an identity
// attribute, from a key outside the table, or from a peer.
func TestHostsAndOtherTypesHaveNoName(t *testing.T) {
	w := newWorld(t, rankObjs)
	podNoName := fpOf(t, catalog.K8sPod, catalog.K8sPodUID, "uid-as-name-9")
	w.feed.write(
		// A host description that carries a name, a host name and the host.id.
		observe(ent(w.h1), agent, hm(9, 30), 0, `{"host.arch":"arm64","host.id":"`+hostSecret+`","host.name":"hostname-x","name":"name-of-host"}`),
		// A pod whose description has no name key, next to a node that has one.
		observe(ent(podNoName), objs, hm(9, 0), 0, `{"phase":"Running"}`),
		observe(edge(podNoName, w.n1, catalog.ScheduledOn), objs, hm(9, 0), 0, `{}`),
	)
	s := w.source(t, rankObjs, true)
	blast, err := s.BlastRadius(ctx, BlastRadiusInput{Entity: w.h1.String(), From: w10, To: w1030})
	if err != nil {
		t.Fatal(err)
	}
	if blast.Seed.Name != nil || blast.Seed.NameFrom != "" {
		t.Errorf("the host seed has the name %s", str(blast.Seed.Name))
	}
	for _, m := range blast.Members {
		if m.Entity.FP == podNoName && m.Entity.Name != nil {
			t.Errorf("a pod with no name in its description has the name %s", str(m.Entity.Name))
		}
	}
	for _, a := range allAnswers(t, w, s) {
		if a.echo {
			continue
		}
		for _, secret := range []string{hostSecret, "uid-as-name-9", "hostname-x", "name-of-host"} {
			if strings.Contains(string(a.json), secret) {
				t.Errorf("%s: %q is in the answer", a.name, secret)
			}
		}
	}
	// The same for a pod resolved from its uid: the uid is not its name.
	res, err := s.ResolveEntity(ctx, ResolveEntityInput{Type: "k8s.pod", Attributes: rawAttrs(`{"k8s.pod.uid":"uid-as-name-9"}`), At: w10})
	if err != nil {
		t.Fatal(err)
	}
	if res.Entity.Name != nil {
		t.Errorf("a pod resolved from its uid has the name %s", str(res.Entity.Name))
	}
}

func TestNameFields(t *testing.T) {
	for _, tc := range []struct {
		name    string
		typ     catalog.EntityType
		payload string
		wantN   string
		wantNS  string
	}{
		{"k8sobjects pod", catalog.K8sPod, `{"name":"a","namespace":"b"}`, "a", "b"},
		{"k8scluster pod", catalog.K8sPod, `{"k8s.pod.name":"a","k8s.namespace.name":"b"}`, "a", "b"},
		{"the first key wins", catalog.K8sPod, `{"k8s.pod.name":"second","name":"first"}`, "first", "<null>"},
		{"a pod's node is not its name", catalog.K8sPod, `{"k8s.node.name":"node"}`, "<null>", "<null>"},
		{"a number is not a name", catalog.K8sPod, `{"name":5}`, "<null>", "<null>"},
		{"an empty string is not a name", catalog.K8sPod, `{"name":""}`, "<null>", "<null>"},
		{"a nested name is not a top-level key", catalog.K8sPod, `{"owner":{"name":"x"}}`, "<null>", "<null>"},
		{"not an object", catalog.K8sPod, `["name"]`, "<null>", "<null>"},
		{"k8sobjects node", catalog.K8sNode, `{"name":"n"}`, "n", "<null>"},
		{"k8scluster node", catalog.K8sNode, `{"k8s.node.name":"n"}`, "n", "<null>"},
		{"k8sobjects container", catalog.Container, `{"name":"c","pod_uid":"u"}`, "c", "<null>"},
		{"k8scluster container", catalog.Container, `{"k8s.container.name":"c","k8s.namespace.name":"ns"}`, "c", "ns"},
		{"a host has none", catalog.Host, `{"name":"h","host.name":"h","host.id":"h"}`, "<null>", "<null>"},
		{"a service is named from its description", catalog.Service, `{"name":"s","service.name":"x"}`, "s", "<null>"},
		{"a service with a namespace", catalog.Service, `{"name":"s","namespace":"ns"}`, "s", "ns"},
		{"a service with no namespace", catalog.Service, `{"name":"s"}`, "s", "<null>"},
		{"a service instance has none", catalog.ServiceInstance, `{"name":"s","service.version":"1"}`, "<null>", "<null>"},
	} {
		n, ns, _ := nameFields(tc.typ, []byte(tc.payload), printable)
		if str(n) != tc.wantN || str(ns) != tc.wantNS {
			t.Errorf("%s: name %s, namespace %s; want %s, %s", tc.name, str(n), str(ns), tc.wantN, tc.wantNS)
		}
	}
}

// After a retention the interval reads that names need are refused: the names are
// null, the answer is whole, and one sentence says why.
func TestNamesAfterRetention(t *testing.T) {
	w := newWorld(t, rankObjs)
	if err := w.st.Retain(ctx, hm(9, 30)); err != nil {
		t.Fatal(err)
	}
	s := w.source(t, rankObjs, true)
	out, err := s.NeighborsAt(ctx, NeighborsAtInput{Entity: w.n1.String(), At: "2026-01-01T10:08:00Z"})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Edges) == 0 {
		t.Fatal("the point read lost its edges")
	}
	for _, r := range append([]Ref{out.Entity}, edgePeers(out.Edges)...) {
		if r.Name != nil {
			t.Errorf("%s has the name %s after a retention", r.ID, str(r.Name))
		}
	}
	var found bool
	for _, n := range out.Notes {
		if strings.HasPrefix(n, "Names are unavailable: The description of ") && strings.Contains(n, "was recorded with an event time before the retention horizon and is no longer kept.") {
			found = true
		}
	}
	if !found {
		t.Errorf("no sentence about the names in %q", out.Notes)
	}
}

func edgePeers(es []NeighborEdge) []Ref {
	var out []Ref
	for _, e := range es {
		out = append(out, e.Peer)
	}
	return out
}

// On a store without range reads no name is read, and the answer says so.
func TestNamesNeedRangeReads(t *testing.T) {
	w := newWorld(t, rankObjs)
	spy := &spyStore{Store: w.st}
	s, err := NewSource(Config{Store: spy, Rank: rankObjs})
	if err != nil {
		t.Fatal(err)
	}
	out, err := s.NeighborsAt(ctx, NeighborsAtInput{Entity: w.n1.String(), At: "2026-01-01T10:08:00Z"})
	if err != nil {
		t.Fatal(err)
	}
	if !contains(out.Notes, "Names need a range read, which this store does not offer.") {
		t.Errorf("notes %q", out.Notes)
	}
	for _, r := range append([]Ref{out.Entity}, edgePeers(out.Edges)...) {
		if r.Name != nil {
			t.Errorf("%s has the name %s", r.ID, str(r.Name))
		}
	}
	// The point reads are all there was: no EntityWindow, no Window.
	for _, a := range spy.asOf {
		if a != w.st.LastSeq() {
			t.Errorf("a read at the token %d", a)
		}
	}
}
