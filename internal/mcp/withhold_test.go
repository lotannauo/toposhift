package mcp

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/graphdiff"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
	"github.com/lotannauo/toposhift/internal/store/pebblestore"
)

const w1012 = "2026-01-01T10:12:00Z"

var errBoom = errors.New("boom: the disk is on fire")

func endpointFP(tb testing.TB, realm, addr string) identity.Fingerprint {
	tb.Helper()
	return fpOf(tb, catalog.NetworkEndpoint, catalog.EndpointRealm, realm, catalog.EndpointAddress, addr)
}

// ids lists the texts of fingerprints, sorted.
func ids(fps ...identity.Fingerprint) []string {
	var out []string
	for _, fp := range fps {
		out = append(out, fp.String())
	}
	slices.Sort(out)
	return out
}

func idSet(fps ...identity.Fingerprint) map[string]bool {
	out := map[string]bool{}
	for _, fp := range fps {
		out[fp.String()] = true
	}
	return out
}

// addrWorld is the fixture of the withholding tests, in the realm k8s/c1, with
// every endpoint observed before it is claimed. The window is [10:00, 10:30).
//
//	pod P (web-a), claims E1 and E2 from 9:00, E5 from 10:10, E6 from 10:05 to 10:20;
//	    E2 is deleted at 10:15; P is scheduled on node N from 9:00 and on N2 from 10:10
//	pod R is scheduled on N from 10:10
//	node N claims E3 (k8s/c1) and E4 (global) from 9:00 and E8 from 10:10
//	pod Q claims E7 from 9:00
//
// Q is the pod a spy store quarantines: no pod or node can be quarantined through
// a write, because only a host record carries a boot id.
type addrWorld struct {
	st                 store.Store
	feed               *feed
	p, q, r, n, n2     identity.Fingerprint
	e                  [9]identity.Fingerprint // e[1]..e[8]
	addrs              [9]string
	pPayload, nPayload string
}

func newAddrWorld(tb testing.TB) *addrWorld {
	tb.Helper()
	return newAddrWorldOn(tb, openMem(tb, memstore.Options{Policy: servePolicy(rankObjs)}))
}

// newAddrWorldOn is the fixture written to st, which must be empty and opened
// with servePolicy(rankObjs).
func newAddrWorldOn(tb testing.TB, st store.Store) *addrWorld {
	tb.Helper()
	w := &addrWorld{
		st: st, feed: newFeed(tb, st),
		p:  fpOf(tb, catalog.K8sPod, catalog.K8sPodUID, "addr-pod-p"),
		q:  fpOf(tb, catalog.K8sPod, catalog.K8sPodUID, "addr-pod-q"),
		r:  fpOf(tb, catalog.K8sPod, catalog.K8sPodUID, "addr-pod-r"),
		n:  fpOf(tb, catalog.K8sNode, catalog.K8sNodeUID, "addr-node-n"),
		n2: fpOf(tb, catalog.K8sNode, catalog.K8sNodeUID, "addr-node-n2"),
	}
	w.addrs = [9]string{"", "10.42.0.7", "10.42.0.8", "10.12.0.3", "1.2.3.4", "10.42.0.9", "10.42.0.10", "10.42.0.11", "10.12.0.4"}
	for i := 1; i <= 8; i++ {
		realm := "k8s/c1"
		if i == 4 {
			realm = "global"
		}
		w.e[i] = endpointFP(tb, realm, w.addrs[i])
	}
	e := w.e
	nine := hm(9, 0)
	w.pPayload = `{"name":"web-a","namespace":"shop","pod_ips":["10.42.0.7","10.42.0.8"]}`
	w.nPayload = `{"addresses":[{"address":"10.12.0.3","type":"InternalIP"},{"address":"1.2.3.4","type":"ExternalIP"}],"name":"worker-n"}`
	recs := []store.Record{
		observe(ent(w.p), objs, nine, 0, w.pPayload),
		observe(ent(w.q), objs, nine, 0, `{"name":"web-q","namespace":"shop","pod_ips":["10.42.0.11"]}`),
		observe(ent(w.r), objs, nine, 0, `{"name":"web-r","namespace":"shop"}`),
		observe(ent(w.n), objs, nine, 0, w.nPayload),
		observe(ent(w.n2), objs, nine, 0, `{"name":"worker-n2"}`),
	}
	for i := 1; i <= 8; i++ {
		recs = append(recs, observe(ent(e[i]), objs, nine, 0, `{}`))
	}
	claim := func(holder, ep identity.Fingerprint, at time.Time) store.Record {
		return observe(edge(holder, ep, catalog.HasAddress), objs, at, 0, `{}`)
	}
	recs = append(recs,
		observe(edge(w.p, w.n, catalog.ScheduledOn), objs, nine, 0, `{}`),
		claim(w.p, e[1], nine),
		claim(w.p, e[2], nine),
		claim(w.n, e[3], nine),
		claim(w.n, e[4], nine),
		claim(w.q, e[7], nine),
		claim(w.p, e[6], hm(10, 5)),
		claim(w.p, e[5], hm(10, 10)),
		claim(w.n, e[8], hm(10, 10)),
		observe(edge(w.p, w.n2, catalog.ScheduledOn), objs, hm(10, 10), 0, `{}`),
		observe(edge(w.r, w.n, catalog.ScheduledOn), objs, hm(10, 10), 0, `{}`),
		deleted(edge(w.p, e[2], catalog.HasAddress), objs, hm(10, 15)),
		deleted(edge(w.p, e[6], catalog.HasAddress), objs, hm(10, 20)),
	)
	w.feed.write(recs...)
	return w
}

// quarStore reports a quarantine for the entities it is given, as a store does
// for an entity with two boots live at once.
type quarStore struct {
	store.Store
	quarantined map[identity.Fingerprint]bool
}

func (q quarStore) Alive(ctx context.Context, fp identity.Fingerprint, t time.Time, sc store.Scope) (bool, error) {
	if q.quarantined[fp] {
		return false, &store.QuarantineError{Entity: fp, Layer: sc.Layer}
	}
	return q.Store.Alive(ctx, fp, t, sc)
}

// failAlive fails the read of one entity.
type failAlive struct {
	store.Store
	fp  identity.Fingerprint
	err error
}

func (f failAlive) Alive(ctx context.Context, fp identity.Fingerprint, t time.Time, sc store.Scope) (bool, error) {
	if fp == f.fp {
		return false, f.err
	}
	return f.Store.Alive(ctx, fp, t, sc)
}

// logSink collects the lines a source logs.
type logSink struct {
	mu    sync.Mutex
	lines []string
}

func (l *logSink) logf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

func (l *logSink) all() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.lines)
}

func sourceOn(tb testing.TB, st store.Store, h *egressHooks, logf func(string, ...any)) *Source {
	tb.Helper()
	s, err := NewSource(Config{Store: st, Rank: rankObjs, RangeReads: true, Logf: logf})
	if err != nil {
		tb.Fatal(err)
	}
	if h != nil {
		hh := *h
		s.hooks = &hh
	}
	return s
}

// addrSources are the three stores the asks run on: the fixture's, one in which
// Q is quarantined, and one in which E7 is.
type addrSources struct{ plain, quarQ, quarE7 *Source }

func (w *addrWorld) sources(tb testing.TB, h *egressHooks) addrSources {
	tb.Helper()
	return addrSources{
		plain:  sourceOn(tb, w.st, h, nil),
		quarQ:  sourceOn(tb, quarStore{w.st, map[identity.Fingerprint]bool{w.q: true}}, h, nil),
		quarE7: sourceOn(tb, quarStore{w.st, map[identity.Fingerprint]bool{w.e[7]: true}}, h, nil),
	}
}

type askOn int

const (
	onPlain askOn = iota
	onQuarQ
	onQuarE7
)

// ask is one question to one tool: the endpoints its request names, and the
// endpoints its answer prints, worked out by hand, when nothing is withheld.
type ask struct {
	name  string
	tool  string
	on    askOn
	named []identity.Fingerprint
	leaks []identity.Fingerprint
	do    func(s *Source) (any, error)
}

func (a addrSources) of(on askOn) *Source {
	switch on {
	case onQuarQ:
		return a.quarQ
	case onQuarE7:
		return a.quarE7
	}
	return a.plain
}

func (w *addrWorld) asks() []ask {
	e := w.e
	nb := func(name string, on askOn, fp identity.Fingerprint, dir string, leaks ...identity.Fingerprint) ask {
		return ask{
			name: name, tool: "neighbors_at", on: on, leaks: leaks,
			do: func(s *Source) (any, error) {
				return s.NeighborsAt(ctx, NeighborsAtInput{Entity: fp.String(), At: w1012, Direction: dir})
			},
		}
	}
	diff := func(name string, scope []identity.Fingerprint, leaks ...identity.Fingerprint) ask {
		return ask{
			name: name, tool: "what_changed", leaks: leaks,
			do: func(s *Source) (any, error) {
				return s.WhatChanged(ctx, WhatChangedInput{Scope: stringsOf(scope), From: w10, To: w1030})
			},
		}
	}
	diffOn := func(name string, on askOn, scope []identity.Fingerprint, leaks ...identity.Fingerprint) ask {
		a := diff(name, scope, leaks...)
		a.on = on
		return a
	}
	return []ask{
		{name: "blast N over the window", tool: "blast_radius", do: func(s *Source) (any, error) {
			return s.BlastRadius(ctx, BlastRadiusInput{Entity: w.n.String(), From: w10, To: w1030})
		}},
		{name: "blast P at 10:12", tool: "blast_radius", do: func(s *Source) (any, error) {
			return s.BlastRadius(ctx, BlastRadiusInput{Entity: w.p.String(), At: w1012})
		}},
		{name: "co-residents of P", tool: "co_residents", do: func(s *Source) (any, error) {
			return s.CoResidents(ctx, CoResidentsInput{Pod: w.p.String(), From: w10, To: w1030})
		}},
		nb("neighbors of P, forward", onPlain, w.p, "forward", e[1], e[2], e[5], e[6]),
		nb("neighbors of P, reverse", onPlain, w.p, "reverse"),
		nb("neighbors of P, both", onPlain, w.p, "both", e[1], e[2], e[5], e[6]),
		nb("neighbors of N, forward", onPlain, w.n, "forward", e[3], e[4], e[8]),
		nb("neighbors of N, reverse", onPlain, w.n, "reverse"),
		nb("neighbors of Q, forward", onPlain, w.q, "forward", e[7]),
		nb("neighbors of a quarantined Q, forward", onQuarQ, w.q, "forward", e[7]),
		nb("neighbors of a quarantined Q, reverse", onQuarQ, w.q, "reverse"),
		nb("neighbors of Q with a quarantined E7, forward", onQuarE7, w.q, "forward", e[7]),
		{name: "history of P", tool: "entity_history", do: func(s *Source) (any, error) {
			return s.EntityHistory(ctx, EntityHistoryInput{Entity: w.p.String(), From: "2026-01-01T09:00:00Z", To: w1030})
		}},
		{name: "history of N", tool: "entity_history", do: func(s *Source) (any, error) {
			return s.EntityHistory(ctx, EntityHistoryInput{Entity: w.n.String(), From: "2026-01-01T09:00:00Z", To: w1030})
		}},
		diff("changes of P and N", []identity.Fingerprint{w.p, w.n}, e[2], e[5], e[6], e[8]),
		diff("changes of P", []identity.Fingerprint{w.p}, e[2], e[5], e[6]),
		{name: "resolve P", tool: "resolve_entity", do: func(s *Source) (any, error) {
			return s.ResolveEntity(ctx, ResolveEntityInput{
				Type: "k8s.pod", Attributes: rawAttrs(`{"k8s.pod.uid":"addr-pod-p"}`), At: w1012,
			})
		}},
		{name: "resolve P without an instant", tool: "resolve_entity", do: func(s *Source) (any, error) {
			return s.ResolveEntity(ctx, ResolveEntityInput{Type: "k8s.pod", Attributes: rawAttrs(`{"k8s.pod.uid":"addr-pod-p"}`)})
		}},
		{name: "history of E1", tool: "entity_history", named: []identity.Fingerprint{e[1]}, do: func(s *Source) (any, error) {
			return s.EntityHistory(ctx, EntityHistoryInput{Entity: e[1].String(), From: "2026-01-01T09:00:00Z", To: w1030})
		}},
		diffOn("changes of a quarantined Q", onQuarQ, []identity.Fingerprint{w.q}, e[7]),
		{name: "schema", tool: "describe_schema", do: func(s *Source) (any, error) {
			return s.DescribeSchema(ctx, DescribeSchemaInput{})
		}},
	}
}

func stringsOf(fps []identity.Fingerprint) []string {
	out := make([]string, len(fps))
	for i, fp := range fps {
		out[i] = fp.String()
	}
	return out
}

// askJSON puts the question and returns the answer's JSON.
func askJSON(tb testing.TB, a ask, srcs addrSources) []byte {
	tb.Helper()
	v, err := a.do(srcs.of(a.on))
	if err != nil {
		tb.Fatalf("%s: %v", a.name, err)
	}
	b, err := json.Marshal(v)
	if err != nil {
		tb.Fatal(err)
	}
	return b
}

// The pattern is built from the catalog's name for the type and matches the text
// of an endpoint's id, and of no other type's.
func TestEndpointIDPatternMatchesAnEndpointRef(t *testing.T) {
	fp := endpointFP(t, "k8s/c1", "10.42.0.7")
	if got := endpointID.FindString(ref(fp).ID); got != ref(fp).ID {
		t.Errorf("the pattern found %q in %q", got, ref(fp).ID)
	}
	pod := fpOf(t, catalog.K8sPod, catalog.K8sPodUID, "x")
	if endpointID.MatchString(ref(pod).ID) {
		t.Errorf("the pattern matches a pod's id %s", ref(pod).ID)
	}
	if endpointID.MatchString(fp.String()[:len(fp.String())-1]) {
		t.Error("the pattern matches 31 digits")
	}
}

func TestNewSourceLeavesTheHooksNil(t *testing.T) {
	w := newAddrWorld(t)
	s, err := NewSource(Config{Store: w.st})
	if err != nil {
		t.Fatal(err)
	}
	if s.hooks != nil {
		t.Error("a source from NewSource has test hooks")
	}
}

func TestRequestNamesWhatItWasGiven(t *testing.T) {
	w := newAddrWorld(t)
	s := sourceOn(t, w.st, nil, nil)
	r := s.newRequest()
	r.nameText("not an id", w.p.String()+" and "+w.e[1].String()+", "+w.e[2].String()[:30], strings.ToUpper(w.e[3].String()))
	if want := idSet(w.e[1]); !reflect.DeepEqual(r.named, want) {
		t.Errorf("named %v, want %v", r.named, want)
	}
	r.name(w.p)
	r.name(w.e[4])
	if want := idSet(w.e[1], w.e[4]); !reflect.DeepEqual(r.named, want) {
		t.Errorf("named %v, want %v: a pod is not an endpoint", r.named, want)
	}
	for _, tc := range []struct {
		fp   identity.Fingerprint
		want bool
	}{{w.e[1], false}, {w.e[4], false}, {w.e[2], true}, {w.p, false}} {
		if got := r.hides(tc.fp); got != tc.want {
			t.Errorf("hides(%s) = %t", tc.fp, got)
		}
	}
	for _, tc := range []struct {
		name string
		sub  store.Subject
		want bool
	}{
		{"a named endpoint", ent(w.e[1]), false},
		{"an unnamed endpoint", ent(w.e[2]), true},
		{"a pod", ent(w.p), false},
		{"an edge to a named endpoint", edge(w.p, w.e[1], catalog.HasAddress), false},
		{"an edge to an unnamed endpoint", edge(w.p, w.e[2], catalog.HasAddress), true},
		{"an edge between named ends", edge(w.p, w.n, catalog.ScheduledOn), false},
	} {
		if got := r.hidesSubject(tc.sub); got != tc.want {
			t.Errorf("hidesSubject(%s) = %t, want %t", tc.name, got, tc.want)
		}
	}
	r.noFilter = true
	if r.hides(w.e[2]) || r.hidesSubject(ent(w.e[2])) {
		t.Error("a request with the filter off hides")
	}
}

// With the filters and the last check off, the questions of the main test print
// the ids: the fixture reaches every path, and the scan sees what is printed.
// Without this, a fixture with no endpoint edges would pass the main test
// vacuously.
func TestWithholdNegativeControl(t *testing.T) {
	w := newAddrWorld(t)
	srcs := w.sources(t, &egressHooks{noFilter: true, noBackstop: true})
	leaked := map[string]bool{}
	for _, a := range w.asks() {
		b := askJSON(t, a, srcs)
		got := unnamedEndpoints(b, idSet(a.named...))
		for _, id := range got {
			leaked[id] = true
		}
		if want := ids(a.leaks...); !slices.Equal(got, want) {
			t.Errorf("%s: with nothing withheld the answer prints %v, want %v", a.name, got, want)
		}
	}
	if len(leaked) != 8 {
		t.Errorf("the control reached %d endpoints, want all 8", len(leaked))
	}

	// The quarantined paths.
	out, err := srcs.quarQ.NeighborsAt(ctx, NeighborsAtInput{Entity: w.q.String(), At: w1012, Direction: "forward"})
	if err != nil || len(out.Edges) != 1 || out.Edges[0].Peer.FP != w.e[7] || out.Edges[0].Quarantined == nil || out.Edges[0].Quarantined.Entity.FP != w.q {
		t.Errorf("Q's uncut edge to E7: %v, %+v", err, out.Edges)
	}
	out, err = srcs.quarE7.NeighborsAt(ctx, NeighborsAtInput{Entity: w.q.String(), At: w1012, Direction: "forward"})
	if err != nil || len(out.Quarantined) != 1 || out.Quarantined[0].Entity == nil || out.Quarantined[0].Entity.FP != w.e[7] {
		t.Errorf("E7 in the quarantined list: %v, %+v", err, out.Quarantined)
	}

	// The diff, by hand: a quarantined edge to E7.
	res := quarantinedDiff(w)
	req := &request{named: map[string]bool{}, noFilter: true}
	kept, _, wh := withholdDiff(res, nil, req)
	if len(kept.Quarantined) != len(res.Quarantined) || wh != (ChangeWithheld{}) {
		t.Errorf("withholdDiff with the filter off dropped %d of %d quarantined elements: %+v", len(res.Quarantined)-len(kept.Quarantined), len(res.Quarantined), wh)
	}
}

// The questions of the negative control, with the filters on, print no endpoint
// id the request did not name and none of the fixture's addresses, carry the
// policy version, and name what was withheld.
func TestWithholdEveryTool(t *testing.T) {
	wm := newAddrWorld(t)
	wp := newAddrWorldOn(t, openPeb(t, servePolicy(rankObjs), pebblestore.DefaultCheckpoints(), pebblestore.Synchronous))
	sm, sp := wm.sources(t, nil), wp.sources(t, nil)
	for _, a := range wm.asks() {
		b := askJSON(t, a, sm)
		if bp := askJSON(t, a, sp); !bytes.Equal(b, bp) {
			t.Errorf("%s differs:\nmemstore    %s\npebblestore %s", a.name, b, bp)
		}
		if got := unnamedEndpoints(b, idSet(a.named...)); len(got) > 0 {
			t.Errorf("%s: the answer prints %v", a.name, got)
		}
		for _, addr := range wm.addrs[1:] {
			if strings.Contains(string(b), addr) {
				t.Errorf("%s: the answer prints the address %s", a.name, addr)
			}
		}
		var m struct {
			Policy struct {
				Withhold int `json:"withhold"`
			} `json:"policy"`
		}
		if err := json.Unmarshal(b, &m); err != nil || m.Policy.Withhold != WithholdVersion || WithholdVersion != 1 {
			t.Errorf("%s: policy.withhold %d, want 1 (%v)", a.name, m.Policy.Withhold, err)
		}
		if len(a.leaks) > 0 {
			phrase := map[string]string{"neighbors_at": "were withheld: ", "what_changed": "Withheld because each names an endpoint"}[a.tool]
			if !strings.Contains(string(b), phrase) && !strings.Contains(string(b), "were withheld, and") {
				t.Errorf("%s: the answer does not say it withheld", a.name)
			}
		}
	}
}

// The counts and the sentences of neighbors_at, worked out by hand from the
// fixture.
func TestWithholdNeighborsAtCounts(t *testing.T) {
	w := newAddrWorld(t)
	srcs := w.sources(t, nil)
	at := func(s *Source, fp identity.Fingerprint, dir string) NeighborsAtOutput {
		out, err := s.NeighborsAt(ctx, NeighborsAtInput{Entity: fp.String(), At: w1012, Direction: dir})
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	peers := func(out NeighborsAtOutput) []string {
		var ps []string
		for _, e := range out.Edges {
			ps = append(ps, e.Peer.ID)
		}
		slices.Sort(ps)
		return ps
	}
	plain := func(withheld, quarantined int) string {
		note := fmt.Sprintf("%d edges to endpoints the request did not name were withheld", withheld)
		if quarantined > 0 {
			note += fmt.Sprintf(", and %d quarantined endpoints", quarantined)
		}
		return note + ": an endpoint's id reveals its address. Name the endpoint, by its id or through resolve_entity, to see it."
	}

	// P forward at 10:12: N and N2 are listed; E1, E2 (ended at 10:15), E5 (from
	// 10:10) and E6 (10:05 to 10:20) are withheld.
	out := at(srcs.plain, w.p, "forward")
	if want := ids(w.n, w.n2); !slices.Equal(peers(out), want) {
		t.Errorf("P forward lists %v, want %v", peers(out), want)
	}
	if want := (NeighborCounts{Edges: 2, Shown: 2, Withheld: NeighborWithheld{Edges: 4}}); out.Counts != want {
		t.Errorf("P forward counts %+v, want %+v", out.Counts, want)
	}
	if !contains(out.Notes, plain(4, 0)) {
		t.Errorf("P forward notes %q", out.Notes)
	}
	// N both: P and R are listed; E3, E4 and E8 are withheld.
	out = at(srcs.plain, w.n, "both")
	if want := ids(w.p, w.r); !slices.Equal(peers(out), want) {
		t.Errorf("N both lists %v, want %v", peers(out), want)
	}
	if want := (NeighborCounts{Edges: 2, Shown: 2, Withheld: NeighborWithheld{Edges: 3}}); out.Counts != want {
		t.Errorf("N both counts %+v, want %+v", out.Counts, want)
	}
	if !contains(out.Notes, plain(3, 0)) {
		t.Errorf("N both notes %q", out.Notes)
	}
	// Nothing is withheld, and nothing is said.
	out = at(srcs.plain, w.p, "reverse")
	if out.Counts.Withheld != (NeighborWithheld{}) || len(out.Notes) != 1 || strings.Contains(out.Notes[0], "withheld") {
		t.Errorf("P reverse: %+v, %q", out.Counts, out.Notes)
	}
	// Q is quarantined: its edge is listed uncut in a store, and here withheld,
	// while Q itself is listed as quarantined.
	out = at(srcs.quarQ, w.q, "forward")
	if len(out.Edges) != 0 || out.Counts.Withheld != (NeighborWithheld{Edges: 1}) || len(out.Quarantined) != 1 || out.Quarantined[0].Entity.FP != w.q {
		t.Errorf("Q quarantined: %+v, quarantined %+v", out.Counts, out.Quarantined)
	}
	if !contains(out.Notes, plain(1, 0)) {
		t.Errorf("Q quarantined notes %q", out.Notes)
	}
	// E7 is quarantined: the edge and the quarantined endpoint are both withheld.
	out = at(srcs.quarE7, w.q, "forward")
	if len(out.Edges) != 0 || len(out.Quarantined) != 0 || out.Counts.Withheld != (NeighborWithheld{Edges: 1, Quarantined: 1}) {
		t.Errorf("E7 quarantined: %+v, edges %+v, quarantined %+v", out.Counts, out.Edges, out.Quarantined)
	}
	if !contains(out.Notes, plain(1, 1)) {
		t.Errorf("E7 quarantined notes %q", out.Notes)
	}
}

// What a request that names an endpoint still sees, so that nothing is withheld
// that was asked for.
func TestWithholdNamedPositives(t *testing.T) {
	w := newAddrWorld(t)
	s := sourceOn(t, w.st, nil, nil)

	out, err := s.NeighborsAt(ctx, NeighborsAtInput{Entity: w.e[1].String(), At: w1012, Direction: "reverse"})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Edges) != 1 || out.Edges[0].Peer.FP != w.p || out.Edges[0].Peer.Name == nil || *out.Edges[0].Peer.Name != "web-a" || out.Counts.Withheld != (NeighborWithheld{}) {
		t.Errorf("the reverse edges of E1: %+v, counts %+v", out.Edges, out.Counts)
	}
	if out.Entity.ID != w.e[1].String() {
		t.Errorf("entity %s", out.Entity.ID)
	}

	subjects := func(es []ChangeEntry) []store.Subject {
		var out []store.Subject
		for _, e := range es {
			out = append(out, entrySubject(e))
		}
		return out
	}
	// A scope that names E5: P to E5 is created, with its appearance; E2, E6 and
	// nothing else are withheld.
	ch, err := s.WhatChanged(ctx, WhatChangedInput{Scope: []string{w.p.String(), w.e[5].String()}, From: w10, To: w1030})
	if err != nil {
		t.Fatal(err)
	}
	wantCreated := []store.Subject{edge(w.p, w.n2, catalog.ScheduledOn), edge(w.p, w.e[5], catalog.HasAddress)}
	slices.SortFunc(wantCreated, store.CompareSubjects)
	if got := subjects(ch.Created); !slices.Equal(got, wantCreated) {
		t.Errorf("created %v, want %v", got, wantCreated)
	}
	var appeared []store.Subject
	for _, ev := range ch.Events {
		if ev.Event == "appeared" && ev.Edge != nil {
			appeared = append(appeared, store.EdgeSubject(ev.Edge.From.FP, ev.Edge.To.FP, catalog.RelationType(ev.Edge.Relation)))
		}
	}
	slices.SortFunc(appeared, store.CompareSubjects)
	if !slices.Equal(appeared, wantCreated) {
		t.Errorf("appearances %v, want %v", appeared, wantCreated)
	}
	want := ChangeWithheld{Deleted: Pair{Relations: 1}, Transient: Pair{Relations: 1}, Events: 3}
	if ch.Counts.Withheld != want {
		t.Errorf("withheld %+v, want %+v", ch.Counts.Withheld, want)
	}

	// A scope that names E2: P to E2 is deleted; E5 and E6 are withheld.
	ch, err = s.WhatChanged(ctx, WhatChangedInput{Scope: []string{w.p.String(), w.e[2].String()}, From: w10, To: w1030})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := subjects(ch.Deleted), []store.Subject{edge(w.p, w.e[2], catalog.HasAddress)}; !slices.Equal(got, want) {
		t.Errorf("deleted %v, want %v", got, want)
	}
	if want := (ChangeWithheld{Created: Pair{Relations: 1}, Transient: Pair{Relations: 1}, Events: 3}); ch.Counts.Withheld != want {
		t.Errorf("withheld %+v, want %+v", ch.Counts.Withheld, want)
	}

	// resolve_entity names the id it computes, on both paths.
	for _, at := range []string{w1012, ""} {
		res, err := s.ResolveEntity(ctx, ResolveEntityInput{
			Type: "topo.endpoint", Attributes: rawAttrs(`{"topo.endpoint.realm":"k8s/c1","topo.endpoint.address":"10.42.0.7"}`), At: at,
		})
		if err != nil {
			t.Fatalf("resolve with at %q: %v", at, err)
		}
		if res.Entity.ID != w.e[1].String() || res.Attributes["topo.endpoint.address"] != "10.42.0.7" {
			t.Errorf("resolve with at %q: %+v", at, res)
		}
		if at != "" && (res.Existence == nil || res.Existence.Status != "alive") {
			t.Errorf("resolve with at %q: existence %+v", at, res.Existence)
		}
	}
	// Only the id it computes: an address that is not E1's does not name E1.
	res, err := s.ResolveEntity(ctx, ResolveEntityInput{
		Type: "topo.endpoint", Attributes: rawAttrs(`{"topo.endpoint.realm":"k8s/c1","topo.endpoint.address":"10.42.0.99"}`), At: w1012,
	})
	if err != nil || res.Existence == nil || res.Existence.Status != "not_alive" {
		t.Errorf("an address nobody claims: %v, %+v", err, res.Existence)
	}
}

func entrySubject(e ChangeEntry) store.Subject {
	if e.Entity != nil {
		return store.EntitySubject(e.Entity.FP)
	}
	return store.EdgeSubject(e.Edge.From.FP, e.Edge.To.FP, catalog.RelationType(e.Edge.Relation))
}

// hasEndpointEnd says, independently of the code under test, that a subject has
// an endpoint at one of its ends.
func hasEndpointEnd(s store.Subject) bool {
	if s.A.Type() == catalog.NetworkEndpoint {
		return true
	}
	return s.Kind == store.SubjectEdge && s.B.Type() == catalog.NetworkEndpoint
}

// The counts of what_changed per bucket equal the entries of the diff package's
// own result that name an endpoint the scope does not name, and each list is the
// rest, in order.
func TestWithholdWhatChangedMatchesTheDiff(t *testing.T) {
	w := newAddrWorld(t)
	s := sourceOn(t, w.st, nil, nil)
	scope := []identity.Fingerprint{w.p, w.n}
	out, err := s.WhatChanged(ctx, WhatChangedInput{Scope: stringsOf(scope), From: w10, To: w1030})
	if err != nil {
		t.Fatal(err)
	}
	slices.SortFunc(scope, store.CompareFingerprints)
	res, err := graphdiff.NewDiffer(w.st).Diff(ctx, scope, hm(10, 0), hm(10, 30), w.st.LastSeq(), graphdiff.Options{Rank: rankObjs})
	if err != nil {
		t.Fatal(err)
	}

	split := func(es []graphdiff.Entry) (kept []store.Subject, withheld Pair) {
		for _, e := range es {
			sub := e.Element.Subject
			switch {
			case !hasEndpointEnd(sub):
				kept = append(kept, sub)
			case sub.Kind == store.SubjectEntity:
				withheld.Entities++
			default:
				withheld.Relations++
			}
		}
		return kept, withheld
	}
	subjects := func(es []ChangeEntry) []store.Subject {
		var out []store.Subject
		for _, e := range es {
			out = append(out, entrySubject(e))
		}
		return out
	}
	for _, b := range []struct {
		name string
		from []graphdiff.Entry
		got  []ChangeEntry
		wh   Pair
		hand Pair
	}{
		{"created", res.Created, out.Created, out.Counts.Withheld.Created, Pair{Relations: 2}},
		{"deleted", res.Deleted, out.Deleted, out.Counts.Withheld.Deleted, Pair{Relations: 1}},
		{"changed", res.Changed, out.Changed, out.Counts.Withheld.Changed, Pair{}},
		{"transient", res.Transient, out.Transient, out.Counts.Withheld.Transient, Pair{Relations: 1}},
	} {
		kept, withheld := split(b.from)
		if b.wh != withheld || b.wh != b.hand {
			t.Errorf("%s: withheld %+v, the diff has %+v, by hand %+v", b.name, b.wh, withheld, b.hand)
		}
		if got := subjects(b.got); !slices.Equal(got, kept) {
			t.Errorf("%s: listed %v, want %v", b.name, got, kept)
		}
	}
	// The bucket counts stay the diff's exact totals.
	if want := (ChangeCounts{
		Created: Pair{Relations: 4}, Deleted: Pair{Relations: 1}, Transient: Pair{Relations: 1},
		Events: 7, Withheld: ChangeWithheld{Created: Pair{Relations: 2}, Deleted: Pair{Relations: 1}, Transient: Pair{Relations: 1}, Events: 5},
	}); out.Counts != want {
		t.Errorf("counts %+v, want %+v", out.Counts, want)
	}
	if len(out.Events) != 2 {
		t.Errorf("%d events listed, want the 2 appearances of printable edges", len(out.Events))
	}
	wantNote := "Withheld because each names an endpoint the scope does not (an endpoint's id reveals its address): 2 created, 1 deleted, 1 transient, 5 events."
	if !contains(out.Notes, wantNote) {
		t.Errorf("notes %q", out.Notes)
	}
}

// The display cap shows only entries that can be printed, and counts only them.
func TestWithholdWhatChangedLimit(t *testing.T) {
	w := newAddrWorld(t)
	s := sourceOn(t, w.st, nil, nil)
	scope := []identity.Fingerprint{w.p, w.n}
	slices.SortFunc(scope, store.CompareFingerprints)
	res, err := graphdiff.NewDiffer(w.st).Diff(ctx, scope, hm(10, 0), hm(10, 30), w.st.LastSeq(), graphdiff.Options{Rank: rankObjs})
	if err != nil {
		t.Fatal(err)
	}
	// The order of the diff: the node's claim sorts before every pod's edge, so the
	// first created entry is withheld and the printable ones come after it.
	if len(res.Created) != 4 || res.Created[0].Element.Subject != edge(w.n, w.e[8], catalog.HasAddress) {
		t.Fatalf("created in the diff's order: %v", res.Created)
	}
	for _, e := range res.Created[1:] {
		if e.Element.Subject.A.Type() != catalog.K8sPod {
			t.Fatalf("created in the diff's order: %v", res.Created)
		}
	}

	out, err := s.WhatChanged(ctx, WhatChangedInput{Scope: stringsOf(scope), From: w10, To: w1030, Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Created) != 1 || hasEndpointEnd(entrySubject(out.Created[0])) {
		t.Fatalf("created with limit 1: %+v", out.Created)
	}
	if want := "Showing 1 of 2 created entries, entities first."; !contains(out.Notes, want) {
		t.Errorf("notes %q, want %q", out.Notes, want)
	}
	if len(out.Events) != 1 || hasEndpointEnd(entrySubject(ChangeEntry{Edge: out.Events[0].Edge, Entity: out.Events[0].Entity})) {
		t.Fatalf("events with limit 1: %+v", out.Events)
	}
	if want := "Showing 1 of 2 events, oldest first."; !contains(out.Notes, want) {
		t.Errorf("notes %q, want %q", out.Notes, want)
	}
	for _, note := range out.Notes {
		if strings.Contains(note, "of 4 created") || strings.Contains(note, "of 7 events") {
			t.Errorf("a note counts what is withheld: %q", note)
		}
	}
}

// quarantinedDiff is a diff whose cap cut its lists, with a withheld created
// entry, a printable one, and a quarantined edge to E7 beside a quarantined
// entity.
func quarantinedDiff(w *addrWorld) graphdiff.Result {
	qe := &store.QuarantineError{Entity: w.q, Layer: catalog.L2}
	return graphdiff.Result{
		Created: []graphdiff.Entry{
			{Element: interval.Element{Subject: edge(w.q, w.e[7], catalog.HasAddress)}},
			{Element: interval.Element{Subject: edge(w.q, w.n2, catalog.ScheduledOn)}},
		},
		Quarantined: []interval.Element{
			{Subject: ent(w.q), Quarantine: qe},
			{Subject: edge(w.q, w.e[7], catalog.HasAddress), Uncascaded: qe},
		},
		Counts:    graphdiff.Counts{Created: graphdiff.Count{Relations: 7}, Quarantined: graphdiff.Count{Entities: 1, Relations: 1}},
		Truncated: graphdiff.Truncation{Created: 5},
	}
}

// A hand-built diff: the quarantined edge to an endpoint is withheld and counted
// in its own bucket, and the cap note says how many of the listed entries were
// withheld.
func TestWithholdDiffByHand(t *testing.T) {
	w := newAddrWorld(t)
	res := quarantinedDiff(w)
	events := []graphdiff.Event{
		{At: hm(10, 5), Subject: edge(w.q, w.e[7], catalog.HasAddress), Appear: true},
		{At: hm(10, 6), Subject: edge(w.q, w.n2, catalog.ScheduledOn), Appear: true},
	}
	req := &request{named: map[string]bool{}}
	kept, keptEvents, wh := withholdDiff(res, events, req)

	if want := (ChangeWithheld{Created: Pair{Relations: 1}, Quarantined: Pair{Relations: 1}, Events: 1}); wh != want {
		t.Errorf("withheld %+v, want %+v", wh, want)
	}
	if len(kept.Created) != 1 || kept.Created[0].Element.Subject != edge(w.q, w.n2, catalog.ScheduledOn) {
		t.Errorf("created %+v", kept.Created)
	}
	if len(kept.Quarantined) != 1 || kept.Quarantined[0].Subject != ent(w.q) {
		t.Errorf("quarantined %+v", kept.Quarantined)
	}
	if len(keptEvents) != 1 || keptEvents[0].Subject != edge(w.q, w.n2, catalog.ScheduledOn) {
		t.Errorf("events %+v", keptEvents)
	}
	if kept.Counts != res.Counts || kept.Truncated != res.Truncated {
		t.Errorf("the counts moved: %+v %+v", kept.Counts, kept.Truncated)
	}
	if len(res.Created) != 2 || len(res.Quarantined) != 2 || len(events) != 2 {
		t.Error("withholdDiff changed its input")
	}
	want := []string{"Of the 2 created entries the diff listed under its cap, 1 were withheld; the 5 it did not list are counted only."}
	if got := changeCapNotes(res, wh); !slices.Equal(got, want) {
		t.Errorf("cap notes %q, want %q", got, want)
	}
	if got := changeCapNotes(res, ChangeWithheld{}); len(got) != 0 {
		t.Errorf("cap notes without a withheld entry: %q", got)
	}
	named := &request{named: idSet(w.e[7])}
	if _, _, wh := withholdDiff(res, events, named); wh != (ChangeWithheld{}) {
		t.Errorf("withheld %+v for a scope that names E7", wh)
	}
}

// endpointGraph is the graph of the neighbors_at property, with two endpoints
// that each pod and each node claims.
func endpointGraph() graphWorld {
	g := newGraphWorld()
	eps := []identity.Fingerprint{numbered(catalog.NetworkEndpoint, 1), numbered(catalog.NetworkEndpoint, 2)}
	for _, o := range g.entities {
		if o.Type() == catalog.K8sPod || o.Type() == catalog.K8sNode {
			for _, ep := range eps {
				g.edges = append(g.edges, edge(o, ep, catalog.HasAddress))
			}
		}
	}
	g.entities = append(g.entities, eps...)
	return g
}

// checkWithheld writes a world and asks neighbors_at for every entity, in each
// direction, at several instants. The edges listed and the edges withheld together
// must be the edges of the interval package, and the withheld ones exactly those
// whose peer is an endpoint other than the entity asked about. It returns how many
// edges were withheld, and how many an endpoint listed.
func checkWithheld(t testing.TB, d drawer, g graphWorld) (withheld, listedByEndpoints int, err error) {
	t.Helper()
	st := openMem(t, memstore.Options{Policy: lifecycle.Policy{BootKey: lifecycle.BootID}})
	newFeed(t, st).write(drawGraph(d, g)...)
	s, err := NewSource(Config{Store: st, RangeReads: true})
	if err != nil {
		return 0, 0, err
	}
	asof, token := "", st.LastSeq()
	if d.Int("pinned", 0, 2) == 0 && st.LastSeq() > 0 {
		token = uint64(d.Int("token", 0, int(st.LastSeq())))
		asof = fmt.Sprint(token)
	}
	dirOf := map[string][]store.Direction{
		"both": {store.Forward, store.Reverse}, "forward": {store.Forward}, "reverse": {store.Reverse},
	}
	for _, fp := range g.entities {
		for _, sec := range []int{0, 7, 12, 25, 44} {
			for _, dirName := range []string{"both", "forward", "reverse"} {
				when := at(sec)
				out, err := s.NeighborsAt(ctx, NeighborsAtInput{Entity: fp.String(), At: when.Format(time.RFC3339Nano), Direction: dirName, AsOf: asof})
				if err != nil {
					return 0, 0, err
				}
				var wantListed, wantWithheld []edgeKey
				for _, k := range intervalKeys(t, st, fp, dirOf[dirName], when, token) {
					if k.peer.Type() == catalog.NetworkEndpoint && k.peer != fp {
						wantWithheld = append(wantWithheld, k)
					} else {
						wantListed = append(wantListed, k)
					}
				}
				if got := neighborKeys(out); !sameKeys(got, wantListed) {
					return 0, 0, fmt.Errorf("neighbors_at of %s at +%ds, token %d, direction %q:\n got %v\nwant %v", fp, sec, token, dirName, got, wantListed)
				}
				if out.Counts.Withheld.Edges != len(wantWithheld) || out.Counts.Edges != len(wantListed) {
					return 0, 0, fmt.Errorf("neighbors_at of %s at +%ds, direction %q: counts %+v, want %d listed and %d withheld", fp, sec, dirName, out.Counts, len(wantListed), len(wantWithheld))
				}
				if dirName != "both" {
					continue
				}
				withheld += len(wantWithheld)
				if fp.Type() == catalog.NetworkEndpoint {
					listedByEndpoints += len(wantListed)
				}
			}
		}
	}
	return withheld, listedByEndpoints, nil
}

func TestWithholdNeighborsAtEqualsIntervalEdges(t *testing.T) {
	g := endpointGraph()
	rapid.Check(t, func(rt *rapid.T) {
		if _, _, err := checkWithheld(t, rapidDrawer{rt}, g); err != nil {
			rt.Fatal(err)
		}
	})
}

// The same over worlds from fixed seeds, so that the claims about what the
// generator covers are the same on every run.
func TestWithholdNeighborsAtOnFixedWorlds(t *testing.T) {
	g := endpointGraph()
	withheld, byEndpoints, worlds := 0, 0, 100
	if raceEnabled {
		worlds = 40
	}
	for seed := range worlds {
		d := randDrawer{rand.New(rand.NewPCG(uint64(seed), 11))}
		wh, le, err := checkWithheld(t, d, g)
		if err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		withheld += wh
		byEndpoints += le
	}
	t.Logf("%d worlds: %d edges withheld, %d edges listed from an endpoint", worlds, withheld, byEndpoints)
	if withheld < 5*worlds || byEndpoints < worlds {
		t.Errorf("the generator is too thin: %d edges withheld and %d listed from an endpoint over %d worlds", withheld, byEndpoints, worlds)
	}
}

// What is made of an error: no sentence, and no log line, prints an endpoint id
// the request did not name, whatever failed and however it is wrapped; and the
// sentence of an internal error, and the log line, print no key in hex.
func TestWithholdErrorSentencesAndLogs(t *testing.T) {
	w := newAddrWorld(t)
	e1 := w.e[1]
	h := e1.Hash()
	hexOfHash := hex.EncodeToString(h[:])
	// A key as pebblestore's parseKey prints it: layer, a 2-byte type id, the 16-byte
	// hash, the direction and 17 more bytes, 37 in all. The type id and the layer
	// are those of an endpoint.
	key := append(append([]byte{2, 0, 12}, h[:]...), append([]byte{0}, make([]byte, 17)...)...)
	if len(key) != 37 {
		t.Fatalf("the key is %d bytes", len(key))
	}
	lowerHash := hexOfHash
	upperHash := strings.ToUpper(hexOfHash)

	for _, tc := range []struct {
		name string
		err  error
		// logged is a fragment the log line must keep; "" when the error is not logged.
		logged string
		// wantUnnamed and wantNamed are fragments of the sentence of the call that did
		// not name E1, and of the one that did; the second is the first if empty.
		wantUnnamed, wantNamed string
		// keeps is a fragment that the sentence and the log line must both print.
		keeps string
		cause error
	}{
		{
			name: "an internal error", err: fmt.Errorf("memstore: Alive: folding %s: %w", e1, errBoom),
			logged: "boom: the disk is on fire", cause: errBoom,
			// A named id is withheld from an internal sentence too, by the second rule.
			wantUnnamed: "Internal error: layer L2: memstore: Alive: folding topo.endpoint:withheld: boom",
		},
		{
			name: "a refusal", err: fmt.Errorf("memstore: Alive: folding %s: %w", e1, store.ErrInvalid),
			cause:       store.ErrInvalid,
			wantUnnamed: "The request was refused: layer L2: memstore: Alive: folding topo.endpoint:withheld",
			wantNamed:   "The request was refused: layer L2: memstore: Alive: folding " + e1.String(),
		},
		{
			name: "a sentence the tool made", err: &Error{Message: "A read of " + e1.String() + " failed."},
			wantUnnamed: "A read of topo.endpoint:withheld failed.",
			wantNamed:   "A read of " + e1.String() + " failed.",
		},
		{
			name: "a key in hex", err: fmt.Errorf("pebblestore: key %x is 3 bytes, want 37", key),
			logged:      "pebblestore: key withheld is 3 bytes, want 37",
			wantUnnamed: "Internal error: layer L2: pebblestore: key withheld is 3 bytes, want 37",
		},
		{
			name: "a key in upper case hex", err: fmt.Errorf("pebblestore: key %X is 3 bytes, want 37", key),
			logged:      "pebblestore: key withheld is 3 bytes, want 37",
			wantUnnamed: "Internal error: layer L2: pebblestore: key withheld is 3 bytes, want 37",
		},
		{
			name: "a key prefix in hex", err: fmt.Errorf("pebblestore: key %x is 20 bytes, want 37", key[:20]),
			logged:      "pebblestore: key withheld is 20 bytes, want 37",
			wantUnnamed: "Internal error: layer L2: pebblestore: key withheld is 20 bytes, want 37",
		},
		{
			name: "a description recorded before the horizon", err: fmt.Errorf("graphdiff: Diff: %w", &interval.RetainedError{
				Subject: ent(e1), At: hm(10, 0), Producer: objs, Seed: store.Record{EventTime: hm(9, 0)},
			}),
			cause:       interval.ErrRetained,
			wantUnnamed: "The description of topo.endpoint:withheld at 2026-01-01T10:00:00Z by k8sobjects/c1 was recorded with an event time before the retention horizon and is no longer kept. It cannot be described until its producer asserts it again.",
			wantNamed:   "The description of " + e1.String() + " at 2026-01-01T10:00:00Z by k8sobjects/c1 was recorded with an event time before the retention horizon and is no longer kept. It cannot be described until its producer asserts it again.",
		},
		{
			name: "the id of another type is kept", err: fmt.Errorf("folding entity %s: %w", w.p, errBoom),
			logged: "folding entity " + w.p.String() + ": boom", cause: errBoom, keeps: w.p.String(),
			wantUnnamed: "Internal error: layer L2: folding entity " + w.p.String() + ": boom",
		},
		{
			name: "what the exception does not keep", err: fmt.Errorf("odd %s and at %d: %w", "k8s.pod:"+hexOfHash+"01", int64(1760000000000000000), errBoom),
			logged: "odd k8s.pod:withheld and at 1760000000000000000: boom", cause: errBoom,
			wantUnnamed: "Internal error: layer L2: odd k8s.pod:withheld and at 1760000000000000000: boom",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, named := range []bool{false, true} {
				var logs logSink
				st := failAlive{Store: w.st, fp: e1, err: tc.err}
				s := sourceOn(t, st, nil, logs.logf)
				entity, want := w.p, tc.wantUnnamed
				if named {
					entity = e1
					if tc.wantNamed != "" {
						want = tc.wantNamed
					}
				}
				_, err := s.NeighborsAt(ctx, NeighborsAtInput{Entity: entity.String(), At: w1012})
				var te *Error
				if !errors.As(err, &te) {
					t.Fatalf("named %t: %v, not an *Error", named, err)
				}
				if !strings.Contains(te.Message, want) {
					t.Errorf("named %t: %q, want %q", named, te.Message, want)
				}
				if !named && strings.Contains(te.Message, e1.String()) {
					t.Errorf("the sentence prints E1: %q", te.Message)
				}
				plain := strings.ReplaceAll(te.Message, e1.String(), "")
				if strings.Contains(plain, lowerHash) || strings.Contains(plain, upperHash) {
					t.Errorf("named %t: the sentence prints the hash of E1 in hex: %q", named, te.Message)
				}
				if tc.keeps != "" && !strings.Contains(te.Message, tc.keeps) {
					t.Errorf("named %t: the sentence lost %q: %q", named, tc.keeps, te.Message)
				}
				if tc.cause != nil && !errors.Is(err, tc.cause) {
					t.Errorf("named %t: the cause was lost", named)
				}
				lines := logs.all()
				if (tc.logged == "") != (len(lines) == 0) {
					t.Errorf("named %t: log lines %q, want a line with %q", named, lines, tc.logged)
				}
				for _, line := range lines {
					if strings.Contains(line, e1.String()) || strings.Contains(line, lowerHash) || strings.Contains(line, upperHash) || endpointID.MatchString(line) {
						t.Errorf("named %t: the log line prints an id: %q", named, line)
					}
					if !strings.Contains(line, tc.logged) {
						t.Errorf("named %t: the log line %q lacks %q", named, line, tc.logged)
					}
					if tc.keeps != "" && !strings.Contains(line, tc.keeps) {
						t.Errorf("named %t: the log line lost %q: %q", named, tc.keeps, line)
					}
				}
			}
		})
	}
}

// The second rule of redaction, on its own: a maximal run of 32 hex digits or more
// is replaced, unless it is the 32 lowercase digits after <type>: for a catalog
// type other than topo.endpoint. A rule that can be dropped whole with this test.
func TestRedactHexRuns(t *testing.T) {
	h := strings.Repeat("ab", 16)
	pod := "k8s.pod:" + h
	ep := "topo.endpoint:" + h
	for _, tc := range []struct{ name, in, want string }{
		{"a bare hash", "key " + h + " is bad", "key withheld is bad"},
		{"a key with a type byte", "key 0c" + h, "key withheld"},
		{"a whole key", "key 02000c" + h + "00" + strings.Repeat("00", 17), "key withheld"},
		{"upper case", "key " + strings.ToUpper(h), "key withheld"},
		{"31 digits", "key " + h[:31], "key " + h[:31]},
		{"a decimal of 19 digits", "at 1760000000000000000", "at 1760000000000000000"},
		{"a pod's id", "reading " + pod + " failed", "reading " + pod + " failed"},
		{"a host's id", "reading host:" + h, "reading host:" + h},
		{"an upper case id", "reading k8s.pod:" + strings.ToUpper(h), "reading k8s.pod:withheld"},
		{"an unknown type", "reading nothing:" + h, "reading nothing:withheld"},
		{"a longer run after a type", "reading " + pod + "01", "reading k8s.pod:withheld"},
		{"a type that only ends like one", "reading xk8s.pod:" + h, "reading xk8s.pod:withheld"},
		{"an endpoint's id", "reading " + ep, "reading topo.endpoint:withheld"},
		{"several", h + " and " + h + "00", "withheld and withheld"},
		{"none", "disk on fire", "disk on fire"},
	} {
		if got := redactHexRuns(tc.in); got != tc.want {
			t.Errorf("%s: %q, want %q", tc.name, got, tc.want)
		}
	}
	// The log line drops the named set, and both rules apply.
	if got := redactLog("a " + ep + " and " + pod + " and " + h); got != "a topo.endpoint:withheld and "+pod+" and withheld" {
		t.Errorf("redactLog: %q", got)
	}
}

func TestRedactEndpoints(t *testing.T) {
	h1, h2 := strings.Repeat("0a", 16), strings.Repeat("1b", 16)
	a, b := "topo.endpoint:"+h1, "topo.endpoint:"+h2
	in := "x " + a + " y " + b + " z " + strings.ToUpper(a)
	if got, want := redactEndpoints(in, map[string]bool{a: true}), "x "+a+" y topo.endpoint:withheld z "+strings.ToUpper(a); got != want {
		t.Errorf("%q, want %q", got, want)
	}
	if got := redactEndpoints(in, nil); strings.Contains(got, h1) || strings.Contains(got, h2) {
		t.Errorf("%q", got)
	}
}

// askErr is the *Error a call returned.
func askErr(t *testing.T, err error) *Error {
	t.Helper()
	var te *Error
	if !errors.As(err, &te) {
		t.Fatalf("%v, not an *Error", err)
	}
	return te
}

// The last check: an answer that would still print an endpoint id the request
// did not name is refused with a sentence that holds no id, and logged.
func TestWithholdBackstop(t *testing.T) {
	w := newAddrWorld(t)

	t.Run("filters off", func(t *testing.T) {
		var logs logSink
		s := sourceOn(t, w.st, &egressHooks{noFilter: true}, logs.logf)
		_, err := s.NeighborsAt(ctx, NeighborsAtInput{Entity: w.p.String(), At: w1012})
		if te := askErr(t, err); te.Message != egressSentence {
			t.Errorf("neighbors_at: %q", te.Message)
		}
		_, err = s.WhatChanged(ctx, WhatChangedInput{Scope: []string{w.p.String(), w.n.String()}, From: w10, To: w1030})
		if te := askErr(t, err); te.Message != egressSentence {
			t.Errorf("what_changed: %q", te.Message)
		}
		lines := logs.all()
		if len(lines) != 2 {
			t.Fatalf("log lines %q", lines)
		}
		for i, want := range []string{
			"mcp: internal error: an answer of type mcp.NeighborsAtOutput would have printed 4 endpoint ids the request did not name",
			"mcp: internal error: an answer of type mcp.WhatChangedOutput would have printed 4 endpoint ids the request did not name",
		} {
			if lines[i] != want {
				t.Errorf("log line %q, want %q", lines[i], want)
			}
		}
	})

	calls := []struct {
		name string
		do   func(s *Source) error
	}{
		{"blast_radius", func(s *Source) error {
			_, err := s.BlastRadius(ctx, BlastRadiusInput{Entity: w.n.String(), From: w10, To: w1030})
			return err
		}},
		{"what_changed", func(s *Source) error {
			_, err := s.WhatChanged(ctx, WhatChangedInput{Scope: []string{w.p.String()}, From: w10, To: w1030})
			return err
		}},
		{"co_residents", func(s *Source) error {
			_, err := s.CoResidents(ctx, CoResidentsInput{Pod: w.p.String(), From: w10, To: w1030})
			return err
		}},
		{"neighbors_at", func(s *Source) error {
			_, err := s.NeighborsAt(ctx, NeighborsAtInput{Entity: w.p.String(), At: w1012})
			return err
		}},
		{"entity_history", func(s *Source) error {
			_, err := s.EntityHistory(ctx, EntityHistoryInput{Entity: w.p.String(), From: w10, To: w1030})
			return err
		}},
		{"resolve_entity with an instant", func(s *Source) error {
			_, err := s.ResolveEntity(ctx, ResolveEntityInput{Type: "k8s.pod", Attributes: rawAttrs(`{"k8s.pod.uid":"addr-pod-p"}`), At: w1012})
			return err
		}},
		{"resolve_entity without one", func(s *Source) error {
			_, err := s.ResolveEntity(ctx, ResolveEntityInput{Type: "k8s.pod", Attributes: rawAttrs(`{"k8s.pod.uid":"addr-pod-p"}`)})
			return err
		}},
		{"describe_schema", func(s *Source) error {
			_, err := s.DescribeSchema(ctx, DescribeSchemaInput{})
			return err
		}},
	}
	appendNote := func(text string) func(any) {
		return func(out any) {
			notes := reflect.ValueOf(out).Elem().FieldByName("Notes")
			notes.Set(reflect.Append(notes, reflect.ValueOf(text)))
		}
	}

	t.Run("an id injected into every tool", func(t *testing.T) {
		var logs logSink
		s := sourceOn(t, w.st, &egressHooks{inject: appendNote("see " + w.e[3].String())}, logs.logf)
		for _, c := range calls {
			te := askErr(t, c.do(s))
			if te.Message != egressSentence {
				t.Errorf("%s: %q", c.name, te.Message)
			}
		}
		if lines := logs.all(); len(lines) != len(calls) {
			t.Errorf("%d log lines for %d refusals: %q", len(lines), len(calls), lines)
		}
		for _, line := range logs.all() {
			if endpointID.MatchString(line) {
				t.Errorf("the log line prints an id: %q", line)
			}
		}
	})

	t.Run("the request's own id is not refused", func(t *testing.T) {
		s := sourceOn(t, w.st, &egressHooks{inject: appendNote("see " + w.e[1].String())}, nil)
		if _, err := s.NeighborsAt(ctx, NeighborsAtInput{Entity: w.e[1].String(), At: w1012, Direction: "reverse"}); err != nil {
			t.Errorf("neighbors_at of E1: %v", err)
		}
		// The same id in a request for another entity is refused.
		if _, err := s.NeighborsAt(ctx, NeighborsAtInput{Entity: w.p.String(), At: w1012}); err == nil {
			t.Error("E1 in an answer about P was not refused")
		}
		attrs := rawAttrs(`{"topo.endpoint.realm":"k8s/c1","topo.endpoint.address":"10.42.0.7"}`)
		for _, at := range []string{"", w1012} {
			if _, err := s.ResolveEntity(ctx, ResolveEntityInput{Type: "topo.endpoint", Attributes: attrs, At: at}); err != nil {
				t.Errorf("resolve_entity of E1 with at %q: %v", at, err)
			}
		}
	})

	t.Run("no last check", func(t *testing.T) {
		s := sourceOn(t, w.st, &egressHooks{noFilter: true, noBackstop: true}, nil)
		if _, err := s.NeighborsAt(ctx, NeighborsAtInput{Entity: w.p.String(), At: w1012}); err != nil {
			t.Errorf("neighbors_at: %v", err)
		}
	})
}

// failSince fails the name read of one entity, as a store whose key is corrupt does,
// with an error that prints the entity's id and a whole key in hex.
type failSince struct {
	store.Store
	fp  identity.Fingerprint
	key []byte
}

func (f failSince) WindowSince(ctx context.Context, fps []identity.Fingerprint, d store.Direction, from, to time.Time, sc store.Scope, o store.SinceOptions) ([]store.Since, error) {
	if slices.Contains(fps, f.fp) {
		return nil, fmt.Errorf("pebblestore: folding %s key %x: %w", f.fp, f.key, errBoom)
	}
	return f.Store.WindowSince(ctx, fps, d, from, to, sc, o)
}

// The read of names is a second place where an error becomes text: a note in the
// answer and a log line. Neither prints an endpoint id or a key in hex.
func TestWithholdNamesReadError(t *testing.T) {
	w := newAddrWorld(t)
	for _, tc := range []struct {
		name   string
		fp     identity.Fingerprint
		entity identity.Fingerprint
		dir    string
	}{
		{"the named endpoint", w.e[1], w.e[1], "reverse"},
		{"a peer", w.n, w.p, "forward"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := tc.fp.Hash()
			key := append(append([]byte{2, 0, 12}, h[:]...), append([]byte{0}, make([]byte, 17)...)...)
			var logs logSink
			s := sourceOn(t, failSince{Store: w.st, fp: tc.fp, key: key}, nil, logs.logf)
			out, err := s.NeighborsAt(ctx, NeighborsAtInput{Entity: tc.entity.String(), At: w1012, Direction: tc.dir})
			if err != nil {
				t.Fatal(err)
			}
			var note string
			for _, n := range out.Notes {
				if strings.HasPrefix(n, "Names are unavailable: ") {
					note = n
				}
			}
			if note == "" {
				t.Fatalf("no note about the names: %q", out.Notes)
			}
			lines := logs.all()
			if len(lines) != 1 || !strings.Contains(lines[0], "internal error reading names") || !strings.Contains(lines[0], "boom") {
				t.Fatalf("log lines %q", lines)
			}
			for what, text := range map[string]string{"note": note, "log line": lines[0]} {
				plain := strings.ReplaceAll(text, tc.fp.String(), "")
				if tc.fp.Type() == catalog.NetworkEndpoint {
					plain = text
				}
				if strings.Contains(plain, hex.EncodeToString(key)) || strings.Contains(plain, hex.EncodeToString(h[:])) || endpointID.MatchString(text) {
					t.Errorf("the %s prints an id or a key: %q", what, text)
				}
			}
		})
	}
}
