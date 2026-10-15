package subgraph_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/query/subgraph"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

var (
	nodeN  = node(1)
	nodeN2 = node(2)
	hostH  = host(1)
	podP1  = pod(1)
	podP2  = pod(2)
	podP3  = pod(3)
	podP4  = pod(4)
)

// window30 is the window [base, base+30s) at a token.
func window30(asOf uint64) interval.Window {
	return interval.Window{From: at(0), To: at(30), AsOf: asOf}
}

// scopeWorld is a node N with pods P1, P2 and P3 on it at different times, a
// host H it runs on, and a second node N2 on the same host with a pod of its
// own, all observed long before the window.
//
//	P1 on N throughout, but P1 itself is deleted at 20;
//	P2 on N until 5; P3 on N from 6;
//	N runs on H; N2 runs on H; P4 is on N2.
func scopeWorld(t testing.TB) (*memstore.Store, *feed) {
	st, f := openStore(t, memstore.Options{})
	for _, fp := range []identity.Fingerprint{nodeN, nodeN2, hostH, podP1, podP2, podP3, podP4} {
		f.write(observe(ent(fp), k8s, -100, 0))
	}
	f.write(
		observe(sched(podP1, nodeN), k8s, -100, 0),
		observe(sched(podP2, nodeN), k8s, -100, 0),
		deleted(sched(podP2, nodeN), k8s, 5),
		observe(sched(podP3, nodeN), k8s, 6, 0),
		observe(runsOn(nodeN, hostH), agent, -100, 0),
		observe(runsOn(nodeN2, hostH), agent, -100, 0),
		observe(sched(podP4, nodeN2), k8s, -100, 0),
		deleted(ent(podP1), k8s, 20),
	)
	return st, f
}

func keysOf(sg subgraph.Subgraph) []string {
	var out []string
	for _, el := range sg.Elements {
		out = append(out, el.Key.String())
	}
	return out
}

func strs(ks ...subgraph.Key) []string {
	var out []string
	for _, k := range ks {
		out = append(out, k.String())
	}
	return out
}

// The scope rule is graph diff's: the scope's entities, and every edge with an
// endpoint among them, read forward and reverse, one hop out. The far end of an
// edge is a boundary, with no existence element; its own history and its other
// edges are not read in.
func TestScopeRule(t *testing.T) {
	st, _ := scopeWorld(t)
	sg, err := subgraph.ReadScope(ctx, st, []identity.Fingerprint{nodeN, podP1}, window30(store.Latest), subgraph.ScopePolicy{})
	if err != nil {
		t.Fatal(err)
	}
	// Elements are in key order: entities, then edges by source, target, relation.
	want := []subgraph.Key{
		ent(nodeN), ent(podP1),
		runsOn(nodeN, hostH), sched(podP1, nodeN), sched(podP2, nodeN), sched(podP3, nodeN),
	}
	slices.SortFunc(want, subgraph.Key.Compare)
	expectEqual(t, "elements", keysOf(sg), strs(want...))

	boundary := map[identity.Fingerprint]bool{}
	for _, e := range sg.Entities {
		boundary[e.Fingerprint] = e.Boundary
	}
	wantBoundary := map[identity.Fingerprint]bool{nodeN: false, podP1: false, podP2: true, podP3: true, hostH: true}
	if fmt.Sprint(boundary) != fmt.Sprint(wantBoundary) {
		t.Errorf("dictionary %v, want %v", boundary, wantBoundary)
	}
	for _, fp := range []identity.Fingerprint{nodeN2, podP4} {
		if _, ok := sg.EntityID(fp); ok {
			t.Errorf("%s, two hops out, is in the dictionary", fp)
		}
	}
	if sg.Token != st.LastSeq() || !sg.From.Equal(at(0)) || !sg.To.Equal(at(30)) || sg.Basis != subgraph.BasisDerived {
		t.Errorf("header token %d from %s to %s", sg.Token, sg.From, sg.To)
	}

	// The deltas: P2's edge goes at 5 (a producer's Delete), P3's arrives at 6,
	// and at 20 P1 goes by a Delete and its edge by the cascade.
	type row struct {
		at     int
		key    string
		appear bool
		reason subgraph.EndReason
	}
	var got []row
	for _, d := range sg.Deltas {
		r := d.After.Reason
		if d.Appear() {
			r = d.Before.Reason
		}
		got = append(got, row{int(d.At / uint64(time.Second)), sg.Elements[d.Element].Key.String(), d.Appear(), r})
	}
	wantRows := []row{
		{5, sched(podP2, nodeN).String(), false, subgraph.EndReasonProducer},
		{6, sched(podP3, nodeN).String(), true, subgraph.EndReasonNotSeenInWindow},
		{20, ent(podP1).String(), false, subgraph.EndReasonProducer},
		{20, sched(podP1, nodeN).String(), false, subgraph.EndReasonCascade},
	}
	if fmt.Sprint(got) != fmt.Sprint(wantRows) {
		t.Errorf("deltas:\ngot:  %v\nwant: %v", got, wantRows)
	}
	checkAgainstPointReads(t, st, []identity.Fingerprint{nodeN, podP1}, sg)
	sum := 0
	for _, el := range sg.Elements {
		sum += el.Transitions
	}
	if sum != len(sg.Deltas) {
		t.Errorf("E = %d deltas, the transitions sum to %d", len(sg.Deltas), sum)
	}
	if len(sg.Quarantine.Entities)+len(sg.Quarantine.Edges) != 0 {
		t.Errorf("quarantine %+v", sg.Quarantine)
	}

	// An edge between two scope entities is read from both ends and is one
	// element; the order and the repeats of the scope change nothing.
	for _, scope := range [][]identity.Fingerprint{{podP1, nodeN}, {podP1, nodeN, podP1, nodeN}} {
		again, err := subgraph.ReadScope(ctx, st, scope, window30(store.Latest), subgraph.ScopePolicy{})
		if err != nil {
			t.Fatal(err)
		}
		if fmt.Sprintf("%+v", again) != fmt.Sprintf("%+v", sg) {
			t.Errorf("scope %v gave another subgraph", scope)
		}
	}
	// A scope of the far peer alone reads that peer's edges.
	h, err := subgraph.ReadScope(ctx, st, []identity.Fingerprint{hostH}, window30(store.Latest), subgraph.ScopePolicy{})
	if err != nil {
		t.Fatal(err)
	}
	expectEqual(t, "host scope", keysOf(h), strs(ent(hostH), runsOn(nodeN, hostH), runsOn(nodeN2, hostH)))
}

// A scope entity that did not exist in the window has no element, and the
// window can be narrower than the history: what began before it is Before.
func TestScopeWindowAndAbsentEntities(t *testing.T) {
	st, _ := scopeWorld(t)
	absent := pod(77)
	w := interval.Window{From: at(7), To: at(12), AsOf: store.Latest}
	sg, err := subgraph.ReadScope(ctx, st, []identity.Fingerprint{nodeN, absent}, w, subgraph.ScopePolicy{})
	if err != nil {
		t.Fatal(err)
	}
	expectEqual(t, "elements", keysOf(sg), func() []string {
		ks := []subgraph.Key{ent(nodeN), runsOn(nodeN, hostH), sched(podP1, nodeN), sched(podP3, nodeN)}
		slices.SortFunc(ks, subgraph.Key.Compare)
		return strs(ks...)
	}())
	if _, ok := sg.EntityID(absent); ok {
		t.Error("an entity that did not exist is in the dictionary")
	}
	if len(sg.Deltas) != 0 || len(sg.Keyframes) != 1 {
		t.Errorf("deltas %d, keyframes %d", len(sg.Deltas), len(sg.Keyframes))
	}
	for _, el := range sg.Elements {
		if el.Class != subgraph.ClassStable || !el.Pieces[0].Before && el.Key != sched(podP3, nodeN) {
			t.Errorf("%s: class %s, pieces %+v", el.Key, el.Class, el.Pieces)
		}
	}
	// P3's edge began at 6, before the window.
	id, _ := sg.ElementID(sched(podP3, nodeN))
	if !sg.Elements[id].Pieces[0].Before {
		t.Error("an edge that began before the window is not marked Before")
	}
}

// The edge read reuses the token the entity read returned: a write between the
// two reads is not seen, and the subgraph names the first token.
func TestScopeTokenIsPinnedOnce(t *testing.T) {
	inner, _ := scopeWorld(t)
	want, err := subgraph.ReadScope(ctx, inner, []identity.Fingerprint{nodeN}, window30(store.Latest), subgraph.ScopePolicy{})
	if err != nil {
		t.Fatal(err)
	}
	before := inner.LastSeq()
	late := newFeed(t, inner)
	// Every interval call resolves its token with one LastSeq. The entity call
	// is the first; the write falls on the second, at the start of the first
	// edge call, before it reads anything.
	st := &spy{Store: inner, onLast: func(n int) {
		if n == 2 {
			late.write(deleted(sched(podP3, nodeN), k8s, 10), observe(sched(podP4, nodeN), k8s, 9, 0))
		}
	}}
	got, err := subgraph.ReadScope(ctx, st, []identity.Fingerprint{nodeN}, window30(store.Latest), subgraph.ScopePolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if inner.LastSeq() != before+2 {
		t.Fatal("the hook did not write")
	}
	if got.Token != before || fmt.Sprintf("%+v", got) != fmt.Sprintf("%+v", want) {
		t.Errorf("token %d, want %d; the write between the reads changed the subgraph:\n%+v\n%+v", got.Token, before, got, want)
	}
	for _, tok := range st.tokens {
		if tok != before {
			t.Fatalf("a read at token %d, want %d: %v", tok, before, st.tokens)
		}
	}
	if st.writes != 0 {
		t.Errorf("%d writes through the reader's store", st.writes)
	}
	// An explicit token, and one above LastSeq, are used as the interval reads do.
	old, err := subgraph.ReadScope(ctx, inner, []identity.Fingerprint{nodeN}, window30(before), subgraph.ScopePolicy{})
	if err != nil || old.Token != before {
		t.Errorf("an explicit token: %d, %v", old.Token, err)
	}
	high, err := subgraph.ReadScope(ctx, inner, []identity.Fingerprint{nodeN}, window30(inner.LastSeq()+50), subgraph.ScopePolicy{})
	if err != nil || high.Token != inner.LastSeq() {
		t.Errorf("a token above LastSeq: %d, %v", high.Token, err)
	}
}

// A quarantined scope entity, and an edge with a quarantined endpoint, are in
// the static section, with the collision, in no keyframe and no delta.
func TestScopeQuarantine(t *testing.T) {
	st, f := openStore(t, memstore.Options{Policy: storetest.QuarantinePolicy()})
	boot := func(id string, sec int) store.Record {
		r := observe(ent(hostH), agent, sec, 15*time.Minute)
		r.Boot = id
		return r
	}
	f.write(
		observe(ent(nodeN), k8s, -100, 0), observe(ent(podP1), k8s, -100, 0), observe(sched(podP1, nodeN), k8s, 3, 0),
		boot("b1", -100), boot("b2", -95), boot("b1", -90),
		observe(runsOn(nodeN, hostH), agent, -100, 0),
	)
	for name, scope := range map[string][]identity.Fingerprint{
		"host in the scope":     {nodeN, hostH},
		"host as a far peer":    {nodeN},
		"host alone":            {hostH},
		"pod and host together": {podP1, hostH, nodeN},
	} {
		t.Run(name, func(t *testing.T) {
			sg, err := subgraph.ReadScope(ctx, st, scope, window30(store.Latest), subgraph.ScopePolicy{})
			if err != nil {
				t.Fatal(err)
			}
			if _, ok := sg.ElementID(ent(hostH)); ok {
				t.Error("the quarantined host is an element")
			}
			if _, ok := sg.ElementID(runsOn(nodeN, hostH)); ok {
				t.Error("the uncascaded edge is an element")
			}
			if len(sg.Quarantine.Edges) != 1 || sg.Quarantine.Edges[0].Key != runsOn(nodeN, hostH) ||
				!errors.Is(sg.Quarantine.Edges[0].Cause, store.ErrQuarantined) || sg.Quarantine.Edges[0].Cause.Entity != hostH ||
				len(sg.Quarantine.Edges[0].Pieces) != 1 {
				t.Errorf("uncascaded edges %+v", sg.Quarantine.Edges)
			}
			inScope := slices.Contains(scope, hostH)
			if inScope != (len(sg.Quarantine.Entities) == 1) {
				t.Errorf("quarantined entities %+v with the host in scope = %t", sg.Quarantine.Entities, inScope)
			}
			if inScope && (sg.Quarantine.Entities[0].Fingerprint != hostH || sg.Quarantine.Entities[0].Cause.Collision == nil) {
				t.Errorf("quarantined entity %+v", sg.Quarantine.Entities[0])
			}
			id, ok := sg.EntityID(hostH)
			if !ok || sg.Entities[id].Boundary == inScope {
				t.Errorf("the host: in the dictionary %t, boundary %t, in scope %t", ok, sg.Entities[id].Boundary, inScope)
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
			checkAgainstPointReads(t, st, scope, sg)
		})
	}
}

// A retained layer fails the whole read.
func TestScopeRetention(t *testing.T) {
	st, f := openStore(t, memstore.Options{Keep: [4]bool{true, false, true, true}})
	f.write(observe(ent(nodeN), k8s, -100, 0), observe(ent(hostH), agent, -100, 0), observe(runsOn(nodeN, hostH), agent, -100, 0))
	if err := st.Retain(ctx, at(-50)); err != nil {
		t.Fatal(err)
	}
	sg, err := subgraph.ReadScope(ctx, st, []identity.Fingerprint{nodeN}, window30(store.Latest), subgraph.ScopePolicy{})
	if !errors.Is(err, interval.ErrRetained) || !errors.Is(err, errors.ErrUnsupported) || sg.Entities != nil || sg.Elements != nil {
		t.Fatalf("%v, %+v; want ErrRetained with a zero subgraph", err, sg)
	}
}

// Arguments are checked before any store call, and a larger scope is refused.
func TestScopeArguments(t *testing.T) {
	inner, _ := scopeWorld(t)
	st := &spy{Store: inner}
	many := make([]identity.Fingerprint, 257)
	for i := range many {
		many[i] = pod(1000 + i)
	}
	w := window30(store.Latest)
	empty := interval.Window{From: at(3), To: at(3)}
	reversed := interval.Window{From: at(4), To: at(3)}
	before := interval.Window{From: time.Unix(-5, 0), To: at(3)}
	for _, tc := range []struct {
		name  string
		scope []identity.Fingerprint
		w     interval.Window
		p     subgraph.ScopePolicy
	}{
		{"empty window", []identity.Fingerprint{nodeN}, empty, subgraph.ScopePolicy{}},
		{"reversed window", []identity.Fingerprint{nodeN}, reversed, subgraph.ScopePolicy{}},
		{"window before the store's times", []identity.Fingerprint{nodeN}, before, subgraph.ScopePolicy{}},
		{"empty scope", nil, w, subgraph.ScopePolicy{}},
		{"zero fingerprint", []identity.Fingerprint{nodeN, {}}, w, subgraph.ScopePolicy{}},
		{"257 entities against the default cap", many, w, subgraph.ScopePolicy{}},
		{"3 entities against a cap of 2", []identity.Fingerprint{nodeN, podP1, podP2}, w, subgraph.ScopePolicy{MaxScope: 2}},
		{"a cap below 1", []identity.Fingerprint{nodeN}, w, subgraph.ScopePolicy{MaxScope: -1}},
	} {
		sg, err := subgraph.ReadScope(ctx, st, tc.scope, tc.w, tc.p)
		if !errors.Is(err, store.ErrInvalid) || sg.Entities != nil || sg.Elements != nil {
			t.Errorf("%s: %v", tc.name, err)
		}
	}
	if st.calls != 0 {
		t.Errorf("%d store calls for invalid arguments", st.calls)
	}
	// 256 distinct entities, one of them named twice, are within the default cap,
	// and the same scope fits a cap of exactly its size.
	if _, err := subgraph.ReadScope(ctx, st, append(many[:256:256], many[0]), w, subgraph.ScopePolicy{}); err != nil {
		t.Errorf("256 distinct entities: %v", err)
	}
	if _, err := subgraph.ReadScope(ctx, st, []identity.Fingerprint{nodeN, podP1, nodeN}, w, subgraph.ScopePolicy{MaxScope: 2}); err != nil {
		t.Errorf("2 distinct entities against a cap of 2: %v", err)
	}
	st.calls = 0
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := subgraph.ReadScope(cancelled, st, []identity.Fingerprint{nodeN}, w, subgraph.ScopePolicy{}); !errors.Is(err, context.Canceled) || st.calls != 0 {
		t.Errorf("cancelled: %v after %d calls", err, st.calls)
	}
	if err := inner.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := subgraph.ReadScope(ctx, st, []identity.Fingerprint{nodeN}, w, subgraph.ScopePolicy{}); !errors.Is(err, store.ErrClosed) {
		t.Errorf("closed: %v", err)
	}
}

// A scope of a type no relation reaches in either direction reads no edges.
func TestScopeOfATypeWithNoEdges(t *testing.T) {
	st, f := openStore(t, memstore.Options{})
	port, err := identity.FingerprintFromHash(catalog.Port, [identity.FingerprintBytes]byte{1})
	if err != nil {
		t.Fatal(err)
	}
	f.write(observe(ent(port), k8s, -10, 0))
	sg, err := subgraph.ReadScope(ctx, st, []identity.Fingerprint{port}, window30(store.Latest), subgraph.ScopePolicy{})
	if err != nil {
		t.Fatal(err)
	}
	expectEqual(t, "elements", keysOf(sg), strs(ent(port)))
	if sg.Elements[0].Class != subgraph.ClassStable {
		t.Errorf("class %s", sg.Elements[0].Class)
	}
}

// An entity that was read and found absent from the window is not a boundary,
// even where an uncascaded edge names it; the far end that was not read is.
func TestScopeReadEntityIsNotABoundary(t *testing.T) {
	st, f := openStore(t, memstore.Options{Policy: storetest.QuarantinePolicy()})
	boot := func(id string, sec int) store.Record {
		r := observe(ent(hostH), agent, sec, 15*time.Minute)
		r.Boot = id
		return r
	}
	// The node is never observed: it did not exist in the window. The host is
	// quarantined, so the edge between them is uncascaded and keeps its pieces.
	f.write(boot("b1", -100), boot("b2", -95), boot("b1", -90), observe(runsOn(nodeN, hostH), agent, -100, 0))
	sg, err := subgraph.ReadScope(ctx, st, []identity.Fingerprint{nodeN}, window30(store.Latest), subgraph.ScopePolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if len(sg.Elements) != 0 || len(sg.Quarantine.Edges) != 1 || sg.Quarantine.Edges[0].Key != runsOn(nodeN, hostH) {
		t.Fatalf("elements %d, uncascaded edges %+v", len(sg.Elements), sg.Quarantine.Edges)
	}
	boundary := map[identity.Fingerprint]bool{}
	for _, e := range sg.Entities {
		boundary[e.Fingerprint] = e.Boundary
	}
	if want := map[identity.Fingerprint]bool{nodeN: false, hostH: true}; fmt.Sprint(boundary) != fmt.Sprint(want) {
		t.Errorf("boundary marks %v, want %v", boundary, want)
	}
}

// The edge read must use the token the entity read pinned: a store that answers
// another token to it fails the read rather than mixing two worlds.
func TestScopeRefusesAnEdgeReadAtAnotherToken(t *testing.T) {
	inner, _ := scopeWorld(t)
	// The second LastSeq call is the edge read's: a smaller answer clamps its token.
	st := &spy{Store: inner, lastFn: func(n int, last uint64) uint64 {
		if n == 2 {
			return last - 1
		}
		return last
	}}
	sg, err := subgraph.ReadScope(ctx, st, []identity.Fingerprint{nodeN}, window30(store.Latest), subgraph.ScopePolicy{})
	if err == nil || sg.Elements != nil {
		t.Fatalf("read at two tokens: %v, %+v", err, sg)
	}
}
