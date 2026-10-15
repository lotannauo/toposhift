package mcp

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// edgeKey is an edge of an answer, as the property compares it: the direction
// read, the relation, the peer, and the entity whose quarantine marks it.
type edgeKey struct {
	dir         string
	relation    string
	peer        identity.Fingerprint
	quarantined identity.Fingerprint
}

func (k edgeKey) String() string {
	return fmt.Sprintf("%s %s %s q=%s", k.dir, k.relation, k.peer, k.quarantined)
}

func neighborKeys(out NeighborsAtOutput) []edgeKey {
	var ks []edgeKey
	for _, e := range out.Edges {
		k := edgeKey{dir: e.Direction, relation: e.Relation, peer: e.Peer.FP}
		if e.Quarantined != nil {
			k.quarantined = e.Quarantined.Entity.FP
		}
		ks = append(ks, k)
	}
	return ks
}

// intervalKeys reads the same edges with the interval package: the window
// [at, at+1ns) with the cascade on, per direction, in the order of the tool.
func intervalKeys(t testing.TB, st store.Store, fp identity.Fingerprint, dirs []store.Direction, at time.Time, token uint64) []edgeKey {
	t.Helper()
	var ks []edgeKey
	rd := interval.NewReader(st, interval.Options{})
	for _, dir := range dirs {
		res, err := rd.Edges(ctx, fp, dir, interval.Window{From: at, To: at.Add(time.Nanosecond), AsOf: token})
		if err != nil {
			t.Fatal(err)
		}
		for _, el := range res.Elements {
			if len(el.Pieces) == 0 {
				continue
			}
			k := edgeKey{dir: dir.String(), relation: string(el.Subject.Relation), peer: el.Subject.B}
			if dir == store.Reverse {
				k.peer = el.Subject.A
			}
			if el.Uncascaded != nil {
				k.quarantined = el.Uncascaded.Entity
			}
			ks = append(ks, k)
		}
	}
	return ks
}

func sameKeys(a, b []edgeKey) bool { return slices.Equal(a, b) }

// drawer draws the integers a world is made of: rapid for the property, and a
// fixed generator for the sweep whose counts are claims.
type drawer interface {
	// Int draws an integer in [lo, hi].
	Int(label string, lo, hi int) int
}

type rapidDrawer struct{ t *rapid.T }

func (d rapidDrawer) Int(label string, lo, hi int) int {
	return rapid.IntRange(lo, hi).Draw(d.t, label)
}

type randDrawer struct{ r *rand.Rand }

func (d randDrawer) Int(_ string, lo, hi int) int { return lo + d.r.IntN(hi-lo+1) }

// graphWorld is the generator's fixed cast: two hosts and a zone, two nodes, two
// pods and a container, with the edges that can join them.
type graphWorld struct {
	entities []identity.Fingerprint
	hosts    []identity.Fingerprint
	edges    []store.Subject
}

func newGraphWorld() graphWorld {
	g := graphWorld{hosts: []identity.Fingerprint{numbered(catalog.Host, 1), numbered(catalog.Host, 2)}}
	zone := numbered(catalog.Zone, 1)
	nodes := []identity.Fingerprint{numbered(catalog.K8sNode, 1), numbered(catalog.K8sNode, 2)}
	pods := []identity.Fingerprint{numbered(catalog.K8sPod, 1), numbered(catalog.K8sPod, 2)}
	ctr := numbered(catalog.Container, 1)
	g.entities = append(append(append(slices.Clone(g.hosts), zone), nodes...), append(slices.Clone(pods), ctr)...)
	for _, p := range pods {
		for _, n := range nodes {
			g.edges = append(g.edges, edge(p, n, catalog.ScheduledOn))
		}
		g.edges = append(g.edges, edge(ctr, p, catalog.PartOf))
	}
	for _, n := range nodes {
		for _, h := range g.hosts {
			g.edges = append(g.edges, edge(n, h, catalog.RunsOn))
		}
	}
	for _, h := range g.hosts {
		g.edges = append(g.edges, edge(h, zone, catalog.LocatedIn))
	}
	return g
}

var graphBase = hm(10, 0)

func at(sec int) time.Time { return graphBase.Add(time.Duration(sec) * time.Second) }

// life draws the records of one subject: observed early, perhaps ended, perhaps
// back, with a TTL or without one.
func life(d drawer, sub store.Subject, boots func(n int) string) []store.Record {
	if d.Int("exists", 0, 9) == 0 {
		return nil
	}
	ttl := func() time.Duration {
		if d.Int("watch", 0, 1) == 0 {
			return 0
		}
		return time.Duration(d.Int("ttl", 15, 60)) * time.Second
	}
	obs := func(sec int) store.Record {
		r := observe(sub, "p1", at(sec), ttl(), `{}`)
		if boots != nil {
			r.Boot = boots(sec)
		}
		return r
	}
	recs := []store.Record{obs(d.Int("start", 0, 8))}
	if d.Int("ends", 0, 2) == 0 {
		recs = append(recs, deleted(sub, "p1", at(d.Int("end", 10, 30))))
		if d.Int("back", 0, 1) == 0 {
			recs = append(recs, obs(d.Int("again", 31, 40)))
		}
	}
	return recs
}

// drawGraph draws a world: every entity and edge lives its life, and a host has
// one boot, none, or a clone collision.
func drawGraph(d drawer, g graphWorld) []store.Record {
	var recs []store.Record
	for _, e := range g.entities {
		var boots func(int) string
		if e.Type() == catalog.Host {
			switch d.Int("host history", 0, 3) {
			case 0:
				boots = func(int) string { return "b1" }
			case 1:
				boots = func(int) string { return "" }
			default:
				// b1, then b2 from 5 s, then b1 again from 10 s: a clone collision for
				// an observation at or after 10 s.
				boots = func(sec int) string {
					switch {
					case sec < 5:
						return "b1"
					case sec < 10:
						return "b2"
					}
					return "b1"
				}
			}
		}
		recs = append(recs, life(d, ent(e), boots)...)
		if e.Type() == catalog.Host && d.Int("second record", 0, 1) == 0 {
			// A late observation at 6 s of the other boot.
			late := observe(ent(e), "p2", at(6), 0, `{}`)
			late.Boot = "b2"
			recs = append(recs, late)
		}
	}
	for _, e := range g.edges {
		recs = append(recs, life(d, e, nil)...)
	}
	return recs
}

// checkNeighborsAt writes a world and asks neighbors_at for every entity, in each
// direction, at several instants; each answer must equal the edges of the interval
// package. It returns how many edges the answers listed, and how many carry a
// quarantine.
func checkNeighborsAt(t testing.TB, d drawer, g graphWorld) (listed, quarantined int, err error) {
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
				got := neighborKeys(out)
				want := intervalKeys(t, st, fp, dirOf[dirName], when, token)
				if !sameKeys(got, want) {
					return 0, 0, fmt.Errorf("neighbors_at of %s at +%ds, token %d, direction %q:\n got %v\nwant %v", fp, sec, token, dirName, got, want)
				}
				if dirName != "both" {
					continue
				}
				for _, k := range got {
					listed++
					if k.quarantined != (identity.Fingerprint{}) {
						quarantined++
					}
				}
			}
		}
	}
	return listed, quarantined, nil
}

// The answer of neighbors_at, which reads points, equals the edges of the interval
// package over the instant with the cascade on, in every world the generator
// draws: quarantines (a host has two boots), edges stored in two layers, late
// records, deletes and expiries.
func TestNeighborsAtEqualsIntervalEdges(t *testing.T) {
	g := newGraphWorld()
	rapid.Check(t, func(rt *rapid.T) {
		if _, _, err := checkNeighborsAt(t, rapidDrawer{rt}, g); err != nil {
			rt.Fatal(err)
		}
	})
}

// The same, over worlds from fixed seeds, so that the claims about what the
// generator covers are the same on every run.
func TestNeighborsAtEqualsIntervalEdgesOnFixedWorlds(t *testing.T) {
	g := newGraphWorld()
	listed, marked, worlds := 0, 0, 150
	if raceEnabled {
		worlds = 60
	}
	for seed := range worlds {
		d := randDrawer{rand.New(rand.NewPCG(uint64(seed), 7))}
		l, q, err := checkNeighborsAt(t, d, g)
		if err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		listed += l
		marked += q
	}
	t.Logf("%d worlds: %d edges listed, %d marked by a quarantine", worlds, listed, marked)
	if listed < 20*worlds || marked < worlds/2 {
		t.Errorf("the generator is too thin: %d edges listed, %d marked by a quarantine, over %d worlds", listed, marked, worlds)
	}
}

// A quarantined source lists its edge uncut even when the target is dead, a
// quarantined target does when the source is dead, and a dead end otherwise
// takes the edge out of the answer.
func TestNeighborsAtQuarantinedEnds(t *testing.T) {
	w := newWorld(t, rankObjs)
	z2 := fpOf(t, catalog.Zone, catalog.ZoneID, "aws/eu-1/eu-1b")
	n9 := fpOf(t, catalog.K8sNode, catalog.K8sNodeUID, "node-uid-9")
	w.feed.write(
		observe(ent(z2), agent, hm(9, 0), 0, `{}`),
		observe(edge(w.h2, z2, catalog.LocatedIn), agent, hm(9, 0), 0, `{}`),
		deleted(ent(z2), agent, hm(9, 30)),
		observe(ent(n9), objs, hm(9, 0), 0, `{"name":"worker-9"}`),
		observe(edge(n9, w.h2, catalog.RunsOn), agent, hm(9, 0), 0, `{}`),
		deleted(ent(n9), objs, hm(9, 30)),
	)
	s := w.source(t, rankObjs, true)
	ask := func(fp identity.Fingerprint, dir string) NeighborsAtOutput {
		out, err := s.NeighborsAt(ctx, NeighborsAtInput{Entity: fp.String(), At: w10, Direction: dir})
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	has := func(out NeighborsAtOutput, rel string, peer identity.Fingerprint) *NeighborEdge {
		for i, e := range out.Edges {
			if e.Relation == rel && e.Peer.FP == peer {
				return &out.Edges[i]
			}
		}
		return nil
	}

	// H2 is the source of located_in to a zone that was deleted at 9:30.
	if e := has(ask(w.h2, "forward"), "located_in", z2); e == nil || e.Quarantined == nil || e.Quarantined.Entity.FP != w.h2 {
		t.Errorf("a quarantined source with a dead target: %+v", e)
	}
	// Read from the dead zone, the same edge is still the quarantined source's.
	if e := has(ask(z2, "reverse"), "located_in", w.h2); e == nil || e.Quarantined == nil || e.Quarantined.Entity.FP != w.h2 {
		t.Errorf("the same edge from the dead target: %+v", e)
	}
	// H2 is the target of runs_on from a node that was deleted at 9:30.
	if e := has(ask(w.h2, "reverse"), "runs_on", n9); e == nil || e.Quarantined == nil || e.Quarantined.Entity.FP != w.h2 {
		t.Errorf("a quarantined target with a dead source: %+v", e)
	}
	// A live source on a quarantined host is listed too, and marked.
	if e := has(ask(w.h2, "reverse"), "runs_on", w.n3); e == nil || e.Quarantined == nil {
		t.Errorf("a live source on a quarantined target: %+v", e)
	}
	// Nothing is quarantined at N1: P2 is listed while it exists, and not after its
	// deletion at 10:05, though the edge to it was never deleted.
	if e := has(ask(w.n1, "reverse"), "scheduled_on", w.p2); e == nil {
		t.Error("a pod that exists at 10:00 is not listed")
	}
	late, err := s.NeighborsAt(ctx, NeighborsAtInput{Entity: w.n1.String(), At: "2026-01-01T10:08:00Z", Direction: "reverse"})
	if err != nil {
		t.Fatal(err)
	}
	if has(late, "scheduled_on", w.p2) != nil {
		t.Error("an edge to a pod that ended at 10:05 is listed at 10:08")
	}
	if has(late, "scheduled_on", w.p1) == nil {
		t.Error("a live pod is not listed")
	}
}

// A runs_on edge (L1) has an L2 endpoint. When the instant lies between the two
// layers' horizons the endpoint's read is refused, and the whole answer gets the
// horizon sentence, naming the layer.
func TestNeighborsAtSplitHorizon(t *testing.T) {
	st := openMem(t, memstore.Options{Offsets: [4]time.Duration{0, time.Hour, 0, 0}, Policy: lifecycle.Policy{BootKey: lifecycle.BootID}})
	n, h := numbered(catalog.K8sNode, 1), numbered(catalog.Host, 1)
	newFeed(t, st).write(
		observe(ent(h), agent, hm(8, 0), 0, `{}`),
		observe(ent(n), objs, hm(8, 0), 0, `{"name":"n"}`),
		observe(edge(n, h, catalog.RunsOn), agent, hm(8, 0), 0, `{}`),
		observe(ent(n), objs, hm(10, 30), 0, `{"name":"n2"}`),
	)
	if err := st.Retain(ctx, hm(10, 0)); err != nil {
		t.Fatal(err)
	}
	// L2's horizon is 10:00 and L1's is 9:00.
	if st.LayerHorizon(catalog.L2).Time != hm(10, 0) || st.LayerHorizon(catalog.L1).Time != hm(9, 0) {
		t.Fatalf("horizons %v %v", st.LayerHorizon(catalog.L2), st.LayerHorizon(catalog.L1))
	}
	s, err := NewSource(Config{Store: st, Rank: rankObjs, RangeReads: true})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.NeighborsAt(ctx, NeighborsAtInput{Entity: n.String(), At: "2026-01-01T09:30:00Z", Direction: "forward"})
	want := fmt.Sprintf("The store no longer keeps history of layer L2 before 2026-01-01T10:00:00Z or below token %d.", st.LayerHorizon(catalog.L2).Seq)
	wantErr(t, "between the horizons", err, want, store.ErrBeforeHorizon)

	// After both horizons the answer is whole, and the edge is listed.
	out, err := s.NeighborsAt(ctx, NeighborsAtInput{Entity: n.String(), At: "2026-01-01T10:15:00Z", Direction: "forward"})
	if err != nil || len(out.Edges) != 1 {
		t.Errorf("after both horizons: %v, %+v", err, out.Edges)
	}
}

// A stored edge that disagrees with the catalog is an error, never dropped.
func TestNeighborsAtRefusesDriftedEdges(t *testing.T) {
	w := newWorld(t, rankObjs)
	rec := func(sub store.Subject) store.Record {
		r := observe(sub, objs, hm(9, 0), 0, `{}`)
		r.Layer = catalog.L2
		return r
	}
	w.feed.write(
		rec(edge(w.p3, w.n2, "mystery_rel")),
		rec(edge(w.n2, w.p1, catalog.ScheduledOn)),
		rec(edge(w.p4, w.n2, catalog.SameAs)),
	)
	s := w.source(t, rankObjs, true)
	for _, tc := range []struct {
		name string
		fp   identity.Fingerprint
		dir  string
		want string
	}{
		{"an unregistered relation", w.p3, "forward", `the relation "mystery_rel" is not registered`},
		{"a relation between the wrong types", w.p1, "reverse", `the relation "scheduled_on" does not connect k8s.node to k8s.pod`},
		{"a derived relation", w.p4, "forward", `the relation "same_as" is derived and never stored`},
	} {
		_, err := s.NeighborsAt(ctx, NeighborsAtInput{Entity: tc.fp.String(), At: w10, Direction: tc.dir})
		var te *Error
		if !errors.As(err, &te) || !strings.Contains(te.Message, tc.want) || !strings.HasPrefix(te.Message, "A stored edge disagrees with the catalog") {
			t.Errorf("%s: %v", tc.name, err)
		}
	}
}
