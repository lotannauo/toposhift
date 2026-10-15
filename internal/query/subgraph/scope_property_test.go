package subgraph_test

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/graphdiff"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/query/subgraph"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// The grid generator, as graph diff's: four pods and three nodes (L2), two hosts
// (L1), their scheduled_on (L2) and runs_on (L1) edges, three producers, and
// records on a one-second grid over about 40 s, about one in five one
// nanosecond off it.

var gridProducers = []lifecycle.Producer{"p1", "p2", "p3"}

type grid struct {
	all   []identity.Fingerprint
	ents  []subgraph.Key
	edges []subgraph.Key
}

func newGrid() grid {
	var g grid
	var pods, nodes, hosts []identity.Fingerprint
	for i := 1; i <= 4; i++ {
		pods = append(pods, pod(i))
	}
	for i := 1; i <= 3; i++ {
		nodes = append(nodes, node(i))
	}
	for i := 1; i <= 2; i++ {
		hosts = append(hosts, host(i))
	}
	for _, fps := range [][]identity.Fingerprint{pods, nodes, hosts} {
		for _, fp := range fps {
			g.all = append(g.all, fp)
			g.ents = append(g.ents, ent(fp))
		}
	}
	for _, p := range pods {
		for _, n := range nodes {
			g.edges = append(g.edges, sched(p, n))
		}
	}
	for _, n := range nodes {
		for _, h := range hosts {
			g.edges = append(g.edges, runsOn(n, h))
		}
	}
	return g
}

func uniform(t *rapid.T, label string, lo, hi int) int {
	vs := make([]int, 0, hi-lo+1)
	for v := lo; v <= hi; v++ {
		vs = append(vs, v)
	}
	return rapid.SampledFrom(vs).Draw(t, label)
}

func drawInstant(t *rapid.T, label string, lo, hi int) time.Time {
	x := at(uniform(t, label, lo, hi))
	switch uniform(t, label+" shift", 0, 9) {
	case 0:
		x = x.Add(-1)
	case 1:
		x = x.Add(1)
	}
	return x
}

func record(k subgraph.Key, p lifecycle.Producer, x time.Time, ttl time.Duration, del bool) store.Record {
	s := sub(k)
	r := store.Record{Layer: layerOf(s), Subject: s, Producer: p, EventTime: x}
	if del {
		r.Kind = lifecycle.Delete
		return r
	}
	r.Kind, r.TTL, r.Payload = lifecycle.Observe, ttl, []byte("{}")
	return r
}

func drawRecords(t *rapid.T, g grid) []store.Record {
	var out []store.Record
	for _, e := range g.ents {
		if uniform(t, "base entity", 0, 3) > 0 {
			out = append(out, record(e, rapid.SampledFrom(gridProducers).Draw(t, "producer"), drawInstant(t, "at", -5, 5), 0, false))
		}
	}
	n := rapid.IntRange(0, 60).Draw(t, "records")
	for range n {
		var k subgraph.Key
		if uniform(t, "kind", 0, 4) == 0 {
			k = rapid.SampledFrom(g.ents).Draw(t, "entity")
		} else {
			k = rapid.SampledFrom(g.edges).Draw(t, "edge")
		}
		p := rapid.SampledFrom(gridProducers).Draw(t, "producer")
		x := drawInstant(t, "at", -2, 40)
		if uniform(t, "delete", 0, 3) == 0 {
			out = append(out, record(k, p, x, 0, true))
			continue
		}
		var ttl time.Duration
		if rapid.Bool().Draw(t, "with ttl") {
			ttl = time.Duration(uniform(t, "ttl", 1, 8)) * time.Second
		}
		out = append(out, record(k, p, x, ttl, false))
	}
	return out
}

func drawScope(t *rapid.T, g grid) []identity.Fingerprint {
	n := uniform(t, "scope size", 1, 4)
	out := make([]identity.Fingerprint, n)
	for i := range out {
		out[i] = rapid.SampledFrom(g.all).Draw(t, "scope entity")
	}
	return out
}

// writeShuffled writes the records in a random order and random batches,
// numbering them in write order.
func writeShuffled(t *rapid.T, st store.Store, recs []store.Record) {
	recs = slices.Clone(rapid.Permutation(recs).Draw(t, "order"))
	seq := st.LastSeq()
	for i := range recs {
		seq++
		recs[i].Seq = seq
	}
	for i := 0; i < len(recs); {
		k := rapid.IntRange(1, len(recs)-i).Draw(t, "batch")
		if err := st.Write(ctx, recs[i:i+k]); err != nil {
			t.Fatal(err)
		}
		i += k
	}
}

// pieces is an element's pieces as comparable strings, in absolute seconds-free
// form: [start, end) in nanoseconds after From, before, reason, producers.
func pieceStrings(ps []subgraph.Piece) []string {
	var out []string
	for _, p := range ps {
		out = append(out, fmt.Sprintf("[%d,%d) before=%t %s %v", p.Start, p.End, p.Before, p.EndReason, p.Producers))
	}
	return out
}

func intervalPieceStrings(ps []interval.Piece, from time.Time) []string {
	var out []string
	for _, p := range ps {
		r, err := subgraph.EndReasonOf(p.EndSource)
		if err != nil {
			panic(err)
		}
		out = append(out, fmt.Sprintf("[%d,%d) before=%t %s %v", p.Start.Sub(from), p.End.Sub(from), p.Before, r, p.Producers))
	}
	return out
}

// Every drawn scope read agrees with the store's point reads at every instant
// of the subgraph, and holds the bounds.
func TestPropertyScopeAgainstPointReads(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		g := newGrid()
		st, err := memstore.Open(memstore.Options{})
		if err != nil {
			t.Fatal(err)
		}
		defer st.Close()
		writeShuffled(t, st, drawRecords(t, g))
		scope := drawScope(t, g)
		from := uniform(t, "from", -3, 38)
		w := interval.Window{From: at(from), To: at(from + rapid.SampledFrom([]int{1, 3, 8, 15, 25}).Draw(t, "width")), AsOf: store.Latest}
		if rapid.Bool().Draw(t, "token") {
			w.AsOf = uint64(uniform(t, "token", 0, int(st.LastSeq())))
		}
		sg, err := subgraph.ReadScope(ctx, st, scope, w, subgraph.ScopePolicy{})
		if err != nil {
			t.Fatal(err)
		}
		checkAgainstPointReads(t, st, scope, sg)
		checkStructure(t, sg)
	})
}

// checkStructure checks the bounds that need no input: the first keyframe at
// From, later ones at delta instants, the entries within N0 + 2E, the seek
// within max(1, |S|), the pieces within N0 + E, and E the sum of the
// transitions of the elements.
func checkStructure(t tb, sg subgraph.Subgraph) {
	t.Helper()
	in := subgraph.Input{From: sg.From, To: sg.To}
	for _, el := range sg.Elements {
		s := subgraph.Source{Key: el.Key, Class: el.Class, Transitions: el.Transitions}
		for _, p := range el.Pieces {
			s.Pieces = append(s.Pieces, subgraph.SourcePiece{
				Start: sg.From.Add(time.Duration(p.Start)), End: sg.From.Add(time.Duration(p.End)),
				Before: p.Before, EndReason: p.EndReason, Producers: p.Producers,
			})
		}
		in.Sources = append(in.Sources, s)
	}
	checkBounds(t, in, sg)
}

// The scope rule is graph diff's. For the same scope, token and instants, every
// element graph diff lists in a bucket (created, deleted, transient, changed)
// is an element here with the same pieces, every element here that graph diff
// lists nowhere is stable, and the quarantined entries are the same.
func TestPropertyScopeRuleFollowsGraphdiff(t *testing.T) {
	seen := map[string]int{}
	t.Cleanup(func() { t.Logf("what the drawn diffs held: %v", seen) })
	rapid.Check(t, func(t *rapid.T) {
		g := newGrid()
		st, err := memstore.Open(memstore.Options{})
		if err != nil {
			t.Fatal(err)
		}
		defer st.Close()
		writeShuffled(t, st, drawRecords(t, g))
		scope := drawScope(t, g)
		a := uniform(t, "t1", -3, 38)
		t1 := at(a)
		t2 := at(a + rapid.SampledFrom([]int{1, 3, 8, 15, 25}).Draw(t, "width"))
		asOf := store.Latest
		if rapid.Bool().Draw(t, "token") {
			asOf = uint64(uniform(t, "token", 0, int(st.LastSeq())))
		}
		pol := graphdiff.Policy{MaxScope: 1 << 10, MaxListed: 1 << 20}
		diff, err := graphdiff.NewDiffer(st).Diff(ctx, scope, t1, t2, asOf, graphdiff.Options{Policy: pol})
		if err != nil {
			t.Fatal(err)
		}
		sg, err := subgraph.ReadScope(ctx, st, scope, interval.Window{From: t1, To: t2.Add(time.Nanosecond), AsOf: asOf}, subgraph.ScopePolicy{})
		if err != nil {
			t.Fatal(err)
		}
		if sg.Token != diff.Token {
			t.Fatalf("token %d, graph diff's %d", sg.Token, diff.Token)
		}
		listed := map[subgraph.Key]bool{}
		for _, bucket := range [][]graphdiff.Entry{diff.Created, diff.Deleted, diff.Changed, diff.Transient} {
			for _, e := range bucket {
				k := subgraph.KeyOf(e.Element.Subject)
				if listed[k] {
					continue
				}
				listed[k] = true
				id, ok := sg.ElementID(k)
				if !ok {
					t.Fatalf("%s is in a bucket of graph diff and not an element", k)
				}
				want := intervalPieceStrings(e.Element.Pieces, sg.From)
				if got := pieceStrings(sg.Elements[id].Pieces); !slices.Equal(got, want) {
					t.Fatalf("%s: pieces %v, graph diff's %v", k, got, want)
				}
				seen["listed elements"]++
			}
		}
		for _, el := range sg.Elements {
			if !listed[el.Key] {
				if el.Class != subgraph.ClassStable {
					t.Fatalf("%s is a %s element and graph diff lists it nowhere", el.Key, el.Class)
				}
				seen["stable elements"]++
			}
		}
		// The quarantined entries.
		var wantQ []string
		for _, el := range diff.Quarantined {
			wantQ = append(wantQ, subgraph.KeyOf(el.Subject).String())
		}
		var gotQ []string
		for _, q := range sg.Quarantine.Entities {
			gotQ = append(gotQ, subgraph.EntityKey(q.Fingerprint).String())
		}
		for _, e := range sg.Quarantine.Edges {
			gotQ = append(gotQ, e.Key.String())
		}
		slices.Sort(wantQ)
		slices.Sort(gotQ)
		if !slices.Equal(gotQ, wantQ) {
			t.Fatalf("quarantine %v, graph diff's %v", gotQ, wantQ)
		}
		seen["draws"]++
		// Every dictionary entity is a scope entity, or the far end of an
		// element's edge and then a boundary unless it is also a scope entity
		// with an existence element.
		inScope := map[identity.Fingerprint]bool{}
		for _, fp := range scope {
			inScope[fp] = true
		}
		for _, e := range sg.Entities {
			if !inScope[e.Fingerprint] && !e.Boundary {
				t.Fatalf("%s is not in the scope and is not a boundary", e.Fingerprint)
			}
		}
	})
}
