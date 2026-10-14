package blastradius_test

import (
	"context"
	"slices"
	"time"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/blastradius"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// The grid generator: two racks, a zone, two hosts, three nodes, four pods,
// three containers, three services and a service instance, with their entity
// records; every edge the catalog allows between them (depends_on cycles
// included, but no edge from an entity to itself); three producers; and
// records on a one-second grid over about 40 s, about one in five one
// nanosecond off it. Each record takes its layer from the catalog.

var gridBase = time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)

var gridProducers = []lifecycle.Producer{"p1", "p2", "p3"}

type grid struct {
	racks, zones, hosts, nodes, pods, containers, services, instances []identity.Fingerprint
	all                                                               []identity.Fingerprint
	ents                                                              []store.Subject
	edges                                                             []store.Subject
	// home is the skeleton: one usual edge per entity that has one, so that
	// paths of several hops are common.
	home []store.Subject
}

func newGrid() grid {
	var g grid
	mk := func(n int, f func(byte) identity.Fingerprint) []identity.Fingerprint {
		var out []identity.Fingerprint
		for i := range n {
			out = append(out, f(byte(1+i)))
		}
		return out
	}
	g.racks, g.zones, g.hosts, g.nodes = mk(2, rack), mk(1, zone), mk(2, host), mk(3, node)
	g.pods, g.containers, g.services, g.instances = mk(4, pod), mk(3, container), mk(3, service), mk(1, instance)
	for _, fps := range [][]identity.Fingerprint{g.racks, g.zones, g.hosts, g.nodes, g.pods, g.containers, g.services, g.instances} {
		g.all = append(g.all, fps...)
	}
	for _, fp := range g.all {
		g.ents = append(g.ents, ent(fp))
	}
	pairs := func(from, to []identity.Fingerprint, f func(a, b identity.Fingerprint) store.Subject) {
		for _, a := range from {
			for _, b := range to {
				if a != b {
					g.edges = append(g.edges, f(a, b))
				}
			}
		}
	}
	pairs(g.hosts, g.racks, locatedIn)
	pairs(g.hosts, g.zones, locatedIn)
	pairs(g.nodes, g.hosts, runsOn)
	pairs(g.pods, g.nodes, sched)
	pairs(g.containers, g.pods, partOf)
	pairs(g.services, g.services, dependsOn)
	pairs(g.instances, g.services, instanceOf)
	pairs(g.instances, g.pods, partOf)
	for i, h := range g.hosts {
		g.home = append(g.home, locatedIn(h, g.racks[i%len(g.racks)]), locatedIn(h, g.zones[0]))
	}
	for i, n := range g.nodes {
		g.home = append(g.home, runsOn(n, g.hosts[i%len(g.hosts)]))
	}
	for i, p := range g.pods {
		g.home = append(g.home, sched(p, g.nodes[i%len(g.nodes)]))
	}
	for i, c := range g.containers {
		g.home = append(g.home, partOf(c, g.pods[i]))
	}
	for i := 1; i < len(g.services); i++ {
		g.home = append(g.home, dependsOn(g.services[i], g.services[i-1]))
	}
	g.home = append(g.home, instanceOf(g.instances[0], g.services[0]), partOf(g.instances[0], g.pods[0]))
	return g
}

// uniform draws an integer in [lo, hi] with every value about as likely:
// rapid.IntRange favors small values.
func uniform(t *rapid.T, label string, lo, hi int) int {
	vs := make([]int, 0, hi-lo+1)
	for v := lo; v <= hi; v++ {
		vs = append(vs, v)
	}
	return rapid.SampledFrom(vs).Draw(t, label)
}

func drawInstant(t *rapid.T, label string, lo, hi int) time.Time {
	at := gridBase.Add(time.Duration(uniform(t, label, lo, hi)) * time.Second)
	switch uniform(t, label+" shift", 0, 9) {
	case 0:
		at = at.Add(-1)
	case 1:
		at = at.Add(1)
	}
	return at
}

// drawRecord is one record with no Seq: an entity a sixth of the time, mostly
// observed without a TTL early on, and otherwise an edge, from the skeleton
// half the time.
func drawRecord(t *rapid.T, g grid) store.Record {
	var sub store.Subject
	switch uniform(t, "kind of subject", 0, 5) {
	case 0:
		sub = rapid.SampledFrom(g.ents).Draw(t, "entity")
		if uniform(t, "entity early", 0, 2) > 0 {
			return observe(sub, rapid.SampledFrom(gridProducers).Draw(t, "producer"), drawInstant(t, "at", -5, 8), 0)
		}
	case 1, 2:
		sub = rapid.SampledFrom(g.home).Draw(t, "skeleton edge")
	default:
		sub = rapid.SampledFrom(g.edges).Draw(t, "edge")
	}
	p := rapid.SampledFrom(gridProducers).Draw(t, "producer")
	at := drawInstant(t, "at", 0, 40)
	if uniform(t, "delete", 0, 3) == 0 {
		return deleted(sub, p, at)
	}
	var ttl time.Duration
	if rapid.Bool().Draw(t, "with ttl") {
		ttl = time.Duration(uniform(t, "ttl", 1, 8)) * time.Second
	}
	r := observe(sub, p, at, ttl)
	if uniform(t, "run", 0, 3) == 0 {
		r.Through = at.Truncate(time.Second).Add(time.Duration(uniform(t, "through", 1, 6)) * time.Second)
	}
	return r
}

// drawRecords gives most entities an Observe without a TTL near the start, and
// most skeleton edges one too, so that most edges meet live ends and paths of
// several hops are live, then draws up to max more records.
func drawRecords(t *rapid.T, g grid, max int) []store.Record {
	var out []store.Record
	for _, e := range g.ents {
		if uniform(t, "base entity", 0, 5) > 0 {
			out = append(out, observe(e, rapid.SampledFrom(gridProducers).Draw(t, "producer"), drawInstant(t, "at", -5, 5), 0))
		}
	}
	for _, e := range g.home {
		if uniform(t, "base edge", 0, 3) > 0 {
			out = append(out, observe(e, "p1", drawInstant(t, "at", -5, 10), 0))
		}
	}
	n := rapid.IntRange(0, max).Draw(t, "records")
	if rapid.Bool().Draw(t, "dense") {
		n = max - n/4
	}
	for range n {
		out = append(out, drawRecord(t, g))
	}
	return out
}

// writeShuffled writes the records in a random order and random batches,
// numbering them in write order, and returns them with their Seq.
func writeShuffled(t *rapid.T, st store.Store, recs []store.Record) []store.Record {
	recs = slices.Clone(rapid.Permutation(recs).Draw(t, "order"))
	seq := st.LastSeq()
	for i := range recs {
		seq++
		recs[i].Seq = seq
	}
	for i := 0; i < len(recs); {
		k := rapid.IntRange(1, len(recs)-i).Draw(t, "batch")
		if err := st.Write(context.Background(), recs[i:i+k]); err != nil {
			t.Fatal(err)
		}
		i += k
	}
	return recs
}

func freshStore(t interface{ Fatal(...any) }, opts memstore.Options, recs []store.Record) *memstore.Store {
	st, err := memstore.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range recs {
		if err := st.Write(context.Background(), []store.Record{r}); err != nil {
			t.Fatal(err)
		}
	}
	return st
}

func drawWindow(t *rapid.T) (time.Time, time.Time) {
	a := uniform(t, "from", -3, 40)
	b := min(44, a+rapid.SampledFrom([]int{1, 3, 8, 15, 25, 47}).Draw(t, "width"))
	from := gridBase.Add(time.Duration(a)*time.Second + time.Duration(uniform(t, "from shift", -1, 1)))
	to := gridBase.Add(time.Duration(b)*time.Second + time.Duration(uniform(t, "to shift", -1, 1)))
	return from, to
}

// drawToken is Latest half the time, and otherwise a token in
// {0, ..., last + 1}.
func drawToken(t *rapid.T, last uint64) uint64 {
	if rapid.Bool().Draw(t, "latest") {
		return store.Latest
	}
	k := uniform(t, "token", 0, int(last)+2)
	if k == int(last)+2 {
		return store.Latest
	}
	return uint64(k)
}

func clampToken(tok, last uint64) uint64 {
	if tok == store.Latest || tok > last {
		return last
	}
	return tok
}

// wide is the policy of the properties: a drawn depth, and caps that cut
// nothing.
func wide(depth int) blastradius.Policy {
	return blastradius.Policy{MaxDepth: depth, MaxFrontier: 1 << 20, MaxMembers: 1 << 20}
}

// gridCase is one drawn store, window, token and depth.
type gridCase struct {
	g     grid
	st    *memstore.Store
	recs  []store.Record
	w     interval.Window
	depth int
}

func drawCase(t *rapid.T) gridCase {
	g := newGrid()
	st := freshStore(t, memstore.Options{}, nil)
	recs := writeShuffled(t, st, drawRecords(t, g, 70))
	from, to := drawWindow(t)
	return gridCase{
		g: g, st: st, recs: recs,
		w:     interval.Window{From: from, To: to, AsOf: drawToken(t, st.LastSeq())},
		depth: uniform(t, "depth", 1, 6),
	}
}

func (c gridCase) options() blastradius.Options { return blastradius.Options{Policy: wide(c.depth)} }
