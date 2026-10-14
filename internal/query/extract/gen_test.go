package extract_test

import (
	"context"
	"slices"
	"time"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/coresidency"
	"github.com/lotannauo/toposhift/internal/query/extract"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// The grid generator: three services, four instances, four pods, three nodes
// and two hosts; depends_on among the services, instance_of, part_of from an
// instance to a pod, scheduled_on and runs_on; three producers, and records on
// a one-second grid over about 40 s, about one in five one nanosecond off it.
// Times, TTLs, producers and Deletes are drawn as in package coresidency's
// generator.

var gridBase = time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)

var gridProducers = []lifecycle.Producer{"p1", "p2", "p3"}

type grid struct {
	services, insts, pods, nodes, hosts []identity.Fingerprint
	entities, deps, instOfs, partOfs    []store.Subject
	scheds, runs                        []store.Subject
}

func newGrid() grid {
	var g grid
	for i := range 3 {
		g.services = append(g.services, svc(byte(1+i)))
	}
	for i := range 4 {
		g.insts = append(g.insts, inst(byte(1+i)))
		g.pods = append(g.pods, pod(byte(1+i)))
	}
	for i := range 3 {
		g.nodes = append(g.nodes, node(byte(1+i)))
	}
	for i := range 2 {
		g.hosts = append(g.hosts, host(byte(1+i)))
	}
	for _, fps := range [][]identity.Fingerprint{g.services, g.insts, g.pods, g.nodes, g.hosts} {
		for _, fp := range fps {
			g.entities = append(g.entities, ent(fp))
		}
	}
	for _, a := range g.services {
		for _, b := range g.services {
			if a != b {
				g.deps = append(g.deps, dependsOn(a, b))
			}
		}
	}
	for _, i := range g.insts {
		for _, s := range g.services {
			g.instOfs = append(g.instOfs, instOf(i, s))
		}
		for _, p := range g.pods {
			g.partOfs = append(g.partOfs, partOf(i, p))
		}
	}
	for _, p := range g.pods {
		for _, n := range g.nodes {
			g.scheds = append(g.scheds, sched(p, n))
		}
	}
	for _, n := range g.nodes {
		for _, h := range g.hosts {
			g.runs = append(g.runs, runsOn(n, h))
		}
	}
	return g
}

func (g grid) all() []identity.Fingerprint {
	return slices.Concat(g.services, g.insts, g.pods, g.nodes, g.hosts)
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

// drawRecord is one record with no Seq. Entities are drawn as often as one
// kind of edge, and mostly observed without a TTL early on, so that edges meet
// live ends.
func drawRecord(t *rapid.T, g grid) store.Record {
	var sub store.Subject
	switch uniform(t, "kind of subject", 0, 6) {
	case 0:
		sub = rapid.SampledFrom(g.entities).Draw(t, "entity")
		if uniform(t, "entity early", 0, 2) > 0 {
			return observe(sub, rapid.SampledFrom(gridProducers).Draw(t, "producer"), drawInstant(t, "at", -5, 8), 0)
		}
	case 1, 2:
		sub = rapid.SampledFrom(g.scheds).Draw(t, "scheduled_on")
	case 3:
		sub = rapid.SampledFrom(g.runs).Draw(t, "runs_on")
	case 4:
		// Half the time the home edge, so that most instances keep one service.
		sub = rapid.SampledFrom(g.instOfs).Draw(t, "instance_of")
		if rapid.Bool().Draw(t, "home instance_of") {
			i := uniform(t, "instance", 0, len(g.insts)-1)
			sub = instOf(g.insts[i], g.services[homeService[i]])
		}
	case 5:
		sub = rapid.SampledFrom(g.partOfs).Draw(t, "part_of")
		if rapid.Bool().Draw(t, "home part_of") {
			i := uniform(t, "instance", 0, len(g.insts)-1)
			sub = partOf(g.insts[i], g.pods[i])
		}
	default:
		sub = rapid.SampledFrom(g.deps).Draw(t, "depends_on")
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

// homeService is each instance's home service: the first and fourth pods share
// the first node and have different services, so the home placement has a
// shared node, and through the first host a shared host.
var homeService = []int{0, 1, 2, 1}

// drawRecords gives most entities an Observe without a TTL before every
// window, and most of a home placement (each instance of a home service and
// part of its own pod, or the last instance part of the first pod; each pod on
// a home node, each node on a home host, the first and third node sharing one;
// and two dependencies), then draws up to max more records.
func drawRecords(t *rapid.T, g grid, max int) []store.Record {
	var out []store.Record
	early := func(sub store.Subject, label string) {
		if uniform(t, label, 0, 7) > 0 {
			out = append(out, observe(sub, rapid.SampledFrom(gridProducers).Draw(t, "producer"), drawInstant(t, "at", -6, -4), 0))
		}
	}
	for _, e := range g.entities {
		if uniform(t, "base entity", 0, 15) > 0 {
			out = append(out, observe(e, rapid.SampledFrom(gridProducers).Draw(t, "producer"), drawInstant(t, "at", -6, -4), 0))
		}
	}
	for i, n := range g.nodes {
		early(runsOn(n, g.hosts[i%len(g.hosts)]), "home host")
	}
	for i, in := range g.insts {
		early(instOf(in, g.services[homeService[i]]), "home service")
		home := g.pods[i]
		if i == len(g.insts)-1 && rapid.Bool().Draw(t, "sidecar") {
			// The last instance runs in the first pod, beside an instance of
			// another service, and its own pod is left with none.
			home = g.pods[0]
		}
		early(partOf(in, home), "home pod")
	}
	for i, p := range g.pods {
		early(sched(p, g.nodes[i%len(g.nodes)]), "home node")
	}
	early(dependsOn(g.services[0], g.services[1]), "dependency")
	early(dependsOn(g.services[2], g.services[0]), "dependency")
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

// drawAt is an instant of [from, to): from, or a grid second inside, or one
// nanosecond either side of one.
func drawAt(t *rapid.T, from, to time.Time) time.Time {
	cands := []time.Time{from}
	for s := -3; s <= 44; s++ {
		for _, d := range []time.Duration{-1, 0, 1} {
			if x := gridBase.Add(time.Duration(s)*time.Second + d); !x.Before(from) && x.Before(to) {
				cands = append(cands, x)
			}
		}
	}
	return rapid.SampledFrom(cands).Draw(t, "at")
}

// drawToken is Latest three times in four, and otherwise a token in
// {0, ..., last + 1}.
func drawToken(t *rapid.T, last uint64) uint64 {
	if uniform(t, "latest", 0, 3) > 0 {
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

// wide is the policy of the properties: caps that truncate nothing, no
// co-residency minimum overlap, and ubiquity never judged.
func wide() extract.Policy {
	return extract.Policy{
		MaxMembers: 1 << 10, MaxInstances: 1 << 10, MaxSeedPods: 1 << 10, MaxPairs: 1 << 20,
		UbiquityNum: 4, UbiquityDen: 5, UbiquityMinNodes: 1 << 10,
		CoResidency: coresidency.Policy{MinOverlap: 0, MaxNodes: 1 << 10, MaxHosts: 1 << 10, MaxPerVia: 1 << 20, MaxResults: 1 << 20},
	}
}

// gridCase is one drawn store, window and query options.
type gridCase struct {
	g        grid
	st       *memstore.Store
	recs     []store.Record
	w        interval.Window
	nodeOnly bool
	pol      extract.Policy
	anchors  []extract.Anchored
}

func drawCase(t *rapid.T) gridCase {
	g := newGrid()
	st := freshStore(t, memstore.Options{}, nil)
	recs := writeShuffled(t, st, drawRecords(t, g, 50))
	from, to := drawWindow(t)
	pol := wide()
	if rapid.Bool().Draw(t, "judge ubiquity") {
		pol.UbiquityDen = uniform(t, "ubiquity den", 1, 5)
		pol.UbiquityNum = uniform(t, "ubiquity num", 0, pol.UbiquityDen)
		pol.UbiquityMinNodes = uniform(t, "ubiquity min nodes", 1, 3)
	}
	c := gridCase{
		g: g, st: st, recs: recs, pol: pol,
		w:        interval.Window{From: from, To: to, AsOf: drawToken(t, st.LastSeq())},
		nodeOnly: uniform(t, "node only", 0, 3) == 0,
	}
	for _, s := range g.services {
		at := drawAt(t, from, to)
		for _, h := range g.hosts {
			c.anchors = append(c.anchors, extract.Anchored{Service: s, Host: h, At: at})
		}
	}
	return c
}

// seedCase is one seed of a case, with its options.
type seedCase struct {
	seed extract.Seed
	o    extract.Options
}

// seeds is every seed of a case with its options: each service exploratory,
// and each service anchored at each host at its drawn instant.
func (c gridCase) seeds() []seedCase {
	var out []seedCase
	for _, s := range c.g.services {
		out = append(out, seedCase{extract.Exploratory{Service: s}, extract.Options{NodeOnly: c.nodeOnly, Policy: c.pol}})
	}
	for _, a := range c.anchors {
		out = append(out, seedCase{a, extract.Options{Policy: c.pol}})
	}
	return out
}
