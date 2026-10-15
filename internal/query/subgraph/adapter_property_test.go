package subgraph_test

import (
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/query/blastradius"
	"github.com/lotannauo/toposhift/internal/query/coresidency"
	"github.com/lotannauo/toposhift/internal/query/extract"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/query/subgraph"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// The world generator draws, on the grid of the scope tests, four pods, three
// nodes, two hosts, three services and four instances, every scheduled_on,
// runs_on, instance_of, part_of and depends_on edge among them, and records on
// a one-second grid, about one in five a nanosecond off it, written in any order.

type world struct {
	ents  []subgraph.Key
	edges []subgraph.Key
	pods  []identity.Fingerprint
	insts []identity.Fingerprint
	svcs  []identity.Fingerprint
	hosts []identity.Fingerprint
	nodes []identity.Fingerprint
}

func newWorld() world {
	var w world
	insts := &w.insts
	for i := 1; i <= 4; i++ {
		w.pods = append(w.pods, pod(i))
		*insts = append(*insts, inst(i))
	}
	for i := 1; i <= 3; i++ {
		w.nodes = append(w.nodes, node(i))
		w.svcs = append(w.svcs, svc(i))
	}
	for i := 1; i <= 2; i++ {
		w.hosts = append(w.hosts, host(i))
	}
	for _, fps := range [][]identity.Fingerprint{w.pods, w.insts, w.nodes, w.svcs, w.hosts} {
		for _, fp := range fps {
			w.ents = append(w.ents, ent(fp))
		}
	}
	for _, p := range w.pods {
		for _, n := range w.nodes {
			w.edges = append(w.edges, sched(p, n))
		}
	}
	// Placement is mostly one-to-one, with an alternative that makes some
	// mappings ambiguous: runs_on, instance_of and part_of.
	for i, n := range w.nodes {
		w.edges = append(w.edges, runsOn(n, w.hosts[i%2]), runsOn(n, w.hosts[(i+1)%2]))
	}
	for i, in := range w.insts {
		w.edges = append(w.edges, instOf(in, w.svcs[i%3]), instOf(in, w.svcs[(i+1)%3]),
			partOf(in, w.pods[i]), partOf(in, w.pods[(i+1)%4]))
	}
	for _, a := range w.svcs {
		for _, b := range w.svcs {
			if a != b {
				w.edges = append(w.edges, dependsOn(a, b))
			}
		}
	}
	return w
}

func drawWorldRecords(t *rapid.T, w world) []store.Record {
	var out []store.Record
	for _, e := range w.ents {
		if uniform(t, "base entity", 0, 7) > 0 {
			out = append(out, record(e, rapid.SampledFrom(gridProducers).Draw(t, "producer"), drawInstant(t, "at", -5, 5), 0, false))
		}
	}
	// Half the worlds start from a settled base: every entity, one-to-one
	// instances, pods and nodes on hosts, and the dependencies, which the
	// random records then change.
	if rapid.Bool().Draw(t, "settled") {
		for _, e := range w.ents {
			out = append(out, record(e, gridProducers[0], at(-50), 0, false))
		}
		for i, in := range w.insts {
			for _, k := range []subgraph.Key{instOf(in, w.svcs[i%3]), partOf(in, w.pods[i]), sched(w.pods[i], w.nodes[i%3])} {
				out = append(out, record(k, gridProducers[0], at(-50), 0, false))
			}
		}
		for i, n := range w.nodes {
			out = append(out, record(runsOn(n, w.hosts[i%2]), gridProducers[0], at(-50), 0, false))
		}
		out = append(out, record(dependsOn(w.svcs[1], w.svcs[0]), gridProducers[0], at(-50), 0, false),
			record(dependsOn(w.svcs[0], w.svcs[2]), gridProducers[0], at(-50), 0, false))
	}
	n := rapid.IntRange(10, 160).Draw(t, "records")
	for range n {
		var k subgraph.Key
		if uniform(t, "kind", 0, 6) == 0 {
			k = rapid.SampledFrom(w.ents).Draw(t, "entity")
		} else {
			k = rapid.SampledFrom(w.edges).Draw(t, "edge")
		}
		p := rapid.SampledFrom(gridProducers).Draw(t, "producer")
		x := drawInstant(t, "at", -4, 30)
		if uniform(t, "delete", 0, 5) == 0 {
			out = append(out, record(k, p, x, 0, true))
			continue
		}
		var ttl time.Duration
		if uniform(t, "with ttl", 0, 4) == 0 {
			ttl = time.Duration(uniform(t, "ttl", 2, 12)) * time.Second
		}
		out = append(out, record(k, p, x, ttl, false))
	}
	return out
}

// Whatever a drawn world holds, every result of the three queries is carried
// without error, and the subgraph agrees with the result at every instant and
// with the store's point reads for the asserted elements. This is the check
// that a derived span is always inside the time its legs exist.
func TestPropertyAdaptersAgainstTheResults(t *testing.T) {
	seen := map[string]int{}
	t.Cleanup(func() { t.Logf("what the drawn worlds held: %v", seen) })
	rapid.Check(t, func(t *rapid.T) {
		w := newWorld()
		st, err := memstore.Open(memstore.Options{})
		if err != nil {
			t.Fatal(err)
		}
		defer st.Close()
		writeShuffled(t, st, drawWorldRecords(t, w))
		from := uniform(t, "from", -3, 30)
		win := interval.Window{From: at(from), To: at(from + rapid.SampledFrom([]int{3, 8, 15, 25}).Draw(t, "width")), AsOf: store.Latest}
		nodeOnly := rapid.Bool().Draw(t, "node only")

		co := coresidency.DefaultPolicy()
		if rapid.Bool().Draw(t, "small caps") {
			co.MaxResults, co.MaxPerVia, co.MaxNodes, co.MaxHosts = 3, 2, 2, 1
		}
		if rapid.Bool().Draw(t, "min overlap") {
			co.MinOverlap = time.Duration(uniform(t, "overlap", 2, 6)) * time.Second
		}
		seed := rapid.SampledFrom(w.pods).Draw(t, "seed pod")
		cres, err := coresidency.NewFinder(st).Find(ctx, seed, win, coresidency.Options{NodeOnly: nodeOnly, Policy: co})
		if err != nil {
			t.Fatal(err)
		}
		sg, err := subgraph.FromCoResidency(ctx, st, cres, subgraph.ReadPolicy{})
		if err != nil {
			t.Fatalf("FromCoResidency: %v", err)
		}
		checkDerived(t, st, sg, coWant(cres))
		seen["co-residents"] += len(cres.CoResidents)
		seen["co suppressed"] += len(cres.Suppressed)
		seen["co unattributed"] += len(cres.Unattributed)

		ex := extract.DefaultPolicy()
		ex.CoResidency = co
		if rapid.Bool().Draw(t, "extract caps") {
			ex.MaxMembers, ex.MaxInstances, ex.MaxPairs = 2, 2, 2
		}
		eres, err := extract.NewExtractor(st).Extract(ctx, extract.Exploratory{Service: rapid.SampledFrom(w.svcs).Draw(t, "seed service")}, win,
			extract.Options{NodeOnly: nodeOnly, Policy: ex})
		if err != nil {
			t.Fatal(err)
		}
		esg, err := subgraph.FromExtraction(ctx, st, eres, subgraph.ReadPolicy{})
		if err != nil {
			t.Fatalf("FromExtraction: %v", err)
		}
		checkDerived(t, st, esg, exWant(eres))
		seen["pairs"] += len(eres.Pairs)
		seen["pair suppressed"] += len(eres.Suppressed)
		seen["unmapped"] += len(eres.Unmapped)
		seen["extract unattributed"] += len(eres.Unattributed)
		for _, p := range eres.Pairs {
			seen["pair "+p.Mechanism.String()]++
		}

		bseed := rapid.SampledFrom(append(append(append([]identity.Fingerprint{}, w.hosts...), w.nodes...), w.svcs...)).Draw(t, "blast seed")
		bp := blastradius.DefaultPolicy()
		if rapid.Bool().Draw(t, "blast caps") {
			bp.MaxDepth, bp.MaxMembers = 2, 5
		}
		bres, err := blastradius.NewWalker(st).Impact(ctx, bseed, win, blastradius.Options{Policy: bp})
		if err != nil {
			t.Fatal(err)
		}
		bsg, err := subgraph.FromBlastRadius(ctx, st, bres, subgraph.ReadPolicy{})
		if err != nil {
			t.Fatalf("FromBlastRadius: %v", err)
		}
		checkDerived(t, st, bsg, blastWant(bres))
		seen["members"] += len(bres.Members)
		seen["blast unattributed"] += len(bres.Unattributed)
	})
}
