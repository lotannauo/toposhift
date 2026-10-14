package coresidency_test

import (
	"context"
	"slices"
	"time"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/coresidency"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// The grid generator: four pods and three nodes (L2), two hosts (L1), their
// scheduled_on (L2) and runs_on (L1) edges, three producers, and records on a
// one-second grid over about 40 s, about one in five one nanosecond off it.

var gridBase = time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)

var gridProducers = []lifecycle.Producer{"p1", "p2", "p3"}

type grid struct {
	pods, nodes, hosts []identity.Fingerprint
	entities, scheds   []store.Subject
	runs               []store.Subject
}

func newGrid() grid {
	var g grid
	for i := range 4 {
		g.pods = append(g.pods, pod(byte(1+i)))
	}
	for i := range 3 {
		g.nodes = append(g.nodes, node(byte(1+i)))
	}
	for i := range 2 {
		g.hosts = append(g.hosts, host(byte(1+i)))
	}
	for _, fps := range [][]identity.Fingerprint{g.pods, g.nodes, g.hosts} {
		for _, fp := range fps {
			g.entities = append(g.entities, ent(fp))
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

// drawRecord is one record with no Seq. Entities are drawn as often as edges,
// and mostly observed without a TTL early on, so that edges meet live ends.
func drawRecord(t *rapid.T, g grid) store.Record {
	var sub store.Subject
	switch uniform(t, "kind of subject", 0, 5) {
	case 0:
		sub = rapid.SampledFrom(g.entities).Draw(t, "entity")
		if uniform(t, "entity early", 0, 2) > 0 {
			return observe(sub, rapid.SampledFrom(gridProducers).Draw(t, "producer"), drawInstant(t, "at", -5, 8), 0)
		}
	case 1, 2, 3:
		sub = rapid.SampledFrom(g.scheds).Draw(t, "scheduled_on")
	default:
		sub = rapid.SampledFrom(g.runs).Draw(t, "runs_on")
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

// drawRecords gives most entities an Observe without a TTL near the start, so
// that most edges meet live ends, and most nodes a runs_on to a home host (the
// first and third node share one), then draws up to max more records.
func drawRecords(t *rapid.T, g grid, max int) []store.Record {
	var out []store.Record
	for _, e := range g.entities {
		if uniform(t, "base entity", 0, 3) > 0 {
			out = append(out, observe(e, rapid.SampledFrom(gridProducers).Draw(t, "producer"), drawInstant(t, "at", -5, 5), 0))
		}
	}
	for i, nodeFP := range g.nodes {
		if uniform(t, "home host", 0, 3) > 0 {
			out = append(out, observe(runsOn(nodeFP, g.hosts[i%len(g.hosts)]), "p1", drawInstant(t, "at", -5, 5), 0))
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

// wide is the policy of the properties: no minimum overlap, and caps that
// truncate nothing.
func wide() coresidency.Policy {
	return coresidency.Policy{MinOverlap: 0, MaxNodes: 1 << 10, MaxHosts: 1 << 10, MaxPerVia: 1 << 20, MaxResults: 1 << 20}
}

// gridCase is one drawn store, window and query.
type gridCase struct {
	g        grid
	st       *memstore.Store
	recs     []store.Record
	w        interval.Window
	nodeOnly bool
}

func drawCase(t *rapid.T) gridCase {
	g := newGrid()
	st := freshStore(t, memstore.Options{}, nil)
	recs := writeShuffled(t, st, drawRecords(t, g, 70))
	from, to := drawWindow(t)
	return gridCase{
		g: g, st: st, recs: recs,
		w:        interval.Window{From: from, To: to, AsOf: drawToken(t, st.LastSeq())},
		nodeOnly: uniform(t, "node only", 0, 3) == 0,
	}
}

func (c gridCase) options() coresidency.Options {
	return coresidency.Options{NodeOnly: c.nodeOnly, Policy: wide()}
}
