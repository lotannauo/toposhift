package graphdiff_test

import (
	"context"
	"slices"
	"time"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/graphdiff"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// The grid generator, as package coresidency's: four pods and three nodes
// (L2), two hosts (L1), their scheduled_on (L2) and runs_on (L1) edges, three
// producers, and records on a one-second grid over about 40 s, about one in
// five one nanosecond off it. Every Observe carries a payload drawn from small
// JSON objects that differ in a state key, an attribute key, both or nothing,
// the empty payload, a respaced object, a number written two ways, a nested
// object reordered and respaced, null and a payload that is not JSON.

var gridBase = time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)

var gridProducers = []lifecycle.Producer{"p1", "p2", "p3"}

var gridPayloads = []string{
	"",
	`{"phase":"a"}`,
	`{"phase":"b"}`,
	`{"name":"x","phase":"a"}`,
	`{"name":"y","phase":"a"}`,
	`{"name":"y","phase":"b"}`,
	`{ "phase": "a" }`,
	`{"a":1}`,
	`{"a":1.0}`,
	`{"x":{"p":1,"q":2},"phase":"a"}`,
	`{ "phase":"a", "x":{ "q":2, "p":1 } }`,
	`null`,
	`x`,
}

type grid struct {
	pods, nodes, hosts []identity.Fingerprint
	all                []identity.Fingerprint
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
			g.all = append(g.all, fp)
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

func drawObserve(t *rapid.T, sub store.Subject, p lifecycle.Producer, at time.Time, ttl time.Duration) store.Record {
	return observeP(sub, p, at, ttl, rapid.SampledFrom(gridPayloads).Draw(t, "payload"))
}

// drawRecord is one record with no Seq. Entities are drawn as often as edges,
// and mostly observed without a TTL early on, so that edges meet live ends.
func drawRecord(t *rapid.T, g grid) store.Record {
	var sub store.Subject
	switch uniform(t, "kind of subject", 0, 5) {
	case 0:
		sub = rapid.SampledFrom(g.entities).Draw(t, "entity")
		if uniform(t, "entity early", 0, 2) > 0 {
			return drawObserve(t, sub, rapid.SampledFrom(gridProducers).Draw(t, "producer"), drawInstant(t, "at", -5, 8), 0)
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
	r := drawObserve(t, sub, p, at, ttl)
	if uniform(t, "run", 0, 3) == 0 {
		r.Through = at.Truncate(time.Second).Add(time.Duration(uniform(t, "through", 1, 6)) * time.Second)
	}
	return r
}

// drawRecords gives most entities an Observe without a TTL near the start, and
// most nodes a runs_on to a home host, then draws up to max more records, a
// fifth of them more observations of entities, so descriptions change.
func drawRecords(t *rapid.T, g grid, max int) []store.Record {
	var out []store.Record
	for _, e := range g.entities {
		if uniform(t, "base entity", 0, 3) > 0 {
			out = append(out, drawObserve(t, e, rapid.SampledFrom(gridProducers).Draw(t, "producer"), drawInstant(t, "at", -5, 5), 0))
		}
	}
	for i, nodeFP := range g.nodes {
		if uniform(t, "home host", 0, 3) > 0 {
			out = append(out, drawObserve(t, runsOn(nodeFP, g.hosts[i%len(g.hosts)]), "p1", drawInstant(t, "at", -5, 5), 0))
		}
	}
	n := rapid.IntRange(0, max).Draw(t, "records")
	if rapid.Bool().Draw(t, "dense") {
		n = max - n/4
	}
	for range n {
		if uniform(t, "redescribe", 0, 4) == 0 {
			e := rapid.SampledFrom(g.entities).Draw(t, "entity")
			p, at := rapid.SampledFrom(gridProducers).Draw(t, "producer"), drawInstant(t, "at", 0, 40)
			switch uniform(t, "entity churn", 0, 3) {
			case 0:
				out = append(out, deleted(e, p, at))
			case 1:
				out = append(out, drawObserve(t, e, p, at, time.Duration(uniform(t, "ttl", 1, 8))*time.Second))
			default:
				out = append(out, drawObserve(t, e, p, at, 0))
			}
			continue
		}
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

// drawInstants draws t1 < t2 on the grid, each possibly a nanosecond off it.
func drawInstants(t *rapid.T) (time.Time, time.Time) {
	a := uniform(t, "t1", -3, 40)
	b := min(44, a+rapid.SampledFrom([]int{1, 3, 8, 15, 25, 47}).Draw(t, "width"))
	t1 := gridBase.Add(time.Duration(a)*time.Second + time.Duration(uniform(t, "t1 shift", -1, 1)))
	t2 := gridBase.Add(time.Duration(b)*time.Second + time.Duration(uniform(t, "t2 shift", -1, 1)))
	return t1, t2
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

// drawRank is no ranks, or ranks drawn over a subset of the producers, ties
// included.
func drawRank(t *rapid.T, label string) map[lifecycle.Producer]int {
	rank := map[lifecycle.Producer]int{}
	for _, p := range gridProducers {
		if rapid.Bool().Draw(t, label+" listed "+string(p)) {
			rank[p] = uniform(t, label+" rank "+string(p), 0, 2)
		}
	}
	return rank
}

// drawScope is one to four entities of the grid, possibly repeated.
func drawScope(t *rapid.T, g grid) []identity.Fingerprint {
	n := uniform(t, "scope size", 1, 4)
	out := make([]identity.Fingerprint, n)
	for i := range out {
		out[i] = rapid.SampledFrom(g.all).Draw(t, "scope entity")
	}
	return out
}

// gridCase is one drawn store, scope, pair of instants, token and ranks.
type gridCase struct {
	g      grid
	st     *memstore.Store
	recs   []store.Record
	scope  []identity.Fingerprint
	t1, t2 time.Time
	asOf   uint64
	rank   map[lifecycle.Producer]int
}

func drawCase(t *rapid.T) gridCase {
	g := newGrid()
	st := freshStore(t, memstore.Options{}, nil)
	recs := writeShuffled(t, st, drawRecords(t, g, 70))
	t1, t2 := drawInstants(t)
	return gridCase{
		g: g, st: st, recs: recs, scope: drawScope(t, g), t1: t1, t2: t2,
		asOf: drawToken(t, st.LastSeq()), rank: drawRank(t, "rank"),
	}
}

func (c gridCase) options() graphdiff.Options {
	return graphdiff.Options{Rank: c.rank, Policy: wide()}
}
