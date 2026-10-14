package coresidency_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/coresidency"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

// The generated cluster streams of storetest, with heartbeats, runs, a second
// producer on placements, late records, reboots and clones, checked at fixed
// windows against the oracle and its read formula (C1, C12) and the store's
// point reads (C2).

type stream struct {
	name   string
	cfg    storetest.Config
	policy lifecycle.Policy
}

func streams() []stream {
	var out []stream
	for _, seed := range []uint64{1, 2, 3} {
		c := storetest.Tiny()
		c.Seed = seed
		out = append(out, stream{fmt.Sprintf("tiny seed %d", seed), c, lifecycle.Policy{}})
	}
	c := storetest.Tiny()
	c.Runs, c.ConfirmProbability, c.LateProbability = true, 0.5, 0.3
	out = append(out, stream{"runs, confirmed, late", c, lifecycle.Policy{}})
	c = storetest.Tiny()
	c.Runs, c.RebootProbability, c.CloneProbability = true, 0.2, 0.5
	out = append(out, stream{"reboots and clones", c, storetest.QuarantinePolicy()})
	return out
}

// windows are five fixed windows over a stream: one that starts before it, one
// that ends after it, a short one off the second, and two inside.
func windows(start, end time.Time) [][2]time.Time {
	return [][2]time.Time{
		{start.Add(-10 * time.Minute), start.Add(4 * time.Minute)},
		{start.Add(3 * time.Minute), start.Add(9 * time.Minute)},
		{start.Add(7*time.Minute + time.Nanosecond), start.Add(7*time.Minute + 40*time.Second)},
		{start.Add(12 * time.Minute), end.Add(10 * time.Minute)},
		{start.Add(time.Minute - time.Nanosecond), end.Add(-time.Minute)},
	}
}

type openedStream struct {
	st          *memstore.Store
	recs        []store.Record
	entities    []identity.Fingerprint
	pods, nodes []identity.Fingerprint
	start, end  time.Time
}

func openStream(t *testing.T, s stream) openedStream {
	t.Helper()
	g, err := storetest.NewGenerator(s.cfg)
	if err != nil {
		t.Fatal(err)
	}
	o := openedStream{recs: g.All(), entities: g.Entities(), start: g.Start(), end: g.End()}
	o.st = openStore(t, memstore.Options{Policy: s.policy})
	for i := 0; i < len(o.recs); i += 64 {
		if err := o.st.Write(ctx, o.recs[i:min(i+64, len(o.recs))]); err != nil {
			t.Fatal(err)
		}
	}
	for _, e := range o.entities {
		switch e.Type() {
		case catalog.K8sPod:
			o.pods = append(o.pods, e)
		case catalog.K8sNode:
			o.nodes = append(o.nodes, e)
		}
	}
	return o
}

// touched is every subject a query of seed can read: its edges, the edges on
// and of its nodes, and the edges on the hosts of those nodes and their nodes.
func touched(o *oracle, seed identity.Fingerprint) []store.Subject {
	seen := map[store.Subject]bool{}
	var out []store.Subject
	add := func(es []store.Subject) {
		for _, e := range es {
			if !seen[e] {
				seen[e] = true
				out = append(out, e)
			}
		}
	}
	add(o.out[seed])
	nodes := map[identity.Fingerprint]bool{}
	for _, e := range o.out[seed] {
		nodes[e.B] = true
	}
	for n := range nodes {
		add(o.in[n])
		add(o.out[n])
		for _, r := range o.out[n] {
			add(o.in[r.B])
			for _, back := range o.in[r.B] {
				add(o.in[back.A])
				add(o.out[back.A])
			}
		}
	}
	return out
}

func TestStreams(t *testing.T) {
	var mu sync.Mutex
	unionOnly := 0
	t.Cleanup(func() {
		t.Logf("seed-peer pairs a union graph pairs on a node with no overlap there: %d", unionOnly)
		if unionOnly == 0 {
			t.Error("no stream has a pair a union graph pairs without an overlap: the streams do not cover the case")
		}
	})
	for _, s := range streams() {
		t.Run(s.name, func(t *testing.T) {
			t.Parallel()
			sm := openStream(t, s)
			last := sm.st.LastSeq()
			ws, toks := windows(sm.start, sm.end), []uint64{store.Latest, last / 2}
			if raceEnabled {
				// The window before the stream and the one past its end, at the
				// latest token; the plain build checks all five at both tokens.
				ws, toks = [][2]time.Time{ws[0], ws[3]}, toks[:1]
			}
			for _, fw := range ws {
				for _, tok := range toks {
					w := interval.Window{From: fw[0], To: fw[1], AsOf: tok}
					for _, seed := range sm.pods {
						checkOracle(t, sm.st, sm.recs, sm.entities, seed, w, coresidency.Options{Policy: wide()})
						if tok == store.Latest {
							checkOracle(t, sm.st, sm.recs, sm.entities, seed, w, coresidency.Options{NodeOnly: true, Policy: wide()})
						}
					}
				}
				w := interval.Window{From: fw[0], To: fw[1], AsOf: store.Latest}
				o := newOracle(sm.recs, last, w.From, w.To, nil)
				for _, seed := range sm.pods {
					checkPointReads(t, sm.st, seed, sm.nodes, w, coresidency.Options{Policy: wide()}, o.crit(touched(o, seed)...))
					n := unionGraphOnly(t, sm.st, seed, w)
					mu.Lock()
					unionOnly += n
					mu.Unlock()
				}
			}
		})
	}
}

// unionGraphOnly counts the peers that had a piece on one of the seed's nodes
// in the window while the seed had one too, but never at the same time.
func unionGraphOnly(t *testing.T, st store.Store, seed identity.Fingerprint, w interval.Window) int {
	t.Helper()
	res := mustFind(t, st, seed, w, coresidency.Options{NodeOnly: true, Policy: wide()})
	w.AsOf = res.Token
	met := map[pathKey]bool{}
	for _, c := range append(res.CoResidents, res.Suppressed...) {
		met[keyOf(c)] = true
	}
	r := interval.NewReader(st, interval.Options{})
	n := 0
	for _, el := range res.Placement {
		on, err := r.Edges(ctx, el.Subject.B, store.Reverse, w)
		if err != nil {
			t.Fatal(err)
		}
		for _, x := range on.Elements {
			if x.Subject.A == seed || x.Subject.Relation != catalog.ScheduledOn {
				continue
			}
			if !met[pathKey{coresidency.SharedNode, x.Subject.A, fmt.Sprint([]identity.Fingerprint{el.Subject.B}), el.Subject.B}] {
				n++
			}
		}
	}
	return n
}
