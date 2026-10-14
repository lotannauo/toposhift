package blastradius_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/blastradius"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

// The generated cluster streams of storetest, with heartbeats, runs, a second
// producer on placements, late records, reboots and clones, checked at fixed
// windows against the oracle and its read formula (R1, R11) and the store's
// point reads (R2), with every rack, host and node as a seed; and a fixed set
// of grid stores. Both count the members a union-graph walk would add.

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
	st         *memstore.Store
	recs       []store.Record
	seeds      []identity.Fingerprint
	start, end time.Time
}

func openStream(t *testing.T, s stream) openedStream {
	t.Helper()
	g, err := storetest.NewGenerator(s.cfg)
	if err != nil {
		t.Fatal(err)
	}
	o := openedStream{recs: g.All(), start: g.Start(), end: g.End()}
	o.st = openStore(t, memstore.Options{Policy: s.policy})
	for i := 0; i < len(o.recs); i += 64 {
		if err := o.st.Write(ctx, o.recs[i:min(i+64, len(o.recs))]); err != nil {
			t.Fatal(err)
		}
	}
	for _, e := range g.Entities() {
		switch e.Type() {
		case catalog.Rack, catalog.Host, catalog.K8sNode:
			o.seeds = append(o.seeds, e)
		}
	}
	return o
}

// sample is at most n of the instants, evenly spread, the first included.
func sample(xs []time.Time, n int) []time.Time {
	if len(xs) <= n {
		return xs
	}
	out := make([]time.Time, 0, n)
	for i := range n {
		out = append(out, xs[i*len(xs)/n])
	}
	return out
}

// unionOnly counts the members a union-graph walk from seed adds that the walk
// does not have: entities no path reaches at any one instant.
func unionOnly(t tb, st store.Store, or *oracle, seed identity.Fingerprint, w interval.Window, depth int) int {
	t.Helper()
	res := mustImpact(t, st, seed, w, blastradius.Options{Policy: wide(depth)})
	got := map[identity.Fingerprint]bool{}
	for _, m := range res.Members {
		got[m.Entity] = true
	}
	n := 0
	for fp := range or.unionWalk(seed, depth) {
		if !got[fp] {
			n++
		}
	}
	return n
}

func TestStreams(t *testing.T) {
	var mu sync.Mutex
	extra := 0
	t.Cleanup(func() {
		t.Logf("(seed, member) pairs a union-graph walk adds with no reach, over the streams: %d", extra)
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
					or, pinned := oracleFor(t, sm.st, sm.recs, w)
					for _, seed := range sm.seeds {
						checkOracle(t, sm.st, or, pinned, seed, w, 6)
					}
					if tok != store.Latest {
						continue
					}
					checkPointReads(t, sm.st, sm.seeds, w, 6, sample(or.crit, 60))
					n := 0
					for _, seed := range sm.seeds {
						n += unionOnly(t, sm.st, or, seed, w, 6)
					}
					mu.Lock()
					extra += n
					mu.Unlock()
				}
			}
		})
	}
}

// fixedGrid is a grid store drawn by the property generator from a fixed
// seed, so that it is the same on every run.
type fixedGrid struct {
	recs     []store.Record
	from, to time.Time
	depth    int
}

var fixedGrids = rapid.Custom(func(t *rapid.T) fixedGrid {
	recs := drawRecords(t, newGrid(), 70)
	from, to := drawWindow(t)
	return fixedGrid{recs: recs, from: from, to: to, depth: uniform(t, "depth", 2, 6)}
})

// TestFixedGrids checks R1, R11 and R2 on twenty grid stores from fixed seeds,
// and requires that a union-graph walk adds a member with no reach to at least
// one of them: otherwise the data does not cover the case the walk exists for.
func TestFixedGrids(t *testing.T) {
	total := 0
	for k := 1; k <= 20; k++ {
		fg := fixedGrids.Example(k)
		st := openStore(t, memstore.Options{})
		recs := newFeed(t, st).write(fg.recs...)
		w := interval.Window{From: fg.from, To: fg.to, AsOf: store.Latest}
		or, tok := oracleFor(t, st, recs, w)
		g := newGrid()
		for _, seed := range g.all {
			checkOracle(t, st, or, tok, seed, w, fg.depth)
			total += unionOnly(t, st, or, seed, w, fg.depth)
		}
		checkPointReads(t, st, g.all, w, fg.depth, or.crit)
	}
	t.Logf("(seed, member) pairs a union-graph walk adds with no reach, over the fixed grids: %d", total)
	if total == 0 {
		t.Error("no fixed grid has a member a union-graph walk adds without a reach: the data does not cover the case")
	}
}
