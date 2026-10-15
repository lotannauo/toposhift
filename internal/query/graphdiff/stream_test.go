package graphdiff_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/graphdiff"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

// The generated cluster streams of storetest, with heartbeats, runs, a second
// producer on placements, late records, reboots and clones, diffed with every
// node as the scope at five fixed pairs of instants, against the oracle and its
// read formula (G1, G10), also with every list capped at one entry, and the
// store's point reads (G2).

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

// instants are five fixed pairs (t1, t2) over a stream: one that starts before
// it, one inside its start, a short one off the second, one that ends after
// it, and a long one inside.
func instants(start, end time.Time) [][2]time.Time {
	return [][2]time.Time{
		{start.Add(-10 * time.Minute), start.Add(4 * time.Minute)},
		{start.Add(3 * time.Minute), start.Add(9 * time.Minute)},
		{start.Add(7*time.Minute + time.Nanosecond), start.Add(7*time.Minute + 40*time.Second)},
		{start.Add(12 * time.Minute), end.Add(10 * time.Minute)},
		{start.Add(time.Minute - time.Nanosecond), end.Add(-time.Minute)},
	}
}

func TestStreams(t *testing.T) {
	var mu sync.Mutex
	seen := map[string]int{}
	t.Cleanup(func() {
		t.Logf("what the stream diffs held: %v", seen)
		for _, src := range []lifecycle.EndSource{lifecycle.EndLivenessExpiry, lifecycle.EndCascade} {
			if seen["deleted by "+src.String()] == 0 {
				t.Errorf("no stream diff has a deletion by %s: the streams do not cover it", src)
			}
		}
	})
	for _, s := range streams() {
		t.Run(s.name, func(t *testing.T) {
			t.Parallel()
			g, err := storetest.NewGenerator(s.cfg)
			if err != nil {
				t.Fatal(err)
			}
			recs, entities := g.All(), g.Entities()
			st := openStore(t, memstore.Options{Policy: s.policy})
			for i := 0; i < len(recs); i += 64 {
				if err := st.Write(ctx, recs[i:min(i+64, len(recs))]); err != nil {
					t.Fatal(err)
				}
			}
			var nodes []identity.Fingerprint
			for _, e := range entities {
				if e.Type() == catalog.K8sNode {
					nodes = append(nodes, e)
				}
			}
			pairs := instants(g.Start(), g.End())
			local := map[string]int{}
			for _, p := range pairs {
				for _, n := range nodes {
					sc := []identity.Fingerprint{n}
					sp := &spy{Store: st}
					res := checkOracle(t, sp, recs, entities, sc, p[0], p[1], store.Latest, graphdiff.Options{Policy: wide()})
					if sp.writes != 0 {
						t.Fatalf("%d writes or retentions through the differ's store", sp.writes)
					}
					checkOracle(t, st, recs, entities, sc, p[0], p[1], store.Latest, graphdiff.Options{Policy: graphdiff.Policy{MaxScope: 1, MaxListed: 1}})
					checkPointReads(t, st, sc, p[0], p[1], store.Latest, nil)
					tallyCoverage(local, res)
				}
			}
			mu.Lock()
			for k, v := range local {
				seen[k] += v
			}
			mu.Unlock()
		})
	}
}
