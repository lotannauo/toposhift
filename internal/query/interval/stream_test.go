package interval_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

// The generated cluster streams of storetest, with heartbeats, runs, a second
// producer on placements, late records, reboots and clones, checked at fixed
// windows against the oracle (P1), the store's point reads (P2) and the token
// prefix (P8).

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

// windows are five fixed windows over a stream: one that starts before it,
// one that ends after it, a short one off the second, and two inside.
func windows(start, end time.Time) [][2]time.Time {
	return [][2]time.Time{
		{start.Add(-10 * time.Minute), start.Add(4 * time.Minute)},
		{start.Add(3 * time.Minute), start.Add(9 * time.Minute)},
		{start.Add(7*time.Minute + time.Nanosecond), start.Add(7*time.Minute + 40*time.Second)},
		{start.Add(12 * time.Minute), end.Add(10 * time.Minute)},
		{start.Add(time.Minute - time.Nanosecond), end.Add(-time.Minute)},
	}
}

func openStream(t *testing.T, s stream) (*memstore.Store, []store.Record, []identity.Fingerprint, time.Time, time.Time) {
	t.Helper()
	g, err := storetest.NewGenerator(s.cfg)
	if err != nil {
		t.Fatal(err)
	}
	recs := g.All()
	st := openStore(t, memstore.Options{Policy: s.policy})
	for i := 0; i < len(recs); i += 64 {
		if err := st.Write(ctx, recs[i:min(i+64, len(recs))]); err != nil {
			t.Fatal(err)
		}
	}
	return st, recs, g.Entities(), g.Start(), g.End()
}

// critFor is every critical instant in [from, to) of an entity, the edges
// touching it, and their endpoints.
func critFor(o *oracle, from, to time.Time) func(identity.Fingerprint) []time.Time {
	return func(e identity.Fingerprint) []time.Time {
		lists := [][]store.Record{o.bySub[store.EntitySubject(e)]}
		for sub, recs := range o.bySub {
			if sub.Kind == store.SubjectEdge && (sub.A == e || sub.B == e) {
				lists = append(lists, recs, o.bySub[store.EntitySubject(sub.A)], o.bySub[store.EntitySubject(sub.B)])
			}
		}
		return critical(lists, from, to)
	}
}

func TestStreams(t *testing.T) {
	for _, s := range streams() {
		t.Run(s.name, func(t *testing.T) {
			t.Parallel()
			st, recs, entities, start, end := openStream(t, s)
			last := st.LastSeq()
			for i, fw := range windows(start, end) {
				for _, tok := range []uint64{store.Latest, last / 2} {
					w := interval.Window{From: fw[0], To: fw[1], AsOf: tok}
					checkOracle(t, st, recs, entities, w)
				}
				w := interval.Window{From: fw[0], To: fw[1], AsOf: store.Latest}
				checkPointReads(t, st, entities, w, critFor(newOracle(recs, last), w.From, w.To))
				w.AsOf = last * uint64(i+1) / 6
				checkPrefix(t, st, memstore.Options{Policy: s.policy}, recs, entities, w)
			}
		})
	}
}

// TestStreamsQuarantine makes sure the stream that clones hosts does
// quarantine one, so the checks above meet a quarantine.
func TestStreamsQuarantine(t *testing.T) {
	s := streams()[4]
	st, _, entities, start, end := openStream(t, s)
	r := interval.NewReader(st, interval.Options{})
	var q, unc int
	for _, e := range entities {
		res, err := r.Entity(ctx, e, interval.Window{From: start, To: end, AsOf: store.Latest})
		if err != nil {
			t.Fatal(err)
		}
		if len(res.Elements) != 1 {
			t.Fatalf("Entity(%s) has %d elements", e, len(res.Elements))
		}
		if res.Elements[0].Quarantine != nil {
			q++
		}
		for _, d := range dirs {
			res, err := r.Edges(ctx, e, d, interval.Window{From: start, To: end, AsOf: store.Latest})
			if err != nil {
				t.Fatal(err)
			}
			for _, el := range res.Elements {
				if el.Uncascaded != nil {
					unc++
				}
			}
		}
	}
	if q == 0 || unc == 0 {
		t.Fatalf("%d quarantined entities and %d uncascaded edges; want some of each", q, unc)
	}
}
