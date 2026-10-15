package svcgraph_test

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/ingest/otlpmetrics"
	"github.com/lotannauo/toposhift/internal/ingest/svcgraph"
)

// A script is what a collector sends one translator over a stretch of time, and what
// happens to each request: a connector (or two) that is called at random, its
// cleanup, its restarts, requests that fail and are sent again, duplicates, points
// from a clock that is ahead, and translators that are made anew.

type step struct {
	renew    bool // the translator is made anew before the request
	points   []otlpmetrics.SumPoint
	received time.Time
	commit   bool // the group is answered with success; otherwise it fails
	// edges and starts are the oracle's view of each point.
	pts []oPoint
}

type script struct {
	seed      uint64
	maxSeries int
	steps     []step
}

// edgePool is the edges a script draws from; each is a series with failed false and
// one with failed true.
func edgePool() []edgeID {
	var out []edgeID
	for _, c := range [][2]string{{"a", "b"}, {"a", "c"}, {"b", "c"}, {"c", "d"}} {
		for _, f := range []bool{false, true} {
			e := edge(c[0], c[1])
			e.failed = f
			out = append(out, e)
		}
	}
	return out
}

func genScript(seed uint64) script {
	r := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	sc := script{seed: seed}
	sc.maxSeries = []int{2, 3, 5, svcgraph.DefaultMaxSeries}[r.IntN(4)]
	pool := edgePool()
	n := 1 + r.IntN(len(pool))
	r.Shuffle(len(pool), func(i, j int) { pool[i], pool[j] = pool[j], pool[i] })
	pool = pool[:n]
	rates := make([]float64, n)
	for i := range rates {
		rates[i] = []float64{0.02, 0.08, 0.3, 0.8}[r.IntN(4)]
	}
	conns := []*conn{newConn(connStart)}
	if r.IntN(4) == 0 {
		// A second connector behind the producer, started earlier or later.
		other := connStart.Add(time.Duration(r.IntN(7)-3) * 10 * time.Minute)
		if other.Equal(connStart) {
			other = other.Add(time.Minute)
		}
		conns = append(conns, newConn(other))
	}
	flushes := 20 + r.IntN(100)
	for k := 0; k < flushes; k++ {
		tm := at(k).Add(time.Duration(r.IntN(400)) * time.Millisecond)
		type request struct {
			pts  []otlpmetrics.SumPoint
			meta []oPoint // conn and lastCall of each point
		}
		var reqs []request
		var joined request
		for ci, c := range conns {
			if r.IntN(60) == 0 {
				c.restart(tm.Add(-time.Duration(1+r.IntN(5)) * time.Minute))
			}
			for i, e := range pool {
				if r.Float64() < rates[i] {
					c.call(e, 1+r.IntN(3), tm.Add(-time.Second))
				}
			}
			pts := c.flush(tm, time.Duration(ci)*500*time.Millisecond)
			meta := make([]oPoint, len(pts))
			for j, p := range pts {
				// The simulator knows when the series was last called.
				meta[j] = oPoint{conn: ci, lastCall: c.series[edgeOf(p)].updated}
			}
			joined.pts = append(joined.pts, pts...)
			joined.meta = append(joined.meta, meta...)
			reqs = append(reqs, request{pts, meta})
		}
		if len(conns) > 1 && r.IntN(2) == 0 {
			reqs = []request{joined}
		}
		for _, rq := range reqs {
			pts := slices.Clone(rq.pts)
			if len(pts) > 0 && r.IntN(40) == 0 {
				// A clock that is ahead.
				pts[r.IntN(len(pts))].TimeUnixNano = uint64(tm.Add(6 * time.Minute).UnixNano())
			}
			s := step{renew: r.IntN(50) == 0, points: pts, received: tm.Add(time.Second), commit: true}
			for _, p := range pts {
				m := rq.meta[len(s.pts)]
				s.pts = append(s.pts, oPoint{
					e:     edgeOf(p),
					start: time.Unix(0, int64(p.StartTimeUnixNano)).UTC(),
					at:    time.Unix(0, int64(p.TimeUnixNano)).UTC(),
					value: p.Value,
					conn:  m.conn, lastCall: m.lastCall,
				})
			}
			switch x := r.IntN(100); {
			case x < 8:
				// Fails, and the collector sends it again.
				f := s
				f.commit = false
				sc.steps = append(sc.steps, f)
				s.renew = false
			case x < 12:
				// Fails and is never sent again.
				s.commit = false
			case x < 20:
				// Succeeds, and is sent again.
				sc.steps = append(sc.steps, s)
				s.renew = false
			}
			sc.steps = append(sc.steps, s)
		}
	}
	return sc
}

func edgeOf(p otlpmetrics.SumPoint) edgeID {
	e := edgeID{client: attrOf(p, "client"), server: attrOf(p, "server"), failed: attrOf(p, "failed") == "true"}
	return e
}

// playScript runs a script through a translator and the oracle, and returns the first
// difference.
func playScript(t tb, sc script) error {
	tr := newTranslator(t, maxSeries(sc.maxSeries))
	or := newOracle(svcgraph.DefaultFlush, svcgraph.DefaultCacheLoop, sc.maxSeries)
	fps := map[string]identity.Fingerprint{}
	svc := func(name string) identity.Fingerprint {
		if f, ok := fps[name]; ok {
			return f
		}
		f := serviceFP(t, "", name)
		fps[name] = f
		return f
	}
	for i, s := range sc.steps {
		if s.renew {
			tr = newTranslator(t, maxSeries(sc.maxSeries))
			or = newOracle(svcgraph.DefaultFlush, svcgraph.DefaultCacheLoop, sc.maxSeries)
		}
		res := tr.Translate(resource(s.points...), s.received)
		var want []call
		wantSkips := map[string]int{}
		for _, p := range s.pts {
			v := or.point(p, s.received)
			switch v {
			case vSighting:
				want = append(want, call{svc(p.e.client), svc(p.e.server), p.at})
			case vNone:
			default:
				wantSkips[string(v)]++
			}
		}
		got := calls(res.Records)
		if len(got) != len(want) {
			return fmt.Errorf("step %d (commit %v): %d sightings, the rule says %d", i, s.commit, len(got), len(want))
		}
		for j := range got {
			if got[j] != want[j] {
				return fmt.Errorf("step %d: sighting %d is %+v, the rule says %+v", i, j, got[j], want[j])
			}
		}
		for _, reason := range []string{svcgraph.SkipWarmUp, svcgraph.SkipStale, svcgraph.SkipSecondConnector, svcgraph.SkipSeriesOverflow, otlpmetrics.SkipClockAhead} {
			if g := skipped(res, reason); g != wantSkips[reason] {
				return fmt.Errorf("step %d: %d points counted %q, the rule says %d", i, g, reason, wantSkips[reason])
			}
		}
		if tr.Series() != len(or.cur.series) {
			return fmt.Errorf("step %d: %d series, the rule keeps %d", i, tr.Series(), len(or.cur.series))
		}
		if s.commit {
			tr.Commit()
			or.commit()
		} else {
			tr.Rollback()
			or.rollback()
		}
		if tr.Series() != len(or.cur.series) {
			return fmt.Errorf("step %d (after the group): %d series, the rule keeps %d", i, tr.Series(), len(or.cur.series))
		}
	}
	return nil
}

// The translator gives the sightings the rule gives, for scripts drawn at random: idle
// series removed and made again with the same start time, at equal and lower values;
// restarts; translators made anew; failed groups sent again; duplicates; the table
// full; two connectors; clocks that are ahead.
func TestTheTranslatorMatchesTheRule(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		seed := rapid.Uint64().Draw(rt, "seed")
		if err := playScript(rt, genScript(seed)); err != nil {
			rt.Fatalf("seed %d: %v", seed, err)
		}
	})
}

// The same, over seeds fixed in this file, so that the claim that these scripts were
// covered does not depend on a random draw.
func TestTheTranslatorMatchesTheRuleOnFixedSeeds(t *testing.T) {
	t.Parallel()
	events := 0
	for seed := uint64(1); seed <= 400; seed++ {
		sc := genScript(seed)
		events += len(sc.steps)
		if err := playScript(t, sc); err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
	}
	if events < 10000 {
		t.Errorf("only %d requests were played", events)
	}
}

// The scripts reach what the property is meant to reach. A property that the rule
// agrees with itself on is worth nothing, so this counts, over the fixed seeds, the
// cases that the oracle answers.
func TestTheScriptsReachEveryCase(t *testing.T) {
	t.Parallel()
	seen := map[verdict]int{}
	failedThenSent, renewed, twoConnectors := 0, 0, 0
	for seed := uint64(1); seed <= 400; seed++ {
		sc := genScript(seed)
		or := newOracle(svcgraph.DefaultFlush, svcgraph.DefaultCacheLoop, sc.maxSeries)
		for i, s := range sc.steps {
			if s.renew {
				or = newOracle(svcgraph.DefaultFlush, svcgraph.DefaultCacheLoop, sc.maxSeries)
				renewed++
			}
			if !s.commit && i+1 < len(sc.steps) && sc.steps[i+1].commit && len(sc.steps[i+1].points) == len(s.points) {
				failedThenSent++
			}
			for _, p := range s.pts {
				seen[or.point(p, s.received)]++
			}
			if s.commit {
				or.commit()
			} else {
				or.rollback()
			}
		}
		if len(sc.steps) > 0 && sc.steps[0].points != nil {
			starts := map[time.Time]bool{}
			for _, p := range sc.steps[0].pts {
				starts[p.start] = true
			}
			if len(starts) > 1 {
				twoConnectors++
			}
		}
	}
	for _, v := range []verdict{vSighting, vNone, vWarmUp, vStale, vSecond, vOverflow, vClockAhead} {
		if seen[v] < 20 {
			t.Errorf("verdict %q was reached %d times in 400 scripts", v, seen[v])
		}
	}
	if failedThenSent < 50 || renewed < 20 {
		t.Errorf("%d failed groups sent again, %d translators made anew", failedThenSent, renewed)
	}
	t.Logf("verdicts over 400 scripts: %v; failed then sent again %d, renewed %d", seen, failedThenSent, renewed)
}

// The oracle repeats the rule, so agreeing with it shows the translator implements the
// rule and not that the rule is right. This test holds the rule to what the connector
// simulator knows, which the rule does not: when each series was last really called.
// Over the fixed seeds, every sighting the translator writes must be backed by a call that was made after the
// series' last increase (the last sighting, or the point that made its state), or, for
// a series with no state, within the last three flushes. A call the rule misses when the
// count is unchanged by a re-creation is sighted late, by case 5, and is still backed. The one allowed exception is the first point of a series
// after a switch from one connector to another behind the producer, which the rule
// takes for a restart and sights once.
func TestEverySightingIsBackedByARealCall(t *testing.T) {
	t.Parallel()
	sightings, switches, unstated := 0, 0, 0
	for seed := uint64(1); seed <= 400; seed++ {
		sc := genScript(seed)
		tr := newTranslator(t, maxSeries(sc.maxSeries))
		or := newOracle(svcgraph.DefaultFlush, svcgraph.DefaultCacheLoop, sc.maxSeries)
		// The connector that made the latest point absorbed for each series, kept past the
		// state's eviction, in the way the state is: pending until the group commits.
		known, pending := map[edgeID]int{}, map[edgeID]int{}
		for i, s := range sc.steps {
			if s.renew {
				tr = newTranslator(t, maxSeries(sc.maxSeries))
				or = newOracle(svcgraph.DefaultFlush, svcgraph.DefaultCacheLoop, sc.maxSeries)
				known, pending = map[edgeID]int{}, map[edgeID]int{}
			}
			res := tr.Translate(resource(s.points...), s.received)
			got := map[time.Time]bool{}
			for _, c := range calls(res.Records) {
				got[c.at] = true
			}
			for _, p := range s.pts {
				prev, had := or.cur.series[p.e]
				before, seen := pending[p.e]
				if !seen {
					before, seen = known[p.e]
				}
				v := or.point(p, s.received)
				switch v {
				case vStale, vSecond, vClockAhead, vOverflow:
				default:
					pending[p.e] = p.conn
				}
				// What is held to the connector's calls is what the translator wrote.
				if !got[p.at] {
					continue
				}
				sightings++
				live := had && p.at.Sub(prev.last) <= or.evict
				switch {
				case seen && p.conn != before:
					switches++ // the allowed exception
				case live:
					if !p.lastCall.After(prev.increase) {
						t.Fatalf("seed %d step %d: a sighting of %s at %s, and no call since its last increase at %s (last call %s)",
							seed, i, p.e, p.at, prev.increase, p.lastCall)
					}
				default:
					unstated++
					if p.at.Sub(p.lastCall) > 3*flush {
						t.Fatalf("seed %d step %d: a sighting of %s at %s with no state, and its last call was at %s",
							seed, i, p.e, p.at, p.lastCall)
					}
				}
			}
			if s.commit {
				tr.Commit()
				or.commit()
				for e, c := range pending {
					known[e] = c
				}
			} else {
				tr.Rollback()
				or.rollback()
			}
			pending = map[edgeID]int{}
		}
	}
	if sightings < 10000 || unstated < 200 || switches < 20 {
		t.Errorf("%d sightings, %d without state, %d across connectors: the check meant to reach more", sightings, unstated, switches)
	}
	t.Logf("%d sightings checked: %d without state, %d across connectors", sightings, unstated, switches)
}
