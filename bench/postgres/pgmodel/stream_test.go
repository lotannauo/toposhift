package pgmodel_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/postgres/pgmodel"
	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/workload"
)

// streamInto writes a workload to a model in batches, moving the horizon at the given
// fractions of the period, and after each batch calls check with the records written
// so far (those a store would accept: a record before the horizon is dropped, as the
// runner's Drive drops it) and the horizon.
func streamInto(t *testing.T, m *pgmodel.Model, cfg workload.Config, batch int, retainAt []float64, check func(written []engine.Record, horizon time.Time)) {
	t.Helper()
	g, err := workload.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var (
		written []engine.Record
		horizon time.Time
		next    int
	)
	span := g.End().Sub(g.Start())
	for {
		recs := g.Batch(batch)
		if len(recs) == 0 {
			break
		}
		kept := recs[:0]
		for _, r := range recs {
			if !r.EventTime.Before(horizon) {
				kept = append(kept, r)
			}
		}
		if len(kept) == 0 {
			continue
		}
		if err := m.Write(kept); err != nil {
			t.Fatal(err)
		}
		written = append(written, kept...)
		if next < len(retainAt) && !kept[len(kept)-1].EventTime.Before(g.Start().Add(time.Duration(retainAt[next]*float64(span)))) {
			horizon = g.Start().Add(time.Duration(retainAt[next] * float64(span)))
			next++
			if err := m.Retain(horizon); err != nil {
				t.Fatal(err)
			}
		}
		check(written, horizon)
	}
}

// TestStreamEqualsBulk is the planner's test: whatever the order the records arrive in
// (late events, run extensions, deletes, outages) and wherever the horizon moves, the
// rows a model has written one batch at a time are, clipped to the horizon, the rows
// a bulk load of the same records makes.
func TestStreamEqualsBulk(t *testing.T) {
	for i, cfg := range conformance.Configs() {
		for _, retain := range [][]float64{nil, {0.5}, {0.3, 0.7}} {
			t.Run(fmt.Sprintf("config %d retain %v", i, retain), func(t *testing.T) {
				m := pgmodel.New()
				batches := 0
				streamInto(t, m, cfg, 50, retain, func(written []engine.Record, horizon time.Time) {
					batches++
					if batches%7 != 0 {
						return
					}
					bulk, err := pgmodel.Bulk(written)
					if err != nil {
						t.Fatal(err)
					}
					h := pgmodel.ToNanos(horizon)
					if horizon.IsZero() {
						h = 0
					}
					got, want := m.Tables().Clipped(h), bulk.Clipped(h)
					if !got.Equal(want) {
						t.Fatalf("after %d records, horizon %s: stream and bulk differ: %s", len(written), horizon.Format(time.RFC3339), got.FirstDifference(want))
					}
				})
			})
		}
	}
}

func configForCounts() workload.Config { return conformance.Configs()[1] }
