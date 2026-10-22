package pgengine_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/postgres/pgengine"
	"github.com/lotannauo/toposhift/bench/postgres/pgmodel"
	"github.com/lotannauo/toposhift/bench/postgres/pgschema"
	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/workload"
)

// stream feeds a workload, in batches and with a retention at each given fraction of
// the period, to every engine, and calls check after each batch with the records
// written so far and the horizon. A record before the horizon is dropped, as a
// store's driver drops it.
func stream(t *testing.T, cfg workload.Config, batch int, retainAt []float64, engines []engine.Engine, check func(written []engine.Record, horizon time.Time)) {
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
			return
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
		for _, e := range engines {
			cp := make([]engine.Record, len(kept))
			copy(cp, kept)
			if err := e.Write(cp); err != nil {
				t.Fatalf("%T: %v", e, err)
			}
		}
		written = append(written, kept...)
		if next < len(retainAt) && !kept[len(kept)-1].EventTime.Before(g.Start().Add(time.Duration(retainAt[next]*float64(span)))) {
			horizon = g.Start().Add(time.Duration(retainAt[next] * float64(span)))
			next++
			for _, e := range engines {
				if err := e.Retain(horizon); err != nil {
					t.Fatalf("%T: %v", e, err)
				}
			}
		}
		check(written, horizon)
	}
}

func nanos(horizon time.Time) int64 {
	if horizon.IsZero() {
		return 0
	}
	return pgmodel.ToNanos(horizon)
}

// TestTablesAreTheModelsAfterEveryBatch holds the SQL engine to the in-memory model:
// fed the same stream, with late events, deletes, outages and retentions, the two
// make the same rows, and count the same row changes and vacuums, after every batch.
// The rows are compared as they are stored, provisional rows included and not clipped,
// because both are made by one planner.
func TestTablesAreTheModelsAfterEveryBatch(t *testing.T) {
	configs := conformance.Configs()
	for _, c := range []struct {
		config int
		retain []float64
	}{{1, nil}, {3, []float64{0.5}}, {5, []float64{0.3, 0.7}}, {6, []float64{0.4}}} {
		for _, arm := range streamArms {
			t.Run(fmt.Sprintf("%s config %d retain %v", arm, c.config, c.retain), func(t *testing.T) {
				sql, model := openEngine(t, arm), pgmodel.NewWith(pgengine.ModelFor(arm))
				batches := 0
				stream(t, configs[c.config], 40, c.retain, []engine.Engine{sql, model}, func(written []engine.Record, horizon time.Time) {
					batches++
					if batches%3 != 0 {
						return
					}
					got, err := sql.Tables(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					// The rows are the model's in the same form: a row is provisional in
					// one if it is in the other.
					if want := model.Tables(); !got.Identical(want) {
						t.Fatalf("after %d records: the SQL rows differ from the model's: %s", len(written), got.FirstDifference(want))
					}
					if got, want := sql.Counts(), model.Counts(); got != want {
						t.Fatalf("after %d records: the SQL engine counts %+v, the model %+v", len(written), got, want)
					}
					if got, want := sql.VacuumRuns(), model.VacuumRuns(); !equalRuns(got, want) {
						t.Fatalf("after %d records: the SQL engine vacuumed %v, the model's schedule %v", len(written), got, want)
					}
					if got, want := sql.AnalyzeRuns(), model.AnalyzeRuns(); !equalRuns(got, want) {
						t.Fatalf("after %d records: the SQL engine analyzed %v, the model's schedule %v", len(written), got, want)
					}
					// And, clipped to the horizon, they are the rows of a bulk load of
					// everything written: a stream's rows are a load's, whatever the batches.
					bulk, err := pgmodel.Bulk(written)
					if err != nil {
						t.Fatal(err)
					}
					h := nanos(horizon)
					if g, w := got.Clipped(h), bulk.Clipped(h); !g.Equal(w) {
						t.Fatalf("after %d records, horizon %s: the stream's rows are not a bulk load's: %s", len(written), horizon.Format(time.RFC3339), g.FirstDifference(w))
					}
				})
			})
		}
	}
}

// TestRebuiltTablesAreABulkLoadsAfterEveryBatch: the bulk arms fold each subject whole
// at every write, so after any stream, clipped to the horizon, their rows are the rows
// of a bulk load of the same records.
func TestRebuiltTablesAreABulkLoadsAfterEveryBatch(t *testing.T) {
	configs := conformance.Configs()
	for _, arm := range []pgschema.Arm{pgschema.Bulk, pgschema.BulkB} {
		for _, c := range []struct {
			config int
			retain []float64
		}{{1, nil}, {5, []float64{0.3, 0.7}}} {
			t.Run(fmt.Sprintf("%s config %d retain %v", arm, c.config, c.retain), func(t *testing.T) {
				sql := openEngine(t, arm)
				batches := 0
				stream(t, configs[c.config], 60, c.retain, []engine.Engine{sql}, func(written []engine.Record, horizon time.Time) {
					batches++
					if batches%4 != 0 {
						return
					}
					bulk, err := pgmodel.Bulk(written)
					if err != nil {
						t.Fatal(err)
					}
					got, err := sql.Tables(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					h := nanos(horizon)
					if g, w := got.Clipped(h), bulk.Clipped(h); !g.Equal(w) {
						t.Fatalf("after %d records, horizon %s: %s", len(written), horizon.Format(time.RFC3339), g.FirstDifference(w))
					}
				})
			})
		}
	}
}

// TestTheOtherArmsConform runs the part of the contract an arm's configuration
// changes: the fixed workloads with a retention, and the scripted checks. The primary
// arm has the whole of it in TestConformance.
func TestTheOtherArmsConform(t *testing.T) {
	for _, arm := range []pgschema.Arm{pgschema.StreamNoPartition, pgschema.StreamNaive, pgschema.Bulk, pgschema.BulkB} {
		t.Run(string(arm), func(t *testing.T) {
			conformance.RunPartial(t, factory(t, arm))
		})
	}
}
