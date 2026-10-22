package pgengine_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/lotannauo/toposhift/bench/postgres/pgengine"
	"github.com/lotannauo/toposhift/bench/postgres/pgschema"
	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/engine"
)

// assertEveryIndexFindsEveryRow asks each GiST index for each key its table holds, and
// compares what the indexes find with what a scan of the table finds. A live row that
// an index cannot find is a wrong answer to every read that goes through the index.
func assertEveryIndexFindsEveryRow(t *testing.T, conn *pgx.Conn, when string) {
	t.Helper()
	ctx := context.Background()
	for _, c := range []struct{ table, key string }{{"edges", "src"}, {"edges", "dst"}, {"entities", "fp"}} {
		var viaIndex, viaTable int
		if _, err := conn.Exec(ctx, "SET enable_seqscan = off; SET enable_bitmapscan = off; SET enable_indexscan = on"); err != nil {
			t.Fatal(err)
		}
		// One scan of the index per key, as a nested loop makes them.
		if err := conn.QueryRow(ctx, "SELECT coalesce(sum(n), 0) FROM (SELECT (SELECT count(*) FROM "+c.table+" e WHERE e."+c.key+" = d."+c.key+") n FROM (SELECT DISTINCT "+c.key+" FROM "+c.table+") d) x").Scan(&viaIndex); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Exec(ctx, "SET enable_seqscan = on; SET enable_bitmapscan = off; SET enable_indexscan = off"); err != nil {
			t.Fatal(err)
		}
		if err := conn.QueryRow(ctx, "SELECT count(*) FROM "+c.table).Scan(&viaTable); err != nil {
			t.Fatal(err)
		}
		if viaIndex != viaTable {
			t.Fatalf("%s: the index on %s.%s finds %d of the %d rows", when, c.table, c.key, viaIndex, viaTable)
		}
	}
	if _, err := conn.Exec(ctx, "RESET enable_seqscan; RESET enable_bitmapscan; RESET enable_indexscan"); err != nil {
		t.Fatal(err)
	}
}

// TestWritesAreNeverPlannedThroughAGiSTIndex: an UPDATE that finds its rows through a
// GiST index while it puts the new versions of them into the same index left the
// entries of the new versions marked dead on the server this baseline is written for, in
// about one run of the workload in five hundred, and every read through the index then
// missed those rows. The planner chose that plan for a key-matching update once the
// table had grown to a few pages, and the same workload at the same step every time. So
// every statement that changes rows is asked how it would be planned, at every step of
// a stream that grows the tables well past that size, and none may use an index: the
// rows are found by ctid (a TID scan, or on a table of a page or two a sequential scan).
// And the indexes must find every row they hold.
func TestWritesAreNeverPlannedThroughAGiSTIndex(t *testing.T) {
	conn := func(e *pgengine.Engine) *pgx.Conn { return sqlConn(t, e) }
	for _, arm := range []pgschema.Arm{pgschema.Stream, pgschema.StreamNoPartition, pgschema.StreamNaive, pgschema.Bulk, pgschema.BulkB} {
		t.Run(string(arm), func(t *testing.T) {
			e := openEngine(t, arm)
			var seen []string
			stop := pgengine.ObserveStatements(func(ctx context.Context, tx pgx.Tx, sql string, args []any) {
				if !strings.HasPrefix(sql, "UPDATE edges") && !strings.HasPrefix(sql, "UPDATE entities") && !strings.HasPrefix(sql, "DELETE") {
					return
				}
				var raw []byte
				if err := tx.QueryRow(ctx, "EXPLAIN (FORMAT JSON) "+sql, args...).Scan(&raw); err != nil {
					t.Errorf("explaining %.60s: %v", sql, err)
					return
				}
				plan, err := pgengine.ParseExplain(raw)
				if err != nil {
					t.Errorf("%v", err)
					return
				}
				// The rows are named by ctid, so no index can answer the statement: a plan with an
				// index in it found them by key, through the index the planner preferred.
				if i := slices.IndexFunc(plan.Plan, func(n pgengine.PlanNode) bool { return n.Index != "" }); i >= 0 {
					t.Errorf("%.40s... is planned through the index %s: %s", strings.Join(strings.Fields(sql), " "), plan.Plan[i].Index, plan.Shape())
				}
				seen = append(seen, plan.Shape())
			})
			defer stop()
			c := conn(e)
			batches := 0
			cfg := conformance.Configs()[1]
			g := newWorkload(t, cfg)
			for batches < 45 {
				recs := g.Batch(30)
				if len(recs) == 0 {
					break
				}
				if err := e.Write(recs); err != nil {
					t.Fatal(err)
				}
				batches++
				if batches%15 == 0 {
					assertEveryIndexFindsEveryRow(t, c, fmt.Sprintf("after %d batches", batches))
				}
			}
			if len(seen) < 40 {
				t.Errorf("only %d statements that change rows were planned: %v", len(seen), seen)
			}
		})
	}
}

// TestEveryIndexFindsEveryRowAfterAStreamWithRetentions: after each stream of the
// conformance workloads, whatever the late events and retentions did to the tables, the
// indexes find every row.
func TestEveryIndexFindsEveryRowAfterAStreamWithRetentions(t *testing.T) {
	configs := conformance.Configs()
	for _, c := range []struct {
		config int
		retain []float64
	}{{1, []float64{0.5}}, {3, []float64{0.3, 0.7}}, {6, nil}} {
		for _, arm := range []pgschema.Arm{pgschema.Stream, pgschema.StreamNaive, pgschema.Bulk} {
			t.Run(fmt.Sprintf("%s config %d retain %v", arm, c.config, c.retain), func(t *testing.T) {
				e := openEngine(t, arm)
				conn := sqlConn(t, e)
				batches := 0
				stream(t, configs[c.config], 40, c.retain, []engine.Engine{e}, func(written []engine.Record, horizon time.Time) {
					batches++
					if batches%10 == 0 {
						assertEveryIndexFindsEveryRow(t, conn, fmt.Sprintf("after %d records", len(written)))
					}
				})
				assertEveryIndexFindsEveryRow(t, conn, "at the end")
			})
		}
	}
}
