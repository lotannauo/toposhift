package pgengine_test

import (
	"context"
	"errors"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/lotannauo/toposhift/bench/postgres/pgengine"
	"github.com/lotannauo/toposhift/bench/postgres/pgmodel"
	"github.com/lotannauo/toposhift/bench/postgres/pgschema"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/workload"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
)

// loaded is an engine of an arm that holds some of a workload, and a pod, a node and
// an instant that have records.
type loaded struct {
	e         *pgengine.Engine
	pod, node identity.Fingerprint
	ns        int64
}

func load(t *testing.T, arm pgschema.Arm) loaded {
	t.Helper()
	e := openEngine(t, arm)
	g, err := workload.New(workload.Tiny())
	if err != nil {
		t.Fatal(err)
	}
	batch := g.Batch(400)
	if err := e.Write(batch); err != nil {
		t.Fatal(err)
	}
	l := loaded{e: e}
	for _, r := range batch {
		if r.Subject.Kind == engine.SubjectEdge {
			l.pod, l.node, l.ns = r.Subject.A, r.Subject.B, pgmodel.ToNanos(r.EventTime)
			break
		}
	}
	if l.pod.IsZero() {
		t.Fatal("the workload wrote no edge")
	}
	// The planner chooses among indexes by what the tables hold.
	conn := sqlConn(t, e)
	for _, table := range []string{"edges", "entities", "log", "log_base"} {
		if _, err := conn.Exec(context.Background(), "ANALYZE "+table); err != nil {
			t.Fatal(err)
		}
	}
	return l
}

// explainNoSeqScan explains a statement with sequential scans made as unattractive as
// the planner allows, so that the plan shows whether an index can answer it at all,
// whatever the size of the tables.
func explainNoSeqScan(t *testing.T, e *pgengine.Engine, sql string, args ...any) pgengine.Counters {
	t.Helper()
	return explainWithout(t, e, nil, sql, args...)
}

// explainWithout is explainNoSeqScan in a transaction that has first dropped the given
// indexes, and rolls the drops back, to ask whether another index can answer the
// statement when the planner has no other choice.
func explainWithout(t *testing.T, e *pgengine.Engine, drop []string, sql string, args ...any) pgengine.Counters {
	t.Helper()
	ctx := context.Background()
	tx, err := sqlConn(t, e).Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	for _, index := range drop {
		if _, err := tx.Exec(ctx, "DROP INDEX "+index); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := tx.Exec(ctx, "SET LOCAL enable_seqscan = off"); err != nil {
		t.Fatal(err)
	}
	c, err := pgengine.Explain(ctx, tx, sql, args...)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// usesIndex reports whether the plan scans the index, or a partition's index of it: the
// indexes of a partitioned table are made for each partition under a name of their own.
func usesIndex(t *testing.T, e *pgengine.Engine, c pgengine.Counters, index string) bool {
	t.Helper()
	conn := sqlConn(t, e)
	for _, n := range c.Plan {
		if n.Index == index {
			return true
		}
		if n.Index == "" {
			continue
		}
		var parent string
		err := conn.QueryRow(context.Background(), `SELECT p.relname FROM pg_inherits i
JOIN pg_class p ON p.oid = i.inhparent JOIN pg_class c ON c.oid = i.inhrelid JOIN pg_namespace s ON s.oid = c.relnamespace
WHERE c.relname = $1 AND s.nspname = $2`, n.Index, e.Schema()).Scan(&parent)
		if err == nil && parent == index {
			return true
		}
	}
	return false
}

// TestReadsUseTheIndexSetOfTheirArm: the statement of each read, planned with
// sequential scans off, is answered by the index of its arm's set: GiST on the
// interval (set G) or the btree on its end (set B) for the interval tables, and the
// log's indexes for a window. A statement an index cannot answer would be a scan of the
// whole table, which no table size would show in a conformance test.
func TestReadsUseTheIndexSetOfTheirArm(t *testing.T) {
	for _, c := range []struct {
		arm                       pgschema.Arm
		srcIdx, dstIdx, entityIdx string
	}{
		{pgschema.Stream, "edges_src_g", "edges_dst_g", "entities_g"},
		{pgschema.StreamNoPartition, "edges_src_g", "edges_dst_g", "entities_g"},
		{pgschema.Bulk, "edges_src_g", "edges_dst_g", "entities_g"},
		{pgschema.BulkB, "edges_src_b", "edges_dst_b", "entities_b"},
	} {
		t.Run(string(c.arm), func(t *testing.T) {
			l := load(t, c.arm)
			layer := int16(catalog.L2)
			fwd, rev := pgmodel.EncodeFingerprint(l.pod), pgmodel.EncodeFingerprint(l.node)
			for _, ns := range []int64{l.ns, math.MaxInt64} {
				for _, q := range []struct {
					name  string
					sql   string
					args  []any
					index string
				}{
					{"neighbors", l.e.NeighborsSQL(engine.Forward, ns), []any{fwd, layer, ns}, c.srcIdx},
					{"reverse neighbors", l.e.NeighborsSQL(engine.Reverse, ns), []any{rev, layer, ns}, c.dstIdx},
					{"batch", l.e.BatchSQL(engine.Forward, ns), []any{[][]byte{fwd, rev}, layer, ns}, c.srcIdx},
					{"reverse batch", l.e.BatchSQL(engine.Reverse, ns), []any{[][]byte{fwd, rev}, layer, ns}, c.dstIdx},
					{"alive", l.e.AliveSQL(ns), []any{fwd, layer, ns}, c.entityIdx},
				} {
					got := explainNoSeqScan(t, l.e, q.sql, q.args...)
					if !usesIndex(t, l.e, got, q.index) {
						t.Errorf("%s at %d: plan %s does not use %s", q.name, ns, got.Shape(), q.index)
					}
				}
			}
			seq := pgengine.SeqToDB(engine.Latest)
			for _, q := range []struct {
				name  string
				sql   string
				args  []any
				index string
			}{
				{"window", pgengine.WindowSQL(engine.Forward, false), []any{fwd, layer, int64(0), l.ns + int64(time.Hour), seq}, "log_g_src"},
				{"unbounded window", pgengine.WindowSQL(engine.Forward, true), []any{fwd, layer, int64(0), seq}, "log_g_src"},
				{"reverse window", pgengine.WindowSQL(engine.Reverse, false), []any{rev, layer, int64(0), l.ns + int64(time.Hour), seq}, "log_g_dst"},
			} {
				if got := explainNoSeqScan(t, l.e, q.sql, q.args...); !usesIndex(t, l.e, got, q.index) {
					t.Errorf("%s: plan %s does not use %s", q.name, got.Shape(), q.index)
				}
			}
		})
	}
}

// TestTheWritersReadsUseTheKeysOfTheStreamingArm: re-folding a subject finds its rows
// by the primary key and its records by the log's index on the subject, not by
// scanning a source's or a producer's whole history. The planner prefers the GiST
// index on a table this small, so the GiST indexes are dropped, in a transaction that
// is rolled back, to see whether the key can answer the statement.
func TestTheWritersReadsUseTheKeysOfTheStreamingArm(t *testing.T) {
	l := load(t, pgschema.Stream)
	src, dst := pgmodel.EncodeFingerprint(l.pod), pgmodel.EncodeFingerprint(l.node)
	rel := string(catalog.ScheduledOn)
	for _, q := range []struct {
		name  string
		drop  []string
		sql   string
		args  []any
		index string
	}{
		{"edge rows", []string{"edges_src_g", "edges_dst_g"}, pgengine.EdgeRowsSQL, []any{src, dst, rel, l.ns, int64(math.MinInt64)}, "edges_pkey"},
		{"entity rows", []string{"entities_g"}, pgengine.EntityRowsSQL, []any{src, l.ns, int64(math.MinInt64)}, "entities_pkey"},
		{"edge records", nil, pgengine.EdgeInputSQL, []any{src, dst, rel, l.ns}, "log_g_subject"},
		{"entity records", nil, pgengine.EntityInputSQL, []any{src, l.ns}, "log_e"},
	} {
		if got := explainWithout(t, l.e, q.drop, q.sql, q.args...); !usesIndex(t, l.e, got, q.index) {
			t.Errorf("%s: plan %s does not use %s", q.name, got.Shape(), q.index)
		}
	}
}

// TestExplainReadsTheServersOwnOutput: a real plan, parsed. A statement that reads
// something has read buffers, the figures are the top node's, and the same statement
// planned twice has one shape.
func TestExplainReadsTheServersOwnOutput(t *testing.T) {
	l := load(t, pgschema.Stream)
	ctx := context.Background()
	conn := sqlConn(t, l.e)
	sql := l.e.NeighborsSQL(engine.Forward, l.ns)
	args := []any{pgmodel.EncodeFingerprint(l.pod), int16(catalog.L2), l.ns}
	a, err := pgengine.Explain(ctx, conn, sql, args...)
	if err != nil {
		t.Fatal(err)
	}
	b, err := pgengine.Explain(ctx, conn, sql, args...)
	if err != nil {
		t.Fatal(err)
	}
	if a.SharedHit+a.SharedRead == 0 || len(a.Plan) == 0 || a.Shape() != b.Shape() {
		t.Errorf("a neighbors read: %+v, then %+v", a, b)
	}
	// The rows it returns are the rows the read returns.
	ns, err := l.e.Neighbors(l.pod, engine.Forward, pgmodel.FromNanos(l.ns), engine.Current(catalog.L2))
	if err != nil {
		t.Fatal(err)
	}
	if a.RowsReturned != int64(len(ns)) {
		t.Errorf("the plan says %d rows, the read returns %d", a.RowsReturned, len(ns))
	}
	// A statement the server rejects is an error, and a transaction is not left
	// aborted by it for a caller that went on.
	if _, err := pgengine.Explain(ctx, conn, "SELECT * FROM there_is_no_such_table"); err == nil {
		t.Error("an explain of a missing table was accepted")
	}
}

// TestTheShippedSettingsAreTheServersOwn: scripts/pg-server.sh started this server
// with settings.conf, so SHOW gives every value in it as written, in the units the
// server prints. A server started by hand with other settings is refused.
func TestTheShippedSettingsAreTheServersOwn(t *testing.T) {
	ctx := context.Background()
	want, _, err := pgengine.LoadSettings(settingsPath)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := pgx.Connect(ctx, serverDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	if err := pgengine.CheckSettings(ctx, conn, want); err != nil {
		t.Fatalf("the server was not started with settings.conf: %v", err)
	}
	changed := slices.Clone(want)
	changed[0].Value += "x"
	changed = append(changed, pgengine.Setting{Name: "no_such_setting", Value: "1"})
	var diff *pgengine.SettingsError
	if err := pgengine.CheckSettings(ctx, conn, changed); err == nil {
		t.Fatal("a server with other settings was accepted")
	} else if !errors.As(err, &diff) || len(diff.Differences) != 2 {
		t.Errorf("%v", err)
	}
}
