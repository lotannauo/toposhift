package pgengine_test

import (
	"context"
	"errors"
	"maps"
	"math"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/postgres/pgengine"
	"github.com/lotannauo/toposhift/bench/postgres/pgmodel"
	"github.com/lotannauo/toposhift/bench/postgres/pgschema"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/workload"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

func fp(typ catalog.EntityType, b byte) identity.Fingerprint {
	var h [identity.FingerprintBytes]byte
	h[0] = b
	f, err := identity.FingerprintFromHash(typ, h)
	if err != nil {
		panic(err)
	}
	return f
}

var (
	pod1, node1, node2 = fp("k8s.pod", 1), fp("k8s.node", 3), fp("k8s.node", 4)
	edge               = engine.EdgeSubject(pod1, node1, catalog.ScheduledOn)
)

func at(s int64) time.Time { return time.Unix(s, 0).UTC() }

func observe(seq uint64, p lifecycle.Producer, s, ttl int64) engine.Record {
	return engine.Record{
		Layer: catalog.L2, Subject: edge, Producer: p, EventTime: at(s), Seq: seq,
		Kind: lifecycle.Observe, TTL: time.Duration(ttl) * time.Second, Payload: []byte{byte(seq)},
	}
}

func del(seq uint64, p lifecycle.Producer, s int64) engine.Record {
	return engine.Record{Layer: catalog.L2, Subject: edge, Producer: p, EventTime: at(s), Seq: seq, Kind: lifecycle.Delete}
}

func neighbors(t *testing.T, e engine.Engine, fp identity.Fingerprint, dir engine.Direction, s int64, sc engine.Scope) []identity.Fingerprint {
	t.Helper()
	ns, err := e.Neighbors(fp, dir, at(s), sc)
	if err != nil {
		t.Fatal(err)
	}
	var out []identity.Fingerprint
	for _, n := range ns {
		out = append(out, n.Peer)
	}
	return out
}

// TestWritesAreRefusedWholeAndLeaveNothingBehind covers each way a batch is refused:
// invalid records, sequence numbers that do not ascend, a subject that changes layer,
// and a record before the horizon. After each, the token and the rows are as they
// were, and the valid records of the batch were not stored.
func TestWritesAreRefusedWholeAndLeaveNothingBehind(t *testing.T) {
	for _, arm := range []pgschema.Arm{pgschema.Stream, pgschema.Bulk} {
		t.Run(string(arm), func(t *testing.T) {
			e := openEngine(t, arm)
			if err := e.Write(nil); err != nil {
				t.Fatalf("an empty batch: %v", err)
			}
			if err := e.Write([]engine.Record{observe(1, "a", 10, 5), observe(2, "a", 12, 5)}); err != nil {
				t.Fatal(err)
			}
			conn := sqlConn(t, e)
			logRows := func() (n int) {
				if err := conn.QueryRow(context.Background(), "SELECT count(*) FROM log").Scan(&n); err != nil {
					t.Fatal(err)
				}
				return n
			}
			before, err := e.Tables(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			rowsBefore := logRows()

			other := observe(3, "a", 14, 5)
			other.Layer = catalog.L3
			otherLater := other
			otherLater.Seq = 4
			noProducer := observe(3, "", 14, 5)
			for name, batch := range map[string][]engine.Record{
				"an invalid record after a valid one":     {observe(3, "a", 14, 5), noProducer},
				"a seq that is already written":           {observe(2, "a", 14, 5)},
				"a seq of zero":                           {observe(0, "a", 14, 5)},
				"a seq that does not ascend in the batch": {observe(5, "a", 14, 5), observe(4, "a", 15, 5)},
				"a seq repeated in the batch":             {observe(5, "a", 14, 5), observe(5, "a", 15, 5)},
				"a subject in two layers in one batch":    {observe(3, "a", 14, 5), otherLater},
				"a subject in another layer than stored":  {other},
			} {
				if err := e.Write(batch); !errors.Is(err, engine.ErrInvalid) {
					t.Errorf("%s: %v", name, err)
				}
				after, err := e.Tables(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				if e.LastSeq() != 2 || !after.Equal(before) || logRows() != rowsBefore {
					t.Errorf("%s: left something behind: seq %d, %d log rows (was %d), rows differ: %v", name, e.LastSeq(), logRows(), rowsBefore, !after.Equal(before))
				}
			}

			// The refusals consumed no sequence number: seq 3 is still free.
			if err := e.Write([]engine.Record{observe(3, "a", 14, 5)}); err != nil {
				t.Fatalf("seq 3 after the refusals: %v", err)
			}

			// A record before the horizon refuses its whole batch, with the error that
			// says so, which is also an ErrInvalid.
			if err := e.Retain(at(20)); err != nil {
				t.Fatal(err)
			}
			before, _ = e.Tables(context.Background())
			rowsBefore = logRows()
			err = e.Write([]engine.Record{observe(4, "a", 30, 5), observe(5, "a", 19, 5), observe(6, "a", 31, 5)})
			if !errors.Is(err, engine.ErrBeforeHorizon) || !errors.Is(err, engine.ErrInvalid) {
				t.Errorf("a record before the horizon: %v", err)
			}
			after, _ := e.Tables(context.Background())
			if e.LastSeq() != 3 || !after.Equal(before) || logRows() != rowsBefore {
				t.Errorf("a record before the horizon left something behind")
			}
			// At the horizon itself a record is accepted.
			if err := e.Write([]engine.Record{observe(4, "a", 20, 5)}); err != nil {
				t.Errorf("a record at the horizon: %v", err)
			}
		})
	}
}

// TestADatabaseFailureRollsTheWholeBatchBack: a batch is one transaction. A constraint
// that only the interval rows violate fails after the log rows have been sent, and
// nothing of the batch remains.
func TestADatabaseFailureRollsTheWholeBatchBack(t *testing.T) {
	e := openEngine(t, pgschema.Stream)
	if err := e.Write([]engine.Record{observe(1, "a", 10, 5)}); err != nil {
		t.Fatal(err)
	}
	conn := sqlConn(t, e)
	if _, err := conn.Exec(context.Background(), "ALTER TABLE edges ADD CONSTRAINT never CHECK (start_ns < 0) NOT VALID"); err != nil {
		t.Fatal(err)
	}
	err := e.Write([]engine.Record{observe(2, "b", 100, 5)})
	if err == nil || errors.Is(err, engine.ErrInvalid) {
		t.Fatalf("a failing write: %v", err)
	}
	var n int
	if err := conn.QueryRow(context.Background(), "SELECT count(*) FROM log").Scan(&n); err != nil || n != 1 {
		t.Errorf("the log has %d rows after a failed batch, want 1 (%v)", n, err)
	}
	if e.LastSeq() != 1 {
		t.Errorf("the token is %d after a failed batch", e.LastSeq())
	}
	if got := e.Counts(); got.Records != 1 {
		t.Errorf("the counts include the failed batch: %+v", got)
	}
}

// TestAnEngineOpenedAgainFindsWhatItLeft: the token, the horizon and a horizon past the
// last instant (which a bigint cannot say) are in the database, not in the engine.
func TestAnEngineOpenedAgainFindsWhatItLeft(t *testing.T) {
	dsn := serverDSN(t)
	for _, arm := range []pgschema.Arm{pgschema.Stream, pgschema.BulkB} {
		t.Run(string(arm), func(t *testing.T) {
			schema := pgschema.SchemaName(t.TempDir())
			t.Cleanup(func() { _ = pgengine.DropSchema(context.Background(), dsn, schema) })
			open := func() *pgengine.Engine {
				e, err := pgengine.Open(context.Background(), pgengine.Config{DSN: dsn, Schema: schema, Arm: arm})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = e.Close() })
				return e
			}
			e := open()
			if err := e.Write([]engine.Record{observe(1<<63+5, "a", 10, 100), observe(1<<63+6, "a", 50, 100)}); err != nil {
				t.Fatal(err)
			}
			if err := e.Retain(at(30)); err != nil {
				t.Fatal(err)
			}
			tables, _ := e.Tables(context.Background())
			if err := e.Close(); err != nil {
				t.Fatal(err)
			}

			e = open()
			if e.LastSeq() != 1<<63+6 {
				t.Errorf("token %d after reopening", e.LastSeq())
			}
			if again, _ := e.Tables(context.Background()); !again.Equal(tables) {
				t.Errorf("rows differ after reopening: %s", again.FirstDifference(tables))
			}
			if err := e.Write([]engine.Record{observe(1<<63+7, "a", 29, 5)}); !errors.Is(err, engine.ErrBeforeHorizon) {
				t.Errorf("a record before the horizon after reopening: %v", err)
			}
			if err := e.Write([]engine.Record{observe(1<<63+6, "a", 60, 5)}); !errors.Is(err, engine.ErrInvalid) {
				t.Errorf("a seq already written, after reopening: %v", err)
			}
			// A token above 2^63 sees what the log has at it, and one below sees nothing.
			sc := engine.Scope{Layer: catalog.L2, AsOf: 1<<63 + 5}
			if got := neighbors(t, e, pod1, engine.Forward, 40, sc); len(got) != 1 {
				t.Errorf("at the first token the edge is alive at 40: %v", got)
			}
			sc.AsOf = 1<<63 - 1
			if got := neighbors(t, e, pod1, engine.Forward, 40, sc); len(got) != 0 {
				t.Errorf("below the first token nothing is alive: %v", got)
			}

			// Past the last instant everything is before the horizon, for good.
			if err := e.Retain(engine.MaxEventTime.Add(time.Nanosecond)); err != nil {
				t.Fatal(err)
			}
			if err := e.Close(); err != nil {
				t.Fatal(err)
			}
			e = open()
			if err := e.Write([]engine.Record{observe(1<<63+7, "a", 4_000_000_000, 5)}); !errors.Is(err, engine.ErrBeforeHorizon) {
				t.Errorf("a write after a retention past the end, and a reopening: %v", err)
			}
			last := observe(1<<63+7, "a", 0, 0)
			last.EventTime = engine.MaxEventTime
			if err := e.Write([]engine.Record{last}); !errors.Is(err, engine.ErrBeforeHorizon) {
				t.Errorf("a write at the last instant after a retention past the end: %v", err)
			}
		})
	}
}

// TestAnEngineRefusesASchemaThatIsNotItsOwn: an arm, a fold version, or a schema that
// is something else.
func TestAnEngineRefusesASchemaThatIsNotItsOwn(t *testing.T) {
	dsn := serverDSN(t)
	e := openEngine(t, pgschema.Stream)
	schema := e.Schema()
	cfg := func(arm pgschema.Arm) pgengine.Config {
		return pgengine.Config{DSN: dsn, Schema: schema, Arm: arm}
	}
	if err := e.Write([]engine.Record{observe(1, "a", 10, 5)}); err != nil {
		t.Fatal(err)
	}
	for _, arm := range []pgschema.Arm{pgschema.StreamNoPartition, pgschema.Bulk, pgschema.BulkB} {
		if o, err := pgengine.Open(context.Background(), cfg(arm)); !errors.Is(err, pgengine.ErrSchema) {
			if o != nil {
				_ = o.Close()
			}
			t.Errorf("a stream schema opened as %s: %v", arm, err)
		}
	}
	conn := sqlConn(t, e)
	if _, err := conn.Exec(context.Background(), "UPDATE meta SET fold_version = fold_version + 1"); err != nil {
		t.Fatal(err)
	}
	if o, err := pgengine.Open(context.Background(), cfg(pgschema.Stream)); !errors.Is(err, pgengine.ErrSchema) {
		if o != nil {
			_ = o.Close()
		}
		t.Errorf("a schema of another fold version: %v", err)
	}
	if _, err := conn.Exec(context.Background(), "UPDATE meta SET fold_version = fold_version - 1"); err != nil {
		t.Fatal(err)
	}
	o, err := pgengine.Open(context.Background(), cfg(pgschema.Stream))
	if err != nil {
		t.Fatalf("the same schema, arm and version: %v", err)
	}
	_ = o.Close()

	// A schema that is something else, with a table of an engine's name in it, is
	// not taken for a new one and written over.
	stray := pgschema.SchemaName(t.TempDir())
	t.Cleanup(func() { _ = pgengine.DropSchema(context.Background(), dsn, stray) })
	if _, err := conn.Exec(context.Background(), "CREATE SCHEMA "+stray+"; CREATE TABLE "+stray+".log (x int)"); err != nil {
		t.Fatal(err)
	}
	if o, err := pgengine.Open(context.Background(), pgengine.Config{DSN: dsn, Schema: stray, Arm: pgschema.Stream}); err == nil {
		_ = o.Close()
		t.Error("a schema with a stray table named log was taken for a new one")
	}
	// A failed open leaves nothing half made: the meta table is not there.
	var exists bool
	if err := conn.QueryRow(context.Background(), "SELECT to_regclass($1) IS NOT NULL", stray+".meta").Scan(&exists); err != nil || exists {
		t.Errorf("a failed open left a meta table (%v)", err)
	}
}

// TestEnginesOpeningOneNewSchemaTogetherAgree: the creation of the schema is
// serialized, so concurrent opens neither fail nor make two of everything.
func TestEnginesOpeningOneNewSchemaTogetherAgree(t *testing.T) {
	dsn := serverDSN(t)
	schema := pgschema.SchemaName(t.TempDir())
	t.Cleanup(func() { _ = pgengine.DropSchema(context.Background(), dsn, schema) })
	var wg sync.WaitGroup
	engines := make([]*pgengine.Engine, 6)
	errs := make([]error, len(engines))
	for i := range engines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			engines[i], errs[i] = pgengine.Open(context.Background(), pgengine.Config{DSN: dsn, Schema: schema, Arm: pgschema.Stream, MaxConns: 2})
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("open %d: %v", i, err)
		}
		defer engines[i].Close()
	}
	if err := engines[0].Write([]engine.Record{observe(1, "a", 10, 5)}); err != nil {
		t.Fatal(err)
	}
	if got := engines[5].LastSeq(); got != 1 {
		t.Errorf("another engine over the schema has the token %d", got)
	}
}

// TestAReadAsOfAnEarlierTokenFoldsTheLog: the interval tables have no ingest time, so a
// read pinned to an earlier token cannot be answered from them; it sees the world the
// token names, a deletion that came later included or not.
func TestAReadAsOfAnEarlierTokenFoldsTheLog(t *testing.T) {
	for _, arm := range []pgschema.Arm{pgschema.Stream, pgschema.Bulk, pgschema.BulkB} {
		t.Run(string(arm), func(t *testing.T) {
			e := openEngine(t, arm)
			pod := engine.EntitySubject(pod1)
			entity := func(seq uint64, kind lifecycle.Kind, s int64) engine.Record {
				r := engine.Record{Layer: catalog.L2, Subject: pod, Producer: "a", EventTime: at(s), Seq: seq, Kind: kind}
				if kind == lifecycle.Observe {
					r.TTL = time.Hour
				}
				return r
			}
			if err := e.Write([]engine.Record{observe(1, "a", 10, 1000), entity(2, lifecycle.Observe, 10)}); err != nil {
				t.Fatal(err)
			}
			if err := e.Write([]engine.Record{del(3, "a", 20), entity(4, lifecycle.Delete, 20)}); err != nil {
				t.Fatal(err)
			}
			// The edge lives from 10 until its deletion at 20 (seq 3), the entity likewise
			// (seq 4); each lives on, absent its deletion, for a thousand seconds and an hour.
			for _, asOf := range []uint64{0, 1, 2, 3, 4, engine.Latest} {
				sc := engine.Scope{Layer: catalog.L2, AsOf: asOf}
				if got, want := len(neighbors(t, e, pod1, engine.Forward, 30, sc)) == 1, asOf == 1 || asOf == 2; got != want {
					t.Errorf("neighbors at 30 as of seq %d: alive %v, want %v", asOf, got, want)
				}
				if got, want := len(neighbors(t, e, node1, engine.Reverse, 15, sc)) == 1, asOf >= 1; got != want {
					t.Errorf("reverse neighbors at 15 as of seq %d: alive %v, want %v", asOf, got, want)
				}
				// The entity's own records decide Alive, not the edge's.
				if alive, err := e.Alive(pod1, at(30), sc); err != nil || alive != (asOf == 2 || asOf == 3) {
					t.Errorf("alive at 30 as of seq %d: %v, %v", asOf, alive, err)
				}
				if alive, err := e.Alive(pod1, at(15), sc); err != nil || alive != (asOf >= 2) {
					t.Errorf("alive at 15 as of seq %d: %v, %v", asOf, alive, err)
				}
				lists, err := e.NeighborsBatch([]identity.Fingerprint{pod1, node2, pod1}, engine.Forward, at(15), sc)
				if err != nil || len(lists) != 3 || len(lists[1]) != 0 || len(lists[0]) != len(lists[2]) || (len(lists[0]) == 1) != (asOf >= 1) {
					t.Errorf("batch at 15 as of seq %d: %v, %v", asOf, lists, err)
				}
			}
		})
	}
}

// TestDayPartitionsAreMadeAheadOfTheWrites: the streaming arm's log has a partition
// for every UTC day a write reaches, including the last day an instant can name, and the
// unpartitioned arm has none.
func TestDayPartitionsAreMadeAheadOfTheWrites(t *testing.T) {
	day := int64(24 * 3600)
	records := []engine.Record{
		observe(1, "a", 3*day+5, 5), observe(2, "a", 3*day+6, 5), // one day, twice
		observe(3, "a", 3*day-1, 5), // the day before
	}
	last := observe(4, "a", 0, 0)
	last.EventTime = engine.MaxEventTime
	records = append(records, last)
	for _, arm := range []pgschema.Arm{pgschema.Stream, pgschema.StreamNoPartition} {
		e := openEngine(t, arm)
		if err := e.Write(records); err != nil {
			t.Fatalf("%s: %v", arm, err)
		}
		// A second write to a known day makes no partition, and to a new day makes one.
		if err := e.Write([]engine.Record{observe(5, "a", 3*day+10, 5), observe(6, "a", 7*day, 5)}); err != nil {
			t.Fatalf("%s: %v", arm, err)
		}
		var names []string
		rows, err := sqlConn(t, e).Query(context.Background(), `SELECT c.relname FROM pg_inherits i
JOIN pg_class c ON c.oid = i.inhrelid JOIN pg_class p ON p.oid = i.inhparent JOIN pg_namespace n ON n.oid = p.relnamespace
WHERE p.relname = 'log' AND n.nspname = $1 ORDER BY 1`, e.Schema())
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var n string
			if err := rows.Scan(&n); err != nil {
				t.Fatal(err)
			}
			names = append(names, n)
		}
		rows.Close()
		want := []string{"log_p19700103", "log_p19700104", "log_p19700108", "log_p22620411"}
		if arm != pgschema.Stream {
			want = nil
		}
		if !slices.Equal(names, want) {
			t.Errorf("%s: partitions %v, want %v", arm, names, want)
		}
		if got, err := e.Window(pod1, engine.Forward, at(0), engine.MaxEventTime.Add(time.Nanosecond), engine.Current(catalog.L2)); err != nil || len(got) != 6 {
			t.Errorf("%s: a window over everything: %d records, %v", arm, len(got), err)
		}
	}
}

// TestProvisionalRowsAreReadAtTheirDeadline: a streaming writer may keep a row
// provisional, its range unbounded and its end in deadline_ns, which no index covers.
// A read must stop at the deadline whatever the range says. The planner of this engine
// writes no such row, so they are made by hand.
func TestProvisionalRowsAreReadAtTheirDeadline(t *testing.T) {
	for _, arm := range []pgschema.Arm{pgschema.Stream, pgschema.StreamNoPartition} {
		t.Run(string(arm), func(t *testing.T) {
			e := openEngine(t, arm)
			conn := sqlConn(t, e)
			src, dst := pgmodel.EncodeFingerprint(pod1), pgmodel.EncodeFingerprint(node1)
			ns := int64(time.Second)
			for _, q := range []struct {
				sql  string
				args []any
			}{
				{`INSERT INTO edges (src, dst, rel, layer, start_ns, during, deadline_ns, end_source) VALUES ($1, $2, 'scheduled_on', $5, $3, int8range($3, NULL), $4, 1)`, []any{src, dst, 10 * ns, 20 * ns, int16(catalog.L2)}},
				{`INSERT INTO entities (fp, layer, start_ns, during, deadline_ns, end_source) VALUES ($1, $4, $2, int8range($2, NULL), $3, 1)`, []any{src, 10 * ns, 20 * ns, int16(catalog.L2)}},
			} {
				if _, err := conn.Exec(context.Background(), q.sql, q.args...); err != nil {
					t.Fatal(err)
				}
			}
			sc := engine.Current(catalog.L2)
			for s, want := range map[int64]bool{9: false, 10: true, 19: true, 20: false, 1000: false} {
				if got := len(neighbors(t, e, pod1, engine.Forward, s, sc)) == 1; got != want {
					t.Errorf("neighbors at %ds: alive %v, want %v", s, got, want)
				}
				if got, err := e.Alive(pod1, at(s), sc); err != nil || got != want {
					t.Errorf("alive at %ds: %v (%v), want %v", s, got, err, want)
				}
				lists, err := e.NeighborsBatch([]identity.Fingerprint{pod1}, engine.Forward, at(s), sc)
				if err != nil || (len(lists[0]) == 1) != want {
					t.Errorf("batch at %ds: %v (%v), want alive %v", s, lists, err, want)
				}
			}
		})
	}
}

// TestSizeIsWhatTheSchemaHolds: it grows as records are stored, and is the tables and
// indexes of the engine's own schema.
func TestSizeIsWhatTheSchemaHolds(t *testing.T) {
	e := openEngine(t, pgschema.Stream)
	empty, err := e.Size()
	if err != nil || empty <= 0 {
		t.Fatalf("an empty schema: %d, %v", empty, err)
	}
	g, err := workload.New(workload.Tiny())
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Write(g.Batch(300)); err != nil {
		t.Fatal(err)
	}
	if size, err := e.Size(); err != nil || size <= empty {
		t.Errorf("after 300 records: %d, %v; empty was %d", size, err, empty)
	}
}

// TestRetentionKeepsOneBaselinePerSubjectAndProducer: what a retention keeps of the
// records it drops is, for each subject and producer, the last of them by (event time,
// seq), and a later retention replaces it, so the baselines do not pile up. An entity's
// records and an edge's are both baselined, and a record at or after the horizon is
// not.
func TestRetentionKeepsOneBaselinePerSubjectAndProducer(t *testing.T) {
	for _, arm := range []pgschema.Arm{pgschema.Stream, pgschema.StreamNoPartition, pgschema.Bulk, pgschema.BulkB} {
		t.Run(string(arm), func(t *testing.T) {
			e := openEngine(t, arm)
			pod := engine.EntitySubject(pod1)
			entity := func(seq uint64, p lifecycle.Producer, s int64) engine.Record {
				return engine.Record{Layer: catalog.L2, Subject: pod, Producer: p, EventTime: at(s), Seq: seq, Kind: lifecycle.Observe, TTL: time.Minute}
			}
			// Seq 5 is the late one: it has a later seq and an earlier event time than seq 4.
			if err := e.Write([]engine.Record{
				observe(1, "a", 10, 5), observe(2, "b", 11, 5), entity(3, "a", 12), observe(4, "a", 14, 5),
				observe(5, "a", 13, 5), entity(6, "a", 40), observe(7, "a", 40, 5),
			}); err != nil {
				t.Fatal(err)
			}
			conn := sqlConn(t, e)
			baselines := func() map[string]int {
				rows, err := conn.Query(context.Background(), `SELECT subject_kind::text || ' ' || producer || ' ' || (seq # $1::bigint)::text, count(*) FROM log_base GROUP BY 1`, int64(math.MinInt64))
				if err != nil {
					t.Fatal(err)
				}
				defer rows.Close()
				out := make(map[string]int)
				for rows.Next() {
					var k string
					var n int
					if err := rows.Scan(&k, &n); err != nil {
						t.Fatal(err)
					}
					out[k] = n
				}
				return out
			}
			if err := e.Retain(at(30)); err != nil {
				t.Fatal(err)
			}
			// Edge: producer a keeps seq 4 (event 14, the last by event time, not by seq),
			// b keeps seq 2. Entity: a keeps seq 3. Seqs 6 and 7 are at or after 30.
			want := map[string]int{"g a 4": 1, "g b 2": 1, "e a 3": 1}
			if got := baselines(); !maps.Equal(got, want) {
				t.Errorf("baselines after the first retention: %v, want %v", got, want)
			}
			if err := e.Write([]engine.Record{observe(8, "a", 50, 5), observe(9, "b", 51, 5)}); err != nil {
				t.Fatal(err)
			}
			if err := e.Retain(at(45)); err != nil {
				t.Fatal(err)
			}
			// The first baselines are replaced where there is a later record, and stand
			// where there is none.
			want = map[string]int{"g a 7": 1, "g b 2": 1, "e a 6": 1}
			if got := baselines(); !maps.Equal(got, want) {
				t.Errorf("baselines after the second retention: %v, want %v", got, want)
			}
		})
	}
}
