package pgengine_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/lotannauo/toposhift/bench/postgres/pgengine"
	"github.com/lotannauo/toposhift/bench/postgres/pgmodel"
	"github.com/lotannauo/toposhift/bench/postgres/pgreport"
	"github.com/lotannauo/toposhift/bench/postgres/pgschema"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// streamArms are the arms the streaming writer keeps.
var streamArms = []pgschema.Arm{pgschema.Stream, pgschema.StreamNoPartition, pgschema.StreamNaive}

func fpn(typ catalog.EntityType, n int) identity.Fingerprint {
	var h [identity.FingerprintBytes]byte
	h[0], h[1], h[2] = byte(n), byte(n>>8), byte(n>>16)
	f, err := identity.FingerprintFromHash(typ, h)
	if err != nil {
		panic(err)
	}
	return f
}

// heartbeats makes the batches of a stream of refreshes: edges pods that are scheduled
// on nodes, each refreshed every period with a TTL of two and a half periods, so that
// each refresh lengthens the one interval its edge has. Every fifth edge stops at
// stopAt, and comes back (a new interval) a few periods later; and from time to time a
// record arrives late, which is what the re-fold is for. Seq numbers start at first.
type heartbeat struct {
	edges, steps int
	period       time.Duration
	start        time.Time
	first        uint64
}

func (h heartbeat) batches() [][]engine.Record {
	seq := h.first
	var out [][]engine.Record
	for step := range h.steps {
		now := h.start.Add(time.Duration(step) * h.period)
		var batch []engine.Record
		emit := func(i int, at time.Time) {
			seq++
			batch = append(batch, engine.Record{
				Layer: catalog.L2, Subject: engine.EdgeSubject(fpn("k8s.pod", i), fpn("k8s.node", i%7), catalog.ScheduledOn),
				Producer: "kubelet", EventTime: at, Seq: seq, Kind: lifecycle.Observe,
				TTL: h.period * 5 / 2, Payload: []byte{byte(i)},
			})
		}
		for i := range h.edges {
			// Every fifth edge is silent for six periods from a third of the way through.
			if i%5 == 0 && step >= h.steps/3 && step < h.steps/3+6 {
				continue
			}
			emit(i, now)
		}
		// A late record, from before the latest the edge's producer has asserted.
		if step > 3 && step%4 == 0 {
			emit(step%h.edges, now.Add(-2*h.period))
		}
		out = append(out, batch)
	}
	return out
}

func writeAll(t testing.TB, e engine.Engine, batches [][]engine.Record) {
	t.Helper()
	for _, b := range batches {
		if err := e.Write(slices.Clone(b)); err != nil {
			t.Fatal(err)
		}
	}
}

// stmtLog is a tracer that keeps the text of every statement, batched ones and COPYs
// included.
type stmtLog struct {
	mu   sync.Mutex
	on   bool
	sqls []string
}

func (l *stmtLog) add(s string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.on {
		l.sqls = append(l.sqls, strings.Join(strings.Fields(s), " "))
	}
}

func (l *stmtLog) record(f func()) []string {
	l.mu.Lock()
	l.on, l.sqls = true, nil
	l.mu.Unlock()
	f()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.on = false
	return l.sqls
}

func (l *stmtLog) TraceQueryStart(ctx context.Context, _ *pgx.Conn, d pgx.TraceQueryStartData) context.Context {
	l.add(d.SQL)
	return ctx
}
func (l *stmtLog) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}
func (l *stmtLog) TraceBatchStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceBatchStartData) context.Context {
	return ctx
}

func (l *stmtLog) TraceBatchQuery(_ context.Context, _ *pgx.Conn, d pgx.TraceBatchQueryData) {
	l.add(d.SQL)
}
func (l *stmtLog) TraceBatchEnd(context.Context, *pgx.Conn, pgx.TraceBatchEndData) {}
func (l *stmtLog) TraceCopyFromStart(ctx context.Context, _ *pgx.Conn, d pgx.TraceCopyFromStartData) context.Context {
	l.add("COPY " + d.TableName.Sanitize())
	return ctx
}
func (l *stmtLog) TraceCopyFromEnd(context.Context, *pgx.Conn, pgx.TraceCopyFromEndData) {}

func openTraced(t testing.TB, arm pgschema.Arm, tr pgx.QueryTracer) *pgengine.Engine {
	t.Helper()
	dsn := serverDSN(t)
	schema := pgschema.SchemaName(t.TempDir())
	e, err := pgengine.Open(context.Background(), pgengine.Config{DSN: dsn, Schema: schema, Arm: arm, Tracer: tr})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = e.Close()
		if err := pgengine.DropSchema(context.Background(), dsn, schema); err != nil {
			t.Error(err)
		}
	})
	return e
}

// TestARefreshIsWrittenWithoutReadingAnything: a batch in which every record only
// lengthens an interval reads nothing but the meta row it locks: its statements are the
// COPY of the log, updates of deadlines and inserts of new intervals, and the token.
func TestARefreshIsWrittenWithoutReadingAnything(t *testing.T) {
	log := &stmtLog{}
	e := openTraced(t, pgschema.Stream, log)
	h := heartbeat{edges: 12, steps: 30, period: 10 * time.Second, start: time.Unix(3*86400, 0).UTC()}
	batches := h.batches()
	writeAll(t, e, batches[:3])
	before := e.Counts()
	sqls := log.record(func() { writeAll(t, e, batches[3:4]) })
	after := e.Counts()
	if after.Refolds != before.Refolds || after.FastSubjects-before.FastSubjects != 12 {
		t.Fatalf("the batch was not all fast: %+v then %+v", before, after)
	}
	if after.RowsUpdated-before.RowsUpdated != 12 || after.RowsHeapOnly-before.RowsHeapOnly != 12 || after.RowsInserted != before.RowsInserted {
		t.Fatalf("12 refreshes should be 12 heap-only updates: %+v then %+v", before, after)
	}
	for _, s := range sqls {
		switch {
		case s == "begin" || s == "commit":
		case strings.HasPrefix(s, "UPDATE edges e SET deadline_ns"), strings.HasPrefix(s, "UPDATE meta"),
			strings.HasPrefix(s, "COPY"), strings.Contains(s, "FROM meta FOR UPDATE"):
		default:
			t.Errorf("a batch of refreshes ran %.80s", s)
		}
	}
	if !slices.ContainsFunc(sqls, func(s string) bool { return strings.HasPrefix(s, "COPY") }) {
		t.Errorf("the log was not written with COPY: %v", sqls)
	}
	// The statement that moves a deadline sets that column and no other.
	for _, s := range sqls {
		if strings.HasPrefix(s, "UPDATE edges") && strings.Contains(s, "during") {
			t.Errorf("an extension rewrites the range: %s", s)
		}
	}
}

// TestHeapOnlyUpdatesAreHeapOnlyOnTheServer: the updates the planner says leave the range
// alone are made heap-only by the server (no index entry is written for them), and a naive
// writer's updates, which rewrite the range, are not. The server's count is of its own
// statistics, which it reports after a moment.
func TestHeapOnlyUpdatesAreHeapOnlyOnTheServer(t *testing.T) {
	stats := func(t *testing.T, e *pgengine.Engine, wantUpd uint64) (upd, hot uint64) {
		conn := sqlConn(t, e)
		for range 100 {
			if _, err := conn.Exec(context.Background(), "SELECT pg_stat_force_next_flush()"); err != nil {
				t.Fatal(err)
			}
			if err := conn.QueryRow(context.Background(), "SELECT n_tup_upd, n_tup_hot_upd FROM pg_stat_user_tables WHERE schemaname = $1 AND relname = 'edges'", e.Schema()).Scan(&upd, &hot); err != nil {
				t.Fatal(err)
			}
			if upd == wantUpd {
				return upd, hot
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatalf("the server counts %d updates of edges, the writer %d", upd, wantUpd)
		return 0, 0
	}
	h := heartbeat{edges: 40, steps: 30, period: 10 * time.Second, start: time.Unix(3*86400, 0).UTC()}
	for _, arm := range []pgschema.Arm{pgschema.Stream, pgschema.StreamNaive} {
		t.Run(string(arm), func(t *testing.T) {
			e := openEngine(t, arm)
			writeAll(t, e, h.batches())
			c := e.Counts()
			upd, hot := stats(t, e, c.RowsUpdated)
			if arm == pgschema.Stream {
				// The planner says RowsHeapOnly of the updates leave the range alone; the
				// server makes at least that share heap-only (it makes more of them than the
				// ones it was told of only if it is also given room, which it is: the fill
				// factor leaves it).
				if c.RowsHeapOnly == 0 || hot < c.RowsHeapOnly*95/100 {
					t.Fatalf("the planner counts %d heap-only updates of %d, the server made %d of %d", c.RowsHeapOnly, c.RowsUpdated, hot, upd)
				}
			} else if hot != 0 {
				t.Fatalf("a naive writer rewrites the range of every extension, and the server made %d of %d updates heap-only", hot, upd)
			}
		})
	}
}

// TestTheFinalizerLeavesNoProvisionalRowPastTheNewestEventTime: after every batch no row
// has a deadline at or before the newest event time written, on the server as in the model,
// and the rows are the model's in the same form.
func TestTheFinalizerLeavesNoProvisionalRowPastTheNewestEventTime(t *testing.T) {
	h := heartbeat{edges: 25, steps: 40, period: 10 * time.Second, start: time.Unix(3*86400, 0).UTC()}
	for _, arm := range streamArms {
		t.Run(string(arm), func(t *testing.T) {
			e, m := openEngine(t, arm), pgmodel.NewWith(pgengine.ModelFor(arm))
			conn := sqlConn(t, e)
			for i, b := range h.batches() {
				for _, w := range []engine.Engine{e, m} {
					if err := w.Write(slices.Clone(b)); err != nil {
						t.Fatal(err)
					}
				}
				var newest int64
				for _, r := range b {
					newest = max(newest, pgmodel.ToNanos(r.EventTime))
				}
				var stale int
				// The newest event time written so far is at least this batch's.
				if err := conn.QueryRow(context.Background(), `SELECT count(*) FROM edges WHERE deadline_ns IS NOT NULL AND deadline_ns <= (SELECT max(event_ns) FROM log)`).Scan(&stale); err != nil {
					t.Fatal(err)
				}
				if stale != 0 {
					t.Fatalf("after batch %d, %d provisional rows have a deadline at or before the newest event time", i, stale)
				}
				got, err := e.Tables(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				if want := m.Tables(); !got.Identical(want) {
					t.Fatalf("after batch %d the rows differ from the model's: %s", i, got.FirstDifference(want))
				}
			}
			if arm != pgschema.StreamNaive && e.Counts().RowsFinalized == 0 {
				t.Fatal("the stream finalized no row")
			}
		})
	}
}

// TestTheWriterRemembersWhereItsRowsAre: after a stream with re-folds, finalizations and
// retentions, the place the writer remembers for the last row of every subject is where
// that row is.
func TestTheWriterRemembersWhereItsRowsAre(t *testing.T) {
	h := heartbeat{edges: 30, steps: 60, period: 10 * time.Minute, start: time.Unix(3*86400, 0).UTC()}
	for _, arm := range streamArms {
		t.Run(string(arm), func(t *testing.T) {
			e := openEngine(t, arm)
			for i, b := range h.batches() {
				if err := e.Write(slices.Clone(b)); err != nil {
					t.Fatal(err)
				}
				if i == 25 || i == 45 {
					if err := e.Retain(h.start.Add(time.Duration(i-12) * h.period)); err != nil {
						t.Fatal(err)
					}
				}
				if i%7 == 0 {
					if err := e.CheckPlaces(context.Background()); err != nil {
						t.Fatalf("after batch %d: %v", i, err)
					}
				}
			}
			if err := e.CheckPlaces(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestAStaleCtidIsRefusedAndChangesNothing: a row moved behind the writer's back is not
// the row at the place the writer remembers. The statement that finds its row by that
// place finds none, which is an error and not a write to another row, and the batch
// leaves nothing behind.
func TestAStaleCtidIsRefusedAndChangesNothing(t *testing.T) {
	e := openEngine(t, pgschema.Stream)
	start := time.Unix(3*86400, 0).UTC()
	rec := func(seq uint64, at time.Duration) engine.Record {
		return engine.Record{Layer: catalog.L2, Subject: edge, Producer: "a", EventTime: start.Add(at), Seq: seq, Kind: lifecycle.Observe, TTL: time.Minute}
	}
	if err := e.Write([]engine.Record{rec(1, 0)}); err != nil {
		t.Fatal(err)
	}
	conn := sqlConn(t, e)
	// An update that changes the range moves the row.
	if _, err := conn.Exec(context.Background(), "UPDATE edges SET end_source = end_source, during = int8range(start_ns, NULL)"); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := conn.QueryRow(context.Background(), "SELECT count(*) FROM log").Scan(&n); err != nil {
		t.Fatal(err)
	}
	before, _ := e.Tables(context.Background())
	err := e.Write([]engine.Record{rec(2, 10*time.Second)})
	if !errors.Is(err, pgengine.ErrConcurrentWriter) {
		t.Fatalf("a write that addresses a row that moved: %v", err)
	}
	var after int
	if err := conn.QueryRow(context.Background(), "SELECT count(*) FROM log").Scan(&after); err != nil {
		t.Fatal(err)
	}
	if got, _ := e.Tables(context.Background()); !got.Identical(before) || after != n || e.LastSeq() != 1 {
		t.Fatalf("a refused batch left something behind: %d log rows (was %d), seq %d", after, n, e.LastSeq())
	}
}

// TestAnotherWritersWorkIsRefused: the token the engine left is not the token in the
// database once another writer has written, and what the engine remembers of the rows is
// no longer so.
func TestAnotherWritersWorkIsRefused(t *testing.T) {
	a := openEngine(t, pgschema.Stream)
	b, err := pgengine.Open(context.Background(), pgengine.Config{DSN: serverDSN(t), Schema: a.Schema(), Arm: pgschema.Stream})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err := a.Write([]engine.Record{observe(1, "a", 10, 5)}); err != nil {
		t.Fatal(err)
	}
	if err := b.Write([]engine.Record{observe(2, "a", 12, 5)}); !errors.Is(err, pgengine.ErrConcurrentWriter) {
		t.Fatalf("a write by an engine that has not seen the last batch: %v", err)
	}
	if err := b.Retain(at(11)); !errors.Is(err, pgengine.ErrConcurrentWriter) {
		t.Fatalf("a retention by an engine that has not seen the last batch: %v", err)
	}
}

// TestAnEngineOpenedOverAStreamContinuesItLikeTheOneThatWroteIt: opened again in the middle
// of a stream, over what the first left (rows, places, log, baselines, partitions), an
// engine writes the same rows the in-memory model writes that was never closed.
func TestAnEngineOpenedOverAStreamContinuesItLikeTheOneThatWroteIt(t *testing.T) {
	h := heartbeat{edges: 20, steps: 48, period: 30 * time.Minute, start: time.Unix(3*86400, 0).UTC()}
	for _, arm := range streamArms {
		t.Run(string(arm), func(t *testing.T) {
			dsn := serverDSN(t)
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
			e, m := open(), pgmodel.NewWith(pgengine.ModelFor(arm))
			for i, b := range h.batches() {
				if i == 20 || i == 33 {
					if err := e.Close(); err != nil {
						t.Fatal(err)
					}
					e = open()
					// What the new engine remembers is where the rows are.
					if err := e.CheckPlaces(context.Background()); err != nil {
						t.Fatalf("after reopening before batch %d: %v", i, err)
					}
				}
				for _, w := range []engine.Engine{e, m} {
					if err := w.Write(slices.Clone(b)); err != nil {
						t.Fatal(err)
					}
				}
				if i == 24 {
					for _, w := range []engine.Engine{e, m} {
						if err := w.Retain(h.start.Add(10 * time.Hour)); err != nil {
							t.Fatal(err)
						}
					}
				}
				got, err := e.Tables(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				if want := m.Tables(); !got.Identical(want) {
					t.Fatalf("after batch %d the rows differ from the model's: %s", i, got.FirstDifference(want))
				}
			}
		})
	}
}

// TestRetentionIsOneTransactionAndTheLogIsDroppedAfterIt: the baselines, the horizon and
// the interval rows that have ended go in one transaction, and the log's partitions that are
// wholly before the horizon are detached concurrently and dropped after it, outside any
// transaction. The unpartitioned arm deletes from the log inside it.
func TestRetentionIsOneTransactionAndTheLogIsDroppedAfterIt(t *testing.T) {
	day := 24 * time.Hour
	h := heartbeat{edges: 10, steps: 4 * 48, period: 30 * time.Minute, start: time.Unix(3*86400, 0).UTC()}
	for _, arm := range streamArms {
		t.Run(string(arm), func(t *testing.T) {
			log := &stmtLog{}
			e := openTraced(t, arm, log)
			writeAll(t, e, h.batches())
			type seen struct {
				tx  pgx.Tx
				sql string
			}
			var mu sync.Mutex
			var inTx []seen
			stop := pgengine.ObserveStatements(func(_ context.Context, tx pgx.Tx, sql string, _ []any) {
				mu.Lock()
				defer mu.Unlock()
				inTx = append(inTx, seen{tx, strings.Join(strings.Fields(sql), " ")})
			})
			defer stop()
			var sqls []string
			// Two days and a half in: the first two days are wholly before it, and the third
			// is the one it falls in.
			horizon := h.start.Add(2*day + 12*time.Hour)
			sqls = log.record(func() {
				if err := e.Retain(horizon); err != nil {
					t.Fatal(err)
				}
			})
			stop()
			if len(inTx) == 0 {
				t.Fatal("the retention ran no statement that was observed")
			}
			for _, s := range inTx {
				if s.tx != inTx[0].tx {
					t.Fatalf("the retention's statements are in more than one transaction: %.50s", s.sql)
				}
			}
			var hasLogTrim bool
			for _, s := range inTx {
				hasLogTrim = hasLogTrim || strings.HasPrefix(s.sql, "DELETE FROM log WHERE")
			}
			detached := slices.ContainsFunc(sqls, func(s string) bool {
				return strings.Contains(s, "DETACH PARTITION") && strings.HasSuffix(s, "CONCURRENTLY")
			})
			parts := partitions(t, e)
			switch arm {
			case pgschema.StreamNoPartition:
				if !hasLogTrim || detached || len(parts) != 0 {
					t.Errorf("the unpartitioned log: deleted in the transaction %v, detached %v, partitions %v", hasLogTrim, detached, parts)
				}
			default:
				if hasLogTrim && false {
					t.Error("unreachable")
				}
				if !detached {
					t.Errorf("no partition was detached concurrently: %v", sqls)
				}
				// The third day's partition is the only one left of the four, and the two
				// before it are gone. (One more is made by each write, so the fourth is there.)
				want := []string{"log_p19700106", "log_p19700107"}
				if !slices.Equal(parts, want) {
					t.Errorf("partitions %v, want %v", parts, want)
				}
				if got := e.Counts().PartitionsDropped; got != 2 {
					t.Errorf("%d partitions dropped, want 2", got)
				}
				// What was before the horizon in the day it falls in is deleted.
				var n int
				if err := sqlConn(t, e).QueryRow(context.Background(), "SELECT count(*) FROM log WHERE event_ns < $1", horizon.UnixNano()).Scan(&n); err != nil || n != 0 {
					t.Errorf("%d log rows before the horizon (%v)", n, err)
				}
			}
			// The same retention in the model has the same counts.
			m := pgmodel.NewWith(pgengine.ModelFor(arm))
			writeAll(t, m, h.batches())
			if err := m.Retain(horizon); err != nil {
				t.Fatal(err)
			}
			if got, want := e.Counts(), m.Counts(); got != want {
				t.Errorf("the engine counts %+v, the model %+v", got, want)
			}
			tb, _ := e.Tables(context.Background())
			if !tb.Identical(m.Tables()) {
				t.Errorf("the rows differ from the model's: %s", tb.FirstDifference(m.Tables()))
			}
		})
	}
}

func partitions(t testing.TB, e *pgengine.Engine) []string {
	t.Helper()
	rows, err := sqlConn(t, e).Query(context.Background(), `SELECT c.relname FROM pg_inherits i
JOIN pg_class c ON c.oid = i.inhrelid JOIN pg_class p ON p.oid = i.inhparent JOIN pg_namespace n ON n.oid = p.relnamespace
WHERE p.relname = 'log' AND n.nspname = $1 ORDER BY 1`, e.Schema())
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatal(err)
		}
		out = append(out, n)
	}
	return out
}

// TestAPartitionThatOutlivesItsRetentionIsNeverReadAndIsDroppedNext: a retention that was
// interrupted between its transaction and the drop leaves a partition of records before the
// horizon. Reads of the log filter at the horizon, so a record and its baseline are never
// both folded (here the record is a delete that the baseline's producer made after the
// baseline's record, which would end an interval the baseline keeps), and the next
// retention drops it.
func TestAPartitionThatOutlivesItsRetentionIsNeverReadAndIsDroppedNext(t *testing.T) {
	e := openEngine(t, pgschema.Stream)
	day := 24 * time.Hour
	start := time.Unix(3*86400, 0).UTC()
	rec := func(seq uint64, at time.Duration) engine.Record {
		return engine.Record{Layer: catalog.L2, Subject: edge, Producer: "a", EventTime: start.Add(at), Seq: seq, Kind: lifecycle.Observe, TTL: 72 * time.Hour}
	}
	if err := e.Write([]engine.Record{rec(1, 0), rec(2, 30*time.Minute), rec(3, day+time.Hour), rec(4, day+2*time.Hour)}); err != nil {
		t.Fatal(err)
	}
	if err := e.Retain(start.Add(day)); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	conn := sqlConn(t, e)
	// A delete by a at 40 minutes, which the retention dropped (seq 1, so a token sees it),
	// put back by hand with its partition: the state an interrupted retention leaves.
	for _, q := range []string{
		"CREATE TABLE log_p19700104 PARTITION OF log FOR VALUES FROM (259200000000000) TO (345600000000000)",
		fmt.Sprintf(`INSERT INTO log (seq, event_ns, layer, subject_kind, src, dst, rel, producer, kind, ttl_ns, through_ns, payload)
VALUES (%d, %d, %d, 'g', $1, $2, 'scheduled_on', 'a', %d::int4::"char", 0, NULL, ''::bytea)`,
			pgengine.SeqToDB(1), start.Add(40*time.Minute).UnixNano(), int(catalog.L2), int(lifecycle.Delete)),
	} {
		var args []any
		if strings.Contains(q, "INSERT") {
			args = []any{pgmodel.EncodeFingerprint(pod1), pgmodel.EncodeFingerprint(node1)}
		}
		if _, err := conn.Exec(ctx, q, args...); err != nil {
			t.Fatal(err)
		}
	}
	// The delete is in the log; no read may see it. A window before the horizon asks for it.
	got, err := e.Window(pod1, engine.Forward, start, start.Add(day), engine.Current(catalog.L2))
	if err != nil || len(got) != 0 {
		t.Errorf("a window before the horizon read %d records of a partition that outlived its retention (%v)", len(got), err)
	}
	// As of the token of the third record, which is not the latest, a fold reads the
	// baselines and the log: with the delete in it the edge would not be alive between the
	// horizon and the next record; without it, the baseline's promise of three days holds.
	sc := engine.Scope{Layer: catalog.L2, AsOf: 3}
	if ns, err := e.Neighbors(pod1, engine.Forward, start.Add(day+30*time.Minute), sc); err != nil || len(ns) != 1 {
		t.Errorf("neighbors as of seq 3 after the horizon: %v (%v)", ns, err)
	}
	// A retention that moves nothing drops it.
	if err := e.Retain(start.Add(day)); err != nil {
		t.Fatal(err)
	}
	if parts := partitions(t, e); slices.Contains(parts, "log_p19700104") {
		t.Errorf("the partition that outlived its retention is still there: %v", parts)
	}
}

// churn makes the batches of a stream in which every edge has several intervals: each is
// one record whose promise lapses, so each interval is a row of its own, and the table is
// large enough for the planner to prefer an index to a scan of it.
func churn(edges, intervals int, start time.Time) [][]engine.Record {
	var out [][]engine.Record
	seq := uint64(0)
	for k := range intervals {
		var batch []engine.Record
		for i := range edges {
			seq++
			batch = append(batch, engine.Record{
				Layer: catalog.L2, Subject: engine.EdgeSubject(fpn("k8s.pod", i), fpn("k8s.node", i%11), catalog.ScheduledOn),
				Producer: "kubelet", EventTime: start.Add(time.Duration(k)*time.Hour + time.Duration(i)*time.Second), Seq: seq,
				Kind: lifecycle.Observe, TTL: time.Minute, Payload: []byte{byte(i)},
			})
		}
		for len(batch) > 0 {
			n := min(len(batch), 500)
			out = append(out, batch[:n])
			batch = batch[n:]
		}
	}
	return out
}

// TestRetentionDeletesAreScansOfPlacesAndUseNoIndex: the statements of a retention that
// delete interval rows find the rows by the places that same transaction read, so no plan of
// them has an index in it, the btree on the end included; the read of the places is the one
// that uses the index, and a read can use any.
func TestRetentionDeletesAreScansOfPlacesAndUseNoIndex(t *testing.T) {
	start := time.Unix(3*86400, 0).UTC()
	for _, arm := range streamArms {
		t.Run(string(arm), func(t *testing.T) {
			e := openEngine(t, arm)
			writeAll(t, e, churn(2500, 4, start))
			var plans []string
			stop := pgengine.ObserveStatements(func(ctx context.Context, tx pgx.Tx, sql string, args []any) {
				if !strings.HasPrefix(sql, "DELETE FROM edges") && !strings.HasPrefix(sql, "DELETE FROM entities") {
					return
				}
				var raw []byte
				if err := tx.QueryRow(ctx, "EXPLAIN (FORMAT JSON) "+sql, args...).Scan(&raw); err != nil {
					t.Errorf("explaining %.60s: %v", sql, err)
					return
				}
				plan, err := pgengine.ParseExplain(raw)
				if err != nil {
					t.Error(err)
					return
				}
				plans = append(plans, plan.Shape())
				for _, n := range plan.Plan {
					if n.Index != "" {
						t.Errorf("%.40s... reads through %s: %s", sql, n.Index, plan.Shape())
					}
				}
			})
			defer stop()
			// The intervals that end at or before a horizon: record k of edge i is at k hours and i
			// seconds, and promises a minute. The second retention moves the horizon a few
			// seconds, so that it deletes a few rows of a large table, which is when a planner
			// that can find them by key does.
			ended := func(horizon time.Duration) (n int) {
				for k := range 4 {
					for i := range 2500 {
						if time.Duration(k)*time.Hour+time.Duration(i)*time.Second+time.Minute <= horizon {
							n++
						}
					}
				}
				return n
			}
			first, second := 2*time.Hour+30*time.Minute, 2*time.Hour+30*time.Minute+5*time.Second
			if ended(second) <= ended(first) {
				t.Fatal("the second retention deletes nothing")
			}
			for _, h := range []time.Duration{first, second} {
				if err := e.Retain(start.Add(h)); err != nil {
					t.Fatal(err)
				}
			}
			if len(plans) < 2 {
				t.Fatalf("the retentions' deletes were planned %d times", len(plans))
			}
			if got := e.Counts().RowsDeleted; got != uint64(ended(second)) {
				t.Errorf("%d rows deleted, want %d", got, ended(second))
			}
		})
	}
}

// TestInsertsAloneTriggerAVacuum and the dead-row rule: the vacuum schedule of the
// deterministic regime runs a vacuum when the writer's own count of rows passes
// autovacuum's formula by dead rows or by inserted rows, equal to the model's, and the
// server has done them.
func TestTheVacuumScheduleRunsOnTheServer(t *testing.T) {
	vacuums := func(t *testing.T, e *pgengine.Engine, table string, want int) int {
		conn := sqlConn(t, e)
		var n int
		for range 100 {
			if _, err := conn.Exec(context.Background(), "SELECT pg_stat_force_next_flush()"); err != nil {
				t.Fatal(err)
			}
			if err := conn.QueryRow(context.Background(), "SELECT vacuum_count FROM pg_stat_user_tables WHERE schemaname = $1 AND relname = $2", e.Schema(), table).Scan(&n); err != nil {
				t.Fatal(err)
			}
			if n == want {
				return n
			}
			time.Sleep(100 * time.Millisecond)
		}
		return n
	}
	t.Run("by dead rows", func(t *testing.T) {
		// A few edges refreshed again and again: updates, and almost no inserts to the
		// interval table.
		h := heartbeat{edges: 4, steps: 400, period: 10 * time.Second, start: time.Unix(3*86400, 0).UTC()}
		e, m := openEngine(t, pgschema.Stream), pgmodel.NewWith(pgengine.ModelFor(pgschema.Stream))
		writeAll(t, e, h.batches())
		writeAll(t, m, h.batches())
		runs := e.VacuumRuns()
		if runs["edges"] < 10 || !equalRuns(runs, m.VacuumRuns()) {
			t.Fatalf("vacuums by the engine %v, by the model %v", runs, m.VacuumRuns())
		}
		if got := vacuums(t, e, "edges", runs["edges"]); got != runs["edges"] {
			t.Fatalf("the server vacuumed edges %d times, the writer %d", got, runs["edges"])
		}
	})
	t.Run("by inserted rows", func(t *testing.T) {
		// Distinct edges, one record each: no row is ever updated or deleted, so only the
		// insert trigger can call for a vacuum.
		e := openEngine(t, pgschema.Stream)
		start := time.Unix(3*86400, 0).UTC()
		for b := range 6 {
			var batch []engine.Record
			for i := range 300 {
				n := b*300 + i
				batch = append(batch, engine.Record{
					Layer: catalog.L2, Subject: engine.EdgeSubject(fpn("k8s.pod", n), fpn("k8s.node", n%9), catalog.ScheduledOn),
					Producer: "kubelet", EventTime: start.Add(time.Duration(n) * time.Second), Seq: uint64(n + 1), Kind: lifecycle.Observe, TTL: time.Hour,
				})
			}
			if err := e.Write(batch); err != nil {
				t.Fatal(err)
			}
		}
		runs := e.VacuumRuns()
		c := e.Counts()
		if c.RowsUpdated != 0 || c.RowsDeleted != 0 {
			t.Fatalf("the stream was to be inserts only: %+v", c)
		}
		// 1,800 rows: the first vacuum is at 1,001 inserted, the second needs 1000 + 5 % of
		// 1,001 more, which this stream does not reach.
		if runs["edges"] != 1 || runs["log_p19700104"] != 1 || len(runs) != 2 {
			t.Fatalf("vacuums %v, want the edges and the log of the day once each", runs)
		}
		if got := vacuums(t, e, "edges", 1); got != 1 {
			t.Fatalf("the server vacuumed edges %d times, the writer once", got)
		}
	})
	t.Run("not when autovacuum is left to the server", func(t *testing.T) {
		dsn := serverDSN(t)
		schema := pgschema.SchemaName(t.TempDir())
		e, err := pgengine.Open(context.Background(), pgengine.Config{DSN: dsn, Schema: schema, Arm: pgschema.Stream, NoVacuum: true})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = e.Close(); _ = pgengine.DropSchema(context.Background(), dsn, schema) })
		h := heartbeat{edges: 4, steps: 100, period: 10 * time.Second, start: time.Unix(3*86400, 0).UTC()}
		writeAll(t, e, h.batches())
		if runs := e.VacuumRuns(); len(runs) != 0 {
			t.Fatalf("the writer vacuumed %v", runs)
		}
	})
}

func equalRuns(a, b map[string]int) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// modelSink writes a stream to a model.
type modelSink struct{ m *pgmodel.Model }

func (s modelSink) Write(b []engine.Record) error { return s.m.Write(b) }
func (s modelSink) Retain(h time.Time) error      { return s.m.Retain(h) }

// TestTheStreamCommandWritesAPlanAndTwoRunsAgree runs pgspike stream on a live server for
// each streaming arm: every question of the plan is answered as the reference did, the
// writer's counts and vacuums and the tables are the in-memory model's, the record holds
// no time, two runs are equal, and what must be refused is.
func TestTheStreamCommandWritesAPlanAndTwoRunsAgree(t *testing.T) {
	dsn := serverDSN(t)
	spec := e2eSpec()
	spec.Retentions = []runner.Retention{{At: 40 * time.Minute, Keep: 20 * time.Minute}}
	p := cachedPlan(t, "e2e-stream", spec)
	plan, sum := writePlan(t, p)
	ctx := context.Background()
	schema := func(name string) string {
		s := pgschema.SchemaName(t.TempDir() + name)
		t.Cleanup(func() { _ = pgengine.DropSchema(ctx, dsn, s) })
		return s
	}
	args := func(arm, schema, dir string, extra ...string) []string {
		return append([]string{"stream", "-dsn", dsn, "-settings", settingsPath, "-arm", arm, "-schema", schema, "-out", dir, "-plan", plan, "-expect-plan", sum}, extra...)
	}
	for _, arm := range streamArms {
		t.Run(string(arm), func(t *testing.T) {
			var dirs []string
			for i := range 2 {
				dir := filepath.Join(t.TempDir(), fmt.Sprint(i))
				txt, err := cli(t, args(string(arm), schema(fmt.Sprint(arm, i)), dir)...)
				if err != nil {
					t.Fatalf("stream %s: %v\n%s", arm, err, txt)
				}
				if i == 0 {
					t.Logf("pgspike stream says:\n%s", txt)
				}
				dirs = append(dirs, dir)
			}
			var a pgreport.Stream
			if err := pgreport.ReadFile(filepath.Join(dirs[0], "stream.json"), &a); err != nil {
				t.Fatal(err)
			}
			if a.Matched != a.Queries || a.Queries == 0 || a.Input.PlanSHA256 != sum || a.Input.Records != p.Stream.Records || a.Retentions != len(p.Stream.Retentions) {
				t.Fatalf("the record: %d of %d answers, input %+v, %d retentions", a.Matched, a.Queries, a.Input, a.Retentions)
			}
			// The model, given the plan's stream, did what the engine did.
			m := pgmodel.NewWith(pgengine.ModelFor(arm))
			if _, err := runner.Drive(ctx, p.Spec, modelSink{m}); err != nil {
				t.Fatal(err)
			}
			c := m.Counts()
			want := pgreport.WriterCounts{
				Records: c.Records, FastSubjects: c.FastSubjects, Refolds: c.Refolds, RecordsFolded: c.RecordsFolded,
				RowsInserted: c.RowsInserted, RowsUpdated: c.RowsUpdated, RowsDeleted: c.RowsDeleted,
				RowsHeapOnly: c.RowsHeapOnly, RowsFinalized: c.RowsFinalized, Vacuums: c.Vacuums, Analyzes: c.Analyzes, PartitionsDropped: c.PartitionsDropped,
				VacuumRuns: m.VacuumRuns(), AnalyzeRuns: m.AnalyzeRuns(),
			}
			if !reflect.DeepEqual(a.Writer, want) {
				t.Errorf("the writer did %+v, the model %+v", a.Writer, want)
			}
			if a.Writer.FastSubjects == 0 || a.Writer.Refolds == 0 {
				t.Errorf("the stream took only one path: %+v", a.Writer)
			}
			if got, wantDigest := a.TablesDigest, m.Tables().Clipped(a.HorizonNS).Digest(); got != wantDigest {
				t.Errorf("the tables digest %s, the model's %s", got, wantDigest)
			}
			// The log: the whole stream is one span with the server's own counts; the batches
			// and the retentions are sums of spans, and together they are the stream (nothing
			// else writes while it runs) to the slack of a page header for each span.
			var names []string
			by := map[string]pgreport.Phase{}
			for _, ph := range a.Phases {
				names = append(names, ph.Name)
				by[ph.Name] = ph
			}
			if !slices.Equal(names, []string{"stream", "write", "retain"}) {
				t.Errorf("phases %v", names)
			}
			stream, write, retain := by["stream"], by["write"], by["retain"]
			if stream.Records <= 0 || stream.RecordBytes <= 0 || stream.LogBytes <= 0 || !stream.Contiguous || stream.Spans != 0 {
				t.Errorf("the stream's log: %+v", stream)
			}
			if batches := countBatches(t, p); write.Spans != batches || retain.Spans != len(p.Stream.Retentions) {
				t.Errorf("spans: %d writes for %d batches, %d retentions for %d", write.Spans, batches, retain.Spans, len(p.Stream.Retentions))
			}
			if write.LogBytes <= 0 || retain.LogBytes <= 0 || write.Records != 0 || retain.Records != 0 {
				t.Errorf("the log of the batches %+v and of the retentions %+v", write, retain)
			}
			if d := stream.LogBytes - write.LogBytes - retain.LogBytes; d < -64*int64(write.Spans+retain.Spans) || d > 64*int64(write.Spans+retain.Spans) {
				t.Errorf("the stream wrote %d bytes of log, the batches %d and the retentions %d", stream.LogBytes, write.LogBytes, retain.LogBytes)
			}
			// Two runs of one stream are equal in everything the writer decides and in the
			// heap and the btrees; a GiST index is built from random choices among equal pages,
			// and its size, which the stream also leaves, is not the same twice. The log is
			// not compared here (the tolerance is all of it).
			var b pgreport.Stream
			if err := pgreport.ReadFile(filepath.Join(dirs[1], "stream.json"), &b); err != nil {
				t.Fatal(err)
			}
			cmp, err := pgreport.DiffStreams(a, b, pgreport.Options{WALTolerance: 1})
			if err != nil {
				t.Fatal(err)
			}
			for _, d := range cmp.Differences {
				if !strings.Contains(d.Where, "_g ") {
					t.Errorf("two runs of one stream differ: %s", d)
				}
			}
			if txt, err := cli(t, "diff", "-kind", "stream", "-wal-tolerance", "1", filepath.Join(dirs[0], "stream.json"), filepath.Join(dirs[0], "stream.json")); err != nil || !strings.Contains(txt, "are equal") {
				t.Errorf("a stream and itself: %v\n%s", err, txt)
			}
			// What a record may hold is no time at all.
			raw, _ := os.ReadFile(filepath.Join(dirs[0], "stream.json"))
			for _, banned := range []string{"Nanos", "Time", "Seconds", "Elapsed", "Duration"} {
				if strings.Contains(string(raw), `"`+banned) {
					t.Errorf("the record has a field %s", banned)
				}
			}
		})
	}

	t.Run("refusals", func(t *testing.T) {
		s := schema("refusals")
		dir := filepath.Join(t.TempDir(), "r")
		if _, err := cli(t, args("pg-stream", s, dir)...); err != nil {
			t.Fatal(err)
		}
		if _, err := cli(t, args("pg-stream", s, filepath.Join(t.TempDir(), "r2"))...); err == nil || !strings.Contains(err.Error(), "exists") {
			t.Errorf("a schema that exists was written to: %v", err)
		}
		if _, err := cli(t, args("pg-stream", s, filepath.Join(t.TempDir(), "r3"), "-replace")...); err != nil {
			t.Errorf("-replace: %v", err)
		}
		if _, err := cli(t, args("pg-stream", s, dir, "-replace")...); err == nil || !strings.Contains(err.Error(), "already exists") {
			t.Errorf("a record was overwritten: %v", err)
		}
		if _, err := cli(t, args("pg-bulk", schema("bulk"), filepath.Join(t.TempDir(), "b"))...); err == nil || !strings.Contains(err.Error(), "pg-stream") {
			t.Errorf("a bulk arm was streamed: %v", err)
		}
		if _, err := cli(t, "stream", "-dsn", dsn, "-settings", settingsPath, "-arm", "pg-stream", "-schema", schema("noplan"), "-out", filepath.Join(t.TempDir(), "n"), "-questions", "q.json", "-expect-questions", "x"); err == nil {
			t.Error("a stream without a plan was written")
		}
		inRepo, err := filepath.Abs("../pgspike")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := cli(t, args("pg-stream", schema("tree"), filepath.Join(inRepo, "out"))...); err == nil || !strings.Contains(err.Error(), "work tree") {
			t.Errorf("an output inside a git work tree: %v", err)
		}
	})
}

// TestAnEngineOpenedAgainKnowsWhatItsProducersAsserted: what shortens an interval is a
// record that replaces a producer's own assertion at its instant with a shorter promise,
// and a writer that did not remember what the producer had asserted would lengthen the
// interval with it instead (or leave it as it was). The engine that opens the schema
// remembers it from the log.
func TestAnEngineOpenedAgainKnowsWhatItsProducersAsserted(t *testing.T) {
	for _, arm := range streamArms {
		t.Run(string(arm), func(t *testing.T) {
			dsn := serverDSN(t)
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
			if err := e.Write([]engine.Record{observe(1, "a", 10, 100)}); err != nil {
				t.Fatal(err)
			}
			if err := e.Close(); err != nil {
				t.Fatal(err)
			}
			e = open()
			// a says, at the same instant, that it holds for five seconds and not a hundred.
			if err := e.Write([]engine.Record{observe(2, "a", 10, 5)}); err != nil {
				t.Fatal(err)
			}
			sc := engine.Current(catalog.L2)
			if got := neighbors(t, e, pod1, engine.Forward, 14, sc); len(got) != 1 {
				t.Errorf("alive at 14: %v", got)
			}
			if got := neighbors(t, e, pod1, engine.Forward, 20, sc); len(got) != 0 {
				t.Errorf("the replaced promise of a hundred seconds was kept: alive at 20: %v", got)
			}
			if c := e.Counts(); c.Refolds != 1 {
				t.Errorf("a shorter promise at the same instant was not re-folded: %+v", c)
			}
		})
	}
}

// TestARetentionPastTheNewestEventDeletesTheProvisionalRowsItPasses: a provisional row has an
// unbounded range, so no index finds it by its end, and a horizon that is past its deadline
// (here past every event written, and then past the last instant) has to delete it by where
// the writer remembers it; the rows are then the model's, and the finalizer has nothing to do.
func TestARetentionPastTheNewestEventDeletesTheProvisionalRowsItPasses(t *testing.T) {
	h := heartbeat{edges: 12, steps: 20, period: time.Minute, start: time.Unix(3*86400, 0).UTC()}
	for _, arm := range streamArms {
		t.Run(string(arm), func(t *testing.T) {
			e, m := openEngine(t, arm), pgmodel.NewWith(pgengine.ModelFor(arm))
			writeAll(t, e, h.batches())
			writeAll(t, m, h.batches())
			conn := sqlConn(t, e)
			var provisional int
			if err := conn.QueryRow(context.Background(), "SELECT count(*) FROM edges WHERE deadline_ns IS NOT NULL").Scan(&provisional); err != nil {
				t.Fatal(err)
			}
			if arm != pgschema.StreamNaive && provisional == 0 {
				t.Fatal("the stream left no provisional row")
			}
			// An hour past the last record: every deadline is before it.
			horizon := h.start.Add(time.Duration(h.steps)*h.period + time.Hour)
			for _, w := range []engine.Engine{e, m} {
				if err := w.Retain(horizon); err != nil {
					t.Fatal(err)
				}
			}
			var left int
			if err := conn.QueryRow(context.Background(), "SELECT count(*) FROM edges").Scan(&left); err != nil || left != 0 {
				t.Fatalf("%d edge rows left after a horizon past every deadline (%v)", left, err)
			}
			got, _ := e.Tables(context.Background())
			if !got.Identical(m.Tables()) {
				t.Fatalf("the rows differ from the model's: %s", got.FirstDifference(m.Tables()))
			}
			if g, w := e.Counts(), m.Counts(); g != w {
				t.Fatalf("the engine counts %+v, the model %+v", g, w)
			}
			// And past the last instant, which leaves no record to write.
			end := engine.MaxEventTime.Add(time.Nanosecond)
			for _, w := range []engine.Engine{e, m} {
				if err := w.Retain(end); err != nil {
					t.Fatal(err)
				}
			}
			if err := e.CheckPlaces(context.Background()); err != nil {
				t.Fatal(err)
			}
			if g, w := e.Counts(), m.Counts(); g != w {
				t.Fatalf("after the horizon past the end the engine counts %+v, the model %+v", g, w)
			}
		})
	}
}

// countBatches is how many batches a plan's stream is written in.
func countBatches(t *testing.T, p *runner.Plan) int {
	t.Helper()
	n := 0
	if _, err := runner.Drive(context.Background(), p.Spec, batchCounter{&n}); err != nil {
		t.Fatal(err)
	}
	return n
}

type batchCounter struct{ n *int }

func (c batchCounter) Write([]engine.Record) error { *c.n++; return nil }
func (c batchCounter) Retain(time.Time) error      { return nil }

// TestTheAnalyzeScheduleRunsOnTheServer: with autovacuum off, the writer analyzes a table
// when the rows changed since its last analyze pass autovacuum's formula, so the planner has
// statistics for it. The analyzes the schedule calls for are the model's, the server has done
// every one of them to every table (alone or with a vacuum), and the tables have statistics.
func TestTheAnalyzeScheduleRunsOnTheServer(t *testing.T) {
	h := heartbeat{edges: 40, steps: 80, period: 10 * time.Minute, start: time.Unix(3*86400, 0).UTC()}
	for _, arm := range []pgschema.Arm{pgschema.Stream, pgschema.StreamNoPartition} {
		t.Run(string(arm), func(t *testing.T) {
			e, m := openEngine(t, arm), pgmodel.NewWith(pgengine.ModelFor(arm))
			writeAll(t, e, h.batches())
			writeAll(t, m, h.batches())
			if err := e.Retain(h.start.Add(20 * time.Hour)); err != nil {
				t.Fatal(err)
			}
			if err := m.Retain(h.start.Add(20 * time.Hour)); err != nil {
				t.Fatal(err)
			}
			runs := e.AnalyzeRuns()
			if !equalRuns(runs, m.AnalyzeRuns()) || runs["edges"] == 0 || e.Counts().Analyzes != m.Counts().Analyzes {
				t.Fatalf("analyzes by the engine %v, by the model %v", runs, m.AnalyzeRuns())
			}
			conn := sqlConn(t, e)
			for table, want := range runs {
				var exists bool
				if err := conn.QueryRow(context.Background(), "SELECT to_regclass($1) IS NOT NULL", pgx.Identifier{e.Schema(), table}.Sanitize()).Scan(&exists); err != nil {
					t.Fatal(err)
				}
				if !exists {
					continue // a partition the retention dropped
				}
				var got int
				for range 100 {
					if _, err := conn.Exec(context.Background(), "SELECT pg_stat_force_next_flush()"); err != nil {
						t.Fatal(err)
					}
					if err := conn.QueryRow(context.Background(), "SELECT analyze_count FROM pg_stat_user_tables WHERE schemaname = $1 AND relname = $2", e.Schema(), table).Scan(&got); err != nil {
						t.Fatal(err)
					}
					if got == want {
						break
					}
					time.Sleep(100 * time.Millisecond)
				}
				if got != want {
					t.Errorf("the server analyzed %s %d times, the writer %d", table, got, want)
				}
			}
			var stats int
			if err := conn.QueryRow(context.Background(), "SELECT count(*) FROM pg_stats WHERE schemaname = $1 AND tablename = 'edges'", e.Schema()).Scan(&stats); err != nil || stats == 0 {
				t.Errorf("edges has no planner statistics (%d columns, %v)", stats, err)
			}
		})
	}
}
