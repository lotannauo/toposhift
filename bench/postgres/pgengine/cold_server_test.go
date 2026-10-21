package pgengine_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/lotannauo/toposhift/bench/postgres/pgengine"
	"github.com/lotannauo/toposhift/bench/postgres/pgmodel"
	"github.com/lotannauo/toposhift/bench/postgres/pgschema"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/runner"
)

// firstQuery is the first question of the plan that the test accepts.
func firstQuery(t testing.TB, p *runner.Plan, accept func(runner.Query) bool) runner.Query {
	t.Helper()
	for _, q := range p.Queries {
		if accept(q) {
			return q
		}
	}
	t.Fatal("the plan has no such question")
	return runner.Query{}
}

// statementsOf asks a question of the engine and returns the statements it ran.
func statementsOf(t testing.TB, e *pgengine.Engine, rec *pgengine.Recorder, q runner.Query) []pgengine.Statement {
	t.Helper()
	rec.Start()
	_, err := runner.Ask(e, q)
	stmts := rec.Stop()
	if err != nil {
		t.Fatal(err)
	}
	return stmts
}

func coldOnce(t testing.TB, conn *pgx.Conn, schema string, evict pgengine.Evictor, stmts []pgengine.Statement) ([]pgengine.ColdRun, error) {
	t.Helper()
	ctx := context.Background()
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	return pgengine.ColdExplain(ctx, tx, schema, evict, stmts)
}

// TestAColdReadReadsBlocksAndCountsTheSameOnRepeat is the cold-read protocol: after the
// buffers of the schema are emptied a question has blocks to read, and emptying them
// again gives the same counters; a read that could not have been cold is refused.
func TestAColdReadReadsBlocksAndCountsTheSameOnRepeat(t *testing.T) {
	p := tinyPlan(t)
	b := loadBulk(t, pgschema.Bulk, p, nil)
	rec := &pgengine.Recorder{}
	e := b.open(t, pgschema.Bulk, rec)
	conn := b.conn(t)
	ctx := context.Background()
	if _, err := conn.Exec(ctx, "CHECKPOINT"); err != nil {
		t.Fatal(err)
	}

	for _, q := range []runner.Query{
		firstQuery(t, p, func(q runner.Query) bool { return q.Op == runner.OpNeighbors && q.Age == runner.AgeNow }),
		firstQuery(t, p, func(q runner.Query) bool { return q.Op == runner.OpBatch }),
		firstQuery(t, p, func(q runner.Query) bool { return q.Op == runner.OpAlive && q.Age == runner.AgeNow }),
		firstQuery(t, p, func(q runner.Query) bool { return q.Op == runner.OpWindow }),
		firstQuery(t, p, func(q runner.Query) bool { return q.Op == runner.OpNeighbors && q.Age == runner.AgeOldToken }),
	} {
		stmts := statementsOf(t, e, rec, q)
		if len(stmts) == 0 {
			t.Fatalf("%s ran no statement", q.Name())
		}
		for _, s := range stmts {
			if strings.HasPrefix(strings.ToLower(strings.TrimSpace(s.SQL)), "begin") || strings.Contains(strings.ToLower(s.SQL), "rollback") {
				t.Errorf("%s: transaction control was recorded as a statement: %s", q.Name(), s.SQL)
			}
		}
		first, err := coldOnce(t, conn, b.schema, nil, stmts)
		if err != nil {
			t.Fatalf("%s: %v", q.Name(), err)
		}
		var read int64
		for _, r := range first {
			read += r.Counters.SharedRead
		}
		if read == 0 {
			t.Errorf("%s: a cold read read no block", q.Name())
		}
		again, err := coldOnce(t, conn, b.schema, nil, stmts)
		if err != nil {
			t.Fatalf("%s: %v", q.Name(), err)
		}
		for i := range first {
			a, b := first[i].Counters, again[i].Counters
			if a.SharedHit != b.SharedHit || a.SharedRead != b.SharedRead || a.IndexEntries != b.IndexEntries || a.RowsReturned != b.RowsReturned || a.Shape() != b.Shape() {
				t.Errorf("%s statement %d: %+v, then %+v", q.Name(), i, a, b)
			}
		}
	}
}

// TestTheStatementsOfAReadAreTheEnginesOwn: what is explained is what the engine ran.
func TestTheStatementsOfAReadAreTheEnginesOwn(t *testing.T) {
	p := tinyPlan(t)
	b := loadBulk(t, pgschema.Bulk, p, nil)
	rec := &pgengine.Recorder{}
	e := b.open(t, pgschema.Bulk, rec)

	q := firstQuery(t, p, func(q runner.Query) bool {
		return q.Op == runner.OpNeighbors && q.Age == runner.AgeNow && q.Dir == engine.Reverse
	})
	ns, _ := pgmodel.ClampInstant(q.At)
	stmts := statementsOf(t, e, rec, q)
	if len(stmts) != 1 || stmts[0].SQL != e.NeighborsSQL(q.Dir, ns) || pgengine.FoldStatement(stmts[0].SQL) {
		t.Errorf("a read of the latest token: %d statements %v", len(stmts), stmts)
	}
	old := firstQuery(t, p, func(q runner.Query) bool { return q.Op == runner.OpNeighbors && q.Age == runner.AgeOldToken })
	stmts = statementsOf(t, e, rec, old)
	folds := 0
	for _, s := range stmts {
		if pgengine.FoldStatement(s.SQL) {
			folds++
		}
	}
	if len(stmts) != 2 || folds != 1 || !strings.Contains(stmts[0].SQL, "last_seq") {
		t.Errorf("a read as of an old token ran %d statements, %d of them folds of the log: %v", len(stmts), folds, stmts)
	}
	batch := firstQuery(t, p, func(q runner.Query) bool { return q.Op == runner.OpBatch })
	if stmts := statementsOf(t, e, rec, batch); len(stmts) != 1 || pgengine.FoldStatement(stmts[0].SQL) {
		t.Errorf("a batch at the latest token: %v", stmts)
	}
	// Nothing is kept when no recording is on.
	if _, err := runner.Ask(e, q); err != nil {
		t.Fatal(err)
	}
	if got := rec.Stop(); len(got) != 0 {
		t.Errorf("%d statements recorded while not recording", len(got))
	}
}

// TestAReadThatWasNotColdIsRefused: the buffers are emptied one by one and counted, and
// a read that reads nothing from storage is not cold whatever the eviction said.
func TestAReadThatWasNotColdIsRefused(t *testing.T) {
	p := tinyPlan(t)
	b := loadBulk(t, pgschema.Bulk, p, nil)
	rec := &pgengine.Recorder{}
	e := b.open(t, pgschema.Bulk, rec)
	conn := b.conn(t)
	ctx := context.Background()
	q := firstQuery(t, p, func(q runner.Query) bool { return q.Op == runner.OpNeighbors && q.Age == runner.AgeNow })
	stmts := statementsOf(t, e, rec, q)

	// Warm: the engine has just read these pages, and the explain has too.
	if _, err := coldOnce(t, conn, b.schema, nil, stmts); err != nil {
		t.Fatal(err)
	}
	var skipped int
	noEvict := func(context.Context, pgengine.Querier, string) (int, error) { skipped++; return 0, nil }
	if _, err := coldOnce(t, conn, b.schema, noEvict, stmts); !errors.Is(err, pgengine.ErrNotCold) || skipped != 1 {
		t.Errorf("a read with the eviction skipped: %v (skipped %d)", err, skipped)
	}
	// The eviction is real: it empties the schema, and says how many buffers it took.
	if _, err := coldOnce(t, conn, b.schema, nil, stmts); err != nil {
		t.Fatal(err)
	}
	n, err := pgengine.EvictSchema(ctx, conn, b.schema)
	if err != nil || n == 0 {
		t.Errorf("evicting a schema that was read: %d buffers, %v", n, err)
	}
	n, err = pgengine.EvictSchema(ctx, conn, b.schema)
	if err != nil || n != 0 {
		t.Errorf("evicting it again: %d buffers, %v", n, err)
	}
	// A buffer that another session has pinned cannot be evicted, and the schema is then
	// not cold: a cursor holds the page it is on.
	other := plainConn(t)
	if _, err := other.Exec(ctx, "SET search_path TO "+quoted(b.schema)+", public"); err != nil {
		t.Fatal(err)
	}
	tx, err := other.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, "DECLARE held CURSOR FOR SELECT * FROM log"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "FETCH 1 FROM held"); err != nil {
		t.Fatal(err)
	}
	if _, err := pgengine.EvictSchema(ctx, conn, b.schema); !errors.Is(err, pgengine.ErrNotCold) {
		t.Errorf("evicting a schema with a page pinned by another session: %v", err)
	}
	if _, err := coldOnce(t, conn, b.schema, nil, stmts); !errors.Is(err, pgengine.ErrNotCold) {
		t.Errorf("a cold read beside a pinned page: %v", err)
	}
	_ = tx.Rollback(ctx)
	// A statement the server rejects is an error, not a cold read of nothing.
	if _, err := coldOnce(t, conn, b.schema, nil, []pgengine.Statement{{SQL: "SELECT * FROM there_is_no_table"}}); err == nil {
		t.Error("a statement that cannot run was explained")
	}
	if _, err := pgengine.ColdExplain(ctx, conn, b.schema, nil, nil); err == nil {
		t.Error("a read with no statement was explained")
	}
}

// TestRecordExplainFixtures records the plans the server prints for the statements of
// every kind of read, on both index sets, from a tiny synthetic plan, into
// testdata/explain. It runs only when TOPOSHIFT_PG_RECORD_FIXTURES names the directory;
// the fixtures are then frozen, and TestExplainFixtures reads them with no server.
func TestRecordExplainFixtures(t *testing.T) {
	dir := os.Getenv("TOPOSHIFT_PG_RECORD_FIXTURES")
	if dir == "" {
		t.Skip("TOPOSHIFT_PG_RECORD_FIXTURES is not set")
	}
	p := tinyPlan(t)
	kinds := []struct {
		name   string
		accept func(runner.Query) bool
	}{
		{"neighbors-forward", func(q runner.Query) bool {
			return q.Op == runner.OpNeighbors && q.Age == runner.AgeNow && q.Dir == engine.Forward
		}},
		{"neighbors-reverse", func(q runner.Query) bool {
			return q.Op == runner.OpNeighbors && q.Age == runner.AgeNow && q.Dir == engine.Reverse
		}},
		{"batch", func(q runner.Query) bool { return q.Op == runner.OpBatch && len(q.Fps) > 1 }},
		{"alive", func(q runner.Query) bool { return q.Op == runner.OpAlive && q.Age == runner.AgeNow }},
		{"window", func(q runner.Query) bool { return q.Op == runner.OpWindow }},
		{"fold-neighbors", func(q runner.Query) bool { return q.Op == runner.OpNeighbors && q.Age == runner.AgeOldToken }},
		{"fold-alive", func(q runner.Query) bool { return q.Op == runner.OpAlive && q.Age == runner.AgeOldToken }},
	}
	for _, arm := range []pgschema.Arm{pgschema.Bulk, pgschema.BulkB} {
		b := loadBulk(t, arm, p, nil)
		rec := &pgengine.Recorder{}
		e := b.open(t, arm, rec)
		conn := b.conn(t)
		if _, err := conn.Exec(context.Background(), "CHECKPOINT"); err != nil {
			t.Fatal(err)
		}
		for _, k := range kinds {
			stmts := statementsOf(t, e, rec, firstQuery(t, p, k.accept))
			runs, err := coldOnce(t, conn, b.schema, nil, stmts)
			if err != nil {
				t.Fatal(err)
			}
			for j, r := range runs {
				name := filepath.Join(dir, string(arm)+"_"+k.name+"_"+string(rune('0'+j))+".json")
				if err := os.MkdirAll(dir, 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(name, append(r.JSON, '\n'), 0o644); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
}
