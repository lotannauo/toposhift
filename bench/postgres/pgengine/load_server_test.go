package pgengine_test

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/lotannauo/toposhift/bench/postgres/g0"
	"github.com/lotannauo/toposhift/bench/postgres/pgengine"
	"github.com/lotannauo/toposhift/bench/postgres/pgmodel"
	"github.com/lotannauo/toposhift/bench/postgres/pgschema"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/spike/workload"
)

// plans are made once: a plan is read, never changed, by every test below.
var (
	plansMu sync.Mutex
	plans   = map[string]*runner.Plan{}
)

func cachedPlan(t testing.TB, name string, spec runner.Spec) *runner.Plan {
	t.Helper()
	plansMu.Lock()
	defer plansMu.Unlock()
	if p, ok := plans[name]; ok {
		return p
	}
	p, err := runner.MakePlan(context.Background(), spec, nil)
	if err != nil {
		t.Fatal(err)
	}
	plans[name] = p
	return p
}

func noRetentionPlan(t testing.TB) *runner.Plan { return cachedPlan(t, "hour", hourSpec()) }
func retainingPlan(t testing.TB) *runner.Plan   { return cachedPlan(t, "retaining", retainingSpec()) }

// tinySpec is an hour and forty minutes of a very small cluster, with a retention at an
// hour and ten minutes that keeps forty: the plan that fixtures are recorded from, and the
// smallest that has every kind of read, the window of the last hour included.
func tinySpec() runner.Spec {
	w := workload.Tiny()
	w.Duration = 100 * time.Minute
	s := runner.DefaultSpec(w)
	s.MinNonEmpty = 0
	s.BatchSize = 100
	s.Retentions = []runner.Retention{{At: 70 * time.Minute, Keep: 40 * time.Minute}}
	return s
}

func tinyPlan(t testing.TB) *runner.Plan { return cachedPlan(t, "tiny", tinySpec()) }

// trace keeps the statements and the COPYs a connection runs, in order.
type trace struct {
	mu     sync.Mutex
	events []string
}

func (tr *trace) add(s string) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	tr.events = append(tr.events, s)
}

func (tr *trace) TraceQueryStart(ctx context.Context, _ *pgx.Conn, d pgx.TraceQueryStartData) context.Context {
	tr.add("query " + strings.Join(strings.Fields(d.SQL), " "))
	return ctx
}
func (tr *trace) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}
func (tr *trace) TraceCopyFromStart(ctx context.Context, _ *pgx.Conn, d pgx.TraceCopyFromStartData) context.Context {
	tr.add("copy " + d.TableName.Sanitize())
	return ctx
}
func (tr *trace) TraceCopyFromEnd(context.Context, *pgx.Conn, pgx.TraceCopyFromEndData) {}

// bulkLoad is a plan's stream loaded into the tables of a bulk arm.
type bulkLoad struct {
	stats  *pgengine.LoadStats
	info   runner.StreamInfo
	schema string
	dsn    string
}

// loadBulk loads the stream of a plan with a [pgengine.Loader] into a schema of its own
// and drops the schema when the test ends.
func loadBulk(t testing.TB, arm pgschema.Arm, p *runner.Plan, tracer pgx.QueryTracer) bulkLoad {
	t.Helper()
	dsn := serverDSN(t)
	schema := pgschema.SchemaName(t.TempDir())
	t.Cleanup(func() {
		if err := pgengine.DropSchema(context.Background(), dsn, schema); err != nil {
			t.Error(err)
		}
	})
	ctx := context.Background()
	l, err := pgengine.NewLoader(ctx, pgengine.LoadConfig{DSN: dsn, Schema: schema, Arm: arm, Tracer: tracer})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	info, err := runner.Drive(ctx, p.Spec, l)
	if err != nil {
		t.Fatal(err)
	}
	stats, err := l.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if err := g0.CheckStream(p, info, stats.LastSeq); err != nil {
		t.Fatal(err)
	}
	return bulkLoad{stats: stats, info: info, schema: schema, dsn: dsn}
}

// open opens an engine on the loaded tables.
func (b bulkLoad) open(t testing.TB, arm pgschema.Arm, tracer pgx.QueryTracer) *pgengine.Engine {
	t.Helper()
	e, err := pgengine.Open(context.Background(), pgengine.Config{DSN: b.dsn, Schema: b.schema, Arm: arm, Tracer: tracer})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e
}

func (b bulkLoad) conn(t testing.TB) *pgx.Conn {
	t.Helper()
	cc, err := pgx.ParseConfig(b.dsn)
	if err != nil {
		t.Fatal(err)
	}
	cc.RuntimeParams["search_path"] = pgx.Identifier{b.schema}.Sanitize() + ", public"
	c, err := pgx.ConnectConfig(context.Background(), cc)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close(context.Background()) })
	return c
}

// rows is a table's rows as text, in the order of a column that is unique.
func (b bulkLoad) rows(t testing.TB, table, order string) []string {
	t.Helper()
	return tableText(t, b.conn(t), table, order)
}

func tableText(t testing.TB, c *pgx.Conn, table, order string) []string {
	t.Helper()
	rs, err := c.Query(context.Background(), fmt.Sprintf("SELECT x::text FROM %s x ORDER BY %s", table, order))
	if err != nil {
		t.Fatal(err)
	}
	defer rs.Close()
	var out []string
	for rs.Next() {
		var s string
		if err := rs.Scan(&s); err != nil {
			t.Fatal(err)
		}
		out = append(out, s)
	}
	if err := rs.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func equalLines(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestABulkLoadAnswersEveryQuestionAndHoldsWhatTheEngineHolds: for a plan with no
// retention and one with, in both index sets, every question has the reference's answer,
// the interval tables are the model's clipped at the horizon, and the log (and the
// baselines a retention left) are the rows the engine's own writer and retention leave,
// though COPY wrote them.
func TestABulkLoadAnswersEveryQuestionAndHoldsWhatTheEngineHolds(t *testing.T) {
	for _, c := range []struct {
		name string
		plan func(testing.TB) *runner.Plan
	}{
		{"no retention", noRetentionPlan},
		{"a retention", retainingPlan},
	} {
		p := c.plan(t)
		model := pgmodel.New()
		if _, err := g0.Run(context.Background(), p, model, nil); err != nil {
			t.Fatal(err)
		}
		var horizon int64
		if n := len(p.Stream.Retentions); n > 0 {
			horizon = pgmodel.ToNanos(p.Stream.Retentions[n-1].Horizon)
		}
		wantDigest := model.Tables().Clipped(horizon).Digest()

		ref := openEngine(t, pgschema.Bulk) // what Write and Retain leave
		if _, err := g0.Run(context.Background(), p, ref, nil); err != nil {
			t.Fatal(err)
		}
		refConn := sqlConn(t, ref)

		for _, arm := range []pgschema.Arm{pgschema.Bulk, pgschema.BulkB} {
			t.Run(c.name+" "+string(arm), func(t *testing.T) {
				b := loadBulk(t, arm, p, nil)
				e := b.open(t, arm, nil)
				rep, err := g0.Ask(context.Background(), p.Queries, b.info, e, nil)
				if err != nil {
					t.Fatal(err)
				}
				if !rep.OK() || rep.Queries != len(p.Queries) {
					t.Fatalf("%d of %d answers equal the reference's; first mismatches %+v", rep.Matched, rep.Queries, rep.Mismatches)
				}
				d, err := pgengine.DigestSchema(context.Background(), b.dsn, b.schema, arm)
				if err != nil {
					t.Fatal(err)
				}
				if d.Digest != wantDigest || d.Horizon != horizon {
					t.Errorf("the interval tables (horizon %d) have digest %s, the model's clipped at %d have %s", d.Horizon, d.Digest, horizon, wantDigest)
				}
				if got, want := b.rows(t, "log", "seq"), tableText(t, refConn, "log", "seq"); !equalLines(got, want) {
					t.Errorf("the log COPY wrote has %d rows and differs from what the engine's writer left (%d rows)", len(got), len(want))
				}
				if got, want := b.rows(t, "log_base", "src, dst, rel, producer, seq"), tableText(t, refConn, "log_base", "src, dst, rel, producer, seq"); !equalLines(got, want) {
					t.Errorf("the baselines of the load (%d) differ from the engine's retention's (%d)", len(got), len(want))
				}
				if len(p.Stream.Retentions) > 0 && (b.stats.LogBaseRows == 0 || b.stats.Retentions != len(p.Stream.Retentions) || b.stats.StreamVacuums == 0) {
					t.Errorf("retentions %d of %d, %d baselines, %d vacuums called for", b.stats.Retentions, len(p.Stream.Retentions), b.stats.LogBaseRows, b.stats.StreamVacuums)
				}
				if len(p.Stream.Retentions) == 0 && (b.stats.LogBaseRows != 0 || b.stats.StreamVacuums != 0) {
					t.Errorf("no retention, and %d baselines, %d vacuums", b.stats.LogBaseRows, b.stats.StreamVacuums)
				}
				// A bulk load keeps no row that a retention would have deleted, so the digest
				// covers every row it holds.
				if b.stats.Records != p.Stream.Records || b.stats.LastSeq != p.Stream.LastSeq || b.stats.EdgeRows+b.stats.EntityRows != d.Rows {
					t.Errorf("counts: %+v, digest of %d rows", b.stats, d.Rows)
				}
			})
		}
	}
}

// TestABulkLoadOfATinyPlanAnswersEveryQuestion is the first gate on a plan that has every
// kind of read, windows and the old snapshot included, and a retention, small enough to
// run in a second: the answers are the reference's and the intervals are the model's.
func TestABulkLoadOfATinyPlanAnswersEveryQuestion(t *testing.T) {
	p := tinyPlan(t)
	model := pgmodel.New()
	if _, err := g0.Run(context.Background(), p, model, nil); err != nil {
		t.Fatal(err)
	}
	horizon := pgmodel.ToNanos(p.Stream.Retentions[len(p.Stream.Retentions)-1].Horizon)
	ref := openEngine(t, pgschema.Bulk) // what Write and Retain leave
	if _, err := g0.Run(context.Background(), p, ref, nil); err != nil {
		t.Fatal(err)
	}
	refConn := sqlConn(t, ref)
	for _, arm := range []pgschema.Arm{pgschema.Bulk, pgschema.BulkB} {
		b := loadBulk(t, arm, p, nil)
		if got, want := b.rows(t, "log", "seq"), tableText(t, refConn, "log", "seq"); !equalLines(got, want) {
			t.Errorf("%s: the log COPY wrote differs from what the engine's writer left", arm)
		}
		if got, want := b.rows(t, "log_base", "src, dst, rel, producer, seq"), tableText(t, refConn, "log_base", "src, dst, rel, producer, seq"); !equalLines(got, want) {
			t.Errorf("%s: the baselines differ from the engine's retention's", arm)
		}
		rep, err := g0.Ask(context.Background(), p.Queries, b.info, b.open(t, arm, nil), nil)
		if err != nil {
			t.Fatal(err)
		}
		if !rep.OK() {
			t.Errorf("%s: %d of %d answers equal the reference's: %+v", arm, rep.Matched, rep.Queries, rep.Mismatches)
		}
		d, err := pgengine.DigestSchema(context.Background(), b.dsn, b.schema, arm)
		if err != nil {
			t.Fatal(err)
		}
		if want := model.Tables().Clipped(horizon).Digest(); d.Digest != want {
			t.Errorf("%s: the interval tables have digest %s, the model's %s", arm, d.Digest, want)
		}
		// A bulk load holds no interval row that a retention would have deleted.
		if b.stats.EdgeRows+b.stats.EntityRows != d.Rows {
			t.Errorf("%s: %d interval rows, %d of them within the horizon", arm, b.stats.EdgeRows+b.stats.EntityRows, d.Rows)
		}
	}
}

// intervalRowChange matches a statement that updates or deletes rows of an interval table.
var intervalRowChange = regexp.MustCompile(`(?i)^query\s+(UPDATE\s+|DELETE\s+FROM\s+)\S*\b(edges|entities)\b`)

// TestTheMeasuredLoadWritesWithCopyAndBuildsItsIndexesAfterTheTables: the log, and the
// intervals, go in with COPY and never with an INSERT; and no index is built before the
// last COPY into the tables, which is what a bulk load is.
func TestTheMeasuredLoadWritesWithCopyAndBuildsItsIndexesAfterTheTables(t *testing.T) {
	tr := &trace{}
	loadBulk(t, pgschema.Bulk, retainingPlan(t), tr)
	lastCopy, firstIndex := -1, -1
	var changes []int
	copies := map[string]int{}
	for i, ev := range tr.events {
		switch {
		case strings.HasPrefix(ev, "copy "):
			copies[strings.TrimPrefix(ev, "copy ")] = copies[strings.TrimPrefix(ev, "copy ")] + 1
			lastCopy = i
		case strings.HasPrefix(ev, "query INSERT INTO log ("):
			// (The retention's INSERT INTO log_base copies baselines inside the server.)
			t.Errorf("the log was written with %q", ev)
		case strings.HasPrefix(ev, "query CREATE INDEX"):
			if firstIndex < 0 {
				firstIndex = i
			}
		case intervalRowChange.MatchString(ev):
			changes = append(changes, i)
		}
	}
	// The loader writes interval rows with COPY. Its retention deletes the rows that have
	// ended, and it does so before any index exists: a change to interval rows made while a
	// GiST index exists is planned through it, and on the server this baseline runs
	// against that leaves the index's entries for rows the statement wrote marked dead.
	for _, c := range changes {
		if firstIndex >= 0 && c > firstIndex {
			t.Errorf("interval rows were changed at event %d, after the first index at event %d: %q", c, firstIndex, tr.events[c])
		}
	}
	if copies[`"log"`] < 2 || copies[`"edges"`] != 1 || copies[`"entities"`] != 1 {
		t.Errorf("COPYs: %v", copies)
	}
	if firstIndex < 0 || firstIndex < lastCopy {
		t.Errorf("the first index is built at event %d, the last COPY is at %d", firstIndex, lastCopy)
	}
}

// TestTheIdArmHoldsTheSameIntervalsUnderIds: the same load with a dictionary and ids in
// place of fingerprints holds intervals with the digest of the fingerprint arm's, a
// dictionary of exactly the entities the retained log refers to, and is smaller.
func TestTheIdArmHoldsTheSameIntervalsUnderIds(t *testing.T) {
	p := retainingPlan(t)
	plain := loadBulk(t, pgschema.Bulk, p, nil)
	ids := loadBulk(t, pgschema.BulkID, p, nil)
	dp, err := pgengine.DigestSchema(context.Background(), plain.dsn, plain.schema, pgschema.Bulk)
	if err != nil {
		t.Fatal(err)
	}
	di, err := pgengine.DigestSchema(context.Background(), ids.dsn, ids.schema, pgschema.BulkID)
	if err != nil {
		t.Fatal(err)
	}
	if dp.Digest != di.Digest || dp.Rows != di.Rows {
		t.Errorf("fingerprint arm %s (%d rows), id arm %s (%d rows)", dp.Digest, dp.Rows, di.Digest, di.Rows)
	}
	a, b := *plain.stats, *ids.stats
	if a.LogRows != b.LogRows || a.LogBaseRows != b.LogBaseRows || a.EdgeRows != b.EdgeRows || a.EntityRows != b.EntityRows {
		t.Errorf("rows differ: %+v and %+v", a, b)
	}
	var distinct int64
	err = plain.conn(t).QueryRow(context.Background(), `SELECT count(*) FROM (
  SELECT src FROM log UNION SELECT dst FROM log WHERE dst IS NOT NULL UNION SELECT src FROM log_base UNION SELECT dst FROM log_base WHERE dst IS NOT NULL) e`).Scan(&distinct)
	if err != nil {
		t.Fatal(err)
	}
	if b.DictionaryRows != distinct {
		t.Errorf("the dictionary has %d entities, the retained log refers to %d", b.DictionaryRows, distinct)
	}
	size := func(l bulkLoad) int64 {
		s, err := pgengine.Sizes(context.Background(), l.conn(t), l.schema)
		if err != nil {
			t.Fatal(err)
		}
		var n int64
		for _, r := range s {
			if r.Name != "entity" && r.Table != "entity" {
				n += r.Total()
			}
		}
		return n
	}
	if sp, si := size(plain), size(ids); si >= sp {
		t.Errorf("without the dictionary the id arm holds %d bytes, the fingerprint arm %d", si, sp)
	}
	// The id arm has no engine: it is for sizes.
	if _, err := pgengine.Open(context.Background(), pgengine.Config{DSN: ids.dsn, Schema: ids.schema, Arm: pgschema.BulkID}); !errors.Is(err, engine.ErrInvalid) {
		t.Errorf("opening the id arm: %v", err)
	}
}

// TestTheLoaderRefusesWhatItCannotLoad: each refusal leaves the log as it was.
func TestTheLoaderRefusesWhatItCannotLoad(t *testing.T) {
	dsn := serverDSN(t)
	ctx := context.Background()
	for name, cfg := range map[string]pgengine.LoadConfig{
		"the streaming arm": {DSN: dsn, Schema: "s_x", Arm: pgschema.Stream},
		"no arm":            {DSN: dsn, Schema: "s_x"},
		"a bad schema name": {DSN: dsn, Schema: `s"; drop`, Arm: pgschema.Bulk},
		"no DSN":            {Schema: "s_x", Arm: pgschema.Bulk},
	} {
		if l, err := pgengine.NewLoader(ctx, cfg); err == nil {
			_ = l.Close()
			t.Errorf("%s: accepted", name)
		}
	}

	schema := pgschema.SchemaName(t.TempDir())
	t.Cleanup(func() { _ = pgengine.DropSchema(ctx, dsn, schema) })
	cfg := pgengine.LoadConfig{DSN: dsn, Schema: schema, Arm: pgschema.Bulk}
	l, err := pgengine.NewLoader(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if _, err := l.Finish(); !errors.Is(err, engine.ErrInvalid) {
		t.Errorf("finishing a load that wrote nothing: %v", err)
	}
	rec := func(seq uint64, s int64) engine.Record { return observe(seq, "k8s", s, 0) }
	bad := rec(2, 10)
	bad.Producer = "" // invalid
	for name, batch := range map[string][]engine.Record{
		"an invalid record":    {rec(1, 5), bad},
		"a seq that goes down": {rec(5, 5), rec(4, 6)},
		"a seq that repeats":   {rec(5, 5), rec(5, 6)},
	} {
		if err := l.Write(batch); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if err := l.Write([]engine.Record{rec(1, 100), rec(2, 200)}); err != nil {
		t.Fatal(err)
	}
	if err := l.Write([]engine.Record{rec(2, 300)}); !errors.Is(err, engine.ErrInvalid) {
		t.Errorf("a seq at or below the last: %v", err)
	}
	if err := l.Retain(at(150)); err != nil {
		t.Fatal(err)
	}
	if err := l.Write([]engine.Record{rec(3, 149)}); !errors.Is(err, engine.ErrBeforeHorizon) {
		t.Errorf("a record before the horizon: %v", err)
	}
	if err := l.Write([]engine.Record{rec(3, 150)}); err != nil {
		t.Errorf("a record at the horizon: %v", err)
	}
	stats, err := l.Finish()
	if err != nil {
		t.Fatal(err)
	}
	// Only what was accepted is in the log: the refused batches wrote nothing. The first
	// record is before the horizon and is a baseline; the second and the third are in the log.
	if stats.Records != 3 || stats.LogRows != 2 || stats.LogBaseRows != 1 || stats.LastSeq != 3 {
		t.Errorf("%+v", stats)
	}

	// A schema that is there is not added to.
	if l2, err := pgengine.NewLoader(ctx, cfg); !errors.Is(err, pgengine.ErrSchemaExists) {
		if l2 != nil {
			_ = l2.Close()
		}
		t.Errorf("a schema that exists: %v", err)
	}
	cfg.Replace = true
	l3, err := pgengine.NewLoader(ctx, cfg)
	if err != nil {
		t.Fatalf("replacing it: %v", err)
	}
	_ = l3.Close()
}

// TestTheLogOfALoadIsCountedByPhase: the stream is one span whose parts are the COPYs,
// the retentions and the vacuums they called for; the other phases follow it.
func TestTheLogOfALoadIsCountedByPhase(t *testing.T) {
	b := loadBulk(t, pgschema.Bulk, retainingPlan(t), nil)
	byName := map[string]pgengine.Phase{}
	var names []string
	for _, p := range b.stats.Phases {
		byName[p.Name] = p
		names = append(names, p.Name)
	}
	if want := "stream copy retain vacuum-stream fold indexes vacuum"; strings.Join(names, " ") != want {
		t.Fatalf("phases %v, want %s", names, want)
	}
	for _, name := range []string{"stream", "fold", "indexes", "vacuum"} {
		p := byName[name]
		if !p.Contiguous || p.LogBytes < p.RecordBytes || p.RecordBytes <= 0 || p.Records <= 0 {
			t.Errorf("%s: %+v", name, p)
		}
	}
	for _, name := range []string{"copy", "retain", "vacuum-stream"} {
		p := byName[name]
		if p.Contiguous || p.LogBytes != 0 || p.RecordBytes <= 0 {
			t.Errorf("%s: %+v", name, p)
		}
	}
	// The parts are the stream, when nothing else wrote the log.
	s := byName["stream"]
	if s.CheckpointsInside == 0 {
		parts := byName["copy"].RecordBytes + byName["retain"].RecordBytes + byName["vacuum-stream"].RecordBytes
		if parts != s.RecordBytes {
			t.Errorf("the parts of the stream are %d record bytes, the stream %d", parts, s.RecordBytes)
		}
	}
	// Every page is all-visible, so that an index-only scan never visits the heap.
	conn := b.conn(t)
	for _, table := range []string{"log", "log_base", "edges", "entities", "meta"} {
		var pages, visible int32
		if err := conn.QueryRow(context.Background(), "SELECT relpages, relallvisible FROM pg_class WHERE oid = $1::regclass", table).Scan(&pages, &visible); err != nil {
			t.Fatal(err)
		}
		if visible < pages {
			t.Errorf("%s has %d pages and %d are all-visible", table, pages, visible)
		}
	}
	// The index phase writes an image of every page of every index.
	if byName["indexes"].FPIs == 0 || byName["vacuum"].FPIs == 0 {
		t.Errorf("no full-page image in the index or the vacuum phase: %+v %+v", byName["indexes"], byName["vacuum"])
	}
}

// TestSizesAreWhatTheSchemaHoldsAndCompactionKeepsTheAnswers: the relations' sizes add up
// to the engine's own count of the schema, a retention's deletes leave space that
// compaction takes back, and the reads are the same on the compacted tables.
func TestSizesAreWhatTheSchemaHoldsAndCompactionKeepsTheAnswers(t *testing.T) {
	p := retainingPlan(t)
	b := loadBulk(t, pgschema.Bulk, p, nil)
	ctx := context.Background()
	conn := b.conn(t)
	sizes, err := pgengine.Sizes(ctx, conn, b.schema)
	if err != nil {
		t.Fatal(err)
	}
	var total int64
	kinds := map[string]int{}
	for _, r := range sizes {
		total += r.Total()
		kinds[r.Kind]++
		if r.Kind == "index" && r.Table == "" {
			t.Errorf("index %s belongs to no table", r.Name)
		}
	}
	e := b.open(t, pgschema.Bulk, nil)
	if size, err := e.Size(); err != nil || size != total {
		t.Errorf("the engine counts %d bytes (%v), the relations add up to %d", size, err, total)
	}
	if kinds["table"] != 5 || kinds["index"] < 8 {
		t.Errorf("relations by kind: %v", kinds)
	}

	if err := pgengine.Compact(ctx, conn, b.schema); err != nil {
		t.Fatal(err)
	}
	after, err := pgengine.Sizes(ctx, conn, b.schema)
	if err != nil {
		t.Fatal(err)
	}
	var totalAfter int64
	for _, r := range after {
		totalAfter += r.Total()
	}
	if totalAfter >= total {
		t.Errorf("compaction left %d bytes of %d", totalAfter, total)
	}
	rep, err := g0.Ask(ctx, p.Queries, b.info, e, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.OK() {
		t.Errorf("after compaction %d of %d answers equal the reference's: %+v", rep.Matched, rep.Queries, rep.Mismatches)
	}
}

// TestTheServerSaysWhatItIs: the build and the machine a record names.
func TestTheServerSaysWhatItIs(t *testing.T) {
	conn, err := pgx.Connect(context.Background(), serverDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	info, err := pgengine.ReadServerInfo(context.Background(), conn)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(info.Version, "PostgreSQL 17.") || info.BlockSize != 8192 || info.WALBlockSize != 8192 || info.Configure == "" || info.CPU == "" ||
		(info.DataChecksums != "on" && info.DataChecksums != "off") {
		t.Errorf("%+v", info)
	}
}

// foldedLoader is a loader that has been given a plan's stream and has not finished.
func foldedLoader(t testing.TB, arm pgschema.Arm, p *runner.Plan, maxMem int64) (*pgengine.Loader, string) {
	t.Helper()
	dsn := serverDSN(t)
	schema := pgschema.SchemaName(t.TempDir())
	t.Cleanup(func() { _ = pgengine.DropSchema(context.Background(), dsn, schema) })
	l, err := pgengine.NewLoader(context.Background(), pgengine.LoadConfig{DSN: dsn, Schema: schema, Arm: arm, MaxIntervalMem: maxMem})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	if _, err := runner.Drive(context.Background(), p.Spec, l); err != nil {
		t.Fatal(err)
	}
	return l, schema
}

// TestAnIntervalRowCostsWhatTheEstimateSays: the memory a fold holds for its interval rows,
// measured on the fixtures' plans in both key forms, is within the estimate the load
// judges itself by, and the estimate is not so far above it as to refuse loads that would
// fit.
func TestAnIntervalRowCostsWhatTheEstimateSays(t *testing.T) {
	if pgengine.IntervalMemory(1000) != 1000*pgengine.IntervalRowBytes || pgengine.IntervalMemory(0) != 0 {
		t.Fatalf("the estimate of 1000 rows is %d", pgengine.IntervalMemory(1000))
	}
	for name, p := range map[string]*runner.Plan{"tiny": tinyPlan(t), "retaining": retainingPlan(t)} {
		for _, arm := range []pgschema.Arm{pgschema.Bulk, pgschema.BulkID} {
			l, _ := foldedLoader(t, arm, p, 0)
			rows, bytes, err := pgengine.FoldMemory(context.Background(), l)
			if err != nil {
				t.Fatal(err)
			}
			if rows < 1000 {
				t.Fatalf("%s %s: only %d rows to measure", name, arm, rows)
			}
			if per := float64(bytes) / float64(rows); per > pgengine.IntervalRowBytes || per < pgengine.IntervalRowBytes/4 {
				t.Errorf("%s %s: a fold holds %.1f bytes a row, the estimate is %d", name, arm, per, pgengine.IntervalRowBytes)
			}
		}
	}
}

// TestALoadThatNeedsMoreMemoryThanItMayUseDoesNotRun: the rows are counted before any is
// held. A limit one byte below the estimate refuses the load, with nothing in the
// interval tables and no index; a limit at the estimate, and the default, accept it.
func TestALoadThatNeedsMoreMemoryThanItMayUseDoesNotRun(t *testing.T) {
	p := tinyPlan(t)
	ref := loadBulk(t, pgschema.Bulk, p, nil)
	rows := ref.stats.EdgeRows + ref.stats.EntityRows
	need := pgengine.IntervalMemory(rows)

	l, schema := foldedLoader(t, pgschema.Bulk, p, need-1)
	_, err := l.Finish()
	if !errors.Is(err, pgengine.ErrIntervalMemory) {
		t.Fatalf("a limit one byte below the estimate: %v", err)
	}
	if msg := err.Error(); !strings.Contains(msg, fmt.Sprint(rows)+" interval rows") || !strings.Contains(msg, fmt.Sprint(need)) || !strings.Contains(msg, fmt.Sprint(need-1)) {
		t.Errorf("the refusal does not say how many rows, how much memory and what limit: %s", msg)
	}
	c := (bulkLoad{dsn: serverDSN(t), schema: schema}).conn(t)
	var edges, entities, indexes int
	if err := c.QueryRow(context.Background(), `SELECT (SELECT count(*) FROM edges), (SELECT count(*) FROM entities),
  (SELECT count(*) FROM pg_indexes WHERE schemaname = $1 AND indexname <> 'meta_pkey')`, schema).Scan(&edges, &entities, &indexes); err != nil {
		t.Fatal(err)
	}
	if edges != 0 || entities != 0 || indexes != 0 {
		t.Errorf("a refused load left %d edge rows, %d entity rows and %d indexes beyond the meta table's key", edges, entities, indexes)
	}

	for name, limit := range map[string]int64{"at the estimate": need, "the default": 0} {
		l, _ := foldedLoader(t, pgschema.Bulk, p, limit)
		stats, err := l.Finish()
		if err != nil || stats.EdgeRows+stats.EntityRows != rows {
			t.Errorf("a limit %s: %v, %v", name, err, stats)
		}
	}
}
