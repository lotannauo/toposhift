package pgengine

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/lotannauo/toposhift/bench/postgres/pgmodel"
	"github.com/lotannauo/toposhift/bench/postgres/pgschema"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// This file is the streaming writer's SQL: the writer of the PG-stream arms, which keeps
// the interval tables current under a stream of batches, late events and retentions.
// The decisions, which rows to insert, move or delete, are [pgmodel.Writer]'s, the same
// that the in-memory model makes; this file reads what a re-fold needs, stores the log
// with COPY, applies the rows it is handed by where each is, and keeps the vacuum
// schedule of the deterministic regime.

// ModelFor is the configuration of the in-memory model that models a streaming arm: its
// writer, and whether its log is partitioned. A model of it, given a stream, makes the
// rows and the counts the engine does.
func ModelFor(arm pgschema.Arm) pgmodel.ModelConfig {
	return pgmodel.ModelConfig{WriterConfig: pgmodel.WriterConfig{Naive: arm.Naive()}, Partitioned: arm.Partitioned()}
}

// AnalyzeRuns is how many times the writer has analyzed each table, by the same schedule.
// It is nil for an arm that is not written by the streaming writer.
func (e *Engine) AnalyzeRuns() map[string]int {
	e.wmu.Lock()
	defer e.wmu.Unlock()
	if e.vac == nil {
		return nil
	}
	return e.vac.AnalyzeRuns()
}

// VacuumRuns is how many times the writer has vacuumed each table, by the schedule of the
// deterministic regime. It is nil for an arm that is not written by the streaming writer.
func (e *Engine) VacuumRuns() map[string]int {
	e.wmu.Lock()
	defer e.wmu.Unlock()
	if e.vac == nil {
		return nil
	}
	return e.vac.Runs()
}

// ErrConcurrentWriter is wrapped by the error a write or a retention returns when the
// database is not as this engine left it: another writer has changed the token or the
// horizon, and what this engine remembers of the rows is no longer what is stored.
var ErrConcurrentWriter = errors.New("pgengine: the schema was changed by another writer")

// locOf is where a row is, as the planner carries it: the six bytes of a ctid.
func locOf(t pgtype.TID) pgmodel.Loc {
	return pgmodel.Loc{byte(t.BlockNumber >> 24), byte(t.BlockNumber >> 16), byte(t.BlockNumber >> 8), byte(t.BlockNumber), byte(t.OffsetNumber >> 8), byte(t.OffsetNumber)}
}

// tidOf is the inverse of locOf.
func tidOf(l pgmodel.Loc) pgtype.TID {
	return pgtype.TID{
		BlockNumber:  uint32(l[0])<<24 | uint32(l[1])<<16 | uint32(l[2])<<8 | uint32(l[3]),
		OffsetNumber: uint16(l[4])<<8 | uint16(l[5]),
		Valid:        true,
	}
}

// The statements that change interval rows, as the streaming writer sends them. Each
// finds its rows by ctid, which the writer read in the transaction that wrote the row
// last or in the one that asked for the rows (see the comment on edgesDelete), and each
// returns the key of every row it changed, and for a row it wrote where it is now, so
// that the writer can check it changed the rows it meant to, and no others.
//
// extend moves the deadline of a provisional row: the only column it sets is outside
// every index, so the server makes the update heap-only. rewrite sets all three columns
// the writer changes, for the updates that give a row another range.
const (
	edgesExtend = `UPDATE edges e SET deadline_ns = d.deadline_ns
FROM unnest($1::tid[], $2::bigint[]) AS d(tid, deadline_ns)
WHERE e.ctid = ANY($1::tid[]) AND e.ctid = d.tid
RETURNING e.ctid, e.src, e.dst, e.rel, e.start_ns`
	edgesRewrite = `UPDATE edges e SET during = int8range(e.start_ns, d.end_ns), deadline_ns = d.deadline_ns, end_source = d.end_source
FROM unnest($1::tid[], $2::bigint[], $3::bigint[], $4::smallint[]) AS d(tid, end_ns, deadline_ns, end_source)
WHERE e.ctid = ANY($1::tid[]) AND e.ctid = d.tid
RETURNING e.ctid, e.src, e.dst, e.rel, e.start_ns`
	edgesInsertReturning = edgesInsert + "\nRETURNING ctid, src, dst, rel, start_ns"
	edgesDeleteReturning = edgesDelete + "\nRETURNING NULL::tid, src, dst, rel, start_ns"
	entitiesExtend       = `UPDATE entities e SET deadline_ns = d.deadline_ns
FROM unnest($1::tid[], $2::bigint[]) AS d(tid, deadline_ns)
WHERE e.ctid = ANY($1::tid[]) AND e.ctid = d.tid
RETURNING e.ctid, e.fp, NULL::text, 0::bigint, e.start_ns`
	entitiesRewrite = `UPDATE entities e SET during = int8range(e.start_ns, d.end_ns), deadline_ns = d.deadline_ns, end_source = d.end_source
FROM unnest($1::tid[], $2::bigint[], $3::bigint[], $4::smallint[]) AS d(tid, end_ns, deadline_ns, end_source)
WHERE e.ctid = ANY($1::tid[]) AND e.ctid = d.tid
RETURNING e.ctid, e.fp, NULL::text, 0::bigint, e.start_ns`
	entitiesInsertReturning = entitiesInsert + "\nRETURNING ctid, fp, NULL::text, 0::bigint, start_ns"
	entitiesDeleteReturning = entitiesDelete + "\nRETURNING NULL::tid, fp, NULL::text, 0::bigint, start_ns"
)

// Every statement returns its rows in one shape, so that one reader serves them all: the
// ctid (null for a delete), the key (an edge's source or an entity), an edge's target or
// null, an edge's relation or null, and the start. (The entity statements pad the two
// columns they do not have, with a null and a zero.)

// The log of a subject from where a re-fold starts, baselines included: the log rows are
// those at or after the start and at or after the horizon, so that a record and the
// baseline that stands for it are never both read while a partition that holds it
// still exists; the baselines are at or after the start.
const (
	edgeInputStream = `SELECT seq, event_ns, producer, kind::int4, ttl_ns, through_ns, layer FROM log
  WHERE subject_kind = 'g' AND src = $1 AND dst = $2 AND rel = $3 AND event_ns >= greatest($4::bigint, $5::bigint)
UNION ALL
SELECT seq, event_ns, producer, kind::int4, ttl_ns, through_ns, layer FROM log_base
  WHERE subject_kind = 'g' AND src = $1 AND dst = $2 AND rel = $3 AND event_ns >= $4::bigint`
	entityInputStream = `SELECT seq, event_ns, producer, kind::int4, ttl_ns, through_ns, layer FROM log
  WHERE subject_kind = 'e' AND src = $1 AND event_ns >= greatest($2::bigint, $3::bigint)
UNION ALL
SELECT seq, event_ns, producer, kind::int4, ttl_ns, through_ns, layer FROM log_base
  WHERE subject_kind = 'e' AND src = $1 AND event_ns >= $2::bigint`
)

// checkBatch checks the records of a batch one at a time, as every engine does: valid,
// not before the horizon, and with sequence numbers above the token and ascending.
func checkBatch(batch []engine.Record, meta metaState) error {
	seq := meta.lastSeq
	for _, r := range batch {
		if err := r.Validate(); err != nil {
			return err
		}
		if meta.pastEnd || pgmodel.ToNanos(r.EventTime) < meta.horizon {
			return fmt.Errorf("record seq %d at %s: %w", r.Seq, r.EventTime.Format(time.RFC3339), engine.ErrBeforeHorizon)
		}
		if r.Seq <= seq {
			return fmt.Errorf("record seq %d is not above the last written seq %d: %w", r.Seq, seq, engine.ErrInvalid)
		}
		seq = r.Seq
	}
	return nil
}

// checkWriter says whether the database is as this engine left it.
func (e *Engine) checkWriter(meta metaState) error {
	if meta.lastSeq != e.seq || meta.horizon != e.w.Horizon() || meta.pastEnd != e.pastEnd {
		return fmt.Errorf("pgengine: the token is %d, the horizon %d and past the end %v; this engine left %d, %d and %v: %w",
			meta.lastSeq, meta.horizon, meta.pastEnd, e.seq, e.w.Horizon(), e.pastEnd, ErrConcurrentWriter)
	}
	return nil
}

// target is a row a statement is to change, by the key the statement returns it under.
type target struct {
	subject engine.Subject
	start   int64
}

func edgeKey(src, dst []byte, rel string, start int64) string {
	return fmt.Sprintf("%d:%s|%d:%s|%s|%d", len(src), src, len(dst), dst, rel, start)
}

func entityKey(fp []byte, start int64) string {
	return fmt.Sprintf("%d:%s|%d", len(fp), fp, start)
}

func keyOf(s engine.Subject, start int64) string {
	if s.Kind == engine.SubjectEdge {
		return edgeKey(pgmodel.EncodeFingerprint(s.A), pgmodel.EncodeFingerprint(s.B), string(s.Relation), start)
	}
	return entityKey(pgmodel.EncodeFingerprint(s.A), start)
}

// A queued statement: its text and arguments, and the rows it is to change by key.
type queued struct {
	sql    string
	args   []any
	want   map[string]target
	edges  bool
	places bool // the rows are written, and where each went is wanted
}

type stmtBuilder struct {
	edgeDel, entityDel []pgtype.TID
	edgeExt, entityExt extension
	edgeUpd, entityUpd tidRows
	edgeIns            edgeCols
	entityIns          entityCols
	// the rows each kind of statement is to change, for edges and for entities
	wantDel, wantExt, wantUpd, wantIns [2]map[string]target
}

type extension struct {
	tid      []pgtype.TID
	deadline []int64
}

func newTargets() map[string]target { return make(map[string]target) }

// build sorts the changes of a batch into the statements that make them, each kind of
// statement over one table once: deletes, then extensions and rewrites, then inserts,
// so that a primary key is free before an insert needs it.
func build(changes []pgmodel.SubjectChanges) []queued {
	var b stmtBuilder
	for i := range 2 {
		b.wantDel[i], b.wantExt[i], b.wantUpd[i], b.wantIns[i] = newTargets(), newTargets(), newTargets(), newTargets()
	}
	for _, sc := range changes {
		edge := sc.Subject.Kind == engine.SubjectEdge
		t := 0
		if !edge {
			t = 1
		}
		layer := int16(sc.Layer)
		for _, c := range sc.Rows {
			switch c.Kind {
			case pgmodel.Delete:
				if edge {
					b.edgeDel = append(b.edgeDel, tidOf(c.Old.Loc))
				} else {
					b.entityDel = append(b.entityDel, tidOf(c.Old.Loc))
				}
				b.wantDel[t][keyOf(sc.Subject, c.Old.Start)] = target{sc.Subject, c.Old.Start}
			case pgmodel.Update:
				tid := tidOf(c.Old.Loc)
				switch {
				case c.Extension && edge:
					b.edgeExt.tid, b.edgeExt.deadline = append(b.edgeExt.tid, tid), append(b.edgeExt.deadline, c.New.Deadline)
					b.wantExt[0][keyOf(sc.Subject, c.Old.Start)] = target{sc.Subject, c.Old.Start}
				case c.Extension:
					b.entityExt.tid, b.entityExt.deadline = append(b.entityExt.tid, tid), append(b.entityExt.deadline, c.New.Deadline)
					b.wantExt[1][keyOf(sc.Subject, c.Old.Start)] = target{sc.Subject, c.Old.Start}
				case edge:
					b.edgeUpd.add(tid, layer, c.New)
					b.wantUpd[0][keyOf(sc.Subject, c.Old.Start)] = target{sc.Subject, c.Old.Start}
				default:
					b.entityUpd.add(tid, layer, c.New)
					b.wantUpd[1][keyOf(sc.Subject, c.Old.Start)] = target{sc.Subject, c.Old.Start}
				}
			case pgmodel.Insert:
				if edge {
					b.edgeIns.add(sc.Subject, layer, c.New)
				} else {
					b.entityIns.add(sc.Subject, layer, c.New)
				}
				b.wantIns[t][keyOf(sc.Subject, c.New.Start)] = target{sc.Subject, c.New.Start}
			}
		}
	}
	var out []queued
	if len(b.edgeDel) > 0 {
		out = append(out, queued{sql: edgesDeleteReturning, args: []any{b.edgeDel}, want: b.wantDel[0], edges: true})
	}
	if len(b.entityDel) > 0 {
		out = append(out, queued{sql: entitiesDeleteReturning, args: []any{b.entityDel}, want: b.wantDel[1]})
	}
	if len(b.edgeExt.tid) > 0 {
		out = append(out, queued{sql: edgesExtend, args: []any{b.edgeExt.tid, b.edgeExt.deadline}, want: b.wantExt[0], edges: true, places: true})
	}
	if len(b.entityExt.tid) > 0 {
		out = append(out, queued{sql: entitiesExtend, args: []any{b.entityExt.tid, b.entityExt.deadline}, want: b.wantExt[1], places: true})
	}
	if len(b.edgeUpd.tid) > 0 {
		out = append(out, queued{sql: edgesRewrite, args: []any{b.edgeUpd.tid, b.edgeUpd.end, b.edgeUpd.deadline, b.edgeUpd.source}, want: b.wantUpd[0], edges: true, places: true})
	}
	if len(b.entityUpd.tid) > 0 {
		out = append(out, queued{sql: entitiesRewrite, args: []any{b.entityUpd.tid, b.entityUpd.end, b.entityUpd.deadline, b.entityUpd.source}, want: b.wantUpd[1], places: true})
	}
	if len(b.edgeIns.src) > 0 {
		out = append(out, queued{sql: edgesInsertReturning, args: []any{b.edgeIns.src, b.edgeIns.dst, b.edgeIns.rel, b.edgeIns.layer, b.edgeIns.start, b.edgeIns.end, b.edgeIns.deadline, b.edgeIns.source}, want: b.wantIns[0], edges: true, places: true})
	}
	if len(b.entityIns.fp) > 0 {
		out = append(out, queued{sql: entitiesInsertReturning, args: []any{b.entityIns.fp, b.entityIns.layer, b.entityIns.start, b.entityIns.end, b.entityIns.deadline, b.entityIns.source}, want: b.wantIns[1], places: true})
	}
	return out
}

// errs of the arrays that the statements are built from, which the caller checks before
// anything is sent (a row with no length).
func buildError(changes []pgmodel.SubjectChanges) error {
	var cols rowColumns
	for _, sc := range changes {
		for _, c := range sc.Rows {
			if c.Kind != pgmodel.Delete {
				cols.add(int16(sc.Layer), c.New)
			}
		}
	}
	return cols.err
}

// placed is a row a statement wrote and where it went.
type placed struct {
	subject engine.Subject
	start   int64
	loc     pgmodel.Loc
}

// returned reads the rows a statement returned, which are in the shape described above,
// and checks them against the rows it was asked to change: each must be one of them, once,
// and every one must come back. A ctid that no longer names the row it was read for would
// change another row or none, and that is the one thing that must not pass. It returns
// where each row it wrote now is.
func (q queued) returned(rows pgx.Rows) ([]placed, error) {
	defer rows.Close()
	seen := make(map[string]bool, len(q.want))
	var wrote []placed
	for rows.Next() {
		var tid pgtype.TID
		var key1, key2 []byte
		var rel *string
		var start int64
		var err error
		if q.edges {
			err = rows.Scan(&tid, &key1, &key2, &rel, &start)
		} else {
			var zero int64
			err = rows.Scan(&tid, &key1, &rel, &zero, &start)
		}
		if err != nil {
			return nil, fmt.Errorf("pgengine: reading what a statement changed: %w", err)
		}
		key := entityKey(key1, start)
		if q.edges {
			if rel == nil {
				return nil, fmt.Errorf("pgengine: a statement returned an edge row with no relation: %.60s", q.sql)
			}
			key = edgeKey(key1, key2, *rel, start)
		}
		t, ok := q.want[key]
		if !ok || seen[key] {
			return nil, fmt.Errorf("pgengine: a statement changed a row it was not asked to (a stale ctid): %.60s...: %w", q.sql, ErrConcurrentWriter)
		}
		seen[key] = true
		if q.places {
			wrote = append(wrote, placed{subject: t.subject, start: t.start, loc: locOf(tid)})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pgengine: reading what a statement changed: %w", err)
	}
	if len(seen) != len(q.want) {
		return nil, fmt.Errorf("pgengine: a statement changed %d of the %d rows it was asked to (a stale ctid): %.60s...: %w", len(seen), len(q.want), q.sql, ErrConcurrentWriter)
	}
	return wrote, nil
}

// execute sends the statements in one round trip, moving the token to last if it is not
// nil, and returns where the rows that were written went, and how many rows each statement
// changed.
func execute(ctx context.Context, tx pgx.Tx, qs []queued, last *int64) ([]placed, []int64, error) {
	b := &pgx.Batch{}
	for _, q := range qs {
		b.Queue(q.sql, q.args...)
	}
	if last != nil {
		b.Queue("UPDATE meta SET last_seq = $1", *last)
	}
	if observe := observeStatement.Load(); observe != nil {
		for _, q := range b.QueuedQueries {
			(*observe)(ctx, tx, q.SQL, q.Arguments)
		}
	}
	br := tx.SendBatch(ctx, b)
	var wrote []placed
	changed := make([]int64, len(qs))
	for i, q := range qs {
		rows, err := br.Query()
		if err != nil {
			_ = br.Close()
			return nil, nil, fmt.Errorf("pgengine: storing a batch: %w", err)
		}
		w, err := q.returned(rows)
		if err != nil {
			_ = br.Close()
			return nil, nil, err
		}
		wrote = append(wrote, w...)
		changed[i] = int64(len(q.want))
	}
	if last != nil {
		if _, err := br.Exec(); err != nil {
			_ = br.Close()
			return nil, nil, fmt.Errorf("pgengine: storing a batch: %w", err)
		}
	}
	if err := br.Close(); err != nil {
		return nil, nil, fmt.Errorf("pgengine: storing a batch: %w", err)
	}
	return wrote, changed, nil
}

// scanStored reads interval rows ascending by start, with where each is.
func scanStored(rows pgx.Rows) ([]pgmodel.Stored, error) {
	defer rows.Close()
	var out []pgmodel.Stored
	for rows.Next() {
		var start int64
		var end, deadline *int64
		var inf bool
		var source, layer int16
		var tid pgtype.TID
		if err := rows.Scan(&start, &end, &inf, &deadline, &source, &layer, &tid); err != nil {
			return nil, fmt.Errorf("pgengine: reading rows: %w", err)
		}
		r, err := dumpRow(start, end, inf, deadline, source)
		if err != nil {
			return nil, err
		}
		out = append(out, pgmodel.Stored{Row: r, Loc: locOf(tid)})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pgengine: reading rows: %w", err)
	}
	return out, nil
}

// resolve reads, for each subject the plan could not write from memory, what its re-fold
// needs, in two round trips for the whole batch: the stored rows from the interval the
// batch lands in, then the log from where that interval starts, and hands them to the
// plan.
func (e *Engine) resolve(ctx context.Context, tx pgx.Tx, plan *pgmodel.BatchPlan, slow []engine.Subject) error {
	if len(slow) == 0 {
		return nil
	}
	b := &pgx.Batch{}
	for _, s := range slow {
		earliest := plan.Earliest(s)
		if s.Kind == engine.SubjectEdge {
			b.Queue(edgeRows, pgmodel.EncodeFingerprint(s.A), pgmodel.EncodeFingerprint(s.B), string(s.Relation), earliest, int64(math.MinInt64))
		} else {
			b.Queue(entityRows, pgmodel.EncodeFingerprint(s.A), earliest, int64(math.MinInt64))
		}
	}
	old := make([][]pgmodel.Stored, len(slow))
	br := tx.SendBatch(ctx, b)
	for i := range slow {
		rows, err := br.Query()
		if err != nil {
			_ = br.Close()
			return fmt.Errorf("pgengine: reading rows: %w", err)
		}
		if old[i], err = scanStored(rows); err != nil {
			_ = br.Close()
			return err
		}
	}
	if err := br.Close(); err != nil {
		return fmt.Errorf("pgengine: reading rows: %w", err)
	}

	b = &pgx.Batch{}
	starts := make([]int64, len(slow))
	for i, s := range slow {
		starts[i] = pgmodel.SlowStart(old[i], plan.Earliest(s))
		if s.Kind == engine.SubjectEdge {
			b.Queue(edgeInputStream, pgmodel.EncodeFingerprint(s.A), pgmodel.EncodeFingerprint(s.B), string(s.Relation), starts[i], e.w.Horizon())
		} else {
			b.Queue(entityInputStream, pgmodel.EncodeFingerprint(s.A), starts[i], e.w.Horizon())
		}
	}
	input := make([][]pgmodel.LogRow, len(slow))
	br = tx.SendBatch(ctx, b)
	for i := range slow {
		rows, err := br.Query()
		if err != nil {
			_ = br.Close()
			return fmt.Errorf("pgengine: reading the log: %w", err)
		}
		if _, err := scanLog(rows, &input[i]); err != nil {
			_ = br.Close()
			return err
		}
	}
	if err := br.Close(); err != nil {
		return fmt.Errorf("pgengine: reading the log: %w", err)
	}
	for i, s := range slow {
		if err := plan.Resolve(s, old[i], input[i]); err != nil {
			return fmt.Errorf("pgengine: folding %s: %w", s.A, err)
		}
	}
	return nil
}

// copyLog stores a batch in the log with COPY, one binary stream for the whole batch.
func (e *Engine) copyLog(ctx context.Context, tx pgx.Tx, batch []engine.Record) error {
	n, err := tx.CopyFrom(ctx, pgx.Identifier{"log"}, pgschema.Names(pgschema.LogColumns),
		pgx.CopyFromSlice(len(batch), func(i int) ([]any, error) { return logCopyRow(batch[i]), nil }))
	if err != nil {
		return fmt.Errorf("pgengine: copying a batch into the log: %w", err)
	}
	if n != int64(len(batch)) {
		return fmt.Errorf("pgengine: copied %d of %d records into the log", n, len(batch))
	}
	return nil
}

// logCopyRow is the values of one record in the columns of [pgschema.LogColumns], as COPY
// sends them.
func logCopyRow(r engine.Record) []any {
	row := make([]any, len(pgschema.LogColumns))
	row[0] = seqToDB(r.Seq)
	row[1] = pgmodel.ToNanos(r.EventTime)
	row[2] = int16(r.Layer)
	row[4] = pgmodel.EncodeFingerprint(r.Subject.A)
	if r.Subject.Kind == engine.SubjectEdge {
		row[3] = byte('g')
		row[5] = pgmodel.EncodeFingerprint(r.Subject.B)
		row[6] = string(r.Subject.Relation)
	} else {
		row[3] = byte('e')
	}
	row[7] = string(r.Producer)
	row[8] = byte(r.Kind)
	row[9] = int64(r.TTL)
	if !r.Through.IsZero() {
		row[10] = pgmodel.ToNanos(r.Through)
	}
	// The column is not null.
	row[11] = r.Payload
	if row[11].([]byte) == nil {
		row[11] = []byte{}
	}
	return row
}

// logTable is the table the log row of an event time is in, for the vacuum schedule.
func (e *Engine) logTable(ns int64) (string, error) {
	if !e.lay.partitioned {
		return "log", nil
	}
	name, _, _, _, err := pgschema.DayBounds(ns)
	return name, err
}

// writeStream is Write for the streaming arms: plan, read what the plan asks for, store
// the log, apply the rows, move the token, commit, and then, only once the transaction is
// stored, let the plan become what the writer remembers.
func (e *Engine) writeStream(batch []engine.Record) (err error) {
	if len(batch) == 0 {
		return nil
	}
	e.wmu.Lock()
	defer e.wmu.Unlock()
	ctx := e.ctx
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("pgengine: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	meta, err := lockMeta(ctx, tx)
	if err != nil {
		return err
	}
	if err := e.checkWriter(meta); err != nil {
		return err
	}
	if err := checkBatch(batch, meta); err != nil {
		return err
	}
	plan, err := e.w.Plan(batch)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			plan.Abort()
		}
	}()
	if err := e.resolve(ctx, tx, plan, plan.Slow()); err != nil {
		return err
	}
	changes, err := plan.Finish()
	if err != nil {
		return err
	}
	if err := buildError(changes); err != nil {
		return err
	}
	created, err := e.createPartitions(ctx, tx, batch)
	if err != nil {
		return err
	}
	if err := e.copyLog(ctx, tx, batch); err != nil {
		return err
	}
	last := batch[len(batch)-1].Seq
	dbLast := seqToDB(last)
	wrote, _, err := execute(ctx, tx, build(changes), &dbLast)
	if err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("pgengine: committing a batch: %w", err)
	}

	// Stored. Now it is what the writer remembers.
	for _, name := range created {
		e.parts[name] = true
	}
	plan.Commit()
	for _, w := range wrote {
		e.w.SetLoc(w.subject, w.start, w.loc)
	}
	e.seq = last
	c := plan.Counts()
	c.Records = uint64(len(batch))
	e.counts.Records += c.Records
	e.counts.FastSubjects += c.FastSubjects
	e.counts.Refolds += c.Refolds
	e.counts.RecordsFolded += c.RecordsFolded
	e.counts.RowsInserted += c.RowsInserted
	e.counts.RowsUpdated += c.RowsUpdated
	e.counts.RowsDeleted += c.RowsDeleted
	e.counts.RowsHeapOnly += c.RowsHeapOnly
	e.counts.RowsFinalized += c.RowsFinalized

	logged := make(map[string]int64)
	for _, r := range batch {
		table, err := e.logTable(pgmodel.ToNanos(r.EventTime))
		if err != nil {
			return err
		}
		logged[table]++
	}
	for table, n := range logged {
		e.vac.Add(table, n, 0, 0)
	}
	var edges, entities [3]int64
	for _, sc := range changes {
		tally := &edges
		if sc.Subject.Kind == engine.SubjectEntity {
			tally = &entities
		}
		for _, c := range sc.Rows {
			tally[c.Kind-pgmodel.Insert]++
		}
	}
	e.vac.Add("edges", edges[0], edges[1], edges[2])
	e.vac.Add("entities", entities[0], entities[1], entities[2])
	return e.vacuumDue(ctx)
}

// vacuumDue runs the vacuums and analyzes the schedule calls for, unless the server's
// autovacuum is left to do it. A table that is due both gets one VACUUM (ANALYZE). They are
// not in a transaction. ANALYZE samples at random on a table with more rows than its sample,
// so the statistics, and the plan shapes that follow, can differ between two loads of one
// stream: that is the registered rule on plan shapes (equal, else not repeatable), and not a
// difference in the schedule, which is a function of the stream.
func (e *Engine) vacuumDue(ctx context.Context) error {
	if e.noVacuum {
		return nil
	}
	for _, a := range e.vac.Due() {
		name := pgx.Identifier{a.Table}.Sanitize()
		stmt := "ANALYZE " + name
		switch {
		case a.Vacuum && a.Analyze:
			stmt = "VACUUM (ANALYZE) " + name
		case a.Vacuum:
			stmt = "VACUUM " + name
		}
		if _, err := e.pool.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("pgengine: %s: %w", stmt, err)
		}
		if a.Vacuum {
			e.counts.Vacuums++
		}
		if a.Analyze {
			e.counts.Analyzes++
		}
	}
	return nil
}

// The reads of the places of the interval rows a retention deletes: those whose range has
// ended at or before the horizon $1, found through the btree on the end. (A provisional row
// has an unbounded range, and is not among them.)
const (
	EdgesEndedSQL    = `SELECT ctid FROM edges WHERE NOT upper_inf(during) AND upper(during) <= $1::bigint`
	EntitiesEndedSQL = `SELECT ctid FROM entities WHERE NOT upper_inf(during) AND upper(during) <= $1::bigint`
)

// deleteChunk is how many places a delete is sent at once.
const deleteChunk = 50_000

// deleteEnded deletes the rows of an interval table that have ended at or before the
// horizon, by their places: the places are read, in the transaction, through the btree on
// the end, and the rows are deleted by them, so that no plan of a delete has an index in it
// (see the comment on edgesDelete). It returns how many rows it deleted.
func deleteEnded(ctx context.Context, tx pgx.Tx, table string, h int64) (uint64, error) {
	read, del := EdgesEndedSQL, edgesDelete
	if table == "entities" {
		read, del = EntitiesEndedSQL, entitiesDelete
	}
	rows, err := tx.Query(ctx, read, h)
	if err != nil {
		return 0, fmt.Errorf("pgengine: reading the %s that have ended: %w", table, err)
	}
	var tids []pgtype.TID
	for rows.Next() {
		var tid pgtype.TID
		if err := rows.Scan(&tid); err != nil {
			rows.Close()
			return 0, fmt.Errorf("pgengine: reading the %s that have ended: %w", table, err)
		}
		tids = append(tids, tid)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("pgengine: reading the %s that have ended: %w", table, err)
	}
	var n uint64
	for chunk := range slices.Chunk(tids, deleteChunk) {
		if observe := observeStatement.Load(); observe != nil {
			(*observe)(ctx, tx, del, []any{chunk})
		}
		tag, err := tx.Exec(ctx, del, chunk)
		if err != nil {
			return 0, fmt.Errorf("pgengine: deleting the %s that have ended: %w", table, err)
		}
		// The places were read in this transaction, which holds the meta row, so every one
		// is its row's.
		if got := tag.RowsAffected(); got != int64(len(chunk)) {
			return 0, fmt.Errorf("pgengine: deleted %d of the %d %s it read the places of: %w", got, len(chunk), table, ErrConcurrentWriter)
		}
		n += uint64(len(chunk))
	}
	return n, nil
}

// retainStream is Retain for the streaming arms. One transaction inserts the baselines,
// sets the horizon and deletes the interval rows that have ended; it commits. Then, outside
// it, a partitioned log has its partitions that are wholly before the horizon detached
// concurrently and dropped, and the rows before the horizon in the partition it falls in
// deleted; an unpartitioned log has them deleted in the transaction and vacuumed as the
// schedule says. Reads filter the log at the horizon, so what a partition still holds is
// never read in the meantime.
func (e *Engine) retainStream(horizon time.Time) error {
	e.wmu.Lock()
	defer e.wmu.Unlock()
	ctx := e.ctx
	h, past := int64(0), horizon.After(engine.MaxEventTime)
	switch {
	case past:
		h = math.MaxInt64
	case horizon.After(engine.MinEventTime):
		h = pgmodel.ToNanos(horizon)
	}
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("pgengine: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	meta, err := lockMeta(ctx, tx)
	if err != nil {
		return err
	}
	if err := e.checkWriter(meta); err != nil {
		return err
	}
	moved := past && !meta.pastEnd || h > meta.horizon
	if !moved {
		// Nothing moves, but a retention that was interrupted after its transaction
		// committed has partitions to drop.
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("pgengine: %w", err)
		}
		if _, err := e.dropLog(ctx, meta.horizon, meta.pastEnd); err != nil {
			return err
		}
		return e.vacuumDue(ctx)
	}
	newHorizon, newPast := max(h, meta.horizon), meta.pastEnd || past

	rp := e.w.PlanRetain(newHorizon)
	var st retainStats
	if _, err := tx.Exec(ctx, "UPDATE meta SET horizon_ns = $1, past_end = $2", newHorizon, newPast); err != nil {
		return fmt.Errorf("pgengine: retaining: %w", err)
	}
	both := []any{newHorizon, newPast}
	steps := []struct {
		sql  string
		args []any
		add  *uint64
	}{
		{baselineInsert, both, &st.baselinesInserted},
		{baselineTrim, nil, &st.baselinesTrimmed},
	}
	if !e.lay.partitioned {
		steps = append(steps, struct {
			sql  string
			args []any
			add  *uint64
		}{logTrim, both, &st.logDeleted})
	}
	for _, s := range steps {
		if observe := observeStatement.Load(); observe != nil {
			(*observe)(ctx, tx, s.sql, s.args)
		}
		tag, err := tx.Exec(ctx, s.sql, s.args...)
		if err != nil {
			return fmt.Errorf("pgengine: retaining: %.40s...: %w", s.sql, err)
		}
		*s.add += uint64(tag.RowsAffected())
	}
	edgesGone, err := deleteEnded(ctx, tx, "edges", newHorizon)
	if err != nil {
		return err
	}
	entitiesGone, err := deleteEnded(ctx, tx, "entities", newHorizon)
	if err != nil {
		return err
	}
	// The provisional rows the horizon has passed: no index covers a deadline, so they
	// are named by where the writer put them, and checked as every change is.
	var edgeTIDs, entityTIDs []pgtype.TID
	wantEdges, wantEntities := newTargets(), newTargets()
	for _, l := range rp.Deleted {
		if l.Subject.Kind == engine.SubjectEdge {
			edgeTIDs = append(edgeTIDs, tidOf(l.Loc))
			wantEdges[keyOf(l.Subject, l.Start)] = target{l.Subject, l.Start}
		} else {
			entityTIDs = append(entityTIDs, tidOf(l.Loc))
			wantEntities[keyOf(l.Subject, l.Start)] = target{l.Subject, l.Start}
		}
	}
	var qs []queued
	if len(edgeTIDs) > 0 {
		qs = append(qs, queued{sql: edgesDeleteReturning, args: []any{edgeTIDs}, want: wantEdges, edges: true})
	}
	if len(entityTIDs) > 0 {
		qs = append(qs, queued{sql: entitiesDeleteReturning, args: []any{entityTIDs}, want: wantEntities})
	}
	if len(qs) > 0 {
		_, changed, err := execute(ctx, tx, qs, nil)
		if err != nil {
			return err
		}
		// The provisional rows count with the others.
		for i, q := range qs {
			if q.edges {
				edgesGone += uint64(changed[i])
			} else {
				entitiesGone += uint64(changed[i])
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("pgengine: committing a retention: %w", err)
	}

	e.w.CommitRetain(newHorizon)
	e.pastEnd = newPast
	e.counts.RowsDeleted += edgesGone + entitiesGone
	e.vac.Add("edges", 0, 0, int64(edgesGone))
	e.vac.Add("entities", 0, 0, int64(entitiesGone))
	e.vac.Add("log_base", int64(st.baselinesInserted), 0, int64(st.baselinesTrimmed))
	if !e.lay.partitioned {
		e.vac.Add("log", 0, 0, int64(st.logDeleted))
	}
	if _, err := e.dropLog(ctx, newHorizon, newPast); err != nil {
		return err
	}
	return e.vacuumDue(ctx)
}

// dropLog does what is left of a retention in a partitioned log, outside its
// transaction: a partition that is wholly before the horizon is detached concurrently and
// dropped, and the rows before the horizon in the partition the horizon falls in are
// deleted. It reports how many partitions it dropped.
func (e *Engine) dropLog(ctx context.Context, horizon int64, past bool) (int, error) {
	if !e.lay.partitioned {
		return 0, nil
	}
	names, err := e.listPartitions(ctx, e.pool)
	if err != nil {
		return 0, err
	}
	dropped := 0
	for _, name := range names {
		lo, err := partitionStart(name)
		if err != nil {
			return dropped, err
		}
		_, _, hi, unbounded, err := pgschema.DayBounds(lo)
		if err != nil {
			return dropped, err
		}
		if !past && (unbounded || hi > horizon) {
			continue
		}
		id := pgx.Identifier{name}.Sanitize()
		if _, err := e.pool.Exec(ctx, "ALTER TABLE log DETACH PARTITION "+id+" CONCURRENTLY"); err != nil {
			return dropped, fmt.Errorf("pgengine: detaching partition %s: %w", name, err)
		}
		if _, err := e.pool.Exec(ctx, "DROP TABLE "+id); err != nil {
			return dropped, fmt.Errorf("pgengine: dropping partition %s: %w", name, err)
		}
		delete(e.parts, name)
		e.vac.Drop(name)
		e.counts.PartitionsDropped++
		dropped++
	}
	if !past {
		// What is left before the horizon is in the partition it falls in.
		tag, err := e.pool.Exec(ctx, logTrim, horizon, false)
		if err != nil {
			return dropped, fmt.Errorf("pgengine: deleting the log before the horizon: %w", err)
		}
		if n := tag.RowsAffected(); n > 0 {
			name, _, _, _, err := pgschema.DayBounds(horizon)
			if err != nil {
				return dropped, err
			}
			e.vac.Add(name, 0, 0, n)
		}
	}
	return dropped, nil
}

// listPartitions lists the day partitions of the log the catalog has, by name. A partition
// whose detaching is pending is not listed: it is gone to every new statement.
func (e *Engine) listPartitions(ctx context.Context, q QueryQuerier) ([]string, error) {
	rows, err := q.Query(ctx, `SELECT c.relname FROM pg_inherits i
JOIN pg_class c ON c.oid = i.inhrelid JOIN pg_class p ON p.oid = i.inhparent JOIN pg_namespace n ON n.oid = p.relnamespace
WHERE p.relname = 'log' AND n.nspname = $1 AND NOT i.inhdetachpending ORDER BY 1`, e.schema)
	if err != nil {
		return nil, fmt.Errorf("pgengine: listing the partitions: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("pgengine: listing the partitions: %w", err)
		}
		out = append(out, name)
	}
	return out, rows.Err()
}

// partitionStart is the first nanosecond of the day a partition's name says.
func partitionStart(name string) (int64, error) {
	day, err := time.Parse("20060102", name[len("log_p"):])
	if err != nil || len(name) != len("log_p20060102") {
		return 0, fmt.Errorf("pgengine: %q is not the name of a day partition", name)
	}
	return day.UnixNano(), nil
}

// restore gives a streaming arm's engine what it needs to continue a schema it opens:
// the planner's memory (the last row of each subject, with where it is, and the latest
// record of each of its producers), the horizon and the newest event time, the day
// partitions of the log, and the row counts the vacuum schedule starts from. All of it is
// read in one snapshot. An empty schema has none of it.
func (e *Engine) restore(ctx context.Context) error {
	tx, err := e.pool.BeginTx(ctx, readOnlyRepeatable)
	if err != nil {
		return fmt.Errorf("pgengine: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var last int64
	var horizon *int64
	var past bool
	if err := tx.QueryRow(ctx, "SELECT last_seq, horizon_ns, past_end FROM meta").Scan(&last, &horizon, &past); err != nil {
		return fmt.Errorf("pgengine: reading the meta row: %w", err)
	}
	e.w, e.vac = pgmodel.NewWriter(pgmodel.WriterConfig{Naive: e.lay.naive}), pgmodel.NewVacuumSchedule()
	h := int64(0)
	if horizon != nil {
		h = *horizon
	}
	e.seq, e.pastEnd = seqFromDB(last), past

	if e.lay.partitioned {
		names, err := e.listPartitions(ctx, tx)
		if err != nil {
			return err
		}
		for _, name := range names {
			e.parts[name] = true
		}
	}

	type memory struct {
		layer catalog.Layer
		last  *pgmodel.Stored
		prods []pgmodel.LogRow
	}
	subjects := make(map[engine.Subject]*memory)
	var order []engine.Subject
	get := func(s engine.Subject, layer catalog.Layer) *memory {
		m := subjects[s]
		if m == nil {
			m = &memory{layer: layer}
			subjects[s] = m
			order = append(order, s)
		}
		return m
	}
	rows, err := tx.Query(ctx, `SELECT DISTINCT ON (src, dst, rel) src, dst, rel, layer, start_ns, upper(during), upper_inf(during), deadline_ns, end_source, ctid
FROM edges ORDER BY src, dst, rel, start_ns DESC`)
	if err != nil {
		return fmt.Errorf("pgengine: reading the last row of each edge: %w", err)
	}
	for rows.Next() {
		var src, dst []byte
		var rel string
		var layer, source int16
		var start int64
		var end, deadline *int64
		var inf bool
		var tid pgtype.TID
		if err := rows.Scan(&src, &dst, &rel, &layer, &start, &end, &inf, &deadline, &source, &tid); err != nil {
			rows.Close()
			return fmt.Errorf("pgengine: reading the last row of each edge: %w", err)
		}
		row, err := dumpRow(start, end, inf, deadline, source)
		if err != nil {
			rows.Close()
			return err
		}
		a, err := pgmodel.DecodeFingerprint(src)
		if err != nil {
			rows.Close()
			return err
		}
		z, err := pgmodel.DecodeFingerprint(dst)
		if err != nil {
			rows.Close()
			return err
		}
		get(engine.EdgeSubject(a, z, catalog.RelationType(rel)), catalog.Layer(layer)).last = &pgmodel.Stored{Row: row, Loc: locOf(tid)}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	rows, err = tx.Query(ctx, `SELECT DISTINCT ON (fp) fp, layer, start_ns, upper(during), upper_inf(during), deadline_ns, end_source, ctid
FROM entities ORDER BY fp, start_ns DESC`)
	if err != nil {
		return fmt.Errorf("pgengine: reading the last row of each entity: %w", err)
	}
	for rows.Next() {
		var fp []byte
		var layer, source int16
		var start int64
		var end, deadline *int64
		var inf bool
		var tid pgtype.TID
		if err := rows.Scan(&fp, &layer, &start, &end, &inf, &deadline, &source, &tid); err != nil {
			rows.Close()
			return fmt.Errorf("pgengine: reading the last row of each entity: %w", err)
		}
		row, err := dumpRow(start, end, inf, deadline, source)
		if err != nil {
			rows.Close()
			return err
		}
		f, err := pgmodel.DecodeFingerprint(fp)
		if err != nil {
			rows.Close()
			return err
		}
		get(engine.EntitySubject(f), catalog.Layer(layer)).last = &pgmodel.Stored{Row: row, Loc: locOf(tid)}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	// The latest record of each producer of each subject, baselines included, and the
	// newest event time of all.
	newest := int64(math.MinInt64)
	rows, err = tx.Query(ctx, `SELECT DISTINCT ON (subject_kind, src, dst, rel, producer) subject_kind::text, src, dst, rel, layer, producer, seq, event_ns, kind::int4, ttl_ns, through_ns
FROM (
  SELECT subject_kind, src, dst, rel, layer, producer, seq, event_ns, kind, ttl_ns, through_ns FROM log
  UNION ALL
  SELECT subject_kind, src, dst, rel, layer, producer, seq, event_ns, kind, ttl_ns, through_ns FROM log_base
) l
ORDER BY subject_kind, src, dst, rel, producer, event_ns DESC, seq DESC`)
	if err != nil {
		return fmt.Errorf("pgengine: reading the latest record of each producer: %w", err)
	}
	for rows.Next() {
		var kind string
		var src, dst []byte
		var rel *string
		var layer int16
		var producer string
		var seq, event, ttl int64
		var k int32
		var through *int64
		if err := rows.Scan(&kind, &src, &dst, &rel, &layer, &producer, &seq, &event, &k, &ttl, &through); err != nil {
			rows.Close()
			return fmt.Errorf("pgengine: reading the latest record of each producer: %w", err)
		}
		a, err := pgmodel.DecodeFingerprint(src)
		if err != nil {
			rows.Close()
			return err
		}
		s := engine.EntitySubject(a)
		if kind == "g" {
			z, err := pgmodel.DecodeFingerprint(dst)
			if err != nil {
				rows.Close()
				return fmt.Errorf("pgengine: an edge record with no target: %w", err)
			}
			if rel == nil {
				rows.Close()
				return errors.New("pgengine: an edge record with no relation")
			}
			s = engine.EdgeSubject(a, z, catalog.RelationType(*rel))
		}
		row := pgmodel.LogRow{
			Producer: lifecycle.Producer(producer), Seq: seqFromDB(seq), Event: event,
			TTL: time.Duration(ttl), Kind: lifecycle.Kind(k),
		}
		if through != nil {
			row.Through, row.HasThrough = *through, true
		}
		m := get(s, catalog.Layer(layer))
		m.prods = append(m.prods, row)
		newest = max(newest, event)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, s := range order {
		m := subjects[s]
		e.w.Restore(s, m.layer, m.last, m.prods)
	}
	e.w.SetState(h, newest)

	// The row counts the vacuum schedule starts from: what each table holds now.
	for _, t := range []string{"edges", "entities", "log_base"} {
		var n int64
		if err := tx.QueryRow(ctx, "SELECT count(*) FROM "+t).Scan(&n); err != nil {
			return fmt.Errorf("pgengine: counting %s: %w", t, err)
		}
		e.vac.Restore(t, n)
	}
	if e.lay.partitioned {
		for name := range e.parts {
			var n int64
			if err := tx.QueryRow(ctx, "SELECT count(*) FROM "+pgx.Identifier{name}.Sanitize()).Scan(&n); err != nil {
				return fmt.Errorf("pgengine: counting %s: %w", name, err)
			}
			e.vac.Restore(name, n)
		}
	} else {
		var n int64
		if err := tx.QueryRow(ctx, "SELECT count(*) FROM log").Scan(&n); err != nil {
			return fmt.Errorf("pgengine: counting the log: %w", err)
		}
		e.vac.Restore("log", n)
	}
	return nil
}
