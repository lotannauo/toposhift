package pgengine

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/lotannauo/toposhift/bench/postgres/pgmodel"
	"github.com/lotannauo/toposhift/bench/postgres/pgschema"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// logInsert stores a batch of records. The arrays are parallel; a dst, rel or
// through that is null is null in the array.
const logInsert = `INSERT INTO log (seq, event_ns, layer, subject_kind, src, dst, rel, producer, kind, ttl_ns, through_ns, payload)
SELECT d.seq, d.event_ns, d.layer, d.subject_kind::"char", d.src, d.dst, d.rel, d.producer, d.kind::int4::"char", d.ttl_ns, d.through_ns, d.payload
FROM unnest($1::bigint[], $2::bigint[], $3::smallint[], $4::text[], $5::bytea[], $6::bytea[], $7::text[], $8::text[], $9::smallint[], $10::bigint[], $11::bigint[], $12::bytea[])
  AS d(seq, event_ns, layer, subject_kind, src, dst, rel, producer, kind, ttl_ns, through_ns, payload)`

// The statements that apply a batch's row changes, one of each kind per table. They
// are set-based over arrays, so a batch costs a statement per kind however many rows
// it changes.
const (
	edgesDelete = `DELETE FROM edges e
USING unnest($1::bytea[], $2::bytea[], $3::text[], $4::bigint[]) AS d(src, dst, rel, start_ns)
WHERE e.src = d.src AND e.dst = d.dst AND e.rel = d.rel AND e.start_ns = d.start_ns`
	edgesUpdate = `UPDATE edges e SET during = int8range(d.start_ns, d.end_ns), deadline_ns = d.deadline_ns, end_source = d.end_source
FROM unnest($1::bytea[], $2::bytea[], $3::text[], $4::bigint[], $5::bigint[], $6::bigint[], $7::smallint[])
  AS d(src, dst, rel, start_ns, end_ns, deadline_ns, end_source)
WHERE e.src = d.src AND e.dst = d.dst AND e.rel = d.rel AND e.start_ns = d.start_ns`
	edgesInsert = `INSERT INTO edges (src, dst, rel, layer, start_ns, during, deadline_ns, end_source)
SELECT d.src, d.dst, d.rel, d.layer, d.start_ns, int8range(d.start_ns, d.end_ns), d.deadline_ns, d.end_source
FROM unnest($1::bytea[], $2::bytea[], $3::text[], $4::smallint[], $5::bigint[], $6::bigint[], $7::bigint[], $8::smallint[])
  AS d(src, dst, rel, layer, start_ns, end_ns, deadline_ns, end_source)`

	entitiesDelete = `DELETE FROM entities e
USING unnest($1::bytea[], $2::bigint[]) AS d(fp, start_ns)
WHERE e.fp = d.fp AND e.start_ns = d.start_ns`
	entitiesUpdate = `UPDATE entities e SET during = int8range(d.start_ns, d.end_ns), deadline_ns = d.deadline_ns, end_source = d.end_source
FROM unnest($1::bytea[], $2::bigint[], $3::bigint[], $4::bigint[], $5::smallint[]) AS d(fp, start_ns, end_ns, deadline_ns, end_source)
WHERE e.fp = d.fp AND e.start_ns = d.start_ns`
	entitiesInsert = `INSERT INTO entities (fp, layer, start_ns, during, deadline_ns, end_source)
SELECT d.fp, d.layer, d.start_ns, int8range(d.start_ns, d.end_ns), d.deadline_ns, d.end_source
FROM unnest($1::bytea[], $2::smallint[], $3::bigint[], $4::bigint[], $5::bigint[], $6::smallint[]) AS d(fp, layer, start_ns, end_ns, deadline_ns, end_source)`
)

// The stored rows of a subject from the start of the last row that begins at or
// before $4 (or from $5, the smallest start, if there is none), and then the log of the
// subject, baselines included, from that start. The second query is given that start.
const (
	edgeRows = `SELECT start_ns, upper(during), upper_inf(during), deadline_ns, end_source, layer
FROM edges WHERE src = $1 AND dst = $2 AND rel = $3
  AND start_ns >= coalesce((SELECT max(start_ns) FROM edges WHERE src = $1 AND dst = $2 AND rel = $3 AND start_ns <= $4::bigint), $5::bigint)
ORDER BY start_ns`
	entityRows = `SELECT start_ns, upper(during), upper_inf(during), deadline_ns, end_source, layer
FROM entities WHERE fp = $1
  AND start_ns >= coalesce((SELECT max(start_ns) FROM entities WHERE fp = $1 AND start_ns <= $2::bigint), $3::bigint)
ORDER BY start_ns`

	edgeInput = `SELECT seq, event_ns, producer, kind::int4, ttl_ns, through_ns, layer FROM log
  WHERE subject_kind = 'g' AND src = $1 AND dst = $2 AND rel = $3 AND event_ns >= $4::bigint
UNION ALL
SELECT seq, event_ns, producer, kind::int4, ttl_ns, through_ns, layer FROM log_base
  WHERE subject_kind = 'g' AND src = $1 AND dst = $2 AND rel = $3 AND event_ns >= $4::bigint`
	entityInput = `SELECT seq, event_ns, producer, kind::int4, ttl_ns, through_ns, layer FROM log
  WHERE subject_kind = 'e' AND src = $1 AND event_ns >= $2::bigint
UNION ALL
SELECT seq, event_ns, producer, kind::int4, ttl_ns, through_ns, layer FROM log_base
  WHERE subject_kind = 'e' AND src = $1 AND event_ns >= $2::bigint`
)

// metaState is the meta row, which a write or a retention holds locked.
type metaState struct {
	lastSeq uint64
	horizon int64
	pastEnd bool
}

func lockMeta(ctx context.Context, tx pgx.Tx) (metaState, error) {
	var last int64
	var horizon *int64
	var m metaState
	if err := tx.QueryRow(ctx, "SELECT last_seq, horizon_ns, past_end FROM meta FOR UPDATE").Scan(&last, &horizon, &m.pastEnd); err != nil {
		return metaState{}, fmt.Errorf("pgengine: reading the meta row: %w", err)
	}
	m.lastSeq = seqFromDB(last)
	if horizon != nil {
		m.horizon = *horizon
	}
	return m, nil
}

// subjectBatch is what one batch holds for one subject.
type subjectBatch struct {
	subject  engine.Subject
	layer    catalog.Layer
	records  []engine.Record
	earliest int64
	// Filled in as the batch is planned.
	start   int64
	old     []pgmodel.Row
	input   []pgmodel.LogRow
	changes pgmodel.Changes
}

// Write implements [engine.Engine]. The batch is validated whole before anything is
// stored, and stored in one transaction, so a refused batch changes nothing.
func (e *Engine) Write(batch []engine.Record) error {
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
	subjects, err := e.validate(batch, meta)
	if err != nil {
		return err
	}
	if err := e.load(ctx, tx, subjects); err != nil {
		return err
	}
	var folded, refolded uint64
	var inserted, updated, deleted uint64
	for _, sb := range subjects {
		_, ch, err := pgmodel.Refold(sb.old, sb.input, sb.start, meta.horizon)
		if err != nil {
			// The batch was validated, so a fold that fails is a bug.
			return fmt.Errorf("pgengine: folding %s: %w", sb.subject.A, err)
		}
		sb.changes = ch
		refolded++
		folded += uint64(len(sb.input))
		inserted += uint64(len(ch.Inserted))
		updated += uint64(len(ch.Updated))
		deleted += uint64(len(ch.Deleted))
	}

	created, err := e.createPartitions(ctx, tx, batch)
	if err != nil {
		return err
	}
	if err := apply(ctx, tx, batch, subjects, seqToDB(batch[len(batch)-1].Seq)); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("pgengine: committing a batch: %w", err)
	}
	for _, name := range created {
		e.parts[name] = true
	}
	e.counts.Records += uint64(len(batch))
	e.counts.Refolds += refolded
	e.counts.RecordsFolded += folded
	e.counts.RowsInserted += inserted
	e.counts.RowsUpdated += updated
	e.counts.RowsDeleted += deleted
	return nil
}

// validate checks every record, in order, by the rules of the contract and of the
// [pgmodel.Model], and groups the records by subject in the order the subjects first
// appear.
func (e *Engine) validate(batch []engine.Record, meta metaState) ([]*subjectBatch, error) {
	seq := meta.lastSeq
	byID := make(map[engine.Subject]*subjectBatch)
	var order []*subjectBatch
	for _, r := range batch {
		if err := r.Validate(); err != nil {
			return nil, err
		}
		if meta.pastEnd || pgmodel.ToNanos(r.EventTime) < meta.horizon {
			return nil, fmt.Errorf("record seq %d at %s: %w", r.Seq, r.EventTime.Format(time.RFC3339), engine.ErrBeforeHorizon)
		}
		if r.Seq <= seq {
			return nil, fmt.Errorf("record seq %d is not above the last written seq %d: %w", r.Seq, seq, engine.ErrInvalid)
		}
		seq = r.Seq
		sb := byID[r.Subject]
		if sb == nil {
			sb = &subjectBatch{subject: r.Subject, layer: r.Layer, earliest: math.MaxInt64}
			byID[r.Subject] = sb
			order = append(order, sb)
		}
		if sb.layer != r.Layer {
			return nil, fmt.Errorf("record seq %d: subject is stored in layer %s, not %s: %w", r.Seq, sb.layer, r.Layer, engine.ErrInvalid)
		}
		sb.records = append(sb.records, r)
		sb.earliest = min(sb.earliest, pgmodel.ToNanos(r.EventTime))
	}
	return order, nil
}

// load reads what each subject needs to be re-folded, in two round trips for the whole
// batch: the stored rows from the interval the batch lands in, then the log from where
// that interval starts. It also finds a subject that was stored in another layer,
// which its rows and its log say.
func (e *Engine) load(ctx context.Context, tx pgx.Tx, subjects []*subjectBatch) error {
	b := &pgx.Batch{}
	for _, sb := range subjects {
		earliest := sb.earliest
		if e.lay.whole {
			earliest = math.MinInt64
		}
		if sb.subject.Kind == engine.SubjectEdge {
			b.Queue(edgeRows, pgmodel.EncodeFingerprint(sb.subject.A), pgmodel.EncodeFingerprint(sb.subject.B), string(sb.subject.Relation), earliest, int64(math.MinInt64))
		} else {
			b.Queue(entityRows, pgmodel.EncodeFingerprint(sb.subject.A), earliest, int64(math.MinInt64))
		}
	}
	br := tx.SendBatch(ctx, b)
	for _, sb := range subjects {
		rows, err := br.Query()
		if err != nil {
			_ = br.Close()
			return fmt.Errorf("pgengine: reading rows: %w", err)
		}
		layer, err := scanRows(rows, &sb.old)
		if err != nil {
			_ = br.Close()
			return err
		}
		if layer != 0 && layer != sb.layer {
			_ = br.Close()
			return layerError(sb, layer)
		}
		sb.start = math.MinInt64
		if len(sb.old) > 0 && sb.old[0].Start <= sb.earliest && !e.lay.whole {
			sb.start = sb.old[0].Start
		}
	}
	if err := br.Close(); err != nil {
		return fmt.Errorf("pgengine: reading rows: %w", err)
	}

	b = &pgx.Batch{}
	for _, sb := range subjects {
		if sb.subject.Kind == engine.SubjectEdge {
			b.Queue(edgeInput, pgmodel.EncodeFingerprint(sb.subject.A), pgmodel.EncodeFingerprint(sb.subject.B), string(sb.subject.Relation), sb.start)
		} else {
			b.Queue(entityInput, pgmodel.EncodeFingerprint(sb.subject.A), sb.start)
		}
	}
	br = tx.SendBatch(ctx, b)
	for _, sb := range subjects {
		rows, err := br.Query()
		if err != nil {
			_ = br.Close()
			return fmt.Errorf("pgengine: reading the log: %w", err)
		}
		layer, err := scanLog(rows, &sb.input)
		if err != nil {
			_ = br.Close()
			return err
		}
		if layer != 0 && layer != sb.layer {
			_ = br.Close()
			return layerError(sb, layer)
		}
		for _, r := range sb.records {
			sb.input = append(sb.input, pgmodel.LogRowOf(r))
		}
	}
	if err := br.Close(); err != nil {
		return fmt.Errorf("pgengine: reading the log: %w", err)
	}
	return nil
}

func layerError(sb *subjectBatch, stored catalog.Layer) error {
	return fmt.Errorf("record seq %d: subject is stored in layer %s, not %s: %w", sb.records[0].Seq, stored, sb.layer, engine.ErrInvalid)
}

// scanRows reads interval rows, ascending by start, and returns the layer they
// carry (zero if there are none, and the first layer if they disagree, which the
// caller then reports against the batch).
func scanRows(rows pgx.Rows, into *[]pgmodel.Row) (catalog.Layer, error) {
	defer rows.Close()
	var layer catalog.Layer
	for rows.Next() {
		var start int64
		var end, deadline *int64
		var inf bool
		var source, l int16
		if err := rows.Scan(&start, &end, &inf, &deadline, &source, &l); err != nil {
			return 0, fmt.Errorf("pgengine: reading rows: %w", err)
		}
		r := pgmodel.Row{Start: start, EndSource: lifecycle.EndSource(source)}
		switch {
		case inf:
			r.Open = true
		case end == nil:
			return 0, fmt.Errorf("pgengine: a row at %d has no upper bound and is not unbounded", start)
		default:
			r.End = *end
		}
		if deadline != nil {
			r.Deadline, r.HasDeadline = *deadline, true
		}
		if layer == 0 {
			layer = catalog.Layer(l)
		}
		*into = append(*into, r)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("pgengine: reading rows: %w", err)
	}
	return layer, nil
}

// scanLog reads log rows in the columns of edgeInput and entityInput.
func scanLog(rows pgx.Rows, into *[]pgmodel.LogRow) (catalog.Layer, error) {
	defer rows.Close()
	var layer catalog.Layer
	for rows.Next() {
		var seq, event, ttl int64
		var through *int64
		var producer string
		var kind int32
		var l int16
		if err := rows.Scan(&seq, &event, &producer, &kind, &ttl, &through, &l); err != nil {
			return 0, fmt.Errorf("pgengine: reading the log: %w", err)
		}
		row := pgmodel.LogRow{
			Producer: lifecycle.Producer(producer), Seq: seqFromDB(seq), Event: event,
			TTL: time.Duration(ttl), Kind: lifecycle.Kind(kind),
		}
		if through != nil {
			row.Through, row.HasThrough = *through, true
		}
		if layer == 0 {
			layer = catalog.Layer(l)
		}
		*into = append(*into, row)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("pgengine: reading the log: %w", err)
	}
	return layer, nil
}

// createPartitions makes the day partitions of the log the batch reaches, in the
// transaction, and returns their names for the engine to remember once it commits.
func (e *Engine) createPartitions(ctx context.Context, tx pgx.Tx, batch []engine.Record) ([]string, error) {
	if !e.lay.partitioned {
		return nil, nil
	}
	var created []string
	seen := make(map[string]bool)
	for _, r := range batch {
		name, stmt, err := pgschema.DayPartition(pgmodel.ToNanos(r.EventTime))
		if err != nil {
			return nil, err
		}
		if e.parts[name] || seen[name] {
			continue
		}
		seen[name] = true
		if _, err := tx.Exec(ctx, stmt, pgx.QueryExecModeSimpleProtocol); err != nil {
			return nil, fmt.Errorf("pgengine: creating partition %s: %w", name, err)
		}
		created = append(created, name)
	}
	return created, nil
}

// apply stores the records and the row changes of a batch, and moves the token, in
// one round trip.
func apply(ctx context.Context, tx pgx.Tx, batch []engine.Record, subjects []*subjectBatch, last int64) error {
	b := &pgx.Batch{}
	b.Queue(logInsert, logArrays(batch)...)

	var edgeDel edgeDeletes
	var edgeUpd, edgeIns edgeCols
	var entityDel entityDeletes
	var entityUpd, entityIns entityCols
	for _, sb := range subjects {
		layer := int16(sb.layer)
		for _, start := range sb.changes.Deleted {
			if sb.subject.Kind == engine.SubjectEdge {
				edgeDel.add(sb.subject, start)
			} else {
				entityDel.add(sb.subject, start)
			}
		}
		for _, r := range sb.changes.Updated {
			if sb.subject.Kind == engine.SubjectEdge {
				edgeUpd.add(sb.subject, layer, r)
			} else {
				entityUpd.add(sb.subject, layer, r)
			}
		}
		for _, r := range sb.changes.Inserted {
			if sb.subject.Kind == engine.SubjectEdge {
				edgeIns.add(sb.subject, layer, r)
			} else {
				entityIns.add(sb.subject, layer, r)
			}
		}
	}
	if err := firstError(edgeUpd.err, edgeIns.err, entityUpd.err, entityIns.err); err != nil {
		return err
	}
	// Deletes first: a primary key is free before an insert needs it.
	if len(edgeDel.src) > 0 {
		b.Queue(edgesDelete, edgeDel.src, edgeDel.dst, edgeDel.rel, edgeDel.start)
	}
	if len(entityDel.fp) > 0 {
		b.Queue(entitiesDelete, entityDel.fp, entityDel.start)
	}
	if len(edgeUpd.src) > 0 {
		b.Queue(edgesUpdate, edgeUpd.src, edgeUpd.dst, edgeUpd.rel, edgeUpd.start, edgeUpd.end, edgeUpd.deadline, edgeUpd.source)
	}
	if len(entityUpd.fp) > 0 {
		b.Queue(entitiesUpdate, entityUpd.fp, entityUpd.start, entityUpd.end, entityUpd.deadline, entityUpd.source)
	}
	if len(edgeIns.src) > 0 {
		b.Queue(edgesInsert, edgeIns.src, edgeIns.dst, edgeIns.rel, edgeIns.layer, edgeIns.start, edgeIns.end, edgeIns.deadline, edgeIns.source)
	}
	if len(entityIns.fp) > 0 {
		b.Queue(entitiesInsert, entityIns.fp, entityIns.layer, entityIns.start, entityIns.end, entityIns.deadline, entityIns.source)
	}
	b.Queue("UPDATE meta SET last_seq = $1", last)

	br := tx.SendBatch(ctx, b)
	for range b.Len() {
		if _, err := br.Exec(); err != nil {
			_ = br.Close()
			return fmt.Errorf("pgengine: storing a batch: %w", err)
		}
	}
	if err := br.Close(); err != nil {
		return fmt.Errorf("pgengine: storing a batch: %w", err)
	}
	return nil
}

func firstError(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// logArrays is the parallel arrays logInsert takes.
func logArrays(batch []engine.Record) []any {
	n := len(batch)
	seq, event, ttl := make([]int64, n), make([]int64, n), make([]int64, n)
	layer, kind := make([]int16, n), make([]int16, n)
	subjectKind, producer := make([]string, n), make([]string, n)
	src, dst, payload := make([][]byte, n), make([][]byte, n), make([][]byte, n)
	rel := make([]*string, n)
	through := make([]*int64, n)
	for i, r := range batch {
		seq[i], event[i], ttl[i] = seqToDB(r.Seq), pgmodel.ToNanos(r.EventTime), int64(r.TTL)
		layer[i], kind[i], producer[i] = int16(r.Layer), int16(r.Kind), string(r.Producer)
		src[i] = pgmodel.EncodeFingerprint(r.Subject.A)
		if r.Subject.Kind == engine.SubjectEdge {
			subjectKind[i] = "g"
			dst[i] = pgmodel.EncodeFingerprint(r.Subject.B)
			s := string(r.Subject.Relation)
			rel[i] = &s
		} else {
			subjectKind[i] = "e"
		}
		if !r.Through.IsZero() {
			t := pgmodel.ToNanos(r.Through)
			through[i] = &t
		}
		// The column is not null, and a null element of the array would be null.
		payload[i] = r.Payload
		if payload[i] == nil {
			payload[i] = []byte{}
		}
	}
	return []any{seq, event, layer, subjectKind, src, dst, rel, producer, kind, ttl, through, payload}
}

// The parallel arrays of the row statements. A row's end is null when its range is
// unbounded, and its deadline is null unless it is provisional.

type edgeDeletes struct {
	src, dst [][]byte
	rel      []string
	start    []int64
}

func (d *edgeDeletes) add(s engine.Subject, start int64) {
	d.src = append(d.src, pgmodel.EncodeFingerprint(s.A))
	d.dst = append(d.dst, pgmodel.EncodeFingerprint(s.B))
	d.rel = append(d.rel, string(s.Relation))
	d.start = append(d.start, start)
}

type entityDeletes struct {
	fp    [][]byte
	start []int64
}

func (d *entityDeletes) add(s engine.Subject, start int64) {
	d.fp = append(d.fp, pgmodel.EncodeFingerprint(s.A))
	d.start = append(d.start, start)
}

type rowColumns struct {
	layer    []int16
	start    []int64
	end      []*int64
	deadline []*int64
	source   []int16
	err      error
}

func (c *rowColumns) add(layer int16, r pgmodel.Row) {
	var end, deadline *int64
	switch {
	case r.HasDeadline:
		d := r.Deadline
		deadline = &d
	case !r.Open:
		// A range with no length is empty, and an empty range has no lower bound for
		// the start column's check to compare, so it would be stored without complaint.
		if r.End <= r.Start {
			c.err = errors.Join(c.err, fmt.Errorf("pgengine: a row [%d, %d) has no length", r.Start, r.End))
		}
		e := r.End
		end = &e
	}
	c.layer = append(c.layer, layer)
	c.start = append(c.start, r.Start)
	c.end = append(c.end, end)
	c.deadline = append(c.deadline, deadline)
	c.source = append(c.source, int16(r.EndSource))
}

type edgeCols struct {
	rowColumns
	src, dst [][]byte
	rel      []string
}

func (c *edgeCols) add(s engine.Subject, layer int16, r pgmodel.Row) {
	c.src = append(c.src, pgmodel.EncodeFingerprint(s.A))
	c.dst = append(c.dst, pgmodel.EncodeFingerprint(s.B))
	c.rel = append(c.rel, string(s.Relation))
	c.rowColumns.add(layer, r)
}

type entityCols struct {
	rowColumns
	fp [][]byte
}

func (c *entityCols) add(s engine.Subject, layer int16, r pgmodel.Row) {
	c.fp = append(c.fp, pgmodel.EncodeFingerprint(s.A))
	c.rowColumns.add(layer, r)
}
