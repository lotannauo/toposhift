package pgengine

import (
	"cmp"
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
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// The phases a load's WAL is counted in. A load is one stream, written with COPY, and
// then built. The stream is the span from the first record to the end of the last
// retention; its COPYs, retentions and the vacuums they call for are interleaved, so
// they are counted as three parts of it, by the records the server wrote for each.
const (
	// PhaseStream is the whole stream, from the first record to the last retention.
	PhaseStream = "stream"
	// PhaseCopy is the COPY of the log.
	PhaseCopy = "copy"
	// PhaseRetain is the retentions of the stream: baselines inserted, the log before the
	// horizon deleted.
	PhaseRetain = "retain"
	// PhaseVacuumStream is the vacuums the writer ran in the stream because the rows a
	// retention deleted passed autovacuum's threshold (see [VacuumDue]).
	PhaseVacuumStream = "vacuum-stream"
	// PhaseFold is the pass that folds the log into intervals and the COPY of them.
	PhaseFold = "fold"
	// PhaseDictionary is the COPY of the dictionary of the id arm.
	PhaseDictionary = "dictionary"
	// PhaseIndexes is the creation of the indexes.
	PhaseIndexes = "indexes"
	// PhaseVacuum is the final VACUUM (FREEZE, ANALYZE).
	PhaseVacuum = "vacuum"
)

var phaseOrder = []string{PhaseStream, PhaseCopy, PhaseRetain, PhaseVacuumStream, PhaseFold, PhaseDictionary, PhaseIndexes, PhaseVacuum}

// Autovacuum's formula for a table, with the scale factor settings.conf registers for
// the deterministic regime: a table is vacuumed when its dead rows pass the threshold
// plus the scale factor of its live rows.
const (
	vacuumThreshold   = 50
	vacuumScaleFactor = 0.01
)

// VacuumDue says whether a table with dead rows after deletes, and live rows, is due a
// vacuum by autovacuum's formula (threshold 50 plus 1 % of the live rows). The writer
// of the deterministic regime runs it on its own counts of rows, so the schedule is a
// function of the stream and not of when a background process happened to look.
func VacuumDue(dead, live int64) bool {
	return float64(dead) > vacuumThreshold+vacuumScaleFactor*float64(max(live, 0))
}

// Phase is the log a phase of a load wrote, counted two ways.
//
// The records are the server's own count of what it wrote for the phase (pg_stat_wal):
// the number of log records, how many were full-page images, and their bytes. They are
// not exactly the same on two runs of one phase: which page a COPY or a vacuum found
// free when a background process held another, the catalog pages the earlier loads left,
// and what ANALYZE draws at random move them a little. The log position is the
// advance of pg_current_wal_insert_lsn over a phase that ran as one span
// (Contiguous): the record bytes, plus the headers and the padding of log pages, so it
// moves by a page header (at most [LogSlack] bytes) with where the span began in its page.
// A phase the stream moves in and out of has no span of its own, so no log position.
//
// A checkpoint that completes inside a phase puts a full-page image of every page the
// phase touches next into the log, so a phase with one is labelled, and is not a count of
// its work alone.
type Phase struct {
	Name       string
	Contiguous bool
	// LogBytes is the advance of the log position; 0 unless Contiguous.
	LogBytes int64
	// Records, FPIs and RecordBytes are pg_stat_wal's counts for the phase.
	Records, FPIs, RecordBytes int64
	CheckpointsInside          int64
	// Spans, if above zero, says the phase is the sum of that many spans, each measured on
	// its own (the batches of a stream): LogBytes is the sum of their advances, exact to
	// the slack of [LogSlack] for each, and the server's own counts are not kept for it.
	Spans int
}

// LogSlack is how far two runs of one contiguous phase can differ in log position
// without the work differing: a page header (24 bytes, 40 at the start of a segment) more
// or less, and the padding of the record at each end.
const LogSlack = 64

// LoadStats is what a load left and cost, counted by the writer and by the server.
type LoadStats struct {
	Arm    pgschema.Arm
	Schema string
	// Records are the records written, LastSeq the last of them, Retentions how many
	// times the horizon moved, and Horizon (Unix nanoseconds, 0 if it never moved) where
	// it ended.
	Records       uint64
	LastSeq       uint64
	Retentions    int
	Horizon       int64
	PastEnd       bool
	StreamVacuums int
	// The rows of each table at the end, counted by the server.
	LogRows, LogBaseRows, EdgeRows, EntityRows, DictionaryRows int64
	Phases                                                     []Phase
	// Indexes are the indexes built after the load, in the order they were built.
	Indexes []string
	// ExtraVacuums is how many more times a table was vacuumed after the registered
	// VACUUM (FREEZE, ANALYZE) because the visibility map did not cover all its pages:
	// a vacuum that cannot get the cleanup lock of a page leaves it not all-visible, and
	// whether it can depends on what the server's background processes hold at that
	// moment, not on the stream.
	ExtraVacuums int
}

// IntervalRowBytes is the memory a load holds for each interval row it folds, in bytes,
// until the rows are written: the row itself (two keys, the relation, the layer and the
// source, the bounds), the key strings it points to, and the unused room an append leaves
// in the slice it grows. A fold of the fixtures holds 66 to 124 bytes a row (the least with
// few rows to a subject, when the keys are shared, the most with many subjects of one
// row); 192 leaves a margin for subjects whose keys are not shared. A test
// (TestAnIntervalRowCostsWhatTheEstimateSays) measures the folds of the fixtures and fails
// if one holds more than this per row, or less than a quarter of it.
const IntervalRowBytes = 192

// IntervalMemory is the estimate of the memory a load needs to hold the interval rows it
// will write: rows times [IntervalRowBytes]. The rows are counted by folding the log, so the
// estimate is a function of the stream and the horizon, not of the machine.
func IntervalMemory(rows int64) int64 { return rows * IntervalRowBytes }

// DefaultMaxIntervalMem is the memory a load may need for its interval rows when
// LoadConfig.MaxIntervalMem is not set: 2 GiB.
const DefaultMaxIntervalMem = 2 << 30

// ErrIntervalMemory is wrapped by the error [Loader.Finish] returns when the interval rows
// the log folds to would need more memory than the load may use. Nothing has been written
// to the interval tables then; the load did not run.
var ErrIntervalMemory = errors.New("pgengine: the interval rows would need more memory than the load may use")

// maxExtraVacuums bounds the vacuums a table is given to become all-visible.
const maxExtraVacuums = 5

// LoadConfig says what a bulk load writes to.
type LoadConfig struct {
	DSN    string
	Schema string
	// Arm is a bulk arm: [pgschema.Bulk], [pgschema.BulkB] or [pgschema.BulkID].
	Arm pgschema.Arm
	// Replace drops a schema that is there. Without it a schema that is there is an
	// error: a load never adds to what another left.
	Replace bool
	// Tracer, if set, is told of every statement and every COPY the load runs.
	Tracer pgx.QueryTracer
	// MaxIntervalMem is the most memory, in bytes, the load may need to hold the interval
	// rows before it writes them (see [IntervalMemory]); zero means [DefaultMaxIntervalMem].
	// Finish refuses a load whose rows would need more, before it writes any of them.
	MaxIntervalMem int64
}

// ErrSchemaExists is wrapped by the error [NewLoader] returns for a schema that is
// already there and was not to be replaced.
var ErrSchemaExists = errors.New("pgengine: the schema already exists")

// A key is what src, dst and fp hold in the arm: the encoded fingerprint, or the id
// of the dictionary of the id arm.
type key struct {
	b  string
	id int64
}

func cmpKey(a, b key) int { return cmp.Or(cmp.Compare(a.b, b.b), cmp.Compare(a.id, b.id)) }

// Loader writes a stream to the tables of a bulk arm the way a bulk load does, and
// then builds what a read needs. The log is written with COPY, in the order of the
// stream; a retention does what the engine's does (baselines in, the log before the
// horizon out); the intervals are folded from the log in one pass, in a client, and
// written with COPY in the order of (source, start), so the heap is in the order the
// reads want it; the indexes are built after the tables are full; and the tables are
// vacuumed and frozen, so that every page is all-visible. It is Postgres's best case
// for a read, and the cost of getting there is counted, not timed: the log written, by
// phase.
//
// A Loader is a sink for the plan driver (runner.Sink): Write and Retain are called in
// stream order, then Finish once. It is not safe for concurrent use.
type Loader struct {
	cfg  LoadConfig
	ctx  context.Context
	conn *pgx.Conn
	ids  bool

	started    bool
	streamFrom mark
	last       uint64
	stats      LoadStats
	// meta, as the retentions left it.
	horizon int64
	pastEnd bool

	dict      map[identity.Fingerprint]int64
	dictOrder []identity.Fingerprint

	// the writer's own counts, for the vacuum schedule: rows deleted since the table was
	// last vacuumed and rows live.
	dead, live map[string]int64

	walAt  map[string]*Phase
	closed bool
}

// NewLoader connects, makes the schema and its tables (and no indexes), and returns the
// loader. It refuses an arm that is not a bulk arm and, unless cfg.Replace is set, a
// schema that exists.
func NewLoader(ctx context.Context, cfg LoadConfig) (*Loader, error) {
	switch cfg.Arm {
	case pgschema.Bulk, pgschema.BulkB, pgschema.BulkID:
	default:
		return nil, fmt.Errorf("pgengine: the arm %q is not loaded in bulk: %w", cfg.Arm, engine.ErrInvalid)
	}
	if !schemaName.MatchString(cfg.Schema) {
		return nil, fmt.Errorf("pgengine: %q is not a usable schema name: %w", cfg.Schema, engine.ErrInvalid)
	}
	if cfg.DSN == "" {
		return nil, errors.New("pgengine: no DSN")
	}
	if cfg.Replace {
		if err := DropSchema(ctx, cfg.DSN, cfg.Schema); err != nil {
			return nil, err
		}
	}
	cc, err := pgx.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("pgengine: DSN: %w", err)
	}
	cc.RuntimeParams["search_path"] = pgx.Identifier{cfg.Schema}.Sanitize() + ", public"
	cc.Tracer = cfg.Tracer
	conn, err := pgx.ConnectConfig(ctx, cc)
	if err != nil {
		return nil, fmt.Errorf("pgengine: %w", err)
	}
	l := &Loader{
		cfg: cfg, ctx: ctx, conn: conn, ids: cfg.Arm == pgschema.BulkID,
		dict: make(map[identity.Fingerprint]int64), dead: make(map[string]int64), live: make(map[string]int64),
		walAt: make(map[string]*Phase),
	}
	l.stats.Arm, l.stats.Schema = cfg.Arm, cfg.Schema
	if err := l.create(ctx); err != nil {
		_ = conn.Close(ctx)
		return nil, err
	}
	return l, nil
}

func (l *Loader) create(ctx context.Context) error {
	tx, err := l.conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("pgengine: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	exists, err := prepareSchema(ctx, tx, l.cfg.Schema)
	if err != nil {
		return err
	}
	if exists {
		return fmt.Errorf("pgengine: schema %s: %w (Replace drops it)", l.cfg.Schema, ErrSchemaExists)
	}
	if err := buildSchema(ctx, tx, l.cfg.Schema, l.cfg.Arm, false); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Close releases the connection. The schema stays.
func (l *Loader) Close() error {
	if l.closed {
		return nil
	}
	l.closed = true
	return l.conn.Close(l.ctx)
}

// A mark is a position in the log, the checkpoints done so far and the server's count of
// the log written.
type mark struct{ lsn, checkpoints, records, fpis, bytes int64 }

// mark reads the position. The server's counts are flushed first (they are kept by the
// backend that wrote the log until it is told to report them).
func (l *Loader) mark() (mark, error) { return readMark(l.ctx, l.conn) }

func readMark(ctx context.Context, conn *pgx.Conn) (mark, error) {
	var m mark
	if _, err := conn.Exec(ctx, "SELECT pg_stat_force_next_flush()"); err != nil {
		return m, fmt.Errorf("pgengine: flushing the server's counts: %w", err)
	}
	err := conn.QueryRow(ctx, `SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(), '0/0')::bigint,
  (SELECT num_timed + num_requested FROM pg_stat_checkpointer)::bigint,
  w.wal_records, w.wal_fpi, w.wal_bytes::bigint
FROM pg_stat_wal w`).Scan(&m.lsn, &m.checkpoints, &m.records, &m.fpis, &m.bytes)
	if err != nil {
		return m, fmt.Errorf("pgengine: reading the log position: %w", err)
	}
	return m, nil
}

func (l *Loader) phase(name string) *Phase {
	p := l.walAt[name]
	if p == nil {
		p = &Phase{Name: name}
		l.walAt[name] = p
	}
	return p
}

// add counts the log written between two marks in a phase.
func (l *Loader) add(name string, contiguous bool, before, after mark) {
	l.phase(name).accumulate(contiguous, before, after)
}

// accumulate adds the log written between two marks to a phase.
func (p *Phase) accumulate(contiguous bool, before, after mark) {
	p.Records += after.records - before.records
	p.FPIs += after.fpis - before.fpis
	p.RecordBytes += after.bytes - before.bytes
	p.CheckpointsInside += after.checkpoints - before.checkpoints
	if contiguous {
		p.Contiguous = true
		p.LogBytes += after.lsn - before.lsn
	}
}

// measure runs f and counts the log it wrote, and the checkpoints that happened while it
// ran, in the phase. A phase measured once is a span of its own: contiguous says so.
func (l *Loader) measure(phase string, contiguous bool, f func() error) error {
	before, err := l.mark()
	if err != nil {
		return err
	}
	if err := f(); err != nil {
		return err
	}
	after, err := l.mark()
	if err != nil {
		return err
	}
	l.add(phase, contiguous, before, after)
	return nil
}

func (l *Loader) checkpoint() error {
	if _, err := l.conn.Exec(l.ctx, "CHECKPOINT"); err != nil {
		return fmt.Errorf("pgengine: checkpoint: %w", err)
	}
	return nil
}

// keyOf is what the table stores for a fingerprint.
func (l *Loader) keyOf(fp identity.Fingerprint) any {
	if !l.ids {
		return pgmodel.EncodeFingerprint(fp)
	}
	id, ok := l.dict[fp]
	if !ok {
		id = int64(len(l.dictOrder)) + 1
		l.dict[fp] = id
		l.dictOrder = append(l.dictOrder, fp)
	}
	return id
}

// logRow is the values of one record in the columns of [pgschema.LogColumns], as COPY
// sends them. The record must be valid.
func (l *Loader) logRow(r engine.Record) []any {
	row := make([]any, len(pgschema.LogColumns))
	row[0] = seqToDB(r.Seq)
	row[1] = pgmodel.ToNanos(r.EventTime)
	row[2] = int16(r.Layer)
	if r.Subject.Kind == engine.SubjectEdge {
		row[3] = byte('g')
		row[4] = l.keyOf(r.Subject.A)
		row[5] = l.keyOf(r.Subject.B)
		row[6] = string(r.Subject.Relation)
	} else {
		row[3] = byte('e')
		row[4] = l.keyOf(r.Subject.A)
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

// Write implements the sink of a stream: it validates the batch whole and writes it to
// the log with COPY. A refused batch writes nothing.
func (l *Loader) Write(batch []engine.Record) error {
	if len(batch) == 0 {
		return nil
	}
	seq := l.last
	for _, r := range batch {
		if err := r.Validate(); err != nil {
			return err
		}
		if l.pastEnd || pgmodel.ToNanos(r.EventTime) < l.horizon {
			return fmt.Errorf("record seq %d at %s: %w", r.Seq, r.EventTime.Format(time.RFC3339), engine.ErrBeforeHorizon)
		}
		if r.Seq <= seq {
			return fmt.Errorf("record seq %d is not above the last written seq %d: %w", r.Seq, seq, engine.ErrInvalid)
		}
		seq = r.Seq
	}
	if !l.started {
		// Whatever the schema's creation left in the log is not the stream's.
		if err := l.checkpoint(); err != nil {
			return err
		}
		m, err := l.mark()
		if err != nil {
			return err
		}
		l.streamFrom, l.started = m, true
	}
	return l.measure(PhaseCopy, false, func() error {
		n, err := l.conn.CopyFrom(l.ctx, pgx.Identifier{"log"}, pgschema.Names(pgschema.LogColumns),
			pgx.CopyFromSlice(len(batch), func(i int) ([]any, error) { return l.logRow(batch[i]), nil }))
		if err != nil {
			return fmt.Errorf("pgengine: copying a batch into the log: %w", err)
		}
		if n != int64(len(batch)) {
			return fmt.Errorf("pgengine: copied %d of %d records into the log", n, len(batch))
		}
		l.last = seq
		l.stats.Records += uint64(len(batch))
		l.live["log"] += n
		return nil
	})
}

// Retain implements the sink of a stream: it moves the horizon as the engine's Retain
// does (baselines in, the log before the horizon out, in one transaction) and, when the
// rows it deleted pass autovacuum's threshold, vacuums the table, as the writer of the
// deterministic regime does.
func (l *Loader) Retain(horizon time.Time) error {
	var rs retainStats
	err := l.measure(PhaseRetain, false, func() error {
		tx, err := l.conn.Begin(l.ctx)
		if err != nil {
			return fmt.Errorf("pgengine: %w", err)
		}
		defer func() { _ = tx.Rollback(l.ctx) }()
		rs, err = retainTx(l.ctx, tx, horizon)
		if err != nil || !rs.changed {
			return err
		}
		if err := tx.Commit(l.ctx); err != nil {
			return fmt.Errorf("pgengine: committing a retention: %w", err)
		}
		return nil
	})
	if err != nil || !rs.changed {
		return err
	}
	l.horizon, l.pastEnd = rs.meta.horizon, rs.meta.pastEnd
	l.stats.Retentions++
	l.dead["log"] += int64(rs.logDeleted)
	l.live["log"] -= int64(rs.logDeleted)
	l.dead["log_base"] += int64(rs.baselinesTrimmed)
	l.live["log_base"] += int64(rs.baselinesInserted) - int64(rs.baselinesTrimmed)
	for _, table := range []string{"log", "log_base"} {
		if !VacuumDue(l.dead[table], l.live[table]) {
			continue
		}
		err := l.measure(PhaseVacuumStream, false, func() error {
			if _, err := l.conn.Exec(l.ctx, "VACUUM "+table); err != nil {
				return fmt.Errorf("pgengine: vacuuming %s: %w", table, err)
			}
			return nil
		})
		if err != nil {
			return err
		}
		l.dead[table] = 0
		l.stats.StreamVacuums++
	}
	return nil
}

// An interval row on its way to its table.
type outRow struct {
	src, dst key
	rel      string
	layer    int16
	source   int16
	start    int64
	end      int64
	open     bool
}

func (o outRow) during() pgtype.Range[pgtype.Int8] {
	r := pgtype.Range[pgtype.Int8]{
		Lower: pgtype.Int8{Int64: o.start, Valid: true}, LowerType: pgtype.Inclusive, Valid: true,
		UpperType: pgtype.Unbounded,
	}
	if !o.open {
		r.Upper, r.UpperType = pgtype.Int8{Int64: o.end, Valid: true}, pgtype.Exclusive
	}
	return r
}

// The log of every subject, baselines included, in groups. Folding the groups into
// intervals is the engine's own refold of a subject whole.
const foldInput = `SELECT subject_kind, src, dst, rel, layer, seq, event_ns, producer, kind::int4, ttl_ns, through_ns FROM log
UNION ALL
SELECT subject_kind, src, dst, rel, layer, seq, event_ns, producer, kind::int4, ttl_ns, through_ns FROM log_base
ORDER BY 1, 2, 3, 4`

// fold reads the log, folds each subject with the planner and returns the interval rows
// of edges and of entities, each in the order they are to be written, and the ids of
// the dictionary that the retained log refers to.
//
// With countOnly it keeps no row and no id, and returns only how many interval rows the
// fold makes: the count the memory a load needs is judged by before any row is held.
func (l *Loader) fold(ctx context.Context, countOnly bool) (edges, entities []outRow, used map[int64]bool, n int64, err error) {
	rows, err := l.conn.Query(ctx, foldInput)
	if err != nil {
		return nil, nil, nil, 0, fmt.Errorf("pgengine: reading the log to fold it: %w", err)
	}
	defer rows.Close()
	used = make(map[int64]bool)
	producers := make(map[string]lifecycle.Producer)

	var (
		cur      subjectKey
		have     bool
		input    []pgmodel.LogRow
		curLayer int16
	)
	flush := func() error {
		if !have {
			return nil
		}
		fresh, _, err := pgmodel.Refold(nil, input, math.MinInt64, l.horizon)
		if err != nil {
			return fmt.Errorf("pgengine: folding a subject: %w", err)
		}
		n += int64(len(fresh))
		if countOnly {
			input = input[:0]
			return nil
		}
		for _, r := range fresh {
			o := outRow{src: cur.src, dst: cur.dst, rel: cur.rel, layer: curLayer, source: int16(r.EndSource), start: r.Start, end: r.End, open: r.Open}
			if cur.edge {
				edges = append(edges, o)
			} else {
				entities = append(entities, o)
			}
		}
		input = input[:0]
		return nil
	}
	for rows.Next() {
		var (
			kind                byte
			src, dst, rel, prod pgtype.DriverBytes
			srcID, dstID        *int64
			layer               int16
			seq, event, ttl     int64
			through             *int64
			kindInt             int32
		)
		var dsts []any
		if l.ids {
			dsts = []any{&kind, &srcID, &dstID, &rel, &layer, &seq, &event, &prod, &kindInt, &ttl, &through}
		} else {
			dsts = []any{&kind, &src, &dst, &rel, &layer, &seq, &event, &prod, &kindInt, &ttl, &through}
		}
		if err := rows.Scan(dsts...); err != nil {
			return nil, nil, nil, 0, fmt.Errorf("pgengine: reading the log to fold it: %w", err)
		}
		sk := subjectKey{edge: kind == 'g', rel: string(rel)}
		if l.ids {
			sk.src = key{id: *srcID}
			if !countOnly {
				used[*srcID] = true
			}
			if dstID != nil {
				sk.dst = key{id: *dstID}
				if !countOnly {
					used[*dstID] = true
				}
			}
		} else {
			sk.src = key{b: string(src)}
			sk.dst = key{b: string(dst)}
		}
		if !have || sk != cur {
			if err := flush(); err != nil {
				return nil, nil, nil, 0, err
			}
			cur, have, curLayer = sk, true, layer
		} else if layer != curLayer {
			return nil, nil, nil, 0, fmt.Errorf("pgengine: a subject is stored in layer %s and in layer %s: %w", catalog.Layer(curLayer), catalog.Layer(layer), engine.ErrInvalid)
		}
		p, ok := producers[string(prod)]
		if !ok {
			p = lifecycle.Producer(prod)
			producers[string(prod)] = p
		}
		row := pgmodel.LogRow{Producer: p, Seq: seqFromDB(seq), Event: event, TTL: time.Duration(ttl), Kind: lifecycle.Kind(kindInt)}
		if through != nil {
			row.Through, row.HasThrough = *through, true
		}
		input = append(input, row)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, nil, 0, fmt.Errorf("pgengine: reading the log to fold it: %w", err)
	}
	if err := flush(); err != nil {
		return nil, nil, nil, 0, err
	}
	// The heap order of the reads: by source, then by when the interval begins.
	slices.SortFunc(edges, func(a, b outRow) int {
		return cmp.Or(cmpKey(a.src, b.src), cmp.Compare(a.start, b.start), cmpKey(a.dst, b.dst), cmp.Compare(a.rel, b.rel))
	})
	slices.SortFunc(entities, func(a, b outRow) int {
		return cmp.Or(cmpKey(a.src, b.src), cmp.Compare(a.start, b.start))
	})
	return edges, entities, used, n, nil
}

// subjectKey identifies a subject in the folded log: its kind and its columns.
type subjectKey struct {
	edge     bool
	src, dst key
	rel      string
}

func (l *Loader) copyIntervals(ctx context.Context, edges, entities []outRow) error {
	n, err := l.conn.CopyFrom(ctx, pgx.Identifier{"edges"}, pgschema.Names(pgschema.EdgeColumns),
		pgx.CopyFromSlice(len(edges), func(i int) ([]any, error) {
			o := edges[i]
			return []any{l.dbKey(o.src), l.dbKey(o.dst), o.rel, o.layer, o.start, o.during(), nil, o.source}, nil
		}))
	if err != nil {
		return fmt.Errorf("pgengine: copying the edge intervals: %w", err)
	}
	if n != int64(len(edges)) {
		return fmt.Errorf("pgengine: copied %d of %d edge intervals", n, len(edges))
	}
	n, err = l.conn.CopyFrom(ctx, pgx.Identifier{"entities"}, pgschema.Names(pgschema.EntityColumns),
		pgx.CopyFromSlice(len(entities), func(i int) ([]any, error) {
			o := entities[i]
			return []any{l.dbKey(o.src), o.layer, o.start, o.during(), nil, o.source}, nil
		}))
	if err != nil {
		return fmt.Errorf("pgengine: copying the entity intervals: %w", err)
	}
	if n != int64(len(entities)) {
		return fmt.Errorf("pgengine: copied %d of %d entity intervals", n, len(entities))
	}
	return nil
}

func (l *Loader) dbKey(k key) any {
	if l.ids {
		return k.id
	}
	return []byte(k.b)
}

// Finish builds what the stream was written for: the intervals, the dictionary of the
// id arm, the indexes, and a vacuum that freezes every page. It returns what the load
// left. Each phase starts from a checkpoint, so the log it writes does not depend on
// the page the one before dirtied.
func (l *Loader) Finish() (*LoadStats, error) {
	ctx := l.ctx
	if !l.started {
		return nil, fmt.Errorf("pgengine: no record was written: %w", engine.ErrInvalid)
	}
	l.stats.LastSeq = l.last
	l.stats.Horizon, l.stats.PastEnd = l.horizon, l.pastEnd

	streamTo, err := l.mark()
	if err != nil {
		return nil, err
	}
	l.add(PhaseStream, true, l.streamFrom, streamTo)

	// The rows are counted before any is held: a load that would need more memory than
	// it may use stops here, with the interval tables empty.
	_, _, _, rows, err := l.fold(ctx, true)
	if err != nil {
		return nil, err
	}
	limit := l.cfg.MaxIntervalMem
	if limit == 0 {
		limit = DefaultMaxIntervalMem
	}
	if need := IntervalMemory(rows); need > limit {
		return nil, fmt.Errorf("%w: %d interval rows need an estimated %d bytes (%d each), and the limit is %d", ErrIntervalMemory, rows, need, IntervalRowBytes, limit)
	}
	if err := l.checkpoint(); err != nil {
		return nil, err
	}
	var used map[int64]bool
	err = l.measure(PhaseFold, true, func() error {
		edges, entities, u, _, err := l.fold(ctx, false)
		if err != nil {
			return err
		}
		used = u
		if err := l.copyIntervals(ctx, edges, entities); err != nil {
			return err
		}
		_, err = l.conn.Exec(ctx, "UPDATE meta SET last_seq = $1", seqToDB(l.last))
		if err != nil {
			return fmt.Errorf("pgengine: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if l.ids {
		if err := l.checkpoint(); err != nil {
			return nil, err
		}
		if err := l.measure(PhaseDictionary, true, func() error { return l.copyDictionary(ctx, used) }); err != nil {
			return nil, err
		}
	}

	if err := l.checkpoint(); err != nil {
		return nil, err
	}
	idx, err := pgschema.Indexes(l.cfg.Arm)
	if err != nil {
		return nil, err
	}
	err = l.measure(PhaseIndexes, true, func() error {
		for _, s := range idx {
			if _, err := l.conn.Exec(ctx, s); err != nil {
				return fmt.Errorf("pgengine: %.60s...: %w", s, err)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	l.stats.Indexes = idx

	if err := l.checkpoint(); err != nil {
		return nil, err
	}
	err = l.measure(PhaseVacuum, true, func() error {
		tables, err := l.tables(ctx)
		if err != nil {
			return err
		}
		for _, t := range tables {
			name := pgx.Identifier{t}.Sanitize()
			if _, err := l.conn.Exec(ctx, "VACUUM (FREEZE, ANALYZE) "+name); err != nil {
				return fmt.Errorf("pgengine: vacuuming %s: %w", t, err)
			}
			// The load is Postgres's best case for a read only if every page is
			// all-visible, so that an index-only scan never visits the heap.
			for pass := 0; ; pass++ {
				var pages, visible int32
				err := l.conn.QueryRow(ctx, "SELECT relpages, relallvisible FROM pg_class WHERE oid = $1::regclass", name).Scan(&pages, &visible)
				if err != nil {
					return fmt.Errorf("pgengine: reading the visibility of %s: %w", t, err)
				}
				if visible >= pages {
					break
				}
				if pass == maxExtraVacuums {
					return fmt.Errorf("pgengine: %s has %d pages and %d are all-visible after %d vacuums", t, pages, visible, pass+1)
				}
				if _, err := l.conn.Exec(ctx, "VACUUM (FREEZE) "+name); err != nil {
					return fmt.Errorf("pgengine: vacuuming %s: %w", t, err)
				}
				l.stats.ExtraVacuums++
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if err := l.checkpoint(); err != nil {
		return nil, err
	}

	for _, c := range []struct {
		table string
		into  *int64
		skip  bool
	}{
		{"log", &l.stats.LogRows, false},
		{"log_base", &l.stats.LogBaseRows, false},
		{"edges", &l.stats.EdgeRows, false},
		{"entities", &l.stats.EntityRows, false},
		{"entity", &l.stats.DictionaryRows, !l.ids},
	} {
		if c.skip {
			continue
		}
		if err := l.conn.QueryRow(ctx, "SELECT count(*) FROM "+c.table).Scan(c.into); err != nil {
			return nil, fmt.Errorf("pgengine: counting %s: %w", c.table, err)
		}
	}
	for _, name := range phaseOrder {
		if p := l.walAt[name]; p != nil {
			l.stats.Phases = append(l.stats.Phases, *p)
		}
	}
	st := l.stats
	return &st, nil
}

// copyDictionary writes the dictionary of the id arm: the fingerprint of every id the
// retained log refers to, in the order of the ids. An id that only a record a retention
// discarded used is not kept, so the dictionary is no larger than what the tables need.
func (l *Loader) copyDictionary(ctx context.Context, used map[int64]bool) error {
	var ids []int64
	for id := range used {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	n, err := l.conn.CopyFrom(ctx, pgx.Identifier{"entity"}, []string{"id", "fp"},
		pgx.CopyFromSlice(len(ids), func(i int) ([]any, error) {
			return []any{ids[i], pgmodel.EncodeFingerprint(l.dictOrder[ids[i]-1])}, nil
		}))
	if err != nil {
		return fmt.Errorf("pgengine: copying the dictionary: %w", err)
	}
	if n != int64(len(ids)) {
		return fmt.Errorf("pgengine: copied %d of %d dictionary rows", n, len(ids))
	}
	return nil
}

// tables lists the tables of the schema by name.
func (l *Loader) tables(ctx context.Context) ([]string, error) {
	rows, err := l.conn.Query(ctx, `SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = $1 AND c.relkind = 'r' ORDER BY c.relname`, l.cfg.Schema)
	if err != nil {
		return nil, fmt.Errorf("pgengine: listing the tables: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}
