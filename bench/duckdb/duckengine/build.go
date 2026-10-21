package duckengine

import (
	"bufio"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"

	"github.com/lotannauo/toposhift/bench/duckdb/duckmodel"
	"github.com/lotannauo/toposhift/bench/duckdb/scripts"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/replay/activity"
)

// The order and the options of every file a build writes. The archive's are those of
// the statement that made the published archive, which a test holds to the file
// bench/cmd/activitybytes/testdata/archive.sql; the others are the baseline's own.
const (
	// ArchiveOrder is the order of the archive: layer, kind, then the source end.
	ArchiveOrder = "layer, subject_kind, source, target, relation, producer, event_time_ns, seq"
	// ReverseOrder is the same rows with the target end first.
	ReverseOrder = "layer, subject_kind, target, source, relation, producer, event_time_ns, seq"

	// ArchiveRowGroup is the archive's row group size, in rows; RecordRowGroup is the
	// record files', and IntervalRowGroup the interval files'.
	ArchiveRowGroup  = 1048576
	RecordRowGroup   = 65536
	IntervalRowGroup = 65536
)

// Source is a stream a build can read twice.
type Source struct {
	// Label names the input the stream was made from; it is recorded in the manifest.
	Label string
	// Horizon is the retention horizon the stream ends with, the zero time if it never
	// moves one. The files keep what a retention at that horizon, applied once, keeps.
	Horizon time.Time
	// Replay writes the whole stream to the sink, batches in ascending Seq and a call
	// of Retain each time the horizon moves, and returns the stream's digest. It is
	// called twice when the stream has a horizon, once otherwise, and must produce the
	// same stream each time.
	Replay func(ctx context.Context, sink runner.Sink) (digest string, err error)
}

// BuildOptions tune a build.
type BuildOptions struct {
	// VerifyModel feeds the records the files keep to [duckmodel.Model] as well, and
	// checks the interval files row for row against the rows the model derives from
	// them. It holds every record's metadata in memory, which a plan of tens of
	// millions of records may not afford.
	VerifyModel bool
	// RowGroups override the row group sizes of the files, in rows, for a test that wants
	// several row groups from a small stream. DuckDB rounds a size to a multiple of
	// 2,048. A zero keeps the baseline's size.
	RowGroups RowGroupRows
}

// RowGroupRows are the row group sizes of the three kinds of file.
type RowGroupRows struct{ Archive, Record, Interval int }

func (o BuildOptions) sizes() RowGroupRows {
	s := o.RowGroups
	if s.Archive == 0 {
		s.Archive = ArchiveRowGroup
	}
	if s.Record == 0 {
		s.Record = RecordRowGroup
	}
	if s.Interval == 0 {
		s.Interval = IntervalRowGroup
	}
	return s
}

// ErrStreamDiffers is wrapped by the error Build returns when the two passes over a
// stream are not one stream, or the horizon it ends with is not the one it was told.
var ErrStreamDiffers = errors.New("duckengine: the stream is not the same on both passes")

// passSink is what the first pass of a stream does: show every record to the filter.
type passSink struct {
	filter     *duckmodel.Filter
	seen       uint64
	lastSeq    uint64
	horizon    time.Time
	retentions int
}

func (p *passSink) Write(batch []engine.Record) error {
	for _, r := range batch {
		p.filter.Add(r)
		p.seen++
		p.lastSeq = r.Seq
	}
	return nil
}

func (p *passSink) Retain(h time.Time) error {
	p.horizon = h
	p.retentions++
	return nil
}

// keepSink is the second pass: the records the filter keeps are written to the activity
// file, and, when asked, to a model of the interval arm.
type keepSink struct {
	passSink
	w     *activity.Writer
	model *duckmodel.Model
	kept  uint64
}

func (k *keepSink) Write(batch []engine.Record) error {
	var forModel []engine.Record
	for _, r := range batch {
		k.seen++
		k.lastSeq = r.Seq
		if !k.filter.Keep(r) {
			continue
		}
		sr, err := duckmodel.StoreRecord(r)
		if err != nil {
			return err
		}
		if err := k.w.Write(sr); err != nil {
			return err
		}
		k.kept++
		if k.model != nil {
			r.Payload = nil // a fold reads no payload, and a model of a plan is large
			forModel = append(forModel, r)
		}
	}
	if len(forModel) > 0 {
		return k.model.Write(forModel)
	}
	return nil
}

// Build writes the files of the arms to dir, a directory that is empty or does not
// exist, from the stream: the activity file with the retention applied, then each arm
// from it with DuckDB, and the manifest. db must be a connection in [ModeBuild]. The
// interval files are folded in Go with the lifecycle specification from the record
// files, and with opts.VerifyModel compared with what the model derives.
func Build(ctx context.Context, db *DB, dir string, src Source, arms []Arm, opts BuildOptions) (*Manifest, error) {
	if db.cfg.Mode != ModeBuild {
		return nil, fmt.Errorf("duckengine: a build needs a connection in %s mode, not %s", ModeBuild, db.cfg.Mode)
	}
	if err := db.Check(ctx); err != nil {
		return nil, err
	}
	arms = normalizeArms(arms)
	if len(arms) == 0 {
		return nil, errors.New("duckengine: no arm to build")
	}
	if src.Replay == nil {
		return nil, errors.New("duckengine: a build needs a stream to replay")
	}
	if err := emptyDir(dir); err != nil {
		return nil, err
	}

	filter := duckmodel.NewFilter(src.Horizon)
	m := &Manifest{Format: ManifestFormat, DuckDB: Version, SettingsSHA256: scripts.SettingsSHA256(), Arms: arms}
	m.Stream.Label = src.Label
	m.Stream.HorizonNanos = horizonNanos(src.Horizon)

	var digest string
	var first *passSink
	if !src.Horizon.IsZero() {
		first = &passSink{filter: filter}
		d, err := src.Replay(ctx, first)
		if err != nil {
			return nil, err
		}
		digest = d
		if !first.horizon.Equal(src.Horizon) {
			return nil, fmt.Errorf("%w: the stream ends at horizon %s, not the %s it was told", ErrStreamDiffers, first.horizon.Format(time.RFC3339Nano), src.Horizon.Format(time.RFC3339Nano))
		}
	}

	actPath := rolePathIn(dir, RoleActivity)
	f, err := os.Create(actPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	bw := bufio.NewWriterSize(f, 1<<20)
	w, err := activity.NewWriter(bw, activity.WriterOptions{})
	if err != nil {
		return nil, err
	}
	second := &keepSink{passSink: passSink{filter: filter}, w: w}
	if opts.VerifyModel && slices.Contains(arms, ArmI) {
		second.model = duckmodel.New(duckmodel.ArmIntervals)
	}
	d, err := src.Replay(ctx, second)
	if err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	if err := bw.Flush(); err != nil {
		return nil, err
	}
	if err := f.Close(); err != nil {
		return nil, err
	}
	if first != nil {
		if d != digest || first.seen != second.seen || first.lastSeq != second.lastSeq || first.retentions != second.retentions {
			return nil, fmt.Errorf("%w: digest %s and %d records the first time, %s and %d the second", ErrStreamDiffers, digest, first.seen, d, second.seen)
		}
	}
	if !second.horizon.Equal(src.Horizon) {
		return nil, fmt.Errorf("%w: the stream ends at horizon %s, not the %s it was told", ErrStreamDiffers, second.horizon.Format(time.RFC3339Nano), src.Horizon.Format(time.RFC3339Nano))
	}
	if second.lastSeq > math.MaxInt64 {
		return nil, fmt.Errorf("duckengine: the stream's last Seq, %d, is above the largest int64, which a statement cannot be given as a token", second.lastSeq)
	}
	m.Stream.Digest = d
	m.Stream.Records, m.Stream.Kept = second.seen, second.kept
	m.Stream.LastSeq, m.Stream.Retentions = second.lastSeq, second.retentions
	if second.kept == 0 {
		return nil, errors.New("duckengine: the stream kept no record")
	}

	needA2 := slices.Contains(arms, ArmA2) || slices.Contains(arms, ArmI)
	if slices.Contains(arms, ArmA1) {
		if err := copyRecords(ctx, db, actPath, rolePathIn(dir, RoleArchive), ArchiveOrder, opts.sizes().Archive); err != nil {
			return nil, err
		}
	}
	if needA2 {
		if err := copyRecords(ctx, db, actPath, rolePathIn(dir, RoleFwd), ArchiveOrder, opts.sizes().Record); err != nil {
			return nil, err
		}
		if err := copyRecords(ctx, db, actPath, rolePathIn(dir, RoleRev), ReverseOrder, opts.sizes().Record); err != nil {
			return nil, err
		}
	}
	if slices.Contains(arms, ArmI) {
		if err := buildIntervals(ctx, db, dir, second.model, opts.sizes().Interval); err != nil {
			return nil, err
		}
	}

	roles := []Role{RoleActivity}
	for _, a := range Arms {
		if slices.Contains(arms, a) {
			roles = append(roles, armRoles[a]...)
		}
	}
	if needA2 && !slices.Contains(arms, ArmA2) {
		roles = append(roles, armRoles[ArmA2]...) // the interval arm's record files
	}
	for _, r := range uniqueRoles(roles) {
		fi, err := describe(ctx, db, dir, r)
		if err != nil {
			return nil, err
		}
		m.Files = append(m.Files, fi)
	}
	if err := writeManifest(dir, m); err != nil {
		return nil, err
	}
	return m, nil
}

func normalizeArms(arms []Arm) []Arm {
	var out []Arm
	for _, a := range Arms {
		if slices.Contains(arms, a) {
			out = append(out, a)
		}
	}
	return out
}

func uniqueRoles(rs []Role) []Role {
	order := []Role{RoleActivity, RoleArchive, RoleFwd, RoleRev, RoleIFwd, RoleIRev, RoleIEnt}
	var out []Role
	for _, r := range order {
		if slices.Contains(rs, r) {
			out = append(out, r)
		}
	}
	return out
}

func horizonNanos(h time.Time) int64 {
	if h.IsZero() {
		return 0
	}
	return h.UnixNano()
}

// emptyDir makes dir if needed and refuses one that holds anything: a build never
// writes into what another left.
func emptyDir(dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	ents, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	if len(ents) > 0 {
		return fmt.Errorf("duckengine: %s is not empty", dir)
	}
	for _, sub := range []string{"a1", "a2", "i"} {
		if err := os.Mkdir(filepath.Join(dir, sub), 0o755); err != nil {
			return err
		}
	}
	return nil
}

// copyRecords rewrites the activity file into a sorted Parquet file.
func copyRecords(ctx context.Context, db *DB, src, dst, order string, rowGroup int) error {
	stmt := fmt.Sprintf("COPY (SELECT * FROM read_parquet(%s) ORDER BY %s) TO %s (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE %d)",
		quote(src), order, quote(dst), rowGroup)
	if _, err := db.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("duckengine: writing %s: %w", filepath.Base(dst), err)
	}
	return nil
}

// describe measures a file of the build.
func describe(ctx context.Context, db *DB, dir string, r Role) (FileInfo, error) {
	p := rolePathIn(dir, r)
	st, err := os.Stat(p)
	if err != nil {
		return FileInfo{}, err
	}
	sum, err := fileSHA256(p)
	if err != nil {
		return FileInfo{}, err
	}
	fi := FileInfo{Role: r, Path: rolePath[r], Bytes: st.Size(), SHA256: sum}
	err = db.QueryRowContext(ctx, "SELECT num_rows, num_row_groups FROM parquet_file_metadata("+quote(p)+")").Scan(&fi.Rows, &fi.RowGroups)
	if err != nil {
		return FileInfo{}, fmt.Errorf("duckengine: reading the footer of %s: %w", fi.Path, err)
	}
	return fi, nil
}

// Intervals are folded from the forward record file, which holds each subject's
// records together, in the order of a file sorted by producer, event time and Seq.

// The staging tables are in memory and hold each interval's end source, which the files
// do not: it is there to be checked against the model, not to be read.
const (
	stageEdges    = `CREATE OR REPLACE TABLE iv_edge (layer VARCHAR, source VARCHAR, target VARCHAR, relation VARCHAR, start_ns BIGINT, end_ns BIGINT, end_source VARCHAR)`
	stageEntities = `CREATE OR REPLACE TABLE iv_ent (layer VARCHAR, a VARCHAR, start_ns BIGINT, end_ns BIGINT, end_source VARCHAR)`
)

// foldHook, if set, changes each subject's intervals after the fold: a test sets it to
// see that a build whose fold is wrong does not pass its check against the model.
var foldHook func([]duckmodel.Interval) []duckmodel.Interval

// subjectKey is what makes records one subject's.
type subjectKey struct {
	layer, kind, source, target, relation string
}

func buildIntervals(ctx context.Context, db *DB, dir string, model *duckmodel.Model, rowGroup int) error {
	for _, s := range []string{stageEdges, stageEntities} {
		if _, err := db.ExecContext(ctx, s); err != nil {
			return fmt.Errorf("duckengine: %w", err)
		}
	}
	// The appender writes on a connection of its own, so the scan of the record file
	// can go on on the one that reads.
	dc, err := db.connector.Connect(ctx)
	if err != nil {
		return fmt.Errorf("duckengine: %w", err)
	}
	edges, err := duckdb.NewAppenderFromConn(dc, "", "iv_edge")
	if err != nil {
		_ = dc.Close()
		return fmt.Errorf("duckengine: %w", err)
	}
	ents, err := duckdb.NewAppenderFromConn(dc, "", "iv_ent")
	if err != nil {
		_ = edges.Close()
		_ = dc.Close()
		return fmt.Errorf("duckengine: %w", err)
	}
	ferr := foldFile(ctx, db, rolePathIn(dir, RoleFwd), edges, ents)
	cerr := errors.Join(edges.Close(), ents.Close(), dc.Close())
	if err := errors.Join(ferr, cerr); err != nil {
		return err
	}

	const copyOpts = "(FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE %d)"
	copies := []struct {
		role Role
		sel  string
	}{
		{RoleIFwd, "SELECT layer, source AS a, target AS peer, relation, start_ns, end_ns FROM iv_edge ORDER BY layer, a, start_ns, peer, relation"},
		{RoleIRev, "SELECT layer, target AS a, source AS peer, relation, start_ns, end_ns FROM iv_edge ORDER BY layer, a, start_ns, peer, relation"},
		{RoleIEnt, "SELECT layer, a, start_ns, end_ns FROM iv_ent ORDER BY layer, a, start_ns"},
	}
	for _, c := range copies {
		stmt := fmt.Sprintf("COPY (%s) TO %s "+copyOpts, c.sel, quote(rolePathIn(dir, c.role)), rowGroup)
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("duckengine: writing %s: %w", rolePath[c.role], err)
		}
	}
	if model != nil {
		if err := verifyIntervals(ctx, db, dir, model); err != nil {
			return err
		}
	}
	for _, s := range []string{"DROP TABLE iv_edge", "DROP TABLE iv_ent"} {
		if _, err := db.ExecContext(ctx, s); err != nil {
			return fmt.Errorf("duckengine: %w", err)
		}
	}
	return nil
}

// foldFile reads the record file in its order, folds each subject with the lifecycle
// specification and appends its intervals.
func foldFile(ctx context.Context, db *DB, file string, edges, ents *duckdb.Appender) error {
	rows, err := db.QueryContext(ctx, `SELECT layer, subject_kind, source, target, relation, producer, event_time_ns, seq, kind, ttl_ns, through_ns
		FROM read_parquet(`+quote(file)+`)`)
	if err != nil {
		return fmt.Errorf("duckengine: %w", err)
	}
	defer rows.Close()

	var (
		cur    subjectKey
		have   bool
		recs   []engine.Record
		layer  catalog.Layer
		subj   engine.Subject
		prev   subjectKey
		prevOK bool
	)
	flush := func() error {
		if !have {
			return nil
		}
		ivs, err := duckmodel.SubjectIntervals(recs)
		if err != nil {
			return fmt.Errorf("duckengine: folding %s %s: %w", cur.kind, cur.source, err)
		}
		if foldHook != nil {
			ivs = foldHook(ivs)
		}
		for _, iv := range ivs {
			var end, endSource driver.Value
			if !iv.Open {
				end, endSource = iv.End, iv.EndSource.String()
			}
			var err error
			if subj.Kind == engine.SubjectEdge {
				err = edges.AppendRow(layer.String(), duckmodel.Text(subj.A), duckmodel.Text(subj.B), string(subj.Relation), iv.Start, end, endSource)
			} else {
				err = ents.AppendRow(layer.String(), duckmodel.Text(subj.A), iv.Start, end, endSource)
			}
			if err != nil {
				return fmt.Errorf("duckengine: %w", err)
			}
		}
		recs = recs[:0]
		return nil
	}
	for rows.Next() {
		var (
			layerS, kindS, source, producer, recKind string
			target, relation                         sql.NullString
			event, ttl                               int64
			through                                  sql.NullInt64
			seq                                      uint64
		)
		if err := rows.Scan(&layerS, &kindS, &source, &target, &relation, &producer, &event, &seq, &recKind, &ttl, &through); err != nil {
			return fmt.Errorf("duckengine: %w", err)
		}
		key := subjectKey{layerS, kindS, source, target.String, relation.String}
		if key != cur || !have {
			if err := flush(); err != nil {
				return err
			}
			if prevOK && !keyLess(prev, key) {
				return fmt.Errorf("duckengine: the record file is not in the order of its subjects (%v after %v)", key, prev)
			}
			prev, prevOK = key, true
			cur, have = key, true
			var err error
			if layer, subj, err = parseSubject(key); err != nil {
				return err
			}
		}
		r := engine.Record{Layer: layer, Subject: subj, Producer: lifecycle.Producer(producer), EventTime: duckmodel.FromNanos(event), Seq: seq, TTL: time.Duration(ttl)}
		if r.Kind, err = parseKind(recKind); err != nil {
			return err
		}
		if through.Valid {
			r.Through = duckmodel.FromNanos(through.Int64)
		}
		recs = append(recs, r)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("duckengine: %w", err)
	}
	return flush()
}

// keyLess reports whether a subject is before another in the order of the forward
// file: layer, kind, source, target, relation, with a null target and relation first
// among their own (an entity has neither, and is the only one in its place).
func keyLess(a, b subjectKey) bool {
	if a.layer != b.layer {
		return a.layer < b.layer
	}
	if a.kind != b.kind {
		return a.kind < b.kind
	}
	if a.source != b.source {
		return a.source < b.source
	}
	if a.target != b.target {
		return a.target < b.target
	}
	return a.relation < b.relation
}

func parseSubject(k subjectKey) (catalog.Layer, engine.Subject, error) {
	layer, err := parseLayer(k.layer)
	if err != nil {
		return 0, engine.Subject{}, err
	}
	a, err := duckmodel.ParseText(k.source)
	if err != nil {
		return 0, engine.Subject{}, fmt.Errorf("duckengine: source: %w", err)
	}
	switch k.kind {
	case "entity":
		return layer, engine.EntitySubject(a), nil
	case "edge":
		b, err := duckmodel.ParseText(k.target)
		if err != nil {
			return 0, engine.Subject{}, fmt.Errorf("duckengine: target: %w", err)
		}
		return layer, engine.EdgeSubject(a, b, catalog.RelationType(k.relation)), nil
	}
	return 0, engine.Subject{}, fmt.Errorf("duckengine: subject kind %q", k.kind)
}

func parseLayer(s string) (catalog.Layer, error) {
	for _, l := range []catalog.Layer{catalog.L0, catalog.L1, catalog.L2, catalog.L3} {
		if l.String() == s {
			return l, nil
		}
	}
	return 0, fmt.Errorf("duckengine: layer %q", s)
}

func parseKind(s string) (lifecycle.Kind, error) {
	switch s {
	case "observe":
		return lifecycle.Observe, nil
	case "delete":
		return lifecycle.Delete, nil
	}
	return 0, fmt.Errorf("duckengine: record kind %q", s)
}

func parseEndSource(s string) (lifecycle.EndSource, error) {
	for e := lifecycle.EndUnknown; e <= lifecycle.EndOperator; e++ {
		if e.String() == s {
			return e, nil
		}
	}
	return 0, fmt.Errorf("duckengine: end source %q", s)
}
