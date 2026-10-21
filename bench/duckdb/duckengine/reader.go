package duckengine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"

	"github.com/lotannauo/toposhift/bench/duckdb/duckmodel"
	"github.com/lotannauo/toposhift/bench/duckdb/scripts"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// ErrReadOnly is wrapped by the error [Reader.Write] and [Reader.Retain] return: a file
// that was built takes no append, no late event and no retention without a rewrite.
var ErrReadOnly = errors.New("duckengine: files that were built take no write or retention without a rewrite")

// ErrManifest is wrapped by the error [OpenReader] returns for a build made for another
// DuckDB version, other settings, or without the files the arm reads.
var ErrManifest = errors.New("duckengine: the build is not one this reader can read")

// Trace is what the last read did: the statement, the plan the profiler reports for it
// and what each file it read admits by the footers.
type Trace struct {
	// Statement names the statement's shape; SQL is its text.
	Statement string
	SQL       string
	Profile   Profile
	// Raw is the profile as DuckDB reported it, before everything that is not a count
	// was stripped.
	Raw RawProfile
	// Admissions are one per file the statement scanned, in the order it names them.
	Admissions []Admission
	// Observed is what the statement read of each file, from the file system's log. It
	// is filled only on a connection in observe mode.
	Observed []ObservedFile
}

// scanSpec is what a statement asks of one file: the columns it touches and the constant
// filters it puts on them. It is what the footers are counted against.
type scanSpec struct {
	role    Role
	cols    []string
	filters []Filter
}

// Reader answers the spike's questions from the files of one arm of a build. It
// implements [engine.Engine] for reads; it takes no write. Its statements run one after
// another on the connection it was given.
type Reader struct {
	db      *DB
	dir     string
	arm     Arm
	man     *Manifest
	lastSeq uint64
	mut     sqlMutant

	mu    sync.Mutex
	stmts map[string]*sql.Stmt
	meta  map[Role]*FileMeta
	probe map[probeKey][]bool
	last  Trace
}

var _ engine.Engine = (*Reader)(nil)

type probeKey struct {
	path string
	col  string
	val  string
}

// OpenReader reads the manifest of a build and opens its files as the arm. It refuses a
// build made by another DuckDB version or other settings, one that lacks a file the arm
// reads, and, with hashes, a file that is not the one built.
func OpenReader(ctx context.Context, db *DB, dir string, arm Arm, hashes bool) (*Reader, error) {
	m, err := ReadManifest(dir)
	if err != nil {
		return nil, err
	}
	return openReader(ctx, db, dir, arm, m, hashes, sqlHonest)
}

func openReader(ctx context.Context, db *DB, dir string, arm Arm, m *Manifest, hashes bool, mut sqlMutant) (*Reader, error) {
	if db.cfg.Mode == ModeBuild {
		return nil, errors.New("duckengine: a reader needs a connection that is not a build's")
	}
	switch {
	case m.DuckDB != Version:
		return nil, fmt.Errorf("%w: made by DuckDB %s, this is %s", ErrManifest, m.DuckDB, Version)
	case m.SettingsSHA256 != scripts.SettingsSHA256():
		return nil, fmt.Errorf("%w: made under settings %s, these are %s", ErrManifest, m.SettingsSHA256, scripts.SettingsSHA256())
	case !slices.Contains(AllArms, arm):
		return nil, fmt.Errorf("duckengine: no arm %q", arm)
	}
	if !m.Has(arm) || arm == ArmI && !hasRoles(m, armRoles[ArmA2]) {
		return nil, fmt.Errorf("%w: arm %s was not built (the arms built are %v)", ErrManifest, arm, m.Arms)
	}
	for _, role := range readRoles(arm) {
		if _, ok := m.File(role); !ok {
			return nil, fmt.Errorf("%w: no file for %s", ErrManifest, role)
		}
	}
	if err := VerifyFiles(dir, m, hashes); err != nil {
		return nil, err
	}
	if arm == ArmH {
		if err := checkGlobs(dir, m); err != nil {
			return nil, err
		}
	}
	if err := db.Check(ctx); err != nil {
		return nil, err
	}
	r := &Reader{
		db: db, dir: dir, arm: arm, man: m, lastSeq: m.Stream.LastSeq, mut: mut,
		stmts: make(map[string]*sql.Stmt), meta: make(map[Role]*FileMeta), probe: make(map[probeKey][]bool),
	}
	for _, role := range readRoles(arm) {
		if err := r.view(ctx, role); err != nil {
			return nil, err
		}
	}
	if arm == ArmH {
		if err := r.warm(ctx); err != nil {
			return nil, err
		}
	}
	return r, nil
}

// warm reads every file of the diagnostic arm's two views once. A scan of several files
// that has not yet read their footers is handed an estimate of the rows it will scan,
// where one that has is handed the rows; the profile of the first read of a view would
// then differ from every later one's in that figure, though the read did the same work.
// Every counted read starts from the footers read, so that a read is the same whichever
// it was.
func (r *Reader) warm(ctx context.Context) error {
	for _, view := range []string{"fwd", "rev"} {
		var n int64
		if err := r.db.QueryRowContext(ctx, "SELECT count(*) FROM "+view).Scan(&n); err != nil {
			return fmt.Errorf("duckengine: reading the footers of the %s files: %w", view, err)
		}
	}
	return nil
}

func hasRoles(m *Manifest, roles []Role) bool {
	for _, r := range roles {
		if _, ok := m.File(r); !ok {
			return false
		}
	}
	return true
}

// globOf is the glob of the files of a role of the diagnostic arm.
func globOf(dir string, role Role) string {
	file := "fwd.parquet"
	if role == RoleHRev {
		file = "rev.parquet"
	}
	return filepath.Join(dir, "h", "*", file)
}

// checkGlobs refuses a build of the diagnostic arm that a read through a glob would not
// read as the manifest says: a directory whose own name has a character a glob reads as a
// pattern, and files the glob names that are not the manifest's, or the manifest's that
// the glob does not.
func checkGlobs(dir string, m *Manifest) error {
	if strings.ContainsAny(dir, "*?[]") {
		return fmt.Errorf("%w: the directory %s has a character a glob reads as a pattern", ErrManifest, dir)
	}
	for _, role := range []Role{RoleHFwd, RoleHRev} {
		got, err := filepath.Glob(globOf(dir, role))
		if err != nil {
			return err
		}
		var want []string
		for _, f := range m.FilesOf(role) {
			want = append(want, filepath.Join(dir, filepath.FromSlash(f.Path)))
		}
		slices.Sort(got)
		slices.Sort(want)
		if !slices.Equal(got, want) {
			return fmt.Errorf("%w: the glob %s names %d files, the manifest lists %d", ErrManifest, globOf(dir, role), len(got), len(want))
		}
	}
	return nil
}

// view defines the view a statement names a file by. The archive is both the forward and
// the reverse file of the arm that has only it, and the diagnostic arm's files are one
// view each, read through a glob.
func (r *Reader) view(ctx context.Context, role Role) error {
	name := string(role)
	path := rolePathIn(r.dir, role)
	switch role {
	case RoleHFwd:
		return r.createView(ctx, "fwd", globOf(r.dir, role))
	case RoleHRev:
		return r.createView(ctx, "rev", globOf(r.dir, role))
	}
	if role == RoleArchive {
		for _, n := range []string{"fwd", "rev"} {
			if err := r.createView(ctx, n, path); err != nil {
				return err
			}
		}
		return nil
	}
	return r.createView(ctx, name, path)
}

func (r *Reader) createView(ctx context.Context, name, path string) error {
	if _, err := r.db.ExecContext(ctx, fmt.Sprintf("CREATE OR REPLACE VIEW %s AS SELECT * FROM read_parquet(%s)", name, quote(path))); err != nil {
		return fmt.Errorf("duckengine: %w", err)
	}
	return nil
}

// fileOf is the file a view names, for the arm.
func (r *Reader) fileOf(view string) Role {
	if r.arm == ArmA1 && (view == "fwd" || view == "rev") {
		return RoleArchive
	}
	if r.arm == ArmH {
		return map[string]Role{"fwd": RoleHFwd, "rev": RoleHRev}[view]
	}
	return Role(view)
}

// Arm returns the arm the reader reads.
func (r *Reader) Arm() Arm { return r.arm }

// Manifest returns the manifest of the build.
func (r *Reader) Manifest() *Manifest { return r.man }

// Trace returns what the last read did. It is filled only on a connection that
// profiles.
func (r *Reader) Trace() Trace {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.last
}

// Write implements [engine.Engine]; it always fails with [ErrReadOnly].
func (r *Reader) Write([]engine.Record) error { return ErrReadOnly }

// Retain implements [engine.Engine]; it always fails with [ErrReadOnly].
func (r *Reader) Retain(time.Time) error { return ErrReadOnly }

// LastSeq implements [engine.Engine]: the last Seq of the stream the files were built
// from.
func (r *Reader) LastSeq() uint64 { return r.lastSeq }

// Size implements [engine.Engine]: the bytes of the files the arm reads.
func (r *Reader) Size() (int64, error) {
	var n int64
	for _, role := range readRoles(r.arm) {
		for _, f := range r.man.FilesOf(role) {
			n += f.Bytes
		}
	}
	return n, nil
}

// Close implements [engine.Engine]: it releases the statements. The connection is the
// caller's.
func (r *Reader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var err error
	for _, s := range r.stmts {
		err = errors.Join(err, s.Close())
	}
	r.stmts = make(map[string]*sql.Stmt)
	return err
}

// useIntervals says a read is answered from the interval files: the arm has them and
// the read is as of the newest token, which an interval can answer.
func (r *Reader) useIntervals(sc engine.Scope) bool {
	return r.arm == ArmI && sc.AsOf >= r.lastSeq
}

// run executes a statement and hands each row to scan, then, if the connection
// profiles, records the trace. The caller holds the lock.
func (r *Reader) run(name, text string, args []any, scans []scanSpec, scan func(*sql.Rows) error) error {
	ctx := context.Background()
	st := r.stmts[text]
	if st == nil {
		var err error
		if st, err = r.db.PrepareContext(ctx, text); err != nil {
			return fmt.Errorf("duckengine: preparing %s: %w", name, err)
		}
		r.stmts[text] = st
	}
	observe := r.db.cfg.Mode == ModeObserve
	if observe {
		if err := r.db.truncateLogs(ctx); err != nil {
			return err
		}
	}
	rows, err := st.QueryContext(ctx, args...)
	if err != nil {
		return fmt.Errorf("duckengine: %s: %w", name, err)
	}
	for rows.Next() {
		if err := scan(rows); err != nil {
			rows.Close()
			return err
		}
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return fmt.Errorf("duckengine: %s: %w", name, err)
	}
	var reads []Read
	if observe {
		var err error
		if reads, err = r.db.fsReads(ctx); err != nil {
			return err
		}
	}
	if !r.db.cfg.Profile {
		return nil
	}
	raw, err := r.db.profile()
	if err != nil {
		return err
	}
	prof, err := ParseProfile(raw)
	if err != nil {
		return err
	}
	tr := Trace{Statement: name, SQL: text, Profile: prof, Raw: raw}
	for _, s := range scans {
		meta, err := r.fileMeta(ctx, s.role)
		if err != nil {
			return err
		}
		a, err := meta.Admit(ctx, s.role, s.cols, s.filters, probeFunc(func(ctx context.Context, path, col string, v Value) ([]bool, error) {
			return r.excluded(ctx, path, col, v)
		}))
		if err != nil {
			return err
		}
		tr.Admissions = append(tr.Admissions, a)
		if observe {
			tr.Observed = append(tr.Observed, meta.Observe(s.role, reads))
		}
	}
	r.last = tr
	return nil
}

type probeFunc func(ctx context.Context, path, col string, v Value) ([]bool, error)

func (f probeFunc) Excluded(ctx context.Context, path, col string, v Value) ([]bool, error) {
	return f(ctx, path, col, v)
}

func (r *Reader) fileMeta(ctx context.Context, role Role) (*FileMeta, error) {
	if m, ok := r.meta[role]; ok {
		return m, nil
	}
	var m *FileMeta
	var err error
	if isSegmentRole(role) {
		// The files in the order of their paths, which is the order of their segments.
		var paths []string
		for _, f := range r.man.FilesOf(role) {
			paths = append(paths, filepath.Join(r.dir, filepath.FromSlash(f.Path)))
		}
		slices.Sort(paths)
		m, err = LoadFilesMeta(ctx, r.db, paths)
	} else {
		m, err = LoadFileMeta(ctx, r.db, rolePathIn(r.dir, role))
	}
	if err != nil {
		return nil, err
	}
	r.meta[role] = m
	return m, nil
}

// excluded asks DuckDB which row groups' bloom filters exclude a value. It is a function
// of the file and the value, so the answer is kept.
func (r *Reader) excluded(ctx context.Context, path, col string, v Value) ([]bool, error) {
	k := probeKey{path, col, v.String()}
	if ex, ok := r.probe[k]; ok {
		return ex, nil
	}
	ex, err := DBProber{DB: r.db}.Excluded(ctx, path, col, v)
	if err != nil {
		return nil, err
	}
	r.probe[k] = ex
	return ex, nil
}

// DBProber answers a [Prober] with DuckDB's own parquet_bloom_probe, so that what is
// excluded is what its reader would exclude.
type DBProber struct{ DB *DB }

// Excluded implements [Prober].
func (p DBProber) Excluded(ctx context.Context, path, col string, v Value) ([]bool, error) {
	var arg any
	switch v.kind {
	case kindString:
		arg = v.s
	case kindInt:
		arg = v.i
	default:
		arg = v.u
	}
	rows, err := p.DB.QueryContext(ctx, "SELECT bloom_filter_excludes FROM parquet_bloom_probe(?, ?, ?) ORDER BY row_group_id", path, col, arg)
	if err != nil {
		return nil, fmt.Errorf("duckengine: probing the bloom filters of %s: %w", col, err)
	}
	defer rows.Close()
	var ex []bool
	for rows.Next() {
		var b sql.NullBool
		if err := rows.Scan(&b); err != nil {
			return nil, err
		}
		ex = append(ex, b.Valid && b.Bool)
	}
	return ex, rows.Err()
}

// profile reads the profile of the connection's last statement.
func (d *DB) profile() (RawProfile, error) {
	info, err := duckdb.GetProfilingInfo(d.conn)
	if err != nil {
		return RawProfile{}, fmt.Errorf("duckengine: reading the profile: %w", err)
	}
	return rawOf(info), nil
}

func rawOf(i duckdb.ProfilingInfo) RawProfile {
	r := RawProfile{Metrics: i.Metrics}
	for _, c := range i.Children {
		r.Children = append(r.Children, rawOf(c))
	}
	return r
}

// The columns each shape of statement touches.
var (
	neighborCols = []string{"layer", "subject_kind", "kind", "ttl_ns", "through_ns", "event_time_ns", "seq", "producer", "relation"}
	aliveCols    = []string{"layer", "subject_kind", "kind", "ttl_ns", "through_ns", "event_time_ns", "seq", "producer", "source"}
	windowCols   = []string{"layer", "subject_kind", "source", "target", "relation", "producer", "event_time_ns", "seq", "kind", "ttl_ns", "through_ns", "payload"}
	ivCols       = []string{"layer", "a", "start_ns", "end_ns"}
)

// recordFilters are the constant filters of a read of an entity's records in a layer.
func recordFilters(layer catalog.Layer, kind, key string, fp identity.Fingerprint, extra ...Filter) []Filter {
	fs := []Filter{
		{Col: "layer", Op: OpEq, Val: Str(layer.String())},
		{Col: "subject_kind", Op: OpEq, Val: Str(kind)},
		{Col: key, Op: OpEq, Val: Str(duckmodel.Text(fp))},
	}
	return append(fs, extra...)
}

func intervalFilters(layer catalog.Layer, fp identity.Fingerprint, t int64) []Filter {
	return []Filter{
		{Col: "layer", Op: OpEq, Val: Str(layer.String())},
		{Col: "a", Op: OpEq, Val: Str(duckmodel.Text(fp))},
		{Col: "start_ns", Op: OpLe, Val: Int(t)},
		{Col: "end_ns", Op: OpGt, Val: Int(t), OrNull: true},
	}
}

// token is the snapshot token as a statement is given it. database/sql passes no
// integer above the largest int64, and no Seq in a file is above it (a build refuses a
// stream that has one), so every token at or above it reads the same rows as it.
func token(s uint64) int64 {
	if s > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(s)
}

func asOf(s uint64) Filter { return Filter{Col: "seq", Op: OpLe, Val: Uint(uint64(token(s)))} }
func upTo(t int64) Filter  { return Filter{Col: "event_time_ns", Op: OpLe, Val: Int(t)} }

// Neighbors implements [engine.Engine].
func (r *Reader) Neighbors(fp identity.Fingerprint, dir engine.Direction, t time.Time, sc engine.Scope) ([]engine.Neighbor, error) {
	if err := sc.Validate(); err != nil {
		return nil, err
	}
	ns, ok := duckmodel.ClampInstant(t)
	if !ok {
		return nil, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	var rows []duckmodel.PeerText
	collect := func(s *sql.Rows) error {
		var p duckmodel.PeerText
		if err := s.Scan(&p.Peer, &p.Relation); err != nil {
			return err
		}
		rows = append(rows, p)
		return nil
	}
	var err error
	if r.useIntervals(sc) {
		view := ivView(dir)
		err = r.run("neighbors."+dirName(dir)+".intervals", neighborsIntervals(dir, r.mut),
			[]any{sc.Layer.String(), duckmodel.Text(fp), ns},
			[]scanSpec{{role: Role(view), cols: append([]string{"peer", "relation"}, ivCols...), filters: intervalFilters(sc.Layer, fp, ns)}}, collect)
	} else {
		view, _, peer := ends(dir, r.mut)
		_, honestKey, _ := ends(dir, sqlHonest)
		err = r.run("neighbors."+dirName(dir)+".records", neighborsRecords(dir, r.mut),
			[]any{sc.Layer.String(), duckmodel.Text(fp), ns, token(sc.AsOf)},
			[]scanSpec{{role: r.fileOf(view), cols: append([]string{peer}, neighborCols...), filters: recordFilters(sc.Layer, "edge", honestKey, fp, upTo(ns), asOf(sc.AsOf))}}, collect)
	}
	if err != nil {
		return nil, err
	}
	out, err := duckmodel.NeighborsFromText(rows)
	if err != nil || len(out) == 0 {
		return nil, err
	}
	return out, nil
}

func dirName(d engine.Direction) string {
	if d == engine.Reverse {
		return "rev"
	}
	return "fwd"
}

// NeighborsBatch implements [engine.Engine]: one statement over the distinct entities,
// whose rows are put back with the entity they are of, so that a list is empty for an
// entity with no neighbor and the answer follows the order and the repeats of fps.
func (r *Reader) NeighborsBatch(fps []identity.Fingerprint, dir engine.Direction, t time.Time, sc engine.Scope) ([][]engine.Neighbor, error) {
	if err := sc.Validate(); err != nil {
		return nil, err
	}
	out := make([][]engine.Neighbor, len(fps))
	ns, ok := duckmodel.ClampInstant(t)
	if !ok || len(fps) == 0 {
		return out, nil
	}
	var distinct []identity.Fingerprint
	seen := make(map[identity.Fingerprint]bool, len(fps))
	for _, fp := range fps {
		if !seen[fp] {
			seen[fp] = true
			distinct = append(distinct, fp)
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	byEntity := make(map[string][]duckmodel.PeerText, len(distinct))
	var texts []any
	var vals []Value
	for _, fp := range distinct {
		texts = append(texts, duckmodel.Text(fp))
		vals = append(vals, Str(duckmodel.Text(fp)))
	}
	collect := func(s *sql.Rows) error {
		var key string
		var p duckmodel.PeerText
		if err := s.Scan(&key, &p.Peer, &p.Relation); err != nil {
			return err
		}
		byEntity[key] = append(byEntity[key], p)
		return nil
	}
	var err error
	if r.useIntervals(sc) {
		view := ivView(dir)
		err = r.run(fmt.Sprintf("batch.%s.intervals", dirName(dir)), batchIntervals(dir, len(distinct), r.mut),
			append([]any{sc.Layer.String(), ns}, texts...),
			[]scanSpec{{role: Role(view), cols: append([]string{"peer", "relation"}, ivCols...), filters: batchIntervalFilters(sc.Layer, vals, ns)}}, collect)
	} else {
		view, _, peer := ends(dir, r.mut)
		_, honestKey, _ := ends(dir, sqlHonest)
		err = r.run(fmt.Sprintf("batch.%s.records", dirName(dir)), batchRecords(dir, len(distinct), r.mut),
			append([]any{sc.Layer.String(), ns, token(sc.AsOf)}, texts...),
			[]scanSpec{{role: r.fileOf(view), cols: append([]string{peer}, neighborCols...), filters: []Filter{
				{Col: "layer", Op: OpEq, Val: Str(sc.Layer.String())},
				{Col: "subject_kind", Op: OpEq, Val: Str("edge")},
				{Col: honestKey, Op: OpIn, In: vals},
				upTo(ns), asOf(sc.AsOf),
			}}}, collect)
	}
	if err != nil {
		return nil, err
	}
	for i, fp := range fps {
		list, err := duckmodel.NeighborsFromText(byEntity[duckmodel.Text(fp)])
		if err != nil {
			return nil, err
		}
		if len(list) > 0 {
			out[i] = list
		}
	}
	return out, nil
}

func batchIntervalFilters(layer catalog.Layer, vals []Value, t int64) []Filter {
	return []Filter{
		{Col: "layer", Op: OpEq, Val: Str(layer.String())},
		{Col: "a", Op: OpIn, In: vals},
		{Col: "start_ns", Op: OpLe, Val: Int(t)},
		{Col: "end_ns", Op: OpGt, Val: Int(t), OrNull: true},
	}
}

// Alive implements [engine.Engine].
func (r *Reader) Alive(fp identity.Fingerprint, t time.Time, sc engine.Scope) (bool, error) {
	if err := sc.Validate(); err != nil {
		return false, err
	}
	ns, ok := duckmodel.ClampInstant(t)
	if !ok {
		return false, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	var n int64
	count := func(s *sql.Rows) error { return s.Scan(&n) }
	var err error
	if r.useIntervals(sc) {
		err = r.run("alive.intervals", aliveIntervals(r.mut),
			[]any{sc.Layer.String(), duckmodel.Text(fp), ns},
			[]scanSpec{{role: RoleIEnt, cols: ivCols, filters: intervalFilters(sc.Layer, fp, ns)}}, count)
	} else {
		err = r.run("alive.records", aliveRecords(r.mut),
			[]any{sc.Layer.String(), duckmodel.Text(fp), ns, token(sc.AsOf)},
			[]scanSpec{{role: r.fileOf("fwd"), cols: aliveCols, filters: recordFilters(sc.Layer, "entity", "source", fp, upTo(ns), asOf(sc.AsOf))}}, count)
	}
	return n > 0, err
}

// Window implements [engine.Engine]. It reads the record files in every arm: an interval
// has lost the records it was made from.
func (r *Reader) Window(fp identity.Fingerprint, dir engine.Direction, from, to time.Time, sc engine.Scope) ([]engine.Record, error) {
	if err := sc.Validate(); err != nil {
		return nil, err
	}
	b := duckmodel.ClampWindow(from, to)
	if b.Empty {
		return nil, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	view, _, _ := ends(dir, r.mut)
	_, honestKey, _ := ends(dir, sqlHonest)
	filters := recordFilters(sc.Layer, "edge", honestKey, fp, Filter{Col: "event_time_ns", Op: OpGe, Val: Int(b.Lo)})
	args := []any{sc.Layer.String(), duckmodel.Text(fp), b.Lo}
	if !b.Unbounded {
		filters = append(filters, Filter{Col: "event_time_ns", Op: OpLt, Val: Int(b.Hi)})
		args = append(args, b.Hi)
	}
	filters = append(filters, asOf(sc.AsOf))
	args = append(args, token(sc.AsOf))
	var out []engine.Record
	err := r.run("window."+dirName(dir)+".records", windowRecords(dir, b.Unbounded, r.mut), args,
		[]scanSpec{{role: r.fileOf(view), cols: windowCols, filters: filters}},
		func(s *sql.Rows) error {
			rec, err := scanRecord(s)
			if err != nil {
				return err
			}
			out = append(out, rec)
			return nil
		})
	if err != nil || len(out) == 0 {
		return nil, err
	}
	return out, nil
}

// scanRecord reads a row of [recordColumns] as an engine record.
func scanRecord(s *sql.Rows) (engine.Record, error) {
	var (
		layerS, kindS, source, producer, recKind string
		target, relation                         sql.NullString
		event, ttl                               int64
		seq                                      uint64
		through                                  sql.NullInt64
		payload                                  []byte
	)
	if err := s.Scan(&layerS, &kindS, &source, &target, &relation, &producer, &event, &seq, &recKind, &ttl, &through, &payload); err != nil {
		return engine.Record{}, err
	}
	layer, subj, err := parseSubject(subjectKey{layerS, kindS, source, target.String, relation.String})
	if err != nil {
		return engine.Record{}, err
	}
	k, err := parseKind(recKind)
	if err != nil {
		return engine.Record{}, err
	}
	rec := engine.Record{
		Layer: layer, Subject: subj, Producer: lifecycle.Producer(producer), EventTime: duckmodel.FromNanos(event),
		Seq: seq, Kind: k, TTL: time.Duration(ttl),
	}
	if through.Valid {
		rec.Through = duckmodel.FromNanos(through.Int64)
	}
	if len(payload) > 0 {
		rec.Payload = slices.Clone(payload)
	}
	return rec, nil
}
