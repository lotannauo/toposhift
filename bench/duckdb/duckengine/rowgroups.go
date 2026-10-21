package duckengine

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// A Parquet file keeps, for each column of each row group, the smallest and largest
// value in it, how many are null, and how many bytes it takes. A scan with a constant
// filter on a column skips the row groups whose range cannot hold a match, and, for an
// equality, those whose bloom filter says the value is not there. This file counts what
// a read admits from the footer and the bloom filters, as a pure function of the file and the filters:
// the same on every run and every machine, which a profiler's figures for a scan are
// not (see the package documentation).

// colKind is how the footer's text for a column's bounds is read and compared.
type colKind uint8

const (
	kindString colKind = iota + 1
	kindInt
	kindUint
)

// columnKinds are the columns of the baseline's files that a filter may name.
var columnKinds = map[string]colKind{
	"layer": kindString, "subject_kind": kindString, "source": kindString, "target": kindString,
	"relation": kindString, "producer": kindString, "kind": kindString,
	"a": kindString, "peer": kindString,
	"seq":           kindUint,
	"event_time_ns": kindInt, "ttl_ns": kindInt, "through_ns": kindInt, "start_ns": kindInt, "end_ns": kindInt,
}

// Value is a constant of a filter.
type Value struct {
	kind colKind
	s    string
	i    int64
	u    uint64
}

// Str is a text constant.
func Str(s string) Value { return Value{kind: kindString, s: s} }

// Int is a signed integer constant.
func Int(i int64) Value { return Value{kind: kindInt, i: i} }

// Uint is an unsigned integer constant.
func Uint(u uint64) Value { return Value{kind: kindUint, u: u} }

func (v Value) String() string {
	switch v.kind {
	case kindString:
		return "'" + v.s + "'"
	case kindInt:
		return strconv.FormatInt(v.i, 10)
	case kindUint:
		return strconv.FormatUint(v.u, 10)
	}
	return "?"
}

// compare orders two values of one kind.
func compare(a, b Value) int {
	switch a.kind {
	case kindString:
		return cmp.Compare(a.s, b.s)
	case kindInt:
		return cmp.Compare(a.i, b.i)
	}
	return cmp.Compare(a.u, b.u)
}

// Op is the comparison of a filter.
type Op uint8

const (
	OpEq Op = iota + 1
	OpLt
	OpLe
	OpGt
	OpGe
	// OpIn is an equality with any of a list of constants. It excludes no row group (see
	// [chunk.admits]).
	OpIn
)

func (o Op) String() string {
	switch o {
	case OpEq:
		return "="
	case OpLt:
		return "<"
	case OpLe:
		return "<="
	case OpGt:
		return ">"
	case OpGe:
		return ">="
	case OpIn:
		return " IN "
	}
	return "?"
}

// Filter is a constant filter a statement puts on one column of a scan: column op
// value, or column is in the list, or either of those or the column is null.
type Filter struct {
	Col    string
	Op     Op
	Val    Value
	In     []Value
	OrNull bool
}

func (f Filter) String() string {
	var s string
	if f.Op == OpIn {
		parts := make([]string, len(f.In))
		for i, v := range f.In {
			parts[i] = v.String()
		}
		s = f.Col + " IN (" + strings.Join(parts, ", ") + ")"
	} else {
		s = f.Col + f.Op.String() + f.Val.String()
	}
	if f.OrNull {
		s = "(" + s + " OR " + f.Col + " IS NULL)"
	}
	return s
}

// chunk is what a footer says of one column of one row group.
type chunk struct {
	min, max   *Value
	nulls      int64
	nullsKnown bool
	compressed int64
	bloom      bool
	// start is where the column's pages begin, its dictionary page first if it has one;
	// the pages take compressed bytes. bloomAt and bloomLen are where its bloom filter
	// is, if bloom.
	start, bloomAt, bloomLen int64
}

type rowGroup struct {
	rows int64
	cols map[string]chunk
	// file is the file of the row groups' list the group is in, for a [FileMeta] of
	// several.
	file int
}

// FileMeta is a file's footer as far as counting needs it. For the footers of several
// files (see [LoadFilesMeta]) the row groups are the files' in order, and a group is
// named by its position in that list.
type FileMeta struct {
	Path   string
	Rows   int64
	Groups []rowGroup
	// Files are the paths of the files, if there are several: Path is then the first.
	Files []string
}

// paths are the files the footers are of.
func (m *FileMeta) paths() []string {
	if len(m.Files) > 0 {
		return m.Files
	}
	return []string{m.Path}
}

// LoadFilesMeta reads the footers of several files, which a read through a glob opens,
// as one: the row groups of the first file, then the second's, and so on. The files are
// in the order given.
func LoadFilesMeta(ctx context.Context, db *DB, paths []string) (*FileMeta, error) {
	if len(paths) == 0 {
		return nil, errors.New("duckengine: no file to read the footers of")
	}
	out := &FileMeta{Path: paths[0], Files: slices.Clone(paths)}
	for i, p := range paths {
		m, err := LoadFileMeta(ctx, db, p)
		if err != nil {
			return nil, err
		}
		for _, g := range m.Groups {
			g.file = i
			out.Groups = append(out.Groups, g)
		}
		out.Rows += m.Rows
	}
	return out, nil
}

// ErrNoColumn is wrapped by the error counting returns for a filter or a column the
// file has no footer entry for.
var ErrNoColumn = errors.New("duckengine: the file has no such column")

// LoadFileMeta reads a file's footer with DuckDB.
func LoadFileMeta(ctx context.Context, db *DB, path string) (*FileMeta, error) {
	rows, err := db.QueryContext(ctx, `SELECT row_group_id, row_group_num_rows, path_in_schema, stats_min_value, stats_max_value,
		stats_null_count, total_compressed_size, bloom_filter_offset, bloom_filter_length,
		coalesce(dictionary_page_offset, data_page_offset)
		FROM parquet_metadata(`+quote(path)+`) ORDER BY row_group_id, column_id`)
	if err != nil {
		return nil, fmt.Errorf("duckengine: reading the footer of %s: %w", path, err)
	}
	defer rows.Close()
	m := &FileMeta{Path: path}
	for rows.Next() {
		var (
			id, n, compressed int64
			col               string
			lo, hi            sql.NullString
			nulls             sql.NullInt64
			bloomAt, bloomLen sql.NullInt64
			start             int64
		)
		if err := rows.Scan(&id, &n, &col, &lo, &hi, &nulls, &compressed, &bloomAt, &bloomLen, &start); err != nil {
			return nil, fmt.Errorf("duckengine: reading the footer of %s: %w", path, err)
		}
		for int64(len(m.Groups)) <= id {
			m.Groups = append(m.Groups, rowGroup{cols: make(map[string]chunk)})
		}
		g := &m.Groups[id]
		if g.rows != 0 && g.rows != n {
			return nil, fmt.Errorf("duckengine: %s: row group %d has two row counts", path, id)
		}
		g.rows = n
		c := chunk{nulls: nulls.Int64, nullsKnown: nulls.Valid, compressed: compressed, bloom: bloomAt.Valid, start: start, bloomAt: bloomAt.Int64, bloomLen: bloomLen.Int64}
		if kind, ok := columnKinds[col]; ok && lo.Valid && hi.Valid {
			if c.min, err = parseBound(kind, lo.String); err != nil {
				return nil, fmt.Errorf("duckengine: %s column %s: %w", path, col, err)
			}
			if c.max, err = parseBound(kind, hi.String); err != nil {
				return nil, fmt.Errorf("duckengine: %s column %s: %w", path, col, err)
			}
		}
		g.cols[col] = c
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("duckengine: reading the footer of %s: %w", path, err)
	}
	for _, g := range m.Groups {
		m.Rows += g.rows
	}
	return m, nil
}

func parseBound(kind colKind, s string) (*Value, error) {
	switch kind {
	case kindString:
		v := Str(s)
		return &v, nil
	case kindInt:
		i, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return nil, err
		}
		v := Int(i)
		return &v, nil
	}
	u, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return nil, err
	}
	v := Uint(u)
	return &v, nil
}

// admits reports whether a row group's chunk of a column can hold a row the filter
// lets through. A chunk with no bounds is admitted unless every value in it is null,
// and then only by a filter that accepts null: no bounds is not a reason to skip a
// row group, and a column with nothing but nulls holds no value to compare.
func (c chunk) admits(rows int64, f Filter) bool {
	if c.min == nil || c.max == nil {
		if c.nullsKnown && c.nulls == rows {
			return f.OrNull
		}
		return true
	}
	var ok bool
	switch f.Op {
	case OpEq:
		ok = compare(*c.min, f.Val) <= 0 && compare(f.Val, *c.max) <= 0
	case OpLt:
		ok = compare(*c.min, f.Val) < 0
	case OpLe:
		ok = compare(*c.min, f.Val) <= 0
	case OpGt:
		ok = compare(*c.max, f.Val) > 0
	case OpGe:
		ok = compare(*c.max, f.Val) >= 0
	case OpIn:
		// A list is given to the scan as an optional filter, which is checked against
		// rows and not against a row group's bounds, and a join on the list narrows the
		// scan while it runs, by filters whose values depend on the data. Neither can be
		// counted from the footer, so the list excludes nothing: what a list admits is an
		// upper bound.
		ok = true
	}
	if !ok && f.OrNull && (!c.nullsKnown || c.nulls > 0) {
		ok = true
	}
	return ok
}

// Share is a number of row groups of a file, with their rows and the bytes of the columns
// the read touches.
type Share struct {
	Groups int
	Rows   int64
	Bytes  int64
}

// Admission is what a scan of one file admits. Admitted is the work counter: the row
// groups DuckDB opens, which are those whose bounds the constant filters do not exclude
// and which an equality's bloom filter does not exclude either. BoundsOnly counts by the
// bounds alone: it is a diagnostic, never a work counter and never in a slope class,
// because DuckDB's bloom filters skip row groups that bounds cannot.
type Admission struct {
	Role Role
	// Total is the file's row groups, rows and column bytes.
	Total Share
	// Admitted is the work counter; BoundsOnly the diagnostic. Admitted is never more
	// than BoundsOnly. Rows is the rows of the row groups, and Bytes the compressed bytes
	// of the columns the read touches in them.
	Admitted, BoundsOnly Share
	// Bound says a filter was a list, which excludes nothing here, so that Admitted is an
	// upper bound on what the scan opened and not a count. Such a read is held to G0 and
	// is in no slope class.
	Bound bool
	// Files and FilesRead are, for a read of several files through a glob, how many files
	// the glob names, every one of which a read opens for its footer, and how many of them
	// have a row group admitted, whose pages the read then reads. They are zero for a read
	// of one file.
	Files, FilesRead int `json:",omitempty"`
	// IDs and BoundsIDs are the row groups Admitted and BoundsOnly count, by position in
	// the file, or, for several files, in the list of all their row groups in order. They
	// are not part of what is written: a counters file has the counts.
	IDs, BoundsIDs []int `json:"-"`
}

// Same reports whether two admissions have the same counts. The row groups by position
// are not compared: they are a function of the counts' inputs.
func (a Admission) Same(b Admission) bool {
	return a.Role == b.Role && a.Total == b.Total && a.BoundsOnly == b.BoundsOnly && a.Admitted == b.Admitted && a.Bound == b.Bound &&
		a.Files == b.Files && a.FilesRead == b.FilesRead
}

// Prober says, for an equality on a column, which row groups' bloom filters exclude the
// value.
type Prober interface {
	// Excluded returns, by row group, whether the column's bloom filter excludes the
	// value. A row group with no filter is not excluded.
	Excluded(ctx context.Context, path, col string, v Value) ([]bool, error)
}

// Admit counts what a scan of the file with the filters admits, and the bytes of cols
// in the row groups admitted. The bloom filters are asked only for the row groups the
// bounds admit and only through the prober; with no prober (nil) Admitted equals BoundsOnly, which
// is not the work counter.
func (m *FileMeta) Admit(ctx context.Context, role Role, cols []string, filters []Filter, p Prober) (Admission, error) {
	a := Admission{Role: role}
	for _, f := range filters {
		a.Bound = a.Bound || f.Op == OpIn
	}
	cols = uniq(append(append([]string(nil), cols...), filterCols(filters)...))
	bytesOf := func(g rowGroup) (int64, error) {
		var n int64
		for _, c := range cols {
			ch, ok := g.cols[c]
			if !ok {
				return 0, fmt.Errorf("%w: %s in %s", ErrNoColumn, c, m.Path)
			}
			n += ch.compressed
		}
		return n, nil
	}
	bounds := make([]bool, len(m.Groups))
	for i, g := range m.Groups {
		b, err := bytesOf(g)
		if err != nil {
			return Admission{}, err
		}
		a.Total.add(g.rows, b)
		admitted := true
		for _, f := range filters {
			ch, ok := g.cols[f.Col]
			if !ok {
				return Admission{}, fmt.Errorf("%w: %s in %s", ErrNoColumn, f.Col, m.Path)
			}
			if !ch.admits(g.rows, f) {
				admitted = false
				break
			}
		}
		bounds[i] = admitted
	}
	bloom := append([]bool(nil), bounds...)
	if p != nil {
		for _, f := range filters {
			// Only an equality is probed: a list reaches the scan as a range, which no
			// bloom filter can exclude. A filter that accepts null is not either, since a
			// null is in no bloom filter.
			if f.Op != OpEq || f.OrNull {
				continue
			}
			if !anyTrue(bloom) {
				break
			}
			ex, err := m.excluded(ctx, p, f.Col, f.Val)
			if err != nil {
				return Admission{}, err
			}
			for i := range bloom {
				bloom[i] = bloom[i] && !ex[i]
			}
		}
	}
	read := make(map[int]bool)
	for i, g := range m.Groups {
		b, _ := bytesOf(g)
		if bounds[i] {
			a.BoundsOnly.add(g.rows, b)
			a.BoundsIDs = append(a.BoundsIDs, i)
		}
		if bloom[i] {
			a.Admitted.add(g.rows, b)
			a.IDs = append(a.IDs, i)
			read[g.file] = true
		}
	}
	if len(m.Files) > 0 {
		a.Files, a.FilesRead = len(m.Files), len(read)
	}
	return a, nil
}

// excluded asks the prober, for each file, which row groups' bloom filters exclude the
// value, and returns the answers for every row group in order.
func (m *FileMeta) excluded(ctx context.Context, p Prober, col string, v Value) ([]bool, error) {
	var out []bool
	for fi, path := range m.paths() {
		n := 0
		for _, g := range m.Groups {
			if g.file == fi {
				n++
			}
		}
		ex, err := p.Excluded(ctx, path, col, v)
		if err != nil {
			return nil, err
		}
		if len(ex) != n {
			return nil, fmt.Errorf("duckengine: %s: the bloom probe of %s answered for %d row groups of %d", path, col, len(ex), n)
		}
		out = append(out, ex...)
	}
	return out, nil
}

func (s *Share) add(rows, bytes int64) {
	s.Groups++
	s.Rows += rows
	s.Bytes += bytes
}

func anyTrue(b []bool) bool {
	for _, x := range b {
		if x {
			return true
		}
	}
	return false
}

func filterCols(fs []Filter) []string {
	out := make([]string, len(fs))
	for i, f := range fs {
		out[i] = f.Col
	}
	return out
}

func uniq(s []string) []string {
	seen := make(map[string]bool, len(s))
	out := s[:0]
	for _, x := range s {
		if !seen[x] {
			seen[x] = true
			out = append(out, x)
		}
	}
	return out
}
