package duckengine

import (
	"context"
	"errors"
	"math"
	"path/filepath"
	"slices"
	"testing"
)

// The fixture is four row groups of 2,048 rows, written by DuckDB, whose bounds are known
// exactly:
//
//	seq            1..8192 in order; group g holds 2048g+1 .. 2048(g+1)
//	event_time_ns  10 * seq
//	layer          "L1" in groups 0 and 1, "L2" in groups 2 and 3
//	source         "Z" in group 0, "a" in group 1 (so byte order puts the capital first),
//	               and "k" followed by a number in 4 steps: row i holds "k" + (i mod 64), so
//	               the groups' ranges all overlap while a value is in every group
//	target         group g holds "t<g>_" followed by the row mod 8: bounds overlap not at all
//	               across groups, but each value is in one group's bloom filter only
//	mix            interleaved so that every group's bounds are the same but each holds only
//	               its own: row i holds "m" + (i mod 4)*4 + group
//	end_ns         null throughout group 1, null in every other row of group 2, a value
//	               (seq) elsewhere
func fixtureFile(t *testing.T) (string, *DB) {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Mode: ModeBuild, TempDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	path := filepath.Join(t.TempDir(), "fixture.parquet")
	stmt := `COPY (SELECT
		(i + 1)::UBIGINT AS seq,
		((i + 1) * 10)::BIGINT AS event_time_ns,
		CASE WHEN i < 4096 THEN 'L1' ELSE 'L2' END AS layer,
		CASE WHEN i < 2048 THEN 'Z' WHEN i < 4096 THEN 'a' ELSE 'k' || lpad((i % 64)::VARCHAR, 2, '0') END AS source,
		't' || (i // 2048)::VARCHAR || '_' || (i % 8)::VARCHAR AS target,
		'm' || lpad((((i % 4) * 4) + (i // 2048))::VARCHAR, 2, '0') AS mix,
		CASE WHEN i // 2048 = 1 THEN NULL WHEN i // 2048 = 2 AND i % 2 = 0 THEN NULL ELSE (i + 1)::BIGINT END AS end_ns
		FROM range(8192) t(i) ORDER BY i) TO ` + quote(path) + ` (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 2048)`
	if _, err := db.ExecContext(ctx, stmt); err != nil {
		t.Fatal(err)
	}
	return path, db
}

func TestFooterIsRead(t *testing.T) {
	path, db := fixtureFile(t)
	m, err := LoadFileMeta(context.Background(), db, path)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Groups) != 4 || m.Rows != 8192 {
		t.Fatalf("%d row groups, %d rows", len(m.Groups), m.Rows)
	}
	for i, g := range m.Groups {
		if g.rows != 2048 {
			t.Errorf("group %d has %d rows", i, g.rows)
		}
		c := g.cols["seq"]
		if c.min == nil || *c.min != Uint(uint64(2048*i+1)) || *c.max != Uint(uint64(2048*(i+1))) {
			t.Errorf("group %d seq bounds %v..%v", i, c.min, c.max)
		}
		if c.compressed <= 0 || c.start <= 0 {
			t.Errorf("group %d seq chunk: %+v", i, c)
		}
	}
	if c := m.Groups[1].cols["end_ns"]; c.min != nil || !c.nullsKnown || c.nulls != 2048 {
		t.Errorf("the all-null chunk: %+v", c)
	}
	if c := m.Groups[2].cols["end_ns"]; c.min == nil || c.nulls != 1024 {
		t.Errorf("the half-null chunk: %+v", c)
	}
}

func ids(t *testing.T, m *FileMeta, p Prober, fs ...Filter) (bounds, bloom []int) {
	t.Helper()
	a, err := m.Admit(context.Background(), RoleFwd, []string{"seq"}, fs, p)
	if err != nil {
		t.Fatal(err)
	}
	bounds = nil
	for i := range m.Groups {
		// Recompute the bounds set: Admit reports the bloom set by position.
		ok := true
		for _, f := range fs {
			if !m.Groups[i].cols[f.Col].admits(m.Groups[i].rows, f) {
				ok = false
			}
		}
		if ok {
			bounds = append(bounds, i)
		}
	}
	if a.BoundsOnly.Groups != len(bounds) {
		t.Fatalf("Admit counted %d row groups by bounds, the chunks admit %d", a.BoundsOnly.Groups, len(bounds))
	}
	return bounds, a.IDs
}

// TestBoundsAdmitExactlyAtTheEdges holds the comparison of a bound to a constant to the
// boundary of every operator: a row group is skipped only if no row of it can pass, and
// a value at its smallest or its largest can.
func TestBoundsAdmitExactlyAtTheEdges(t *testing.T) {
	path, db := fixtureFile(t)
	m, err := LoadFileMeta(context.Background(), db, path)
	if err != nil {
		t.Fatal(err)
	}
	seq := func(op Op, v uint64) Filter { return Filter{Col: "seq", Op: op, Val: Uint(v)} }
	for _, c := range []struct {
		name string
		f    Filter
		want []int
	}{
		{"= the last of group 0", seq(OpEq, 2048), []int{0}},
		{"= the first of group 1", seq(OpEq, 2049), []int{1}},
		{"= below the first", seq(OpEq, 0), nil},
		{"= above the last", seq(OpEq, 8193), nil},
		{"<= the last of group 0", seq(OpLe, 2048), []int{0}},
		{"<= the first of group 1", seq(OpLe, 2049), []int{0, 1}},
		{"< the first of group 1", seq(OpLt, 2049), []int{0}},
		{"< the first", seq(OpLt, 1), nil},
		{"< above the first", seq(OpLt, 2), []int{0}},
		{">= the last of group 0", seq(OpGe, 2048), []int{0, 1, 2, 3}},
		{">= the first of group 1", seq(OpGe, 2049), []int{1, 2, 3}},
		{"> the last of group 0", seq(OpGt, 2048), []int{1, 2, 3}},
		{"> the last", seq(OpGt, 8192), nil},
		{">= the last", seq(OpGe, 8192), []int{3}},
		{"<= the largest uint64", seq(OpLe, math.MaxUint64), []int{0, 1, 2, 3}},
		{"= the largest uint64", seq(OpEq, math.MaxUint64), nil},
		{"event time <=", Filter{Col: "event_time_ns", Op: OpLe, Val: Int(20480)}, []int{0}},
		{"event time < the first of group 1", Filter{Col: "event_time_ns", Op: OpLt, Val: Int(20490)}, []int{0}},
		{"event time < above the first of group 1", Filter{Col: "event_time_ns", Op: OpLt, Val: Int(20491)}, []int{0, 1}},
		{"event time = a value of group 2", Filter{Col: "event_time_ns", Op: OpEq, Val: Int(50000)}, []int{2}},
		{"text order is bytes: Z before a", Filter{Col: "source", Op: OpLt, Val: Str("a")}, []int{0}},
		{"text = Z", Filter{Col: "source", Op: OpEq, Val: Str("Z")}, []int{0}},
		{"text = a", Filter{Col: "source", Op: OpEq, Val: Str("a")}, []int{1}},
		{"text inside the range of groups 2 and 3", Filter{Col: "source", Op: OpEq, Val: Str("k30")}, []int{2, 3}},
		{"text below the range of groups 2 and 3", Filter{Col: "source", Op: OpEq, Val: Str("b")}, nil},
		{"text above all", Filter{Col: "source", Op: OpGt, Val: Str("k99")}, nil},
		{"layer", Filter{Col: "layer", Op: OpEq, Val: Str("L2")}, []int{2, 3}},
	} {
		bounds, _ := ids(t, m, nil, c.f)
		if !slices.Equal(bounds, c.want) {
			t.Errorf("%s: %s admits %v, want %v", c.name, c.f, bounds, c.want)
		}
	}
	// Two filters must both admit.
	bounds, _ := ids(t, m, nil, seq(OpLe, 5000), Filter{Col: "layer", Op: OpEq, Val: Str("L2")})
	if !slices.Equal(bounds, []int{2}) {
		t.Errorf("two filters admit %v", bounds)
	}
	// A list excludes nothing: it reaches the scan as a filter that is checked against
	// rows.
	bounds, _ = ids(t, m, nil, Filter{Col: "seq", Op: OpIn, In: []Value{Uint(1), Uint(8192)}})
	if !slices.Equal(bounds, []int{0, 1, 2, 3}) {
		t.Errorf("a list admits %v", bounds)
	}
	a, _ := m.Admit(context.Background(), RoleFwd, nil, []Filter{{Col: "seq", Op: OpIn, In: []Value{Uint(1)}}}, nil)
	if !a.Bound {
		t.Error("a list is not marked as an upper bound")
	}
}

// TestNullsAreNotNoStatistics: a column with nothing but nulls has no bounds, and a
// value filter rejects its row group while one that accepts null keeps it; a chunk whose
// bounds are unknown is kept.
func TestNullsAreNotNoStatistics(t *testing.T) {
	path, db := fixtureFile(t)
	m, err := LoadFileMeta(context.Background(), db, path)
	if err != nil {
		t.Fatal(err)
	}
	gt := Filter{Col: "end_ns", Op: OpGt, Val: Int(0)}
	bounds, _ := ids(t, m, nil, gt)
	if !slices.Equal(bounds, []int{0, 2, 3}) {
		t.Errorf("a value filter on a column that is null throughout group 1 admits %v", bounds)
	}
	gt.OrNull = true
	bounds, _ = ids(t, m, nil, gt)
	if !slices.Equal(bounds, []int{0, 1, 2, 3}) {
		t.Errorf("the same or null admits %v", bounds)
	}
	// Past every value of group 2, only its nulls (and group 3) can pass.
	late := Filter{Col: "end_ns", Op: OpGt, Val: Int(6200), OrNull: true}
	bounds, _ = ids(t, m, nil, late)
	if !slices.Equal(bounds, []int{1, 2, 3}) {
		t.Errorf("group 0 has no null and no value above 6200, groups 1 and 2 have nulls, group 3 values: %v", bounds)
	}
	late.OrNull = false
	bounds, _ = ids(t, m, nil, late)
	if !slices.Equal(bounds, []int{3}) {
		t.Errorf("without null: %v", bounds)
	}
	// A chunk with no bounds and not all null is not a reason to skip.
	unknown := chunk{}
	if !unknown.admits(10, Filter{Col: "seq", Op: OpEq, Val: Uint(1)}) {
		t.Error("a chunk with no statistics was skipped")
	}
	allNull := chunk{nulls: 10, nullsKnown: true}
	if allNull.admits(10, Filter{Col: "seq", Op: OpEq, Val: Uint(1)}) || !allNull.admits(10, Filter{Col: "seq", Op: OpEq, Val: Uint(1), OrNull: true}) {
		t.Error("an all-null chunk")
	}
}

// TestBloomFiltersExcludeWhatBoundsCannot: a value in the range of every row group but
// in one of them is excluded from the others by their bloom filters, as DuckDB's own probe
// says, and an unknown value from all.
func TestBloomFiltersExcludeWhatBoundsCannot(t *testing.T) {
	path, db := fixtureFile(t)
	ctx := context.Background()
	m, err := LoadFileMeta(ctx, db, path)
	if err != nil {
		t.Fatal(err)
	}
	p := DBProber{DB: db}
	// "m05" is held by group 1 only (row i mod 4 = 1, group 1).
	f := Filter{Col: "mix", Op: OpEq, Val: Str("m05")}
	bounds, bloom := ids(t, m, p, f)
	if !slices.Equal(bounds, []int{0, 1, 2, 3}) || !slices.Equal(bloom, []int{1}) {
		t.Errorf("bounds admit %v and bloom filters %v; want all four and group 1", bounds, bloom)
	}
	// A value nobody holds but inside every range.
	f.Val = Str("m05x")
	_, bloom = ids(t, m, p, f)
	if len(bloom) != 0 {
		t.Errorf("a value no row group holds is admitted by %v", bloom)
	}
	// The probe is only asked for equalities that are not or-null, and only if some row
	// group is left to exclude.
	asked := 0
	counting := probeFunc(func(ctx context.Context, path, col string, v Value) ([]bool, error) {
		asked++
		return DBProber{DB: db}.Excluded(ctx, path, col, v)
	})
	if _, err := m.Admit(ctx, RoleFwd, nil, []Filter{{Col: "seq", Op: OpLe, Val: Uint(5)}, {Col: "mix", Op: OpIn, In: []Value{Str("m05")}}}, counting); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Admit(ctx, RoleFwd, nil, []Filter{{Col: "mix", Op: OpEq, Val: Str("m05"), OrNull: true}}, counting); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Admit(ctx, RoleFwd, nil, []Filter{{Col: "seq", Op: OpGt, Val: Uint(9000)}, {Col: "mix", Op: OpEq, Val: Str("m05")}}, counting); err != nil {
		t.Fatal(err)
	}
	if asked != 0 {
		t.Errorf("the probe was asked %d times where it could not exclude anything", asked)
	}
}

func TestSharesAreCountedFromTheChunks(t *testing.T) {
	path, db := fixtureFile(t)
	ctx := context.Background()
	m, err := LoadFileMeta(ctx, db, path)
	if err != nil {
		t.Fatal(err)
	}
	cols := []string{"seq", "source"}
	a, err := m.Admit(ctx, RoleFwd, cols, []Filter{{Col: "layer", Op: OpEq, Val: Str("L2")}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	var total, l2 int64
	for i, g := range m.Groups {
		b := g.cols["seq"].compressed + g.cols["source"].compressed + g.cols["layer"].compressed
		total += b
		if i >= 2 {
			l2 += b
		}
	}
	if a.Total.Groups != 4 || a.Total.Rows != 8192 || a.Total.Bytes != total {
		t.Errorf("total %+v, want 4 groups, 8192 rows, %d bytes (the filter's column is read too)", a.Total, total)
	}
	if a.BoundsOnly.Groups != 2 || a.BoundsOnly.Rows != 4096 || a.BoundsOnly.Bytes != l2 || a.Admitted != a.BoundsOnly {
		t.Errorf("admitted %+v, want 2 groups, 4096 rows, %d bytes", a.BoundsOnly, l2)
	}
	if a.Bound {
		t.Error("a scan with no list is counted by a bound")
	}
	if _, err := m.Admit(ctx, RoleFwd, []string{"nope"}, nil, nil); !errors.Is(err, ErrNoColumn) {
		t.Errorf("a column the file does not have: %v", err)
	}
	if _, err := m.Admit(ctx, RoleFwd, nil, []Filter{{Col: "nope", Op: OpEq, Val: Str("x")}}, nil); !errors.Is(err, ErrNoColumn) {
		t.Errorf("a filter on a column the file does not have: %v", err)
	}
}
