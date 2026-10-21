package duckengine

import (
	"context"
	"database/sql"
	"fmt"
	"slices"

	"github.com/lotannauo/toposhift/bench/duckdb/duckmodel"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
)

// readIntervalRows reads interval rows from a query: an edge query returns peer and
// relation after the interval's four columns, and a query with an end source returns it
// after the end. Without one the rows have none, which is how a file holds them; it is
// kept only in the build's staging tables, which are in memory.
func readIntervalRows(ctx context.Context, db *DB, q string, edges, withEndSource bool) ([]duckmodel.IntervalRow, error) {
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("duckengine: %w", err)
	}
	defer rows.Close()
	var out []duckmodel.IntervalRow
	for rows.Next() {
		var (
			layer, a         string
			start            int64
			end              sql.NullInt64
			endSource        sql.NullString
			peer, relationNS sql.NullString
		)
		dest := []any{&layer, &a, &start, &end}
		if withEndSource {
			dest = append(dest, &endSource)
		}
		if edges {
			dest = append(dest, &peer, &relationNS)
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, fmt.Errorf("duckengine: %w", err)
		}
		l, err := parseLayer(layer)
		if err != nil {
			return nil, err
		}
		row := duckmodel.IntervalRow{Layer: l, A: a, Peer: peer.String, Interval: duckmodel.Interval{Start: start}}
		row.Relation = catalog.RelationType(relationNS.String)
		switch {
		case withEndSource && end.Valid != endSource.Valid:
			return nil, fmt.Errorf("duckengine: an interval of %s has an end and no end source, or the reverse", a)
		case end.Valid:
			row.End = end.Int64
			if withEndSource {
				if row.EndSource, err = parseEndSource(endSource.String); err != nil {
					return nil, err
				}
			}
		default:
			row.Open = true
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("duckengine: %w", err)
	}
	return out, nil
}

// withoutEndSource is rows as a file holds them: the end source is not a column of an
// interval file, so it is not compared there.
func withoutEndSource(rows []duckmodel.IntervalRow) []duckmodel.IntervalRow {
	out := slices.Clone(rows)
	for i := range out {
		out[i].EndSource = 0
	}
	return out
}

// verifyIntervals compares the interval files, row for row and in the order they hold
// their rows, with the rows the model derives from the same records, and the staged rows
// the files were written from, with their end sources, with the model's in full. The
// files hold no end source (the registered interval file is layer, a, peer, relation,
// start and end), so the end sources are checked where they are, in memory. It must be
// called before the staging tables are dropped.
func verifyIntervals(ctx context.Context, db *DB, dir string, model *duckmodel.Model) error {
	fwd, err := model.EdgeIntervals(engine.Forward)
	if err != nil {
		return err
	}
	rev, err := model.EdgeIntervals(engine.Reverse)
	if err != nil {
		return err
	}
	ents, err := model.EntityIntervals()
	if err != nil {
		return err
	}
	for _, c := range []struct {
		role  Role
		edges bool
		want  []duckmodel.IntervalRow
	}{{RoleIFwd, true, fwd}, {RoleIRev, true, rev}, {RoleIEnt, false, ents}} {
		q := "SELECT layer, a, start_ns, end_ns FROM read_parquet(" + quote(rolePathIn(dir, c.role)) + ")"
		if c.edges {
			q = "SELECT layer, a, start_ns, end_ns, peer, relation FROM read_parquet(" + quote(rolePathIn(dir, c.role)) + ")"
		}
		got, err := readIntervalRows(ctx, db, q, c.edges, false)
		if err != nil {
			return err
		}
		if err := EqualRows(got, withoutEndSource(c.want)); err != nil {
			return fmt.Errorf("duckengine: %s: %w", rolePath[c.role], err)
		}
	}

	// The staged rows are of edges from their source; the reverse rows are the same rows
	// seen from the target.
	staged, err := readIntervalRows(ctx, db, "SELECT layer, source, start_ns, end_ns, end_source, target, relation FROM iv_edge", true, true)
	if err != nil {
		return err
	}
	stagedRev := slices.Clone(staged)
	for i := range stagedRev {
		stagedRev[i].A, stagedRev[i].Peer = stagedRev[i].Peer, stagedRev[i].A
	}
	stagedEnts, err := readIntervalRows(ctx, db, "SELECT layer, a, start_ns, end_ns, end_source FROM iv_ent", false, true)
	if err != nil {
		return err
	}
	duckmodel.SortRows(staged)
	duckmodel.SortRows(stagedRev)
	duckmodel.SortRows(stagedEnts)
	for name, c := range map[string][2][]duckmodel.IntervalRow{"edges from the source": {staged, fwd}, "edges from the target": {stagedRev, rev}, "entities": {stagedEnts, ents}} {
		if err := EqualRows(c[0], c[1]); err != nil {
			return fmt.Errorf("duckengine: the intervals of %s, with their end sources: %w", name, err)
		}
	}
	return nil
}

// EqualRows reports the first row at which two lists of interval rows differ.
func EqualRows(got, want []duckmodel.IntervalRow) error {
	for i := 0; i < len(got) && i < len(want); i++ {
		if got[i] != want[i] {
			return fmt.Errorf("row %d is %+v, the model's is %+v", i, got[i], want[i])
		}
	}
	if len(got) != len(want) {
		return fmt.Errorf("%d rows, the model has %d", len(got), len(want))
	}
	return nil
}
