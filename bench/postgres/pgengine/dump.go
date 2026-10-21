package pgengine

import (
	"context"
	"fmt"

	"github.com/lotannauo/toposhift/bench/postgres/pgmodel"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// Tables reads the interval tables whole, as the value a [pgmodel.Model] returns from
// its own Tables, in the order of their primary keys, so that the two can be compared
// row for row (Digest, FirstDifference). Both tables are read in one snapshot.
func (e *Engine) Tables(ctx context.Context) (pgmodel.Tables, error) {
	var t pgmodel.Tables
	tx, err := e.pool.BeginTx(ctx, readOnlyRepeatable)
	if err != nil {
		return t, fmt.Errorf("pgengine: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, `SELECT src, dst, rel, layer, start_ns, upper(during), upper_inf(during), deadline_ns, end_source
FROM edges ORDER BY src, dst, rel, start_ns`)
	if err != nil {
		return t, fmt.Errorf("pgengine: dumping edges: %w", err)
	}
	for rows.Next() {
		var src, dst []byte
		var rel string
		var layer, source int16
		var start int64
		var end, deadline *int64
		var inf bool
		if err := rows.Scan(&src, &dst, &rel, &layer, &start, &end, &inf, &deadline, &source); err != nil {
			rows.Close()
			return t, fmt.Errorf("pgengine: dumping edges: %w", err)
		}
		row, err := dumpRow(start, end, inf, deadline, source)
		if err != nil {
			rows.Close()
			return t, err
		}
		a, err := pgmodel.DecodeFingerprint(src)
		if err != nil {
			rows.Close()
			return t, err
		}
		z, err := pgmodel.DecodeFingerprint(dst)
		if err != nil {
			rows.Close()
			return t, err
		}
		t.Edges = append(t.Edges, pgmodel.EdgeRow{Src: a, Dst: z, Rel: catalog.RelationType(rel), Layer: catalog.Layer(layer), Row: row})
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return t, fmt.Errorf("pgengine: dumping edges: %w", err)
	}

	rows, err = tx.Query(ctx, `SELECT fp, layer, start_ns, upper(during), upper_inf(during), deadline_ns, end_source
FROM entities ORDER BY fp, start_ns`)
	if err != nil {
		return t, fmt.Errorf("pgengine: dumping entities: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var fp []byte
		var layer, source int16
		var start int64
		var end, deadline *int64
		var inf bool
		if err := rows.Scan(&fp, &layer, &start, &end, &inf, &deadline, &source); err != nil {
			return t, fmt.Errorf("pgengine: dumping entities: %w", err)
		}
		row, err := dumpRow(start, end, inf, deadline, source)
		if err != nil {
			return t, err
		}
		f, err := pgmodel.DecodeFingerprint(fp)
		if err != nil {
			return t, err
		}
		t.Entities = append(t.Entities, pgmodel.EntityRow{Fp: f, Layer: catalog.Layer(layer), Row: row})
	}
	if err := rows.Err(); err != nil {
		return t, fmt.Errorf("pgengine: dumping entities: %w", err)
	}
	return t, nil
}

func dumpRow(start int64, end *int64, inf bool, deadline *int64, source int16) (pgmodel.Row, error) {
	r := pgmodel.Row{Start: start, EndSource: lifecycle.EndSource(source)}
	switch {
	case inf:
		r.Open = true
	case end == nil:
		return r, fmt.Errorf("pgengine: a row at %d has no upper bound and is not unbounded", start)
	default:
		r.End = *end
	}
	if deadline != nil {
		r.Deadline, r.HasDeadline = *deadline, true
	}
	return r, nil
}
