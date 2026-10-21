package pgengine

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/lotannauo/toposhift/bench/postgres/pgmodel"
	"github.com/lotannauo/toposhift/bench/postgres/pgschema"
	"github.com/lotannauo/toposhift/bench/spike/engine"
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

// SchemaDigest is the digest of the interval tables of a schema, and how many rows it
// covers.
type SchemaDigest struct {
	// Digest is [pgmodel.DigestWriter]'s digest of the edge rows and then the entity
	// rows, in the order of [pgmodel.Tables], clipped at the horizon the schema's meta
	// row records, so that a schema that kept an interval that began before the horizon
	// and one that began it at the horizon are one.
	Digest string
	Rows   int64
	// Horizon is the horizon the rows were clipped at: Unix nanoseconds, 0 if none.
	Horizon int64
}

// The id arm names an entity by its id, and the digest is of the fingerprints, so its
// rows are joined to the dictionary and sorted by them.
const (
	digestEdges = `SELECT src, dst, rel, layer, start_ns, upper(during), upper_inf(during), deadline_ns, end_source
FROM edges ORDER BY src, dst, rel, start_ns`
	digestEntities = `SELECT fp, layer, start_ns, upper(during), upper_inf(during), deadline_ns, end_source
FROM entities ORDER BY fp, start_ns`
	digestEdgesID = `SELECT s.fp, d.fp, e.rel, e.layer, e.start_ns, upper(e.during), upper_inf(e.during), e.deadline_ns, e.end_source
FROM edges e JOIN entity s ON s.id = e.src JOIN entity d ON d.id = e.dst ORDER BY s.fp, d.fp, e.rel, e.start_ns`
	digestEntitiesID = `SELECT s.fp, e.layer, e.start_ns, upper(e.during), upper_inf(e.during), e.deadline_ns, e.end_source
FROM entities e JOIN entity s ON s.id = e.fp ORDER BY s.fp, e.start_ns`
)

// DigestSchema reads the interval tables of a schema, in one snapshot and without
// holding them, and digests them. It works for every arm, the id arm included, whose
// keys it reads through the dictionary: a schema of the id arm and one of the bulk arm
// loaded from one stream have one digest.
func DigestSchema(ctx context.Context, dsn, schema string, arm pgschema.Arm) (SchemaDigest, error) {
	var d SchemaDigest
	if !schemaName.MatchString(schema) {
		return d, fmt.Errorf("pgengine: %q is not a usable schema name: %w", schema, engine.ErrInvalid)
	}
	cc, err := pgx.ParseConfig(dsn)
	if err != nil {
		return d, fmt.Errorf("pgengine: DSN: %w", err)
	}
	cc.RuntimeParams["search_path"] = pgx.Identifier{schema}.Sanitize() + ", public"
	conn, err := pgx.ConnectConfig(ctx, cc)
	if err != nil {
		return d, fmt.Errorf("pgengine: %w", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	tx, err := conn.BeginTx(ctx, readOnlyRepeatable)
	if err != nil {
		return d, fmt.Errorf("pgengine: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var horizon *int64
	if err := tx.QueryRow(ctx, "SELECT horizon_ns FROM meta").Scan(&horizon); err != nil {
		return d, fmt.Errorf("pgengine: reading the horizon: %w", err)
	}
	if horizon != nil {
		d.Horizon = *horizon
	}
	w := pgmodel.NewDigestWriter(d.Horizon)
	edges, entities := digestEdges, digestEntities
	if arm == pgschema.BulkID {
		edges, entities = digestEdgesID, digestEntitiesID
	}
	rows, err := tx.Query(ctx, edges)
	if err != nil {
		return d, fmt.Errorf("pgengine: digesting edges: %w", err)
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
			return d, fmt.Errorf("pgengine: digesting edges: %w", err)
		}
		row, err := dumpRow(start, end, inf, deadline, source)
		if err != nil {
			rows.Close()
			return d, err
		}
		a, err := pgmodel.DecodeFingerprint(src)
		if err != nil {
			rows.Close()
			return d, err
		}
		z, err := pgmodel.DecodeFingerprint(dst)
		if err != nil {
			rows.Close()
			return d, err
		}
		w.Edge(pgmodel.EdgeRow{Src: a, Dst: z, Rel: catalog.RelationType(rel), Layer: catalog.Layer(layer), Row: row})
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return d, fmt.Errorf("pgengine: digesting edges: %w", err)
	}
	rows, err = tx.Query(ctx, entities)
	if err != nil {
		return d, fmt.Errorf("pgengine: digesting entities: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var fp []byte
		var layer, source int16
		var start int64
		var end, deadline *int64
		var inf bool
		if err := rows.Scan(&fp, &layer, &start, &end, &inf, &deadline, &source); err != nil {
			return d, fmt.Errorf("pgengine: digesting entities: %w", err)
		}
		row, err := dumpRow(start, end, inf, deadline, source)
		if err != nil {
			return d, err
		}
		f, err := pgmodel.DecodeFingerprint(fp)
		if err != nil {
			return d, err
		}
		w.Entity(pgmodel.EntityRow{Fp: f, Layer: catalog.Layer(layer), Row: row})
	}
	if err := rows.Err(); err != nil {
		return d, fmt.Errorf("pgengine: digesting entities: %w", err)
	}
	d.Digest, d.Rows = w.Sum(), w.Rows()
	return d, nil
}
