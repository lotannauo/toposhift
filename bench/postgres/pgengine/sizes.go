package pgengine

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// RelationSize is the bytes of one table or index of a schema, by fork: the main fork
// (the pages the rows or entries are in), the free space map and the visibility map.
type RelationSize struct {
	Name string
	// Kind is "table", "index" or "toast" (a toast table or its index).
	Kind string
	// Table is the table an index or a toast table belongs to; empty for a table.
	Table         string
	Main, FSM, VM int64
}

// Total is the bytes of all forks.
func (r RelationSize) Total() int64 { return r.Main + r.FSM + r.VM }

// QueryQuerier is what [Sizes] runs its statements on.
type QueryQuerier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// The relations of a schema with their sizes: its tables and indexes, and the toast
// tables of its tables with their indexes. An index belongs to the table it indexes;
// a toast table belongs to the table that has it, and is named for it, because the
// name the server gives one is from an object number that differs from schema to schema.
const relationSizes = `SELECT c.relname,
  CASE WHEN c.relkind = 'r' THEN 'table' ELSE 'index' END,
  coalesce(t.relname, ''),
  pg_relation_size(c.oid, 'main'), pg_relation_size(c.oid, 'fsm'), pg_relation_size(c.oid, 'vm')
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_index i ON i.indexrelid = c.oid
LEFT JOIN pg_class t ON t.oid = i.indrelid
WHERE n.nspname = $1 AND c.relkind IN ('r', 'i')
UNION ALL
SELECT 'toast of ' || p.relname, 'toast', p.relname,
  pg_relation_size(x.oid, 'main'), pg_relation_size(x.oid, 'fsm'), pg_relation_size(x.oid, 'vm')
FROM pg_class p JOIN pg_namespace n ON n.oid = p.relnamespace
JOIN pg_class x ON x.oid = p.reltoastrelid
WHERE n.nspname = $1 AND p.reltoastrelid <> 0
UNION ALL
SELECT 'toast index of ' || p.relname, 'toast', p.relname,
  pg_relation_size(xi.oid, 'main'), pg_relation_size(xi.oid, 'fsm'), pg_relation_size(xi.oid, 'vm')
FROM pg_class p JOIN pg_namespace n ON n.oid = p.relnamespace
JOIN pg_index i ON i.indrelid = p.reltoastrelid
JOIN pg_class xi ON xi.oid = i.indexrelid
WHERE n.nspname = $1 AND p.reltoastrelid <> 0
ORDER BY 1`

// Sizes returns the size of every relation of the schema, by name. The numbers are the
// server's own and do not depend on the machine, only on the data and how it was
// written.
func Sizes(ctx context.Context, q QueryQuerier, schema string) ([]RelationSize, error) {
	rows, err := q.Query(ctx, relationSizes, schema)
	if err != nil {
		return nil, fmt.Errorf("pgengine: sizes of schema %s: %w", schema, err)
	}
	defer rows.Close()
	var out []RelationSize
	for rows.Next() {
		var r RelationSize
		if err := rows.Scan(&r.Name, &r.Kind, &r.Table, &r.Main, &r.FSM, &r.VM); err != nil {
			return nil, fmt.Errorf("pgengine: sizes of schema %s: %w", schema, err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pgengine: sizes of schema %s: %w", schema, err)
	}
	return out, nil
}

// Compact rewrites every table of the schema and rebuilds its indexes (VACUUM FULL, then
// REINDEX), so that the sizes afterwards are of the data and its indexes with no dead
// space and no bloat left by how they were written. It changes the pages of the
// tables: reads that are to be counted on the layout the load left are made before it.
func Compact(ctx context.Context, conn *pgx.Conn, schema string) error {
	rows, err := conn.Query(ctx, `SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = $1 AND c.relkind = 'r' ORDER BY c.relname`, schema)
	if err != nil {
		return fmt.Errorf("pgengine: listing the tables of schema %s: %w", schema, err)
	}
	var tables []string
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			rows.Close()
			return err
		}
		tables = append(tables, t)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, t := range tables {
		name := pgx.Identifier{schema, t}.Sanitize()
		if _, err := conn.Exec(ctx, "VACUUM (FULL) "+name); err != nil {
			return fmt.Errorf("pgengine: vacuum full of %s: %w", name, err)
		}
		if _, err := conn.Exec(ctx, "REINDEX TABLE "+name); err != nil {
			return fmt.Errorf("pgengine: reindex of %s: %w", name, err)
		}
	}
	if _, err := conn.Exec(ctx, "CHECKPOINT"); err != nil {
		return fmt.Errorf("pgengine: checkpoint: %w", err)
	}
	return nil
}
