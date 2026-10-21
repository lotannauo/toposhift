// Package pgengine is an [engine.Engine] over a Postgres 17 server: the interval
// tables of package pgschema, written by the planner of package pgmodel and read with
// range types and GiST, so that the spike's reads can be asked of a real database and
// checked against the reference engine.
//
// # What it stores
//
// Every record goes to the log, so that Window and a read as of an old snapshot token
// have what they read. The intervals derived from the log, one row per maximal
// interval of existence of each edge and each entity, are what a read at an instant
// finds, and are written by [pgmodel.Refold]: for each subject a batch touches, the
// engine reads the stored rows from the start of the interval the batch lands in and
// the log from there, folds them, and applies the rows that differ. The SQL and the
// in-memory [pgmodel.Model] therefore make the same row changes, and a test holds the
// two to each other after every batch.
//
// A read as of the latest token is answered from the interval tables. A read as of an
// earlier token has no interval table to use, because the intervals carry no ingest
// time, so the log of each subject is read and folded by the reader.
//
// # Arms
//
// The engine runs the arms of package pgschema that a read can be asked of. The
// streaming arms ([pgschema.Stream], [pgschema.StreamNoPartition]) re-fold from the
// interval a batch lands in. The bulk arms ([pgschema.Bulk], [pgschema.BulkB]) have no
// primary key and are rebuilt: each subject a batch touches is folded whole. They
// differ in the index set their reads use, G (GiST on the interval) or B (a btree on
// its end). The id-dictionary arm is for sizes only and is refused.
//
// # Consistency
//
// A batch, and a retention, is one transaction, so a reader sees a whole batch or none
// of it. A read at the latest token is one statement. A read at an earlier token needs
// the token to decide whether the interval tables are current, and so runs in one
// repeatable-read transaction that reads it first. Write and Retain are serialized
// (by the caller, as the contract says, and by a lock on the meta row).
package pgengine

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/lotannauo/toposhift/bench/postgres/pgmodel"
	"github.com/lotannauo/toposhift/bench/postgres/pgschema"
	"github.com/lotannauo/toposhift/bench/spike/engine"
)

// FoldVersion is the version of the fold logic the interval rows are built with.
// It is stored with them, and a schema built with another version is refused: its
// rows may not be what this planner would write.
const FoldVersion = 1

// defaultMaxConns is the size of an engine's connection pool when Config gives none.
const defaultMaxConns = 4

// setupLock is the advisory lock that serializes the creation of extensions and
// schemas, which concurrent engines opening the same database would otherwise race
// on. Its value is arbitrary.
const setupLock = 7428710

// ErrSchema is wrapped by the error Open returns when the schema it was asked to
// open holds something else than an engine of this version and arm.
var ErrSchema = errors.New("pgengine: the schema is not this engine's")

var schemaName = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

// Config says which server and which tables an engine uses.
type Config struct {
	// DSN is a pgx connection string, as a URL or as keyword/value pairs.
	DSN string
	// Schema is the schema the arm's tables live in, created if it is absent.
	// [pgschema.SchemaName] makes one from a directory.
	Schema string
	// Arm is the set of tables and indexes.
	Arm pgschema.Arm
	// MaxConns bounds the connections the engine opens (default 4).
	MaxConns int32
}

// layout is what differs between the arms the engine runs.
type layout struct {
	arm pgschema.Arm
	// whole says a subject is folded whole at every write, as a bulk load builds it,
	// not from the interval a batch lands in.
	whole bool
	// partitioned says the log is partitioned by day, and a write creates the days it
	// reaches.
	partitioned bool
	// provisional says rows may carry a deadline, which a read must honour: the
	// bulk arms never write one and omit the term.
	provisional bool
}

func newLayout(a pgschema.Arm) (layout, error) {
	switch a {
	case pgschema.Stream:
		return layout{arm: a, partitioned: true, provisional: true}, nil
	case pgschema.StreamNoPartition:
		return layout{arm: a, provisional: true}, nil
	case pgschema.Bulk, pgschema.BulkB:
		return layout{arm: a, whole: true}, nil
	case pgschema.BulkID:
		return layout{}, fmt.Errorf("pgengine: the arm %s is for sizes and has no engine: %w", a, engine.ErrInvalid)
	}
	return layout{}, fmt.Errorf("pgengine: unknown arm %q: %w", a, engine.ErrInvalid)
}

// Engine is an engine over one schema of a Postgres server. It implements
// [engine.Engine]. Reads run concurrently with each other and with Write.
type Engine struct {
	pool   *pgxpool.Pool
	schema string
	lay    layout
	ctx    context.Context
	cancel context.CancelFunc

	// wmu serializes Write and Retain, and guards counts and parts.
	wmu    sync.Mutex
	counts pgmodel.Counts
	// parts are the day partitions of the log this engine has seen committed.
	parts map[string]bool
}

var _ engine.Engine = (*Engine)(nil)

// Open connects to the server and opens the schema, creating it, its extensions and
// its tables if the schema is new. A schema that an engine of the same arm and fold
// version left is opened as it is, with its token and its horizon: nothing is kept in
// memory.
func Open(ctx context.Context, cfg Config) (*Engine, error) {
	lay, err := newLayout(cfg.Arm)
	if err != nil {
		return nil, err
	}
	if !schemaName.MatchString(cfg.Schema) {
		return nil, fmt.Errorf("pgengine: %q is not a usable schema name: %w", cfg.Schema, engine.ErrInvalid)
	}
	if cfg.DSN == "" {
		return nil, errors.New("pgengine: no DSN")
	}
	pc, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("pgengine: DSN: %w", err)
	}
	pc.MaxConns = cfg.MaxConns
	if pc.MaxConns <= 0 {
		pc.MaxConns = defaultMaxConns
	}
	// Every statement names its tables bare: the schema is the connection's.
	pc.ConnConfig.RuntimeParams["search_path"] = pgx.Identifier{cfg.Schema}.Sanitize() + ", public"
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return nil, fmt.Errorf("pgengine: %w", err)
	}
	e := &Engine{pool: pool, schema: cfg.Schema, lay: lay, parts: make(map[string]bool)}
	e.ctx, e.cancel = context.WithCancel(context.Background())
	if err := e.setup(ctx); err != nil {
		pool.Close()
		e.cancel()
		return nil, err
	}
	return e, nil
}

// setup creates the schema, or checks the one that is there.
func (e *Engine) setup(ctx context.Context) error {
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("pgengine: connecting: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	simple := pgx.QueryExecModeSimpleProtocol
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", setupLock); err != nil {
		return fmt.Errorf("pgengine: %w", err)
	}
	for _, s := range pgschema.Extensions() {
		if _, err := tx.Exec(ctx, s, simple); err != nil {
			return fmt.Errorf("pgengine: %s: %w", s, err)
		}
	}
	q := pgx.Identifier{e.schema}.Sanitize()
	if _, err := tx.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+q, simple); err != nil {
		return fmt.Errorf("pgengine: creating schema %s: %w", e.schema, err)
	}
	var exists bool
	if err := tx.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", q+".meta").Scan(&exists); err != nil {
		return fmt.Errorf("pgengine: %w", err)
	}
	if exists {
		var arm string
		var fold int
		if err := tx.QueryRow(ctx, "SELECT arm, fold_version FROM meta").Scan(&arm, &fold); err != nil {
			return fmt.Errorf("pgengine: reading the meta row of schema %s: %w: %w", e.schema, err, ErrSchema)
		}
		if arm != string(e.lay.arm) || fold != FoldVersion {
			return fmt.Errorf("pgengine: schema %s holds arm %q built with fold version %d; this engine is arm %q, version %d: %w",
				e.schema, arm, fold, e.lay.arm, FoldVersion, ErrSchema)
		}
		return tx.Commit(ctx)
	}
	// A new schema. The bulk arms build their indexes after a load, and the engine's
	// loads are rebuilds of single subjects, so it has them from the start.
	stmts, err := pgschema.Tables(e.lay.arm)
	if err != nil {
		return err
	}
	idx, err := pgschema.Indexes(e.lay.arm)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, "SET LOCAL search_path TO "+q+", public", simple); err != nil {
		return fmt.Errorf("pgengine: %w", err)
	}
	for _, s := range append(stmts, idx...) {
		if _, err := tx.Exec(ctx, s, simple); err != nil {
			return fmt.Errorf("pgengine: %.60s...: %w", s, err)
		}
	}
	if _, err := tx.Exec(ctx, "INSERT INTO meta (last_seq, fold_version, arm) VALUES ($1, $2, $3)", seqToDB(0), FoldVersion, string(e.lay.arm)); err != nil {
		return fmt.Errorf("pgengine: %w", err)
	}
	return tx.Commit(ctx)
}

// Close releases the connections. The schema and what it holds stay, for Open.
func (e *Engine) Close() error {
	e.cancel()
	e.pool.Close()
	return nil
}

// Schema is the schema the engine's tables are in.
func (e *Engine) Schema() string { return e.schema }

// Counts returns what the engine has written, in the units of [pgmodel.Counts]: rows
// inserted, updated and deleted in the interval tables, the subjects re-folded and the
// records those folds read. For a streaming arm they equal the counts of a
// [pgmodel.Model] that was given the same stream.
func (e *Engine) Counts() pgmodel.Counts {
	e.wmu.Lock()
	defer e.wmu.Unlock()
	return e.counts
}

// LastSeq implements [engine.Engine]. It reads the token from the database, so an
// engine that was opened over an existing schema has it.
func (e *Engine) LastSeq() uint64 {
	var seq int64
	if err := e.pool.QueryRow(e.ctx, "SELECT last_seq FROM meta").Scan(&seq); err != nil {
		// The contract gives LastSeq no error. A database that cannot answer has no
		// token, and a caller that then writes finds out from Write.
		return 0
	}
	return seqFromDB(seq)
}

// Size implements [engine.Engine]: the bytes of every table and index of the
// schema, toast included, as the server reports them.
func (e *Engine) Size() (int64, error) {
	var n int64
	err := e.pool.QueryRow(e.ctx, `SELECT coalesce(sum(pg_total_relation_size(c.oid)), 0)::bigint
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = $1 AND c.relkind = 'r'`, e.schema).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("pgengine: size: %w", err)
	}
	return n, nil
}

// DropSchema removes the schema and everything in it, which Close leaves alone. The
// extensions, which live in public, stay.
func DropSchema(ctx context.Context, dsn, schema string) error {
	if !schemaName.MatchString(schema) {
		return fmt.Errorf("pgengine: %q is not a usable schema name: %w", schema, engine.ErrInvalid)
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return fmt.Errorf("pgengine: %w", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	if _, err := conn.Exec(ctx, "DROP SCHEMA IF EXISTS "+pgx.Identifier{schema}.Sanitize()+" CASCADE", pgx.QueryExecModeSimpleProtocol); err != nil {
		return fmt.Errorf("pgengine: dropping schema %s: %w", schema, err)
	}
	return nil
}

// A sequence number is stored as a bigint with its top bit flipped: the ingest sequence
// is a uint64, and the conformance workloads cross 2^63, which a bigint cannot hold. The
// flip maps the order of uint64 to the order of int64, so a comparison with a stored
// number (seq <= token) is a comparison of bigints, and the largest token, which sees
// every record, is the largest bigint. A sequence number read with psql is therefore
// shifted by 2^63.

// seqToDB is the bigint a sequence number or a snapshot token is stored as.
func seqToDB(seq uint64) int64 { return int64(seq ^ (1 << 63)) }

// seqFromDB is the inverse of seqToDB.
func seqFromDB(v int64) uint64 { return uint64(v) ^ (1 << 63) }
