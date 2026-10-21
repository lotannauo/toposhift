package pgengine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5"
)

// Statement is a statement a read ran and the arguments it ran with.
type Statement struct {
	SQL  string
	Args []any
}

// Recorder is a [pgx.QueryTracer] that keeps the statements run while it is recording,
// in the order they ran: handed to an engine as its Config.Tracer, it lets a harness
// explain exactly the statements a read runs, with the arguments it ran them with,
// rather than statements written again beside the engine's own. Transaction control
// (begin, commit, rollback) is not a statement of the read and is not kept.
type Recorder struct {
	mu    sync.Mutex
	on    bool
	stmts []Statement
}

var _ pgx.QueryTracer = (*Recorder)(nil)

// Start begins a recording, forgetting any before it.
func (r *Recorder) Start() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.on, r.stmts = true, nil
}

// Stop ends the recording and returns the statements it kept.
func (r *Recorder) Stop() []Statement {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.on = false
	out := r.stmts
	r.stmts = nil
	return out
}

// TraceQueryStart implements [pgx.QueryTracer].
func (r *Recorder) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.on || transactionControl(data.SQL) {
		return ctx
	}
	r.stmts = append(r.stmts, Statement{SQL: data.SQL, Args: append([]any(nil), data.Args...)})
	return ctx
}

// TraceQueryEnd implements [pgx.QueryTracer].
func (r *Recorder) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func transactionControl(sql string) bool {
	s := strings.ToLower(strings.TrimSpace(sql))
	for _, p := range []string{"begin", "commit", "rollback", "start transaction", "end"} {
		if s == p || strings.HasPrefix(s, p+" ") {
			return true
		}
	}
	return false
}

// ErrNotCold is wrapped by the error [ColdExplain] returns when the buffers of the
// schema could not be emptied, or a cold read found nothing to read from storage: a
// counter taken from a read that was not cold is not a cold counter.
var ErrNotCold = errors.New("pgengine: the read was not cold")

// The relations whose buffers a cold read must find empty: every table and index of
// the schema, the toast tables of its tables and their indexes. A buffer is of a
// relation's file, so each is named by its filenode in this database.
const schemaFilenodes = `WITH rels AS (
  SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = $1 AND c.relkind IN ('r', 'i', 'm', 't')
  UNION
  SELECT c.reltoastrelid FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = $1 AND c.reltoastrelid <> 0
  UNION
  SELECT i.indexrelid FROM pg_index i JOIN pg_class c ON c.reltoastrelid = i.indrelid JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = $1
)
SELECT pg_relation_filenode(oid) FROM rels WHERE pg_relation_filenode(oid) IS NOT NULL`

const (
	evictBuffers = `SELECT count(*) FILTER (WHERE pg_buffercache_evict(b.bufferid))
FROM pg_buffercache b
WHERE b.reldatabase = (SELECT oid FROM pg_database WHERE datname = current_database())
  AND b.relfilenode IN (` + schemaFilenodes + `)`
	remainingBuffers = `SELECT count(*)
FROM pg_buffercache b
WHERE b.reldatabase = (SELECT oid FROM pg_database WHERE datname = current_database())
  AND b.relfilenode IN (` + schemaFilenodes + `)`
)

// EvictSchema empties the shared buffers of every relation of the schema, one buffer at
// a time (pg_buffercache_evict, which needs a superuser), and returns how many it
// evicted. It then counts the buffers of the schema that are left and refuses if there
// are any: a buffer that is pinned, or that a concurrent session holds, cannot be
// evicted, and a read made beside it is not cold. Dirty buffers are written first; a
// CHECKPOINT before a measured section keeps that from being work of the read's.
func EvictSchema(ctx context.Context, q Querier, schema string) (int, error) {
	var evicted, left int
	if err := q.QueryRow(ctx, evictBuffers, schema).Scan(&evicted); err != nil {
		return 0, fmt.Errorf("pgengine: evicting the buffers of schema %s: %w", schema, err)
	}
	if err := q.QueryRow(ctx, remainingBuffers, schema).Scan(&left); err != nil {
		return 0, fmt.Errorf("pgengine: counting the buffers of schema %s: %w", schema, err)
	}
	if left != 0 {
		return evicted, fmt.Errorf("%w: %d buffers of schema %s remain after eviction", ErrNotCold, left, schema)
	}
	return evicted, nil
}

// Evictor empties the buffers of a schema before a cold read. It exists so that the
// protocol can be shown to refuse a read when it does not.
type Evictor func(ctx context.Context, q Querier, schema string) (int, error)

// ColdRun is a statement explained cold: the counters of its plan, and the server's JSON
// they were read from.
type ColdRun struct {
	Counters Counters
	JSON     []byte
}

// ColdExplain is the cold-read protocol: it runs the statements of one read once, to warm
// the connection's own caches, empties the buffers of the schema and then explains the
// statements in order, in the transaction q is, so that they
// are of one snapshot. The counters of each are the server's own, from the top node of
// its plan. A read that touched nothing in storage (no block read by any of its
// statements) cannot have been cold, whatever the eviction said, and is refused.
func ColdExplain(ctx context.Context, q Querier, schema string, evict Evictor, stmts []Statement) ([]ColdRun, error) {
	if len(stmts) == 0 {
		return nil, errors.New("pgengine: a cold read has no statement")
	}
	if evict == nil {
		evict = EvictSchema
	}
	// The connection's own caches are warmed first: a statement run on a connection that
	// has not run it before reads the catalogs for its relations and indexes, and those
	// buffers are hits in the first run and not in the next. Only the buffers of the
	// schema are emptied, so what is counted is the schema's.
	for _, s := range stmts {
		if _, err := Explain(ctx, q, s.SQL, s.Args...); err != nil {
			return nil, err
		}
	}
	if _, err := evict(ctx, q, schema); err != nil {
		return nil, err
	}
	out := make([]ColdRun, len(stmts))
	var read int64
	for i, s := range stmts {
		raw, c, err := ExplainJSON(ctx, q, s.SQL, s.Args...)
		if err != nil {
			return nil, err
		}
		out[i] = ColdRun{Counters: c, JSON: raw}
		read += c.SharedRead
	}
	if read == 0 {
		return nil, fmt.Errorf("%w: no block of schema %s was read from storage by %d statements", ErrNotCold, schema, len(stmts))
	}
	return out, nil
}
