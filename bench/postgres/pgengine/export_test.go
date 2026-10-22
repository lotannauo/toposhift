package pgengine

import (
	"context"
	"fmt"
	"runtime"

	"github.com/jackc/pgx/v5"

	"github.com/lotannauo/toposhift/bench/postgres/pgmodel"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
)

// The writer's statements, for the tests that explain them.
const (
	EdgeRowsSQL    = edgeRows
	EntityRowsSQL  = entityRows
	EdgeInputSQL   = edgeInput
	EntityInputSQL = entityInput
)

// SeqToDB is the stored form of a sequence number.
func SeqToDB(seq uint64) int64 { return seqToDB(seq) }

// SeqFromDB is the inverse of SeqToDB.
func SeqFromDB(v int64) uint64 { return seqFromDB(v) }

// ObserveStatements sets what is called with each statement a batch runs, until the
// returned function is called.
func ObserveStatements(f func(ctx context.Context, tx pgx.Tx, sql string, args []any)) (stop func()) {
	observeStatement.Store(&f)
	return func() { observeStatement.Store(nil) }
}

// FoldMemory folds the log of a loader that has been given a stream, as Finish does, and
// returns the interval rows it holds and the heap that holds them, measured after a
// collection with the rows still live.
func FoldMemory(ctx context.Context, l *Loader) (rows int, bytes int64, err error) {
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	edges, entities, used, _, err := l.fold(ctx, false)
	if err != nil {
		return 0, 0, err
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	rows = len(edges) + len(entities)
	runtime.KeepAlive(edges)
	runtime.KeepAlive(entities)
	runtime.KeepAlive(used)
	return rows, int64(after.HeapAlloc) - int64(before.HeapAlloc), nil
}

// CheckPlaces says whether the place the writer remembers for the last row of each subject
// is where that row is, and the row there is the one it remembers.
func (e *Engine) CheckPlaces(ctx context.Context) error {
	e.wmu.Lock()
	defer e.wmu.Unlock()
	var bad error
	e.w.EachLast(func(s engine.Subject, _ catalog.Layer, last pgmodel.Stored) {
		if bad != nil {
			return
		}
		var start int64
		var end, deadline *int64
		var inf bool
		var source int16
		var err error
		if s.Kind == engine.SubjectEdge {
			err = e.pool.QueryRow(ctx, `SELECT start_ns, upper(during), upper_inf(during), deadline_ns, end_source FROM edges
WHERE ctid = $1 AND src = $2 AND dst = $3 AND rel = $4`, tidOf(last.Loc), pgmodel.EncodeFingerprint(s.A), pgmodel.EncodeFingerprint(s.B), string(s.Relation)).
				Scan(&start, &end, &inf, &deadline, &source)
		} else {
			err = e.pool.QueryRow(ctx, `SELECT start_ns, upper(during), upper_inf(during), deadline_ns, end_source FROM entities
WHERE ctid = $1 AND fp = $2`, tidOf(last.Loc), pgmodel.EncodeFingerprint(s.A)).Scan(&start, &end, &inf, &deadline, &source)
		}
		if err != nil {
			bad = fmt.Errorf("the writer remembers the last row of %s at %v, and no row of it is there: %w", s.A, last.Loc, err)
			return
		}
		row, err := dumpRow(start, end, inf, deadline, source)
		if err != nil {
			bad = err
			return
		}
		if row != last.Row {
			bad = fmt.Errorf("the writer remembers %+v at %v for %s; the row there is %+v", last.Row, last.Loc, s.A, row)
		}
	})
	return bad
}
