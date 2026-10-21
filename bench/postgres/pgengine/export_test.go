package pgengine

import (
	"context"
	"runtime"

	"github.com/jackc/pgx/v5"
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
