package pgengine

import (
	"context"

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
