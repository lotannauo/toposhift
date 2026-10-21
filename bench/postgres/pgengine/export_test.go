package pgengine

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
