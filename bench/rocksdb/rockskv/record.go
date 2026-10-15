package rockskv

import (
	"sync"

	"github.com/lotannauo/toposhift/bench/spike/engine"
)

// The perf context metrics a read with [Config.Perf] reports, by their ids in
// RocksDB's C API (rocksdb/c.h, the enum that starts at
// rocksdb_user_key_comparison_count). The ids are the same in RocksDB 11.1.2 and
// 11.8.1; a test holds them to the header the package is compiled against.
const (
	perfBlockCacheHitCount    = 1
	perfBlockReadCount        = 2
	perfBlockReadByte         = 3
	perfInternalKeySkippedCnt = 10
)

// perfMetrics are the perf context metrics reported, with the names they are
// reported under.
var perfMetrics = []struct {
	id   int
	name string
}{
	{perfInternalKeySkippedCnt, "internal_key_skipped"},
	{perfBlockReadCount, "block_read_count"},
	{perfBlockReadByte, "block_read_byte"},
	{perfBlockCacheHitCount, "block_cache_hit_count"},
}

// readNames are the names one kind of read records under.
type readNames struct {
	reads, seeks, steps string
	perf                []string // parallel to perfMetrics
}

var readNameCache sync.Map // op -> *readNames

func namesFor(op string) *readNames {
	if n, ok := readNameCache.Load(op); ok {
		return n.(*readNames)
	}
	p := "read." + op + "."
	n := &readNames{reads: p + "reads", seeks: p + "seeks", steps: p + "steps"}
	for _, m := range perfMetrics {
		n.perf = append(n.perf, p+"rocks."+m.name)
	}
	got, _ := readNameCache.LoadOrStore(op, n)
	return got.(*readNames)
}

// RecordIter reports what one read did, under names that start "read.<op>.", as
// the bench module's RecordIter does for Pebble: a count of reads, and a sample of
// the seeks and steps the read made of its iterator, counted as Pebble counts the
// calls made to its own (First and SeekGE as seeks, Next as steps). Those are the
// counters both engines have; what Pebble counts inside its iterator (its internal
// steps, the blocks it loaded) has no counterpart here.
//
// A read made with the perf context ([Config.Perf], [KV.NewReadIter]) also samples
// RocksDB's own counts under "read.<op>.rocks.": internal_key_skipped,
// block_read_count, block_read_byte and block_cache_hit_count. They are
// diagnostics, in RocksDB's units, and are never compared with Pebble's.
//
// Call it once per read, before closing the iterator. It panics on a nil
// recorder, as the bench module's does.
func RecordIter(rec engine.Recorder, op string, it *Iterator) {
	if rec == nil {
		panic("rockskv: RecordIter needs a recorder")
	}
	n := namesFor(op)
	rec.Count(n.reads, 1)
	rec.Sample(n.seeks, it.seeks)
	rec.Sample(n.steps, it.steps)
	if it.perf != nil {
		for j, m := range perfMetrics {
			rec.Sample(n.perf[j], int64(it.perf.Metric(m.id)))
		}
	}
}
