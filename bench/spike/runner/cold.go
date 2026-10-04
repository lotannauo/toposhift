package runner

import (
	"runtime"
	"runtime/debug"
	"strings"
)

// The counters of a read that do not depend on what an earlier read left in the
// block cache or in the allocator, named as the decision rules name them.
//
//   - block_loads is how many blocks the read had to bring in, and cold_block_bytes
//     their compressed size, with the block cache empty and the tables open: the
//     distinct blocks the read needs (a block read again after it was loaded is
//     served from the cache, so it counts once, which the gross block bytes of the
//     iterator do not do), the I/O a first read of that question costs. Blocks of
//     the index, the filters and the values are in the loads; the bytes are those of
//     index, filter and data blocks, and the bytes fetched from value blocks are
//     counted apart (separated_value_bytes_fetched). cold_cache_bytes is what the
//     cache holds afterwards, uncompressed and complete, a diagnostic that tells
//     whether the cache was large enough for the read not to load a block twice.
//   - allocs and alloc_bytes are the allocations of one more of the same read once
//     the pools are full, with the collector off, and include what the runner does
//     with the answer, which is the same for every candidate that gives the same
//     answer.
const (
	CounterBlockLoads     = "block_loads"
	CounterColdBlockBytes = "cold_block_bytes"
	CounterColdCacheBytes = "cold_cache_bytes"
	CounterAllocs         = "allocs"
	CounterAllocBytes     = "alloc_bytes"
)

// coldAsk puts the query to an engine whose block cache has just been emptied and
// reports what the read had to load.
func coldAsk(e measurable, rec *Capture, q Query) (Answer, map[string]int64, error) {
	e.ColdStart()
	before := e.Stats()
	rec.Take()
	a, err := Ask(e, q)
	if err != nil {
		return Answer{}, nil, err
	}
	after := e.Stats()
	c := rec.Take()
	return a, map[string]int64{
		CounterBlockLoads:     after["cache_misses"] - before["cache_misses"],
		CounterColdBlockBytes: sumSuffix(c, ".block_bytes") - sumSuffix(c, ".block_bytes_cached"),
		CounterColdCacheBytes: after["cache_size"],
	}, nil
}

// sumSuffix adds the counters whose name ends in suffix.
func sumSuffix(m map[string]int64, suffix string) int64 {
	var n int64
	for k, v := range m {
		if strings.HasSuffix(k, suffix) {
			n += v
		}
	}
	return n
}

// allocReps is how many times a read is counted for its allocations.
const allocReps = 3

// allocAsk reports the allocations of a read. The read is made twice to fill the
// pools and the caches it uses, and then counted allocReps times with the
// collector off, so that a collection in the middle cannot empty a pool and make
// the count depend on when it ran.
//
// The count is not exactly the same each time. A read of a few objects can
// allocate several more than the time before (a sync.Pool is kept per processor,
// and a read that moves to another one misses it), so the smallest of the counts
// is reported, and nothing is held to be steady: the counters the rules decide on are above a floor that this noise does
// not reach, and are means over many reads.
func allocAsk(e measurable, rec *Capture, q Query) (map[string]int64, error) {
	runtime.GC()
	defer debug.SetGCPercent(debug.SetGCPercent(-1))
	for range 2 {
		if _, err := Ask(e, q); err != nil {
			return nil, err
		}
	}
	var allocs, bytes int64
	for rep := range allocReps {
		rec.Take()
		var m0, m1 runtime.MemStats
		runtime.ReadMemStats(&m0)
		if _, err := Ask(e, q); err != nil {
			return nil, err
		}
		runtime.ReadMemStats(&m1)
		n, b := int64(m1.Mallocs-m0.Mallocs), int64(m1.TotalAlloc-m0.TotalAlloc)
		if rep == 0 || n < allocs {
			allocs = n
		}
		if rep == 0 || b < bytes {
			bytes = b
		}
	}
	rec.Take()
	return map[string]int64{CounterAllocs: allocs, CounterAllocBytes: bytes}, nil
}

// sameValues is whether two maps hold the same names and values.
func sameValues(a, b map[string]int64) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || w != v {
			return false
		}
	}
	return true
}
