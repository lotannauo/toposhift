package pebblekv

import (
	"sync"

	"github.com/cockroachdb/pebble/v2"
)

// Recorder receives the counters and samples a measurement collects. A nil
// Recorder is allowed where this package takes one, and records nothing.
type Recorder interface {
	// Count adds n to the named counter.
	Count(name string, n int64)
	// Sample records one observation of the named quantity, for a distribution.
	Sample(name string, v int64)
}

// iterNames are the names one kind of read records under.
type iterNames struct {
	reads, blockBytes, blockBytesCached, blockReadNs                 string
	points, keyBytes, valueBytes                                     string
	seeks, steps, internalSeeks, internalSteps                       string
	coveredByTombstones, separatedValues, separatedValueBytesFetched string
}

var iterNameCache sync.Map // op -> *iterNames

func namesFor(op string) *iterNames {
	if n, ok := iterNameCache.Load(op); ok {
		return n.(*iterNames)
	}
	p := "read." + op + "."
	n := &iterNames{
		reads: p + "reads", blockBytes: p + "block_bytes", blockBytesCached: p + "block_bytes_cached", blockReadNs: p + "block_read_ns",
		points: p + "points", keyBytes: p + "key_bytes", valueBytes: p + "value_bytes",
		seeks: p + "seeks", steps: p + "steps", internalSeeks: p + "internal_seeks", internalSteps: p + "internal_steps",
		coveredByTombstones: p + "covered_by_tombstones", separatedValues: p + "separated_values",
		separatedValueBytesFetched: p + "separated_value_bytes_fetched",
	}
	got, _ := iterNameCache.LoadOrStore(op, n)
	return got.(*iterNames)
}

// RecordIter reports what one read did to the recorder, from the statistics
// Pebble keeps on the iterator, under names that start "read.<op>.": the count of
// reads, and for each a sample of
//
//   - block_bytes, block_bytes_cached, block_read_ns: bytes of blocks the read
//     loaded (compressed, index and data blocks), how many of them were in the
//     block cache, and the time spent fetching the others;
//   - points, key_bytes, value_bytes: the points the read iterated over and their
//     sizes (a point is iterated more than once if the read goes back to it);
//   - seeks, steps: calls the read made to the iterator, SeekGE, SeekPrefixGE and
//     First as seeks, Next as steps, and internal_seeks, internal_steps for the
//     calls the iterator made on its own inner iterators in turn, which is what
//     the reading really cost;
//   - covered_by_tombstones: points iterated over that a range tombstone covered;
//   - separated_values, separated_value_bytes_fetched: points whose value lives in
//     a value block, and the bytes fetched from them.
//
// These are in one unit for every layout, which a layout's own counters
// (versions or records stepped over) are not: they count what each layout means
// by a step. Call it once per read, before closing the iterator. A nil recorder
// does nothing.
func RecordIter(rec Recorder, op string, it *pebble.Iterator) {
	if rec == nil {
		return
	}
	s := it.Stats()
	n := namesFor(op)
	in := s.InternalStats
	rec.Count(n.reads, 1)
	rec.Sample(n.blockBytes, int64(in.BlockBytes))
	rec.Sample(n.blockBytesCached, int64(in.BlockBytesInCache))
	rec.Sample(n.blockReadNs, int64(in.BlockReadDuration))
	rec.Sample(n.points, int64(in.PointCount))
	rec.Sample(n.keyBytes, int64(in.KeyBytes))
	rec.Sample(n.valueBytes, int64(in.ValueBytes))
	rec.Sample(n.seeks, int64(s.ForwardSeekCount[pebble.InterfaceCall]+s.ReverseSeekCount[pebble.InterfaceCall]))
	rec.Sample(n.steps, int64(s.ForwardStepCount[pebble.InterfaceCall]+s.ReverseStepCount[pebble.InterfaceCall]))
	rec.Sample(n.internalSeeks, int64(s.ForwardSeekCount[pebble.InternalIterCall]+s.ReverseSeekCount[pebble.InternalIterCall]))
	rec.Sample(n.internalSteps, int64(s.ForwardStepCount[pebble.InternalIterCall]+s.ReverseStepCount[pebble.InternalIterCall]))
	rec.Sample(n.coveredByTombstones, int64(in.PointsCoveredByRangeTombstones))
	rec.Sample(n.separatedValues, int64(in.SeparatedPointValue.Count))
	rec.Sample(n.separatedValueBytesFetched, int64(in.SeparatedPointValue.ValueBytesFetched))
}
