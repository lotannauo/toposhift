package pebblekv

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// What a measurement reads off a database: how much work each read did, what the
// database holds and what it has done to get there.

// Snapshot is a reading of the state a measurement reports. It is plain numbers,
// so a runner can print it as it is, and two readings can be subtracted.
type Snapshot struct {
	Flushes, Compactions, ReadCompactions int64 // completed since the database was opened

	TablesPerLevel [7]int64 // tables in each level
	BytesPerLevel  [7]int64 // their size
	ReadAmp        int      // sublevels of L0 plus the other levels with tables

	MemtableBytes  uint64
	LiveTableBytes uint64
	// ObsoleteBytes and ZombieBytes are files no longer in the database that are
	// still on disk, or still held open by a reader.
	ObsoleteBytes, ZombieBytes uint64

	TombstoneCount uint64
	// The bytes Pebble estimates deleted keys still hold in tables, which a
	// compaction would reclaim.
	PointTombstoneGarbage, RangeTombstoneGarbage uint64

	CacheHits, CacheMisses int64
	// CacheSize is the bytes the block cache holds (uncompressed, as it holds them).
	CacheSize int64

	// BytesIn is what was written to the database, BytesFlushed and
	// BytesCompacted what Pebble wrote to tables on account of it (summed over
	// the levels), so (BytesFlushed + BytesCompacted) / BytesIn is the write
	// amplification.
	BytesIn, BytesFlushed, BytesCompacted uint64

	MarkedFiles int
	// StatsComplete is false while Pebble is still reading the tables it opened
	// to estimate their garbage; the estimates above are not final until then.
	StatsComplete bool
}

// Snapshot reads the database's metrics.
func (k *KV) Snapshot() Snapshot {
	m := k.Metrics()
	s := Snapshot{
		Flushes: m.Flush.Count, Compactions: m.Compact.Count, ReadCompactions: m.Compact.ReadCount,
		ReadAmp:       m.ReadAmp(),
		MemtableBytes: m.MemTable.Size,
		ObsoleteBytes: m.Table.ObsoleteSize, ZombieBytes: m.Table.ZombieSize,
		LiveTableBytes:        m.Table.Local.LiveSize,
		TombstoneCount:        m.Keys.TombstoneCount,
		PointTombstoneGarbage: m.Table.Garbage.PointDeletionsBytesEstimate,
		RangeTombstoneGarbage: m.Table.Garbage.RangeDeletionsBytesEstimate,
		CacheHits:             m.BlockCache.Hits, CacheMisses: m.BlockCache.Misses, CacheSize: m.BlockCache.Size,
		MarkedFiles:   m.Compact.MarkedFiles,
		StatsComplete: m.Table.InitialStatsCollectionComplete && m.Table.PendingStatsCollectionCount == 0,
	}
	for i := 0; i < len(m.Levels) && i < len(s.TablesPerLevel); i++ {
		s.TablesPerLevel[i] = m.Levels[i].TablesCount
		s.BytesPerLevel[i] = m.Levels[i].TablesSize
	}
	t := m.Total()
	s.BytesIn = t.TableBytesIn
	s.BytesFlushed, s.BytesCompacted = t.TableBytesFlushed, t.TableBytesCompacted
	return s
}

// Flat is the snapshot as named counters, for an engine to hand to a runner
// without the runner knowing Pebble.
func (s Snapshot) Flat() map[string]int64 {
	out := map[string]int64{
		"flushes": s.Flushes, "compactions": s.Compactions, "read_compactions": s.ReadCompactions,
		"read_amp": int64(s.ReadAmp), "memtable_bytes": int64(s.MemtableBytes),
		"live_table_bytes": int64(s.LiveTableBytes), "obsolete_bytes": int64(s.ObsoleteBytes), "zombie_bytes": int64(s.ZombieBytes),
		"tombstones": int64(s.TombstoneCount), "point_tombstone_garbage": int64(s.PointTombstoneGarbage),
		"range_tombstone_garbage": int64(s.RangeTombstoneGarbage),
		"cache_hits":              s.CacheHits, "cache_misses": s.CacheMisses, "cache_size": s.CacheSize,
		"bytes_in": int64(s.BytesIn), "bytes_flushed": int64(s.BytesFlushed), "bytes_compacted": int64(s.BytesCompacted),
		"marked_files": int64(s.MarkedFiles),
	}
	for i := range s.TablesPerLevel {
		out[fmt.Sprintf("tables_l%d", i)] = s.TablesPerLevel[i]
		out[fmt.Sprintf("bytes_l%d", i)] = s.BytesPerLevel[i]
	}
	if s.StatsComplete {
		out["stats_complete"] = 1
	}
	return out
}

// Quiesce waits until the database is at rest, which is what a size or a read
// has to be measured against: nothing flushing or compacting, the estimates of
// every table's garbage collected, no file marked for a compaction that is
// coming (unless automatic compactions are off, when none is), and all of that
// unchanged for a whole second. Settle is the quick version for a test.
//
// It returns when ctx is done with its error, so the caller sets the patience:
// minutes are right for a gigabyte.
func (k *KV) Quiesce(ctx context.Context) error {
	if k.cfg.ReadOnly {
		return ctx.Err() // nothing runs in a read-only database, and nothing is left in memory
	}
	if err := k.Flush(); err != nil {
		return err
	}
	const (
		poll   = 20 * time.Millisecond
		stable = time.Second
	)
	var last Snapshot
	var since time.Time
	for {
		s := k.Snapshot()
		m := k.Metrics()
		busy := m.Compact.NumInProgress != 0 || m.Flush.NumInProgress != 0 || !s.StatsComplete ||
			(!k.cfg.DisableAutoCompactions && s.MarkedFiles != 0)
		if busy || s.Flushes != last.Flushes || s.Compactions != last.Compactions || s.LiveTableBytes != last.LiveTableBytes {
			since = time.Time{}
		} else if since.IsZero() {
			since = time.Now()
		} else if time.Since(since) >= stable {
			return nil
		}
		last = s
		select {
		case <-ctx.Done():
			return fmt.Errorf("pebblekv: not at rest: %w", ctx.Err())
		case <-time.After(poll):
		}
	}
}

// CompactAll flushes the memtable and compacts everything the database holds
// into as few tables as it will, then waits for rest. It is what makes the size
// of a database repeatable: how many tables and levels the data lies in
// otherwise depends on when compactions happened to run.
//
// It compacts the span of the tables, tombstones included, and not the span of
// the keys that are alive: an iterator does not show a deleted key, so the tombstones
// of keys deleted before the first live key or after the last would be left where a
// background compaction happened not to have reached them. And it goes on until a
// round changes nothing, because a background compaction that was running when one
// round started can leave work for the next.
func (k *KV) CompactAll(ctx context.Context) error {
	if err := k.Flush(); err != nil {
		return err
	}
	var last Snapshot
	for round := range maxCompactRounds {
		lo, hi, err := k.tableSpan()
		if err != nil {
			return err
		}
		if lo != nil && k.cmp(lo, hi) < 0 { // one key needs no compacting
			if err := k.Compact(ctx, lo, hi, true); err != nil {
				return fmt.Errorf("pebblekv: compacting: %w", err)
			}
		}
		if err := k.Quiesce(ctx); err != nil {
			return err
		}
		s := k.Snapshot()
		if s.TombstoneCount == 0 { // nothing left for another round to drop
			return nil
		}
		if round > 0 && s.TombstoneCount == last.TombstoneCount && s.LiveTableBytes == last.LiveTableBytes && s.TablesPerLevel == last.TablesPerLevel {
			return nil
		}
		last = s
	}
	return nil
}

// maxCompactRounds bounds how often CompactAll compacts again.
const maxCompactRounds = 6

// tableSpan is the smallest and largest user key of any table, deletions
// included, or nil if there is none. The largest key of a table of range
// deletions is where the last one ends, which is a key the comparer accepts.
func (k *KV) tableSpan() (lo, hi []byte, err error) {
	levels, err := k.SSTables()
	if err != nil {
		return nil, nil, err
	}
	for _, level := range levels {
		for _, t := range level {
			if s := t.Smallest.UserKey; lo == nil || k.cmp(s, lo) < 0 {
				lo = append([]byte(nil), s...)
			}
			if l := t.Largest.UserKey; hi == nil || k.cmp(l, hi) > 0 {
				hi = append([]byte(nil), l...)
			}
		}
	}
	return lo, hi, nil
}

// CloseClean flushes the memtable and closes the database, so that opening it
// again recovers nothing from the log. Closing without it leaves the memtable's
// contents in the log, and opening replays them and writes them to a table of its
// own at that moment (see [KV.RecoveredBytes]): the data ends up in tables either
// way, but a built database that a measurement opens should not do any work, or
// have tables of a different shape from the ones its own flushes made, at open.
func (k *KV) CloseClean() error {
	if err := k.Flush(); err != nil {
		_ = k.Close()
		return err
	}
	return k.Close()
}

// RecoveredBytes is how many bytes of tables opening this database wrote from
// its log. It is zero for a database that was closed with [KV.CloseClean] (or
// flushed and then closed), and a measurement that opens a built database
// refuses one that is not zero.
func (k *KV) RecoveredBytes() uint64 { return k.recovered }

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
// These are in one unit for every layout, which the layouts' own counters
// (versions or records stepped over) are not: they count what each layout means
// by a step. Call it once per read, before closing the iterator.
func RecordIter(rec engine.Recorder, op string, it *pebble.Iterator) {
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

// The parts a layout's logical bytes are divided into by [engine.Breakdowner].
// They partition the bytes of every data key and value (the meta keys are not
// counted): a stored record's bytes are in the part of its kind, except its
// payload, which is in the part of the direction it was stored in, since an edge
// is stored once from each end and the payload goes with both.
const (
	PartObserve        = "observe"   // a record that asserts, not a run extension
	PartExtension      = "extension" // a record that re-asserts a run with a later Through
	PartDelete         = "delete"
	PartPayloadForward = "payload forward"
	PartPayloadReverse = "payload reverse"
	PartPayloadEntity  = "payload entity"
	PartCheckpoint     = "checkpoint"
	PartBaseline       = "baseline"
)

// RecordParts splits the total logical bytes of one stored record, its key and
// its value, into the bytes of its kind and the bytes of its payload, and names
// the part each belongs to. dir is the direction byte of the prefix it is stored
// under: 0 for an entity's existence, otherwise an [engine.Direction].
func RecordParts(v Value, dir byte, total int) (kind string, kindBytes int, payload string, payloadBytes int) {
	switch {
	case v.Kind == lifecycle.Delete:
		kind = PartDelete
	case v.HasThrough:
		kind = PartExtension
	default:
		kind = PartObserve
	}
	switch engine.Direction(dir) {
	case engine.Forward:
		payload = PartPayloadForward
	case engine.Reverse:
		payload = PartPayloadReverse
	default:
		payload = PartPayloadEntity
	}
	return kind, total - len(v.Payload), payload, len(v.Payload)
}

// Describe says how the database is set up, in what a measurement should record
// next to its numbers: the settings it was opened with and, read from its tables,
// what they were really written with (the key schema, and the block-property
// collectors that ran). The second half is empty until a table exists, and is
// what shows a variant is in effect and not only asked for.
func (k *KV) Describe() (map[string]string, error) {
	t := k.cfg.Tuning
	out := map[string]string{
		"comparer":                k.layoutName,
		"block_bytes":             fmt.Sprint(t.BlockSize),
		"memtable_bytes":          fmt.Sprint(t.MemTableSize),
		"target_file_bytes":       fmt.Sprint(t.TargetFileSize),
		"l_base_max_bytes":        fmt.Sprint(t.LBaseMaxBytes),
		"l0_compaction_threshold": fmt.Sprint(t.L0CompactionThreshold),
		"cache_bytes":             fmt.Sprint(t.CacheBytes),
		"sync":                    fmt.Sprint(k.cfg.Sync),
		"auto_compactions":        fmt.Sprint(!k.cfg.DisableAutoCompactions),
		"read_only":               fmt.Sprint(k.cfg.ReadOnly),
		"read_compactions":        fmt.Sprint(!k.cfg.DisableReadCompactions),
		"time_filter_asked":       fmt.Sprint(k.cfg.TimeFilter),
		"recovered_bytes":         fmt.Sprint(k.recovered),
		"pebble_options":          k.options,
	}
	props, err := k.TableProperties()
	if err != nil {
		return nil, err
	}
	schemas, collectors := map[string]bool{}, map[string]bool{}
	for _, p := range props {
		schemas[p.KeySchemaName] = true
		for name := range p.UserProperties {
			collectors[name] = true
		}
	}
	out["key_schema_in_tables"] = joinSorted(schemas)
	out["collectors_in_tables"] = joinSorted(collectors)
	return out, nil
}

func joinSorted(set map[string]bool) string {
	if len(set) == 0 {
		return "none"
	}
	names := make([]string, 0, len(set))
	for n := range set {
		names = append(names, n)
	}
	slices.Sort(names)
	return strings.Join(names, ",")
}
