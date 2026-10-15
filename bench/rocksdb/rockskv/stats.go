package rockskv

import (
	"fmt"
	"maps"
	"strconv"
	"strings"
)

// Snapshot is a reading of the database's statistics, as named counters: the names
// the runner reads off the bench module's Pebble snapshot, each from its RocksDB
// counterpart.
//
//   - live_table_bytes: rocksdb.live-sst-files-size;
//   - tables_l0 to tables_l6: rocksdb.num-files-at-level<N>;
//   - cache_misses, cache_hits: RocksDB's statistics rocksdb.block.cache.miss and
//     .hit, since the database was opened;
//   - cache_size: rocksdb.block-cache-usage (with the memtables' charge, when the
//     database is written to);
//   - compaction_debt: rocksdb.estimate-pending-compaction-bytes;
//   - memtable_count: rocksdb.num-immutable-mem-table, plus the mutable one;
//     memtable_bytes: rocksdb.cur-size-all-mem-tables;
//   - bytes_in, bytes_flushed, bytes_compacted: the statistics
//     rocksdb.bytes.written, rocksdb.flush.write.bytes and
//     rocksdb.compact.write.bytes;
//   - compactions: the count of RocksDB's histogram of files per compaction
//     (rocksdb.numfiles.in.singlecompaction), one per compaction;
//   - compactions_in_progress, flushes_in_progress: rocksdb.num-running-compactions
//     and rocksdb.num-running-flushes;
//   - write_stalls: the delays and stops RocksDB began (total-delays and
//     total-stops of rocksdb.cf-write-stall-stats, and the write buffer manager's
//     stops of rocksdb.db-write-stall-stats);
//   - tombstones: the point and range deletions in the tables
//     (rocksdb.aggregated-table-properties), absent when it cannot be read.
//
// Pebble's pending_stats_tables and stats_complete have no counterpart and are
// absent: RocksDB keeps no queue of tables whose statistics are still to be read.
type Snapshot map[string]int64

// Flat is the snapshot as named counters.
func (s Snapshot) Flat() map[string]int64 { return maps.Clone(s) }

// Snapshot reads the database's statistics.
func (k *KV) Snapshot() Snapshot {
	s := Snapshot{}
	intProp := func(name, prop string) {
		if v, ok := k.db.GetIntProperty(prop); ok {
			s[name] = int64(v)
		}
	}
	intProp("live_table_bytes", "rocksdb.live-sst-files-size")
	intProp("cache_size", "rocksdb.block-cache-usage")
	intProp("compaction_debt", "rocksdb.estimate-pending-compaction-bytes")
	intProp("memtable_bytes", "rocksdb.cur-size-all-mem-tables")
	intProp("compactions_in_progress", "rocksdb.num-running-compactions")
	intProp("flushes_in_progress", "rocksdb.num-running-flushes")
	if v, ok := k.db.GetIntProperty("rocksdb.num-immutable-mem-table"); ok {
		s["memtable_count"] = int64(v) + 1
	}
	for l := range numLevels {
		if v, err := strconv.ParseInt(strings.TrimSpace(k.db.GetProperty(fmt.Sprintf("rocksdb.num-files-at-level%d", l))), 10, 64); err == nil {
			s[fmt.Sprintf("tables_l%d", l)] = v
		}
	}
	st := parseStatistics(k.opts.GetStatisticsString())
	for name, stat := range map[string]string{
		"cache_misses": "rocksdb.block.cache.miss", "cache_hits": "rocksdb.block.cache.hit",
		"bytes_in": "rocksdb.bytes.written", "bytes_flushed": "rocksdb.flush.write.bytes",
		"bytes_compacted": "rocksdb.compact.write.bytes", "compactions": "rocksdb.numfiles.in.singlecompaction",
	} {
		s[name] = st[stat]
	}
	stalls := parseFields(k.db.GetProperty("rocksdb.cf-write-stall-stats"), ",", ":")
	dbStalls := parseFields(k.db.GetProperty("rocksdb.db-write-stall-stats"), ",", ":")
	s["write_stalls"] = stalls["total-delays"] + stalls["total-stops"] + dbStalls["write-buffer-manager-limit-stops"]
	props := parseFields(k.db.GetProperty("rocksdb.aggregated-table-properties"), ";", "=")
	if d, ok := props["# deletions"]; ok {
		s["tombstones"] = d + props["# range deletions"]
	}
	return s
}

// parseStatistics reads RocksDB's statistics text: a ticker's line is "<name> COUNT
// : <n>", a histogram's "<name> P50 : ... COUNT : <n> SUM : <s>"; both give the
// count.
func parseStatistics(text string) map[string]int64 {
	out := map[string]int64{}
	for line := range strings.SplitSeq(text, "\n") {
		f := strings.Fields(line)
		for i := 1; i+2 < len(f); i++ {
			if f[i] == "COUNT" && f[i+1] == ":" {
				if n, err := strconv.ParseInt(f[i+2], 10, 64); err == nil {
					out[f[0]] = n
				}
				break
			}
		}
	}
	return out
}

// parseFields reads "name<kv>value<sep>name<kv>value..." into the values that are
// integers, after a "Write Stall (count):" lead if there is one.
func parseFields(text, sep, kv string) map[string]int64 {
	if _, rest, ok := strings.Cut(text, "(count):"); ok {
		text = rest
	}
	out := map[string]int64{}
	for field := range strings.SplitSeq(text, sep) {
		name, value, ok := strings.Cut(field, kv)
		if !ok {
			continue
		}
		if n, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64); err == nil {
			out[strings.TrimSpace(name)] = n
		}
	}
	return out
}
