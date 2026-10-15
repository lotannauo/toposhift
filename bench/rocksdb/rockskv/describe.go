package rockskv

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
)

// The descriptions a runner reads as the bench module's Pebble database gives them,
// and the ones only RL has. [KV.Describe] gives both.
const (
	// ComparerName is the name of RocksDB's default comparator, which orders keys by
	// their bytes as Pebble's default comparer does.
	ComparerName = "leveldb.BytewiseComparator"
	// grocksdbPath is the module path of the Go binding.
	grocksdbPath = "github.com/linxGnu/grocksdb"
)

// Describe says how the database is set up, in what a measurement records next to
// its numbers. The keys the runner reads of a Pebble database have the same names
// and meanings here (comparer, block_bytes, memtable_bytes, target_file_bytes,
// l_base_max_bytes, l0_compaction_threshold, cache_bytes, sync, auto_compactions,
// read_only, time_filter_asked, settle_tombstones, settle_deadline,
// key_schema_in_tables and collectors_in_tables: the tables of a bytewise RocksDB
// database record neither); read_compactions is "none", since RocksDB has none, and
// recovered_bytes is absent (recovered_bytes_note says why). The rest is RL's own:
//
//   - engine, tuning, statistics_level, perf_level;
//   - rocksdb_version, from the options file the database last wrote, and
//     rocksdb_options, that file's whole text (the options of the last writable
//     opening: a read-only opening writes none);
//   - grocksdb_version, cgo_cflags, cgo_ldflags, cc_version and rocksdb_portable
//     (the environment's ROCKSDB_PORTABLE, "unknown" when it is not set): what the
//     binary and its native library were built with;
//   - compression_per_level, index_type, metadata_block_bytes,
//     level0_stop_writes_trigger, target_file_size_base, target_file_multiplier,
//     target_file_bytes_by_level_from_base (the target size of the files of each
//     level from the base level down), max_background_jobs, block_cache_capacity,
//     memtables_charged_to_cache;
//   - what RocksDB does that Pebble does not, recorded and not mirrored:
//     memtable_delay_at (the unflushed memtables at which it delays writes),
//     max_open_files and pin_top_level_index (whether the top level of a table's
//     index stays pinned in the cache, through a cold start);
//   - cpu_model.
func (k *KV) Describe() (map[string]string, error) {
	cfg := k.cfg
	t := cfg.Tuning
	deadline := cfg.SettleDeadline
	if deadline == 0 {
		deadline = DefaultSettleDeadline
	}
	perf := "disable"
	if cfg.Perf {
		perf = "enable_count"
	}
	out := map[string]string{
		"comparer":                ComparerName,
		"block_bytes":             fmt.Sprint(t.BlockSize),
		"memtable_bytes":          fmt.Sprint(t.MemTableSize),
		"target_file_bytes":       fmt.Sprint(t.TargetFileSize),
		"l_base_max_bytes":        fmt.Sprint(t.LBaseMaxBytes),
		"l0_compaction_threshold": fmt.Sprint(t.L0CompactionThreshold),
		"cache_bytes":             fmt.Sprint(t.CacheBytes),
		"sync":                    fmt.Sprint(cfg.Sync),
		"auto_compactions":        fmt.Sprint(!cfg.DisableAutoCompactions),
		"read_only":               fmt.Sprint(cfg.ReadOnly),
		"read_compactions":        "none",
		"time_filter_asked":       "false",
		"settle_tombstones":       fmt.Sprint(cfg.SettleRetention),
		"settle_deadline":         deadline.String(),
		"key_schema_in_tables":    "none",
		"collectors_in_tables":    "none",
		"recovered_bytes_note":    "not measured: RocksDB's recovery of its log at opening is not counted",

		"engine":                               "rocksdb",
		"tuning":                               k.tuning,
		"statistics_level":                     "except_timers",
		"perf_level":                           perf,
		"compression_per_level":                strings.Repeat("snappy,", numLevels-1) + "snappy",
		"index_type":                           "two_level_index_search",
		"metadata_block_bytes":                 fmt.Sprint(t.BlockSize),
		"level0_stop_writes_trigger":           fmt.Sprint(l0StopWrites),
		"target_file_size_base":                fmt.Sprint(2 * t.TargetFileSize),
		"target_file_multiplier":               fmt.Sprint(targetFileMultiplier),
		"target_file_bytes_by_level_from_base": joinInts(targetFileBytesFromBase(t)),
		"memtable_delay_at":                    fmt.Sprint(memtableDelayAt),
		"max_open_files":                       fmt.Sprint(maxOpenFiles),
		"pin_top_level_index":                  fmt.Sprint(pinTopLevelIndex),
		"max_background_jobs":                  fmt.Sprint(backgroundJobs),
		"block_cache_capacity":                 fmt.Sprint(k.cacheBytes),
		"memtables_charged_to_cache":           fmt.Sprint(k.wbm != nil),
		"cc_version":                           ccVersion(),
		"cpu_model":                            cpuModel(),
		"rocksdb_portable":                     "unknown",
	}
	if p := os.Getenv("ROCKSDB_PORTABLE"); p != "" {
		out["rocksdb_portable"] = p
	}
	out["grocksdb_version"], out["cgo_cflags"], out["cgo_ldflags"] = "unknown", "", ""
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, d := range bi.Deps {
			if d.Path == grocksdbPath {
				out["grocksdb_version"] = d.Version
			}
		}
		for _, s := range bi.Settings {
			switch s.Key {
			case "CGO_CFLAGS":
				out["cgo_cflags"] = s.Value
			case "CGO_LDFLAGS":
				out["cgo_ldflags"] = s.Value
			}
		}
	}
	text, err := k.optionsText()
	if err != nil {
		return nil, err
	}
	out["rocksdb_options"] = text
	out["rocksdb_version"] = optionValue(text, "rocksdb_version")
	return out, nil
}

// optionsText is the text of the newest options file in the database's directory,
// which RocksDB writes at every writable opening and change of options.
func (k *KV) optionsText() (string, error) {
	entries, err := os.ReadDir(k.dir)
	if err != nil {
		return "", err
	}
	best, bestN := "", int64(-1)
	for _, e := range entries {
		rest, ok := strings.CutPrefix(e.Name(), "OPTIONS-")
		if !ok {
			continue
		}
		n, err := strconv.ParseInt(rest, 10, 64)
		if err != nil {
			continue // a temporary file
		}
		if n > bestN {
			best, bestN = e.Name(), n
		}
	}
	if best == "" {
		return "", fmt.Errorf("rockskv: %s holds no options file", k.dir)
	}
	b, err := os.ReadFile(filepath.Join(k.dir, best))
	return string(b), err
}

// optionValue is the value of the first "name=value" line of an options file, or
// "unknown".
func optionValue(text, name string) string {
	for line := range strings.SplitSeq(text, "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), name+"="); ok {
			return v
		}
	}
	return "unknown"
}

// joinInts is the numbers, comma-separated.
func joinInts(ns []int64) string {
	parts := make([]string, len(ns))
	for i, n := range ns {
		parts[i] = strconv.FormatInt(n, 10)
	}
	return strings.Join(parts, ",")
}
