// Package rulejudge judges layout L on RocksDB against layout L on Pebble by Rule R,
// the rule of their comparison. It reads the bench workflow's timed builds of both
// engines, each with its job file, and the read of each candidate's first
// repetition; it checks that the RocksDB builds are what the rule compares (the
// answers and comparable counters of Pebble's, the build stamp of the library, the
// Go memory limit, the regime), compares the medians of G2, G3, G4 and the peak
// resident set, and says which verdict the rule gives.
//
// The runner's gates refuse a set built with cgo, by design, so the judge never
// gives them a RocksDB build: [runner.Check] sees the Pebble builds only, and G2,
// G3 and G4 are read from each manifest here, as the runner reads them. The package
// needs no cgo and never opens a database, so it runs, and is tested, with cgo off.
//
// It also holds the two parts of the rule that rocksbench and its tests use: which
// counters both engines define alike ([Comparable]) and the Go memory limit of a
// RocksDB build ([GoMemoryLimit]).
package rulejudge

import (
	"fmt"
	"strings"

	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
	"github.com/lotannauo/toposhift/bench/spike/runner"
)

// The candidates the rule compares, by the start of their names: layout L on Pebble,
// on RocksDB (RL), and on RocksDB with base-level files of four times the tuning's
// target file size (RL4, the sensitivity arm). The rest of a name is the checkpoint
// policy, the same for the candidates a comparison sets side by side.
const (
	PebblePrefix = "L/"
	RocksPrefix  = "RL/"
	Rocks4Prefix = "RL4/"
)

// The scope of the verdict and the numbers of the rule.
const (
	// VerdictWindow and VerdictPolicy are where the verdict is judged: the policy
	// k64a2l5m at 30 days. Every other window and policy is reported.
	VerdictWindow = 30
	VerdictPolicy = "k64a2l5m"
	// DecidingArch decides, and GuardArch guards the direction: an outcome there that
	// is decided the other way makes the gate not decided.
	DecidingArch = "aarch64"
	GuardArch    = "x86_64"
	// minReps is the repetitions of a candidate below which a set is refused, and
	// below which builds on more than one CPU model are not compared.
	minReps = 3
	// margin is the factor by more than which, and by more than the noise floor, one
	// engine is better or worse than the other: about one bucket of the commit
	// histogram, which has eight to each doubling (1.09).
	margin = 1.10
	// decisive is the factor of G3 by at least which RocksDB is decisively better.
	decisive = 1.5
	// spread is the factor by more than which a candidate's own values may vary over
	// its repetitions on the deciding architecture before its scope is not decided.
	spread = 1.5
	// floorPercent is the percentile, over the pairs of a candidate's repetitions, of
	// the ratio of the larger value to the smaller that is its noise floor.
	floorPercent = 95
)

// The keys of a description that the rule reads and the runner does not name.
const (
	// StampKey and StampSHA256Key record the build stamp of the RocksDB library a
	// build linked (its text and the text's sha256), each "none" for a library that
	// has none; rockskv writes them.
	StampKey       = "rocksdb_build_stamp"
	StampSHA256Key = "rocksdb_build_stamp_sha256"
	// PerfLevelKey is RocksDB's perf context level, "disable" unless a read sampled it.
	PerfLevelKey = "perf_level"
)

// Comparable is whether a counter is one both engines define the same way: the
// layout's own counters, and the reads, seeks and steps of each kind of read. What
// Pebble counts inside its iterator, the cold and allocation counters derived from
// it and the engine's timings are not; nor are RL's own diagnostics (rocks.*).
// The cold counters differ in meaning between the engines (RocksDB's block-cache
// misses after a cold start against Pebble's blocks loaded, a cache charge that
// depends on each engine's model of it), and are reported, never compared.
func Comparable(name string) bool {
	switch name {
	case runner.CounterBlockLoads, runner.CounterColdBlockBytes, runner.CounterColdCacheBytes, runner.CounterAllocs, runner.CounterAllocBytes:
		return false
	}
	if strings.HasSuffix(name, "_ns") {
		return false
	}
	if rest, ok := strings.CutPrefix(name, "read."); ok {
		// read.<op>.<stat> is one kind of read's; read.<name> is the layout's own.
		if _, stat, ok := strings.Cut(rest, "."); ok {
			return stat == "reads" || stat == "seeks" || stat == "steps"
		}
	}
	return true
}

// Tuning is the tuning both engines build a plan with: the bench tuning, with the
// plan's block cache when it names one.
func Tuning(cacheBytes int64) pebblekv.Tuning {
	t := pebblekv.BenchTuning()
	if cacheBytes > 0 {
		t.CacheBytes = cacheBytes
	}
	return t
}

// GoMemoryLimit is the Go memory limit of an RL build that is compared with a build of
// layout L on Pebble that ran under pebbleLimit bytes (zero or less: no limit), both
// with the tuning t and its cache. Pebble's block cache and memtables are Go
// allocations inside its limit, and RocksDB's are not, so RL's limit is Pebble's less
// the cache and the memtables a written database charges to it: t.CacheBytes plus
// t.MemTableReserve(). Where Pebble had no limit RL has none (zero). A Pebble limit
// that does not exceed that charge is refused.
func GoMemoryLimit(pebbleLimit int64, t pebblekv.Tuning) (int64, error) {
	if pebbleLimit <= 0 {
		return 0, nil
	}
	if t == (pebblekv.Tuning{}) {
		t = pebblekv.BenchTuning()
	}
	charge := t.CacheBytes + t.MemTableReserve()
	if pebbleLimit <= charge {
		return 0, fmt.Errorf("rulejudge: a Go memory limit of %d bytes is not above the %d bytes of cache and memtables RocksDB holds outside it", pebbleLimit, charge)
	}
	return pebbleLimit - charge, nil
}

// stampValue is the value of the first "key=value" line of a build stamp with that
// key, or "" when it has none.
func stampValue(text, key string) string {
	for line := range strings.SplitSeq(text, "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), key+"="); ok {
			return v
		}
	}
	return ""
}
