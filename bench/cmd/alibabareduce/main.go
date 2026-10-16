// Command alibabareduce reduces one hour of the Alibaba cluster trace v2022
// (the MSMetricsUpdate and CallGraph tables) to an interval log.
//
// The placement samples (a pod on a node, every 60 seconds) become activity files
// of the records the product's ingest layer would store: pods, nodes and
// pod-scheduled-on-node edges, asserted by the producer "alibaba/placement" with a
// TTL of three sampling intervals, and coalesced by the product's ingest coalescer
// at two cadences (every sample, and every fifth). The calls become a per-minute
// rollup table and the minute edges of the service graph ("alibaba/calls"). A
// small fixture keeps whole nodes, sampled by hash, for the first minutes of the
// hour, and is replayed into the in-memory store to check that the stored records
// say what the samples said.
//
// The archives are streamed (tar, gzip and CSV from the standard library) and
// never extracted. A row is used or skipped, never repaired, and every skip is
// counted by reason; for each file the rows read equal the rows used plus the
// skips. Ids are used exactly as given.
//
//	go run ./cmd/alibabareduce -in ~/datasets/alibaba-v2022 -out ~/spikebench/alibaba-reduce/2026-10-15
//
// Two more streams of the placement can be written, for the same samples and
// cadences, with -arms: s, every assertion as the producer made it with no
// coalescer (placement_c60_s.parquet, placement_c300_s.parquet), and i, one record
// per maximal interval of a pod, a node or a pod-on-node edge
// (placement_c60_i.parquet, placement_c300_i.parquet). They give the other two
// ends of a comparison of what the coalesced stream costs to store. The S stream
// of the first hour of the trace is larger than the default -max-file-bytes: pass
// -max-file-bytes 0 with it, or the file is dropped and only its records counted.
// -ttl-factor sets how many sampling intervals a placement sample promises (the
// product's is 3); the call edges keep their TTL. With no -arms and the default
// factor, a run writes what it wrote before these flags existed. The independent
// check in testdata/check.sql assumes the default factor and is valid only for it.
//
// The output directory is never inside a git work tree. It receives out/ (the
// activity files and the tables), check/ and logs/, and params.json, manifest.json
// and REDUCTION.md. Everything is local and informational: nothing in it is
// published, and nothing derived from the trace belongs in the repository.
//
// Memory: tokens are interned to 32-bit ids, and the placement rows of one archive
// are held as 12-byte tuples, grouped by minute as they are read and dropped as
// each minute is reduced. The two placement cadences are reduced one after the
// other, each from its own read of the archives, and a stream lets go of its
// coalescer when it is finished, so only one coalescer of the placement is alive.
// A coalescer is split into parts along the order of its subjects and flushed a
// part and a deadline at a time, so that flushing every run of a stream at once
// does not build a list as long as the stream. Call archives are reduced one at a
// time, in order. Unless GOGC is set, the command lets the heap grow 40 percent
// over its live data between collections, not 100. Run it with GOMEMLIMIT=2GiB. The S
// stream holds no coalescer, and the I stream is made one record at a time from the
// intervals the run already holds, through a sorted list of 24-byte references to them.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"time"
)

// gcPercent is how far the heap may grow before a collection, when GOGC does not
// say: 40 percent of the live data, where the Go default is 100. A run holds most
// of a gigabyte of live data in a coalescer, and the default lets the heap reach
// twice that between collections.
const gcPercent = 40

// tuneGC sets the collector's growth to gcPercent unless GOGC is set.
func tuneGC() {
	if _, set := os.LookupEnv("GOGC"); !set {
		debug.SetGCPercent(gcPercent)
	}
}

func main() {
	tuneGC()
	cfg := defaultConfig()
	home, _ := os.UserHomeDir()
	flag.StringVar(&cfg.InDir, "in", filepath.Join(home, "datasets", "alibaba-v2022"), "the directory with MSMetricsUpdate/, CallGraph/ and SHA256SUMS (read only)")
	flag.StringVar(&cfg.OutDir, "out", "", "the directory the results are written to, never inside a git work tree (required)")
	epoch := flag.String("epoch", cfg.Epoch.Format(time.RFC3339), "the time of the trace's time zero (RFC 3339, a whole second)")
	flag.IntVar(&cfg.PlacementFiles, "placement-files", cfg.PlacementFiles, "the MSMetricsUpdate archives the hour has")
	flag.IntVar(&cfg.CallFiles, "call-files", cfg.CallFiles, "the CallGraph archives the hour has")
	flag.IntVar(&cfg.PlacementFileMinutes, "placement-file-minutes", cfg.PlacementFileMinutes, "the minutes of samples in one placement archive")
	flag.Int64Var(&cfg.CallFileMillis, "call-file-ms", cfg.CallFileMillis, "the milliseconds of calls in one call archive")
	flag.IntVar(&cfg.FixtureMinutes, "fixture-minutes", cfg.FixtureMinutes, "the minutes the fixture holds")
	flag.IntVar(&cfg.FixtureNodes, "fixture-nodes", cfg.FixtureNodes, "about how many nodes the fixture keeps")
	flag.BoolVar(&cfg.WriteC60, "write-c60", cfg.WriteC60, "write the C60 stream (its records are counted either way)")
	flag.Int64Var(&cfg.MaxFileBytes, "max-file-bytes", cfg.MaxFileBytes, "stop writing a stream that passes this size, and count its records only (0: no limit)")
	flag.IntVar(&cfg.MaxCallKeys, "max-call-keys", cfg.MaxCallKeys, "stop if one call archive has more rollup cells than this")
	flag.IntVar(&cfg.MaxEdges, "max-edges", cfg.MaxEdges, "stop if more distinct service edges than this are open")
	arms := flag.String("arms", "", "also write the placement's other streams: s (every assertion, no coalescer), i (one record per maximal interval), or s,i")
	flag.IntVar(&cfg.TTLFactor, "ttl-factor", cfg.TTLFactor, "how many sampling intervals a placement sample promises, 1 to 60 (the call edges keep their TTL)")
	flag.BoolVar(&cfg.VerifyStreams, "verify-streams", false, "check every stream against its samples in an in-memory store: holds the samples in memory, for test-sized input only")
	flag.Parse()
	t, err := time.Parse(time.RFC3339, *epoch)
	if err != nil {
		fmt.Fprintln(os.Stderr, "alibabareduce: -epoch:", err)
		os.Exit(2)
	}
	cfg.Epoch = t.UTC()
	if cfg.WriteS, cfg.WriteI, err = parseArms(*arms); err != nil {
		fmt.Fprintln(os.Stderr, "alibabareduce:", err)
		os.Exit(2)
	}
	cfg.Commit = currentCommit()
	cfg.Progress = os.Stderr
	if cfg.OutDir == "" {
		fmt.Fprintln(os.Stderr, "alibabareduce: -out is required")
		os.Exit(2)
	}
	start := time.Now()
	if _, err := reduce(cfg); err != nil {
		fmt.Fprintln(os.Stderr, "alibabareduce:", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "alibabareduce: done in %s; results in %s\n", time.Since(start).Round(time.Second), cfg.OutDir)
}
