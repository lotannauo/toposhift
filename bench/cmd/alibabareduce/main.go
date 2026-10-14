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
// The output directory is never inside a git work tree. It receives out/ (the
// activity files and the tables), check/ and logs/, and params.json, manifest.json
// and REDUCTION.md. Everything is local and informational: nothing in it is
// published, and nothing derived from the trace belongs in the repository.
//
// Memory: tokens are interned to 32-bit ids, placement rows are held as 16-byte
// tuples one archive at a time, and call archives are reduced one at a time, in
// order. Run it with GOMEMLIMIT=2GiB.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

func main() {
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
	flag.BoolVar(&cfg.VerifyStreams, "verify-streams", false, "check every stream against its samples in an in-memory store: holds the samples in memory, for test-sized input only")
	flag.Parse()
	t, err := time.Parse(time.RFC3339, *epoch)
	if err != nil {
		fmt.Fprintln(os.Stderr, "alibabareduce: -epoch:", err)
		os.Exit(2)
	}
	cfg.Epoch = t.UTC()
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
