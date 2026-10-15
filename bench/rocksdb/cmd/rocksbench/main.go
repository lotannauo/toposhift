// Command rocksbench builds and reads layout L on RocksDB (the RL candidates) on a
// plan made by spikebench, and writes spikebench's manifests and results, so that
// RL and layout L on Pebble are measured on the same stream and the same queries.
//
//	rocksbench build -out DIR -candidate RL/k64a2l5m   write the planned stream to an RL candidate
//	rocksbench read  -out DIR -candidate RL/k64a2l5m   ask the plan's queries of a built RL candidate
//
// DIR holds plan.json, made by `spikebench plan`, and the candidate is built under
// it as spikebench builds one. The candidates are RL/off and RL/ followed by a
// checkpoint policy written as layout L's candidates write it. The binary is built
// with cgo and RocksDB; the runner admits cgo for these candidates only, and every
// other guard of a result still applies (-untimed allows a binary that breaks one,
// for a validation run). The output directory must be outside the repository. This
// program never builds or reads a candidate of layout L on Pebble.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"syscall"
	"time"

	"github.com/lotannauo/toposhift/bench/rocksdb/rockslog"
	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
	"github.com/lotannauo/toposhift/bench/spike/runner"
)

// planFile is the plan spikebench writes in its output directory.
const planFile = "plan.json"

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1], os.Args[2:], os.Stderr); err != nil {
		if errors.Is(err, errUsage) {
			usage()
		}
		fmt.Fprintln(os.Stderr, "rocksbench:", err)
		os.Exit(1)
	}
}

var errUsage = errors.New("usage")

func usage() {
	fmt.Fprintln(os.Stderr, "usage: rocksbench build|read -out DIR -candidate RL/... [flags]   (rocksbench <command> -h for the flags)")
	os.Exit(2)
}

// run runs one command, saying what it does on say.
func run(ctx context.Context, cmd string, args []string, say io.Writer) error {
	progress := func(format string, a ...any) {
		fmt.Fprintf(say, "%s  %s\n", time.Now().Format("15:04:05"), fmt.Sprintf(format, a...))
	}
	switch cmd {
	case "build":
		return doBuild(ctx, args, progress)
	case "read":
		return doRead(ctx, args, progress)
	}
	return errUsage
}

// common are the flags of both commands.
type common struct {
	out, candidate string
	untimed, perf  bool
}

func (c *common) flags(fs *flag.FlagSet) {
	fs.StringVar(&c.out, "out", "", "output directory holding spikebench's plan.json, outside the repository (required)")
	fs.StringVar(&c.candidate, "candidate", "", "the RL candidate: RL/off, or RL/ and a checkpoint policy of layout L such as RL/k64a2l5m (required)")
	fs.BoolVar(&c.untimed, "untimed", false, "allow an instrumented or dirty binary: a validation run, not a result")
}

// load checks the flags and reads the plan and the candidate.
func (c *common) load() (*runner.Plan, runner.Guards, error) {
	if c.out == "" || c.candidate == "" {
		return nil, runner.Guards{}, errors.New("-out and -candidate are required")
	}
	if err := runner.CheckOutside(c.out); err != nil {
		return nil, runner.Guards{}, err
	}
	plan, err := runner.LoadPlan(filepath.Join(c.out, planFile))
	if err != nil {
		return nil, runner.Guards{}, err
	}
	return plan, runner.Guards{Info: runner.ReadBuildInfo(), Untimed: c.untimed}, nil
}

func doBuild(ctx context.Context, args []string, progress runner.Progress) error {
	fs := flag.NewFlagSet("build", flag.ContinueOnError)
	var c common
	var rest, sync bool
	var pebbleLimit string
	c.flags(fs)
	fs.BoolVar(&rest, "rest-after-retention", false, "wait for the database to be at rest after each retention, untimed (default: on for an -untimed build, off otherwise, as spikebench)")
	fs.BoolVar(&sync, "sync", false, "commit every batch with a sync of the log to the disk; recorded in the manifest")
	fs.StringVar(&pebbleLimit, "pebble-memory-limit", "", "the Go memory limit, in bytes or \"none\", of the Pebble builds this one is compared with: the build runs under that limit less the plan's cache and the four memtables, which RocksDB holds outside the Go heap (none stays none); recorded in the manifest as go_memory_limit (default: the limit the process has)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	restSet := false
	fs.Visit(func(f *flag.Flag) { restSet = restSet || f.Name == "rest-after-retention" })
	if !restSet {
		rest = c.untimed
	}
	plan, g, err := c.load()
	if err != nil {
		return err
	}
	v, err := rockslog.Variant(c.candidate, false)
	if err != nil {
		return err
	}
	if pebbleLimit != "" {
		if err := applyMemoryLimit(pebbleLimit, plan.CacheBytes, progress); err != nil {
			return err
		}
	}
	_, err = runner.BuildWith(ctx, plan, v, runner.CandidateDir(c.out, v.Name), g, runner.BuildOptions{RestAfterRetention: rest, Sync: sync}, progress)
	return err
}

func doRead(ctx context.Context, args []string, progress runner.Progress) error {
	fs := flag.NewFlagSet("read", flag.ContinueOnError)
	var c common
	c.flags(fs)
	fs.BoolVar(&c.perf, "perf", false, "sample RocksDB's perf context on every read (under read.<op>.rocks.*), a diagnostic: its counts of blocks depend on the cache, so the passes of the read differ and every query that loads a block is reported unstable")
	if err := fs.Parse(args); err != nil {
		return err
	}
	plan, g, err := c.load()
	if err != nil {
		return err
	}
	v, err := rockslog.Variant(c.candidate, c.perf)
	if err != nil {
		return err
	}
	res, err := runner.Read(ctx, plan, v, runner.CandidateDir(c.out, v.Name), g, progress)
	if err != nil {
		return err
	}
	progress("%s: %d queries, %d answers differ from the reference engine's, %d unstable", v.Name, len(res.Queries), len(res.Mismatches), len(res.Unstable))
	if n := len(res.Mismatches); n > 0 {
		return fmt.Errorf("%s answered %d queries differently from the reference engine (the first: %s); the results are written", v.Name, n, res.Mismatches[0])
	}
	return nil
}

// applyMemoryLimit sets the Go memory limit of an RL build from the limit of the Pebble
// builds it is compared with (see [rockslog.GoMemoryLimit]): "none", or bytes.
func applyMemoryLimit(pebble string, cacheBytes int64, progress runner.Progress) error {
	var limit int64
	if pebble != "none" {
		n, err := strconv.ParseInt(pebble, 10, 64)
		if err != nil || n <= 0 {
			return fmt.Errorf("-pebble-memory-limit %q is not a number of bytes or \"none\"", pebble)
		}
		t := pebblekv.BenchTuning()
		if cacheBytes > 0 {
			t.CacheBytes = cacheBytes
		}
		if limit, err = rockslog.GoMemoryLimit(n, t); err != nil {
			return err
		}
	}
	if limit == 0 {
		debug.SetMemoryLimit(math.MaxInt64) // the runtime's value for no limit
		progress("Go memory limit: none")
		return nil
	}
	debug.SetMemoryLimit(limit)
	progress("Go memory limit: %d bytes", limit)
	return nil
}
