package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/lotannauo/toposhift/internal/coalesce"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// The names the reduction gives the producers of the trace. The prefix
// "alibaba/" labels the records as the trace's, and "fixture/" is reserved for
// tests, so neither is a name a real source uses.
const (
	placementProducer lifecycle.Producer = "alibaba/placement"
	callsProducer     lifecycle.Producer = "alibaba/calls"
)

// The two tables of the trace and the header each archive's CSV must have.
const (
	placementTable = "MSMetricsUpdate"
	callsTable     = "CallGraph"
)

var (
	placementHeader = []string{"timestamp", "msname", "msinstanceid", "nodeid", "cpu_utilization", "memory_utilization"}
	callsHeader     = []string{"timestamp", "traceid", "service", "rpc_id", "rpctype", "um", "uminstanceid", "interface", "dm", "dminstanceid", "rt"}
)

// sampleInterval is the cadence of the placement samples in the trace. The
// minute is the unit of every grid in this command.
const sampleInterval = time.Minute

// ttlFactor is how many sampling intervals a sample promises: three.
const ttlFactor = 3

// callEdgeTTL is how long a minute's call rollup promises its edge.
const callEdgeTTL = 3 * time.Minute

// A cadence is how often the placement is sampled for a reduction: every
// sample (C60) or every fifth (C300, the objects receiver's pull).
type cadenceSpec struct {
	name string
	step int32 // minutes between samples
}

var (
	cadenceC60  = cadenceSpec{"c60", 1}
	cadenceC300 = cadenceSpec{"c300", 5}
)

// ttl is the TTL of a sample of this cadence: three of its intervals.
func (c cadenceSpec) ttl() time.Duration {
	return time.Duration(c.step) * sampleInterval * ttlFactor
}

// ttlMinutes is the TTL in minutes.
func (c cadenceSpec) ttlMinutes() int32 { return c.step * ttlFactor }

// coalesceConfig is the product's ingest coalescer configuration.
func coalesceConfig() coalesce.Config {
	return coalesce.Config{ExtendTTLFraction: 0.5, RunAgeFromTTL: true}
}

// mutant is a deliberate fault. Only tests set one: each is the fault a test
// of the same behaviour must catch.
type mutant int

const (
	mutNone                mutant = iota
	mutSplitAtFileBoundary        // intervals end where an archive ends
	mutGapAtTTLBreaks             // a gap of exactly one TTL splits an interval
	mutUnknownIsService           // UNKNOWN is a service like any other
	mutInferDelete                // a Delete is written where a pod stops being seen
	mutUniformDownsample          // the fixture keeps rows, not nodes
	mutCPUInPayload               // the pod's payload carries the CPU utilization
	mutServiceFromToken           // the service is read off the instance token
)

// Config is everything a run depends on, and what the manifest records of it.
type Config struct {
	// InDir holds MSMetricsUpdate/, CallGraph/ and SHA256SUMS. It is read only.
	InDir string
	// OutDir receives out/, check/, logs/ and the report. It is never inside a git work tree.
	OutDir string
	// Epoch places the trace's time zero on a calendar.
	Epoch time.Time

	// PlacementFiles and CallFiles are how many archives of each table the hour has.
	PlacementFiles, CallFiles int
	// PlacementFileMinutes is how many minutes of samples one placement archive holds.
	PlacementFileMinutes int
	// CallFileMillis is how many milliseconds of calls one call archive holds.
	CallFileMillis int64

	// FixtureMinutes and FixtureNodes size the fixture: its first minutes, and
	// about how many nodes it keeps.
	FixtureMinutes, FixtureNodes int

	// WriteC60 says whether the C60 stream is written. Its records are counted either way.
	WriteC60 bool
	// MaxFileBytes stops writing a stream that passes this size (zero is no limit);
	// its records are still counted.
	MaxFileBytes int64
	// MaxCallKeys and MaxEdges stop the run when the call rollups of one archive, or
	// the distinct edges of the hour, pass this many.
	MaxCallKeys, MaxEdges int

	// Commit is recorded in the manifest.
	Commit string
	// Progress receives one line per step. Nil is silent.
	Progress io.Writer

	// VerifyStreams checks the coalesced C60, C300 and call streams against the
	// raw samples, which are held in memory for it: for test-sized input only.
	VerifyStreams bool

	mut mutant
}

// defaultConfig is the configuration of the first hour of the trace.
func defaultConfig() Config {
	return Config{
		Epoch:                time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC),
		PlacementFiles:       2,
		CallFiles:            20,
		PlacementFileMinutes: 30,
		CallFileMillis:       180000,
		FixtureMinutes:       10,
		FixtureNodes:         200,
		WriteC60:             true,
		MaxFileBytes:         1 << 30,
		MaxCallKeys:          8_000_000,
		MaxEdges:             8_000_000,
	}
}

// hourMinutes is the length of the placement the archives hold, in minutes.
func (c *Config) hourMinutes() int32 {
	return int32(c.PlacementFiles * c.PlacementFileMinutes)
}

func (c *Config) logf(format string, args ...any) {
	if c.Progress != nil {
		fmt.Fprintf(c.Progress, format+"\n", args...)
	}
}

// validate reports the first setting a run cannot use.
func (c *Config) validate() error {
	switch {
	case c.InDir == "" || c.OutDir == "":
		return errors.New("the input and output directories are required")
	case c.PlacementFiles < 1 || c.CallFiles < 1:
		return errors.New("the number of archives of each table must be at least 1")
	case c.PlacementFileMinutes < 1:
		return errors.New("a placement archive must hold at least one minute")
	case c.CallFileMillis < 60000 || c.CallFileMillis%60000 != 0:
		return fmt.Errorf("a call archive must hold a whole number of minutes, not %d ms", c.CallFileMillis)
	case c.FixtureMinutes < 1 || c.FixtureNodes < 1:
		return errors.New("the fixture needs at least one minute and one node")
	case c.MaxCallKeys < 1 || c.MaxEdges < 1:
		return errors.New("the call limits must be positive")
	case c.Epoch.IsZero() || c.Epoch.Before(time.Unix(0, 0)):
		return fmt.Errorf("the epoch %s must be after the store's earliest event time", c.Epoch.Format(time.RFC3339))
	}
	if !c.Epoch.Equal(c.Epoch.Truncate(time.Second)) {
		return errors.New("the epoch must be a whole second")
	}
	return nil
}

// refuseWorkTree is an error if dir, or the nearest directory above it that
// exists, is inside a git work tree: the results are not written into a repository.
func refuseWorkTree(dir string) error {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return err
	}
	for d := abs; ; d = filepath.Dir(d) {
		if _, err := os.Stat(filepath.Join(d, ".git")); err == nil {
			return fmt.Errorf("%s is inside the git work tree %s: the results are not written into a repository", dir, d)
		}
		if filepath.Dir(d) == d {
			return nil
		}
	}
}
