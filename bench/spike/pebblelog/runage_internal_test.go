package pebblelog

import (
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/workload"
)

// An extension of a run is a record at the run's first instant, so it deletes every
// checkpoint after that instant. When the coalescer bounds a run's age by A, every
// record about a run whose description does not change is written before the run is A
// plus its refresh interval old, so a checkpoint at least A plus the extension
// interval behind the newest record is behind every run that can still be written to,
// and none is deleted; and when A is the extension interval itself no run is ever
// extended, so a checkpoint just behind the newest record survives as well. Without
// the bound, or with a lag short of it, extensions delete them. A run whose
// description changes (a pod made again under its old identity) is closed by a record
// at its first instant whatever A is, so the second rule needs fresh identities, as the
// cluster presets have. Every engine answers as the reference engine does throughout.
func TestALagPastTheRunAgeKeepsCheckpointsThroughExtensions(t *testing.T) {
	conformance.SkipWhenTrimmed(t)
	t.Parallel()

	// Heartbeats, rollups and every pod's placement refreshed every minute with a TTL
	// of four, extended at most every two (half the TTL), on prefixes that hold many
	// runs begun at different instants (a node's reverse prefix holds its pods'); nothing
	// late, so only the coalescer can put a record behind a checkpoint.
	const extension = 2 * time.Minute
	for name, c := range map[string]struct {
		fresh          bool
		runMaxAge, lag time.Duration
		retain         []float64
		invalidated    bool
	}{
		"no bound, lag 1ns":                           {true, 0, time.Nanosecond, nil, true},
		"bound 4m, lag 1ns":                           {true, 4 * time.Minute, time.Nanosecond, nil, true},
		"bound 4m, lag 4m + 2m":                       {true, 4 * time.Minute, 4*time.Minute + extension, []float64{0.5}, false},
		"bound 2m (the extension), lag 1ns":           {true, extension, time.Nanosecond, nil, false},
		"bound 2m, lag 1ns, descriptions that change": {false, extension, time.Nanosecond, nil, true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cfg := workload.Tiny()
			cfg.Seed, cfg.Duration = 77, 40*time.Minute
			cfg.PodHeartbeatInterval, cfg.FreshIdentities = time.Minute, c.fresh
			cfg.CoalesceRuns, cfg.ExtendTTLFraction, cfg.RunMaxAge = true, 0.5, c.runMaxAge
			cfg.LateProbability, cfg.LateMeanDelay = 0, 0
			rec := &engine.MemRecorder{}
			e := openMem(t, Options{Checkpoints: CheckpointOptions{On: true, KMin: 4, Alpha: 1, Lag: c.lag}, Recorder: rec})
			if err := conformance.Check(e, cfg, conformance.Options{RetainAt: c.retain}); err != nil {
				t.Fatal(err)
			}
			if rec.Counter("checkpoint.written") == 0 || rec.Counter("read.checkpoint_hits") == 0 {
				t.Fatalf("written %d, used %d: the stream does not exercise checkpoints", rec.Counter("checkpoint.written"), rec.Counter("read.checkpoint_hits"))
			}
			if got := rec.Counter("checkpoint.invalidated"); (got > 0) != c.invalidated {
				t.Errorf("%d checkpoints deleted of %d written; want some deleted: %v", got, rec.Counter("checkpoint.written"), c.invalidated)
			}
		})
	}
}
