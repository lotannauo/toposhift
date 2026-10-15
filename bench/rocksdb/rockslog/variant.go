package rockslog

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/lotannauo/toposhift/bench/rocksdb/rockskv"
	"github.com/lotannauo/toposhift/bench/spike/candidates"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
)

// NamePrefix starts the name of every RL candidate: RL/off, and RL/ followed by a
// checkpoint policy written as layout L's candidates write it (RL/k64a2l5m is
// L/k64a2l5m on RocksDB).
const NamePrefix = "RL/"

// NamePrefix4 starts the name of every RL4 candidate, named as the RL candidates
// are: RL with the files of RocksDB's base level four times the tuning's target file
// size where RL's are twice it ([rockskv.Config].BaseFileFactor 4), so that from the
// level below the base down its files are Pebble's, where RL's are half of them. It
// is the sensitivity of a comparison to which level's file size is matched, not a
// tuning.
const NamePrefix4 = "RL4/"

// rl4BaseFileFactor is the base file factor of the RL4 candidates.
const rl4BaseFileFactor = 4

// Variant is the RL or RL4 candidate of that name, made by [candidates.External] as a
// candidate of layout L that declares cgo. perf turns on RocksDB's perf context for
// every read (see [rockskv.Config].Perf): for a counters run, never for a timed one;
// the candidate's name is the same either way, and its description says which.
//
// Its options are the runner's ([candidates.Options]) mapped to RocksDB's, and it
// refuses those it cannot honour: a file system (a RocksDB database lives on the
// real one) and a tuning that is not mapped (see [rockskv.Open]).
func Variant(name string, perf bool) (candidates.Variant, error) {
	cp, baseFiles, err := policyOf(name)
	if err != nil {
		return candidates.Variant{}, err
	}
	return candidates.External(name, "L", true, func(dir string, o candidates.Options) (engine.Engine, error) {
		cfg, err := ConfigOf(o)
		if err != nil {
			return nil, err
		}
		cfg.Perf = perf
		cfg.BaseFileFactor = baseFiles
		return Open(dir, Options{Config: cfg, Recorder: o.Recorder, Checkpoints: cp, TallyPrefixes: o.TallyPrefixes})
	})
}

// ConfigOf is the database config of a run's options, or why RL cannot honour them.
// A zero tuning is the bench tuning, and a cache size replaces the tuning's, as for
// layout L on Pebble.
func ConfigOf(o candidates.Options) (rockskv.Config, error) {
	if o.FS != nil {
		return rockskv.Config{}, errors.New("rockslog: a RocksDB database lives on the real file system: a file system in the options cannot be honoured")
	}
	t := o.Tuning
	if t == (pebblekv.Tuning{}) {
		t = pebblekv.BenchTuning()
	}
	if o.CacheBytes > 0 {
		t.CacheBytes = o.CacheBytes
	}
	return rockskv.Config{
		Tuning: t, Sync: o.Sync,
		DisableAutoCompactions: o.DisableAutoCompactions, DisableReadCompactions: o.DisableReadCompactions,
		ReadOnly: o.ReadOnly, SettleRetention: o.SettleRetention,
	}, nil
}

// policyOf is the checkpoint policy and the base file factor an RL or RL4 name
// stands for: no checkpoints for RL/off and RL4/off, and for RL/<policy> and
// RL4/<policy> the policy of layout L's candidate L/<policy>, which must be written
// as that candidate is (the same spelling, so the two name one point); the factor
// is 4 for RL4 and RocksDB's default (zero) for RL.
func policyOf(name string) (CheckpointOptions, int, error) {
	prefix, baseFiles := NamePrefix, 0
	rest, ok := strings.CutPrefix(name, NamePrefix)
	if !ok {
		if rest, ok = strings.CutPrefix(name, NamePrefix4); !ok {
			return CheckpointOptions{}, 0, fmt.Errorf("rockslog: %q does not begin with %q or %q", name, NamePrefix, NamePrefix4)
		}
		prefix, baseFiles = NamePrefix4, rl4BaseFileFactor
	}
	if rest == "off" {
		return CheckpointOptions{}, baseFiles, nil
	}
	if _, err := candidates.Lookup("L/" + rest); err != nil {
		return CheckpointOptions{}, 0, fmt.Errorf("rockslog: %q is not %soff or %s and a checkpoint policy of layout L: %w", name, prefix, prefix, err)
	}
	var k int
	var alpha float64
	var lag time.Duration
	var replace int
	if err := parsePolicy(rest, &k, &alpha, &lag, &replace); err != nil {
		return CheckpointOptions{}, 0, fmt.Errorf("rockslog: %q: %w", name, err)
	}
	return CheckpointOptions{On: true, KMin: k, Alpha: alpha, Lag: lag, Replace: replace}, baseFiles, nil
}

// parsePolicy reads k<K>a<alpha>[l<lag>][r<N>], a name candidates.Lookup has
// already accepted, and checks it reads back as the same name.
func parsePolicy(s string, k *int, alpha *float64, lag *time.Duration, replace *int) error {
	body, ok := strings.CutPrefix(s, "k")
	if !ok {
		return errors.New("a policy begins with k")
	}
	kText, body, ok := strings.Cut(body, "a")
	if !ok {
		return errors.New("a policy has an alpha")
	}
	if _, err := fmt.Sscanf(kText, "%d", k); err != nil {
		return err
	}
	if before, after, found := strings.Cut(body, "r"); found {
		if _, err := fmt.Sscanf(after, "%d", replace); err != nil {
			return err
		}
		body = before
	}
	alphaText, lagText, hasLag := strings.Cut(body, "l")
	if _, err := fmt.Sscanf(alphaText, "%g", alpha); err != nil {
		return err
	}
	if hasLag {
		d, err := time.ParseDuration(lagText)
		if err != nil {
			return err
		}
		*lag = d
	}
	want := candidates.LCheckpointsReplacing(*k, *alpha, *lag, *replace).Name
	if want != "L/"+s {
		return fmt.Errorf("reads back as %q", want)
	}
	return nil
}
