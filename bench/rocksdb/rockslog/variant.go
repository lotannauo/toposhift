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

// Variant is the RL candidate of that name, made by [candidates.External] as a
// candidate of layout L that declares cgo. perf turns on RocksDB's perf context for
// every read (see [rockskv.Config].Perf): for a counters run, never for a timed one;
// the candidate's name is the same either way, and its description says which.
//
// Its options are the runner's ([candidates.Options]) mapped to RocksDB's, and it
// refuses those it cannot honour: a file system (a RocksDB database lives on the
// real one) and a tuning that is not mapped (see [rockskv.Open]).
func Variant(name string, perf bool) (candidates.Variant, error) {
	cp, err := policyOf(name)
	if err != nil {
		return candidates.Variant{}, err
	}
	return candidates.External(name, "L", true, func(dir string, o candidates.Options) (engine.Engine, error) {
		cfg, err := ConfigOf(o)
		if err != nil {
			return nil, err
		}
		cfg.Perf = perf
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

// policyOf is the checkpoint policy an RL name stands for: none for RL/off, and for
// RL/<policy> the policy of layout L's candidate L/<policy>, which must be written
// as that candidate is (the same spelling, so the two name one point).
func policyOf(name string) (CheckpointOptions, error) {
	rest, ok := strings.CutPrefix(name, NamePrefix)
	if !ok {
		return CheckpointOptions{}, fmt.Errorf("rockslog: %q does not begin with %q", name, NamePrefix)
	}
	if rest == "off" {
		return CheckpointOptions{}, nil
	}
	if _, err := candidates.Lookup("L/" + rest); err != nil {
		return CheckpointOptions{}, fmt.Errorf("rockslog: %q is not RL/off or RL/ and a checkpoint policy of layout L: %w", name, err)
	}
	var k int
	var alpha float64
	var lag time.Duration
	var replace int
	if err := parsePolicy(rest, &k, &alpha, &lag, &replace); err != nil {
		return CheckpointOptions{}, fmt.Errorf("rockslog: %q: %w", name, err)
	}
	return CheckpointOptions{On: true, KMin: k, Alpha: alpha, Lag: lag, Replace: replace}, nil
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
		return 0, fmt.Errorf("rockslog: a Go memory limit of %d bytes is not above the %d bytes of cache and memtables RocksDB holds outside it", pebbleLimit, charge)
	}
	return pebbleLimit - charge, nil
}
