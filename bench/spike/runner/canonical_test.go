package runner_test

import (
	"context"
	"errors"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/bench/spike/candidates"
	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/runner"
)

// stepsOf is what a read counted that does not depend on the tables it crossed: the
// blocks, the cache, the time, the allocations and where a value was kept are left
// out.
func stepsOf(c map[string]int64) map[string]int64 {
	out := map[string]int64{}
	for k, v := range c {
		switch {
		case strings.Contains(k, "block_"), strings.Contains(k, "separated_value"), strings.HasSuffix(k, ".value_bytes"),
			k == runner.CounterBlockLoads, k == runner.CounterColdBlockBytes, k == runner.CounterColdCacheBytes,
			k == runner.CounterAllocs, k == runner.CounterAllocBytes:
		default:
			out[k] = v
		}
	}
	return out
}

// A build told to rewrite its tables into the canonical layout says so in its
// manifest, and one that is not told says it did not (Build never does); the read
// accepts it, its answers are the plan's, and the engine's counts, the logical
// bytes and what every read steps over are those of a plain build. Its tables are
// all in the bottom level and hold no tombstone.
func TestACanonicalBuildAnswersAndStepsAsAPlainOne(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t)

	plan := mustPlan(t, tinySpec())
	for _, name := range []string{"L/k64a4", "M/crdb1"} {
		v := lookup(t, name)
		_, plain, rp := run(t, plan, v)
		dir := runner.CandidateDir(t.TempDir(), v.Name)
		canon, err := runner.BuildWith(context.Background(), plan, v, dir, clean, runner.BuildOptions{CanonicalLayout: true}, nil)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		rc, err := runner.Read(context.Background(), plan, v, dir, clean, nil)
		if err != nil {
			t.Fatalf("%s: reading the canonical build: %v", name, err)
		}
		if plain.Describe[runner.CanonicalKey] != "false" || canon.Describe[runner.CanonicalKey] != "true" {
			t.Errorf("%s: %s is %q by default and %q when asked", name, runner.CanonicalKey, plain.Describe[runner.CanonicalKey], canon.Describe[runner.CanonicalKey])
		}
		if _, ok := rc.Describe[runner.CanonicalKey]; ok {
			t.Errorf("%s: the read's description has %s, which only the build knows", name, runner.CanonicalKey)
		}
		if len(rc.Mismatches)+len(rc.Unstable)+len(canon.UncompactedWrong) > 0 {
			t.Errorf("%s: canonical: %v wrong, %v unstable", name, rc.Mismatches, rc.Unstable)
		}
		if !reflect.DeepEqual(plain.Counters, canon.Counters) || !reflect.DeepEqual(plain.Breakdown, canon.Breakdown) {
			t.Errorf("%s: what the build counted or holds differs with the canonical layout", name)
		}
		for i, q := range rp.Queries {
			if a, b := stepsOf(q.Counters), stepsOf(rc.Queries[i].Counters); !maps.Equal(a, b) || q.Digest != rc.Queries[i].Digest {
				t.Errorf("%s: %s steps %v, canonical %v", name, plan.Queries[i].Name(), a, b)
				break
			}
		}
		s := canon.StatsCompacted
		if s["tombstones"] != 0 || s["tables_l6"] == 0 || s["tables_l0"]+s["tables_l1"]+s["tables_l2"]+s["tables_l3"]+s["tables_l4"]+s["tables_l5"] != 0 {
			t.Errorf("%s: the canonical tables are not all in the bottom level, without tombstones: %v", name, s)
		}
	}
}

// failingCanon is an engine whose rewrite into the canonical layout fails.
type failingCanon struct{ fullEngine }

func (failingCanon) Canonicalize(context.Context) error {
	return errors.New("injected canonical failure")
}

var _ engine.Canonicalizer = failingCanon{}

// A build that cannot rewrite its tables when told to, because the engine cannot or
// because the rewrite fails, writes no manifest.
func TestACanonicalBuildThatFailsWritesNoManifest(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t)

	plan := mustPlan(t, tinySpec())
	for name, c := range map[string]struct {
		wrap func(e engine.Engine) engine.Engine
		want string
	}{
		"an engine that cannot": {func(e engine.Engine) engine.Engine { return struct{ fullEngine }{e.(fullEngine)} }, "cannot rewrite its tables"},
		"a rewrite that fails":  {func(e engine.Engine) engine.Engine { return failingCanon{e.(fullEngine)} }, "injected canonical failure"},
	} {
		v := candidates.Wrap(lookup(t, "L/off"), "L/canon", func(e engine.Engine, o candidates.Options) (engine.Engine, error) {
			if o.ReadOnly {
				return e, nil
			}
			return c.wrap(e), nil
		})
		dir := runner.CandidateDir(t.TempDir(), v.Name)
		if _, err := runner.BuildWith(context.Background(), plan, v, dir, clean, runner.BuildOptions{CanonicalLayout: true}, nil); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v", name, err)
		}
		if _, err := os.Stat(filepath.Join(dir, runner.ManifestFile)); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("%s: the build wrote a manifest (%v)", name, err)
		}
		// Not told to, the same engine builds.
		if name == "an engine that cannot" {
			if _, err := runner.BuildWith(context.Background(), plan, v, runner.CandidateDir(t.TempDir(), v.Name), clean, runner.BuildOptions{}, nil); err != nil {
				t.Errorf("%s, not told to rewrite: %v", name, err)
			}
		}
	}
}
