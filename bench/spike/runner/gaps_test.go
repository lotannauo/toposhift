package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"

	"github.com/lotannauo/toposhift/bench/spike/candidates"
	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/pebblelog"
	"github.com/lotannauo/toposhift/bench/spike/workload"
)

func TestGapStatsAreTheMedianTheNinetiethAndTheHoles(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		lengths                     []int
		median, p90, largest, holes int
	}{
		"none":        {nil, 0, 0, 0, 0},
		"one":         {[]int{7}, 7, 7, 7, 0},
		"a hole":      {[]int{10, 10, 10, 10, 10, 10, 10, 10, 10, 17}, 10, 10, 17, 1},
		"not a hole":  {[]int{10, 10, 10, 10, 10, 10, 10, 10, 10, 16}, 10, 10, 16, 0}, // exactly 1.6 times is not over it
		"upper":       {[]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 6, 9, 10, 1},            // the upper middle value; 10 is over 9.6
		"unsorted":    {[]int{300, 100, 100, 100, 100}, 100, 300, 300, 1},
		"all equal":   {[]int{5, 5, 5}, 5, 5, 5, 0},
		"two lengths": {[]int{4, 100}, 100, 100, 100, 0},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if m, p, l, h := GapStats(tc.lengths); m != tc.median || p != tc.p90 || l != tc.largest || h != tc.holes {
				t.Errorf("GapStats(%v) = median %d, p90 %d, largest %d, holes %d; want %d, %d, %d, %d", tc.lengths, m, p, l, h, tc.median, tc.p90, tc.largest, tc.holes)
			}
		})
	}
}

// keysOf is a prefix, newest first: a record, a checkpoint (instant, W) or the
// baseline, each at its event time.
func keysOf() []gapKey {
	rec := func(ns int64) gapKey { return gapKey{kind: pebblelog.KeyRecord, ns: ns} }
	ckpt := func(ns int64, w uint64, current bool) gapKey {
		return gapKey{kind: pebblelog.KeyCheckpoint, ns: ns, w: w, current: current}
	}
	return []gapKey{
		rec(100), rec(90), ckpt(85, 5, true), rec(80), rec(70), ckpt(60, 3, true), rec(50), rec(45), {kind: pebblelog.KeyBaseline, ns: 40},
	}
}

func TestTheModelWalksAReadAsTheEngineDoes(t *testing.T) {
	t.Parallel()
	keys := keysOf()
	const latest = ^uint64(0)
	for name, tc := range map[string]struct {
		at      int64
		asOf    uint64
		steps   int64
		ends    string
		skipped int
	}{
		"after everything":       {200, latest, 2, "checkpoint", 0},
		"between records":        {95, latest, 1, "checkpoint", 0},
		"on the first record":    {90, latest, 1, "checkpoint", 0},
		"on the checkpoint":      {85, latest, 0, "checkpoint", 0},
		"just under it":          {84, latest, 2, "checkpoint", 0}, // 80, 70, then the checkpoint at 60
		"a token below its W":    {84, 2, 5, "baseline", 1},        // past 60 and on to the baseline
		"a token at its W":       {84, 3, 2, "checkpoint", 0},
		"below the last":         {55, latest, 2, "baseline", 0}, // 50 and 45, then the baseline at 40: the checkpoint at 60 is newer
		"before the baseline":    {30, latest, 0, "none", 0},
		"at the baseline":        {40, latest, 0, "baseline", 0},
		"above the first record": {1 << 62, latest, 2, "checkpoint", 0},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			steps, ends, skipped := walkSteps(keys, tc.at, tc.asOf)
			if steps != tc.steps || ends != tc.ends || skipped != tc.skipped {
				t.Errorf("walkSteps(at %d, token %d) = %d steps to the %s past %d; want %d, %s, %d", tc.at, tc.asOf, steps, ends, skipped, tc.steps, tc.ends, tc.skipped)
			}
		})
	}
	// Another fold version is not a base: a read walks past it.
	other := slices.Clone(keys)
	other[2].current = false
	if steps, ends, skipped := walkSteps(other, 95, ^uint64(0)); steps != 4 || ends != "checkpoint" || skipped != 1 {
		t.Errorf("past a checkpoint of another fold version: %d steps to the %s past %d; want 4, checkpoint, 1", steps, ends, skipped)
	}
	// A prefix with no checkpoint and no baseline runs out.
	bare := []gapKey{{kind: pebblelog.KeyRecord, ns: 30}, {kind: pebblelog.KeyRecord, ns: 20}, {kind: pebblelog.KeyRecord, ns: 10}}
	if steps, ends, _ := walkSteps(bare, 25, ^uint64(0)); steps != 2 || ends != "end" {
		t.Errorf("a prefix that runs out: %d steps to the %s; want 2 (the keys left), end", steps, ends)
	}
}

func TestGapsAreTheRecordsBetweenConsecutiveCheckpoints(t *testing.T) {
	t.Parallel()
	keys := keysOf()
	lengths, cks := gapsOf(keys)
	if !slices.Equal(lengths, []int{2}) || !slices.Equal(cks, []int{2, 5}) {
		t.Errorf("gaps %v at checkpoints %v; want [2] at [2 5]: the tail above the newest and the part below the oldest are not gaps", lengths, cks)
	}
	for name, tc := range map[string]struct {
		at    int64
		where string
		gap   int
	}{
		"in the tail":             {95, "tail", -1},
		"on the newest":           {85, "tail", -1},
		"in the gap":              {84, "gap", 0},
		"on the older boundary":   {60, "gap", 0}, // the checkpoint at exactly the instant is the one the read uses
		"below the oldest":        {55, "below", -1},
		"before the first record": {10, "below", -1},
	} {
		if where, gap := place(keys, cks, tc.at); where != tc.where || gap != tc.gap {
			t.Errorf("%s: place(%d) = %s %d, want %s %d", name, tc.at, where, gap, tc.where, tc.gap)
		}
	}
}

// gapsSpec is a stream with late records and runs that are refreshed, long enough
// for a few prefixes to hold several dozen records, whose checkpoints a policy of
// eight records writes and late records delete.
func gapsSpec() Spec {
	cfg := workload.Tiny()
	cfg.Duration = 75 * time.Minute
	cfg.LateProbability, cfg.LateMeanDelay = 0.2, 3*time.Minute
	cfg.CoalesceRuns = true
	cfg.EventsPerSecond, cfg.PodSkew = 1, 3
	s := DefaultSpec(cfg)
	s.BatchSize = 64
	s.MinNonEmpty = 0
	return s
}

// quickSpec is a stream of a few minutes, for the checks that need a build and not
// a history.
func quickSpec() Spec {
	s := DefaultSpec(workload.Tiny())
	s.BatchSize = 64
	s.MinNonEmpty = 0
	return s
}

var untimedGuards = Guards{Info: BuildInfo{GoVersion: "test", GOOS: "test", GOARCH: "test", Revision: "r"}, Untimed: true}

// buildAndRead builds a candidate canonically under a new directory, reads it, and
// returns the directory.
func buildAndRead(t *testing.T, plan *Plan, name string, g Guards) (candidates.Variant, string) {
	t.Helper()
	v, err := candidates.Lookup(name)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	dir := CandidateDir(t.TempDir(), v.Name)
	if _, err := BuildWith(ctx, plan, v, dir, g, BuildOptions{RestAfterRetention: g.Untimed, CanonicalLayout: true}, nil); err != nil {
		t.Fatalf("building %s: %v", name, err)
	}
	if _, err := Read(ctx, plan, v, dir, g, nil); err != nil {
		t.Fatalf("reading %s: %v", name, err)
	}
	return v, dir
}

type quietLogger struct{}

func (quietLogger) Infof(string, ...any)  {}
func (quietLogger) Errorf(string, ...any) {}
func (quietLogger) Fatalf(format string, args ...any) {
	panic(fmt.Sprintf("pebble: "+format, args...))
}

// rawGaps reads a prefix's keys from the database a build left, apart from the
// engine and by the layout of the key (the prefix, the inverted event time and
// Seq, the kind): the records between consecutive checkpoints, as the tool counts
// them.
func rawGaps(t *testing.T, dbDir string, prefix []byte) (records int, lengths []int, checkpoints int) {
	t.Helper()
	db, err := pebble.Open(dbDir, &pebble.Options{ReadOnly: true, Logger: quietLogger{}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	hi := slices.Clone(prefix)
	hi[len(hi)-1]++
	it, err := db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: hi})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = it.Close() }()
	n := 0
	for ok := it.First(); ok; ok = it.Next() {
		k := it.Key()
		if len(k) != len(prefix)+17 {
			t.Fatalf("key %x is %d bytes, want %d", k, len(k), len(prefix)+17)
		}
		switch k[len(k)-1] {
		case 0:
			records++
			n++
		case 1:
			if checkpoints > 0 {
				lengths = append(lengths, n)
			}
			checkpoints++
			n = 0
		}
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	return records, lengths, checkpoints
}

// The tool reads the same gaps off a built store as the keys say, its model walks
// every pinned read in exactly the steps the read took, and the tallies it is given
// add up to the counts of the build: for the policy as it was and with checkpoints
// put back.
func TestGapsModelTheReadsOfACanonicalBuild(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t)
	plan, err := MakePlan(context.Background(), gapsSpec(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Queries) == 0 {
		t.Fatal("no queries")
	}
	for _, name := range []string{"L/k8a1", "L/k8a1r4"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			v, dir := buildAndRead(t, plan, name, untimedGuards)
			rep, err := Gaps(plan, v, dir)
			if err != nil {
				t.Fatal(err)
			}
			if !rep.Canonical || !rep.HasTallies || !rep.HasResults {
				t.Errorf("canonical %v, tallies %v, results %v; want all three", rep.Canonical, rep.HasTallies, rep.HasResults)
			}
			if len(rep.Prefixes) == 0 {
				t.Fatalf("no prefix is looked at: the plan has no %q reads", GapsGroup)
			}

			// The model against the reads, query by query.
			compared := 0
			for _, p := range rep.Prefixes {
				for _, q := range p.Queries {
					if q.ReadSteps >= 0 {
						compared++
					}
					if q.ReadSteps >= 0 && q.ModelSteps != q.ReadSteps {
						t.Errorf("%s %s: the model walks %d steps, the read took %d", p.Fp, q.Age, q.ModelSteps, q.ReadSteps)
					}
				}
			}
			if compared == 0 || rep.Mismatched != 0 {
				t.Errorf("%d queries compared, %d differ", compared, rep.Mismatched)
			}

			// The gaps against the raw keys.
			opened, err := openReadOnly(plan, v, dir, NewCapture(), false)
			if err != nil {
				t.Fatal(err)
			}
			sc := opened.(pebblelog.PrefixScanner)
			prefixes := map[string][]byte{}
			seen := map[string]bool{}
			for _, q := range plan.Queries {
				if q.Group != GapsGroup || seen[q.Fps[0].String()] {
					continue
				}
				seen[q.Fps[0].String()] = true
				p, ok := sc.PrefixOf(q.Layer, q.Fps[0], q.Dir)
				if !ok {
					t.Fatalf("no prefix for %s", q.Fps[0])
				}
				prefixes[q.Fps[0].String()] = p
			}
			if err := opened.Close(); err != nil {
				t.Fatal(err)
			}
			totalCkpts := 0
			for _, p := range rep.Prefixes {
				records, lengths, ckpts := rawGaps(t, filepath.Join(dir, DBDir), prefixes[p.Fp])
				median, p90, largest, holes := GapStats(lengths)
				if p.Records != records || p.Checkpoints != ckpts || p.Gaps != len(lengths) || p.Median != median || p.P90 != p90 || p.Max != largest || p.Holes != holes {
					t.Errorf("%s: the tool says %d records, %d checkpoints, %d gaps (median %d, p90 %d, max %d, holes %d); the keys say %d, %d, %d (%d, %d, %d, %d)",
						p.Fp, p.Records, p.Checkpoints, p.Gaps, p.Median, p.P90, p.Max, p.Holes, records, ckpts, len(lengths), median, p90, largest, holes)
				}
				totalCkpts += ckpts
			}
			if totalCkpts == 0 {
				t.Error("no pinned prefix holds a checkpoint: the case proves nothing")
			}

			// The tallies add up to the build's counts, over all prefixes.
			m, err := LoadManifest(dir)
			if err != nil {
				t.Fatal(err)
			}
			tallies, ok, err := LoadTallies(dir)
			if err != nil || !ok {
				t.Fatalf("tallies: %v, %v", ok, err)
			}
			var sum [3]int64
			for _, tl := range tallies {
				for i := range sum {
					sum[i] += tl[i]
				}
			}
			counts := [3]int64{m.Counters["checkpoint.invalidated"], m.Counters["checkpoint.replaced"], m.Counters["checkpoint.replace_capped"]}
			if sum != counts {
				t.Errorf("the tallies add up to %v (invalidated, replaced, capped); the build counted %v", sum, counts)
			}
			if sum[0] == 0 {
				t.Error("no checkpoint was invalidated: the stream no longer reaches what this case is for")
			}
			if strings.HasSuffix(name, "r4") != (sum[1] > 0) {
				t.Errorf("%s put back %d checkpoints", name, sum[1])
			}
			if sum[1] > sum[0] {
				t.Errorf("%d put back of %d deleted", sum[1], sum[0])
			}

			// The report is printed and kept for tools.
			var out bytes.Buffer
			rep.Write(&out, AgeNow, Age1h) // the stream is too short for reads a day back
			for _, want := range []string{"gaps of " + name, "canonical", "holes", "now model", "1h  model", "model against the read: 0 queries differ"} {
				if !strings.Contains(out.String(), want) {
					t.Errorf("the report does not say %q:\n%s", want, out.String())
				}
			}
			path := filepath.Join(t.TempDir(), GapsFile)
			if err := WriteGapsFile(path, rep); err != nil {
				t.Fatal(err)
			}
			b, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var back GapReport
			if err := json.Unmarshal(b, &back); err != nil || len(back.Prefixes) != len(rep.Prefixes) || back.Holes != rep.Holes {
				t.Errorf("the file holds %+v, %v", back, err)
			}
		})
	}
}

func TestTalliesAreWrittenByAnUntimedBuildOnly(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t)
	plan, err := MakePlan(context.Background(), quickSpec(), nil)
	if err != nil {
		t.Fatal(err)
	}
	timed := Guards{Info: untimedGuards.Info}
	for name, tc := range map[string]struct {
		g    Guards
		name string
		file bool
	}{
		"untimed layout L":      {untimedGuards, "L/k8a1", true},
		"untimed without ckpts": {untimedGuards, "L/off", true},
		"untimed layout M":      {untimedGuards, "M/crdb1", false},
		"timed layout L":        {timed, "L/k8a1", false},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, dir := buildAndRead(t, plan, tc.name, tc.g)
			_, ok, err := LoadTallies(dir)
			if err != nil || ok != tc.file {
				t.Errorf("tallies file present %v (%v), want %v", ok, err, tc.file)
			}
			if _, err := os.Stat(filepath.Join(dir, ManifestFile)); err != nil {
				t.Error(err)
			}
		})
	}
}

func TestTalliesFileHoldsHexPrefixesAndOnlyWhatIsCounted(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), TalliesFile)
	in := map[string][3]int64{"\x03\xff\x01": {2, 1, 1}, "\x01\x02": {1, 0, 0}, "\x02": {}}
	if err := WriteTallies(path, in); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"0102":[1,0,0],"03ff01":[2,1,1]}` + "\n"; string(b) != want {
		t.Errorf("the file is %q, want %q: hex prefixes in order, none with nothing counted", b, want)
	}
	got, ok, err := LoadTallies(filepath.Dir(path))
	if err != nil || !ok || !maps.Equal(got, map[string][3]int64{"\x03\xff\x01": {2, 1, 1}, "\x01\x02": {1, 0, 0}}) {
		t.Errorf("read back %v, %v, %v", got, ok, err)
	}
	if _, ok, err := LoadTallies(t.TempDir()); ok || err != nil {
		t.Errorf("a directory without the file: %v, %v", ok, err)
	}
}

func TestGapsRefusesABuildOfAnotherPlanOrCandidate(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t)
	plan, err := MakePlan(context.Background(), quickSpec(), nil)
	if err != nil {
		t.Fatal(err)
	}
	v, dir := buildAndRead(t, plan, "L/k8a1", untimedGuards)
	other, err := candidates.Lookup("L/k8a1r4")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Gaps(plan, other, dir); !errors.Is(err, errMismatch) {
		t.Errorf("another candidate's name over this build: %v", err)
	}
	spec := quickSpec()
	spec.Workload.Seed++
	again, err := MakePlan(context.Background(), spec, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Gaps(again, v, dir); !errors.Is(err, errMismatch) {
		t.Errorf("another plan over this build: %v", err)
	}
	m, err := candidates.Lookup("M/crdb1")
	if err != nil {
		t.Fatal(err)
	}
	_, mdir := buildAndRead(t, plan, m.Name, untimedGuards)
	if _, err := Gaps(plan, m, mdir); err == nil || !strings.Contains(err.Error(), "layout L") {
		t.Errorf("a build of layout M: %v, want an error that says it is not layout L", err)
	}
}
