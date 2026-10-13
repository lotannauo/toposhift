package runner_test

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/candidates"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/internal/store"
)

// The threshold reaches the root store through the candidate and its engine: a negative
// one, which the store refuses at its Open, is refused for both root-store candidates.
func TestTheThresholdReachesTheRootStore(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"Lroot/off", "Lroot/k64a2l1ns"} {
		e, err := lookup(t, name).Open(t.TempDir(), candidates.Options{RetentionMode: candidates.RetentionBackground, RetainFlushDels: -1})
		if err == nil {
			_ = e.Close()
		}
		if !errors.Is(err, store.ErrInvalid) {
			t.Errorf("%s with a threshold of -1: %v, want the store's refusal", name, err)
		}
	}
}

// A build through the root store records the retention flush threshold its store used:
// the one it was given, or the default's number for none. A threshold is refused before anything is built for a candidate that is not
// through the root store and for a retention that is synchronous.
func TestABuildRecordsItsRetentionFlushThreshold(t *testing.T) {
	t.Parallel()

	plan := mustPlan(t, tinySpec())
	v := lookup(t, "Lroot/k64a2l1ns")
	build := func(n int) *runner.Manifest {
		t.Helper()
		m, err := runner.BuildWith(context.Background(), plan, v, runner.CandidateDir(t.TempDir(), v.Name), clean,
			runner.BuildOptions{RetentionMode: candidates.RetentionBackground, RetainFlushDels: n}, nil)
		if err != nil {
			t.Fatal(err)
		}
		return m
	}
	const threshold = 8
	m := build(threshold)
	if got := m.Describe[runner.RetainFlushDelsKey]; got != "8" {
		t.Errorf("a build at a threshold of 8 records %q", got)
	}
	// A pass looks at its unflushed deletions between its chunks, and the tiny plan's
	// short passes start no flush even at a threshold of 8; the store's own tests show
	// that a pass flushes at the threshold this field gives. The judged part's flushes
	// and stalls are recorded all the same, the stalls from the engine's counter.
	if c := m.Gate.JudgedCounts; c == nil || c["retain.flushes"] > m.Counters["retain.flushes"] {
		t.Errorf("an unpaced build's judged flushes are %v, and it counted %d", c, m.Counters["retain.flushes"])
	}
	if !m.Gate.StallsRead {
		t.Error("the build did not read its write stalls")
	}

	m = build(0)
	if got := m.Describe[runner.RetainFlushDelsKey]; got != "32768" {
		t.Errorf("a build at the default records %q, want the default's number", got)
	}

	for _, c := range []struct {
		name string
		cand string
		mode string
		want string
	}{
		{"a candidate not through the root store", "L/k64a2l1ns", candidates.RetentionBackground, "not built through the root store"},
		{"a synchronous retention", "Lroot/k64a2l1ns", candidates.RetentionSync, "only a build in the background mode"},
		{"the store's own mode", "Lroot/k64a2l1ns", "", "only a build in the background mode"},
	} {
		dir := runner.CandidateDir(t.TempDir(), c.cand)
		_, err := runner.BuildWith(context.Background(), plan, lookup(t, c.cand), dir, clean,
			runner.BuildOptions{RetentionMode: c.mode, RetainFlushDels: 24576}, nil)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v, want a refusal saying %q", c.name, err, c.want)
		}
		if _, err := os.Stat(dir); err == nil {
			t.Errorf("%s: the build made %s before it was refused", c.name, dir)
		}
	}
	if _, err := runner.BuildWith(context.Background(), plan, v, runner.CandidateDir(t.TempDir(), v.Name), clean,
		runner.BuildOptions{RetentionMode: candidates.RetentionBackground, RetainFlushDels: -1}, nil); err == nil {
		t.Error("a negative threshold was built")
	}
}

// The gate pools the repetitions of a candidate only when they are alike in where they
// came from as in their settings: one retention flush threshold (a build that does not
// record one is not taken for the default), one plan, and either all local or all of one
// workflow run's first attempt.
func TestTheGatePoolsOnlyBuildsOfOneThresholdAndOneOrigin(t *testing.T) {
	t.Parallel()

	rep := func(n int, mods ...func(*runner.StoreGateBuild)) *runner.StoreGateBuild {
		return synth("Lroot/k64a2l1ns", append([]func(*runner.StoreGateBuild){func(b *runner.StoreGateBuild) {
			b.Dir += "/rep" + string(rune('0'+n))
			b.Job = &runner.Job{RunID: "7", RunAttempt: "1", Rep: n}
			b.Manifest.Describe[runner.RetainFlushDelsKey] = "32768"
		}}, mods...)...)
	}
	threshold := func(v string) func(*runner.StoreGateBuild) {
		return func(b *runner.StoreGateBuild) {
			if v == "" {
				delete(b.Manifest.Describe, runner.RetainFlushDelsKey)
				return
			}
			b.Manifest.Describe[runner.RetainFlushDelsKey] = v
		}
	}
	if err := runner.CheckStoreGateSettings([]*runner.StoreGateBuild{rep(1), rep(2), rep(3)}); err != nil {
		t.Fatalf("three repetitions alike: %v", err)
	}
	for _, c := range []struct {
		name   string
		builds []*runner.StoreGateBuild
		want   []string
	}{
		{
			"two thresholds",
			[]*runner.StoreGateBuild{rep(1, threshold("16384")), rep(2, threshold("24576"))},
			[]string{"different retain_flush_dels", `retain_flush_dels="16384"`, `retain_flush_dels="24576"`},
		},
		{
			"a threshold not recorded beside the default's",
			[]*runner.StoreGateBuild{rep(1, threshold("")), rep(2)},
			[]string{`retain_flush_dels="not recorded"`, `retain_flush_dels="32768"`},
		},
		{
			"a re-run attempt",
			[]*runner.StoreGateBuild{rep(1), rep(2, func(b *runner.StoreGateBuild) { b.Job.RunAttempt = "2" })},
			[]string{`attempt "2"`, "only a first attempt is judged"},
		},
		{
			"two workflow runs",
			[]*runner.StoreGateBuild{rep(1), rep(2, func(b *runner.StoreGateBuild) { b.Job.RunID = "8" })},
			[]string{"different workflow runs", `run "7"`, `run "8"`},
		},
		{
			"a workflow build and a local one",
			[]*runner.StoreGateBuild{rep(1), rep(2, func(b *runner.StoreGateBuild) { b.Job = nil })},
			[]string{"mix builds of the bench workflow"},
		},
		{
			"two plans",
			[]*runner.StoreGateBuild{rep(1), rep(2, func(b *runner.StoreGateBuild) { b.Manifest.PlanDigest = "other" })},
			[]string{"different plans", `plan "other"`},
		},
	} {
		err := runner.CheckStoreGateSettings(c.builds)
		if err == nil {
			t.Errorf("%s: pooled", c.name)
			continue
		}
		for _, w := range c.want {
			if !strings.Contains(err.Error(), w) {
				t.Errorf("%s: %q does not say %q", c.name, err, w)
			}
		}
		if judged := runner.JudgeStoreGate(c.builds); judged.Refusal == "" || len(judged.Rows) != 0 {
			t.Errorf("%s: judged %+v", c.name, judged.Rows)
		}
	}
	// local builds alike are pooled
	local := func(b *runner.StoreGateBuild) { b.Job = nil }
	if err := runner.CheckStoreGateSettings([]*runner.StoreGateBuild{rep(1, local), rep(2, local)}); err != nil {
		t.Errorf("two local builds: %v", err)
	}
}

// A series of paced builds in the background is judged only as the builds of one
// commit: without -commit they are refused, and with it every build's manifest revision,
// and the commit of its job if it has one, must be that commit. A local build has no
// job and is checked by its revision alone. Builds that were not paced need no commit.
func TestTheGateJudgesPacedBuildsOfOneCommit(t *testing.T) {
	t.Parallel()

	const commit = "89abcdef0123456789abcdef0123456789abcdef"
	of := func(rev string, job bool, jobSHA string) *runner.StoreGateBuild {
		return synth("Lroot/k64a2l1ns", func(b *runner.StoreGateBuild) {
			b.Manifest.Build.Revision = rev
			if job {
				b.Job = &runner.Job{SHA: jobSHA, RunID: "7", RunAttempt: "1", Rep: 1}
			}
		})
	}
	if err := runner.CheckStoreGateCommit([]*runner.StoreGateBuild{of(commit, true, commit)}, ""); err == nil || !strings.Contains(err.Error(), "-commit") {
		t.Errorf("paced builds without -commit: %v", err)
	}
	if err := runner.CheckStoreGateCommit([]*runner.StoreGateBuild{of(commit, true, commit), of(commit, false, "")}, commit); err != nil {
		t.Errorf("builds of the commit, one local: %v", err)
	}
	for _, c := range []struct {
		name  string
		build *runner.StoreGateBuild
		want  string
	}{
		{"another revision, local", of(strings.Repeat("1", 40), false, ""), "was built from revision"},
		{"another revision, workflow", of(strings.Repeat("1", 40), true, commit), "was built from revision"},
		{"a job of another commit", of(commit, true, strings.Repeat("2", 40)), "has a job file of commit"},
	} {
		if err := runner.CheckStoreGateCommit([]*runner.StoreGateBuild{c.build}, commit); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v, want %q", c.name, err, c.want)
		}
	}
	if err := runner.CheckStoreGateCommit([]*runner.StoreGateBuild{of(commit, false, "")}, "89abcdef"); err == nil {
		t.Error("a commit of 8 digits was taken")
	}
	if err := runner.CheckStoreGateCommit([]*runner.StoreGateBuild{synthSync("Lroot/k64a2l1ns")}, ""); err != nil {
		t.Errorf("a synchronous build without -commit: %v", err)
	}
}

// The report gives each build's threshold and, apart, the figures of its judged part and
// of the rest, and, for a retaining build in the background, the retention-chunk
// criteria, each met or not; none of it changes a row. A build recorded before the
// figures says so, and the Lroot/off baseline gets no criteria.
func TestTheReportGivesTheJudgedPartAndTheCriteria(t *testing.T) {
	t.Parallel()

	ms := int64(time.Millisecond)
	figures := func(b *runner.StoreGateBuild) {
		b.Manifest.Describe[runner.RetainFlushDelsKey] = "16384"
		g := b.Manifest.Gate
		g.JudgedChunks = map[string]runner.GateSpread{
			"retain.chunk_open_ns":  {Count: 40, Min: ms, P50: 2 * ms, P90: 9 * ms, P99: 12 * ms, Max: 13 * ms, Sum: 120 * ms, Over: 4},
			"retain.chunk_hold_ns":  {Count: 40, Min: 10 * ms, P50: 22 * ms, P90: 26 * ms, P99: 29 * ms, Max: 30 * ms, Sum: 880 * ms},
			"retain.chunk_prefixes": {Count: 40, Min: 3, P50: 120, P50Low: 112, P90: 300, P99: 400, Max: 410, Sum: 6000},
		}
		g.UnjudgedChunks = map[string]runner.GateSpread{"retain.chunk_open_ns": {Count: 2, Min: ms, P50: ms, P90: ms, P99: ms, Max: ms, Sum: 2 * ms}}
		g.JudgedCounts = map[string]int64{"retain.flushes": 5, "retain.flush_deferred": 0, "retain.flush_errors": 0}
		g.StallsAtPaceStart, g.StallsAtStreamEnd, g.StallsRead = 2, 2, true
		b.Manifest.Counters = map[string]int64{"retain.flushes": 7, "retain.flush_deferred": 1}
		b.Metrics.Lines, b.Metrics.JudgedLines, b.Metrics.JudgedL0SublevelsPeak, b.Metrics.UnjudgedL0SublevelsPeak = 9, 6, 8, 11
	}
	recorded := runner.JudgeStoreGate([]*runner.StoreGateBuild{synth("Lroot/k64a2l1ns", figures)})
	info := strings.Join(recorded.Info, "\n")
	for _, want := range []string{
		"retain_flush_dels 16384",
		"the judged part, the passes published from the start of the pacing",
		"1 of 1 passes",
		"retain.chunk_open_ns count 40, min 1ms, p50 2ms, p90 9ms, p99 12ms, max 13ms, mean 3ms, over 10ms: 4 of 40 (10.00%)",
		"retain.chunk_hold_ns count 40, min 10ms, p50 22ms, p90 26ms, p99 29ms, max 30ms, mean 22ms",
		"retain.chunk_prefixes count 40, min 3, p50 120, p90 300, p99 400, max 410, mean 150.0",
		"flushes: retain.flushes 5, retain.flush_deferred 0, retain.flush_errors 0; write stalls 0; level 0 sublevels: peak 8 over 6 metrics lines",
		"the rest of the build",
		"flushes: retain.flushes 2, retain.flush_deferred 1, retain.flush_errors 0; write stalls before the pacing 2; level 0 sublevels: peak 11 over 3 metrics lines",
		"C1 met (4 of 40 judged chunks opened in more than 10ms, at most 10%)",
		"C2 met", "C3 met (the median prefixes, at the bottom of its bucket, 112, at least 100)", "C4 met", "C5 met",
		"the build meets the criteria",
	} {
		if !strings.Contains(info, want) {
			t.Errorf("the report lacks %q:\n%s", want, info)
		}
	}
	bare := runner.JudgeStoreGate([]*runner.StoreGateBuild{synth("Lroot/k64a2l1ns")})
	info = strings.Join(bare.Info, "\n")
	for _, want := range []string{"retain_flush_dels not recorded", "the figures of the judged part not recorded"} {
		if !strings.Contains(info, want) {
			t.Errorf("the report of a build before the figures lacks %q:\n%s", want, info)
		}
	}
	for _, q := range []string{"Q1", "Q2", "Q3", "Q4", "Q5"} {
		if a, b := find(t, recorded, "Lroot/k64a2l1ns", q), find(t, bare, "Lroot/k64a2l1ns", q); a.State != b.State {
			t.Errorf("%s is %v with the figures and %v without", q, a.State, b.State)
		}
	}
	off := runner.JudgeStoreGate([]*runner.StoreGateBuild{synth("Lroot/off", figures)})
	if info := strings.Join(off.Info, "\n"); strings.Contains(info, "retention-chunk criteria") || !strings.Contains(info, "the judged part") {
		t.Errorf("the baseline's report:\n%s", info)
	}
}
