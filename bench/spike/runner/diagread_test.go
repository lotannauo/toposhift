package runner_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/runner"
)

// Every manifest, job and metrics file below is synthetic: made from the structs of this
// package and from the diag workflow's job.json as its collect script writes it, never cut
// from an artifact of a real run.

const (
	synthDiagCommit = "0123456789abcdef0123456789abcdef01234567"
	synthDiagRun    = "1000000001"
	synthDiagShape  = "target_file_sizes=67108864,4194304,8388608,67108864,134217728,268435456,536870912 delete_only_compactions=off"
	ms              = time.Millisecond
	sec             = int64(time.Second)
)

// synthDiagBuild is a build the diag reader reads: the store gate's synthetic build with
// the settings of the table-shape comparison (Sync on, a flush threshold of 32,768, a
// window of 7 days paced from day 11.5, and the commit).
func synthDiagBuild(cand string, mods ...func(*runner.StoreGateBuild)) *runner.StoreGateBuild {
	settings := func(b *runner.StoreGateBuild) {
		m := b.Manifest
		m.Build.Revision = synthDiagCommit
		m.Describe[runner.SyncKey] = "true"
		m.Describe[runner.RetainFlushDelsKey] = "32768"
		m.Describe[runner.PaceFromDayKey] = "11.5"
		m.Gate.KeepNs = int64(7 * 24 * time.Hour)
	}
	return synth(cand, append([]func(*runner.StoreGateBuild){settings}, mods...)...)
}

// metricsText is a metrics file with one line for each live heap given.
func metricsText(heaps ...int64) string {
	var sb strings.Builder
	for i, h := range heaps {
		fmt.Fprintf(&sb, `{"elapsed_ms":%d,"phase":"write","batches":%d,"retentions":0,"stats":{"tombstones":1},"go":{"total":9,"heap_live":%d}}`+"\n", (i+1)*10000, i+1, h)
	}
	return sb.String()
}

// windowsOf are the windows of a build whose stream is 1000 s long, paced from its 100th
// second.
func windowsOf(ws ...runner.GateWindow) func(*runner.GateTrace) {
	return func(g *runner.GateTrace) {
		g.Windows = slices.Clone(ws)
		for i := range g.Windows {
			g.Windows[i].ReturnNs = g.Windows[i].StartNs
		}
		g.PaceStartNs = 100 * sec
		g.WriteNs, g.StreamEndNs = 1000*sec, 1000*sec
	}
}

// diagCase is a synthetic build and the live heaps its metrics file holds.
type diagCase struct {
	name  string
	trace func(*runner.GateTrace)
	heaps []int64
}

var diagFigureCases = []diagCase{
	{name: "the store gate's own build", heaps: []int64{gib / 2, gib}},
	{
		// 150 batches inside: the rank of the 99th percentile is the 149th, one past the
		// 148th that a rounded-down rank takes; its bucket's top is not the bucket's
		// bottom, and the longest is another batch.
		name: "a rank across a bucket edge",
		trace: func(g *runner.GateTrace) {
			ds := slices.Repeat([]time.Duration{10 * ms}, 148)
			g.Inside.Batches = 150
			g.Inside.Commit = histOf(append(ds, 40*ms, 90*ms)...)
			g.Outside.Commit = histOf(append(slices.Repeat([]time.Duration{20 * ms}, 1000), 130*ms)...)
		},
		heaps: []int64{gib},
	},
	{
		// Of the windows: an unjudged one that starts before the pacing and rewrites
		// longest of all; two that ended (50 s and 60 s); one that a later retention
		// replaced 100 s after its start; one still rewriting when the stream ended,
		// 300 s after its start. The longest judged rewrite is the 300 s lower bound.
		name: "lower bounds beyond the longest finished rewrite",
		trace: windowsOf(
			runner.GateWindow{StartNs: 10 * sec, RewriteEndNs: 900 * sec, SettleEndNs: 910 * sec},
			runner.GateWindow{StartNs: 150 * sec, RewriteEndNs: 200 * sec, SettleEndNs: 210 * sec},
			runner.GateWindow{StartNs: 300 * sec, RewriteEndNs: 380 * sec, SettleEndNs: 380 * sec, SupersededNs: 400 * sec},
			runner.GateWindow{StartNs: 500 * sec, RewriteEndNs: 560 * sec, SettleEndNs: 570 * sec},
			runner.GateWindow{StartNs: 700 * sec, RewriteEndNs: 1500 * sec, SettleEndNs: 1600 * sec},
		),
		heaps: []int64{gib},
	},
	{
		// The same, with the lower bounds shorter than the longest finished rewrite (60 s).
		name: "lower bounds within the longest finished rewrite",
		trace: windowsOf(
			runner.GateWindow{StartNs: 150 * sec, RewriteEndNs: 210 * sec, SettleEndNs: 220 * sec},
			runner.GateWindow{StartNs: 300 * sec, RewriteEndNs: 380 * sec, SettleEndNs: 380 * sec, SupersededNs: 330 * sec},
			runner.GateWindow{StartNs: 950 * sec, RewriteEndNs: 1500 * sec, SettleEndNs: 1600 * sec},
		),
		heaps: []int64{gib},
	},
	{
		name: "no lower bound",
		trace: windowsOf(
			runner.GateWindow{StartNs: 150 * sec, RewriteEndNs: 190 * sec, SettleEndNs: 200 * sec},
			runner.GateWindow{StartNs: 400 * sec, RewriteEndNs: 470 * sec, SettleEndNs: 480 * sec},
		),
		heaps: []int64{gib},
	},
	{
		// The live heap peaks in the middle of the file, above the resident set the
		// manifest records (2 GiB).
		name:  "a live heap that peaks mid-file above the resident set",
		heaps: []int64{gib / 2, 5 * gib / 2, 11 * gib / 10, 2 * gib},
		trace: func(g *runner.GateTrace) { g.PeakRSS = 3 * gib / 2 },
	},
}

var (
	reQ1 = regexp.MustCompile(`^(\S+) \(control, longest outside windows: (\S+)\)$`)
	reQ2 = regexp.MustCompile(`^(\S+) \(outside: from \S+ to \S+; over (\d+) inside batches\)$`)
	reQ3 = regexp.MustCompile(`^longest rewrite (\S+) over (\d+) passes(?:, and (\d+) passes with only a lower bound, the longest (\S+))? \(`)
)

// checkDiagFigures is nil when the figures of the build equal what the store gate prints
// for Q1 (and its control), Q2 (and its batches), Q3 and the Q5 heap of the same build.
func checkDiagFigures(c diagCase, mut runner.DiagMutant) error {
	mods := []func(*runner.StoreGateBuild){func(b *runner.StoreGateBuild) {
		b.Metrics = runner.StoreGateMetrics{Lines: len(c.heaps), HeapLiveSamples: len(c.heaps), HeapLivePeak: slices.Max(c.heaps)}
	}}
	if c.trace != nil {
		mods = append(mods, trace(c.trace))
	}
	b := synthDiagBuild("Lroot/k64a2l5m", mods...)
	rep := runner.JudgeStoreGate([]*runner.StoreGateBuild{synthDiagBuild("Lroot/off"), b})
	row := func(q string) string {
		for _, r := range rep.Rows {
			if r.Candidate == "Lroot/k64a2l5m" && strings.HasPrefix(r.Gate, q) {
				return r.Value
			}
		}
		return ""
	}
	got, err := runner.DiagFiguresWith(b.Manifest, strings.NewReader(metricsText(c.heaps...)), mut)
	if err != nil {
		return err
	}
	var problems []string
	say := func(format string, args ...any) { problems = append(problems, fmt.Sprintf(format, args...)) }

	if m := reQ1.FindStringSubmatch(row("Q1")); m == nil {
		say("the gate's Q1 reads %q", row("Q1"))
	} else if m[1] != runner.FmtDur(got.Q1Ns) || m[2] != runner.FmtDur(got.OutsideMaxNs) {
		say("Q1 %s with control %s, the gate prints %s with %s", runner.FmtDur(got.Q1Ns), runner.FmtDur(got.OutsideMaxNs), m[1], m[2])
	}
	if m := reQ2.FindStringSubmatch(row("Q2")); m == nil {
		say("the gate's Q2 reads %q", row("Q2"))
	} else if n, _ := strconv.ParseInt(m[2], 10, 64); m[1] != runner.FmtDur(got.Q2UpperNs) || n != got.InsideBatches {
		say("Q2 %s over %d batches, the gate prints %s over %s", runner.FmtDur(got.Q2UpperNs), got.InsideBatches, m[1], m[2])
	}
	if m := reQ3.FindStringSubmatch(row("Q3")); m == nil {
		say("the gate's Q3 reads %q", row("Q3"))
	} else {
		longest, bounded := m[1], 0
		if m[3] != "" {
			bounded, _ = strconv.Atoi(m[3])
			d1, _ := time.ParseDuration(m[1])
			if d2, _ := time.ParseDuration(m[4]); d2 > d1 {
				longest = m[4]
			}
		}
		if runner.FmtDur(got.RNs) != longest || got.RLowerBounds != bounded {
			say("R %s with %d lower bounds, the gate prints %s with %d", runner.FmtDur(got.RNs), got.RLowerBounds, longest, bounded)
		}
	}
	if want := row("Q5 peak live"); runner.FmtMB(got.PeakHeapBytes) != want {
		say("peak heap %s, the gate prints %s", runner.FmtMB(got.PeakHeapBytes), want)
	}
	if len(problems) > 0 {
		return errors.New(strings.Join(problems, "; "))
	}
	return nil
}

// The figures of a build are those the store gate prints for it: Q1 with the longest
// outside, the top of the bucket of Q2's 99th percentile over the same batches, the longest
// judged rewrite with the number of lower bounds (as Q3's line gives them), and the peak
// live heap. Each case is what one mutant of the reader (see
// TestDiagMutantsAreCaught) gets wrong.
func TestSyntheticDiagFiguresEqualWhatTheStoreGatePrints(t *testing.T) {
	t.Parallel()

	for _, c := range diagFigureCases {
		if err := checkDiagFigures(c, runner.DiagMutNone); err != nil {
			t.Errorf("%s: %v", c.name, err)
		}
	}
}

// The numbers behind the comparison above, from the structures and not from the gate's text.
func TestSyntheticDiagFiguresAreTheKnownValues(t *testing.T) {
	t.Parallel()

	figures := func(name string) runner.DiagBuild {
		t.Helper()
		for _, c := range diagFigureCases {
			if c.name != name {
				continue
			}
			b := synthDiagBuild("Lroot/k64a2l5m", trace(c.trace))
			got, err := runner.DiagFigures(b.Manifest, strings.NewReader(metricsText(c.heaps...)))
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			return got
		}
		t.Fatalf("no case %q", name)
		return runner.DiagBuild{}
	}
	edge := figures("a rank across a bucket edge")
	// 40 ms is in the bucket [37748736, 41943040) ns: its top is 41943040.
	if edge.Q1Ns != int64(90*ms) || edge.OutsideMaxNs != int64(130*ms) || edge.Q2UpperNs != 41943040 || edge.InsideBatches != 150 {
		t.Errorf("a rank across a bucket edge: %+v", edge)
	}
	for _, c := range []struct {
		name   string
		r      int64
		bounds int
	}{
		{"lower bounds beyond the longest finished rewrite", 300 * sec, 2},
		{"lower bounds within the longest finished rewrite", 60 * sec, 2},
		{"no lower bound", 70 * sec, 0},
	} {
		if got := figures(c.name); got.RNs != c.r || got.RLowerBounds != c.bounds {
			t.Errorf("%s: R %s with %d lower bounds, want %s with %d", c.name, time.Duration(got.RNs), got.RLowerBounds, time.Duration(c.r), c.bounds)
		}
	}
	if got := figures("a live heap that peaks mid-file above the resident set"); got.PeakHeapBytes != 5*gib/2 {
		t.Errorf("the peak heap is %d", got.PeakHeapBytes)
	}
}

// The inside's quantile is the store's own: the top of the bucket Histogram.Quantile
// finds, for any number of batches, not a value picked at a rank.
func TestSyntheticDiagQ2IsTheStoresBucketTop(t *testing.T) {
	t.Parallel()

	for _, n := range []int{1, 2, 99, 100, 101, 150, 1000} {
		var ds []time.Duration
		for i := range n {
			ds = append(ds, time.Duration(1+i*i)*100*time.Microsecond)
		}
		h := histOf(ds...)
		b := synthDiagBuild("Lroot/k64a2l5m", trace(func(g *runner.GateTrace) { g.Inside.Batches, g.Inside.Commit = int64(n), h }))
		got, err := runner.DiagFigures(b.Manifest, strings.NewReader(metricsText(gib)))
		if err != nil {
			t.Fatal(err)
		}
		if want := h.Quantile(0.99); got.Q2UpperNs != want {
			t.Errorf("%d batches: Q2 %d, the histogram's 99th percentile is %d", n, got.Q2UpperNs, want)
		}
	}
}

// A build the gate would not judge, or that has no figure, gives none.
func TestSyntheticDiagFiguresRefuseWhatHasNoFigure(t *testing.T) {
	t.Parallel()

	ok := metricsText(gib)
	for _, c := range []struct {
		name    string
		mods    []func(*runner.StoreGateBuild)
		metrics string
		want    string
	}{
		{"no gate record", []func(*runner.StoreGateBuild){func(b *runner.StoreGateBuild) { b.Manifest.Gate = nil }}, ok, "no gate record"},
		{"an untimed build", []func(*runner.StoreGateBuild){func(b *runner.StoreGateBuild) { b.Manifest.Untimed = true }}, ok, "untimed"},
		{"a synchronous retention", []func(*runner.StoreGateBuild){func(b *runner.StoreGateBuild) { b.Manifest.Gate.Mode = "sync" }}, ok, "not background"},
		{"not paced", []func(*runner.StoreGateBuild){func(b *runner.StoreGateBuild) {
			delete(b.Manifest.Describe, runner.PaceKey)
			delete(b.Manifest.Describe, runner.PaceFromDayKey)
		}}, ok, "not paced"},
		{"no judged window", []func(*runner.StoreGateBuild){trace(windowsOf(runner.GateWindow{StartNs: 10 * sec, RewriteEndNs: 20 * sec, SettleEndNs: 30 * sec}))}, ok, "no judged window"},
		{"no batch inside", []func(*runner.StoreGateBuild){trace(func(g *runner.GateTrace) { g.Inside = runner.GateBatches{} })}, ok, "no batch was written inside"},
		{"no live heap sample", nil, metricsText(0, 0), "no live heap samples"},
		{"an empty metrics file", nil, "", "no live heap samples"},
		{"a damaged metrics file", nil, ok + "not json\n", "line 2"},
	} {
		b := synthDiagBuild("Lroot/k64a2l5m", c.mods...)
		_, err := runner.DiagFigures(b.Manifest, strings.NewReader(c.metrics))
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v, want an error saying %q", c.name, err, c.want)
		}
	}
	if _, err := runner.DiagFigures(nil, strings.NewReader(ok)); err == nil {
		t.Errorf("no manifest: no error")
	}
}

// diagSpec is one synthetic build of a diag run, as the artifact the diag workflow keeps.
type diagSpec struct {
	os, arch, name string
	rep            int
	cpu, binary    string
	shape          string
	trace          func(*runner.GateTrace)
	heaps          []int64
	job            func(map[string]any)
	manifest       func(*runner.Manifest)
	// omit is a path under the build's directory to leave out; dir is the directory's
	// name where it is not the one the job gives.
	omit, dir string
}

func (s diagSpec) filled() diagSpec {
	if s.os == "" {
		s.os = "ubuntu-24.04-arm"
	}
	if s.arch == "" {
		s.arch = "x86_64"
		if strings.HasSuffix(s.os, "-arm") {
			s.arch = "aarch64"
		}
	}
	if s.name == "" {
		s.name = "control"
	}
	if s.rep == 0 {
		s.rep = 1
	}
	if s.cpu == "" {
		s.cpu = "synthetic cpu " + s.arch
	}
	if s.binary == "" {
		s.binary = strings.Repeat(map[string]string{"aarch64": "a", "x86_64": "b"}[s.arch], 64)
	}
	if s.heaps == nil {
		s.heaps = []int64{gib}
	}
	return s
}

// write makes the build's directory under root and returns its path.
func (s diagSpec) write(t *testing.T, root string) string {
	t.Helper()
	s = s.filled()
	role := "A"
	if s.name != "control" {
		role = "B"
	}
	job := map[string]any{
		"diagnostic": true, "judged": false, "os": s.os, "arch": s.arch, "name": s.name, "role": role, "rep": s.rep,
		"base_sha": synthDiagCommit, "patched": "false", "cpu_model": s.cpu, "run_id": synthDiagRun, "run_attempt": "1",
		"binary_sha256": s.binary, "peak_rss_mib": 2048, "candidate": "Lroot/k64a2l5m", "window": 7,
	}
	if s.job != nil {
		s.job(job)
	}
	mods := []func(*runner.StoreGateBuild){func(b *runner.StoreGateBuild) {
		if s.shape != "" {
			b.Manifest.Describe[runner.ShapeKey] = s.shape
		}
		if s.manifest != nil {
			s.manifest(b.Manifest)
		}
	}}
	if s.trace != nil {
		mods = append(mods, trace(s.trace))
	}
	man, err := json.Marshal(synthDiagBuild("Lroot/k64a2l5m", mods...).Manifest)
	if err != nil {
		t.Fatal(err)
	}
	jobJSON, err := json.Marshal(job)
	if err != nil {
		t.Fatal(err)
	}
	dir := s.dir
	if dir == "" {
		dir = fmt.Sprintf("diag-build-%v-%s-%s-rep%d", job["run_id"], job["os"], job["name"], s.rep)
	}
	dir = filepath.Join(root, dir)
	if err := os.MkdirAll(filepath.Join(dir, "logs"), 0o755); err != nil {
		t.Fatal(err)
	}
	for name, content := range map[string]string{
		"job.json": string(jobJSON), "manifest.json": string(man), "metrics.jsonl": metricsText(s.heaps...),
		filepath.Join("logs", "time.txt"): "synthetic time output\n",
	} {
		if name == s.omit {
			continue
		}
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// validSpecs are the builds of a run that reads: two repetitions of the control and of the
// variant on an Arm runner.
func validSpecs() []diagSpec {
	return []diagSpec{
		{name: "control", rep: 1},
		{name: "control", rep: 2},
		{name: "p5shape", rep: 1, shape: synthDiagShape},
		{name: "p5shape", rep: 2, shape: synthDiagShape},
	}
}

func diagOptions() runner.DiagReadOptions {
	return runner.DiagReadOptions{
		Commit: synthDiagCommit, Run: synthDiagRun,
		Expect: map[string]string{"control": runner.DiagAbsentShape, "p5shape": synthDiagShape},
	}
}

func writeDiagRun(t *testing.T, specs []diagSpec) string {
	t.Helper()
	root := t.TempDir()
	for _, s := range specs {
		s.write(t, root)
	}
	return root
}

// refusalCase breaks one build of a valid run.
type refusalCase struct {
	name string
	// which is the index in validSpecs of the build that is broken (every case breaks one
	// build, or adds one).
	which  int
	mutate func(*diagSpec)
	// extra adds a build to the run instead.
	extra *diagSpec
	opts  func(*runner.DiagReadOptions)
	want  string
}

func jobSet(k string, v any) func(*diagSpec) {
	return func(s *diagSpec) {
		prev := s.job
		s.job = func(j map[string]any) {
			if prev != nil {
				prev(j)
			}
			j[k] = v
		}
	}
}

func manifestSet(f func(*runner.Manifest)) func(*diagSpec) {
	return func(s *diagSpec) { s.manifest = f }
}

func describe(k, v string) func(*diagSpec) {
	return manifestSet(func(m *runner.Manifest) { m.Describe[k] = v })
}

var refusalCases = []refusalCase{
	{name: "a job of another commit", which: 0, mutate: jobSet("base_sha", strings.Repeat("f", 40)), want: "base_sha"},
	{name: "a manifest of another revision", which: 3, mutate: manifestSet(func(m *runner.Manifest) { m.Build.Revision = strings.Repeat("e", 40) }), want: "manifest revision"},
	{name: "a patched build", which: 1, mutate: jobSet("patched", "true"), want: "patched"},
	{name: "a patched field that is not the word false", which: 1, mutate: jobSet("patched", ""), want: "patched"},
	{name: "a second attempt", which: 2, mutate: jobSet("run_attempt", "2"), want: "run_attempt"},
	{name: "a job of another run", which: 0, mutate: func(s *diagSpec) {
		jobSet("run_id", "2000000002")(s)
		s.dir = "diag-build-2000000002-ubuntu-24.04-arm-control-rep1"
	}, want: "run id"},
	{name: "a directory not named for its job", which: 0, mutate: func(s *diagSpec) { s.dir = "diag-build-1000000001-ubuntu-24.04-arm-control-rep9" }, want: "not named for its job"},
	{name: "two binaries on one architecture", which: 1, mutate: func(s *diagSpec) { s.binary = strings.Repeat("c", 64) }, want: "more than one binary"},
	{name: "a binary hash that is not a hash", which: 1, mutate: func(s *diagSpec) { s.binary = "unknown" }, want: "binary_sha256"},
	{
		name: "the same architecture, configuration and repetition twice", which: -1,
		extra: &diagSpec{os: "ubuntu-22.04-arm", arch: "aarch64", name: "control", rep: 1}, want: "two builds",
	},
	{name: "a control with a table shape", which: 0, mutate: func(s *diagSpec) { s.shape = synthDiagShape }, want: "pebble_shape"},
	{name: "a variant without its table shape", which: 2, mutate: func(s *diagSpec) { s.shape = "" }, want: "pebble_shape"},
	{name: "a variant with another table shape", which: 3, mutate: func(s *diagSpec) { s.shape = "delete_only_compactions=off" }, want: "pebble_shape"},
	{name: "a configuration -expect names that no build has", which: -1, opts: func(o *runner.DiagReadOptions) { o.Expect["p5-shape"] = "none" }, want: "-expect names"},
	{name: "a retention that is not in the background", which: 0, mutate: func(s *diagSpec) {
		s.manifest = func(m *runner.Manifest) { m.Gate.Mode = "sync"; m.Describe[runner.RetentionModeKey] = "sync" }
	}, want: "not in the background"},
	{name: "a flush threshold of 24576", which: 2, mutate: describe(runner.RetainFlushDelsKey, "24576"), want: runner.RetainFlushDelsKey},
	{name: "a flush threshold not recorded", which: 2, mutate: manifestSet(func(m *runner.Manifest) { delete(m.Describe, runner.RetainFlushDelsKey) }), want: runner.RetainFlushDelsKey},
	{name: "a pace of 4000", which: 1, mutate: describe(runner.PaceKey, "4000"), want: "pace is"},
	{name: "no pace", which: 1, mutate: manifestSet(func(m *runner.Manifest) { delete(m.Describe, runner.PaceKey) }), want: "pace is"},
	{name: "Sync off", which: 3, mutate: describe(runner.SyncKey, "false"), want: "sync is"},
	{name: "a window of 30 days", which: 0, mutate: manifestSet(func(m *runner.Manifest) {
		m.Gate.KeepNs = int64(30 * 24 * time.Hour)
		m.Describe[runner.PaceFromDayKey] = "46.5"
	}), want: "not 7 days"},
	{name: "a window of 6 days", which: 0, mutate: manifestSet(func(m *runner.Manifest) {
		m.Gate.KeepNs = int64(6 * 24 * time.Hour)
		m.Describe[runner.PaceFromDayKey] = "9.5"
	}), want: "not 7 days"},
	{name: "no job.json", which: 0, mutate: func(s *diagSpec) { s.omit = "job.json" }, want: "job.json is missing"},
	{name: "no manifest.json", which: 1, mutate: func(s *diagSpec) { s.omit = "manifest.json" }, want: "manifest.json is missing"},
	{name: "no metrics.jsonl", which: 2, mutate: func(s *diagSpec) { s.omit = "metrics.jsonl" }, want: "metrics.jsonl is missing"},
	{name: "no logs/time.txt", which: 3, mutate: func(s *diagSpec) { s.omit = filepath.Join("logs", "time.txt") }, want: "time.txt is missing"},
	{name: "no live heap sample", which: 0, mutate: func(s *diagSpec) { s.heaps = []int64{0} }, want: "no live heap samples"},
	{name: "no commit asked for", which: -1, opts: func(o *runner.DiagReadOptions) { o.Commit = "" }, want: "-commit"},
	{name: "a commit of the wrong form", which: -1, opts: func(o *runner.DiagReadOptions) { o.Commit = strings.ToUpper(synthDiagCommit) }, want: "-commit"},
	{name: "a run id that is not digits", which: -1, opts: func(o *runner.DiagReadOptions) { o.Run = "latest" }, want: "-run"},
	{name: "a run other than the one asked for", which: -1, opts: func(o *runner.DiagReadOptions) { o.Run = "1000000002" }, want: "run id"},
	{name: "a commit other than the builds'", which: -1, opts: func(o *runner.DiagReadOptions) { o.Commit = strings.Repeat("9", 40) }, want: "base_sha"},
}

// checkDiagRefusal is nil when the read of the run with the case's break refuses, with the
// words the case gives, and gives no report.
func checkDiagRefusal(t *testing.T, c refusalCase, mut runner.DiagMutant) error {
	t.Helper()
	specs := validSpecs()
	if c.which >= 0 && c.mutate != nil {
		c.mutate(&specs[c.which])
	}
	if c.extra != nil {
		specs = append(specs, *c.extra)
	}
	opts := diagOptions()
	if c.opts != nil {
		c.opts(&opts)
	}
	rep, err := runner.ReadDiagWith(writeDiagRun(t, specs), opts, mut)
	switch {
	case err == nil:
		return fmt.Errorf("%s: read without a refusal", c.name)
	case rep != nil:
		return fmt.Errorf("%s: refused, and gave a report", c.name)
	case !strings.Contains(err.Error(), c.want):
		return fmt.Errorf("%s: refused with %w, which does not say %q", c.name, err, c.want)
	}
	return nil
}

// The run that is broken in none of the ways below reads; each break refuses with nothing
// judged and no report.
func TestSyntheticDiagReadRefusesEachBreak(t *testing.T) {
	t.Parallel()

	if _, err := runner.ReadDiag(writeDiagRun(t, validSpecs()), diagOptions()); err != nil {
		t.Fatalf("the valid run: %v", err)
	}
	for _, c := range refusalCases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			if err := checkDiagRefusal(t, c, runner.DiagMutNone); err != nil {
				t.Error(err)
			}
		})
	}
}

// A refusal names every cause it found, not only the first.
func TestSyntheticDiagReadNamesEveryCause(t *testing.T) {
	t.Parallel()

	specs := validSpecs()
	jobSet("run_attempt", "2")(&specs[0])
	describe(runner.SyncKey, "false")(&specs[2])
	_, err := runner.ReadDiag(writeDiagRun(t, specs), diagOptions())
	if err == nil || !strings.Contains(err.Error(), "run_attempt") || !strings.Contains(err.Error(), "sync is") {
		t.Errorf("%v", err)
	}
}

// An empty directory, and one that holds no build of the diag workflow, are refused; other
// artifacts beside the builds (the plans) are left alone.
func TestSyntheticDiagReadFindsOnlyTheBuilds(t *testing.T) {
	t.Parallel()

	if _, err := runner.ReadDiag(t.TempDir(), diagOptions()); err == nil || !strings.Contains(err.Error(), "holds no") {
		t.Errorf("an empty directory: %v", err)
	}
	if _, err := runner.ReadDiag(filepath.Join(t.TempDir(), "absent"), diagOptions()); err == nil {
		t.Errorf("a directory that is not there: no error")
	}
	root := writeDiagRun(t, validSpecs())
	if err := os.MkdirAll(filepath.Join(root, "diag-plans-"+synthDiagRun), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "notes.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	rep, err := runner.ReadDiag(root, diagOptions())
	if err != nil || len(rep.Builds) != 4 {
		t.Errorf("%d builds, %v", len(rep.Builds), err)
	}
}

// diagFor is a build whose four figures are those given: Q1 and Q2 through the batches
// inside (one batch at the Q1 value among a hundred at the Q2 value), R through the length
// of a single window, and the heap through the largest of its lines.
func diagFor(name string, rep int, q1, q2 time.Duration, r int64, heap int64) diagSpec {
	return diagSpec{
		name: name, rep: rep, heaps: []int64{heap / 3, heap, heap / 2},
		trace: func(g *runner.GateTrace) {
			g.Inside.Batches = 101
			g.Inside.Commit = histOf(append(slices.Repeat([]time.Duration{q2}, 100), q1)...)
			g.Windows = []runner.GateWindow{{StartNs: 0, RewriteEndNs: r, SettleEndNs: r + sec}}
		},
	}
}

// Per architecture, CPU model and configuration the report gives the count, the median
// (the lower of the middle two for an even count), the least and the largest of each
// figure, whatever the order of the builds.
func TestSyntheticDiagGroupsGiveMedianLeastAndLargest(t *testing.T) {
	t.Parallel()

	odd := []diagSpec{
		diagFor("control", 1, 300*ms, 20*ms, 90*sec, 3e9),
		diagFor("control", 2, 100*ms, 40*ms, 30*sec, 1e9),
		diagFor("control", 3, 200*ms, 30*ms, 60*sec, 2e9),
	}
	even := []diagSpec{
		diagFor("p5shape", 1, 400*ms, 30*ms, 20*sec, 4e9),
		diagFor("p5shape", 2, 100*ms, 60*ms, 80*sec, 1e9),
		diagFor("p5shape", 3, 300*ms, 20*ms, 40*sec, 3e9),
		diagFor("p5shape", 4, 200*ms, 50*ms, 60*sec, 2e9),
	}
	for i := range even {
		even[i].shape = synthDiagShape
	}
	rep, err := runner.ReadDiag(writeDiagRun(t, append(odd, even...)), diagOptions())
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Groups) != 2 {
		t.Fatalf("%d groups: %+v", len(rep.Groups), rep.Groups)
	}
	// Q2 values are bucket tops: that of 20 ms is 20971520 ns, of 30 ms 31457280, of 40 ms
	// 41943040, of 50 ms 50331648 and of 60 ms 62914560.
	top := map[time.Duration]int64{20 * ms: 20971520, 30 * ms: 31457280, 40 * ms: 41943040, 50 * ms: 50331648, 60 * ms: 62914560}
	spread := func(med, lo, hi int64) runner.DiagSpread { return runner.DiagSpread{Median: med, Min: lo, Max: hi} }
	for i, want := range []runner.DiagGroup{
		{
			Arch: "aarch64", CPUModel: "synthetic cpu aarch64", Name: "control", N: 3,
			Q1: spread(int64(200*ms), int64(100*ms), int64(300*ms)), Q2: spread(top[30*ms], top[20*ms], top[40*ms]),
			R: spread(60*sec, 30*sec, 90*sec), PeakHeap: spread(2e9, 1e9, 3e9),
		},
		{
			Arch: "aarch64", CPUModel: "synthetic cpu aarch64", Name: "p5shape", N: 4,
			Q1: spread(int64(200*ms), int64(100*ms), int64(400*ms)), Q2: spread(top[30*ms], top[20*ms], top[60*ms]),
			R: spread(40*sec, 20*sec, 80*sec), PeakHeap: spread(2e9, 1e9, 4e9),
		},
	} {
		if got := rep.Groups[i]; got != want {
			t.Errorf("group %d:\n got %+v\nwant %+v", i, got, want)
		}
	}
}

// Builds on different CPU models are different groups, an architecture's binary is one,
// and a runner label whose jobs ran on two models is named.
func TestSyntheticDiagGroupsSeparateCPUModelsAndArchitectures(t *testing.T) {
	t.Parallel()

	specs := []diagSpec{
		{name: "control", rep: 1, cpu: "synthetic model one"},
		{name: "control", rep: 2, cpu: "synthetic model two"},
		{name: "control", rep: 1, os: "ubuntu-24.04"},
		{name: "control", rep: 2, os: "ubuntu-24.04"},
	}
	rep, err := runner.ReadDiag(writeDiagRun(t, specs), runner.DiagReadOptions{Commit: synthDiagCommit, Run: synthDiagRun})
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, g := range rep.Groups {
		got = append(got, fmt.Sprintf("%s/%s/%s/%d", g.Arch, g.CPUModel, g.Name, g.N))
	}
	want := []string{
		"aarch64/synthetic model one/control/1", "aarch64/synthetic model two/control/1", "x86_64/synthetic cpu x86_64/control/2",
	}
	if !slices.Equal(got, want) {
		t.Errorf("groups %v, want %v", got, want)
	}
	if len(rep.Notes) != 1 || !strings.Contains(rep.Notes[0], "ubuntu-24.04-arm ran on 2 CPU models") {
		t.Errorf("notes %v", rep.Notes)
	}
}

// goldenDiagRun is a run of two configurations, two repetitions each, on an Arm runner,
// and one of each on an x86 runner.
func goldenDiagRun(t *testing.T) *runner.DiagReport {
	t.Helper()
	specs := []diagSpec{
		diagFor("control", 1, 140*ms, 20*ms, 52*sec, 4_100_000_000),
		diagFor("control", 2, 120*ms, 21*ms, 50*sec, 4_300_000_000),
		diagFor("p5shape", 1, 90*ms, 15*ms, 40*sec, 4_500_000_123),
		diagFor("p5shape", 2, 95*ms, 16*ms, 41*sec, 4_400_000_000),
		diagFor("control", 1, 200*ms, 30*ms, 70*sec, 5_000_000_000),
		diagFor("p5shape", 1, 210*ms, 31*ms, 71*sec, 5_100_000_000),
	}
	specs[2].shape, specs[3].shape, specs[5].shape = synthDiagShape, synthDiagShape, synthDiagShape
	specs[4].os, specs[5].os = "ubuntu-24.04", "ubuntu-24.04"
	// A replaced pass and one that outlived the stream, to show a lower bound.
	specs[1].trace = func(g *runner.GateTrace) {
		diagFor("", 0, 120*ms, 21*ms, 50*sec, 0).trace(g)
		windowsOf(
			runner.GateWindow{StartNs: 150 * sec, RewriteEndNs: 200 * sec, SettleEndNs: 210 * sec},
			runner.GateWindow{StartNs: 300 * sec, RewriteEndNs: 380 * sec, SettleEndNs: 380 * sec, SupersededNs: 400 * sec},
		)(g)
	}
	rep, err := runner.ReadDiag(writeDiagRun(t, specs), diagOptions())
	if err != nil {
		t.Fatal(err)
	}
	return rep
}

// The text of a report is pinned: the order of the builds and groups, every field and
// the notation of a duration and of the heap.
func TestSyntheticDiagReportTextIsUnchanged(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	runner.WriteDiag(&buf, goldenDiagRun(t))
	checkGolden(t, "testdata/diagread_report.golden", buf.Bytes())
}

// The report's text carries no verdict, limit or ratio of its own.
func TestSyntheticDiagReportJudgesNothing(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	runner.WriteDiag(&buf, goldenDiagRun(t))
	text := strings.ReplaceAll(buf.String(), "no verdict, no limit and no ratio", "")
	for _, word := range []string{"OVER", "verdict", "limit", "ratio", "faster", "slower", "chosen"} {
		if regexp.MustCompile(`\b` + word + `\b`).MatchString(text) {
			t.Errorf("the report says %q:\n%s", word, buf.String())
		}
	}
}

// Each fault of the reader is caught by the check of the test that names it, and the same
// check passes on the honest reader (in the tests above).
func TestDiagMutantsAreCaught(t *testing.T) {
	figuresFail := func(mut runner.DiagMutant, only string) func(t *testing.T) bool {
		return func(t *testing.T) bool {
			t.Helper()
			for _, c := range diagFigureCases {
				if c.name == only {
					err := checkDiagFigures(c, mut)
					t.Logf("%s: %v", c.name, err)
					return err != nil
				}
			}
			t.Fatalf("no case %q", only)
			return false
		}
	}
	refusalFails := func(mut runner.DiagMutant, names ...string) func(t *testing.T) bool {
		return func(t *testing.T) bool {
			t.Helper()
			for _, c := range refusalCases {
				if slices.Contains(names, c.name) && checkDiagRefusal(t, c, mut) == nil {
					return false
				}
			}
			return true
		}
	}
	for _, c := range []struct {
		name     string
		caughtBy string // the test whose check must fail
		mut      runner.DiagMutant
		fails    func(t *testing.T) bool
	}{
		{
			"Q2 read at the bottom of its bucket, as a nearest-rank value sits", "TestSyntheticDiagFiguresEqualWhatTheStoreGatePrints", runner.DiagMutQ2Bottom,
			figuresFail(runner.DiagMutQ2Bottom, "a rank across a bucket edge"),
		},
		{
			"Q2 at the rank rounded down", "TestSyntheticDiagFiguresEqualWhatTheStoreGatePrints", runner.DiagMutQ2FloorRank,
			figuresFail(runner.DiagMutQ2FloorRank, "a rank across a bucket edge"),
		},
		{
			"R without the lower bounds", "TestSyntheticDiagFiguresEqualWhatTheStoreGatePrints", runner.DiagMutRIgnoresBounds,
			figuresFail(runner.DiagMutRIgnoresBounds, "lower bounds beyond the longest finished rewrite"),
		},
		{
			"the heap taken from the resident set", "TestSyntheticDiagFiguresEqualWhatTheStoreGatePrints", runner.DiagMutHeapFromRSS,
			figuresFail(runner.DiagMutHeapFromRSS, "a live heap that peaks mid-file above the resident set"),
		},
		{
			"no check of the commit", "TestSyntheticDiagReadRefusesEachBreak", runner.DiagMutNoCommitCheck,
			refusalFails(runner.DiagMutNoCommitCheck, "a job of another commit", "a manifest of another revision", "a commit other than the builds'"),
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			if !c.fails(t) {
				t.Errorf("the check of %s does not fail on the mutant", c.caughtBy)
			}
		})
	}
}
