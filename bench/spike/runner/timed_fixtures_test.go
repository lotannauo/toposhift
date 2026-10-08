package runner_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/spike/workload"
)

// The fixtures write real artifacts of the bench workflow under a temporary directory.
// Every number in them is invented.

const (
	armModel  = "Test ARM A"
	x86ModelA = "Test X86 A"
	x86ModelB = "Test X86 B"
)

var (
	fixtureRevision = strings.Repeat("a", 40)
	armExecutable   = strings.Repeat("1", 64)
	x86Executable   = strings.Repeat("2", 64)
)

// bound is the 99th-percentile bucket bound a build of a hundred batches of d gets.
func bound(d time.Duration) int64 {
	var h runner.Histogram
	for range 100 {
		h.Add(d)
	}
	return h.Quantile(0.99)
}

// makePlan is a plan of the tiny preset with the given stream; it is a function of
// the stream alone.
func makePlan(streamDigest string, records uint64) *runner.Plan {
	spec := runner.DefaultSpec(workload.Tiny())
	specDigest, err := spec.Digest()
	if err != nil {
		panic(err)
	}
	queries, err := runner.QueriesDigest(nil)
	if err != nil {
		panic(err)
	}
	rules, err := runner.DefaultRules().Digest()
	if err != nil {
		panic(err)
	}
	return &runner.Plan{
		Spec: spec, SpecDigest: specDigest, RulesDigest: rules,
		Stream:        runner.StreamInfo{Digest: streamDigest, Records: records},
		QueriesDigest: queries, CacheBytes: 1 << 20,
	}
}

// writePlan saves a plan of the stream under dir/plans/R<window>/plan.json and returns
// it and its digest.
func writePlan(t *testing.T, dir string, window int, streamDigest string, records uint64) (*runner.Plan, string) {
	t.Helper()
	p := makePlan(streamDigest, records)
	return savePlan(t, dir, window, p)
}

func savePlan(t *testing.T, dir string, window int, p *runner.Plan) (*runner.Plan, string) {
	t.Helper()
	d := filepath.Join(dir, "plans", fmt.Sprintf("R%d", window))
	if err := os.MkdirAll(d, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := p.Save(filepath.Join(d, "plan.json")); err != nil {
		t.Fatal(err)
	}
	digest, err := p.Digest()
	if err != nil {
		t.Fatal(err)
	}
	return p, digest
}

// fixtureBuild is one artifact. A zero field takes the default of [fixtureBuild.filled].
type fixtureBuild struct {
	Window        int
	Candidate     string
	Arch          string // aarch64 or x86_64
	CPU           string
	Rep           int
	Run           string
	Family        string
	PinsWindow    int
	Revision      string
	Executable    string
	GoVersion     string
	Commit        time.Duration // every batch took this long
	After, Retain time.Duration // 0: none
	// Excess is how much longer than the median commit one more batch after each
	// retention took; it is the slowness a retention leaves behind, which G2 counts and the
	// retention's own time does not show.
	Excess time.Duration
	// SettleTime is how long each retention waited for the database to be at rest, and
	// DeadlineHits how many of the two retentions reached the deadline.
	SettleTime   time.Duration
	DeadlineHits int
	// NoPost leaves out the batches recorded after each retention. NoBatches leaves the
	// histogram of commits empty, and ZeroBytes the tables without a byte, so that the
	// gates that read them have no value.
	NoPost, NoBatches, ZeroBytes bool
	BytesPerRecord               int64
	CheckpointPct                int64 // percent of the bytes in written as checkpoints
	// Sync, Rest, Canonical and Metrics are the manifest's descriptions; "-" leaves the
	// key out.
	Sync, Rest, Canonical, Metrics string
	// Settle, Deadline and PostBatches are the descriptions settle_tombstones,
	// settle_deadline and post_retention_batches ("true", "2m0s" and "100" by default; "-"
	// leaves the key out).
	Settle, Deadline, PostBatches string
	Options                       string
	// Stream names the stream of the plan (default: one per window), ManifestStream
	// the one the manifest says it holds (default: the plan's).
	Stream, ManifestStream string
	Records                uint64
	Untimed                bool
	Modified               bool
	WrongBeforeCompaction  bool
}

func (b fixtureBuild) filled() fixtureBuild {
	if b.Rep == 0 {
		b.Rep = 1
	}
	if b.Run == "" {
		b.Run = "100"
	}
	if b.Family == "" {
		b.Family = "rmae"
	}
	if b.PinsWindow == 0 {
		b.PinsWindow = 2
	}
	if b.Revision == "" {
		b.Revision = fixtureRevision
	}
	if b.Arch == "" {
		b.Arch = "aarch64"
	}
	if b.Executable == "" {
		b.Executable = armExecutable
		if b.Arch == "x86_64" {
			b.Executable = x86Executable
		}
	}
	if b.GoVersion == "" {
		b.GoVersion = "go1.27.1"
	}
	if b.CPU == "" {
		b.CPU = armModel
		if b.Arch == "x86_64" {
			b.CPU = x86ModelA
		}
	}
	if b.Commit == 0 {
		b.Commit = 10 * time.Millisecond
	}
	if b.BytesPerRecord == 0 {
		b.BytesPerRecord = 40
	}
	for _, p := range []*string{&b.Sync, &b.Rest, &b.Canonical} {
		if *p == "" {
			*p = "false"
		}
	}
	if b.Metrics == "" {
		b.Metrics = "0s"
	}
	if b.Settle == "" {
		b.Settle = "true"
	}
	if b.Deadline == "" {
		b.Deadline = "2m0s"
	}
	if b.PostBatches == "" {
		b.PostBatches = "100"
	}
	if b.SettleTime == 0 {
		b.SettleTime = time.Second
	}
	if b.Options == "" {
		b.Options = "[Options]\n  max_open_files=1000\n"
	}
	if b.Stream == "" {
		b.Stream = fmt.Sprintf("stream-R%d", b.Window)
	}
	if b.Records == 0 {
		b.Records = 1000
	}
	return b
}

func (b fixtureBuild) osName() string {
	if b.Arch == "x86_64" {
		return "ubuntu-24.04"
	}
	return "ubuntu-24.04-arm"
}

func (b fixtureBuild) goarch() string {
	if b.Arch == "x86_64" {
		return "amd64"
	}
	return "arm64"
}

func (b fixtureBuild) planDigest() string {
	d, err := makePlan(b.Stream, b.Records).Digest()
	if err != nil {
		panic(err)
	}
	return d
}

func (b fixtureBuild) manifest() *runner.Manifest {
	var h runner.Histogram
	for range 100 {
		if !b.NoBatches {
			h.Add(b.Commit)
		}
	}
	t := runner.Timing{Writes: h}
	if b.After > 0 {
		t.AfterRetention = []int64{int64(b.After) / 2, int64(b.After)}
	}
	if b.Retain > 0 {
		t.Retains = []int64{int64(b.Retain) / 2, int64(b.Retain)}
		// The batches after each retention: the first, then one that took Excess longer than
		// the median commit, then batches that took the median. G2's value is then the
		// retention plus the first batch's excess plus Excess.
		median := h.Quantile(0.5)
		for i, first := range []int64{int64(b.After) / 2, int64(b.After)} {
			batches := []int64{first, median + int64(b.Excess)}
			for range 5 {
				batches = append(batches, median)
			}
			if b.After == 0 {
				batches = batches[1:]
			}
			if !b.NoPost {
				t.PostRetention = append(t.PostRetention, batches)
			}
			t.RetainPhases = append(t.RetainPhases, runner.RetainPhase{Work: t.Retains[i], Settle: int64(b.SettleTime), DeadlineHit: i < b.DeadlineHits})
		}
	}
	describe := map[string]string{"pebble_options": b.Options}
	for k, v := range map[string]string{
		runner.SyncKey: b.Sync, runner.RestKey: b.Rest, runner.CanonicalKey: b.Canonical, runner.MetricsKey: b.Metrics,
		runner.SettleKey: b.Settle, "settle_deadline": b.Deadline, runner.PostRetentionKey: b.PostBatches,
	} {
		if v != "-" {
			describe[k] = v
		}
	}
	stream := runner.StreamInfo{Digest: b.Stream, Records: b.Records}
	if b.ManifestStream != "" {
		stream.Digest = b.ManifestStream
	}
	m := &runner.Manifest{
		Candidate: b.Candidate, PlanDigest: b.planDigest(), Stream: stream,
		Build: runner.BuildInfo{
			GoVersion: b.GoVersion, GOOS: "linux", GOARCH: b.goarch(), Revision: b.Revision,
			Executable: b.Executable, Modified: b.Modified,
		},
		Untimed:        b.Untimed,
		Describe:       describe,
		Counters:       map[string]int64{},
		StatsBuilt:     map[string]int64{"bytes_in": 1000},
		StatsCompacted: map[string]int64{"live_table_bytes": b.BytesPerRecord * int64(b.Records)},
		Timing:         t,
	}
	if b.CheckpointPct > 0 {
		m.Counters["checkpoint.bytes_written"] = 10 * b.CheckpointPct
	}
	if b.ZeroBytes {
		m.StatsCompacted["live_table_bytes"] = 0
	}
	if b.WrongBeforeCompaction {
		m.UncompactedWrong = []string{"a query"}
	}
	return m
}

func (b fixtureBuild) job() runner.Job {
	sync, metrics := b.Sync, b.Metrics
	if sync == "-" {
		sync = "false"
	}
	if metrics == "-" || metrics == "0s" {
		metrics = ""
	}
	return runner.Job{
		SHA: b.Revision, OS: b.osName(), Arch: b.Arch, Family: b.Family, Window: b.Window, Candidate: b.Candidate,
		Rep: b.Rep, PinsWindow: b.PinsWindow, Sync: sync, MetricsEvery: metrics,
		CPUModel: b.CPU, NProc: 4, MemTotalKB: 16 << 20, ImageOS: "ubuntu24", ImageVersion: "20260101.1.0",
		RunnerName: "Test Runner", Kernel: "6.0.0-test", RunID: b.Run, RunAttempt: "1", BinarySHA256: b.Executable,
	}
}

func writeJSON(t *testing.T, path string, v any) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	data, err := json.MarshalIndent(v, "", " ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}

// writeArtifact writes one artifact of the bench workflow under root and returns its
// directory.
func writeArtifact(t *testing.T, root string, b fixtureBuild) string {
	t.Helper()
	b = b.filled()
	slug := strings.ReplaceAll(b.Candidate, "/", "_")
	dir := filepath.Join(root, fmt.Sprintf("bench-%s-R%d-%s-%s-rep%d-%s", b.Family, b.Window, slug, b.osName(), b.Rep, b.Run))
	writeJSON(t, filepath.Join(dir, runner.ManifestFile), b.manifest())
	writeJSON(t, filepath.Join(dir, runner.JobFile), b.job())
	if err := os.MkdirAll(filepath.Join(dir, "logs"), 0o755); err != nil {
		t.Fatal(err)
	}
	return dir
}

// writeCountersRun writes a candidate directory of a counters run on a plan: a manifest
// and the results read from it.
func writeCountersRun(t *testing.T, root, planDigest, candidate string, mismatches []string) {
	t.Helper()
	m := &runner.Manifest{Candidate: candidate, PlanDigest: planDigest, Build: runner.BuildInfo{GoVersion: "go1.27.1", GOOS: "linux", GOARCH: "arm64"}}
	entries, _ := os.ReadDir(root)
	dir := filepath.Join(root, fmt.Sprintf("%s-%d", strings.ReplaceAll(candidate, "/", "_"), len(entries)))
	writeJSON(t, filepath.Join(dir, runner.ManifestFile), m)
	data, err := os.ReadFile(filepath.Join(dir, runner.ManifestFile))
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	writeJSON(t, filepath.Join(dir, runner.ResultsFile), &runner.Results{
		Candidate: candidate, PlanDigest: planDigest, ManifestDigest: hex.EncodeToString(sum[:]), Mismatches: mismatches,
	})
}

// world is a set of artifacts to write: the builds, the plans and the counters runs.
type world struct {
	Builds []fixtureBuild
	// Counters are the counters runs: the window whose plan they read, the candidate and
	// the answers they got wrong.
	Counters []worldCounters
	// NoPlans leaves the plans of these windows out; PlanRules replaces the rules digest
	// of the plan of a window.
	NoPlans   map[int]bool
	PlanRules map[int]string
	// ExtraPlans are written besides the builds' (window, stream).
	ExtraPlans []worldPlan
}

type worldCounters struct {
	Window     int
	Candidate  string
	Mismatches []string
	Stream     string // the stream of the plan, if not the window's
}

type worldPlan struct {
	Window  int
	Stream  string
	Records uint64
}

// write writes the world under two new directories and returns them.
func (w world) write(t *testing.T) (in, counters string) {
	t.Helper()
	in, counters = t.TempDir(), t.TempDir()
	written := map[string]bool{}
	writePlanOnce := func(window int, stream string, records uint64) {
		if w.NoPlans[window] || written[stream] {
			return
		}
		written[stream] = true
		p := makePlan(stream, records)
		if r, ok := w.PlanRules[window]; ok {
			p.RulesDigest = r
		}
		savePlan(t, filepath.Join(in, fmt.Sprintf("p%d", len(written))), window, p)
	}
	for _, b := range w.Builds {
		b = b.filled()
		writeArtifact(t, in, b)
		writePlanOnce(b.Window, b.Stream, b.Records)
	}
	for _, p := range w.ExtraPlans {
		writePlanOnce(p.Window, p.Stream, p.Records)
	}
	for _, c := range w.Counters {
		stream := c.Stream
		if stream == "" {
			stream = fmt.Sprintf("stream-R%d", c.Window)
		}
		d, err := makePlan(stream, 1000).Digest()
		if err != nil {
			t.Fatal(err)
		}
		writeCountersRun(t, counters, d, c.Candidate, c.Mismatches)
	}
	return in, counters
}

// load writes a world and reads it back as the command does.
func (w world) load(t *testing.T) runner.TimedInputs {
	t.Helper()
	in, counters := w.write(t)
	return loadInputs(t, []string{in}, []string{counters})
}

// memory is the inputs of a world without the files, for the tests that judge many of
// them.
func (w world) memory() runner.TimedInputs {
	var in runner.TimedInputs
	seen := map[string]bool{}
	addPlan := func(window int, stream string, records uint64) {
		p := makePlan(stream, records)
		d, err := p.Digest()
		if err != nil {
			panic(err)
		}
		if w.NoPlans[window] || seen[d] {
			return
		}
		seen[d] = true
		if r, ok := w.PlanRules[window]; ok {
			p.RulesDigest = r
		}
		in.Plans = append(in.Plans, p)
	}
	for _, b := range w.Builds {
		b = b.filled()
		in.Builds = append(in.Builds, runner.TimedBuild{Job: b.job(), Manifest: b.manifest()})
		addPlan(b.Window, b.Stream, b.Records)
	}
	for _, p := range w.ExtraPlans {
		addPlan(p.Window, p.Stream, p.Records)
	}
	for _, c := range w.Counters {
		stream := c.Stream
		if stream == "" {
			stream = fmt.Sprintf("stream-R%d", c.Window)
		}
		d, err := makePlan(stream, 1000).Digest()
		if err != nil {
			panic(err)
		}
		in.Counters = append(in.Counters, &runner.Candidate{
			Manifest: &runner.Manifest{Candidate: c.Candidate, PlanDigest: d},
			Results:  &runner.Results{Candidate: c.Candidate, PlanDigest: d, Mismatches: c.Mismatches},
		})
	}
	return in
}

func loadInputs(t *testing.T, in, counters []string) runner.TimedInputs {
	t.Helper()
	builds, plans, err := runner.LoadTimedBuilds(in)
	if err != nil {
		t.Fatal(err)
	}
	cs, err := runner.LoadCountersRuns(counters)
	if err != nil {
		t.Fatal(err)
	}
	return runner.TimedInputs{Builds: builds, Plans: plans, Counters: cs}
}

// ancestor is a check that finds every revision an ancestor of the ref.
func ancestor(string) runner.RevisionCheck { return runner.RevisionCheck{Ancestor: true} }

func verified() runner.TimingOptions {
	return runner.TimingOptions{Rules: runner.DefaultRules(), Ref: "origin/main", Check: ancestor}
}

func unverified() runner.TimingOptions {
	return runner.TimingOptions{Rules: runner.DefaultRules(), Ref: "origin/main"}
}

// replica has the shape of the first timed run with invented numbers: one window, one
// run, one repetition of each of three candidates on each architecture; the x86_64
// builds of the reference set on two CPU models; counters runs for the reference set
// only.
func replica() world {
	b := func(cand, arch, cpu string, commit, after, retain time.Duration, bytes, ckpt int64) fixtureBuild {
		return fixtureBuild{Window: 7, Candidate: cand, Arch: arch, CPU: cpu, Commit: commit, After: after, Retain: retain, BytesPerRecord: bytes, CheckpointPct: ckpt}
	}
	return world{
		Builds: []fixtureBuild{
			b("L/off", "aarch64", armModel, 10*time.Millisecond, 5*time.Millisecond, 20*time.Second, 40, 0),
			b("L/off", "x86_64", x86ModelA, 15*time.Millisecond, 5*time.Millisecond, 20*time.Second, 40, 0),
			b("L/k64a2l1ns", "aarch64", armModel, 900*time.Millisecond, 6*time.Second, 7*time.Second, 44, 8),
			b("L/k64a2l1ns", "x86_64", x86ModelB, 800*time.Millisecond, 6*time.Second, 7*time.Second, 44, 8),
			b("M/crdb1", "aarch64", armModel, 2*time.Second, 5*time.Second, 6*time.Second, 30, 0),
			b("M/crdb1", "x86_64", x86ModelA, 3*time.Second, 5*time.Second, 6*time.Second, 30, 0),
		},
		Counters: []worldCounters{{Window: 7, Candidate: "L/off"}, {Window: 7, Candidate: "L/k64a2l1ns"}},
	}
}

// full has the candidates of the reference set and the joiner at two windows on both
// architectures, one CPU model each, reps repetitions with small spreads, every
// precondition met and counters runs for all.
func full(reps int) world {
	base := map[string][2]time.Duration{ // commit on aarch64 and x86_64
		"L/off": {10 * time.Millisecond, 15 * time.Millisecond}, "L/k64a2l1ns": {15 * time.Millisecond, 22 * time.Millisecond}, "M/crdb1": {12 * time.Millisecond, 17 * time.Millisecond},
	}
	bytes := map[string]int64{"L/off": 40, "L/k64a2l1ns": 44, "M/crdb1": 42}
	var w world
	for _, window := range []int{2, 7} {
		for _, cand := range []string{"L/off", "L/k64a2l1ns", "M/crdb1"} {
			w.Counters = append(w.Counters, worldCounters{Window: window, Candidate: cand})
			for i, arch := range []string{"aarch64", "x86_64"} {
				for rep := 1; rep <= reps; rep++ {
					b := fixtureBuild{
						Window: window, Candidate: cand, Arch: arch, Rep: rep,
						Commit: base[cand][i] + time.Duration(rep-1)*50*time.Microsecond,
						After:  5*time.Millisecond + time.Duration(rep)*50*time.Microsecond, Retain: time.Duration(20+rep) * time.Second,
						BytesPerRecord: bytes[cand] + int64(rep%2),
					}
					if cand == "L/k64a2l1ns" {
						b.CheckpointPct = 8
					}
					w.Builds = append(w.Builds, b)
				}
			}
		}
	}
	return w
}

// with returns the world with f applied to every build for which keep is true.
func (w world) with(keep func(fixtureBuild) bool, f func(*fixtureBuild)) world {
	out := w
	out.Builds = append([]fixtureBuild(nil), w.Builds...)
	for i := range out.Builds {
		if keep(out.Builds[i]) {
			f(&out.Builds[i])
		}
	}
	return out
}

func every(fixtureBuild) bool { return true }

// without returns the world without the builds for which drop is true.
func (w world) without(drop func(fixtureBuild) bool) world {
	out := w
	out.Builds = nil
	for _, b := range w.Builds {
		if !drop(b) {
			out.Builds = append(out.Builds, b)
		}
	}
	return out
}

func timingText(rep runner.TimingReport) string {
	var sb strings.Builder
	runner.WriteTiming(&sb, rep)
	return sb.String()
}
