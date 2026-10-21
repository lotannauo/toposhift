package runner

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"
)

// The reader of a CI diag run (`spikebench diagread`) puts the figures of each of its
// builds side by side and judges nothing: no verdict, no limit and no ratio. Each figure
// is the store gate's own: it is computed by the functions the gate's rows are, so that a
// build's Q1, Q2, rewrite and peak live heap read here are the ones `storegate` would
// print for it.

// The settings every build of the table-shape comparison was registered with; a build
// that differs is refused.
const (
	diagPaceRate   = StoreGatePaceRate // records per second
	diagFlushDels  = "32768"           // the retention flush threshold
	diagWindowDays = 7
)

// The names the diag workflow gives its artifacts, and the values of its job.json that
// the reader requires.
const (
	diagBuildDirPrefix = "diag-build-"
	diagLogsDir        = "logs"
	diagTimeFile       = "time.txt"
	diagFirstAttempt   = "1"
	diagNotPatched     = "false"
)

// DiagAbsentShape is how a read of a diag run says a manifest has no table shape (Pebble's
// own rule), and how -expect asks for none.
const DiagAbsentShape = "none"

// DiagStatusRefused is the exit status of the command when it refuses: nothing is
// printed.
const DiagStatusRefused = 3

// DiagBuild is what is read of one build's manifest and metrics.
type DiagBuild struct {
	// Q1Ns is the longest writer batch inside a judged retention window, and
	// OutsideMaxNs the longest batch outside every window and settle (the control; 0 with
	// OutsideBatches 0 for none).
	Q1Ns, OutsideMaxNs, OutsideBatches int64
	// Q2UpperNs is the 99th percentile of a batch's commit inside windows as Q2 reads it, the
	// upper bound of its histogram bucket, over InsideBatches batches.
	Q2UpperNs, InsideBatches int64
	// RNs is the longest rewrite over the judged windows (the value Q3 judges: the
	// largest end of the rewrite less the start of its window), a pass that a later
	// retention replaced or that outlived the stream counted at its lower bound, and
	// RLowerBounds is how many such passes there were.
	RNs          int64
	RLowerBounds int
	// PeakHeapBytes is the largest live heap over the lines of the build's metrics file.
	PeakHeapBytes int64
}

// diagMutant is a deliberate fault. Only tests set one: each is the fault a test must
// catch.
type diagMutant int

const (
	diagMutNone diagMutant = iota
	// diagMutQ2Bottom reads Q2 at the bottom of its bucket, as a value at the nearest
	// rank sits, not at the bucket's upper bound.
	diagMutQ2Bottom
	// diagMutQ2FloorRank takes the rank of the 99th percentile rounded down.
	diagMutQ2FloorRank
	// diagMutRIgnoresBounds leaves the lower bounds of replaced and unfinished passes out
	// of R.
	diagMutRIgnoresBounds
	// diagMutHeapFromRSS takes the peak resident set of the build for its peak heap.
	diagMutHeapFromRSS
	// diagMutNoCommitCheck does not compare the commit of a job and a manifest with the
	// commit asked for.
	diagMutNoCommitCheck
)

// DiagFigures reads the figures of one build off its manifest and its metrics file, by
// the functions the store gate judges Q1, Q2, Q3 and Q5 with. It refuses what the gate
// would not judge: a build that is not through the root store, is untimed, has no gate
// record, is not in the background and paced, has no judged window, no batch inside one,
// or no live heap sample.
func DiagFigures(m *Manifest, metrics io.Reader) (DiagBuild, error) {
	return diagFigures(m, metrics, diagMutNone)
}

func diagFigures(m *Manifest, metrics io.Reader, mut diagMutant) (DiagBuild, error) {
	var out DiagBuild
	if m == nil || m.Gate == nil {
		return out, errors.New("runner: the build has no gate record (built by an earlier binary, or not through the root store)")
	}
	b := &StoreGateBuild{Manifest: m}
	if why := b.refusal(); why != "" {
		return out, fmt.Errorf("runner: the store gate would not judge the build: %s", why)
	}
	t := m.Gate
	if t.Mode != "background" {
		return out, fmt.Errorf("runner: the build's retention is %q, not background", t.Mode)
	}
	if rate, _ := b.paceOf(); rate <= 0 {
		return out, errors.New("runner: the build was not paced")
	}
	judged := 0
	for _, w := range t.Windows {
		if t.judged(w) {
			judged++
		}
	}
	switch {
	case judged == 0:
		return out, errors.New("runner: no judged window: no retention was published at or after the day the pacing started")
	case t.Inside.Batches == 0:
		return out, errors.New("runner: no batch was written inside a judged window")
	}
	heap, err := scanStoreGateMetrics(metrics, MetricsFile)
	if err != nil {
		return out, err
	}
	if heap.HeapLiveSamples == 0 {
		return out, errors.New("runner: no live heap samples in the metrics file")
	}

	out.Q1Ns = t.Inside.Commit.MaxNs
	out.OutsideMaxNs, out.OutsideBatches = t.Outside.Commit.MaxNs, t.Outside.Batches
	out.InsideBatches = t.Inside.Batches
	out.Q2UpperNs = t.insideP99()
	switch mut {
	case diagMutQ2Bottom:
		out.Q2UpperNs, _ = quantileBounds(t.Inside.Commit, 0.99)
	case diagMutQ2FloorRank:
		out.Q2UpperNs = floorRankBound(t.Inside.Commit, 0.99)
	}
	q := t.judgedRewrites()
	out.RNs, out.RLowerBounds = q.longest(), q.bounded
	if mut == diagMutRIgnoresBounds {
		out.RNs, out.RLowerBounds = q.rewriteMax, 0
	}
	out.PeakHeapBytes = heap.HeapLivePeak
	if mut == diagMutHeapFromRSS {
		out.PeakHeapBytes = t.PeakRSS
	}
	return out, nil
}

// floorRankBound is the upper bound of the bucket holding the sample at the rank of the
// q-quantile rounded down: the fault of [diagMutQ2FloorRank].
func floorRankBound(h Histogram, q float64) int64 {
	want := min(max(int64(float64(h.Count)*q), 1), h.Count)
	var seen int64
	for b, n := range h.Buckets {
		if seen += n; seen >= want {
			return boundOf(b)
		}
	}
	return h.MaxNs
}

// DiagReadOptions are what a read of a diag run is given.
type DiagReadOptions struct {
	// Commit is the 40-hexadecimal-digit commit every build must be of, and Run the
	// workflow run id.
	Commit, Run string
	// Expect gives, for a configuration name, the table shape its manifests must record
	// ([ShapeKey]): "none" for no shape (Pebble's own rule), otherwise the text.
	Expect map[string]string
}

// diagJob is what the diag workflow's job.json holds that the reader uses.
type diagJob struct {
	OS           string `json:"os"`
	Arch         string `json:"arch"`
	Name         string `json:"name"`
	Role         string `json:"role"`
	Rep          int    `json:"rep"`
	BaseSHA      string `json:"base_sha"`
	Patched      string `json:"patched"`
	CPUModel     string `json:"cpu_model"`
	RunID        string `json:"run_id"`
	RunAttempt   string `json:"run_attempt"`
	BinarySHA256 string `json:"binary_sha256"`
	PeakRSSMiB   int64  `json:"peak_rss_mib"`
}

// DiagReportBuild is one build of the run: what it ran on and its figures.
type DiagReportBuild struct {
	OS, Arch, Name, Role string
	Rep                  int
	CPUModel             string
	BinarySHA256         string
	RunAttempt           string
	// PebbleShape is the manifest's table shape, "none" for a build without one.
	PebbleShape string
	DiagBuild
	// PeakRSSMiB is the peak resident set of the process as GNU time measured it, in MiB
	// of 2^20 bytes: for information only.
	PeakRSSMiB int64
}

// DiagSpread is the median (the lower of the two middle values for an even count), the
// least and the largest of a figure over the builds of a group: in nanoseconds, or in
// bytes for the heap.
type DiagSpread struct {
	Median, Min, Max int64
}

// DiagGroup is the builds of one configuration on one architecture and CPU model.
type DiagGroup struct {
	Arch, CPUModel, Name string
	N                    int
	Q1, Q2, R, PeakHeap  DiagSpread
	// RLowerBoundBuilds is how many of the builds have a lower bound among the passes
	// of their R.
	RLowerBoundBuilds int
}

// DiagReport is a read of a diag run.
type DiagReport struct {
	Commit, Run string
	Builds      []DiagReportBuild
	Groups      []DiagGroup
	// Notes say what a reader of the groups must know: a runner label whose jobs ran on
	// more than one CPU model.
	Notes []string
}

// ReadDiag reads the builds under root: each subdirectory named
// diag-build-<run>-<os>-<name>-rep<n>, with its job.json, manifest.json, metrics.jsonl
// and logs/time.txt. It refuses, naming every cause it found and judging nothing, when:
// a job's base_sha or its manifest's revision is not Commit; a job was patched or is not
// the first attempt of its run; the jobs are of more than one run, or of a run other than
// Run, or an architecture has more than one binary; two builds are of the same
// architecture, configuration and repetition; a manifest's table shape is not the one
// expected for its configuration; a manifest is not background retention paced at 4,500
// records a second with a flush threshold of 32,768, Sync on and a 7-day window; or a
// file is missing or does not read.
func ReadDiag(root string, o DiagReadOptions) (*DiagReport, error) {
	return readDiag(root, o, diagMutNone)
}

func readDiag(root string, o DiagReadOptions, mut diagMutant) (*DiagReport, error) {
	switch {
	case !hex40.MatchString(o.Commit):
		return nil, errors.New("runner: -commit: not 40 lowercase hexadecimal digits")
	case !digits.MatchString(o.Run):
		return nil, errors.New("runner: -run: not a workflow run id (decimal digits)")
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	var problems []string
	problem := func(format string, args ...any) { problems = append(problems, fmt.Sprintf(format, args...)) }
	var builds []DiagReportBuild
	for _, e := range entries {
		if !e.IsDir() || !strings.HasPrefix(e.Name(), diagBuildDirPrefix) {
			continue
		}
		b, ok := readDiagBuild(filepath.Join(root, e.Name()), e.Name(), o, mut, problem)
		if ok {
			builds = append(builds, b)
		}
	}
	if len(builds) == 0 && len(problems) == 0 {
		problem("%s holds no %s* directory", root, diagBuildDirPrefix)
	}
	// A configuration -expect names that no build has is a mistyped name, not a check
	// that passed; it is said once every build was read.
	if len(problems) == 0 {
		named := map[string]bool{}
		for _, b := range builds {
			named[b.Name] = true
		}
		for _, name := range slices.Sorted(maps.Keys(o.Expect)) {
			if !named[name] {
				problem("-expect names the configuration %q, which no build has", name)
			}
		}
	}

	// What must be one across the builds.
	type key struct {
		arch, name string
		rep        int
	}
	seen := map[key]bool{}
	binaries := map[string]map[string]bool{}
	for _, b := range builds {
		k := key{b.Arch, b.Name, b.Rep}
		if seen[k] {
			problem("two builds of architecture %s, configuration %s and repetition %d", b.Arch, b.Name, b.Rep)
		}
		seen[k] = true
		if binaries[b.Arch] == nil {
			binaries[b.Arch] = map[string]bool{}
		}
		binaries[b.Arch][b.BinarySHA256] = true
	}
	for _, arch := range slices.Sorted(maps.Keys(binaries)) {
		if len(binaries[arch]) > 1 {
			problem("more than one binary on %s: %s", arch, strings.Join(slices.Sorted(maps.Keys(binaries[arch])), ", "))
		}
	}
	if len(problems) > 0 {
		return nil, fmt.Errorf("runner: the diag run is not read, and nothing is judged: %s", strings.Join(problems, "; "))
	}

	slices.SortFunc(builds, func(a, b DiagReportBuild) int {
		return cmp.Or(strings.Compare(a.Arch, b.Arch), strings.Compare(a.Role, b.Role), strings.Compare(a.Name, b.Name),
			strings.Compare(a.OS, b.OS), cmp.Compare(a.Rep, b.Rep))
	})
	rep := &DiagReport{Commit: o.Commit, Run: o.Run, Builds: builds}
	rep.Groups = diagGroups(builds)
	rep.Notes = diagNotes(builds)
	return rep, nil
}

// readDiagBuild reads one build's directory, giving each cause it refuses for to problem.
// ok is false if no figures could be read.
func readDiagBuild(dir, name string, o DiagReadOptions, mut diagMutant, problem func(string, ...any)) (b DiagReportBuild, ok bool) {
	file := func(parts ...string) (string, bool) {
		p := filepath.Join(append([]string{dir}, parts...)...)
		st, err := os.Stat(p)
		if err != nil || !st.Mode().IsRegular() {
			problem("%s: %s is missing", name, filepath.Join(parts...))
			return p, false
		}
		return p, true
	}
	jobPath, okJob := file(JobFile)
	_, okMan := file(ManifestFile)
	metPath, okMet := file(MetricsFile)
	_, okTime := file(diagLogsDir, diagTimeFile)
	if !okJob || !okMan || !okMet || !okTime {
		return b, false
	}
	var j diagJob
	raw, err := os.ReadFile(jobPath)
	if err == nil {
		err = json.Unmarshal(raw, &j)
	}
	if err != nil {
		problem("%s: %s: %v", name, JobFile, err)
		return b, false
	}
	m, _, err := readManifest(dir)
	if err != nil {
		problem("%s: %v", name, err)
		return b, false
	}

	good := true
	bad := func(format string, args ...any) {
		good = false
		problem("%s: %s", name, fmt.Sprintf(format, args...))
	}
	if j.OS == "" || j.Arch == "" || j.Name == "" || j.Role == "" || j.CPUModel == "" {
		bad("job.json lacks its os, arch, name, role or cpu_model")
	}
	if !hex64.MatchString(j.BinarySHA256) {
		bad("binary_sha256 %q is not 64 hexadecimal digits", j.BinarySHA256)
	}
	if want := fmt.Sprintf("%s%s-%s-%s-rep%d", diagBuildDirPrefix, j.RunID, j.OS, j.Name, j.Rep); name != want {
		bad("the directory is not named for its job (%s)", want)
	}
	if j.RunID != o.Run {
		bad("job of run id %q, not %q", j.RunID, o.Run)
	}
	if mut != diagMutNoCommitCheck {
		if j.BaseSHA != o.Commit {
			bad("job base_sha %q is not the commit %s", j.BaseSHA, o.Commit)
		}
		if r := m.Build.Revision; r != o.Commit {
			bad("manifest revision %q is not the commit %s", r, o.Commit)
		}
	}
	if j.Patched != diagNotPatched {
		bad("patched is %q, not %q", j.Patched, diagNotPatched)
	}
	if j.RunAttempt != diagFirstAttempt {
		bad("run_attempt is %q, not %s", j.RunAttempt, diagFirstAttempt)
	}

	shape := describedOr(m, ShapeKey, DiagAbsentShape)
	if want, asked := o.Expect[j.Name]; asked && shape != want {
		bad("%s is %q, not the %q expected for configuration %s", ShapeKey, shape, want, j.Name)
	}
	g := m.Gate
	switch {
	case g == nil || g.Mode != "background" || (&StoreGateBuild{Manifest: m}).retentionModeOf() != "background":
		bad("the retention is not in the background")
	default:
		if v := describedOr(m, RetainFlushDelsKey, "not recorded"); v != diagFlushDels {
			bad("%s is %q, not %s", RetainFlushDelsKey, v, diagFlushDels)
		}
		if v := describedOr(m, PaceKey, "0"); v != strconv.Itoa(diagPaceRate) {
			bad("pace is %q, not %d", v, diagPaceRate)
		}
		if v := describedOr(m, SyncKey, "false"); v != "true" {
			bad("sync is %q, not true", v)
		}
		if days, ok := g.window(); !ok || days != diagWindowDays {
			bad("the window is %s, not %d days", time.Duration(g.KeepNs), diagWindowDays)
		}
	}
	if !good {
		return b, false
	}
	mf, err := os.Open(metPath)
	if err != nil {
		bad("%v", err)
		return b, false
	}
	defer mf.Close()
	figures, err := diagFigures(m, mf, mut)
	if err != nil {
		bad("%v", err)
		return b, false
	}
	return DiagReportBuild{
		OS: j.OS, Arch: j.Arch, Name: j.Name, Role: j.Role, Rep: j.Rep, CPUModel: j.CPUModel,
		BinarySHA256: j.BinarySHA256, RunAttempt: j.RunAttempt, PebbleShape: shape,
		DiagBuild: figures, PeakRSSMiB: j.PeakRSSMiB,
	}, true
}

// diagGroups gathers the builds by architecture, CPU model and configuration, in the
// order of the first of each among the sorted builds.
func diagGroups(builds []DiagReportBuild) []DiagGroup {
	type key struct{ arch, cpu, name string }
	var order []key
	by := map[key][]DiagReportBuild{}
	for _, b := range builds {
		k := key{b.Arch, b.CPUModel, b.Name}
		if _, ok := by[k]; !ok {
			order = append(order, k)
		}
		by[k] = append(by[k], b)
	}
	var out []DiagGroup
	for _, k := range order {
		bs := by[k]
		pick := func(f func(DiagReportBuild) int64) DiagSpread {
			xs := make([]int64, len(bs))
			for i, b := range bs {
				xs[i] = f(b)
			}
			return DiagSpread{Median: lowerMedian(xs), Min: slices.Min(xs), Max: slices.Max(xs)}
		}
		g := DiagGroup{
			Arch: k.arch, CPUModel: k.cpu, Name: k.name, N: len(bs),
			Q1:       pick(func(b DiagReportBuild) int64 { return b.Q1Ns }),
			Q2:       pick(func(b DiagReportBuild) int64 { return b.Q2UpperNs }),
			R:        pick(func(b DiagReportBuild) int64 { return b.RNs }),
			PeakHeap: pick(func(b DiagReportBuild) int64 { return b.PeakHeapBytes }),
		}
		for _, b := range bs {
			if b.RLowerBounds > 0 {
				g.RLowerBoundBuilds++
			}
		}
		out = append(out, g)
	}
	return out
}

// diagNotes names each runner label whose jobs ran on more than one CPU model.
func diagNotes(builds []DiagReportBuild) []string {
	models := map[string]map[string]bool{}
	for _, b := range builds {
		if models[b.OS] == nil {
			models[b.OS] = map[string]bool{}
		}
		models[b.OS][b.CPUModel] = true
	}
	var notes []string
	for _, label := range slices.Sorted(maps.Keys(models)) {
		if len(models[label]) > 1 {
			notes = append(notes, fmt.Sprintf("runner label %s ran on %d CPU models (%s): read each model apart", label, len(models[label]), strings.Join(slices.Sorted(maps.Keys(models[label])), "; ")))
		}
	}
	return notes
}

// WriteDiag prints the report: every build, then each group's median, least and largest.
// A duration is exact (the nanoseconds, in Go's notation) and the heap is in MB of 10^6
// bytes.
func WriteDiag(w io.Writer, rep *DiagReport) {
	fmt.Fprintf(w, "diag run %s, commit %s, %d builds\n", rep.Run, rep.Commit, len(rep.Builds))
	fmt.Fprintln(w, "figures only: no verdict, no limit and no ratio; a duration is exact, the heap is in MB of 10^6 bytes")
	for _, n := range rep.Notes {
		fmt.Fprintf(w, "note: %s\n", n)
	}
	for _, b := range rep.Builds {
		fmt.Fprintf(w, "\nbuild os=%s arch=%s name=%s role=%s rep=%d\n", b.OS, b.Arch, b.Name, b.Role, b.Rep)
		fmt.Fprintf(w, "  cpu_model=%q\n  binary_sha256=%s\n  run_attempt=%s pebble_shape=%q\n", b.CPUModel, b.BinarySHA256, b.RunAttempt, b.PebbleShape)
		outside := "no batch outside"
		if b.OutsideBatches > 0 {
			outside = fmt.Sprintf("outside maximum %s over %d batches", time.Duration(b.OutsideMaxNs), b.OutsideBatches)
		}
		fmt.Fprintf(w, "  Q1 %s (%s)\n", time.Duration(b.Q1Ns), outside)
		fmt.Fprintf(w, "  Q2 %s over %d inside batches\n", time.Duration(b.Q2UpperNs), b.InsideBatches)
		fmt.Fprintf(w, "  R %s (%d passes counted at a lower bound)\n", time.Duration(b.RNs), b.RLowerBounds)
		fmt.Fprintf(w, "  peak heap %s MB\n  peak_rss_mib %d (information only)\n", diagMB(b.PeakHeapBytes), b.PeakRSSMiB)
	}
	for _, g := range rep.Groups {
		fmt.Fprintf(w, "\ngroup arch=%s cpu_model=%q name=%s n=%d\n", g.Arch, g.CPUModel, g.Name, g.N)
		dur := func(s DiagSpread) string {
			return fmt.Sprintf("median %s min %s max %s", time.Duration(s.Median), time.Duration(s.Min), time.Duration(s.Max))
		}
		fmt.Fprintf(w, "  Q1 %s\n  Q2 %s\n", dur(g.Q1), dur(g.Q2))
		fmt.Fprintf(w, "  R %s (a lower bound among the passes of %d of %d builds)\n", dur(g.R), g.RLowerBoundBuilds, g.N)
		fmt.Fprintf(w, "  peak heap median %s MB min %s MB max %s MB\n", diagMB(g.PeakHeap.Median), diagMB(g.PeakHeap.Min), diagMB(g.PeakHeap.Max))
	}
}

// diagMB is a count of bytes in MB of 10^6 bytes, to the kilobyte.
func diagMB(n int64) string { return fmt.Sprintf("%.3f", float64(n)/1e6) }
