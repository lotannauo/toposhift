package runner

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"
)

// JobFile is the file the bench workflow writes beside a timed build's manifest.
const JobFile = "job.json"

// Job is what the bench workflow records of one build job: what it ran and on what.
type Job struct {
	SHA          string `json:"sha"`
	OS           string `json:"os"`
	Arch         string `json:"arch"`
	Family       string `json:"family"`
	Window       int    `json:"window"`
	Candidate    string `json:"candidate"`
	Rep          int    `json:"rep"`
	PinsWindow   int    `json:"pins_window"`
	Sync         string `json:"sync"`
	MetricsEvery string `json:"metrics_every"`
	CPUModel     string `json:"cpu_model"`
	NProc        int    `json:"nproc"`
	MemTotalKB   int64  `json:"mem_total_kb"`
	ImageOS      string `json:"image_os"`
	ImageVersion string `json:"image_version"`
	RunnerName   string `json:"runner_name"`
	Kernel       string `json:"kernel"`
	RunID        string `json:"run_id"`
	RunAttempt   string `json:"run_attempt"`
	BinarySHA256 string `json:"binary_sha256"`
}

// TimedBuild is one artifact of the bench workflow: a timed build's manifest and its job.
type TimedBuild struct {
	Dir      string `json:"-"` // where it was found; in error messages only, never in a report
	Job      Job
	Manifest *Manifest
}

// TimedInputs are what JudgeTiming judges.
type TimedInputs struct {
	Builds   []TimedBuild
	Plans    []*Plan      // every plan found under the -in roots, deduplicated by digest
	Counters []*Candidate // counters runs (G0)
	JoinFrom []TimedBuild // nil: decide the join from Builds
	// JoinPlans are the plans found beside JoinFrom.
	JoinPlans []*Plan
}

var (
	hex40  = regexp.MustCompile(`^[0-9a-f]{40}$`)
	hex64  = regexp.MustCompile(`^[0-9a-f]{64}$`)
	digits = regexp.MustCompile(`^[0-9]+$`)
)

// normalRoots makes the roots absolute and free of symbolic links, and drops a root
// that is, or lies inside, another, so that a build under overlapping roots is found
// once. They come back in lexical order.
func normalRoots(roots []string) ([]string, error) {
	var abs []string
	for _, r := range roots {
		a, err := filepath.Abs(r)
		if err != nil {
			return nil, err
		}
		real, err := filepath.EvalSymlinks(a)
		if err != nil {
			return nil, fmt.Errorf("runner: %s: %w", r, err)
		}
		st, err := os.Stat(real)
		if err != nil {
			return nil, fmt.Errorf("runner: %s: %w", r, err)
		}
		if !st.IsDir() {
			return nil, fmt.Errorf("runner: %s is not a directory", r)
		}
		abs = append(abs, real)
	}
	slices.Sort(abs)
	abs = slices.Compact(abs)
	var out []string
	for _, a := range abs {
		inside := false
		for _, o := range out {
			if a == o || strings.HasPrefix(a, o+string(filepath.Separator)) {
				inside = true
				break
			}
		}
		if !inside {
			out = append(out, a)
		}
	}
	return out, nil
}

// walkFiles calls visit for every regular file named one of names under the roots, at
// any depth, in lexical order within a root. A symbolic link to a directory is not
// followed, and a directory named [DBDir] (a database, which holds no artifact) is
// skipped.
func walkFiles(roots []string, names []string, visit func(path string) error) error {
	rs, err := normalRoots(roots)
	if err != nil {
		return err
	}
	for _, root := range rs {
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				if d.Name() == DBDir && path != root {
					return filepath.SkipDir
				}
				return nil
			}
			if d.Type().IsRegular() && slices.Contains(names, d.Name()) {
				return visit(path)
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// compareBuilds orders builds by window, candidate, architecture, CPU model, run and
// repetition, and then by what else tells two builds apart, so that the order of the
// result is a function of the builds and not of where they were found.
func compareBuilds(a, b TimedBuild) int {
	return cmp.Or(
		cmp.Compare(a.Job.Window, b.Job.Window),
		cmp.Compare(a.Job.Candidate, b.Job.Candidate),
		cmp.Compare(a.Job.Arch, b.Job.Arch),
		cmp.Compare(a.Job.CPUModel, b.Job.CPUModel),
		cmp.Compare(a.Job.RunID, b.Job.RunID),
		cmp.Compare(a.Job.Rep, b.Job.Rep),
		cmp.Compare(a.Job.RunAttempt, b.Job.RunAttempt),
		cmp.Compare(a.Job.OS, b.Job.OS),
		cmp.Compare(a.Job.ImageVersion, b.Job.ImageVersion),
		cmp.Compare(a.Job.RunnerName, b.Job.RunnerName),
		cmp.Compare(a.Manifest.Timing.Writes.Quantile(0.99), b.Manifest.Timing.Writes.Quantile(0.99)),
		cmp.Compare(a.Manifest.StatsCompacted["live_table_bytes"], b.Manifest.StatsCompacted["live_table_bytes"]),
		cmp.Compare(measureOf(a.Manifest).longest, measureOf(b.Manifest).longest),
		cmp.Compare(measureOf(a.Manifest).after, measureOf(b.Manifest).after),
		cmp.Compare(a.Manifest.Counters["checkpoint.bytes_written"], b.Manifest.Counters["checkpoint.bytes_written"]),
	)
}

// LoadTimedBuilds finds the artifacts of the bench workflow and the plans under roots,
// at any depth. An artifact is a directory that holds a manifest, and it must hold the
// job file the workflow writes beside it: a manifest without one cannot be attributed
// to a job, and this command judges only builds from CI. Each job file is checked
// against its manifest (see [Job]); any plan is loaded with [LoadPlan] and kept once
// by its digest. Symbolic links to directories are not followed, a directory named db
// is skipped, and a root inside another is dropped, so that no artifact is loaded
// twice. The builds come back sorted by window, candidate, architecture, CPU model, run
// and repetition, and the plans by digest. Nothing else is read: the logs are not.
func LoadTimedBuilds(roots []string) ([]TimedBuild, []*Plan, error) {
	var builds []TimedBuild
	plans := map[string]*Plan{}
	err := walkFiles(roots, []string{ManifestFile, planFileName}, func(path string) error {
		dir := filepath.Dir(path)
		if filepath.Base(path) == planFileName {
			p, err := LoadPlan(path)
			if err != nil {
				return err
			}
			d, err := p.Digest()
			if err != nil {
				return fmt.Errorf("runner: %s: %w", path, err)
			}
			plans[d+" "+p.RulesDigest] = p // the digest of a plan does not cover its rules
			return nil
		}
		b, err := loadTimedBuild(dir)
		if err != nil {
			return err
		}
		builds = append(builds, b)
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	slices.SortStableFunc(builds, compareBuilds)
	var ps []*Plan
	for _, d := range sortedPlanKeys(plans) {
		ps = append(ps, plans[d])
	}
	return builds, ps, nil
}

// planFileName is the name of a plan's file.
const planFileName = "plan.json"

func sortedPlanKeys(m map[string]*Plan) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// loadTimedBuild reads the artifact in dir and checks the job against the manifest.
func loadTimedBuild(dir string) (TimedBuild, error) {
	manifestPath := filepath.Join(dir, ManifestFile)
	jobPath := filepath.Join(dir, JobFile)
	m, _, err := readManifest(dir)
	if err != nil {
		return TimedBuild{}, err
	}
	b, err := os.ReadFile(jobPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return TimedBuild{}, fmt.Errorf("runner: %s: a manifest without %s: not an artifact of the bench workflow (this command judges only CI builds)", dir, JobFile)
		}
		return TimedBuild{}, err
	}
	var job Job
	if err := json.Unmarshal(b, &job); err != nil {
		return TimedBuild{}, fmt.Errorf("runner: %s: %w", jobPath, err)
	}
	if err := checkJob(job, m, jobPath, manifestPath); err != nil {
		return TimedBuild{}, err
	}
	return TimedBuild{Dir: dir, Job: job, Manifest: m}, nil
}

// checkJob is the error for the first field of the job that is missing, malformed or
// not what the manifest says. No field has a default: a missing number is 0 and a
// missing string is empty, and both are refused here.
func checkJob(j Job, m *Manifest, jobPath, manifestPath string) error {
	bad := func(field string, value any, why string) error {
		return fmt.Errorf("runner: %s: %s %q: %s", jobPath, field, fmt.Sprint(value), why)
	}
	if !hex40.MatchString(j.SHA) {
		return bad("sha", j.SHA, "not 40 lowercase hexadecimal digits")
	}
	if j.SHA != m.Build.Revision {
		return bad("sha", j.SHA, fmt.Sprintf("not the revision the manifest says it was built from (%q in %s)", m.Build.Revision, manifestPath))
	}
	if !hex64.MatchString(j.BinarySHA256) {
		return bad("binary_sha256", j.BinarySHA256, "not 64 lowercase hexadecimal digits")
	}
	if j.BinarySHA256 != m.Build.Executable {
		return bad("binary_sha256", j.BinarySHA256, fmt.Sprintf("not the executable the manifest says ran (%q in %s)", m.Build.Executable, manifestPath))
	}
	goarch := map[string]string{"aarch64": "arm64", "x86_64": "amd64"}[j.Arch]
	if goarch == "" {
		return bad("arch", j.Arch, "not aarch64 or x86_64")
	}
	if goarch != m.Build.GOARCH {
		return bad("arch", j.Arch, fmt.Sprintf("the manifest's binary is for %q", m.Build.GOARCH))
	}
	if strings.TrimSpace(j.CPUModel) == "" || j.CPUModel == "unknown" {
		return bad("cpu_model", j.CPUModel, "no CPU model: a timing is comparable only within one")
	}
	if j.NProc < 1 {
		return bad("nproc", j.NProc, "not at least 1")
	}
	if j.MemTotalKB < 1 {
		return bad("mem_total_kb", j.MemTotalKB, "not at least 1")
	}
	for _, f := range []struct{ name, value string }{{"os", j.OS}, {"family", j.Family}, {"run_id", j.RunID}, {"run_attempt", j.RunAttempt}} {
		if f.value == "" {
			return bad(f.name, f.value, "empty")
		}
	}
	for _, f := range []struct{ name, value string }{{"run_id", j.RunID}, {"run_attempt", j.RunAttempt}} {
		if !digits.MatchString(f.value) {
			return bad(f.name, f.value, "not all digits")
		}
	}
	if j.Window < 1 {
		return bad("window", j.Window, "not at least 1")
	}
	if j.Rep < 1 {
		return bad("rep", j.Rep, "not at least 1")
	}
	if j.PinsWindow < 1 {
		return bad("pins_window", j.PinsWindow, "not at least 1")
	}
	if j.Candidate != m.Candidate {
		return bad("candidate", j.Candidate, fmt.Sprintf("not the manifest's (%q in %s)", m.Candidate, manifestPath))
	}
	if j.Sync != "true" && j.Sync != "false" {
		return bad("sync", j.Sync, `not "true" or "false"`)
	}
	if want := describedOr(m, SyncKey, "false"); j.Sync != want {
		return bad("sync", j.Sync, fmt.Sprintf("not the manifest's (%q in %s)", want, manifestPath))
	}
	every := time.Duration(0)
	if j.MetricsEvery != "" {
		d, err := time.ParseDuration(j.MetricsEvery)
		if err != nil {
			return bad("metrics_every", j.MetricsEvery, "not a duration")
		}
		every = d
	}
	described := describedOr(m, MetricsKey, "0s")
	d, err := time.ParseDuration(described)
	if err != nil {
		return fmt.Errorf("runner: %s: %s %q: not a duration", manifestPath, MetricsKey, described)
	}
	if d != every {
		return bad("metrics_every", j.MetricsEvery, fmt.Sprintf("not the manifest's (%q in %s)", described, manifestPath))
	}
	if m.Timing.Writes.Count == 0 {
		return fmt.Errorf("runner: %s: the manifest records no batch timings", manifestPath)
	}
	return nil
}

// LoadCountersRuns finds the counters runs under roots: every directory with a results
// file is loaded with [LoadCandidate], which refuses results read from another manifest
// than the one beside them. The roots are walked as [LoadTimedBuilds] walks them. The
// runs come back sorted by plan, candidate and directory.
func LoadCountersRuns(roots []string) ([]*Candidate, error) {
	type found struct {
		dir string
		c   *Candidate
	}
	var runs []found
	err := walkFiles(roots, []string{ResultsFile}, func(path string) error {
		dir := filepath.Dir(path)
		c, err := LoadCandidate(dir)
		if err != nil {
			return err
		}
		runs = append(runs, found{dir, c})
		return nil
	})
	if err != nil {
		return nil, err
	}
	slices.SortStableFunc(runs, func(a, b found) int {
		return cmp.Or(
			cmp.Compare(a.c.Results.PlanDigest, b.c.Results.PlanDigest),
			cmp.Compare(a.c.Results.Candidate, b.c.Results.Candidate),
			cmp.Compare(a.dir, b.dir),
		)
	})
	out := make([]*Candidate, len(runs))
	for i, f := range runs {
		out[i] = f.c
	}
	return out, nil
}
