package runner_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/bench/spike/runner"
)

// editJSON reads the JSON object in path, applies f and writes it back.
func editJSON(t *testing.T, path string, f func(map[string]any)) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}
	f(m)
	out, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, out, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestLoadRefusesWhatItCannotAttributeToAJob(t *testing.T) {
	t.Parallel()

	job := func(key string, value any) func(t *testing.T, dir string) {
		return func(t *testing.T, dir string) {
			editJSON(t, filepath.Join(dir, runner.JobFile), func(m map[string]any) { m[key] = value })
		}
	}
	drop := func(key string) func(t *testing.T, dir string) {
		return func(t *testing.T, dir string) {
			editJSON(t, filepath.Join(dir, runner.JobFile), func(m map[string]any) { delete(m, key) })
		}
	}
	for _, c := range []struct {
		name  string
		build func(*fixtureBuild)
		edit  func(t *testing.T, dir string)
		want  []string // words the error must hold: the file and the field
	}{
		{"unchanged", nil, nil, nil},
		{"no job file", nil, func(t *testing.T, dir string) {
			if err := os.Remove(filepath.Join(dir, runner.JobFile)); err != nil {
				t.Fatal(err)
			}
		}, []string{"job.json", "not an artifact of the bench workflow"}},
		{"job file not JSON", nil, func(t *testing.T, dir string) {
			if err := os.WriteFile(filepath.Join(dir, runner.JobFile), []byte("{not json"), 0o644); err != nil {
				t.Fatal(err)
			}
		}, []string{"job.json"}},
		{"short sha", nil, job("sha", "abc"), []string{"job.json", "sha"}},
		{"sha of another revision", nil, job("sha", strings.Repeat("b", 40)), []string{"job.json", "sha"}},
		{"uppercase sha", nil, job("sha", strings.Repeat("A", 40)), []string{"job.json", "sha"}},
		{"binary unknown", nil, job("binary_sha256", "unknown"), []string{"job.json", "binary_sha256"}},
		{"binary of another executable", nil, job("binary_sha256", strings.Repeat("3", 64)), []string{"job.json", "binary_sha256"}},
		{"arch i686", nil, job("arch", "i686"), []string{"job.json", "arch"}},
		{"arch aarch64 for an amd64 binary", func(b *fixtureBuild) { b.Arch = "x86_64" }, job("arch", "aarch64"), []string{"job.json", "arch"}},
		{"cpu model empty", nil, job("cpu_model", ""), []string{"job.json", "cpu_model"}},
		{"cpu model spaces", nil, job("cpu_model", "  "), []string{"job.json", "cpu_model"}},
		{"cpu model unknown", nil, job("cpu_model", "unknown"), []string{"job.json", "cpu_model"}},
		{"nproc 0", nil, job("nproc", 0), []string{"job.json", "nproc"}},
		{"mem_total_kb 0", nil, job("mem_total_kb", 0), []string{"job.json", "mem_total_kb"}},
		{"run_id not digits", nil, job("run_id", "abc"), []string{"job.json", "run_id"}},
		{"run_attempt empty", nil, job("run_attempt", ""), []string{"job.json", "run_attempt"}},
		{"os empty", nil, job("os", ""), []string{"job.json", "os"}},
		{"family empty", nil, job("family", ""), []string{"job.json", "family"}},
		{"window 0", nil, job("window", 0), []string{"job.json", "window"}},
		{"rep 0", nil, job("rep", 0), []string{"job.json", "rep"}},
		{"pins_window 0", nil, job("pins_window", 0), []string{"job.json", "pins_window"}},
		{"candidate of another build", nil, job("candidate", "L/off"), []string{"job.json", "candidate"}},
		{"sync yes", nil, job("sync", "yes"), []string{"job.json", "sync"}},
		{"sync true for a build without", nil, job("sync", "true"), []string{"job.json", "sync"}},
		{"metrics 30s for a build without", nil, job("metrics_every", "30s"), []string{"job.json", "metrics_every"}},
		{"metrics not a duration", nil, job("metrics_every", "often"), []string{"job.json", "metrics_every"}},
		{"a limit of 11GiB for a manifest of the same bytes", func(b *fixtureBuild) { b.MemLimit = "11811160064" }, nil, nil},
		{"a limit of 512MiB for a manifest of the same bytes", func(b *fixtureBuild) { b.MemLimit = "536870912" }, nil, nil},
		{"no limit for a manifest without the key", nil, drop("go_mem_limit"), nil},
		{"no limit for a manifest that says none", func(b *fixtureBuild) { b.MemLimit = "none" }, nil, nil},
		{"a limit of 11GiB for a manifest that says none", func(b *fixtureBuild) { b.MemLimit = "none" }, job("go_mem_limit", "11GiB"), []string{"job.json", "go_mem_limit", "not the manifest's"}},
		{"a limit of 11GiB for a manifest without the key", nil, job("go_mem_limit", "11GiB"), []string{"job.json", "go_mem_limit", "not the manifest's"}},
		{"a limit of 12GiB for a manifest of 11GiB", func(b *fixtureBuild) { b.MemLimit = "11811160064" }, job("go_mem_limit", "12GiB"), []string{"job.json", "go_mem_limit", "not the manifest's"}},
		{"no limit for a manifest of 11GiB", func(b *fixtureBuild) { b.MemLimit = "11811160064" }, job("go_mem_limit", ""), []string{"job.json", "go_mem_limit", "not the manifest's"}},
		{"a limit in GB", func(b *fixtureBuild) { b.MemLimit = "11811160064" }, job("go_mem_limit", "11GB"), []string{"job.json", "go_mem_limit"}},
		{"missing nproc", nil, drop("nproc"), []string{"job.json", "nproc"}},
		{"missing rep", nil, drop("rep"), []string{"job.json", "rep"}},
		{"missing cpu_model", nil, drop("cpu_model"), []string{"job.json", "cpu_model"}},
		{"missing binary_sha256", nil, drop("binary_sha256"), []string{"job.json", "binary_sha256"}},
		{"missing sync", nil, drop("sync"), []string{"job.json", "sync"}},
		{"no batch timings", nil, func(t *testing.T, dir string) {
			editJSON(t, filepath.Join(dir, runner.ManifestFile), func(m map[string]any) {
				m["Timing"].(map[string]any)["Writes"].(map[string]any)["Count"] = 0
			})
		}, []string{"manifest.json", "no batch timings"}},
		{"manifest not JSON", nil, func(t *testing.T, dir string) {
			if err := os.WriteFile(filepath.Join(dir, runner.ManifestFile), []byte("{"), 0o644); err != nil {
				t.Fatal(err)
			}
		}, []string{"manifest.json"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			b := fixtureBuild{Window: 7, Candidate: "L/k64a2l1ns"}
			if c.build != nil {
				c.build(&b)
			}
			dir := writeArtifact(t, root, b)
			if c.edit != nil {
				c.edit(t, dir)
			}
			builds, _, err := runner.LoadTimedBuilds([]string{root})
			if c.want == nil {
				if err != nil || len(builds) != 1 {
					t.Fatalf("the unchanged artifact: %d builds, error %v", len(builds), err)
				}
				return
			}
			if err == nil {
				t.Fatalf("loaded %d builds from an artifact with a change, want an error naming %v", len(builds), c.want)
			}
			for _, w := range c.want {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("the error %q does not name %q", err, w)
				}
			}
		})
	}
}

func TestLoadFindsArtifactsAtAnyDepthOnce(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	deep := filepath.Join(root, "a", "b", "c")
	for _, d := range []string{filepath.Join(root, "a"), deep} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	writeArtifact(t, root, fixtureBuild{Window: 7, Candidate: "L/off"})
	writeArtifact(t, filepath.Join(root, "a"), fixtureBuild{Window: 7, Candidate: "L/k64a2l1ns"})
	writeArtifact(t, deep, fixtureBuild{Window: 7, Candidate: "M/crdb1"})
	writePlan(t, filepath.Join(root, "a", "b"), 7, "stream-R7", 1000)
	// A database holds a manifest-like file of its own that is not an artifact.
	if err := os.MkdirAll(filepath.Join(root, "a", "L_off", runner.DBDir), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "a", "L_off", runner.DBDir, runner.ManifestFile), []byte("not an artifact"), 0o644); err != nil {
		t.Fatal(err)
	}

	for name, roots := range map[string][]string{
		"one root":               {root},
		"a root twice":           {root, root},
		"overlapping roots":      {root, filepath.Join(root, "a"), deep},
		"a root and its sibling": {deep, root, filepath.Join(root, "a")},
	} {
		builds, plans, err := runner.LoadTimedBuilds(roots)
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		if len(builds) != 3 {
			t.Errorf("%s: %d builds, want 3 (depth 1, 2 and 4, each once)", name, len(builds))
		}
		if len(plans) != 1 {
			t.Errorf("%s: %d plans, want the one under plans/R7", name, len(plans))
		}
		for i := 1; i < len(builds); i++ {
			if builds[i-1].Job.Candidate > builds[i].Job.Candidate {
				t.Errorf("%s: builds are not sorted by candidate: %s before %s", name, builds[i-1].Job.Candidate, builds[i].Job.Candidate)
			}
		}
	}
}

func TestLoadDoesNotFollowASymbolicLinkToADirectory(t *testing.T) {
	t.Parallel()

	outside, root := t.TempDir(), t.TempDir()
	writeArtifact(t, outside, fixtureBuild{Window: 7, Candidate: "L/off"})
	if err := os.Symlink(outside, filepath.Join(root, "link")); err != nil {
		t.Skipf("no symbolic links: %v", err)
	}
	writeArtifact(t, root, fixtureBuild{Window: 7, Candidate: "M/crdb1"})
	builds, _, err := runner.LoadTimedBuilds([]string{root})
	if err != nil {
		t.Fatal(err)
	}
	if len(builds) != 1 || builds[0].Job.Candidate != "M/crdb1" {
		t.Errorf("found %d builds through a link, want only the one in the root", len(builds))
	}
	// A root that is itself a link is resolved: that is what the user asked for.
	builds, _, err = runner.LoadTimedBuilds([]string{filepath.Join(root, "link")})
	if err != nil || len(builds) != 1 {
		t.Errorf("the root given as a link: %d builds, error %v", len(builds), err)
	}
}

func TestLoadRefusesARootThatIsNotADirectory(t *testing.T) {
	t.Parallel()

	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := runner.LoadTimedBuilds([]string{file}); err == nil {
		t.Error("a file was accepted as a root")
	}
	if _, _, err := runner.LoadTimedBuilds([]string{filepath.Join(t.TempDir(), "absent")}); err == nil {
		t.Error("an absent directory was accepted as a root")
	}
}

func TestLoadCountersRunsFindsTheCandidateDirectories(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	_, digest := writePlan(t, t.TempDir(), 7, "stream-R7", 1000)
	writeCountersRun(t, filepath.Join(root, "x", "R7"), digest, "L/off", nil)
	writeCountersRun(t, filepath.Join(root, "x", "R7"), digest, "L/k64a2l1ns", []string{"a query"})
	cs, err := runner.LoadCountersRuns([]string{root, root})
	if err != nil {
		t.Fatal(err)
	}
	if len(cs) != 2 || cs[0].Results.Candidate != "L/k64a2l1ns" || len(cs[0].Results.Mismatches) != 1 {
		t.Errorf("counters runs %+v, want the two candidates sorted by name", cs)
	}
	// Results read from another manifest than the one beside them are refused.
	dir := filepath.Join(root, "x", "R7", "L_off-0")
	editJSON(t, filepath.Join(dir, runner.ManifestFile), func(m map[string]any) { m["Layout"] = "changed" })
	if _, err := runner.LoadCountersRuns([]string{root}); err == nil {
		t.Error("results read from another manifest were accepted")
	}
}

// The workflow writes the job file and this program reads it: the keys of the one are
// the tags of the other, in order, so a field added to either fails here.
func TestJobFieldsAreTheWorkflows(t *testing.T) {
	t.Parallel()

	b, err := os.ReadFile("../../../.github/workflows/bench.yml")
	if err != nil {
		t.Fatal(err)
	}
	obj := regexp.MustCompile(`\{sha: \$sha,[^}]*binary_sha256: \$binary_sha256\}`).FindString(string(b))
	if obj == "" {
		t.Fatal("the jq object of the job file is not in the workflow")
	}
	var workflow []string
	for _, m := range regexp.MustCompile(`(\w+): \$\w+`).FindAllStringSubmatch(obj, -1) {
		workflow = append(workflow, m[1])
	}
	var tags []string
	typ := reflect.TypeOf(runner.Job{})
	for i := range typ.NumField() {
		tags = append(tags, strings.Split(typ.Field(i).Tag.Get("json"), ",")[0])
	}
	if !reflect.DeepEqual(workflow, tags) {
		t.Errorf("the workflow writes\n  %v\nand the judge reads\n  %v", workflow, tags)
	}
}

// What the report holds is fixed: a field added, renamed or dropped is seen here,
// because the JSON is read by tools written against it.
func TestTimingReportKeepsItsFields(t *testing.T) {
	t.Parallel()

	fields := func(v any) string {
		typ := reflect.TypeOf(v)
		names := make([]string, typ.NumField())
		for i := range names {
			names[i] = typ.Field(i).Name
		}
		return strings.Join(names, ",")
	}
	for name, c := range map[string]struct {
		v    any
		want string
	}{
		"Timing report":     {runner.TimingReport{}, "RulesDigest,RulesVersion,Judged,NotJudged,Problems,Findings,Revision,RevisionChecked,RevisionRef,Family,PinsWindow,Runs,Reference,Join,Builds,Verdicts,Diagnostics,RevisionError,Settings,Binaries,Plans,Rule"},
		"Timing join":       {runner.TimingJoin{}, "Decided,Joins,Reason,Revision,Runs,Arches"},
		"Timing join arch":  {runner.TimingJoinArch{}, "Arch,Scope,Comparable,Below,JoinerP99,BaselineP99,JoinerReps,BaselineReps"},
		"Timing build":      {runner.TimingBuild{}, "Window,Candidate,Arch,CPUModel,OS,ImageVersion,RunID,RunAttempt,Rep,CommitP99,AfterRetention,LongestRetention,Stall,SettleLongest,SettleDeadlineHits,BytesPerRecord,CheckpointShare"},
		"Timing verdict":    {runner.TimingVerdict{}, "Window,Candidate,Gate,Arch,Scope,Reps,Value,Best,Limit,ValueText,LimitText,Pass,Judged,NotComparable,Reason"},
		"Revision check":    {runner.RevisionCheck{}, "Ancestor,Err"},
		"Timing settings":   {runner.TimingSettings{}, "Sync,RestAfterRetention,CanonicalLayout,MetricsEvery,SettleTombstones,SettleDeadline,PostRetentionBatches,GoMemoryLimit"},
		"Timing rule":       {runner.TimingRule{}, "StallBudgetSeconds,CommitFactor,BytesPerRecordFactor,PostRetentionBatches"},
		"Timing diagnostic": {runner.TimingDiagnostic{}, "Window,Candidate,Arch,Scope,What,Reps,Value,Best,Text,FormerPass"},
		"Timing binary":     {runner.TimingBinary{}, "Arch,Executable,GoVersion"},
		"Timing plan":       {runner.TimingPlan{}, "Window,Digest,RulesDigest,Records,Found,StreamDigest"},
	} {
		if got := fields(c.v); got != c.want {
			t.Errorf("%s has fields\n  %s\nwant\n  %s", name, got, c.want)
		}
	}
}

// Two plans of one digest and different rules are both kept: the digest does not cover
// the rules.
func TestLoadKeepsPlansThatShareADigestAndNotTheirRules(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	a := makePlan("stream-R7", 1000)
	b := makePlan("stream-R7", 1000)
	b.RulesDigest = strings.Repeat("e", 64)
	savePlan(t, filepath.Join(root, "a"), 7, a)
	savePlan(t, filepath.Join(root, "b"), 7, b)
	_, plans, err := runner.LoadTimedBuilds([]string{root})
	if err != nil || len(plans) != 2 {
		t.Fatalf("%d plans, error %v", len(plans), err)
	}
}
