package scripts_test

import (
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"
)

// A bench dispatch hands its plans and its RocksDB library to the build jobs as
// artifacts of the run, never through the cache: a push to main can evict any cache
// entry while a dispatch runs, and the cache's limit makes it do so. These tests
// read the workflow and fail if a build job reads the cache again, if anything in it
// saves one, if an artifact a job fetches is not the one another job of the run
// keeps, if a fetch could reach another run, or if a job that needs the library job
// could be skipped on a pull request.

type handoffStep struct {
	Name string         `yaml:"name"`
	ID   string         `yaml:"id"`
	Uses string         `yaml:"uses"`
	With map[string]any `yaml:"with"`
	Env  map[string]any `yaml:"env"`
	Run  string         `yaml:"run"`
}

type handoffJob struct {
	If    string        `yaml:"if"`
	Needs any           `yaml:"needs"`
	Steps []handoffStep `yaml:"steps"`
}

func benchJobs(t *testing.T) map[string]handoffJob {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", ".github", "workflows", "bench.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var wf struct {
		Jobs map[string]handoffJob `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(data, &wf); err != nil {
		t.Fatalf("bench.yml: %v", err)
	}
	return wf.Jobs
}

func action(s handoffStep) string { return strings.SplitN(s.Uses, "@", 2)[0] }

// step is the one step of job named name, which must use the action uses.
func step(t *testing.T, jobs map[string]handoffJob, job, name, uses string) handoffStep {
	t.Helper()
	var found []handoffStep
	for _, s := range jobs[job].Steps {
		if s.Name == name {
			found = append(found, s)
		}
	}
	if len(found) != 1 || action(found[0]) != uses {
		t.Fatalf("job %s: %d steps named %q (want 1, using %s)", job, len(found), name, uses)
	}
	return found[0]
}

func TestTheBuildJobsTakeTheirInputsFromTheRun(t *testing.T) {
	t.Parallel()
	jobs := benchJobs(t)

	for name, job := range jobs {
		for _, s := range job.Steps {
			switch action(s) {
			case "actions/cache/save", "actions/cache":
				t.Errorf("job %s, step %q writes the cache: rocksdb.yml on main is the library's one writer, and nothing else of a dispatch may need the cache", name, s.Name)
			case "actions/cache/restore":
				if name != "rocksdb-library" {
					t.Errorf("job %s, step %q reads the cache; only the library job may, and it builds on a miss", name, s.Name)
				}
			case "actions/download-artifact":
				for k := range s.With {
					if k != "name" && k != "path" {
						t.Errorf("job %s, step %q sets %s: a fetch names one artifact of this run and nothing else", name, s.Name, k)
					}
				}
			}
		}
	}

	needs, _ := jobs["build"].Needs.([]any)
	for _, want := range []string{"setup", "plans", "rocksdb-library"} {
		if !slices.Contains(needs, any(want)) {
			t.Errorf("the build job does not need %s: %v", want, jobs["build"].Needs)
		}
	}
	if jobs["dry-run-rocksdb"].Needs != "rocksdb-library" {
		t.Errorf("the RocksDB dry run needs %v, want the library job", jobs["dry-run-rocksdb"].Needs)
	}

	// A pull request skips setup, which the library job needs; a job below it
	// without a status function would be skipped with it.
	for job, wants := range map[string][]string{
		"build":           {"!cancelled()", "needs.rocksdb-library.result == 'success'", "needs.rocksdb-library.result == 'skipped'"},
		"dry-run-rocksdb": {"!cancelled()", "github.event_name == 'pull_request'", "needs.rocksdb-library.result == 'success'"},
		"rocksdb-library": {"!cancelled()", "github.event_name == 'pull_request'", "needs.setup.result == 'success'"},
	} {
		for _, want := range wants {
			if !strings.Contains(jobs[job].If, want) {
				t.Errorf("job %s: its if %q does not say %s", job, jobs[job].If, want)
			}
		}
	}

	kept := step(t, jobs, "plans", "Keep the plans and their logs", "actions/upload-artifact").With["name"]
	if got := step(t, jobs, "build", "Fetch the plans", "actions/download-artifact").With["name"]; got != kept {
		t.Errorf("the build jobs fetch the plans as %v, and the plans job keeps them as %v", got, kept)
	}
	dryKept := step(t, jobs, "dry-run", "Keep it as the plans job does", "actions/upload-artifact").With["name"]
	if got := step(t, jobs, "dry-run", "Fetch it as a build job does", "actions/download-artifact").With["name"]; got != dryKept {
		t.Errorf("the dry run fetches its plan as %v, and keeps it as %v", got, dryKept)
	}
	lib := step(t, jobs, "rocksdb-library", "Keep the library for the build jobs", "actions/upload-artifact").With["name"]
	for _, job := range []string{"build", "dry-run-rocksdb"} {
		if got := step(t, jobs, job, "Fetch the RocksDB build", "actions/download-artifact").With["name"]; got != lib {
			t.Errorf("job %s fetches the library as %v, and the library job keeps it as %v", job, got, lib)
		}
	}

	// The RocksDB dry run proves the hand-off the build jobs rely on only if its
	// copies of the two steps are the build jobs' own: the same script, the same
	// environment and the same place for the artifact.
	for name, uses := range map[string]string{"Fetch the RocksDB build": "actions/download-artifact", "Unpack the RocksDB build": ""} {
		b, d := step(t, jobs, "build", name, uses), step(t, jobs, "dry-run-rocksdb", name, uses)
		if b.Run != d.Run || !reflect.DeepEqual(b.With, d.With) || !reflect.DeepEqual(b.Env, d.Env) {
			t.Errorf("step %q: the build job's copy differs from the RocksDB dry run's (run %q and %q, with %v and %v, env %v and %v)", name, b.Run, d.Run, b.With, d.With, b.Env, d.Env)
		}
	}

	// The plan is fetched to the same place in a build job and in the dry run, and the
	// build job reads it from there.
	if got, want := step(t, jobs, "build", "Fetch the plans", "actions/download-artifact").With["path"],
		step(t, jobs, "dry-run", "Fetch it as a build job does", "actions/download-artifact").With["path"]; got != want {
		t.Errorf("the build jobs fetch the plans to %v, and the dry run to %v", got, want)
	}
	var prepare []handoffStep
	for _, s := range jobs["build"].Steps {
		if s.Name == "Prepare the build directory" {
			prepare = append(prepare, s)
		}
	}
	if len(prepare) != 1 || !strings.Contains(prepare[0].Run, `"$RUNNER_TEMP/planset/plans/R$WINDOW/plan.json"`) {
		t.Errorf("the build directory is not prepared from the plan fetched to $RUNNER_TEMP/planset: %d steps, the script %q", len(prepare), prepare)
	}

	// The collect step records the unpack step's cache-hit by this id, and the
	// RocksDB dry run runs that step as a timed job has it: in both, exactly one
	// step has the id, and it is the unpack step.
	for _, job := range []string{"build", "dry-run-rocksdb"} {
		var ids []string
		for _, s := range jobs[job].Steps {
			if s.ID == "rockscache" {
				ids = append(ids, s.Name)
			}
		}
		if len(ids) != 1 || ids[0] != "Unpack the RocksDB build" {
			t.Errorf("job %s: the steps with id rockscache are %q, want exactly the unpack step", job, ids)
		}
	}
}
