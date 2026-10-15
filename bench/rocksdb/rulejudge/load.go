package rulejudge

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"github.com/lotannauo/toposhift/bench/spike/runner"
)

// Job is a job file of the bench workflow, with what the rule reads beyond the
// runner's [runner.Job]: the engine the job built with ("rocksdb" or "pebble"; empty
// in a job file made before it was recorded) and the peak resident set of the build
// process in KiB, from GNU time's report (0 when it was not read).
type Job struct {
	runner.Job
	Engine   string `json:"engine"`
	MaxRSSKB int64  `json:"max_rss_kb"`
}

// Build is one job of the bench workflow: its job file, its manifest, nil when the
// job left none (it was killed, ran out of memory or timed out), and its results,
// nil when it did not read.
type Build struct {
	Dir      string // where it was found; in errors only, never in a report
	Job      Job
	Manifest *runner.Manifest
	Results  *runner.Results
}

// Set is what the judge judges: the builds, and the plans they were made from by
// digest.
type Set struct {
	Builds []*Build
	Plans  map[string]*runner.Plan
}

var (
	hex40 = regexp.MustCompile(`^[0-9a-f]{40}$`)
	hex64 = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

// planFile is the name of a plan's file.
const planFile = "plan.json"

// goarchOf is the Go architecture of a job's.
var goarchOf = map[string]string{"aarch64": "arm64", "x86_64": "amd64"}

// Load finds the artifacts of the bench workflow and the plans under roots, at any
// depth. A build is a directory that holds a job file; its manifest and results
// beside it are read where they are there, the results with [runner.LoadCandidate],
// which refuses results read from another manifest. A manifest without a job file
// is refused (the rule judges only builds from CI), as is a job file that is
// malformed or says other than its manifest. A directory named db is never entered,
// and a symbolic link to a directory is not followed. The builds come back in the
// order of their directories, and every plan once, by its digest.
func Load(roots []string) (*Set, error) {
	dirs := map[string]bool{}
	s := &Set{Plans: map[string]*runner.Plan{}}
	for _, root := range roots {
		abs, err := filepath.Abs(root)
		if err != nil {
			return nil, err
		}
		resolved, err := filepath.EvalSymlinks(abs)
		if err != nil {
			return nil, fmt.Errorf("rulejudge: %s: %w", root, err)
		}
		if st, err := os.Stat(resolved); err != nil || !st.IsDir() {
			return nil, fmt.Errorf("rulejudge: %s is not a directory", root)
		}
		err = filepath.WalkDir(resolved, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				if d.Name() == runner.DBDir && path != resolved {
					return filepath.SkipDir
				}
				return nil
			}
			if !d.Type().IsRegular() {
				return nil
			}
			switch d.Name() {
			case runner.JobFile, runner.ManifestFile:
				dirs[filepath.Dir(path)] = true
			case planFile:
				p, err := runner.LoadPlan(path)
				if err != nil {
					return err
				}
				digest, err := p.Digest()
				if err != nil {
					return fmt.Errorf("rulejudge: %s: %w", path, err)
				}
				if _, ok := s.Plans[digest]; !ok {
					s.Plans[digest] = p
				}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	for _, dir := range slices.Sorted(maps.Keys(dirs)) {
		b, err := loadBuild(dir)
		if err != nil {
			return nil, err
		}
		s.Builds = append(s.Builds, b)
	}
	return s, nil
}

// loadBuild reads the build in dir.
func loadBuild(dir string) (*Build, error) {
	jobPath := filepath.Join(dir, runner.JobFile)
	raw, err := os.ReadFile(jobPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("rulejudge: %s: a manifest without %s: not an artifact of the bench workflow (the rule judges only builds from CI)", dir, runner.JobFile)
		}
		return nil, err
	}
	b := &Build{Dir: dir}
	if err := json.Unmarshal(raw, &b.Job); err != nil {
		return nil, fmt.Errorf("rulejudge: %s: %w", jobPath, err)
	}
	switch _, err := os.Stat(filepath.Join(dir, runner.ManifestFile)); {
	case errors.Is(err, fs.ErrNotExist):
	case err != nil:
		return nil, err
	default:
		if _, err := os.Stat(filepath.Join(dir, runner.ResultsFile)); err == nil {
			c, err := runner.LoadCandidate(dir)
			if err != nil {
				return nil, err
			}
			b.Manifest, b.Results = c.Manifest, c.Results
		} else if b.Manifest, err = runner.LoadManifest(dir); err != nil {
			return nil, err
		}
	}
	if why := checkJob(b.Job, b.Manifest); why != "" {
		return nil, fmt.Errorf("rulejudge: %s: %s", jobPath, why)
	}
	return b, nil
}

// checkJob is why a job file is malformed or says other than its manifest (nil for a
// job that left none), or "".
func checkJob(j Job, m *runner.Manifest) string {
	switch {
	case !hex40.MatchString(j.SHA):
		return fmt.Sprintf("sha %q is not 40 lowercase hexadecimal digits", j.SHA)
	case goarchOf[j.Arch] == "":
		return fmt.Sprintf("arch %q is not aarch64 or x86_64", j.Arch)
	case strings.TrimSpace(j.CPUModel) == "" || j.CPUModel == "unknown":
		return fmt.Sprintf("cpu_model %q: no CPU model, and a timing is compared only within one", j.CPUModel)
	case j.Candidate == "":
		return "no candidate"
	case j.Window < 1:
		return fmt.Sprintf("window %d is not at least 1", j.Window)
	case j.Rep < 1:
		return fmt.Sprintf("rep %d is not at least 1", j.Rep)
	case j.Engine != "" && j.Engine != engineOf(j.Candidate):
		return fmt.Sprintf("engine %q is not that of %s", j.Engine, j.Candidate)
	case m == nil:
		return ""
	case j.SHA != m.Build.Revision:
		return fmt.Sprintf("sha %q is not the revision the manifest says it was built from (%q)", j.SHA, m.Build.Revision)
	case !hex64.MatchString(j.BinarySHA256) || j.BinarySHA256 != m.Build.Executable:
		return fmt.Sprintf("binary_sha256 %q is not the executable the manifest says ran (%q)", j.BinarySHA256, m.Build.Executable)
	case goarchOf[j.Arch] != m.Build.GOARCH:
		return fmt.Sprintf("arch %q: the manifest's binary is for %q", j.Arch, m.Build.GOARCH)
	case j.Candidate != m.Candidate:
		return fmt.Sprintf("candidate %q is not the manifest's (%q)", j.Candidate, m.Candidate)
	}
	return ""
}

// engineOf is the engine of a candidate the rule compares, as a job file names it:
// "rocksdb" for RL and RL4, "pebble" for layout L, and "" for any other.
func engineOf(name string) string {
	switch {
	case strings.HasPrefix(name, RocksPrefix), strings.HasPrefix(name, Rocks4Prefix):
		return "rocksdb"
	case strings.HasPrefix(name, PebblePrefix):
		return "pebble"
	}
	return ""
}
