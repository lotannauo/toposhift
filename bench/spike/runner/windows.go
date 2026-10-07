package runner

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
)

// WindowDir is where the runs over a window of days are kept under out.
func WindowDir(out string, days int) string { return filepath.Join(out, "R"+strconv.Itoa(days)) }

// PinsFile is where the pins of a family of windows are kept under out.
const PinsFile = "pins.json"

var windowDir = regexp.MustCompile(`^R([0-9]+)$`)

// LoadAllCandidates loads every candidate that has been read under out, whatever
// its name: a directory with results in it is a candidate, not only one of the
// default list.
func LoadAllCandidates(out string) ([]*Candidate, error) {
	entries, err := os.ReadDir(out)
	if err != nil {
		return nil, err
	}
	var cs []*Candidate
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		dir := filepath.Join(out, e.Name())
		if _, err := os.Stat(filepath.Join(dir, ResultsFile)); err != nil {
			continue
		}
		c, err := LoadCandidate(dir)
		if err != nil {
			return nil, err
		}
		cs = append(cs, c)
	}
	return cs, nil
}

// LoadWindows loads the windows under out: a directory R<days> for each, with its
// plan and every candidate read from it, in order of days.
func LoadWindows(out string) ([]RetainedWindow, error) {
	entries, err := os.ReadDir(out)
	if err != nil {
		return nil, err
	}
	var ws []RetainedWindow
	for _, e := range entries {
		m := windowDir.FindStringSubmatch(e.Name())
		if m == nil || !e.IsDir() {
			continue
		}
		days, _ := strconv.Atoi(m[1])
		dir := filepath.Join(out, e.Name())
		plan, err := LoadPlan(filepath.Join(dir, "plan.json"))
		if err != nil {
			return nil, err
		}
		cs, err := LoadAllCandidates(dir)
		if err != nil {
			return nil, err
		}
		if len(cs) == 0 {
			return nil, fmt.Errorf("runner: no candidate under %s has been read", dir)
		}
		ws = append(ws, RetainedWindow{Days: days, Plan: plan, Candidates: cs})
	}
	sort.Slice(ws, func(i, j int) bool { return ws[i].Days < ws[j].Days })
	return ws, nil
}

// CheckWindows returns the reasons the windows cannot be judged together, or nil:
// each is checked as a set of candidates is, and they must have been made of the
// same pins and the same cache, the same candidates, and the same binary, over streams
// of the lengths a window of its days makes.
func CheckWindows(ws []RetainedWindow, allowUntimed bool) []string {
	var out []string
	for _, w := range ws {
		for _, p := range Check(w.Plan, w.Candidates, allowUntimed) {
			out = append(out, fmt.Sprintf("window of %d days: %s", w.Days, p))
		}
	}
	if len(ws) == 0 {
		return append(out, "no window")
	}
	ref := ws[0]
	for _, w := range ws {
		switch {
		case w.Plan.Spec.Pins == nil || ref.Plan.Spec.Pins == nil:
			out = append(out, fmt.Sprintf("window of %d days: the plan has no pins", w.Days))
		case w.Plan.Spec.Pins.Digest != ref.Plan.Spec.Pins.Digest:
			out = append(out, fmt.Sprintf("windows of %d and %d days were made of different pins", ref.Days, w.Days))
		case w.Plan.Spec.FullStore != ref.Plan.Spec.FullStore:
			out = append(out, fmt.Sprintf("windows of %d and %d days are not both full stores or both projections", ref.Days, w.Days))
		}
		if w.Plan.CacheBytes != ref.Plan.CacheBytes {
			out = append(out, fmt.Sprintf("windows of %d and %d days have block caches of %d and %d bytes", ref.Days, w.Days, ref.Plan.CacheBytes, w.Plan.CacheBytes))
		}
		if w.Plan.Spec.Workload.Seed != ref.Plan.Spec.Workload.Seed {
			out = append(out, fmt.Sprintf("windows of %d and %d days are of different seeds", ref.Days, w.Days))
		}
		if a, err := ref.Plan.Spec.ScenarioDigest(); err != nil {
			out = append(out, err.Error())
		} else if b, err := w.Plan.Spec.ScenarioDigest(); err != nil || a != b {
			out = append(out, fmt.Sprintf("windows of %d and %d days are of different scenarios (the workload, the extension interval, the pod heartbeat, the run age, the batch size or the queries asked differ)", ref.Days, w.Days))
		}
		if want := WindowSpec(ref.Plan.Spec, w.Days).Workload.Duration; w.Plan.Spec.Workload.Duration != want {
			out = append(out, fmt.Sprintf("window of %d days is %s long, not the %s of a window of its days", w.Days, w.Plan.Spec.Workload.Duration, want))
		}
		names := func(w RetainedWindow) []string {
			var n []string
			for _, c := range w.Candidates {
				n = append(n, c.Results.Candidate)
			}
			slices.Sort(n)
			return n
		}
		if !slices.Equal(names(w), names(ref)) {
			out = append(out, fmt.Sprintf("windows of %d and %d days hold different candidates: %v and %v", ref.Days, w.Days, names(ref), names(w)))
		}
		if len(w.Candidates) > 0 && len(ref.Candidates) > 0 && !sameBinary(w.Candidates[0].Manifest.Build, ref.Candidates[0].Manifest.Build) {
			out = append(out, fmt.Sprintf("windows of %d and %d days were built by different binaries", ref.Days, w.Days))
		}
		if len(w.Candidates) > 0 && len(ref.Candidates) > 0 {
			if a, b := ref.Candidates[0].Manifest.Describe[CanonicalKey], w.Candidates[0].Manifest.Describe[CanonicalKey]; a != b {
				out = append(out, fmt.Sprintf("windows of %d and %d days were built with and without the canonical layout (%q and %q)", ref.Days, w.Days, a, b))
			}
			// A rest after each retention changes when the compactions run, and with it the
			// table boundaries and the block counters of a build that is not canonical.
			if a, b := ref.Candidates[0].Manifest.Describe[RestKey], w.Candidates[0].Manifest.Describe[RestKey]; a != b {
				out = append(out, fmt.Sprintf("windows of %d and %d days were built with and without a rest after each retention (%q and %q)", ref.Days, w.Days, a, b))
			}
			// A sync of every commit changes how fast the writer ran against the compactions,
			// and with it the table boundaries of a build that is not canonical. Sampling the
			// metrics changes no counter that G1 judges, so it is not compared here.
			if a, b := describedOr(ref.Candidates[0].Manifest, SyncKey, "false"), describedOr(w.Candidates[0].Manifest, SyncKey, "false"); a != b {
				out = append(out, fmt.Sprintf("windows of %d and %d days were built with and without a sync of every commit (%q and %q)", ref.Days, w.Days, a, b))
			}
		}
	}
	return out
}
