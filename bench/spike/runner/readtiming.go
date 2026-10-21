package runner

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

// The rules of the read timing judge, version 1. It reports the timed reads of
// [ReadTimed] and decides nothing.
const (
	// ReadTimingRulesVersion is the version of the rules text.
	ReadTimingRulesVersion = 1
	// ReadTimingCacheBytes is the block cache a timed read must have run with: the
	// product's (equal to pebblekv.BenchTuning().CacheBytes, which a test checks).
	ReadTimingCacheBytes int64 = 256 << 20
	// ReadTimingWarmup, ReadTimingReps and ReadTimingReaders are the asks of a timed
	// read: untimed, timed, and the readers making them.
	ReadTimingWarmup  = 3
	ReadTimingReps    = 31
	ReadTimingReaders = 1
	// ReadTimingMinJobs is the fewest jobs a candidate needs for a cell to be reported.
	ReadTimingMinJobs = 3
	// ReadTimingFloorNs is the duration below which nothing is printed as a number.
	ReadTimingFloorNs = 1000
	// ReadTimingStatusRefused and ReadTimingStatusPartial are the exit statuses of the
	// command: nothing judged, and some cells not reported.
	ReadTimingStatusRefused = 3
	ReadTimingStatusPartial = 2
)

// ReadTimingRulesText is the text of the rules, made from the constants above: a
// change to a number or a word changes it, and with it [ReadTimingRulesDigest].
func ReadTimingRulesText() string {
	return fmt.Sprintf(`read timing rules %d
report only: the judge decides nothing, sets no threshold and changes no verdict of any other rule
what is timed: the engine's call for one query, from just before it to just after it, on a monotonic clock; the hashing of the answer is not timed; the engine's counter reporting is, with the recorder muted; the collector is left as it is (GOGC unset, no memory limit), collections that ran during the timed asks are counted and not removed
asks: per query %d untimed warm-up asks and then %d timed asks, %d reader, in the plan's order, on one open store
cache: the block cache is %d MiB (the product's), in the plan, in the manifest and in the store that was read
warm: a query is warm in a job when the engine's warm counter did not move over its timed asks; a query that is not warm in a job is left out of its cell for that job and is counted
partial: a cell with a query left out in a job is partial for that job; its value is printed apart and is never counted among the cell's job values
query value: the median of its timed asks
cell: a group of the plan at an age; its value in a job is the statistic of G1 over the values of its warm queries: the largest for a group of the hottest prefixes, the lower median for a group around the median (a group whose name holds " median ")
jobs: a candidate needs at least %d counted job values in a cell for the cell to be reported; a cell is reported within one CPU model, or pooled over models when every candidate compared has at least %d counted job values, and otherwise within each model every candidate ran on; a cell that none of these makes reportable is not reported, and is named; a job on a CPU model that no reported scope of a cell holds is named under the cell, and the architecture's summary line gives how many jobs are outside every scope and their models, without changing the exit status
statistics: primary cells are named on the command line and printed first; a candidate's cell is its job values in job order (the order of the repetitions) with their minimum, median (the lower median of an even count) and maximum, never one number; its spread is the largest of its job values over the smallest, and the run's noise the largest spread of a candidate in a primary cell; the ratio of a candidate to the base is the ratio of the medians, with the bound from the smallest over the base's largest to the largest over the base's smallest, and no interval is claimed; collections counted: the collections that ran during the timed asks are printed per job, and per query where there was one, in a diagnostic column labelled "collections counted", and per query in the JSON, and enter no statistic, ratio, spread, refusal or exit status
sub-microsecond: a cell value, minimum, median or maximum below %d ns is printed as "< 1 µs" and is no number of the report; no ratio and no spread is formed from one; a single ask's duration is never printed
refused, with exit status %d and nothing judged: more than one commit, not the one named, more than one run id or attempt, the same job twice, more than one binary per architecture, more than one plan digest or one that is not among the plans, an untimed read, a cache other than %d MiB in the plan, the manifest, the store or the timings, other asks than %d, %d and %d, different GOMAXPROCS in one architecture, GOGC set or a memory limit, an answer that differs from the reference engine's, fewer jobs than asked for a candidate and architecture, a base that is not a candidate, timings that are not the plan's queries
not reported, with exit status %d: some cell of some candidate and architecture, named
`, ReadTimingRulesVersion, ReadTimingWarmup, ReadTimingReps, ReadTimingReaders, ReadTimingCacheBytes>>20,
		ReadTimingMinJobs, ReadTimingMinJobs, ReadTimingFloorNs,
		ReadTimingStatusRefused, ReadTimingCacheBytes>>20, ReadTimingWarmup, ReadTimingReps, ReadTimingReaders,
		ReadTimingStatusPartial)
}

// ReadTimingRulesDigest is the SHA-256 of [ReadTimingRulesText], as hex.
func ReadTimingRulesDigest() string {
	sum := sha256.Sum256([]byte(ReadTimingRulesText()))
	return hex.EncodeToString(sum[:])
}

// ReadTimingDigestLine is the first line the judge prints.
func ReadTimingDigestLine() string {
	return fmt.Sprintf("read timing rules %d digest %s", ReadTimingRulesVersion, ReadTimingRulesDigest())
}

// LoadReadTimingPlans loads every plan file under roots, at any depth.
func LoadReadTimingPlans(roots []string) ([]*Plan, error) {
	var out []*Plan
	err := walkFiles(roots, []string{planFileName}, func(path string) error {
		p, err := LoadPlan(path)
		if err != nil {
			return err
		}
		out = append(out, p)
		return nil
	})
	return out, err
}

// TimedRead is one job's timed read: the job file of the bench workflow, the timings,
// and the results read beside them. A loader of another engine builds the same, with
// Results nil.
type TimedRead struct {
	Job     Job
	Timings *ReadTimings
	Results *Results
	Dir     string `json:"-"` // where it was found; in error messages only
}

// LoadReadTimings finds the timed reads under roots, at any depth: a directory with
// a timings file is an artifact of the bench workflow, and must hold the job file, the
// manifest and the results as well. The job is checked against the manifest as
// [LoadTimedBuilds] checks it, the results against the manifest as [LoadCandidate]
// does, and the three files against each other: the same candidate, plan and manifest,
// the job's binary the manifest's, the job's commit the build's. The reads come back
// sorted by architecture, candidate, repetition and the rest of the job.
func LoadReadTimings(roots []string) ([]TimedRead, error) {
	var out []TimedRead
	err := walkFiles(roots, []string{TimingsFile}, func(path string) error {
		dir := filepath.Dir(path)
		b, err := loadTimedBuild(dir)
		if err != nil {
			return err
		}
		c, err := LoadCandidate(dir)
		if err != nil {
			return err
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var t ReadTimings
		if err := json.Unmarshal(raw, &t); err != nil {
			return fmt.Errorf("runner: %s: %w", path, err)
		}
		if err := crossCheckTimings(dir, b, c, &t); err != nil {
			return err
		}
		out = append(out, TimedRead{Job: b.Job, Timings: &t, Results: c.Results, Dir: dir})
		return nil
	})
	if err != nil {
		return nil, err
	}
	slices.SortStableFunc(out, func(a, b TimedRead) int {
		return cmp.Or(
			cmp.Compare(a.Job.Arch, b.Job.Arch),
			cmp.Compare(a.Job.Candidate, b.Job.Candidate),
			cmp.Compare(a.Job.Rep, b.Job.Rep),
			cmp.Compare(a.Job.RunID, b.Job.RunID),
			cmp.Compare(a.Job.RunAttempt, b.Job.RunAttempt),
			cmp.Compare(a.Job.RunnerName, b.Job.RunnerName),
			cmp.Compare(a.Dir, b.Dir),
		)
	})
	return out, nil
}

func crossCheckTimings(dir string, b TimedBuild, c *Candidate, t *ReadTimings) error {
	path := filepath.Join(dir, TimingsFile)
	bad := func(why string) error { return fmt.Errorf("runner: %s: %s", path, why) }
	switch {
	case t.Version != ReadTimingsVersion:
		return bad(fmt.Sprintf("version %d, not %d", t.Version, ReadTimingsVersion))
	case t.Candidate != b.Job.Candidate || c.Results.Candidate != b.Job.Candidate:
		return bad(fmt.Sprintf("candidate %q, the job's is %q and the results' %q", t.Candidate, b.Job.Candidate, c.Results.Candidate))
	case t.PlanDigest != b.Manifest.PlanDigest || c.Results.PlanDigest != b.Manifest.PlanDigest:
		return bad("not of the plan the manifest and the results are of")
	case t.ManifestDigest != c.Results.ManifestDigest:
		return bad("not read from the manifest the results were read from")
	case b.Manifest.Describe["cache_bytes"] != strconv.FormatInt(t.CacheBytes, 10):
		return bad(fmt.Sprintf("read at another block cache than the build's (%d bytes, the manifest says %q)", t.CacheBytes, b.Manifest.Describe["cache_bytes"]))
	case t.Build.Executable != b.Job.BinarySHA256:
		return bad("not read by the executable the job says ran")
	case t.Build.Revision != b.Job.SHA:
		return bad("not read by the revision the job says it is of")
	}
	return nil
}

// ReadTimingOptions say what the judge is asked.
type ReadTimingOptions struct {
	// Commit is the revision every job must be of.
	Commit string
	// Plans are the plans of the run, one of which every job was read from.
	Plans []*Plan
	// Base is the candidate the others are compared with, and Jobs the fewest jobs a
	// candidate must have on an architecture.
	Base string
	Jobs int
	// Primary names the primary cells as "group@age"; the rest are secondary.
	Primary []string
}

// ReadTimingRefusal is the error of a set of reads the judge does not judge: the
// reasons, in order.
type ReadTimingRefusal struct{ Reasons []string }

func (r *ReadTimingRefusal) Error() string {
	return "read timing: nothing judged:\n  " + strings.Join(r.Reasons, "\n  ")
}

// splitCell reads "group@age".
func splitCell(s string) (group, age string, ok bool) {
	i := strings.LastIndex(s, "@")
	if i <= 0 || i == len(s)-1 {
		return "", "", false
	}
	return s[:i], s[i+1:], true
}

// CheckReadTimings returns the reasons the reads are not to be judged, or nil.
func CheckReadTimings(reads []TimedRead, o ReadTimingOptions) []string {
	var why []string
	add := func(format string, args ...any) { why = append(why, fmt.Sprintf(format, args...)) }
	if len(reads) == 0 {
		return []string{"no timed read found: no directory under the roots holds a " + TimingsFile}
	}
	if !hex40.MatchString(o.Commit) {
		add("-commit %q is not 40 lowercase hexadecimal digits", o.Commit)
	}
	if o.Jobs < 1 {
		add("-jobs %d is not at least 1", o.Jobs)
	}
	set := func(f func(TimedRead) string) []string {
		m := map[string]bool{}
		for _, r := range reads {
			m[f(r)] = true
		}
		return sortedSet(m)
	}
	if shas := set(func(r TimedRead) string { return r.Job.SHA }); len(shas) > 1 || (len(shas) == 1 && shas[0] != o.Commit) {
		add("the jobs are of the commits %s, not only of %s", strings.Join(shas, ", "), o.Commit)
	}
	if runs := set(func(r TimedRead) string { return r.Job.RunID + " attempt " + r.Job.RunAttempt }); len(runs) > 1 {
		add("the jobs are of more than one run or attempt (never pooled): %s", strings.Join(runs, ", "))
	}
	seenJob := map[string]bool{}
	for _, r := range reads {
		k := fmt.Sprintf("%s|%s|%s|%s|%d", r.Job.RunID, r.Job.RunAttempt, r.Job.Arch, r.Job.Candidate, r.Job.Rep)
		if seenJob[k] {
			add("the same job twice: %s rep %d on %s (run %s attempt %s)", r.Job.Candidate, r.Job.Rep, r.Job.Arch, r.Job.RunID, r.Job.RunAttempt)
		}
		seenJob[k] = true
	}
	byArch := map[string][]TimedRead{}
	cands := map[string]bool{}
	for _, r := range reads {
		byArch[r.Job.Arch] = append(byArch[r.Job.Arch], r)
		cands[r.Job.Candidate] = true
	}
	for _, arch := range sortedKeysOf(byArch) {
		rs := byArch[arch]
		bins := map[string]bool{}
		procs := map[int]bool{}
		for _, r := range rs {
			bins[r.Job.BinarySHA256] = true
			procs[r.Timings.GOMAXPROCS] = true
		}
		if len(bins) > 1 {
			add("%s: more than one binary: %s", arch, strings.Join(sortedSet(bins), ", "))
		}
		if len(procs) > 1 {
			add("%s: different GOMAXPROCS among the jobs", arch)
		}
		for cand := range cands {
			n := 0
			for _, r := range rs {
				if r.Job.Candidate == cand {
					n++
				}
			}
			if n < o.Jobs {
				add("%s: %s has %d jobs, fewer than the %d asked for", arch, cand, n, o.Jobs)
			}
		}
	}
	if !cands[o.Base] {
		add("-base %q is not among the candidates (%s)", o.Base, strings.Join(sortedSet(cands), ", "))
	}
	digests := set(func(r TimedRead) string { return r.Timings.PlanDigest })
	if len(digests) > 1 {
		add("the jobs were read from more than one plan: %s", strings.Join(digests, ", "))
	}
	var plan *Plan
	for _, p := range o.Plans {
		if d, err := p.Digest(); err == nil && len(digests) == 1 && d == digests[0] {
			plan = p
		}
	}
	if len(digests) == 1 && plan == nil {
		add("the plan the jobs were read from (%s) is not among the plans given", digests[0])
	}
	if plan != nil {
		if plan.CacheBytes != ReadTimingCacheBytes {
			add("the plan's block cache is %d bytes, not %d MiB", plan.CacheBytes, ReadTimingCacheBytes>>20)
		}
		for _, p := range o.Primary {
			g, a, ok := splitCell(p)
			found := false
			if ok {
				for _, q := range plan.Queries {
					found = found || (q.Group == g && q.Age == a)
				}
			}
			if !found {
				add("-primary %q names no cell of the plan", p)
			}
		}
	}
	for _, r := range reads {
		t, id := r.Timings, fmt.Sprintf("%s rep %d on %s", r.Job.Candidate, r.Job.Rep, r.Job.Arch)
		switch {
		case t.Untimed:
			add("%s: an untimed read", id)
		case t.CacheBytes != ReadTimingCacheBytes:
			add("%s: read with a block cache of %d bytes, not %d MiB", id, t.CacheBytes, ReadTimingCacheBytes>>20)
		}
		if r.Results != nil && r.Results.Describe["cache_bytes"] != strconv.FormatInt(ReadTimingCacheBytes, 10) {
			add("%s: the results' block cache is %q, not %d", id, r.Results.Describe["cache_bytes"], ReadTimingCacheBytes)
		}
		if t.Warmup != ReadTimingWarmup || t.Reps != ReadTimingReps || t.Readers != ReadTimingReaders {
			add("%s: %d warm-up, %d timed asks and %d readers, not %d, %d and %d", id, t.Warmup, t.Reps, t.Readers, ReadTimingWarmup, ReadTimingReps, ReadTimingReaders)
		}
		if t.GOGC != "" {
			add("%s: GOGC was set (%q)", id, t.GOGC)
		}
		if t.GoMemLimit != math.MaxInt64 {
			add("%s: a memory limit was set (%d)", id, t.GoMemLimit)
		}
		if r.Results != nil && len(r.Results.Mismatches) > 0 {
			add("%s: %d answers of the read differ from the reference engine's", id, len(r.Results.Mismatches))
		}
		if plan != nil {
			if len(t.Queries) != len(plan.Queries) {
				add("%s: timings of %d queries, the plan has %d", id, len(t.Queries), len(plan.Queries))
				continue
			}
			for i, q := range t.Queries {
				switch {
				case q.Name != plan.Queries[i].Name():
					add("%s: timing %d is of %q, the plan's query is %q", id, i, q.Name, plan.Queries[i].Name())
				case q.Mismatch:
					add("%s: an answer to %q differs from the reference engine's", id, q.Name)
				case len(q.Ns) != t.Reps:
					add("%s: %q has %d durations, not %d", id, q.Name, len(q.Ns), t.Reps)
				case slices.ContainsFunc(q.Ns, func(n int64) bool { return n < 0 }):
					add("%s: %q has a negative duration", id, q.Name)
				}
			}
		}
	}
	slices.Sort(why)
	return slices.Compact(why)
}

// Dur is a duration as the report gives it: nanoseconds, or only that it was below
// [ReadTimingFloorNs], in which case Ns is 0.
type Dur struct {
	Ns    int64
	Below bool `json:",omitempty"`
}

func durOf(ns int64) Dur {
	if ns < ReadTimingFloorNs {
		return Dur{Below: true}
	}
	return Dur{Ns: ns}
}

// Number is the duration in microseconds to three significant figures, or "< 1 µs".
func (d Dur) Number() string {
	if d.Below {
		return "< 1 µs"
	}
	return sig3(float64(d.Ns) / 1000)
}

// String is [Dur.Number] with its unit.
func (d Dur) String() string {
	if d.Below {
		return d.Number()
	}
	return d.Number() + " µs"
}

// sig3 formats v (positive) with three significant figures, without an exponent.
func sig3(v float64) string {
	e := int(math.Floor(math.Log10(v)))
	scale := math.Pow(10, float64(e-2))
	r := math.Round(v/scale) * scale
	dec := max(0, 2-e)
	return strconv.FormatFloat(r, 'f', dec, 64)
}

// ReadTimingJob is one job's value in a cell.
type ReadTimingJob struct {
	Rep int
	// Value is the statistic over the warm queries; unset when there were none.
	Value Dur
	// Warm of Queries of the cell were warm in the job; the job is partial when they
	// are not all.
	Warm, Queries int
	Partial       bool
	// Collections is the collections counted during the timed asks of the cell's
	// queries in the job: a diagnostic that is used in no statistic and no refusal.
	Collections uint32
}

// ReadTimingQueryGC is the collections counted during one query's timed asks in each
// counted job of a candidate, in job order: a diagnostic, used in nothing.
type ReadTimingQueryGC struct {
	Query  string
	PerJob []uint32
}

// ReadTimingCandidate is a candidate in a cell within one CPU scope.
type ReadTimingCandidate struct {
	Name string
	// Jobs are the job values that count, in job order; Partial the jobs that are not
	// counted.
	Jobs, Partial       []ReadTimingJob
	CollectionsByQuery  []ReadTimingQueryGC
	Min, Median, Max    Dur
	LeftOut             int
	Spread              float64 `json:",omitempty"`
	SpreadNotFormed     bool    `json:",omitempty"`
	Ratio               float64 `json:",omitempty"`
	RatioLow, RatioHigh float64 `json:",omitempty"`
	RatioNotFormed      bool    `json:",omitempty"`
}

// ReadTimingScope is a cell within one CPU scope, for every candidate.
type ReadTimingScope struct {
	Scope string
	// NotReported is why the cell is not reported in this scope, or empty.
	NotReported string
	Candidates  []ReadTimingCandidate
}

// ReadTimingCell is a group of the plan at an age, on one architecture.
type ReadTimingCell struct {
	Group, Age string
	Primary    bool
	// Statistic is "largest" or "median", and Queries how many queries the cell has.
	Statistic string
	Queries   int
	Scopes    []ReadTimingScope
	// Outside names the jobs on a CPU model that no reported scope of the cell holds, so
	// that they are not silently left out of the print.
	Outside []string `json:",omitempty"`
}

// ReadTimingArch is the cells of one architecture.
type ReadTimingArch struct {
	Arch      string
	CPUModels []string
	Jobs      map[string]int
	Cells     []ReadTimingCell
	// OutsideJobs counts the jobs that are outside every reported scope in some cell,
	// and OutsideModels are their CPU models. They do not change the exit status.
	OutsideJobs   int      `json:",omitempty"`
	OutsideModels []string `json:",omitempty"`
}

// ReadTimingReport is what the judge found.
type ReadTimingReport struct {
	RulesVersion         int
	RulesDigest          string
	Commit, RunID, Label string
	Family               string
	Window               int
	Base                 string
	Arches               []ReadTimingArch
	// Noise is the largest spread of a candidate in a primary cell, and 0 when none
	// was formed.
	Noise float64
	// NotReported names the cells and scopes the report could not give.
	NotReported []string
}

type rtCell struct {
	group, age string
	queries    []int
	median     bool
}

func readTimingCells(plan *Plan) []rtCell {
	var cells []rtCell
	idx := map[string]int{}
	for i, q := range plan.Queries {
		k := q.Group + "@" + q.Age
		j, ok := idx[k]
		if !ok {
			j = len(cells)
			idx[k] = j
			cells = append(cells, rtCell{group: q.Group, age: q.Age, median: strings.Contains(q.Group, " median ")})
		}
		cells[j].queries = append(cells[j].queries, i)
	}
	return cells
}

// queryValue is the median of a query's timed asks (the lower of the two middle ones for an even count).
func queryValue(ns []int64) int64 {
	return lowerMedian(ns)
}

// jobCellValue is a job's value in a cell: the statistic of G1 over the values of the
// cell's warm queries.
func jobCellValue(t *ReadTimings, c rtCell) (value int64, warm int) {
	var vals []int64
	for _, qi := range c.queries {
		if q := t.Queries[qi]; q.Warm && !q.Mismatch {
			vals = append(vals, queryValue(q.Ns))
		}
	}
	if len(vals) == 0 {
		return 0, 0
	}
	slices.Sort(vals)
	if c.median {
		return lowerMedian(vals), len(vals)
	}
	return vals[len(vals)-1], len(vals)
}

// JudgeReadTimings reports the reads, or returns a [ReadTimingRefusal].
func JudgeReadTimings(reads []TimedRead, o ReadTimingOptions) (*ReadTimingReport, error) {
	if why := CheckReadTimings(reads, o); len(why) > 0 {
		return nil, &ReadTimingRefusal{Reasons: why}
	}
	var plan *Plan
	for _, p := range o.Plans {
		if d, _ := p.Digest(); d == reads[0].Timings.PlanDigest {
			plan = p
		}
	}
	// Jobs are taken in job order, whatever order the reads came in.
	reads = slices.Clone(reads)
	slices.SortStableFunc(reads, func(a, b TimedRead) int {
		return cmp.Or(cmp.Compare(a.Job.Arch, b.Job.Arch), cmp.Compare(a.Job.Candidate, b.Job.Candidate), cmp.Compare(a.Job.Rep, b.Job.Rep),
			cmp.Compare(a.Job.RunnerName, b.Job.RunnerName), cmp.Compare(a.Dir, b.Dir))
	})
	cells := readTimingCells(plan)
	primary := map[string]int{}
	for i, p := range o.Primary {
		primary[p] = i
	}
	// Primary cells first, in the order given, then the rest in the plan's.
	slices.SortStableFunc(cells, func(a, b rtCell) int {
		ia, oka := primary[a.group+"@"+a.age]
		ib, okb := primary[b.group+"@"+b.age]
		switch {
		case oka && okb:
			return cmp.Compare(ia, ib)
		case oka:
			return -1
		case okb:
			return 1
		}
		return 0
	})

	rep := &ReadTimingReport{
		RulesVersion: ReadTimingRulesVersion, RulesDigest: ReadTimingRulesDigest(), Commit: o.Commit, RunID: reads[0].Job.RunID,
		Family: reads[0].Job.Family, Window: reads[0].Job.Window, Base: o.Base,
	}
	byArch := map[string][]TimedRead{}
	for _, r := range reads {
		byArch[r.Job.Arch] = append(byArch[r.Job.Arch], r)
	}
	for _, arch := range sortedKeysOf(byArch) {
		a := judgeArch(arch, byArch[arch], cells, primary, o, rep)
		rep.Arches = append(rep.Arches, a)
	}
	rep.Label = readTimingLabel(rep)
	return rep, nil
}

func judgeArch(arch string, rs []TimedRead, cells []rtCell, primary map[string]int, o ReadTimingOptions, rep *ReadTimingReport) ReadTimingArch {
	out := ReadTimingArch{Arch: arch, Jobs: map[string]int{}}
	models := map[string]bool{}
	cands := map[string]bool{}
	for _, r := range rs {
		models[r.Job.CPUModel] = true
		cands[r.Job.Candidate] = true
		out.Jobs[r.Job.Candidate]++
	}
	out.CPUModels = sortedSet(models)
	order := []string{o.Base}
	for _, c := range sortedSet(cands) {
		if c != o.Base {
			order = append(order, c)
		}
	}
	outsideJobs, outsideModels := map[string]bool{}, map[string]bool{}
	for _, c := range cells {
		isPrimary := false
		_, isPrimary = primary[c.group+"@"+c.age]
		cell := ReadTimingCell{Group: c.group, Age: c.age, Primary: isPrimary, Statistic: "largest", Queries: len(c.queries)}
		if c.median {
			cell.Statistic = "median"
		}
		// Every job's value, by candidate.
		type jobv struct {
			ReadTimingJob
			model string
			raw   int64
			gcs   []uint32
		}
		all := map[string][]jobv{}
		smp := map[string][]sample{}
		for _, r := range rs {
			v, warm := jobCellValue(r.Timings, c)
			j := jobv{ReadTimingJob: ReadTimingJob{Rep: r.Job.Rep, Warm: warm, Queries: len(c.queries)}, model: r.Job.CPUModel, raw: v}
			for _, qi := range c.queries {
				g := r.Timings.Queries[qi].GCs
				j.gcs = append(j.gcs, g)
				j.Collections += g
			}
			j.Partial = warm < len(c.queries)
			if warm > 0 {
				j.Value = durOf(v)
			}
			all[r.Job.Candidate] = append(all[r.Job.Candidate], j)
			if !j.Partial {
				smp[r.Job.Candidate] = append(smp[r.Job.Candidate], sample{v: v, model: r.Job.CPUModel})
			}
		}
		reportedModels := map[string]bool{}
		for _, row := range scopes(order, smp) {
			sc := ReadTimingScope{Scope: row.scope}
			if row.notComparable {
				sc.Scope, sc.NotReported = "", row.text
			}
			if sc.NotReported == "" {
				for _, n := range order {
					if len(row.samples[n]) < ReadTimingMinJobs {
						sc.NotReported = fmt.Sprintf("%s has %d counted jobs, fewer than %d", n, len(row.samples[n]), ReadTimingMinJobs)
						break
					}
				}
			}
			if sc.NotReported != "" {
				cell.Scopes = append(cell.Scopes, sc)
				rep.NotReported = append(rep.NotReported, fmt.Sprintf("%s: %s @ %s%s: %s", arch, c.group, c.age, scopeSuffix(sc.Scope), sc.NotReported))
				continue
			}
			inScope := map[string]bool{}
			for _, n := range order {
				for _, s := range row.samples[n] {
					inScope[s.model] = true
					reportedModels[s.model] = true
				}
			}
			var baseRaw []int64
			var baseMed int64
			for _, n := range order {
				rc := ReadTimingCandidate{Name: n}
				var raw []int64
				var counted []jobv
				for _, j := range all[n] {
					if !inScope[j.model] {
						continue
					}
					rc.LeftOut += j.Queries - j.Warm
					if j.Partial {
						rc.Partial = append(rc.Partial, j.ReadTimingJob)
						continue
					}
					rc.Jobs = append(rc.Jobs, j.ReadTimingJob)
					raw = append(raw, j.raw)
					counted = append(counted, j)
				}
				for i, qi := range c.queries {
					g := ReadTimingQueryGC{Query: rs[0].Timings.Queries[qi].Name}
					for _, j := range counted {
						g.PerJob = append(g.PerJob, j.gcs[i])
					}
					rc.CollectionsByQuery = append(rc.CollectionsByQuery, g)
				}
				sorted := slices.Clone(raw)
				slices.Sort(sorted)
				minV, med, maxV := sorted[0], lowerMedian(sorted), sorted[len(sorted)-1]
				rc.Min, rc.Median, rc.Max = durOf(minV), durOf(med), durOf(maxV)
				if len(raw) >= 2 {
					if minV >= ReadTimingFloorNs {
						rc.Spread = float64(maxV) / float64(minV)
						if isPrimary {
							rep.Noise = max(rep.Noise, rc.Spread)
						}
					} else {
						rc.SpreadNotFormed = true
					}
				}
				if n == o.Base {
					baseRaw, baseMed = sorted, med
				} else {
					bmin, bmax := baseRaw[0], baseRaw[len(baseRaw)-1]
					if minV >= ReadTimingFloorNs && bmin >= ReadTimingFloorNs {
						rc.Ratio = float64(med) / float64(baseMed)
						rc.RatioLow, rc.RatioHigh = float64(minV)/float64(bmax), float64(maxV)/float64(bmin)
					} else {
						rc.RatioNotFormed = true
					}
				}
				sc.Candidates = append(sc.Candidates, rc)
			}
			cell.Scopes = append(cell.Scopes, sc)
		}
		if len(reportedModels) > 0 {
			for _, n := range order {
				for _, j := range all[n] {
					if !reportedModels[j.model] {
						kind := "counted"
						if j.Partial {
							kind = "partial"
						}
						cell.Outside = append(cell.Outside, fmt.Sprintf("%s rep %d on %s (%s)", n, j.Rep, j.model, kind))
						outsideJobs[fmt.Sprintf("%s|%d|%s", n, j.Rep, j.model)] = true
						outsideModels[j.model] = true
					}
				}
			}
		}
		out.Cells = append(out.Cells, cell)
	}
	out.OutsideJobs, out.OutsideModels = len(outsideJobs), sortedSet(outsideModels)
	return out
}

func scopeSuffix(scope string) string {
	if scope == "" {
		return ""
	}
	return " [" + scope + "]"
}

func readTimingLabel(rep *ReadTimingReport) string {
	var parts []string
	for _, a := range rep.Arches {
		var jobs []string
		for _, c := range sortedKeysOf(a.Jobs) {
			jobs = append(jobs, fmt.Sprintf("%s %d", c, a.Jobs[c]))
		}
		parts = append(parts, fmt.Sprintf("%s on %s (jobs: %s)", a.Arch, strings.Join(a.CPUModels, ", "), strings.Join(jobs, ", ")))
	}
	sha := rep.Commit
	if len(sha) > 7 {
		sha = sha[:7]
	}
	return fmt.Sprintf("CI timing, run %s, commit %s, %s; warm block cache (no miss in the timed asks), %d warm-up and %d timed asks per query, %d reader; "+
		"spike engine, family %s, window %d days; times include the engine's counter reporting, with the recorder muted; the answer's hashing excluded; "+
		"GOGC default; collections counted; block cache %d MiB (the product's); report-only",
		rep.RunID, sha, strings.Join(parts, "; "), ReadTimingWarmup, ReadTimingReps, ReadTimingReaders, rep.Family, rep.Window, ReadTimingCacheBytes>>20)
}

func factor(f float64) string { return fmt.Sprintf("×%.2f", f) }

// WriteReadTimings prints the report: the digest of the rules first, then the cells,
// the primary ones first. No cell is given as one number: a candidate's cell is its job
// values with their minimum, median and maximum.
func WriteReadTimings(w io.Writer, rep *ReadTimingReport) {
	p := func(format string, args ...any) { _, _ = fmt.Fprintf(w, format+"\n", args...) }
	p("%s", ReadTimingDigestLine())
	p("commit %s  run %s  base %s", rep.Commit, rep.RunID, rep.Base)
	p("label: %s", rep.Label)
	if rep.Noise > 0 {
		p("the run's noise (largest spread of a candidate in a primary cell): %s", factor(rep.Noise))
	}
	for _, a := range rep.Arches {
		p("")
		p("architecture %s: CPU %s", a.Arch, strings.Join(a.CPUModels, ", "))
		for _, c := range sortedKeysOf(a.Jobs) {
			p("  %s: %d jobs", c, a.Jobs[c])
		}
		if a.OutsideJobs > 0 {
			p("  %d jobs outside every scope (models %s)", a.OutsideJobs, strings.Join(a.OutsideModels, ", "))
		}
		kind := ""
		for _, c := range a.Cells {
			if k := map[bool]string{true: "primary", false: "secondary"}[c.Primary]; k != kind {
				kind = k
				p("")
				p("%s CELLS", strings.ToUpper(kind))
			}
			p("")
			p("cell %s @ %s  (%s of %d queries)", c.Group, c.Age, c.Statistic, c.Queries)
			for _, s := range c.Scopes {
				if s.NotReported != "" {
					p("  NOT REPORTED%s: %s", scopeSuffix(s.Scope), s.NotReported)
					continue
				}
				p("  CPU scope: %s", s.Scope)
				for _, cand := range s.Candidates {
					writeReadTimingCandidate(w, cand, cand.Name == rep.Base)
				}
			}
			if len(c.Outside) > 0 {
				p("  outside every reported scope: %s", strings.Join(c.Outside, ", "))
			}
		}
	}
	if len(rep.NotReported) > 0 {
		p("")
		p("NOT REPORTED (%d):", len(rep.NotReported))
		for _, n := range rep.NotReported {
			p("  %s", n)
		}
	}
}

func writeReadTimingCandidate(w io.Writer, c ReadTimingCandidate, base bool) {
	vals := make([]string, len(c.Jobs))
	for i, j := range c.Jobs {
		vals[i] = j.Value.Number()
	}
	name := c.Name
	if base {
		name += " (base)"
	}
	line := fmt.Sprintf("    %s: jobs (µs) [%s]; min %s, median %s, max %s; n %d", name, strings.Join(vals, " "), c.Min, c.Median, c.Max, len(c.Jobs))
	switch {
	case c.Spread > 0:
		line += "; spread " + factor(c.Spread)
	case c.SpreadNotFormed:
		line += "; spread not formed (< 1 µs)"
	}
	if !base {
		switch {
		case c.Ratio > 0:
			line += fmt.Sprintf("; ratio to base %s [%s, %s]", factor(c.Ratio), factor(c.RatioLow), factor(c.RatioHigh))
		case c.RatioNotFormed:
			line += "; ratio to base not formed (< 1 µs)"
		}
	}
	line += fmt.Sprintf("; queries left out %d", c.LeftOut)
	if len(c.Partial) > 0 {
		var ps []string
		for _, j := range c.Partial {
			v := "no warm query"
			if j.Warm > 0 {
				v = j.Value.String()
			}
			ps = append(ps, fmt.Sprintf("rep %d (%d of %d warm, %s, not counted)", j.Rep, j.Warm, j.Queries, v))
		}
		line += "; partial: " + strings.Join(ps, ", ")
	}
	gcs := make([]string, len(c.Jobs))
	for i, j := range c.Jobs {
		gcs[i] = strconv.FormatUint(uint64(j.Collections), 10)
	}
	line += fmt.Sprintf("; collections counted (diagnostic) per job [%s]", strings.Join(gcs, " "))
	_, _ = fmt.Fprintln(w, line)
	for _, g := range c.CollectionsByQuery {
		if slices.ContainsFunc(g.PerJob, func(n uint32) bool { return n > 0 }) {
			parts := make([]string, len(g.PerJob))
			for i, n := range g.PerJob {
				parts[i] = strconv.FormatUint(uint64(n), 10)
			}
			_, _ = fmt.Fprintf(w, "      collections counted, %s: [%s]\n", g.Query, strings.Join(parts, " "))
		}
	}
}
