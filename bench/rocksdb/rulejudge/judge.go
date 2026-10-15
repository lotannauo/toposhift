package rulejudge

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/lotannauo/toposhift/bench/spike/runner"
)

// Report is the judgement of a set by the rule.
type Report struct {
	// Revision is the commit every build is of; "" when they are of none or several.
	// It is not checked to be a commit of main (see index).
	Revision string
	// Refused says why the set is not one the rule judges: then there is no verdict,
	// and the rest of the report is what the set shows.
	Refused []string
	Verdict Verdict
	Why     []string
	// Classes are RL's and RL4's at the verdict's scope.
	Classes []Class
	// NotResults says, for each scope or candidate that gives no result, why: void
	// builds, jobs that left no manifest, a Pebble set the runner refuses.
	NotResults  []string
	Comparisons []Comparison
	Builds      []BuildRow
	Plans       []PlanRow
}

// BuildRow is one build as the judge read it.
type BuildRow struct {
	Window                    int
	Arch, Candidate, CPUModel string
	Rep                       int
	Manifest, Read            bool
	// Values are its gates' values by gate; a gate it has no value of is absent.
	Values map[string]float64
}

// PlanRow is the plan of a window.
type PlanRow struct {
	Window          int
	Digest          string
	Records         uint64
	Queries         int
	Found, Multiple bool
}

// checkFunc is [runner.Check]'s shape: a test passes one that records what it is given.
type checkFunc func(plan *runner.Plan, cs []*runner.Candidate, allowUntimed bool) []string

// Judge judges a set by Rule R: it never gives a RocksDB build to the runner's
// judgements, which refuse cgo; [runner.Check] sees each window's Pebble set alone.
func Judge(s *Set) *Report { return judgeWith(s, runner.Check) }

type cellKey struct {
	arch   string
	window int
	cand   string
}

// scopeKey is an architecture and a window.
type scopeKey struct {
	arch   string
	window int
}

// cell is the builds of one candidate at one window on one architecture.
type cell struct {
	key    cellKey
	builds []*Build // by repetition
	// why says why the cell gives no result: one reason for each void build or job
	// that left no manifest.
	why []string
}

func (c *cell) first() *Build {
	for _, b := range c.builds {
		if b.Job.Rep == 1 {
			return b
		}
	}
	return nil
}

// pebbleScope is what a window's Pebble builds on one architecture say for the
// RocksDB builds beside them, and why they give no result there.
type pebbleScope struct {
	limit, sync string
	why         []string
}

type judge struct {
	s       *Set
	check   checkFunc
	rep     *Report
	cells   map[cellKey]*cell
	plans   map[int]*runner.Plan // by window; nil where there is none to use
	pebble  map[scopeKey]*pebbleScope
	archWhy map[string][]string
}

func judgeWith(s *Set, check checkFunc) *Report {
	j := &judge{
		s: s, check: check, rep: &Report{}, cells: map[cellKey]*cell{}, plans: map[int]*runner.Plan{},
		pebble: map[scopeKey]*pebbleScope{}, archWhy: map[string][]string{},
	}
	j.index()
	j.windows()
	for _, k := range j.scopes() {
		j.checkPebble(k)
	}
	for _, k := range j.sortedCells() {
		if c := j.cells[k]; engineOf(k.cand) == "rocksdb" {
			j.checkRocks(c)
		}
	}
	j.checkArches()
	j.compareAll()
	j.decide()
	j.rows()
	return j.rep
}

func (j *judge) refuse(format string, args ...any) {
	j.rep.Refused = append(j.rep.Refused, fmt.Sprintf(format, args...))
}

// index sorts the builds into cells, and refuses a set the rule does not judge: no
// build, a candidate it does not compare, builds of several commits, two jobs that
// are one repetition, a job with a manifest whose job file records no peak resident
// set (a workflow that does not write max_rss_kb would otherwise leave every verdict
// not decided, silently), or a candidate with fewer than [minReps] jobs somewhere.
//
// The one commit of the builds is not checked to be a commit of main: the judge runs
// no git. The commit is named at dispatch and recorded with it in the gate log.
func (j *judge) index() {
	if len(j.s.Builds) == 0 {
		j.refuse("the set holds no build")
	}
	revs := map[string]bool{}
	seen := map[string]bool{}
	var noRSS []string
	for _, b := range j.s.Builds {
		name := b.Job.Candidate
		policy := policyOf(name)
		if engineOf(name) == "" || policy == "" {
			j.refuse("%s is not a candidate the rule compares (%s, %s or %s, and a policy)", name, PebblePrefix, RocksPrefix, Rocks4Prefix)
			continue
		}
		revs[b.Job.SHA] = true
		id := fmt.Sprintf("repetition %d of %s at %d days on %s", b.Job.Rep, name, b.Job.Window, b.Job.Arch)
		if seen[id] {
			j.refuse("two jobs are %s", id)
		}
		seen[id] = true
		if b.Manifest != nil && b.Job.MaxRSSKB <= 0 {
			noRSS = append(noRSS, id)
		}
		k := cellKey{b.Job.Arch, b.Job.Window, name}
		c := j.cells[k]
		if c == nil {
			c = &cell{key: k}
			j.cells[k] = c
		}
		c.builds = append(c.builds, b)
	}
	if len(noRSS) > 0 {
		j.refuse("%d jobs with a manifest record no max_rss_kb in %s, the peak resident set the rule guards on (the first: %s)", len(noRSS), runner.JobFile, noRSS[0])
	}
	switch rs := slices.Sorted(maps.Keys(revs)); len(rs) {
	case 0:
	case 1:
		j.rep.Revision = rs[0]
	default:
		j.refuse("the builds are of %d commits (%s): a set is of one", len(rs), strings.Join(rs, ", "))
	}
	for _, k := range j.sortedCells() {
		c := j.cells[k]
		slices.SortStableFunc(c.builds, func(a, b *Build) int { return cmp.Compare(a.Job.Rep, b.Job.Rep) })
		if len(c.builds) < minReps {
			j.refuse("%s at %d days on %s has %d repetitions, fewer than %d", k.cand, k.window, k.arch, len(c.builds), minReps)
		}
	}
}

// policyOf is the policy of a name the rule compares, or "".
func policyOf(name string) string {
	for _, p := range []string{Rocks4Prefix, RocksPrefix, PebblePrefix} {
		if rest, ok := strings.CutPrefix(name, p); ok {
			return rest
		}
	}
	return ""
}

func (j *judge) sortedCells() []cellKey {
	return slices.SortedFunc(maps.Keys(j.cells), func(a, b cellKey) int {
		return cmp.Or(cmp.Compare(a.window, b.window), cmp.Compare(a.arch, b.arch), cmp.Compare(a.cand, b.cand))
	})
}

func (j *judge) scopes() []scopeKey {
	seen := map[scopeKey]bool{}
	for k := range j.cells {
		seen[scopeKey{k.arch, k.window}] = true
	}
	return slices.SortedFunc(maps.Keys(seen), func(a, b scopeKey) int {
		return cmp.Or(cmp.Compare(a.window, b.window), cmp.Compare(a.arch, b.arch))
	})
}

// windows finds the plan of each window, the one its builds were made from, and
// refuses a window whose builds were made from several plans, or from one that is
// not among the inputs.
func (j *judge) windows() {
	digests := map[int]map[string]bool{}
	for _, b := range j.s.Builds {
		if digests[b.Job.Window] == nil {
			digests[b.Job.Window] = map[string]bool{}
		}
		if b.Manifest != nil {
			digests[b.Job.Window][b.Manifest.PlanDigest] = true
		}
	}
	for _, w := range slices.Sorted(maps.Keys(digests)) {
		ds := slices.Sorted(maps.Keys(digests[w]))
		row := PlanRow{Window: w, Multiple: len(ds) > 1}
		switch len(ds) {
		case 0:
		case 1:
			row.Digest = ds[0]
			if p := j.s.Plans[ds[0]]; p != nil {
				j.plans[w] = p
				row.Found, row.Records, row.Queries = true, p.Stream.Records, len(p.Queries)
			} else {
				j.refuse("the plan of the builds at %d days (%.12s) is not among the inputs", w, ds[0])
			}
		default:
			j.refuse("the builds at %d days were made from %d plans: a window is built from one", w, len(ds))
		}
		j.rep.Plans = append(j.rep.Plans, row)
	}
}

// describedOr is what a description says of key, or def where it says nothing.
func describedOr(d map[string]string, key, def string) string {
	if v := d[key]; v != "" {
		return v
	}
	return def
}

// noManifest is the reason for a job that left no manifest.
func noManifest(b *Build) string {
	return fmt.Sprintf("repetition %d left no manifest: the job was killed, ran out of memory or timed out", b.Job.Rep)
}

// checkPebble checks the Pebble builds at one window on one architecture: each
// build's own record (the plan and stream, a binary a result may come from, without
// cgo, every answer the reference engine's, the regime), and then the set of every
// candidate's first repetition with [runner.Check] (J2). A reason of the set leaves
// every comparison there not decided.
func (j *judge) checkPebble(k scopeKey) {
	ps := &pebbleScope{}
	j.pebble[k] = ps
	plan := j.plans[k.window]
	limits, syncs := map[string]bool{}, map[string]bool{}
	var set []*runner.Candidate
	for _, ck := range j.sortedCells() {
		c := j.cells[ck]
		if ck.arch != k.arch || ck.window != k.window || engineOf(ck.cand) != "pebble" {
			continue
		}
		for _, b := range c.builds {
			m := b.Manifest
			if m == nil {
				c.why = append(c.why, noManifest(b))
				continue
			}
			say := func(format string, args ...any) {
				c.why = append(c.why, fmt.Sprintf("repetition %d: ", b.Job.Rep)+fmt.Sprintf(format, args...))
			}
			if plan != nil {
				j.checkRecord(b, plan, say)
			}
			if m.Build.CGO {
				say("built with cgo, which layout L on Pebble is not")
			}
			j.checkRegime(m.Describe, say)
			limits[describedOr(m.Describe, runner.GoMemoryLimitKey, "none")] = true
			syncs[describedOr(m.Describe, runner.SyncKey, "false")] = true
		}
		if first := c.first(); first == nil || first.Results == nil || first.Manifest == nil {
			ps.why = append(ps.why, fmt.Sprintf("%s has no read of repetition 1: the answers of the Pebble builds are not established", ck.cand))
		} else {
			set = append(set, &runner.Candidate{Manifest: first.Manifest, Results: first.Results})
		}
	}
	if plan != nil && len(set) > 0 {
		ps.why = append(ps.why, j.check(plan, set, false)...)
	}
	switch ls := slices.Sorted(maps.Keys(limits)); len(ls) {
	case 1:
		ps.limit = ls[0]
	case 0:
	default:
		ps.why = append(ps.why, fmt.Sprintf("the Pebble builds ran under %d Go memory limits (%s)", len(ls), strings.Join(ls, ", ")))
	}
	switch ss := slices.Sorted(maps.Keys(syncs)); len(ss) {
	case 1:
		ps.sync = ss[0]
	case 0:
	default:
		ps.why = append(ps.why, "the Pebble builds were built with and without a sync of every commit")
	}
}

// checkRecord checks what every build of either engine records of its plan: the
// plan, the stream, a binary a result may come from, and every answer the reference
// engine's before the compaction (and after, with the read of repetition 1).
func (j *judge) checkRecord(b *Build, plan *runner.Plan, say func(string, ...any)) {
	m, r := b.Manifest, b.Results
	digest, err := plan.Digest()
	if err != nil {
		say("%v", err)
		return
	}
	if m.PlanDigest != digest {
		say("built from another plan than the window's")
	}
	if m.Stream.Digest != plan.Stream.Digest {
		say("built from another stream than the plan's")
	}
	if m.Untimed {
		say("built by a binary a result may not come from (an untimed validation run)")
	}
	if n := len(m.UncompactedWrong); n > 0 {
		say("%d answers differ from the reference engine's before the compaction, first: %s", n, m.UncompactedWrong[0])
	}
	if r == nil {
		return
	}
	if r.PlanDigest != digest {
		say("read from another plan than the window's")
	}
	if len(r.Queries) != len(plan.Queries) {
		say("%d results for the plan's %d queries", len(r.Queries), len(plan.Queries))
	}
	if r.Untimed {
		say("read by a binary a result may not come from (an untimed validation run)")
	}
	if n := len(r.Mismatches); n > 0 {
		say("%d answers differ from the reference engine's, first: %s", n, r.Mismatches[0])
	}
	if n := len(r.Unstable); n > 0 {
		say("%d queries are unstable (their counters differ between two passes), first: %s", n, r.Unstable[0])
	}
	if m.Build != r.Build {
		say("built and read by different binaries")
	}
}

// checkRegime checks the regime both engines are built in: every retention settled,
// no rest after a retention.
func (j *judge) checkRegime(d map[string]string, say func(string, ...any)) {
	if v := describedOr(d, runner.SettleKey, "false"); v != "true" {
		say("settle_tombstones %q, not \"true\": every retention is settled", v)
	}
	if v := describedOr(d, runner.RestKey, "false"); v != "false" {
		say("rest_after_retention %q, not \"false\"", v)
	}
}

// checkRocks checks every build of a RocksDB candidate by the rule's own checks
// (J3): its record as [checkRecord] reads it, cgo in the build and the read, the
// perf context off, sync as the Pebble builds', the library's build stamp, the Go
// memory limit, and, for the read of repetition 1, the answers and comparable
// counters of the Pebble candidate of its policy.
func (j *judge) checkRocks(c *cell) {
	k := c.key
	plan := j.plans[k.window]
	ps := j.pebble[scopeKey{k.arch, k.window}]
	against := j.cells[cellKey{k.arch, k.window, PebblePrefix + policyOf(k.cand)}]
	for _, b := range c.builds {
		m, r := b.Manifest, b.Results
		if m == nil {
			c.why = append(c.why, noManifest(b))
			continue
		}
		say := func(format string, args ...any) {
			c.why = append(c.why, fmt.Sprintf("repetition %d: ", b.Job.Rep)+fmt.Sprintf(format, args...))
		}
		if plan != nil {
			j.checkRecord(b, plan, say)
		}
		if !m.Build.CGO || (r != nil && !r.Build.CGO) {
			say("built or read without cgo, which RocksDB needs: not a binary of rocksbench")
		}
		j.checkRegime(m.Describe, say)
		if v := m.Describe[PerfLevelKey]; v != "disable" {
			say("built with perf_level %q, not \"disable\": the perf context is off in every build and read the rule uses", v)
		}
		if r != nil && r.Describe[PerfLevelKey] != "disable" {
			say("read with perf_level %q, not \"disable\": the perf context is off in every build and read the rule uses", r.Describe[PerfLevelKey])
		}
		if ps == nil || ps.sync == "" || ps.limit == "" {
			say("no Pebble build at this window on this architecture to take the sync and the Go memory limit from")
		} else {
			if v := describedOr(m.Describe, runner.SyncKey, "false"); v != ps.sync {
				say("sync %q, not the Pebble builds' %q", v, ps.sync)
			}
			if plan != nil {
				if want, why := rocksLimit(ps.limit, plan); why != "" {
					say("%s", why)
				} else if v := describedOr(m.Describe, runner.GoMemoryLimitKey, "none"); v != want {
					say("go_memory_limit %q, not %q: the Pebble builds' %s less the plan's cache and four memtables", v, want, ps.limit)
				}
			}
		}
		for _, why := range stampWhy(m.Describe) {
			say("%s", why)
		}
		if r != nil && r.Describe[StampSHA256Key] != m.Describe[StampSHA256Key] {
			say("read with another build stamp than it was built with")
		}
		if b.Job.Rep != 1 {
			continue
		}
		if r == nil {
			say("not read: its answers and counters are not established")
			continue
		}
		first := (*Build)(nil)
		if against != nil {
			first = against.first()
		}
		if first == nil || first.Results == nil {
			say("no read of %s%s's repetition 1 to compare answers and counters with", PebblePrefix, policyOf(k.cand))
			continue
		}
		for _, why := range sameAnswers(r, first.Results) {
			say("%s", why)
		}
	}
}

// rocksLimit is the Go memory limit, as a manifest records it, of a RocksDB build
// beside Pebble builds that recorded pebble, or why there is none.
func rocksLimit(pebble string, plan *runner.Plan) (want, why string) {
	if pebble == "none" {
		return "none", ""
	}
	n, err := strconv.ParseInt(pebble, 10, 64)
	if err != nil {
		return "", fmt.Sprintf("the Pebble builds' go_memory_limit %q is not a number of bytes", pebble)
	}
	limit, err := GoMemoryLimit(n, Tuning(plan.CacheBytes))
	if err != nil {
		return "", err.Error()
	}
	if limit == 0 {
		return "none", ""
	}
	return strconv.FormatInt(limit, 10), ""
}

// stampWhy is why a build's description does not record a stamp the rule accepts:
// none, a sha256 that is not the text's, or a library not built portable and without
// malloc_usable_size.
func stampWhy(d map[string]string) []string {
	text, sum := d[StampKey], d[StampSHA256Key]
	if text == "" || text == "none" {
		return []string{"no build stamp of the RocksDB library: the build is of a library this rule does not name"}
	}
	var why []string
	if h := sha256.Sum256([]byte(text)); hex.EncodeToString(h[:]) != sum {
		why = append(why, "the build stamp's sha256 is not that of its text")
	}
	if v := stampValue(text, "portable"); v != "1" {
		why = append(why, fmt.Sprintf("the build stamp says portable=%q, not 1", v))
	}
	if v := stampValue(text, "malloc_usable_size"); v != "off" {
		why = append(why, fmt.Sprintf("the build stamp says malloc_usable_size=%q, not off", v))
	}
	return why
}

// sameAnswers is why a RocksDB read is not the Pebble read of the same policy, query
// by query: an answer that differs, or a comparable counter ([Comparable]).
func sameAnswers(rocks, pebble *runner.Results) []string {
	if len(rocks.Queries) != len(pebble.Queries) {
		return []string{fmt.Sprintf("%d results, against %d of %s", len(rocks.Queries), len(pebble.Queries), pebble.Candidate)}
	}
	var answers, counters []int
	for i := range rocks.Queries {
		rq, pq := rocks.Queries[i], pebble.Queries[i]
		if rq.Digest != pq.Digest || rq.Size != pq.Size {
			answers = append(answers, i)
		}
		if differingCounter(rq.Counters, pq.Counters) != "" {
			counters = append(counters, i)
		}
	}
	var why []string
	if len(answers) > 0 {
		why = append(why, fmt.Sprintf("%d answers differ from %s's, first: query %d", len(answers), pebble.Candidate, answers[0]))
	}
	if len(counters) > 0 {
		i := counters[0]
		why = append(why, fmt.Sprintf("%d queries count differently from %s's, first: query %d, %s", len(counters), pebble.Candidate, i, differingCounter(rocks.Queries[i].Counters, pebble.Queries[i].Counters)))
	}
	return why
}

// differingCounter is the first comparable counter, by name, that two reads count
// differently, as "name a against b", or "".
func differingCounter(a, b map[string]int64) string {
	names := map[string]bool{}
	for k := range a {
		names[k] = true
	}
	for k := range b {
		names[k] = true
	}
	for _, k := range slices.Sorted(maps.Keys(names)) {
		if Comparable(k) && a[k] != b[k] {
			return fmt.Sprintf("%s %d against %d", k, a[k], b[k])
		}
	}
	return ""
}

// checkArches checks what holds across every build of an engine on one
// architecture: one binary each, and one build stamp for every RocksDB build.
func (j *judge) checkArches() {
	type key struct{ arch, engine string }
	exes := map[key]map[string]bool{}
	stamps := map[string]map[string]bool{}
	for _, b := range j.s.Builds {
		if b.Manifest == nil || engineOf(b.Job.Candidate) == "" {
			continue
		}
		k := key{b.Job.Arch, engineOf(b.Job.Candidate)}
		if exes[k] == nil {
			exes[k] = map[string]bool{}
		}
		exes[k][b.Manifest.Build.Executable] = true
		if k.engine == "rocksdb" {
			if stamps[k.arch] == nil {
				stamps[k.arch] = map[string]bool{}
			}
			stamps[k.arch][describedOr(b.Manifest.Describe, StampSHA256Key, "none")] = true
		}
	}
	for _, k := range slices.SortedFunc(maps.Keys(exes), func(a, b key) int { return cmp.Or(cmp.Compare(a.arch, b.arch), cmp.Compare(a.engine, b.engine)) }) {
		if n := len(exes[k]); n > 1 {
			j.archWhy[k.arch] = append(j.archWhy[k.arch], fmt.Sprintf("the %s builds on %s ran %d binaries: an engine's builds on an architecture are of one", k.engine, k.arch, n))
		}
	}
	for _, arch := range slices.Sorted(maps.Keys(stamps)) {
		if n := len(stamps[arch]); n > 1 {
			j.archWhy[arch] = append(j.archWhy[arch], fmt.Sprintf("the RocksDB builds on %s record %d build stamps: every RL and RL4 build on an architecture links one library", arch, n))
		}
	}
}

// valueOf is a build's value of a gate, and whether it has one: G3, G2 and G4 as the
// runner reads them from a manifest, the peak resident set from the job file.
func valueOf(b *Build, gate string) (float64, bool) {
	m := b.Manifest
	if m == nil {
		return 0, false
	}
	switch gate {
	case GateG3:
		v := m.Timing.Writes.Quantile(0.99)
		return float64(v), v > 0
	case GateG2:
		n, err := strconv.Atoi(m.Describe[runner.PostRetentionKey])
		if err != nil {
			return 0, false
		}
		v, ok := stallOf(m.Timing, n)
		return float64(v), ok
	case GateG4:
		return float64(m.StatsCompacted["live_table_bytes"]) / float64(max(m.Stream.Records, 1)), true
	case GateRSS:
		return float64(b.Job.MaxRSSKB), b.Job.MaxRSSKB > 0
	}
	return 0, false
}

// stallOf is G2's value of a build, as the runner computes it: the largest, over its
// retentions, of the retention's time plus the excess over the build's median commit
// of each of the first n batches after it. ok is false when the build did not record
// the batches after its retentions or retained nothing.
func stallOf(t runner.Timing, n int) (stall int64, ok bool) {
	if len(t.Retains) == 0 || len(t.PostRetention) != len(t.Retains) {
		return 0, false
	}
	n = max(n, 0)
	m := t.Writes.Quantile(0.5)
	for i, retain := range t.Retains {
		s := retain
		for _, d := range t.PostRetention[i][:min(n, len(t.PostRetention[i]))] {
			s += max(0, d-m)
		}
		stall = max(stall, s)
	}
	return stall, true
}

// compareAll compares every RocksDB candidate with the Pebble candidate of its
// policy, on every gate, at every window, on every architecture.
func (j *judge) compareAll() {
	for _, k := range j.sortedCells() {
		if engineOf(k.cand) != "rocksdb" {
			continue
		}
		against := PebblePrefix + policyOf(k.cand)
		for _, gate := range Gates {
			j.rep.Comparisons = append(j.rep.Comparisons, j.compareCell(k, against, gate))
		}
	}
}

// compareCell compares a RocksDB cell with the Pebble cell of its policy on a gate.
func (j *judge) compareCell(k cellKey, against, gate string) Comparison {
	c := Comparison{Window: k.window, Arch: k.arch, Candidate: k.cand, Against: against, Gate: gate, Outcome: NotDecided}
	rocks, pebble := j.cells[k], j.cells[cellKey{k.arch, k.window, against}]
	var why []string
	why = append(why, j.archWhy[k.arch]...)
	if ps := j.pebble[scopeKey{k.arch, k.window}]; ps != nil && len(ps.why) > 0 {
		why = append(why, fmt.Sprintf("the Pebble builds at this window on this architecture give no result (%d reasons)", len(ps.why)))
	}
	if pebble == nil {
		why = append(why, fmt.Sprintf("no build of %s", against))
	} else if len(pebble.why) > 0 {
		why = append(why, fmt.Sprintf("%s gives no result (%d reasons)", against, len(pebble.why)))
	}
	if len(rocks.why) > 0 {
		why = append(why, fmt.Sprintf("%s gives no result (%d reasons)", k.cand, len(rocks.why)))
	}
	if len(why) > 0 {
		c.Why = strings.Join(why, "; ")
		return c
	}
	var smp [2][]sample
	for i, cl := range []*cell{rocks, pebble} {
		for _, b := range cl.builds {
			v, ok := valueOf(b, gate)
			if !ok {
				c.Why = fmt.Sprintf("repetition %d of %s has no %s value", b.Job.Rep, cl.key.cand, gate)
				return c
			}
			smp[i] = append(smp[i], sample{v, b.Job.CPUModel})
		}
	}
	return compare(c, smp[0], smp[1])
}

// find is the comparison of a candidate on a gate at a window on an architecture,
// or a comparison that is not decided because there is none.
func (j *judge) find(arch string, window int, cand, gate string) Comparison {
	for _, c := range j.rep.Comparisons {
		if c.Arch == arch && c.Window == window && c.Candidate == cand && c.Gate == gate {
			return c
		}
	}
	return Comparison{
		Window: window, Arch: arch, Candidate: cand, Against: PebblePrefix + policyOf(cand), Gate: gate, Outcome: NotDecided,
		Why: fmt.Sprintf("no build of %s at %d days on %s", cand, window, arch),
	}
}

// decide gives the verdict, from RL's and RL4's classes at the verdict's scope.
func (j *judge) decide() {
	for _, prefix := range []string{RocksPrefix, Rocks4Prefix} {
		cand := prefix + VerdictPolicy
		var gates []GateOutcome
		for _, gate := range Gates {
			gates = append(gates, guard(gate, j.find(DecidingArch, VerdictWindow, cand, gate), j.find(GuardArch, VerdictWindow, cand, gate)))
		}
		j.rep.Classes = append(j.rep.Classes, classify(cand, gates))
	}
	j.rep.Verdict, j.rep.Why = verdictOf(j.rep.Classes[0], j.rep.Classes[1])
	if len(j.rep.Refused) > 0 {
		j.rep.Verdict, j.rep.Why = "", nil
	}
}

// rows fills in the builds and the reasons that are not results.
func (j *judge) rows() {
	for _, k := range j.scopes() {
		for _, why := range j.pebble[k].why {
			j.rep.NotResults = append(j.rep.NotResults, fmt.Sprintf("the Pebble builds at %d days on %s: %s", k.window, k.arch, why))
		}
	}
	for _, arch := range slices.Sorted(maps.Keys(j.archWhy)) {
		j.rep.NotResults = append(j.rep.NotResults, j.archWhy[arch]...)
	}
	for _, k := range j.sortedCells() {
		c := j.cells[k]
		for _, why := range c.why {
			j.rep.NotResults = append(j.rep.NotResults, fmt.Sprintf("%s at %d days on %s, %s", k.cand, k.window, k.arch, why))
		}
		for _, b := range c.builds {
			row := BuildRow{
				Window: k.window, Arch: k.arch, Candidate: k.cand, CPUModel: b.Job.CPUModel, Rep: b.Job.Rep,
				Manifest: b.Manifest != nil, Read: b.Results != nil, Values: map[string]float64{},
			}
			for _, gate := range Gates {
				if v, ok := valueOf(b, gate); ok {
					row.Values[gate] = v
				}
			}
			j.rep.Builds = append(j.rep.Builds, row)
		}
	}
}
