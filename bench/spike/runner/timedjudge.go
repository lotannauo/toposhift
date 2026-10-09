package runner

import (
	"cmp"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
)

// The words of the log of 2026-10-07 in code. They are not fields of [Rules], which
// that log did not change, so that the digest of the rules is the one the plans were
// made under. The reference set is [timingBaseline] and [timingChosenPolicy], and
// [timingJoiner] joins it if, at [timingJoinWindow] days, its median 99th-percentile
// commit is more than 10% below the baseline's on either architecture: in integers,
// 10*joiner < 9*baseline, so that exactly 10% below does not join.
const (
	timingBaseline     = "L/off"       // in the reference set
	timingChosenPolicy = "L/k64a2l1ns" // the checkpoint policy G1 chose: K_min 64, alpha 2, lag 1 ns
	timingJoiner       = "M/crdb1"     // joins the reference set by the rule above
	timingJoinWindow   = 7             // the window the join is decided at, in days
	timingMinReps      = 3             // repetitions per candidate and architecture for a judged timing
)

// timingAAFloor is the factor over which L/off's own values may vary over its repetitions
// before a comparison with it is not decided. The log of 2026-10-08 says: "A G3 comparison
// in a scope where L/off's own 99th-percentile commit varies by more than a factor of 1.5
// over its repetitions is not decided." In integers: 2*max > 3*min.
const timingAAFloor = 1.5

// settleDeadlineKey is the key of a manifest's Describe that gives the deadline of the
// wait a retention makes for the database to be at rest (the engine's own description).
const settleDeadlineKey = "settle_deadline"

// timingArches are the architectures a gate must pass on, in the order they are shown.
var timingArches = []string{"aarch64", "x86_64"}

// The gates of a verdict, in the order they are shown.
const (
	GateG0         = "G0"
	GateG2         = "G2"
	GateG3Commit   = "G3 commit"
	GateG4         = "G4"
	GateCheckpoint = "checkpoint share"
)

var timingGates = []string{GateG0, GateG2, GateG3Commit, GateG4, GateCheckpoint}

// ArchBoth is the architecture of the verdict that needs both.
const ArchBoth = "both"

// TimingOptions are what the judgement is given besides the artifacts.
type TimingOptions struct {
	Rules Rules // DefaultRules() in the command; tests may pass others only to test arithmetic
	// Ref is the ref a revision must be an ancestor of; "" when no check was asked for.
	Ref string
	// Check says whether rev is an ancestor of Ref; nil when no check was asked for. It is
	// called once per distinct revision (the builds', and the join set's if it differs),
	// in sorted order.
	Check func(rev string) RevisionCheck
}

// RevisionCheck is the answer of a check that a revision is an ancestor of a ref.
type RevisionCheck struct {
	Ancestor bool
	Err      string // the check could not be made: why ("" when it was made)
}

// TimingReport is the judgement of G2, G3 and G4 from the timed builds of the bench
// workflow: what was judged, what stops it from being judged, and each verdict.
type TimingReport struct {
	RulesDigest     string
	RulesVersion    int
	Judged          bool
	NotJudged       []string // sorted, deduplicated; empty, never nil
	Problems        []string // sorted, deduplicated; empty, never nil
	Findings        []string // sorted, deduplicated; empty, never nil: what the builds show about the engine, which is not a defect of the inputs
	Revision        string
	RevisionChecked bool   // the check was made and found the revision an ancestor
	RevisionRef     string // "" when not asked for
	Family          string
	PinsWindow      int
	Runs            []string // run ids, sorted
	Reference       []string // the reference set in effect, in the order baseline, chosen policy, joiner
	Join            TimingJoin
	Builds          []TimingBuild
	Verdicts        []TimingVerdict
	Diagnostics     []TimingDiagnostic

	// What the text states of the inputs and of the rules, which the fields above do not hold.
	RevisionError string // why the check could not be made; "" when it was made or not asked for
	Settings      TimingSettings
	Binaries      []TimingBinary
	Plans         []TimingPlan
	Rule          TimingRule
}

// TimingRule is the numbers of the rules the judgement used, which the text of the rule
// states.
type TimingRule struct {
	StallBudgetSeconds, CommitFactor, BytesPerRecordFactor float64
	PostRetentionBatches                                   int
}

// TimingDiagnostic is a number that is shown and does not decide: the first batch after
// a retention with the verdict the former rule gave it, the retention alone, and the
// settling of a retention. Value and Best are nanoseconds; Text is what the line says
// after the value; FormerPass is the former rule's verdict, for "first batch" only.
type TimingDiagnostic struct {
	Window                 int
	Candidate, Arch, Scope string
	What                   string // "first batch", "retention alone" or "settling"
	Reps                   int
	Value, Best            int64
	Text                   string
	FormerPass             bool
}

// TimingSettings are the choices the builds were made with, as their manifests record
// them (those of the first build, when they differ, which is a problem).
type TimingSettings struct {
	Sync, RestAfterRetention, CanonicalLayout, MetricsEvery string
	SettleTombstones, SettleDeadline, PostRetentionBatches  string
	// GoMemoryLimit is the Go memory limit in bytes, or "none".
	GoMemoryLimit string
}

// TimingBinary is a binary the builds of one architecture ran.
type TimingBinary struct {
	Arch, Executable, GoVersion string
}

// TimingPlan is the plan the builds of a window were made from, and whether it was
// among the inputs.
type TimingPlan struct {
	Window              int
	Digest, RulesDigest string
	Records             uint64
	Found               bool
	StreamDigest        string
}

// TimingJoin is whether the joiner of the reference set joined it, and from what.
type TimingJoin struct {
	Decided, Joins bool
	Reason         string
	Revision       string
	Runs           []string
	Arches         []TimingJoinArch
}

// TimingJoinArch is the comparison of the joiner with the baseline at the join window
// on one architecture. Scope is the CPU model, or the models pooled; where there are
// rows in several models, Below is whether it is below in every one and the figures
// are those of the first.
type TimingJoinArch struct {
	Arch, Scope              string
	Comparable, Below        bool
	JoinerP99, BaselineP99   int64 // ns, medians
	JoinerReps, BaselineReps int
}

// TimingBuild is one timed build, as the judgement saw it. It carries no directory
// and no runner name, so the report of the same artifacts is the same bytes wherever
// they were downloaded.
type TimingBuild struct {
	Window                                      int
	Candidate, Arch, CPUModel, OS, ImageVersion string
	RunID, RunAttempt                           string
	Rep                                         int
	CommitP99, AfterRetention, LongestRetention int64 // ns; 0 when absent
	Stall                                       int64 // ns, G2's value; 0 when the build did not record the batches after its retentions
	SettleLongest                               int64 // ns, the longest wait of a retention for the database to be at rest; 0 when none
	SettleDeadlineHits                          int
	BytesPerRecord, CheckpointShare             float64
}

// TimingVerdict is how one candidate does on one gate at one window, on one
// architecture or on both.
type TimingVerdict struct {
	Window                       int
	Candidate, Gate, Arch, Scope string // Gate: "G0", "G2", "G3 commit", "G4", "checkpoint share"; Arch: "aarch64", "x86_64", "both", or "" for G0
	Reps                         int
	Value, Best, Limit           float64 // ns for G2 and G3, bytes per record for G4, a share for the checkpoints; 0 where not applicable
	ValueText, LimitText         string  // the text columns
	Pass, Judged, NotComparable  bool
	Reason                       string // why not judged; "" when judged
}

// lowerMedian is the median of xs, the lower of the two middle values for an even
// number: the definition the rules give for the median population of G1. It sorts a
// copy, and is the zero value for no values.
func lowerMedian[T cmp.Ordered](xs []T) T {
	var zero T
	if len(xs) == 0 {
		return zero
	}
	s := slices.Clone(xs)
	slices.Sort(s)
	return s[(len(s)-1)/2]
}

// tbuild is a build with the numbers the gates read from its manifest.
type tbuild struct {
	TimedBuild
	g gateMeasure
}

func (b *tbuild) label() string {
	return fmt.Sprintf("%s at %d days on %s", b.Job.Candidate, b.Job.Window, b.Job.Arch)
}

func newBuilds(bs []TimedBuild) []*tbuild {
	out := make([]*tbuild, len(bs))
	for i, b := range bs {
		out[i] = &tbuild{TimedBuild: b, g: measureOf(b.Manifest)}
	}
	slices.SortStableFunc(out, func(a, b *tbuild) int { return compareBuilds(a.TimedBuild, b.TimedBuild) })
	return out
}

func short(s string) string {
	if len(s) > 12 {
		return s[:12]
	}
	return s
}

// list joins words as "a, b and c".
func list(xs []string) string {
	switch len(xs) {
	case 0:
		return ""
	case 1:
		return xs[0]
	}
	return strings.Join(xs[:len(xs)-1], ", ") + " and " + xs[len(xs)-1]
}

func sortedSet(m map[string]bool) []string { return sortedKeys(m) }

// g0State is what is known of G0 of a candidate on a plan.
type g0State int

const (
	g0Unknown g0State = iota
	g0Pass
	g0Fail
)

func (s g0State) String() string {
	switch s {
	case g0Pass:
		return "passed"
	case g0Fail:
		return "failed"
	}
	return "not established"
}

// g0Result is G0 of one candidate on one plan: the timed builds' answers before the
// compaction and the counters runs' answers.
type g0Result struct {
	state                        g0State
	plan                         string
	wrong, uncompacted, unstable int
	runs                         int
}

// why is the sentence for a G0 that is not a pass.
func (g g0Result) why(cand string) string {
	if g.state == g0Fail {
		return fmt.Sprintf("G0 of %s failed", cand)
	}
	return fmt.Sprintf("G0 of %s is not established: no counters run of plan %s read it", cand, short(g.plan))
}

type judge struct {
	opts      TimingOptions
	r         Rules
	builds    []*tbuild
	joinSet   []*tbuild
	joinGiven bool
	all       []*tbuild
	plans     map[string]*Plan
	counters  []*Candidate
	rep       TimingReport
	problems  map[string]bool

	revisions   map[string]RevisionCheck
	binaryWhy   string
	settingsWhy string
	revisionWhy string
	joinWhy     string
	reference   []string
	g0cache     map[[2]string]g0Result
	g2modes     map[string]g2Mode
	loaded      []loadedPlan
	findings    map[string]bool
}

// loadedPlan is a plan found among the inputs and its digest.
type loadedPlan struct {
	digest string
	plan   *Plan
}

// window is the builds of one window.
type window struct {
	days   int
	plan   string // the digest of the plan of the builds ("" if there is none)
	byCand map[string]map[string][]*tbuild
	cands  []string
	g0     map[string]g0Result
	// bestWhy is why the best of G3 and G4 is not established at this window, "" if it is.
	bestWhy string
}

// JudgeTiming judges G2, G3 and G4 from the timed builds, by the rules the logs of
// 2026-10-07 and 2026-10-08 state, with the arithmetic of [Gates]. G2 is, for a
// candidate, the largest over its repetitions of the build's largest S_i, a retention's
// time plus the excess of the batches after it, within the budget, or, on an
// architecture at a window where no member of the reference set fits the budget, within
// the factor of the best of them (and then a finding says so). G3 is the median
// 99th-percentile commit over the repetitions against the best median of the reference
// set, not decided where L/off's own commit varies over its repetitions by more than
// [timingAAFloor]; G4 the median bytes per record against the best median. A gate passes
// only if it passes on both architectures, and a timing is compared only within one CPU
// model (or pooled, where every candidate compared has [timingMinReps] repetitions). The
// verdicts are labelled "CI timing" only where every precondition holds, and each says
// why it is not judged otherwise. It reads nothing and runs nothing; opts.Check is the
// only thing it asks of the world.
func JudgeTiming(in TimedInputs, opts TimingOptions) TimingReport {
	j := &judge{
		opts: opts, r: opts.Rules, builds: newBuilds(in.Builds), plans: map[string]*Plan{}, counters: in.Counters,
		problems: map[string]bool{}, joinGiven: in.JoinFrom != nil, g0cache: map[[2]string]g0Result{},
		g2modes: map[string]g2Mode{}, findings: map[string]bool{},
	}
	for _, p := range append(slices.Clone(in.Plans), in.JoinPlans...) {
		if d, err := p.Digest(); err == nil {
			// The digest of a plan does not cover the rules it was made under, so two plans
			// can share it: each is checked, whichever is kept.
			j.loaded = append(j.loaded, loadedPlan{d, p})
			if _, ok := j.plans[d]; !ok {
				j.plans[d] = p
			}
		}
	}
	j.joinSet = j.builds
	j.all = j.builds
	if j.joinGiven {
		j.joinSet = newBuilds(in.JoinFrom)
		j.all = append(slices.Clone(j.builds), j.joinSet...)
	}
	j.rep = TimingReport{
		RulesVersion: j.r.Version, NotJudged: []string{}, Problems: []string{}, Findings: []string{}, Runs: []string{}, Reference: []string{}, RevisionRef: opts.Ref,
		Join: TimingJoin{Runs: []string{}, Arches: []TimingJoinArch{}}, Builds: []TimingBuild{}, Verdicts: []TimingVerdict{}, Diagnostics: []TimingDiagnostic{}, Binaries: []TimingBinary{}, Plans: []TimingPlan{},
		Rule: TimingRule{StallBudgetSeconds: j.r.StallBudgetSeconds, CommitFactor: j.r.CommitFactor, BytesPerRecordFactor: j.r.BytesPerRecordFactor, PostRetentionBatches: j.r.PostRetentionBatches},
	}
	if d, err := j.r.Digest(); err == nil {
		j.rep.RulesDigest = d
	}
	j.findProblems()
	j.describe()
	j.decideJoin()
	j.judgeWindows()
	j.finish()
	return j.rep
}

// findProblems records why the inputs cannot be judged together.
func (j *judge) findProblems() {
	j.checkSet(j.builds, "")
	if j.joinGiven {
		j.checkSet(j.joinSet, "the join set: ")
		j.checkJoinSet()
	}
	// A plan made under other rules than this build's.
	rulesNow := j.rep.RulesDigest
	slices.SortStableFunc(j.loaded, func(a, b loadedPlan) int {
		return cmp.Or(cmp.Compare(a.digest, b.digest), cmp.Compare(a.plan.RulesDigest, b.plan.RulesDigest))
	})
	for _, lp := range j.loaded {
		d, p := lp.digest, lp.plan
		if p.RulesDigest == rulesNow {
			continue
		}
		windows := map[string]bool{}
		for _, b := range j.all {
			if b.Manifest.PlanDigest == d {
				windows[strconv.Itoa(b.Job.Window)] = true
			}
		}
		name := "the plan " + short(d)
		if ws := sortedSet(windows); len(ws) > 0 {
			slices.SortFunc(ws, func(a, b string) int { x, _ := strconv.Atoi(a); y, _ := strconv.Atoi(b); return cmp.Compare(x, y) })
			name = "the plan of " + list(ws) + " days"
		}
		j.problem("%s was made under rules %s; this binary judges by rules %s", name, short(p.RulesDigest), short(rulesNow))
	}
	// The revision is an ancestor of the ref.
	j.revisions = map[string]RevisionCheck{}
	if j.opts.Check != nil {
		revs := map[string]bool{}
		for _, b := range j.all {
			revs[b.Manifest.Build.Revision] = true
		}
		for _, rev := range sortedSet(revs) {
			c := j.opts.Check(rev)
			j.revisions[rev] = c
			if c.Err == "" && !c.Ancestor {
				j.problem("revision %s is not an ancestor of %s: a timed run is dispatched from main", short(rev), j.opts.Ref)
			}
		}
	}
	for p := range j.problems {
		j.rep.Problems = append(j.rep.Problems, p)
	}
	sort.Strings(j.rep.Problems)
}

// checkJoinSet applies the refusals about a join set that is not the builds themselves:
// it must be of the same family, with pins from the same window, and its plan of the
// join window must be among the inputs and the builds' own, where they have one.
func (j *judge) checkJoinSet() {
	if len(j.builds) == 0 || len(j.joinSet) == 0 {
		return
	}
	families := func(bs []*tbuild) string {
		set := map[string]bool{}
		for _, b := range bs {
			set[b.Job.Family] = true
		}
		return strings.Join(sortedSet(set), ", ")
	}
	pins := func(bs []*tbuild) string {
		set := map[string]bool{}
		for _, b := range bs {
			set[strconv.Itoa(b.Job.PinsWindow)] = true
		}
		ps := sortedSet(set)
		slices.SortFunc(ps, func(a, b string) int { x, _ := strconv.Atoi(a); y, _ := strconv.Atoi(b); return cmp.Compare(x, y) })
		return strings.Join(ps, ", ")
	}
	if a, b := families(j.joinSet), families(j.builds); a != b {
		j.problem("the join set: its builds are of family %s and the builds judged are of family %s", a, b)
	}
	if a, b := pins(j.joinSet), pins(j.builds); a != b {
		j.problem("the join set: its pins were chosen from the %s-day window and those of the builds judged from the %s-day window", a, b)
	}
	own := map[string]bool{}
	for _, b := range j.builds {
		if b.Job.Window == timingJoinWindow {
			own[b.Manifest.PlanDigest] = true
		}
	}
	seen := map[string]bool{}
	for _, b := range j.joinSet {
		d := b.Manifest.PlanDigest
		if b.Job.Window != timingJoinWindow || seen[d] {
			continue
		}
		seen[d] = true
		if _, ok := j.plans[d]; !ok {
			j.problem("the join set: no plan of %d days (%s) among the inputs: give its bench-plans artifact with -join-from", timingJoinWindow, short(d))
		}
		if len(own) > 0 && !own[d] {
			j.problem("the join set: its plan of %d days (%s) is not the builds' (%s)", timingJoinWindow, short(d), short(sortedSet(own)[0]))
		}
	}
}

func (j *judge) problem(format string, args ...any) { j.problems[fmt.Sprintf(format, args...)] = true }

// checkSet applies the refusals about the builds of one set among themselves.
func (j *judge) checkSet(builds []*tbuild, prefix string) {
	add := func(format string, args ...any) { j.problems[prefix+fmt.Sprintf(format, args...)] = true }
	if len(builds) == 0 {
		return
	}
	first := builds[0]

	revs := map[string]bool{}
	for _, b := range builds {
		revs[b.Manifest.Build.Revision] = true
	}
	switch rs := sortedSet(revs); {
	case len(rs) == 2:
		add("builds of two revisions, %s and %s", short(rs[0]), short(rs[1]))
	case len(rs) > 2:
		short12 := make([]string, len(rs))
		for i, r := range rs {
			short12[i] = short(r)
		}
		add("builds of %d revisions, %s", len(rs), list(short12))
	}

	binaries := map[string]map[string]bool{}
	for _, b := range builds {
		if binaries[b.Job.Arch] == nil {
			binaries[b.Job.Arch] = map[string]bool{}
		}
		binaries[b.Job.Arch][b.Manifest.Build.Executable+" "+b.Manifest.Build.GoVersion] = true
	}
	for _, arch := range sortedKeysOf(binaries) {
		if len(binaries[arch]) > 1 {
			add("on %s the builds were made by different binaries", arch)
		}
	}

	// The settings that make timings alike, in the words of [Check].
	diff := func(value func(*Manifest) string, sentence func(a, b, x, y string) string) {
		for _, b := range builds[1:] {
			if x, y := value(first.Manifest), value(b.Manifest); x != y {
				add("%s", sentence(first.label(), b.label(), x, y))
				return
			}
		}
	}
	diff(func(m *Manifest) string { return describedOr(m, SyncKey, "false") }, func(a, b, x, y string) string {
		return fmt.Sprintf("%s and %s were built with and without a sync of every commit (%q and %q): their timings are not comparable", a, b, x, y)
	})
	diff(func(m *Manifest) string { return m.Describe[RestKey] }, func(a, b, x, y string) string {
		return fmt.Sprintf("%s and %s were built with and without a rest after each retention (%q and %q): their timings are not comparable", a, b, x, y)
	})
	diff(func(m *Manifest) string { return describedOr(m, CanonicalKey, "false") }, func(a, b, x, y string) string {
		return fmt.Sprintf("%s and %s were built with and without the canonical layout (%q and %q): the blocks their reads load are not comparable", a, b, x, y)
	})
	diff(func(m *Manifest) string { return describedOr(m, MetricsKey, "0s") }, func(a, b, x, y string) string {
		return fmt.Sprintf("%s and %s were built with different metric sampling (%q and %q): their timings are not comparable", a, b, x, y)
	})
	diff(func(m *Manifest) string { return describedOr(m, SettleKey, "false") }, func(a, b, x, y string) string {
		return fmt.Sprintf("%s and %s were built with and without settling a retention's tombstones (%q and %q): their timings are not comparable", a, b, x, y)
	})
	diff(func(m *Manifest) string { return m.Describe[settleDeadlineKey] }, func(a, b, x, y string) string {
		return fmt.Sprintf("%s and %s settled a retention with different deadlines (%q and %q): their timings are not comparable", a, b, x, y)
	})
	diff(func(m *Manifest) string { return m.Describe[PostRetentionKey] }, func(a, b, x, y string) string {
		return fmt.Sprintf("%s and %s recorded different numbers of batches after a retention (%q and %q): their timings are not comparable", a, b, x, y)
	})
	// The limit is a setting of the run, chosen before it, like the absence of one: no
	// precondition below refuses a build for the limit it recorded, and the judge does
	// not check at which window a limit is used. It refuses only a mix within one
	// judgement, because a limit changes when the collector runs and so the timings.
	diff(func(m *Manifest) string { return describedOr(m, GoMemoryLimitKey, "none") }, func(a, b, x, y string) string {
		return fmt.Sprintf("%s and %s were built with different Go memory limits (%q and %q): their timings are not comparable", a, b, x, y)
	})
	for _, b := range builds[1:] {
		if d := optionsDiff(first.Manifest.Describe["pebble_options"], b.Manifest.Describe["pebble_options"]); d != "" {
			add("%s and %s ran with different Pebble options: %s", first.label(), b.label(), d)
			break
		}
	}

	// Plans and windows.
	plansAt := map[int]map[string]bool{}
	windowsOf := map[string]map[int]bool{}
	byPlan := map[string]*tbuild{}
	for _, b := range builds {
		d := b.Manifest.PlanDigest
		if plansAt[b.Job.Window] == nil {
			plansAt[b.Job.Window] = map[string]bool{}
		}
		plansAt[b.Job.Window][d] = true
		if windowsOf[d] == nil {
			windowsOf[d] = map[int]bool{}
		}
		windowsOf[d][b.Job.Window] = true
		if other, ok := byPlan[d]; !ok {
			byPlan[d] = b
		} else if o, m := other.Manifest.Stream, b.Manifest.Stream; o.Digest != m.Digest || o.Records != m.Records {
			add("the builds of plan %s hold different streams (%s and %s)", short(d), short(o.Digest), short(m.Digest))
		}
	}
	for _, w := range sortedIntKeys(plansAt) {
		if ds := sortedSet(plansAt[w]); len(ds) > 1 {
			add("two plans at %d days, %s and %s", w, short(ds[0]), short(ds[1]))
		}
	}
	for _, d := range sortedKeysOf(windowsOf) {
		if ws := sortedIntKeys(windowsOf[d]); len(ws) > 1 {
			add("plan %s was built at two windows, %d and %d days", short(d), ws[0], ws[1])
		}
		if p, ok := j.plans[d]; ok {
			m := byPlan[d].Manifest.Stream
			if p.Stream.Digest != m.Digest || p.Stream.Records != m.Records {
				add("the plan of %d days (%s) does not hold the stream the builds were made of (stream %s against %s, %d against %d records)",
					byPlan[d].Job.Window, short(d), short(p.Stream.Digest), short(m.Digest), p.Stream.Records, m.Records)
			}
		}
	}

	families, pins := map[string]bool{}, map[int]bool{}
	for _, b := range builds {
		families[b.Job.Family], pins[b.Job.PinsWindow] = true, true
	}
	if fs := sortedSet(families); len(fs) > 1 {
		add("builds of %d families, %s", len(fs), list(fs))
	}
	if ps := sortedIntKeys(pins); len(ps) > 1 {
		strs := make([]string, len(ps))
		for i, p := range ps {
			strs[i] = strconv.Itoa(p)
		}
		add("the pins were chosen from different windows (%s days)", list(strs))
	}

	seen := map[string]bool{}
	for _, b := range builds {
		k := fmt.Sprintf("%s %s %d %s %d", b.Job.RunID, b.Job.OS, b.Job.Window, b.Job.Candidate, b.Job.Rep)
		if seen[k] {
			add("the same job twice: %s, repetition %d of run %s on %s", b.label(), b.Job.Rep, b.Job.RunID, b.Job.OS)
		}
		seen[k] = true
	}
}

func sortedKeysOf[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortedIntKeys[V any](m map[int]V) []int {
	out := make([]int, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Ints(out)
	return out
}

// buildsWhy is why the binaries or the settings of the builds are not ones a result
// may come from, or "": the second and third precondition. The settings include, after
// the pre-registered ones, that each retention settled its tombstones and that the
// batches after it were recorded as the rules count them. The two come back apart, so
// that the one that stops the judgement first is the one said.
func (j *judge) buildsWhy(builds []*tbuild) (binary, settings string) {
	for _, b := range builds {
		if b.Manifest.Untimed {
			return fmt.Sprintf("%s was built by a binary a result may not come from: an untimed validation run", b.label()), ""
		}
		if err := (Guards{Info: b.Manifest.Build}).Check(); err != nil {
			why := strings.TrimPrefix(err.Error(), "runner: this binary is not one a result may come from: ")
			why = strings.TrimSuffix(why, " (use -untimed for a validation run)")
			return fmt.Sprintf("%s was built by a binary a result may not come from: %s", b.label(), why), ""
		}
	}
	for _, b := range builds {
		if describedOr(b.Manifest, SyncKey, "false") != "false" {
			return "", "these builds synced every commit: G2, G3 and G4 are judged on builds that do not, and a synced build is a separate measurement"
		}
	}
	for _, b := range builds {
		switch b.Manifest.Describe[RestKey] {
		case "false":
		case "true":
			return "", "these builds rested after each retention"
		default:
			return "", "these builds do not record whether they rested after each retention"
		}
	}
	for _, b := range builds {
		if describedOr(b.Manifest, CanonicalKey, "false") != "false" {
			return "", "these builds were rewritten into the canonical layout"
		}
	}
	for _, b := range builds {
		if d := describedOr(b.Manifest, MetricsKey, "0s"); d != "0s" {
			if v, err := time.ParseDuration(d); err != nil || v != 0 {
				return "", fmt.Sprintf("these builds sampled their metrics every %s, which slows a build", d)
			}
		}
	}
	for _, b := range builds {
		if describedOr(b.Manifest, SettleKey, "false") != "true" {
			return "", fmt.Sprintf("these builds did not settle a retention's tombstones: rules %d judge only builds that do", j.r.Version)
		}
	}
	for _, b := range builds {
		m := b.Manifest
		recorded := m.Describe[PostRetentionKey] == strconv.Itoa(j.r.PostRetentionBatches)
		if recorded && len(m.Timing.Retains) > 0 && len(m.Timing.PostRetention) != len(m.Timing.Retains) {
			recorded = false
		}
		if !recorded {
			value := m.Describe[PostRetentionKey]
			if value == "" {
				value = "not recorded"
			}
			return "", fmt.Sprintf("these builds did not record the batches after a retention as rules %d count them (%s)", j.r.Version, value)
		}
	}
	return "", ""
}

// describe fills in what the report says of the inputs and the preconditions that are
// the same for every row.
func (j *judge) describe() {
	rep := &j.rep
	j.binaryWhy, j.settingsWhy = j.buildsWhy(j.builds)
	if len(j.builds) > 0 {
		revs, runs, families := map[string]bool{}, map[string]bool{}, map[string]bool{}
		pinsWindow := j.builds[0].Job.PinsWindow
		for _, b := range j.builds {
			revs[b.Manifest.Build.Revision], runs[b.Job.RunID], families[b.Job.Family] = true, true, true
			pinsWindow = min(pinsWindow, b.Job.PinsWindow)
		}
		rep.Revision, rep.Runs, rep.Family, rep.PinsWindow = sortedSet(revs)[0], sortedSet(runs), sortedSet(families)[0], pinsWindow
		first := j.builds[0].Manifest
		rep.Settings = TimingSettings{
			Sync: describedOr(first, SyncKey, "false"), RestAfterRetention: first.Describe[RestKey],
			CanonicalLayout: describedOr(first, CanonicalKey, "false"), MetricsEvery: describedOr(first, MetricsKey, "0s"),
			SettleTombstones: describedOr(first, SettleKey, "false"), SettleDeadline: first.Describe[settleDeadlineKey],
			PostRetentionBatches: first.Describe[PostRetentionKey],
			GoMemoryLimit:        describedOr(first, GoMemoryLimitKey, "none"),
		}
		seen := map[TimingBinary]bool{}
		for _, b := range j.builds {
			tb := TimingBinary{Arch: b.Job.Arch, Executable: b.Manifest.Build.Executable, GoVersion: b.Manifest.Build.GoVersion}
			if !seen[tb] {
				seen[tb] = true
				rep.Binaries = append(rep.Binaries, tb)
			}
		}
		slices.SortFunc(rep.Binaries, func(a, b TimingBinary) int {
			return cmp.Or(cmp.Compare(a.Arch, b.Arch), cmp.Compare(a.Executable, b.Executable), cmp.Compare(a.GoVersion, b.GoVersion))
		})
		plans := map[[2]string]bool{}
		for _, b := range j.builds {
			k := [2]string{strconv.Itoa(b.Job.Window), b.Manifest.PlanDigest}
			if plans[k] {
				continue
			}
			plans[k] = true
			tp := TimingPlan{Window: b.Job.Window, Digest: b.Manifest.PlanDigest, StreamDigest: b.Manifest.Stream.Digest, Records: b.Manifest.Stream.Records}
			if p, ok := j.plans[b.Manifest.PlanDigest]; ok {
				tp.Found, tp.RulesDigest, tp.Records = true, p.RulesDigest, p.Stream.Records
			}
			rep.Plans = append(rep.Plans, tp)
		}
		slices.SortFunc(rep.Plans, func(a, b TimingPlan) int {
			return cmp.Or(cmp.Compare(a.Window, b.Window), cmp.Compare(a.Digest, b.Digest))
		})
	}

	ref := j.opts.Ref
	if ref == "" {
		ref = "origin/main"
	}
	if j.opts.Check == nil {
		j.revisionWhy = fmt.Sprintf("the revision was not checked against %s (give -git with a checkout of the repository)", ref)
		return
	}
	for _, rev := range sortedKeysOf(j.revisions) {
		if c := j.revisions[rev]; c.Err != "" {
			j.revisionWhy = fmt.Sprintf("the revision could not be checked against %s: %s", ref, c.Err)
			rep.RevisionError = c.Err
			break
		}
	}
	rep.RevisionChecked = len(j.revisions) > 0
	for _, c := range j.revisions {
		if c.Err != "" || !c.Ancestor {
			rep.RevisionChecked = false
		}
	}
}

// g0Of is G0 of a candidate on a plan: it fails if any timed build of it on the plan
// answered wrongly before compaction or any counters run of it on the plan fails G0,
// passes if a counters run exists, and is not established otherwise.
func (j *judge) g0Of(plan, cand string) g0Result {
	key := [2]string{plan, cand}
	if r, ok := j.g0cache[key]; ok {
		return r
	}
	res := g0Result{plan: plan}
	fail := false
	for _, b := range j.all {
		if b.Manifest.PlanDigest == plan && b.Job.Candidate == cand {
			res.uncompacted = max(res.uncompacted, len(b.Manifest.UncompactedWrong))
			fail = fail || len(b.Manifest.UncompactedWrong) > 0
		}
	}
	for _, c := range j.counters {
		if c.Results.PlanDigest != plan || c.Results.Candidate != cand {
			continue
		}
		res.runs++
		res.wrong = max(res.wrong, len(c.Results.Mismatches))
		res.unstable = max(res.unstable, len(c.Results.Unstable))
		res.uncompacted = max(res.uncompacted, len(c.Manifest.UncompactedWrong))
		fail = fail || !passesG0(c)
	}
	switch {
	case fail:
		res.state = g0Fail
	case res.runs > 0:
		res.state = g0Pass
	}
	j.g0cache[key] = res
	return res
}

// sample is the value of one build for a timing gate, and the CPU model it ran on.
type sample struct {
	v     int64
	model string
}

// scopeRow is a set of builds a timing is compared within.
type scopeRow struct {
	scope         string
	samples       map[string][]sample
	notComparable bool
	text          string
}

// scopes cuts the samples of the candidates compared (in the order given) into the
// sets within which they may be compared: one CPU model if all ran on one; else all
// pooled, if every candidate has at least [timingMinReps]; else one set for each model
// every candidate ran on; else none that can be compared.
func scopes(order []string, smp map[string][]sample) []scopeRow {
	for _, n := range order {
		if len(smp[n]) == 0 { // a candidate with builds and no value cannot be compared
			return []scopeRow{{notComparable: true, samples: smp, text: fmt.Sprintf("not comparable: %s has no value to compare", n)}}
		}
	}
	models := map[string]bool{}
	allMin := true
	for _, n := range order {
		allMin = allMin && len(smp[n]) >= timingMinReps
		for _, s := range smp[n] {
			models[s.model] = true
		}
	}
	ms := sortedSet(models)
	if len(ms) == 1 {
		return []scopeRow{{scope: ms[0], samples: smp}}
	}
	if allMin {
		return []scopeRow{{scope: "pooled over " + strings.Join(ms, ", "), samples: smp}}
	}
	var rows []scopeRow
	for _, m := range ms {
		row := scopeRow{scope: m, samples: map[string][]sample{}}
		ok := true
		for _, n := range order {
			for _, s := range smp[n] {
				if s.model == m {
					row.samples[n] = append(row.samples[n], s)
				}
			}
			ok = ok && len(row.samples[n]) > 0
		}
		if ok {
			rows = append(rows, row)
		}
	}
	if len(rows) > 0 {
		return rows
	}
	names := slices.Clone(order)
	slices.Sort(names)
	var parts []string
	for _, n := range names {
		on := map[string]bool{}
		for _, s := range smp[n] {
			on[s.model] = true
		}
		parts = append(parts, fmt.Sprintf("%s on %s", n, list(sortedSet(on))))
	}
	parts[0] = strings.Replace(parts[0], " on ", " ran on ", 1)
	return []scopeRow{{notComparable: true, samples: smp, text: fmt.Sprintf("not comparable: %s (fewer than %d repetitions each)", strings.Join(parts, ", "), timingMinReps)}}
}

func values(ss []sample) []int64 {
	out := make([]int64, len(ss))
	for i, s := range ss {
		out[i] = s.v
	}
	return out
}

// samplesOf is the samples of the builds that have a value.
func samplesOf(bs []*tbuild, value func(*tbuild) (int64, bool)) []sample {
	var out []sample
	for _, b := range bs {
		if v, ok := value(b); ok {
			out = append(out, sample{v, b.Job.CPUModel})
		}
	}
	return out
}

func commitOf(b *tbuild) (int64, bool) { return b.g.commit, true }
func afterOf(b *tbuild) (int64, bool)  { return b.g.after, b.g.hasAfter }

// decideJoin decides whether the joiner joins the reference set, from the join set's
// builds at the join window.
func (j *judge) decideJoin() {
	join := &j.rep.Join
	runs, revs := map[string]bool{}, map[string]bool{}
	for _, b := range j.joinSet {
		runs[b.Job.RunID], revs[b.Manifest.Build.Revision] = true, true
	}
	join.Runs = append(join.Runs, sortedSet(runs)...)
	if rs := sortedSet(revs); len(rs) > 0 {
		join.Revision = rs[0]
	}
	finish := func() {
		j.reference = []string{timingBaseline, timingChosenPolicy}
		if join.Joins {
			j.reference = append(j.reference, timingJoiner)
		}
		j.rep.Reference = append(j.rep.Reference, j.reference...)
		if !join.Decided {
			j.joinWhy = fmt.Sprintf("whether %s joins is not decided: %s", timingJoiner, join.Reason)
		}
	}
	if j.joinGiven {
		if b, s := j.buildsWhy(j.joinSet); b != "" || s != "" {
			join.Reason = "the join set: " + b + s
			finish()
			return
		}
	}
	var undecided []string
	var plan string
	anyBelow := false
	allComparable := true
	for _, arch := range timingArches {
		ja := TimingJoinArch{Arch: arch}
		var ms, ls []*tbuild
		for _, b := range j.joinSet {
			if b.Job.Window != timingJoinWindow || b.Job.Arch != arch {
				continue
			}
			switch b.Job.Candidate {
			case timingJoiner:
				ms = append(ms, b)
			case timingBaseline:
				ls = append(ls, b)
			}
		}
		if len(ms) > 0 && plan == "" {
			plan = ms[0].Manifest.PlanDigest
		}
		switch {
		case len(ms) == 0 || len(ls) == 0:
			var missing []string
			if len(ms) == 0 {
				missing = append(missing, timingJoiner)
			}
			if len(ls) == 0 {
				missing = append(missing, timingBaseline)
			}
			undecided = append(undecided, fmt.Sprintf("no builds of %s at %d days on %s", list(missing), timingJoinWindow, arch))
			allComparable = false
		default:
			order := []string{timingJoiner, timingBaseline}
			smp := map[string][]sample{timingJoiner: samplesOf(ms, commitOf), timingBaseline: samplesOf(ls, commitOf)}
			rows := scopes(order, smp)
			if rows[0].notComparable {
				undecided = append(undecided, fmt.Sprintf("%s on %s: %s", timingJoiner, arch, rows[0].text))
				allComparable = false
				break
			}
			ja.Comparable, ja.Below = true, true
			var scopeNames []string
			for i, row := range rows {
				jm, bm := lowerMedian(values(row.samples[timingJoiner])), lowerMedian(values(row.samples[timingBaseline]))
				scopeNames = append(scopeNames, row.scope)
				if i == 0 {
					ja.JoinerP99, ja.BaselineP99 = jm, bm
					ja.JoinerReps, ja.BaselineReps = len(row.samples[timingJoiner]), len(row.samples[timingBaseline])
				}
				ja.Below = ja.Below && 10*jm < 9*bm
			}
			ja.Scope = strings.Join(scopeNames, ", ")
			anyBelow = anyBelow || ja.Below
		}
		join.Arches = append(join.Arches, ja)
	}
	switch {
	case anyBelow:
		g := j.g0Of(plan, timingJoiner)
		switch g.state {
		case g0Pass:
			join.Decided, join.Joins = true, true
		case g0Fail:
			join.Decided = true
			join.Reason = fmt.Sprintf("%s qualifies by its commit but its G0 fails", timingJoiner)
		default:
			join.Reason = fmt.Sprintf("%s qualifies by its commit but its G0 is %s", timingJoiner, g.state)
		}
	case allComparable:
		join.Decided = true
	default:
		join.Reason = strings.Join(undecided, "; ")
	}
	finish()
}

// judgeWindows makes the verdicts of every window.
func (j *judge) judgeWindows() {
	for _, b := range j.builds {
		settle, hits, _ := settlingOf(b.Manifest.Timing)
		j.rep.Builds = append(j.rep.Builds, TimingBuild{
			Window: b.Job.Window, Candidate: b.Job.Candidate, Arch: b.Job.Arch, CPUModel: b.Job.CPUModel, OS: b.Job.OS,
			ImageVersion: b.Job.ImageVersion, RunID: b.Job.RunID, RunAttempt: b.Job.RunAttempt, Rep: b.Job.Rep,
			CommitP99: b.g.commit, AfterRetention: b.g.after, LongestRetention: b.g.longest, Stall: b.g.stall,
			SettleLongest: int64(settle), SettleDeadlineHits: hits,
			BytesPerRecord: b.g.perRecord, CheckpointShare: b.g.checkpointShare,
		})
	}
	windows := map[int]*window{}
	for _, b := range j.builds {
		w := windows[b.Job.Window]
		if w == nil {
			w = &window{days: b.Job.Window, plan: b.Manifest.PlanDigest, byCand: map[string]map[string][]*tbuild{}, g0: map[string]g0Result{}}
			windows[b.Job.Window] = w
		}
		if w.byCand[b.Job.Candidate] == nil {
			w.byCand[b.Job.Candidate] = map[string][]*tbuild{}
		}
		w.byCand[b.Job.Candidate][b.Job.Arch] = append(w.byCand[b.Job.Candidate][b.Job.Arch], b)
	}
	for _, days := range sortedIntKeys(windows) {
		w := windows[days]
		w.cands = sortedKeysOf(w.byCand)
		for _, c := range w.cands {
			w.g0[c] = j.g0Of(w.plan, c)
		}
		for _, m := range j.reference {
			if _, ok := w.g0[m]; !ok {
				w.g0[m] = j.g0Of(w.plan, m)
			}
		}
		j.setBestWhy(w)
		for _, c := range w.cands {
			j.judgeCandidate(w, c)
			j.diagnose(w, c)
		}
	}
	gateIndex := func(g string) int { return slices.Index(timingGates, g) }
	archIndex := func(a string) int { return slices.Index(append(slices.Clone(timingArches), ArchBoth), a) }
	whatIndex := func(w string) int { return slices.Index([]string{"first batch", "retention alone", "settling"}, w) }
	slices.SortStableFunc(j.rep.Diagnostics, func(a, b TimingDiagnostic) int {
		return cmp.Or(
			cmp.Compare(a.Window, b.Window), cmp.Compare(a.Candidate, b.Candidate), cmp.Compare(archIndex(a.Arch), archIndex(b.Arch)),
			cmp.Compare(whatIndex(a.What), whatIndex(b.What)), cmp.Compare(a.Scope, b.Scope),
		)
	})
	slices.SortStableFunc(j.rep.Verdicts, func(a, b TimingVerdict) int {
		return cmp.Or(
			cmp.Compare(a.Window, b.Window), cmp.Compare(a.Candidate, b.Candidate),
			cmp.Compare(gateIndex(a.Gate), gateIndex(b.Gate)), cmp.Compare(archIndex(a.Arch), archIndex(b.Arch)),
			cmp.Compare(a.Scope, b.Scope),
		)
	})
}

// setBestWhy says why the best of G3 and G4 is not established at the window.
func (j *judge) setBestWhy(w *window) {
	for _, m := range j.reference {
		if len(w.byCand[m]) == 0 {
			w.bestWhy = fmt.Sprintf("%s is in the reference set and was not built at %d days", m, w.days)
			return
		}
	}
	passing := 0
	for _, m := range j.reference {
		switch w.g0[m].state {
		case g0Unknown:
			w.bestWhy = fmt.Sprintf("the best is not established: G0 of %s is not established", m)
			return
		case g0Pass:
			passing++
		}
	}
	if passing == 0 {
		w.bestWhy = "the best is not established: no member of the reference set passes G0"
	}
}

// rowCtx is what a row says of itself to the preconditions that depend on the row.
type rowCtx struct {
	// minReps is the fewest repetitions, in the row's scope, among the candidates the row
	// compares.
	minReps       int
	notComparable bool
	ncText        string
	// noBest is why the best is not established for the row, "" if it is.
	noBest string
	// open is why it is not known whether G2 is judged against the best, for a value over
	// the budget, "" if it is known.
	open string
	// noise is why L/off's own values are too spread to compare with, "" if they are not.
	noise string
}

// reason is why a row is not judged, in the order of the preconditions, or "". The
// second result says that the reason is about the row's architecture.
func (j *judge) reason(w *window, gate, cand, arch string, c rowCtx) (string, bool) {
	switch {
	case len(j.rep.Problems) > 0:
		return "the timings cannot be judged together", false
	case j.binaryWhy != "":
		return j.binaryWhy, false
	case j.settingsWhy != "":
		return j.settingsWhy, false
	case j.revisionWhy != "":
		return j.revisionWhy, false
	}
	if _, ok := j.plans[w.plan]; !ok {
		return fmt.Sprintf("no plan of %d days (%s) among the inputs: give the bench-plans artifact with -in", w.days, short(w.plan)), false
	}
	if gate == GateG0 {
		if g := w.g0[cand]; g.state == g0Unknown {
			return g.why(cand), false
		}
		return "", false
	}
	if j.joinWhy != "" {
		return j.joinWhy, false
	}
	if gate == GateG3Commit || gate == GateG4 {
		if w.bestWhy != "" {
			return w.bestWhy, false
		}
		for _, m := range j.reference {
			if len(w.byCand[m][arch]) == 0 && arch != ArchBoth {
				return fmt.Sprintf("%s is in the reference set and was not built on %s at %d days", m, arch, w.days), true
			}
		}
	}
	if c.noBest != "" {
		return c.noBest, true
	}
	if gate == GateG2 && c.open != "" {
		return c.open, c.open != w.bestWhy
	}
	if gate != GateG2 && gate != GateCheckpoint {
		if g := w.g0[cand]; g.state != g0Pass {
			return g.why(cand), false
		}
	}
	if !c.notComparable && c.minReps < timingMinReps {
		if c.minReps == 1 {
			return "single repetition: indicative only", true
		}
		return fmt.Sprintf("%d repetitions: indicative only (at least %d are needed)", c.minReps, timingMinReps), true
	}
	if c.notComparable {
		return c.ncText, true
	}
	if c.noise != "" {
		return c.noise, true
	}
	return "", false
}

// judgeCandidate makes every verdict of one candidate at one window.
func (j *judge) judgeCandidate(w *window, cand string) {
	g0 := w.g0[cand]
	v := TimingVerdict{Window: w.days, Candidate: cand, Gate: GateG0, Pass: g0.state == g0Pass}
	if g0.state == g0Unknown {
		v.ValueText = "not established"
	} else {
		runs := fmt.Sprintf("%d counters runs", g0.runs)
		if g0.runs == 1 {
			runs = "1 counters run"
		}
		v.ValueText = fmt.Sprintf("%d wrong, %d wrong before compaction, %d unstable (%s)", g0.wrong, g0.uncompacted, g0.unstable, runs)
	}
	v.LimitText = "none"
	v.Reason, _ = j.reason(w, GateG0, cand, "", rowCtx{})
	v.Judged = v.Reason == ""
	j.rep.Verdicts = append(j.rep.Verdicts, v)

	for _, gate := range []string{GateG2, GateG3Commit, GateG4, GateCheckpoint} {
		var rows []TimingVerdict
		for _, arch := range timingArches {
			if len(w.byCand[cand][arch]) == 0 {
				continue
			}
			switch gate {
			case GateG2:
				rows = append(rows, j.rowsG2(w, cand, arch)...)
			case GateG3Commit:
				rows = append(rows, j.rowsG3(w, cand, arch)...)
			case GateG4:
				rows = append(rows, j.rowG4(w, cand, arch)...)
			case GateCheckpoint:
				rows = append(rows, j.rowCheckpoint(w, cand, arch)...)
			}
		}
		if len(rows) == 0 {
			continue
		}
		j.rep.Verdicts = append(j.rep.Verdicts, rows...)
		j.rep.Verdicts = append(j.rep.Verdicts, j.combine(w, cand, gate, rows))
	}
}

// finishRow sets whether a per-architecture row is judged and why not.
func (j *judge) finishRow(w *window, v *TimingVerdict, c rowCtx) {
	c.notComparable, c.ncText = v.NotComparable, v.LimitText
	why, _ := j.reason(w, v.Gate, v.Candidate, v.Arch, c)
	v.Reason, v.Judged = why, why == ""
}

// g2Of is G2's value of a build: the largest retention plus the excess of the batches
// after it; for a build that did not record them, shown and not judged, the longest
// retention alone.
func g2Of(b *tbuild) (int64, bool) {
	if b.g.hasStall {
		return b.g.stall, true
	}
	return b.g.longest, b.g.hasLongest
}

// g2Mode is whether G2 on an architecture at a window is judged against the best of the
// reference set instead of the budget: it is where no member of the reference set that
// passes G0 is within the budget. open says why that is not known.
type g2Mode struct {
	fallback bool
	open     string
}

// claimable is whether the inputs are judged up to the plan of the window: there are
// no problems, and the binaries, the settings and the revision are the pre-registered
// ones, the join of the joiner is decided (the reference set is known) and the plan is
// among the inputs.
func (j *judge) claimable(w *window) bool {
	_, planFound := j.plans[w.plan]
	return len(j.rep.Problems) == 0 && j.binaryWhy == "" && j.settingsWhy == "" && j.revisionWhy == "" && j.joinWhy == "" && planFound
}

// g2ModeOf decides g2Mode, and records the finding where the fallback applies.
func (j *judge) g2ModeOf(w *window, arch string) g2Mode {
	key := fmt.Sprintf("%d %s", w.days, arch)
	if m, ok := j.g2modes[key]; ok {
		return m
	}
	var m g2Mode
	defer func() { j.g2modes[key] = m }()
	// A claim about the engine is made only from inputs that are judged: with a problem,
	// builds a result may not come from, settings that are not the pre-registered ones, a
	// revision not checked or a plan not found, G2 stays against the budget and no finding
	// is recorded.
	if !j.claimable(w) {
		return m
	}
	if w.bestWhy != "" {
		m.open = w.bestWhy
		return m
	}
	for _, mem := range j.reference {
		if len(w.byCand[mem][arch]) == 0 {
			m.open = fmt.Sprintf("%s is in the reference set and was not built on %s at %d days", mem, arch, w.days)
			return m
		}
	}
	smp := map[string][]sample{}
	var order []string
	for _, mem := range j.membersWithBuilds(w, arch) {
		ss := samplesOf(w.byCand[mem][arch], g2Of)
		smp[mem] = ss
		order = append(order, mem)
		if len(ss) > 0 {
			if _, _, pass := g2Verdict(time.Duration(slices.Max(values(ss))), j.r); pass {
				return m
			}
		}
	}
	if len(order) == 0 {
		return m
	}
	m.fallback = true
	var bests []string
	for _, row := range scopes(order, smp) {
		if row.notComparable {
			bests = nil
			break
		}
		best := int64(0)
		for _, n := range order {
			if v := slices.Max(values(row.samples[n])); best == 0 || v < best {
				best = v
			}
		}
		bests = append(bests, fmt.Sprintf("%s in %s", time.Duration(best).Round(time.Millisecond), row.scope))
	}
	text := ""
	switch len(bests) {
	case 0:
	case 1:
		text = " (best " + strings.SplitN(bests[0], " in ", 2)[0] + ")"
	default:
		text = " (best " + strings.Join(bests, ", ") + ")"
	}
	budget := time.Duration(j.r.StallBudgetSeconds * float64(time.Second))
	// A finding is a claim about the engine: it is made only from members with repetitions
	// enough to be judged, and not from a single run.
	for _, n := range order {
		if len(smp[n]) < timingMinReps {
			return m
		}
	}
	j.findings[fmt.Sprintf("at %d days on %s synchronous retention fits %s for no layout of the reference set%s: the store must retain asynchronously before it ingests real data; G2 there is judged against the best", w.days, arch, budget, text)] = true
	return m
}

// noiseWhy says that L/off's own values over its repetitions in the scope vary by more
// than [timingAAFloor], so that a comparison with it cannot be resolved, or "". The
// scope is a CPU model or the models pooled.
func (j *judge) noiseWhy(w *window, arch, scope, what string, value func(*tbuild) (int64, bool)) string {
	var vals []int64
	for _, b := range w.byCand[timingBaseline][arch] {
		if v, ok := value(b); ok && (strings.HasPrefix(scope, "pooled over ") || b.Job.CPUModel == scope) {
			vals = append(vals, v)
		}
	}
	if len(vals) < 2 {
		return ""
	}
	if float64(slices.Max(vals)) > timingAAFloor*float64(slices.Min(vals)) {
		return fmt.Sprintf("%s's own %s varies by more than x%.1f over its repetitions in %s: the noise is larger than the gate can resolve", timingBaseline, what, timingAAFloor, scope)
	}
	return ""
}

// rowsG2 makes the rows of G2 of a candidate on an architecture: its largest value
// against the budget, and where no member of the reference set fits the budget, against
// the best of them within each scope.
func (j *judge) rowsG2(w *window, cand, arch string) []TimingVerdict {
	mine := samplesOf(w.byCand[cand][arch], g2Of)
	if len(mine) == 0 {
		return nil
	}
	mode := j.g2ModeOf(w, arch)
	largest := slices.Max(values(mine))
	value, limit, within := g2Verdict(time.Duration(largest), j.r)
	budget := j.r.StallBudgetSeconds * float64(time.Second)
	absolute := TimingVerdict{
		Window: w.days, Candidate: cand, Gate: GateG2, Arch: arch, Reps: len(mine),
		Value: float64(largest), Limit: budget, ValueText: value, LimitText: limit, Pass: within,
	}
	if within || !mode.fallback {
		c := rowCtx{minReps: len(mine)}
		if !within {
			c.open = mode.open
		}
		j.finishRow(w, &absolute, c)
		return []TimingVerdict{absolute}
	}
	smp := map[string][]sample{cand: mine}
	order := []string{cand}
	members := j.membersWithBuilds(w, arch)
	for _, m := range members {
		if m != cand {
			smp[m] = samplesOf(w.byCand[m][arch], g2Of)
			order = append(order, m)
		}
	}
	budgetText := time.Duration(budget).String()
	var out []TimingVerdict
	for _, row := range scopes(order, smp) {
		v := TimingVerdict{Window: w.days, Candidate: cand, Gate: GateG2, Arch: arch, Scope: row.scope}
		if row.notComparable {
			v.Reps, v.Value, v.ValueText, v.LimitText, v.NotComparable = len(mine), float64(largest), value, row.text, true
			j.finishRow(w, &v, rowCtx{})
			out = append(out, v)
			continue
		}
		cv := slices.Max(values(row.samples[cand]))
		minReps, best := len(row.samples[cand]), int64(0)
		for _, m := range members {
			ss := row.samples[m]
			minReps = min(minReps, len(ss))
			if len(ss) == 0 {
				continue
			}
			if mv := slices.Max(values(ss)); mv > 0 && (best == 0 || mv < best) {
				best = mv
			}
		}
		v.Reps, v.Value = len(row.samples[cand]), float64(cv)
		var rowWithin bool
		v.ValueText, _, rowWithin = g2Verdict(time.Duration(cv), j.r)
		c := rowCtx{minReps: minReps}
		if best > 0 {
			v.Best, v.Limit = float64(best), float64(best)*j.r.CommitFactor
			v.LimitText = fmt.Sprintf("%s (x%.1f the best, %s; no layout of the reference set fits %s)", time.Duration(v.Limit), j.r.CommitFactor, time.Duration(best), budgetText)
			v.Pass = rowWithin || float64(cv) <= v.Limit
			c.noise = j.noiseWhy(w, arch, row.scope, "retention plus the slowness after it", g2Of)
		} else {
			v.LimitText = "no best"
			v.Pass = rowWithin
			c.noBest = fmt.Sprintf("the best is not established on %s: no member of the reference set has a value", arch)
		}
		j.finishRow(w, &v, c)
		out = append(out, v)
	}
	return out
}

func (j *judge) rowCheckpoint(w *window, cand, arch string) []TimingVerdict {
	var shares []float64
	any := false
	for _, b := range w.byCand[cand][arch] {
		shares = append(shares, b.g.checkpointShare)
		any = any || b.g.hasCheckpoint
	}
	if !any {
		return nil
	}
	largest := slices.Max(shares)
	value, limit, pass := checkpointVerdict(largest, j.r)
	v := TimingVerdict{
		Window: w.days, Candidate: cand, Gate: GateCheckpoint, Arch: arch, Reps: len(shares),
		Value: largest, Limit: j.r.ReopenCheckpointWrites, ValueText: value, LimitText: limit, Pass: pass,
	}
	j.finishRow(w, &v, rowCtx{minReps: len(shares)})
	return []TimingVerdict{v}
}

// passingMembers are the members of the reference set that pass G0 at the window.
func (j *judge) passingMembers(w *window) []string {
	var out []string
	for _, m := range j.reference {
		if w.g0[m].state == g0Pass {
			out = append(out, m)
		}
	}
	return out
}

func (j *judge) rowG4(w *window, cand, arch string) []TimingVerdict {
	per := func(bs []*tbuild) []float64 {
		out := make([]float64, len(bs))
		for i, b := range bs {
			out[i] = b.g.perRecord
		}
		return out
	}
	mine := per(w.byCand[cand][arch])
	value := lowerMedian(mine)
	c := rowCtx{minReps: len(mine)}
	best := 0.0
	for _, m := range j.passingMembers(w) {
		bs := w.byCand[m][arch]
		if len(bs) == 0 {
			continue
		}
		c.minReps = min(c.minReps, len(bs))
		if mv := lowerMedian(per(bs)); mv > 0 && (best == 0 || mv < best) {
			best = mv
		}
	}
	v := TimingVerdict{Window: w.days, Candidate: cand, Gate: GateG4, Arch: arch, Reps: len(mine), Value: value}
	if best > 0 {
		var limit string
		v.ValueText, limit, v.Pass = g4Verdict(value, best, j.r)
		v.LimitText, v.Best, v.Limit = limit, best, best*j.r.BytesPerRecordFactor
	} else {
		v.ValueText, v.LimitText = fmt.Sprintf("%.1f", value), "no best"
		c.noBest = fmt.Sprintf("the best is not established on %s: no member of the reference set has a value", arch)
	}
	j.finishRow(w, &v, c)
	return []TimingVerdict{v}
}

// membersWithBuilds are the members of the reference set that pass G0, in the order
// of the set, that have builds on the architecture, whether or not a build has the
// value a gate reads.
func (j *judge) membersWithBuilds(w *window, arch string) []string {
	var out []string
	for _, m := range j.passingMembers(w) {
		if len(w.byCand[m][arch]) > 0 {
			out = append(out, m)
		}
	}
	return out
}

// rowsG3 makes the rows of G3 of a candidate on an architecture, the 99th percentile of
// the commit: one for each scope its timings are compared within.
func (j *judge) rowsG3(w *window, cand, arch string) []TimingVerdict {
	members := j.membersWithBuilds(w, arch)
	smp := map[string][]sample{cand: samplesOf(w.byCand[cand][arch], commitOf)}
	if len(smp[cand]) == 0 {
		return nil
	}
	order := []string{cand}
	for _, m := range members {
		if m != cand {
			smp[m] = samplesOf(w.byCand[m][arch], commitOf)
			order = append(order, m)
		}
	}
	var out []TimingVerdict
	for _, row := range scopes(order, smp) {
		v := TimingVerdict{Window: w.days, Candidate: cand, Gate: GateG3Commit, Arch: arch, Scope: row.scope}
		if row.notComparable {
			all := lowerMedian(values(smp[cand]))
			v.Reps, v.Value, v.ValueText, v.LimitText, v.NotComparable = len(smp[cand]), float64(all), time.Duration(all).String(), row.text, true
			j.finishRow(w, &v, rowCtx{})
			out = append(out, v)
			continue
		}
		mine := values(row.samples[cand])
		med := lowerMedian(mine)
		c := rowCtx{minReps: len(mine)}
		best := int64(0)
		for _, m := range members {
			ss := row.samples[m]
			c.minReps = min(c.minReps, len(ss))
			if mv := lowerMedian(values(ss)); mv > 0 && (best == 0 || mv < best) {
				best = mv
			}
		}
		v.Reps, v.Value = len(mine), float64(med)
		if best > 0 {
			v.ValueText, v.LimitText, v.Pass = g3Verdict(float64(med), float64(best), j.r)
			v.Best, v.Limit = float64(best), float64(best)*j.r.CommitFactor
			c.noise = j.noiseWhy(w, arch, row.scope, "99th-percentile commit", commitOf)
		} else {
			v.ValueText, v.LimitText = time.Duration(med).String(), "no best"
			c.noBest = fmt.Sprintf("the best is not established on %s: no member of the reference set has a value", arch)
		}
		j.finishRow(w, &v, c)
		out = append(out, v)
	}
	return out
}

// combine is the verdict of a gate that needs both architectures.
func (j *judge) combine(w *window, cand, gate string, rows []TimingVerdict) TimingVerdict {
	v := TimingVerdict{Window: w.days, Candidate: cand, Gate: gate, Arch: ArchBoth, LimitText: "both architectures", Pass: true, Judged: true}
	var parts []string
	missing := ""
	for _, arch := range timingArches {
		status, found := "ok", false
		for _, r := range rows {
			if r.Arch != arch {
				continue
			}
			found = true
			switch {
			case r.NotComparable:
				status = "not comparable"
				v.NotComparable = true
			case !r.Pass && status == "ok":
				status = "OVER"
			}
			v.Pass = v.Pass && r.Pass
			if !r.Judged && v.Reason == "" {
				v.Reason, v.Judged = r.Reason, false
			}
		}
		if !found {
			status, v.Pass = "no builds", false
			if missing == "" {
				missing = fmt.Sprintf("no builds of %s on %s at %d days", cand, arch, w.days)
			}
		}
		parts = append(parts, arch+" "+status)
	}
	if missing != "" && v.Reason == "" {
		v.Reason, v.Judged = missing, false
	}
	v.ValueText = strings.Join(parts, ", ")
	return v
}

// finish sets the overall verdict and the reasons it is not one.
func (j *judge) finish() {
	reasons := map[string]bool{}
	judged := len(j.rep.Problems) == 0 && len(j.rep.Verdicts) > 0
	for _, v := range j.rep.Verdicts {
		if v.Judged {
			continue
		}
		judged = false
		if v.Arch == ArchBoth && j.componentSays(v) {
			continue
		}
		prefix := fmt.Sprintf("%d days", v.Window)
		if _, arch := j.archSpecific(v); arch {
			prefix += ", " + v.Arch
		}
		reasons[prefix+": "+v.Reason] = true
	}
	if len(j.rep.Verdicts) == 0 {
		reasons["no builds"] = true
	}
	j.rep.Judged = judged
	j.rep.Findings = append(j.rep.Findings, sortedSet(j.findings)...)
	j.rep.NotJudged = append(j.rep.NotJudged, sortedSet(reasons)...)
}

// componentSays is whether a row of one architecture, of the gate a combined verdict
// is the verdict of, already gives its reason, so that it is listed once.
func (j *judge) componentSays(both TimingVerdict) bool {
	for _, v := range j.rep.Verdicts {
		if v.Window == both.Window && v.Candidate == both.Candidate && v.Gate == both.Gate && v.Arch != ArchBoth && v.Reason == both.Reason {
			return true
		}
	}
	return false
}

// archSpecific says whether the reason a verdict is not judged is about its
// architecture: too few repetitions, timings that cannot be compared, a member of the
// reference set that has no build on it, or no builds on an architecture.
func (j *judge) archSpecific(v TimingVerdict) (string, bool) {
	if v.Arch == "" || v.Arch == ArchBoth {
		return v.Reason, false
	}
	switch {
	case strings.Contains(v.Reason, "indicative only"), strings.HasPrefix(v.Reason, "not comparable"),
		strings.Contains(v.Reason, "varies by more than"), strings.Contains(v.Reason, "the best is not established on "+v.Arch),
		strings.Contains(v.Reason, "was not built on "+v.Arch):
		return v.Reason, true
	}
	return v.Reason, false
}

// diagnose adds the numbers of a candidate at a window that are shown and do not decide:
// the first batch after a retention with the verdict the former rule gave it (the
// median over the repetitions against the best median of the reference set, within each
// scope), the retention alone, and the settling of a retention.
func (j *judge) diagnose(w *window, cand string) {
	g0 := w.g0[cand]
	for _, arch := range timingArches {
		bs := w.byCand[cand][arch]
		if len(bs) == 0 {
			continue
		}
		if smp := samplesOf(bs, afterOf); len(smp) > 0 {
			j.rep.Diagnostics = append(j.rep.Diagnostics, j.formerFirstBatch(w, cand, arch, g0, smp)...)
		}
		if vals := values(samplesOf(bs, func(b *tbuild) (int64, bool) { return b.g.longest, b.g.hasLongest })); len(vals) > 0 {
			j.rep.Diagnostics = append(j.rep.Diagnostics, TimingDiagnostic{
				Window: w.days, Candidate: cand, Arch: arch, What: "retention alone", Reps: len(vals), Value: slices.Max(vals),
			})
		}
		var settle int64
		var hits, n int
		for _, b := range bs {
			if l, h, ok := settlingOf(b.Manifest.Timing); ok {
				settle, hits, n = max(settle, int64(l)), hits+h, n+1
			}
		}
		if n > 0 {
			j.rep.Diagnostics = append(j.rep.Diagnostics, TimingDiagnostic{
				Window: w.days, Candidate: cand, Arch: arch, What: "settling", Reps: n, Value: settle,
				Text: fmt.Sprintf("deadline reached %d times", hits),
			})
		}
	}
}

// formerFirstBatch is the first batch after a retention of a candidate on an
// architecture as the former G3 judged it: the median over the repetitions of the
// longest first batch after a retention, within [Rules.CommitFactor] of the best median
// of the reference members that pass G0, compared within the scopes of G3. It is a
// diagnostic and decides nothing.
func (j *judge) formerFirstBatch(w *window, cand, arch string, g0 g0Result, mine []sample) []TimingDiagnostic {
	base := TimingDiagnostic{Window: w.days, Candidate: cand, Arch: arch, What: "first batch"}
	if g0.state != g0Pass {
		base.Reps, base.Value = len(mine), lowerMedian(values(mine))
		base.Text = "former rule not applied: " + g0.why(cand)
		return []TimingDiagnostic{base}
	}
	smp := map[string][]sample{cand: mine}
	order := []string{cand}
	members := j.passingMembers(w)
	for _, m := range members {
		if m == cand {
			continue
		}
		if ss := samplesOf(w.byCand[m][arch], afterOf); len(ss) > 0 {
			smp[m] = ss
			order = append(order, m)
		}
	}
	var out []TimingDiagnostic
	for _, row := range scopes(order, smp) {
		d := base
		d.Scope = row.scope
		if row.notComparable {
			d.Scope, d.Reps, d.Value = "", len(mine), lowerMedian(values(mine))
			d.Text = "former rule: " + row.text
			out = append(out, d)
			continue
		}
		ours := values(row.samples[cand])
		d.Reps, d.Value = len(ours), lowerMedian(ours)
		for _, m := range members {
			if mv := lowerMedian(values(row.samples[m])); mv > 0 && (d.Best == 0 || mv < d.Best) {
				d.Best = mv
			}
		}
		if d.Best == 0 {
			d.Text = "former rule: no best"
		} else {
			_, _, d.FormerPass = g3Verdict(float64(d.Value), float64(d.Best), j.r)
			verdict := "ok"
			if !d.FormerPass {
				verdict = "OVER"
			}
			d.Text = fmt.Sprintf("former rule x%.1f the best %s: %s", j.r.CommitFactor, time.Duration(d.Best), verdict)
		}
		out = append(out, d)
	}
	return out
}
