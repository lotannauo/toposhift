// Command spikebench builds the candidate layouts on one planned stream and
// compares what their reads cost, in counters that do not depend on timing.
//
//	spikebench plan   -preset ci -out DIR            analyze the stream, choose and answer the queries
//	spikebench build  -out DIR -candidate L/off      write the planned stream to one candidate
//	spikebench read   -out DIR -candidate L/off      ask the queries of a built candidate
//	spikebench report -out DIR                       set the candidates side by side
//	spikebench run    -preset ci -out DIR            all of the above, a process per step
//	spikebench pins   -preset ci -window 2 -out DIR  choose the prefixes the windows are read at
//	spikebench windows -preset ci -out DIR           run over windows of 2, 7, 14 and 30 days of retained history
//	spikebench g1     -out DIR                       judge G1 from the windows
//
// Build with CGO_ENABLED=0 and without -race or the invariants tag, from a clean
// tree; the program refuses to produce a result otherwise (-untimed allows it
// for a validation run whose results are not to be compared). The output
// directory must be outside the repository.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/candidates"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/spike/workload"
)

const (
	planFile = "plan.json"
	// childEnv marks a step run by `run`.
	childEnv = "SPIKEBENCH_STEP"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// After the first signal the steps are asked to stop; a second one is the
	// default's, which ends the process even if a step is deep in a compaction.
	// A step run by `run` keeps catching signals: the terminal's interrupt reaches
	// it as well as the parent, and one that stops catching it after the first
	// would be killed by the parent's request to stop.
	if os.Getenv(childEnv) == "" {
		go func() {
			<-ctx.Done()
			stop()
		}()
	}
	cmd, args := os.Args[1], os.Args[2:]
	var err error
	switch cmd {
	case "plan":
		err = doPlan(ctx, args)
	case "build":
		err = doBuild(ctx, args)
	case "read":
		err = doRead(ctx, args)
	case "report":
		err = doReport(args)
	case "run":
		err = doRun(ctx, args)
	case "pins":
		err = doPins(ctx, args)
	case "windows":
		err = doWindows(ctx, args)
	case "g1":
		err = doG1(args)
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "spikebench:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: spikebench plan|build|read|report|run|pins|windows|g1 [flags]   (spikebench <command> -h for the flags)")
	os.Exit(2)
}

func progress(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "%s  %s\n", time.Now().Format("15:04:05"), fmt.Sprintf(format, args...))
}

// common are the flags every command takes.
type common struct {
	out     string
	untimed bool
}

func (c *common) flags(fs *flag.FlagSet) {
	fs.StringVar(&c.out, "out", "", "output directory, outside the repository (required)")
	fs.BoolVar(&c.untimed, "untimed", false, "allow an instrumented, dirty or cgo binary: a validation run, not a result")
}

func (c *common) guards() runner.Guards {
	return runner.Guards{Info: runner.ReadBuildInfo(), Untimed: c.untimed}
}

func (c *common) require() error {
	if c.out == "" {
		return fmt.Errorf("-out is required")
	}
	return runner.CheckOutside(c.out)
}

// planFlags are the flags that choose the spec.
type planFlags struct {
	preset        string
	days          int
	seed          uint64
	batch         int
	retain        string
	hot, median   int
	cacheFraction float64
	cacheMB       int
	window        int
	pins          string
	full          bool
	rate          float64
	extend        string
	podHeartbeat  string
	runMaxAge     string
	payloadPad    int
	minNonEmpty   float64
}

func (p *planFlags) flags(fs *flag.FlagSet) {
	fs.StringVar(&p.preset, "preset", "ci", "tiny, small, ci (3 days), week (10 days) or month (31 days)")
	fs.IntVar(&p.days, "days", 0, "override the number of days of the cluster presets")
	fs.Uint64Var(&p.seed, "seed", 0, "override the seed")
	fs.IntVar(&p.batch, "batch", 0, "records per write (default: the spec's)")
	fs.StringVar(&p.retain, "retain", "", "retentions as at/keep durations, for example 48h/24h,72h/24h (default: the spec's; \"none\" for none)")
	fs.IntVar(&p.hot, "hot", 0, "busiest prefixes queried per class (default: the spec's)")
	fs.IntVar(&p.median, "median", 0, "prefixes around the middle queried per class (default: the spec's)")
	fs.Float64Var(&p.cacheFraction, "cache-fraction", 0, "block cache as a share of the stream's payload bytes, instead of an absolute size (default: the spec's absolute size)")
	fs.Float64Var(&p.rate, "events-per-second", 0, "override the rate of churn events of the preset")
	fs.StringVar(&p.extend, "extend", "", "how often a refreshed run is re-asserted: \"every\" refresh (the control) or a share of its TTL such as 0.5 (default: the preset's)")
	fs.StringVar(&p.podHeartbeat, "pod-heartbeat", "", "how often the cluster collector refreshes every pod's placement, such as 5m, or \"off\" (default: the preset's)")
	fs.StringVar(&p.runMaxAge, "run-max-age", "", "the greatest age of a run of refreshes before the ingest coalescer continues it with a new one, such as 2h, or \"off\" (default: the preset's, none)")
	fs.IntVar(&p.payloadPad, "payload-pad", 0, "random bytes added to every payload, drawn apart from the rest of the stream so that only the payloads change (default: none)")
	fs.IntVar(&p.window, "window", 0, "run over a window of this many days of retained history: a stream of 2R + 1.5 days with a daily retention that keeps R (see runner.WindowSpec)")
	fs.StringVar(&p.pins, "pins", "", "the pinned prefixes of a family of windows, as made by `pins` (needs -window; only the records that touch them are written, unless -full)")
	fs.BoolVar(&p.full, "full", false, "with -pins, write the whole stream and ask the pins' queries of it: a full store read at the pinned prefixes")
	fs.IntVar(&p.cacheMB, "cache-mb", 0, "block cache in MiB, the same for every run (default: the spec's)")
	fs.Float64Var(&p.minNonEmpty, "min-non-empty", -1, "least share of each group of queries with a non-empty answer (default: the spec's)")
}

// args are the flags again, to pass to a process of this program.
func (p planFlags) args() []string {
	var out []string
	add := func(name, v string) { out = append(out, "-"+name, v) }
	add("preset", p.preset)
	if p.days != 0 {
		add("days", fmt.Sprint(p.days))
	}
	if p.seed != 0 {
		add("seed", fmt.Sprint(p.seed))
	}
	if p.batch != 0 {
		add("batch", fmt.Sprint(p.batch))
	}
	if p.retain != "" {
		add("retain", p.retain)
	}
	if p.hot != 0 {
		add("hot", fmt.Sprint(p.hot))
	}
	if p.median != 0 {
		add("median", fmt.Sprint(p.median))
	}
	if p.cacheFraction != 0 {
		add("cache-fraction", fmt.Sprint(p.cacheFraction))
	}
	if p.cacheMB != 0 {
		add("cache-mb", fmt.Sprint(p.cacheMB))
	}
	if p.rate != 0 {
		add("events-per-second", fmt.Sprint(p.rate))
	}
	if p.extend != "" {
		add("extend", p.extend)
	}
	if p.podHeartbeat != "" {
		add("pod-heartbeat", p.podHeartbeat)
	}
	if p.runMaxAge != "" {
		add("run-max-age", p.runMaxAge)
	}
	if p.payloadPad != 0 {
		add("payload-pad", fmt.Sprint(p.payloadPad))
	}
	if p.window != 0 {
		add("window", fmt.Sprint(p.window))
	}
	if p.pins != "" {
		add("pins", p.pins)
	}
	if p.full {
		out = append(out, "-full")
	}
	if p.minNonEmpty >= 0 {
		add("min-non-empty", fmt.Sprint(p.minNonEmpty))
	}
	return out
}

func (p planFlags) spec() (runner.Spec, error) {
	var w workload.Config
	switch p.preset {
	case "tiny":
		w = workload.Tiny()
	case "small":
		w = workload.Small()
	case "ci":
		w = workload.CI()
	case "week":
		w = workload.Week()
	case "month":
		w = workload.Month()
	default:
		return runner.Spec{}, fmt.Errorf("unknown preset %q", p.preset)
	}
	if p.days > 0 {
		if p.preset == "tiny" || p.preset == "small" {
			return runner.Spec{}, fmt.Errorf("-days applies to the cluster presets")
		}
		w.Duration = time.Duration(p.days) * 24 * time.Hour
	}
	if p.seed > 0 {
		w.Seed = p.seed
	}
	if p.rate < 0 {
		return runner.Spec{}, fmt.Errorf("-events-per-second cannot be negative")
	}
	if p.rate > 0 {
		w.EventsPerSecond = p.rate
	}
	switch p.extend {
	case "":
	case "every":
		w.CoalesceRuns, w.ExtendTTLFraction, w.ExtendEvery = true, 0, 0
	default:
		f, err := strconv.ParseFloat(p.extend, 64)
		if err != nil || f <= 0 || f > 1 {
			return runner.Spec{}, fmt.Errorf("-extend: %q is not \"every\" or a share of the TTL in (0, 1]", p.extend)
		}
		w.CoalesceRuns, w.ExtendTTLFraction, w.ExtendEvery = true, f, 0
	}
	switch p.podHeartbeat {
	case "":
	case "off":
		w.PodHeartbeatInterval = 0
	default:
		d, err := time.ParseDuration(p.podHeartbeat)
		if err != nil || d <= 0 {
			return runner.Spec{}, fmt.Errorf("-pod-heartbeat: %q is not \"off\" or a duration such as 5m", p.podHeartbeat)
		}
		w.PodHeartbeatInterval = d
	}
	switch p.runMaxAge {
	case "":
	case "off":
		w.RunMaxAge = 0
	default:
		d, err := time.ParseDuration(p.runMaxAge)
		if err != nil || d < time.Minute {
			return runner.Spec{}, fmt.Errorf("-run-max-age: %q is not \"off\" or a duration of at least a minute such as 2h", p.runMaxAge)
		}
		w.CoalesceRuns, w.RunMaxAge = true, d
	}
	if p.payloadPad < 0 {
		return runner.Spec{}, fmt.Errorf("-payload-pad cannot be negative")
	}
	w.PayloadPad = p.payloadPad
	s := runner.DefaultSpec(w)
	if p.batch > 0 {
		s.BatchSize = p.batch
	}
	switch p.retain {
	case "":
	case "none":
		s.Retentions = nil
	default:
		s.Retentions = nil
		for _, part := range strings.Split(p.retain, ",") {
			at, keep, ok := strings.Cut(part, "/")
			if !ok {
				return runner.Spec{}, fmt.Errorf("-retain: %q is not at/keep", part)
			}
			a, err1 := time.ParseDuration(at)
			k, err2 := time.ParseDuration(keep)
			if err1 != nil || err2 != nil {
				return runner.Spec{}, fmt.Errorf("-retain: %q: durations such as 48h", part)
			}
			s.Retentions = append(s.Retentions, runner.Retention{At: a, Keep: k})
		}
	}
	if p.hot > 0 {
		s.Hot = p.hot
	}
	if p.median > 0 {
		s.Median = p.median
	}
	if p.cacheFraction > 0 && p.cacheMB > 0 {
		return runner.Spec{}, fmt.Errorf("-cache-fraction and -cache-mb are two ways to size the cache: use one")
	}
	if p.cacheMB > 0 {
		s.CacheBytes, s.CacheFraction = int64(p.cacheMB)<<20, 0
	}
	if p.cacheFraction > 0 {
		s.CacheFraction, s.CacheBytes = p.cacheFraction, 0
	}
	if p.minNonEmpty >= 0 {
		s.MinNonEmpty = p.minNonEmpty
	}
	if p.window > 0 {
		if p.days > 0 || p.retain != "" {
			return runner.Spec{}, fmt.Errorf("-window sets the length of the stream and its retentions: not with -days or -retain")
		}
		s = runner.WindowSpec(s, p.window)
	}
	if p.pins != "" {
		if p.window <= 0 {
			return runner.Spec{}, fmt.Errorf("-pins is for a window: give -window as well")
		}
		b, err := os.ReadFile(p.pins)
		if err != nil {
			return runner.Spec{}, err
		}
		var pins runner.Pins
		if err := json.Unmarshal(b, &pins); err != nil {
			return runner.Spec{}, fmt.Errorf("%s: %w", p.pins, err)
		}
		s.Pins = &pins
	}
	if p.full {
		if p.pins == "" {
			return runner.Spec{}, fmt.Errorf("-full is for -pins: without pins every store is full")
		}
		s.FullStore = true
	}
	if (p.minNonEmpty < 0 && p.minNonEmpty != -1) || p.cacheFraction < 0 || p.cacheMB < 0 || p.window < 0 || p.days < 0 || p.batch < 0 || p.hot < 0 || p.median < 0 {
		return runner.Spec{}, fmt.Errorf("-days, -batch, -hot, -median, -min-non-empty, -cache-fraction and -cache-mb cannot be negative (leave a flag out, or 0, for the spec's own value)")
	}
	return s, s.Validate()
}

func doPlan(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("plan", flag.ExitOnError)
	var c common
	var p planFlags
	c.flags(fs)
	p.flags(fs)
	_ = fs.Parse(args)
	if err := c.require(); err != nil {
		return err
	}
	spec, err := p.spec()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(c.out, 0o755); err != nil {
		return err
	}
	path := filepath.Join(c.out, planFile)
	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("%s exists: a plan is made once; use another -out", path)
	}
	plan, err := runner.MakePlan(ctx, spec, progress)
	if err != nil {
		return err
	}
	if err := plan.Save(path); err != nil {
		return err
	}
	progress("plan written to %s (%d queries)", path, len(plan.Queries))
	return nil
}

func loadPlan(c common) (*runner.Plan, error) {
	if err := c.require(); err != nil {
		return nil, err
	}
	return runner.LoadPlan(filepath.Join(c.out, planFile))
}

func doBuild(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("build", flag.ExitOnError)
	var c common
	var b buildFlags
	var name string
	c.flags(fs)
	b.flags(fs)
	fs.StringVar(&name, "candidate", "", "the candidate to build (required)")
	_ = fs.Parse(args)
	if err := b.resolve(fs, c.untimed); err != nil {
		return err
	}
	plan, err := loadPlan(c)
	if err != nil {
		return err
	}
	v, err := candidates.Lookup(name)
	if err != nil {
		return err
	}
	_, err = runner.BuildWith(ctx, plan, v, runner.CandidateDir(c.out, v.Name), c.guards(), b.options(), progress)
	return err
}

// buildFlags are the choices of a build that do not change what it writes. build
// takes them, and run and windows take them too and pass them to every build they
// start, so that the candidates of a family are all built alike (a report refuses to
// compare builds that are not).
type buildFlags struct {
	rest, canonical, sync bool
	metricsEvery          time.Duration
}

func (b *buildFlags) flags(fs *flag.FlagSet) {
	fs.BoolVar(&b.rest, "rest-after-retention", false, "wait for the database to be at rest after each retention, so that the deletions of a retention do not pile up in memory; the first batch after a retention then does not carry the compactions' catch-up, and the report does not compare that timing (default: on for an -untimed build, off otherwise)")
	fs.BoolVar(&b.canonical, "canonical-layout", false, "after compacting everything, rewrite the data once in key order into bottom-level tables of the target file size, so the tables and the blocks every read loads are a function of the data and not of how fast the build ran; recorded in the manifest, and a report refuses to compare builds with and without it")
	fs.BoolVar(&b.sync, "sync", false, "commit every batch with a sync of the log to the disk, as production does; recorded in the manifest, and a report refuses to compare builds with and without it (default: off)")
	fs.DurationVar(&b.metricsEvery, "metrics-every", 0, "every this long while a build runs, append the database's statistics and the Go runtime's memory to metrics.jsonl in the candidate's directory (0, the default: off; at least 1s otherwise)")
}

// resolve gives -rest-after-retention its default once fs is parsed: on for an
// -untimed build, off otherwise, unless it was given. It refuses a -metrics-every
// that is negative or shorter than a second.
func (b *buildFlags) resolve(fs *flag.FlagSet, untimed bool) error {
	given := false
	fs.Visit(func(f *flag.Flag) { given = given || f.Name == "rest-after-retention" })
	if !given {
		b.rest = untimed
	}
	if b.metricsEvery < 0 || (b.metricsEvery > 0 && b.metricsEvery < time.Second) {
		return fmt.Errorf("-metrics-every %s: give 0 for none, or at least 1s", b.metricsEvery)
	}
	return nil
}

// args are the flags that make a build step do what b says, both given explicitly.
func (b buildFlags) args() []string {
	return []string{
		"-rest-after-retention=" + strconv.FormatBool(b.rest), "-canonical-layout=" + strconv.FormatBool(b.canonical),
		"-sync=" + strconv.FormatBool(b.sync), "-metrics-every=" + b.metricsEvery.String(),
	}
}

func (b buildFlags) options() runner.BuildOptions {
	return runner.BuildOptions{RestAfterRetention: b.rest, CanonicalLayout: b.canonical, Sync: b.sync, MetricsEvery: b.metricsEvery}
}

// forward is step with b's flags added to every build it starts.
func (b buildFlags) forward(step func(cmd string, extra ...string) error) func(cmd string, extra ...string) error {
	return func(cmd string, extra ...string) error {
		if cmd == "build" {
			extra = append(slices.Clone(extra), b.args()...)
		}
		return step(cmd, extra...)
	}
}

// agrees returns why the candidate built under dir, if one is, was built otherwise
// than b says, or nil. A manifest without the key of the canonical layout is of a
// build without it, and one without the key of the sync, which every engine has always
// described, is of a build without it; one without the key of the metrics sampling is
// of a build that did not sample; one without the key of the rest is of a binary that
// did not record it, which agrees with neither.
func (b buildFlags) agrees(dir string) error {
	if _, err := os.Stat(filepath.Join(dir, runner.ManifestFile)); errors.Is(err, os.ErrNotExist) {
		return nil
	}
	m, err := runner.LoadManifest(dir)
	if err != nil {
		return err
	}
	orDefault := func(key, def string) string {
		if v := m.Describe[key]; v != "" {
			return v
		}
		return def
	}
	canonical := orDefault(runner.CanonicalKey, "false")
	for _, f := range []struct{ flag, key, have, want string }{
		{"-canonical-layout", runner.CanonicalKey, canonical, strconv.FormatBool(b.canonical)},
		{"-rest-after-retention", runner.RestKey, m.Describe[runner.RestKey], strconv.FormatBool(b.rest)},
		{"-sync", runner.SyncKey, orDefault(runner.SyncKey, "false"), strconv.FormatBool(b.sync)},
		{"-metrics-every", runner.MetricsKey, orDefault(runner.MetricsKey, "0s"), b.metricsEvery.String()},
	} {
		if f.have != f.want {
			return fmt.Errorf("%s was built with %s %q, and this run asks for %s=%s: a family is built all with or all without it (remove that build, give the flag its value, or use another -out)", dir, f.key, f.have, f.flag, f.want)
		}
	}
	return nil
}

// agreeAll is the first candidate under out, of any that has a manifest and not only
// those a run names, built otherwise than b says.
func (b buildFlags) agreeAll(out string) error {
	entries, err := os.ReadDir(out)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, e := range entries {
		dir := filepath.Join(out, e.Name())
		isDir := e.IsDir()
		if !isDir && e.Type()&os.ModeSymlink != 0 { // a link to a candidate directory is one
			st, err := os.Stat(dir)
			isDir = err == nil && st.IsDir()
		}
		if !isDir {
			continue
		}
		if err := b.agrees(dir); err != nil {
			return err
		}
	}
	return nil
}

func doRead(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("read", flag.ExitOnError)
	var c common
	var name string
	c.flags(fs)
	fs.StringVar(&name, "candidate", "", "the candidate to read (required)")
	_ = fs.Parse(args)
	plan, err := loadPlan(c)
	if err != nil {
		return err
	}
	v, err := candidates.Lookup(name)
	if err != nil {
		return err
	}
	res, err := runner.Read(ctx, plan, v, runner.CandidateDir(c.out, v.Name), c.guards(), progress)
	if err != nil {
		return err
	}
	progress("%s: %d queries, %d answers differ from the reference engine's, %d unstable", v.Name, len(res.Queries), len(res.Mismatches), len(res.Unstable))
	if n := len(res.Mismatches); n > 0 {
		return fmt.Errorf("%s answered %d queries differently from the reference engine (the first: %s); the results are written", v.Name, n, res.Mismatches[0])
	}
	return nil
}

func doReport(args []string) error {
	fs := flag.NewFlagSet("report", flag.ExitOnError)
	var c common
	var names string
	var all bool
	c.flags(fs)
	fs.StringVar(&names, "candidates", "", "the candidates to compare (default: every one that has been read)")
	fs.BoolVar(&all, "all", false, "print every age of every read (the full report is always written to report.txt)")
	_ = fs.Parse(args)
	plan, err := loadPlan(c)
	if err != nil {
		return err
	}
	var cs []*runner.Candidate
	if names == "" { // every candidate that has been read, whatever its name
		if cs, err = runner.LoadAllCandidates(c.out); err != nil {
			return err
		}
	} else {
		for _, n := range strings.Split(names, ",") {
			v, err := candidates.Lookup(n)
			if err != nil {
				return err
			}
			cand, err := runner.LoadCandidate(runner.CandidateDir(c.out, v.Name))
			if err != nil {
				return err
			}
			cs = append(cs, cand)
		}
	}
	if len(cs) == 0 {
		return fmt.Errorf("no candidate under %s has been read", c.out)
	}
	var full strings.Builder
	runner.Write(&full, plan, cs, true)
	if err := os.WriteFile(filepath.Join(c.out, "report.txt"), []byte(full.String()), 0o644); err != nil {
		return err
	}
	if all {
		fmt.Print(full.String())
	} else {
		runner.Write(os.Stdout, plan, cs, false)
	}
	if problems := runner.Check(plan, cs, c.untimed); len(problems) > 0 {
		return fmt.Errorf("these results cannot be compared (%d reasons, listed above): %s", len(problems), problems[0])
	}
	return nil
}

// doRun makes the plan if there is none, then builds and reads each candidate in
// a process of its own (one engine per process keeps the Go heap and the block
// cache apart), then reports.
func doRun(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	var c common
	var p planFlags
	var b buildFlags
	var names string
	c.flags(fs)
	p.flags(fs)
	b.flags(fs)
	fs.StringVar(&names, "candidates", strings.Join(candidates.Names(), ","), "the candidates to run")
	_ = fs.Parse(args)
	if err := b.resolve(fs, c.untimed); err != nil {
		return err
	}
	planFlagSet := false
	fs.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "preset", "days", "seed", "batch", "retain", "hot", "median", "cache-fraction", "cache-mb", "min-non-empty", "window", "pins", "events-per-second", "extend", "pod-heartbeat", "run-max-age", "payload-pad", "full":
			planFlagSet = true
		}
	})
	if err := c.require(); err != nil {
		return err
	}
	self, err := os.Executable()
	if err != nil {
		return err
	}
	step := func(cmd string, extra ...string) error {
		argv := append([]string{cmd, "-out", c.out}, extra...)
		if c.untimed {
			argv = append(argv, "-untimed")
		}
		return selfStep(ctx, self, argv...)
	}
	if _, err := os.Stat(filepath.Join(c.out, planFile)); err != nil {
		if err := step("plan", p.args()...); err != nil {
			return err
		}
	} else if planFlagSet {
		// A plan is made once and every build is checked against it: flags that
		// would make another plan are not quietly dropped.
		return fmt.Errorf("%s exists, so the plan flags (preset, days, seed, batch, retain, hot, median, cache-fraction, cache-mb, min-non-empty, window, pins, events-per-second, extend, pod-heartbeat, run-max-age, payload-pad, full) cannot be applied: drop them to use that plan, or use another -out", filepath.Join(c.out, planFile))
	}
	list := strings.Split(names, ",")
	// A run that resumes builds the candidates it has not built yet as the flags say,
	// so the ones it built before must have been built so too.
	if err := b.agreeAll(c.out); err != nil {
		return err
	}
	plan, err := loadPlan(c)
	if err != nil {
		return err
	}
	planDigest, err := plan.Digest()
	if err != nil {
		return err
	}
	exe := runner.ReadBuildInfo().Executable
	ran, failed := runCandidates(ctx, list, b.forward(step), func(n string) candidateState {
		return diskState(runner.CandidateDir(c.out, n), planDigest, exe, c.untimed)
	})
	if len(ran) > 0 && ctx.Err() == nil {
		if err := step("report", "-candidates", strings.Join(ran, ",")); err != nil {
			failed = append(failed, "report: "+err.Error())
		}
	}
	if len(failed) > 0 {
		return fmt.Errorf("%s", strings.Join(failed, "\n"))
	}
	return nil
}

// diskState reads what is on disk under a candidate's directory: results of any
// kind, a manifest of a build from this plan by the binary with this hash in the same
// mode (timed or not), and results read from that.
func diskState(dir, planDigest, executable string, untimed bool) candidateState {
	var st candidateState
	if _, err := os.Stat(filepath.Join(dir, runner.ResultsFile)); err == nil {
		st.hasResults = true
	}
	m, err := runner.LoadManifest(dir)
	if err != nil || m.PlanDigest != planDigest || m.Build.Executable != executable || m.Untimed != untimed {
		return st
	}
	st.built = true
	// (LoadCandidate refuses results read from another manifest than this one.)
	if cand, err := runner.LoadCandidate(dir); err == nil && cand.Results.Untimed == untimed {
		st.done = true
	}
	return st
}

// candidateState is what is on disk for a candidate: built from the plan by this
// binary (a manifest), read as well (results that match), or results of any kind.
type candidateState struct{ built, done, hasResults bool }

// runCandidates builds and reads each candidate with step. A candidate already
// built and read from this plan is skipped, and one built and not read is only read,
// so an interrupted run is resumed by running it again. It stops starting
// candidates once ctx is done. A candidate that fails is reported and the others
// still run, so one bad candidate does not hide the rest. It returns the candidates
// that have results, and what failed.
func runCandidates(ctx context.Context, names []string, step func(cmd string, extra ...string) error, state func(name string) candidateState) (ran, failed []string) {
	for _, n := range names {
		if ctx.Err() != nil {
			failed = append(failed, "stopped before "+n)
			break
		}
		st := state(n)
		if st.done {
			progress("%s was built and read from this plan already: not again", n)
			ran = append(ran, n)
			continue
		}
		cmds := []string{"build", "read"}
		if st.built {
			progress("%s was built from this plan already: only read", n)
			cmds = cmds[1:]
		}
		ok, readFailed := true, false
		for _, cmd := range cmds {
			if err := step(cmd, "-candidate", n); err != nil {
				failed = append(failed, fmt.Sprintf("%s of %s: %v", cmd, n, err))
				ok, readFailed = false, cmd == "read"
				break
			}
		}
		// A read that found answers wrong still writes its results, and they are
		// reported; a build that failed has none of this plan's.
		if ok || (readFailed && state(n).hasResults) {
			ran = append(ran, n)
		}
	}
	return ran, failed
}

// selfStep runs a step of this program in a process of its own: asked to stop, not
// killed, so that it stops between batches and queries.
func selfStep(ctx context.Context, self string, argv ...string) error {
	cm := exec.CommandContext(ctx, self, argv...)
	cm.Cancel = func() error { return cm.Process.Signal(syscall.SIGTERM) }
	cm.WaitDelay = time.Minute
	cm.Env = append(os.Environ(), childEnv+"=1")
	cm.Stdout, cm.Stderr = os.Stdout, os.Stderr
	return cm.Run()
}

// parseWindows reads a list of days such as 2,7,14,30, which must be increasing.
func parseWindows(list string) ([]int, error) {
	var out []int
	for _, part := range strings.Split(list, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil || n < 1 || (len(out) > 0 && n <= out[len(out)-1]) {
			return nil, fmt.Errorf("-windows: %q is not an increasing list of days", list)
		}
		out = append(out, n)
	}
	if len(out) < 3 {
		return nil, fmt.Errorf("-windows: G1 needs at least three windows")
	}
	return out, nil
}

// doPins chooses the pinned prefixes from the stream of the shortest window and
// writes them where every window will find them.
func doPins(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("pins", flag.ExitOnError)
	var c common
	var p planFlags
	c.flags(fs)
	p.flags(fs)
	_ = fs.Parse(args)
	if err := c.require(); err != nil {
		return err
	}
	if p.window < 1 || p.pins != "" {
		return fmt.Errorf("pins: give -window (the shortest window, whose stream the prefixes are chosen from) and not -pins")
	}
	p.full = false // the pins are chosen from the whole stream either way
	spec, err := p.spec()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(c.out, 0o755); err != nil {
		return err
	}
	path := filepath.Join(c.out, runner.PinsFile)
	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("%s exists: the pins of a family of windows are chosen once; use another -out", path)
	}
	progress("pins: choosing the prefixes from a stream of %s", spec.Workload.Duration)
	pins, err := runner.MakePins(ctx, spec)
	if err != nil {
		return err
	}
	b, err := json.MarshalIndent(pins, "", " ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(path, append(b, '\n'), 0o644); err != nil {
		return err
	}
	n := 0
	for _, pc := range pins.Classes {
		n += len(pc.Records) + len(pc.Extensions) + len(pc.Median)
	}
	progress("pins written to %s: %d prefixes in %d classes", path, n, len(pins.Classes))
	return nil
}

// doWindows runs the candidates over each window of retained history, a process per
// step, from pins chosen once, and then judges G1 from them.
func doWindows(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("windows", flag.ExitOnError)
	var c common
	var p planFlags
	var b buildFlags
	var names, windows string
	c.flags(fs)
	p.flags(fs)
	b.flags(fs)
	fs.StringVar(&windows, "windows", "2,7,14,30", "the windows of retained history to run, in days")
	fs.StringVar(&names, "candidates", strings.Join(candidates.Names(), ","), "the candidates to run")
	_ = fs.Parse(args)
	if err := b.resolve(fs, c.untimed); err != nil {
		return err
	}
	if err := c.require(); err != nil {
		return err
	}
	if p.window != 0 || p.pins != "" {
		return fmt.Errorf("windows: -window and -pins are set for each window, from -windows")
	}
	days, err := parseWindows(windows)
	if err != nil {
		return err
	}
	self, err := os.Executable()
	if err != nil {
		return err
	}
	pinsPath := filepath.Join(c.out, runner.PinsFile)
	if _, err := os.Stat(pinsPath); err != nil {
		argv := append([]string{"pins", "-out", c.out, "-window", strconv.Itoa(days[0])}, p.args()...)
		if c.untimed {
			argv = append(argv, "-untimed")
		}
		if err := selfStep(ctx, self, argv...); err != nil {
			return fmt.Errorf("choosing the pins: %w", err)
		}
	}
	// A family is built all with or all without these flags: every window is checked
	// before the first one is run, so that a window refused late does not follow hours of
	// builds in the windows before it.
	for _, d := range days {
		if err := b.agreeAll(runner.WindowDir(c.out, d)); err != nil {
			return fmt.Errorf("the window of %d days: %w", d, err)
		}
	}
	for _, d := range days {
		if ctx.Err() != nil {
			return fmt.Errorf("stopped before the window of %d days", d)
		}
		progress("windows: %d days of retained history", d)
		// What this window would be planned as, from the flags and the pins: a check that
		// the pins are of this scenario before anything runs, and the thing a plan from an
		// earlier, stopped run must be.
		wp := p
		wp.window, wp.pins = d, pinsPath
		spec, err := wp.spec()
		if err != nil {
			return fmt.Errorf("the window of %d days: %w", d, err)
		}
		want, err := spec.Digest()
		if err != nil {
			return err
		}
		dir := runner.WindowDir(c.out, d)
		// Each run refuses to resume over a build made otherwise than these flags say.
		argv := append([]string{"run", "-out", dir, "-candidates", names}, b.args()...)
		if _, statErr := os.Stat(filepath.Join(dir, planFile)); statErr != nil {
			argv = append(argv, "-window", strconv.Itoa(d), "-pins", pinsPath)
			argv = append(argv, p.args()...)
		} else if have, err := runner.LoadPlan(filepath.Join(dir, planFile)); err != nil {
			return err
		} else if have.SpecDigest != want { // a run that stopped is resumed on its own plan, if the flags still make it
			return fmt.Errorf("the plan in %s is not the one these flags make (the pins or the binary changed since): use another -out", dir)
		}
		if c.untimed {
			argv = append(argv, "-untimed")
		}
		if err := selfStep(ctx, self, argv...); err != nil {
			return fmt.Errorf("the window of %d days: %w", d, err)
		}
	}
	g1 := []string{"g1", "-out", c.out, "-windows", windows}
	if c.untimed {
		g1 = append(g1, "-untimed")
	}
	return selfStep(ctx, self, g1...)
}

// doG1 judges G1 from the windows under -out and writes the verdicts to g1.txt, and
// for tools to g1.json.
func doG1(args []string) error {
	fs := flag.NewFlagSet("g1", flag.ExitOnError)
	var c common
	var windows string
	c.flags(fs)
	fs.StringVar(&windows, "windows", "", "the windows to judge over, in days (default: the rules')")
	_ = fs.Parse(args)
	if err := c.require(); err != nil {
		return err
	}
	rules := runner.DefaultRules()
	if windows != "" {
		days, err := parseWindows(windows)
		if err != nil {
			return err
		}
		rules.Windows, rules.TargetWindow = days, days[len(days)-1]
	}
	// A report of an earlier judgement is not left beside a text that disagrees with it.
	if err := os.Remove(filepath.Join(c.out, "g1.json")); err != nil && !os.IsNotExist(err) {
		return err
	}
	ws, err := runner.LoadWindows(c.out)
	if err != nil {
		return err
	}
	var text strings.Builder
	problems := runner.CheckWindows(ws, c.untimed)
	if len(problems) > 0 {
		text.WriteString("THESE WINDOWS ARE NOT TO BE JUDGED TOGETHER:\n")
		for _, p := range problems {
			text.WriteString("  - " + p + "\n")
		}
	}
	cells, gerr := runner.G1(ws, rules)
	var shown strings.Builder
	shown.WriteString(text.String())
	if gerr == nil {
		runner.WriteG1(&text, cells, rules, 0) // the file has every cell
		runner.WriteG1(&shown, cells, rules, 25)
	}
	if err := os.WriteFile(filepath.Join(c.out, "g1.txt"), []byte(text.String()), 0o644); err != nil {
		return err
	}
	if gerr == nil { // the same verdicts for tools
		b, err := json.MarshalIndent(runner.NewG1Report(cells, rules, problems), "", " ")
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(c.out, "g1.json"), append(b, '\n'), 0o644); err != nil {
			return err
		}
	}
	fmt.Print(shown.String())
	if gerr != nil {
		return gerr
	}
	if len(problems) > 0 {
		return fmt.Errorf("these windows cannot be judged together (%d reasons, listed above): %s", len(problems), problems[0])
	}
	return nil
}
