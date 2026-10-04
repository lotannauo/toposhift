// Command spikebench builds the candidate layouts on one planned stream and
// compares what their reads cost, in counters that do not depend on timing.
//
//	spikebench plan   -preset ci -out DIR            analyze the stream, choose and answer the queries
//	spikebench build  -out DIR -candidate L/off      write the planned stream to one candidate
//	spikebench read   -out DIR -candidate L/off      ask the queries of a built candidate
//	spikebench report -out DIR                       set the candidates side by side
//	spikebench run    -preset ci -out DIR            all of the above, a process per step
//
// Build with CGO_ENABLED=0 and without -race or the invariants tag, from a clean
// tree; the program refuses to produce a result otherwise (-untimed allows it
// for a validation run whose results are not to be compared). The output
// directory must be outside the repository.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
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
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "spikebench:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: spikebench plan|build|read|report|run [flags]   (spikebench <command> -h for the flags)")
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
	fs.Float64Var(&p.cacheFraction, "cache-fraction", 0, "block cache as a share of the stream's payload bytes (default: the spec's)")
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
	if p.cacheFraction > 0 {
		s.CacheFraction = p.cacheFraction
	}
	if p.minNonEmpty >= 0 {
		s.MinNonEmpty = p.minNonEmpty
	}
	if (p.minNonEmpty < 0 && p.minNonEmpty != -1) || p.cacheFraction < 0 || p.days < 0 || p.batch < 0 || p.hot < 0 || p.median < 0 {
		return runner.Spec{}, fmt.Errorf("-days, -batch, -hot, -median, -min-non-empty and -cache-fraction cannot be negative (leave a flag out, or 0, for the spec's own value)")
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
	var name string
	c.flags(fs)
	fs.StringVar(&name, "candidate", "", "the candidate to build (required)")
	_ = fs.Parse(args)
	plan, err := loadPlan(c)
	if err != nil {
		return err
	}
	v, err := candidates.Lookup(name)
	if err != nil {
		return err
	}
	_, err = runner.Build(ctx, plan, v, runner.CandidateDir(c.out, v.Name), c.guards(), progress)
	return err
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
	list := candidates.Names()
	if names != "" {
		list = strings.Split(names, ",")
	}
	var cs []*runner.Candidate
	for _, n := range list {
		v, err := candidates.Lookup(n)
		if err != nil {
			return err
		}
		dir := runner.CandidateDir(c.out, v.Name)
		if _, err := os.Stat(filepath.Join(dir, runner.ResultsFile)); names == "" && err != nil {
			continue // not asked for by name, and not run
		}
		cand, err := runner.LoadCandidate(dir)
		if err != nil {
			return err
		}
		cs = append(cs, cand)
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
	var names string
	c.flags(fs)
	p.flags(fs)
	fs.StringVar(&names, "candidates", strings.Join(candidates.Names(), ","), "the candidates to run")
	_ = fs.Parse(args)
	planFlagSet := false
	fs.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "preset", "days", "seed", "batch", "retain", "hot", "median", "cache-fraction", "min-non-empty":
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
		cm := exec.CommandContext(ctx, self, argv...)
		// A step is asked to stop, not killed: it stops between batches and queries.
		cm.Cancel = func() error { return cm.Process.Signal(syscall.SIGTERM) }
		cm.WaitDelay = time.Minute
		cm.Env = append(os.Environ(), childEnv+"=1")
		cm.Stdout, cm.Stderr = os.Stdout, os.Stderr
		return cm.Run()
	}
	if _, err := os.Stat(filepath.Join(c.out, planFile)); err != nil {
		if err := step("plan", p.args()...); err != nil {
			return err
		}
	} else if planFlagSet {
		// A plan is made once and every build is checked against it: flags that
		// would make another plan are not quietly dropped.
		return fmt.Errorf("%s exists, so the plan flags (preset, days, seed, batch, retain, hot, median, cache-fraction, min-non-empty) cannot be applied: drop them to use that plan, or use another -out", filepath.Join(c.out, planFile))
	}
	plan, err := loadPlan(c)
	if err != nil {
		return err
	}
	planDigest, err := plan.Digest()
	if err != nil {
		return err
	}
	list := strings.Split(names, ",")
	exe := runner.ReadBuildInfo().Executable
	ran, failed := runCandidates(ctx, list, step, func(n string) candidateState {
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
	if cand, err := runner.LoadCandidate(dir); err == nil && cand.Results.PlanDigest == planDigest && cand.Results.Untimed == untimed {
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
