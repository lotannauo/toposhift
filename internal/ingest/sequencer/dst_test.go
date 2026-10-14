package sequencer_test

import (
	"fmt"
	"maps"
	"os"
	"slices"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// Seeded simulation of ingest on the reference store.
//
// Each seed draws a small cluster with churn (three nodes and a dozen pods that are
// created, changed and deleted), delivered every five minutes by both receivers and
// by watch events, and a schedule of two to four faults after a warm-up: restarts
// and crashes, writes refused, failed or landed, translation panics, lookups that
// abort or fail to read, silences, duplicates, drops, late backlogs and shuffled
// requests. The path under test is the one a request takes: receiver request,
// translators, resume step, checks, coalescer, store.
//
// Two oracles judge it. The control is the same cluster through the same code with
// no fault, given the requests the faulted run had answered 200, each at its
// scheduled instant. The simulated cluster is the other: what existed, and when.
// Every seed is run twice if its number is a multiple of eight, and the two runs'
// digests must match, so that a failure reproduces from its seed.

// dstSeedBase is the draw of seed 0; seed i is dstSeedBase + i.
const dstSeedBase = 20261013

// dstKnownFailing maps a seed that fails on a bug already recorded in the known
// issues to the issue's id. A listed seed is skipped; a listed seed that passes fails
// the test, so that a fixed bug is noticed. It is empty unless a bug was found and
// recorded.
var dstKnownFailing = map[int]string{}

// dstDefaultSeeds is how many seeds a plain run takes, a run with the race detector,
// and a short run.
func dstDefaultSeeds() int {
	switch {
	case testing.Short():
		return 6
	case dstRaceOn:
		return 8
	}
	return 32
}

// dstSeedIndexes are the seeds to run: TOPOSHIFT_DST_SEED=n takes exactly seed n,
// TOPOSHIFT_DST_SEEDS=n takes seeds 0 to n-1, and otherwise the tier's.
func dstSeedIndexes(t *testing.T) []int {
	t.Helper()
	if v := os.Getenv("TOPOSHIFT_DST_SEED"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			t.Fatalf("TOPOSHIFT_DST_SEED=%q: want a seed number", v)
		}
		return []int{n}
	}
	n := dstDefaultSeeds()
	if v := os.Getenv("TOPOSHIFT_DST_SEEDS"); v != "" {
		var err error
		if n, err = strconv.Atoi(v); err != nil || n < 1 {
			t.Fatalf("TOPOSHIFT_DST_SEEDS=%q: want a count", v)
		}
	}
	out := make([]int, n)
	for i := range out {
		out[i] = i
	}
	return out
}

type dstOptions struct {
	// report runs class R faults, whose differences from the control are reported.
	report bool
	// off runs with the resume step off.
	off bool
	// schedule, when set, replaces the drawn schedule.
	schedule func(*dstSchedule)
}

// dstOutcome is what a seed gave.
type dstOutcome struct {
	sd    *dstSchedule
	fail  *dstFailures
	lines []string
	info  string
	run   *dstRun
	ctl   *dstRun
}

func dstHeader(idx int, sd *dstSchedule) string {
	rerun := fmt.Sprintf("go test ./internal/ingest/sequencer -run 'TestDST/seed=%d$' -count=1 -v", idx)
	if idx >= dstDefaultSeeds() {
		rerun = fmt.Sprintf("TOPOSHIFT_DST_SEED=%d ", idx) + rerun
	}
	return fmt.Sprintf("dst seed=%d (rerun: %s)\n%s", idx, rerun, sd)
}

// dstRunSeed runs a seed, with its control, and judges it.
func dstRunSeed(t *testing.T, idx int, o dstOptions) *dstOutcome {
	t.Helper()
	began := time.Now()
	sd := dstNewSchedule(idx, o.report)
	if o.schedule != nil {
		o.schedule(sd)
	}
	world := newDSTWorld(t, sd.draw, sd.k, sd.force)
	plan := sd.plan(world)
	fail := &dstFailures{}

	run := newDSTRun(t, sd, world, plan, nil, fail)
	run.off, run.report = o.off, o.report
	run.run()

	cfail := &dstFailures{}
	ctl := newDSTRun(t, sd, world, nil, run.accepted, cfail)
	ctl.off, ctl.report = o.off, o.report
	ctl.run()
	for _, l := range cfail.list {
		fail.add("control", "%s", l)
	}

	rg, cg := dstReadGrid(t, run.mem, world), dstReadGrid(t, ctl.mem, world)
	wins := dstWindows(run.faults)
	dstNoPhantom(fail, "run", world, rg)
	dstNoPhantom(fail, "control", world, cg)
	resets := dstResets(run.faults)
	compared, inside, explained := dstCompare(fail, world, rg, cg, wins, resets, !o.report)
	pf := fail
	if o.report {
		pf = &dstFailures{}
	}
	phases := dstPhases(pf, world, run.records(), ctl.records(), rg.at, wins, resets)
	rs, cs := run.stats(), ctl.stats()
	dstCheckCounters(fail, run, rs)
	dstCheckCounters(fail, ctl, cs)
	runLate, ctlLate := dstLateness(run.batches(), run.batchAt), dstLateness(ctl.batches(), ctl.batchAt)
	dstCheckLateness(fail, sd, runLate, ctlLate)
	runRecs, ctlRecs := run.records(), ctl.records()
	absence := dstCheckDeletes(fail, world, runRecs, ctlRecs, wins)

	var lines []string
	for _, r := range runRecs {
		lines = append(lines, "record "+dstRecLine(r))
	}
	for _, a := range run.answers {
		lines = append(lines, "answer "+a.String())
	}
	lines = append(lines, fmt.Sprintf("stats %v", counters(rs)))
	for _, l := range rg.lines(world) {
		lines = append(lines, "grid "+l)
	}

	var names []string
	for _, f := range run.faults {
		names = append(names, string(f.class))
	}
	info := fmt.Sprintf("seed %d: faults %v; answers compared %d (and %d pod phases), differing inside windows %d, differing where the cluster explains it %d; lateness over 5m: run %d (max %s), control %d (max %s); longest absence in a W3 window %s; resume %v; %d requests, %d answers; %s",
		idx, names, compared, phases, inside, explained, runLate.overFive, runLate.max, ctlLate.overFive, ctlLate.max, absence, counters(rs), len(world.reqs), len(run.answers), time.Since(began).Round(time.Millisecond))
	return &dstOutcome{sd: sd, fail: fail, lines: lines, info: info, run: run, ctl: ctl}
}

// dstCheckSeed runs one seed and reports what it found. A seed whose number is a
// multiple of eight runs twice, and the digests of the two must match.
func dstCheckSeed(t *testing.T, idx int) {
	t.Helper()
	out := dstRunSeed(t, idx, dstOptions{})
	t.Log(out.info)
	if idx%8 == 0 {
		again := dstRunSeed(t, idx, dstOptions{})
		if a, b := dstDigest(out.lines), dstDigest(again.lines); a != b {
			out.fail.add("determinism", "two runs of the seed differ (digests %.12s and %.12s); %s", a, b, dstFirstDifference(out.lines, again.lines))
		}
	}
	id, known := dstKnownFailing[idx]
	switch {
	case known && !out.fail.any():
		t.Errorf("seed %d passes: remove it from dstKnownFailing and close %s", idx, id)
	case known:
		t.Skipf("known failing: %s", id)
	case out.fail.any():
		msg := dstHeader(idx, out.sd)
		for _, l := range out.fail.list {
			msg += "\n  " + l
		}
		for _, check := range slices.Sorted(maps.Keys(out.fail.counts)) {
			msg += fmt.Sprintf("\n  (%s failed %d times)", check, out.fail.counts[check])
		}
		t.Error(msg)
	}
}

func TestDST(t *testing.T) {
	for _, idx := range dstSeedIndexes(t) {
		t.Run(fmt.Sprintf("seed=%d", idx), func(t *testing.T) {
			t.Parallel()
			dstCheckSeed(t, idx)
		})
	}
}

// The schedules of the default seeds hold every judged class at least twice, and
// the pairs that make a fault meet the state another left.
func TestDSTCoverage(t *testing.T) {
	counts := map[dstClass]int{}
	pairs := map[string]bool{}
	for idx := range 32 {
		sd := dstNewSchedule(idx, false)
		for i, f := range sd.faults {
			counts[f.class]++
			if f.class == dstT7 && f.multi {
				pairs["T7 in a group"] = true
			}
			if f.class == dstT3 && f.multi {
				pairs["T3 in a group"] = true
			}
			if i == 0 {
				continue
			}
			p := sd.faults[i-1].class
			for _, want := range [][2]dstClass{{dstT1, dstW2}, {dstW1, dstT1}, {dstT8, dstT5}, {dstT2, dstT2}} {
				if p == want[0] && f.class == want[1] {
					pairs[string(want[0])+" then "+string(want[1])] = true
				}
			}
		}
	}
	for _, c := range dstJudged {
		if counts[c] < 2 {
			t.Errorf("class %s occurs %d times in the 32 default seeds, want at least 2", c, counts[c])
		}
	}
	for _, p := range []string{"T1 then W2", "W1 then T1", "T7 in a group", "T3 in a group", "T8 then T5", "T2 then T2"} {
		if !pairs[p] {
			t.Errorf("no default seed holds %s", p)
		}
	}
	t.Logf("classes %v; pairs %v", counts, pairs)
}

// The power of the simulation: with the resume step off, a restart or a crash
// writes the records of the objects it saw again timed at their creation, hours
// late, and the lateness bound must fail on at least one such seed.
func TestDSTPowerWithoutResume(t *testing.T) {
	var seeds []int
	for _, idx := range dstSeedIndexes(t) {
		sd := dstNewSchedule(idx, false)
		if (sd.has(dstT1) || sd.has(dstT2)) && len(seeds) < 6 {
			seeds = append(seeds, idx)
		}
	}
	if len(seeds) == 0 {
		t.Fatal("no seed holds a restart or a crash")
	}
	var failed atomic.Int32
	t.Run("seeds", func(t *testing.T) {
		for _, idx := range seeds {
			t.Run(fmt.Sprintf("seed=%d", idx), func(t *testing.T) {
				t.Parallel()
				out := dstRunSeed(t, idx, dstOptions{off: true})
				if out.fail.failed("I5") {
					failed.Add(1)
					t.Logf("seed %d: %s", idx, out.fail.list[0])
				}
			})
		}
	})
	if failed.Load() == 0 {
		t.Errorf("the lateness bound held on all of %v with the resume step off: the simulation has no power there", seeds)
	}
}

// Without a fault, the faulted path and the control are the same configuration,
// and the record lists must be equal: the harness is deterministic. A duplicate
// that the coalescer absorbs writes nothing more.
func TestDSTHarnessDeterministic(t *testing.T) {
	for _, c := range []struct {
		name   string
		faults []dstFault
	}{
		{"no fault", nil},
		{"one duplicate", []dstFault{{class: dstT11, start: t0.Add(90 * time.Minute), mode: int(dstPullReq)}}},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			out := dstRunSeed(t, 100, dstOptions{schedule: func(sd *dstSchedule) {
				sd.faults, sd.configured, sd.k, sd.noGroups, sd.force = slices.Clone(c.faults), true, 0, true, map[int]bool{}
			}})
			if out.fail.any() {
				t.Error(dstHeader(100, out.sd), out.fail.list)
			}
			if d := diffRecords(out.run.records(), out.ctl.records()); d != "" {
				t.Errorf("the record lists differ: %s", d)
			}
		})
	}
}

// The requests of class R faults are outside what is supported, or beyond a bound
// the code states. Their answers are only reported; the checks that do not compare
// with the control are judged.
func TestDSTReport(t *testing.T) {
	if testing.Short() || dstRaceOn {
		t.Skip("class R faults are reported in the plain run")
	}
	for idx := range 8 {
		t.Run(fmt.Sprintf("seed=%d", idx), func(t *testing.T) {
			t.Parallel()
			out := dstRunSeed(t, idx, dstOptions{report: true})
			t.Log(out.info)
			if out.fail.any() {
				t.Error(dstHeader(idx, out.sd), out.fail.list)
			}
		})
	}
}
