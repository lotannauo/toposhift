package runner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/candidates"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
	"github.com/lotannauo/toposhift/bench/spike/pebblelog"
	"github.com/lotannauo/toposhift/internal/store/pebblestore"
)

// Manifest says what a built database is: which candidate, built from which plan,
// by which binary, and what it held when the build finished. A read refuses a
// database whose manifest is not the plan's.
type Manifest struct {
	Candidate, Layout string
	// PlanDigest is [Plan.Digest] of the plan it was built from.
	PlanDigest string
	Stream     StreamInfo
	Build      BuildInfo
	// Untimed says the binary was one a result may not come from.
	Untimed bool
	// Describe is how the engine was set up and what its tables were written
	// with, read back from the tables.
	Describe map[string]string
	// Counters are the engine's own counts while it was building: records and
	// versions written, retention work, checkpoints written, invalidated and
	// failed.
	Counters map[string]int64
	// StatsBuilt is the database as the stream left it, before anything was
	// compacted on purpose, and StatsCompacted after everything was.
	StatsBuilt, StatsCompacted map[string]int64
	// Breakdown is the logical bytes of every part, by "layer/part", and
	// SizeByLayer Pebble's estimate of the table bytes of each layer, both after
	// the compaction. Size is the bytes of the directory as the engine counts it.
	Breakdown, SizeByLayer map[string]int64
	Size                   int64
	// Timing is how long the writes took; see [Timing].
	Timing Timing
	// Uncompacted is what the reads of "now" cost at the end of the build, before
	// anything was compacted on purpose: the shape the stream left the tables in,
	// with the memtable and level 0 in it, which the reads after the compaction do not
	// see. Each answer was checked against the plan's, and UncompactedWrong names the
	// queries whose answer was not (a layout that answers wrongly only from the
	// tables as the stream left them): the report refuses to compare such a candidate,
	// as it does one that answers wrongly after the compaction.
	Uncompacted      []UncompactedRead
	UncompactedWrong []string
	// Gate is what the store gate judges a root-store retention from: the windows of the
	// retentions, the batches inside and outside them, and the memory (see [GateTrace]).
	// Only a candidate built through the root store records it.
	Gate *GateTrace `json:",omitempty"`
}

// UncompactedRead is what one read cost at the end of a build.
type UncompactedRead struct {
	Query    string
	Counters map[string]int64
}

// ManifestFile and ResultsFile are the names of the files in a candidate's
// directory; DBDir is the database.
const (
	ManifestFile = "manifest.json"
	ResultsFile  = "results.json"
	DBDir        = "db"
	// TalliesFile is where an untimed build of the spike's layout L writes, beside
	// the manifest, how many checkpoints each prefix lost to a late record and had
	// put back or not (see [WriteTallies]). It is not part of the manifest, whose
	// shape other tools read.
	TalliesFile = "tallies.json"
	// RestKey is the key of a manifest's Describe that says whether the build rested
	// after each retention ([BuildOptions]).
	RestKey = "rest_after_retention"
	// CanonicalKey is the key of a manifest's Describe that says whether the build
	// rewrote its compacted tables into the canonical layout ([BuildOptions]).
	CanonicalKey = "canonical_layout"
	// SyncKey is the key of a manifest's Describe that says whether every commit of
	// the build waited for the log to reach the disk ([BuildOptions]). It is the
	// engine's own description, not one the runner adds, and is among the descriptions
	// a read must find as the build gave them.
	SyncKey = "sync"
	// PaceKey is the key of a manifest's Describe that gives the rate, in records per
	// second, a background build held its writer to from the day it was paced from ("0":
	// not paced; a manifest without it is of a build that was not). PaceFromDayKey gives
	// that day, in stream days from the start ("11.5"), and is left out of a build that
	// was not paced.
	PaceKey        = "pace_records_per_second"
	PaceFromDayKey = "pace_from_day"
	// RetainFlushDelsKey is the key of a manifest's Describe that gives the count of
	// unflushed range deletions at which the root store's retention flushed, as the
	// number the store used (the default's number for a build that did not set it). Only
	// a candidate built through the root store has it; such a manifest without it is of
	// a build before it was recorded, whose threshold is not recorded, and is never read
	// as the default.
	RetainFlushDelsKey = "retain_flush_dels"
	// MetricsKey is the key of a manifest's Describe that gives the interval at which
	// the build sampled its metrics, as a duration ("0s" for none).
	MetricsKey = "metrics_every"
	// PostRetentionKey is the key of a manifest's Describe that gives how many batches
	// after each retention the build timed one by one (Timing.PostRetention).
	PostRetentionKey = "post_retention_batches"
	// GoMemoryLimitKey is the key of a manifest's Describe that gives the Go runtime's
	// memory limit the build ran under (GOMEMLIMIT), in bytes, or "none". A manifest
	// without it was built before the limit was recorded, and ran with none.
	GoMemoryLimitKey = "go_memory_limit"
	// SettleKey is the key of a manifest's Describe that says whether each retention of
	// the build settled its tombstones before returning. It is the engine's own
	// description, not one the runner adds.
	SettleKey = "settle_tombstones"
	// ShapeKey is the key of a manifest's Describe that says which table-shape and
	// compaction settings the build was opened with ([BuildOptions.Shape]), in the words
	// of [pebblekv.Tuning.ShapeString]. It is the engine's own description, written only
	// for a database opened with one; a manifest without it is of a build with Pebble's
	// own rule.
	ShapeKey = pebblekv.ShapeKey
	// MetricsFile is the name of the file in a candidate's directory that the
	// sampling of a build appends to.
	MetricsFile = "metrics.jsonl"
)

// CandidateDir is where a candidate is built under out.
func CandidateDir(out, name string) string {
	return filepath.Join(out, strings.NewReplacer("/", "_").Replace(name))
}

// partKey names a part of the breakdown.
func partKey(p engine.Part) string { return fmt.Sprintf("%s/%s", p.Layer, p.Kind) }

// optional interfaces a candidate must have to be measured.
type measurable interface {
	engine.Engine
	engine.Quiescer
	engine.ColdStarter
	engine.Statser
	engine.Describer
	engine.Breakdowner
	engine.LayerSizer
}

// buildSink writes the stream to a candidate, and stops the build at the first
// thing that is not as planned.
type buildSink struct {
	ctx context.Context
	e   measurable
	t   *Timing
	// rest says to wait for the database to be at rest after each retention.
	rest bool
	// afterRetention is set by a retention and cleared by the next write.
	afterRetention *bool
	// post counts the batches after the last retention that are still to be timed one
	// by one ([Timing].PostRetention); nil for none.
	post *postState
	// prog is told the phase and the progress, for the sampling of the build's
	// metrics; nil for none.
	prog *buildProgress
	// gate is told the batches and the retentions, for the store gate; nil for none.
	gate *gateTracker
	// background says the engine retains beside the writer: its windows are fed by the
	// store's observer, not made from the phases a retention reports when it returns,
	// and the build waits for a pass (its settle included) before it rests.
	background bool
	// pace holds the writer to a rate from a stream day on; nil for a build that is not
	// paced.
	pace *paceState
	// now reads the clock that times each batch; nil for [time.Now]. A test sets it to
	// move the clock by exactly what the engine and the pacer's sleep take.
	now func() time.Time
	// prof writes the profiles of the windows; nil for a build without them.
	prof *windowProfiler
}

// clock is the time now on the clock that times a batch.
func (s buildSink) clock() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}

// paceState is the pacing of a build: it switches on at the first batch whose stream day
// is fromDay or later and stays on.
type paceState struct {
	pacer    *pacer
	fromDay  float64
	streamAt time.Time // the start of the stream
	on       bool
}

// streamDay is the stream day of a batch: its last record's event time, which is what
// the stream is driven by (a retention comes when the last record of a batch reaches
// its instant), from the start of the stream, in days.
func (p *paceState) streamDay(batch []engine.Record) float64 {
	return batch[len(batch)-1].EventTime.Sub(p.streamAt).Hours() / 24
}

// postState is how many more batches after the last retention are to be recorded in
// [Timing].PostRetention, and how many a retention records at most.
type postState struct {
	left, n int
}

// lastRetainer is what an engine says of how its last retention spent its time.
type lastRetainer interface {
	LastRetain() (work, flush, settle time.Duration, deadlineHit bool)
}

// stallCounter is what an engine through the root store offers: the count of the times
// its database began to delay writes since it was opened, read without taking the
// database's statistics.
type stallCounter interface {
	WriteStalls() int64
}

// retainWaiter is what an engine that retains beside the writer offers: a wait for
// the retention in the background to end, its settle included.
type retainWaiter interface {
	WaitRetained(ctx context.Context) error
}

func (s buildSink) Write(batch []engine.Record) error {
	// The wait for the schedule comes before the batch's clock starts: neither the gate
	// nor the timings ever hold the sleep.
	if ps := s.pace; ps != nil {
		if !ps.on && ps.streamDay(batch) >= ps.fromDay {
			ps.on = true
			// The judged part starts here: its stalls are counted from the database's count
			// now, and its metrics lines are marked from now on.
			if s.gate != nil {
				if sc, ok := s.e.(stallCounter); ok {
					s.gate.stallsAtPacing(sc.WriteStalls())
				}
				s.prog.setJudged(true)
			}
		}
		if ps.on {
			ps.pacer.wait(len(batch))
			defer ps.pacer.done(len(batch))
		}
	}
	start := s.clock()
	if s.pace != nil && s.pace.on {
		s.gate.startPacing(start)
		s.prof.startPacing(start)
	}
	err := s.e.Write(batch)
	d := s.clock().Sub(start)
	s.t.Writes.Add(d)
	if *s.afterRetention {
		*s.afterRetention = false
		s.t.AfterRetention = append(s.t.AfterRetention, int64(d))
	}
	if p := s.post; p != nil && p.left > 0 && len(s.t.PostRetention) > 0 {
		p.left--
		last := len(s.t.PostRetention) - 1
		s.t.PostRetention[last] = append(s.t.PostRetention[last], int64(d))
	}
	if err != nil {
		return err
	}
	s.gate.batch(start, d, len(batch))
	s.prof.batch(d)
	s.prog.batch()
	if got, want := s.e.LastSeq(), batch[len(batch)-1].Seq; got != want {
		return fmt.Errorf("LastSeq is %d after a batch ending at %d", got, want)
	}
	return nil
}

func (s buildSink) Retain(h time.Time) error {
	s.prog.setPhase(phaseRetain)
	windows := s.gate.count()
	s.prof.retaining(len(s.t.Retains))
	start := time.Now()
	err := s.e.Retain(h)
	returned := time.Now()
	s.t.Retains = append(s.t.Retains, int64(returned.Sub(start)))
	if lr, ok := s.e.(lastRetainer); ok {
		work, flush, settle, hit := lr.LastRetain()
		switch {
		case err != nil:
		case s.background:
			// The store has told the tracker of the publication, from the call, and will
			// tell it of the rest as it happens.
			s.gate.returnedSince(windows, returned)
		default:
			s.gate.syncWindow(start, time.Now(), work, flush, settle)
		}
		s.t.RetainPhases = append(s.t.RetainPhases, RetainPhase{Work: int64(work), Flush: int64(flush), Settle: int64(settle), DeadlineHit: hit})
	}
	// A retention starts its own list of batches, and ends the previous one's.
	s.t.PostRetention = append(s.t.PostRetention, []int64{})
	if s.post != nil {
		s.post.left = s.post.n
	}
	*s.afterRetention = true
	if err != nil {
		return err
	}
	s.prog.retention()
	if !s.rest {
		s.prog.setPhase(phaseWrite)
		return nil
	}
	s.prog.setPhase(phaseRest)
	defer s.prog.setPhase(phaseWrite)
	restStart := time.Now()
	defer func() { s.gate.rested(time.Since(restStart)) }()
	if s.background {
		// A pass that is still running is not at rest: wait for it and its settle first.
		if err := s.e.(retainWaiter).WaitRetained(s.ctx); err != nil {
			return err
		}
	}
	// The retention is timed alone; then, untimed, the build waits until the database
	// is at rest. A retention writes a deletion for every prefix it rewrites, and a
	// writer that runs on before the compactions and Pebble's statistics of those
	// deletions have caught up piles them up in memory (several gigabytes at seven
	// days), which a store ingesting at the cluster's own pace does not do.
	return s.e.Quiesce(s.ctx)
}

// BuildOptions are the choices of a build that do not change what it writes.
type BuildOptions struct {
	// RestAfterRetention makes the build wait, after each retention and not counted in
	// its time, until the database is at rest. A writer that runs on before the
	// compactions and Pebble's statistics of a retention's deletions have caught up
	// piles them up in memory, which an ingest at the cluster's own pace does not; but
	// the first batch after a retention then does not carry the compactions' catch-up
	// either, so a timing of it from a rested build is not comparable with one from a
	// build that did not rest. It is recorded in the manifest (Describe, key
	// rest_after_retention).
	RestAfterRetention bool
	// CanonicalLayout makes the build, once it has compacted everything, rewrite the
	// data once in key order into bottom-level tables of the tuning's target file
	// size ([engine.Canonicalizer]), so the tables, and with them the blocks every
	// read after the compaction loads, are a function of the data and not of how fast
	// the writer ran against the compactions. Answers and the engine's counters do not
	// change, nor what a read steps over, except where the compacted tables still held
	// tombstones, which the rewrite drops (its points, internal steps and key bytes
	// then fall); the blocks a read loads, the bytes of the tables and where a value is
	// kept (in place or in a value block) do change, so the reads of a build with it
	// are not comparable with those of one without. It is
	// recorded in the manifest (Describe, key canonical_layout).
	CanonicalLayout bool
	// Sync makes every commit wait for the log to reach the disk, as production does.
	// It changes no stored byte and changes every commit's time, so the timings of a
	// build with it are not comparable with those of one without. It is recorded in
	// the manifest as the engine's own description (Describe, key sync), and builds
	// with and without it are never compared.
	Sync bool
	// MetricsEvery, if above zero, makes the build append, every MetricsEvery while
	// it runs, a line of the database's statistics and the Go runtime's memory to
	// [MetricsFile] in the candidate's directory: it is for finding what grows during
	// a build. Reading the statistics takes the database's metrics lock for a moment,
	// so the interval is recorded in the manifest (Describe, key metrics_every) and
	// builds with different intervals are not compared. For the same reason the
	// first-batch-after-a-retention figure of a sampled build is comparable only with
	// builds sampled the same way, which [Check] enforces. Zero is off.
	MetricsEvery time.Duration
	// RetentionMode is how the root store's retention runs, for the candidates built
	// through it and ignored by the others: "" and "sync" are its Synchronous mode,
	// "background" its Background one (see [candidates.CheckRetentionMode]). In the
	// background mode a retention returns when its horizons are published, the build
	// goes on writing while the store rewrites and settles, the gate's windows come from
	// the store's own report of each phase, and the build waits for the last retention to
	// end, its settle included, before it reads, sizes or compacts anything. It is
	// recorded in the manifest of such a candidate (Describe, key retention_mode), and
	// builds in different modes are not compared.
	RetentionMode string
	// Pace, in records per second, holds the writer of a build in the background mode to
	// that rate from stream day [PaceFromDay] of its window on, on an open-loop schedule
	// (see [pacer]); the days before are written as fast as the store takes them. 0 is not
	// paced. The store gate judges a build in the background only when paced at
	// [StoreGatePaceRate]. A build whose retention is synchronous, or that is not built
	// through the root store, or whose plan is not a window plan, is not paced. It is
	// recorded in the manifest (Describe, keys pace_records_per_second and
	// pace_from_day), and builds paced differently are not compared.
	Pace int
	// RetainFlushDels, if above 0, is the count of unflushed range deletions at which the
	// root store's retention flushes ([pebblestore.Options.RetainFlushDels]), for a
	// measurement that compares thresholds; 0 is the store's default. Only a build through
	// the root store in the background mode takes a value above 0, and none takes a
	// negative one. The threshold the store used is recorded in the manifest of every
	// build through the root store (Describe, key retain_flush_dels), and builds with
	// different thresholds are never compared or pooled.
	RetainFlushDels int
	// ProfileWindows makes a build in the background mode of the root store write, under
	// [ProfilesDir] in the candidate's directory, a CPU profile and a mutex profile of each
	// retention pass from its publication to the end of its rewrite, and a separate CPU
	// profile of the first 2 s after the publication, with an index of what
	// was written for each pass and why a pass that overlapped the window before it has
	// none (see [ProfilesDir] for the files). It is for finding where the writer's time
	// goes in a window. It sets the process's mutex profile fraction for the whole build,
	// and the profiles take CPU of their own, so the build is a diagnostic and not a
	// measurement: it is recorded in the manifest (Describe, key profile_windows). Off, the
	// build starts no profile, sets no fraction, writes no file under [ProfilesDir] and
	// records nothing of it.
	ProfileWindows bool
	// Shape holds the table-shape and compaction settings of the build, the fields of
	// [pebblekv.Tuning] from LBaseTargetFileSize on (the sizing fields of it are not
	// read: the candidate keeps the bench tuning's, and the plan's block cache). With
	// any set, the candidate is opened with [pebblekv.BenchTuning] and these. The zero
	// value leaves the candidate's tuning as it is. It is recorded in the manifest
	// (Describe, key pebble_shape, beside the Pebble options it comes to). The store gate
	// neither pools nor compares builds of different shapes, and a family is built with
	// one. The candidates built through Pebble take
	// it; a candidate made by [candidates.External] may refuse it.
	Shape pebblekv.Tuning
}

// RetainFlushDelsOf is the threshold a build through the root store runs with when it
// is given n: n itself, or the store's default for 0.
func RetainFlushDelsOf(n int) int {
	if n == 0 {
		return pebblestore.DefaultRetainFlushDels
	}
	return n
}

// checkRetainFlushDels refuses a threshold the build cannot take: a negative one, and
// one above 0 for a candidate that is not built through the root store or a build whose
// retention is synchronous.
func checkRetainFlushDels(v candidates.Variant, opts BuildOptions) error {
	switch n := opts.RetainFlushDels; {
	case n < 0:
		return fmt.Errorf("runner: retention flush threshold %d: a count of range deletions, or 0 for the store's default", n)
	case n == 0:
		return nil
	case !v.Root():
		return fmt.Errorf("runner: %s is not built through the root store: only a build in the background mode of the root store takes a retention flush threshold", v.Name)
	case opts.RetentionMode != candidates.RetentionBackground:
		return fmt.Errorf("runner: retention flush threshold %d with the retention mode %q: only a build in the background mode takes one", n, opts.RetentionMode)
	}
	return nil
}

// Build is [BuildWith] with the options of a build by this binary: it rests after
// each retention if the binary may not produce a result (untimed, as every run
// that chooses designs by counters is), and does not if a timing may come of it.
func Build(ctx context.Context, plan *Plan, v candidates.Variant, dir string, g Guards, say Progress) (*Manifest, error) {
	return BuildWith(ctx, plan, v, dir, g, BuildOptions{RestAfterRetention: g.Untimed}, say)
}

// BuildWith writes the plan's stream to the candidate under dir, compacts
// everything, and records what the database holds in dir's manifest. The
// directory must be new and outside any git worktree.
func BuildWith(ctx context.Context, plan *Plan, v candidates.Variant, dir string, g Guards, opts BuildOptions, say Progress) (*Manifest, error) {
	if err := g.CheckFor(v); err != nil {
		return nil, err
	}
	paceFrom, err := checkPace(plan, v, opts)
	if err != nil {
		return nil, err
	}
	if err := checkRetainFlushDels(v, opts); err != nil {
		return nil, err
	}
	if err := checkProfileWindows(v, opts); err != nil {
		return nil, err
	}
	// The shape is checked as the whole tuning it makes, which is what the database is
	// opened with; the base tuning's own fields are valid.
	if err := pebblekv.BenchTuning().WithShapeOf(opts.Shape).Validate(); err != nil {
		return nil, fmt.Errorf("runner: %w", err)
	}
	if err := CheckOutside(dir); err != nil {
		return nil, err
	}
	if entries, err := os.ReadDir(dir); err == nil && len(entries) > 0 {
		return nil, fmt.Errorf("runner: %s is not empty: a build starts from nothing (a directory with no manifest is a build that did not finish: remove it; one with a manifest and no results is built and not yet read, and `run` reads it)", dir)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	planDigest, err := plan.Digest()
	if err != nil {
		return nil, err
	}

	rec := NewCapture()
	var recorder engine.Recorder = rec
	var maxima *maxRecorder // only a candidate built through the root store has samples to keep the largest of
	if v.Root() {
		maxima = newMaxRecorder(rec)
		recorder = maxima
	}
	var gate *gateTracker // nil for a candidate that is not built through the root store
	if v.Root() {
		gate = newGateTracker(time.Now())
		maxima.gate = gate
		if opts.Pace > 0 {
			gate.setPaced()
		}
	}
	background := v.Root() && opts.RetentionMode == candidates.RetentionBackground
	// An untimed build keeps the per-prefix tallies of layout L: it changes no stored
	// byte, and a build whose time may be a result does not carry its cost.
	co := candidates.Options{CacheBytes: plan.CacheBytes, Recorder: recorder, DisableReadCompactions: true, Sync: opts.Sync, SettleRetention: true, RetentionMode: opts.RetentionMode, RetainFlushDels: opts.RetainFlushDels, TallyPrefixes: g.Untimed}
	if opts.Shape.HasShape() {
		co.Tuning = pebblekv.BenchTuning().WithShapeOf(opts.Shape)
	}
	var prof *windowProfiler // nil for a build that does not profile its windows
	if background {
		co.RetainObserver = gateObserver{gate}
		if opts.ProfileWindows {
			if prof, err = newWindowProfiler(dir, gate.epoch, opts.Pace > 0); err != nil {
				return nil, fmt.Errorf("runner: %s: profiles: %w", v.Name, err)
			}
			// Registered before the engine's close, so it runs after it: the store tells
			// nothing more once it is closed.
			defer func() { _ = prof.close() }()
			co.RetainObserver = profileObserver{gateObserver{gate}, prof}
		}
	}
	opened, err := v.Open(filepath.Join(dir, DBDir), co)
	if err != nil {
		return nil, err
	}
	e, ok := opened.(measurable)
	if !ok {
		_ = opened.Close()
		return nil, fmt.Errorf("runner: %s cannot report what a measurement needs (compaction, statistics, description, breakdown)", v.Name)
	}
	closed := false
	defer func() {
		if !closed {
			_ = e.Close()
		}
	}()

	prog := newBuildProgress()
	var samp *sampler
	if opts.MetricsEvery > 0 {
		if samp, err = startSampler(filepath.Join(dir, MetricsFile), opts.MetricsEvery, prog, e.Stats); err != nil {
			return nil, fmt.Errorf("runner: %s: %w", v.Name, err)
		}
		// Registered after the engine's close, so it runs before it: a sampler that
		// outlived the engine would read a closed database.
		defer func() { _ = samp.stop() }()
	}

	say.say("build %s: writing %d records in batches of %d", v.Name, plan.Stream.Records, plan.Spec.BatchSize)
	var timing Timing
	after := false
	postBatches := DefaultRules().PostRetentionBatches
	if background {
		if _, ok := opened.(retainWaiter); !ok {
			return nil, fmt.Errorf("runner: %s retains in the background and cannot be waited for", v.Name)
		}
	}
	sink := buildSink{ctx: ctx, e: e, t: &timing, afterRetention: &after, post: &postState{n: postBatches}, rest: opts.RestAfterRetention, prog: prog, gate: gate, background: background, prof: prof}
	if opts.Pace > 0 {
		sink.pace = &paceState{pacer: newPacer(opts.Pace), fromDay: paceFrom, streamAt: plan.Stream.Start}
	} else if gate != nil {
		// A build that is not paced is judged on its whole stream.
		gate.startJudged()
		prog.setJudged(true)
	}
	info, err := Drive(ctx, plan.Spec, sink)
	streamEnd := time.Now() // the end of the stream for the gate, whatever the pass of a background store still has to do
	if err != nil {
		return nil, fmt.Errorf("runner: %s: %w", v.Name, err)
	}
	if gate != nil {
		// The judged part ends with the stream: its stalls are counted to here, from a
		// counter the engine keeps (the statistics of the database are read only once the
		// pass is over).
		var stalls int64
		sc, ok := opened.(stallCounter)
		if ok {
			stalls = sc.WriteStalls()
		}
		gate.endStream(stalls, ok)
		prog.setJudged(false)
	}
	if sink.pace != nil && !sink.pace.on {
		return nil, fmt.Errorf("runner: %s: the stream ended before stream day %v, which the writer was to be paced from", v.Name, paceFrom)
	}
	if background {
		// Everything after this reads, sizes or compacts the database: the last pass is
		// over first, its settle included, and the windows of the gate have ended.
		prog.setPhase(phaseRetain)
		if err := opened.(retainWaiter).WaitRetained(ctx); err != nil {
			return nil, fmt.Errorf("runner: %s: waiting for the retention in the background: %w", v.Name, err)
		}
		// The last pass is over: no window is profiled from here on.
		if err := prof.close(); err != nil {
			return nil, fmt.Errorf("runner: %s: profiles: %w", v.Name, err)
		}
	}
	mode := ""
	if v.Root() {
		if mode = opts.RetentionMode; mode == "" {
			mode = candidates.RetentionSync
		}
	}
	var keep time.Duration
	for _, r := range plan.Spec.Retentions {
		keep = max(keep, r.Keep)
	}
	var trace *GateTrace
	if gate != nil {
		trace = gate.trace(streamEnd, mode, keep, maxima.maxima())
	}
	if err := sameAsPlan(plan.Stream, info); err != nil {
		return nil, fmt.Errorf("runner: %s wrote a different stream than planned: %w", v.Name, err)
	}
	totals := rec.Totals()
	if n := totals["checkpoint.errors"]; n > 0 {
		return nil, fmt.Errorf("runner: %s failed to write %d checkpoints (counted, not returned)", v.Name, n)
	}
	var tallies map[string][3]int64
	if pt, ok := opened.(pebblelog.PrefixTallier); ok {
		tallies = pt.PrefixTallies()
	}
	m := &Manifest{
		Candidate: v.Name, Layout: v.Layout, PlanDigest: planDigest, Stream: info,
		Build: g.Info, Untimed: g.Untimed, Counters: totals, StatsBuilt: e.Stats(), Timing: timing,
	}
	prog.setPhase(phaseUncompacted)
	say.say("build %s: reading what the stream left, before compacting", v.Name)
	if m.Uncompacted, m.UncompactedWrong, err = readUncompacted(e, rec, plan, v.Name); err != nil {
		return nil, err
	}

	prog.setPhase(phaseCompact)
	say.say("build %s: compacting everything", v.Name)
	if err := e.CompactAll(ctx); err != nil {
		return nil, fmt.Errorf("runner: %s: %w", v.Name, err)
	}
	if opts.CanonicalLayout {
		c, ok := e.(engine.Canonicalizer)
		if !ok {
			return nil, fmt.Errorf("runner: %s cannot rewrite its tables into the canonical layout", v.Name)
		}
		prog.setPhase(phaseCanonical)
		say.say("build %s: rewriting the tables in the canonical layout", v.Name)
		if err := c.Canonicalize(ctx); err != nil {
			return nil, fmt.Errorf("runner: %s: %w", v.Name, err)
		}
	}
	m.StatsCompacted = e.Stats()
	if m.Describe, err = e.Describe(); err != nil {
		return nil, err
	}
	m.Describe[RestKey] = strconv.FormatBool(opts.RestAfterRetention)
	m.Describe[CanonicalKey] = strconv.FormatBool(opts.CanonicalLayout)
	m.Describe[MetricsKey] = opts.MetricsEvery.String()
	m.Describe[PostRetentionKey] = strconv.Itoa(postBatches)
	// A negative argument reads the limit and leaves it as it is.
	m.Describe[GoMemoryLimitKey] = memoryLimitText(debug.SetMemoryLimit(-1))
	if processGC != "" {
		m.Describe[GoGCKey] = processGC
	}
	if mode != "" {
		m.Describe[RetentionModeKey] = mode
		m.Describe[RetainFlushDelsKey] = strconv.Itoa(RetainFlushDelsOf(opts.RetainFlushDels))
		m.Describe[PaceKey] = strconv.Itoa(opts.Pace)
		if opts.Pace > 0 {
			m.Describe[PaceFromDayKey] = strconv.FormatFloat(paceFrom, 'f', -1, 64)
		}
	}
	if opts.ProfileWindows {
		m.Describe[ProfileWindowsKey] = "true"
	}
	parts, err := e.Breakdown()
	if err != nil {
		return nil, err
	}
	m.Breakdown = map[string]int64{}
	for p, n := range parts {
		m.Breakdown[partKey(p)] = n
	}
	layers, err := e.SizeByLayer()
	if err != nil {
		return nil, err
	}
	m.SizeByLayer = map[string]int64{}
	for l, n := range layers {
		m.SizeByLayer[l.String()] = n
	}
	if m.Size, err = e.Size(); err != nil {
		return nil, err
	}
	prog.setPhase(phaseDone)
	if err := samp.failure(v.Name); err != nil {
		return nil, err
	}
	closed = true
	if err := e.Close(); err != nil {
		return nil, err
	}
	// The manifest is written only if a read will find what it records: the
	// database is opened as a read opens it and checked as a read checks it. A
	// compaction that started after the measurement, and that the close waited for,
	// would otherwise leave a manifest no read accepts.
	if err := checkAsRead(plan, v, dir, m); err != nil {
		return nil, fmt.Errorf("runner: %s changed after it was measured, and a read would refuse it: %s %w", v.Name, v.Name, err)
	}
	if trace != nil {
		trace.PeakRSS = peakRSS()
		m.Gate = trace
	}
	// The tallies go first: a directory that has a manifest is a finished build.
	if tallies != nil {
		if err := WriteTallies(filepath.Join(dir, TalliesFile), tallies); err != nil {
			return nil, err
		}
	}
	if err := writeJSON(filepath.Join(dir, ManifestFile), m); err != nil {
		return nil, err
	}
	say.say("build %s: %d table bytes, %d records", v.Name, m.StatsCompacted["live_table_bytes"], info.Records)
	return m, nil
}

// WriteTallies writes the per-prefix counts of a build to path: {"<prefix hex>":
// [invalidated, replaced, capped], ...}, only the prefixes with a count above zero,
// in key order.
func WriteTallies(path string, tallies map[string][3]int64) error {
	hexed := make(map[string][3]int64, len(tallies))
	for k, t := range tallies {
		if t != ([3]int64{}) {
			hexed[hex.EncodeToString([]byte(k))] = t
		}
	}
	b, err := json.Marshal(hexed) // a map's keys are sorted
	if err != nil {
		return err
	}
	return writeFileAtomic(path, append(b, '\n'))
}

// LoadTallies reads what [WriteTallies] wrote, by prefix (the key bytes). A build
// without a tallies file gives ok false.
func LoadTallies(dir string) (tallies map[string][3]int64, ok bool, err error) {
	b, err := os.ReadFile(filepath.Join(dir, TalliesFile))
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	var hexed map[string][3]int64
	if err := json.Unmarshal(b, &hexed); err != nil {
		return nil, false, fmt.Errorf("runner: %s: %w", filepath.Join(dir, TalliesFile), err)
	}
	tallies = make(map[string][3]int64, len(hexed))
	for k, t := range hexed {
		raw, err := hex.DecodeString(k)
		if err != nil {
			return nil, false, fmt.Errorf("runner: %s: %q is not a prefix in hex: %w", filepath.Join(dir, TalliesFile), k, err)
		}
		tallies[string(raw)] = t
	}
	return tallies, true, nil
}

// checkPace refuses a pacing that cannot be judged, before anything is written, and
// returns the stream day the build is paced from (0 for a build that is not paced).
func checkPace(plan *Plan, v candidates.Variant, opts BuildOptions) (float64, error) {
	switch {
	case opts.Pace < 0:
		return 0, fmt.Errorf("runner: pace %d: a rate in records per second, or 0 for none", opts.Pace)
	case opts.Pace == 0:
		return 0, nil
	case !v.Root():
		return 0, fmt.Errorf("runner: %s is not built through the root store: only a build in the background mode of the root store is paced", v.Name)
	case opts.RetentionMode != candidates.RetentionBackground:
		return 0, fmt.Errorf("runner: pace %d with the retention mode %q: a build whose retention is synchronous is not paced", opts.Pace, opts.RetentionMode)
	}
	days, err := windowOf(plan.Spec)
	if err != nil {
		return 0, fmt.Errorf("runner: pace %d: %w", opts.Pace, err)
	}
	return PaceFromDay(days), nil
}

// windowOf is the days of retained history a spec is the window of: its retentions all
// keep that many whole days, and the stream is 2W + 1.5 days long (see [WindowSpec]).
func windowOf(s Spec) (int, error) {
	if len(s.Retentions) == 0 {
		return 0, errors.New("the plan has no retention, so it is not a window plan (a stream of 2W + 1.5 days with a daily retention that keeps W days)")
	}
	keep := s.Retentions[0].Keep
	for _, r := range s.Retentions {
		if r.Keep != keep {
			return 0, errors.New("the plan's retentions do not all keep the same time, so it is not a window plan")
		}
	}
	if keep <= 0 || keep%(24*time.Hour) != 0 {
		return 0, fmt.Errorf("the plan's retentions keep %s, which is not a whole number of days: it is not a window plan", keep)
	}
	if s.Workload.Duration != 2*keep+36*time.Hour {
		return 0, fmt.Errorf("the plan is %s long and its retentions keep %s: it is not a window plan, a stream of 2W + 1.5 days that keeps W", s.Workload.Duration, keep)
	}
	return int(keep / (24 * time.Hour)), nil
}

// checkAsRead opens the database a build has closed read-only, as [Read] does, and
// returns the first way it differs from what the manifest records.
func checkAsRead(plan *Plan, v candidates.Variant, dir string, m *Manifest) error {
	e, err := openReadOnly(plan, v, dir, NewCapture(), m.Describe[SyncKey] == "true")
	if err != nil {
		return err
	}
	desc, err := e.Describe()
	if err == nil {
		err = sameAsBuilt(e, desc, m, plan)
	}
	if cerr := e.Close(); err == nil {
		err = cerr
	}
	return err
}

// sameAsPlan returns the first way a stream differs from the planned one.
func sameAsPlan(want, got StreamInfo) error {
	switch {
	case want.Digest != got.Digest:
		return fmt.Errorf("stream digest %s, planned %s: %w", got.Digest, want.Digest, errMismatch)
	case want.Records != got.Records:
		return fmt.Errorf("%d records written, planned %d: %w", got.Records, want.Records, errMismatch)
	case want.Dropped != got.Dropped:
		return fmt.Errorf("%d records dropped before the horizon, planned %d: %w", got.Dropped, want.Dropped, errMismatch)
	case want.LastSeq != got.LastSeq:
		return fmt.Errorf("the stream ends at seq %d, planned %d: %w", got.LastSeq, want.LastSeq, errMismatch)
	case want.OldToken != got.OldToken:
		return fmt.Errorf("the old snapshot is at seq %d, planned %d: %w", got.OldToken, want.OldToken, errMismatch)
	case len(want.Retentions) != len(got.Retentions):
		return fmt.Errorf("%d retentions, planned %d: %w", len(got.Retentions), len(want.Retentions), errMismatch)
	}
	return nil
}

func writeJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", " ")
	if err != nil {
		return err
	}
	return writeFileAtomic(path, append(b, '\n'))
}

// writeFileAtomic writes the file whole or not at all: a step that is killed
// while writing leaves the old file, or none, and not a truncated one.
func writeFileAtomic(path string, b []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// LoadManifest reads the manifest of the candidate built under dir.
func LoadManifest(dir string) (*Manifest, error) {
	m, _, err := readManifest(dir)
	return m, err
}

// readManifest loads a candidate's manifest and the digest of the file it came
// from.
func readManifest(dir string) (*Manifest, string, error) {
	b, err := os.ReadFile(filepath.Join(dir, ManifestFile))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, "", fmt.Errorf("runner: %s has no manifest: it was not built, or the build did not finish", dir)
		}
		return nil, "", err
	}
	var m Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, "", fmt.Errorf("runner: %s: %w", filepath.Join(dir, ManifestFile), err)
	}
	sum := sha256.Sum256(b)
	return &m, hex.EncodeToString(sum[:]), nil
}

// readUncompacted asks the queries of "now" of the plan once, at the end of the
// build and before anything is compacted, and records what each cost and which
// answers are not the plan's: a layout that answers wrongly only from the tables as
// the stream left them is not one a read after the compaction would find.
func readUncompacted(e measurable, rec *Capture, plan *Plan, name string) (reads []UncompactedRead, wrong []string, err error) {
	rec.Take()
	for _, q := range plan.Queries {
		if q.Age != AgeNow {
			continue
		}
		a, err := Ask(e, q)
		if err != nil {
			return nil, nil, fmt.Errorf("runner: %s: %s before compaction: %w", name, q.Name(), err)
		}
		reads = append(reads, UncompactedRead{Query: q.Name(), Counters: rec.Take()})
		if a.Digest != q.Expect {
			wrong = append(wrong, q.Name())
		}
	}
	return reads, wrong, nil
}

// memoryLimitText is how a manifest records the Go memory limit: the bytes in
// decimal, or "none" for no limit (math.MaxInt64, the runtime's value for none).
func memoryLimitText(n int64) string {
	if n == math.MaxInt64 || n <= 0 {
		return "none"
	}
	return strconv.FormatInt(n, 10)
}
