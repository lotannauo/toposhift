package runner

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/candidates"
)

// The profiles of a build with [BuildOptions.ProfileWindows] are written under the
// candidate's directory, in ProfilesDir:
//
//   - pass<N>_first.cpu.pprof, a CPU profile from the publication of pass N to
//     profileFirstSpan after the profile started, or to the end of the rewrite if that
//     comes first;
//   - pass<N>_window.cpu.pprof, a CPU profile from the end of the first one to the end
//     of the rewrite. Only one CPU profile runs at a time in a process, so the first is
//     stopped and this one started as soon as Go has finished writing the first, which
//     takes up to its profile writer's 100 ms poll and the encoding: the samples stop
//     at first_cpu_end_ns and start again at window_cpu_start_ns of the index, and that
//     gap (about 0.1 to 0.3 s) is in neither profile. Given together to pprof
//     (`go tool pprof -top first window`), the two cover the window from the
//     publication to the end of the rewrite but for that gap. A pass whose rewrite ends
//     within the first span has no window profile;
//   - pass<N>_start.mutex.pprof and pass<N>_window.mutex.pprof, the process's mutex
//     profile at the publication and at the end of the rewrite. Each is the cumulative
//     profile since the build set the mutex profile fraction, not a delta: the
//     contention within the window is the second less the first, which is what
//     `go tool pprof -base pass<N>_start.mutex.pprof pass<N>_window.mutex.pprof` shows;
//   - ProfileIndexFile, what was written for each pass, when, and why a pass has no
//     profile (see [ProfileIndex]).
//
// N counts the publications the store reported, from 1, so pass N is the window
// Gate.Windows[N-1] of the manifest; its retention field is the index into the manifest's
// Timing.PostRetention and Timing.RetainPhases of the call that published it.
//
// The profiles are written so that neither the writer nor the retainer waits for a
// CPU profile to stop: the store's observer calls that end a window only hand the
// window to a goroutine of the profiler, which stops the profile and writes the files.
// The call that publishes a pass writes the first mutex profile and starts the CPU
// profile itself, before it returns (open_cost_ns of the index, about a millisecond).
const (
	ProfilesDir      = "profiles"
	ProfileIndexFile = "index.json"
	// ProfileWindowsKey is the key of a manifest's Describe that says the build wrote
	// the profiles of its windows ("true"); a build that did not has no such key.
	ProfileWindowsKey = "profile_windows"
)

const (
	// profileFirstSpan is how long the first CPU profile of a pass runs, from its start
	// just after the publication.
	profileFirstSpan = 2 * time.Second
	// profileMutexFraction is the mutex profile fraction a build that profiles its windows
	// sets for its whole run: on average 1 in that many contention events is recorded,
	// and pprof scales the counts and delays it writes by it.
	profileMutexFraction = 5
)

// ProfileIndex is what [ProfileIndexFile] holds. Instants ending in _ns are since the
// epoch of the gate, as the manifest's Gate.Windows are, and -1 for one that did not
// happen.
type ProfileIndex struct {
	FirstSpanNs   int64         `json:"first_span_ns"`
	MutexFraction int           `json:"mutex_fraction"`
	Passes        []ProfilePass `json:"passes"`
}

// ProfilePass is what was profiled of one pass.
type ProfilePass struct {
	// Pass is N, from 1; Gen is the store's generation of the pass; Retention is the
	// index of the retention call that published it among the build's retentions.
	Pass      int    `json:"pass"`
	Gen       uint64 `json:"gen"`
	Retention int    `json:"retention"`
	// Judged says the pass was published at or after the start of the pacing, or in a
	// build that is not paced: the windows the store gate judges.
	Judged bool `json:"judged"`
	// PublishedNs, RewriteEndNs and SettleEndNs are the store's instants of the pass,
	// the earliest it gave; PublishedAt is the first as a wall-clock time.
	PublishedNs  int64  `json:"published_ns"`
	PublishedAt  string `json:"published_at"`
	RewriteEndNs int64  `json:"rewrite_end_ns"`
	SettleEndNs  int64  `json:"settle_end_ns"`
	// The instants the CPU profiles started and stopped sampling, as read by the
	// profiler.
	FirstCPUStartNs  int64 `json:"first_cpu_start_ns"`
	FirstCPUEndNs    int64 `json:"first_cpu_end_ns"`
	WindowCPUStartNs int64 `json:"window_cpu_start_ns"`
	WindowCPUEndNs   int64 `json:"window_cpu_end_ns"`
	// OpenCostNs is how long the call that published the pass took to write the first
	// mutex profile and start the CPU profile, a time the writer's retention call
	// includes; CloseCostNs how long the profiler's own goroutine took to write the
	// second mutex profile and stop the CPU profile.
	OpenCostNs  int64 `json:"open_cost_ns"`
	CloseCostNs int64 `json:"close_cost_ns"`
	// The writer's batches the profiler was told of while the window was open: those
	// in the first span, and all of them; each with the count, the sum and the longest
	// of their commit times (the times the build already takes of every batch).
	FirstBatches    int64 `json:"first_batches"`
	FirstBatchNs    int64 `json:"first_batch_ns"`
	FirstMaxBatchNs int64 `json:"first_max_batch_ns"`
	Batches         int64 `json:"batches"`
	BatchNs         int64 `json:"batch_ns"`
	MaxBatchNs      int64 `json:"max_batch_ns"`
	// Files are the profiles written for the pass, by name under ProfilesDir, in the
	// order they were written.
	Files []string `json:"files"`
	// Skipped says why the pass has no profile; Notes what else to know of its
	// profiles; Errors what failed to be written.
	Skipped string   `json:"skipped,omitempty"`
	Notes   []string `json:"notes,omitempty"`
	Errors  []string `json:"errors,omitempty"`
}

// cpuProfiler starts and stops the process's CPU profile; a test gives a fake one.
type cpuProfiler interface {
	start(w io.Writer) error
	stop()
}

type runtimeCPU struct{}

func (runtimeCPU) start(w io.Writer) error { return pprof.StartCPUProfile(w) }
func (runtimeCPU) stop()                   { pprof.StopCPUProfile() }

// writeMutexProfile writes the process's cumulative mutex profile.
func writeMutexProfile(w io.Writer) error { return pprof.Lookup("mutex").WriteTo(w, 0) }

// afterFunc is time.AfterFunc, with what stops it.
func afterFunc(d time.Duration, f func()) (stop func() bool) { return time.AfterFunc(d, f).Stop }

// The CPU profile a window runs: its first, its window profile, or none (the window
// profile did not start, or the window has ended).
const (
	cpuFirst = iota
	cpuWindow
	cpuNone
)

// profileWindow is the pass whose profiles are being written.
type profileWindow struct {
	i   int // its index in the index's passes
	gen uint64

	// mu is held through every operation on the window's profiles, by the call that
	// opens it, its timer and the goroutine that closes it, so that they come one after
	// the other; only one window is active at a time, so the CPU profile is started and
	// stopped by one of them at a time.
	mu        sync.Mutex
	cpu       int
	file      *os.File // the CPU profile running, nil for none
	stopTimer func() bool
	ended     bool
}

// windowProfiler writes the profiles of the windows of a build in the background (see
// [BuildOptions.ProfileWindows]). Only one window is active at a time, from its
// publication until its files are written: a pass published while one is active is
// skipped, and the index says so. Every method accepts a nil receiver and then does
// nothing, which is what a build without the option has. It is safe to call from the
// writer, the retainer and its own goroutines.
type windowProfiler struct {
	dir   string
	epoch time.Time
	paced bool
	now   func() time.Time
	after func(d time.Duration, f func()) (stop func() bool)
	cpu   cpuProfiler
	mutex func(w io.Writer) error
	// restore puts the mutex profile fraction back as it was before the build.
	restore func()

	// mu guards what follows, and is held only for moments: the writer takes it for each
	// batch.
	mu sync.Mutex
	// index is what the index file is written from.
	index ProfileIndex
	// open is the window whose batches are counted, from its publication to the end of
	// its rewrite; active is the window from its publication until its files are written.
	open, active *profileWindow
	// first says the open window is in its first span, for the batches.
	first     bool
	retention int
	paceStart time.Time
	// early holds the end of a rewrite the store told before the publication of its
	// generation reached the profiler (the calls come from different goroutines).
	early  map[uint64]time.Time
	closed bool
	err    error // the first failure to write the index

	// closing counts the goroutines that close windows; indexMu orders the writes of the
	// index.
	closing sync.WaitGroup
	indexMu sync.Mutex
}

// newWindowProfiler makes the profiles directory under the candidate's directory dir
// and sets the process's mutex profile fraction for the rest of the build. epoch is
// the gate's.
func newWindowProfiler(dir string, epoch time.Time, paced bool) (*windowProfiler, error) {
	p := &windowProfiler{dir: filepath.Join(dir, ProfilesDir), epoch: epoch, paced: paced, now: time.Now, after: afterFunc, cpu: runtimeCPU{}, mutex: writeMutexProfile}
	if err := p.begin(); err != nil {
		return nil, err
	}
	prev := runtime.SetMutexProfileFraction(profileMutexFraction)
	p.restore = func() { runtime.SetMutexProfileFraction(prev) }
	return p, nil
}

// begin makes the directory and writes an index with no pass.
func (p *windowProfiler) begin() error {
	if err := os.MkdirAll(p.dir, 0o755); err != nil {
		return err
	}
	p.index = ProfileIndex{FirstSpanNs: int64(profileFirstSpan), MutexFraction: profileMutexFraction, Passes: []ProfilePass{}}
	p.keepIndex()
	return p.err
}

// checkProfileWindows refuses the option for a build that has no windows from the
// store's observer: one not through the root store, or not in the background mode.
func checkProfileWindows(v candidates.Variant, opts BuildOptions) error {
	switch {
	case !opts.ProfileWindows:
		return nil
	case !v.Root():
		return fmt.Errorf("runner: %s is not built through the root store: only a build in the background mode of the root store profiles its windows", v.Name)
	case opts.RetentionMode != candidates.RetentionBackground:
		return fmt.Errorf("runner: profiling the windows with the retention mode %q: only a build in the background mode has windows to profile", opts.RetentionMode)
	}
	return nil
}

func (p *windowProfiler) off(t time.Time) int64 { return t.Sub(p.epoch).Nanoseconds() }

// pass is the entry of window w. The caller holds p.mu.
func (p *windowProfiler) pass(w *profileWindow) *ProfilePass { return &p.index.Passes[w.i] }

// retaining says the retention call with index i (among the build's) is about to be
// made: a pass it publishes is that call's.
func (p *windowProfiler) retaining(i int) {
	if p == nil {
		return
	}
	p.mu.Lock()
	p.retention = i
	p.mu.Unlock()
}

// startPacing records the start of the first paced batch, once.
func (p *windowProfiler) startPacing(at time.Time) {
	if p == nil {
		return
	}
	p.mu.Lock()
	if p.paceStart.IsZero() {
		p.paceStart = at
	}
	p.mu.Unlock()
}

// batch is a batch of the writer that took d to commit: it is counted to the open
// window, if there is one.
func (p *windowProfiler) batch(d time.Duration) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.open == nil {
		return
	}
	ps := p.pass(p.open)
	ns := d.Nanoseconds()
	if p.first {
		ps.FirstBatches++
		ps.FirstBatchNs += ns
		ps.FirstMaxBatchNs = max(ps.FirstMaxBatchNs, ns)
	}
	ps.Batches++
	ps.BatchNs += ns
	ps.MaxBatchNs = max(ps.MaxBatchNs, ns)
}

// published opens the window of generation gen, published at the store's instant at:
// it writes the first mutex profile and starts the first CPU profile, and the timer
// that ends it.
func (p *windowProfiler) published(gen uint64, at time.Time) {
	if p == nil {
		return
	}
	p.mu.Lock()
	judged := !p.paced || (!p.paceStart.IsZero() && !at.Before(p.paceStart))
	p.index.Passes = append(p.index.Passes, ProfilePass{
		Pass: len(p.index.Passes) + 1, Gen: gen, Retention: p.retention, Judged: judged,
		PublishedNs: p.off(at), PublishedAt: at.UTC().Format(time.RFC3339Nano),
		RewriteEndNs: -1, SettleEndNs: -1, FirstCPUStartNs: -1, FirstCPUEndNs: -1, WindowCPUStartNs: -1, WindowCPUEndNs: -1,
		Files: []string{},
	})
	i := len(p.index.Passes) - 1
	ps := &p.index.Passes[i]
	if end, ok := p.early[gen]; ok {
		delete(p.early, gen)
		ps.RewriteEndNs = p.off(end)
	}
	switch {
	case p.closed:
		ps.Skipped = "published after the profiling ended"
	case ps.RewriteEndNs != -1:
		ps.Skipped = "the store told the end of the rewrite before the publication reached the profiler: the window was over"
	case p.open != nil:
		ps.Skipped = fmt.Sprintf("published while the window of pass %d was still open: one CPU profile runs at a time", p.pass(p.open).Pass)
	case p.active != nil:
		ps.Skipped = fmt.Sprintf("published while the profiles of pass %d were still being written: one CPU profile runs at a time", p.pass(p.active).Pass)
	}
	if ps.Skipped != "" {
		p.mu.Unlock()
		p.keepIndex()
		return
	}
	w := &profileWindow{i: i, gen: gen, cpu: cpuNone}
	w.mu.Lock() // before the window is seen: its close waits for its start
	defer w.mu.Unlock()
	p.open, p.active, p.first = w, w, true
	p.mu.Unlock()

	t0 := p.now()
	p.writeMutex(w, "start")
	f, name, err := p.startCPU(w, "first")
	started := p.now()
	p.mu.Lock()
	defer p.mu.Unlock()
	ps = p.pass(w)
	ps.OpenCostNs = started.Sub(t0).Nanoseconds()
	if err != nil {
		// No profile runs: the window ends here, and the next publication is profiled.
		ps.Skipped = "the CPU profile did not start: " + err.Error()
		w.ended = true
		if p.open == w {
			p.open = nil
		}
		p.active = nil
		return
	}
	ps.Files = append(ps.Files, name)
	ps.FirstCPUStartNs = p.off(started)
	w.cpu, w.file = cpuFirst, f
	w.stopTimer = p.after(profileFirstSpan, func() { p.firstSpanEnded(w) })
}

// firstSpanEnded stops the first CPU profile of w and starts its window profile, if w
// is still in its first span.
func (p *windowProfiler) firstSpanEnded(w *profileWindow) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.ended || w.cpu != cpuFirst {
		return // the window closed first
	}
	stopped := p.stopCPU(w)
	p.mu.Lock()
	p.pass(w).FirstCPUEndNs = p.off(stopped)
	if p.open == w {
		p.first = false
	}
	p.mu.Unlock()
	f, name, err := p.startCPU(w, "window")
	started := p.now()
	p.mu.Lock()
	defer p.mu.Unlock()
	ps := p.pass(w)
	if err != nil {
		ps.Errors = append(ps.Errors, "the window CPU profile did not start: "+err.Error())
		return
	}
	ps.Files = append(ps.Files, name)
	ps.WindowCPUStartNs = p.off(started)
	w.cpu, w.file = cpuWindow, f
}

// rewriteEnded is the store's end of the rewrite of generation gen: it ends the window
// if it is gen's, and hands it to a goroutine that writes its last profiles. A call can
// arrive after a later publication's, and then ends a window the profiler had kept open
// meanwhile; or before its own publication's, and then that publication opens no
// window.
func (p *windowProfiler) rewriteEnded(gen uint64, at time.Time) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	switch ps := p.passOf(gen); {
	case ps == nil:
		if p.early == nil {
			p.early = map[uint64]time.Time{}
		}
		if end, ok := p.early[gen]; !ok || at.Before(end) {
			p.early[gen] = at
		}
		return
	case ps.RewriteEndNs == -1 || p.off(at) < ps.RewriteEndNs:
		ps.RewriteEndNs = p.off(at)
	}
	if w := p.open; w != nil && w.gen == gen {
		p.open = nil
		p.closing.Add(1)
		go func() {
			defer p.closing.Done()
			p.closeWindow(w, "")
		}()
	}
}

// settleEnded records the end of the settle of generation gen.
func (p *windowProfiler) settleEnded(gen uint64, at time.Time) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if ps := p.passOf(gen); ps != nil && (ps.SettleEndNs == -1 || p.off(at) < ps.SettleEndNs) {
		ps.SettleEndNs = p.off(at)
	}
}

// passOf is the last pass of generation gen, or nil. The caller holds p.mu.
func (p *windowProfiler) passOf(gen uint64) *ProfilePass {
	for i := len(p.index.Passes) - 1; i >= 0; i-- {
		if p.index.Passes[i].Gen == gen {
			return &p.index.Passes[i]
		}
	}
	return nil
}

// wait waits for the windows being closed to have their files written.
func (p *windowProfiler) wait() { p.closing.Wait() }

// close ends the profiling: it waits for the windows being closed, closes a window
// still open with a note, puts the mutex profile fraction back, and writes the index.
// It returns the first failure to write the index. A second call does nothing more.
func (p *windowProfiler) close() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	if p.closed {
		defer p.mu.Unlock()
		return p.err
	}
	p.closed = true
	w := p.open
	p.open = nil
	p.mu.Unlock()
	p.wait()
	if w != nil {
		p.closeWindow(w, "the profiling ended before the store told the end of this rewrite: the profiles end there")
	}
	if p.restore != nil {
		p.restore()
	}
	p.keepIndex()
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}

// closeWindow writes w's second mutex profile and stops its CPU profile, and ends it.
func (p *windowProfiler) closeWindow(w *profileWindow, note string) {
	w.mu.Lock()
	if w.ended { // its CPU profile did not start, and its publication ended it
		w.mu.Unlock()
		return
	}
	t0 := p.now()
	p.writeMutex(w, "window")
	cpu := w.cpu
	var stopped time.Time
	if cpu == cpuFirst {
		w.stopTimer()
	}
	if cpu != cpuNone {
		stopped = p.stopCPU(w)
	}
	w.ended = true
	done := p.now()
	w.mu.Unlock()

	p.mu.Lock()
	ps := p.pass(w)
	switch cpu {
	case cpuFirst:
		ps.FirstCPUEndNs = p.off(stopped)
		ps.Notes = append(ps.Notes, "the rewrite ended within the first span: the first CPU profile covers the whole window, and there is no window CPU profile")
	case cpuWindow:
		ps.WindowCPUEndNs = p.off(stopped)
	}
	if note != "" {
		ps.Notes = append(ps.Notes, note)
	}
	ps.CloseCostNs = done.Sub(t0).Nanoseconds()
	if p.active == w {
		p.active = nil
	}
	p.mu.Unlock()
	p.keepIndex()
}

// startCPU creates w's CPU profile of the kind ("first" or "window") and starts it.
// The caller holds w.mu.
func (p *windowProfiler) startCPU(w *profileWindow, kind string) (*os.File, string, error) {
	name := p.fileName(w, kind+".cpu")
	f, err := os.Create(filepath.Join(p.dir, name))
	if err != nil {
		return nil, "", err
	}
	if err := p.cpu.start(f); err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		return nil, "", err
	}
	return f, name, nil
}

// stopCPU stops the CPU profile w runs and closes its file. It returns the instant it
// was called: the profile stops sampling then, and the call returns once the profile
// is written. The caller holds w.mu.
func (p *windowProfiler) stopCPU(w *profileWindow) time.Time {
	stopped := p.now()
	p.cpu.stop()
	err := w.file.Close()
	w.file, w.cpu = nil, cpuNone
	if err != nil {
		p.mu.Lock()
		ps := p.pass(w)
		ps.Errors = append(ps.Errors, err.Error())
		p.mu.Unlock()
	}
	return stopped
}

// writeMutex writes w's mutex profile of the kind ("start" or "window"). The caller
// holds w.mu.
func (p *windowProfiler) writeMutex(w *profileWindow, kind string) {
	name := p.fileName(w, kind+".mutex")
	f, err := os.Create(filepath.Join(p.dir, name))
	if err == nil {
		err = p.mutex(f)
		if cerr := f.Close(); err == nil {
			err = cerr
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	ps := p.pass(w)
	if err != nil {
		ps.Errors = append(ps.Errors, fmt.Sprintf("%s: %v", name, err))
		return
	}
	ps.Files = append(ps.Files, name)
}

// fileName is the name of w's profile of the kind ("first.cpu", "start.mutex"...).
func (p *windowProfiler) fileName(w *profileWindow, kind string) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return fmt.Sprintf("pass%d_%s.pprof", p.pass(w).Pass, kind)
}

// keepIndex writes the index, and keeps the first failure. The caller does not hold
// p.mu.
func (p *windowProfiler) keepIndex() {
	p.indexMu.Lock()
	defer p.indexMu.Unlock()
	p.mu.Lock()
	b, err := json.MarshalIndent(p.index, "", " ")
	p.mu.Unlock()
	if err == nil {
		err = writeFileAtomic(filepath.Join(p.dir, ProfileIndexFile), append(b, '\n'))
	}
	if err != nil {
		p.mu.Lock()
		if p.err == nil {
			p.err = err
		}
		p.mu.Unlock()
	}
}

// LoadProfileIndex reads the index of the profiles of the candidate built under dir.
func LoadProfileIndex(dir string) (*ProfileIndex, error) {
	b, err := os.ReadFile(filepath.Join(dir, ProfilesDir, ProfileIndexFile))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("runner: %s has no profiles: it was not built with them", dir)
		}
		return nil, err
	}
	var idx ProfileIndex
	if err := json.Unmarshal(b, &idx); err != nil {
		return nil, fmt.Errorf("runner: %s: %w", filepath.Join(dir, ProfilesDir, ProfileIndexFile), err)
	}
	return &idx, nil
}

// profileObserver is the store's observer of a build that profiles its windows: it
// tells the gate first, as without the profiles, and then the profiler.
type profileObserver struct {
	gate gateObserver
	p    *windowProfiler
}

func (o profileObserver) Published(gen uint64, at time.Time) {
	o.gate.Published(gen, at)
	o.p.published(gen, at)
}

func (o profileObserver) RewriteEnded(gen uint64, at time.Time) {
	o.gate.RewriteEnded(gen, at)
	o.p.rewriteEnded(gen, at)
}

func (o profileObserver) SettleEnded(gen uint64, at time.Time) {
	o.gate.SettleEnded(gen, at)
	o.p.settleEnded(gen, at)
}
