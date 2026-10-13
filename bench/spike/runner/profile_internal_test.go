package runner

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeCPU is a CPU profiler that refuses a second profile while one runs, as the
// runtime's does, and writes "cpu <n>" for the nth profile when it stops.
type fakeCPU struct {
	running io.Writer
	starts  int
	fail    error
}

func (c *fakeCPU) start(w io.Writer) error {
	switch {
	case c.fail != nil:
		return c.fail
	case c.running != nil:
		return errors.New("cpu profiling already in use")
	}
	c.starts++
	c.running = w
	return nil
}

func (c *fakeCPU) stop() {
	if c.running != nil {
		fmt.Fprintf(c.running, "cpu %d", c.starts)
		c.running = nil
	}
}

// fakeTimers holds the timers a profiler set, which a test fires.
type fakeTimers struct {
	set []*fakeTimer
}

type fakeTimer struct {
	d       time.Duration
	f       func()
	stopped bool
}

func (ts *fakeTimers) after(d time.Duration, f func()) func() bool {
	t := &fakeTimer{d: d, f: f}
	ts.set = append(ts.set, t)
	return func() bool {
		was := !t.stopped
		t.stopped = true
		return was
	}
}

// fire runs the last timer's function, stopped or not: a timer whose function has
// started when it is stopped runs it anyway.
func (ts *fakeTimers) fire(t *testing.T) {
	t.Helper()
	if len(ts.set) == 0 {
		t.Fatal("no timer was set")
	}
	ts.set[len(ts.set)-1].f()
}

type profilerFixture struct {
	p        *windowProfiler
	cpu      *fakeCPU
	timers   *fakeTimers
	mu       sync.Mutex // the clock is read by the goroutine that closes a window
	clock    time.Time
	mutexes  int
	restored int
	// hold, if not nil, makes the next mutex profile wait for it to be closed.
	hold chan struct{}
}

// newProfilerFixture is a profiler under a temporary directory with a fake clock, CPU
// profiler, timers and mutex profile; its epoch is at(0).
func newProfilerFixture(t *testing.T, paced bool) *profilerFixture {
	t.Helper()
	fx := &profilerFixture{cpu: &fakeCPU{}, timers: &fakeTimers{}, clock: at(0)}
	fx.p = &windowProfiler{
		dir: filepath.Join(t.TempDir(), ProfilesDir), epoch: at(0), paced: paced,
		now: func() time.Time {
			fx.mu.Lock()
			defer fx.mu.Unlock()
			return fx.clock
		},
		after: fx.timers.after, cpu: fx.cpu,
		mutex: func(w io.Writer) error {
			if fx.hold != nil {
				<-fx.hold
			}
			fx.mutexes++
			_, err := fmt.Fprintf(w, "mutex %d", fx.mutexes)
			return err
		},
		restore: func() { fx.restored++ },
	}
	if err := fx.p.begin(); err != nil {
		t.Fatal(err)
	}
	return fx
}

func (fx *profilerFixture) set(ns int64) {
	fx.mu.Lock()
	fx.clock = at(ns)
	fx.mu.Unlock()
}

// ended tells the profiler the end of gen's rewrite, and waits for the window's files.
func (fx *profilerFixture) ended(gen uint64, ns int64) {
	fx.p.rewriteEnded(gen, at(ns))
	fx.p.wait()
}

// files are the files of the profiles directory, by name, with their contents.
func (fx *profilerFixture) files(t *testing.T) map[string]string {
	t.Helper()
	entries, err := os.ReadDir(fx.p.dir)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, e := range entries {
		if e.Name() == ProfileIndexFile {
			continue
		}
		b, err := os.ReadFile(filepath.Join(fx.p.dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		out[e.Name()] = string(b)
	}
	return out
}

// index is the index as written to its file.
func (fx *profilerFixture) index(t *testing.T) ProfileIndex {
	t.Helper()
	idx, err := LoadProfileIndex(filepath.Dir(fx.p.dir))
	if err != nil {
		t.Fatal(err)
	}
	return *idx
}

func sameFiles(t *testing.T, got map[string]string, want map[string]string) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("files %v, want %v", got, want)
		return
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("files %v, want %v", got, want)
			return
		}
	}
}

// A pass is profiled from its publication: the mutex profile then, the first CPU
// profile until its timer of two seconds fires, then the window CPU profile until the
// store tells the end of the rewrite, and the mutex profile then. One CPU profile runs
// at a time, the batches are counted to the span they came in, and the index records
// every instant, the files in the order they were written and the end of the settle.
func TestAPassIsProfiledFromItsPublicationToTheEndOfItsRewrite(t *testing.T) {
	t.Parallel()

	fx := newProfilerFixture(t, false)
	obs := profileObserver{gateObserver{nil}, fx.p}
	fx.p.retaining(3)
	fx.set(1_000_010)
	obs.Published(7, at(1_000_000))
	if len(fx.timers.set) != 1 || fx.timers.set[0].d != profileFirstSpan || fx.cpu.running == nil {
		t.Fatalf("timers %+v, CPU profile running %v", fx.timers.set, fx.cpu.running != nil)
	}
	fx.p.batch(10 * time.Millisecond)
	fx.p.batch(30 * time.Millisecond)
	fx.set(2_001_000_000)
	fx.timers.fire(t)
	if fx.cpu.starts != 2 || fx.cpu.running == nil {
		t.Fatalf("%d CPU profiles started, one running %v", fx.cpu.starts, fx.cpu.running != nil)
	}
	fx.p.batch(400 * time.Millisecond)
	fx.set(9_000_000_100)
	obs.RewriteEnded(7, at(9_000_000_000))
	fx.p.wait()
	if fx.cpu.running != nil {
		t.Fatal("a CPU profile runs after the end of the rewrite")
	}
	obs.SettleEnded(7, at(12_000_000_000))
	fx.p.batch(time.Second) // after the window: not counted
	// The end of the settle is in the index once the profiling has ended.
	if err := fx.p.close(); err != nil || fx.restored != 1 {
		t.Errorf("close: %v, the mutex fraction restored %d times", err, fx.restored)
	}
	sameFiles(t, fx.files(t), map[string]string{
		"pass1_start.mutex.pprof": "mutex 1", "pass1_first.cpu.pprof": "cpu 1",
		"pass1_window.cpu.pprof": "cpu 2", "pass1_window.mutex.pprof": "mutex 2",
	})
	idx := fx.index(t)
	if idx.FirstSpanNs != int64(2*time.Second) || idx.MutexFraction != profileMutexFraction || len(idx.Passes) != 1 {
		t.Fatalf("index %+v", idx)
	}
	ps := idx.Passes[0]
	want := ProfilePass{
		Pass: 1, Gen: 7, Retention: 3, Judged: true, PublishedNs: 1_000_000, PublishedAt: at(1_000_000).UTC().Format(time.RFC3339Nano),
		RewriteEndNs: 9_000_000_000, SettleEndNs: 12_000_000_000,
		FirstCPUStartNs: 1_000_010, FirstCPUEndNs: 2_001_000_000, WindowCPUStartNs: 2_001_000_000, WindowCPUEndNs: 9_000_000_100,
		FirstBatches: 2, FirstBatchNs: int64(40 * time.Millisecond), FirstMaxBatchNs: int64(30 * time.Millisecond),
		Batches: 3, BatchNs: int64(440 * time.Millisecond), MaxBatchNs: int64(400 * time.Millisecond),
		Files: []string{"pass1_start.mutex.pprof", "pass1_first.cpu.pprof", "pass1_window.cpu.pprof", "pass1_window.mutex.pprof"},
	}
	if !reflect.DeepEqual(ps, want) {
		t.Errorf("pass\n%+v, want\n%+v", ps, want)
	}
}

// A rewrite that ends within the first span ends the first CPU profile and stops its
// timer: no window CPU profile is written, the index says why, and a timer whose
// function runs after the window closed starts nothing.
func TestARewriteWithinTheFirstSpanHasOnlyTheFirstProfile(t *testing.T) {
	t.Parallel()

	fx := newProfilerFixture(t, false)
	fx.p.published(1, at(100))
	fx.set(500_000_000)
	fx.ended(1, 500_000_000)
	if !fx.timers.set[0].stopped {
		t.Error("the timer of the first span was not stopped")
	}
	fx.timers.fire(t)
	if fx.cpu.starts != 1 || fx.cpu.running != nil {
		t.Errorf("%d CPU profiles started, one running %v", fx.cpu.starts, fx.cpu.running != nil)
	}
	sameFiles(t, fx.files(t), map[string]string{"pass1_start.mutex.pprof": "mutex 1", "pass1_first.cpu.pprof": "cpu 1", "pass1_window.mutex.pprof": "mutex 2"})
	ps := fx.index(t).Passes[0]
	if ps.FirstCPUEndNs != 500_000_000 || ps.WindowCPUStartNs != -1 || ps.WindowCPUEndNs != -1 || len(ps.Notes) != 1 || !strings.Contains(ps.Notes[0], "no window CPU profile") {
		t.Errorf("pass %+v", ps)
	}
}

// A pass published while the window of the one before is open (the end of that
// rewrite told late) is skipped: no file is written for it, the index names the pass
// whose window was open, and the end of its own rewrite is still recorded. The open
// window ends at its own rewrite's end, and the next pass is profiled again. A pass
// replaced in its rewrite, whose end the store tells before the publication that
// replaced it, leaves no window open, and the next is profiled.
func TestAPassPublishedWhileAWindowIsOpenIsSkipped(t *testing.T) {
	t.Parallel()

	fx := newProfilerFixture(t, false)
	fx.p.published(1, at(1e9))
	fx.timers.fire(t)
	fx.p.published(2, at(5e9))
	fx.p.batch(time.Millisecond) // counted to pass 1, whose window is open
	fx.ended(1, 4e9)
	fx.ended(2, 6e9)
	if fx.cpu.starts != 2 || fx.cpu.running != nil {
		t.Fatalf("%d CPU profiles started, one running %v", fx.cpu.starts, fx.cpu.running != nil)
	}
	// Replaced in its rewrite: its end and the next publication at one instant, in
	// that order.
	fx.p.published(3, at(10e9))
	fx.ended(3, 11e9)
	fx.ended(3, 11e9) // told twice: the second does nothing
	fx.p.published(4, at(11e9))
	fx.ended(4, 13e9)

	files := fx.files(t)
	for name := range files {
		if strings.HasPrefix(name, "pass2_") {
			t.Errorf("the skipped pass has %s", name)
		}
	}
	for _, name := range []string{"pass1_window.cpu.pprof", "pass3_first.cpu.pprof", "pass4_first.cpu.pprof", "pass4_window.mutex.pprof"} {
		if _, ok := files[name]; !ok {
			t.Errorf("no %s among %v", name, files)
		}
	}
	idx := fx.index(t)
	if len(idx.Passes) != 4 {
		t.Fatalf("%d passes", len(idx.Passes))
	}
	p1, p2 := idx.Passes[0], idx.Passes[1]
	if p2.Skipped == "" || !strings.Contains(p2.Skipped, "pass 1 was still open") || len(p2.Files) != 0 || p2.RewriteEndNs != 6e9 || p2.Batches != 0 {
		t.Errorf("pass 2 %+v", p2)
	}
	if p1.Skipped != "" || p1.RewriteEndNs != 4e9 || p1.Batches != 1 {
		t.Errorf("pass 1 %+v", p1)
	}
	for _, ps := range idx.Passes[2:] {
		if ps.Skipped != "" || len(ps.Errors) != 0 || len(ps.Files) != 3 {
			t.Errorf("pass %d %+v", ps.Pass, ps)
		}
	}
}

// A pass published while the files of the window before are still being written (its
// CPU profile stopping) is skipped too, without the writer waiting for them.
func TestAPassPublishedWhileTheProfilesAreWrittenIsSkipped(t *testing.T) {
	t.Parallel()

	fx := newProfilerFixture(t, false)
	fx.p.published(1, at(1e9))
	fx.hold = make(chan struct{})
	fx.p.rewriteEnded(1, at(2e9)) // the window's goroutine waits on hold
	fx.p.published(2, at(2e9))
	close(fx.hold)
	fx.p.wait()
	fx.p.published(3, at(5e9))
	fx.ended(3, 6e9)
	idx := fx.index(t)
	if p := idx.Passes[1]; !strings.Contains(p.Skipped, "profiles of pass 1 were still being written") || len(p.Files) != 0 {
		t.Errorf("pass 2 %+v", p)
	}
	for _, i := range []int{0, 2} {
		if p := idx.Passes[i]; p.Skipped != "" || len(p.Files) != 3 {
			t.Errorf("pass %d %+v", p.Pass, p)
		}
	}
}

// The end of a rewrite told before its publication reached the profiler leaves that
// pass with no window, which the index says, and does not hold the next one off.
func TestARewriteEndToldBeforeItsPublicationOpensNoWindow(t *testing.T) {
	t.Parallel()

	fx := newProfilerFixture(t, false)
	fx.ended(1, 2e9)
	fx.p.published(1, at(1e9))
	fx.p.published(2, at(5e9))
	fx.ended(2, 6e9)
	idx := fx.index(t)
	if p := idx.Passes[0]; p.Skipped == "" || p.RewriteEndNs != 2e9 || len(p.Files) != 0 {
		t.Errorf("pass 1 %+v", p)
	}
	if p := idx.Passes[1]; p.Skipped != "" || len(p.Files) != 3 {
		t.Errorf("pass 2 %+v", p)
	}
}

// In a paced build a pass is judged if it was published at or after the start of the
// first paced batch, as the gate judges its windows; in one that is not paced, every
// pass is.
func TestAPassIsJudgedAsTheGateJudgesItsWindow(t *testing.T) {
	t.Parallel()

	fx := newProfilerFixture(t, true)
	fx.p.published(1, at(1e9))
	fx.ended(1, 2e9)
	fx.p.startPacing(at(3e9))
	fx.p.startPacing(at(4e9)) // only the first counts
	fx.p.published(2, at(3e9))
	fx.ended(2, 5e9)
	idx := fx.index(t)
	if idx.Passes[0].Judged || !idx.Passes[1].Judged {
		t.Errorf("judged %v and %v, want false and true", idx.Passes[0].Judged, idx.Passes[1].Judged)
	}

	unpaced := newProfilerFixture(t, false)
	unpaced.p.published(1, at(1e9))
	if !unpaced.p.index.Passes[0].Judged {
		t.Error("a pass of a build that is not paced is not judged")
	}
}

// The close of the profiling ends an open window with a note, puts the mutex profile
// fraction back once, and leaves a later publication unprofiled; a second close does
// nothing.
func TestTheCloseEndsAnOpenWindow(t *testing.T) {
	t.Parallel()

	fx := newProfilerFixture(t, false)
	fx.p.published(1, at(1e9))
	fx.timers.fire(t)
	if err := fx.p.close(); err != nil {
		t.Fatal(err)
	}
	fx.p.published(2, at(5e9))
	if err := fx.p.close(); err != nil {
		t.Fatal(err)
	}
	if fx.restored != 1 || fx.cpu.running != nil || fx.cpu.starts != 2 {
		t.Errorf("fraction restored %d times, CPU profile running %v, %d started", fx.restored, fx.cpu.running != nil, fx.cpu.starts)
	}
	idx := fx.index(t)
	p1 := idx.Passes[0]
	if len(p1.Files) != 4 || len(p1.Notes) != 1 || !strings.Contains(p1.Notes[0], "profiling ended before") {
		t.Errorf("pass 1 %+v", p1)
	}
	if p := idx.Passes[1]; p.Skipped == "" || len(p.Files) != 0 {
		t.Errorf("pass 2 %+v", p)
	}
}

// A CPU profile that does not start (another runs in the process) skips the pass with
// the error, opens no window, and the next pass tries again.
func TestACPUProfileThatDoesNotStartSkipsThePass(t *testing.T) {
	t.Parallel()

	fx := newProfilerFixture(t, false)
	fx.cpu.fail = errors.New("cpu profiling already in use")
	fx.p.published(1, at(1e9))
	fx.ended(1, 2e9)
	fx.cpu.fail = nil
	fx.p.published(2, at(3e9))
	fx.ended(2, 4e9)
	idx := fx.index(t)
	if p := idx.Passes[0]; !strings.Contains(p.Skipped, "already in use") || slices.Contains(p.Files, "pass1_first.cpu.pprof") || len(fx.timers.set) != 1 {
		t.Errorf("pass 1 %+v, %d timers", p, len(fx.timers.set))
	}
	if _, err := os.Stat(filepath.Join(fx.p.dir, "pass1_first.cpu.pprof")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the profile that did not start left its file: %v", err)
	}
	if p := idx.Passes[1]; p.Skipped != "" || len(p.Files) != 3 {
		t.Errorf("pass 2 %+v", p)
	}
}

// A build without profiles has no profiler: every call of a nil one does nothing.
func TestANilProfilerDoesNothing(t *testing.T) {
	t.Parallel()

	var p *windowProfiler
	p.retaining(1)
	p.startPacing(at(1))
	p.batch(time.Second)
	p.published(1, at(2))
	p.rewriteEnded(1, at(3))
	p.settleEnded(1, at(4))
	if err := p.close(); err != nil {
		t.Error(err)
	}
}
