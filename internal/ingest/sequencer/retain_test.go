package sequencer_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/ingest/sequencer"
	"github.com/lotannauo/toposhift/internal/replay/capture"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// window is W in these tests, as in serve.
const window = 30 * 24 * time.Hour

const day = 24 * time.Hour

// The messages the tests look for.
const (
	msgHeld    = "retention is held"
	msgNotYet  = "retention has not started"
	msgStopped = "retention is stopped"
	msgSkipped = "skipped"
	msgTooLong = "pending for more than two hours"
	msgCapped  = "capped advance"
	msgFirst   = "retained for the first time"
	msgResumed = "resumed at open is pending"
	msgCatchUp = "catch-up"
)

// targetAt is the target of a check at now: now less W, truncated to the hour.
func targetAt(now time.Time) time.Time { return now.Add(-window).UTC().Truncate(time.Hour) }

// gid is the id of the calling goroutine.
func gid() uint64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	f := bytes.Fields(buf[:n])
	id, err := strconv.ParseUint(string(f[1]), 10, 64)
	if err != nil {
		panic(err)
	}
	return id
}

// ---- the store's retention, and the wait ----

// retainStore is the store a harness writes and retains: memstore or
// pebblestore.
type retainStore interface {
	sequencer.Writer
	sequencer.Retainer
}

// retainer is the retention check's store: it records each call, with the
// goroutine that made it, and forwards it to the store unless noFwd is set.
// err, when set, is returned instead.
type retainer struct {
	fwd sequencer.Retainer

	mu       sync.Mutex
	noFwd    bool
	err      error
	calls    []time.Time
	gids     []uint64
	inFlight atomic.Int32
}

func (r *retainer) Retain(ctx context.Context, h time.Time) error {
	r.inFlight.Add(1)
	defer r.inFlight.Add(-1)
	r.mu.Lock()
	r.calls = append(r.calls, h)
	r.gids = append(r.gids, gid())
	noFwd, err := r.noFwd, r.err
	r.mu.Unlock()
	if err != nil {
		return err
	}
	if noFwd {
		return nil
	}
	return r.fwd.Retain(ctx, h)
}

func (r *retainer) got() []time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]time.Time(nil), r.calls...)
}

func (r *retainer) setErr(err error) {
	r.mu.Lock()
	r.err = err
	r.mu.Unlock()
}

// fakeWait is the wait for a pass. It returns err at once, unless it is held:
// then it waits for the release or the context. lag, when set, is waited for
// after the context ends and before the context's error is returned.
type fakeWait struct {
	mu    sync.Mutex
	hold  chan struct{}
	err   error
	lag   chan struct{}
	calls atomic.Int32
}

func (f *fakeWait) Wait(ctx context.Context) error {
	f.calls.Add(1)
	f.mu.Lock()
	hold, err, lag := f.hold, f.err, f.lag
	f.mu.Unlock()
	if hold != nil {
		if err := ctx.Err(); err != nil {
			return err // a probe: the context ended before the wait began
		}
		select {
		case <-hold:
		case <-ctx.Done():
			if lag != nil {
				<-lag
			}
			return ctx.Err()
		}
	}
	return err
}

// held makes the next waits block until release.
func (f *fakeWait) held() {
	f.mu.Lock()
	f.hold = make(chan struct{})
	f.mu.Unlock()
}

func (f *fakeWait) release() {
	f.mu.Lock()
	if f.hold != nil {
		close(f.hold)
		f.hold = nil
	}
	f.mu.Unlock()
}

func (f *fakeWait) setErr(err error) {
	f.mu.Lock()
	f.err = err
	f.mu.Unlock()
}

// ---- the log ----

type logLine struct {
	level slog.Level
	msg   string
}

// logCapture keeps every log line, and writes it to out too.
type logCapture struct {
	mu    sync.Mutex
	lines []logLine
	out   io.Writer
	text  slog.Handler
}

func newLogCapture(out io.Writer) *logCapture {
	return &logCapture{out: out, text: slog.NewTextHandler(out, &slog.HandlerOptions{Level: slog.LevelDebug})}
}

func (c *logCapture) Enabled(context.Context, slog.Level) bool { return true }

func (c *logCapture) Handle(ctx context.Context, r slog.Record) error {
	c.mu.Lock()
	c.lines = append(c.lines, logLine{r.Level, r.Message})
	c.mu.Unlock()
	return c.text.Handle(ctx, r)
}

func (c *logCapture) WithAttrs([]slog.Attr) slog.Handler { return c }
func (c *logCapture) WithGroup(string) slog.Handler      { return c }

// take returns the lines logged since the last take.
func (c *logCapture) take() []logLine {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := c.lines
	c.lines = nil
	return out
}

// count is how many of lines are at level and contain sub.
func count(lines []logLine, level slog.Level, sub string) int {
	n := 0
	for _, l := range lines {
		if l.level == level && strings.Contains(l.msg, sub) {
			n++
		}
	}
	return n
}

// atLeast is how many of lines are at level or above.
func atLeast(lines []logLine, level slog.Level) int {
	n := 0
	for _, l := range lines {
		if l.level >= level {
			n++
		}
	}
	return n
}

// ---- the harness ----

// rh is a sequencer that retains a memstore, with the fake ingest clock, a fake
// uptime, the check's tick and the fake wait.
type rh struct {
	*harness
	store   retainStore
	ret     *retainer
	wait    *fakeWait
	logs    *logCapture
	up      atomic.Int64
	rtick   chan time.Time
	checked chan struct{}
	waited  chan struct{}
	uid     int
	// the check's configuration, before New
	window time.Duration
	waitFn func(context.Context) error
	// async: a check does not wait for the waiter a call starts, whose wait is
	// the real store's.
	async bool
}

type retainOption func(*rh)

func withWindow(w time.Duration) retainOption { return func(h *rh) { h.window = w } }

// withStore makes the harness write and retain st, and wait for its passes with
// wait.
func withStore(st retainStore, wait func(context.Context) error) retainOption {
	return func(h *rh) { h.store, h.ret.fwd, h.waitFn, h.async = st, st, wait, true }
}

// newRetainHarness makes a sequencer over a memstore opened with serve's layer
// options (L0 kept, L1 60 days behind), at now, and does not start it. hooks are
// added to the harness's own.
func newRetainHarness(t *testing.T, now time.Time, hooks sequencer.Hooks, opts ...retainOption) *rh {
	t.Helper()
	mem, err := memstore.Open(memstore.Options{
		Keep:    [4]bool{true, false, false, false},
		Offsets: [4]time.Duration{0, 60 * day, 0, 0},
	})
	if err != nil {
		t.Fatal(err)
	}
	h := &rh{
		store: mem, ret: &retainer{fwd: mem}, wait: &fakeWait{}, logs: newLogCapture(t.Output()),
		rtick: make(chan time.Time), checked: make(chan struct{}, 100), waited: make(chan struct{}, 100),
		window: window,
	}
	h.waitFn = h.wait.Wait
	for _, o := range opts {
		o(h)
	}
	afterCheck, afterWaiter := hooks.AfterRetainCheck, hooks.AfterWaiter
	hooks.AfterRetainCheck = func() {
		if afterCheck != nil {
			afterCheck()
		}
		h.checked <- struct{}{}
	}
	hooks.AfterWaiter = func() {
		if afterWaiter != nil {
			afterWaiter()
		}
		h.waited <- struct{}{}
	}
	h.harness = newHarness(t, &fakeStore{inner: h.store}, withHooks(hooks), withConfig(func(c *sequencer.Config) {
		c.Logger = slog.New(h.logs)
		c.Retain = &sequencer.RetainConfig{
			Store:  h.ret,
			Wait:   h.waitFn,
			Window: h.window,
			Offset: 0,
			Every:  sequencer.DefaultRetainEvery,
			Tick:   func(time.Duration) <-chan time.Time { return h.rtick },
			Uptime: func() time.Duration { return time.Duration(h.up.Load()) },
		}
	}))
	h.clock.set(now)
	return h
}

// begin starts the loop and, unless the wait is held, waits for the boot
// waiter's report.
func (h *rh) begin() *rh {
	h.t.Helper()
	held := h.isHeld()
	h.start()
	if !held {
		recv(h.t, h.waited)
	}
	return h
}

func (h *rh) isHeld() bool {
	h.wait.mu.Lock()
	defer h.wait.mu.Unlock()
	return h.wait.hold != nil
}

// check runs one retention check and waits for it, and for the report of the
// waiter a call started, unless the wait is held.
func (h *rh) check() {
	h.t.Helper()
	before := len(h.ret.got())
	h.rtick <- h.clock.now()
	recv(h.t, h.checked)
	if len(h.ret.got()) > before && !h.isHeld() && h.ret.err == nil && !h.async {
		recv(h.t, h.waited)
	}
}

// advance moves the ingest clock and the uptime together; jump moves the clock
// alone; idle moves the uptime alone.
func (h *rh) advance(d time.Duration) {
	h.clock.set(h.clock.now().Add(d))
	h.up.Add(int64(d))
}

func (h *rh) jump(d time.Duration) { h.clock.set(h.clock.now().Add(d)) }

func (h *rh) idle(d time.Duration) { h.up.Add(int64(d)) }

// observed sends a pull of a new pod observed at tm (an event time on the
// collector's clock) and fails the test unless it is answered 200; rejected is
// how many of its log records must be rejected.
func (h *rh) observed(tm time.Time, rejected int64) {
	h.t.Helper()
	h.uid++
	uid := fmt.Sprintf("r-%d", h.uid)
	p, err := h.send(logs("", pullPod(tm, pod{uid, uid, "", time.Time{}, time.Time{}, "Running"})))
	if err != nil || p.Rejected != rejected {
		h.t.Fatalf("pull observed at %s: partial %+v, error %v; want %d rejected", tm, p, err, rejected)
	}
}

// fresh sends a pull observed now.
func (h *rh) fresh() { h.t.Helper(); h.observed(h.clock.now(), 0) }

func (h *rh) status() sequencer.RetentionStatus {
	h.t.Helper()
	st, ok := h.seq.RetentionStatus()
	if !ok {
		h.t.Fatal("the sequencer does not retain")
	}
	return st
}

// retainTo retains the memstore to tm before the loop starts.
func (h *rh) retainTo(tm time.Time) {
	h.t.Helper()
	if err := h.store.Retain(context.Background(), tm); err != nil {
		h.t.Fatal(err)
	}
}

func (h *rh) wantCalls(want ...time.Time) {
	h.t.Helper()
	got := h.ret.got()
	if len(got) != len(want) {
		h.t.Fatalf("Retain called %d times %v, want %d %v", len(got), got, len(want), want)
	}
	for i := range got {
		if !got[i].Equal(want[i]) {
			h.t.Fatalf("Retain call %d to %s, want %s", i, got[i], want[i])
		}
	}
}

// trueNow is the true time of the scenarios whose server clock is wrong.
var trueNow = time.Date(2026, 10, 1, 7, 30, 0, 0, time.UTC)

const year = 365 * day

// ---- R1 to R12 ----

// R1. A retention is due exactly when the target is a day past the stored
// horizon, and not a nanosecond before.
func TestR1DueAtExactlyADay(t *testing.T) {
	stored := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	h := newRetainHarness(t, stored.Add(window+day-1), sequencer.Hooks{})
	h.retainTo(stored)
	h.begin()
	h.fresh()
	h.check()
	h.wantCalls()
	h.advance(1)
	h.fresh()
	h.check()
	h.wantCalls(stored.Add(day))
}

// R2. A store with no horizon, empty: no call while the agreement is unknown,
// with a warning at each check; once a record agrees, exactly one call, to the
// target. With the clock a year ahead and producers on true time, no call ever.
func TestR2EmptyStoreWaitsForAgreement(t *testing.T) {
	h := newRetainHarness(t, trueNow, sequencer.Hooks{}).begin()
	h.logs.take()
	for range 3 {
		h.check()
		if lines := h.logs.take(); count(lines, slog.LevelWarn, msgNotYet) != 1 || atLeast(lines, slog.LevelError) != 0 {
			t.Fatalf("a check with no agreement logged %+v, want one warning", lines)
		}
		h.advance(time.Hour)
	}
	h.wantCalls()
	h.fresh()
	h.check()
	want := targetAt(h.clock.now())
	h.wantCalls(want)
	if lines := h.logs.take(); count(lines, slog.LevelWarn, msgFirst) != 1 {
		t.Errorf("the first retention logged %+v, want a warning naming its target", lines)
	}
	h.advance(time.Hour)
	h.fresh()
	h.check()
	h.wantCalls(want)

	t.Run("clock a year ahead", func(t *testing.T) {
		h := newRetainHarness(t, trueNow.Add(year), sequencer.Hooks{}).begin()
		for i := range 10 {
			h.observed(trueNow.Add(time.Duration(i)*12*time.Hour), 0)
			h.check()
			h.advance(12 * time.Hour)
		}
		h.wantCalls()
	})
}

// R3. A store with 60 days of history and no horizon: no call before an
// agreeing record is committed; then exactly one call, to the target, and the
// history before it is gone.
func TestR3HistoryWithNoHorizonWaitsForAgreement(t *testing.T) {
	h := newRetainHarness(t, trueNow, sequencer.Hooks{})
	ref := newReference(t)
	for d := 60; d > 0; d-- {
		tm := trueNow.Add(-time.Duration(d) * day)
		uid := fmt.Sprintf("old-%d", d)
		ref.request(logs("", pullPod(tm, pod{uid, uid, "", time.Time{}, time.Time{}, "Running"})), tm)
	}
	if err := h.store.Write(context.Background(), ref.written); err != nil {
		t.Fatal(err)
	}
	h.begin()
	for range 3 {
		h.check()
		h.advance(time.Hour)
	}
	h.wantCalls()
	h.fresh()
	h.check()
	target := targetAt(h.clock.now())
	h.wantCalls(target)
	if got := h.store.LayerHorizon(catalog.L2).Time; !got.Equal(target) {
		t.Errorf("L2 horizon %s, want %s", got, target)
	}
}

// R4. Ten days of downtime, then ingest resumes with an agreeing record: one
// catch-up call.
func TestR4CatchUpAfterDowntime(t *testing.T) {
	h := newRetainHarness(t, trueNow, sequencer.Hooks{})
	h.retainTo(targetAt(trueNow).Add(-10 * day))
	h.begin()
	h.fresh()
	h.logs.take()
	h.check()
	if lines := h.logs.take(); count(lines, slog.LevelInfo, msgCatchUp) != 1 {
		t.Errorf("the catch-up logged %+v", lines)
	}
	h.wantCalls(targetAt(trueNow))
	if st := h.status(); st.CatchUps != 1 || st.Calls != 1 || st.Capped != 0 {
		t.Errorf("counters %+v, want one call, a catch-up", st)
	}
	h.advance(time.Hour)
	h.check()
	h.wantCalls(targetAt(trueNow))
}

// R5. Agree, then the clock jumps a year ahead: the agreement becomes disagree,
// nothing is retained, and every check logs, at warning level for a day of
// uptime, then at error level.
func TestR5ClockJumpDisagrees(t *testing.T) {
	h := newRetainHarness(t, trueNow, sequencer.Hooks{})
	h.retainTo(targetAt(trueNow).Add(-day))
	h.begin()
	h.fresh()
	h.check()
	h.wantCalls(targetAt(trueNow))
	h.logs.take()

	h.jump(year)
	for _, step := range []struct {
		idle  time.Duration
		level slog.Level
	}{{0, slog.LevelWarn}, {12 * time.Hour, slog.LevelWarn}, {12*time.Hour - 1, slog.LevelWarn}, {1, slog.LevelError}, {12 * time.Hour, slog.LevelError}} {
		h.idle(step.idle)
		h.check()
		lines := h.logs.take()
		if count(lines, step.level, msgHeld) != 1 || atLeast(lines, slog.LevelInfo) != 1 {
			t.Fatalf("a check after %s of disagreement logged %+v, want one line at %s", step.idle, lines, step.level)
		}
		if st := h.status(); st.Agreement != sequencer.Disagree {
			t.Fatalf("agreement %s, want disagree", st.Agreement)
		}
	}
	h.wantCalls(targetAt(trueNow))
}

// R5c. Idle for three days on a right clock (the ingest clock and the uptime
// advance together): the agreement stays agree, and the daily call goes on.
func TestR5cIdleOnARightClockAgrees(t *testing.T) {
	h := newRetainHarness(t, trueNow, sequencer.Hooks{})
	h.retainTo(targetAt(trueNow).Add(-day))
	h.begin()
	h.fresh()
	h.check()
	want := []time.Time{targetAt(trueNow)}
	for i := range 3 {
		h.advance(day)
		h.check()
		want = append(want, targetAt(trueNow).Add(time.Duration(i+1)*day))
		if st := h.status(); st.Agreement != sequencer.Agree {
			t.Fatalf("after %d idle days: agreement %s, want agree", i+1, st.Agreement)
		}
	}
	h.wantCalls(want...)
}

// R5d. Idle, then the clock jumps a year: disagree.
func TestR5dIdleThenJumpDisagrees(t *testing.T) {
	h := newRetainHarness(t, trueNow, sequencer.Hooks{}).begin()
	h.fresh()
	h.advance(2 * day)
	h.check()
	if st := h.status(); st.Agreement != sequencer.Agree {
		t.Fatalf("idle on a right clock: agreement %s, want agree", st.Agreement)
	}
	calls := h.ret.got()
	h.jump(year)
	h.check()
	if st := h.status(); st.Agreement != sequencer.Disagree {
		t.Fatalf("after the jump: agreement %s, want disagree", st.Agreement)
	}
	h.wantCalls(calls...)
}

// R5f. A collector two days ahead for one hour, then corrected: agree again at
// the next check after the corrected batch.
func TestR5fCollectorAheadThenCorrected(t *testing.T) {
	h := newRetainHarness(t, trueNow, sequencer.Hooks{})
	h.retainTo(targetAt(trueNow).Add(-day))
	h.begin()
	h.fresh()
	h.observed(trueNow.Add(2*day), 0)
	h.check()
	if st := h.status(); st.Agreement != sequencer.Disagree {
		t.Fatalf("with the collector ahead: agreement %s, want disagree", st.Agreement)
	}
	h.wantCalls()
	h.advance(time.Hour)
	h.fresh()
	h.check()
	if st := h.status(); st.Agreement != sequencer.Agree {
		t.Fatalf("after the corrected batch: agreement %s, want agree", st.Agreement)
	}
	h.wantCalls(targetAt(h.clock.now()))
}

// R5g. A batch of replayed old records reads disagree while it is the latest,
// and a fresh batch agrees again.
func TestR5gReplayThenFreshAgrees(t *testing.T) {
	h := newRetainHarness(t, trueNow, sequencer.Hooks{}).begin()
	h.fresh()
	h.check()
	if st := h.status(); st.Agreement != sequencer.Agree {
		t.Fatalf("agreement %s, want agree", st.Agreement)
	}
	h.advance(time.Minute)
	h.observed(trueNow.Add(-10*day), 0)
	h.check()
	if st := h.status(); st.Agreement != sequencer.Disagree {
		t.Fatalf("after the replayed batch: agreement %s, want disagree", st.Agreement)
	}
	h.advance(time.Minute)
	h.fresh()
	h.check()
	if st := h.status(); st.Agreement != sequencer.Agree {
		t.Fatalf("after the fresh batch: agreement %s, want agree", st.Agreement)
	}
}

// R5e. A horizon ahead of true time, then a record on true time refused as
// before it: disagree, and no further call, however long the uptime.
func TestR5eRefusedRecordsCount(t *testing.T) {
	h := newRetainHarness(t, trueNow.Add(year), sequencer.Hooks{})
	h.retainTo(trueNow.Add(5 * day))
	h.begin()
	h.observed(trueNow, 1)
	for range 4 {
		h.check()
		if st := h.status(); st.Agreement != sequencer.Disagree {
			t.Fatalf("agreement %s, want disagree", st.Agreement)
		}
		h.advance(day)
	}
	h.wantCalls()
}

// R5b. Unknown, with the clock far ahead: capped calls of two days, at most once
// per day of uptime, even when the clock jumps by days between checks; each is a
// warning.
func TestR5bUnknownAdvancesCapped(t *testing.T) {
	stored := targetAt(trueNow)
	h := newRetainHarness(t, trueNow.Add(year), sequencer.Hooks{})
	h.retainTo(stored)
	h.begin()
	h.logs.take()
	h.check()
	h.wantCalls(stored.Add(2 * day))
	if lines := h.logs.take(); count(lines, slog.LevelWarn, msgCapped) != 1 {
		t.Errorf("the capped call logged %+v, want a warning", lines)
	}
	for range 3 {
		h.jump(5 * day)
		h.idle(10 * time.Minute)
		h.check()
	}
	h.wantCalls(stored.Add(2 * day))
	h.idle(day - 30*time.Minute)
	h.check()
	h.wantCalls(stored.Add(2*day), stored.Add(4*day))
	if st := h.status(); st.Capped != 2 || st.Agreement != sequencer.AgreementUnknown {
		t.Errorf("status %+v, want two capped calls, agreement unknown", st)
	}
}

// R6. The agreement data: receipt, unknown and object_field records never
// count; observed and producer_event records within a day agree and two days off
// disagree; a write that failed without landing does not count, and one that
// returned an error but advanced the last sequence number does.
func TestR6AgreementData(t *testing.T) {
	for b, want := range map[store.EventTimeBasis]bool{
		store.BasisUnknown: false, store.BasisObjectField: false, store.BasisObserved: true,
		store.BasisReceipt: false, store.BasisProducerEvent: true,
	} {
		if sequencer.Qualifies(b) != want {
			t.Errorf("basis %s counts: %v, want %v", b, !want, want)
		}
	}

	// An entity event of a pod that carries no creation time, so that its records
	// take the event's own time: the producer's, or, with none, the receipt's.
	podEvent := func(uid string, tm time.Time) capture.LogsData {
		lr := entity("entity.state", "k8s.pod", "k8s.pod.uid", uid, tm, map[string]string{
			"k8s.pod.name": uid, "k8s.namespace.name": "default", "k8s.pod.phase": "Running",
		})
		if tm.IsZero() {
			lr.TimeUnixNano = 0
		}
		return logs("", lr)
	}
	type step struct {
		name  string
		d     capture.LogsData
		basis store.EventTimeBasis
		want  sequencer.Agreement
	}
	created := pod{"made", "made", "", trueNow.Add(-2 * day), time.Time{}, "Running"}
	for _, steps := range [][]step{
		{
			{"receipt", podEvent("e-1", time.Time{}), store.BasisReceipt, sequencer.AgreementUnknown},
			{"object field", logs("", pullPod(trueNow, created)), store.BasisObjectField, sequencer.AgreementUnknown},
			{"observed within a day", logs("", pullPod(trueNow.Add(-23*time.Hour), pod{"o-1", "o-1", "", time.Time{}, time.Time{}, "Running"})), store.BasisObserved, sequencer.Agree},
			{"receipt after it", podEvent("e-2", time.Time{}), store.BasisReceipt, sequencer.Agree},
			{"observed two days off", logs("", pullPod(trueNow.Add(-2*day), pod{"o-2", "o-2", "", time.Time{}, time.Time{}, "Running"})), store.BasisObserved, sequencer.Disagree},
		},
		{
			{"producer event within a day", podEvent("e-3", trueNow.Add(-23*time.Hour)), store.BasisProducerEvent, sequencer.Agree},
			{"producer event two days off", podEvent("e-4", trueNow.Add(-2*day)), store.BasisProducerEvent, sequencer.Disagree},
			{"producer event right again", podEvent("e-5", trueNow), store.BasisProducerEvent, sequencer.Agree},
		},
	} {
		var bases []store.EventTimeBasis
		h := newRetainHarness(t, trueNow, sequencer.Hooks{BeforeWrite: func(b []store.Record) {
			for _, r := range b {
				bases = append(bases, r.EventTimeBasis)
			}
		}})
		h.ret.noFwd = true
		h.begin()
		for _, s := range steps {
			bases = nil
			if _, err := h.send(s.d); err != nil {
				t.Fatalf("%s: %v", s.name, err)
			}
			if len(bases) == 0 {
				t.Fatalf("%s: nothing written", s.name)
			}
			for _, b := range bases {
				if b != s.basis {
					t.Fatalf("%s: a record of basis %s, want every one %s", s.name, b, s.basis)
				}
			}
			h.check()
			if got := h.status().Agreement; got != s.want {
				t.Fatalf("%s: agreement %s, want %s", s.name, got, s.want)
			}
		}
	}

	t.Run("failed writes", func(t *testing.T) {
		h := newRetainHarness(t, trueNow, sequencer.Hooks{})
		h.ret.noFwd = true
		st := h.st
		h.begin()
		st.inject = func(int, []store.Record) (bool, error) { return false, errInjected }
		if _, err := h.send(logs("", pullPod(trueNow, pod{"f-1", "f-1", "", time.Time{}, time.Time{}, "Running"}))); err == nil {
			t.Fatal("a write that failed was answered 200")
		}
		h.check()
		if got := h.status().Agreement; got != sequencer.AgreementUnknown {
			t.Fatalf("after a write that did not land: agreement %s, want unknown", got)
		}
		st.inject = func(int, []store.Record) (bool, error) { return true, errInjected }
		h.sendOK(logs("", pullPod(trueNow, pod{"f-2", "f-2", "", time.Time{}, time.Time{}, "Running"})))
		h.check()
		if got := h.status().Agreement; got != sequencer.Agree {
			t.Fatalf("after a write that errored but landed: agreement %s, want agree", got)
		}
	})
}

// R7. The log levels: a warning while unknown; for a disagreement, a warning for
// its first day of uptime, then an error.
func TestR7LogLevels(t *testing.T) {
	h := newRetainHarness(t, trueNow, sequencer.Hooks{}).begin()
	h.logs.take()
	h.check()
	if lines := h.logs.take(); count(lines, slog.LevelWarn, msgNotYet) != 1 || atLeast(lines, slog.LevelError) != 0 {
		t.Fatalf("unknown: %+v, want one warning", lines)
	}
	h.observed(trueNow.Add(-3*day), 0)
	for _, s := range []struct {
		idle  time.Duration
		level slog.Level
	}{{0, slog.LevelWarn}, {day - 1, slog.LevelWarn}, {1, slog.LevelError}} {
		h.idle(s.idle)
		h.check()
		if lines := h.logs.take(); count(lines, s.level, msgHeld) != 1 || atLeast(lines, slog.LevelInfo) != 1 {
			t.Fatalf("disagreeing for %s more: %+v, want one line at %s", s.idle, lines, s.level)
		}
	}
	// Agreement ends the disagreement: the next one starts at warning level.
	h.fresh()
	h.check()
	h.logs.take()
	h.observed(trueNow.Add(-3*day), 0)
	h.check()
	if lines := h.logs.take(); count(lines, slog.LevelWarn, msgHeld) != 1 {
		t.Fatalf("a new disagreement: %+v, want a warning", lines)
	}
}

// R8. The edge cases of the due rule: a clock behind the stored horizon, a
// horizon equal to the target, a lowered W (one catch-up call) and a raised W
// (no call).
func TestR8EdgeCases(t *testing.T) {
	t.Run("clock behind the horizon", func(t *testing.T) {
		h := newRetainHarness(t, trueNow, sequencer.Hooks{})
		h.retainTo(trueNow.Add(10 * day))
		h.begin()
		h.observed(trueNow, 1)
		h.check()
		h.wantCalls()
	})
	t.Run("horizon equal to the target", func(t *testing.T) {
		h := newRetainHarness(t, trueNow, sequencer.Hooks{})
		h.retainTo(targetAt(trueNow))
		h.begin()
		h.fresh()
		h.check()
		h.wantCalls()
	})
	t.Run("W lowered", func(t *testing.T) {
		h := newRetainHarness(t, trueNow, sequencer.Hooks{})
		h.retainTo(trueNow.Add(-60 * day).Truncate(time.Hour))
		h.begin()
		h.fresh()
		h.check()
		h.check()
		h.wantCalls(targetAt(trueNow))
		if st := h.status(); st.CatchUps != 1 {
			t.Errorf("status %+v, want one catch-up", st)
		}
	})
	t.Run("W raised", func(t *testing.T) {
		h := newRetainHarness(t, trueNow, sequencer.Hooks{}, withWindow(60*day))
		h.retainTo(targetAt(trueNow))
		h.begin()
		for range 10 {
			h.fresh()
			h.check()
			h.advance(2 * day)
		}
		h.wantCalls()
	})
}

// R9. A due check while a pass is pending is skipped: no call, counted, a
// warning, and an error too once the pass has been pending for over two hours.
// When the pass ends, the next check calls.
func TestR9SkipWhilePending(t *testing.T) {
	h := newRetainHarness(t, trueNow, sequencer.Hooks{})
	h.retainTo(targetAt(trueNow).Add(-day))
	h.wait.held()
	h.begin()
	h.fresh()
	h.logs.take()
	h.advance(time.Hour)
	h.fresh()
	h.check()
	if lines := h.logs.take(); count(lines, slog.LevelWarn, msgSkipped) != 1 || atLeast(lines, slog.LevelError) != 0 {
		t.Fatalf("a skip an hour into the pass: %+v, want one warning", lines)
	}
	h.advance(90 * time.Minute)
	h.fresh()
	h.check()
	if lines := h.logs.take(); count(lines, slog.LevelWarn, msgSkipped) != 1 || count(lines, slog.LevelError, msgTooLong) != 1 {
		t.Fatalf("a skip two and a half hours into the pass: %+v, want a warning and an error", lines)
	}
	h.wantCalls()
	if st := h.status(); st.Skipped != 2 || st.PendingSince.IsZero() {
		t.Fatalf("status %+v, want two skips, pending", st)
	}
	h.wait.release()
	recv(t, h.waited)
	if st := h.status(); !st.PendingSince.IsZero() {
		t.Fatalf("status %+v, want nothing pending", st)
	}
	h.check()
	h.wantCalls(targetAt(h.clock.now()))
}

// R10. A pass is marked pending before its waiter starts: a check made between
// the two, at boot and after a call, sees it pending.
func TestR10PendingBeforeTheWaiter(t *testing.T) {
	var h *rh
	var nested atomic.Int32
	h = newRetainHarness(t, trueNow, sequencer.Hooks{BetweenPendingAndWaiter: func() {
		if nested.Add(1) <= 2 {
			sequencer.RetainCheck(context.Background(), h.seq)
		}
	}})
	// The store does not move: every check finds the retention due.
	h.retainTo(targetAt(trueNow).Add(-day))
	h.ret.noFwd = true
	h.start()
	recv(t, h.checked) // the check made at boot
	recv(t, h.waited)
	h.wantCalls()
	h.check() // calls, and the check made after the call skips
	recv(t, h.checked)
	h.wantCalls(targetAt(trueNow))
	if st := h.status(); st.Skipped != 2 {
		t.Fatalf("status %+v, want the two checks between pending and the waiter skipped", st)
	}
}

// R11. The waiter's report: an error from the store stops retention, with an
// error at every check; the store's closing and the context's error log no error
// and stop retaining.
func TestR11WaiterResults(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
		stop bool
	}{
		{"latched", errors.New("pebblestore: a commit's outcome was lost"), true},
		{"closed", fmt.Errorf("pebblestore: WaitRetained: %w", store.ErrClosed), false},
		{"context", fmt.Errorf("pebblestore: WaitRetained: %w", context.Canceled), false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			h := newRetainHarness(t, trueNow, sequencer.Hooks{})
			h.retainTo(targetAt(trueNow).Add(-day))
			h.begin()
			h.wait.setErr(tt.err)
			h.fresh()
			h.logs.take()
			h.check()
			h.wantCalls(targetAt(trueNow))
			lines := h.logs.take()
			if tt.stop != (count(lines, slog.LevelError, msgStopped) == 1) || (!tt.stop && atLeast(lines, slog.LevelError) != 0) {
				t.Fatalf("the waiter's report logged %+v", lines)
			}
			for range 2 {
				h.advance(day)
				h.fresh()
				h.check()
				lines := h.logs.take()
				if tt.stop && count(lines, slog.LevelError, msgStopped) != 1 {
					t.Fatalf("a check once stopped logged %+v, want an error", lines)
				}
				if !tt.stop && atLeast(lines, slog.LevelError) != 0 {
					t.Fatalf("a check after the shutdown report logged %+v, want no error", lines)
				}
			}
			h.wantCalls(targetAt(trueNow))
			if st := h.status(); (st.StoppedBy != "") != tt.stop {
				t.Errorf("stopped by %q", st.StoppedBy)
			}
		})
	}
}

// R11b and R12. A validation error from Retain stops retention: no call after it,
// an error at every check, and ingest still answers 200.
func TestR11bValidationErrorStopsRetentionNotIngest(t *testing.T) {
	h := newRetainHarness(t, trueNow, sequencer.Hooks{})
	h.retainTo(targetAt(trueNow).Add(-day))
	h.begin()
	h.ret.setErr(fmt.Errorf("memstore: Retain: horizon out of range: %w", store.ErrInvalid))
	h.fresh()
	h.logs.take()
	h.check()
	if lines := h.logs.take(); count(lines, slog.LevelError, msgStopped) != 1 {
		t.Fatalf("the refused retention logged %+v, want an error", lines)
	}
	h.ret.setErr(nil)
	for range 3 {
		h.advance(day)
		h.fresh() // ingest goes on: answered 200
		h.check()
		if lines := h.logs.take(); count(lines, slog.LevelError, msgStopped) != 1 {
			t.Fatalf("a check once stopped logged %+v, want an error", lines)
		}
	}
	h.wantCalls(targetAt(trueNow))
	if st := h.status(); !strings.Contains(st.StoppedBy, "ErrInvalid") && !strings.Contains(st.StoppedBy, "invalid") {
		t.Errorf("stopped by %q, want the validation error", st.StoppedBy)
	}
}

// R12. Without a RetainConfig the sequencer never retains, and says so.
func TestR12NoRetainConfig(t *testing.T) {
	h := newHarness(t, &fakeStore{inner: newMem(t)}).start()
	h.sendOK(logs("", pullPod(t0, p1)))
	if _, ok := h.seq.RetentionStatus(); ok {
		t.Error("a sequencer with no RetainConfig reports a retention status")
	}
}

// R16. The loop alone calls Retain: on the goroutine that writes. Run with -race.
func TestR16RetainOnTheLoop(t *testing.T) {
	var mu sync.Mutex
	writers := map[uint64]bool{}
	h := newRetainHarness(t, trueNow, sequencer.Hooks{BeforeWrite: func([]store.Record) {
		mu.Lock()
		writers[gid()] = true
		mu.Unlock()
	}})
	h.retainTo(targetAt(trueNow).Add(-day))
	h.begin()
	for range 3 {
		h.fresh()
		h.check()
		h.advance(day)
	}
	h.ret.mu.Lock()
	gids := append([]uint64(nil), h.ret.gids...)
	h.ret.mu.Unlock()
	mu.Lock()
	defer mu.Unlock()
	if len(gids) != 3 || len(writers) != 1 {
		t.Fatalf("%d calls, %d writing goroutines", len(gids), len(writers))
	}
	for _, g := range gids {
		if !writers[g] {
			t.Errorf("Retain called on goroutine %d, which does not write (%v)", g, writers)
		}
	}
}

// R18. Run returns only after the waiter has exited, on a stop and on a cancel:
// with a held wait that lingers after its context ends, Run does not return
// until the wait is let go and the waiter's exit hook has run, and the waiter's
// report, which nobody reads, does not block it.
func TestR18RunWaitsForTheWaiter(t *testing.T) {
	for _, how := range []string{"stop", "cancel"} {
		t.Run(how, func(t *testing.T) {
			var exited atomic.Bool
			h := newRetainHarness(t, trueNow, sequencer.Hooks{WaiterExited: func() { exited.Store(true) }})
			lag := make(chan struct{})
			var once sync.Once
			letGo := func() { once.Do(func() { close(lag) }) }
			t.Cleanup(letGo)
			h.wait.lag = lag
			h.wait.held()
			h.begin()
			deadline := time.Now().Add(10 * time.Second)
			for h.wait.calls.Load() < 2 { // the probe, then the wait itself
				if time.Now().After(deadline) {
					t.Fatal("the waiter did not wait")
				}
				time.Sleep(time.Millisecond)
			}
			switch how {
			case "stop":
				go func() { _ = h.seq.Stop(context.Background()) }()
			case "cancel":
				h.cancel()
			}
			select {
			case err := <-h.runErr:
				h.runErr <- err
				t.Fatalf("Run returned (%v) while the waiter was still waiting", err)
			case <-time.After(200 * time.Millisecond):
			}
			if exited.Load() {
				t.Fatal("the waiter exited while its wait was held")
			}
			letGo()
			select {
			case err := <-h.runErr:
				h.runErr <- err
				if !exited.Load() {
					t.Fatal("Run returned before the waiter's exit hook ran")
				}
				if how == "stop" && err != nil {
					t.Errorf("Run after Stop: %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("Run did not return once the wait was let go: the waiter is blocked")
			}
		})
	}
}

// The configuration of the check is checked by New.
func TestNewChecksTheRetainConfig(t *testing.T) {
	r := &retainer{}
	w := (&fakeWait{}).Wait
	for _, tt := range []struct {
		name string
		c    sequencer.RetainConfig
	}{
		{"no store", sequencer.RetainConfig{Wait: w, Window: window}},
		{"no wait", sequencer.RetainConfig{Store: r, Window: window}},
		{"no window", sequencer.RetainConfig{Store: r, Wait: w}},
		{"negative offset", sequencer.RetainConfig{Store: r, Wait: w, Window: window, Offset: -1}},
		{"negative interval", sequencer.RetainConfig{Store: r, Wait: w, Window: window, Every: -1}},
	} {
		c := tt.c
		if _, err := sequencer.New(sequencer.Config{Store: newMem(t), Retain: &c}); err == nil {
			t.Errorf("%s: New accepted it", tt.name)
		}
	}
}

// A call the store refuses is not counted among the calls, the catch-ups or the
// capped advances.
func TestRefusedCallsAreNotCounted(t *testing.T) {
	h := newRetainHarness(t, trueNow, sequencer.Hooks{})
	h.retainTo(targetAt(trueNow).Add(-10 * day))
	h.begin()
	h.ret.setErr(fmt.Errorf("memstore: Retain: %w", store.ErrInvalid))
	h.fresh()
	h.check()
	h.wantCalls(targetAt(trueNow))
	if st := h.status(); st.Calls != 0 || st.CatchUps != 0 || st.Capped != 0 || st.StoppedBy == "" {
		t.Errorf("status %+v, want no call counted, retention stopped", st)
	}
}
