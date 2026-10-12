package sequencer_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/ingest/sequencer"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/replay/capture"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/pebblestore"
)

// The Background-mode pebblestore integration of the retention check (P1 to
// P5). The store is opened as serve opens it: Background retention that settles,
// L0 kept, L1 60 days behind L2.

// holdObserver records the phases of each pass, and, while armed, holds the
// store's goroutine in RewriteEnded: the rewriting is over and the marker is in
// its settling phase, with the store's lock free, so writes go on and the pass
// stays pending.
type holdObserver struct {
	mu                            sync.Mutex
	published, rewritten, settled []uint64
	hold, entered                 chan struct{}
	enterOnce                     *sync.Once
}

func (o *holdObserver) Published(gen uint64, _ time.Time) {
	o.mu.Lock()
	o.published = append(o.published, gen)
	o.mu.Unlock()
}

func (o *holdObserver) RewriteEnded(gen uint64, _ time.Time) {
	o.mu.Lock()
	o.rewritten = append(o.rewritten, gen)
	hold, entered, once := o.hold, o.entered, o.enterOnce
	o.mu.Unlock()
	if hold != nil {
		once.Do(func() { close(entered) })
		<-hold
	}
}

func (o *holdObserver) SettleEnded(gen uint64, _ time.Time) {
	o.mu.Lock()
	o.settled = append(o.settled, gen)
	o.mu.Unlock()
}

// arm makes the next RewriteEnded hold until release, and returns the channel
// closed when it holds. It is released when the test ends, before the store is
// closed, if the test has not.
func (o *holdObserver) arm(t *testing.T) <-chan struct{} {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.hold, o.entered, o.enterOnce = make(chan struct{}), make(chan struct{}), &sync.Once{}
	t.Cleanup(o.release)
	return o.entered
}

func (o *holdObserver) release() {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.hold != nil {
		close(o.hold)
		o.hold = nil
	}
}

func (o *holdObserver) phases() (published, rewritten, settled []uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return slices.Clone(o.published), slices.Clone(o.rewritten), slices.Clone(o.settled)
}

// chunkSample is the sample a pass takes after each chunk's commit, under the
// store's lock.
const chunkSample = "retain.chunk_hold_ns"

// countRecorder keeps the store's counts and samples. While holdUntilClosed
// names the store, the sample of a chunk's commit holds the pass, the store's
// lock held, until that store is marked closed, and a little longer, so that a
// Close stops the pass at the next chunk boundary.
type countRecorder struct {
	mu              sync.Mutex
	counts          map[string]int64
	samples         map[string][]int64
	holdUntilClosed atomic.Pointer[pebblestore.Store]
	entered         chan struct{}
	enterOnce       sync.Once
}

func newCountRecorder() *countRecorder {
	return &countRecorder{counts: map[string]int64{}, samples: map[string][]int64{}, entered: make(chan struct{})}
}

func (r *countRecorder) Count(name string, n int64) {
	r.mu.Lock()
	r.counts[name] += n
	r.mu.Unlock()
}

func (r *countRecorder) Sample(name string, v int64) {
	r.mu.Lock()
	r.samples[name] = append(r.samples[name], v)
	r.mu.Unlock()
	st := r.holdUntilClosed.Load()
	if name != chunkSample || st == nil {
		return
	}
	r.enterOnce.Do(func() { close(r.entered) })
	deadline := time.Now().Add(30 * time.Second)
	for !isClosed(st) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	// Close marks the store closed and then, at once, cancels the pass: the pass
	// is let go well after that.
	time.Sleep(50 * time.Millisecond)
}

func (r *countRecorder) count(name string) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.counts[name]
}

// recvPass is recv with room for a pass, its settle, or a Close to end on a
// busy machine: the settle alone may take up to its deadline of 120 s.
func recvPass[T any](t testing.TB, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(3 * time.Minute):
		t.Fatal("timed out waiting for a pass or a Close to end")
	}
	var zero T
	return zero
}

// isClosed says whether st is marked closed. A Write of nothing takes no lock.
func isClosed(st *pebblestore.Store) bool {
	return errors.Is(st.Write(context.Background(), nil), store.ErrClosed)
}

// openServeStore opens a pebblestore in dir as serve does.
func openServeStore(t *testing.T, dir string, obs pebblestore.RetainObserver, rec pebblestore.Recorder) *pebblestore.Store {
	t.Helper()
	o := pebblestore.DefaultOptions()
	o.Keep[0] = true // L0
	o.Offsets[catalog.L1-catalog.L0] = 60 * day
	o.Retention = pebblestore.Background
	o.Observer = obs
	if rec != nil {
		o.Recorder = rec
	}
	st, err := pebblestore.Open(dir, o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

// oldPods writes n pods directly, observed at tm with no TTL, each with a
// description of size bytes, so that a retention after tm keeps a baseline of
// each: enough of them, large enough, make a pass take several chunks.
func oldPods(t *testing.T, st *pebblestore.Store, n, size int, tm time.Time) {
	t.Helper()
	res := identity.NewResolver(catalog.Default())
	payload := bytes.Repeat([]byte("x"), size)
	var batch []store.Record
	for i := range n {
		id, err := res.Resolve(catalog.K8sPod, []identity.Attr{{Key: catalog.K8sPodUID, Value: fmt.Sprintf("old-%05d", i)}})
		if err != nil {
			t.Fatal(err)
		}
		batch = append(batch, store.Record{
			Layer: catalog.L2, Subject: store.EntitySubject(id.Fingerprint()), Producer: "k8sobjects/default",
			EventTime: tm, Seq: st.LastSeq() + uint64(len(batch)) + 1, Kind: lifecycle.Observe,
			Payload: payload, EventTimeBasis: store.BasisObserved,
		})
		if len(batch) == 2000 || i == n-1 {
			if err := st.Write(context.Background(), batch); err != nil {
				t.Fatal(err)
			}
			batch = nil
		}
	}
}

// horizons are the stored horizons of every layer.
func horizons(w sequencer.Writer) [4]store.Horizon {
	var out [4]store.Horizon
	for l := catalog.L0; l <= catalog.L3; l++ {
		out[l-catalog.L0] = w.LayerHorizon(l)
	}
	return out
}

// waitLog waits until the log holds a line that contains sub.
func (h *rh) waitLog(sub string) {
	h.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var seen []logLine
	for {
		seen = append(seen, h.logs.take()...)
		for _, l := range seen {
			if strings.Contains(l.msg, sub) {
				return
			}
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("no log line contains %q: %+v", sub, seen)
		}
		time.Sleep(time.Millisecond)
	}
}

// P1. Ingest during a pass: the same requests and the same retentions given to a
// sequencer over a Background pebblestore and to one over memstore answer the
// same, and the two stores answer every probe the same, before the retention,
// while the pass is pending (held after its rewriting, with writes going on),
// after it, and after a second retention.
func TestP1IngestDuringAPassMatchesMemstore(t *testing.T) {
	obs := &holdObserver{}
	st := openServeStore(t, t.TempDir(), obs, nil)
	start := trueNow.Add(-40 * day)
	hp := newRetainHarness(t, start, sequencer.Hooks{}, withStore(st, st.Instrument().WaitRetained)).begin()
	hm := newRetainHarness(t, start, sequencer.Hooks{}).begin()
	both := []*rh{hp, hm}

	var uids []string
	send := func(recs ...podAt) {
		t.Helper()
		var answers [2]string
		for i, h := range both {
			var lrs []capture.LogRecord
			for _, r := range recs {
				uid, lr := r(h.clock.now())
				lrs = append(lrs, lr)
				if i == 0 && !slices.Contains(uids, uid) {
					uids = append(uids, uid)
				}
			}
			partial, err := h.send(logs("", lrs...))
			answers[i] = fmt.Sprintf("%+v %v", partial, err)
		}
		if answers[0] != answers[1] {
			t.Fatalf("pebblestore answered %s, memstore %s", answers[0], answers[1])
		}
	}
	probe := func(when string) {
		t.Helper()
		ctx := context.Background()
		if hz, hzm := horizons(st), horizons(hm.store); hz != hzm {
			t.Fatalf("%s: horizons %v, memstore's %v", when, hz, hzm)
		}
		from := st.LayerHorizon(catalog.L2).Time
		if from.Before(store.MinEventTime) {
			from = store.MinEventTime
		}
		now := hp.clock.now()
		sc := store.Current(catalog.L2)
		for _, uid := range uids {
			fp := podFP(t, uid)
			type answer struct {
				ents, edges []store.Record
				alive       []bool
				nbrs        [][]store.Neighbor
			}
			read := func(s store.Store) (a answer) {
				var err error
				if a.ents, err = s.EntityWindow(ctx, fp, from, store.MaxEventTime, sc); err != nil {
					t.Fatalf("%s: %s: %v", when, uid, err)
				}
				if a.edges, err = s.Window(ctx, fp, store.Forward, from, store.MaxEventTime, sc); err != nil {
					t.Fatalf("%s: %s: %v", when, uid, err)
				}
				for _, at := range []time.Time{from, from.Add(day), now.Add(-time.Hour), now} {
					ok, err := s.Alive(ctx, fp, at, sc)
					if err != nil && !isQuarantine(err) {
						t.Fatalf("%s: %s: %v", when, uid, err)
					}
					a.alive = append(a.alive, ok)
					n, err := s.Neighbors(ctx, fp, store.Forward, at, sc)
					if err != nil && !isQuarantine(err) {
						t.Fatalf("%s: %s: %v", when, uid, err)
					}
					a.nbrs = append(a.nbrs, n)
				}
				return a
			}
			if p, m := read(st), read(hm.store.(store.Store)); !reflect.DeepEqual(p, m) {
				t.Fatalf("%s: %s: pebblestore answers %+v, memstore %+v", when, uid, p, m)
			}
		}
	}
	advance := func(d time.Duration) {
		for _, h := range both {
			h.advance(d)
		}
	}

	// 40 days of history: steady pods pulled each day, a new pod each day placed
	// on a node, and one removed every tenth day.
	for d := range 40 {
		recs := []podAt{steadyPod("steady-a"), steadyPod("steady-b"), placedPod(fmt.Sprintf("day-%02d", d))}
		if d%10 == 9 {
			recs = append(recs, removedPod(fmt.Sprintf("day-%02d", d-5)))
		}
		send(recs...)
		advance(day)
	}
	send(steadyPod("steady-a"), placedPod("fresh-0"))
	probe("before the retention")

	entered := obs.arm(t)
	for _, h := range both {
		h.check()
	}
	recv(t, entered)
	if st := hp.status(); st.PendingSince.IsZero() || st.Calls != 1 {
		t.Fatalf("status %+v, want one call, its pass pending", st)
	}
	probe("with the pass pending")
	advance(time.Minute)
	send(steadyPod("steady-a"), placedPod("during-0"), latePod("late-0", 35*day))
	advance(time.Minute)
	send(steadyPod("steady-b"), removedPod("day-37"), placedPod("during-1"))
	probe("ingest during the pass")

	obs.release()
	recvPass(t, hp.waited)
	if st := hp.status(); !st.PendingSince.IsZero() || st.LastSettleEnd.IsZero() {
		t.Fatalf("status %+v, want the pass ended", st)
	}
	probe("after the pass")

	advance(day)
	send(steadyPod("steady-a"), placedPod("next-day"))
	for _, h := range both {
		h.check()
	}
	recvPass(t, hp.waited)
	probe("after the second retention")
	if got := hp.ret.got(); len(got) != 2 {
		t.Fatalf("Retain called %v, want twice", got)
	}
	if _, rewritten, settled := obs.phases(); len(rewritten) != 2 || len(settled) != 2 {
		t.Errorf("rewrite ends %v, settles %v, want two of each", rewritten, settled)
	}
}

// podAt is a log record of a pod, made at the clock's instant, and its UID.
type podAt func(now time.Time) (uid string, lr capture.LogRecord)

// steadyPod is a pull, at the clock's instant, of a pod that runs throughout.
func steadyPod(uid string) podAt {
	return func(now time.Time) (string, capture.LogRecord) {
		return uid, pullPod(now, pod{uid, uid, "", time.Time{}, time.Time{}, "Running"})
	}
}

// placedPod is a pull of a new pod placed on a node, created and scheduled a
// minute before the clock's instant.
func placedPod(uid string) podAt {
	return func(now time.Time) (string, capture.LogRecord) {
		made := now.Add(-time.Minute)
		return uid, pullPod(now, pod{uid, uid, "node-1", made, made, "Running"})
	}
}

// removedPod is the watch event of a pod's deletion at the clock's instant.
func removedPod(uid string) podAt {
	return func(now time.Time) (string, capture.LogRecord) {
		return uid, watchPod(now, "DELETED", pod{uid, uid, "", time.Time{}, time.Time{}, "Succeeded"})
	}
}

// latePod is a pull observed ago before the clock's instant.
func latePod(uid string, ago time.Duration) podAt {
	return func(now time.Time) (string, capture.LogRecord) {
		return uid, pullPod(now.Add(-ago), pod{uid, uid, "", time.Time{}, time.Time{}, "Running"})
	}
}

func isQuarantine(err error) bool {
	var q *store.QuarantineError
	return errors.As(err, &q)
}

// P2. A pass the store resumes at open: the first process closes the store in
// the middle of the rewriting; the second opens it, and the pass resumes with
// no Published. The waiter keeps the pass pending, so a due check skips; when
// the pass ends, nothing is pending and the next check calls.
func TestP2ReopenMidPassKeepsPending(t *testing.T) {
	dir := t.TempDir()
	stored := targetAt(trueNow).Add(-day) // a check at trueNow is due

	rec := newCountRecorder()
	st := openServeStore(t, dir, nil, rec)
	// A chunk ends after 20 ms of work or 4 MiB of writes. Visiting a prefix
	// takes microseconds (about 3 measured, several times more under the race
	// detector), so the first chunk cannot finish a pass over this many: the
	// Close comes in the middle of the rewriting, which the reopen checks
	// (RewriteEnded is called only for a pass whose rewriting had not ended).
	oldPods(t, st, midPassPrefixes, 16, trueNow.Add(-40*day))
	if err := st.Instrument().WaitScanned(context.Background()); err != nil {
		t.Fatal(err)
	}
	rec.holdUntilClosed.Store(st)
	if err := st.Retain(context.Background(), stored); err != nil {
		t.Fatal(err)
	}
	recv(t, rec.entered) // the first chunk is committed, and the pass held
	closed := make(chan error, 1)
	go func() { closed <- st.Close() }()
	if err := recvPass(t, closed); err != nil {
		t.Fatalf("Close: %v", err)
	}

	obs := &holdObserver{}
	entered := obs.arm(t)
	rec2 := newCountRecorder()
	st2 := openServeStore(t, dir, obs, rec2)
	t.Cleanup(obs.release) // before the store closes, if the test fails
	if got := st2.LayerHorizon(catalog.L2).Time; !got.Equal(stored) {
		t.Fatalf("after the reopen L2's horizon is %s, want %s as published", got, stored)
	}
	h := newRetainHarness(t, trueNow, sequencer.Hooks{}, withStore(st2, st2.Instrument().WaitRetained))
	h.start()
	h.waitLog(msgResumed)
	began := time.Now()
	select { // the resumed pass ended its rewriting: Close stopped it in the middle
	case <-entered:
	case <-time.After(5 * time.Minute):
		t.Fatal("the resumed pass did not end its rewriting")
	}
	t.Logf("the resumed pass ended its rewriting %s after the reopen", time.Since(began))
	if published, _, _ := obs.phases(); len(published) != 0 {
		t.Fatalf("the resumed pass was published again: %v", published)
	}
	if n := rec2.count("retain.resumed"); n != 1 {
		t.Fatalf("retain.resumed %d, want 1", n)
	}
	h.fresh()
	h.check()
	h.wantCalls()
	if st := h.status(); st.Skipped != 1 || st.PendingSince.IsZero() {
		t.Fatalf("status %+v, want the due check skipped while the resumed pass is pending", st)
	}
	obs.release()
	recvPass(t, h.waited)
	if st := h.status(); !st.PendingSince.IsZero() || st.LastSettleEnd.IsZero() {
		t.Fatalf("status %+v, want the resumed pass ended", st)
	}
	h.waitLog("resumed at open has ended")
	h.check()
	h.wantCalls(targetAt(trueNow))
	recvPass(t, h.waited)
}

// P3. Close during the settle, then reopen: the store settles again, from the
// marker, and the horizons are those published.
func TestP3CloseMidSettleSettlesAgain(t *testing.T) {
	dir := t.TempDir()
	obs := &holdObserver{}
	st := openServeStore(t, dir, obs, nil)
	oldPods(t, st, 100, 64, trueNow.Add(-40*day))
	if err := st.Instrument().WaitScanned(context.Background()); err != nil {
		t.Fatal(err)
	}
	entered := obs.arm(t)
	if err := st.Retain(context.Background(), targetAt(trueNow)); err != nil {
		t.Fatal(err)
	}
	recv(t, entered) // the rewriting is over; the settle has not begun
	published := horizons(st)
	closed := make(chan error, 1)
	go func() { closed <- st.Close() }()
	deadline := time.Now().Add(10 * time.Second)
	for !isClosed(st) {
		if time.Now().After(deadline) {
			t.Fatal("the store is not marked closed")
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond) // Close cancels the pass at once after marking it
	obs.release()
	if err := recvPass(t, closed); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, _, settled := obs.phases(); len(settled) != 0 {
		t.Fatalf("the settle ended before the Close: %v", settled)
	}

	obs2 := &holdObserver{}
	rec2 := newCountRecorder()
	st2 := openServeStore(t, dir, obs2, rec2)
	if got := horizons(st2); got != published {
		t.Fatalf("after the reopen the horizons are %v, want %v as published", got, published)
	}
	if err := st2.Instrument().WaitRetained(context.Background()); err != nil {
		t.Fatal(err)
	}
	pub, rewritten, settled := obs2.phases()
	if len(pub) != 0 || len(rewritten) != 0 || len(settled) != 1 || rec2.count("retain.resumed") != 1 {
		t.Fatalf("after the reopen: published %v, rewrite ends %v, settles %v, resumed %d; want one settle, resumed",
			pub, rewritten, settled, rec2.count("retain.resumed"))
	}
	if got := horizons(st2); got != published {
		t.Fatalf("after the settle the horizons are %v, want %v", got, published)
	}
}

// inFlight wraps a store and counts its calls in flight: the writes, the
// retentions and the waits.
type inFlight struct {
	*pebblestore.Store
	n atomic.Int64
}

func (s *inFlight) Write(ctx context.Context, b []store.Record) error {
	s.n.Add(1)
	defer s.n.Add(-1)
	return s.Store.Write(ctx, b)
}

func (s *inFlight) Retain(ctx context.Context, h time.Time) error {
	s.n.Add(1)
	defer s.n.Add(-1)
	return s.Store.Retain(ctx, h)
}

// Wait is the store's wait, which lingers 200 ms once its context has ended
// before it returns, as a wait slow to notice would: Run must wait for it all
// the same.
func (s *inFlight) Wait(ctx context.Context) error {
	s.n.Add(1)
	defer s.n.Add(-1)
	err := s.Instrument().WaitRetained(ctx)
	if ctx.Err() != nil {
		time.Sleep(200 * time.Millisecond)
	}
	return err
}

// P4. A graceful stop during a pass: Run returns once the waiter has (its exit
// hook has run), with no call of the store in flight; Close then stops the pass
// within the bound (one chunk or one settle range: seconds), the marker stays,
// and the next open resumes the pass.
func TestP4GracefulStopDuringAPass(t *testing.T) {
	dir := t.TempDir()
	obs := &holdObserver{}
	st := openServeStore(t, dir, obs, nil)
	oldPods(t, st, 100, 64, trueNow.Add(-40*day))
	fl := &inFlight{Store: st}
	var exited atomic.Bool
	h := newRetainHarness(t, trueNow, sequencer.Hooks{WaiterExited: func() { exited.Store(true) }}, withStore(fl, fl.Wait)).begin()
	exited.Store(false) // the boot waiter's
	entered := obs.arm(t)
	h.fresh()
	h.check()
	h.wantCalls(targetAt(trueNow))
	recv(t, entered)
	if st := h.status(); st.PendingSince.IsZero() {
		t.Fatal("the pass is not pending")
	}

	h.stop()
	if !exited.Load() {
		t.Fatal("Run returned before the waiter exited")
	}
	if n := fl.n.Load(); n != 0 {
		t.Fatalf("%d calls of the store in flight after Run returned", n)
	}
	begin := time.Now()
	closed := make(chan error, 1)
	go func() { closed <- st.Close() }()
	deadline := time.Now().Add(10 * time.Second)
	for !isClosed(st) {
		if time.Now().After(deadline) {
			t.Fatal("the store is not marked closed")
		}
		time.Sleep(time.Millisecond)
	}
	obs.release()
	if err := recvPass(t, closed); err != nil {
		t.Fatalf("Close: %v", err)
	}
	took := time.Since(begin)
	t.Logf("Close took %s", took)
	if took > 10*time.Second {
		t.Errorf("Close took %s, over the bound of seconds", took)
	}

	obs2 := &holdObserver{}
	rec2 := newCountRecorder()
	st2 := openServeStore(t, dir, obs2, rec2)
	if err := st2.Instrument().WaitRetained(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, _, settled := obs2.phases(); len(settled) != 1 || rec2.count("retain.resumed") != 1 {
		t.Fatalf("the next open: settles %v, resumed %d; want the pass resumed and settled", settled, rec2.count("retain.resumed"))
	}
}

// P5. A real retention, then a request with a record before L2's horizon: that
// record goes to the partial success, and the rest are stored.
func TestP5LateRecordAfterARetention(t *testing.T) {
	st := openServeStore(t, t.TempDir(), nil, nil)
	h := newRetainHarness(t, trueNow, sequencer.Hooks{}, withStore(st, st.Instrument().WaitRetained)).begin()
	h.fresh()
	h.check()
	recvPass(t, h.waited)
	target := targetAt(trueNow)
	if got := st.LayerHorizon(catalog.L2).Time; !got.Equal(target) {
		t.Fatalf("L2's horizon %s, want %s", got, target)
	}
	late := pod{"late-1", "late-1", "", time.Time{}, time.Time{}, "Running"}
	on := pod{"on-1", "on-1", "", time.Time{}, time.Time{}, "Running"}
	p, err := h.send(logs("", pullPod(target.Add(-time.Hour), late), pullPod(trueNow, on)))
	if err != nil || p.Rejected != 1 || !strings.Contains(p.Message, "retention horizon") {
		t.Fatalf("partial %+v, error %v; want the late record rejected for the horizon", p, err)
	}
	ctx := context.Background()
	got, err := st.EntityWindow(ctx, podFP(t, "on-1"), target, store.MaxEventTime, store.Current(catalog.L2))
	if err != nil || len(got) == 0 {
		t.Fatalf("the record on time is not stored: %v %v", got, err)
	}
	got, err = st.EntityWindow(ctx, podFP(t, "late-1"), target, store.MaxEventTime, store.Current(catalog.L2))
	if err != nil || len(got) != 0 {
		t.Fatalf("the late record is stored: %v %v", got, err)
	}
}
