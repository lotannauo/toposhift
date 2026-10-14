package sequencer_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/ingest/otlphttp"
	"github.com/lotannauo/toposhift/internal/ingest/resume"
	"github.com/lotannauo/toposhift/internal/ingest/sequencer"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/replay/capture"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// dstFailures collects the failed checks of a seed. The first few of each check
// are kept with their detail, and the rest are counted.
type dstFailures struct {
	list   []string
	counts map[string]int
}

func (f *dstFailures) add(check, format string, a ...any) {
	if f.counts == nil {
		f.counts = map[string]int{}
	}
	f.counts[check]++
	if f.counts[check] <= 4 {
		f.list = append(f.list, check+": "+fmt.Sprintf(format, a...))
	}
}

func (f *dstFailures) failed(check string) bool { return f.counts[check] > 0 }

func (f *dstFailures) any() bool { return len(f.counts) > 0 }

// dstAnswer is the collector's view of one delivery of a request.
type dstAnswer struct {
	id, attempt int
	at          time.Time
	code        int
	rejected    int64
	dup         bool
}

func (a dstAnswer) String() string {
	return fmt.Sprintf("req %d attempt %d dup %v at %s: %d rejected %d", a.id, a.attempt, a.dup, a.at.Sub(t0), a.code, a.rejected)
}

// dstCode is the HTTP status the receiver would give an answer.
func dstCode(err error) int {
	var refused *otlphttp.RefusedError
	var unavailable *otlphttp.UnavailableError
	switch {
	case err == nil:
		return 200
	case errors.As(err, &refused):
		return 400
	case errors.Is(err, otlphttp.ErrBusy), errors.As(err, &unavailable):
		return 503
	}
	return 500
}

type dstItem struct {
	rq      *dstReq
	attempt int
	data    capture.LogsData
}

// dstSilence is the resume count at the start of a silence that keeps the slot.
type dstSilence struct {
	snap resume.Stats
	done bool
}

type dstResend struct {
	rq      *dstReq
	attempt int
}

// dstMaxResends is how many times the collector sends a request again after a 503.
const dstMaxResends = 3

// dstRun is one run of a seed's cluster through the sequencer on a memstore: the
// faulted run when it has a plan, the control otherwise: no fault, and the requests
// the faulted run had answered 200, each at its scheduled instant.
type dstRun struct {
	t      *testing.T
	sd     *dstSchedule
	world  *dstWorld
	plan   *dstPlan
	accept map[int]bool
	off    bool // no lookup: the resume step is off
	fail   *dstFailures
	// report says the run is of class R faults: its Dropped count is reported.
	report bool

	faults []dstFault
	clock  *fakeClock
	mem    *memstore.Store
	lk     *dstLookups
	h      *harness
	fs     *fakeStore
	byTime map[time.Time][]*dstReq

	// mu guards what the loop's hooks read and write.
	mu         sync.Mutex
	inTick     bool
	w3         int
	armedWrite []int
	panicBody  *capture.AnyValue
	panicIdx   int
	events     map[dstClass]int
	batchAt    map[uint64]time.Time

	prior       [][]store.Record
	priorStats  resume.Stats
	answers     []dstAnswer
	resends     []dstResend
	accepted    map[int]bool
	shuffled    map[int]capture.LogsData
	resetsStep  int
	cleanStats  resume.Stats
	silences    map[int]*dstSilence
	lookupFault int
	w2Fired     bool
	w2Hits      int
	aborts      int
	finished    bool
	w1Seen      map[int]bool
}

func newDSTRun(t *testing.T, sd *dstSchedule, world *dstWorld, plan *dstPlan, accept map[int]bool, fail *dstFailures) *dstRun {
	r := &dstRun{
		t: t, sd: sd, world: world, plan: plan, accept: accept, fail: fail,
		faults: slices.Clone(sd.faults), clock: newClock(t0),
		events: map[dstClass]int{}, batchAt: map[uint64]time.Time{},
		accepted: map[int]bool{}, shuffled: map[int]capture.LogsData{},
		silences: map[int]*dstSilence{}, w1Seen: map[int]bool{}, lookupFault: -1, w3: -1,
	}
	if plan == nil {
		r.faults = nil
	}
	r.mem = newMem(t)
	t.Cleanup(func() { _ = r.mem.Close() })
	r.lk = &dstLookups{m: r.mem}
	r.byTime = map[time.Time][]*dstReq{}
	for _, rq := range world.reqs {
		switch {
		case plan == nil && accept[rq.id]:
			r.byTime[rq.at] = append(r.byTime[rq.at], rq)
		case plan != nil && !plan.skip[rq.id]:
			d := plan.deliverAt(rq)
			r.byTime[d] = append(r.byTime[d], rq)
		}
	}
	return r
}

// fire notes a fault took effect. It holds mu.
func (r *dstRun) fire(i int) {
	f := &r.faults[i]
	f.fired, f.firedAt = true, r.clock.now()
	r.events[f.class]++
}

// start makes a sequencer over the memstore.
func (r *dstRun) start() {
	r.fs = &fakeStore{inner: r.mem}
	r.fs.inject = r.inject
	r.h = newHarness(r.t, r.fs,
		withConfig(func(c *sequencer.Config) {
			c.Now = r.clock.Now
			c.Logger = slog.New(slog.DiscardHandler)
			c.MergeWait = time.Hour
			if !r.off {
				c.Lookup = r.lk.open
			}
			if r.sd.configured || r.plan == nil {
				c.ClusterName, c.ClusterNameSet = dstCluster, true
			}
		}),
		withHooks(sequencer.Hooks{
			BeforeWrite: func(batch []store.Record) {
				if len(batch) > 0 {
					r.mu.Lock()
					r.batchAt[batch[0].Seq] = r.clock.now()
					r.mu.Unlock()
				}
			},
			BeforeTranslate: r.beforeTranslate,
			AfterTick: func() {
				r.mu.Lock()
				r.inTick = false
				r.mu.Unlock()
			},
		}))
	r.h.clock = r.clock
	r.h.start()
}

func (r *dstRun) beforeTranslate(_ string, lr capture.LogRecord) {
	r.mu.Lock()
	hit := r.panicBody != nil && lr.Body == r.panicBody
	if hit {
		r.panicBody = nil
		r.fire(r.panicIdx)
	}
	r.mu.Unlock()
	if hit {
		panic("a translation panic injected by the simulation")
	}
}

const dstObjects lifecycle.Producer = "k8sobjects/" + dstCluster

func dstHoldsObjects(batch []store.Record) bool {
	return slices.ContainsFunc(batch, func(rec store.Record) bool { return rec.Producer == dstObjects })
}

// dstHoldsBackdated says the batch holds a first sighting of the objects receiver
// that is timed at least three minutes before now (by the producer's clock).
func dstHoldsBackdated(batch []store.Record, now time.Time, k time.Duration) bool {
	return slices.ContainsFunc(batch, func(rec store.Record) bool {
		return rec.Producer == dstObjects && rec.Kind == lifecycle.Observe && rec.Through.After(rec.EventTime) &&
			!rec.EventTime.Add(-k).After(now.Add(-3*time.Minute))
	})
}

// inject is the fakeStore's decision for each write. A fault armed for the step
// takes the writes of its requests; W3 takes the first write of a flush.
func (r *dstRun) inject(_ int, batch []store.Record) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(batch) == 0 {
		return true, nil
	}
	now := r.clock.now()
	if r.inTick {
		if i := r.w3; i >= 0 && !r.faults[i].fired {
			r.fire(i)
			return false, errInjected
		}
		return true, nil
	}
	for _, i := range r.armedWrite {
		f := &r.faults[i]
		switch f.class {
		case dstT3:
			// Every write that holds a record of the faulted request is refused, so
			// that its own rerun is refused too.
			if dstHoldsObjects(batch) {
				if !f.fired {
					r.fire(i)
				}
				return false, fmt.Errorf("injected: %w", store.ErrInvalid)
			}
		case dstT4:
			if !f.fired {
				r.fire(i)
				return false, fmt.Errorf("injected: %w", store.ErrBeforeHorizon)
			}
		case dstT5:
			if !f.fired && dstHoldsBackdated(batch, now, r.world.k) {
				r.fire(i)
				return false, errInjected
			}
		case dstT6:
			if !f.fired {
				r.fire(i)
				return true, errInjected
			}
		}
	}
	return true, nil
}

// restart stops the sequencer (or abandons it, with no final flush) and starts
// another over the same memstore.
func (r *dstRun) restart(crash bool) {
	r.t.Helper()
	if crash {
		r.h.cancel()
		if err := <-r.h.runErr; !errors.Is(err, context.Canceled) {
			r.t.Fatalf("Run after the crash: %v", err)
		}
		r.h.runErr <- nil
	} else {
		r.h.stop()
	}
	r.collect()
	r.start()
}

func (r *dstRun) collect() {
	r.fs.mu.Lock()
	r.prior = append(r.prior, r.fs.committed...)
	r.fs.mu.Unlock()
	s, _ := r.h.seq.ResumeStats()
	r.priorStats.Add(s)
}

func (r *dstRun) stats() resume.Stats {
	s, _ := r.h.seq.ResumeStats()
	s.Add(r.priorStats)
	return s
}

func (r *dstRun) batches() [][]store.Record {
	if r.finished {
		return slices.Clone(r.prior)
	}
	r.fs.mu.Lock()
	defer r.fs.mu.Unlock()
	return append(slices.Clone(r.prior), r.fs.committed...)
}

func (r *dstRun) records() []store.Record {
	var out []store.Record
	for _, b := range r.batches() {
		out = append(out, b...)
	}
	return out
}

// run drives the clock in steps of 30 seconds: the faults that restart the
// sequencer, the tick, and then the requests due, as a collector would send them.
func (r *dstRun) run() {
	r.t.Helper()
	r.start()
	for n := 0; n <= int(dstRunLen/dstStep); n++ {
		tm := t0.Add(time.Duration(n) * dstStep)
		r.clock.set(tm)
		r.instantFaults(tm)
		r.takeSilenceSnapshots(tm)
		r.armLookups(tm)
		r.tickBegin(tm)
		r.deliverStep(tm)
	}
	r.h.stop()
	r.collect()
	r.finished = true
	cur := r.priorStats
	for i := range r.faults {
		f := r.faults[i]
		if r.plan != nil && !f.fired && dstMustFire(f.class) {
			r.fail.add("I3", "the fault %s never took effect", f)
		}
		if sl, ok := r.silences[i]; ok && !sl.done {
			r.checkSilence(f, sl.snap, cur)
		}
	}
	for _, rs := range r.resends {
		r.fail.add("I3", "request %d was not answered 200 by the end of the run", rs.rq.id)
	}
}

// dstMustFire says a fault of the class has to be seen to take effect.
func dstMustFire(c dstClass) bool {
	switch c {
	case dstT1, dstT2, dstT3, dstT4, dstT5, dstT6, dstT7, dstT8, dstW2, dstW3:
		return true
	}
	return false
}

func (r *dstRun) instantFaults(tm time.Time) {
	for i := range r.faults {
		f := &r.faults[i]
		if f.fired || tm.Before(f.start) || (f.class != dstT1 && f.class != dstT2) {
			continue
		}
		r.mu.Lock()
		r.fire(i)
		r.mu.Unlock()
		r.resetsStep++
		r.restart(f.class == dstT2)
	}
}

// takeSilenceSnapshots notes the resume counts at the start of a silence that
// keeps the slot.
func (r *dstRun) takeSilenceSnapshots(tm time.Time) {
	for i := range r.faults {
		f := r.faults[i]
		if f.class == dstW1 && !r.w1Seen[i] && !tm.Before(f.start) {
			// The slot is freed: the first requests after it are marked.
			r.w1Seen[i] = true
			r.resetsStep++
		}
		if _, ok := r.silences[i]; ok || tm.Before(f.start) || (f.class != dstT9 && f.class != dstT10) {
			continue
		}
		r.silences[i] = &dstSilence{snap: r.cleanStats}
	}
}

// checkSilence is I4's clause: after a silence that keeps the slot, nothing is
// raised or changed until the translators are made anew. The translators keep
// their state, so the first requests after it carry no mark at all.
func (r *dstRun) checkSilence(f dstFault, snap, cur resume.Stats) {
	if cur.Raised != snap.Raised || cur.Changed != snap.Changed {
		r.fail.add("I4", "after the %s the resume step raised %d and changed %d records, where the slot is kept and nothing should be marked",
			f, cur.Raised-snap.Raised, cur.Changed-snap.Changed)
	}
}

func (r *dstRun) armLookups(tm time.Time) {
	if r.lookupFault >= 0 || r.lk.isArmed() {
		return
	}
	for i := range r.faults {
		f := &r.faults[i]
		if (f.class != dstT8 && f.class != dstW2) || f.fired || tm.Before(f.start) {
			continue
		}
		modes := map[dstClass][]dstLookupMode{
			dstT8: {dstOpenAbort, dstLastAbort, dstLastCancel}, dstW2: {dstOpenError, dstLastError},
		}
		r.lk.arm(modes[f.class][f.mode])
		r.lookupFault = i
		return
	}
}

// tickBegin sends the flush tick. The loop stays in the tick's last seam until
// release, so that requests can be queued behind it.
func (r *dstRun) tickBegin(tm time.Time) {
	r.mu.Lock()
	r.inTick, r.w3 = true, -1
	for i := range r.faults {
		if f := r.faults[i]; f.class == dstW3 && !f.fired && !tm.Before(f.start) {
			r.w3 = i
			break
		}
	}
	r.mu.Unlock()
	r.h.tickCh <- tm
}

func (r *dstRun) release() { <-r.h.ticked }

func (r *dstRun) dataOf(rq *dstReq) capture.LogsData {
	if r.plan == nil || !r.plan.shuffle[rq.id] {
		return rq.data
	}
	if d, ok := r.shuffled[rq.id]; ok {
		return d
	}
	d := dstShuffled(rq.data, r.sd.draw, rq.id)
	r.shuffled[rq.id] = d
	return d
}

type dstStepAnswer struct {
	it       dstItem
	code     int
	group    int
	rejected int64
	dup      bool
}

func (r *dstRun) deliverStep(tm time.Time) {
	var items []dstItem
	for _, rs := range r.resends {
		items = append(items, dstItem{rs.rq, rs.attempt, r.dataOf(rs.rq)})
	}
	r.resends = nil
	for _, rq := range r.byTime[tm] {
		items = append(items, dstItem{rq, 0, r.dataOf(rq)})
	}
	group := r.plan != nil && !r.sd.noGroups && len(items) == 2 && items[0].attempt == 0 && items[1].attempt == 0 &&
		items[0].rq.kind == dstPullReq && items[1].rq.kind == dstReportReq &&
		items[0].rq.pull == items[1].rq.pull && r.sd.grouped[items[0].rq.pull]
	r.armStep(tm, items)
	var ans []dstStepAnswer
	if group {
		order := items
		if r.sd.reportFirst[items[0].rq.pull] {
			order = []dstItem{items[1], items[0]}
		}
		chans := make([]<-chan answer, len(order))
		for i, it := range order {
			chans[i] = r.h.sendAsync(context.Background(), it.data)
			r.h.waitQueued(i + 1)
		}
		r.release()
		for i, it := range order {
			a := recv(r.t, chans[i])
			ans = append(ans, r.record(tm, it, a.p, a.err, len(order), false))
		}
		for _, it := range order {
			ans = r.maybeDup(tm, it, ans)
		}
	} else {
		r.release()
		for _, it := range items {
			p, err := r.h.send(it.data)
			ans = append(ans, r.record(tm, it, p, err, 1, false))
			ans = r.maybeDup(tm, it, ans)
		}
	}
	r.endDeliver(tm, ans)
}

// maybeDup sends a request again right after its 200, if the plan says so.
func (r *dstRun) maybeDup(tm time.Time, it dstItem, ans []dstStepAnswer) []dstStepAnswer {
	if r.plan == nil || it.attempt != 0 || !r.plan.dup[it.rq.id] {
		return ans
	}
	for _, a := range ans {
		if a.it.rq == it.rq && !a.dup && a.code != 200 {
			return ans
		}
	}
	r.mu.Lock()
	r.events[dstT11]++
	r.mu.Unlock()
	p, err := r.h.send(it.data)
	return append(ans, r.record(tm, it, p, err, 1, true))
}

func (r *dstRun) record(tm time.Time, it dstItem, p otlphttp.Partial, err error, group int, dup bool) dstStepAnswer {
	a := dstAnswer{id: it.rq.id, attempt: it.attempt, at: tm, code: dstCode(err), rejected: p.Rejected, dup: dup}
	r.answers = append(r.answers, a)
	return dstStepAnswer{it: it, code: a.code, group: group, rejected: a.rejected, dup: dup}
}

// armStep arms the faults that take the writes or the translation of a step that
// delivers a pull and nothing but reports besides.
func (r *dstRun) armStep(tm time.Time, items []dstItem) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.armedWrite, r.panicBody = nil, nil
	var pull *dstItem
	for i := range items {
		switch items[i].rq.kind {
		case dstPullReq:
			if pull != nil {
				return
			}
			pull = &items[i]
		case dstWatchReq:
			return
		}
	}
	if pull == nil || r.plan == nil {
		return
	}
	for i := range r.faults {
		f := &r.faults[i]
		if f.fired || tm.Before(f.start) {
			continue
		}
		switch f.class {
		case dstT3, dstT4, dstT5, dstT6:
			r.armedWrite = append(r.armedWrite, i)
		case dstT7:
			r.panicBody, r.panicIdx = pull.data.ResourceLogs[0].ScopeLogs[0].LogRecords[0].Body, i
		}
	}
}

// endDeliver judges the answers of a step, keeps what is accepted, and queues
// the 503s to be sent again.
func (r *dstRun) endDeliver(tm time.Time, ans []dstStepAnswer) {
	r.mu.Lock()
	ev := r.events
	r.events = map[dstClass]int{}
	r.armedWrite, r.panicBody = nil, nil
	r.mu.Unlock()
	aborts := 0
	for _, fire := range r.lk.take() {
		switch {
		case fire.abort:
			aborts++
		default:
			r.w2Fired = true
			r.w2Hits += fire.hits
		}
		if r.lookupFault >= 0 {
			f := &r.faults[r.lookupFault]
			f.fired, f.firedAt = true, tm
		}
		r.lookupFault = -1
	}
	r.aborts += aborts
	r.resetsStep += ev[dstT3] + ev[dstT4] + ev[dstT5] + ev[dstT7] + aborts

	var n400, n503, nOther int
	group := 1
	for _, a := range ans {
		group = max(group, a.group)
		switch a.code {
		case 200:
		case 400:
			n400++
		case 503:
			n503++
		default:
			nOther++
		}
	}
	want400 := ev[dstT3] + ev[dstT7]
	want503 := (ev[dstT5] + aborts) * group
	if n400 != want400 || n503 != want503 || nOther != 0 {
		r.fail.add("I3", "at %s the answers are %d refused (400), %d busy (503) and %d others, want %d, %d and 0 (faults that took effect: %v, lookup aborts %d); the answers: %v",
			tm.Sub(t0), n400, n503, nOther, want400, want503, ev, aborts, r.lastAnswers(len(ans)))
	}
	if r.plan == nil && (n400 != 0 || n503 != 0) {
		r.fail.add("I3", "the control was not answered 200 at %s: %v", tm.Sub(t0), r.lastAnswers(len(ans)))
	}
	for _, a := range ans {
		rq := a.it.rq
		switch a.code {
		case 200:
			if a.rejected != 0 && !r.report {
				r.fail.add("I3", "request %d was answered 200 with %d records rejected", rq.id, a.rejected)
			}
			r.accepted[rq.id] = true
		case 503:
			if a.it.attempt >= dstMaxResends {
				r.fail.add("I3", "the collector gave up on request %d after %d resends", rq.id, dstMaxResends)
				continue
			}
			r.resends = append(r.resends, dstResend{rq, a.it.attempt + 1})
		}
	}
	r.checkDurability(ans)

	cur := r.stats()
	if r.resetsStep > 0 {
		for i, sl := range r.silences {
			if !sl.done {
				r.checkSilence(r.faults[i], sl.snap, r.cleanStats)
				sl.done = true
			}
		}
	} else {
		r.cleanStats = cur
	}
	r.resetsStep = 0
}

func (r *dstRun) lastAnswers(n int) []string {
	var out []string
	for _, a := range r.answers[max(len(r.answers)-n, 0):] {
		out = append(out, a.String())
	}
	return out
}

// checkDurability is I1: a pull answered 200 is in the store. Read at once at the
// latest token, every pod the pull listed and its node are alive a minute after the
// observation, and the pod holds its edge to the node.
func (r *dstRun) checkDurability(ans []dstStepAnswer) {
	ctx := context.Background()
	scope := store.Current(catalog.L2)
	for _, a := range ans {
		rq := a.it.rq
		if rq.kind != dstPullReq || a.code != 200 || a.rejected != 0 {
			continue
		}
		at := rq.at.Add(time.Minute)
		for _, l := range rq.listed {
			if ok, err := r.mem.Alive(ctx, l.pod, at, scope); err != nil || !ok {
				r.fail.add("I1", "the pull of request %d was answered 200 and pod %s is not alive at %s (error %v)", rq.id, r.world.nameOf(l.pod), at.Sub(t0), err)
			}
			if ok, err := r.mem.Alive(ctx, l.node, at, scope); err != nil || !ok {
				r.fail.add("I1", "the pull of request %d was answered 200 and node %s is not alive at %s (error %v)", rq.id, r.world.nameOf(l.node), at.Sub(t0), err)
			}
			ns, err := r.mem.Neighbors(ctx, l.pod, store.Forward, at, scope)
			if err != nil || !slices.ContainsFunc(ns, func(n store.Neighbor) bool {
				return n.Peer == l.node && n.Relation == catalog.ScheduledOn
			}) {
				r.fail.add("I1", "the pull of request %d was answered 200 and pod %s holds no scheduled_on edge to its node at %s (error %v)", rq.id, r.world.nameOf(l.pod), at.Sub(t0), err)
			}
		}
	}
}
