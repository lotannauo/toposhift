package sequencer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/store"
)

// DefaultRetainEvery is the interval of the retention check.
const DefaultRetainEvery = 10 * time.Minute

// The fixed numbers of retention. See "Retention" in the package documentation.
const (
	// retainDay is the cadence: a retention is due once the target is a day past
	// the stored horizon, and a capped advance is made at most once per day of
	// uptime.
	retainDay = 24 * time.Hour
	// retainCapStep is how far a capped advance moves the stored horizon, and how
	// far the target may be past it for a call to go there whatever the agreement.
	retainCapStep = 2 * retainDay
	// clockTolerance is how far the projection of the newest event time stamped by
	// another machine's clock may be from the ingest clock while they agree.
	clockTolerance = retainDay
	// disagreeWarnFor is how long a disagreement is logged at warning level before
	// it is logged at error level: a replay of old records ends within hours.
	disagreeWarnFor = retainDay
	// pendingErrorAfter is how long a pass may stay pending before a skipped check
	// also logs at error level: one twelfth of the daily cadence.
	pendingErrorAfter = 2 * time.Hour
)

// referenceLayer is the layer whose stored horizon says when a retention is due.
const referenceLayer = catalog.L2

// Retainer is what the retention check needs of a store.
type Retainer interface {
	Retain(ctx context.Context, horizon time.Time) error
}

// RetainConfig configures the retention check of a [Sequencer]. See "Retention"
// in the package documentation.
type RetainConfig struct {
	// Store is retained by the sequencer's loop alone, between groups and flushes.
	Store Retainer
	// Wait returns when the store has no retention pass pending, with nil, or with
	// the error that stopped the pass, an error wrapping [store.ErrClosed] when the
	// store is closed with the pass pending, or ctx's error. It must return nil at
	// once when no pass is pending, and must return when ctx ends.
	Wait func(ctx context.Context) error
	// Window is W, the history the reference layer (L2) keeps behind the ingest
	// clock.
	Window time.Duration
	// Offset is the retention offset of the reference layer in the store's
	// options, so that its stored horizon plus Offset is the horizon last asked of
	// Retain.
	Offset time.Duration
	// Every is the interval of the check; [DefaultRetainEvery] when zero.
	Every time.Duration
	// Tick returns the channel of the check's tick, called once with Every;
	// time.Tick when nil.
	Tick func(d time.Duration) <-chan time.Time
	// Uptime is the monotonic time since start, which a change of the ingest clock
	// does not move; time.Since of the instant New ran when nil.
	Uptime func() time.Duration
}

// Agreement says whether the newest event time stamped by another machine's
// clock agrees with the ingest clock. See "Retention" in the package
// documentation.
type Agreement uint8

const (
	// AgreementUnknown means no batch since start carried a record stamped by
	// another machine's clock.
	AgreementUnknown Agreement = iota
	// Agree means the newest such time, moved forward by the monotonic time since
	// its batch, is within a day of the ingest clock.
	Agree
	// Disagree means it is more than a day away.
	Disagree
)

func (a Agreement) String() string {
	switch a {
	case Agree:
		return "agree"
	case Disagree:
		return "disagree"
	}
	return "unknown"
}

// RetentionStatus is a snapshot of the retention check, published by the loop.
type RetentionStatus struct {
	// CheckedAt is the ingest clock's instant the snapshot was taken at.
	CheckedAt time.Time
	// Horizons are the stored horizons of L0 to L3; zero for a layer never
	// retained.
	Horizons [4]time.Time
	// Agreement is the agreement at CheckedAt.
	Agreement Agreement
	// PendingSince is when the sequencer began waiting for a pass, while one is
	// pending; zero otherwise.
	PendingSince time.Time
	// LastSettleEnd is when the sequencer learned that the last pass it waited for
	// had ended, settle included; zero before.
	LastSettleEnd time.Time
	// StoppedBy is why retention is stopped until a restart; empty while it runs.
	StoppedBy string
	// The counters: the calls of Retain the store took (a refused call is not
	// counted), the due checks skipped because a pass was pending, and, among the
	// calls, the capped advances and the catch-ups.
	Calls, Skipped, Capped, CatchUps uint64
}

// retainKind is why a call of Retain is made.
type retainKind uint8

const (
	retainDaily retainKind = iota
	retainFirst
	retainCatchUp
	retainCapped
)

// retention is the state of the retention check. Everything but status belongs
// to the loop.
type retention struct {
	cfg     RetainConfig
	results chan waitResult // buffered: the waiter never blocks on its report
	wg      sync.WaitGroup
	wctx    context.Context
	status  atomic.Pointer[RetentionStatus]

	// The agreement data: the newest qualifying event time of the latest batch
	// that had one, and the Uptime of that batch.
	have   bool
	newest time.Time
	seen   time.Duration
	// disagreeing and disagreeSince: whether the last check found a
	// disagreement, and the Uptime of the check that found it first.
	disagreeing   bool
	disagreeSince time.Duration

	pending        bool
	pendingSince   time.Time
	pendingSinceUp time.Duration

	// capped and cappedAt: whether a capped advance was made since start, and the
	// Uptime of the last.
	capped   bool
	cappedAt time.Duration

	stopped       error // why retention is stopped until a restart
	ended         bool  // the store or the context ended: nothing more is retained
	lastSettleEnd time.Time

	calls, skipped, cappedN, catchUps uint64
}

// waitResult is the waiter's report.
type waitResult struct {
	err error
	// waited says the waiter waited for a pass: one a Retain published, or one the
	// store resumed at open.
	waited bool
	// resumed says it was a pass the store resumed at open.
	resumed bool
}

// newRetention checks the configuration and fills its defaults.
func newRetention(c RetainConfig) (*retention, error) {
	switch {
	case c.Store == nil:
		return nil, errors.New("sequencer: Retain.Store is required")
	case c.Wait == nil:
		return nil, errors.New("sequencer: Retain.Wait is required")
	case c.Window <= 0:
		return nil, fmt.Errorf("sequencer: Retain.Window %s is not above zero", c.Window)
	case c.Offset < 0:
		return nil, fmt.Errorf("sequencer: Retain.Offset %s is negative", c.Offset)
	case c.Every < 0:
		return nil, fmt.Errorf("sequencer: Retain.Every %s is negative", c.Every)
	}
	if c.Every == 0 {
		c.Every = DefaultRetainEvery
	}
	if c.Tick == nil {
		c.Tick = time.Tick
	}
	if c.Uptime == nil {
		start := time.Now() // with its monotonic reading
		c.Uptime = func() time.Duration { return time.Since(start) }
	}
	return &retention{cfg: c, results: make(chan waitResult, 1)}, nil
}

// RetentionStatus returns the last snapshot of the retention check, and false
// when the sequencer does not retain. It reads only the snapshot the loop
// published, never the store, and is safe to call from any goroutine.
func (s *Sequencer) RetentionStatus() (RetentionStatus, bool) {
	if s.ret == nil {
		return RetentionStatus{}, false
	}
	if p := s.ret.status.Load(); p != nil {
		return *p, true
	}
	return RetentionStatus{}, true
}

// qualifies says whether a record's event time was stamped by another machine's
// clock: a collector's observation, or a producer's own event time. A time on
// this store's receipt clock always agrees with the clock under suspicion, an
// object's own field may be any age, and an unknown basis says nothing.
func qualifies(b store.EventTimeBasis) bool {
	return b == store.BasisObserved || b == store.BasisProducerEvent
}

// newestQualifying is the newest event time among the qualifying records, or the
// zero time when there is none.
func newestQualifying(recs []store.Record) time.Time {
	var out time.Time
	for _, r := range recs {
		if qualifies(r.EventTimeBasis) && r.EventTime.After(out) {
			out = r.EventTime
		}
	}
	return out
}

// observeClocks takes the agreement data of one batch: the qualifying records it
// committed (written is nil when the write did not land) and those of its
// requests refused as before a horizon. When any qualifies, the newest of them
// and the batch's Uptime replace what was kept: the latest batch counts, not the
// largest time ever seen.
func (s *Sequencer) observeClocks(written []store.Record, ws []*work) {
	r := s.ret
	if r == nil {
		return
	}
	newest := newestQualifying(written)
	for _, w := range ws {
		if w.refused == "" && w.refusedClock.After(newest) {
			newest = w.refusedClock
		}
	}
	if newest.IsZero() {
		return
	}
	r.have, r.newest, r.seen = true, newest, r.cfg.Uptime()
}

// agreementAt evaluates the agreement now: the kept newest time, moved forward by
// the monotonic time since its batch, against the ingest clock.
func (r *retention) agreementAt(now time.Time, up time.Duration) Agreement {
	if !r.have {
		return AgreementUnknown
	}
	d := r.newest.Add(up - r.seen).Sub(now)
	if d < 0 {
		d = -d
	}
	if d <= clockTolerance {
		return Agree
	}
	return Disagree
}

// startRetention starts the retention check of Run: the waiter for a pass the
// store may have resumed at open, and the first snapshot. It returns the check's
// tick, and the function Run defers: it cancels the waiter's context and waits
// for the waiter, so that no call of the store is in flight when Run returns.
func (s *Sequencer) startRetention(ctx context.Context) (<-chan time.Time, func()) {
	r := s.ret
	wctx, cancel := context.WithCancel(ctx)
	r.wctx = wctx
	tick := r.cfg.Tick(r.cfg.Every)
	s.startWaiter(true)
	s.publishStatus()
	return tick, func() {
		cancel()
		r.wg.Wait()
	}
}

// startWaiter marks a pass pending, and then starts the one goroutine that waits
// for it. pending is set first, so that no check between the two can find the
// pass not pending. A waiter is started only when none runs: at boot, and after a
// Retain, which is never called while one is pending.
func (s *Sequencer) startWaiter(boot bool) {
	r := s.ret
	r.pending = true
	r.pendingSince, r.pendingSinceUp = s.now(), r.cfg.Uptime()
	if s.hooks.betweenPendingAndWaiter != nil {
		s.hooks.betweenPendingAndWaiter()
	}
	r.wg.Add(1)
	go func() {
		defer func() {
			if s.hooks.waiterExited != nil {
				s.hooks.waiterExited()
			}
			r.wg.Done()
		}()
		r.results <- s.wait(boot)
	}()
}

// wait is the waiter's work. At boot it first asks, with a context already done,
// whether a pass is pending: the store answers nil at once when none is, so a
// context error says the store resumed one at open.
func (s *Sequencer) wait(boot bool) waitResult {
	r := s.ret
	if !boot {
		return waitResult{err: r.cfg.Wait(r.wctx), waited: true}
	}
	probe, cancel := context.WithCancel(r.wctx)
	cancel()
	err := r.cfg.Wait(probe)
	if !isContextError(err) || r.wctx.Err() != nil {
		return waitResult{err: err}
	}
	s.log.Info("sequencer: a retention pass the store resumed at open is pending; no retention is asked for until it ends")
	return waitResult{err: r.cfg.Wait(r.wctx), waited: true, resumed: true}
}

func isContextError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// waited takes the waiter's report: the pass is no longer pending. An error that
// is neither the store's closing nor the context's stops retention.
func (s *Sequencer) waited(res waitResult) {
	r := s.ret
	r.pending = false
	switch {
	case res.err == nil:
		if res.waited {
			r.lastSettleEnd = s.now()
		}
		if res.resumed {
			s.log.Info("sequencer: the retention pass the store resumed at open has ended")
		}
	case errors.Is(res.err, store.ErrClosed), isContextError(res.err):
		// Shutdown: nothing is logged, and nothing more is retained.
		r.ended = true
	default:
		s.stopRetention("the wait for the retention pass failed", res.err)
	}
	s.publishStatus()
	if s.hooks.afterWaiter != nil {
		s.hooks.afterWaiter()
	}
}

// stopRetention stops retention until a restart.
func (s *Sequencer) stopRetention(why string, err error) {
	s.ret.stopped = fmt.Errorf("%s: %w", why, err)
	s.log.Error("sequencer: retention is stopped until serve is restarted", "why", why, "error", err)
}

// retainCheck is the retention check, at each of its ticks: it decides whether a
// retention is due, and how far, and asks for it. See "Retention" in the package
// documentation.
func (s *Sequencer) retainCheck(ctx context.Context) {
	defer func() {
		s.publishStatus()
		if s.hooks.afterRetainCheck != nil {
			s.hooks.afterRetainCheck()
		}
	}()
	r := s.ret
	if r.ended {
		return
	}
	if r.stopped != nil {
		s.log.Error("sequencer: retention is stopped until serve is restarted", "stopped_by", r.stopped)
		return
	}
	now, up := s.now(), r.cfg.Uptime()
	agreement := r.agreementAt(now, up)
	if agreement == Disagree {
		if !r.disagreeing {
			r.disagreeing, r.disagreeSince = true, up
		}
		level := slog.LevelWarn
		if up-r.disagreeSince >= disagreeWarnFor {
			level = slog.LevelError
		}
		s.log.Log(ctx, level, "sequencer: retention is held: the newest event time stamped by another machine's clock disagrees with this server's clock by more than a day; nothing is retained until they agree",
			"newest_event_time", r.newest, "projected", r.newest.Add(up-r.seen), "now", now, "disagreeing_for", up-r.disagreeSince)
		return
	}
	r.disagreeing = false

	target := now.Add(-r.cfg.Window).UTC().Truncate(time.Hour)
	h := s.cfg.Store.LayerHorizon(referenceLayer).Time
	var to time.Time
	var kind retainKind
	if h.IsZero() {
		// Never retained: the first call waits for agreement, whether the store is
		// empty or not.
		if agreement != Agree {
			s.log.Warn("sequencer: retention has not started: the store has never been retained, and no record stamped by another machine's clock has agreed with this server's clock since start",
				"target", target)
			return
		}
		to, kind = target, retainFirst
	} else {
		stored := h.Add(r.cfg.Offset)
		if target.Before(stored.Add(retainDay)) {
			return // not due
		}
		switch {
		case !target.After(stored.Add(retainCapStep)):
			to, kind = target, retainDaily
		case agreement == Agree:
			to, kind = target, retainCatchUp
		default:
			if r.capped && up-r.cappedAt < retainDay {
				return // the cap allows one advance per day of uptime
			}
			to, kind = stored.Add(retainCapStep), retainCapped
		}
	}
	if r.pending {
		r.skipped++
		pendingFor := up - r.pendingSinceUp
		s.log.Warn("sequencer: a retention is due, and the last pass is still pending; skipped", "target", to, "pending_since", r.pendingSince, "pending_for", pendingFor)
		if pendingFor > pendingErrorAfter {
			s.log.Error("sequencer: the retention pass has been pending for more than two hours", "pending_since", r.pendingSince, "pending_for", pendingFor)
		}
		return
	}
	if kind == retainCapped {
		r.capped, r.cappedAt = true, up
	}
	if err := r.cfg.Store.Retain(ctx, to); err != nil {
		if errors.Is(err, store.ErrClosed) || (isContextError(err) && ctx.Err() != nil) {
			r.ended = true // shutdown
			return
		}
		s.stopRetention("the store refused the retention", err)
		return
	}
	// The counters count the calls the store took, not those it refused.
	r.calls++
	switch kind {
	case retainCapped:
		r.cappedN++
	case retainCatchUp:
		r.catchUps++
	}
	switch kind {
	case retainFirst:
		s.log.Warn("sequencer: retained for the first time: the history before the target is discarded", "to", to, "window", r.cfg.Window)
	case retainCapped:
		s.log.Warn("sequencer: retained by a capped advance of two days: no record stamped by another machine's clock has confirmed this server's clock since start",
			"to", to, "target", target)
	case retainCatchUp:
		s.log.Info("sequencer: retained in one catch-up call", "from", h.Add(r.cfg.Offset), "to", to)
	default:
		s.log.Info("sequencer: retained", "to", to)
	}
	s.startWaiter(false)
}

// publishStatus publishes a snapshot of the retention check. The loop alone calls
// it, and so the store.
func (s *Sequencer) publishStatus() {
	r := s.ret
	now := s.now()
	st := RetentionStatus{
		CheckedAt:     now,
		Agreement:     r.agreementAt(now, r.cfg.Uptime()),
		LastSettleEnd: r.lastSettleEnd,
		Calls:         r.calls,
		Skipped:       r.skipped,
		Capped:        r.cappedN,
		CatchUps:      r.catchUps,
	}
	for l := catalog.L0; l <= catalog.L3; l++ {
		st.Horizons[l-catalog.L0] = s.cfg.Store.LayerHorizon(l).Time
	}
	if r.pending {
		st.PendingSince = r.pendingSince
	}
	if r.stopped != nil {
		st.StoppedBy = r.stopped.Error()
	}
	r.status.Store(&st)
}
