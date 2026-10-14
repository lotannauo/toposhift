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
	"github.com/lotannauo/toposhift/internal/coalesce"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/ingest/detector"
	"github.com/lotannauo/toposhift/internal/ingest/otlphttp"
	"github.com/lotannauo/toposhift/internal/ingest/resume"
	"github.com/lotannauo/toposhift/internal/replay/capture"
	"github.com/lotannauo/toposhift/internal/store"
)

// The defaults of [Config]. The queue length, the records per commit and the
// number of cluster names are estimates.
const (
	DefaultPullInterval       = 5 * time.Minute
	DefaultCollectionInterval = 5 * time.Minute
	DefaultNodeInterval       = 5 * time.Minute
	DefaultMergeWait          = 10 * time.Second
	DefaultQueueLen           = 64
	DefaultMaxGroupRecords    = 20_000
	DefaultMaxClusters        = 16
)

// DefaultClusterName is the cluster name of a resource that carries no
// k8s.cluster.name when no cluster name is configured.
const DefaultClusterName = "default"

// idleMultiple is how many of the longer of the two intervals a cluster name other
// than the configured one may go without a request before [Sequencer] frees its
// slot (an estimate).
const idleMultiple = 3

// resumeLogEvery is how often, at most, the sequencer logs each kind of resume
// warning (on the ingest clock).
const resumeLogEvery = time.Minute

// Writer is what the sequencer needs of a store: the writing, the last sequence
// number, and the horizons. Both store packages have it.
type Writer interface {
	Write(ctx context.Context, batch []store.Record) error
	LastSeq() uint64
	LayerHorizon(layer catalog.Layer) store.Horizon
}

// Config configures a [Sequencer]. Store is required; a zero field of any other
// kind takes its default.
type Config struct {
	// Store is written to by the sequencer alone.
	Store Writer
	// Now is the ingest clock; time.Now when nil. The sequencer never lets the
	// instants it uses go backwards, whatever the clock does.
	Now func() time.Time
	// Tick returns the channel of the flush tick, called once with
	// [coalesce.FlushPeriod]; time.Tick when nil.
	Tick func(d time.Duration) <-chan time.Time
	// ClusterName is the configured cluster name, when ClusterNameSet. It must pass
	// [store.CheckClusterNameLimit]. Without one, each resource's k8s.cluster.name
	// names its cluster, or [DefaultClusterName].
	ClusterName    string
	ClusterNameSet bool
	// PullInterval is the interval of the Kubernetes objects receiver's pulls.
	PullInterval time.Duration
	// CollectionInterval is the cluster receiver's report interval, used for an
	// event that carries none, and the detector's windows: a warmup of one and an
	// alive time of three.
	CollectionInterval time.Duration
	// NodeInterval is the report interval the node agents are configured with
	// (--node-interval): the TTL of the node agents' producer is three times it,
	// and a cluster whose host events stop for three of them is warned about.
	NodeInterval time.Duration
	// MergeWait is how long a request waits to be merged into a group before it is
	// answered 503.
	MergeWait time.Duration
	// QueueLen is how many requests wait at most; one more is answered 503 at once.
	QueueLen int
	// MaxGroupRecords is how many OTLP log records one synced commit takes at most,
	// except that a request is never split.
	MaxGroupRecords int
	// MaxClusters is how many cluster names have translators at once.
	MaxClusters int
	// Logger receives the notes, refusals, failures and resets; slog.Default when
	// nil.
	Logger *slog.Logger
	// Retain configures the retention check; nil never retains. See "Retention"
	// in the package documentation.
	Retain *RetainConfig
	// Lookup opens the lookup of the resume step, once per group: the store's own,
	// such as a wrapper of (*pebblestore.Store).NewLookup. Nil turns resume off,
	// and then a record a translator backdates is written as the translator timed
	// it. See "Resume" in the package documentation.
	Lookup func() (resume.Lookup, error)
}

// Sequencer takes the requests of the receiver, translates and coalesces their
// log records, and commits them to the store, one synced commit per group of
// requests. See the package documentation.
type Sequencer struct {
	cfg      Config
	log      *slog.Logger
	resolver *identity.Resolver
	coCfg    coalesce.Config

	// mu orders the requests' sends on the queue with stopping: once stopping is
	// set under it, nothing more is sent.
	mu       sync.RWMutex
	stopping bool
	ran      bool
	queue    chan *request
	stopCh   chan struct{}
	stopOnce sync.Once
	done     chan struct{}

	// What follows belongs to the loop.
	carry    *request // dequeued but over the group's bound: the next group's first
	clusters map[string]*cluster
	co       *coalesce.Coalescer
	det      *detector.Detector
	horizons [layers]time.Time
	last     time.Time  // the latest ingest instant used
	ret      *retention // nil: never retains
	hooks    hooks
	// resumeWarned is the ingest instant each kind of resume warning was last
	// logged at.
	resumeWarned map[string]time.Time

	// agentMu guards what NodeAgentStatus reads from any goroutine: agentCount,
	// the node agents' counters, and agentSeen, the clusters that have had a host
	// entity event. The loop writes them.
	agentMu    sync.Mutex
	agentCount map[string]int64
	agentSeen  map[string]*agentCluster

	// resumeMu guards resumeStats, the resume counts of every group answered
	// without error, which ResumeStats reads from any goroutine.
	resumeMu    sync.Mutex
	resumeStats resume.Stats
}

// layers is the number of layers, L0 to L3.
const layers = int(catalog.L3-catalog.L0) + 1

// hooks are test seams. Each is called by the loop when set.
type hooks struct {
	// beforeWrite is called before each Write, with the batch.
	beforeWrite func(batch []store.Record)
	// beforeTranslate is called before each record is given to a translator.
	beforeTranslate func(cluster string, lr capture.LogRecord)
	// afterTick is called when a flush tick has been handled.
	afterTick func()
	// afterRetainCheck is called when a retention check has been handled, and
	// afterWaiter when the waiter's report has.
	afterRetainCheck func()
	afterWaiter      func()
	// betweenPendingAndWaiter is called after a pass is marked pending and before
	// its waiter starts.
	betweenPendingAndWaiter func()
	// waiterExited is called by the waiter's goroutine as it exits.
	waiterExited func()
}

// The states of a request's claim.
const (
	statePending int32 = iota
	stateClaimed       // the loop merged it: its outcome is its group's
	stateExpired       // its MergeWait passed first: it was answered 503
	stateDropped       // its context was done first: it was dropped
)

// request is one request waiting for, or in, a group.
type request struct {
	ctx     context.Context
	data    capture.LogsData
	records int // OTLP log records
	state   atomic.Int32
	done    chan outcome // buffered: the loop never waits on it

	// What follows belongs to the loop.
	received time.Time // the ingest instant it was claimed at
	panicked string    // set when translating it panicked: it is refused for good
}

type outcome struct {
	partial otlphttp.Partial
	err     error
}

// New returns a sequencer. It checks the configuration and starts the detector's
// warmup at the ingest clock's present.
func New(c Config) (*Sequencer, error) {
	if c.Store == nil {
		return nil, errors.New("sequencer: Store is required")
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	if c.Tick == nil {
		c.Tick = time.Tick
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	durations := []struct {
		name string
		d    *time.Duration
		def  time.Duration
	}{
		{"PullInterval", &c.PullInterval, DefaultPullInterval},
		{"CollectionInterval", &c.CollectionInterval, DefaultCollectionInterval},
		{"NodeInterval", &c.NodeInterval, DefaultNodeInterval},
		{"MergeWait", &c.MergeWait, DefaultMergeWait},
	}
	for _, f := range durations {
		switch {
		case *f.d < 0:
			return nil, fmt.Errorf("sequencer: %s %s is negative", f.name, *f.d)
		case *f.d == 0:
			*f.d = f.def
		}
	}
	counts := []struct {
		name string
		n    *int
		def  int
	}{
		{"QueueLen", &c.QueueLen, DefaultQueueLen},
		{"MaxGroupRecords", &c.MaxGroupRecords, DefaultMaxGroupRecords},
		{"MaxClusters", &c.MaxClusters, DefaultMaxClusters},
	}
	for _, f := range counts {
		switch {
		case *f.n < 0:
			return nil, fmt.Errorf("sequencer: %s %d is negative", f.name, *f.n)
		case *f.n == 0:
			*f.n = f.def
		}
	}
	switch {
	case c.ClusterNameSet:
		if err := store.CheckClusterNameLimit(c.ClusterName); err != nil {
			return nil, fmt.Errorf("sequencer: configured cluster name: %w", err)
		}
	case c.ClusterName != "":
		return nil, errors.New("sequencer: ClusterName is set but ClusterNameSet is false")
	}
	s := &Sequencer{
		cfg:      c,
		log:      c.Logger,
		resolver: identity.NewResolver(catalog.Default()),
		// The configuration of ingest: a run is re-asserted at most once per half
		// its TTL, and its age is bounded by that same interval. DefaultConfig's
		// fixed 30-minute age bound must not be used: it would delete node
		// checkpoints.
		coCfg:    coalesce.Config{ExtendTTLFraction: 0.5, RunAgeFromTTL: true},
		queue:    make(chan *request, c.QueueLen),
		stopCh:   make(chan struct{}),
		done:     make(chan struct{}),
		clusters: make(map[string]*cluster),

		resumeWarned: make(map[string]time.Time),
		agentCount:   make(map[string]int64),
		agentSeen:    make(map[string]*agentCluster),
	}
	if c.Retain != nil {
		r, err := newRetention(*c.Retain)
		if err != nil {
			return nil, err
		}
		s.ret = r
	}
	// The translators are made on first use; one pair is made here only so that a
	// configuration they refuse is refused now.
	if _, err := s.newCluster(DefaultClusterName); err != nil {
		return nil, fmt.Errorf("sequencer: %w", err)
	}
	var err error
	if s.co, err = coalesce.New(s.coCfg); err != nil {
		return nil, fmt.Errorf("sequencer: %w", err)
	}
	start := s.now()
	if start.IsZero() {
		return nil, errors.New("sequencer: the ingest clock returned the zero time")
	}
	s.det, err = detector.New(detector.Config{
		Warmup:       c.CollectionInterval,
		DefaultAlive: 3 * c.CollectionInterval,
	}, start)
	if err != nil {
		return nil, fmt.Errorf("sequencer: %w", err)
	}
	return s, nil
}

// now is the ingest clock, without its monotonic reading and never before an
// instant it returned earlier: the detector requires instants that do not go
// backwards. It is called by New and then by the loop alone.
func (s *Sequencer) now() time.Time {
	t := s.cfg.Now().Round(0).UTC()
	if t.Before(s.last) {
		return s.last
	}
	s.last = t
	return t
}

// errStopping answers a request that arrives once the sequencer is stopping.
var errStopping = &otlphttp.UnavailableError{Message: "ingest is stopping: send the request again later"}

// Accept is [Sequencer.AcceptPartial] without the partial success: it returns
// nil for a request accepted in part.
func (s *Sequencer) Accept(ctx context.Context, d capture.LogsData) error {
	_, err := s.AcceptPartial(ctx, d)
	return err
}

// AcceptPartial queues the request, waits for its group's commit, and returns its
// outcome. It returns, mapped by the receiver:
//
//   - nil, with the log records rejected, once the group's synced commit has
//     returned without error;
//   - [otlphttp.ErrBusy] at once when the queue is full, and when MergeWait
//     passes before the loop claims the request;
//   - an [otlphttp.RefusedError] for a request refused whole;
//   - an error wrapping ErrBusy, or an [otlphttp.UnavailableError] when the
//     store is closed or the sequencer is stopping, when the commit failed.
//
// A request whose context is done before the loop claims it is dropped. Once the
// loop has claimed it, its outcome is its group's, whatever its context.
func (s *Sequencer) AcceptPartial(ctx context.Context, d capture.LogsData) (otlphttp.Partial, error) {
	r := &request{ctx: ctx, data: d, records: countRecords(d), done: make(chan outcome, 1)}
	s.mu.RLock()
	if s.stopping {
		s.mu.RUnlock()
		return otlphttp.Partial{}, errStopping
	}
	select {
	case s.queue <- r:
	default:
		s.mu.RUnlock()
		return otlphttp.Partial{}, fmt.Errorf("%w: the queue of %d requests is full", otlphttp.ErrBusy, s.cfg.QueueLen)
	}
	s.mu.RUnlock()

	// The wait is enforced here, not by the loop, so that a request is answered
	// in time while the loop is held in a long commit.
	timer := time.NewTimer(s.cfg.MergeWait)
	defer timer.Stop()
	select {
	case o := <-r.done:
		return o.partial, o.err
	case <-timer.C:
		if r.state.CompareAndSwap(statePending, stateExpired) {
			return otlphttp.Partial{}, fmt.Errorf("%w: the request was not merged within %s", otlphttp.ErrBusy, s.cfg.MergeWait)
		}
	case <-ctx.Done():
		if r.state.CompareAndSwap(statePending, stateDropped) {
			return otlphttp.Partial{}, dropped(ctx.Err())
		}
	}
	// The loop claimed it first, or dropped it: it answers.
	o := <-r.done
	return o.partial, o.err
}

// dropped is the answer to a request whose context was done before it was
// claimed. Its client has gone; it is a 503 all the same, never a 500.
func dropped(err error) error {
	return fmt.Errorf("%w: the request was dropped before it was merged: %w", otlphttp.ErrBusy, err)
}

func countRecords(d capture.LogsData) int {
	n := 0
	for _, rl := range d.ResourceLogs {
		for _, sl := range rl.ScopeLogs {
			n += len(sl.LogRecords)
		}
	}
	return n
}

// Run is the loop. It returns nil after [Sequencer.Stop] once the claimed
// requests are answered and the coalescer's absorbed refreshes are written, or
// the context's error when ctx is done first. ctx is the sequencer's own: it is
// given to every Write, so it must outlive the requests, the signal that stops
// the server, and Stop. Run may be called once.
func (s *Sequencer) Run(ctx context.Context) error {
	s.mu.Lock()
	if s.ran {
		s.mu.Unlock()
		return errors.New("sequencer: Run called twice")
	}
	s.ran = true
	s.mu.Unlock()
	defer close(s.done)

	tick := s.cfg.Tick(coalesce.FlushPeriod)
	// The retention check's tick and the waiter's reports; nil channels, never
	// ready, when the sequencer does not retain.
	var retainTick <-chan time.Time
	var waits <-chan waitResult
	if s.ret != nil {
		var end func()
		retainTick, end = s.startRetention(ctx)
		waits = s.ret.results
		// Run returns only once the waiter has: no call of the store is in flight
		// when the caller closes it. The report it may still send is not read.
		defer end()
	}
	for {
		// A stop goes first: from then on finish merges what is queued, so the
		// end does not depend on which ready case a select picks.
		select {
		case <-s.stopCh:
			return s.finish(ctx)
		default:
		}
		if s.carry != nil {
			// A request dequeued earlier waits; the ticks and a stop are not
			// starved by a backlog of large requests.
			select {
			case <-ctx.Done():
				s.abandon()
				return ctx.Err()
			case <-s.stopCh:
				return s.finish(ctx)
			case <-tick:
				s.tick(ctx)
			case <-retainTick:
				s.retainCheck(ctx)
			case res := <-waits:
				s.waited(res)
			default:
			}
			s.group(ctx, nil)
			continue
		}
		select {
		case <-ctx.Done():
			s.abandon()
			return ctx.Err()
		case <-s.stopCh:
			return s.finish(ctx)
		case <-tick:
			s.tick(ctx)
		case <-retainTick:
			s.retainCheck(ctx)
		case res := <-waits:
			s.waited(res)
		case r := <-s.queue:
			s.group(ctx, r)
		}
	}
}

// Stop stops the sequencer: new requests are answered 503 at once, the requests
// already queued are merged and answered, the coalescer's absorbed refreshes are
// written, and Run returns. Stop waits for that, or for ctx. A second call
// returns nil at once, and so does a call before Run.
func (s *Sequencer) Stop(ctx context.Context) error {
	first := false
	s.stopOnce.Do(func() {
		first = true
		s.setStopping()
		close(s.stopCh)
	})
	if !first {
		return nil
	}
	s.mu.RLock()
	ran := s.ran
	s.mu.RUnlock()
	if !ran {
		return nil
	}
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("sequencer: stop: %w", ctx.Err())
	}
}

func (s *Sequencer) setStopping() {
	s.mu.Lock()
	s.stopping = true
	s.mu.Unlock()
}

// finish is the end of Run after Stop: nothing is sent on the queue any more, so
// what it holds is merged and answered, and then every refresh the coalescer
// absorbed is written.
func (s *Sequencer) finish(ctx context.Context) error {
	for {
		if s.carry == nil {
			select {
			case r := <-s.queue:
				s.carry = r
			default:
			}
		}
		if s.carry == nil {
			break
		}
		s.group(ctx, nil)
	}
	s.readHorizons()
	out := s.co.FlushAll(nil)
	s.writeDerived(ctx, out, "the flush of every absorbed refresh on stop")
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

// abandon is the end of Run when its context is done: the requests not yet
// claimed are answered 503, and nothing more is written.
func (s *Sequencer) abandon() {
	s.setStopping()
	for {
		r := s.carry
		s.carry = nil
		if r == nil {
			select {
			case r = <-s.queue:
			default:
				return
			}
		}
		if r.state.CompareAndSwap(statePending, stateClaimed) {
			r.done <- outcome{err: errStopping}
		}
	}
}

// group claims the waiting requests, first (or the carried request when first is
// nil) and then those queued behind it, in arrival order, up to MaxGroupRecords
// log records, and commits them. A request that does not fit is carried to the
// next group, unclaimed.
func (s *Sequencer) group(ctx context.Context, first *request) {
	if first == nil {
		first, s.carry = s.carry, nil
	}
	var reqs []*request
	total := 0
	take := func(r *request) bool {
		if len(reqs) > 0 && total+r.records > s.cfg.MaxGroupRecords {
			s.carry = r
			return false
		}
		if s.claim(r) {
			reqs = append(reqs, r)
			total += r.records
		}
		return true
	}
	if take(first) {
	more:
		for {
			select {
			case r := <-s.queue:
				if !take(r) {
					break more
				}
			default:
				break more
			}
		}
	}
	if len(reqs) > 0 {
		s.commit(ctx, reqs, true, true)
	}
}

// claim takes a request into a group, unless its MergeWait passed or its context
// is done first: the loser of the claim does nothing.
func (s *Sequencer) claim(r *request) bool {
	if err := r.ctx.Err(); err != nil {
		if r.state.CompareAndSwap(statePending, stateDropped) {
			r.done <- outcome{err: dropped(err)}
		}
		return false
	}
	if !r.state.CompareAndSwap(statePending, stateClaimed) {
		return false
	}
	r.received = s.now()
	return true
}

// ResumeStats returns the resume step's counts, over every group answered without
// error since New, and whether the step is on ([Config.Lookup] set). It may be
// called from any goroutine.
func (s *Sequencer) ResumeStats() (resume.Stats, bool) {
	s.resumeMu.Lock()
	defer s.resumeMu.Unlock()
	return s.resumeStats, s.cfg.Lookup != nil
}
