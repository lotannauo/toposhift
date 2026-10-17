package demo

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/ingest/otlphttp"
	"github.com/lotannauo/toposhift/internal/ingest/otlpmetrics"
	"github.com/lotannauo/toposhift/internal/ingest/resume"
	"github.com/lotannauo/toposhift/internal/ingest/sequencer"
	"github.com/lotannauo/toposhift/internal/ingest/svcgraph"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/replay/capture"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// Step is the flush tick of the sequencer: the driver moves the clock in steps of
// this much.
const Step = 30 * time.Second

// busyRetries is how often a request the sequencer answers busy is sent again, at
// the same instant. A request that was not claimed was never seen by the loop, so
// sending it again is the same input.
const busyRetries = 3

// clock is the ingest clock of the sequencer: it reads what the driver set and
// nothing else.
type clock struct{ ns atomic.Int64 }

func (c *clock) set(t time.Time) { c.ns.Store(t.UnixNano()) }
func (c *clock) now() time.Time  { return time.Unix(0, c.ns.Load()).UTC() }

// digestWriter writes through to the store and feeds every record that was stored
// to a SHA-256, in sequence order and in a fixed form, so that two runs that wrote
// the same records have one digest.
type digestWriter struct {
	inner   *memstore.Store
	h       hash.Hash
	records int
	// producers are the producers of the records written.
	producers map[lifecycle.Producer]bool
	// tee, if set, is written each batch after the store has committed it, so that
	// it holds the same records in the same order. Only tests set it.
	tee store.Store
}

func (d *digestWriter) Write(ctx context.Context, batch []store.Record) error {
	if err := d.inner.Write(ctx, batch); err != nil {
		return err
	}
	if d.tee != nil {
		if err := d.tee.Write(ctx, batch); err != nil {
			return fmt.Errorf("demo: the second store: %w", err)
		}
	}
	for i := range batch {
		d.feed(batch[i])
	}
	return nil
}

func (d *digestWriter) LastSeq() uint64 { return d.inner.LastSeq() }

func (d *digestWriter) LayerHorizon(l catalog.Layer) store.Horizon { return d.inner.LayerHorizon(l) }

func (d *digestWriter) feed(r store.Record) {
	var through int64
	if !r.Through.IsZero() {
		through = r.Through.UnixNano()
	}
	_, _ = fmt.Fprintf(d.h, "%d|%d|%s|%s|%s|%s|%d|%d|%d|%d|%d|%x|%s|%d\n",
		r.Layer, r.Subject.Kind, r.Subject.A, r.Subject.B, r.Subject.Relation, r.Producer,
		r.EventTime.UnixNano(), r.Seq, r.Kind, r.TTL, through, r.Payload, r.Boot, r.EventTimeBasis)
	d.records++
	d.producers[r.Producer] = true
}

// Count is how many records a translator set aside or remarked on, for one
// producer and one reason.
type Count struct {
	Producer, Reason string
	N                int
}

// Total is the sum of the counts.
func Total(cs []Count) int {
	n := 0
	for _, c := range cs {
		n += c.N
	}
	return n
}

// The messages with which the sequencer logs what its translators set aside (at
// debug level) and remarked on (at info level), once for each producer and reason
// of each request, with the number as the attribute count. The sequencer's own
// package owns the text; the build below reads them back, and a text that changed
// would show as no skip counted at all, which the namespace events it sends make
// an error (see [Built.UnexplainedSkips]).
const (
	skipMessage = "sequencer: translator skip"
	noteMessage = "sequencer: translator note"
)

// logCounts is what the sequencer's logger keeps of the lines it was given.
type logCounts struct {
	// warns counts the lines of warn level or above.
	warns atomic.Int64

	mu           sync.Mutex
	skips, notes map[[2]string]int
}

func (c *logCounts) add(r slog.Record) {
	var producer, reason string
	var n int64
	r.Attrs(func(a slog.Attr) bool {
		switch a.Key {
		case "producer":
			producer = a.Value.String()
		case "reason":
			reason = a.Value.String()
		case "count":
			if a.Value.Kind() == slog.KindInt64 {
				n = a.Value.Int64()
			}
		}
		return true
	})
	c.mu.Lock()
	defer c.mu.Unlock()
	m := &c.skips
	if r.Message == noteMessage {
		m = &c.notes
	}
	if *m == nil {
		*m = map[[2]string]int{}
	}
	(*m)[[2]string{producer, reason}] += int(n)
}

func (c *logCounts) counts(m map[[2]string]int) []Count {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []Count
	for k, n := range m {
		out = append(out, Count{Producer: k[0], Reason: k[1], N: n})
	}
	slices.SortFunc(out, func(a, b Count) int {
		if c := strings.Compare(a.Producer, b.Producer); c != 0 {
			return c
		}
		return strings.Compare(a.Reason, b.Reason)
	})
	return out
}

// lineCounter is the sequencer's logger. It reads the lines that say what the
// translators set aside or remarked on, counts the lines of warn level or above,
// and passes on to the logger the caller gave, if any, those of warn level or
// above and no others: what is below stays out of the caller's log.
type lineCounter struct {
	next slog.Handler
	c    *logCounts
}

func (h lineCounter) Enabled(_ context.Context, l slog.Level) bool { return l >= slog.LevelDebug }

func (h lineCounter) Handle(ctx context.Context, r slog.Record) error {
	if r.Message == skipMessage || r.Message == noteMessage {
		h.c.add(r)
	}
	if r.Level < slog.LevelWarn {
		return nil
	}
	h.c.warns.Add(1)
	if h.next != nil && h.next.Enabled(ctx, r.Level) {
		return h.next.Handle(ctx, r)
	}
	return nil
}

func (h lineCounter) WithAttrs(a []slog.Attr) slog.Handler {
	if h.next != nil {
		h.next = h.next.WithAttrs(a)
	}
	return h
}

func (h lineCounter) WithGroup(name string) slog.Handler {
	if h.next != nil {
		h.next = h.next.WithGroup(name)
	}
	return h
}

// namespaceType is the entity type of the namespace events the cluster receiver
// sends. A namespace is not in the catalog: the cluster translator sets each such
// event aside, and the world sends them for the detector of cluster identity.
const namespaceType = "k8s.namespace"

// namespaceEvents counts the namespace events of a request.
func namespaceEvents(data capture.LogsData) int {
	n := 0
	for _, rl := range data.ResourceLogs {
		for _, sl := range rl.ScopeLogs {
			for _, lr := range sl.LogRecords {
				for _, kv := range lr.Attributes {
					if kv.Key == "entity.type" && kv.Value.StringValue != nil && *kv.Value.StringValue == namespaceType {
						n++
					}
				}
			}
		}
	}
	return n
}

// Options configures [Build].
type Options struct {
	// Policy is the lifecycle policy the store folds with. The package knows
	// nothing of any command: its caller gives the policy a server of the same
	// cluster name opens its store with, so the authority order of the producers
	// is the same.
	Policy lifecycle.Policy
	// Logger receives the lines of the sequencer at warn level and above; there
	// should be none. Nil discards them.
	Logger *slog.Logger

	// tee, if set, receives every batch the store has committed, in order. A test
	// gives it a second store, to compare the two.
	tee store.Store
}

// driver is a sequencer over a new in-memory store, with the clock and the tick the
// driver moves, and the loop running.
type driver struct {
	st   *memstore.Store
	d    *digestWriter
	clk  *clock
	tick chan time.Time
	seq  *sequencer.Sequencer
	logs logCounts
	// nsEvents is how many namespace events the requests accepted so far held,
	// databasePoints how many service graph points of calls to a database, and
	// warmUpSeries how many series of other calls the first service graph flush
	// held: the translator sets these aside while it warms up.
	nsEvents, databasePoints, warmUpSeries int
	flushSeen                              bool
	// stop ends the loop's context; done receives Run's result.
	stop context.CancelFunc
	done chan error
}

// newDriver builds the sequencer as serve builds it for a configured cluster name,
// with the resume step on, over a memstore, and starts its loop. The clock reads
// the anchor.
func newDriver(o Options) (*driver, error) {
	st, err := memstore.Open(memstore.Options{Policy: o.Policy})
	if err != nil {
		return nil, fmt.Errorf("demo: %w", err)
	}
	dr := &driver{st: st, d: &digestWriter{inner: st, h: sha256.New(), producers: map[lifecycle.Producer]bool{}, tee: o.tee}, clk: &clock{}, tick: make(chan time.Time), done: make(chan error, 1)}
	dr.clk.set(anchor)
	var next slog.Handler
	if o.Logger != nil {
		next = o.Logger.Handler()
	}
	dr.seq, err = sequencer.New(sequencer.Config{
		Store:            dr.d,
		Now:              dr.clk.now,
		Tick:             func(time.Duration) <-chan time.Time { return dr.tick },
		ClusterName:      ClusterName,
		ClusterNameSet:   true,
		Logger:           slog.New(lineCounter{next: next, c: &dr.logs}),
		DependencyExpiry: Expiry,
		InstanceTTL:      Expiry,
		Lookup: func() (resume.Lookup, error) {
			lk, err := st.NewLookup()
			if err != nil {
				return nil, err
			}
			return lk, nil
		},
	})
	if err != nil {
		_ = st.Close()
		return nil, fmt.Errorf("demo: %w", err)
	}
	// The loop's own context outlives every request and the stop.
	var runCtx context.Context
	runCtx, dr.stop = context.WithCancel(context.Background())
	go func() { dr.done <- dr.seq.Run(runCtx) }()
	return dr, nil
}

// abort ends the loop without flushing and closes the store.
func (dr *driver) abort() {
	dr.stop()
	<-dr.done
	_ = dr.st.Close()
}

// send runs one call to the sequencer for a request. A request answered busy is
// sent again at the same instant, up to busyRetries times; any other refusal, and
// any record or data point rejected, is an error that names the request.
func (dr *driver) send(ctx context.Context, what string, at time.Time, unit string, call func() (otlphttp.Partial, error)) error {
	for attempt := 0; ; attempt++ {
		p, err := call()
		switch {
		case err == nil && p.Rejected == 0:
			return nil
		case err == nil:
			return fmt.Errorf("demo: the %s at %s had %d %s rejected: %s", what, at.Format(time.RFC3339), p.Rejected, unit, p.Message)
		case ctx.Err() != nil:
			return ctx.Err()
		case errors.Is(err, otlphttp.ErrBusy) && attempt < busyRetries:
			continue
		}
		return fmt.Errorf("demo: the %s at %s was refused: %w", what, at.Format(time.RFC3339), err)
	}
}

// accept sends a logs request.
func (dr *driver) accept(ctx context.Context, data capture.LogsData, what string, at time.Time) error {
	err := dr.send(ctx, what, at, "log records", func() (otlphttp.Partial, error) { return dr.seq.AcceptPartial(ctx, data) })
	if err == nil {
		dr.nsEvents += namespaceEvents(data)
	}
	return err
}

// acceptMetrics sends a metrics request, through the sequencer's metrics path:
// the queue, the claim and the answers of a logs request.
func (dr *driver) acceptMetrics(ctx context.Context, data otlpmetrics.Data, what string, at time.Time) error {
	err := dr.send(ctx, what, at, "data points", func() (otlphttp.Partial, error) { return dr.seq.AcceptMetrics(ctx, data) })
	if err == nil {
		dr.countMetrics(data)
	}
	return err
}

// countMetrics counts what a metrics request that was taken in whole holds, for the
// skips it explains: the calls to databases, and the series the first service graph
// flush made, which the translator sets aside while it warms up.
func (dr *driver) countMetrics(data otlpmetrics.Data) {
	points, databases := 0, 0
	for _, res := range data.Resources {
		for _, p := range res.Points {
			if p.Scope == svcgraph.Scope && p.Metric == svcgraph.Metric {
				points++
				if isDatabasePoint(p) {
					databases++
				}
			}
		}
	}
	dr.databasePoints += databases
	if points > 0 && !dr.flushSeen {
		dr.flushSeen, dr.warmUpSeries = true, points-databases
	}
}

// flush sets the clock to t, sends the flush tick, and sends the barrier.
func (dr *driver) flush(ctx context.Context, t time.Time) error {
	dr.clk.set(t)
	select {
	case dr.tick <- t:
	case <-ctx.Done():
		return ctx.Err()
	case err := <-dr.done:
		dr.done <- err // for abort
		return fmt.Errorf("demo: the sequencer stopped: %w", err)
	}
	return dr.accept(ctx, capture.LogsData{}, "barrier", t)
}

// Build runs the world through the real ingest path into a new in-memory store.
//
// The requests of the world's collectors are sent to a [sequencer.Sequencer]
// exactly as the receiver would send them, on an ingest clock the driver sets and
// a flush tick it sends, in steps of thirty seconds from the anchor to the end of
// the window. The clock is set to the instant of each request before it is sent,
// and to the step before the tick. Nothing in the path reads the wall clock, so
// one seed gives one store: the same sequence number, record count and digest on
// any machine.
//
// The loop is one goroutine, and nothing outside the sequencer can wait for a tick
// to be handled. So each tick is followed by a request with no records, the
// barrier: it is claimed only once the tick has returned, its commit writes
// nothing, and once it is answered the loop reads no clock before its next
// select. The clock may then move.
//
// The caller closes the store of the returned value. On an error the store is
// closed and nothing is returned.
func Build(ctx context.Context, w *World, o Options) (*Built, error) {
	began := time.Now()
	dr, err := newDriver(o)
	if err != nil {
		return nil, err
	}
	reqs := w.Requests()
	sent := 0
	for k := 0; k <= int(Span/Step); k++ {
		t := anchor.Add(time.Duration(k) * Step)
		for sent < len(reqs) && !reqs[sent].At.After(t) {
			r := reqs[sent]
			sent++
			dr.clk.set(r.At)
			what := r.Emitter.String() + " (" + r.What + ")"
			var err error
			if r.IsMetrics() {
				err = dr.acceptMetrics(ctx, r.Metrics(), what, r.At)
			} else {
				err = dr.accept(ctx, r.Data(), what, r.At)
			}
			if err != nil {
				dr.abort()
				return nil, err
			}
		}
		if err := dr.flush(ctx, t); err != nil {
			dr.abort()
			return nil, err
		}
	}

	// The stop flushes every refresh the coalescer absorbed.
	if err := dr.seq.Stop(context.Background()); err != nil {
		dr.abort()
		return nil, err
	}
	if err := <-dr.done; err != nil {
		_ = dr.st.Close()
		return nil, fmt.Errorf("demo: the sequencer: %w", err)
	}
	dr.stop()
	return &Built{
		Store:           dr.st,
		LastSeq:         dr.st.LastSeq(),
		Records:         dr.d.records,
		Digest:          hex.EncodeToString(dr.d.h.Sum(nil)),
		Producers:       slices.Sorted(maps.Keys(dr.d.producers)),
		Warnings:        int(dr.logs.warns.Load()),
		Skips:           dr.logs.counts(dr.logs.skips),
		Notes:           dr.logs.counts(dr.logs.notes),
		NamespaceEvents: dr.nsEvents,
		DatabasePoints:  dr.databasePoints,
		WarmUpSeries:    dr.warmUpSeries,
		Metrics:         dr.seq.MetricsStatus().Counters,
		Took:            time.Since(began),
		Story:           w.Story(),
	}, nil
}
