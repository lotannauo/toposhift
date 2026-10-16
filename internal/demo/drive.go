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
	"sync/atomic"
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
}

func (d *digestWriter) Write(ctx context.Context, batch []store.Record) error {
	if err := d.inner.Write(ctx, batch); err != nil {
		return err
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

// warnCounter is the sequencer's logger: it keeps only the lines at warn level or
// above, counts them, and passes them on to the logger the caller gave, if any.
type warnCounter struct {
	next slog.Handler
	n    *atomic.Int64
}

func (h warnCounter) Enabled(_ context.Context, l slog.Level) bool { return l >= slog.LevelWarn }

func (h warnCounter) Handle(ctx context.Context, r slog.Record) error {
	h.n.Add(1)
	if h.next != nil && h.next.Enabled(ctx, r.Level) {
		return h.next.Handle(ctx, r)
	}
	return nil
}

func (h warnCounter) WithAttrs(a []slog.Attr) slog.Handler {
	if h.next != nil {
		h.next = h.next.WithAttrs(a)
	}
	return h
}

func (h warnCounter) WithGroup(name string) slog.Handler {
	if h.next != nil {
		h.next = h.next.WithGroup(name)
	}
	return h
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
}

// driver is a sequencer over a new in-memory store, with the clock and the tick the
// driver moves, and the loop running.
type driver struct {
	st    *memstore.Store
	d     *digestWriter
	clk   *clock
	tick  chan time.Time
	seq   *sequencer.Sequencer
	warns atomic.Int64
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
	dr := &driver{st: st, d: &digestWriter{inner: st, h: sha256.New(), producers: map[lifecycle.Producer]bool{}}, clk: &clock{}, tick: make(chan time.Time), done: make(chan error, 1)}
	dr.clk.set(anchor)
	var next slog.Handler
	if o.Logger != nil {
		next = o.Logger.Handler()
	}
	dr.seq, err = sequencer.New(sequencer.Config{
		Store:          dr.d,
		Now:            dr.clk.now,
		Tick:           func(time.Duration) <-chan time.Time { return dr.tick },
		ClusterName:    ClusterName,
		ClusterNameSet: true,
		Logger:         slog.New(warnCounter{next: next, n: &dr.warns}),
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

// accept sends a request. A request answered busy is sent again at the same
// instant, up to busyRetries times; any other refusal, and any record rejected, is
// an error that names the request.
func (dr *driver) accept(ctx context.Context, data capture.LogsData, what string, at time.Time) error {
	for attempt := 0; ; attempt++ {
		p, err := dr.seq.AcceptPartial(ctx, data)
		switch {
		case err == nil && p.Rejected == 0:
			return nil
		case err == nil:
			return fmt.Errorf("demo: the %s at %s had %d log records rejected: %s", what, at.Format(time.RFC3339), p.Rejected, p.Message)
		case ctx.Err() != nil:
			return ctx.Err()
		case errors.Is(err, otlphttp.ErrBusy) && attempt < busyRetries:
			continue
		}
		return fmt.Errorf("demo: the %s at %s was refused: %w", what, at.Format(time.RFC3339), err)
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
			if err := dr.accept(ctx, r.Data(), r.Emitter.String()+" ("+r.What+")", r.At); err != nil {
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
		Store:     dr.st,
		LastSeq:   dr.st.LastSeq(),
		Records:   dr.d.records,
		Digest:    hex.EncodeToString(dr.d.h.Sum(nil)),
		Producers: slices.Sorted(maps.Keys(dr.d.producers)),
		Warnings:  int(dr.warns.Load()),
		Took:      time.Since(began),
		Story:     w.Story(),
	}, nil
}
