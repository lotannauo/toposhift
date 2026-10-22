package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/coalesce"
	"github.com/lotannauo/toposhift/internal/identity"
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

// busyRetries is how often a request answered 503 is sent again, at the same
// instant. The sequencer answers busy only for a request it never claimed, so
// sending it again is the same input.
const busyRetries = 3

// clusterNameKey is the resource attribute the sequencer reads a cluster name from.
const clusterNameKey = "k8s.cluster.name"

// clock is the ingest clock of the sequencer: it reads what the replay set and
// nothing else.
type clock struct{ ns atomic.Int64 }

func (c *clock) set(t time.Time) { c.ns.Store(t.UnixNano()) }
func (c *clock) now() time.Time  { return time.Unix(0, c.ns.Load()).UTC() }

// pipeline is the product's ingest path for one expiry, over a memory store:
// the receiver's handler (protobuf in, otlpmetrics.Data out), whose sink is the
// sequencer (the real translators, coalescer and writer) and, beside it, a second
// service graph translator that is given what the sequencer was given and whose
// output is kept whole, as the truth.
//
// The sequencer's clock and flush tick are the replay's, in event time, as the
// demo's driver moves them: nothing here reads the wall clock.
type pipeline struct {
	expiry time.Duration
	st     *memstore.Store
	seq    *sequencer.Sequencer
	clk    clock
	tick   chan time.Time
	stop   context.CancelFunc
	done   chan error
	h      http.Handler

	// last is what the sink did with the request it served last.
	last sinkOutcome

	resolver *identity.Resolver
	fp       otlpmetrics.Fingerprints
	graph    map[string]*svcgraph.Translator
	truth    *truth
	// names say what each service the points named is called, for the report.
	names map[identity.Fingerprint]string

	// nextTick is the next flush tick, on the grid of the coalescer's flush period.
	nextTick time.Time
	// sampleAt is the instant of the next sample, and sampleN its index; tokens
	// holds, for each sample taken, the store's last sequence number when the replay
	// reached its instant.
	sample0  time.Time
	sampleAt time.Time
	step     time.Duration
	tokens   []uint64
}

// sinkOutcome is what the sink returned for a request.
type sinkOutcome struct {
	called bool
	part   otlphttp.Partial
	err    error
}

// tee is the receiver's sink: it gives the request to the sequencer, and, when the
// sequencer took it, to the truth.
type tee struct{ p *pipeline }

func (t tee) AcceptMetrics(ctx context.Context, d otlpmetrics.Data) (otlphttp.Partial, error) {
	part, err := t.p.seq.AcceptMetrics(ctx, d)
	t.p.last = sinkOutcome{called: true, part: part, err: err}
	if err != nil || part.Rejected > 0 {
		return part, err
	}
	if err := t.p.feedTruth(d); err != nil {
		return part, err
	}
	return part, nil
}

// newPipeline builds the sequencer as serve builds it for a cluster name taken
// from the resources, with the resume step on, over a new memory store, with the
// clock at start, and starts its loop. The service graph and span count producers
// have the expiry as TTL.
func newPipeline(expiry time.Duration, start, sampleStart time.Time, step time.Duration) (*pipeline, error) {
	st, err := memstore.Open(memstore.Options{})
	if err != nil {
		return nil, err
	}
	p := &pipeline{
		expiry:   expiry,
		st:       st,
		tick:     make(chan time.Time),
		done:     make(chan error, 1),
		resolver: identity.NewResolver(catalog.Default()),
		graph:    map[string]*svcgraph.Translator{},
		truth:    newTruth(),
		names:    map[identity.Fingerprint]string{},
	}
	p.fp = otlpmetrics.NewFingerprints(p.resolver)
	p.clk.set(start)
	p.seq, err = sequencer.New(sequencer.Config{
		Store:            st,
		Now:              p.clk.now,
		Tick:             func(time.Duration) <-chan time.Time { return p.tick },
		DependencyExpiry: expiry,
		InstanceTTL:      expiry,
		Logger:           slog.New(slog.DiscardHandler),
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
		return nil, err
	}
	p.h = otlphttp.MetricsHandler(tee{p}, otlphttp.Options{})
	var runCtx context.Context
	runCtx, p.stop = context.WithCancel(context.Background())
	go func() { p.done <- p.seq.Run(runCtx) }()
	p.nextTick = start.Truncate(coalesce.FlushPeriod).Add(coalesce.FlushPeriod)
	p.sample0, p.sampleAt, p.step = sampleStart, sampleStart, step
	return p, nil
}

// abort ends the loop without flushing and closes the store.
func (p *pipeline) abort() {
	p.stop()
	<-p.done
	_ = p.st.Close()
}

// finish stops the sequencer, which writes what the coalescer absorbed, and
// waits for the loop.
func (p *pipeline) finish() error {
	err := p.seq.Stop(context.Background())
	if lerr := <-p.done; lerr != nil && err == nil {
		err = lerr
	}
	p.stop()
	return err
}

// flush sets the clock to t, sends the flush tick, and sends a request with no
// records, which the loop claims only once the tick has been handled and which
// writes nothing: after it is answered the clock may move.
func (p *pipeline) flush(ctx context.Context, t time.Time) error {
	p.advanceClock(t)
	select {
	case p.tick <- t:
	case <-ctx.Done():
		return ctx.Err()
	case err := <-p.done:
		p.done <- err
		return fmt.Errorf("the sequencer stopped: %w", err)
	}
	for attempt := 0; ; attempt++ {
		_, err := p.seq.AcceptPartial(ctx, capture.LogsData{})
		switch {
		case err == nil:
			return nil
		case errors.Is(err, otlphttp.ErrBusy) && attempt < busyRetries && ctx.Err() == nil:
			continue
		}
		return fmt.Errorf("the barrier at %s: %w", t.Format(time.RFC3339), err)
	}
}

// advanceClock moves the clock to t, never backwards.
func (p *pipeline) advanceClock(t time.Time) {
	if t.After(p.clk.now()) {
		p.clk.set(t)
	}
}

// advance does what the replay's clock passing limit does: every flush tick and
// every sample at or before it, in order of instant, a tick before a sample at the
// same instant. A sample takes the store's last sequence number: the token a reader
// at that instant, having seen every request that arrived before it, would read at.
func (p *pipeline) advance(ctx context.Context, limit time.Time) error {
	for {
		tickDue := !p.nextTick.After(limit)
		sampleDue := !p.sampleAt.After(limit)
		switch {
		case tickDue && (!sampleDue || !p.sampleAt.Before(p.nextTick)):
			if err := p.flush(ctx, p.nextTick); err != nil {
				return err
			}
			p.nextTick = p.nextTick.Add(coalesce.FlushPeriod)
		case sampleDue:
			p.tokens = append(p.tokens, p.st.LastSeq())
			p.sampleAt = p.sampleAt.Add(p.step)
		default:
			return nil
		}
	}
}

// post sends one protobuf body to the receiver's handler, in process, at the
// instant of the line: the clock is set to it first. A request answered busy is
// sent again; any other refusal, and any data point rejected, is an error.
func (p *pipeline) post(ctx context.Context, body []byte, at time.Time) error {
	p.advanceClock(at)
	for attempt := 0; ; attempt++ {
		p.last = sinkOutcome{}
		req := httptest.NewRequestWithContext(ctx, http.MethodPost, otlphttp.MetricsPath, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/x-protobuf")
		rec := httptest.NewRecorder()
		p.h.ServeHTTP(rec, req)
		switch {
		case rec.Code == http.StatusOK && p.last.called && p.last.part.Rejected == 0:
			return nil
		case rec.Code == http.StatusOK:
			return fmt.Errorf("the request at %s had %d data points rejected: %s", at.Format(time.RFC3339), p.last.part.Rejected, p.last.part.Message)
		case rec.Code == http.StatusServiceUnavailable && errors.Is(p.last.err, otlphttp.ErrBusy) && attempt < busyRetries && ctx.Err() == nil:
			continue
		}
		return fmt.Errorf("the request at %s was answered %d: %s", at.Format(time.RFC3339), rec.Code, strings.TrimSpace(rec.Body.String()))
	}
}

// clusterName is the sequencer's cluster name for a resource, when none is
// configured: the last k8s.cluster.name that is a non-empty string, or the default.
func clusterName(res otlpmetrics.Resource) string {
	name := ""
	for _, kv := range res.Attributes {
		if kv.Key == clusterNameKey && kv.Value.StringValue != nil {
			name = *kv.Value.StringValue
		}
	}
	if name == "" {
		return sequencer.DefaultClusterName
	}
	return name
}

// translator is the truth's service graph translator of a cluster name, made as
// the sequencer makes its own.
func (p *pipeline) translator(name string) (*svcgraph.Translator, error) {
	if t := p.graph[name]; t != nil {
		return t, nil
	}
	t, err := svcgraph.New(svcgraph.Config{
		Producer:  lifecycle.Producer(svcgraph.Source + "/" + name),
		Resolver:  p.resolver,
		Expiry:    p.expiry,
		MaxSeries: svcgraph.DefaultMaxSeries,
	})
	if err != nil {
		return nil, err
	}
	p.graph[name] = t
	return t, nil
}

// feedTruth gives a request the sequencer took to the truth's translators, as the
// sequencer gives it to its own: the service graph points of each resource, at the
// instant the request was received, which is the clock. Every sighting is kept with
// the store's last sequence number, which the request's commit has set.
func (p *pipeline) feedTruth(d otlpmetrics.Data) error {
	received := p.clk.now()
	stamp := p.st.LastSeq()
	var touched []*svcgraph.Translator
	for _, res := range d.Resources {
		var points []otlpmetrics.SumPoint
		for _, pt := range res.Points {
			if pt.Scope == svcgraph.Scope && pt.Metric == svcgraph.Metric {
				points = append(points, pt)
			}
		}
		if len(points) == 0 {
			continue
		}
		name := clusterName(res)
		tr, err := p.translator(name)
		if err != nil {
			return err
		}
		if !slices.Contains(touched, tr) {
			touched = append(touched, tr)
		}
		p.noteNames(points)
		out := tr.Translate(otlpmetrics.Resource{Attributes: res.Attributes, NewestTimeUnixNano: res.NewestTimeUnixNano, Points: points}, received)
		for _, r := range out.Records {
			if r.Subject.Kind == store.SubjectEdge && r.Subject.Relation == catalog.DependsOn {
				p.truth.add(tkey{sub: r.Subject, prod: r.Producer}, r.EventTime, stamp)
			}
		}
	}
	for _, tr := range touched {
		tr.Commit()
	}
	return nil
}

// noteNames remembers what the services of the points are called.
func (p *pipeline) noteNames(points []otlpmetrics.SumPoint) {
	for _, pt := range points {
		attrs := otlpmetrics.Collect(pt.Attributes)
		for _, side := range []string{"client", "server"} {
			name, st := attrs.Get(side)
			if st != otlpmetrics.StatusOK {
				continue
			}
			ns, st := attrs.Get(side + "_" + otlpmetrics.KeyServiceNamespace)
			if st == otlpmetrics.StatusBad {
				continue
			}
			fp, err := p.fp.Service(ns, name)
			if err != nil {
				continue
			}
			label := name
			if ns != "" {
				label = ns + "/" + name
			}
			p.names[fp] = label
		}
	}
}

// label names an edge by its two services.
func (p *pipeline) label(sub store.Subject) string {
	name := func(fp identity.Fingerprint) string {
		if n, ok := p.names[fp]; ok {
			return n
		}
		return fp.String()
	}
	return name(sub.A) + " -> " + name(sub.B)
}
