package mcp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
	"github.com/lotannauo/toposhift/internal/store/pebblestore"
)

// The bounds on calls. All three numbers are placeholders until a read budget is
// measured on a real store; none of them has been.
const (
	// DefaultMaxConcurrent is how many calls read the store at once.
	DefaultMaxConcurrent = 2
	// DefaultBusyAfter is how long a call waits for a free slot before it is
	// refused as busy.
	DefaultBusyAfter = 5 * time.Second
	// DefaultCallTimeout is the deadline of one call, queueing excluded.
	DefaultCallTimeout = 30 * time.Second
)

// Config configures a [Source].
type Config struct {
	// Store is the store the tools read. The source reads it through a wrapper
	// that never writes, retains or closes it. Required.
	Store store.Store
	// Rank is the authority order of descriptions the store was opened with
	// (lifecycle.Policy.Rank). It decides which producer's description a name or
	// a payload comes from, so a source built with other ranks gives other names.
	Rank map[lifecycle.Producer]int
	// RangeReads says the range tools may run on Store. It is false unless the
	// caller sets it, and [RangeReadsFor] is the policy: true for memstore and
	// pebblestore. While it is false each range tool answers that range answers
	// are not available, and no name is read.
	RangeReads bool
	// MaxConcurrent, BusyAfter and CallTimeout bound the calls; zero means the
	// default.
	MaxConcurrent int
	BusyAfter     time.Duration
	CallTimeout   time.Duration
	// Logf receives one line for each internal error. Nil discards them.
	Logf func(format string, args ...any)
}

// RangeReadsFor says whether the range tools may run on st: true for the two
// stores of this module, memstore and pebblestore. Both read a window from each
// reference's seed, so a range read costs its window and a walk to the seeds, not
// the prefix's history; pebblestore's walk is bounded under the conditions of its
// seed-walk budget, and memstore is the reference. Any other store, a wrapper of
// one of these included, gets false: its reads are not known to be the inner
// store's, and its range tools answer that range answers are not available.
func RangeReadsFor(st store.Store) bool {
	switch st.(type) {
	case *memstore.Store, *pebblestore.Store:
		return true
	}
	return false
}

// Source answers the tools over one store. It is safe for concurrent use.
type Source struct {
	st         store.Store
	lister     Lister
	rank       map[lifecycle.Producer]int
	rankDigest string
	rangeReads bool
	cat        *catalog.Catalog
	resolver   *identity.Resolver
	logf       func(format string, args ...any)
	// hooks is for tests; it is nil in production.
	hooks *egressHooks

	gate        chan struct{}
	busyAfter   time.Duration
	callTimeout time.Duration

	base   context.Context
	cancel context.CancelFunc

	mu       sync.Mutex
	draining bool
	inflight sync.WaitGroup
}

// NewSource returns a source over cfg.Store. The resolver it builds is exact:
// it never ignores an attribute that does not identify.
func NewSource(cfg Config) (*Source, error) {
	if cfg.Store == nil {
		return nil, errors.New("mcp: NewSource: no store")
	}
	if cfg.MaxConcurrent < 0 || cfg.BusyAfter < 0 || cfg.CallTimeout < 0 {
		return nil, errors.New("mcp: NewSource: a bound is negative")
	}
	maxConc := cfg.MaxConcurrent
	if maxConc == 0 {
		maxConc = DefaultMaxConcurrent
	}
	s := &Source{
		st:          readOnly{cfg.Store},
		rank:        maps.Clone(cfg.Rank),
		rangeReads:  cfg.RangeReads,
		cat:         catalog.Default(),
		resolver:    identity.NewResolver(catalog.Default()),
		logf:        cfg.Logf,
		gate:        make(chan struct{}, maxConc),
		busyAfter:   cfg.BusyAfter,
		callTimeout: cfg.CallTimeout,
	}
	if s.busyAfter == 0 {
		s.busyAfter = DefaultBusyAfter
	}
	if s.callTimeout == 0 {
		s.callTimeout = DefaultCallTimeout
	}
	// The store given, not the read-only wrapper, which hides the method.
	if l, ok := cfg.Store.(Lister); ok {
		s.lister = l
	}
	s.rankDigest = digestRank(s.rank)
	s.base, s.cancel = context.WithCancel(context.Background())
	return s, nil
}

// RangeReads says whether the range tools run on this source's store.
func (s *Source) RangeReads() bool { return s.rangeReads }

// digestRank is a short digest of the rank map: the same ranks give the same
// digest in any process, and other ranks give another one. Ranks are part of the
// world an answer was read in, because they decide which description wins.
func digestRank(rank map[lifecycle.Producer]int) string {
	h := sha256.New()
	for _, p := range slices.Sorted(maps.Keys(rank)) {
		fmt.Fprintf(h, "%d:%s=%d\n", len(p), string(p), rank[p])
	}
	return hex.EncodeToString(h.Sum(nil)[:8])
}

// readOnly is the store the query packages see: Write and Retain fail with an
// error wrapping [store.ErrInvalid], and Close does nothing. The source never
// calls them, and a package that did would find them closed.
type readOnly struct{ store.Store }

var errReadOnly = fmt.Errorf("the tools never write to the store: %w", store.ErrInvalid)

func (readOnly) Write(context.Context, []store.Record) error { return errReadOnly }

func (readOnly) Retain(context.Context, time.Time) error { return errReadOnly }

func (readOnly) Close() error { return nil }

// begin takes a slot for one call and derives its context. The context ends
// when the caller's does, when the source is drained, or when the call's
// deadline passes. The returned function releases the slot and must be called
// once, after the call's last read.
//
// A call is counted as in flight from before it waits for a slot, so a drain
// waits for the queued calls too.
func (s *Source) begin(ctx context.Context) (context.Context, func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	s.mu.Lock()
	if s.draining {
		s.mu.Unlock()
		return nil, nil, ErrShuttingDown
	}
	s.inflight.Add(1)
	s.mu.Unlock()

	timer := time.NewTimer(s.busyAfter)
	defer timer.Stop()
	select {
	case s.gate <- struct{}{}:
	case <-timer.C:
		s.inflight.Done()
		return nil, nil, ErrBusy
	case <-ctx.Done():
		s.inflight.Done()
		return nil, nil, ctx.Err()
	case <-s.base.Done():
		s.inflight.Done()
		return nil, nil, ErrShuttingDown
	}

	callCtx, cancel := context.WithCancel(s.base)
	stop := context.AfterFunc(ctx, cancel)
	callCtx, cancelTimeout := context.WithTimeout(callCtx, s.callTimeout)
	var once sync.Once
	release := func() {
		once.Do(func() {
			cancelTimeout()
			stop()
			cancel()
			<-s.gate
			s.inflight.Done()
		})
	}
	return callCtx, release, nil
}

// Drain stops the source taking calls and waits for the ones in flight. It
// withdraws the store, so a new call is refused as shutting down, and cancels
// the context every call's context derives from, so each running call ends
// within one store read. It returns when every call, running or queued at the
// gate, has returned, or with ctx's error if ctx ends first. A caller that gets
// an error must not close the store: a call may still be reading it.
func (s *Source) Drain(ctx context.Context) error {
	s.mu.Lock()
	s.draining = true
	s.mu.Unlock()
	s.cancel()
	done := make(chan struct{})
	go func() {
		s.inflight.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Source) isDraining() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.draining
}

// run is the frame of every tool that reads the store: take a slot, refuse a
// range tool on a store without range reads, run f, check that the answer prints
// no endpoint id the request did not name, and turn any error into a sentence.
// req is what the request named; f may add to it.
func run[Out any](s *Source, ctx context.Context, needsRange bool, req *request, f func(context.Context) (Out, error)) (Out, error) {
	var zero Out
	ctx, release, err := s.begin(ctx)
	if err != nil {
		return zero, s.fail(err, req)
	}
	defer release()
	if needsRange && !s.rangeReads {
		return zero, s.fail(ErrNoRangeReads, req)
	}
	out, err := f(ctx)
	if err != nil {
		return zero, s.fail(err, req)
	}
	if err := s.egress(&out, req); err != nil {
		return zero, s.fail(err, req)
	}
	return out, nil
}

// pin resolves the token of one answer, once. An absent asof is the store's
// LastSeq; a token above it is clamped to it, and the sentence says so. A token
// below the retention horizon is refused by the first read at it.
func (s *Source) pin(asof string) (token uint64, ahead string, err error) {
	last := s.st.LastSeq()
	if asof == "" {
		return last, "", nil
	}
	n, err := strconv.ParseUint(asof, 10, 64)
	if err != nil || strconv.FormatUint(n, 10) != asof {
		return 0, "", invalid("The argument \"asof\" must be a token as a decimal number, such as 81234; got %s.", quote(asof))
	}
	if n > last {
		return last, fmt.Sprintf("Token %d is ahead of the store; answered at %d.", n, last), nil
	}
	return n, "", nil
}
