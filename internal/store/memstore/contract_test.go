package memstore_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// callArgs are the arguments of a read. A read uses the ones it takes: t is the
// instant (for the windows, from), and to ends the windows.
type callArgs struct {
	fp  identity.Fingerprint
	fps []identity.Fingerprint
	dir store.Direction
	t   time.Time
	to  time.Time
	sc  store.Scope
}

// read is one of the five reads of the contract. call returns whether the read
// leaked a result alongside an error (every read returns the zero result with
// one), and the error.
type read struct {
	name        string
	takesDir    bool
	takesFP     bool // false for NeighborsBatch, which takes fps
	call        func(ctx context.Context, s *memstore.Store, a callArgs) (leaked bool, err error)
	answer      func(ctx context.Context, s *memstore.Store, a callArgs) (any, error)
	emptyResult bool // the answer for from >= to is nil, not just empty
}

var reads = []read{
	{
		name: "Neighbors", takesDir: true, takesFP: true,
		call: func(ctx context.Context, s *memstore.Store, a callArgs) (bool, error) {
			r, err := s.Neighbors(ctx, a.fp, a.dir, a.t, a.sc)
			return r != nil, err
		},
		answer: func(ctx context.Context, s *memstore.Store, a callArgs) (any, error) {
			return s.Neighbors(ctx, a.fp, a.dir, a.t, a.sc)
		},
	},
	{
		name: "NeighborsBatch", takesDir: true,
		call: func(ctx context.Context, s *memstore.Store, a callArgs) (bool, error) {
			r, err := s.NeighborsBatch(ctx, a.fps, a.dir, a.t, a.sc)
			return r != nil, err
		},
		answer: func(ctx context.Context, s *memstore.Store, a callArgs) (any, error) {
			return s.NeighborsBatch(ctx, a.fps, a.dir, a.t, a.sc)
		},
	},
	{
		name: "Alive", takesFP: true,
		call: func(ctx context.Context, s *memstore.Store, a callArgs) (bool, error) {
			r, err := s.Alive(ctx, a.fp, a.t, a.sc)
			return r, err
		},
		answer: func(ctx context.Context, s *memstore.Store, a callArgs) (any, error) {
			return s.Alive(ctx, a.fp, a.t, a.sc)
		},
	},
	{
		name: "Window", takesDir: true, takesFP: true, emptyResult: true,
		call: func(ctx context.Context, s *memstore.Store, a callArgs) (bool, error) {
			r, err := s.Window(ctx, a.fp, a.dir, a.t, a.to, a.sc)
			return r != nil, err
		},
		answer: func(ctx context.Context, s *memstore.Store, a callArgs) (any, error) {
			return s.Window(ctx, a.fp, a.dir, a.t, a.to, a.sc)
		},
	},
	{
		name: "EntityWindow", takesFP: true, emptyResult: true,
		call: func(ctx context.Context, s *memstore.Store, a callArgs) (bool, error) {
			r, err := s.EntityWindow(ctx, a.fp, a.t, a.to, a.sc)
			return r != nil, err
		},
		answer: func(ctx context.Context, s *memstore.Store, a callArgs) (any, error) {
			return s.EntityWindow(ctx, a.fp, a.t, a.to, a.sc)
		},
	},
}

// validArgs are arguments every read accepts, in the topology of w, with the
// instant in the middle of what populate writes.
func validArgs(w topology) callArgs {
	return callArgs{
		fp: w.pod, fps: []identity.Fingerprint{w.pod, w.node}, dir: store.Forward,
		t: at(6 * time.Minute), to: at(time.Hour), sc: l2,
	}
}

// populate writes four records in layer L2 from base to base+10m, with seqs 1 to 4:
// the pod's existence and its placement, each at the start and at the end.
func populate(t testing.TB, s *memstore.Store, w topology) {
	t.Helper()
	write(t, s,
		podRecord(w, 1, "k8s", 0, lifecycle.Observe, "x"),
		placed(w, w.node, 2, "k8s", 0, lifecycle.Observe, 0),
		podRecord(w, 3, "k8s", 10*time.Minute, lifecycle.Observe, "y"),
		placed(w, w.node, 4, "k8s", 10*time.Minute, lifecycle.Observe, 0),
	)
}

func canceled() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func expired() context.Context {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Hour))
	cancel()
	return ctx
}

func TestAfterCloseEveryMethodIsRefused(t *testing.T) {
	t.Parallel()
	w := newTopology(t)
	s, err := memstore.Open(memstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	populate(t, s, w)
	if err := s.Retain(context.Background(), at(5*time.Minute)); err != nil {
		t.Fatal(err)
	}
	lastSeq, horizon := s.LastSeq(), s.Horizon()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	a := validArgs(w)
	argSets := map[string]callArgs{
		"valid arguments":               a,
		"an invalid scope":              func() callArgs { b := a; b.sc = store.Scope{}; return b }(),
		"an invalid direction":          func() callArgs { b := a; b.dir = 0; return b }(),
		"the zero fingerprint":          func() callArgs { b := a; b.fp, b.fps = identity.Fingerprint{}, []identity.Fingerprint{{}}; return b }(),
		"an instant before the horizon": func() callArgs { b := a; b.t = at(0); return b }(),
		"empty fingerprints":            func() callArgs { b := a; b.fps = nil; return b }(),
	}
	contexts := map[string]context.Context{"a live context": context.Background(), "a cancelled context": canceled(), "an expired context": expired()}
	for _, r := range reads {
		for argName, args := range argSets {
			for ctxName, ctx := range contexts {
				leaked, err := r.call(ctx, s, args)
				if !errors.Is(err, store.ErrClosed) {
					t.Errorf("%s with %s and %s after Close: err = %v, want ErrClosed", r.name, argName, ctxName, err)
				}
				if leaked {
					t.Errorf("%s with %s after Close returned a result with its error", r.name, argName)
				}
			}
		}
	}

	valid := []store.Record{placed(w, w.node, 5, "k8s", 6*time.Minute, lifecycle.Observe, 0)}
	invalid := []store.Record{{}}
	for name, batch := range map[string][]store.Record{"a batch": valid, "an invalid batch": invalid, "an empty batch": {}, "a nil batch": nil} {
		for ctxName, ctx := range contexts {
			if err := s.Write(ctx, batch); !errors.Is(err, store.ErrClosed) {
				t.Errorf("Write of %s with %s after Close: err = %v, want ErrClosed", name, ctxName, err)
			}
		}
	}
	for ctxName, ctx := range contexts {
		if err := s.Retain(ctx, at(6*time.Minute)); !errors.Is(err, store.ErrClosed) {
			t.Errorf("Retain with %s after Close: err = %v, want ErrClosed", ctxName, err)
		}
	}

	if got := s.LastSeq(); got != lastSeq {
		t.Errorf("LastSeq after Close = %d, want %d", got, lastSeq)
	}
	if got := s.Horizon(); !got.Time.Equal(horizon.Time) || got.Seq != horizon.Seq || got.IsZero() {
		t.Errorf("Horizon after Close = %+v, want %+v", got, horizon)
	}
	for range 2 {
		if err := s.Close(); err != nil {
			t.Errorf("a second Close = %v, want nil", err)
		}
	}
	if got := s.Layers(w.pod); got != nil {
		t.Errorf("Layers after Close = %v, want nil", got)
	}
	if got := s.LastSeq(); got != lastSeq {
		t.Errorf("LastSeq after a second Close = %d, want %d", got, lastSeq)
	}
}

func TestACancelledContextRefusesEveryMethod(t *testing.T) {
	t.Parallel()
	w := newTopology(t)
	contexts := map[string]struct {
		ctx  context.Context
		want error
	}{
		"cancelled": {canceled(), context.Canceled},
		"expired":   {expired(), context.DeadlineExceeded},
	}
	for name, c := range contexts {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s := open(t)
			populate(t, s, w)
			for _, r := range reads {
				leaked, err := r.call(c.ctx, s, validArgs(w))
				if !errors.Is(err, c.want) {
					t.Errorf("%s: err = %v, want it to wrap %v", r.name, err, c.want)
				}
				if leaked {
					t.Errorf("%s returned a result with its error", r.name)
				}
			}

			// A write with a context error stores nothing, and its sequence numbers
			// stay unused: the same batch is accepted afterwards.
			batch := []store.Record{
				placed(w, w.node2, 5, "k8s", 6*time.Minute, lifecycle.Observe, 0),
				placed(w, w.node2, 6, "k8s", 7*time.Minute, lifecycle.Observe, 0),
			}
			before := s.LastSeq()
			if err := s.Write(c.ctx, batch); !errors.Is(err, c.want) {
				t.Errorf("Write: err = %v, want it to wrap %v", err, c.want)
			}
			if err := s.Write(c.ctx, nil); !errors.Is(err, c.want) {
				t.Errorf("Write of an empty batch: err = %v, want it to wrap %v", err, c.want)
			}
			if got := s.LastSeq(); got != before {
				t.Errorf("a refused Write moved LastSeq from %d to %d", before, got)
			}
			if ns := neighbors(t, s, w.pod, store.Forward, at(8*time.Minute)); len(ns) != 1 {
				t.Errorf("a refused Write left %v behind", ns)
			}
			if err := s.Write(context.Background(), batch); err != nil {
				t.Errorf("the same batch with a live context: %v", err)
			}

			// A Retain with a context error leaves the horizon alone.
			if err := s.Retain(c.ctx, at(5*time.Minute)); !errors.Is(err, c.want) {
				t.Errorf("Retain: err = %v, want it to wrap %v", err, c.want)
			}
			if h := s.Horizon(); !h.IsZero() {
				t.Errorf("a refused Retain moved the horizon to %+v", h)
			}
		})
	}
}

func TestArgumentsAreCheckedBeforeTheContextAndTheContextBeforeTheHorizon(t *testing.T) {
	t.Parallel()
	w := newTopology(t)
	s := open(t)
	populate(t, s, w)
	if err := s.Retain(context.Background(), at(5*time.Minute)); err != nil {
		t.Fatal(err)
	}
	for _, r := range reads {
		// An instant before the horizon, a cancelled context and an invalid scope:
		// the scope is what is reported.
		a := validArgs(w)
		a.t, a.sc = at(0), store.Scope{}
		if _, err := r.call(canceled(), s, a); !errors.Is(err, store.ErrInvalid) || errors.Is(err, context.Canceled) || errors.Is(err, store.ErrBeforeHorizon) {
			t.Errorf("%s with an invalid scope, a cancelled context and an old instant: err = %v, want only ErrInvalid", r.name, err)
		}
		// With valid arguments, the context is reported before the horizon.
		a.sc = l2
		if _, err := r.call(canceled(), s, a); !errors.Is(err, context.Canceled) || errors.Is(err, store.ErrBeforeHorizon) {
			t.Errorf("%s with a cancelled context and an old instant: err = %v, want only context.Canceled", r.name, err)
		}
		if _, err := r.call(context.Background(), s, a); !errors.Is(err, store.ErrBeforeHorizon) {
			t.Errorf("%s with an old instant: err = %v, want ErrBeforeHorizon", r.name, err)
		}
	}
}

func TestInvalidArgumentsAreRefused(t *testing.T) {
	t.Parallel()
	w := newTopology(t)
	zero := identity.Fingerprint{}

	type change struct {
		name string
		mod  func(*callArgs)
		only func(read) bool // the reads it applies to; nil for all
	}
	changes := []change{
		{"scope layer 0", func(a *callArgs) { a.sc.Layer = 0 }, nil},
		{"scope layer above L3", func(a *callArgs) { a.sc.Layer = catalog.L3 + 1 }, nil},
		{"direction 0", func(a *callArgs) { a.dir = 0 }, func(r read) bool { return r.takesDir }},
		{"direction 3", func(a *callArgs) { a.dir = 3 }, func(r read) bool { return r.takesDir }},
		{"the zero fingerprint", func(a *callArgs) { a.fp = zero }, func(r read) bool { return r.takesFP }},
		{"the zero fingerprint first among valid ones", func(a *callArgs) { a.fps = []identity.Fingerprint{zero, w.pod, w.node} }, func(r read) bool { return !r.takesFP }},
		{"the zero fingerprint in the middle", func(a *callArgs) { a.fps = []identity.Fingerprint{w.pod, zero, w.node} }, func(r read) bool { return !r.takesFP }},
		{"the zero fingerprint last", func(a *callArgs) { a.fps = []identity.Fingerprint{w.pod, w.node, zero} }, func(r read) bool { return !r.takesFP }},
		{"only the zero fingerprint", func(a *callArgs) { a.fps = []identity.Fingerprint{zero} }, func(r read) bool { return !r.takesFP }},
	}

	for _, withHorizon := range []bool{false, true} {
		t.Run(fmt.Sprint("horizon ", withHorizon), func(t *testing.T) {
			t.Parallel()
			s := open(t)
			populate(t, s, w)
			if withHorizon {
				if err := s.Retain(context.Background(), at(5*time.Minute)); err != nil {
					t.Fatal(err)
				}
			}
			for _, r := range reads {
				for _, c := range changes {
					if c.only != nil && !c.only(r) {
						continue
					}
					a := validArgs(w)
					c.mod(&a)
					if withHorizon {
						a.t = at(0) // before the horizon: the arguments are still what is reported
						a.sc.AsOf = 0
					}
					leaked, err := r.call(context.Background(), s, a)
					if !errors.Is(err, store.ErrInvalid) {
						t.Errorf("%s with %s: err = %v, want ErrInvalid", r.name, c.name, err)
					}
					if errors.Is(err, store.ErrBeforeHorizon) {
						t.Errorf("%s with %s: err = %v, must not wrap ErrBeforeHorizon", r.name, c.name, err)
					}
					if leaked {
						t.Errorf("%s with %s returned a result with its error", r.name, c.name)
					}
				}
			}
		})
	}
}

func TestEmptyReadsAreAnsweredWithoutError(t *testing.T) {
	t.Parallel()
	w := newTopology(t)
	s := open(t)
	populate(t, s, w)
	ctx := context.Background()

	for _, r := range reads {
		if !r.emptyResult {
			continue
		}
		for name, to := range map[string]time.Time{"from == to": at(6 * time.Minute), "from after to": at(5 * time.Minute), "far after": at(-time.Hour)} {
			a := validArgs(w)
			a.to = to
			res, err := r.answer(ctx, s, a)
			if err != nil {
				t.Errorf("%s with %s: %v", r.name, name, err)
			}
			if rs, _ := res.([]store.Record); rs != nil {
				t.Errorf("%s with %s = %v, want nil", r.name, name, rs)
			}
		}
	}
	got, err := s.NeighborsBatch(ctx, nil, store.Forward, at(0), l2)
	if err != nil || len(got) != 0 {
		t.Errorf("NeighborsBatch(nil) = %v, %v; want an empty result", got, err)
	}
	got, err = s.NeighborsBatch(ctx, []identity.Fingerprint{}, store.Reverse, at(0), l2)
	if err != nil || len(got) != 0 {
		t.Errorf("NeighborsBatch of no fingerprints = %v, %v; want an empty result", got, err)
	}
}

func TestWithoutAHorizonEveryInstantIsAnswered(t *testing.T) {
	t.Parallel()
	w := newTopology(t)
	s := open(t)
	populate(t, s, w)
	instants := map[string]time.Time{
		"the zero time":                   {},
		"year 9999":                       time.Date(9999, 12, 31, 0, 0, 0, 0, time.UTC),
		"a nanosecond before the minimum": store.MinEventTime.Add(-1),
		"the minimum":                     store.MinEventTime,
		"the maximum":                     store.MaxEventTime,
		"a nanosecond after the maximum":  store.MaxEventTime.Add(1),
		"before 1970":                     time.Date(1960, 1, 1, 0, 0, 0, 0, time.UTC),
		"a fixed zone":                    at(6 * time.Minute).In(time.FixedZone("x", -5*3600)),
	}
	for _, r := range reads {
		for name, when := range instants {
			for _, token := range []uint64{0, 1, store.Latest} {
				a := validArgs(w)
				a.t, a.sc.AsOf = when, token
				a.to = when.Add(time.Hour)
				if _, err := r.call(context.Background(), s, a); err != nil {
					t.Errorf("%s at %s as of %d: %v", r.name, name, token, err)
				}
			}
		}
	}

	// The instant means what it says at the edges of the range.
	if alive, err := s.Alive(context.Background(), w.pod, time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC), l2); err != nil || !alive {
		t.Errorf("Alive in 9999 = %v, %v; want true: the record has no TTL", alive, err)
	}
	if alive, err := s.Alive(context.Background(), w.pod, store.MinEventTime.Add(-1), l2); err != nil || alive {
		t.Errorf("Alive before the minimum = %v, %v; want false", alive, err)
	}
}

func TestTheHorizonRefusesEarlierReads(t *testing.T) {
	t.Parallel()
	w := newTopology(t)
	s := open(t)
	ctx := context.Background()
	populate(t, s, w)
	const n = 4
	h := at(5 * time.Minute)
	if got := s.Horizon(); !got.IsZero() {
		t.Fatalf("the horizon before any Retain is %+v", got)
	}
	if err := s.Retain(ctx, h); err != nil {
		t.Fatal(err)
	}
	if got := s.Horizon(); !got.Time.Equal(h) || got.Time.Location() != time.UTC || got.Seq != n {
		t.Fatalf("Horizon = %+v, want {%s, %d}", got, h, n)
	}

	rows := []struct {
		name    string
		t       time.Time
		to      time.Time
		asOf    uint64
		refused bool
	}{
		{"a nanosecond before the horizon", h.Add(-1), h.Add(time.Hour), n, true},
		{"a nanosecond before the horizon, as of Latest", h.Add(-1), h.Add(time.Hour), store.Latest, true},
		{"long before the horizon", at(0), h.Add(time.Hour), n, true},
		{"the horizon", h, h.Add(time.Hour), n, false},
		{"after the horizon", h.Add(time.Minute), h.Add(time.Hour), n, false},
		{"an old instant with from == to", h.Add(-1), h.Add(-1), n, true},
		{"an old instant with from after to", h.Add(-1), h.Add(-time.Hour), n, true},
		{"the horizon with from == to", h, h, n, false},
		{"the token below the horizon's", h, h.Add(time.Hour), n - 1, true},
		{"token 0", h, h.Add(time.Hour), 0, true},
		{"the horizon's token", h, h.Add(time.Hour), n, false},
		{"a token above LastSeq", h, h.Add(time.Hour), n + 100, false},
		{"Latest", h, h.Add(time.Hour), store.Latest, false},
		{"both too old", h.Add(-time.Hour), h.Add(time.Hour), 0, true},
	}
	for _, r := range reads {
		for _, row := range rows {
			a := validArgs(w)
			a.t, a.to, a.sc.AsOf = row.t, row.to, row.asOf
			leaked, err := r.call(ctx, s, a)
			switch {
			case row.refused && (!errors.Is(err, store.ErrBeforeHorizon) || !errors.Is(err, store.ErrInvalid)):
				t.Errorf("%s, %s: err = %v, want ErrBeforeHorizon", r.name, row.name, err)
			case !row.refused && err != nil:
				t.Errorf("%s, %s: %v", r.name, row.name, err)
			}
			if row.refused && leaked {
				t.Errorf("%s, %s: a result came with the error", r.name, row.name)
			}
		}
	}

	// A write before the horizon is refused, whole; one exactly at it is accepted.
	err := s.Write(ctx, []store.Record{placed(w, w.node2, 5, "k8s", 5*time.Minute-1, lifecycle.Observe, 0)})
	if !errors.Is(err, store.ErrBeforeHorizon) {
		t.Errorf("a record a nanosecond before the horizon: err = %v, want ErrBeforeHorizon", err)
	}
	if got := s.LastSeq(); got != n {
		t.Errorf("a refused write moved LastSeq to %d", got)
	}
	if err := s.Write(ctx, []store.Record{placed(w, w.node2, 5, "k8s", 5*time.Minute, lifecycle.Observe, 0)}); err != nil {
		t.Errorf("a record exactly at the horizon: %v", err)
	}

	// A Retain that does not move the horizon changes nothing, and does not raise
	// its token to the new LastSeq.
	for _, earlier := range []time.Time{at(4 * time.Minute), h, at(-time.Hour)} {
		if err := s.Retain(ctx, earlier); err != nil {
			t.Fatal(err)
		}
		if got := s.Horizon(); !got.Time.Equal(h) || got.Seq != n {
			t.Errorf("after Retain(%s) the horizon is %+v, want {%s, %d}", earlier.Sub(base), got, h, n)
		}
		if _, err := s.Alive(ctx, w.pod, h, store.Scope{Layer: catalog.L2, AsOf: n}); err != nil {
			t.Errorf("a read as of the old horizon's token after Retain(%s): %v", earlier.Sub(base), err)
		}
	}
	if err := s.Retain(ctx, at(6*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if got := s.Horizon(); !got.Time.Equal(at(6*time.Minute)) || got.Seq != s.LastSeq() || got.Seq != 5 {
		t.Errorf("Horizon after a later Retain = %+v, want {%s, %d}", got, at(6*time.Minute), s.LastSeq())
	}
	if _, err := s.Alive(ctx, w.pod, at(6*time.Minute), store.Scope{Layer: catalog.L2, AsOf: n}); !errors.Is(err, store.ErrBeforeHorizon) {
		t.Errorf("a read as of the old token after the horizon moved: err = %v, want ErrBeforeHorizon", err)
	}
}

func TestRetainComparesInstantsNotRepresentations(t *testing.T) {
	t.Parallel()
	w := newTopology(t)
	ctx := context.Background()

	t.Run("another location", func(t *testing.T) {
		t.Parallel()
		s := open(t)
		populate(t, s, w)
		h := at(5 * time.Minute).In(time.FixedZone("x", 9*3600))
		if err := s.Retain(ctx, h); err != nil {
			t.Fatal(err)
		}
		got := s.Horizon()
		if !got.Time.Equal(h) || got.Time.Location() != time.UTC {
			t.Errorf("Horizon time = %s, want %s in UTC", got.Time, h)
		}
		if _, err := s.Alive(ctx, w.pod, at(5*time.Minute).Add(-1), l2); !errors.Is(err, store.ErrBeforeHorizon) {
			t.Errorf("a read a nanosecond before: err = %v, want ErrBeforeHorizon", err)
		}
		if _, err := s.Alive(ctx, w.pod, at(5*time.Minute).In(time.FixedZone("y", -3600)), l2); err != nil {
			t.Errorf("a read at the horizon, in another zone: %v", err)
		}
		// An earlier instant in a later-looking zone does not move the horizon.
		if err := s.Retain(ctx, at(4*time.Minute).In(time.FixedZone("z", -10*3600))); err != nil {
			t.Fatal(err)
		}
		if got := s.Horizon(); !got.Time.Equal(h) {
			t.Errorf("Horizon moved to %s", got.Time)
		}
	})

	t.Run("a monotonic clock reading", func(t *testing.T) {
		t.Parallel()
		s := open(t)
		now := time.Now()
		if !strings.Contains(now.String(), "m=") {
			t.Skip("this clock gives no monotonic reading")
		}
		write(t, s, placed(w, w.node, 1, "k8s", 0, lifecycle.Observe, 0))
		if err := s.Retain(ctx, now); err != nil {
			t.Fatal(err)
		}
		got := s.Horizon()
		if strings.Contains(got.Time.String(), "m=") || got.Time.Location() != time.UTC || !got.Time.Equal(now) {
			t.Errorf("Horizon time = %s, want %s in UTC without a monotonic reading", got.Time, now.UTC())
		}
		before := now.Round(0).Add(-time.Nanosecond) // a reading without the monotonic clock
		if _, err := s.Alive(ctx, w.pod, before, l2); !errors.Is(err, store.ErrBeforeHorizon) {
			t.Errorf("a read a nanosecond before: err = %v, want ErrBeforeHorizon", err)
		}
		if _, err := s.Alive(ctx, w.pod, now.Round(0), l2); err != nil {
			t.Errorf("a read at the horizon: %v", err)
		}
		if _, err := s.Alive(ctx, w.pod, now, l2); err != nil {
			t.Errorf("a read at the horizon with a monotonic reading: %v", err)
		}
	})
}

func TestATokenAboveLastSeqIsLastSeq(t *testing.T) {
	t.Parallel()
	w := newTopology(t)
	s := open(t)
	ctx := context.Background()
	populate(t, s, w)

	answer := func(r read, asOf uint64) string {
		a := validArgs(w)
		a.t, a.to, a.sc.AsOf = at(11*time.Minute), at(time.Hour), asOf
		res, err := r.answer(ctx, s, a)
		if err != nil {
			t.Fatalf("%s as of %d: %v", r.name, asOf, err)
		}
		return fmt.Sprintf("%+v", res)
	}
	for _, r := range reads {
		for _, above := range []uint64{s.LastSeq() + 1, s.LastSeq() + 5, 1 << 62} {
			if got, want := answer(r, above), answer(r, store.Latest); got != want {
				t.Errorf("%s as of %d = %s, want what Latest gives, %s", r.name, above, got, want)
			}
		}
	}

	// A record written later, with a seq the token already named, is seen by it.
	token := s.LastSeq() + 5
	later := placed(w, w.node2, token, "k8s", 8*time.Minute, lifecycle.Observe, 0)
	write(t, s, later)
	if got := s.LastSeq(); got != token {
		t.Fatalf("LastSeq = %d, want %d", got, token)
	}
	sc := store.Scope{Layer: catalog.L2, AsOf: token}
	if ns, err := s.Neighbors(ctx, w.pod, store.Forward, at(9*time.Minute), sc); err != nil || len(ns) != 2 {
		t.Errorf("a token that named the seq before it was written sees %v, %v; want both nodes", ns, err)
	}
	sc.AsOf = token - 1
	if ns, err := s.Neighbors(ctx, w.pod, store.Forward, at(9*time.Minute), sc); err != nil || len(ns) != 1 {
		t.Errorf("the token before sees %v, %v; want one node", ns, err)
	}
}

func TestWriteRulesBeyondTheTable(t *testing.T) {
	t.Parallel()
	w := newTopology(t)
	ctx := context.Background()

	t.Run("an empty batch changes nothing", func(t *testing.T) {
		t.Parallel()
		s := open(t)
		populate(t, s, w)
		for _, batch := range [][]store.Record{nil, {}} {
			if err := s.Write(ctx, batch); err != nil {
				t.Errorf("Write(%v) = %v, want nil", batch, err)
			}
		}
		if got := s.LastSeq(); got != 4 {
			t.Errorf("LastSeq = %d, want 4", got)
		}
	})

	t.Run("sequence numbers", func(t *testing.T) {
		t.Parallel()
		good := func(seq uint64) store.Record {
			return placed(w, w.node, seq, "k8s", time.Duration(seq)*time.Second, lifecycle.Observe, 0)
		}
		tests := map[string]struct {
			prior []store.Record
			batch []store.Record
		}{
			"seq 0":                       {nil, []store.Record{good(0)}},
			"seq 0 after others":          {nil, []store.Record{good(3), good(0)}},
			"a repeated seq":              {nil, []store.Record{good(3), good(3)}},
			"descending within a batch":   {nil, []store.Record{good(5), good(4)}},
			"equal to the last seq":       {[]store.Record{good(3)}, []store.Record{good(3)}},
			"below the last seq":          {[]store.Record{good(3)}, []store.Record{good(2)}},
			"a valid record then a stale": {[]store.Record{good(3)}, []store.Record{good(4), good(3)}},
			"seq equal to Latest":         {nil, []store.Record{good(store.Latest)}},
			"Latest after valid records":  {nil, []store.Record{good(1), good(2), good(store.Latest)}},
			"Latest after a prior write":  {[]store.Record{good(3)}, []store.Record{good(store.Latest)}},
		}
		for name, tt := range tests {
			s := open(t)
			if tt.prior != nil {
				write(t, s, tt.prior...)
			}
			before := s.LastSeq()
			if err := s.Write(ctx, tt.batch); !errors.Is(err, store.ErrInvalid) || errors.Is(err, store.ErrBeforeHorizon) {
				t.Errorf("%s: err = %v, want ErrInvalid", name, err)
			}
			if got := s.LastSeq(); got != before {
				t.Errorf("%s: LastSeq moved from %d to %d", name, before, got)
			}
		}
	})

	t.Run("the largest seq below Latest is accepted", func(t *testing.T) {
		t.Parallel()
		s := open(t)
		top := placed(w, w.node, store.Latest-1, "k8s", 0, lifecycle.Observe, 0)
		if err := s.Write(ctx, []store.Record{top}); err != nil {
			t.Fatalf("seq %d: %v", top.Seq, err)
		}
		if got := s.LastSeq(); got != store.Latest-1 {
			t.Errorf("LastSeq = %d, want %d", got, store.Latest-1)
		}
		// Nothing can follow it, because the next seq would be Latest.
		next := placed(w, w.node, store.Latest, "k8s", time.Second, lifecycle.Observe, 0)
		if err := s.Write(ctx, []store.Record{next}); !errors.Is(err, store.ErrInvalid) {
			t.Errorf("seq Latest after seq Latest-1: err = %v, want ErrInvalid", err)
		}
		if got := s.LastSeq(); got != store.Latest-1 {
			t.Errorf("LastSeq = %d after a refused write, want %d", got, store.Latest-1)
		}
	})

	t.Run("a refused batch stores none of its valid records", func(t *testing.T) {
		t.Parallel()
		s := open(t)
		good := func(seq uint64, node identity.Fingerprint) store.Record {
			return placed(w, node, seq, "k8s", time.Duration(seq)*time.Second, lifecycle.Observe, 0)
		}
		bad := good(3, w.node)
		bad.Producer = ""
		batch := []store.Record{good(1, w.node), good(2, w.node2), bad, good(4, w.node)}
		if err := s.Write(ctx, batch); !errors.Is(err, store.ErrInvalid) {
			t.Fatalf("err = %v, want ErrInvalid", err)
		}
		if got := s.LastSeq(); got != 0 {
			t.Errorf("LastSeq = %d after a refused batch", got)
		}
		if got := s.Layers(w.pod); len(got) != 0 {
			t.Errorf("a refused batch left the layers %v", got)
		}
		if ns := neighbors(t, s, w.pod, store.Forward, at(time.Hour)); len(ns) != 0 {
			t.Errorf("a refused batch left %v behind", ns)
		}
		if recs, err := s.Window(ctx, w.pod, store.Forward, at(0), at(time.Hour), l2); err != nil || len(recs) != 0 {
			t.Errorf("a refused batch left the records %v (%v)", seqs(recs), err)
		}
		// The valid records, without the bad one, are accepted with the seqs they had.
		if err := s.Write(ctx, []store.Record{batch[0], batch[1], batch[3]}); err != nil {
			t.Fatal(err)
		}
		if got := s.LastSeq(); got != 4 {
			t.Errorf("LastSeq = %d, want 4", got)
		}
		if ns := neighbors(t, s, w.pod, store.Forward, at(time.Hour)); len(ns) != 2 {
			t.Errorf("neighbors = %v, want both nodes", ns)
		}
	})

	t.Run("payloads are copied in and out", func(t *testing.T) {
		t.Parallel()
		s := open(t)
		edge := placed(w, w.node, 1, "k8s", 0, lifecycle.Observe, 0)
		edge.Payload = []byte("edge")
		entity := podRecord(w, 2, "k8s", 0, lifecycle.Observe, "entity")
		write(t, s, edge, entity)
		// The caller reuses its buffers.
		for _, b := range [][]byte{edge.Payload, entity.Payload} {
			for i := range b {
				b[i] = '!'
			}
		}
		wantPayloads := func(when string) {
			t.Helper()
			ws, err := s.Window(ctx, w.pod, store.Forward, at(0), at(time.Hour), l2)
			if err != nil || len(ws) != 1 || string(ws[0].Payload) != "edge" {
				t.Errorf("%s: Window = %+v, %v; want payload %q", when, ws, err, "edge")
			}
			es, err := s.EntityWindow(ctx, w.pod, at(0), at(time.Hour), l2)
			if err != nil || len(es) != 1 || string(es[0].Payload) != "entity" {
				t.Errorf("%s: EntityWindow = %+v, %v; want payload %q", when, es, err, "entity")
			}
		}
		wantPayloads("after the caller changed what it wrote")

		// And what the store returns is the caller's.
		ws, _ := s.Window(ctx, w.pod, store.Forward, at(0), at(time.Hour), l2)
		es, _ := s.EntityWindow(ctx, w.pod, at(0), at(time.Hour), l2)
		for _, b := range [][]byte{ws[0].Payload, es[0].Payload} {
			for i := range b {
				b[i] = '?'
			}
		}
		wantPayloads("after the caller changed what it read")
	})
}
