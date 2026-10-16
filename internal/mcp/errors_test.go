package mcp

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
	"github.com/lotannauo/toposhift/internal/store/pebblestore"
)

// The sentence for an error is chosen in a fixed order, whatever else the error
// wraps: cancellation, a closing store, the retention horizon (before the
// ErrInvalid it wraps), a description recorded before the horizon, an invalid argument, and last an
// internal error.
func TestSentenceOrder(t *testing.T) {
	w := newWorld(t, rankObjs)
	if err := w.st.Retain(ctx, hm(9, 30)); err != nil {
		t.Fatal(err)
	}
	s := w.source(t, rankObjs, true)
	both := func(a, b error) error { return fmt.Errorf("%w: %w", a, b) }

	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{"cancelled", context.Canceled, "The call was cancelled."},
		{"deadline", context.DeadlineExceeded, "The call took longer than 30 s and was stopped."},
		{"cancelled before closed", both(context.Canceled, store.ErrClosed), "The call was cancelled."},
		{"cancelled before the horizon", both(context.Canceled, store.ErrBeforeHorizon), "The call was cancelled."},
		{"closed", store.ErrClosed, "The server is shutting down."},
		{"withdrawn", ErrShuttingDown, "The server is shutting down."},
		{"closed before the horizon", both(store.ErrClosed, store.ErrBeforeHorizon), "The server is shutting down."},
		{"the horizon", store.ErrBeforeHorizon, "The store no longer keeps history of layer "},
		{"the horizon before invalid", both(store.ErrInvalid, store.ErrBeforeHorizon), "The store no longer keeps history of layer "},
		{"the horizon before retained", both(interval.ErrRetained, store.ErrBeforeHorizon), "The store no longer keeps history of layer "},
		{"retained", interval.ErrRetained, bareRetained},
		{"retained before invalid", both(store.ErrInvalid, interval.ErrRetained), bareRetained},
		{"retained with no TTL", fmt.Errorf("graphdiff: Diff: %w", &interval.RetainedError{
			Subject: ent(w.p1), At: hm(10, 0), Producer: objs, Seed: store.Record{EventTime: hm(9, 0)},
		}), "The description of " + w.p1.String() + " at 2026-01-01T10:00:00Z by k8sobjects/c1 was recorded with an event time before the retention horizon and is no longer kept. It cannot be described until its producer asserts it again."},
		{"retained with a TTL", fmt.Errorf("graphdiff: Diff: %w", &interval.RetainedError{
			Subject: ent(w.p1), At: hm(10, 0), Producer: objs, Seed: store.Record{EventTime: hm(9, 50), TTL: 15 * time.Minute},
		}), "The description of " + w.p1.String() + " at 2026-01-01T10:00:00Z by k8sobjects/c1 was recorded with an event time before the retention horizon and is no longer kept. A window at this token that starts at or after 2026-01-01T10:05:00Z does not need it."},
		{"retained edge", &interval.RetainedError{
			Subject: edge(w.p1, w.n1, catalog.ScheduledOn), At: hm(10, 0), Producer: objs, Seed: store.Record{EventTime: hm(9, 0), Through: hm(9, 50), TTL: 15 * time.Minute},
		}, "The description of the scheduled_on edge from " + w.p1.String() + " to " + w.n1.String() + " at 2026-01-01T10:00:00Z by k8sobjects/c1 was recorded with an event time before the retention horizon and is no longer kept. A window at this token that starts at or after 2026-01-01T10:05:00Z does not need it."},
		{"invalid", fmt.Errorf("x: %w", store.ErrInvalid), "The request was refused: x."},
		{"no range reads", ErrNoRangeReads, "Range answers are not available on this store."},
		{"busy", ErrBusy, "The server is busy; try again shortly."},
		{"internal", errors.New("disk on fire"), "Internal error: disk on fire"},
	} {
		got, _ := s.sentence(tc.err)
		if !strings.HasPrefix(got, tc.want) {
			t.Errorf("%s: %q, want %q", tc.name, got, tc.want)
		}
	}
	if _, internal := s.sentence(errors.New("x")); !internal {
		t.Error("an unknown error is not internal")
	}
}

// bareRetained is the sentence for a retention refusal that carries no seed.
const bareRetained = "A description this answer needs was recorded with an event time before the retention horizon and is no longer kept."

// The order, end to end, on a retained store, on memstore and on pebblestore
// retaining both ways: a point read before the horizon is refused for the horizon
// and not as an invalid argument, naming the layer; so is a range read whose
// window starts before it. A range read from after the horizon is answered, except
// that a description recorded before the horizon, whose payload the store no
// longer keeps, is refused for retention, naming the entity, the instant and the
// producer, and, for a seed with a TTL, the instant from which a window does not
// need it.
func TestRefusalsOnARetainedStore(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func(testing.TB) store.Store
	}{
		{"memstore", func(tb testing.TB) store.Store {
			return openMem(tb, memstore.Options{Policy: servePolicy(rankObjs)})
		}},
		{"pebblestore, synchronous", func(tb testing.TB) store.Store {
			return openPeb(tb, servePolicy(rankObjs), pebblestore.CheckpointOptions{On: true, KMin: 1}, pebblestore.Synchronous)
		}},
		{"pebblestore, background", func(tb testing.TB) store.Store {
			return openPeb(tb, servePolicy(rankObjs), pebblestore.CheckpointOptions{On: true, KMin: 8, Alpha: 1, Lag: time.Minute}, pebblestore.Background)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newWorldOn(t, tc.open(t))
			// Two pods whose last assertion is before the horizon, with a TTL: px
			// lapses at 09:40, and py, asserted at 09:00 and refreshed through 09:45,
			// at 10:00.
			px, py := fpOf(t, catalog.K8sPod, catalog.K8sPodUID, "pod-uid-x"), fpOf(t, catalog.K8sPod, catalog.K8sPodUID, "pod-uid-y")
			ry := observe(ent(py), objs, hm(9, 0), 15*time.Minute, `{"name":"web-y","namespace":"shop"}`)
			ry.Through = hm(9, 45)
			newFeed(t, w.st).write(observe(ent(px), objs, hm(9, 25), 15*time.Minute, `{"name":"web-x","namespace":"shop"}`), ry)
			if err := w.st.Retain(ctx, hm(9, 30)); err != nil {
				t.Fatal(err)
			}
			last := w.st.LastSeq()
			s := w.source(t, rankObjs, RangeReadsFor(w.st))
			check := func(t *testing.T) {
				hz := fmt.Sprintf("or below token %d.", last)
				note := " A window that starts at or just after a horizon can still be refused for a description recorded before it."

				_, err := s.NeighborsAt(ctx, NeighborsAtInput{Entity: w.n1.String(), At: "2026-01-01T09:00:00Z"})
				wantErr(t, "neighbors_at before the horizon", err, "The store no longer keeps history of layer L1 before 2026-01-01T09:30:00Z "+hz+note, interval.ErrRetained, store.ErrBeforeHorizon)
				if strings.Contains(err.Error(), "The request was refused") {
					t.Errorf("the horizon was reported as an invalid argument: %v", err)
				}
				_, err = s.NeighborsAt(ctx, NeighborsAtInput{Entity: w.n1.String(), At: "2026-01-01T10:00:00Z", AsOf: "1"})
				wantErr(t, "neighbors_at below the horizon's token", err, "The store no longer keeps history of layer L1 before 2026-01-01T09:30:00Z "+hz+note, store.ErrBeforeHorizon)
				_, err = s.ResolveEntity(ctx, ResolveEntityInput{Type: "host", Attributes: rawAttrs(`{"host.id":"x"}`), At: "2026-01-01T09:00:00Z"})
				wantErr(t, "resolve_entity before the horizon", err, "The store no longer keeps history of layer L1 before", store.ErrBeforeHorizon)
				if !strings.HasSuffix(err.Error(), note) {
					t.Errorf("resolve_entity: the horizon sentence does not end with the note: %v", err)
				}
				_, err = s.EntityHistory(ctx, EntityHistoryInput{Entity: w.p1.String(), From: "2026-01-01T09:00:00Z", To: w1030})
				wantErr(t, "entity_history from before the horizon", err, "The store no longer keeps history of layer L0 before 2026-01-01T09:30:00Z "+hz, store.ErrBeforeHorizon)
				if !strings.HasSuffix(err.Error(), note) {
					t.Errorf("entity_history from before the horizon: the horizon sentence does not end with the note: %v", err)
				}
				_, err = s.WhatChanged(ctx, WhatChangedInput{Scope: []string{w.p1.String()}, From: "2026-01-01T09:00:00Z", To: w1030})
				wantErr(t, "what_changed from before the horizon", err, "The store no longer keeps history of layer L0 before 2026-01-01T09:30:00Z "+hz, store.ErrBeforeHorizon)
				if !strings.HasSuffix(err.Error(), note) {
					t.Errorf("what_changed from before the horizon: the horizon sentence does not end with the note: %v", err)
				}

				if _, err := s.BlastRadius(ctx, BlastRadiusInput{Entity: w.h1.String(), From: w10, To: w1030}); err != nil {
					t.Errorf("blast_radius after the horizon: %v", err)
				}
				if _, err := s.CoResidents(ctx, CoResidentsInput{Pod: w.p1.String(), From: w10, To: w1030}); err != nil {
					t.Errorf("co_residents after the horizon: %v", err)
				}
				// The diff describes p1 at From, by an assertion made once, before the
				// horizon: only its producer asserting it again lets it be described.
				_, err = s.WhatChanged(ctx, WhatChangedInput{Scope: []string{w.p1.String()}, From: w10, To: w1030})
				wantErr(t, "what_changed of a description before the horizon", err,
					"The description of "+w.p1.String()+" at 2026-01-01T10:00:00Z by k8sobjects/c1 was recorded with an event time before the retention horizon and is no longer kept. It cannot be described until its producer asserts it again.",
					interval.ErrRetained)
				var re *interval.RetainedError
				if !errors.As(err, &re) {
					t.Errorf("the cause of the retention refusal carries no seed: %v", err)
				}
				// px has a TTL: a window from the instant it lapses does not need it.
				_, err = s.WhatChanged(ctx, WhatChangedInput{Scope: []string{px.String()}, From: "2026-01-01T09:35:00Z", To: "2026-01-01T09:50:00Z"})
				wantErr(t, "what_changed of a pod with a TTL", err,
					"The description of "+px.String()+" at 2026-01-01T09:35:00Z by k8sobjects/c1 was recorded with an event time before the retention horizon and is no longer kept. A window at this token that starts at or after 2026-01-01T09:40:00Z does not need it.",
					interval.ErrRetained)
				if _, err := s.WhatChanged(ctx, WhatChangedInput{Scope: []string{px.String()}, From: "2026-01-01T09:40:00Z", To: "2026-01-01T09:50:00Z"}); err != nil {
					t.Errorf("what_changed of a pod with a TTL from its deadline: %v", err)
				}
				// py was asserted at 09:00 and refreshed through 09:45: it lapses at 10:00.
				_, err = s.WhatChanged(ctx, WhatChangedInput{Scope: []string{py.String()}, From: "2026-01-01T09:35:00Z", To: "2026-01-01T09:50:00Z"})
				wantErr(t, "what_changed of a backdated pod", err,
					"The description of "+py.String()+" at 2026-01-01T09:35:00Z by k8sobjects/c1 was recorded with an event time before the retention horizon and is no longer kept. A window at this token that starts at or after 2026-01-01T10:00:00Z does not need it.",
					interval.ErrRetained)
				if _, err := s.WhatChanged(ctx, WhatChangedInput{Scope: []string{py.String()}, From: "2026-01-01T10:00:00Z", To: "2026-01-01T10:15:00Z"}); err != nil {
					t.Errorf("what_changed of a backdated pod from its deadline: %v", err)
				}
			}
			check(t)
			if _, ok := w.st.(*pebblestore.Store); ok {
				settle(t, w.st)
				t.Run("after the retention pass", check)
			}
		})
	}
}

func wantErr(t *testing.T, what string, err error, prefix string, is ...error) {
	t.Helper()
	var te *Error
	if !errors.As(err, &te) {
		t.Errorf("%s: %v, not an *Error", what, err)
		return
	}
	if !strings.HasPrefix(te.Message, prefix) {
		t.Errorf("%s: %q, want %q", what, te.Message, prefix)
	}
	// The cause is the error the sentence was made from.
	if len(is) > 0 {
		matched := false
		for _, target := range is {
			if errors.Is(err, target) {
				matched = true
			}
		}
		if !matched {
			t.Errorf("%s: the error wraps none of %v", what, is)
		}
	}
}

// A closed store, a cancelled context, a deadline, a store that fails, and a
// store without range reads.
func TestRefusalsOfTheSource(t *testing.T) {
	w := newWorld(t, rankObjs)
	blast := BlastRadiusInput{Entity: w.h1.String(), From: w10, To: w1030}

	t.Run("closed", func(t *testing.T) {
		w := newWorld(t, rankObjs)
		s := w.source(t, rankObjs, true)
		if err := w.st.Close(); err != nil {
			t.Fatal(err)
		}
		_, err := s.BlastRadius(ctx, BlastRadiusInput{Entity: w.h1.String(), From: w10, To: w1030})
		wantErr(t, "closed", err, "The server is shutting down.", store.ErrClosed)
		_, err = s.NeighborsAt(ctx, NeighborsAtInput{Entity: w.n1.String(), At: w10})
		wantErr(t, "closed", err, "The server is shutting down.", store.ErrClosed)
	})

	t.Run("cancelled before the call", func(t *testing.T) {
		s := w.source(t, rankObjs, true)
		c, cancel := context.WithCancel(ctx)
		cancel()
		_, err := s.BlastRadius(c, blast)
		wantErr(t, "cancelled", err, "The call was cancelled.", context.Canceled)
	})

	t.Run("cancelled during the call", func(t *testing.T) {
		bs := newBlockStore(w.st)
		s, err := NewSource(Config{Store: bs, Rank: rankObjs, RangeReads: true})
		if err != nil {
			t.Fatal(err)
		}
		c, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { _, err := s.BlastRadius(c, blast); done <- err }()
		<-bs.entered
		cancel()
		wantErr(t, "cancelled", <-done, "The call was cancelled.", context.Canceled)
	})

	t.Run("deadline", func(t *testing.T) {
		bs := newBlockStore(w.st)
		s, err := NewSource(Config{Store: bs, Rank: rankObjs, RangeReads: true, CallTimeout: 40 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		_, err = s.BlastRadius(ctx, blast)
		wantErr(t, "deadline", err, "The call took longer than 40ms and was stopped.", context.DeadlineExceeded)
	})

	t.Run("internal", func(t *testing.T) {
		var mu sync.Mutex
		var logged []string
		bad := &failStore{Store: w.st, err: errors.New("disk on fire")}
		s, err := NewSource(Config{Store: bad, Rank: rankObjs, RangeReads: true, Logf: func(f string, a ...any) {
			mu.Lock()
			logged = append(logged, fmt.Sprintf(f, a...))
			mu.Unlock()
		}})
		if err != nil {
			t.Fatal(err)
		}
		_, err = s.BlastRadius(ctx, blast)
		wantErr(t, "internal", err, "Internal error: ")
		if !strings.Contains(err.Error(), "disk on fire") {
			t.Errorf("the cause is not in the sentence: %v", err)
		}
		mu.Lock()
		defer mu.Unlock()
		if len(logged) != 1 || !strings.Contains(logged[0], "disk on fire") {
			t.Errorf("log lines %q", logged)
		}
	})

	t.Run("no range reads", func(t *testing.T) {
		spy := &spyStore{Store: w.st}
		s, err := NewSource(Config{Store: spy, Rank: rankObjs})
		if err != nil {
			t.Fatal(err)
		}
		want := "Range answers are not available on this store."
		_, err = s.BlastRadius(ctx, blast)
		wantErr(t, "blast_radius", err, want, ErrNoRangeReads)
		_, err = s.WhatChanged(ctx, WhatChangedInput{Scope: []string{w.p1.String()}, From: w10, To: w1030})
		wantErr(t, "what_changed", err, want, ErrNoRangeReads)
		_, err = s.CoResidents(ctx, CoResidentsInput{Pod: w.p1.String(), From: w10, To: w1030})
		wantErr(t, "co_residents", err, want, ErrNoRangeReads)
		if spy.reads != 0 {
			t.Errorf("%d reads before the refusal", spy.reads)
		}
		// The point tools, and the records of entity_history, still run.
		if _, err := s.NeighborsAt(ctx, NeighborsAtInput{Entity: w.n1.String(), At: "2026-01-01T10:08:00Z"}); err != nil {
			t.Errorf("neighbors_at: %v", err)
		}
		out, err := s.EntityHistory(ctx, EntityHistoryInput{Entity: w.p1.String(), From: "2026-01-01T09:00:00Z", To: w1030})
		if err != nil {
			t.Fatalf("entity_history: %v", err)
		}
		if out.Existence != nil || len(out.Records) != 3 || out.Derived {
			t.Errorf("entity_history without range reads: existence %v, %d records, derived %t", out.Existence, len(out.Records), out.Derived)
		}
		if !contains(out.Notes, "Existence intervals need a range read, which this store does not offer; the stored records are shown.") {
			t.Errorf("notes %q", out.Notes)
		}
	})
}

// failStore fails every Window read, and every WindowSince of edges, which
// stands for Window reads.
type failStore struct {
	store.Store
	err error
}

func (f *failStore) Window(context.Context, identity.Fingerprint, store.Direction, time.Time, time.Time, store.Scope) ([]store.Record, error) {
	return nil, f.err
}

func (f *failStore) WindowSince(ctx context.Context, fps []identity.Fingerprint, d store.Direction, from, to time.Time, sc store.Scope, o store.SinceOptions) ([]store.Since, error) {
	if d != 0 {
		return nil, f.err
	}
	return f.Store.WindowSince(ctx, fps, d, from, to, sc, o)
}

// Arguments are refused with a sentence that names the argument, before any read.
func TestArgumentsAreNamed(t *testing.T) {
	w := newWorld(t, rankObjs)
	spy := &spyStore{Store: w.st}
	s, err := NewSource(Config{Store: spy, Rank: rankObjs, RangeReads: true})
	if err != nil {
		t.Fatal(err)
	}
	good := w.h1.String()
	many := make([]string, maxScope+1)
	for i := range many {
		many[i] = good
	}
	for _, tc := range []struct {
		name string
		call func() error
		want string
	}{
		{"entity missing", func() error { _, e := s.BlastRadius(ctx, BlastRadiusInput{From: w10, To: w1030}); return e }, `"entity" is required`},
		{"entity malformed", func() error {
			_, e := s.BlastRadius(ctx, BlastRadiusInput{Entity: "host", From: w10, To: w1030})
			return e
		}, `"entity" must be an entity id`},
		{"entity upper case", func() error {
			_, e := s.BlastRadius(ctx, BlastRadiusInput{Entity: strings.ToUpper(good), From: w10, To: w1030})
			return e
		}, `"entity" must be an entity id`},
		{"entity of an unknown type", func() error {
			_, e := s.BlastRadius(ctx, BlastRadiusInput{Entity: "nothing:" + strings.Repeat("a", 32), From: w10, To: w1030})
			return e
		}, `not in the catalog`},
		{"time without a zone", func() error {
			_, e := s.BlastRadius(ctx, BlastRadiusInput{Entity: good, From: "2026-01-01T10:00:00", To: w1030})
			return e
		}, `"from" must be an RFC 3339 time with a zone`},
		{"relative time", func() error {
			_, e := s.BlastRadius(ctx, BlastRadiusInput{Entity: good, At: "now"})
			return e
		}, `"at" must be an RFC 3339 time with a zone`},
		{"empty window", func() error {
			_, e := s.BlastRadius(ctx, BlastRadiusInput{Entity: good, From: w1030, To: w10})
			return e
		}, `"from" (2026-01-01T10:30:00Z) must be before "to"`},
		{"at and from", func() error {
			_, e := s.BlastRadius(ctx, BlastRadiusInput{Entity: good, At: w10, From: w10, To: w1030})
			return e
		}, `either "at", or "from" and "to"`},
		{"no window", func() error { _, e := s.BlastRadius(ctx, BlastRadiusInput{Entity: good}); return e }, `Give "at", or "from" and "to"`},
		{"depth too big", func() error {
			_, e := s.BlastRadius(ctx, BlastRadiusInput{Entity: good, At: w10, Depth: 7})
			return e
		}, `"depth" must be between 1 and 6`},
		{"depth negative", func() error {
			_, e := s.BlastRadius(ctx, BlastRadiusInput{Entity: good, At: w10, Depth: -1})
			return e
		}, `"depth" must be between 1 and 6`},
		{"limit too big", func() error {
			_, e := s.BlastRadius(ctx, BlastRadiusInput{Entity: good, At: w10, Limit: 501})
			return e
		}, `"limit" must be between 1 and 500`},
		{"scope empty", func() error {
			_, e := s.WhatChanged(ctx, WhatChangedInput{From: w10, To: w1030})
			return e
		}, `"scope" must name at least one entity`},
		{"scope too big", func() error {
			_, e := s.WhatChanged(ctx, WhatChangedInput{Scope: many, From: w10, To: w1030})
			return e
		}, `"scope" names 257 entities; at most 256`},
		{"scope entry malformed", func() error {
			_, e := s.WhatChanged(ctx, WhatChangedInput{Scope: []string{good, "x"}, From: w10, To: w1030})
			return e
		}, `"scope[1]" must be an entity id`},
		{"pod missing", func() error { _, e := s.CoResidents(ctx, CoResidentsInput{From: w10, To: w1030}); return e }, `"pod" is required`},
		{"direction", func() error {
			_, e := s.NeighborsAt(ctx, NeighborsAtInput{Entity: good, At: w10, Direction: "sideways"})
			return e
		}, `"direction" must be "forward", "reverse" or "both"`},
		{"at missing", func() error { _, e := s.NeighborsAt(ctx, NeighborsAtInput{Entity: good}); return e }, `"at" is required`},
		{"history window", func() error {
			_, e := s.EntityHistory(ctx, EntityHistoryInput{Entity: good, From: w10})
			return e
		}, `"to" is required`},
	} {
		err := tc.call()
		var te *Error
		if !errors.As(err, &te) || !strings.Contains(te.Message, tc.want) || !errors.Is(err, store.ErrInvalid) {
			t.Errorf("%s: %v, want %q wrapping ErrInvalid", tc.name, err, tc.want)
		}
	}
	if spy.reads != 0 {
		t.Errorf("%d reads for refused arguments", spy.reads)
	}
}
