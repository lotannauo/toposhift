package mcp

import (
	"context"
	"sync"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/store"
)

// spyStore wraps a store and records every read's snapshot token and every call
// that could change the store. afterFirst, if set, runs once, right after the
// first read returns.
type spyStore struct {
	store.Store

	mu         sync.Mutex
	asOf       []uint64
	reads      int
	writes     int
	retains    int
	closes     int
	lists      int
	lastSeqs   int
	afterFirst func()
	fired      bool
}

func (s *spyStore) note(sc store.Scope) {
	s.mu.Lock()
	s.asOf = append(s.asOf, sc.AsOf)
	s.reads++
	s.mu.Unlock()
}

func (s *spyStore) after() {
	s.mu.Lock()
	run := s.afterFirst
	if s.fired || run == nil {
		s.mu.Unlock()
		return
	}
	s.fired = true
	s.mu.Unlock()
	run()
}

func (s *spyStore) Write(ctx context.Context, b []store.Record) error {
	s.mu.Lock()
	s.writes++
	s.mu.Unlock()
	return s.Store.Write(ctx, b)
}

func (s *spyStore) Retain(ctx context.Context, h time.Time) error {
	s.mu.Lock()
	s.retains++
	s.mu.Unlock()
	return s.Store.Retain(ctx, h)
}

func (s *spyStore) Close() error {
	s.mu.Lock()
	s.closes++
	s.mu.Unlock()
	return s.Store.Close()
}

// LastSeq counts the calls: every interval read begins with one, and so a count of
// them is a count of the interval reads made through the spy, when nothing else asks.
func (s *spyStore) LastSeq() uint64 {
	s.mu.Lock()
	s.lastSeqs++
	s.mu.Unlock()
	return s.Store.LastSeq()
}

func (s *spyStore) Neighbors(ctx context.Context, fp identity.Fingerprint, d store.Direction, t time.Time, sc store.Scope) ([]store.Neighbor, error) {
	s.note(sc)
	defer s.after()
	return s.Store.Neighbors(ctx, fp, d, t, sc)
}

func (s *spyStore) NeighborsBatch(ctx context.Context, fps []identity.Fingerprint, d store.Direction, t time.Time, sc store.Scope) ([][]store.Neighbor, error) {
	s.note(sc)
	defer s.after()
	return s.Store.NeighborsBatch(ctx, fps, d, t, sc)
}

func (s *spyStore) Alive(ctx context.Context, fp identity.Fingerprint, t time.Time, sc store.Scope) (bool, error) {
	s.note(sc)
	defer s.after()
	return s.Store.Alive(ctx, fp, t, sc)
}

func (s *spyStore) Window(ctx context.Context, fp identity.Fingerprint, d store.Direction, from, to time.Time, sc store.Scope) ([]store.Record, error) {
	s.note(sc)
	defer s.after()
	return s.Store.Window(ctx, fp, d, from, to, sc)
}

func (s *spyStore) EntityWindow(ctx context.Context, fp identity.Fingerprint, from, to time.Time, sc store.Scope) ([]store.Record, error) {
	s.note(sc)
	defer s.after()
	return s.Store.EntityWindow(ctx, fp, from, to, sc)
}

// WindowSince records the token once per fingerprint, as the Window or
// EntityWindow reads it stands for.
func (s *spyStore) WindowSince(ctx context.Context, fps []identity.Fingerprint, d store.Direction, from, to time.Time, sc store.Scope, o store.SinceOptions) ([]store.Since, error) {
	for range fps {
		s.note(sc)
	}
	defer s.after()
	return s.Store.WindowSince(ctx, fps, d, from, to, sc, o)
}

func (s *spyStore) LayerHorizon(l catalog.Layer) store.Horizon { return s.Store.LayerHorizon(l) }

// ListEntities passes the listing read of the store underneath through, and counts
// it. It is not a read at a token, so it records none and fires nothing.
func (s *spyStore) ListEntities(ctx context.Context, typ catalog.EntityType, after identity.Fingerprint, n int) ([]identity.Fingerprint, error) {
	s.mu.Lock()
	s.lists++
	s.mu.Unlock()
	return s.Store.(Lister).ListEntities(ctx, typ, after, n)
}
