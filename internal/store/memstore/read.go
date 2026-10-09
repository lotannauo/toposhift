package memstore

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

// checkArgs checks the arguments of a read, before anything is read: the
// scope, the direction (when the read has one), and the fingerprints, none of
// which may be zero. Every error wraps [store.ErrInvalid].
func checkArgs(op string, sc store.Scope, dir store.Direction, hasDir bool, fps ...identity.Fingerprint) error {
	if err := sc.Validate(); err != nil {
		return fmt.Errorf("memstore: %s: %w", op, err)
	}
	if hasDir && dir != store.Forward && dir != store.Reverse {
		return fmt.Errorf("memstore: %s: direction %s: %w", op, dir, store.ErrInvalid)
	}
	for i, fp := range fps {
		if fp.IsZero() {
			return fmt.Errorf("memstore: %s: fingerprint %d is the zero fingerprint: %w", op, i, store.ErrInvalid)
		}
	}
	return nil
}

// checkWhen checks the context and then the horizon of the scope's layer for a
// read of instant t in the scope, whose layer is valid. While that horizon is
// zero nothing is refused for it.
func (s *Store) checkWhen(ctx context.Context, op string, t time.Time, sc store.Scope) error {
	if err := ctx.Err(); err != nil {
		return contextError(op, err)
	}
	i, _ := layerIndex(sc.Layer)
	hz := s.hz[i]
	if hz.IsZero() {
		return nil
	}
	if t.Before(hz.Time) {
		return fmt.Errorf("memstore: %s: instant %s is before the horizon %s of layer %s: %w",
			op, formatTime(t), formatTime(hz.Time), sc.Layer, store.ErrBeforeHorizon)
	}
	if sc.AsOf < hz.Seq {
		return fmt.Errorf("memstore: %s: token %d is below the seq %d of the horizon of layer %s: %w",
			op, sc.AsOf, hz.Seq, sc.Layer, store.ErrBeforeHorizon)
	}
	return nil
}

// aliveAt folds what the scope sees of an edge and asks it.
func (s *Store) aliveAt(sub store.Subject, t time.Time, sc store.Scope) (bool, error) {
	if s.layers[sub] != sc.Layer {
		return false, nil
	}
	tl, err := s.timeline(sub, visible(s.bySubject[sub], sc.AsOf))
	if err != nil {
		return false, fmt.Errorf("memstore: folding %v: %w", sub, err)
	}
	return tl.AliveAt(t), nil
}

func (s *Store) neighbors(fp identity.Fingerprint, dir store.Direction, t time.Time, sc store.Scope) ([]store.Neighbor, error) {
	var out []store.Neighbor
	for _, e := range s.incident[dir][fp] {
		alive, err := s.aliveAt(e.subject, t, sc)
		if err != nil {
			return nil, err
		}
		if alive {
			out = append(out, store.Neighbor{Peer: e.peer, Relation: e.subject.Relation})
		}
	}
	store.SortNeighbors(out)
	return out, nil
}

// Neighbors implements [store.Store]. It folds each incident edge's visible
// records with the zero policy and never folds an endpoint's existence.
func (s *Store) Neighbors(ctx context.Context, fp identity.Fingerprint, dir store.Direction, t time.Time, sc store.Scope) ([]store.Neighbor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, closedError("Neighbors")
	}
	if err := checkArgs("Neighbors", sc, dir, true, fp); err != nil {
		return nil, err
	}
	if err := s.checkWhen(ctx, "Neighbors", t, sc); err != nil {
		return nil, err
	}
	return s.neighbors(fp, dir, t, sc)
}

// NeighborsBatch implements [store.Store]. The whole call is answered under
// one hold of the lock, so from one snapshot.
func (s *Store) NeighborsBatch(ctx context.Context, fps []identity.Fingerprint, dir store.Direction, t time.Time, sc store.Scope) ([][]store.Neighbor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, closedError("NeighborsBatch")
	}
	if err := checkArgs("NeighborsBatch", sc, dir, true, fps...); err != nil {
		return nil, err
	}
	if err := s.checkWhen(ctx, "NeighborsBatch", t, sc); err != nil {
		return nil, err
	}
	if len(fps) == 0 {
		return [][]store.Neighbor{}, nil
	}
	return store.NeighborsEach(fps, func(fp identity.Fingerprint) ([]store.Neighbor, error) {
		return s.neighbors(fp, dir, t, sc)
	})
}

// Alive implements [store.Store]. It folds every record of the entity visible
// at the scope's token with the store's policy, whatever t is, so whether the
// entity is quarantined does not depend on t.
func (s *Store) Alive(ctx context.Context, fp identity.Fingerprint, t time.Time, sc store.Scope) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false, closedError("Alive")
	}
	if err := checkArgs("Alive", sc, 0, false, fp); err != nil {
		return false, err
	}
	if err := s.checkWhen(ctx, "Alive", t, sc); err != nil {
		return false, err
	}
	sub := store.EntitySubject(fp)
	if s.layers[sub] != sc.Layer {
		return false, nil
	}
	tl, err := s.timeline(sub, visible(s.bySubject[sub], sc.AsOf))
	if err != nil {
		var cc *lifecycle.CloneCollisionError
		if errors.As(err, &cc) {
			// The cached error is shared by every later read of this prefix, so the
			// caller gets a copy it may change.
			collision := *cc
			return false, &store.QuarantineError{Entity: fp, Layer: sc.Layer, Collision: &collision}
		}
		return false, fmt.Errorf("memstore: Alive: folding %s: %w", fp, err)
	}
	return tl.AliveAt(t), nil
}

// window returns the records of the subject that the scope sees with
// from <= EventTime < to. The payloads are copies.
func (s *Store) window(sub store.Subject, from, to time.Time, sc store.Scope) []store.Record {
	if s.layers[sub] != sc.Layer {
		return nil
	}
	var out []store.Record
	recs := s.bySubject[sub]
	for _, st := range recs[:visible(recs, sc.AsOf)] {
		r := st.record(sub, sc.Layer)
		if !r.EventTime.Before(from) && r.EventTime.Before(to) {
			r.Payload = slices.Clone(r.Payload)
			out = append(out, r)
		}
	}
	return out
}

// Window implements [store.Store].
func (s *Store) Window(ctx context.Context, fp identity.Fingerprint, dir store.Direction, from, to time.Time, sc store.Scope) ([]store.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, closedError("Window")
	}
	if err := checkArgs("Window", sc, dir, true, fp); err != nil {
		return nil, err
	}
	if err := s.checkWhen(ctx, "Window", from, sc); err != nil {
		return nil, err
	}
	if !from.Before(to) {
		return nil, nil
	}
	var out []store.Record
	for _, e := range s.incident[dir][fp] {
		out = append(out, s.window(e.subject, from, to, sc)...)
	}
	store.SortRecords(out)
	return out, nil
}

// EntityWindow implements [store.Store]. It returns the records of the
// entity's own existence, never those of an edge.
func (s *Store) EntityWindow(ctx context.Context, fp identity.Fingerprint, from, to time.Time, sc store.Scope) ([]store.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, closedError("EntityWindow")
	}
	if err := checkArgs("EntityWindow", sc, 0, false, fp); err != nil {
		return nil, err
	}
	if err := s.checkWhen(ctx, "EntityWindow", from, sc); err != nil {
		return nil, err
	}
	if !from.Before(to) {
		return nil, nil
	}
	out := s.window(store.EntitySubject(fp), from, to, sc)
	store.SortRecords(out)
	return out, nil
}
