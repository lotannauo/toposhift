package memstore

import (
	"context"
	"fmt"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

// closedError is the error every method that returns one gives after Close.
func closedError(op string) error {
	return fmt.Errorf("memstore: %s: %w", op, store.ErrClosed)
}

// contextError is the error a cancelled context gives, wrapping the context's.
func contextError(op string, err error) error {
	return fmt.Errorf("memstore: %s: %w", op, err)
}

func formatTime(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

// Write implements [store.Store]. It checks every record of the batch, in
// order, before it stores any, so a refused batch stores nothing and leaves
// LastSeq unchanged. A Seq equal to [store.Latest] is refused, like 0, because
// Latest names no record. It also refuses a record whose layer differs from the
// layer its subject was first stored in: that is the caller's precondition for
// every store, and the reference is where a bad workload is caught.
func (s *Store) Write(ctx context.Context, batch []store.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return closedError("Write")
	}
	if err := ctx.Err(); err != nil {
		return contextError("Write", err)
	}
	if len(batch) == 0 {
		return nil
	}

	prev := s.lastSeq
	inBatch := make(map[store.Subject]catalog.Layer)
	for i, r := range batch {
		fail := func(format string, args ...any) error {
			return fmt.Errorf("memstore: Write: batch[%d] seq %d: %s: %w", i, r.Seq, fmt.Sprintf(format, args...), store.ErrInvalid)
		}
		if err := r.Validate(); err != nil {
			return fmt.Errorf("memstore: Write: batch[%d]: %w", i, err)
		}
		if r.Seq <= prev {
			return fail("seq is not above the previous seq %d", prev)
		}
		if r.Seq == store.Latest {
			return fail("seq %d is the snapshot token Latest, which no record may carry", r.Seq)
		}
		prev = r.Seq
		// Validate has checked the record's layer.
		li, _ := layerIndex(r.Layer)
		if hz := s.hz[li]; !hz.IsZero() && r.EventTime.Before(hz.Time) {
			return fmt.Errorf("memstore: Write: batch[%d] seq %d: event time %s is before the horizon %s of layer %s: %w",
				i, r.Seq, formatTime(r.EventTime), formatTime(hz.Time), r.Layer, store.ErrBeforeHorizon)
		}
		known, ok := s.layers[r.Subject]
		if !ok {
			known, ok = inBatch[r.Subject]
		}
		if ok && known != r.Layer {
			return fail("subject is stored in layer %s, not %s", known, r.Layer)
		}
		inBatch[r.Subject] = r.Layer
		if r.Subject.Kind == store.SubjectEntity && s.policy.BootKey != "" {
			if _, err := lifecycle.Fold([]lifecycle.Assertion{r.Assertion()}, s.policy); err != nil {
				return fmt.Errorf("memstore: Write: batch[%d] seq %d: the store's policy cannot fold the record: %w: %w",
					i, r.Seq, store.ErrInvalid, err)
			}
		}
	}

	for _, r := range batch {
		if _, known := s.bySubject[r.Subject]; !known {
			s.layers[r.Subject] = r.Layer
			if r.Subject.Kind == store.SubjectEdge {
				s.incident[store.Forward][r.Subject.A] = append(s.incident[store.Forward][r.Subject.A], edge{r.Subject, r.Subject.B})
				s.incident[store.Reverse][r.Subject.B] = append(s.incident[store.Reverse][r.Subject.B], edge{r.Subject, r.Subject.A})
			}
		}
		s.bySubject[r.Subject] = append(s.bySubject[r.Subject], keep(r))
	}
	s.lastSeq = prev
	return nil
}

// Retain implements [store.Store]. The store keeps everything, which answers
// every question at or after a horizon trivially; Retain only publishes the
// horizons, so that reads and writes before them are refused. The horizon of a
// retained layer is the instant minus the layer's offset, moved only if that is
// after the layer's horizon; the horizon of a kept layer never moves. Horizons
// are stored in UTC and without a monotonic clock reading, so comparisons are by
// instant only. Horizon then reports the latest of the horizons this Retain
// moved (they share its Seq), and is
// unchanged if none moved.
func (s *Store) Retain(ctx context.Context, horizon time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return closedError("Retain")
	}
	if err := ctx.Err(); err != nil {
		return contextError("Retain", err)
	}
	var latest store.Horizon
	for i := range s.hz {
		if s.keep[i] {
			continue
		}
		if h := horizon.Add(-s.offsets[i]); h.After(s.hz[i].Time) {
			s.hz[i] = store.Horizon{Time: h.UTC(), Seq: s.lastSeq}
			if s.hz[i].Time.After(latest.Time) {
				latest = s.hz[i]
			}
		}
	}
	if !latest.IsZero() {
		s.horizon = latest
	}
	return nil
}
