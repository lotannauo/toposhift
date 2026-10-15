package memstore

import (
	"context"
	"slices"
	"time"

	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

// WindowSince implements [store.Store]. It checks what every read checks, in the
// same order (closed, the arguments with the direction 0 allowed, the context, the
// horizon of the scope's layer against from and the token), then the empty cases,
// and answers the whole call under one hold of the lock.
//
// The window of each fingerprint is what Window or EntityWindow returns. Its seeds
// are found by the definition: for each reference (a subject and a producer) of
// the fingerprint in the scope's layer, the visible record with the highest
// (EventTime, Seq) strictly before from, kept if it is an Observe that holds at the
// instant just before from (from itself when from is the layer's horizon). The
// store discards nothing at Retain, so it still has every payload; the payload rule
// of [store.Seed] is what makes it give the answer of a store that has discarded
// the history before the horizon.
func (s *Store) WindowSince(ctx context.Context, fps []identity.Fingerprint, dir store.Direction, from, to time.Time, sc store.Scope, o store.SinceOptions) ([]store.Since, error) {
	const op = "WindowSince"
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, closedError(op)
	}
	if err := checkArgs(op, sc, dir, dir != 0, fps...); err != nil {
		return nil, err
	}
	if err := s.checkWhen(ctx, op, from, sc); err != nil {
		return nil, err
	}
	out := make([]store.Since, len(fps))
	if !from.Before(to) || len(fps) == 0 {
		return out, nil
	}
	i, _ := layerIndex(sc.Layer)
	sw := seedRule{from: from, horizon: s.hz[i], payloads: o.Payloads}
	for k, fp := range fps {
		var subs []store.Subject
		if dir == 0 {
			subs = []store.Subject{store.EntitySubject(fp)}
		} else {
			for _, e := range s.incident[dir][fp] {
				subs = append(subs, e.subject)
			}
		}
		for _, sub := range subs {
			out[k].Window = append(out[k].Window, s.window(sub, from, to, sc)...)
			out[k].Seeds = append(out[k].Seeds, s.seeds(sub, sw, sc)...)
		}
		store.SortRecords(out[k].Window)
		store.SortSeeds(out[k].Seeds)
	}
	return out, nil
}

// seedRule is what decides the seeds of one read: the window's start, the horizon
// of the scope's layer, and whether payloads were asked for.
type seedRule struct {
	from     time.Time
	horizon  store.Horizon
	payloads bool
}

// liveAt is the instant a seed must hold at: the one just before from, which is
// from itself when from is the horizon (the state there stands for the one before).
func (r seedRule) liveAt() time.Time {
	if !r.horizon.IsZero() && r.from.Equal(r.horizon.Time) {
		return r.from
	}
	return r.from.Add(-time.Nanosecond)
}

// payloadKnown says whether a seed recorded at event time at carries its payload.
func (r seedRule) payloadKnown(at time.Time) bool {
	return r.payloads && (r.horizon.IsZero() || !at.Before(r.horizon.Time))
}

// seeds are the seeds of one subject, in no particular order.
func (s *Store) seeds(sub store.Subject, r seedRule, sc store.Scope) []store.Seed {
	if s.layers[sub] != sc.Layer {
		return nil
	}
	recs := s.bySubject[sub]
	recs = recs[:visible(recs, sc.AsOf)]
	deciding := map[lifecycle.Producer]int{} // producer -> index of its deciding record
	var order []lifecycle.Producer
	for i, st := range recs {
		if !time.Unix(0, st.event).Before(r.from) {
			continue
		}
		j, ok := deciding[st.producer]
		if !ok {
			order = append(order, st.producer)
		}
		// Ascending Seq: a later record at the same instant takes over.
		if !ok || st.event >= recs[j].event {
			deciding[st.producer] = i
		}
	}
	live := r.liveAt()
	var out []store.Seed
	for _, p := range order {
		st := recs[deciding[p]]
		if st.kind != lifecycle.Observe || !aliveAt(st, live) {
			continue
		}
		rec := st.record(sub, sc.Layer)
		known := r.payloadKnown(rec.EventTime)
		rec.Payload = nil
		if known && len(st.payload) > 0 {
			rec.Payload = slices.Clone(st.payload)
		}
		out = append(out, store.Seed{Record: rec, PayloadKnown: known})
	}
	return out
}
