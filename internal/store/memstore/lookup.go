package memstore

import (
	"context"
	"fmt"
	"time"

	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

// Lookup finds a producer's last assertion of a subject, with the same answers as
// the pebblestore's (see [store.LookupRef] for the types). It is not part of
// [store.Store].
//
// A Lookup holds no iterator, and reads the store as it is at each call, not as it
// was when the Lookup was opened. A Lookup is used by one goroutine at a time and
// closed before the group's write, as the pebblestore's is.
type Lookup struct {
	s      *Store
	closed bool
}

// NewLookup opens a Lookup. It returns an error wrapping [store.ErrClosed] if the
// store is closed.
func (s *Store) NewLookup() (*Lookup, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, closedError("NewLookup")
	}
	return &Lookup{s: s}, nil
}

// Close ends the Lookup. A second call does nothing.
func (l *Lookup) Close() error {
	l.closed = true
	return nil
}

// Last finds the newest record of the reference: the records of the reference at
// the latest event time, and of those the one with the latest [store.LastOf], then
// the highest Seq.
//
// The store keeps no baseline, and answers as the pebblestore does where one would
// stand. If the reference's newest record is at or after its layer's horizon, the
// answer is [store.LookupFound]. If every record of the reference is before the
// horizon, a layout that had retained there would hold at most one entry for it,
// from its newest record (the one with the highest Seq at the latest event time),
// and only if that is an Observe still alive at the horizon: then the answer is
// [store.LookupBaselineOnly], with that record's times, kind and basis and no
// payload, and otherwise [store.LookupNone]. It never walks a bound, so it never
// answers [store.LookupExhausted].
func (l *Lookup) Last(ctx context.Context, ref store.LookupRef) (store.LastAssertion, error) {
	s := l.s
	s.mu.Lock()
	defer s.mu.Unlock()
	if l.closed || s.closed {
		return store.LastAssertion{}, closedError("Lookup.Last")
	}
	if err := ctx.Err(); err != nil {
		return store.LastAssertion{}, contextError("Lookup.Last", err)
	}
	if err := ref.Validate(); err != nil {
		return store.LastAssertion{}, fmt.Errorf("memstore: Lookup.Last: %w", err)
	}
	if layer, known := s.layers[ref.Subject]; !known || layer != ref.Layer {
		return store.LastAssertion{Outcome: store.LookupNone}, nil
	}
	hz := s.hz[mustLayerIndex(ref)]
	var above, below *stored // the newest at or after the horizon; the newest before it
	for i := range s.bySubject[ref.Subject] {
		st := &s.bySubject[ref.Subject][i]
		if st.producer != ref.Producer {
			continue
		}
		if !hz.IsZero() && time.Unix(0, st.event).Before(hz.Time) {
			// Records are in ascending Seq, so a later one wins a tie.
			if below == nil || st.event >= below.event {
				below = st
			}
			continue
		}
		if above == nil || st.event > above.event || (st.event == above.event && lastOf(*st) >= lastOf(*above)) {
			above = st
		}
	}
	switch {
	case above != nil:
		return assertionOf(store.LookupFound, *above, true), nil
	case below != nil && below.kind == lifecycle.Observe && aliveAt(*below, hz.Time):
		return assertionOf(store.LookupBaselineOnly, *below, false), nil
	}
	return store.LastAssertion{Outcome: store.LookupNone}, nil
}

// mustLayerIndex is the index of the layer of a reference that passed Validate.
func mustLayerIndex(ref store.LookupRef) int {
	i, _ := layerIndex(ref.Layer)
	return i
}

// lastOf is the latest instant the record says its producer asserted its subject,
// in Unix nanoseconds.
func lastOf(st stored) int64 {
	if st.hasThrough && st.through > st.event {
		return st.through
	}
	return st.event
}

// aliveAt reports whether an Observe still holds at t, with no later assertion by
// its producer: with no TTL it holds until replaced, and with one until its last
// instant plus the TTL.
func aliveAt(st stored, t time.Time) bool {
	if st.ttl == 0 {
		return true
	}
	return t.Before(time.Unix(0, lastOf(st)).Add(st.ttl))
}

// assertionOf is the answer for a stored record; withPayload says whether it
// carries the record's payload (a copy).
func assertionOf(outcome store.LookupOutcome, st stored, withPayload bool) store.LastAssertion {
	a := store.LastAssertion{
		Outcome: outcome, Kind: st.kind, EventTime: time.Unix(0, st.event).UTC(), TTL: st.ttl,
		Basis: st.basis, Seq: st.seq,
	}
	if st.hasThrough {
		a.Through = time.Unix(0, st.through).UTC()
	}
	a.Last, a.LastFromThrough = store.LastOf(a.EventTime, a.Through)
	if withPayload && len(st.payload) > 0 {
		a.Payload = append([]byte(nil), st.payload...)
	}
	return a
}
