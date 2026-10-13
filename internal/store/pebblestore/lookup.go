package pebblestore

import (
	"bytes"
	"context"
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/pebblekv"
)

// LookupWalkBound is the most keys one [Lookup.Last] walks before it gives up with
// [store.LookupExhausted].
const LookupWalkBound = 1024

// Lookup finds a producer's last assertion of a subject. It is not part of
// [store.Store]: see [store.LookupRef] for the types it speaks in.
//
// One Lookup is one iterator, opened by [Store.NewLookup] and used for any number
// of calls of [Lookup.Last], which suits a caller that resolves a group of
// references together. The iterator sees the database as it was when the Lookup
// was opened, so a Lookup is for reading before a group's Write and is closed
// before it; a record written afterwards is seen by a new one.
//
// A Lookup is used by one goroutine at a time, and is closed before the store is:
// the caller closes every Lookup first. Reads take no lock, and neither does a
// Lookup.
//
// The Lookup answers as a store that has finished every retention it has published
// would, whether or not a Background pass has reached the prefix yet: it keeps the
// horizons as they were once its iterator was open, and judges a record below the
// reference's layer's horizon as the retention's baseline would.
type Lookup struct {
	s        *Store
	it       *pebble.Iterator
	horizons [layers]store.Horizon
	closed   bool
}

// NewLookup opens a Lookup. It counts "lookup.iterators". It returns an error
// wrapping [store.ErrClosed] if the store is closed.
func (s *Store) NewLookup() (*Lookup, error) {
	if s.closed.Load() {
		return nil, closedError("NewLookup")
	}
	it, err := s.kv.NewIter(nil)
	if err != nil {
		return nil, fmt.Errorf("pebblestore: NewLookup: %w", err)
	}
	s.rec.Count("lookup.iterators", 1)
	// The horizons are read after the iterator is opened, as a read's are: a
	// retention publishes its horizon before it discards anything, so they are never
	// older than the data the iterator sees.
	return &Lookup{s: s, it: it, horizons: *s.horizons.Load()}, nil
}

// Close releases the iterator. A second call does nothing, and neither does a call
// after the store was closed, which has released the database already: close every
// Lookup before the store.
func (l *Lookup) Close() error {
	if l.closed {
		return nil
	}
	l.closed = true
	if l.s.closed.Load() {
		return nil
	}
	if err := l.it.Close(); err != nil {
		return fmt.Errorf("pebblestore: Lookup.Close: %w", err)
	}
	return nil
}

// Last finds the newest record of the reference, as of the latest sequence number
// the Lookup saw when it was opened. It reads one side of the subject only: the
// forward prefix of an edge's source, which holds the edge's reference under
// every producer, or the entity's own prefix. It never reads the target's reverse
// prefix, however many records that holds.
//
// The walk goes from the newest key of that prefix toward older ones:
//
//   - a checkpoint is stepped over: it holds no payload, and is not a record of the
//     reference;
//   - a record of another reference is stepped over;
//   - at the first record of the reference, the walk stops at the end of that
//     record's instant, and of the records of the reference at it takes the one with
//     the latest [store.LastOf], then the highest Seq: [store.LookupFound];
//   - at the baseline a retention left, the walk stops: if the baseline names the
//     reference, that is its newest assertion, with its times and no payload
//     ([store.LookupBaselineOnly]), and if not, what was asserted before the horizon
//     is gone ([store.LookupNone]);
//   - at the first record of the reference before the layer's horizon (a prefix a
//     Background pass has not rewritten yet), the walk stops as it would at the
//     baseline the pass will write: [store.LookupBaselineOnly] with that record's
//     times and no payload if it is an Observe that still holds at the horizon, and
//     [store.LookupNone] if not. A baseline entry that no longer holds at the
//     horizon, because a later retention moved it, is none;
//   - at the end of the prefix it stops with [store.LookupNone].
//
// After [LookupWalkBound] keys it stops with [store.LookupExhausted], which says
// nothing about the reference. The keys counted are those the walk used: the key
// that ends the walk without being used, the next after the answer's instant or
// the one past the bound, is read but not counted. The count is sampled under
// "lookup.keys_walked" for every call that reaches the walk, and each exhaustion
// counts "lookup.exhausted".
//
// A reference of an entity type or relation the catalog does not know can hold
// nothing, and is [store.LookupNone] with no key walked. An error is returned as
// an error, never as an outcome, and an answer never has the outcome
// [store.LookupInvalid].
func (l *Lookup) Last(ctx context.Context, ref store.LookupRef) (store.LastAssertion, error) {
	if l.closed || l.s.closed.Load() {
		return store.LastAssertion{}, closedError("Lookup.Last")
	}
	if err := ctx.Err(); err != nil {
		return store.LastAssertion{}, contextError("Lookup.Last", err)
	}
	if err := ref.Validate(); err != nil {
		return store.LastAssertion{}, fmt.Errorf("pebblestore: Lookup.Last: %w", err)
	}
	prefix, key, ok := l.s.lookupKey(ref)
	if !ok {
		l.s.rec.Sample("lookup.keys_walked", 0)
		return store.LastAssertion{Outcome: store.LookupNone}, nil
	}
	var hNs int64
	var hasH bool
	if h := l.horizons[int(ref.Layer)-int(catalog.L0)]; !h.IsZero() {
		var where pebblekv.Where
		hNs, where = pebblekv.Locate(h.Time)
		hasH = where == pebblekv.Inside
	}
	got, keys, err := l.walk(prefix, key, hNs, hasH)
	l.s.rec.Sample("lookup.keys_walked", keys)
	if err != nil {
		return store.LastAssertion{}, fmt.Errorf("pebblestore: Lookup.Last: %w", err)
	}
	if got.Outcome == store.LookupExhausted {
		l.s.rec.Count("lookup.exhausted", 1)
	}
	return got, nil
}

// lookupKey is the prefix to search for a reference, and the reference as that
// prefix files it (see [appendRef]), or ok false if the catalog does not know its
// entity types or relation, so that nothing can be stored under it.
func (s *Store) lookupKey(ref store.LookupRef) (prefix, key []byte, ok bool) {
	sub := ref.Subject
	if sub.Kind == store.SubjectEntity {
		prefix, ok = s.prefixOf(ref.Layer, sub.A, dirEntity)
		return prefix, appendRef(nil, nil, nil, ref.Producer), ok
	}
	if prefix, ok = s.prefixOf(ref.Layer, sub.A, byte(store.Forward)); !ok {
		return nil, nil, false
	}
	peer, err := s.ids.AppendFingerprint(nil, sub.B)
	if err != nil {
		return nil, nil, false
	}
	rel, err := s.ids.AppendRelation(nil, sub.Relation)
	if err != nil {
		return nil, nil, false
	}
	return prefix, appendRef(nil, peer, rel, ref.Producer), true
}

// walk is the search of [Lookup.Last] over one prefix, for one reference, where
// the layer's horizon is hNs if hasH. It returns the number of keys it used.
func (l *Lookup) walk(prefix, ref []byte, hNs int64, hasH bool) (got store.LastAssertion, keys int64, err error) {
	dir := prefix[prefixLen-1]
	it := l.it
	it.SetBounds(prefixBounds(prefix))
	var (
		best   pebblekv.Value
		bestNs int64
		found  bool
	)
	for ok := it.First(); ok; ok = it.Next() {
		_, ns, seq, kind, err := parseKey(it.Key())
		if err != nil {
			return got, keys, err
		}
		if found && (kind != kindRecord || ns != bestNs) {
			break // the records of the reference at its newest instant are all seen
		}
		if keys == LookupWalkBound {
			return store.LastAssertion{Outcome: store.LookupExhausted}, keys, nil
		}
		keys++
		switch kind {
		case kindCheckpoint:
			continue
		case kindRecord:
			r, v, err := decodeRecordValue(dir, it.Value())
			if err != nil {
				return got, keys, err
			}
			if v.Seq != seq {
				return got, keys, fmt.Errorf("a record's key has seq %d and its value %d", seq, v.Seq)
			}
			if !bytes.Equal(r, ref) {
				continue
			}
			if hasH && ns < hNs {
				// Before the horizon this is the record a baseline would hold the
				// reference by, the first met (a retention decides by the key order,
				// not by Last), if it is an Observe still alive at the horizon.
				if v.Holds(ns, hNs) {
					v.Payload = nil
					return lastAssertionOf(store.LookupBaselineOnly, ns, v), keys, nil
				}
				return store.LastAssertion{Outcome: store.LookupNone}, keys, nil
			}
			// Highest Seq first within an instant, so a later record of the same
			// instant takes over only with a strictly later Last.
			if !found || lastNs(ns, v) > lastNs(bestNs, best) {
				best, bestNs, found = v, ns, true
				best.Payload = bytes.Clone(v.Payload)
			}
		case kindBaseline:
			st, err := decodeStamp(it.Value())
			if err != nil {
				return got, keys, err
			}
			if st.kind != kindBaseline {
				return got, keys, fmt.Errorf("a baseline key holds a stamp of kind %d", st.kind)
			}
			for _, en := range st.entries {
				if bytes.Equal(en.ref, ref) {
					if hasH && !en.value.Holds(en.eventNs, hNs) {
						return store.LastAssertion{Outcome: store.LookupNone}, keys, nil
					}
					v := en.value
					v.Payload = nil // an entry keeps none
					return lastAssertionOf(store.LookupBaselineOnly, en.eventNs, v), keys, nil
				}
			}
			return store.LastAssertion{Outcome: store.LookupNone}, keys, nil
		}
	}
	if err := it.Error(); err != nil {
		return got, keys, err
	}
	if !found {
		return store.LastAssertion{Outcome: store.LookupNone}, keys, nil
	}
	return lastAssertionOf(store.LookupFound, bestNs, best), keys, nil
}

// lastNs is the latest instant a record at event time ns with value v says its
// producer asserted its subject, as [store.LastOf] gives it.
func lastNs(ns int64, v pebblekv.Value) int64 {
	if v.HasThrough && v.Through > ns {
		return v.Through
	}
	return ns
}

// lastAssertionOf is the answer for the record at event time ns with value v.
func lastAssertionOf(outcome store.LookupOutcome, ns int64, v pebblekv.Value) store.LastAssertion {
	a := store.LastAssertion{
		Outcome: outcome, Kind: v.Kind, EventTime: pebblekv.Time(ns), TTL: v.TTL,
		Basis: store.EventTimeBasis(v.Basis), Seq: v.Seq,
	}
	if v.HasThrough {
		a.Through = pebblekv.Time(v.Through)
	}
	a.Last, a.LastFromThrough = store.LastOf(a.EventTime, a.Through)
	if len(v.Payload) > 0 {
		a.Payload = v.Payload
	}
	return a
}
