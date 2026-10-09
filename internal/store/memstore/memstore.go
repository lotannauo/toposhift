package memstore

import (
	"fmt"
	"maps"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

// Options configures a store.
type Options struct {
	// Policy is the lifecycle policy entity existence is folded with. It is fixed
	// for the life of the store. The zero value tracks no boots.
	Policy lifecycle.Policy
	// Offsets is the retention offset of each layer, indexed by layer minus L0
	// (Offsets[0] is L0's). Retain(h) sets the horizon of a retained layer to h
	// minus its offset, so a layer with a longer offset keeps more history. The
	// zero value is no offset: every layer's horizon is h. An offset must not be
	// negative.
	Offsets [4]time.Duration
	// Keep says, per layer and indexed like Offsets, that the layer is kept: its
	// horizon never moves, whatever its offset.
	Keep [4]bool
}

// Store is the reference implementation of [store.Store]. It is safe for
// concurrent use: one mutex guards everything, because a read changes the fold
// cache, and a read, even a whole NeighborsBatch, then sees one snapshot of
// whole batches.
type Store struct {
	mu      sync.Mutex
	closed  bool
	policy  lifecycle.Policy
	lastSeq uint64
	offsets [4]time.Duration
	keep    [4]bool
	hz      [4]store.Horizon // the horizon of each layer, indexed by layer minus L0
	horizon store.Horizon    // the horizon of the layer retained most recently

	bySubject map[store.Subject][]stored         // subject -> its records, ascending Seq
	layers    map[store.Subject]catalog.Layer    // the one layer each subject is stored in
	incident  [3]map[identity.Fingerprint][]edge // by direction: entity -> its edges
	folds     map[store.Subject]map[int]fold     // subject -> number of visible records -> fold
}

var _ store.Store = (*Store)(nil)

// stored is a record as the store keeps it, without what its subject says (the
// subject and its layer are kept once, not in every record), and with its times
// as Unix nanoseconds.
type stored struct {
	producer       lifecycle.Producer
	seq            uint64
	event, through int64 // Unix nanoseconds; through is meaningful only if hasThrough
	hasThrough     bool
	ttl            time.Duration
	payload        []byte
	boot           string // the boot id of a host observation, or empty
	kind           lifecycle.Kind
	basis          store.EventTimeBasis // where the event time came from
}

func keep(r store.Record) stored {
	st := stored{
		producer: r.Producer, seq: r.Seq, event: r.EventTime.UnixNano(),
		ttl: r.TTL, payload: slices.Clone(r.Payload), boot: r.Boot, kind: r.Kind, basis: r.EventTimeBasis,
	}
	if !r.Through.IsZero() {
		st.through, st.hasThrough = r.Through.UnixNano(), true
	}
	return st
}

// record is the [store.Record] of a stored one of the subject, whose layer is
// layer. Its times are in UTC, and its payload is shared with the store.
func (st stored) record(s store.Subject, layer catalog.Layer) store.Record {
	r := store.Record{
		Layer: layer, Subject: s, Producer: st.producer, EventTime: time.Unix(0, st.event).UTC(),
		Seq: st.seq, Kind: st.kind, TTL: st.ttl, Payload: st.payload, Boot: st.boot,
		EventTimeBasis: st.basis,
	}
	if st.hasThrough {
		r.Through = time.Unix(0, st.through).UTC()
	}
	return r
}

// maxCachedFolds bounds the folds kept per subject, so probing many tokens
// cannot grow the cache without limit.
const maxCachedFolds = 32

// fold is a cached result of folding a prefix of a subject's records. The
// error is cached too, so a quarantined entity is not folded again for every
// read.
type fold struct {
	tl  lifecycle.Timeline
	err error
}

type edge struct {
	subject store.Subject
	peer    identity.Fingerprint
}

// layerIndex is the position of a layer in the per-layer arrays, and whether the
// layer is one of L0 to L3.
func layerIndex(l catalog.Layer) (int, bool) {
	if l < catalog.L0 || l > catalog.L3 {
		return 0, false
	}
	return int(l - catalog.L0), true
}

// Open returns a new, empty store. It refuses a policy the lifecycle
// specification refuses, with an error wrapping [store.ErrInvalid] and
// [lifecycle.ErrInvalid], and a negative retention offset, with an error
// wrapping [store.ErrInvalid].
func Open(opts Options) (*Store, error) {
	for i, o := range opts.Offsets {
		if o < 0 {
			return nil, fmt.Errorf("memstore: Open: the retention offset of layer %s is %s, which is negative: %w",
				catalog.L0+catalog.Layer(i), o, store.ErrInvalid)
		}
	}
	// Fold validates the policy before anything else, and the check itself is
	// not exported.
	if _, err := lifecycle.Fold(nil, opts.Policy); err != nil {
		return nil, fmt.Errorf("memstore: Open: %w: %w", store.ErrInvalid, err)
	}
	policy := opts.Policy
	policy.Rank = maps.Clone(policy.Rank)
	s := &Store{policy: policy, offsets: opts.Offsets, keep: opts.Keep}
	s.reset()
	return s, nil
}

// reset gives the store empty maps.
func (s *Store) reset() {
	s.bySubject = make(map[store.Subject][]stored)
	s.layers = make(map[store.Subject]catalog.Layer)
	s.folds = make(map[store.Subject]map[int]fold)
	s.incident[store.Forward] = make(map[identity.Fingerprint][]edge)
	s.incident[store.Reverse] = make(map[identity.Fingerprint][]edge)
}

// LastSeq implements [store.Store]. After Close it returns the value it had.
func (s *Store) LastSeq() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastSeq
}

// Horizon implements [store.Store]: the horizon of the layer retained most
// recently. After Close it returns the value it had.
func (s *Store) Horizon() store.Horizon {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.horizon
}

// LayerHorizon implements [store.Store]. A layer outside L0 to L3 gets the zero
// Horizon. After Close it returns the value it had.
func (s *Store) LayerHorizon(layer catalog.Layer) store.Horizon {
	s.mu.Lock()
	defer s.mu.Unlock()
	i, ok := layerIndex(layer)
	if !ok {
		return store.Horizon{}
	}
	return s.hz[i]
}

// Close implements [store.Store]. The first call marks the store closed and
// drops what it holds, so the memory is released; later calls return nil.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	s.bySubject, s.layers, s.folds = nil, nil, nil
	s.incident = [3]map[identity.Fingerprint][]edge{}
	return nil
}

// Layers returns, ascending, the layers in which fp has anything stored: its
// own existence or any edge touching it, in either direction. It ignores tokens
// and the horizon. It is not part of [store.Store]: the conformance suite uses
// it to probe the layers an entity is in and one it is not. After Close it
// returns nil.
func (s *Store) Layers(fp identity.Fingerprint) []catalog.Layer {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	seen := map[catalog.Layer]bool{}
	if l, ok := s.layers[store.EntitySubject(fp)]; ok {
		seen[l] = true
	}
	for _, dir := range []store.Direction{store.Forward, store.Reverse} {
		for _, e := range s.incident[dir][fp] {
			seen[s.layers[e.subject]] = true
		}
	}
	out := make([]catalog.Layer, 0, len(seen))
	for l := range seen {
		out = append(out, l)
	}
	slices.Sort(out)
	return out
}

// visible is how many of a subject's records a token sees. The records are in
// ascending Seq, so they are a prefix.
func visible(recs []stored, asOf uint64) int {
	return sort.Search(len(recs), func(i int) bool { return recs[i].seq > asOf })
}

// policyOf is the policy a subject is folded with: the store's for the
// existence of an entity, and the zero policy for an edge.
func (s *Store) policyOf(sub store.Subject) lifecycle.Policy {
	if sub.Kind == store.SubjectEntity {
		return s.policy
	}
	return lifecycle.Policy{}
}

// timeline folds the first n records of a subject: what a token that sees n of
// them sees. The first n records never change, so a cached fold never goes
// stale.
func (s *Store) timeline(sub store.Subject, n int) (lifecycle.Timeline, error) {
	if n == 0 {
		return lifecycle.Timeline{}, nil
	}
	if f, ok := s.folds[sub][n]; ok {
		return f.tl, f.err
	}
	recs := s.bySubject[sub][:n]
	as := make([]lifecycle.Assertion, n)
	for i, st := range recs {
		as[i] = st.record(sub, s.layers[sub]).Assertion()
	}
	tl, err := lifecycle.Fold(as, s.policyOf(sub))
	cache := s.folds[sub]
	if cache == nil || len(cache) >= maxCachedFolds {
		cache = make(map[int]fold)
		s.folds[sub] = cache
	}
	cache[n] = fold{tl: tl, err: err}
	return tl, err
}
