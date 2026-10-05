// Package oracle is the reference engine: it keeps every record in memory and
// answers each question by folding the stored assertions with the lifecycle
// specification, with no indexes, no cleverness and no persistence.
//
// A candidate layout earns trust by agreeing with it. It is deliberately
// slow: its only job is to be obviously correct.
package oracle

import (
	"fmt"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// Oracle implements [engine.Engine].
type Oracle struct {
	mu      sync.Mutex
	lastSeq uint64
	horizon time.Time

	bySubject map[engine.Subject][]stored        // subject -> its records, ascending Seq
	layers    map[engine.Subject]catalog.Layer   // the one layer each subject is stored in
	incident  [3]map[identity.Fingerprint][]edge // by direction: entity -> its edges
	folds     map[engine.Subject]map[int]lifecycle.Timeline
}

// stored is a record as the oracle keeps it, without what its subject says (the
// subject and its layer are kept once, not in every record), and with its times as
// Unix nanoseconds: about a third of the memory of an [engine.Record], which is what
// lets the reference engine hold a month of a busy prefix.
type stored struct {
	producer       lifecycle.Producer
	seq            uint64
	event, through int64 // Unix nanoseconds; through is zero for a record that is not a run
	ttl            time.Duration
	payload        []byte
	kind           lifecycle.Kind
}

func keep(r engine.Record) stored {
	st := stored{producer: r.Producer, seq: r.Seq, event: r.EventTime.UnixNano(), ttl: r.TTL, payload: r.Payload, kind: r.Kind}
	if !r.Through.IsZero() {
		st.through = r.Through.UnixNano()
	}
	return st
}

// record is the [engine.Record] of a stored one of the subject, whose layer is layer.
func (st stored) record(s engine.Subject, layer catalog.Layer) engine.Record {
	r := engine.Record{
		Layer: layer, Subject: s, Producer: st.producer, EventTime: time.Unix(0, st.event).UTC(),
		Seq: st.seq, Kind: st.kind, TTL: st.ttl, Payload: st.payload,
	}
	if st.through != 0 {
		r.Through = time.Unix(0, st.through).UTC()
	}
	return r
}

// maxCachedFolds bounds the folds kept per subject, so probing many tokens
// cannot grow the cache without limit.
const maxCachedFolds = 32

type edge struct {
	subject engine.Subject
	peer    identity.Fingerprint
}

var _ engine.Engine = (*Oracle)(nil)

// New returns an empty oracle.
func New() *Oracle {
	o := &Oracle{
		bySubject: make(map[engine.Subject][]stored),
		layers:    make(map[engine.Subject]catalog.Layer),
		folds:     make(map[engine.Subject]map[int]lifecycle.Timeline),
	}
	o.incident[engine.Forward] = make(map[identity.Fingerprint][]edge)
	o.incident[engine.Reverse] = make(map[identity.Fingerprint][]edge)
	return o
}

// Write implements [engine.Engine]. It also refuses a record whose layer
// differs from the layer its subject was first stored in: that is the caller's
// precondition for every engine, and the oracle is where a bad workload is
// caught.
func (o *Oracle) Write(batch []engine.Record) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	seq := o.lastSeq
	inBatch := make(map[engine.Subject]catalog.Layer)
	for _, r := range batch {
		if err := r.Validate(); err != nil {
			return err
		}
		if r.EventTime.Before(o.horizon) {
			return fmt.Errorf("record seq %d at %s: %w", r.Seq, r.EventTime.Format(time.RFC3339), engine.ErrBeforeHorizon)
		}
		if r.Seq <= seq {
			return fmt.Errorf("record seq %d is not above the last written seq %d: %w", r.Seq, seq, engine.ErrInvalid)
		}
		seq = r.Seq
		known, ok := o.layers[r.Subject]
		if !ok {
			known, ok = inBatch[r.Subject]
		}
		if ok && known != r.Layer {
			return fmt.Errorf("record seq %d: subject is stored in layer %s, not %s: %w", r.Seq, known, r.Layer, engine.ErrInvalid)
		}
		inBatch[r.Subject] = r.Layer
	}
	for _, r := range batch {
		r.Payload = slices.Clone(r.Payload)
		if _, known := o.bySubject[r.Subject]; !known {
			o.layers[r.Subject] = r.Layer
			if r.Subject.Kind == engine.SubjectEdge {
				o.incident[engine.Forward][r.Subject.A] = append(o.incident[engine.Forward][r.Subject.A], edge{r.Subject, r.Subject.B})
				o.incident[engine.Reverse][r.Subject.B] = append(o.incident[engine.Reverse][r.Subject.B], edge{r.Subject, r.Subject.A})
			}
		}
		o.bySubject[r.Subject] = append(o.bySubject[r.Subject], keep(r))
	}
	o.lastSeq = seq
	return nil
}

// LastSeq implements [engine.Engine].
func (o *Oracle) LastSeq() uint64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.lastSeq
}

// Layers returns, ascending, the layers in which fp has anything stored: its
// own existence or any edge touching it, in either direction. It is not part of
// [engine.Engine]; the conformance test uses it to probe the layers an entity is
// in and one it is not.
func (o *Oracle) Layers(fp identity.Fingerprint) []catalog.Layer {
	o.mu.Lock()
	defer o.mu.Unlock()
	seen := map[catalog.Layer]bool{}
	if l, ok := o.layers[engine.EntitySubject(fp)]; ok {
		seen[l] = true
	}
	for _, dir := range []engine.Direction{engine.Forward, engine.Reverse} {
		for _, e := range o.incident[dir][fp] {
			seen[o.layers[e.subject]] = true
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
func (o *Oracle) visible(recs []stored, asOf uint64) int {
	return sort.Search(len(recs), func(i int) bool { return recs[i].seq > asOf })
}

// timeline folds the first n records of a subject: what a token that sees n of
// them sees. The first n records never change, so a cached fold never goes
// stale.
func (o *Oracle) timeline(s engine.Subject, n int) (lifecycle.Timeline, error) {
	if n == 0 {
		return lifecycle.Timeline{}, nil
	}
	if tl, ok := o.folds[s][n]; ok {
		return tl, nil
	}
	recs := o.bySubject[s][:n]
	as := make([]lifecycle.Assertion, n)
	for i, st := range recs {
		as[i] = st.record(s, o.layers[s]).Assertion()
	}
	tl, err := lifecycle.Fold(as, lifecycle.Policy{})
	if err != nil {
		return lifecycle.Timeline{}, fmt.Errorf("oracle: folding %v: %w", s, err)
	}
	cache := o.folds[s]
	if cache == nil || len(cache) >= maxCachedFolds {
		cache = make(map[int]lifecycle.Timeline)
		o.folds[s] = cache
	}
	cache[n] = tl
	return tl, nil
}

// aliveAt folds what the scope sees of a subject and asks it.
func (o *Oracle) aliveAt(s engine.Subject, t time.Time, sc engine.Scope) (bool, error) {
	if o.layers[s] != sc.Layer {
		return false, nil
	}
	tl, err := o.timeline(s, o.visible(o.bySubject[s], sc.AsOf))
	if err != nil {
		return false, err
	}
	return tl.AliveAt(t), nil
}

func (o *Oracle) neighbors(fp identity.Fingerprint, dir engine.Direction, t time.Time, sc engine.Scope) ([]engine.Neighbor, error) {
	var out []engine.Neighbor
	for _, e := range o.incident[dir][fp] {
		alive, err := o.aliveAt(e.subject, t, sc)
		if err != nil {
			return nil, err
		}
		if alive {
			out = append(out, engine.Neighbor{Peer: e.peer, Relation: e.subject.Relation})
		}
	}
	engine.SortNeighbors(out)
	return out, nil
}

// Neighbors implements [engine.Engine].
func (o *Oracle) Neighbors(fp identity.Fingerprint, dir engine.Direction, t time.Time, sc engine.Scope) ([]engine.Neighbor, error) {
	if err := sc.Validate(); err != nil {
		return nil, err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.neighbors(fp, dir, t, sc)
}

// NeighborsBatch implements [engine.Engine].
func (o *Oracle) NeighborsBatch(fps []identity.Fingerprint, dir engine.Direction, t time.Time, sc engine.Scope) ([][]engine.Neighbor, error) {
	if err := sc.Validate(); err != nil {
		return nil, err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	return engine.NeighborsEach(fps, func(fp identity.Fingerprint) ([]engine.Neighbor, error) {
		return o.neighbors(fp, dir, t, sc)
	})
}

// Alive implements [engine.Engine].
func (o *Oracle) Alive(fp identity.Fingerprint, t time.Time, sc engine.Scope) (bool, error) {
	if err := sc.Validate(); err != nil {
		return false, err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.aliveAt(engine.EntitySubject(fp), t, sc)
}

// Window implements [engine.Engine].
func (o *Oracle) Window(fp identity.Fingerprint, dir engine.Direction, from, to time.Time, sc engine.Scope) ([]engine.Record, error) {
	if err := sc.Validate(); err != nil {
		return nil, err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	var out []engine.Record
	for _, e := range o.incident[dir][fp] {
		if o.layers[e.subject] != sc.Layer {
			continue
		}
		recs := o.bySubject[e.subject]
		for _, st := range recs[:o.visible(recs, sc.AsOf)] {
			r := st.record(e.subject, o.layers[e.subject])
			if !r.EventTime.Before(from) && r.EventTime.Before(to) {
				r.Payload = slices.Clone(r.Payload)
				out = append(out, r)
			}
		}
	}
	engine.SortRecords(out)
	return out, nil
}

// Retain implements [engine.Engine]. The oracle keeps everything, which
// answers every question at or after the horizon trivially; it only records
// the horizon so that Write can refuse what is older.
func (o *Oracle) Retain(horizon time.Time) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if horizon.After(o.horizon) {
		o.horizon = horizon
	}
	return nil
}

// Size implements [engine.Engine]: the payload bytes plus a fixed overhead per
// record, only as a rough guide.
func (o *Oracle) Size() (int64, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	var n int64
	for _, recs := range o.bySubject {
		for _, st := range recs {
			n += int64(len(st.payload)) + 96
		}
	}
	return n, nil
}

// Close implements [engine.Engine].
func (o *Oracle) Close() error { return nil }
