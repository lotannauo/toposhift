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
	"sync"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// Oracle implements [engine.Engine].
type Oracle struct {
	mu      sync.Mutex
	lastSeq uint64
	horizon time.Time
	recs    []engine.Record

	bySubject map[engine.Subject][]int           // subject -> indexes into recs
	incident  [3]map[identity.Fingerprint][]edge // by direction: entity -> its edges
	folds     map[engine.Subject]fold
}

type edge struct {
	subject engine.Subject
	peer    identity.Fingerprint
}

type fold struct {
	n  int // records folded, so a cache entry is stale after a write
	tl lifecycle.Timeline
}

var _ engine.Engine = (*Oracle)(nil)

// New returns an empty oracle.
func New() *Oracle {
	o := &Oracle{
		bySubject: make(map[engine.Subject][]int),
		folds:     make(map[engine.Subject]fold),
	}
	o.incident[engine.Forward] = make(map[identity.Fingerprint][]edge)
	o.incident[engine.Reverse] = make(map[identity.Fingerprint][]edge)
	return o
}

// Write implements [engine.Engine].
func (o *Oracle) Write(batch []engine.Record) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	seq := o.lastSeq
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
	}
	for _, r := range batch {
		r.Payload = slices.Clone(r.Payload)
		if _, known := o.bySubject[r.Subject]; !known && r.Subject.Kind == engine.SubjectEdge {
			o.incident[engine.Forward][r.Subject.A] = append(o.incident[engine.Forward][r.Subject.A], edge{r.Subject, r.Subject.B})
			o.incident[engine.Reverse][r.Subject.B] = append(o.incident[engine.Reverse][r.Subject.B], edge{r.Subject, r.Subject.A})
		}
		o.bySubject[r.Subject] = append(o.bySubject[r.Subject], len(o.recs))
		o.recs = append(o.recs, r)
	}
	o.lastSeq = seq
	return nil
}

// timeline folds a subject's records, reusing the last fold if nothing was
// written since.
func (o *Oracle) timeline(s engine.Subject) (lifecycle.Timeline, error) {
	idx := o.bySubject[s]
	if f, ok := o.folds[s]; ok && f.n == len(idx) {
		return f.tl, nil
	}
	as := make([]lifecycle.Assertion, len(idx))
	for i, j := range idx {
		as[i] = o.recs[j].Assertion()
	}
	tl, err := lifecycle.Fold(as, lifecycle.Policy{})
	if err != nil {
		return lifecycle.Timeline{}, fmt.Errorf("oracle: folding %v: %w", s, err)
	}
	o.folds[s] = fold{n: len(idx), tl: tl}
	return tl, nil
}

// Neighbors implements [engine.Engine].
func (o *Oracle) Neighbors(fp identity.Fingerprint, dir engine.Direction, t time.Time) ([]engine.Neighbor, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	var out []engine.Neighbor
	for _, e := range o.incident[dir][fp] {
		tl, err := o.timeline(e.subject)
		if err != nil {
			return nil, err
		}
		if tl.AliveAt(t) {
			out = append(out, engine.Neighbor{Peer: e.peer, Relation: e.subject.Relation})
		}
	}
	engine.SortNeighbors(out)
	return out, nil
}

// Alive implements [engine.Engine].
func (o *Oracle) Alive(fp identity.Fingerprint, t time.Time) (bool, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	tl, err := o.timeline(engine.EntitySubject(fp))
	if err != nil {
		return false, err
	}
	return tl.AliveAt(t), nil
}

// Window implements [engine.Engine].
func (o *Oracle) Window(fp identity.Fingerprint, dir engine.Direction, from, to time.Time) ([]engine.Record, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	var out []engine.Record
	for _, e := range o.incident[dir][fp] {
		for _, j := range o.bySubject[e.subject] {
			r := o.recs[j]
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
	for _, r := range o.recs {
		n += int64(len(r.Payload)) + 96
	}
	return n, nil
}

// Close implements [engine.Engine].
func (o *Oracle) Close() error { return nil }
