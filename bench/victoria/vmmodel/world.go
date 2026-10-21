package vmmodel

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// World is the reference's view of the subjects a set of entities touch: every record of
// each, folded by lifecycle.Fold, which is the rule of the reference engine. It answers
// what a boundary classification needs: the maximal existence intervals of a subject and
// the subjects incident to an entity. It keeps the whole history it is given, so a
// retention that moved the horizon during the stream does not shorten an interval: the
// store compared with it keeps everything too.
//
// A World is a runner.Sink. It is not safe for concurrent use.
type World struct {
	only     map[identity.Fingerprint]struct{}
	records  map[engine.Subject][]engine.Record
	layers   map[engine.Subject]catalog.Layer
	incident [3][]map[identity.Fingerprint][]engine.Subject // by direction, then layer
	folds    map[engine.Subject][]lifecycle.Interval
	lastSeq  uint64
}

// NewWorld returns a world that keeps the records touching the given entities (the
// subject, or either end of an edge), as the reference engine's shadow does. With no
// entities it keeps every record.
func NewWorld(entities ...identity.Fingerprint) *World {
	w := &World{
		records: map[engine.Subject][]engine.Record{},
		layers:  map[engine.Subject]catalog.Layer{},
		folds:   map[engine.Subject][]lifecycle.Interval{},
	}
	if len(entities) > 0 {
		w.only = map[identity.Fingerprint]struct{}{}
		for _, fp := range entities {
			w.only[fp] = struct{}{}
		}
	}
	for _, d := range []engine.Direction{engine.Forward, engine.Reverse} {
		w.incident[d] = make([]map[identity.Fingerprint][]engine.Subject, catalog.L3+1)
	}
	return w
}

func (w *World) touches(r engine.Record) bool {
	if w.only == nil {
		return true
	}
	if _, ok := w.only[r.Subject.A]; ok {
		return true
	}
	if r.Subject.Kind == engine.SubjectEdge {
		_, ok := w.only[r.Subject.B]
		return ok
	}
	return false
}

// Write implements runner.Sink. It keeps what touches its entities, without payloads.
func (w *World) Write(batch []engine.Record) error {
	for _, r := range batch {
		if r.Seq <= w.lastSeq {
			return fmt.Errorf("vmmodel: world: seq %d follows seq %d", r.Seq, w.lastSeq)
		}
		w.lastSeq = r.Seq
		if !w.touches(r) {
			continue
		}
		if known, ok := w.layers[r.Subject]; ok && known != r.Layer {
			return fmt.Errorf("vmmodel: world: subject %s is in layer %s and layer %s", describe(r.Subject), known, r.Layer)
		} else if !ok {
			w.layers[r.Subject] = r.Layer
			if r.Subject.Kind == engine.SubjectEdge {
				w.link(engine.Forward, r.Layer, r.Subject.A, r.Subject)
				w.link(engine.Reverse, r.Layer, r.Subject.B, r.Subject)
			}
		}
		r.Payload = nil
		w.records[r.Subject] = append(w.records[r.Subject], r)
		delete(w.folds, r.Subject)
	}
	return nil
}

func (w *World) link(d engine.Direction, l catalog.Layer, fp identity.Fingerprint, s engine.Subject) {
	if w.incident[d][l] == nil {
		w.incident[d][l] = map[identity.Fingerprint][]engine.Subject{}
	}
	w.incident[d][l][fp] = append(w.incident[d][l][fp], s)
}

// Retain implements runner.Sink and does nothing: see the type.
func (w *World) Retain(time.Time) error { return nil }

// Existence returns the maximal existence intervals of the subject, ascending. A subject
// with no records has none.
func (w *World) Existence(s engine.Subject) ([]lifecycle.Interval, error) {
	if ivs, ok := w.folds[s]; ok {
		return ivs, nil
	}
	recs := w.records[s]
	if len(recs) == 0 {
		return nil, nil
	}
	as := make([]lifecycle.Assertion, len(recs))
	for i, r := range recs {
		as[i] = lifecycle.Assertion{
			Producer: r.Producer, EventTime: r.EventTime, Seq: r.Seq,
			Kind: r.Kind, TTL: r.TTL, Through: r.Through,
		}
	}
	tl, err := lifecycle.Fold(as, lifecycle.Policy{})
	if err != nil {
		return nil, fmt.Errorf("vmmodel: world: folding %s: %w", describe(s), err)
	}
	ivs := tl.Existence()
	w.folds[s] = ivs
	return ivs, nil
}

// AliveAt reports whether the subject exists at t.
func (w *World) AliveAt(s engine.Subject, t time.Time) (bool, error) {
	ivs, err := w.Existence(s)
	if err != nil {
		return false, err
	}
	for _, iv := range ivs {
		if !iv.Start.After(t) && (iv.Open() || t.Before(iv.End)) {
			return true, nil
		}
	}
	return false, nil
}

// Incident returns the edges stored in the layer that touch the entity in the direction
// (forward: it is the source; reverse: the target), ordered by peer and relation as a
// read returns them.
func (w *World) Incident(fp identity.Fingerprint, d engine.Direction, l catalog.Layer) []engine.Subject {
	if l < catalog.L0 || int(l) >= len(w.incident[d]) {
		return nil
	}
	out := slices.Clone(w.incident[d][l][fp])
	slices.SortFunc(out, func(a, b engine.Subject) int {
		return compareNeighbor(peerOf(a, d), a.Relation, peerOf(b, d), b.Relation)
	})
	return out
}

// Neighbors returns the edges alive at t that touch the entity in the direction, in the
// layer, as the reference engine's Neighbors does for the latest snapshot. It exists to
// check this package against that engine.
func (w *World) Neighbors(fp identity.Fingerprint, d engine.Direction, l catalog.Layer, t time.Time) ([]engine.Neighbor, error) {
	var out []engine.Neighbor
	for _, s := range w.Incident(fp, d, l) {
		ok, err := w.AliveAt(s, t)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, engine.Neighbor{Peer: peerOf(s, d), Relation: s.Relation})
		}
	}
	return out, nil
}

func peerOf(s engine.Subject, d engine.Direction) identity.Fingerprint {
	if d == engine.Forward {
		return s.B
	}
	return s.A
}

func compareNeighbor(pa identity.Fingerprint, ra catalog.RelationType, pb identity.Fingerprint, rb catalog.RelationType) int {
	if c := engine.CompareFingerprints(pa, pb); c != 0 {
		return c
	}
	return cmp.Compare(ra, rb)
}

// ErrUnclassifiable marks a query the world cannot classify because it is not a read of
// the latest snapshot at an instant (the metrics store has no such read to compare).
var ErrUnclassifiable = errors.New("vmmodel: the query is not an instant read of the latest snapshot")
