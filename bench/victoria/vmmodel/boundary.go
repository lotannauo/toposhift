package vmmodel

import (
	"fmt"
	"slices"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// Slack is the width of the band around every start and end of existence: the
// truncation of a timestamp to the millisecond (assumption (i), verified), which can move
// a sample, and the instant a read is made at, by less than one.
const Slack = time.Millisecond

// InBand reports whether the existence described by the intervals starts or ends within
// (t-band, t+band]: the left end excluded, the right end included. Intervals that
// overlap or touch are one run first, because the place where one ends and the next
// begins is not a change of existence. An open interval has no end.
func InBand(ivs []lifecycle.Interval, t time.Time, band time.Duration) bool {
	lo, hi := t.Add(-band), t.Add(band)
	in := func(x time.Time) bool { return x.After(lo) && !x.After(hi) }
	for _, iv := range merge(ivs) {
		if in(iv.Start) || (!iv.Open() && in(iv.End)) {
			return true
		}
	}
	return false
}

// merge joins the intervals that overlap or touch, ascending. It does not change its
// argument.
func merge(ivs []lifecycle.Interval) []lifecycle.Interval {
	if len(ivs) < 2 {
		return ivs
	}
	s := slices.Clone(ivs)
	slices.SortFunc(s, func(a, b lifecycle.Interval) int { return a.Start.Compare(b.Start) })
	out := []lifecycle.Interval{s[0]}
	for _, iv := range s[1:] {
		last := &out[len(out)-1]
		if last.Open() || !iv.Start.After(last.End) {
			switch {
			case last.Open():
			case iv.Open():
				last.End = time.Time{}
			case iv.End.After(last.End):
				last.End = iv.End
			}
			continue
		}
		out = append(out, iv)
	}
	return out
}

// Kind is how a query is classified.
type Kind uint8

const (
	// Compared: the store's answer must equal the plan's.
	Compared Kind = iota
	// Boundary: an involved subject starts or ends within [Slack] of the instant, or the
	// coalesced stream the plan's answer comes from has ended it earlier than the
	// every-sample stream the store holds (the coalesced-end gap): the store may answer
	// differently from the reference by construction, so the query is counted and never
	// compared.
	Boundary
	// ReadsDeletedFact: the store returned a subject the reference does not have because a
	// producer deleted it at most its window ago, and a range function over a window does
	// not see the staleness marker of the delete (assumption (k), verified: it reads the
	// last sample, which is before the delete). It is counted apart, never hidden as a
	// boundary, and never compared; everything else in the answer is checked to be the
	// reference's.
	ReadsDeletedFact
)

func (k Kind) String() string {
	switch k {
	case Compared:
		return "compared"
	case Boundary:
		return "boundary"
	case ReadsDeletedFact:
		return "reads a deleted fact"
	}
	return fmt.Sprintf("Kind(%d)", uint8(k))
}

// Class is how a query is classified, and the number of subjects involved.
type Class struct {
	Kind     Kind
	Subjects int
}

// Boundary reports whether the class is [Boundary].
func (c Class) Boundary() bool { return c.Kind == Boundary }

// NeighborSubjects is the edges a list of neighbors of an entity stands for.
func NeighborSubjects(fp identity.Fingerprint, d engine.Direction, ns []engine.Neighbor) []engine.Subject {
	out := make([]engine.Subject, len(ns))
	for i, n := range ns {
		if d == engine.Forward {
			out[i] = engine.EdgeSubject(fp, n.Peer, n.Relation)
		} else {
			out[i] = engine.EdgeSubject(n.Peer, fp, n.Relation)
		}
	}
	return out
}

func checkInstant(q runner.Query) error {
	if q.AsOf != engine.Latest || (q.Op != runner.OpNeighbors && q.Op != runner.OpBatch && q.Op != runner.OpAlive) {
		return fmt.Errorf("%w: %s", ErrUnclassifiable, q.Name())
	}
	return nil
}

// Answer is the set of subjects the reference answers an instant query with: for a
// neighbors or batch read the edges alive at its instant, for an existence read the
// entity if it is alive. It is in no order and without repeats.
func (w *World) Answer(q runner.Query) ([]engine.Subject, error) {
	if err := checkInstant(q); err != nil {
		return nil, err
	}
	var out []engine.Subject
	seen := map[engine.Subject]struct{}{}
	add := func(s engine.Subject) {
		if _, dup := seen[s]; !dup {
			seen[s] = struct{}{}
			out = append(out, s)
		}
	}
	for _, fp := range q.Fps {
		if q.Op == runner.OpAlive {
			s := engine.EntitySubject(fp)
			ok, err := w.AliveAt(s, q.At)
			if err != nil {
				return nil, err
			}
			if ok {
				add(s)
			}
			continue
		}
		for _, s := range w.Incident(fp, q.Dir, q.Layer) {
			ok, err := w.AliveAt(s, q.At)
			if err != nil {
				return nil, err
			}
			if ok {
				add(s)
			}
		}
	}
	return out, nil
}

// involved are the subjects a query can differ on before any answer is read: every edge
// of the entities asked about that either view has alive at some instant within [Slack]
// of the query's (those the reference answers with, those the store holds and may
// return, and those that start or end at the edge of the band), and for an existence
// read the entity itself, alive or not.
func (w *World) involved(q runner.Query, store *World) ([]engine.Subject, error) {
	if err := checkInstant(q); err != nil {
		return nil, err
	}
	var out []engine.Subject
	for _, fp := range q.Fps {
		if q.Op == runner.OpAlive {
			out = append(out, engine.EntitySubject(fp))
			continue
		}
		for _, v := range []*World{w, store} {
			for _, s := range v.Incident(fp, q.Dir, q.Layer) {
				ok, err := v.near(s, q.At)
				if err != nil {
					return nil, err
				}
				if ok {
					out = append(out, s)
				}
			}
		}
	}
	slices.SortFunc(out, compareSubjects)
	return slices.Compact(out), nil
}

// near reports whether the subject exists at some instant of [t-Slack, t+Slack].
func (w *World) near(s engine.Subject, t time.Time) (bool, error) {
	ivs, err := w.Existence(s)
	if err != nil {
		return false, err
	}
	lo, hi := t.Add(-Slack), t.Add(Slack)
	for _, iv := range ivs {
		if !iv.Start.After(hi) && (iv.Open() || iv.End.After(lo)) {
			return true, nil
		}
	}
	return false, nil
}

// Classify says whether a query is a boundary instant, from the intervals alone, before
// any answer of the store is read. The receiver is the reference's view (the coalesced
// stream the plan was made from) and store is the view of the stream the store is
// written (the every-sample stream); both are folded by the reference's rule. A subject
// involved in the query makes it a boundary if
//
//   - it starts or ends within [Slack] of the instant in either view, or
//   - the store's stream has it alive at the instant and the reference's does not: the
//     coalesced-end gap, the one thing the coalescer is allowed to trade away (it can end
//     an interval early, never late, see [CheckSameFacts]).
//
// The band is that narrow: a producer's window does not widen it, because the store
// reads each producer through that producer's own window, which is the window the
// reference expires it by. Pass the receiver as store if there is no second view.
func (w *World) Classify(store *World, q runner.Query) (Class, error) {
	inv, err := w.involved(q, store)
	if err != nil {
		return Class{}, err
	}
	c := Class{Subjects: len(inv)}
	for _, s := range inv {
		boundary, err := w.atBoundary(store, s, q.At)
		if err != nil {
			return Class{}, err
		}
		if boundary {
			c.Kind = Boundary
			break
		}
	}
	return c, nil
}

func (w *World) atBoundary(store *World, s engine.Subject, t time.Time) (bool, error) {
	ref, err := w.Existence(s)
	if err != nil {
		return false, err
	}
	if InBand(ref, t, Slack) {
		return true, nil
	}
	held, err := store.Existence(s)
	if err != nil {
		return false, err
	}
	if InBand(held, t, Slack) {
		return true, nil
	}
	refAlive, err := w.AliveAt(s, t)
	if err != nil {
		return false, err
	}
	heldAlive, err := store.AliveAt(s, t)
	if err != nil {
		return false, err
	}
	return heldAlive && !refAlive, nil
}

// ClassifyAnswer classifies a query with the subjects the store returned for it (the
// edges of a neighbors or batch read, the entity if an existence read said alive). It is
// [World.Classify] and then, for an instant that is not a boundary, the check of what
// differs from the reference: if the store returned subjects the reference does not
// have, and every one of them is a fact a producer deleted at most its window before the
// instant and the answer has everything else the reference has, the class is
// [ReadsDeletedFact]; any other difference leaves the class [Compared], where it is an
// answer that differs. With sem.StaleVisible a marker ends a window, so nothing is read
// from a deleted fact and no difference is explained.
func (w *World) ClassifyAnswer(reg *Registry, store *World, sem Semantics, q runner.Query, returned []engine.Subject) (Class, error) {
	c, err := w.Classify(store, q)
	if err != nil || c.Kind == Boundary {
		return c, err
	}
	ref, err := w.Answer(q)
	if err != nil {
		return Class{}, err
	}
	want := make(map[engine.Subject]struct{}, len(ref))
	for _, s := range ref {
		want[s] = struct{}{}
	}
	got := make(map[engine.Subject]struct{}, len(returned))
	for _, s := range returned {
		got[s] = struct{}{}
	}
	for s := range want {
		if _, ok := got[s]; !ok {
			return c, nil // a missing subject is never explained
		}
	}
	extras := 0
	for s := range got {
		if _, ok := want[s]; ok {
			continue
		}
		extras++
		if sem.StaleVisible || !store.deletedWithin(reg, s, q.At) {
			return c, nil
		}
	}
	if extras > 0 {
		c.Kind = ReadsDeletedFact
	}
	return c, nil
}

// deletedWithin reports whether a producer that holds the subject deleted it at most its
// window before t: some Delete record at d of a holder with window W has d <= t < d+W.
func (w *World) deletedWithin(reg *Registry, s engine.Subject, t time.Time) bool {
	for _, r := range w.records[s] {
		if r.Kind != lifecycle.Delete {
			continue
		}
		h, ok := reg.holds(s, r.Producer)
		if !ok {
			continue
		}
		if !t.Before(r.EventTime) && t.Before(r.EventTime.Add(h.Window)) {
			return true
		}
	}
	return false
}
