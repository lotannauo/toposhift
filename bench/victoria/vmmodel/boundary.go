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

// Slack is added to a band for the truncation of a timestamp to the millisecond [U] (i).
const Slack = time.Millisecond

// Band is the width of the boundary band of a set of subjects: the largest window among
// the producers that hold any of them, plus [Slack]. For a pod's placement that is the
// window of the cluster-level collector, and for a node the node collector's.
func Band(reg *Registry, subjects []engine.Subject) time.Duration {
	var w time.Duration
	for _, s := range subjects {
		w = max(w, reg.SubjectWindow(s))
	}
	return w + Slack
}

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

// Class is how a query is classified.
type Class struct {
	// Boundary is true when an involved subject starts or ends within the band of the
	// instant: the store may answer differently from the reference there by construction
	// (its sample resolution), so the query is counted and never compared.
	Boundary bool
	// Band is the width of the band, and Subjects the number of subjects involved.
	Band     time.Duration
	Subjects int
}

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

// ReferenceSubjects are the subjects in the reference answer of an instant query: for a
// neighbors or batch read the edges alive at its instant, for an existence read the
// entity itself, whether or not it is alive.
func (w *World) ReferenceSubjects(q runner.Query) ([]engine.Subject, error) {
	if q.AsOf != engine.Latest || (q.Op != runner.OpNeighbors && q.Op != runner.OpBatch && q.Op != runner.OpAlive) {
		return nil, fmt.Errorf("%w: %s", ErrUnclassifiable, q.Name())
	}
	var out []engine.Subject
	for _, fp := range q.Fps {
		if q.Op == runner.OpAlive {
			out = append(out, engine.EntitySubject(fp))
			continue
		}
		for _, s := range w.Incident(fp, q.Dir, q.Layer) {
			ok, err := w.AliveAt(s, q.At)
			if err != nil {
				return nil, err
			}
			if ok {
				out = append(out, s)
			}
		}
	}
	return out, nil
}

// Classify says whether a query is a boundary instant: any subject in the reference
// answer, or any subject the store returned (pass none to classify from the reference
// alone, before any server answer is read), has an existence interval, folded by the
// reference's rule, that starts or ends within the band of the instant. The band is that
// of all the subjects involved together.
func (w *World) Classify(reg *Registry, q runner.Query, returned []engine.Subject) (Class, error) {
	ref, err := w.ReferenceSubjects(q)
	if err != nil {
		return Class{}, err
	}
	seen := map[engine.Subject]struct{}{}
	var involved []engine.Subject
	for _, s := range slices.Concat(ref, returned) {
		if _, dup := seen[s]; !dup {
			seen[s] = struct{}{}
			involved = append(involved, s)
		}
	}
	c := Class{Band: Band(reg, involved), Subjects: len(involved)}
	for _, s := range involved {
		ivs, err := w.Existence(s)
		if err != nil {
			return Class{}, err
		}
		if InBand(ivs, q.At, c.Band) {
			c.Boundary = true
			break
		}
	}
	return c, nil
}
