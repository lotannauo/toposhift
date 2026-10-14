package interval

import (
	"fmt"
	"slices"
	"time"

	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

// ival is an unclipped existence interval: [start, end), or [start, +inf) when
// open, ended by src.
type ival struct {
	start, end time.Time
	open       bool
	src        lifecycle.EndSource
}

// meets reports whether the interval has an instant in [from, to).
func (a ival) meets(from, to time.Time) bool {
	return a.start.Before(to) && (a.open || a.end.After(from))
}

// endsBy reports whether a ends at or before b: an open interval ends after
// every finite one.
func (a ival) endsBy(b ival) bool {
	switch {
	case b.open:
		return true
	case a.open:
		return false
	}
	return !a.end.After(b.end)
}

// folded is the existence of one subject, and of each of its producers alone.
type folded struct {
	ivs        []ival
	producers  []lifecycle.Producer // sorted
	byProducer [][]ival             // parallel to producers
}

// foldSubject folds the records of one subject with the zero policy:
// existence does not depend on the policy, and quarantine is the store's to
// report. The records must all be of one subject.
func foldSubject(recs []store.Record) (folded, error) {
	var f folded
	ivs, err := foldRecords(recs)
	if err != nil {
		return folded{}, err
	}
	f.ivs = ivs
	groups := map[lifecycle.Producer][]store.Record{}
	for _, r := range recs {
		if _, ok := groups[r.Producer]; !ok {
			f.producers = append(f.producers, r.Producer)
		}
		groups[r.Producer] = append(groups[r.Producer], r)
	}
	slices.Sort(f.producers)
	f.byProducer = make([][]ival, len(f.producers))
	for i, p := range f.producers {
		if f.byProducer[i], err = foldRecords(groups[p]); err != nil {
			return folded{}, err
		}
	}
	return f, nil
}

// foldRecords folds records into existence intervals, refusing a finite
// interval with no end source: the lifecycle uses EndUnknown both for "open" and
// for "unknown", and a finite piece with no source would hide which.
func foldRecords(recs []store.Record) ([]ival, error) {
	if len(recs) == 0 {
		return nil, nil
	}
	as := make([]lifecycle.Assertion, len(recs))
	for i, r := range recs {
		as[i] = r.Assertion()
	}
	tl, err := lifecycle.Fold(as, lifecycle.Policy{})
	if err != nil {
		return nil, fmt.Errorf("folding %s: %w", describeSubject(recs[0].Subject), err)
	}
	ex := tl.Existence()
	out := make([]ival, len(ex))
	for i, iv := range ex {
		if !iv.Open() && iv.EndSource == lifecycle.EndUnknown {
			return nil, fmt.Errorf("folding %s: the interval [%s, %s) is finite and has no end source",
				describeSubject(recs[0].Subject), formatTime(iv.Start), formatTime(iv.End))
		}
		out[i] = ival{start: iv.Start, end: iv.End, open: iv.Open(), src: iv.EndSource}
	}
	return out, nil
}

// cut intersects an edge's intervals with one endpoint's existence. A cut
// interval ends where the edge's own does, with the edge's own source, when
// that is at or before the endpoint's end (the edge's source wins a tie);
// otherwise it ends where the endpoint's does, by cascade. Both lists are
// ascending and disjoint; so is the result.
func cut(edge, end []ival) []ival {
	var out []ival
	i, j := 0, 0
	for i < len(edge) && j < len(end) {
		e, n := edge[i], end[j]
		iv := e
		if n.start.After(iv.start) {
			iv.start = n.start
		}
		if !e.endsBy(n) {
			iv.end, iv.open, iv.src = n.end, n.open, lifecycle.EndCascade
		}
		if iv.open || iv.start.Before(iv.end) {
			out = append(out, iv)
		}
		switch {
		case e.endsBy(n) && n.endsBy(e):
			i++
			j++
		case e.endsBy(n):
			i++
		default:
			j++
		}
	}
	return out
}

// clip clips unclipped intervals to [from, to). A piece that reaches to ends
// there with no end source, whatever is known after it. A finite interval that
// ends inside the window with no end source is refused.
func clip(ivs []ival, from, to time.Time) ([]Piece, error) {
	var out []Piece
	for _, iv := range ivs {
		if !iv.meets(from, to) {
			continue
		}
		p := Piece{Span: Span{Start: iv.start, End: to}, Before: iv.start.Before(from)}
		if p.Before {
			p.Start = from
		}
		if !iv.open && iv.end.Before(to) {
			if iv.src == lifecycle.EndUnknown {
				return nil, fmt.Errorf("the interval [%s, %s) ends inside the window with no end source",
					formatTime(iv.start), formatTime(iv.end))
			}
			p.End, p.EndSource = iv.end, iv.src
		}
		out = append(out, p)
	}
	return out, nil
}

// attribute sets each piece's producers: those with an interval of their own
// overlapping the piece.
func attribute(ps []Piece, f folded) {
	for k := range ps {
		p := &ps[k]
		for i, prod := range f.producers {
			for _, iv := range f.byProducer[i] {
				if iv.start.Before(p.End) && (iv.open || p.Start.Before(iv.end)) {
					p.Producers = append(p.Producers, prod)
					break
				}
			}
		}
	}
}

// classTable is the class of one piece, by whether it starts at From and
// whether it ends at To.
var classTable = [2][2]Class{
	// [startsAtFrom][endsAtTo]
	{ClassTransient, ClassAppeared},
	{ClassDisappeared, ClassStable},
}

// classify gives the class of an element's pieces in [from, to), and its
// transitions: the piece boundaries strictly inside the window.
func classify(ps []Piece, from, to time.Time) (Class, int) {
	n := 0
	for _, p := range ps {
		if p.Start.After(from) {
			n++
		}
		if p.End.Before(to) {
			n++
		}
	}
	switch len(ps) {
	case 0:
		return ClassNone, 0
	case 1:
		return classTable[b2i(ps[0].Start.Equal(from))][b2i(ps[0].End.Equal(to))], n
	}
	return ClassFlapping, n
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

func describeSubject(s store.Subject) string {
	if s.Kind == store.SubjectEntity {
		return "entity " + s.A.String()
	}
	return fmt.Sprintf("edge %s -[%s]-> %s", s.A, s.Relation, s.B)
}
