package interval_test

import (
	"cmp"
	"slices"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
)

// The oracle answers range reads straight from the definitions, by probing
// liveness at every instant where it can change. It shares no code with
// lifecycle.Fold or with the package.

// oracle holds the records a token sees, by subject, and the entities the store
// quarantines at that token (quarantine is the store's to decide).
type oracle struct {
	bySub       map[store.Subject][]store.Record
	quarantined map[identity.Fingerprint]*store.QuarantineError
}

// newOracle keeps the records with Seq at or below token: all of them,
// whatever their event time, so the oracle also shows that records at or after
// To change nothing.
func newOracle(recs []store.Record, token uint64) *oracle {
	o := &oracle{bySub: map[store.Subject][]store.Record{}, quarantined: map[identity.Fingerprint]*store.QuarantineError{}}
	for _, r := range recs {
		if r.Seq <= token {
			o.bySub[r.Subject] = append(o.bySub[r.Subject], r)
		}
	}
	return o
}

// deadline is when an Observe with a TTL lapses: its last observation plus the
// TTL.
func deadline(r store.Record) (time.Time, bool) {
	if r.Kind != lifecycle.Observe || r.TTL == 0 {
		return time.Time{}, false
	}
	last := r.EventTime
	if r.Through.After(last) {
		last = r.Through
	}
	return last.Add(r.TTL), true
}

// latest is each producer's assertion in force at x: of those with
// EventTime <= x, the one at the latest instant, and of those, the highest Seq.
func latest(recs []store.Record, x time.Time) map[lifecycle.Producer]store.Record {
	out := map[lifecycle.Producer]store.Record{}
	for _, r := range recs {
		if r.EventTime.After(x) {
			continue
		}
		cur, ok := out[r.Producer]
		if !ok || r.EventTime.After(cur.EventTime) || (r.EventTime.Equal(cur.EventTime) && r.Seq > cur.Seq) {
			out[r.Producer] = r
		}
	}
	return out
}

// liveAt says whether the subject exists at x, and which producers' references
// are live there, sorted.
func liveAt(recs []store.Record, x time.Time) (bool, []lifecycle.Producer) {
	var ps []lifecycle.Producer
	for p, r := range latest(recs, x) {
		if r.Kind != lifecycle.Observe {
			continue
		}
		if d, ok := deadline(r); ok && !x.Before(d) {
			continue
		}
		ps = append(ps, p)
	}
	slices.Sort(ps)
	return len(ps) > 0, ps
}

// critical lists From, and every record event time and deadline strictly
// inside (From, To) of the given records, ascending and once each.
func critical(lists [][]store.Record, from, to time.Time) []time.Time {
	out := []time.Time{from}
	in := func(x time.Time) bool { return x.After(from) && x.Before(to) }
	for _, recs := range lists {
		for _, r := range recs {
			if in(r.EventTime) {
				out = append(out, r.EventTime)
			}
			if d, ok := deadline(r); ok && in(d) {
				out = append(out, d)
			}
		}
	}
	slices.SortFunc(out, func(a, b time.Time) int { return a.Compare(b) })
	return slices.CompactFunc(out, func(a, b time.Time) bool { return a.Equal(b) })
}

// endCause is why the subject stopped at x, given the producers live at the
// last critical instant before it: producer if that producer's assertion in
// force at x is a Delete at x, else expiry; the strongest wins.
func endCause(recs []store.Record, x time.Time, live []lifecycle.Producer) lifecycle.EndSource {
	in := latest(recs, x)
	src := lifecycle.EndLivenessExpiry
	for _, p := range live {
		if r, ok := in[p]; ok && r.Kind == lifecycle.Delete && r.EventTime.Equal(x) {
			src = lifecycle.EndProducer
		}
	}
	return src
}

// pieces probes own (and, for the cascade, the endpoints in ends) at every
// critical instant of all of them.
func pieces(own []store.Record, ends [][]store.Record, from, to time.Time) []interval.Piece {
	alive := func(x time.Time) bool {
		if ok, _ := liveAt(own, x); !ok {
			return false
		}
		for _, e := range ends {
			if ok, _ := liveAt(e, x); !ok {
				return false
			}
		}
		return true
	}
	crit := critical(append([][]store.Record{own}, ends...), from, to)
	var out []interval.Piece
	var cur *interval.Piece
	var last time.Time
	for _, x := range crit {
		if alive(x) {
			if cur == nil {
				cur = &interval.Piece{Span: interval.Span{Start: x}}
				cur.Before = x.Equal(from) && alive(from.Add(-time.Nanosecond))
			}
			_, ps := liveAt(own, x)
			for _, p := range ps {
				if !slices.Contains(cur.Producers, p) {
					cur.Producers = append(cur.Producers, p)
				}
			}
			last = x
			continue
		}
		if cur == nil {
			continue
		}
		cur.End = x
		if ok, _ := liveAt(own, x); ok {
			cur.EndSource = lifecycle.EndCascade
		} else {
			_, ps := liveAt(own, last)
			cur.EndSource = endCause(own, x, ps)
		}
		slices.Sort(cur.Producers)
		out = append(out, *cur)
		cur = nil
	}
	if cur != nil {
		cur.End = to
		slices.Sort(cur.Producers)
		out = append(out, *cur)
	}
	return out
}

// classOf is the class table and the transition count, from their definitions.
func classOf(ps []interval.Piece, from, to time.Time) (interval.Class, int) {
	n := 0
	for _, p := range ps {
		if p.Start.After(from) && p.Start.Before(to) {
			n++
		}
		if p.End.After(from) && p.End.Before(to) {
			n++
		}
	}
	k := len(ps)
	if k == 0 {
		return interval.ClassNone, n
	}
	if k >= 2 {
		return interval.ClassFlapping, n
	}
	a, b := ps[0].Start.Equal(from), ps[k-1].End.Equal(to)
	switch {
	case a && b:
		return interval.ClassStable, n
	case b:
		return interval.ClassAppeared, n
	case a:
		return interval.ClassDisappeared, n
	}
	return interval.ClassTransient, n
}

func (o *oracle) element(sub store.Subject, layer catalog.Layer, ps []interval.Piece, from, to time.Time) interval.Element {
	el := interval.Element{Subject: sub, Layer: layer, Pieces: ps}
	el.Class, el.Transitions = classOf(ps, from, to)
	return el
}

// entity is what Entity must answer.
func (o *oracle) entity(fp identity.Fingerprint, from, to time.Time) interval.Element {
	sub := store.EntitySubject(fp)
	if q := o.quarantined[fp]; q != nil {
		return interval.Element{Subject: sub, Layer: layerOf(sub), Quarantine: q}
	}
	return o.element(sub, layerOf(sub), pieces(o.bySub[sub], nil, from, to), from, to)
}

// edges is what Edges must answer, raw or with the cascade.
func (o *oracle) edges(fp identity.Fingerprint, dir store.Direction, from, to time.Time, raw bool) []interval.Element {
	var out []interval.Element
	for sub, own := range o.bySub {
		if sub.Kind != store.SubjectEdge || (dir == store.Forward && sub.A != fp) || (dir == store.Reverse && sub.B != fp) {
			continue
		}
		var ps []interval.Piece
		var unc *store.QuarantineError
		switch {
		case raw:
			ps = pieces(own, nil, from, to)
		case o.quarantined[sub.A] != nil || o.quarantined[sub.B] != nil:
			unc = o.quarantined[sub.A]
			if unc == nil {
				unc = o.quarantined[sub.B]
			}
			ps = pieces(own, nil, from, to)
		default:
			ps = pieces(own, [][]store.Record{o.bySub[store.EntitySubject(sub.A)], o.bySub[store.EntitySubject(sub.B)]}, from, to)
		}
		if len(ps) == 0 {
			continue
		}
		el := o.element(sub, layerOf(sub), ps, from, to)
		el.Uncascaded = unc
		out = append(out, el)
	}
	peer := func(s store.Subject) identity.Fingerprint {
		if dir == store.Forward {
			return s.B
		}
		return s.A
	}
	slices.SortFunc(out, func(a, b interval.Element) int {
		if c := store.CompareFingerprints(peer(a.Subject), peer(b.Subject)); c != 0 {
			return c
		}
		return cmp.Compare(a.Subject.Relation, b.Subject.Relation)
	})
	return out
}

// allCritical is every critical instant of every subject in [from, to).
func (o *oracle) allCritical(from, to time.Time) []time.Time {
	var lists [][]store.Record
	for _, recs := range o.bySub {
		lists = append(lists, recs)
	}
	return critical(lists, from, to)
}
