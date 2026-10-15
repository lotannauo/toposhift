package blastscore

import (
	"cmp"
	"slices"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/query/blastradius"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
)

// projectPods is W->pod: the evaluation's projection of the walk's service
// members onto pods. It is an evaluation rule, not a product rule; the walk
// itself stops at instances (instance_of propagates nothing).
//
// For a service member M, a pod P is named where interval.Along of M's reach,
// an instance I's instance_of edge to M, and I's part_of edge to P is not
// empty. Each leg is read with the cascade at the walk's token. An instance
// counts only where it maps singly, by the rule of package extract: over the
// instants it is part of two pods at once it names neither (several pods), and
// over those it is an instance of two services at once it names neither
// (several services). A claim whose other end is quarantined is never followed
// but still counts as a claim. Every pod is given the hop of the nearest member
// that names it, and the list is ranked by hop, then by the time the projection
// holds (longest first), then by fingerprint.
func (x *runner) projectPods(walked blastradius.Result) ([]Prediction, error) {
	members := map[identity.Fingerprint]blastradius.Member{}
	var svcs []identity.Fingerprint
	for _, m := range walked.Members {
		if m.Entity.Type() == catalog.Service {
			members[m.Entity] = m
			svcs = append(svcs, m.Entity)
		}
	}
	if len(svcs) == 0 {
		return nil, nil
	}

	// The instances of the members: their instance_of edges, read from the service.
	_, per, err := x.r.EdgesBatch(x.ctx, svcs, store.Reverse, x.w)
	if err != nil {
		return nil, err
	}
	var insts []identity.Fingerprint
	seen := map[identity.Fingerprint]struct{}{}
	for _, els := range per {
		for _, el := range els {
			if el.Subject.Relation != catalog.InstanceOf || el.Subject.A.Type() != catalog.ServiceInstance {
				continue
			}
			if _, ok := seen[el.Subject.A]; !ok {
				seen[el.Subject.A] = struct{}{}
				insts = append(insts, el.Subject.A)
			}
		}
	}
	if len(insts) == 0 {
		return nil, nil
	}
	slices.SortFunc(insts, store.CompareFingerprints)

	// What each instance claims, read from the instance.
	_, out, err := x.r.EdgesBatch(x.ctx, insts, store.Forward, x.w)
	if err != nil {
		return nil, err
	}
	type reach struct {
		hop int
		dur time.Duration
	}
	best := map[identity.Fingerprint]reach{}
	for i := range insts {
		var svcEls, podEls []interval.Element
		for _, el := range out[i] {
			switch {
			case el.Subject.Relation == catalog.InstanceOf && el.Subject.B.Type() == catalog.Service:
				svcEls = append(svcEls, el)
			case el.Subject.Relation == catalog.PartOf && el.Subject.B.Type() == catalog.K8sPod:
				podEls = append(podEls, el)
			}
		}
		svcEff := x.claims(svcEls)
		podEff := x.claims(podEls)
		for _, s := range sortedFPs(svcEff) {
			m, ok := members[s]
			if !ok {
				continue
			}
			for _, p := range sortedFPs(podEff) {
				d := interval.Duration(interval.Along(m.Reach, svcEff[s], podEff[p]))
				if d <= 0 {
					continue
				}
				r, ok := best[p]
				switch {
				case !ok:
					best[p] = reach{hop: m.Depth, dur: d}
				case m.Depth < r.hop:
					best[p] = reach{hop: m.Depth, dur: d}
				case m.Depth == r.hop:
					best[p] = reach{hop: r.hop, dur: max(r.dur, d)}
				}
			}
		}
	}

	pods := make([]identity.Fingerprint, 0, len(best))
	for p := range best {
		pods = append(pods, p)
	}
	slices.SortFunc(pods, func(a, b identity.Fingerprint) int {
		if c := cmp.Compare(best[a].hop, best[b].hop); c != 0 {
			return c
		}
		if c := cmp.Compare(best[b].dur, best[a].dur); c != 0 {
			return c
		}
		return store.CompareFingerprints(a, b)
	})
	list := make([]Prediction, len(pods))
	for i, p := range pods {
		list[i] = Prediction{Entity: p, Hop: best[p].hop}
	}
	return list, nil
}

// claims derives one relation's claims of an instance: for each target, the
// instants the instance claims it and no other target of the relation. An
// instance that claims several targets at once is unattributed over the overlap,
// and each target it claims there is recorded for the counter. A claim whose
// target is quarantined is never followed (it has no effective spans), but it
// still counts against the others.
func (x *runner) claims(els []interval.Element) map[identity.Fingerprint][]interval.Span {
	eff := map[identity.Fingerprint][]interval.Span{}
	for i, el := range els {
		var others [][]interval.Span
		for j, o := range els {
			if j != i {
				others = append(others, interval.Spans(o))
			}
		}
		rest := unionSpans(others...)
		mine := interval.Spans(el)
		if len(interval.Intersect(mine, rest)) > 0 {
			x.unattributed[el.Subject.B] = struct{}{}
		}
		if el.Uncascaded != nil {
			continue
		}
		eff[el.Subject.B] = subtractSpans(mine, rest)
	}
	return eff
}

func sortedFPs(m map[identity.Fingerprint][]interval.Span) []identity.Fingerprint {
	out := make([]identity.Fingerprint, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.SortFunc(out, store.CompareFingerprints)
	return out
}

// unionSpans is the instants in any of the lists, as a sorted list of disjoint,
// non-touching spans.
func unionSpans(lists ...[]interval.Span) []interval.Span {
	var all []interval.Span
	for _, l := range lists {
		all = append(all, l...)
	}
	slices.SortFunc(all, func(a, b interval.Span) int { return a.Start.Compare(b.Start) })
	var out []interval.Span
	for _, s := range all {
		if n := len(out); n > 0 && !s.Start.After(out[n-1].End) {
			if s.End.After(out[n-1].End) {
				out[n-1].End = s.End
			}
			continue
		}
		out = append(out, s)
	}
	return out
}

// subtractSpans is the instants of a that are not in b, both sorted lists of
// disjoint spans.
func subtractSpans(a, b []interval.Span) []interval.Span {
	var out []interval.Span
	j := 0
	for _, s := range a {
		cur := s
		for j < len(b) && !b[j].End.After(cur.Start) {
			j++
		}
		k := j
		for k < len(b) && b[k].Start.Before(cur.End) {
			if b[k].Start.After(cur.Start) {
				out = append(out, interval.Span{Start: cur.Start, End: b[k].Start})
			}
			if !b[k].End.Before(cur.End) {
				cur.Start = cur.End
				break
			}
			cur.Start = b[k].End
			k++
		}
		if cur.Start.Before(cur.End) {
			out = append(out, cur)
		}
	}
	return out
}
