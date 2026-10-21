package subgraph

import (
	"fmt"
	"time"

	"github.com/lotannauo/toposhift/internal/identity"
)

// Dropped counts what [Without] took out of a subgraph, one count per list.
type Dropped struct {
	// Entities, Edges and Derived are the elements taken out, by the kind of
	// their key: an edge of a derived relation counts as an edge, and a derived
	// subject counts once whether its key named a dropped entity or one of its
	// legs was taken out.
	Entities, Edges, Derived int
	// QuarantinedEntities and QuarantinedEdges are the entries of the static
	// section taken out.
	QuarantinedEntities, QuarantinedEdges int
}

// Without is sg without the elements that name an entity drop reports, built
// again by [Build] so that the ids, the keyframes, the deltas and both bounds
// hold for what is left. It is a pure function of sg and drop: it reads
// nothing, writes nothing, and changes no policy version.
//
// It turns sg back into the [Input] that builds it, with every time made
// absolute again, and takes out of that input:
//
//   - every element whose key names a dropped entity, as the entity, an edge's
//     source or target, or a derived subject's end;
//   - every derived element one of whose legs was taken out: a derived span is
//     true only while its legs exist, so it is never kept without them;
//   - every quarantined entity drop reports, and every uncascaded edge with a
//     dropped endpoint;
//   - every dropped entity from the entities read, so a dropped entity leaves
//     the dictionary unless a section names it.
//
// The result is what [Build] makes of that input. For a subgraph [Build] made,
// it equals the subgraph built from the same input with the same things taken
// out, and dropping nothing returns a subgraph equal to sg. Each thing taken
// out is counted in the [Dropped] returned; none is taken out silently. The
// result shares no slice with sg; a quarantine's collision is shared, as
// [Build] shares it with its input.
//
// The sections are carried as they are: no section is cut. A section that
// names a dropped entity keeps that entity in the dictionary, so a caller that
// must not show a dropped entity checks the dictionary of the result and
// refuses it whole. A pod section that lists as quarantined an edge taken out
// no longer matches the static section, and [Build] refuses the input, so
// Without does too.
//
// drop is called with every entity a key or a read names and must answer the
// same for the same entity. A nil drop is refused. A subgraph that [Build]
// could not have made, of another policy version or with a leg, a piece or a
// static entry that does not resolve, is refused with an error wrapping
// store.ErrInvalid, as is anything [Build] refuses.
func Without(sg Subgraph, drop func(identity.Fingerprint) bool) (Subgraph, Dropped, error) {
	if drop == nil {
		return Subgraph{}, Dropped{}, invalid("subgraph: without: no function says which entities to drop")
	}
	in, err := inverse(sg)
	if err != nil {
		return Subgraph{}, Dropped{}, fmt.Errorf("subgraph: without: %w", err)
	}
	in, dropped := withoutEntities(in, drop)
	out, err := Build(in)
	if err != nil {
		return Subgraph{}, Dropped{}, err
	}
	return out, dropped, nil
}

// inverse is the input that builds sg: every element, quarantined entity and
// uncascaded edge as a source, with its pieces at absolute times and its legs
// as keys; every entity of the dictionary that is not a boundary as read; and
// the sections as they are. The keyframes, the deltas, the labels and the
// dictionary, apart from which of its entries are boundaries, are not read:
// [Build] computes them again from the rest.
func inverse(sg Subgraph) (Input, error) {
	switch {
	case sg.Basis != BasisDerived:
		return Input{}, invalid("a subgraph whose basis is %s", sg.Basis)
	case sg.PolicyVersion != PolicyVersion:
		return Input{}, invalid("a subgraph of policy version %d, and this package builds version %d", sg.PolicyVersion, PolicyVersion)
	}
	w, err := newWindow(sg.From, sg.To)
	if err != nil {
		return Input{}, err
	}
	in := Input{
		Token: sg.Token, From: sg.From, To: sg.To,
		Sources:  make([]Source, 0, len(sg.Elements)+len(sg.Quarantine.Entities)+len(sg.Quarantine.Edges)),
		Sections: sg.Sections,
	}
	for i, el := range sg.Elements {
		s, err := elementSource(sg, w, el)
		if err != nil {
			return Input{}, fmt.Errorf("element %d (%s): %w", i, el.Key, err)
		}
		in.Sources = append(in.Sources, s)
	}
	for i, q := range sg.Quarantine.Entities {
		if q.Cause == nil {
			return Input{}, invalid("quarantined entity %d (%s) has no collision", i, q.Fingerprint)
		}
		in.Sources = append(in.Sources, Source{Key: EntityKey(q.Fingerprint), Class: ClassNone, Quarantine: q.Cause})
	}
	for i, e := range sg.Quarantine.Edges {
		if e.Cause == nil {
			return Input{}, invalid("uncascaded edge %d (%s) has no collision", i, e.Key)
		}
		s, err := elementSource(sg, w, e.Element)
		if err != nil {
			return Input{}, fmt.Errorf("uncascaded edge %d (%s): %w", i, e.Key, err)
		}
		s.Uncascaded = e.Cause
		in.Sources = append(in.Sources, s)
	}
	for _, e := range sg.Entities {
		if !e.Boundary {
			in.Read = append(in.Read, e.Fingerprint)
		}
	}
	return in, nil
}

// elementSource is the source of one element of sg: its pieces at absolute
// times, and its legs as the keys of the elements they name.
func elementSource(sg Subgraph, w window, el Element) (Source, error) {
	s := Source{Key: el.Key, Class: el.Class, Transitions: el.Transitions}
	if len(el.Pieces) > 0 {
		s.Pieces = make([]SourcePiece, len(el.Pieces))
	}
	abs := func(rel uint64) time.Time { return time.Unix(0, w.from+int64(rel)).UTC() }
	for i, p := range el.Pieces {
		if p.Start >= p.End || p.End > w.width() {
			return Source{}, invalid("piece %d [%d, %d) is not a non-empty interval inside the window of %d ns", i, p.Start, p.End, w.width())
		}
		s.Pieces[i] = SourcePiece{
			Start: abs(p.Start), End: abs(p.End),
			Before: p.Before, EndReason: p.EndReason, Producers: p.Producers, Ends: p.Ends,
		}
	}
	for i, path := range el.Paths {
		keys := make([]Key, len(path))
		for j, id := range path {
			if int64(id) >= int64(len(sg.Elements)) {
				return Source{}, invalid("leg %d of path %d is the element %d of %d", j, i, id, len(sg.Elements))
			}
			keys[j] = sg.Elements[id].Key
		}
		s.Paths = append(s.Paths, keys)
	}
	return s, nil
}

// withoutEntities takes out of in every source whose key names a dropped
// entity, then every derived source with a leg taken out, and every dropped
// read. A leg is always an edge and an edge has no legs, so one pass over the
// derived sources after the first finds every source a removal reaches.
func withoutEntities(in Input, drop func(identity.Fingerprint) bool) (Input, Dropped) {
	names := func(k Key) bool { return drop(k.A) || (!k.B.IsZero() && drop(k.B)) }
	removed := make(map[Key]bool)
	for _, s := range in.Sources {
		if names(s.Key) {
			removed[s.Key] = true
		}
	}
	for _, s := range in.Sources {
		if s.Key.Kind != KindDerived || removed[s.Key] {
			continue
		}
	paths:
		for _, path := range s.Paths {
			for _, leg := range path {
				if removed[leg] {
					removed[s.Key] = true
					break paths
				}
			}
		}
	}
	var d Dropped
	out := in
	out.Sources = make([]Source, 0, len(in.Sources))
	for _, s := range in.Sources {
		if !removed[s.Key] {
			out.Sources = append(out.Sources, s)
			continue
		}
		switch {
		case s.Quarantine != nil:
			d.QuarantinedEntities++
		case s.Uncascaded != nil:
			d.QuarantinedEdges++
		case s.Key.Kind == KindEntity:
			d.Entities++
		case s.Key.Kind == KindEdge:
			d.Edges++
		default:
			d.Derived++
		}
	}
	out.Read = nil
	for _, fp := range in.Read {
		if !drop(fp) {
			out.Read = append(out.Read, fp)
		}
	}
	return out, d
}
