package subgraph

import (
	"cmp"
	"fmt"
	"slices"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/store"
)

// Build builds the temporal subgraph of an input.
//
// # Elements and the states between them
//
// S(t) is the set of elements with a piece containing t. The quarantine section
// is not part of it: a quarantined entity has no piece, and an uncascaded edge
// is shown with its own uncut pieces but has no deltas (a keyframe holding it
// would hold an edge whose quarantined endpoint is never present). N0 is
// |S(From)|, and E the number of deltas: the piece boundaries strictly inside
// (From, To), which is the sum of the Transitions of the elements in keyframes
// and deltas.
//
// # The keyframe rule
//
// Group the deltas by instant, t1 < ... < tm, with e_i deltas at t_i. The first
// keyframe is at From. After t_i, e_i is added to d, the number of deltas since
// the last keyframe; if d >= max(1, |S(t_i)|), a keyframe of S(t_i) is emitted
// and d is reset. A keyframe is emitted only between instants, so it always
// equals the state at its time, and none is emitted at To.
//
// The rule holds two bounds at once, without looking ahead of t_i. Transfer: a
// later keyframe costs |S(t_i)| <= d, and the later keyframes charge disjoint
// runs of deltas, so together they cost at most E, and [Subgraph.Entries] is at
// most N0 + 2E. The bound is tight: at a constant size N with one element out
// and one in at each instant, it is reached. Seek: for t in [t_i, t_i+1), S(t)
// is S(t_i) and the deltas from the last keyframe number d, which is 0 if a
// keyframe was emitted at t_i and otherwise less than max(1, |S(t)|). A keyframe
// held at N0 would break the transfer bound when the set grows; one held at the
// window's largest size would make a seek cost that much at a small set, and
// needs the whole window in advance.
//
// # Deltas
//
// A delta carries the state before and after: present, or absent with a reason.
// An appearance's before-state is the end reason of the element's previous piece
// in the window, or [EndReasonNotSeenInWindow]; a disappearance's after-state is
// the end reason of the piece. So a delta can be applied forward and inverted.
//
// # Labels
//
// Every element is derived, and the subgraph's basis is [BasisDerived]. An
// element's Subject is asserted for an entity, and for an edge whose relation a
// producer asserts (catalog.StorageFrom); an edge of a derived relation, and a
// derived key, are derived, with the relation's name or the key's rule as the
// element's Rule.
//
// The builder is pure and never truncates: it refuses an input that breaks the
// rules above, with an error wrapping store.ErrInvalid, rather than correcting
// it.
func Build(in Input) (Subgraph, error) {
	w, err := newWindow(in.From, in.To)
	if err != nil {
		return Subgraph{}, err
	}
	cks, err := checkSources(in.Sources, w)
	if err != nil {
		return Subgraph{}, err
	}
	for i, fp := range in.Read {
		if err := checkEntity(fp); err != nil {
			return Subgraph{}, fmt.Errorf("subgraph: read entity %d: %w", i, err)
		}
	}
	sg := Subgraph{
		Basis: BasisDerived, PolicyVersion: PolicyVersion,
		Token: in.Token, From: in.From.UTC(), To: in.To.UTC(),
	}
	var paths [][][]Key
	for _, c := range cks {
		switch {
		case c.quarantine != nil:
			sg.Quarantine.Entities = append(sg.Quarantine.Entities, QuarantinedEntity{Fingerprint: c.el.Key.A, Cause: c.quarantine})
		case c.uncascaded != nil:
			sg.Quarantine.Edges = append(sg.Quarantine.Edges, StaticEdge{Element: c.el, Cause: c.uncascaded})
		case len(c.el.Pieces) > 0:
			sg.Elements = append(sg.Elements, c.el)
			paths = append(paths, c.paths)
		}
	}
	if err := resolvePaths(&sg, paths); err != nil {
		return Subgraph{}, err
	}
	if sg.Sections, err = checkSections(in.Sections, w, sg); err != nil {
		return Subgraph{}, fmt.Errorf("subgraph: sections: %w", err)
	}
	sg.Entities = dictionary(sg, in.Read)
	sg.Deltas = deltas(sg.Elements, w.width())
	sg.Keyframes = keyframes(sg.Elements, sg.Deltas)
	return sg, nil
}

// window is the input's window in Unix nanoseconds.
type window struct{ from, to int64 }

func newWindow(from, to time.Time) (window, error) {
	if !from.Before(to) {
		return window{}, fmt.Errorf("subgraph: the window [%s, %s) is empty: %w", formatTime(from), formatTime(to), store.ErrInvalid)
	}
	if from.Before(store.MinEventTime) || to.After(store.MaxEventTime) {
		return window{}, fmt.Errorf("subgraph: the window [%s, %s) is outside the event times a store holds: %w",
			formatTime(from), formatTime(to), store.ErrInvalid)
	}
	return window{from: from.UnixNano(), to: to.UnixNano()}, nil
}

func (w window) width() uint64 { return uint64(w.to - w.from) }

// rel is t in nanoseconds after From, and whether t is in [From, To].
func (w window) rel(t time.Time) (uint64, bool) {
	if t.Before(store.MinEventTime) || t.After(store.MaxEventTime) {
		return 0, false
	}
	n := t.UnixNano()
	if n < w.from || n > w.to {
		return 0, false
	}
	return uint64(n - w.from), true
}

func formatTime(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

// checked is a source that passed every check, in the output's shape.
type checked struct {
	el                     Element
	paths                  [][]Key
	quarantine, uncascaded *store.QuarantineError
}

func checkSources(srcs []Source, w window) ([]checked, error) {
	out := make([]checked, 0, len(srcs))
	for i, s := range srcs {
		c, err := checkSource(s, w)
		if err != nil {
			return nil, fmt.Errorf("subgraph: source %d (%s): %w", i, s.Key, err)
		}
		out = append(out, c)
	}
	slices.SortFunc(out, func(a, b checked) int { return a.el.Key.Compare(b.el.Key) })
	for i := 1; i < len(out); i++ {
		if out[i-1].el.Key.Compare(out[i].el.Key) == 0 {
			return nil, fmt.Errorf("subgraph: two sources have the key %s: %w", out[i].el.Key, store.ErrInvalid)
		}
	}
	return out, nil
}

func invalid(format string, args ...any) error {
	return fmt.Errorf(format+": %w", append(args, store.ErrInvalid)...)
}

func checkSource(s Source, w window) (checked, error) {
	subject, rule, err := label(s.Key)
	if err != nil {
		return checked{}, err
	}
	if s.Quarantine != nil && s.Uncascaded != nil {
		return checked{}, invalid("both quarantined and uncascaded")
	}
	if s.Quarantine != nil {
		if s.Key.Kind != KindEntity || s.Quarantine.Entity != s.Key.A {
			return checked{}, invalid("a quarantine that is not of the entity the key names")
		}
		if len(s.Pieces) != 0 || s.Class != ClassNone || s.Transitions != 0 {
			return checked{}, invalid("a quarantined entity has no pieces, class or transitions")
		}
	}
	if s.Uncascaded != nil && (s.Key.Kind != KindEdge || (s.Uncascaded.Entity != s.Key.A && s.Uncascaded.Entity != s.Key.B)) {
		return checked{}, invalid("an uncascaded mark that is not of an endpoint of the edge")
	}
	spec, err := pathSpecOf(s)
	if err != nil {
		return checked{}, err
	}
	ps, err := checkPieces(s.Pieces, subject, spec, w)
	if err != nil {
		return checked{}, err
	}
	class, transitions := classify(ps, w.width())
	if s.Class != class || s.Transitions != transitions {
		return checked{}, invalid("the class %s and %d transitions claimed, %s and %d computed from the pieces",
			s.Class, s.Transitions, class, transitions)
	}
	return checked{
		el:         Element{Key: s.Key, Subject: subject, Rule: rule, Class: class, Transitions: transitions, Pieces: ps},
		paths:      spec.paths,
		quarantine: s.Quarantine, uncascaded: s.Uncascaded,
	}, nil
}

func checkEntity(fp identity.Fingerprint) error {
	if fp.IsZero() {
		return invalid("the zero fingerprint")
	}
	if _, ok := catalog.Default().Entity(fp.Type()); !ok {
		return invalid("the unknown entity type %q", fp.Type())
	}
	return nil
}

// label checks a key and gives its subject label and rule.
func label(k Key) (Subject, string, error) {
	cat := catalog.Default()
	known := func(fp identity.Fingerprint) error { return checkEntity(fp) }
	switch k.Kind {
	case KindEntity:
		if err := known(k.A); err != nil {
			return 0, "", err
		}
		if !k.B.IsZero() || k.Relation != "" || k.Rule != "" || k.Detail != "" {
			return 0, "", invalid("an entity key with more than an entity")
		}
		return SubjectAsserted, RuleExistence, nil
	case KindEdge:
		if err := known(k.A); err != nil {
			return 0, "", err
		}
		if err := known(k.B); err != nil {
			return 0, "", err
		}
		if k.Rule != "" || k.Detail != "" {
			return 0, "", invalid("an edge key with a rule or a detail")
		}
		rel, ok := cat.Relation(k.Relation)
		switch {
		case !ok:
			return 0, "", invalid("the relation %q is not registered", k.Relation)
		case !rel.Allows(k.A.Type(), k.B.Type()):
			return 0, "", invalid("the relation %q does not connect %s to %s", k.Relation, k.A.Type(), k.B.Type())
		case rel.Storage() == catalog.StorageFrom:
			return SubjectAsserted, RuleExistence, nil
		case rel.Storage() == catalog.StorageDerived:
			if dk, ok := DerivedKindOf(string(k.Relation)); !ok || !derivedKinds[dk].edge {
				return 0, "", invalid("the derived relation %q has no rule in the derived-kind table", k.Relation)
			}
			return SubjectDerived, string(k.Relation), nil
		}
		return 0, "", invalid("the relation %q has the unknown storage %s", k.Relation, rel.Storage())
	case KindDerived:
		if k.Rule == "" {
			return 0, "", invalid("a derived key without a rule")
		}
		if dk, ok := DerivedKindOf(k.Rule); !ok || derivedKinds[dk].edge {
			return 0, "", invalid("the rule %q is not a derived key rule of the derived-kind table", k.Rule)
		}
		if k.Relation != "" {
			return 0, "", invalid("a derived key with a relation")
		}
		if err := known(k.A); err != nil {
			return 0, "", fmt.Errorf("the first end: %w", err)
		}
		if err := known(k.B); err != nil {
			return 0, "", fmt.Errorf("the second end: %w", err)
		}
		return SubjectDerived, k.Rule, nil
	}
	return 0, "", invalid("the key kind %s", k.Kind)
}

// pathSpec is what a source's pieces are checked against: its legs, and whether
// its rule names the legs that end.
type pathSpec struct {
	paths   [][]Key
	derived bool
	// legEnds says a piece that ends inside the window must list the legs that
	// end there.
	legEnds bool
}

func pathSpecOf(s Source) (pathSpec, error) {
	var spec pathSpec
	if s.Key.Kind == KindDerived {
		spec.derived = true
		dk, _ := DerivedKindOf(s.Key.Rule)
		spec.legEnds = derivedKinds[dk].legEnds
	}
	if len(s.Paths) > 0 && !spec.derived {
		return spec, invalid("legs on a subject that is not a derived key")
	}
	for i, p := range s.Paths {
		if len(p) == 0 {
			return spec, invalid("path %d has no leg", i)
		}
		for j, k := range p {
			if k.Kind != KindEdge {
				return spec, invalid("leg %d of path %d is the %s %s, not an edge", j, i, k.Kind, k)
			}
		}
	}
	spec.paths = s.Paths
	return spec, nil
}

func checkPieces(in []SourcePiece, subject Subject, spec pathSpec, w window) ([]Piece, error) {
	out := make([]Piece, 0, len(in))
	for i, p := range in {
		start, okS := w.rel(p.Start)
		end, okE := w.rel(p.End)
		if !okS || !okE || start >= end || end > w.width() {
			return nil, invalid("piece %d [%s, %s) is not a non-empty interval inside the window", i, formatTime(p.Start), formatTime(p.End))
		}
		if i > 0 && start <= out[i-1].End {
			return nil, invalid("piece %d starts at or before the end of the piece before it", i)
		}
		if p.Before && start != 0 {
			return nil, invalid("piece %d began before From but starts after it", i)
		}
		if !p.EndReason.Valid() {
			return nil, invalid("piece %d has the end reason %s", i, p.EndReason)
		}
		switch {
		case end == w.width():
			if p.EndReason != EndReasonUnknown {
				return nil, invalid("piece %d reaches To and has the end reason %s", i, p.EndReason)
			}
		case subject == SubjectAsserted && !p.EndReason.lifecycleOnly(),
			subject == SubjectDerived && (p.EndReason == EndReasonUnknown || p.EndReason == EndReasonNotSeenInWindow):
			return nil, invalid("piece %d ends inside the window with the end reason %s", i, p.EndReason)
		}
		for k := 1; k < len(p.Producers); k++ {
			if p.Producers[k-1] >= p.Producers[k] {
				return nil, invalid("piece %d producers are not sorted and distinct", i)
			}
		}
		if err := checkEnds(p.Ends, spec, end == w.width()); err != nil {
			return nil, fmt.Errorf("piece %d: %w", i, err)
		}
		out = append(out, Piece{
			Start: start, End: end, Before: p.Before, EndReason: p.EndReason, Producers: cloneProducers(p.Producers),
			Ends: slices.Clone(p.Ends),
		})
	}
	return out, nil
}

// checkEnds checks the leg ends of a piece: they are only of a derived subject,
// ascending, of legs it has, each with a reason a leg can end by; a piece that
// reaches To has none, and one that ends inside the window has some exactly
// when the subject's rule names them.
func checkEnds(ends []LegEnd, spec pathSpec, atTo bool) error {
	switch {
	case len(ends) > 0 && (!spec.derived || atTo || !spec.legEnds):
		return invalid("leg ends on a piece that is not a derived subject's end naming its legs")
	case len(ends) == 0 && spec.derived && spec.legEnds && !atTo && len(spec.paths) > 0:
		return invalid("a derived piece ends inside the window and names no leg that ends")
	}
	for i, e := range ends {
		if e.Path < 0 || e.Path >= len(spec.paths) || e.Leg < 0 || e.Leg >= len(spec.paths[e.Path]) {
			return invalid("leg end %d is of the leg %d of path %d, which does not exist", i, e.Leg, e.Path)
		}
		if i > 0 && (ends[i-1].Path > e.Path || (ends[i-1].Path == e.Path && ends[i-1].Leg >= e.Leg)) {
			return invalid("leg ends are not ascending by path and leg")
		}
		if !e.Reason.lifecycleOnly() && e.Reason != EndReasonUnattributed {
			return invalid("leg end %d has the reason %s", i, e.Reason)
		}
	}
	return nil
}

// resolvePaths gives each derived element its legs as element ids, and checks
// that every leg is an edge that exists in the window and that every piece of
// the element lies within the instants at which all the legs of some path do.
// Only an interval check of the legs makes a derived span true: it is never a
// claim the builder takes on trust.
func resolvePaths(sg *Subgraph, paths [][][]Key) error {
	for i := range sg.Elements {
		el := &sg.Elements[i]
		if len(paths[i]) == 0 {
			continue
		}
		var held [][]span
		for _, p := range paths[i] {
			ids := make([]uint32, len(p))
			cover := []span{{0, ^uint64(0)}}
			for j, k := range p {
				id, ok := sg.ElementID(k)
				if !ok {
					return invalid("subgraph: %s has the leg %s, which is not an element", el.Key, k)
				}
				ids[j] = id
				cover = meet(cover, spansOf(sg.Elements[id].Pieces))
			}
			el.Paths = append(el.Paths, ids)
			held = append(held, cover)
		}
		cover := join(held)
		for j, p := range el.Pieces {
			if !within(cover, span{p.Start, p.End}) {
				return invalid("subgraph: piece %d of %s is not inside the time at which all legs of some path exist", j, el.Key)
			}
		}
	}
	return nil
}

// span is a half-open interval of relative times.
type span struct{ start, end uint64 }

func spansOf(ps []Piece) []span {
	out := make([]span, len(ps))
	for i, p := range ps {
		out[i] = span{p.Start, p.End}
	}
	return out
}

// meet is the intersection of two ascending lists of disjoint spans.
func meet(a, b []span) []span {
	var out []span
	for i, j := 0, 0; i < len(a) && j < len(b); {
		lo, hi := max(a[i].start, b[j].start), min(a[i].end, b[j].end)
		if lo < hi {
			out = append(out, span{lo, hi})
		}
		if a[i].end < b[j].end {
			i++
		} else {
			j++
		}
	}
	return out
}

// join is the union of lists of spans, as ascending disjoint spans that do not
// touch.
func join(lists [][]span) []span {
	var all []span
	for _, l := range lists {
		all = append(all, l...)
	}
	slices.SortFunc(all, func(a, b span) int { return cmp.Compare(a.start, b.start) })
	var out []span
	for _, s := range all {
		if n := len(out); n > 0 && s.start <= out[n-1].end {
			out[n-1].end = max(out[n-1].end, s.end)
			continue
		}
		out = append(out, s)
	}
	return out
}

// within reports whether s lies inside one span of the list.
func within(list []span, s span) bool {
	return slices.ContainsFunc(list, func(c span) bool { return c.start <= s.start && s.end <= c.end })
}

// classify gives the class of pieces in a window of the given width, and its
// transitions: the piece boundaries strictly inside the window.
func classify(ps []Piece, width uint64) (Class, int) {
	n := 0
	for _, p := range ps {
		if p.Start > 0 {
			n++
		}
		if p.End < width {
			n++
		}
	}
	switch len(ps) {
	case 0:
		return ClassNone, 0
	case 1:
		atFrom, atTo := ps[0].Start == 0, ps[0].End == width
		switch {
		case atFrom && atTo:
			return ClassStable, n
		case atTo:
			return ClassAppeared, n
		case atFrom:
			return ClassDisappeared, n
		}
		return ClassTransient, n
	}
	return ClassFlapping, n
}

// dictionary lists every entity a key names, with the boundary mark.
func dictionary(sg Subgraph, read []identity.Fingerprint) []Entity {
	// existence is true for an entity with an element, a quarantine entry, or a
	// read: whatever the subgraph knows of it beyond an edge naming it.
	type mark struct{ existence bool }
	seen := map[identity.Fingerprint]*mark{}
	note := func(fp identity.Fingerprint, existence bool) {
		m := seen[fp]
		if m == nil {
			m = &mark{}
			seen[fp] = m
		}
		m.existence = m.existence || existence
	}
	for _, el := range sg.Elements {
		switch el.Key.Kind {
		case KindEntity:
			note(el.Key.A, true)
		case KindEdge, KindDerived:
			note(el.Key.A, false)
			note(el.Key.B, false)
		}
	}
	for _, fp := range sg.Sections.entities() {
		note(fp, false)
	}
	for _, q := range sg.Quarantine.Entities {
		note(q.Fingerprint, true)
	}
	for _, e := range sg.Quarantine.Edges {
		note(e.Key.A, false)
		note(e.Key.B, false)
	}
	for _, fp := range read {
		if seen[fp] != nil {
			seen[fp].existence = true
		}
	}
	out := make([]Entity, 0, len(seen))
	for fp, m := range seen {
		out = append(out, Entity{Fingerprint: fp, Boundary: !m.existence})
	}
	slices.SortFunc(out, func(a, b Entity) int { return store.CompareFingerprints(a.Fingerprint, b.Fingerprint) })
	return out
}

// deltas lists every piece boundary strictly inside the window, ordered by time,
// then element id, then appearance before disappearance.
func deltas(els []Element, width uint64) []Delta {
	var out []Delta
	for id, el := range els {
		for i, p := range el.Pieces {
			if p.Start > 0 {
				before := State{Reason: EndReasonNotSeenInWindow}
				if i > 0 {
					before.Reason = el.Pieces[i-1].EndReason
				}
				out = append(out, Delta{At: p.Start, Element: uint32(id), Before: before, After: State{Present: true}})
			}
			if p.End < width {
				out = append(out, Delta{At: p.End, Element: uint32(id), Before: State{Present: true}, After: State{Reason: p.EndReason}})
			}
		}
	}
	slices.SortFunc(out, func(a, b Delta) int {
		if c := cmp.Compare(a.At, b.At); c != 0 {
			return c
		}
		if c := cmp.Compare(a.Element, b.Element); c != 0 {
			return c
		}
		switch {
		case a.Appear() && !b.Appear():
			return -1
		case !a.Appear() && b.Appear():
			return 1
		}
		return 0
	})
	return out
}

// keyframes applies the keyframe rule to the deltas of els.
func keyframes(els []Element, ds []Delta) []Keyframe {
	var present []uint32
	for id, el := range els {
		if el.Pieces[0].Start == 0 {
			present = append(present, uint32(id))
		}
	}
	out := []Keyframe{{At: 0, Elements: slices.Clone(present)}}
	d := 0
	for i := 0; i < len(ds); {
		at := ds[i].At
		for ; i < len(ds) && ds[i].At == at; i++ {
			d++
			j, found := slices.BinarySearch(present, ds[i].Element)
			switch {
			case ds[i].Appear() && !found:
				present = slices.Insert(present, j, ds[i].Element)
			case !ds[i].Appear() && found:
				present = slices.Delete(present, j, j+1)
			}
		}
		if d >= max(1, len(present)) {
			out = append(out, Keyframe{At: at, Elements: slices.Clone(present)})
			d = 0
		}
	}
	return out
}
