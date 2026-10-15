package wire

import (
	"cmp"
	"slices"
	"time"
	"unicode/utf8"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/query/subgraph"
	"github.com/lotannauo/toposhift/internal/store"
)

// validate checks the invariants the format relies on, and no others. Both
// directions use it: the encoder refuses what it returns an error for, and the
// decoder refuses the same document, so no document is encodable and not
// decodable, or the other way round. It checks structure: the order of every
// list, the ranges of every number and reference, and the labels a key
// determines. It checks that the deltas and the keyframes say what the pieces
// say, which is the format's meaning of them. It does not check where the
// keyframes are placed, or the transfer and seek bounds: those are the builder's
// rule under the policy version the document names, and a reader of a stored
// document of an older policy must still read it.
func validate(d *Document) error {
	sg := &d.Subgraph
	if sg.Basis != subgraph.BasisDerived {
		return invalid("the basis %s: a subgraph is derived", sg.Basis)
	}
	if sg.PolicyVersion < 0 {
		return invalid("a negative subgraph policy version")
	}
	if !inEventTimes(sg.From) || !inEventTimes(sg.To) || !sg.From.Before(sg.To) {
		return invalid("the window [%s, %s) is empty or outside the event times a store holds", sg.From.UTC(), sg.To.UTC())
	}
	c := checker{d: d, width: uint64(sg.To.UnixNano() - sg.From.UnixNano())}
	if err := c.policy(); err != nil {
		return err
	}
	if err := c.query(); err != nil {
		return err
	}
	if err := c.dictionary(); err != nil {
		return err
	}
	if err := c.elements(sg.Elements, true); err != nil {
		return err
	}
	if err := c.keyframes(); err != nil {
		return err
	}
	if err := c.deltas(); err != nil {
		return err
	}
	if err := c.replay(); err != nil {
		return err
	}
	if err := c.quarantine(); err != nil {
		return err
	}
	return c.sections()
}

func inEventTimes(t time.Time) bool {
	return !t.Before(store.MinEventTime) && !t.After(store.MaxEventTime)
}

type checker struct {
	d     *Document
	width uint64
}

func (c *checker) dictID(fp identity.Fingerprint) (uint32, bool) {
	return c.d.Subgraph.EntityID(fp)
}

// ref checks that fp is in the dictionary.
func (c *checker) ref(fp identity.Fingerprint) error {
	if _, ok := c.dictID(fp); !ok {
		return invalid("the entity %s is not in the dictionary", fp)
	}
	return nil
}

func (c *checker) policy() error {
	p := c.d.Policy
	for _, v := range []int{p.Interval, p.CoResidency, p.Extract, p.BlastRadius, p.GraphDiff, p.IdentityEncoding} {
		if v < 0 {
			return invalid("a negative policy version")
		}
	}
	if n := len(p.Rank); n != 0 && n != len(p.CatalogIDs) {
		return invalid("a rank digest of %d bytes", n)
	}
	return nil
}

func (c *checker) query() error {
	q := c.d.Query
	if !q.Kind.Valid() {
		return invalid("the query kind %s", q.Kind)
	}
	for i, fp := range q.Seeds {
		if err := knownEntity(fp); err != nil {
			return invalid("seed %d: %v", i, err)
		}
	}
	for i, o := range q.Options {
		if o.Name == "" || !utf8.ValidString(o.Name) {
			return invalid("option %d has the name %q", i, o.Name)
		}
		if i > 0 && q.Options[i-1].Name >= o.Name {
			return invalid("options are not ascending by name, or repeat one")
		}
	}
	return nil
}

// knownEntity checks that fp is not zero and its type is in the catalog, which
// is what an id on the wire stands for.
func knownEntity(fp identity.Fingerprint) error {
	if fp.IsZero() {
		return invalid("the zero fingerprint")
	}
	if _, ok := catalog.Default().Entity(fp.Type()); !ok {
		return unknownType(fp.Type())
	}
	return nil
}

func (c *checker) dictionary() error {
	es := c.d.Subgraph.Entities
	for i, e := range es {
		if err := knownEntity(e.Fingerprint); err != nil {
			return invalid("dictionary entry %d: %v", i, err)
		}
		if i > 0 && store.CompareFingerprints(es[i-1].Fingerprint, e.Fingerprint) >= 0 {
			return invalid("dictionary entry %d is not after the entry before it", i)
		}
	}
	return nil
}

// label gives the subject and the rule a key determines, and checks the key:
// the same rules as the builder's, which this package cannot call.
func (c *checker) label(k subgraph.Key) (subgraph.Subject, string, error) {
	cat := catalog.Default()
	switch k.Kind {
	case subgraph.KindEntity:
		if !k.B.IsZero() || k.Relation != "" || k.Rule != "" || k.Detail != "" {
			return 0, "", invalid("an entity key with more than an entity")
		}
		if err := c.ref(k.A); err != nil {
			return 0, "", err
		}
		return subgraph.SubjectAsserted, subgraph.RuleExistence, nil
	case subgraph.KindEdge:
		if k.Rule != "" || k.Detail != "" {
			return 0, "", invalid("an edge key with a rule or a detail")
		}
		if err := c.ref(k.A); err != nil {
			return 0, "", err
		}
		if err := c.ref(k.B); err != nil {
			return 0, "", err
		}
		rel, ok := cat.Relation(k.Relation)
		switch {
		case !ok:
			return 0, "", invalid("the relation %q is not registered", k.Relation)
		case !rel.Allows(k.A.Type(), k.B.Type()):
			return 0, "", invalid("the relation %q does not connect %s to %s", k.Relation, k.A.Type(), k.B.Type())
		case rel.Storage() == catalog.StorageFrom:
			return subgraph.SubjectAsserted, subgraph.RuleExistence, nil
		case rel.Storage() == catalog.StorageDerived:
			if dk, ok := subgraph.DerivedKindOf(string(k.Relation)); !ok || dk != subgraph.DerivedSameAs {
				return 0, "", invalid("the derived relation %q has no rule in the derived-kind table", k.Relation)
			}
			return subgraph.SubjectDerived, string(k.Relation), nil
		}
		return 0, "", invalid("the relation %q has the unknown storage %s", k.Relation, rel.Storage())
	case subgraph.KindDerived:
		dk, ok := subgraph.DerivedKindOf(k.Rule)
		if !ok || dk == subgraph.DerivedSameAs {
			return 0, "", invalid("the rule %q is not a derived key rule of the derived-kind table", k.Rule)
		}
		if k.Relation != "" {
			return 0, "", invalid("a derived key with a relation")
		}
		if !utf8.ValidString(k.Detail) {
			return 0, "", invalid("a derived key whose detail is not UTF-8")
		}
		if err := c.ref(k.A); err != nil {
			return 0, "", err
		}
		if err := c.ref(k.B); err != nil {
			return 0, "", err
		}
		return subgraph.SubjectDerived, k.Rule, nil
	}
	return 0, "", invalid("the key kind %s", k.Kind)
}

// classify is the class of pieces in a window of the given width and its
// transitions: the piece boundaries strictly inside the window.
func classify(ps []subgraph.Piece, width uint64) (subgraph.Class, int) {
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
		return subgraph.ClassNone, 0
	case 1:
		atFrom, atTo := ps[0].Start == 0, ps[0].End == width
		switch {
		case atFrom && atTo:
			return subgraph.ClassStable, n
		case atTo:
			return subgraph.ClassAppeared, n
		case atFrom:
			return subgraph.ClassDisappeared, n
		}
		return subgraph.ClassTransient, n
	}
	return subgraph.ClassFlapping, n
}

// elements checks a list of elements: the main list, or the static edges of the
// quarantine section, which may have no piece.
func (c *checker) elements(els []subgraph.Element, main bool) error {
	for i := range els {
		el := &els[i]
		if i > 0 && els[i-1].Key.Compare(el.Key) >= 0 {
			return invalid("element %d is not after the element before it", i)
		}
		if !main && el.Key.Kind != subgraph.KindEdge {
			return invalid("static element %d is not an edge", i)
		}
		subject, rule, err := c.label(el.Key)
		if err != nil {
			return invalid("element %d (%s): %v", i, el.Key, err)
		}
		if el.Subject != subject || el.Rule != rule {
			return invalid("element %d (%s) is labelled %s %q and its key makes it %s %q", i, el.Key, el.Subject, el.Rule, subject, rule)
		}
		if main && len(el.Pieces) == 0 {
			return invalid("element %d has no piece", i)
		}
		if err := c.pieces(el, subject); err != nil {
			return invalid("element %d (%s): %v", i, el.Key, err)
		}
		class, transitions := classify(el.Pieces, c.width)
		if el.Class != class || el.Transitions != transitions {
			return invalid("element %d claims the class %s and %d transitions, its pieces make %s and %d",
				i, el.Class, el.Transitions, class, transitions)
		}
		if err := c.paths(el); err != nil {
			return invalid("element %d (%s): %v", i, el.Key, err)
		}
	}
	return nil
}

func (c *checker) paths(el *subgraph.Element) error {
	if len(el.Paths) > 0 && el.Key.Kind != subgraph.KindDerived {
		return invalid("legs on a subject that is not a derived key")
	}
	els := c.d.Subgraph.Elements
	for i, p := range el.Paths {
		if len(p) == 0 {
			return invalid("path %d has no leg", i)
		}
		for j, id := range p {
			if int64(id) >= int64(len(els)) || els[id].Key.Kind != subgraph.KindEdge {
				return invalid("leg %d of path %d is not an edge element", j, i)
			}
		}
	}
	for i, p := range el.Pieces {
		for k, e := range p.Ends {
			if e.Path < 0 || e.Path >= len(el.Paths) || e.Leg < 0 || e.Leg >= len(el.Paths[e.Path]) {
				return invalid("piece %d leg end %d names a leg that does not exist", i, k)
			}
			if k > 0 && (p.Ends[k-1].Path > e.Path || (p.Ends[k-1].Path == e.Path && p.Ends[k-1].Leg >= e.Leg)) {
				return invalid("piece %d leg ends are not ascending by path and leg", i)
			}
			if !lifecycleReason(e.Reason) && e.Reason != subgraph.EndReasonUnattributed {
				return invalid("piece %d leg end %d has the reason %s", i, k, e.Reason)
			}
		}
	}
	return nil
}

// lifecycleReason says r is one of the lifecycle's own end sources.
func lifecycleReason(r subgraph.EndReason) bool {
	return r >= subgraph.EndReasonProducer && r <= subgraph.EndReasonOperator
}

func (c *checker) pieces(el *subgraph.Element, subject subgraph.Subject) error {
	for i, p := range el.Pieces {
		if p.Start >= p.End || p.End > c.width {
			return invalid("piece %d [%d, %d) is not a non-empty interval inside the window", i, p.Start, p.End)
		}
		if i > 0 && p.Start <= el.Pieces[i-1].End {
			return invalid("piece %d starts at or before the end of the piece before it", i)
		}
		if p.Before && p.Start != 0 {
			return invalid("piece %d began before From but starts after it", i)
		}
		if !p.EndReason.Valid() {
			return invalid("piece %d has the end reason %s", i, p.EndReason)
		}
		switch {
		case p.End == c.width:
			if p.EndReason != subgraph.EndReasonUnknown {
				return invalid("piece %d reaches To and has the end reason %s", i, p.EndReason)
			}
		case p.EndReason == subgraph.EndReasonUnknown || p.EndReason == subgraph.EndReasonNotSeenInWindow,
			subject == subgraph.SubjectAsserted && !lifecycleReason(p.EndReason):
			return invalid("piece %d ends inside the window with the end reason %s", i, p.EndReason)
		}
		for k, pr := range p.Producers {
			if !utf8.ValidString(string(pr)) {
				return invalid("piece %d has a producer that is not UTF-8", i)
			}
			if k > 0 && p.Producers[k-1] >= pr {
				return invalid("piece %d producers are not sorted and distinct", i)
			}
		}
		if len(p.Ends) > 0 && (p.End == c.width || el.Key.Kind != subgraph.KindDerived) {
			return invalid("piece %d has leg ends and is not a derived subject's end inside the window", i)
		}
	}
	return nil
}

func (c *checker) keyframes() error {
	sg := &c.d.Subgraph
	kfs := sg.Keyframes
	if len(kfs) == 0 || kfs[0].At != 0 {
		return invalid("the keyframes do not start with one at From")
	}
	n := int64(len(sg.Elements))
	for i, k := range kfs {
		if i > 0 && (k.At <= kfs[i-1].At || k.At >= c.width) {
			return invalid("keyframe %d at %d is not after the keyframe before it and inside the window", i, k.At)
		}
		for j, id := range k.Elements {
			if int64(id) >= n {
				return invalid("keyframe %d lists the element %d of %d", i, id, n)
			}
			if j > 0 && k.Elements[j-1] >= id {
				return invalid("keyframe %d elements are not ascending", i)
			}
		}
	}
	return nil
}

func (c *checker) deltas() error {
	sg := &c.d.Subgraph
	n := int64(len(sg.Elements))
	for i, d := range sg.Deltas {
		if d.At == 0 || d.At >= c.width {
			return invalid("delta %d at %d is not strictly inside the window", i, d.At)
		}
		if int64(d.Element) >= n {
			return invalid("delta %d names the element %d of %d", i, d.Element, n)
		}
		if i > 0 {
			p := sg.Deltas[i-1]
			if d.At < p.At || (d.At == p.At && d.Element <= p.Element) {
				return invalid("delta %d is not after the delta before it by time and element", i)
			}
		}
		if d.Before.Present == d.After.Present {
			return invalid("delta %d is neither an appearance nor a disappearance", i)
		}
		for _, s := range []subgraph.State{d.Before, d.After} {
			switch {
			case s.Present && s.Reason != subgraph.EndReasonUnknown:
				return invalid("delta %d has a present state with the reason %s", i, s.Reason)
			case !s.Present && (!s.Reason.Valid() || s.Reason == subgraph.EndReasonUnknown):
				return invalid("delta %d has an absent state with the reason %s", i, s.Reason)
			}
		}
		if !d.Appear() && d.After.Reason == subgraph.EndReasonNotSeenInWindow {
			return invalid("delta %d is a disappearance that was not seen", i)
		}
	}
	return nil
}

// replay checks the deltas and the keyframes against the elements' own pieces.
// These are what the format means by them, not a policy: a delta is a piece
// boundary strictly inside the window with the states on its two sides, and a
// keyframe is the set of elements present at its time. Where a keyframe is
// placed, and how many there are, is the builder's policy and is not checked.
// The structure of both has been checked.
func (c *checker) replay() error {
	sg := &c.d.Subgraph
	want := boundaries(sg.Elements, c.width)
	if len(want) != len(sg.Deltas) {
		return invalid("the pieces have %d boundaries inside the window and the document has %d deltas", len(want), len(sg.Deltas))
	}
	for i, d := range sg.Deltas {
		if d != want[i] {
			return invalid("delta %d is %+v and the pieces make %+v", i, d, want[i])
		}
	}
	present := make([]bool, len(sg.Elements))
	n := 0
	for i, el := range sg.Elements {
		if el.Pieces[0].Start == 0 {
			present[i] = true
			n++
		}
	}
	next := 0
	for i, k := range sg.Keyframes {
		for ; next < len(sg.Deltas) && sg.Deltas[next].At <= k.At; next++ {
			d := sg.Deltas[next]
			present[d.Element] = d.Appear()
			if d.Appear() {
				n++
			} else {
				n--
			}
		}
		if len(k.Elements) != n {
			return invalid("keyframe %d at %d lists %d elements and %d are present", i, k.At, len(k.Elements), n)
		}
		for _, id := range k.Elements {
			if !present[id] {
				return invalid("keyframe %d at %d lists the element %d, which is not present then", i, k.At, id)
			}
		}
	}
	return nil
}

// boundaries lists the deltas the pieces make, in the order of the format: by
// time, then element, then appearance before disappearance.
func boundaries(els []subgraph.Element, width uint64) []subgraph.Delta {
	var out []subgraph.Delta
	for id, el := range els {
		for i, p := range el.Pieces {
			if p.Start > 0 {
				before := subgraph.State{Reason: subgraph.EndReasonNotSeenInWindow}
				if i > 0 {
					before.Reason = el.Pieces[i-1].EndReason
				}
				out = append(out, subgraph.Delta{At: p.Start, Element: uint32(id), Before: before, After: subgraph.State{Present: true}})
			}
			if p.End < width {
				out = append(out, subgraph.Delta{At: p.End, Element: uint32(id), Before: subgraph.State{Present: true}, After: subgraph.State{Reason: p.EndReason}})
			}
		}
	}
	slices.SortFunc(out, func(a, b subgraph.Delta) int {
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

func (c *checker) quarantine() error {
	q := c.d.Subgraph.Quarantine
	for i, e := range q.Entities {
		if err := c.ref(e.Fingerprint); err != nil {
			return invalid("quarantined entity %d: %v", i, err)
		}
		if i > 0 && store.CompareFingerprints(q.Entities[i-1].Fingerprint, e.Fingerprint) >= 0 {
			return invalid("quarantined entity %d is not after the one before it", i)
		}
		if e.Cause == nil || e.Cause.Entity != e.Fingerprint {
			return invalid("quarantined entity %d has no collision record of its own", i)
		}
		if err := checkCause(e.Cause); err != nil {
			return invalid("quarantined entity %d: %v", i, err)
		}
	}
	els := make([]subgraph.Element, len(q.Edges))
	for i, e := range q.Edges {
		els[i] = e.Element
	}
	if err := c.elements(els, false); err != nil {
		return invalid("static edges: %v", err)
	}
	for i, e := range q.Edges {
		if e.Cause == nil || (e.Cause.Entity != e.Key.A && e.Cause.Entity != e.Key.B) {
			return invalid("static edge %d has a cause that is not of one of its endpoints", i)
		}
		if err := checkCause(e.Cause); err != nil {
			return invalid("static edge %d: %v", i, err)
		}
		if _, ok := c.d.Subgraph.ElementID(e.Key); ok {
			return invalid("static edge %d is also an element", i)
		}
	}
	return nil
}

func validLayer(l catalog.Layer) bool { return l >= catalog.L0 && l <= catalog.L3 }

// checkCause checks a quarantine's layer and collision. The entity is checked
// by the caller.
func checkCause(q *store.QuarantineError) error {
	if !validLayer(q.Layer) {
		return invalid("the layer %d", uint8(q.Layer))
	}
	if cc := q.Collision; cc != nil {
		if !utf8.ValidString(cc.StaleBoot) || !utf8.ValidString(cc.NewerBoot) {
			return invalid("a boot id that is not UTF-8")
		}
		if !inEventTimes(cc.ObservedAt) || !inEventTimes(cc.NewerFirstSeen) {
			return invalid("a collision time outside the event times a store holds")
		}
	}
	return nil
}

func (c *checker) truncation(t subgraph.Truncation) error {
	for _, fps := range [][]identity.Fingerprint{t.Nodes, t.Hosts, t.Members, t.Instances, t.SeedPods, t.Depth} {
		for _, fp := range fps {
			if err := c.ref(fp); err != nil {
				return err
			}
		}
	}
	for _, f := range t.Frontier {
		if f.Depth < 1 || len(f.Dropped) == 0 {
			return invalid("a frontier cut at depth %d with %d entities", f.Depth, len(f.Dropped))
		}
		for _, fp := range f.Dropped {
			if err := c.ref(fp); err != nil {
				return err
			}
		}
	}
	for _, v := range t.PerVia {
		if err := c.ref(v.Via); err != nil {
			return err
		}
		if !v.Mechanism.Valid() || v.Dropped < 1 {
			return invalid("a per-via count of %d for the mechanism %s", v.Dropped, v.Mechanism)
		}
	}
	if t.Results < 0 || t.Pairs < 0 || t.MembersDropped < 0 || t.SuppressedDropped < 0 {
		return invalid("a negative count of what was left out")
	}
	return nil
}

func (c *checker) spans(ss []subgraph.Span) error {
	if len(ss) == 0 {
		return invalid("a section entry without a span")
	}
	for i, p := range ss {
		if p.Start >= p.End || p.End > c.width {
			return invalid("span %d [%d, %d) is not a non-empty interval inside the window", i, p.Start, p.End)
		}
		if i > 0 && p.Start <= ss[i-1].End {
			return invalid("span %d starts at or before the end of the span before it", i)
		}
		if p.Before && p.Start != 0 {
			return invalid("span %d began before From but starts after it", i)
		}
		if (p.End == c.width) != (p.EndReason == subgraph.EndReasonUnknown) ||
			(p.End < c.width && p.EndReason != subgraph.EndReasonReachEnded) {
			return invalid("span %d ends at %d of %d with the end reason %s", i, p.End, c.width, p.EndReason)
		}
	}
	return nil
}

func (c *checker) suppressed(x subgraph.Suppressed) error {
	if x.Key.Kind != subgraph.KindDerived {
		return invalid("a suppressed entry with the key %s", x.Key)
	}
	if _, _, err := c.label(x.Key); err != nil {
		return err
	}
	if !x.Reason.Valid() {
		return invalid("a suppressed entry with the reason %s", x.Reason)
	}
	if x.Reason != subgraph.SuppressedUbiquitous && x.Coverage != [2]int{} {
		return invalid("a suppressed entry with a coverage and the reason %s", x.Reason)
	}
	if x.Coverage[0] < 0 || x.Coverage[1] < 0 {
		return invalid("a negative coverage")
	}
	if err := c.spans(x.Spans); err != nil {
		return err
	}
	ps := make([]subgraph.Piece, len(x.Spans))
	for i, s := range x.Spans {
		ps[i] = subgraph.Piece{Start: s.Start, End: s.End}
	}
	if class, transitions := classify(ps, c.width); x.Class != class || x.Transitions != transitions {
		return invalid("the class %s and %d transitions claimed, %s and %d computed from the spans", x.Class, x.Transitions, class, transitions)
	}
	return nil
}

func (c *checker) sections() error {
	s := c.d.Subgraph.Sections
	if err := c.truncation(s.Truncated); err != nil {
		return invalid("truncated: %v", err)
	}
	for i, u := range s.Unattributed {
		if err := c.ref(u.Subject); err != nil {
			return invalid("unattributed %d: %v", i, err)
		}
		if !u.SeedPod.IsZero() {
			if err := c.ref(u.SeedPod); err != nil {
				return invalid("unattributed %d: %v", i, err)
			}
		}
		if !u.Reason.Valid() {
			return invalid("unattributed %d has the reason %s", i, u.Reason)
		}
		if u.Reason == subgraph.UnattributedSeveralClaims {
			if _, ok := catalog.Default().Relation(u.Relation); !ok {
				return invalid("unattributed %d claims through the relation %q, which is not registered", i, u.Relation)
			}
			if _, ok := catalog.Default().Entity(u.Target); !ok {
				return invalid("unattributed %d claims targets of the unknown type %q", i, u.Target)
			}
		} else if u.Relation != "" || u.Target != "" {
			return invalid("unattributed %d has a relation or a target and the reason %s", i, u.Reason)
		}
		for _, fp := range u.Candidates {
			if err := c.ref(fp); err != nil {
				return invalid("unattributed %d: %v", i, err)
			}
		}
		if err := c.spans(u.Spans); err != nil {
			return invalid("unattributed %d: %v", i, err)
		}
	}
	for i, u := range s.Unmapped {
		if err := c.ref(u.Pod); err != nil {
			return invalid("unmapped %d: %v", i, err)
		}
		if err := c.spans(u.Spans); err != nil {
			return invalid("unmapped %d: %v", i, err)
		}
	}
	for i, x := range s.Suppressed {
		if err := c.suppressed(x); err != nil {
			return invalid("suppressed %d: %v", i, err)
		}
	}
	if !s.Anchor.Valid() {
		return invalid("the anchor state %s", s.Anchor)
	}
	if s.Ubiquity.FrontierNodes < 0 {
		return invalid("a ubiquity frontier of %d nodes", s.Ubiquity.FrontierNodes)
	}
	quarantined := map[subgraph.Key]bool{}
	for _, q := range c.d.Subgraph.Quarantine.Entities {
		quarantined[subgraph.EntityKey(q.Fingerprint)] = true
	}
	for _, e := range c.d.Subgraph.Quarantine.Edges {
		quarantined[e.Key] = true
	}
	seeds := map[identity.Fingerprint]bool{}
	for i, p := range s.Pods {
		if err := c.ref(p.Seed); err != nil {
			return invalid("pod section %d: %v", i, err)
		}
		if seeds[p.Seed] {
			return invalid("two pod sections of the seed %s", p.Seed)
		}
		seeds[p.Seed] = true
		if err := c.truncation(p.Truncated); err != nil {
			return invalid("pod section %d: %v", i, err)
		}
		for _, k := range p.Quarantined {
			if !quarantined[k] {
				return invalid("pod section %d lists %s as quarantined, and the quarantine section lacks it", i, k)
			}
		}
		for j, x := range p.Suppressed {
			if err := c.suppressed(x); err != nil {
				return invalid("pod section %d suppressed %d: %v", i, j, err)
			}
		}
	}
	return nil
}

func unknownType(t catalog.EntityType) error { return invalid("the unknown entity type %q", t) }
