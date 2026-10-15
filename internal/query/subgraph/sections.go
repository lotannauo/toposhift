package subgraph

import (
	"fmt"
	"slices"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
)

// Sections hold what a query reports that is not an element: the cuts its
// policy made, the placements and mappings it refused to guess, the entries it
// suppressed, and what each pod-level search reported. The builder never
// truncates or drops any of it: a section is carried as the adapter gives it,
// after the checks below, so a client can say what is missing instead of
// presenting a partial answer as whole. Every time in a section is a count of
// nanoseconds after From, and every entity a section names is in the
// dictionary.
type Sections struct {
	// Truncated is what the query's caps left out.
	Truncated Truncation
	// Unattributed lists, in the order the query gave them, the placements,
	// mappings and claims it refused to guess.
	Unattributed []Unattributed
	// Unmapped lists the pods with no instance mapped singly to a service.
	Unmapped []Unmapped
	// Suppressed lists the entries shown and not counted among the elements.
	Suppressed []Suppressed
	// Pods lists what each pod-level search of an extraction reported, in the
	// order they ran.
	Pods []PodSection
	// Anchor is what an anchored extraction found on its host.
	Anchor AnchorState
	// Ubiquity says whether the ubiquity rule was judged.
	Ubiquity Ubiquity
}

// Truncation reports what a query's caps left out. A field the query does not
// have is zero.
type Truncation struct {
	// Nodes and Hosts were not read: over the caps of a co-residency search.
	Nodes, Hosts []identity.Fingerprint
	// Members, Instances and SeedPods were not used by an extraction.
	Members, Instances, SeedPods []identity.Fingerprint
	// Depth names the members of a blast-radius walk reached at the depth limit
	// that were not expanded.
	Depth []identity.Fingerprint
	// Frontier lists, by depth, the entities a walk's frontier cap left
	// unexpanded.
	Frontier []FrontierCut
	// PerVia counts the entries a co-residency search dropped for a via and
	// mechanism.
	PerVia []ViaCount
	// Results counts the co-residents a search dropped, Pairs the pairs an
	// extraction dropped, MembersDropped the members a walk left out of its
	// list, and SuppressedDropped the suppressed entries dropped.
	Results, Pairs, MembersDropped, SuppressedDropped int
}

// IsZero reports whether nothing was left out.
func (t Truncation) IsZero() bool {
	return len(t.Nodes) == 0 && len(t.Hosts) == 0 && len(t.Members) == 0 && len(t.Instances) == 0 &&
		len(t.SeedPods) == 0 && len(t.Depth) == 0 && len(t.Frontier) == 0 && len(t.PerVia) == 0 &&
		t.Results == 0 && t.Pairs == 0 && t.MembersDropped == 0 && t.SuppressedDropped == 0
}

// FrontierCut names the entities reached at Depth that a frontier cap left
// unexpanded.
type FrontierCut struct {
	Depth   int
	Dropped []identity.Fingerprint
}

// ViaCount counts the entries of one via and mechanism a per-via cap dropped.
type ViaCount struct {
	Via       identity.Fingerprint
	Mechanism Mechanism
	Dropped   int
}

// Span is an interval of a section, in nanoseconds after From. EndReason is
// [EndReasonUnknown] when the span reaches the window's end and otherwise
// [EndReasonReachEnded]: the span ends where an ambiguity or an intersection
// does, and nothing is inferred about why.
type Span struct {
	Start, End uint64
	// Before says the span starts at From and began before it.
	Before    bool
	EndReason EndReason
}

// Unattributed is a placement, mapping or claim the query refused to guess,
// over some spans. It is derived, and its rule is "unattributed." followed by
// the reason.
type Unattributed struct {
	// SeedPod is the pod whose search reported it; zero when the query itself did.
	SeedPod identity.Fingerprint
	Subject identity.Fingerprint
	Reason  UnattributedReason
	// Relation and Target are set for [UnattributedSeveralClaims] only: the
	// relation and the entity type of the targets claimed.
	Relation catalog.RelationType
	Target   catalog.EntityType
	Spans    []Span
	// Candidates are the entities claimed over the spans.
	Candidates []identity.Fingerprint
}

// Unmapped is a pod with no instance mapped singly to a service over some
// spans of its co-residency.
type Unmapped struct {
	Pod   identity.Fingerprint
	Spans []Span
}

// Suppressed is an entry the query suppressed and shows here, never dropped. It
// is not an element: it is in no keyframe or delta. It carries its key, why it
// was suppressed and, for a pair judged ubiquitous, the coverage of its peer;
// its legs are not carried.
type Suppressed struct {
	Key    Key
	Reason SuppressReason
	// Coverage is (nodes covered, frontier nodes) of the pair's peer service
	// when ubiquity was judged, and zero otherwise.
	Coverage    [2]int
	Class       Class
	Transitions int
	Spans       []Span
}

// PodSection is what one pod-level search of an extraction reported that the
// extraction's own lists lack.
type PodSection struct {
	// Seed is the pod the search started from.
	Seed identity.Fingerprint
	// Truncated is what the search's caps left out.
	Truncated Truncation
	// Quarantined are the keys of the edges the search read and did not use
	// because an endpoint is quarantined. Each is in the quarantine section.
	Quarantined []Key
	// Suppressed are the search's own suppressed entries.
	Suppressed []Suppressed
}

// Ubiquity says whether the ubiquity rule was judged, and over how many nodes.
type Ubiquity struct {
	Judged        bool
	FrontierNodes int
}

// entities lists, once each, every entity the sections name.
func (s Sections) entities() []identity.Fingerprint {
	var out []identity.Fingerprint
	add := func(fps ...identity.Fingerprint) { out = append(out, fps...) }
	truncation := func(t Truncation) {
		add(t.Nodes...)
		add(t.Hosts...)
		add(t.Members...)
		add(t.Instances...)
		add(t.SeedPods...)
		add(t.Depth...)
		for _, c := range t.Frontier {
			add(c.Dropped...)
		}
		for _, v := range t.PerVia {
			add(v.Via)
		}
	}
	suppressed := func(ss []Suppressed) {
		for _, x := range ss {
			add(x.Key.A, x.Key.B)
		}
	}
	truncation(s.Truncated)
	for _, u := range s.Unattributed {
		if !u.SeedPod.IsZero() {
			add(u.SeedPod)
		}
		add(u.Subject)
		add(u.Candidates...)
	}
	for _, u := range s.Unmapped {
		add(u.Pod)
	}
	suppressed(s.Suppressed)
	for _, p := range s.Pods {
		add(p.Seed)
		truncation(p.Truncated)
		suppressed(p.Suppressed)
		for _, k := range p.Quarantined {
			add(k.A)
			if !k.B.IsZero() {
				add(k.B)
			}
		}
	}
	return out
}

// checkSections checks the sections against the window and the built
// subgraph's quarantine section and elements, and returns them as they are
// carried.
func checkSections(s Sections, w window, sg Subgraph) (Sections, error) {
	width := w.width()
	checkTruncation := func(t Truncation) error {
		for _, fps := range [][]identity.Fingerprint{t.Nodes, t.Hosts, t.Members, t.Instances, t.SeedPods, t.Depth} {
			for _, fp := range fps {
				if err := checkEntity(fp); err != nil {
					return err
				}
			}
		}
		for _, c := range t.Frontier {
			if c.Depth < 1 || len(c.Dropped) == 0 {
				return invalid("a frontier cut at depth %d with %d entities", c.Depth, len(c.Dropped))
			}
			for _, fp := range c.Dropped {
				if err := checkEntity(fp); err != nil {
					return err
				}
			}
		}
		for _, v := range t.PerVia {
			if err := checkEntity(v.Via); err != nil {
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
	checkSpans := func(ss []Span) error {
		if len(ss) == 0 {
			return invalid("a section entry without a span")
		}
		for i, p := range ss {
			if p.Start >= p.End || p.End > width {
				return invalid("span %d [%d, %d) is not a non-empty interval inside the window", i, p.Start, p.End)
			}
			if i > 0 && p.Start <= ss[i-1].End {
				return invalid("span %d starts at or before the end of the span before it", i)
			}
			if p.Before && p.Start != 0 {
				return invalid("span %d began before From but starts after it", i)
			}
			if (p.End == width && p.EndReason != EndReasonUnknown) || (p.End < width && p.EndReason != EndReasonReachEnded) {
				return invalid("span %d ends at %d of %d with the end reason %s", i, p.End, width, p.EndReason)
			}
		}
		return nil
	}
	checkSuppressed := func(x Suppressed) error {
		if x.Key.Kind != KindDerived {
			return invalid("a suppressed entry with the key %s", x.Key)
		}
		if _, _, err := label(x.Key); err != nil {
			return err
		}
		if !x.Reason.Valid() {
			return invalid("a suppressed entry with the reason %s", x.Reason)
		}
		if x.Reason != SuppressedUbiquitous && x.Coverage != [2]int{} {
			return invalid("a suppressed entry with a coverage and the reason %s", x.Reason)
		}
		if x.Coverage[0] < 0 || x.Coverage[1] < 0 || x.Coverage[0] > x.Coverage[1] {
			return invalid("a coverage of %v", x.Coverage)
		}
		class, transitions := classifySpans(x.Spans, width)
		if x.Class != class || x.Transitions != transitions {
			return invalid("the class %s and %d transitions claimed, %s and %d computed from the spans", x.Class, x.Transitions, class, transitions)
		}
		return checkSpans(x.Spans)
	}
	quarantined := map[Key]bool{}
	for _, q := range sg.Quarantine.Entities {
		quarantined[EntityKey(q.Fingerprint)] = true
	}
	for _, e := range sg.Quarantine.Edges {
		quarantined[e.Key] = true
	}

	if err := checkTruncation(s.Truncated); err != nil {
		return Sections{}, err
	}
	for i, u := range s.Unattributed {
		if err := checkEntity(u.Subject); err != nil {
			return Sections{}, fmt.Errorf("unattributed %d: %w", i, err)
		}
		if !u.SeedPod.IsZero() {
			if err := checkEntity(u.SeedPod); err != nil {
				return Sections{}, fmt.Errorf("unattributed %d: %w", i, err)
			}
		}
		if !u.Reason.Valid() {
			return Sections{}, invalid("unattributed %d has the reason %s", i, u.Reason)
		}
		claims := u.Reason == UnattributedSeveralClaims
		if _, ok := catalog.Default().Relation(u.Relation); claims && !ok {
			return Sections{}, invalid("unattributed %d claims through the relation %q, which is not registered", i, u.Relation)
		}
		if _, ok := catalog.Default().Entity(u.Target); claims && !ok {
			return Sections{}, invalid("unattributed %d claims targets of the unknown type %q", i, u.Target)
		}
		if !claims && (u.Relation != "" || u.Target != "") {
			return Sections{}, invalid("unattributed %d has a relation or a target and the reason %s", i, u.Reason)
		}
		for _, fp := range u.Candidates {
			if err := checkEntity(fp); err != nil {
				return Sections{}, fmt.Errorf("unattributed %d: %w", i, err)
			}
		}
		if err := checkSpans(u.Spans); err != nil {
			return Sections{}, fmt.Errorf("unattributed %d: %w", i, err)
		}
	}
	for i, u := range s.Unmapped {
		if err := checkEntity(u.Pod); err != nil {
			return Sections{}, fmt.Errorf("unmapped %d: %w", i, err)
		}
		if err := checkSpans(u.Spans); err != nil {
			return Sections{}, fmt.Errorf("unmapped %d: %w", i, err)
		}
	}
	seen := map[Key]bool{}
	for _, el := range sg.Elements {
		seen[el.Key] = true
	}
	for i, x := range s.Suppressed {
		if err := checkSuppressed(x); err != nil {
			return Sections{}, fmt.Errorf("suppressed %d (%s): %w", i, x.Key, err)
		}
		if seen[x.Key] {
			return Sections{}, invalid("suppressed %d has the key %s of another entry", i, x.Key)
		}
		seen[x.Key] = true
	}
	switch {
	case !s.Anchor.Valid():
		return Sections{}, invalid("the anchor state %s", s.Anchor)
	case s.Ubiquity.FrontierNodes < 0:
		return Sections{}, invalid("a ubiquity frontier of %d nodes", s.Ubiquity.FrontierNodes)
	}
	podSeeds := map[identity.Fingerprint]bool{}
	for i, p := range s.Pods {
		if err := checkEntity(p.Seed); err != nil {
			return Sections{}, fmt.Errorf("pod section %d: %w", i, err)
		}
		if podSeeds[p.Seed] {
			return Sections{}, invalid("two pod sections of the seed %s", p.Seed)
		}
		podSeeds[p.Seed] = true
		if err := checkTruncation(p.Truncated); err != nil {
			return Sections{}, fmt.Errorf("pod section %d: %w", i, err)
		}
		for _, k := range p.Quarantined {
			if !quarantined[k] {
				return Sections{}, invalid("pod section %d lists %s as quarantined, and the quarantine section lacks it", i, k)
			}
		}
		for j, x := range p.Suppressed {
			if err := checkSuppressed(x); err != nil {
				return Sections{}, fmt.Errorf("pod section %d suppressed %d (%s): %w", i, j, x.Key, err)
			}
			if seen[x.Key] {
				return Sections{}, invalid("pod section %d suppressed %d has the key %s of another entry", i, j, x.Key)
			}
			seen[x.Key] = true
		}
	}
	return cloneSections(s), nil
}

// classifySpans is the class of spans in a window of the given width and its
// transitions, as [classify] gives them for pieces.
func classifySpans(ss []Span, width uint64) (Class, int) {
	ps := make([]Piece, len(ss))
	for i, s := range ss {
		ps[i] = Piece{Start: s.Start, End: s.End}
	}
	return classify(ps, width)
}

// cloneSections copies every slice, so the subgraph shares nothing with the
// input.
func cloneSections(s Sections) Sections {
	s.Truncated = cloneTruncation(s.Truncated)
	s.Unattributed = slices.Clone(s.Unattributed)
	for i := range s.Unattributed {
		s.Unattributed[i].Spans = slices.Clone(s.Unattributed[i].Spans)
		s.Unattributed[i].Candidates = slices.Clone(s.Unattributed[i].Candidates)
	}
	s.Unmapped = slices.Clone(s.Unmapped)
	for i := range s.Unmapped {
		s.Unmapped[i].Spans = slices.Clone(s.Unmapped[i].Spans)
	}
	s.Suppressed = cloneSuppressed(s.Suppressed)
	s.Pods = slices.Clone(s.Pods)
	for i := range s.Pods {
		s.Pods[i].Truncated = cloneTruncation(s.Pods[i].Truncated)
		s.Pods[i].Quarantined = slices.Clone(s.Pods[i].Quarantined)
		s.Pods[i].Suppressed = cloneSuppressed(s.Pods[i].Suppressed)
	}
	return s
}

func cloneSuppressed(ss []Suppressed) []Suppressed {
	ss = slices.Clone(ss)
	for i := range ss {
		ss[i].Spans = slices.Clone(ss[i].Spans)
	}
	return ss
}

func cloneTruncation(t Truncation) Truncation {
	t.Nodes, t.Hosts = slices.Clone(t.Nodes), slices.Clone(t.Hosts)
	t.Members, t.Instances, t.SeedPods = slices.Clone(t.Members), slices.Clone(t.Instances), slices.Clone(t.SeedPods)
	t.Depth = slices.Clone(t.Depth)
	t.Frontier = slices.Clone(t.Frontier)
	for i := range t.Frontier {
		t.Frontier[i].Dropped = slices.Clone(t.Frontier[i].Dropped)
	}
	t.PerVia = slices.Clone(t.PerVia)
	return t
}
