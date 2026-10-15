package extract

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/query/coresidency"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
)

// Extractor derives extractions from a store, which it only reads.
type Extractor struct {
	st store.Store
	// capacity is the size of the memo each Extract builds, in the units of
	// memoCap; 0 keeps nothing.
	capacity int
}

// NewExtractor returns an extractor over st.
func NewExtractor(st store.Store) *Extractor { return &Extractor{st: st, capacity: memoCap} }

// counting is a store an extraction reads through, its Finds included: it
// counts the reads the budget is stated in, and never writes. An extraction has
// two: one above its memo, which counts every read request, and one below it,
// which counts the reads that reach the store.
type counting struct {
	store.Store
	mu    sync.Mutex
	reads coresidency.Reads
}

func (c *counting) count(f func(*coresidency.Reads)) {
	c.mu.Lock()
	f(&c.reads)
	c.mu.Unlock()
}

func (c *counting) Window(ctx context.Context, fp identity.Fingerprint, d store.Direction, from, to time.Time, s store.Scope) ([]store.Record, error) {
	c.count(func(r *coresidency.Reads) { r.Windows++ })
	return c.Store.Window(ctx, fp, d, from, to, s)
}

func (c *counting) EntityWindow(ctx context.Context, fp identity.Fingerprint, from, to time.Time, s store.Scope) ([]store.Record, error) {
	c.count(func(r *coresidency.Reads) { r.EntityWindows++ })
	return c.Store.EntityWindow(ctx, fp, from, to, s)
}

// WindowSince counts one Window per fingerprint of an edge read, and one
// EntityWindow per fingerprint of an existence read (direction 0): the reads it
// stands for.
func (c *counting) WindowSince(ctx context.Context, fps []identity.Fingerprint, d store.Direction, from, to time.Time, s store.Scope, o store.SinceOptions) ([]store.Since, error) {
	c.count(func(r *coresidency.Reads) {
		if d == 0 {
			r.EntityWindows += len(fps)
		} else {
			r.Windows += len(fps)
		}
	})
	return c.Store.WindowSince(ctx, fps, d, from, to, s, o)
}

func (c *counting) Alive(ctx context.Context, fp identity.Fingerprint, t time.Time, s store.Scope) (bool, error) {
	c.count(func(r *coresidency.Reads) { r.Alive++ })
	return c.Store.Alive(ctx, fp, t, s)
}

// Extract derives the members of seed's dependency hop and the services that
// shared a pod, a node or a host with their seed pods in w. Arguments are
// checked before any store call. After that, any error of a read fails the
// whole extraction with a zero Result: there is no partial answer, and a window
// that starts before a layer's retention horizon gives an error wrapping
// store.ErrBeforeHorizon.
func (e *Extractor) Extract(ctx context.Context, seed Seed, w interval.Window, o Options) (Result, error) {
	fail := func(err error) (Result, error) { return Result{}, fmt.Errorf("extract: Extract: %w", err) }
	x, err := newExtraction(ctx, seed, w, o)
	if err != nil {
		return fail(err)
	}
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	// The reader and every Find read through cnt, which counts the requests;
	// the memo answers the ones it has seen; storeCnt counts the rest.
	storeCnt := &counting{Store: e.st}
	cnt := &counting{Store: newMemo(storeCnt, e.capacity)}
	x.r, x.f = interval.NewReader(cnt, interval.Options{}), coresidency.NewFinder(cnt)
	if err := x.run(); err != nil {
		return fail(err)
	}
	x.res.StoreReads = storeCnt.reads
	x.res.Reads = cnt.reads
	x.res.Reads.Calls = x.calls
	for _, fr := range x.res.PodResults {
		x.res.Reads.Calls += fr.Reads.Calls
	}
	return x.res, nil
}

// newExtraction checks the arguments, in the order of the package
// documentation, with no store call.
func newExtraction(ctx context.Context, seed Seed, w interval.Window, o Options) (*extraction, error) {
	if !w.From.Before(w.To) {
		return nil, fmt.Errorf("the window [%s, %s) is empty: %w", formatTime(w.From), formatTime(w.To), store.ErrInvalid)
	}
	x := &extraction{
		ctx: ctx, w: w, seed: seed, nodeOnly: o.NodeOnly,
		byService: map[identity.Fingerprint]*member{}, inst: map[identity.Fingerprint]*instance{},
		podInst: map[identity.Fingerprint][]identity.Fingerprint{}, podRead: map[identity.Fingerprint]bool{},
		quarantined: map[store.Subject]interval.Element{}, finds: map[identity.Fingerprint]int{},
	}
	switch s := seed.(type) {
	case Exploratory:
		x.service = s.Service
	case Anchored:
		x.service = s.Service
		x.anchor = &s
	default:
		return nil, fmt.Errorf("the seed %T is neither Exploratory nor Anchored: %w", seed, store.ErrInvalid)
	}
	if x.service.IsZero() || x.service.Type() != catalog.Service {
		return nil, fmt.Errorf("the seed service %s is not a %s: %w", x.service, catalog.Service, store.ErrInvalid)
	}
	if a := x.anchor; a != nil {
		switch {
		case a.Host.IsZero() || a.Host.Type() != catalog.Host:
			return nil, fmt.Errorf("the anchor %s is not a %s: %w", a.Host, catalog.Host, store.ErrInvalid)
		case a.At.Before(w.From) || !a.At.Before(w.To):
			return nil, fmt.Errorf("the anchor's instant %s is outside the window [%s, %s): %w",
				formatTime(a.At), formatTime(w.From), formatTime(w.To), store.ErrInvalid)
		case o.NodeOnly:
			return nil, fmt.Errorf("an anchored seed needs its host's runs_on, which NodeOnly does not read: %w", store.ErrInvalid)
		}
	}
	x.pol = o.Policy
	if x.pol == (Policy{}) {
		x.pol = DefaultPolicy()
	}
	if x.pol.CoResidency == (coresidency.Policy{}) {
		x.pol.CoResidency = coresidency.DefaultPolicy()
	}
	if err := x.pol.validate(); err != nil {
		return nil, err
	}
	return x, nil
}

// extraction is one Extract in progress.
type extraction struct {
	ctx      context.Context
	r        *interval.Reader
	f        *coresidency.Finder
	w        interval.Window // AsOf is the pinned token after the first call
	seed     Seed
	service  identity.Fingerprint
	anchor   *Anchored
	pol      Policy
	nodeOnly bool
	calls    int
	res      Result

	members   []*member // ranked and capped
	byService map[identity.Fingerprint]*member
	inst      map[identity.Fingerprint]*instance              // the instances read forward
	podInst   map[identity.Fingerprint][]identity.Fingerprint // each pod read in reverse: its instances, sorted
	podRead   map[identity.Fingerprint]bool
	own       []Unattributed
	seedPods  []identity.Fingerprint
	finds     map[identity.Fingerprint]int // a pod's Find, as an index into res.PodResults
	contribs  []*contrib

	quarantined map[store.Subject]interval.Element
}

// member is a Member with its rank.
type member struct {
	Member
	rank int
}

// instance is what one instance's forward read says: its claims of services and
// of pods.
type instance struct{ svc, pod claims }

// claims are an instance's edges of one relation, and for each target the
// instants at which the instance claims it and no other target.
type claims struct {
	els map[identity.Fingerprint]interval.Element
	eff map[identity.Fingerprint][]interval.Span
}

// side is one instance mapped to one service through one pod: the two mapping
// legs and the instants each counts for.
type side struct {
	inst, pod, svc identity.Fingerprint
	svcEl, podEl   interval.Element
	svcEff, podEff []interval.Span
}

// contrib is a Contribution in progress, with the instants each leg counts for
// and the pod-level entry it came from.
type contrib struct {
	Contribution
	mech         Mechanism
	member, peer identity.Fingerprint // the services on the MemberPod and PeerPod sides
	eff          [4][]interval.Span
	entry        *coresidency.CoResident
}

// edges is one interval call at the pinned token.
func (x *extraction) edges(fps []identity.Fingerprint, dir store.Direction) ([][]interval.Element, error) {
	x.calls++
	_, out, err := x.r.EdgesBatch(x.ctx, fps, dir, x.w)
	return out, err
}

func (x *extraction) quarantine(el interval.Element) { x.quarantined[el.Subject] = el }

func (x *extraction) run() error {
	if err := x.dependencies(); err != nil {
		return err
	}
	var err error
	if x.anchor != nil {
		err = x.anchored()
	} else {
		err = x.exploratory()
	}
	if err != nil {
		return err
	}
	if err := x.mapPeers(); err != nil {
		return err
	}
	return x.finish()
}

// dependencies makes hops S1 and S2, and ranks and caps the members.
func (x *extraction) dependencies() error {
	// S1: the seed's instances and callers. Its token is every later call's.
	x.calls++
	a, err := x.r.Edges(x.ctx, x.service, store.Reverse, x.w)
	if err != nil {
		return err
	}
	x.w.AsOf = a.Token
	x.res = Result{
		Token: a.Token, From: a.From, To: a.To, Seed: x.seed,
		PolicyVersion: PolicyVersion, CoResidencyPolicy: coresidency.PolicyVersion, IntervalPolicy: interval.PolicyVersion,
	}
	if x.anchor != nil {
		x.res.Anchor = NotOnHost
	}
	seed := &member{Member: Member{Service: x.service, Role: RoleSeed}}
	x.byService[x.service] = seed
	neighbour := func(fp identity.Fingerprint, role Role, el interval.Element) {
		m := x.byService[fp]
		if m == nil {
			m = &member{Member: Member{Service: fp}}
			x.byService[fp] = m
		}
		m.Role |= role
		m.Dependency = append(m.Dependency, el)
	}
	for _, el := range a.Elements {
		switch {
		case el.Subject.Relation == catalog.InstanceOf && el.Subject.A.Type() == catalog.ServiceInstance:
			if el.Uncascaded != nil {
				x.quarantine(el)
				continue
			}
			seed.Instances = append(seed.Instances, el)
		case el.Subject.Relation == catalog.DependsOn && el.Subject.A.Type() == catalog.Service && el.Subject.A != x.service:
			if el.Uncascaded != nil {
				x.quarantine(el)
				continue
			}
			neighbour(el.Subject.A, RoleCaller, el)
		}
	}

	// S2: the seed's callees.
	out, err := x.edges([]identity.Fingerprint{x.service}, store.Forward)
	if err != nil {
		return err
	}
	for _, el := range out[0] {
		if el.Subject.Relation != catalog.DependsOn || el.Subject.B.Type() != catalog.Service || el.Subject.B == x.service {
			continue
		}
		if el.Uncascaded != nil {
			x.quarantine(el)
			continue
		}
		neighbour(el.Subject.B, RoleCallee, el)
	}

	var others []*member
	for fp, m := range x.byService {
		if fp != x.service {
			others = append(others, m)
		}
	}
	dur := func(m *member) time.Duration {
		var ss [][]interval.Span
		for _, el := range m.Dependency {
			ss = append(ss, interval.Spans(el))
		}
		return interval.Duration(union(ss...))
	}
	slices.SortFunc(others, func(a, b *member) int {
		if c := cmp.Compare(dur(b), dur(a)); c != 0 {
			return c
		}
		return store.CompareFingerprints(a.Service, b.Service)
	})
	if keep := x.pol.MaxMembers - 1; len(others) > keep {
		for _, m := range others[keep:] {
			x.res.Truncated.Members = append(x.res.Truncated.Members, m.Service)
			delete(x.byService, m.Service)
		}
		others = others[:keep]
	}
	x.members = append([]*member{seed}, others...)
	for i, m := range x.members {
		m.rank = i
	}
	return nil
}

// capInstances keeps a member's MaxInstances instance_of elements with the
// longest spans, then by instance.
func (x *extraction) capInstances(m *member) {
	slices.SortFunc(m.Instances, func(a, b interval.Element) int {
		if c := cmp.Compare(interval.Duration(interval.Spans(b)), interval.Duration(interval.Spans(a))); c != 0 {
			return c
		}
		return store.CompareFingerprints(a.Subject.A, b.Subject.A)
	})
	if len(m.Instances) > x.pol.MaxInstances {
		for _, el := range m.Instances[x.pol.MaxInstances:] {
			x.res.Truncated.Instances = append(x.res.Truncated.Instances, el.Subject.A)
		}
		m.Instances = m.Instances[:x.pol.MaxInstances]
	}
	sortElements(m.Instances)
}

func sortElements(els []interval.Element) {
	slices.SortFunc(els, func(a, b interval.Element) int { return compareSubjects(a.Subject, b.Subject) })
}

// exploratory makes hops X1 and X2, chooses the seed pods and runs their Finds.
func (x *extraction) exploratory() error {
	// X1: the neighbours' instances. The second dependency hop it also reads is
	// ignored.
	if len(x.members) > 1 {
		var nbrs []identity.Fingerprint
		for _, m := range x.members[1:] {
			nbrs = append(nbrs, m.Service)
		}
		out, err := x.edges(nbrs, store.Reverse)
		if err != nil {
			return err
		}
		for i, m := range x.members[1:] {
			for _, el := range out[i] {
				if el.Subject.Relation != catalog.InstanceOf || el.Subject.A.Type() != catalog.ServiceInstance {
					continue
				}
				if el.Uncascaded != nil {
					x.quarantine(el)
					continue
				}
				m.Instances = append(m.Instances, el)
			}
		}
	}
	var insts []identity.Fingerprint
	seen := map[identity.Fingerprint]bool{}
	for _, m := range x.members {
		x.capInstances(m)
		for _, el := range m.Instances {
			if !seen[el.Subject.A] {
				seen[el.Subject.A] = true
				insts = append(insts, el.Subject.A)
			}
		}
	}
	if len(insts) == 0 {
		return nil
	}

	// X2: the instances' pods and services.
	out, err := x.edges(insts, store.Forward)
	if err != nil {
		return err
	}
	x.readInstances(insts, out)
	cands := map[identity.Fingerprint]*podCand{}
	for _, m := range x.members {
		for _, el := range m.Instances {
			in := x.inst[el.Subject.A]
			for p, eff := range in.pod.eff {
				x.offer(cands, p, m, interval.Along(in.svc.eff[m.Service], eff))
			}
		}
	}
	x.seedPods = x.rankPods(cands)
	for _, p := range x.seedPods {
		if _, err := x.find(p); err != nil {
			return err
		}
	}
	return nil
}

// podCand is a pod that may be a seed pod: its best member's rank, and its
// useful spans for that member.
type podCand struct {
	pod    identity.Fingerprint
	rank   int
	useful []interval.Span
}

// offer adds useful spans of pod p for member m, keeping the best-ranked member.
func (x *extraction) offer(cands map[identity.Fingerprint]*podCand, p identity.Fingerprint, m *member, useful []interval.Span) {
	if len(useful) == 0 {
		return
	}
	c := cands[p]
	switch {
	case c == nil:
		cands[p] = &podCand{pod: p, rank: m.rank, useful: useful}
	case m.rank < c.rank:
		c.rank, c.useful = m.rank, useful
	case m.rank == c.rank:
		c.useful = union(c.useful, useful)
	}
}

// rankPods ranks candidate pods by member rank, then useful duration,
// descending, then fingerprint, and keeps MaxSeedPods of them.
func (x *extraction) rankPods(cands map[identity.Fingerprint]*podCand) []identity.Fingerprint {
	cs := slices.Collect(maps.Values(cands))
	slices.SortFunc(cs, func(a, b *podCand) int {
		if c := cmp.Compare(a.rank, b.rank); c != 0 {
			return c
		}
		if c := cmp.Compare(interval.Duration(b.useful), interval.Duration(a.useful)); c != 0 {
			return c
		}
		return store.CompareFingerprints(a.pod, b.pod)
	})
	var out []identity.Fingerprint
	for i, c := range cs {
		if i >= x.pol.MaxSeedPods {
			x.res.Truncated.SeedPods = append(x.res.Truncated.SeedPods, c.pod)
			continue
		}
		out = append(out, c.pod)
	}
	return out
}

// anchored makes hops A1 to A4 from the host side, runs the candidates' Finds
// and confirms each by its seed hosts.
func (x *extraction) anchored() error {
	a := x.anchor
	x.capInstances(x.members[0])

	// A1: the host's nodes at the instant.
	out, err := x.edges([]identity.Fingerprint{a.Host}, store.Reverse)
	if err != nil {
		return err
	}
	var nodes []identity.Fingerprint
	for _, el := range out[0] {
		if el.Subject.Relation != catalog.RunsOn || el.Subject.A.Type() != catalog.K8sNode {
			continue
		}
		if el.Uncascaded != nil {
			x.quarantine(el)
			continue
		}
		if at(interval.Spans(el), a.At) && !slices.Contains(nodes, el.Subject.A) {
			nodes = append(nodes, el.Subject.A)
		}
	}
	if len(nodes) == 0 {
		return nil
	}

	// A2: the pods on them at the instant.
	if out, err = x.edges(nodes, store.Reverse); err != nil {
		return err
	}
	var pods []identity.Fingerprint
	for i := range nodes {
		for _, el := range out[i] {
			if el.Subject.Relation != catalog.ScheduledOn || el.Subject.A.Type() != catalog.K8sPod {
				continue
			}
			if el.Uncascaded != nil {
				x.quarantine(el)
				continue
			}
			if at(interval.Spans(el), a.At) && !slices.Contains(pods, el.Subject.A) {
				pods = append(pods, el.Subject.A)
			}
		}
	}
	if len(pods) == 0 {
		return nil
	}

	// A3: the instances part of those pods.
	if out, err = x.edges(pods, store.Reverse); err != nil {
		return err
	}
	insts := x.readPods(pods, out)
	if len(insts) == 0 {
		return nil
	}

	// A4: those instances' services and pods.
	if out, err = x.edges(insts, store.Forward); err != nil {
		return err
	}
	x.readInstances(insts, out)

	// The candidates: the pods with an instance of a member, both mapped singly
	// at the instant.
	cands := map[identity.Fingerprint]*podCand{}
	found := map[store.Subject]bool{}
	for _, m := range x.members {
		for _, el := range m.Instances {
			found[el.Subject] = true
		}
	}
	for _, i := range insts {
		in := x.inst[i]
		for _, svc := range sortedKeys(in.svc.els) {
			if m := x.byService[svc]; m != nil && !found[in.svc.els[svc].Subject] {
				found[in.svc.els[svc].Subject] = true
				m.Instances = append(m.Instances, in.svc.els[svc])
			}
		}
	}
	for _, m := range x.members {
		sortElements(m.Instances)
	}
	// Every instance used is listed: one the cap left out of the seed's but
	// found on the anchor is no longer truncated.
	x.res.Truncated.Instances = slices.DeleteFunc(x.res.Truncated.Instances, func(i identity.Fingerprint) bool {
		return found[store.EdgeSubject(i, x.service, catalog.InstanceOf)]
	})
	for _, p := range pods {
		var of []*member
		for _, i := range x.podInst[p] {
			in := x.inst[i]
			if !at(in.pod.eff[p], a.At) {
				continue
			}
			for _, svc := range sortedKeys(in.svc.eff) {
				if m := x.byService[svc]; m != nil && at(in.svc.eff[svc], a.At) && !slices.Contains(of, m) {
					of = append(of, m)
				}
			}
		}
		// A candidate is ranked like a member pod: by its member, then by its
		// useful time over the window.
		for _, m := range of {
			for _, i := range x.podInst[p] {
				in := x.inst[i]
				x.offer(cands, p, m, interval.Along(in.svc.eff[m.Service], in.pod.eff[p]))
			}
		}
	}
	ranked := x.rankPods(cands)
	if len(ranked) == 0 || cands[ranked[0]].rank != 0 {
		return nil
	}

	// The Finds, in rank order: the seed's candidates first. The neighbours'
	// are run only once one of the seed's is confirmed.
	confirmed := false
	for _, p := range ranked {
		if cands[p].rank > 0 && !confirmed {
			break
		}
		fr, err := x.find(p)
		if err != nil {
			return err
		}
		if onHost(fr, a.Host, a.At) {
			x.seedPods = append(x.seedPods, p)
			if cands[p].rank == 0 {
				confirmed = true
			}
		}
	}
	if confirmed {
		x.res.Anchor = OnHost
	} else {
		x.res.Anchor = HostUnattributed
		x.seedPods = nil
	}
	return nil
}

// onHost reports whether a Find attributes its seed to host h at t.
func onHost(fr coresidency.Result, h identity.Fingerprint, t time.Time) bool {
	return slices.ContainsFunc(fr.SeedHosts, func(hs coresidency.HostSpans) bool { return hs.Host == h && at(hs.Spans, t) })
}

// find runs one pod's Find at the pinned token.
func (x *extraction) find(p identity.Fingerprint) (coresidency.Result, error) {
	fr, err := x.f.Find(x.ctx, p, x.w, coresidency.Options{NodeOnly: x.nodeOnly, Policy: x.pol.CoResidency})
	if err != nil {
		return coresidency.Result{}, err
	}
	x.finds[p] = len(x.res.PodResults)
	x.res.PodResults = append(x.res.PodResults, fr)
	return fr, nil
}

// readPods records each pod's instances from its reverse read, and returns the
// instances not yet seen in this read, in order.
func (x *extraction) readPods(pods []identity.Fingerprint, out [][]interval.Element) []identity.Fingerprint {
	var insts []identity.Fingerprint
	seen := map[identity.Fingerprint]bool{}
	for i, p := range pods {
		x.podRead[p] = true
		for _, el := range out[i] {
			if el.Subject.Relation != catalog.PartOf || el.Subject.A.Type() != catalog.ServiceInstance {
				continue
			}
			if el.Uncascaded != nil {
				x.quarantine(el)
				continue
			}
			in := el.Subject.A
			if !slices.Contains(x.podInst[p], in) {
				x.podInst[p] = append(x.podInst[p], in)
			}
			if !seen[in] {
				seen[in] = true
				insts = append(insts, in)
			}
		}
		slices.SortFunc(x.podInst[p], store.CompareFingerprints)
	}
	return insts
}

// readInstances records each instance's claims of services and pods from its
// forward read, and lists the instants at which it claims several of either.
func (x *extraction) readInstances(insts []identity.Fingerprint, out [][]interval.Element) {
	for i, fp := range insts {
		var svc, pod []interval.Element
		for _, el := range out[i] {
			switch {
			case el.Subject.Relation == catalog.InstanceOf && el.Subject.B.Type() == catalog.Service:
				svc = append(svc, el)
			case el.Subject.Relation == catalog.PartOf && el.Subject.B.Type() == catalog.K8sPod:
				pod = append(pod, el)
			}
		}
		x.inst[fp] = &instance{
			svc: x.claim(fp, svc, SeveralServices),
			pod: x.claim(fp, pod, SeveralPods),
		}
	}
}

// claim derives one relation's claims of an instance. A claim whose target is
// quarantined is never followed, but it still counts as a claim: the instance
// is not mapped to another target over it.
func (x *extraction) claim(fp identity.Fingerprint, els []interval.Element, why UnattributedReason) claims {
	c := claims{els: map[identity.Fingerprint]interval.Element{}, eff: map[identity.Fingerprint][]interval.Span{}}
	var several [][]interval.Span
	for i, el := range els {
		var others [][]interval.Span
		for j, o := range els {
			if j != i {
				others = append(others, interval.Spans(o))
			}
		}
		rest := union(others...)
		mine := interval.Spans(el)
		several = append(several, interval.Intersect(mine, rest))
		if el.Uncascaded != nil {
			x.quarantine(el)
			continue
		}
		c.els[el.Subject.B] = el
		c.eff[el.Subject.B] = subtract(mine, rest)
	}
	if s := union(several...); len(s) > 0 {
		var cands []identity.Fingerprint
		for _, el := range els {
			if len(interval.Intersect(interval.Spans(el), s)) > 0 && !slices.Contains(cands, el.Subject.B) {
				cands = append(cands, el.Subject.B)
			}
		}
		slices.SortFunc(cands, store.CompareFingerprints)
		x.own = append(x.own, Unattributed{Subject: fp, Spans: s, Mapping: why, Candidates: cands})
	}
	return c
}

// mapPeers makes hops M1 and M2: the instances of the seed pods and their
// co-residents that A3 has not read, and the services of the instances that no
// earlier hop has read forward.
func (x *extraction) mapPeers() error {
	var pods []identity.Fingerprint
	seen := map[identity.Fingerprint]bool{}
	add := func(p identity.Fingerprint) {
		if !x.podRead[p] && !seen[p] {
			seen[p] = true
			pods = append(pods, p)
		}
	}
	for _, p := range x.seedPods {
		add(p)
	}
	for _, p := range x.seedPods {
		for _, e := range x.res.PodResults[x.finds[p]].CoResidents {
			add(e.Peer)
		}
	}
	if len(pods) == 0 {
		return nil
	}
	out, err := x.edges(pods, store.Reverse)
	if err != nil {
		return err
	}
	var insts []identity.Fingerprint
	for _, in := range x.readPods(pods, out) {
		if x.inst[in] == nil {
			insts = append(insts, in)
		}
	}
	if len(insts) == 0 {
		return nil
	}
	if out, err = x.edges(insts, store.Forward); err != nil {
		return err
	}
	x.readInstances(insts, out)
	return nil
}

// sides lists the instances of pod p mapped to a service, each with its
// mapping legs; with members set, only those mapped to a member.
func (x *extraction) sides(p identity.Fingerprint, members bool) []side {
	var out []side
	for _, i := range x.podInst[p] {
		in := x.inst[i]
		if in == nil {
			continue
		}
		podEl, ok := in.pod.els[p]
		if !ok {
			continue
		}
		for _, svc := range sortedKeys(in.svc.eff) {
			if members && x.byService[svc] == nil {
				continue
			}
			out = append(out, side{
				inst: i, pod: p, svc: svc, svcEl: in.svc.els[svc], podEl: podEl,
				svcEff: in.svc.eff[svc], podEff: in.pod.eff[p],
			})
		}
	}
	return out
}

// contributions derives every path from a seed pod's mapped instances to
// another service: through the pod itself, and through each of its Find's
// entries.
func (x *extraction) contributions() {
	for _, p := range x.seedPods {
		mine := x.sides(p, true)
		if len(mine) == 0 {
			continue
		}
		for _, a := range mine {
			for _, b := range x.sides(p, false) {
				if a.inst == b.inst || a.svc == b.svc {
					continue
				}
				x.contribute(SharedPod, a, b, p, -1, nil)
			}
		}
		fr := &x.res.PodResults[x.finds[p]]
		for k := range fr.CoResidents {
			e := &fr.CoResidents[k]
			mech := SharedNode
			if e.Mechanism == coresidency.SharedHost {
				mech = SharedHost
			}
			theirs := x.sides(e.Peer, false)
			for _, a := range mine {
				for _, b := range theirs {
					if a.svc != b.svc {
						x.contribute(mech, a, b, e.Via, k, e)
					}
				}
			}
		}
	}
}

func (x *extraction) contribute(mech Mechanism, a, b side, via identity.Fingerprint, k int, e *coresidency.CoResident) {
	c := &contrib{
		Contribution: Contribution{
			MemberPod: a.pod, PeerPod: b.pod, MemberInst: a.inst, PeerInst: b.inst, Via: via, Entry: k,
			Legs: []interval.Element{a.svcEl, a.podEl, b.podEl, b.svcEl},
		},
		mech: mech, member: a.svc, peer: b.svc,
		eff:   [4][]interval.Span{a.svcEff, a.podEff, b.podEff, b.svcEff},
		entry: e,
	}
	legs := c.eff[:]
	if e != nil {
		legs = append(legs, e.Spans())
	}
	if c.Spans = interval.Along(legs...); len(c.Spans) > 0 {
		x.contribs = append(x.contribs, c)
	}
}

// pairKey names a pair: two members ordered by fingerprint, or a member and a
// peer.
type pairKey struct {
	member, peer identity.Fingerprint
	mech         Mechanism
}

func (x *extraction) keyOf(c *contrib) pairKey {
	m, p := c.member, c.peer
	if x.byService[p] != nil && store.CompareFingerprints(p, m) < 0 {
		m, p = p, m
	}
	return pairKey{m, p, c.mech}
}

// finish derives the pairs, applies ubiquity, the caps and the ranking, and
// orders everything.
func (x *extraction) finish() error {
	x.contributions()
	from, to := x.res.From, x.res.To

	var keys []pairKey
	byKey := map[pairKey][]*contrib{}
	for _, c := range x.contribs {
		k := x.keyOf(c)
		if _, ok := byKey[k]; !ok {
			keys = append(keys, k)
		}
		byKey[k] = append(byKey[k], c)
	}

	// Ubiquity: coverage of the frontier of the seed pods' nodes.
	frontier := map[identity.Fingerprint]bool{}
	for _, p := range x.seedPods {
		for _, n := range attributedNodes(x.res.PodResults[x.finds[p]].Placement) {
			frontier[n] = true
		}
	}
	covered := map[identity.Fingerprint]map[identity.Fingerprint]bool{}
	for _, c := range x.contribs {
		if c.mech == SharedNode && frontier[c.Via] {
			if covered[c.peer] == nil {
				covered[c.peer] = map[identity.Fingerprint]bool{}
			}
			covered[c.peer][c.Via] = true
		}
	}
	f := len(frontier)
	judged := x.anchor == nil && f >= x.pol.UbiquityMinNodes
	x.res.Ubiquity = Ubiquity{Judged: judged, FrontierNodes: f}
	ubiquitous := func(svc identity.Fingerprint) bool {
		return judged && len(covered[svc])*x.pol.UbiquityDen >= x.pol.UbiquityNum*f
	}

	var kept, suppressed []Pair
	for _, k := range keys {
		p, err := x.pair(k, byKey[k], from, to)
		if err != nil {
			return err
		}
		// Only a peer that is not a member is judged: the frontier is the
		// members' own pods, so a member's coverage refers back to itself.
		ubiq := false
		if x.byService[k.peer] == nil {
			ubiq = ubiquitous(k.peer)
			if judged {
				p.Coverage = [2]int{len(covered[k.peer]), f}
			}
		}
		if ubiq {
			p.Reason = coresidency.Ubiquitous
			suppressed = append(suppressed, p)
			continue
		}
		kept = append(kept, p)
	}
	slices.SortFunc(kept, x.rankPairs)
	slices.SortFunc(suppressed, x.rankPairs)
	if len(kept) > x.pol.MaxPairs {
		x.res.Truncated.Pairs = len(kept) - x.pol.MaxPairs
		kept = kept[:x.pol.MaxPairs]
	}
	if len(suppressed) > x.pol.MaxPairs {
		x.res.Truncated.SuppressedDropped = len(suppressed) - x.pol.MaxPairs
		suppressed = suppressed[:x.pol.MaxPairs]
	}
	x.res.Pairs, x.res.Suppressed = kept, suppressed

	x.unmapped()
	for _, m := range x.members {
		x.res.Members = append(x.res.Members, m.Member)
	}
	slices.SortFunc(x.own, func(a, b Unattributed) int {
		if c := store.CompareFingerprints(a.Subject, b.Subject); c != 0 {
			return c
		}
		if c := a.Spans[0].Start.Compare(b.Spans[0].Start); c != 0 {
			return c
		}
		return cmp.Compare(a.Mapping, b.Mapping)
	})
	x.res.Unattributed = x.own
	for _, fr := range x.res.PodResults {
		for _, u := range fr.Unattributed {
			x.res.Unattributed = append(x.res.Unattributed, Unattributed{
				SeedPod: fr.Seed, Subject: u.Subject, Spans: u.Spans, Placement: u.Reason, Candidates: u.Candidates,
			})
		}
	}
	for _, el := range x.quarantined {
		x.res.Quarantined = append(x.res.Quarantined, el)
	}
	sortElements(x.res.Quarantined)
	tr := &x.res.Truncated
	for _, l := range [][]identity.Fingerprint{tr.Members, tr.Instances, tr.SeedPods} {
		slices.SortFunc(l, store.CompareFingerprints)
	}
	return nil
}

// attributedNodes is the nodes a pod's placement has it on alone at some
// instant.
func attributedNodes(placement []interval.Element) []identity.Fingerprint {
	var out []identity.Fingerprint
	for i, el := range placement {
		var others [][]interval.Span
		for j, o := range placement {
			if j != i {
				others = append(others, interval.Spans(o))
			}
		}
		if len(subtract(interval.Spans(el), union(others...))) > 0 {
			out = append(out, el.Subject.B)
		}
	}
	return out
}

// pair is the union of a pair's contributions, with why each span ends.
func (x *extraction) pair(k pairKey, cs []*contrib, from, to time.Time) (Pair, error) {
	p := Pair{Member: k.member, Peer: k.peer, Mechanism: k.mech, Vias: map[identity.Fingerprint][]interval.Span{}}
	var all [][]interval.Span
	for _, c := range cs {
		all = append(all, c.Spans)
		p.Vias[c.Via] = union(p.Vias[c.Via], c.Spans)
		p.Contributions = append(p.Contributions, c.Contribution)
	}
	for _, s := range union(all...) {
		ps := Span{Span: s}
		for ci, c := range cs {
			for _, cspan := range c.Spans {
				if s.Start.Equal(from) && cspan.Start.Equal(from) {
					b, err := c.before(cspan)
					if err != nil {
						return Pair{}, err
					}
					ps.Before = ps.Before || b
				}
				if !s.End.Equal(to) && cspan.End.Equal(s.End) {
					ends, err := c.ends(ci, cspan)
					if err != nil {
						return Pair{}, err
					}
					ps.Ends = append(ps.Ends, ends...)
				}
			}
		}
		p.Spans = append(p.Spans, ps)
	}
	ss := Spans(p)
	p.Duration = interval.Duration(ss)
	p.Class, p.Transitions = classOf(ss, from, to)
	return p, nil
}

// errInternal marks an answer that breaks this package's own invariants.
var errInternal = errors.New("internal inconsistency")

// piece is the piece of leg j that contains s.
func (c *contrib) piece(j int, s interval.Span) (interval.Piece, error) {
	ps := c.Legs[j].Pieces
	k := slices.IndexFunc(ps, func(p interval.Piece) bool { return !p.Start.After(s.Start) && !p.End.Before(s.End) })
	if k < 0 {
		return interval.Piece{}, fmt.Errorf("a span [%s, %s) of %s lies in no piece of leg %d: %w",
			formatTime(s.Start), formatTime(s.End), c.MemberPod, j, errInternal)
	}
	return ps[k], nil
}

// overlap is the pod-level overlap that contains s.
func (c *contrib) overlap(s interval.Span) (coresidency.Overlap, error) {
	k := containing(c.entry.Spans(), s)
	if k < 0 {
		return coresidency.Overlap{}, fmt.Errorf("a span [%s, %s) of %s lies in no overlap of its entry: %w",
			formatTime(s.Start), formatTime(s.End), c.MemberPod, errInternal)
	}
	return c.entry.Overlaps[k], nil
}

// before reports whether every leg's piece containing s, a span of the
// contribution starting at From, began before From.
func (c *contrib) before(s interval.Span) (bool, error) {
	for j := range c.Legs {
		p, err := c.piece(j, s)
		if err != nil {
			return false, err
		}
		if !p.Before {
			return false, nil
		}
	}
	if c.entry != nil {
		o, err := c.overlap(s)
		if err != nil {
			return false, err
		}
		return o.Before, nil
	}
	return true, nil
}

// ends lists the legs of the contribution that end where its span s ends.
func (c *contrib) ends(ci int, s interval.Span) ([]End, error) {
	var out []End
	for j := range c.Legs {
		k := containing(c.eff[j], s)
		if k < 0 {
			return nil, fmt.Errorf("a span of %s lies in no span of mapping leg %d: %w", c.MemberPod, j, errInternal)
		}
		if !c.eff[j][k].End.Equal(s.End) {
			continue
		}
		p, err := c.piece(j, s)
		if err != nil {
			return nil, err
		}
		if p.End.Equal(s.End) {
			out = append(out, End{Contribution: ci, Leg: j, Source: p.EndSource})
		} else {
			out = append(out, End{Contribution: ci, Leg: j, Unattributed: true})
		}
	}
	if c.entry != nil {
		o, err := c.overlap(s)
		if err != nil {
			return nil, err
		}
		if o.End.Equal(s.End) {
			for _, e := range o.Ends {
				out = append(out, End{Contribution: ci, Leg: len(c.Legs) + e.Leg, Pod: &e})
			}
		}
	}
	return out, nil
}

// rankPairs orders pairs: total duration descending, then the mechanism
// (shared pod, shared node, shared host), then the best rank of their
// members, then peer, then member.
func (x *extraction) rankPairs(a, b Pair) int {
	if c := cmp.Compare(b.Duration, a.Duration); c != 0 {
		return c
	}
	if c := cmp.Compare(a.Mechanism, b.Mechanism); c != 0 {
		return c
	}
	if c := cmp.Compare(x.bestRank(a), x.bestRank(b)); c != 0 {
		return c
	}
	if c := store.CompareFingerprints(a.Peer, b.Peer); c != 0 {
		return c
	}
	return store.CompareFingerprints(a.Member, b.Member)
}

func (x *extraction) bestRank(p Pair) int {
	r := x.byService[p.Member].rank
	if m := x.byService[p.Peer]; m != nil {
		r = min(r, m.rank)
	}
	return r
}

// unmapped lists, for each co-resident pod, the instants of its entries at
// which no instance of it is mapped singly to a service.
func (x *extraction) unmapped() {
	entries := map[identity.Fingerprint][][]interval.Span{}
	for _, p := range x.seedPods {
		for _, e := range x.res.PodResults[x.finds[p]].CoResidents {
			entries[e.Peer] = append(entries[e.Peer], e.Spans())
		}
	}
	for _, pod := range sortedKeys(entries) {
		var mapped [][]interval.Span
		for _, i := range x.podInst[pod] {
			in := x.inst[i]
			if in == nil {
				continue
			}
			mapped = append(mapped, interval.Along(in.pod.eff[pod], union(slices.Collect(maps.Values(in.svc.eff))...)))
		}
		if s := subtract(union(entries[pod]...), union(mapped...)); len(s) > 0 {
			x.res.Unmapped = append(x.res.Unmapped, Unmapped{Pod: pod, Spans: s})
		}
	}
}

func sortedKeys[V any](m map[identity.Fingerprint]V) []identity.Fingerprint {
	out := slices.Collect(maps.Keys(m))
	slices.SortFunc(out, store.CompareFingerprints)
	return out
}

func compareSubjects(a, b store.Subject) int {
	if c := cmp.Compare(a.Kind, b.Kind); c != 0 {
		return c
	}
	if c := store.CompareFingerprints(a.A, b.A); c != 0 {
		return c
	}
	if c := store.CompareFingerprints(a.B, b.B); c != 0 {
		return c
	}
	return cmp.Compare(a.Relation, b.Relation)
}

func formatTime(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }
