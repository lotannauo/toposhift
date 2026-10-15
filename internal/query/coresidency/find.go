package coresidency

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
)

// Finder derives co-residency from a store, which it only reads.
type Finder struct{ st store.Store }

// NewFinder returns a finder over st.
func NewFinder(st store.Store) *Finder { return &Finder{st: st} }

// counting is the store a query reads through: it counts the reads the budget
// is stated in, and never writes.
type counting struct {
	store.Store
	mu    sync.Mutex
	reads Reads
}

func (c *counting) count(f func(*Reads)) {
	c.mu.Lock()
	f(&c.reads)
	c.mu.Unlock()
}

func (c *counting) Window(ctx context.Context, fp identity.Fingerprint, d store.Direction, from, to time.Time, s store.Scope) ([]store.Record, error) {
	c.count(func(r *Reads) { r.Windows++ })
	return c.Store.Window(ctx, fp, d, from, to, s)
}

func (c *counting) EntityWindow(ctx context.Context, fp identity.Fingerprint, from, to time.Time, s store.Scope) ([]store.Record, error) {
	c.count(func(r *Reads) { r.EntityWindows++ })
	return c.Store.EntityWindow(ctx, fp, from, to, s)
}

// WindowSince counts one Window per fingerprint of an edge read, and one
// EntityWindow per fingerprint of an existence read (direction 0): the reads it
// stands for.
func (c *counting) WindowSince(ctx context.Context, fps []identity.Fingerprint, d store.Direction, from, to time.Time, s store.Scope, o store.SinceOptions) ([]store.Since, error) {
	c.count(func(r *Reads) {
		if d == 0 {
			r.EntityWindows += len(fps)
		} else {
			r.Windows += len(fps)
		}
	})
	return c.Store.WindowSince(ctx, fps, d, from, to, s, o)
}

func (c *counting) Alive(ctx context.Context, fp identity.Fingerprint, t time.Time, s store.Scope) (bool, error) {
	c.count(func(r *Reads) { r.Alive++ })
	return c.Store.Alive(ctx, fp, t, s)
}

// Find derives the pods co-resident with seed, a k8s.pod, in w. Arguments are
// checked before any store call. After that, any error of a read fails the
// whole query with a zero Result: there is no partial answer, and a window
// that starts before a layer's retention horizon gives an error wrapping
// store.ErrBeforeHorizon.
func (f *Finder) Find(ctx context.Context, seed identity.Fingerprint, w interval.Window, o Options) (Result, error) {
	fail := func(err error) (Result, error) { return Result{}, fmt.Errorf("coresidency: Find: %w", err) }
	if !w.From.Before(w.To) {
		return fail(fmt.Errorf("the window [%s, %s) is empty: %w", formatTime(w.From), formatTime(w.To), store.ErrInvalid))
	}
	if seed.IsZero() || seed.Type() != catalog.K8sPod {
		return fail(fmt.Errorf("the seed %s is not a %s: %w", seed, catalog.K8sPod, store.ErrInvalid))
	}
	pol := o.Policy
	if pol == (Policy{}) {
		pol = DefaultPolicy()
	}
	if err := pol.validate(); err != nil {
		return fail(err)
	}
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	cnt := &counting{Store: f.st}
	q := &query{
		ctx: ctx, r: interval.NewReader(cnt, interval.Options{}), seed: seed, w: w, pol: pol, nodeOnly: o.NodeOnly,
		seedOn: map[identity.Fingerprint][]interval.Span{}, seedEl: map[identity.Fingerprint]interval.Element{},
		hosts: map[identity.Fingerprint]*placement{}, quarantined: map[store.Subject]interval.Element{},
	}
	if err := q.run(); err != nil {
		return fail(err)
	}
	q.res.Reads = cnt.reads
	q.res.Reads.Calls = q.calls
	return q.res, nil
}

// query is one Find in progress.
type query struct {
	ctx      context.Context
	r        *interval.Reader
	seed     identity.Fingerprint
	w        interval.Window // AsOf is the pinned token after the first call
	pol      Policy
	nodeOnly bool
	calls    int
	res      Result

	n1s         []identity.Fingerprint
	seedOn      map[identity.Fingerprint][]interval.Span  // the seed's attributed spans on each node
	seedEl      map[identity.Fingerprint]interval.Element // the seed's scheduled_on to each node
	hosts       map[identity.Fingerprint]*placement       // each node read forward: its runs_on claims
	quarantined map[store.Subject]interval.Element
	cands       []candidate
}

// placement is what one node's runs_on elements say: the claims, and for each
// host the instants at which the node is on it alone.
type placement struct {
	claims []interval.Element
	on     map[identity.Fingerprint][]interval.Span
	edge   map[identity.Fingerprint]interval.Element
}

// candidate is one path, before its overlaps are known.
type candidate struct {
	mech  Mechanism
	peer  identity.Fingerprint
	nodes []identity.Fingerprint
	via   identity.Fingerprint
	legs  []interval.Element
	eff   [][]interval.Span // per leg, the instants it counts for
}

// edges is one interval call at the pinned token.
func (q *query) edges(fps []identity.Fingerprint, dir store.Direction) ([][]interval.Element, error) {
	q.calls++
	_, out, err := q.r.EdgesBatch(q.ctx, fps, dir, q.w)
	return out, err
}

func (q *query) quarantine(el interval.Element) { q.quarantined[el.Subject] = el }

func (q *query) run() error {
	// Hop A: the seed's placement. Its token is every later call's.
	q.calls++
	a, err := q.r.Edges(q.ctx, q.seed, store.Forward, q.w)
	if err != nil {
		return err
	}
	q.w.AsOf = a.Token
	q.res = Result{
		Token: a.Token, From: a.From, To: a.To, Seed: q.seed,
		PolicyVersion: PolicyVersion, IntervalPolicy: interval.PolicyVersion,
	}
	for _, el := range a.Elements {
		if el.Subject.Relation != catalog.ScheduledOn || el.Subject.B.Type() != catalog.K8sNode {
			continue
		}
		if el.Uncascaded != nil {
			q.quarantine(el)
			continue
		}
		q.res.Placement = append(q.res.Placement, el)
	}
	q.attributeSeed()
	if len(q.n1s) > 0 {
		if err := q.hops(); err != nil {
			return err
		}
	}
	return q.finish()
}

// attributeSeed keeps, for each node, the seed's spans on it alone: over the
// instants the seed is scheduled on two or more nodes, its node is unattributed.
func (q *query) attributeSeed() {
	var ambiguous [][]interval.Span
	durations := map[identity.Fingerprint]time.Duration{}
	for i, el := range q.res.Placement {
		var others [][]interval.Span
		for j, o := range q.res.Placement {
			if j != i {
				others = append(others, interval.Spans(o))
			}
		}
		rest := union(others...)
		mine := interval.Spans(el)
		ambiguous = append(ambiguous, interval.Intersect(mine, rest))
		n := el.Subject.B
		q.seedEl[n] = el
		if s := subtract(mine, rest); len(s) > 0 {
			q.seedOn[n] = s
			durations[n] = interval.Duration(s)
			q.n1s = append(q.n1s, n)
		}
	}
	if amb := union(ambiguous...); len(amb) > 0 {
		q.unattributed(q.seed, amb, SeveralNodes, q.res.Placement, func(el interval.Element) identity.Fingerprint { return el.Subject.B })
	}
	q.n1s = q.cap(q.n1s, durations, q.pol.MaxNodes, &q.res.Truncated.Nodes)
}

// unattributed lists subject as unattributed over spans, with the candidates
// whose elements meet them.
func (q *query) unattributed(subject identity.Fingerprint, spans []interval.Span, why UnattributedReason, els []interval.Element, target func(interval.Element) identity.Fingerprint) {
	var cands []identity.Fingerprint
	for _, el := range els {
		if len(interval.Intersect(interval.Spans(el), spans)) > 0 && !slices.Contains(cands, target(el)) {
			cands = append(cands, target(el))
		}
	}
	slices.SortFunc(cands, store.CompareFingerprints)
	q.res.Unattributed = append(q.res.Unattributed, Unattributed{Subject: subject, Spans: spans, Reason: why, Candidates: cands})
}

// cap ranks fps by duration, descending, then by fingerprint, keeps the first
// limit of them, and adds the rest to dropped.
func (q *query) cap(fps []identity.Fingerprint, d map[identity.Fingerprint]time.Duration, limit int, dropped *[]identity.Fingerprint) []identity.Fingerprint {
	slices.SortFunc(fps, func(a, b identity.Fingerprint) int {
		if c := cmp.Compare(d[b], d[a]); c != 0 {
			return c
		}
		return store.CompareFingerprints(a, b)
	})
	if len(fps) > limit {
		*dropped = append(*dropped, fps[limit:]...)
		fps = fps[:limit]
	}
	return fps
}

// hops makes hops B to F.
func (q *query) hops() error {
	// Hop B: the pods on the seed's nodes.
	out, err := q.edges(q.n1s, store.Reverse)
	if err != nil {
		return err
	}
	for i, n := range q.n1s {
		for _, el := range q.pods(out[i]) {
			q.cands = append(q.cands, candidate{
				mech: SharedNode, peer: el.Subject.A, nodes: []identity.Fingerprint{n}, via: n,
				legs: []interval.Element{q.seedEl[n], el},
				eff:  [][]interval.Span{q.seedOn[n], interval.Spans(el)},
			})
		}
	}
	if q.nodeOnly {
		return nil
	}

	// Hop C: the hosts of the seed's nodes.
	if out, err = q.edges(q.n1s, store.Forward); err != nil {
		return err
	}
	hostTime := map[identity.Fingerprint]time.Duration{}
	var hs []identity.Fingerprint
	for i, n := range q.n1s {
		p := q.place(n, out[i])
		q.attributeNode(n, p)
		for h, on := range p.on {
			seedOnH := interval.Intersect(q.seedOn[n], on)
			if d := interval.Duration(seedOnH); d > 0 {
				q.res.SeedHosts = append(q.res.SeedHosts, HostSpans{Node: n, Host: h, Spans: seedOnH})
				if _, ok := hostTime[h]; !ok {
					hs = append(hs, h)
				}
				hostTime[h] += d
			}
		}
	}
	hs = q.cap(hs, hostTime, q.pol.MaxHosts, &q.res.Truncated.Hosts)
	if len(hs) == 0 {
		return nil
	}

	// Hop D: the other nodes on those hosts, over the seed's time there.
	if out, err = q.edges(hs, store.Reverse); err != nil {
		return err
	}
	nodeTime := map[identity.Fingerprint]time.Duration{}
	var n2s []identity.Fingerprint
	for i, h := range hs {
		for _, el := range out[i] {
			if el.Subject.Relation != catalog.RunsOn || el.Subject.A.Type() != catalog.K8sNode {
				continue
			}
			if el.Uncascaded != nil {
				q.quarantine(el)
				continue
			}
			n2 := el.Subject.A
			for _, n1 := range q.n1s {
				if n1 == n2 {
					continue
				}
				seedOnH := interval.Intersect(q.seedOn[n1], q.hosts[n1].on[h])
				if d := interval.Duration(interval.Intersect(interval.Spans(el), seedOnH)); d > 0 {
					if _, ok := nodeTime[n2]; !ok {
						n2s = append(n2s, n2)
					}
					nodeTime[n2] += d
				}
			}
		}
	}
	n2s = q.cap(n2s, nodeTime, q.pol.MaxNodes, &q.res.Truncated.Nodes)
	if len(n2s) == 0 {
		return nil
	}

	// Hop E: those nodes' hosts.
	if out, err = q.edges(n2s, store.Forward); err != nil {
		return err
	}
	for i, n := range n2s {
		q.place(n, out[i])
	}

	// Hop F: the pods on those nodes.
	if out, err = q.edges(n2s, store.Reverse); err != nil {
		return err
	}
	for i, n2 := range n2s {
		for _, el := range q.pods(out[i]) {
			for _, n1 := range q.n1s {
				if n1 == n2 {
					continue
				}
				for _, h := range hs {
					on1, on2 := q.hosts[n1].on[h], q.hosts[n2].on[h]
					if len(on1) == 0 || len(on2) == 0 {
						continue
					}
					q.cands = append(q.cands, candidate{
						mech: SharedHost, peer: el.Subject.A, nodes: []identity.Fingerprint{n1, n2}, via: h,
						legs: []interval.Element{q.seedEl[n1], q.hosts[n1].edge[h], el, q.hosts[n2].edge[h]},
						eff:  [][]interval.Span{q.seedOn[n1], on1, interval.Spans(el), on2},
					})
				}
			}
		}
	}
	return nil
}

// pods keeps the scheduled_on elements from a pod other than the seed, and
// puts aside those with a quarantined endpoint.
func (q *query) pods(els []interval.Element) []interval.Element {
	var out []interval.Element
	for _, el := range els {
		if el.Subject.Relation != catalog.ScheduledOn || el.Subject.A.Type() != catalog.K8sPod || el.Subject.A == q.seed {
			continue
		}
		if el.Uncascaded != nil {
			q.quarantine(el)
			continue
		}
		out = append(out, el)
	}
	return out
}

// place records a node's runs_on claims and, for each host, the instants at
// which the node claims it and no other host. A claim of a quarantined host
// is never lifted through, but it still counts as a claim.
func (q *query) place(n identity.Fingerprint, els []interval.Element) *placement {
	p := &placement{on: map[identity.Fingerprint][]interval.Span{}, edge: map[identity.Fingerprint]interval.Element{}}
	for _, el := range els {
		if el.Subject.Relation == catalog.RunsOn && el.Subject.B.Type() == catalog.Host {
			p.claims = append(p.claims, el)
		}
	}
	for i, el := range p.claims {
		if el.Uncascaded != nil {
			q.quarantine(el)
			continue
		}
		var others [][]interval.Span
		for j, o := range p.claims {
			if j != i {
				others = append(others, interval.Spans(o))
			}
		}
		if on := subtract(interval.Spans(el), union(others...)); len(on) > 0 {
			p.on[el.Subject.B] = on
			p.edge[el.Subject.B] = el
		}
	}
	q.hosts[n] = p
	return p
}

// attributeNode lists the seed's time on node n that has no single host.
func (q *query) attributeNode(n identity.Fingerprint, p *placement) {
	seed := q.seedOn[n]
	host := func(el interval.Element) identity.Fingerprint { return el.Subject.B }
	var all, several, alone [][]interval.Span
	var quarantinedClaims []interval.Element
	for i, el := range p.claims {
		mine := interval.Spans(el)
		all = append(all, mine)
		var others [][]interval.Span
		for j, o := range p.claims {
			if j != i {
				others = append(others, interval.Spans(o))
			}
		}
		rest := union(others...)
		several = append(several, interval.Intersect(mine, rest))
		if el.Uncascaded != nil {
			alone = append(alone, subtract(mine, rest))
			quarantinedClaims = append(quarantinedClaims, el)
		}
	}
	if s := subtract(seed, union(all...)); len(s) > 0 {
		q.res.Unattributed = append(q.res.Unattributed, Unattributed{Subject: n, Spans: s, Reason: NoHost})
	}
	if s := interval.Intersect(seed, union(several...)); len(s) > 0 {
		q.unattributed(n, s, SeveralHosts, p.claims, host)
	}
	if s := interval.Intersect(seed, union(alone...)); len(s) > 0 {
		q.unattributed(n, s, QuarantinedHost, quarantinedClaims, host)
	}
}

// overlaps derives a candidate's spans, with the leg pieces they lie in and
// why each ends.
func (q *query) overlaps(c candidate) ([]Overlap, error) {
	from, to := q.res.From, q.res.To
	var out []Overlap
	for _, s := range interval.Along(c.eff...) {
		o := Overlap{Span: s, Before: s.Start.Equal(from), Pieces: make([]int, len(c.legs))}
		for i, leg := range c.legs {
			k := slices.IndexFunc(leg.Pieces, func(p interval.Piece) bool {
				return !p.Start.After(s.Start) && !p.End.Before(s.End)
			})
			if k < 0 {
				return nil, fmt.Errorf("an overlap [%s, %s) of %s lies in no piece of leg %d: %w",
					formatTime(s.Start), formatTime(s.End), c.peer, i, errInternal)
			}
			o.Pieces[i] = k
			p := leg.Pieces[k]
			if !p.Before {
				o.Before = false
			}
			if s.End.Equal(to) {
				continue
			}
			switch {
			case p.End.Equal(s.End):
				o.Ends = append(o.Ends, End{Leg: i, Source: p.EndSource})
			case endsAt(c.eff[i], s):
				o.Ends = append(o.Ends, End{Leg: i, Source: lifecycle.EndUnknown, Unattributed: true})
			}
		}
		out = append(out, o)
	}
	return out, nil
}

// errInternal marks an answer that breaks this package's own invariants.
var errInternal = errors.New("internal inconsistency")

// endsAt reports whether the span of ss containing s ends where s does.
func endsAt(ss []interval.Span, s interval.Span) bool {
	for _, e := range ss {
		if !e.Start.After(s.Start) && !e.End.Before(s.End) {
			return e.End.Equal(s.End)
		}
	}
	return false
}

// finish derives the entries, applies the policy and the budget, and orders
// everything.
func (q *query) finish() error {
	from, to := q.res.From, q.res.To
	var kept, suppressed []CoResident
	for _, c := range q.cands {
		ovs, err := q.overlaps(c)
		if err != nil {
			return err
		}
		if len(ovs) == 0 {
			continue
		}
		e := CoResident{Mechanism: c.mech, Peer: c.peer, Nodes: c.nodes, Via: c.via, Legs: c.legs, Overlaps: ovs}
		ss := e.Spans()
		e.Duration = interval.Duration(ss)
		e.Class, e.Transitions = classOf(ss, from, to)
		open := ovs[0].Before || ovs[len(ovs)-1].End.Equal(to)
		if e.Duration < q.pol.MinOverlap && !open {
			e.Reason = BelowMinOverlap
			suppressed = append(suppressed, e)
			continue
		}
		kept = append(kept, e)
	}
	slices.SortFunc(kept, rank)
	slices.SortFunc(suppressed, rank)

	type via struct {
		fp   identity.Fingerprint
		mech Mechanism
	}
	per, dropped := map[via]int{}, map[via]int{}
	var within []CoResident
	for _, e := range kept {
		k := via{e.Via, e.Mechanism}
		if per[k] >= q.pol.MaxPerVia {
			dropped[k]++
			continue
		}
		per[k]++
		within = append(within, e)
	}
	tr := &q.res.Truncated
	for k, n := range dropped {
		tr.PerVia = append(tr.PerVia, ViaCount{Via: k.fp, Mechanism: k.mech, Dropped: n})
	}
	slices.SortFunc(tr.PerVia, func(a, b ViaCount) int {
		if c := store.CompareFingerprints(a.Via, b.Via); c != 0 {
			return c
		}
		return cmp.Compare(a.Mechanism, b.Mechanism)
	})
	if len(within) > q.pol.MaxResults {
		tr.Results = len(within) - q.pol.MaxResults
		within = within[:q.pol.MaxResults]
	}
	if len(suppressed) > q.pol.MaxResults {
		tr.SuppressedDropped = len(suppressed) - q.pol.MaxResults
		suppressed = suppressed[:q.pol.MaxResults]
	}
	q.res.CoResidents, q.res.Suppressed = within, suppressed
	slices.SortFunc(tr.Nodes, store.CompareFingerprints)
	slices.SortFunc(tr.Hosts, store.CompareFingerprints)

	slices.SortFunc(q.res.Unattributed, func(a, b Unattributed) int {
		if c := store.CompareFingerprints(a.Subject, b.Subject); c != 0 {
			return c
		}
		if c := a.Spans[0].Start.Compare(b.Spans[0].Start); c != 0 {
			return c
		}
		return cmp.Compare(a.Reason, b.Reason)
	})
	slices.SortFunc(q.res.SeedHosts, func(a, b HostSpans) int {
		if c := store.CompareFingerprints(a.Node, b.Node); c != 0 {
			return c
		}
		return store.CompareFingerprints(a.Host, b.Host)
	})
	for _, el := range q.quarantined {
		q.res.Quarantined = append(q.res.Quarantined, el)
	}
	slices.SortFunc(q.res.Quarantined, func(a, b interval.Element) int { return compareSubjects(a.Subject, b.Subject) })
	return nil
}

// rank orders entries: total duration descending, then shared node before
// shared host, then peer, then the path's nodes, then Via.
func rank(a, b CoResident) int {
	if c := cmp.Compare(b.Duration, a.Duration); c != 0 {
		return c
	}
	if c := cmp.Compare(a.Mechanism, b.Mechanism); c != 0 {
		return c
	}
	if c := store.CompareFingerprints(a.Peer, b.Peer); c != 0 {
		return c
	}
	if c := slices.CompareFunc(a.Nodes, b.Nodes, store.CompareFingerprints); c != 0 {
		return c
	}
	return store.CompareFingerprints(a.Via, b.Via)
}

func compareSubjects(a, b store.Subject) int {
	if c := store.CompareFingerprints(a.A, b.A); c != 0 {
		return c
	}
	if c := store.CompareFingerprints(a.B, b.B); c != 0 {
		return c
	}
	return cmp.Compare(a.Relation, b.Relation)
}
