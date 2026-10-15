package extract_test

import (
	"cmp"
	"slices"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/coresidency"
	"github.com/lotannauo/toposhift/internal/query/extract"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
)

// The oracle answers an extraction straight from the definitions, by probing
// liveness at every instant where it can change. It shares no code with
// lifecycle.Fold, with the non-test code of packages interval and coresidency,
// or with this package. Its liveness core (deadline, latest, liveAt, critical,
// endCause, pieces) and its pod-level co-residency (find and what it calls) are
// copied from package coresidency's test oracle, which copied the core from
// package interval's.

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

// oracle holds the records a token sees, the entities the store quarantines at
// that token (quarantine is the store's to decide), and the window.
type oracle struct {
	bySub       map[store.Subject][]store.Record
	q           map[identity.Fingerprint]*store.QuarantineError
	from, to    time.Time
	out, in     map[identity.Fingerprint][]store.Subject // edges by source and by target
	legPieceMem map[store.Subject][]interval.Piece
	all         []time.Time // the critical instants of every subject
}

func newOracle(recs []store.Record, token uint64, from, to time.Time, q map[identity.Fingerprint]*store.QuarantineError) *oracle {
	o := &oracle{
		bySub: map[store.Subject][]store.Record{}, q: q, from: from.UTC(), to: to.UTC(),
		out: map[identity.Fingerprint][]store.Subject{}, in: map[identity.Fingerprint][]store.Subject{},
		legPieceMem: map[store.Subject][]interval.Piece{},
	}
	for _, r := range recs {
		if r.Seq > token {
			continue
		}
		if _, seen := o.bySub[r.Subject]; !seen && r.Subject.Kind == store.SubjectEdge {
			o.out[r.Subject.A] = append(o.out[r.Subject.A], r.Subject)
			o.in[r.Subject.B] = append(o.in[r.Subject.B], r.Subject)
		}
		o.bySub[r.Subject] = append(o.bySub[r.Subject], r)
	}
	var lists [][]store.Record
	for _, rs := range o.bySub {
		lists = append(lists, rs)
	}
	o.all = critical(lists, o.from, o.to)
	return o
}

func (o *oracle) live(sub store.Subject, x time.Time) bool {
	ok, _ := liveAt(o.bySub[sub], x)
	return ok
}

func (o *oracle) uncascaded(e store.Subject) *store.QuarantineError {
	if q := o.q[e.A]; q != nil {
		return q
	}
	return o.q[e.B]
}

// edgeAlive is the cascade: the edge and both ends, or the edge alone when an
// end is quarantined.
func (o *oracle) edgeAlive(e store.Subject, x time.Time) bool {
	if o.uncascaded(e) != nil {
		return o.live(e, x)
	}
	return o.live(e, x) && o.live(store.EntitySubject(e.A), x) && o.live(store.EntitySubject(e.B), x)
}

// legPieces is the edge as Edges reports it.
func (o *oracle) legPieces(e store.Subject) []interval.Piece {
	if ps, ok := o.legPieceMem[e]; ok {
		return ps
	}
	var ends [][]store.Record
	if o.uncascaded(e) == nil {
		ends = [][]store.Record{o.bySub[store.EntitySubject(e.A)], o.bySub[store.EntitySubject(e.B)]}
	}
	ps := pieces(o.bySub[e], ends, o.from, o.to)
	o.legPieceMem[e] = ps
	return ps
}

// hasOwn says the edge has an interval of its own in the window.
func (o *oracle) hasOwn(e store.Subject) bool { return len(pieces(o.bySub[e], nil, o.from, o.to)) > 0 }

func (o *oracle) edgesOf(m map[identity.Fingerprint][]store.Subject, fp identity.Fingerprint, rel catalog.RelationType, other catalog.EntityType, forward bool) []store.Subject {
	var out []store.Subject
	for _, e := range m[fp] {
		peer := e.B
		if !forward {
			peer = e.A
		}
		if e.Relation == rel && peer.Type() == other {
			out = append(out, e)
		}
	}
	return out
}

// nodesOf is the nodes the pod is scheduled on at x.
func (o *oracle) nodesOf(p identity.Fingerprint, x time.Time) []identity.Fingerprint {
	var out []identity.Fingerprint
	for _, e := range o.edgesOf(o.out, p, catalog.ScheduledOn, catalog.K8sNode, true) {
		if o.uncascaded(e) == nil && o.edgeAlive(e, x) {
			out = append(out, e.B)
		}
	}
	return out
}

// hostsOf is the hosts the node claims at x, a quarantined one included.
func (o *oracle) hostsOf(n identity.Fingerprint, x time.Time) []identity.Fingerprint {
	var out []identity.Fingerprint
	for _, e := range o.edgesOf(o.out, n, catalog.RunsOn, catalog.Host, true) {
		if o.edgeAlive(e, x) {
			out = append(out, e.B)
		}
	}
	return out
}

func (o *oracle) onHost(n, h identity.Fingerprint, x time.Time) bool {
	hs := o.hostsOf(n, x)
	return len(hs) == 1 && hs[0] == h && o.q[h] == nil
}

func only(fps []identity.Fingerprint, fp identity.Fingerprint) bool {
	return len(fps) == 1 && fps[0] == fp
}

// crit is the critical instants of the subjects and their endpoints.
func (o *oracle) crit(subs ...store.Subject) []time.Time {
	var lists [][]store.Record
	for _, s := range subs {
		lists = append(lists, o.bySub[s])
		if s.Kind == store.SubjectEdge {
			lists = append(lists, o.bySub[store.EntitySubject(s.A)], o.bySub[store.EntitySubject(s.B)])
		}
	}
	return critical(lists, o.from, o.to)
}

// probe is the spans of [From, To) over which pred holds, judged at crit.
func (o *oracle) probe(crit []time.Time, pred func(time.Time) bool) []interval.Span {
	var out []interval.Span
	for i, x := range crit {
		if !pred(x) {
			continue
		}
		end := o.to
		if i+1 < len(crit) {
			end = crit[i+1]
		}
		if n := len(out); n > 0 && out[n-1].End.Equal(x) {
			out[n-1].End = end
			continue
		}
		out = append(out, interval.Span{Start: x, End: end})
	}
	return out
}

// gather is every value of f at the critical instants where pred holds.
func gather(crit []time.Time, pred func(time.Time) bool, f func(time.Time) []identity.Fingerprint) []identity.Fingerprint {
	var out []identity.Fingerprint
	for _, x := range crit {
		if pred(x) {
			out = append(out, f(x)...)
		}
	}
	return sortedSet(out)
}

type leg struct {
	edge  store.Subject
	truth func(time.Time) bool
}

// entry builds a candidate's entry, or false if it has no overlap.
func (o *oracle) entry(mech coresidency.Mechanism, peer identity.Fingerprint, nodes []identity.Fingerprint, via identity.Fingerprint, crit []time.Time, legs []leg) (coresidency.CoResident, bool) {
	all := func(x time.Time) bool {
		for _, l := range legs {
			if !l.truth(x) {
				return false
			}
		}
		return true
	}
	spans := o.probe(crit, all)
	if len(spans) == 0 {
		return coresidency.CoResident{}, false
	}
	c := coresidency.CoResident{Mechanism: mech, Peer: peer, Nodes: nodes, Via: via}
	for _, l := range legs {
		c.Legs = append(c.Legs, interval.Element{Subject: l.edge, Layer: layerOf(l.edge), Pieces: o.legPieces(l.edge)})
	}
	for _, s := range spans {
		ov := coresidency.Overlap{Span: s, Before: s.Start.Equal(o.from)}
		for i, l := range legs {
			ps := o.legPieces(l.edge)
			k := slices.IndexFunc(ps, func(p interval.Piece) bool { return p.Contains(s.Start) })
			ov.Pieces = append(ov.Pieces, k)
			if !o.edgeAlive(l.edge, o.from.Add(-time.Nanosecond)) {
				ov.Before = false
			}
			if s.End.Equal(o.to) || l.truth(s.End) {
				continue
			}
			if o.edgeAlive(l.edge, s.End) {
				ov.Ends = append(ov.Ends, coresidency.End{Leg: i, Unattributed: true})
			} else {
				ov.Ends = append(ov.Ends, coresidency.End{Leg: i, Source: ps[k].EndSource})
			}
		}
		c.Overlaps = append(c.Overlaps, ov)
		c.Duration += s.End.Sub(s.Start)
	}
	c.Class, c.Transitions = spanClass(spans, o.from, o.to)
	return c, true
}

func spanClass(ss []interval.Span, from, to time.Time) (interval.Class, int) {
	n := 0
	for _, s := range ss {
		if s.Start.After(from) {
			n++
		}
		if s.End.Before(to) {
			n++
		}
	}
	k := len(ss)
	switch {
	case k == 0:
		return interval.ClassNone, n
	case k >= 2:
		return interval.ClassFlapping, n
	}
	a, b := ss[0].Start.Equal(from), ss[0].End.Equal(to)
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

// oracleRank is the ranking of entries, from its definition.
func oracleRank(a, b coresidency.CoResident) int {
	if a.Duration != b.Duration {
		return cmp.Compare(b.Duration, a.Duration)
	}
	if a.Mechanism != b.Mechanism {
		return cmp.Compare(a.Mechanism, b.Mechanism)
	}
	if c := store.CompareFingerprints(a.Peer, b.Peer); c != 0 {
		return c
	}
	if c := slices.CompareFunc(a.Nodes, b.Nodes, store.CompareFingerprints); c != 0 {
		return c
	}
	return store.CompareFingerprints(a.Via, b.Via)
}

func sortedSet(fps []identity.Fingerprint) []identity.Fingerprint {
	out := slices.Clone(fps)
	slices.SortFunc(out, store.CompareFingerprints)
	return slices.Compact(out)
}

// find is what Find must answer for seed, with caps large enough to truncate
// nothing and MinOverlap 0, the reads it must make, and the seed's attributed
// nodes.
func (o *oracle) find(seed identity.Fingerprint, nodeOnly bool) (coresidency.Result, coresidency.Reads, []identity.Fingerprint) {
	res := coresidency.Result{Seed: seed, From: o.from, To: o.to, PolicyVersion: coresidency.PolicyVersion, IntervalPolicy: interval.PolicyVersion}
	seedEdges := o.edgesOf(o.out, seed, catalog.ScheduledOn, catalog.K8sNode, true)
	seedCrit := o.crit(seedEdges...)

	var n1s []identity.Fingerprint
	for _, e := range seedEdges {
		n := e.B
		if len(o.probe(seedCrit, func(x time.Time) bool { return only(o.nodesOf(seed, x), n) })) > 0 {
			n1s = append(n1s, n)
		}
	}
	n1s = sortedSet(n1s)
	twoNodes := func(x time.Time) bool { return len(o.nodesOf(seed, x)) >= 2 }
	if amb := o.probe(seedCrit, twoNodes); len(amb) > 0 {
		several := gather(seedCrit, twoNodes, func(x time.Time) []identity.Fingerprint { return o.nodesOf(seed, x) })
		res.Unattributed = append(res.Unattributed, coresidency.Unattributed{Subject: seed, Spans: amb, Reason: coresidency.SeveralNodes, Candidates: several})
	}

	var cands []coresidency.CoResident
	add := func(c coresidency.CoResident, ok bool) {
		if ok {
			cands = append(cands, c)
		}
	}
	podsOn := func(n identity.Fingerprint) []store.Subject {
		var out []store.Subject
		for _, e := range o.edgesOf(o.in, n, catalog.ScheduledOn, catalog.K8sPod, false) {
			if e.A != seed && o.uncascaded(e) == nil {
				out = append(out, e)
			}
		}
		return out
	}
	for _, n := range n1s {
		for _, xe := range podsOn(n) {
			add(o.entry(coresidency.SharedNode, xe.A, []identity.Fingerprint{n}, n, o.crit(append(slices.Clone(seedEdges), xe)...), []leg{
				{sched(seed, n), func(x time.Time) bool { return only(o.nodesOf(seed, x), n) }},
				{xe, func(x time.Time) bool { return o.edgeAlive(xe, x) }},
			}))
		}
	}

	var hs, n2s []identity.Fingerprint
	if !nodeOnly && len(n1s) > 0 {
		for _, n := range n1s {
			claims := o.edgesOf(o.out, n, catalog.RunsOn, catalog.Host, true)
			crit := o.crit(append(slices.Clone(seedEdges), claims...)...)
			on := func(x time.Time) bool { return only(o.nodesOf(seed, x), n) }
			if s := o.probe(crit, func(x time.Time) bool { return on(x) && len(o.hostsOf(n, x)) == 0 }); len(s) > 0 {
				res.Unattributed = append(res.Unattributed, coresidency.Unattributed{Subject: n, Spans: s, Reason: coresidency.NoHost})
			}
			hostsAt := func(x time.Time) []identity.Fingerprint { return o.hostsOf(n, x) }
			several := func(x time.Time) bool { return on(x) && len(o.hostsOf(n, x)) >= 2 }
			if s := o.probe(crit, several); len(s) > 0 {
				res.Unattributed = append(res.Unattributed, coresidency.Unattributed{Subject: n, Spans: s, Reason: coresidency.SeveralHosts, Candidates: gather(crit, several, hostsAt)})
			}
			quarantinedOnly := func(x time.Time) bool {
				h := o.hostsOf(n, x)
				return on(x) && len(h) == 1 && o.q[h[0]] != nil
			}
			if s := o.probe(crit, quarantinedOnly); len(s) > 0 {
				res.Unattributed = append(res.Unattributed, coresidency.Unattributed{Subject: n, Spans: s, Reason: coresidency.QuarantinedHost, Candidates: gather(crit, quarantinedOnly, hostsAt)})
			}
			for _, ce := range claims {
				h := ce.B
				if s := o.probe(crit, func(x time.Time) bool { return on(x) && o.onHost(n, h, x) }); len(s) > 0 {
					hs = append(hs, h)
					res.SeedHosts = append(res.SeedHosts, coresidency.HostSpans{Node: n, Host: h, Spans: s})
				}
			}
		}
		hs = sortedSet(hs)
		for _, h := range hs {
			for _, e := range o.edgesOf(o.in, h, catalog.RunsOn, catalog.K8sNode, false) {
				n2 := e.A
				if o.uncascaded(e) != nil {
					continue
				}
				for _, n1 := range n1s {
					if n1 == n2 {
						continue
					}
					crit := o.crit(append(append(slices.Clone(seedEdges), o.edgesOf(o.out, n1, catalog.RunsOn, catalog.Host, true)...), e)...)
					if len(o.probe(crit, func(x time.Time) bool {
						return only(o.nodesOf(seed, x), n1) && o.onHost(n1, h, x) && o.edgeAlive(e, x)
					})) > 0 {
						n2s = append(n2s, n2)
					}
				}
			}
		}
		n2s = sortedSet(n2s)
		for _, n2 := range n2s {
			for _, xe := range podsOn(n2) {
				for _, n1 := range n1s {
					if n1 == n2 {
						continue
					}
					for _, h := range hs {
						subs := append(slices.Clone(seedEdges), xe)
						subs = append(subs, o.edgesOf(o.out, n1, catalog.RunsOn, catalog.Host, true)...)
						subs = append(subs, o.edgesOf(o.out, n2, catalog.RunsOn, catalog.Host, true)...)
						add(o.entry(coresidency.SharedHost, xe.A, []identity.Fingerprint{n1, n2}, h, o.crit(subs...), []leg{
							{sched(seed, n1), func(x time.Time) bool { return only(o.nodesOf(seed, x), n1) }},
							{runsOn(n1, h), func(x time.Time) bool { return o.onHost(n1, h, x) }},
							{xe, func(x time.Time) bool { return o.edgeAlive(xe, x) }},
							{runsOn(n2, h), func(x time.Time) bool { return o.onHost(n2, h, x) }},
						}))
					}
				}
			}
		}
	}
	slices.SortFunc(res.SeedHosts, func(a, b coresidency.HostSpans) int {
		if c := store.CompareFingerprints(a.Node, b.Node); c != 0 {
			return c
		}
		return store.CompareFingerprints(a.Host, b.Host)
	})
	slices.SortFunc(cands, oracleRank)
	res.CoResidents = cands
	slices.SortFunc(res.Unattributed, func(a, b coresidency.Unattributed) int {
		if c := store.CompareFingerprints(a.Subject, b.Subject); c != 0 {
			return c
		}
		if c := a.Spans[0].Start.Compare(b.Spans[0].Start); c != 0 {
			return c
		}
		return cmp.Compare(a.Reason, b.Reason)
	})

	var reads coresidency.Reads
	o.call(&reads, []identity.Fingerprint{seed}, o.out, catalog.L2)
	if len(n1s) > 0 {
		o.call(&reads, n1s, o.in, catalog.L2)
		if !nodeOnly {
			o.call(&reads, n1s, o.out, nodeForward()...)
			if len(hs) > 0 {
				o.call(&reads, hs, o.in, catalog.L1)
				if len(n2s) > 0 {
					o.call(&reads, n2s, o.out, nodeForward()...)
					o.call(&reads, n2s, o.in, catalog.L2)
				}
			}
		}
	}
	return res, reads, n1s
}

// call adds one interval call to reads by the budget's formula: one Window per
// distinct fingerprint and layer, and one existence read per distinct endpoint
// of the edges with an interval of their own in the window.
func (o *oracle) call(reads *coresidency.Reads, fps []identity.Fingerprint, m map[identity.Fingerprint][]store.Subject, layers ...catalog.Layer) {
	reads.Calls++
	fps = sortedSet(fps)
	reads.Windows += len(fps) * len(layers)
	ends := map[identity.Fingerprint]bool{}
	for _, fp := range fps {
		for _, e := range m[fp] {
			if slices.Contains(layers, layerOf(e)) && o.hasOwn(e) {
				ends[e.A], ends[e.B] = true, true
			}
		}
	}
	for e := range ends {
		reads.Alive++
		if o.q[e] == nil {
			reads.EntityWindows++
		}
	}
}

// nodeForward is the layers a node's forward edges are stored in, read from the
// catalog: one Window each.
func nodeForward() []catalog.Layer {
	var out []catalog.Layer
	for rel := range catalog.Default().Relations() {
		for ep := range rel.Endpoints() {
			if ep.From == catalog.K8sNode && !slices.Contains(out, rel.Layer()) {
				out = append(out, rel.Layer())
			}
		}
	}
	return out
}

// The extraction, from the definitions.

// claimsAt is what an instance's edges of one relation claim at x, a
// quarantined target included.
func (o *oracle) claimsAt(i identity.Fingerprint, rel catalog.RelationType, typ catalog.EntityType, x time.Time) []identity.Fingerprint {
	var out []identity.Fingerprint
	for _, e := range o.edgesOf(o.out, i, rel, typ, true) {
		if o.edgeAlive(e, x) {
			out = append(out, e.B)
		}
	}
	return out
}

// single says the instance claims target alone at x, and may follow it.
func (o *oracle) single(i, target identity.Fingerprint, rel catalog.RelationType, x time.Time) bool {
	return only(o.claimsAt(i, rel, target.Type(), x), target) && o.uncascaded(store.EdgeSubject(i, target, rel)) == nil
}

func (o *oracle) svcOf(i, s identity.Fingerprint, x time.Time) bool {
	return o.single(i, s, catalog.InstanceOf, x)
}

func (o *oracle) podOf(i, p identity.Fingerprint, x time.Time) bool {
	return o.single(i, p, catalog.PartOf, x)
}

// mapped says some service is mapped singly to instance i at x.
func (o *oracle) mappedSvc(i identity.Fingerprint, x time.Time) bool {
	c := o.claimsAt(i, catalog.InstanceOf, catalog.Service, x)
	return len(c) == 1 && o.uncascaded(store.EdgeSubject(i, c[0], catalog.InstanceOf)) == nil
}

// shown says the edge has pieces and no quarantined endpoint: an element the
// derivation uses.
func (o *oracle) shown(e store.Subject) bool {
	return o.uncascaded(e) == nil && len(o.legPieces(e)) > 0
}

func (o *oracle) element(e store.Subject) interval.Element {
	ps := o.legPieces(e)
	var ss []interval.Span
	for _, p := range ps {
		ss = append(ss, p.Span)
	}
	el := interval.Element{Subject: e, Layer: layerOf(e), Pieces: ps}
	el.Class, el.Transitions = spanClass(ss, o.from, o.to)
	return el
}

// podInstances is the instances part of pod p, as its reverse read lists them.
func (o *oracle) podInstances(p identity.Fingerprint) []identity.Fingerprint {
	var out []identity.Fingerprint
	for _, e := range o.edgesOf(o.in, p, catalog.PartOf, catalog.ServiceInstance, false) {
		if o.shown(e) {
			out = append(out, e.A)
		}
	}
	return sortedSet(out)
}

// services is the services instance i has a usable instance_of to.
func (o *oracle) services(i identity.Fingerprint) []identity.Fingerprint {
	var out []identity.Fingerprint
	for _, e := range o.edgesOf(o.out, i, catalog.InstanceOf, catalog.Service, true) {
		if o.shown(e) {
			out = append(out, e.B)
		}
	}
	return sortedSet(out)
}

type omember struct {
	extract.Member
	rank int
}

// ocontrib is a contribution with its truth at each instant.
type ocontrib struct {
	extract.Contribution
	mech         extract.Mechanism
	member, peer identity.Fingerprint
	legs         [4]func(time.Time) bool
	entry        *coresidency.CoResident
}

func (o *oracle) entryAt(e *coresidency.CoResident, x time.Time) bool {
	return slices.ContainsFunc(e.Overlaps, func(ov coresidency.Overlap) bool { return ov.Contains(x) })
}

func (o *oracle) truth(c *ocontrib, x time.Time) bool {
	for _, l := range c.legs {
		if !l(x) {
			return false
		}
	}
	return c.entry == nil || o.entryAt(c.entry, x)
}

// extract is what Extract must answer with caps that truncate nothing and the
// co-residency MinOverlap 0, and the reads it must make.
func (o *oracle) extract(seed extract.Seed, nodeOnly bool, pol extract.Policy) (extract.Result, coresidency.Reads) {
	var s identity.Fingerprint
	var anchor *extract.Anchored
	switch sd := seed.(type) {
	case extract.Exploratory:
		s = sd.Service
	case extract.Anchored:
		s, anchor = sd.Service, &sd
	}
	res := extract.Result{
		From: o.from, To: o.to, Seed: seed,
		PolicyVersion: extract.PolicyVersion, CoResidencyPolicy: coresidency.PolicyVersion, IntervalPolicy: interval.PolicyVersion,
	}
	var reads coresidency.Reads

	// Members.
	byService := map[identity.Fingerprint]*omember{s: {Member: extract.Member{Service: s, Role: extract.RoleSeed}}}
	for _, e := range o.edgesOf(o.in, s, catalog.DependsOn, catalog.Service, false) {
		if e.A != s && o.shown(e) {
			if byService[e.A] == nil {
				byService[e.A] = &omember{Member: extract.Member{Service: e.A}}
			}
			byService[e.A].Role |= extract.RoleCaller
			byService[e.A].Dependency = append(byService[e.A].Dependency, o.element(e))
		}
	}
	for _, e := range o.edgesOf(o.out, s, catalog.DependsOn, catalog.Service, true) {
		if e.B != s && o.shown(e) {
			if byService[e.B] == nil {
				byService[e.B] = &omember{Member: extract.Member{Service: e.B}}
			}
			byService[e.B].Role |= extract.RoleCallee
			byService[e.B].Dependency = append(byService[e.B].Dependency, o.element(e))
		}
	}
	depTime := func(m *omember) time.Duration {
		d := o.probe(o.all, func(x time.Time) bool {
			for _, el := range m.Dependency {
				if o.edgeAlive(el.Subject, x) {
					return true
				}
			}
			return false
		})
		return interval.Duration(d)
	}
	var members []*omember
	for fp, m := range byService {
		if fp != s {
			members = append(members, m)
		}
	}
	slices.SortFunc(members, func(a, b *omember) int {
		if c := cmp.Compare(depTime(b), depTime(a)); c != 0 {
			return c
		}
		return store.CompareFingerprints(a.Service, b.Service)
	})
	members = append([]*omember{byService[s]}, members...)
	for i, m := range members {
		m.rank = i
	}
	instancesOf := func(m identity.Fingerprint) []store.Subject {
		var out []store.Subject
		for _, e := range o.edgesOf(o.in, m, catalog.InstanceOf, catalog.ServiceInstance, false) {
			if o.shown(e) {
				out = append(out, e)
			}
		}
		return out
	}
	byService[s].Instances = nil
	for _, e := range instancesOf(s) {
		byService[s].Instances = append(byService[s].Instances, o.element(e))
	}
	o.call(&reads, []identity.Fingerprint{s}, o.in, catalog.L2, catalog.L3)
	o.call(&reads, []identity.Fingerprint{s}, o.out, catalog.L3)

	// The seed pods.
	type cand struct {
		pod    identity.Fingerprint
		rank   int
		useful time.Duration
	}
	rankCands := func(cs map[identity.Fingerprint]*cand) []*cand {
		out := make([]*cand, 0, len(cs))
		for _, c := range cs {
			out = append(out, c)
		}
		slices.SortFunc(out, func(a, b *cand) int {
			if a.rank != b.rank {
				return cmp.Compare(a.rank, b.rank)
			}
			if a.useful != b.useful {
				return cmp.Compare(b.useful, a.useful)
			}
			return store.CompareFingerprints(a.pod, b.pod)
		})
		return out
	}
	// useful is the time pod p has an instance mapped singly to m and to p.
	useful := func(p identity.Fingerprint, m identity.Fingerprint, insts []identity.Fingerprint) time.Duration {
		return interval.Duration(o.probe(o.all, func(x time.Time) bool {
			return slices.ContainsFunc(insts, func(i identity.Fingerprint) bool { return o.svcOf(i, m, x) && o.podOf(i, p, x) })
		}))
	}
	var fwd, podsRead []identity.Fingerprint
	var seedPods []identity.Fingerprint
	finds := map[identity.Fingerprint]coresidency.Result{}
	frontier := map[identity.Fingerprint]bool{}
	runFind := func(p identity.Fingerprint) coresidency.Result {
		fr, rd, _ := o.find(p, nodeOnly)
		fr.Token = res.Token
		reads.Calls += rd.Calls
		reads.Windows += rd.Windows
		reads.Alive += rd.Alive
		reads.EntityWindows += rd.EntityWindows
		finds[p] = fr
		res.PodResults = append(res.PodResults, fr)
		return fr
	}

	if anchor == nil {
		if len(members) > 1 {
			var nbrs []identity.Fingerprint
			for _, m := range members[1:] {
				nbrs = append(nbrs, m.Service)
				for _, e := range instancesOf(m.Service) {
					m.Instances = append(m.Instances, o.element(e))
				}
			}
			o.call(&reads, nbrs, o.in, catalog.L2, catalog.L3)
		}
		cands := map[identity.Fingerprint]*cand{}
		for _, m := range members {
			for _, el := range m.Instances {
				fwd = append(fwd, el.Subject.A)
			}
		}
		fwd = sortedSet(fwd)
		if len(fwd) > 0 {
			o.call(&reads, fwd, o.out, catalog.L2)
		}
		for _, m := range members {
			var insts []identity.Fingerprint
			for _, el := range m.Instances {
				insts = append(insts, el.Subject.A)
			}
			var pods []identity.Fingerprint
			for _, i := range insts {
				for _, e := range o.edgesOf(o.out, i, catalog.PartOf, catalog.K8sPod, true) {
					pods = append(pods, e.B)
				}
			}
			for _, p := range sortedSet(pods) {
				if _, ok := cands[p]; ok {
					continue
				}
				if d := useful(p, m.Service, insts); d > 0 {
					cands[p] = &cand{pod: p, rank: m.rank, useful: d}
				}
			}
		}
		for _, c := range rankCands(cands) {
			seedPods = append(seedPods, c.pod)
			runFind(c.pod)
		}
	} else {
		res.Anchor = extract.NotOnHost
		h, at := anchor.Host, anchor.At
		var nodes []identity.Fingerprint
		for _, e := range o.edgesOf(o.in, h, catalog.RunsOn, catalog.K8sNode, false) {
			if o.shown(e) && slices.ContainsFunc(o.legPieces(e), func(p interval.Piece) bool { return p.Contains(at) }) {
				nodes = append(nodes, e.A)
			}
		}
		nodes = sortedSet(nodes)
		o.call(&reads, []identity.Fingerprint{h}, o.in, catalog.L1)
		var pods, insts []identity.Fingerprint
		if len(nodes) > 0 {
			for _, n := range nodes {
				for _, e := range o.edgesOf(o.in, n, catalog.ScheduledOn, catalog.K8sPod, false) {
					if o.shown(e) && slices.ContainsFunc(o.legPieces(e), func(p interval.Piece) bool { return p.Contains(at) }) {
						pods = append(pods, e.A)
					}
				}
			}
			pods = sortedSet(pods)
			o.call(&reads, nodes, o.in, catalog.L2)
		}
		if len(pods) > 0 {
			podsRead = pods
			for _, p := range pods {
				insts = append(insts, o.podInstances(p)...)
			}
			insts = sortedSet(insts)
			o.call(&reads, pods, o.in, catalog.L2)
		}
		cands := map[identity.Fingerprint]*cand{}
		if len(insts) > 0 {
			fwd = insts
			o.call(&reads, insts, o.out, catalog.L2)
			for _, i := range insts {
				for _, u := range o.services(i) {
					if m := byService[u]; m != nil && !slices.ContainsFunc(m.Instances, func(el interval.Element) bool { return el.Subject.A == i }) {
						m.Instances = append(m.Instances, o.element(instOf(i, u)))
					}
				}
			}
			for _, m := range members {
				for _, p := range pods {
					if _, ok := cands[p]; ok {
						continue
					}
					pi := o.podInstances(p)
					if slices.ContainsFunc(pi, func(i identity.Fingerprint) bool { return o.svcOf(i, m.Service, at) && o.podOf(i, p, at) }) {
						cands[p] = &cand{pod: p, rank: m.rank, useful: useful(p, m.Service, pi)}
					}
				}
			}
		}
		ranked := rankCands(cands)
		if len(ranked) > 0 && ranked[0].rank == 0 {
			confirmed := false
			for _, c := range ranked {
				if c.rank > 0 && !confirmed {
					break
				}
				runFind(c.pod)
				ns := o.nodesOf(c.pod, at)
				if len(ns) == 1 && o.onHost(ns[0], h, at) {
					seedPods = append(seedPods, c.pod)
					confirmed = confirmed || c.rank == 0
				}
			}
			if confirmed {
				res.Anchor = extract.OnHost
			} else {
				res.Anchor = extract.HostUnattributed
				seedPods = nil
			}
		}
	}
	for _, m := range members {
		slices.SortFunc(m.Instances, func(a, b interval.Element) int { return store.CompareFingerprints(a.Subject.A, b.Subject.A) })
		res.Members = append(res.Members, m.Member)
	}

	// M1 and M2.
	var m1 []identity.Fingerprint
	for _, p := range seedPods {
		m1 = append(m1, p)
		for _, e := range finds[p].CoResidents {
			m1 = append(m1, e.Peer)
		}
	}
	m1 = slices.DeleteFunc(sortedSet(m1), func(p identity.Fingerprint) bool { return slices.Contains(podsRead, p) })
	if len(m1) > 0 {
		o.call(&reads, m1, o.in, catalog.L2)
		var m2 []identity.Fingerprint
		for _, p := range m1 {
			for _, i := range o.podInstances(p) {
				if !slices.Contains(fwd, i) {
					m2 = append(m2, i)
				}
			}
		}
		if m2 = sortedSet(m2); len(m2) > 0 {
			o.call(&reads, m2, o.out, catalog.L2)
			fwd = append(fwd, m2...)
		}
	}

	// The contributions.
	type mside struct{ inst, svc identity.Fingerprint }
	sides := func(p identity.Fingerprint, membersOnly bool) []mside {
		var out []mside
		for _, i := range o.podInstances(p) {
			if !slices.ContainsFunc(o.edgesOf(o.out, i, catalog.PartOf, catalog.K8sPod, true), func(e store.Subject) bool { return e.B == p && o.shown(e) }) {
				continue
			}
			for _, u := range o.services(i) {
				if !membersOnly || byService[u] != nil {
					out = append(out, mside{i, u})
				}
			}
		}
		return out
	}
	var contribs []*ocontrib
	contribute := func(mech extract.Mechanism, p, x identity.Fingerprint, a, b mside, via identity.Fingerprint, k int, e *coresidency.CoResident) {
		c := &ocontrib{
			Contribution: extract.Contribution{
				MemberPod: p, PeerPod: x, MemberInst: a.inst, PeerInst: b.inst, Via: via, Entry: k,
				Legs: []interval.Element{o.element(instOf(a.inst, a.svc)), o.element(partOf(a.inst, p)), o.element(partOf(b.inst, x)), o.element(instOf(b.inst, b.svc))},
			},
			mech: mech, member: a.svc, peer: b.svc, entry: e,
			legs: [4]func(time.Time) bool{
				func(t time.Time) bool { return o.svcOf(a.inst, a.svc, t) },
				func(t time.Time) bool { return o.podOf(a.inst, p, t) },
				func(t time.Time) bool { return o.podOf(b.inst, x, t) },
				func(t time.Time) bool { return o.svcOf(b.inst, b.svc, t) },
			},
		}
		if c.Spans = o.probe(o.all, func(t time.Time) bool { return o.truth(c, t) }); len(c.Spans) > 0 {
			contribs = append(contribs, c)
		}
	}
	for _, p := range seedPods {
		mine := sides(p, true)
		for _, a := range mine {
			for _, b := range sides(p, false) {
				if a.inst != b.inst && a.svc != b.svc {
					contribute(extract.SharedPod, p, p, a, b, p, -1, nil)
				}
			}
		}
		fr := finds[p]
		for k := range fr.CoResidents {
			e := &fr.CoResidents[k]
			mech := extract.SharedNode
			if e.Mechanism == coresidency.SharedHost {
				mech = extract.SharedHost
			}
			for _, a := range mine {
				for _, b := range sides(e.Peer, false) {
					if a.svc != b.svc {
						contribute(mech, p, e.Peer, a, b, e.Via, k, e)
					}
				}
			}
		}
		_, _, n1s := o.find(p, nodeOnly)
		for _, n := range n1s {
			frontier[n] = true
		}
	}

	// Ubiquity.
	f := len(frontier)
	judged := anchor == nil && f >= pol.UbiquityMinNodes
	res.Ubiquity = extract.Ubiquity{Judged: judged, FrontierNodes: f}
	covered := map[identity.Fingerprint]map[identity.Fingerprint]bool{}
	for _, c := range contribs {
		if c.mech == extract.SharedNode && frontier[c.Via] {
			if covered[c.peer] == nil {
				covered[c.peer] = map[identity.Fingerprint]bool{}
			}
			covered[c.peer][c.Via] = true
		}
	}

	// The pairs.
	type key struct {
		m, p identity.Fingerprint
		mech extract.Mechanism
	}
	groups := map[key][]*ocontrib{}
	for _, c := range contribs {
		m, p := c.member, c.peer
		if byService[p] != nil && store.CompareFingerprints(p, m) < 0 {
			m, p = p, m
		}
		k := key{m, p, c.mech}
		groups[k] = append(groups[k], c)
	}
	bestRank := func(pr extract.Pair) int {
		r := byService[pr.Member].rank
		if m := byService[pr.Peer]; m != nil {
			r = min(r, m.rank)
		}
		return r
	}
	for k, cs := range groups {
		pr := extract.Pair{Member: k.m, Peer: k.p, Mechanism: k.mech, Vias: map[identity.Fingerprint][]interval.Span{}}
		for _, c := range cs {
			pr.Contributions = append(pr.Contributions, c.Contribution)
		}
		for _, v := range func() []identity.Fingerprint {
			var vs []identity.Fingerprint
			for _, c := range cs {
				vs = append(vs, c.Via)
			}
			return sortedSet(vs)
		}() {
			pr.Vias[v] = o.probe(o.all, func(t time.Time) bool {
				return slices.ContainsFunc(cs, func(c *ocontrib) bool { return c.Via == v && o.truth(c, t) })
			})
		}
		spans := o.probe(o.all, func(t time.Time) bool {
			return slices.ContainsFunc(cs, func(c *ocontrib) bool { return o.truth(c, t) })
		})
		for _, s := range spans {
			span := extract.Span{Span: s}
			for ci, c := range cs {
				for _, cspan := range c.Spans {
					if s.Start.Equal(o.from) && cspan.Start.Equal(o.from) {
						b := true
						for _, l := range c.Legs {
							b = b && o.edgeAlive(l.Subject, o.from.Add(-time.Nanosecond))
						}
						if c.entry != nil {
							i := slices.IndexFunc(c.entry.Overlaps, func(ov coresidency.Overlap) bool { return ov.Contains(o.from) })
							b = b && c.entry.Overlaps[i].Before
						}
						span.Before = span.Before || b
					}
					if s.End.Equal(o.to) || !cspan.End.Equal(s.End) {
						continue
					}
					for j, l := range c.legs {
						if l(s.End) {
							continue
						}
						leg := c.Legs[j]
						if o.edgeAlive(leg.Subject, s.End) {
							span.Ends = append(span.Ends, extract.End{Contribution: ci, Leg: j, Unattributed: true})
							continue
						}
						k := slices.IndexFunc(leg.Pieces, func(p interval.Piece) bool { return p.End.Equal(s.End) })
						span.Ends = append(span.Ends, extract.End{Contribution: ci, Leg: j, Source: leg.Pieces[k].EndSource})
					}
					if c.entry != nil && !o.entryAt(c.entry, s.End) {
						i := slices.IndexFunc(c.entry.Overlaps, func(ov coresidency.Overlap) bool { return ov.End.Equal(s.End) })
						for _, pe := range c.entry.Overlaps[i].Ends {
							span.Ends = append(span.Ends, extract.End{Contribution: ci, Leg: 4 + pe.Leg, Pod: &pe})
						}
					}
				}
			}
			pr.Spans = append(pr.Spans, span)
		}
		pr.Duration = interval.Duration(spans)
		pr.Class, pr.Transitions = spanClass(spans, o.from, o.to)
		// A member is never judged; a peer that is not one is.
		ubiq := false
		if byService[k.p] == nil {
			n := len(covered[k.p])
			ubiq = judged && n*pol.UbiquityDen >= pol.UbiquityNum*f
			if judged {
				pr.Coverage = [2]int{n, f}
			}
		}
		if ubiq {
			pr.Reason = coresidency.Ubiquitous
			res.Suppressed = append(res.Suppressed, pr)
		} else {
			res.Pairs = append(res.Pairs, pr)
		}
	}
	rank := func(a, b extract.Pair) int {
		if a.Duration != b.Duration {
			return cmp.Compare(b.Duration, a.Duration)
		}
		if a.Mechanism != b.Mechanism {
			return cmp.Compare(a.Mechanism, b.Mechanism)
		}
		if ra, rb := bestRank(a), bestRank(b); ra != rb {
			return cmp.Compare(ra, rb)
		}
		if c := store.CompareFingerprints(a.Peer, b.Peer); c != 0 {
			return c
		}
		return store.CompareFingerprints(a.Member, b.Member)
	}
	slices.SortFunc(res.Pairs, rank)
	slices.SortFunc(res.Suppressed, rank)

	// Unmapped.
	var peers []identity.Fingerprint
	for _, p := range seedPods {
		for _, e := range finds[p].CoResidents {
			peers = append(peers, e.Peer)
		}
	}
	for _, x := range sortedSet(peers) {
		inEntry := func(t time.Time) bool {
			for _, p := range seedPods {
				for k := range finds[p].CoResidents {
					if e := &finds[p].CoResidents[k]; e.Peer == x && o.entryAt(e, t) {
						return true
					}
				}
			}
			return false
		}
		insts := o.podInstances(x)
		if ss := o.probe(o.all, func(t time.Time) bool {
			return inEntry(t) && !slices.ContainsFunc(insts, func(i identity.Fingerprint) bool { return o.podOf(i, x, t) && o.mappedSvc(i, t) })
		}); len(ss) > 0 {
			res.Unmapped = append(res.Unmapped, extract.Unmapped{Pod: x, Spans: ss})
		}
	}

	// Unattributed: the instances read forward, then every Find's.
	for _, i := range sortedSet(fwd) {
		for _, r := range []struct {
			why extract.UnattributedReason
			rel catalog.RelationType
			typ catalog.EntityType
		}{{extract.SeveralPods, catalog.PartOf, catalog.K8sPod}, {extract.SeveralServices, catalog.InstanceOf, catalog.Service}} {
			several := func(t time.Time) bool { return len(o.claimsAt(i, r.rel, r.typ, t)) >= 2 }
			if ss := o.probe(o.all, several); len(ss) > 0 {
				res.Unattributed = append(res.Unattributed, extract.Unattributed{
					Subject: i, Spans: ss, Mapping: r.why,
					Candidates: gather(o.all, several, func(t time.Time) []identity.Fingerprint { return o.claimsAt(i, r.rel, r.typ, t) }),
				})
			}
		}
	}
	slices.SortStableFunc(res.Unattributed, func(a, b extract.Unattributed) int {
		if c := store.CompareFingerprints(a.Subject, b.Subject); c != 0 {
			return c
		}
		if c := a.Spans[0].Start.Compare(b.Spans[0].Start); c != 0 {
			return c
		}
		return cmp.Compare(a.Mapping, b.Mapping)
	})
	for _, fr := range res.PodResults {
		for _, u := range fr.Unattributed {
			res.Unattributed = append(res.Unattributed, extract.Unattributed{
				SeedPod: fr.Seed, Subject: u.Subject, Spans: u.Spans, Placement: u.Reason, Candidates: u.Candidates,
			})
		}
	}
	return res, reads
}
