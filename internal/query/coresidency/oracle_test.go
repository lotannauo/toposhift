package coresidency_test

import (
	"cmp"
	"slices"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/coresidency"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
)

// The oracle answers a co-residency query straight from the definitions, by
// probing liveness at every instant where it can change. It shares no code with
// lifecycle.Fold, with package interval's non-test code, or with this package.
// Its liveness core (deadline, latest, liveAt, critical, endCause, pieces) is
// copied from package interval's test oracle.

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

// oracleRank is the ranking, from its definition.
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
// nothing and MinOverlap 0, and the reads it must make.
func (o *oracle) find(seed identity.Fingerprint, nodeOnly bool) (coresidency.Result, coresidency.Reads) {
	res := coresidency.Result{Seed: seed, From: o.from, To: o.to, PolicyVersion: coresidency.PolicyVersion, IntervalPolicy: interval.PolicyVersion}
	seedEdges := o.edgesOf(o.out, seed, catalog.ScheduledOn, catalog.K8sNode, true)
	seedCrit := o.crit(seedEdges...)
	quarantined := map[store.Subject]bool{}
	note := func(es []store.Subject) {
		for _, e := range es {
			if o.uncascaded(e) != nil && o.hasOwn(e) {
				quarantined[e] = true
			}
		}
	}
	note(seedEdges)

	// The seed's attributed nodes, and its ambiguous time.
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
		note(o.edgesOf(o.in, n, catalog.ScheduledOn, catalog.K8sPod, false))
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
			note(claims)
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
			into := o.edgesOf(o.in, h, catalog.RunsOn, catalog.K8sNode, false)
			note(into)
			for _, e := range into {
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
			note(o.edgesOf(o.out, n2, catalog.RunsOn, catalog.Host, true))
			note(o.edgesOf(o.in, n2, catalog.ScheduledOn, catalog.K8sPod, false))
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
	for e := range quarantined {
		res.Quarantined = append(res.Quarantined, interval.Element{Subject: e})
	}
	slices.SortFunc(res.Quarantined, func(a, b interval.Element) int {
		if c := store.CompareFingerprints(a.Subject.A, b.Subject.A); c != 0 {
			return c
		}
		if c := store.CompareFingerprints(a.Subject.B, b.Subject.B); c != 0 {
			return c
		}
		return cmp.Compare(a.Subject.Relation, b.Subject.Relation)
	})

	// The reads, by the budget's formula: per call, one Window per fingerprint
	// and layer read, and the distinct endpoints of the edges with an interval
	// of their own in the window.
	var reads coresidency.Reads
	call := func(fps []identity.Fingerprint, m map[identity.Fingerprint][]store.Subject, layers ...catalog.Layer) {
		reads.Calls++
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
	call([]identity.Fingerprint{seed}, o.out, catalog.L2)
	if len(n1s) > 0 {
		call(n1s, o.in, catalog.L2)
		if !nodeOnly {
			call(n1s, o.out, nodeForward()...)
			if len(hs) > 0 {
				call(hs, o.in, catalog.L1)
				if len(n2s) > 0 {
					call(n2s, o.out, nodeForward()...)
					call(n2s, o.in, catalog.L2)
				}
			}
		}
	}
	return res, reads
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
