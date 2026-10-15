package blastradius_test

import (
	"cmp"
	"slices"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/blastradius"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
)

// The oracle answers a walk straight from the definitions, instant by instant:
// at every instant where liveness can change it builds the graph of the edges
// live there and walks it breadth first from the seed. It shares no code with
// lifecycle.Fold, with the non-test code of package interval or package
// coresidency, or with this package: it uses only package interval's types
// (Span, Piece, Element, Class) and the catalog, and reads the layers an
// entity's edges are stored in from the catalog itself. Its liveness core
// (deadline, latest, liveAt, critical, endCause, pieces) is copied from package
// interval's test oracle.

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

// The definitions of the walk, written again here.

func propagatesRel(rel catalog.RelationType) bool {
	r, ok := catalog.Default().Relation(rel)
	return ok && (r.Propagation() == catalog.PropagateDown || r.Propagation() == catalog.PropagateUp)
}

func singleClaimRel(rel catalog.RelationType) bool {
	return rel == catalog.RunsOn || rel == catalog.LocatedIn
}

// layersOf is, from the catalog alone, the layers edges of type t are stored
// in when read in dir: the layer of every stored relation with an endpoint
// whose To (Reverse) or From (Forward) is t.
func layersOf(t catalog.EntityType, dir store.Direction) []catalog.Layer {
	var out []catalog.Layer
	for rel := range catalog.Default().Relations() {
		if rel.Derived() {
			continue
		}
		for ep := range rel.Endpoints() {
			end := ep.To
			if dir == store.Forward {
				end = ep.From
			}
			if end == t && !slices.Contains(out, rel.Layer()) {
				out = append(out, rel.Layer())
			}
		}
	}
	return out
}

func reverseLayers(fp identity.Fingerprint) int { return len(layersOf(fp.Type(), store.Reverse)) }

func forwardLayers(fp identity.Fingerprint) int { return len(layersOf(fp.Type(), store.Forward)) }

// duration is the total length of spans.
func duration(ss []interval.Span) time.Duration {
	var d time.Duration
	for _, s := range ss {
		d += s.End.Sub(s.Start)
	}
	return d
}

// oracle holds the records a token sees, the entities the store quarantines at
// that token (quarantine is the store's to decide), the window, the critical
// instants of every subject in it, and each subject's own liveness at each.
type oracle struct {
	bySub     map[store.Subject][]store.Record
	q         map[identity.Fingerprint]*store.QuarantineError
	from, to  time.Time
	out, in   map[identity.Fingerprint][]store.Subject // edges by source and by target
	crit      []time.Time
	live      map[store.Subject][]bool
	legMem    map[store.Subject][]interval.Piece
	retMem    map[store.Subject]bool
	followMem map[store.Subject][]bool
}

func newOracle(recs []store.Record, token uint64, from, to time.Time, q map[identity.Fingerprint]*store.QuarantineError) *oracle {
	o := &oracle{
		bySub: map[store.Subject][]store.Record{}, q: q, from: from.UTC(), to: to.UTC(),
		out: map[identity.Fingerprint][]store.Subject{}, in: map[identity.Fingerprint][]store.Subject{},
		live: map[store.Subject][]bool{}, legMem: map[store.Subject][]interval.Piece{}, retMem: map[store.Subject]bool{},
		followMem: map[store.Subject][]bool{},
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
	o.crit = critical(lists, o.from, o.to)
	// A subject's liveness is constant between its own critical instants, so
	// it is judged at those and read off at every other.
	for sub, rs := range o.bySub {
		own := critical([][]store.Record{rs}, o.from, o.to)
		vals := make([]bool, len(own))
		for k, x := range own {
			vals[k], _ = liveAt(rs, x)
		}
		lv := make([]bool, len(o.crit))
		k := 0
		for i, x := range o.crit {
			for k+1 < len(own) && !own[k+1].After(x) {
				k++
			}
			lv[i] = vals[k]
		}
		o.live[sub] = lv
	}
	return o
}

func (o *oracle) liveIdx(sub store.Subject, i int) bool {
	lv, ok := o.live[sub]
	return ok && lv[i]
}

func (o *oracle) uncascaded(e store.Subject) *store.QuarantineError {
	if q := o.q[e.A]; q != nil {
		return q
	}
	return o.q[e.B]
}

// edgeAlive is the cascade: the edge and both ends, or the edge alone when an
// end is quarantined.
func (o *oracle) edgeAlive(e store.Subject, i int) bool {
	if o.uncascaded(e) != nil {
		return o.liveIdx(e, i)
	}
	return o.liveIdx(e, i) && o.liveIdx(store.EntitySubject(e.A), i) && o.liveIdx(store.EntitySubject(e.B), i)
}

// hasOwn says the edge has an interval of its own in the window.
func (o *oracle) hasOwn(e store.Subject) bool { return slices.Contains(o.live[e], true) }

// returned says interval returns the edge with a piece and it is followed:
// its relation propagates and no endpoint is quarantined.
func (o *oracle) returned(e store.Subject) bool {
	if r, ok := o.retMem[e]; ok {
		return r
	}
	r := false
	if propagatesRel(e.Relation) && o.uncascaded(e) == nil {
		for i := range o.crit {
			if o.edgeAlive(e, i) {
				r = true
				break
			}
		}
	}
	o.retMem[e] = r
	return r
}

// claims is the targets of e's from entity's live claims at i of e's relation
// and target type, e's own target included when e is live.
func (o *oracle) claims(e store.Subject, i int) []identity.Fingerprint {
	var out []identity.Fingerprint
	for _, c := range o.out[e.A] {
		if c.Relation == e.Relation && c.B.Type() == e.B.Type() && o.edgeAlive(c, i) {
			out = append(out, c.B)
		}
	}
	return out
}

// livePropagating says e is live at i, propagates, and has no quarantined end.
func (o *oracle) livePropagating(e store.Subject, i int) bool {
	return propagatesRel(e.Relation) && o.uncascaded(e) == nil && o.edgeAlive(e, i)
}

// follow says the walk follows e at i: live, propagating, unquarantined, and
// for a single-claim relation, its from entity's only claim of that relation
// and target type.
func (o *oracle) follow(e store.Subject, i int) bool {
	f, ok := o.followMem[e]
	if !ok {
		// The answer does not depend on the seed, so it is judged once per edge.
		f = make([]bool, len(o.crit))
		for k := range o.crit {
			f[k] = o.livePropagating(e, k) && (!singleClaimRel(e.Relation) || len(o.claims(e, k)) == 1)
		}
		o.followMem[e] = f
	}
	return f[i]
}

// bfs is the walk at instant i: the least hops from seed to every entity it
// reaches within maxDepth, the seed at 0 and never entered again, and the
// excluded entity (the zero fingerprint excludes none) never entered.
func (o *oracle) bfs(seed, exclude identity.Fingerprint, maxDepth, i int) map[identity.Fingerprint]int {
	depth := map[identity.Fingerprint]int{seed: 0}
	level := []identity.Fingerprint{seed}
	for d := 1; d <= maxDepth && len(level) > 0; d++ {
		var next []identity.Fingerprint
		for _, x := range level {
			for _, e := range o.in[x] {
				if !o.follow(e, i) || e.A == exclude {
					continue
				}
				if _, seen := depth[e.A]; !seen {
					depth[e.A] = d
					next = append(next, e.A)
				}
			}
		}
		level = next
	}
	return depth
}

// probe turns per-instant flags into spans of [From, To).
func (o *oracle) probe(flags []bool) []interval.Span {
	var out []interval.Span
	for i, x := range o.crit {
		if !flags[i] {
			continue
		}
		end := o.to
		if i+1 < len(o.crit) {
			end = o.crit[i+1]
		}
		if n := len(out); n > 0 && out[n-1].End.Equal(x) {
			out[n-1].End = end
			continue
		}
		out = append(out, interval.Span{Start: x, End: end})
	}
	return out
}

// legPieces is the edge as interval reads it, with the cascade unless an end
// is quarantined.
func (o *oracle) legPieces(e store.Subject) []interval.Piece {
	if ps, ok := o.legMem[e]; ok {
		return ps
	}
	var ends [][]store.Record
	if o.uncascaded(e) == nil {
		ends = [][]store.Record{o.bySub[store.EntitySubject(e.A)], o.bySub[store.EntitySubject(e.B)]}
	}
	ps := pieces(o.bySub[e], ends, o.from, o.to)
	o.legMem[e] = ps
	return ps
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

func sortedSet(fps []identity.Fingerprint) []identity.Fingerprint {
	out := slices.Clone(fps)
	slices.SortFunc(out, store.CompareFingerprints)
	return slices.Compact(out)
}

func subjectOrder(a, b store.Subject) int {
	if c := store.CompareFingerprints(a.A, b.A); c != 0 {
		return c
	}
	if c := store.CompareFingerprints(a.B, b.B); c != 0 {
		return c
	}
	return cmp.Compare(a.Relation, b.Relation)
}

type oracleArrival struct {
	y, x identity.Fingerprint
	rel  catalog.RelationType
}

type oracleUnattr struct {
	y      identity.Fingerprint
	rel    catalog.RelationType
	target catalog.EntityType
}

// impact is what Impact must answer for seed with caps that cut nothing, and
// the reads it must make by the budget's formula.
func (o *oracle) impact(seed identity.Fingerprint, maxDepth int) (blastradius.Result, blastradius.Reads) {
	return o.impactRestricted(seed, maxDepth, nil, identity.Fingerprint{})
}

// countCall adds one interval call over fps to reads, by the budget's formula:
// one Window per edge layer of each, and per distinct endpoint of the edges
// with an interval of their own in the window, one Alive and, unless it is
// quarantined, one EntityWindow. Quarantined propagating edges are put in
// quarantined.
func (o *oracle) countCall(reads *blastradius.Reads, quarantined map[store.Subject]bool, fps []identity.Fingerprint, m map[identity.Fingerprint][]store.Subject, layers func(identity.Fingerprint) int) {
	reads.Calls++
	ends := map[identity.Fingerprint]bool{}
	for _, fp := range fps {
		reads.Windows += layers(fp)
		for _, e := range m[fp] {
			if !o.hasOwn(e) {
				continue
			}
			ends[e.A], ends[e.B] = true, true
			if propagatesRel(e.Relation) && o.uncascaded(e) != nil {
				quarantined[e] = true
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

// impactRestricted is impact started over the critical instants flagged in
// start (all of them if nil), never entering exclude (the zero fingerprint
// excludes none).
func (o *oracle) impactRestricted(seed identity.Fingerprint, maxDepth int, start []bool, exclude identity.Fingerprint) (blastradius.Result, blastradius.Reads) {
	res := blastradius.Result{Seed: seed, From: o.from, To: o.to, PolicyVersion: blastradius.PolicyVersion, IntervalPolicy: interval.PolicyVersion}
	n := len(o.crit)
	flags := func(m map[identity.Fingerprint][]bool, k identity.Fingerprint) []bool {
		if m[k] == nil {
			m[k] = make([]bool, n)
		}
		return m[k]
	}
	memberAt := map[identity.Fingerprint][]bool{}
	least := map[identity.Fingerprint]int{}
	arrAt := map[oracleArrival][]bool{}
	unAt := map[oracleUnattr][]bool{}
	unCands := map[oracleUnattr][]identity.Fingerprint{}
	levels := make([]map[identity.Fingerprint]bool, maxDepth+1)
	for d := range levels {
		levels[d] = map[identity.Fingerprint]bool{}
	}
	levels[0][seed] = true
	for i := range o.crit {
		if start != nil && !start[i] {
			continue
		}
		depth := o.bfs(seed, exclude, maxDepth, i)
		for y, d := range depth {
			if y == seed {
				continue
			}
			flags(memberAt, y)[i] = true
			levels[d][y] = true
			if l, ok := least[y]; !ok || d < l {
				least[y] = d
			}
		}
		for x, d := range depth {
			if d >= maxDepth {
				continue
			}
			for _, e := range o.in[x] {
				if e.A == seed || (!exclude.IsZero() && e.A == exclude) {
					// An edge from the seed back into the walk is never followed,
					// so the seed is never a claimant either, and nor is the
					// excluded entity.
					continue
				}
				if o.follow(e, i) {
					k := oracleArrival{e.A, x, e.Relation}
					if arrAt[k] == nil {
						arrAt[k] = make([]bool, n)
					}
					arrAt[k][i] = true
					continue
				}
				if singleClaimRel(e.Relation) && o.livePropagating(e, i) {
					k := oracleUnattr{e.A, e.Relation, e.B.Type()}
					if unAt[k] == nil {
						unAt[k] = make([]bool, n)
					}
					unAt[k][i] = true
					unCands[k] = append(unCands[k], o.claims(e, i)...)
				}
			}
		}
	}

	for y, at := range memberAt {
		m := blastradius.Member{Entity: y, Depth: least[y], Reach: o.probe(at)}
		m.Duration = duration(m.Reach)
		m.Class, m.Transitions = spanClass(m.Reach, o.from, o.to)
		for k, at := range arrAt {
			if k.y != y {
				continue
			}
			rel, _ := catalog.Default().Relation(k.rel)
			e := store.EdgeSubject(k.y, k.x, k.rel)
			m.Arrivals = append(m.Arrivals, blastradius.Arrival{
				Parent: k.x, Relation: k.rel, Propagation: rel.Propagation(),
				Leg:   interval.Element{Subject: e, Layer: rel.Layer(), Pieces: o.legPieces(e)},
				Spans: o.probe(at),
			})
		}
		slices.SortFunc(m.Arrivals, func(a, b blastradius.Arrival) int {
			if c := store.CompareFingerprints(a.Parent, b.Parent); c != 0 {
				return c
			}
			return cmp.Compare(a.Relation, b.Relation)
		})
		res.Members = append(res.Members, m)
	}
	slices.SortFunc(res.Members, func(a, b blastradius.Member) int {
		if a.Depth != b.Depth {
			return cmp.Compare(a.Depth, b.Depth)
		}
		if a.Duration != b.Duration {
			return cmp.Compare(b.Duration, a.Duration)
		}
		return store.CompareFingerprints(a.Entity, b.Entity)
	})
	for k, at := range unAt {
		res.Unattributed = append(res.Unattributed, blastradius.Unattributed{
			Subject: k.y, Relation: k.rel, Target: k.target, Spans: o.probe(at), Candidates: sortedSet(unCands[k]),
		})
	}
	slices.SortFunc(res.Unattributed, func(a, b blastradius.Unattributed) int {
		if c := store.CompareFingerprints(a.Subject, b.Subject); c != 0 {
			return c
		}
		if c := cmp.Compare(a.Relation, b.Relation); c != 0 {
			return c
		}
		if c := cmp.Compare(a.Target, b.Target); c != 0 {
			return c
		}
		return a.Spans[0].Start.Compare(b.Spans[0].Start)
	})

	// The calls, and the reads by the budget's formula: per call, the distinct
	// endpoints of the edges with an interval of their own in the window.
	var reads blastradius.Reads
	quarantined := map[store.Subject]bool{}
	call := func(fps []identity.Fingerprint, m map[identity.Fingerprint][]store.Subject, layers func(identity.Fingerprint) int) {
		o.countCall(&reads, quarantined, fps, m, layers)
	}
	expandable := func(set map[identity.Fingerprint]bool) []identity.Fingerprint {
		var out []identity.Fingerprint
		for fp := range set {
			if reverseLayers(fp) > 0 {
				out = append(out, fp)
			}
		}
		return sortedSet(out)
	}
	frontier := []identity.Fingerprint{seed}
	for d := 1; d <= maxDepth && len(frontier) > 0; d++ {
		call(frontier, o.in, reverseLayers)
		var claimants []identity.Fingerprint
		for _, x := range frontier {
			for _, e := range o.in[x] {
				if singleClaimRel(e.Relation) && o.returned(e) && e.A != seed && (exclude.IsZero() || e.A != exclude) {
					claimants = append(claimants, e.A)
				}
			}
		}
		if claimants = sortedSet(claimants); len(claimants) > 0 {
			call(claimants, o.out, forwardLayers)
		}
		if d < maxDepth {
			frontier = expandable(levels[d])
		}
	}
	res.Truncated.Depth = expandable(levels[maxDepth])
	for e := range quarantined {
		res.Quarantined = append(res.Quarantined, interval.Element{Subject: e})
	}
	slices.SortFunc(res.Quarantined, func(a, b interval.Element) int { return subjectOrder(a.Subject, b.Subject) })
	return res, reads
}

// unionWalk is the members a union-graph walk gives: it follows every edge
// interval returns with a piece in the window, as if each were live
// throughout it, to maxDepth hops.
func (o *oracle) unionWalk(seed identity.Fingerprint, maxDepth int) map[identity.Fingerprint]bool {
	seen := map[identity.Fingerprint]bool{seed: true}
	out := map[identity.Fingerprint]bool{}
	level := []identity.Fingerprint{seed}
	for d := 1; d <= maxDepth && len(level) > 0; d++ {
		var next []identity.Fingerprint
		for _, x := range level {
			for _, e := range o.in[x] {
				if o.returned(e) && !seen[e.A] {
					seen[e.A], out[e.A] = true, true
					next = append(next, e.A)
				}
			}
		}
		level = next
	}
	return out
}

// upBFS is the upward walk at instant i: the least hops from the pod to every
// entity it is placed on within maxDepth, along the edges the walk follows, the
// pod at 0.
func (o *oracle) upBFS(pod identity.Fingerprint, maxDepth, i int) map[identity.Fingerprint]int {
	depth := map[identity.Fingerprint]int{pod: 0}
	level := []identity.Fingerprint{pod}
	for d := 1; d <= maxDepth && len(level) > 0; d++ {
		var next []identity.Fingerprint
		for _, x := range level {
			for _, e := range o.out[x] {
				if !o.follow(e, i) {
					continue
				}
				if _, seen := depth[e.B]; !seen {
					depth[e.B] = d
					next = append(next, e.B)
				}
			}
		}
		level = next
	}
	return depth
}

type oracleVia struct {
	from, to identity.Fingerprint
	rel      catalog.RelationType
}

// around is what Around must answer for pod with caps that cut nothing, racks
// and zones expanded only if expandFabric (nodes and hosts always are), and the
// reads it must make.
func (o *oracle) around(pod identity.Fingerprint, maxDepth int, expandFabric bool) (blastradius.AroundResult, blastradius.Reads) {
	res := blastradius.AroundResult{Seed: pod, From: o.from, To: o.to, PolicyVersion: blastradius.PolicyVersion, IntervalPolicy: interval.PolicyVersion}
	n := len(o.crit)
	onAt := map[identity.Fingerprint][]bool{}
	least := map[identity.Fingerprint]int{}
	viaAt := map[oracleVia][]bool{}
	unAt := map[oracleUnattr][]bool{}
	unCands := map[oracleUnattr][]identity.Fingerprint{}
	levels := make([]map[identity.Fingerprint]bool, maxDepth+1)
	for d := range levels {
		levels[d] = map[identity.Fingerprint]bool{}
	}
	levels[0][pod] = true
	for i := range o.crit {
		depth := o.upBFS(pod, maxDepth, i)
		for y, d := range depth {
			if y == pod {
				continue
			}
			if onAt[y] == nil {
				onAt[y] = make([]bool, n)
			}
			onAt[y][i] = true
			levels[d][y] = true
			if l, ok := least[y]; !ok || d < l {
				least[y] = d
			}
		}
		for x, d := range depth {
			if d >= maxDepth {
				continue
			}
			for _, e := range o.out[x] {
				if o.follow(e, i) {
					k := oracleVia{e.A, e.B, e.Relation}
					if viaAt[k] == nil {
						viaAt[k] = make([]bool, n)
					}
					viaAt[k][i] = true
					continue
				}
				if singleClaimRel(e.Relation) && o.livePropagating(e, i) {
					k := oracleUnattr{e.A, e.Relation, e.B.Type()}
					if unAt[k] == nil {
						unAt[k] = make([]bool, n)
					}
					unAt[k][i] = true
					unCands[k] = append(unCands[k], o.claims(e, i)...)
				}
			}
		}
	}

	for y, at := range onAt {
		s := blastradius.Substrate{Entity: y, Depth: least[y], On: o.probe(at)}
		s.Duration = duration(s.On)
		for k, at := range viaAt {
			if k.to != y {
				continue
			}
			e := store.EdgeSubject(k.from, k.to, k.rel)
			rel, _ := catalog.Default().Relation(k.rel)
			s.Vias = append(s.Vias, blastradius.Via{
				From: k.from, Relation: k.rel,
				Leg:   interval.Element{Subject: e, Layer: rel.Layer(), Pieces: o.legPieces(e)},
				Spans: o.probe(at),
			})
		}
		slices.SortFunc(s.Vias, func(a, b blastradius.Via) int {
			if c := store.CompareFingerprints(a.From, b.From); c != 0 {
				return c
			}
			return cmp.Compare(a.Relation, b.Relation)
		})
		res.Substrates = append(res.Substrates, s)
	}
	slices.SortFunc(res.Substrates, func(a, b blastradius.Substrate) int {
		if a.Depth != b.Depth {
			return cmp.Compare(a.Depth, b.Depth)
		}
		if a.Duration != b.Duration {
			return cmp.Compare(b.Duration, a.Duration)
		}
		return store.CompareFingerprints(a.Entity, b.Entity)
	})
	for k, at := range unAt {
		res.Unattributed = append(res.Unattributed, blastradius.Unattributed{
			Subject: k.y, Relation: k.rel, Target: k.target, Spans: o.probe(at), Candidates: sortedSet(unCands[k]),
		})
	}
	slices.SortFunc(res.Unattributed, func(a, b blastradius.Unattributed) int {
		if c := store.CompareFingerprints(a.Subject, b.Subject); c != 0 {
			return c
		}
		if c := cmp.Compare(a.Relation, b.Relation); c != 0 {
			return c
		}
		if c := cmp.Compare(a.Target, b.Target); c != 0 {
			return c
		}
		return a.Spans[0].Start.Compare(b.Spans[0].Start)
	})

	// The upward walk's calls: one per depth with a frontier entity that has a
	// forward edge layer, which every frontier entity has by construction.
	var reads blastradius.Reads
	quarantined := map[store.Subject]bool{}
	expandable := func(set map[identity.Fingerprint]bool) []identity.Fingerprint {
		var out []identity.Fingerprint
		for fp := range set {
			if forwardLayers(fp) > 0 {
				out = append(out, fp)
			}
		}
		return sortedSet(out)
	}
	frontier := []identity.Fingerprint{pod}
	for d := 1; d <= maxDepth && len(frontier) > 0; d++ {
		o.countCall(&reads, quarantined, frontier, o.out, forwardLayers)
		if d < maxDepth {
			frontier = expandable(levels[d])
		}
	}
	res.Truncated.Depth = expandable(levels[maxDepth])
	for e := range quarantined {
		res.Quarantined = append(res.Quarantined, interval.Element{Subject: e})
	}
	slices.SortFunc(res.Quarantined, func(a, b interval.Element) int { return subjectOrder(a.Subject, b.Subject) })

	// Each expanded substrate's walk, started over the pod's time on it and
	// without the pod.
	for i := range res.Substrates {
		s := &res.Substrates[i]
		if t := s.Entity.Type(); t != catalog.K8sNode && t != catalog.Host && !expandFabric {
			continue
		}
		r, rr := o.impactRestricted(s.Entity, maxDepth, onAt[s.Entity], pod)
		s.Expanded, s.Impact = true, r
		s.Impact.Reads = rr
		reads.Calls += rr.Calls
		reads.Windows += rr.Windows
		reads.Alive += rr.Alive
		reads.EntityWindows += rr.EntityWindows
	}
	return res, reads
}
