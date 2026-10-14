package blastradius

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
)

// Walker derives blast radii from a store, which it only reads.
type Walker struct{ st store.Store }

// NewWalker returns a walker over st.
func NewWalker(st store.Store) *Walker { return &Walker{st: st} }

// counting is the store a walk reads through: it counts the reads the budget
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

func (c *counting) Alive(ctx context.Context, fp identity.Fingerprint, t time.Time, s store.Scope) (bool, error) {
	c.count(func(r *Reads) { r.Alive++ })
	return c.Store.Alive(ctx, fp, t, s)
}

// Impact derives what a failing seed reaches in win: every entity a chain of
// at most MaxDepth edges, followed in reverse along relations whose failure
// propagates and all live at once, leads to from the seed, with the instants
// at which it does. Arguments are checked before any store call. After that,
// any error of a read fails the whole walk with a zero Result: there is no
// partial answer, and a retained layer gives an error wrapping
// interval.ErrRetained.
func (w *Walker) Impact(ctx context.Context, seed identity.Fingerprint, win interval.Window, o Options) (Result, error) {
	fail := func(err error) (Result, error) { return Result{}, fmt.Errorf("blastradius: Impact: %w", err) }
	if !win.From.Before(win.To) {
		return fail(fmt.Errorf("the window [%s, %s) is empty: %w", formatTime(win.From), formatTime(win.To), store.ErrInvalid))
	}
	cat := catalog.Default()
	if seed.IsZero() {
		return fail(fmt.Errorf("the seed is the zero fingerprint: %w", store.ErrInvalid))
	}
	if _, ok := cat.Entity(seed.Type()); !ok {
		return fail(fmt.Errorf("the seed %s has the unknown entity type %q: %w", seed, seed.Type(), store.ErrInvalid))
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
	cnt := &counting{Store: w.st}
	q := &walk{
		ctx: ctx, r: interval.NewReader(cnt, interval.Options{}), cat: cat, seed: seed, w: win, pol: pol,
		from: win.From.UTC(), to: win.To.UTC(),
		members: map[identity.Fingerprint]*member{}, claims: map[identity.Fingerprint][]interval.Element{},
		unattr: map[unattrKey][]interval.Span{}, quarantined: map[store.Subject]interval.Element{},
	}
	if err := q.run(); err != nil {
		return fail(err)
	}
	q.res.Reads = cnt.reads
	q.res.Reads.Calls = q.calls
	return q.res, nil
}

// walk is one Impact in progress.
type walk struct {
	ctx      context.Context
	r        *interval.Reader
	cat      *catalog.Catalog
	seed     identity.Fingerprint
	w        interval.Window // AsOf is the pinned token after the first call
	from, to time.Time
	pol      Policy
	calls    int
	res      Result

	members     map[identity.Fingerprint]*member
	claims      map[identity.Fingerprint][]interval.Element // each claimant's forward edges, from its claims call
	unattr      map[unattrKey][]interval.Span
	quarantined map[store.Subject]interval.Element
}

// member is what the walk knows of one member so far.
type member struct {
	depth    int
	reach    []interval.Span
	arrivals map[arrivalKey]*Arrival
}

type arrivalKey struct {
	parent identity.Fingerprint
	rel    catalog.RelationType
}

type unattrKey struct {
	subject identity.Fingerprint
	rel     catalog.RelationType
	target  catalog.EntityType
}

// leg is one edge Y→X read in reverse from a frontier entity X and followed.
type leg struct {
	x   identity.Fingerprint
	el  interval.Element
	rel catalog.Relation
}

// edges is one interval call. The first resolves the window's token, and every
// later one passes it.
func (q *walk) edges(fps []identity.Fingerprint, dir store.Direction) ([][]interval.Element, error) {
	q.calls++
	tok, out, err := q.r.EdgesBatch(q.ctx, fps, dir, q.w)
	if err != nil {
		return nil, err
	}
	if q.calls == 1 {
		q.w.AsOf = tok
		q.res.Token = tok
	}
	return out, nil
}

func (q *walk) quarantine(el interval.Element) { q.quarantined[el.Subject] = el }

// propagates reports whether failure travels along the relation: down or up.
func propagates(rel catalog.Relation) bool {
	return rel.Propagation() == catalog.PropagateDown || rel.Propagation() == catalog.PropagateUp
}

// expandable reports whether edges can lead into fp: its type has a reverse
// edge layer.
func (q *walk) expandable(fp identity.Fingerprint) bool {
	return len(interval.EdgeLayers(q.cat, fp.Type(), store.Reverse)) > 0
}

func (q *walk) run() error {
	q.res = Result{From: q.from, To: q.to, Seed: q.seed, PolicyVersion: PolicyVersion, IntervalPolicy: interval.PolicyVersion}
	frontier := []identity.Fingerprint{q.seed}
	delta := map[identity.Fingerprint][]interval.Span{q.seed: {{Start: q.from, End: q.to}}}
	for d := 1; d <= q.pol.MaxDepth && len(frontier) > 0; d++ {
		legs, err := q.reverse(frontier)
		if err != nil {
			return err
		}
		if err := q.readClaims(legs); err != nil {
			return err
		}
		frontier, delta = q.propagate(d, legs, delta)
	}
	q.finish()
	return nil
}

// reverse reads the frontier's edges in reverse and keeps those whose relation
// propagates failure. One with a quarantined endpoint is never followed, and is
// put aside. One from the seed, which a cycle returns to, is never followed
// either, so the seed is never a member and never a claimant.
func (q *walk) reverse(frontier []identity.Fingerprint) ([]leg, error) {
	out, err := q.edges(frontier, store.Reverse)
	if err != nil {
		return nil, err
	}
	var legs []leg
	for i, x := range frontier {
		for _, el := range out[i] {
			rel, ok := q.cat.Relation(el.Subject.Relation)
			if !ok || !propagates(rel) {
				continue
			}
			if el.Uncascaded != nil {
				q.quarantine(el)
				continue
			}
			if el.Subject.A == q.seed {
				continue
			}
			legs = append(legs, leg{x: x, el: el, rel: rel})
		}
	}
	return legs, nil
}

// readClaims reads forward, in one call, the from entity of every followed leg
// of a single-claim relation, for its other claims. A claim of a quarantined
// target is put aside, and still counts as a claim.
func (q *walk) readClaims(legs []leg) error {
	var ys []identity.Fingerprint
	seen := map[identity.Fingerprint]bool{}
	for _, l := range legs {
		if y := l.el.Subject.A; singleClaim(l.rel.Type()) && !seen[y] {
			seen[y] = true
			ys = append(ys, y)
		}
	}
	if len(ys) == 0 {
		return nil
	}
	slices.SortFunc(ys, store.CompareFingerprints)
	out, err := q.edges(ys, store.Forward)
	if err != nil {
		return err
	}
	for i, y := range ys {
		q.claims[y] = out[i]
		for _, el := range out[i] {
			if rel, ok := q.cat.Relation(el.Subject.Relation); ok && propagates(rel) && el.Uncascaded != nil {
				q.quarantine(el)
			}
		}
	}
	return nil
}

// effective is the instants a leg counts for: its own spans, less, for a
// single-claim relation, every instant at which its from entity claims another
// target of that relation and type. The instants it loses while its target is
// reached are unattributed.
func (q *walk) effective(l leg, reached []interval.Span) []interval.Span {
	own := interval.Spans(l.el)
	rel := l.rel.Type()
	if !singleClaim(rel) {
		return own
	}
	y, target := l.el.Subject.A, l.el.Subject.B.Type()
	var others [][]interval.Span
	for _, c := range q.claims[y] {
		if c.Subject != l.el.Subject && c.Subject.Relation == rel && c.Subject.B.Type() == target {
			others = append(others, interval.Spans(c))
		}
	}
	rest := union(others...)
	if lost := interval.Intersect(interval.Intersect(own, rest), reached); len(lost) > 0 {
		k := unattrKey{subject: y, rel: rel, target: target}
		q.unattr[k] = union(q.unattr[k], lost)
	}
	return subtract(own, rest)
}

// propagate carries the frontier's new reach one hop along the followed legs,
// and returns the next frontier with its new reach.
func (q *walk) propagate(d int, legs []leg, delta map[identity.Fingerprint][]interval.Span) ([]identity.Fingerprint, map[identity.Fingerprint][]interval.Span) {
	gained := map[identity.Fingerprint][][]interval.Span{}
	var ys []identity.Fingerprint
	for _, l := range legs {
		s := interval.Intersect(delta[l.x], q.effective(l, delta[l.x]))
		y := l.el.Subject.A
		if len(s) == 0 {
			continue
		}
		m := q.members[y]
		if m == nil {
			m = &member{arrivals: map[arrivalKey]*Arrival{}}
			q.members[y] = m
		}
		k := arrivalKey{parent: l.x, rel: l.rel.Type()}
		a := m.arrivals[k]
		if a == nil {
			a = &Arrival{Parent: l.x, Relation: l.rel.Type(), Propagation: l.rel.Propagation(), Leg: l.el}
			m.arrivals[k] = a
		}
		a.Spans = union(a.Spans, s)
		if _, ok := gained[y]; !ok {
			ys = append(ys, y)
		}
		gained[y] = append(gained[y], s)
	}
	next := map[identity.Fingerprint][]interval.Span{}
	var reached []identity.Fingerprint
	for _, y := range ys {
		m := q.members[y]
		grown := subtract(union(gained[y]...), m.reach)
		if len(grown) == 0 {
			continue
		}
		m.reach = union(m.reach, grown)
		if m.depth == 0 {
			m.depth = d
		}
		next[y] = grown
		if q.expandable(y) {
			reached = append(reached, y)
		}
	}
	slices.SortFunc(reached, func(a, b identity.Fingerprint) int {
		if c := cmp.Compare(interval.Duration(next[b]), interval.Duration(next[a])); c != 0 {
			return c
		}
		return store.CompareFingerprints(a, b)
	})
	if d == q.pol.MaxDepth {
		q.res.Truncated.Depth = append(q.res.Truncated.Depth, reached...)
		return nil, nil
	}
	if len(reached) > q.pol.MaxFrontier {
		dropped := slices.Clone(reached[q.pol.MaxFrontier:])
		slices.SortFunc(dropped, store.CompareFingerprints)
		q.res.Truncated.Frontier = append(q.res.Truncated.Frontier, FrontierCut{Depth: d, Dropped: dropped})
		reached = reached[:q.pol.MaxFrontier]
	}
	return reached, next
}

// finish derives the members, applies MaxMembers, and orders everything.
func (q *walk) finish() {
	for y, m := range q.members {
		mem := Member{Entity: y, Depth: m.depth, Reach: m.reach, Duration: interval.Duration(m.reach)}
		mem.Class, mem.Transitions = classOf(m.reach, q.from, q.to)
		for _, a := range m.arrivals {
			mem.Arrivals = append(mem.Arrivals, *a)
		}
		slices.SortFunc(mem.Arrivals, func(a, b Arrival) int {
			if c := store.CompareFingerprints(a.Parent, b.Parent); c != 0 {
				return c
			}
			return cmp.Compare(a.Relation, b.Relation)
		})
		q.res.Members = append(q.res.Members, mem)
	}
	slices.SortFunc(q.res.Members, rank)
	if n := len(q.res.Members) - q.pol.MaxMembers; n > 0 {
		q.res.Truncated.Members = n
		q.res.Members = q.res.Members[:q.pol.MaxMembers]
	}
	slices.SortFunc(q.res.Truncated.Depth, store.CompareFingerprints)

	for k, spans := range q.unattr {
		u := Unattributed{Subject: k.subject, Relation: k.rel, Target: k.target, Spans: spans}
		for _, c := range q.claims[k.subject] {
			if c.Subject.Relation == k.rel && c.Subject.B.Type() == k.target &&
				len(interval.Intersect(interval.Spans(c), spans)) > 0 && !slices.Contains(u.Candidates, c.Subject.B) {
				u.Candidates = append(u.Candidates, c.Subject.B)
			}
		}
		slices.SortFunc(u.Candidates, store.CompareFingerprints)
		q.res.Unattributed = append(q.res.Unattributed, u)
	}
	slices.SortFunc(q.res.Unattributed, func(a, b Unattributed) int {
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
	for _, el := range q.quarantined {
		q.res.Quarantined = append(q.res.Quarantined, el)
	}
	slices.SortFunc(q.res.Quarantined, func(a, b interval.Element) int { return compareSubjects(a.Subject, b.Subject) })
}

// rank orders members: depth ascending, then reach duration descending, then
// fingerprint.
func rank(a, b Member) int {
	if c := cmp.Compare(a.Depth, b.Depth); c != 0 {
		return c
	}
	if c := cmp.Compare(b.Duration, a.Duration); c != 0 {
		return c
	}
	return store.CompareFingerprints(a.Entity, b.Entity)
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
