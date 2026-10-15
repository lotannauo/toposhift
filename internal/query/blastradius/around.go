package blastradius

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
)

// Via is one upward leg into a substrate. It is derived.
type Via struct {
	// From is the pod, or the substrate one hop nearer it.
	From     identity.Fingerprint
	Relation catalog.RelationType
	// Leg is the edge From→substrate, with the cascade.
	Leg interval.Element
	// Spans is From's time, intersected with the leg's effective spans: the
	// leg's own, less the instants From claims another target of a single-claim
	// relation and type. Ascending, disjoint, never touching, never empty.
	Spans []interval.Span
}

// Substrate is an entity a pod was placed on in the window, with what its
// failure reaches while the pod was on it. It is derived.
type Substrate struct {
	// Entity is a node, a host, a rack or a zone.
	Entity identity.Fingerprint
	// Depth is the fewest upward hops from the pod: 1 a node, 2 a host, 3 a
	// rack or a zone.
	Depth int
	// On is the pod's time on the substrate: ascending, disjoint, never
	// touching and never empty.
	On       []interval.Span
	Duration time.Duration
	// Vias are ordered by From, then relation.
	Vias []Via
	// Expanded says Impact was derived. Only a node and a host are expanded
	// unless Options.ExpandFabric is set; any other substrate is listed, and its
	// Impact is the zero Result.
	Expanded bool
	// Impact is the walk from Entity, started over On: its Seed is Entity, the
	// pod is not a member and is never expanded, and every member's reach lies
	// within On.
	Impact Result
}

// AroundResult is the answer of Around, read at one token. It is derived.
type AroundResult struct {
	Token    uint64
	From, To time.Time
	Seed     identity.Fingerprint
	// Substrates are ordered by depth, then Duration descending, then
	// fingerprint.
	Substrates []Substrate
	// Unattributed, Quarantined and Truncated are the upward walk's: those of
	// each substrate's walk are in its Impact.
	Unattributed []Unattributed
	Quarantined  []interval.Element
	Truncated    Truncation
	// PolicyVersion is this package's; IntervalPolicy is interval.PolicyVersion.
	PolicyVersion, IntervalPolicy int
	// Reads is the upward walk's plus every substrate walk's.
	Reads Reads
}

// walkedByDefault reports whether Around walks from a substrate without
// Options.ExpandFabric: only a node and a host are. Everything else the upward
// walk reaches, a rack, a zone, and any type the catalog later places above a
// host, is listed and not walked, so a new type can never be walked by default
// outside the budget.
func walkedByDefault(fp identity.Fingerprint) bool {
	return fp.Type() == catalog.K8sNode || fp.Type() == catalog.Host
}

// Around derives, for each substrate a pod was on in win, what that
// substrate's failure reaches while the pod was on it. The pod's substrates
// are found by an upward walk from the pod along the relations Impact follows,
// each leg read with the cascade, a single-claim relation counted for a from
// entity only where it claims no other target of that relation and type, and a
// pod scheduled on two nodes at once on both. Each substrate that is expanded
// is then walked as Impact walks it, at the same token, but started over the
// pod's time on it, and without the pod: the pod is never a member and never
// expanded. Only nodes and hosts are walked by default; every other substrate,
// racks and zones included, is walked only if o.ExpandFabric is set.
// Arguments are checked before any store call, and the seed must be a pod.
// After that, any error of a read fails the whole query with a zero result, and
// a window that starts before a layer's retention horizon gives an error
// wrapping store.ErrBeforeHorizon.
func (w *Walker) Around(ctx context.Context, pod identity.Fingerprint, win interval.Window, o Options) (AroundResult, error) {
	fail := func(err error) (AroundResult, error) {
		return AroundResult{}, fmt.Errorf("blastradius: Around: %w", err)
	}
	if !win.From.Before(win.To) {
		return fail(fmt.Errorf("the window [%s, %s) is empty: %w", formatTime(win.From), formatTime(win.To), store.ErrInvalid))
	}
	if pod.IsZero() {
		return fail(fmt.Errorf("the seed is the zero fingerprint: %w", store.ErrInvalid))
	}
	if pod.Type() != catalog.K8sPod {
		return fail(fmt.Errorf("the seed %s is a %q, not a %q: %w", pod, pod.Type(), catalog.K8sPod, store.ErrInvalid))
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
	up := newWalk(ctx, cnt, walkPlan{seed: pod, win: win, pol: pol, up: true})
	if err := up.run(); err != nil {
		return fail(err)
	}
	res := AroundResult{
		Token: up.res.Token, From: up.from, To: up.to, Seed: pod, Substrates: upwardSubstrates(up),
		Unattributed: up.res.Unattributed, Quarantined: up.res.Quarantined, Truncated: up.res.Truncated,
		PolicyVersion: PolicyVersion, IntervalPolicy: interval.PolicyVersion,
	}
	res.Reads = cnt.reads
	res.Reads.Calls = up.calls

	pinned := win
	pinned.AsOf = res.Token
	for i := range res.Substrates {
		s := &res.Substrates[i]
		if !walkedByDefault(s.Entity) && !o.ExpandFabric {
			continue
		}
		c := &counting{Store: w.st}
		q := newWalk(ctx, c, walkPlan{seed: s.Entity, win: pinned, pol: pol, start: s.On, exclude: pod})
		if err := q.run(); err != nil {
			return fail(err)
		}
		q.res.Reads = c.reads
		q.res.Reads.Calls = q.calls
		s.Expanded, s.Impact = true, q.res
		res.Reads.Calls += q.res.Reads.Calls
		res.Reads.Windows += q.res.Reads.Windows
		res.Reads.Alive += q.res.Reads.Alive
		res.Reads.EntityWindows += q.res.Reads.EntityWindows
	}
	return res, nil
}

// upwardSubstrates lists the entities an upward walk reached, ranked.
func upwardSubstrates(up *walk) []Substrate {
	var out []Substrate
	for x, m := range up.members {
		s := Substrate{Entity: x, Depth: m.depth, On: m.reach, Duration: interval.Duration(m.reach)}
		for _, a := range m.arrivals {
			s.Vias = append(s.Vias, Via{From: a.Parent, Relation: a.Relation, Leg: a.Leg, Spans: a.Spans})
		}
		slices.SortFunc(s.Vias, func(a, b Via) int {
			if c := store.CompareFingerprints(a.From, b.From); c != 0 {
				return c
			}
			return cmp.Compare(a.Relation, b.Relation)
		})
		out = append(out, s)
	}
	slices.SortFunc(out, func(a, b Substrate) int {
		if c := cmp.Compare(a.Depth, b.Depth); c != 0 {
			return c
		}
		if c := cmp.Compare(b.Duration, a.Duration); c != 0 {
			return c
		}
		return store.CompareFingerprints(a.Entity, b.Entity)
	})
	return out
}
