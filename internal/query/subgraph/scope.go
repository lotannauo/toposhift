package subgraph

import (
	"context"
	"fmt"
	"slices"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
)

// ScopePolicy is the budget of a scope read.
type ScopePolicy struct {
	// MaxScope is the most distinct entities a scope may name; a larger scope
	// is refused, because a partial subgraph would mislead. At least 1.
	MaxScope int
}

// DefaultScopePolicy is version 1's default: a scope of at most 256 entities,
// graphdiff's placeholder until it is measured.
func DefaultScopePolicy() ScopePolicy { return ScopePolicy{MaxScope: 256} }

func (p ScopePolicy) validate() error {
	if p.MaxScope < 1 {
		return fmt.Errorf("a cap of %+v is below 1: %w", p, store.ErrInvalid)
	}
	return nil
}

// ReadScope reads the temporal subgraph of a scope over the window w.
//
// A store cannot list its subjects, so the scope is explicit: a set of
// entities, each read in its catalog layer, and every edge with an endpoint
// among them, read forward and reverse, one hop out, as graph diff reads its
// scope. An edge between two scope entities is one element. The peer at the far
// end of an edge is a dictionary entry marked as a boundary, with no existence
// element: it is read only for the cascade, and neither its own history nor its
// other edges are in the subgraph. An entity of the scope that did not exist in
// the window has no element either.
//
// The first read resolves the token, and every later read uses it, so a write
// between two reads is not seen; the subgraph names that token. An edge exists
// only while it and both its endpoints do. A quarantined scope entity, and an
// edge with a quarantined endpoint, are in the static section. The zero policy
// means [DefaultScopePolicy]. Arguments are checked before any store call;
// after that any error fails the whole read, with no partial answer, and a
// window that starts before a layer's retention horizon gives an error wrapping
// store.ErrBeforeHorizon. Reading it costs
// what package interval's reads cost: one entity call, and one edge call per
// direction in which a scope entity has an edge layer.
func ReadScope(ctx context.Context, st store.Store, scope []identity.Fingerprint, w interval.Window, p ScopePolicy) (Subgraph, error) {
	fail := func(err error) (Subgraph, error) { return Subgraph{}, fmt.Errorf("subgraph: ReadScope: %w", err) }
	fps, err := checkScope(scope, w, p)
	if err != nil {
		return fail(err)
	}
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	token, els, err := readScope(ctx, st, fps, w)
	if err != nil {
		return fail(err)
	}
	in := Input{Token: token, From: w.From.UTC(), To: w.To.UTC(), Read: fps}
	for _, el := range els {
		src, err := sourceOf(el)
		if err != nil {
			return fail(err)
		}
		in.Sources = append(in.Sources, src)
	}
	sg, err := Build(in)
	if err != nil {
		return fail(err)
	}
	return sg, nil
}

// checkScope checks a read's arguments, and returns the scope once each and
// ordered.
func checkScope(scope []identity.Fingerprint, w interval.Window, p ScopePolicy) ([]identity.Fingerprint, error) {
	if _, err := newWindow(w.From, w.To); err != nil {
		return nil, err
	}
	if len(scope) == 0 {
		return nil, fmt.Errorf("the scope is empty: %w", store.ErrInvalid)
	}
	cat := catalog.Default()
	for i, fp := range scope {
		if fp.IsZero() {
			return nil, fmt.Errorf("scope entity %d is the zero fingerprint: %w", i, store.ErrInvalid)
		}
		if _, ok := cat.Entity(fp.Type()); !ok {
			return nil, fmt.Errorf("scope entity %d has the unknown entity type %q: %w", i, fp.Type(), store.ErrInvalid)
		}
	}
	if p == (ScopePolicy{}) {
		p = DefaultScopePolicy()
	}
	if err := p.validate(); err != nil {
		return nil, err
	}
	fps := slices.Clone(scope)
	slices.SortFunc(fps, store.CompareFingerprints)
	fps = slices.Compact(fps)
	if len(fps) > p.MaxScope {
		return nil, fmt.Errorf("the scope has %d distinct entities, over the cap of %d: %w", len(fps), p.MaxScope, store.ErrInvalid)
	}
	return fps, nil
}

// readScope makes the read's interval calls, all at the token of the first: the
// scope's entities, then the edges forward and reverse, each skipped when no
// scope entity has an edge layer in its direction. It returns the token, and
// the entities and then the edges, once each.
func readScope(ctx context.Context, st store.Store, fps []identity.Fingerprint, w interval.Window) (uint64, []interval.Element, error) {
	rd := interval.NewReader(st, interval.Options{})
	token, els, err := rd.Entities(ctx, fps, w)
	if err != nil {
		return 0, nil, err
	}
	// Every later read names this token; re-resolving w.AsOf would let a write
	// between the calls into the edges only.
	w.AsOf = token
	cat := catalog.Default()
	seen := map[store.Subject]bool{}
	for _, dir := range []store.Direction{store.Forward, store.Reverse} {
		if !slices.ContainsFunc(fps, func(fp identity.Fingerprint) bool { return len(interval.EdgeLayers(cat, fp.Type(), dir)) > 0 }) {
			continue
		}
		tok, out, err := rd.EdgesBatch(ctx, fps, dir, w)
		if err != nil {
			return 0, nil, err
		}
		if tok != token {
			return 0, nil, fmt.Errorf("the %s edge read used the token %d, not the pinned %d", dir, tok, token)
		}
		// An edge between two scope entities is read from both ends, at one
		// token and with one cascade, so the two reads are one element.
		for _, edges := range out {
			for _, el := range edges {
				if !seen[el.Subject] {
					seen[el.Subject] = true
					els = append(els, el)
				}
			}
		}
	}
	return token, els, nil
}

// sourceOf is an interval element as a builder's source, its enums mapped
// through the tables.
func sourceOf(el interval.Element) (Source, error) {
	class, err := ClassOf(el.Class)
	if err != nil {
		return Source{}, err
	}
	src := Source{
		Key: KeyOf(el.Subject), Class: class, Transitions: el.Transitions,
		Quarantine: el.Quarantine, Uncascaded: el.Uncascaded,
	}
	for _, p := range el.Pieces {
		reason, err := EndReasonOf(p.EndSource)
		if err != nil {
			return Source{}, err
		}
		src.Pieces = append(src.Pieces, SourcePiece{
			Start: p.Start, End: p.End, Before: p.Before, EndReason: reason, Producers: p.Producers,
		})
	}
	return src, nil
}
