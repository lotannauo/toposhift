package interval

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/store"
)

// ErrRetained is wrapped by the error a read gives when it touches a layer whose
// retention horizon is not zero. It wraps [errors.ErrUnsupported]: after a
// retention the state at the horizon lives in a baseline no store read returns,
// so the intervals of a subject alive there cannot be rebuilt from records.
var ErrRetained = fmt.Errorf("the layer has been retained, and a range read needs its whole history: %w", errors.ErrUnsupported)

// Options configures a [Reader].
type Options struct {
	// Raw reads edges without the cascade: an edge's own intervals, with no read
	// of its endpoints. It is for diagnostics, and for the equality with
	// store.Store.Neighbors.
	Raw bool
}

// Reader reads windows of a store as intervals. It holds no state between
// calls and is safe for concurrent use if the store is.
type Reader struct {
	st   store.Store
	opts Options
	cat  *catalog.Catalog
	// everyEndpoint turns off the skip of endpoint reads for edges with no
	// interval in the window. Only tests set it, to compare the answers with
	// those of reading every endpoint.
	everyEndpoint bool
}

// NewReader returns a reader of st, which it only reads.
func NewReader(st store.Store, opts Options) *Reader {
	return &Reader{st: st, opts: opts, cat: catalog.Default()}
}

// Entity reads one entity's existence in w, in its catalog layer. The answer
// has exactly one element, with no piece if the entity did not exist in the
// window, or marked quarantined. A refusal fails the call, with no answer.
func (r *Reader) Entity(ctx context.Context, fp identity.Fingerprint, w Window) (Result, error) {
	c, err := r.begin(ctx, "Entity", w, false, 0, fp)
	if err != nil {
		return Result{}, err
	}
	ex, err := c.existence(fp)
	if err != nil {
		return Result{}, err
	}
	el := Element{Subject: store.EntitySubject(fp), Layer: ex.layer, Quarantine: ex.quarantine}
	if ex.quarantine == nil {
		if el.Pieces, err = clip(ex.f.ivs, c.from, c.to); err != nil {
			return Result{}, c.fail(fmt.Errorf("%s: %w", describeSubject(el.Subject), err))
		}
		attribute(el.Pieces, ex.f)
		el.Class, el.Transitions = classify(el.Pieces, c.from, c.to)
	}
	return Result{Token: c.token, From: c.from, To: c.to, Elements: []Element{el}}, nil
}

// Edges reads every edge touching fp in dir that has a piece in w, from each
// layer in [EdgeLayers] (one layer per relation, from the catalog), ordered by
// peer ([store.CompareFingerprints]) then relation, as
// store.Store.Neighbors orders. Any error, a refusal for retention or a stored
// edge that disagrees with the catalog included, fails the whole call: there
// is no partial answer.
func (r *Reader) Edges(ctx context.Context, fp identity.Fingerprint, dir store.Direction, w Window) (Result, error) {
	c, out, err := r.batch(ctx, "Edges", []identity.Fingerprint{fp}, dir, w)
	if err != nil {
		return Result{}, err
	}
	return Result{Token: c.token, From: c.from, To: c.to, Elements: out[0]}, nil
}

// EdgesBatch is Edges for a frontier at one token: out[i] answers fps[i], and
// token is the one every read used. fps may repeat and may mix entity types.
// One failure, a refusal included, fails the whole call, with a nil out: there
// is no partial answer.
func (r *Reader) EdgesBatch(ctx context.Context, fps []identity.Fingerprint, dir store.Direction, w Window) (token uint64, out [][]Element, err error) {
	c, out, err := r.batch(ctx, "EdgesBatch", fps, dir, w)
	if err != nil {
		return 0, nil, err
	}
	return c.token, out, nil
}

// EdgeLayers returns, ascending and distinct, the layers that edges of entity
// type t are stored in when read in dir: Relation.Layer() of every stored
// relation with an endpoint whose From (Forward) or To (Reverse) is t. (The
// catalog gives every stored relation at least one endpoint.) It reads only the
// catalog. A type that no relation reaches in dir, and a direction other than
// Forward or Reverse, give nil.
func EdgeLayers(c *catalog.Catalog, t catalog.EntityType, dir store.Direction) []catalog.Layer {
	if dir != store.Forward && dir != store.Reverse {
		return nil
	}
	var out []catalog.Layer
	for rel := range c.Relations() {
		if rel.Derived() {
			continue
		}
		touches := false
		for ep := range rel.Endpoints() {
			end := ep.From
			if dir == store.Reverse {
				end = ep.To
			}
			if end == t {
				touches = true
				break
			}
		}
		if touches && !slices.Contains(out, rel.Layer()) {
			out = append(out, rel.Layer())
		}
	}
	slices.Sort(out)
	return out
}

// call is one answer in progress: its window, its one token, and what it has
// read so far.
type call struct {
	r        *Reader
	ctx      context.Context
	op       string
	from, to time.Time
	token    uint64
	clear    map[catalog.Layer]bool // layers whose horizon was checked and is zero
	ends     map[identity.Fingerprint]*existence
}

// existence is what a call has read of one entity: its layer, and either its
// quarantine or its fold.
type existence struct {
	layer      catalog.Layer
	quarantine *store.QuarantineError
	f          folded
}

// begin checks the arguments, in this order: the window, the direction (when
// the read has one), and the fingerprints. Then it checks the context and pins
// the one token of the answer, before any other read of the store.
func (r *Reader) begin(ctx context.Context, op string, w Window, hasDir bool, dir store.Direction, fps ...identity.Fingerprint) (*call, error) {
	if !w.From.Before(w.To) {
		return nil, fmt.Errorf("interval: %s: the window [%s, %s) is empty: %w",
			op, formatTime(w.From), formatTime(w.To), store.ErrInvalid)
	}
	if hasDir && dir != store.Forward && dir != store.Reverse {
		return nil, fmt.Errorf("interval: %s: direction %s: %w", op, dir, store.ErrInvalid)
	}
	c := &call{
		r: r, ctx: ctx, op: op, from: w.From.UTC(), to: w.To.UTC(),
		clear: map[catalog.Layer]bool{}, ends: map[identity.Fingerprint]*existence{},
	}
	for i, fp := range fps {
		if fp.IsZero() {
			return nil, c.fail(fmt.Errorf("fingerprint %d is the zero fingerprint: %w", i, store.ErrInvalid))
		}
		if _, ok := r.cat.Entity(fp.Type()); !ok {
			return nil, c.fail(fmt.Errorf("fingerprint %d has the unknown entity type %q: %w", i, fp.Type(), store.ErrInvalid))
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, c.fail(err)
	}
	c.token = w.AsOf
	if last := r.st.LastSeq(); c.token == store.Latest || c.token > last {
		c.token = last
	}
	return c, nil
}

func (c *call) fail(err error) error { return fmt.Errorf("interval: %s: %w", c.op, err) }

func (c *call) scope(l catalog.Layer) store.Scope { return store.Scope{Layer: l, AsOf: c.token} }

// checkRetention refuses a layer whose horizon is not zero.
func (c *call) checkRetention(l catalog.Layer) error {
	if c.clear[l] {
		return nil
	}
	if h := c.r.st.LayerHorizon(l); !h.IsZero() {
		return c.fail(fmt.Errorf("layer %s has the retention horizon %s (seq %d): %w", l, formatTime(h.Time), h.Seq, ErrRetained))
	}
	c.clear[l] = true
	return nil
}

func (r *Reader) batch(ctx context.Context, op string, fps []identity.Fingerprint, dir store.Direction, w Window) (*call, [][]Element, error) {
	c, err := r.begin(ctx, op, w, true, dir, fps...)
	if err != nil {
		return nil, nil, err
	}
	// Every edge layer is checked before the first read.
	for _, fp := range fps {
		for _, l := range EdgeLayers(r.cat, fp.Type(), dir) {
			if err := c.checkRetention(l); err != nil {
				return nil, nil, err
			}
		}
	}
	out := make([][]Element, len(fps))
	first := map[identity.Fingerprint]int{}
	for i, fp := range fps {
		if j, ok := first[fp]; ok {
			out[i] = cloneElements(out[j])
			continue
		}
		if out[i], err = c.edges(fp, dir); err != nil {
			return nil, nil, err
		}
		first[fp] = i
	}
	return c, out, nil
}

// group is the records of one subject, in the order the store returned them.
type group struct {
	sub  store.Subject
	recs []store.Record
}

func groupBySubject(recs []store.Record) []group {
	var out []group
	at := map[store.Subject]int{}
	for _, r := range recs {
		i, ok := at[r.Subject]
		if !ok {
			i = len(out)
			at[r.Subject] = i
			out = append(out, group{sub: r.Subject})
		}
		out[i].recs = append(out[i].recs, r)
	}
	return out
}

// edges reads the edges of one fingerprint, one Window per layer at the call's
// token, and orders them by peer then relation.
func (c *call) edges(fp identity.Fingerprint, dir store.Direction) ([]Element, error) {
	var out []Element
	for _, l := range EdgeLayers(c.r.cat, fp.Type(), dir) {
		recs, err := c.r.st.Window(c.ctx, fp, dir, store.MinEventTime, c.to, c.scope(l))
		if err != nil {
			return nil, c.fail(fmt.Errorf("reading the %s edges of %s in layer %s: %w", dir, fp, l, err))
		}
		for _, g := range groupBySubject(recs) {
			if err := c.checkEdge(fp, dir, l, g.sub); err != nil {
				return nil, err
			}
			el, err := c.edge(g, l)
			if err != nil {
				return nil, err
			}
			if len(el.Pieces) > 0 {
				out = append(out, el)
			}
		}
	}
	peer := func(s store.Subject) identity.Fingerprint {
		if dir == store.Forward {
			return s.B
		}
		return s.A
	}
	slices.SortFunc(out, func(a, b Element) int {
		if c := store.CompareFingerprints(peer(a.Subject), peer(b.Subject)); c != 0 {
			return c
		}
		return cmp.Compare(a.Subject.Relation, b.Subject.Relation)
	})
	return out, nil
}

// checkEdge refuses a stored edge that disagrees with the catalog: a writer has
// drifted from it, and dropping the edge would hide that.
func (c *call) checkEdge(fp identity.Fingerprint, dir store.Direction, l catalog.Layer, sub store.Subject) error {
	why := ""
	rel, ok := c.r.cat.Relation(sub.Relation)
	switch {
	case sub.Kind != store.SubjectEdge,
		dir == store.Forward && sub.A != fp,
		dir == store.Reverse && sub.B != fp:
		why = fmt.Sprintf("it is not a %s edge of %s", dir, fp)
	case !ok:
		why = fmt.Sprintf("the relation %q is not registered", sub.Relation)
	case rel.Derived():
		why = fmt.Sprintf("the relation %q is derived and never stored", sub.Relation)
	case rel.Layer() != l:
		why = fmt.Sprintf("the relation %q is stored in layer %s", sub.Relation, rel.Layer())
	case !rel.Allows(sub.A.Type(), sub.B.Type()):
		why = fmt.Sprintf("the relation %q does not connect %s to %s", sub.Relation, sub.A.Type(), sub.B.Type())
	default:
		return nil
	}
	return c.fail(fmt.Errorf("%s read in layer %s: %s: %w", describeSubject(sub), l, why, store.ErrInvalid))
}

// edge folds one edge, cuts it by its endpoints unless the reader is raw, and
// clips it to the window.
func (c *call) edge(g group, l catalog.Layer) (Element, error) {
	el := Element{Subject: g.sub, Layer: l}
	f, err := foldSubject(g.recs)
	if err != nil {
		return Element{}, c.fail(err)
	}
	ivs := f.ivs
	if !c.r.everyEndpoint && !slices.ContainsFunc(ivs, func(iv ival) bool { return iv.meets(c.from, c.to) }) {
		// No interval of the edge's own meets the window, so it has no piece,
		// cut or uncut, and its endpoints are not read: the cascade costs one
		// existence read per endpoint of an edge in the window, not per endpoint
		// in the whole history before To.
		return el, nil
	}
	if !c.r.opts.Raw {
		var ends [2]*existence
		for i, e := range []identity.Fingerprint{g.sub.A, g.sub.B} {
			if ends[i], err = c.existence(e); err != nil {
				return Element{}, err
			}
		}
		switch {
		case ends[0].quarantine != nil:
			el.Uncascaded = ends[0].quarantine
		case ends[1].quarantine != nil:
			el.Uncascaded = ends[1].quarantine
		default:
			ivs = cut(cut(ivs, ends[0].f.ivs), ends[1].f.ivs)
		}
	}
	if el.Pieces, err = clip(ivs, c.from, c.to); err != nil {
		return Element{}, c.fail(fmt.Errorf("%s: %w", describeSubject(g.sub), err))
	}
	attribute(el.Pieces, f)
	el.Class, el.Transitions = classify(el.Pieces, c.from, c.to)
	return el, nil
}

// existence reads an entity once per call, in its own catalog layer: first
// Alive, for the quarantine, then its records.
func (c *call) existence(fp identity.Fingerprint) (*existence, error) {
	if ex, ok := c.ends[fp]; ok {
		return ex, nil
	}
	ent, ok := c.r.cat.Entity(fp.Type())
	if !ok {
		return nil, c.fail(fmt.Errorf("entity %s has an unknown type: %w", fp, store.ErrInvalid))
	}
	l := ent.Layer()
	if err := c.checkRetention(l); err != nil {
		return nil, err
	}
	ex := &existence{layer: l}
	if _, err := c.r.st.Alive(c.ctx, fp, c.from, c.scope(l)); err != nil {
		var qe *store.QuarantineError
		if !errors.As(err, &qe) {
			return nil, c.fail(fmt.Errorf("reading whether %s is quarantined: %w", fp, err))
		}
		ex.quarantine = qe
	} else {
		recs, err := c.r.st.EntityWindow(c.ctx, fp, store.MinEventTime, c.to, c.scope(l))
		if err != nil {
			return nil, c.fail(fmt.Errorf("reading the existence of %s in layer %s: %w", fp, l, err))
		}
		if ex.f, err = foldSubject(recs); err != nil {
			return nil, c.fail(err)
		}
	}
	c.ends[fp] = ex
	return ex, nil
}

// cloneElements copies elements and their pieces, so two answers of one batch
// share nothing a caller may change.
func cloneElements(els []Element) []Element {
	out := slices.Clone(els)
	for i := range out {
		out[i].Pieces = slices.Clone(out[i].Pieces)
		for k := range out[i].Pieces {
			out[i].Pieces[k].Producers = slices.Clone(out[i].Pieces[k].Producers)
		}
	}
	return out
}
