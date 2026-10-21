package interval

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

// ErrRetained is wrapped by the error a reader with [Options].Describe gives when
// the description it must give was asserted before the retention horizon of its
// layer: the store keeps the assertion's times, but not its payload, so the
// description is refused rather than guessed or left empty. It wraps
// [errors.ErrUnsupported]. It is the only error a retention adds: a window that
// starts before a layer's horizon is refused by the store, with an error
// wrapping [store.ErrBeforeHorizon].
var ErrRetained = fmt.Errorf("asserted before the retention horizon and no longer kept: %w", errors.ErrUnsupported)

// RetainedError is the refusal of a description: the one a reader with
// [Options].Describe must give of Subject at At is decided by Seed, Producer's
// assertion before the window, recorded before the retention horizon of its layer,
// whose payload the store no longer keeps. Seed has every field the store keeps,
// both time axes included, and no payload. It wraps [ErrRetained]; the call's error
// wraps it.
type RetainedError struct {
	Subject  store.Subject
	At       time.Time
	Producer lifecycle.Producer
	Seed     store.Record
}

func (e *RetainedError) Error() string {
	return fmt.Sprintf("the description of %s at %s by %s was %v", describeSubject(e.Subject), formatTime(e.At), e.Producer, ErrRetained)
}

func (e *RetainedError) Unwrap() error { return ErrRetained }

// Deadline is when Seed's reference lapses: its last asserted instant (Through
// for a run, else its event time) plus its TTL. A window read at the same token
// that starts at or after it does not need this seed. ok is false for a seed with
// no TTL, which lapses only when its producer asserts the subject again.
func (e *RetainedError) Deadline() (time.Time, bool) {
	if e.Seed.TTL <= 0 {
		return time.Time{}, false
	}
	last := e.Seed.EventTime
	if e.Seed.Through.After(last) {
		last = e.Seed.Through
	}
	return last.Add(e.Seed.TTL), true
}

// Options configures a [Reader].
type Options struct {
	// Raw reads edges without the cascade: an edge's own intervals, with no read
	// of its endpoints. It is for diagnostics, and for the equality with
	// store.Store.Neighbors.
	Raw bool
	// Describe fills [Element].AtFrom and [Element].AtLast. It folds a second
	// time, under Rank, the records of the subject the call has already read,
	// and reads nothing more.
	Describe bool
	// Evidence fills [Element].Last: the record that decides each live producer's
	// reference at the window's last instant. It is read from the records the call
	// has already read (the seeds and the window of each subject), so it adds no
	// store read and no refusal, and it asks for no payload: a reader with it and
	// no Describe reads exactly what a reader without it reads. Nothing else in an
	// answer depends on it.
	Evidence bool
	// Rank is the authority order of descriptions, as lifecycle.Policy.Rank: a
	// listed producer beats an unlisted one, a higher rank beats a lower one,
	// and the smaller name breaks every remaining tie, so with no Rank the
	// smallest name wins. Only Describe uses it: no existence depends on it.
	// NewReader copies it.
	Rank map[lifecycle.Producer]int
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
	opts.Rank = maps.Clone(opts.Rank)
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
	c.keepRecords, c.keepEvidence = r.opts.Describe, r.opts.Evidence
	if err := c.existences([]identity.Fingerprint{fp}); err != nil {
		return Result{}, err
	}
	el, err := c.entity(fp)
	if err != nil {
		return Result{}, err
	}
	return Result{Token: c.token, From: c.from, To: c.to, Elements: []Element{el}}, nil
}

// Entities is Entity for many entities at one token: out[i] answers fps[i],
// and token is the one every read used. fps may repeat and may mix entity
// types; an entity listed twice is read once, and its two answers share
// nothing a caller may change. The arguments are checked as Entity checks
// them, and every entity layer before the first read. No fingerprints give the
// token and an empty out, with no read but the token's. One failure, a refusal
// included, fails the whole call, with a nil out: there is no partial answer.
func (r *Reader) Entities(ctx context.Context, fps []identity.Fingerprint, w Window) (token uint64, out []Element, err error) {
	c, err := r.begin(ctx, "Entities", w, false, 0, fps...)
	if err != nil {
		return 0, nil, err
	}
	c.keepRecords, c.keepEvidence = r.opts.Describe, r.opts.Evidence
	if err := c.existences(fps); err != nil {
		return 0, nil, err
	}
	out = make([]Element, len(fps))
	first := map[identity.Fingerprint]int{}
	for i, fp := range fps {
		if j, ok := first[fp]; ok {
			out[i] = cloneElement(out[j])
			continue
		}
		if out[i], err = c.entity(fp); err != nil {
			return 0, nil, err
		}
		first[fp] = i
	}
	return c.token, out, nil
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
	ends     map[identity.Fingerprint]*existence
	// keepRecords keeps each entity's records for its description: set by the
	// entity reads of a reader that describes, never by an edge read, whose
	// endpoints are read for the cascade only.
	keepRecords bool
	// keepEvidence keeps the same records for the evidence at the window's last
	// instant, which needs no payload: set by the entity reads of a reader with
	// Evidence, never by an edge read, whose endpoints are read for the cascade only.
	keepEvidence bool
}

// existence is what a call has read of one entity: its layer, and either its
// quarantine or its fold, with the records folded when the call keeps them.
type existence struct {
	layer      catalog.Layer
	quarantine *store.QuarantineError
	f          folded
	recs       []store.Record
	// unknown are the seeds that carry no payload, by producer: their assertion
	// before From was before the retention horizon.
	unknown map[lifecycle.Producer]store.Record
}

// entity is the element of one entity the call has read (see existences): its
// existence in the call's window, or its quarantine.
func (c *call) entity(fp identity.Fingerprint) (Element, error) {
	ex := c.ends[fp]
	var err error
	el := Element{Subject: store.EntitySubject(fp), Layer: ex.layer, Quarantine: ex.quarantine}
	if ex.quarantine != nil {
		return el, nil
	}
	if el.Pieces, err = clip(ex.f.ivs, c.from, c.to); err != nil {
		return Element{}, c.fail(fmt.Errorf("%s: %w", describeSubject(el.Subject), err))
	}
	attribute(el.Pieces, ex.f)
	el.Class, el.Transitions = classify(el.Pieces, c.from, c.to)
	if err := c.describe(&el, ex.recs, ex.unknown); err != nil {
		return Element{}, err
	}
	if err := c.evidence(&el, ex.recs, ex.f); err != nil {
		return Element{}, err
	}
	return el, nil
}

// describe sets an element's descriptions at From and at the window's last
// instant, To-1ns, when the reader describes and a piece contains the
// instant. recs are the subject's records the call has read, its seeds and its
// window: they are folded again under the reader's Rank, and nothing more is
// read. unknown are the seeds that have no payload, by producer.
func (c *call) describe(el *Element, recs []store.Record, unknown map[lifecycle.Producer]store.Record) error {
	if !c.r.opts.Describe {
		return nil
	}
	last := c.to.Add(-time.Nanosecond)
	atFrom, atLast := containsAt(el.Pieces, c.from), containsAt(el.Pieces, last)
	if !atFrom && !atLast {
		return nil
	}
	tl, err := foldRanked(el.Subject, recs, c.r.opts.Rank)
	if err != nil {
		return c.fail(err)
	}
	if atFrom {
		if el.AtFrom, err = c.describedAt(tl, el.Subject, c.from, recs, unknown); err != nil {
			return err
		}
	}
	if atLast {
		if el.AtLast, err = c.describedAt(tl, el.Subject, last, recs, unknown); err != nil {
			return err
		}
	}
	return nil
}

// evidence sets an element's Last when the reader asks for evidence and a piece
// contains the window's last instant, To-1ns: for each producer whose reference
// is live there (by the producer's own fold, as the pieces' Producers are), the
// producer's latest record at or before it by event time and then by sequence
// number, and the latest last instant of any of the producer's records read. recs are the subject's records the call has read, its seeds and its
// window, all before To; the seed of a reference is its latest record before
// From, so the latest of a producer's seed and window records is the record that
// decides it. Nothing more is read.
func (c *call) evidence(el *Element, recs []store.Record, f folded) error {
	if !c.r.opts.Evidence {
		return nil
	}
	last := c.to.Add(-time.Nanosecond)
	if !containsAt(el.Pieces, last) {
		return nil
	}
	latest := map[lifecycle.Producer]*store.Record{}
	seen := map[lifecycle.Producer]time.Time{}
	for i := range recs {
		r := &recs[i]
		if d, ok := latest[r.Producer]; !ok || r.EventTime.After(d.EventTime) || (r.EventTime.Equal(d.EventTime) && r.Seq > d.Seq) {
			latest[r.Producer] = r
		}
		if r.Kind == lifecycle.Observe {
			if l, _ := store.LastOf(r.EventTime, r.Through); l.After(seen[r.Producer]) {
				seen[r.Producer] = l
			}
		}
	}
	for i, p := range f.producers {
		if !liveAt(f.byProducer[i], last) {
			continue
		}
		d := latest[p]
		if d == nil || d.Kind != lifecycle.Observe {
			return c.fail(fmt.Errorf("%s is live by %s at %s with no observation deciding it",
				describeSubject(el.Subject), p, formatTime(last)))
		}
		el.Last = append(el.Last, Evidence{
			Producer: p, EventTime: d.EventTime.UTC(), Through: d.Through.UTC(), TTL: d.TTL, Seq: d.Seq,
			Latest: seen[p].UTC(),
		})
	}
	return nil
}

// liveAt reports whether x is in one of the intervals.
func liveAt(ivs []ival, x time.Time) bool {
	return slices.ContainsFunc(ivs, func(iv ival) bool {
		return !iv.start.After(x) && (iv.open || x.Before(iv.end))
	})
}

// describedAt is the description at x, or the refusal of one whose winning
// producer's latest assertion at or before x is a seed with no payload: that
// assertion was before the retention horizon, and its payload is not kept. A
// seed is before From, so the producer's latest assertion at x is its seed
// exactly when it has no window record at or before x.
func (c *call) describedAt(tl lifecycle.Timeline, sub store.Subject, x time.Time, recs []store.Record, unknown map[lifecycle.Producer]store.Record) (*Description, error) {
	d, err := describeAt(tl, sub, x)
	if err != nil {
		return nil, c.fail(err)
	}
	if seed, ok := unknown[d.Producer]; ok && !slices.ContainsFunc(recs, func(r store.Record) bool {
		return r.Producer == d.Producer && !r.EventTime.Before(c.from) && !r.EventTime.After(x)
	}) {
		return nil, c.fail(&RetainedError{Subject: sub, At: x, Producer: d.Producer, Seed: seed})
	}
	return d, nil
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
		ends: map[identity.Fingerprint]*existence{},
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

// since reads, in one store read, the window and the seeds of each of fps in one
// layer and direction (0 for their own existence), at the call's token, with
// the seeds' payloads when the answer describes what it reads.
func (c *call) since(fps []identity.Fingerprint, dir store.Direction, l catalog.Layer, payloads bool) ([]store.Since, error) {
	return c.r.st.WindowSince(c.ctx, fps, dir, c.from, c.to, c.scope(l), store.SinceOptions{Payloads: payloads})
}

// sinceRecords are the records one answer of WindowSince gives to fold: the
// seeds, which stand for everything before From, then the window. unknown lists,
// per subject and producer, the seed that carries no payload, when payloads were
// asked for (a reference has one seed, so there is one per subject and producer).
func sinceRecords(s store.Since) (recs []store.Record, unknown map[store.Subject]map[lifecycle.Producer]store.Record) {
	recs = make([]store.Record, 0, len(s.Seeds)+len(s.Window))
	for _, sd := range s.Seeds {
		recs = append(recs, sd.Record)
		if !sd.PayloadKnown {
			if unknown == nil {
				unknown = map[store.Subject]map[lifecycle.Producer]store.Record{}
			}
			if unknown[sd.Record.Subject] == nil {
				unknown[sd.Record.Subject] = map[lifecycle.Producer]store.Record{}
			}
			unknown[sd.Record.Subject][sd.Record.Producer] = sd.Record
		}
	}
	return append(recs, s.Window...), unknown
}

func (r *Reader) batch(ctx context.Context, op string, fps []identity.Fingerprint, dir store.Direction, w Window) (*call, [][]Element, error) {
	c, err := r.begin(ctx, op, w, true, dir, fps...)
	if err != nil {
		return nil, nil, err
	}
	var distinct []identity.Fingerprint
	first := map[identity.Fingerprint]int{}
	for _, fp := range fps {
		if _, ok := first[fp]; !ok {
			first[fp] = len(distinct)
			distinct = append(distinct, fp)
		}
	}
	answers, err := c.edges(distinct, dir)
	if err != nil {
		return nil, nil, err
	}
	out := make([][]Element, len(fps))
	given := make([]bool, len(distinct))
	for i, fp := range fps {
		j := first[fp]
		if given[j] {
			out[i] = cloneElements(answers[j])
			continue
		}
		out[i], given[j] = answers[j], true
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

// edgeRead is one edge a call has read and folded: its records (seeds and
// window), the seeds that have no payload by producer, and its fold.
type edgeRead struct {
	g       group
	layer   catalog.Layer
	unknown map[lifecycle.Producer]store.Record
	f       folded
	// own says the edge has an interval of its own in the window.
	own bool
}

// edges reads the edges of distinct fingerprints: one WindowSince per edge layer
// at the call's token, of every fingerprint whose type has edges there; then the
// existence of the endpoints of the edges with an interval of their own in the
// window, at most once each; then each fingerprint's elements, ordered by peer
// then relation. out[i] answers fps[i].
func (c *call) edges(fps []identity.Fingerprint, dir store.Direction) ([][]Element, error) {
	reads := make([][]edgeRead, len(fps))
	byLayer := map[catalog.Layer][]int{}
	var layers []catalog.Layer
	for i, fp := range fps {
		for _, l := range EdgeLayers(c.r.cat, fp.Type(), dir) {
			if _, ok := byLayer[l]; !ok {
				layers = append(layers, l)
			}
			byLayer[l] = append(byLayer[l], i)
		}
	}
	slices.Sort(layers)
	for _, l := range layers {
		idx := byLayer[l]
		of := make([]identity.Fingerprint, len(idx))
		for k, i := range idx {
			of[k] = fps[i]
		}
		answers, err := c.since(of, dir, l, c.r.opts.Describe)
		if err != nil {
			return nil, c.fail(fmt.Errorf("reading the %s edges of %d entities in layer %s: %w", dir, len(of), l, err))
		}
		for k, i := range idx {
			recs, unknown := sinceRecords(answers[k])
			for _, g := range groupBySubject(recs) {
				if err := c.checkEdge(fps[i], dir, l, g.sub); err != nil {
					return nil, err
				}
				f, err := foldSubject(g.recs)
				if err != nil {
					return nil, c.fail(err)
				}
				own := slices.ContainsFunc(f.ivs, func(iv ival) bool { return iv.meets(c.from, c.to) })
				reads[i] = append(reads[i], edgeRead{g: g, layer: l, unknown: unknown[g.sub], f: f, own: own})
			}
		}
	}
	if !c.r.opts.Raw {
		// Only an edge with an interval of its own in the window has a piece,
		// cut or uncut, so only its endpoints are read: the cascade costs one
		// existence read per endpoint of an edge in the window.
		var ends []identity.Fingerprint
		for _, rs := range reads {
			for _, e := range rs {
				if e.own || c.r.everyEndpoint {
					ends = append(ends, e.g.sub.A, e.g.sub.B)
				}
			}
		}
		if err := c.existences(ends); err != nil {
			return nil, err
		}
	}
	peer := func(s store.Subject) identity.Fingerprint {
		if dir == store.Forward {
			return s.B
		}
		return s.A
	}
	out := make([][]Element, len(fps))
	for i, rs := range reads {
		for _, e := range rs {
			el, err := c.edge(e)
			if err != nil {
				return nil, err
			}
			if len(el.Pieces) > 0 {
				out[i] = append(out[i], el)
			}
		}
		slices.SortFunc(out[i], func(a, b Element) int {
			if c := store.CompareFingerprints(peer(a.Subject), peer(b.Subject)); c != 0 {
				return c
			}
			return cmp.Compare(a.Subject.Relation, b.Subject.Relation)
		})
	}
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

// edge cuts a folded edge by its endpoints unless the reader is raw, and clips
// it to the window. An edge with no interval of its own in the window has no
// piece, and its endpoints were not read.
func (c *call) edge(e edgeRead) (Element, error) {
	el := Element{Subject: e.g.sub, Layer: e.layer}
	if !e.own {
		return el, nil
	}
	ivs := e.f.ivs
	if !c.r.opts.Raw {
		a, b := c.ends[e.g.sub.A], c.ends[e.g.sub.B]
		switch {
		case a.quarantine != nil:
			el.Uncascaded = a.quarantine
		case b.quarantine != nil:
			el.Uncascaded = b.quarantine
		default:
			ivs = cut(cut(ivs, a.f.ivs), b.f.ivs)
		}
	}
	var err error
	if el.Pieces, err = clip(ivs, c.from, c.to); err != nil {
		return Element{}, c.fail(fmt.Errorf("%s: %w", describeSubject(e.g.sub), err))
	}
	attribute(el.Pieces, e.f)
	el.Class, el.Transitions = classify(el.Pieces, c.from, c.to)
	if err := c.describe(&el, e.g.recs, e.unknown); err != nil {
		return Element{}, err
	}
	if err := c.evidence(&el, e.g.recs, e.f); err != nil {
		return Element{}, err
	}
	return el, nil
}

// existences reads the existence of each entity of fps the call has not read
// yet, in its own catalog layer: an Alive each, for the quarantine, in the order
// of fps, then one WindowSince per entity layer for those not quarantined, and
// folds each from its seeds and window.
func (c *call) existences(fps []identity.Fingerprint) error {
	byLayer := map[catalog.Layer][]identity.Fingerprint{}
	var layers []catalog.Layer
	for _, fp := range fps {
		if _, ok := c.ends[fp]; ok {
			continue
		}
		ent, ok := c.r.cat.Entity(fp.Type())
		if !ok {
			return c.fail(fmt.Errorf("entity %s has an unknown type: %w", fp, store.ErrInvalid))
		}
		l := ent.Layer()
		ex := &existence{layer: l}
		c.ends[fp] = ex
		if _, err := c.r.st.Alive(c.ctx, fp, c.from, c.scope(l)); err != nil {
			var qe *store.QuarantineError
			if !errors.As(err, &qe) {
				return c.fail(fmt.Errorf("reading whether %s is quarantined: %w", fp, err))
			}
			ex.quarantine = qe
			continue
		}
		if _, ok := byLayer[l]; !ok {
			layers = append(layers, l)
		}
		byLayer[l] = append(byLayer[l], fp)
	}
	slices.Sort(layers)
	for _, l := range layers {
		of := byLayer[l]
		// The payloads are for descriptions alone: evidence is every field of a
		// seed but the payload.
		answers, err := c.since(of, 0, l, c.keepRecords)
		if err != nil {
			return c.fail(fmt.Errorf("reading the existence of %d entities in layer %s: %w", len(of), l, err))
		}
		for k, fp := range of {
			recs, unknown := sinceRecords(answers[k])
			ex := c.ends[fp]
			if ex.f, err = foldSubject(recs); err != nil {
				return c.fail(err)
			}
			if c.keepRecords || c.keepEvidence {
				ex.recs, ex.unknown = recs, unknown[store.EntitySubject(fp)]
			}
		}
	}
	return nil
}

// cloneElements copies elements, so two answers of one batch share nothing a
// caller may change.
func cloneElements(els []Element) []Element {
	out := slices.Clone(els)
	for i := range out {
		out[i] = cloneElement(out[i])
	}
	return out
}

// cloneElement copies an element's pieces, descriptions and evidence.
func cloneElement(el Element) Element {
	el.Pieces = slices.Clone(el.Pieces)
	for k := range el.Pieces {
		el.Pieces[k].Producers = slices.Clone(el.Pieces[k].Producers)
	}
	el.AtFrom, el.AtLast = el.AtFrom.clone(), el.AtLast.clone()
	el.Last = slices.Clone(el.Last)
	return el
}
