package bytesgate_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/subgraph"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/wire"
)

// The reader below is written from the description of the Arrow streams and from
// what the wire format derives, apart from the writer. It rebuilds a document from
// the twelve streams, so a column the writer drops, or adds, or writes wrongly,
// makes the rebuilt document differ from the one that was written.

func u8(a arrow.Array, i int) uint8     { return a.(*array.Uint8).Value(i) }
func u16(a arrow.Array, i int) uint16   { return a.(*array.Uint16).Value(i) }
func u32(a arrow.Array, i int) uint32   { return a.(*array.Uint32).Value(i) }
func u64(a arrow.Array, i int) uint64   { return a.(*array.Uint64).Value(i) }
func i64(a arrow.Array, i int) int64    { return a.(*array.Int64).Value(i) }
func flag(a arrow.Array, i int) bool    { return a.(*array.Boolean).Value(i) }
func text(a arrow.Array, i int) string  { return a.(*array.String).Value(i) }
func fixed(a arrow.Array, i int) []byte { return a.(*array.FixedSizeBinary).Value(i) }

// inList is the span of a list value: its bounds in the list's child, and the child.
func inList(a arrow.Array, i int) (lo, hi int, child arrow.Array) {
	l := a.(*array.List)
	s, e := l.ValueOffsets(i)
	return int(s), int(e), l.ListValues()
}

func structOfList(a arrow.Array, i int) (lo, hi int, st *array.Struct) {
	lo, hi, child := inList(a, i)
	return lo, hi, child.(*array.Struct)
}

type reading struct {
	t     testing.TB
	es    []identity.Fingerprint
	width uint64
}

// fromBytes is a fingerprint written whole: the type's number and the hash.
func fromBytes(t testing.TB, b []byte) identity.Fingerprint {
	t.Helper()
	ent, ok := catalog.Default().EntityByID(catalog.EntityID(uint16(b[0])<<8 | uint16(b[1])))
	if !ok {
		t.Fatalf("the entity number %x is not in the catalog", b[:2])
	}
	fp, err := identity.FingerprintFromHash(ent.Type(), [identity.FingerprintBytes]byte(b[2:]))
	if err != nil {
		t.Fatal(err)
	}
	return fp
}

// ref reads an entity reference: an index into the entities, or a value of a
// dictionary column.
func (r *reading) ref(a arrow.Array, i int) identity.Fingerprint {
	r.t.Helper()
	switch c := a.(type) {
	case *array.Uint32:
		if int(c.Value(i)) >= len(r.es) {
			r.t.Fatalf("the reference %d is not one of %d entities", c.Value(i), len(r.es))
		}
		return r.es[c.Value(i)]
	case *array.Dictionary:
		return fromBytes(r.t, c.Dictionary().(*array.FixedSizeBinary).Value(c.GetValueIndex(i)))
	}
	r.t.Fatalf("a column of %T where an entity reference was meant", a)
	return identity.Fingerprint{}
}

func (r *reading) refs(a arrow.Array, i int) []identity.Fingerprint {
	lo, hi, child := inList(a, i)
	var out []identity.Fingerprint
	for k := lo; k < hi; k++ {
		out = append(out, r.ref(child, k))
	}
	return out
}

func relationOf(t testing.TB, id uint16) catalog.Relation {
	t.Helper()
	rel, ok := catalog.Default().RelationByID(catalog.RelationID(id))
	if !ok {
		t.Fatalf("the relation number %d is not in the catalog", id)
	}
	return rel
}

func (r *reading) spans(a arrow.Array, i int) []subgraph.Span {
	lo, hi, st := structOfList(a, i)
	var out []subgraph.Span
	for k := lo; k < hi; k++ {
		s := subgraph.Span{
			Start: uint64(i64(st.Field(0), k)), End: uint64(i64(st.Field(1), k)), Before: flag(st.Field(2), k),
			EndReason: subgraph.EndReasonReachEnded,
		}
		// What the wire format derives: a span that reaches the end of the window
		// ends for no reason that is known, and one that does not ends where an
		// ambiguity or an intersection does.
		if s.End == r.width {
			s.EndReason = subgraph.EndReasonUnknown
		}
		out = append(out, s)
	}
	return out
}

// element reads one element from the fields of a record or of a struct.
func (r *reading) element(f func(int) arrow.Array, i int) subgraph.Element {
	key := subgraph.Key{A: r.ref(f(0), i)}
	el := subgraph.Element{}
	switch {
	case f(1).IsNull(i):
		key.Kind = subgraph.KindEntity
		el.Rule = subgraph.RuleExistence
	case !f(2).IsNull(i):
		key.Kind = subgraph.KindEdge
		key.B = r.ref(f(1), i)
		rel := relationOf(r.t, u16(f(2), i))
		key.Relation = rel.Type()
		el.Rule = subgraph.RuleExistence
		if rel.Storage() == catalog.StorageDerived {
			el.Rule = string(rel.Type())
		}
	default:
		key.Kind = subgraph.KindDerived
		key.B = r.ref(f(1), i)
		dk := subgraph.DerivedKind(u8(f(3), i))
		key.Rule, key.Detail = dk.Rule(), text(f(4), i)
		el.Rule = key.Rule
	}
	el.Key = key
	el.Subject = subgraph.Subject(u8(f(5), i))
	el.Class = subgraph.Class(u8(f(6), i))
	el.Transitions = int(u32(f(7), i))
	plo, phi, legs := inList(f(8), i)
	for p := plo; p < phi; p++ {
		lo, hi, ids := inList(legs, p)
		var path []uint32
		for k := lo; k < hi; k++ {
			path = append(path, u32(ids, k))
		}
		el.Paths = append(el.Paths, path)
	}
	lo, hi, st := structOfList(f(9), i)
	for k := lo; k < hi; k++ {
		p := subgraph.Piece{
			Start: uint64(i64(st.Field(0), k)), End: uint64(i64(st.Field(1), k)), Before: flag(st.Field(2), k),
			EndReason: subgraph.EndReason(u8(st.Field(3), k)),
		}
		plo, phi, names := inList(st.Field(4), k)
		for q := plo; q < phi; q++ {
			p.Producers = append(p.Producers, lifecycle.Producer(text(names, q)))
		}
		elo, ehi, ends := structOfList(st.Field(5), k)
		for q := elo; q < ehi; q++ {
			p.Ends = append(p.Ends, subgraph.LegEnd{
				Path: int(u32(ends.Field(0), q)), Leg: int(u32(ends.Field(1), q)), Reason: subgraph.EndReason(u8(ends.Field(2), q)),
			})
		}
		el.Pieces = append(el.Pieces, p)
	}
	return el
}

func structFields(st *array.Struct) func(int) arrow.Array { return st.Field }

func (r *reading) collision(a arrow.Array, i int) *lifecycle.CloneCollisionError {
	if a.IsNull(i) {
		return nil
	}
	st := a.(*array.Struct)
	return &lifecycle.CloneCollisionError{
		StaleBoot: text(st.Field(0), i), ObservedAt: time.Unix(0, i64(st.Field(1), i)).UTC(),
		NewerBoot: text(st.Field(2), i), NewerFirstSeen: time.Unix(0, i64(st.Field(3), i)).UTC(),
	}
}

func (r *reading) truncation(f func(int) arrow.Array, i int) subgraph.Truncation {
	t := subgraph.Truncation{
		Nodes: r.refs(f(0), i), Hosts: r.refs(f(1), i), Members: r.refs(f(2), i), Instances: r.refs(f(3), i),
		SeedPods: r.refs(f(4), i), Depth: r.refs(f(5), i),
		Results: int(u32(f(8), i)), Pairs: int(u32(f(9), i)),
		MembersDropped: int(u32(f(10), i)), SuppressedDropped: int(u32(f(11), i)),
	}
	lo, hi, st := structOfList(f(6), i)
	for k := lo; k < hi; k++ {
		t.Frontier = append(t.Frontier, subgraph.FrontierCut{Depth: int(u32(st.Field(0), k)), Dropped: r.refs(st.Field(1), k)})
	}
	lo, hi, st = structOfList(f(7), i)
	for k := lo; k < hi; k++ {
		t.PerVia = append(t.PerVia, subgraph.ViaCount{
			Via: r.ref(st.Field(0), k), Mechanism: subgraph.Mechanism(u8(st.Field(1), k)), Dropped: int(u32(st.Field(2), k)),
		})
	}
	return t
}

func (r *reading) suppressed(f func(int) arrow.Array, i int) subgraph.Suppressed {
	dk := subgraph.DerivedKind(u8(f(2), i))
	x := subgraph.Suppressed{
		Key: subgraph.Key{
			Kind: subgraph.KindDerived, A: r.ref(f(0), i), B: r.ref(f(1), i), Rule: dk.Rule(), Detail: text(f(3), i),
		},
		Reason: subgraph.SuppressReason(u8(f(4), i)),
		Class:  subgraph.Class(u8(f(7), i)), Transitions: int(u32(f(8), i)), Spans: r.spans(f(9), i),
	}
	if !f(5).IsNull(i) {
		x.Coverage = [2]int{int(u32(f(5), i)), int(u32(f(6), i))}
	}
	return x
}

// rebuild reads the streams of Arrow bytes back into the document they carry.
func rebuild(t testing.TB, b []byte) wire.Document {
	t.Helper()
	var recs []arrow.RecordBatch
	n := streamsOf(t, b, func(_ int, rec arrow.RecordBatch) {
		rec.Retain()
		recs = append(recs, rec)
	})
	t.Cleanup(func() {
		for _, rec := range recs {
			rec.Release()
		}
	})
	if n != 12 {
		t.Fatalf("%d streams", n)
	}
	r := &reading{t: t}
	col := func(stream int) func(int) arrow.Array { return recs[stream].Column }
	rows := func(stream int) int { return int(recs[stream].NumRows()) }

	var d wire.Document
	sg := &d.Subgraph
	h := col(0)
	sg.Basis = subgraph.Basis(u8(h(2), 0))
	sg.Token = u64(h(3), 0)
	sg.From, sg.To = time.Unix(0, i64(h(4), 0)).UTC(), time.Unix(0, i64(h(5), 0)).UTC()
	sg.PolicyVersion = int(u32(h(6), 0))
	d.Policy = wire.Policy{
		Interval: int(u32(h(7), 0)), CoResidency: int(u32(h(8), 0)), Extract: int(u32(h(9), 0)),
		BlastRadius: int(u32(h(10), 0)), GraphDiff: int(u32(h(11), 0)), IdentityEncoding: int(u32(h(12), 0)),
	}
	copy(d.Policy.CatalogIDs[:], fixed(h(13), 0))
	if rank := h(14).(*array.Binary).Value(0); len(rank) > 0 {
		d.Policy.Rank = bytes.Clone(rank)
	}
	d.Query.Kind = wire.QueryKind(u8(h(15), 0))
	lo, hi, seeds := inList(h(16), 0)
	for k := lo; k < hi; k++ {
		d.Query.Seeds = append(d.Query.Seeds, fromBytes(t, fixed(seeds, k)))
	}
	lo, hi, opts := structOfList(h(17), 0)
	for k := lo; k < hi; k++ {
		d.Query.Options = append(d.Query.Options, wire.Option{Name: text(opts.Field(0), k), Value: i64(opts.Field(1), k)})
	}
	d.Query.AsOf = u64(h(18), 0)
	r.width = uint64(i64(h(5), 0) - i64(h(4), 0))

	e := col(1)
	for i := range rows(1) {
		fp := fromBytes(t, fixed(e(0), i))
		sg.Entities = append(sg.Entities, subgraph.Entity{Fingerprint: fp, Boundary: flag(e(1), i)})
		r.es = append(r.es, fp)
	}
	for i := range rows(2) {
		sg.Elements = append(sg.Elements, r.element(col(2), i))
	}
	kf := col(3)
	for i := range rows(3) {
		lo, hi, ids := inList(kf(1), i)
		k := subgraph.Keyframe{At: uint64(i64(kf(0), i))}
		for j := lo; j < hi; j++ {
			k.Elements = append(k.Elements, u32(ids, j))
		}
		sg.Keyframes = append(sg.Keyframes, k)
	}
	dl := col(4)
	for i := range rows(4) {
		// What the wire format derives: the other side of a delta is present.
		dd := subgraph.Delta{At: uint64(i64(dl(0), i)), Element: u32(dl(1), i)}
		reason := subgraph.EndReason(u8(dl(3), i))
		switch op := u8(dl(2), i); op {
		case 1:
			dd.Before, dd.After = subgraph.State{Reason: reason}, subgraph.State{Present: true}
		case 2:
			dd.Before, dd.After = subgraph.State{Present: true}, subgraph.State{Reason: reason}
		default:
			t.Fatalf("the delta operation %d", op)
		}
		sg.Deltas = append(sg.Deltas, dd)
	}

	q := col(5)
	lo, hi, qs := structOfList(q(0), 0)
	for k := lo; k < hi; k++ {
		fp := r.ref(qs.Field(0), k)
		cause := &store.QuarantineError{Entity: fp, Layer: catalog.Layer(u8(qs.Field(1), k)), Collision: r.collision(qs.Field(2), k)}
		sg.Quarantine.Entities = append(sg.Quarantine.Entities, subgraph.QuarantinedEntity{Fingerprint: fp, Cause: cause})
	}
	lo, hi, qe := structOfList(q(1), 0)
	for k := lo; k < hi; k++ {
		el := r.element(structFields(qe.Field(0).(*array.Struct)), k)
		entity := el.Key.A
		if flag(qe.Field(1), k) {
			entity = el.Key.B
		}
		cause := &store.QuarantineError{Entity: entity, Layer: catalog.Layer(u8(qe.Field(2), k)), Collision: r.collision(qe.Field(3), k)}
		sg.Quarantine.Edges = append(sg.Quarantine.Edges, subgraph.StaticEdge{Element: el, Cause: cause})
	}

	s := &sg.Sections
	s.Truncated = r.truncation(col(6), 0)
	un := col(7)
	for i := range rows(7) {
		x := subgraph.Unattributed{
			Subject: r.ref(un(1), i), Reason: subgraph.UnattributedReason(u8(un(2), i)),
			Spans: r.spans(un(5), i), Candidates: r.refs(un(6), i),
		}
		if !un(0).IsNull(i) {
			x.SeedPod = r.ref(un(0), i)
		}
		if !un(3).IsNull(i) {
			ent, ok := catalog.Default().EntityByID(catalog.EntityID(u16(un(4), i)))
			if !ok {
				t.Fatalf("the target number %d is not in the catalog", u16(un(4), i))
			}
			x.Relation, x.Target = relationOf(t, u16(un(3), i)).Type(), ent.Type()
		}
		s.Unattributed = append(s.Unattributed, x)
	}
	um := col(8)
	for i := range rows(8) {
		s.Unmapped = append(s.Unmapped, subgraph.Unmapped{Pod: r.ref(um(0), i), Spans: r.spans(um(1), i)})
	}
	for i := range rows(9) {
		s.Suppressed = append(s.Suppressed, r.suppressed(col(9), i))
	}
	pd := col(10)
	for i := range rows(10) {
		p := subgraph.PodSection{Seed: r.ref(pd(0), i), Truncated: r.truncation(structFields(pd(1).(*array.Struct)), i)}
		lo, hi, ks := structOfList(pd(2), i)
		for k := lo; k < hi; k++ {
			key := subgraph.Key{Kind: subgraph.Kind(u8(ks.Field(0), k)), A: r.ref(ks.Field(1), k)}
			if key.Kind == subgraph.KindEdge {
				key.B, key.Relation = r.ref(ks.Field(2), k), relationOf(t, u16(ks.Field(3), k)).Type()
			}
			p.Quarantined = append(p.Quarantined, key)
		}
		lo, hi, ss := structOfList(pd(3), i)
		for k := lo; k < hi; k++ {
			p.Suppressed = append(p.Suppressed, r.suppressed(structFields(ss), k))
		}
		s.Pods = append(s.Pods, p)
	}
	an := col(11)
	s.Anchor = subgraph.AnchorState(u8(an(0), 0))
	s.Ubiquity = subgraph.Ubiquity{Judged: flag(an(1), 0), FrontierNodes: int(u32(an(2), 0))}
	return d
}

// requireSame fails unless two documents have the same canonical bytes and the
// same canonical twin, which is what it is for two documents to be equal.
func requireSame(t testing.TB, name string, want, got wire.Document) {
	t.Helper()
	wb, err := wire.Encode(want)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	gb, err := wire.Encode(got)
	if err != nil {
		t.Errorf("%s: the document rebuilt from Arrow is not one the format accepts: %v", name, err)
		return
	}
	if !bytes.Equal(wb, gb) {
		t.Errorf("%s: the document rebuilt from Arrow is not the document written", name)
	}
	wj, _ := wire.JSON(want)
	gj, _ := wire.JSON(got)
	if !bytes.Equal(wj, gj) {
		t.Errorf("%s: the twins of the two documents differ", name)
	}
}
