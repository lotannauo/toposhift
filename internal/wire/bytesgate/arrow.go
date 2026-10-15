package bytesgate

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/query/subgraph"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/wire"
)

// Arrow writes a document as Arrow IPC: twelve streams, one for each section of
// the wire format, in the order of the format's tags, each with its own schema
// and one record batch, and each closed with the end-of-stream marker. A stream
// is not compressed, and a batch is written whole.
//
// An entity a section names is a uint32: its position in the entities section, as
// in the wire format, where it is a dictionary id. The entities section lists
// every entity once, as a fixed_size_binary(18) with its boundary flag: the 18
// bytes are the entity type's catalog number as two bytes, big-endian, and the 16
// bytes of the hash. The query's seeds are not references: as in the wire format
// they are written whole, as the same 18 bytes, because the header comes first
// and a seed need not be an entity of the dictionary. An element is named by its
// position in the elements section, as a uint32. A time relative to From is an
// int64 count of nanoseconds, and so is every absolute one. A flag is a bool, an
// enumeration its frozen number as a uint8, and an optional value a null.
//
// Arrow carries what the wire format writes and no more. The wire format derives
// some values and does not write them, and so does Arrow: an element's kind (an
// entity has no second end and no relation, an edge has a relation, a derived
// subject has a derived kind), its rule (existence, or the derived kind's rule,
// or the relation's name for a derived relation), the end reason of a span of a
// section (unknown where it reaches To, reach_ended before), the entity that
// caused a static edge to be held (the edge writes which end it is, as a flag),
// the state on the other side of a delta (the delta writes its operation and the
// reason of the absent side), and the entity a quarantined entity's cause names
// (it is the entity).
//
// The sections, as the schemas of the streams, a to be an entity reference:
//
//	header        one row: format, basis, token, from, to, the policy numbers,
//	              the catalog digest, the rank, the query kind, seeds, options, asof
//	entities      one row per entity: fingerprint, boundary
//	elements      one row per element: a, b, relation, derived_kind, detail,
//	              subject, class, transitions, paths list<list<uint32>>, and
//	              pieces list<struct<start, end, before, end_reason, producers,
//	              ends list<struct<path, leg, reason>>>>
//	keyframes     one row per keyframe: at, elements list<uint32>
//	deltas        one row per delta: at, element, op, reason
//	quarantine    one row: entities and edges, as lists of structs
//	truncated     one row: the lists and counts of a truncation
//	unattributed  one row per entry
//	unmapped      one row per pod
//	suppressed    one row per entry
//	pods          one row per pod-level search
//	anchor        one row
//
// A reader of these streams rebuilds the document the format carries: the tests
// do, and require it to be the document that was written.
func Arrow(d wire.Document) ([]byte, error) { return writeArrow(d, true) }

// ArrowDictionaries is [Arrow] with every entity reference a column of the type
// dictionary(int32, fixed_size_binary(18)) in place of an index, so that Arrow
// writes into each stream a dictionary of the entities that stream names, and a
// fingerprint is written once for each stream that names it. It is reported
// beside Arrow for what it shows, and is not the encoding the gate decides on.
func ArrowDictionaries(d wire.Document) ([]byte, error) { return writeArrow(d, false) }

func writeArrow(d wire.Document, byIndex bool) ([]byte, error) {
	if _, err := wire.Encode(d); err != nil {
		return nil, fmt.Errorf("bytesgate: a document the format refuses: %w", err)
	}
	a := &arrowWriter{d: &d, mem: memory.NewGoAllocator(), byIndex: byIndex}
	if byIndex {
		a.ref = tU32
	} else {
		a.ref = fpDict
	}
	a.stream("header", a.headerFields(), a.header)
	a.stream("entities", entityFields(), a.entities)
	a.stream("elements", a.elementFields(), a.elements)
	a.stream("keyframes", keyframeFields(), a.keyframes)
	a.stream("deltas", deltaFields(), a.deltas)
	a.stream("quarantine", a.quarantineFields(), a.quarantine)
	a.stream("truncated", a.truncationFields(), func(get cols) { a.truncation(get, d.Subgraph.Sections.Truncated) })
	a.stream("unattributed", a.unattributedFields(), a.unattributed)
	a.stream("unmapped", a.unmappedFields(), a.unmapped)
	a.stream("suppressed", a.suppressedFields(), func(get cols) { a.suppressedRows(get, d.Subgraph.Sections.Suppressed) })
	a.stream("pods", a.podFields(), a.pods)
	a.stream("anchor", anchorFields(), a.anchor)
	if a.err != nil {
		return nil, a.err
	}
	return a.out.Bytes(), nil
}

// The column types. A fingerprint is 18 bytes: the type's number and the hash.
var (
	fpValue = &arrow.FixedSizeBinaryType{ByteWidth: 2 + identity.FingerprintBytes}
	fpDict  = &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: fpValue}

	tU8, tU16, tU32, tU64 = arrow.PrimitiveTypes.Uint8, arrow.PrimitiveTypes.Uint16, arrow.PrimitiveTypes.Uint32, arrow.PrimitiveTypes.Uint64
	tI64                  = arrow.PrimitiveTypes.Int64
)

func req(name string, t arrow.DataType) arrow.Field { return arrow.Field{Name: name, Type: t} }

func opt(name string, t arrow.DataType) arrow.Field {
	return arrow.Field{Name: name, Type: t, Nullable: true}
}

func listOf(t arrow.DataType) arrow.DataType { return arrow.ListOfNonNullable(t) }

func structOf(fs ...arrow.Field) *arrow.StructType { return arrow.StructOf(fs...) }

// refList is a list of entity references.
func (a *arrowWriter) refList() arrow.DataType { return listOf(a.ref) }

func (a *arrowWriter) headerFields() []arrow.Field {
	return []arrow.Field{
		req("format_major", tU8), req("format_minor", tU8), req("basis", tU8),
		req("token", tU64), req("from", tI64), req("to", tI64),
		req("policy_subgraph", tU32), req("policy_interval", tU32), req("policy_coresidency", tU32),
		req("policy_extract", tU32), req("policy_blastradius", tU32), req("policy_graphdiff", tU32),
		req("identity_encoding", tU32),
		req("catalog_ids", &arrow.FixedSizeBinaryType{ByteWidth: 32}),
		req("rank", arrow.BinaryTypes.Binary),
		req("query_kind", tU8), req("seeds", listOf(fpValue)),
		req("options", listOf(structOf(req("name", arrow.BinaryTypes.String), req("value", tI64)))),
		req("asof", tU64),
	}
}

func entityFields() []arrow.Field {
	return []arrow.Field{req("fingerprint", fpValue), req("boundary", arrow.FixedWidthTypes.Boolean)}
}

func legEndType() arrow.DataType {
	return structOf(req("path", tU32), req("leg", tU32), req("reason", tU8))
}

func pieceType() arrow.DataType {
	return structOf(
		req("start", tI64), req("end", tI64), req("before", arrow.FixedWidthTypes.Boolean),
		req("end_reason", tU8), req("producers", listOf(arrow.BinaryTypes.String)),
		req("ends", listOf(legEndType())),
	)
}

func (a *arrowWriter) elementFields() []arrow.Field {
	return []arrow.Field{
		req("a", a.ref), opt("b", a.ref), opt("relation", tU16),
		opt("derived_kind", tU8), opt("detail", arrow.BinaryTypes.String),
		req("subject", tU8), req("class", tU8), req("transitions", tU32),
		req("paths", listOf(listOf(tU32))), req("pieces", listOf(pieceType())),
	}
}

func keyframeFields() []arrow.Field {
	return []arrow.Field{req("at", tI64), req("elements", listOf(tU32))}
}

func deltaFields() []arrow.Field {
	return []arrow.Field{req("at", tI64), req("element", tU32), req("op", tU8), req("reason", tU8)}
}

func collisionType() arrow.DataType {
	return structOf(
		req("stale_boot", arrow.BinaryTypes.String), req("observed_at", tI64),
		req("newer_boot", arrow.BinaryTypes.String), req("newer_first_seen", tI64),
	)
}

func (a *arrowWriter) quarantineFields() []arrow.Field {
	entities := structOf(req("entity", a.ref), req("layer", tU8), opt("collision", collisionType()))
	edges := structOf(
		req("element", structOf(a.elementFields()...)), req("side", arrow.FixedWidthTypes.Boolean),
		req("layer", tU8), opt("collision", collisionType()),
	)
	return []arrow.Field{req("entities", listOf(entities)), req("edges", listOf(edges))}
}

func (a *arrowWriter) truncationFields() []arrow.Field {
	return []arrow.Field{
		req("nodes", a.refList()), req("hosts", a.refList()), req("members", a.refList()), req("instances", a.refList()),
		req("seed_pods", a.refList()), req("depth", a.refList()),
		req("frontier", listOf(structOf(req("depth", tU32), req("dropped", a.refList())))),
		req("per_via", listOf(structOf(req("via", a.ref), req("mechanism", tU8), req("dropped", tU32)))),
		req("results", tU32), req("pairs", tU32), req("members_dropped", tU32), req("suppressed_dropped", tU32),
	}
}

func spanType() arrow.DataType {
	return structOf(req("start", tI64), req("end", tI64), req("before", arrow.FixedWidthTypes.Boolean))
}

func (a *arrowWriter) unattributedFields() []arrow.Field {
	return []arrow.Field{
		opt("seed_pod", a.ref), req("subject", a.ref), req("reason", tU8),
		opt("relation", tU16), opt("target", tU16),
		req("spans", listOf(spanType())), req("candidates", a.refList()),
	}
}

func (a *arrowWriter) unmappedFields() []arrow.Field {
	return []arrow.Field{req("pod", a.ref), req("spans", listOf(spanType()))}
}

func (a *arrowWriter) suppressedFields() []arrow.Field {
	return []arrow.Field{
		req("a", a.ref), req("b", a.ref), req("derived_kind", tU8), req("detail", arrow.BinaryTypes.String),
		req("reason", tU8), opt("coverage_nodes", tU32), opt("coverage_frontier", tU32),
		req("class", tU8), req("transitions", tU32), req("spans", listOf(spanType())),
	}
}

func (a *arrowWriter) plainKeyFields() []arrow.Field {
	return []arrow.Field{req("kind", tU8), req("a", a.ref), opt("b", a.ref), opt("relation", tU16)}
}

func (a *arrowWriter) podFields() []arrow.Field {
	return []arrow.Field{
		req("seed", a.ref), req("truncated", structOf(a.truncationFields()...)),
		req("quarantined", listOf(structOf(a.plainKeyFields()...))),
		req("suppressed", listOf(structOf(a.suppressedFields()...))),
	}
}

func anchorFields() []arrow.Field {
	return []arrow.Field{req("anchor", tU8), req("judged", arrow.FixedWidthTypes.Boolean), req("frontier_nodes", tU32)}
}

// cols gives the builder of the i-th field of a record or of a struct.
type cols func(i int) array.Builder

type arrowWriter struct {
	d   *wire.Document
	mem memory.Allocator
	out bytes.Buffer
	err error
	// byIndex says an entity reference is its index in the entities section;
	// otherwise it is a column of the dictionary type. ref is that column's type.
	byIndex bool
	ref     arrow.DataType
}

func (a *arrowWriter) fail(err error) {
	if a.err == nil {
		a.err = err
	}
}

// stream writes one section: its schema, and the one batch fill makes.
func (a *arrowWriter) stream(name string, fields []arrow.Field, fill func(cols)) {
	if a.err != nil {
		return
	}
	schema := arrow.NewSchema(fields, nil)
	rb := array.NewRecordBuilder(a.mem, schema)
	defer rb.Release()
	fill(rb.Field)
	if a.err != nil {
		return
	}
	rec := rb.NewRecordBatch()
	defer rec.Release()
	w := ipc.NewWriter(&a.out, ipc.WithSchema(schema), ipc.WithAllocator(a.mem))
	if err := w.Write(rec); err != nil {
		a.fail(fmt.Errorf("bytesgate: the %s stream: %w", name, err))
		return
	}
	if err := w.Close(); err != nil {
		a.fail(fmt.Errorf("bytesgate: the %s stream: %w", name, err))
	}
}

func fpBytes(fp identity.Fingerprint) ([]byte, error) {
	e, ok := catalog.Default().Entity(fp.Type())
	if !ok {
		return nil, fmt.Errorf("bytesgate: the entity type %q is not in the catalog: %w", fp.Type(), store.ErrInvalid)
	}
	h := fp.Hash()
	out := binary.BigEndian.AppendUint16(make([]byte, 0, len(h)+2), uint16(e.ID()))
	return append(out, h[:]...), nil
}

// The put functions append one value to a builder of the type they name. A
// builder of another type is a bug in this file and is reported as an error.
func put[B array.Builder, V any](a *arrowWriter, b array.Builder, v V, add func(B, V)) {
	x, ok := b.(B)
	if !ok {
		a.fail(fmt.Errorf("bytesgate: a column of %T where %T was meant", b, x))
		return
	}
	add(x, v)
}

func (a *arrowWriter) u8(b array.Builder, v uint8) {
	put(a, b, v, (*array.Uint8Builder).Append)
}

func (a *arrowWriter) u16(b array.Builder, v uint16) {
	put(a, b, v, (*array.Uint16Builder).Append)
}

func (a *arrowWriter) u32(b array.Builder, v uint32) {
	put(a, b, v, (*array.Uint32Builder).Append)
}

func (a *arrowWriter) n(b array.Builder, v int) { a.u32(b, uint32(v)) }

func (a *arrowWriter) u64(b array.Builder, v uint64) {
	put(a, b, v, (*array.Uint64Builder).Append)
}

func (a *arrowWriter) i64(b array.Builder, v int64) {
	put(a, b, v, (*array.Int64Builder).Append)
}

func (a *arrowWriter) flag(b array.Builder, v bool) {
	put(a, b, v, (*array.BooleanBuilder).Append)
}

func (a *arrowWriter) str(b array.Builder, v string) {
	put(a, b, v, (*array.StringBuilder).Append)
}

func (a *arrowWriter) bin(b array.Builder, v []byte) {
	put(a, b, v, (*array.BinaryBuilder).Append)
}

func (a *arrowWriter) fsb(b array.Builder, v []byte) {
	put(a, b, v, (*array.FixedSizeBinaryBuilder).Append)
}

// fp appends an entity reference: its index, or the fingerprint into a
// dictionary column.
func (a *arrowWriter) fp(b array.Builder, fp identity.Fingerprint) {
	if a.byIndex {
		id, ok := a.d.Subgraph.EntityID(fp)
		if !ok {
			a.fail(fmt.Errorf("bytesgate: the entity %s is not in the dictionary: %w", fp, store.ErrInvalid))
			return
		}
		a.u32(b, id)
		return
	}
	raw, err := fpBytes(fp)
	if err != nil {
		a.fail(err)
		return
	}
	x, ok := b.(*array.FixedSizeBinaryDictionaryBuilder)
	if !ok {
		a.fail(fmt.Errorf("bytesgate: a column of %T where a fingerprint was meant", b))
		return
	}
	if err := x.Append(raw); err != nil {
		a.fail(err)
	}
}

// whole appends a fingerprint written out in full, as the 18 bytes.
func (a *arrowWriter) whole(b array.Builder, fp identity.Fingerprint) {
	raw, err := fpBytes(fp)
	if err != nil {
		a.fail(err)
		return
	}
	a.fsb(b, raw)
}

func (a *arrowWriter) fps(b array.Builder, fs []identity.Fingerprint) {
	lb := a.list(b)
	for _, fp := range fs {
		a.fp(lb.ValueBuilder(), fp)
	}
}

// list starts one list value and gives its element builder.
func (a *arrowWriter) list(b array.Builder) *array.ListBuilder {
	lb, ok := b.(*array.ListBuilder)
	if !ok {
		a.fail(fmt.Errorf("bytesgate: a column of %T where a list was meant", b))
		return array.NewListBuilder(a.mem, tU8)
	}
	lb.Append(true)
	return lb
}

// row starts one struct value, or a null one, and gives the builders of its
// fields.
func (a *arrowWriter) row(b array.Builder, present bool) (*array.StructBuilder, cols) {
	sb, ok := b.(*array.StructBuilder)
	if !ok {
		a.fail(fmt.Errorf("bytesgate: a column of %T where a struct was meant", b))
		return nil, func(int) array.Builder { return nil }
	}
	if !present {
		sb.AppendNull()
		return sb, sb.FieldBuilder
	}
	sb.Append(true)
	return sb, sb.FieldBuilder
}

func isNull(b array.Builder) { b.AppendNull() }

func (a *arrowWriter) header(get cols) {
	d := a.d
	sg := &d.Subgraph
	a.u8(get(0), wire.FormatMajor)
	a.u8(get(1), wire.FormatMinor)
	a.u8(get(2), uint8(sg.Basis))
	a.u64(get(3), sg.Token)
	a.i64(get(4), sg.From.UnixNano())
	a.i64(get(5), sg.To.UnixNano())
	p := d.Policy
	for i, v := range []int{sg.PolicyVersion, p.Interval, p.CoResidency, p.Extract, p.BlastRadius, p.GraphDiff, p.IdentityEncoding} {
		a.n(get(6+i), v)
	}
	a.fsb(get(13), p.CatalogIDs[:])
	a.bin(get(14), p.Rank)
	q := d.Query
	a.u8(get(15), uint8(q.Kind))
	seeds := a.list(get(16))
	for _, fp := range q.Seeds {
		a.whole(seeds.ValueBuilder(), fp)
	}
	lb := a.list(get(17))
	for _, o := range q.Options {
		_, f := a.row(lb.ValueBuilder(), true)
		a.str(f(0), o.Name)
		a.i64(f(1), o.Value)
	}
	a.u64(get(18), q.AsOf)
}

func (a *arrowWriter) entities(get cols) {
	for _, e := range a.d.Subgraph.Entities {
		a.whole(get(0), e.Fingerprint)
		a.flag(get(1), e.Boundary)
	}
}

func relationID(r catalog.RelationType) (uint16, bool) {
	rel, ok := catalog.Default().Relation(r)
	return uint16(rel.ID()), ok
}

func entityTypeID(t catalog.EntityType) (uint16, bool) {
	e, ok := catalog.Default().Entity(t)
	return uint16(e.ID()), ok
}

// element appends one element as the fields of get.
func (a *arrowWriter) element(get cols, el subgraph.Element) {
	k := el.Key
	a.fp(get(0), k.A)
	switch k.Kind {
	case subgraph.KindEntity:
		isNull(get(1))
		isNull(get(2))
		isNull(get(3))
		isNull(get(4))
	case subgraph.KindEdge:
		a.fp(get(1), k.B)
		id, ok := relationID(k.Relation)
		if !ok {
			a.fail(fmt.Errorf("bytesgate: the relation %q is not registered: %w", k.Relation, store.ErrInvalid))
		}
		a.u16(get(2), id)
		isNull(get(3))
		isNull(get(4))
	case subgraph.KindDerived:
		a.fp(get(1), k.B)
		isNull(get(2))
		dk, _ := subgraph.DerivedKindOf(k.Rule)
		a.u8(get(3), uint8(dk))
		a.str(get(4), k.Detail)
	}
	a.u8(get(5), uint8(el.Subject))
	a.u8(get(6), uint8(el.Class))
	a.n(get(7), el.Transitions)
	paths := a.list(get(8))
	for _, p := range el.Paths {
		leg := a.list(paths.ValueBuilder())
		for _, id := range p {
			a.u32(leg.ValueBuilder(), id)
		}
	}
	pieces := a.list(get(9))
	for _, p := range el.Pieces {
		_, f := a.row(pieces.ValueBuilder(), true)
		a.i64(f(0), int64(p.Start))
		a.i64(f(1), int64(p.End))
		a.flag(f(2), p.Before)
		a.u8(f(3), uint8(p.EndReason))
		prods := a.list(f(4))
		for _, pr := range p.Producers {
			a.str(prods.ValueBuilder(), string(pr))
		}
		ends := a.list(f(5))
		for _, e := range p.Ends {
			_, g := a.row(ends.ValueBuilder(), true)
			a.n(g(0), e.Path)
			a.n(g(1), e.Leg)
			a.u8(g(2), uint8(e.Reason))
		}
	}
}

func (a *arrowWriter) elements(get cols) {
	for _, el := range a.d.Subgraph.Elements {
		a.element(get, el)
	}
}

func (a *arrowWriter) keyframes(get cols) {
	for _, k := range a.d.Subgraph.Keyframes {
		a.i64(get(0), int64(k.At))
		lb := a.list(get(1))
		for _, id := range k.Elements {
			a.u32(lb.ValueBuilder(), id)
		}
	}
}

func (a *arrowWriter) deltas(get cols) {
	for _, d := range a.d.Subgraph.Deltas {
		a.i64(get(0), int64(d.At))
		a.u32(get(1), d.Element)
		reason := d.After.Reason
		op := uint8(2)
		if d.Appear() {
			op, reason = 1, d.Before.Reason
		}
		a.u8(get(2), op)
		a.u8(get(3), uint8(reason))
	}
}

// collision appends the collision of a cause, or a null.
func (a *arrowWriter) collision(b array.Builder, c *store.QuarantineError) {
	cc := c.Collision
	_, f := a.row(b, cc != nil)
	if cc == nil {
		return
	}
	a.str(f(0), cc.StaleBoot)
	a.i64(f(1), cc.ObservedAt.UnixNano())
	a.str(f(2), cc.NewerBoot)
	a.i64(f(3), cc.NewerFirstSeen.UnixNano())
}

func (a *arrowWriter) quarantine(get cols) {
	q := a.d.Subgraph.Quarantine
	es := a.list(get(0))
	for _, e := range q.Entities {
		_, f := a.row(es.ValueBuilder(), true)
		a.fp(f(0), e.Fingerprint)
		a.u8(f(1), uint8(e.Cause.Layer))
		a.collision(f(2), e.Cause)
	}
	ed := a.list(get(1))
	for _, e := range q.Edges {
		_, f := a.row(ed.ValueBuilder(), true)
		_, g := a.row(f(0), true)
		a.element(g, e.Element)
		a.flag(f(1), e.Cause.Entity != e.Key.A)
		a.u8(f(2), uint8(e.Cause.Layer))
		a.collision(f(3), e.Cause)
	}
}

func (a *arrowWriter) truncation(get cols, t subgraph.Truncation) {
	a.fps(get(0), t.Nodes)
	a.fps(get(1), t.Hosts)
	a.fps(get(2), t.Members)
	a.fps(get(3), t.Instances)
	a.fps(get(4), t.SeedPods)
	a.fps(get(5), t.Depth)
	fr := a.list(get(6))
	for _, c := range t.Frontier {
		_, f := a.row(fr.ValueBuilder(), true)
		a.n(f(0), c.Depth)
		a.fps(f(1), c.Dropped)
	}
	pv := a.list(get(7))
	for _, v := range t.PerVia {
		_, f := a.row(pv.ValueBuilder(), true)
		a.fp(f(0), v.Via)
		a.u8(f(1), uint8(v.Mechanism))
		a.n(f(2), v.Dropped)
	}
	a.n(get(8), t.Results)
	a.n(get(9), t.Pairs)
	a.n(get(10), t.MembersDropped)
	a.n(get(11), t.SuppressedDropped)
}

func (a *arrowWriter) spans(b array.Builder, ss []subgraph.Span) {
	lb := a.list(b)
	for _, s := range ss {
		_, f := a.row(lb.ValueBuilder(), true)
		a.i64(f(0), int64(s.Start))
		a.i64(f(1), int64(s.End))
		a.flag(f(2), s.Before)
	}
}

func (a *arrowWriter) unattributed(get cols) {
	for _, u := range a.d.Subgraph.Sections.Unattributed {
		if u.SeedPod.IsZero() {
			isNull(get(0))
		} else {
			a.fp(get(0), u.SeedPod)
		}
		a.fp(get(1), u.Subject)
		a.u8(get(2), uint8(u.Reason))
		if u.Reason == subgraph.UnattributedSeveralClaims {
			rel, ok1 := relationID(u.Relation)
			ent, ok2 := entityTypeID(u.Target)
			if !ok1 || !ok2 {
				a.fail(fmt.Errorf("bytesgate: the claim %q on %q is not in the catalog: %w", u.Relation, u.Target, store.ErrInvalid))
			}
			a.u16(get(3), rel)
			a.u16(get(4), ent)
		} else {
			isNull(get(3))
			isNull(get(4))
		}
		a.spans(get(5), u.Spans)
		a.fps(get(6), u.Candidates)
	}
}

func (a *arrowWriter) unmapped(get cols) {
	for _, u := range a.d.Subgraph.Sections.Unmapped {
		a.fp(get(0), u.Pod)
		a.spans(get(1), u.Spans)
	}
}

// suppressedRows appends one row to each column of get for each entry.
func (a *arrowWriter) suppressedRows(get cols, ss []subgraph.Suppressed) {
	for _, x := range ss {
		a.suppressed(get, x)
	}
}

func (a *arrowWriter) suppressed(get cols, x subgraph.Suppressed) {
	a.fp(get(0), x.Key.A)
	a.fp(get(1), x.Key.B)
	dk, _ := subgraph.DerivedKindOf(x.Key.Rule)
	a.u8(get(2), uint8(dk))
	a.str(get(3), x.Key.Detail)
	a.u8(get(4), uint8(x.Reason))
	if x.Reason == subgraph.SuppressedUbiquitous {
		a.n(get(5), x.Coverage[0])
		a.n(get(6), x.Coverage[1])
	} else {
		isNull(get(5))
		isNull(get(6))
	}
	a.u8(get(7), uint8(x.Class))
	a.n(get(8), x.Transitions)
	a.spans(get(9), x.Spans)
}

func (a *arrowWriter) pods(get cols) {
	for _, p := range a.d.Subgraph.Sections.Pods {
		a.fp(get(0), p.Seed)
		_, t := a.row(get(1), true)
		a.truncation(t, p.Truncated)
		qs := a.list(get(2))
		for _, k := range p.Quarantined {
			_, f := a.row(qs.ValueBuilder(), true)
			a.u8(f(0), uint8(k.Kind))
			a.fp(f(1), k.A)
			if k.Kind == subgraph.KindEdge {
				a.fp(f(2), k.B)
				id, _ := relationID(k.Relation)
				a.u16(f(3), id)
			} else {
				isNull(f(2))
				isNull(f(3))
			}
		}
		ss := a.list(get(3))
		for _, x := range p.Suppressed {
			_, f := a.row(ss.ValueBuilder(), true)
			a.suppressed(f, x)
		}
	}
}

func (a *arrowWriter) anchor(get cols) {
	s := a.d.Subgraph.Sections
	a.u8(get(0), uint8(s.Anchor))
	a.flag(get(1), s.Ubiquity.Judged)
	a.n(get(2), s.Ubiquity.FrontierNodes)
}

// Streams reads the bytes of [Arrow] back as its streams, in order, with what
// each holds. It is how a test knows the bytes are Arrow IPC that a reader
// accepts, and how the report splits them into the bytes of the columns and the
// bytes of everything else.
func Streams(b []byte) ([]StreamInfo, error) {
	var out []StreamInfo
	for off := 0; off < len(b); {
		// The message reader finds where the stream ends, at its end-of-stream
		// marker, and how many bytes of it are the bodies of messages.
		r := bytes.NewReader(b[off:])
		mr := ipc.NewMessageReader(r, ipc.WithAllocator(memory.NewGoAllocator()))
		var body int64
		for {
			msg, err := mr.Message()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				mr.Release()
				return nil, err
			}
			body += msg.BodyLen()
			msg.Release()
		}
		mr.Release()
		size := len(b[off:]) - r.Len()
		if size == 0 {
			return nil, errors.New("bytesgate: a stream of no bytes")
		}
		sr, err := ipc.NewReader(bytes.NewReader(b[off:off+size]), ipc.WithAllocator(memory.NewGoAllocator()))
		if err != nil {
			return nil, err
		}
		info := StreamInfo{Fields: sr.Schema().NumFields(), Bytes: size, Meta: size - int(body)}
		for sr.Next() {
			info.Rows += int(sr.RecordBatch().NumRows())
			info.Batches++
		}
		err = sr.Err()
		sr.Release()
		if err != nil {
			return nil, err
		}
		out = append(out, info)
		off += size
	}
	if len(out) == 0 {
		return nil, errors.New("bytesgate: no stream")
	}
	return out, nil
}

// StreamInfo is what [Streams] reads of one stream: its fields, its rows, its
// record batches, its bytes, and of them the bytes that are not the bodies of its
// messages: the schema, the message headers, the padding of a header and the
// end-of-stream marker.
type StreamInfo struct {
	Fields, Rows, Batches int
	Bytes, Meta           int
}

// Metadata is the bytes of Arrow that are not the bodies of its messages, over
// all the streams.
func Metadata(b []byte) (int, error) {
	infos, err := Streams(b)
	if err != nil {
		return 0, err
	}
	var n int
	for _, s := range infos {
		n += s.Meta
	}
	return n, nil
}
