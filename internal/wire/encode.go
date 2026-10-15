package wire

import (
	"encoding/binary"
	"fmt"
	"slices"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/subgraph"
	"github.com/lotannauo/toposhift/internal/store"
)

// Encode writes the document's one canonical encoding, uncompressed. It refuses
// a document that [Decode] would refuse, with an error wrapping store.ErrInvalid:
// Decode(Encode(d)) is d, and Encode(Decode(b)) is b for every b a decoder
// accepts that has no section it skipped.
func Encode(d Document) ([]byte, error) {
	if err := validate(&d); err != nil {
		return nil, err
	}
	out := &enc{d: &d}
	out.b = append(out.b, magic[:]...)
	out.b = append(out.b, FormatMajor, FormatMinor)
	out.section(tagHeader, (*enc).header)
	out.section(tagDictionary, (*enc).dictionary)
	out.section(tagElements, func(e *enc) { e.elementBlock(d.Subgraph.Elements) })
	out.section(tagKeyframes, (*enc).keyframes)
	out.section(tagDeltas, (*enc).deltas)
	out.section(tagQuarantine, (*enc).quarantine)
	out.section(tagTruncated, func(e *enc) { e.truncation(d.Subgraph.Sections.Truncated) })
	out.section(tagUnattributed, (*enc).unattributed)
	out.section(tagUnmapped, (*enc).unmapped)
	out.section(tagSuppressed, func(e *enc) { e.suppressedList(d.Subgraph.Sections.Suppressed) })
	out.section(tagPods, (*enc).pods)
	out.section(tagAnchor, (*enc).anchor)
	if out.err != nil {
		return nil, out.err
	}
	return out.b, nil
}

// enc appends to b. An error is kept, the first one, and the writers go on
// writing zeros: validate has checked what the writers look up, so an error here
// is a bug, and is returned rather than a panic.
type enc struct {
	b   []byte
	d   *Document
	err error
}

func (e *enc) fail(format string, args ...any) {
	if e.err == nil {
		e.err = fmt.Errorf("wire: "+format+": %w", append(args, store.ErrInvalid)...)
	}
}

func (e *enc) section(tag byte, fn func(*enc)) {
	sec := &enc{d: e.d}
	fn(sec)
	if sec.err != nil && e.err == nil {
		e.err = sec.err
	}
	e.b = append(e.b, tag)
	e.b = binary.AppendUvarint(e.b, uint64(len(sec.b)))
	e.b = append(e.b, sec.b...)
}

func (e *enc) uv(x uint64)  { e.b = binary.AppendUvarint(e.b, x) }
func (e *enc) n(x int)      { e.uv(uint64(x)) }
func (e *enc) u8(x byte)    { e.b = append(e.b, x) }
func (e *enc) u64(x uint64) { e.b = binary.BigEndian.AppendUint64(e.b, x) }

// i64 is a signed value as a zigzag uvarint.
func (e *enc) i64(x int64) { e.uv(uint64(x<<1) ^ uint64(x>>63)) }

func (e *enc) str(s string) {
	e.n(len(s))
	e.b = append(e.b, s...)
}

// bits writes one bit for each value, the first in the lowest bit of the first
// byte, and the padding bits zero.
func (e *enc) bits(vs []bool) {
	out := make([]byte, (len(vs)+7)/8)
	for i, v := range vs {
		if v {
			out[i/8] |= 1 << (i % 8)
		}
	}
	e.b = append(e.b, out...)
}

// id is the dictionary id of fp.
func (e *enc) id(fp identity.Fingerprint) uint32 {
	i, ok := e.d.Subgraph.EntityID(fp)
	if !ok {
		e.fail("the entity %s is not in the dictionary", fp)
	}
	return i
}

func entityID(fp identity.Fingerprint) (uint64, bool) {
	ent, ok := catalog.Default().Entity(fp.Type())
	return uint64(ent.ID()), ok
}

func (e *enc) fingerprint(fp identity.Fingerprint) {
	id, ok := entityID(fp)
	if !ok {
		e.fail("the entity type %q is not in the catalog", fp.Type())
	}
	e.uv(id)
	h := fp.Hash()
	e.b = append(e.b, h[:]...)
}

// strictIDs writes a strictly ascending list of ids: the first as it is, then
// each as its distance from the one before, less one.
func (e *enc) strictIDs(ids []uint32) {
	for i, id := range ids {
		if i == 0 {
			e.uv(uint64(id))
		} else {
			e.uv(uint64(id - ids[i-1] - 1))
		}
	}
}

// ascIDs writes a non-decreasing list of ids: the first as it is, then each as
// its distance from the one before.
func (e *enc) ascIDs(ids []uint32) {
	for i, id := range ids {
		if i == 0 {
			e.uv(uint64(id))
		} else {
			e.uv(uint64(id - ids[i-1]))
		}
	}
}

func (e *enc) header() {
	d := e.d
	sg := &d.Subgraph
	e.u8(byte(sg.Basis))
	e.u64(sg.Token)
	e.u64(uint64(sg.From.UnixNano()))
	e.u64(uint64(sg.To.UnixNano()))
	p := d.Policy
	for _, v := range []int{sg.PolicyVersion, p.Interval, p.CoResidency, p.Extract, p.BlastRadius, p.GraphDiff, p.IdentityEncoding} {
		e.n(v)
	}
	e.b = append(e.b, p.CatalogIDs[:]...)
	e.n(len(p.Rank))
	e.b = append(e.b, p.Rank...)
	q := d.Query
	e.u8(byte(q.Kind))
	e.n(len(q.Seeds))
	for _, fp := range q.Seeds {
		e.fingerprint(fp)
	}
	e.n(len(q.Options))
	for _, o := range q.Options {
		e.str(o.Name)
		e.i64(o.Value)
	}
	e.u64(q.AsOf)
}

// dictionary is the entities in three columns: the types as runs, the 16 bytes of
// each hash, and the boundary flags.
func (e *enc) dictionary() {
	es := e.d.Subgraph.Entities
	e.n(len(es))
	type run struct {
		id uint64
		n  int
	}
	var runs []run
	for _, ent := range es {
		id, ok := entityID(ent.Fingerprint)
		if !ok {
			e.fail("the entity type %q is not in the catalog", ent.Fingerprint.Type())
		}
		if n := len(runs); n > 0 && runs[n-1].id == id {
			runs[n-1].n++
		} else {
			runs = append(runs, run{id, 1})
		}
	}
	e.n(len(runs))
	for _, r := range runs {
		e.uv(r.id)
		e.n(r.n)
	}
	boundary := make([]bool, len(es))
	for i, ent := range es {
		h := ent.Fingerprint.Hash()
		e.b = append(e.b, h[:]...)
		boundary[i] = ent.Boundary
	}
	e.bits(boundary)
}

// elementBlock writes elements in columns. The elements are sorted, so those of
// one kind are together, and the block gives the three counts and no per-element
// kind. Pieces, their flags, producers and leg ends are columns over all the
// pieces of all the elements.
func (e *enc) elementBlock(els []subgraph.Element) {
	var nEnt, nEdge, nDer int
	for _, el := range els {
		switch el.Key.Kind {
		case subgraph.KindEntity:
			nEnt++
		case subgraph.KindEdge:
			nEdge++
		case subgraph.KindDerived:
			nDer++
		}
	}
	e.n(nEnt)
	e.n(nEdge)
	e.n(nDer)

	// The producer table: every producer of every piece, once, ascending.
	var table []lifecycle.Producer
	for _, el := range els {
		for _, p := range el.Pieces {
			table = append(table, p.Producers...)
		}
	}
	slices.Sort(table)
	table = slices.Compact(table)
	e.n(len(table))
	for _, p := range table {
		e.str(string(p))
	}

	entities, edges, derived := els[:nEnt], els[nEnt:nEnt+nEdge], els[nEnt+nEdge:]
	aIDs := func(es []subgraph.Element) []uint32 {
		ids := make([]uint32, len(es))
		for i, el := range es {
			ids[i] = e.id(el.Key.A)
		}
		return ids
	}
	bIDs := func(es []subgraph.Element) {
		for _, el := range es {
			e.uv(uint64(e.id(el.Key.B)))
		}
	}
	e.strictIDs(aIDs(entities))
	e.ascIDs(aIDs(edges))
	bIDs(edges)
	for _, el := range edges {
		rel, ok := catalog.Default().Relation(el.Key.Relation)
		if !ok {
			e.fail("the relation %q is not registered", el.Key.Relation)
		}
		e.uv(uint64(rel.ID()))
	}
	e.ascIDs(aIDs(derived))
	bIDs(derived)
	for _, el := range derived {
		dk, _ := subgraph.DerivedKindOf(el.Key.Rule)
		e.u8(byte(dk))
	}
	for _, el := range derived {
		e.str(el.Key.Detail)
	}
	for _, el := range derived {
		e.n(len(el.Paths))
	}
	for _, el := range derived {
		for _, p := range el.Paths {
			e.n(len(p))
		}
	}
	for _, el := range derived {
		for _, p := range el.Paths {
			for _, leg := range p {
				e.uv(uint64(leg))
			}
		}
	}

	for _, el := range els {
		e.u8(byte(el.Class))
	}
	for _, el := range els {
		e.n(el.Transitions)
	}
	subject := make([]bool, len(els))
	for i, el := range els {
		subject[i] = el.Subject == subgraph.SubjectDerived
	}
	e.bits(subject)

	for _, el := range els {
		e.n(len(el.Pieces))
	}
	// Starts: the first piece's as it is, then each piece's distance from the end
	// of the piece before it, less one, so pieces that touch cannot be written.
	for _, el := range els {
		for i, p := range el.Pieces {
			if i == 0 {
				e.uv(p.Start)
			} else {
				e.uv(p.Start - el.Pieces[i-1].End - 1)
			}
		}
	}
	// Lengths, less one, so an empty piece cannot be written.
	for _, el := range els {
		for _, p := range el.Pieces {
			e.uv(p.End - p.Start - 1)
		}
	}
	var before []bool
	for _, el := range els {
		for _, p := range el.Pieces {
			before = append(before, p.Before)
		}
	}
	e.bits(before)
	for _, el := range els {
		for _, p := range el.Pieces {
			e.u8(byte(p.EndReason))
		}
	}
	for _, el := range els {
		for _, p := range el.Pieces {
			e.n(len(p.Producers))
		}
	}
	for _, el := range els {
		for _, p := range el.Pieces {
			prev := -1
			for _, pr := range p.Producers {
				i, _ := slices.BinarySearch(table, pr)
				if prev < 0 {
					e.uv(uint64(i))
				} else {
					e.uv(uint64(i - prev - 1))
				}
				prev = i
			}
		}
	}
	for _, el := range els {
		for _, p := range el.Pieces {
			e.n(len(p.Ends))
		}
	}
	for _, col := range []func(subgraph.LegEnd){
		func(l subgraph.LegEnd) { e.n(l.Path) },
		func(l subgraph.LegEnd) { e.n(l.Leg) },
		func(l subgraph.LegEnd) { e.u8(byte(l.Reason)) },
	} {
		for _, el := range els {
			for _, p := range el.Pieces {
				for _, l := range p.Ends {
					col(l)
				}
			}
		}
	}
}

// keyframes: the count, the times of the keyframes after the first (which is at
// From) as distances less one, the element counts, and the ids of all the
// keyframes in one column.
func (e *enc) keyframes() {
	kfs := e.d.Subgraph.Keyframes
	e.n(len(kfs))
	for i := 1; i < len(kfs); i++ {
		e.uv(kfs[i].At - kfs[i-1].At - 1)
	}
	for _, k := range kfs {
		e.n(len(k.Elements))
	}
	for _, k := range kfs {
		e.strictIDs(k.Elements)
	}
}

// deltas: the count, then columns: the time of each as its distance from the
// one before (the first from From), the element, the operation, and the reason
// of the state that is absent.
func (e *enc) deltas() {
	ds := e.d.Subgraph.Deltas
	e.n(len(ds))
	var prev uint64
	for _, d := range ds {
		e.uv(d.At - prev)
		prev = d.At
	}
	for _, d := range ds {
		e.uv(uint64(d.Element))
	}
	for _, d := range ds {
		if d.Appear() {
			e.u8(opAppear)
		} else {
			e.u8(opDisappear)
		}
	}
	for _, d := range ds {
		if d.Appear() {
			e.u8(byte(d.Before.Reason))
		} else {
			e.u8(byte(d.After.Reason))
		}
	}
}

func (e *enc) collisions(cs []*store.QuarantineError) {
	has := make([]bool, len(cs))
	for i, c := range cs {
		has[i] = c.Collision != nil
	}
	e.bits(has)
	for _, c := range cs {
		if cc := c.Collision; cc != nil {
			e.str(cc.StaleBoot)
			e.u64(uint64(cc.ObservedAt.UnixNano()))
			e.str(cc.NewerBoot)
			e.u64(uint64(cc.NewerFirstSeen.UnixNano()))
		}
	}
}

func (e *enc) quarantine() {
	q := e.d.Subgraph.Quarantine
	e.n(len(q.Entities))
	ids := make([]uint32, len(q.Entities))
	causes := make([]*store.QuarantineError, len(q.Entities))
	for i, qe := range q.Entities {
		ids[i] = e.id(qe.Fingerprint)
		causes[i] = qe.Cause
	}
	e.strictIDs(ids)
	for _, c := range causes {
		e.u8(byte(c.Layer))
	}
	e.collisions(causes)

	els := make([]subgraph.Element, len(q.Edges))
	causes = make([]*store.QuarantineError, len(q.Edges))
	side := make([]bool, len(q.Edges))
	for i, se := range q.Edges {
		els[i], causes[i] = se.Element, se.Cause
		side[i] = se.Cause.Entity != se.Key.A
	}
	e.elementBlock(els)
	e.bits(side)
	for _, c := range causes {
		e.u8(byte(c.Layer))
	}
	e.collisions(causes)
}

func (e *enc) fingerprints(fps []identity.Fingerprint) {
	e.n(len(fps))
	for _, fp := range fps {
		e.uv(uint64(e.id(fp)))
	}
}

func (e *enc) truncation(t subgraph.Truncation) {
	e.fingerprints(t.Nodes)
	e.fingerprints(t.Hosts)
	e.fingerprints(t.Members)
	e.fingerprints(t.Instances)
	e.fingerprints(t.SeedPods)
	e.fingerprints(t.Depth)
	e.n(len(t.Frontier))
	for _, f := range t.Frontier {
		e.n(f.Depth)
		e.fingerprints(f.Dropped)
	}
	e.n(len(t.PerVia))
	for _, v := range t.PerVia {
		e.uv(uint64(e.id(v.Via)))
		e.u8(byte(v.Mechanism))
		e.n(v.Dropped)
	}
	e.n(t.Results)
	e.n(t.Pairs)
	e.n(t.MembersDropped)
	e.n(t.SuppressedDropped)
}

// spans: the count, then for each the distance from the end of the one before
// (the first from From, the others less one), the length less one, and the
// before flag. The end reason is not written: it is unknown at To and
// reach_ended before.
func (e *enc) spans(ss []subgraph.Span) {
	e.n(len(ss))
	for i, s := range ss {
		if i == 0 {
			e.uv(s.Start)
		} else {
			e.uv(s.Start - ss[i-1].End - 1)
		}
		e.uv(s.End - s.Start - 1)
		if s.Before {
			e.u8(1)
		} else {
			e.u8(0)
		}
	}
}

func (e *enc) unattributed() {
	us := e.d.Subgraph.Sections.Unattributed
	e.n(len(us))
	for _, u := range us {
		if u.SeedPod.IsZero() {
			e.uv(0)
		} else {
			e.uv(uint64(e.id(u.SeedPod)) + 1)
		}
		e.uv(uint64(e.id(u.Subject)))
		e.u8(byte(u.Reason))
		if u.Reason == subgraph.UnattributedSeveralClaims {
			rel, _ := catalog.Default().Relation(u.Relation)
			ent, _ := catalog.Default().Entity(u.Target)
			e.uv(uint64(rel.ID()))
			e.uv(uint64(ent.ID()))
		}
		e.spans(u.Spans)
		e.fingerprints(u.Candidates)
	}
}

func (e *enc) unmapped() {
	us := e.d.Subgraph.Sections.Unmapped
	e.n(len(us))
	for _, u := range us {
		e.uv(uint64(e.id(u.Pod)))
		e.spans(u.Spans)
	}
}

func (e *enc) suppressedList(ss []subgraph.Suppressed) {
	e.n(len(ss))
	for _, x := range ss {
		e.uv(uint64(e.id(x.Key.A)))
		e.uv(uint64(e.id(x.Key.B)))
		dk, _ := subgraph.DerivedKindOf(x.Key.Rule)
		e.u8(byte(dk))
		e.str(x.Key.Detail)
		e.u8(byte(x.Reason))
		if x.Reason == subgraph.SuppressedUbiquitous {
			e.n(x.Coverage[0])
			e.n(x.Coverage[1])
		}
		e.u8(byte(x.Class))
		e.n(x.Transitions)
		e.spans(x.Spans)
	}
}

// plainKey writes the key of an entity or an edge.
func (e *enc) plainKey(k subgraph.Key) {
	e.u8(byte(k.Kind))
	e.uv(uint64(e.id(k.A)))
	if k.Kind == subgraph.KindEdge {
		e.uv(uint64(e.id(k.B)))
		rel, _ := catalog.Default().Relation(k.Relation)
		e.uv(uint64(rel.ID()))
	}
}

func (e *enc) pods() {
	ps := e.d.Subgraph.Sections.Pods
	e.n(len(ps))
	for _, p := range ps {
		e.uv(uint64(e.id(p.Seed)))
		e.truncation(p.Truncated)
		e.n(len(p.Quarantined))
		for _, k := range p.Quarantined {
			e.plainKey(k)
		}
		e.suppressedList(p.Suppressed)
	}
}

func (e *enc) anchor() {
	s := e.d.Subgraph.Sections
	e.u8(byte(s.Anchor))
	if s.Ubiquity.Judged {
		e.u8(1)
	} else {
		e.u8(0)
	}
	e.n(s.Ubiquity.FrontierNodes)
}
