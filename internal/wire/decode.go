package wire

import (
	"fmt"
	"math"
	"time"
	"unicode/utf8"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/subgraph"
	"github.com/lotannauo/toposhift/internal/store"
)

// Decode reads a document. It accepts exactly the canonical encodings: bytes
// that [Encode] could have written, plus sections of a later minor version it
// skips. Every count is checked against the bytes that remain before anything
// is allocated for it, so the memory a decode takes is a small multiple of the
// input's size.
//
// A document of another major version is refused with [ErrVersion], the
// reserved rebase section and the reserved attr operation with [ErrReserved],
// and any other fault, including a non-canonical value, with [ErrFormat].
func Decode(b []byte) (Document, error) {
	d, _, err := decode(b)
	return d, err
}

// decodeInfo says what a decode did not read back.
type decodeInfo struct {
	// skipped is the number of optional sections of a later minor version that
	// were skipped, and minor the document's minor version. When both are what
	// Encode writes, Encode(Decode(b)) is b.
	skipped int
	minor   byte
}

func decode(b []byte) (Document, decodeInfo, error) {
	var info decodeInfo
	if len(b) < len(magic)+2 || [4]byte(b[:4]) != magic {
		return Document{}, info, fmt.Errorf("%w: no magic", ErrFormat)
	}
	if uint64(len(b)) > math.MaxUint32 {
		return Document{}, info, fmt.Errorf("%w: %d bytes", ErrFormat, len(b))
	}
	if b[4] != FormatMajor {
		return Document{}, info, fmt.Errorf("%w: major version %d, this decoder reads %d", ErrVersion, b[4], FormatMajor)
	}
	info.minor = b[5]
	dc := &dec{}
	r := &reader{b: b[6:]}
	next := byte(tagHeader)
	prev := -1
	for r.remaining() > 0 {
		tag := r.u8()
		n := r.uv()
		if r.err == nil && n > uint64(r.remaining()) {
			r.fail("the section %#x is %d bytes and %d remain", tag, n, r.remaining())
		}
		if r.err != nil {
			return Document{}, info, r.err
		}
		payload := r.take(int(n))
		if int(tag) <= prev {
			return Document{}, info, fmt.Errorf("%w: the section %#x is not after the section before it", ErrFormat, tag)
		}
		prev = int(tag)
		switch {
		case tag == tagRebase:
			return Document{}, info, fmt.Errorf("%w: the rebase section %#x", ErrReserved, tag)
		case tag >= optionalBit:
			info.skipped++
			continue
		case tag != next || tag > lastRequiredTag:
			return Document{}, info, fmt.Errorf("%w: the section %#x where %#x is required", ErrFormat, tag, next)
		}
		sub := &reader{b: payload}
		dc.section(tag, sub)
		if sub.err == nil && sub.remaining() > 0 {
			sub.fail("%d bytes after the end of the section %#x", sub.remaining(), tag)
		}
		if sub.err != nil {
			return Document{}, info, sub.err
		}
		next++
	}
	if next != lastRequiredTag+1 {
		return Document{}, info, fmt.Errorf("%w: the required section %#x is missing", ErrFormat, next)
	}
	if err := validate(&dc.doc); err != nil {
		return Document{}, info, fmt.Errorf("%w: %w", ErrFormat, err)
	}
	return dc.doc, info, nil
}

// reader reads from b. The first fault is kept and every later read returns zero,
// so a section's parser checks once at its end.
type reader struct {
	b   []byte
	off int
	err error
}

func (r *reader) remaining() int { return len(r.b) - r.off }

func (r *reader) fail(format string, args ...any) {
	if r.err == nil {
		r.err = fmt.Errorf("%w: "+format, append([]any{ErrFormat}, args...)...)
	}
}

func (r *reader) take(n int) []byte {
	if r.err != nil {
		return nil
	}
	if n < 0 || n > r.remaining() {
		r.fail("%d bytes wanted and %d remain", n, r.remaining())
		return nil
	}
	out := r.b[r.off : r.off+n]
	r.off += n
	return out
}

func (r *reader) u8() byte {
	if p := r.take(1); p != nil {
		return p[0]
	}
	return 0
}

func (r *reader) u64() uint64 {
	if p := r.take(8); p != nil {
		return uint64(p[0])<<56 | uint64(p[1])<<48 | uint64(p[2])<<40 | uint64(p[3])<<32 |
			uint64(p[4])<<24 | uint64(p[5])<<16 | uint64(p[6])<<8 | uint64(p[7])
	}
	return 0
}

// uv reads a uvarint in its shortest form.
func (r *reader) uv() uint64 {
	if r.err != nil {
		return 0
	}
	var x uint64
	for i := range 10 {
		if r.off >= len(r.b) {
			r.fail("a varint is cut short")
			return 0
		}
		c := r.b[r.off]
		r.off++
		if c < 0x80 {
			switch {
			case i == 9 && c > 1:
				r.fail("a varint does not fit 64 bits")
				return 0
			case i > 0 && c == 0:
				r.fail("a varint is not the shortest form")
				return 0
			}
			return x | uint64(c)<<(7*i)
		}
		x |= uint64(c&0x7f) << (7 * i)
	}
	r.fail("a varint is longer than 10 bytes")
	return 0
}

// i64 reads a zigzag varint.
func (r *reader) i64() int64 {
	u := r.uv()
	return int64(u>>1) ^ -int64(u&1)
}

// n reads a number that is a Go int.
func (r *reader) n() int {
	u := r.uv()
	if u > math.MaxInt {
		r.fail("the number %d does not fit an int", u)
		return 0
	}
	return int(u)
}

// count reads a count of items that each take at least min more bytes, and
// refuses one the remaining bytes cannot hold, before anyone allocates for it.
func (r *reader) count(min int) int {
	u := r.uv()
	if r.err != nil {
		return 0
	}
	if u > uint64(r.remaining()/min) {
		r.fail("a count of %d does not fit the %d bytes that remain", u, r.remaining())
		return 0
	}
	return int(u)
}

func (r *reader) str() string {
	n := r.count(1)
	p := r.take(n)
	if !utf8.Valid(p) {
		r.fail("a string is not UTF-8")
		return ""
	}
	return string(p)
}

// col reads n numbers, each at least one byte.
func (r *reader) col(n int) []uint64 {
	if r.err != nil {
		return nil
	}
	if n > r.remaining() {
		r.fail("a column of %d numbers does not fit the %d bytes that remain", n, r.remaining())
		return nil
	}
	out := make([]uint64, n)
	for i := range out {
		out[i] = r.uv()
	}
	return out
}

// bytesCol reads n bytes.
func (r *reader) bytesCol(n int) []byte { return r.take(n) }

// bits reads n flags, which a canonical encoding pads with zero bits.
func (r *reader) bits(n int) []bool {
	p := r.take((n + 7) / 8)
	if r.err != nil {
		return nil
	}
	out := make([]bool, n)
	for i := range out {
		out[i] = p[i/8]&(1<<(i%8)) != 0
	}
	if n%8 != 0 && p[len(p)-1]>>(n%8) != 0 {
		r.fail("the padding bits of a flag column are not zero")
	}
	return out
}

// strictIDs reads n strictly ascending ids below limit, as strictIDs writes
// them.
func (r *reader) strictIDs(n int, limit uint64) []uint32 {
	return r.idList(n, limit, 1)
}

// ascIDs reads n non-decreasing ids below limit.
func (r *reader) ascIDs(n int, limit uint64) []uint32 {
	return r.idList(n, limit, 0)
}

func (r *reader) idList(n int, limit, step uint64) []uint32 {
	if r.err != nil {
		return nil
	}
	if n > r.remaining() {
		r.fail("a list of %d ids does not fit the %d bytes that remain", n, r.remaining())
		return nil
	}
	ids := make([]uint32, 0, n)
	var prev uint64
	for i := 0; i < n && r.err == nil; i++ {
		v := r.uv()
		id := v
		if i > 0 {
			if v >= limit {
				r.fail("an id list steps by %d, past the %d ids there are", v, limit)
				break
			}
			id = prev + step + v
		}
		if id >= limit {
			r.fail("the id %d is not one of %d", id, limit)
			break
		}
		ids = append(ids, uint32(id))
		prev = id
	}
	return ids
}

// id reads a reference below limit.
func (r *reader) id(limit int) uint32 {
	v := r.uv()
	if r.err == nil && v >= uint64(limit) {
		r.fail("the id %d is not one of %d", v, limit)
		return 0
	}
	return uint32(v)
}

func (r *reader) time64() time.Time {
	u := r.u64()
	if u > math.MaxInt64 {
		r.fail("the time %d does not fit an int64", u)
		return time.Time{}
	}
	return time.Unix(0, int64(u)).UTC()
}

// dec holds what the sections read so far, which later sections refer to.
type dec struct {
	doc   Document
	width uint64
}

func (dc *dec) section(tag byte, r *reader) {
	switch tag {
	case tagHeader:
		dc.header(r)
	case tagDictionary:
		dc.dictionary(r)
	case tagElements:
		dc.doc.Subgraph.Elements = dc.elementBlock(r, true)
	case tagKeyframes:
		dc.keyframes(r)
	case tagDeltas:
		dc.deltas(r)
	case tagQuarantine:
		dc.quarantine(r)
	case tagTruncated:
		dc.doc.Subgraph.Sections.Truncated = dc.truncation(r)
	case tagUnattributed:
		dc.unattributed(r)
	case tagUnmapped:
		dc.unmapped(r)
	case tagSuppressed:
		dc.doc.Subgraph.Sections.Suppressed = dc.suppressedList(r)
	case tagPods:
		dc.pods(r)
	case tagAnchor:
		dc.anchor(r)
	}
}

// fingerprint reads an inline fingerprint: the entity id and the hash.
func (dc *dec) fingerprint(r *reader) identity.Fingerprint {
	id := r.uv()
	h := r.take(identity.FingerprintBytes)
	if r.err != nil {
		return identity.Fingerprint{}
	}
	ent, ok := catalog.Default().EntityByID(catalog.EntityID(id))
	if !ok || id > math.MaxUint16 {
		r.fail("the entity id %d is not in the catalog", id)
		return identity.Fingerprint{}
	}
	fp, err := identity.FingerprintFromHash(ent.Type(), [identity.FingerprintBytes]byte(h))
	if err != nil {
		r.fail("%v", err)
	}
	return fp
}

func (dc *dec) header(r *reader) {
	sg := &dc.doc.Subgraph
	sg.Basis = subgraph.Basis(r.u8())
	sg.Token = r.u64()
	sg.From, sg.To = r.time64(), r.time64()
	sg.PolicyVersion = r.n()
	p := &dc.doc.Policy
	p.Interval, p.CoResidency, p.Extract = r.n(), r.n(), r.n()
	p.BlastRadius, p.GraphDiff, p.IdentityEncoding = r.n(), r.n(), r.n()
	copy(p.CatalogIDs[:], r.take(len(p.CatalogIDs)))
	switch n := r.n(); n {
	case 0:
	case len(p.CatalogIDs):
		p.Rank = append([]byte(nil), r.take(n)...)
	default:
		r.fail("a rank digest of %d bytes", n)
	}
	q := &dc.doc.Query
	q.Kind = QueryKind(r.u8())
	for range r.count(identity.FingerprintBytes + 1) {
		q.Seeds = append(q.Seeds, dc.fingerprint(r))
	}
	for range r.count(2) {
		q.Options = append(q.Options, Option{Name: r.str(), Value: r.i64()})
	}
	q.AsOf = r.u64()
	if r.err == nil && sg.From.Before(sg.To) {
		dc.width = uint64(sg.To.UnixNano() - sg.From.UnixNano())
	} else {
		r.fail("the window is empty")
	}
}

func (dc *dec) dictionary(r *reader) {
	n := r.count(identity.FingerprintBytes)
	type run struct {
		typ catalog.EntityType
		n   int
	}
	var runs []run
	total := 0
	for range r.count(2) {
		id := r.uv()
		c := r.uv()
		if r.err != nil {
			return
		}
		ent, ok := catalog.Default().EntityByID(catalog.EntityID(id))
		switch {
		case !ok || id > math.MaxUint16:
			r.fail("the entity id %d is not in the catalog", id)
		case c == 0 || c > uint64(n-total):
			r.fail("a run of %d entities with %d of %d left", c, n-total, n)
		case len(runs) > 0 && runs[len(runs)-1].typ == ent.Type():
			r.fail("two runs of the type %s in a row", ent.Type())
		}
		if r.err != nil {
			return
		}
		runs = append(runs, run{ent.Type(), int(c)})
		total += int(c)
	}
	if r.err == nil && total != n {
		r.fail("the runs cover %d of %d entities", total, n)
	}
	hashes := r.take(n * identity.FingerprintBytes)
	boundary := r.bits(n)
	if r.err != nil {
		return
	}
	es := make([]subgraph.Entity, 0, n)
	for _, ru := range runs {
		for range ru.n {
			i := len(es)
			fp, err := identity.FingerprintFromHash(ru.typ, [identity.FingerprintBytes]byte(hashes[i*identity.FingerprintBytes:]))
			if err != nil {
				r.fail("%v", err)
				return
			}
			es = append(es, subgraph.Entity{Fingerprint: fp, Boundary: boundary[i]})
		}
	}
	dc.doc.Subgraph.Entities = es
}

// ruleOf is the rule a key determines, for the keys that have one the
// validation will accept; validate refuses the rest.
func ruleOf(k subgraph.Key) string {
	switch k.Kind {
	case subgraph.KindEdge:
		if rel, ok := catalog.Default().Relation(k.Relation); ok && rel.Storage() == catalog.StorageDerived {
			return string(k.Relation)
		}
	case subgraph.KindDerived:
		return k.Rule
	}
	return subgraph.RuleExistence
}

// elementBlock reads what elementBlock writes. main says the legs may refer to
// the elements of the document; otherwise the block is the static
// edges of the quarantine section: edges only, with no legs.
func (dc *dec) elementBlock(r *reader, main bool) []subgraph.Element {
	nDict := len(dc.doc.Subgraph.Entities)
	nEnt, nEdge, nDer := r.count(1), r.count(1), r.count(1)
	if r.err != nil {
		return nil
	}
	if !main && (nEnt != 0 || nDer != 0) {
		r.fail("a static element that is not an edge")
		return nil
	}
	total := nEnt + nEdge + nDer
	if total > r.remaining()/4 {
		r.fail("%d elements do not fit the %d bytes that remain", total, r.remaining())
		return nil
	}

	var table []lifecycle.Producer
	for range r.count(1) {
		p := lifecycle.Producer(r.str())
		if len(table) > 0 && table[len(table)-1] >= p {
			r.fail("the producer table is not ascending")
			return nil
		}
		table = append(table, p)
	}

	els := make([]subgraph.Element, total)
	entities, edges, derived := els[:nEnt], els[nEnt:nEnt+nEdge], els[nEnt+nEdge:]
	fp := func(id uint32) identity.Fingerprint { return dc.doc.Subgraph.Entities[id].Fingerprint }
	for i, id := range r.strictIDs(nEnt, uint64(nDict)) {
		entities[i].Key = subgraph.EntityKey(fp(id))
	}
	aEdges := r.ascIDs(nEdge, uint64(nDict))
	bEdges := r.col(nEdge)
	rels := r.col(nEdge)
	aDer := r.ascIDs(nDer, uint64(nDict))
	bDer := r.col(nDer)
	kinds := r.bytesCol(nDer)
	details := make([]string, 0, nDer)
	for range nDer {
		details = append(details, r.str())
	}
	if r.err != nil {
		return nil
	}
	for i := range edges {
		if bEdges[i] >= uint64(nDict) || rels[i] > math.MaxUint16 {
			r.fail("an edge refers to an entity or a relation that is not there")
			return nil
		}
		rel, ok := catalog.Default().RelationByID(catalog.RelationID(rels[i]))
		if !ok {
			r.fail("the relation id %d is not in the catalog", rels[i])
			return nil
		}
		edges[i].Key = subgraph.EdgeKey(fp(aEdges[i]), fp(uint32(bEdges[i])), rel.Type())
	}
	for i := range derived {
		dk := subgraph.DerivedKind(kinds[i])
		if !dk.Valid() || dk == subgraph.DerivedSameAs || bDer[i] >= uint64(nDict) {
			r.fail("a derived key with the kind %d or an end that is not there", kinds[i])
			return nil
		}
		derived[i].Key = subgraph.Key{
			Kind: subgraph.KindDerived, A: fp(aDer[i]), B: fp(uint32(bDer[i])), Rule: dk.Rule(), Detail: details[i],
		}
	}

	// The legs of the derived subjects.
	pathCounts := r.col(nDer)
	var nPaths uint64
	for _, c := range pathCounts {
		nPaths += c
		if nPaths > uint64(r.remaining()) || c > uint64(r.remaining()) {
			r.fail("%d paths do not fit the %d bytes that remain", nPaths, r.remaining())
			return nil
		}
	}
	legCounts := r.col(int(nPaths))
	var nLegs uint64
	for _, c := range legCounts {
		nLegs += c
		if nLegs > uint64(r.remaining()) || c > uint64(r.remaining()) {
			r.fail("%d legs do not fit the %d bytes that remain", nLegs, r.remaining())
			return nil
		}
	}
	legs := r.col(int(nLegs))
	if r.err != nil {
		return nil
	}
	pi, li := 0, 0
	for i := range derived {
		for range pathCounts[i] {
			path := make([]uint32, legCounts[pi])
			for j := range path {
				if legs[li] >= uint64(total) || !main {
					r.fail("a leg that is not an element")
					return nil
				}
				path[j] = uint32(legs[li])
				li++
			}
			pi++
			derived[i].Paths = append(derived[i].Paths, path)
		}
	}

	classes := r.bytesCol(total)
	transitions := r.col(total)
	subject := r.bits(total)
	pieceCounts := r.col(total)
	if r.err != nil {
		return nil
	}
	var nPieces uint64
	for _, c := range pieceCounts {
		nPieces += c
		if c > uint64(r.remaining()/5) || nPieces > uint64(r.remaining()/5) {
			r.fail("%d pieces do not fit the %d bytes that remain", nPieces, r.remaining())
			return nil
		}
	}
	starts := r.col(int(nPieces))
	lens := r.col(int(nPieces))
	before := r.bits(int(nPieces))
	reasons := r.bytesCol(int(nPieces))
	prodCounts := r.col(int(nPieces))
	var nProds uint64
	for _, c := range prodCounts {
		nProds += c
		if c > uint64(r.remaining()) || nProds > uint64(r.remaining()) {
			r.fail("%d producers do not fit the %d bytes that remain", nProds, r.remaining())
			return nil
		}
	}
	prodIdx := make([]uint32, 0, nProds)
	used := make([]bool, len(table))
	for _, c := range prodCounts {
		ids := r.strictIDs(int(c), uint64(len(table)))
		for _, id := range ids {
			used[id] = true
		}
		prodIdx = append(prodIdx, ids...)
	}
	endCounts := r.col(int(nPieces))
	var nEnds uint64
	for _, c := range endCounts {
		nEnds += c
		if c > uint64(r.remaining()/3) || nEnds > uint64(r.remaining()/3) {
			r.fail("%d leg ends do not fit the %d bytes that remain", nEnds, r.remaining())
			return nil
		}
	}
	endPath := r.col(int(nEnds))
	endLeg := r.col(int(nEnds))
	endReason := r.bytesCol(int(nEnds))
	if r.err != nil {
		return nil
	}
	for _, u := range used {
		if !u {
			r.fail("a producer in the table is used by no piece")
			return nil
		}
	}

	pc, ec, pri := 0, 0, 0
	for i := range els {
		el := &els[i]
		el.Class = subgraph.Class(classes[i])
		if transitions[i] > math.MaxInt {
			r.fail("%d transitions do not fit an int", transitions[i])
			return nil
		}
		el.Transitions = int(transitions[i])
		el.Subject = subgraph.SubjectAsserted
		if subject[i] {
			el.Subject = subgraph.SubjectDerived
		}
		el.Rule = ruleOf(el.Key)
		if pieceCounts[i] > 0 {
			el.Pieces = make([]subgraph.Piece, pieceCounts[i])
		}
		var prevEnd uint64
		for j := range el.Pieces {
			s, l := starts[pc], lens[pc]
			if j > 0 {
				if s >= dc.width {
					r.fail("a piece starts %d after the piece before it, in a window of %d", s, dc.width)
					return nil
				}
				s += prevEnd + 1
			}
			if s > dc.width || l >= dc.width || s+l+1 > dc.width {
				r.fail("a piece [%d, +%d] reaches past the window of %d", s, l+1, dc.width)
				return nil
			}
			p := &el.Pieces[j]
			p.Start, p.End = s, s+l+1
			prevEnd = p.End
			p.Before = before[pc]
			p.EndReason = subgraph.EndReason(reasons[pc])
			for k := 0; k < int(prodCounts[pc]); k++ {
				p.Producers = append(p.Producers, table[prodIdx[pri]])
				pri++
			}
			for k := 0; k < int(endCounts[pc]); k++ {
				if endPath[ec] > math.MaxInt || endLeg[ec] > math.MaxInt {
					r.fail("a leg end that does not fit an int")
					return nil
				}
				p.Ends = append(p.Ends, subgraph.LegEnd{Path: int(endPath[ec]), Leg: int(endLeg[ec]), Reason: subgraph.EndReason(endReason[ec])})
				ec++
			}
			pc++
		}
	}
	return els
}

func (dc *dec) keyframes(r *reader) {
	nEl := len(dc.doc.Subgraph.Elements)
	k := r.count(1)
	if r.err == nil && k == 0 {
		r.fail("no keyframe")
		return
	}
	gaps := r.col(k - 1)
	counts := r.col(k)
	if r.err != nil {
		return
	}
	var total uint64
	for _, c := range counts {
		total += c
		if c > uint64(r.remaining()) || total > uint64(r.remaining()) {
			r.fail("%d keyframe entries do not fit the %d bytes that remain", total, r.remaining())
			return
		}
	}
	kfs := make([]subgraph.Keyframe, k)
	for i := range kfs {
		if i > 0 {
			if gaps[i-1] >= dc.width || kfs[i-1].At+1+gaps[i-1] >= dc.width {
				r.fail("a keyframe at or past the end of the window")
				return
			}
			kfs[i].At = kfs[i-1].At + 1 + gaps[i-1]
		}
		if ids := r.strictIDs(int(counts[i]), uint64(nEl)); len(ids) > 0 {
			kfs[i].Elements = ids
		}
	}
	dc.doc.Subgraph.Keyframes = kfs
}

func (dc *dec) deltas(r *reader) {
	nEl := len(dc.doc.Subgraph.Elements)
	n := r.count(4)
	dts := r.col(n)
	els := r.col(n)
	ops := r.bytesCol(n)
	reasons := r.bytesCol(n)
	if r.err != nil {
		return
	}
	var out []subgraph.Delta
	if n > 0 {
		out = make([]subgraph.Delta, n)
	}
	var at uint64
	for i := range out {
		if dts[i] >= dc.width || at+dts[i] >= dc.width {
			r.fail("a delta at or past the end of the window")
			return
		}
		at += dts[i]
		if els[i] >= uint64(nEl) {
			r.fail("a delta of the element %d of %d", els[i], nEl)
			return
		}
		d := subgraph.Delta{At: at, Element: uint32(els[i])}
		reason := subgraph.EndReason(reasons[i])
		switch ops[i] {
		case opAppear:
			d.Before = subgraph.State{Reason: reason}
			d.After = subgraph.State{Present: true}
		case opDisappear:
			d.Before = subgraph.State{Present: true}
			d.After = subgraph.State{Reason: reason}
		case opAttr:
			r.err = fmt.Errorf("%w: the attr operation of delta %d", ErrReserved, i)
			return
		default:
			r.fail("the operation %d of delta %d", ops[i], i)
			return
		}
		out[i] = d
	}
	dc.doc.Subgraph.Deltas = out
}

// collisions reads what collisions writes into the causes, which are set up with
// their entity and layer.
func (dc *dec) collisions(r *reader, causes []*store.QuarantineError) {
	has := r.bits(len(causes))
	for i, c := range causes {
		if r.err != nil {
			return
		}
		if has[i] {
			c.Collision = &lifecycle.CloneCollisionError{
				StaleBoot: r.str(), ObservedAt: r.time64(), NewerBoot: r.str(), NewerFirstSeen: r.time64(),
			}
		}
	}
}

func (dc *dec) quarantine(r *reader) {
	sg := &dc.doc.Subgraph
	nDict := len(sg.Entities)
	n := r.count(3)
	ids := r.strictIDs(n, uint64(nDict))
	layers := r.bytesCol(n)
	if r.err != nil {
		return
	}
	causes := make([]*store.QuarantineError, n)
	for i := range causes {
		fp := sg.Entities[ids[i]].Fingerprint
		causes[i] = &store.QuarantineError{Entity: fp, Layer: catalog.Layer(layers[i])}
	}
	dc.collisions(r, causes)
	for i, c := range causes {
		sg.Quarantine.Entities = append(sg.Quarantine.Entities, subgraph.QuarantinedEntity{Fingerprint: c.Entity, Cause: causes[i]})
	}

	els := dc.elementBlock(r, false)
	side := r.bits(len(els))
	layers = r.bytesCol(len(els))
	if r.err != nil {
		return
	}
	causes = make([]*store.QuarantineError, len(els))
	for i, el := range els {
		entity := el.Key.A
		if side[i] {
			entity = el.Key.B
			if entity == el.Key.A {
				r.fail("a static edge names its second end for a cause that is also its first")
				return
			}
		}
		causes[i] = &store.QuarantineError{Entity: entity, Layer: catalog.Layer(layers[i])}
	}
	dc.collisions(r, causes)
	for i, el := range els {
		sg.Quarantine.Edges = append(sg.Quarantine.Edges, subgraph.StaticEdge{Element: el, Cause: causes[i]})
	}
}

func (dc *dec) fingerprints(r *reader) []identity.Fingerprint {
	nDict := len(dc.doc.Subgraph.Entities)
	var out []identity.Fingerprint
	for range r.count(1) {
		if id := r.id(nDict); r.err == nil {
			out = append(out, dc.doc.Subgraph.Entities[id].Fingerprint)
		}
	}
	return out
}

func (dc *dec) truncation(r *reader) subgraph.Truncation {
	var t subgraph.Truncation
	t.Nodes = dc.fingerprints(r)
	t.Hosts = dc.fingerprints(r)
	t.Members = dc.fingerprints(r)
	t.Instances = dc.fingerprints(r)
	t.SeedPods = dc.fingerprints(r)
	t.Depth = dc.fingerprints(r)
	for range r.count(2) {
		depth := r.n()
		t.Frontier = append(t.Frontier, subgraph.FrontierCut{Depth: depth, Dropped: dc.fingerprints(r)})
	}
	nDict := len(dc.doc.Subgraph.Entities)
	for range r.count(3) {
		id := r.id(nDict)
		m := subgraph.Mechanism(r.u8())
		dropped := r.n()
		if r.err == nil {
			t.PerVia = append(t.PerVia, subgraph.ViaCount{Via: dc.doc.Subgraph.Entities[id].Fingerprint, Mechanism: m, Dropped: dropped})
		}
	}
	t.Results, t.Pairs, t.MembersDropped, t.SuppressedDropped = r.n(), r.n(), r.n(), r.n()
	return t
}

func (dc *dec) spans(r *reader) []subgraph.Span {
	var out []subgraph.Span
	var prevEnd uint64
	for i := range r.count(3) {
		s, l := r.uv(), r.uv()
		before := r.u8()
		if r.err != nil {
			return nil
		}
		if i > 0 {
			if s >= dc.width {
				r.fail("a span starts %d after the span before it, in a window of %d", s, dc.width)
				return nil
			}
			s += prevEnd + 1
		}
		if s > dc.width || l >= dc.width || s+l+1 > dc.width || before > 1 {
			r.fail("a span [%d, +%d] with the flag %d does not fit the window of %d", s, l+1, before, dc.width)
			return nil
		}
		sp := subgraph.Span{Start: s, End: s + l + 1, Before: before == 1, EndReason: subgraph.EndReasonReachEnded}
		if sp.End == dc.width {
			sp.EndReason = subgraph.EndReasonUnknown
		}
		prevEnd = sp.End
		out = append(out, sp)
	}
	return out
}

func (dc *dec) unattributed(r *reader) {
	nDict := len(dc.doc.Subgraph.Entities)
	es := dc.doc.Subgraph.Entities
	var out []subgraph.Unattributed
	for range r.count(8) {
		var u subgraph.Unattributed
		if v := r.uv(); v > 0 {
			if v-1 >= uint64(nDict) {
				r.fail("the seed pod %d is not one of %d", v-1, nDict)
				return
			}
			u.SeedPod = es[v-1].Fingerprint
		}
		subject := r.id(nDict)
		if r.err != nil {
			return
		}
		u.Subject = es[subject].Fingerprint
		u.Reason = subgraph.UnattributedReason(r.u8())
		if u.Reason == subgraph.UnattributedSeveralClaims {
			rel, okR := catalog.Default().RelationByID(catalog.RelationID(min(r.uv(), math.MaxUint16+1)))
			ent, okE := catalog.Default().EntityByID(catalog.EntityID(min(r.uv(), math.MaxUint16+1)))
			if r.err == nil && (!okR || !okE) {
				r.fail("a claim through a relation or to a type that is not in the catalog")
				return
			}
			u.Relation, u.Target = rel.Type(), ent.Type()
		}
		u.Spans = dc.spans(r)
		u.Candidates = dc.fingerprints(r)
		if r.err != nil {
			return
		}
		out = append(out, u)
	}
	dc.doc.Subgraph.Sections.Unattributed = out
}

func (dc *dec) unmapped(r *reader) {
	nDict := len(dc.doc.Subgraph.Entities)
	var out []subgraph.Unmapped
	for range r.count(4) {
		id := r.id(nDict)
		sp := dc.spans(r)
		if r.err != nil {
			return
		}
		out = append(out, subgraph.Unmapped{Pod: dc.doc.Subgraph.Entities[id].Fingerprint, Spans: sp})
	}
	dc.doc.Subgraph.Sections.Unmapped = out
}

func (dc *dec) suppressedList(r *reader) []subgraph.Suppressed {
	nDict := len(dc.doc.Subgraph.Entities)
	var out []subgraph.Suppressed
	for range r.count(10) {
		a, b := r.id(nDict), r.id(nDict)
		dk := subgraph.DerivedKind(r.u8())
		detail := r.str()
		if r.err != nil {
			return nil
		}
		if !dk.Valid() || dk == subgraph.DerivedSameAs {
			r.fail("a suppressed entry with the derived kind %d", uint8(dk))
			return nil
		}
		x := subgraph.Suppressed{Key: subgraph.Key{
			Kind: subgraph.KindDerived, A: dc.doc.Subgraph.Entities[a].Fingerprint, B: dc.doc.Subgraph.Entities[b].Fingerprint,
			Rule: dk.Rule(), Detail: detail,
		}}
		x.Reason = subgraph.SuppressReason(r.u8())
		if x.Reason == subgraph.SuppressedUbiquitous {
			x.Coverage = [2]int{r.n(), r.n()}
		}
		x.Class = subgraph.Class(r.u8())
		x.Transitions = r.n()
		x.Spans = dc.spans(r)
		if r.err != nil {
			return nil
		}
		out = append(out, x)
	}
	return out
}

func (dc *dec) pods(r *reader) {
	nDict := len(dc.doc.Subgraph.Entities)
	var out []subgraph.PodSection
	for range r.count(15) {
		id := r.id(nDict)
		if r.err != nil {
			return
		}
		p := subgraph.PodSection{Seed: dc.doc.Subgraph.Entities[id].Fingerprint}
		p.Truncated = dc.truncation(r)
		for range r.count(2) {
			p.Quarantined = append(p.Quarantined, dc.plainKey(r))
		}
		p.Suppressed = dc.suppressedList(r)
		if r.err != nil {
			return
		}
		out = append(out, p)
	}
	dc.doc.Subgraph.Sections.Pods = out
}

func (dc *dec) plainKey(r *reader) subgraph.Key {
	nDict := len(dc.doc.Subgraph.Entities)
	kind := subgraph.Kind(r.u8())
	a := r.id(nDict)
	if r.err != nil {
		return subgraph.Key{}
	}
	es := dc.doc.Subgraph.Entities
	switch kind {
	case subgraph.KindEntity:
		return subgraph.EntityKey(es[a].Fingerprint)
	case subgraph.KindEdge:
		b := r.id(nDict)
		rel := r.uv()
		if r.err != nil {
			return subgraph.Key{}
		}
		rr, ok := catalog.Default().RelationByID(catalog.RelationID(min(rel, math.MaxUint16+1)))
		if !ok {
			r.fail("the relation id %d is not in the catalog", rel)
			return subgraph.Key{}
		}
		return subgraph.EdgeKey(es[a].Fingerprint, es[b].Fingerprint, rr.Type())
	}
	r.fail("the key kind %d", uint8(kind))
	return subgraph.Key{}
}

func (dc *dec) anchor(r *reader) {
	s := &dc.doc.Subgraph.Sections
	s.Anchor = subgraph.AnchorState(r.u8())
	switch r.u8() {
	case 0:
	case 1:
		s.Ubiquity.Judged = true
	default:
		r.fail("a ubiquity flag that is not 0 or 1")
	}
	s.Ubiquity.FrontierNodes = r.n()
}
