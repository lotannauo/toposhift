package wire_test

import (
	"encoding/binary"
	"reflect"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/subgraph"
	"github.com/lotannauo/toposhift/internal/wire"
)

// from is the From of the fixtures' windows.
var from = time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)

const (
	sec = uint64(time.Second)
	// pow53 is 2^53 nanoseconds, where a float stops holding every integer.
	pow53 = uint64(1) << 53
)

// mkfp is an entity of type typ whose hash starts with the two bytes of n.
func mkfp(typ catalog.EntityType, n int) identity.Fingerprint {
	var h [identity.FingerprintBytes]byte
	h[0], h[1] = byte(n>>8), byte(n)
	fp, err := identity.FingerprintFromHash(typ, h)
	if err != nil {
		panic(err)
	}
	return fp
}

func pod(n int) identity.Fingerprint  { return mkfp(catalog.K8sPod, n) }
func node(n int) identity.Fingerprint { return mkfp(catalog.K8sNode, n) }
func host(n int) identity.Fingerprint { return mkfp(catalog.Host, n) }
func svc(n int) identity.Fingerprint  { return mkfp(catalog.Service, n) }
func inst(n int) identity.Fingerprint { return mkfp(catalog.ServiceInstance, n) }

func ent(fp identity.Fingerprint) subgraph.Key { return subgraph.EntityKey(fp) }

func sched(p, n identity.Fingerprint) subgraph.Key {
	return subgraph.EdgeKey(p, n, catalog.ScheduledOn)
}

func instOf(i, s identity.Fingerprint) subgraph.Key {
	return subgraph.EdgeKey(i, s, catalog.InstanceOf)
}

func derivedKey(rule string, a, b identity.Fingerprint, detail string) subgraph.Key {
	return subgraph.Key{Kind: subgraph.KindDerived, A: a, B: b, Rule: rule, Detail: detail}
}

// pc is one piece in nanoseconds after From; an end of 0 is the window's To.
type pc struct {
	s, e   uint64
	reason subgraph.EndReason
	before bool
	prod   []lifecycle.Producer
	ends   []subgraph.LegEnd
}

// source makes the source of a key over a window of the given width, with the
// class and the transitions computed here, from the specification.
func source(width uint64, key subgraph.Key, pcs ...pc) subgraph.Source {
	s := subgraph.Source{Key: key}
	for _, p := range pcs {
		end, reason := p.e, p.reason
		if end == 0 || end == width {
			end, reason = width, subgraph.EndReasonUnknown
		}
		s.Pieces = append(s.Pieces, subgraph.SourcePiece{
			Start: from.Add(time.Duration(p.s)), End: from.Add(time.Duration(end)), Before: p.before, EndReason: reason,
			Producers: p.prod, Ends: p.ends,
		})
	}
	s.Class, s.Transitions = classOf(width, spansOf(pcs, width))
	return s
}

func spansOf(pcs []pc, width uint64) [][2]uint64 {
	var out [][2]uint64
	for _, p := range pcs {
		e := p.e
		if e == 0 {
			e = width
		}
		out = append(out, [2]uint64{p.s, e})
	}
	return out
}

// classOf is the class table and the transition count, written out from the
// specification.
func classOf(width uint64, ss [][2]uint64) (subgraph.Class, int) {
	n := 0
	for _, s := range ss {
		if s[0] > 0 {
			n++
		}
		if s[1] < width {
			n++
		}
	}
	switch {
	case len(ss) == 0:
		return subgraph.ClassNone, 0
	case len(ss) > 1:
		return subgraph.ClassFlapping, n
	}
	a, b := ss[0][0] == 0, ss[0][1] == width
	switch {
	case a && b:
		return subgraph.ClassStable, n
	case b:
		return subgraph.ClassAppeared, n
	case a:
		return subgraph.ClassDisappeared, n
	}
	return subgraph.ClassTransient, n
}

// span is a section span; an end of 0 is the window's To.
func span(width, s, e uint64, before bool) subgraph.Span {
	r := subgraph.EndReasonReachEnded
	if e == 0 || e == width {
		e, r = width, subgraph.EndReasonUnknown
	}
	return subgraph.Span{Start: s, End: e, Before: before, EndReason: r}
}

func suppressed(width uint64, key subgraph.Key, reason subgraph.SuppressReason, cov [2]int, ss ...subgraph.Span) subgraph.Suppressed {
	var raw [][2]uint64
	for _, s := range ss {
		raw = append(raw, [2]uint64{s.Start, s.End})
	}
	class, transitions := classOf(width, raw)
	return subgraph.Suppressed{Key: key, Reason: reason, Coverage: cov, Class: class, Transitions: transitions, Spans: ss}
}

// fixturePolicy is the policy vector of the fixtures: written out, not read from
// the packages, so a version bump elsewhere does not change a golden file.
func fixturePolicy() wire.Policy {
	p := wire.Policy{Interval: 1, CoResidency: 1, Extract: 1, BlastRadius: 1, GraphDiff: 1, IdentityEncoding: 1}
	for i := range p.CatalogIDs {
		p.CatalogIDs[i] = byte(i)
	}
	return p
}

func mustBuild(tb testing.TB, in subgraph.Input) subgraph.Subgraph {
	tb.Helper()
	sg, err := subgraph.Build(in)
	if err != nil {
		tb.Fatalf("Build: %v", err)
	}
	return sg
}

func mustEncode(tb testing.TB, d wire.Document) []byte {
	tb.Helper()
	b, err := wire.Encode(d)
	if err != nil {
		tb.Fatalf("Encode: %v", err)
	}
	return b
}

// equal reports whether two documents are the same value. A nil slice and an
// empty one are the same: the model does not tell them apart, and the decoder
// leaves a list nil where the builder may leave it empty.
func equal(a, b any) bool { return eq(reflect.ValueOf(a), reflect.ValueOf(b)) }

func eq(a, b reflect.Value) bool {
	if a.Type() != b.Type() {
		return false
	}
	switch a.Kind() {
	case reflect.Slice:
		if a.Len() != b.Len() {
			return false
		}
		for i := range a.Len() {
			if !eq(a.Index(i), b.Index(i)) {
				return false
			}
		}
		return true
	case reflect.Pointer:
		if a.IsNil() || b.IsNil() {
			return a.IsNil() == b.IsNil()
		}
		return eq(a.Elem(), b.Elem())
	case reflect.Struct:
		// A struct with unexported fields (a fingerprint, a time) is a value that
		// compares with ==.
		for i := range a.NumField() {
			if !a.Type().Field(i).IsExported() {
				return a.Interface() == b.Interface()
			}
		}
		for i := range a.NumField() {
			if !eq(a.Field(i), b.Field(i)) {
				return false
			}
		}
		return true
	case reflect.Array:
		if a.Type().Comparable() {
			return a.Interface() == b.Interface()
		}
		for i := range a.Len() {
			if !eq(a.Index(i), b.Index(i)) {
				return false
			}
		}
		return true
	}
	return a.Interface() == b.Interface()
}

// rawSection is one section of a document's bytes.
type rawSection struct {
	tag     byte
	payload []byte
}

// header is the magic and the two version bytes.
const headerLen = 6

// sections splits a document into its sections, as the layout says.
func sections(tb testing.TB, b []byte) []rawSection {
	tb.Helper()
	var out []rawSection
	for rest := b[headerLen:]; len(rest) > 0; {
		tag := rest[0]
		n, k := binary.Uvarint(rest[1:])
		if k <= 0 || uint64(len(rest)-1-k) < n {
			tb.Fatalf("a section is cut short at %d bytes from the end", len(rest))
		}
		out = append(out, rawSection{tag, rest[1+k : 1+k+int(n)]})
		rest = rest[1+k+int(n):]
	}
	return out
}

// assemble is the inverse of sections, with the given version bytes.
func assemble(major, minor byte, ss ...rawSection) []byte {
	out := []byte{'T', 'S', 'G', 'F', major, minor}
	for _, s := range ss {
		out = append(out, s.tag)
		out = binary.AppendUvarint(out, uint64(len(s.payload)))
		out = append(out, s.payload...)
	}
	return out
}

func withSection(ss []rawSection, tag byte, payload []byte) []rawSection {
	out := append([]rawSection(nil), ss...)
	for i := range out {
		if out[i].tag == tag {
			out[i].payload = payload
		}
	}
	return out
}
