package identity_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
)

// normalized is an identity in canonical Go types: string, int64, or UTC
// time.Time per key. Absent optional keys are not present.
type normalized map[catalog.AttributeKey]any

// sample is one generated identity: its type, the same attributes written in
// a random mix of accepted spellings, and the normalized form.
type sample struct {
	typ   catalog.EntityType
	attrs []identity.Attr
	norm  normalized
}

// signature renders a sample's meaning, independent of spelling, for
// equality checks that do not go through the code under test.
func (s sample) signature() string {
	keys := make([]string, 0, len(s.norm))
	for k := range s.norm {
		keys = append(keys, string(k))
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(string(s.typ))
	for _, k := range keys {
		switch v := s.norm[catalog.AttributeKey(k)].(type) {
		case string:
			fmt.Fprintf(&b, "|%s=s%q", k, v)
		case int64:
			fmt.Fprintf(&b, "|%s=i%d", k, v)
		case time.Time:
			fmt.Fprintf(&b, "|%s=t%d.%09d", k, v.Unix(), v.Nanosecond())
		}
	}
	return b.String()
}

// stringChars are the characters that would break a delimiter-based encoding.
var stringChars = []rune{0, 0x1e, 0x1f, ':', '/', '.', 'a', 'b', 'é', ' ', '\n', 0x7f, '"'}

func genString() *rapid.Generator[string] {
	usable := func(s string) bool {
		return strings.TrimSpace(s) != "" && len(s) <= identity.MaxValueLen && utf8.ValidString(s)
	}
	return rapid.OneOf(
		rapid.String().Filter(usable),
		rapid.StringOf(rapid.SampledFrom(stringChars)).Filter(usable),
		rapid.SampledFrom([]string{"a", "host", "\x00", "\x1f", "host.id", "s:x", "toposhift/identity", "0", "1", "-1"}),
	)
}

func genInstant() *rapid.Generator[time.Time] {
	// Year 1 (just after the zero time, which means "unset") to year 9999.
	return rapid.Custom(func(t *rapid.T) time.Time {
		sec := rapid.Int64Range(-62135596799, 253402300799).Draw(t, "seconds")
		ns := rapid.IntRange(0, 999_999_999).Draw(t, "nanoseconds")
		return time.Unix(sec, int64(ns)).UTC()
	})
}

var zones = []*time.Location{
	time.UTC,
	time.FixedZone("A", 3600),
	time.FixedZone("B", -5*3600-1800),
	time.FixedZone("C", 14*3600),
}

// genSample draws an entity type from the default catalog and a value for
// each of its keys, writing each value in a random accepted spelling.
func genSample() *rapid.Generator[sample] {
	var types []catalog.EntityType
	for e := range catalog.Default().Entities() {
		types = append(types, e.Type())
	}
	return rapid.Custom(func(t *rapid.T) sample {
		typ := rapid.SampledFrom(types).Draw(t, "type")
		ent, _ := catalog.Default().Entity(typ)
		s := sample{typ: typ, norm: normalized{}}
		for k := range ent.Keys() {
			if k.Optional {
				switch rapid.IntRange(0, 2).Draw(t, "optional") {
				case 0:
					continue // absent
				case 1:
					s.attrs = append(s.attrs, identity.Attr{Key: k.Name, Value: ""})
					continue // present but empty: also absent
				}
			}
			var written any
			switch k.Kind {
			case catalog.KindString:
				v := genString().Draw(t, string(k.Name))
				s.norm[k.Name], written = v, v
			case catalog.KindInt:
				v := rapid.Int64().Draw(t, string(k.Name))
				s.norm[k.Name] = v
				switch rapid.IntRange(0, 2).Draw(t, "int spelling") {
				case 0:
					written = v
				case 1:
					written = strconv.FormatInt(v, 10)
				default:
					if v >= 0 {
						written = uint64(v)
					} else {
						written = v
					}
				}
			case catalog.KindTime:
				v := genInstant().Draw(t, string(k.Name))
				s.norm[k.Name] = v
				zone := rapid.SampledFrom(zones).Draw(t, "zone")
				// RFC 3339 has four-digit years, and a zone offset can push an
				// instant in year 9999 into year 10000, so such instants are
				// only ever written as a time.Time.
				if y := v.In(zone).Year(); y >= 1 && y <= 9999 && rapid.Bool().Draw(t, "time as string") {
					written = v.In(zone).Format(time.RFC3339Nano)
				} else {
					written = v.In(zone)
				}
			}
			s.attrs = append(s.attrs, identity.Attr{Key: k.Name, Value: written})
		}
		return s
	})
}

// reference is a deliberately naive encoder written from the specification in
// the package documentation, sharing no code with the real one. It is the
// oracle the optimized encoder is checked against.
func reference(typ catalog.EntityType, norm normalized) []byte {
	var buf bytes.Buffer
	put := func(v any) {
		if err := binary.Write(&buf, binary.BigEndian, v); err != nil {
			panic(err)
		}
	}
	putString := func(s string) {
		put(uint32(len(s)))
		buf.WriteString(s)
	}

	buf.WriteString("toposhift/identity")
	buf.WriteByte(1)
	putString(string(typ))
	put(uint32(len(norm)))

	names := make([]string, 0, len(norm))
	for k := range norm {
		names = append(names, string(k))
	}
	sort.Strings(names)
	for _, name := range names {
		putString(name)
		switch v := norm[catalog.AttributeKey(name)].(type) {
		case string:
			buf.WriteByte(1)
			putString(v)
		case int64:
			buf.WriteByte(2)
			put(v)
		case time.Time:
			buf.WriteByte(3)
			put(v.Unix())
			put(uint32(v.Nanosecond()))
		}
	}
	return buf.Bytes()
}

func TestPropertyMatchesReferenceEncoder(t *testing.T) {
	t.Parallel()
	r := newResolver()
	rapid.Check(t, func(t *rapid.T) {
		s := genSample().Draw(t, "sample")
		id, err := r.Resolve(s.typ, s.attrs)
		if err != nil {
			t.Fatalf("Resolve(%v): %v", s.attrs, err)
		}
		want := reference(s.typ, s.norm)
		if id.Canonical() != string(want) {
			t.Fatalf("canonical = %x\nreference = %x", id.Canonical(), want)
		}
		sum := sha256.Sum256(want)
		if got, wantFP := id.Fingerprint().String(), string(s.typ)+":"+fmt.Sprintf("%x", sum[:identity.FingerprintBytes]); got != wantFP {
			t.Fatalf("fingerprint = %s, want %s", got, wantFP)
		}
	})
}

func TestPropertyAttributeOrderIsIrrelevant(t *testing.T) {
	t.Parallel()
	r := newResolver()
	rapid.Check(t, func(t *rapid.T) {
		s := genSample().Draw(t, "sample")
		shuffled := rapid.Permutation(s.attrs).Draw(t, "shuffled")
		a, errA := r.Resolve(s.typ, s.attrs)
		b, errB := r.Resolve(s.typ, shuffled)
		if errA != nil || errB != nil {
			t.Fatal(errA, errB)
		}
		if a != b {
			t.Fatalf("order changed the identity: %s vs %s", a, b)
		}
	})
}

func TestPropertySpellingIsIrrelevant(t *testing.T) {
	t.Parallel()
	r := newResolver()
	rapid.Check(t, func(t *rapid.T) {
		s := genSample().Draw(t, "sample")
		a, err := r.Resolve(s.typ, s.attrs)
		if err != nil {
			t.Fatal(err)
		}
		// The normalized spelling: native types, no empty optional keys.
		var plain []identity.Attr
		for k, v := range s.norm {
			plain = append(plain, identity.Attr{Key: k, Value: v})
		}
		b, err := r.Resolve(s.typ, plain)
		if err != nil {
			t.Fatal(err)
		}
		if a != b {
			t.Fatalf("spelling changed the identity: %s vs %s", a, b)
		}
	})
}

func TestPropertyRoundTrip(t *testing.T) {
	t.Parallel()
	r := newResolver()
	rapid.Check(t, func(t *rapid.T) {
		s := genSample().Draw(t, "sample")
		id, err := r.Resolve(s.typ, s.attrs)
		if err != nil {
			t.Fatal(err)
		}

		parsed, err := r.Parse(id.Canonical())
		if err != nil || parsed != id {
			t.Fatalf("Parse(Canonical()) = %v, %v; want %s", parsed, err, id)
		}
		again, err := r.Resolve(id.Type(), id.Attrs())
		if err != nil || again != id {
			t.Fatalf("Resolve(Attrs()) = %v, %v; want %s", again, err, id)
		}
		fp, err := identity.ParseFingerprint(id.Fingerprint().String())
		if err != nil || fp != id.Fingerprint() {
			t.Fatalf("ParseFingerprint(String()) = %v, %v", fp, err)
		}
		fromHash, err := identity.FingerprintFromHash(id.Type(), id.Fingerprint().Hash())
		if err != nil || fromHash != id.Fingerprint() {
			t.Fatalf("FingerprintFromHash(Type(), Hash()) = %v, %v; want %s", fromHash, err, id.Fingerprint())
		}
		if got := (sample{typ: s.typ, norm: attrsToNorm(id.Attrs())}).signature(); got != s.signature() {
			t.Fatalf("Attrs() changed the meaning:\n got %s\nwant %s", got, s.signature())
		}
	})
}

func attrsToNorm(attrs []identity.Attr) normalized {
	n := normalized{}
	for _, a := range attrs {
		n[a.Key] = a.Value
	}
	return n
}

// TestPropertyDistinctIdentitiesHaveDistinctBytes is injectivity: two
// identities with different meanings never share canonical bytes, and
// therefore, short of a hash collision, never share a fingerprint. The second
// sample is often a one-value edit of the first, so near misses are tested,
// not only unrelated pairs.
func TestPropertyDistinctIdentitiesHaveDistinctBytes(t *testing.T) {
	t.Parallel()
	r := newResolver()
	rapid.Check(t, func(t *rapid.T) {
		a := genSample().Draw(t, "a")
		b := genSample().Draw(t, "b")
		if rapid.Bool().Draw(t, "edit a instead") {
			b = editOne(t, a)
		}

		ia, errA := r.Resolve(a.typ, a.attrs)
		ib, errB := r.Resolve(b.typ, b.attrs)
		if errA != nil || errB != nil {
			t.Fatal(errA, errB)
		}
		sameMeaning := a.signature() == b.signature()
		if sameMeaning != (ia.Canonical() == ib.Canonical()) {
			t.Fatalf("meaning equal = %v but canonical equal = %v\n a: %s\n b: %s",
				sameMeaning, ia.Canonical() == ib.Canonical(), a.signature(), b.signature())
		}
		if !sameMeaning && ia.Fingerprint() == ib.Fingerprint() {
			t.Fatalf("different identities share a fingerprint: %s", ia)
		}
	})
}

// editOne returns a copy of s with one present string value replaced, or s
// itself if it has none. It rewrites the attributes in normalized form.
func editOne(t *rapid.T, s sample) sample {
	out := sample{typ: s.typ, norm: normalized{}}
	for k, v := range s.norm {
		out.norm[k] = v
	}
	var strKeys []catalog.AttributeKey
	for k, v := range out.norm {
		if _, ok := v.(string); ok {
			strKeys = append(strKeys, k)
		}
	}
	slices.Sort(strKeys)
	if len(strKeys) > 0 {
		k := rapid.SampledFrom(strKeys).Draw(t, "key to edit")
		out.norm[k] = genString().Draw(t, "edited value")
	}
	for k, v := range out.norm {
		out.attrs = append(out.attrs, identity.Attr{Key: k, Value: v})
	}
	return out
}

// toiseStylePreimage is the delimiter-based scheme the plan rejected: record
// and field separators with textual type tags, no length prefixes. It exists
// only to show the regression test below is meaningful.
func toiseStylePreimage(typ string, kv [][2]string) string {
	var b strings.Builder
	b.WriteString(typ + "\x1f")
	for _, p := range kv {
		b.WriteString(p[0] + "\x1es:" + p[1] + "\x1f")
	}
	return b.String()
}

// TestSeparatorInjectionCannotForgeAnIdentity builds the attack that a
// delimiter-based encoding falls to: a one-attribute identity whose value
// embeds the separators and a second attribute, to look like a genuine
// two-attribute identity.
func TestSeparatorInjectionCannotForgeAnIdentity(t *testing.T) {
	t.Parallel()

	genuine := [][2]string{{"service.name", "web"}, {"service.namespace", "shop"}}
	forged := [][2]string{{"service.name", "web\x1fservice.namespace\x1es:shop"}}
	if toiseStylePreimage("service", genuine) != toiseStylePreimage("service", forged) {
		t.Fatal("the delimiter-based scheme no longer collides, so this test proves nothing")
	}

	r := newResolver()
	real, err := r.Resolve(catalog.Service, attrs(catalog.ServiceName, "web", catalog.ServiceNamespace, "shop"))
	if err != nil {
		t.Fatal(err)
	}
	fake, err := r.Resolve(catalog.Service, attrs(catalog.ServiceName, "web\x1fservice.namespace\x1es:shop"))
	if err != nil {
		t.Fatal(err)
	}
	if real.Canonical() == fake.Canonical() || real.Fingerprint() == fake.Fingerprint() {
		t.Error("an injected value forged another identity")
	}
}
