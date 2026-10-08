package identity_test

import (
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
)

func newResolver(opts ...identity.Option) *identity.Resolver {
	return identity.NewResolver(catalog.Default(), opts...)
}

func attrs(kv ...any) []identity.Attr {
	out := make([]identity.Attr, 0, len(kv)/2)
	for i := 0; i < len(kv); i += 2 {
		var key catalog.AttributeKey
		switch k := kv[i].(type) {
		case catalog.AttributeKey:
			key = k
		case string:
			key = catalog.AttributeKey(k)
		default:
			panic("attrs: key must be a string or catalog.AttributeKey")
		}
		out = append(out, identity.Attr{Key: key, Value: kv[i+1]})
	}
	return out
}

const (
	goodTime = "2023-11-21T09:25:34.853Z"
	goodHost = "i-0abc"
)

func TestResolveRejects(t *testing.T) {
	t.Parallel()

	proc := func(pid, created any) []identity.Attr {
		return attrs(catalog.HostID, goodHost, catalog.ProcessPID, pid, catalog.ProcessCreationTime, created)
	}

	tests := []struct {
		name   string
		typ    catalog.EntityType
		attrs  []identity.Attr
		want   error
		offend catalog.AttributeKey // the key the error must name; empty for type-level errors
	}{
		// Entity type.
		{"unknown entity type", "ghost", attrs(catalog.HostID, "x"), identity.ErrUnknownType, ""},

		// Missing, empty and whitespace-only. The kwok case from the design:
		// a fake node template with an empty machine ID must be rejected, not
		// merged with every other empty one.
		{"missing required", catalog.Host, nil, identity.ErrMissing, catalog.HostID},
		{"empty required", catalog.Host, attrs(catalog.HostID, ""), identity.ErrEmpty, catalog.HostID},
		{"whitespace-only required", catalog.Host, attrs(catalog.HostID, " \t\n"), identity.ErrEmpty, catalog.HostID},
		{"unicode whitespace-only required", catalog.Host, attrs(catalog.HostID, "  "), identity.ErrEmpty, catalog.HostID},
		{
			"whitespace-only optional", catalog.Service,
			attrs(catalog.ServiceNamespace, "  ", catalog.ServiceName, "web"), identity.ErrEmpty, catalog.ServiceNamespace,
		},
		{"empty int key", catalog.Process, proc("", goodTime), identity.ErrEmpty, catalog.ProcessPID},

		// Attribute set.
		{"unregistered key", catalog.Host, attrs(catalog.HostID, goodHost, "host.name", "web-1"), identity.ErrUnregistered, "host.name"},
		{"duplicate key", catalog.Host, attrs(catalog.HostID, "a", catalog.HostID, "b"), identity.ErrDuplicateKey, catalog.HostID},
		{"duplicate key, same value", catalog.Host, attrs(catalog.HostID, "a", catalog.HostID, "a"), identity.ErrDuplicateKey, catalog.HostID},

		// Wrong Go types for a string key.
		{"int for string key", catalog.Host, attrs(catalog.HostID, 5), identity.ErrType, catalog.HostID},
		{"bool", catalog.Host, attrs(catalog.HostID, true), identity.ErrType, catalog.HostID},
		{"time for string key", catalog.Host, attrs(catalog.HostID, time.Now()), identity.ErrType, catalog.HostID},
		{"float64", catalog.Host, attrs(catalog.HostID, 1.5), identity.ErrFloat, catalog.HostID},
		{"float32", catalog.Host, attrs(catalog.HostID, float32(1)), identity.ErrFloat, catalog.HostID},
		{"integral float", catalog.Host, attrs(catalog.HostID, 1.0), identity.ErrFloat, catalog.HostID},
		{"nil", catalog.Host, attrs(catalog.HostID, nil), identity.ErrNotScalar, catalog.HostID},
		{"slice", catalog.Host, attrs(catalog.HostID, []string{"a"}), identity.ErrNotScalar, catalog.HostID},
		{"bytes", catalog.Host, attrs(catalog.HostID, []byte("a")), identity.ErrNotScalar, catalog.HostID},
		{"map", catalog.Host, attrs(catalog.HostID, map[string]string{"a": "b"}), identity.ErrNotScalar, catalog.HostID},
		{"struct", catalog.Host, attrs(catalog.HostID, struct{}{}), identity.ErrNotScalar, catalog.HostID},
		{"pointer", catalog.Host, attrs(catalog.HostID, new(string)), identity.ErrNotScalar, catalog.HostID},
		{"invalid UTF-8", catalog.Host, attrs(catalog.HostID, "a\xffb"), identity.ErrType, catalog.HostID},
		{"over length", catalog.Host, attrs(catalog.HostID, strings.Repeat("a", identity.MaxValueLen+1)), identity.ErrTooLong, catalog.HostID},

		// Int keys.
		{"int key: float", catalog.Process, proc(8.0, goodTime), identity.ErrFloat, catalog.ProcessPID},
		{"int key: bool", catalog.Process, proc(true, goodTime), identity.ErrType, catalog.ProcessPID},
		{"int key: leading zero", catalog.Process, proc("08", goodTime), identity.ErrType, catalog.ProcessPID},
		{"int key: plus sign", catalog.Process, proc("+8", goodTime), identity.ErrType, catalog.ProcessPID},
		{"int key: hex", catalog.Process, proc("0x10", goodTime), identity.ErrType, catalog.ProcessPID},
		{"int key: leading space", catalog.Process, proc(" 8", goodTime), identity.ErrType, catalog.ProcessPID},
		{"int key: trailing space", catalog.Process, proc("8 ", goodTime), identity.ErrType, catalog.ProcessPID},
		{"int key: negative zero", catalog.Process, proc("-0", goodTime), identity.ErrType, catalog.ProcessPID},
		{"int key: decimal point", catalog.Process, proc("8.0", goodTime), identity.ErrType, catalog.ProcessPID},
		{"int key: lone minus", catalog.Process, proc("-", goodTime), identity.ErrType, catalog.ProcessPID},
		{"int key: non-ASCII digits", catalog.Process, proc("٣", goodTime), identity.ErrType, catalog.ProcessPID},
		{"int key: string overflow", catalog.Process, proc("9223372036854775808", goodTime), identity.ErrType, catalog.ProcessPID},
		{"int key: uint64 overflow", catalog.Process, proc(uint64(math.MaxInt64)+1, goodTime), identity.ErrType, catalog.ProcessPID},

		// Time keys.
		{"time key: zero time", catalog.Process, proc(1, time.Time{}), identity.ErrEmpty, catalog.ProcessCreationTime},
		{"time key: not a timestamp", catalog.Process, proc(1, "yesterday"), identity.ErrType, catalog.ProcessCreationTime},
		{"time key: space separator", catalog.Process, proc(1, "2023-11-21 09:25:34Z"), identity.ErrType, catalog.ProcessCreationTime},
		{"time key: ISO 8601 basic format", catalog.Process, proc(1, "20231121T092534Z"), identity.ErrType, catalog.ProcessCreationTime},
		{"time key: no zone", catalog.Process, proc(1, "2023-11-21T09:25:34"), identity.ErrType, catalog.ProcessCreationTime},
		{"time key: integer", catalog.Process, proc(1, 1700000000), identity.ErrType, catalog.ProcessCreationTime},
		{"time key: float", catalog.Process, proc(1, 1.7e9), identity.ErrFloat, catalog.ProcessCreationTime},
	}

	r := newResolver()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			id, err := r.Resolve(tt.typ, tt.attrs)
			if err == nil {
				t.Fatalf("Resolve succeeded: %s", id)
			}
			if !id.IsZero() {
				t.Error("Resolve returned an identity alongside an error")
			}
			if !errors.Is(err, tt.want) {
				t.Errorf("error does not wrap %v: %v", tt.want, err)
			}
			var ae *identity.AttrError
			if !errors.As(err, &ae) {
				t.Fatalf("error is not an *AttrError: %v", err)
			}
			if ae.Type != tt.typ || ae.Key != tt.offend {
				t.Errorf("error names (%q, %q), want (%q, %q): %v", ae.Type, ae.Key, tt.typ, tt.offend, err)
			}
		})
	}
}

func TestResolveReportsEveryViolation(t *testing.T) {
	t.Parallel()

	_, err := newResolver().Resolve(catalog.Process, attrs(
		catalog.ProcessPID, 1.5, // ErrFloat
		catalog.ProcessCreationTime, "yesterday", // ErrType
		"stray", "x", // ErrUnregistered; host.id is also missing (ErrMissing)
	))
	for _, want := range []error{identity.ErrFloat, identity.ErrType, identity.ErrUnregistered, identity.ErrMissing} {
		if !errors.Is(err, want) {
			t.Errorf("error does not wrap %v: %v", want, err)
		}
	}
}

func TestLenientIgnoresUnregisteredKeysOnly(t *testing.T) {
	t.Parallel()

	strict, lenient := newResolver(), newResolver(identity.WithLenient())
	withExtra := attrs(catalog.HostID, goodHost, "host.name", "web-1", "cloud.region", "eu-west-1")

	if _, err := strict.Resolve(catalog.Host, withExtra); !errors.Is(err, identity.ErrUnregistered) {
		t.Errorf("strict: err = %v, want ErrUnregistered", err)
	}
	got, err := lenient.Resolve(catalog.Host, withExtra)
	if err != nil {
		t.Fatalf("lenient: %v", err)
	}
	want, _ := strict.Resolve(catalog.Host, attrs(catalog.HostID, goodHost))
	if got != want {
		t.Errorf("extra attributes changed the identity: %s vs %s", got, want)
	}

	// Every other rule still applies.
	if _, err := lenient.Resolve(catalog.Host, attrs(catalog.HostID, "", "host.name", "x")); !errors.Is(err, identity.ErrEmpty) {
		t.Errorf("lenient accepted an empty required key: %v", err)
	}
	if _, err := lenient.Resolve(catalog.Host, attrs(catalog.HostID, "a", catalog.HostID, "b")); !errors.Is(err, identity.ErrDuplicateKey) {
		t.Errorf("lenient accepted a duplicate key: %v", err)
	}
}

func TestResolveBoundaries(t *testing.T) {
	t.Parallel()

	r := newResolver()
	if _, err := r.Resolve(catalog.Host, attrs(catalog.HostID, strings.Repeat("a", identity.MaxValueLen))); err != nil {
		t.Errorf("a value of exactly MaxValueLen was rejected: %v", err)
	}
	// Surrounding whitespace is part of the value, never trimmed.
	padded, err1 := r.Resolve(catalog.Host, attrs(catalog.HostID, " a "))
	plain, err2 := r.Resolve(catalog.Host, attrs(catalog.HostID, "a"))
	if err1 != nil || err2 != nil {
		t.Fatal(err1, err2)
	}
	if padded == plain {
		t.Error("whitespace around a value was trimmed")
	}
	// Case is part of the value too.
	upper, _ := r.Resolve(catalog.Host, attrs(catalog.HostID, "A"))
	if upper == plain {
		t.Error("case was folded")
	}
}

// TestSameIdentityDifferentSpellings checks that producers that disagree on
// how to write a value still agree on the entity, and that a different value
// is a different entity.
func TestSameIdentityDifferentSpellings(t *testing.T) {
	t.Parallel()

	r := newResolver()
	instant := time.Date(2023, 11, 21, 9, 25, 34, 853_000_000, time.UTC)
	cet := time.FixedZone("CET", 3600)

	process := func(pid, created any) identity.Identity {
		t.Helper()
		id, err := r.Resolve(catalog.Process, attrs(
			catalog.HostID, goodHost, catalog.ProcessPID, pid, catalog.ProcessCreationTime, created))
		if err != nil {
			t.Fatalf("pid %v, time %v: %v", pid, created, err)
		}
		return id
	}

	want := process(8080, instant)
	same := map[string]identity.Identity{
		"pid as int64":            process(int64(8080), instant),
		"pid as uint16":           process(uint16(8080), instant),
		"pid as decimal string":   process("8080", instant),
		"time as UTC string":      process(8080, goodTime),
		"time in another zone":    process(8080, instant.In(cet)),
		"time string with offset": process(8080, "2023-11-21T10:25:34.853+01:00"),
		"time with padded zeros":  process(8080, "2023-11-21T09:25:34.853000000Z"),
	}
	for name, got := range same {
		if got != want {
			t.Errorf("%s: identity differs: %s vs %s", name, got, want)
		}
	}

	different := map[string]identity.Identity{
		"another pid":        process(8081, instant),
		"one millisecond on": process(8080, instant.Add(time.Millisecond)),
		"one nanosecond on":  process(8080, instant.Add(time.Nanosecond)),
	}
	for name, got := range different {
		if got == want {
			t.Errorf("%s: identity is the same", name)
		}
	}
}

func TestOptionalNamespace(t *testing.T) {
	t.Parallel()

	r := newResolver()
	svc := func(a ...identity.Attr) identity.Identity {
		t.Helper()
		id, err := r.Resolve(catalog.Service, a)
		if err != nil {
			t.Fatal(err)
		}
		return id
	}

	absent := svc(identity.Attr{Key: catalog.ServiceName, Value: "web"})
	empty := svc(identity.Attr{Key: catalog.ServiceNamespace, Value: ""}, identity.Attr{Key: catalog.ServiceName, Value: "web"})
	set := svc(identity.Attr{Key: catalog.ServiceNamespace, Value: "shop"}, identity.Attr{Key: catalog.ServiceName, Value: "web"})

	if absent != empty {
		t.Errorf("an empty namespace is not the same as an absent one: %s vs %s", empty, absent)
	}
	if absent == set {
		t.Error("a namespace did not change the identity")
	}
	if got := len(absent.Attrs()); got != 1 {
		t.Errorf("absent namespace: %d attributes, want 1", got)
	}
}

// TestDifferentTypesNeverCollide checks that the entity type is part of the
// identity: the same attributes on two types give different fingerprints.
func TestDifferentTypesNeverCollide(t *testing.T) {
	t.Parallel()

	r := newResolver()
	a, err1 := r.Resolve(catalog.K8sPod, attrs(catalog.K8sPodUID, "abc"))
	b, err2 := r.Resolve(catalog.K8sNode, attrs(catalog.K8sNodeUID, "abc"))
	if err1 != nil || err2 != nil {
		t.Fatal(err1, err2)
	}
	if a.Fingerprint() == b.Fingerprint() || a.Canonical() == b.Canonical() {
		t.Error("a pod and a node with the same uid collide")
	}
}

func TestAttrsReturnsTypedValues(t *testing.T) {
	t.Parallel()

	id, err := newResolver().Resolve(catalog.Process, attrs(
		catalog.ProcessCreationTime, "2023-11-21T10:25:34.853+01:00",
		catalog.ProcessPID, "1234",
		catalog.HostID, goodHost,
	))
	if err != nil {
		t.Fatal(err)
	}
	want := []identity.Attr{ // in name order
		{Key: catalog.HostID, Value: goodHost},
		{Key: catalog.ProcessCreationTime, Value: time.Date(2023, 11, 21, 9, 25, 34, 853_000_000, time.UTC)},
		{Key: catalog.ProcessPID, Value: int64(1234)},
	}
	got := id.Attrs()
	if len(got) != len(want) {
		t.Fatalf("Attrs() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i].Key != want[i].Key {
			t.Errorf("Attrs()[%d].Key = %s, want %s", i, got[i].Key, want[i].Key)
		}
		if tw, ok := want[i].Value.(time.Time); ok {
			if tg, ok := got[i].Value.(time.Time); !ok || !tg.Equal(tw) || tg.Location() != time.UTC {
				t.Errorf("Attrs()[%d].Value = %v, want %v in UTC", i, got[i].Value, tw)
			}
		} else if got[i].Value != want[i].Value {
			t.Errorf("Attrs()[%d].Value = %#v, want %#v", i, got[i].Value, want[i].Value)
		}
	}
}

func TestParse(t *testing.T) {
	t.Parallel()

	r := newResolver()
	id, err := r.Resolve(catalog.Process, attrs(
		catalog.HostID, goodHost, catalog.ProcessPID, 7, catalog.ProcessCreationTime, goodTime))
	if err != nil {
		t.Fatal(err)
	}
	back, err := r.Parse(id.Canonical())
	if err != nil {
		t.Fatal(err)
	}
	if back != id {
		t.Errorf("Parse(Canonical()) = %s, want %s", back, id)
	}

	// An identity valid in structure but not under this catalog.
	other := identity.NewResolver(mustCatalog(t, []catalog.EntitySpec{
		{ID: 1, Type: "gadget", Layer: catalog.L1, Keys: []catalog.Key{{Name: "gadget.id", Kind: catalog.KindString}}},
		{ID: 2, Type: "host", Layer: catalog.L1, Keys: []catalog.Key{{Name: "host.id", Kind: catalog.KindInt}}},
	}))
	gadget, err := other.Resolve("gadget", attrs(catalog.AttributeKey("gadget.id"), "g"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Parse(gadget.Canonical()); !errors.Is(err, identity.ErrUnknownType) {
		t.Errorf("unknown type: err = %v", err)
	}
	intHost, err := other.Resolve("host", attrs(catalog.HostID, 5))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Parse(intHost.Canonical()); !errors.Is(err, identity.ErrType) {
		t.Errorf("wrong kind: err = %v", err)
	}

	for _, bad := range []string{"", "x", id.Canonical() + "x", id.Canonical()[:len(id.Canonical())-1]} {
		if _, err := r.Parse(bad); !errors.Is(err, identity.ErrNonCanonical) {
			t.Errorf("Parse(%q): err = %v, want ErrNonCanonical", bad, err)
		}
	}
}

func mustCatalog(t *testing.T, e []catalog.EntitySpec) *catalog.Catalog {
	t.Helper()
	c, err := catalog.New(e, nil)
	if err != nil {
		t.Fatal(err)
	}
	return c
}
