package endpoint_test

import (
	"errors"
	"net/netip"
	"slices"
	"testing"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/endpoint"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/store"
)

const cluster = "default"

// The canonical spelling: one address, however it is written, is one value.
func TestParse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in, want string
		err      error
	}{
		{"10.42.0.5", "10.42.0.5", nil},
		{"2001:DB8::1", "2001:db8::1", nil},
		{"2001:0db8:0000:0000:0000:0000:0000:0001", "2001:db8::1", nil},
		{"::ffff:10.42.0.5", "10.42.0.5", nil},
		{"::FFFF:0a2a:0005", "10.42.0.5", nil},
		{"::10.42.0.5", "::a2a:5", nil}, // IPv4-compatible, not mapped: stays IPv6
		{"010.42.0.5", "", endpoint.ErrNotAddress},
		{"10.42.0.05", "", endpoint.ErrNotAddress},
		{"::ffff:010.42.0.5", "", endpoint.ErrNotAddress},
		{"fe80::1%eth0", "", endpoint.ErrScoped},
		{"2001:db8::1%1", "", endpoint.ErrScoped},
		{"10.42.0.5%eth0", "", endpoint.ErrNotAddress},
		{" 10.42.0.5", "", endpoint.ErrNotAddress},
		{"10.42.0.5/24", "", endpoint.ErrNotAddress},
		{"10.42.0.5:80", "", endpoint.ErrNotAddress},
		{"[2001:db8::1]", "", endpoint.ErrNotAddress},
		{"localhost", "", endpoint.ErrNotAddress},
		{"", "", endpoint.ErrNotAddress},
	}
	for _, tt := range tests {
		a, err := endpoint.Parse(tt.in)
		if tt.err != nil {
			if !errors.Is(err, tt.err) || a.IsValid() {
				t.Errorf("Parse(%q) = %v, %v; want %v", tt.in, a, err, tt.err)
			}
			continue
		}
		if err != nil || a.String() != tt.want {
			t.Errorf("Parse(%q) = %v, %v; want %s", tt.in, a, err, tt.want)
		}
	}
}

// The realm of each block's first and last address, and of the address just
// outside it, written out by hand from the registries rather than read from the
// table, so that a block moved by one address fails here. IPv6 outside
// 2000::/3 is never global. G is global, K the cluster's realm, S refused as
// scoped.
func TestRealmAtTheBlockEdges(t *testing.T) {
	t.Parallel()

	const (
		G = "global"
		K = "k8s/default"
		S = "scoped"
	)
	tests := []struct{ addr, want string }{
		{"0.0.0.0", S},
		{"0.0.0.1", K},
		{"0.255.255.255", K},
		{"1.0.0.0", G},
		{"9.255.255.255", G},
		{"10.0.0.0", K},
		{"10.255.255.255", K},
		{"11.0.0.0", G},
		{"100.63.255.255", G},
		{"100.64.0.0", K},
		{"100.127.255.255", K},
		{"100.128.0.0", G},
		{"126.255.255.255", G},
		{"127.0.0.0", S},
		{"127.255.255.255", S},
		{"128.0.0.0", G},
		{"169.253.255.255", G},
		{"169.254.0.0", S},
		{"169.254.255.255", S},
		{"169.255.0.0", G},
		{"172.15.255.255", G},
		{"172.16.0.0", K},
		{"172.31.255.255", K},
		{"172.32.0.0", G},
		{"191.255.255.255", G},
		{"192.0.0.0", K},
		{"192.0.0.9", K},
		{"192.0.0.10", K},
		{"192.0.0.255", K},
		{"192.0.1.0", G},
		{"192.0.1.255", G},
		{"192.0.2.0", K},
		{"192.0.2.255", K},
		{"192.0.3.0", G},
		{"192.31.196.1", G},
		{"192.52.193.1", G},
		{"192.175.48.1", G},
		{"192.88.98.255", G},
		{"192.88.99.0", K},
		{"192.88.99.255", K},
		{"192.88.100.0", G},
		{"192.167.255.255", G},
		{"192.168.0.0", K},
		{"192.168.255.255", K},
		{"192.169.0.0", G},
		{"198.17.255.255", G},
		{"198.18.0.0", K},
		{"198.19.255.255", K},
		{"198.20.0.0", G},
		{"198.51.99.255", G},
		{"198.51.100.0", K},
		{"198.51.100.255", K},
		{"198.51.101.0", G},
		{"203.0.112.255", G},
		{"203.0.113.0", K},
		{"203.0.113.255", K},
		{"203.0.114.0", G},
		{"223.255.255.255", G},
		{"224.0.0.0", S},
		{"239.255.255.255", S},
		{"240.0.0.0", K},
		{"255.255.255.254", K},
		{"255.255.255.255", K},
		{"8.8.8.8", G},
		{"1.1.1.1", G},

		{"::", S},
		{"::1", S},
		{"::2", K},
		{"::fffe:ffff:ffff", K},
		{"::ffff:0.0.0.0", S},
		{"::ffff:10.0.0.0", K},
		{"::ffff:255.255.255.255", K},
		{"::1:0:0:0", K},
		{"64:ff9b::808:808", K},
		{"64:ff9b:0:ffff:ffff:ffff:ffff:ffff", K},
		{"64:ff9b:1::", K},
		{"64:ff9b:1:ffff:ffff:ffff:ffff:ffff", K},
		{"64:ff9b:2::", K},
		{"ff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", K},
		{"100::", K},
		{"100::ffff:ffff:ffff:ffff", K},
		{"100:0:0:1::", K},
		{"100:0:0:1:ffff:ffff:ffff:ffff", K},
		{"100:0:0:2::", K},
		{"2000:ffff:ffff:ffff:ffff:ffff:ffff:ffff", G},
		{"2001::", K},
		{"2001:1::1", K},
		{"2001:3::1", K},
		{"2001:20::1", K},
		{"2001:1ff:ffff:ffff:ffff:ffff:ffff:ffff", K},
		{"2001:200::", G},
		{"2001:db7:ffff:ffff:ffff:ffff:ffff:ffff", G},
		{"2001:db8::", K},
		{"2001:db8:ffff:ffff:ffff:ffff:ffff:ffff", K},
		{"2001:db9::", G},
		{"2001:ffff:ffff:ffff:ffff:ffff:ffff:ffff", G},
		{"2002::", K},
		{"2002:ffff:ffff:ffff:ffff:ffff:ffff:ffff", K},
		{"2003::", G},
		{"3ffe:ffff:ffff:ffff:ffff:ffff:ffff:ffff", G},
		{"3fff::", K},
		{"3fff:fff:ffff:ffff:ffff:ffff:ffff:ffff", K},
		{"3fff:1000::", G},
		{"5eff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", K},
		{"5f00::", K},
		{"5f00:ffff:ffff:ffff:ffff:ffff:ffff:ffff", K},
		{"5f01::", K},
		{"fbff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", K},
		{"fc00::", K},
		{"fdff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", K},
		{"fe00::", K},
		{"fe7f:ffff:ffff:ffff:ffff:ffff:ffff:ffff", K},
		{"fe80::", S},
		{"febf:ffff:ffff:ffff:ffff:ffff:ffff:ffff", S},
		{"fec0::", K},
		{"ff00::", S},
		{"ff02::1", S},
		{"2600:1f18::1", G},
		// IPv6 outside 2000::/3 that no block lists is not known to be unique.
		{"::10.42.0.5", K},
		{"64:ff9b::1", K},
		{"fec0::1", K},
		{"fe00::1", K},
		{"1fff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", K},
		{"2000::", G},
		{"3000::1", G},
		{"3fff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", G},
		{"4000::", K},
		{"4000::1", K},
		{"8000::1", K},
	}
	for _, tt := range tests {
		a := netip.MustParseAddr(tt.addr)
		got, err := endpoint.Realm(cluster, a)
		switch {
		case tt.want == S:
			if !errors.Is(err, endpoint.ErrScoped) {
				t.Errorf("Realm(%s) = %q, %v; want ErrScoped", tt.addr, got, err)
			}
		case err != nil || got != tt.want:
			t.Errorf("Realm(%s) = %q, %v; want %q", tt.addr, got, err, tt.want)
		}
	}
}

func TestRealmRefuses(t *testing.T) {
	t.Parallel()

	a := netip.MustParseAddr("10.42.0.5")
	for _, c := range []string{"", "a b", "Ä"} {
		if r, err := endpoint.Realm(c, a); !errors.Is(err, store.ErrInvalid) {
			t.Errorf("Realm(%q) = %q, %v; want store.ErrInvalid", c, r, err)
		}
	}
	// The cluster is checked for a global address too, so a writer's bad name
	// never depends on which address it reads first.
	if r, err := endpoint.Realm("", netip.MustParseAddr("8.8.8.8")); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("Realm of a global address in no cluster = %q, %v", r, err)
	}
	if r, err := endpoint.Realm(cluster, netip.Addr{}); !errors.Is(err, endpoint.ErrNotAddress) {
		t.Errorf("Realm of the zero address = %q, %v", r, err)
	}
	if r, err := endpoint.Realm(cluster, netip.MustParseAddr("fd00::1%eth0")); !errors.Is(err, endpoint.ErrScoped) {
		t.Errorf("Realm of an address with a zone = %q, %v", r, err)
	}
	// A mapped address is judged as the address it maps.
	if r, err := endpoint.Realm(cluster, netip.MustParseAddr("::ffff:8.8.8.8")); err != nil || r != endpoint.GlobalRealm {
		t.Errorf("Realm(::ffff:8.8.8.8) = %q, %v", r, err)
	}
	// The cluster name is taken exactly, "/" and ":" included.
	if r, err := endpoint.Realm("prod/eu-1:a", a); err != nil || r != "k8s/prod/eu-1:a" {
		t.Errorf("Realm in prod/eu-1:a = %q, %v", r, err)
	}
}

// The realm is in the identity: a private address of two clusters is two
// endpoints, a global one is one.
func TestIdentity(t *testing.T) {
	t.Parallel()

	id := func(c, s string) identity.Identity {
		t.Helper()
		v, err := endpoint.Identity(c, s)
		if err != nil {
			t.Fatalf("Identity(%q, %q): %v", c, s, err)
		}
		return v
	}
	if a, b := id("one", "10.42.0.5"), id("two", "10.42.0.5"); a.Fingerprint() == b.Fingerprint() {
		t.Error("10.42.0.5 of two clusters is one endpoint")
	}
	if a, b := id("one", "8.8.8.8"), id("two", "8.8.8.8"); a != b {
		t.Errorf("8.8.8.8 of two clusters is %s and %s", a, b)
	}
	for _, same := range [][]string{
		{"10.42.0.5", "::ffff:10.42.0.5", "::FFFF:a2a:5"},
		{"2001:db8::1", "2001:DB8::1", "2001:0db8:0:0:0:0:0:0001"},
	} {
		want := id(cluster, same[0])
		for _, s := range same[1:] {
			if got := id(cluster, s); got != want {
				t.Errorf("%q is %s, %q is %s", s, got, same[0], want)
			}
		}
	}
	v := id("prod/eu-1:a", "::ffff:10.42.0.5")
	if v.Type() != catalog.NetworkEndpoint || !slices.Equal(v.Attrs(), []identity.Attr{
		{Key: catalog.EndpointAddress, Value: "10.42.0.5"}, {Key: catalog.EndpointRealm, Value: "k8s/prod/eu-1:a"},
	}) {
		t.Errorf("identity %s, attributes %v", v.Type(), v.Attrs())
	}
	for _, s := range []string{"010.42.0.5", "fe80::1", "fe80::1%eth0", "127.0.0.1", "nonsense"} {
		if v, err := endpoint.Identity(cluster, s); err == nil || !v.IsZero() {
			t.Errorf("Identity(%q) = %s, %v", s, v, err)
		}
	}
	if v, err := endpoint.Identity("", "10.42.0.5"); !errors.Is(err, store.ErrInvalid) || !v.IsZero() {
		t.Errorf("Identity in no cluster = %s, %v", v, err)
	}
}

// An optional key added to topo.endpoint later, such as a port, keeps every
// fingerprint made without it: an absent optional key is not encoded.
func TestAnOptionalKeyAddedLaterKeepsEveryFingerprint(t *testing.T) {
	t.Parallel()

	e, ok := catalog.Default().Entity(catalog.NetworkEndpoint)
	if !ok {
		t.Fatal("topo.endpoint is not registered")
	}
	keys := append(slices.Collect(e.Keys()), catalog.Key{Name: "topo.endpoint.port", Kind: catalog.KindInt, Optional: true})
	c, err := catalog.New([]catalog.EntitySpec{{ID: e.ID(), Type: e.Type(), Layer: e.Layer(), Keys: keys}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	later := identity.NewResolver(c)
	for _, s := range []string{"10.42.0.5", "8.8.8.8", "fd00::1", "2600:1f18::1"} {
		want, err := endpoint.Identity(cluster, s)
		if err != nil {
			t.Fatal(err)
		}
		got, err := later.Resolve(catalog.NetworkEndpoint, want.Attrs())
		if err != nil || got.Fingerprint() != want.Fingerprint() {
			t.Errorf("%s with a port key registered: %v, %v; want %s", s, got, err, want)
		}
		withPort, err := later.Resolve(catalog.NetworkEndpoint, append(want.Attrs(), identity.Attr{Key: "topo.endpoint.port", Value: int64(80)}))
		if err != nil || withPort.Fingerprint() == want.Fingerprint() {
			t.Errorf("%s with a port: %v, %v", s, withPort, err)
		}
	}
}

// The fingerprints of two endpoints, computed outside Go from the identity
// encoding's specification: the preimage built by an independent script and
// hashed with SHA-256, taking the first 32 hex digits.
func TestHandVerifiedFingerprints(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct{ in, want string }{
		{"10.42.0.5", "topo.endpoint:5748edb0ec76293370c331921ba78add"},   // realm k8s/default
		{"8.8.8.8", "topo.endpoint:130354458a38130bd6c9e3332a612714"},     // realm global
		{"2001:DB8::1", "topo.endpoint:11029333a10afd831654cfbd7868077e"}, // realm k8s/default
	} {
		id, err := endpoint.Identity(cluster, tt.in)
		if err != nil || id.String() != tt.want {
			t.Errorf("Identity(%q) = %v, %v; want %s", tt.in, id, err, tt.want)
		}
	}
}
