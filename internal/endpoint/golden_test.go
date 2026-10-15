package endpoint

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"net/netip"
	"os"
	"slices"
	"testing"

	"github.com/lotannauo/toposhift/internal/store"
)

// The canonical form is a stored format: an endpoint's fingerprint keys every
// record about it, so a changed realm or address spelling silently splits one
// address into two entities. This file freezes the form, the table included.
// If an output changes, the form changed, and that needs a new file version and
// a migration of stored endpoints, never a regenerated golden.
//
// Regenerate only to add a case:
//
//	go test ./internal/endpoint -run TestCanonicalFormIsFrozen -update
var update = flag.Bool("update", false, "rewrite testdata/canonical_v1.json from canonicalCases")

const canonicalFile = "testdata/canonical_v1.json"

type canonicalCase struct {
	Cluster string `json:"cluster"`
	Input   string `json:"input"`
}

type canonicalVector struct {
	canonicalCase
	Realm       string `json:"realm,omitempty"`
	Address     string `json:"address,omitempty"`
	Fingerprint string `json:"fingerprint,omitempty"`
	Refused     string `json:"refused,omitempty"`
}

var canonicalCases = []canonicalCase{
	{"default", "10.42.0.5"},
	{"prod/eu-1:a", "10.42.0.5"},
	{"default", "8.8.8.8"},
	{"prod/eu-1:a", "8.8.8.8"},
	{"default", "2001:DB8::1"},
	{"default", "2001:0db8:0000:0000:0000:0000:0000:0001"},
	{"default", "2600:1F18::1"},
	{"default", "2600:1f18::1"},
	{"default", "::ffff:10.42.0.5"},
	{"default", "::FFFF:8.8.8.8"},
	{"default", "fd00::1"},
	{"default", "100.64.0.1"},
	{"default", "010.42.0.5"},
	{"default", "10.42.0.05"},
	{"default", " 10.42.0.5"},
	{"default", "10.42.0.5/24"},
	{"default", ""},
	{"default", "fe80::1%eth0"},
	{"default", "2001:db8::1%eth0"},
	{"default", "127.0.0.1"},
	{"default", "::1"},
	{"default", "0.0.0.0"},
	{"default", "::"},
	{"default", "169.254.1.1"},
	{"default", "fe80::1"},
	{"default", "224.0.0.1"},
	{"default", "ff02::1"},
	{"default", "::ffff:127.0.0.1"},
	{"", "10.42.0.5"},
}

// outsideEdges are, written by hand from the registries and never read from the
// table, the address just below the first and just above the last of every
// table block where one exists, and both sides of 2000::/3, so the file pins
// both sides of every edge whatever the table later says.
var outsideEdges = []string{
	"1.0.0.0", "0.0.0.1",
	"9.255.255.255", "11.0.0.0",
	"100.63.255.255", "100.128.0.0",
	"126.255.255.255", "128.0.0.0",
	"169.253.255.255", "169.255.0.0",
	"172.15.255.255", "172.32.0.0",
	"191.255.255.255", "192.0.1.0",
	"192.0.0.7", "192.0.0.9",
	"192.0.0.169", "192.0.0.172",
	"192.0.1.255", "192.0.3.0",
	"192.88.98.255", "192.88.100.0",
	"192.88.99.1", "192.88.99.3",
	"192.167.255.255", "192.169.0.0",
	"198.17.255.255", "198.20.0.0",
	"198.51.99.255", "198.51.101.0",
	"203.0.112.255", "203.0.114.0",
	"239.255.255.255", "255.255.255.254",
	"::2",
	"::fffe:ffff:ffff", "::1:0:0:0",
	"64:ff9b:0:ffff:ffff:ffff:ffff:ffff", "64:ff9b:2::",
	"ff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", "100:0:0:2::",
	"2000:ffff:ffff:ffff:ffff:ffff:ffff:ffff", "2001:200::",
	"2001:1::",
	"2001:1:ffff:ffff:ffff:ffff:ffff:ffff", "2001:2:1::",
	"2001:f:ffff:ffff:ffff:ffff:ffff:ffff", "2001:20::",
	"2001:db7:ffff:ffff:ffff:ffff:ffff:ffff", "2001:db9::",
	"2001:ffff:ffff:ffff:ffff:ffff:ffff:ffff", "2003::",
	"3ffe:ffff:ffff:ffff:ffff:ffff:ffff:ffff", "3fff:1000::",
	"5eff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", "5f01::",
	"fbff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", "fe00::",
	"fe7f:ffff:ffff:ffff:ffff:ffff:ffff:ffff", "fec0::",
	"1fff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", "2000::",
	"3fff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", "4000::",
	"::10.42.0.5", "64:ff9b::1", "fec0::1", "fe00::1", "3000::1", "4000::1", "8000::1",
}

// tableCases are the first and last address of every block of the table.
func tableCases() []canonicalCase {
	var out []canonicalCase
	for _, p := range notGlobalBlocks {
		out = append(out, canonicalCase{"default", p.Masked().Addr().String()}, canonicalCase{"default", lastOf(p).String()})
	}
	return out
}

// lastOf is the last address of p.
func lastOf(p netip.Prefix) netip.Addr {
	b := p.Masked().Addr().AsSlice()
	for i := p.Bits(); i < len(b)*8; i++ {
		b[i/8] |= 0x80 >> (i % 8)
	}
	a, _ := netip.AddrFromSlice(b)
	return a
}

func refusal(err error) string {
	switch {
	case errors.Is(err, ErrNotAddress):
		return "not_address"
	case errors.Is(err, ErrScoped):
		return "scoped"
	case errors.Is(err, store.ErrInvalid):
		return "cluster"
	}
	return "other: " + err.Error()
}

func buildCanonical() []canonicalVector {
	var out []canonicalVector
	cases := slices.Clone(canonicalCases)
	for _, a := range outsideEdges {
		cases = append(cases, canonicalCase{"default", a})
	}
	for _, c := range append(cases, tableCases()...) {
		v := canonicalVector{canonicalCase: c}
		id, err := Identity(c.Cluster, c.Input)
		if err != nil {
			v.Refused = refusal(err)
		} else {
			for _, a := range id.Attrs() {
				switch a.Key {
				case "topo.endpoint.realm":
					v.Realm = a.Value.(string)
				case "topo.endpoint.address":
					v.Address = a.Value.(string)
				}
			}
			v.Fingerprint = id.Fingerprint().String()
		}
		out = append(out, v)
	}
	return out
}

func TestCanonicalFormIsFrozen(t *testing.T) {
	doc, err := json.MarshalIndent(buildCanonical(), "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	doc = append(doc, '\n')
	if *update {
		if err := os.WriteFile(canonicalFile, doc, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(canonicalFile)
	if err != nil {
		t.Fatalf("%v (create it with -update)", err)
	}
	if !bytes.Equal(doc, want) {
		t.Fatalf("the canonical form of an address no longer matches %s.\n"+
			"Stored endpoints would be split from their records. If the change is intended it needs a new "+
			"file version and a migration; do not regenerate this file. Diff the output of "+
			"`go test -run TestCanonicalFormIsFrozen -update` against git to see what moved.", canonicalFile)
	}
}

// TestCanonicalFileReplays reads the file itself, so a case removed from the
// lists but still on disk is still checked, and checks what every vector must
// hold whatever the table says.
func TestCanonicalFileReplays(t *testing.T) {
	t.Parallel()

	raw, err := os.ReadFile(canonicalFile)
	if err != nil {
		t.Fatal(err)
	}
	var vs []canonicalVector
	if err := json.Unmarshal(raw, &vs); err != nil {
		t.Fatal(err)
	}
	for _, v := range vs {
		id, err := Identity(v.Cluster, v.Input)
		if v.Refused != "" {
			if err == nil || refusal(err) != v.Refused {
				t.Errorf("%q in %q: %v, %v; want refused as %s", v.Input, v.Cluster, id, err, v.Refused)
			}
			continue
		}
		if err != nil || id.Fingerprint().String() != v.Fingerprint {
			t.Errorf("%q in %q: %v, %v; want %s", v.Input, v.Cluster, id, err, v.Fingerprint)
		}
		// The address is canonical: it reads back as itself.
		if a, err := Parse(v.Address); err != nil || a.String() != v.Address {
			t.Errorf("%q: the address %q reads back as %v, %v", v.Input, v.Address, a, err)
		}
		if v.Realm != GlobalRealm && v.Realm != "k8s/"+v.Cluster {
			t.Errorf("%q in %q: realm %q", v.Input, v.Cluster, v.Realm)
		}
	}
	if len(vs) < len(canonicalCases) {
		t.Errorf("the file has %d vectors, fewer than the %d cases", len(vs), len(canonicalCases))
	}
}

// Every block's first and last address is outside the global realm, and the
// table's blocks are well formed.
func TestTableBlocks(t *testing.T) {
	t.Parallel()

	for _, p := range notGlobalBlocks {
		if p != p.Masked() || p.Addr().Zone() != "" {
			t.Errorf("block %s is not in canonical form", p)
		}
		for _, a := range []netip.Addr{p.Masked().Addr(), lastOf(p)} {
			if !notGlobal(a) {
				t.Errorf("%s of %s is global", a, p)
			}
			if r, err := Realm("default", a); err == nil && r == GlobalRealm {
				t.Errorf("Realm(%s) of %s is global", a, p)
			}
		}
	}
}
