package endpoint_test

import (
	"testing"

	"github.com/lotannauo/toposhift/internal/endpoint"
)

// FuzzCanonicalForm feeds two arbitrary strings through the canonical form. It
// must never panic. What Parse accepts reads back as itself from its own text
// (the form is idempotent), and two inputs that parse to one address are one
// identity.
func FuzzCanonicalForm(f *testing.F) {
	f.Add("10.42.0.5", "::ffff:10.42.0.5")
	f.Add("2001:DB8::1", "2001:0db8:0:0:0:0:0:1")
	f.Add("8.8.8.8", "::FFFF:808:808")
	f.Add("fe80::1%eth0", "fe80::1")
	f.Add("010.42.0.5", "10.42.0.5")
	f.Add("", "::")

	f.Fuzz(func(t *testing.T, s1, s2 string) {
		a1, err1 := endpoint.Parse(s1)
		if err1 != nil {
			if a1.IsValid() {
				t.Fatalf("Parse(%q) returned %v alongside %v", s1, a1, err1)
			}
			return
		}
		again, err := endpoint.Parse(a1.String())
		if err != nil || again != a1 || again.String() != a1.String() {
			t.Fatalf("Parse(%q) = %v, which reads back as %v, %v", s1, a1, again, err)
		}
		if a1.Is4In6() || a1.Zone() != "" {
			t.Fatalf("Parse(%q) = %v, not canonical", s1, a1)
		}
		id1, err1 := endpoint.Identity("default", s1)
		idc, errc := endpoint.Identity("default", a1.String())
		if (err1 == nil) != (errc == nil) || id1 != idc {
			t.Fatalf("Identity(%q) = %v, %v; of its canonical text %v, %v", s1, id1, err1, idc, errc)
		}
		a2, err := endpoint.Parse(s2)
		if err != nil || a2 != a1 {
			return
		}
		id2, err2 := endpoint.Identity("default", s2)
		if (err1 == nil) != (err2 == nil) || id1 != id2 {
			t.Fatalf("%q and %q are one address, but identities %v, %v and %v, %v", s1, s2, id1, err1, id2, err2)
		}
	})
}
