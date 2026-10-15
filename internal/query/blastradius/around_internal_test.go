package blastradius

import (
	"testing"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
)

// TestWalkedByDefault checks which substrates Around walks without the option:
// a node and a host, and no other type, whether the catalog registers it today
// (a rack, a zone, a pod) or a type it adds later above a host, here a type
// the catalog does not know.
func TestWalkedByDefault(t *testing.T) {
	fp := func(typ catalog.EntityType) identity.Fingerprint {
		f, err := identity.FingerprintFromHash(typ, [identity.FingerprintBytes]byte{1})
		if err != nil {
			t.Fatal(err)
		}
		return f
	}
	for e := range catalog.Default().Entities() {
		want := e.Type() == catalog.K8sNode || e.Type() == catalog.Host
		if got := walkedByDefault(fp(e.Type())); got != want {
			t.Errorf("%s: walked by default %t, want %t", e.Type(), got, want)
		}
	}
	for _, typ := range []catalog.EntityType{"x.datacenter", "x.node.pool", "x.cluster"} {
		if walkedByDefault(fp(typ)) {
			t.Errorf("%s, a type above a host, is walked by default", typ)
		}
	}
}
