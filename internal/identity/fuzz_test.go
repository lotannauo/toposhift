package identity_test

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
)

// FuzzResolve feeds arbitrary strings through every entity type, as every
// kind of value a string can spell. It must never panic, and anything it
// accepts must round-trip.
func FuzzResolve(f *testing.F) {
	f.Add("host", "host.id", "h1", "", "")
	f.Add("process", "host.id", "i-0abc", "process.pid", "1234")
	f.Add("process", "process.creation.time", "2023-11-21T09:25:34.853Z", "process.pid", "-1")
	f.Add("service", "service.namespace", "", "service.name", "web")
	f.Add("host", "host.id", "a\x1fb", "host.id", "dup")
	f.Add("", "", "", "", "")

	r := newResolver(identity.WithLenient())
	strict := newResolver()
	f.Fuzz(func(t *testing.T, typ, k1, v1, k2, v2 string) {
		in := attrs(k1, v1, k2, v2)
		id, err := r.Resolve(catalog.EntityType(typ), in)
		if err != nil {
			if !id.IsZero() {
				t.Fatal("identity returned alongside an error")
			}
			return
		}
		if again, err := r.Resolve(catalog.EntityType(typ), in); err != nil || again != id {
			t.Fatalf("not deterministic: %v, %v", again, err)
		}
		parsed, err := strict.Parse(id.Canonical())
		if err != nil || parsed != id {
			t.Fatalf("Parse(Canonical()) = %v, %v; want %s", parsed, err, id)
		}
		if again, err := strict.Resolve(id.Type(), id.Attrs()); err != nil || again != id {
			t.Fatalf("Resolve(Attrs()) = %v, %v; want %s", again, err, id)
		}
	})
}

// FuzzParse feeds arbitrary bytes to the strict decoder. It must never
// panic, and anything it accepts must be the one canonical encoding of what
// it decodes to: re-resolving the decoded attributes reproduces the bytes.
func FuzzParse(f *testing.F) {
	raw, err := os.ReadFile(goldenFile)
	if err != nil {
		f.Fatal(err)
	}
	var doc goldenDoc
	if err := json.Unmarshal(raw, &doc); err != nil {
		f.Fatal(err)
	}
	for _, v := range doc.Vectors {
		canon, _ := hex.DecodeString(v.Canonical)
		f.Add(string(canon))
	}
	f.Add("")
	f.Add("toposhift/identity")

	r := newResolver()
	f.Fuzz(func(t *testing.T, canonical string) {
		id, err := r.Parse(canonical)
		if err != nil {
			return
		}
		if id.Canonical() != canonical {
			t.Fatal("Parse changed the bytes")
		}
		again, err := r.Resolve(id.Type(), id.Attrs())
		if err != nil {
			t.Fatalf("an accepted identity does not resolve: %v", err)
		}
		if again.Canonical() != canonical {
			t.Fatalf("accepted a non-canonical encoding:\n got %x\nwant %x", canonical, again.Canonical())
		}
	})
}
