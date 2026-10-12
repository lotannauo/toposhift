package pebblestore

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/pebblekv"
)

// The value at the meta key "skew" is a stored format: see the file's "about".
func TestMetaSkewGoldenVectors(t *testing.T) {
	t.Parallel()
	raw, err := os.ReadFile("testdata/meta_skew_v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var f struct {
		Key     string
		Vectors []struct {
			Name    string
			SkewNs  int64
			Encoded string
		}
		Refused []struct{ Name, Encoded string }
	}
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	if got := hex.EncodeToString(metaKey(metaSkew)); got != f.Key {
		t.Errorf("the key is %s, the vectors say %s", got, f.Key)
	}
	if len(f.Vectors) == 0 || len(f.Refused) == 0 {
		t.Fatal("no vectors")
	}
	for _, v := range f.Vectors {
		want, err := hex.DecodeString(v.Encoded)
		if err != nil {
			t.Fatal(err)
		}
		if got := encodeSkew(time.Duration(v.SkewNs)); !bytes.Equal(got, want) {
			t.Errorf("%s: encoded %x, want %x", v.Name, got, want)
		}
		if d, err := decodeSkew(want); err != nil || d != time.Duration(v.SkewNs) {
			t.Errorf("%s: decoded %v, %v", v.Name, d, err)
		}
	}
	for _, r := range f.Refused {
		b, err := hex.DecodeString(r.Encoded)
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			b = []byte{}
		}
		if d, err := decodeSkew(b); err == nil {
			t.Errorf("%s: decoded %v, want a refusal", r.Name, d)
		}
	}
	if d, err := decodeSkew(nil); err != nil || d != 0 {
		t.Errorf("an absent key: %v, %v; want zero", d, err)
	}
}

// metaOf is every meta key and value of a store, in key order.
func metaOf(t *testing.T, s *Store) map[string]string {
	t.Helper()
	lo, _ := dataBounds()
	it, err := s.kv.NewIter(&pebble.IterOptions{LowerBound: []byte{metaLead}, UpperBound: lo})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = it.Close() }()
	out := map[string]string{}
	for ok := it.First(); ok; ok = it.Next() {
		out[string(it.Key()[1:])] = string(it.Value())
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	return out
}

// A skew that is not zero is stored when the database is created, and a store
// opened with another, zero included, is refused with an error that names both; a
// store with a skew of zero writes no key, so its meta is what it was before the
// key, and opens as it did.
func TestTheSkewIsStoredAndAnotherIsRefused(t *testing.T) {
	t.Parallel()
	three := lifecycle.Policy{BootKey: lifecycle.BootID, Skew: 3}
	for name, tc := range map[string]struct{ created, reopened lifecycle.Policy }{
		"a skew then none":                   {three, bootIDPolicy},
		"none then a skew":                   {bootIDPolicy, three},
		"one skew then another":              {three, lifecycle.Policy{BootKey: lifecycle.BootID, Skew: 4}},
		"a skew with no boot key, then none": {lifecycle.Policy{Skew: time.Second}, lifecycle.Policy{}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cfg := pebblekv.Config{Tuning: pebblekv.TinyTuning(), FS: vfs.NewMem()}
			s, err := Open("db", Options{Config: cfg, Policy: tc.created})
			if err != nil {
				t.Fatal(err)
			}
			if got, want := metaOf(t, s)[metaSkew], string(encodeSkew(tc.created.Skew)); tc.created.Skew != 0 && got != want {
				t.Errorf("the skew key holds %x, want %x", got, want)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			_, err = Open("db", Options{Config: cfg, Policy: tc.reopened})
			if !errors.Is(err, store.ErrInvalid) || !strings.Contains(err.Error(), "skew of "+tc.created.Skew.String()+", not "+tc.reopened.Skew.String()) {
				t.Fatalf("reopened with another skew: %v, want an error wrapping ErrInvalid that names both", err)
			}
			again, err := Open("db", Options{Config: cfg, Policy: tc.created})
			if err != nil {
				t.Fatalf("reopened with the same skew: %v", err)
			}
			_ = again.Close()
		})
	}

	// A skew of zero writes no key: the meta of a new database is exactly what it
	// was before the key, under the zero policy and the product's.
	for name, pol := range map[string]lifecycle.Policy{"the zero policy": {}, "the product's policy": DefaultOptions().Policy} {
		s := openMem(t, Options{Policy: pol})
		want := map[string]string{
			pebblekv.MetaFormat: formatTag,
			metaPebble:          string(pebbleTag(linkedPebbleVersion(), s.kv.FormatMajorVersion())),
		}
		if pol.BootKey != "" {
			want[metaBootKey] = string(pol.BootKey)
		}
		got := metaOf(t, s)
		delete(got, pebblekv.MetaLastSeq) // written by the first record, or not at all
		if len(got) != len(want) {
			t.Errorf("%s: the meta keys are %q, want %q", name, keysOf(got), keysOf(want))
		}
		for k, v := range want {
			if got[k] != v {
				t.Errorf("%s: meta %q = %q, want %q", name, k, got[k], v)
			}
		}
	}
}

func keysOf(m map[string]string) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	return out
}

// A database that holds a skew key no store writes is refused.
func TestAStoredSkewNoStoreWritesIsRefused(t *testing.T) {
	t.Parallel()
	for name, raw := range map[string][]byte{"zero": encodeSkew(0), "seven bytes": encodeSkew(3)[1:], "empty": {}} {
		cfg := pebblekv.Config{Tuning: pebblekv.TinyTuning(), FS: vfs.NewMem()}
		s, err := Open("db", Options{Config: cfg})
		if err != nil {
			t.Fatal(err)
		}
		if err := s.kv.Set(metaKey(metaSkew), raw, pebble.Sync); err != nil {
			t.Fatal(err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		if s, err := Open("db", Options{Config: cfg}); !errors.Is(err, store.ErrInvalid) {
			if s != nil {
				_ = s.Close()
			}
			t.Errorf("%s: Open = %v, want ErrInvalid", name, err)
		}
	}
}

// A negative skew is refused before anything is written, so neither a new
// directory nor an existing database is left in a state another Open refuses.
func TestANegativeSkewIsRefusedAndWritesNothing(t *testing.T) {
	t.Parallel()
	negative := lifecycle.Policy{BootKey: lifecycle.BootID, Skew: -1}
	for name, existing := range map[string]bool{"a new directory": false, "an existing database": true} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			fs := vfs.NewMem()
			cfg := pebblekv.Config{Tuning: pebblekv.TinyTuning(), FS: fs}
			if existing {
				s, err := Open("db", Options{Config: cfg, Policy: bootIDPolicy})
				if err != nil {
					t.Fatal(err)
				}
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if s, err := Open("db", Options{Config: cfg, Policy: negative}); !errors.Is(err, store.ErrInvalid) {
				if s != nil {
					_ = s.Close()
				}
				t.Fatalf("Open with a negative skew = %v, want ErrInvalid", err)
			}
			if !existing {
				if names, _ := fs.List("db"); len(names) != 0 {
					t.Errorf("a refused Open left %v in the directory", names)
				}
			}
			s, err := Open("db", Options{Config: cfg, Policy: bootIDPolicy})
			if err != nil {
				t.Fatalf("Open with a skew of zero after the refusal: %v", err)
			}
			defer func() { _ = s.Close() }()
			if _, ok := metaOf(t, s)[metaSkew]; ok {
				t.Error("a skew key was written")
			}
		})
	}
}
