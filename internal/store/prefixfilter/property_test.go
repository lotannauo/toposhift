package prefixfilter

import (
	"math/rand/v2"
	"slices"
	"testing"

	"pgregory.net/rapid"
)

// TestNoFalseNegatives checks the property the store depends on: whatever set
// of keys a filter is built from, it contains every one of them. Nothing here
// depends on what a draw reaches; the assertions hold for every draw.
func TestNoFalseNegatives(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Stored prefixes are 20 bytes and share leading bytes, so half the keys
		// are one fixed 16-byte head with a short varying tail, and the rest are
		// arbitrary strings of 0 to 40 bytes, the empty one included.
		head := rapid.SliceOfN(rapid.Byte(), 16, 16).Draw(t, "head")
		shared := rapid.Custom(func(t *rapid.T) []byte {
			return append(slices.Clone(head), rapid.SliceOfN(rapid.Byte(), 4, 4).Draw(t, "tail")...)
		})
		anyKey := rapid.SliceOfN(rapid.Byte(), 0, 40)
		keys := rapid.SliceOfN(rapid.OneOf(shared, anyKey), 0, 5000).Draw(t, "keys")

		f, err := Build(slices.Values(keys))
		if err != nil {
			t.Fatalf("Build of %d keys: %v", len(keys), err)
		}

		distinct := make(map[string]struct{}, len(keys))
		for _, k := range keys {
			distinct[string(k)] = struct{}{}
			if !f.Contains(k) {
				t.Fatalf("false negative for %x among %d keys", k, len(keys))
			}
		}
		// Two keys can share a 64-bit hash and then count once, so Len is at most
		// the number of distinct keys, and zero only for no keys.
		if f.Len() > len(distinct) || (f.Len() == 0) != (len(keys) == 0) {
			t.Fatalf("Len = %d for %d keys, %d distinct", f.Len(), len(keys), len(distinct))
		}
	})
}

// TestNoFalseNegativesInLargeSets is the same property over sets that rapid's own
// size choices would rarely reach: the count and the contents come from a drawn
// seed, up to 5,000 keys with a long run of shared leading bytes.
func TestNoFalseNegativesInLargeSets(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		n := rapid.IntRange(0, 5000).Draw(t, "n")
		rng := rand.New(rand.NewPCG(rapid.Uint64().Draw(t, "seed"), 1))
		keys := make([][]byte, n)
		for i := range keys {
			k := make([]byte, 20)
			for j := range k {
				if j < 12 {
					k[j] = byte(j) // shared leading bytes
				} else {
					k[j] = byte(rng.UintN(256))
				}
			}
			keys[i] = k[:20-rng.IntN(3)] // mostly 20 bytes, a few shorter
		}
		f, err := Build(slices.Values(keys))
		if err != nil {
			t.Fatalf("Build of %d keys: %v", n, err)
		}
		for _, k := range keys {
			if !f.Contains(k) {
				t.Fatalf("false negative for %x among %d keys", k, n)
			}
		}
	})
}
