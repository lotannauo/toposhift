package prefixfilter

import (
	"bytes"
	"encoding/binary"
	"hash/fnv"
	"iter"
	"math/rand/v2"
	"slices"
	"testing"
)

// testKeys returns n distinct 20-byte keys, the size of a stored prefix. The
// first 12 bytes come from a source seeded with seed; the last 8 are the index
// plus first, so the keys are distinct by construction and keys made with
// different first values never collide.
func testKeys(seed, first uint64, n int) [][]byte {
	rng := rand.New(rand.NewPCG(seed, seed^0x5851f42d4c957f2d))
	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = make([]byte, 20)
		binary.LittleEndian.PutUint64(keys[i][0:8], rng.Uint64())
		binary.LittleEndian.PutUint32(keys[i][8:12], rng.Uint32())
		binary.BigEndian.PutUint64(keys[i][12:20], first+uint64(i))
	}
	return keys
}

func seq(keys [][]byte) iter.Seq[[]byte] {
	return slices.Values(keys)
}

func mustBuild(t testing.TB, keys [][]byte) *Filter {
	t.Helper()
	f, err := Build(seq(keys))
	if err != nil {
		t.Fatalf("Build of %d keys: %v", len(keys), err)
	}
	return f
}

func TestEmpty(t *testing.T) {
	f := mustBuild(t, nil)
	if f.Len() != 0 {
		t.Errorf("Len = %d, want 0", f.Len())
	}
	// Some of these would match an all-zero array, so only an explicit empty
	// case keeps them out.
	for _, key := range append(testKeys(1, 0, 5000), nil, []byte{}, []byte{0}) {
		if f.Contains(key) {
			t.Fatalf("empty filter contains %x", key)
		}
	}
}

func TestOneKey(t *testing.T) {
	key := []byte("entity/layer/direction")
	f := mustBuild(t, [][]byte{key})
	if f.Len() != 1 {
		t.Errorf("Len = %d, want 1", f.Len())
	}
	if !f.Contains(key) {
		t.Error("filter does not contain its only key")
	}
}

func TestEmptyKeyIsAKey(t *testing.T) {
	f := mustBuild(t, [][]byte{nil, []byte("a")})
	if f.Len() != 2 {
		t.Errorf("Len = %d, want 2", f.Len())
	}
	if !f.Contains(nil) || !f.Contains([]byte{}) {
		t.Error("filter does not contain the empty key")
	}
}

func TestDuplicatesCountOnce(t *testing.T) {
	distinct := testKeys(2, 0, 1000)
	var keys [][]byte
	for round := range 3 {
		for _, k := range distinct {
			keys = append(keys, slices.Clone(k)) // equal bytes, separate slices
		}
		keys = append(keys, distinct[round]) // and an immediate repeat
	}
	f := mustBuild(t, keys)
	if f.Len() != len(distinct) {
		t.Errorf("Len = %d, want %d", f.Len(), len(distinct))
	}
	for _, k := range distinct {
		if !f.Contains(k) {
			t.Fatalf("filter does not contain %x", k)
		}
	}
	if !slices.Equal(f.fingerprints, mustBuild(t, distinct).fingerprints) {
		t.Error("repeated keys change the filter")
	}
}

func TestDeterministic(t *testing.T) {
	for _, n := range []int{0, 1, 2, 3, 100, 20000} {
		keys := testKeys(3, 0, n)
		want := mustBuild(t, keys)
		rng := rand.New(rand.NewPCG(7, 8))
		for range 3 {
			rng.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
			got := mustBuild(t, keys)
			if got.seed != want.seed || got.blockLength != want.blockLength || got.n != want.n {
				t.Fatalf("n=%d: header differs: got (%#x, %d, %d), want (%#x, %d, %d)", n,
					got.seed, got.blockLength, got.n, want.seed, want.blockLength, want.n)
			}
			if !bytes.Equal(got.fingerprints, want.fingerprints) {
				t.Fatalf("n=%d: fingerprints differ between key orders", n)
			}
		}
	}
}

func TestSizes(t *testing.T) {
	// capacity = 32 + ceil(1.23*n); blockLength = capacity/3.
	tests := []struct {
		n           int
		blockLength uint32
	}{
		{0, 10},         // 32
		{1, 11},         // 34
		{2, 11},         // 35
		{3, 12},         // 36
		{100, 51},       // 155
		{100000, 41010}, // 123032
	}
	for _, tc := range tests {
		keys := testKeys(4, 0, tc.n)
		f := mustBuild(t, keys)
		if f.blockLength != tc.blockLength {
			t.Errorf("n=%d: blockLength = %d, want %d", tc.n, f.blockLength, tc.blockLength)
		}
		if f.SizeBytes() != 3*int(f.blockLength) {
			t.Errorf("n=%d: SizeBytes = %d, want %d", tc.n, f.SizeBytes(), 3*int(f.blockLength))
		}
		if f.Len() != tc.n {
			t.Errorf("n=%d: Len = %d", tc.n, f.Len())
		}
		for _, k := range keys {
			if !f.Contains(k) {
				t.Fatalf("n=%d: filter does not contain %x", tc.n, k)
			}
		}
	}
}

func TestBuildKeepsNoReferenceToKeys(t *testing.T) {
	keys := testKeys(5, 0, 2000)
	buf := make([]byte, 20)
	reused := func(yield func([]byte) bool) {
		for _, k := range keys {
			copy(buf, k)
			if !yield(buf) {
				return
			}
		}
	}
	f, err := Build(reused)
	if err != nil {
		t.Fatal(err)
	}
	clear(buf)
	for _, k := range keys {
		if !f.Contains(k) {
			t.Fatalf("filter does not contain %x", k)
		}
	}
	if !bytes.Equal(f.fingerprints, mustBuild(t, keys).fingerprints) {
		t.Error("a reused key buffer changes the filter")
	}
}

func TestHashMatchesFNV(t *testing.T) {
	for _, key := range append(testKeys(8, 0, 200), nil, []byte{0}, []byte("a"), bytes.Repeat([]byte{0xff}, 100)) {
		h := fnv.New64a()
		h.Write(key)
		if got, want := fnv1a(key), h.Sum64(); got != want {
			t.Fatalf("fnv1a(%x) = %#x, want %#x", key, got, want)
		}
	}
}

func TestMix64(t *testing.T) {
	// Values of the murmur3 64-bit finalizer, worked out separately from the code.
	for in, want := range map[uint64]uint64{
		0:          0,
		1:          0x1bca8d10af824044,
		0xdeadbeef: 0xc69e54196056fa2e,
	} {
		if got := mix64(in); got != want {
			t.Errorf("mix64(%#x) = %#x, want %#x", in, got, want)
		}
	}
}

func TestReduceStaysBelowN(t *testing.T) {
	for _, n := range []uint32{1, 2, 3, 11, 41010, 1 << 31, 1<<32 - 1} {
		for _, v := range []uint64{0, 1, 1<<32 - 1, 1 << 32, 1<<64 - 1, 0x9e3779b97f4a7c15} {
			if got := reduce(v, n); got >= n {
				t.Fatalf("reduce(%#x, %d) = %d, not below n", v, n, got)
			}
		}
	}
	// The product must be taken in 64 bits: the largest input with the largest n.
	if got, want := reduce(1<<32-1, 1<<32-1), uint32(1<<32-2); got != want {
		t.Errorf("reduce(2^32-1, 2^32-1) = %d, want %d", got, want)
	}
}

func TestPositionsLieEachInTheirOwnThird(t *testing.T) {
	rng := rand.New(rand.NewPCG(9, 10))
	for _, blockLength := range []uint32{1, 2, 11, 41010, 5_500_000} {
		for range 1000 {
			h0, h1, h2 := positions(rng.Uint64(), blockLength)
			if h0 >= blockLength ||
				h1 < blockLength || h1 >= 2*blockLength ||
				h2 < 2*blockLength || h2 >= 3*blockLength {
				t.Fatalf("blockLength %d: positions (%d, %d, %d) leave their thirds", blockLength, h0, h1, h2)
			}
		}
	}
}

func TestContainsUsesTheFingerprintBuildAssigned(t *testing.T) {
	// For every key in the set, the three cells xor to the key's fingerprint.
	keys := testKeys(11, 0, 5000)
	f := mustBuild(t, keys)
	for _, k := range keys {
		x := seeded(keyHash(k), f.seed)
		h0, h1, h2 := positions(x, f.blockLength)
		if got, want := f.fingerprints[h0]^f.fingerprints[h1]^f.fingerprints[h2], fingerprint(x); got != want {
			t.Fatalf("cells of %x xor to %#x, want %#x", k, got, want)
		}
	}
}

// sortedHashes is what Build hands to construct.
func sortedHashes(keys [][]byte) []uint64 {
	hashes := make([]uint64, len(keys))
	for i, k := range keys {
		hashes[i] = keyHash(k)
	}
	slices.Sort(hashes)
	return slices.Compact(hashes)
}

func TestConstructRetriesWithTheNextSeed(t *testing.T) {
	// Small sets are the ones most likely to fail to peel with a given seed. The
	// keys are fixed, so which sizes retry is fixed too.
	retried := 0
	for n := 1; n <= 400; n++ {
		keys := testKeys(12, 0, n)
		blockLength, err := blockLengthFor(n)
		if err != nil {
			t.Fatal(err)
		}
		f, attempts, err := construct(sortedHashes(keys), blockLength, maxSeeds)
		if err != nil {
			t.Fatalf("n=%d: %v", n, err)
		}
		if attempts > 1 {
			retried++
			if f.seed == initialSeed {
				t.Fatalf("n=%d: %d attempts but the initial seed was kept", n, attempts)
			}
		}
		for _, k := range keys {
			if !f.Contains(k) {
				t.Fatalf("n=%d: filter built after %d attempts does not contain %x", n, attempts, k)
			}
		}
	}
	if retried == 0 {
		t.Error("no size from 1 to 400 needed a second seed, so the retry path is not covered")
	}
}

func TestConstructFailsWhenNoSeedCanWork(t *testing.T) {
	// 1,000 keys cannot be peeled out of 300 cells under any seed.
	f, attempts, err := construct(sortedHashes(testKeys(13, 0, 1000)), 100, 5)
	if err == nil || f != nil {
		t.Fatalf("construct = (%v, %d, %v), want an error and no filter", f, attempts, err)
	}
	if attempts != 5 {
		t.Errorf("attempts = %d, want 5", attempts)
	}
}

func TestBlockLengthForRejectsWhatCannotBeAddressed(t *testing.T) {
	for _, n := range []int{-1, 1 << 62, 1<<32 + 1<<31} {
		if _, err := blockLengthFor(n); err == nil {
			t.Errorf("blockLengthFor(%d) did not fail", n)
		}
	}
}

func TestFalsePositiveRate(t *testing.T) {
	const (
		built   = 200_000
		queries = 1_000_000
	)
	// Queries are keys numbered after the built ones, so none is in the set.
	keys := testKeys(14, 0, built)
	f := mustBuild(t, keys)
	hits := 0
	for _, k := range testKeys(14, built, queries) {
		if f.Contains(k) {
			hits++
		}
	}
	// Expected queries/256 = 3,906 with a standard deviation near 62; the bounds
	// are about 8 deviations out. The keys and queries are fixed, so the count is
	// the same on every run.
	if hits < 3400 || hits > 4450 {
		t.Errorf("%d false positives in %d queries (%.3f%%), want 3,400 to 4,450 (expected 0.391%%)",
			hits, queries, 100*float64(hits)/queries)
	}
}

func BenchmarkContains(b *testing.B) {
	keys := testKeys(15, 0, 100_000)
	f := mustBuild(b, keys)
	misses := testKeys(15, 100_000, 100_000)
	b.Run("hit", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if !f.Contains(keys[i%len(keys)]) {
				b.Fatal("false negative")
			}
		}
	})
	b.Run("miss", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			f.Contains(misses[i%len(misses)])
		}
	})
}

func BenchmarkBuild100k(b *testing.B) {
	keys := testKeys(16, 0, 100_000)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := Build(seq(keys)); err != nil {
			b.Fatal(err)
		}
	}
}
