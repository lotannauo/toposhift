package prefixfilter

import "math/bits"

const (
	fnvOffset64 = 14695981039346656037
	fnvPrime64  = 1099511628211
)

// fnv1a is the 64-bit FNV-1a hash of key. It gives the same value as hash/fnv's
// New64a, without allocating a hasher per key.
func fnv1a(key []byte) uint64 {
	h := uint64(fnvOffset64)
	for _, b := range key {
		h ^= uint64(b)
		h *= fnvPrime64
	}
	return h
}

// mix64 is the murmur3 64-bit finalizer. It is a bijection, so distinct inputs
// stay distinct.
func mix64(h uint64) uint64 {
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb33fe1a85ec2
	h ^= h >> 33
	return h
}

// keyHash is the seed-independent 64-bit hash of a key. Keys with the same
// keyHash are one key to the filter.
func keyHash(key []byte) uint64 {
	return mix64(fnv1a(key))
}

// seeded is the hash the cells and the fingerprint are taken from. Because mix64
// and the addition are bijections, distinct key hashes stay distinct under any
// one seed.
func seeded(h, seed uint64) uint64 {
	return mix64(h + seed)
}

// reduce maps the low 32 bits of v onto [0, n) without a division.
func reduce(v uint64, n uint32) uint32 {
	return uint32((uint64(uint32(v)) * uint64(n)) >> 32)
}

// positions are the three cells of a seeded hash, one in each third of the array.
func positions(x uint64, blockLength uint32) (h0, h1, h2 uint32) {
	h0 = reduce(x, blockLength)
	h1 = reduce(bits.RotateLeft64(x, 21), blockLength) + blockLength
	h2 = reduce(bits.RotateLeft64(x, 42), blockLength) + 2*blockLength
	return h0, h1, h2
}

// fingerprint is the 8 bits a seeded hash must xor to over its three cells.
func fingerprint(x uint64) uint8 {
	return uint8(x ^ (x >> 32))
}

// splitmix64 is the step that takes one seed to the next.
func splitmix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	z := x
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}
