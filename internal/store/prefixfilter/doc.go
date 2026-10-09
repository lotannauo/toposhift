// Package prefixfilter is a static, approximate set of byte strings: an xor
// filter with 8-bit fingerprints (Graf and Lemire, "Xor Filters: Faster and
// Smaller Than Bloom and Cuckoo Filters", 2020).
//
// # What it is for
//
// A store that keeps something for every key prefix (one entity, one layer, one
// direction) has to ask, before a write, whether a prefix is already known. At
// tens of millions of prefixes an exact in-memory set takes gigabytes. A
// [Filter] answers the same question in about 9.84 bits per key (about 20 MB at
// 16.5 million keys), in three memory reads, and is rebuilt from the prefixes a
// retention pass visits. It is never updated in place: prefixes created after
// the build belong in a separate exact set that the caller checks as well.
//
// # What a wrong answer costs
//
// [Filter.Contains] answers "maybe present" or "certainly absent". A "certainly
// absent" lets the caller skip a read. A "maybe present" costs one read, which
// may find nothing; that happens for about one key in 256 (0.39 percent) that
// was not built in, and costs time only.
//
// The filter must never answer "absent" for a key it was built from. The caller
// treats such a key as having no stored state and writes as if it were new, which
// loses what the store holds for it. This holds by construction: [Build] assigns
// the fingerprint cells so that the three cells of every built-in key xor to the
// key's fingerprint, or it fails with an error and returns no filter. The tests
// check it on random sets.
//
// # Keys and hashes
//
// A key is reduced to a 64-bit hash (FNV-1a followed by the murmur3
// finalizer), and everything after that works on the hash. Two different keys
// with the same 64-bit hash are one key to the filter: [Filter.Len] counts
// them once, and [Filter.Contains] is true for both. With 16.5 million keys the
// expected number of such pairs is about 7e-6. Build keeps no reference to the
// caller's key slices.
//
// # Construction
//
// The array holds 3*blockLength fingerprints, with blockLength a third of
// 32 + ceil(1.23*n). Each hash picks one cell in each third. Build peels the
// keys: a cell touched by one key names it, the key is set aside and removed
// from its other two cells, and so on. If every key is set aside, the
// fingerprints are assigned in reverse order of peeling, so that each key's
// peeled cell is written after, and from, its other two. If peeling stalls, Build
// tries the next seed from a fixed sequence. The seed sequence and the sorted
// order of the hashes make Build deterministic: the same set of keys gives the
// same filter whatever the order the keys arrive in.
//
// A [Filter] is immutable once built and safe for concurrent use.
package prefixfilter
