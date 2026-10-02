// Package identity turns a set of identifying attributes into a stable,
// collision-checked fingerprint.
//
// A [Resolver] is built from a [catalog.Catalog]. [Resolver.Resolve] takes an
// entity type and attributes from a producer, converts every value to the
// kind the catalog declares for its key, rejects anything ambiguous, and
// returns an [Identity]: the fingerprint, which is the handle consumers hold,
// and the canonical bytes, which are the full identity the store keeps and
// compares on every fingerprint hit. [Registry] performs that comparison in
// memory. Everything here is a pure function of its inputs: no storage, clock
// or I/O. Resolvers and identities are immutable and safe for concurrent use.
//
// # Rules
//
// Matching is exact. There is no tolerance for "differs by one value": a
// different identity is a different entity. Values are never trimmed,
// case-folded or Unicode-normalized.
//
// Conversion is per key kind and strict, because it handles untrusted input:
//
//   - A string key accepts only a Go string, valid UTF-8, at most
//     [MaxValueLen] bytes. An integer given for a string key is an error.
//   - An int key accepts any Go integer type that fits in int64, or a
//     canonical decimal string: "0", or an optional "-" then digits with no
//     leading zero. No "+", spaces, hex or leading zeros. (OTLP/JSON carries
//     64-bit integers as decimal strings.)
//   - A time key accepts a [time.Time] or a string that [time.Parse] accepts
//     as RFC 3339 with the layout [time.RFC3339Nano]. The zero time is
//     rejected as unset. The value is the UTC instant, never rounded.
//   - Floating-point values, booleans, nil and composite values are rejected.
//
// A required key that is missing, empty or whitespace-only is an error. An
// optional key given as the empty string is absent, because OpenTelemetry
// treats a zero-length service.namespace as an unspecified one; a
// whitespace-only optional value is an error. A resolver is strict by
// default: an attribute that is not an identifying key of the entity type is
// an error. [WithLenient] ignores such attributes, for callers that pass a
// whole resource. The same registered key given twice is always an error.
//
// # Encoding, version 1
//
// The fingerprint is a hash of a canonical byte string, the preimage, also
// returned by [Identity.Canonical]. Integers are big-endian.
//
//	preimage = "toposhift/identity" version type count entry*
//	version  = 0x01
//	type     = u32 length, then the entity type name bytes
//	count    = u32, the number of entries
//	entry    = name tag payload
//	name     = u32 length, then the attribute name bytes
//	tag      = 0x01 string | 0x02 int | 0x03 time
//	payload  = string: u32 length, then the UTF-8 bytes
//	           int:    8 bytes, two's complement
//	           time:   8 bytes of Unix seconds (two's complement), then
//	                   4 bytes of nanoseconds, 0 to 999999999
//
// Entries are sorted by name, bytewise, and an optional key that is absent
// has no entry. Every variable-length part carries a fixed-width length, so
// no choice of values can make one identity's bytes read as another's, and
// every identity has exactly one encoding: [Resolver.Parse] rejects anything
// else with [ErrNonCanonical]. The entity type is part of the preimage, so
// equal attributes on different types differ. The OpenTelemetry schema
// version is not part of it; the ingest translation layer converts every
// producer to one schema version first.
//
// The fingerprint is the entity type, a colon, and the first
// [FingerprintBytes] bytes of the SHA-256 of the preimage in lowercase hex.
// Equal fingerprints are not proof of equal identity, only equal canonical
// bytes are, which is why the full identity is kept.
//
// The encoding is a stored format: changing it changes every fingerprint and
// orphans data keyed by them. testdata/golden_v1.json freezes it. A change
// needs a new version byte, new vectors and a migration, never an edit.
//
// # Producer contract
//
// Two producers that compute the same attribute differently get different
// fingerprints for one real entity. process.creation.time is the practical
// case: a producer that derives it from 10 ms scheduler ticks and one that
// reports nanoseconds disagree. Exact matching is deliberate, and the cure
// is for producers to agree on the value, not for this package to guess.
package identity
