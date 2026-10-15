// Package wire is the binary format of a temporal subgraph, and the codec that
// writes and reads it. Encode and Decode are hand-written and pure: no store, no
// clock, no compression. A result is compressed once, when an immutable one is
// stored, and the codec's bytes are what is compressed.
//
// A [Document] holds a [subgraph.Subgraph] from package subgraph, the [Query] it
// answers echoed back, and the [Policy] vector it was derived under. Everything
// in it is derived, and the header says so once.
//
// # Layout
//
// A document is the four bytes "TSGF", the format's major and minor version as
// one byte each, and sections until the end. A section is a tag byte, its
// payload length as a uvarint, and the payload. Tags 1 to 12 are required, are
// written once each in ascending order, and every tag is greater than the one
// before it. A tag of 0x80 or above is optional: a decoder that does not know
// it skips it, which is how a later minor version adds a section. A new major
// version is refused. A required tag a decoder does not know is refused, and so
// is tag 0x20, which is reserved for live mode's rebase message. A section's
// payload is read to its last byte: a minor version never adds to a section.
//
//	1  header        basis, token, From, To, the policy vector, the query echo
//	2  dictionary    the entities, in columns
//	3  elements      entity, edge and derived elements, in columns
//	4  keyframes     the keyframes
//	5  deltas        the invertible deltas, in columns
//	6  quarantine    quarantined entities and uncascaded edges
//	7  truncated     what the query's caps left out
//	8  unattributed  placements, mappings and claims it refused to guess
//	9  unmapped      pods with no instance mapped singly to a service
//	10 suppressed    entries shown and not counted
//	11 pods          what each pod-level search of an extraction reported
//	12 anchor        the anchor state and the ubiquity judgment
//
// # Numbers
//
// A uvarint is the shortest LEB128 form: a decoder refuses a longer one. The
// token, From and To, and every absolute time, are eight bytes, big-endian, of
// an int64 (a token, a uint64). A relative time is a uvarint count of
// nanoseconds after From. The format does not cap the window: a relative time
// passes 2^53 at about 104 days, and a reader that holds numbers as floats
// reads the token, From and To as 64-bit integers, and the relative times as
// them when the window, To less From, is 2^53 nanoseconds or more. A signed
// option value is a zigzag uvarint. A flag column is one bit per item, the first
// item in the lowest bit of the first byte, padded with zero bits.
//
// # One encoding for one value
//
// Where the order of items is fixed, the item is written as its distance from
// the one before, less one if it must be strictly after it, so a list that is
// out of order cannot be written. Pieces and spans are written as the distance
// from the end of the piece before, less one, and a length less one, so
// touching or empty ones cannot be written. A sorted list of ids is written
// first as it is and then as distances. A value a rule determines is not
// written: the rule of an element, and the end reason of a span (unknown at To,
// reach_ended before). A value that is written and that a rule also fixes, an
// element's subject and its class, is checked against what the rule says. A
// producer table is the producers the pieces use, once each: an entry no piece
// uses is refused. So every document has one encoding, and a decoder accepts
// only that one: Encode(Decode(b)) is b for the bytes of this minor version
// that skip no section. A document of a later minor version, or one that has a
// section to skip, decodes and encodes as this minor version without it.
//
// # Bounds
//
// A count is checked against the bytes that remain, at the least that one item
// takes, before anything is allocated for it, so a decode holds a small
// multiple of its input.
//
// # What the codec checks
//
// It checks structure: the order of every list, the range of every number and
// reference, the labels a key determines, and that a class and a transition
// count agree with the pieces. It also checks that the deltas and the keyframes
// say what the pieces say, which is what the format means by them: the deltas
// are exactly the piece boundaries strictly inside the window, in order, each
// with the state on both sides, and every keyframe is the set of elements
// present at its time, replayed from the keyframe at From through the deltas.
// It does not check where keyframes are placed or how many there are: that is
// the builder's rule under the subgraph's policy version, and a stored document
// of another policy version still reads.
//
// # The query echo and the cache key
//
// The options of a [Query] are integers: a duration is a count of nanoseconds, a
// flag is 0 or 1. A section carries no field a later minor version adds, so an
// option of another type comes as an optional section of a later minor version,
// which a decoder of this one skips, or with a new major version. The cache key
// of a result is a function of the [Query] value with its AsOf left out, which
// the resolved token in the subgraph replaces, and not of the header's bytes.
package wire
