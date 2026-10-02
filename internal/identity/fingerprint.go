package identity

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/lotannauo/toposhift/internal/catalog"
)

// FingerprintBytes is how many bytes of the SHA-256 digest a fingerprint
// keeps. 128 bits leaves 64 bits of collision resistance, which is why the
// full identity is stored and compared on every hit: a collision is then a
// loud error, never a silent merge. The width is reversible, because handles
// can be recomputed from the stored canonical identities.
const FingerprintBytes = 16

// hashFunc maps a preimage to the bytes a fingerprint keeps. It is a field of
// the resolver only so tests can force collisions.
type hashFunc func(preimage []byte) [FingerprintBytes]byte

func sha256Prefix(preimage []byte) [FingerprintBytes]byte {
	sum := sha256.Sum256(preimage)
	return [FingerprintBytes]byte(sum[:FingerprintBytes])
}

// Fingerprint is the handle for an entity: its type and a hash of its
// canonical identity. It is comparable, so it can be a map key. The zero
// value is invalid.
type Fingerprint struct {
	typ catalog.EntityType
	sum [FingerprintBytes]byte
}

// Type returns the entity type.
func (f Fingerprint) Type() catalog.EntityType { return f.typ }

// IsZero reports whether f is the zero, invalid fingerprint.
func (f Fingerprint) IsZero() bool { return f == Fingerprint{} }

// String returns the type, a colon and the hash in lowercase hex. The zero
// fingerprint returns the empty string.
func (f Fingerprint) String() string {
	if f.IsZero() {
		return ""
	}
	var b strings.Builder
	b.Grow(len(f.typ) + 1 + 2*FingerprintBytes)
	b.WriteString(string(f.typ))
	b.WriteByte(':')
	b.WriteString(hex.EncodeToString(f.sum[:]))
	return b.String()
}

// MarshalText implements [encoding.TextMarshaler]. It fails for the zero
// fingerprint, so an unset value cannot be written out unnoticed.
func (f Fingerprint) MarshalText() ([]byte, error) {
	if f.IsZero() {
		return nil, fmt.Errorf("%w: zero fingerprint", ErrFingerprint)
	}
	return []byte(f.String()), nil
}

// UnmarshalText implements [encoding.TextUnmarshaler] using
// [ParseFingerprint].
func (f *Fingerprint) UnmarshalText(text []byte) error {
	parsed, err := ParseFingerprint(string(text))
	if err != nil {
		return err
	}
	*f = parsed
	return nil
}

// ParseFingerprint parses the form [Fingerprint.String] produces. It is
// strict: a valid type name, a colon, and exactly 32 lowercase hex digits.
// It does not need a catalog, so it does not check that the type exists.
func ParseFingerprint(s string) (Fingerprint, error) {
	typ, hash, ok := strings.Cut(s, ":")
	if !ok || !catalog.EntityType(typ).Valid() {
		return Fingerprint{}, fmt.Errorf("%w: %q is not type:hash", ErrFingerprint, s)
	}
	if len(hash) != 2*FingerprintBytes || strings.ToLower(hash) != hash {
		return Fingerprint{}, fmt.Errorf("%w: hash in %q is not %d lowercase hex digits", ErrFingerprint, s, 2*FingerprintBytes)
	}
	var f Fingerprint
	if _, err := hex.Decode(f.sum[:], []byte(hash)); err != nil {
		return Fingerprint{}, fmt.Errorf("%w: hash in %q is not hex", ErrFingerprint, s)
	}
	f.typ = catalog.EntityType(typ)
	return f, nil
}
