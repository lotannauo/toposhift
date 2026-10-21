package pgmodel

import (
	"fmt"
	"slices"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
)

// EncodeFingerprint is the bytea a fingerprint is stored as: its type name, a zero
// byte, then its 16 hash bytes. Comparing two encodings with bytes.Compare gives
// engine.CompareFingerprints: the type decides first, and a type that is a prefix of
// another sorts before it, because the zero byte is below every byte a type name
// holds; and the hashes follow, so a database's ORDER BY on the column is the
// contract's order with no collation.
func EncodeFingerprint(fp identity.Fingerprint) []byte {
	typ, hash := string(fp.Type()), fp.Hash()
	b := make([]byte, 0, len(typ)+1+len(hash))
	b = append(b, typ...)
	b = append(b, 0)
	return append(b, hash[:]...)
}

// DecodeFingerprint is the inverse of [EncodeFingerprint].
func DecodeFingerprint(b []byte) (identity.Fingerprint, error) {
	n := len(b) - identity.FingerprintBytes - 1
	if n < 1 || b[n] != 0 {
		return identity.Fingerprint{}, fmt.Errorf("pgmodel: %d bytes are not an encoded fingerprint", len(b))
	}
	return identity.FingerprintFromHash(catalog.EntityType(b[:n]), [identity.FingerprintBytes]byte(b[n+1:]))
}

// CompareEncoded orders two encoded fingerprints the way bytes.Compare does, which
// is the order of engine.CompareFingerprints.
func CompareEncoded(a, b []byte) int { return slices.Compare(a, b) }
