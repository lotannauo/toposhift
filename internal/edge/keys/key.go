package keys

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"strings"
)

// Scope is what a key may do.
type Scope string

const (
	// ScopeRead reads the served surfaces.
	ScopeRead Scope = "read"
	// ScopeIngest writes records. No listener mounts it yet; the scope exists
	// so that keys of it can be issued ahead of the listener that admits them.
	ScopeIngest Scope = "ingest"
)

// ParseScope reads a scope name.
func ParseScope(s string) (Scope, error) {
	switch Scope(s) {
	case ScopeRead, ScopeIngest:
		return Scope(s), nil
	}
	return "", fmt.Errorf("keys: unknown scope %q, want read or ingest", s)
}

func (s Scope) letter() (byte, bool) {
	switch s {
	case ScopeRead:
		return 'r', true
	case ScopeIngest:
		return 'i', true
	}
	return 0, false
}

func scopeOf(letter byte) (Scope, bool) {
	switch letter {
	case 'r':
		return ScopeRead, true
	case 'i':
		return ScopeIngest, true
	}
	return "", false
}

const (
	prefix     = "tps_"
	entropyLen = 32
	bodyLen    = 43
	checkLen   = 6
	// textLen is the length of a key: "tps_", the scope letter, "_", the body
	// and the check.
	textLen = len(prefix) + 2 + bodyLen + checkLen
	// checked is how much of a key the check covers.
	checked  = textLen - checkLen
	alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
)

var (
	// ErrMalformed: the text is not a key: wrong length, prefix, scope letter,
	// characters or check. No file was read to decide it.
	ErrMalformed = errors.New("keys: not a key")
	// ErrUnknown: a well formed key that no stored key matches (never issued,
	// or revoked).
	ErrUnknown = errors.New("keys: unknown key")
	// ErrExpired: the key matches and is past its expiry.
	ErrExpired = errors.New("keys: key expired")
	// ErrScope: the key matches and is for another scope.
	ErrScope = errors.New("keys: key is for another scope")
	// ErrUnavailable: the file of keys cannot be trusted, so no key is.
	ErrUnavailable = errors.New("keys: the key file is unavailable")
	// ErrNotFound: no key has the id.
	ErrNotFound = errors.New("keys: no key with that id")
	// ErrFull: the file already holds the most keys it may.
	ErrFull = errors.New("keys: too many keys")
	// ErrLocked: the lock on the file could not be taken in time.
	ErrLocked = errors.New("keys: the key file is locked by another process")
)

// Secret is the text of a key. It prints as a placeholder however it is
// formatted, so that a log line or an error that takes it by mistake does not
// leak it; [Secret.Reveal] is the one way to the text.
type Secret string

const redacted = "tps_[redacted]"

// Reveal returns the key.
func (s Secret) Reveal() string { return string(s) }

// String returns a placeholder.
func (Secret) String() string { return redacted }

// GoString returns a placeholder, for %#v.
func (Secret) GoString() string { return redacted }

// LogValue returns a placeholder, for log/slog.
func (Secret) LogValue() slog.Value { return slog.StringValue(redacted) }

// MarshalText returns a placeholder, so that encoding/json and the other
// encoders that use it write no key.
func (Secret) MarshalText() ([]byte, error) { return []byte(redacted), nil }

// newSecret makes a key of the scope from 32 bytes read from r.
func newSecret(scope Scope, r io.Reader) (Secret, error) {
	letter, ok := scope.letter()
	if !ok {
		return "", fmt.Errorf("keys: unknown scope %q", scope)
	}
	var raw [entropyLen]byte
	if _, err := io.ReadFull(r, raw[:]); err != nil {
		return "", fmt.Errorf("keys: reading random bytes: %w", err)
	}
	text := prefix + string(letter) + "_" + base62(new(big.Int).SetBytes(raw[:]), bodyLen)
	return Secret(text + base62(new(big.Int).SetUint64(uint64(crc32.ChecksumIEEE([]byte(text)))), checkLen)), nil
}

// base62 writes n in base 62, most significant digit first, padded with the
// first digit to width. n must fit.
func base62(n *big.Int, width int) string {
	out := make([]byte, width)
	for i := range out {
		out[i] = alphabet[0]
	}
	n = new(big.Int).Set(n)
	m := big.NewInt(62)
	var rem big.Int
	for i := width - 1; i >= 0 && n.Sign() > 0; i-- {
		n.DivMod(n, m, &rem)
		out[i] = alphabet[rem.Int64()]
	}
	if n.Sign() > 0 {
		panic("keys: value does not fit the width")
	}
	return string(out)
}

// Parse checks the form of a key and returns its scope. It reads no file. It
// refuses text of the wrong length, a wrong prefix or scope letter, characters
// outside the base62 alphabet, and a check that is not the CRC-32 of the rest:
// the last catches every single mistyped character and every swap of
// neighbouring characters.
func Parse(text string) (Scope, error) {
	if len(text) != textLen || !strings.HasPrefix(text, prefix) || text[len(prefix)+1] != '_' {
		return "", ErrMalformed
	}
	scope, ok := scopeOf(text[len(prefix)])
	if !ok {
		return "", ErrMalformed
	}
	var sum uint64
	for i := len(prefix) + 2; i < textLen; i++ {
		d := strings.IndexByte(alphabet, text[i])
		if d < 0 {
			return "", ErrMalformed
		}
		if i >= checked {
			sum = sum*62 + uint64(d)
		}
	}
	if sum != uint64(crc32.ChecksumIEEE([]byte(text[:checked]))) {
		return "", ErrMalformed
	}
	return scope, nil
}

// hashOf is the SHA-256 of the key's text.
func hashOf(text string) [sha256.Size]byte { return sha256.Sum256([]byte(text)) }

func hexOf(h [sha256.Size]byte) string { return hex.EncodeToString(h[:]) }

// BearerToken returns the key in an Authorization header that uses the Bearer
// scheme: one header, the scheme in any case, one space, then the key with no
// space or control character in it. It reads no other header and no query.
func BearerToken(h http.Header) (string, bool) {
	vals := h.Values("Authorization")
	if len(vals) != 1 {
		return "", false
	}
	scheme, tok, found := strings.Cut(vals[0], " ")
	if !found || !strings.EqualFold(scheme, "Bearer") || tok == "" {
		return "", false
	}
	for i := 0; i < len(tok); i++ {
		if tok[i] <= ' ' || tok[i] >= 0x7f {
			return "", false
		}
	}
	return tok, true
}
