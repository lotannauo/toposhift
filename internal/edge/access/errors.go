package access

import (
	"errors"
	"fmt"
)

// ErrUnauthenticated is matched, with errors.Is, by every error a Verifier
// returns for a request it does not admit.
var ErrUnauthenticated = errors.New("access: no verified identity")

// Reason says which check refused an assertion. It is for the log: a caller
// shows a requester the same refusal whatever the reason.
type Reason string

// The reasons, in the order the checks run.
const (
	// ReasonMissing: the request had no Cf-Access-Jwt-Assertion header.
	ReasonMissing Reason = "missing"
	// ReasonMalformed: the header was repeated, or the token is not three
	// strict base64url parts of the allowed size with a JSON object header.
	ReasonMalformed Reason = "malformed"
	// ReasonAlgorithm: the header's alg is not exactly RS256, or the header
	// has a crit member.
	ReasonAlgorithm Reason = "algorithm"
	// ReasonNoKeys: the header named a key id and the verifier holds no keys
	// at all, so no assertion can be admitted.
	ReasonNoKeys Reason = "no_keys"
	// ReasonKey: the header has no kid, or names one the team's certificates
	// do not hold.
	ReasonKey Reason = "unknown_key"
	// ReasonSignature: the signature does not verify under the key.
	ReasonSignature Reason = "signature"
	// ReasonClaims: the signed payload is not a JSON object with claims of the
	// types read.
	ReasonClaims Reason = "claims"
	// ReasonIssuer: iss is not the team's issuer.
	ReasonIssuer Reason = "issuer"
	// ReasonAudience: aud does not hold the configured tag.
	ReasonAudience Reason = "audience"
	// ReasonExpired: exp is missing or past.
	ReasonExpired Reason = "expired"
	// ReasonNotYetValid: nbf is in the future.
	ReasonNotYetValid Reason = "not_yet_valid"
	// ReasonIdentity: the assertion verifies but names no email, common name
	// or subject, so there is nobody to admit.
	ReasonIdentity Reason = "no_identity"
)

// Error is the refusal of an assertion. Its text names the check and never
// holds any part of the token.
type Error struct {
	Reason Reason
	// Detail is a fixed sentence about the check, or the failure of a key
	// fetch; it holds nothing from the token.
	Detail string
}

func (e *Error) Error() string {
	if e.Detail == "" {
		return fmt.Sprintf("access: assertion refused (%s)", e.Reason)
	}
	return fmt.Sprintf("access: assertion refused (%s): %s", e.Reason, e.Detail)
}

// Is reports that every Error is ErrUnauthenticated.
func (e *Error) Is(target error) bool { return target == ErrUnauthenticated }

// ReasonOf returns the reason an error from a Verifier carries, or "" for an
// error that is not one of its refusals.
func ReasonOf(err error) Reason {
	var e *Error
	if errors.As(err, &e) {
		return e.Reason
	}
	return ""
}

func refuse(r Reason, detail string) error { return &Error{Reason: r, Detail: detail} }
