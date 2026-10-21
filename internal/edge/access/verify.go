package access

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"
)

// maxTokenSize bounds an assertion; Cloudflare's are about a kilobyte.
const maxTokenSize = 16 << 10

// Identity is who a verified assertion names.
type Identity struct {
	// Email is the email of the person, empty for a service token.
	Email string
	// CommonName is the common name of a service token, empty for a person.
	CommonName string
	// Subject is the assertion's sub claim, which is empty for a service token.
	Subject string
}

// Name is the identity for a log line: the email, else the common name, else
// the subject. A verified Identity has at least one. It is text from a signed assertion and is
// to be written as a JSON string or quoted, not interpolated.
func (i Identity) Name() string {
	switch {
	case i.Email != "":
		return i.Email
	case i.CommonName != "":
		return i.CommonName
	}
	return i.Subject
}

// Verifier checks Access assertions for one team and one application. It is
// safe for concurrent use.
type Verifier struct {
	s    settings
	keys *keyHolder
}

// New returns a Verifier for the configured team and audience. It makes no
// request: the keys are fetched when the first assertion needs them, or by
// [Verifier.Refresh].
func New(c Config) (*Verifier, error) {
	s, err := resolve(c)
	if err != nil {
		return nil, err
	}
	return &Verifier{s: s, keys: &keyHolder{s: s}}, nil
}

// Issuer is the iss an assertion must carry.
func (v *Verifier) Issuer() string { return v.s.issuer() }

// CertsURL is where the team's keys are fetched from.
func (v *Verifier) CertsURL() string { return v.s.certsURL() }

// Refresh fetches the team's keys now, whatever the time since the last
// attempt. A caller that wants to know at start-up whether the keys can be
// reached uses it; a failure leaves the Verifier usable, and it fetches again
// when a request needs it.
func (v *Verifier) Refresh(ctx context.Context) error { return v.keys.refresh(ctx, true) }

// VerifyRequest verifies the assertion in the request's Cf-Access-Jwt-Assertion
// header. The request's cookies and query are not read.
func (v *Verifier) VerifyRequest(r *http.Request) (Identity, error) {
	vals := r.Header.Values(HeaderName)
	switch len(vals) {
	case 0:
		return Identity{}, refuse(ReasonMissing, "")
	case 1:
		return v.Verify(r.Context(), vals[0])
	}
	return Identity{}, refuse(ReasonMalformed, "the assertion header is repeated")
}

// object reads a JSON object into its members. Members are looked up by their
// exact names: encoding/json would also match "ALG" to alg, and a verifier
// that reads claims should not.
func object(b []byte) (map[string]json.RawMessage, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(b, &m); err != nil || m == nil {
		return nil, errors.New("not a JSON object")
	}
	return m, nil
}

// member decodes the member name into dst; an absent member leaves dst alone,
// and a null one decodes to nothing.
func member(m map[string]json.RawMessage, name string, dst any) error {
	raw, ok := m[name]
	if !ok {
		return nil
	}
	return json.Unmarshal(raw, dst)
}

type header struct {
	alg, kid string
	crit     bool
}

func parseHeader(b []byte) (header, error) {
	m, err := object(b)
	if err != nil {
		return header{}, err
	}
	var h header
	if err := member(m, "alg", &h.alg); err != nil {
		return header{}, err
	}
	if err := member(m, "kid", &h.kid); err != nil {
		return header{}, err
	}
	_, h.crit = m["crit"]
	return h, nil
}

type claims struct {
	iss, sub, email, commonName string
	aud                         audience
	exp, nbf                    *numericDate
}

func parseClaims(b []byte) (claims, error) {
	m, err := object(b)
	if err != nil {
		return claims{}, err
	}
	var c claims
	for _, f := range []struct {
		name string
		dst  any
	}{
		{"iss", &c.iss},
		{"sub", &c.sub},
		{"email", &c.email},
		{"common_name", &c.commonName},
		{"aud", &c.aud},
		{"exp", &c.exp},
		{"nbf", &c.nbf},
	} {
		if err := member(m, f.name, f.dst); err != nil {
			return claims{}, err
		}
	}
	return c, nil
}

// audience is the aud claim: one string or a list of strings.
type audience []string

func (a *audience) UnmarshalJSON(b []byte) error {
	var one string
	if err := json.Unmarshal(b, &one); err == nil {
		*a = audience{one}
		return nil
	}
	var many []string
	if err := json.Unmarshal(b, &many); err != nil {
		return err
	}
	*a = many
	return nil
}

// numericDate is seconds since the epoch, possibly fractional. It is kept as a
// float so that no value, however large, overflows a time.
type numericDate float64

func (d *numericDate) UnmarshalJSON(b []byte) error {
	var f float64
	if err := json.Unmarshal(b, &f); err != nil {
		return err
	}
	*d = numericDate(f)
	return nil
}

// Verify checks one assertion, the value of the header, and returns the
// identity it names.
func (v *Verifier) Verify(ctx context.Context, token string) (Identity, error) {
	if token == "" {
		return Identity{}, refuse(ReasonMissing, "")
	}
	if len(token) > maxTokenSize {
		return Identity{}, refuse(ReasonMalformed, "the assertion is too large")
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return Identity{}, refuse(ReasonMalformed, "the assertion does not have three parts")
	}
	enc := base64.RawURLEncoding.Strict()
	hb, err1 := enc.DecodeString(parts[0])
	pb, err2 := enc.DecodeString(parts[1])
	sig, err3 := enc.DecodeString(parts[2])
	if err1 != nil || err2 != nil || err3 != nil {
		return Identity{}, refuse(ReasonMalformed, "a part of the assertion is not base64url")
	}
	h, err := parseHeader(hb)
	if err != nil {
		return Identity{}, refuse(ReasonMalformed, "the header is not a JSON object with an alg and a kid of the types read")
	}

	// The algorithm is the verifier's, never the token's: it is checked before
	// any key is looked up, so "none" and the HMAC family never get further.
	if h.alg != "RS256" {
		return Identity{}, refuse(ReasonAlgorithm, "the algorithm is not RS256")
	}
	if h.crit {
		return Identity{}, refuse(ReasonAlgorithm, "the header has a crit member")
	}
	if h.kid == "" {
		return Identity{}, refuse(ReasonKey, "the header has no key id")
	}
	pk, err := v.keys.get(ctx, h.kid)
	if err != nil {
		return Identity{}, err
	}
	signed := token[:len(parts[0])+1+len(parts[1])]
	digest := sha256.Sum256([]byte(signed))
	if err := rsa.VerifyPKCS1v15(pk, crypto.SHA256, digest[:], sig); err != nil {
		return Identity{}, refuse(ReasonSignature, "")
	}

	// The signature holds; only now is the payload read.
	c, err := parseClaims(pb)
	if err != nil {
		return Identity{}, refuse(ReasonClaims, "the payload is not a JSON object with claims of the types read")
	}
	if err := v.checkClaims(c); err != nil {
		return Identity{}, err
	}
	if c.email == "" && c.commonName == "" && c.sub == "" {
		return Identity{}, refuse(ReasonIdentity, "the assertion names no email, common name or subject")
	}
	return Identity{Email: c.email, CommonName: c.commonName, Subject: c.sub}, nil
}

func (v *Verifier) checkClaims(c claims) error {
	if c.iss != v.s.issuer() {
		return refuse(ReasonIssuer, "")
	}
	ok := false
	for _, a := range c.aud {
		if a == v.s.audience {
			ok = true
		}
	}
	if !ok {
		return refuse(ReasonAudience, "")
	}
	now := seconds(v.s.now())
	skew := v.s.skew.Seconds()
	if c.exp == nil || now >= float64(*c.exp)+skew {
		return refuse(ReasonExpired, "")
	}
	if c.nbf != nil && now+skew < float64(*c.nbf) {
		return refuse(ReasonNotYetValid, "")
	}
	return nil
}

func seconds(t time.Time) float64 { return float64(t.UnixNano()) / 1e9 }
