package access

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestAValidAssertionNamesItsIdentity(t *testing.T) {
	t.Parallel()
	e := newEnv(t)

	id, err := e.verify(e.token(t, nil))
	if err != nil {
		t.Fatal(err)
	}
	if want := (Identity{Email: "alice@example.com", Subject: "user-1"}); id != want {
		t.Errorf("identity = %+v, want %+v", id, want)
	}
	if id.Name() != "alice@example.com" {
		t.Errorf("Name = %q", id.Name())
	}

	// aud as one string, and a list that holds the tag among others.
	for _, aud := range []any{testAudience, []string{"other", testAudience}} {
		if _, err := e.verify(e.token(t, func(_, p map[string]any) { p["aud"] = aud })); err != nil {
			t.Errorf("aud %v: %v", aud, err)
		}
	}

	// A service token has a common name and no email or subject.
	svc := e.token(t, func(_, p map[string]any) {
		delete(p, "email")
		p["sub"] = ""
		p["common_name"] = "0123.access"
	})
	id, err = e.verify(svc)
	if err != nil {
		t.Fatal(err)
	}
	if want := (Identity{CommonName: "0123.access"}); id != want || id.Name() != "0123.access" {
		t.Errorf("service identity = %+v (%q), want %+v", id, id.Name(), want)
	}

	// A subject alone is an identity.
	subOnly := e.token(t, func(_, p map[string]any) { delete(p, "email") })
	if id, err = e.verify(subOnly); err != nil || id != (Identity{Subject: "user-1"}) || id.Name() != "user-1" {
		t.Errorf("subject only: %+v (%q), %v", id, id.Name(), err)
	}
}

func TestAVerifiedAssertionThatNamesNobodyIsRefused(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	for name, edit := range map[string]func(p map[string]any){
		"all three absent": func(p map[string]any) { delete(p, "email"); delete(p, "sub") },
		"all three empty":  func(p map[string]any) { p["email"], p["sub"], p["common_name"] = "", "", "" },
		"all three null":   func(p map[string]any) { p["email"], p["sub"], p["common_name"] = nil, nil, nil },
	} {
		id, err := e.verify(e.token(t, func(_, p map[string]any) { edit(p) }))
		if ReasonOf(err) != ReasonIdentity || !errors.Is(err, ErrUnauthenticated) || id != (Identity{}) {
			t.Errorf("%s: %+v, %v, want a no_identity refusal", name, id, err)
		}
	}
}

func TestEveryRefusalNamesTheCheckThatMadeIt(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	now := e.clock.Now().Unix()
	_, _, c := testKeys(t)

	tamperedPayload := func() string {
		tok := e.token(t, nil)
		parts := strings.Split(tok, ".")
		p := e.payload()
		p["aud"] = []string{"another-app"}
		p["email"] = "mallory@example.com"
		// the payload of a different assertion under this one's header and signature
		return parts[0] + "." + b64(jsonBytes(t, p)) + "." + parts[2]
	}()
	flipped := func() string {
		tok := e.token(t, nil)
		i := len(tok) - 10
		b := []byte(tok)
		if b[i] == 'A' {
			b[i] = 'B'
		} else {
			b[i] = 'A'
		}
		return string(b)
	}()

	// The same signature written with a non-zero bit in the unused tail of its
	// last character: the same bytes, a different text.
	nonCanonical := func() string {
		tok := e.token(t, nil)
		const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
		v := strings.IndexByte(alphabet, tok[len(tok)-1])
		if v&0x0f != 0 {
			t.Fatalf("the signature's last character %q is not canonical", tok[len(tok)-1])
		}
		return tok[:len(tok)-1] + string(alphabet[v|1])
	}()

	cases := []struct {
		name string
		tok  string
		want Reason
	}{
		{"empty", "", ReasonMissing},
		{"signature with a non-canonical last character", nonCanonical, ReasonMalformed},
		{"one part", "abc", ReasonMalformed},
		{"two parts", "abc.def", ReasonMalformed},
		{"four parts", e.token(t, nil) + ".x", ReasonMalformed},
		{"padded base64", strings.Replace(e.token(t, nil), ".", "=.", 1), ReasonMalformed},
		{"not base64", "!!!.???.***", ReasonMalformed},
		{"header not JSON", b64([]byte("nope")) + "." + b64([]byte("{}")) + ".AAAA", ReasonMalformed},
		{"header is an array", b64([]byte("[]")) + "." + b64([]byte("{}")) + ".AAAA", ReasonMalformed},
		{"header null", b64([]byte("null")) + "." + b64([]byte("{}")) + ".AAAA", ReasonMalformed},
		{"alg is a number", signRS256(t, e.a, signingInput(t, map[string]any{"alg": 256, "kid": "k1"}, e.payload())), ReasonMalformed},
		{"too large", strings.Repeat("a", maxTokenSize+1), ReasonMalformed},

		{"alg none, empty signature", signingInput(t, map[string]any{"alg": "none"}, e.payload()) + ".", ReasonAlgorithm},
		{"alg none with a kid and a real signature part", signRS256(t, e.a, signingInput(t, map[string]any{"alg": "none", "kid": "k1"}, e.payload())), ReasonAlgorithm},
		{"alg None", signRS256(t, e.a, signingInput(t, map[string]any{"alg": "None", "kid": "k1"}, e.payload())), ReasonAlgorithm},
		{"alg rs256 in lower case", signRS256(t, e.a, signingInput(t, map[string]any{"alg": "rs256", "kid": "k1"}, e.payload())), ReasonAlgorithm},
		{"alg RS384", signRS256(t, e.a, signingInput(t, map[string]any{"alg": "RS384", "kid": "k1"}, e.payload())), ReasonAlgorithm},
		{"alg PS256", signRS256(t, e.a, signingInput(t, map[string]any{"alg": "PS256", "kid": "k1"}, e.payload())), ReasonAlgorithm},
		{"alg ES256", signRS256(t, e.a, signingInput(t, map[string]any{"alg": "ES256", "kid": "k1"}, e.payload())), ReasonAlgorithm},
		{"no alg", signRS256(t, e.a, signingInput(t, map[string]any{"kid": "k1"}, e.payload())), ReasonAlgorithm},
		{"alg under another case of the name", signRS256(t, e.a, signingInput(t, map[string]any{"ALG": "RS256", "kid": "k1"}, e.payload())), ReasonAlgorithm},
		{"crit", e.token(t, func(h, _ map[string]any) { h["crit"] = []string{"exp"} }), ReasonAlgorithm},
		{"crit null", e.token(t, func(h, _ map[string]any) { h["crit"] = nil }), ReasonAlgorithm},

		{"no kid", e.token(t, func(h, _ map[string]any) { delete(h, "kid") }), ReasonKey},
		{"empty kid", e.token(t, func(h, _ map[string]any) { h["kid"] = "" }), ReasonKey},
		{"unknown kid", e.token(t, func(h, _ map[string]any) { h["kid"] = "k9" }), ReasonKey},
		{"kid under another case of the name", e.token(t, func(h, _ map[string]any) { delete(h, "kid"); h["KID"] = "k1" }), ReasonKey},

		{"signed by another key under a known kid", signRS256(t, c, signingInput(t, e.header(), e.payload())), ReasonSignature},
		{"signature byte flipped", flipped, ReasonSignature},
		{"payload replaced after signing", tamperedPayload, ReasonSignature},
		{"empty signature under RS256", signingInput(t, e.header(), e.payload()) + ".", ReasonSignature},
		{"signature of 256 zero bytes", signingInput(t, e.header(), e.payload()) + "." + b64(make([]byte, 256)), ReasonSignature},

		{"payload not JSON", signRS256(t, e.a, b64(jsonBytes(t, e.header()))+"."+b64([]byte("nope"))), ReasonClaims},
		{"payload is an array", signRS256(t, e.a, b64(jsonBytes(t, e.header()))+"."+b64([]byte("[]"))), ReasonClaims},
		{"payload has trailing data", signRS256(t, e.a, b64(jsonBytes(t, e.header()))+"."+b64([]byte(`{} {}`))), ReasonClaims},
		{"iss is a number", e.token(t, func(_, p map[string]any) { p["iss"] = 5 }), ReasonClaims},
		{"aud is a number", e.token(t, func(_, p map[string]any) { p["aud"] = 5 }), ReasonClaims},
		{"exp is a string", e.token(t, func(_, p map[string]any) { p["exp"] = "9999999999" }), ReasonClaims},
		{"email is a number", e.token(t, func(_, p map[string]any) { p["email"] = 1 }), ReasonClaims},

		{"wrong iss: another team", e.token(t, func(_, p map[string]any) { p["iss"] = "https://other.cloudflareaccess.com" }), ReasonIssuer},
		{"wrong iss: trailing slash", e.token(t, func(_, p map[string]any) { p["iss"] = "https://" + testHost + "/" }), ReasonIssuer},
		{"wrong iss: http", e.token(t, func(_, p map[string]any) { p["iss"] = "http://" + testHost }), ReasonIssuer},
		{"wrong iss: bare host", e.token(t, func(_, p map[string]any) { p["iss"] = testHost }), ReasonIssuer},
		{"no iss", e.token(t, func(_, p map[string]any) { delete(p, "iss") }), ReasonIssuer},
		{"iss under another case of the name", e.token(t, func(_, p map[string]any) { delete(p, "iss"); p["ISS"] = "https://" + testHost }), ReasonIssuer},

		{"wrong aud: string", e.token(t, func(_, p map[string]any) { p["aud"] = "another-app" }), ReasonAudience},
		{"wrong aud: list without the tag", e.token(t, func(_, p map[string]any) { p["aud"] = []string{"a", "b"} }), ReasonAudience},
		{"wrong aud: empty list", e.token(t, func(_, p map[string]any) { p["aud"] = []string{} }), ReasonAudience},
		{"wrong aud: the tag with a suffix", e.token(t, func(_, p map[string]any) { p["aud"] = testAudience + "0" }), ReasonAudience},
		{"wrong aud: a prefix of the tag", e.token(t, func(_, p map[string]any) { p["aud"] = testAudience[:10] }), ReasonAudience},
		{"wrong aud: the tag in another case", e.token(t, func(_, p map[string]any) { p["aud"] = strings.ToUpper(testAudience) }), ReasonAudience},
		{"no aud", e.token(t, func(_, p map[string]any) { delete(p, "aud") }), ReasonAudience},
		{"aud null", e.token(t, func(_, p map[string]any) { p["aud"] = nil }), ReasonAudience},
		{"aud under another case of the name", e.token(t, func(_, p map[string]any) { delete(p, "aud"); p["AUD"] = []string{testAudience} }), ReasonAudience},

		{"expired an hour ago", e.token(t, func(_, p map[string]any) { p["exp"] = now - 3600 }), ReasonExpired},
		{"expired past the skew", e.token(t, func(_, p map[string]any) { p["exp"] = now - 60 }), ReasonExpired},
		{"exp zero", e.token(t, func(_, p map[string]any) { p["exp"] = 0 }), ReasonExpired},
		{"exp negative", e.token(t, func(_, p map[string]any) { p["exp"] = -1 }), ReasonExpired},
		{"exp null", e.token(t, func(_, p map[string]any) { p["exp"] = nil }), ReasonExpired},
		{"no exp", e.token(t, func(_, p map[string]any) { delete(p, "exp") }), ReasonExpired},

		{"nbf past the skew", e.token(t, func(_, p map[string]any) { p["nbf"] = now + 61 }), ReasonNotYetValid},
		{"nbf an hour away", e.token(t, func(_, p map[string]any) { p["nbf"] = now + 3600 }), ReasonNotYetValid},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			id, err := e.verify(tc.tok)
			if err == nil {
				t.Fatalf("admitted as %+v", id)
			}
			if !errors.Is(err, ErrUnauthenticated) {
				t.Errorf("error %v does not match ErrUnauthenticated", err)
			}
			if got := ReasonOf(err); got != tc.want {
				t.Errorf("reason = %q (%v), want %q", got, err, tc.want)
			}
			if id != (Identity{}) {
				t.Errorf("a refusal returned identity %+v", id)
			}
			if tc.tok != "" && strings.Contains(err.Error(), tc.tok) {
				t.Errorf("the error holds the token: %v", err)
			}
		})
	}
}

func TestTheClockSkewIsSixtySecondsEitherWay(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	now := e.clock.Now().Unix()
	for _, tc := range []struct {
		name string
		edit func(p map[string]any)
		want Reason // "" admits
	}{
		{"exp 59 s past", func(p map[string]any) { p["exp"] = now - 59 }, ""},
		{"exp 60 s past", func(p map[string]any) { p["exp"] = now - 60 }, ReasonExpired},
		{"exp 59.5 s past, fractional", func(p map[string]any) { p["exp"] = float64(now) - 59.5 }, ""},
		{"exp 60.5 s past, fractional", func(p map[string]any) { p["exp"] = float64(now) - 60.5 }, ReasonExpired},
		{"nbf 60 s ahead", func(p map[string]any) { p["nbf"] = now + 60 }, ""},
		{"nbf 61 s ahead", func(p map[string]any) { p["nbf"] = now + 61 }, ReasonNotYetValid},
		{"no nbf", func(p map[string]any) { delete(p, "nbf") }, ""},
		{"exp far in the future", func(p map[string]any) { p["exp"] = 1e300 }, ""},
		{"nbf far in the future", func(p map[string]any) { p["nbf"] = 1e300 }, ReasonNotYetValid},
	} {
		_, err := e.verify(e.token(t, func(_, p map[string]any) { tc.edit(p) }))
		if got := ReasonOf(err); got != tc.want || (tc.want == "" && err != nil) {
			t.Errorf("%s: %v, want reason %q", tc.name, err, tc.want)
		}
	}

	// The same token ages out as the clock moves.
	tok := e.token(t, func(_, p map[string]any) { p["exp"] = now + 10 })
	if _, err := e.verify(tok); err != nil {
		t.Fatal(err)
	}
	e.clock.Advance(69 * time.Second)
	if _, err := e.verify(tok); err != nil {
		t.Fatalf("69 s on, inside the skew: %v", err)
	}
	e.clock.Advance(time.Second)
	if _, err := e.verify(tok); ReasonOf(err) != ReasonExpired {
		t.Fatalf("70 s on: %v, want expired", err)
	}
}

// Algorithm confusion: an attacker who knows the public key signs with it as an
// HMAC secret and names HS256. The verifier decides the algorithm, so this is
// refused before any key is read, whatever bytes are used as the secret.
func TestAnHMACAssertionKeyedWithThePublicKeyIsRefused(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	der, err := x509.MarshalPKIXPublicKey(&e.a.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})
	pkcs1 := x509.MarshalPKCS1PublicKey(&e.a.PublicKey)
	secrets := map[string][]byte{
		"PKIX DER": der, "PKIX PEM": pemBytes, "PKCS#1 DER": pkcs1,
		"modulus":    e.a.N.Bytes(),
		"JWK n text": []byte(b64(e.a.N.Bytes())),
	}
	macOver := func(secret []byte, input string) string {
		mac := hmac.New(sha256.New, secret)
		mac.Write([]byte(input))
		return input + "." + b64(mac.Sum(nil))
	}
	for name, secret := range secrets {
		for _, alg := range []string{"HS256", "HS384", "HS512"} {
			h := e.header()
			h["alg"] = alg
			tok := macOver(secret, signingInput(t, h, e.payload()))
			if _, err := e.verify(tok); ReasonOf(err) != ReasonAlgorithm {
				t.Errorf("%s with %s: %v, want an algorithm refusal", alg, name, err)
			}
		}
	}
	if n := e.certs.calls.Load(); n != 0 {
		t.Errorf("%d fetches for assertions refused on their algorithm, want none", n)
	}
	// Naming RS256 over the same MAC does not help either.
	for name, secret := range secrets {
		tok := macOver(secret, signingInput(t, e.header(), e.payload()))
		if _, err := e.verify(tok); ReasonOf(err) != ReasonSignature {
			t.Errorf("RS256 header with a MAC keyed with %s: %v, want a signature refusal", name, err)
		}
	}
}

func TestAlgorithmNoneIsRefusedBeforeAnyKeyIsFetched(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	for _, tok := range []string{
		signingInput(t, map[string]any{"alg": "none"}, e.payload()) + ".",
		signingInput(t, map[string]any{"alg": "none", "kid": "k1"}, e.payload()) + ".",
		signingInput(t, map[string]any{"alg": "none", "kid": "k1"}, e.payload()) + "." + b64([]byte("sig")),
	} {
		if _, err := e.verify(tok); ReasonOf(err) != ReasonAlgorithm {
			t.Errorf("%v, want an algorithm refusal", err)
		}
	}
	if n := e.certs.calls.Load(); n != 0 {
		t.Errorf("%d fetches, want none", n)
	}
}

func TestOnlyTheHeaderIsRead(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	good := e.token(t, nil)

	mk := func(mutate func(r *http.Request)) *http.Request {
		r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/mcp", http.NoBody)
		mutate(r)
		return r
	}
	for _, tc := range []struct {
		name string
		req  *http.Request
		want Reason
	}{
		{"no header", mk(func(*http.Request) {}), ReasonMissing},
		{"the cookie holds it", mk(func(r *http.Request) { r.AddCookie(&http.Cookie{Name: "CF_Authorization", Value: good}) }), ReasonMissing},
		{"the query holds it", mk(func(r *http.Request) { r.URL.RawQuery = "token=" + good + "&" + HeaderName + "=" + good }), ReasonMissing},
		{"an Authorization header holds it", mk(func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+good) }), ReasonMissing},
		{"the header is empty", mk(func(r *http.Request) { r.Header.Set(HeaderName, "") }), ReasonMissing},
		{"the header is repeated", mk(func(r *http.Request) { r.Header.Add(HeaderName, good); r.Header.Add(HeaderName, good) }), ReasonMalformed},
		{"the header is repeated, the first bad", mk(func(r *http.Request) { r.Header.Add(HeaderName, "x.y.z"); r.Header.Add(HeaderName, good) }), ReasonMalformed},
		{"the header is padded with a space", mk(func(r *http.Request) { r.Header.Set(HeaderName, " "+good) }), ReasonMalformed},
		{"the header holds a Bearer prefix", mk(func(r *http.Request) { r.Header.Set(HeaderName, "Bearer "+good) }), ReasonMalformed},
	} {
		if _, err := e.v.VerifyRequest(tc.req); ReasonOf(err) != tc.want {
			t.Errorf("%s: %v, want reason %q", tc.name, err, tc.want)
		}
	}

	r := mk(func(r *http.Request) { r.Header.Set(HeaderName, good) })
	id, err := e.v.VerifyRequest(r)
	if err != nil || id.Email != "alice@example.com" {
		t.Errorf("a request with the header: %+v, %v", id, err)
	}
}

func TestACancelledRequestDoesNotBreakTheNextOnesKeys(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// The fetch is made for the key whatever happens to the request that asked.
	if _, err := e.v.Verify(ctx, e.token(t, nil)); err != nil {
		t.Fatalf("cancelled context, fetch is independent of it: %v", err)
	}
	if _, err := e.verify(e.token(t, nil)); err != nil {
		t.Fatal(err)
	}
	if n := e.certs.calls.Load(); n != 1 {
		t.Errorf("%d fetches, want 1", n)
	}
}
