package access

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	testTeam     = "acme"
	testHost     = "acme.cloudflareaccess.com"
	testAudience = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
)

var (
	keyOnce sync.Once
	keys    [3]*rsa.PrivateKey
)

// testKeys returns three RSA keys, made once for the package.
func testKeys(t testing.TB) (a, b, c *rsa.PrivateKey) {
	t.Helper()
	keyOnce.Do(func() {
		for i := range keys {
			k, err := rsa.GenerateKey(rand.Reader, 2048)
			if err != nil {
				panic(err)
			}
			keys[i] = k
		}
	})
	return keys[0], keys[1], keys[2]
}

func b64(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

func jsonBytes(t testing.TB, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// jwkOf is the certificates entry for a public key.
func jwkOf(kid string, pub *rsa.PublicKey) map[string]any {
	return map[string]any{
		"kty": "RSA", "alg": "RS256", "use": "sig", "kid": kid,
		"n": b64(pub.N.Bytes()),
		"e": b64(big.NewInt(int64(pub.E)).Bytes()),
	}
}

// signingInput is "<header>.<payload>" for the two JSON values.
func signingInput(t testing.TB, h, p any) string {
	t.Helper()
	return b64(jsonBytes(t, h)) + "." + b64(jsonBytes(t, p))
}

// signRS256 signs the input with the key, as an Access issuer does.
func signRS256(t testing.TB, key *rsa.PrivateKey, input string) string {
	t.Helper()
	d := sha256.Sum256([]byte(input))
	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, d[:])
	if err != nil {
		t.Fatal(err)
	}
	return input + "." + b64(sig)
}

// certs is a fake of the team's certificates endpoint.
type certs struct {
	mu     sync.Mutex
	keys   []map[string]any
	status int    // 0 is 200
	body   []byte // replaces the document when set
	calls  atomic.Int64
	hook   func() // runs inside each request
}

func (c *certs) set(keys ...map[string]any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.keys = keys
}

func (c *certs) setStatus(code int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.status = code
}

func (c *certs) setBody(b []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.body = b
}

func (c *certs) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c.calls.Add(1)
	if c.hook != nil {
		c.hook()
	}
	if r.URL.Path != "/cdn-cgi/access/certs" || r.Host != testHost || r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.status != 0 {
		w.WriteHeader(c.status)
		return
	}
	if c.body != nil {
		_, _ = w.Write(c.body)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	b, err := json.Marshal(map[string]any{"keys": c.keys})
	if err != nil {
		panic(err)
	}
	_, _ = w.Write(b)
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// client routes every request to the handler in process, with no network.
func (c *certs) client() *http.Client {
	return &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if err := r.Context().Err(); err != nil {
			return nil, err
		}
		rec := httptest.NewRecorder()
		c.ServeHTTP(rec, r)
		resp := rec.Result()
		resp.Request = r
		return resp, nil
	})}
}

// clock is a settable clock.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock() *clock { return &clock{t: time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)} }

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// env is a verifier over a fake endpoint that publishes key A as "k1".
type env struct {
	v     *Verifier
	certs *certs
	clock *clock
	a, b  *rsa.PrivateKey
}

func newEnv(t testing.TB) *env {
	t.Helper()
	a, b, _ := testKeys(t)
	e := &env{certs: &certs{}, clock: newClock(), a: a, b: b}
	e.certs.set(jwkOf("k1", &a.PublicKey))
	v, err := New(Config{Team: testTeam, Audience: testAudience, Client: e.certs.client(), Now: e.clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	e.v = v
	return e
}

func (e *env) header() map[string]any {
	return map[string]any{"alg": "RS256", "kid": "k1", "typ": "JWT"}
}

func (e *env) payload() map[string]any {
	now := e.clock.Now()
	return map[string]any{
		"iss":   "https://" + testHost,
		"aud":   []string{testAudience},
		"exp":   now.Add(time.Hour).Unix(),
		"iat":   now.Unix(),
		"nbf":   now.Unix(),
		"sub":   "user-1",
		"email": "alice@example.com",
	}
}

// token signs the default header and payload with key A, after mutate edits them.
func (e *env) token(t testing.TB, mutate func(h, p map[string]any)) string {
	t.Helper()
	h, p := e.header(), e.payload()
	if mutate != nil {
		mutate(h, p)
	}
	return signRS256(t, e.a, signingInput(t, h, p))
}

func (e *env) verify(tok string) (Identity, error) { return e.v.Verify(context.Background(), tok) }
