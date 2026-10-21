package s3store

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"os"
	"slices"
	"strings"
	"testing"
	"time"
)

// vectorFile is testdata/sigv4_vectors.json: examples AWS publishes, with the
// canonical request, string to sign and signature each must give.
type vectorFile struct {
	Provenance string `json:"provenance"`
	SigningKey struct {
		Source          string `json:"source"`
		SecretAccessKey string `json:"secret_access_key"`
		Date            string `json:"date"`
		Region          string `json:"region"`
		Service         string `json:"service"`
		DerivedHex      string `json:"derived_hex"`
	} `json:"signing_key"`
	Vectors []vector `json:"vectors"`
}

type vector struct {
	Name             string      `json:"name"`
	Source           string      `json:"source"`
	AccessKeyID      string      `json:"access_key_id"`
	SecretAccessKey  string      `json:"secret_access_key"`
	Region           string      `json:"region"`
	Service          string      `json:"service"`
	Time             string      `json:"time"`
	Method           string      `json:"method"`
	URI              string      `json:"uri"`
	Headers          [][2]string `json:"headers"`
	SignedHeaders    string      `json:"signed_headers"`
	PayloadHash      string      `json:"payload_hash"`
	CanonicalRequest string      `json:"canonical_request"`
	StringToSign     string      `json:"string_to_sign"`
	Signature        string      `json:"signature"`
	Authorization    string      `json:"authorization"`
}

func loadVectors(t *testing.T) vectorFile {
	t.Helper()
	b, err := os.ReadFile("testdata/sigv4_vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var f vectorFile
	if err := json.Unmarshal(b, &f); err != nil {
		t.Fatal(err)
	}
	if len(f.Vectors) < 8 {
		t.Fatalf("%d vectors, want the nine of the file", len(f.Vectors))
	}
	return f
}

// request builds the request a vector describes, as a client would hold it.
func (v vector) request(t *testing.T) *http.Request {
	t.Helper()
	var host string
	for _, h := range v.Headers {
		if strings.EqualFold(h[0], "host") {
			host = h[1]
		}
	}
	r, err := http.NewRequestWithContext(t.Context(), v.Method, "https://"+host+v.URI, nil)
	if err != nil {
		t.Fatal(err)
	}
	r.Host = host
	for _, h := range v.Headers {
		if !strings.EqualFold(h[0], "host") {
			r.Header.Add(h[0], h[1])
		}
	}
	return r
}

func (v vector) when(t *testing.T) time.Time {
	t.Helper()
	at, err := time.Parse(amzDateLayout, v.Time)
	if err != nil {
		t.Fatal(err)
	}
	return at
}

func (v vector) signer() *signer {
	return &signer{accessKey: v.AccessKeyID, secret: v.SecretAccessKey, region: v.Region, service: v.Service}
}

// TestAWSVectors reproduces, byte for byte, each step of the examples AWS
// publishes: the canonical request, the string to sign, the signature and the
// Authorization header.
func TestAWSVectors(t *testing.T) {
	f := loadVectors(t)
	for _, v := range f.Vectors {
		t.Run(v.Name, func(t *testing.T) {
			r := v.request(t)
			at := v.when(t)
			signed := strings.Split(v.SignedHeaders, ";")

			canonical := canonicalRequest(r, signed, v.PayloadHash)
			if canonical != v.CanonicalRequest {
				t.Fatalf("canonical request:\n%s\nwant:\n%s", canonical, v.CanonicalRequest)
			}
			s := v.signer()
			toSign := stringToSign(at, s.scope(at), canonical)
			if toSign != v.StringToSign {
				t.Fatalf("string to sign:\n%s\nwant:\n%s", toSign, v.StringToSign)
			}
			if sig := s.signature(at, toSign); sig != v.Signature {
				t.Fatalf("signature %s, want %s", sig, v.Signature)
			}
		})
	}
}

// TestSignSetsTheAuthorizationHeader runs the whole of sign on the vectors
// whose signed headers are the ones sign chooses, and checks the header it
// writes against the published one.
func TestSignSetsTheAuthorizationHeader(t *testing.T) {
	f := loadVectors(t)
	checked := 0
	for _, v := range f.Vectors {
		t.Run(v.Name, func(t *testing.T) {
			r := v.request(t)
			at := v.when(t)
			// sign sets these two itself.
			r.Header.Del("X-Amz-Date")
			r.Header.Del("X-Amz-Content-Sha256")
			if v.Service != "s3" {
				t.Skip("sign always sends x-amz-content-sha256, which only the s3 examples sign")
			}
			v.signer().sign(r, v.PayloadHash, at)
			if got := strings.Join(signedHeaderNames(r), ";"); got != v.SignedHeaders {
				t.Skipf("sign signs %s, the example signs %s", got, v.SignedHeaders)
			}
			checked++
			if got := r.Header.Get("Authorization"); got != v.Authorization {
				t.Errorf("Authorization %q, want %q", got, v.Authorization)
			}
			if got := r.Header.Get("X-Amz-Date"); got != v.Time {
				t.Errorf("x-amz-date %q, want %q", got, v.Time)
			}
			if got := r.Header.Get("X-Amz-Content-Sha256"); got != v.PayloadHash {
				t.Errorf("x-amz-content-sha256 %q, want %q", got, v.PayloadHash)
			}
		})
	}
	if checked < 2 {
		t.Errorf("only %d examples were signed end to end, want at least 2", checked)
	}
}

func TestSigningKey(t *testing.T) {
	f := loadVectors(t)
	k := f.SigningKey
	s := &signer{secret: k.SecretAccessKey, region: k.Region, service: k.Service}
	at, err := time.Parse(scopeDateLayout, k.Date)
	if err != nil {
		t.Fatal(err)
	}
	if got := hex.EncodeToString(s.signingKey(at)); got != k.DerivedHex {
		t.Errorf("signing key %s, want %s", got, k.DerivedHex)
	}
}

func TestEmptyPayloadHash(t *testing.T) {
	sum := sha256.Sum256(nil)
	if got := hex.EncodeToString(sum[:]); got != emptyPayloadSHA256 {
		t.Errorf("emptyPayloadSHA256 is %s, the SHA-256 of no bytes is %s", emptyPayloadSHA256, got)
	}
}

func TestSignedHeaderNames(t *testing.T) {
	r, err := http.NewRequestWithContext(t.Context(), http.MethodPut, "https://example.test/b/k", nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range []string{
		"X-Amz-Meta-Sha256", "Content-Type", "If-None-Match", "X-Amz-Date", "X-Amz-Content-Sha256",
		"X-Amz-Meta-Alpha", "X-Amz-Meta-Zulu", "X-Amz-Acl", "Content-Length", "User-Agent", "Accept-Encoding",
	} {
		r.Header.Set(h, "v")
	}
	want := []string{
		"content-type", "host", "if-none-match", "x-amz-acl", "x-amz-content-sha256", "x-amz-date",
		"x-amz-meta-alpha", "x-amz-meta-sha256", "x-amz-meta-zulu",
	}
	for range 50 { // map order is random; the result must not be
		if got := signedHeaderNames(r); !slices.Equal(got, want) {
			t.Fatalf("signed headers %v, want %v", got, want)
		}
	}
}

func TestURIEncode(t *testing.T) {
	tests := []struct {
		in         string
		encodeSlas bool
		want       string
	}{
		{"abcXYZ019-_.~", true, "abcXYZ019-_.~"},
		{"a/b", false, "a/b"},
		{"a/b", true, "a%2Fb"},
		{"a b", true, "a%20b"},
		{"a+b", true, "a%2Bb"},
		{"$&,:;=?@", false, "%24%26%2C%3A%3B%3D%3F%40"},
		{"\xe1\x88\xb4", false, "%E1%88%B4"},
		{"%", true, "%25"},
	}
	for _, tc := range tests {
		if got := uriEncode(tc.in, tc.encodeSlas); got != tc.want {
			t.Errorf("uriEncode(%q, %v) = %q, want %q", tc.in, tc.encodeSlas, got, tc.want)
		}
	}
}

func TestCanonicalQuery(t *testing.T) {
	tests := []struct{ in, want string }{
		{"", ""},
		{"lifecycle", "lifecycle="},
		{"b=2&a=1", "a=1&b=2"},
		{"a=2&a=1", "a=1&a=2"},
		// Sorted by name first: "a" before "a-b", though "a-b=" sorts before "a=".
		{"a-b=1&a=2", "a=2&a-b=1"},
		{"prefix=backup%2Fx%20y", "prefix=backup%2Fx%20y"},
		{"prefix=a/b c", "prefix=a%2Fb%20c"},
	}
	for _, tc := range tests {
		if got := canonicalQuery(tc.in); got != tc.want {
			t.Errorf("canonicalQuery(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestCanonicalHeaderValue(t *testing.T) {
	r, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://h.test/", nil)
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("X-Amz-Meta-A", "  a   b \t c ")
	r.Header.Add("X-Amz-Meta-B", " one ")
	r.Header.Add("X-Amz-Meta-B", "two")
	if got := canonicalHeaderValue(r, "x-amz-meta-a"); got != "a b c" {
		t.Errorf("got %q, want %q", got, "a b c")
	}
	if got := canonicalHeaderValue(r, "x-amz-meta-b"); got != "one,two" {
		t.Errorf("got %q, want %q", got, "one,two")
	}
	if got := r.Header.Values("X-Amz-Meta-B"); got[0] != " one " {
		t.Errorf("canonicalizing changed the request's header to %q", got)
	}
	if got := canonicalHeaderValue(r, "host"); got != "h.test" {
		t.Errorf("host %q, want h.test", got)
	}
}
