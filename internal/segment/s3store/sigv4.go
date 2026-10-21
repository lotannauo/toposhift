package s3store

import (
	"cmp"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"
)

// AWS Signature Version 4, for the header form of authentication: the request
// carries its signature in the Authorization header. See "Authenticating
// Requests: Using the Authorization Header (AWS Signature Version 4)" in the
// Amazon S3 API reference. The code is checked against the examples that page
// and the signing guide publish (testdata/sigv4_vectors.json) and, in the tests
// of the store, against a server that verifies signatures with code written
// separately (package s3fake).

const (
	sigAlgorithm = "AWS4-HMAC-SHA256"
	// amzDateLayout is the layout of x-amz-date, the ISO 8601 basic format.
	amzDateLayout = "20060102T150405Z"
	// scopeDateLayout is the layout of the date in a credential scope.
	scopeDateLayout = "20060102"
	scopeTerminator = "aws4_request"
	// emptyPayloadSHA256 is the hex SHA-256 of no bytes: the payload hash of a
	// request without a body.
	emptyPayloadSHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
)

// signer signs requests for one service and region with one access key.
type signer struct {
	accessKey string
	secret    string
	region    string
	service   string
}

// Format implements fmt.Formatter so that a signer printed with any verb shows
// no secret.
func (s signer) Format(f fmt.State, _ rune) {
	_, _ = fmt.Fprintf(f, "signer{%s %s %s}", s.region, s.service, "<redacted>")
}

// sign adds x-amz-date, x-amz-content-sha256 (payloadHash, the hex SHA-256 of
// the body) and Authorization to r. The headers it signs are host, every x-amz-*
// header, if-none-match and content-type, whichever r carries when it is called.
func (s *signer) sign(r *http.Request, payloadHash string, now time.Time) {
	now = now.UTC()
	r.Header.Set("X-Amz-Date", now.Format(amzDateLayout))
	r.Header.Set("X-Amz-Content-Sha256", payloadHash)
	signed := signedHeaderNames(r)
	canonical := canonicalRequest(r, signed, payloadHash)
	sig := s.signature(now, stringToSign(now, s.scope(now), canonical))
	r.Header.Set("Authorization", sigAlgorithm+
		" Credential="+s.accessKey+"/"+s.scope(now)+
		", SignedHeaders="+strings.Join(signed, ";")+
		", Signature="+sig)
}

// scope is the credential scope: date/region/service/aws4_request.
func (s *signer) scope(t time.Time) string {
	return t.UTC().Format(scopeDateLayout) + "/" + s.region + "/" + s.service + "/" + scopeTerminator
}

// signingKey derives the key of a day, a region and a service from the secret:
// four HMAC-SHA256 steps, each keyed by the one before.
func (s *signer) signingKey(t time.Time) []byte {
	k := hmacSHA256([]byte("AWS4"+s.secret), t.UTC().Format(scopeDateLayout))
	k = hmacSHA256(k, s.region)
	k = hmacSHA256(k, s.service)
	return hmacSHA256(k, scopeTerminator)
}

// signature is the hex HMAC-SHA256 of the string to sign under the signing key.
func (s *signer) signature(t time.Time, toSign string) string {
	return hex.EncodeToString(hmacSHA256(s.signingKey(t), toSign))
}

func hmacSHA256(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))
	return h.Sum(nil)
}

// stringToSign is the text the signature covers: the algorithm, the request
// time, the credential scope and the hex SHA-256 of the canonical request.
func stringToSign(t time.Time, scope, canonical string) string {
	sum := sha256.Sum256([]byte(canonical))
	return sigAlgorithm + "\n" + t.UTC().Format(amzDateLayout) + "\n" + scope + "\n" + hex.EncodeToString(sum[:])
}

// signedHeaderNames lists, in ascending order and in lower case, the headers
// of r that sign covers.
func signedHeaderNames(r *http.Request) []string {
	names := []string{"host"}
	for k := range r.Header {
		l := strings.ToLower(k)
		if strings.HasPrefix(l, "x-amz-") || l == "if-none-match" || l == "content-type" {
			names = append(names, l)
		}
	}
	slices.Sort(names)
	return slices.Compact(names)
}

// canonicalRequest is the request in the one form both ends compute: the
// method, the URI-encoded path, the sorted and encoded query, the signed
// headers one a line (lower-case name, trimmed value), the list of their names,
// and the payload hash, joined by newlines. signed must be sorted.
func canonicalRequest(r *http.Request, signed []string, payloadHash string) string {
	var b strings.Builder
	b.WriteString(r.Method)
	b.WriteByte('\n')
	b.WriteString(canonicalPath(r.URL.Path))
	b.WriteByte('\n')
	b.WriteString(canonicalQuery(r.URL.RawQuery))
	b.WriteByte('\n')
	for _, name := range signed {
		b.WriteString(name)
		b.WriteByte(':')
		b.WriteString(canonicalHeaderValue(r, name))
		b.WriteByte('\n')
	}
	b.WriteByte('\n')
	b.WriteString(strings.Join(signed, ";"))
	b.WriteByte('\n')
	b.WriteString(payloadHash)
	return b.String()
}

// canonicalHeaderValue is the value of a header with surrounding space removed
// and each run of white space inside it a single space. host is the request's
// Host, which net/http keeps out of the header map.
func canonicalHeaderValue(r *http.Request, name string) string {
	if name == "host" {
		host := r.Host
		if host == "" {
			host = r.URL.Host
		}
		return strings.Join(strings.Fields(host), " ")
	}
	var values []string
	for _, v := range r.Header.Values(name) {
		values = append(values, strings.Join(strings.Fields(v), " "))
	}
	return strings.Join(values, ",")
}

// canonicalPath encodes a decoded path once, keeping its slashes. S3 does not
// encode a path twice, which most AWS services do; an empty path is "/".
func canonicalPath(decoded string) string {
	if decoded == "" {
		return "/"
	}
	return uriEncode(decoded, false)
}

// canonicalQuery re-encodes each name and value of a query string and sorts the
// pairs by encoded name, then by encoded value. A name without "=" has an empty
// value.
func canonicalQuery(raw string) string {
	if raw == "" {
		return ""
	}
	type pair struct{ k, v string }
	var enc []pair
	for _, p := range strings.Split(raw, "&") {
		if p == "" {
			continue
		}
		k, v, _ := strings.Cut(p, "=")
		// PathUnescape, not QueryUnescape: a '+' is a plus here, and the
		// requests this package builds never contain one unescaped.
		if dk, err := url.PathUnescape(k); err == nil {
			k = dk
		}
		if dv, err := url.PathUnescape(v); err == nil {
			v = dv
		}
		enc = append(enc, pair{uriEncode(k, true), uriEncode(v, true)})
	}
	slices.SortFunc(enc, func(a, b pair) int {
		if c := cmp.Compare(a.k, b.k); c != 0 {
			return c
		}
		return cmp.Compare(a.v, b.v)
	})
	out := make([]string, len(enc))
	for i, p := range enc {
		out[i] = p.k + "=" + p.v
	}
	return strings.Join(out, "&")
}

// uriEncode percent-encodes every byte except the unreserved characters
// A-Z a-z 0-9 '-' '_' '.' '~', with upper-case hex digits, and the slash when
// encodeSlash is false.
func uriEncode(s string, encodeSlash bool) string {
	const hexDigits = "0123456789ABCDEF"
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9',
			c == '-', c == '_', c == '.', c == '~':
			b.WriteByte(c)
		case c == '/' && !encodeSlash:
			b.WriteByte(c)
		default:
			b.WriteByte('%')
			b.WriteByte(hexDigits[c>>4])
			b.WriteByte(hexDigits[c&15])
		}
	}
	return b.String()
}
