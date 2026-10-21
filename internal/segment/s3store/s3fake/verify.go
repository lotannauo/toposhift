package s3fake

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

// The signature check below is written without reference to the client's
// signing code in package s3store, from the same published description, so that
// a misunderstanding of the description made on one side is not made on the
// other. Both are held to the examples AWS publishes (testdata/sigv4_vectors.json).

// verifier checks the Signature Version 4 header form of authentication.
type verifier struct {
	accessKey string
	secret    string
	region    string
	service   string
	// requireContentSHA makes the x-amz-content-sha256 header, and signing it,
	// mandatory, as S3 does. Off, a request without the header is taken to have
	// an empty body.
	requireContentSHA bool
	// skew is how far a request's time may be from now.
	skew time.Duration
}

// authError is a refused request, in the terms of an S3 error response.
type authError struct {
	status  int
	code    string
	message string
	// note is for the test log, never for the client: the canonical request the
	// server computed.
	note string
}

func (e *authError) Error() string { return e.code + ": " + e.message }

const (
	algorithm = "AWS4-HMAC-SHA256"
	// unsignedPayload is the payload hash of a request whose body is not signed.
	unsignedPayload = "UNSIGNED-PAYLOAD"
	emptyHash       = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
)

// verify checks r. On success it returns the payload hash the request declared:
// the body must still be checked against it by whoever reads the body.
func (v *verifier) verify(r *http.Request, now time.Time) (declared string, _ *authError) {
	auth := r.Header.Get("Authorization")
	if auth == "" {
		return "", &authError{status: 403, code: "AccessDenied", message: "Request is missing Authentication Token"}
	}
	rest, ok := strings.CutPrefix(auth, algorithm+" ")
	if !ok {
		return "", &authError{status: 400, code: "AuthorizationHeaderMalformed", message: "unsupported authorization algorithm"}
	}
	fields := map[string]string{}
	for _, part := range strings.Split(rest, ",") {
		k, val, found := strings.Cut(strings.TrimSpace(part), "=")
		if _, dup := fields[k]; !found || dup {
			return "", &authError{status: 400, code: "AuthorizationHeaderMalformed", message: "malformed Authorization header"}
		}
		fields[k] = val
	}
	credential, signedList, signature := fields["Credential"], fields["SignedHeaders"], fields["Signature"]
	if len(fields) != 3 || credential == "" || signedList == "" || signature == "" {
		return "", &authError{status: 400, code: "AuthorizationHeaderMalformed", message: "the Authorization header needs Credential, SignedHeaders and Signature"}
	}

	scope := strings.Split(credential, "/")
	if len(scope) != 5 || scope[4] != "aws4_request" {
		return "", &authError{status: 400, code: "AuthorizationHeaderMalformed", message: "malformed credential scope"}
	}
	if scope[0] != v.accessKey {
		return "", &authError{status: 403, code: "InvalidAccessKeyId", message: "The access key id you provided does not exist"}
	}
	if scope[2] != v.region || scope[3] != v.service {
		return "", &authError{status: 400, code: "AuthorizationHeaderMalformed", message: "the credential scope names another region or service"}
	}

	stamp := r.Header.Get("X-Amz-Date")
	when, err := time.Parse("20060102T150405Z", stamp)
	if err != nil {
		return "", &authError{status: 403, code: "AccessDenied", message: "x-amz-date is missing or malformed"}
	}
	if stamp[:8] != scope[1] {
		return "", &authError{status: 400, code: "AuthorizationHeaderMalformed", message: "the credential date is not the request date"}
	}
	if d := now.Sub(when); d > v.skew || -d > v.skew {
		return "", &authError{status: 403, code: "RequestTimeTooSkewed", message: "The difference between the request time and the current time is too large"}
	}

	signed := strings.Split(signedList, ";")
	present := map[string]bool{}
	for i, name := range signed {
		if name != strings.ToLower(name) || name == "" || (i > 0 && signed[i-1] >= name) {
			return "", &authError{status: 403, code: "SignatureDoesNotMatch", message: "the signed header names are not in ascending lower-case order"}
		}
		present[name] = true
	}
	need := []string{"host", "x-amz-date"}
	if v.requireContentSHA {
		need = append(need, "x-amz-content-sha256")
	}
	for _, name := range need {
		if !present[name] {
			return "", &authError{status: 403, code: "AccessDenied", message: "the header " + name + " must be signed"}
		}
	}
	for name := range r.Header {
		l := strings.ToLower(name)
		if strings.HasPrefix(l, "x-amz-") && !present[l] {
			return "", &authError{status: 403, code: "AccessDenied", message: "There were headers present in the request which were not signed: " + l}
		}
	}

	declared = r.Header.Get("X-Amz-Content-Sha256")
	payload := declared
	if declared == "" {
		if v.requireContentSHA {
			return "", &authError{status: 400, code: "InvalidRequest", message: "Missing required header for this request: x-amz-content-sha256"}
		}
		payload = emptyHash
	}

	canonical := v.canonicalRequest(r, signed, payload)
	sum := sha256.Sum256([]byte(canonical))
	toSign := algorithm + "\n" + stamp + "\n" + strings.Join(scope[1:], "/") + "\n" + hex.EncodeToString(sum[:])
	want := hex.EncodeToString(mac(v.signingKey(scope[1]), toSign))
	if !hmac.Equal([]byte(want), []byte(signature)) {
		return "", &authError{
			status:  403,
			code:    "SignatureDoesNotMatch",
			message: "The request signature we calculated does not match the signature you provided",
			note:    canonical,
		}
	}
	return declared, nil
}

func (v *verifier) signingKey(date string) []byte {
	k := mac([]byte("AWS4"+v.secret), date)
	k = mac(k, v.region)
	k = mac(k, v.service)
	return mac(k, "aws4_request")
}

func mac(key []byte, msg string) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(msg))
	return m.Sum(nil)
}

func (v *verifier) canonicalRequest(r *http.Request, signed []string, payload string) string {
	rawPath, rawQuery := r.URL.EscapedPath(), r.URL.RawQuery
	if r.RequestURI != "" {
		rawPath, rawQuery, _ = strings.Cut(r.RequestURI, "?")
	}
	var b bytes.Buffer
	fmt.Fprintf(&b, "%s\n%s\n%s\n", r.Method, encodePath(rawPath), encodeQuery(rawQuery))
	for _, name := range signed {
		var vals []string
		if name == "host" {
			vals = []string{r.Host}
		} else {
			vals = append([]string(nil), r.Header.Values(name)...)
		}
		for i := range vals {
			vals[i] = squeeze(vals[i])
		}
		fmt.Fprintf(&b, "%s:%s\n", name, strings.Join(vals, ","))
	}
	fmt.Fprintf(&b, "\n%s\n%s", signedList(signed), payload)
	return b.String()
}

func signedList(signed []string) string { return strings.Join(signed, ";") }

// squeeze trims a header value and turns each run of spaces and tabs inside it
// into one space.
func squeeze(s string) string {
	var out []byte
	space := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == ' ' || c == '\t' {
			space = true
			continue
		}
		if space && len(out) > 0 {
			out = append(out, ' ')
		}
		space = false
		out = append(out, c)
	}
	return string(out)
}

// unreserved marks the bytes SigV4 leaves unencoded.
var unreserved = func() (t [256]bool) {
	for _, r := range "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.~" {
		t[r] = true
	}
	return t
}()

// encode percent-encodes every byte that is not unreserved, in upper-case hex;
// slash too unless keepSlash.
func encode(s string, keepSlash bool) string {
	var out []byte
	for i := 0; i < len(s); i++ {
		c := s[i]
		if unreserved[c] || (keepSlash && c == '/') {
			out = append(out, c)
		} else {
			out = append(out, fmt.Sprintf("%%%02X", c)...)
		}
	}
	return string(out)
}

// encodePath is the canonical URI: the path as the client sent it, decoded and
// encoded again once, slashes kept.
func encodePath(raw string) string {
	dec, err := url.PathUnescape(raw)
	if err != nil {
		dec = raw
	}
	if dec == "" {
		return "/"
	}
	return encode(dec, true)
}

// encodeQuery is the canonical query string: each pair decoded and encoded
// again, sorted by name and then value.
func encodeQuery(raw string) string {
	if raw == "" {
		return ""
	}
	type pair struct{ k, v string }
	var pairs []pair
	for _, p := range strings.Split(raw, "&") {
		if p == "" {
			continue
		}
		k, val, _ := strings.Cut(p, "=")
		if d, err := url.PathUnescape(k); err == nil {
			k = d
		}
		if d, err := url.PathUnescape(val); err == nil {
			val = d
		}
		pairs = append(pairs, pair{encode(k, false), encode(val, false)})
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].k != pairs[j].k {
			return pairs[i].k < pairs[j].k
		}
		return pairs[i].v < pairs[j].v
	})
	parts := make([]string, len(pairs))
	for i, p := range pairs {
		parts[i] = p.k + "=" + p.v
	}
	return strings.Join(parts, "&")
}
