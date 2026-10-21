package s3fake

import (
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

type vector struct {
	Name            string      `json:"name"`
	AccessKeyID     string      `json:"access_key_id"`
	SecretAccessKey string      `json:"secret_access_key"`
	Region          string      `json:"region"`
	Service         string      `json:"service"`
	Time            string      `json:"time"`
	Method          string      `json:"method"`
	URI             string      `json:"uri"`
	Headers         [][2]string `json:"headers"`
	Authorization   string      `json:"authorization"`
}

func loadVectors(t *testing.T) []vector {
	t.Helper()
	b, err := os.ReadFile("../testdata/sigv4_vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var f struct {
		Vectors []vector `json:"vectors"`
	}
	if err := json.Unmarshal(b, &f); err != nil {
		t.Fatal(err)
	}
	if len(f.Vectors) < 8 {
		t.Fatalf("%d vectors, want the nine of the file", len(f.Vectors))
	}
	return f.Vectors
}

// request is the request a server would receive for the vector, with the
// Authorization header the example publishes.
func (v vector) request(t *testing.T) *http.Request {
	t.Helper()
	r, err := http.NewRequestWithContext(t.Context(), v.Method, "https://x.test"+v.URI, nil)
	if err != nil {
		t.Fatal(err)
	}
	r.RequestURI = v.URI
	for _, h := range v.Headers {
		if strings.EqualFold(h[0], "host") {
			r.Host = h[1]
		} else {
			r.Header.Add(h[0], h[1])
		}
	}
	r.Header.Set("Authorization", v.Authorization)
	return r
}

func (v vector) verifier() *verifier {
	return &verifier{
		accessKey: v.AccessKeyID, secret: v.SecretAccessKey, region: v.Region, service: v.Service,
		skew: 15 * time.Minute,
	}
}

func (v vector) now(t *testing.T) time.Time {
	t.Helper()
	at, err := time.Parse("20060102T150405Z", v.Time)
	if err != nil {
		t.Fatal(err)
	}
	return at
}

// TestVerifierAcceptsTheAWSExamples holds the server's check, which is written
// apart from the client's signing code, to the same published examples.
func TestVerifierAcceptsTheAWSExamples(t *testing.T) {
	for _, v := range loadVectors(t) {
		t.Run(v.Name, func(t *testing.T) {
			declared, err := v.verifier().verify(v.request(t), v.now(t))
			if err != nil {
				t.Fatalf("refused: %v\ncanonical request computed:\n%s", err, err.note)
			}
			if want := v.request(t).Header.Get("X-Amz-Content-Sha256"); declared != want {
				t.Errorf("declared payload hash %q, want %q", declared, want)
			}
		})
	}
}

// TestVerifierRefusesWhatDiffers changes one thing at a time in each published
// example and requires the check to refuse it: a check that cannot tell is no
// check.
func TestVerifierRefusesWhatDiffers(t *testing.T) {
	mutations := []struct {
		name string
		// edit changes the request and the verifier, and the clock when it returns
		// a time.
		edit func(r *http.Request, v *verifier, now time.Time) time.Time
		code string
	}{
		{"method", func(r *http.Request, _ *verifier, now time.Time) time.Time {
			if r.Method == http.MethodGet {
				r.Method = http.MethodHead
			} else {
				r.Method = http.MethodGet
			}
			return now
		}, "SignatureDoesNotMatch"},
		{"path gains a byte", func(r *http.Request, _ *verifier, now time.Time) time.Time {
			p, q, found := strings.Cut(r.RequestURI, "?")
			r.RequestURI = p + "x"
			if found {
				r.RequestURI += "?" + q
			}
			return now
		}, "SignatureDoesNotMatch"},
		{"query gains a parameter", func(r *http.Request, _ *verifier, now time.Time) time.Time {
			if strings.Contains(r.RequestURI, "?") {
				r.RequestURI += "&extra=1"
			} else {
				r.RequestURI += "?extra=1"
			}
			return now
		}, "SignatureDoesNotMatch"},
		{"host", func(r *http.Request, _ *verifier, now time.Time) time.Time {
			r.Host = "other." + r.Host
			return now
		}, "SignatureDoesNotMatch"},
		{"x-amz-date value", func(r *http.Request, _ *verifier, now time.Time) time.Time {
			// Another second of the same day: the scope still matches.
			r.Header.Set("X-Amz-Date", r.Header.Get("X-Amz-Date")[:13]+"01Z")
			return now.Add(time.Second)
		}, "SignatureDoesNotMatch"},
		{"signature", func(r *http.Request, _ *verifier, now time.Time) time.Time {
			a := r.Header.Get("Authorization")
			i := strings.LastIndex(a, "Signature=") + len("Signature=")
			c := byte('0')
			if a[i] == '0' {
				c = '1'
			}
			r.Header.Set("Authorization", a[:i]+string(c)+a[i+1:])
			return now
		}, "SignatureDoesNotMatch"},
		{"secret", func(_ *http.Request, v *verifier, now time.Time) time.Time {
			v.secret += "x"
			return now
		}, "SignatureDoesNotMatch"},
		{"access key", func(_ *http.Request, v *verifier, now time.Time) time.Time {
			v.accessKey += "x"
			return now
		}, "InvalidAccessKeyId"},
		{"region", func(_ *http.Request, v *verifier, now time.Time) time.Time {
			v.region = "eu-west-1"
			return now
		}, "AuthorizationHeaderMalformed"},
		{"service", func(_ *http.Request, v *verifier, now time.Time) time.Time {
			v.service += "x"
			return now
		}, "AuthorizationHeaderMalformed"},
		{"clock a minute past the allowed skew", func(_ *http.Request, _ *verifier, now time.Time) time.Time {
			return now.Add(16 * time.Minute)
		}, "RequestTimeTooSkewed"},
		{"clock behind by more than the skew", func(_ *http.Request, _ *verifier, now time.Time) time.Time {
			return now.Add(-16 * time.Minute)
		}, "RequestTimeTooSkewed"},
		{"no Authorization header", func(r *http.Request, _ *verifier, now time.Time) time.Time {
			r.Header.Del("Authorization")
			return now
		}, "AccessDenied"},
		{"a signed header is absent", func(r *http.Request, _ *verifier, now time.Time) time.Time {
			r.Header.Del("X-Amz-Date")
			return now
		}, "AccessDenied"},
		{"an x-amz header that is not signed", func(r *http.Request, _ *verifier, now time.Time) time.Time {
			r.Header.Set("X-Amz-Meta-Extra", "1")
			return now
		}, "AccessDenied"},
		{"signed header names out of order", func(r *http.Request, _ *verifier, now time.Time) time.Time {
			a := r.Header.Get("Authorization")
			i := strings.Index(a, "SignedHeaders=") + len("SignedHeaders=")
			j := i + strings.Index(a[i:], ",")
			names := strings.Split(a[i:j], ";")
			names[0], names[1] = names[1], names[0]
			r.Header.Set("Authorization", a[:i]+strings.Join(names, ";")+a[j:])
			return now
		}, "SignatureDoesNotMatch"},
	}
	for _, v := range loadVectors(t) {
		for _, m := range mutations {
			t.Run(v.Name+"/"+m.name, func(t *testing.T) {
				r, ver := v.request(t), v.verifier()
				now := m.edit(r, ver, v.now(t))
				_, err := ver.verify(r, now)
				if err == nil {
					t.Fatal("accepted")
				}
				if err.code != m.code {
					t.Errorf("refused with %s (%s), want %s", err.code, err.message, m.code)
				}
			})
		}
	}
}

// TestVerifierRequiresTheContentHashForS3 holds a request that omits or leaves
// unsigned x-amz-content-sha256 to the rule S3 has.
func TestVerifierRequiresTheContentHashForS3(t *testing.T) {
	for _, v := range loadVectors(t) {
		if v.Service != "s3" {
			continue
		}
		t.Run(v.Name, func(t *testing.T) {
			ver := v.verifier()
			ver.requireContentSHA = true
			if _, err := ver.verify(v.request(t), v.now(t)); err != nil {
				t.Fatalf("the example, which signs x-amz-content-sha256, is refused: %v", err)
			}
		})
	}
	// The IAM example signs no x-amz-content-sha256.
	for _, v := range loadVectors(t) {
		if v.Service == "s3" || v.Service == "service" {
			continue
		}
		ver := v.verifier()
		ver.requireContentSHA = true
		if _, err := ver.verify(v.request(t), v.now(t)); err == nil || err.code != "AccessDenied" {
			t.Errorf("%s: want AccessDenied for a request that does not sign x-amz-content-sha256, got %v", v.Name, err)
		}
	}
}

func TestEncoders(t *testing.T) {
	paths := []struct{ in, want string }{
		{"", "/"},
		{"/", "/"},
		{"/a/b", "/a/b"},
		{"/test$file.text", "/test%24file.text"},
		{"/test%24file.text", "/test%24file.text"},
		{"/a%2Fb", "/a/b"}, // an encoded slash is decoded, then kept as the separator it is
		{"/a b", "/a%20b"},
	}
	for _, tc := range paths {
		if got := encodePath(tc.in); got != tc.want {
			t.Errorf("encodePath(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
	queries := []struct{ in, want string }{
		{"", ""},
		{"lifecycle", "lifecycle="},
		{"b=2&a=1", "a=1&b=2"},
		{"a-b=1&a=2", "a=2&a-b=1"},
		{"prefix=a%2Fb", "prefix=a%2Fb"},
		{"prefix=a/b", "prefix=a%2Fb"},
	}
	for _, tc := range queries {
		if got := encodeQuery(tc.in); got != tc.want {
			t.Errorf("encodeQuery(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
	for in, want := range map[string]string{"  a   b \t c ": "a b c", "": "", "x": "x", " \t ": ""} {
		if got := squeeze(in); got != want {
			t.Errorf("squeeze(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestFaultRules(t *testing.T) {
	s := &Server{}
	s.Inject(Fault{Op: "PutObject", Key: "k", After: 1, Times: 2, Kind: Fail500})
	s.Inject(Fault{Op: "GetObject", Kind: Drop})
	var got []FaultKind
	for _, op := range []string{"PutObject", "PutObject", "GetObject", "PutObject", "PutObject", "PutObject", "GetObject", "HeadObject"} {
		k := FaultKind(0)
		if f := s.takeFault(op, "k"); f != nil {
			k = f.Kind
		}
		got = append(got, k)
	}
	want := []FaultKind{0, Fail500, Drop, Fail500, 0, 0, 0, 0}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("faults applied: %v, want %v", got, want)
		}
	}
	if f := s.takeFault("PutObject", "other"); f != nil {
		t.Errorf("a rule for key k applied to key other")
	}
}
