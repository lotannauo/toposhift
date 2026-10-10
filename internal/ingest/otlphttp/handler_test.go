package otlphttp_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/lotannauo/toposhift/internal/ingest/otlphttp"
	"github.com/lotannauo/toposhift/internal/replay/capture"
)

// statusOf decodes the google.rpc.Status a refusal carries, in the encoding of
// its Content-Type.
func statusOf(t testing.TB, rec *httptest.ResponseRecorder) *status.Status {
	t.Helper()
	b := bodyOf(t, rec)
	st := &status.Status{}
	switch ct := rec.Header().Get("Content-Type"); ct {
	case protoType:
		if err := proto.Unmarshal(b, st); err != nil {
			t.Fatalf("status body is not a google.rpc.Status: %v", err)
		}
	case jsonType:
		var j struct {
			Code    int32  `json:"code"`
			Message string `json:"message"`
		}
		if err := json.Unmarshal(b, &j); err != nil {
			t.Fatalf("status body is not JSON: %v: %q", err, b)
		}
		st.Code, st.Message = j.Code, j.Message
	default:
		t.Fatalf("refusal has Content-Type %q, body %q", ct, b)
	}
	return st
}

func TestSuccess(t *testing.T) {
	good := marshalProto(t, sampleProto())
	goodJSON := encodeJSON(t, sampleModel())
	cases := []struct {
		name string
		q    request
		body string // the exact response body
		ct   string
	}{
		{"protobuf", request{ctype: protoType, body: good}, "", protoType},
		{"protobuf gzip", request{ctype: protoType, encoding: "gzip", body: gz(t, good)}, "", protoType},
		{"protobuf identity", request{ctype: protoType, encoding: "identity", body: good}, "", protoType},
		{"protobuf empty message", request{ctype: protoType}, "", protoType},
		{"json", request{ctype: jsonType, body: goodJSON}, "{}", jsonType},
		{"json gzip", request{ctype: jsonType, encoding: "gzip", body: gz(t, goodJSON)}, "{}", jsonType},
		{"json with charset", request{ctype: "application/json; charset=utf-8", body: goodJSON}, "{}", jsonType},
		{"json with a trailing carriage return", request{ctype: jsonType, body: append(append([]byte{}, goodJSON...), '\r')}, "{}", jsonType},
		{"json with a trailing newline", request{ctype: jsonType, body: append(append([]byte{}, goodJSON...), '\r', '\n')}, "{}", jsonType},
		{"json empty resource list", request{ctype: jsonType, body: []byte(`{"resourceLogs":[]}`)}, "{}", jsonType},
		{"content type in capitals", request{ctype: "Application/JSON", body: goodJSON}, "{}", jsonType},
		{"encoding in capitals", request{ctype: protoType, encoding: "GZIP", body: gz(t, good)}, "", protoType},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h, sink := newHandler(otlphttp.Options{})
			rec := do(t, h, c.q)
			if rec.Code != http.StatusOK {
				t.Fatalf("status %d, body %q", rec.Code, rec.Body.String())
			}
			if got := rec.Body.String(); got != c.body {
				t.Errorf("body %q, want %q", got, c.body)
			}
			if got := rec.Header().Get("Content-Type"); got != c.ct {
				t.Errorf("Content-Type %q, want %q", got, c.ct)
			}
			if sink.calls() != 1 {
				t.Errorf("sink called %d times, want 1", sink.calls())
			}
		})
	}
}

func TestRefusals(t *testing.T) {
	good := marshalProto(t, sampleProto())
	goodJSON := encodeJSON(t, sampleModel())
	gzGood := gz(t, good)

	badTrace := sampleProto()
	badTrace.ResourceLogs[0].ScopeLogs[0].LogRecords[0].TraceId = []byte{1, 2, 3}
	badSpan := sampleProto()
	badSpan.ResourceLogs[0].ScopeLogs[0].LogRecords[0].SpanId = make([]byte, 16)
	strindex := sampleProto()
	strindex.ResourceLogs[0].Resource.Attributes[0].KeyStrindex = 1

	cases := []struct {
		name   string
		q      request
		status int
		code   int32 // google.rpc.Code in the body
	}{
		{"GET", request{method: http.MethodGet, ctype: protoType}, http.StatusMethodNotAllowed, 0},
		{"PUT", request{method: http.MethodPut, ctype: protoType, body: good}, http.StatusMethodNotAllowed, 0},
		{"DELETE", request{method: http.MethodDelete}, http.StatusMethodNotAllowed, 0},
		{"HEAD", request{method: http.MethodHead}, http.StatusMethodNotAllowed, 0},
		{"another path", request{path: "/v1/traces", ctype: protoType, body: good}, http.StatusNotFound, 0},
		{"a longer path", request{path: "/v1/logs/", ctype: protoType, body: good}, http.StatusNotFound, 0},
		{"no content type", request{body: good}, http.StatusUnsupportedMediaType, 3},
		{"text", request{ctype: "text/plain", body: good}, http.StatusUnsupportedMediaType, 3},
		{"xml", request{ctype: "application/xml", body: good}, http.StatusUnsupportedMediaType, 3},
		{"malformed content type", request{ctype: "application/json; =", body: good}, http.StatusUnsupportedMediaType, 3},
		{"brotli", request{ctype: protoType, encoding: "br", body: good}, http.StatusUnsupportedMediaType, 3},
		{"deflate", request{ctype: jsonType, encoding: "deflate", body: goodJSON}, http.StatusUnsupportedMediaType, 3},
		{"two encodings", request{ctype: protoType, encoding: "gzip, gzip", body: gzGood}, http.StatusUnsupportedMediaType, 3},

		{"protobuf that is not protobuf", request{ctype: protoType, body: []byte("not protobuf at all")}, http.StatusBadRequest, 3},
		{"protobuf cut short", request{ctype: protoType, body: good[:len(good)-1]}, http.StatusBadRequest, 3},
		{"protobuf with a bad trace id", request{ctype: protoType, body: marshalProto(t, badTrace)}, http.StatusBadRequest, 3},
		{"protobuf with a bad span id", request{ctype: protoType, body: marshalProto(t, badSpan)}, http.StatusBadRequest, 3},
		{"protobuf with a key index", request{ctype: protoType, body: marshalProto(t, strindex)}, http.StatusBadRequest, 3},
		{"protobuf with a string index", request{ctype: protoType, body: stringIndexBody()}, http.StatusBadRequest, 3},
		{"json that is not json", request{ctype: jsonType, body: []byte("{")}, http.StatusBadRequest, 3},
		{"json empty body", request{ctype: jsonType}, http.StatusBadRequest, 3},
		{"json with no signal", request{ctype: jsonType, body: []byte(`{"other":1}`)}, http.StatusBadRequest, 3},
		{"json with something after the empty object", request{ctype: jsonType, body: []byte(`{} {}`)}, http.StatusBadRequest, 3},
		{"json metrics", request{ctype: jsonType, body: []byte(`{"resourceMetrics":[]}`)}, http.StatusBadRequest, 3},
		{"json array", request{ctype: jsonType, body: []byte(`[]`)}, http.StatusBadRequest, 3},
		{"json with a bad trace id", request{ctype: jsonType, body: []byte(`{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"traceId":"0102"}]}]}]}`)}, http.StatusBadRequest, 3},
		{"json sent as protobuf", request{ctype: protoType, body: goodJSON}, http.StatusBadRequest, 3},
		{"protobuf sent as json", request{ctype: jsonType, body: good}, http.StatusBadRequest, 3},
		{"gzip that is not gzip", request{ctype: protoType, encoding: "gzip", body: good}, http.StatusBadRequest, 3},
		{"gzip with no body", request{ctype: protoType, encoding: "gzip"}, http.StatusBadRequest, 3},
		{"gzip cut short", request{ctype: protoType, encoding: "gzip", body: gzGood[:len(gzGood)-6]}, http.StatusBadRequest, 3},
		{"gzip with a bad checksum", request{ctype: protoType, encoding: "gzip", body: func() []byte {
			b := append([]byte{}, gzGood...)
			b[len(b)-8] ^= 0xff
			return b
		}()}, http.StatusBadRequest, 3},
		{"gzip with trailing bytes", request{ctype: protoType, encoding: "gzip", body: append(append([]byte{}, gzGood...), 1, 2, 3)}, http.StatusBadRequest, 3},
		{"gzip of json sent as protobuf", request{ctype: protoType, encoding: "gzip", body: gz(t, goodJSON)}, http.StatusBadRequest, 3},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h, sink := newHandler(otlphttp.Options{})
			rec := do(t, h, c.q)
			if rec.Code != c.status {
				t.Fatalf("status %d, want %d; body %q", rec.Code, c.status, rec.Body.String())
			}
			if sink.calls() != 0 {
				t.Errorf("the sink was called for a refused request")
			}
			if c.status == http.StatusMethodNotAllowed {
				if got := rec.Header().Get("Allow"); got != http.MethodPost {
					t.Errorf("Allow %q, want POST", got)
				}
			}
			if c.code != 0 {
				st := statusOf(t, rec)
				if st.Code != c.code || st.Message == "" {
					t.Errorf("status body %v, want code %d and a message", st, c.code)
				}
			}
		})
	}
}

// stringIndexBody is a request whose first log record has a body that is a
// string_value_strindex (field 8 of AnyValue), written by hand because no
// message the sample builds sets it.
func stringIndexBody() []byte {
	value := protowire.AppendVarint(protowire.AppendTag(nil, 8, protowire.VarintType), 1)
	record := protowire.AppendBytes(protowire.AppendTag(nil, 5, protowire.BytesType), value) // LogRecord.body
	scope := protowire.AppendBytes(protowire.AppendTag(nil, 2, protowire.BytesType), record) // ScopeLogs.log_records
	res := protowire.AppendBytes(protowire.AppendTag(nil, 2, protowire.BytesType), scope)    // ResourceLogs.scope_logs
	return protowire.AppendBytes(protowire.AppendTag(nil, 1, protowire.BytesType), res)      // LogsData.resource_logs
}

func TestRefusalFormat(t *testing.T) {
	h, _ := newHandler(otlphttp.Options{})
	t.Run("json request, json status", func(t *testing.T) {
		rec := do(t, h, request{ctype: jsonType, body: []byte("{")})
		if rec.Code != http.StatusBadRequest || rec.Header().Get("Content-Type") != jsonType {
			t.Fatalf("status %d, Content-Type %q", rec.Code, rec.Header().Get("Content-Type"))
		}
		var j map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &j); err != nil {
			t.Fatal(err)
		}
		if j["code"] != float64(3) || j["message"] == "" {
			t.Errorf("body %v", j)
		}
	})
	t.Run("protobuf request, protobuf status", func(t *testing.T) {
		rec := do(t, h, request{ctype: protoType, body: []byte("garbage")})
		st := statusOf(t, rec)
		if rec.Code != http.StatusBadRequest || st.Code != 3 {
			t.Fatalf("status %d, body %v", rec.Code, st)
		}
	})
}

// A sink that is busy gets 503 and Retry-After, wrapped or not; any other error
// is 500 and its text stays in the server; and neither is ever 200.
func TestSinkErrors(t *testing.T) {
	good := marshalProto(t, sampleProto())
	secret := errors.New("disk /var/secret is full")
	cases := []struct {
		name       string
		err        error
		status     int
		retryAfter bool
	}{
		{"busy", otlphttp.ErrBusy, http.StatusServiceUnavailable, true},
		{"wrapped busy", fmt.Errorf("queue full: %w", otlphttp.ErrBusy), http.StatusServiceUnavailable, true},
		{"other", secret, http.StatusInternalServerError, false},
	}
	for _, c := range cases {
		for _, enc := range []string{protoType, jsonType} {
			t.Run(c.name+" "+enc, func(t *testing.T) {
				h, sink := newHandler(otlphttp.Options{})
				sink.err = c.err
				body := good
				if enc == jsonType {
					body = encodeJSON(t, sampleModel())
				}
				rec := do(t, h, request{ctype: enc, body: body})
				if rec.Code != c.status {
					t.Fatalf("status %d, want %d", rec.Code, c.status)
				}
				if got := rec.Header().Get("Retry-After"); (got != "") != c.retryAfter || (c.retryAfter && got != "1") {
					t.Errorf("Retry-After %q, want %v", got, map[bool]string{true: `"1"`, false: "none"}[c.retryAfter])
				}
				if strings.Contains(rec.Body.String(), "secret") {
					t.Errorf("the response leaks the sink's error: %q", rec.Body.String())
				}
				if sink.calls() != 1 {
					t.Errorf("sink called %d times, want 1", sink.calls())
				}
			})
		}
	}
}

// The same data in either encoding reaches the sink as an equal value, and it is
// the value written down by hand.
func TestProtobufAndJSONAgree(t *testing.T) {
	want := sampleModel()
	bodies := map[string]request{
		"protobuf":      {ctype: protoType, body: marshalProto(t, sampleProto())},
		"protobuf gzip": {ctype: protoType, encoding: "gzip", body: gz(t, marshalProto(t, sampleProto()))},
		"json":          {ctype: jsonType, body: encodeJSON(t, want)},
		"json gzip":     {ctype: jsonType, encoding: "gzip", body: gz(t, encodeJSON(t, want))},
	}
	for name, q := range bodies {
		t.Run(name, func(t *testing.T) {
			h, sink := newHandler(otlphttp.Options{})
			if rec := do(t, h, q); rec.Code != http.StatusOK {
				t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
			}
			if len(sink.got) != 1 {
				t.Fatalf("sink called %d times", len(sink.got))
			}
			if !reflect.DeepEqual(sink.got[0], want) {
				got, _ := capture.EncodeLogs(sink.got[0])
				w, _ := capture.EncodeLogs(want)
				t.Errorf("sink got\n%s\nwant\n%s", got, w)
			}
		})
	}
}

// Posting a line of a capture file as JSON delivers the value decoding the
// line gives, and that value encodes and decodes to itself.
func TestCaptureLinesRoundTrip(t *testing.T) {
	lines := readCaptureLines(t, "../../replay/capture/testdata/logs_example.jsonl")
	if len(lines) == 0 {
		t.Fatal("no lines")
	}
	for i, line := range lines {
		t.Run(fmt.Sprintf("line %d", i+1), func(t *testing.T) {
			want, err := capture.DecodeLogs(line)
			if err != nil {
				t.Fatal(err)
			}
			h, sink := newHandler(otlphttp.Options{})
			// Lines are posted as they are in the file, and again compressed.
			for _, q := range []request{
				{ctype: jsonType, body: line},
				{ctype: jsonType, encoding: "gzip", body: gz(t, line)},
			} {
				sink.got = nil
				if rec := do(t, h, q); rec.Code != http.StatusOK {
					t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
				}
				if len(sink.got) != 1 || !reflect.DeepEqual(sink.got[0], want) {
					t.Fatalf("the sink did not receive the decoded line")
				}
			}
			again, err := capture.DecodeLogs(encodeJSON(t, sink.got[0]))
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(again, want) {
				t.Error("the delivered value does not survive an encode and a decode")
			}
		})
	}
}

// unknownPadding is a protobuf field no message has (field 1000, bytes) that
// makes a message of n bytes in all, so a valid body can be made any size.
func unknownPadding(n int) []byte {
	const tagSize = 2 // the tag of field 1000
	for lenSize := 1; lenSize <= 5; lenSize++ {
		body := n - tagSize - lenSize
		if body >= 0 && protowire.SizeVarint(uint64(body)) == lenSize {
			b := protowire.AppendTag(make([]byte, 0, n), 1000, protowire.BytesType)
			b = protowire.AppendVarint(b, uint64(body))
			return append(b, make([]byte, body)...)
		}
	}
	panic("padding out of range")
}

// The limit applies to the bytes on the wire and to the bytes after
// decompression, and a body of exactly the limit is accepted.
func TestBodyLimit(t *testing.T) {
	const limit = 1000
	opts := otlphttp.Options{MaxBodyBytes: limit}
	pad := func(n int) []byte { return unknownPadding(n) }
	random := func(n int) []byte {
		r := rand.New(rand.NewPCG(1, 2))
		b := make([]byte, n)
		for i := range b {
			b[i] = byte(r.Uint32())
		}
		return b
	}

	cases := []struct {
		name   string
		q      request
		status int
	}{
		{"at the limit", request{ctype: protoType, body: pad(limit)}, http.StatusOK},
		{"one byte over", request{ctype: protoType, body: pad(limit + 1)}, http.StatusRequestEntityTooLarge},
		{"json at the limit", request{ctype: jsonType, body: jsonOfSize(limit)}, http.StatusOK},
		{"json one byte over", request{ctype: jsonType, body: jsonOfSize(limit + 1)}, http.StatusRequestEntityTooLarge},
		{"gzip decompressing to the limit", request{ctype: protoType, encoding: "gzip", body: gz(t, pad(limit))}, http.StatusOK},
		{"gzip decompressing one byte over", request{ctype: protoType, encoding: "gzip", body: gz(t, pad(limit+1))}, http.StatusRequestEntityTooLarge},
		{"gzip decompressing far over", request{ctype: protoType, encoding: "gzip", body: gz(t, make([]byte, 100*limit))}, http.StatusRequestEntityTooLarge},
		{"gzip over on the wire", request{ctype: protoType, encoding: "gzip", body: gz(t, random(2*limit))}, http.StatusRequestEntityTooLarge},
		{"far over", request{ctype: protoType, body: make([]byte, 100*limit)}, http.StatusRequestEntityTooLarge},
		{"chunked one byte over", request{ctype: protoType, chunked: true, body: pad(limit + 1)}, http.StatusRequestEntityTooLarge},
		{"chunked at the limit", request{ctype: protoType, chunked: true, body: pad(limit)}, http.StatusOK},
		// A valid message of a few bytes, in a gzip stream whose header carries an
		// extra field that makes the stream longer than the limit.
		{"gzip stream above the limit with a small message", request{ctype: protoType, encoding: "gzip", body: gzWithExtra(t, pad(150), 2*limit)}, http.StatusRequestEntityTooLarge},
		{"chunked gzip stream above the limit with a small message", request{ctype: protoType, encoding: "gzip", chunked: true, body: gzWithExtra(t, pad(150), 2*limit)}, http.StatusRequestEntityTooLarge},
		{"chunked gzip stream just within the limit", request{ctype: protoType, encoding: "gzip", chunked: true, body: gzWithExtra(t, pad(150), limit-100)}, http.StatusOK},
		{"chunked gzip over on the wire", request{ctype: protoType, encoding: "gzip", chunked: true, body: gz(t, random(2*limit))}, http.StatusRequestEntityTooLarge},
		{"chunked gzip decompressing one byte over", request{ctype: protoType, encoding: "gzip", chunked: true, body: gz(t, pad(limit+1))}, http.StatusRequestEntityTooLarge},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h, sink := newHandler(opts)
			rec := do(t, h, c.q)
			if rec.Code != c.status {
				t.Fatalf("status %d, want %d; body %q", rec.Code, c.status, rec.Body.String())
			}
			if c.status != http.StatusOK && sink.calls() != 0 {
				t.Error("the sink was called for a refused request")
			}
			if c.status == http.StatusRequestEntityTooLarge {
				if st := statusOf(t, rec); st.Code != 8 {
					t.Errorf("rpc code %d, want 8", st.Code)
				}
			}
		})
	}
}

// The default limit is 64 MiB, on the wire and after decompression: a body just
// under it and exactly at it are accepted, and one byte over is refused.
func TestDefaultLimit(t *testing.T) {
	const limit = otlphttp.DefaultMaxBodyBytes
	if limit != 64<<20 {
		t.Fatalf("the default limit is %d, want 64 MiB", limit)
	}
	cases := []struct {
		name   string
		size   int
		gz     bool
		chunk  bool
		status int
	}{
		{"just under", limit - 1, false, false, http.StatusOK},
		{"at the limit", limit, false, false, http.StatusOK},
		{"one byte over", limit + 1, false, false, http.StatusRequestEntityTooLarge},
		{"chunked just under", limit - 1, false, true, http.StatusOK},
		{"chunked at the limit", limit, false, true, http.StatusOK},
		{"chunked one byte over", limit + 1, false, true, http.StatusRequestEntityTooLarge},
		{"gzip just under", limit - 1, true, false, http.StatusOK},
		{"gzip at the limit", limit, true, false, http.StatusOK},
		{"gzip one byte over", limit + 1, true, false, http.StatusRequestEntityTooLarge},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			body := unknownPadding(c.size)
			q := request{ctype: protoType, chunked: c.chunk, body: body}
			if c.gz {
				q.encoding, q.body = "gzip", gzFast(t, body)
			}
			h, sink := newHandler(otlphttp.Options{})
			rec := do(t, h, q)
			if rec.Code != c.status {
				t.Fatalf("status %d, want %d", rec.Code, c.status)
			}
			if want := map[bool]int{true: 1, false: 0}[c.status == http.StatusOK]; sink.calls() != want {
				t.Errorf("sink called %d times, want %d", sink.calls(), want)
			}
		})
	}
}

// jsonOfSize is a valid logs line of exactly n bytes: whitespace after the
// object.
func jsonOfSize(n int) []byte {
	const base = `{"resourceLogs":[]}`
	return []byte(base + strings.Repeat(" ", n-len(base)))
}

// A request that declares a length above the limit is refused without reading
// its body, and a chunked one is refused when it passes the limit.
func TestBodyLimitByLength(t *testing.T) {
	const limit = 1000
	h, sink := newHandler(otlphttp.Options{MaxBodyBytes: limit})

	t.Run("declared", func(t *testing.T) {
		body := &countingReader{r: strings.NewReader(strings.Repeat("x", 5*limit))}
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, otlphttp.Path, body)
		req.ContentLength = 5 * limit
		req.Header.Set("Content-Type", protoType)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("status %d", rec.Code)
		}
		if body.n != 0 {
			t.Errorf("%d bytes of the body were read, want none", body.n)
		}
	})
	t.Run("chunked", func(t *testing.T) {
		body := &countingReader{r: io.LimitReader(zeros{}, 1<<30)}
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, otlphttp.Path, body)
		req.ContentLength = -1
		req.Header.Set("Content-Type", protoType)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("status %d", rec.Code)
		}
		if body.n > 2*limit {
			t.Errorf("%d bytes of a chunked body were read for a limit of %d", body.n, limit)
		}
	})
	if sink.calls() != 0 {
		t.Error("the sink was called")
	}
}

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

type zeros struct{}

func (zeros) Read(p []byte) (int, error) { clear(p); return len(p), nil }

func TestNilSinkPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("Handler(nil) did not panic")
		}
	}()
	otlphttp.Handler(nil, otlphttp.Options{})
}
