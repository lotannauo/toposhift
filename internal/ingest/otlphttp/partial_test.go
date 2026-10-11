package otlphttp_test

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"testing"
	"unicode/utf8"

	"google.golang.org/protobuf/encoding/protowire"

	"github.com/lotannauo/toposhift/internal/ingest/otlphttp"
	"github.com/lotannauo/toposhift/internal/replay/capture"
)

// partialSink accepts in part: it returns p and err from AcceptPartial, and
// counts the calls of each method.
type partialSink struct {
	mu            sync.Mutex
	p             otlphttp.Partial
	err           error
	partial, full int
}

func (s *partialSink) Accept(context.Context, capture.LogsData) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.full++
	return nil
}

func (s *partialSink) AcceptPartial(context.Context, capture.LogsData) (otlphttp.Partial, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.partial++
	return s.p, s.err
}

// decodedPartial is a partial_success read back from a response.
type decodedPartial struct {
	present  bool
	rejected int64
	message  string
}

// decodeProtoResponse reads an ExportLogsServiceResponse by hand: field 1 is
// partial_success, whose field 1 is rejected_log_records (int64) and field 2 is
// error_message (string). Any other field fails the test.
func decodeProtoResponse(t testing.TB, b []byte) decodedPartial {
	t.Helper()
	var out decodedPartial
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			t.Fatalf("response: bad tag: %v", protowire.ParseError(n))
		}
		b = b[n:]
		if num != 1 || typ != protowire.BytesType {
			t.Fatalf("response: unexpected field %d of type %d", num, typ)
		}
		inner, n := protowire.ConsumeBytes(b)
		if n < 0 {
			t.Fatalf("response: bad partial_success: %v", protowire.ParseError(n))
		}
		b = b[n:]
		out.present = true
		for len(inner) > 0 {
			num, typ, n := protowire.ConsumeTag(inner)
			if n < 0 {
				t.Fatalf("partial_success: bad tag: %v", protowire.ParseError(n))
			}
			inner = inner[n:]
			switch {
			case num == 1 && typ == protowire.VarintType:
				v, n := protowire.ConsumeVarint(inner)
				if n < 0 {
					t.Fatalf("rejected_log_records: %v", protowire.ParseError(n))
				}
				out.rejected = int64(v)
				inner = inner[n:]
			case num == 2 && typ == protowire.BytesType:
				v, n := protowire.ConsumeString(inner)
				if n < 0 {
					t.Fatalf("error_message: %v", protowire.ParseError(n))
				}
				out.message = v
				inner = inner[n:]
			default:
				t.Fatalf("partial_success: unexpected field %d of type %d", num, typ)
			}
		}
	}
	return out
}

// decodeJSONResponse reads an ExportLogsServiceResponse in OTLP JSON, where the
// int64 is a decimal string.
func decodeJSONResponse(t testing.TB, b []byte) decodedPartial {
	t.Helper()
	var j struct {
		PartialSuccess *struct {
			RejectedLogRecords string `json:"rejectedLogRecords"`
			ErrorMessage       string `json:"errorMessage"`
		} `json:"partialSuccess"`
	}
	dec := json.NewDecoder(strings.NewReader(string(b)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&j); err != nil {
		t.Fatalf("response %q: %v", b, err)
	}
	if j.PartialSuccess == nil {
		return decodedPartial{}
	}
	var n int64
	if err := json.Unmarshal([]byte(j.PartialSuccess.RejectedLogRecords), &n); err != nil {
		t.Fatalf("rejectedLogRecords %q is not a decimal integer: %v", j.PartialSuccess.RejectedLogRecords, err)
	}
	return decodedPartial{present: true, rejected: n, message: j.PartialSuccess.ErrorMessage}
}

func decodeResponse(t testing.TB, enc string, b []byte) decodedPartial {
	t.Helper()
	if enc == jsonType {
		return decodeJSONResponse(t, b)
	}
	return decodeProtoResponse(t, b)
}

// A 200 with rejected records carries partial_success in either encoding, and
// decodes by hand to what the sink returned; the sink's AcceptPartial is called
// and its Accept is not.
func TestPartialSuccess(t *testing.T) {
	for _, enc := range encodings {
		t.Run(enc, func(t *testing.T) {
			sink := &partialSink{p: otlphttp.Partial{Rejected: 3, Message: "3 log records rejected: event time before the retention horizon"}}
			h := otlphttp.Handler(sink, otlphttp.Options{})
			rec := do(t, h, request{ctype: enc, body: sampleBody(t, enc)})
			if rec.Code != http.StatusOK {
				t.Fatalf("status %d, body %q", rec.Code, rec.Body.String())
			}
			if ct := rec.Header().Get("Content-Type"); ct != enc {
				t.Errorf("Content-Type %q, want %q", ct, enc)
			}
			got := decodeResponse(t, enc, bodyOf(t, rec))
			want := decodedPartial{present: true, rejected: 3, message: sink.p.Message}
			if got != want {
				t.Errorf("partial_success %+v, want %+v", got, want)
			}
			if sink.partial != 1 || sink.full != 0 {
				t.Errorf("AcceptPartial called %d times and Accept %d, want 1 and 0", sink.partial, sink.full)
			}
		})
	}
}

// The JSON form is exactly the OTLP JSON one, with the count as a string.
func TestPartialSuccessJSONForm(t *testing.T) {
	sink := &partialSink{p: otlphttp.Partial{Rejected: 12, Message: "bad"}}
	rec := do(t, otlphttp.Handler(sink, otlphttp.Options{}), request{ctype: jsonType, body: sampleBody(t, jsonType)})
	const want = `{"partialSuccess":{"rejectedLogRecords":"12","errorMessage":"bad"}}`
	if got := rec.Body.String(); got != want {
		t.Errorf("body %s, want %s", got, want)
	}
}

// With nothing rejected the response is as it was: empty for protobuf and {}
// for JSON. A negative count is nothing rejected.
func TestPartialSuccessNoneRejected(t *testing.T) {
	for _, rejected := range []int64{0, -1} {
		for _, enc := range encodings {
			sink := &partialSink{p: otlphttp.Partial{Rejected: rejected, Message: "ignored"}}
			rec := do(t, otlphttp.Handler(sink, otlphttp.Options{}), request{ctype: enc, body: sampleBody(t, enc)})
			want := map[string]string{protoType: "", jsonType: "{}"}[enc]
			if rec.Code != http.StatusOK || rec.Body.String() != want {
				t.Errorf("rejected %d %s: status %d body %q, want 200 and %q", rejected, enc, rec.Code, rec.Body.String(), want)
			}
		}
	}
}

// The message of a partial success is made valid UTF-8 and cut to the bound of a
// status message, ellipsis included.
func TestPartialSuccessMessageBound(t *testing.T) {
	long := strings.Repeat("é", otlphttp.MaxStatusMessageBytes) + "\xff"
	for _, enc := range encodings {
		sink := &partialSink{p: otlphttp.Partial{Rejected: 1, Message: "\xffbad " + long}}
		rec := do(t, otlphttp.Handler(sink, otlphttp.Options{}), request{ctype: enc, body: sampleBody(t, enc)})
		got := decodeResponse(t, enc, bodyOf(t, rec))
		if !utf8.ValidString(got.message) {
			t.Errorf("%s: message is not valid UTF-8", enc)
		}
		if len(got.message) > otlphttp.MaxStatusMessageBytes || !strings.HasSuffix(got.message, "…") || !strings.HasPrefix(got.message, "�bad ") {
			t.Errorf("%s: message of %d bytes, %q…%q", enc, len(got.message), got.message[:8], got.message[len(got.message)-8:])
		}
	}
}

// An error from AcceptPartial is mapped as Accept's is, and carries no partial
// success.
func TestPartialSinkErrors(t *testing.T) {
	cases := []struct {
		err    error
		status int
	}{
		{otlphttp.ErrBusy, http.StatusServiceUnavailable},
		{&otlphttp.UnavailableError{Message: "off"}, http.StatusServiceUnavailable},
		{&otlphttp.RefusedError{Message: "two clusters"}, http.StatusBadRequest},
		{context.Canceled, http.StatusInternalServerError},
	}
	for _, c := range cases {
		for _, enc := range encodings {
			sink := &partialSink{p: otlphttp.Partial{Rejected: 5, Message: "x"}, err: c.err}
			rec := do(t, otlphttp.Handler(sink, otlphttp.Options{}), request{ctype: enc, body: sampleBody(t, enc)})
			if rec.Code != c.status {
				t.Errorf("%v %s: status %d, want %d", c.err, enc, rec.Code, c.status)
			}
			if st := statusOf(t, rec); strings.Contains(st.Message, "rejected") {
				t.Errorf("%v %s: status carries the partial success: %q", c.err, enc, st.Message)
			}
		}
	}
}
