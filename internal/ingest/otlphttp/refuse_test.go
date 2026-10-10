package otlphttp_test

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"unicode/utf8"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/lotannauo/toposhift/internal/ingest/otlphttp"
)

// encodings are the two body encodings a test runs a request in.
var encodings = []string{protoType, jsonType}

// sampleBody is the sample batch in the given encoding.
func sampleBody(t testing.TB, enc string) []byte {
	t.Helper()
	if enc == jsonType {
		return encodeJSON(t, sampleModel())
	}
	return marshalProto(t, sampleProto())
}

// checkStatus checks the status of a refusal: the HTTP status, the
// google.rpc.Code and the exact message, in the encoding of the request.
func checkStatus(t testing.TB, rec *httptest.ResponseRecorder, enc string, httpStatus int, code int32, msg string) *status.Status {
	t.Helper()
	if rec.Code != httpStatus {
		t.Fatalf("status %d, want %d, body %q", rec.Code, httpStatus, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); got != enc {
		t.Errorf("Content-Type %q, want %q", got, enc)
	}
	st := statusOf(t, rec)
	if st.Code != code {
		t.Errorf("google.rpc.Code %d, want %d", st.Code, code)
	}
	if st.Message != msg {
		t.Errorf("message %q, want %q", st.Message, msg)
	}
	return st
}

// A sink that refuses the request gets 400 with INVALID_ARGUMENT and its
// message, as the sink wrote it, in both encodings.
func TestRefusedError(t *testing.T) {
	const msg = "cluster name prod differs from configured staging"
	for _, enc := range encodings {
		t.Run(enc, func(t *testing.T) {
			h, sink := newHandler(otlphttp.Options{})
			sink.err = &otlphttp.RefusedError{Message: msg}
			rec := do(t, h, request{ctype: enc, body: sampleBody(t, enc)})
			checkStatus(t, rec, enc, http.StatusBadRequest, 3, msg)
			if got := rec.Header().Get("Retry-After"); got != "" {
				t.Errorf("Retry-After %q on a refusal that is not to be sent again", got)
			}
			if sink.calls() != 1 {
				t.Errorf("sink called %d times, want 1", sink.calls())
			}
		})
	}
}

// A refusal is found through wrapping and joining, and the client gets the
// refusal's message, not the wrapper's.
func TestRefusedErrorWrapped(t *testing.T) {
	const msg = "cluster name prod differs from configured staging"
	refused := &otlphttp.RefusedError{Message: msg}
	cases := []struct {
		name string
		err  error
	}{
		{"wrapped", fmt.Errorf("x: %w", refused)},
		{"wrapped twice", fmt.Errorf("y: %w", fmt.Errorf("x: %w", refused))},
		{"joined with another error", errors.Join(errors.New("other"), refused)},
	}
	for _, c := range cases {
		for _, enc := range encodings {
			t.Run(c.name+" "+enc, func(t *testing.T) {
				h, sink := newHandler(otlphttp.Options{})
				sink.err = c.err
				rec := do(t, h, request{ctype: enc, body: sampleBody(t, enc)})
				checkStatus(t, rec, enc, http.StatusBadRequest, 3, msg)
			})
		}
	}
}

// The refusal is whole. The sink saw the request, and the handler wrote
// nothing but the 400: the body is exactly the status, with no success body and
// no second write.
func TestRefusalIsWhole(t *testing.T) {
	const msg = "cluster name prod differs from configured staging"
	want := map[string][]byte{
		protoType: mustMarshal(t, &status.Status{Code: 3, Message: msg}),
		jsonType:  []byte(`{"code":3,"message":"` + msg + `"}`),
	}
	for _, enc := range encodings {
		t.Run(enc, func(t *testing.T) {
			h, sink := newHandler(otlphttp.Options{})
			sink.err = &otlphttp.RefusedError{Message: msg}
			rec := do(t, h, request{ctype: enc, body: sampleBody(t, enc)})
			if sink.calls() != 1 {
				t.Fatalf("sink called %d times, want 1", sink.calls())
			}
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status %d, want 400", rec.Code)
			}
			if got := rec.Body.Bytes(); string(got) != string(want[enc]) {
				t.Errorf("body %q, want exactly the status %q", got, want[enc])
			}
		})
	}
}

func mustMarshal(t testing.TB, m proto.Message) []byte {
	t.Helper()
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// A message the sink wrote is cut to at most MaxStatusMessageBytes bytes, the
// ellipsis included, at a rune boundary, in both encodings, for a refusal and
// for ingest being off.
func TestStatusMessageBound(t *testing.T) {
	const ellipsis = "…"
	if otlphttp.MaxStatusMessageBytes != 1024 {
		t.Fatalf("MaxStatusMessageBytes is %d, want 1024", otlphttp.MaxStatusMessageBytes)
	}
	max := otlphttp.MaxStatusMessageBytes

	type text struct {
		name string
		msg  string
		want string // the message the client gets
	}
	var texts []text
	// room is what is left of the bound for the text when there is an ellipsis.
	room := max - len(ellipsis)
	// A plain message just under and at the bound is not cut; one just over, and a
	// long one, is cut to the room, so the whole is the bound.
	for _, n := range []int{max - 1, max} {
		m := strings.Repeat("a", n)
		texts = append(texts, text{fmt.Sprintf("%d ASCII bytes", n), m, m})
	}
	texts = append(texts,
		text{"1025 ASCII bytes", strings.Repeat("a", max+1), strings.Repeat("a", room) + ellipsis},
		text{"3000 ASCII bytes", strings.Repeat("a", 3000), strings.Repeat("a", room) + ellipsis},
	)
	// A rune of w bytes that straddles the cut at every position inside it: the
	// cut goes back to where the rune starts.
	for _, r := range []string{"é", "€", "😀"} {
		for k := 1; k < len(r); k++ {
			pad := room - k // the rune starts at pad and the cut falls k bytes into it
			m := strings.Repeat("a", pad) + strings.Repeat(r, 3000/len(r))
			texts = append(texts, text{
				fmt.Sprintf("%d-byte rune cut %d bytes in", len(r), k), m,
				strings.Repeat("a", pad) + ellipsis,
			})
		}
	}
	// A 3,000-byte message whose rune ends exactly at the cut is cut there.
	texts = append(texts, text{
		"rune ending at the cut",
		strings.Repeat("a", room-2) + strings.Repeat("é", 1500),
		strings.Repeat("a", room-2) + "é" + ellipsis,
	})

	sinks := []struct {
		name string
		err  func(string) error
		code int
		http int
	}{
		{"refused", func(m string) error { return &otlphttp.RefusedError{Message: m} }, 3, http.StatusBadRequest},
		{"unavailable", func(m string) error { return &otlphttp.UnavailableError{Message: m} }, 14, http.StatusServiceUnavailable},
	}
	for _, s := range sinks {
		for _, tx := range texts {
			for _, enc := range encodings {
				t.Run(s.name+" "+tx.name+" "+enc, func(t *testing.T) {
					h, sink := newHandler(otlphttp.Options{})
					sink.err = s.err(tx.msg)
					rec := do(t, h, request{ctype: enc, body: sampleBody(t, enc)})
					st := statusOf(t, rec)
					if rec.Code != s.http || st.Code != int32(s.code) {
						t.Fatalf("status %d code %d, want %d code %d", rec.Code, st.Code, s.http, s.code)
					}
					if !utf8.ValidString(st.Message) {
						t.Errorf("the message is not valid UTF-8: ends %q", tail(st.Message))
					}
					if st.Message != tx.want {
						t.Errorf("message of %d bytes does not match the expected cut of %d bytes: ends %q", len(st.Message), len(tx.want), tail(st.Message))
					}
					if len(st.Message) > max {
						t.Errorf("message of %d bytes, above %d", len(st.Message), max)
					}
				})
			}
		}
	}
}

func tail(s string) string {
	if len(s) > 16 {
		return s[len(s)-16:]
	}
	return s
}

// nestedKeys is a protobuf body whose conversion fails at the innermost value,
// n key-value lists down. The error names every key on the way, so its text
// grows with n and with nothing the sender cannot choose.
func nestedKeys(t testing.TB, n int) []byte {
	t.Helper()
	v := &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValueStrindex{StringValueStrindex: 1}}
	key := strings.Repeat("k", 40) // the error names at most 32 bytes of a key
	for range n {
		v = &commonpb.AnyValue{Value: &commonpb.AnyValue_KvlistValue{KvlistValue: &commonpb.KeyValueList{
			Values: []*commonpb.KeyValue{{Key: key, Value: v}},
		}}}
	}
	return marshalProto(t, &logspb.LogsData{ResourceLogs: []*logspb.ResourceLogs{{
		Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{{Key: "k", Value: v}}},
	}}})
}

// The text of a decoder error is bounded like any other message. A body
// nested as deeply as the protobuf limit allows makes the error name a key at
// every level, which comes to more than 1,024 bytes.
func TestDecoderErrorBound(t *testing.T) {
	h, sink := newHandler(otlphttp.Options{})
	rec := do(t, h, request{ctype: protoType, body: nestedKeys(t, 45)})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status %d, want 400", rec.Code)
	}
	st := statusOf(t, rec)
	if st.Code != 3 {
		t.Errorf("google.rpc.Code %d, want 3", st.Code)
	}
	if !strings.HasPrefix(st.Message, "malformed request body: ") {
		t.Errorf("message does not start with the decoder's text: %q", st.Message)
	}
	// Cut, and not by chance short: the text before the cut was above the bound,
	// and the cut message, ellipsis included, fills it (the text is ASCII).
	if !strings.HasSuffix(st.Message, "…") || len(st.Message) != otlphttp.MaxStatusMessageBytes {
		t.Errorf("message of %d bytes, want %d bytes ending in an ellipsis: ends %q",
			len(st.Message), otlphttp.MaxStatusMessageBytes, tail(st.Message))
	}
	if !utf8.ValidString(st.Message) {
		t.Errorf("the message is not valid UTF-8")
	}
	if sink.calls() != 0 {
		t.Errorf("sink called %d times for a body that does not decode", sink.calls())
	}
	// A shallower body of the same shape is not cut: the test above is about the
	// bound and not about the shape.
	rec = do(t, h, request{ctype: protoType, body: nestedKeys(t, 5)})
	if msg := statusOf(t, rec).Message; rec.Code != http.StatusBadRequest || strings.HasSuffix(msg, "…") || len(msg) > otlphttp.MaxStatusMessageBytes {
		t.Errorf("status %d, message %q: a short error must come through whole", rec.Code, msg)
	}
}

// A message that is not valid UTF-8 is answered with a status a client can
// read: a protobuf string must be valid UTF-8, and a refusal can name a value the
// sender chose, such as a cluster name. A run of invalid bytes becomes one
// U+FFFD, and the replacement is done before the bound, because it makes the
// message longer.
func TestStatusMessageUTF8(t *testing.T) {
	max := otlphttp.MaxStatusMessageBytes
	cases := []struct {
		name string
		msg  string
		want string
	}{
		{"two invalid bytes", "cluster name \xff\xfe differs from configured staging", "cluster name \uFFFD differs from configured staging"},
		{"invalid at the end", "cluster name \xff", "cluster name \uFFFD"},
		{"truncated rune", "cluster name \xe2\x82 differs", "cluster name \uFFFD differs"},
		{"only invalid bytes", strings.Repeat("\xff", 1500), "\uFFFD"},
		// Each invalid byte becomes three, so the replaced text is above the bound
		// though the message the sink wrote is under it.
		{"replacement makes it longer", strings.Repeat("a\xff", 600), strings.Repeat("a\uFFFD", 255) + "a" + "…"},
	}
	sinks := []struct {
		name string
		err  func(string) error
		http int
	}{
		{"refused", func(m string) error { return &otlphttp.RefusedError{Message: m} }, http.StatusBadRequest},
		{"unavailable", func(m string) error { return &otlphttp.UnavailableError{Message: m} }, http.StatusServiceUnavailable},
	}
	for _, s := range sinks {
		for _, c := range cases {
			for _, enc := range encodings {
				t.Run(s.name+" "+c.name+" "+enc, func(t *testing.T) {
					h, sink := newHandler(otlphttp.Options{})
					sink.err = s.err(c.msg)
					rec := do(t, h, request{ctype: enc, body: sampleBody(t, enc)})
					if rec.Code != s.http {
						t.Fatalf("status %d, want %d", rec.Code, s.http)
					}
					st := statusOf(t, rec) // fails if the protobuf body is not a readable Status
					if !utf8.ValidString(st.Message) {
						t.Errorf("the message is not valid UTF-8: %q", tail(st.Message))
					}
					if len(st.Message) > max {
						t.Errorf("message of %d bytes, above %d", len(st.Message), max)
					}
					if st.Message != c.want {
						t.Errorf("message %q, want %q", tail(st.Message), tail(c.want))
					}
				})
			}
		}
	}
}

// A sink that returns a nil *RefusedError or *UnavailableError, which errors.As
// matches, has no message to send. The request is answered 500 like any other
// unknown error, and does not panic.
func TestTypedNilSinkError(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"refused", (*otlphttp.RefusedError)(nil)},
		{"unavailable", (*otlphttp.UnavailableError)(nil)},
		{"wrapped refused", fmt.Errorf("x: %w", (*otlphttp.RefusedError)(nil))},
		{"wrapped unavailable", fmt.Errorf("x: %w", (*otlphttp.UnavailableError)(nil))},
	}
	for _, c := range cases {
		for _, enc := range encodings {
			t.Run(c.name+" "+enc, func(t *testing.T) {
				if c.err.Error() == "" { // the error text must not panic either
					t.Error("empty error text")
				}
				h, sink := newHandler(otlphttp.Options{})
				sink.err = c.err
				rec := do(t, h, request{ctype: enc, body: sampleBody(t, enc)})
				checkStatus(t, rec, enc, http.StatusInternalServerError, 13, "internal error")
				if got := rec.Header().Get("Retry-After"); got != "" {
					t.Errorf("Retry-After %q on a 500", got)
				}
			})
		}
	}
}
