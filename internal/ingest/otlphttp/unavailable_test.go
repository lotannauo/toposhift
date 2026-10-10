package otlphttp_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"

	"github.com/lotannauo/toposhift/internal/ingest/otlphttp"
	"github.com/lotannauo/toposhift/internal/replay/capture"
)

const offMessage = "ingest is off: no data directory; set --data-dir or TOPOSHIFT_DATA_DIR"

// A sink that says ingest is off gets 503, Retry-After: 30, UNAVAILABLE and
// its message, in both encodings.
func TestUnavailableError(t *testing.T) {
	for _, enc := range encodings {
		t.Run(enc, func(t *testing.T) {
			h, sink := newHandler(otlphttp.Options{})
			sink.err = &otlphttp.UnavailableError{Message: offMessage}
			rec := do(t, h, request{ctype: enc, body: sampleBody(t, enc)})
			st := checkStatus(t, rec, enc, http.StatusServiceUnavailable, 14, offMessage)
			if got := rec.Header().Get("Retry-After"); got != "30" {
				t.Errorf("Retry-After %q, want %q", got, "30")
			}
			if !strings.Contains(st.Message, "--data-dir") {
				t.Errorf("the message does not name --data-dir")
			}
			if sink.calls() != 1 {
				t.Errorf("sink called %d times, want 1", sink.calls())
			}
		})
	}
}

// Ingest off is found through wrapping and joining.
func TestUnavailableErrorWrapped(t *testing.T) {
	off := &otlphttp.UnavailableError{Message: offMessage}
	cases := []struct {
		name string
		err  error
	}{
		{"wrapped", fmt.Errorf("x: %w", off)},
		{"wrapped twice", fmt.Errorf("y: %w", fmt.Errorf("x: %w", off))},
		{"joined with another error", errors.Join(errors.New("other"), off)},
	}
	for _, c := range cases {
		for _, enc := range encodings {
			t.Run(c.name+" "+enc, func(t *testing.T) {
				h, sink := newHandler(otlphttp.Options{})
				sink.err = c.err
				rec := do(t, h, request{ctype: enc, body: sampleBody(t, enc)})
				checkStatus(t, rec, enc, http.StatusServiceUnavailable, 14, offMessage)
				if got := rec.Header().Get("Retry-After"); got != "30" {
					t.Errorf("Retry-After %q, want %q", got, "30")
				}
			})
		}
	}
}

// gate is a sink as the Sink documentation asks one to be: while it is off it
// returns UnavailableError before it looks at the request, and when it is on it
// refuses a request whose resource has a cluster name other than its own.
type gate struct {
	mu         sync.Mutex
	off        bool
	configured string
	seen       int
}

const clusterKey = "k8s.cluster.name"

func (g *gate) Accept(_ context.Context, d capture.LogsData) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.seen++
	if g.off {
		return &otlphttp.UnavailableError{Message: offMessage}
	}
	for _, rl := range d.ResourceLogs {
		for _, kv := range rl.Resource.Attributes {
			if kv.Key == clusterKey && kv.Value.StringValue != nil && *kv.Value.StringValue != g.configured {
				return &otlphttp.RefusedError{Message: fmt.Sprintf(
					"cluster name %s differs from configured %s", *kv.Value.StringValue, g.configured)}
			}
		}
	}
	return nil
}

// clusterBody is a request whose resource carries the cluster name.
func clusterBody(t testing.TB, enc, cluster string) []byte {
	t.Helper()
	if enc == jsonType {
		return encodeJSON(t, capture.LogsData{ResourceLogs: []capture.ResourceLogs{{
			Resource: capture.Resource{Attributes: []capture.KeyValue{{Key: clusterKey, Value: cstr(cluster)}}},
		}}})
	}
	return marshalProto(t, &logspb.LogsData{ResourceLogs: []*logspb.ResourceLogs{{
		Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{{Key: clusterKey, Value: pstr(cluster)}}},
	}}})
}

// While ingest is off its 503 takes precedence over the 400 a request would
// get. The same request is refused once the sink is on, which shows that the
// request is one the sink would refuse.
func TestUnavailablePrecedesRefusal(t *testing.T) {
	for _, enc := range encodings {
		t.Run(enc, func(t *testing.T) {
			g := &gate{off: true, configured: "staging"}
			h := otlphttp.Handler(g, otlphttp.Options{})
			q := request{ctype: enc, body: clusterBody(t, enc, "prod")}

			rec := do(t, h, q)
			checkStatus(t, rec, enc, http.StatusServiceUnavailable, 14, offMessage)
			if got := rec.Header().Get("Retry-After"); got != "30" {
				t.Errorf("Retry-After %q, want %q", got, "30")
			}

			g.off = false
			rec = do(t, h, q)
			checkStatus(t, rec, enc, http.StatusBadRequest, 3, "cluster name prod differs from configured staging")
			if got := rec.Header().Get("Retry-After"); got != "" {
				t.Errorf("Retry-After %q on a refusal", got)
			}
			if g.seen != 2 {
				t.Errorf("sink saw %d requests, want 2", g.seen)
			}
		})
	}
}

// A sink that checks the request and the gate and reports both
// causes in one error still gets the 503.
func TestUnavailablePrecedesRefusalJoined(t *testing.T) {
	both := errors.Join(
		&otlphttp.RefusedError{Message: "cluster name prod differs from configured staging"},
		&otlphttp.UnavailableError{Message: offMessage},
	)
	for _, enc := range encodings {
		t.Run(enc, func(t *testing.T) {
			h, sink := newHandler(otlphttp.Options{})
			sink.err = both
			rec := do(t, h, request{ctype: enc, body: clusterBody(t, enc, "prod")})
			checkStatus(t, rec, enc, http.StatusServiceUnavailable, 14, offMessage)
			if got := rec.Header().Get("Retry-After"); got != "30" {
				t.Errorf("Retry-After %q, want %q", got, "30")
			}
		})
	}
}

// The order of the mapping holds when one error carries several causes, which is
// how a sink that checks more than one thing reports them: ingest off first,
// then busy, then a refusal, in whatever order they are joined.
func TestSinkErrorOrder(t *testing.T) {
	off := &otlphttp.UnavailableError{Message: offMessage}
	refused := &otlphttp.RefusedError{Message: "cluster name prod differs from configured staging"}
	busy := fmt.Errorf("queue full: %w", otlphttp.ErrBusy)
	const busyMessage = "busy: send the request again later"

	cases := []struct {
		name       string
		err        error
		status     int
		code       int32
		message    string
		retryAfter string
	}{
		{"refused then unavailable", errors.Join(refused, off), http.StatusServiceUnavailable, 14, offMessage, "30"},
		{"unavailable then refused", errors.Join(off, refused), http.StatusServiceUnavailable, 14, offMessage, "30"},
		{"busy then unavailable", errors.Join(busy, off), http.StatusServiceUnavailable, 14, offMessage, "30"},
		{"unavailable then busy", errors.Join(off, busy), http.StatusServiceUnavailable, 14, offMessage, "30"},
		{"refused then busy", errors.Join(refused, busy), http.StatusServiceUnavailable, 14, busyMessage, "1"},
		{"busy then refused", errors.Join(busy, refused), http.StatusServiceUnavailable, 14, busyMessage, "1"},
		{"all three", errors.Join(refused, busy, off), http.StatusServiceUnavailable, 14, offMessage, "30"},
		{"wrapped joins", fmt.Errorf("a: %w", errors.Join(fmt.Errorf("b: %w", refused), fmt.Errorf("c: %w", off))), http.StatusServiceUnavailable, 14, offMessage, "30"},
	}
	for _, c := range cases {
		for _, enc := range encodings {
			t.Run(c.name+" "+enc, func(t *testing.T) {
				h, sink := newHandler(otlphttp.Options{})
				sink.err = c.err
				rec := do(t, h, request{ctype: enc, body: sampleBody(t, enc)})
				checkStatus(t, rec, enc, c.status, c.code, c.message)
				if got := rec.Header().Get("Retry-After"); got != c.retryAfter {
					t.Errorf("Retry-After %q, want %q", got, c.retryAfter)
				}
			})
		}
	}
}

// The error types print their message, with a prefix that says what they are, so
// a log line that holds one is readable.
func TestErrorText(t *testing.T) {
	var err error = &otlphttp.RefusedError{Message: "m1"}
	if got := err.Error(); !strings.Contains(got, "m1") || got == "m1" {
		t.Errorf("RefusedError text %q", got)
	}
	err = &otlphttp.UnavailableError{Message: "m2"}
	if got := err.Error(); !strings.Contains(got, "m2") || got == "m2" {
		t.Errorf("UnavailableError text %q", got)
	}
}
