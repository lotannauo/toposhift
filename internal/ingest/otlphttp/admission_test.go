package otlphttp_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/lotannauo/toposhift/internal/ingest/otlphttp"
	"github.com/lotannauo/toposhift/internal/replay/capture"
)

// holdSink holds its first call until release is closed, and answers every
// call with nil. entered is closed when the first call has begun.
type holdSink struct {
	calls   atomic.Int64
	entered chan struct{}
	release chan struct{}
}

func newHoldSink() *holdSink {
	return &holdSink{entered: make(chan struct{}), release: make(chan struct{})}
}

func (s *holdSink) Accept(_ context.Context, _ capture.LogsData) error {
	if s.calls.Add(1) == 1 {
		close(s.entered)
		<-s.release
	}
	return nil
}

// post serves one request whose body is read through a counting reader, and
// returns the response and the bytes of the body that were read.
func post(t testing.TB, h http.Handler, encoding string, length int64, body []byte) (*httptest.ResponseRecorder, int64) {
	t.Helper()
	cr := &countingReader{r: bytes.NewReader(body)}
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, otlphttp.Path, cr)
	req.ContentLength = length
	req.Header.Set("Content-Type", protoType)
	if encoding != "" {
		req.Header.Set("Content-Encoding", encoding)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec, cr.n
}

// holding starts a request that the sink holds, and returns a function that
// releases it and waits for its answer.
func holding(t *testing.T, h http.Handler, sink *holdSink, encoding string, length int64, body []byte) func() *httptest.ResponseRecorder {
	t.Helper()
	var (
		wg  sync.WaitGroup
		rec *httptest.ResponseRecorder
	)
	wg.Go(func() { rec, _ = post(t, h, encoding, length, body) })
	<-sink.entered
	return func() *httptest.ResponseRecorder {
		close(sink.release)
		wg.Wait()
		return rec
	}
}

func checkBusy(t testing.TB, rec *httptest.ResponseRecorder, read int64) {
	t.Helper()
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status %d, want 503", rec.Code)
	}
	if got := rec.Header().Get("Retry-After"); got != "1" {
		t.Errorf("Retry-After %q, want 1", got)
	}
	if st := statusOf(t, rec); st.Code != 14 || !strings.Contains(st.Message, "busy") {
		t.Errorf("status %d %q", st.Code, st.Message)
	}
	if read != 0 {
		t.Errorf("%d bytes of the refused body were read, want none", read)
	}
}

// S12. A request that would take the bytes in flight above the budget is
// answered 503 with Retry-After: 1 at once, without its body being read, while
// another request holds its reservation; once that one is answered, the same
// request is admitted.
func TestAdmissionOverBudget(t *testing.T) {
	body := marshalProto(t, sampleProto())
	n := int64(len(body))
	sink := newHoldSink()
	h := otlphttp.Handler(sink, otlphttp.Options{MaxBodyBytes: 1 << 20, MaxInFlightBytes: 2*n - 1})

	done := holding(t, h, sink, "", n, body)
	rec, read := post(t, h, "", n, body)
	checkBusy(t, rec, read)
	if got := sink.calls.Load(); got != 1 {
		t.Errorf("the sink was called %d times, want 1", got)
	}
	if rec := done(); rec.Code != http.StatusOK {
		t.Fatalf("the held request: status %d", rec.Code)
	}
	if rec, _ := post(t, h, "", n, body); rec.Code != http.StatusOK {
		t.Fatalf("after the release: status %d, body %q", rec.Code, rec.Body.String())
	}
}

// S12. A chunked request, whose length is not declared, and a gzip request,
// however small its compressed body, each reserve the body limit; a request of
// the same body with its length declared and no compression reserves that
// length. The budget is the limit plus less than one small body, so a second
// small request fits only beside a small reservation.
func TestAdmissionReservesTheLimit(t *testing.T) {
	const limit = 1 << 16
	body := marshalProto(t, sampleProto())
	small := int64(len(body))
	zipped := gz(t, body)
	cases := []struct {
		name     string
		encoding string
		length   int64
		body     []byte
		admitted bool // whether a second small request is admitted beside it
	}{
		{"declared length", "", small, body, true},
		{"chunked", "", -1, body, false},
		{"small gzip", "gzip", int64(len(zipped)), zipped, false},
		{"small gzip, chunked", "gzip", -1, zipped, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sink := newHoldSink()
			h := otlphttp.Handler(sink, otlphttp.Options{MaxBodyBytes: limit, MaxInFlightBytes: limit + small - 1})
			done := holding(t, h, sink, c.encoding, c.length, c.body)
			rec, read := post(t, h, "", small, body)
			if c.admitted {
				if rec.Code != http.StatusOK {
					t.Errorf("second request: status %d, want 200", rec.Code)
				}
			} else {
				checkBusy(t, rec, read)
			}
			if rec := done(); rec.Code != http.StatusOK {
				t.Fatalf("the held request: status %d, body %q", rec.Code, rec.Body.String())
			}
		})
	}
}

// panicSink panics in Accept.
type panicSink struct{}

func (panicSink) Accept(context.Context, capture.LogsData) error { panic("sink panic") }

// failingReader returns an error that is not the end of the body.
type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { return 0, errors.New("connection reset") }

// switchSink passes each call to the sink it holds now, so that one handler can
// be driven down a path and then checked with a sink that accepts.
type switchSink struct{ cur atomic.Pointer[otlphttp.Sink] }

func (s *switchSink) set(k otlphttp.Sink) { s.cur.Store(&k) }

func (s *switchSink) Accept(ctx context.Context, d capture.LogsData) error {
	return (*s.cur.Load()).Accept(ctx, d)
}

func (s *switchSink) AcceptPartial(ctx context.Context, d capture.LogsData) (otlphttp.Partial, error) {
	if p, ok := (*s.cur.Load()).(otlphttp.PartialSink); ok {
		return p.AcceptPartial(ctx, d)
	}
	return otlphttp.Partial{}, (*s.cur.Load()).Accept(ctx, d)
}

// Every path a request can end on releases its reservation: after each, taken
// twice, a gzip request, which reserves the whole budget, is admitted.
func TestAdmissionReleasedOnEveryPath(t *testing.T) {
	const limit = 1 << 12
	good := marshalProto(t, sampleProto())
	zipped := gz(t, good)
	send := func(encoding string, length int64, body []byte) func(t *testing.T, h http.Handler) {
		return func(t *testing.T, h http.Handler) { post(t, h, encoding, length, body) }
	}
	errSink := func(err error) otlphttp.Sink { return &recorder{err: err} }
	paths := []struct {
		name string
		sink otlphttp.Sink
		run  func(t *testing.T, h http.Handler)
	}{
		{"success", &recorder{}, send("", int64(len(good)), good)},
		{"partial success", &partialSink{p: otlphttp.Partial{Rejected: 1, Message: "x"}}, send("", int64(len(good)), good)},
		{"busy", errSink(otlphttp.ErrBusy), send("", int64(len(good)), good)},
		{"refused", errSink(&otlphttp.RefusedError{Message: "no"}), send("", int64(len(good)), good)},
		{"unavailable", errSink(&otlphttp.UnavailableError{Message: "off"}), send("", int64(len(good)), good)},
		{"internal", errSink(errors.New("disk")), send("", int64(len(good)), good)},
		{"malformed", &recorder{}, send("", 3, []byte{0xff, 0xff, 0xff})},
		{"bad gzip", &recorder{}, send("gzip", 4, []byte("nope"))},
		{"inflates over the limit", &recorder{}, send("gzip", -1, gzFast(t, make([]byte, 4*limit)))},
		{"chunked over the limit", &recorder{}, send("", -1, make([]byte, 2*limit))},
		{"declared over the limit", &recorder{}, send("", 2*limit, make([]byte, 2*limit))},
		{"unsupported type", &recorder{}, func(t *testing.T, h http.Handler) { do(t, h, request{ctype: "text/plain", body: good}) }},
		{"body read fails", &recorder{}, func(t *testing.T, h http.Handler) {
			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, otlphttp.Path,
				io.MultiReader(bytes.NewReader(good[:5]), failingReader{}))
			req.ContentLength = int64(len(good))
			req.Header.Set("Content-Type", protoType)
			h.ServeHTTP(httptest.NewRecorder(), req)
		}},
		{"sink panics", panicSink{}, func(t *testing.T, h http.Handler) {
			defer func() {
				if recover() == nil {
					t.Error("the sink's panic did not reach the caller")
				}
			}()
			post(t, h, "", int64(len(good)), good)
		}},
	}
	for _, p := range paths {
		t.Run(p.name, func(t *testing.T) {
			sw := &switchSink{}
			h := otlphttp.Handler(sw, otlphttp.Options{MaxBodyBytes: limit, MaxInFlightBytes: limit})
			sw.set(p.sink)
			p.run(t, h)
			p.run(t, h)
			sw.set(&recorder{})
			if rec, _ := post(t, h, "gzip", int64(len(zipped)), zipped); rec.Code != http.StatusOK {
				t.Fatalf("a request reserving the whole budget after the path: status %d, body %q", rec.Code, rec.Body.String())
			}
		})
	}
}

// Without a budget the handler admits every request, as before.
func TestAdmissionOff(t *testing.T) {
	body := marshalProto(t, sampleProto())
	sink := newHoldSink()
	h := otlphttp.Handler(sink, otlphttp.Options{MaxBodyBytes: 1 << 20})
	done := holding(t, h, sink, "gzip", -1, gz(t, body))
	if rec, _ := post(t, h, "gzip", -1, gz(t, body)); rec.Code != http.StatusOK {
		t.Errorf("status %d, want 200", rec.Code)
	}
	if rec := done(); rec.Code != http.StatusOK {
		t.Errorf("held request: status %d", rec.Code)
	}
}
