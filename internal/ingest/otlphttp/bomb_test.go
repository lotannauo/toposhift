package otlphttp_test

import (
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"testing"

	"github.com/lotannauo/toposhift/internal/ingest/otlphttp"
)

// A small gzip stream that inflates to far more than the limit is refused with
// 413 after the handler has produced the limit and no more. The stream is 1 GiB
// of zeros, about a MiB on the wire and so within the limit on the wire, and
// many times the limit decompressed. A handler that inflated it all would read
// all of it and hold a GiB; this one reads the few tens of KiB that inflate to
// the limit, and holds a small multiple of the limit.
func TestGzipBombIsNotInflated(t *testing.T) {
	const total = 1 << 30
	for _, c := range []struct {
		name  string
		limit int64 // 0 is the default
	}{
		{"default limit", 0},
		{"8 MiB", 8 << 20},
	} {
		t.Run(c.name, func(t *testing.T) {
			limit := c.limit
			if limit == 0 {
				limit = otlphttp.DefaultMaxBodyBytes
			}
			pr, pw := io.Pipe()
			done := make(chan struct{})
			go func() {
				defer close(done)
				zw := gzip.NewWriter(pw)
				_, err := io.CopyN(zw, zeros{}, total)
				if err == nil {
					err = zw.Close()
				}
				pw.CloseWithError(err)
			}()

			wire := &countingReader{r: pr}
			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, otlphttp.Path, wire)
			req.ContentLength = -1
			req.Header.Set("Content-Type", protoType)
			req.Header.Set("Content-Encoding", "gzip")
			h, sink := newHandler(otlphttp.Options{MaxBodyBytes: c.limit})
			rec := httptest.NewRecorder()
			var before, after runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&before)
			h.ServeHTTP(rec, req)
			runtime.ReadMemStats(&after)
			pr.CloseWithError(io.ErrClosedPipe) // lets the writer end
			<-done

			if rec.Code != http.StatusRequestEntityTooLarge {
				t.Fatalf("status %d, want 413", rec.Code)
			}
			// Zeros deflate about 1000 to 1, so the limit inflates from limit/1000
			// bytes of the stream; the rest is the buffers of the readers.
			if bound := limit/256 + 32<<10; wire.n > bound {
				t.Errorf("%d compressed bytes were read before the refusal, want at most %d: the stream was inflated further than the limit", wire.n, bound)
			}
			// Holding the limit costs a small multiple of it as the buffer grows.
			// The stream is many times the limit, so inflating it all would cost
			// far more than this.
			if alloc, bound := after.TotalAlloc-before.TotalAlloc, uint64(3*limit); alloc > bound {
				t.Errorf("the request allocated %d bytes for a limit of %d, want at most %d", alloc, limit, bound)
			}
			if sink.calls() != 0 {
				t.Error("the sink was called")
			}
		})
	}
}
