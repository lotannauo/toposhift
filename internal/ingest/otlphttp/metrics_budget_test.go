package otlphttp_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/ingest/otlphttp"
)

// The logs route and the metrics route count their requests against one
// budget: a request held in one makes the other busy, in both directions, and
// the busy one is refused at once, without its body being read.
func TestOneBudgetForBothRoutes(t *testing.T) {
	logsBody := marshalProto(t, sampleProto())
	metricsBody := marshalProto(t, sampleMetricsProto())

	t.Run("a held logs request blocks metrics", func(t *testing.T) {
		n := int64(len(logsBody))
		budget := otlphttp.NewBudget(n)
		logsSink := newHoldSink()
		logs := otlphttp.Handler(logsSink, otlphttp.Options{MaxBodyBytes: 1 << 20, Budget: budget})
		metricsSink := &metricsRecorder{}
		metrics := otlphttp.MetricsHandler(metricsSink, otlphttp.Options{MaxBodyBytes: 1 << 20, Budget: budget})

		done := holding(t, logs, logsSink, "", n, logsBody)
		rec, read := postPath(t, metrics, otlphttp.MetricsPath, int64(len(metricsBody)), metricsBody)
		checkBusy(t, rec, read)
		if metricsSink.calls() != 0 {
			t.Error("the metrics sink was called")
		}
		if rec := done(); rec.Code != http.StatusOK {
			t.Fatalf("the held request: status %d", rec.Code)
		}
		rec, _ = postPath(t, metrics, otlphttp.MetricsPath, int64(len(metricsBody)), metricsBody)
		if rec.Code != http.StatusOK {
			t.Fatalf("after the release: status %d, body %q", rec.Code, rec.Body.String())
		}
	})

	t.Run("a held metrics request blocks logs", func(t *testing.T) {
		n := int64(len(metricsBody))
		budget := otlphttp.NewBudget(n)
		metricsSink := newHoldMetricsSink()
		metrics := otlphttp.MetricsHandler(metricsSink, otlphttp.Options{MaxBodyBytes: 1 << 20, Budget: budget})
		logsSink := &recorder{}
		logs := otlphttp.Handler(logsSink, otlphttp.Options{MaxBodyBytes: 1 << 20, Budget: budget})

		finished := make(chan int, 1)
		go func() {
			rec, _ := postPath(t, metrics, otlphttp.MetricsPath, n, metricsBody)
			finished <- rec.Code
		}()
		<-metricsSink.entered
		rec, read := post(t, logs, "", int64(len(logsBody)), logsBody)
		checkBusy(t, rec, read)
		if logsSink.calls() != 0 {
			t.Error("the logs sink was called")
		}
		close(metricsSink.release)
		select {
		case code := <-finished:
			if code != http.StatusOK {
				t.Fatalf("the held request: status %d", code)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("the held request was not answered")
		}
		if rec, _ := post(t, logs, "", int64(len(logsBody)), logsBody); rec.Code != http.StatusOK {
			t.Fatalf("after the release: status %d", rec.Code)
		}
	})

	t.Run("a budget replaces MaxInFlightBytes, and none is no bound", func(t *testing.T) {
		n := int64(len(logsBody))
		budget := otlphttp.NewBudget(2*n + 100)
		sink := newHoldSink()
		// MaxInFlightBytes would refuse the second request; the budget does not.
		h := otlphttp.Handler(sink, otlphttp.Options{MaxBodyBytes: 1 << 20, MaxInFlightBytes: 1, Budget: budget})
		done := holding(t, h, sink, "", n, logsBody)
		if rec, _ := post(t, h, "", n, logsBody); rec.Code != http.StatusOK {
			t.Errorf("second request: status %d, want 200", rec.Code)
		}
		done()
		if b := otlphttp.NewBudget(0); b != nil {
			t.Errorf("NewBudget(0) = %v, want none", b)
		}
	})
}
