package capture_test

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/internal/replay/capture"
)

func newWriter(t fataler, w io.Writer, s capture.Signal) *capture.Writer {
	t.Helper()
	cw, err := capture.NewWriter(w, s, capture.WriterOptions{})
	if err != nil {
		t.Fatalf("NewWriter(%v): %v", s, err)
	}
	return cw
}

func TestNewWriterRefusesInvalidSignals(t *testing.T) {
	t.Parallel()
	for _, s := range []capture.Signal{0, 4, 255} {
		var out bytes.Buffer
		if w, err := capture.NewWriter(&out, s, capture.WriterOptions{}); err == nil || w != nil {
			t.Errorf("NewWriter(%d) = %v, %v; want an error", uint8(s), w, err)
		}
	}
}

func TestWriterRawRefusals(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		signal capture.Signal
		raw    string
	}{
		{"newline inside", capture.Logs, "{\"resourceLogs\":[],\n\"x\":1}"},
		{"trailing newline", capture.Logs, logsLine + "\n"},
		{"another signal", capture.Logs, metricsLine},
		{"another signal, metrics writer", capture.Metrics, tracesLine},
		{"invalid JSON", capture.Logs, `{"resourceLogs":[}`},
		{"not an object", capture.Logs, `[{"resourceLogs":[]}]`},
		{"empty", capture.Logs, ``},
		{"blank", capture.Logs, `  `},
		{"no signal", capture.Logs, `{}`},
		{"two signals", capture.Logs, `{"resourceLogs":[],"resourceSpans":[]}`},
		{"invalid UTF-8", capture.Logs, "{\"resourceLogs\":[],\"x\":\"\xff\"}"},
		{"ends in a carriage return", capture.Logs, logsLine + "\r"},
		{"byte order mark", capture.Logs, "\xef\xbb\xbf" + logsLine},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var out bytes.Buffer
			w := newWriter(t, &out, tc.signal)
			err := w.WriteRaw([]byte(tc.raw))
			if err == nil {
				t.Fatalf("WriteRaw(%q) succeeded, want an error", tc.raw)
			}
			if !errors.Is(err, capture.ErrFormat) {
				t.Errorf("error %q does not wrap ErrFormat", err)
			}
			if err := w.Flush(); err != nil || out.Len() != 0 {
				t.Errorf("a refused line wrote %q (flush error %v)", out.Bytes(), err)
			}
		})
	}
}

func TestWriterLines(t *testing.T) {
	t.Parallel()
	var out bytes.Buffer
	w := newWriter(t, &out, capture.Metrics)
	for _, raw := range []string{metricsLine, ` {"resourceMetrics":[{"x":"<&>"}]}`} {
		if err := w.WriteRaw([]byte(raw)); err != nil {
			t.Fatalf("WriteRaw(%s): %v", raw, err)
		}
	}
	if out.Len() != 0 {
		t.Errorf("wrote %q before Flush, want buffering", out.Bytes())
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	want := metricsLine + "\n" + ` {"resourceMetrics":[{"x":"<&>"}]}` + "\n"
	if out.String() != want {
		t.Errorf("output = %q, want %q", out.String(), want)
	}
}

func TestWriteLogsOnOtherWriter(t *testing.T) {
	t.Parallel()
	for _, s := range []capture.Signal{capture.Metrics, capture.Traces} {
		var out bytes.Buffer
		w := newWriter(t, &out, s)
		if err := w.WriteLogs(capture.LogsData{ResourceLogs: []capture.ResourceLogs{{}}}); err == nil {
			t.Errorf("WriteLogs on a %s writer succeeded", s)
		}
		_ = w.Flush()
		if out.Len() != 0 {
			t.Errorf("%s writer wrote %q", s, out.Bytes())
		}
	}
}

func TestWriteLogsEscaping(t *testing.T) {
	t.Parallel()
	html := "<a href=\"x\">&</a>"
	s := html
	d := capture.LogsData{ResourceLogs: []capture.ResourceLogs{{
		Resource: capture.Resource{Attributes: []capture.KeyValue{{Key: "k<>&", Value: capture.AnyValue{StringValue: &s}}}},
		ScopeLogs: []capture.ScopeLogs{{
			Scope: capture.InstrumentationScope{Name: "<scope>"},
			LogRecords: []capture.LogRecord{{
				SeverityText: "<&>",
				EventName:    "a&b",
				Body: &capture.AnyValue{ArrayValue: &capture.ArrayValue{Values: []capture.AnyValue{
					{KVListValue: &capture.KeyValueList{Values: []capture.KeyValue{{Key: "<", Value: capture.AnyValue{StringValue: &s}}}}},
				}}},
			}},
		}},
	}}}
	var out bytes.Buffer
	w := newWriter(t, &out, capture.Logs)
	if err := w.WriteLogs(d); err != nil {
		t.Fatalf("WriteLogs: %v", err)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	got := out.String()
	if strings.Contains(got, `\u0026`) || strings.Contains(got, `\u003c`) || strings.Contains(got, `\u003e`) {
		t.Errorf("output escapes <, > or &: %s", got)
	}
	for _, want := range []string{`"k<>&"`, `"<scope>"`, `"<&>"`, `"a&b"`, `"<a href=\"x\">&</a>"`} {
		if !strings.Contains(got, want) {
			t.Errorf("output lacks %s: %s", want, got)
		}
	}
	if !strings.HasSuffix(got, "}\n") || strings.Count(got, "\n") != 1 {
		t.Errorf("output is not one newline-terminated line: %q", got)
	}
	back, err := capture.DecodeLogs([]byte(strings.TrimSuffix(got, "\n")))
	if err != nil {
		t.Fatalf("DecodeLogs: %v", err)
	}
	if got := back.ResourceLogs[0].ScopeLogs[0].LogRecords[0].SeverityText; got != "<&>" {
		t.Errorf("SeverityText = %q", got)
	}
}

func TestWriteLogsLineEndings(t *testing.T) {
	t.Parallel()
	var out bytes.Buffer
	w := newWriter(t, &out, capture.Logs)
	text := "line one\nline two\r\nend "
	d := capture.LogsData{ResourceLogs: []capture.ResourceLogs{{ScopeLogs: []capture.ScopeLogs{{
		LogRecords: []capture.LogRecord{{SeverityText: text}, {SeverityText: text}},
	}}}}}
	for range 3 {
		if err := w.WriteLogs(d); err != nil {
			t.Fatalf("WriteLogs: %v", err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if n := strings.Count(out.String(), "\n"); n != 3 {
		t.Errorf("%d newlines for 3 lines: %q", n, out.String())
	}
	lines, err := readAll(capture.NewReader(&out, capture.ReaderOptions{}))
	if err != nil || len(lines) != 3 {
		t.Fatalf("read back %d lines, error %v", len(lines), err)
	}
	back, err := capture.DecodeLogs(lines[0].Raw)
	if err != nil || back.ResourceLogs[0].ScopeLogs[0].LogRecords[0].SeverityText != text {
		t.Errorf("text did not survive: %v", err)
	}
}

// TestWriteLogsEmpty checks that an encoded value is always a line a reader
// accepts, even with no resources.
func TestWriteLogsEmpty(t *testing.T) {
	t.Parallel()
	var out bytes.Buffer
	w := newWriter(t, &out, capture.Logs)
	if err := w.WriteLogs(capture.LogsData{}); err != nil {
		t.Fatalf("WriteLogs: %v", err)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got, want := out.String(), `{"resourceLogs":[]}`+"\n"; got != want {
		t.Errorf("output = %q, want %q", got, want)
	}
	if _, err := readAll(capture.NewReader(&out, capture.ReaderOptions{})); err != nil {
		t.Errorf("read back: %v", err)
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("disk full") }

func TestWriterFlushError(t *testing.T) {
	t.Parallel()
	w := newWriter(t, failingWriter{}, capture.Logs)
	if err := w.WriteRaw([]byte(logsLine)); err != nil {
		t.Fatalf("WriteRaw: %v", err)
	}
	if err := w.Flush(); err == nil {
		t.Errorf("Flush succeeded on a failing writer")
	}
}

func TestWriterMaxLine(t *testing.T) {
	t.Parallel()
	line := func(n int) string { // a logs line of n bytes
		base := `{"resourceLogs":[],"x":""}`
		return base[:len(base)-2] + strings.Repeat("a", n-len(base)) + base[len(base)-2:]
	}
	var out bytes.Buffer
	w, err := capture.NewWriter(&out, capture.Logs, capture.WriterOptions{MaxLine: 40})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.WriteRaw([]byte(line(40))); err != nil {
		t.Errorf("a line of exactly MaxLine: %v", err)
	}
	for _, n := range []int{41, 100} {
		err := w.WriteRaw([]byte(line(n)))
		if err == nil || !errors.Is(err, capture.ErrFormat) || !strings.Contains(err.Error(), "limit") {
			t.Errorf("a line of %d bytes: error = %v, want a format error about the limit", n, err)
		}
	}
	// WriteLogs is bound the same way.
	long := strings.Repeat("a", 50)
	d := capture.LogsData{ResourceLogs: []capture.ResourceLogs{{SchemaURL: long}}}
	if err := w.WriteLogs(d); err == nil || !errors.Is(err, capture.ErrFormat) {
		t.Errorf("WriteLogs of a long line: %v", err)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if want := line(40) + "\n"; out.String() != want {
		t.Errorf("output = %q, want only the line that fit", out.String())
	}
	// The limit does not count the newline: a reader with the same limit
	// reads what was written.
	lines, err := readAll(capture.NewReader(&out, capture.ReaderOptions{MaxLine: 40}))
	if err != nil || len(lines) != 1 {
		t.Errorf("read back %d lines: %v", len(lines), err)
	}
}

// TestWriterDefaultMaxLine checks the default writer never writes a line the
// default reader refuses.
func TestWriterDefaultMaxLine(t *testing.T) {
	t.Parallel()
	w, err := capture.NewWriter(io.Discard, capture.Logs, capture.WriterOptions{})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	prefix, suffix := `{"resourceLogs":[],"x":"`, `"}`
	fill := func(n int) []byte {
		return []byte(prefix + strings.Repeat("a", n-len(prefix)-len(suffix)) + suffix)
	}
	if err := w.WriteRaw(fill(capture.DefaultMaxLine)); err != nil {
		t.Errorf("a line of exactly DefaultMaxLine: %v", err)
	}
	if err := w.WriteRaw(fill(capture.DefaultMaxLine + 1)); err == nil || !errors.Is(err, capture.ErrFormat) {
		t.Errorf("a line of DefaultMaxLine+1: error = %v", err)
	}
}
