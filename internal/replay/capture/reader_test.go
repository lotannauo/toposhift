package capture_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/internal/replay/capture"
)

const (
	logsLine    = `{"resourceLogs":[]}`
	metricsLine = `{"resourceMetrics":[]}`
	tracesLine  = `{"resourceSpans":[]}`
)

// readAll reads every line, returning them and the error that ended the read
// (nil for a clean end).
func readAll(r *capture.Reader) ([]capture.Line, error) {
	var lines []capture.Line
	for {
		line, err := r.Next()
		if errors.Is(err, io.EOF) {
			return lines, nil
		}
		if err != nil {
			return lines, err
		}
		lines = append(lines, line)
	}
}

func TestReaderGolden(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile("testdata/logs_example.jsonl")
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	r := capture.NewReader(bytes.NewReader(data), capture.ReaderOptions{})
	if got := r.Signal(); got != 0 {
		t.Errorf("Signal before reading = %v, want 0", got)
	}
	lines, err := readAll(r)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(lines) != 2 {
		t.Fatalf("read %d lines, want 2", len(lines))
	}
	var out bytes.Buffer
	w := newWriter(t, &out, capture.Logs)
	for i, line := range lines {
		if line.Number != i+1 || line.Signal != capture.Logs {
			t.Errorf("line %d = number %d, signal %v", i, line.Number, line.Signal)
		}
		if err := w.WriteRaw(line.Raw); err != nil {
			t.Fatalf("WriteRaw: %v", err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if !bytes.Equal(out.Bytes(), data) {
		t.Errorf("rewriting Raw gives a different file:\n got %q\nwant %q", out.Bytes(), data)
	}
	if got := r.Signal(); got != capture.Logs {
		t.Errorf("Signal = %v, want logs", got)
	}
}

func TestReaderAccepts(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   string
		want []string
	}{
		{"empty file", ``, nil},
		{"one line", logsLine + "\n", []string{logsLine}},
		{"no final newline", logsLine + "\n" + logsLine, []string{logsLine, logsLine}},
		{"single line without newline", logsLine, []string{logsLine}},
		{"unknown top-level key", `{"resourceLogs":[],"other":{"a":[1,2]}}` + "\n", []string{`{"resourceLogs":[],"other":{"a":[1,2]}}`}},
		{"unknown nested keys", `{"resourceLogs":[{"resource":{"x":1},"scopeLogs":[{"y":true}]}]}` + "\n", []string{`{"resourceLogs":[{"resource":{"x":1},"scopeLogs":[{"y":true}]}]}`}},
		{"signal key null", `{"resourceLogs":null}` + "\n", []string{`{"resourceLogs":null}`}},
		{"leading space kept", ` {"resourceLogs":[]} ` + "\n", []string{` {"resourceLogs":[]} `}},
		{"non-ASCII", `{"resourceLogs":[],"x":"héllo ☃"}` + "\n", []string{`{"resourceLogs":[],"x":"héllo ☃"}`}},
		{"metrics", metricsLine + "\n" + metricsLine + "\n", []string{metricsLine, metricsLine}},
		{"traces", tracesLine + "\n", []string{tracesLine}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			lines, err := readAll(capture.NewReader(strings.NewReader(tc.in), capture.ReaderOptions{}))
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			var got []string
			for i, l := range lines {
				got = append(got, string(l.Raw))
				if l.Number != i+1 {
					t.Errorf("line %d has number %d", i, l.Number)
				}
			}
			if strings.Join(got, "\n") != strings.Join(tc.want, "\n") || len(got) != len(tc.want) {
				t.Errorf("lines = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestReaderSignals(t *testing.T) {
	t.Parallel()
	tests := []struct {
		line string
		want capture.Signal
	}{
		{logsLine, capture.Logs},
		{metricsLine, capture.Metrics},
		{tracesLine, capture.Traces},
	}
	for _, tc := range tests {
		r := capture.NewReader(strings.NewReader(tc.line+"\n"), capture.ReaderOptions{})
		line, err := r.Next()
		if err != nil {
			t.Fatalf("Next(%s): %v", tc.line, err)
		}
		if line.Signal != tc.want || r.Signal() != tc.want {
			t.Errorf("Next(%s) signal = %v, reader %v; want %v", tc.line, line.Signal, r.Signal(), tc.want)
		}
	}
}

func TestSignalString(t *testing.T) {
	t.Parallel()
	for s, want := range map[capture.Signal]string{
		capture.Logs: "logs", capture.Metrics: "metrics", capture.Traces: "traces", 0: "signal(0)",
	} {
		if got := s.String(); got != want {
			t.Errorf("Signal(%d).String() = %q, want %q", uint8(s), got, want)
		}
	}
}

func TestReaderErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		in       string
		maxLine  int
		wantLine int
		contains string
	}{
		{name: "CRLF", in: logsLine + "\r\n", wantLine: 1, contains: `\r\n`},
		{name: "CRLF on a later line", in: logsLine + "\n" + logsLine + "\r\n", wantLine: 2, contains: `\r\n`},
		{name: "CR before end of file", in: logsLine + "\r", wantLine: 1, contains: `\r\n`},
		{name: "empty line", in: logsLine + "\n\n" + logsLine + "\n", wantLine: 2, contains: "empty line"},
		{name: "empty first line", in: "\n" + logsLine + "\n", wantLine: 1, contains: "empty line"},
		{name: "whitespace line", in: logsLine + "\n \t \n", wantLine: 2, contains: "empty line"},
		{name: "invalid UTF-8", in: logsLine + "\n" + `{"resourceLogs":[],"x":"` + "\xff" + `"}` + "\n", wantLine: 2, contains: "UTF-8"},
		{name: "not an object", in: "[1]\n", wantLine: 1, contains: "not a JSON object"},
		{name: "byte order mark", in: "\xef\xbb\xbf" + logsLine + "\n", wantLine: 1, contains: "byte order mark"},
		{name: "byte order mark alone", in: "\xef\xbb\xbf\n", wantLine: 1, contains: "byte order mark"},
		{name: "byte order mark before space", in: "\xef\xbb\xbf " + logsLine + "\n", wantLine: 1, contains: "byte order mark"},
		{name: "byte order mark on a later line", in: logsLine + "\n\xef\xbb\xbf" + logsLine + "\n", wantLine: 2, contains: "byte order mark"},
		{name: "a string", in: `"resourceLogs"` + "\n", wantLine: 1, contains: "not a JSON object"},
		{name: "null", in: "null\n", wantLine: 1, contains: "not a JSON object"},
		{name: "invalid JSON", in: logsLine + "\n" + logsLine + "\n" + `{"resourceLogs":[}` + "\n", wantLine: 3, contains: "invalid JSON"},
		{name: "trailing data", in: logsLine + " " + logsLine + "\n", wantLine: 1, contains: "invalid JSON"},
		{name: "no signal key", in: `{"other":1}` + "\n", wantLine: 1, contains: "none of"},
		{name: "empty object", in: "{}\n", wantLine: 1, contains: "none of"},
		{name: "signal key in the wrong case", in: `{"ResourceLogs":[]}` + "\n", wantLine: 1, contains: "none of"},
		{name: "original field name", in: `{"resource_logs":[]}` + "\n", wantLine: 1, contains: "none of"},
		{name: "two signal keys", in: `{"resourceLogs":[],"resourceMetrics":[]}` + "\n", wantLine: 1, contains: "one signal"},
		{name: "mixed signals", in: logsLine + "\n" + logsLine + "\n" + metricsLine + "\n", wantLine: 3, contains: "one type of data"},
		{name: "mixed signals, traces then logs", in: tracesLine + "\n" + logsLine + "\n", wantLine: 2, contains: "one type of data"},
		{name: "line over the limit", in: logsLine + "\n" + `{"resourceLogs":[],"x":"` + strings.Repeat("a", 40) + `"}` + "\n", maxLine: 30, wantLine: 2, contains: "limit"},
		{name: "last line over the limit, no newline", in: `{"resourceLogs":[],"x":"` + strings.Repeat("a", 40) + `"}`, maxLine: 30, wantLine: 1, contains: "limit"},
		{name: "line over the limit across buffers", in: `{"resourceLogs":[],"x":"` + strings.Repeat("a", 200_000) + `"}` + "\n", maxLine: 100_000, wantLine: 1, contains: "limit"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := capture.NewReader(strings.NewReader(tc.in), capture.ReaderOptions{MaxLine: tc.maxLine})
			_, err := readAll(r)
			if err == nil {
				t.Fatalf("read succeeded, want an error")
			}
			if !errors.Is(err, capture.ErrFormat) {
				t.Errorf("error %q does not wrap ErrFormat", err)
			}
			if want := fmt.Sprintf("line %d: ", tc.wantLine); !strings.HasPrefix(err.Error(), want) {
				t.Errorf("error %q does not start with %q", err, want)
			}
			if !strings.Contains(err.Error(), tc.contains) {
				t.Errorf("error %q does not mention %q", err, tc.contains)
			}
			// The error is final.
			if _, again := r.Next(); again == nil || again.Error() != err.Error() {
				t.Errorf("Next after an error = %v, want the same error", again)
			}
		})
	}
}

func TestReaderMaxLineBoundary(t *testing.T) {
	t.Parallel()
	line := `{"resourceLogs":[],"x":"` + strings.Repeat("a", 6) + `"}`
	for _, tc := range []struct {
		name    string
		in      string
		maxLine int
		wantErr bool
	}{
		{"exactly the limit", line + "\n", len(line), false},
		{"exactly the limit, no newline", line, len(line), false},
		{"one over", line + "\n", len(line) - 1, true},
		{"one over, no newline", line, len(line) - 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := readAll(capture.NewReader(strings.NewReader(tc.in), capture.ReaderOptions{MaxLine: tc.maxLine}))
			if (err != nil) != tc.wantErr {
				t.Errorf("error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestReaderLongLine(t *testing.T) {
	t.Parallel()
	// Longer than the reader's buffer and than bufio.Scanner's default.
	line := `{"resourceLogs":[],"x":"` + strings.Repeat("é", 300_000) + `"}`
	lines, err := readAll(capture.NewReader(strings.NewReader(line+"\n"+logsLine+"\n"), capture.ReaderOptions{}))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(lines) != 2 || string(lines[0].Raw) != line || string(lines[1].Raw) != logsLine {
		t.Errorf("read %d lines, first of %d bytes", len(lines), len(lines[0].Raw))
	}
}

// TestReaderRawIsStable checks a returned line is not overwritten by later
// reads.
func TestReaderRawIsStable(t *testing.T) {
	t.Parallel()
	in := `{"resourceLogs":[],"n":1}` + "\n" + `{"resourceLogs":[],"n":2}` + "\n"
	lines, err := readAll(capture.NewReader(strings.NewReader(in), capture.ReaderOptions{}))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(lines[0].Raw) != `{"resourceLogs":[],"n":1}` {
		t.Errorf("first line = %q after reading the second", lines[0].Raw)
	}
}

type failingReader struct{ err error }

func (f failingReader) Read([]byte) (int, error) { return 0, f.err }

func TestReaderIOError(t *testing.T) {
	t.Parallel()
	boom := errors.New("boom")
	r := capture.NewReader(io.MultiReader(strings.NewReader(logsLine+"\n"), failingReader{boom}), capture.ReaderOptions{})
	if _, err := r.Next(); err != nil {
		t.Fatalf("first line: %v", err)
	}
	_, err := r.Next()
	if !errors.Is(err, boom) || !strings.HasPrefix(err.Error(), "line 2: ") {
		t.Errorf("error = %v, want line 2 wrapping the read error", err)
	}
	if errors.Is(err, capture.ErrFormat) {
		t.Errorf("a read error should not be a format error")
	}
}

func TestReaderOffsets(t *testing.T) {
	t.Parallel()
	long := `{"resourceLogs":[],"x":"` + strings.Repeat("é", 100_000) + `"}`
	lines := []string{logsLine, ` {"resourceLogs":[],"n":1} `, long, `{"resourceLogs":[],"x":"☃"}`, logsLine}
	for _, finalNewline := range []bool{true, false} {
		in := strings.Join(lines, "\n")
		if finalNewline {
			in += "\n"
		}
		got, err := readAll(capture.NewReader(strings.NewReader(in), capture.ReaderOptions{}))
		if err != nil || len(got) != len(lines) {
			t.Fatalf("read %d lines, error %v", len(got), err)
		}
		offset := 0
		for i, l := range got {
			if l.Offset != int64(offset) {
				t.Errorf("line %d offset = %d, want %d", i+1, l.Offset, offset)
			}
			if end := int(l.Offset) + len(l.Raw); in[l.Offset:end] != string(l.Raw) {
				t.Errorf("line %d is not at its offset", i+1)
			}
			offset += len(lines[i]) + 1
		}
	}
}

// TestReaderOffsetsGolden checks a line read again from its offset is the
// same line.
func TestReaderOffsetsGolden(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile("testdata/logs_example.jsonl")
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	lines, err := readAll(capture.NewReader(bytes.NewReader(data), capture.ReaderOptions{}))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	for _, l := range lines {
		again, err := capture.NewReader(bytes.NewReader(data[l.Offset:]), capture.ReaderOptions{}).Next()
		if err != nil || !bytes.Equal(again.Raw, l.Raw) {
			t.Errorf("line %d read at offset %d: %v", l.Number, l.Offset, err)
		}
	}
	if lines[0].Offset != 0 || lines[1].Offset != int64(len(lines[0].Raw)+1) {
		t.Errorf("offsets = %d, %d", lines[0].Offset, lines[1].Offset)
	}
}

func TestReaderDuplicateSignalKey(t *testing.T) {
	t.Parallel()
	// encoding/json accepts duplicate keys; they are one key.
	lines, err := readAll(capture.NewReader(strings.NewReader(`{"resourceLogs":[],"resourceLogs":[1]}`+"\n"), capture.ReaderOptions{}))
	if err != nil || len(lines) != 1 || lines[0].Signal != capture.Logs {
		t.Errorf("lines = %v, error %v", lines, err)
	}
}
