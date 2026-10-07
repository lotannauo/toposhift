package capture

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"unicode/utf8"
)

// ErrFormat is wrapped by every error that reports a capture which does not
// follow the format.
var ErrFormat = errors.New("invalid capture")

// Signal is the kind of telemetry a capture holds.
type Signal uint8

// The three signals the file exporter supports. A capture holds exactly one.
const (
	Logs Signal = iota + 1
	Metrics
	Traces
)

// String returns "logs", "metrics" or "traces".
func (s Signal) String() string {
	switch s {
	case Logs:
		return "logs"
	case Metrics:
		return "metrics"
	case Traces:
		return "traces"
	default:
		return "signal(" + strconv.Itoa(int(s)) + ")"
	}
}

// signalKeys are the top-level keys that give a line its signal.
var signalKeys = [...]struct {
	key    string
	signal Signal
}{
	{"resourceLogs", Logs},
	{"resourceMetrics", Metrics},
	{"resourceSpans", Traces},
}

// Line is one line of a capture file as it was read.
type Line struct {
	Number int             // 1-based line number in the file
	Offset int64           // byte offset of the line's first byte in the stream
	Signal Signal          // the line's signal
	Raw    json.RawMessage // the line's bytes, without the newline, exactly as read
}

// formatError is an error that wraps [ErrFormat], optionally at a line.
type formatError struct {
	line int // 0 when the error is not about one line of a file
	msg  string
	err  error // the underlying cause, if any
}

func (e *formatError) Error() string {
	if e.line > 0 {
		return fmt.Sprintf("line %d: %s", e.line, e.msg)
	}
	return e.msg
}

func (e *formatError) Unwrap() []error {
	if e.err != nil {
		return []error{ErrFormat, e.err}
	}
	return []error{ErrFormat}
}

func formatErrorf(line int, cause error, format string, args ...any) error {
	return &formatError{line: line, msg: fmt.Sprintf(format, args...), err: cause}
}

var utf8BOM = []byte{0xef, 0xbb, 0xbf}

// present is a JSON value of which only the presence is wanted. Decoding it
// reads nothing, so a map of them records a line's top-level keys without
// copying the values, which can be megabytes.
type present struct{}

func (*present) UnmarshalJSON([]byte) error { return nil }

// isJSONSpace reports whether c is whitespace between JSON tokens.
func isJSONSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\r' || c == '\n'
}

// parseLine checks one line (without its newline) and finds its signal. The
// returned error has no line number; callers add it.
func parseLine(raw []byte) (Signal, error) {
	if len(raw) > 0 && raw[len(raw)-1] == '\r' {
		return 0, formatErrorf(0, nil, `line ends in "\r\n"; the line separator is "\n"`)
	}
	first := -1
	for i, c := range raw {
		if !isJSONSpace(c) {
			first = i
			break
		}
	}
	if first < 0 {
		return 0, formatErrorf(0, nil, "empty line: every line must be a JSON object")
	}
	if !utf8.Valid(raw) {
		return 0, formatErrorf(0, nil, "invalid UTF-8")
	}
	if bytes.HasPrefix(raw[first:], utf8BOM) {
		return 0, formatErrorf(0, nil, "starts with a UTF-8 byte order mark; a capture has none")
	}
	if raw[first] != '{' {
		return 0, formatErrorf(0, nil, "not a JSON object")
	}
	var top map[string]present
	if err := json.Unmarshal(raw, &top); err != nil {
		return 0, formatErrorf(0, err, "invalid JSON: %v", err)
	}
	var found Signal
	for _, k := range signalKeys {
		if _, ok := top[k.key]; !ok {
			continue
		}
		if found != 0 {
			return 0, formatErrorf(0, nil, "both %q and %q: a line holds one signal", signalKey(found), k.key)
		}
		found = k.signal
	}
	if found == 0 {
		return 0, formatErrorf(0, nil, "none of resourceLogs, resourceMetrics, resourceSpans")
	}
	return found, nil
}

func signalKey(s Signal) string {
	for _, k := range signalKeys {
		if k.signal == s {
			return k.key
		}
	}
	return ""
}

// withLine adds a line number to an error from parseLine.
func withLine(n int, err error) error {
	var fe *formatError
	if errors.As(err, &fe) {
		return &formatError{line: n, msg: fe.msg, err: fe.err}
	}
	return fmt.Errorf("line %d: %w", n, err)
}
