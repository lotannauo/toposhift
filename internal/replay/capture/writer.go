package capture

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

// WriterOptions configures a [Writer].
type WriterOptions struct {
	// MaxLine bounds the length of one line in bytes, not counting its
	// newline. Zero means [DefaultMaxLine], the bound a [Reader] applies by
	// default, so a writer with the default never writes a line a default
	// reader refuses. A longer line is refused, not truncated.
	MaxLine int
}

// Writer writes the lines of a capture. It buffers; the caller must call
// Flush. It is not safe for concurrent use.
type Writer struct {
	bw     *bufio.Writer
	signal Signal
	max    int
}

// NewWriter writes a capture of signal s to w. It returns an error if s is not
// one of [Logs], [Metrics] and [Traces].
func NewWriter(w io.Writer, s Signal, opts WriterOptions) (*Writer, error) {
	if s != Logs && s != Metrics && s != Traces {
		return nil, fmt.Errorf("capture: invalid signal %d", uint8(s))
	}
	limit := opts.MaxLine
	if limit <= 0 {
		limit = DefaultMaxLine
	}
	return &Writer{bw: bufio.NewWriter(w), signal: s, max: limit}, nil
}

// WriteRaw writes one line as given, then a newline. It refuses a line that
// contains a newline, that is longer than the writer's MaxLine, or that a
// [Reader] would refuse: not valid UTF-8, not a JSON object, ending in a
// carriage return, starting with a byte order mark, or not a line of the
// writer's signal. Refusals wrap [ErrFormat] and write nothing.
func (w *Writer) WriteRaw(raw json.RawMessage) error {
	if len(raw) > w.max {
		return formatErrorf(0, nil, "line is %d bytes, longer than the limit of %d", len(raw), w.max)
	}
	if bytes.IndexByte(raw, '\n') >= 0 {
		return formatErrorf(0, nil, "line contains a newline")
	}
	sig, err := parseLine(raw)
	if err != nil {
		return err
	}
	if sig != w.signal {
		return formatErrorf(0, nil, "%s line in a %s capture: a file holds exactly one type of data", sig, w.signal)
	}
	if _, err := w.bw.Write(raw); err != nil {
		return err
	}
	return w.bw.WriteByte('\n')
}

// WriteLogs encodes d with [EncodeLogs] and writes it as one line, under the
// same rules as [Writer.WriteRaw]. It refuses a writer whose signal is not
// [Logs].
func (w *Writer) WriteLogs(d LogsData) error {
	if w.signal != Logs {
		return fmt.Errorf("capture: WriteLogs on a %s writer", w.signal)
	}
	raw, err := EncodeLogs(d)
	if err != nil {
		return err
	}
	return w.WriteRaw(raw)
}

// Flush writes buffered lines to the underlying writer.
func (w *Writer) Flush() error { return w.bw.Flush() }
