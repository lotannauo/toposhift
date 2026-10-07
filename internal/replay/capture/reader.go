package capture

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
)

// DefaultMaxLine is the longest line a [Reader] accepts when
// [ReaderOptions.MaxLine] is zero. One collector batch can be large.
const DefaultMaxLine = 64 << 20 // 64 MiB

// ReaderOptions configures a [Reader].
type ReaderOptions struct {
	// MaxLine bounds the length of one line in bytes, not counting its
	// newline. Zero means [DefaultMaxLine]. A longer line is an error.
	MaxLine int
}

// Reader reads the lines of a capture. It is not safe for concurrent use.
type Reader struct {
	br     *bufio.Reader
	max    int
	buf    []byte
	number int
	start  int64 // offset of the line being read
	offset int64 // bytes consumed so far
	signal Signal
	err    error // sticky: the error that ended the read
}

// NewReader reads a capture from r.
func NewReader(r io.Reader, opts ReaderOptions) *Reader {
	limit := opts.MaxLine
	if limit <= 0 {
		limit = DefaultMaxLine
	}
	return &Reader{br: bufio.NewReaderSize(r, 64<<10), max: limit}
}

// Signal is the capture's signal once a line has been read, and 0 before.
func (r *Reader) Signal() Signal { return r.signal }

// Next returns the next line, or io.EOF at the end. Every other error carries
// the line number, and a format error wraps [ErrFormat]. After an error, Next
// returns that error again: a capture is not read past a bad line.
//
// A line must end in "\n" (the last line may omit it), must not be empty, must
// be valid UTF-8 and a JSON object, and must hold the same signal as the
// first line: the file exporter's specification says a file contains exactly
// one type of data. Unknown top-level keys are ignored.
// A UTF-8 byte order mark is an error, not skipped. Each [Line] carries the
// byte offset of its start, so a caller that keeps small summaries of lines
// can read a line again by seeking to its offset and reading with a new
// Reader.
func (r *Reader) Next() (Line, error) {
	if r.err != nil {
		return Line{}, r.err
	}
	line, err := r.next()
	if err != nil {
		r.err = err
		return Line{}, err
	}
	return line, nil
}

func (r *Reader) next() (Line, error) {
	content, err := r.readLine()
	if err != nil {
		return Line{}, err
	}
	n := r.number
	sig, err := parseLine(content)
	if err != nil {
		return Line{}, withLine(n, err)
	}
	if r.signal == 0 {
		r.signal = sig
	} else if sig != r.signal {
		return Line{}, formatErrorf(n, nil, "%s line in a %s capture: a file holds exactly one type of data", sig, r.signal)
	}
	return Line{Number: n, Offset: r.start, Signal: sig, Raw: bytes.Clone(content)}, nil
}

// readLine reads up to the next newline and returns the line without it. The
// result is valid until the next call. It returns io.EOF only when no byte
// was read.
func (r *Reader) readLine() ([]byte, error) {
	r.buf = r.buf[:0]
	r.start = r.offset
	for {
		chunk, err := r.br.ReadSlice('\n')
		r.buf = append(r.buf, chunk...)
		switch {
		case err == nil:
			r.number++
			r.offset += int64(len(r.buf))
			content := r.buf[:len(r.buf)-1]
			if len(content) > r.max {
				return nil, r.tooLong()
			}
			return content, nil
		case errors.Is(err, bufio.ErrBufferFull):
			if len(r.buf) > r.max {
				r.number++
				return nil, r.tooLong()
			}
		case errors.Is(err, io.EOF):
			if len(r.buf) == 0 {
				return nil, io.EOF
			}
			r.number++
			r.offset += int64(len(r.buf))
			if len(r.buf) > r.max {
				return nil, r.tooLong()
			}
			return r.buf, nil
		default:
			return nil, fmt.Errorf("line %d: %w", r.number+1, err)
		}
	}
}

func (r *Reader) tooLong() error {
	return formatErrorf(r.number, nil, "longer than the limit of %d bytes", r.max)
}
