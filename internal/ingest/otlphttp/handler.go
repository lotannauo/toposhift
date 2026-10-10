package otlphttp

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
	"unicode/utf8"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	logspb "go.opentelemetry.io/proto/otlp/logs/v1"

	"github.com/lotannauo/toposhift/internal/replay/capture"
)

// Path is the path the handler serves, which is the OTLP/HTTP path of logs.
const Path = "/v1/logs"

// DefaultMaxBodyBytes is the body limit when [Options.MaxBodyBytes] is not set,
// 64 MiB. The package documentation says why it is so large.
const DefaultMaxBodyBytes = 64 << 20

// MaxStatusMessageBytes bounds the message of every google.rpc.Status the
// handler writes, in bytes. A longer message is cut at a UTF-8 rune boundary and
// ends with an ellipsis; the whole message, ellipsis included, is at most
// MaxStatusMessageBytes bytes.
const MaxStatusMessageBytes = 1024

// ellipsis ends a status message that was cut.
const ellipsis = "…"

// The Retry-After values of a 503, in seconds. A busy sink is expected to
// have room again almost at once. A sink that is off is off until an operator
// acts, so the client is asked to wait longer.
const (
	retryAfterBusy        = "1"
	retryAfterUnavailable = "30"
)

// ErrBusy is what a [Sink] returns when it cannot take the batch now and the
// client should send it again later. The handler answers 503 with Retry-After.
var ErrBusy = errors.New("otlphttp: sink busy")

// RefusedError is what a [Sink] returns to refuse the whole request: the
// handler answers 400 with a google.rpc.Status whose message is Message, and
// the client is expected not to send the request again. The message reaches the
// client, so it names the values the client can act on and nothing else. It is
// cut to [MaxStatusMessageBytes].
type RefusedError struct{ Message string }

func (e *RefusedError) Error() string {
	if e == nil {
		return "otlphttp: request refused"
	}
	return "otlphttp: request refused: " + e.Message
}

// UnavailableError is what a [Sink] returns when ingest is off: the handler
// answers 503 with Retry-After (30 seconds) and a google.rpc.Status whose
// message is Message. The message reaches the client, and is cut to
// [MaxStatusMessageBytes].
type UnavailableError struct{ Message string }

func (e *UnavailableError) Error() string {
	if e == nil {
		return "otlphttp: ingest unavailable"
	}
	return "otlphttp: ingest unavailable: " + e.Message
}

// Sink takes the logs of one request. It is called once per accepted request,
// on the goroutine of the request, and the request is answered when it
// returns. The error, which may be wrapped or joined, is mapped in this order:
//
//   - nil is 200;
//   - an [UnavailableError] is 503 with Retry-After: 30 and its message;
//   - [ErrBusy] is 503 with Retry-After: 1;
//   - a [RefusedError] is 400 with its message;
//   - any other error is 500, and its text stays in the server.
//
// A request the sink returned an error for is not accepted, and the client may
// send it again, so a sink must not keep part of a batch it refused.
//
// The precedence of ingest being off over a refusal belongs to the sink: a sink
// that is off returns an [UnavailableError] before it checks anything about the
// request, so a request that would be refused gets the 503 while ingest is off.
// The handler orders the mapping the same way, so an error that holds both
// (errors.Join) is also a 503.
type Sink interface {
	Accept(ctx context.Context, d capture.LogsData) error
}

// Options configures [Handler]. The zero value is usable.
type Options struct {
	// MaxBodyBytes limits the body: the bytes on the wire, and the bytes after
	// decompression. A body above it is answered with 413. Zero or negative is
	// [DefaultMaxBodyBytes].
	MaxBodyBytes int64
}

// Handler returns the handler of POST /v1/logs. It panics if s is nil.
func Handler(s Sink, o Options) http.Handler {
	if s == nil {
		panic("otlphttp: nil Sink")
	}
	if o.MaxBodyBytes <= 0 {
		o.MaxBodyBytes = DefaultMaxBodyBytes
	}
	return &handler{sink: s, limit: o.MaxBodyBytes}
}

type handler struct {
	sink  Sink
	limit int64
}

// kind is the encoding of a body.
type kind uint8

const (
	kindProto kind = iota + 1
	kindJSON
)

const (
	contentProto = "application/x-protobuf"
	contentJSON  = "application/json"
)

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != Path {
		http.Error(w, "not found: logs are received at "+Path, http.StatusNotFound)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed: use POST", http.StatusMethodNotAllowed)
		return
	}
	k, ok := bodyKind(r.Header.Get("Content-Type"))
	if !ok {
		// The body is not read, so the answer is in the format the client
		// most likely understands.
		writeStatus(w, kindProto, http.StatusUnsupportedMediaType,
			"unsupported Content-Type: use "+contentProto+" or "+contentJSON)
		return
	}
	gz, ok := bodyGzip(r.Header.Get("Content-Encoding"))
	if !ok {
		writeStatus(w, k, http.StatusUnsupportedMediaType, "unsupported Content-Encoding: use gzip or none")
		return
	}
	if r.ContentLength > h.limit {
		writeStatus(w, k, http.StatusRequestEntityTooLarge, h.tooLarge())
		return
	}
	body, status, err := h.readBody(w, r, gz)
	if err != nil {
		writeStatus(w, k, status, err.Error())
		return
	}
	d, err := decodeBody(k, body)
	if err != nil {
		writeStatus(w, k, http.StatusBadRequest, "malformed request body: "+err.Error())
		return
	}
	if err := h.sink.Accept(r.Context(), d); err != nil {
		h.writeSinkError(w, k, err)
		return
	}
	writeSuccess(w, k)
}

// writeSinkError answers the error a sink returned. The order is the one
// documented on [Sink]: ingest off, busy, refused, and then the rest. A nil
// *UnavailableError or *RefusedError, which errors.As matches, has no message
// and is an error like any other: 500.
func (h *handler) writeSinkError(w http.ResponseWriter, k kind, err error) {
	var unavailable *UnavailableError
	var refused *RefusedError
	switch {
	case errors.As(err, &unavailable) && unavailable != nil:
		w.Header().Set("Retry-After", retryAfterUnavailable)
		writeStatus(w, k, http.StatusServiceUnavailable, unavailable.Message)
	case errors.Is(err, ErrBusy):
		w.Header().Set("Retry-After", retryAfterBusy)
		writeStatus(w, k, http.StatusServiceUnavailable, "busy: send the request again later")
	case errors.As(err, &refused) && refused != nil:
		writeStatus(w, k, http.StatusBadRequest, refused.Message)
	default:
		writeStatus(w, k, http.StatusInternalServerError, "internal error")
	}
}

func (h *handler) tooLarge() string {
	return fmt.Sprintf("request body above the limit of %d bytes", h.limit)
}

// bodyKind returns the encoding a Content-Type names, if it is a supported one.
// Parameters (charset=utf-8) are ignored.
func bodyKind(contentType string) (kind, bool) {
	mt, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		return 0, false
	}
	switch mt {
	case contentProto:
		return kindProto, true
	case contentJSON:
		return kindJSON, true
	}
	return 0, false
}

// bodyGzip reports whether a Content-Encoding is gzip, and whether it is
// supported at all (gzip, identity, or none).
func bodyGzip(contentEncoding string) (isGzip, ok bool) {
	switch strings.ToLower(strings.TrimSpace(contentEncoding)) {
	case "", "identity":
		return false, true
	case "gzip":
		return true, true
	}
	return false, false
}

// readBody reads the request body, decompressing it if gz, and returns at most
// h.limit bytes. It reads at most h.limit bytes from the wire (the same limit
// as for the decompressed bytes: a client has no reason to send more than it
// would send uncompressed) and produces at most h.limit bytes from them, so
// neither a large body nor a small compressed one that inflates to a large one
// costs more than the limit. The buffer grows with what arrives and not with
// the declared length. On failure it returns the status to answer with.
func (h *handler) readBody(w http.ResponseWriter, r *http.Request, gz bool) ([]byte, int, error) {
	var src io.Reader = http.MaxBytesReader(w, r.Body, h.limit)
	if gz {
		zr, err := gzip.NewReader(src)
		if err != nil {
			return nil, readStatus(err), h.readError(err)
		}
		defer zr.Close()
		src = zr
	}
	data, err := readUpTo(src, h.limit)
	if err != nil {
		return nil, readStatus(err), h.readError(err)
	}
	if int64(len(data)) == h.limit {
		// Exactly the limit has been read. One byte more, or an error, tells
		// whether the body ends here; the read to its end also checks the
		// trailer of a gzip stream.
		var probe [1]byte
		n, err := io.ReadFull(src, probe[:])
		if n > 0 {
			return nil, http.StatusRequestEntityTooLarge, errors.New(h.tooLarge())
		}
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, readStatus(err), h.readError(err)
		}
	}
	return data, 0, nil
}

// initialBuffer is the size a body's buffer starts at. A request is held to
// what it sends, not to what it says it will send: the length a client declares
// is not trusted, so a client that claims the limit and sends nothing costs this
// much and no more.
const initialBuffer = 64 << 10

// readUpTo reads r to its end or until it has limit bytes, and returns what it
// read. The buffer starts at [initialBuffer] and doubles up to the limit, so a
// body of the limit costs less than twice the limit in all, however the reader
// delivers it.
func readUpTo(r io.Reader, limit int64) ([]byte, error) {
	buf := make([]byte, 0, min(initialBuffer, limit))
	for int64(len(buf)) < limit {
		if len(buf) == cap(buf) {
			buf = append(make([]byte, 0, min(2*int64(cap(buf)), limit)), buf...)
		}
		n, err := r.Read(buf[len(buf):cap(buf)])
		buf = buf[:len(buf)+n]
		if errors.Is(err, io.EOF) {
			return buf, nil
		}
		if err != nil {
			return nil, err
		}
	}
	return buf, nil
}

// readStatus is the status for an error while reading the body: 413 when the
// wire limit was hit, 400 for everything else (a gzip stream that is not valid,
// or a connection that ended early).
func readStatus(err error) int {
	var mbe *http.MaxBytesError
	if errors.As(err, &mbe) {
		return http.StatusRequestEntityTooLarge
	}
	return http.StatusBadRequest
}

func (h *handler) readError(err error) error {
	if readStatus(err) == http.StatusRequestEntityTooLarge {
		return errors.New(h.tooLarge())
	}
	if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
		return errors.New("malformed request body: the body ended early")
	}
	return fmt.Errorf("malformed request body: %w", err)
}

// The depth limits. A value of the model nests as deep as the sender writes it,
// and what costs time and memory later is the depth, not the size: decoding
// and encoding JSON get slower faster than the body grows as it nests deeper.
// Real records nest far less than these (a Kubernetes object nests about 13
// values deep, 53 levels of JSON).
const (
	// maxValueDepth is how deep the values of a record may nest, in both
	// encodings. A scalar is 1, and each array or key-value list adds 1.
	maxValueDepth = 32
	// maxJSONDepth is how deep a JSON body may nest, as brackets, checked
	// before the body is decoded. It leaves room for values of maxValueDepth
	// (four levels each when they are key-value lists, and a few to reach them).
	maxJSONDepth = 160
	// maxProtoDepth is the same for messages of a protobuf body, whose levels
	// are more numerous than values: three messages per key-value list.
	maxProtoDepth = 160
)

// decodeBody decodes a body of the given encoding into the model.
func decodeBody(k kind, body []byte) (capture.LogsData, error) {
	var d capture.LogsData
	switch k {
	case kindProto:
		var msg logspb.LogsData
		opts := proto.UnmarshalOptions{DiscardUnknown: true, RecursionLimit: maxProtoDepth}
		if err := opts.Unmarshal(body, &msg); err != nil {
			return capture.LogsData{}, err
		}
		var err error
		if d, err = convertLogs(&msg); err != nil {
			return capture.LogsData{}, err
		}
	case kindJSON:
		// A trailing newline or carriage return is the client's, not part of
		// the message; capture lines refuse the carriage return.
		body = bytes.TrimRight(body, " \t\r\n")
		if isEmptyObject(body) {
			// The empty request, as the empty protobuf body is.
			return capture.LogsData{}, nil
		}
		if jsonTooDeep(body, maxJSONDepth) {
			return capture.LogsData{}, fmt.Errorf("JSON nested deeper than %d levels", maxJSONDepth)
		}
		var err error
		if d, err = capture.DecodeLogs(body); err != nil {
			return capture.LogsData{}, err
		}
	default:
		return capture.LogsData{}, fmt.Errorf("unknown encoding %d", k)
	}
	if err := checkDepth(d); err != nil {
		return capture.LogsData{}, err
	}
	return d, nil
}

// isEmptyObject reports whether b is {} with optional whitespace. b has no
// trailing whitespace.
func isEmptyObject(b []byte) bool {
	b = bytes.TrimLeft(b, " \t\r\n")
	if len(b) == 0 || b[0] != '{' {
		return false
	}
	return len(bytes.TrimLeft(b[1:], " \t\r\n")) == 1 && b[len(b)-1] == '}'
}

// jsonTooDeep reports whether the brackets of b nest deeper than limit. It
// scans once, skipping strings, and stops at the first excess, so its cost is
// linear in the body whatever the nesting. It does not validate; the decoder
// does.
func jsonTooDeep(b []byte, limit int) bool {
	depth := 0
	inString := false
	for i := 0; i < len(b); i++ {
		c := b[i]
		if inString {
			switch c {
			case '\\':
				i++
			case '"':
				inString = false
			}
			continue
		}
		switch c {
		case '"':
			inString = true
		case '{', '[':
			depth++
			if depth > limit {
				return true
			}
		case '}', ']':
			if depth > 0 {
				depth--
			}
		}
	}
	return false
}

// checkDepth refuses a batch with a value that nests deeper than maxValueDepth.
func checkDepth(d capture.LogsData) error {
	check := func(kvs []capture.KeyValue) error {
		for _, kv := range kvs {
			if valueDepth(kv.Value) > maxValueDepth {
				return fmt.Errorf("the value of attribute %s nests deeper than %d levels", snippet(kv.Key), maxValueDepth)
			}
		}
		return nil
	}
	for _, rl := range d.ResourceLogs {
		if err := check(rl.Resource.Attributes); err != nil {
			return err
		}
		for _, sl := range rl.ScopeLogs {
			if err := check(sl.Scope.Attributes); err != nil {
				return err
			}
			for _, lr := range sl.LogRecords {
				if lr.Body != nil && valueDepth(*lr.Body) > maxValueDepth {
					return fmt.Errorf("a log record body nests deeper than %d levels", maxValueDepth)
				}
				if err := check(lr.Attributes); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// valueDepth is the nesting of a value: 1 for a scalar, and 1 more than its
// deepest element for an array or a key-value list.
func valueDepth(v capture.AnyValue) int {
	deepest := 0
	switch {
	case v.ArrayValue != nil:
		for _, e := range v.ArrayValue.Values {
			deepest = max(deepest, valueDepth(e))
		}
	case v.KVListValue != nil:
		for _, kv := range v.KVListValue.Values {
			deepest = max(deepest, valueDepth(kv.Value))
		}
	}
	return 1 + deepest
}

// writeSuccess writes the empty ExportLogsServiceResponse.
func writeSuccess(w http.ResponseWriter, k kind) {
	if k == kindJSON {
		w.Header().Set("Content-Type", contentJSON)
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "{}")
		return
	}
	w.Header().Set("Content-Type", contentProto)
	w.WriteHeader(http.StatusOK)
}

// rpcCode is the google.rpc.Code of an HTTP status, as the OTLP specification
// maps them.
func rpcCode(status int) int32 {
	switch status {
	case http.StatusRequestEntityTooLarge:
		return 8 // RESOURCE_EXHAUSTED
	case http.StatusServiceUnavailable:
		return 14 // UNAVAILABLE
	case http.StatusInternalServerError:
		return 13 // INTERNAL
	default:
		return 3 // INVALID_ARGUMENT
	}
}

// boundMessage returns msg unchanged if it has at most [MaxStatusMessageBytes]
// bytes. Otherwise it returns the longest prefix that ends on a rune boundary
// and leaves room for the ellipsis, followed by the ellipsis, so the result has
// at most [MaxStatusMessageBytes] bytes. msg must be valid UTF-8.
func boundMessage(msg string) string {
	if len(msg) <= MaxStatusMessageBytes {
		return msg
	}
	cut := MaxStatusMessageBytes - len(ellipsis)
	for cut > 0 && !utf8.RuneStart(msg[cut]) {
		cut--
	}
	return msg[:cut] + ellipsis
}

// writeStatus answers a refusal with a google.rpc.Status in the encoding of the
// request. The message is made valid UTF-8 (a run of invalid bytes becomes one
// U+FFFD, since a protobuf string must be valid UTF-8 and a refusal can name a
// value the client chose) and then bounded, whatever its source: the sink's, or
// the text of a decoder error. The order matters: replacing bytes can make the
// message longer.
func writeStatus(w http.ResponseWriter, k kind, status int, msg string) {
	code := rpcCode(status)
	msg = boundMessage(strings.ToValidUTF8(msg, "\uFFFD"))
	var body []byte
	if k == kindJSON {
		w.Header().Set("Content-Type", contentJSON)
		body, _ = json.Marshal(struct {
			Code    int32  `json:"code"`
			Message string `json:"message"`
		}{code, msg})
	} else {
		w.Header().Set("Content-Type", contentProto)
		body = protowire.AppendTag(body, 1, protowire.VarintType)
		body = protowire.AppendVarint(body, uint64(code))
		body = protowire.AppendTag(body, 2, protowire.BytesType)
		body = protowire.AppendString(body, msg)
	}
	w.WriteHeader(status)
	_, _ = w.Write(body)
}
