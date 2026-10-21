// Package s3fake is an S3-compatible server in the test process, for tests of
// code that talks to an object store. It is a test aid: nothing outside tests
// imports it.
//
// It speaks the five calls package s3store makes (PutObject, GetObject,
// HeadObject, ListObjectsV2 and DeleteObject), path-style, over plain HTTP on a
// loopback address, and it is strict where a real service is:
//
//   - Every request's AWS Signature Version 4 is recomputed from the request as
//     received, by code written separately from the client's, and a request
//     whose signature differs is refused with 403 SignatureDoesNotMatch. So are
//     a wrong access key, region or service, a request time more than 15 minutes
//     from the server's clock, signed header names out of order, and an x-amz-*
//     header that is not signed.
//   - A PutObject body is checked against the payload hash that was signed. An
//     object appears only once its whole body has arrived and matches, so a
//     body cut short or altered stores nothing.
//   - PutObject with If-None-Match: * is atomic, and answers 412 for a name that
//     exists. [IgnoreIfNoneMatch] models a service that ignores the header.
//   - x-amz-meta-* headers are stored and returned by HeadObject and GetObject.
//   - ListObjectsV2 honours prefix, max-keys and continuation-token, and
//     answers in ascending byte order of key, as a real one does.
//
// It can also misbehave on request: see [Fault]. Every request it receives is
// recorded for assertions: see [Server.Requests].
//
// It is a model written from the documentation. That the client and the fake
// agree shows that the client does what the documentation says, not that a
// service does: only a request to the service shows that.
package s3fake

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Defaults of a [Server].
const (
	DefaultAccessKeyID     = "FAKEACCESSKEYID"
	DefaultSecretAccessKey = "fake-secret-access-key-for-tests-only"
	DefaultRegion          = "auto"
	DefaultBucket          = "test-bucket"
	defaultMaxObject       = 512 << 20
	defaultPageLimit       = 1000
	signatureSkew          = 15 * time.Minute
)

type config struct {
	accessKey, secret, region, bucket string
	now                               func() time.Time
	maxObject                         int64
	pageLimit                         int
	ignoreIfNoneMatch                 bool
}

// Option configures [New].
type Option func(*config)

// WithCredentials sets the access key id and the secret the server accepts.
func WithCredentials(accessKeyID, secret string) Option {
	return func(c *config) { c.accessKey, c.secret = accessKeyID, secret }
}

// WithRegion sets the region the credential scope must name.
func WithRegion(region string) Option { return func(c *config) { c.region = region } }

// WithBucket sets the one bucket the server holds.
func WithBucket(bucket string) Option { return func(c *config) { c.bucket = bucket } }

// WithClock sets the server's clock, which the request time is held to.
func WithClock(now func() time.Time) Option { return func(c *config) { c.now = now } }

// WithMaxObjectSize makes the server refuse a body longer than n bytes.
func WithMaxObjectSize(n int64) Option { return func(c *config) { c.maxObject = n } }

// WithPageLimit caps the max-keys of a listing, as a service's own limit does.
func WithPageLimit(n int) Option { return func(c *config) { c.pageLimit = n } }

// IgnoreIfNoneMatch makes PutObject disregard If-None-Match, as a service that
// does not implement conditional writes does: a second Put replaces the object.
func IgnoreIfNoneMatch() Option { return func(c *config) { c.ignoreIfNoneMatch = true } }

type object struct {
	data []byte
	meta map[string]string // lower-case name without the x-amz-meta- prefix
	time time.Time
}

// Server is a running fake. The zero value is not usable; call [New].
type Server struct {
	cfg config
	srv *httptest.Server
	v   verifier

	done    chan struct{}
	stalled chan struct{}

	mu       sync.Mutex
	objects  map[string]*object
	faults   []*fault
	requests []Request
}

// New starts a server on a loopback address. Close it when done.
func New(opts ...Option) *Server {
	cfg := config{
		accessKey: DefaultAccessKeyID,
		secret:    DefaultSecretAccessKey,
		region:    DefaultRegion,
		bucket:    DefaultBucket,
		now:       time.Now,
		maxObject: defaultMaxObject,
		pageLimit: defaultPageLimit,
	}
	for _, o := range opts {
		o(&cfg)
	}
	s := &Server{
		cfg:     cfg,
		done:    make(chan struct{}),
		stalled: make(chan struct{}, 64),
		objects: map[string]*object{},
		v: verifier{
			accessKey:         cfg.accessKey,
			secret:            cfg.secret,
			region:            cfg.region,
			service:           "s3",
			requireContentSHA: true,
			skew:              signatureSkew,
		},
	}
	s.srv = httptest.NewServer(http.HandlerFunc(s.handle))
	return s
}

// URL is the server's endpoint, http://127.0.0.1:port.
func (s *Server) URL() string { return s.srv.URL }

// Credentials are the access key id and the secret the server accepts.
func (s *Server) Credentials() (accessKeyID, secret string) { return s.cfg.accessKey, s.cfg.secret }

// Region is the region the server expects in a credential scope.
func (s *Server) Region() string { return s.cfg.region }

// Bucket is the name of the bucket the server holds.
func (s *Server) Bucket() string { return s.cfg.bucket }

// Close stops the server, releasing any request stalled by a fault.
func (s *Server) Close() {
	select {
	case <-s.done:
	default:
		close(s.done)
	}
	s.srv.Close()
}

// Stalled yields once each time a [Stall] fault holds a request.
func (s *Server) Stalled() <-chan struct{} { return s.stalled }

// Seed stores an object directly, as if somebody else had written it. meta is
// the user metadata by name, without the x-amz-meta- prefix.
func (s *Server) Seed(key string, data []byte, meta map[string]string) {
	m := map[string]string{}
	for k, v := range meta {
		m[strings.ToLower(k)] = v
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects[key] = &object{data: slices.Clone(data), meta: m, time: s.cfg.now()}
}

// Get returns a copy of an object's bytes and metadata.
func (s *Server) Get(key string) (data []byte, meta map[string]string, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.objects[key]
	if !ok {
		return nil, nil, false
	}
	meta = map[string]string{}
	for k, v := range o.meta {
		meta[k] = v
	}
	return slices.Clone(o.data), meta, true
}

// Keys lists the keys of every object, in ascending order.
func (s *Server) Keys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]string, 0, len(s.objects))
	for k := range s.objects {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// Request is what the server saw of one request.
type Request struct {
	Method string
	// Op is PutObject, GetObject, HeadObject, ListObjectsV2 or DeleteObject, or
	// empty for a request that is none of them or was refused before it was
	// recognized.
	Op  string
	Key string
	// Path and Query are as the client sent them, still encoded.
	Path, Query string
	Host        string
	Header      http.Header
	// BodyLen is the number of body bytes the server read.
	BodyLen int64
	// Status is the HTTP status answered; 0 when the connection was dropped
	// without an answer.
	Status int
	// Fault is the kind of the [Fault] applied, if any.
	Fault FaultKind
	// AuthCode is the error code of a request refused for its credentials or
	// signature, and AuthNote the canonical request the server computed for one
	// refused for its signature (to compare with the client's, in a test log).
	AuthCode, AuthNote string
}

// Requests returns every request received so far, oldest first.
func (s *Server) Requests() []Request {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.requests)
}

// ResetRequests forgets the requests received so far.
func (s *Server) ResetRequests() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests = nil
}

// Count returns how many requests of the operation (see [Request.Op]) were
// received.
func (s *Server) Count(op string) int {
	n := 0
	for _, r := range s.Requests() {
		if r.Op == op {
			n++
		}
	}
	return n
}

// AuthFailures returns the requests refused for their credentials or signature.
func (s *Server) AuthFailures() []Request {
	var out []Request
	for _, r := range s.Requests() {
		if r.AuthCode != "" {
			out = append(out, r)
		}
	}
	return out
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(p)
}

func (w *statusWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// drain reads a request's body to its end, so that an error answer reaches a
// client that is still sending, instead of a broken pipe.
func drain(r *http.Request) { _, _ = io.Copy(io.Discard, r.Body) }

// abort drops the connection without finishing the answer.
func abort() { panic(http.ErrAbortHandler) }

func (s *Server) handle(rw http.ResponseWriter, r *http.Request) {
	rawPath, rawQuery, _ := strings.Cut(r.RequestURI, "?")
	rec := &Request{
		Method: r.Method,
		Path:   rawPath,
		Query:  rawQuery,
		Host:   r.Host,
		Header: r.Header.Clone(),
	}
	w := &statusWriter{ResponseWriter: rw}
	defer func() {
		rec.Status = w.status
		s.mu.Lock()
		s.requests = append(s.requests, *rec)
		s.mu.Unlock()
	}()

	declared, aerr := s.v.verify(r, s.cfg.now())
	if aerr != nil {
		rec.AuthCode, rec.AuthNote = aerr.code, aerr.note
		writeError(w, r, aerr.status, aerr.code, aerr.message)
		return
	}

	decoded, err := url.PathUnescape(rawPath)
	if err != nil || !strings.HasPrefix(decoded, "/") {
		writeError(w, r, http.StatusBadRequest, "InvalidURI", "Couldn't parse the specified URI.")
		return
	}
	bucket, key, _ := strings.Cut(decoded[1:], "/")
	if bucket != s.cfg.bucket {
		writeError(w, r, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist")
		return
	}

	switch {
	case key != "" && r.Method == http.MethodPut:
		rec.Op = "PutObject"
	case key != "" && r.Method == http.MethodGet:
		rec.Op = "GetObject"
	case key != "" && r.Method == http.MethodHead:
		rec.Op = "HeadObject"
	case key != "" && r.Method == http.MethodDelete:
		rec.Op = "DeleteObject"
	case key == "" && r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2":
		rec.Op = "ListObjectsV2"
	default:
		writeError(w, r, http.StatusNotImplemented, "NotImplemented", "A header or query you provided requested a function that is not implemented.")
		return
	}
	rec.Key = key
	if rec.Op != "PutObject" && declared != unsignedPayload && declared != emptyHash {
		// A request without a body must say so, in the one way there is.
		writeError(w, r, http.StatusBadRequest, "XAmzContentSHA256Mismatch", "The provided 'x-amz-content-sha256' header does not match what was computed.")
		return
	}

	f := s.takeFault(rec.Op, key)
	if f != nil {
		rec.Fault = f.Kind
	}
	if f != nil {
		switch f.Kind {
		case Fail500:
			drain(r)
			writeError(w, r, http.StatusInternalServerError, "InternalError", "We encountered an internal error. Please try again.")
			return
		case SlowDown:
			drain(r)
			writeError(w, r, http.StatusServiceUnavailable, "SlowDown", "Please reduce your request rate.")
			return
		case Drop:
			abort()
		}
	}

	switch rec.Op {
	case "PutObject":
		s.put(w, r, rec, key, declared, f)
	case "GetObject", "HeadObject":
		s.get(w, r, key, f)
	case "DeleteObject":
		s.mu.Lock()
		delete(s.objects, key)
		s.mu.Unlock()
		if f != nil && f.Kind == DropAfterStore {
			abort()
		}
		w.WriteHeader(http.StatusNoContent)
	case "ListObjectsV2":
		s.list(w, r)
	}
}

func (s *Server) put(w *statusWriter, r *http.Request, rec *Request, key, declared string, f *fault) {
	if r.ContentLength < 0 {
		writeError(w, r, http.StatusLengthRequired, "MissingContentLength", "You must provide the Content-Length HTTP header.")
		return
	}
	if r.ContentLength > s.cfg.maxObject {
		writeError(w, r, http.StatusBadRequest, "EntityTooLarge", "Your proposed upload exceeds the maximum allowed size")
		return
	}
	if cond := r.Header.Get("If-None-Match"); cond != "" && cond != "*" {
		writeError(w, r, http.StatusBadRequest, "InvalidArgument", "only If-None-Match: * is supported")
		return
	}
	if strings.HasPrefix(declared, "STREAMING-") {
		writeError(w, r, http.StatusNotImplemented, "NotImplemented", "streaming payload signatures are not supported")
		return
	}

	body := make([]byte, r.ContentLength)
	if f != nil && f.Kind == Stall {
		// Take the first bytes, then hold the request, not reading, until the
		// client gives up or the server closes.
		n := min(len(body), 64<<10)
		if _, err := io.ReadFull(r.Body, body[:n]); err != nil {
			abort()
		}
		s.stalled <- struct{}{}
		<-s.done
		abort()
	}
	n, err := io.ReadFull(r.Body, body)
	rec.BodyLen = int64(n)
	if err != nil {
		// The body ended before its Content-Length: nothing is stored.
		abort()
	}
	if declared != unsignedPayload {
		sum := sha256.Sum256(body)
		if hex.EncodeToString(sum[:]) != declared {
			writeError(w, r, http.StatusBadRequest, "XAmzContentSHA256Mismatch", "The provided 'x-amz-content-sha256' header does not match what was computed.")
			return
		}
	}

	meta := map[string]string{}
	for name, vals := range r.Header {
		if suffix, ok := strings.CutPrefix(strings.ToLower(name), "x-amz-meta-"); ok && len(vals) > 0 {
			meta[suffix] = vals[0]
		}
	}
	s.mu.Lock()
	if _, exists := s.objects[key]; exists && r.Header.Get("If-None-Match") == "*" && !s.cfg.ignoreIfNoneMatch {
		s.mu.Unlock()
		writeError(w, r, http.StatusPreconditionFailed, "PreconditionFailed", "At least one of the pre-conditions you specified did not hold")
		return
	}
	s.objects[key] = &object{data: body, meta: meta, time: s.cfg.now()}
	s.mu.Unlock()

	if f != nil && f.Kind == DropAfterStore {
		abort() // the object is stored; the answer is lost
	}
	w.Header().Set("ETag", `"`+declared[:min(len(declared), 32)]+`"`)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) get(w *statusWriter, r *http.Request, key string, f *fault) {
	s.mu.Lock()
	o, ok := s.objects[key]
	var data []byte
	var meta map[string]string
	if ok {
		data = o.data // objects are never modified in place
		meta = o.meta
	}
	s.mu.Unlock()
	if !ok {
		writeError(w, r, http.StatusNotFound, "NoSuchKey", "The specified key does not exist.")
		return
	}
	h := w.Header()
	h.Set("Content-Type", "application/octet-stream")
	h.Set("Content-Length", strconv.Itoa(len(data)))
	h.Set("Last-Modified", o.time.UTC().Format(http.TimeFormat))
	for k, v := range meta {
		if k == "sha256" && f != nil && f.Kind == NoDigest {
			continue
		}
		h.Set("X-Amz-Meta-"+k, v)
	}
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}
	if f != nil {
		switch f.Kind {
		case CorruptBody:
			if len(data) > 0 {
				data = slices.Clone(data)
				data[len(data)/2] ^= 0xff
			}
		case TruncateBody:
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(data[:len(data)/2])
			w.Flush()
			abort()
		case Stall:
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(data[:len(data)/2])
			w.Flush()
			s.stalled <- struct{}{}
			select {
			case <-r.Context().Done():
			case <-s.done:
			}
			abort()
		}
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

type listResult struct {
	XMLName     xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListBucketResult"`
	Name        string
	Prefix      string
	KeyCount    int
	MaxKeys     int
	IsTruncated bool
	Contents    []listItem
	NextToken   string `xml:"NextContinuationToken,omitempty"`
}

type listItem struct {
	Key          string
	LastModified string
	Size         int
	StorageClass string
}

func (s *Server) list(w *statusWriter, r *http.Request) {
	q := r.URL.Query()
	prefix := q.Get("prefix")
	limit := s.cfg.pageLimit
	if v := q.Get("max-keys"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			writeError(w, r, http.StatusBadRequest, "InvalidArgument", "Argument maxKeys must be an integer between 1 and 2147483647")
			return
		}
		limit = min(n, s.cfg.pageLimit)
	}
	after := ""
	if t := q.Get("continuation-token"); t != "" {
		b, err := base64.RawURLEncoding.DecodeString(t)
		if err != nil {
			writeError(w, r, http.StatusBadRequest, "InvalidArgument", "The continuation token provided is incorrect")
			return
		}
		after = string(b)
	}

	s.mu.Lock()
	keys := make([]string, 0, len(s.objects))
	for k := range s.objects {
		if strings.HasPrefix(k, prefix) && (after == "" || k > after) {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)
	res := listResult{Name: s.cfg.bucket, Prefix: prefix, MaxKeys: limit}
	if len(keys) > limit {
		keys = keys[:limit]
		res.IsTruncated = true
		res.NextToken = base64.RawURLEncoding.EncodeToString([]byte(keys[len(keys)-1]))
	}
	for _, k := range keys {
		o := s.objects[k]
		res.Contents = append(res.Contents, listItem{
			Key:          k,
			LastModified: o.time.UTC().Format("2006-01-02T15:04:05.000Z"),
			Size:         len(o.data),
			StorageClass: "STANDARD",
		})
	}
	s.mu.Unlock()
	res.KeyCount = len(res.Contents)

	var buf bytes.Buffer
	buf.WriteString(xml.Header)
	if err := xml.NewEncoder(&buf).Encode(res); err != nil {
		writeError(w, r, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set("Content-Length", strconv.Itoa(buf.Len()))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(buf.Bytes())
}

// writeError answers in the form of an S3 error: an XML document, except for
// HeadObject, whose answer has no body.
func writeError(w http.ResponseWriter, r *http.Request, status int, code, message string) {
	if r.Method == http.MethodHead {
		w.WriteHeader(status)
		return
	}
	var msg bytes.Buffer
	_ = xml.EscapeText(&msg, []byte(message))
	body := fmt.Sprintf("%s<Error><Code>%s</Code><Message>%s</Message><RequestId>fake</RequestId></Error>", xml.Header, code, msg.String())
	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(status)
	_, _ = io.WriteString(w, body)
}
