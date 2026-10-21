// Package s3store is a [segment.Store] in a bucket of an S3-compatible object
// store, reached over HTTPS with AWS Signature Version 4. It uses the standard
// library only: five calls (PutObject, GetObject, HeadObject, ListObjectsV2 and
// DeleteObject), path-style addressing, no multipart upload, no retries of its
// own.
//
// # Writing a segment
//
// Put copies the segment to a temporary file in [Config.SpoolDir], hashing it
// (SHA-256) and counting it as it goes, and only then uploads the file with one
// PutObject carrying Content-Length, the digest as the user metadata
// x-amz-meta-sha256 and the same digest as the signed payload hash. The store
// that receives it checks the body against the signed digest, so a body that
// arrives short or altered is refused; the object appears when the whole body
// has, so a segment is never visible half-written, and a context that ends
// mid-upload stores nothing. A segment is spooled rather than held in memory
// because one may be [segment.MaxSize] (256 MiB), and several Puts at once
// would multiply that.
//
// Put sends If-None-Match: *, so the store refuses to replace an object (HTTP
// 412) and Put reports [segment.ErrExists]; the check and the write are one
// atomic step where the service's documentation says so (R2's does). For a
// service that ignores the header, set [Config.NoConditionalPut]: Put then asks
// with HeadObject first and writes only if the object is absent. That is safe
// with one writer per bucket prefix and no more (the product has one writer per
// store); two processes could both find the name free. Within a process the
// store serializes such Puts.
//
// A Put that fails with a lost connection, a timeout or a 5xx may or may not
// have stored the segment. The store does not retry it. The caller retries, and
// when the retry gets [segment.ErrExists] it calls Stat and compares the SHA256
// with the digest of the bytes it meant to store (see [segment.Store]).
//
// # The digest
//
// Stat reads the digest from the object's metadata with a HeadObject, so it
// costs one request whatever the size. An object with no digest, or one that is
// not 64 hexadecimal digits, is an error and never a zero digest: such an
// object was not written by Put. Open checks the same header and then hashes
// what it reads: a body that is short, or whose SHA-256 differs, ends with
// [ErrDigestMismatch] instead of io.EOF. List leaves the digest zero, which the
// contract allows, since ListObjectsV2 does not return metadata.
//
// # Names and keys
//
// A segment named n has the key <Prefix>/<n>, or n when the prefix is empty.
// Objects under the prefix whose keys are not valid segment names (a folder
// marker made by a console, a file put there by hand) are not segments and are
// not listed. A bucket that does not exist reads, to Stat, as a segment that
// does not (a HeadObject answer has no body to tell them apart); the first Put
// fails loudly.
//
// # Credentials and addresses
//
// The access key id and the secret access key are passed in [Config]; a program
// reads them from the environment with [CredentialsFromEnv] and from nowhere
// else. The secret is a [Secret], which prints as "<redacted>", it is never put
// in an error, and it never leaves the process: requests carry only a
// signature. A plain http endpoint is accepted only for a loopback address,
// where tests run; anything else must be https, so that segments never cross a
// network unencrypted. Redirects are not followed.
//
// # What is verified, and what is not
//
// The signature code reproduces the examples AWS publishes, and the tests run
// the whole store against a server that checks signatures with code written
// separately (package s3fake). Nothing here has been run against R2 or B2:
// that R2's region is "auto", that a PutObject carrying user metadata and
// If-None-Match is accepted by each, and that ListObjectsV2 answers as modeled
// are the claims only a request to the real service can check.
package s3store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"iter"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/lotannauo/toposhift/internal/segment"
)

var _ segment.Store = (*Store)(nil)

const (
	// DefaultRegion is the region used when [Config.Region] is empty: the value R2
	// is expected to accept, which no request to R2 has yet confirmed. A service
	// that wants a real region is given one.
	DefaultRegion = "auto"

	// digestHeader is the user metadata that carries a segment's SHA-256, as 64
	// lower-case hexadecimal digits.
	digestHeader = "X-Amz-Meta-Sha256"

	// copyChunk is how much Put copies between checks of the context.
	copyChunk = 256 << 10

	maxPageSize = 1000
	// maxListBody bounds one page of a listing, which is 1000 keys of at most
	// 512 bytes each plus markup; a body larger than this is not a listing.
	maxListBody = 16 << 20
	// maxErrorBody bounds how much of an error response is read.
	maxErrorBody = 64 << 10
	// maxMessage bounds how much of a service's message an error repeats.
	maxMessage = 200
)

// ErrDigestMismatch is returned by the reader Open gives when the bytes it read
// are not the bytes the digest in the object's metadata was taken of: they are
// fewer, or their SHA-256 differs.
var ErrDigestMismatch = errors.New("s3store: the bytes read do not match the recorded SHA-256")

// ErrNoDigest is returned by Stat and Open for an object that has no valid
// sha256 user metadata.
var ErrNoDigest = errors.New("s3store: the object has no valid sha256 metadata")

// Config is how a [Store] is reached. Endpoint, Bucket, AccessKeyID,
// SecretAccessKey and SpoolDir are required.
type Config struct {
	// Endpoint is the service's address as scheme://host[:port], with no path.
	// It must be https, except for a loopback host.
	Endpoint string
	// Region is the signing region; empty means [DefaultRegion].
	Region string
	// Bucket is the bucket, addressed path-style (/bucket/key).
	Bucket string
	// Prefix is where in the bucket the segments live: empty, or elements as in a
	// segment name (lower-case letters, digits and . _ -, separated by single
	// slashes, no leading or trailing slash).
	Prefix string

	AccessKeyID     string
	SecretAccessKey Secret

	// SpoolDir is an existing directory, given as an absolute path, where Put
	// keeps the segment it is uploading; it needs room for one segment per Put
	// in flight. Files left behind by a crash are named s3store-put-*.tmp and
	// are safe to delete when no Put is running.
	SpoolDir string

	// NoConditionalPut makes Put ask with HeadObject, and write only if the
	// object is absent, instead of sending If-None-Match: *. See the package
	// documentation for when that is safe.
	NoConditionalPut bool

	// MaxSize is the largest segment Put accepts. Zero means [segment.MaxSize],
	// which is also the most it may be; a smaller value is for tests.
	MaxSize int64
	// PageSize is the max-keys List asks for. Zero leaves it to the service
	// (1,000 at most).
	PageSize int

	// HTTPClient sends the requests. Nil means a client with dial, TLS and
	// response-header timeouts, no overall timeout (a segment may be large; the
	// context bounds a call) and no redirects.
	HTTPClient *http.Client
	// Now is the clock the signatures use; nil means time.Now.
	Now func() time.Time
}

// Store is a [segment.Store] in a bucket. It is safe for concurrent use.
type Store struct {
	scheme, host string
	bucket       string
	prefix       string
	signer       signer
	client       *http.Client
	spool        string
	noCondPut    bool
	maxSize      int64
	pageSize     int
	now          func() time.Time

	// putMu serializes the check and the write of a Put in the NoConditionalPut
	// mode, so that two Puts in this process cannot both find the name free.
	putMu sync.Mutex
}

// New checks cfg and returns a Store. It makes no request and writes nothing.
// Its errors say which setting is wrong and never repeat the secret.
func New(cfg Config) (*Store, error) {
	u, err := url.Parse(cfg.Endpoint)
	if err != nil || u.Host == "" {
		return nil, errors.New("s3store: the endpoint is not a URL of the form scheme://host[:port]")
	}
	switch {
	case u.User != nil:
		return nil, errors.New("s3store: the endpoint must not carry credentials")
	case u.Path != "" && u.Path != "/", u.RawQuery != "", u.Fragment != "":
		return nil, errors.New("s3store: the endpoint must be scheme://host[:port] with no path or query")
	case u.Scheme != "https" && (u.Scheme != "http" || !isLoopback(u.Hostname())):
		return nil, errors.New("s3store: the endpoint must be https (http only for a loopback address)")
	}
	region := cfg.Region
	if region == "" {
		region = DefaultRegion
	}
	switch {
	case !simpleToken(region, "-"):
		return nil, errors.New("s3store: the region must be lower-case letters, digits and '-'")
	case !validBucket(cfg.Bucket):
		return nil, errors.New("s3store: the bucket name must be 3 to 63 characters of lower-case letters, digits, '.' and '-'")
	case cfg.Prefix != "" && segment.ValidName(cfg.Prefix) != nil:
		return nil, errors.New("s3store: the prefix must be elements of lower-case letters, digits and . _ - separated by single slashes")
	case !validAccessKeyID(cfg.AccessKeyID):
		return nil, errors.New("s3store: the access key id is empty or has a character other than printable ASCII without space, ',' and '/'")
	case cfg.SecretAccessKey == "":
		return nil, errors.New("s3store: the secret access key is empty")
	case cfg.MaxSize < 0 || cfg.MaxSize > segment.MaxSize:
		return nil, fmt.Errorf("s3store: MaxSize must be between 0 and %d", segment.MaxSize)
	case cfg.PageSize < 0 || cfg.PageSize > maxPageSize:
		return nil, fmt.Errorf("s3store: PageSize must be between 0 and %d", maxPageSize)
	}
	if !filepath.IsAbs(cfg.SpoolDir) {
		return nil, errors.New("s3store: the spool directory must be an absolute path")
	}
	if fi, err := os.Stat(cfg.SpoolDir); err != nil || !fi.IsDir() {
		return nil, errors.New("s3store: the spool directory does not exist or is not a directory")
	}
	s := &Store{
		scheme:    u.Scheme,
		host:      u.Host,
		bucket:    cfg.Bucket,
		prefix:    cfg.Prefix,
		signer:    signer{accessKey: cfg.AccessKeyID, secret: string(cfg.SecretAccessKey), region: region, service: "s3"},
		client:    cfg.HTTPClient,
		spool:     cfg.SpoolDir,
		noCondPut: cfg.NoConditionalPut,
		maxSize:   cfg.MaxSize,
		pageSize:  cfg.PageSize,
		now:       cfg.Now,
	}
	if s.client == nil {
		s.client = defaultClient()
	}
	if s.maxSize == 0 {
		s.maxSize = segment.MaxSize
	}
	if s.now == nil {
		s.now = time.Now
	}
	return s, nil
}

// String implements fmt.Stringer. It names the bucket and the prefix and holds
// nothing secret, so that a Store printed with any verb leaks nothing.
func (s *Store) String() string {
	return fmt.Sprintf("s3store.Store{bucket: %q, prefix: %q}", s.bucket, s.prefix)
}

// GoString implements fmt.GoStringer.
func (s *Store) GoString() string { return s.String() }

// Format implements fmt.Formatter for every verb, so that a verb that prints a
// pointer's struct (%d, for one) cannot reach the fields.
func (s *Store) Format(f fmt.State, _ rune) { _, _ = io.WriteString(f, s.String()) }

func defaultClient() *http.Client {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.DialContext = (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext
	t.TLSHandshakeTimeout = 10 * time.Second
	t.ResponseHeaderTimeout = 2 * time.Minute
	return &http.Client{
		Transport: t,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

func isLoopback(host string) bool {
	if host == "localhost" || strings.HasSuffix(host, ".localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// simpleToken reports whether s is non-empty and made of lower-case letters,
// digits and the bytes in extra.
func simpleToken(s, extra string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		lower, digit := c >= 'a' && c <= 'z', c >= '0' && c <= '9'
		if !lower && !digit && strings.IndexByte(extra, c) < 0 {
			return false
		}
	}
	return true
}

func validBucket(b string) bool {
	return len(b) >= 3 && len(b) <= 63 && simpleToken(b, ".-") &&
		b[0] != '.' && b[0] != '-' && b[len(b)-1] != '.' && b[len(b)-1] != '-'
}

func validAccessKeyID(id string) bool {
	if id == "" {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		if c <= ' ' || c >= 0x7f || c == ',' || c == '/' {
			return false
		}
	}
	return true
}

// key is the object key of a segment name.
func (s *Store) key(name string) string {
	if s.prefix == "" {
		return name
	}
	return s.prefix + "/" + name
}

// request builds a signed request for an object (key non-empty), or for the
// bucket (key empty), with the query a ready, already encoded string.
func (s *Store) request(ctx context.Context, method, key, query string, body io.Reader) (*http.Request, error) {
	path := "/" + s.bucket
	if key != "" {
		path += "/" + uriEncode(key, false)
	}
	target := s.scheme + "://" + s.host + path
	if query != "" {
		target += "?" + query
	}
	req, err := http.NewRequestWithContext(ctx, method, target, body)
	if err != nil {
		return nil, err
	}
	req.Host = s.host
	return req, nil
}

func (s *Store) do(req *http.Request, payloadHash string) (*http.Response, error) {
	s.signer.sign(req, payloadHash, s.now())
	return s.client.Do(req)
}

// Error is a failure the service reported.
type Error struct {
	// Op names the call: PutObject, GetObject, HeadObject, ListObjectsV2 or
	// DeleteObject.
	Op string
	// Status is the HTTP status.
	Status int
	// Code and Message are the service's, when its answer had them; Message is
	// cut to 200 bytes.
	Code, Message string
}

func (e *Error) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "s3store: %s: HTTP %d", e.Op, e.Status)
	if e.Code != "" {
		b.WriteString(" " + e.Code)
	}
	if e.Message != "" {
		fmt.Fprintf(&b, ": %q", e.Message)
	}
	return b.String()
}

// apiError reads the service's error from resp, which it closes.
func apiError(op string, resp *http.Response) *Error {
	defer drainClose(resp)
	e := &Error{Op: op, Status: resp.StatusCode}
	if resp.Request != nil && resp.Request.Method == http.MethodHead {
		return e
	}
	var doc struct {
		Code    string `xml:"Code"`
		Message string `xml:"Message"`
	}
	if xml.NewDecoder(io.LimitReader(resp.Body, maxErrorBody)).Decode(&doc) == nil {
		e.Code = cleanText(doc.Code, 64)
		e.Message = cleanText(doc.Message, maxMessage)
	}
	return e
}

// cleanText cuts s to at most n bytes at a character boundary.
func cleanText(s string, n int) string {
	if len(s) <= n {
		return s
	}
	s = s[:n]
	for len(s) > 0 && !utf8.ValidString(s) {
		s = s[:len(s)-1]
	}
	return s
}

// drainClose reads a little of what is left of resp's body, so that the
// connection can be reused, and closes it.
func drainClose(resp *http.Response) {
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
	_ = resp.Body.Close()
}

// Put implements [segment.Store].
func (s *Store) Put(ctx context.Context, name string, r io.Reader, size int64) (segment.Info, error) {
	if err := segment.ValidName(name); err != nil {
		return segment.Info{}, err
	}
	if err := ctx.Err(); err != nil {
		return segment.Info{}, fmt.Errorf("s3store: put %q: %w", name, err)
	}
	if size > s.maxSize {
		return segment.Info{}, fmt.Errorf("s3store: put %q: announced size %d: %w", name, size, segment.ErrTooLarge)
	}
	file, n, sum, err := s.spoolBody(ctx, name, r, size)
	if err != nil {
		return segment.Info{}, err
	}
	defer func() {
		_ = file.Close()
		_ = os.Remove(file.Name())
	}()
	info := segment.Info{Name: name, Size: n, SHA256: sum}
	hexSum := hex.EncodeToString(sum[:])

	if s.noCondPut {
		s.putMu.Lock()
		defer s.putMu.Unlock()
		switch exists, err := s.exists(ctx, name); {
		case err != nil:
			return segment.Info{}, err
		case exists:
			return segment.Info{}, fmt.Errorf("s3store: put %q: %w", name, segment.ErrExists)
		}
	}

	var body io.Reader = http.NoBody
	if n > 0 {
		body = io.NewSectionReader(file, 0, n)
	}
	req, err := s.request(ctx, http.MethodPut, s.key(name), "", body)
	if err != nil {
		return segment.Info{}, fmt.Errorf("s3store: put %q: %w", name, err)
	}
	req.ContentLength = n
	if n > 0 {
		req.GetBody = func() (io.ReadCloser, error) {
			return io.NopCloser(io.NewSectionReader(file, 0, n)), nil
		}
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set(digestHeader, hexSum)
	if !s.noCondPut {
		req.Header.Set("If-None-Match", "*")
	}
	resp, err := s.do(req, hexSum)
	if err != nil {
		return segment.Info{}, fmt.Errorf("s3store: put %q: %w", name, err)
	}
	defer drainClose(resp)
	switch resp.StatusCode {
	case http.StatusOK:
		return info, nil
	case http.StatusPreconditionFailed:
		return segment.Info{}, fmt.Errorf("s3store: put %q: %w", name, segment.ErrExists)
	}
	return segment.Info{}, fmt.Errorf("s3store: put %q: %w", name, apiError("PutObject", resp))
}

// spoolBody copies what r yields to a temporary file, enforcing the announced
// size and the limit, and returns the file, its length and its SHA-256. On an
// error nothing is left behind.
func (s *Store) spoolBody(ctx context.Context, name string, r io.Reader, size int64) (_ *os.File, n int64, sum [32]byte, err error) {
	f, err := os.CreateTemp(s.spool, "s3store-put-*.tmp")
	if err != nil {
		return nil, 0, sum, fmt.Errorf("s3store: put %q: spool: %w", name, err)
	}
	defer func() {
		if err != nil {
			_ = f.Close()
			_ = os.Remove(f.Name())
		}
	}()
	// Read one byte more than may be stored, so that too much is seen and the
	// announced size, which may lie, is not taken as the bound.
	bound := s.maxSize + 1
	if size >= 0 {
		bound = size + 1
	}
	lr := io.LimitReader(r, bound)
	h := sha256.New()
	buf := make([]byte, copyChunk)
	for {
		if err := ctx.Err(); err != nil {
			return nil, 0, sum, fmt.Errorf("s3store: put %q: %w", name, err)
		}
		m, rerr := lr.Read(buf)
		if m > 0 {
			h.Write(buf[:m])
			if _, werr := f.Write(buf[:m]); werr != nil {
				return nil, 0, sum, fmt.Errorf("s3store: put %q: spool: %w", name, werr)
			}
			n += int64(m)
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return nil, 0, sum, fmt.Errorf("s3store: put %q: reading the segment: %w", name, rerr)
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, 0, sum, fmt.Errorf("s3store: put %q: %w", name, err)
	}
	switch {
	case n == bound && size >= 0:
		return nil, 0, sum, fmt.Errorf("s3store: put %q: the reader yielded more than the announced %d bytes", name, size)
	case n == bound:
		return nil, 0, sum, fmt.Errorf("s3store: put %q: more than %d bytes: %w", name, s.maxSize, segment.ErrTooLarge)
	case size >= 0 && n != size:
		return nil, 0, sum, fmt.Errorf("s3store: put %q: the reader yielded %d bytes, announced %d", name, n, size)
	}
	h.Sum(sum[:0])
	return f, n, sum, nil
}

// exists asks with HeadObject whether an object is stored under name.
func (s *Store) exists(ctx context.Context, name string) (bool, error) {
	resp, err := s.head(ctx, name)
	if err != nil {
		return false, err
	}
	defer drainClose(resp)
	switch resp.StatusCode {
	case http.StatusOK:
		return true, nil
	case http.StatusNotFound:
		return false, nil
	}
	return false, fmt.Errorf("s3store: head %q: %w", name, apiError("HeadObject", resp))
}

func (s *Store) head(ctx context.Context, name string) (*http.Response, error) {
	req, err := s.request(ctx, http.MethodHead, s.key(name), "", nil)
	if err != nil {
		return nil, fmt.Errorf("s3store: head %q: %w", name, err)
	}
	resp, err := s.do(req, emptyPayloadSHA256)
	if err != nil {
		return nil, fmt.Errorf("s3store: head %q: %w", name, err)
	}
	return resp, nil
}

// digestOf reads the digest from an object's headers.
func digestOf(h http.Header) ([32]byte, error) {
	var sum [32]byte
	v := h.Get(digestHeader)
	if len(v) != 2*len(sum) {
		return sum, ErrNoDigest
	}
	if _, err := hex.Decode(sum[:], []byte(v)); err != nil {
		return [32]byte{}, ErrNoDigest
	}
	return sum, nil
}

// Stat implements [segment.Store].
func (s *Store) Stat(ctx context.Context, name string) (segment.Info, error) {
	if err := segment.ValidName(name); err != nil {
		return segment.Info{}, err
	}
	if err := ctx.Err(); err != nil {
		return segment.Info{}, fmt.Errorf("s3store: stat %q: %w", name, err)
	}
	resp, err := s.head(ctx, name)
	if err != nil {
		return segment.Info{}, err
	}
	defer drainClose(resp)
	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusNotFound:
		return segment.Info{}, fmt.Errorf("s3store: stat %q: %w", name, segment.ErrNotFound)
	default:
		return segment.Info{}, fmt.Errorf("s3store: stat %q: %w", name, apiError("HeadObject", resp))
	}
	sum, err := digestOf(resp.Header)
	if err != nil {
		return segment.Info{}, fmt.Errorf("s3store: stat %q: %w", name, err)
	}
	if resp.ContentLength < 0 {
		return segment.Info{}, fmt.Errorf("s3store: stat %q: the answer has no Content-Length", name)
	}
	return segment.Info{Name: name, Size: resp.ContentLength, SHA256: sum}, nil
}

// Open implements [segment.Store]. The reader it returns ends with
// [ErrDigestMismatch] rather than io.EOF if what it read is not what Put stored.
func (s *Store) Open(ctx context.Context, name string) (io.ReadCloser, error) {
	if err := segment.ValidName(name); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("s3store: open %q: %w", name, err)
	}
	req, err := s.request(ctx, http.MethodGet, s.key(name), "", nil)
	if err != nil {
		return nil, fmt.Errorf("s3store: open %q: %w", name, err)
	}
	resp, err := s.do(req, emptyPayloadSHA256)
	if err != nil {
		return nil, fmt.Errorf("s3store: open %q: %w", name, err)
	}
	if resp.StatusCode != http.StatusOK {
		e := apiError("GetObject", resp)
		if e.Status == http.StatusNotFound && e.Code != "NoSuchBucket" {
			return nil, fmt.Errorf("s3store: open %q: %w", name, segment.ErrNotFound)
		}
		return nil, fmt.Errorf("s3store: open %q: %w", name, e)
	}
	sum, err := digestOf(resp.Header)
	if err != nil {
		drainClose(resp)
		return nil, fmt.Errorf("s3store: open %q: %w", name, err)
	}
	return &verifiedBody{body: resp.Body, want: sum, size: resp.ContentLength, hash: sha256.New()}, nil
}

// verifiedBody hashes what it reads and compares the total, at the end, with the
// digest the object recorded and the length the service announced.
type verifiedBody struct {
	body io.ReadCloser
	hash interface {
		io.Writer
		Sum([]byte) []byte
	}
	want [32]byte
	size int64 // -1 when the service did not announce one
	read int64
	done bool
}

func (v *verifiedBody) Read(p []byte) (int, error) {
	n, err := v.body.Read(p)
	if n > 0 {
		_, _ = v.hash.Write(p[:n])
		v.read += int64(n)
	}
	if err == io.EOF && !v.done {
		v.done = true
		if (v.size >= 0 && v.read != v.size) || [32]byte(v.hash.Sum(nil)) != v.want {
			return n, ErrDigestMismatch
		}
	}
	return n, err
}

func (v *verifiedBody) Close() error { return v.body.Close() }

// List implements [segment.Store]. It asks the service for one page at a time
// and yields as it goes, so a failure on a later page comes after the segments
// of the earlier ones and ends the sequence. Segments written or deleted while
// it runs may or may not appear.
func (s *Store) List(ctx context.Context, prefix string) iter.Seq2[segment.Info, error] {
	return func(yield func(segment.Info, error) bool) {
		if err := ctx.Err(); err != nil {
			yield(segment.Info{}, fmt.Errorf("s3store: list: %w", err))
			return
		}
		if prefix != "" {
			if err := segment.ValidName(strings.TrimSuffix(prefix, "/")); err != nil {
				yield(segment.Info{}, err)
				return
			}
		}
		root := ""
		if s.prefix != "" {
			root = s.prefix + "/"
		}
		token := ""
		for {
			page, err := s.listPage(ctx, root+prefix, token)
			if err != nil {
				yield(segment.Info{}, err)
				return
			}
			for _, c := range page.Contents {
				name, ok := strings.CutPrefix(c.Key, root)
				if !ok || segment.ValidName(name) != nil {
					continue // not a segment: somebody else's object
				}
				if !yield(segment.Info{Name: name, Size: c.Size}, nil) {
					return
				}
			}
			if !page.IsTruncated {
				return
			}
			if page.NextContinuationToken == "" {
				yield(segment.Info{}, errors.New("s3store: list: the answer is truncated and has no continuation token"))
				return
			}
			token = page.NextContinuationToken
		}
	}
}

type listPage struct {
	XMLName               xml.Name `xml:"ListBucketResult"`
	IsTruncated           bool     `xml:"IsTruncated"`
	NextContinuationToken string   `xml:"NextContinuationToken"`
	Contents              []struct {
		Key  string `xml:"Key"`
		Size int64  `xml:"Size"`
	} `xml:"Contents"`
}

func (s *Store) listPage(ctx context.Context, prefix, token string) (*listPage, error) {
	// The parameters are written in the order the canonical form sorts them,
	// encoded with %20 for a space (never '+'), and '/' as %2F.
	var q []string
	if token != "" {
		q = append(q, "continuation-token="+uriEncode(token, true))
	}
	q = append(q, "list-type=2")
	if s.pageSize > 0 {
		q = append(q, "max-keys="+strconv.Itoa(s.pageSize))
	}
	if prefix != "" {
		q = append(q, "prefix="+uriEncode(prefix, true))
	}
	req, err := s.request(ctx, http.MethodGet, "", strings.Join(q, "&"), nil)
	if err != nil {
		return nil, fmt.Errorf("s3store: list: %w", err)
	}
	resp, err := s.do(req, emptyPayloadSHA256)
	if err != nil {
		return nil, fmt.Errorf("s3store: list: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("s3store: list: %w", apiError("ListObjectsV2", resp))
	}
	defer drainClose(resp)
	var page listPage
	if err := xml.NewDecoder(io.LimitReader(resp.Body, maxListBody)).Decode(&page); err != nil {
		return nil, fmt.Errorf("s3store: list: the answer is not a listing: %w", err)
	}
	return &page, nil
}

// Delete implements [segment.Store].
func (s *Store) Delete(ctx context.Context, name string) error {
	if err := segment.ValidName(name); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("s3store: delete %q: %w", name, err)
	}
	req, err := s.request(ctx, http.MethodDelete, s.key(name), "", nil)
	if err != nil {
		return fmt.Errorf("s3store: delete %q: %w", name, err)
	}
	resp, err := s.do(req, emptyPayloadSHA256)
	if err != nil {
		return fmt.Errorf("s3store: delete %q: %w", name, err)
	}
	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNoContent {
		drainClose(resp)
		return nil
	}
	e := apiError("DeleteObject", resp)
	if e.Status == http.StatusNotFound && e.Code != "NoSuchBucket" {
		return nil // removing what is not there is not an error
	}
	return fmt.Errorf("s3store: delete %q: %w", name, e)
}
