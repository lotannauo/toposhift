package s3store_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"runtime/debug"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/segment"
	"github.com/lotannauo/toposhift/internal/segment/s3store"
	"github.com/lotannauo/toposhift/internal/segment/s3store/s3fake"
	"github.com/lotannauo/toposhift/internal/segment/segmenttest"
)

// The credentials every test uses. The secret is a marker that no output of any
// test may contain: TestMain scans everything the test binary prints for it.
const (
	testKeyID  = "TESTKEYID0123456789"
	testSecret = "marker-secret-9f2b6c1d4e8a7305-must-never-be-printed"
)

func TestMain(m *testing.M) {
	out, errs := capture(&os.Stdout), capture(&os.Stderr)
	code := m.Run()
	printed := append(out(), errs()...)
	if bytes.Contains(printed, []byte(testSecret)) {
		fmt.Fprintln(os.Stderr, "FAIL: the secret access key appeared in the output of a test")
		code = 1
	}
	os.Exit(code)
}

// capture tees what is written to *f into a buffer and returns the function that
// restores *f and returns what was written.
func capture(f **os.File) (finish func() []byte) {
	orig := *f
	r, w, err := os.Pipe()
	if err != nil {
		return func() []byte { return nil }
	}
	*f = w
	var buf bytes.Buffer
	done := make(chan struct{})
	go func() {
		_, _ = io.Copy(io.MultiWriter(orig, &buf), r)
		close(done)
	}()
	return func() []byte {
		*f = orig
		_ = w.Close()
		<-done
		_ = r.Close()
		return buf.Bytes()
	}
}

func payload(seed byte, n int) []byte {
	var key [32]byte
	key[0] = seed
	b := make([]byte, n)
	_, _ = rand.NewChaCha8(key).Read(b)
	return b
}

func sum(b []byte) [32]byte { return sha256.Sum256(b) }

// newFake starts a fake that accepts the test credentials.
func newFake(t testing.TB, opts ...s3fake.Option) *s3fake.Server {
	t.Helper()
	opts = append([]s3fake.Option{s3fake.WithCredentials(testKeyID, testSecret)}, opts...)
	srv := s3fake.New(opts...)
	t.Cleanup(srv.Close)
	return srv
}

// noReuse is a client that opens a connection for every request, so that a
// fault applies to the request it was meant for and net/http does not resend a
// request on another connection.
func noReuse() *http.Client {
	return &http.Client{
		Transport:     &http.Transport{DisableKeepAlives: true},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

// config is the configuration of a store on srv; edit adjusts it.
func config(t testing.TB, srv *s3fake.Server, edit ...func(*s3store.Config)) s3store.Config {
	t.Helper()
	cfg := s3store.Config{
		Endpoint:        srv.URL(),
		Region:          srv.Region(),
		Bucket:          srv.Bucket(),
		AccessKeyID:     testKeyID,
		SecretAccessKey: testSecret,
		SpoolDir:        t.TempDir(),
		HTTPClient:      noReuse(),
	}
	for _, e := range edit {
		e(&cfg)
	}
	return cfg
}

func newStore(t testing.TB, srv *s3fake.Server, edit ...func(*s3store.Config)) *s3store.Store {
	t.Helper()
	st, err := s3store.New(config(t, srv, edit...))
	if err != nil {
		t.Fatal(err)
	}
	return st
}

var bg = context.Background()

func put(t testing.TB, st segment.Store, name string, data []byte) segment.Info {
	t.Helper()
	info, err := st.Put(bg, name, bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("Put(%q): %v", name, err)
	}
	return info
}

func list(t testing.TB, st segment.Store, prefix string) []string {
	t.Helper()
	var names []string
	for info, err := range st.List(bg, prefix) {
		if err != nil {
			t.Fatalf("List(%q): %v", prefix, err)
		}
		names = append(names, info.Name)
	}
	return names
}

func assertEmptyDir(t testing.TB, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		var names []string
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Errorf("the spool directory holds %v, want it empty", names)
	}
}

// TestSuite holds the store to the suite every segment store passes, against
// the fake, with a small page so that listings cross pages.
func TestSuite(t *testing.T) {
	segmenttest.Run(t, func(t *testing.T) segment.Store {
		return newStore(t, newFake(t), func(c *s3store.Config) {
			c.MaxSize = 4 << 20
			c.PageSize = 4
		})
	}, segmenttest.WithMaxSize(4<<20))
}

// TestSuiteAtMaxSize runs the suite with the real limit, 256 MiB, which moves
// about a gigabyte through the fake and the spool and holds a large share of
// that in memory, so it runs only when TOPOSHIFT_TEST_S3_MAXSIZE is 1.
func TestSuiteAtMaxSize(t *testing.T) {
	if os.Getenv("TOPOSHIFT_TEST_S3_MAXSIZE") != "1" {
		t.Skip("set TOPOSHIFT_TEST_S3_MAXSIZE=1 to move 256 MiB segments through the fake")
	}
	segmenttest.Run(t, func(t *testing.T) segment.Store { return newStore(t, newFake(t)) })
}

// TestSuiteWithoutConditionalPut holds the mode for a service that ignores
// If-None-Match to the same suite, on a fake that ignores it.
func TestSuiteWithoutConditionalPut(t *testing.T) {
	segmenttest.Run(t, func(t *testing.T) segment.Store {
		return newStore(t, newFake(t, s3fake.IgnoreIfNoneMatch()), func(c *s3store.Config) {
			c.NoConditionalPut = true
			c.MaxSize = 4 << 20
		})
	}, segmenttest.WithMaxSize(4<<20))
}

func TestPutRequestShape(t *testing.T) {
	for _, prefix := range []string{"", "backup", "a/b"} {
		t.Run("prefix="+prefix, func(t *testing.T) {
			srv := newFake(t)
			st := newStore(t, srv, func(c *s3store.Config) { c.Prefix = prefix })
			data := payload(1, 100_000)
			info := put(t, st, "files/one", data)
			want := sum(data)
			if info.SHA256 != want || info.Size != int64(len(data)) || info.Name != "files/one" {
				t.Errorf("Put returned %+v", info)
			}
			var puts []s3fake.Request
			for _, r := range srv.Requests() {
				if r.Op == "PutObject" {
					puts = append(puts, r)
				}
			}
			if len(puts) != 1 {
				t.Fatalf("%d PutObject requests, want 1", len(puts))
			}
			r := puts[0]
			wantKey := "files/one"
			if prefix != "" {
				wantKey = prefix + "/" + wantKey
			}
			hexSum := hex.EncodeToString(want[:])
			if r.BodyLen != int64(len(data)) {
				t.Errorf("the service read %d body bytes, want %d", r.BodyLen, len(data))
			}
			for header, wantValue := range map[string]string{
				"If-None-Match":        "*",
				"X-Amz-Meta-Sha256":    hexSum,
				"X-Amz-Content-Sha256": hexSum,
				"Content-Type":         "application/octet-stream",
			} {
				if got := r.Header.Get(header); got != wantValue {
					t.Errorf("%s = %q, want %q", header, got, wantValue)
				}
			}
			if r.Key != wantKey || r.Path != "/"+srv.Bucket()+"/"+wantKey {
				t.Errorf("key %q at path %q, want key %q", r.Key, r.Path, wantKey)
			}
			stored, meta, ok := srv.Get(wantKey)
			if !ok || !bytes.Equal(stored, data) || meta["sha256"] != hexSum {
				t.Errorf("the object under %q is not the segment with its digest (found %v, metadata %v)", wantKey, ok, meta)
			}
			// A second store, nothing shared, reads the same digest back.
			other := newStore(t, srv, func(c *s3store.Config) { c.Prefix = prefix })
			stat, err := other.Stat(bg, "files/one")
			if err != nil || stat.SHA256 != want || stat.Size != int64(len(data)) {
				t.Errorf("Stat from a new store = %+v, %v; want the digest and size Put reported", stat, err)
			}
		})
	}
}

func TestPutWithoutConditionalPutAsksFirst(t *testing.T) {
	srv := newFake(t, s3fake.IgnoreIfNoneMatch())
	st := newStore(t, srv, func(c *s3store.Config) { c.NoConditionalPut = true })
	put(t, st, "x/one", []byte("one"))
	if _, err := st.Put(bg, "x/one", strings.NewReader("two"), 3); !errors.Is(err, segment.ErrExists) {
		t.Fatalf("second Put = %v, want ErrExists", err)
	}
	var ops []string
	for _, r := range srv.Requests() {
		ops = append(ops, r.Op)
		if r.Op == "PutObject" && r.Header.Get("If-None-Match") != "" {
			t.Errorf("PutObject carried If-None-Match %q", r.Header.Get("If-None-Match"))
		}
	}
	if want := []string{"HeadObject", "PutObject", "HeadObject"}; !slices.Equal(ops, want) {
		t.Errorf("requests %v, want %v", ops, want)
	}
	data, _, _ := srv.Get("x/one")
	if string(data) != "one" {
		t.Errorf("the object holds %q, want the first segment", data)
	}
}

// TestIgnoredConditionOverwrites shows what NoConditionalPut is for: against a
// service that ignores If-None-Match, a store that relies on it replaces
// segments. That is why the option exists, and why the default is not it.
func TestIgnoredConditionOverwrites(t *testing.T) {
	srv := newFake(t, s3fake.IgnoreIfNoneMatch())
	st := newStore(t, srv)
	put(t, st, "x/one", []byte("one"))
	put(t, st, "x/one", []byte("two"))
	if data, _, _ := srv.Get("x/one"); string(data) != "two" {
		t.Fatalf("the object holds %q; the fake should have overwritten it", data)
	}
}

// TestLostResponse: a Put whose answer is lost has stored the segment; the
// retry is told so, and Stat's digest says whether the segment is the writer's.
func TestLostResponse(t *testing.T) {
	srv := newFake(t)
	st := newStore(t, srv)
	mine, other := payload(2, 5000), payload(3, 5000)
	srv.Inject(s3fake.Fault{Op: "PutObject", Kind: s3fake.DropAfterStore})
	_, err := st.Put(bg, "retry/seg", bytes.NewReader(mine), int64(len(mine)))
	if err == nil || errors.Is(err, segment.ErrExists) {
		t.Fatalf("Put with a lost answer = %v, want an error that is not ErrExists", err)
	}
	if _, _, ok := srv.Get("retry/seg"); !ok {
		t.Fatal("the fault should have left the segment stored")
	}
	_, err = st.Put(bg, "retry/seg", bytes.NewReader(mine), int64(len(mine)))
	if !errors.Is(err, segment.ErrExists) {
		t.Fatalf("the retry = %v, want ErrExists", err)
	}
	stat, err := st.Stat(bg, "retry/seg")
	if err != nil {
		t.Fatal(err)
	}
	if stat.SHA256 != sum(mine) {
		t.Errorf("Stat after the retry reports %x, want the digest of the bytes written", stat.SHA256)
	}
	if stat.SHA256 == sum(other) {
		t.Error("Stat reports the digest of other bytes")
	}
}

// failAfter is a request body that fails after n bytes.
type failAfter struct {
	rc io.ReadCloser
	n  int64
}

func (f *failAfter) Read(p []byte) (int, error) {
	if f.n <= 0 {
		return 0, errors.New("injected: the upload was cut")
	}
	if int64(len(p)) > f.n {
		p = p[:f.n]
	}
	n, err := f.rc.Read(p)
	f.n -= int64(n)
	return n, err
}

func (f *failAfter) Close() error { return f.rc.Close() }

// flipOne is a request body that changes one byte on its way.
type flipOne struct {
	rc  io.ReadCloser
	off int64
	at  int64
}

func (f *flipOne) Read(p []byte) (int, error) {
	n, err := f.rc.Read(p)
	if i := f.off - f.at; i >= 0 && i < int64(n) {
		p[i] ^= 0xff
	}
	f.at += int64(n)
	return n, err
}

func (f *flipOne) Close() error { return f.rc.Close() }

// bodyTransport changes the body of every PutObject on its way out.
type bodyTransport struct {
	wrap func(rc io.ReadCloser, length int64) io.ReadCloser
}

func (b bodyTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Method == http.MethodPut && r.Body != nil && r.ContentLength > 0 {
		r2 := r.Clone(r.Context())
		r2.Body = b.wrap(r.Body, r.ContentLength)
		r2.GetBody = nil
		r = r2
	}
	tr := &http.Transport{DisableKeepAlives: true}
	defer tr.CloseIdleConnections()
	return tr.RoundTrip(r)
}

// TestFailedPutStoresNothing: whichever way a Put fails, nothing is listed or
// readable under the name, the spool is empty, and the Put can be done again.
func TestFailedPutStoresNothing(t *testing.T) {
	const name = "failed/seg"
	data := payload(4, 16<<20)
	tests := []struct {
		name string
		// setup arranges the failure; it returns the context to Put with, and a
		// function to call once Put has started (to cancel, say).
		setup func(t *testing.T, srv *s3fake.Server, cfg *s3store.Config) (context.Context, func())
		check func(t *testing.T, err error, srv *s3fake.Server)
	}{
		{"the service answers 500", func(t *testing.T, srv *s3fake.Server, _ *s3store.Config) (context.Context, func()) {
			srv.Inject(s3fake.Fault{Op: "PutObject", Kind: s3fake.Fail500})
			return bg, nil
		}, func(t *testing.T, err error, _ *s3fake.Server) {
			var e *s3store.Error
			if !errors.As(err, &e) || e.Status != 500 || e.Code != "InternalError" || e.Op != "PutObject" {
				t.Errorf("error %v, want an *s3store.Error for a 500 InternalError", err)
			}
		}},
		{"the service answers 503 SlowDown", func(t *testing.T, srv *s3fake.Server, _ *s3store.Config) (context.Context, func()) {
			srv.Inject(s3fake.Fault{Op: "PutObject", Kind: s3fake.SlowDown})
			return bg, nil
		}, func(t *testing.T, err error, _ *s3fake.Server) {
			var e *s3store.Error
			if !errors.As(err, &e) || e.Status != 503 || e.Code != "SlowDown" {
				t.Errorf("error %v, want an *s3store.Error for a 503 SlowDown", err)
			}
		}},
		{"the connection is dropped", func(t *testing.T, srv *s3fake.Server, _ *s3store.Config) (context.Context, func()) {
			srv.Inject(s3fake.Fault{Op: "PutObject", Kind: s3fake.Drop})
			return bg, nil
		}, func(t *testing.T, err error, _ *s3fake.Server) {
			if err == nil {
				t.Error("Put returned nil")
			}
		}},
		{"the upload is cut short", func(t *testing.T, _ *s3fake.Server, cfg *s3store.Config) (context.Context, func()) {
			cfg.HTTPClient = &http.Client{Transport: bodyTransport{func(rc io.ReadCloser, n int64) io.ReadCloser {
				return &failAfter{rc: rc, n: n / 2}
			}}}
			return bg, nil
		}, func(t *testing.T, err error, srv *s3fake.Server) {
			if err == nil {
				t.Error("Put returned nil")
			}
			// The fake saw a body shorter than its Content-Length and dropped it.
			waitFor(t, "the fake to give up on the short body", func() bool {
				for _, r := range srv.Requests() {
					if r.Op == "PutObject" && r.Status == 0 && r.BodyLen < int64(len(data)) {
						return true
					}
				}
				return false
			})
		}},
		{"the body is altered on its way", func(t *testing.T, _ *s3fake.Server, cfg *s3store.Config) (context.Context, func()) {
			cfg.HTTPClient = &http.Client{Transport: bodyTransport{func(rc io.ReadCloser, n int64) io.ReadCloser {
				return &flipOne{rc: rc, off: n / 3}
			}}}
			return bg, nil
		}, func(t *testing.T, err error, _ *s3fake.Server) {
			var e *s3store.Error
			if !errors.As(err, &e) || e.Code != "XAmzContentSHA256Mismatch" {
				t.Errorf("error %v, want the service's XAmzContentSHA256Mismatch", err)
			}
		}},
		{"the context ends mid-upload", func(t *testing.T, srv *s3fake.Server, _ *s3store.Config) (context.Context, func()) {
			srv.Inject(s3fake.Fault{Op: "PutObject", Kind: s3fake.Stall})
			ctx, cancel := context.WithCancel(bg)
			t.Cleanup(cancel)
			return ctx, func() {
				select {
				case <-srv.Stalled():
				case <-time.After(20 * time.Second):
					t.Error("the fake never held the upload")
				}
				cancel()
			}
		}, func(t *testing.T, err error, _ *s3fake.Server) {
			if !errors.Is(err, context.Canceled) {
				t.Errorf("error %v, want one wrapping context.Canceled", err)
			}
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := newFake(t)
			cfg := config(t, srv)
			ctx, afterStart := tc.setup(t, srv, &cfg)
			st, err := s3store.New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() {
				_, err := st.Put(ctx, name, bytes.NewReader(data), int64(len(data)))
				done <- err
			}()
			if afterStart != nil {
				afterStart()
			}
			select {
			case err = <-done:
			case <-time.After(30 * time.Second):
				t.Fatal("Put did not return")
			}
			if err == nil {
				t.Fatal("Put returned nil")
			}
			tc.check(t, err, srv)
			if keys := srv.Keys(); len(keys) != 0 {
				t.Errorf("the service holds %v after the failed Put, want nothing", keys)
			}
			if names := list(t, st, ""); len(names) != 0 {
				t.Errorf("List = %v after the failed Put, want nothing", names)
			}
			if _, err := st.Stat(bg, name); !errors.Is(err, segment.ErrNotFound) {
				t.Errorf("Stat = %v, want ErrNotFound", err)
			}
			assertEmptyDir(t, cfg.SpoolDir)

			// The name is free: the same Put succeeds once the trouble has passed.
			srv.ClearFaults()
			good, err := s3store.New(config(t, srv))
			if err != nil {
				t.Fatal(err)
			}
			put(t, good, name, data)
			if got := srv.Keys(); !slices.Equal(got, []string{name}) {
				t.Errorf("keys after the retry %v, want [%s]", got, name)
			}
		})
	}
}

func waitFor(t testing.TB, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestSpoolIsEmptiedOnEveryPath checks the spool after successes and refusals.
func TestSpoolIsEmptiedOnEveryPath(t *testing.T) {
	srv := newFake(t)
	cfg := config(t, srv, func(c *s3store.Config) { c.MaxSize = 1 << 20 })
	st, err := s3store.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	put(t, st, "a", []byte("a"))
	if _, err := st.Put(bg, "a", strings.NewReader("b"), 1); !errors.Is(err, segment.ErrExists) {
		t.Fatal(err)
	}
	if _, err := st.Put(bg, "b", strings.NewReader("b"), 2); err == nil {
		t.Fatal("a short reader was accepted")
	}
	if _, err := st.Put(bg, "c", bytes.NewReader(make([]byte, 2<<20)), -1); !errors.Is(err, segment.ErrTooLarge) {
		t.Fatalf("Put of too much = %v, want ErrTooLarge", err)
	}
	if _, err := st.Put(bg, "d", &errReader{}, -1); err == nil {
		t.Fatal("a failing reader was accepted")
	}
	assertEmptyDir(t, cfg.SpoolDir)
	if got := srv.Keys(); !slices.Equal(got, []string{"a"}) {
		t.Errorf("keys %v, want [a]", got)
	}
}

type errReader struct{ n int }

func (r *errReader) Read(p []byte) (int, error) {
	if r.n > 0 {
		return 0, errors.New("injected read error")
	}
	r.n++
	return copy(p, "some bytes"), nil
}

// TestDigestIsNeverGuessed: an object without a valid sha256 metadata is an
// error to Stat and Open, never a zero digest, never a missing segment.
func TestDigestIsNeverGuessed(t *testing.T) {
	srv := newFake(t)
	st := newStore(t, srv)
	data := []byte("some bytes")
	good := sum(data)
	srv.Seed("good", data, map[string]string{"sha256": hex.EncodeToString(good[:])})
	srv.Seed("none", data, nil)
	srv.Seed("other-meta", data, map[string]string{"color": "red"})
	srv.Seed("short", data, map[string]string{"sha256": hex.EncodeToString(good[:])[:62]})
	srv.Seed("nothex", data, map[string]string{"sha256": strings.Repeat("zz", 32)})
	srv.Seed("empty", data, map[string]string{"sha256": ""})
	put(t, st, "faulted", data)

	info, err := st.Stat(bg, "good")
	if err != nil || info.SHA256 != good || info.Size != int64(len(data)) {
		t.Fatalf("Stat of an object with a digest = %+v, %v", info, err)
	}
	for _, name := range []string{"none", "other-meta", "short", "nothex", "empty"} {
		info, err := st.Stat(bg, name)
		if !errors.Is(err, s3store.ErrNoDigest) || errors.Is(err, segment.ErrNotFound) {
			t.Errorf("Stat(%q) = %+v, %v; want ErrNoDigest", name, info, err)
		}
		if info != (segment.Info{}) {
			t.Errorf("Stat(%q) returned %+v with its error, want the zero Info", name, info)
		}
		if rc, err := st.Open(bg, name); !errors.Is(err, s3store.ErrNoDigest) {
			if rc != nil {
				rc.Close()
			}
			t.Errorf("Open(%q) error %v, want ErrNoDigest", name, err)
		}
	}
	// The service leaves the digest out of its answer for an object Put wrote.
	srv.Inject(s3fake.Fault{Op: "HeadObject", Kind: s3fake.NoDigest})
	if info, err := st.Stat(bg, "faulted"); !errors.Is(err, s3store.ErrNoDigest) {
		t.Errorf("Stat with the digest left out = %+v, %v; want ErrNoDigest", info, err)
	}
	srv.Inject(s3fake.Fault{Op: "GetObject", Kind: s3fake.NoDigest})
	if rc, err := st.Open(bg, "faulted"); !errors.Is(err, s3store.ErrNoDigest) {
		if rc != nil {
			rc.Close()
		}
		t.Errorf("Open with the digest left out: error %v, want ErrNoDigest", err)
	}
}

// TestOpenChecksWhatItReads: a body that is altered or cut short ends with an
// error, never with io.EOF.
func TestOpenChecksWhatItReads(t *testing.T) {
	data := payload(5, 1<<20)
	read := func(t *testing.T, kind s3fake.FaultKind) error {
		srv := newFake(t)
		st := newStore(t, srv)
		put(t, st, "seg", data)
		srv.Inject(s3fake.Fault{Op: "GetObject", Kind: kind})
		rc, err := st.Open(bg, "seg")
		if err != nil {
			t.Fatal(err)
		}
		defer rc.Close()
		got, err := io.ReadAll(rc)
		if err == nil && bytes.Equal(got, data) {
			t.Fatal("the fault did not change what is read")
		}
		return err
	}
	t.Run("one byte changed", func(t *testing.T) {
		if err := read(t, s3fake.CorruptBody); !errors.Is(err, s3store.ErrDigestMismatch) {
			t.Errorf("error %v, want ErrDigestMismatch", err)
		}
	})
	t.Run("half the body", func(t *testing.T) {
		if err := read(t, s3fake.TruncateBody); err == nil {
			t.Error("a body cut short ended with io.EOF")
		}
	})
	t.Run("an intact body", func(t *testing.T) {
		srv := newFake(t)
		st := newStore(t, srv)
		put(t, st, "seg", data)
		rc, err := st.Open(bg, "seg")
		if err != nil {
			t.Fatal(err)
		}
		defer rc.Close()
		if got, err := io.ReadAll(rc); err != nil || !bytes.Equal(got, data) {
			t.Errorf("read %d bytes, error %v", len(got), err)
		}
	})
	t.Run("a slow read the context ends", func(t *testing.T) {
		srv := newFake(t)
		st := newStore(t, srv)
		put(t, st, "seg", data)
		srv.Inject(s3fake.Fault{Op: "GetObject", Kind: s3fake.Stall})
		ctx, cancel := context.WithCancel(bg)
		defer cancel()
		rc, err := st.Open(ctx, "seg")
		if err != nil {
			t.Fatal(err)
		}
		defer rc.Close()
		errc := make(chan error, 1)
		go func() {
			_, err := io.ReadAll(rc)
			errc <- err
		}()
		select {
		case <-srv.Stalled():
		case <-time.After(20 * time.Second):
			t.Fatal("the fake never held the download")
		}
		cancel()
		select {
		case err := <-errc:
			if err == nil || errors.Is(err, io.EOF) {
				t.Errorf("read error %v, want the end of the context", err)
			}
		case <-time.After(20 * time.Second):
			t.Fatal("the read did not end with its context")
		}
	})
}

func TestListing(t *testing.T) {
	srv := newFake(t, s3fake.WithPageLimit(3))
	st := newStore(t, srv, func(c *s3store.Config) {
		c.Prefix = "backup"
		c.PageSize = 100 // more than the service allows
	})
	var want []string
	for i := range 20 {
		name := fmt.Sprintf("files/p%02d", 19-i)
		put(t, st, name, payload(byte(i), 10+i))
		want = append(want, name)
	}
	put(t, st, "manifests/m1", []byte("m"))
	slices.Sort(want)
	all := append(slices.Clone(want), "manifests/m1")

	// Objects that are not segments: the service holds them under the prefix,
	// or beside it.
	srv.Seed("backup/UPPER", []byte("x"), nil)
	srv.Seed("backup/", nil, nil)
	srv.Seed("backup/.hidden", []byte("x"), nil)
	srv.Seed("backup/a b", []byte("x"), nil)
	srv.Seed("backupx/files/p00", []byte("x"), nil)
	srv.Seed("other/files/p00", []byte("x"), nil)

	srv.ResetRequests()
	if got := list(t, st, ""); !slices.Equal(got, all) {
		t.Errorf("List(\"\") = %v\nwant %v", got, all)
	}
	lists := 0
	var queries []string
	for _, r := range srv.Requests() {
		if r.Op == "ListObjectsV2" {
			lists++
			queries = append(queries, r.Query)
		}
	}
	// The service holds four more keys under the prefix, which fill pages too.
	if wantPages := (len(all) + 4 + 2) / 3; lists != wantPages {
		t.Errorf("%d list requests, want %d pages of 3 for %d keys", lists, wantPages, len(all)+4)
	}
	if !strings.Contains(queries[0], "prefix=backup%2F") || strings.Contains(queries[0], "continuation-token") ||
		!strings.Contains(queries[1], "continuation-token=") || !strings.Contains(queries[0], "max-keys=100") {
		t.Errorf("queries %q do not look like a first page and a continuation", queries)
	}

	if got := list(t, st, "files/p1"); !slices.Equal(got, want[10:]) {
		t.Errorf("List(files/p1) = %v, want %v", got, want[10:])
	}
	if got := list(t, st, "manifests/"); !slices.Equal(got, []string{"manifests/m1"}) {
		t.Errorf("List(manifests/) = %v", got)
	}
	if got := list(t, st, "nothing/"); len(got) != 0 {
		t.Errorf("List(nothing/) = %v", got)
	}
	// Sizes come from the listing; digests are left zero.
	for info, err := range st.List(bg, "manifests/") {
		if err != nil || info.Size != 1 || info.SHA256 != ([32]byte{}) {
			t.Errorf("listed %+v, %v; want size 1 and no digest", info, err)
		}
	}
	// Stopping early stops asking.
	srv.ResetRequests()
	n := 0
	for range st.List(bg, "") {
		n++
		if n == 2 {
			break
		}
	}
	// The first page of three holds only keys that are not segments.
	if got := srv.Count("ListObjectsV2"); n != 2 || got != 2 {
		t.Errorf("a loop stopped after %d segments made %d list requests, want 2", n, got)
	}
	// Another store in the bucket sees nothing of this prefix, and one with
	// none sees the keys whole.
	if got := list(t, newStore(t, srv, func(c *s3store.Config) { c.Prefix = "backupx" }), ""); !slices.Equal(got, []string{"files/p00"}) {
		t.Errorf("a store under backupx lists %v, want [files/p00]", got)
	}
	top := newStore(t, srv)
	if got := list(t, top, "other/"); !slices.Equal(got, []string{"other/files/p00"}) {
		t.Errorf("a store with no prefix lists %v under other/", got)
	}
}

func TestListFailsLoudlyOnALaterPage(t *testing.T) {
	srv := newFake(t, s3fake.WithPageLimit(2))
	st := newStore(t, srv)
	for i := range 7 {
		put(t, st, fmt.Sprintf("s/%d", i), []byte{byte(i)})
	}
	srv.Inject(s3fake.Fault{Op: "ListObjectsV2", After: 2, Kind: s3fake.Fail500})
	var names []string
	var failure error
	for info, err := range st.List(bg, "") {
		if err != nil {
			failure = err
			continue // the sequence ends with the error: nothing may follow
		}
		if failure != nil {
			t.Fatalf("%q was yielded after the error", info.Name)
		}
		names = append(names, info.Name)
	}
	var e *s3store.Error
	if !errors.As(failure, &e) || e.Status != 500 {
		t.Errorf("error %v, want the 500", failure)
	}
	if want := []string{"s/0", "s/1", "s/2", "s/3"}; !slices.Equal(names, want) {
		t.Errorf("segments before the failure %v, want %v", names, want)
	}
}

// TestCredentialsAreChecked: the fake recomputes each signature, so a request
// signed with the wrong secret, key, region or clock is refused by it.
func TestCredentialsAreChecked(t *testing.T) {
	tests := []struct {
		name string
		edit func(*s3store.Config)
		code string
	}{
		{"wrong secret", func(c *s3store.Config) { c.SecretAccessKey += "x" }, "SignatureDoesNotMatch"},
		{"wrong access key id", func(c *s3store.Config) { c.AccessKeyID += "X" }, "InvalidAccessKeyId"},
		{"wrong region", func(c *s3store.Config) { c.Region = "eu-west-1" }, "AuthorizationHeaderMalformed"},
		{"clock an hour behind", func(c *s3store.Config) { c.Now = func() time.Time { return time.Now().Add(-time.Hour) } }, "RequestTimeTooSkewed"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := newFake(t)
			st := newStore(t, srv, tc.edit)
			wantCode := func(what string, err error) {
				t.Helper()
				var e *s3store.Error
				if !errors.As(err, &e) || e.Code != tc.code {
					t.Errorf("%s: error %v, want the service's %s", what, err, tc.code)
				}
			}
			_, err := st.Put(bg, "x", strings.NewReader("x"), 1)
			wantCode("Put", err)
			_, err = st.Open(bg, "x")
			wantCode("Open", err)
			wantCode("Delete", st.Delete(bg, "x"))
			var listErr error
			for _, err := range st.List(bg, "") {
				listErr = err
			}
			wantCode("List", listErr)
			if keys := srv.Keys(); len(keys) != 0 {
				t.Errorf("a refused Put stored %v", keys)
			}
			if got := len(srv.AuthFailures()); got < 4 {
				t.Errorf("the fake recorded %d refused requests, want at least 4", got)
			}
		})
	}
}

// TestClientSignaturesAgreeWithTheFake: with the right credentials the fake
// never refuses a request, whatever the names and the sizes.
func TestClientSignaturesAgreeWithTheFake(t *testing.T) {
	srv := newFake(t)
	st := newStore(t, srv, func(c *s3store.Config) { c.Prefix = "p/q" })
	names := []string{"a", "a/b/c", "x.y_z-w/0", "files/0000000000000001-0000000000000002.parquet", strings.Repeat("e", 128), "z/" + strings.Repeat("d", 100) + "/" + strings.Repeat("f", 100)}
	for i, n := range names {
		put(t, st, n, payload(byte(i), i*1000))
		if _, err := st.Stat(bg, n); err != nil {
			t.Errorf("Stat(%q): %v", n, err)
		}
		rc, err := st.Open(bg, n)
		if err != nil {
			t.Fatalf("Open(%q): %v", n, err)
		}
		if got, err := io.ReadAll(rc); err != nil || len(got) != i*1000 {
			t.Errorf("read %q: %d bytes, %v", n, len(got), err)
		}
		rc.Close()
	}
	slices.Sort(names)
	for _, prefix := range []string{"", "a", "a/", "x.y", "files/", "z/d"} {
		var want []string
		for _, n := range names {
			if strings.HasPrefix(n, prefix) {
				want = append(want, n)
			}
		}
		if got := list(t, st, prefix); !slices.Equal(got, want) {
			t.Errorf("List(%q) = %v, want %v", prefix, got, want)
		}
	}
	for _, n := range names {
		if err := st.Delete(bg, n); err != nil {
			t.Errorf("Delete(%q): %v", n, err)
		}
	}
	if f := srv.AuthFailures(); len(f) != 0 {
		t.Errorf("the fake refused %d requests with the right credentials; the first: %+v\ncanonical request it computed:\n%s", len(f), f[0].Header, f[0].AuthNote)
	}
}

func TestMissingBucketIsLoud(t *testing.T) {
	srv := newFake(t)
	st := newStore(t, srv, func(c *s3store.Config) { c.Bucket = "no-such-bucket" })
	var e *s3store.Error
	if _, err := st.Put(bg, "x", strings.NewReader("x"), 1); !errors.As(err, &e) || e.Code != "NoSuchBucket" {
		t.Errorf("Put = %v, want NoSuchBucket", err)
	}
	if _, err := st.Open(bg, "x"); errors.Is(err, segment.ErrNotFound) || !errors.As(err, &e) || e.Code != "NoSuchBucket" {
		t.Errorf("Open = %v, want NoSuchBucket and not ErrNotFound", err)
	}
	if err := st.Delete(bg, "x"); !errors.As(err, &e) || e.Code != "NoSuchBucket" {
		t.Errorf("Delete = %v, want NoSuchBucket", err)
	}
	for _, err := range st.List(bg, "") {
		if !errors.As(err, &e) || e.Code != "NoSuchBucket" {
			t.Errorf("List error %v, want NoSuchBucket", err)
		}
	}
}

func TestNew(t *testing.T) {
	srv := newFake(t)
	dir := t.TempDir()
	file := dir + "/file"
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	good := func() s3store.Config {
		return s3store.Config{
			Endpoint: "https://objects.example.test", Bucket: "bucket-1", AccessKeyID: testKeyID,
			SecretAccessKey: testSecret, SpoolDir: dir,
		}
	}
	if _, err := s3store.New(good()); err != nil {
		t.Fatalf("a valid configuration is refused: %v", err)
	}
	ok := []func(*s3store.Config){
		func(c *s3store.Config) { c.Endpoint = srv.URL() }, // http, loopback
		func(c *s3store.Config) { c.Endpoint = "http://localhost:9000/" },
		func(c *s3store.Config) { c.Endpoint = "http://[::1]:9000" },
		func(c *s3store.Config) { c.Endpoint = "https://objects.example.test:8443" },
		func(c *s3store.Config) {
			c.Region = "us-east-1"
			c.Prefix = "a/b"
			c.MaxSize = segment.MaxSize
			c.PageSize = 1000
		},
	}
	for i, edit := range ok {
		c := good()
		edit(&c)
		if _, err := s3store.New(c); err != nil {
			t.Errorf("accepted configuration %d refused: %v", i, err)
		}
	}
	bad := []struct {
		name string
		edit func(*s3store.Config)
	}{
		{"no endpoint", func(c *s3store.Config) { c.Endpoint = "" }},
		{"endpoint without a scheme", func(c *s3store.Config) { c.Endpoint = "objects.example.test" }},
		{"endpoint with a path", func(c *s3store.Config) { c.Endpoint = "https://objects.example.test/bucket" }},
		{"endpoint with a query", func(c *s3store.Config) { c.Endpoint = "https://objects.example.test/?x=1" }},
		{"endpoint with credentials", func(c *s3store.Config) {
			c.Endpoint = "https://" + testKeyID + ":" + testSecret + "@objects.example.test"
		}},
		{"plain http to another host", func(c *s3store.Config) { c.Endpoint = "http://objects.example.test" }},
		{"ftp", func(c *s3store.Config) { c.Endpoint = "ftp://objects.example.test" }},
		{"no bucket", func(c *s3store.Config) { c.Bucket = "" }},
		{"bucket with a slash", func(c *s3store.Config) { c.Bucket = "a/b" }},
		{"bucket in upper case", func(c *s3store.Config) { c.Bucket = "Bucket" }},
		{"bucket too short", func(c *s3store.Config) { c.Bucket = "ab" }},
		{"bucket starting with a dash", func(c *s3store.Config) { c.Bucket = "-bucket" }},
		{"region with a slash", func(c *s3store.Config) { c.Region = "us/east" }},
		{"prefix with a trailing slash", func(c *s3store.Config) { c.Prefix = "backup/" }},
		{"prefix in upper case", func(c *s3store.Config) { c.Prefix = "Backup" }},
		{"prefix with dot dot", func(c *s3store.Config) { c.Prefix = "a/../b" }},
		{"no access key id", func(c *s3store.Config) { c.AccessKeyID = "" }},
		{"access key id with a slash", func(c *s3store.Config) { c.AccessKeyID = "a/b" }},
		{"access key id with a space", func(c *s3store.Config) { c.AccessKeyID = "a b" }},
		{"no secret", func(c *s3store.Config) { c.SecretAccessKey = "" }},
		{"no spool directory", func(c *s3store.Config) { c.SpoolDir = "" }},
		{"relative spool directory", func(c *s3store.Config) { c.SpoolDir = "relative" }},
		{"missing spool directory", func(c *s3store.Config) { c.SpoolDir = dir + "/missing" }},
		{"spool directory that is a file", func(c *s3store.Config) { c.SpoolDir = file }},
		{"negative MaxSize", func(c *s3store.Config) { c.MaxSize = -1 }},
		{"MaxSize above the segment limit", func(c *s3store.Config) { c.MaxSize = segment.MaxSize + 1 }},
		{"PageSize above 1000", func(c *s3store.Config) { c.PageSize = 1001 }},
	}
	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			c := good()
			tc.edit(&c)
			st, err := s3store.New(c)
			if err == nil {
				t.Fatalf("accepted: %v", st)
			}
			if strings.Contains(err.Error(), testSecret) {
				t.Errorf("the error repeats the secret: %v", err)
			}
		})
	}
	if _, err := os.Stat(dir + "/missing"); err == nil {
		t.Error("New created the spool directory")
	}
}

func TestCredentialsFromEnv(t *testing.T) {
	env := func(m map[string]string) func(string) string { return func(k string) string { return m[k] } }
	id, secret, err := s3store.CredentialsFromEnv(env(map[string]string{
		s3store.EnvAccessKeyID: "the-id", s3store.EnvSecretAccessKey: "the-secret",
	}))
	if err != nil || id != "the-id" || string(secret) != "the-secret" {
		t.Fatalf("got %q, %v", id, err)
	}
	for name, m := range map[string]map[string]string{
		"neither":      {},
		"no id":        {s3store.EnvSecretAccessKey: testSecret},
		"no secret":    {s3store.EnvAccessKeyID: "the-id"},
		"empty secret": {s3store.EnvAccessKeyID: "the-id", s3store.EnvSecretAccessKey: ""},
	} {
		_, _, err := s3store.CredentialsFromEnv(env(m))
		if !errors.Is(err, s3store.ErrNoCredentials) {
			t.Errorf("%s: error %v, want ErrNoCredentials", name, err)
			continue
		}
		if strings.Contains(err.Error(), testSecret) || strings.Contains(err.Error(), "the-id") {
			t.Errorf("%s: the error repeats a value: %v", name, err)
		}
		if name == "no id" && !strings.Contains(err.Error(), s3store.EnvAccessKeyID) {
			t.Errorf("%s: the error does not name the missing variable: %v", name, err)
		}
	}
}

// TestSecretNeverAppears drives every operation down every failure path, and
// prints and logs the configuration and the store every way there is, and
// requires that the secret appear in none of it, nor in what the service
// received.
func TestSecretNeverAppears(t *testing.T) {
	srv := newFake(t)
	cfg := config(t, srv)
	st, err := s3store.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var out strings.Builder
	say := func(format string, args ...any) { fmt.Fprintf(&out, format+"\n", args...) }

	// Every way to print the configuration and the store.
	var logs bytes.Buffer
	slog.New(slog.NewJSONHandler(&logs, nil)).Info("config", "cfg", cfg, "secret", cfg.SecretAccessKey, "store", st)
	slog.New(slog.NewTextHandler(&logs, nil)).Info("config", "cfg", cfg, "secret", cfg.SecretAccessKey, "store", st)
	say("%s", logs.String())
	for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%d"} {
		say(verb, cfg)
		say(verb, &cfg)
		say(verb, cfg.SecretAccessKey)
		say(verb, st)
		say(verb, []any{cfg, st, cfg.SecretAccessKey})
	}
	js, err := json.Marshal(map[string]any{"id": cfg.AccessKeyID, "secret": cfg.SecretAccessKey, "pointer": &cfg.SecretAccessKey})
	if err != nil {
		t.Fatal(err)
	}
	say("%s", js)
	if txt, err := cfg.SecretAccessKey.MarshalText(); err == nil {
		say("%s", txt)
	}

	// Every error the operations return.
	data := payload(6, 3000)
	put(t, st, "seg", data)
	srv.Inject(s3fake.Fault{Kind: s3fake.Fail500, Times: -1})
	_, e1 := st.Put(bg, "x", bytes.NewReader(data), int64(len(data)))
	_, e2 := st.Open(bg, "seg")
	_, e3 := st.Stat(bg, "seg")
	e4 := st.Delete(bg, "seg")
	var e5 error
	for _, err := range st.List(bg, "") {
		e5 = err
	}
	srv.ClearFaults()
	srv.Inject(s3fake.Fault{Kind: s3fake.Drop, Times: -1})
	_, e6 := st.Put(bg, "x", bytes.NewReader(data), int64(len(data)))
	_, e7 := st.Stat(bg, "seg")
	srv.ClearFaults()
	_, e8 := st.Put(bg, "seg", bytes.NewReader(data), int64(len(data)))
	_, e9 := st.Stat(bg, "missing")
	ctx, cancel := context.WithCancel(bg)
	cancel()
	_, e10 := st.Put(ctx, "y", bytes.NewReader(data), int64(len(data)))
	_, e11 := st.Put(bg, "Bad Name", bytes.NewReader(data), int64(len(data)))
	wrong := cfg
	wrong.SecretAccessKey += "-wrong"
	wst, err := s3store.New(wrong)
	if err != nil {
		t.Fatal(err)
	}
	_, e12 := wst.Put(bg, "x", bytes.NewReader(data), int64(len(data)))
	srv.Seed("foreign", data, nil)
	_, e13 := st.Stat(bg, "foreign")
	_, e14 := st.Open(bg, "foreign")
	_, e15 := s3store.New(s3store.Config{Endpoint: "http://objects.example.test", Bucket: "b-1", AccessKeyID: testKeyID, SecretAccessKey: testSecret, SpoolDir: cfg.SpoolDir})
	_, _, e16 := s3store.CredentialsFromEnv(func(string) string { return "" })
	for i, err := range []error{e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16} {
		if err == nil {
			t.Errorf("error %d is nil", i+1)
		}
		say("%v | %+v | %#v | %q", err, err, err, fmt.Sprint(err))
	}

	// A panic's message and stack.
	func() {
		defer func() {
			r := recover()
			say("%v %s", r, debug.Stack())
		}()
		_, _ = st.Put(nil, "x", bytes.NewReader(data), 1) //nolint:staticcheck // a nil context is the point
	}()

	// What the service received.
	for _, r := range srv.Requests() {
		say("%+v", r)
	}

	if strings.Contains(out.String(), testSecret) {
		i := strings.Index(out.String(), testSecret)
		t.Fatalf("the secret appears in the output: ...%s...", out.String()[max(0, i-200):min(len(out.String()), i+100)])
	}
	if !strings.Contains(out.String(), "<redacted>") {
		t.Error("nothing printed was redacted: the test is not looking at the secret")
	}
	// And the spool, which holds segments, holds no secret.
	assertEmptyDir(t, cfg.SpoolDir)
}
