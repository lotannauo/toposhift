package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/datadir"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/ingest/sequencer"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/pebblestore"
)

// setSeam sets a seam of the boot for the test, and puts it back after. It is
// called before serve starts, so that serve has stopped when it is put back.
func setSeam[T any](t *testing.T, p *T, v T) {
	t.Helper()
	old := *p
	*p = v
	t.Cleanup(func() { *p = old })
}

// post sends a body to the receiver. It does not fail the test, so that it can
// be called from serve's goroutine through a seam.
func post(ctx context.Context, base string, body []byte) (status int, retryAfter, text string, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/v1/logs", bytes.NewReader(body))
	if err != nil {
		return 0, "", "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return 0, "", "", err
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	text = string(b)
	// A refusal is a google.rpc.Status: its message, unescaped, is what is
	// compared.
	var st struct {
		Message *string `json:"message"`
	}
	if json.Unmarshal(b, &st) == nil && st.Message != nil {
		text = *st.Message
	}
	return resp.StatusCode, resp.Header.Get("Retry-After"), text, nil
}

// waitAnswer posts an empty request until the answer is not the one given while
// the store opens, and returns it.
func waitAnswer(t testing.TB, base string) (status int, retryAfter, text string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		status, retryAfter, text, err := post(t.Context(), base, []byte("{}"))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(text, "ingest is starting") {
			return status, retryAfter, text
		}
		if time.Now().After(deadline) {
			t.Fatalf("ingest is still starting: %d %q", status, text)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// waitIngestOn waits until the receiver takes requests.
func waitIngestOn(t testing.TB, base string) {
	t.Helper()
	if status, _, text := waitAnswer(t, base); status != http.StatusOK {
		t.Fatalf("ingest did not start: %d %q", status, text)
	}
}

// podBody is one pull record of a pod, as OTLP JSON. Its object has no creation
// time, so every record of it is timed at its observation.
func podBody(uid string, observed time.Time) []byte {
	return fmt.Appendf(nil, `{"resourceLogs":[{"resource":{},"scopeLogs":[{"logRecords":[{"observedTimeUnixNano":"%d",`+
		`"body":{"kvlistValue":{"values":[{"key":"kind","value":{"stringValue":"Pod"}},{"key":"metadata","value":{"kvlistValue":{"values":[`+
		`{"key":"uid","value":{"stringValue":%q}},{"key":"name","value":{"stringValue":%q}},{"key":"namespace","value":{"stringValue":"default"}}]}}},`+
		`{"key":"status","value":{"kvlistValue":{"values":[{"key":"phase","value":{"stringValue":"Running"}}]}}}]}},`+
		`"attributes":[{"key":"k8s.resource.name","value":{"stringValue":"pods"}}]}]}]}]}`, observed.UnixNano(), uid, uid)
}

func postOK(t testing.TB, base string, body []byte) {
	t.Helper()
	status, _, text, err := post(t.Context(), base, body)
	if err != nil || status != http.StatusOK || text != "{}" {
		t.Fatalf("POST = %d %q %v, want 200 {}", status, text, err)
	}
}

func dirEnv(t testing.TB, dir string) func(string) string {
	return testEnv(t, map[string]string{"TOPOSHIFT_DATA_DIR": dir})
}

// openStopped opens the store a stopped serve left in dir.
func openStopped(t *testing.T, dir string) *pebblestore.Store {
	t.Helper()
	st, err := pebblestore.Open(filepath.Join(dir, "store"), serveStoreOptions("", false, nil))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

// podRecords are the stored records of a pod's existence.
func podRecords(t *testing.T, st store.Store, uid string) []store.Record {
	t.Helper()
	id, err := identity.NewResolver(catalog.Default()).Resolve(catalog.K8sPod, []identity.Attr{{Key: catalog.K8sPodUID, Value: uid}})
	if err != nil {
		t.Fatal(err)
	}
	e, _ := catalog.Default().Entity(catalog.K8sPod)
	recs, err := st.EntityWindow(t.Context(), id.Fingerprint(), store.MinEventTime, store.MaxEventTime, store.Current(e.Layer()))
	if err != nil {
		t.Fatal(err)
	}
	return recs
}

func stopOK(t *testing.T, s *served) {
	t.Helper()
	if code := s.stop(t); code != 0 {
		t.Fatalf("exit code = %d, want 0:\n%s", code, s.stderr)
	}
}

// B1. serve on an empty data directory with --cluster-name prod: a request while
// the store opens is answered 503, ingest starting; the name is recorded in the
// store before the receiver switches to the sequencer, which the seam between
// the two sees; then ingest takes requests.
func TestServeRecordsTheClusterNameBeforeIngestStarts(t *testing.T) {
	dir := t.TempDir()
	opening, release := make(chan struct{}), make(chan struct{})
	setSeam(t, &openStore, func(path string, o pebblestore.Options) (*pebblestore.Store, error) {
		close(opening)
		<-release
		return pebblestore.Open(path, o)
	})
	var base string
	var seamRan atomic.Bool
	setSeam(t, &beforeSwitch, func(st *pebblestore.Store) {
		seamRan.Store(true)
		name, set, present, err := st.ClusterName()
		if err != nil || name != "prod" || !set || !present {
			t.Errorf("at the switch the store records %q set %v present %v (%v), want prod recorded", name, set, present, err)
		}
		status, _, text, err := post(context.Background(), base, []byte("{}"))
		if err != nil || status != http.StatusServiceUnavailable || !strings.Contains(text, "ingest is starting") {
			t.Errorf("before the switch the receiver answered %d %q %v, want 503 starting", status, text, err)
		}
	})

	s := startServe(t, dirEnv(t, dir), "--cluster-name", "prod")
	base = s.base
	// Registered after serve's, so it runs first: a failing test does not leave
	// serve held in Open.
	var once sync.Once
	open := func() { once.Do(func() { close(release) }) }
	t.Cleanup(open)
	select {
	case <-opening:
	case <-time.After(15 * time.Second):
		t.Fatal("serve did not open the store")
	}
	status, retryAfter, text, err := post(t.Context(), s.base, []byte("{}"))
	if err != nil || status != http.StatusServiceUnavailable || retryAfter != "30" ||
		!strings.Contains(text, "ingest is starting: the store is opening") {
		t.Errorf("while the store opens: %d Retry-After %q %q %v, want 503 starting", status, retryAfter, text, err)
	}
	if got := get(t, s.base+"/healthz"); got != http.StatusOK {
		t.Errorf("GET /healthz while the store opens = %d", got)
	}
	open()
	waitIngestOn(t, s.base)
	if !seamRan.Load() {
		t.Error("the seam between the record and the switch was not called")
	}
	stopOK(t, s)
	name, set, present, err := openStopped(t, dir).ClusterName()
	if err != nil || name != "prod" || !set || !present {
		t.Errorf("after the stop the store records %q %v %v (%v)", name, set, present, err)
	}
}

// B2. A store started with prod, rebooted with staging: ingest is off with the
// decision's message and Retry-After: 30; an accept flag naming the old name is
// ignored with a warning, and neither does a variable of that name accept
// anything; the flag naming the configured name starts ingest and records it.
func TestServeChecksTheClusterNameAtBoot(t *testing.T) {
	dir := t.TempDir()
	s := startServe(t, dirEnv(t, dir), "--cluster-name", "prod")
	waitIngestOn(t, s.base)
	postOK(t, s.base, podBody("pod-1", time.Now()))
	stopOK(t, s)

	const changed = `ingest is off: this store was started with cluster name "prod" and is now configured with "staging"`
	off := func(s *served) {
		t.Helper()
		status, retryAfter, text := waitAnswer(t, s.base)
		if status != http.StatusServiceUnavailable || retryAfter != "30" || !strings.Contains(text, changed) {
			t.Errorf("answer %d Retry-After %q %q, want 503 with the decision's message", status, retryAfter, text)
		}
	}
	s = startServe(t, testEnv(t, map[string]string{
		"TOPOSHIFT_DATA_DIR": dir, "TOPOSHIFT_CLUSTER_NAME": "staging", "TOPOSHIFT_ACCEPT_NEW_CLUSTER_NAME": "staging",
	}))
	off(s)
	stopOK(t, s)

	s = startServe(t, dirEnv(t, dir), "--cluster-name", "staging", "--accept-new-cluster-name=prod")
	off(s)
	if !strings.Contains(s.stderr.String(), "--accept-new-cluster-name=prod is ignored") {
		t.Errorf("no warning for the stale flag:\n%s", s.stderr)
	}
	stopOK(t, s)

	s = startServe(t, dirEnv(t, dir), "--cluster-name", "staging", "--accept-new-cluster-name=staging")
	waitIngestOn(t, s.base)
	if !strings.Contains(s.stderr.String(), "accepted the change of cluster name") {
		t.Errorf("the accepted change is not logged:\n%s", s.stderr)
	}
	stopOK(t, s)
	if name, set, _, err := openStopped(t, dir).ClusterName(); err != nil || name != "staging" || !set {
		t.Errorf("the store records %q %v (%v), want staging", name, set, err)
	}
}

// The environment twin of --cluster-name names the cluster, and an empty one is
// no name: a new store records that none is set.
func TestServeClusterNameFromTheEnvironment(t *testing.T) {
	for _, tt := range []struct {
		value string
		set   bool
	}{{"staging", true}, {"", false}} {
		dir := t.TempDir()
		s := startServe(t, testEnv(t, map[string]string{"TOPOSHIFT_DATA_DIR": dir, "TOPOSHIFT_CLUSTER_NAME": tt.value}))
		waitIngestOn(t, s.base)
		stopOK(t, s)
		name, set, present, err := openStopped(t, dir).ClusterName()
		if err != nil || name != tt.value || set != tt.set || !present {
			t.Errorf("TOPOSHIFT_CLUSTER_NAME=%q: recorded %q set %v present %v (%v)", tt.value, name, set, present, err)
		}
	}
}

// A configured cluster name the store cannot hold, a pull interval that is not
// above zero, and a node interval that is not a whole number of seconds from a
// minute to an hour, are mistakes in how serve was called: exit 2, before anything
// listens.
func TestServeRefusesBadIngestFlags(t *testing.T) {
	long := strings.Repeat("c", store.MaxClusterNameLen+1)
	tests := []struct {
		name    string
		env     map[string]string
		args    []string
		wantErr string
	}{
		{"a long flag", nil, []string{"--cluster-name", long}, "--cluster-name"},
		{"a long variable", map[string]string{"TOPOSHIFT_CLUSTER_NAME": long}, nil, "--cluster-name"},
		{"a byte outside the format", nil, []string{"--cluster-name", "<CLUSTER_NAME>"}, "outside"},
		{"a zero pull interval", nil, []string{"--pull-interval", "0s"}, "--pull-interval"},
		{"a node interval under a minute", nil, []string{"--node-interval", "59s"}, "--node-interval"},
		{"a node interval over an hour", nil, []string{"--node-interval", "61m"}, "--node-interval"},
		{"a node interval with a part of a second", nil, []string{"--node-interval", "90s500ms"}, "whole number of seconds"},
		{"a node interval of zero", nil, []string{"--node-interval", "0s"}, "--node-interval"},
		{"a node interval in the variable out of bounds", map[string]string{"TOPOSHIFT_NODE_INTERVAL": "10s"}, nil, "--node-interval"},
		{"a node interval in the variable that is not a duration", map[string]string{"TOPOSHIFT_NODE_INTERVAL": "soon"}, nil, "TOPOSHIFT_NODE_INTERVAL"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stderr bytes.Buffer
			code := run(t.Context(), append([]string{"serve", "--listen", "127.0.0.1:0"}, tt.args...), testEnv(t, tt.env), io.Discard, &stderr)
			if code != 2 || !strings.Contains(stderr.String(), tt.wantErr) || strings.Contains(stderr.String(), "listening") {
				t.Errorf("exit %d, stderr %q", code, &stderr)
			}
		})
	}
}

// B4. A graceful stop writes the refreshes the coalescer absorbed: two pulls of
// a pod five minutes apart, the second absorbed, then a stop. After it, the store
// holds the run's extension through the second pull. The stop takes less than
// the server's 8 s and the sequencer's 5 s.
func TestServeGracefulStopWritesAbsorbedRefreshes(t *testing.T) {
	dir := t.TempDir()
	s := startServe(t, dirEnv(t, dir))
	waitIngestOn(t, s.base)
	first := time.Now().Add(-10 * time.Minute).Truncate(time.Second)
	postOK(t, s.base, podBody("steady", first))
	postOK(t, s.base, podBody("steady", first.Add(5*time.Minute)))
	start := time.Now()
	stopOK(t, s)
	took := time.Since(start)
	t.Logf("the stop took %s", took)
	if took >= 13*time.Second {
		t.Errorf("the stop took %s, want under 8 s + 5 s", took)
	}
	recs := podRecords(t, openStopped(t, dir), "steady")
	found := false
	for _, r := range recs {
		if r.EventTime.Equal(first) && r.Through.Equal(first.Add(5*time.Minute)) {
			found = true
		}
	}
	if !found {
		t.Errorf("the absorbed refresh was not written at the stop: %d records %+v", len(recs), recs)
	}
	for _, step := range []string{"server stopped", "sequencer stopped", "store closed"} {
		if !strings.Contains(s.stderr.String(), step) {
			t.Errorf("the log does not time %q:\n%s", step, s.stderr)
		}
	}
}

// heldWriter holds every write that begins once it is armed until the gate
// opens, counts the writes in flight, and says whether a write found the store
// closed.
type heldWriter struct {
	sequencer.Writer
	armed       *atomic.Bool
	gate        chan struct{}
	inFlight    *atomic.Int64
	foundClosed *atomic.Bool
}

func (w heldWriter) Write(ctx context.Context, batch []store.Record) error {
	w.inFlight.Add(1)
	defer w.inFlight.Add(-1)
	if w.armed.Load() {
		<-w.gate
	}
	err := w.Writer.Write(ctx, batch)
	if errors.Is(err, store.ErrClosed) {
		w.foundClosed.Store(true)
	}
	return err
}

// A sequencer that does not stop in time is cancelled, and Run is waited for
// before the store closes. The stop of the sequencer is held in its write of
// the absorbed refresh: nothing cancels the sequencer before its stop times
// out, so the loop takes the stop and begins that write, which the test holds.
// When the timeout is logged the write is in flight, and serve must not return
// while it is; once it is let go, no write finds the store closed, serve exits
// 0 saying that absorbed refreshes may be unwritten, and the store reopens with
// what was answered 200.
func TestServeStopTimeoutCancelsTheSequencer(t *testing.T) {
	setSeam(t, &sequencerStopTimeout, 300*time.Millisecond)
	var armed, foundClosed atomic.Bool
	var inFlight atomic.Int64
	gate := make(chan struct{})
	var once sync.Once
	open := func() { once.Do(func() { close(gate) }) }
	setSeam(t, &writerFor, func(st *pebblestore.Store) sequencer.Writer {
		return heldWriter{st, &armed, gate, &inFlight, &foundClosed}
	})
	dir := t.TempDir()
	s := startServe(t, dirEnv(t, dir))
	t.Cleanup(open) // runs before serve's cleanup: a failing test does not leave the write held
	waitIngestOn(t, s.base)
	first := time.Now().Add(-10 * time.Minute).Truncate(time.Second)
	postOK(t, s.base, podBody("pod-1", first))
	postOK(t, s.base, podBody("pod-1", first.Add(5*time.Minute))) // absorbed
	armed.Store(true)

	s.cancel()
	deadline := time.Now().Add(15 * time.Second)
	for !strings.Contains(s.stderr.String(), "did not stop in time") {
		if time.Now().After(deadline) {
			t.Fatalf("the stop did not time out:\n%s", s.stderr)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if n := inFlight.Load(); n != 1 {
		t.Fatalf("%d writes in flight when the stop timed out, want the held one", n)
	}
	select {
	case code := <-s.done:
		s.done <- code
		t.Fatalf("serve returned (exit %d) while a write of the sequencer was in flight", code)
	case <-time.After(time.Second):
	}
	open()
	select {
	case code := <-s.done:
		s.done <- code
		if code != 0 {
			t.Fatalf("exit code = %d, want 0:\n%s", code, s.stderr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("serve did not stop after the write was let go")
	}
	if inFlight.Load() != 0 || foundClosed.Load() {
		t.Errorf("after serve returned: %d writes in flight, a write found the store closed: %v", inFlight.Load(), foundClosed.Load())
	}
	if !strings.Contains(s.stderr.String(), "absorbed refreshes may be unwritten") {
		t.Errorf("the timed-out stop is not logged:\n%s", s.stderr)
	}
	if len(podRecords(t, openStopped(t, dir), "pod-1")) == 0 {
		t.Error("the record answered 200 is not in the store")
	}
}

// B6. A store that cannot be opened keeps the server up: health is served, and
// the receiver answers 503 naming the error. serve stops cleanly and releases
// the directory.
func TestServeStaysUpWhenTheStoreCannotOpen(t *testing.T) {
	dir := t.TempDir()
	setSeam(t, &openStore, func(string, pebblestore.Options) (*pebblestore.Store, error) {
		return nil, errors.New("injected open failure")
	})
	s := startServe(t, dirEnv(t, dir))
	status, retryAfter, text := waitAnswer(t, s.base)
	if status != http.StatusServiceUnavailable || retryAfter != "30" || !strings.Contains(text, "injected open failure") {
		t.Errorf("answer %d Retry-After %q %q, want 503 naming the error", status, retryAfter, text)
	}
	if got := get(t, s.base+"/healthz"); got != http.StatusOK {
		t.Errorf("GET /healthz = %d", got)
	}
	if !strings.Contains(s.stderr.String(), "the store could not be opened") {
		t.Errorf("the error is not logged:\n%s", s.stderr)
	}
	stopOK(t, s)
	d, err := datadir.Open(datadir.Result{Dir: dir})
	if err != nil {
		t.Fatalf("the directory is still held: %v", err)
	}
	_ = d.Close()
}

// B3. The real binary, killed with SIGKILL right after its last 200: every
// request it answered 200 is in the store when the store is opened again.
func TestServeKilledKeepsWhatItAnswered(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping the binary test in short mode")
	}
	bin := filepath.Join(t.TempDir(), "toposhift")
	build := exec.CommandContext(t.Context(), goTool(), "build", "-o", bin, ".")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}
	dir := t.TempDir()
	cmd := exec.CommandContext(t.Context(), bin, "serve", "--listen", "127.0.0.1:0", "--cluster-name", "prod")
	cmd.Env = []string{"TOPOSHIFT_DATA_DIR=" + dir}
	cmd.Dir = t.TempDir()
	stderr := &lockedBuffer{}
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	t.Cleanup(func() { _ = cmd.Process.Kill() })

	var base string
	deadline := time.Now().Add(15 * time.Second)
	for base == "" {
		if m := listeningRE.FindStringSubmatch(stderr.String()); m != nil {
			base = "http://" + m[1]
			break
		}
		select {
		case err := <-exited:
			t.Fatalf("serve exited: %v\n%s", err, stderr)
		case <-time.After(10 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatalf("serve did not listen:\n%s", stderr)
		}
	}
	waitIngestOn(t, base)
	var answered []string
	now := time.Now()
	for i := range 20 {
		uid := fmt.Sprintf("pod-%02d", i)
		postOK(t, base, podBody(uid, now))
		answered = append(answered, uid)
	}
	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	if err := <-exited; err == nil {
		t.Fatal("serve exited cleanly after SIGKILL")
	}
	st := openStopped(t, dir)
	for _, uid := range answered {
		if len(podRecords(t, st, uid)) == 0 {
			t.Errorf("%s was answered 200 and is not in the store", uid)
		}
	}
	if name, _, _, err := st.ClusterName(); err != nil || name != "prod" {
		t.Errorf("the store records %q (%v)", name, err)
	}
}
