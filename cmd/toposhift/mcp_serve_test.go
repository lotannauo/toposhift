package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/mcp/mcphttp"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/pebblestore"
)

// mcpResult is the result of a tools/call, as the wire carries it.
type mcpResult struct {
	IsError    bool
	Text       string
	Structured map[string]any
}

// mcpCall calls a tool at base/mcp with a bare JSON-RPC request. The server keeps
// no session, so no initialize comes first. It does not fail the test, so that
// it can be called from a goroutine.
func mcpCall(ctx context.Context, base, name string, args any) (mcpResult, int, error) {
	body, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": 1, "method": "tools/call",
		"params": map[string]any{"name": name, "arguments": args},
	})
	if err != nil {
		return mcpResult{}, 0, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/mcp", bytes.NewReader(body))
	if err != nil {
		return mcpResult{}, 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	resp, err := client.Do(req)
	if err != nil {
		return mcpResult{}, 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	var rpc struct {
		Result struct {
			IsError bool `json:"isError"`
			Content []struct {
				Text string `json:"text"`
			} `json:"content"`
			Structured map[string]any `json:"structuredContent"`
		} `json:"result"`
	}
	if resp.StatusCode != http.StatusOK || json.Unmarshal(b, &rpc) != nil {
		return mcpResult{}, resp.StatusCode, nil
	}
	out := mcpResult{IsError: rpc.Result.IsError, Structured: rpc.Result.Structured}
	if len(rpc.Result.Content) > 0 {
		out.Text = rpc.Result.Content[0].Text
	}
	return out, resp.StatusCode, nil
}

func mcpOK(t testing.TB, base, name string, args any) mcpResult {
	t.Helper()
	res, status, err := mcpCall(t.Context(), base, name, args)
	if err != nil || status != http.StatusOK {
		t.Fatalf("%s: status %d, %v", name, status, err)
	}
	return res
}

// podAttrs and the instant to ask about identify the pod that podBody posts.
func podAttrs(uid string) map[string]any {
	return map[string]any{"type": "k8s.pod", "attributes": map[string]any{"k8s.pod.uid": uid}}
}

func resolveAtArgs(uid string, at time.Time) map[string]any {
	a := podAttrs(uid)
	a["at"] = at.UTC().Format(time.RFC3339Nano)
	return a
}

// The tools are served on serve's own listener from the store ingest fills: a pod
// that was answered 200 is alive to resolve_entity at the next call, at a token,
// and the range tools answer on it, with names.
func TestServeServesTheToolsFromTheStore(t *testing.T) {
	dir := t.TempDir()
	s := startServe(t, dirEnv(t, dir))
	waitIngestOn(t, s.base)
	first := time.Now().Add(-10 * time.Minute).Truncate(time.Second)
	postOK(t, s.base, podBody("web-0", first))

	res := mcpOK(t, s.base, "resolve_entity", resolveAtArgs("web-0", first.Add(time.Minute)))
	if res.IsError {
		t.Fatalf("resolve_entity: %s", res.Text)
	}
	ex, _ := res.Structured["existence"].(map[string]any)
	token, _ := res.Structured["token"].(string)
	if ex["status"] != "alive" || token == "" || token == "0" {
		t.Errorf("a pod that was answered 200: existence %v, token %q; want alive at a token", ex, token)
	}
	// An instant before the pod was first seen: not alive.
	res = mcpOK(t, s.base, "resolve_entity", resolveAtArgs("web-0", first.Add(-time.Hour)))
	if ex, _ := res.Structured["existence"].(map[string]any); res.IsError || ex["status"] != "not_alive" {
		t.Errorf("before the pod existed: %+v", res)
	}

	schema := mcpOK(t, s.base, "describe_schema", map[string]any{})
	if schema.IsError || schema.Structured["range_reads"] != true {
		t.Errorf("describe_schema: %+v, want range_reads true on a pebblestore", schema)
	}
	id := resolveAtArgs("web-0", first)
	resolved := mcpOK(t, s.base, "resolve_entity", id).Structured["entity"].(map[string]any)
	pod := resolved["id"].(string)
	if resolved["name"] != "web-0" {
		t.Errorf("resolve_entity: name %v, want web-0 (names are read on a pebblestore)", resolved["name"])
	}
	for _, name := range []string{"blast_radius", "what_changed", "co_residents"} {
		args := map[string]any{"entity": pod, "from": first.Format(time.RFC3339), "to": first.Add(time.Hour).Format(time.RFC3339)}
		switch name {
		case "what_changed":
			args = map[string]any{"scope": []string{pod}, "from": args["from"], "to": args["to"]}
		case "co_residents":
			args = map[string]any{"pod": pod, "from": args["from"], "to": args["to"]}
		}
		res := mcpOK(t, s.base, name, args)
		if res.IsError || res.Structured["token"] != token {
			t.Errorf("%s on a pebblestore: %+v, want an answer at token %s", name, res, token)
		}
	}
	nb := mcpOK(t, s.base, "neighbors_at", map[string]any{"entity": pod, "at": first.Add(time.Minute).Format(time.RFC3339)})
	if nb.IsError || nb.Structured["token"] != token {
		t.Errorf("neighbors_at: %+v, want an answer at token %s", nb, token)
	}

	if status := get(t, s.base+"/mcp"); status != http.StatusMethodNotAllowed {
		t.Errorf("GET /mcp = %d, want 405", status)
	}
	stopOK(t, s)
}

// Reading needs no cluster name: when ingest stays off for a change of the name,
// the tools still read the store.
func TestServeReadsWhileIngestIsOffForTheClusterName(t *testing.T) {
	dir := t.TempDir()
	s := startServe(t, dirEnv(t, dir), "--cluster-name", "prod")
	waitIngestOn(t, s.base)
	first := time.Now().Add(-10 * time.Minute).Truncate(time.Second)
	postOK(t, s.base, podBody("pod-1", first))
	stopOK(t, s)

	s = startServe(t, testEnv(t, map[string]string{"TOPOSHIFT_DATA_DIR": dir, "TOPOSHIFT_CLUSTER_NAME": "staging"}))
	if status, _, text := waitAnswer(t, s.base); status != http.StatusServiceUnavailable || !strings.Contains(text, "ingest is off") {
		t.Fatalf("ingest answered %d %q, want it off for the name", status, text)
	}
	res := mcpOK(t, s.base, "resolve_entity", resolveAtArgs("pod-1", first.Add(time.Minute)))
	if ex, _ := res.Structured["existence"].(map[string]any); res.IsError || ex["status"] != "alive" {
		t.Errorf("with ingest off for the cluster name: %+v, want the pod alive", res)
	}
	stopOK(t, s)
}

// The sentence of the receiver is the sentence of the tools while there is no
// store: with no data directory, while the store opens, and when it could not be
// opened. The tools that read no store answer all the same.
func TestServeToolsGateLikeTheReceiver(t *testing.T) {
	readArgs := func() map[string]any {
		return map[string]any{"entity": "k8s.pod:00000000000000000000000000000000", "at": "2026-01-01T10:00:00Z"}
	}
	noStore := func(t *testing.T, base, sentence string) {
		t.Helper()
		res := mcpOK(t, base, "neighbors_at", readArgs())
		if !res.IsError || !strings.Contains(res.Text, sentence) {
			t.Errorf("neighbors_at = %+v, want the sentence %q", res, sentence)
		}
		if res := mcpOK(t, base, "describe_schema", map[string]any{}); res.IsError {
			t.Errorf("describe_schema: %s", res.Text)
		}
		if res := mcpOK(t, base, "resolve_entity", podAttrs("x")); res.IsError || res.Structured["token"] != nil {
			t.Errorf("resolve_entity with no instant: %+v", res)
		}
		// The receiver says the same thing.
		_, _, body := postLogs(t, base)
		if !strings.Contains(body, sentence) {
			t.Errorf("the receiver says %q, not %q", body, sentence)
		}
	}

	t.Run("no data directory", func(t *testing.T) {
		s := startServe(t, testEnv(t, nil))
		noStore(t, s.base, sentenceNoDirectory)
		stopOK(t, s)
	})

	t.Run("the store is opening", func(t *testing.T) {
		opening, release := make(chan struct{}), make(chan struct{})
		setSeam(t, &openStore, func(path string, o pebblestore.Options) (*pebblestore.Store, error) {
			close(opening)
			<-release
			return pebblestore.Open(path, o)
		})
		s := startServe(t, dirEnv(t, t.TempDir()))
		<-opening
		noStore(t, s.base, ingestStarting)
		close(release)
		waitIngestOn(t, s.base)
		if res := mcpOK(t, s.base, "neighbors_at", readArgs()); res.IsError {
			t.Errorf("neighbors_at once the store is open: %s", res.Text)
		}
		stopOK(t, s)
	})

	t.Run("the store could not be opened", func(t *testing.T) {
		setSeam(t, &openStore, func(string, pebblestore.Options) (*pebblestore.Store, error) {
			return nil, errors.New("injected open failure")
		})
		s := startServe(t, dirEnv(t, t.TempDir()))
		waitAnswer(t, s.base)
		noStore(t, s.base, "ingest is off: the store could not be opened: injected open failure; see the server's log")
		stopOK(t, s)
	})
}

// On a listener that is not loopback the tools are not mounted, and the log says
// so once. The server has no authentication yet.
func TestServeLeavesTheToolsOffOnANonLoopbackListener(t *testing.T) {
	s := startServe(t, dirEnv(t, t.TempDir()), "--listen", "0.0.0.0:0")
	base := strings.Replace(s.base, "0.0.0.0", "127.0.0.1", 1)
	waitIngestOn(t, base)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, base+"/mcp", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("POST /mcp on a non-loopback listener = %d, want 404", resp.StatusCode)
	}
	if n := strings.Count(s.stderr.String(), "are off: the server listens on a non-loopback address"); n != 1 {
		t.Errorf("the log says the tools are off %d times, want once:\n%s", n, s.stderr)
	}
	stopOK(t, s)

	// And on loopback nothing of the kind is logged.
	s = startServe(t, dirEnv(t, t.TempDir()))
	if strings.Contains(s.stderr.String(), "are off:") {
		t.Errorf("the log says the tools are off on loopback:\n%s", s.stderr)
	}
	stopOK(t, s)
}

// heldReader holds every Alive until released, and says whether the store was
// found closed when the held read went on.
type heldReader struct {
	store.Store
	entered     chan struct{}
	release     chan struct{}
	once        sync.Once
	foundClosed atomic.Bool
	finished    atomic.Int32
}

func newHeldReader(st store.Store) *heldReader {
	return &heldReader{Store: st, entered: make(chan struct{}, 16), release: make(chan struct{})}
}

func (h *heldReader) Alive(ctx context.Context, fp identity.Fingerprint, at time.Time, sc store.Scope) (bool, error) {
	h.entered <- struct{}{}
	<-h.release
	// The read goes on after the call was cancelled, as one already inside the
	// store does.
	alive, err := h.Store.Alive(context.WithoutCancel(ctx), fp, at, sc)
	if errors.Is(err, store.ErrClosed) {
		h.foundClosed.Store(true)
	}
	h.finished.Add(1)
	return alive, err
}

// open lets every held read go; it may be called more than once.
func (h *heldReader) open() { h.once.Do(func() { close(h.release) }) }

func (h *heldReader) waitEntered(t testing.TB, n int) {
	t.Helper()
	for range n {
		select {
		case <-h.entered:
		case <-time.After(15 * time.Second):
			t.Fatal("a call did not reach the store")
		}
	}
}

// probe says whether the store answers a read.
func probe(st *pebblestore.Store) error {
	fp, err := identity.NewResolver(catalog.Default()).Resolve(catalog.K8sPod, []identity.Attr{{Key: catalog.K8sPodUID, Value: "probe"}})
	if err != nil {
		return err
	}
	_, err = st.Alive(context.Background(), fp.Fingerprint(), time.Now(), store.Current(catalog.L2))
	return err
}

// stopFixture is a store with the tools attached over a reader that holds its
// reads, and three calls sent to the tools: two reading, the third queued for a
// slot.
type stopFixture struct {
	st      *pebblestore.Store
	reads   *mcphttp.Server
	held    *heldReader
	in      *ingest
	log     *slog.Logger
	logs    *lockedBuffer
	wg      sync.WaitGroup
	answers [3]atomic.Pointer[mcpResult]
}

func newStopFixture(t *testing.T) *stopFixture {
	t.Helper()
	f := &stopFixture{logs: &lockedBuffer{}}
	f.log = slog.New(slog.NewTextHandler(f.logs, nil))
	rank := serveRank("", false)
	st, err := pebblestore.Open(filepath.Join(t.TempDir(), "store"), serveStoreOptions("", false, nil))
	if err != nil {
		t.Fatal(err)
	}
	f.st = st
	t.Cleanup(func() { _ = st.Close() }) // a second Close is the store's to refuse
	f.reads, err = mcphttp.New(mcphttp.Config{Rank: rank})
	if err != nil {
		t.Fatal(err)
	}
	f.held = newHeldReader(st)
	if err := f.reads.Attach(f.held); err != nil {
		t.Fatal(err)
	}
	f.in = &ingest{st: st}
	return f
}

// send sends the three calls to the handler at base.
func (f *stopFixture) send(t *testing.T, base string) {
	t.Helper()
	for i := range f.answers {
		f.wg.Add(1)
		go func() {
			defer f.wg.Done()
			res, _, _ := mcpCall(context.Background(), base, "resolve_entity", resolveAtArgs("x", time.Now()))
			f.answers[i].Store(&res)
		}()
	}
	f.held.waitEntered(t, 2)
	time.Sleep(100 * time.Millisecond) // the third has reached the gate
}

// A stop ends the tools before it closes the store: a call that is reading and a
// call that is queued are waited for, and the store closes only after both have
// returned.
func TestStopDrainsTheToolsBeforeTheStoreCloses(t *testing.T) {
	f := newStopFixture(t)
	ts := httptest.NewServer(f.mux())
	defer ts.Close()
	defer f.held.open() // a failed test must not leave the calls held
	f.send(t, ts.URL)

	drained := make(chan bool, 1)
	go func() { drained <- drainReads(f.reads, f.log) }()
	select {
	case ok := <-drained:
		t.Fatalf("the drain returned (%v) while two calls were reading", ok)
	case <-time.After(300 * time.Millisecond):
	}
	if err := probe(f.st); err != nil {
		t.Fatalf("the store was closed during the drain: %v", err)
	}

	f.held.open()
	if ok := <-drained; !ok {
		t.Fatal("the drain failed")
	}
	f.in.stop(f.log, true)
	f.wg.Wait()
	if f.held.foundClosed.Load() {
		t.Error("a call found the store closed")
	}
	if err := probe(f.st); !errors.Is(err, store.ErrClosed) {
		t.Errorf("the store is not closed after the stop: %v", err)
	}
	if !strings.Contains(f.logs.String(), "tools stopped") || !strings.Contains(f.logs.String(), "store closed") {
		t.Errorf("the log does not time the steps:\n%s", f.logs)
	}
}

// A drain that times out says so, and the stop leaves the store open: closing it
// under a read is not an option.
func TestStopLeavesTheStoreOpenWhenTheDrainFails(t *testing.T) {
	setSeam(t, &mcpDrainTimeout, 100*time.Millisecond)
	f := newStopFixture(t)
	ts := httptest.NewServer(f.mux())
	defer ts.Close()
	defer f.held.open() // a failed test must not leave the calls held
	f.send(t, ts.URL)

	if drainReads(f.reads, f.log) {
		t.Fatal("the drain reported success while two calls were reading")
	}
	f.in.stop(f.log, false)
	if err := probe(f.st); err != nil {
		t.Errorf("the store was closed under a read: %v", err)
	}
	for _, want := range []string{"the tools did not stop in time", "the store was not closed"} {
		if !strings.Contains(f.logs.String(), want) {
			t.Errorf("the log does not say %q:\n%s", want, f.logs)
		}
	}
	f.held.open()
	f.wg.Wait()
	if f.held.foundClosed.Load() {
		t.Error("a call found the store closed")
	}
}

func (f *stopFixture) mux() http.Handler {
	mux := http.NewServeMux()
	mux.Handle(mcpPattern, f.reads.Handler())
	return mux
}

// The whole of it, as a signal sends it: serve is stopped while a call is reading
// and another is queued. The server's own shutdown gives up on them after its
// timeout and cuts their connections; the stop then waits for the calls and
// closes the store after both have returned.
func TestServeStopWaitsForTheToolsBeforeClosingTheStore(t *testing.T) {
	if testing.Short() {
		t.Skip("the server's shutdown waits eight seconds for the calls")
	}
	var held *heldReader
	var mu sync.Mutex
	setSeam(t, &readerFor, func(st *pebblestore.Store) store.Store {
		mu.Lock()
		defer mu.Unlock()
		held = newHeldReader(st)
		return held
	})
	dir := t.TempDir()
	s := startServe(t, dirEnv(t, dir))
	waitIngestOn(t, s.base)
	mu.Lock()
	h := held
	mu.Unlock()
	if h == nil {
		t.Fatal("the tools were not handed the store")
	}
	t.Cleanup(h.open)

	var wg sync.WaitGroup
	for range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, _ = mcpCall(context.Background(), s.base, "resolve_entity", resolveAtArgs("x", time.Now()))
		}()
	}
	h.waitEntered(t, 2)
	time.Sleep(100 * time.Millisecond)

	s.cancel() // the signal
	deadline := time.Now().Add(20 * time.Second)
	for !strings.Contains(s.stderr.String(), "forced close of connections still open") {
		if time.Now().After(deadline) {
			t.Fatalf("the server did not give up on the calls:\n%s", s.stderr)
		}
		time.Sleep(20 * time.Millisecond)
	}
	// The server has returned and the tools are being drained; the calls are held.
	time.Sleep(300 * time.Millisecond)
	select {
	case code := <-s.done:
		s.done <- code
		t.Fatalf("serve exited (%d) while two calls were reading:\n%s", code, s.stderr)
	default:
	}
	if strings.Contains(s.stderr.String(), "store closed") {
		t.Fatalf("the store was closed while two calls were reading:\n%s", s.stderr)
	}

	h.open()
	select {
	case code := <-s.done:
		s.done <- code
		if code != 0 {
			t.Errorf("exit code %d:\n%s", code, s.stderr)
		}
	case <-time.After(20 * time.Second):
		t.Fatalf("serve did not stop once the calls returned:\n%s", s.stderr)
	}
	wg.Wait()
	if h.foundClosed.Load() {
		t.Error("a call found the store closed")
	}
	log := s.stderr.String()
	if i, j := strings.Index(log, "tools stopped"), strings.Index(log, "store closed"); i < 0 || j < 0 || i > j {
		t.Errorf("the tools stopped at %d and the store closed at %d; want the tools first:\n%s", i, j, log)
	}
}
