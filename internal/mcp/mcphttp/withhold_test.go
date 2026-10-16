package mcphttp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	core "github.com/lotannauo/toposhift/internal/mcp"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// topoEndpoint matches the text of any endpoint id.
var topoEndpoint = regexp.MustCompile(`topo\.endpoint:[0-9a-f]{32}`)

const w1012 = "2026-01-01T10:12:00Z"

var errBoom = errors.New("boom: the disk is on fire")

// addrStore is a pod P with two addresses, scheduled on a node N.
type addrStore struct {
	st     *memstore.Store
	p, n   identity.Fingerprint
	e1, e2 identity.Fingerprint
}

func newAddrStore(t testing.TB) *addrStore {
	t.Helper()
	st := openMem(t, memstore.Options{})
	a := &addrStore{
		st: st,
		p:  fpOf(t, catalog.K8sPod, catalog.K8sPodUID, "addr-pod-p"),
		n:  fpOf(t, catalog.K8sNode, catalog.K8sNodeUID, "addr-node-n"),
		e1: fpOf(t, catalog.NetworkEndpoint, catalog.EndpointRealm, "k8s/c1", catalog.EndpointAddress, "10.42.0.7"),
		e2: fpOf(t, catalog.NetworkEndpoint, catalog.EndpointRealm, "k8s/c1", catalog.EndpointAddress, "10.42.0.8"),
	}
	nine := hm(9, 0)
	newFeed(t, st).write(
		observe(ent(a.p), objs, nine, 0, `{"name":"web-a","namespace":"shop","pod_ips":["10.42.0.7","10.42.0.8"]}`),
		observe(ent(a.n), objs, nine, 0, `{"name":"worker-n"}`),
		observe(ent(a.e1), objs, nine, 0, `{}`),
		observe(ent(a.e2), objs, nine, 0, `{}`),
		observe(edge(a.p, a.n, catalog.ScheduledOn), objs, nine, 0, `{}`),
		observe(edge(a.p, a.e1, catalog.HasAddress), objs, nine, 0, `{}`),
		observe(edge(a.p, a.e2, catalog.HasAddress), objs, nine, 0, `{}`),
	)
	return a
}

// failAlive fails the read of one entity with an error that prints its id, and
// its hash in hex, as a store does. With sentence set the failure is a sentence
// the tool made, whose cause holds the raw error.
type failAlive struct {
	store.Store
	fp       identity.Fingerprint
	sentence bool
}

func (f failAlive) Alive(ctx context.Context, fp identity.Fingerprint, t time.Time, sc store.Scope) (bool, error) {
	if fp == f.fp {
		h := fp.Hash()
		// The key as pebblestore prints it: layer, type id, hash, direction, 17 bytes.
		key := append(append([]byte{2, 0, 12}, h[:]...), append([]byte{0}, make([]byte, 17)...)...)
		raw := fmt.Errorf("memstore: Alive: folding %s (key %x): %w", fp, key, errBoom)
		if f.sentence {
			return false, &core.Error{Message: "A read of " + fp.String() + " failed.", Cause: raw}
		}
		return false, raw
	}
	return f.Store.Alive(ctx, fp, t, sc)
}

// tee records every byte the handler writes.
type tee struct {
	http.ResponseWriter
	mu  *sync.Mutex
	buf *bytes.Buffer
}

func (t tee) Write(b []byte) (int, error) {
	t.mu.Lock()
	t.buf.Write(b)
	t.mu.Unlock()
	return t.ResponseWriter.Write(b)
}

func (t tee) Flush() {
	if f, ok := t.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// wire is what an HTTP server wrote.
type wire struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (w *wire) take() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	s := w.buf.String()
	w.buf.Reset()
	return s
}

// serveWire mounts the handler as serve does, recording every byte it writes.
func serveWire(t testing.TB, s *Server) (string, *wire) {
	t.Helper()
	rec := &wire{}
	h := s.Handler()
	mux := http.NewServeMux()
	mux.Handle("POST /mcp", http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		h.ServeHTTP(tee{ResponseWriter: rw, mu: &rec.mu, buf: &rec.buf}, r)
	}))
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	return ts.URL + "/mcp", rec
}

func httpClient(t testing.TB, url string) *sdk.ClientSession {
	t.Helper()
	cs, err := sdk.NewClient(&sdk.Implementation{Name: "test", Version: "0"}, nil).Connect(ctx,
		&sdk.StreamableClientTransport{Endpoint: url, DisableStandaloneSSE: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	return cs
}

// withheldEdges is counts.withheld.edges of a neighbors_at answer.
func withheldEdges(t testing.TB, m map[string]any) float64 {
	t.Helper()
	counts, _ := m["counts"].(map[string]any)
	withheld, _ := counts["withheld"].(map[string]any)
	n, ok := withheld["edges"].(float64)
	if !ok {
		t.Fatalf("no counts.withheld.edges in %v", counts)
	}
	return n
}

// Over HTTP an answer, in its structured and in its text content and on the wire,
// prints no endpoint id the request did not name; a request that names one still
// gets it; and a failing read prints none in its error.
func TestWithholdOverHTTP(t *testing.T) {
	a := newAddrStore(t)
	url, rec := serveWire(t, attached(t, a.st, rankObjs))
	cs := httpClient(t, url)
	rec.take()

	res := callTool(t, cs, "neighbors_at", map[string]any{"entity": a.p.String(), "at": w1012})
	m := answer(t, "neighbors_at of P", res)
	structured, _ := json.Marshal(res.StructuredContent)
	for what, got := range map[string]string{"structured content": string(structured), "text content": text(t, res), "wire": rec.take()} {
		if ids := topoEndpoint.FindAllString(got, -1); len(ids) > 0 {
			t.Errorf("neighbors_at of P: the %s prints %v", what, ids)
		}
	}
	if n := withheldEdges(t, m); n != 2 {
		t.Errorf("counts.withheld.edges %v, want 2", n)
	}
	if !strings.Contains(notes(m), "2 edges to endpoints the request did not name were withheld") {
		t.Errorf("notes %q", notes(m))
	}

	res = callTool(t, cs, "neighbors_at", map[string]any{"entity": a.e1.String(), "at": w1012, "direction": "reverse"})
	m = answer(t, "neighbors_at of E1", res)
	edges, _ := m["edges"].([]any)
	if len(edges) != 1 || withheldEdges(t, m) != 0 {
		t.Fatalf("the reverse edges of E1: %v", m["edges"])
	}
	if peer := edges[0].(map[string]any)["peer"].(map[string]any); peer["id"] != a.p.String() {
		t.Errorf("peer %v, want P", peer)
	}
	for what, got := range map[string]string{"text content": text(t, res), "wire": rec.take()} {
		if ids := topoEndpoint.FindAllString(got, -1); len(ids) == 0 || strings.Contains(strings.Join(ids, " "), a.e2.String()) {
			t.Errorf("neighbors_at of E1: the %s prints %v, want E1 alone", what, ids)
		}
	}

	// A failing read of E1, which the request did not name.
	fail := attached(t, failAlive{Store: a.st, fp: a.e1}, rankObjs)
	url, rec = serveWire(t, fail)
	cs = httpClient(t, url)
	rec.take()
	res = callTool(t, cs, "neighbors_at", map[string]any{"entity": a.p.String(), "at": w1012})
	if !res.IsError {
		t.Fatal("the failing read was not an error")
	}
	h := a.e1.Hash()
	for what, got := range map[string]string{"text content": text(t, res), "wire": rec.take()} {
		if topoEndpoint.MatchString(got) || strings.Contains(got, hex.EncodeToString(h[:])) {
			t.Errorf("the failing call: the %s prints an id: %q", what, tail(got))
		}
		if !strings.Contains(got, "boom") {
			t.Errorf("the failing call: the %s lost the cause's text: %q", what, tail(got))
		}
	}

	// A sentence the tool made, whose cause holds the raw error: the client is sent
	// the sentence, and never the cause.
	url, rec = serveWire(t, attached(t, failAlive{Store: a.st, fp: a.e1, sentence: true}, rankObjs))
	cs = httpClient(t, url)
	rec.take()
	res = callTool(t, cs, "neighbors_at", map[string]any{"entity": a.p.String(), "at": w1012})
	if !res.IsError {
		t.Fatal("the failing read was not an error")
	}
	for what, got := range map[string]string{"text content": text(t, res), "wire": rec.take()} {
		if topoEndpoint.MatchString(got) || strings.Contains(got, hex.EncodeToString(h[:])) || strings.Contains(got, "boom") {
			t.Errorf("the failing call with a sentence: the %s prints the cause or an id: %q", what, tail(got))
		}
		if !strings.Contains(got, "A read of topo.endpoint:withheld failed.") {
			t.Errorf("the failing call with a sentence: the %s lacks the sentence: %q", what, tail(got))
		}
	}
}

// frame is a tools/call request.
func frame(id int, tool string, args map[string]any) string {
	b, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": id, "method": "tools/call", "params": map[string]any{"name": tool, "arguments": args},
	})
	if err != nil {
		panic(err)
	}
	return string(b) + "\n"
}

// byID reads the lines a session wrote, by the id of the request each answers.
func byID(t testing.TB, out *bytes.Buffer) map[int]string {
	t.Helper()
	lines := map[int]string{}
	sc := bufio.NewScanner(out)
	sc.Buffer(nil, 1<<20)
	for sc.Scan() {
		var m struct {
			ID int `json:"id"`
		}
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
			t.Fatalf("a line of output is not JSON: %q", tail(sc.Text()))
		}
		lines[m.ID] = sc.Text()
	}
	return lines
}

// Over stdio, every byte written holds no endpoint id the request did not name,
// and an error from a failing read holds none.
func TestWithholdOverStdio(t *testing.T) {
	a := newAddrStore(t)
	s := attached(t, a.st, rankObjs)
	var out bytes.Buffer
	frames := initFrame +
		frame(2, "neighbors_at", map[string]any{"entity": a.p.String(), "at": w1012}) +
		frame(3, "neighbors_at", map[string]any{"entity": a.e1.String(), "at": w1012, "direction": "reverse"})
	if err := s.ServeStdio(ctx, holdOpen(frames, 500*time.Millisecond), &out); err != nil {
		t.Fatalf("ServeStdio: %v", err)
	}
	lines := byID(t, &out)
	if ids := topoEndpoint.FindAllString(lines[2], -1); len(ids) > 0 {
		t.Errorf("neighbors_at of P: the output prints %v", ids)
	}
	var m struct {
		Result struct {
			Structured map[string]any `json:"structuredContent"`
		} `json:"result"`
	}
	if err := json.Unmarshal([]byte(lines[2]), &m); err != nil {
		t.Fatal(err)
	}
	if n := withheldEdges(t, m.Result.Structured); n != 2 {
		t.Errorf("counts.withheld.edges %v, want 2", n)
	}
	if ids := topoEndpoint.FindAllString(lines[3], -1); len(ids) == 0 || strings.Contains(lines[3], a.e2.String()) {
		t.Errorf("neighbors_at of E1: the output prints %v, want E1 alone", ids)
	}
	if !strings.Contains(lines[3], a.p.String()) {
		t.Errorf("neighbors_at of E1 does not list P: %q", tail(lines[3]))
	}

	fail := attached(t, failAlive{Store: a.st, fp: a.e1}, rankObjs)
	out.Reset()
	if err := fail.ServeStdio(ctx, holdOpen(initFrame+frame(2, "neighbors_at", map[string]any{"entity": a.p.String(), "at": w1012}), 500*time.Millisecond), &out); err != nil {
		t.Fatalf("ServeStdio: %v", err)
	}
	h := a.e1.Hash()
	all := out.String()
	if topoEndpoint.MatchString(all) || strings.Contains(all, hex.EncodeToString(h[:])) {
		t.Errorf("the failing call: the output prints an id: %q", tail(all))
	}
	if !strings.Contains(all, "boom") || !strings.Contains(all, `"isError":true`) {
		t.Errorf("the failing call: %q", tail(all))
	}

	sentence := attached(t, failAlive{Store: a.st, fp: a.e1, sentence: true}, rankObjs)
	out.Reset()
	if err := sentence.ServeStdio(ctx, holdOpen(initFrame+frame(2, "neighbors_at", map[string]any{"entity": a.p.String(), "at": w1012}), 500*time.Millisecond), &out); err != nil {
		t.Fatalf("ServeStdio: %v", err)
	}
	all = out.String()
	if topoEndpoint.MatchString(all) || strings.Contains(all, hex.EncodeToString(h[:])) || strings.Contains(all, "boom") {
		t.Errorf("the failing call with a sentence: the output prints the cause or an id: %q", tail(all))
	}
	if !strings.Contains(all, "A read of topo.endpoint:withheld failed.") {
		t.Errorf("the failing call with a sentence: %q", tail(all))
	}
}
