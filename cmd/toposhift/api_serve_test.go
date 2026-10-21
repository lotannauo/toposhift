package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/mcp/apihttp"
	"github.com/lotannauo/toposhift/internal/mcp/mcphttp"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// scopeBody is a scope question about the pods the ingest tests post.
func scopeBody(t testing.TB, pod string, from, to time.Time) map[string]any {
	t.Helper()
	return map[string]any{"scope": []string{pod}, "from": from.Format(time.RFC3339), "to": to.Format(time.RFC3339)}
}

// The routes are served on serve's own listener, from the store ingest fills,
// beside the tools: a pod that was answered 200 is in the subgraph of its scope,
// named, at the token the tools report, and the answer is labelled derived.
func TestServeServesTheSubgraphRoutesFromTheStore(t *testing.T) {
	t.Parallel()
	s := startServe(t, dirEnv(t, t.TempDir()))
	waitIngestOn(t, s.base)
	first := time.Now().Add(-10 * time.Minute).Truncate(time.Second)
	postOK(t, s.base, podBody("web-0", first))

	res := mcpOK(t, s.base, "resolve_entity", resolveAtArgs("web-0", first.Add(time.Minute)))
	pod := res.Structured["entity"].(map[string]any)["id"].(string)
	token := res.Structured["token"].(string)

	status, header, body := apiPost(t, s.base, apihttp.PathScope, scopeBody(t, pod, first, first.Add(time.Hour)))
	if status != http.StatusOK {
		t.Fatalf("status %d: %s", status, body)
	}
	ans := decodeObject(t, body)
	doc := obj(ans["document"])
	if ans["token"] != token || doc["token"] != token || ans["derived"] != true || ans["question"] != "scope" {
		t.Errorf("token %v, document %v, derived %v, question %v; want %s, derived, scope", ans["token"], doc["token"], ans["derived"], ans["question"], token)
	}
	names := arr(ans["names"])
	if len(names) == 0 || obj(names[0])["id"] != pod || obj(names[0])["name"] != "web-0" {
		t.Errorf("names %v, want the pod named web-0", ans["names"])
	}
	if header.Get("Cache-Control") != "no-store" {
		t.Errorf("Cache-Control %q", header.Get("Cache-Control"))
	}
	// Only POST.
	if status := get(t, s.base+apihttp.PathScope); status != http.StatusMethodNotAllowed {
		t.Errorf("GET %s = %d, want 405", apihttp.PathScope, status)
	}
	stopOK(t, s)
}

// With no data directory there is no store to read, and a route says so with the
// sentence the receiver gives: the source is the server's, set by the same call.
func TestServeAnswersTheSubgraphRoutesBeforeThereIsAStore(t *testing.T) {
	t.Parallel()
	s := startServe(t, testEnv(t, map[string]string{}))
	status, _, body := apiPost(t, s.base, apihttp.PathBlastRadius, map[string]any{
		"entity": "k8s.node:" + strings.Repeat("0", 32), "from": "2026-01-01T00:00:00Z", "to": "2026-01-01T01:00:00Z",
	})
	var e struct {
		Error struct{ Code, Message string }
	}
	if err := json.Unmarshal(body, &e); err != nil || status != http.StatusServiceUnavailable || e.Error.Code != "unavailable" || e.Error.Message != sentenceNoDirectory {
		t.Errorf("%d %s, want 503 unavailable with %q", status, body, sentenceNoDirectory)
	}
	stopOK(t, s)
}

// On a listener that is not loopback neither the tools nor the routes are
// mounted, and one line says so.
func TestServeDoesNotMountTheSubgraphRoutesOffLoopback(t *testing.T) {
	t.Parallel()
	s := startServe(t, dirEnv(t, t.TempDir()), "--listen", "0.0.0.0:0")
	base := strings.Replace(s.base, "0.0.0.0", "127.0.0.1", 1)
	waitIngestOn(t, base)
	for _, p := range apihttp.Paths() {
		if status, _, body := apiPost(t, base, p, map[string]any{}); status != http.StatusNotFound {
			t.Errorf("POST %s on a non-loopback listener = %d (%s), want 404", p, status, body)
		}
	}
	if n := strings.Count(s.stderr.String(), "and the subgraph routes under "+apiPrefix+" are off: the server listens on a non-loopback address"); n != 1 {
		t.Errorf("the log says the routes are off %d times, want once:\n%s", n, s.stderr)
	}
	stopOK(t, s)

	s = startServe(t, dirEnv(t, t.TempDir()))
	if strings.Contains(s.stderr.String(), "are off:") {
		t.Errorf("the log says the routes are off on loopback:\n%s", s.stderr)
	}
	stopOK(t, s)
}

// The routes take their source from the server they were given: the sentence it
// is told to give while there is no store, the store once it has one, the same
// token the tools answer at, and the refusal once it is drained, without a second
// attach or a second drain.
func TestAPIRoutesRunOnTheToolsServer(t *testing.T) {
	rank := serveRank("", false)
	st, err := memstore.Open(memstore.Options{Policy: lifecycle.Policy{Rank: rank, BootKey: lifecycle.BootID}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	res, err := identity.NewResolver(catalog.Default()).Resolve(catalog.K8sPod, []identity.Attr{{Key: catalog.K8sPodUID, Value: "uid-1"}})
	if err != nil {
		t.Fatal(err)
	}
	pod := res.Fingerprint()
	layer, _ := catalog.Default().Entity(catalog.K8sPod)
	err = st.Write(t.Context(), []store.Record{{
		Seq: 1, Layer: layer.Layer(), Subject: store.EntitySubject(pod), Producer: "k8sobjects/c1",
		EventTime: time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC), Kind: lifecycle.Observe, Payload: []byte(`{"name":"web-0","namespace":"shop"}`),
	}})
	if err != nil {
		t.Fatal(err)
	}

	reads, err := mcphttp.New(mcphttp.Config{Rank: rank})
	if err != nil {
		t.Fatal(err)
	}
	reads.Unavailable("The store is opening, as the test says.")
	mux := http.NewServeMux()
	mux.Handle(mcpPattern, reads.Handler())
	for _, r := range apiRoutes(reads) {
		mux.Handle(r.Pattern, r.Handler)
	}
	ts := httptest.NewServer(mux)
	defer ts.Close()
	args := scopeBody(t, pod.String(), time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC), time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC))
	refusal := func() (int, string) {
		status, _, body := apiPost(t, ts.URL, apihttp.PathScope, args)
		var e struct {
			Error struct{ Code, Message string }
		}
		_ = json.Unmarshal(body, &e)
		return status, e.Error.Code + ": " + e.Error.Message
	}

	if status, said := refusal(); status != http.StatusServiceUnavailable || said != "unavailable: The store is opening, as the test says." {
		t.Errorf("before a store: %d %s", status, said)
	}
	if err := reads.Attach(st); err != nil {
		t.Fatal(err)
	}
	status, _, body := apiPost(t, ts.URL, apihttp.PathScope, args)
	if status != http.StatusOK {
		t.Fatalf("after Attach: %d %s", status, body)
	}
	if tool := mcpOK(t, ts.URL, "resolve_entity", resolveAtArgs("uid-1", time.Date(2026, 1, 1, 9, 30, 0, 0, time.UTC))); tool.Structured["token"] != decodeObject(t, body)["token"] {
		t.Errorf("the tools answer at token %v, the route at %v", tool.Structured["token"], decodeObject(t, body)["token"])
	}
	if err := reads.Drain(t.Context()); err != nil {
		t.Fatal(err)
	}
	if status, said := refusal(); status != http.StatusServiceUnavailable || !strings.HasPrefix(said, "shutting_down: ") {
		t.Errorf("after Drain: %d %s", status, said)
	}
}
