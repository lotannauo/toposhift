package mcphttp

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/store"
)

// callBody is a tools/call request for describe_schema, which reads no store.
const callBody = `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"describe_schema","arguments":{}}}`

// serve mounts the handler as serve does and returns the server's URL.
func serve(t testing.TB, s *Server) string {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle("POST /mcp", s.Handler())
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	return ts.URL + "/mcp"
}

// send posts body as the headers say and returns the status and content type of
// the answer. A header set to "-" is left out.
func send(t testing.TB, url, host, contentType string, body []byte, extra map[string]string) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if host != "" {
		req.Host = host
	}
	if contentType != "-" {
		req.Header.Set("Content-Type", contentType)
	}
	req.Header.Set("Accept", "application/json, text/event-stream")
	for k, v := range extra {
		req.Header.Set(k, v)
	}
	resp, err := (&http.Client{Transport: &http.Transport{DisableKeepAlives: true}}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, resp.Header.Get("Content-Type")
}

func status(t testing.TB, url, host, contentType string, body []byte, extra map[string]string) int {
	t.Helper()
	code, _ := send(t, url, host, contentType, body, extra)
	return code
}

// What the endpoint refuses before a tool is reached, and what it takes.
func TestEndpointProtections(t *testing.T) {
	url := serve(t, newServer(t, nil))
	body := []byte(callBody)

	if got := status(t, url, "", "application/json", body, nil); got != http.StatusOK {
		t.Fatalf("a plain call: status %d, want 200", got)
	}
	if _, ctype := send(t, url, "", "application/json", body, nil); !strings.HasPrefix(ctype, "application/json") {
		t.Errorf("the answer is %q, want JSON and not an event stream", ctype)
	}

	cases := []struct {
		name        string
		host        string
		contentType string
		body        []byte
		extra       map[string]string
		want        int
	}{
		{"a Host that is not loopback, as DNS rebinding sends it", "evil.example", "application/json", body, nil, http.StatusForbidden},
		{"a Host that only starts like loopback", "127.0.0.1.evil.example", "application/json", body, nil, http.StatusForbidden},
		{
			"a cross-site browser POST", "", "application/json", body,
			map[string]string{"Sec-Fetch-Site": "cross-site", "Origin": "http://evil.example"},
			http.StatusForbidden,
		},
		{
			"a cross-origin POST with only an Origin", "", "application/json", body,
			map[string]string{"Origin": "http://evil.example"},
			http.StatusForbidden,
		},
		{
			"a same-origin browser POST", "", "application/json", body,
			map[string]string{"Sec-Fetch-Site": "same-origin"},
			http.StatusOK,
		},
		{"text/plain", "", "text/plain", body, nil, http.StatusUnsupportedMediaType},
		{"no content type", "", "-", body, nil, http.StatusUnsupportedMediaType},
		{"a body over 64 KiB", "", "application/json", padded(MaxBodyBytes + 1024), nil, http.StatusRequestEntityTooLarge},
		{"a body of exactly the cap", "", "application/json", padded(MaxBodyBytes), nil, http.StatusOK},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := status(t, url, c.host, c.contentType, c.body, c.extra); got != c.want {
				t.Errorf("status %d, want %d", got, c.want)
			}
		})
	}

	// Only POST: the handler keeps no session to read or to end.
	for _, method := range []string{http.MethodGet, http.MethodDelete} {
		req, _ := http.NewRequestWithContext(ctx, method, url, nil)
		req.Header.Set("Accept", "application/json, text/event-stream")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Errorf("%s: status %d, want 405", method, resp.StatusCode)
		}
	}
}

// padded is a valid request of n bytes: the call with spaces after it.
func padded(n int) []byte {
	b := []byte(callBody)
	return append(b, bytes.Repeat([]byte(" "), n-len(b))...)
}

// The handler checks the address the connection arrived on, not the address the
// server was asked to listen on.
func TestRefusesAConnectionNotOnLoopback(t *testing.T) {
	h := newServer(t, nil).Handler()
	try := func(local net.Addr) int {
		req := httptest.NewRequestWithContext(ctx, http.MethodPost, "http://127.0.0.1:7070/mcp", strings.NewReader(callBody))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json, text/event-stream")
		if local != nil {
			req = req.WithContext(context.WithValue(req.Context(), http.LocalAddrContextKey, local))
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec.Code
	}
	cases := []struct {
		name  string
		local net.Addr
		want  int
	}{
		{"IPv4 loopback", &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 7070}, http.StatusOK},
		{"another IPv4 loopback address", &net.TCPAddr{IP: net.ParseIP("127.9.9.9"), Port: 7070}, http.StatusOK},
		{"IPv6 loopback", &net.TCPAddr{IP: net.ParseIP("::1"), Port: 7070}, http.StatusOK},
		{"a private address", &net.TCPAddr{IP: net.ParseIP("10.0.0.5"), Port: 7070}, http.StatusForbidden},
		{"a public address", &net.TCPAddr{IP: net.ParseIP("192.0.2.7"), Port: 7070}, http.StatusForbidden},
		{"the unspecified address", &net.TCPAddr{IP: net.IPv4zero, Port: 7070}, http.StatusForbidden},
		{"an IPv6 address", &net.TCPAddr{IP: net.ParseIP("2001:db8::1"), Port: 7070}, http.StatusForbidden},
		{"no local address at all", nil, http.StatusForbidden},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := try(c.local); got != c.want {
				t.Errorf("status %d, want %d", got, c.want)
			}
		})
	}
}

// A listener on every address, reached through an address of this machine that is
// not loopback, is refused: the case the configured address cannot see.
func TestRefusesAnOutsideAddressOnAWildcardListener(t *testing.T) {
	var outside net.IP
	addrs, _ := net.InterfaceAddrs()
	for _, a := range addrs {
		if ipn, ok := a.(*net.IPNet); ok && !ipn.IP.IsLoopback() && ipn.IP.To4() != nil && ipn.IP.IsGlobalUnicast() {
			outside = ipn.IP
			break
		}
	}
	if outside == nil {
		t.Skip("this machine has no non-loopback IPv4 address")
	}
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp4", "0.0.0.0:0")
	if err != nil {
		t.Skipf("cannot listen on every address: %v", err)
	}
	srv := &http.Server{Handler: newServer(t, nil).Handler()}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	port := ln.Addr().(*net.TCPAddr).Port

	for _, c := range []struct {
		ip   net.IP
		want int
	}{{net.IPv4(127, 0, 0, 1), http.StatusOK}, {outside, http.StatusForbidden}} {
		url := "http://" + net.JoinHostPort(c.ip.String(), strconv.Itoa(port)) + "/"
		if got := status(t, url, "", "application/json", []byte(callBody), nil); got != c.want {
			t.Errorf("through %s: status %d, want %d", c.ip, got, c.want)
		}
	}
}

// A client of the SDK, over HTTP, lists the tools and calls one.
func TestClientOverHTTP(t *testing.T) {
	w := newWorld(t, rankObjs)
	url := serve(t, attached(t, w.st, rankObjs))
	cs, err := sdk.NewClient(&sdk.Implementation{Name: "test", Version: "0"}, nil).Connect(ctx,
		&sdk.StreamableClientTransport{Endpoint: url, DisableStandaloneSSE: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	lt, err := cs.ListTools(ctx, nil)
	if err != nil || len(lt.Tools) != 9 {
		t.Fatalf("%d tools, %v", len(lt.Tools), err)
	}
	res := callTool(t, cs, "neighbors_at", map[string]any{"entity": w.n1.String(), "at": "2026-01-01T10:08:00Z"})
	m := answer(t, "neighbors_at", res)
	if m["token"] != tokenOf(w.st) {
		t.Errorf("token %v, want %s", m["token"], tokenOf(w.st))
	}
}

// ctxStore blocks every Alive until the call's context ends, and reports it.
type ctxStore struct {
	store.Store
	entered chan struct{}
	ended   chan error
}

func (c *ctxStore) Alive(ctx context.Context, _ identity.Fingerprint, _ time.Time, _ store.Scope) (bool, error) {
	c.entered <- struct{}{}
	<-ctx.Done()
	c.ended <- ctx.Err()
	return false, ctx.Err()
}

// A client that goes away ends the call it was waiting for: the handler's context
// follows the request's, so a read stops within one store read.
func TestRequestCancellationReachesTheStore(t *testing.T) {
	w := newWorld(t, rankObjs)
	cstore := &ctxStore{Store: w.st, entered: make(chan struct{}, 1), ended: make(chan error, 1)}
	url := serve(t, attached(t, cstore, rankObjs))
	cs, err := sdk.NewClient(&sdk.Implementation{Name: "test", Version: "0"}, nil).Connect(ctx,
		&sdk.StreamableClientTransport{Endpoint: url, DisableStandaloneSSE: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cs.Close() })

	callCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		_, _ = cs.CallTool(callCtx, &sdk.CallToolParams{Name: "resolve_entity", Arguments: resolveAt(w)})
	}()
	select {
	case <-cstore.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the call did not reach the store")
	}
	cancel()
	select {
	case err := <-cstore.ended:
		if err == nil {
			t.Error("the read ended with no error")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the read went on after the client went away")
	}
}
