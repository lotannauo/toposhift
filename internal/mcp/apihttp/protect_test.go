package apihttp

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/internal/mcp"
)

// The handler checks the address the connection arrived on, not the address the
// server was asked to listen on.
func TestRefusesAConnectionNotOnLoopback(t *testing.T) {
	w := newWorld(t)
	api, _ := apiOn(t, w.st, mcp.Config{})
	good := body(t, map[string]any{"scope": strs(w.p), "from": w10, "to": w1030})
	try := func(local net.Addr) int {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "http://127.0.0.1:7070"+PathScope, strings.NewReader(good))
		req.Header.Set("Content-Type", "application/json")
		if local != nil {
			req = req.WithContext(context.WithValue(req.Context(), http.LocalAddrContextKey, local))
		}
		rec := httptest.NewRecorder()
		api.Handler().ServeHTTP(rec, req)
		return rec.Code
	}
	for _, c := range []struct {
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
	} {
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
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp4", "0.0.0.0:0")
	if err != nil {
		t.Skipf("cannot listen on every address: %v", err)
	}
	w := newWorld(t)
	api, _ := apiOn(t, w.st, mcp.Config{})
	mux := http.NewServeMux()
	mux.Handle(PathScope, api.Handler())
	srv := &http.Server{Handler: mux}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	port := ln.Addr().(*net.TCPAddr).Port
	good := body(t, map[string]any{"scope": strs(w.p), "from": w10, "to": w1030})
	for _, c := range []struct {
		ip   net.IP
		want int
	}{{net.IPv4(127, 0, 0, 1), http.StatusOK}, {outside, http.StatusForbidden}} {
		base := "http://" + net.JoinHostPort(c.ip.String(), strconv.Itoa(port))
		if got := post(t, base, PathScope, good); got.status != c.want {
			t.Errorf("through %s: status %d, want %d", c.ip, got.status, c.want)
		}
	}
}

// The Host header is the name the client believes it is talking to: a page on
// another site whose name was made to resolve to this machine sends its own.
func TestRefusesAHostThatIsNotLoopback(t *testing.T) {
	w := newWorld(t)
	api, _ := apiOn(t, w.st, mcp.Config{})
	base := serve(t, api)
	good := body(t, map[string]any{"scope": strs(w.p), "from": w10, "to": w1030})
	for _, c := range []struct {
		host string
		want int
	}{
		{"", http.StatusOK},
		{"localhost", http.StatusOK},
		{"localhost:7070", http.StatusOK},
		{"127.0.0.1", http.StatusOK},
		{"127.0.0.1:7070", http.StatusOK},
		{"127.9.9.9:80", http.StatusOK},
		{"[::1]:7070", http.StatusOK},
		{"[::1]", http.StatusOK},
		{"evil.example", http.StatusForbidden},
		{"evil.example:7070", http.StatusForbidden},
		{"127.0.0.1.evil.example", http.StatusForbidden},
		{"localhost.evil.example", http.StatusForbidden},
		{"10.0.0.5:7070", http.StatusForbidden},
		{"0.0.0.0:7070", http.StatusForbidden},
	} {
		headers := map[string]string{}
		if c.host != "" {
			headers["Host"] = c.host
		}
		got := send(t, base, PathScope, good, headers)
		if got.status != c.want {
			t.Errorf("Host %q: status %d, want %d", c.host, got.status, c.want)
		}
		if c.want == http.StatusForbidden && strings.Contains(got.text(), "evil") {
			t.Errorf("Host %q: the refusal echoes it: %s", c.host, got.body)
		}
	}
}

// A browser that says the request comes from another site is refused, whatever
// the content type; one that says it is the page's own is served. A request
// that says nothing, as a command-line client does not, is served.
func TestRefusesACrossSiteRequest(t *testing.T) {
	w := newWorld(t)
	api, _ := apiOn(t, w.st, mcp.Config{})
	base := serve(t, api)
	good := body(t, map[string]any{"scope": strs(w.p), "from": w10, "to": w1030})
	for _, c := range []struct {
		name    string
		headers map[string]string
		want    int
	}{
		{"nothing said", nil, http.StatusOK},
		{"same-origin", map[string]string{"Sec-Fetch-Site": "same-origin"}, http.StatusOK},
		{"none, as a typed address bar", map[string]string{"Sec-Fetch-Site": "none"}, http.StatusOK},
		{"cross-site", map[string]string{"Sec-Fetch-Site": "cross-site"}, http.StatusForbidden},
		{"same-site", map[string]string{"Sec-Fetch-Site": "same-site"}, http.StatusForbidden},
		{"cross-site with an Origin", map[string]string{"Sec-Fetch-Site": "cross-site", "Origin": "http://evil.example"}, http.StatusForbidden},
		{"an Origin alone", map[string]string{"Origin": "http://evil.example"}, http.StatusForbidden},
	} {
		if got := send(t, base, PathScope, good, c.headers); got.status != c.want {
			t.Errorf("%s: status %d, want %d", c.name, got.status, c.want)
		}
	}
	// No route answers a preflight, and none says who may call it.
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodOptions, base+PathScope, nil)
	req.Header.Set("Origin", "http://evil.example")
	req.Header.Set("Access-Control-Request-Method", "POST")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNoContent || resp.Header.Get("Access-Control-Allow-Origin") != "" {
		t.Errorf("a preflight is answered %d with Access-Control-Allow-Origin %q", resp.StatusCode, resp.Header.Get("Access-Control-Allow-Origin"))
	}
}

// Routes answers a valid POST with the headers of every response, and the other
// checks of the request still apply; but it does not refuse a Host that is not
// loopback, a cross-site request or a connection that is not on loopback, which a
// verifier placed in front of it decides.
func TestRoutesLeavesTheProtectionsToTheCaller(t *testing.T) {
	w := newWorld(t)
	api, _ := apiOn(t, w.st, mcp.Config{})
	good := body(t, map[string]any{"scope": strs(w.p), "from": w10, "to": w1030})
	do := func(method, contentType, host string, headers map[string]string, local net.Addr) *httptest.ResponseRecorder {
		req := httptest.NewRequestWithContext(t.Context(), method, "http://"+host+PathScope, strings.NewReader(good))
		if contentType != "" {
			req.Header.Set("Content-Type", contentType)
		}
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		if local != nil {
			req = req.WithContext(context.WithValue(req.Context(), http.LocalAddrContextKey, local))
		}
		rec := httptest.NewRecorder()
		api.Routes().ServeHTTP(rec, req)
		return rec
	}
	loop := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 7070}
	rec := do(http.MethodPost, "application/json", "127.0.0.1:7070", nil, loop)
	if rec.Code != http.StatusOK {
		t.Fatalf("a valid POST: %d %s", rec.Code, rec.Body)
	}
	for k, want := range map[string]string{"Cache-Control": "no-store", "X-Content-Type-Options": "nosniff", "Referrer-Policy": "no-referrer", "Content-Type": "application/json; charset=utf-8"} {
		if got := rec.Header().Get(k); got != want {
			t.Errorf("%s is %q, want %q", k, got, want)
		}
	}
	// Not refused here: a Host that is not loopback, a cross-site request, a
	// connection on another address, and no local address at all.
	for name, rec := range map[string]*httptest.ResponseRecorder{
		"a Host that is not loopback": do(http.MethodPost, "application/json", "evil.example", nil, loop),
		"a cross-site request":        do(http.MethodPost, "application/json", "127.0.0.1:7070", map[string]string{"Sec-Fetch-Site": "cross-site"}, loop),
		"a connection off loopback":   do(http.MethodPost, "application/json", "127.0.0.1:7070", nil, &net.TCPAddr{IP: net.ParseIP("10.0.0.5"), Port: 7070}),
		"no local address":            do(http.MethodPost, "application/json", "127.0.0.1:7070", nil, nil),
	} {
		if rec.Code != http.StatusOK {
			t.Errorf("%s: %d, want it served: the caller's verifier decides", name, rec.Code)
		}
	}
	// The checks of the request itself stay.
	if rec := do(http.MethodPost, "text/plain", "127.0.0.1:7070", nil, loop); rec.Code != http.StatusUnsupportedMediaType {
		t.Errorf("text/plain: %d, want 415", rec.Code)
	}
	if rec := do(http.MethodGet, "application/json", "127.0.0.1:7070", nil, loop); rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET: %d, want 405", rec.Code)
	}
}
