package server_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/server"
)

func TestDefaultConfigIsLoopback(t *testing.T) {
	cfg := server.DefaultConfig()
	if !cfg.IsLoopback() {
		t.Fatalf("default listen address %q must be loopback", cfg.Listen)
	}
}

func TestIsLoopback(t *testing.T) {
	tests := []struct {
		listen string
		want   bool
	}{
		{"127.0.0.1:7070", true},
		{"[::1]:7070", true},
		{"localhost:7070", true},
		{"0.0.0.0:7070", false},
		{"[::]:7070", false},
		{":7070", false},
		{"10.0.0.5:7070", false},
		{"not an address", false},
	}
	for _, tt := range tests {
		t.Run(tt.listen, func(t *testing.T) {
			if got := (server.Config{Listen: tt.listen}).IsLoopback(); got != tt.want {
				t.Errorf("IsLoopback(%q) = %v, want %v", tt.listen, got, tt.want)
			}
		})
	}
}

func TestServeHealthzAndGracefulShutdown(t *testing.T) {
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, ln, discardLogger()) }()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+ln.Addr().String()+"/healthz", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || string(body) != "ok\n" {
		t.Fatalf("GET /healthz = %d %q, want 200 \"ok\\n\"", resp.StatusCode, body)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve returned %v after cancel, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Serve did not return after context cancel")
	}
}

func TestRunRejectsEmptyListen(t *testing.T) {
	if err := server.Run(t.Context(), server.Config{}, discardLogger()); err == nil {
		t.Fatal("Run with empty listen address returned nil, want error")
	}
}

func TestServeServesExtraRoutesBesideHealthz(t *testing.T) {
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	route := server.Route{
		Pattern: "POST /v1/logs",
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) }),
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, ln, discardLogger(), route) }()

	base := "http://" + ln.Addr().String()
	for _, tt := range []struct {
		method, path string
		want         int
	}{
		{http.MethodPost, "/v1/logs", http.StatusTeapot},
		{http.MethodGet, "/healthz", http.StatusOK},
		{http.MethodGet, "/v1/logs", http.StatusMethodNotAllowed},
		{http.MethodGet, "/nothing", http.StatusNotFound},
	} {
		req, err := http.NewRequestWithContext(ctx, tt.method, base+tt.path, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := noKeepAlive.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != tt.want {
			t.Errorf("%s %s = %d, want %d", tt.method, tt.path, resp.StatusCode, tt.want)
		}
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve returned %v after cancel, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Serve did not return after context cancel")
	}
}

// noKeepAlive keeps no connection open, so the shutdown at the end of a test
// has none to wait for.
var noKeepAlive = &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}

func TestInvalidRoutesAreErrorsNotPanics(t *testing.T) {
	ok := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	tests := []struct {
		name   string
		routes []server.Route
	}{
		{"conflicts with healthz", []server.Route{{Pattern: "GET /healthz", Handler: ok}}},
		{"conflicts with another route", []server.Route{{Pattern: "GET /a", Handler: ok}, {Pattern: "GET /a", Handler: ok}}},
		{"malformed pattern", []server.Route{{Pattern: "GET", Handler: ok}}},
		{"no pattern", []server.Route{{Handler: ok}}},
		{"no handler", []server.Route{{Pattern: "GET /a"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = ln.Close() }()
			if err := server.Serve(t.Context(), ln, discardLogger(), tt.routes...); err == nil {
				t.Error("Serve with an invalid route returned nil")
			}
			cfg := server.Config{Listen: "127.0.0.1:0", Routes: tt.routes}
			if err := server.Run(t.Context(), cfg, discardLogger()); err == nil {
				t.Error("Run with an invalid route returned nil")
			}
		})
	}
}

const forcedCloseLog = "forced close of connections still open"

// TestShutdownTimeoutExceedsNeverUsedConnectionGrace pins the default. net/http
// treats a connection that never sent a request as idle only after 5 whole
// seconds, noticed by a poll at most 500 ms apart, so a shutdown deadline of
// 6.5 s or less can race that grace period.
func TestShutdownTimeoutExceedsNeverUsedConnectionGrace(t *testing.T) {
	if got := server.ShutdownTimeout(); got <= 6500*time.Millisecond {
		t.Fatalf("shutdown timeout = %v, want more than 6.5s", got)
	}
}

// TestShutdownForcesCloseOfConnectionThatNeverSentARequest reproduces a client
// that dialed a spare connection and keeps it open while another connection is
// served. Graceful shutdown cannot drain the spare one before its deadline,
// which must not turn a clean stop into an error.
func TestShutdownForcesCloseOfConnectionThatNeverSentARequest(t *testing.T) {
	defer server.SetShutdownTimeout(300 * time.Millisecond)()

	ln := listen(t)
	var logs bytes.Buffer
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, ln, bufferLogger(&logs)) }()

	// The spare connection is dialed first and the listener accepts in order,
	// so once the request below is answered the spare one is registered.
	spare, err := (&net.Dialer{}).DialContext(ctx, "tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = spare.Close() }()
	getHealthz(t, ctx, ln)

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve returned %v after cancel, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Serve did not return after context cancel")
	}
	if !strings.Contains(logs.String(), "level=WARN") || !strings.Contains(logs.String(), forcedCloseLog) {
		t.Errorf("log lacks the warning %q:\n%s", forcedCloseLog, logs.String())
	}

	// The server must have closed the spare connection, not just returned.
	// Its header timeout would also close it, but only after 5 s.
	if err := spare.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := spare.Read(make([]byte, 1)); err == nil || errors.Is(err, os.ErrDeadlineExceeded) {
		t.Errorf("read on the spare connection = %v, want it closed by the server", err)
	}
}

func TestShutdownWithNothingOpenDoesNotForceClose(t *testing.T) {
	ln := listen(t)
	var logs bytes.Buffer
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, ln, bufferLogger(&logs)) }()

	getHealthz(t, ctx, ln)

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve returned %v after cancel, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Serve did not return after context cancel")
	}
	if strings.Contains(logs.String(), forcedCloseLog) {
		t.Errorf("log has %q after a shutdown with nothing open:\n%s", forcedCloseLog, logs.String())
	}
	if !strings.Contains(logs.String(), "stopped") {
		t.Errorf("log lacks \"stopped\":\n%s", logs.String())
	}
}

// closeErrListener fails Close after closing, so Shutdown returns an error
// that is not a missed deadline.
type closeErrListener struct {
	net.Listener
	err error
}

func (l closeErrListener) Close() error {
	_ = l.Listener.Close()
	return l.err
}

func TestShutdownErrorOtherThanDeadlineIsReturned(t *testing.T) {
	boom := errors.New("close failed")
	ln := closeErrListener{Listener: listen(t), err: boom}
	var logs bytes.Buffer
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, ln, bufferLogger(&logs)) }()

	getHealthz(t, ctx, ln)

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, boom) {
			t.Fatalf("Serve returned %v after cancel, want an error wrapping %v", err, boom)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Serve did not return after context cancel")
	}
	if strings.Contains(logs.String(), forcedCloseLog) {
		t.Errorf("log has %q for an error that is not a missed deadline:\n%s", forcedCloseLog, logs.String())
	}
}

func listen(t *testing.T) net.Listener {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return ln
}

// getHealthz fetches /healthz on a connection of its own that is closed
// afterwards, so no pooled connection outlives the call.
func getHealthz(t *testing.T, ctx context.Context, ln net.Listener) {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+ln.Addr().String()+"/healthz", nil)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: &http.Transport{}}
	defer client.CloseIdleConnections()
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /healthz = %d, want 200", resp.StatusCode)
	}
}

func bufferLogger(w io.Writer) *slog.Logger {
	return slog.New(slog.NewTextHandler(w, nil))
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
