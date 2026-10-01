package server_test

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
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

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
