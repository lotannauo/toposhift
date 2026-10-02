package main

import (
	"context"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/server"
)

// TestBootsWithNoConfig is the zero-config contract: the built
// binary, started with no flags, no environment and no config file, serves on
// the default loopback address and stops cleanly on SIGINT.
func TestBootsWithNoConfig(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping binary boot test in short mode")
	}
	if ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", server.DefaultListen); err != nil {
		t.Skipf("default address %s is in use; run this test on a clean machine or CI: %v", server.DefaultListen, err)
	} else {
		_ = ln.Close()
	}

	bin := filepath.Join(t.TempDir(), "toposhift")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", bin, ".")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}

	cmd := exec.CommandContext(t.Context(), bin, "serve")
	cmd.Env = []string{} // no configuration of any kind
	cmd.Dir = t.TempDir()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
	})

	if err := waitHealthy(t.Context(), "http://"+server.DefaultListen+"/healthz", exited); err != nil {
		t.Fatal(err)
	}

	if err := cmd.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-exited:
		if err != nil {
			t.Fatalf("toposhift serve exited uncleanly after SIGINT: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("toposhift serve did not exit within 10s of SIGINT")
	}
}

func waitHealthy(ctx context.Context, url string, exited <-chan error) error {
	deadline := time.Now().Add(15 * time.Second)
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return err
		}
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		select {
		case err := <-exited:
			return &earlyExitError{err}
		case <-time.After(100 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			return context.DeadlineExceeded
		}
	}
}

type earlyExitError struct{ err error }

func (e *earlyExitError) Error() string {
	return "toposhift serve exited before becoming healthy: " + e.err.Error()
}

func (e *earlyExitError) Unwrap() error { return e.err }
