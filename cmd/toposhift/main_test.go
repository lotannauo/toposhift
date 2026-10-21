package main

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func noEnv(string) string { return "" }

func TestRunExitCodes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		args     []string
		wantCode int
		wantOut  string // substring of stdout
		wantErr  string // substring of stderr
	}{
		{"no args prints usage", nil, 2, "", "Usage: toposhift"},
		{"unknown command", []string{"frobnicate"}, 2, "", `unknown command "frobnicate"`},
		{"help", []string{"help"}, 0, "Usage: toposhift", ""},
		{"version", []string{"version"}, 0, "toposhift ", ""},
		{"query needs an operation", []string{"query"}, 2, "", "give exactly one operation"},
		{"replay needs a source", []string{"replay"}, 2, "", "say what to replay"},
		{"serve rejects stray arguments", []string{"serve", "extra"}, 2, "", `unexpected argument "extra"`},
		{"serve rejects unknown flags", []string{"serve", "--nope"}, 2, "", "flag provided but not defined"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := run(t.Context(), tt.args, noEnv, &stdout, &stderr)
			if code != tt.wantCode {
				t.Errorf("exit code = %d, want %d", code, tt.wantCode)
			}
			if !strings.Contains(stdout.String(), tt.wantOut) {
				t.Errorf("stdout = %q, want substring %q", stdout.String(), tt.wantOut)
			}
			if !strings.Contains(stderr.String(), tt.wantErr) {
				t.Errorf("stderr = %q, want substring %q", stderr.String(), tt.wantErr)
			}
		})
	}
}

func TestServeListenPrecedence(t *testing.T) {
	t.Parallel()
	// An unusable address makes serve fail fast, and the error names the
	// address that won, which shows which layer took precedence.
	tests := []struct {
		name    string
		env     string
		args    []string
		wantErr string
	}{
		{"env overrides default", "127.0.0.1:bad-env", nil, "bad-env"},
		{"flag overrides env", "127.0.0.1:bad-env", []string{"--listen", "127.0.0.1:bad-flag"}, "bad-flag"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			getenv := func(k string) string {
				if k == envListen {
					return tt.env
				}
				return ""
			}
			var stderr bytes.Buffer
			code := run(context.Background(), append([]string{"serve"}, tt.args...), getenv, &bytes.Buffer{}, &stderr)
			if code != 1 {
				t.Errorf("exit code = %d, want 1", code)
			}
			if !strings.Contains(stderr.String(), tt.wantErr) {
				t.Errorf("stderr = %q, want substring %q", stderr.String(), tt.wantErr)
			}
		})
	}
}

// A server that stops on a signal is timed in the log even when the goroutine
// that notes the signal runs late: here the callback is held for 50 ms.
func TestWatchStopLogsWhenTheSignalIsNotedLate(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	entered, release := make(chan struct{}), make(chan struct{})
	logStop := watchStop(ctx, func() {
		close(entered)
		<-release
	})
	cancel()
	<-entered
	// The callback is held for 50 ms after the context is done, so logStop is
	// almost always called while it is still held; either way the line must
	// be logged.
	time.AfterFunc(50*time.Millisecond, func() { close(release) })
	var out bytes.Buffer
	logStop(slog.New(slog.NewTextHandler(&out, nil)))
	if !strings.Contains(out.String(), "server stopped") {
		t.Errorf("the log does not time the stop:\n%s", &out)
	}
}

// A server that returns while ctx is live, as one that failed at startup does,
// logs nothing and does not wait for a signal that never comes.
func TestWatchStopDoesNotWaitWhenNothingSignalled(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	logStop := watchStop(ctx, nil)
	var out bytes.Buffer
	done := make(chan struct{})
	go func() {
		logStop(slog.New(slog.NewTextHandler(&out, nil)))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("logStop waits for a signal that never comes")
	}
	if out.Len() != 0 {
		t.Errorf("the log has %q, want nothing", &out)
	}
}
