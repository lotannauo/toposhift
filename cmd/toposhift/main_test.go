package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func noEnv(string) string { return "" }

func TestRunExitCodes(t *testing.T) {
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
		{"query is a stub", []string{"query"}, 1, "", "not implemented yet"},
		{"replay is a stub", []string{"replay"}, 1, "", "not implemented yet"},
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
