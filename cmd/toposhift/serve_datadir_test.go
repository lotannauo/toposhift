package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/datadir"
)

const (
	sentenceNoDirectory = "ingest is off: no data directory; set --data-dir or TOPOSHIFT_DATA_DIR"
	sentenceNotYet      = "ingest is not available in this build yet"
)

// lockedBuffer is a stderr that the test reads while serve writes it.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

var listeningRE = regexp.MustCompile(`listening addr=(\S+)`)

// served is a serve running in this process on a port the system picks.
type served struct {
	base   string // http://host:port
	stderr *lockedBuffer
	cancel context.CancelFunc
	done   chan int
}

// startServe runs `serve --listen 127.0.0.1:0 args...` and waits until it
// listens. It fails the test if serve exits first.
func startServe(t *testing.T, getenv func(string) string, args ...string) *served {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	s := &served{stderr: &lockedBuffer{}, cancel: cancel, done: make(chan int, 1)}
	go func() {
		s.done <- run(ctx, append([]string{"serve", "--listen", "127.0.0.1:0"}, args...), getenv, io.Discard, s.stderr)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case code := <-s.done:
			s.done <- code
		case <-time.After(10 * time.Second):
			t.Error("serve did not stop")
		}
	})

	deadline := time.After(15 * time.Second)
	for {
		if m := listeningRE.FindStringSubmatch(s.stderr.String()); m != nil {
			s.base = "http://" + m[1]
			return s
		}
		select {
		case code := <-s.done:
			s.done <- code // for the cleanup
			t.Fatalf("serve exited with %d before listening:\n%s", code, s.stderr)
		case <-deadline:
			t.Fatalf("serve did not listen:\n%s", s.stderr)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// stop shuts serve down and returns its exit code.
func (s *served) stop(t *testing.T) int {
	t.Helper()
	s.cancel()
	select {
	case code := <-s.done:
		s.done <- code
		return code
	case <-time.After(10 * time.Second):
		t.Fatal("serve did not stop")
		return -1
	}
}

// client opens a connection for each request and closes it after the answer.
// A pooled client can dial a spare connection while a request is served on an
// idle one, and the server's shutdown waits five seconds for a connection that
// has never sent a request.
var client = &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}

// postLogs sends an empty OTLP/JSON logs request to the receiver at base.
func postLogs(t testing.TB, base string) (status int, retryAfter, body string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, base+"/v1/logs", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, resp.Header.Get("Retry-After"), string(b)
}

func get(t testing.TB, url string) int {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	return resp.StatusCode
}

// requireIngestOff checks the answer of the receiver when ingest is off for
// want: 503, to be retried later, saying why.
func requireIngestOff(t testing.TB, base, want string) {
	t.Helper()
	status, retryAfter, body := postLogs(t, base)
	if status != http.StatusServiceUnavailable {
		t.Errorf("POST /v1/logs = %d, want 503 (body %q)", status, body)
	}
	if retryAfter == "" {
		t.Error("POST /v1/logs answered 503 without Retry-After")
	}
	if !strings.Contains(body, want) {
		t.Errorf("POST /v1/logs body = %q, want it to contain %q", body, want)
	}
}

func TestServeWithoutADataDirectoryKeepsIngestOff(t *testing.T) {
	tests := []struct {
		name      string
		env       map[string]string
		wantNotes []string
	}{
		{name: "nothing set"},
		{
			name:      "several state directories",
			env:       map[string]string{"STATE_DIRECTORY": "/a:/b"},
			wantNotes: []string{"--data-dir"},
		},
		{
			name:      "XDG_DATA_HOME alone",
			env:       map[string]string{"XDG_DATA_HOME": "/x"},
			wantNotes: []string{"XDG_DATA_HOME", "TOPOSHIFT_DATA_DIR"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Nothing may be made in the temp directory: it is not a fallback.
			tmp := t.TempDir()
			t.Setenv("TMPDIR", tmp)

			s := startServe(t, testEnv(t, tt.env))
			if got := get(t, s.base+"/healthz"); got != http.StatusOK {
				t.Errorf("GET /healthz = %d, want 200", got)
			}
			requireIngestOff(t, s.base, sentenceNoDirectory)
			if n := strings.Count(s.stderr.String(), sentenceNoDirectory); n != 1 {
				t.Errorf("boot printed the no-directory sentence %d times, want once:\n%s", n, s.stderr)
			}
			for _, note := range tt.wantNotes {
				if !strings.Contains(s.stderr.String(), note) {
					t.Errorf("stderr does not contain %q:\n%s", note, s.stderr)
				}
			}
			if code := s.stop(t); code != 0 {
				t.Errorf("exit code = %d, want 0", code)
			}
			if entries, err := os.ReadDir(tmp); err != nil || len(entries) != 0 {
				t.Errorf("the temp directory has %v (%v), want nothing made there", entries, err)
			}
		})
	}
}

func TestServeWithADataDirectoryLaysItOut(t *testing.T) {
	dir := t.TempDir()
	s := startServe(t, testEnv(t, map[string]string{"TOPOSHIFT_DATA_DIR": dir}))

	log := s.stderr.String()
	if !strings.Contains(log, "path="+dir) {
		t.Errorf("boot does not print the path %s:\n%s", dir, log)
	}
	if !strings.Contains(log, "data may be lost at reboot") {
		t.Errorf("boot does not warn about the temp directory:\n%s", log)
	}
	for _, part := range []string{"store", "bus", "meta"} {
		if info, err := os.Stat(filepath.Join(dir, part)); err != nil || !info.IsDir() {
			t.Errorf("%s was not created: %v", part, err)
		}
	}
	if b, err := os.ReadFile(filepath.Join(dir, "meta", "format")); err != nil || string(b) != "toposhift data directory, layout 1\n" {
		t.Errorf("meta/format = %q, %v", b, err)
	}
	if got := get(t, s.base+"/healthz"); got != http.StatusOK {
		t.Errorf("GET /healthz = %d, want 200", got)
	}
	requireIngestOff(t, s.base, sentenceNotYet)

	// The directory belongs to this instance while it runs, and is released
	// when it stops.
	if _, err := datadir.Open(datadir.Result{Dir: dir}); err == nil {
		t.Error("the data directory of a running serve could be opened")
	}
	if code := s.stop(t); code != 0 {
		t.Errorf("exit code = %d, want 0", code)
	}
	d, err := datadir.Open(datadir.Result{Dir: dir})
	if err != nil {
		t.Fatalf("the data directory was not released at shutdown: %v", err)
	}
	_ = d.Close()
}

func TestServeDataDirFlagBeatsTheVariable(t *testing.T) {
	flagDir, envDir := t.TempDir(), t.TempDir()
	s := startServe(t, testEnv(t, map[string]string{"TOPOSHIFT_DATA_DIR": envDir}), "--data-dir", flagDir)
	if _, err := os.Stat(filepath.Join(flagDir, "meta", "format")); err != nil {
		t.Errorf("the flag's directory was not laid out: %v", err)
	}
	if entries, _ := os.ReadDir(envDir); len(entries) != 0 {
		t.Errorf("the variable's directory was written to: %v", entries)
	}
	if !strings.Contains(s.stderr.String(), "source=flag") {
		t.Errorf("boot does not say the directory came from the flag:\n%s", s.stderr)
	}
}

func TestServePrintsASymlinkAndItsTarget(t *testing.T) {
	root := t.TempDir()
	real := filepath.Join(root, "real")
	link := filepath.Join(root, "link")
	if err := os.Mkdir(real, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(real, link); err != nil {
		t.Fatal(err)
	}
	target, err := filepath.EvalSymlinks(real)
	if err != nil {
		t.Fatal(err)
	}
	s := startServe(t, testEnv(t, map[string]string{"TOPOSHIFT_DATA_DIR": link}))
	log := s.stderr.String()
	for _, want := range []string{"path=" + link, "resolves_to=" + target} {
		if !strings.Contains(log, want) {
			t.Errorf("boot output does not contain %q:\n%s", want, log)
		}
	}
	if _, err := os.Stat(filepath.Join(real, "meta", "format")); err != nil {
		t.Errorf("the target was not laid out: %v", err)
	}
}

func TestServeRefusesWhatItCannotUse(t *testing.T) {
	held := t.TempDir()
	d, err := datadir.Open(datadir.Result{Dir: held})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })

	foreign := t.TempDir()
	if err := os.WriteFile(filepath.Join(foreign, "mine.txt"), nil, 0o600); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name     string
		env      map[string]string
		wantCode int
		wantErr  string
	}{
		{"held by another instance", map[string]string{"TOPOSHIFT_DATA_DIR": held}, 1, fmt.Sprintf("is in use by pid %d", os.Getpid())},
		{"a directory that is not ours", map[string]string{"TOPOSHIFT_DATA_DIR": foreign}, 1, "is not empty and is not a toposhift data directory"},
		{"a leading tilde", map[string]string{"TOPOSHIFT_DATA_DIR": "~/data"}, 2, `begins with "~"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stderr bytes.Buffer
			code := run(t.Context(), []string{"serve", "--listen", "127.0.0.1:0"}, testEnv(t, tt.env), io.Discard, &stderr)
			if code != tt.wantCode {
				t.Errorf("exit code = %d, want %d:\n%s", code, tt.wantCode, &stderr)
			}
			if !strings.Contains(stderr.String(), tt.wantErr) {
				t.Errorf("stderr = %q, want it to contain %q", stderr.String(), tt.wantErr)
			}
			if strings.Contains(stderr.String(), "listening") {
				t.Errorf("serve listened despite the error:\n%s", &stderr)
			}
		})
	}
}

func TestServeBootsWhenTheDefaultDirectoryCannotBeUsed(t *testing.T) {
	tests := []struct {
		name  string
		setup func(t *testing.T, home string)
	}{
		{"a home nobody can write to", func(t *testing.T, home string) {
			if os.Getuid() == 0 {
				t.Skip("root can write to a read-only home")
			}
			if err := os.Chmod(home, 0o500); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.Chmod(home, 0o700) }) // so the temp directory can be removed
		}},
		{"a home that is not a directory", func(t *testing.T, home string) {
			if err := os.WriteFile(filepath.Join(home, ".local"), nil, 0o600); err != nil {
				t.Fatal(err)
			}
		}},
		{"a default directory with somebody's files in it", func(t *testing.T, home string) {
			dir := filepath.Join(home, ".local", "share", "toposhift")
			if err := os.MkdirAll(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "notes.txt"), nil, 0o600); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			home := t.TempDir()
			tt.setup(t, home)
			want := "ingest is off: the data directory " + filepath.Join(home, ".local", "share", "toposhift") +
				" could not be opened; set --data-dir or TOPOSHIFT_DATA_DIR"

			s := startServe(t, testEnvHome(t, home))
			if got := get(t, s.base+"/healthz"); got != http.StatusOK {
				t.Errorf("GET /healthz = %d, want 200", got)
			}
			requireIngestOff(t, s.base, want)
			if !strings.Contains(s.stderr.String(), "the data directory could not be opened") {
				t.Errorf("boot does not say why ingest is off:\n%s", s.stderr)
			}
			if code := s.stop(t); code != 0 {
				t.Errorf("exit code = %d, want 0", code)
			}
		})
	}
}

func TestServeReleasesTheDataDirectoryWhenItCannotListen(t *testing.T) {
	dir := t.TempDir()
	var stderr bytes.Buffer
	code := run(t.Context(), []string{"serve", "--listen", "127.0.0.1:bad"}, testEnv(t, map[string]string{"TOPOSHIFT_DATA_DIR": dir}), io.Discard, &stderr)
	if code != 1 {
		t.Fatalf("exit code = %d, want 1:\n%s", code, &stderr)
	}
	d, err := datadir.Open(datadir.Result{Dir: dir})
	if err != nil {
		t.Fatalf("the directory is still held after serve failed: %v", err)
	}
	_ = d.Close()
}

func TestServeHelpDocumentsTheDataDirectory(t *testing.T) {
	var stderr bytes.Buffer
	if code := run(t.Context(), []string{"serve", "-h"}, noEnv, io.Discard, &stderr); code != 0 {
		t.Fatalf("exit code = %d, want 0", code)
	}
	for _, want := range []string{
		"-data-dir", "Data directory", "$TOPOSHIFT_DATA_DIR", "$STATE_DIRECTORY", "$HOME/.local/share/toposhift",
		"XDG_DATA_HOME", "/var/lib/toposhift", "fsGroup", "read-only root filesystem", "emptyDir", "Machine",
	} {
		if !strings.Contains(stderr.String(), want) {
			t.Errorf("serve -h does not mention %q:\n%s", want, &stderr)
		}
	}
}
