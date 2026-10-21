package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/ingest/sequencer"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
	"github.com/lotannauo/toposhift/internal/store/pebblestore"
)

const day = 24 * time.Hour

// R14. The options every command opens its store with keep L0 and set the
// offsets: storeOptions, serve's, and the replay's and query's open. The
// retention check's offset is the one of L2 in the options serve opens with.
func TestR14EveryCommandKeepsL0(t *testing.T) {
	t.Parallel()
	o := storeOptions()
	if o.Keep != [4]bool{true, false, false, false} || o.Offsets != [4]time.Duration{0, 60 * day, 0, 0} {
		t.Fatalf("storeOptions: Keep %v Offsets %v, want L0 kept and L1 60 days behind", o.Keep, o.Offsets)
	}
	so := serveStoreOptions("prod", true, nil)
	if so.Keep != o.Keep || so.Offsets != o.Offsets {
		t.Fatalf("serveStoreOptions: Keep %v Offsets %v, want storeOptions'", so.Keep, so.Offsets)
	}
	st, err := pebblestore.Open(t.TempDir(), so)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if rc := serveRetainConfig(st, so, newRetainLog(slog.New(slog.DiscardHandler), so)); rc.Offset != so.Offsets[catalog.L2-catalog.L0] ||
		rc.Window != 30*day || rc.Every != retainEvery {
		t.Errorf("the retention check: %+v, want L2's offset, a window of 30 days, the fixed interval", rc)
	}

	// The open of replay (which writes) and of query (which reads) is openPebble.
	dir := t.TempDir()
	rw, err := openPebble(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	h := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	if err := rw.Retain(context.Background(), h); err != nil {
		t.Fatal(err)
	}
	wantHorizons(t, "the replay's store", rw, h)
	if err := rw.Close(); err != nil {
		t.Fatal(err)
	}
	ro, err := openPebble(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ro.Close() }()
	wantHorizons(t, "the query's store", ro, h)
}

// wantHorizons checks the horizons of a store retained to h with the default
// layers: L0 never moves, L1 is h less 60 days, L2 and L3 are h.
func wantHorizons(t *testing.T, what string, s interface {
	LayerHorizon(catalog.Layer) store.Horizon
}, h time.Time,
) {
	t.Helper()
	want := [4]time.Time{{}, h.Add(-60 * day), h, h}
	for l := catalog.L0; l <= catalog.L3; l++ {
		if got := s.LayerHorizon(l).Time; !got.Equal(want[l-catalog.L0]) {
			t.Errorf("%s: %s horizon %s, want %s", what, l, got, want[l-catalog.L0])
		}
	}
}

// R13. A retention moves each layer to h less its offset, and never L0, in
// memstore opened with the default layers and in the pebblestore serve opens.
func TestR13PerLayerHorizons(t *testing.T) {
	t.Parallel()
	o := storeOptions()
	mem, err := memstore.Open(memstore.Options{Policy: o.Policy, Keep: o.Keep, Offsets: o.Offsets})
	if err != nil {
		t.Fatal(err)
	}
	st, err := pebblestore.Open(t.TempDir(), serveStoreOptions("", false, nil))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	h := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	for _, s := range []store.Store{mem, st} {
		for _, at := range []time.Time{h, h.Add(-day), h} { // never backward
			if err := s.Retain(context.Background(), at); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := st.Instrument().WaitRetained(context.Background()); err != nil {
		t.Fatal(err)
	}
	wantHorizons(t, "memstore", mem, h)
	wantHorizons(t, "pebblestore", st, h)
}

// R15. --retain on or off, and its environment twin; a bad value of either is
// a mistake in how serve was called (exit 2, before anything listens), and the
// flag wins over the variable.
func TestR15RetainFlagAndVariable(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name string
		env  map[string]string
		args []string
	}{
		{"a bad flag", nil, []string{"--retain", "yes"}},
		{"a bad variable", map[string]string{"TOPOSHIFT_RETAIN": "Off"}, nil},
	} {
		var stderr bytes.Buffer
		code := run(t.Context(), append([]string{"serve", "--listen", "127.0.0.1:0"}, tt.args...), testEnv(t, tt.env), io.Discard, &stderr)
		if code != 2 || !strings.Contains(stderr.String(), "--retain") || strings.Contains(stderr.String(), "listening") {
			t.Errorf("%s: exit %d, stderr %q", tt.name, code, &stderr)
		}
	}
	for _, tt := range []struct {
		name  string
		env   map[string]string
		args  []string
		mode  string
		setBy string
	}{
		{"the default", nil, nil, defaultRetain, "default"},
		{"the flag off", nil, []string{"--retain", "off"}, "off", "flag"},
		{"the variable off", map[string]string{"TOPOSHIFT_RETAIN": "off"}, nil, "off", "environment"},
		{"the variable on", map[string]string{"TOPOSHIFT_RETAIN": "on"}, nil, "on", "environment"},
		{"the flag over the variable", map[string]string{"TOPOSHIFT_RETAIN": "off"}, []string{"--retain=on"}, "on", "flag"},
		{"an empty variable", map[string]string{"TOPOSHIFT_RETAIN": ""}, nil, defaultRetain, "default"},
	} {
		s := startServe(t, testEnv(t, tt.env), tt.args...)
		st := getStatus(t, s.base)
		if st.Retention.Mode != tt.mode || st.Retention.SetBy != tt.setBy {
			t.Errorf("%s: mode %q set by %q, want %q set by %q", tt.name, st.Retention.Mode, st.Retention.SetBy, tt.mode, tt.setBy)
		}
		if off := strings.Contains(s.stderr.String(), "retention is off: no retention is asked for"); off != (tt.mode == "off") {
			t.Errorf("%s: the boot line is logged: %v, want %v", tt.name, off, tt.mode == "off")
		}
		stopOK(t, s)
	}
}

// The boot line that retention is on says where the mode came from, as the line
// that it is off does: the help says the boot log does.
func TestTheRetentionOnLineSaysWhereTheModeCameFrom(t *testing.T) {
	for _, tt := range []struct {
		name  string
		env   map[string]string
		args  []string
		setBy string
	}{
		{"the flag", nil, []string{"--retain", "on"}, "flag"},
		{"the variable", map[string]string{"TOPOSHIFT_RETAIN": "on"}, nil, "environment"},
		{"the default", nil, nil, "default"},
	} {
		setSeam(t, &defaultRetain, "on")
		env := map[string]string{"TOPOSHIFT_DATA_DIR": t.TempDir()}
		maps.Copy(env, tt.env)
		s := startServe(t, testEnv(t, env), tt.args...)
		waitIngestOn(t, s.base)
		var line string
		for deadline := time.Now().Add(10 * time.Second); line == "" && time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
			for l := range strings.SplitSeq(s.stderr.String(), "\n") {
				if strings.Contains(l, "retention is on") {
					line = l
				}
			}
		}
		if !strings.Contains(line, "set_by="+tt.setBy) {
			t.Errorf("%s: the retention is on line lacks set_by=%s:\n%s", tt.name, tt.setBy, s.stderr)
		}
		stopOK(t, s)
	}
}

// statusBody is GET /status.
type statusBody struct {
	Retention struct {
		Mode          string                `json:"mode"`
		SetBy         string                `json:"set_by"`
		CheckedAt     *time.Time            `json:"checked_at"`
		Horizons      map[string]*time.Time `json:"horizons"`
		Agreement     string                `json:"agreement"`
		PendingSince  *time.Time            `json:"pending_since"`
		LastSettleEnd *time.Time            `json:"last_settle_end"`
		StoppedBy     string                `json:"stopped_by"`
		Counters      map[string]uint64     `json:"counters"`
	} `json:"retention"`
}

func getBody(t testing.TB, url string) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func getStatus(t testing.TB, base string) statusBody {
	t.Helper()
	code, body := getBody(t, base+"/status")
	var st statusBody
	if err := json.Unmarshal([]byte(body), &st); code != http.StatusOK || err != nil {
		t.Fatalf("GET /status = %d %q (%v)", code, body, err)
	}
	return st
}

// gid is the id of the calling goroutine.
func gid() uint64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	id, err := strconv.ParseUint(string(bytes.Fields(buf[:n])[1]), 10, 64)
	if err != nil {
		panic(err)
	}
	return id
}

// callers wraps the sequencer's writer and keeps the goroutines that call it.
type callers struct {
	sequencer.Writer
	mu   sync.Mutex
	seen map[uint64]int
}

func (c *callers) note() {
	c.mu.Lock()
	c.seen[gid()]++
	c.mu.Unlock()
}

func (c *callers) Write(ctx context.Context, b []store.Record) error {
	c.note()
	return c.Writer.Write(ctx, b)
}

func (c *callers) LastSeq() uint64 { c.note(); return c.Writer.LastSeq() }

func (c *callers) LayerHorizon(l catalog.Layer) store.Horizon {
	c.note()
	return c.Writer.LayerHorizon(l)
}

// R17. serve retains once the producers' clocks agree, and GET /status answers
// the snapshot the sequencer published: the mode, each layer's horizon (L0 kept,
// L1 60 days behind L2), the agreement, nothing pending, the end of the last
// settle, nothing stopped, and the counters. The handler calls nothing of the
// store: every call of the store's writer comes from one goroutine, the loop.
// GET /healthz is unchanged.
func TestR17StatusAfterARetention(t *testing.T) {
	setSeam(t, &retainEvery, 20*time.Millisecond)
	var c *callers
	setSeam(t, &writerFor, func(st *pebblestore.Store) sequencer.Writer {
		c = &callers{Writer: st, seen: map[uint64]int{}}
		return c
	})
	dir := t.TempDir()
	s := startServe(t, dirEnv(t, dir), "--retain", "on")
	waitIngestOn(t, s.base)
	if st := getStatus(t, s.base); st.Retention.Mode != "on" || st.Retention.Horizons["L2"] != nil {
		t.Fatalf("before anything agrees: %+v", st.Retention)
	}
	now := time.Now()
	postOK(t, s.base, podBody("pod-1", now))
	var st statusBody
	deadline := time.Now().Add(15 * time.Second)
	for {
		st = getStatus(t, s.base)
		r := st.Retention
		if r.Counters["serve.retain_calls"] == 1 && r.PendingSince == nil && r.LastSettleEnd != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no retention seen: %+v\n%s", r, s.stderr)
		}
		time.Sleep(10 * time.Millisecond)
	}
	r := st.Retention
	l2 := r.Horizons["L2"]
	if l2 == nil || r.Horizons["L0"] != nil || r.Horizons["L1"] == nil || r.Horizons["L3"] == nil ||
		!r.Horizons["L1"].Equal(l2.Add(-60*day)) || !r.Horizons["L3"].Equal(*l2) {
		t.Fatalf("horizons %v", r.Horizons)
	}
	if want := now.Add(-30 * day).UTC().Truncate(time.Hour); !l2.Equal(want) && !l2.Equal(want.Add(time.Hour)) {
		t.Errorf("L2 retained to %s, want %s", l2, want)
	}
	if r.Agreement != "agree" || r.StoppedBy != "" || r.CheckedAt == nil {
		t.Errorf("status %+v", r)
	}
	for _, k := range []string{"serve.retain_skipped", "serve.retain_capped", "serve.retain_catchups"} {
		if v, ok := r.Counters[k]; !ok || v != 0 {
			t.Errorf("counter %s = %d (present %v), want 0", k, v, ok)
		}
	}
	for range 20 {
		getStatus(t, s.base)
	}
	if code, body := getBody(t, s.base+"/healthz"); code != http.StatusOK || body != "ok\n" {
		t.Errorf("GET /healthz = %d %q", code, body)
	}
	for _, line := range []string{"retained for the first time", "retention published", "rewrite ended", "settle ended", "retention is on"} {
		if !strings.Contains(s.stderr.String(), line) {
			t.Errorf("the log does not say %q:\n%s", line, s.stderr)
		}
	}
	stopOK(t, s)
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.seen) != 1 {
		t.Errorf("the store's writer was called from %d goroutines (%v), want the loop's alone", len(c.seen), c.seen)
	}
}

// R12. With --retain off nothing is retained, however much agrees, and the
// status says so.
func TestServeRetainOffRetainsNothing(t *testing.T) {
	setSeam(t, &retainEvery, 10*time.Millisecond)
	dir := t.TempDir()
	s := startServe(t, dirEnv(t, dir), "--retain", "off")
	waitIngestOn(t, s.base)
	postOK(t, s.base, podBody("pod-1", time.Now()))
	time.Sleep(200 * time.Millisecond)
	code, body := getBody(t, s.base+"/status")
	var st map[string]json.RawMessage
	if err := json.Unmarshal([]byte(body), &st); code != http.StatusOK || err != nil || string(st["retention"]) != `{"mode":"off","set_by":"flag"}` {
		t.Errorf("GET /status = %d %q", code, body)
	}
	stopOK(t, s)
	if h := openStopped(t, dir).LayerHorizon(catalog.L2); !h.IsZero() {
		t.Errorf("L2 horizon %v after serve --retain off", h)
	}
}

// The default this build ships with. Only a release commit changes it, together
// with want here, so a change of the default on any other commit fails.
func TestTheRetainDefault(t *testing.T) {
	t.Parallel()
	const want = "on"
	if defaultRetain != want {
		t.Fatalf("defaultRetain = %q, want %q", defaultRetain, want)
	}
}

// A build whose default is off (a release's): serve keeps everything, warns at boot
// that this is the default and how to turn retention on when ingest is on (there is
// a store to grow), GET /status says so, and serve -h names the default. With ingest
// off it only says that retention is off. Either way of asking for retention turns
// it on, and either way of asking for none turns it off, without the warning; an
// empty variable is no ask. Each line of the boot log says where the mode came from.
func TestARetainDefaultOfOff(t *testing.T) {
	setSeam(t, &defaultRetain, "off")
	s := startServe(t, dirEnv(t, t.TempDir()))
	waitIngestOn(t, s.base)
	if st := getStatus(t, s.base); st.Retention.Mode != "off" || st.Retention.SetBy != "default" {
		t.Errorf("status %+v, want off by default", st.Retention)
	}
	// One line says it all, and it is a warning (the log has other warnings: a
	// data directory in the temp directory gets one).
	var warning string
	for line := range strings.SplitSeq(s.stderr.String(), "\n") {
		if strings.Contains(line, "off by default") {
			warning = line
		}
	}
	for _, want := range []string{"level=WARN", "set_by=default", "--retain on", "TOPOSHIFT_RETAIN=on"} {
		if !strings.Contains(warning, want) {
			t.Errorf("the boot warning does not say %q:\n%s", want, s.stderr)
		}
	}
	stopOK(t, s)

	// No data directory: ingest is off, nothing is stored, so there is no warning
	// that the store grows; the line that retention is off is still logged.
	s = startServe(t, testEnv(t, nil))
	if st := getStatus(t, s.base); st.Retention.Mode != "off" || st.Retention.SetBy != "default" {
		t.Errorf("ingest off: status %+v, want off by default", st.Retention)
	}
	if log := s.stderr.String(); strings.Contains(log, "off by default") || !strings.Contains(log, "retention is off: no retention is asked for") {
		t.Errorf("ingest off: the boot log warns of the default, or does not say retention is off:\n%s", log)
	}
	stopOK(t, s)

	// With a data directory, each way of saying what retention is to be: only the
	// default, however it comes (unset, or an empty variable), is warned of.
	for _, tt := range []struct {
		name  string
		env   map[string]string
		args  []string
		mode  string
		setBy string
	}{
		{"the flag on", nil, []string{"--retain", "on"}, "on", "flag"},
		{"the variable on", map[string]string{"TOPOSHIFT_RETAIN": "on"}, nil, "on", "environment"},
		{"the flag off", nil, []string{"--retain", "off"}, "off", "flag"},
		{"the variable off", map[string]string{"TOPOSHIFT_RETAIN": "off"}, nil, "off", "environment"},
		{"an empty variable", map[string]string{"TOPOSHIFT_RETAIN": ""}, nil, "off", "default"},
	} {
		env := map[string]string{"TOPOSHIFT_DATA_DIR": t.TempDir()}
		maps.Copy(env, tt.env)
		s := startServe(t, testEnv(t, env), tt.args...)
		waitIngestOn(t, s.base)
		if st := getStatus(t, s.base); st.Retention.Mode != tt.mode || st.Retention.SetBy != tt.setBy {
			t.Errorf("%s: mode %q set by %q, want %q set by %q", tt.name, st.Retention.Mode, st.Retention.SetBy, tt.mode, tt.setBy)
		}
		if warned := strings.Contains(s.stderr.String(), "off by default"); warned != (tt.setBy == "default") {
			t.Errorf("%s: warns of the default: %v, want %v", tt.name, warned, tt.setBy == "default")
		}
		stopOK(t, s)
	}
	var stderr bytes.Buffer
	if code := run(t.Context(), []string{"serve", "-h"}, noEnv, io.Discard, &stderr); code != 0 {
		t.Fatalf("exit code = %d", code)
	}
	if !strings.Contains(stderr.String(), "The default of this build is --retain off") {
		t.Errorf("serve -h does not name the default off:\n%s", &stderr)
	}
}

// serve -h documents retention.
func TestServeHelpDocumentsRetention(t *testing.T) {
	t.Parallel()
	var stderr bytes.Buffer
	if code := run(t.Context(), []string{"serve", "-h"}, noEnv, io.Discard, &stderr); code != 0 {
		t.Fatalf("exit code = %d", code)
	}
	for _, want := range []string{
		"-retain", "TOPOSHIFT_RETAIN", "--retain off asks for no retention", "L2 up to 31 days, L1 up to 91",
		"growth check", "restarted", "GET /status", "agree", "loses its records older",
		"The default of this build is --retain " + defaultRetain, "set_by",
	} {
		if !strings.Contains(stderr.String(), want) {
			t.Errorf("serve -h does not mention %q", want)
		}
	}
}

// retainedRE finds the target of serve's first retention in its log.
var retainedRE = regexp.MustCompile(`retained for the first time[^\n]* to=(\S+)`)

// P6. serve killed with SIGKILL right after a retention returned, with its pass
// held in the middle: the horizons are those published, and the next open
// resumes the pass. serve runs in a child process, the test binary run again
// (see retain_child_test.go), so that its check can run every 50 ms and its
// pass be held; the crash is a real one.
func TestP6KilledAfterARetainResumes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping the child process test in short mode")
	}
	dir := t.TempDir()
	cmd := exec.CommandContext(t.Context(), os.Args[0], childArg, dir)
	cmd.Env = []string{}
	cmd.Dir = t.TempDir()
	stderr := &lockedBuffer{}
	cmd.Stderr = stderr
	cmd.Stdout = stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	t.Cleanup(func() { _ = cmd.Process.Kill() })

	var base string
	deadline := time.Now().Add(15 * time.Second)
	for base == "" {
		if m := listeningRE.FindStringSubmatch(stderr.String()); m != nil {
			base = "http://" + m[1]
			break
		}
		select {
		case err := <-exited:
			t.Fatalf("serve exited: %v\n%s", err, stderr)
		case <-time.After(10 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatalf("serve did not listen:\n%s", stderr)
		}
	}
	waitIngestOn(t, base)
	postOK(t, base, podBody("pod-1", time.Now()))
	var to time.Time
	for to.IsZero() {
		if m := retainedRE.FindStringSubmatch(stderr.String()); m != nil {
			var err error
			if to, err = time.Parse(time.RFC3339Nano, m[1]); err != nil {
				t.Fatalf("the target %q: %v", m[1], err)
			}
			break
		}
		if time.Now().After(deadline.Add(15 * time.Second)) {
			t.Fatalf("serve did not retain:\n%s", stderr)
		}
		time.Sleep(10 * time.Millisecond)
	}
	for !strings.Contains(stderr.String(), childHeld) {
		if time.Now().After(deadline.Add(30 * time.Second)) {
			t.Fatalf("the pass was not held:\n%s", stderr)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	if err := <-exited; err == nil {
		t.Fatal("serve exited cleanly after SIGKILL")
	}

	rec := &resumeCounter{}
	o := serveStoreOptions("", false, nil)
	o.Recorder = rec
	st, err := pebblestore.Open(filepath.Join(dir, "store"), o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	wantHorizons(t, "after the crash", st, to)
	if err := st.Instrument().WaitRetained(t.Context()); err != nil {
		t.Fatal(err)
	}
	if n := rec.resumed(); n != 1 {
		t.Errorf("retain.resumed = %d, want the pass resumed once", n)
	}
	wantHorizons(t, "after the resumed pass", st, to)
	id, err := identity.NewResolver(catalog.Default()).Resolve(catalog.K8sPod, []identity.Attr{{Key: catalog.K8sPodUID, Value: "pod-1"}})
	if err != nil {
		t.Fatal(err)
	}
	recs, err := st.EntityWindow(t.Context(), id.Fingerprint(), to, store.MaxEventTime, store.Current(catalog.L2))
	if err != nil || len(recs) == 0 {
		t.Errorf("the record answered 200 is not in the store: %v %v", recs, err)
	}
}

// resumeCounter counts the passes a store resumes at open.
type resumeCounter struct {
	mu sync.Mutex
	n  int64
}

func (r *resumeCounter) Count(name string, n int64) {
	if name == "retain.resumed" {
		r.mu.Lock()
		r.n += n
		r.mu.Unlock()
	}
}

func (r *resumeCounter) Sample(string, int64) {}

func (r *resumeCounter) resumed() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.n
}
