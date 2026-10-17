package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/mcp/mcphttp"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/pebblestore"
)

// replayed is a store that replay filled, and what is known to be in it.
type replayed struct {
	dir  string
	last uint64
	pod  identity.Fingerprint // a pod with a scheduled_on edge
	at   time.Time            // an instant at which the pod has it
}

// replayFixture replays a generated stream into a new directory through the
// command line, as a replayed store is made.
func replayFixture(t *testing.T) replayed {
	t.Helper()
	_, recs := stream(t)
	base := t.TempDir()
	data := filepath.Join(base, "data")
	file := filepath.Join(base, "all.parquet")
	writeFile(t, file, recs)
	mustCLI(t, "replay", "activity", file, "--data-dir", data)
	r := replayed{dir: data, last: recs[len(recs)-1].Seq}
	for _, rec := range recs {
		if rec.Subject.Kind == store.SubjectEdge && rec.Subject.A.Type() == catalog.K8sPod && rec.Subject.Relation == catalog.ScheduledOn {
			r.pod, r.at = rec.Subject.A, rec.EventTime.Add(time.Minute)
			break
		}
	}
	if r.at.IsZero() {
		t.Fatal("the stream has no scheduled pod")
	}
	return r
}

// session is one run of the command in this process, with the two ends of its
// input and output in the test's hands.
type session struct {
	stdin   *io.PipeWriter // what the client writes
	stdoutR *io.PipeReader // what the client reads
	stdout  *lockedBuffer  // every byte the command wrote
	stderr  *lockedBuffer
	cancel  context.CancelFunc
	done    chan int
}

// runSession runs toposhift mcp with args. The command's input is a pipe the
// test writes, and its output is a pipe the test reads, and a copy of it is kept.
func runSession(t *testing.T, args ...string) *session {
	t.Helper()
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	ctx, cancel := context.WithCancel(t.Context())
	s := &session{
		stdin: inW, stdoutR: outR, stdout: &lockedBuffer{}, stderr: &lockedBuffer{},
		cancel: cancel, done: make(chan int, 1),
	}
	setSeam(t, &mcpStdin, io.Reader(inR))
	go func() {
		code := run(ctx, append([]string{"mcp"}, args...), noEnv, io.MultiWriter(outW, s.stdout), s.stderr)
		_ = outW.Close()
		s.done <- code
	}()
	t.Cleanup(func() {
		cancel()
		_ = inW.Close()
		_ = outR.Close()
		select {
		case <-s.done:
		case <-time.After(30 * time.Second):
			t.Error("the command did not end when the test did")
		}
	})
	return s
}

// connect speaks the protocol to the command with the SDK's client.
func (s *session) connect(t *testing.T) *sdk.ClientSession {
	t.Helper()
	cs, err := sdk.NewClient(&sdk.Implementation{Name: "test", Version: "0"}, nil).
		Connect(t.Context(), &sdk.IOTransport{Reader: s.stdoutR, Writer: s.stdin}, nil)
	if err != nil {
		t.Fatalf("connecting a client: %v\nstderr:\n%s", err, s.stderr)
	}
	t.Cleanup(func() { _ = cs.Close() })
	return cs
}

// exit waits for the command to end and returns its exit code.
func (s *session) exit(t testing.TB) int {
	t.Helper()
	select {
	case code := <-s.done:
		s.done <- code
		return code
	case <-time.After(30 * time.Second):
		t.Fatalf("the command did not end:\n%s", s.stderr)
		return -1
	}
}

// ended says whether the command has ended.
func (s *session) ended() bool {
	select {
	case code := <-s.done:
		s.done <- code
		return true
	default:
		return false
	}
}

func call(t *testing.T, cs *sdk.ClientSession, name string, args any) *sdk.CallToolResult {
	t.Helper()
	res, err := cs.CallTool(t.Context(), &sdk.CallToolParams{Name: name, Arguments: args})
	if err != nil {
		t.Fatalf("%s: protocol error: %v", name, err)
	}
	return res
}

func resultText(res *sdk.CallToolResult) string {
	if len(res.Content) == 0 {
		return ""
	}
	if c, ok := res.Content[0].(*sdk.TextContent); ok {
		return c.Text
	}
	return ""
}

// structured is the structured answer of a call that did not fail.
func structured(t *testing.T, name string, res *sdk.CallToolResult) map[string]any {
	t.Helper()
	if res.IsError {
		t.Fatalf("%s failed: %s", name, resultText(res))
	}
	b, err := json.Marshal(res.StructuredContent)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func rfc(tm time.Time) string { return tm.UTC().Format(time.RFC3339Nano) }

// onlyFrames fails the test unless every line of out is one JSON-RPC message, and
// returns how many there were.
func onlyFrames(t testing.TB, out string) int {
	t.Helper()
	if out != "" && !strings.HasSuffix(out, "\n") {
		t.Errorf("the output does not end with a newline: %q", out)
	}
	n := 0
	sc := bufio.NewScanner(strings.NewReader(out))
	sc.Buffer(nil, 4<<20)
	for sc.Scan() {
		var m map[string]any
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil || m["jsonrpc"] != "2.0" {
			t.Errorf("a line of stdout is not a JSON-RPC message: %.200q", sc.Text())
		}
		n++
	}
	return n
}

// The tools are served over the command's input and output from a store replay
// filled: every tool answers at one token, the range tools included, and when
// the client closes its input the command ends with status 0, the store closed
// and the directory as it was.
func TestMCPServesEveryToolFromAReplayedStore(t *testing.T) {
	r := replayFixture(t)
	before := snapshotDir(t, r.dir)
	s := runSession(t, "--data-dir", r.dir)
	cs := s.connect(t)
	token := strconv.FormatUint(r.last, 10)

	lt, err := cs.ListTools(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(lt.Tools) != 9 {
		t.Errorf("%d tools offered, want 9", len(lt.Tools))
	}

	schema := structured(t, "describe_schema", call(t, cs, "describe_schema", map[string]any{}))
	if schema["range_reads"] != true {
		t.Errorf("describe_schema says range_reads %v, want true", schema["range_reads"])
	}

	nb := structured(t, "neighbors_at", call(t, cs, "neighbors_at", map[string]any{"entity": r.pod.String(), "at": rfc(r.at)}))
	if nb["token"] != token {
		t.Errorf("neighbors_at: token %v, want the store's last seq %s", nb["token"], token)
	}
	if edges, _ := nb["edges"].([]any); len(edges) == 0 {
		t.Errorf("neighbors_at of a scheduled pod: no edges: %v", nb)
	}

	// A token above the store's last is answered at the last.
	ahead := structured(t, "neighbors_at", call(t, cs, "neighbors_at",
		map[string]any{"entity": r.pod.String(), "at": rfc(r.at), "asof": "99999999999"}))
	if ahead["token"] != token {
		t.Errorf("an asof above the store: token %v, want %s", ahead["token"], token)
	}

	// The replayed payloads are not JSON objects: the descriptions are withheld
	// whole, with a note, and the existence is read.
	hist := structured(t, "entity_history", call(t, cs, "entity_history",
		map[string]any{"entity": r.pod.String(), "from": rfc(r.at.Add(-24 * time.Hour)), "to": rfc(r.at.Add(24 * time.Hour))}))
	if hist["existence"] == nil {
		t.Errorf("entity_history: existence is null, want the existence intervals")
	}
	ex, _ := hist["existence"].(map[string]any)
	end, _ := ex["description_at_end"].(map[string]any)
	if p, ok := end["payload"].(map[string]any); !ok || len(p) != 0 {
		t.Errorf("entity_history: description_at_end payload %v, want the empty object", end["payload"])
	}
	if note, _ := end["note"].(string); !strings.Contains(note, "not a JSON object") {
		t.Errorf("entity_history: description_at_end note %q, want the withheld-payload note", note)
	}

	res := call(t, cs, "resolve_entity", map[string]any{"type": "k8s.pod", "attributes": map[string]any{"k8s.pod.uid": "x"}, "at": rfc(r.at)})
	if ex, _ := structured(t, "resolve_entity", res)["existence"].(map[string]any); ex["status"] != "not_alive" {
		t.Errorf("resolve_entity of an unknown pod: %v", ex)
	}

	// A listing of the pods at the instant of the fixture lists the pod, at the
	// store's token: the payloads are not JSON objects, so no name is known.
	found := structured(t, "find_entities", call(t, cs, "find_entities", map[string]any{"type": "k8s.pod", "at": rfc(r.at)}))
	if found["token"] != token {
		t.Errorf("find_entities: token %v, want the store's last seq %s", found["token"], token)
	}
	listed := false
	matches, _ := found["matches"].([]any)
	for _, m := range matches {
		if e, _ := m.(map[string]any)["entity"].(map[string]any); e["id"] == r.pod.String() {
			listed = true
		}
	}
	if !listed || found["derived"] != true {
		t.Errorf("find_entities: the pod %s is not among %d matches (derived %v)", r.pod, len(matches), found["derived"])
	}

	pod := r.pod.String()
	win := map[string]any{"from": rfc(r.at.Add(-time.Hour)), "to": rfc(r.at.Add(time.Hour))}
	for name, args := range map[string]map[string]any{
		"blast_radius": {"entity": pod, "from": win["from"], "to": win["to"]},
		"what_changed": {"scope": []string{pod}, "from": win["from"], "to": win["to"]},
		"co_residents": {"pod": pod, "from": win["from"], "to": win["to"]},
		// A service the replayed store has no record of: an answer, whole and empty.
		"service_context": {"service": serviceID(t, "checkout"), "from": win["from"], "to": win["to"]},
	} {
		res := call(t, cs, name, args)
		if res.IsError {
			t.Errorf("%s: error %q; want an answer", name, resultText(res))
			continue
		}
		if got := structured(t, name, res)["token"]; got != token {
			t.Errorf("%s: token %v, want the store's last seq %s", name, got, token)
		}
	}

	if err := cs.Close(); err != nil {
		t.Logf("closing the client: %v", err)
	}
	if code := s.exit(t); code != 0 {
		t.Fatalf("exit %d after the client closed its input:\n%s", code, s.stderr)
	}
	log := s.stderr.String()
	for _, want := range []string{"store opened read-only", "the client closed its input", "tools stopped", "store closed"} {
		if !strings.Contains(log, want) {
			t.Errorf("the log does not say %q:\n%s", want, log)
		}
	}
	if n := onlyFrames(t, s.stdout.String()); n < 8 {
		t.Errorf("%d messages on stdout, want a response to every request", n)
	}
	if strings.Contains(s.stdout.String(), "store opened") {
		t.Error("a log line reached stdout")
	}
	if after := snapshotDir(t, r.dir); !slices.Equal(before, after) {
		t.Errorf("the session changed the data directory:\nbefore %v\nafter  %v", before, after)
	}
}

// snapshotDir is a digest of every file under dir, by path.
func snapshotDir(t *testing.T, dir string) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		sum := sha256.Sum256(b)
		rel, _ := filepath.Rel(dir, p)
		out = append(out, rel+" "+hex.EncodeToString(sum[:8]))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// spyStore holds every Alive, as heldReader does, and counts what is done to the
// store: the tools may never write or retain, and the command closes it once.
type spyStore struct {
	*heldReader
	writes, retains, closes atomic.Int32
	readsEndedAtClose       atomic.Int32
	began                   chan struct{} // closed when the first read begins
	beganOnce               sync.Once
}

// Alive says a read has begun, to any number of waiters, and then holds it.
func (s *spyStore) Alive(ctx context.Context, fp identity.Fingerprint, at time.Time, sc store.Scope) (bool, error) {
	s.beganOnce.Do(func() { close(s.began) })
	return s.heldReader.Alive(ctx, fp, at, sc)
}

func (s *spyStore) Write(ctx context.Context, rs []store.Record) error {
	s.writes.Add(1)
	return s.Store.Write(ctx, rs)
}

func (s *spyStore) Retain(ctx context.Context, h time.Time) error {
	s.retains.Add(1)
	return s.Store.Retain(ctx, h)
}

func (s *spyStore) Close() error {
	s.closes.Add(1)
	s.readsEndedAtClose.Store(s.finished.Load())
	return s.Store.Close()
}

// spyOnTheStore makes the command open its store through a spy.
func spyOnTheStore(t *testing.T) *spyStore {
	t.Helper()
	spy := &spyStore{began: make(chan struct{})}
	setSeam(t, &openMCPStore, func(dir string) (store.Store, error) {
		st, err := openQueryStore(dir)
		if err != nil {
			return nil, err
		}
		spy.heldReader = newHeldReader(st)
		return spy, nil
	})
	return spy
}

// heldCall sends a resolve_entity with an instant, which reads the store once,
// and waits until the read has begun. The call's own result is dropped.
func heldCall(t *testing.T, cs *sdk.ClientSession, spy *spyStore) {
	t.Helper()
	go func() {
		_, _ = cs.CallTool(context.Background(), &sdk.CallToolParams{Name: "resolve_entity", Arguments: resolveAtArgs("x", time.Now())})
	}()
	spy.waitEntered(t, 1)
}

func assertStoppedInOrder(t *testing.T, s *session, spy *spyStore) {
	t.Helper()
	if spy.foundClosed.Load() {
		t.Error("a call found the store closed")
	}
	if got := spy.closes.Load(); got != 1 {
		t.Errorf("the store was closed %d times, want once", got)
	}
	if got := spy.readsEndedAtClose.Load(); got != 1 {
		t.Errorf("%d reads had ended when the store closed, want the 1 that was running", got)
	}
	if w, r := spy.writes.Load(), spy.retains.Load(); w != 0 || r != 0 {
		t.Errorf("the tools wrote %d times and retained %d times", w, r)
	}
	log := s.stderr.String()
	if i, j := strings.Index(log, "tools stopped"), strings.Index(log, "store closed"); i < 0 || j < 0 || i > j {
		t.Errorf("the tools stopped at %d and the store closed at %d; want the tools first:\n%s", i, j, log)
	}
}

// When the client closes its input with a call reading the store, the command
// does not end, and does not close the store, until that read has returned.
func TestMCPEndOfInputWaitsForARunningCallBeforeClosingTheStore(t *testing.T) {
	r := replayFixture(t)
	spy := spyOnTheStore(t)
	s := runSession(t, "--data-dir", r.dir)
	cs := s.connect(t)
	t.Cleanup(func() { spy.open() }) // a failed test must not leave the call held
	heldCall(t, cs, spy)

	_ = s.stdin.Close() // the client goes away
	time.Sleep(300 * time.Millisecond)
	if s.ended() || spy.closes.Load() != 0 {
		t.Fatalf("the command ended (%v) or closed the store (%d) while a call was reading:\n%s", s.ended(), spy.closes.Load(), s.stderr)
	}
	spy.open()
	if code := s.exit(t); code != 0 {
		t.Fatalf("exit %d:\n%s", code, s.stderr)
	}
	if !strings.Contains(s.stderr.String(), "the client closed its input") {
		t.Errorf("the log does not say why it stopped:\n%s", s.stderr)
	}
	assertStoppedInOrder(t, s, spy)
}

// The same on a signal: it ends the session, says so, and waits for the read.
func TestMCPSignalWaitsForARunningCallBeforeClosingTheStore(t *testing.T) {
	r := replayFixture(t)
	spy := spyOnTheStore(t)
	s := runSession(t, "--data-dir", r.dir)
	cs := s.connect(t)
	t.Cleanup(func() { spy.open() })
	heldCall(t, cs, spy)

	s.cancel() // the signal
	time.Sleep(300 * time.Millisecond)
	if s.ended() || spy.closes.Load() != 0 {
		t.Fatalf("the command ended (%v) or closed the store (%d) while a call was reading:\n%s", s.ended(), spy.closes.Load(), s.stderr)
	}
	spy.open()
	if code := s.exit(t); code != 0 {
		t.Fatalf("exit %d after a signal:\n%s", code, s.stderr)
	}
	if !strings.Contains(s.stderr.String(), "stopped by a signal") {
		t.Errorf("the log does not say a signal stopped it:\n%s", s.stderr)
	}
	assertStoppedInOrder(t, s, spy)
}

// A session that ends while a call is still reading, as one that did not wait
// for its calls would, still does not let the store close: the drain waits.
func TestMCPDrainHoldsTheStoreOpenForACallTheSessionLeftRunning(t *testing.T) {
	r := replayFixture(t)
	spy := spyOnTheStore(t)
	setSeam(t, &serveMCP, func(ctx context.Context, reads *mcphttp.Server, in io.Reader, out io.Writer) error {
		go func() { _ = reads.ServeStdio(ctx, in, out) }()
		select { // the session ends as soon as a call is in the store
		case <-spy.began:
		case <-ctx.Done():
		}
		return nil
	})
	s := runSession(t, "--data-dir", r.dir)
	cs := s.connect(t)
	t.Cleanup(func() { spy.open() })
	go func() {
		_, _ = cs.CallTool(context.Background(), &sdk.CallToolParams{Name: "resolve_entity", Arguments: resolveAtArgs("x", time.Now())})
	}()
	spy.waitEntered(t, 1)

	time.Sleep(300 * time.Millisecond)
	if s.ended() || spy.closes.Load() != 0 {
		t.Fatalf("the command ended (%v) or closed the store (%d) under a read:\n%s", s.ended(), spy.closes.Load(), s.stderr)
	}
	spy.open()
	if code := s.exit(t); code != 0 {
		t.Fatalf("exit %d:\n%s", code, s.stderr)
	}
	assertStoppedInOrder(t, s, spy)
}

// Nothing is written to the store whatever is asked: a spy sees no Write and no
// Retain across every tool, range tools included.
func TestMCPNeverWritesToTheStore(t *testing.T) {
	r := replayFixture(t)
	spy := spyOnTheStore(t)
	s := runSession(t, "--data-dir", r.dir)
	cs := s.connect(t)
	spy.open()
	pod := r.pod.String()
	from, to := rfc(r.at.Add(-time.Hour)), rfc(r.at.Add(time.Hour))
	for name, args := range map[string]any{
		"blast_radius":    map[string]any{"entity": pod, "from": from, "to": to},
		"what_changed":    map[string]any{"scope": []string{pod}, "from": from, "to": to},
		"co_residents":    map[string]any{"pod": pod, "from": from, "to": to},
		"service_context": map[string]any{"service": serviceID(t, "checkout"), "from": from, "to": to},
		"neighbors_at":    map[string]any{"entity": pod, "at": rfc(r.at)},
		"entity_history":  map[string]any{"entity": pod, "from": from, "to": to},
		"resolve_entity":  resolveAtArgs("x", r.at),
		"describe_schema": map[string]any{},
	} {
		call(t, cs, name, args)
	}
	_ = cs.Close()
	if code := s.exit(t); code != 0 {
		t.Fatalf("exit %d:\n%s", code, s.stderr)
	}
	if w, rt, c := spy.writes.Load(), spy.retains.Load(), spy.closes.Load(); w != 0 || rt != 0 || c != 1 {
		t.Errorf("writes %d, retains %d, closes %d; want 0, 0 and 1", w, rt, c)
	}
}

// failingReader fails the test if the command reads its input.
type failingReader struct{ t *testing.T }

func (f failingReader) Read([]byte) (int, error) {
	f.t.Error("the command read its input")
	return 0, io.EOF
}

// A directory that is no store, a directory another process holds, and a bad
// command line are each refused in a sentence, before the input is read and with
// nothing on stdout, and nothing is created.
func TestMCPRefusals(t *testing.T) {
	setSeam(t, &mcpStdin, io.Reader(failingReader{t}))
	r := replayFixture(t)

	refused := func(t *testing.T, wantCode int, wantErr string, args ...string) {
		t.Helper()
		code, out, errs := cli(t, append([]string{"mcp"}, args...)...)
		if code != wantCode || out != "" || !strings.Contains(errs, wantErr) {
			t.Errorf("mcp %v: exit %d, stdout %q, stderr %q; want exit %d, nothing on stdout, and %q", args, code, out, errs, wantCode, wantErr)
		}
		if wantCode == 1 && strings.Count(errs, "\n") != 1 { // a mistake in the flags gets the usage, as for query
			t.Errorf("mcp %v: stderr is not a sentence: %q", args, errs)
		}
	}

	t.Run("command line", func(t *testing.T) {
		refused(t, 2, "toposhift mcp: --data-dir is required")
		refused(t, 2, `unexpected argument "extra"`, "--data-dir", r.dir, "extra")
		refused(t, 2, "flag provided but not defined", "--nope")
		refused(t, 2, "flag provided but not defined", "--data-dir", r.dir, "--listen", "127.0.0.1:0")
	})
	t.Run("help", func(t *testing.T) {
		for _, args := range [][]string{{"mcp", "help"}, {"mcp", "-h"}, {"mcp", "--help"}} {
			code, out, errs := cli(t, args...)
			if code != 0 || !strings.HasPrefix(out, "Usage: toposhift mcp --data-dir DIR") || errs != "" {
				t.Errorf("%v: exit %d, stdout %.60q, stderr %q", args, code, out, errs)
			}
		}
		if code, out, _ := cli(t, "help"); code != 0 || !strings.Contains(out, "  mcp ") {
			t.Errorf("the top-level usage does not list mcp: %q", out)
		}
	})
	t.Run("no store", func(t *testing.T) {
		missing := filepath.Join(t.TempDir(), "nowhere")
		refused(t, 1, "there is no store in "+missing, "--data-dir", missing)
		if _, err := os.Stat(missing); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("the command created %s (%v)", missing, err)
		}
		empty := t.TempDir()
		refused(t, 1, "there is no store in "+empty, "--data-dir", empty)
		if entries, _ := os.ReadDir(empty); len(entries) != 0 {
			t.Errorf("the command left %d entries in an empty directory", len(entries))
		}
		other := t.TempDir()
		if err := os.WriteFile(filepath.Join(other, "notes.txt"), []byte("mine"), 0o600); err != nil {
			t.Fatal(err)
		}
		refused(t, 1, "there is no store in "+other, "--data-dir", other)
		if entries, _ := os.ReadDir(other); len(entries) != 1 {
			t.Errorf("the command changed a directory that holds other files: %d entries", len(entries))
		}
		file := filepath.Join(t.TempDir(), "file")
		if err := os.WriteFile(file, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		refused(t, 1, file+" is not a directory", "--data-dir", file)
	})
	t.Run("held by another open", func(t *testing.T) {
		held, err := pebblestore.Open(r.dir, pebblestore.DefaultOptions())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = held.Close() }()
		refused(t, 1, "another toposhift process has "+r.dir+" open", "--data-dir", r.dir)
	})
}

// A store the command opened is closed, and so released: another open succeeds
// once the command has ended.
func TestMCPReleasesTheDirectory(t *testing.T) {
	r := replayFixture(t)
	s := runSession(t, "--data-dir", r.dir)
	cs := s.connect(t)
	call(t, cs, "describe_schema", map[string]any{})
	// While it runs, the directory is held.
	if code, _, errs := cli(t, "query", "alive", "--data-dir", r.dir, "--fp", r.pod.String(), "--at", rfc(r.at)); code != 1 || !strings.Contains(errs, "another toposhift process has") {
		t.Errorf("a query while the command runs: exit %d, stderr %q", code, errs)
	}
	_ = cs.Close()
	if code := s.exit(t); code != 0 {
		t.Fatalf("exit %d:\n%s", code, s.stderr)
	}
	if code, _, errs := cli(t, "query", "alive", "--data-dir", r.dir, "--fp", r.pod.String(), "--at", rfc(r.at)); code != 0 {
		t.Errorf("a query after the command ended: exit %d, stderr %q", code, errs)
	}
}

// lines reads what a child process writes to stdout, a line at a time.
type lines struct {
	ch chan string
}

func readLines(r io.Reader) *lines {
	l := &lines{ch: make(chan string, 256)}
	go func() {
		sc := bufio.NewScanner(r)
		sc.Buffer(nil, 4<<20)
		for sc.Scan() {
			l.ch <- sc.Text()
		}
		close(l.ch)
	}()
	return l
}

// response waits for the response to request id.
func (l *lines) response(t *testing.T, id int) map[string]any {
	t.Helper()
	for {
		select {
		case line, ok := <-l.ch:
			if !ok {
				t.Fatalf("stdout ended before the response to request %d", id)
			}
			var m map[string]any
			if err := json.Unmarshal([]byte(line), &m); err != nil {
				t.Fatalf("a line of stdout is not JSON: %.200q", line)
			}
			if got, _ := m["id"].(float64); int(got) == id {
				return m
			}
		case <-time.After(30 * time.Second):
			t.Fatalf("no response to request %d", id)
		}
	}
}

// child is the built binary running toposhift mcp.
type child struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	out    *lines
	stderr *lockedBuffer
	waited chan error
}

func startChild(t *testing.T, bin string, args ...string) *child {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), bin, append([]string{"mcp"}, args...)...)
	cmd.Env = []string{}
	cmd.Dir = t.TempDir()
	c := &child{cmd: cmd, stderr: &lockedBuffer{}, waited: make(chan error, 1)}
	cmd.Stderr = c.stderr
	var err error
	if c.stdin, err = cmd.StdinPipe(); err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	c.out = readLines(stdout)
	go func() { c.waited <- cmd.Wait() }()
	return c
}

func (c *child) send(t *testing.T, line string) {
	t.Helper()
	if _, err := io.WriteString(c.stdin, line+"\n"); err != nil {
		t.Fatal(err)
	}
}

func (c *child) handshake(t *testing.T) {
	t.Helper()
	c.send(t, `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"x","version":"0"}}}`)
	c.out.response(t, 1)
	c.send(t, `{"jsonrpc":"2.0","method":"notifications/initialized"}`)
}

// wait returns the exit code of the child, which must end within a bound.
func (c *child) wait(t *testing.T) int {
	t.Helper()
	select {
	case err := <-c.waited:
		if err == nil {
			return 0
		}
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			return ee.ExitCode()
		}
		t.Fatal(err)
	case <-time.After(30 * time.Second):
		t.Fatalf("the process did not end:\n%s", c.stderr)
	}
	return -1
}

// The built binary, as a client starts it: a real stdio session on a replayed
// store, a second process refused while the first holds the directory, and the
// two ways of ending, the input closing and SIGTERM, each with exit status 0.
func TestMCPAsAProcess(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping binary test in short mode")
	}
	r := replayFixture(t)
	bin := filepath.Join(t.TempDir(), "toposhift")
	if out, err := exec.CommandContext(t.Context(), "go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}
	var all []string // everything the first process wrote to stdout

	t.Run("a session, and a second process refused", func(t *testing.T) {
		c := startChild(t, bin, "--data-dir", r.dir)
		c.handshake(t)
		c.send(t, `{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"neighbors_at","arguments":{"entity":"`+r.pod.String()+`","at":"`+rfc(r.at)+`"}}}`)
		m := c.out.response(t, 2)
		res, _ := m["result"].(map[string]any)
		sc, _ := res["structuredContent"].(map[string]any)
		if res["isError"] == true || sc["token"] != strconvU64(r.last) {
			t.Errorf("neighbors_at: %v, want an answer at token %d", m, r.last)
		}
		c.send(t, `{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"blast_radius","arguments":{"entity":"`+r.pod.String()+`","from":"`+rfc(r.at)+`","to":"`+rfc(r.at.Add(time.Hour))+`"}}}`)
		m = c.out.response(t, 3)
		res, _ = m["result"].(map[string]any)
		sc, _ = res["structuredContent"].(map[string]any)
		if res["isError"] == true || sc["token"] != strconvU64(r.last) {
			t.Errorf("blast_radius: %v, want an answer at token %d", m, r.last)
		}

		// Another process, while this one holds the directory.
		second := exec.CommandContext(t.Context(), bin, "mcp", "--data-dir", r.dir)
		second.Env = []string{}
		second.Dir = t.TempDir()
		var out, errb bytes.Buffer
		second.Stdout, second.Stderr = &out, &errb
		second.Stdin = strings.NewReader("")
		err := second.Run()
		var ee *exec.ExitError
		if !errors.As(err, &ee) || ee.ExitCode() != 1 || out.Len() != 0 ||
			!strings.Contains(errb.String(), "toposhift mcp: another toposhift process has "+r.dir+" open") {
			t.Errorf("a second mcp: %v, stdout %q, stderr %q; want exit 1 and the sentence", err, out.String(), errb.String())
		}

		if err := c.cmd.Process.Signal(syscall.SIGTERM); err != nil {
			t.Fatal(err)
		}
		if code := c.wait(t); code != 0 {
			t.Errorf("exit %d after SIGTERM:\n%s", code, c.stderr)
		}
		log := c.stderr.String()
		for _, want := range []string{"stopped by a signal", "tools stopped", "store closed"} {
			if !strings.Contains(log, want) {
				t.Errorf("the log does not say %q:\n%s", want, log)
			}
		}
		for line := range c.out.ch {
			all = append(all, line)
		}
		for _, l := range all {
			onlyFrames(t, l+"\n")
		}
	})

	t.Run("the input closes", func(t *testing.T) {
		c := startChild(t, bin, "--data-dir", r.dir)
		c.handshake(t)
		_ = c.stdin.Close()
		if code := c.wait(t); code != 0 {
			t.Errorf("exit %d when the input closed:\n%s", code, c.stderr)
		}
		if !strings.Contains(c.stderr.String(), "the client closed its input") {
			t.Errorf("the log does not say why it stopped:\n%s", c.stderr)
		}
		for line := range c.out.ch {
			onlyFrames(t, line+"\n")
		}
	})
}

func strconvU64(n uint64) string { return strconv.FormatUint(n, 10) }

// serviceID is the id of the service with a name and no namespace, as resolve_entity
// computes it.
func serviceID(t *testing.T, name string) string {
	t.Helper()
	id, err := identity.NewResolver(catalog.Default()).Resolve(catalog.Service, []identity.Attr{{Key: catalog.ServiceName, Value: name}})
	if err != nil {
		t.Fatal(err)
	}
	return id.Fingerprint().String()
}
