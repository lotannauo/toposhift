package mcphttp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	core "github.com/lotannauo/toposhift/internal/mcp"
)

// runOverMemory runs the server over in-memory transports and returns a client
// session and the channel Run's result arrives on.
func runOverMemory(t testing.TB, s *Server, runCtx context.Context) (*sdk.ClientSession, <-chan error) {
	t.Helper()
	t1, t2 := sdk.NewInMemoryTransports()
	done := make(chan error, 1)
	go func() { done <- s.Run(runCtx, t1) }()
	cs, err := sdk.NewClient(&sdk.Implementation{Name: "test", Version: "0"}, nil).Connect(ctx, t2, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	return cs, done
}

func waitRun(t testing.TB, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return")
		return nil
	}
}

// Over a session the tools are the core's, with the hints of the HTTP route; on a
// store with no range reads the point tools answer and each range tool says why
// it does not; and a client that closes its end ends Run with no error.
func TestRunServesThePointToolsAndRefusesTheRangeToolsOverASession(t *testing.T) {
	w := newWorld(t, rankObjs)
	s := attached(t, pointReadsOnly{w.st}, rankObjs)
	cs, done := runOverMemory(t, s, ctx)

	lt, err := cs.ListTools(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(lt.Tools) != len(core.Tools()) {
		t.Fatalf("%d tools offered, the core has %d", len(lt.Tools), len(core.Tools()))
	}
	for _, tl := range lt.Tools {
		if a := tl.Annotations; a == nil || !a.ReadOnlyHint || !a.IdempotentHint || a.OpenWorldHint == nil || *a.OpenWorldHint {
			t.Errorf("%s: annotations %+v, want read-only, idempotent and closed-world", tl.Name, a)
		}
	}

	const refusal = "Range answers are not available on this store."
	token := tokenOf(w.st)
	var ranged, point int
	for _, c := range callsOn(w) {
		res := callTool(t, cs, c.name, c.args)
		if c.ranged {
			ranged++
			if !res.IsError || text(t, res) != refusal {
				t.Errorf("%s: error %v, %q; want the refusal sentence", c.label, res.IsError, text(t, res))
			}
			continue
		}
		point++
		m := answer(t, c.label, res)
		// Every answer that reads the store is read at one token, the store's last.
		if got := m["token"]; got != nil && got != token {
			t.Errorf("%s: token %v, want %s", c.label, got, token)
		}
	}
	if ranged == 0 || point == 0 {
		t.Fatalf("the calls cover %d range and %d point tools", ranged, point)
	}

	if err := cs.Close(); err != nil {
		t.Logf("client close: %v", err)
	}
	if err := waitRun(t, done); err != nil {
		t.Errorf("Run after the client closed: %v, want nil", err)
	}
}

// A cancelled context ends Run, with the context's error, and the client sees the
// session close.
func TestRunEndsWhenItsContextIsCancelled(t *testing.T) {
	w := newWorld(t, rankObjs)
	s := attached(t, pointReadsOnly{w.st}, rankObjs)
	runCtx, cancel := context.WithCancel(ctx)
	cs, done := runOverMemory(t, s, runCtx)
	callTool(t, cs, "describe_schema", map[string]any{})
	cancel()
	if err := waitRun(t, done); !errors.Is(err, context.Canceled) {
		t.Errorf("Run: %v, want the context's error", err)
	}
	if _, err := cs.CallTool(ctx, &sdk.CallToolParams{Name: "describe_schema", Arguments: map[string]any{}}); err == nil {
		t.Error("a call after the session was closed succeeded")
	}
}

const (
	initFrame = `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"x","version":"0"}}}` + "\n" +
		`{"jsonrpc":"2.0","method":"notifications/initialized"}` + "\n"
	describeFrame = `{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"describe_schema","arguments":{}}}` + "\n"
)

// holdOpen writes the frames to a pipe, then holds the input open for hold before it
// ends it, as a client that waits for its answers does.
func holdOpen(frames string, hold time.Duration) io.Reader {
	pr, pw := io.Pipe()
	go func() {
		_, _ = io.WriteString(pw, frames)
		time.Sleep(hold)
		_ = pw.Close()
	}()
	return pr
}

// Over the stdio framing the output holds nothing but one JSON-RPC response per
// request: no log line, no banner, no blank line.
func TestServeStdioWritesOnlyFrames(t *testing.T) {
	w := newWorld(t, rankObjs)
	s := attached(t, pointReadsOnly{w.st}, rankObjs)
	var out bytes.Buffer
	if err := s.ServeStdio(ctx, holdOpen(initFrame+describeFrame, 300*time.Millisecond), &out); err != nil {
		t.Fatalf("ServeStdio: %v", err)
	}
	if !strings.HasSuffix(out.String(), "\n") {
		t.Fatalf("the output does not end in a newline: %q", tail(out.String()))
	}
	ids := map[float64]bool{}
	sc := bufio.NewScanner(&out)
	sc.Buffer(nil, 1<<20)
	for sc.Scan() {
		var m map[string]any
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
			t.Fatalf("a line of output is not JSON: %q", tail(sc.Text()))
		}
		if m["jsonrpc"] != "2.0" || m["result"] == nil {
			t.Errorf("a line of output is not a JSON-RPC result: %q", tail(sc.Text()))
		}
		id, _ := m["id"].(float64)
		ids[id] = true
	}
	if len(ids) != 2 || !ids[1] || !ids[2] {
		t.Errorf("answered requests %v, want 1 and 2", ids)
	}
}

func tail(s string) string {
	if len(s) > 120 {
		return s[:120] + "..."
	}
	return s
}

// A cancelled context ends the session although nothing was ever written to the
// input and the read of it never returns.
func TestServeStdioEndsOnCancelWhileTheInputIsSilent(t *testing.T) {
	w := newWorld(t, rankObjs)
	s := attached(t, pointReadsOnly{w.st}, rankObjs)
	pr, pw := io.Pipe()
	defer func() { _ = pw.Close() }()
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	var out bytes.Buffer
	go func() { done <- s.ServeStdio(runCtx, pr, &out) }()
	time.Sleep(100 * time.Millisecond)
	cancel()
	if err := waitRun(t, done); !errors.Is(err, context.Canceled) {
		t.Errorf("ServeStdio: %v, want the context's error", err)
	}
	if out.Len() != 0 {
		t.Errorf("a session that was never spoken to wrote %q", tail(out.String()))
	}
}

// A line longer than a request body over HTTP ends the session with an error, and
// nothing is written.
func TestServeStdioRefusesAnOversizedLine(t *testing.T) {
	w := newWorld(t, rankObjs)
	s := attached(t, pointReadsOnly{w.st}, rankObjs)
	long := `{"jsonrpc":"2.0","id":1,"method":"` + strings.Repeat("a", MaxBodyBytes+1) + `"}` + "\n"
	var out bytes.Buffer
	if err := s.ServeStdio(ctx, holdOpen(long, 200*time.Millisecond), &out); err == nil {
		t.Error("an oversized line ended the session with no error")
	}
	if out.Len() != 0 {
		t.Errorf("an oversized line was answered: %q", tail(out.String()))
	}
}

// The input ending is the client going away: the call that was reading is
// cancelled, and the session is not over until it has returned, so that the
// store is never closed under it. Drain, which comes next, has nothing left to
// wait for.
func TestEndOfInputCancelsARunningCallAndWaitsForIt(t *testing.T) {
	w := newWorld(t, rankObjs)
	b := newBlocked(w.st)
	s := attached(t, b, rankObjs)
	t.Cleanup(b.open) // a failed test must not leave the call held

	call := `{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"resolve_entity","arguments":{"type":"host","attributes":{"host.id":"host-two"},"at":"` + w10 + `"}}}` + "\n"
	pr, pw := io.Pipe()
	done := make(chan error, 1)
	var out lockedBytes
	go func() { done <- s.ServeStdio(ctx, pr, &out) }()
	_, _ = io.WriteString(pw, initFrame+call)
	b.waitEntered(t, 1)
	_ = pw.Close() // the client goes away with a call in the store

	select {
	case err := <-done:
		t.Fatalf("ServeStdio returned (%v) while a call was reading", err)
	case <-time.After(300 * time.Millisecond):
	}
	b.open()
	if err := waitRun(t, done); err != nil {
		t.Errorf("ServeStdio after the input ended: %v, want nil", err)
	}
	if begun, finished := b.begun.Load(), b.finished.Load(); begun != 1 || finished != 1 {
		t.Errorf("when ServeStdio returned, %d reads had begun and %d had ended", begun, finished)
	}
	if err := s.Drain(ctx); err != nil {
		t.Errorf("Drain: %v", err)
	}
}

// lockedBytes is an output the test reads while the session writes it.
type lockedBytes struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBytes) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}
