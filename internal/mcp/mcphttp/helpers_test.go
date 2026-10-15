package mcphttp

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

var ctx = context.Background()

const (
	w10   = "2026-01-01T10:00:00Z"
	w1030 = "2026-01-01T10:30:00Z"
)

// newServer returns a server with no store, closed when the test ends.
func newServer(t testing.TB, rank map[lifecycle.Producer]int) *Server {
	t.Helper()
	s, err := New(Config{Rank: rank, Version: "test"})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// attached returns a server over the fixture's store.
func attached(t testing.TB, st store.Store, rank map[lifecycle.Producer]int) *Server {
	t.Helper()
	s := newServer(t, rank)
	if err := s.Attach(st); err != nil {
		t.Fatal(err)
	}
	return s
}

// connect returns a client session over in-memory transports to the server's
// tools, with none of the HTTP handler's protections.
func connect(t testing.TB, s *Server) *sdk.ClientSession {
	t.Helper()
	t1, t2 := sdk.NewInMemoryTransports()
	ss, err := s.sdk.Connect(ctx, t1, nil)
	if err != nil {
		t.Fatal(err)
	}
	cs, err := sdk.NewClient(&sdk.Implementation{Name: "test", Version: "0"}, nil).Connect(ctx, t2, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cs.Close(); _ = ss.Close() })
	return cs
}

// call calls a tool. A protocol error fails the test: it is what an answer that
// does not fit its output schema becomes.
func callTool(t testing.TB, cs *sdk.ClientSession, name string, args any) *sdk.CallToolResult {
	t.Helper()
	res, err := cs.CallTool(ctx, &sdk.CallToolParams{Name: name, Arguments: args})
	if err != nil {
		t.Fatalf("%s: protocol error: %v", name, err)
	}
	return res
}

// text is the text of a result's first content.
func text(t testing.TB, res *sdk.CallToolResult) string {
	t.Helper()
	if len(res.Content) == 0 {
		t.Fatal("the result has no content")
	}
	c, ok := res.Content[0].(*sdk.TextContent)
	if !ok {
		t.Fatalf("content is %T, want text", res.Content[0])
	}
	return c.Text
}

// answer is the structured answer of a result that is not an error, as generic
// JSON.
func answer(t testing.TB, name string, res *sdk.CallToolResult) map[string]any {
	t.Helper()
	if res.IsError {
		t.Fatalf("%s: the call failed: %s", name, text(t, res))
	}
	b, err := json.Marshal(res.StructuredContent)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("%s: %v in %s", name, err, b)
	}
	return m
}

// notes lists the sentences of an answer's notes.
func notes(m map[string]any) string {
	var sb strings.Builder
	for _, n := range m["notes"].([]any) {
		sb.WriteString(n.(string))
		sb.WriteByte('\n')
	}
	return sb.String()
}

func tokenOf(st store.Store) string { return strconv.FormatUint(st.LastSeq(), 10) }
