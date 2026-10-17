package mcphttp

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/lotannauo/toposhift/internal/lifecycle"
	core "github.com/lotannauo/toposhift/internal/mcp"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// MaxBodyBytes is the largest request body the handler reads. A request is one
// tool call: a few hundred bytes, or at most a scope of 256 entity ids.
const MaxBodyBytes = 64 << 10

// sentenceUnavailable is what a tool that reads the store answers before it has
// been given one, when the caller said nothing else.
const sentenceUnavailable = "The store is not available yet."

// sentenceShuttingDown is what a call answers once the server is drained.
const sentenceShuttingDown = "The server is shutting down."

// Config configures a [Server].
type Config struct {
	// Rank is the authority order of the descriptions of the store that will be
	// attached (lifecycle.Policy.Rank). Every answer carries its digest, before
	// the store is attached and after.
	Rank map[lifecycle.Producer]int
	// Version is the server's version, as the protocol's initialize answer
	// reports it.
	Version string
	// Logf receives one line for each internal error. Nil discards them.
	Logf func(format string, args ...any)
	// Instructions, when not empty, is appended after one space to the
	// server's own instructions, which the protocol's initialize answer carries.
	Instructions string
}

// A Server serves the tools over HTTP. Its zero value is not usable; start from
// [New]. It is safe for concurrent use.
type Server struct {
	cfg     Config
	sdk     *sdk.Server
	handler http.Handler
	// noStore answers the tools that read no store, until a store is attached. It
	// is built over a closed store, so a tool that did read it would be refused.
	noStore *core.Source

	mu    sync.Mutex // serializes the changes of state
	state atomic.Pointer[state]
}

// state is what a call sees of the store: a source over it, or the sentence to
// answer with while there is none.
type state struct {
	src      *core.Source
	sentence string
	drained  bool
}

// New returns a server with no store: every tool that reads one answers that the
// store is not available until [Server.Attach].
func New(cfg Config) (*Server, error) {
	closed, err := memstore.Open(memstore.Options{})
	if err != nil {
		return nil, fmt.Errorf("mcphttp: %w", err)
	}
	_ = closed.Close()
	noStore, err := core.NewSource(core.Config{Store: closed, Rank: cfg.Rank, Logf: cfg.Logf})
	if err != nil {
		return nil, fmt.Errorf("mcphttp: %w", err)
	}
	s := &Server{cfg: cfg, noStore: noStore}
	s.state.Store(&state{sentence: sentenceUnavailable})

	version := cfg.Version
	if version == "" {
		version = "devel"
	}
	text := instructions
	if cfg.Instructions != "" {
		text += " " + cfg.Instructions
	}
	s.sdk = sdk.NewServer(&sdk.Implementation{Name: "toposhift", Title: "toposhift topology graph", Version: version}, &sdk.ServerOptions{
		Instructions: text,
		// Only tools, and no list-changed notification: the list never changes and
		// there is no session to send one to.
		Capabilities: &sdk.ServerCapabilities{Tools: &sdk.ToolCapabilities{}},
	})
	if err := s.register(); err != nil {
		return nil, err
	}
	s.handler = s.buildHandler()
	return s, nil
}

// instructions is what a client is told about the server as a whole.
const instructions = "Read-only questions about a temporal graph of infrastructure. " +
	"Every answer is read at one snapshot token, named in the answer; pass it as asof to read the same world again. " +
	"Call describe_schema first for the entity types, the relations and the words every answer uses. " +
	"To go from a name to an id, call find_entities."

// Handler is the handler of the MCP endpoint. Mount it for POST only, on a
// loopback listener, and not at all on any other.
func (s *Server) Handler() http.Handler { return s.handler }

// Attach hands the server the open store its tools read. The store must have
// been opened with the authority order given to [New], which the tools use
// before and after, so the rank digest of an answer never differs between the
// two. The range tools run on a store that [core.RangeReadsFor] allows, memstore
// or pebblestore. It fails if a store is already attached or the server is
// drained.
func (s *Server) Attach(st store.Store) error {
	return s.attach(st, core.RangeReadsFor(st))
}

// attach is Attach with the range switch given, for tests that put a spy between
// the source and the in-memory store.
func (s *Server) attach(st store.Store, rangeReads bool) error {
	src, err := core.NewSource(core.Config{Store: st, Rank: s.cfg.Rank, RangeReads: rangeReads, Logf: s.cfg.Logf})
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cur := s.state.Load()
	switch {
	case cur.drained:
		return errors.New("mcphttp: Attach: the server is drained")
	case cur.src != nil:
		return errors.New("mcphttp: Attach: a store is already attached")
	}
	s.state.Store(&state{src: src})
	return nil
}

// Unavailable sets the sentence the tools that read the store answer with while
// there is none: the store is opening, or there is no data directory, or the
// store could not be opened. It has no effect once a store is attached or the
// server is drained.
func (s *Server) Unavailable(sentence string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cur := s.state.Load(); cur.src == nil && !cur.drained {
		s.state.Store(&state{sentence: sentence})
	}
}

// Drain stops the server taking calls and waits for the ones in flight: a new
// call is refused as shutting down, each running call ends within one store
// read, and a call queued for a free slot is let go. It returns when every call
// has returned, or with ctx's error if ctx ends first. A caller that gets an
// error must not close the store: a call may still be reading it.
func (s *Server) Drain(ctx context.Context) error {
	s.mu.Lock()
	cur := s.state.Load()
	s.state.Store(&state{src: cur.src, drained: true})
	s.mu.Unlock()
	if cur.src == nil {
		return nil
	}
	return cur.src.Drain(ctx)
}

// source is the source a call runs on. A tool that reads the store has none
// until one is attached, and gets the sentence instead.
func (s *Server) source(readsStore bool) (*core.Source, error) {
	st := s.state.Load()
	switch {
	case st.src != nil:
		return st.src, nil
	case !readsStore:
		return s.noStore, nil
	case st.drained:
		return nil, &core.Error{Message: sentenceShuttingDown, Cause: core.ErrShuttingDown}
	}
	return nil, &core.Error{Message: st.sentence}
}

// buildHandler is the endpoint with its protections, outermost first: the local
// address of the connection must be loopback, the standard library refuses a
// request a browser says comes from another site, and the SDK refuses a Host
// header that is not loopback's. The body cap and the content type are the SDK's.
func (s *Server) buildHandler() http.Handler {
	h := sdk.NewStreamableHTTPHandler(func(*http.Request) *sdk.Server { return s.sdk }, &sdk.StreamableHTTPOptions{
		// Every call carries its token, so there is no session to keep.
		Stateless:    true,
		JSONResponse: true,
		// The SDK's own protection against DNS rebinding stays on.
		DisableLocalhostProtection:   false,
		MaxRequestBodyBytes:          MaxBodyBytes,
		PropagateRequestCancellation: true,
	})
	return loopbackOnly(http.NewCrossOriginProtection().Handler(h))
}

// loopbackOnly answers 403 to a request whose connection was not accepted on a
// loopback address. The listener's configured address says only what was asked
// for; this is the address the connection arrived on. A request with no local
// address is refused too.
func loopbackOnly(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !isLoopbackAddr(r.Context().Value(http.LocalAddrContextKey)) {
			http.Error(w, "Forbidden: the tools are served on loopback only", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func isLoopbackAddr(v any) bool {
	a, ok := v.(net.Addr)
	if !ok || a == nil {
		return false
	}
	var ip net.IP
	switch a := a.(type) {
	case *net.TCPAddr:
		ip = a.IP
	default:
		host, _, err := net.SplitHostPort(a.String())
		if err != nil {
			return false
		}
		ip = net.ParseIP(host)
	}
	return ip != nil && ip.IsLoopback()
}
