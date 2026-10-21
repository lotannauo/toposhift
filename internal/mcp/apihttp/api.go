package apihttp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strconv"

	"github.com/lotannauo/toposhift/internal/mcp"
)

// MaxBodyBytes is the largest request body the routes read. A request is one
// question: a few hundred bytes, or at most a scope of 256 entity ids.
const MaxBodyBytes = 64 << 10

// The routes. Each takes POST.
const (
	PathScope          = "/api/v1/subgraph/scope"
	PathCoResidents    = "/api/v1/subgraph/co_residents"
	PathServiceContext = "/api/v1/subgraph/service_context"
	PathBlastRadius    = "/api/v1/subgraph/blast_radius"
)

// Paths lists the routes of [API.Handler], to mount it at each.
func Paths() []string {
	return []string{PathScope, PathCoResidents, PathServiceContext, PathBlastRadius}
}

// Sources gives the source the routes run on. *mcphttp.Server satisfies it, so
// the routes and its tools share the source: its slots, its deadline, its
// sentence before a store is attached, and its drain. The source is asked for on
// every request, because a server has none until a store is attached and none
// again once it is drained.
type Sources interface {
	// Source returns the source for a call that reads the store when readsStore
	// is true. Its error is an [*mcp.Error] that carries the sentence to answer
	// with, and wraps [mcp.ErrShuttingDown] once the server is drained.
	Source(readsStore bool) (*mcp.Source, error)
}

// An API serves the four routes. Its zero value is not usable; start from [New].
// It is safe for concurrent use.
type API struct {
	src     Sources
	routes  http.Handler
	handler http.Handler
}

// New returns the API over src.
func New(src Sources) *API {
	a := &API{src: src}
	mux := http.NewServeMux()
	mux.Handle(PathScope, question(a, (*mcp.Source).ScopeSubgraph, "scope, from, to, asof"))
	mux.Handle(PathCoResidents, question(a, (*mcp.Source).CoResidentsSubgraph, "pod, from, to, node_only, asof"))
	mux.Handle(PathServiceContext, question(a, (*mcp.Source).ServiceContextSubgraph, "service, from, to, node_only, asof"))
	mux.Handle(PathBlastRadius, question(a, (*mcp.Source).BlastRadiusSubgraph, "entity, from, to, depth, asof"))
	a.routes = setHeaders(mux)
	a.handler = protect(mux)
	return a
}

// Handler serves the routes of [Paths] behind the protections of the package. A
// caller mounts it at each path, on a loopback listener and on no other. The
// status of a refusal is chosen from its cause, in the order the MCP tools give
// their sentences:
//
//	not loopback, Host not loopback, cross-site browser request   403, plain text
//	not POST                                                      405  method_not_allowed
//	not application/json                                          415  unsupported_media_type
//	body over MaxBodyBytes                                        413  request_too_large
//	malformed JSON, unknown field, wrong type, data after it      400  invalid_request
//	busy                                                          503  busy, Retry-After: 5
//	no store attached yet                                         503  unavailable
//	shutting down, store closed, call cancelled by the drain      503  shutting_down
//	the store has no range reads                                  501  no_range_reads
//	deadline                                                      504  deadline
//	token ahead of the store                                      409  future_token
//	before the retention horizon, description not kept            410  before_horizon
//	invalid argument, identity error                              400  invalid_argument
//	answer over the size cap                                      422  answer_too_large
//	egress refusal, anything else                                 500  internal
func (a *API) Handler() http.Handler { return a.handler }

// Routes serves the routes of [Paths] with the method, content type, body and
// decoding checks and the headers of every response, and with none of the
// loopback, Host and cross-origin protections of [API.Handler]: a caller that
// puts its own verifier and its own checks in front owns that decision, and must
// not mount Routes where nothing does.
func (a *API) Routes() http.Handler { return a.routes }

// question is the route of one question: it checks the method, the content type
// and the body, decodes the body into the question's input, and asks the
// source. fields names the body's fields for the sentence that refuses another.
func question[In any](a *API, ask func(*mcp.Source, context.Context, In) (mcp.SubgraphAnswer, error), fields string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Use POST, with a JSON body.")
			return
		}
		if !isJSON(r.Header.Get("Content-Type")) {
			writeError(w, http.StatusUnsupportedMediaType, "unsupported_media_type", "The Content-Type must be application/json.")
			return
		}
		body, status := readBody(w, r)
		switch {
		case status == http.StatusRequestEntityTooLarge:
			writeError(w, status, "request_too_large", "The request body is over "+strconv.Itoa(MaxBodyBytes)+" bytes.")
			return
		case status != 0:
			if r.Context().Err() == nil {
				writeError(w, http.StatusBadRequest, "invalid_request", "The request body could not be read.")
			}
			return
		}
		var in In
		if !decode(body, &in) {
			writeError(w, http.StatusBadRequest, "invalid_request",
				"The request body must be one JSON object with only these fields: "+fields+".")
			return
		}

		src, err := a.src.Source(true)
		if err != nil {
			writeSourceError(w, err)
			return
		}
		ans, err := ask(src, r.Context(), in)
		if err != nil {
			if r.Context().Err() != nil {
				// The client went away: there is no one to answer.
				return
			}
			writeFailure(w, err)
			return
		}
		// The bytes the last check scanned, written as they are.
		b, err := ans.JSON()
		if err != nil {
			writeError(w, http.StatusInternalServerError, "internal", "Internal error: the answer could not be written.")
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(b)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(b)
	})
}

// isJSON says a Content-Type header is application/json, with or without
// parameters.
func isJSON(header string) bool {
	mt, _, err := mime.ParseMediaType(header)
	return err == nil && mt == "application/json"
}

// readBody reads the body, at most [MaxBodyBytes]. A non-zero status says it
// could not: 413 for a body over the cap, anything else for a read that failed.
func readBody(w http.ResponseWriter, r *http.Request) ([]byte, int) {
	if r.ContentLength > MaxBodyBytes {
		return nil, http.StatusRequestEntityTooLarge
	}
	b, err := io.ReadAll(http.MaxBytesReader(w, r.Body, MaxBodyBytes))
	var tooBig *http.MaxBytesError
	switch {
	case errors.As(err, &tooBig):
		return nil, http.StatusRequestEntityTooLarge
	case err != nil:
		return nil, http.StatusBadRequest
	}
	return b, 0
}

// decode reads body as exactly one JSON object into in, refusing a field in does
// not have and anything after the object.
func decode(body []byte, in any) bool {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(in); err != nil {
		return false
	}
	_, err := dec.Token()
	return errors.Is(err, io.EOF)
}
