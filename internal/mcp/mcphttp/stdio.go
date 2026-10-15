package mcphttp

import (
	"context"
	"io"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

// Run serves the tools to one client over t, until the client ends the session
// or ctx is done. It returns nil when the client ended it, ctx's error when ctx
// did, and otherwise what went wrong with the transport. The tools are the same
// as over HTTP, with the same gate before a store is attached and the same drain
// after; only the framing differs.
//
// A session is stateful on the wire, as the protocol's handshake makes it, but
// the tools keep nothing between calls: every answer carries the token it was
// read at.
func (s *Server) Run(ctx context.Context, t sdk.Transport) error { return s.sdk.Run(ctx, t) }

// ServeStdio is Run over the protocol's stdio framing: one JSON-RPC message per
// line, read from in and written to out. Nothing but those frames is ever
// written to out, and neither is closed.
//
// A cancelled ctx ends the session without waiting for the read of in, which may
// not be interruptible, as a read of a terminal is not; the read ends with in, or
// with the process. A line longer than [MaxBodyBytes] ends the session with an
// error, as a body of that size is refused over HTTP.
func (s *Server) ServeStdio(ctx context.Context, in io.Reader, out io.Writer) error {
	return s.Run(ctx, &sdk.IOTransport{Reader: io.NopCloser(in), Writer: nopCloser{out}, MaxLineLength: MaxBodyBytes})
}

// nopCloser is a writer the SDK may close without closing the process's stdout.
type nopCloser struct{ io.Writer }

func (nopCloser) Close() error { return nil }
