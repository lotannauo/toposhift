package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/lotannauo/toposhift/internal/mcp/mcphttp"
	"github.com/lotannauo/toposhift/internal/store"
)

const mcpUsage = `Usage: toposhift mcp --data-dir DIR

Serves the read-only topology tools to one MCP client over stdio, from a store
that toposhift replay filled and no other process holds. The client starts this
command and speaks the protocol on its input and output, one JSON-RPC message
per line. Nothing but those messages is written to the output; the log goes to
stderr. The command ends, with exit status 0, when the client closes its input
or on SIGINT or SIGTERM; a call still reading then is cancelled and waited for
before the store closes.

Every answer is read at one snapshot token and names it. The store is opened
read-only, as toposhift query opens it, so nothing is written to it; Pebble's
lock on the directory is still exclusive, so the command fails, saying so,
while a replay, a query, a serve or another mcp has the directory open.

Every tool answers, the range tools blast_radius, what_changed and
co_residents included. On a retained store a window that starts before a
layer's horizon is refused, and so is a description recorded before it, which
the store no longer keeps; the refusal says from when a window does not need it.

A server keeps its store in the store directory of its data directory: to read
it, stop the server and give DIR/store, not DIR. Closing the input before the
client has read its answers gives no output.

To serve the same tools from a running server instead, see toposhift serve.

Flags:
  --data-dir DIR   the store's directory (required), as for toposhift query
`

// mcpStdin is the input of the mcp command, a variable so that a test can give
// it a pipe.
var mcpStdin io.Reader = os.Stdin

// openMCPStore opens the store the tools read: read-only, as query opens it. It
// is a variable so that a test can put a spy between the tools and the store.
var openMCPStore = openQueryStore

// serveMCP runs the session. It is a variable so that a test can end the session
// without waiting for the calls it had running, to show that the stop still does.
var serveMCP = func(ctx context.Context, reads *mcphttp.Server, in io.Reader, out io.Writer) error {
	return reads.ServeStdio(ctx, in, out)
}

// mcpCmd is toposhift mcp.
func mcpCmd(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if len(args) > 0 && args[0] == "help" {
		args = []string{"-h"}
	}
	dataDir, err := parseMCPArgs(args, stdout, stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return finish(stderr, "mcp", err)
	}
	return finish(stderr, "mcp", runMCP(ctx, dataDir, stdin, stdout, stderr))
}

// parseMCPArgs reads a command line, with the conventions of the other commands:
// a mistake in the flags has already been printed and comes back as a flagError,
// and flag.ErrHelp means help was printed to stdout.
func parseMCPArgs(args []string, stdout, stderr io.Writer) (dataDir string, err error) {
	fs := flag.NewFlagSet("toposhift mcp", flag.ContinueOnError)
	fs.StringVar(&dataDir, "data-dir", "", "the store's directory")
	rest, err := parseFlags(fs, args, mcpUsage, stdout, stderr)
	if err != nil {
		return "", err
	}
	if len(rest) > 0 {
		return "", usagef("unexpected argument %q", rest[0])
	}
	if dataDir == "" {
		return "", usagef("--data-dir is required")
	}
	return dataDir, nil
}

// runMCP opens the store read-only, serves the tools on stdin and stdout until
// the client's input ends or ctx is done, and then stops the tools before it
// closes the store. Whatever is logged goes to stderr: stdout is the protocol's.
func runMCP(ctx context.Context, dataDir string, stdin io.Reader, stdout, stderr io.Writer) error {
	log := slog.New(slog.NewTextHandler(stderr, nil))
	st, err := openMCPStore(dataDir)
	if err != nil {
		return err
	}
	reads, err := newMCPServer(st, log)
	if err != nil {
		_ = st.Close()
		return err
	}
	log.Info("store opened read-only; serving the tools on stdio", "path", dataDir, "last_seq", st.LastSeq())

	serveErr := serveMCP(ctx, reads, stdin, stdout)
	switch {
	case errors.Is(serveErr, context.Canceled):
		log.Info("stopped by a signal")
		serveErr = nil
	case serveErr == nil:
		log.Info("the client closed its input")
	default:
		serveErr = fmt.Errorf("the session ended with an error: %w", serveErr)
	}

	// The session has returned, so no call is taking a new read; the drain waits
	// for any that is still in one. The store closes only after it.
	if !drainReads(reads, log) {
		return errors.Join(serveErr, errors.New("a call was still reading the store; it was left open"))
	}
	if err := st.Close(); err != nil {
		return errors.Join(serveErr, fmt.Errorf("closing the store: %w", err))
	}
	log.Info("store closed")
	return serveErr
}

// newMCPServer is the tools over st, with the authority order a replayed store
// is opened with. Every tool runs on it, the range tools included.
func newMCPServer(st store.Store, log *slog.Logger) (*mcphttp.Server, error) {
	reads, err := mcphttp.New(mcphttp.Config{
		Rank:    storeOptions().Policy.Rank,
		Version: version(),
		Logf:    func(format string, args ...any) { log.Error(fmt.Sprintf(format, args...)) },
	})
	if err != nil {
		return nil, err
	}
	if err := reads.Attach(st); err != nil {
		return nil, err
	}
	return reads, nil
}
