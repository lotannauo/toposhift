package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"syscall"

	"github.com/lotannauo/toposhift/internal/demo"
	"github.com/lotannauo/toposhift/internal/mcp/mcphttp"
	"github.com/lotannauo/toposhift/internal/server"
)

const demoUsage = `Usage: toposhift demo [--listen ADDR | --stdio | --no-serve]

Builds a synthetic Kubernetes cluster in memory and serves the read-only
topology tools from it, with no cluster, no data directory and no configuration.
Nothing in it is real data. The cluster has two machines, four nodes and forty
pods, and its history is fixed: from 2026-10-01T00:00:00Z to 2026-10-01T06:00:00Z,
in which a rollout runs, a report job runs every hour, node-2 stops being ready,
its pods are evicted and created again on the other nodes (the pods a DaemonSet
keeps on node-2 stay), and node-2 is ready again. The same build always gives the same cluster.

The cluster is built the way a real one is read: synthetic requests in the shapes
the collectors send go through the same translators and sequencer that
toposhift serve runs, on a clock the command sets, into a store in memory. The
one simplification is that an evicted pod is deleted at once; the API would keep
it terminating until its kubelet confirms. What is built is printed first: the
window, the number of records, a digest of them, and two questions to ask.

The tools are served at POST /mcp, on the loopback listener only (the server has
no authentication), beside GET /healthz. There is no ingest: POST /v1/logs is
not served. With --stdio the tools are served to one client on standard input
and output instead, one JSON-RPC message per line, and nothing else is written
to the output. With --no-serve the command prints what it built and ends.

The command reads no environment variable (TOPOSHIFT_LISTEN does not apply),
has no data directory, and writes nothing to disk. It ends, with exit status 0,
on SIGINT or SIGTERM; a stdio session also ends when the client closes its
input. For real data, run toposhift serve.

Flags:
  --listen ADDR   the loopback address to serve on (default 127.0.0.1:7070)
  --stdio         serve one client on standard input and output
  --no-serve      build, print what was built, and end
`

// demoMode is what the demo does once it has built the cluster.
type demoMode int

const (
	demoHTTP demoMode = iota
	demoStdio
	demoNone
)

// demoCmd is toposhift demo. It takes no environment: it cannot find a data
// directory, and so cannot write one.
func demoCmd(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if len(args) > 0 && args[0] == "help" {
		args = []string{"-h"}
	}
	listen, mode, err := parseDemoArgs(args, stdout, stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return finish(stderr, "demo", err)
	}
	return finish(stderr, "demo", runDemo(ctx, listen, mode, stdin, stdout, stderr))
}

// parseDemoArgs reads a command line, with the conventions of the other commands.
func parseDemoArgs(args []string, stdout, stderr io.Writer) (listen string, mode demoMode, err error) {
	fs := flag.NewFlagSet("toposhift demo", flag.ContinueOnError)
	fs.StringVar(&listen, "listen", server.DefaultListen, "the loopback address to serve on")
	stdio := fs.Bool("stdio", false, "serve one client on standard input and output")
	noServe := fs.Bool("no-serve", false, "build, print what was built, and end")
	rest, err := parseFlags(fs, args, demoUsage, stdout, stderr)
	if err != nil {
		return "", 0, err
	}
	if len(rest) > 0 {
		return "", 0, usagef("unexpected argument %q", rest[0])
	}
	set := setFlags(fs)
	given := 0
	for _, name := range []string{"listen", "stdio", "no-serve"} {
		if set[name] {
			given++
		}
	}
	if given > 1 {
		return "", 0, usagef("give at most one of --listen, --stdio and --no-serve")
	}
	switch {
	case *stdio:
		mode = demoStdio
	case *noServe:
		mode = demoNone
	default:
		if !(server.Config{Listen: listen}).IsLoopback() {
			return "", 0, usagef("--listen %s: the demo serves the tools on loopback only, because the server has no authentication; give a loopback address such as %s", listen, server.DefaultListen)
		}
	}
	return listen, mode, nil
}

// runDemo builds the cluster and serves the tools from it. Whatever is logged goes
// to stderr: stdout is the protocol's on stdio.
func runDemo(ctx context.Context, listen string, mode demoMode, stdin io.Reader, stdout, stderr io.Writer) error {
	log := slog.New(slog.NewTextHandler(stderr, nil))
	policy := storeOptions().Policy
	policy.Rank = serveRank(demo.ClusterName, true)

	built, err := demo.Build(ctx, demo.NewWorld(demo.DefaultSeed), demo.Options{Policy: policy, Logger: log})
	if err != nil {
		if errors.Is(err, context.Canceled) {
			log.Info("stopped by a signal before the cluster was built")
			return nil
		}
		return err
	}
	log.Info("built the cluster in memory", "took", built.Took, "records", built.Records,
		"skips", demo.Total(built.Skips), "skips_unexplained", built.UnexplainedSkips(), "notes", demo.Total(built.Notes), "warnings", built.Warnings)
	_, _ = fmt.Fprint(stderr, built.Banner())
	if mode == demoNone {
		return errors.Join(built.Store.Close())
	}

	reads, err := mcphttp.New(mcphttp.Config{
		Rank:         policy.Rank,
		Version:      version(),
		Logf:         func(format string, args ...any) { log.Error(fmt.Sprintf(format, args...)) },
		Instructions: built.Instructions(),
	})
	if err == nil {
		err = reads.Attach(built.Store)
	}
	if err != nil {
		return errors.Join(err, built.Store.Close())
	}

	var serveErr error
	switch mode {
	case demoStdio:
		_, _ = fmt.Fprintln(stderr, "serving the tools on stdio")
		serveErr = serveMCP(ctx, reads, stdin, stdout)
		switch {
		case errors.Is(serveErr, context.Canceled):
			log.Info("stopped by a signal")
			serveErr = nil
		case serveErr == nil:
			log.Info("the client closed its input")
		default:
			serveErr = fmt.Errorf("the session ended with an error: %w", serveErr)
		}
	default:
		// The listener is bound here so that the address printed is the one
		// bound, and the one the server logs when it listens: the port is not 0
		// when 0 was asked for.
		var lc net.ListenConfig
		ln, err := lc.Listen(ctx, "tcp", listen)
		if err != nil {
			serveErr = fmt.Errorf("server: listen on %q: %w", listen, err)
			if errors.Is(err, syscall.EADDRINUSE) {
				serveErr = fmt.Errorf("%w; give another address with --listen", serveErr)
			}
			break
		}
		_, _ = fmt.Fprintf(stderr, "serving the tools at http://%s/mcp (POST), loopback only\n", ln.Addr())
		if err := server.Serve(ctx, ln, log, server.Route{Pattern: mcpPattern, Handler: reads.Handler()}); err != nil {
			serveErr = err
		}
	}

	// The server and the session have returned, so no call is taking a new read;
	// the drain waits for any that is still in one. The store closes only after it.
	if !drainReads(reads, log) {
		return errors.Join(serveErr, errors.New("a call was still reading the store; it was left open"))
	}
	if err := built.Store.Close(); err != nil {
		return errors.Join(serveErr, fmt.Errorf("closing the store: %w", err))
	}
	return serveErr
}
