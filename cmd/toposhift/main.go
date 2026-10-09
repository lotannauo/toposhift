// Command toposhift is the single toposhift binary: server and client in one.
//
//	toposhift serve     run the ingest and query service (boots with no config)
//	toposhift query     ask a store that replay filled a question, as JSON lines
//	toposhift replay    replay an activity file into a store
//	toposhift version   print build information
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"

	"github.com/lotannauo/toposhift/internal/server"
)

const envListen = "TOPOSHIFT_LISTEN"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Getenv, os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

// run is main without the process-global state, so tests can drive it.
// It returns the process exit code.
func run(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		usage(stderr)
		return 2
	}
	switch args[0] {
	case "serve":
		return serve(ctx, args[1:], getenv, stderr)
	case "query":
		return queryCmd(ctx, args[1:], stdout, stderr)
	case "replay":
		return replayCmd(ctx, args[1:], stdout, stderr)
	case "version":
		_, _ = fmt.Fprintln(stdout, version())
		return 0
	case "help", "-h", "--help":
		usage(stdout)
		return 0
	default:
		_, _ = fmt.Fprintf(stderr, "toposhift: unknown command %q\n\n", args[0])
		usage(stderr)
		return 2
	}
}

func serve(ctx context.Context, args []string, getenv func(string) string, stderr io.Writer) int {
	// Precedence, lowest to highest: defaults, environment, flags.
	// YAML config joins the chain when there is something to configure.
	cfg := server.DefaultConfig()
	if v := getenv(envListen); v != "" {
		cfg.Listen = v
	}

	fs := flag.NewFlagSet("toposhift serve", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&cfg.Listen, "listen", cfg.Listen, "address to listen on (env "+envListen+")")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if fs.NArg() > 0 {
		_, _ = fmt.Fprintf(stderr, "toposhift serve: unexpected argument %q\n", fs.Arg(0))
		return 2
	}

	log := slog.New(slog.NewTextHandler(stderr, nil))
	if err := server.Run(ctx, cfg, log); err != nil {
		log.Error("server failed", "err", err)
		return 1
	}
	return 0
}

func usage(w io.Writer) {
	_, _ = fmt.Fprint(w, `Usage: toposhift <command> [flags]

Commands:
  serve     run the ingest and query service (no configuration required)
  query     ask a store that replay filled a question (run: toposhift query help)
  replay    replay an activity file into a store (run: toposhift replay help)
  version   print build information
`)
}

func version() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "toposhift (unknown version)"
	}
	v := info.Main.Version
	if v == "" || v == "(devel)" {
		v = "devel"
	}
	return "toposhift " + v
}
