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
	"time"

	"github.com/lotannauo/toposhift/internal/datadir"
	"github.com/lotannauo/toposhift/internal/ingest/clustername"
	"github.com/lotannauo/toposhift/internal/ingest/otlphttp"
	"github.com/lotannauo/toposhift/internal/ingest/sequencer"
	"github.com/lotannauo/toposhift/internal/server"
	"github.com/lotannauo/toposhift/internal/store"
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
	// An empty TOPOSHIFT_CLUSTER_NAME is the same as an unset one: no name.
	clusterName := getenv(envClusterName)

	fs := flag.NewFlagSet("toposhift serve", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&cfg.Listen, "listen", cfg.Listen, "address to listen on (env "+envListen+")")
	dataDir := fs.String("data-dir", "", "directory for the data (env "+datadir.EnvDataDir+"); without one, ingest is off")
	fs.StringVar(&clusterName, "cluster-name", clusterName,
		"the name of the cluster the logs come from, at most 21 bytes of [A-Za-z0-9._:/-] (env "+envClusterName+"); empty is none")
	accept := fs.String(clustername.FlagName, "",
		"accept, for this boot, that the store's recorded cluster name changes to the configured one (command line only; give it empty to accept that none is set)")
	pullInterval := fs.Duration("pull-interval", sequencer.DefaultPullInterval, "the interval of the collector's pulls of pods and nodes")
	fs.Usage = func() {
		_, _ = fmt.Fprint(stderr, serveUsage)
		fs.PrintDefaults()
		_, _ = fmt.Fprint(stderr, dataDirHelp)
		_, _ = fmt.Fprint(stderr, ingestHelp)
	}
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
	ic := ingestConfig{
		clusterName:  clusterName,
		set:          clusterName != "",
		accept:       *accept,
		acceptGiven:  setFlags(fs)[clustername.FlagName],
		pullInterval: *pullInterval,
	}
	if ic.set {
		if err := store.CheckClusterNameLimit(ic.clusterName); err != nil {
			_, _ = fmt.Fprintf(stderr, "toposhift serve: --cluster-name: %v\n", err)
			return 2
		}
	}
	if ic.pullInterval <= 0 {
		_, _ = fmt.Fprintf(stderr, "toposhift serve: --pull-interval %s is not above zero\n", ic.pullInterval)
		return 2
	}

	log := slog.New(slog.NewTextHandler(stderr, nil))
	dir, gate, code := bootDataDir(*dataDir, getenv, log, stderr)
	if code != 0 {
		return code
	}
	defer func() { _ = dir.Close() }()

	// The server starts first, so that health is served and a request is
	// answered 503 while the store opens.
	sink := &switchSink{}
	sink.set(gateSink{message: gate})
	cfg.Routes = []server.Route{{
		Pattern: "POST /v1/logs",
		Handler: otlphttp.Handler(sink, otlphttp.Options{MaxInFlightBytes: 2 * otlphttp.DefaultMaxBodyBytes}),
	}}
	signalled := make(chan time.Time, 1)
	context.AfterFunc(ctx, func() { signalled <- time.Now() })
	stopped := make(chan struct{})
	var srvErr error
	go func() {
		srvErr = server.Run(ctx, cfg, log)
		close(stopped)
	}()

	var in *ingest
	if dir != nil {
		in = startIngest(ctx, dir, ic, sink, log, stopped)
	}

	// The server returns after its graceful shutdown, while the sequencer still
	// runs, so the requests in flight are answered; then ingest stops, and the
	// store and the directory close.
	<-stopped
	select {
	case at := <-signalled:
		log.Info("server stopped", "took", time.Since(at))
	default:
	}
	in.stop(log)
	if srvErr != nil {
		log.Error("server failed", "err", srvErr)
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
