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
	"github.com/lotannauo/toposhift/internal/mcp/mcphttp"
	"github.com/lotannauo/toposhift/internal/server"
	"github.com/lotannauo/toposhift/internal/store"
)

const envListen = "TOPOSHIFT_LISTEN"

// mcpPattern is the route of the read-only tools.
const mcpPattern = "POST /mcp"

// mcpDrainTimeout is how long a stop waits for the tools' calls to end once it
// has cancelled them; each ends within one store read. It is an estimate. It is
// a variable only so tests can lower it.
var mcpDrainTimeout = 5 * time.Second

// drainReads ends the tools: no new call is taken, the ones running are cancelled,
// and it waits for them. It returns whether the store may be closed. It may not
// when a call is still reading it after mcpDrainTimeout: the store is then left
// open for the process to end with.
func drainReads(reads *mcphttp.Server, log *slog.Logger) (storeMayClose bool) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), mcpDrainTimeout)
	defer cancel()
	if err := reads.Drain(ctx); err != nil {
		log.Error("the tools did not stop in time; the store is left open, because a call may still be reading it",
			"timeout", mcpDrainTimeout, "err", err)
		return false
	}
	log.Info("tools stopped", "took", time.Since(start))
	return true
}

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
	// An empty TOPOSHIFT_NODE_INTERVAL is the same as an unset one.
	nodeInterval := sequencer.DefaultNodeInterval
	if v := getenv(envNodeInterval); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			_, _ = fmt.Fprintf(stderr, "toposhift serve: %s %q: %v\n", envNodeInterval, v, err)
			return 2
		}
		nodeInterval = d
	}
	// An empty TOPOSHIFT_RETAIN is the same as an unset one: on.
	retain := getenv(envRetain)
	if retain == "" {
		retain = "on"
	}

	fs := flag.NewFlagSet("toposhift serve", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&cfg.Listen, "listen", cfg.Listen, "address to listen on (env "+envListen+")")
	dataDir := fs.String("data-dir", "", "directory for the data (env "+datadir.EnvDataDir+"); without one, ingest is off")
	fs.StringVar(&clusterName, "cluster-name", clusterName,
		"the name of the cluster the logs come from, at most 21 bytes of [A-Za-z0-9._:/-] (env "+envClusterName+"); empty is none")
	accept := fs.String(clustername.FlagName, "",
		"accept, for this boot, that the store's recorded cluster name changes to the configured one (command line only; give it empty to accept that none is set)")
	pullInterval := fs.Duration("pull-interval", sequencer.DefaultPullInterval, "the interval of the collector's pulls of pods and nodes")
	fs.DurationVar(&nodeInterval, "node-interval", nodeInterval,
		"the report interval of the node agents, a whole number of seconds from 1m to 1h; their records live for three of them (env "+envNodeInterval+")")
	fs.StringVar(&retain, "retain", retain, "on or off: discard history older than each layer's window once a day, or keep everything (env "+envRetain+")")
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
		nodeInterval: nodeInterval,
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
	if err := checkNodeInterval(ic.nodeInterval); err != nil {
		_, _ = fmt.Fprintf(stderr, "toposhift serve: %v\n", err)
		return 2
	}
	switch retain {
	case "on":
		ic.retain = true
	case "off":
	default:
		_, _ = fmt.Fprintf(stderr, "toposhift serve: --retain %q: give on or off\n", retain)
		return 2
	}

	log := slog.New(slog.NewTextHandler(stderr, nil))
	if !ic.retain {
		log.Info("retention is off: no retention is asked for; a pass an earlier boot started still completes")
	}
	dir, gate, code := bootDataDir(*dataDir, getenv, log, stderr)
	if code != 0 {
		return code
	}
	defer func() { _ = dir.Close() }()

	// The server starts first, so that health is served and a request is
	// answered 503 while the store opens.
	sink := &switchSink{}
	sink.set(gateSink{message: gate})
	status := &statusRoute{retain: ic.retain}
	cfg.Routes = []server.Route{{
		Pattern: "POST /v1/logs",
		Handler: otlphttp.Handler(sink, otlphttp.Options{MaxInFlightBytes: 2 * otlphttp.DefaultMaxBodyBytes}),
	}, {
		Pattern: "GET /status",
		Handler: status,
	}}
	// The read-only tools are mounted the same way: before the store opens, and
	// answering with the receiver's sentence until it has. They are served on a
	// loopback listener only, because the server has no authentication yet.
	reads, err := mcphttp.New(mcphttp.Config{
		Rank:    serveRank(ic.clusterName, ic.set),
		Version: version(),
		Logf:    func(format string, args ...any) { log.Error(fmt.Sprintf(format, args...)) },
	})
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "toposhift serve: %v\n", err)
		return 1
	}
	reads.Unavailable(gate)
	if cfg.IsLoopback() {
		cfg.Routes = append(cfg.Routes, server.Route{Pattern: mcpPattern, Handler: reads.Handler()})
	} else {
		log.Info("the tools at " + mcpPattern + " are off: the server listens on a non-loopback address and has no authentication yet")
	}
	logStop := watchStop(ctx, nil)
	stopped := make(chan struct{})
	var srvErr error
	go func() {
		srvErr = server.Run(ctx, cfg, log)
		close(stopped)
	}()

	var in *ingest
	if dir != nil {
		in = startIngest(ctx, dir, ic, sink, status, reads, log, stopped)
	}

	// The server returns after its graceful shutdown, while the sequencer still
	// runs, so the requests in flight are answered; the tools' calls still
	// running are ended and waited for, because the server's shutdown does not wait
	// for them; then ingest stops, and the store and the directory close.
	<-stopped
	logStop(log)
	in.stop(log, drainReads(reads, log))
	if srvErr != nil {
		log.Error("server failed", "err", srvErr)
		return 1
	}
	return 0
}

// watchStop notes when ctx is done and returns the function that logs how long
// the server took to stop after that. beforeSend, when not nil, runs in the
// callback after the time is noted and before it is passed on; tests use it to
// make the callback slow.
func watchStop(ctx context.Context, beforeSend func()) (logStop func(*slog.Logger)) {
	signalled := make(chan time.Time, 1)
	context.AfterFunc(ctx, func() {
		at := time.Now()
		if beforeSend != nil {
			beforeSend()
		}
		signalled <- at
	})
	return func(log *slog.Logger) {
		// With ctx live, the server returned on its own, as one that failed at
		// startup does: no signal came, and none is waited for. With ctx done,
		// the callback runs exactly once, on a goroutine that may not have run
		// yet, and sends into a channel with room for it, so this receive ends.
		if ctx.Err() == nil {
			return
		}
		at := <-signalled
		log.Info("server stopped", "took", time.Since(at))
	}
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
