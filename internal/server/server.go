// Package server provides the HTTP server for toposhift. It answers health
// checks itself and serves the routes its caller adds in [Config.Routes];
// ingest and query handlers are mounted that way.
package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"
)

// DefaultListen is the default loopback address used when unconfigured.
// A server started with no configuration must not be reachable from the network.
const DefaultListen = "127.0.0.1:7070"

// shutdownTimeout bounds the graceful shutdown. It must exceed the point at
// which net/http counts a connection that never sent a request as idle (after
// 5 whole seconds, noticed by a poll at most 500 ms apart, so up to about
// 6.5 s); otherwise Shutdown races its own deadline whenever a client holds a
// spare connection open. It is kept under 10 s so that even a forced close
// finishes within the default stop grace of container runtimes. It is a
// variable only so tests can lower it.
var shutdownTimeout = 8 * time.Second

// Config is the server configuration. The zero value is invalid; start from
// DefaultConfig.
type Config struct {
	// Listen is the TCP address to bind, in host:port form.
	Listen string

	// Routes are served beside GET /healthz. The zero value adds none.
	Routes []Route
}

// Route is one handler the server serves in addition to GET /healthz.
type Route struct {
	// Pattern is an [http.ServeMux] pattern, such as "POST /v1/logs". It must
	// not conflict with GET /healthz or another route.
	Pattern string
	Handler http.Handler
}

// DefaultConfig returns the default server configuration.
func DefaultConfig() Config {
	return Config{Listen: DefaultListen}
}

// IsLoopback reports whether the configured address binds only to loopback.
func (c Config) IsLoopback() bool {
	host, _, err := net.SplitHostPort(c.Listen)
	if err != nil {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// Run binds cfg.Listen and serves until ctx is cancelled.
func Run(ctx context.Context, cfg Config, log *slog.Logger) error {
	if cfg.Listen == "" {
		return errors.New("server: empty listen address")
	}
	handler, err := newHandler(cfg.Routes)
	if err != nil {
		return err
	}
	if !cfg.IsLoopback() {
		log.Warn("listening on a non-loopback address; the server has no authentication yet",
			"listen", cfg.Listen)
	}
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", cfg.Listen)
	if err != nil {
		return fmt.Errorf("server: listen on %q: %w", cfg.Listen, err)
	}
	return serve(ctx, ln, log, handler)
}

// Serve runs the HTTP server on ln until ctx is cancelled, then shuts down gracefully.
// It serves GET /healthz and routes. It returns nil on a clean shutdown, and an
// error, before serving, if a route is invalid or conflicts with another.
func Serve(ctx context.Context, ln net.Listener, log *slog.Logger, routes ...Route) error {
	handler, err := newHandler(routes)
	if err != nil {
		return err
	}
	return serve(ctx, ln, log, handler)
}

func serve(ctx context.Context, ln net.Listener, log *slog.Logger, handler http.Handler) error {
	srv := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}

	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()
	log.Info("listening", "addr", ln.Addr().String())

	select {
	case err := <-errc:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("server: serve: %w", err)
	case <-ctx.Done():
	}

	// ctx is already cancelled, so shutdown needs a fresh deadline.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		if !errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("server: shutdown: %w", err)
		}
		// Connections that outlived the deadline were asked to stop and did
		// not; the process is ending, so close them rather than fail.
		// srv.Close closes the listeners and every connection, so it cuts a
		// request still in flight. That is acceptable because ingest
		// acknowledges a write only after it is committed, so the sender
		// retries a request that was cut.
		log.Warn("forced close of connections still open")
		_ = srv.Close()
	}
	log.Info("stopped")
	return nil
}

// newHandler builds the mux. A pattern that is malformed or conflicts with
// another makes http.ServeMux panic; here it is an error.
func newHandler(routes []Route) (h http.Handler, err error) {
	defer func() {
		if r := recover(); r != nil {
			h, err = nil, fmt.Errorf("server: invalid route: %v", r)
		}
	}()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok\n"))
	})
	for _, r := range routes {
		if r.Pattern == "" || r.Handler == nil {
			return nil, errors.New("server: a route needs a pattern and a handler")
		}
		mux.Handle(r.Pattern, r.Handler)
	}
	return mux, nil
}
