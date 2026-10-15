package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/lotannauo/toposhift/internal/datadir"
	"github.com/lotannauo/toposhift/internal/ingest/otlphttp"
	"github.com/lotannauo/toposhift/internal/replay/capture"
)

const serveUsage = `Usage: toposhift serve [flags]

Runs the ingest and query service. It needs no configuration: with none, it
listens on loopback and serves health; it keeps its data in
$HOME/.local/share/toposhift, takes OpenTelemetry logs at POST /v1/logs into
the store there, and keeps ingest off when no data directory resolves.

On a loopback listener it also serves read-only topology tools to MCP clients,
at POST /mcp (streamable HTTP, no session). They answer from the same store,
each answer read at one token, and are off on any other listen address until
the server has authentication.

Flags:
`

const dataDirHelp = `
Data directory

  The data directory is the first of these that is set:

    1. --data-dir
    2. $TOPOSHIFT_DATA_DIR
    3. $STATE_DIRECTORY, when it is one absolute path (systemd sets it for a
       unit with StateDirectory=); with several, colon-separated, or when it
       is not an absolute path, there is no data directory (it does not fall
       through to HOME) and you must name one with --data-dir
    4. $HOME/.local/share/toposhift, when HOME is set and absolute

  $XDG_DATA_HOME is not consulted, on macOS as on Linux. A relative path is
  taken against the working directory; a path that begins with "~" is refused.
  Boot prints the absolute path, and the symlink target when it differs.

  Missing directories are created with mode 0700; an existing directory is
  never chmodded or chowned. It holds store/, bus/ and meta/. One instance owns
  a directory at a time; a second one is refused and told which pid and host
  hold it. A directory with files in it that toposhift did not put there is
  refused.

  With no data directory, serve still boots and answers /healthz, and a POST
  to /v1/logs answers 503 saying to set --data-dir or TOPOSHIFT_DATA_DIR.
  The same happens when the default directory under HOME cannot be opened
  (HOME is read-only or missing, or the directory has other files in it); a
  directory you asked for that cannot be opened ends serve with an error.
  The temp directory is never a fallback. A directory inside it, a world-writable
  one without the sticky bit, and one on NFS or SMB each get a warning at boot.

  A container image is to set TOPOSHIFT_DATA_DIR=/var/lib/toposhift, a volume
  owned by the image's non-root user. In Kubernetes, set fsGroup so the mounted
  volume is writable by that user; with a read-only root filesystem the data
  directory must be a mounted volume. An emptyDir volume lasts only as long as
  its pod. A backup that copies the directory while toposhift runs, Time
  Machine included, may copy it mid-write; stop the instance or snapshot the
  volume for a consistent copy.
`

// bootDataDir resolves the data directory, says where it is, and opens it. It
// returns the open directory (nil when there is none; Close on nil is safe),
// the sentence the receiver answers 503 with, and a nonzero exit code when
// serve must not start: a refused value is a mistake in how serve was called
// (2), and a directory that was asked for and cannot be opened is a failure
// (1). The default location under HOME is not asked for: when it cannot be
// opened, ingest is off and serve boots.
func bootDataDir(flagValue string, getenv func(string) string, log *slog.Logger, stderr io.Writer) (*datadir.Dir, string, int) {
	cwd, _ := os.Getwd() // unknown means a relative value is refused
	res, err := datadir.Resolve(datadir.Flags{DataDir: flagValue}, getenv, cwd)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "toposhift serve: %v\n", err)
		return nil, "", 2
	}
	for _, note := range res.Notes {
		log.Warn(note)
	}
	if res.Dir == "" {
		log.Warn(datadir.NoDirectoryMessage)
		return nil, datadir.NoDirectoryMessage, 0
	}
	attrs := []any{"path", res.Dir, "source", res.Source}
	if res.Target != "" {
		attrs = append(attrs, "resolves_to", res.Target)
	}
	log.Info("data directory", attrs...)

	dir, err := datadir.Open(res)
	if err != nil && res.Source == datadir.SourceHome {
		// Nobody asked for this directory: it is where the data goes when
		// nothing says otherwise. Where it cannot be used (a read-only or
		// missing home, files of somebody else's in it), serve still boots
		// with ingest off, as it does with no directory.
		msg := "ingest is off: the data directory " + res.Dir + " could not be opened; set --data-dir or " + datadir.EnvDataDir
		log.Warn("ingest is off: the data directory could not be opened", "path", res.Dir, "err", err)
		return nil, msg, 0
	}
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "toposhift serve: %v\n", err)
		return nil, "", 1
	}
	for _, w := range dir.Warnings {
		log.Warn(w)
	}
	return dir, ingestStarting, 0
}

// gateSink takes no logs: it answers 503 with Retry-After: 30 and the reason,
// while ingest is off or the store is opening.
type gateSink struct{ message string }

func (g gateSink) Accept(context.Context, capture.LogsData) error {
	return &otlphttp.UnavailableError{Message: g.message}
}
