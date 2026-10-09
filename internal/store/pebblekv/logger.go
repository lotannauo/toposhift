package pebblekv

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/cockroachdb/pebble/v2"
)

// logger sends Pebble's messages to log/slog: its informational output at debug
// level, its errors as warnings (Pebble recovers from them, and the caller
// learns of a failure that matters from the error it gets back), and a fatal
// condition as the end of the process.
//
// The destination is whatever slog's default logger is at the time of each
// message, so the application's own setup of slog applies without this package
// holding a logger.
type logger struct{}

var _ pebble.Logger = logger{}

func (logger) Infof(format string, args ...any) {
	l := slog.Default()
	if !l.Enabled(context.Background(), slog.LevelDebug) {
		return
	}
	l.Debug(fmt.Sprintf(format, args...), "component", "pebble")
}

func (logger) Errorf(format string, args ...any) {
	slog.Warn(fmt.Sprintf(format, args...), "component", "pebble")
}

// fatalExitCode is the exit status of a process Pebble reported a fatal
// condition in (EX_SOFTWARE).
const fatalExitCode = 70

// fatal ends the process. It is a variable so that a test can replace it with a
// panic, and must restore it afterwards.
var fatal = func(string) { os.Exit(fatalExitCode) }

// Fatalf logs the message at error level and ends the process. Pebble calls it for
// conditions it cannot continue from (a failed write to the log, some failed I/O):
// the database is unusable, and what the ingest layer had not acknowledged is
// replayed on the next start. It exits, as Pebble's own default logger does,
// instead of panicking, because a panic can be recovered (net/http recovers the
// panic of a handler) and the process would go on serving from a database Pebble
// considers broken.
func (logger) Fatalf(format string, args ...any) {
	msg := fmt.Sprintf("pebble: "+format, args...)
	slog.Error(msg, "component", "pebble")
	fatal(msg)
}
