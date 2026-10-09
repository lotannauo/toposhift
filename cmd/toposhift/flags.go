package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"syscall"

	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
	"github.com/lotannauo/toposhift/internal/store/pebblestore"
)

// usageError is a mistake in how a command was called. The command exits with
// status 2 and a plain sentence; any other error is a failure of the work asked
// for and exits with status 1.
type usageError struct{ msg string }

func (e usageError) Error() string { return e.msg }

func usagef(format string, args ...any) error {
	return usageError{msg: fmt.Sprintf(format, args...)}
}

// flagError is a mistake in the flags that the flag package has already printed.
type flagError struct{ err error }

func (e flagError) Error() string { return e.err.Error() }
func (e flagError) Unwrap() error { return e.err }

// finish prints err as "toposhift <name>: <sentence>" and returns the exit code.
func finish(stderr io.Writer, name string, err error) int {
	if err == nil {
		return 0
	}
	if errors.As(err, new(flagError)) {
		return 2 // the flag package has said what was wrong
	}
	_, _ = fmt.Fprintf(stderr, "toposhift %s: %v\n", name, err)
	if errors.As(err, new(usageError)) {
		return 2
	}
	return 1
}

// parseInterspersed parses flags that may come before, between or after the
// positional arguments (toposhift replay activity FILE --data-dir DIR), which
// the flag package alone would take for positional arguments from the first one
// on. Everything after a lone "--" is positional. It returns the positional
// arguments. flag.ErrHelp means help was asked for.
func parseInterspersed(fs *flag.FlagSet, args []string) ([]string, error) {
	var rest []string
	for i, a := range args {
		if a == "--" {
			args, rest = args[:i], args[i+1:]
			break
		}
	}
	var positional []string
	for {
		if err := fs.Parse(args); err != nil {
			return nil, err
		}
		if fs.NArg() == 0 {
			return append(positional, rest...), nil
		}
		positional = append(positional, fs.Arg(0))
		args = fs.Args()[1:]
	}
}

// parseFlags is parseInterspersed with the messages routed: help goes to stdout
// and exits 0, as a command's own help does, and a mistake goes to stderr as a
// flagError, the flag package having said what was wrong.
func parseFlags(fs *flag.FlagSet, args []string, usage string, stdout, stderr io.Writer) ([]string, error) {
	var buf bytes.Buffer
	fs.SetOutput(&buf)
	fs.Usage = func() { _, _ = fmt.Fprint(&buf, usage) }
	positional, err := parseInterspersed(fs, args)
	switch {
	case err == nil:
		return positional, nil
	case errors.Is(err, flag.ErrHelp):
		_, _ = stdout.Write(buf.Bytes())
		return nil, err
	}
	_, _ = stderr.Write(buf.Bytes())
	return nil, flagError{err}
}

// setFlags is the names of the flags a command line set.
func setFlags(fs *flag.FlagSet) map[string]bool {
	set := make(map[string]bool)
	fs.Visit(func(f *flag.Flag) { set[f.Name] = true })
	return set
}

// storeOptions are the options of the store, whichever engine holds it. Its
// lifecycle policy is the one a store that boots with no configuration has: it
// tells the boots of a host apart by the boot id the activity file carries on
// every host observation, so two live boots of one host are a loud clone
// collision and not a silent merge. A store that ignored the boot column would
// lose exactly that.
func storeOptions() pebblestore.Options { return pebblestore.DefaultOptions() }

// markerPrefix starts the name of the file Pebble keeps in every database
// directory to name its current manifest.
const markerPrefix = "marker.manifest."

// finderFile is the file macOS leaves in a directory it has shown; it does not
// make the directory non-empty.
const finderFile = ".DS_Store"

// checkDataDir says whether dir holds a Pebble database, and refuses a directory
// that is not empty and does not, so that a mistyped --data-dir cannot fill
// somebody's documents with database files. A directory that does not exist is
// fine (replay creates it), and so is one that holds nothing but a Finder file.
func checkDataDir(dir string) (holds bool, err error) {
	entries, err := os.ReadDir(dir)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("cannot read the data directory %s: %w", dir, err)
	}
	other := 0
	for _, e := range entries {
		switch {
		case strings.HasPrefix(e.Name(), markerPrefix):
			return true, nil
		case e.Name() != finderFile:
			other++
		}
	}
	if other > 0 {
		return false, fmt.Errorf("%s is not empty and does not hold a Pebble store; give an empty or new directory", dir)
	}
	return false, nil
}

// isLockHeld reports whether err says another open holds the database's
// directory lock: Pebble takes it exclusively, even to read.
func isLockHeld(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "lock held by current process") || strings.Contains(msg, "resource temporarily unavailable")
}

// lockHeld is the sentence for a directory another open holds.
func lockHeld(dir string) error {
	return fmt.Errorf("another toposhift process has %s open (a replay or a query); try again when it finishes", dir)
}

// openPebble opens the store in dir, read-only or not, with a plain sentence for
// the lock another open holds.
func openPebble(dir string, readOnly bool) (store.Store, error) {
	o := storeOptions()
	o.ReadOnly = readOnly
	st, err := pebblestore.Open(dir, o)
	switch {
	case err == nil:
		return st, nil
	case isLockHeld(err):
		return nil, lockHeld(dir)
	}
	return nil, fmt.Errorf("cannot open the store in %s: %w", dir, err)
}

// peekLastSeq opens the Pebble database in dir read-only, as a query does, and
// returns its last sequence number. It is how a replay learns where a store
// stands, and whether a directory that holds some Pebble database holds a
// toposhift store, before it opens the store for writing: pebblestore takes a
// Pebble database with no data keys of its own for a new one and would write
// its meta keys into it, and a read-only open of that cannot.
func peekLastSeq(dir string) (uint64, error) {
	o := storeOptions()
	o.ReadOnly = true
	st, err := pebblestore.Open(dir, o)
	switch {
	case isLockHeld(err):
		return 0, lockHeld(dir)
	case err != nil:
		return 0, fmt.Errorf("%s holds a Pebble database that is not a usable toposhift store, and it was left as it was: %w", dir, err)
	}
	last := st.LastSeq()
	return last, st.Close()
}

// openReplayStore opens the store a replay writes to. "pebble" is the durable
// layout in dir. "mem" is the reference store in memory: it holds every record
// in memory and keeps nothing, so it takes no directory.
//
// It is a variable so that a test can wrap the store.
var openReplayStore = func(kind, dir string) (store.Store, error) {
	switch kind {
	case "pebble":
		return openPebble(dir, false)
	case "mem":
		return memstore.Open(memstore.Options{Policy: storeOptions().Policy})
	}
	return nil, usagef("unknown store %q; the stores are pebble and mem", kind)
}

// openQueryStore opens the store in dir for reading only: it writes nothing,
// and a directory that holds no store is refused. The directory's lock is still
// exclusive, so a replay or another query running against it is reported.
func openQueryStore(dir string) (store.Store, error) {
	info, err := os.Stat(dir)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return nil, fmt.Errorf("there is no store in %s; replay an activity file into it first", dir)
	case err != nil:
		return nil, fmt.Errorf("cannot read the data directory %s: %w", dir, err)
	case !info.IsDir():
		return nil, fmt.Errorf("%s is not a directory", dir)
	}
	if holds, err := checkDataDir(dir); err != nil || !holds {
		return nil, fmt.Errorf("there is no store in %s; replay an activity file into it first", dir)
	}
	return openPebble(dir, true)
}
