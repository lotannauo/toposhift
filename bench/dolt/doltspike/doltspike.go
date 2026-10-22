// Package doltspike is the command that loads a stream into Dolt, one Dolt commit per batch,
// and records what the reads of a plan's questions are answered as, and what the history the
// commits keep costs in bytes: in counts and bytes, never in time. Its subcommands:
//
//	env        what the binary and, with a server, the server are
//	questions  choose and answer the questions of an activity file, before any load
//	load       load a plan's stream, or an activity file's, into a database of an arm
//	read       ask every question of a load, check each answer, and count how each was answered
//	explain    the server's plans for the statements of the reads of a load
//	sizes      the bytes of a loaded database, as left and after garbage collection
//	diff       compare two records of one kind
//	sizing     load the stream and say only whether it fits the runner (on CI only)
//
// The arms are commit (the registered design: the tables, and a Dolt commit per batch that
// versions the state table), rows (the control: the same tables and no commits) and, for read,
// commit-time (the diagnostic: the commit database read as if a commit's place in history
// were an event time; its wrong answers are its result and it is never a comparator).
//
// Every command that reaches a server refuses one that is not the registered Dolt, whose
// settings differ from scripts/server.yaml, or whose image was not named as the registered
// one; a plan or question set that is not named by its SHA-256; and an output directory
// inside a git work tree. A record is never overwritten. Nothing a command prints or writes
// holds a time.
package doltspike

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"runtime/debug"

	"github.com/lotannauo/toposhift/bench/dolt/doltengine"
	"github.com/lotannauo/toposhift/bench/dolt/doltmodel"
	"github.com/lotannauo/toposhift/bench/dolt/scripts"
)

const usage = `usage: doltspike env|questions|load|read|explain|sizes|diff|sizing [flags]
run a subcommand with -h for its flags`

// Run runs the command line args (without the program name), writing what it found to stdout
// and its progress to stderr.
func Run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return errors.New(usage)
	}
	rest := args[1:]
	switch args[0] {
	case "env":
		return runEnv(ctx, rest, stdout, stderr)
	case "questions":
		return runQuestions(ctx, rest, stdout, stderr)
	case "load":
		return runLoad(ctx, rest, stdout, stderr)
	case "read":
		return runRead(ctx, rest, stdout, stderr)
	case "explain":
		return runExplain(ctx, rest, stdout, stderr)
	case "sizes":
		return runSizes(ctx, rest, stdout, stderr)
	case "diff":
		return runDiff(rest, stdout, stderr)
	case "sizing":
		return runSizing(ctx, rest, stdout, stderr)
	}
	return fmt.Errorf("doltspike: no command %q\n%s", args[0], usage)
}

func newFlags(name string, stderr io.Writer) *flag.FlagSet {
	fs := flag.NewFlagSet("doltspike "+name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	return fs
}

func progress(w io.Writer) func(format string, args ...any) {
	return func(format string, args ...any) { fmt.Fprintf(w, format+"\n", args...) }
}

// common is the flags of the commands that reach a server.
type common struct {
	dsn, image, db, out string
}

func (c *common) register(fs *flag.FlagSet, needDB bool) {
	fs.StringVar(&c.dsn, "dsn", os.Getenv("TOPOSHIFT_DOLT_DSN"), "the server, as a go-sql-driver DSN (default $TOPOSHIFT_DOLT_DSN)")
	fs.StringVar(&c.image, "image", os.Getenv("TOPOSHIFT_DOLT_IMAGE"), "the image the server was started from, as scripts/dolt-server.sh image prints it (default $TOPOSHIFT_DOLT_IMAGE)")
	fs.StringVar(&c.out, "out", "", "the directory the record is written to; never inside a git work tree")
	if needDB {
		fs.StringVar(&c.db, "db", "", "the database of the arm")
	}
}

// validate is everything about the flags that is refused before a server is reached.
func (c *common) validate(needOut, needDB bool) error {
	switch {
	case c.dsn == "":
		return errors.New("doltspike: -dsn is required (or $TOPOSHIFT_DOLT_DSN)")
	case needOut && c.out == "":
		return errors.New("doltspike: -out is required")
	case needDB && c.db == "":
		return errors.New("doltspike: -db is required")
	}
	if err := CheckImage(c.image); err != nil {
		return err
	}
	if c.out != "" {
		return RefuseInWorkTree(c.out)
	}
	return nil
}

// serverEnv checks the server (its version and its observable settings) and returns the
// environment of a run against it.
func serverEnv(ctx context.Context, dsn, image string) (Env, error) {
	e := buildEnv(image)
	v, err := doltengine.ServerVersion(ctx, dsn)
	if err != nil {
		return e, err
	}
	e.DoltVersion = v
	return e, nil
}

// buildEnv is the environment of a run apart from the server's own version: the binary's
// build and file, and the files that make the server.
func buildEnv(image string) Env {
	e := Env{
		Commit: "unknown", Modified: "unknown", GoVersion: runtime.Version(), GOOS: runtime.GOOS, GOARCH: runtime.GOARCH,
		BinarySHA256: binarySHA(), Image: image,
		ServerYAMLSHA256: scripts.SHA256(scripts.ServerYAML), GlobalJSONSHA256: scripts.SHA256(scripts.GlobalJSON), ServerScriptSHA256: scripts.SHA256(scripts.ServerScript),
		DriverVersion: "unknown", InterpolateParams: doltengine.InterpolateParams,
		SchemaVersion: doltmodel.SchemaVersion, PayloadCapacity: doltmodel.RegisteredPayloadCapacity,
	}
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, s := range bi.Settings {
			switch s.Key {
			case "vcs.revision":
				e.Commit = s.Value
			case "vcs.modified":
				e.Modified = s.Value
			}
		}
		for _, d := range bi.Deps {
			if d.Path == "github.com/go-sql-driver/mysql" {
				e.DriverVersion = d.Version
			}
		}
	}
	return e
}

func binarySHA() string {
	path, err := os.Executable()
	if err != nil {
		return "unknown"
	}
	f, err := os.Open(path)
	if err != nil {
		return "unknown"
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "unknown"
	}
	return hex.EncodeToString(h.Sum(nil))
}

func runEnv(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := newFlags("env", stderr)
	var c common
	c.register(fs, false)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("usage: doltspike env [-dsn dsn -image image] [-out dir]")
	}
	e := buildEnv(c.image)
	var checked bool
	if c.dsn != "" {
		if err := c.validate(false, false); err != nil {
			return err
		}
		var err error
		if e, err = serverEnv(ctx, c.dsn, c.image); err != nil {
			return err
		}
		checked = true
	} else if c.out != "" {
		if err := RefuseInWorkTree(c.out); err != nil {
			return err
		}
	}
	if c.out != "" {
		if err := writeNew(c.out, "env.json", e); err != nil {
			return err
		}
	}
	image, err := scripts.Image()
	if err != nil {
		return err
	}
	version, err := scripts.Version()
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "registered image: %s\nregistered Dolt: %s\n", image, version)
	if checked {
		fmt.Fprintf(stdout, "server: Dolt %s, the registered image was named, and every observable setting equals scripts/server.yaml's\n", e.DoltVersion)
	} else {
		fmt.Fprintln(stdout, "server: not reached (-dsn names one)")
	}
	fmt.Fprintf(stdout, "settings sha256 %s, global configuration sha256 %s, script sha256 %s\n", e.ServerYAMLSHA256, e.GlobalJSONSHA256, e.ServerScriptSHA256)
	fmt.Fprintf(stdout, "driver %s, prepared statements %v (interpolateParams false), schema version %d\n", e.DriverVersion, !e.InterpolateParams, e.SchemaVersion)
	fmt.Fprintf(stdout, "payload capacity %d bytes (registered)\n", e.PayloadCapacity)
	fmt.Fprintf(stdout, "binary: commit %s (modified %s), sha256 %s, %s %s/%s\n", e.Commit, e.Modified, e.BinarySHA256, e.GoVersion, e.GOOS, e.GOARCH)
	return nil
}
