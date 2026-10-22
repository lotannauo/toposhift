// Package pgspike is the command that loads a stream into Postgres and records what the
// reads of a plan's questions cost there, in counts, bytes and the log written, never in
// time. Its subcommands:
//
//	env        check the server against settings.conf and record what it is
//	questions  choose and answer the questions of an activity file, before any load
//	load       load a plan's stream or an activity file into the tables of a bulk arm
//	stream     write a plan's stream to a streaming arm, batch by batch with its retentions,
//	           and record what the writer did and what it cost in log
//	read       ask every question of the load, check each answer, and explain each cold
//	explain    print the cold plan of the statements of one question
//	sizes      record the bytes of the tables, as left and compacted
//	diff       compare two records of one kind
//
// Every command that touches a server refuses one whose settings differ from
// settings.conf; a plan or question set that is not named by its SHA-256; and an output
// directory inside a git work tree. The records name the stream, the build of the
// binary, the server image and the settings, so that two can be compared, and a
// comparison refuses two that are not of one stream on one server. Nothing it writes holds
// a time.
package pgspike

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"

	"github.com/jackc/pgx/v5"

	"github.com/lotannauo/toposhift/bench/postgres/pgengine"
	"github.com/lotannauo/toposhift/bench/postgres/pgreport"
)

const usage = `usage: pgspike env|questions|load|stream|read|explain|sizes|diff [flags]
run a subcommand with -h for its flags`

// Run runs the command line args (without the program name), writing what it found to
// stdout and its progress to stderr.
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
	case "stream":
		return runStream(ctx, rest, stdout, stderr)
	case "read":
		return runRead(ctx, rest, stdout, stderr)
	case "explain":
		return runExplain(ctx, rest, stdout, stderr)
	case "sizes":
		return runSizes(ctx, rest, stdout, stderr)
	case "diff":
		return runDiff(rest, stdout, stderr)
	}
	return fmt.Errorf("pgspike: no command %q\n%s", args[0], usage)
}

// common is the flags of the commands that touch a server.
type common struct {
	dsn, settings, out, arm, schema string
}

func (c *common) register(fs *flag.FlagSet, needSchema bool) {
	fs.StringVar(&c.dsn, "dsn", os.Getenv("TOPOSHIFT_PG_DSN"), "the server, as a pgx connection string (default $TOPOSHIFT_PG_DSN)")
	fs.StringVar(&c.settings, "settings", "", "the settings.conf the server was started with (required): the run is refused if the server differs")
	fs.StringVar(&c.out, "out", "", "the directory the record is written to; never inside a git work tree")
	if needSchema {
		fs.StringVar(&c.arm, "arm", "", "the arm: pg-bulk, pg-bulk-b, pg-bulk-id, pg-stream, pg-stream-nopart or pg-stream-naive")
		fs.StringVar(&c.schema, "schema", "", "the schema the arm's tables are in")
	}
}

// session is a connection that has passed the guards, and the environment it found.
type session struct {
	conn *pgx.Conn
	env  pgreport.Env
}

func (s *session) close(ctx context.Context) { _ = s.conn.Close(ctx) }

// begin checks what can be checked before a server is reached (the output directory),
// then connects, refuses a server whose settings differ, and reads what the server is.
func (c *common) begin(ctx context.Context, needOut bool) (*session, error) {
	if err := c.validate(needOut); err != nil {
		return nil, err
	}
	return connect(ctx, c.dsn, c.settings)
}

// validate is everything about the flags that is refused before a server is reached:
// the settings and the server named, and an output directory that is not inside a git
// work tree.
func (c *common) validate(needOut bool) error {
	switch {
	case c.settings == "":
		return errors.New("pgspike: -settings is required")
	case c.dsn == "":
		return errors.New("pgspike: -dsn is required (or $TOPOSHIFT_PG_DSN)")
	case needOut && c.out == "":
		return errors.New("pgspike: -out is required")
	}
	if c.out != "" {
		return RefuseInWorkTree(c.out)
	}
	return nil
}

// connect opens a connection that has passed the settings guard.
func connect(ctx context.Context, dsn, settings string) (*session, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("pgspike: %w", err)
	}
	s := &session{conn: conn}
	sum, err := RequireSettings(ctx, conn, settings)
	if err != nil {
		s.close(ctx)
		return nil, err
	}
	image, err := pgengine.PinnedImage(filepath.Join(filepath.Dir(settings), "pg-server.sh"))
	if err != nil {
		s.close(ctx)
		return nil, err
	}
	info, err := pgengine.ReadServerInfo(ctx, conn)
	if err != nil {
		s.close(ctx)
		return nil, err
	}
	s.env = buildEnv(sum, image, info)
	return s, nil
}

// buildEnv is the environment of a run: the binary's build and file, and the server.
func buildEnv(settingsSHA, image string, info pgengine.ServerInfo) pgreport.Env {
	e := pgreport.Env{
		Commit: "unknown", Modified: "unknown", GoVersion: runtime.Version(), GOOS: runtime.GOOS, GOARCH: runtime.GOARCH,
		BinarySHA256: binarySHA(), SettingsSHA256: settingsSHA, Image: image,
		ServerVersion: info.Version, Configure: info.Configure, BlockSize: info.BlockSize, WALBlockSize: info.WALBlockSize,
		DataChecksums: info.DataChecksums, ServerCPU: info.CPU,
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

// writeNew writes a record under dir, refusing to replace one that is there: a record
// that was overwritten by a later run is a number that cannot be traced.
func writeNew(dir, name string, v any) error {
	if err := checkFree(dir, name); err != nil {
		return err
	}
	return pgreport.Write(dir, name, v)
}

// checkFree refuses, before any work is done, an output that would replace a record.
func checkFree(dir, name string) error {
	if _, err := os.Lstat(filepath.Join(dir, name)); err == nil {
		return fmt.Errorf("pgspike: %s already exists in %s (a record is never overwritten)", name, dir)
	}
	return nil
}

func newFlags(name string, stderr io.Writer) *flag.FlagSet {
	fs := flag.NewFlagSet("pgspike "+name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	return fs
}

func progress(w io.Writer) func(format string, args ...any) {
	return func(format string, args ...any) { fmt.Fprintf(w, format+"\n", args...) }
}

func runEnv(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := newFlags("env", stderr)
	var c common
	c.register(fs, false)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("usage: pgspike env -settings settings.conf [-dsn dsn] [-out dir]")
	}
	s, err := c.begin(ctx, false)
	if err != nil {
		return err
	}
	defer s.close(ctx)
	if c.out != "" {
		if err := writeNew(c.out, "env.json", s.env); err != nil {
			return err
		}
	}
	e := s.env
	fmt.Fprintf(stdout, "settings: the server has every setting of settings.conf (sha256 %s)\n", e.SettingsSHA256)
	fmt.Fprintf(stdout, "image: %s\nserver: %s\nbuild options: %s\n", e.Image, e.ServerVersion, e.Configure)
	fmt.Fprintf(stdout, "block size %d, WAL block size %d, data checksums %s\nserver processor: %s\n", e.BlockSize, e.WALBlockSize, e.DataChecksums, e.ServerCPU)
	fmt.Fprintf(stdout, "binary: commit %s (modified %s), sha256 %s, %s %s/%s\n", e.Commit, e.Modified, e.BinarySHA256, e.GoVersion, e.GOOS, e.GOARCH)
	return nil
}
