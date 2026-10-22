// Package xtspike is the command that loads a stream into an XTDB node and records what
// the node answered, held, and was asked to hold, in counts, bytes and digests, and never in
// time. Its subcommands:
//
//	env        record what the node and the build are, and whether the node is the registered one
//	questions  choose and answer the questions of an activity file, before any load
//	load       write a plan's stream or an activity file into the node, and let it settle
//	read       ask every question of the plan of a loaded node, and check each answer (G0)
//	sizes      record the bytes of the node's log and storage directories, after it settles
//	sizing     on CI only: whether the stream can be loaded within the registered limits
//
// Every command that reaches the node refuses a plan or question set that is not named by
// its SHA-256, and an output directory inside a git work tree; a record is never
// overwritten. With -registered, a node that is not the registered one is refused: the
// image, the JVM options and the configuration file the container runs are those of
// scripts/xt-server.sh and scripts/xtdb.yaml, and neither override of the script is set.
// Without it, a record says that it is not registered and why.
//
// A node is sized only after its last block has been finished (POST /system/finish-block, from
// the load), because what is in an unfinished block is in the log directory and not in the
// storage directory, and the tail varies with where the stream ended.
//
// What is written holds no time: the write timing of a later job includes, in each batch's
// commit, the read-back of its meta document, and this package takes no timing but the one
// sizing decides on, which is never printed.
package xtspike

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
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/xtdb/xtengine"
	"github.com/lotannauo/toposhift/bench/xtdb/xtmodel"
	"github.com/lotannauo/toposhift/bench/xtdb/xtnode"
	"github.com/lotannauo/toposhift/bench/xtdb/xtreport"
)

const usage = `usage: xtspike env|questions|load|read|sizes|sizing [flags]
run a subcommand with -h for its flags`

// WriteLabel is how every write timing taken of the node must be labelled: a batch's commit
// is followed, before the write returns, by a point read of the batch's meta document at its
// own system time, so the time of a write includes that read, and so does the time of a
// retention's transaction.
const WriteLabel = "commit and read-back of the meta document"

// Node is what the commands ask of an engine beyond the engine contract: the planner's
// counts, the transactions it committed, the batches it refused for a boundary off a whole
// microsecond, and how its reads were answered.
type Node interface {
	engine.Engine
	Counts() xtmodel.Counts
	Transactions() uint64
	// RefusedOffMicrosecond are the records in batches refused because a boundary is not a
	// whole microsecond.
	RefusedOffMicrosecond() uint64
	// Reads are the entity reads answered at a system time or the newest, and the reads
	// answered by folding the log in Go.
	Reads() (native, clientFold uint64)
}

// liveNode is a Node over the XTDB engine.
type liveNode struct{ *xtengine.Engine }

func (n liveNode) RefusedOffMicrosecond() uint64 { return n.Engine.Refused().OffMicrosecond }

func (n liveNode) Reads() (uint64, uint64) {
	rc := n.ReadCounts()
	return rc.Native, rc.ClientFold
}

// App is what the commands run on. Run uses the real one; the tests replace the parts that
// reach a node, a clock or the environment.
type App struct {
	Stdout, Stderr io.Writer
	// Getenv looks up an environment variable (os.LookupEnv).
	Getenv func(string) (string, bool)
	// Open opens an engine over the node.
	Open func(ctx context.Context, cfg xtengine.Config) (Node, error)
	// Admin reads the health port at a base address, and can ask the node to finish its block.
	Admin func(base string) xtnode.Admin
	// Docker runs the docker command.
	Docker xtnode.Docker
	// Now is the clock sizing measures a load by.
	Now func() time.Time
	// Free is the free space of the file system that holds a path (xtnode.FreeBytes).
	Free func(path string) (uint64, error)
	// Clock, if set, gives the settle protocol a clock of its own.
	Clock func(*xtnode.Config)
}

// Run runs the command line args (without the program name), writing what it found to stdout
// and its progress to stderr.
func Run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	return (&App{Stdout: stdout, Stderr: stderr}).Run(ctx, args)
}

func (a *App) defaults() {
	if a.Stdout == nil {
		a.Stdout = io.Discard
	}
	if a.Stderr == nil {
		a.Stderr = io.Discard
	}
	if a.Getenv == nil {
		a.Getenv = os.LookupEnv
	}
	if a.Open == nil {
		a.Open = func(ctx context.Context, cfg xtengine.Config) (Node, error) {
			e, err := xtengine.Open(ctx, cfg)
			if err != nil {
				return nil, err
			}
			return liveNode{e}, nil
		}
	}
	if a.Admin == nil {
		a.Admin = func(base string) xtnode.Admin { return xtnode.HTTPProber{Base: base} }
	}
	if a.Docker == nil {
		a.Docker = xtnode.RunDocker
	}
	if a.Now == nil {
		a.Now = time.Now
	}
	if a.Free == nil {
		a.Free = xtnode.FreeBytes
	}
}

// Run is [Run] on an app.
func (a *App) Run(ctx context.Context, args []string) error {
	a.defaults()
	if len(args) == 0 {
		return errors.New(usage)
	}
	rest := args[1:]
	switch args[0] {
	case "env":
		return a.runEnv(ctx, rest)
	case "questions":
		return a.runQuestions(rest)
	case "load":
		return a.runLoad(ctx, rest)
	case "read":
		return a.runRead(ctx, rest)
	case "sizes":
		return a.runSizes(ctx, rest)
	case "sizing":
		return a.runSizing(ctx, rest)
	}
	return fmt.Errorf("xtspike: no command %q\n%s", args[0], usage)
}

func (a *App) say(format string, args ...any) { fmt.Fprintf(a.Stderr, format+"\n", args...) }

func (a *App) out(format string, args ...any) { fmt.Fprintf(a.Stdout, format+"\n", args...) }

// common is the flags of the commands that reach a node.
type common struct {
	dsn, health, prefix, out, script, container string
	registered                                  bool
	quiet, max, poll                            time.Duration
}

func (a *App) register(c *common, fs *flag.FlagSet) {
	env := func(name, def string) string {
		if v, ok := a.Getenv(name); ok && v != "" {
			return v
		}
		return def
	}
	fs.StringVar(&c.dsn, "dsn", env("TOPOSHIFT_XTDB_DSN", ""), "the node's Postgres wire address, as a pgx connection string (default $TOPOSHIFT_XTDB_DSN)")
	fs.StringVar(&c.health, "health", env("TOPOSHIFT_XTDB_HEALTH", "http://127.0.0.1:55480"), "the node's health port (default $TOPOSHIFT_XTDB_HEALTH)")
	fs.StringVar(&c.prefix, "prefix", "", "the prefix of the tables of the load (default none: the baseline's own tables)")
	fs.StringVar(&c.out, "out", "", "the directory the record is written to; never inside a git work tree")
	fs.StringVar(&c.script, "script", "", "scripts/xt-server.sh of this baseline, which names the registered image and options (required)")
	fs.StringVar(&c.container, "container", "", "the name of the node's container, to check it against the registered configuration")
	fs.BoolVar(&c.registered, "registered", false, "refuse a node that is not the registered one (needs -container)")
	fs.DurationVar(&c.quiet, "settle-quiet", xtnode.RegisteredQuiet, "the window the node must be quiet for; a registered run refuses any other")
	fs.DurationVar(&c.max, "settle-max", xtnode.RegisteredMax, "the longest wait for the node to settle; a registered run refuses any other")
	fs.DurationVar(&c.poll, "settle-poll", 5*time.Second, "the time between polls of the node")
}

func (a *App) settleConfig(c *common) xtnode.Config {
	cfg := xtnode.Config{Quiet: c.quiet, Max: c.max, Poll: c.poll}
	if a.Clock != nil {
		a.Clock(&cfg)
	}
	return cfg
}

// validate is everything about the flags that is refused before a node is reached.
func (c *common) validate(needOut, needDSN bool) error {
	switch {
	case c.script == "":
		return errors.New("xtspike: -script is required")
	case needDSN && c.dsn == "":
		return errors.New("xtspike: -dsn is required (or $TOPOSHIFT_XTDB_DSN)")
	case needOut && c.out == "":
		return errors.New("xtspike: -out is required")
	}
	if c.out != "" {
		return RefuseInWorkTree(c.out)
	}
	return nil
}

// buildEnv is the environment of a run: the binary's build and file, the machine, the
// registered pins, the node's container if it is named, and whether the node is the
// registered one. With -registered it refuses a node that is not.
func (a *App) buildEnv(ctx context.Context, c *common) (xtreport.Env, error) {
	pin, err := xtnode.ReadPinned(c.script)
	if err != nil {
		return xtreport.Env{}, err
	}
	e := xtreport.Env{
		Commit: "unknown", Modified: "unknown", GoVersion: runtime.Version(), GOOS: runtime.GOOS, GOARCH: runtime.GOARCH,
		BinarySHA256: binarySHA(), CPUModel: xtnode.CPUModel(), NumCPU: runtime.NumCPU(), QueryMode: string(xtengine.DefaultMode),
		Image: pin.Image, JavaOptions: pin.JavaOptions, ConfigSHA256: pin.ConfigSHA256,
		Overrides: xtnode.Overrides(a.Getenv),
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
	var cont *xtnode.Container
	if c.container != "" {
		cc, err := xtnode.Inspect(ctx, a.Docker, c.container)
		if err != nil {
			return xtreport.Env{}, err
		}
		cont = &cc
		e.Container = &xtreport.Container{
			Name: cc.Name, ImageRef: cc.ImageRef, ImageID: cc.ImageID, JavaOptions: cc.JavaOptions,
			XTDBVersion: cc.XTDBVersion, GitSHA: cc.GitSHA, ConfigSHA256: cc.ConfigSHA256, Running: cc.Running,
		}
	}
	sc := a.settleConfig(c)
	e.Settle = xtreport.Settle{QuietSeconds: int(c.quiet / time.Second), MaxSeconds: int(c.max / time.Second), Registered: sc.IsRegistered()}
	e.NotRegistered = xtnode.NotRegistered(pin, cont, e.Overrides)
	if !e.Settle.Registered {
		e.NotRegistered = append(e.NotRegistered, fmt.Sprintf("the settle protocol is not the registered one (a window of %d seconds in at most %d)", int(xtnode.RegisteredQuiet/time.Second), int(xtnode.RegisteredMax/time.Second)))
	}
	e.Registered = len(e.NotRegistered) == 0
	if c.registered && !e.Registered {
		return xtreport.Env{}, fmt.Errorf("xtspike: -registered was given and the node is not the registered one:\n  %s", joinLines(e.NotRegistered))
	}
	return e, nil
}

func joinLines(ss []string) string {
	out := ""
	for i, s := range ss {
		if i > 0 {
			out += "\n  "
		}
		out += s
	}
	return out
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

// writeNew writes a record under dir, refusing to replace one that is there.
func writeNew(dir, name string, v any) error {
	if err := checkFree(dir, name); err != nil {
		return err
	}
	return xtreport.Write(dir, name, v)
}

// checkFree refuses, before any work is done, an output that would replace a record.
func checkFree(dir, name string) error {
	if _, err := os.Lstat(filepath.Join(dir, name)); err == nil {
		return fmt.Errorf("xtspike: %s already exists in %s (a record is never overwritten)", name, dir)
	}
	return nil
}

func newFlags(name string, stderr io.Writer) *flag.FlagSet {
	fs := flag.NewFlagSet("xtspike "+name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	return fs
}

// openFresh opens an engine over the node under the prefix and refuses one that already holds
// a stream: a load into tables that have one would be a load of something else.
func (a *App) openFresh(ctx context.Context, cfg xtengine.Config) (Node, error) {
	n, err := a.Open(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if n.LastSeq() != 0 || n.Transactions() != 0 {
		_ = n.Close()
		return nil, fmt.Errorf("xtspike: the node already holds a stream under the prefix %q (last seq %d, %d transactions): a load needs a node of its own, started afresh", cfg.Prefix, n.LastSeq(), n.Transactions())
	}
	return n, nil
}

// settle waits for the node to go quiet.
func (a *App) settle(ctx context.Context, c *common) (xtnode.Result, error) {
	a.say("settle: waiting for the node to be quiet for %d seconds (at most %d)", int(c.quiet/time.Second), int(c.max/time.Second))
	return xtnode.Settle(ctx, a.Admin(c.health), a.settleConfig(c))
}

// finishAndSettle is what follows the last write: the node is asked to finish the block it is
// filling, which puts the tail of the stream in its storage directory, and then waited for.
func (a *App) finishAndSettle(ctx context.Context, c *common) (xtnode.Result, error) {
	a.say("settle: asking the node to finish its block, then waiting for it to be quiet for %d seconds (at most %d)", int(c.quiet/time.Second), int(c.max/time.Second))
	return xtnode.FinishAndSettle(ctx, a.Admin(c.health), a.settleConfig(c))
}

// writerOf is the record of what the planner wrote.
func writerOf(c xtmodel.Counts, transactions uint64) xtreport.Writer {
	w := xtreport.Writer{
		Batches: c.Batches, Records: c.Records, Transactions: transactions, Retentions: transactions - min(transactions, c.Batches),
		InOrder: c.InOrder, Late: c.Late, MergedAway: c.MergedAway, Statements: c.Statements(),
	}
	for t := range xtmodel.NumTables {
		tc := c.Tables[t]
		w.Tables = append(w.Tables, xtreport.Table{Name: xtmodel.Table(t).String(), Inserts: tc.Inserts, PortionDeletes: tc.PortionDeletes})
		w.Documents += tc.Inserts
	}
	if c.Records > 0 {
		w.StatementsPerRecord = float64(w.Statements) / float64(c.Records)
		w.DocumentsPerRecord = float64(w.Documents) / float64(c.Records)
	}
	return w
}

// DefaultBase is the system time of the zeroth transaction a run registers, which is the
// engine's own default.
var DefaultBase = xtmodel.BaseSystemTime

// parseBase reads the -base flag: empty is the registered base (the zero time, which the
// engine takes as its default), anything else is an RFC 3339 instant on a whole microsecond,
// for a test that shares a node with others (a node refuses a transaction at a system time
// before its latest, so engines that share one open with later and later bases). A registered
// run does not take one.
func parseBase(s string, registered bool) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	if registered {
		return time.Time{}, errors.New("xtspike: -registered was given with -base, which is not the registered system-time base")
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("xtspike: -base: %w", err)
	}
	return t.UTC(), nil
}

// baseName is how a record names the system time base of a load.
func baseName(t time.Time) string {
	if t.IsZero() {
		return time.Unix(0, int64(DefaultBase)).UTC().Format(time.RFC3339Nano)
	}
	return t.UTC().Format(time.RFC3339Nano)
}
