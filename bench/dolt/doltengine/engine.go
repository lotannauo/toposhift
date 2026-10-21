package doltengine

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/lotannauo/toposhift/bench/dolt/doltmodel"
	"github.com/lotannauo/toposhift/bench/spike/engine"
)

// ErrSchema is wrapped by the error Open returns when the database it was asked to open
// holds something else than an engine of this schema version and arm.
var ErrSchema = errors.New("doltengine: the database is not this engine's")

// ErrServer is wrapped by the error Open returns for a server that is not the registered
// one: another version of Dolt, or settings that differ from scripts/server.yaml.
var ErrServer = errors.New("doltengine: the server is not the registered one")

// dbName is a database name the engine accepts: lower case, so that no server's case
// rules can make two names one, and short enough for any server.
var dbName = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

// DatabaseName makes a database name from a directory, as the conformance test's factory
// does: the same directory always names the same database, so that an engine closed and
// opened again over it finds what it left, and two directories never name one.
func DatabaseName(dir string) string {
	sum := sha256.Sum256([]byte(dir))
	return "toposhift_" + hex.EncodeToString(sum[:10])
}

// Config says which server and which database an engine uses.
type Config struct {
	// DSN is a go-sql-driver DSN for the server, such as
	// "root@tcp(127.0.0.1:3306)/bench". Its database name is ignored: the engine's
	// database is Database.
	DSN string
	// Database is the database the tables live in, created if it is absent.
	Database string
	// Arm is the design: [doltmodel.ArmCommit] (the default), a commit per batch, or
	// [doltmodel.ArmRows], the control with no commits.
	Arm doltmodel.Arm
}

// Engine is an [engine.Engine] over one database of a Dolt server. See the package
// documentation. Reads run concurrently with each other and with Write, and Write and
// Retain are serialized by the caller, as the contract says, and by a lock here.
type Engine struct {
	arm  doltmodel.Arm
	name string
	// rdb and wdb are one connection each: the writer's and the reader's.
	rdb, wdb *sql.DB
	ctx      context.Context
	cancel   context.CancelFunc
	f        faults

	// wmu serializes Write and Retain, and guards w and fatal. A write is planned from w
	// and applied to it only once the batch is committed.
	wmu   sync.Mutex
	w     *writer
	fatal error

	// mu is held for reading by every read for the whole of the read, and for writing by a
	// batch or a retention for the short step in which its commits return, its
	// transaction ends and the engine's view of the database moves: so a read sees
	// the database as the last published batch left it, or the next, never between.
	mu      sync.RWMutex
	lastSeq uint64
	commits []Commit
	byToken map[uint64]int // token -> index in commits of the newest commit at it

	cmu    sync.Mutex
	counts doltmodel.Counts

	reads readStats
}

var _ engine.Engine = (*Engine)(nil)

// Check validates a configuration without touching the network and returns the arm and the
// driver's configuration to connect with.
func (c Config) Check() (doltmodel.Arm, *mysql.Config, error) {
	arm := c.Arm
	if arm == 0 {
		arm = doltmodel.ArmCommit
	}
	if arm != doltmodel.ArmCommit && arm != doltmodel.ArmRows {
		return 0, nil, fmt.Errorf("doltengine: unknown arm %v: %w", arm, engine.ErrInvalid)
	}
	if !dbName.MatchString(c.Database) {
		return 0, nil, fmt.Errorf("doltengine: %q is not a usable database name: %w", c.Database, engine.ErrInvalid)
	}
	if c.DSN == "" {
		return 0, nil, errors.New("doltengine: no DSN")
	}
	dc, err := mysql.ParseDSN(c.DSN)
	if err != nil {
		return 0, nil, fmt.Errorf("doltengine: DSN: %w", err)
	}
	// The driver's modes are fixed: the registered statements are each one statement, with
	// their arguments sent to the server, and a time is never converted.
	dc.InterpolateParams = InterpolateParams
	dc.MultiStatements = false
	dc.ParseTime = false
	if dc.Timeout == 0 {
		dc.Timeout = 30 * time.Second
	}
	return arm, dc, nil
}

// InterpolateParams is whether the driver writes a statement's arguments into its text
// (true) or sends them to the server as a prepared statement (false). It is false: Dolt
// accepts prepared statements for every statement the engine sends (the server tests prove
// it), and the registered reads are the ones a user of Dolt would send.
const InterpolateParams = false

// connect opens a pool of one connection to the database (or to none, with an empty name).
func connect(dc *mysql.Config, database string) (*sql.DB, error) {
	c := dc.Clone()
	c.DBName = database
	conn, err := mysql.NewConnector(c)
	if err != nil {
		return nil, fmt.Errorf("doltengine: %w", err)
	}
	db := sql.OpenDB(conn)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)
	return db, nil
}

// Open connects to the server, creates the database if it is new, checks the server, and
// opens the tables, creating them if the database is new. A database that an engine of
// the same schema version and arm left is opened as it is, with its token, its horizon
// and its commits: what the writer remembers is rebuilt from the tables and the commit log.
func Open(ctx context.Context, cfg Config) (*Engine, error) {
	arm, dc, err := cfg.Check()
	if err != nil {
		return nil, err
	}
	admin, err := connect(dc, "")
	if err != nil {
		return nil, err
	}
	defer func() { _ = admin.Close() }()
	if err := CheckServer(ctx, admin); err != nil {
		return nil, err
	}
	// The name has matched dbName: it needs no quoting beyond the backticks.
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS `"+cfg.Database+"`"); err != nil {
		return nil, fmt.Errorf("doltengine: creating database %s: %w", cfg.Database, err)
	}
	wdb, err := connect(dc, cfg.Database)
	if err != nil {
		return nil, err
	}
	rdb, err := connect(dc, cfg.Database)
	if err != nil {
		_ = wdb.Close()
		return nil, err
	}
	e := newEngine(arm, cfg.Database, wdb, rdb)
	if err := e.setup(ctx); err != nil {
		_ = e.Close()
		return nil, err
	}
	return e, nil
}

func newEngine(arm doltmodel.Arm, name string, wdb, rdb *sql.DB) *Engine {
	e := &Engine{arm: arm, name: name, wdb: wdb, rdb: rdb, byToken: make(map[uint64]int)}
	e.ctx, e.cancel = context.WithCancel(context.Background())
	e.w = newWriter(&e.f)
	return e
}

// Close releases the connections. The database and what it holds stay, for Open.
func (e *Engine) Close() error {
	e.cancel()
	err := errors.Join(e.wdb.Close(), e.rdb.Close())
	return err
}

// Database is the database the engine's tables are in.
func (e *Engine) Database() string { return e.name }

// Arm is the design the engine runs.
func (e *Engine) Arm() doltmodel.Arm { return e.arm }

// Counts returns what the engine has written, in the units of [doltmodel.Counts]: rows
// inserted into the logs, rows replaced in the state tables, rows retention deleted,
// commits, and records refused for a payload over the column. They are the rows the engine
// sent, so for one stream they equal the counts of a [doltmodel.Model] given the same
// stream.
func (e *Engine) Counts() doltmodel.Counts {
	e.cmu.Lock()
	defer e.cmu.Unlock()
	return e.counts
}

func (e *Engine) addCounts(c doltmodel.Counts) {
	e.cmu.Lock()
	defer e.cmu.Unlock()
	e.counts.Records += c.Records
	e.counts.LogRows += c.LogRows
	e.counts.StateWrites += c.StateWrites
	e.counts.LogDeletes += c.LogDeletes
	e.counts.StateDeletes += c.StateDeletes
	e.counts.Commits += c.Commits
	e.counts.PayloadRefusals += c.PayloadRefusals
}

// LastSeq implements [engine.Engine]. It rises only after the batch's transaction and its
// commit have both returned.
func (e *Engine) LastSeq() uint64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.lastSeq
}

// Commits returns a copy of the commits the engine has made, oldest first, retention
// commits among them. It is empty in the arm with no commits.
func (e *Engine) Commits() []Commit {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return append([]Commit(nil), e.commits...)
}

// Head is the newest commit, or false if there is none.
func (e *Engine) Head() (Commit, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if len(e.commits) == 0 {
		return Commit{}, false
	}
	return e.commits[len(e.commits)-1], true
}

// readStats counts the reads by kind and outcome, and the keys the rule settled each way,
// as the model does ([doltmodel.Model.Paths]), so that the two are held to each other.
type readStats struct {
	queries [numOps][numOutcomes]atomic.Uint64
	keys    [numOps][2]atomic.Uint64 // settled by the state row, answered from the log
}

const (
	opNeighbors = iota
	opBatch
	opAlive
	opWindow
	numOps
)

var opNames = [numOps]string{"neighbors", "batch", "alive", "window"}

const (
	outAsOfAlone = iota
	outNeededLog
	outNoCommit
	outWindowLog
	numOutcomes
)

var outcomeNames = [numOutcomes]string{"as-of-alone", "needed-log", "no-commit", "log"}

// Paths returns what the reads answered so far, by kind, as "kind/what" counts; only
// those that happened. The names and meanings are the model's:
//
//	kind/as-of-alone   reads answered from the state as of a commit alone, with no key
//	                   sent to the log
//	kind/needed-log    reads that needed the log for some key
//	kind/no-commit     reads whose token names no commit, answered from the log alone
//	kind/keys-state    state keys settled by their row
//	kind/keys-log      state keys answered from the log
//	window/log         windows
//
// All of it is a function of the reads asked, not of timing.
func (e *Engine) Paths() map[string]uint64 {
	out := make(map[string]uint64)
	for op := range numOps {
		for o := range numOutcomes {
			if n := e.reads.queries[op][o].Load(); n > 0 {
				out[opNames[op]+"/"+outcomeNames[o]] = n
			}
		}
		for i, name := range [2]string{"keys-state", "keys-log"} {
			if n := e.reads.keys[op][i].Load(); n > 0 {
				out[opNames[op]+"/"+name] = n
			}
		}
	}
	return out
}

// tally is what one read did: the keys settled by a row, the keys sent to the log because
// their row was after the instant, and whether the read had no commit to read.
type tally struct {
	state, log uint64
	noCommit   bool
}

func (e *Engine) countKeyed(op int, ty tally) {
	o := outAsOfAlone
	switch {
	case ty.noCommit:
		o = outNoCommit
	case ty.log > 0:
		o = outNeededLog
	}
	e.reads.queries[op][o].Add(1)
	e.reads.keys[op][0].Add(ty.state)
	e.reads.keys[op][1].Add(ty.log)
}

// Size implements [engine.Engine]: the bytes the server reports for the engine's tables,
// data and indexes, which is an estimate (the server does not say what its files hold). The
// bytes of the data directory, as left and after garbage collection, are the benchmark's
// own measurement, made where the server's files are.
func (e *Engine) Size() (int64, error) {
	tx, err := e.rdb.BeginTx(e.ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return 0, fmt.Errorf("doltengine: size: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var n sql.NullInt64
	if err := tx.QueryRowContext(e.ctx, "SELECT SUM(data_length + index_length) FROM information_schema.tables WHERE table_schema = ?", e.name).Scan(&n); err != nil {
		return 0, fmt.Errorf("doltengine: size: %w", err)
	}
	return n.Int64, nil
}

// DropDatabase removes the database and everything in it, which Close leaves alone, and purges
// it: the server keeps a dropped database in its data directory until it is purged, and the
// history a test left (a commit per batch) would otherwise fill the disk of a runner. The
// purge removes every database dropped so far, which none is wanted back.
func DropDatabase(ctx context.Context, dsn, database string) error {
	if !dbName.MatchString(database) {
		return fmt.Errorf("doltengine: %q is not a usable database name: %w", database, engine.ErrInvalid)
	}
	dc, err := mysql.ParseDSN(dsn)
	if err != nil {
		return fmt.Errorf("doltengine: DSN: %w", err)
	}
	dc.InterpolateParams = InterpolateParams
	db, err := connect(dc, "")
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx, "DROP DATABASE IF EXISTS `"+database+"`"); err != nil {
		return fmt.Errorf("doltengine: dropping database %s: %w", database, err)
	}
	if _, err := db.ExecContext(ctx, "CALL dolt_purge_dropped_databases()"); err != nil {
		return fmt.Errorf("doltengine: purging dropped databases: %w", err)
	}
	return nil
}
