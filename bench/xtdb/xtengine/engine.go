package xtengine

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/xtdb/xtmodel"
	"github.com/lotannauo/toposhift/internal/catalog"
)

// ErrNoDataDirs is returned by Size when the engine was not told where the node keeps
// its files.
var ErrNoDataDirs = errors.New("xtengine: no data directories to measure")

// QueryMode is how the driver sends a statement.
type QueryMode string

const (
	// ModeCache prepares each statement once per connection and describes it (the
	// driver's default). XTDB 2.1.0 refuses a write sent this way: it describes a
	// statement's parameters as none.
	ModeCache QueryMode = "cache"
	// ModeExec sends the statement and its arguments in one round trip, with no describe
	// and no cache. XTDB 2.1.0 refuses a write sent this way: it needs the type of each
	// argument of a write in the parse message, which the driver does not give in this mode.
	ModeExec QueryMode = "exec"
	// ModeSimple sends the statement as text, with its arguments written into it by the
	// driver as quoted strings. It is the only mode XTDB 2.1.0 accepts for the statements of
	// the engine.
	ModeSimple QueryMode = "simple"
)

// DefaultMode is the mode the baseline registers: the first of the driver's default, its exec
// mode and the simple protocol, in that order, that the node accepts (see the package
// documentation).
const DefaultMode = ModeSimple

func (m QueryMode) exec() (pgx.QueryExecMode, error) {
	if m == "" {
		m = DefaultMode
	}
	switch m {
	case ModeCache:
		return pgx.QueryExecModeCacheStatement, nil
	case ModeExec:
		return pgx.QueryExecModeExec, nil
	case ModeSimple:
		return pgx.QueryExecModeSimpleProtocol, nil
	}
	return 0, fmt.Errorf("xtengine: unknown query mode %q: %w", string(m), engine.ErrInvalid)
}

// Config says which server and which tables an engine uses.
type Config struct {
	// DSN is a pgx connection string, as a URL or as keyword/value pairs.
	DSN string
	// Prefix is put before the name of every table, so that engines that share a
	// server do not share tables. The tables of the baseline are those of an empty
	// prefix; a prefix is lower case letters, digits and underscores.
	Prefix string
	// Mode is how statements are sent (default [DefaultMode]).
	Mode QueryMode
	// Base is the system time of the zeroth batch: the n-th batch is committed at Base
	// plus n microseconds. The zero value is [xtmodel.BaseSystemTime], 2000-01-01, which
	// is what a measurement uses on a node of its own. A node orders all its transactions
	// by system time, so engines that share a node (a test's) must open with later and
	// later bases and not write in turn: a transaction at a system time before the last
	// one the node committed is refused. A base is kept in the meta documents of the node,
	// and an engine opened over a node that has them takes it from there; a Base that
	// differs from it is an error.
	Base time.Time
	// DataDirs are the directories on this machine in which the server keeps its log
	// and its storage, for Size. XTDB reports no size of its own.
	DataDirs []string
}

// conn is what the engine asks of a connection: a *pgx.Conn, or a test's fake.
type conn interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	Close(ctx context.Context) error
}

// Engine is an engine over a node of XTDB 2.1, reached over its Postgres wire
// protocol. It implements [engine.Engine]. Reads run concurrently with each other
// and with Write, one at a time on a connection of their own.
type Engine struct {
	cfg   Config
	names tableNames
	// w is the connection of Write, Retain and the planner's reads of the log; r is the
	// connection of every read of the engine. rmu serializes the use of r.
	w, r conn
	rmu  sync.Mutex
	ctx  context.Context
	stop context.CancelFunc

	// wmu serializes Write and Retain, and guards the planner and the counts. A
	// transaction's outcome is unknown after a failure the engine cannot see the end
	// of: broken then holds that error and refuses every later write.
	wmu     sync.Mutex
	pl      *xtmodel.Planner
	counts  xtmodel.Counts
	refused RefusedCounts
	broken  error
	// wctx is the context of the write in progress, which has a deadline, and writeTimeout
	// is how long that is.
	wctx         context.Context
	writeTimeout time.Duration

	// mu guards what the readers need to resolve a token: the sequence number of the
	// last committed batch, the ordinal of the last transaction (a batch or a
	// retention), the system time each token names, the retention horizon and the layer of each
	// subject.
	mu      sync.RWMutex
	base    xtmodel.Instant
	lastSeq uint64
	ordinal uint64
	tokens  map[uint64]uint64
	horizon int64
	pastEnd bool
	layers  map[engine.Subject]catalog.Layer

	reads readCounters

	// mut is a deliberate fault, for the tests.
	mut mutation
}

var _ engine.Engine = (*Engine)(nil)

// defaultWriteTimeout is the deadline of a write: a batch of a few hundred records takes
// seconds, and a commit that takes this long has hung.
const defaultWriteTimeout = 5 * time.Minute

// RefusedCounts are the batches the engine refused for what XTDB cannot hold, which
// are capability rows of a measurement and not failures.
type RefusedCounts struct {
	// OffMicrosecond are the records, in refused batches, with a boundary that is not
	// a whole microsecond: XTDB's temporal columns hold microseconds.
	OffMicrosecond uint64
}

// Open connects to the node and opens the tables under the prefix. A node that holds
// what an engine left under the same prefix is opened as it is: the token, the system
// time each token names, the retention horizon (as far as it was stored: see [Engine.Retain])
// and what the planner remembers are read back from it.
func Open(ctx context.Context, cfg Config) (*Engine, error) {
	names, err := namesFor(cfg.Prefix)
	if err != nil {
		return nil, err
	}
	mode, err := cfg.Mode.exec()
	if err != nil {
		return nil, err
	}
	if cfg.DSN == "" {
		return nil, errors.New("xtengine: no DSN")
	}
	cc, err := pgx.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("xtengine: DSN: %w", err)
	}
	cc.DefaultQueryExecMode = mode
	w, err := pgx.ConnectConfig(ctx, cc)
	if err != nil {
		return nil, fmt.Errorf("xtengine: %w", err)
	}
	r, err := pgx.ConnectConfig(ctx, cc)
	if err != nil {
		_ = w.Close(ctx)
		return nil, fmt.Errorf("xtengine: %w", err)
	}
	return openWith(ctx, cfg, names, w, r)
}

// openWith is Open over connections it is given, which it owns from then on.
func openWith(ctx context.Context, cfg Config, names tableNames, w, r conn) (*Engine, error) {
	e := &Engine{
		cfg: cfg, names: names, w: w, r: r,
		tokens: make(map[uint64]uint64), layers: make(map[engine.Subject]catalog.Layer),
	}
	e.ctx, e.stop = context.WithCancel(context.Background())
	e.wctx = e.ctx
	e.writeTimeout = defaultWriteTimeout
	if err := e.load(ctx); err != nil {
		e.stop()
		_ = w.Close(ctx)
		_ = r.Close(ctx)
		return nil, err
	}
	return e, nil
}

// sysTime is the system time of the n-th transaction: the base plus n microseconds.
func (e *Engine) sysTime(n uint64) xtmodel.Instant {
	return e.base + xtmodel.Instant(n)*xtmodel.Instant(nanosPerMicro)
}

// Close releases the connections. What the node holds stays, for Open.
func (e *Engine) Close() error {
	e.stop()
	ctx := context.Background()
	return errors.Join(e.w.Close(ctx), e.r.Close(ctx))
}

// LastSeq implements [engine.Engine]: the sequence number the last committed batch
// ended at. It changes only after the transaction has committed.
func (e *Engine) LastSeq() uint64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.lastSeq
}

// Transactions returns how many transactions the engine has committed, batches and
// retentions together: the ordinal of the last one, which the system times count.
func (e *Engine) Transactions() uint64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.ordinal
}

// Counts returns what the planner has planned for the batches committed so far, in
// the units of [xtmodel.Counts]. For one stream and one batching it equals the counts
// of an [xtmodel.Model] that was given the same stream.
func (e *Engine) Counts() xtmodel.Counts {
	e.wmu.Lock()
	defer e.wmu.Unlock()
	return e.counts
}

// Refused returns the capability rows: what batches were refused for what XTDB cannot
// hold.
func (e *Engine) Refused() RefusedCounts {
	e.wmu.Lock()
	defer e.wmu.Unlock()
	return e.refused
}

// SystemTimeOf returns the system time of the batch that ended at token, and whether
// a committed batch ended there.
func (e *Engine) SystemTimeOf(token uint64) (xtmodel.Instant, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	n, ok := e.tokens[token]
	return e.sysTime(n), ok
}

// Size implements [engine.Engine]: the bytes of the regular files under the data
// directories of the configuration, which is what the node keeps (its log and its
// storage). It is [ErrNoDataDirs] if there are none to measure.
func (e *Engine) Size() (int64, error) {
	if len(e.cfg.DataDirs) == 0 {
		return 0, ErrNoDataDirs
	}
	var n int64
	for _, dir := range e.cfg.DataDirs {
		err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !d.Type().IsRegular() {
				return nil
			}
			info, err := d.Info()
			if err != nil {
				return err
			}
			n += info.Size()
			return nil
		})
		if err != nil {
			return 0, fmt.Errorf("xtengine: size of %s: %w", dir, err)
		}
	}
	return n, nil
}
