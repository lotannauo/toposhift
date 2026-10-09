package pebblekv

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/sstable"
	"github.com/cockroachdb/pebble/v2/sstable/colblk"
	"github.com/cockroachdb/pebble/v2/vfs"
)

// Schema is how a table lays out the keys of a block.
type Schema uint8

const (
	// SchemaCRDB is cockroachkvs's own columnar schema, "crdb1". This module does
	// not link cockroachkvs, so [BytewiseLayout] has no such schema; the constant
	// is for a layout built with [NewLayout] by a module that does.
	SchemaCRDB Schema = iota + 1
	// SchemaDefault is Pebble's default columnar schema over the layout's
	// comparer: the key split at the comparer's sentinel into a prefix and a
	// suffix, each stored as bytes.
	SchemaDefault
)

func (s Schema) String() string {
	switch s {
	case SchemaCRDB:
		return "crdb1"
	case SchemaDefault:
		return "default"
	}
	return fmt.Sprintf("Schema(%d)", uint8(s))
}

// Tuning is the part of the Pebble options that sizes things. Tests use
// [TinyTuning], so a few thousand records reach several levels of tables and
// reads cross many blocks; measurements use [BenchTuning].
type Tuning struct {
	MemTableSize          uint64
	BlockSize             int
	TargetFileSize        int64
	LBaseMaxBytes         int64
	L0CompactionThreshold int
	// CacheBytes is the block cache for blocks. A database that is written to gets
	// [Tuning.MemTableReserve] more, which Pebble holds back for its memtables.
	CacheBytes int64
}

// TinyTuning makes everything small, to exercise flushes, compactions and block
// boundaries with little data.
func TinyTuning() Tuning {
	return Tuning{
		MemTableSize:          32 << 10,
		BlockSize:             512,
		TargetFileSize:        8 << 10,
		LBaseMaxBytes:         32 << 10,
		L0CompactionThreshold: 2,
		CacheBytes:            1 << 20,
	}
}

// BenchTuning is for measurement: 32 KB blocks (CockroachDB's choice; Pebble's
// default is 4 KB, and larger blocks favour a layout that reads a long run of
// keys, so the block size is a variable to measure and not a given), 64 MB tables
// and a 256 MB level base (Pebble's defaults are 2 MB and 64 MB), a larger
// memtable than its 4 MB, and a cache that holds a small working set. The numbers
// are placeholders until the measurement stage chooses them.
func BenchTuning() Tuning {
	return Tuning{
		MemTableSize:          64 << 20,
		BlockSize:             32 << 10,
		TargetFileSize:        64 << 20,
		LBaseMaxBytes:         256 << 20,
		L0CompactionThreshold: 4,
		CacheBytes:            256 << 20,
	}
}

// Layout is what a layout's keys need from Pebble: the comparer that orders
// them, the key schemas its tables may be written in, and the block-property
// collectors it can use. The layout's own package passes it to [Open]; it is not
// part of [Config], because a layout opened with another's comparer would sort
// its keys wrongly (Pebble only checks the comparer's name against a database
// that already exists).
//
// The comparer's name, the key schema's name and every collector's name are
// persisted in the tables. They are part of the layout's stored format.
type Layout struct {
	// Name is the layout's name, for messages.
	Name     string
	Comparer *pebble.Comparer
	schemas  map[Schema]*colblk.KeySchema
	// collectors are installed when [Config.TimeFilter] is set; none means the
	// layout has no filter to offer.
	collectors []func() pebble.BlockPropertyCollector
}

// NewLayout makes a layout from a comparer, the key schemas a table may be
// written in (each registered, so a database written under one can be opened
// under another), and the collectors that [Config.TimeFilter] installs, if any.
// It is how a module that links more of Pebble than this one does (the
// benchmarks use cockroachkvs's comparer) offers a layout to [Open].
func NewLayout(name string, comparer *pebble.Comparer, schemas map[Schema]*colblk.KeySchema, collectors []func() pebble.BlockPropertyCollector) Layout {
	return Layout{
		Name:       name,
		Comparer:   comparer,
		schemas:    maps.Clone(schemas),
		collectors: slices.Clone(collectors),
	}
}

// BytewiseLayout is layout L's: Pebble's default comparer, which orders keys by
// their bytes, and its default columnar key schema over it. It offers no
// block-property filter.
var BytewiseLayout = func() Layout {
	def := colblk.DefaultKeySchema(pebble.DefaultComparer, 16)
	return NewLayout(pebble.DefaultComparer.Name, pebble.DefaultComparer,
		map[Schema]*colblk.KeySchema{SchemaDefault: &def}, nil)
}()

// DefaultSettleDeadline is how long [Config.SettleRetention] waits when
// [Config.SettleDeadline] is zero. The wait is bounded so a retention cannot hold
// the writer forever; two minutes is twice the stall budget, so a settle that
// reaches it has already failed the stall gate, and the time it took is still
// measured.
const DefaultSettleDeadline = 2 * time.Minute

// Config says how a database is opened. Every database is opened at
// [pebble.FormatValueSeparation]: the format is a one-way decision (a database
// written at it cannot be read by an older Pebble), and it is written out here
// instead of following [pebble.FormatNewest], which moves with a Pebble upgrade.
type Config struct {
	// Schema is the key schema new tables are written in; the layout says which
	// it supports.
	Schema Schema
	// TimeFilter installs the layout's time-interval block-property collector, so
	// every table records the span of wall times in each block, and says a layout
	// may use it to skip blocks it does not need. A layout with none refuses it.
	TimeFilter bool
	Tuning     Tuning
	// Sync makes every commit wait for the log to reach the disk. Off, a commit
	// survives a closed process but not a crashed machine.
	Sync bool
	// DisableAutoCompactions turns off the compactions Pebble schedules on its
	// own (a flush still happens). A measurement opens a built database with it,
	// so the shape of the tables does not change under the reads being timed.
	DisableAutoCompactions bool
	// DisableReadCompactions turns off the compactions Pebble triggers by sampling
	// reads, which are on by default and would rewrite tables in the middle of a
	// measurement of reads.
	DisableReadCompactions bool
	// SettleRetention makes a retention end only when the database has settled what it
	// wrote: after its last commit, the retention flushes and waits, at most
	// SettleDeadline, until the database is at rest, so that the compactions its range
	// deletions call for have run before the writer resumes (see
	// [KV.SettleAfterRetention]). Off, a retention returns after its last commit.
	SettleRetention bool
	// SettleDeadline bounds the wait of SettleRetention; zero is DefaultSettleDeadline.
	// When it passes, the retention returns without error and records that it did.
	SettleDeadline time.Duration
	// ReadOnly opens an existing database without the ability to write to it: no
	// log is created, nothing is flushed or compacted, no background statistics
	// are collected, and a write is an error. A measurement of reads opens the
	// database it built this way, so the database cannot have worked while being
	// read, and nothing runs behind a read.
	ReadOnly bool
	// FS is the file system the database lives on. Nil is the real one. A test
	// that runs many small databases passes [vfs.NewMem]: the tables, the log and
	// the compactions are the same code, without the cost of a disk's flush.
	FS vfs.FS

	// logger replaces the log/slog logger of this package, for a test that wants
	// Pebble quiet or wants to see what it reports. Nil is the default.
	logger pebble.Logger
}

// KV is an open database and the settings its layouts need next to it.
type KV struct {
	*pebble.DB
	cfg Config
	wo  *pebble.WriteOptions
	cmp func(a, b []byte) int
	// recovered is what opening wrote to tables from the log of a database that
	// was not closed clean.
	recovered uint64
	// cache is the block cache the database uses; this holds a reference to it so
	// that [KV.ColdStart] can empty it.
	cache *pebble.Cache
	// dir and opts are where the database lives and the options it was opened with,
	// defaults filled in.
	dir       string
	opts      *pebble.Options
	closeOnce sync.Once
	closeErr  error
}

// Close closes the database and releases the block cache.
func (k *KV) Close() error {
	k.closeOnce.Do(func() {
		k.closeErr = k.DB.Close()
		k.cache.Unref()
	})
	return k.closeErr
}

// Dir is the directory the database lives in.
func (k *KV) Dir() string { return k.dir }

// Options is a copy of the Pebble options the database was opened with, with
// Pebble's defaults filled in, for code outside this package that writes tables
// or reads the settings. Changing the copy changes nothing in the database.
func (k *KV) Options() *pebble.Options { return k.opts.Clone() }

// Cache is the block cache of the database. It is owned by the KV and released by
// [KV.Close]: a caller that keeps it past Close takes its own reference first.
func (k *KV) Cache() *pebble.Cache { return k.cache }

// ColdStart empties the block cache, so that the next read finds none of the
// blocks it needs in it. The tables stay open, which is what a database that has
// been running for a while has, and what is measured is then the blocks a read
// has to bring in, not the opening of files. It must not be called while a read
// is in progress, whose blocks are held and are not evicted.
func (k *KV) ColdStart() {
	k.cache.Reserve(int(k.cache.MaxSize()))()
}

// memTableStopWritesThreshold is how many memtables, full or being filled, Pebble
// holds before it stops writes until one is flushed.
const memTableStopWritesThreshold = 4

// MemTableReserve is about as much of the block cache as Pebble holds back for the
// memtables of a database that is written to: it charges each memtable, the one
// being filled and the ones waiting for their flush, to the cache, up to the
// threshold at which it stops writes (a flushed memtable that an open iterator still
// holds keeps its charge a while longer, so it can briefly be more). A database
// opened read-only has none.
func (t Tuning) MemTableReserve() int64 { return int64(t.MemTableSize) * memTableStopWritesThreshold }

// Open opens the database under dir, creating it if there is none.
func Open(dir string, layout Layout, cfg Config) (*KV, error) {
	t := cfg.Tuning
	if t == (Tuning{}) {
		t = BenchTuning() // a forgotten setting must not measure a toy
		cfg.Tuning = t
	}
	// Pebble charges its memtables to the block cache, so a database that is written
	// to gets room for them on top of the blocks: with a cache no larger than the
	// memtables it would cache no block at all while it is written.
	size := t.CacheBytes
	if !cfg.ReadOnly {
		size += t.MemTableReserve()
	}
	cache := pebble.NewCache(size) // this reference is the KV's, released by Close

	opts, err := buildOptions(layout, cfg, cache)
	if err != nil {
		cache.Unref()
		return nil, err
	}
	db, err := pebble.Open(dir, opts)
	if err != nil {
		cache.Unref()
		return nil, fmt.Errorf("pebblekv: opening %s: %w", dir, err)
	}
	wo := pebble.NoSync
	if cfg.Sync {
		wo = pebble.Sync
	}
	kv := &KV{DB: db, cfg: cfg, wo: wo, cmp: layout.Comparer.Compare, cache: cache, dir: dir}
	kv.recovered = db.Metrics().Total().TableBytesFlushed
	full := opts.Clone() // Open filled in the defaults on a copy of its own
	full.EnsureDefaults()
	kv.opts = full
	return kv, nil
}

// buildOptions is the Pebble options a layout and a config come to.
func buildOptions(layout Layout, cfg Config, cache *pebble.Cache) (*pebble.Options, error) {
	t := cfg.Tuning
	if layout.Comparer == nil {
		return nil, errors.New("pebblekv: the layout has no comparer")
	}
	// Every schema the layout has is registered, and the config only chooses which
	// one new tables are written in: a table written under one can be read by a
	// database opened under the other. (An unregistered schema is a panic inside
	// Pebble when the first such table is read, not an error from Open.)
	chosen, ok := layout.schemas[cfg.Schema]
	if !ok {
		return nil, fmt.Errorf("pebblekv: layout %s has no %s key schema", layout.Name, cfg.Schema)
	}
	all := make([]*colblk.KeySchema, 0, len(layout.schemas))
	for _, s := range layout.schemas {
		all = append(all, s)
	}
	var log pebble.Logger = logger{}
	if cfg.logger != nil {
		log = cfg.logger
	}
	opts := &pebble.Options{
		Comparer: layout.Comparer,
		// Written out, not pebble.FormatNewest: the format only ratchets forward, and
		// the newest one moves with the Pebble version.
		FormatMajorVersion:          pebble.FormatValueSeparation,
		Cache:                       cache,
		Logger:                      log,
		MemTableSize:                t.MemTableSize,
		MemTableStopWritesThreshold: memTableStopWritesThreshold,
		L0CompactionThreshold:       t.L0CompactionThreshold,
		LBaseMaxBytes:               t.LBaseMaxBytes,
		FS:                          cfg.FS,
		KeySchema:                   chosen.Name,
		KeySchemas:                  sstable.MakeKeySchemas(all...),
	}
	// Off, which is Pebble's default at the pinned version, written out so that a
	// future default cannot reshape the tables.
	opts.Experimental.ValueSeparationPolicy = func() pebble.ValueSeparationPolicy {
		return pebble.ValueSeparationPolicy{Enabled: false}
	}
	opts.DisableAutomaticCompactions = cfg.DisableAutoCompactions
	opts.ReadOnly = cfg.ReadOnly
	if cfg.DisableReadCompactions {
		opts.Experimental.ReadSamplingMultiplier = -1
	}
	opts.Levels[0].BlockSize = t.BlockSize
	opts.Levels[0].IndexBlockSize = t.BlockSize
	opts.TargetFileSizes[0] = t.TargetFileSize
	if cfg.TimeFilter {
		if layout.collectors == nil {
			return nil, fmt.Errorf("pebblekv: layout %s has no time-interval filter", layout.Name)
		}
		opts.BlockPropertyCollectors = layout.collectors
	}
	return opts, nil
}

// Config returns the settings the database was opened with.
func (k *KV) Config() Config { return k.cfg }

// WriteOptions is how this database commits.
func (k *KV) WriteOptions() *pebble.WriteOptions { return k.wo }

// Settle flushes the memtable and waits for background flushing and compaction
// to go quiet, so what is on disk, and what a read has to cross, stops changing
// under the caller. It is how a test makes a read hit tables.
//
// Quiet means no flush or compaction running and none finished since the last
// poll, three polls in a row. Compactions are scheduled asynchronously, so a
// poll can in principle catch the gap before one starts; that costs a test some
// coverage of the compacted paths and never correctness. (Pebble's estimate of
// compaction debt is no use here: with small tables it can stay above zero
// forever, because no compaction is worth picking.) A retention that must leave
// the database at rest uses [KV.SettleAfterRetention].
func (k *KV) Settle() error {
	if err := k.Flush(); err != nil {
		return err
	}
	var lastCompactions, lastFlushes int64 = -1, -1
	quiet := 0
	deadline := time.Now().Add(2 * time.Minute)
	for quiet < 3 {
		m := k.Metrics()
		if m.Compact.NumInProgress == 0 && m.Flush.NumInProgress == 0 &&
			m.Compact.Count == lastCompactions && m.Flush.Count == lastFlushes {
			quiet++
		} else {
			quiet = 0
		}
		lastCompactions, lastFlushes = m.Compact.Count, m.Flush.Count
		if time.Now().After(deadline) {
			return errors.New("pebblekv: compactions did not settle in two minutes")
		}
		time.Sleep(time.Millisecond)
	}
	return nil
}

// TableProperties returns the properties of every table, which say what the
// tables were written with: the comparer, the key schema, and the block-property
// collectors that ran. A test uses it to check a variant is in effect and not
// just asked for.
func (k *KV) TableProperties() ([]*sstable.Properties, error) {
	levels, err := k.SSTables(pebble.WithProperties())
	if err != nil {
		return nil, err
	}
	var out []*sstable.Properties
	for _, level := range levels {
		for _, t := range level {
			out = append(out, t.Properties)
		}
	}
	return out, nil
}

// Size is the bytes the database holds on disk: tables, log and the rest.
func (k *KV) Size() (int64, error) {
	return int64(k.Metrics().DiskSpaceUsage()), nil
}
