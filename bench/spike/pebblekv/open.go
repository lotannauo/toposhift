package pebblekv

import (
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/cockroachkvs"
	"github.com/cockroachdb/pebble/v2/sstable"
	"github.com/cockroachdb/pebble/v2/sstable/colblk"
	"github.com/cockroachdb/pebble/v2/vfs"
)

// Schema is how a table lays out the keys of a block.
type Schema uint8

const (
	// SchemaCRDB is cockroachkvs's own columnar schema, "crdb1": the roach key,
	// the wall time and the logical time are separate columns, so a block of
	// versions of one key stores the key once and the times as integers.
	SchemaCRDB Schema = iota + 1
	// SchemaDefault is Pebble's default columnar schema over the same
	// comparer: the key split at the sentinel into a prefix and a suffix, each
	// stored as bytes.
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
// reads cross many blocks; the benchmarks use [BenchTuning].
type Tuning struct {
	MemTableSize          uint64
	BlockSize             int
	TargetFileSize        int64
	LBaseMaxBytes         int64
	L0CompactionThreshold int
	CacheBytes            int64
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

// BenchTuning is for measurement: Pebble's own defaults for block and table
// sizes, a larger memtable and a cache that holds a small working set. The
// numbers are placeholders until the measurement stage chooses them.
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
type Layout struct {
	// Name is the comparer's name, for messages.
	Name     string
	Comparer *pebble.Comparer
	schemas  map[Schema]*colblk.KeySchema
	// collectors are installed when [Config.TimeFilter] is set; none means the
	// layout has no filter to offer.
	collectors []func() pebble.BlockPropertyCollector
}

// CockroachLayout is layout M's: cockroachkvs's comparer, versions as MVCC
// timestamps, either key schema, and the MVCC time-interval collector.
var CockroachLayout = func() Layout {
	def := colblk.DefaultKeySchema(&cockroachkvs.Comparer, 16)
	return Layout{
		Name:       cockroachkvs.Comparer.Name,
		Comparer:   &cockroachkvs.Comparer,
		schemas:    map[Schema]*colblk.KeySchema{SchemaCRDB: &cockroachkvs.KeySchema, SchemaDefault: &def},
		collectors: cockroachkvs.BlockPropertyCollectors,
	}
}()

// BytewiseLayout is layout L's: Pebble's default comparer, which orders keys by
// their bytes, and its default columnar key schema over it. It offers no
// block-property filter.
var BytewiseLayout = func() Layout {
	def := colblk.DefaultKeySchema(pebble.DefaultComparer, 16)
	return Layout{
		Name:     pebble.DefaultComparer.Name,
		Comparer: pebble.DefaultComparer,
		schemas:  map[Schema]*colblk.KeySchema{SchemaDefault: &def},
	}
}()

// Config says how a database is opened. Every database is opened at
// [pebble.FormatNewest]: the format is a one-way decision (a database written at
// it cannot be read by an older Pebble), and crdb1 needs a version that
// supports columnar blocks, which Pebble's default does not.
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
	// FS is the file system the database lives on. Nil is the real one. A test
	// that runs many small databases passes [vfs.NewMem]: the tables, the log and
	// the compactions are the same code, without the cost of a disk's flush.
	FS vfs.FS
}

// quietLogger drops Pebble's logging, which a test run does not want, and turns
// a fatal condition into a panic, as the default does by exiting.
type quietLogger struct{}

func (quietLogger) Infof(string, ...any)  {}
func (quietLogger) Errorf(string, ...any) {}
func (quietLogger) Fatalf(format string, args ...any) {
	panic(fmt.Sprintf("pebble: "+format, args...))
}

// KV is an open database and the settings its layouts need next to it.
type KV struct {
	*pebble.DB
	cfg Config
	wo  *pebble.WriteOptions
}

// Open opens the database under dir, creating it if there is none.
func Open(dir string, layout Layout, cfg Config) (*KV, error) {
	t := cfg.Tuning
	if t == (Tuning{}) {
		t = BenchTuning() // a forgotten setting must not measure a toy
		cfg.Tuning = t
	}
	cache := pebble.NewCache(t.CacheBytes)
	defer cache.Unref() // the database holds its own reference

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
	opts := &pebble.Options{
		Comparer:                    layout.Comparer,
		FormatMajorVersion:          pebble.FormatNewest,
		Cache:                       cache,
		Logger:                      quietLogger{},
		MemTableSize:                t.MemTableSize,
		MemTableStopWritesThreshold: 4,
		L0CompactionThreshold:       t.L0CompactionThreshold,
		LBaseMaxBytes:               t.LBaseMaxBytes,
		FS:                          cfg.FS,
		KeySchema:                   chosen.Name,
		KeySchemas:                  sstable.MakeKeySchemas(all...),
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
	db, err := pebble.Open(dir, opts)
	if err != nil {
		return nil, fmt.Errorf("pebblekv: opening %s: %w", dir, err)
	}
	wo := pebble.NoSync
	if cfg.Sync {
		wo = pebble.Sync
	}
	return &KV{DB: db, cfg: cfg, wo: wo}, nil
}

// Config returns the settings the database was opened with.
func (k *KV) Config() Config { return k.cfg }

// WriteOptions is how this database commits.
func (k *KV) WriteOptions() *pebble.WriteOptions { return k.wo }

// Settle flushes the memtable and waits for background flushing and compaction
// to go quiet, so what is on disk, and what a read has to cross, stops changing
// under the caller. It is how the conformance test makes a read hit tables.
//
// Quiet means no flush or compaction running and none finished since the last
// poll, three polls in a row. Compactions are scheduled asynchronously, so a
// poll can in principle catch the gap before one starts; that costs a test some
// coverage of the compacted paths and never correctness. (Pebble's estimate of
// compaction debt is no use here: with small tables it can stay above zero
// forever, because no compaction is worth picking.) A measurement that needs a
// settled size should not rely on this.
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
// tables were written with: the key schema, and the block-property collectors
// that ran. A test uses it to check a variant is in effect and not just asked
// for.
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

// Meta keys hold what a layout needs to survive a reopening. Each layout
// encodes the key for its own comparer; the names and the value forms are
// shared.
const (
	MetaFormat  = "format"
	MetaLastSeq = "lastSeq"
	MetaHorizon = "horizon"
)

// GetMeta returns the value at a meta key, or nil if there is none.
func (k *KV) GetMeta(key []byte) ([]byte, error) {
	v, closer, err := k.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = closer.Close() }()
	return slices.Clone(v), nil
}

// EncodeSeq is the stored form of a sequence number.
func EncodeSeq(seq uint64) []byte { return binary.BigEndian.AppendUint64(nil, seq) }

// DecodeSeq reads a sequence number written by [EncodeSeq]; nil is zero, the
// token of an empty store.
func DecodeSeq(b []byte) (uint64, error) {
	if b == nil {
		return 0, nil
	}
	if len(b) != 8 {
		return 0, fmt.Errorf("sequence number is %d bytes, want 8: %w", len(b), ErrValue)
	}
	return binary.BigEndian.Uint64(b), nil
}

// EncodeHorizon is the stored form of a retention horizon: the instant exactly,
// whatever it is, in time.Time's own binary form.
func EncodeHorizon(h time.Time) ([]byte, error) { return h.MarshalBinary() }

// DecodeHorizon reads a horizon written by [EncodeHorizon]; nil is the zero
// time, the horizon of a store that has never retained.
func DecodeHorizon(b []byte) (time.Time, error) {
	var h time.Time
	if b == nil {
		return h, nil
	}
	if err := h.UnmarshalBinary(b); err != nil {
		return time.Time{}, fmt.Errorf("horizon: %w: %w", err, ErrValue)
	}
	return h, nil
}

// CheckFormat makes sure a database holds the format a layout expects, writing
// it if the database is new, and refuses one written by another layout or
// another version of this one. key is the layout's meta key for [MetaFormat].
func (k *KV) CheckFormat(key, want []byte) error {
	got, err := k.GetMeta(key)
	if err != nil {
		return err
	}
	switch {
	case got == nil:
		return k.Set(key, want, k.wo)
	case !slices.Equal(got, want):
		return fmt.Errorf("pebblekv: the database holds format %q, this is %q", got, want)
	}
	return nil
}
