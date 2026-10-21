package pebblekv

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
	// FlushDelayDeleteRange, if not zero, is how long after the first range deletion
	// is applied to a memtable Pebble flushes that memtable, at the latest. Zero, the
	// value of both tunings here, is off: a retention pass flushes its own range
	// deletions once they reach a count (see pebblestore). A delay that is set
	// rotates the memtable on Pebble's own timer, through the same wait on Pebble's
	// write-stall conditions as any rotation, with Pebble's commit mutex held and
	// without the check of level 0 a pass makes before it flushes: if level 0 is at
	// its stop threshold, every commit then waits for compactions.
	FlushDelayDeleteRange time.Duration

	// The fields below choose how Pebble shapes and compacts its tables. Every one of
	// them is zero (or false) in both tunings here, and zero leaves what Pebble does by
	// itself at the pinned version: the options the store is opened with are then
	// exactly the ones it was opened with before the fields existed (see
	// TestDefaultTuningsLeaveTheOptionsAsTheyWere). A value that is set and cannot be
	// honoured is refused by [Tuning.Validate], which [Open] calls, and not replaced by
	// a default: Pebble itself turns a negative size or threshold into its default
	// without a word.

	// LBaseTargetFileSize, if above zero, is the target size of the tables of the base
	// level (the highest level below level 0 that holds tables, whichever that is for
	// the database's size), and each level below it doubles it; level 0 keeps
	// TargetFileSize. Zero leaves Pebble's own rule: the base level doubles
	// TargetFileSize, and each level below doubles again. A smaller base target bounds
	// how many of a retention pass's range deletions one compaction can gather into one
	// table above the bottom level. TargetFileSize stays the size of the canonical
	// layout's tables, which this does not change. Not with TargetFileSizes.
	LBaseTargetFileSize int64
	// TargetFileSizes, if any entry is above zero, gives the target size of the tables
	// level by level, indexed as Pebble indexes its own: [0] is level 0, [1] the base
	// level, [2] the level below it, and so on, whichever levels those are for the
	// database's size. An entry left at zero is twice the one before it. Entry [0], if
	// given, must be TargetFileSize, which stays the size of the canonical layout's
	// tables; and LBaseTargetFileSize must then be zero, since it sets entry [1].
	TargetFileSizes [numTargetLevels]int64

	// CompactionConcurrencyLower and CompactionConcurrencyUpper are the range Pebble
	// runs compactions in parallel within (not counting delete-only compactions, which
	// Pebble schedules apart from it). Both zero leave Pebble's (1, 1). Otherwise the
	// lower bound is at least 1, and a lower bound left at zero is 1 and an upper left
	// at zero is the lower; the lower may not be above the upper. Pebble raises the
	// number above the lower bound only for one of three signals, whichever gives the
	// most: one extra compaction for every 10 sublevels of level 0, one for every
	// gigabyte of compaction debt, or a share of the range between the bounds that
	// grows with the estimated garbage of point and range deletions as a fraction of
	// the database (40 percent gives the upper bound). The 10, the gigabyte and the 40
	// percent are Pebble's, and are not set here. With level 0 stopping writes at 12
	// sublevels, the first signal is rarely the one; the garbage is the one a
	// retention's deletions move.
	CompactionConcurrencyLower int
	CompactionConcurrencyUpper int
	// L0StopWritesThreshold is how many sublevels level 0 may hold before Pebble stops
	// writes (and a flush that rotates the memtable) until compactions bring them down.
	// Zero leaves Pebble's 12. It must not be below L0CompactionThreshold, which, left
	// at zero, is Pebble's 4. A retention pass puts off its flush when level 0 is within
	// two sublevels of this number (see pebblestore), so the threshold moves that too.
	L0StopWritesThreshold int
	// DisableDeleteOnlyCompactions turns off Pebble's delete-only compactions, which
	// drop a table that a range deletion covers wholly. Pebble then neither collects
	// the deletion hints it would examine for them (one for each range deletion span
	// over tables beneath it, read from the tables' statistics) nor schedules the
	// compactions. The tables such a compaction would drop are reclaimed by ordinary
	// compactions instead, later, so the database holds more bytes in the meantime. Pebble has the setting only among
	// its private options, which it describes as for its tests and upgrade paths, set
	// through [pebble.Options.Parse]; it is tied to the pinned version (see
	// TestTheDeleteOnlyCompactionsAreOff). False leaves them on.
	DisableDeleteOnlyCompactions bool
	// L0CompactionConcurrency is how many sublevels of level 0 Pebble wants for each
	// compaction above the lower bound of the concurrency range (see above). Zero leaves
	// Pebble's 10 ([ProductTuning] writes it out). At least 1 otherwise.
	L0CompactionConcurrency int
	// CompactionDebtConcurrency is how many bytes of compaction debt Pebble wants for
	// each compaction above the lower bound. Zero leaves Pebble's 1 GiB ([ProductTuning]
	// writes it out).
	CompactionDebtConcurrency uint64
	// CompactionGarbageFraction is the share of the database that is estimated garbage of
	// point and range deletions at which Pebble allows the upper bound of the concurrency
	// range; below it, the allowed number grows in proportion. Zero leaves Pebble's 0.4
	// ([ProductTuning] writes it out); otherwise it is in (0, 1]. Pebble can turn this
	// signal off with a value at or below zero; this field does not offer that.
	CompactionGarbageFraction float64
	// Compression is how the blocks of every table are compressed; see [Compression].
	// The zero value leaves Pebble's default, Snappy.
	Compression Compression
}

// Compression is how a table's blocks are compressed, the same for every level but in
// [CompressionBalanced]. A database may be opened under another compression than the
// one it was written under: each block says how it was compressed, so tables of
// different kinds sit side by side until compactions rewrite them.
type Compression uint8

const (
	// CompressionDefault leaves Pebble's own default, Snappy, which is pure Go. The
	// options are then untouched.
	CompressionDefault Compression = iota
	// CompressionSnappy sets Snappy on every level explicitly. The tables are the same
	// as CompressionDefault's; the choice is written out so that it is recorded.
	CompressionSnappy
	// CompressionZstd is Pebble's ZSTD profile (level 3) on every level. Pebble uses a
	// pure-Go encoder when cgo is off or the build tag pebblegozstd is set, and a cgo
	// one otherwise; the two write different bytes, both of which either reads.
	CompressionZstd
	// CompressionBalanced is Pebble's Balanced settings: the fast codec on every level,
	// with ZSTD level 1 for the value blocks of the second-lowest level and for the data
	// and value blocks of the bottom level. The fast codec is MinLZ on amd64 and Snappy on
	// arm64 (Pebble's choice, by architecture), so the bytes of the levels that use it are
	// not the same on the two.
	CompressionBalanced
)

func (c Compression) String() string {
	switch c {
	case CompressionDefault:
		return "default"
	case CompressionSnappy:
		return "snappy"
	case CompressionZstd:
		return "zstd"
	case CompressionBalanced:
		return "balanced"
	}
	return fmt.Sprintf("Compression(%d)", uint8(c))
}

// ParseCompression is the compression a name stands for: "default" (or ""), "snappy",
// "zstd" or "balanced".
func ParseCompression(s string) (Compression, error) {
	switch s {
	case "", "default":
		return CompressionDefault, nil
	case "snappy":
		return CompressionSnappy, nil
	case "zstd":
		return CompressionZstd, nil
	case "balanced":
		return CompressionBalanced, nil
	}
	return 0, fmt.Errorf("pebblekv: compression %q is not default, snappy, zstd or balanced", s)
}

// settings is the per-level profiles Pebble applies for c, or false for the default,
// which leaves them to Pebble.
func (c Compression) settings() (pebble.DBCompressionSettings, bool, error) {
	switch c {
	case CompressionDefault:
		return pebble.DBCompressionSettings{}, false, nil
	case CompressionSnappy:
		return pebble.UniformDBCompressionSettings(sstable.SnappyCompression), true, nil
	case CompressionZstd:
		return pebble.UniformDBCompressionSettings(sstable.ZstdCompression), true, nil
	case CompressionBalanced:
		return pebble.DBCompressionBalanced, true, nil
	}
	return pebble.DBCompressionSettings{}, false, fmt.Errorf("pebblekv: unknown compression %s", c)
}

// pebbleL0CompactionThreshold and pebbleL0StopWritesThreshold are what Pebble fills in
// for a zero (see pebble.Options.EnsureDefaults at the pinned version;
// TestPebbleDefaultsAreAsAssumed checks them).
const (
	pebbleL0CompactionThreshold = 4
	pebbleL0StopWritesThreshold = 12
	// The signals that raise compaction concurrency above its lower bound, at the
	// pinned version.
	pebbleL0CompactionConcurrency   = 10
	pebbleCompactionDebtConcurrency = 1 << 30
	pebbleCompactionGarbageFraction = 0.4
)

// RetainFlushL0Margin is how many sublevels below its stop threshold level 0 must be for
// a retention pass to start a flush (pebblestore puts the flush off at or above it, since
// a flush started near the stop could wait for compactions with Pebble's commit mutex
// held). [Tuning.Validate] refuses a stop threshold that leaves the flush put off at the
// depth at which compactions of level 0 start.
const RetainFlushL0Margin = 2

// numTargetLevels is how many entries Pebble's TargetFileSizes has: one per level.
const numTargetLevels = len(pebble.Options{}.TargetFileSizes)

// disableDeleteOnlyCompactions is the text Pebble parses into its private setting
// that turns delete-only compactions off.
const disableDeleteOnlyCompactions = "[Options]\n  disable_delete_only_compactions=true\n"

// Validate refuses a tuning whose compaction and table-shape fields cannot be honoured
// as given. It does not look at the sizing fields that existed before them, which
// Pebble defaults when they are zero.
func (t Tuning) Validate() error {
	if t.LBaseTargetFileSize < 0 {
		return fmt.Errorf("pebblekv: base-level target file size %d: give a size in bytes, or 0 for Pebble's doubling of the level-0 target", t.LBaseTargetFileSize)
	}
	if t.TargetFileSizes != ([numTargetLevels]int64{}) {
		for i, size := range t.TargetFileSizes {
			if size < 0 {
				return fmt.Errorf("pebblekv: target file size %d at index %d: give sizes in bytes, or 0 for twice the entry before", size, i)
			}
		}
		switch {
		case t.LBaseTargetFileSize != 0:
			return errors.New("pebblekv: both a base-level target file size and the targets of every level: give one")
		case t.TargetFileSizes[0] != 0 && t.TargetFileSizes[0] != t.TargetFileSize:
			return fmt.Errorf("pebblekv: the level-0 target %d differs from TargetFileSize %d, the canonical layout's table size: give the same, or 0", t.TargetFileSizes[0], t.TargetFileSize)
		}
	}
	if t.CompactionConcurrencyLower < 0 || t.CompactionConcurrencyUpper < 0 {
		return fmt.Errorf("pebblekv: compaction concurrency (%d, %d): give counts of at least 1, or 0 for Pebble's own", t.CompactionConcurrencyLower, t.CompactionConcurrencyUpper)
	}
	if lower, upper := t.concurrency(); lower > upper {
		return fmt.Errorf("pebblekv: compaction concurrency lower bound %d is above the upper bound %d, which Pebble would silently take for both", lower, upper)
	}
	if t.L0StopWritesThreshold < 0 {
		return fmt.Errorf("pebblekv: level 0 stop threshold %d: give a count of sublevels, or 0 for Pebble's %d", t.L0StopWritesThreshold, pebbleL0StopWritesThreshold)
	}
	stop, compact := t.L0StopWritesThreshold, t.L0CompactionThreshold
	if stop == 0 {
		stop = pebbleL0StopWritesThreshold
	}
	if compact <= 0 {
		compact = pebbleL0CompactionThreshold
	}
	switch {
	case stop < compact:
		return fmt.Errorf("pebblekv: level 0 stop threshold %d is below the compaction threshold %d, so writes would stop before level 0 was compacted", stop, compact)
	case stop-RetainFlushL0Margin <= compact:
		// A retention pass puts off its flush at that many sublevels below the stop, which
		// would then be at the depth where compactions of level 0 begin, that is, always.
		return fmt.Errorf("pebblekv: level 0 stop threshold %d is within %d sublevels of the compaction threshold %d: a retention pass puts off its flush that far below the stop, so it would put it off at the depth level 0 normally has (give a stop threshold above %d)", stop, RetainFlushL0Margin, compact, compact+RetainFlushL0Margin)
	}
	if t.L0CompactionConcurrency < 0 {
		return fmt.Errorf("pebblekv: L0 compaction concurrency %d: give a count of sublevels per extra compaction, at least 1, or 0 for Pebble's %d", t.L0CompactionConcurrency, pebbleL0CompactionConcurrency)
	}
	if f := t.CompactionGarbageFraction; f != 0 && !(f > 0 && f <= 1) { // also refuses NaN
		return fmt.Errorf("pebblekv: compaction garbage fraction %v: give a share above 0 and at most 1, or 0 for Pebble's %v", f, pebbleCompactionGarbageFraction)
	}
	if _, _, err := t.Compression.settings(); err != nil {
		return err
	}
	return nil
}

// shape is the tuning's fields that shape tables and compactions, the ones after
// FlushDelayDeleteRange, and nothing else: [Tuning.WithShapeOf] copies exactly these,
// and [Tuning.ShapeString] names exactly these.
func (t Tuning) shape() Tuning {
	return Tuning{
		LBaseTargetFileSize: t.LBaseTargetFileSize, TargetFileSizes: t.TargetFileSizes,
		CompactionConcurrencyLower: t.CompactionConcurrencyLower, CompactionConcurrencyUpper: t.CompactionConcurrencyUpper,
		L0StopWritesThreshold: t.L0StopWritesThreshold, DisableDeleteOnlyCompactions: t.DisableDeleteOnlyCompactions,
		L0CompactionConcurrency: t.L0CompactionConcurrency, CompactionDebtConcurrency: t.CompactionDebtConcurrency,
		CompactionGarbageFraction: t.CompactionGarbageFraction,
		Compression:               t.Compression,
	}
}

// WithShapeOf is t with the table-shape and compaction fields of o (those that
// Validate checks, from LBaseTargetFileSize on), and the rest of t as it was.
func (t Tuning) WithShapeOf(o Tuning) Tuning {
	s := o.shape()
	t.LBaseTargetFileSize, t.TargetFileSizes = s.LBaseTargetFileSize, s.TargetFileSizes
	t.CompactionConcurrencyLower, t.CompactionConcurrencyUpper = s.CompactionConcurrencyLower, s.CompactionConcurrencyUpper
	t.L0StopWritesThreshold, t.DisableDeleteOnlyCompactions, t.Compression = s.L0StopWritesThreshold, s.DisableDeleteOnlyCompactions, s.Compression
	t.L0CompactionConcurrency, t.CompactionDebtConcurrency, t.CompactionGarbageFraction = s.L0CompactionConcurrency, s.CompactionDebtConcurrency, s.CompactionGarbageFraction
	return t
}

// HasShape says [Tuning.ShapeString] has anything to record: the three concurrency
// signals written out at Pebble's value are left out; every other field that is set is
// recorded, even at Pebble's value.
func (t Tuning) HasShape() bool { return t.ShapeString() != "" }

// ShapeString says in one line which of those fields are set and to what, for a
// measurement to record: empty when none is, so that a record of a build with the
// defaults has nothing to carry. The three concurrency signals written out at Pebble's
// value, as [ProductTuning] does, are left out; every other field that is set is
// recorded, even at Pebble's value. The words and their order are fixed; a concurrency
// range is given with its bounds filled in.
func (t Tuning) ShapeString() string {
	var parts []string
	if t.LBaseTargetFileSize != 0 {
		parts = append(parts, fmt.Sprintf("lbase_target_file_size=%d", t.LBaseTargetFileSize))
	}
	if t.TargetFileSizes != ([numTargetLevels]int64{}) {
		sizes := make([]string, len(t.TargetFileSizes))
		for i, n := range t.TargetFileSizes {
			sizes[i] = fmt.Sprint(n)
		}
		parts = append(parts, "target_file_sizes="+strings.Join(sizes, ","))
	}
	if lower, upper := t.concurrency(); upper > 0 {
		parts = append(parts, fmt.Sprintf("compaction_concurrency=%d-%d", lower, upper))
	}
	if t.L0StopWritesThreshold != 0 {
		parts = append(parts, fmt.Sprintf("l0_stop_writes_threshold=%d", t.L0StopWritesThreshold))
	}
	if t.DisableDeleteOnlyCompactions {
		parts = append(parts, "delete_only_compactions=off")
	}
	if n := t.L0CompactionConcurrency; n != 0 && n != pebbleL0CompactionConcurrency {
		parts = append(parts, fmt.Sprintf("l0_compaction_concurrency=%d", n))
	}
	if n := t.CompactionDebtConcurrency; n != 0 && n != pebbleCompactionDebtConcurrency {
		parts = append(parts, fmt.Sprintf("compaction_debt_concurrency=%d", n))
	}
	if f := t.CompactionGarbageFraction; f != 0 && f != pebbleCompactionGarbageFraction {
		parts = append(parts, "compaction_garbage_fraction="+strconv.FormatFloat(f, 'g', -1, 64))
	}
	if t.Compression != CompressionDefault {
		parts = append(parts, "compression="+t.Compression.String())
	}
	return strings.Join(parts, " ")
}

// concurrency is the range the tuning asks for, with a bound left at zero filled in
// as [Tuning.CompactionConcurrencyLower] says; both zero is (0, 0), no choice.
func (t Tuning) concurrency() (lower, upper int) {
	lower, upper = t.CompactionConcurrencyLower, t.CompactionConcurrencyUpper
	if lower == 0 && upper == 0 {
		return 0, 0
	}
	if lower == 0 {
		lower = 1
	}
	if upper == 0 {
		upper = lower
	}
	return lower, upper
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

// ProductTuning is the tuning of the store that boots with no configuration
// (ProductTuning's users are pebblestore's DefaultOptions and toposhift serve). Today it is
// [BenchTuning]'s sizes with the three signals of compaction concurrency written out at
// the values Pebble has by default at the pinned version, so that the options Pebble is
// opened with come to exactly the ones [BenchTuning] gives (TestProductTuningComesToTheOptionsOfBefore
// checks them against the files in testdata). It is where a chosen change to the product's
// Pebble settings goes; [BenchTuning] stays as it is. The numbers of the benchmark
// spike are made with [BenchTuning], and the RocksDB module maps only that and
// [TinyTuning].
func ProductTuning() Tuning {
	t := BenchTuning()
	t.L0CompactionConcurrency = pebbleL0CompactionConcurrency
	t.CompactionDebtConcurrency = pebbleCompactionDebtConcurrency
	t.CompactionGarbageFraction = pebbleCompactionGarbageFraction
	return t
}

// BenchTuning is for measurement: 32 KB blocks (CockroachDB's choice; Pebble's
// default is 4 KB, and larger blocks favour a layout that reads a long run of
// keys, so the block size is a variable to measure and not a given), 64 MB tables
// and a 256 MB level base (Pebble's defaults are 2 MB and 64 MB), a larger
// memtable than its 4 MB, and a cache that holds a small working set. The numbers
// are placeholders until the measurement stage chooses them. It is frozen: the
// benchmark module's numbers and the RocksDB module's mapping rest on it, so a change
// to the product's settings is made to [ProductTuning]. It is also the tuning of a
// Config whose Tuning is zero.
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

	// Logger receives Pebble's messages. Nil is the default, which sends them to
	// log/slog and, on a fatal condition, logs and ends the process with status 70.
	// A test may install one that drops the messages, records them, or panics on a
	// fatal condition instead of ending the process.
	Logger pebble.Logger
	// EventListener, if not nil, is told of the database's events (flushes,
	// compactions, write stalls and the rest), beside the listener of the KV itself,
	// which counts write stalls (see [KV.WriteStalls]): for tests and measurements.
	// Pebble calls some of its functions with its own mutex held, so they must not
	// call back into the database.
	EventListener *pebble.EventListener
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
	// stalls counts the times Pebble began to delay writes (see [KV.WriteStalls]).
	stalls *atomic.Int64
}

// Close closes the database and releases the block cache.
func (k *KV) Close() error {
	k.closeOnce.Do(func() {
		k.closeErr = k.DB.Close()
		k.cache.Unref()
	})
	return k.closeErr
}

// L0Sublevels is how many sublevels level 0 has now, and how many make Pebble stop
// writes (and a flush that rotates the memtable, which checks the same condition)
// until compactions bring them down.
func (k *KV) L0Sublevels() (n, stop int) {
	return int(k.Metrics().Levels[0].Sublevels), k.opts.L0StopWritesThreshold
}

// WriteStalls is how many times since it was opened the database began to delay
// writes, because its memtables were waiting to be flushed or level 0 held too many
// tables.
func (k *KV) WriteStalls() int64 { return k.stalls.Load() }

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
	stalls := new(atomic.Int64)
	opts.AddEventListener(pebble.EventListener{
		WriteStallBegin: func(pebble.WriteStallBeginInfo) { stalls.Add(1) },
	})
	db, err := pebble.Open(dir, opts)
	if err != nil {
		cache.Unref()
		return nil, fmt.Errorf("pebblekv: opening %s: %w", dir, err)
	}
	wo := pebble.NoSync
	if cfg.Sync {
		wo = pebble.Sync
	}
	kv := &KV{DB: db, cfg: cfg, wo: wo, cmp: layout.Comparer.Compare, cache: cache, dir: dir, stalls: stalls}
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
	if err := t.Validate(); err != nil {
		return nil, err
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
	if cfg.Logger != nil {
		log = cfg.Logger
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
		FlushDelayDeleteRange:       t.FlushDelayDeleteRange,
		FS:                          cfg.FS,
		KeySchema:                   chosen.Name,
		KeySchemas:                  sstable.MakeKeySchemas(all...),
		L0StopWritesThreshold:       t.L0StopWritesThreshold, // zero: Pebble's 12
	}
	if t.DisableDeleteOnlyCompactions {
		// Parse sets only the keys it is given, and the value separation policy, which is
		// written out next.
		if err := opts.Parse(disableDeleteOnlyCompactions, nil); err != nil {
			return nil, fmt.Errorf("pebblekv: turning delete-only compactions off: %w", err)
		}
	}
	if lower, upper := t.concurrency(); upper > 0 {
		opts.CompactionConcurrencyRange = func() (int, int) { return lower, upper }
	}
	if t.L0CompactionConcurrency > 0 {
		opts.Experimental.L0CompactionConcurrency = t.L0CompactionConcurrency
	}
	if t.CompactionDebtConcurrency > 0 {
		opts.Experimental.CompactionDebtConcurrency = t.CompactionDebtConcurrency
	}
	if f := t.CompactionGarbageFraction; f > 0 {
		opts.Experimental.CompactionGarbageFractionForMaxConcurrency = func() float64 { return f }
	}
	// Off, which is Pebble's default at the pinned version, written out so that a
	// future default cannot reshape the tables.
	opts.Experimental.ValueSeparationPolicy = func() pebble.ValueSeparationPolicy {
		return pebble.ValueSeparationPolicy{Enabled: false}
	}
	if cfg.EventListener != nil {
		// A copy, with the functions left nil filled in as Pebble would, so that a
		// background error is still logged once another listener is teed beside it.
		l := *cfg.EventListener
		l.EnsureDefaults(log)
		opts.EventListener = &l
	}
	opts.DisableAutomaticCompactions = cfg.DisableAutoCompactions
	opts.ReadOnly = cfg.ReadOnly
	if cfg.DisableReadCompactions {
		opts.Experimental.ReadSamplingMultiplier = -1
	}
	opts.Levels[0].BlockSize = t.BlockSize
	opts.Levels[0].IndexBlockSize = t.BlockSize
	opts.TargetFileSizes[0] = t.TargetFileSize
	// TargetFileSizes is indexed from the base level, not by level number: [0] is level
	// 0, [1] the base level, [2] the level below it, and so on; Pebble fills each entry
	// left at zero with twice the one before. Only [1] is set by the base-level target,
	// so the levels below the base double it. Left at zero, the options are what they
	// were before the fields.
	if t.LBaseTargetFileSize > 0 {
		opts.TargetFileSizes[1] = t.LBaseTargetFileSize
	}
	// The explicit targets, entry by entry, with the same indexing; the zero entries are
	// left to Pebble's doubling.
	for i, size := range t.TargetFileSizes {
		if size > 0 {
			opts.TargetFileSizes[i] = size
		}
	}
	if cs, set, err := t.Compression.settings(); err != nil {
		return nil, err
	} else if set {
		opts.ApplyCompressionSettings(func() pebble.DBCompressionSettings { return cs })
	}
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
