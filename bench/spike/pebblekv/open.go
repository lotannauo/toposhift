package pebblekv

import (
	"fmt"
	"time"

	"github.com/cockroachdb/pebble/v2/cockroachkvs"
	"github.com/cockroachdb/pebble/v2/sstable/colblk"

	rootkv "github.com/lotannauo/toposhift/internal/store/pebblekv"
)

// The database plumbing is the root module's: how a database is opened, the
// options it is opened with, the settings that size it and the meta helpers are
// defined there, once, so that what the benchmarks measure is what the product
// store runs on. This package adds what only a measurement needs (the cockroachkvs
// layout, the rest and compaction waits, the snapshot, the description and the
// canonical rewrite) on top of it.

// Schema is how a table lays out the keys of a block.
type Schema = rootkv.Schema

const (
	// SchemaCRDB is cockroachkvs's own columnar schema, "crdb1": the roach key,
	// the wall time and the logical time are separate columns, so a block of
	// versions of one key stores the key once and the times as integers.
	SchemaCRDB = rootkv.SchemaCRDB
	// SchemaDefault is Pebble's default columnar schema over the same
	// comparer: the key split at the sentinel into a prefix and a suffix, each
	// stored as bytes.
	SchemaDefault = rootkv.SchemaDefault
)

// Tuning is the part of the Pebble options that sizes things.
type Tuning = rootkv.Tuning

// TinyTuning makes everything small, to exercise flushes, compactions and block
// boundaries with little data.
func TinyTuning() Tuning { return rootkv.TinyTuning() }

// BenchTuning is for measurement; see the root module's definition.
func BenchTuning() Tuning { return rootkv.BenchTuning() }

// Layout is what a layout's keys need from Pebble: the comparer, the key schemas
// its tables may be written in, and the block-property collectors it can use.
type Layout = rootkv.Layout

// CockroachLayout is layout M's: cockroachkvs's comparer, versions as MVCC
// timestamps, either key schema, and the MVCC time-interval collector.
var CockroachLayout = func() Layout {
	def := colblk.DefaultKeySchema(&cockroachkvs.Comparer, 16)
	return rootkv.NewLayout(cockroachkvs.Comparer.Name, &cockroachkvs.Comparer,
		map[Schema]*colblk.KeySchema{SchemaCRDB: &cockroachkvs.KeySchema, SchemaDefault: &def},
		cockroachkvs.BlockPropertyCollectors)
}()

// BytewiseLayout is layout L's: Pebble's default comparer, which orders keys by
// their bytes, and its default columnar key schema over it. It offers no
// block-property filter.
var BytewiseLayout = rootkv.BytewiseLayout

// DefaultSettleDeadline is how long [Config.SettleRetention] waits when
// [Config.SettleDeadline] is zero.
const DefaultSettleDeadline = rootkv.DefaultSettleDeadline

// Config says how a database is opened.
type Config = rootkv.Config

// quietLogger drops Pebble's logging, which a test run does not want, and turns
// a fatal condition into a panic, where the root module's default ends the
// process. The benchmarks' tests rely on the panic.
type quietLogger struct{}

func (quietLogger) Infof(string, ...any)  {}
func (quietLogger) Errorf(string, ...any) {}
func (quietLogger) Fatalf(format string, args ...any) {
	panic(fmt.Sprintf("pebble: "+format, args...))
}

// KV is an open database and the settings its layouts need next to it: the root
// module's, with what a measurement reads off it.
type KV struct {
	*rootkv.KV
	cmp func(a, b []byte) int
	// layoutName is the comparer's name, for [KV.Describe].
	layoutName string
	// options is the full text of the options the database was opened with, once
	// Pebble had filled in its defaults.
	options string
}

// Open opens the database under dir, creating it if there is none. Unless the
// config names a logger, Pebble's messages are dropped and a fatal condition is a
// panic.
func Open(dir string, layout Layout, cfg Config) (*KV, error) {
	if cfg.Logger == nil {
		cfg.Logger = quietLogger{}
	}
	base, err := rootkv.Open(dir, layout, cfg)
	if err != nil {
		return nil, err
	}
	return &KV{KV: base, cmp: layout.Comparer.Compare, layoutName: layout.Name, options: base.Options().String()}, nil
}

// Meta keys hold what a layout needs to survive a reopening. Each layout
// encodes the key for its own comparer; the names and the value forms are
// shared.
const (
	MetaFormat  = rootkv.MetaFormat
	MetaLastSeq = rootkv.MetaLastSeq
	MetaHorizon = rootkv.MetaHorizon
)

// EncodeSeq is the stored form of a sequence number.
func EncodeSeq(seq uint64) []byte { return rootkv.EncodeSeq(seq) }

// DecodeSeq reads a sequence number written by [EncodeSeq]; nil is zero, the
// token of an empty store.
func DecodeSeq(b []byte) (uint64, error) { return rootkv.DecodeSeq(b) }

// EncodeHorizon is the stored form of a retention horizon.
func EncodeHorizon(h time.Time) ([]byte, error) { return rootkv.EncodeHorizon(h) }

// DecodeHorizon reads a horizon written by [EncodeHorizon]; nil is the zero time.
func DecodeHorizon(b []byte) (time.Time, error) { return rootkv.DecodeHorizon(b) }
