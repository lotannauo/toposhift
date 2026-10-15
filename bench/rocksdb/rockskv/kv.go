// Package rockskv opens a RocksDB database for layout L, and gives the layout the
// calls it makes of Pebble in Pebble's own shapes (an iterator with SetBounds,
// First, SeekGE and Next, a batch with Set, Delete and DeleteRange, a direct Set,
// GetMeta), so that the layout's code is Pebble's with only those calls changed.
//
// It also does what a measurement needs of the database: the settle of a retention,
// the rest a build waits for, a full compaction, a cold start of the block cache,
// the statistics a runner reads (under the names the runner reads them by) and a
// description of how the database is set up.
//
// The options mirror Pebble's as the bench module opens it (see [Open]); what
// RocksDB does that Pebble does not, with those options, is RocksDB's behaviour and
// is counted as such.
package rockskv

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/linxGnu/grocksdb"

	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
)

// DefaultSettleDeadline is how long [Config.SettleRetention] waits when
// [Config.SettleDeadline] is zero: the bench module's, so both engines settle
// against the same deadline.
const DefaultSettleDeadline = pebblekv.DefaultSettleDeadline

// Config says how a database is opened. It has the fields of the bench module's
// Pebble config that layout L uses, and no file system: a RocksDB database lives
// on the real one.
type Config struct {
	// Tuning sizes the database: [pebblekv.BenchTuning] or [pebblekv.TinyTuning],
	// with any block cache size; the zero value is BenchTuning. Any other tuning is
	// refused, because only those two are mapped to RocksDB's options.
	Tuning pebblekv.Tuning
	// Sync makes every commit wait for the log to reach the disk.
	Sync bool
	// DisableAutoCompactions turns off the compactions RocksDB schedules on its own.
	DisableAutoCompactions bool
	// DisableReadCompactions is accepted and recorded: RocksDB has no compactions
	// triggered by reads, so there is nothing to turn off.
	DisableReadCompactions bool
	// SettleRetention makes a retention end only when the database has settled what
	// it wrote (see [KV.SettleAfterRetention]); SettleDeadline bounds the wait, zero
	// being DefaultSettleDeadline.
	SettleRetention bool
	SettleDeadline  time.Duration
	// ReadOnly opens an existing database for reading only: nothing is flushed or
	// compacted, and a write is an error.
	ReadOnly bool
	// Perf samples RocksDB's perf context on every read made through
	// [KV.NewReadIter], under "read.<op>.rocks.<name>" (see [RecordIter]). It is
	// for a counters run only: it locks each read to its thread and counts in
	// RocksDB's hot paths, so a timed run never has it on.
	Perf bool
}

// The mirrored settings that do not come from the tuning: Pebble's values as the
// bench module opens it (pebble v2.1.7's defaults, and the root store's options).
const (
	// memTableStopWrites is Pebble's MemTableStopWritesThreshold: the memtables,
	// full or being filled, at which writes stop.
	memTableStopWrites = 4
	// l0StopWrites is Pebble's default L0StopWritesThreshold.
	l0StopWrites = 12
	// bytesPerSync and walBytesPerSync are Pebble's defaults.
	bytesPerSync    = 512 << 10
	walBytesPerSync = 0
	// backgroundJobs gives one flush and one compaction at a time, as Pebble's
	// default compaction concurrency (1) beside its flush.
	backgroundJobs = 2
	// minWriteBuffer is the smallest memtable RocksDB keeps: it raises a smaller
	// write_buffer_size to it when it opens a database.
	minWriteBuffer = 64 << 10
	// numLevels is the levels of both engines.
	numLevels = 7
	// targetFileMultiplier: Pebble's target file size doubles from one level to
	// the next below the base level. RocksDB applies it from the second level below
	// the base on (see [targetFileBytesFromBase]).
	targetFileMultiplier = 2
)

// What RocksDB does, with these options, that Pebble does not, recorded by
// [KV.Describe] and not mirrored.
const (
	// memtableDelayAt is the count of unflushed memtables at which RocksDB delays
	// writes: max_write_buffer_number less one, when that is above three. Pebble
	// only stops writes, at four. The delays are counted in write_stalls.
	memtableDelayAt = memTableStopWrites - 1
	// maxOpenFiles is RocksDB's default, which keeps every table open; Pebble's is
	// 1000.
	maxOpenFiles = -1
	// pinTopLevelIndex is RocksDB's default for a two-level index: the top level of
	// each table's index stays pinned in the cache, through a cold start.
	pinTopLevelIndex = true
)

// targetFileBytesFromBase is the target size of the files RocksDB writes at each
// level from the base level down, for the levels below level 0: with dynamic level
// sizes it indexes its sizes from the base level, and the first two are both
// target_file_size_base, so they are 2T, 2T, 4T, 8T, ... for the tuning's target
// file size T. Pebble's, at the same levels, are 2T, 4T, 8T, ...: below the base
// level RL's files are half of Pebble's.
func targetFileBytesFromBase(t pebblekv.Tuning) []int64 {
	out := make([]int64, numLevels-1)
	for i := range out {
		switch i {
		case 0, 1:
			out[i] = 2 * t.TargetFileSize
		default:
			out[i] = out[i-1] * targetFileMultiplier
		}
	}
	return out
}

// statisticsLevel is the level RocksDB's statistics are kept at: every counter, no
// timer.
const statisticsLevel = grocksdb.StatisticsLevelExceptTimers

// tuningName is the name of a tuning RL maps, with its block cache size left
// out, or false.
func tuningName(t pebblekv.Tuning) (string, bool) {
	t.CacheBytes = 0
	for name, m := range map[string]pebblekv.Tuning{"bench": pebblekv.BenchTuning(), "tiny": pebblekv.TinyTuning()} {
		m.CacheBytes = 0
		if t == m {
			return name, true
		}
	}
	return "", false
}

// KV is an open database and the settings the layout needs next to it.
type KV struct {
	db        *grocksdb.DB
	opts      *grocksdb.Options
	bbto      *grocksdb.BlockBasedTableOptions
	cache     *grocksdb.Cache
	wbm       *grocksdb.WriteBufferManager
	ro        *grocksdb.ReadOptions
	flushOpts *grocksdb.FlushOptions
	wo        *WriteOptions

	cfg Config
	// cacheBytes is the capacity of the block cache: the tuning's, plus the
	// memtables' charge when the database is written to.
	cacheBytes uint64
	// tuning names the mapped tuning, for the description.
	tuning string
	dir    string
	// cold serializes ColdStart, which changes the cache's capacity and back.
	cold      sync.Mutex
	closeOnce sync.Once
}

// WriteOptions is how a commit is made: synced or not.
type WriteOptions struct {
	wo *grocksdb.WriteOptions
}

// Open opens the database under dir, creating it if there is none (unless it is
// opened read-only). The options mirror Pebble's (see the package's constants):
//
//   - a memtable of the tuning's size, and four of them before writes stop;
//   - the memtables charged to the block cache through a write buffer manager,
//     whose cache has the tuning's size plus four memtables when the database is
//     written to, and the tuning's size alone when it is read only;
//   - level 0 compacted at the tuning's threshold, and writes stopped at Pebble's
//     stop threshold, with no level-0 slowdown before it (Pebble has none) and no
//     stall on pending compaction bytes (RocksDB still delays writes at three
//     unflushed memtables of four, which Pebble does not: see memtableDelayAt);
//   - the level base of the tuning, and the base level's files twice the tuning's
//     target file size, as Pebble's; below it the sizes double from the second
//     level on, so RL's files there are half of Pebble's (see
//     [targetFileBytesFromBase]: which level to match is a choice of the comparison);
//   - blocks and index partitions of the tuning's block size, a two-level index,
//     index blocks in the cache, no filter, Snappy at every level;
//   - one flush and one compaction at a time, and Pebble's sync sizes.
//
// The rest is RocksDB's default, recorded by [KV.Describe].
func Open(dir string, cfg Config) (*KV, error) {
	t := cfg.Tuning
	if t == (pebblekv.Tuning{}) {
		t = pebblekv.BenchTuning()
		cfg.Tuning = t
	}
	name, ok := tuningName(t)
	if !ok {
		return nil, fmt.Errorf("rockskv: tuning %+v is not one RocksDB's options are mapped from (the bench tuning or the tiny one, with any cache size)", t)
	}
	if t.CacheBytes <= 0 {
		return nil, fmt.Errorf("rockskv: a block cache of %d bytes", t.CacheBytes)
	}
	k := &KV{cfg: cfg, tuning: name, dir: dir}
	memtable := max(t.MemTableSize, minWriteBuffer)
	reserve := uint64(memTableStopWrites) * memtable
	k.cacheBytes = uint64(t.CacheBytes)
	if !cfg.ReadOnly {
		k.cacheBytes += reserve
	}
	k.cache = grocksdb.NewLRUCache(k.cacheBytes)

	k.bbto = grocksdb.NewDefaultBlockBasedTableOptions()
	k.bbto.SetBlockCache(k.cache)
	k.bbto.SetBlockSize(t.BlockSize)
	k.bbto.SetIndexType(grocksdb.KTwoLevelIndexSearchIndexType)
	k.bbto.SetMetadataBlockSize(uint64(t.BlockSize))
	k.bbto.SetCacheIndexAndFilterBlocks(true)
	k.bbto.SetPinTopLevelIndexAndFilter(pinTopLevelIndex) // RocksDB's default, written out

	o := grocksdb.NewDefaultOptions()
	k.opts = o
	o.SetCreateIfMissing(!cfg.ReadOnly)
	o.SetInfoLogLevel(grocksdb.WarnInfoLogLevel)
	o.SetKeepLogFileNum(1)
	o.SetBlockBasedTableFactory(k.bbto)
	o.SetWriteBufferSize(t.MemTableSize)
	o.SetMaxWriteBufferNumber(memTableStopWrites)
	if !cfg.ReadOnly {
		k.wbm = grocksdb.NewWriteBufferManagerWithCache(int(reserve), k.cache, false)
		o.SetWriteBufferManager(k.wbm)
	}
	o.SetLevel0FileNumCompactionTrigger(t.L0CompactionThreshold)
	o.SetLevel0SlowdownWritesTrigger(l0StopWrites)
	o.SetLevel0StopWritesTrigger(l0StopWrites)
	o.SetSoftPendingCompactionBytesLimit(0)
	o.SetHardPendingCompactionBytesLimit(0)
	o.SetNumLevels(numLevels)
	o.SetMaxBytesForLevelBase(uint64(t.LBaseMaxBytes))
	o.SetTargetFileSizeBase(uint64(2 * t.TargetFileSize))
	o.SetTargetFileSizeMultiplier(targetFileMultiplier)
	o.SetCompression(grocksdb.SnappyCompression)
	o.SetCompressionPerLevel(slices.Repeat([]grocksdb.CompressionType{grocksdb.SnappyCompression}, numLevels))
	o.SetMaxBackgroundJobs(backgroundJobs)
	o.SetMaxOpenFiles(maxOpenFiles) // RocksDB's default, written out
	o.SetBytesPerSync(bytesPerSync)
	o.SetWALBytesPerSync(walBytesPerSync)
	o.SetDisableAutoCompactions(cfg.DisableAutoCompactions)
	o.EnableStatistics()
	o.SetStatisticsLevel(statisticsLevel)

	var err error
	if cfg.ReadOnly {
		k.db, err = grocksdb.OpenDbForReadOnly(o, dir, false)
	} else {
		k.db, err = grocksdb.OpenDb(o, dir)
	}
	if err != nil {
		k.destroy()
		return nil, fmt.Errorf("rockskv: opening %s: %w", dir, err)
	}
	k.ro = grocksdb.NewDefaultReadOptions()
	k.flushOpts = grocksdb.NewDefaultFlushOptions()
	k.flushOpts.SetWait(true)
	k.wo = &WriteOptions{wo: grocksdb.NewDefaultWriteOptions()}
	k.wo.wo.SetSync(cfg.Sync)
	return k, nil
}

// destroy releases what Open made, the database first.
func (k *KV) destroy() {
	if k.db != nil {
		k.db.Close()
	}
	for _, f := range []func(){
		func() {
			if k.ro != nil {
				k.ro.Destroy()
			}
		},
		func() {
			if k.flushOpts != nil {
				k.flushOpts.Destroy()
			}
		},
		func() {
			if k.wo != nil {
				k.wo.wo.Destroy()
			}
		},
		func() { k.opts.Destroy() },
		func() { k.bbto.Destroy() },
		func() {
			if k.wbm != nil {
				k.wbm.Destroy()
			}
		},
		func() { k.cache.Destroy() },
	} {
		f()
	}
}

// Close closes the database and releases its cache and options. No iterator may
// be open, and nothing may run on the database meanwhile.
func (k *KV) Close() error {
	k.closeOnce.Do(k.destroy)
	return nil
}

// Config returns the settings the database was opened with.
func (k *KV) Config() Config { return k.cfg }

// WriteOptions is how this database commits.
func (k *KV) WriteOptions() *WriteOptions { return k.wo }

// Dir is the directory the database lives in.
func (k *KV) Dir() string { return k.dir }

// Apply commits a batch.
func (k *KV) Apply(b *Batch, wo *WriteOptions) error {
	if err := k.db.Write(wo.wo, b.wb); err != nil {
		return fmt.Errorf("rockskv: commit: %w", err)
	}
	return nil
}

// Set writes one key and value in a commit of its own.
func (k *KV) Set(key, value []byte, wo *WriteOptions) error {
	if err := k.db.Put(wo.wo, key, value); err != nil {
		return fmt.Errorf("rockskv: commit: %w", err)
	}
	return nil
}

// GetMeta returns the value at a meta key, or nil if there is none.
func (k *KV) GetMeta(key []byte) ([]byte, error) {
	h, err := k.db.GetPinned(k.ro, key)
	if err != nil {
		return nil, err
	}
	defer h.Destroy()
	if !h.Exists() {
		return nil, nil
	}
	return slices.Clone(h.Data()), nil
}

// CheckFormat makes sure a database holds the format a layout expects, writing it
// if the database is new, and refuses one written by another layout or another
// version of it, as the bench module's does.
func (k *KV) CheckFormat(key, want []byte) error {
	got, err := k.GetMeta(key)
	if err != nil {
		return err
	}
	switch {
	case got == nil:
		if k.cfg.ReadOnly {
			return errors.New("rockskv: a read-only database holds no format")
		}
		return k.Set(key, want, k.wo)
	case !slices.Equal(got, want):
		return fmt.Errorf("rockskv: the database holds format %q, this is %q", got, want)
	}
	return nil
}

// Flush writes the memtable to a table and waits for it.
func (k *KV) Flush() error {
	if err := k.db.Flush(k.flushOpts); err != nil {
		return fmt.Errorf("rockskv: flush: %w", err)
	}
	return nil
}

// waitForCompact waits for every flush and compaction to finish, at most timeout
// (none when zero), and says whether the timeout ended the wait.
func (k *KV) waitForCompact(timeout time.Duration) (timedOut bool, err error) {
	w := grocksdb.NewWaitForCompactOptions()
	defer w.Destroy()
	w.SetFlush(false)
	if timeout > 0 {
		w.SetTimeout(uint64(max(timeout.Microseconds(), 1)))
	}
	if err := k.db.WaitForCompact(w); err != nil {
		if isTimedOut(err) {
			return true, nil
		}
		return false, fmt.Errorf("rockskv: waiting for compactions: %w", err)
	}
	return false, nil
}

// isTimedOut says an error is RocksDB's Status::TimedOut, as its text gives it.
func isTimedOut(err error) bool {
	return strings.HasPrefix(err.Error(), "Operation timed out")
}

// Settle flushes the memtable and waits for every flush and compaction to finish,
// so what a read crosses stops changing under the caller. A read-only database has
// nothing to settle.
func (k *KV) Settle() error {
	if k.cfg.ReadOnly {
		return nil
	}
	if err := k.Flush(); err != nil {
		return err
	}
	_, err := k.waitForCompact(0)
	return err
}

// SettleAfterRetention is the end of a retention under [Config.SettleRetention]: a
// flush, timed alone, and then RocksDB's wait for its flushes and compactions to
// finish (WaitForCompact, without a flush of its own), bounded by deadline
// (DefaultSettleDeadline when zero). A wait the deadline ends is not an error: it
// returns deadlineHit, and the writer resumes. This is RocksDB's settle, not
// Pebble's (which also waits for table statistics and a quiet second): each
// engine's own settle is part of its retention.
func (k *KV) SettleAfterRetention(deadline time.Duration) (flush, settle time.Duration, deadlineHit bool, err error) {
	if k.cfg.ReadOnly {
		return 0, 0, false, nil
	}
	if deadline == 0 {
		deadline = DefaultSettleDeadline
	}
	start := time.Now()
	if err := k.Flush(); err != nil {
		return time.Since(start), 0, false, err
	}
	flush = time.Since(start)
	start = time.Now()
	hit, err := k.waitForCompact(deadline)
	return flush, time.Since(start), hit, err
}

// restPoll is how long one wait of Quiesce lasts before it looks at its context.
const restPoll = 200 * time.Millisecond

// Quiesce flushes and waits until no flush or compaction is running or due, or
// until ctx is done. A read-only database is at rest.
func (k *KV) Quiesce(ctx context.Context) error {
	if k.cfg.ReadOnly {
		return ctx.Err()
	}
	if err := k.Flush(); err != nil {
		return err
	}
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("rockskv: not at rest: %w", err)
		}
		timedOut, err := k.waitForCompact(restPoll)
		if err != nil || !timedOut {
			return err
		}
	}
}

// CompactAll flushes and compacts every key, the bottom level included, into as
// few tables as RocksDB will, then waits for rest: what makes the size of a
// database repeatable.
func (k *KV) CompactAll(ctx context.Context) error {
	if err := k.Flush(); err != nil {
		return err
	}
	o := grocksdb.NewCompactRangeOptions()
	defer o.Destroy()
	o.SetBottommostLevelCompaction(grocksdb.KForce)
	k.db.CompactRangeOpt(grocksdb.Range{}, o)
	return k.Quiesce(ctx)
}

// ColdStart empties the block cache, keeping the tables open: its capacity goes to
// zero, which evicts every block no reader holds, and back. It must not be called
// while a read is in progress. A block the table reader pins (RocksDB's default
// pins the top level of a two-level index) stays.
func (k *KV) ColdStart() {
	k.cold.Lock()
	defer k.cold.Unlock()
	k.cache.SetCapacity(0)
	k.cache.SetCapacity(k.cacheBytes)
}

// EstimateDiskUsage is an estimate of the bytes of tables that hold the keys in
// [lo, hi): RocksDB's approximate size of the range, which, in a table the range
// covers only part of, is the distance between the offsets of lo and hi in it. Both
// offsets are those of the blocks the keys fall in, so a range whose keys all lie in
// one block of a table comes to nothing, where Pebble's estimate counts the blocks a
// range overlaps. So a range that comes to nothing and still holds a key of a table
// is given one block, the tuning's block size: as small as Pebble's estimate can be
// for a range that holds a key.
func (k *KV) EstimateDiskUsage(lo, hi []byte) (uint64, error) {
	sizes, err := k.db.GetApproximateSizes([]grocksdb.Range{{Start: lo, Limit: hi}})
	if err != nil || sizes[0] > 0 {
		return sizes[0], err
	}
	inTable := slices.ContainsFunc(k.db.GetLiveFilesMetaData(), func(f grocksdb.LiveFileMetadata) bool {
		return bytes.Compare(f.SmallestKey, hi) < 0 && bytes.Compare(f.LargestKey, lo) >= 0
	})
	if !inTable {
		return 0, nil
	}
	it := k.newIter(&IterOptions{LowerBound: lo, UpperBound: hi}, false)
	found := it.First()
	if err := it.Close(); err != nil {
		return 0, err
	}
	if found {
		return uint64(k.cfg.Tuning.BlockSize), nil
	}
	return 0, nil
}

// Size is the bytes the database holds on disk: tables, log, manifest and options,
// and not RocksDB's own message log (LOG), which Pebble does not write.
func (k *KV) Size() (int64, error) {
	var n int64
	err := filepath.WalkDir(k.dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || strings.HasPrefix(d.Name(), "LOG") {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			if errors.Is(err, os.ErrNotExist) { // removed by a compaction meanwhile
				return nil
			}
			return err
		}
		n += info.Size()
		return nil
	})
	return n, err
}
