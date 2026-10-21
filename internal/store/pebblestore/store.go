package pebblestore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"math"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/v2"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/pebblekv"
	"github.com/lotannauo/toposhift/internal/store/prefixfilter"
)

// formatTag identifies the layout and its encodings in a database's meta: the key
// layout, the record value, the stamp and the meta keys, each by version. A
// database written under another tag is refused, whatever its other contents,
// but for formatTagStamp1.
const formatTag = "toposhift/store/log;key=1;record=1;stamp=2;meta=1"

// formatTagStamp1 is the tag of a database written before stamps of format 2
// (baselines with a boot state). Such a database holds only stamps of format 1,
// which this store reads, and, under a boot key, may hold host prefixes that
// retentions kept whole, which a read folds whole and the next retention rewrites.
// It is accepted, and an Open that can write rewrites its tag to formatTag before
// anything else is written, so that a binary that knows only format 1 refuses it
// from then on; one opened read-only leaves it as it is.
const formatTagStamp1 = "toposhift/store/log;key=1;record=1;stamp=1;meta=1"

// layers is how many layers there are, and so how many horizons a store keeps.
const layers = 4

// Store is the store of layout L: a log per entity, direction and layer, in the
// bytewise key order of Pebble. It implements [store.Store]. Reads may run
// concurrently with each other and with Write and Retain, which the caller
// serializes (the store also serializes them, so a caller that forgets cannot
// corrupt it).
type Store struct {
	kv     *pebblekv.KV
	ids    *pebblekv.IDs
	rec    Recorder
	policy lifecycle.Policy
	// hostType is the id of the host entity type in the two bytes a key carries it
	// in, and hasHostType is false for a catalog without a host type: see foldsWhole.
	hostType    [2]byte
	hasHostType bool

	// lastSeq is the highest Seq committed. It is raised only after the commit
	// that carries it, so a read pinned to it never sees less than it names.
	lastSeq atomic.Uint64
	// horizons are the retention horizons, indexed by layer (L0 first). Every read
	// and write is judged by the horizon of its own layer, through
	// [Store.horizonOf]. A Retain publishes a new array whole, after its commit and
	// before it discards anything.
	horizons atomic.Pointer[[layers]store.Horizon]
	// lastMoved is the horizon the last Retain that moved a layer published.
	lastMoved atomic.Pointer[store.Horizon]
	closed    atomic.Bool
	// offsets and keep are the retention offset of each layer and whether it is
	// kept (see [Options]). They are fixed when the store is opened.
	offsets [layers]time.Duration
	keep    [layers]bool

	retainBytes       int
	chunkTime         time.Duration
	stopAfter         int
	resumeStopAfter   int
	afterRetainCommit func()
	// chunkMinPrefixes is the least a background chunk rewrites before it ends for a
	// waiting write, and writersWaiting how many writes have said they will take the
	// lock and have not yet (see [retainPass.yields]). afterChunkPrefix is the test
	// hook of the same name in Options.
	chunkMinPrefixes int64
	writersWaiting   atomic.Int32
	afterChunkPrefix func(prefixes int64)
	// retainFlushDels is the count of unflushed range deletions at which a pass
	// flushes, none if it is not above zero (see flushBound); fl is the count,
	// guarded by mu. chunkNow is the clock of a chunk's time, and afterChunkOpen,
	// flushHook, beforeFlushWait, beforeApply and beforeKVClose are the test hooks
	// of the same names in Options.
	retainFlushDels int64
	// retainFlushL0Margin and l0Sublevels decide when a flush is put off (see
	// [Store.nearStall]).
	retainFlushL0Margin int
	l0Sublevels         func() (n, stop int)
	fl                  flushBound
	chunkNow            func() time.Time
	afterChunkOpen      func()
	flushHook           func(flush func() (<-chan struct{}, error)) (<-chan struct{}, error)
	beforeFlushWait     func()
	beforeApply         func(*pebble.Batch)
	beforeKVClose       func()

	// retention is the mode Retain works in. In Background mode ctx is the store's
	// own context, which Close cancels, wake tells the retainer there is a pass,
	// and retainerDone is closed when the retainer has exited (nil when none was
	// started, in a store opened read-only). retainerExited is the test hook of
	// the same event.
	retention      RetentionMode
	ctx            context.Context
	cancel         context.CancelFunc
	wake           chan struct{}
	retainerDone   chan struct{}
	retainerExited chan struct{}
	// beforeRetainChunk and betweenSettleRanges are the test hooks of the same names
	// in Options; observer is the Options field of the same name, read without the
	// lock because it is fixed at Open.
	beforeRetainChunk   func()
	betweenSettleRanges func(done int)
	observer            RetainObserver

	beforeRecordApply, afterRecordApply, rereadFails func() error
	beforeHorizonApply, afterHorizonApply            func() error
	// beforeTagApply is the test hook of the same name in Options.
	beforeTagApply func() error
	// beforeCheckpointApply and afterCheckpointApply are the hooks of the checkpoint
	// commit (see [Options]).
	beforeCheckpointApply, afterCheckpointApply func() error
	// checkValue is what the value of a record must pass before the batch is taken in.
	checkValue func(pebblekv.Value) error

	// ckpt is the policy checkpoints are written by.
	ckpt CheckpointOptions
	// fullStateRead makes the writer read the whole of a prefix to learn its state
	// and never trust the map to be complete: for tests that compare the two.
	fullStateRead bool
	// timed says the store has a recorder to give the phases of a Write to, and so
	// reads the clock; with none it never does.
	timed bool
	clock func() time.Time
	// stamp1 says the database's format tag is formatTagStamp1, which an Open that
	// can write rewrites before anything else.
	stamp1 bool

	// mu serializes Write and Retain, which the caller is already required to do;
	// it keeps the horizon coherent if a caller forgets. It also guards the fields
	// below.
	mu sync.Mutex
	// failed is set when a commit's outcome could not be learned: the store then
	// refuses every Write and Retain (reads continue) until it is reopened.
	failed error
	// retainGen is the generation of the last retention that published a marker, or
	// of the marker Open found: the next retention's is one more. It is not stored
	// when no marker is, which is when it does not matter.
	retainGen uint64
	// states, filter and deltas are the writer's map (see prefixstate.go): the
	// cache of what the writer remembers of the prefixes it touched most recently
	// (see checkpoint.go), the filter of the stored prefixes the last scan
	// installed (nil before the first), and the exact sets of the prefixes written
	// since, oldest first. anyCkpt says a checkpoint may be in the database (one was,
	// or a commit that reported failure may have landed), in which case a prefix
	// whose state is not kept is read for the checkpoints it holds, and a retention
	// deletes the one at its horizon. flagOnDisk says the meta key that records it is
	// durably there; until a commit carrying it succeeds, every checkpoint batch
	// carries it (see applyCheckpoints).
	//
	// complete says a filter is installed and every prefix created since its scan
	// began is in a live delta: a prefix in neither holds no key, so the writer
	// knows its state (empty) without reading it. Only the install of a scan's
	// filter makes it hold, and only if nothing was lost since the scan began. It
	// does not hold before the first scan of a store opened over data, and stops
	// holding, until a scan that begins after, on anything that drops what is
	// remembered (lose): a failed commit or read, a Write that does not finish once
	// it has begun to touch state, a retention that rewrites a layer, and a write
	// with checkpoints off and none in the database, which stores records and
	// remembers nothing. lost counts those, so that a scan that began before one
	// does not call the map complete.
	states     *stateCache
	filter     *prefixfilter.Filter
	deltas     []*delta
	complete   bool
	lost       uint64
	anyCkpt    bool
	flagOnDisk bool
	// scan is the scan of the stored prefixes that is the store's, if one is
	// running (see startScan); scanWait is closed, and set to nil, when there is
	// none any more, to release the callers of WaitScanned. scanCtx is cancelled by
	// Close, which stops every scan, and scans and liveScans count the goroutines
	// of the scans, the ones stopped included, which Close waits for.
	scan      *scanRun
	scanWait  chan struct{}
	scanCtx   context.Context
	scanStop  context.CancelFunc
	scans     sync.WaitGroup
	liveScans atomic.Int32
	// laterCounts are the counts of the scans, kept for the caller's next call (see
	// countLater).
	laterCounts map[string]int64
	// filterMaybe makes the filter answer that every prefix may hold keys, for tests
	// and measurements (see Instrument). scanHook is the test hook of the same name
	// in Options.
	filterMaybe bool
	scanHook    func(at scanPoint, hops int, last []byte)
	// iterators counts the iterators the Write in progress opens for its state.
	iterators int64
	// onInvalidate is the hook of [Instrument.OnInvalidate], nil unless a test sets
	// it, and invalidated what the Write in progress has noted for it, which is built
	// only while the hook is set.
	onInvalidate func(prefix []byte, n int)
	invalidated  []invalidation
	// last is how the last Retain spent its time (see [Instrument.LastRetain]).
	last retainPhases
	// pass is the background pass that has not ended, if there is one (see
	// retainPass). passDone is closed, and set to nil, when no pass is pending any
	// more or the store is stopping, to release the callers of WaitRetained; it is
	// not nil while a pass is pending. bgErr is the error that stopped the
	// retainer, which Close reports.
	pass     *retainPass
	passDone chan struct{}
	bgErr    error
}

var _ store.Store = (*Store)(nil)

// defaultRetainBatchBytes is how much a retention accumulates before it ends a
// chunk and commits it. Each prefix's range delete and baseline are always in one
// commit, and the commits fall between prefixes, so the state after any of them is
// correct for every instant at or after the horizon.
const defaultRetainBatchBytes = 4 << 20

// defaultRetainChunkTime is how long a chunk of a retention runs before it ends
// and commits, if it has not reached defaultRetainBatchBytes first. Like the
// bytes, it is checked between prefixes, so a chunk is as long as its last prefix
// makes it.
const defaultRetainChunkTime = 20 * time.Millisecond

// defaultRetainMinPrefixes is how many prefixes a background chunk rewrites at
// least before it may end for a waiting write (see [retainPass.yields]). The gate
// reads the median of a chunk's prefixes at the bottom of its histogram bucket
// (eight buckets to a doubling), where 100 is in the bucket from 96; 128 is the
// lower edge of a bucket, above 104, so chunks of exactly the minimum are read at
// the minimum.
const defaultRetainMinPrefixes = 128

// retainYieldSpins bounds how many times the retainer yields the processor, before
// it takes the lock for its next chunk, to a write that is waiting for the lock.
const retainYieldSpins = 64

var errInjected = errors.New("pebblestore: injected failure")

// retainPhases is how a call of Retain spent its time.
type retainPhases struct {
	work, flush, settle time.Duration
	deadlineHit         bool
}

// Open opens the store under dir, new or as an earlier one left it: its records,
// its last sequence number and its retention horizons. A database is refused (with
// an error wrapping [store.ErrInvalid]) if it was written in another format, by
// another Pebble version or at another format major version (there is no option to
// upgrade yet), or created with another lifecycle boot key or skew; or if it holds
// data keys and no format; or if it holds the marker of a retention that did not
// finish and the marker disagrees with the horizons stored beside it. A database that
// holds such a marker has the retention finished, in the caller's goroutine, before
// Open returns (see [Store.Retain]). That work can fail, for instance on a key in
// the range that cannot be parsed, and then Open fails with it, loudly: the store
// is not handed out half retained; and it can wait, without end, for a flush that
// keeps failing (see [Store.Retain]). Only a database opened read-only still opens, and
// is left as it is. A marker whose horizons the stored ones have all moved past (a
// writer that did not know the marker moved them) is removed and counted as
// "retain.superseded". A database that holds checkpoints opens, whatever
// [Options.Checkpoints] says: the store deletes the ones a later record makes
// untrue even when it writes none. An earlier version of this package, which
// neither wrote nor invalidated them, refuses such a database.
func Open(dir string, o Options) (*Store, error) {
	for i, off := range o.Offsets {
		if off < 0 {
			return nil, fmt.Errorf("pebblestore: Open: the retention offset of layer %s is %s, which is negative: %w",
				catalog.L0+catalog.Layer(i), off, store.ErrInvalid)
		}
	}
	if o.Retention != 0 && o.Retention != Synchronous && o.Retention != Background {
		return nil, fmt.Errorf("pebblestore: Open: retention mode %d: %w", o.Retention, store.ErrInvalid)
	}
	ckpt := DefaultCheckpoints()
	if o.Checkpoints != nil {
		ckpt = *o.Checkpoints
	}
	if err := ckpt.validate(); err != nil {
		return nil, fmt.Errorf("pebblestore: Open: %w", err)
	}
	if o.StateCacheBytes < 0 {
		return nil, fmt.Errorf("pebblestore: Open: the state cache of %d bytes is negative: %w", o.StateCacheBytes, store.ErrInvalid)
	}
	if o.RetainFlushDels < 0 {
		return nil, fmt.Errorf("pebblestore: Open: the retention flush threshold of %d range deletions is negative: %w", o.RetainFlushDels, store.ErrInvalid)
	}
	cacheBytes := o.StateCacheBytes
	if cacheBytes == 0 {
		cacheBytes = DefaultStateCacheBytes
	}
	// Fold validates the policy before anything else, and the check itself is not
	// exported.
	if _, err := lifecycle.Fold(nil, o.Policy); err != nil {
		return nil, fmt.Errorf("pebblestore: Open: %w: %w", store.ErrInvalid, err)
	}
	cfg := o.Config
	if cfg.Schema == 0 {
		cfg.Schema = pebblekv.SchemaDefault
	}
	kv, err := pebblekv.Open(dir, pebblekv.BytewiseLayout, cfg)
	if err != nil {
		return nil, err
	}
	s := &Store{
		kv: kv, ids: pebblekv.Default, checkValue: pebblekv.Value.Check, rec: o.Recorder, policy: o.Policy,
		retainBytes: o.retainBatchBytes, chunkTime: o.retainChunkTime, stopAfter: o.retainStopAfter,
		resumeStopAfter: o.resumeStopAfter, afterRetainCommit: o.afterRetainCommit,
		beforeRecordApply: o.beforeRecordApply, afterRecordApply: o.afterRecordApply, rereadFails: o.rereadFails,
		beforeHorizonApply: o.beforeHorizonApply, afterHorizonApply: o.afterHorizonApply, beforeTagApply: o.beforeTagApply,
		beforeCheckpointApply: o.beforeCheckpointApply, afterCheckpointApply: o.afterCheckpointApply,
		ckpt: ckpt, fullStateRead: o.fullStateRead,
		offsets: o.Offsets, keep: o.Keep,
		retention: o.Retention, retainerExited: o.retainerExited, beforeRetainChunk: o.beforeRetainChunk,
		betweenSettleRanges: o.betweenSettleRanges, observer: o.Observer,
		scanHook: o.scanHook,
		chunkNow: o.chunkNow, afterChunkOpen: o.afterChunkOpen, afterChunkPrefix: o.afterChunkPrefix, flushHook: o.flushHook,
		beforeFlushWait: o.beforeFlushWait, beforeApply: o.beforeApply, beforeKVClose: o.beforeKVClose,
	}
	s.policy.Rank = maps.Clone(o.Policy.Rank)
	if id, ok := s.ids.EntityID(catalog.Host); ok {
		binary.BigEndian.PutUint16(s.hostType[:], uint16(id))
		s.hasHostType = true
	}
	if s.retention == Background && !cfg.ReadOnly {
		s.ctx, s.cancel = context.WithCancel(context.Background())
		s.wake = make(chan struct{}, 1)
	}
	if o.checkValue != nil {
		s.checkValue = o.checkValue
	}
	if s.rec == nil {
		s.rec = nopRecorder{}
	} else {
		// Only a store that has a recorder reads the clock for the phases of a Write.
		s.timed = true
	}
	s.states = newStateCache(cacheBytes, s.rec)
	s.scanCtx, s.scanStop = context.WithCancel(context.Background())
	if !cfg.ReadOnly {
		s.newDelta() // the delta of the opening, which the writes before the first scan add to
	}
	s.clock = time.Now
	if o.clock != nil {
		s.clock = o.clock
	}
	if s.retainBytes == 0 {
		s.retainBytes = defaultRetainBatchBytes
	}
	if s.chunkNow == nil {
		s.chunkNow = time.Now
	}
	s.retainFlushL0Margin = o.retainFlushL0Margin
	if s.retainFlushL0Margin == 0 {
		s.retainFlushL0Margin = defaultRetainFlushL0Margin
	}
	s.l0Sublevels = o.l0Sublevels
	if s.l0Sublevels == nil {
		s.l0Sublevels = kv.L0Sublevels
	}
	// The test seam, which may turn the flushes off, wins over the option.
	flushDels := o.retainFlushDels
	if flushDels == 0 {
		flushDels = o.RetainFlushDels
	}
	switch {
	case flushDels == 0:
		s.retainFlushDels = defaultRetainFlushDels
	case flushDels > 0:
		s.retainFlushDels = int64(flushDels)
	}
	s.chunkMinPrefixes = int64(o.retainMinPrefixes)
	if s.chunkMinPrefixes == 0 {
		s.chunkMinPrefixes = defaultRetainMinPrefixes
	}
	if s.chunkTime == 0 {
		s.chunkTime = defaultRetainChunkTime
		if o.retainStopAfter != 0 || o.resumeStopAfter != 0 {
			// A retention told to fail at its k-th commit must fail at a place that does
			// not depend on how fast the machine is, so its chunks end by size alone.
			s.chunkTime = time.Duration(1<<63 - 1)
		}
	}
	unfinished, empty, err := s.load()
	if err != nil {
		s.abandon()
		_ = kv.Close()
		return nil, err
	}
	if s.stamp1 && !kv.Config().ReadOnly {
		// Before anything else is written: a retention finished below may write a
		// stamp of format 2.
		if err := s.upgradeFormatTag(); err != nil {
			s.abandon()
			_ = kv.Close()
			return nil, err
		}
	}
	if unfinished != nil && !kv.Config().ReadOnly {
		// A retention of an earlier process did not finish: its horizons are
		// published and its marker says how far it got. It is finished here, before
		// the store is handed out. (A database opened read-only cannot be written to:
		// its reads are right, since the horizons are in force, and the marker waits
		// for a store that can write.)
		if s.retention == Background {
			err = s.resumeInBackground(*unfinished)
		} else {
			err = s.resumeRetention(*unfinished)
		}
		if err != nil {
			s.abandon()
			_ = kv.Close()
			return nil, err
		}
	}
	// The writer's map: a database with no data has the empty filter at once, and
	// is complete; one with data is scanned, now, or, with a background pass
	// pending, when its rewriting is over, and is read as it is touched until then.
	// (A store that reads whole prefixes, for tests, trusts nothing it has not read.)
	if !kv.Config().ReadOnly && !s.fullStateRead {
		s.mu.Lock()
		switch {
		case empty:
			s.openEmpty()
		case s.pass == nil:
			s.startScan()
		}
		s.flushCounts()
		s.mu.Unlock()
	}
	if s.cancel != nil {
		s.startRetainer()
	}
	return s, nil
}

// encodeSkew is the value of the skew meta key: the skew in nanoseconds, 8 bytes
// big endian. It is written only for a skew that is not zero.
func encodeSkew(d time.Duration) []byte {
	return binary.BigEndian.AppendUint64(nil, uint64(d))
}

// decodeSkew reads the skew meta key: zero if it is absent (nil), and an error if
// it is not a positive number of nanoseconds in 8 bytes (zero is never written),
// an empty value included.
func decodeSkew(raw []byte) (time.Duration, error) {
	if raw == nil {
		return 0, nil
	}
	if len(raw) != 8 {
		return 0, fmt.Errorf("the skew key holds %d bytes, not 8", len(raw))
	}
	n := binary.BigEndian.Uint64(raw)
	if n == 0 || n > math.MaxInt64 {
		return 0, fmt.Errorf("the skew key holds %d nanoseconds, which no store writes", n)
	}
	return time.Duration(n), nil
}

// upgradeFormatTag rewrites the tag of a database written before stamps of format
// 2 (see formatTagStamp1), synced, in a commit of its own.
func (s *Store) upgradeFormatTag() error {
	b := s.kv.NewBatch()
	defer func() { _ = b.Close() }()
	if err := b.Set(metaKey(pebblekv.MetaFormat), []byte(formatTag), nil); err != nil {
		return err
	}
	if s.beforeTagApply != nil {
		if err := s.beforeTagApply(); err != nil {
			return err // tests: a commit that failed without landing
		}
	}
	if err := s.kv.Apply(b, pebble.Sync); err != nil {
		return err
	}
	s.stamp1 = false
	return nil
}

// abandon releases what a store that failed to open holds besides the database.
func (s *Store) abandon() {
	if s.cancel != nil {
		s.cancel()
	}
	s.scanStop()
}

// load reads, or on a new database writes, the meta keys. It returns the marker of
// a retention that did not finish, if the database holds one, after checking that
// it agrees with the horizons stored beside it.
func (s *Store) load() (_ *retainMarker, empty bool, err error) {
	format, err := s.kv.GetMeta(metaKey(pebblekv.MetaFormat))
	if err != nil {
		return nil, false, err
	}
	pebbleNow := pebbleTag(linkedPebbleVersion(), s.kv.FormatMajorVersion())
	switch {
	case format == nil:
		// A directory with data keys and no format is not a new database but
		// somebody's, and claiming it would mix two layouts in one keyspace.
		lo, hi := dataBounds()
		it, err := s.kv.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
		if err != nil {
			return nil, false, err
		}
		held := it.First()
		if err := errors.Join(it.Error(), it.Close()); err != nil {
			return nil, false, err
		}
		if held {
			return nil, false, fmt.Errorf("pebblestore: Open: the directory holds data keys and no format: %w", store.ErrInvalid)
		}
		// A new database: everything that is fixed at creation goes in one commit,
		// so that a database with a format has all of it.
		b := s.kv.NewBatch()
		defer func() { _ = b.Close() }()
		if err := b.Set(metaKey(pebblekv.MetaFormat), []byte(formatTag), nil); err != nil {
			return nil, false, err
		}
		if err := b.Set(metaKey(metaPebble), pebbleNow, nil); err != nil {
			return nil, false, err
		}
		if s.policy.BootKey != "" {
			if err := b.Set(metaKey(metaBootKey), []byte(s.policy.BootKey), nil); err != nil {
				return nil, false, err
			}
		}
		if s.policy.Skew != 0 {
			if err := b.Set(metaKey(metaSkew), encodeSkew(s.policy.Skew), nil); err != nil {
				return nil, false, err
			}
		}
		if err := s.kv.Apply(b, pebble.Sync); err != nil {
			return nil, false, err
		}
	case string(format) != formatTag && string(format) != formatTagStamp1:
		return nil, false, fmt.Errorf("pebblestore: Open: the database holds format %q, this is %q: %w", format, formatTag, store.ErrInvalid)
	default:
		s.stamp1 = string(format) == formatTagStamp1
		// There is no option to upgrade a database to another Pebble yet, so a
		// database made by another version, or at another format major version,
		// is refused.
		made, err := s.kv.GetMeta(metaKey(metaPebble))
		if err != nil {
			return nil, false, err
		}
		if string(made) != string(pebbleNow) {
			return nil, false, fmt.Errorf("pebblestore: Open: the database was made by %q, this is %q: %w", made, pebbleNow, store.ErrInvalid)
		}
	}
	// An absent bootKey is the zero policy's, which is what a database created
	// without one has, so any mismatch is a refusal.
	bootKey, err := s.kv.GetMeta(metaKey(metaBootKey))
	if err != nil {
		return nil, false, err
	}
	if string(bootKey) != string(s.policy.BootKey) {
		return nil, false, fmt.Errorf("pebblestore: Open: the database was created with boot key %q, not %q: %w", bootKey, s.policy.BootKey, store.ErrInvalid)
	}
	// So is an absent skew zero's. The boot states of the baselines were folded
	// under the stored one, and a fold under another would not be the fold of the
	// whole history.
	// GetMeta does not tell an empty value from an absent one, so the key is read
	// directly: a skew key that holds nothing is refused, never taken for zero.
	var rawSkew []byte
	switch v, closer, err := s.kv.Get(metaKey(metaSkew)); {
	case errors.Is(err, pebble.ErrNotFound):
	case err != nil:
		return nil, false, err
	default:
		rawSkew = append([]byte{}, v...) // not nil, even when empty
		_ = closer.Close()
	}
	skew, err := decodeSkew(rawSkew)
	if err != nil {
		return nil, false, fmt.Errorf("pebblestore: Open: %w: %w", err, store.ErrInvalid)
	}
	if skew != s.policy.Skew {
		return nil, false, fmt.Errorf("pebblestore: Open: the database was created with a boot skew of %s, not %s: %w", skew, s.policy.Skew, store.ErrInvalid)
	}
	raw, err := s.kv.GetMeta(metaKey(pebblekv.MetaLastSeq))
	if err != nil {
		return nil, false, err
	}
	seq, err := pebblekv.DecodeSeq(raw)
	if err != nil {
		return nil, false, err
	}
	s.lastSeq.Store(seq)
	var hs [layers]store.Horizon
	for i := range hs {
		raw, err := s.kv.GetMeta(metaKey(pebblekv.HorizonMetaName(catalog.L0 + catalog.Layer(i))))
		if err != nil {
			return nil, false, err
		}
		t, hseq, err := pebblekv.DecodeLayerHorizon(raw)
		if err != nil {
			return nil, false, err
		}
		hs[i] = store.Horizon{Time: t, Seq: hseq}
	}
	s.horizons.Store(&hs)
	rawLast, err := s.kv.GetMeta(metaKey(metaHorizonLast))
	if err != nil {
		return nil, false, err
	}
	latest := lastHorizon(rawLast, hs)
	s.lastMoved.Store(&latest)
	// A database that holds a checkpoint says so, and this store then deletes the
	// ones a later record makes untrue, whether or not it writes any.
	raw, err = s.kv.GetMeta(metaKey(metaCheckpoints))
	if err != nil {
		return nil, false, err
	}
	s.anyCkpt = raw != nil
	s.flagOnDisk = s.anyCkpt
	// A database that holds no data has no prefix to scan: every one a write meets
	// is new, and there is no need to read it to find that out (see Open).
	lo, hi := dataBounds()
	it, err := s.kv.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return nil, false, err
	}
	empty = !it.First()
	if err := errors.Join(it.Error(), it.Close()); err != nil {
		return nil, false, err
	}
	// A marker means a retention began and did not finish. Its horizons were
	// committed with it, so they must be the ones stored; a database where they
	// are not is not one this store wrote.
	raw, err = s.kv.GetMeta(metaKey(metaRetain))
	if err != nil {
		return nil, false, err
	}
	if raw == nil {
		return nil, empty, nil
	}
	m, err := decodeRetainMarker(raw)
	if err != nil {
		return nil, false, fmt.Errorf("pebblestore: Open: %w: %w", err, store.ErrInvalid)
	}
	s.retainGen = m.generation
	if markerSuperseded(m, hs) {
		// A writer that knew nothing of the marker moved the horizons on: the retention
		// it stands for was overtaken, and whatever it left is below a horizon that is
		// now later in every layer it names. There is nothing to finish; the marker is
		// only removed (a database opened read-only is left as it is).
		if !s.kv.Config().ReadOnly {
			if err := s.releaseMarker(m.generation); err != nil {
				return nil, false, err
			}
			s.rec.Count("retain.superseded", 1)
		}
		return nil, empty, nil
	}
	if err := checkMarker(m, hs); err != nil {
		return nil, false, fmt.Errorf("pebblestore: Open: the retention marker %w: %w", err, store.ErrInvalid)
	}
	return &m, empty, nil
}

// markerSuperseded says the stored horizon of every layer the marker names is
// later than the marker's and not lower in its sequence number. Any other disagreement (a horizon earlier, unequal in
// its sequence number, or later in some layers and not in others) is not a
// retention that was overtaken, and checkMarker refuses it.
func markerSuperseded(m retainMarker, hs [layers]store.Horizon) bool {
	for _, l := range m.layers {
		// Later in time, and, since any later retention was published with the last
		// sequence number of its moment, at least as high in the sequence.
		if h := hs[int(l.layer)-int(catalog.L0)]; !h.Time.After(l.horizon) || h.Seq < l.last {
			return false
		}
	}
	return true
}

// checkMarker says whether a marker agrees with the stored horizons, and, in its
// rewriting phase, whether its resume key is one the rewriting can start from: a
// key of the data keyspace no longer than a prefix.
func checkMarker(m retainMarker, hs [layers]store.Horizon) error {
	for _, l := range m.layers {
		h := hs[int(l.layer)-int(catalog.L0)]
		if !h.Time.Equal(l.horizon) || h.Seq != l.last {
			return fmt.Errorf("moves layer %d to {%v, %d}, and the stored horizon is {%v, %d}", l.layer, l.horizon, l.last, h.Time, h.Seq)
		}
	}
	if m.phase == phaseRewrite {
		lo, hi := dataBounds()
		if len(m.resume) == 0 || len(m.resume) > prefixLen || bytes.Compare(m.resume, lo) < 0 || bytes.Compare(m.resume, hi) >= 0 {
			return fmt.Errorf("resumes at %x, which is not a place in the data keys", m.resume)
		}
	}
	return nil
}

// pebbleTag is the value of the pebble meta key: the Pebble version and the format
// major version the database was created with.
func pebbleTag(version string, format pebble.FormatMajorVersion) []byte {
	return fmt.Appendf(nil, "pebble=%s;fmv=%d", version, format)
}

// linkedPebbleVersion is the version of Pebble this binary was built with, or
// "unknown" for a binary with no build information.
func linkedPebbleVersion() string {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	for _, dep := range bi.Deps {
		if dep.Path == "github.com/cockroachdb/pebble/v2" {
			return dep.Version
		}
	}
	return "unknown"
}

type nopRecorder struct{}

func (nopRecorder) Count(string, int64)  {}
func (nopRecorder) Sample(string, int64) {}

// horizonOf is the retention horizon of a layer.
func (s *Store) horizonOf(l catalog.Layer) store.Horizon {
	i := int(l) - int(catalog.L0)
	if i < 0 || i >= layers {
		return store.Horizon{}
	}
	return s.horizons.Load()[i]
}

// LastSeq implements [store.Store]. After Close it returns the value it had.
func (s *Store) LastSeq() uint64 { return s.lastSeq.Load() }

// Horizon implements [store.Store]: the horizon of the layer retained most
// recently, and when one Retain moved several, the latest of them. It is the
// horizon the last Retain that moved any layer published, and a reopening reads it
// back from the meta key "horizon/last" (see [lastHorizon]). After Close it returns
// the value it had.
func (s *Store) Horizon() store.Horizon { return *s.lastMoved.Load() }

// lastHorizon is the horizon [Store.Horizon] returns for a database whose layers
// hold the horizons hs and whose "horizon/last" holds raw (nil if the key is
// absent). The stored value is trusted only if it is exactly one of the layers'
// horizons (the same instant and the same Seq) and its Seq is the largest among
// them: then it is the one the last Retain that moved a layer named. Otherwise (the
// key is absent, as in a database from before it; or a Retain by a binary that
// does not write it has moved a layer since; or the bytes are not a horizon) the
// answer is derived from the layers alone: the stored horizon with the largest
// Seq, and among those the latest instant. That is the right answer unless two
// Retains with no write between them moved different layers, which the key exists
// to tell apart, and the next Retain that moves a layer rewrites the key.
func lastHorizon(raw []byte, hs [layers]store.Horizon) store.Horizon {
	derived := hs[0]
	for _, h := range hs[1:] {
		if h.Seq > derived.Seq || (h.Seq == derived.Seq && h.Time.After(derived.Time)) {
			derived = h
		}
	}
	if raw == nil {
		return derived
	}
	t, seq, err := pebblekv.DecodeLayerHorizon(raw)
	if err != nil || seq != derived.Seq {
		return derived // not a horizon, or one that a later Retain has passed
	}
	for _, h := range hs {
		if h.Seq == seq && h.Time.Equal(t) {
			return h
		}
	}
	return derived
}

// LayerHorizon implements [store.Store]. A layer outside L0 to L3 has the zero
// Horizon. After Close it returns the value it had.
func (s *Store) LayerHorizon(layer catalog.Layer) store.Horizon { return s.horizonOf(layer) }

// Close implements [store.Store]. It closes the database and its cache; a second
// call returns nil. It does not take the lock Write and Retain hold, so it must
// not run while either does: a Retain that settles can hold the database for up to
// its deadline.
//
// A Background store first stops its retainer: the store is marked closed, the
// retainer's context is cancelled and Close waits for the retainer to exit, which
// it does at its next chunk boundary (a chunk in progress commits first, so Close
// waits for at most one chunk) or, in a settle, between two of its key ranges, or
// sooner (a compaction that has started is not stopped, and closing the database
// waits for it). The pass that was pending is not lost: its marker stays in the
// database, in its settling phase if the rewriting was over, and the next Open
// resumes it, settling again. If the retainer stopped because a commit failed, Close
// returns that error, after closing the database.
//
// Close also stops a scan of the stored prefixes that is running, between two
// prefixes, and waits for its goroutine to exit before it closes the database. The
// writer's memory is never stored: the next Open scans again.
func (s *Store) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	// A scan is stopped first, between two prefixes, and waited for: its iterator
	// must be closed before the database is.
	s.scanStop()
	var stopped error
	if s.retainerDone != nil {
		s.cancel()
		s.signal()
		<-s.retainerDone
	}
	// No scan starts once the store is closed (startScan, under the lock, looks), so
	// after this one the goroutines of the scans can be waited for.
	s.mu.Lock()
	stopped = s.bgErr
	s.mu.Unlock()
	s.scans.Wait()
	s.mu.Lock()
	s.releaseWriterMap()
	s.mu.Unlock()
	if s.beforeKVClose != nil {
		s.beforeKVClose()
	}
	err := s.kv.Close()
	if stopped != nil {
		return errors.Join(fmt.Errorf("pebblestore: Close: the retention stopped: %w", stopped), err)
	}
	return err
}

// closedError is the error every method that returns one gives after Close.
func closedError(op string) error {
	return fmt.Errorf("pebblestore: %s: %w", op, store.ErrClosed)
}

// contextError is the error a cancelled context gives, wrapping the context's.
func contextError(op string, err error) error {
	return fmt.Errorf("pebblestore: %s: %w", op, err)
}

func formatTime(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

// failedError is the error every Write and Retain gives once a commit's outcome
// has been lost. The caller holds s.mu.
func (s *Store) failedError(op string) error {
	return fmt.Errorf("pebblestore: %s: %w", op, s.failed)
}

// settleRetention is the end of every retention that does not return early, under
// SettleRetention, whether or not it deleted anything: see
// [pebblekv.KV.SettleAfterRetention]. It is the Synchronous mode's, and runs under
// the lock; a Background pass settles by [Store.settleBackground].
func (s *Store) settleRetention() error {
	cfg := s.kv.Config()
	if !cfg.SettleRetention {
		return nil
	}
	flush, settle, hit, err := s.kv.SettleAfterRetention(cfg.SettleDeadline)
	s.last.flush, s.last.settle, s.last.deadlineHit = flush, settle, hit
	s.countSettle(flush, settle, hit)
	return err
}

// countSettle counts a settle: the flush and the wait in nanoseconds, and one if
// the wait reached its deadline.
func (s *Store) countSettle(flush, settle time.Duration, hit bool) {
	s.rec.Count("retain.flush_ns", flush.Nanoseconds())
	s.rec.Count("retain.settle_ns", settle.Nanoseconds())
	hits := int64(0)
	if hit {
		hits = 1
	}
	s.rec.Count("retain.settle_deadline_hits", hits)
}

// stamp is the reading of the clock for an observer of a retention, or the zero time
// when the store has none, in which case the clock is not read.
func (s *Store) stamp() time.Time {
	if s.observer == nil {
		return time.Time{}
	}
	return s.clock()
}

// now is the start of a timed span: the clock's reading, or the zero time when the
// store has no recorder, in which case the clock is never read.
func (s *Store) now() time.Time {
	if !s.timed {
		return time.Time{}
	}
	return s.clock()
}

// since is the nanoseconds from a reading of [Store.now] to now, or zero when the
// store has no recorder.
func (s *Store) since(t time.Time) int64 {
	if !s.timed {
		return 0
	}
	return s.clock().Sub(t).Nanoseconds()
}
