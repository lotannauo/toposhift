package pebblestore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/v2"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/pebblekv"
)

// formatTag identifies the layout and its encodings in a database's meta: the key
// layout, the record value, the stamp and the meta keys, each by version. A
// database written under another tag is refused, whatever its other contents.
const formatTag = "toposhift/store/log;key=1;record=1;stamp=1;meta=1"

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
	// beforeRetainChunk is the test hook of the same name in Options.
	beforeRetainChunk func()

	beforeRecordApply, afterRecordApply, rereadFails func() error
	beforeHorizonApply, afterHorizonApply            func() error
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
	// states is what the writer remembers of each prefix (see checkpoint.go).
	// anyCkpt says a checkpoint may be in the database (one was, or a commit that
	// reported failure may have landed), in which case a prefix first touched is
	// read for the checkpoints it holds, and a retention deletes the one at its
	// horizon. flagOnDisk says the meta key that records it is durably there; until
	// a commit carrying it succeeds, every checkpoint batch carries it (see
	// applyCheckpoints).
	//
	// complete says the map is not only right about the prefixes in it but
	// exhaustive: a prefix it lacks holds no record and no checkpoint, so the
	// writer knows its state (empty) without reading it. It holds from the opening
	// of a database with no data, and from the end of a retention that worked out
	// the state of every prefix it rewrote in its own pass (see Retain), if it held
	// before or if the retention rewrote every layer. It does not hold, and the
	// prefixes are read as they are touched, in a database that already holds data
	// when it is opened; after a retention that does not work the state out (no
	// checkpoint in the database yet, the policy off, a horizon after the range, a
	// key that cannot be read, a store that always reads whole prefixes (tests), a
	// retention that stops or fails, or one that Open finishes for an earlier
	// process), which forgets the layers it rewrote and keeps what it knew of the
	// others; after a retention that leaves a layer alone, if it did not hold
	// before, since the pass vouches for no prefix of that layer; and after anything
	// that drops what is remembered (a failed commit or read, or a Write that does
	// not finish once it has begun to touch state), and after a write with
	// checkpoints off and none in the database, which stores records and remembers
	// nothing, so the prefixes it leaves are read if a checkpoint is later written.
	// A horizon before the first instant changes nothing and forgets nothing.
	//
	// The price is memory: one entry per live prefix, kept for as long as the store
	// is open, where the map would otherwise hold only the prefixes touched since
	// the last retention.
	states     map[string]*prefixState
	complete   bool
	anyCkpt    bool
	flagOnDisk bool
	// iterators counts the iterators the Write in progress opens for its state.
	iterators int64
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
// upgrade yet), or created with another lifecycle boot key; or if it holds data
// keys and no format; or if it holds the marker of a retention that did not finish
// and the marker disagrees with the horizons stored beside it. A database that
// holds such a marker has the retention finished, in the caller's goroutine, before
// Open returns (see [Store.Retain]). That work can fail, for instance on a key in
// the range that cannot be parsed, and then Open fails with it, loudly: the store
// is not handed out half retained. Only a database opened read-only still opens, and
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
		beforeHorizonApply: o.beforeHorizonApply, afterHorizonApply: o.afterHorizonApply,
		beforeCheckpointApply: o.beforeCheckpointApply, afterCheckpointApply: o.afterCheckpointApply,
		ckpt: ckpt, fullStateRead: o.fullStateRead, states: map[string]*prefixState{},
		offsets: o.Offsets, keep: o.Keep,
		retention: o.Retention, retainerExited: o.retainerExited, beforeRetainChunk: o.beforeRetainChunk,
	}
	s.policy.Rank = maps.Clone(o.Policy.Rank)
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
	s.clock = time.Now
	if o.clock != nil {
		s.clock = o.clock
	}
	if s.retainBytes == 0 {
		s.retainBytes = defaultRetainBatchBytes
	}
	if s.chunkTime == 0 {
		s.chunkTime = defaultRetainChunkTime
		if o.retainStopAfter != 0 || o.resumeStopAfter != 0 {
			// A retention told to fail at its k-th commit must fail at a place that does
			// not depend on how fast the machine is, so its chunks end by size alone.
			s.chunkTime = time.Duration(1<<63 - 1)
		}
	}
	unfinished, err := s.load()
	if err != nil {
		s.abandon()
		_ = kv.Close()
		return nil, err
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
	if s.cancel != nil {
		s.startRetainer()
	}
	return s, nil
}

// abandon releases what a store that failed to open holds besides the database.
func (s *Store) abandon() {
	if s.cancel != nil {
		s.cancel()
	}
}

// load reads, or on a new database writes, the meta keys. It returns the marker of
// a retention that did not finish, if the database holds one, after checking that
// it agrees with the horizons stored beside it.
func (s *Store) load() (*retainMarker, error) {
	format, err := s.kv.GetMeta(metaKey(pebblekv.MetaFormat))
	if err != nil {
		return nil, err
	}
	pebbleNow := pebbleTag(linkedPebbleVersion(), s.kv.FormatMajorVersion())
	switch {
	case format == nil:
		// A directory with data keys and no format is not a new database but
		// somebody's, and claiming it would mix two layouts in one keyspace.
		lo, hi := dataBounds()
		it, err := s.kv.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
		if err != nil {
			return nil, err
		}
		held := it.First()
		if err := errors.Join(it.Error(), it.Close()); err != nil {
			return nil, err
		}
		if held {
			return nil, fmt.Errorf("pebblestore: Open: the directory holds data keys and no format: %w", store.ErrInvalid)
		}
		// A new database: everything that is fixed at creation goes in one commit,
		// so that a database with a format has all of it.
		b := s.kv.NewBatch()
		defer func() { _ = b.Close() }()
		if err := b.Set(metaKey(pebblekv.MetaFormat), []byte(formatTag), nil); err != nil {
			return nil, err
		}
		if err := b.Set(metaKey(metaPebble), pebbleNow, nil); err != nil {
			return nil, err
		}
		if s.policy.BootKey != "" {
			if err := b.Set(metaKey(metaBootKey), []byte(s.policy.BootKey), nil); err != nil {
				return nil, err
			}
		}
		if err := s.kv.Apply(b, pebble.Sync); err != nil {
			return nil, err
		}
	case string(format) != formatTag:
		return nil, fmt.Errorf("pebblestore: Open: the database holds format %q, this is %q: %w", format, formatTag, store.ErrInvalid)
	default:
		// There is no option to upgrade a database to another Pebble yet, so a
		// database made by another version, or at another format major version,
		// is refused.
		made, err := s.kv.GetMeta(metaKey(metaPebble))
		if err != nil {
			return nil, err
		}
		if string(made) != string(pebbleNow) {
			return nil, fmt.Errorf("pebblestore: Open: the database was made by %q, this is %q: %w", made, pebbleNow, store.ErrInvalid)
		}
	}
	// An absent bootKey is the zero policy's, which is what a database created
	// without one has, so any mismatch is a refusal.
	bootKey, err := s.kv.GetMeta(metaKey(metaBootKey))
	if err != nil {
		return nil, err
	}
	if string(bootKey) != string(s.policy.BootKey) {
		return nil, fmt.Errorf("pebblestore: Open: the database was created with boot key %q, not %q: %w", bootKey, s.policy.BootKey, store.ErrInvalid)
	}
	raw, err := s.kv.GetMeta(metaKey(pebblekv.MetaLastSeq))
	if err != nil {
		return nil, err
	}
	seq, err := pebblekv.DecodeSeq(raw)
	if err != nil {
		return nil, err
	}
	s.lastSeq.Store(seq)
	var hs [layers]store.Horizon
	for i := range hs {
		raw, err := s.kv.GetMeta(metaKey(pebblekv.HorizonMetaName(catalog.L0 + catalog.Layer(i))))
		if err != nil {
			return nil, err
		}
		t, hseq, err := pebblekv.DecodeLayerHorizon(raw)
		if err != nil {
			return nil, err
		}
		hs[i] = store.Horizon{Time: t, Seq: hseq}
	}
	s.horizons.Store(&hs)
	rawLast, err := s.kv.GetMeta(metaKey(metaHorizonLast))
	if err != nil {
		return nil, err
	}
	latest := lastHorizon(rawLast, hs)
	s.lastMoved.Store(&latest)
	// A database that holds a checkpoint says so, and this store then deletes the
	// ones a later record makes untrue, whether or not it writes any.
	raw, err = s.kv.GetMeta(metaKey(metaCheckpoints))
	if err != nil {
		return nil, err
	}
	s.anyCkpt = raw != nil
	s.flagOnDisk = s.anyCkpt
	// A database that holds no data has no prefix to learn: every one a write meets
	// is new, and there is no need to read it to find that out. (A store that reads
	// the whole of every prefix, for tests, trusts nothing it has not read.)
	lo, hi := dataBounds()
	it, err := s.kv.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return nil, err
	}
	empty := !it.First()
	if err := errors.Join(it.Error(), it.Close()); err != nil {
		return nil, err
	}
	s.complete = empty && !s.fullStateRead
	// A marker means a retention began and did not finish. Its horizons were
	// committed with it, so they must be the ones stored; a database where they
	// are not is not one this store wrote.
	raw, err = s.kv.GetMeta(metaKey(metaRetain))
	if err != nil {
		return nil, err
	}
	if raw == nil {
		return nil, nil
	}
	m, err := decodeRetainMarker(raw)
	if err != nil {
		return nil, fmt.Errorf("pebblestore: Open: %w: %w", err, store.ErrInvalid)
	}
	s.retainGen = m.generation
	if markerSuperseded(m, hs) {
		// A writer that knew nothing of the marker moved the horizons on: the retention
		// it stands for was overtaken, and whatever it left is below a horizon that is
		// now later in every layer it names. There is nothing to finish; the marker is
		// only removed (a database opened read-only is left as it is).
		if !s.kv.Config().ReadOnly {
			if err := s.releaseMarker(m.generation); err != nil {
				return nil, err
			}
			s.rec.Count("retain.superseded", 1)
		}
		return nil, nil
	}
	if err := checkMarker(m, hs); err != nil {
		return nil, fmt.Errorf("pebblestore: Open: the retention marker %w: %w", err, store.ErrInvalid)
	}
	return &m, nil
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
// waits for at most one chunk). The pass that was pending is not lost: its marker
// stays in the database and the next Open resumes it. If the retainer stopped
// because a commit failed, Close returns that error, after closing the database.
func (s *Store) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	var stopped error
	if s.retainerDone != nil {
		s.cancel()
		s.signal()
		<-s.retainerDone
		s.mu.Lock()
		stopped = s.bgErr
		s.mu.Unlock()
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
// [pebblekv.KV.SettleAfterRetention].
func (s *Store) settleRetention() error {
	cfg := s.kv.Config()
	if !cfg.SettleRetention {
		return nil
	}
	flush, settle, hit, err := s.kv.SettleAfterRetention(cfg.SettleDeadline)
	s.last.flush, s.last.settle, s.last.deadlineHit = flush, settle, hit
	s.rec.Count("retain.flush_ns", flush.Nanoseconds())
	s.rec.Count("retain.settle_ns", settle.Nanoseconds())
	hits := int64(0)
	if hit {
		hits = 1
	}
	s.rec.Count("retain.settle_deadline_hits", hits)
	return err
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
