package pebblestore

import (
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
	// horizons are the retention horizons, indexed by layer (L0 first). They move
	// together in this version (no layer has a retention offset or is kept), but
	// every read and write is judged by the horizon of its own layer, through
	// [Store.horizonOf]. A Retain publishes a new array whole, after its commit and
	// before it discards anything.
	horizons atomic.Pointer[[layers]store.Horizon]
	// lastMoved is the horizon the last Retain that moved a layer published.
	lastMoved atomic.Pointer[store.Horizon]
	closed    atomic.Bool

	retainBytes       int
	stopAfter         int
	afterRetainCommit func()

	beforeRecordApply, afterRecordApply, rereadFails func() error
	beforeHorizonApply, afterHorizonApply            func() error
	// checkValue is what the value of a record must pass before the batch is taken in.
	checkValue func(pebblekv.Value) error

	// mu serializes Write and Retain, which the caller is already required to do;
	// it keeps the horizon coherent if a caller forgets. It also guards the fields
	// below.
	mu sync.Mutex
	// failed is set when a commit's outcome could not be learned: the store then
	// refuses every Write and Retain (reads continue) until it is reopened.
	failed error
	// anyCkpt says a checkpoint may be in the database, in which case a retention
	// deletes the one at its horizon. Open refuses such a database for now, so it
	// is false until checkpoints are written.
	anyCkpt bool
	// last is how the last Retain spent its time (see [Instrument.LastRetain]).
	last retainPhases
}

var _ store.Store = (*Store)(nil)

// defaultRetainBatchBytes is how much a retention accumulates before it commits.
// Each prefix's range delete and baseline are always in one commit, and the
// commits fall between prefixes, so the state after any of them is correct for
// every instant at or after the horizon.
const defaultRetainBatchBytes = 4 << 20

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
// upgrade yet), or created with another lifecycle boot key; if it may hold
// checkpoints, which this version cannot keep up to date (it does not invalidate
// them when an older record arrives); or if it holds data keys and no format.
func Open(dir string, o Options) (*Store, error) {
	if o.Retention != 0 && o.Retention != Synchronous {
		return nil, fmt.Errorf("pebblestore: Open: retention mode %d: %w", o.Retention, store.ErrInvalid)
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
		retainBytes: o.retainBatchBytes, stopAfter: o.retainStopAfter, afterRetainCommit: o.afterRetainCommit,
		beforeRecordApply: o.beforeRecordApply, afterRecordApply: o.afterRecordApply, rereadFails: o.rereadFails,
		beforeHorizonApply: o.beforeHorizonApply, afterHorizonApply: o.afterHorizonApply,
	}
	s.policy.Rank = maps.Clone(o.Policy.Rank)
	if o.checkValue != nil {
		s.checkValue = o.checkValue
	}
	if s.rec == nil {
		s.rec = nopRecorder{}
	}
	if s.retainBytes == 0 {
		s.retainBytes = defaultRetainBatchBytes
	}
	if err := s.load(); err != nil {
		_ = kv.Close()
		return nil, err
	}
	return s, nil
}

// load reads, or on a new database writes, the meta keys.
func (s *Store) load() error {
	format, err := s.kv.GetMeta(metaKey(pebblekv.MetaFormat))
	if err != nil {
		return err
	}
	pebbleNow := pebbleTag(linkedPebbleVersion(), s.kv.FormatMajorVersion())
	switch {
	case format == nil:
		// A directory with data keys and no format is not a new database but
		// somebody's, and claiming it would mix two layouts in one keyspace.
		lo, hi := dataBounds()
		it, err := s.kv.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
		if err != nil {
			return err
		}
		held := it.First()
		if err := errors.Join(it.Error(), it.Close()); err != nil {
			return err
		}
		if held {
			return fmt.Errorf("pebblestore: Open: the directory holds data keys and no format: %w", store.ErrInvalid)
		}
		// A new database: everything that is fixed at creation goes in one commit,
		// so that a database with a format has all of it.
		b := s.kv.NewBatch()
		defer func() { _ = b.Close() }()
		if err := b.Set(metaKey(pebblekv.MetaFormat), []byte(formatTag), nil); err != nil {
			return err
		}
		if err := b.Set(metaKey(metaPebble), pebbleNow, nil); err != nil {
			return err
		}
		if s.policy.BootKey != "" {
			if err := b.Set(metaKey(metaBootKey), []byte(s.policy.BootKey), nil); err != nil {
				return err
			}
		}
		if err := s.kv.Apply(b, pebble.Sync); err != nil {
			return err
		}
	case string(format) != formatTag:
		return fmt.Errorf("pebblestore: Open: the database holds format %q, this is %q: %w", format, formatTag, store.ErrInvalid)
	default:
		// There is no option to upgrade a database to another Pebble yet, so a
		// database made by another version, or at another format major version,
		// is refused.
		made, err := s.kv.GetMeta(metaKey(metaPebble))
		if err != nil {
			return err
		}
		if string(made) != string(pebbleNow) {
			return fmt.Errorf("pebblestore: Open: the database was made by %q, this is %q: %w", made, pebbleNow, store.ErrInvalid)
		}
	}
	// An absent bootKey is the zero policy's, which is what a database created
	// without one has, so any mismatch is a refusal.
	bootKey, err := s.kv.GetMeta(metaKey(metaBootKey))
	if err != nil {
		return err
	}
	if string(bootKey) != string(s.policy.BootKey) {
		return fmt.Errorf("pebblestore: Open: the database was created with boot key %q, not %q: %w", bootKey, s.policy.BootKey, store.ErrInvalid)
	}
	raw, err := s.kv.GetMeta(metaKey(pebblekv.MetaLastSeq))
	if err != nil {
		return err
	}
	seq, err := pebblekv.DecodeSeq(raw)
	if err != nil {
		return err
	}
	s.lastSeq.Store(seq)
	var hs [layers]store.Horizon
	for i := range hs {
		raw, err := s.kv.GetMeta(metaKey(pebblekv.HorizonMetaName(catalog.L0 + catalog.Layer(i))))
		if err != nil {
			return err
		}
		t, hseq, err := pebblekv.DecodeLayerHorizon(raw)
		if err != nil {
			return err
		}
		hs[i] = store.Horizon{Time: t, Seq: hseq}
	}
	s.horizons.Store(&hs)
	latest := hs[0]
	for _, h := range hs[1:] {
		if h.Time.After(latest.Time) {
			latest = h
		}
	}
	s.lastMoved.Store(&latest)
	// Nothing here writes checkpoints, and nothing here invalidates one when an
	// older record arrives, so a database that may hold one would be answered
	// from stale summaries.
	raw, err = s.kv.GetMeta(metaKey(metaCheckpoints))
	if err != nil {
		return err
	}
	if raw != nil {
		return fmt.Errorf("pebblestore: Open: the database may hold checkpoints, which this version cannot keep up to date: %w", store.ErrInvalid)
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
// horizon the last Retain that moved any layer published. Nothing stored says
// which layers that Retain moved, so after a reopening it is the latest of the
// layers' stored horizons, which is the same thing while every layer moves
// together, as they do in this version. After Close it returns the value it had.
func (s *Store) Horizon() store.Horizon { return *s.lastMoved.Load() }

// LayerHorizon implements [store.Store]. A layer outside L0 to L3 has the zero
// Horizon. After Close it returns the value it had.
func (s *Store) LayerHorizon(layer catalog.Layer) store.Horizon { return s.horizonOf(layer) }

// Close implements [store.Store]. It closes the database and its cache; a second
// call returns nil. It does not take the lock Write and Retain hold, so it must
// not run while either does: a Retain that settles can hold the database for up to
// its deadline.
func (s *Store) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	return s.kv.Close()
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
