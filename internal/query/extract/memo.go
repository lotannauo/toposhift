package extract

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/store"
)

// memoCap is the most a memo holds, in units: each stored answer costs the
// number of records it holds plus one, so an existence answer or an empty read
// costs one. It was sized when a read returned a prefix's whole history, of which
// one hot node's reverse history was the largest an extraction made, at an
// estimated 475,000 records for 110 live pods over 30 days, and holds that and as
// much again; a read now returns its window and seeds, which are far fewer. A read that would pass it is answered
// from the store and not kept, so an answer never depends on the cap, only the
// number of reads that reach the store does.
const memoCap = 1 << 20

// memo is the store an extraction's reader and Finds read through. It keeps the
// answer of a Window, EntityWindow or Alive read, and each fingerprint's answer of
// a WindowSince, so a read the extraction repeats (the same entity as an endpoint
// of several hops, or of several Finds) reaches the store once: a WindowSince
// passes on to the store only the fingerprints it does not hold, once each. It is one extraction's: Extract builds it and drops
// it when it returns.
//
// What it keeps are facts read at one snapshot token, never a derived result,
// so no policy version is needed to keep or serve them. It serves a hit only
// when the store would give the same answer: the token names a snapshot that
// is complete (not Latest, and not above the store's last Seq), and the read's
// layer has no retention horizon. Every other read, and every other method,
// goes to the store. It writes nothing and does not close the store it reads.
type memo struct {
	store.Store
	capacity int

	mu   sync.Mutex
	used int
	m    map[memoKey]memoEntry
}

type memoOp uint8

const (
	opWindow memoOp = iota + 1
	opEntityWindow
	opAlive
	opSince
)

// memoKey is everything a read's answer depends on. The instants are kept as
// (Unix seconds, nanoseconds), never as a time.Time, whose location and
// monotonic reading would split equal instants.
type memoKey struct {
	op           memoOp
	fp           identity.Fingerprint
	dir          store.Direction // zero for the reads that take none
	fromS, toS   int64
	fromNs, toNs int
	layer        catalog.Layer
	asOf         uint64
	payloads     bool // a WindowSince's option
}

// memoEntry is a stored answer: the records of a Window or EntityWindow, one
// fingerprint's answer of a WindowSince, or an Alive's boolean, or the quarantine
// error an Alive returned.
type memoEntry struct {
	recs  []store.Record
	since store.Since
	alive bool
	err   error
}

func newMemo(st store.Store, capacity int) *memo {
	return &memo{Store: st, capacity: capacity, m: map[memoKey]memoEntry{}}
}

func memoErr(op string, err error) error { return fmt.Errorf("extract: memo: %s: %w", op, err) }

func (m *memo) key(op memoOp, fp identity.Fingerprint, d store.Direction, from, to time.Time, s store.Scope) memoKey {
	k := memoKey{op: op, fp: fp, dir: d, layer: s.Layer, asOf: s.AsOf, fromS: from.Unix(), fromNs: from.Nanosecond()}
	if op != opAlive {
		k.toS, k.toNs = to.Unix(), to.Nanosecond()
	}
	return k
}

// get returns the stored answer for k when it may be served, or reports that
// the read must go to the store. A hit is served only for a complete snapshot
// (a Latest read is never stored) and only while the layer has no retention
// horizon, which refuses reads the store must judge itself. The caller holds
// m.mu.
func (m *memo) get(k memoKey) (memoEntry, bool) {
	e, ok := m.m[k]
	if !ok || !m.Store.LayerHorizon(k.layer).IsZero() {
		return memoEntry{}, false
	}
	return e, true
}

// storable reports whether the answer to k may be kept: the token names a
// snapshot that is complete, because it is at or below the last Seq taken
// before the read, and the layer has no horizon.
func (m *memo) storable(k memoKey, last uint64) bool {
	return k.asOf != store.Latest && k.asOf <= last && m.Store.LayerHorizon(k.layer).IsZero()
}

// keep stores e under k if it fits the cap. The caller holds m.mu.
func (m *memo) keep(k memoKey, e memoEntry) {
	cost := len(e.recs) + len(e.since.Seeds) + len(e.since.Window) + 1
	if m.used+cost > m.capacity {
		return
	}
	m.used += cost
	m.m[k] = e
}

func (m *memo) records(ctx context.Context, op string, k memoKey, read func() ([]store.Record, error)) ([]store.Record, error) {
	if err := ctx.Err(); err != nil {
		return nil, memoErr(op, err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if e, ok := m.get(k); ok {
		return slices.Clone(e.recs), nil
	}
	last := m.LastSeq()
	recs, err := read()
	if err != nil {
		return nil, err
	}
	if m.storable(k, last) {
		m.keep(k, memoEntry{recs: slices.Clone(recs)})
	}
	return recs, nil
}

// Window serves from the memo when it may, and otherwise reads the store.
func (m *memo) Window(ctx context.Context, fp identity.Fingerprint, d store.Direction, from, to time.Time, s store.Scope) ([]store.Record, error) {
	return m.records(ctx, "Window", m.key(opWindow, fp, d, from, to, s), func() ([]store.Record, error) {
		return m.Store.Window(ctx, fp, d, from, to, s)
	})
}

// EntityWindow serves from the memo when it may, and otherwise reads the store.
func (m *memo) EntityWindow(ctx context.Context, fp identity.Fingerprint, from, to time.Time, s store.Scope) ([]store.Record, error) {
	return m.records(ctx, "EntityWindow", m.key(opEntityWindow, fp, 0, from, to, s), func() ([]store.Record, error) {
		return m.Store.EntityWindow(ctx, fp, from, to, s)
	})
}

// WindowSince serves each fingerprint from the memo when it may, and reads the
// others from the store in one call, each once however often fps repeats it.
// With no fingerprint to read it makes no store read; with no fingerprints at
// all it asks the store, which still judges the horizon.
func (m *memo) WindowSince(ctx context.Context, fps []identity.Fingerprint, d store.Direction, from, to time.Time, s store.Scope, o store.SinceOptions) ([]store.Since, error) {
	if err := ctx.Err(); err != nil {
		return nil, memoErr("WindowSince", err)
	}
	if len(fps) == 0 {
		return m.Store.WindowSince(ctx, fps, d, from, to, s, o)
	}
	key := func(fp identity.Fingerprint) memoKey {
		k := m.key(opSince, fp, d, from, to, s)
		k.payloads = o.Payloads
		return k
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]store.Since, len(fps))
	var missing []identity.Fingerprint
	at := map[identity.Fingerprint][]int{}
	for i, fp := range fps {
		if e, ok := m.get(key(fp)); ok {
			out[i] = cloneSince(e.since)
			continue
		}
		if _, seen := at[fp]; !seen {
			missing = append(missing, fp)
		}
		at[fp] = append(at[fp], i)
	}
	if len(missing) == 0 {
		return out, nil
	}
	last := m.LastSeq()
	got, err := m.Store.WindowSince(ctx, missing, d, from, to, s, o)
	if err != nil {
		return nil, err
	}
	for j, fp := range missing {
		if k := key(fp); m.storable(k, last) {
			m.keep(k, memoEntry{since: cloneSince(got[j])})
		}
		for n, i := range at[fp] {
			if n == 0 {
				out[i] = got[j]
			} else {
				out[i] = cloneSince(got[j])
			}
		}
	}
	return out, nil
}

// cloneSince copies an answer's lists, so a caller that changes them changes
// nothing kept.
func cloneSince(s store.Since) store.Since {
	return store.Since{Seeds: slices.Clone(s.Seeds), Window: slices.Clone(s.Window)}
}

// Alive serves from the memo when it may, and otherwise reads the store. A
// quarantine is an answer and is kept; any other error is not.
func (m *memo) Alive(ctx context.Context, fp identity.Fingerprint, t time.Time, s store.Scope) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, memoErr("Alive", err)
	}
	k := m.key(opAlive, fp, 0, t, time.Time{}, s)
	m.mu.Lock()
	defer m.mu.Unlock()
	if e, ok := m.get(k); ok {
		return e.alive, e.err
	}
	last := m.LastSeq()
	alive, err := m.Store.Alive(ctx, fp, t, s)
	var q *store.QuarantineError
	if err != nil && !errors.As(err, &q) {
		return alive, err
	}
	if m.storable(k, last) {
		m.keep(k, memoEntry{alive: alive, err: err})
	}
	return alive, err
}

// Write refuses: an extraction only reads.
func (m *memo) Write(context.Context, []store.Record) error {
	return fmt.Errorf("extract: memo: Write: the memo is read-only: %w", store.ErrInvalid)
}

// Retain refuses: an extraction only reads.
func (m *memo) Retain(context.Context, time.Time) error {
	return fmt.Errorf("extract: memo: Retain: the memo is read-only: %w", store.ErrInvalid)
}

// Close does nothing: the memo does not own the store it reads.
func (m *memo) Close() error { return nil }
