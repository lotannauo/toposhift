package backup_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"iter"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/backup"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/segment"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
	"github.com/lotannauo/toposhift/internal/store/pebblekv"
	"github.com/lotannauo/toposhift/internal/store/pebblestore"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

var ctx = context.Background()

// The tests' retention: L0 kept, L1 30 minutes behind the horizon asked for, L2
// and L3 at it.
var (
	testOffsets = [4]time.Duration{0, 30 * time.Minute, 0, 0}
	testKeep    = [4]bool{true, false, false, false}
)

// testOptions are the cutter's options in the tests: a margin of 10 minutes and a
// clock bound of 1, so an Observe may hold up to 9 minutes.
func testOptions(meta backup.StoreMeta) backup.Options {
	return backup.Options{Margin: 10 * time.Minute, MaxClockAhead: time.Minute, Store: meta}
}

func metaFor(p lifecycle.Policy) backup.StoreMeta {
	return backup.StoreMeta{BootKey: p.BootKey, Skew: p.Skew, Offsets: testOffsets, Keep: testKeep}
}

func policyOf(meta backup.StoreMeta) lifecycle.Policy {
	return lifecycle.Policy{BootKey: meta.BootKey, Skew: meta.Skew}
}

func openMem(t testing.TB, meta backup.StoreMeta) *memstore.Store {
	t.Helper()
	s, err := memstore.Open(memstore.Options{Policy: policyOf(meta), Offsets: meta.Offsets, Keep: meta.Keep})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// The engines a backup is restored into.
const (
	engineMem        = "memstore"
	enginePebble     = "pebblestore"
	enginePebbleBack = "pebblestore, background retention"
)

// openEngine opens an empty store of an engine with the meta's policy, offsets and
// kept layers, and a function that waits for its retention to be done.
func openEngine(t testing.TB, engine string, meta backup.StoreMeta) (store.Store, func() error) {
	t.Helper()
	if engine == engineMem {
		return openMem(t, meta), func() error { return nil }
	}
	o := pebblestore.DefaultOptions()
	o.Config = pebblekv.Config{Tuning: pebblekv.TinyTuning()}
	o.Policy = policyOf(meta)
	o.Offsets, o.Keep = meta.Offsets, meta.Keep
	if engine == enginePebbleBack {
		o.Retention = pebblestore.Background
	}
	s, err := pebblestore.Open(t.TempDir(), o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, func() error { return s.Instrument().WaitRetained(ctx) }
}

// restoreInto restores the latest manifest under prefix into a new store of the
// engine.
func restoreInto(t testing.TB, src segment.Store, prefix, engine string, meta backup.StoreMeta, batch int, o ...func(*backup.RestoreOptions)) (store.Store, backup.Result) {
	t.Helper()
	m, err := backup.Latest(ctx, src, prefix)
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	dst, wait := openEngine(t, engine, meta)
	ro := backup.RestoreOptions{Batch: batch, Store: meta, TempDir: t.TempDir()}
	for _, f := range o {
		f(&ro)
	}
	res, err := backup.Restore(ctx, src, m, dst, ro)
	if err != nil {
		t.Fatalf("Restore into %s with batch %d: %v", engine, batch, err)
	}
	if err := wait(); err != nil {
		t.Fatal(err)
	}
	return dst, res
}

// memSegments is a segment store in memory, for tests that cut thousands of files:
// it keeps the contract of segment.Store without a disk's flushes.
type memSegments struct {
	mu sync.Mutex
	m  map[string][]byte
}

func newMemSegments() *memSegments { return &memSegments{m: map[string][]byte{}} }

func (s *memSegments) Put(ctx context.Context, name string, r io.Reader, size int64) (segment.Info, error) {
	if err := segment.ValidName(name); err != nil {
		return segment.Info{}, err
	}
	b, err := io.ReadAll(io.LimitReader(r, segment.MaxSize+1))
	if err != nil {
		return segment.Info{}, err
	}
	if err := ctx.Err(); err != nil {
		return segment.Info{}, err
	}
	switch {
	case len(b) > segment.MaxSize:
		return segment.Info{}, segment.ErrTooLarge
	case size >= 0 && int64(len(b)) != size:
		return segment.Info{}, errors.New("memSegments: the size does not match")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.m[name]; ok {
		return segment.Info{}, segment.ErrExists
	}
	s.m[name] = b
	return segment.Info{Name: name, Size: int64(len(b)), SHA256: sha256.Sum256(b)}, nil
}

func (s *memSegments) Open(_ context.Context, name string) (io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.m[name]
	if !ok {
		return nil, segment.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}

func (s *memSegments) Stat(_ context.Context, name string) (segment.Info, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.m[name]
	if !ok {
		return segment.Info{}, segment.ErrNotFound
	}
	return segment.Info{Name: name, Size: int64(len(b)), SHA256: sha256.Sum256(b)}, nil
}

func (s *memSegments) List(_ context.Context, prefix string) iter.Seq2[segment.Info, error] {
	s.mu.Lock()
	var names []string
	for n := range s.m {
		if strings.HasPrefix(n, prefix) {
			names = append(names, n)
		}
	}
	s.mu.Unlock()
	slices.Sort(names)
	return func(yield func(segment.Info, error) bool) {
		for _, n := range names {
			if !yield(segment.Info{Name: n}, nil) {
				return
			}
		}
	}
}

func (s *memSegments) Delete(_ context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, name)
	return nil
}

// replace puts other bytes under a name.
func (s *memSegments) replace(name string, b []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[name] = b
}

var errFault = errors.New("injected fault")

// faulty fails the Put or Delete calls fail picks.
type faulty struct {
	segment.Store
	fail func(op, name string) bool
}

func (f *faulty) Put(ctx context.Context, name string, r io.Reader, size int64) (segment.Info, error) {
	if f.fail != nil && f.fail("put", name) {
		return segment.Info{}, errFault
	}
	return f.Store.Put(ctx, name, r, size)
}

func (f *faulty) Delete(ctx context.Context, name string) error {
	if f.fail != nil && f.fail("delete", name) {
		return errFault
	}
	return f.Store.Delete(ctx, name)
}

// isFile and isManifest tell a backup's segment names apart.
func isFile(name string) bool     { return strings.Contains(name, "/v1/files/") }
func isManifest(name string) bool { return strings.Contains(name, "/v1/manifests/") }

// reachOf is a record's max(EventTime, Through).
func reachOf(r store.Record) time.Time {
	last, _ := store.LastOf(r.EventTime, r.Through)
	return last
}

// horizons returns a store's four layer horizons.
func horizons(s store.Store) [4]store.Horizon {
	var h [4]store.Horizon
	for l := catalog.L0; l <= catalog.L3; l++ {
		h[l-catalog.L0] = s.LayerHorizon(l)
	}
	return h
}

// liveUpTo is a live store holding the records written through seq, retained as
// the manifest logs: the live store as the backup saw it.
func liveUpTo(t testing.TB, meta backup.StoreMeta, written []store.Record, m backup.Manifest) *memstore.Store {
	t.Helper()
	s := openMem(t, meta)
	var batch []store.Record
	for _, r := range written {
		if r.Seq > m.Seq {
			break
		}
		batch = append(batch, r)
	}
	if len(batch) > 0 {
		if err := s.Write(ctx, batch); err != nil {
			t.Fatal(err)
		}
	}
	if len(m.Retentions) > 0 {
		asked := m.Retentions[0].AskedNS
		for _, r := range m.Retentions {
			asked = max(asked, r.AskedNS)
		}
		if err := s.Retain(ctx, time.Unix(0, asked).UTC()); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

// compare compares a restored store with the live one, and fails the test on any
// difference.
func compare(t testing.TB, live *memstore.Store, restored store.Store, o storetest.RestoredOptions) {
	t.Helper()
	if err := storetest.CompareRestored(live, restored, o); err != nil {
		t.Fatal(err)
	}
}
