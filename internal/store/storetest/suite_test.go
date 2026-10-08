package storetest_test

import (
	"context"
	"errors"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

func TestWorkloadsAreTheFixedSet(t *testing.T) {
	t.Parallel()
	ws := storetest.Workloads()
	wantNames := []string{
		"tiny", "late and confirmed", "watch mode, seq across 2^32", "runs and lateness", "busy, seq near 2^63", "reboots and clones",
	}
	if len(ws) != len(wantNames) {
		t.Fatalf("%d workloads, want %d", len(ws), len(wantNames))
	}
	for i, w := range ws {
		if w.Name != wantNames[i] {
			t.Errorf("workload %d is %q, want %q", i, w.Name, wantNames[i])
		}
		if w.Config.Seed != uint64(100+i) {
			t.Errorf("workload %q has seed %d, want %d", w.Name, w.Config.Seed, 100+i)
		}
		if err := w.Config.Validate(); err != nil {
			t.Errorf("workload %q: %v", w.Name, err)
		}
		wantPolicy := lifecycle.Policy{}
		if i == 5 {
			wantPolicy = storetest.QuarantinePolicy()
		}
		if w.Policy.BootKey != wantPolicy.BootKey {
			t.Errorf("workload %q has boot key %q, want %q", w.Name, w.Policy.BootKey, wantPolicy.BootKey)
		}
	}
}

func TestQuarantinePolicyTracksTheBootIDOfARecord(t *testing.T) {
	t.Parallel()
	if got := storetest.QuarantinePolicy(); got.BootKey != lifecycle.BootID || got.Skew != 0 || len(got.Rank) != 0 {
		t.Errorf("QuarantinePolicy() = %+v, want only the boot key %q", got, lifecycle.BootID)
	}
	// The assertion of a record that carries a boot has it under the boot key, and
	// the payload stays an ordinary attribute.
	a := store.Record{Kind: lifecycle.Observe, Payload: []byte("p"), Boot: "boot-a"}.Assertion()
	want := []identity.Attr{{Key: "payload", Value: "p"}, {Key: lifecycle.BootID, Value: "boot-a"}}
	if !slices.Equal(a.Attrs, want) {
		t.Errorf("attrs = %v, want %v", a.Attrs, want)
	}
}

// The workloads must exercise what they are named for.
func TestTheWorkloadsReachWhatTheyAreFor(t *testing.T) {
	t.Parallel()
	ws := storetest.Workloads()
	recordsOf := func(w storetest.Workload) []store.Record {
		g, err := storetest.NewGenerator(w.Config)
		if err != nil {
			t.Fatal(err)
		}
		return g.All()
	}

	// Workload 5 quarantines at least one host, as the reference sees it.
	w := ws[5]
	ref, err := memstore.Open(memstore.Options{Policy: w.Policy})
	if err != nil {
		t.Fatal(err)
	}
	g, err := storetest.NewGenerator(w.Config)
	if err != nil {
		t.Fatal(err)
	}
	if err := ref.Write(bg, g.All()); err != nil {
		t.Fatal(err)
	}
	quarantined := 0
	for _, fp := range g.Entities() {
		if fp.Type() != catalog.Host {
			continue
		}
		if _, err := ref.Alive(bg, fp, w.Config.Start, store.Current(catalog.L1)); isQuarantineError(err) {
			quarantined++
		}
	}
	if quarantined == 0 {
		t.Error("the workload of reboots and clones quarantines no host")
	}

	// Workload 2's sequence numbers cross 2^32, and workload 4's stay below 2^64.
	recs := recordsOf(ws[2])
	if first, last := recs[0].Seq, recs[len(recs)-1].Seq; first >= 1<<32 || last < 1<<32 {
		t.Errorf("workload 2 has seqs %d to %d, want them to cross 2^32", first, last)
	}
	recs = recordsOf(ws[4])
	if first, last := recs[0].Seq, recs[len(recs)-1].Seq; first >= 1<<63 || last < 1<<63 || last == store.Latest {
		t.Errorf("workload 4 has seqs %d to %d, want them to cross 2^63 and stay below 2^64", first, last)
	}
}

func TestTrimmedIsShort(t *testing.T) {
	t.Parallel()
	if got, want := storetest.Trimmed(), testing.Short(); got != want {
		t.Errorf("Trimmed() = %v, want %v", got, want)
	}
}

func TestCheckPassesAnHonestStoreWithRetention(t *testing.T) {
	t.Parallel()
	for _, w := range storetest.Workloads() {
		m, err := memstore.Open(memstore.Options{Policy: w.Policy})
		if err != nil {
			t.Fatal(err)
		}
		if err := storetest.Check(m, w, storetest.Options{RetainAt: []float64{0.4, 0.7}, CheckEvery: 3}); err != nil {
			t.Errorf("workload %q: %v", w.Name, err)
		}
	}
}

func TestRetainAtIsValidated(t *testing.T) {
	t.Parallel()
	open := func() store.Store {
		m, err := memstore.Open(memstore.Options{})
		if err != nil {
			t.Fatal(err)
		}
		return m
	}
	w := storetest.Workloads()[0]
	for _, bad := range [][]float64{{0}, {1}, {-0.1}, {1.5}, {0.5, 0.5}, {0.7, 0.3}} {
		if err := storetest.Check(open(), w, storetest.Options{RetainAt: bad}); err == nil {
			t.Errorf("RetainAt %v was accepted", bad)
		}
	}
	if err := storetest.Check(open(), storetest.Workload{}, storetest.Options{}); err == nil {
		t.Error("an invalid generator config was accepted")
	}
}

// failing wraps a store and makes one method fail, so the suite's own error
// reporting is checked: an error of the store must surface, with its cause.
type failing struct {
	store.Store
	method string
	err    error
}

func (f failing) fail(m string) error {
	if f.method == m {
		return f.err
	}
	return nil
}

func (f failing) Write(ctx context.Context, b []store.Record) error {
	if err := f.fail("Write"); err != nil {
		return err
	}
	return f.Store.Write(ctx, b)
}

func (f failing) Retain(ctx context.Context, h time.Time) error {
	if err := f.fail("Retain"); err != nil {
		return err
	}
	return f.Store.Retain(ctx, h)
}

func (f failing) Alive(ctx context.Context, fp identity.Fingerprint, at time.Time, sc store.Scope) (bool, error) {
	if err := f.fail("Alive"); err != nil {
		return false, err
	}
	return f.Store.Alive(ctx, fp, at, sc)
}

func (f failing) Neighbors(ctx context.Context, fp identity.Fingerprint, d store.Direction, at time.Time, sc store.Scope) ([]store.Neighbor, error) {
	if err := f.fail("Neighbors"); err != nil {
		return nil, err
	}
	return f.Store.Neighbors(ctx, fp, d, at, sc)
}

func (f failing) NeighborsBatch(ctx context.Context, fps []identity.Fingerprint, d store.Direction, at time.Time, sc store.Scope) ([][]store.Neighbor, error) {
	if err := f.fail("NeighborsBatch"); err != nil {
		return nil, err
	}
	return f.Store.NeighborsBatch(ctx, fps, d, at, sc)
}

func (f failing) Window(ctx context.Context, fp identity.Fingerprint, d store.Direction, from, to time.Time, sc store.Scope) ([]store.Record, error) {
	if err := f.fail("Window"); err != nil {
		return nil, err
	}
	return f.Store.Window(ctx, fp, d, from, to, sc)
}

func (f failing) EntityWindow(ctx context.Context, fp identity.Fingerprint, from, to time.Time, sc store.Scope) ([]store.Record, error) {
	if err := f.fail("EntityWindow"); err != nil {
		return nil, err
	}
	return f.Store.EntityWindow(ctx, fp, from, to, sc)
}

func TestStoreErrorsSurface(t *testing.T) {
	t.Parallel()

	boom := errors.New("injected")
	for _, method := range []string{"Write", "Retain", "Alive", "Neighbors", "NeighborsBatch", "Window", "EntityWindow"} {
		m, err := memstore.Open(memstore.Options{})
		if err != nil {
			t.Fatal(err)
		}
		cand := failing{Store: m, method: method, err: boom}
		err = storetest.Check(cand, storetest.Workloads()[0], storetest.Options{RetainAt: []float64{0.5}, CheckEvery: 2})
		if !errors.Is(err, boom) {
			t.Errorf("a failing %s: err = %v, want it to wrap the injected error", method, err)
		}
	}
}

// With no late records, nothing the generator offers after a retention is older
// than it, as long as Check tells it the horizon. Without that, a heartbeating run
// is extended at its start, which the store would refuse.
func TestCheckTellsTheGeneratorTheHorizon(t *testing.T) {
	t.Parallel()
	w := storetest.Workloads()[3]
	w.Config.LateProbability = 0
	m, err := memstore.Open(memstore.Options{Policy: w.Policy})
	if err != nil {
		t.Fatal(err)
	}
	e := &refusing{Store: m}
	if err := storetest.Check(e, w, storetest.Options{RetainAt: []float64{0.3, 0.6}}); err != nil {
		t.Fatal(err)
	}
	if n := e.refused.Load(); n != 0 {
		t.Errorf("%d writes were refused for being before the horizon, in a stream with no late records", n)
	}
}

// closeCounter records how a factory's stores are opened and closed.
type closeCounter struct {
	store.Store
	closes *sync.Map
	dir    string
}

func (c closeCounter) Close() error {
	n, _ := c.closes.LoadOrStore(c.dir, new(int))
	*n.(*int)++
	return c.Store.Close()
}

// Run opens every store in a directory of its own, which is empty, and closes it.
func TestRunOpensEachStoreInItsOwnEmptyDirectoryAndClosesIt(t *testing.T) {
	t.Parallel()
	var (
		mu     sync.Mutex
		opened []string
		closes sync.Map
	)
	f := storetest.Factory{Open: func(dir string, p lifecycle.Policy) (store.Store, error) {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return nil, err
		}
		if len(entries) != 0 {
			t.Errorf("Open was given %s, which holds %d files", dir, len(entries))
		}
		m, err := memstore.Open(memstore.Options{Policy: p})
		if err != nil {
			return nil, err
		}
		mu.Lock()
		opened = append(opened, dir)
		mu.Unlock()
		return closeCounter{Store: m, closes: &closes, dir: dir}, nil
	}}
	// Run's parallel subtests finish before this subtest returns.
	t.Run("suite", func(t *testing.T) { storetest.Run(t, f) })

	seen := map[string]bool{}
	for _, dir := range opened {
		if seen[dir] {
			t.Errorf("directory %s was given to Open twice", dir)
		}
		seen[dir] = true
	}
	// One store for each scripted check and each workload, and one for the
	// concurrent reads; the trimmed tier has one workload and no random ones.
	want := 15
	if storetest.Trimmed() {
		want = 13
	}
	if len(opened) < want {
		t.Errorf("%d stores were opened, want at least %d, one for each scripted check and workload", len(opened), want)
	}
	for _, dir := range opened {
		if _, ok := closes.Load(dir); !ok {
			t.Errorf("the store in %s was never closed", dir)
		}
	}
}
