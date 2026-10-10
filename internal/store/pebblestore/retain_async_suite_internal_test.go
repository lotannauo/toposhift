package pebblestore

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/pebblekv"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

// waiting is a store that returns from Retain only when the pass it began is over,
// which is what the suite sees of a store that retains synchronously.
type waiting struct{ *Store }

func (w waiting) Retain(ctx context.Context, h time.Time) error {
	if err := w.Store.Retain(ctx, h); err != nil {
		return err
	}
	return w.Store.Instrument().WaitRetained(ctx)
}

// backgroundFactory opens stores that retain in the background, on in-memory file
// systems that outlive the store (the suite closes and opens a directory again).
func backgroundFactory(ck *CheckpointOptions, wait bool) storetest.Factory {
	var fss sync.Map
	return storetest.Factory{
		Open: func(dir string, p lifecycle.Policy) (store.Store, error) {
			fs, _ := fss.LoadOrStore(dir, vfs.NewMem())
			o := DefaultOptions()
			o.Tuning = pebblekv.TinyTuning()
			o.Sync, o.SettleRetention = false, false
			o.Policy, o.Checkpoints, o.Retention = p, ck, Background
			o.FS = fs.(vfs.FS)
			s, err := Open(dir, o)
			if err != nil {
				return nil, err
			}
			if wait {
				return waiting{s}, nil
			}
			return s, nil
		},
		Durable: true,
	}
}

// The store passes the whole conformance suite retaining in the background: as it
// is, since the contract lets a store discard after Retain returns, and with a
// wrapper that waits for the pass after every Retain, which is what the suite is
// written against.
func TestConformsRetainingInTheBackground(t *testing.T) {
	t.Parallel()
	dflt, k8 := DefaultCheckpoints(), CheckpointOptions{On: true, KMin: 8, Alpha: 1}
	variants := map[string]storetest.Factory{
		"as is/default":        backgroundFactory(&dflt, false),
		"as is/k8":             backgroundFactory(&k8, false),
		"waiting/default":      backgroundFactory(&dflt, true),
		"waiting after Retain": backgroundFactory(&k8, true),
	}
	if raceEnabled {
		// As the conformance suite does under the race detector: almost all of it is one
		// goroutine, where the detector finds nothing a plain build does not and costs
		// minutes. What can race is kept, on two of the variants: the reads beside writes
		// and retentions, Close, the contexts and one workload that quarantines, retained.
		t.Run("as is/default", func(t *testing.T) { t.Parallel(); runBackgroundForTheRaceDetector(t, variants["as is/default"], false) })
		t.Run("waiting after Retain", func(t *testing.T) {
			t.Parallel()
			runBackgroundForTheRaceDetector(t, variants["waiting after Retain"], true)
		})
		return
	}
	for name, f := range variants {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			storetest.Run(t, f)
		})
	}
}

// runBackgroundForTheRaceDetector is the part of storetest.Run that has goroutines or
// a Close to race with, for a race build; with concurrent, the reads beside writes
// and retentions as well.
func runBackgroundForTheRaceDetector(t *testing.T, f storetest.Factory, concurrent bool) {
	t.Helper()
	open := func(t *testing.T, p lifecycle.Policy) store.Store {
		s, err := f.Open(t.TempDir(), p)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = s.Close() })
		return s
	}
	if concurrent {
		t.Run("concurrent reads", func(t *testing.T) {
			t.Parallel()
			if err := storetest.CheckConcurrentReads(open(t, lifecycle.Policy{})); err != nil {
				t.Fatal(err)
			}
		})
	}
	for name, check := range map[string]func(store.Store) error{"close": storetest.CheckClose, "context": storetest.CheckContext} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if err := check(open(t, lifecycle.Policy{})); err != nil {
				t.Fatal(err)
			}
		})
	}
	workloads := storetest.Workloads()
	w := workloads[len(workloads)-1]
	t.Run("workload "+w.Name, func(t *testing.T) {
		t.Parallel()
		if err := storetest.Check(open(t, w.Policy), w, storetest.Options{RetainAt: []float64{0.5}}); err != nil {
			t.Fatal(err)
		}
	})
}

// Writers, readers and a Retain every few batches, in the background, under the race
// detector (the readers take no lock, and the writers take the lock the pass takes
// for each chunk). CheckConcurrentReads is the readers and a writer that retains,
// with every answer checked against the reference at a boundary the reader could have
// seen. The second part has a writer, a goroutine that retains behind the writer's
// progress and readers, all running while the pass does its chunks; at the end the
// store answers as the reference does, holds only true checkpoints and remembers what
// a read finds.
func TestARetentionInTheBackgroundBesideWritersAndReaders(t *testing.T) {
	t.Parallel()
	dflt := DefaultCheckpoints()
	small := CheckpointOptions{On: true, KMin: 2}
	open := func(t *testing.T, ck *CheckpointOptions) *Store {
		o := chunkOptions(vfs.NewMem(), ck)
		o.Retention = Background
		o.retainBatchBytes, o.retainChunkTime = 1, time.Hour
		return mustOpen(t, o)
	}

	t.Run("readers", func(t *testing.T) {
		t.Parallel()
		s := open(t, &dflt)
		if err := storetest.CheckConcurrentReads(s); err != nil {
			t.Fatal(err)
		}
		waitRetained(t, s)
		if err := asyncCheckpointsTrue(s); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("writer, retainer and readers", func(t *testing.T) {
		t.Parallel()
		batches := 400
		if raceEnabled {
			batches = 120
		}
		s := open(t, &small)
		ref := newRef(t)
		w := newAsyncWorld(41)
		fps := w.entities()
		var progress atomic.Int64 // the instant the writer has reached, in nanoseconds after the base
		progress.Store(1000)
		var done atomic.Bool
		errs := &asyncErrs{}
		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			defer wg.Done()
			defer done.Store(true)
			rng := rand.New(rand.NewSource(43))
			var seq uint64
			n := 1000
			for range batches {
				var batch []store.Record
				for range 1 + rng.Intn(3) {
					seq++
					i := rng.Intn(len(w.pods))
					r := edgeAt(layerOfPod(i), w.pods[i], w.nodes[rng.Intn(len(w.nodes))], w.at(n+rng.Intn(3)), seq)
					if rng.Intn(4) == 0 {
						r.Kind, r.Payload = lifecycle.Delete, nil
					}
					batch = append(batch, r)
				}
				n += 3
				for _, x := range []store.Store{ref, s} {
					if err := x.Write(bg, cloneAll(batch)); err != nil {
						errs.add(err)
						return
					}
				}
				progress.Store(int64(n))
				time.Sleep(300 * time.Microsecond) // so that retentions and their passes overlap the writes
			}
		}()

		var retained atomic.Int64
		retainBehind := func() {
			if p := progress.Load(); p > 1100 && p-80 >= retained.Load()+30 {
				h := w.at(int(p) - 80)
				retained.Store(p - 80)
				for _, x := range []store.Store{ref, s} {
					if err := x.Retain(bg, h); err != nil {
						errs.add(err)
					}
				}
			}
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !done.Load() {
				retainBehind()
				time.Sleep(200 * time.Microsecond)
			}
		}()

		for r := range 3 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				rng := rand.New(rand.NewSource(int64(100 + r)))
				for !done.Load() {
					fp := fps[rng.Intn(len(fps))]
					layer := catalog.L0 + catalog.Layer(rng.Intn(4))
					tm := w.at(1000 + rng.Intn(int(progress.Load())-999))
					_, err := s.Neighbors(bg, fp, store.Direction(1+rng.Intn(2)), tm, store.Current(layer))
					if err != nil && !errors.Is(err, store.ErrBeforeHorizon) {
						errs.add(err)
						return
					}
					if _, err := s.Alive(bg, fp, tm, store.Current(layer)); err != nil && !errors.Is(err, store.ErrBeforeHorizon) {
						errs.add(err)
						return
					}
				}
			}()
		}
		wg.Wait()
		errs.check(t)
		retainBehind()
		waitRetained(t, s)
		errs.check(t)

		if m := readMarker(t, s); m != nil {
			t.Errorf("the marker %+v is left", m)
		}
		if retained.Load() <= 1200 || s.LayerHorizon(catalog.L3).IsZero() {
			t.Errorf("the retainer never retained (reached %d)", retained.Load())
		}
		if err := asyncCheckpointsTrue(s); err != nil {
			t.Error(err)
		}
		s.mu.Lock()
		err := asyncStateMismatch(s, false)
		s.mu.Unlock()
		if err != nil {
			t.Error(err)
		}
		times := []time.Time{w.at(int(retained.Load())), w.at(int(retained.Load()) + 17), w.at(int(progress.Load()))}
		if err := asyncAnswers(s, ref, fps, times, []uint64{store.Latest, s.LastSeq()}); err != nil {
			t.Error(err)
		}
	})
}
