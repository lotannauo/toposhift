package memstore_test

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

// TestReadsRunAlongsideAWriter exists for the race detector: one writer writes a
// generated stream in batches and retains twice while four readers read. Every
// read must succeed, or be refused for the horizon, and only when the instant or
// the token is below the latest horizon. It does not check that the answers are
// consistent; that is the conformance suite's job.
func TestReadsRunAlongsideAWriter(t *testing.T) {
	t.Parallel()
	c := storetest.Tiny()
	c.Duration = 10 * time.Minute
	c.LateProbability = 0 // a record arriving after Retain must not be before the horizon
	c.Runs = true
	g, err := storetest.NewGenerator(c)
	if err != nil {
		t.Fatal(err)
	}
	s := open(t)
	entities := g.Entities()
	layers := []catalog.Layer{catalog.L0, catalog.L1, catalog.L2, catalog.L3}

	var done atomic.Bool
	var wg sync.WaitGroup
	for reader := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(reader), 7))
			ctx := context.Background()
			var lastSeen uint64
			for reads := 0; reads < 50 || !done.Load(); reads++ {
				seq := s.LastSeq()
				if seq < lastSeen {
					t.Errorf("reader %d: LastSeq went from %d back to %d", reader, lastSeen, seq)
					return
				}
				lastSeen = seq
				fp := entities[rng.IntN(len(entities))]
				when := g.Start().Add(time.Duration(rng.IntN(int(c.Duration/time.Second))) * time.Second)
				asOf := store.Latest
				if reads%2 == 1 {
					asOf = seq
				}
				sc := store.Scope{Layer: layers[rng.IntN(len(layers))], AsOf: asOf}
				var err error
				switch rng.IntN(5) {
				case 0:
					_, err = s.Neighbors(ctx, fp, store.Forward, when, sc)
				case 1:
					_, err = s.NeighborsBatch(ctx, entities[:1+rng.IntN(10)], store.Reverse, when, sc)
				case 2:
					_, err = s.Alive(ctx, fp, when, sc)
				case 3:
					_, err = s.Window(ctx, fp, store.Reverse, when, when.Add(time.Minute), sc)
				default:
					_, err = s.EntityWindow(ctx, fp, when, when.Add(time.Minute), sc)
				}
				if err == nil {
					continue
				}
				// The horizon only moves forward, so the one read afterwards is at least
				// as late as the one the read met.
				h := s.Horizon()
				if !errors.Is(err, store.ErrBeforeHorizon) || (!when.Before(h.Time) && sc.AsOf >= h.Seq) {
					t.Errorf("reader %d: read at %s as of %d: %v (horizon %+v)", reader, when.Format(time.RFC3339), sc.AsOf, err, h)
					return
				}
			}
		}()
	}

	// Whatever the writer does, the readers are stopped and waited for before the
	// test returns: a Fatalf below must not leave them reading a closed store.
	stopReaders := func() {
		done.Store(true)
		wg.Wait()
	}
	defer stopReaders()

	ctx := context.Background()
	retained := 0
	for batches := 0; ; batches++ {
		batch := g.Batch(25)
		if len(batch) == 0 {
			break
		}
		if err := s.Write(ctx, batch); err != nil {
			t.Fatalf("Write: %v", err)
		}
		if batches == 10 || batches == 25 {
			// Nothing later is before the last event time written, as nothing is late.
			h := batch[len(batch)-1].EventTime
			if err := s.Retain(ctx, h); err != nil {
				t.Fatalf("Retain: %v", err)
			}
			g.SetHorizon(h)
			retained++
		}
	}
	stopReaders()
	if retained != 2 {
		t.Errorf("retained %d times, want 2: the stream is too short", retained)
	}
}
