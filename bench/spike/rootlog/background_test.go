package rootlog_test

import (
	"context"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/rootlog"
	"github.com/lotannauo/toposhift/internal/store/pebblestore"
)

// backgroundOptions open the engine with a retention in the background that settles.
func backgroundOptions(c pebblestore.CheckpointOptions) rootlog.Options {
	o := options(c)
	o.Retention = pebblestore.Background
	o.SettleRetention = true
	return o
}

// phases is a RetainObserver that keeps the generations it was told of, in order.
type phases struct {
	published, rewritten, settled []uint64
}

func (p *phases) Published(gen uint64, _ time.Time)    { p.published = append(p.published, gen) }
func (p *phases) RewriteEnded(gen uint64, _ time.Time) { p.rewritten = append(p.rewritten, gen) }
func (p *phases) SettleEnded(gen uint64, _ time.Time)  { p.settled = append(p.settled, gen) }

// An engine whose retention runs in the background passes the whole conformance
// test, both settings, as the synchronous one does: the answers during and after a
// pass are the oracle's.
func TestConformsRetainingInTheBackground(t *testing.T) {
	t.Parallel()
	for name, c := range settings {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			conformance.Run(t, inMemory(backgroundOptions(c)))
		})
	}
}

// The engine passes the store's retention mode and observer on, and waits for the
// pass and its settle: after WaitRetained the observer has been told all three
// phases of each retention, in order, and the pass is over.
func TestWaitRetainedWaitsForTheStoresPass(t *testing.T) {
	t.Parallel()
	recs := stream(t)
	obs := &phases{}
	o := backgroundOptions(settings["k8a1"])
	o.Observer = obs
	e, err := inMemory(o)(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	if err := e.Write(recs); err != nil {
		t.Fatal(err)
	}
	last := recs[len(recs)-1].EventTime
	if err := e.Retain(last.Add(-2 * time.Minute)); err != nil {
		t.Fatal(err)
	}
	w, ok := e.(interface {
		WaitRetained(ctx context.Context) error
	})
	if !ok {
		t.Fatal("the engine cannot be waited for")
	}
	if err := w.WaitRetained(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(obs.published) != 1 || len(obs.rewritten) != 1 || len(obs.settled) != 1 || obs.published[0] != obs.settled[0] || obs.published[0] != obs.rewritten[0] {
		t.Errorf("told of published %v, rewritten %v, settled %v", obs.published, obs.rewritten, obs.settled)
	}
	_, flush, settle, _ := e.(interface {
		LastRetain() (work, flush, settle time.Duration, deadlineHit bool)
	}).LastRetain()
	if flush <= 0 || settle <= 0 {
		t.Errorf("the pass settled in %s after a flush of %s", settle, flush)
	}

	// A synchronous engine has nothing to wait for, and tells nobody anything.
	s, err := inMemory(options(settings["off"]))(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if err := s.(interface {
		WaitRetained(ctx context.Context) error
	}).WaitRetained(context.Background()); err != nil {
		t.Errorf("WaitRetained of a synchronous engine = %v", err)
	}
}
