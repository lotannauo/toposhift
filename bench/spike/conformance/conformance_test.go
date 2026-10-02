package conformance_test

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/oracle"
	"github.com/lotannauo/toposhift/bench/spike/workload"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// The oracle satisfies its own harness: the checks are consistent.
func TestOracleIsAConformingEngine(t *testing.T) {
	t.Parallel()
	conformance.Run(t, func(string) (engine.Engine, error) { return oracle.New(), nil })
}

// broken wraps a correct engine and damages one behavior. A harness that cannot
// tell is worthless, so each damage must be caught.
type broken struct {
	engine.Engine
	inner *oracle.Oracle

	write     func([]engine.Record) []engine.Record
	neighbors func(inner engine.Engine, fp identity.Fingerprint, dir engine.Direction, t time.Time) ([]engine.Neighbor, error)
	window    func(inner engine.Engine, fp identity.Fingerprint, dir engine.Direction, from, to time.Time) ([]engine.Record, error)
	retain    func(b *broken, horizon time.Time) error
	written   []engine.Record

	// partial applies a batch record by record, so a refused batch leaves its
	// valid prefix behind.
	partial bool
	// swallow turns a refusal the engine should have returned into success.
	swallow func(error) bool
}

func newBroken() *broken {
	o := oracle.New()
	return &broken{Engine: o, inner: o}
}

func (b *broken) Write(batch []engine.Record) error {
	if b.write != nil {
		batch = b.write(batch)
	}
	var err error
	if b.partial {
		for _, r := range batch {
			if err = b.inner.Write([]engine.Record{r}); err != nil {
				break
			}
			b.written = append(b.written, r)
		}
	} else if err = b.inner.Write(batch); err == nil {
		b.written = append(b.written, batch...)
	}
	if err != nil && b.swallow != nil && b.swallow(err) {
		return nil
	}
	return err
}

func (b *broken) Neighbors(fp identity.Fingerprint, dir engine.Direction, t time.Time) ([]engine.Neighbor, error) {
	if b.neighbors != nil {
		return b.neighbors(b.inner, fp, dir, t)
	}
	return b.inner.Neighbors(fp, dir, t)
}

func (b *broken) Window(fp identity.Fingerprint, dir engine.Direction, from, to time.Time) ([]engine.Record, error) {
	if b.window != nil {
		return b.window(b.inner, fp, dir, from, to)
	}
	return b.inner.Window(fp, dir, from, to)
}

func (b *broken) Retain(h time.Time) error {
	if b.retain != nil {
		return b.retain(b, h)
	}
	return b.inner.Retain(h)
}

func TestHarnessCatchesBrokenEngines(t *testing.T) {
	t.Parallel()

	dropDeletes := func(batch []engine.Record) []engine.Record {
		return slices.DeleteFunc(slices.Clone(batch), func(r engine.Record) bool { return r.Kind == lifecycle.Delete })
	}
	forgetTTL := func(batch []engine.Record) []engine.Record {
		out := slices.Clone(batch)
		for i := range out {
			out[i].TTL = 0
		}
		return out
	}
	forgetThrough := func(batch []engine.Record) []engine.Record {
		out := slices.Clone(batch)
		for i := range out {
			out[i].Through = time.Time{}
		}
		return out
	}

	// Alters, in place, the payload bytes of the batch it was given. If the
	// harness handed the candidate and the oracle the same arrays, the oracle
	// would be altered too and nothing would differ.
	scribble := func(batch []engine.Record) []engine.Record {
		for _, r := range batch {
			for i := range r.Payload {
				r.Payload[i] ^= 0xff
			}
		}
		return batch
	}

	cases := map[string]func() *broken{
		// Boundary errors: an interval is half-open, so an edge that starts at t
		// is not visible at t-1ns, and one that ends at its deadline is gone at
		// the deadline. Reading one nanosecond early or late gets one of those
		// wrong, and only a probe right at a boundary can tell.
		"reads one nanosecond early": func() *broken {
			b := newBroken()
			b.neighbors = func(in engine.Engine, fp identity.Fingerprint, dir engine.Direction, t time.Time) ([]engine.Neighbor, error) {
				return in.Neighbors(fp, dir, t.Add(time.Nanosecond))
			}
			return b
		},
		"reads one nanosecond late": func() *broken {
			b := newBroken()
			b.neighbors = func(in engine.Engine, fp identity.Fingerprint, dir engine.Direction, t time.Time) ([]engine.Neighbor, error) {
				return in.Neighbors(fp, dir, t.Add(-time.Nanosecond))
			}
			return b
		},
		"accepts records before the horizon": func() *broken {
			b := newBroken()
			b.swallow = func(err error) bool { return errors.Is(err, engine.ErrBeforeHorizon) }
			return b
		},
		"alters the caller's payload bytes": func() *broken { b := newBroken(); b.write = scribble; return b },
		"a second retention loses the baseline": func() *broken {
			b := newBroken()
			calls := 0
			b.retain = func(b *broken, h time.Time) error {
				if calls++; calls < 2 {
					return nil
				}
				fresh := oracle.New()
				var keep []engine.Record
				for _, r := range b.written {
					if !r.EventTime.Before(h) {
						keep = append(keep, r)
					}
				}
				if err := fresh.Write(keep); err != nil {
					return err
				}
				b.inner, b.Engine = fresh, fresh
				return nil
			}
			return b
		},
		"drops deletes":   func() *broken { b := newBroken(); b.write = dropDeletes; return b },
		"ignores TTL":     func() *broken { b := newBroken(); b.write = forgetTTL; return b },
		"ignores Through": func() *broken { b := newBroken(); b.write = forgetThrough; return b },
		"reverse reads forward": func() *broken {
			b := newBroken()
			b.neighbors = func(in engine.Engine, fp identity.Fingerprint, dir engine.Direction, t time.Time) ([]engine.Neighbor, error) {
				return in.Neighbors(fp, engine.Forward, t)
			}
			return b
		},
		"window end is inclusive": func() *broken {
			b := newBroken()
			b.window = func(in engine.Engine, fp identity.Fingerprint, dir engine.Direction, from, to time.Time) ([]engine.Record, error) {
				return in.Window(fp, dir, from, to.Add(time.Nanosecond))
			}
			return b
		},
		"window start is exclusive": func() *broken {
			b := newBroken()
			b.window = func(in engine.Engine, fp identity.Fingerprint, dir engine.Direction, from, to time.Time) ([]engine.Record, error) {
				return in.Window(fp, dir, from.Add(time.Nanosecond), to)
			}
			return b
		},
		"retention loses the baseline": func() *broken {
			b := newBroken()
			b.retain = func(b *broken, h time.Time) error {
				// Rebuild from only the records at or after the horizon: whatever
				// was alive across the horizon is forgotten.
				fresh := oracle.New()
				var keep []engine.Record
				for _, r := range b.written {
					if !r.EventTime.Before(h) {
						keep = append(keep, r)
					}
				}
				if err := fresh.Write(keep); err != nil {
					return err
				}
				b.inner, b.Engine = fresh, fresh
				return nil
			}
			return b
		},
	}

	for name, make := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			caught := 0
			for _, cfg := range conformance.Configs() {
				if err := conformance.Check(make(), cfg, conformance.Options{RetainAt: []float64{0.3, 0.6}}); err != nil {
					caught++
				}
			}
			if caught == 0 {
				t.Fatalf("the harness did not notice an engine that %s", name)
			}
		})
	}
}

func TestCheckPassesAnHonestEngineWithRetention(t *testing.T) {
	t.Parallel()
	cfg := workload.Tiny()
	if err := conformance.Check(oracle.New(), cfg, conformance.Options{RetainAt: []float64{0.4, 0.7}, CheckEvery: 3}); err != nil {
		t.Fatal(err)
	}
}

// TestContractCatchesBrokenEngines does the same for the write contract: an
// engine that stores part of a refused batch, or accepts records it should
// refuse, must fail it.
func TestContractCatchesBrokenEngines(t *testing.T) {
	t.Parallel()

	if err := conformance.CheckWriteContract(oracle.New()); err != nil {
		t.Fatalf("the oracle fails its own contract: %v", err)
	}
	cases := map[string]func() *broken{
		"applies the valid prefix of a refused batch": func() *broken { b := newBroken(); b.partial = true; return b },
		"lets the horizon move backward": func() *broken {
			b := newBroken()
			b.retain = func(b *broken, h time.Time) error {
				// Replays everything into a fresh oracle whose horizon is just h.
				fresh := oracle.New()
				if err := fresh.Write(slices.Clone(b.written)); err != nil {
					return err
				}
				if err := fresh.Retain(h); err != nil {
					return err
				}
				b.inner, b.Engine = fresh, fresh
				return nil
			}
			return b
		},
		"accepts invalid records": func() *broken {
			b := newBroken()
			b.swallow = func(err error) bool { return errors.Is(err, engine.ErrInvalid) }
			return b
		},
	}
	for name, make := range cases {
		if err := conformance.CheckWriteContract(make()); err == nil {
			t.Errorf("the contract did not notice an engine that %s", name)
		}
	}
}

func TestRetainAtIsValidated(t *testing.T) {
	t.Parallel()

	for _, bad := range [][]float64{{0}, {1}, {-0.1}, {1.5}, {0.5, 0.5}, {0.7, 0.3}} {
		if err := conformance.Check(oracle.New(), workload.Tiny(), conformance.Options{RetainAt: bad}); err == nil {
			t.Errorf("RetainAt %v was accepted", bad)
		}
	}
	if err := conformance.Check(oracle.New(), workload.Config{}, conformance.Options{}); err == nil {
		t.Error("an invalid workload config was accepted")
	}
}

// failing wraps an engine and makes one method fail, so the harness's own error
// reporting is checked: a candidate error must surface, with its cause.
type failing struct {
	engine.Engine
	method string
	err    error
}

func (f failing) fail(m string) error {
	if f.method == m {
		return f.err
	}
	return nil
}

func (f failing) Write(b []engine.Record) error {
	if err := f.fail("Write"); err != nil {
		return err
	}
	return f.Engine.Write(b)
}

func (f failing) Retain(h time.Time) error {
	if err := f.fail("Retain"); err != nil {
		return err
	}
	return f.Engine.Retain(h)
}

func (f failing) Alive(fp identity.Fingerprint, at time.Time) (bool, error) {
	if err := f.fail("Alive"); err != nil {
		return false, err
	}
	return f.Engine.Alive(fp, at)
}

func (f failing) Neighbors(fp identity.Fingerprint, d engine.Direction, at time.Time) ([]engine.Neighbor, error) {
	if err := f.fail("Neighbors"); err != nil {
		return nil, err
	}
	return f.Engine.Neighbors(fp, d, at)
}

func (f failing) Window(fp identity.Fingerprint, d engine.Direction, from, to time.Time) ([]engine.Record, error) {
	if err := f.fail("Window"); err != nil {
		return nil, err
	}
	return f.Engine.Window(fp, d, from, to)
}

func TestCandidateErrorsSurface(t *testing.T) {
	t.Parallel()

	boom := errors.New("injected")
	for _, method := range []string{"Write", "Retain", "Alive", "Neighbors", "Window"} {
		cand := failing{Engine: oracle.New(), method: method, err: boom}
		err := conformance.Check(cand, workload.Tiny(), conformance.Options{RetainAt: []float64{0.5}, CheckEvery: 2})
		if !errors.Is(err, boom) {
			t.Errorf("a failing %s: err = %v, want it to wrap the injected error", method, err)
		}
	}
}
