package conformance_test

import (
	"errors"
	"flag"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/oracle"
	"github.com/lotannauo/toposhift/bench/spike/workload"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// TestMain lowers the number of random workloads rapid runs in this package,
// which is where the harness is tested against engines that are not real: the
// oracle and its mutants, under the race detector. A candidate's own package
// keeps rapid's default. A -rapid.checks given on the command line still wins,
// because it is parsed after this.
func TestMain(m *testing.M) {
	if err := flag.Set("rapid.checks", "25"); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

// The oracle satisfies its own harness: the checks are consistent.
func TestOracleIsAConformingEngine(t *testing.T) {
	t.Parallel()
	conformance.Run(t, func(string) (engine.Engine, error) { return oracle.New(), nil })
}

// broken wraps a correct engine and damages one behavior. A harness that cannot
// tell is worthless, so each damage must be caught.
type broken struct {
	inner *oracle.Oracle
	view  *oracle.Oracle // if set, reads are answered from it instead of inner

	write func([]engine.Record) []engine.Record
	// scope and when change every read's scope and every point read's instant.
	scope func(engine.Scope) engine.Scope
	when  func(time.Time) time.Time

	neighbors func(r engine.Engine, fp identity.Fingerprint, dir engine.Direction, t time.Time, sc engine.Scope) ([]engine.Neighbor, error)
	alive     func(r engine.Engine, fp identity.Fingerprint, t time.Time, sc engine.Scope) (bool, error)
	batch     func(r engine.Engine, fps []identity.Fingerprint, dir engine.Direction, t time.Time, sc engine.Scope) ([][]engine.Neighbor, error)
	window    func(r engine.Engine, fp identity.Fingerprint, dir engine.Direction, from, to time.Time, sc engine.Scope) ([]engine.Record, error)
	retain    func(b *broken, horizon time.Time) error
	lastSeq   func(real uint64) uint64
	written   []engine.Record
	maxSeq    uint64
	horizon   time.Time // for a retain hook that needs to know how far the horizon has moved

	// partial applies a batch record by record, so a refused batch leaves its
	// valid prefix behind.
	partial bool
	// swallow turns a refusal the engine should have returned into success.
	swallow func(error) bool
	// overwrite keeps only the newest record of a producer at one instant, as an
	// engine that overwrites in place would, and answers reads from what is left.
	overwrite bool
	// overwriteBy says what overwrites: the producer's own record at that
	// instant (the default), any producer's, or any record of the subject.
	overwriteBy overwriteScope
	survivors   map[survivorKey]engine.Record
	// tokenOnRefusal moves LastSeq even when the batch is refused.
	tokenOnRefusal bool
	// retainMovesToken moves LastSeq when Retain is called.
	retainMovesToken bool
	// acceptMixed accepts a batch that mixes records before the horizon with
	// valid ones, storing the valid ones, though it refuses a record before the
	// horizon on its own.
	acceptMixed bool
	// pinnedAtHorizon answers wrongly at exactly the horizon under a pinned token.
	pinnedAtHorizon bool
}

type overwriteScope int

const (
	overwriteSameProducerAndInstant overwriteScope = iota
	overwriteAnyProducerAtInstant
	overwriteAnythingOfTheSubject
)

type survivorKey struct {
	subject  engine.Subject
	producer lifecycle.Producer
	at       int64
}

func newBroken() *broken { return &broken{inner: oracle.New()} }

func (b *broken) reader() engine.Engine {
	if b.view != nil {
		return b.view
	}
	return b.inner
}

func (b *broken) Write(batch []engine.Record) error {
	// The engine's token follows the batch it was given, whatever it then chose
	// to store of it.
	last := uint64(0)
	if len(batch) > 0 {
		last = batch[len(batch)-1].Seq
	}
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
		if b.overwrite {
			if b.survivors == nil {
				b.survivors = map[survivorKey]engine.Record{}
			}
			for _, r := range batch {
				k := survivorKey{r.Subject, r.Producer, r.EventTime.UnixNano()}
				switch b.overwriteBy {
				case overwriteAnyProducerAtInstant:
					k.producer = ""
				case overwriteAnythingOfTheSubject:
					k.producer, k.at = "", 0
				}
				b.survivors[k] = r
			}
			kept := make([]engine.Record, 0, len(b.survivors))
			for _, r := range b.survivors {
				kept = append(kept, r)
			}
			slices.SortFunc(kept, func(x, y engine.Record) int { return int(x.Seq) - int(y.Seq) })
			view := oracle.New()
			if err := view.Write(kept); err != nil {
				return err
			}
			b.view = view
		}
	}
	if err == nil && last > 0 {
		b.maxSeq = last
	}
	if err != nil && b.tokenOnRefusal && len(batch) > 0 {
		// The valid records processed before the refusal moved the token, and
		// nothing moved it back.
		for _, r := range batch {
			if !r.EventTime.Before(b.horizon) {
				b.maxSeq = max(b.maxSeq, r.Seq)
				break
			}
		}
	}
	if err != nil && b.acceptMixed && errors.Is(err, engine.ErrBeforeHorizon) && len(batch) > 1 {
		rest := slices.DeleteFunc(slices.Clone(batch), func(r engine.Record) bool { return r.EventTime.Before(b.horizon) })
		if len(rest) > 0 && len(rest) < len(batch) {
			if err = b.inner.Write(rest); err == nil {
				b.written = append(b.written, rest...)
				b.maxSeq = rest[len(rest)-1].Seq
				return nil
			}
		}
	}
	if err != nil && b.swallow != nil && b.swallow(err) {
		return nil
	}
	return err
}

func (b *broken) scoped(sc engine.Scope) engine.Scope {
	if b.scope != nil {
		return b.scope(sc)
	}
	return sc
}

func (b *broken) at(t time.Time) time.Time {
	if b.when != nil {
		return b.when(t)
	}
	return t
}

func (b *broken) LastSeq() uint64 {
	if b.lastSeq != nil {
		return b.lastSeq(b.maxSeq)
	}
	return b.maxSeq
}

func (b *broken) Neighbors(fp identity.Fingerprint, dir engine.Direction, t time.Time, sc engine.Scope) ([]engine.Neighbor, error) {
	if b.pinnedAtHorizon && !b.horizon.IsZero() && t.Equal(b.horizon) && sc.AsOf != engine.Latest {
		return nil, nil
	}
	sc, t = b.scoped(sc), b.at(t)
	if b.neighbors != nil {
		return b.neighbors(b.reader(), fp, dir, t, sc)
	}
	return b.reader().Neighbors(fp, dir, t, sc)
}

func (b *broken) NeighborsBatch(fps []identity.Fingerprint, dir engine.Direction, t time.Time, sc engine.Scope) ([][]engine.Neighbor, error) {
	sc, t = b.scoped(sc), b.at(t)
	if b.batch != nil {
		return b.batch(b.reader(), fps, dir, t, sc)
	}
	return b.reader().NeighborsBatch(fps, dir, t, sc)
}

func (b *broken) Alive(fp identity.Fingerprint, t time.Time, sc engine.Scope) (bool, error) {
	sc, t = b.scoped(sc), b.at(t)
	if b.alive != nil {
		return b.alive(b.reader(), fp, t, sc)
	}
	return b.reader().Alive(fp, t, sc)
}

func (b *broken) Window(fp identity.Fingerprint, dir engine.Direction, from, to time.Time, sc engine.Scope) ([]engine.Record, error) {
	sc = b.scoped(sc)
	if b.window != nil {
		return b.window(b.reader(), fp, dir, from, to, sc)
	}
	return b.reader().Window(fp, dir, from, to, sc)
}

func (b *broken) Retain(h time.Time) error {
	if b.retainMovesToken {
		b.maxSeq++
	}
	if h.After(b.horizon) && b.retain == nil {
		b.horizon = h
	}
	if b.retain != nil {
		return b.retain(b, h)
	}
	if b.view != nil {
		if err := b.view.Retain(h); err != nil {
			return err
		}
	}
	return b.inner.Retain(h)
}

func (b *broken) Size() (int64, error) { return b.inner.Size() }
func (b *broken) Close() error         { return nil }

// rebuild replaces the engine's contents with the records keep selects from
// everything written so far, and gives the result the horizon h, as a real
// retention would. (Without it a rebuilt engine would also accept records before
// the horizon, and a mutant meant to lose a baseline would be caught for that
// instead.)
func (b *broken) rebuild(h time.Time, keep func(engine.Record) bool) error {
	fresh := oracle.New()
	var kept []engine.Record
	for _, r := range b.written {
		if keep(r) {
			kept = append(kept, r)
		}
	}
	if err := fresh.Write(kept); err != nil {
		return err
	}
	if err := fresh.Retain(h); err != nil {
		return err
	}
	b.inner = fresh
	return nil
}

// allLayers answers a layer-scoped read for every layer and merges the answers,
// as an engine that ignores the layer would.
var allLayers = []catalog.Layer{catalog.L0, catalog.L1, catalog.L2, catalog.L3}

func withToken(sc engine.Scope, layer catalog.Layer) engine.Scope {
	return engine.Scope{Layer: layer, AsOf: sc.AsOf}
}

// detectors are the scripted checks. Each takes a fresh engine.
var detectors = map[string]func(engine.Engine) error{
	"instant":       conformance.CheckInstant,
	"producers":     conformance.CheckProducers,
	"extremes":      conformance.CheckExtremes,
	"read contract": conformance.CheckReadContract,
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

	// rebuildKeeping returns a Retain that forgets, by rebuilding from the
	// records keep selects, whatever the real Retain would not.
	rebuildKeeping := func(keep func(r engine.Record, h time.Time) bool) func(*broken, time.Time) error {
		return func(b *broken, h time.Time) error {
			return b.rebuild(h, func(r engine.Record) bool { return keep(r, h) })
		}
	}

	// mutants are damaged engines. A mutant must be caught by Check on some
	// workload or by a scripted check; if it names detectors, each of those must
	// catch it on its own, because it is the check written for exactly that.
	type mutant struct {
		make func() *broken
		by   []string
		// scriptedOnly says only a scripted check can catch it: the random
		// workloads never reach what it gets wrong. Every other mutant must be
		// caught by Check itself, which is what keeps the random rounds honest.
		scriptedOnly bool
		// wrong means the engine gives wrong answers to reads, so some check must
		// fail with [conformance.ErrMismatch], not merely with a refused write, a
		// wrong LastSeq or a failed call.
		wrong bool
	}
	cases := map[string]mutant{
		// Boundary errors: an interval is half-open, so an edge that starts at t
		// is not visible at t-1ns, and one that ends at its deadline is gone at
		// the deadline. Reading one nanosecond early or late gets one of those
		// wrong, and only a probe right at a boundary can tell.
		"reads one nanosecond early": {make: func() *broken {
			b := newBroken()
			b.when = func(t time.Time) time.Time { return t.Add(time.Nanosecond) }
			return b
		}, by: []string{"instant"}, wrong: true},

		"reads one nanosecond late": {make: func() *broken {
			b := newBroken()
			b.when = func(t time.Time) time.Time { return t.Add(-time.Nanosecond) }
			return b
		}, by: []string{"instant"}, wrong: true},

		"treats the first instant as before everything": {make: func() *broken {
			b := newBroken()
			b.when = func(t time.Time) time.Time {
				if t.Equal(engine.MinEventTime) {
					return t.Add(-time.Nanosecond)
				}
				return t
			}
			return b
		}, by: []string{"extremes"}, wrong: true, scriptedOnly: true},

		"accepts records before the horizon": {make: func() *broken {
			b := newBroken()
			b.swallow = func(err error) bool { return errors.Is(err, engine.ErrBeforeHorizon) }
			return b
		}},
		"alters the caller's payload bytes": {make: func() *broken { b := newBroken(); b.write = scribble; return b }},
		// The first retention is honest. The second forgets what the first kept
		// standing for the history before it: it keeps only the records at or
		// after the first horizon, so whatever was alive across it is lost.
		"a second retention loses the first baseline": {make: func() *broken {
			b := newBroken()
			var first time.Time
			b.retain = func(b *broken, h time.Time) error {
				if first.IsZero() {
					first = h
					return b.inner.Retain(h)
				}
				return b.rebuild(h, func(r engine.Record) bool { return !r.EventTime.Before(first) })
			}
			return b
		}, wrong: true},
		"drops deletes": {make: func() *broken { b := newBroken(); b.write = dropDeletes; return b }, wrong: true},

		"ignores TTL": {make: func() *broken { b := newBroken(); b.write = forgetTTL; return b }, wrong: true},

		"ignores Through": {make: func() *broken { b := newBroken(); b.write = forgetThrough; return b }, wrong: true},

		"reverse reads forward": {make: func() *broken {
			b := newBroken()
			b.neighbors = func(r engine.Engine, fp identity.Fingerprint, _ engine.Direction, t time.Time, sc engine.Scope) ([]engine.Neighbor, error) {
				return r.Neighbors(fp, engine.Forward, t, sc)
			}
			return b
		}, wrong: true},

		"window end is inclusive": {make: func() *broken {
			b := newBroken()
			b.window = func(r engine.Engine, fp identity.Fingerprint, dir engine.Direction, from, to time.Time, sc engine.Scope) ([]engine.Record, error) {
				return r.Window(fp, dir, from, to.Add(time.Nanosecond), sc)
			}
			return b
		}, by: []string{"instant"}, wrong: true},

		// The same edge from the other side: a record exactly one nanosecond
		// before the end is missed. Only a window whose end is one nanosecond
		// past a record's instant can tell.
		"window end loses the last nanosecond": {make: func() *broken {
			b := newBroken()
			b.window = func(r engine.Engine, fp identity.Fingerprint, dir engine.Direction, from, to time.Time, sc engine.Scope) ([]engine.Record, error) {
				return r.Window(fp, dir, from, to.Add(-time.Nanosecond), sc)
			}
			return b
		}, by: []string{"instant"}, wrong: true},

		// A record just before the window's start is let in.
		"window start is one nanosecond early": {make: func() *broken {
			b := newBroken()
			b.window = func(r engine.Engine, fp identity.Fingerprint, dir engine.Direction, from, to time.Time, sc engine.Scope) ([]engine.Record, error) {
				return r.Window(fp, dir, from.Add(-time.Nanosecond), to, sc)
			}
			return b
		}, by: []string{"instant"}, wrong: true},

		"window start is exclusive": {make: func() *broken {
			b := newBroken()
			b.window = func(r engine.Engine, fp identity.Fingerprint, dir engine.Direction, from, to time.Time, sc engine.Scope) ([]engine.Record, error) {
				return r.Window(fp, dir, from.Add(time.Nanosecond), to, sc)
			}
			return b
		}, by: []string{"instant"}, wrong: true},

		"retention loses the baseline": {make: func() *broken {
			b := newBroken()
			// Rebuild from only the records at or after the horizon: whatever
			// was alive across the horizon is forgotten.
			b.retain = rebuildKeeping(func(r engine.Record, h time.Time) bool { return !r.EventTime.Before(h) })
			return b
		}, wrong: true},
		// Records at exactly the horizon are not before it, so they must stay.
		"retention drops the records at exactly the horizon": {make: func() *broken {
			b := newBroken()
			b.retain = rebuildKeeping(func(r engine.Record, h time.Time) bool { return !r.EventTime.Equal(h) })
			return b
		}, by: []string{"instant"}, wrong: true},

		// The snapshot token and the layer.
		"ignores the snapshot token": {make: func() *broken {
			b := newBroken()
			b.scope = func(sc engine.Scope) engine.Scope { sc.AsOf = engine.Latest; return sc }
			return b
		}, by: []string{"instant"}, wrong: true},

		"treats the snapshot token as exclusive": {make: func() *broken {
			b := newBroken()
			b.scope = func(sc engine.Scope) engine.Scope {
				if sc.AsOf != engine.Latest && sc.AsOf > 0 {
					sc.AsOf--
				}
				return sc
			}
			return b
		}, by: []string{"instant"}, wrong: true},

		"ignores the layer in Neighbors": {make: func() *broken {
			b := newBroken()
			b.neighbors = func(r engine.Engine, fp identity.Fingerprint, dir engine.Direction, t time.Time, sc engine.Scope) ([]engine.Neighbor, error) {
				var out []engine.Neighbor
				for _, l := range allLayers {
					ns, err := r.Neighbors(fp, dir, t, withToken(sc, l))
					if err != nil {
						return nil, err
					}
					out = append(out, ns...)
				}
				engine.SortNeighbors(out)
				return out, nil
			}
			return b
		}, by: []string{"instant"}, wrong: true},

		"ignores the layer in Alive": {make: func() *broken {
			b := newBroken()
			b.alive = func(r engine.Engine, fp identity.Fingerprint, t time.Time, sc engine.Scope) (bool, error) {
				for _, l := range allLayers {
					if ok, err := r.Alive(fp, t, withToken(sc, l)); err != nil || ok {
						return ok, err
					}
				}
				return false, nil
			}
			return b
		}, wrong: true},

		"mixes layers in Window": {make: func() *broken {
			b := newBroken()
			b.window = func(r engine.Engine, fp identity.Fingerprint, dir engine.Direction, from, to time.Time, sc engine.Scope) ([]engine.Record, error) {
				var out []engine.Record
				for _, l := range allLayers {
					rs, err := r.Window(fp, dir, from, to, withToken(sc, l))
					if err != nil {
						return nil, err
					}
					out = append(out, rs...)
				}
				engine.SortRecords(out)
				return out, nil
			}
			return b
		}, by: []string{"instant"}, wrong: true},

		// Overwriting in place loses the versions a query pinned to an earlier
		// token needs, and the records Window must still return.
		// A producer that deletes must not hide another producer's record at the
		// same instant, and the newest record of a subject must not decide for all.
		"keeps only the newest record at an instant, whichever producer made it": {make: func() *broken {
			b := newBroken()
			b.overwrite, b.overwriteBy = true, overwriteAnyProducerAtInstant
			return b
		}, by: []string{"producers"}, wrong: true},
		"lets the newest record of a subject decide, whichever producer made it": {make: func() *broken {
			b := newBroken()
			b.overwrite, b.overwriteBy = true, overwriteAnythingOfTheSubject
			return b
		}, by: []string{"producers"}, wrong: true},
		// A long window is cut short: only windows that span many records, not
		// ones that hold a record or two, can show it.
		"a window returns at most three records": {make: func() *broken {
			b := newBroken()
			b.window = func(r engine.Engine, fp identity.Fingerprint, dir engine.Direction, from, to time.Time, sc engine.Scope) ([]engine.Record, error) {
				rs, err := r.Window(fp, dir, from, to, sc)
				if len(rs) > 3 {
					rs = rs[:3]
				}
				return rs, err
			}
			return b
		}, wrong: true},
		"keeps only the newest record at an instant": {make: func() *broken { b := newBroken(); b.overwrite = true; return b }, by: []string{"instant"}, wrong: true},

		"accepts an invalid scope": {make: func() *broken {
			b := newBroken()
			b.scope = func(sc engine.Scope) engine.Scope {
				if sc.Layer < catalog.L0 || sc.Layer > catalog.L3 {
					sc.Layer = catalog.L2
				}
				return sc
			}
			return b
		}, by: []string{"read contract"}, scriptedOnly: true},

		// The batched read and the token.
		"a batch drops repeated fingerprints": {make: func() *broken {
			b := newBroken()
			b.batch = func(r engine.Engine, fps []identity.Fingerprint, dir engine.Direction, t time.Time, sc engine.Scope) ([][]engine.Neighbor, error) {
				var unique []identity.Fingerprint
				for _, fp := range fps {
					if !slices.Contains(unique, fp) {
						unique = append(unique, fp)
					}
				}
				return r.NeighborsBatch(unique, dir, t, sc)
			}
			return b
		}, wrong: true},

		"a batch answers every fingerprint like the first": {make: func() *broken {
			b := newBroken()
			b.batch = func(r engine.Engine, fps []identity.Fingerprint, dir engine.Direction, t time.Time, sc engine.Scope) ([][]engine.Neighbor, error) {
				out := make([][]engine.Neighbor, len(fps))
				for i := range fps {
					ns, err := r.Neighbors(fps[0], dir, t, sc)
					if err != nil {
						return nil, err
					}
					out[i] = ns
				}
				return out, nil
			}
			return b
		}, wrong: true},

		"a batch answers an empty batch with something": {make: func() *broken {
			b := newBroken()
			b.batch = func(r engine.Engine, fps []identity.Fingerprint, dir engine.Direction, t time.Time, sc engine.Scope) ([][]engine.Neighbor, error) {
				if len(fps) == 0 {
					return [][]engine.Neighbor{nil}, nil
				}
				return r.NeighborsBatch(fps, dir, t, sc)
			}
			return b
		}, wrong: true},

		"a batch ignores the token": {make: func() *broken {
			b := newBroken()
			b.batch = func(r engine.Engine, fps []identity.Fingerprint, dir engine.Direction, t time.Time, sc engine.Scope) ([][]engine.Neighbor, error) {
				sc.AsOf = engine.Latest
				return r.NeighborsBatch(fps, dir, t, sc)
			}
			return b
		}, by: []string{"instant"}, wrong: true},

		// A reference belongs to the producer that made it.
		"ignores which producer a record is from": {make: func() *broken {
			b := newBroken()
			b.write = func(batch []engine.Record) []engine.Record {
				out := slices.Clone(batch)
				for i := range out {
					out[i].Producer = "anyone"
				}
				return out
			}
			return b
		}, by: []string{"producers"}, wrong: true},
		// A sequence number that does not fit 32 bits wraps, so the engine sees
		// them go backwards.
		"stores Seq in 32 bits": {make: func() *broken {
			b := newBroken()
			b.write = func(batch []engine.Record) []engine.Record {
				out := slices.Clone(batch)
				for i := range out {
					out[i].Seq &= 0xFFFFFFFF
				}
				return out
			}
			return b
		}, by: []string{"instant", "producers"}},
		"LastSeq moves when a batch is refused": {make: func() *broken { b := newBroken(); b.tokenOnRefusal = true; return b }},
		"LastSeq moves when Retain is called":   {make: func() *broken { b := newBroken(); b.retainMovesToken = true; return b }},
		// A baseline built for the latest token is used for a pinned one.
		"answers wrongly at the horizon instant under a pinned token": {make: func() *broken { b := newBroken(); b.pinnedAtHorizon = true; return b }, wrong: true},
		"LastSeq is always zero": {make: func() *broken { b := newBroken(); b.lastSeq = func(uint64) uint64 { return 0 }; return b }, by: []string{"instant"}},
		"LastSeq runs one ahead": {make: func() *broken { b := newBroken(); b.lastSeq = func(real uint64) uint64 { return real + 1 }; return b }, by: []string{"instant"}},
	}

	for name, m := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			isMismatch := func(err error) bool { return errors.Is(err, conformance.ErrMismatch) }

			// The scripted checks are cheap and deterministic. Each one a mutant
			// names must catch it on its own.
			scripted, scriptedMismatch := false, false
			for dname, d := range detectors {
				err := d(m.make())
				if err == nil && slices.Contains(m.by, dname) {
					t.Errorf("the %s check did not notice an engine that %s", dname, name)
				}
				scripted = scripted || err != nil
				scriptedMismatch = scriptedMismatch || isMismatch(err)
			}

			// The random workloads must catch it too, unless nothing they do can
			// reach what it gets wrong: that is what keeps their probing honest.
			// They stop at the first config that notices (for a wrong answer, the
			// first that notices it as one).
			random, randomMismatch := false, false
			for _, cfg := range conformance.Configs() {
				if random && (!m.wrong || randomMismatch) {
					break
				}
				if err := conformance.Check(m.make(), cfg, conformance.Options{RetainAt: []float64{0.3, 0.6}}); err != nil {
					random = true
					randomMismatch = randomMismatch || isMismatch(err)
				}
			}
			switch {
			case !scripted && !random:
				t.Fatalf("the harness did not notice an engine that %s", name)
			case !m.scriptedOnly && !random:
				t.Errorf("the random workloads did not notice an engine that %s", name)
			}
			if m.wrong && !scriptedMismatch && !randomMismatch {
				t.Errorf("an engine that %s was caught, but never for a wrong answer: it was caught by something incidental", name)
			}
			if m.wrong && !m.scriptedOnly && !randomMismatch {
				t.Errorf("the random workloads caught an engine that %s, but never for a wrong answer", name)
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
	if err := conformance.CheckReadContract(oracle.New()); err != nil {
		t.Fatalf("the oracle fails its own read contract: %v", err)
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
				b.inner = fresh
				return nil
			}
			return b
		},
		"moves LastSeq when it refuses a batch": func() *broken { b := newBroken(); b.tokenOnRefusal = true; return b },
		"accepts a batch that mixes stale and valid records": func() *broken {
			b := newBroken()
			b.acceptMixed = true
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

func (f failing) Alive(fp identity.Fingerprint, at time.Time, sc engine.Scope) (bool, error) {
	if err := f.fail("Alive"); err != nil {
		return false, err
	}
	return f.Engine.Alive(fp, at, sc)
}

func (f failing) Neighbors(fp identity.Fingerprint, d engine.Direction, at time.Time, sc engine.Scope) ([]engine.Neighbor, error) {
	if err := f.fail("Neighbors"); err != nil {
		return nil, err
	}
	return f.Engine.Neighbors(fp, d, at, sc)
}

func (f failing) NeighborsBatch(fps []identity.Fingerprint, d engine.Direction, at time.Time, sc engine.Scope) ([][]engine.Neighbor, error) {
	if err := f.fail("NeighborsBatch"); err != nil {
		return nil, err
	}
	return f.Engine.NeighborsBatch(fps, d, at, sc)
}

func (f failing) Window(fp identity.Fingerprint, d engine.Direction, from, to time.Time, sc engine.Scope) ([]engine.Record, error) {
	if err := f.fail("Window"); err != nil {
		return nil, err
	}
	return f.Engine.Window(fp, d, from, to, sc)
}

func TestCandidateErrorsSurface(t *testing.T) {
	t.Parallel()

	boom := errors.New("injected")
	for _, method := range []string{"Write", "Retain", "Alive", "Neighbors", "NeighborsBatch", "Window"} {
		cand := failing{Engine: oracle.New(), method: method, err: boom}
		err := conformance.Check(cand, workload.Tiny(), conformance.Options{RetainAt: []float64{0.5}, CheckEvery: 2})
		if !errors.Is(err, boom) {
			t.Errorf("a failing %s: err = %v, want it to wrap the injected error", method, err)
		}
	}
}

// lazy is a conforming engine that takes every freedom the contract gives. After
// a retention it answers garbage for an instant before the horizon and for a
// token below the sequence it had reached, which callers must not ask about. A
// harness that asks anyway reports a failure that is not one.
type lazy struct {
	*oracle.Oracle
	horizon time.Time
	floor   uint64
}

func (l *lazy) Retain(h time.Time) error {
	if h.After(l.horizon) {
		l.horizon, l.floor = h, l.LastSeq()
	}
	return l.Oracle.Retain(h)
}

func (l *lazy) unspecified(t time.Time, sc engine.Scope) bool {
	return !l.horizon.IsZero() && (t.Before(l.horizon) || sc.AsOf < l.floor)
}

func (l *lazy) Neighbors(fp identity.Fingerprint, d engine.Direction, t time.Time, sc engine.Scope) ([]engine.Neighbor, error) {
	if l.unspecified(t, sc) {
		return []engine.Neighbor{{Peer: fp, Relation: catalog.PartOf}}, nil
	}
	return l.Oracle.Neighbors(fp, d, t, sc)
}

func (l *lazy) NeighborsBatch(fps []identity.Fingerprint, d engine.Direction, t time.Time, sc engine.Scope) ([][]engine.Neighbor, error) {
	if l.unspecified(t, sc) {
		return make([][]engine.Neighbor, len(fps)), nil
	}
	return l.Oracle.NeighborsBatch(fps, d, t, sc)
}

func (l *lazy) Alive(fp identity.Fingerprint, t time.Time, sc engine.Scope) (bool, error) {
	if l.unspecified(t, sc) {
		return true, nil
	}
	return l.Oracle.Alive(fp, t, sc)
}

func (l *lazy) Window(fp identity.Fingerprint, d engine.Direction, from, to time.Time, sc engine.Scope) ([]engine.Record, error) {
	if l.unspecified(from, sc) {
		return []engine.Record{{}}, nil
	}
	return l.Oracle.Window(fp, d, from, to, sc)
}

func TestCheckDoesNotAskWhatTheContractLeavesUnspecified(t *testing.T) {
	t.Parallel()
	for i, cfg := range conformance.Configs() {
		cand := &lazy{Oracle: oracle.New()}
		if err := conformance.Check(cand, cfg, conformance.Options{RetainAt: []float64{0.3, 0.6}}); err != nil {
			t.Errorf("config %d: %v", i, err)
		}
	}
	if err := conformance.CheckInstant(&lazy{Oracle: oracle.New()}); err != nil {
		t.Errorf("CheckInstant asked about something unspecified: %v", err)
	}
}

// TestCheckAcceptsAnHonestCompactingEngine is the positive control for
// retention: an engine that really discards history, by the rule the layouts
// are meant to follow, must pass. It keeps every record at or after the horizon,
// and for each producer's reference to a subject the newest record before it,
// only if that reference is still live at the horizon; everything else before
// the horizon is gone. If the harness asked about anything the contract leaves
// unspecified, or the rule were wrong, this would fail.
func TestCheckAcceptsAnHonestCompactingEngine(t *testing.T) {
	t.Parallel()

	live := func(r engine.Record, h time.Time) bool {
		if r.Kind != lifecycle.Observe {
			return false
		}
		end := r.EventTime
		if r.Through.After(end) {
			end = r.Through
		}
		return r.TTL == 0 || h.Before(end.Add(r.TTL))
	}
	compact := func(b *broken, h time.Time) error {
		if !h.After(b.horizon) {
			return nil // the horizon only moves forward
		}
		b.horizon = h
		type ref struct {
			subject  engine.Subject
			producer lifecycle.Producer
		}
		newest := map[ref]engine.Record{}
		var kept []engine.Record
		for _, r := range b.written {
			if !r.EventTime.Before(h) {
				kept = append(kept, r)
				continue
			}
			k := ref{r.Subject, r.Producer}
			if cur, ok := newest[k]; !ok || r.EventTime.After(cur.EventTime) || (r.EventTime.Equal(cur.EventTime) && r.Seq > cur.Seq) {
				newest[k] = r
			}
		}
		for _, r := range newest {
			if live(r, h) {
				kept = append(kept, r)
			}
		}
		slices.SortFunc(kept, func(x, y engine.Record) int {
			switch {
			case x.Seq < y.Seq:
				return -1
			case x.Seq > y.Seq:
				return 1
			}
			return 0
		})
		fresh := oracle.New()
		if err := fresh.Write(kept); err != nil {
			return err
		}
		if err := fresh.Retain(h); err != nil {
			return err
		}
		b.inner, b.written = fresh, kept
		return nil
	}
	conformance.Run(t, func(string) (engine.Engine, error) {
		b := newBroken()
		b.retain = compact
		return b, nil
	})
}
