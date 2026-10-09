package conformance_test

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
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

// TestMain sets the number of random workloads rapid runs in this package, which
// is where the harness is tested against engines that are not real: the oracle
// and its mutants. A candidate's own package sets its own. The fast tier sets
// TOPOSHIFT_RAPID_CHECKS higher, and a -rapid.checks given on the command line
// still wins, because it is parsed after this.
func TestMain(m *testing.M) {
	if err := flag.Set("rapid.checks", conformance.RapidChecks("25")); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

// The oracle satisfies its own harness: the checks are consistent.
func TestOracleIsAConformingEngine(t *testing.T) {
	t.Parallel()
	conformance.Run(t, func(dir string) (engine.Engine, error) { return oracle.OpenDurable(dir) })
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
	"relations":     conformance.CheckRelations,
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

		// The workload fixes an edge's relation by the types at its ends, so no
		// random workload gives a pair two relations: only the scripted check
		// can tell an engine that identifies an edge by its ends alone.
		"identifies an edge by its two ends, not its relation": {make: func() *broken {
			b := newBroken()
			b.neighbors = func(r engine.Engine, fp identity.Fingerprint, dir engine.Direction, t time.Time, sc engine.Scope) ([]engine.Neighbor, error) {
				ns, err := r.Neighbors(fp, dir, t, sc)
				if err != nil {
					return nil, err
				}
				var out []engine.Neighbor
				for _, n := range ns {
					if len(out) == 0 || out[len(out)-1].Peer != n.Peer {
						out = append(out, n)
					}
				}
				return out, nil
			}
			return b
		}, by: []string{"relations"}, wrong: true, scriptedOnly: true},

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
	// It lives in memory and is not safe for concurrent use, so it takes the
	// serial run.
	conformance.RunSerial(t, func(string) (engine.Engine, error) {
		b := newBroken()
		b.retain = compact
		return b, nil
	})
}

// volatileStore keeps engines in memory by directory, so a "reopen" returns the
// engine that was "closed", after onReopen has damaged it as a bad persistence
// layer would.
type volatileStore struct {
	mu       sync.Mutex
	engines  map[string]*broken
	onReopen func(b *broken)
}

func (s *volatileStore) open(dir string) (engine.Engine, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.engines == nil {
		s.engines = map[string]*broken{}
	}
	b, ok := s.engines[dir]
	if !ok {
		b = newBroken()
		s.engines[dir] = b
	} else if s.onReopen != nil {
		s.onReopen(b)
	}
	return b, nil
}

func TestCheckReopenCatchesAnEngineThatForgets(t *testing.T) {
	t.Parallel()

	// Persisting in memory is correct, so the check must pass it; and it must
	// pass the oracle that persists to a file.
	honest := &volatileStore{}
	if err := conformance.CheckReopen(honest.open); err != nil {
		t.Fatalf("an engine that keeps everything fails the reopen check: %v", err)
	}
	if err := conformance.CheckReopen(func(dir string) (engine.Engine, error) { return oracle.OpenDurable(dir) }); err != nil {
		t.Fatalf("the durable oracle fails the reopen check: %v", err)
	}

	for name, damage := range map[string]func(b *broken){
		// The records come back, but the horizon is not remembered, so a record
		// before it is accepted again.
		"forgets the retention horizon": func(b *broken) {
			fresh := oracle.New()
			if err := fresh.Write(slices.Clone(b.written)); err != nil {
				panic(err)
			}
			b.inner = fresh
		},
		// Whatever it is handed with a Seq that is not above the last is taken.
		"accepts a sequence that is not above the last": func(b *broken) {
			b.swallow = func(err error) bool {
				return errors.Is(err, engine.ErrInvalid) && !errors.Is(err, engine.ErrBeforeHorizon)
			}
		},
		"forgets the last sequence":    func(b *broken) { b.maxSeq = 0 },
		"reports a sequence one ahead": func(b *broken) { b.maxSeq++ },
		"loses the newest record": func(b *broken) {
			b.written = b.written[:len(b.written)-1]
			fresh := oracle.New()
			if err := fresh.Write(slices.Clone(b.written)); err != nil {
				panic(err)
			}
			b.inner = fresh
		},
	} {
		store := &volatileStore{onReopen: damage}
		if err := conformance.CheckReopen(store.open); err == nil {
			t.Errorf("the reopen check did not notice an engine that %s", name)
		}
	}

	// Reopening needs somewhere to reopen to, and valid fractions.
	if err := conformance.Check(oracle.New(), workload.Tiny(), conformance.Options{ReopenAt: []float64{0.5}}); err == nil {
		t.Error("ReopenAt without Reopen was accepted")
	}
	for _, bad := range [][]float64{{0}, {1}, {0.6, 0.4}} {
		opts := conformance.Options{ReopenAt: bad, Reopen: func(e engine.Engine) (engine.Engine, error) { return e, nil }}
		if err := conformance.Check(oracle.New(), workload.Tiny(), opts); err == nil {
			t.Errorf("ReopenAt %v was accepted", bad)
		}
	}
}

// settling counts how often it is asked to settle.
type settling struct {
	*oracle.Oracle
	settled atomic.Int64
}

func (s *settling) Settle() error { s.settled.Add(1); return nil }

func TestCheckSettlesAnEngineThatCan(t *testing.T) {
	t.Parallel()
	s := &settling{Oracle: oracle.New()}
	if err := conformance.Check(s, workload.Tiny(), conformance.Options{CheckEvery: 3}); err != nil {
		t.Fatal(err)
	}
	if s.settled.Load() == 0 {
		t.Error("an engine that can settle was never asked to")
	}
	// And one that cannot is not troubled.
	if err := conformance.Check(oracle.New(), workload.Tiny(), conformance.Options{CheckEvery: 3}); err != nil {
		t.Fatal(err)
	}
	// A failing Settle is reported with its cause.
	boom := errors.New("injected")
	if err := conformance.Check(&failingSettle{Oracle: oracle.New(), err: boom}, workload.Tiny(), conformance.Options{CheckEvery: 1}); !errors.Is(err, boom) {
		t.Errorf("err = %v, want the Settle error", err)
	}
}

type failingSettle struct {
	*oracle.Oracle
	err error
}

func (f *failingSettle) Settle() error { return f.err }

// slow is an engine that is consistent but slow: it takes its time inside each
// read, so writes land while a read is in progress. A concurrent-read check that
// failed it would be wrong.
type slow struct {
	*oracle.Oracle
	// tear makes Neighbors inconsistent by answering one half from before a
	// pause and the other half from after it.
	tear bool
	// tearBatchEnd answers a batched read with one snapshot per entry.
	tearBatchEnd bool
	// ahead raises LastSeq before a batch is visible.
	ahead   bool
	pending atomic.Uint64
	// retainTear makes Retain briefly lose what is before the horizon: reads
	// during it see nothing, as they would from an engine that deletes history
	// before it has written the baseline that stands for it.
	retainTear bool
	retaining  atomic.Bool
	// backward makes LastSeq lose ground on every other call.
	backward bool
	calls    atomic.Uint64
}

func (s *slow) Retain(h time.Time) error {
	if !s.retainTear {
		return s.Oracle.Retain(h)
	}
	s.retaining.Store(true)
	pause()
	defer s.retaining.Store(false)
	return s.Oracle.Retain(h)
}

func pause() { runtime.Gosched(); time.Sleep(300 * time.Microsecond) }

// pivot splits peers into two halves by the first byte of their hash.
func pivot(n engine.Neighbor) bool { h := n.Peer.Hash(); return h[0] < 128 }

func (s *slow) Neighbors(fp identity.Fingerprint, d engine.Direction, at time.Time, sc engine.Scope) ([]engine.Neighbor, error) {
	if s.retaining.Load() {
		return nil, nil
	}
	first, err := s.Oracle.Neighbors(fp, d, at, sc)
	pause()
	if err != nil || !s.tear {
		return first, err
	}
	// The first half of the answer comes from before the pause and the second
	// half from after it, as an engine would answer if it read one key range from
	// one state and the next from another.
	second, err := s.Oracle.Neighbors(fp, d, at, sc)
	if err != nil {
		return nil, err
	}
	var out []engine.Neighbor
	for _, n := range first {
		if pivot(n) {
			out = append(out, n)
		}
	}
	for _, n := range second {
		if !pivot(n) {
			out = append(out, n)
		}
	}
	engine.SortNeighbors(out)
	return out, nil
}

func (s *slow) NeighborsBatch(fps []identity.Fingerprint, d engine.Direction, at time.Time, sc engine.Scope) ([][]engine.Neighbor, error) {
	if s.retaining.Load() {
		return make([][]engine.Neighbor, len(fps)), nil
	}
	if !s.tearBatchEnd {
		pause()
		return s.Oracle.NeighborsBatch(fps, d, at, sc)
	}
	// One snapshot per entry instead of one for the whole batch.
	out := make([][]engine.Neighbor, len(fps))
	for i, fp := range fps {
		ns, err := s.Oracle.Neighbors(fp, d, at, sc)
		if err != nil {
			return nil, err
		}
		out[i] = ns
		pause()
	}
	return out, nil
}

func (s *slow) Write(batch []engine.Record) error {
	if s.ahead && len(batch) > 0 {
		s.pending.Store(batch[len(batch)-1].Seq)
		pause()
	}
	return s.Oracle.Write(batch)
}

func (s *slow) LastSeq() uint64 {
	if s.backward && s.calls.Add(1)%2 == 0 && s.Oracle.LastSeq() > 0 {
		return s.Oracle.LastSeq() - 1
	}
	if s.ahead {
		return max(s.Oracle.LastSeq(), s.pending.Load())
	}
	return s.Oracle.LastSeq()
}

func TestCheckConcurrentReads(t *testing.T) {
	t.Parallel()

	if err := conformance.CheckConcurrentReads(&slow{Oracle: oracle.New()}); err != nil {
		t.Fatalf("a slow but consistent engine was reported torn: %v", err)
	}
	if err := conformance.CheckConcurrentReads(&slow{Oracle: oracle.New(), backward: true}); err == nil {
		t.Error("an engine whose LastSeq goes backward was not noticed")
	}
	for name, mk := range map[string]func() engine.Engine{
		"answers half a read from before a batch and half from after it": func() engine.Engine { return &slow{Oracle: oracle.New(), tear: true} },
		"answers a batched read from a snapshot per entry":               func() engine.Engine { return &slow{Oracle: oracle.New(), tearBatchEnd: true} },
		"loses what is before the horizon while it retains":              func() engine.Engine { return &slow{Oracle: oracle.New(), retainTear: true} },
		"raises LastSeq before the batch is visible":                     func() engine.Engine { return &slow{Oracle: oracle.New(), ahead: true} },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			// Whether a tear shows depends on how the goroutines happen to
			// interleave, which a slow or busy machine changes. The check is built so
			// that it shows almost every time; a few attempts make a miss on a
			// loaded runner vanishingly unlikely without hiding a check that cannot
			// see the tear at all, which would miss on every attempt.
			var err error
			for range 5 {
				if err = conformance.CheckConcurrentReads(mk()); errors.Is(err, conformance.ErrTorn) {
					return
				}
			}
			t.Errorf("an engine that %s: err = %v, want ErrTorn", name, err)
		})
	}
}

// refusesReads is an engine that refuses a read of an instant before its retention
// horizon, as a store whose contract allows it does, instead of answering. It
// publishes the horizon before it retains, as the contract says a store does.
// wrongRefusal makes it refuse reads at or after the horizon instead, and
// otherError refuses with an error that is not a refusal for the horizon.
type refusesReads struct {
	*oracle.Oracle
	horizon                  atomic.Int64 // Unix nanoseconds; zero for none
	refused                  atomic.Int64
	wrongRefusal, otherError bool
}

func (r *refusesReads) Retain(h time.Time) error {
	r.horizon.Store(h.UnixNano())
	return r.Oracle.Retain(h)
}

// refuse is the error for a read of the instant t, or nil to answer it.
func (r *refusesReads) refuse(t time.Time) error {
	h := r.horizon.Load()
	if h == 0 {
		return nil
	}
	before := t.Before(time.Unix(0, h))
	switch {
	case r.otherError && before:
		return errors.New("disk on fire")
	case r.wrongRefusal && !before, !r.wrongRefusal && !r.otherError && before:
		r.refused.Add(1)
		return fmt.Errorf("read at %s: %w", t.Format(time.RFC3339), engine.ErrBeforeHorizon)
	}
	return nil
}

func (r *refusesReads) Neighbors(fp identity.Fingerprint, d engine.Direction, at time.Time, sc engine.Scope) ([]engine.Neighbor, error) {
	if err := r.refuse(at); err != nil {
		return nil, err
	}
	return r.Oracle.Neighbors(fp, d, at, sc)
}

func (r *refusesReads) NeighborsBatch(fps []identity.Fingerprint, d engine.Direction, at time.Time, sc engine.Scope) ([][]engine.Neighbor, error) {
	if err := r.refuse(at); err != nil {
		return nil, err
	}
	return r.Oracle.NeighborsBatch(fps, d, at, sc)
}

func (r *refusesReads) Alive(fp identity.Fingerprint, at time.Time, sc engine.Scope) (bool, error) {
	if err := r.refuse(at); err != nil {
		return false, err
	}
	return r.Oracle.Alive(fp, at, sc)
}

func (r *refusesReads) Window(fp identity.Fingerprint, d engine.Direction, from, to time.Time, sc engine.Scope) ([]engine.Record, error) {
	if err := r.refuse(from); err != nil {
		return nil, err
	}
	return r.Oracle.Window(fp, d, from, to, sc)
}

// A read of an instant before the horizon may be refused with ErrBeforeHorizon
// instead of answered (the store contract allows it); the check accepts that. A
// refusal of an instant at or after the horizon, or an error of any other kind,
// still fails it.
func TestConcurrentReadsAcceptARefusalBeforeTheHorizon(t *testing.T) {
	t.Parallel()

	e := &refusesReads{Oracle: oracle.New()}
	if err := conformance.CheckConcurrentReads(e); err != nil {
		t.Fatalf("an engine that refuses reads before the horizon was reported: %v", err)
	}
	// The control means something only if the engine refused some reads.
	if e.refused.Load() == 0 {
		t.Fatal("the engine refused no read, so the check was not put to the test")
	}

	for name, c := range map[string]struct {
		mk   func() *refusesReads
		want func(error) bool
	}{
		"refuses a read at or after the horizon": {
			func() *refusesReads { return &refusesReads{Oracle: oracle.New(), wrongRefusal: true} },
			func(err error) bool { return errors.Is(err, engine.ErrBeforeHorizon) },
		},
		"fails a read before the horizon with another error": {
			func() *refusesReads { return &refusesReads{Oracle: oracle.New(), otherError: true} },
			func(err error) bool { return err != nil && strings.Contains(err.Error(), "disk on fire") },
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			// Which reads overlap the horizon depends on the interleaving; a few
			// attempts make a miss on a loaded runner vanishingly unlikely.
			var err error
			for range 5 {
				if err = conformance.CheckConcurrentReads(c.mk()); c.want(err) {
					return
				}
			}
			t.Errorf("an engine that %s: err = %v", name, err)
		})
	}
}

// refusing counts the writes an engine refuses for being before the horizon.
type refusing struct {
	*oracle.Oracle
	refused atomic.Int64
}

func (r *refusing) Write(batch []engine.Record) error {
	err := r.Oracle.Write(batch)
	if errors.Is(err, engine.ErrBeforeHorizon) {
		r.refused.Add(1)
	}
	return err
}

// TestCheckTellsTheGeneratorTheHorizon: with no late records, nothing the
// generator offers after a retention is older than it, as long as Check tells it
// the horizon. Without that, a heartbeating run is extended at its start, which
// the store would refuse.
func TestCheckTellsTheGeneratorTheHorizon(t *testing.T) {
	t.Parallel()
	cfg := workload.Tiny()
	cfg.CoalesceRuns, cfg.LateProbability = true, 0
	e := &refusing{Oracle: oracle.New()}
	if err := conformance.Check(e, cfg, conformance.Options{RetainAt: []float64{0.3, 0.6}}); err != nil {
		t.Fatal(err)
	}
	if n := e.refused.Load(); n != 0 {
		t.Errorf("%d writes were refused for being before the horizon, in a stream with no late records", n)
	}
}

// lockedCompactor is an engine that really discards history on Retain, by the
// rule the layouts will follow, and is safe for concurrent use because every
// operation holds a lock: a read is one consistent state, a Retain is atomic.
// It is the honest control for the concurrent-read check, which the oracle (that
// discards nothing) cannot be.
type lockedCompactor struct {
	mu      sync.RWMutex
	inner   *oracle.Oracle
	written []engine.Record
	horizon time.Time
	last    uint64
	dropped int // records discarded by retentions so far
}

func (c *lockedCompactor) Write(batch []engine.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.inner.Write(slices.Clone(batch)); err != nil {
		return err
	}
	c.written = append(c.written, cloneRecs(batch)...)
	if len(batch) > 0 {
		c.last = batch[len(batch)-1].Seq
	}
	return nil
}

func cloneRecs(rs []engine.Record) []engine.Record {
	out := slices.Clone(rs)
	for i := range out {
		out[i].Payload = slices.Clone(out[i].Payload)
	}
	return out
}

func (c *lockedCompactor) Retain(h time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !h.After(c.horizon) {
		return nil
	}
	c.horizon = h
	type ref struct {
		subject  engine.Subject
		producer lifecycle.Producer
	}
	newest := map[ref]engine.Record{}
	var kept []engine.Record
	for _, r := range c.written {
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
		end := r.EventTime
		if r.Through.After(end) {
			end = r.Through
		}
		if r.Kind == lifecycle.Observe && (r.TTL == 0 || h.Before(end.Add(r.TTL))) {
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
	if err := fresh.Write(cloneRecs(kept)); err != nil {
		return err
	}
	if err := fresh.Retain(h); err != nil {
		return err
	}
	c.dropped += len(c.written) - len(kept)
	c.inner, c.written = fresh, kept
	return nil
}

func (c *lockedCompactor) LastSeq() uint64      { c.mu.RLock(); defer c.mu.RUnlock(); return c.last }
func (c *lockedCompactor) Size() (int64, error) { return 0, nil }
func (c *lockedCompactor) Close() error         { return nil }

func (c *lockedCompactor) Neighbors(fp identity.Fingerprint, d engine.Direction, at time.Time, sc engine.Scope) ([]engine.Neighbor, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.inner.Neighbors(fp, d, at, sc)
}

func (c *lockedCompactor) NeighborsBatch(fps []identity.Fingerprint, d engine.Direction, at time.Time, sc engine.Scope) ([][]engine.Neighbor, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.inner.NeighborsBatch(fps, d, at, sc)
}

func (c *lockedCompactor) Alive(fp identity.Fingerprint, at time.Time, sc engine.Scope) (bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.inner.Alive(fp, at, sc)
}

func (c *lockedCompactor) Window(fp identity.Fingerprint, d engine.Direction, from, to time.Time, sc engine.Scope) ([]engine.Record, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.inner.Window(fp, d, from, to, sc)
}

func TestConcurrentReadsAgreeWithAnEngineThatReallyDiscardsHistory(t *testing.T) {
	t.Parallel()
	for i := range 2 {
		c := &lockedCompactor{inner: oracle.New()}
		if err := conformance.CheckConcurrentReads(c); err != nil {
			t.Fatalf("run %d: an honest engine that compacts on every retention was reported: %v", i, err)
		}
		// The control means something only if the retentions took history away.
		if c.dropped == 0 || len(c.written) == 0 {
			t.Fatalf("run %d: the engine dropped %d records and kept %d", i, c.dropped, len(c.written))
		}
	}
}
