package storetest

import (
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// LayeredFactory opens the store under test with a retention offset for each layer
// and a flag that keeps it, the options [store.Store.Retain] speaks of. Both are
// indexed by layer minus L0. A store with every offset zero and no layer kept is
// what [Factory] opens.
type LayeredFactory struct {
	// Open opens the store kept under dir, folding entity existence with policy,
	// with the given offsets and kept layers. It must refuse a negative offset with
	// an error wrapping [store.ErrInvalid], whether or not that layer is kept. dir is
	// empty or holds what a store opened by the same factory left when it was
	// closed, possibly with other offsets or kept layers; the store must then come
	// back with its records, LastSeq and every layer's horizon as they were, and
	// its Horizon.
	Open func(dir string, policy lifecycle.Policy, offsets [4]time.Duration, keep [4]bool) (store.Store, error)
	// Durable says that a store opened over the directory of a closed one comes
	// back as it was. The checks that reopen it with other offsets run only for a
	// durable store.
	Durable bool
}

// RunLayers runs [CheckLayerOffsets] as a test.
func RunLayers(t *testing.T, f LayeredFactory) {
	t.Helper()
	t.Run("layer offsets", func(t *testing.T) {
		if err := CheckLayerOffsets(f); err != nil {
			t.Fatal(err)
		}
	})
}

// layerConfig is the offsets and the kept layers a store is opened with.
type layerConfig struct {
	offsets [4]time.Duration
	keep    [4]bool
}

// CheckLayerOffsets checks what the contract says of a store whose layers have
// retention offsets, and of layers that are kept. A layer's horizon after a Retain
// is the Retain's horizon less the layer's offset, but only where that is later
// than the layer's own; a kept layer's horizon never moves. It opens the store with
// offsets of 5, 20, 8 and 0 minutes for L0 to L3 and L0 kept, and
//
//   - requires a negative offset to be refused at Open, for a kept layer too;
//   - writes records in all four layers, retains, and after each step requires
//     LayerHorizon of every layer, Horizon and LastSeq to be what the contract
//     computes, and every read of every kind, one nanosecond before and at each
//     layer's horizon (and at the horizons of the other layers), under the tokens
//     around its Seq, to be refused with [store.ErrBeforeHorizon] by the horizon of
//     the layer the read names, or else answered as a store that has discarded
//     nothing answers;
//   - writes a record one nanosecond before each layer's horizon (refused whole,
//     with no Seq consumed, even in a batch with a valid record) and at it
//     (accepted), and a record of a kept layer however old it is;
//   - retains again with the same horizon, which moves nothing.
//
// For a durable store it then closes it and opens it again with other offsets and
// other kept layers, three times, requiring every layer's horizon and Horizon to be
// as they were (a horizon never moves backward, whatever the offsets), and runs
// Retains that move some layers and not others, two of them with no write between
// that move different layers (so that the Horizon the second reports is not the
// latest time among the layers' horizons), requiring Horizon to be the horizon of
// the layer retained most recently across a reopening too. It closes the store it
// opens. Use a factory whose store is empty to begin with, and the zero policy.
func CheckLayerOffsets(f LayeredFactory) error {
	for i := range 4 {
		for _, kept := range []bool{false, true} {
			var offsets [4]time.Duration
			var keep [4]bool
			offsets[i], keep[i] = -time.Nanosecond, kept
			dir, err := os.MkdirTemp("", "storetest-layers-negative")
			if err != nil {
				return err
			}
			s, err := f.Open(dir, lifecycle.Policy{}, offsets, keep)
			if err == nil {
				_ = s.Close()
			}
			_ = os.RemoveAll(dir)
			if err := wantInvalid(fmt.Sprintf("Open with a retention offset of -1ns for layer %s (kept %v)", catalog.L0+catalog.Layer(i), kept), err); err != nil {
				return err
			}
		}
	}
	c, err := newLayerCheck(f)
	if err != nil {
		return err
	}
	defer c.cleanup()
	return c.run()
}

// layerCheck is the state of one run of [CheckLayerOffsets]: the store, an oracle
// that has been given every accepted record and discards nothing, and the horizons
// the contract says the store has.
type layerCheck struct {
	f      LayeredFactory
	dir    string
	cand   store.Store
	oracle *memstore.Store
	cfg    layerConfig

	// model is the horizon of each layer, last the Horizon, and seq the highest Seq
	// committed: what the contract says the store reports.
	model [4]store.Horizon
	last  store.Horizon
	seq   uint64

	x time.Time
	// pods are the owners of the edges, one for each layer, and ents the entities
	// whose existence is recorded, of the type that lives in the layer.
	pods, ents [4]identity.Fingerprint
	node       identity.Fingerprint
	what       string
}

func newLayerCheck(f LayeredFactory) (*layerCheck, error) {
	c := &layerCheck{f: f, x: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)}
	var err error
	if c.node, err = entityFP(catalog.K8sNode, catalog.K8sNodeUID, "offsets-node"); err != nil {
		return nil, err
	}
	for i, l := range allLayers {
		if c.pods[i], err = entityFP(catalog.K8sPod, catalog.K8sPodUID, "offsets-pod-"+l.String()); err != nil {
			return nil, err
		}
	}
	for i, e := range []struct {
		typ catalog.EntityType
		key catalog.AttributeKey
	}{{catalog.Rack, catalog.RackID}, {catalog.Host, catalog.HostID}, {catalog.K8sPod, catalog.K8sPodUID}, {catalog.Service, catalog.ServiceName}} {
		if c.ents[i], err = entityFP(e.typ, e.key, "offsets-ent"); err != nil {
			return nil, err
		}
	}
	if c.oracle, err = memstore.Open(memstore.Options{}); err != nil {
		return nil, err
	}
	if c.dir, err = os.MkdirTemp("", "storetest-layers"); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *layerCheck) cleanup() {
	if c.cand != nil {
		_ = c.cand.Close()
	}
	_ = c.oracle.Close()
	_ = os.RemoveAll(c.dir)
}

// open closes the store, if there is one, and opens it again with cfg.
func (c *layerCheck) open(cfg layerConfig) error {
	if c.cand != nil {
		if err := c.cand.Close(); err != nil {
			return fmt.Errorf("closing: %w", err)
		}
		c.cand = nil
	}
	s, err := c.f.Open(c.dir, lifecycle.Policy{}, cfg.offsets, cfg.keep)
	if err != nil {
		return fmt.Errorf("opening with offsets %v and kept layers %v: %w", cfg.offsets, cfg.keep, err)
	}
	c.cand, c.cfg = s, cfg
	return nil
}

// after is the Seq the next record follows: the highest committed, or, in an empty
// store, the base that makes the scripted checks' sequence numbers straddle 2^32.
func (c *layerCheck) after() uint64 { return max(c.seq, seqBase) }

func (c *layerCheck) at(minutes int) time.Time { return c.x.Add(time.Duration(minutes) * time.Minute) }

func (c *layerCheck) fail(format string, args ...any) error {
	return fmt.Errorf("%s: %w", c.what, fmt.Errorf(format, args...))
}

// edge and entity build the records the check writes. A subject is in one layer,
// so each layer has its own pod.
func (c *layerCheck) edge(l catalog.Layer, seq uint64, at time.Time, kind lifecycle.Kind) store.Record {
	r := store.Record{
		Layer: l, Subject: store.EdgeSubject(c.pods[l-catalog.L0], c.node, catalog.ScheduledOn), Producer: "alpha",
		EventTime: at, Seq: seq, Kind: kind,
	}
	if kind == lifecycle.Observe { // a delete carries no payload
		r.Payload = []byte(fmt.Sprintf("edge %d", seq))
	}
	return r
}

func (c *layerCheck) entity(l catalog.Layer, seq uint64, at time.Time, kind lifecycle.Kind) store.Record {
	r := store.Record{
		Layer: l, Subject: store.EntitySubject(c.ents[l-catalog.L0]), Producer: "alpha",
		EventTime: at, Seq: seq, Kind: kind,
	}
	if kind == lifecycle.Observe {
		r.Payload = []byte(fmt.Sprintf("entity %d", seq))
	}
	return r
}

// bulk writes, in every layer, an edge and an entity record at each of the minutes
// after the start, in ascending time. All of them must be accepted.
func (c *layerCheck) bulk(minutes ...int) error {
	var batch []store.Record
	for _, m := range minutes {
		edgeKind, entityKind := lifecycle.Observe, lifecycle.Observe
		if m == 9 || m == 35 {
			edgeKind = lifecycle.Delete
		}
		if m == 13 || m == 40 {
			entityKind = lifecycle.Delete
		}
		for _, l := range allLayers {
			batch = append(batch, c.edge(l, c.after()+1+uint64(len(batch)), c.at(m), edgeKind))
			batch = append(batch, c.entity(l, c.after()+1+uint64(len(batch)), c.at(m), entityKind))
		}
	}
	return c.write(batch)
}

// write gives a batch to the store. The contract decides whether it is accepted:
// any record before the horizon of its own layer refuses the whole batch, which
// must then leave no trace, and not even its Seq, behind.
func (c *layerCheck) write(batch []store.Record) error {
	var refused []store.Record
	for _, r := range batch {
		if r.EventTime.Before(c.model[r.Layer-catalog.L0].Time) {
			refused = append(refused, r)
		}
	}
	before := c.cand.LastSeq()
	if before != c.seq {
		return c.fail("LastSeq = %d, want %d", before, c.seq)
	}
	err := c.cand.Write(bg, cloneRecords(batch))
	if len(refused) > 0 {
		r := refused[0]
		if !errors.Is(err, store.ErrBeforeHorizon) {
			return c.fail("a batch with a record of layer %s at %s, before that layer's horizon %s, must be refused with ErrBeforeHorizon, got %w",
				r.Layer, r.EventTime.Format(time.RFC3339Nano), c.model[r.Layer-catalog.L0].Time.Format(time.RFC3339Nano), orNil(err))
		}
		if got := c.cand.LastSeq(); got != before {
			return c.fail("a batch refused for the horizon moved LastSeq from %d to %d", before, got)
		}
		return nil
	}
	if err != nil {
		return c.fail("a batch of %d records at or after the horizon of each layer was refused: %w", len(batch), err)
	}
	if err := c.oracle.Write(bg, cloneRecords(batch)); err != nil {
		return c.fail("the oracle refused a batch the store accepted: %w", err)
	}
	c.seq = batch[len(batch)-1].Seq
	if got := c.cand.LastSeq(); got != c.seq {
		return c.fail("LastSeq = %d after a batch whose last Seq is %d", got, c.seq)
	}
	return nil
}

// retain applies the contract to the model and gives the Retain to the store.
func (c *layerCheck) retain(h time.Time) error {
	var latest store.Horizon
	for i := range c.model {
		if c.cfg.keep[i] {
			continue
		}
		if t := h.UTC().Add(-c.cfg.offsets[i]); t.After(c.model[i].Time) {
			c.model[i] = store.Horizon{Time: t, Seq: c.seq}
			if t.After(latest.Time) {
				latest = c.model[i]
			}
		}
	}
	if !latest.IsZero() {
		c.last = latest
	}
	if err := c.cand.Retain(bg, h); err != nil {
		return c.fail("Retain(%s): %w", h.Format(time.RFC3339Nano), err)
	}
	return nil
}

// horizons requires every layer's horizon, Horizon and LastSeq to be the model's.
func (c *layerCheck) horizons() error {
	for i, l := range allLayers {
		if got := c.cand.LayerHorizon(l); !sameHorizon(got, c.model[i]) {
			return c.fail("LayerHorizon(%s) = %v, want %v", l, got, c.model[i])
		}
	}
	for _, l := range outsideLayers {
		if got := c.cand.LayerHorizon(l); !got.IsZero() {
			return c.fail("LayerHorizon(%s) = %v, want the zero horizon", l, got)
		}
	}
	if got := c.cand.Horizon(); !sameHorizon(got, c.last) {
		return c.fail("Horizon() = %v, want %v (the horizon of the layer retained most recently; the layers hold %v)", got, c.last, c.model)
	}
	if got := c.cand.LastSeq(); got != c.seq {
		return c.fail("LastSeq = %d, want %d", got, c.seq)
	}
	return nil
}

// verify requires the horizons, and then every read around them, to be right.
func (c *layerCheck) verify(what string) error {
	c.what = what
	if err := c.horizons(); err != nil {
		return err
	}
	return c.reads()
}

// reads puts every kind of read, in every layer, at the instants and under the
// tokens around the horizons. A read the contract refuses must be refused with
// ErrBeforeHorizon, and any other must be answered as the oracle, which has
// discarded nothing, answers it.
func (c *layerCheck) reads() error {
	times := []time.Time{store.MinEventTime, c.x.Add(-100 * time.Hour), c.x, c.at(7), c.at(19), c.at(31), c.at(46), c.at(60), c.at(90), c.at(200)}
	for _, h := range c.model {
		if !h.IsZero() {
			times = append(times, h.Time.Add(-time.Nanosecond), h.Time, h.Time.Add(time.Nanosecond))
		}
	}
	for i, l := range allLayers {
		hz := c.model[i]
		tokens := []uint64{store.Latest, c.seq}
		if hz.Seq > 0 {
			tokens = append(tokens, hz.Seq, hz.Seq-1)
		}
		pod := c.pods[i]
		for _, kind := range readKinds {
			owner := pod
			if kind == "Alive" || kind == "EntityWindow" {
				owner = c.ents[i]
			}
			// Neighbors and Window are asked of both ends of the edge.
			ends := []struct {
				fp  identity.Fingerprint
				dir store.Direction
			}{{owner, store.Forward}}
			if kind == "Neighbors" || kind == "Window" {
				ends = append(ends, struct {
					fp  identity.Fingerprint
					dir store.Direction
				}{c.node, store.Reverse})
			}
			for _, end := range ends {
				for _, at := range times {
					for _, tok := range tokens {
						a := readArgs{
							ctx: bg, fp: end.fp, fps: []identity.Fingerprint{pod, c.node}, dir: end.dir,
							t: at, to: at.Add(24 * time.Hour), sc: store.Scope{Layer: l, AsOf: tok},
						}
						refused := at.Before(hz.Time) || tok < hz.Seq
						got, err := ask(c.cand, kind, a)
						if refused {
							if !errors.Is(err, store.ErrBeforeHorizon) {
								return c.fail("%s must be refused with ErrBeforeHorizon (the horizon of %s is %v), got %w", describeArgs(kind, a), l, hz, orNil(err))
							}
							continue
						}
						if err != nil {
							return c.fail("%s must be answered (the horizon of %s is %v): %w", describeArgs(kind, a), l, hz, err)
						}
						want, err := ask(c.oracle, kind, a)
						if err != nil {
							return c.fail("the oracle cannot answer %s: %w", describeArgs(kind, a), err)
						}
						if d := got.diff(want); d != "" {
							return c.fail("%w: %s (the horizon of %s is %v): %s", ErrMismatch, describeArgs(kind, a), l, hz, d)
						}
					}
				}
			}
		}
	}
	return nil
}

// writes probes the horizon of each layer with Writes: one nanosecond before it
// is refused, alone and in a batch with a record that is valid; at it, or for a
// layer with no horizon far in the past, is accepted. These raise LastSeq.
func (c *layerCheck) writes(what string) error {
	c.what = what
	far := c.at(100000) // after every horizon
	for i, l := range allLayers {
		hz := c.model[i]
		other := catalog.L0 + catalog.Layer((i+1)%4)
		if !hz.IsZero() {
			before := c.edge(l, c.after()+1, hz.Time.Add(-time.Nanosecond), lifecycle.Observe)
			if err := c.write([]store.Record{before}); err != nil {
				return err
			}
			// A batch with a valid record first: refused whole, and the valid record's
			// Seq is not consumed.
			valid := c.edge(other, c.after()+1, far, lifecycle.Observe)
			if err := c.write([]store.Record{valid, c.edge(l, c.after()+2, hz.Time.Add(-time.Nanosecond), lifecycle.Observe)}); err != nil {
				return err
			}
		}
		// A layer with no horizon accepts a record however old it is.
		in := c.x.Add(-100 * time.Hour)
		if !hz.IsZero() {
			in = hz.Time
		}
		if err := c.write([]store.Record{c.edge(l, c.after()+1, in, lifecycle.Observe)}); err != nil {
			return err
		}
	}
	return c.verify(what)
}

// run is the script.
func (c *layerCheck) run() error {
	offsets := [4]time.Duration{5 * time.Minute, 20 * time.Minute, 8 * time.Minute, 0}
	a := layerConfig{offsets: offsets, keep: [4]bool{true, false, false, false}}
	c.what = "opening"
	if err := c.open(a); err != nil {
		return err
	}
	if err := c.verify("a new store"); err != nil {
		return err
	}

	c.what = "writing"
	if err := c.bulk(1, 4, 9, 13); err != nil {
		return err
	}
	// The kept layer, L0, has the offset 5 minutes and is not moved; L1 is 20
	// minutes behind the Retain, L2 8, and L3 at it.
	if err := c.retain(c.at(25)); err != nil {
		return err
	}
	if err := c.verify("Retain(+25m) with offsets 5, 20, 8, 0 minutes and L0 kept"); err != nil {
		return err
	}
	if err := c.writes("writes around the horizons after Retain(+25m)"); err != nil {
		return err
	}
	if err := c.retain(c.at(25)); err != nil {
		return err
	}
	if err := c.verify("Retain(+25m) again, which moves nothing"); err != nil {
		return err
	}
	c.what = "writing"
	if err := c.bulk(30, 40, 55, 70); err != nil {
		return err
	}
	if err := c.retain(c.at(40)); err != nil {
		return err
	}
	if err := c.verify("Retain(+40m)"); err != nil {
		return err
	}

	if !c.f.Durable {
		return c.finish()
	}

	// Reopened with other offsets: L1, retained before, is now kept; L0, which was
	// kept and so has no horizon, is not. Every stored horizon stays.
	b := layerConfig{offsets: [4]time.Duration{0, 0, 15 * time.Minute, 0}, keep: [4]bool{false, true, false, false}}
	c.what = "reopening"
	if err := c.open(b); err != nil {
		return err
	}
	if err := c.verify("a reopening with offsets 0, 0, 15, 0 minutes and L1 kept"); err != nil {
		return err
	}
	// Moves L0 alone, to a horizon earlier than the others', with no write since
	// the Retain that moved L1, L2 and L3: the Horizon is L0's, not the latest time.
	if err := c.retain(c.at(10)); err != nil {
		return err
	}
	if err := c.verify("Retain(+10m), which moves only L0, to a horizon earlier than the others'"); err != nil {
		return err
	}
	c.what = "reopening"
	if err := c.open(b); err != nil {
		return err
	}
	if err := c.verify("a reopening after a Retain that moved only L0"); err != nil {
		return err
	}
	if err := c.retain(c.at(50)); err != nil {
		return err
	}
	if err := c.verify("Retain(+50m), which moves L0, L2 and L3 and not the kept L1"); err != nil {
		return err
	}
	if err := c.writes("writes around the horizons after Retain(+50m)"); err != nil {
		return err
	}
	if err := c.bulk(80, 90); err != nil {
		return err
	}

	// Reopened with L0 kept and larger offsets: no layer moves backward, and a
	// Retain moves only the layers whose new horizon is later than their own.
	cfgC := layerConfig{offsets: [4]time.Duration{0, 0, 30 * time.Minute, 10 * time.Minute}, keep: [4]bool{true, false, false, false}}
	c.what = "reopening"
	if err := c.open(cfgC); err != nil {
		return err
	}
	if err := c.verify("a reopening with offsets 0, 0, 30, 10 minutes and L0 kept"); err != nil {
		return err
	}
	if err := c.retain(c.at(45)); err != nil {
		return err
	}
	if err := c.verify("Retain(+45m), which moves only L1"); err != nil {
		return err
	}
	for _, m := range []int{45, 44, 0} {
		if err := c.retain(c.at(m)); err != nil {
			return err
		}
		if err := c.verify(fmt.Sprintf("Retain(+%dm), which moves nothing", m)); err != nil {
			return err
		}
	}
	if err := c.retain(c.at(100)); err != nil {
		return err
	}
	if err := c.verify("Retain(+100m) with offsets 0, 0, 30, 10 minutes and L0 kept"); err != nil {
		return err
	}
	if err := c.writes("writes around the horizons after Retain(+100m)"); err != nil {
		return err
	}
	return c.finish()
}

// finish closes the store, which keeps reporting the horizons it had.
func (c *layerCheck) finish() error {
	c.what = "closing"
	if err := c.cand.Close(); err != nil {
		return c.fail("Close: %w", err)
	}
	return c.horizons()
}
