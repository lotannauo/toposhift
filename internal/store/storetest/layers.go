package storetest

import (
	"fmt"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

// outsideLayers are layers that are not L0 to L3: the zero value, the one past L3,
// and the largest value.
var outsideLayers = []catalog.Layer{0, catalog.L3 + 1, catalog.Layer(255)}

// CheckLayerHorizons checks the horizon each layer has. A new store has the zero
// horizon in every layer. After a Retain, with every retention offset zero (which
// is how the suite opens a store), the horizon of each of the four layers is the
// one Horizon returns, {the instant, the LastSeq at the call}; a Retain that does
// not move it changes none of them; and a layer outside L0 to L3 has the zero
// Horizon, always. Whatever a layer's horizon is, a read in that layer one
// nanosecond before it, or a token below its Seq, is refused with
// store.ErrBeforeHorizon, and the instant and token themselves are answered; a
// write of a record of that layer one nanosecond before it is refused, whole and
// without consuming a sequence number, and one at it is accepted. After Close,
// LayerHorizon keeps the values it had. It closes the store it is given. Use a
// fresh store opened with the zero policy and every retention offset zero.
func CheckLayerHorizons(s store.Store) error {
	if h := s.Horizon(); !h.IsZero() {
		return fmt.Errorf("a new store has Horizon %v, want the zero horizon", h)
	}
	if err := wantLayerHorizons(s, "a new store", uniform(store.Horizon{})); err != nil {
		return err
	}

	// One edge in each layer, from a pod of its own to one node, and one record of
	// each, a minute apart.
	x := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	node, err := entityFP(catalog.K8sNode, catalog.K8sNodeUID, "layer-node")
	if err != nil {
		return err
	}
	pods := map[catalog.Layer]identity.Fingerprint{}
	for _, l := range allLayers {
		if pods[l], err = entityFP(catalog.K8sPod, catalog.K8sPodUID, "layer-pod-"+l.String()); err != nil {
			return err
		}
	}
	edge := func(l catalog.Layer, seq uint64, at time.Time) store.Record {
		return store.Record{
			Layer: l, Subject: store.EdgeSubject(pods[l], node, catalog.ScheduledOn), Producer: "alpha",
			EventTime: at, Seq: seq, Kind: lifecycle.Observe, Payload: []byte("edge"),
		}
	}
	n0 := uint64(seqBase)
	var batch []store.Record
	for i, l := range allLayers {
		batch = append(batch, edge(l, n0+1+uint64(i), x.Add(time.Duration(i)*time.Minute)))
	}
	if err := s.Write(bg, cloneRecords(batch)); err != nil {
		return fmt.Errorf("a valid batch was refused: %w", err)
	}
	n := n0 + uint64(len(batch))

	// A Retain moves every layer to the same horizon, because every offset is zero.
	h1 := x.Add(10 * time.Minute)
	if err := s.Retain(bg, h1); err != nil {
		return fmt.Errorf("retaining: %w", err)
	}
	want := store.Horizon{Time: h1, Seq: n}
	if err := wantLayerHorizons(s, fmt.Sprintf("Retain(%s)", h1.Format(time.RFC3339Nano)), uniform(want)); err != nil {
		return err
	}

	// Reads and writes are refused by the horizon of their own layer.
	for _, l := range allLayers {
		hz := s.LayerHorizon(l)
		for _, kind := range readKinds {
			a := readArgs{
				ctx: bg, fp: pods[l], fps: []identity.Fingerprint{pods[l], node}, dir: store.Forward,
				t: hz.Time, to: hz.Time.Add(time.Hour), sc: store.Current(l),
			}
			for _, c := range []struct {
				what    string
				mod     func(*readArgs)
				refused bool
			}{
				{"one nanosecond before the horizon", func(a *readArgs) { a.t = hz.Time.Add(-time.Nanosecond) }, true},
				{"the horizon itself", func(*readArgs) {}, false},
				{"the token below the horizon's Seq", func(a *readArgs) { a.sc.AsOf = hz.Seq - 1 }, true},
				{"the horizon's Seq", func(a *readArgs) { a.sc.AsOf = hz.Seq }, false},
			} {
				b := a
				c.mod(&b)
				if err := horizonOutcome(s, kind, b, "the "+l.String()+" horizon: "+c.what, c.refused); err != nil {
					return err
				}
			}
		}
	}
	for _, l := range allLayers {
		hz := s.LayerHorizon(l)
		err := s.Write(bg, []store.Record{edge(l, n+1, hz.Time.Add(-time.Nanosecond))})
		if err := wantIs(fmt.Sprintf("a Write in layer %s one nanosecond before its horizon", l), err, store.ErrBeforeHorizon); err != nil {
			return err
		}
		if got := s.LastSeq(); got != n {
			return fmt.Errorf("a Write in layer %s refused for the horizon moved LastSeq to %d, want %d", l, got, n)
		}
	}
	for i, l := range allLayers {
		hz := s.LayerHorizon(l)
		if err := s.Write(bg, []store.Record{edge(l, n+1+uint64(i), hz.Time)}); err != nil {
			return fmt.Errorf("a Write in layer %s exactly at its horizon was refused: %w", l, err)
		}
	}
	last := n + uint64(len(allLayers))

	// A Retain that does not move the horizon moves none of them, and raises no Seq.
	for _, earlier := range []time.Time{x.Add(9 * time.Minute), h1} {
		if err := s.Retain(bg, earlier); err != nil {
			return fmt.Errorf("Retain(%s), which does not move the horizon, must be accepted: %w", earlier.Format(time.RFC3339Nano), err)
		}
		if err := wantLayerHorizons(s, fmt.Sprintf("Retain(%s), which does not move it,", earlier.Format(time.RFC3339Nano)), uniform(want)); err != nil {
			return err
		}
	}

	// A Retain that moves it publishes the LastSeq at the call, in every layer.
	h2 := x.Add(15 * time.Minute)
	if err := s.Retain(bg, h2); err != nil {
		return fmt.Errorf("retaining: %w", err)
	}
	want = store.Horizon{Time: h2, Seq: last}
	if err := wantLayerHorizons(s, fmt.Sprintf("Retain(%s)", h2.Format(time.RFC3339Nano)), uniform(want)); err != nil {
		return err
	}

	// After Close the horizons are the ones the store had.
	if err := s.Close(); err != nil {
		return fmt.Errorf("closing: %w", err)
	}
	return wantLayerHorizons(s, "Close", uniform(want))
}

// uniform is the horizon of every layer being h.
func uniform(h store.Horizon) func(catalog.Layer) store.Horizon {
	return func(catalog.Layer) store.Horizon { return h }
}

// wantLayerHorizons requires LayerHorizon to give want(l) for each of the four
// layers, that to be what Horizon gives (every offset is zero, so the layers share
// one horizon), and the zero Horizon for a layer outside L0 to L3. after says what
// preceded the call, for the message.
func wantLayerHorizons(s store.Store, after string, want func(catalog.Layer) store.Horizon) error {
	for _, l := range allLayers {
		got, w := s.LayerHorizon(l), want(l)
		if !sameHorizon(got, w) {
			return fmt.Errorf("LayerHorizon(%s) = %v after %s; want %v", l, got, after, w)
		}
		if h := s.Horizon(); !sameHorizon(got, h) {
			return fmt.Errorf("LayerHorizon(%s) = %v after %s but Horizon() = %v; with every offset zero they are the same", l, got, after, h)
		}
	}
	for _, l := range outsideLayers {
		if got := s.LayerHorizon(l); !got.IsZero() {
			return fmt.Errorf("LayerHorizon(%s) = %v after %s; a layer outside L0 to L3 has the zero horizon", l, got, after)
		}
	}
	return nil
}
