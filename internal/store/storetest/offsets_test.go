package storetest_test

import (
	"context"
	"fmt"
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

// The reference satisfies the check of retention offsets. As memstore keeps nothing
// across a close, this is the part of the check that does not reopen.
func TestMemstoreConformsToTheLayerOffsets(t *testing.T) {
	t.Parallel()
	storetest.RunLayers(t, storetest.LayeredFactory{
		Open: func(_ string, p lifecycle.Policy, offsets [4]time.Duration, keep [4]bool) (store.Store, error) {
			return memstore.Open(memstore.Options{Policy: p, Offsets: offsets, Keep: keep})
		},
	})
}

// layerDamage is one way a store with retention offsets gets the contract wrong.
// Each is applied to a double that is otherwise honest.
type layerDamage struct {
	// readLayer, if set, names the layer whose horizon judges a read of a layer, and
	// writeLayer the same for a record.
	readLayer, writeLayer func(catalog.Layer) catalog.Layer
	// moveKept lets a Retain move a kept layer.
	moveKept bool
	// moveAll makes a Retain publish the horizon of every layer that is not kept,
	// whether or not it moves it, and so move one backward.
	moveAll bool
	// ignoreOffsets makes every retained layer's horizon the Retain's.
	ignoreOffsets bool
	// addOffsets adds the offset to the horizon where it should be subtracted.
	addOffsets bool
	// maxHorizon makes Horizon the latest of the layers' horizons.
	maxHorizon bool
	// acceptNegative lets Open take a negative offset.
	acceptNegative bool
	// rebaseOnOpen moves a stored horizon back by the layer's offset when the store
	// is opened again; lastDerivedOnOpen derives Horizon from the layers when it is
	// opened again; forgetOnOpen loses the horizon of the first layer that has one.
	rebaseOnOpen, lastDerivedOnOpen, forgetOnOpen bool
	// keepSeq leaves a layer a Retain moves its old Seq.
	keepSeq bool
}

// layerDisks is the double of the directories a durable store keeps: for each, the
// batches it accepted, the horizon of each layer and the Horizon. A store opened
// over a directory that is known has them replayed.
type layerDisks struct {
	mu     sync.Mutex
	dirs   map[string]*layerDisk
	damage layerDamage
}

type layerDisk struct {
	log  [][]store.Record
	hz   [4]store.Horizon
	last store.Horizon
}

// layerDouble answers reads from a memstore that is never retained, and judges them
// by the horizons it keeps itself, so a damage to how it judges is not undone by the
// memstore's own horizons.
type layerDouble struct {
	mu      sync.Mutex
	inner   *memstore.Store
	disk    *layerDisk
	disks   *layerDisks
	offsets [4]time.Duration
	keep    [4]bool
	closed  bool
	d       layerDamage
}

func layerIndex(l catalog.Layer) (int, bool) {
	if l < catalog.L0 || l > catalog.L3 {
		return 0, false
	}
	return int(l - catalog.L0), true
}

func (ds *layerDisks) open(dir string, p lifecycle.Policy, offsets [4]time.Duration, keep [4]bool) (store.Store, error) {
	if !ds.damage.acceptNegative {
		for i, o := range offsets {
			if o < 0 {
				return nil, fmt.Errorf("the offset of layer %d is negative: %w", i, store.ErrInvalid)
			}
		}
	}
	inner, err := memstore.Open(memstore.Options{Policy: p})
	if err != nil {
		return nil, err
	}
	ds.mu.Lock()
	defer ds.mu.Unlock()
	if ds.dirs == nil {
		ds.dirs = map[string]*layerDisk{}
	}
	disk, known := ds.dirs[dir]
	if !known {
		disk = &layerDisk{}
		ds.dirs[dir] = disk
	}
	for _, batch := range disk.log {
		if err := inner.Write(bg, cloneRecs(batch)); err != nil {
			return nil, err
		}
	}
	if known {
		if ds.damage.rebaseOnOpen {
			for i := range disk.hz {
				if !disk.hz[i].IsZero() {
					disk.hz[i].Time = disk.hz[i].Time.Add(-offsets[i])
				}
			}
		}
		if ds.damage.lastDerivedOnOpen {
			disk.last = latest(disk.hz)
		}
		if ds.damage.forgetOnOpen {
			for i := range disk.hz {
				if !disk.hz[i].IsZero() {
					disk.hz[i] = store.Horizon{}
					break
				}
			}
		}
	}
	return &layerDouble{inner: inner, disk: disk, disks: ds, offsets: offsets, keep: keep, d: ds.damage}, nil
}

// latest is the horizon with the latest time, the lowest layer's among equals.
func latest(hz [4]store.Horizon) store.Horizon {
	var out store.Horizon
	for _, h := range hz {
		if h.Time.After(out.Time) {
			out = h
		}
	}
	return out
}

func (d *layerDouble) judge(l catalog.Layer, t time.Time, asOf uint64) error {
	if d.d.readLayer != nil {
		l = d.d.readLayer(l)
	}
	i, ok := layerIndex(l)
	if !ok {
		return nil
	}
	if h := d.disk.hz[i]; t.Before(h.Time) || asOf < h.Seq {
		return fmt.Errorf("layerDouble: %w", store.ErrBeforeHorizon)
	}
	return nil
}

func (d *layerDouble) Write(ctx context.Context, batch []store.Record) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return store.ErrClosed
	}
	for _, r := range batch {
		l := r.Layer
		if d.d.writeLayer != nil {
			l = d.d.writeLayer(l)
		}
		if i, ok := layerIndex(l); ok && r.EventTime.Before(d.disk.hz[i].Time) {
			return fmt.Errorf("layerDouble: %w", store.ErrBeforeHorizon)
		}
	}
	if err := d.inner.Write(ctx, batch); err != nil {
		return err
	}
	if len(batch) > 0 {
		d.disks.mu.Lock()
		d.disk.log = append(d.disk.log, cloneRecs(batch))
		d.disks.mu.Unlock()
	}
	return nil
}

func (d *layerDouble) LastSeq() uint64 { return d.inner.LastSeq() }

func (d *layerDouble) Horizon() store.Horizon {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.d.maxHorizon {
		return latest(d.disk.hz)
	}
	return d.disk.last
}

func (d *layerDouble) LayerHorizon(l catalog.Layer) store.Horizon {
	d.mu.Lock()
	defer d.mu.Unlock()
	if i, ok := layerIndex(l); ok {
		return d.disk.hz[i]
	}
	return store.Horizon{}
}

func (d *layerDouble) Retain(_ context.Context, horizon time.Time) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return store.ErrClosed
	}
	seq := d.inner.LastSeq()
	var moved store.Horizon
	for i := range d.disk.hz {
		if d.keep[i] && !d.d.moveKept {
			continue
		}
		off := d.offsets[i]
		switch {
		case d.d.ignoreOffsets:
			off = 0
		case d.d.addOffsets:
			off = -off
		}
		t := horizon.UTC().Add(-off)
		if !t.After(d.disk.hz[i].Time) && !d.d.moveAll {
			continue
		}
		s := seq
		if d.d.keepSeq && !d.disk.hz[i].IsZero() {
			s = d.disk.hz[i].Seq
		}
		d.disk.hz[i] = store.Horizon{Time: t, Seq: s}
		if t.After(moved.Time) {
			moved = d.disk.hz[i]
		}
	}
	if !moved.IsZero() {
		d.disk.last = moved
	}
	return nil
}

func (d *layerDouble) Neighbors(ctx context.Context, fp identity.Fingerprint, dir store.Direction, t time.Time, sc store.Scope) ([]store.Neighbor, error) {
	if err := d.judge(sc.Layer, t, sc.AsOf); err != nil {
		return nil, err
	}
	return d.inner.Neighbors(ctx, fp, dir, t, sc)
}

func (d *layerDouble) NeighborsBatch(ctx context.Context, fps []identity.Fingerprint, dir store.Direction, t time.Time, sc store.Scope) ([][]store.Neighbor, error) {
	if err := d.judge(sc.Layer, t, sc.AsOf); err != nil {
		return nil, err
	}
	return d.inner.NeighborsBatch(ctx, fps, dir, t, sc)
}

func (d *layerDouble) Alive(ctx context.Context, fp identity.Fingerprint, t time.Time, sc store.Scope) (bool, error) {
	if err := d.judge(sc.Layer, t, sc.AsOf); err != nil {
		return false, err
	}
	return d.inner.Alive(ctx, fp, t, sc)
}

func (d *layerDouble) Window(ctx context.Context, fp identity.Fingerprint, dir store.Direction, from, to time.Time, sc store.Scope) ([]store.Record, error) {
	if err := d.judge(sc.Layer, from, sc.AsOf); err != nil {
		return nil, err
	}
	return d.inner.Window(ctx, fp, dir, from, to, sc)
}

func (d *layerDouble) EntityWindow(ctx context.Context, fp identity.Fingerprint, from, to time.Time, sc store.Scope) ([]store.Record, error) {
	if err := d.judge(sc.Layer, from, sc.AsOf); err != nil {
		return nil, err
	}
	return d.inner.EntityWindow(ctx, fp, from, to, sc)
}

func (d *layerDouble) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.closed = true
	return d.inner.Close()
}

func layeredDouble(d layerDamage, durable bool) storetest.LayeredFactory {
	ds := &layerDisks{damage: d}
	return storetest.LayeredFactory{Open: ds.open, Durable: durable}
}

// A double with no damage conforms, durable or not, so that what the mutants below
// are caught for is the damage.
func TestAnHonestLayeredDoubleConforms(t *testing.T) {
	t.Parallel()
	for _, durable := range []bool{false, true} {
		t.Run(fmt.Sprintf("durable %v", durable), func(t *testing.T) {
			t.Parallel()
			storetest.RunLayers(t, layeredDouble(layerDamage{}, durable))
		})
	}
}

// A check that cannot tell a broken store from a good one is worthless, so each of
// these must fail it.
func TestTheLayerOffsetsCheckCatchesBrokenStores(t *testing.T) {
	t.Parallel()
	layer := func(l catalog.Layer) func(catalog.Layer) catalog.Layer {
		return func(catalog.Layer) catalog.Layer { return l }
	}
	next := func(l catalog.Layer) catalog.Layer { return catalog.L0 + (l-catalog.L0+1)%4 }
	for name, tc := range map[string]struct {
		d       layerDamage
		durable bool // the damage needs a store that is opened again
	}{
		"reads judged by the horizon of L3":             {d: layerDamage{readLayer: layer(catalog.L3)}},
		"reads judged by the horizon of the next layer": {d: layerDamage{readLayer: next}},
		"reads judged by the horizon of L0":             {d: layerDamage{readLayer: layer(catalog.L0)}},
		"writes judged by the horizon of L3":            {d: layerDamage{writeLayer: layer(catalog.L3)}},
		"writes judged by the horizon of the next":      {d: layerDamage{writeLayer: next}},
		"a Retain that moves a kept layer":              {d: layerDamage{moveKept: true}},
		"a Retain that publishes every layer":           {d: layerDamage{moveAll: true}},
		"offsets ignored":                               {d: layerDamage{ignoreOffsets: true}},
		"offsets added":                                 {d: layerDamage{addOffsets: true}},
		"Horizon is the latest of the layers":           {d: layerDamage{maxHorizon: true}, durable: true},
		"a negative offset accepted":                    {d: layerDamage{acceptNegative: true}},
		"a moved layer keeps its Seq":                   {d: layerDamage{keepSeq: true}},
		"a reopening moves horizons back":               {d: layerDamage{rebaseOnOpen: true}, durable: true},
		"a reopening derives Horizon":                   {d: layerDamage{lastDerivedOnOpen: true}, durable: true},
		"a reopening loses a horizon":                   {d: layerDamage{forgetOnOpen: true}, durable: true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if err := storetest.CheckLayerOffsets(layeredDouble(tc.d, true)); err == nil {
				t.Error("the check did not catch it")
			} else {
				t.Logf("caught: %v", err)
			}
			if !tc.durable {
				// A store that is not durable is caught too, by the part that does not reopen it.
				if err := storetest.CheckLayerOffsets(layeredDouble(tc.d, false)); err == nil {
					t.Error("the check did not catch it without a reopening")
				}
			}
		})
	}
}
