package pebblemvcc

import (
	"context"
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
	"github.com/lotannauo/toposhift/internal/catalog"
)

var (
	_ engine.Quiescer    = (*Engine)(nil)
	_ engine.Statser     = (*Engine)(nil)
	_ engine.LayerSizer  = (*Engine)(nil)
	_ engine.Describer   = (*Engine)(nil)
	_ engine.Breakdowner = (*Engine)(nil)
)

// Quiesce implements [engine.Quiescer].
func (e *Engine) Quiesce(ctx context.Context) error { return e.kv.Quiesce(ctx) }

// CompactAll implements [engine.Quiescer].
func (e *Engine) CompactAll(ctx context.Context) error { return e.kv.CompactAll(ctx) }

// Stats implements [engine.Statser].
func (e *Engine) Stats() map[string]int64 { return e.kv.Snapshot().Flat() }

// Describe implements [engine.Describer].
func (e *Engine) Describe() (map[string]string, error) {
	d, err := e.kv.Describe()
	if err != nil {
		return nil, err
	}
	d["layout"] = "M"
	return d, nil
}

// SizeByLayer implements [engine.LayerSizer]: Pebble's estimate of the bytes of
// tables that hold each layer's keys (the key range of a layer is one contiguous
// span, since the layer leads every roach key). It counts tables, not the log or
// what is still in the memtable, so it is meant for a database at rest.
func (e *Engine) SizeByLayer() (map[catalog.Layer]int64, error) {
	out := map[catalog.Layer]int64{}
	for _, layer := range []catalog.Layer{catalog.L0, catalog.L1, catalog.L2, catalog.L3} {
		lo, hi := prefixBounds([]byte{pebblekv.LayerByte(layer)})
		n, err := e.kv.EstimateDiskUsage(lo, hi)
		if err != nil {
			return nil, fmt.Errorf("pebblemvcc: size of %s: %w", layer, err)
		}
		if n > 0 {
			out[layer] = int64(n)
		}
	}
	return out, nil
}

// Breakdown implements [engine.Breakdowner]. It scans every data key and divides
// the bytes of key and value into the parts named in [pebblekv]: a version's bytes
// by its kind, and its payload by the direction it is stored under.
func (e *Engine) Breakdown() (map[engine.Part]int64, error) {
	lo, hi := dataBounds()
	it, err := e.kv.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return nil, err
	}
	defer func() { _ = it.Close() }()
	out := map[engine.Part]int64{}
	for ok := it.First(); ok; ok = it.Next() {
		roach, _, _, err := split(it.Key())
		if err != nil {
			return nil, err
		}
		layer, ok := pebblekv.LayerFromByte(roach[0])
		if !ok {
			return nil, fmt.Errorf("pebblemvcc: key %x has no layer", it.Key())
		}
		v, err := pebblekv.DecodeValue(it.Value())
		if err != nil {
			return nil, err
		}
		k, kb, p, pb := pebblekv.RecordParts(v, roach[prefixLen-1], len(it.Key())+len(it.Value()))
		out[engine.Part{Layer: layer, Kind: k}] += int64(kb)
		out[engine.Part{Layer: layer, Kind: p}] += int64(pb)
	}
	return out, it.Error()
}
