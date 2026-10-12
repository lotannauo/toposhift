package pebblelog

import (
	"context"
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
)

var (
	_ engine.Quiescer      = (*Engine)(nil)
	_ engine.ColdStarter   = (*Engine)(nil)
	_ engine.Statser       = (*Engine)(nil)
	_ engine.LayerSizer    = (*Engine)(nil)
	_ engine.Describer     = (*Engine)(nil)
	_ engine.Breakdowner   = (*Engine)(nil)
	_ engine.Canonicalizer = (*Engine)(nil)
)

// Quiesce implements [engine.Quiescer].
func (e *Engine) Quiesce(ctx context.Context) error { return e.kv.Quiesce(ctx) }

// ColdStart implements [engine.ColdStarter].
func (e *Engine) ColdStart() { e.kv.ColdStart() }

// CompactAll implements [engine.Quiescer].
func (e *Engine) CompactAll(ctx context.Context) error { return e.kv.CompactAll(ctx) }

// Canonicalize implements [engine.Canonicalizer]. The engine's own sequence number,
// horizon and the rest of its state are keys, rewritten with the others.
func (e *Engine) Canonicalize(ctx context.Context) error { return e.kv.Canonicalize(ctx) }

// Stats implements [engine.Statser].
func (e *Engine) Stats() map[string]int64 { return e.kv.Snapshot().Flat() }

// Describe implements [engine.Describer].
func (e *Engine) Describe() (map[string]string, error) {
	d, err := e.kv.Describe()
	if err != nil {
		return nil, err
	}
	d["layout"] = "L"
	cp := e.ckpt
	d["checkpoints"] = "off"
	if cp.On {
		d["checkpoints"] = fmt.Sprintf("kmin=%d alpha=%g lag=%s", cp.KMin, cp.Alpha, cp.Lag)
		if cp.Replace > 0 { // said only when on, so every earlier description reads as before
			d["checkpoints"] += fmt.Sprintf(" replace=%d", cp.Replace)
		}
	}
	return d, nil
}

// SizeByLayer implements [engine.LayerSizer]: Pebble's estimate of the bytes of
// tables that hold each layer's keys (the key range of a layer is one contiguous
// span, since a layer leads every key). It counts tables, not the log or what is
// still in the memtable, so it is meant for a database at rest.
func (e *Engine) SizeByLayer() (map[catalog.Layer]int64, error) {
	out := map[catalog.Layer]int64{}
	for _, layer := range []catalog.Layer{catalog.L0, catalog.L1, catalog.L2, catalog.L3} {
		lo, hi := prefixBounds([]byte{pebblekv.LayerByte(layer)})
		n, err := e.kv.EstimateDiskUsage(lo, hi)
		if err != nil {
			return nil, fmt.Errorf("pebblelog: size of %s: %w", layer, err)
		}
		if n > 0 {
			out[layer] = int64(n)
		}
	}
	return out, nil
}

// Breakdown implements [engine.Breakdowner]. It scans every data key and divides
// the bytes of key and value into the parts named in [pebblekv]: a record's bytes
// by its kind, its payload by the direction it is stored under, and a checkpoint
// or the baseline as a whole.
func (e *Engine) Breakdown() (map[engine.Part]int64, error) {
	lo, hi := dataBounds()
	it, err := e.kv.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return nil, err
	}
	defer func() { _ = it.Close() }()
	out := map[engine.Part]int64{}
	for ok := it.First(); ok; ok = it.Next() {
		prefix, _, _, kind, err := parseKey(it.Key())
		if err != nil {
			return nil, err
		}
		layer, ok := pebblekv.LayerFromByte(prefix[0])
		if !ok {
			return nil, fmt.Errorf("pebblelog: key %x has no layer", it.Key())
		}
		total := len(it.Key()) + len(it.Value())
		switch kind {
		case kindCheckpoint:
			out[engine.Part{Layer: layer, Kind: pebblekv.PartCheckpoint}] += int64(total)
		case kindBaseline:
			out[engine.Part{Layer: layer, Kind: pebblekv.PartBaseline}] += int64(total)
		default:
			dir := prefix[prefixLen-1]
			_, v, err := decodeRecordValue(dir, it.Value())
			if err != nil {
				return nil, err
			}
			k, kb, p, pb := pebblekv.RecordParts(v, dir, total)
			out[engine.Part{Layer: layer, Kind: k}] += int64(kb)
			out[engine.Part{Layer: layer, Kind: p}] += int64(pb)
		}
	}
	return out, it.Error()
}

// PrefixTallier is what an engine offers that counts, per prefix, the checkpoints a
// record deleted and those a write put back or did not.
type PrefixTallier interface {
	// PrefixTallies is, for each prefix (its key bytes) with a count above zero,
	// the checkpoints deleted, put back and not put back for either cap, or nil if
	// the engine does not keep them.
	PrefixTallies() map[string][3]int64
}

var _ PrefixTallier = (*Engine)(nil)

// PrefixTallies implements [PrefixTallier]: it is nil unless the engine was
// opened with [Options].TallyPrefixes. The counts are of this opening, in memory,
// and not stored.
func (e *Engine) PrefixTallies() map[string][3]int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.tallies == nil {
		return nil
	}
	out := make(map[string][3]int64, len(e.tallies))
	for k, t := range e.tallies {
		out[k] = *t
	}
	return out
}

// The kinds of key [PrefixScanner] gives.
const (
	KeyRecord     = kindRecord
	KeyCheckpoint = kindCheckpoint
	KeyBaseline   = kindBaseline
)

// StoredKey is one key of a prefix as a scan meets it, in key order (newest event
// time first).
type StoredKey struct {
	Kind    byte  // [KeyRecord], [KeyCheckpoint] or [KeyBaseline]
	EventNs int64 // the event time, in Unix nanoseconds
	// Seq is a record's sequence number; zero for a checkpoint or the baseline.
	Seq uint64
	// W is a checkpoint's or the baseline's W (see checkpoint.go), and Current says
	// the fold logic that built it is this code's; both are zero for a record.
	W       uint64
	Current bool
}

// PrefixScanner is what an engine offers that lists the keys of one prefix, for a
// tool that looks at where the checkpoints lie.
type PrefixScanner interface {
	// ScanPrefix calls fn for each key of the prefix (layer, entity, direction) in
	// key order, until fn returns false. Reading a prefix the catalog does not know
	// finds nothing.
	ScanPrefix(layer catalog.Layer, fp identity.Fingerprint, dir engine.Direction, fn func(StoredKey) bool) error
	// PrefixOf is the key bytes of the prefix (the keys of [PrefixTallier]), or false
	// if the entity's type is not in the catalog.
	PrefixOf(layer catalog.Layer, fp identity.Fingerprint, dir engine.Direction) ([]byte, bool)
}

var _ PrefixScanner = (*Engine)(nil)

// PrefixOf implements [PrefixScanner].
func (e *Engine) PrefixOf(layer catalog.Layer, fp identity.Fingerprint, dir engine.Direction) ([]byte, bool) {
	return e.prefixOf(layer, fp, byte(dir))
}

// ScanPrefix implements [PrefixScanner]. It reads no value but a stamp's.
func (e *Engine) ScanPrefix(layer catalog.Layer, fp identity.Fingerprint, dir engine.Direction, fn func(StoredKey) bool) error {
	if err := checkDir(dir); err != nil {
		return err
	}
	prefix, ok := e.prefixOf(layer, fp, byte(dir))
	if !ok {
		return nil
	}
	lo, hi := prefixBounds(prefix)
	it, err := e.kv.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return err
	}
	defer func() { _ = it.Close() }()
	for ok := it.First(); ok; ok = it.Next() {
		_, ns, seq, kind, err := parseKey(it.Key())
		if err != nil {
			return err
		}
		k := StoredKey{Kind: kind, EventNs: ns, Seq: seq}
		if kind != kindRecord {
			st, err := decodeStamp(it.Value())
			if err != nil {
				return err
			}
			if st.Kind != kind {
				return fmt.Errorf("pebblelog: a key of kind %d holds a stamp of kind %d", kind, st.Kind)
			}
			k.W, k.Current = st.W, st.FoldVersion == FoldVersion
		}
		if !fn(k) {
			return nil
		}
	}
	return it.Error()
}
