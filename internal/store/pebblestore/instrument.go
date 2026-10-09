package pebblestore

import (
	"fmt"
	"time"

	"github.com/cockroachdb/pebble/v2"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store/pebblekv"
)

// Instrument is what tests and the benchmarks look into a store with. It is not
// part of the store's contract, and a caller that is neither has no use for it.
type Instrument struct{ s *Store }

// Instrument returns the store's instrument.
func (s *Store) Instrument() *Instrument { return &Instrument{s} }

// KV is the database under the store. Writing to it breaks the store.
func (i *Instrument) KV() *pebblekv.KV { return i.s.kv }

// LastRetain is how the last call of Retain spent its time: the rewrite up to its
// last commit, the flush and the wait of SettleRetention, and whether the wait
// reached its deadline. All zero before the first Retain; flush and settle are
// zero when SettleRetention is off or the retention returned early, before
// rewriting anything.
func (i *Instrument) LastRetain() (work, flush, settle time.Duration, deadlineHit bool) {
	i.s.mu.Lock()
	defer i.s.mu.Unlock()
	return i.s.last.work, i.s.last.flush, i.s.last.settle, i.s.last.deadlineHit
}

// SizeByLayer is Pebble's estimate of the bytes of tables that hold each layer's
// keys (the key range of a layer is one contiguous span, since a layer leads every
// key). It counts tables, not the log or what is still in the memtable, so it is
// meant for a database at rest. A layer with none is left out.
func (i *Instrument) SizeByLayer() (map[catalog.Layer]int64, error) {
	out := map[catalog.Layer]int64{}
	for l := catalog.L0; l <= catalog.L3; l++ {
		lo, hi := prefixBounds([]byte{pebblekv.LayerByte(l)})
		n, err := i.s.kv.EstimateDiskUsage(lo, hi)
		if err != nil {
			return nil, fmt.Errorf("pebblestore: size of %s: %w", l, err)
		}
		if n > 0 {
			out[l] = int64(n)
		}
	}
	return out, nil
}

// Part names a share of the logical bytes of a layer's keys and values.
type Part struct {
	Layer catalog.Layer
	// Kind is "observe" (a record that asserts, not a run extension), "extension"
	// (a record that re-asserts a run with a later Through), "delete",
	// "payload forward", "payload reverse" or "payload entity" (the payload of the
	// records stored under that direction), "checkpoint" or "baseline".
	Kind string
}

// The kinds of Part.
const (
	partObserve        = "observe"
	partExtension      = "extension"
	partDelete         = "delete"
	partPayloadForward = "payload forward"
	partPayloadReverse = "payload reverse"
	partPayloadEntity  = "payload entity"
	partCheckpoint     = "checkpoint"
	partBaseline       = "baseline"
)

// recordParts splits the total logical bytes of one stored record, its key and
// its value, into the bytes of its kind and the bytes of its payload, and names
// the part each belongs to. dir is the direction byte of the prefix it is stored
// under.
func recordParts(v pebblekv.Value, dir byte, total int) (kind string, kindBytes int, payload string, payloadBytes int) {
	switch {
	case v.Kind == lifecycle.Delete:
		kind = partDelete
	case v.HasThrough:
		kind = partExtension
	default:
		kind = partObserve
	}
	switch dir {
	case 1:
		payload = partPayloadForward
	case 2:
		payload = partPayloadReverse
	default:
		payload = partPayloadEntity
	}
	return kind, total - len(v.Payload), payload, len(v.Payload)
}

// Breakdown scans every data key and divides the bytes of key and value into the
// parts: a record's bytes by its kind, its payload by the direction it is stored
// under, and a checkpoint or the baseline as a whole. The parts add up to the
// bytes of every data key and value.
func (i *Instrument) Breakdown() (map[Part]int64, error) {
	lo, hi := dataBounds()
	it, err := i.s.kv.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return nil, err
	}
	defer func() { _ = it.Close() }()
	out := map[Part]int64{}
	for ok := it.First(); ok; ok = it.Next() {
		prefix, _, _, kind, err := parseKey(it.Key())
		if err != nil {
			return nil, err
		}
		layer, ok := pebblekv.LayerFromByte(prefix[0])
		if !ok {
			return nil, fmt.Errorf("pebblestore: key %x has no layer", it.Key())
		}
		total := len(it.Key()) + len(it.Value())
		switch kind {
		case kindCheckpoint:
			out[Part{layer, partCheckpoint}] += int64(total)
		case kindBaseline:
			out[Part{layer, partBaseline}] += int64(total)
		default:
			dir := prefix[prefixLen-1]
			_, v, err := decodeRecordValue(dir, it.Value())
			if err != nil {
				return nil, err
			}
			k, kb, p, pb := recordParts(v, dir, total)
			out[Part{layer, k}] += int64(kb)
			out[Part{layer, p}] += int64(pb)
		}
	}
	return out, it.Error()
}
