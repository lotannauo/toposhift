package conformance

import (
	"errors"
	"fmt"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
	"github.com/lotannauo/toposhift/bench/spike/workload"
	"github.com/lotannauo/toposhift/internal/catalog"
)

// ErrNotMeasurable is returned by CheckMeasurement for an engine that does not
// implement the hooks a measurement reads its sizes through.
var ErrNotMeasurable = errors.New("the candidate does not implement engine.Breakdowner and engine.LayerSizer")

// CheckMeasurement checks that what an engine says about the bytes it holds is
// true, because a bytes-per-event figure is only as good as these. It writes a
// workload whose payloads are known and compares:
//
//   - the payload parts of Breakdown with the payload bytes in the stream, which
//     are known exactly: an edge's payload is stored once from each end, an
//     entity's once, and a deletion has none, so each layer's payload parts are
//     the sums of the stream's, direction by direction;
//   - the kinds of record: the stream has assertions and deletions, and has
//     extension bytes if and only if it has run extensions (CheckMeasurementPlain
//     is the stream without);
//   - that every part is a part the engine is documented to have, and positive;
//   - SizeByLayer with the layers written to (each has tables, none is invented)
//     and with Size (the layers add up to no more than the whole).
//
// Use a fresh engine. An engine that is a Settler is settled first, so what it
// reports is in tables.
func CheckMeasurement(cand engine.Engine) error { return checkMeasurement(cand, true) }

// CheckMeasurementPlain is CheckMeasurement for a stream whose refreshes are not
// coalesced, which has no run extension: the engine must report no extension
// bytes at all, which is what shows the extension part is not a guess. Use a
// fresh engine.
func CheckMeasurementPlain(cand engine.Engine) error { return checkMeasurement(cand, false) }

func checkMeasurement(cand engine.Engine, coalesce bool) error {
	bd, okB := cand.(engine.Breakdowner)
	ls, okL := cand.(engine.LayerSizer)
	if !okB || !okL {
		return ErrNotMeasurable
	}
	cfg := workload.Tiny()
	cfg.Duration = 15 * time.Minute
	cfg.EventsPerSecond = 1
	cfg.CoalesceRuns = coalesce
	cfg.ConfirmProbability, cfg.ConfirmTTL = 0.3, 3*time.Minute
	cfg.PayloadMin, cfg.PayloadMax = 16, 96
	g, err := workload.New(cfg)
	if err != nil {
		return err
	}
	recs := g.All()
	type payloads struct{ forward, reverse, entity int64 }
	want := map[catalog.Layer]*payloads{}
	extensions := 0
	for _, r := range recs {
		if r.Through.IsZero() {
			// a plain assertion or a deletion
		} else {
			extensions++
		}
		p := want[r.Layer]
		if p == nil {
			p = &payloads{}
			want[r.Layer] = p
		}
		n := int64(len(r.Payload))
		if r.Subject.Kind == engine.SubjectEdge {
			p.forward += n
			p.reverse += n
		} else {
			p.entity += n
		}
	}
	if (extensions > 0) != coalesce {
		return fmt.Errorf("conformance: the measurement workload has %d run extensions with coalescing %v", extensions, coalesce)
	}
	for i := 0; i < len(recs); i += 64 {
		if err := cand.Write(cloneRecords(recs[i:min(i+64, len(recs))])); err != nil {
			return fmt.Errorf("writing: %w", err)
		}
	}
	if s, ok := cand.(engine.Settler); ok {
		if err := s.Settle(); err != nil {
			return err
		}
	}

	parts, err := bd.Breakdown()
	if err != nil {
		return fmt.Errorf("breakdown: %w", err)
	}
	known := map[string]bool{
		pebblekv.PartObserve: true, pebblekv.PartExtension: true, pebblekv.PartDelete: true,
		pebblekv.PartPayloadForward: true, pebblekv.PartPayloadReverse: true, pebblekv.PartPayloadEntity: true,
		pebblekv.PartCheckpoint: true, pebblekv.PartBaseline: true,
	}
	byKind := map[string]int64{}
	for part, n := range parts {
		if !known[part.Kind] {
			return fmt.Errorf("the breakdown has a part %q in %s that the engine does not document", part.Kind, part.Layer)
		}
		if n <= 0 {
			return fmt.Errorf("the breakdown has %d bytes in %s of %s", n, part.Kind, part.Layer)
		}
		byKind[part.Kind] += n
	}
	for layer, w := range want {
		for kind, bytes := range map[string]int64{
			pebblekv.PartPayloadForward: w.forward, pebblekv.PartPayloadReverse: w.reverse, pebblekv.PartPayloadEntity: w.entity,
		} {
			if got := parts[engine.Part{Layer: layer, Kind: kind}]; got != bytes {
				return fmt.Errorf("the breakdown says %d bytes of %s in %s; the stream's payloads add up to %d", got, kind, layer, bytes)
			}
		}
	}
	for _, kind := range []string{pebblekv.PartObserve, pebblekv.PartDelete} {
		if byKind[kind] == 0 {
			return fmt.Errorf("the breakdown has no %s bytes for a stream with assertions and deletions", kind)
		}
	}
	if coalesce && byKind[pebblekv.PartExtension] == 0 {
		return fmt.Errorf("the breakdown has no extension bytes for a stream with %d run extensions", extensions)
	}
	if !coalesce && byKind[pebblekv.PartExtension] != 0 {
		return fmt.Errorf("the breakdown has %d extension bytes for a stream with no run extension", byKind[pebblekv.PartExtension])
	}

	sizes, err := ls.SizeByLayer()
	if err != nil {
		return fmt.Errorf("layer sizes: %w", err)
	}
	var sum int64
	for layer, n := range sizes {
		if want[layer] == nil {
			return fmt.Errorf("the layer sizes reports %d bytes in %s, which nothing was written to", n, layer)
		}
		sum += n
	}
	for layer := range want {
		if sizes[layer] <= 0 {
			return fmt.Errorf("the layer sizes reports nothing in %s, which was written to", layer)
		}
	}
	total, err := cand.Size()
	if err != nil {
		return err
	}
	if sum > total {
		return fmt.Errorf("the layers add up to %d bytes, more than the engine's %d", sum, total)
	}
	return nil
}
