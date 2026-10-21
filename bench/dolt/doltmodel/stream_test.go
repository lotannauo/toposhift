package doltmodel_test

import (
	"fmt"
	"slices"
	"time"

	"github.com/lotannauo/toposhift/bench/dolt/doltmodel"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/workload"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
)

// written is what a stream has put in a model, for the checks that ask the model about it.
type written struct {
	records []engine.Record
	// floor is the highest sequence number committed when the horizon last moved: a token
	// below it, and an instant before the horizon, are not specified.
	floor   uint64
	horizon time.Time
}

// stream writes a workload to a model in batches of the given size, moving the horizon at
// the given fractions of the period (a record before the horizon is dropped, as the
// runner's Drive drops it), and calls check with what has been written before each
// retention and at the end. It stops at the first error check returns.
func stream(m *doltmodel.Model, cfg workload.Config, batch int, retainAt []float64, check func(written) error) error {
	g, err := workload.New(cfg)
	if err != nil {
		return err
	}
	var w written
	next := 0
	span := g.End().Sub(g.Start())
	for {
		recs := g.Batch(batch)
		if len(recs) == 0 {
			break
		}
		kept := recs[:0]
		for _, r := range recs {
			if !r.EventTime.Before(w.horizon) {
				kept = append(kept, r)
			}
		}
		if len(kept) == 0 {
			continue
		}
		if err := m.Write(slices.Clone(kept)); err != nil {
			return err
		}
		w.records = append(w.records, kept...)
		if next < len(retainAt) && !kept[len(kept)-1].EventTime.Before(g.Start().Add(time.Duration(retainAt[next]*float64(span)))) {
			if err := check(w); err != nil {
				return fmt.Errorf("before the retention at %v: %w", retainAt[next], err)
			}
			w.horizon = g.Start().Add(time.Duration(retainAt[next] * float64(span)))
			next++
			if err := m.Retain(w.horizon); err != nil {
				return err
			}
			g.SetHorizon(w.horizon)
			w.floor = m.LastSeq()
		}
	}
	if err := check(w); err != nil {
		return fmt.Errorf("at the end: %w", err)
	}
	return nil
}

// entityLayers collects the layers each entity is an endpoint or the subject of.
func entityLayers(records []engine.Record) map[identity.Fingerprint][]catalog.Layer {
	out := make(map[identity.Fingerprint][]catalog.Layer)
	add := func(fp identity.Fingerprint, l catalog.Layer) {
		if fp.IsZero() || slices.Contains(out[fp], l) {
			return
		}
		out[fp] = append(out[fp], l)
	}
	for _, r := range records {
		add(r.Subject.A, r.Layer)
		add(r.Subject.B, r.Layer)
	}
	return out
}

func sortedEntities(layers map[identity.Fingerprint][]catalog.Layer) []identity.Fingerprint {
	fps := make([]identity.Fingerprint, 0, len(layers))
	for fp := range layers {
		fps = append(fps, fp)
	}
	doltmodel.SortFingerprints(fps)
	return fps
}
