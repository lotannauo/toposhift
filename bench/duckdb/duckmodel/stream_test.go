package duckmodel

import (
	"slices"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/workload"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
)

// streamed is a workload as it was written to some engines.
type streamed struct {
	// written are the records the engines accepted, in write order.
	written []engine.Record
	// horizon is the last horizon the stream moved to, zero if none, and floor the
	// highest Seq at the last retention.
	horizon  time.Time
	floor    uint64
	entities []identity.Fingerprint
}

// stream writes a workload to every engine in batches of batch records, moving the
// horizon at the given fractions of the period and dropping the records that arrive
// before it, as a store refuses them and the runner's Drive does not offer them. The
// retained engines are retained at each horizon; the plain ones only follow the writes,
// so they hold what the stream would have been without a retention.
func stream(t testingT, cfg workload.Config, batch int, retainAt []float64, retained []engine.Engine, plain ...engine.Engine) streamed {
	t.Helper()
	g, err := workload.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var s streamed
	next := 0
	span := g.End().Sub(g.Start())
	for {
		recs := g.Batch(batch)
		if len(recs) == 0 {
			break
		}
		kept := recs[:0]
		for _, r := range recs {
			if !r.EventTime.Before(s.horizon) {
				kept = append(kept, r)
			}
		}
		if len(kept) == 0 {
			continue
		}
		for _, e := range slices.Concat(retained, plain) {
			if err := e.Write(cloneRecords(kept)); err != nil {
				t.Fatal(err)
			}
		}
		s.written = append(s.written, kept...)
		if next < len(retainAt) && !kept[len(kept)-1].EventTime.Before(g.Start().Add(time.Duration(retainAt[next]*float64(span)))) {
			s.horizon = g.Start().Add(time.Duration(retainAt[next] * float64(span)))
			next++
			s.floor = retained[0].LastSeq()
			for _, e := range retained {
				if err := e.Retain(s.horizon); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	s.entities = g.Entities()
	return s
}

func cloneRecords(rs []engine.Record) []engine.Record {
	out := make([]engine.Record, len(rs))
	for i, r := range rs {
		out[i] = r
		out[i].Payload = append([]byte(nil), r.Payload...)
	}
	return out
}

var layers = []catalog.Layer{catalog.L0, catalog.L1, catalog.L2, catalog.L3}
