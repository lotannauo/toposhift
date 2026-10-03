package workload

import (
	"bytes"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// The ingest-side run coalescer.

type runKey struct {
	subject  engine.Subject
	producer lifecycle.Producer
}

// run is a producer's current run of refreshes of one subject.
type run struct {
	start, last time.Time // last is the latest refresh seen, whether or not it was recorded
	recorded    time.Time // the Through of the latest extension written
	ttl         time.Duration
	payload     []byte
}

// coalesce is the ingest-side coalescer. It turns a refresh that continues a
// producer's run into an extension of that run, drops a refresh the run already
// covers, and passes everything else through unchanged.
//
// An extension re-asserts the run at its first event time, so once the store has
// retained past that time it would be refused and the run would silently die at
// its deadline. A run that started before the retention horizon is therefore
// continued by a new run, asserted at the refresh's own event time: the old run
// ends at the new one's first assertion, so existence is continuous.
func (g *Generator) coalesce(r engine.Record) (engine.Record, bool) {
	key := runKey{r.Subject, r.Producer}
	if r.Kind == lifecycle.Delete || r.TTL == 0 {
		delete(g.runs, key) // a delete, or a watch-mode assertion, ends any run
		return r, true
	}
	st := g.runs[key]
	switch {
	case st == nil || st.ttl != r.TTL || !bytes.Equal(st.payload, r.Payload):
		// No run, or the description changed: this starts one.
		g.runs[key] = &run{start: r.EventTime, last: r.EventTime, recorded: r.EventTime, ttl: r.TTL, payload: r.Payload}
		return r, true
	case r.EventTime.Before(st.start):
		// Older than the run, and not part of it: leave it as it is.
		return r, true
	case !r.EventTime.After(st.last):
		return engine.Record{}, false // already covered by the run
	case st.start.Before(g.horizon) || r.EventTime.After(st.last.Add(st.ttl)):
		// A run the store can no longer extend, or a gap longer than the TTL: a
		// new run.
		g.runs[key] = &run{start: r.EventTime, last: r.EventTime, recorded: r.EventTime, ttl: r.TTL, payload: r.Payload}
		return r, true
	}
	st.last = r.EventTime
	if every := g.extendEvery(st.ttl); every > 0 && st.last.Sub(st.recorded) < every {
		return engine.Record{}, false // absorbed: the run is not re-asserted yet
	}
	st.recorded = st.last
	r.EventTime, r.Through = st.start, st.recorded
	return r, true
}

// pruneEvery is how many records pass between sweeps of runs that have ended.
const pruneEvery = 1 << 14

// pruneRuns forgets the runs whose deadline has passed the time of the next event
// (FreshIdentities only). With fresh identities there is a new subject for every
// pod created, so a map that never forgets one grows with the length of the
// simulation. The next refresh of a forgotten run would have started a new run
// anyway, because it came after the deadline; only a record that arrives late
// enough to have continued the run is passed through instead, and passing a
// record through is always valid.
func (g *Generator) pruneRuns() {
	now := g.frontier()
	for k, st := range g.runs {
		if st.last.Add(st.ttl).Before(now) {
			delete(g.runs, k)
		}
	}
}

// extendEvery is how close to its last extension a run with this TTL may be
// refreshed without a new record.
func (g *Generator) extendEvery(ttl time.Duration) time.Duration {
	if g.cfg.ExtendTTLFraction > 0 {
		return time.Duration(g.cfg.ExtendTTLFraction * float64(ttl))
	}
	return g.cfg.ExtendEvery
}
