package workload

import (
	"bytes"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// The ingest-side run coalescer.

type runKey struct {
	subject  engine.Subject
	producer lifecycle.Producer
}

// run is a producer's current run of refreshes of one subject.
type run struct {
	// prev is the run this one replaced, and no further back: a refresh that
	// arrives late, after this run started, may belong to it.
	prev        *run
	layer       catalog.Layer
	start, last time.Time // last is the latest refresh seen, whether or not it was recorded
	recorded    time.Time // the Through of the latest extension written
	ttl         time.Duration
	payload     []byte
}

// deadline is when the store's copy of the run lapses: the last extension written
// plus the TTL. It can be earlier than the producer's own (last plus TTL), because
// refreshes inside the extension interval are absorbed without a record.
func (st *run) deadline() time.Time { return st.recorded.Add(st.ttl) }

func newRun(r engine.Record, replaced *run) *run {
	if replaced != nil {
		replaced.prev = nil // the chain is one run long
	}
	return &run{prev: replaced, layer: r.Layer, start: r.EventTime, last: r.EventTime, recorded: r.EventTime, ttl: r.TTL, payload: r.Payload}
}

// covers is whether the run, whose refreshes the store already has through its
// stored deadline, stands for the refresh: the same description, at an instant
// between its first and last.
func (st *run) covers(r engine.Record) bool {
	return st != nil && st.ttl == r.TTL && bytes.Equal(st.payload, r.Payload) &&
		!r.EventTime.Before(st.start) && !r.EventTime.After(st.last)
}

// coalesce is the ingest-side coalescer. It turns a refresh that continues a
// producer's run into an extension of that run, drops a refresh the run already
// covers, and passes everything else through unchanged. It appends what the
// store is to be given for the refresh to out: nothing, the refresh, or, where a
// run is replaced by its successor, the record that closes the old one and then
// the refresh.
//
// An extension re-asserts the run at its first event time, so once the store has
// retained past that time it would be refused and the run would silently die at
// its deadline. A run that started before the retention horizon is therefore
// continued by a new run, asserted at the refresh's own event time. A new run also
// starts when the description changes.
//
// A new run replaces the old run's deadline, so existence stays continuous only
// if the old run's stored deadline reaches the new run's start. Refreshes inside
// the extension interval are absorbed, so it may not: the producer was seen at
// the last of them and its promise runs to then plus the TTL. [closeRun] writes
// what the store is missing in that case. A run that ends in a real silence (the
// next refresh comes after the producer's own deadline) is not closed: it ends at
// its stored deadline, early by less than the extension interval, which is what
// bounding the extension trades away.
func (g *Generator) coalesce(r engine.Record, out []engine.Record) []engine.Record {
	key := runKey{r.Subject, r.Producer}
	if r.Kind == lifecycle.Delete || r.TTL == 0 {
		delete(g.runs, key) // a delete, or a watch-mode assertion, ends any run
		return append(out, r)
	}
	st := g.runs[key]
	switch {
	case st == nil:
		g.runs[key] = newRun(r, nil)
		return append(out, r)
	case r.EventTime.Before(st.start):
		// Older than the run, and not part of it, whatever it describes: a late refresh
		// must not start a run in the past and replace the current one. If the run before it covered that
		// instant, it already stands for the refresh: passing it through would be an
		// assertion that replaces that run's deadline from its instant on, and could
		// end its existence before the new run begins. Otherwise leave it as it is.
		if st.prev.covers(r) {
			return out
		}
		return append(out, r)
	case st.ttl != r.TTL || !bytes.Equal(st.payload, r.Payload):
		// The description changed: this starts a run.
		out = g.closeRun(key, st, r.EventTime, out)
		g.runs[key] = newRun(r, st)
		return append(out, r)
	case !r.EventTime.After(st.last):
		return out // already covered by the run
	case st.start.Before(g.horizon) || r.EventTime.After(st.last.Add(st.ttl)) ||
		(g.cfg.RunMaxAge > 0 && r.EventTime.Sub(st.start) >= g.cfg.RunMaxAge):
		// A run the store can no longer extend, a gap longer than the TTL, or a run
		// that has reached its greatest age: a new run.
		out = g.closeRun(key, st, r.EventTime, out)
		g.runs[key] = newRun(r, st)
		return append(out, r)
	}
	st.last = r.EventTime
	if every := g.extendEvery(st.ttl); every > 0 && st.last.Sub(st.recorded) < every {
		return out // absorbed: the run is not re-asserted yet
	}
	st.recorded = st.last
	r.EventTime, r.Through = st.start, st.recorded
	return append(out, r)
}

// closeRun appends the record that makes the stored deadline of st reach until,
// the event time of the run that replaces it, when the producer's refreshes
// reach until and the stored deadline does not. It appends nothing when the store
// already covers until, when the producer itself was silent past its deadline
// (there is a real gap, and the new run follows it), or when until is before the
// horizon (the store would refuse the new run's start, and nothing before the
// horizon is kept).
//
// A run the store can still extend is extended: the same record the coalescer
// would have written at the refresh it absorbed last. A run that began before the
// horizon is continued by one asserted at that last refresh, or at the horizon
// itself if the refresh was before it: the producer's promise from there is
// replaced at until by the new run's, so only the existence from there to until is
// what it adds. The record starts no later than the stored deadline, so the two
// runs touch, except when the deadline lapsed before the horizon: the gap before
// the horizon stays, and the store keeps nothing there.
func (g *Generator) closeRun(key runKey, st *run, until time.Time, out []engine.Record) []engine.Record {
	if !st.deadline().Before(until) || until.After(st.last.Add(st.ttl)) || until.Before(g.horizon) {
		return out
	}
	rec := engine.Record{
		Layer: st.layer, Subject: key.subject, Producer: key.producer,
		Kind: lifecycle.Observe, TTL: st.ttl, Payload: st.payload,
	}
	if !st.start.Before(g.horizon) {
		rec.EventTime, rec.Through = st.start, st.last
		return append(out, rec)
	}
	at := st.last
	if d := st.deadline(); d.Before(at) {
		at = d // the store's copy lapsed before the last refresh: begin at the lapse
	}
	if at.Before(g.horizon) {
		at = g.horizon
	}
	rec.EventTime = at
	return append(out, rec)
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
