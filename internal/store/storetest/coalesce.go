package storetest

import (
	"bytes"
	"time"

	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

// runKey identifies one producer's runs of one subject.
type runKey struct {
	subject  store.Subject
	producer lifecycle.Producer
}

// run is a producer's current run of refreshes of one subject.
type run struct {
	start, last time.Time // last is the latest refresh seen; it is the run's Through
	ttl         time.Duration
	payload     []byte
	boot        string
}

// coalesce is the run coalescer, applied as a record is returned (with
// Config.Runs), because it depends on the horizon the store has told the
// generator about. It draws nothing from the random source, so a stream with
// runs and the same stream without them are the same events.
//
// A refresh that repeats its producer's current run (the same payload, boot
// and TTL, after the run's last observation, and not after its deadline, the
// last observation plus the TTL) is returned as an extension: an Observe at the
// run's first event time, with Through set to the refresh and a Seq the caller
// assigns. Any other refresh starts a new run, and is returned as it is. A
// delete, or a watch-mode record, ends the run. A run that began before the
// horizon is not extended, because the store would refuse a record at its start;
// the refresh starts a new run at its own instant instead, and since the old
// run's deadline is not before that instant, existence stays continuous.
func (g *Generator) coalesce(it item) store.Record {
	rec := it.rec
	if !g.cfg.Runs {
		return rec
	}
	key := runKey{rec.Subject, rec.Producer}
	if !it.refresh {
		if rec.Kind == lifecycle.Delete || rec.TTL == 0 {
			delete(g.runs, key)
		}
		return rec
	}
	if st := g.runs[key]; st != nil && st.ttl == rec.TTL && bytes.Equal(st.payload, rec.Payload) && st.boot == rec.Boot &&
		rec.EventTime.After(st.last) && !rec.EventTime.After(st.last.Add(st.ttl)) &&
		!st.start.Before(g.horizon) {
		st.last = rec.EventTime
		rec.EventTime, rec.Through = st.start, st.last
		return rec
	}
	g.runs[key] = &run{start: rec.EventTime, last: rec.EventTime, ttl: rec.TTL, payload: rec.Payload, boot: rec.Boot}
	return rec
}
