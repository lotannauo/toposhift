package xtmodel

import (
	"fmt"
	"math/rand/v2"
	"time"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// drawer is where a test stream's choices come from: rapid, which shrinks a failing
// stream, or a generator with a fixed seed, which is the same stream on every run.
type drawer interface {
	// intRange draws an integer in [lo, hi].
	intRange(label string, lo, hi int) int
}

type rapidDrawer struct{ t *rapid.T }

func (d rapidDrawer) intRange(label string, lo, hi int) int {
	return rapid.IntRange(lo, hi).Draw(d.t, label)
}

type seeded struct{ r *rand.Rand }

func seedDrawer(seed uint64) seeded {
	return seeded{rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))}
}

func (d seeded) intRange(_ string, lo, hi int) int { return lo + d.r.IntN(hi-lo+1) }

// genRecord draws a record about one of the test subjects. Event times fall in a
// minute starting one second past the epoch, so that many records share instants
// and many segments touch; TTLs are zero (watch mode) or a few seconds; a delete
// carries none; one record in four is a run, with a through time.
func genRecord(d drawer, label string) engine.Record {
	r := engine.Record{
		Layer: catalog.L2, Subject: subjects[d.intRange(label+" subject", 0, len(subjects)-1)],
		Producer:  producers[d.intRange(label+" producer", 0, len(producers)-1)],
		EventTime: at(int64(1 + d.intRange(label+" at", 0, 60))),
	}
	if d.intRange(label+" delete", 0, 3) == 0 {
		r.Kind = lifecycle.Delete
		return r
	}
	r.Kind = lifecycle.Observe
	r.Payload = []byte{byte(d.intRange(label+" payload", 0, 3))}
	if d.intRange(label+" has ttl", 0, 1) == 1 {
		r.TTL = time.Duration(d.intRange(label+" ttl", 1, 8)) * time.Second
	}
	if d.intRange(label+" run", 0, 3) == 0 {
		r.Through = r.EventTime.Add(time.Duration(d.intRange(label+" through", 0, 5)) * time.Second)
	}
	return r
}

// genStream draws up to maxSteps steps: batches of one to six records, with late
// events, shared instants, overwritten records, several producers, runs and deletes,
// and, if withRetain, a retention horizon moved now and then, after which no record
// before it is offered.
func genStream(d drawer, maxSteps int, withRetain bool) []step {
	var out []step
	var horizon time.Time
	seq := uint64(d.intRange("first seq", 1, 5))
	for i := range d.intRange("steps", 1, maxSteps) {
		var batch []engine.Record
		for j := range d.intRange("batch size", 1, 6) {
			r := genRecord(d, fmt.Sprintf("record %d.%d", i, j))
			if r.EventTime.Before(horizon) {
				continue
			}
			r.Seq = seq
			seq += uint64(d.intRange("seq gap", 1, 3))
			batch = append(batch, r)
		}
		st := step{batch: batch}
		if withRetain && d.intRange("retain", 0, 4) == 0 {
			st.retain = at(int64(1 + d.intRange("horizon", 0, 64)))
			if st.retain.After(horizon) {
				horizon = st.retain
			}
		}
		out = append(out, st)
	}
	return out
}

// run writes a stream to a model, comparing it to the reference at every step, and
// returns the first difference.
func run(m *Model, steps []step) error {
	c := newChecker(m)
	for i, s := range steps {
		if err := c.do(s); err != nil {
			return fmt.Errorf("step %d: %w", i, err)
		}
	}
	return nil
}
