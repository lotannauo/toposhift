package main

import (
	"strconv"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/store"
)

// recency counts how far behind a sample the stored end of its subject's run is.
//
// The coalescer absorbs a refresh instead of storing it, so until the run is extended
// (or Flush writes what was absorbed) the stored run ends at the last record of the
// subject, up to the extension interval before the producer's latest sample. That gap
// is what a read sees as the run's staleness, and what a crash loses. It only grows
// when a sample is absorbed and only shrinks when a record is written, so its largest
// value is the one just after a sample, which is where it is counted.
type recency struct {
	step  time.Duration // one sampling interval of the stream
	cover map[recencyKey]time.Time
	hist  map[int64]int64
	max   int64
}

type recencyKey struct {
	subject store.Subject
	layer   catalog.Layer
}

func newRecency(stepMinutes int32) *recency {
	return &recency{
		step:  time.Duration(stepMinutes) * sampleInterval,
		cover: map[recencyKey]time.Time{},
		hist:  map[int64]int64{},
	}
}

// stored notes a record the stream holds: the run it asserts is stored through its
// Through, or at its event time if it has none.
func (c *recency) stored(r store.Record) {
	end := r.EventTime
	if r.Through.After(end) {
		end = r.Through
	}
	k := recencyKey{r.Subject, r.Layer}
	if end.After(c.cover[k]) {
		c.cover[k] = end
	}
}

// sample counts the sample r, after the coalescer has handled it.
func (c *recency) sample(r store.Record) {
	lag := int64(r.EventTime.Sub(c.cover[recencyKey{r.Subject, r.Layer}]) / c.step)
	c.hist[lag]++
	c.max = max(c.max, lag)
}

func (c *recency) result() map[string]int64 {
	out := make(map[string]int64, len(c.hist)+1)
	for lag, n := range c.hist {
		out[strconv.FormatInt(lag, 10)] = n
	}
	out["max"] = c.max
	return out
}
