package main

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/coalesce"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

// bufferKeep is the most records the coalescer keeps its flush buffer for.
const bufferKeep = 1 << 15

// shardedCoalescer is the product's ingest coalescer split into parts by the
// start of the subject, for a stream whose event times and TTLs are whole minutes.
//
// A run is a producer's refreshes of one subject, and a refresh meets only the run
// of its own key, so each part answers an Add exactly as the whole would. The
// parts differ in one thing: when the coalescer is flushed, it builds a list of
// every run that is due and sorts it, and for a stream whose subjects are all due
// at once that list is as large as the stream. Here a flush is made one part at a
// time and one deadline at a time, so no list is larger than a part's share of a
// deadline.
//
// The records of a flush are in the order of [coalesce.Coalescer.Flush]: by the
// deadline each run had, then by subject, then by producer. A run's deadline is
// the event time of its latest extension plus its TTL, so it falls on the minute
// grid of the stream, and the flush takes one grid point at a time, in order. The
// parts are cut along the order of the subjects (kind, entity type, and the first
// bits of the hash), and flushed in that order, so the records of the parts of one
// deadline follow each other in the whole's order and need no merging.
type shardedCoalescer struct {
	cfg   coalesce.Config
	parts map[partKey]*coalesce.Coalescer
	order []partKey // the keys of parts, in the order of the subjects they hold

	started bool
	origin  time.Time // the event time of the first record: a point of the grid
	done    time.Time // every deadline up to this grid point has been flushed
	buf     []store.Record
}

// partKey is the start of a subject's sort key: its kind, the type of its first
// entity and the high bits of that entity's hash. Subjects with different part
// keys are ordered as their part keys are.
type partKey struct {
	kind store.SubjectKind
	typ  catalog.EntityType
	bits uint8
}

func (k partKey) compare(o partKey) int {
	return cmp.Or(cmp.Compare(k.kind, o.kind), cmp.Compare(k.typ, o.typ), cmp.Compare(k.bits, o.bits))
}

func partOf(s store.Subject) partKey {
	h := s.A.Hash()
	return partKey{s.Kind, s.A.Type(), h[0] >> 4}
}

func newShardedCoalescer(cfg coalesce.Config) (*shardedCoalescer, error) {
	if _, err := coalesce.New(cfg); err != nil {
		return nil, err
	}
	return &shardedCoalescer{cfg: cfg, parts: make(map[partKey]*coalesce.Coalescer)}, nil
}

// part returns the coalescer for a key, making it if it is new.
func (c *shardedCoalescer) part(k partKey) *coalesce.Coalescer {
	if p, ok := c.parts[k]; ok {
		return p
	}
	p, err := coalesce.New(c.cfg) // the configuration was checked when c was made
	if err != nil {
		panic(err)
	}
	c.parts[k] = p
	i, _ := slices.BinarySearchFunc(c.order, k, partKey.compare)
	c.order = slices.Insert(c.order, i, k)
	return p
}

// add is [coalesce.Coalescer.Add] for the part of the record's key. A record the
// coalescer would keep a run for must be on the grid, because the order of a flush
// rests on it: its event time a whole number of minutes after the first record's,
// and its TTL a whole number of minutes.
func (c *shardedCoalescer) add(r store.Record, out []store.Record) ([]store.Record, error) {
	if !c.started {
		c.started = true
		c.origin, c.done = r.EventTime, r.EventTime
	}
	if r.EventTime.Before(c.origin) {
		return out, fmt.Errorf("a record at %s is before the stream's first, at %s", r.EventTime.Format(time.RFC3339), c.origin.Format(time.RFC3339))
	}
	if keepsRun(r) {
		if r.TTL <= 0 || r.TTL%sampleInterval != 0 || r.EventTime.Sub(c.origin)%sampleInterval != 0 {
			return out, fmt.Errorf("a record at %s with TTL %s is off the minute grid of the stream", r.EventTime.Format(time.RFC3339), r.TTL)
		}
		// A refresh that a run absorbs comes less than the extension interval after the
		// run's last extension, so the run's deadline is more than the TTL minus that
		// interval after the refresh. The flush at a refresh's time has gone as far as a
		// flush lead and a second past it; a deadline that could be behind that would
		// be missed. The interval itself must also be longer than that, or a run could
		// absorb a refresh the flush has already passed.
		if every := c.cfg.ExtensionInterval(r.TTL); every > 0 && (every <= coalesce.FlushLead+time.Second || r.TTL-every <= coalesce.FlushLead+time.Second) {
			return out, fmt.Errorf("a record at %s with TTL %s is refreshed too closely for the flush clock", r.EventTime.Format(time.RFC3339), r.TTL)
		}
	}
	return c.part(partOf(r.Subject)).Add(r, out), nil
}

// keepsRun is whether the coalescer keeps a run for a record. A delete, a record
// without a TTL and a record with its own Through are passed on as given.
func keepsRun(r store.Record) bool {
	return r.Kind != lifecycle.Delete && r.TTL != 0 && r.Through.IsZero()
}

// flush gives write the records [coalesce.Coalescer.Flush] would return for now,
// in its order, a part of them at a time. write must not keep the slice.
func (c *shardedCoalescer) flush(now time.Time, write func([]store.Record) error) error {
	return c.flushThrough(now.Add(coalesce.FlushLead), write)
}

// flushThrough gives write the records of every run whose deadline is no later
// than limit, one grid point at a time.
func (c *shardedCoalescer) flushThrough(limit time.Time, write func([]store.Record) error) error {
	if !c.started {
		return nil
	}
	for g := c.done.Add(sampleInterval); !g.After(limit); g = g.Add(sampleInterval) {
		for _, k := range c.order {
			c.buf = c.parts[k].Flush(g.Add(-coalesce.FlushLead), c.buf[:0])
			if err := write(c.buf); err != nil {
				return err
			}
			if cap(c.buf) > bufferKeep {
				c.buf = nil
			}
		}
		c.done = g
	}
	return nil
}

// flushAll gives write the records that write every refresh the parts still hold
// absorbed, in the order [coalesce.Coalescer.FlushAll] writes them. A deadline is
// at most a TTL past the latest record, so once the flush has passed that, a part
// holds nothing absorbed: if one does, a record was off the grid, and that is an
// error and not a record in the wrong place.
func (c *shardedCoalescer) flushAll(latest time.Time, maxTTL time.Duration, write func([]store.Record) error) error {
	if err := c.flushThrough(latest.Add(maxTTL), write); err != nil {
		return err
	}
	for _, k := range c.order {
		if len(c.parts[k].FlushAll(nil)) > 0 {
			return errors.New("a coalescer holds refreshes past the last deadline of the stream")
		}
	}
	return nil
}
