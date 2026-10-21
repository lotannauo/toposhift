package duckmodel

import (
	"cmp"
	"slices"
	"sort"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// rec is a record as a record file holds it: the columns of the file, with its times as
// Unix nanoseconds. The subject and its layer are kept once, with the subject.
type rec struct {
	producer lifecycle.Producer
	event    int64
	seq      uint64
	through  int64 // meaningful only if hasThrough
	ttl      int64 // nanoseconds
	payload  []byte
	del      bool // the record is a delete, not an observation

	hasThrough bool
}

// keep converts a valid record. The payload is copied.
func keep(r engine.Record) rec {
	c := rec{
		producer: r.Producer, event: ToNanos(r.EventTime), seq: r.Seq, ttl: int64(r.TTL),
		payload: slices.Clone(r.Payload), del: r.Kind == lifecycle.Delete,
	}
	if !r.Through.IsZero() {
		c.through, c.hasThrough = ToNanos(r.Through), true
	}
	return c
}

// record is the engine record a row stands for, with the subject's layer. The payload
// is copied, so a caller cannot change what the model holds.
func (c rec) record(s engine.Subject, layer catalog.Layer) engine.Record {
	r := engine.Record{
		Layer: layer, Subject: s, Producer: c.producer, EventTime: FromNanos(c.event),
		Seq: c.seq, Kind: lifecycle.Observe, TTL: time.Duration(c.ttl), Payload: slices.Clone(c.payload),
	}
	if c.del {
		r.Kind = lifecycle.Delete
	}
	if c.hasThrough {
		r.Through = FromNanos(c.through)
	}
	return r
}

// cmpInFile orders the rows of one subject as a file sorted by producer, event time and
// Seq does.
func (c rec) cmpInFile(o rec) int {
	return cmp.Or(
		cmp.Compare(c.producer, o.producer),
		cmp.Compare(c.event, o.event),
		cmp.Compare(c.seq, o.seq),
	)
}

// last is the latest instant the record says the producer observed the subject: the
// through time of a run, else the event time. It is greatest(event_time_ns,
// coalesce(through_ns, event_time_ns)).
func (c rec) last() int64 {
	if c.hasThrough && c.through > c.event {
		return c.through
	}
	return c.event
}

// holdsAt reports whether a record that is a producer's newest as of t makes the subject
// hold at t: an observation that is not a delete, whose TTL is zero or whose deadline,
// the last observation plus the TTL, is after t.
func (c rec) holdsAt(t int64, mut mutant) bool {
	if c.del && mut != mutDeleteHolds {
		return false
	}
	if c.ttl == 0 {
		return mut != mutTTLZeroExpires
	}
	deadline := c.last() + c.ttl
	if mut == mutDeadlineFromEvent {
		deadline = c.event + c.ttl
	}
	return deadline > t
}

// holds applies the read rule to the records of one subject, ascending by [rec.cmpInFile]:
// some producer's newest record with event time at or before t and Seq at or below s
// makes the subject hold.
func holds(recs []rec, t int64, s uint64, mut mutant) bool {
	if mut == mutSeqIgnored {
		s = ^uint64(0)
	}
	for a := 0; a < len(recs); {
		b := a + 1
		for b < len(recs) && recs[b].producer == recs[a].producer {
			b++
		}
		if newest, ok := newestOf(recs[a:b], t, s, mut); ok && newest.holdsAt(t, mut) {
			return true
		}
		a = b
	}
	return false
}

// newestOf is the newest record of one producer's records, ascending by event time and
// Seq, with event time at or before t and Seq at or below s. The records at or before t
// are a prefix, and the newest of those the token lets through is the last of them.
func newestOf(group []rec, t int64, s uint64, mut mutant) (rec, bool) {
	j := sort.Search(len(group), func(i int) bool { return group[i].event > t })
	for ; j > 0; j-- {
		if group[j-1].seq > s {
			continue
		}
		i := j - 1
		if mut == mutTieByEventOnly {
			for i > 0 && group[i-1].event == group[i].event && group[i-1].seq <= s {
				i--
			}
		}
		return group[i], true
	}
	return rec{}, false
}
