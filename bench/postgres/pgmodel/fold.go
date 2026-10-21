package pgmodel

import (
	"fmt"
	"slices"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// rec is a record as the log holds it: the columns of the log table, with its times
// as Unix nanoseconds. The subject and its layer are kept once, with the subject.
type rec struct {
	producer lifecycle.Producer
	seq      uint64
	event    int64
	through  int64 // meaningful only if hasThrough
	ttl      time.Duration
	payload  []byte
	kind     lifecycle.Kind

	hasThrough bool
}

// keep converts a valid record. The payload is copied.
func keep(r engine.Record) rec {
	c := rec{
		producer: r.Producer, seq: r.Seq, event: ToNanos(r.EventTime), ttl: r.TTL,
		payload: slices.Clone(r.Payload), kind: r.Kind,
	}
	if !r.Through.IsZero() {
		c.through, c.hasThrough = ToNanos(r.Through), true
	}
	return c
}

// record is the engine record a log row stands for, with the subject's layer. The
// payload is copied, so a caller cannot change the log.
func (c rec) record(s engine.Subject, layer catalog.Layer) engine.Record {
	r := engine.Record{
		Layer: layer, Subject: s, Producer: c.producer, EventTime: FromNanos(c.event),
		Seq: c.seq, Kind: c.kind, TTL: c.ttl, Payload: slices.Clone(c.payload),
	}
	if c.hasThrough {
		r.Through = FromNanos(c.through)
	}
	return r
}

// cmp orders log rows by (event time, seq), the order of the log's index.
func (c rec) cmp(o rec) int {
	switch {
	case c.event < o.event:
		return -1
	case c.event > o.event:
		return 1
	case c.seq < o.seq:
		return -1
	case c.seq > o.seq:
		return 1
	}
	return 0
}

// assertion is the lifecycle assertion of the row. The attributes are left out:
// existence does not depend on them under the zero policy, and the fold would only
// copy them.
func (c rec) assertion() lifecycle.Assertion {
	a := lifecycle.Assertion{
		Producer: c.producer, EventTime: FromNanos(c.event), Seq: c.seq,
		Kind: c.kind, TTL: c.ttl,
	}
	if c.hasThrough {
		a.Through = FromNanos(c.through)
	}
	return a
}

// foldRows folds records of one subject with the lifecycle specification and returns
// the maximal intervals of existence as rows, in ascending order.
func foldRows(recs []rec) ([]Row, error) {
	tl, err := foldTimeline(recs)
	if err != nil {
		return nil, err
	}
	return rowsOf(tl), nil
}

func foldTimeline(recs []rec) (lifecycle.Timeline, error) {
	if len(recs) == 0 {
		return lifecycle.Timeline{}, nil
	}
	as := make([]lifecycle.Assertion, len(recs))
	for i, c := range recs {
		as[i] = c.assertion()
	}
	tl, err := lifecycle.Fold(as, lifecycle.Policy{})
	if err != nil {
		return lifecycle.Timeline{}, fmt.Errorf("pgmodel: %w", err)
	}
	return tl, nil
}

func rowsOf(tl lifecycle.Timeline) []Row {
	ivs := tl.Existence()
	if len(ivs) == 0 {
		return nil
	}
	rows := make([]Row, len(ivs))
	for i, iv := range ivs {
		if iv.Open() {
			rows[i] = OpenRow(ToNanos(iv.Start))
		} else {
			rows[i] = Final(ToNanos(iv.Start), ToNanos(iv.End), iv.EndSource)
		}
	}
	return rows
}
