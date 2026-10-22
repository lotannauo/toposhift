package pgmodel

import (
	"slices"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// LogRow is the part of a log row that decides existence: the columns of the log
// table without the subject, the layer and the payload. A SQL engine reads these
// back from the database to re-fold a subject, and the planner folds them with the
// lifecycle specification. Times are Unix nanoseconds.
type LogRow struct {
	Producer lifecycle.Producer
	Seq      uint64
	Event    int64
	// Through is meaningful only if HasThrough.
	Through    int64
	HasThrough bool
	TTL        time.Duration
	Kind       lifecycle.Kind
}

// LogRowOf converts a valid record to the log row it is stored as.
func LogRowOf(r engine.Record) LogRow {
	l := LogRow{Producer: r.Producer, Seq: r.Seq, Event: ToNanos(r.EventTime), TTL: r.TTL, Kind: r.Kind}
	if !r.Through.IsZero() {
		l.Through, l.HasThrough = ToNanos(r.Through), true
	}
	return l
}

// logRow is the log row of a kept record.
func (c rec) logRow() LogRow {
	return LogRow{Producer: c.producer, Seq: c.seq, Event: c.event, Through: c.through, HasThrough: c.hasThrough, TTL: c.ttl, Kind: c.kind}
}

func (l LogRow) rec() rec {
	return rec{
		producer: l.Producer, seq: l.Seq, event: l.Event, through: l.Through,
		ttl: l.TTL, kind: l.Kind, hasThrough: l.HasThrough,
	}
}

func recsOf(rows []LogRow) []rec {
	out := make([]rec, len(rows))
	for i, l := range rows {
		out[i] = l.rec()
	}
	return out
}

// Changes are the row changes that take the stored rows of a subject to the rows
// its log folds to, keyed by the start of a row, which with the subject is its
// primary key: a start only in the stored rows is deleted, a start only in the new
// rows is inserted, and a start in both is updated if the row differs. A row whose
// start changes is therefore a delete and an insert.
type Changes struct {
	Deleted  []int64
	Inserted []Row
	Updated  []Row
}

// Empty reports whether nothing changes.
func (c Changes) Empty() bool { return len(c.Deleted)+len(c.Inserted)+len(c.Updated) == 0 }

// diffRows is the changes that take old to fresh, both ascending by start.
func diffRows(old, fresh []Row) Changes {
	var c Changes
	i, j := 0, 0
	for i < len(old) || j < len(fresh) {
		switch {
		case j == len(fresh) || i < len(old) && old[i].Start < fresh[j].Start:
			c.Deleted = append(c.Deleted, old[i].Start)
			i++
		case i == len(old) || fresh[j].Start < old[i].Start:
			c.Inserted = append(c.Inserted, fresh[j])
			j++
		default:
			if old[i] != fresh[j] {
				c.Updated = append(c.Updated, fresh[j])
			}
			i++
			j++
		}
	}
	return c
}

// refold folds the records of one subject that are at or after start and returns the
// rows from start on that the subject should have, and the changes that take old,
// its stored rows from start on, to them. A row that ends at or before the horizon
// is not kept: a retention would delete it.
func refold(old []Row, input []rec, start, horizon int64) ([]Row, Changes, error) {
	folded, err := foldRows(input)
	if err != nil {
		return nil, Changes{}, err
	}
	// Keep what begins at or after start and has not ended before the horizon: the
	// rest is either unchanged (before start) or a row a retention deletes.
	fresh := folded[:0:0]
	for _, r := range folded {
		if r.Start >= start && !r.EndsAtOrBefore(horizon) {
			fresh = append(fresh, r)
		}
	}
	return fresh, diffRows(old, fresh), nil
}

// Refold is the planner of the interval tables for one subject, the step both the
// [Model] and a SQL engine take for every subject a batch touches.
//
// start is the start of the last stored row that begins at or before the earliest
// new event of the subject, or math.MinInt64 if there is none (and for an engine
// that rebuilds a subject whole). old are the stored rows that start at or after
// start, ascending. input is every record of the subject, baselines included, whose
// event time is at or after start, the new records among them, in any order. A record
// before start cannot matter: start begins a maximal interval, so no segment of an
// earlier record overlaps or touches it. Baselines before start must be left out, not
// just because they are not needed: without the later record of their producer that
// ended them they would run on.
//
// It returns the rows the subject has from start on afterwards, and the changes that
// take old to them. horizon is the retention horizon; rows that end at or before it
// are neither kept nor inserted.
func Refold(old []Row, input []LogRow, start, horizon int64) (rows []Row, changes Changes, err error) {
	rs := recsOf(input)
	slices.SortFunc(rs, rec.cmp)
	return refold(old, rs, start, horizon)
}

// AliveAt folds the records of one subject, as visible to a snapshot, and reports
// whether the subject exists at t. It is what a read as of an old snapshot token does,
// for an engine whose interval tables have no system-time axis: the log is folded by
// the reader.
func AliveAt(input []LogRow, t int64) (bool, error) {
	rs := recsOf(input)
	slices.SortFunc(rs, rec.cmp)
	tl, err := foldTimeline(rs)
	if err != nil {
		return false, err
	}
	return tl.AliveAt(FromNanos(t)), nil
}
