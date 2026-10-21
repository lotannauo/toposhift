package duckmodel

import (
	"cmp"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// Interval is one maximal run of existence [Start, End) in Unix nanoseconds, End unbounded
// when Open, with how it ended. It is the row an interval file holds, less the subject.
type Interval struct {
	Start int64
	// End is the end of the run; it is meaningless when Open.
	End  int64
	Open bool
	// EndSource says how a closed run ended; it is unknown for an open one.
	EndSource lifecycle.EndSource
}

// Contains reports whether t is in the run.
func (iv Interval) Contains(t int64) bool {
	return t >= iv.Start && (iv.Open || t < iv.End)
}

// IntervalRow is one row of an interval file: the maximal run of existence of one
// subject, seen from one end. For an edge read forward A is the source and Peer the
// target; read in reverse, A is the target and Peer the source. For an entity A is the
// entity and Peer and Relation are empty.
type IntervalRow struct {
	Layer    catalog.Layer
	A, Peer  string
	Relation catalog.RelationType
	Interval
}

// foldIntervals folds the records of one subject, in any order, with the lifecycle
// specification and returns its maximal intervals in ascending order.
func foldIntervals(recs []rec, mut mutant) ([]Interval, error) {
	if mut == mutIntervalsSplit {
		return foldPerProducer(recs, mut)
	}
	if len(recs) == 0 {
		return nil, nil
	}
	as := make([]lifecycle.Assertion, len(recs))
	for i, c := range recs {
		as[i] = c.assertion()
	}
	tl, err := lifecycle.Fold(as, lifecycle.Policy{})
	if err != nil {
		return nil, fmt.Errorf("duckmodel: %w", err)
	}
	var out []Interval
	for _, iv := range tl.Existence() {
		x := Interval{Start: ToNanos(iv.Start), EndSource: iv.EndSource}
		if iv.Open() {
			x.Open = true
			if mut == mutOpenGetsEnd {
				x.Open, x.End = false, int64(1<<63-1)
			}
		} else {
			x.End = ToNanos(iv.End)
		}
		if mut == mutEndSourceDropped {
			x.EndSource = lifecycle.EndUnknown
		}
		out = append(out, x)
	}
	return out, nil
}

// foldPerProducer is the fault of leaving each producer's intervals as they are.
func foldPerProducer(recs []rec, mut mutant) ([]Interval, error) {
	var out []Interval
	for a := 0; a < len(recs); {
		b := a + 1
		for b < len(recs) && recs[b].producer == recs[a].producer {
			b++
		}
		ivs, err := foldIntervals(recs[a:b], mutNone)
		if err != nil {
			return nil, err
		}
		out = append(out, ivs...)
		a = b
	}
	slices.SortFunc(out, func(x, y Interval) int { return cmp.Compare(x.Start, y.Start) })
	return out, nil
}

// assertion is the lifecycle assertion of the row. The attributes are left out:
// existence does not depend on them under the zero policy, and the fold would only copy
// them.
func (c rec) assertion() lifecycle.Assertion {
	a := lifecycle.Assertion{
		Producer: c.producer, EventTime: FromNanos(c.event), Seq: c.seq,
		Kind: lifecycle.Observe, TTL: time.Duration(c.ttl),
	}
	if c.del {
		a.Kind = lifecycle.Delete
	}
	if c.hasThrough {
		a.Through = FromNanos(c.through)
	}
	return a
}

// intervalsContain finds the run that could contain t, the last that starts at or
// before it, and asks it. The runs of a subject are maximal, so they neither overlap
// nor touch, and at most one of them contains t.
func intervalsContain(ivs []Interval, t int64) bool {
	i := sort.Search(len(ivs), func(i int) bool { return ivs[i].Start > t })
	return i > 0 && ivs[i-1].Contains(t)
}

// sortRows puts rows in the order of an interval file: by layer, A, start, then peer and
// relation, all as a file orders them, A and Peer by their text.
func sortRows(rows []IntervalRow) {
	slices.SortFunc(rows, func(x, y IntervalRow) int {
		return cmp.Or(
			cmp.Compare(x.Layer.String(), y.Layer.String()),
			cmp.Compare(x.A, y.A),
			cmp.Compare(x.Start, y.Start),
			cmp.Compare(x.Peer, y.Peer),
			cmp.Compare(x.Relation, y.Relation),
		)
	})
}

// SubjectIntervals folds the records of one subject, in any order, and returns its
// maximal intervals ascending: what the model would hold for the subject, for a builder
// that folds the subjects of a file one after another. The records must all be of one
// subject; the payloads are not read.
func SubjectIntervals(records []engine.Record) ([]Interval, error) {
	recs := make([]rec, len(records))
	for i, r := range records {
		recs[i] = keep(r)
	}
	return foldIntervals(recs, mutNone)
}

// SortRows puts the rows of an interval file in the file's order: by layer, then the
// text of the fingerprint they are seen from, then start, then peer and relation.
func SortRows(rows []IntervalRow) { sortRows(rows) }
