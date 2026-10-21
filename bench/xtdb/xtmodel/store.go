package xtmodel

import (
	"fmt"
	"slices"
	"sort"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// version is one row of a bitemporal table: a document as it was over a stretch of
// valid time [vFrom, vTo), as the database knew it over a stretch of system time
// [sFrom, sTo). A version that is not superseded has sTo = Forever.
type version struct {
	vFrom, vTo Instant
	sFrom, sTo Instant
}

// docState is the versions of one segment document.
//
// XTDB keeps a document's history by rewriting it: a statement over a stretch of valid
// time closes, at the transaction's system time, every version that overlaps the
// stretch, and writes the parts of those versions outside it again from that system
// time. The store does the same, so the versions that are live (sTo = Forever) are
// always the current valid-time picture: ascending and disjoint. The closed ones are
// what a read at an earlier system time sees.
type docState struct {
	key    Key
	layer  catalog.Layer
	live   []version
	closed []version
}

// paint applies a statement at system time sys: the document is present over
// [from, to) if insert and absent there otherwise, and unchanged elsewhere.
func (d *docState) paint(from, to, sys Instant, insert bool) {
	i := sort.Search(len(d.live), func(k int) bool { return d.live[k].vTo > from })
	j := sort.Search(len(d.live), func(k int) bool { return d.live[k].vFrom >= to })
	var repl []version
	for _, v := range d.live[i:j] {
		// A version made earlier in this same transaction was never visible, so it
		// leaves no history.
		if v.sFrom < sys {
			d.closed = append(d.closed, version{v.vFrom, v.vTo, v.sFrom, sys})
		}
	}
	if i < j && d.live[i].vFrom < from {
		repl = append(repl, version{d.live[i].vFrom, from, sys, Forever})
	}
	if insert {
		repl = append(repl, version{from, to, sys, Forever})
	}
	if i < j && d.live[j-1].vTo > to {
		repl = append(repl, version{to, d.live[j-1].vTo, sys, Forever})
	}
	d.live = slices.Replace(d.live, i, j, repl...)
}

// view is the system time a read is made at.
type view struct {
	// latest says the read is of the newest system time, which has no clause; sys is
	// meaningful only if it is false.
	latest bool
	sys    Instant
}

// at reports whether the document is present at valid time t, as the database knew
// it at the view's system time.
func (d *docState) at(t Instant, v view) bool {
	k := sort.Search(len(d.live), func(k int) bool { return d.live[k].vFrom > t }) - 1
	if k >= 0 && t < d.live[k].vTo && (v.latest || d.live[k].sFrom <= v.sys) {
		return true
	}
	if v.latest {
		return false
	}
	for _, c := range d.closed {
		if c.vFrom <= t && t < c.vTo && c.sFrom <= v.sys && v.sys < c.sTo {
			return true
		}
	}
	return false
}

// logEntry is a log document and the system time of the transaction that wrote it.
// A log document is never overwritten, so it is visible from then on.
type logEntry struct {
	rec *Rec
	sys Instant
}

func (a logEntry) cmp(b logEntry) int {
	switch {
	case a.rec.Event < b.rec.Event:
		return -1
	case a.rec.Event > b.rec.Event:
		return 1
	case a.rec.Seq < b.rec.Seq:
		return -1
	case a.rec.Seq > b.rec.Seq:
		return 1
	}
	return 0
}

// metaDoc is a document of the meta table: the batch with ordinal n ended at lastSeq,
// and was committed at system time sys.
type metaDoc struct {
	n, lastSeq uint64
	sys        Instant
}

// image is everything an XTDB node holds for the baseline's tables, as a value: what
// a directory keeps. A [Model] is an image with the engine's state in memory over
// it, rebuilt from the image on every open.
type image struct {
	// lastSys is the system time of the last transaction. A transaction must be
	// strictly later.
	lastSys Instant
	// docs and incident hold the segment tables' documents, by key and by the entity
	// they are read from.
	docs     map[Key]*docState
	incident [NumTables]map[identity.Fingerprint][]*docState
	// logs hold the log tables' documents by the entity they are read from,
	// ascending by (event time, seq), which is the order of a window.
	logs [NumTables]map[identity.Fingerprint][]logEntry
	// times is an index of the log by segKey: the distinct event times of the
	// producer's records about the subject, ascending, which is what the planner
	// asks for a late record.
	times  map[segKey][]int64
	layers map[engine.Subject]catalog.Layer
	metas  []metaDoc

	// horizon is the retention horizon: records before it are refused, in Unix
	// nanoseconds, and pastEnd says it was moved after the last representable
	// instant, so every record is before it. A server adapter keeps it in a document
	// of its own; here it is part of what the directory holds.
	horizon int64
	pastEnd bool
}

func newImage() *image {
	im := &image{
		docs:   make(map[Key]*docState),
		times:  make(map[segKey][]int64),
		layers: make(map[engine.Subject]catalog.Layer),
	}
	for t := range NumTables {
		im.incident[t] = make(map[identity.Fingerprint][]*docState)
		im.logs[t] = make(map[identity.Fingerprint][]logEntry)
	}
	return im
}

// apply commits a transaction, all of it or none: a transaction that cannot commit
// changes nothing, as in the database.
func (im *image) apply(tx Txn) error {
	if tx.System <= im.lastSys {
		return fmt.Errorf("xtmodel: a transaction at system time %s is not after the last, %s", tx.System, im.lastSys)
	}
	for i, o := range tx.Ops {
		switch {
		case o.Kind != Insert && o.Kind != DeletePortion:
			return fmt.Errorf("xtmodel: statement %d has kind %d", i, o.Kind)
		case o.Key.Table.segment():
			if o.From >= o.To {
				return fmt.Errorf("xtmodel: statement %d (%s) has an empty valid-time range", i, o)
			}
		case o.Key.Table.logs():
			if o.Kind != Insert || o.Rec == nil {
				return fmt.Errorf("xtmodel: statement %d (%s) is not an insert of a record", i, o)
			}
		case o.Key.Table == Meta:
			if o.Kind != Insert {
				return fmt.Errorf("xtmodel: statement %d (%s) is not an insert", i, o)
			}
		default:
			return fmt.Errorf("xtmodel: statement %d is on table %s", i, o.Key.Table)
		}
	}
	im.lastSys = tx.System
	for _, o := range tx.Ops {
		switch {
		case o.Key.Table.segment():
			im.segment(o, tx.System)
		case o.Key.Table.logs():
			im.log(o, tx.System)
		default:
			im.metas = append(im.metas, metaDoc{n: o.Key.Seq, lastSeq: o.LastSeq, sys: tx.System})
		}
	}
	return nil
}

func (im *image) segment(o Op, sys Instant) {
	d := im.docs[o.Key]
	if d == nil {
		if o.Kind == DeletePortion {
			return // no document to delete from
		}
		d = &docState{key: o.Key, layer: o.Layer}
		im.docs[o.Key] = d
		fp := o.Key.entity()
		im.incident[o.Key.Table][fp] = append(im.incident[o.Key.Table][fp], d)
	}
	d.paint(o.From, o.To, sys, o.Kind == Insert)
}

func (im *image) log(o Op, sys Instant) {
	e := logEntry{rec: o.Rec, sys: sys}
	fp := o.Key.entity()
	l := im.logs[o.Key.Table][fp]
	i := sort.Search(len(l), func(k int) bool { return l[k].cmp(e) > 0 })
	im.logs[o.Key.Table][fp] = slices.Insert(l, i, e)
	if o.Key.Table == LogR {
		return // the index and the layer are made once, from the forward document
	}
	im.layers[o.Rec.Subject] = o.Rec.Layer
	sk := segKey{o.Rec.Subject, o.Rec.Producer}
	ts := im.times[sk]
	k, found := slices.BinarySearch(ts, o.Rec.Event)
	if !found {
		im.times[sk] = slices.Insert(ts, k, o.Rec.Event)
	}
}

// Next implements [History].
func (im *image) Next(s engine.Subject, p lifecycle.Producer, after int64) (int64, bool, error) {
	ts := im.times[segKey{s, p}]
	k := sort.Search(len(ts), func(k int) bool { return ts[k] > after })
	if k == len(ts) {
		return 0, false, nil
	}
	return ts[k], true, nil
}

// planner returns a planner over the image's log that remembers what the log says: it
// is how an engine reopened over the image gets the memory it had.
func (im *image) planner() *Planner {
	b := NewRebuilder()
	for _, t := range []Table{LogF, LogE} {
		for _, l := range im.logs[t] {
			for _, e := range l {
				b.add(e.rec)
			}
		}
	}
	return b.Planner(im)
}
