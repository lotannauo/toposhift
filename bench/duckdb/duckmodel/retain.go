package duckmodel

import (
	"math"
	"slices"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// newer reports whether a record at (event, seq) is newer for one producer than the
// record at (otherEvent, otherSeq): by event time, then by Seq.
func newer(event int64, seq uint64, otherEvent int64, otherSeq uint64) bool {
	return event > otherEvent || event == otherEvent && seq > otherSeq
}

// retainRecs applies a retention to the records of one subject, ascending as a file
// has them (see [rec.cmpInFile]): it keeps those at or after the horizon and, for each
// producer, the last record before it, its baseline. past says the horizon is after
// every instant, so every record is before it.
func retainRecs(recs []rec, h int64, past bool, mut mutant) []rec {
	before := func(c rec) bool { return past || c.event < h }
	var out []rec
	var best rec
	have := false
	consider := func(c rec) {
		better := !have || newer(c.event, c.seq, best.event, best.seq)
		if mut == mutBaselineBySeq {
			better = !have || c.seq > best.seq
		}
		if better {
			best, have = c, true
		}
	}
	flush := func() {
		if have {
			out = append(out, best)
		}
		have = false
	}
	for a := 0; a < len(recs); {
		b := a + 1
		for b < len(recs) && recs[b].producer == recs[a].producer {
			b++
		}
		for _, c := range recs[a:b] {
			if before(c) {
				consider(c)
			} else {
				out = append(out, c)
			}
		}
		if mut != mutBaselinePerSubject {
			flush()
		}
		a = b
	}
	flush() // with one baseline for the subject, the only one
	slices.SortFunc(out, rec.cmpInFile)
	return out
}

// baselineKey is what a baseline stands for: one producer's reference to one subject.
type baselineKey struct {
	subject  engine.Subject
	producer lifecycle.Producer
}

// Filter is a retention applied to a whole stream: which records a file written from the
// stream keeps when the horizon is the last the stream moved to. Applying the final
// horizon once keeps what applying each earlier one in turn would, because each keeps
// the newest record of a producer before its horizon, and the newest before a later
// horizon is among what an earlier one kept or is at or after the earlier horizon.
//
// A Filter reads the stream twice: every record is shown to Add, then each is asked of
// Keep.
type Filter struct {
	horizon int64
	past    bool
	mut     mutant
	best    map[baselineKey]rec
}

// NewFilter returns the filter of a retention at horizon. A horizon before the range
// keeps every record; one after it keeps each producer's last record only.
func NewFilter(horizon time.Time) *Filter { return newFilter(horizon, mutNone) }

func newFilter(horizon time.Time, mut mutant) *Filter {
	f := &Filter{mut: mut, best: make(map[baselineKey]rec)}
	switch {
	case horizon.After(engine.MaxEventTime):
		f.horizon, f.past = math.MaxInt64, true
	case horizon.After(engine.MinEventTime):
		f.horizon = ToNanos(horizon)
	}
	return f
}

// meta is the part of a record a retention looks at, without the payload.
func meta(r engine.Record) rec {
	return rec{producer: r.Producer, event: ToNanos(r.EventTime), seq: r.Seq}
}

func (f *Filter) before(c rec) bool { return f.past || c.event < f.horizon }

func (f *Filter) key(r engine.Record) baselineKey {
	k := baselineKey{subject: r.Subject, producer: r.Producer}
	if f.mut == mutBaselinePerSubject {
		k.producer = ""
	}
	return k
}

// Add shows the filter a record of the stream, in any order.
func (f *Filter) Add(r engine.Record) {
	c := meta(r)
	if !f.before(c) {
		return
	}
	k := f.key(r)
	best, ok := f.best[k]
	better := !ok || newer(c.event, c.seq, best.event, best.seq)
	if f.mut == mutBaselineBySeq {
		better = !ok || c.seq > best.seq
	}
	if better {
		f.best[k] = c
	}
}

// Keep reports whether the retention keeps the record: it is at or after the horizon, or
// it is the baseline of its subject and producer among the records Add was shown.
func (f *Filter) Keep(r engine.Record) bool {
	c := meta(r)
	if !f.before(c) {
		return true
	}
	best, ok := f.best[f.key(r)]
	return ok && best.seq == c.seq
}
