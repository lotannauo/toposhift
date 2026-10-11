package coalesce

import (
	"bytes"
	"cmp"
	"slices"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
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
	// prev is the run this one replaced, and no further back: a refresh that
	// arrives late, after this run started, may belong to it.
	prev        *run
	layer       catalog.Layer
	start, last time.Time // last is the latest refresh seen, whether or not it was recorded
	recorded    time.Time // the Through of the latest extension written
	ttl         time.Duration
	boot        string
	basis       store.EventTimeBasis // of the refresh that started the run, whose time is the run's start
	payload     []byte               // the run's own copy
}

// deadline is when the store's copy of the run lapses: the last extension written
// plus the TTL. It can be earlier than the producer's own (last plus TTL), because
// refreshes inside the extension interval are absorbed without a record.
func (st *run) deadline() time.Time { return st.recorded.Add(st.ttl) }

// describes is whether a refresh repeats what the run asserts: the same TTL,
// payload and boot, and the same basis for its event time. A reboot is a new run,
// and so is a change of basis: the run's first event time is the time it asserts,
// and where that time came from is part of what the record says about it.
func (st *run) describes(r store.Record) bool {
	return st.ttl == r.TTL && st.boot == r.Boot && st.basis == r.EventTimeBasis && bytes.Equal(st.payload, r.Payload)
}

func newRun(r store.Record, replaced *run) *run {
	if replaced != nil {
		replaced.prev = nil // the chain is one run long
	}
	return &run{
		prev: replaced, layer: r.Layer, start: r.EventTime, last: r.EventTime, recorded: r.EventTime,
		ttl: r.TTL, boot: r.Boot, basis: r.EventTimeBasis, payload: slices.Clone(r.Payload),
	}
}

// covers is whether the run, whose refreshes the store already has through its
// stored deadline, stands for the refresh: the same description, at an instant
// between its first and last.
func (st *run) covers(r store.Record) bool {
	return st != nil && st.describes(r) && !r.EventTime.Before(st.start) && !r.EventTime.After(st.last)
}

// Coalescer is the ingest-side run coalescer. It is not safe for concurrent use: the
// product has one, run by the single sequencer, in arrival order.
type Coalescer struct {
	cfg      Config
	runs     map[runKey]*run
	horizons [int(catalog.L3) + 1]time.Time // by layer; the zero time is no horizon
}

// New returns a coalescer with no runs and no horizon, or the error that
// [Config.Validate] reports.
func New(c Config) (*Coalescer, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &Coalescer{cfg: c, runs: make(map[runKey]*run)}, nil
}

// SetHorizon tells the coalescer the store has retained layer before h. It only moves
// forward. A layer outside L0..L3 is ignored: [store.Record.Validate] refuses a record
// in one, so no run is ever in it.
func (c *Coalescer) SetHorizon(layer catalog.Layer, h time.Time) {
	if layer < catalog.L0 || layer > catalog.L3 {
		return
	}
	if h.After(c.horizons[layer]) {
		c.horizons[layer] = h
	}
}

// SetHorizonAll sets every layer, as a store that retains every layer under one
// horizon does.
func (c *Coalescer) SetHorizonAll(h time.Time) {
	for l := catalog.L0; l <= catalog.L3; l++ {
		c.SetHorizon(l, h)
	}
}

// Horizon is the horizon the coalescer holds for layer: the zero time if it was
// told none, or the layer is not one the catalog has.
func (c *Coalescer) Horizon(layer catalog.Layer) time.Time {
	if layer < catalog.L0 || layer > catalog.L3 {
		return time.Time{}
	}
	return c.horizons[layer]
}

// Runs is how many runs the coalescer remembers.
func (c *Coalescer) Runs() int { return len(c.runs) }

// Add takes the next record in arrival order and appends to out what the store is to be
// given for it: nothing (a refresh the run already stands for), the record itself, an
// extension of its run, or the record that closes the run it replaces followed by the
// record. Seq is ignored on input and zero on output: the caller assigns Seq to what Add
// returns, in order. Add does not validate (the store does).
//
// A record that is returned itself, or as an extension, keeps the Payload slice it was
// given; the record that closes a run carries a copy of its own, and the coalescer keeps
// another, so the caller may reuse its buffers once it has used what Add returned for
// the record. Add takes raw records: one that already carries a Through is passed on
// as given, like a delete, and ends the subject's run, whose state it no longer
// matches, with two exceptions at its layer's horizon h: one that starts before h and
// reaches it (its Through is at or after h) is re-timed to h and keeps its Through,
// and one that ends before h is passed on unchanged and leaves the runs alone.
// Nothing is written for the tail of the run that its absorbed refreshes stood for,
// so existence can show a gap of less than one extension interval before such a
// record, as it can before a watch-mode record or a delete; that gap is accepted. A
// caller that calls [Coalescer.Flush] accepts it only for these records, and for a
// run whose last refresh is before its layer's horizon.
//
// An extension re-asserts the run at its first event time, so once the store has
// retained past that time it would be refused and the run would silently die at
// its deadline. A run that started before the retention horizon of its layer is
// therefore continued by a new run, asserted at the refresh's own event time. A new
// run also starts when the description changes (a payload, a TTL, a boot or the
// basis of the event time), and when the run has reached the age [Config.RunMaxAge]
// allows or, with [Config.RunAgeFromTTL], its own extension interval (such a run is
// never extended by a refresh, but is closed with a Through when its stored copy
// lapsed before the new run). An extension and the record that closes a run carry
// the basis of the refresh that started it; a record that is passed on as given
// keeps its own.
//
// A new run replaces the old run's deadline, so existence stays continuous only
// if the old run's stored deadline reaches the new run's start. Refreshes inside
// the extension interval are absorbed, so it may not: the producer was seen at
// the last of them and its promise runs to then plus the TTL. The record closeRun makes
// is what the store is missing in that case. A run that ends in a real silence (the
// next refresh comes after the producer's own deadline) is not closed: it ends at
// its stored deadline, early by less than the extension interval, which is what
// bounding the extension trades away, unless [Coalescer.Flush] wrote the absorbed
// refreshes before the stored copy lapsed.
func (c *Coalescer) Add(r store.Record, out []store.Record) []store.Record {
	r.Seq = 0
	key := runKey{r.Subject, r.Producer}
	if r.Kind == lifecycle.Delete || r.TTL == 0 || !r.Through.IsZero() {
		if !r.Through.IsZero() {
			// A record that stands for a span, and starts before its layer's horizon,
			// is kept from the horizon on, as closeRun does for a run that began
			// before it. One that ends before the horizon is wholly past what the
			// store keeps: it is passed on as given, for the late path after the
			// coalescer to refuse and count, and the run is left alone, because a
			// record the store will never hold must not end a live run.
			if h := c.Horizon(r.Layer); r.EventTime.Before(h) {
				if r.Through.Before(h) {
					return append(out, r)
				}
				r.EventTime = h
			}
		}
		// A delete, a watch-mode assertion, or a record that already stands for a run of
		// its own (a Through is the coalescer's to set, so this one is not a refresh)
		// ends any run. It is passed on as given: overwriting its Through, or absorbing
		// it, would lose the longer one. The run is dropped without closeRun, as for a
		// delete or a watch-mode assertion, so the tail its absorbed refreshes stood for
		// is not written: if the run's stored deadline falls short of its last refresh
		// (an extension interval longer than the TTL), a record from the same producer
		// that starts after the deadline shows a gap of less than one extension interval
		// that the producer never had. That gap is accepted for these records, which end
		// a run before Flush has written its absorbed refreshes; any other run's
		// absorbed refreshes are written by Flush, when the caller calls it, before its
		// stored copy lapses.
		delete(c.runs, key)
		return append(out, r)
	}
	st := c.runs[key]
	switch {
	case st == nil:
		c.runs[key] = newRun(r, nil)
		return append(out, r)
	case r.EventTime.Before(st.start):
		// Older than the run, and not part of it, whatever it describes: a late refresh
		// must not start a run in the past and replace the current one. If the run before it covered that
		// instant, it already stands for the refresh: passing it through would be an
		// assertion that replaces that run's deadline from its instant on, and could
		// end its existence before the new run begins. Otherwise leave it as it is.
		if st.prev.covers(r) {
			return out
		}
		return append(out, r)
	case !st.describes(r):
		// The description changed: this starts a run.
		out = c.closeRun(key, st, r.EventTime, out)
		c.runs[key] = newRun(r, st)
		return append(out, r)
	case !r.EventTime.After(st.last):
		return out // already covered by the run
	case st.start.Before(c.Horizon(st.layer)) || r.EventTime.After(st.last.Add(st.ttl)) ||
		(c.cfg.ageBound(st.ttl) > 0 && r.EventTime.Sub(st.start) >= c.cfg.ageBound(st.ttl)):
		// A run the store can no longer extend, a gap longer than the TTL, or a run
		// that has reached its greatest age: a new run.
		out = c.closeRun(key, st, r.EventTime, out)
		c.runs[key] = newRun(r, st)
		return append(out, r)
	}
	st.last = r.EventTime
	if every := c.cfg.ExtensionInterval(st.ttl); every > 0 && st.last.Sub(st.recorded) < every {
		return out // absorbed: the run is not re-asserted yet
	}
	st.recorded = st.last
	r.EventTime, r.Through = st.start, st.recorded
	return append(out, r)
}

// closeRun appends the record that makes the stored deadline of st reach until,
// the event time of the run that replaces it, when the producer's refreshes
// reach until and the stored deadline does not. It appends nothing when the store
// already covers until, when the producer itself was silent past its deadline
// (there is a real gap, and the new run follows it), or when until is before the
// horizon (the store would refuse the new run's start, and nothing before the
// horizon is kept).
//
// A run the store can still extend is extended: the same record the coalescer
// would have written at the refresh it absorbed last. A run that began before the
// horizon is continued by one that starts at the earlier of its last refresh and
// the lapse of its stored copy, or at the horizon if that is later, and carries a
// Through (the last refresh) only when it starts before that refresh. The producer's
// promise from the last refresh is replaced at until by the new run's, so only the
// existence from the record's start to until is what it adds. The record starts no
// later than the stored deadline, so the two runs touch, except when the deadline
// lapsed before the horizon: the gap before the horizon stays, and the store keeps
// nothing there.
//
// The record carries the basis of the run, which names the clock that stamped the
// run's refreshes. Its event time is the refresh's only when it is the run's start or
// last refresh: when it is the stored deadline or the horizon, the coalescer computed
// it, and the clock the basis names did not stamp it. That is accepted: a reader
// treats the time of a closing or continuation record as derived.
func (c *Coalescer) closeRun(key runKey, st *run, until time.Time, out []store.Record) []store.Record {
	horizon := c.Horizon(st.layer)
	if !st.deadline().Before(until) || until.After(st.last.Add(st.ttl)) || until.Before(horizon) {
		return out
	}
	return append(out, c.carry(key, st))
}

// carry is the record that carries st to its last refresh: the extension the
// coalescer would have written at that refresh, or, for a run that began before the
// horizon of its layer, the record that continues it (see closeRun). It is the one
// place the two are built, so a run closed by the refresh that replaces it and a run
// flushed before its stored copy lapses are written alike. It carries a copy of the
// run's payload, and Seq is zero.
func (c *Coalescer) carry(key runKey, st *run) store.Record {
	horizon := c.Horizon(st.layer)
	rec := store.Record{
		Layer: st.layer, Subject: key.subject, Producer: key.producer,
		Kind: lifecycle.Observe, TTL: st.ttl, Payload: slices.Clone(st.payload), Boot: st.boot, EventTimeBasis: st.basis,
	}
	if !st.start.Before(horizon) {
		rec.EventTime, rec.Through = st.start, st.last
		return rec
	}
	at := st.last
	if d := st.deadline(); d.Before(at) {
		at = d // the store's copy lapsed before the last refresh: begin at the lapse
	}
	if at.Before(horizon) {
		at = horizon
	}
	rec.EventTime = at
	if at.Before(st.last) {
		// The record begins at the lapse of the store's copy, or at the horizon, before the
		// run's last refresh. Without a Through it would stand for [at, at+TTL) only, which
		// ends before last+TTL when the copy lapsed more than a TTL before the last
		// refresh (an extension interval longer than the TTL) and leaves a hole before the
		// next run. Every refresh of the run, from its start to last, came within the TTL
		// of the one before it, so the producer was seen continuously from at to last.
		rec.Through = st.last
	}
	return rec
}

// FlushPeriod is the longest the caller of [Coalescer.Flush] lets pass between two calls.
const FlushPeriod = 30 * time.Second

// FlushLead is how close to its stored deadline a run is flushed: [Coalescer.Flush]
// writes a run's absorbed refreshes when the run's stored copy lapses no later than
// FlushLead after now. It is two flush periods, so a caller that misses one period
// still writes them before the copy lapses.
const FlushLead = 2 * FlushPeriod

// Flush appends to out the records that write the refreshes the coalescer has
// absorbed, for the runs whose stored copy lapses no later than [FlushLead] after now
// (or has lapsed already). now is the caller's clock, compared with event-time
// deadlines; the coalescer has none of its own. The caller calls Flush at least every
// [FlushPeriod]. See "Flush" in the package documentation.
//
// For a run with something absorbed (its last refresh is after its latest extension
// written), Flush writes the extension the coalescer would have written at that
// refresh, and the run's stored deadline becomes its last refresh plus the TTL. A run
// that began before the horizon of its layer is continued as closeRun continues it,
// by a new run that starts where the record does and carries the old one as its
// previous run, so that a late refresh the old run covered is still covered. A run
// whose last refresh is before the horizon of its layer is left as it is, and nothing
// is written for it: the store keeps nothing of it.
//
// The records are in a fixed order: by the stored deadline the run had before the
// flush, then by subject (kind, then each entity by type and hash, then the
// relation), then by producer. Seq is zero on output, as for [Coalescer.Add]; the
// caller assigns it in order. The cost is a scan of the runs and a sort of the records
// written.
func (c *Coalescer) Flush(now time.Time, out []store.Record) []store.Record {
	return c.flush(now.Add(FlushLead), true, out)
}

// FlushAll is Flush with no deadline test: it writes the absorbed refreshes of every
// run, in the same order. The caller uses it when ingest stops gracefully.
func (c *Coalescer) FlushAll(out []store.Record) []store.Record {
	return c.flush(time.Time{}, false, out)
}

// flushed is a record written by a flush with the stored deadline its run had before.
type flushed struct {
	lapse time.Time
	key   runKey
	rec   store.Record
}

func (c *Coalescer) flush(limit time.Time, bounded bool, out []store.Record) []store.Record {
	var written []flushed
	for key, st := range c.runs {
		if !st.last.After(st.recorded) || (bounded && st.deadline().After(limit)) {
			continue
		}
		horizon := c.Horizon(st.layer)
		if st.last.Before(horizon) {
			continue // wholly before what the store keeps
		}
		lapse := st.deadline()
		rec := c.carry(key, st)
		if st.start.Before(horizon) {
			// The record starts at rec.EventTime and its run is continued from there. The
			// old run stays as the previous run, for a refresh that arrives late.
			st.prev = nil
			c.runs[key] = &run{
				prev: st, layer: st.layer, start: rec.EventTime, last: st.last, recorded: st.last,
				ttl: st.ttl, boot: st.boot, basis: st.basis, payload: st.payload, // never written to
			}
		} else {
			st.recorded = st.last
		}
		written = append(written, flushed{lapse, key, rec})
	}
	slices.SortFunc(written, func(a, b flushed) int {
		return cmp.Or(
			a.lapse.Compare(b.lapse),
			compareSubjects(a.key.subject, b.key.subject),
			cmp.Compare(a.key.producer, b.key.producer),
		)
	})
	for _, w := range written {
		out = append(out, w.rec)
	}
	return out
}

// compareSubjects orders subjects by kind, then source entity (type, then hash), then
// target entity, then relation. It is total on the subjects the store takes.
func compareSubjects(a, b store.Subject) int {
	return cmp.Or(
		cmp.Compare(a.Kind, b.Kind),
		compareFingerprints(a.A, b.A),
		compareFingerprints(a.B, b.B),
		cmp.Compare(a.Relation, b.Relation),
	)
}

func compareFingerprints(a, b identity.Fingerprint) int {
	ha, hb := a.Hash(), b.Hash()
	return cmp.Or(cmp.Compare(a.Type(), b.Type()), bytes.Compare(ha[:], hb[:]))
}

// Prune forgets the runs whose producer's own deadline (last refresh plus TTL) is before
// now, and returns how many it forgot. The caller decides when, and what now is: a
// sweep every few thousand records, with the event time of the next record, keeps the
// work small.
//
// With a stream of subjects that come and go (a pod is a new subject each time it is
// created), a map that never forgets grows with the length of the stream. The next
// refresh of a forgotten run would have started a new run anyway, because it came after
// the deadline; only a record that arrives late enough to have continued the run is
// passed through instead, and passing a record through is always valid.
//
// Prune keeps a run with an absorbed refresh (its last refresh is after its latest
// extension written) whose last refresh is at or after the horizon of its layer,
// until [Coalescer.Flush] or [Coalescer.FlushAll] writes it: forgetting it would lose
// the refresh. A run whose last refresh is before the horizon of its layer is
// forgotten as before, because nothing is ever written for it. A caller that prunes
// but never flushes keeps every such run in memory.
func (c *Coalescer) Prune(now time.Time) int {
	n := 0
	for k, st := range c.runs {
		if !st.last.Add(st.ttl).Before(now) {
			continue
		}
		if st.last.After(st.recorded) && !st.last.Before(c.Horizon(st.layer)) {
			continue // Flush has yet to write what it absorbed
		}
		delete(c.runs, k)
		n++
	}
	return n
}
