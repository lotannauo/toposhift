package vmmodel

import (
	"container/heap"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"math"
	"slices"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// The errors a [Converter] returns. Each wraps one of these, so a caller tells the
// reason with errors.Is.
var (
	// ErrUnregistered marks a record whose producer does not hold its subject in the
	// registry.
	ErrUnregistered = errors.New("vmmodel: the producer is not registered as a holder of the subject")
	// ErrWindow marks an Observe whose TTL is not the one its producer is registered
	// with: the window of a refreshing producer, none for a producer in watch mode. The
	// registry and the stream disagree about how long a sample counts.
	ErrWindow = errors.New("vmmodel: the TTL of the record is not its producer's registered one")
	// ErrCoalesced marks a record that stands for a run of refreshes (a Through). The
	// converter takes the every-sample stream, where each refresh is a record, and cannot
	// turn a run into the samples a scraper would have written.
	ErrCoalesced = errors.New("vmmodel: a record that stands for a run of refreshes is not an every-sample record")
	// ErrOrder marks a record whose sequence number is not above the one before it.
	ErrOrder = errors.New("vmmodel: records are not in ascending sequence order")
	// ErrTooLate marks a record whose event time is a whole reorder window or more behind
	// the newest event time seen: a sample that old could collide with one already written,
	// and a collision cannot be told apart once the sample is out.
	ErrTooLate = errors.New("vmmodel: the record is older than the reorder window")
	// ErrTime marks a record whose event time a sample cannot carry.
	ErrTime = errors.New("vmmodel: the event time is before 1970")
	// ErrClosed marks a use of a converter after Close or after an error.
	ErrClosed = errors.New("vmmodel: the converter is closed")
)

// DefaultReorderWindow is how far behind the newest event time a record may arrive.
const DefaultReorderWindow = 6 * time.Hour

// Sample is one sample of a series: a value 1 at a time, or a staleness marker at a
// time. Series is an index into the converter's series, in order of first appearance.
type Sample struct {
	Series      uint32
	TimestampMs int64
	// Stale marks a staleness marker (StaleNaN), the end of the series at its
	// timestamp (assumption (b), verified); its value is not 1.
	Stale bool
}

// Options configure a [Converter].
type Options struct {
	// Registry says which producer holds which subject and with which window. Required.
	Registry *Registry
	// ReorderWindow is how far behind the newest event time seen a record may be. A
	// converter holds back the samples within it, so that two records of one series in
	// one millisecond are told apart and the later by sequence number wins; a record
	// older than it is an error ([ErrTooLate]), never a silent second sample. Zero is
	// [DefaultReorderWindow]. It must be a whole number of milliseconds.
	ReorderWindow time.Duration
	// Until is the end of the period: a fact held by a producer in watch mode that has
	// not been deleted by then is written at every scrape through this instant
	// (inclusive). It is required when the registry has a producer in watch mode.
	Until time.Time
	// Emit receives samples at the end of each Write and of Close, and in chunks while a
	// long-lived fact is written out. The slice is reused after Emit returns. If it
	// returns an error the converter is closed with it. Nil drops the samples, which
	// leaves the counts.
	Emit func([]Sample) error
}

// emitChunk is the most samples held before Emit is called while a fact's scrapes are
// written out.
const emitChunk = 1 << 16

// HourCount is the number of series whose first sample falls in one hour.
type HourCount struct {
	// HourMs is the start of the hour, in Unix milliseconds.
	HourMs  int64
	Created int
}

// Counts are the numbers a conversion comes to. Every one is a function of the stream
// and the registry, and the same on every run.
type Counts struct {
	// Records, Observes and Deletes are what was read.
	Records, Observes, Deletes uint64
	// Samples are the samples of value 1 written and Markers the staleness markers.
	// Samples include Scraped.
	Samples, Markers uint64
	// Scraped are the samples of value 1 that no record asked for: the scrapes at which
	// a fact held by a producer in watch mode is written again between its assertion
	// and its delete (or the end of the period). They are the cost of a metrics store
	// holding state, and they are counted with the rest. Reasserted are the Observes
	// of a series that was already being scraped, which write nothing: the scraper was
	// writing the fact already.
	Scraped, Reasserted uint64
	// CollapsedObserves and CollapsedDeletes are the records dropped because a later one
	// of the same series had the same millisecond (the later by sequence number is kept,
	// whatever its kind).
	CollapsedObserves, CollapsedDeletes uint64
	// Series are the series seen, EntitySeries and EdgeSeries by metric.
	Series, EntitySeries, EdgeSeries int
	// Churn is the series created in each hour, by the time of the earliest sample of the
	// series, in ascending hours, leaving out the hours in which none was.
	Churn []HourCount
}

// Collapses is the records dropped for sharing a millisecond with a later one.
func (c Counts) Collapses() uint64 { return c.CollapsedObserves + c.CollapsedDeletes }

type seriesKey struct {
	kind     engine.SubjectKind
	layer    catalog.Layer
	producer lifecycle.Producer
	rel      catalog.RelationType
	a, b     identity.Fingerprint
}

func (k seriesKey) series() Series {
	if k.kind == engine.SubjectEntity {
		return EntitySeries(k.layer, k.a, k.producer)
	}
	return EdgeSeries(k.layer, k.a, k.b, k.rel, k.producer)
}

type sampleKey struct {
	series uint32
	ms     int64
}

// pend is a sample held back until no record can collide with it.
type pend struct {
	series uint32
	ms     int64
	stale  bool
	dead   bool // superseded by a later record of the same series and millisecond
}

// Converter turns the every-sample stream into samples. It is a runner.Sink, so a
// stream is driven into it as into an engine.
//
// The mapping is the series model's:
//
//   - an Observe of a refreshing producer at event time e writes one sample, value 1, at
//     e truncated to the millisecond (assumption (i), verified);
//   - an Observe of a producer in watch mode opens its series: a sample at e, and then
//     one at every [Holder.Scrape] after it (the scrapes of an exporter that writes the
//     fact again each time it is scraped) until the Delete, strictly before it, or the
//     end of the period;
//   - a Delete writes a staleness marker at its event time, as a Prometheus scraper does
//     when an exporter stops exporting a deleted object (assumption (b), verified);
//   - a producer that goes silent writes nothing: its series ends when the window its
//     queries name passes. Expiry is never a marker.
//
// Two records of one series in one millisecond leave one sample, the later by sequence
// number (a Delete after an Observe leaves the marker, an Observe after a Delete the
// sample), and the number dropped is counted. The Converter holds samples back for the
// reorder window to know. Records must arrive in ascending sequence order, as an engine
// is written. The records of a producer in watch mode are held for the window too, and
// handled in event-time order (then sequence order), so that a delete that arrives
// before the assertion it ends, in a stream whose arrival is out of order, still ends it.
//
// A Converter is not safe for concurrent use.
type Converter struct {
	reg      *Registry
	windowMs int64
	untilMs  int64
	emit     func([]Sample) error

	series []seriesKey
	minMs  []int64
	index  map[seriesKey]uint32

	pending map[sampleKey]*pend
	fifo    []*pend
	head    int

	// held are the records of producers in watch mode, until none can arrive before them
	// in event time, and open the series being scraped, with the millisecond of the
	// sample that opened each and the interval it is scraped at.
	held watchHeap
	open map[uint32]openSeries

	lastSeq uint64
	maxMs   int64
	counts  Counts
	out     []Sample
	digest  hash.Hash
	buf     [13]byte
	closed  error
	done    bool
	final   string
}

// NewConverter returns a converter with no records.
func NewConverter(o Options) (*Converter, error) {
	if o.Registry == nil {
		return nil, errors.New("vmmodel: a converter needs a registry")
	}
	w := o.ReorderWindow
	if w == 0 {
		w = DefaultReorderWindow
	}
	if w < 0 || w%time.Millisecond != 0 {
		return nil, fmt.Errorf("vmmodel: the reorder window %s is not a positive whole number of milliseconds", w)
	}
	c := &Converter{
		reg: o.Registry, windowMs: w.Milliseconds(), emit: o.Emit,
		index: map[seriesKey]uint32{}, pending: map[sampleKey]*pend{}, open: map[uint32]openSeries{},
		digest: sha256.New(),
	}
	if o.Registry.HasWatch() {
		if o.Until.IsZero() {
			return nil, errors.New("vmmodel: a registry with a producer in watch mode needs the end of the period (Options.Until)")
		}
		c.untilMs = o.Until.UnixMilli()
	}
	return c, nil
}

// Retain implements runner.Sink. It does nothing: the metrics store of the baseline
// keeps everything it is given, and the samples before a horizon stay in it.
func (c *Converter) Retain(time.Time) error { return nil }

// Write converts a batch. An error closes the converter: what it held back is lost and
// every later call returns [ErrClosed] wrapping the first error.
func (c *Converter) Write(batch []engine.Record) error {
	if c.closed != nil {
		return c.closed
	}
	for _, r := range batch {
		if err := c.add(r); err != nil {
			return c.fail(err)
		}
	}
	if err := c.sweep(false); err != nil {
		return c.fail(err)
	}
	c.release(false)
	return c.flush()
}

func (c *Converter) fail(err error) error {
	c.closed = fmt.Errorf("%w: %w", ErrClosed, err)
	return err
}

func (c *Converter) add(r engine.Record) error {
	if r.Seq <= c.lastSeq {
		return fmt.Errorf("%w: seq %d follows seq %d", ErrOrder, r.Seq, c.lastSeq)
	}
	if !r.Through.IsZero() {
		return fmt.Errorf("%w: seq %d has Through %s", ErrCoalesced, r.Seq, r.Through.Format(time.RFC3339Nano))
	}
	h, ok := c.reg.holds(r.Subject, r.Producer)
	if !ok {
		return fmt.Errorf("%w: seq %d, producer %q, subject %s", ErrUnregistered, r.Seq, r.Producer, describe(r.Subject))
	}
	switch r.Kind {
	case lifecycle.Observe:
		want := h.Window
		if h.Watch() {
			want = 0
		}
		if r.TTL != want {
			return fmt.Errorf("%w: seq %d, producer %q has TTL %s, registered %s", ErrWindow, r.Seq, r.Producer, r.TTL, want)
		}
	case lifecycle.Delete:
	default:
		return fmt.Errorf("vmmodel: seq %d has record kind %d", r.Seq, r.Kind)
	}
	if r.EventTime.Before(engine.MinEventTime) {
		return fmt.Errorf("%w: seq %d at %s", ErrTime, r.Seq, r.EventTime.Format(time.RFC3339Nano))
	}
	if r.Subject.Kind != engine.SubjectEntity && r.Subject.Kind != engine.SubjectEdge {
		return fmt.Errorf("%w: seq %d has subject kind %d", ErrSeries, r.Seq, r.Subject.Kind)
	}
	ms := r.EventTime.UnixMilli()

	c.lastSeq = r.Seq
	c.counts.Records++
	if ms > c.maxMs {
		c.maxMs = ms
	}
	if c.maxMs-ms >= c.windowMs {
		return fmt.Errorf("%w: seq %d is %d ms behind the newest event time, the window is %d ms", ErrTooLate, r.Seq, c.maxMs-ms, c.windowMs)
	}

	key := seriesKey{kind: r.Subject.Kind, layer: r.Layer, producer: r.Producer, rel: r.Subject.Relation, a: r.Subject.A, b: r.Subject.B}
	idx, ok := c.index[key]
	if !ok {
		if len(c.series) >= math.MaxUint32 {
			return errors.New("vmmodel: more than 2^32-1 series")
		}
		idx = uint32(len(c.series))
		c.index[key] = idx
		c.series = append(c.series, key)
		c.minMs = append(c.minMs, ms)
	} else if ms < c.minMs[idx] {
		c.minMs[idx] = ms
	}

	stale := r.Kind == lifecycle.Delete
	if stale {
		c.counts.Deletes++
	} else {
		c.counts.Observes++
	}
	if h.Watch() {
		heap.Push(&c.held, heldRecord{series: idx, ms: ms, seq: r.Seq, stale: stale, scrapeMs: h.Scrape.Milliseconds()})
		return nil
	}
	c.queue(idx, ms, stale)
	return nil
}

// queue holds a sample back until no record can collide with it. A sample of the same
// series and millisecond that is still held is superseded: the later one wins.
func (c *Converter) queue(idx uint32, ms int64, stale bool) {
	sk := sampleKey{idx, ms}
	if old, ok := c.pending[sk]; ok {
		old.dead = true
		if old.stale {
			c.counts.CollapsedDeletes++
		} else {
			c.counts.CollapsedObserves++
		}
	}
	p := &pend{series: idx, ms: ms, stale: stale}
	c.pending[sk] = p
	c.fifo = append(c.fifo, p)
}

// openSeries is a series of a producer in watch mode that is being scraped.
type openSeries struct {
	// startMs is the sample that opened it, and the scrapes follow at intervals of
	// scrapeMs from it.
	startMs, scrapeMs int64
}

// heldRecord is a record of a producer in watch mode, waiting to be handled in order.
type heldRecord struct {
	series   uint32
	ms       int64
	seq      uint64
	stale    bool
	scrapeMs int64
}

// watchHeap orders the held records by event millisecond and then sequence number.
type watchHeap []heldRecord

func (h watchHeap) Len() int { return len(h) }
func (h watchHeap) Less(i, j int) bool {
	if h[i].ms != h[j].ms {
		return h[i].ms < h[j].ms
	}
	return h[i].seq < h[j].seq
}
func (h watchHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *watchHeap) Push(x any)   { *h = append(*h, x.(heldRecord)) }
func (h *watchHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// sweep handles the held records of producers in watch mode that no record can precede
// any more (all of them if final), in event-time order. An Observe of a series that is
// not being scraped opens it, with a sample queued like any other; one of a series that
// is changes nothing. A Delete writes the scrapes from the opening up to, not
// including, its millisecond, and a marker queued at it, so that an Observe in the same
// millisecond and a Delete collapse as two records of any series do.
func (c *Converter) sweep(final bool) error {
	for len(c.held) > 0 && (final || c.maxMs-c.held[0].ms >= c.windowMs) {
		r := heap.Pop(&c.held).(heldRecord)
		if r.stale {
			if o, ok := c.open[r.series]; ok {
				delete(c.open, r.series)
				if err := c.scrapes(r.series, o, r.ms-1); err != nil {
					return err
				}
			}
			c.queue(r.series, r.ms, true)
			continue
		}
		if _, ok := c.open[r.series]; ok {
			c.counts.Reasserted++
			continue
		}
		c.open[r.series] = openSeries{startMs: r.ms, scrapeMs: r.scrapeMs}
		c.queue(r.series, r.ms, false)
	}
	return nil
}

// scrapes writes the samples of an open series after the one that opened it, at its
// interval, up to last (inclusive). They are written at once, in order, outside the
// held-back samples: no record of the series can collide with them.
func (c *Converter) scrapes(idx uint32, o openSeries, last int64) error {
	for ms := o.startMs + o.scrapeMs; ms <= last; ms += o.scrapeMs {
		c.write(idx, ms, false)
		c.counts.Scraped++
		if len(c.out) >= emitChunk {
			if err := c.flush(); err != nil {
				return err
			}
		}
	}
	return nil
}

// write puts a sample into the digest, the counts and the output.
func (c *Converter) write(idx uint32, ms int64, stale bool) {
	binary.BigEndian.PutUint32(c.buf[0:4], idx)
	binary.BigEndian.PutUint64(c.buf[4:12], uint64(ms))
	c.buf[12] = 0
	if stale {
		c.buf[12] = 1
		c.counts.Markers++
	} else {
		c.counts.Samples++
	}
	c.digest.Write(c.buf[:])
	if c.emit != nil {
		c.out = append(c.out, Sample{Series: idx, TimestampMs: ms, Stale: stale})
	}
}

// release writes the samples that no record can collide with any more, in the order of
// the records that made them, or all of them if final.
func (c *Converter) release(final bool) {
	for c.head < len(c.fifo) {
		p := c.fifo[c.head]
		if !p.dead && !final && c.maxMs-p.ms < c.windowMs {
			break // a record may still arrive for this series and millisecond
		}
		c.fifo[c.head] = nil
		c.head++
		if p.dead {
			continue
		}
		delete(c.pending, sampleKey{p.series, p.ms})
		c.write(p.series, p.ms, p.stale)
	}
	if c.head > 4096 && c.head > len(c.fifo)/2 {
		c.fifo = append(c.fifo[:0], c.fifo[c.head:]...)
		c.head = 0
	}
}

func (c *Converter) flush() error {
	if len(c.out) == 0 {
		return nil
	}
	err := c.emit(c.out)
	c.out = c.out[:0]
	if err != nil {
		return c.fail(err)
	}
	return nil
}

// Close writes the samples still held back and ends the conversion. Counts and Digest
// are final after it.
func (c *Converter) Close() error {
	if c.done {
		return nil
	}
	if c.closed != nil {
		return c.closed
	}
	if err := c.sweep(true); err != nil {
		return c.fail(err)
	}
	// What is still being scraped is scraped through the end of the period, in the order
	// the series were opened.
	idxs := make([]uint32, 0, len(c.open))
	for idx := range c.open {
		idxs = append(idxs, idx)
	}
	slices.Sort(idxs)
	for _, idx := range idxs {
		if err := c.scrapes(idx, c.open[idx], c.untilMs); err != nil {
			return c.fail(err)
		}
	}
	clear(c.open)
	c.release(true)
	if err := c.flush(); err != nil {
		return err
	}
	c.done = true
	c.closed = fmt.Errorf("%w: already closed", ErrClosed)
	series := sha256.New()
	for _, k := range c.series {
		key := k.series().Key()
		fmt.Fprintf(series, "%d:%s\n", len(key), key)
	}
	sum := sha256.New()
	sum.Write(c.digest.Sum(nil))
	sum.Write(series.Sum(nil))
	c.final = hex.EncodeToString(sum.Sum(nil))
	return nil
}

// Series returns the series with the index a [Sample] carries.
func (c *Converter) Series(i uint32) Series { return c.series[i].series() }

// Counts returns the numbers of the conversion so far. After Close they are final.
func (c *Converter) Counts() Counts {
	out := c.counts
	out.Series = len(c.series)
	byHour := map[int64]int{}
	for i, k := range c.series {
		if k.kind == engine.SubjectEntity {
			out.EntitySeries++
		} else {
			out.EdgeSeries++
		}
		byHour[floorDiv(c.minMs[i], int64(time.Hour/time.Millisecond))]++
	}
	hours := make([]int64, 0, len(byHour))
	for h := range byHour {
		hours = append(hours, h)
	}
	slices.Sort(hours)
	for _, h := range hours {
		out.Churn = append(out.Churn, HourCount{HourMs: h * int64(time.Hour/time.Millisecond), Created: byHour[h]})
	}
	return out
}

// Digest is the SHA-256, as hex, of the samples written (series index, millisecond and
// kind, in the order written) and of the series (their labels, in index order). It is
// the same for two runs over the same stream and registry, and defined only after Close;
// before it the result is empty.
func (c *Converter) Digest() string { return c.final }

func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && (a < 0) != (b < 0) {
		q--
	}
	return q
}

func describe(s engine.Subject) string {
	if s.Kind == engine.SubjectEdge {
		return fmt.Sprintf("%s -%s-> %s", s.A, s.Relation, s.B)
	}
	return s.A.String()
}
