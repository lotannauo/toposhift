package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/coalesce"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/replay/activity"
	"github.com/lotannauo/toposhift/internal/store"
)

// errSizeLimit is what the file wrapper returns once a stream passes its size limit.
var errSizeLimit = errors.New("the stream passed its size limit")

// countWriter hashes and counts what is written, and refuses more than limit bytes.
type countWriter struct {
	w     io.Writer
	h     hash.Hash
	n     int64
	limit int64
}

func (c *countWriter) Write(p []byte) (int, error) {
	if c.limit > 0 && c.n+int64(len(p)) > c.limit {
		return 0, errSizeLimit
	}
	n, err := c.w.Write(p)
	c.h.Write(p[:n])
	c.n += int64(n)
	return n, err
}

// rawAssertion is one assertion the producer made, before coalescing.
type rawAssertion struct {
	subject store.Subject
	layer   catalog.Layer
	at      time.Time
	ttl     time.Duration
}

// sinkResult is what a finished sink reports.
type sinkResult struct {
	Name string `json:"name"`
	File string `json:"file,omitempty"`
	// Records is the number of records the stream holds, written or not: the
	// coalescer's output, or for a stream with no coalescer the records handed in.
	Records int64 `json:"records"`
	// Written is whether the file holds them.
	Written bool `json:"written"`
	// DroppedOverLimit is set if the file was removed because it passed its size limit.
	DroppedOverLimit bool `json:"dropped_over_limit,omitempty"`
	// Recency, with -recency, counts the samples by how many sampling intervals the
	// stored end of the sample's run was behind it once the sample was handled
	// (the key "0" is a sample the stored run ends at), and "max" is the largest.
	Recency map[string]int64 `json:"recency_samples_by_lag,omitempty"`
	// FinishRecords, with -recency, is how many of Records the stream wrote when it
	// ended: the flushes that follow the last sample and the graceful stop's write of
	// every refresh still absorbed. They are an artifact of where the hour ends.
	FinishRecords int64  `json:"finish_records,omitempty"`
	Assertions    int64  `json:"assertions"`
	Deletes       int64  `json:"deletes"`
	WithThrough   int64  `json:"with_through"`
	MinSeq        uint64 `json:"min_seq"`
	MaxSeq        uint64 `json:"max_seq"`
	RowGroups     int    `json:"row_groups"`
	Bytes         int64  `json:"bytes"`
	FileSHA256    string `json:"file_sha256,omitempty"`
	// ContentDigest is the digest in the file's footer: of the records, in their row
	// groups, and not of the file's bytes. Two files of the same records have the same
	// one, so repeats are compared by it where their bytes may differ.
	ContentDigest string `json:"content_digest,omitempty"`
	// CoalescerFlush is how the stream's coalescer flushes: "split" (by subject, one
	// deadline at a time) or "whole" (one coalescer, as the product's); empty for a
	// stream with no coalescer.
	CoalescerFlush string `json:"coalescer_flush,omitempty"`
}

// sink takes a producer's assertions in event-time order, passes them through
// the product's ingest coalescer on a simulated ingest clock, numbers what comes
// out 1, 2, ... and writes it as an activity file.
//
// The ingest clock of an assertion is its event time plus one second, and the
// coalescer is flushed on that clock every [coalesce.FlushPeriod], so the cadence
// follows the product's. Nothing is late.
type sink struct {
	name string
	path string
	f    *os.File
	cw   *countWriter
	w    *activity.Writer
	co   *shardedCoalescer // nil for a stream with no coalescer
	one  [1]store.Record   // the record of a stream with no coalescer, to write it as a slice

	seq      uint64
	buf      []store.Record
	started  bool
	next     time.Time // the next scheduled flush
	last     time.Time // the ingest time of the latest assertion
	maxTTL   time.Duration
	rec      *recency // nil unless -recency
	res      sinkResult
	capture  bool
	raw      []rawAssertion
	err      error
	finished bool
}

// newSink starts a stream. If write is false the records are counted and not
// written. If capture is set the raw assertions are kept in memory for a check.
func newSink(name, path string, write bool, maxBytes int64, capture bool) (*sink, error) {
	return newSinkConfig(coalescerChoice{cfg: coalesceConfig()}, name, path, write, maxBytes, capture)
}

// coalescerChoice is the coalescer of a stream: its configuration, the TTL of the
// stream's records if it is known, and whether to use one whole coalescer (see
// [newCoalescerFor]).
type coalescerChoice struct {
	cfg   coalesce.Config
	ttl   time.Duration
	whole bool
}

// newSinkConfig is newSink with the coalescer chosen.
func newSinkConfig(cc coalescerChoice, name, path string, write bool, maxBytes int64, capture bool) (*sink, error) {
	co, err := newCoalescerFor(cc.cfg, cc.ttl, cc.whole)
	if err != nil {
		return nil, err
	}
	return newSinkWith(name, path, co, write, maxBytes, capture)
}

// newPlainSink starts a stream with no coalescer: every record handed to assert
// is written as it is, numbered in the order it came, with no flush clock.
func newPlainSink(name, path string, maxBytes int64, capture bool) (*sink, error) {
	return newSinkWith(name, path, nil, true, maxBytes, capture)
}

func newSinkWith(name, path string, co *shardedCoalescer, write bool, maxBytes int64, capture bool) (*sink, error) {
	s := &sink{name: name, path: path, co: co, capture: capture}
	s.res.Name = name
	if co != nil {
		s.res.CoalescerFlush = co.flushName()
	}
	if !write {
		return s, nil
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	s.f = f
	s.cw = &countWriter{w: f, h: sha256.New(), limit: maxBytes}
	s.w, err = activity.NewWriter(s.cw, activity.WriterOptions{})
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return s, nil
}

// assert takes the next assertion of a producer.
func (s *sink) assert(r store.Record) error {
	if s.err != nil {
		return s.err
	}
	if s.co == nil {
		s.res.Assertions++
		if s.capture {
			s.raw = append(s.raw, rawAssertion{r.Subject, r.Layer, r.EventTime, r.TTL})
		}
		s.one[0] = r
		return s.emit(s.one[:])
	}
	ingest := r.EventTime.Add(time.Second)
	if !s.started {
		s.started = true
		s.next = ingest.Add(coalesce.FlushPeriod)
	}
	for s.next.Before(ingest) {
		if err := s.co.flush(s.next, s.emit); err != nil {
			return err
		}
		s.next = s.next.Add(coalesce.FlushPeriod)
	}
	s.last = ingest
	s.maxTTL = max(s.maxTTL, r.TTL)
	s.res.Assertions++
	if s.capture {
		s.raw = append(s.raw, rawAssertion{r.Subject, r.Layer, r.EventTime, r.TTL})
	}
	recs, err := s.co.add(r, s.buf[:0])
	if err != nil {
		s.err = fmt.Errorf("%s: %w", s.name, err)
		return s.err
	}
	if err := s.write(recs); err != nil {
		return err
	}
	if s.rec != nil {
		s.rec.sample(r)
	}
	return nil
}

// write numbers the records the coalescer returned and writes them.
func (s *sink) write(recs []store.Record) error {
	s.buf = recs[:0]
	return s.emit(recs)
}

// emit numbers and writes records. It does not keep the slice.
func (s *sink) emit(recs []store.Record) error {
	for _, r := range recs {
		if s.rec != nil {
			s.rec.stored(r)
		}
		s.seq++
		r.Seq = s.seq
		s.res.Records++
		if r.Kind == lifecycle.Delete {
			s.res.Deletes++
		}
		if !r.Through.IsZero() {
			s.res.WithThrough++
		}
		if s.w == nil {
			continue
		}
		if err := s.w.Write(r); err != nil {
			if errors.Is(err, errSizeLimit) {
				s.drop()
				continue
			}
			s.err = fmt.Errorf("%s: %w", s.name, err)
			return s.err
		}
	}
	return nil
}

// drop abandons the file of a stream that passed its limit. The records go on
// being counted.
func (s *sink) drop() {
	_ = s.f.Close()
	_ = os.Remove(s.path)
	s.w, s.f, s.cw = nil, nil, nil
	s.res.DroppedOverLimit = true
}

// finish flushes the clock, writes what the coalescer absorbed and closes the file.
func (s *sink) finish() (sinkResult, error) {
	if s.finished {
		return s.res, s.err
	}
	s.finished = true
	if s.err != nil {
		return s.res, s.err
	}
	atFinish := s.res.Records
	if s.rec != nil {
		s.res.Recency = s.rec.result() // no sample follows the last
	}
	if s.started {
		for !s.next.After(s.last) {
			if err := s.co.flush(s.next, s.emit); err != nil {
				return s.res, err
			}
			s.next = s.next.Add(coalesce.FlushPeriod)
		}
		if err := s.co.flushAll(s.last, s.maxTTL, s.emit); err != nil {
			s.err = fmt.Errorf("%s: %w", s.name, err)
			return s.res, s.err
		}
	}
	if s.rec != nil {
		s.res.FinishRecords = s.res.Records - atFinish
	}
	// The coalescer holds a run for every subject of the stream, and is not used again.
	s.co, s.buf = nil, nil
	if s.w == nil {
		return s.res, nil
	}
	err := s.w.Close()
	if errors.Is(err, errSizeLimit) {
		s.drop()
		return s.res, nil
	}
	s.res.File = s.path
	s.res.Written = true
	s.res.Bytes = s.cw.n
	s.res.FileSHA256 = hex.EncodeToString(s.cw.h.Sum(nil))
	err = errors.Join(err, s.f.Close())
	if err != nil {
		s.err = fmt.Errorf("%s: %w", s.name, err)
		return s.res, s.err
	}
	info, err := readInfo(s.path)
	if err != nil {
		s.err = err
		return s.res, err
	}
	s.res.MinSeq, s.res.MaxSeq, s.res.RowGroups, s.res.ContentDigest = info.MinSeq, info.MaxSeq, info.RowGroups, info.Digest
	if info.Records != s.res.Records {
		s.err = fmt.Errorf("%s: the file holds %d records, but %d were written", s.name, info.Records, s.res.Records)
		return s.res, s.err
	}
	return s.res, nil
}

// readInfo opens an activity file and returns its footer's summary, which the
// reader checks against the file before it returns.
func readInfo(path string) (activity.Info, error) {
	f, err := os.Open(path)
	if err != nil {
		return activity.Info{}, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return activity.Info{}, err
	}
	r, err := activity.NewReader(f, st.Size(), activity.ReaderOptions{})
	if err != nil {
		return activity.Info{}, fmt.Errorf("%s: %w", path, err)
	}
	defer r.Close()
	return r.Info(), nil
}

// fpTable resolves each id of an interner to its entity fingerprint once.
type fpTable struct {
	res   *identity.Resolver
	typ   catalog.EntityType
	key   catalog.AttributeKey
	names *interner
	fps   []identity.Fingerprint
}

func newFPTable(res *identity.Resolver, typ catalog.EntityType, key catalog.AttributeKey, names *interner) *fpTable {
	return &fpTable{res: res, typ: typ, key: key, names: names}
}

// get is the fingerprint of the entity with the interner's token id.
func (t *fpTable) get(id uint32) identity.Fingerprint {
	t.fps = grow(t.fps, int(id)+1)
	if fp := t.fps[id]; !fp.IsZero() {
		return fp
	}
	ident, err := t.res.Resolve(t.typ, []identity.Attr{{Key: t.key, Value: t.names.name(id)}})
	if err != nil {
		// validID has already refused every value the identity layer refuses.
		panic(fmt.Sprintf("alibabareduce: %s %q: %v", t.typ, t.names.name(id), err))
	}
	t.fps[id] = ident.Fingerprint()
	return t.fps[id]
}
