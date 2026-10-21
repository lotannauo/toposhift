// Package filestream turns an activity file into what the spike's runner makes of a
// generated stream: the stream written to sinks in batches, the description of it a plan
// holds (digest, counts, the old snapshot token and the instant it is read at), and,
// from it, the questions to ask of an engine with the reference engine's answer to each.
//
// The runner can only plan a stream its own generator makes. An activity file made from
// real data (one hour of a trace, reduced to the records a store takes) has no generator,
// so its stream is replayed from the file and the plan is rebuilt from it by the same
// steps: the analyzer picks the prefixes, [runner.BuildQueries] makes the questions, and
// the reference engine answers them from the records that touch the entities asked about.
// Nothing here reads a DuckDB file and nothing is linked from C: the package is the
// stream and the questions, which the DuckDB baseline then holds its reads to.
//
// A stream replayed from a file has no retention: it keeps every record, never moves a
// horizon, and drops nothing.
package filestream

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"time"

	"github.com/lotannauo/toposhift/bench/duckdb/duckmodel"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/replay/activity"
)

// DefaultBatchSize is the batch the spike's streams are written in.
const DefaultBatchSize = 1000

// Period is the span of time a stream is about: the instant its first record may take
// effect, and how long it lasts. The questions are asked as of its end and at the ages
// before it, so it is part of what a question set is, and it is given, not read from
// the file, so that the questions do not move with the data.
type Period struct {
	Start    time.Time
	Duration time.Duration
}

// End is the end of the period.
func (p Period) End() time.Time { return p.Start.Add(p.Duration) }

// Validate reports a period a stream cannot be about.
func (p Period) Validate() error {
	switch {
	case p.Start.IsZero() || p.Start.Before(engine.MinEventTime):
		return fmt.Errorf("filestream: the period starts at %s, before the first event time a store holds", p.Start.Format(time.RFC3339))
	case p.Duration <= 0:
		return fmt.Errorf("filestream: the period lasts %s", p.Duration)
	case p.End().After(engine.MaxEventTime) || p.End().Before(p.Start):
		return fmt.Errorf("filestream: the period ends after the last event time a store holds")
	}
	return nil
}

// Options say how a file is replayed.
type Options struct {
	Period Period
	// BatchSize is how many records a Write takes; zero is [DefaultBatchSize]. The old
	// snapshot token is a batch boundary, so it is part of what a stream is.
	BatchSize int
	// Digest is the content digest the file must have (the footer's, "sha256:" and 64
	// hex digits); it is required, so that a stream is only ever the one that was named.
	Digest string
}

func (o Options) batch() int {
	if o.BatchSize == 0 {
		return DefaultBatchSize
	}
	return o.BatchSize
}

// ErrDigest is wrapped by the error returned for a file whose content digest is not the
// one expected.
var ErrDigest = errors.New("filestream: the file is not the one that was named")

// ErrPeriod is wrapped by the error returned for a record that is not inside the period.
var ErrPeriod = errors.New("filestream: a record is outside the period")

// convert is how a record of the file becomes an engine record: the event time basis,
// which says which clock stamped the time and which no read depends on, is dropped. A
// boot id is not accepted: it would say the file holds host observations, which a read
// of this baseline would answer without it.
var convert = duckmodel.Options{DropBasis: true}

// mutant is a deliberate fault. Only tests set one: each is a fault the equivalence of
// [Replay] with the runner's own stream must catch.
type mutant uint8

const (
	mutNone mutant = iota
	// mutTokenAfterBatch takes the old snapshot token after the batch that reaches the
	// instant, not before it.
	mutTokenAfterBatch
	// mutOldAtNominal reads the old snapshot at the nominal three quarters of the period,
	// not at the newest event time the earlier batches reached.
	mutOldAtNominal
	// mutNoUnseen leaves out the bound that the records after the token put on the
	// instant.
	mutNoUnseen
	// mutDigestWithoutThrough leaves the through time out of the stream digest.
	mutDigestWithoutThrough
	// mutLastBatchDropped leaves out the last, shorter batch.
	mutLastBatchDropped
	// mutShadowSourceOnly feeds the reference engine only the records an entity is the
	// source of, so that a read from the target end is answered from half the edges.
	mutShadowSourceOnly
)

var fault = mutNone

// Replay writes the stream of the file to the sinks in batches in ascending Seq, as
// [runner.Drive] writes a generated stream, and returns what the stream amounts to.
// It reads the whole file, which verifies it against its footer, and refuses a file
// whose content digest is not opts.Digest, a record before the period starts or after it
// ends (by its event time or its through time), and a Seq above the largest int64.
//
// The description is [runner.Drive]'s for a stream without retentions: the digest covers
// every record in order, the old snapshot token is the Seq reached just before the first
// batch that reaches three quarters of the way through the period, and the instant it is
// read at is the newest event time that earlier batches reached, held below the instant
// any later record takes effect. A test holds the two to be one function on streams the
// runner generates.
func Replay(ctx context.Context, path string, opts Options, sinks ...runner.Sink) (runner.StreamInfo, error) {
	if err := opts.Period.Validate(); err != nil {
		return runner.StreamInfo{}, err
	}
	if !validDigest(opts.Digest) {
		return runner.StreamInfo{}, fmt.Errorf("filestream: the content digest to expect, %q, is not %q and 64 lowercase hex digits", opts.Digest, "sha256:")
	}
	f, err := os.Open(path)
	if err != nil {
		return runner.StreamInfo{}, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return runner.StreamInfo{}, err
	}
	rd, err := activity.NewReader(f, st.Size(), activity.ReaderOptions{})
	if err != nil {
		return runner.StreamInfo{}, fmt.Errorf("filestream: %s: %w", path, err)
	}
	defer rd.Close()
	if got := rd.Info().Digest; got != opts.Digest {
		return runner.StreamInfo{}, fmt.Errorf("%w: %s has content digest %s, not the expected %s", ErrDigest, path, got, opts.Digest)
	}

	start, end := opts.Period.Start, opts.Period.End()
	info := runner.StreamInfo{Start: start, End: end}
	h := sha256.New()
	oldAt := start.Add(opts.Period.Duration / 4 * 3)
	oldFound := false
	var frontier time.Time // the newest event time of the batches so far
	var unseen time.Time   // the earliest instant a record after the old token takes effect
	var buf []byte
	size := opts.batch()

	more := true
	for more {
		if err := ctx.Err(); err != nil {
			return info, err
		}
		batch := make([]engine.Record, 0, size)
		for len(batch) < size {
			sr, err := rd.Next()
			if errors.Is(err, io.EOF) {
				more = false
				break
			}
			if err != nil {
				return info, fmt.Errorf("filestream: %s: %w", path, err)
			}
			r, err := duckmodel.EngineRecord(sr, convert)
			if err != nil {
				return info, err
			}
			if err := checkRecord(r, start, end); err != nil {
				return info, err
			}
			batch = append(batch, r)
		}
		if len(batch) == 0 {
			break
		}
		if fault == mutLastBatchDropped && len(batch) < size {
			break
		}
		if !oldFound && !batch[len(batch)-1].EventTime.Before(oldAt) {
			oldFound, info.OldToken, info.OldAt = true, info.LastSeq, oldAt
			if fault == mutTokenAfterBatch {
				info.OldToken = batch[len(batch)-1].Seq
			}
			if !frontier.IsZero() && fault != mutOldAtNominal {
				info.OldAt = frontier
			}
		}
		if oldFound && fault != mutNoUnseen { // what arrives from the token on may take effect before that instant
			for _, r := range batch {
				at := r.EventTime
				if r.Through.After(at) {
					at = r.Through
				}
				if unseen.IsZero() || at.Before(unseen) {
					unseen = at
				}
			}
		}
		for _, r := range batch {
			if r.EventTime.After(frontier) {
				frontier = r.EventTime
			}
		}
		for _, r := range batch {
			buf = appendRecord(buf[:0], r)
			h.Write(buf)
			info.PayloadBytes += uint64(len(r.Payload))
		}
		for _, s := range sinks {
			if err := s.Write(batch); err != nil {
				return info, fmt.Errorf("filestream: writing the batch ending at seq %d: %w", batch[len(batch)-1].Seq, err)
			}
		}
		info.Records += uint64(len(batch))
		info.LastSeq = batch[len(batch)-1].Seq
	}
	if info.Records == 0 {
		return info, errors.New("filestream: the file holds no record")
	}
	if int64(info.Records) != rd.Info().Records {
		return info, fmt.Errorf("filestream: replayed %d records of the file's %d", info.Records, rd.Info().Records)
	}
	if !oldFound { // a stream with a single batch
		info.OldToken = info.LastSeq
	}
	// The instant is read as of a token, so it must come before the instant any record
	// after the token takes effect: a pipeline that is backed up at the token has
	// delivered a refresh of an edge only up to a while ago, and a read of a later instant
	// would find it lapsed, which every engine agrees on trivially.
	if !unseen.IsZero() && unseen.Add(-time.Nanosecond).Before(info.OldAt) {
		info.OldAt = unseen.Add(-time.Nanosecond)
	}
	if info.OldAt.IsZero() { // a stream of a single batch
		info.OldAt = oldAt
	}
	info.Digest = hex.EncodeToString(h.Sum(nil))
	return info, nil
}

// checkRecord refuses a record that is not in the period, or whose Seq a statement cannot
// be given as a token. That the Seq ascend is the activity reader's to check, and it does.
func checkRecord(r engine.Record, start, end time.Time) error {
	switch {
	case r.Seq > math.MaxInt64:
		return fmt.Errorf("filestream: seq %d is above the largest int64, which a statement cannot be given as a token", r.Seq)
	case r.EventTime.Before(start) || r.EventTime.After(end):
		return fmt.Errorf("%w: seq %d has event time %s, the period is %s to %s", ErrPeriod, r.Seq, r.EventTime.Format(time.RFC3339Nano), start.Format(time.RFC3339), end.Format(time.RFC3339))
	case !r.Through.IsZero() && (r.Through.Before(start) || r.Through.After(end)):
		return fmt.Errorf("%w: seq %d has through time %s, the period is %s to %s", ErrPeriod, r.Seq, r.Through.Format(time.RFC3339Nano), start.Format(time.RFC3339), end.Format(time.RFC3339))
	}
	return nil
}

func validDigest(s string) bool {
	hexPart, ok := strings.CutPrefix(s, "sha256:")
	if !ok || len(hexPart) != 64 {
		return false
	}
	for _, c := range hexPart {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// The canonical bytes of a record for the stream digest are the runner's, so that a
// stream replayed from a file and the stream the runner generated, when they are the
// same records, have the same digest. A test holds the two functions to each other.

func appendRecord(b []byte, r engine.Record) []byte {
	b = binary.BigEndian.AppendUint64(b, r.Seq)
	b = append(b, byte(r.Layer), byte(r.Kind), byte(r.Subject.Kind))
	b = appendFingerprint(b, r.Subject.A)
	b = appendFingerprint(b, r.Subject.B)
	b = appendString(b, string(r.Subject.Relation))
	b = appendString(b, string(r.Producer))
	b = binary.BigEndian.AppendUint64(b, uint64(r.EventTime.UnixNano()))
	b = binary.BigEndian.AppendUint64(b, uint64(r.TTL))
	through := int64(0)
	if !r.Through.IsZero() && fault != mutDigestWithoutThrough {
		through = r.Through.UnixNano()
	}
	b = binary.BigEndian.AppendUint64(b, uint64(through))
	b = binary.BigEndian.AppendUint32(b, uint32(len(r.Payload)))
	return append(b, r.Payload...)
}

func appendString(b []byte, s string) []byte {
	b = binary.BigEndian.AppendUint16(b, uint16(len(s)))
	return append(b, s...)
}

func appendFingerprint(b []byte, f identity.Fingerprint) []byte {
	if f.IsZero() {
		return append(b, 0)
	}
	b = appendString(append(b, 1), string(f.Type()))
	sum := f.Hash()
	return append(b, sum[:]...)
}

// FileDigest is the content digest in the footer of an activity file, the name its stream
// is expected by (see [Options.Digest]). It reads the footer only, which the reader checks
// against the row groups' digests the footer lists; a replay reads, and so verifies, the
// rows.
func FileDigest(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return "", err
	}
	rd, err := activity.NewReader(f, st.Size(), activity.ReaderOptions{})
	if err != nil {
		return "", fmt.Errorf("filestream: %s: %w", path, err)
	}
	defer rd.Close()
	return rd.Info().Digest, nil
}
