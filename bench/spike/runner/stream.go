package runner

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/workload"
	"github.com/lotannauo/toposhift/internal/identity"
)

// Sink is what a stream is written to: a candidate, the reference engine, the
// analyzer.
type Sink interface {
	// Write receives a batch of records in ascending Seq, none before the horizon.
	Write(batch []engine.Record) error
	// Retain is called when the horizon moves.
	Retain(horizon time.Time) error
}

// AppliedRetention is a retention as it happened: after how many records were
// written, and with what horizon.
type AppliedRetention struct {
	AfterRecords uint64
	LastSeq      uint64
	Horizon      time.Time
}

// StreamInfo is what a run of the stream amounts to. Every candidate's build must
// come to exactly the one in the plan.
type StreamInfo struct {
	// Digest covers every record written and every retention, in order.
	Digest string
	// Records are the records written; Dropped those that arrived with an event
	// time before the horizon and were not offered to any store.
	Records, Dropped uint64
	LastSeq          uint64
	PayloadBytes     uint64
	Retentions       []AppliedRetention
	// TokenFloor is the highest Seq at the last retention, below which a store
	// guarantees nothing; OldToken is the Seq reached just before OldAt, which is
	// always above it, and which the reads of an old snapshot use.
	TokenFloor, OldToken uint64
	// OldAt is the instant a read of the old snapshot is made at, the instant of its
	// token: the newest event time the stream had reached when the token was taken,
	// which is the batch before the first that reaches three quarters through the
	// period, or halfway from the last retention to the end if that is later, and
	// before the instant any record after the token takes effect (its event time,
	// or for an extension its Through), so that everything that took effect by then
	// is in the snapshot, but not before the horizon. A read of the newest instant as of an old token would find
	// every refreshed edge expired, and ask about nothing.
	OldAt time.Time
	// Start and End are the period; Horizon is the final retention horizon.
	Start, End, Horizon time.Time
}

// Drive generates the stream of the spec and writes it to the sinks in batches
// of the spec's size, dropping the records before the horizon as a store would
// refuse them, and moving the horizon as the spec says. It is the one place the
// stream is shaped, so the plan and every candidate see the same one.
func Drive(ctx context.Context, spec Spec, sinks ...Sink) (StreamInfo, error) {
	if err := spec.Validate(); err != nil {
		return StreamInfo{}, err
	}
	g, err := workload.New(spec.Workload)
	if err != nil {
		return StreamInfo{}, err
	}
	info := StreamInfo{Start: g.Start(), End: g.End()}
	h := sha256.New()
	var horizon time.Time
	next := 0
	// The old snapshot is taken three quarters through the period, or halfway from
	// the last retention to the end if that is later: after the last retention, so
	// that its token is above the one a store guarantees nothing below, and away
	// from the instant the horizon last moved to.
	oldAt := g.Start().Add(spec.Workload.Duration / 4 * 3)
	if n := len(spec.Retentions); n > 0 {
		last := g.Start().Add(spec.Retentions[n-1].At)
		if mid := last.Add(g.End().Sub(last) / 2); mid.After(oldAt) {
			oldAt = mid
		}
	}
	oldFound := false
	var frontier time.Time // the newest event time of the batches so far
	var unseen time.Time   // the earliest instant a record after the old token takes effect
	var buf []byte
	var pinned map[identity.Fingerprint]struct{}
	if spec.Pins != nil {
		pinned = spec.Pins.entities()
	}

	for {
		if err := ctx.Err(); err != nil {
			return info, err
		}
		batch := g.Batch(spec.BatchSize)
		if len(batch) == 0 {
			break
		}
		kept := batch[:0]
		for _, r := range batch {
			if r.EventTime.Before(horizon) {
				info.Dropped++
				continue
			}
			kept = append(kept, r)
		}
		if len(kept) == 0 {
			continue
		}
		if !oldFound && !kept[len(kept)-1].EventTime.Before(oldAt) {
			// The token is the one before this batch, so the instant it is read at is the
			// newest event time that batch's predecessors reached, at most.
			oldFound, info.OldToken, info.OldAt = true, info.LastSeq, oldAt
			if !frontier.IsZero() {
				info.OldAt = frontier
			}
		}
		if oldFound { // what arrives from the token on may take effect before that instant
			for _, r := range kept {
				at := r.EventTime
				if r.Through.After(at) {
					at = r.Through
				}
				if unseen.IsZero() || at.Before(unseen) {
					unseen = at
				}
			}
		}
		for _, r := range kept {
			if r.EventTime.After(frontier) {
				frontier = r.EventTime
			}
		}
		// What is written is the batch, or with pins the part of it that touches
		// them, in the same order and with the same sequence numbers; when the
		// horizon moves is decided by the whole batch.
		out := kept
		if pinned != nil {
			out = nil // a slice of its own: a sink may keep what it is given
			for _, r := range kept {
				if touches(pinned, r) {
					out = append(out, r)
				}
			}
		}
		if len(out) > 0 {
			for _, r := range out {
				buf = appendRecord(buf[:0], r)
				h.Write(buf)
				info.PayloadBytes += uint64(len(r.Payload))
			}
			for _, s := range sinks {
				if err := s.Write(out); err != nil {
					return info, fmt.Errorf("runner: writing the batch ending at seq %d: %w", out[len(out)-1].Seq, err)
				}
			}
			info.Records += uint64(len(out))
			info.LastSeq = out[len(out)-1].Seq
		}

		for next < len(spec.Retentions) && !kept[len(kept)-1].EventTime.Before(g.Start().Add(spec.Retentions[next].At)) {
			horizon = spec.Retentions[next].Horizon(g.Start())
			g.SetHorizon(horizon)
			for _, s := range sinks {
				if err := s.Retain(horizon); err != nil {
					return info, fmt.Errorf("runner: retaining before %s: %w", horizon.Format(time.RFC3339), err)
				}
			}
			info.Retentions = append(info.Retentions, AppliedRetention{AfterRecords: info.Records, LastSeq: info.LastSeq, Horizon: horizon})
			info.TokenFloor = info.LastSeq
			h.Write(appendRetention(buf[:0], info.Records, horizon))
			next++
		}
	}
	if next < len(spec.Retentions) {
		return info, fmt.Errorf("runner: the stream ended before retention %d (at %s) was reached", next, spec.Retentions[next].At)
	}
	if !oldFound { // a stream with a single batch
		info.OldToken = info.LastSeq
	}
	if info.OldToken < info.TokenFloor {
		return info, fmt.Errorf("runner: the old snapshot (seq %d, at %s) comes before the last retention (seq %d) within one batch: use a smaller batch or a retention further from the end", info.OldToken, oldAt.Format(time.RFC3339), info.TokenFloor)
	}
	info.Horizon = horizon
	// The instant is read as of a token, so it must come before the instant any
	// record after the token takes effect: a pipeline that is backed up at the
	// token has delivered a refresh of an edge only up to a while ago, and a read of
	// a later instant would find it lapsed, which every candidate agrees on
	// trivially.
	if !unseen.IsZero() && unseen.Add(-time.Nanosecond).Before(info.OldAt) {
		info.OldAt = unseen.Add(-time.Nanosecond)
	}
	if info.OldAt.IsZero() { // a stream of a single batch
		info.OldAt = oldAt
	}
	// A store answers from the horizon on. A record that takes effect at the horizon
	// itself (a run that began before it, restarted there) and comes after the token
	// would put the instant before it: the snapshot is then read at the horizon, which
	// the baseline of the retention covers.
	if info.OldAt.Before(horizon) {
		info.OldAt = horizon
	}
	info.Digest = hex.EncodeToString(h.Sum(nil))
	return info, nil
}

// appendRecord is the canonical bytes of a record for the stream digest: every
// field a store is given, with lengths where a field varies.
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
	if !r.Through.IsZero() {
		through = r.Through.UnixNano()
	}
	b = binary.BigEndian.AppendUint64(b, uint64(through))
	b = binary.BigEndian.AppendUint32(b, uint32(len(r.Payload)))
	return append(b, r.Payload...)
}

func appendRetention(b []byte, afterRecords uint64, horizon time.Time) []byte {
	b = append(b, 0xff, 'R')
	b = binary.BigEndian.AppendUint64(b, afterRecords)
	return binary.BigEndian.AppendUint64(b, uint64(horizon.UnixNano()))
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
