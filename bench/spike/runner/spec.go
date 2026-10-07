// Package runner builds the candidate layouts on one planned stream, reads the
// same queries from each, and reports what the reads did, so the layouts can be
// compared on counters that do not depend on timing.
//
// The steps are separate and each is a process of its own:
//
//   - [MakePlan] reads the workload twice without any store. The first pass finds
//     the prefixes the queries will ask about (the busiest, and some in the
//     middle); the second feeds the reference engine only the records that touch
//     them and records its answers. The plan is the one file every later step
//     takes: the stream's digest, the queries, and the answer to each.
//   - [Build] writes the planned stream to one candidate, refusing a candidate
//     whose written records are not the planned ones, compacts everything and
//     records what the database holds.
//   - [Read] opens the built database read-only in a fresh process, asks every
//     query twice and checks every answer against the plan, asks it twice more
//     with the block cache empty for the blocks a first read needs, and counts
//     its allocations. It records what each query cost, in Pebble's own unit
//     for the reads and in blocks and bytes for the cold ones.
//   - [Report] sets the results of the candidates side by side.
//
// Nothing here times anything. A counter is the same on every run and every
// machine; a timing is only a confirmation, taken on CI hardware later.
package runner

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/workload"
)

// Retention is one move of the retention horizon: when the event time of the
// last record of a batch reaches Start+At, the store is told to retain
// everything before Start+At-Keep.
type Retention struct {
	At   time.Duration
	Keep time.Duration
}

// Horizon is the event time before which the retention discards history, for a
// stream that starts at start.
func (r Retention) Horizon(start time.Time) time.Time { return start.Add(r.At - r.Keep) }

// SpecVersion is bumped when the meaning of a spec field, or the way the
// queries are chosen from it, changes, so that digests of different meanings
// cannot be equal. 3: the reads three and nine hours back and the read after
// every refresh has lapsed (AgeDead).
const SpecVersion = 3

// Spec is everything that must be identical for every candidate. Its digest is
// recorded in the plan and in every manifest.
type Spec struct {
	Version  int
	Workload workload.Config
	// BatchSize is the number of records per Write. A smaller batch at the end of
	// the stream is the only exception.
	BatchSize  int
	Retentions []Retention
	// Hot is how many of the busiest prefixes of each class are queried, by
	// records and by run extensions, and Median how many around the middle.
	Hot, Median int
	// CacheBytes is the block cache every candidate gets, the same absolute size in
	// every run, so that runs of different lengths and windows are read under the
	// same eviction. If it is zero, CacheFraction sizes the cache instead, as a share
	// of the payload bytes of the stream; exactly one of them is set.
	CacheBytes    int64
	CacheFraction float64
	// MinNonEmpty is the least share of each group of queries that must have a
	// non-empty answer: an empty answer agrees between candidates trivially.
	MinNonEmpty float64
	// Pins, if set, are the prefixes the stream is read at and, unless FullStore is
	// set, the only ones it is written for: the records that touch none of them are
	// not written to any store. The queries are the pins', not chosen from the stream.
	Pins *Pins `json:",omitempty"`
	// FullStore, with Pins, writes the whole stream to every store and asks the pins'
	// queries of it: a full store read at the pinned prefixes, whose block counters,
	// stall, commit, bytes and checkpoint shares are a full build's, as a projection's
	// are not. Without pins every store is full and it is not set.
	FullStore bool `json:",omitempty"`
}

// WindowSpec is the spec of a run that has always held days days of history, from
// the spec of the workload it is a window of: the stream is 2R + 1.5 days long,
// the retention keeps R days and runs every day from R + 1 days on, and the stream
// ends twelve hours after the last. The runs that began at the start, which are
// all of the long-lived ones and begin together, then restart every R + 1 days, so
// at the end the oldest are R + 0.5 days old whatever R is, the same phase in every
// window, and the store has held R days since the first retention and has been
// through R + 1 daily retentions that each kept R.
func WindowSpec(base Spec, days int) Spec {
	r := time.Duration(days) * 24 * time.Hour
	s := base
	s.Workload.Duration = 2*r + 36*time.Hour
	s.Retentions = nil
	for k := 0; k <= days; k++ {
		s.Retentions = append(s.Retentions, Retention{At: r + 24*time.Hour + time.Duration(k)*24*time.Hour, Keep: r})
	}
	return s
}

// DefaultCacheBytes is the block cache of a default spec: large enough that one
// read from empty fills a small part of it (the report warns above a half), small
// enough to hold in a laptop's memory next to the stream.
const DefaultCacheBytes = 64 << 20

// DefaultSpec is the spec of a workload: the starting point of the options, and
// not a decision about any of them.
func DefaultSpec(w workload.Config) Spec {
	s := Spec{
		Version: SpecVersion, Workload: w, BatchSize: 1000,
		Hot: 10, Median: 5, CacheBytes: DefaultCacheBytes, MinNonEmpty: 0.25,
	}
	// Retain half a day before the end, keeping a day and a half: a day back is
	// then in the middle of the retained history and not on the instant every run
	// that began before the horizon starts again, which would make the reads of a
	// refreshed prefix a day back cost nothing in a layout that keeps a baseline.
	if w.Duration >= 72*time.Hour {
		s.Retentions = []Retention{{At: w.Duration - 12*time.Hour, Keep: 36 * time.Hour}}
	}
	return s
}

// Validate reports a spec that cannot be run.
func (s Spec) Validate() error {
	bad := func(format string, args ...any) error { return fmt.Errorf("runner: spec: "+format, args...) }
	switch {
	case s.Version != SpecVersion:
		return bad("version %d, this build runs %d", s.Version, SpecVersion)
	case s.BatchSize < 1:
		return bad("BatchSize must be at least 1")
	case s.Hot < 1 || s.Median < 1:
		return bad("Hot and Median must be at least 1")
	case s.CacheBytes < 0 || s.CacheFraction < 0 || (s.CacheBytes > 0) == (s.CacheFraction > 0):
		return bad("exactly one of CacheBytes and CacheFraction must be set, and neither is negative")
	case s.MinNonEmpty < 0 || s.MinNonEmpty > 1:
		return bad("MinNonEmpty is a share")
	}
	if _, err := workload.New(s.Workload); err != nil {
		return bad("workload: %w", err)
	}
	if s.FullStore && s.Pins == nil {
		return bad("FullStore is for a spec with pins: without them every store is full")
	}
	if s.Pins != nil {
		if err := s.Pins.Verify(); err != nil {
			return bad("%w", err)
		}
		// The hubs are the same entities in every seed and every scenario, so the pins
		// of another stream would be accepted and no longer be its busiest prefixes.
		if d, err := s.ScenarioDigest(); err != nil {
			return err
		} else if d != s.Pins.Source {
			return bad("the pins were chosen from a stream of another scenario (%.12s, not %.12s)", s.Pins.Source, d)
		}
	}
	var prevAt, prevHorizon time.Duration
	for i, r := range s.Retentions {
		switch {
		case r.Keep <= 0 || r.At <= 0 || r.At >= s.Workload.Duration:
			return bad("retention %d: At %s must be inside the period (and before its end) (%s) and Keep above 0", i, r.At, s.Workload.Duration)
		case i > 0 && r.At <= prevAt:
			return bad("retention %d: At %s is not after the previous %s", i, r.At, prevAt)
		case i > 0 && r.At-r.Keep <= prevHorizon:
			return bad("retention %d: the horizon %s does not move forward from %s", i, r.At-r.Keep, prevHorizon)
		case r.At-r.Keep <= 0:
			return bad("retention %d: keeps more than has passed", i)
		}
		prevAt, prevHorizon = r.At, r.At-r.Keep
	}
	return nil
}

// cache is the block cache of a stream with the given payload bytes.
func (s Spec) cache(payload uint64) int64 {
	if s.CacheBytes > 0 {
		return s.CacheBytes
	}
	return cacheBytes(payload, s.CacheFraction)
}

// ScenarioDigest is the SHA-256 of the spec without the window it is a stream of: its
// workload and its options, but not its length, its retentions, its pins or whether
// its stores are full or projected. The windows of one family have the same, and the
// pins are chosen from a stream of it.
func (s Spec) ScenarioDigest() (string, error) {
	s.Pins, s.Retentions, s.Workload.Duration, s.FullStore = nil, nil, 0, false
	return s.Digest()
}

// Projected is whether the stores of the spec are written the projection of the
// stream on its pins rather than the whole stream.
func (s Spec) Projected() bool { return s.Pins != nil && !s.FullStore }

// Digest is the SHA-256 of the spec, as hex.
func (s Spec) Digest() (string, error) {
	b, err := json.Marshal(s)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

// errMismatch marks a result that is not what the plan said it would be.
var errMismatch = errors.New("does not match the plan")
