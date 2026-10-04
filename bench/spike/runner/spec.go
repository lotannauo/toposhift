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
//   - [Read] opens the built database in a fresh process with the compactions
//     held still, asks every query twice, and checks every answer against the
//     plan. It records what each query cost in Pebble's own unit.
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
// cannot be equal.
const SpecVersion = 1

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
	// CacheFraction sizes the block cache as a share of the payload bytes of the
	// stream, so every candidate gets the same absolute cache and a bigger stream
	// gets a bigger one.
	CacheFraction float64
	// MinNonEmpty is the least share of each group of queries that must have a
	// non-empty answer: an empty answer agrees between candidates trivially.
	MinNonEmpty float64
}

// DefaultSpec is the spec of a workload: the starting point of the options, and
// not a decision about any of them.
func DefaultSpec(w workload.Config) Spec {
	s := Spec{
		Version: SpecVersion, Workload: w, BatchSize: 1000,
		Hot: 10, Median: 5, CacheFraction: 0.25, MinNonEmpty: 0.25,
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
	case s.CacheFraction <= 0:
		return bad("CacheFraction must be above 0")
	case s.MinNonEmpty < 0 || s.MinNonEmpty > 1:
		return bad("MinNonEmpty is a share")
	}
	if _, err := workload.New(s.Workload); err != nil {
		return bad("workload: %w", err)
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
