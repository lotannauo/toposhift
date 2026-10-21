// Package decay derives, from the evidence a range read gives (package
// interval), how long the log proves a dependency has gone unseen, and a
// confidence that halves with that silence.
//
// Everything it returns is derived: a pure function of the evidence it is given,
// the instant it is asked at and a [Policy]. It reads no clock, keeps no state,
// stores no score and has no write path, so a result is never a fact and is
// labelled derived wherever it is shown. It decides no membership: whether an
// element exists is the lifecycle's, from its producers' references and their
// TTLs, and a derived recency neither adds an element to an answer nor removes
// one, orders nothing and cuts nothing.
//
// # What it says
//
// For an element alive at an instant at, [Of] takes the record that decides
// each live producer's reference there ([interval.Evidence]) and gives, for the
// producer seen most recently, the last instant its record says it was seen
// (LastSeen), a lower bound on how long it has not been seen since
// (SilentAtLeast), and Confidence = 2^(-SilentAtLeast / h), which is 1 for a
// bound of zero and halves for every h more of silence.
//
// # The bound
//
// A record stands for the producer's refreshes in a stretch after its last
// instant. Ingest's coalescer re-asserts a run at most once per extension
// interval X (a fraction of the TTL) and, with each run's age bounded by that
// same interval, starts a new run at the first refresh at least X after the
// run's start: so the refreshes a run's record stands for lie in [start,
// start+X), and the run's last instant is within X of the producer's last
// sighting. Taking at minus the last instant as the silence would show every
// steady producer as silent for up to X, a sawtooth of the coalescer's phase and
// not of anything the producer did. Subtracting X from it makes the bound sound:
//
//	SilentAtLeast = max(0, at - LastSeen - X)
//
// is never more than the producer's true silence (the time since its last
// sighting, counting the sightings the coalescer absorbed), and falls short of
// it by at most X. X is subtracted after every record, one with a Through
// included, because the coalescer may absorb refreshes after the record it
// flushed; this keeps one rule, sound for every TTL and for records whose
// Through only the coalescer sets (see the residual below). The last instant is
// the later of the deciding record's and the latest any record of the producer
// that the read returned names ([interval.Evidence].Latest), so a later record
// that replaces an earlier one's attested span from its event time does not hide
// the span. What is shown is therefore a lower bound of what the log proves, and
// a silence shorter than X cannot show at all.
//
// The residual. A record with its own Through (only the Kubernetes writers'
// backdated observations write one outside the coalescer) whose attested span
// reaches past a later record of the same producer, written before the read's
// window, is not seen by the bounded read, which returns the producer's seed and
// the window's records: in that case the bound can exceed the true silence. No
// writer of service dependencies writes a Through of its own, so a recency of a
// dependency is unaffected; a recency of an entity or a placement should not be
// shown until this is closed.
//
// A reference with no TTL (watch mode) promises nothing and ends only by a
// Delete, so the log proves no silence for it and an element it holds has no
// recency: [Of] gives none, rather than a confidence of 1. It gives none also
// when a producer with no TTL is among the live ones, because that producer
// may have seen the element at any time.
//
// # No stored score
//
// The confidence depends on the deciding record alone, so RFC 2439's
// composability, f(f(m, t1), t2) = f(m, t1+t2), holds by construction: waiting
// a further d multiplies the confidence by 2^(-d/h) once the silence exceeds X,
// and nothing is stored, swept or written back. The record is the state.
// Accumulating the sightings (each adds, the sum decays) is not done: it would
// need every record of several half-lives before the instant, which a bounded
// read does not give, and on coalesced data it would count the coalescer's
// records and not the producer's calls.
//
// # The instant
//
// The caller names the instant, and it is the instant the evidence was read at,
// the window's last instant. It is never a clock reading, so the same log read
// at the same token and instant gives the same answer, and a record that arrives
// late moves the answer at a later token and not at an earlier one. A caller that
// reads a window ending after the newest record the token sees should name the
// earlier of the window's last instant and that newest event time: silence past
// the end of the log is not silence the log proves.
//
// # The policy
//
// [Policy] is versioned. Its ExtendFraction is the coalescer's: a change to the
// coalescer's configuration is a change to what the bound means, and the test
// in package sequencer that pins them fails when they differ. The half-life is
// a fraction of X, so it follows the record's own TTL and needs no setting per
// producer. Changing a value is a new [PolicyVersion] and never a migration:
// nothing derived is stored.
package decay

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/lotannauo/toposhift/internal/coalesce"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
)

// PolicyVersion is the version of the derivation policy: the bound, the
// half-life and the rule that picks the producer. A cache of derived results
// keys on it, and a change to any of them raises it.
const PolicyVersion = 1

// ErrPolicy marks an invalid [Policy]. The error [Policy.Validate] returns wraps
// it.
var ErrPolicy = errors.New("decay: invalid policy")

// Policy is what the derivation depends on besides the evidence and the instant.
type Policy struct {
	// Version is the policy's version, [PolicyVersion] for the policy this package
	// ships.
	Version int
	// ExtendFraction is the share of a record's TTL that ingest's coalescer lets a
	// run's refreshes be absorbed over (its ExtendTTLFraction, with each run's age
	// bounded by it): X = ExtendFraction x TTL. It is in (0, 1].
	ExtendFraction float64
	// HalfLifeOfX is the half-life of the confidence, as a share of X: h =
	// HalfLifeOfX x X. It is positive and finite.
	HalfLifeOfX float64
}

// DefaultPolicy is the policy of this version: the coalescer's extension
// fraction of a half, and a half-life of a quarter of X (an eighth of the TTL),
// so a member's confidence, whose silence never passes TTL minus X before it
// lapses, stays within [2^-4, 1].
func DefaultPolicy() Policy {
	return Policy{Version: PolicyVersion, ExtendFraction: 0.5, HalfLifeOfX: 0.25}
}

// Validate reports the first rule broken, wrapping [ErrPolicy].
func (p Policy) Validate() error {
	switch {
	case !(p.ExtendFraction > 0 && p.ExtendFraction <= 1): // also false for NaN
		return fmt.Errorf("%w: ExtendFraction %g is not in (0, 1]", ErrPolicy, p.ExtendFraction)
	case !(p.HalfLifeOfX > 0) || math.IsInf(p.HalfLifeOfX, 0): // also false for NaN
		return fmt.Errorf("%w: HalfLifeOfX %g is not positive and finite", ErrPolicy, p.HalfLifeOfX)
	}
	return nil
}

// Extension is X for a record with this TTL: the longest a run's record may fall
// short of its producer's last sighting, ExtendFraction x ttl, computed as the
// coalescer computes it.
func (p Policy) Extension(ttl time.Duration) time.Duration {
	return coalesce.Config{ExtendTTLFraction: p.ExtendFraction}.ExtensionInterval(ttl)
}

// HalfLife is h for a record with this TTL: HalfLifeOfX x X. It is zero when X
// is, which is only for a TTL too short to have an extension interval.
func (p Policy) HalfLife(ttl time.Duration) time.Duration {
	return time.Duration(p.HalfLifeOfX * float64(p.Extension(ttl)))
}

// Recency is the derived recency of one element at one instant. It is not a
// fact and is never stored.
type Recency struct {
	// Producer is the producer seen most recently: the one with the smallest
	// SilentAtLeast, then the latest LastSeen, then the smallest name.
	Producer lifecycle.Producer
	// LastSeen is the last instant the records read say Producer saw the
	// element (the later of the deciding record's and Evidence.Latest), not after
	// the instant asked at. The producer may have been seen up
	// to X later, by refreshes ingest absorbed.
	LastSeen time.Time
	// SilentAtLeast is a lower bound on how long Producer has not seen the
	// element, at the instant asked at: at less LastSeen less X, and not below
	// zero. It is never more than the true silence.
	SilentAtLeast time.Duration
	// Confidence is 2^(-SilentAtLeast / h), in [0, 1]: 1 when SilentAtLeast is
	// zero, and one half for every h more (it reaches 0 only by underflow, or for
	// a TTL too short to have an extension interval). It is no probability.
	Confidence float64
}

// Of derives the recency of an element at the instant at from last, the evidence
// of an element read at that instant ([interval.Element].Last). It gives false,
// and no recency, when last is empty; when any live producer has no TTL (the log
// proves no silence for it); and when p is not valid.
//
// at is the instant the evidence was read at, never a clock: see "The instant" in
// the package documentation. Evidence whose last instant is after at is clipped
// to it, so a silence is never negative.
func Of(last []interval.Evidence, at time.Time, p Policy) (Recency, bool) {
	if len(last) == 0 || p.Validate() != nil {
		return Recency{}, false
	}
	var best Recency
	have := false
	for _, e := range last {
		if e.TTL <= 0 {
			return Recency{}, false
		}
		seen, _ := store.LastOf(e.EventTime, e.Through)
		if e.Latest.After(seen) {
			seen = e.Latest
		}
		if seen.After(at) {
			seen = at
		}
		silent := max(0, at.Sub(seen)-p.Extension(e.TTL))
		r := Recency{
			Producer: e.Producer, LastSeen: seen.UTC(), SilentAtLeast: silent,
			Confidence: confidence(silent, p.HalfLife(e.TTL)),
		}
		if !have || fresher(r, best) {
			best, have = r, true
		}
	}
	return best, have
}

// fresher says a is seen more recently than b: a smaller bound, then a later
// last instant, then the smaller producer name.
func fresher(a, b Recency) bool {
	switch {
	case a.SilentAtLeast != b.SilentAtLeast:
		return a.SilentAtLeast < b.SilentAtLeast
	case !a.LastSeen.Equal(b.LastSeen):
		return a.LastSeen.After(b.LastSeen)
	}
	return a.Producer < b.Producer
}

// confidence is 2^(-silent/h): 1 for no silence, and for a half-life of zero
// (no extension interval) the limit, 0 for any silence.
func confidence(silent, h time.Duration) float64 {
	switch {
	case silent <= 0:
		return 1
	case h <= 0:
		return 0
	}
	return math.Exp2(-float64(silent) / float64(h))
}
