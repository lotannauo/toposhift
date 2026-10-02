package lifecycle

import (
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/lotannauo/toposhift/internal/identity"
)

// Sentinel errors wrapped by the errors this package returns, so callers can
// tell the kind of failure with [errors.Is].
var (
	// ErrInvalid marks an assertion or policy that breaks a rule of the
	// specification.
	ErrInvalid = errors.New("invalid")
	// ErrCloneCollision marks two live boots for one subject. The error is a
	// [*CloneCollisionError].
	ErrCloneCollision = errors.New("clone collision")
)

// Producer names a source of assertions. It must be a stable logical name,
// not a process instance; see the package documentation. The zero value is
// invalid.
type Producer string

// Kind is what an assertion says. The zero value is invalid.
type Kind uint8

const (
	// Observe says the subject exists and gives the producer's complete
	// current description of it.
	Observe Kind = iota + 1
	// Delete says the producer no longer sees the subject.
	Delete
)

func (k Kind) String() string {
	switch k {
	case Observe:
		return "observe"
	case Delete:
		return "delete"
	}
	return fmt.Sprintf("Kind(%d)", uint8(k))
}

// Assertion is one statement by one producer about one subject.
type Assertion struct {
	// Producer made the statement. Required.
	Producer Producer
	// EventTime is when the statement was true. Required.
	EventTime time.Time
	// Seq is the ingest sequence number: it orders assertions with equal
	// event times and is the snapshot token. Unique within a fold.
	Seq uint64
	// Kind says what the statement is. Required.
	Kind Kind
	// TTL is how long an Observe promises to stay true without being
	// repeated. Zero promises nothing (watch mode): only a Delete ends the
	// reference. Must be zero on a Delete, and never negative.
	TTL time.Duration
	// Through, if set, says the producer repeated this Observe continuously,
	// at least once per TTL, from EventTime through this instant, so a run of
	// refreshes is one assertion. It never precedes EventTime, and is zero on a
	// Delete. Zero means a single observation at EventTime. [Coalesce] produces
	// runs, and a store extends one by re-asserting it at the same EventTime
	// with a later Seq and a later Through.
	Through time.Time
	// Attrs is the producer's complete description of the subject. Must be
	// empty on a Delete, and may not repeat a key. Values are retained, not
	// copied.
	Attrs []identity.Attr
}

// last is the latest instant the assertion says the producer observed the
// subject: Through for a run, else EventTime.
func (a Assertion) last() time.Time {
	if a.Through.After(a.EventTime) {
		return a.Through
	}
	return a.EventTime
}

// deadline is when an Observe's reference lapses, if it has a TTL: a promise
// to repeat within TTL is kept until the last observation plus TTL.
func (a Assertion) deadline() time.Time { return a.last().Add(a.TTL) }

// before orders assertions by event time, then by sequence number.
func before(a, b Assertion) int {
	if c := a.EventTime.Compare(b.EventTime); c != 0 {
		return c
	}
	switch {
	case a.Seq < b.Seq:
		return -1
	case a.Seq > b.Seq:
		return 1
	}
	return 0
}

// Visible returns the assertions that a query pinned to snapshot token may
// see: those with Seq at or below it. It returns a new slice and keeps the
// order of the input.
func Visible(as []Assertion, token uint64) []Assertion {
	out := make([]Assertion, 0, len(as))
	for _, a := range as {
		if a.Seq <= token {
			out = append(out, a)
		}
	}
	return out
}

// validate checks every assertion and the set as a whole, reporting all
// violations joined.
func validate(as []Assertion, p Policy) error {
	var errs []error
	seen := make(map[uint64]int, len(as))
	for i, a := range as {
		fail := func(format string, args ...any) {
			errs = append(errs, fmt.Errorf("assertion %d (producer %q, seq %d): %s: %w",
				i, a.Producer, a.Seq, fmt.Sprintf(format, args...), ErrInvalid))
		}
		if a.Producer == "" {
			fail("producer is empty")
		}
		if a.EventTime.IsZero() {
			fail("event time is unset")
		}
		if first, dup := seen[a.Seq]; dup {
			fail("seq already used by assertion %d", first)
		} else {
			seen[a.Seq] = i
		}
		switch a.Kind {
		case Observe:
			if a.TTL < 0 {
				fail("negative TTL %s", a.TTL)
			}
			if k, dup := duplicateKey(a.Attrs); dup {
				fail("attribute %q appears twice", k)
			}
			if !a.Through.IsZero() && a.Through.Before(a.EventTime) {
				fail("through %s precedes the event time", a.Through.Format(time.RFC3339Nano))
			}
		case Delete:
			if a.TTL != 0 {
				fail("a delete cannot carry a TTL")
			}
			if len(a.Attrs) != 0 {
				fail("a delete cannot carry attributes")
			}
			if !a.Through.IsZero() {
				fail("a delete cannot carry a through time")
			}
		default:
			fail("kind %s", a.Kind)
		}
		if p.BootKey != "" && a.Kind == Observe {
			if _, err := bootID(a, p.BootKey); err != nil {
				fail("%v", err)
			}
		}
	}
	return errors.Join(errs...)
}

func duplicateKey(attrs []identity.Attr) (string, bool) {
	for i, a := range attrs {
		for _, b := range attrs[i+1:] {
			if a.Key == b.Key {
				return string(a.Key), true
			}
		}
	}
	return "", false
}

// prepare validates, copies and orders the input: UTC event times, attribute
// slices cloned, sorted by (EventTime, Seq).
func prepare(as []Assertion, p Policy) ([]Assertion, error) {
	if err := validate(as, p); err != nil {
		return nil, err
	}
	out := make([]Assertion, len(as))
	for i, a := range as {
		a.EventTime = a.EventTime.UTC()
		if !a.Through.IsZero() {
			a.Through = a.Through.UTC()
		}
		a.Attrs = slices.Clone(a.Attrs)
		out[i] = a
	}
	slices.SortFunc(out, before)
	return out, nil
}
