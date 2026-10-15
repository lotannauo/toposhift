package mcp

import (
	"strings"
	"time"

	"github.com/lotannauo/toposhift/internal/identity"
)

// The display cap: the most entries of a list an answer shows.
const (
	defaultLimit = 50
	maxLimit     = 500
)

// maxQuarantined is how many quarantined entries an answer lists. They are
// listed before the cap applies to anything else.
const maxQuarantined = 50

// maxAnswerBytes is the hard cap on the JSON of an answer. A larger answer
// drops whole trailing entries, and says how many.
const maxAnswerBytes = 256 << 10

func parseLimit(n int) (int, error) {
	switch {
	case n == 0:
		return defaultLimit, nil
	case n < 0 || n > maxLimit:
		return 0, invalid("The argument \"limit\" must be between 1 and %d; got %d.", maxLimit, n)
	}
	return n, nil
}

// parseTime reads an RFC 3339 time with a zone. There is no "now" and no
// relative time: answers echo absolute instants.
func parseTime(name, v string) (time.Time, error) {
	if v == "" {
		return time.Time{}, invalid("The argument %q is required.", name)
	}
	t, err := time.Parse(time.RFC3339Nano, v)
	if err != nil {
		return time.Time{}, invalid("The argument %q must be an RFC 3339 time with a zone, such as 2026-01-01T10:00:00Z; got %s.", name, quote(v))
	}
	return t.UTC(), nil
}

// parseEntity reads a fingerprint, such as k8s.pod:0123456789abcdef0123456789abcdef,
// and checks that the catalog knows its type.
func (s *Source) parseEntity(name, v string) (identity.Fingerprint, error) {
	if v == "" {
		return identity.Fingerprint{}, invalid("The argument %q is required.", name)
	}
	fp, err := identity.ParseFingerprint(v)
	if err != nil {
		return identity.Fingerprint{}, invalid("The argument %q must be an entity id such as host:0123456789abcdef0123456789abcdef; got %s.", name, quote(v))
	}
	if _, ok := s.cat.Entity(fp.Type()); !ok {
		return identity.Fingerprint{}, invalid("The argument %q names the entity type %s, which is not in the catalog. The types are %s.", name, quote(string(fp.Type())), s.typeList())
	}
	return fp, nil
}

func (s *Source) typeList() string {
	var ts []string
	for e := range s.cat.Entities() {
		ts = append(ts, string(e.Type()))
	}
	return strings.Join(ts, ", ")
}

// windowArgs is the window of a range question: one instant, at, which is the
// window [at, at+1ns), or the window [from, to).
func windowArgs(at, from, to string) (time.Time, time.Time, error) {
	if at != "" {
		if from != "" || to != "" {
			return time.Time{}, time.Time{}, invalid("Give either \"at\", or \"from\" and \"to\", not both.")
		}
		t, err := parseTime("at", at)
		if err != nil {
			return time.Time{}, time.Time{}, err
		}
		return t, t.Add(time.Nanosecond), nil
	}
	if from == "" && to == "" {
		return time.Time{}, time.Time{}, invalid("Give \"at\", or \"from\" and \"to\".")
	}
	return rangeArgs(from, to)
}

// rangeArgs reads the two ends of a range, from before to.
func rangeArgs(from, to string) (time.Time, time.Time, error) {
	f, err := parseTime("from", from)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	t, err := parseTime("to", to)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	if !f.Before(t) {
		return time.Time{}, time.Time{}, invalid("The argument \"from\" (%s) must be before \"to\" (%s).", fmtTime(f), fmtTime(t))
	}
	return f, t, nil
}
