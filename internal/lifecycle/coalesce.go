package lifecycle

import (
	"reflect"
	"slices"

	"github.com/lotannauo/toposhift/internal/identity"
)

// Coalesce turns every run of redundant refreshes into one assertion, so that
// a heartbeating producer costs one record per run instead of one per beat.
// It does not modify its input, and returns the results in (EventTime, Seq)
// order.
//
// Per producer, an Observe continues the run before it if all of these hold:
// the run is an Observe with the same description; either both have no TTL, or
// both have the same TTL and the run's deadline still reaches this
// assertion's event time, so existence is continuous across the join, and the
// assertion starts after the run's last observation. A
// continuing Observe is folded into the run: the run keeps its first event
// time, its Through becomes the last observation, and its Seq is the greatest
// of its members. Every Delete, every change of description, every change of
// TTL and every gap longer than the TTL ends a run.
//
// Merging beats into one assertion, rather than dropping the redundant ones,
// is what makes a run cheap: each refresh extends the deadline, so none can be
// dropped without leaving a gap, but Through lets one assertion carry them all.
//
// Folding the result gives the same Existence, Boots, AliveAt and
// DescribeAt as folding the input, and a clone collision in one is a collision
// in the other. Coalesce is idempotent. What is lost is that a refresh inside
// a run is no longer visible on its own to a query pinned to an earlier
// snapshot token, so the as-known-at view of liveness has the granularity of
// the run's versions.
//
// Coalesce does not validate; [Fold] does.
func Coalesce(as []Assertion) []Assertion {
	byProducer := make(map[Producer][]Assertion)
	for _, a := range as {
		byProducer[a.Producer] = append(byProducer[a.Producer], a)
	}

	out := make([]Assertion, 0, len(as))
	for _, list := range byProducer {
		slices.SortFunc(list, before)
		var runs []Assertion
		for _, a := range overwritten(list) {
			if n := len(runs); n > 0 && continues(runs[n-1], a) {
				runs[n-1] = extend(runs[n-1], a)
			} else {
				runs = append(runs, a)
			}
		}
		out = append(out, runs...)
	}
	slices.SortFunc(out, before)
	return out
}

// overwritten drops the assertions that a later one by the same producer at
// the same event time replaces. list must be in (EventTime, Seq) order. They
// have no effect on any answer, which is what makes dropping them safe.
func overwritten(list []Assertion) []Assertion {
	out := make([]Assertion, 0, len(list))
	for _, a := range list {
		if n := len(out); n > 0 && out[n-1].EventTime.Equal(a.EventTime) {
			out[n-1] = a
		} else {
			out = append(out, a)
		}
	}
	return out
}

// continues reports whether the Observe a repeats the run before it without
// changing any answer.
func continues(run, a Assertion) bool {
	if run.Kind != Observe || a.Kind != Observe || !sameAttrs(run.Attrs, a.Attrs) {
		return false
	}
	// An assertion that starts inside the run's span replaces the run's claim
	// from that instant on (it may even shorten it), so it is not a
	// continuation of it.
	if !a.EventTime.After(run.last()) {
		return false
	}
	if run.TTL == 0 || a.TTL == 0 {
		return run.TTL == a.TTL // watch mode: nothing lapses, so nothing is lost
	}
	return run.TTL == a.TTL && !run.deadline().Before(a.EventTime)
}

// extend folds a into run, which it continues.
func extend(run, a Assertion) Assertion {
	if end := a.last(); end.After(run.last()) {
		run.Through = end
	}
	if a.Seq > run.Seq {
		run.Seq = a.Seq
	}
	return run
}

// sameAttrs reports whether two descriptions hold the same attributes with
// deeply equal values, in any order.
func sameAttrs(a, b []identity.Attr) bool {
	if len(a) != len(b) {
		return false
	}
	for _, x := range a {
		i := slices.IndexFunc(b, func(y identity.Attr) bool { return y.Key == x.Key })
		if i < 0 || !reflect.DeepEqual(x.Value, b[i].Value) {
			return false
		}
	}
	return true
}
