package main

import (
	"slices"
	"sort"
	"time"

	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

// A sighting is an instant a producer saw an edge: the event time of a record the
// service graph translator made for a point that was evidence of a call. The truth
// is the translator's whole output, which the coalescer afterwards thins: the store
// never holds all of it, and so a silence measured from it is the silence the bound
// is a lower bound of.

// tkey is one producer's view of one edge.
type tkey struct {
	sub  store.Subject
	prod lifecycle.Producer
}

// sight is one sighting. stamp is the store's last sequence number when the request
// that held it had been committed: the token from which the log can know of it.
type sight struct {
	at    time.Time
	stamp uint64
}

// truth keeps the sightings of every edge, by producer, ascending by instant (a
// sighting that arrives late is placed where its instant puts it, after any that
// has the same instant).
type truth struct {
	by map[tkey][]sight
	n  int
}

func newTruth() *truth { return &truth{by: map[tkey][]sight{}} }

// add records a sighting.
func (t *truth) add(k tkey, at time.Time, stamp uint64) {
	s := t.by[k]
	i, _ := slices.BinarySearchFunc(s, at, func(x sight, at time.Time) int {
		if x.at.After(at) {
			return 1
		}
		return -1 // equal instants: the new one goes after
	})
	t.by[k] = slices.Insert(s, i, sight{at: at, stamp: stamp})
	t.n++
}

// lastBy is the latest instant at or before at that the producer saw the edge,
// among the sightings the token knows of.
func (t *truth) lastBy(k tkey, at time.Time, token uint64) (time.Time, bool) {
	s := t.by[k]
	i := sort.Search(len(s), func(i int) bool { return s[i].at.After(at) })
	// Sightings known at the token are all of them, but for those that arrived
	// after it, which are late ones: the walk back passes only those.
	for i--; i >= 0; i-- {
		if s[i].stamp <= token {
			return s[i].at, true
		}
	}
	return time.Time{}, false
}

// edges are the subjects with a sighting, in a fixed order.
func (t *truth) edges() []store.Subject {
	seen := map[store.Subject]bool{}
	var out []store.Subject
	for k := range t.by {
		if !seen[k.sub] {
			seen[k.sub] = true
			out = append(out, k.sub)
		}
	}
	slices.SortFunc(out, func(a, b store.Subject) int {
		if c := store.CompareFingerprints(a.A, b.A); c != 0 {
			return c
		}
		return store.CompareFingerprints(a.B, b.B)
	})
	return out
}

// sightings is how many sightings the edge has, over all its producers.
func (t *truth) sightings(sub store.Subject) int {
	n := 0
	for k, s := range t.by {
		if k.sub == sub {
			n += len(s)
		}
	}
	return n
}
