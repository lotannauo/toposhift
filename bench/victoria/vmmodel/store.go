package vmmodel

import (
	"errors"
	"fmt"
	"slices"
	"sort"
)

// point is one sample as the store holds it.
type point struct {
	ms    int64
	stale bool
}

type posting struct{ label, value string }

// Store holds samples in memory with the read semantics assumed of a metrics store, so
// that the queries of package vmquery can be evaluated, and checked against the
// reference, without a server. It is a model: nothing in it is a statement about how any
// server stores a byte. Fill it from a [Converter] with [Store.Sink], then [Store.Seal]
// it and read.
type Store struct {
	series  []Series
	points  [][]point
	byIndex map[uint32]int
	posts   map[posting][]int
	sealed  bool
}

// NewStore returns an empty store.
func NewStore() *Store {
	return &Store{byIndex: map[uint32]int{}}
}

// Sink returns the function to give a converter as Options.Emit, bound to the converter
// the samples come from (it names the series).
func (st *Store) Sink(c *Converter) func([]Sample) error {
	return func(ss []Sample) error {
		if st.sealed {
			return errors.New("vmmodel: the store is sealed")
		}
		for _, s := range ss {
			i, ok := st.byIndex[s.Series]
			if !ok {
				i = len(st.series)
				st.byIndex[s.Series] = i
				st.series = append(st.series, c.Series(s.Series))
				st.points = append(st.points, nil)
			}
			st.points[i] = append(st.points[i], point{s.TimestampMs, s.Stale})
		}
		return nil
	}
}

// Seal sorts the samples of each series by time, builds the index of the labels a read
// selects by (src, dst and fp) and ends the writing. Two samples of one series at one
// millisecond are an error: the converter leaves one.
func (st *Store) Seal() error {
	if st.sealed {
		return nil
	}
	st.posts = map[posting][]int{}
	for i, ps := range st.points {
		sort.SliceStable(ps, func(a, b int) bool { return ps[a].ms < ps[b].ms })
		for j := 1; j < len(ps); j++ {
			if ps[j].ms == ps[j-1].ms {
				return fmt.Errorf("vmmodel: series %s has two samples at %d ms", st.series[i], ps[j].ms)
			}
		}
		for _, name := range []string{LabelSrc, LabelDst, LabelFP} {
			if v := st.series[i].Get(name); v != "" {
				k := posting{name, v}
				st.posts[k] = append(st.posts[k], i)
			}
		}
	}
	st.sealed = true
	return nil
}

// Len is the number of series.
func (st *Store) Len() int { return len(st.series) }

// Samples is the number of samples held, markers included.
func (st *Store) Samples() int {
	n := 0
	for _, ps := range st.points {
		n += len(ps)
	}
	return n
}

// Lookup returns the positions of the series whose label has the value, in the order they
// were first written. The store must be sealed.
func (st *Store) Lookup(label, value string) []int {
	return slices.Clone(st.posts[posting{label, value}])
}

// At returns the series at a position.
func (st *Store) At(i int) Series { return st.series[i] }

// Semantics are the assumptions about how a window is read that the model has to make.
type Semantics struct {
	// StaleVisible says a staleness marker is the last sample of a window when it is the
	// last in it, so that a range function over the window reports nothing. The default,
	// false, is the assumption (k) of the package documentation [U]: the function skips
	// markers, and a series ended by a Delete reads as present until its window ends.
	StaleVisible bool
}

// Last returns whether the series has a sample in (after, upTo] (in milliseconds) that a
// window reads: the last one there, a marker being skipped or ending the answer
// according to the semantics. This is assumption (j) [U]: the window excludes its left
// end and includes its right end.
func (st *Store) Last(i int, after, upTo int64, sem Semantics) bool {
	ps := st.points[i]
	j := sort.Search(len(ps), func(k int) bool { return ps[k].ms > upTo }) - 1
	for ; j >= 0 && ps[j].ms > after; j-- {
		if !ps[j].stale {
			return true
		}
		if sem.StaleVisible {
			return false
		}
	}
	return false
}
