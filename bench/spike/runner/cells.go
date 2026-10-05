package runner

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"
)

// A cell is a group of queries at an age, the unit the decision rules compare two
// candidates in. What a candidate's counter comes to in a cell is its mean over the
// queries of the cell, which is what the report prints.

// CellKey names a cell.
func CellKey(q Query) string { return q.Group + " " + q.Age }

// counterValue is the value of a rule's counter for one query. A counter is named
// as the rules name it: a term for each thing it adds up, where a term is one of
// Pebble's iterator statistics under the read ("seeks"), one of a layout's own
// counters ("checkpoint_entries_decoded"), or a counter of the runner's
// ("block_loads"); "a+b" is their sum.
func counterValue(r QueryResult, q Query, expr string) int64 {
	var n int64
	for term := range strings.SplitSeq(expr, "+") {
		switch {
		case hasKey(r.Counters, "read."+string(q.Op)+"."+term):
			n += r.Counters["read."+string(q.Op)+"."+term]
		case hasKey(r.Counters, "read."+term):
			n += r.Counters["read."+term]
		default:
			n += r.Counters[term]
		}
	}
	return n
}

func hasKey(m map[string]int64, k string) bool { _, ok := m[k]; return ok }

// CellMeans returns, for each cell, the mean of the counter over its queries for
// each candidate, in the order of cs. A candidate with no result for a query of
// the cell is left out of the cell's mean.
func CellMeans(plan *Plan, cs []*Candidate, expr string) map[string][]float64 {
	type acc struct{ sum, n int64 }
	sums := map[string][]acc{}
	for i, q := range plan.Queries {
		key := CellKey(q)
		if _, ok := sums[key]; !ok {
			sums[key] = make([]acc, len(cs))
		}
		for j, c := range cs {
			if i < len(c.Results.Queries) {
				sums[key][j].sum += counterValue(c.Results.Queries[i], q, expr)
				sums[key][j].n++
			}
		}
	}
	out := make(map[string][]float64, len(sums))
	for key, accs := range sums {
		means := make([]float64, len(accs))
		for j, a := range accs {
			means[j] = -1
			if a.n > 0 {
				means[j] = float64(a.sum) / float64(a.n)
			}
		}
		out[key] = means
	}
	return out
}

// Orders says which way a counter puts two values in order, under the rules: -1 if
// a is the smaller and the counter orders them, +1 if b is, 0 if it does not. A
// counter orders two values if the larger is above the counter's floor and more
// than OrderTolerance times the smaller.
func (r Rules) Orders(counter string, a, b float64) int {
	hi, lo, smaller := a, b, +1 // a is the larger: b is the smaller
	if b > a {
		hi, lo, smaller = b, a, -1
	}
	if hi <= float64(r.CounterFloors[counter]) || hi <= r.OrderTolerance*lo {
		return 0
	}
	return smaller
}

// Verdict is how the counters of the rules put two candidates in order in one cell,
// judged query by query: a query puts the pair in order one way if some counter
// orders it that way and none the other way, and is mixed if counters order it in
// opposite directions. A cell is ordered one way if at least CellMajority of its
// queries are, and none is mixed or ordered the other way; it is mixed if any query
// is mixed or queries are ordered both ways; otherwise it is tied on counters.
type Verdict struct {
	Cell string
	// A and B are the candidates, A before B in name order.
	A, B string
	// BetterA and BetterB are the counters, over all the queries of the cell, on
	// which the candidate costs less beyond the tolerance and floor.
	BetterA, BetterB []string
	// Queries is how many queries the pair was compared on, AheadA and AheadB how
	// many of them put that candidate first, and MixedQueries how many are mixed.
	Queries, AheadA, AheadB, MixedQueries int
	ordered                               int // +1 or -1 if the cell is ordered, 0 otherwise
}

// Mixed is whether counters put the cell in opposite directions.
func (v Verdict) Mixed() bool { return v.MixedQueries > 0 || (v.AheadA > 0 && v.AheadB > 0) }

// Ordered is whether the counters put the cell in one direction.
func (v Verdict) Ordered() bool { return v.ordered != 0 }

// First is the candidate that costs less in an ordered cell, and "" in one that is
// not ordered.
func (v Verdict) First() string {
	switch v.ordered {
	case -1:
		return v.A
	case +1:
		return v.B
	}
	return ""
}

// Verdicts compares every pair of candidates in every cell, in the order of the
// cells in the plan and of the names of the candidates, whatever order cs is in.
func Verdicts(plan *Plan, cs []*Candidate, r Rules) []Verdict {
	cs = slices.Clone(cs)
	slices.SortFunc(cs, func(a, b *Candidate) int { return strings.Compare(a.Results.Candidate, b.Results.Candidate) })
	var cells []string
	members := map[string][]int{} // the queries of a cell, by index in the plan
	for i, q := range plan.Queries {
		cell := CellKey(q)
		if _, ok := members[cell]; !ok {
			cells = append(cells, cell)
		}
		members[cell] = append(members[cell], i)
	}
	var out []Verdict
	for _, cell := range cells {
		for i := range cs {
			for j := i + 1; j < len(cs); j++ {
				v := Verdict{Cell: cell, A: cs[i].Results.Candidate, B: cs[j].Results.Candidate}
				seenA, seenB := map[string]bool{}, map[string]bool{}
				for _, qi := range members[cell] {
					if qi >= len(cs[i].Results.Queries) || qi >= len(cs[j].Results.Queries) {
						continue
					}
					v.Queries++
					q := plan.Queries[qi]
					var a, b []string
					for _, counter := range r.OrderCounters {
						switch r.Orders(counter, float64(counterValue(cs[i].Results.Queries[qi], q, counter)), float64(counterValue(cs[j].Results.Queries[qi], q, counter))) {
						case -1:
							a = append(a, counter)
							seenA[counter] = true
						case +1:
							b = append(b, counter)
							seenB[counter] = true
						}
					}
					switch {
					case len(a) > 0 && len(b) > 0:
						v.MixedQueries++
					case len(a) > 0:
						v.AheadA++
					case len(b) > 0:
						v.AheadB++
					}
				}
				for _, counter := range r.OrderCounters { // in the order of the rules
					if seenA[counter] {
						v.BetterA = append(v.BetterA, counter)
					}
					if seenB[counter] {
						v.BetterB = append(v.BetterB, counter)
					}
				}
				majority := func(n int) bool { return v.Queries > 0 && float64(n) >= r.CellMajority*float64(v.Queries) }
				switch {
				case v.Mixed():
				case majority(v.AheadA):
					v.ordered = -1
				case majority(v.AheadB):
					v.ordered = +1
				}
				out = append(out, v)
			}
		}
	}
	return out
}

// MixedCells are the verdicts in which counters disagree. They are the cells that
// timing on CI hardware settles, and the list is a function of the counters alone:
// it is printed with a digest, and fixed before any timing is collected.
func MixedCells(vs []Verdict) []Verdict {
	return slices.DeleteFunc(slices.Clone(vs), func(v Verdict) bool { return !v.Mixed() })
}

// MixedDigest is the SHA-256 of the list of mixed cells, as hex.
func MixedDigest(mixed []Verdict) string {
	h := sha256.New()
	for _, v := range mixed {
		fmt.Fprintf(h, "%s|%s|%s|%s|%s|%d|%d|%d|%d\n", v.Cell, v.A, v.B, strings.Join(v.BetterA, ","), strings.Join(v.BetterB, ","),
			v.Queries, v.AheadA, v.AheadB, v.MixedQueries)
	}
	return hex.EncodeToString(h.Sum(nil))
}
