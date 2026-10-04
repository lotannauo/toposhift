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

// Verdict is how the counters of the rules put two candidates in order in one cell.
type Verdict struct {
	Cell string
	// A and B are the candidates, A before B in name order.
	A, B string
	// BetterA and BetterB are the counters that put the candidate first, i.e. the
	// counters on which it costs less beyond the tolerance and floor.
	BetterA, BetterB []string
}

// Mixed is whether counters put the cell in opposite directions.
func (v Verdict) Mixed() bool { return len(v.BetterA) > 0 && len(v.BetterB) > 0 }

// Ordered is whether counters put the cell in one direction.
func (v Verdict) Ordered() bool { return (len(v.BetterA) > 0) != (len(v.BetterB) > 0) }

// Verdicts compares every pair of candidates in every cell, in the order of the
// cells in the plan and of the names of the candidates.
func Verdicts(plan *Plan, cs []*Candidate, r Rules) []Verdict {
	names := make([]string, len(cs))
	for i, c := range cs {
		names[i] = c.Results.Candidate
	}
	means := map[string]map[string][]float64{}
	for _, counter := range r.OrderCounters {
		means[counter] = CellMeans(plan, cs, counter)
	}
	var cells []string
	seen := map[string]bool{}
	for _, q := range plan.Queries {
		if key := CellKey(q); !seen[key] {
			seen[key] = true
			cells = append(cells, key)
		}
	}
	var out []Verdict
	for _, cell := range cells {
		for i := range cs {
			for j := i + 1; j < len(cs); j++ {
				v := Verdict{Cell: cell, A: names[i], B: names[j]}
				for _, counter := range r.OrderCounters {
					m := means[counter][cell]
					if m[i] < 0 || m[j] < 0 {
						continue
					}
					switch r.Orders(counter, m[i], m[j]) {
					case -1:
						v.BetterA = append(v.BetterA, counter)
					case +1:
						v.BetterB = append(v.BetterB, counter)
					}
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
		fmt.Fprintf(h, "%s|%s|%s|%s|%s\n", v.Cell, v.A, v.B, strings.Join(v.BetterA, ","), strings.Join(v.BetterB, ","))
	}
	return hex.EncodeToString(h.Sum(nil))
}
