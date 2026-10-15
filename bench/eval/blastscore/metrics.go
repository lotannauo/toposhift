package blastscore

import (
	"encoding/json"
	"math"
	"slices"

	"github.com/lotannauo/toposhift/internal/identity"
)

// Ks are the cut-offs of the at-k metrics.
var Ks = [3]int{1, 3, 5}

// MaxHop is the deepest hop the by-hop table lists. The walk's default depth is
// 6; a hop beyond MaxHop is scored but not tabulated.
const MaxHop = 6

// Num is a float that is absent from JSON when it is not defined, which a NaN
// or an infinity is not.
type Num float64

// MarshalJSON writes null for a number that is not defined.
func (n Num) MarshalJSON() ([]byte, error) {
	if !finite(float64(n)) {
		return []byte("null"), nil
	}
	return json.Marshal(float64(n))
}

// ratio is a/b, or NaN when b is 0.
func ratio(a, b int) float64 {
	if b == 0 {
		return math.NaN()
	}
	return float64(a) / float64(b)
}

// CaseScore is one predictor's list scored against one case's truth at one
// level. It holds counts only, so any pooled metric is a sum over cases.
type CaseScore struct {
	Case      string    `json:"-"` // the key of the case in results
	Level     Level     `json:"level"`
	Predictor Predictor `json:"predictor"`
	// N is the length of the list, Hits the entities of it in the truth, and T the
	// size of the truth: the mapped truth less the seed, plus what no rule maps
	// or places. Unmappable, Unplaced and Uninstrumented are the parts of T that
	// the list could never contain; Uninstrumented truth has no entity in the
	// facts at all over the window.
	N              int `json:"n"`
	Hits           int `json:"hits"`
	T              int `json:"truth"`
	Unmappable     int `json:"unmappable"`
	Unplaced       int `json:"unplaced"`
	Uninstrumented int `json:"uninstrumented"`
	// Truncated says the case's answer was cut by a cap of any predictor.
	Truncated bool `json:"truncated"`
	// HitsAt[i] is the hits among the first Ks[i] entities of the list.
	HitsAt [len(Ks)]int `json:"-"`
	// HopN[h] is the entities of the list at hop h, and HopHits[h] those of them
	// in the truth, for h from 1 to MaxHop.
	HopN, HopHits [MaxHop + 1]int `json:"-"`
}

// Precision is hits over list length, undefined for an empty list.
func (s CaseScore) Precision() float64 { return ratio(s.Hits, s.N) }

// Recall is hits over truth size, undefined for an empty truth.
func (s CaseScore) Recall() float64 { return ratio(s.Hits, s.T) }

// PrecisionAt is the hits among the first k=Ks[i] entities over min(k, N),
// undefined for an empty list.
func (s CaseScore) PrecisionAt(i int) float64 { return ratio(s.HitsAt[i], min(Ks[i], s.N)) }

// RecallAt is the hits among the first k=Ks[i] entities over the truth size.
func (s CaseScore) RecallAt(i int) float64 { return ratio(s.HitsAt[i], s.T) }

// scoreList scores a ranked list against a truth set. The list is de-duplicated
// keeping the first of each entity, and an entity of skip is dropped from it:
// the seed is never a prediction. unresolved parts are added to the truth size.
func scoreList(list []Prediction, truth map[identity.Fingerprint]struct{}, skip map[identity.Fingerprint]struct{}, t Truth, uninstrumented int) CaseScore {
	s := CaseScore{
		T:          len(truth) + t.Unmappable + t.Unplaced,
		Unmappable: t.Unmappable, Unplaced: t.Unplaced, Uninstrumented: uninstrumented,
	}
	seen := make(map[identity.Fingerprint]struct{}, len(list))
	for _, p := range list {
		if _, ok := skip[p.Entity]; ok {
			continue
		}
		if _, ok := seen[p.Entity]; ok {
			continue
		}
		seen[p.Entity] = struct{}{}
		_, hit := truth[p.Entity]
		s.N++
		inHop := p.Hop >= 1 && p.Hop <= MaxHop
		if inHop {
			s.HopN[p.Hop]++
		}
		if hit {
			s.Hits++
			if inHop {
				s.HopHits[p.Hop]++
			}
			for i, k := range Ks {
				if s.N <= k {
					s.HitsAt[i]++
				}
			}
		}
	}
	return s
}

// truthSet is the truth entities as a set with the seed removed.
func truthSet(t Truth, seed identity.Fingerprint) map[identity.Fingerprint]struct{} {
	out := make(map[identity.Fingerprint]struct{}, len(t.Entities))
	for _, e := range t.Entities {
		if e != seed {
			out[e] = struct{}{}
		}
	}
	return out
}

// Variant picks which cases and which truth a pooled number is over.
type Variant string

const (
	// VariantAll is every scorable case with its whole truth. It is the one the
	// acceptance rule judges.
	VariantAll Variant = "all"
	// VariantNoTruncated leaves out the cases a cap cut.
	VariantNoTruncated Variant = "no_truncated"
	// VariantNoUninstrumented leaves the uninstrumented truth out of the truth
	// size. It is reported and not judged.
	VariantNoUninstrumented Variant = "no_uninstrumented"
)

// Pooled is the metrics of a set of case scores of one predictor.
type Pooled struct {
	// Cases is the number of scores, and Scorable those with a truth to recall.
	Cases    int `json:"cases"`
	Scorable int `json:"scorable"`
	// N, Hits and T are the sums over the cases.
	N    int `json:"n"`
	Hits int `json:"hits"`
	T    int `json:"truth"`
	// Precision, Recall and F1 are micro: from the sums. F1 is 2PR/(P+R) of the
	// micro values.
	Precision Num `json:"precision"`
	Recall    Num `json:"recall"`
	F1        Num `json:"f1"`
	// MacroPrecision and MacroRecall are the means of the defined per-case values.
	MacroPrecision Num `json:"macro_precision"`
	MacroRecall    Num `json:"macro_recall"`
	// AtK holds, for each cut-off, the median and the mean over the cases where
	// the value is defined.
	AtK [len(Ks)]AtK `json:"at_k"`
	// ByHop[i] is the micro precision at hop i+1, with both counts.
	ByHop [MaxHop]HopPrecision `json:"by_hop"`
}

// AtK is the at-k metrics of one cut-off.
type AtK struct {
	K               int `json:"k"`
	PrecisionMedian Num `json:"precision_median"`
	PrecisionMean   Num `json:"precision_mean"`
	RecallMedian    Num `json:"recall_median"`
	RecallMean      Num `json:"recall_mean"`
}

// HopPrecision is the micro precision of the entities named at one hop.
type HopPrecision struct {
	Hop       int `json:"hop"`
	Predicted int `json:"predicted"`
	Hits      int `json:"hits"`
	Precision Num `json:"precision"`
}

// f1 is the harmonic mean of micro precision and recall from the sums. It is
// undefined when either is, and 0 when both are 0.
func f1(hits, n, t int) float64 {
	p, r := ratio(hits, n), ratio(hits, t)
	if math.IsNaN(p) || math.IsNaN(r) {
		return math.NaN()
	}
	if p+r == 0 {
		return 0
	}
	return 2 * p * r / (p + r)
}

// variant applies a variant to one score: whether it stays, and the score to
// pool.
func (v Variant) apply(s CaseScore) (CaseScore, bool) {
	switch v {
	case VariantNoTruncated:
		return s, !s.Truncated
	case VariantNoUninstrumented:
		s.T -= s.Uninstrumented
		s.Uninstrumented = 0
		return s, true
	}
	return s, true
}

// pool pools scores of one predictor under a variant.
func pool(scores []CaseScore, v Variant) Pooled {
	var p Pooled
	var ps, rs []float64
	var pk, rk [len(Ks)][]float64
	for _, s0 := range scores {
		s, ok := v.apply(s0)
		if !ok {
			continue
		}
		p.Cases++
		if s.T > 0 {
			p.Scorable++
		}
		p.N += s.N
		p.Hits += s.Hits
		p.T += s.T
		if x := s.Precision(); !math.IsNaN(x) {
			ps = append(ps, x)
		}
		if x := s.Recall(); !math.IsNaN(x) {
			rs = append(rs, x)
		}
		for i := range Ks {
			if x := s.PrecisionAt(i); !math.IsNaN(x) {
				pk[i] = append(pk[i], x)
			}
			if x := s.RecallAt(i); !math.IsNaN(x) {
				rk[i] = append(rk[i], x)
			}
		}
		for h := 1; h <= MaxHop; h++ {
			p.ByHop[h-1].Predicted += s.HopN[h]
			p.ByHop[h-1].Hits += s.HopHits[h]
		}
	}
	p.Precision, p.Recall = Num(ratio(p.Hits, p.N)), Num(ratio(p.Hits, p.T))
	p.F1 = Num(f1(p.Hits, p.N, p.T))
	p.MacroPrecision, p.MacroRecall = Num(mean(ps)), Num(mean(rs))
	for i, k := range Ks {
		p.AtK[i] = AtK{
			K:               k,
			PrecisionMedian: Num(median(pk[i])), PrecisionMean: Num(mean(pk[i])),
			RecallMedian: Num(median(rk[i])), RecallMean: Num(mean(rk[i])),
		}
	}
	for h := 1; h <= MaxHop; h++ {
		hp := &p.ByHop[h-1]
		hp.Hop = h
		hp.Precision = Num(ratio(hp.Hits, hp.Predicted))
	}
	return p
}

func mean(xs []float64) float64 {
	if len(xs) == 0 {
		return math.NaN()
	}
	var s float64
	for _, x := range xs {
		s += x
	}
	return s / float64(len(xs))
}

func median(xs []float64) float64 {
	if len(xs) == 0 {
		return math.NaN()
	}
	ys := slices.Clone(xs)
	slices.Sort(ys)
	if n := len(ys); n%2 == 1 {
		return ys[n/2]
	}
	n := len(ys)
	return (ys[n/2-1] + ys[n/2]) / 2
}

// lift is the micro precision over the base rate's, both over the same cases.
func lift(hits, n, baseHits, baseN int) float64 {
	return ratio(hits, n) / ratio(baseHits, baseN)
}
