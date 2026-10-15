package blastscore

import (
	"math"
	"math/bits"
	"math/rand/v2"
	"slices"
)

// Interval is a percentile interval of a bootstrapped statistic.
type Interval struct {
	Lower Num `json:"lower"`
	Upper Num `json:"upper"`
	// Defined is the number of resamples in which the statistic was defined; the
	// interval is over those.
	Defined int `json:"defined"`
}

// resampler draws case indices for the bootstrap. Its draws depend on the seed
// and on n alone, so two statistics bootstrapped over the same n cases use the
// same resamples.
type resampler struct{ rng *rand.PCG }

func newResampler(seed uint64) *resampler { return &resampler{rng: rand.NewPCG(seed, seed)} }

// next fills idx with len(idx) cases drawn with replacement. The index is the
// high word of a 64 x 64 bit product, which is uniform to within n/2^64 and is
// fixed by the generator's stream, not by a library's choice of algorithm.
func (r *resampler) next(idx []int) {
	n := uint64(len(idx))
	for i := range idx {
		hi, _ := bits.Mul64(r.rng.Uint64(), n)
		idx[i] = int(hi)
	}
}

// bootstrap draws resamples of n cases with replacement and evaluates stat on
// each; stat returns a vector of statistics, every one of them over the same
// resample. That is what pairs them: a difference between two predictors, and
// the predictors' own intervals, come from the same cases in the same draws.
// It returns an interval for each statistic. resamples and seed are
// Resamples and BootstrapSeed outside tests.
func bootstrap(n, resamples int, seed uint64, stat func(idx []int) []float64) []Interval {
	if n == 0 || resamples <= 0 {
		return nil
	}
	r := newResampler(seed)
	idx := make([]int, n)
	var vals [][]float64
	for b := 0; b < resamples; b++ {
		r.next(idx)
		v := stat(idx)
		if vals == nil {
			vals = make([][]float64, len(v))
		}
		for j, x := range v {
			if !math.IsNaN(x) && !math.IsInf(x, 0) {
				vals[j] = append(vals[j], x)
			}
		}
	}
	out := make([]Interval, len(vals))
	for j, xs := range vals {
		out[j] = percentileInterval(xs)
	}
	return out
}

// percentileInterval is the 95 % percentile interval of xs: the values at
// ranks floor(m/40) and ceil(39m/40) - 1 of the m sorted ones (2.5 % and 97.5 %,
// in integers so that no rounding moves a rank).
func percentileInterval(xs []float64) Interval {
	m := len(xs)
	if m == 0 {
		return Interval{Lower: Num(math.NaN()), Upper: Num(math.NaN())}
	}
	ys := slices.Clone(xs)
	slices.Sort(ys)
	lo := m / 40
	hi := (39*m+39)/40 - 1
	lo, hi = min(max(lo, 0), m-1), min(max(hi, 0), m-1)
	return Interval{Lower: Num(ys[lo]), Upper: Num(ys[hi]), Defined: m}
}
