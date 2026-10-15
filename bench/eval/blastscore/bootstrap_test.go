package blastscore

import (
	"math"
	"slices"
	"testing"
)

func meanOf(xs []float64) func(idx []int) []float64 {
	return func(idx []int) []float64 {
		var s float64
		for _, i := range idx {
			s += xs[i]
		}
		return []float64{s / float64(len(idx))}
	}
}

// The same seed gives the same draws, and so the same intervals; a different
// seed gives different draws. An unseeded draw would fail the first.
func TestBootstrapIsDeterministic(t *testing.T) {
	xs := []float64{0.2, 0.9, 0.4, 0.7, 0.1, 1, 0.5, 0.6}
	a := bootstrap(len(xs), 2000, BootstrapSeed, meanOf(xs))
	b := bootstrap(len(xs), 2000, BootstrapSeed, meanOf(xs))
	if a[0] != b[0] {
		t.Errorf("two runs differ: %+v and %+v", a[0], b[0])
	}
	if c := bootstrap(len(xs), 2000, BootstrapSeed+1, meanOf(xs)); c[0] == a[0] {
		t.Error("another seed gave the same interval")
	}

	r1, r2 := newResampler(BootstrapSeed), newResampler(BootstrapSeed)
	i1, i2 := make([]int, 25), make([]int, 25)
	r1.next(i1)
	r2.next(i2)
	if !slices.Equal(i1, i2) {
		t.Error("two resamplers with one seed draw differently")
	}
	// The first draws of the frozen seed over 25 cases, pinned so that a change
	// in the generator or in how an index is taken from it is seen.
	if want := []int{9, 16, 17, 21, 7, 10, 21, 4}; !slices.Equal(i1[:8], want) {
		t.Errorf("first draws %v, want %v", i1[:8], want)
	}
	for _, i := range i1 {
		if i < 0 || i >= 25 {
			t.Errorf("index %d out of range", i)
		}
	}
}

// The interval of a mean contains the sample mean here, and is ordered.
func TestBootstrapIntervalContainsThePoint(t *testing.T) {
	xs := []float64{0.2, 0.9, 0.4, 0.7, 0.1, 1, 0.5, 0.6, 0.8, 0.3}
	iv := bootstrap(len(xs), Resamples, BootstrapSeed, meanOf(xs))[0]
	point := meanOf(xs)([]int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})[0]
	if !(float64(iv.Lower) <= point && point <= float64(iv.Upper)) || iv.Lower >= iv.Upper {
		t.Errorf("interval [%v, %v] does not hold the point %v", iv.Lower, iv.Upper, point)
	}
	if iv.Defined != Resamples {
		t.Errorf("defined in %d resamples, want %d", iv.Defined, Resamples)
	}
}

// Percentile ranks: of 10,000 sorted values the 2.5 % and 97.5 % ones are
// those at ranks 250 and 9749.
func TestPercentileInterval(t *testing.T) {
	xs := make([]float64, 10000)
	for i := range xs {
		xs[len(xs)-1-i] = float64(i) // unsorted on purpose
	}
	iv := percentileInterval(xs)
	if iv.Lower != 250 || iv.Upper != 9749 {
		t.Errorf("interval [%v, %v], want [250, 9749]", iv.Lower, iv.Upper)
	}
	if iv := percentileInterval([]float64{5}); iv.Lower != 5 || iv.Upper != 5 {
		t.Errorf("one value: [%v, %v]", iv.Lower, iv.Upper)
	}
	if iv := percentileInterval(nil); !math.IsNaN(float64(iv.Lower)) {
		t.Error("an interval of nothing is defined")
	}
}

// Every statistic of a resample is over the same draw: two statistics that are
// the same function of the cases have a difference of exactly zero in every
// resample, so an interval of exactly [0, 0]. Independent draws would not.
func TestBootstrapStatisticsShareTheirResamples(t *testing.T) {
	xs := []float64{0.2, 0.9, 0.4, 0.7, 0.1, 1, 0.5, 0.6}
	m := meanOf(xs)
	ivs := bootstrap(len(xs), 500, BootstrapSeed, func(idx []int) []float64 {
		a, b := m(idx)[0], m(idx)[0]
		return []float64{a, b, a - b}
	})
	if ivs[2].Lower != 0 || ivs[2].Upper != 0 || ivs[0] != ivs[1] {
		t.Errorf("paired difference [%v, %v]; the two intervals %+v and %+v", ivs[2].Lower, ivs[2].Upper, ivs[0], ivs[1])
	}
}

// Statistics that are undefined in some resamples are left out of those, per
// statistic.
func TestBootstrapLeavesUndefinedValuesOut(t *testing.T) {
	ivs := bootstrap(4, 100, 1, func(idx []int) []float64 {
		if idx[0]%2 == 0 {
			return []float64{math.NaN(), 1}
		}
		return []float64{1, 1}
	})
	if ivs[0].Defined >= 100 || ivs[0].Defined == 0 || ivs[1].Defined != 100 {
		t.Errorf("defined: %d and %d", ivs[0].Defined, ivs[1].Defined)
	}
	if bootstrap(0, 100, 1, nil) != nil {
		t.Error("a bootstrap of no cases is not nil")
	}
}
