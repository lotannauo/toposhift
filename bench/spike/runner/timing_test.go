package runner_test

import (
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/runner"
)

func TestHistogramCountsDurationsWithinAnEighth(t *testing.T) {
	t.Parallel()

	var h runner.Histogram
	if h.Quantile(0.5) != 0 {
		t.Error("a quantile of nothing is not zero")
	}
	for _, d := range []time.Duration{0, 1, 3, 100, 100, 100, 100, 5000, 70_000, -5} {
		h.Add(d)
	}
	if h.Count != 10 || h.TotalNs != 0+1+3+400+5000+70_000 || h.MaxNs != 70_000 {
		t.Errorf("count %d, total %d, max %d", h.Count, h.TotalNs, h.MaxNs)
	}
	// Sorted: 0 0 1 3 100 100 100 100 5000 70000. Below 8 ns a bucket is exact; 100 is in
	// [96, 104), 5000 in [4608, 5120) and 70000 in [65536, 73728).
	for q, want := range map[float64]int64{0.1: 0, 0.2: 0, 0.3: 1, 0.4: 3, 0.5: 104, 0.8: 104, 0.9: 5120, 0.99: 73728, 1: 73728} {
		if got := h.Quantile(q); got != want {
			t.Errorf("quantile %v = %d, want %d", q, got, want)
		}
	}
}

// A quantile is above the duration and within an eighth of it, whatever the size: the
// gates compare quantiles against a factor of 2, and a bucket that doubled could make
// one candidate look three or four times the other's.
func TestAHistogramQuantileIsWithinAnEighthOfTheDuration(t *testing.T) {
	t.Parallel()

	for _, ns := range []int64{1, 7, 8, 9, 15, 16, 17, 99, 1000, 1_100_000, 4_100_000, 1 << 30, 1<<40 + 12345} {
		var h runner.Histogram
		h.Add(time.Duration(ns))
		got := h.Quantile(0.99)
		if got <= ns && ns >= 8 || got < ns || float64(got) > float64(ns)*1.125+1 {
			t.Errorf("%d ns is reported as %d", ns, got)
		}
	}
}
