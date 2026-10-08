package runner

import (
	"math/bits"
	"time"
)

// Timing is how long the writes of a build took, as the engine saw them. It is
// informational, like every timing taken off a CI runner: it says where the cost is
// (the commit of a batch, a retention, the first batch after one) on the machine the
// build ran on, never what to choose, and a result is a timing from CI hardware.
type Timing struct {
	// Writes is how long each batch took to write, and AfterRetention how long the
	// first batch after each retention took.
	Writes         Histogram
	AfterRetention []int64 // nanoseconds, one per retention that a batch followed
	// Retains is how long each retention took, in nanoseconds, in order.
	Retains []int64
	// PostRetention is, for each retention in order, how long each of the first batches
	// written after it took, in nanoseconds: at most the build's post_retention_batches
	// (Describe) of them, fewer when the stream or the next retention came first, none when
	// no batch followed. AfterRetention is the first of each.
	PostRetention [][]int64
	// RetainPhases is, for each retention in order, how its time was spent, when the engine
	// says so: the rewrite up to its last commit, the flush and the wait for the database
	// to be at rest, and whether that wait reached its deadline.
	RetainPhases []RetainPhase
}

// RetainPhase is how one retention spent its time, in nanoseconds.
type RetainPhase struct {
	Work, Flush, Settle int64
	DeadlineHit         bool
}

// Histogram counts durations in buckets that are exact below 8 ns and, above, eight to
// each doubling (a duration d is in a bucket at most d/8 wide), so that a quantile is
// within an eighth of the duration it stands for.
type Histogram struct {
	Count   int64
	TotalNs int64
	MaxNs   int64
	Buckets [histogramBuckets]int64
}

const histogramBuckets = 8 + 8*(48-3) // up to 2^48 ns, about 78 hours: a longer duration is counted in the last bucket and reported as its bound

// bucketOf is the bucket a duration is counted in.
func bucketOf(ns int64) int {
	if ns < 8 {
		return int(ns)
	}
	k := bits.Len64(uint64(ns)) // at least 4
	sub := int(ns>>(k-4)) & 7   // the three bits after the leading one
	return min(8+(k-4)*8+sub, histogramBuckets-1)
}

// boundOf is the upper bound of a bucket, in nanoseconds: its duration for the exact
// ones, and the end of its range for the others.
func boundOf(b int) int64 {
	if b < 8 {
		return int64(b)
	}
	k, sub := (b-8)/8+4, (b-8)%8
	return int64(8+sub+1) << (k - 4)
}

// Add counts one duration.
func (h *Histogram) Add(d time.Duration) {
	ns := max(int64(d), 0)
	h.Count++
	h.TotalNs += ns
	h.MaxNs = max(h.MaxNs, ns)
	h.Buckets[bucketOf(ns)]++
}

// Quantile is the upper bound, in nanoseconds, of the bucket the q-quantile falls in
// (within an eighth above the duration, and exact below 8 ns), or 0 for no durations.
func (h Histogram) Quantile(q float64) int64 {
	if h.Count == 0 {
		return 0
	}
	want := int64(float64(h.Count)*q + 0.999999999)
	want = min(max(want, 1), h.Count)
	var seen int64
	for b, n := range h.Buckets {
		if seen += n; seen >= want {
			return boundOf(b)
		}
	}
	return h.MaxNs
}
