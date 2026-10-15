package runner

import (
	"strings"
	"testing"
)

// C3 reads the median of a judged chunk's prefixes at the bottom of its histogram
// bucket, which has eight buckets to a doubling: a chunk of 100 prefixes is in the
// bucket from 96, so a store whose chunks have 100 do not meet the criterion, and
// chunks of 104 (the next bucket edge) and of 128 do. This pins the arithmetic that
// the minimum a background chunk keeps for a waiting write (128) relies on.
func TestTheMedianOfChunksOfExactlyTheMinimumIsReadAtTheBottomOfItsBucket(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		prefixes int64
		low      int64
		met      bool
	}{
		{99, 96, false},
		{100, 96, false},
		{103, 96, false},
		{104, 104, true},
		{127, 120, true},
		{128, 128, true},
	} {
		var s sampleSpread
		for range 11 {
			s.add(map[string]int64{"retain.chunk_prefixes": c.prefixes})
		}
		sp := s.spread()["retain.chunk_prefixes"]
		if sp.P50Low != c.low {
			t.Errorf("chunks of %d prefixes: the median is read at %d, want %d", c.prefixes, sp.P50Low, c.low)
		}
		tr := meets()
		tr.JudgedChunks["retain.chunk_prefixes"] = sp
		var none [gateRows]gateRow
		_, parts := retainChunkCriteria(tr, none)
		got := strings.Contains(parts[2], "C3 met")
		if got != c.met {
			t.Errorf("chunks of %d prefixes: C3 %q, want met %v", c.prefixes, parts[2], c.met)
		}
	}
}
