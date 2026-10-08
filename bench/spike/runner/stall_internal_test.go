package runner

import (
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
)

// timingOf is a Timing whose batches were the given durations, with the retentions and
// the batches after each of them as given. The values in these tests are invented.
func timingOf(writes []time.Duration, retains []time.Duration, post [][]time.Duration) Timing {
	var t Timing
	for _, w := range writes {
		t.Writes.Add(w)
	}
	for _, r := range retains {
		t.Retains = append(t.Retains, int64(r))
	}
	if post != nil {
		t.PostRetention = make([][]int64, len(post))
		for i, p := range post {
			t.PostRetention[i] = []int64{}
			for _, d := range p {
				t.PostRetention[i] = append(t.PostRetention[i], int64(d))
			}
		}
	}
	return t
}

// repeat is d, n times.
func repeat(d time.Duration, n int) []time.Duration {
	out := make([]time.Duration, n)
	for i := range out {
		out[i] = d
	}
	return out
}

// G2's value of a build is the largest, over its retentions, of the retention's time
// plus the excess over the build's median commit of each of the first n batches after
// it.
func TestStallOf(t *testing.T) {
	t.Parallel()

	second := time.Second
	// The median of a histogram is a bucket bound, which is not the duration that was
	// added: the expected values are made from it.
	steady := repeat(4*time.Millisecond, 100)
	var probe Timing
	for _, w := range steady {
		probe.Writes.Add(w)
	}
	m := probe.Writes.Quantile(0.5)
	if m < int64(4*time.Millisecond) {
		t.Fatalf("the median is %d, below the durations added", m)
	}

	// A build with a few very slow batches, so that its mean is far above its median.
	skewed := append(repeat(time.Millisecond, 90), repeat(second, 10)...)
	var skew Timing
	for _, w := range skewed {
		skew.Writes.Add(w)
	}
	ms := skew.Writes.Quantile(0.5)
	if mean := skew.Writes.TotalNs / skew.Writes.Count; mean <= 10*ms {
		t.Fatalf("the skewed build's mean %d is not far above its median %d", mean, ms)
	}

	for _, c := range []struct {
		name string
		t    Timing
		n    int
		want int64
		ok   bool
	}{
		{"no batch after the retention", timingOf(steady, []time.Duration{7 * second}, [][]time.Duration{{}}), 100, int64(7 * second), true},
		{"batches below the median never subtract", timingOf(steady, []time.Duration{7 * second},
			[][]time.Duration{repeat(time.Millisecond, 50)}), 100, int64(7 * second), true},
		{"batches at the median add nothing", timingOf(steady, []time.Duration{7 * second},
			[][]time.Duration{{time.Duration(m), time.Duration(m)}}), 100, int64(7 * second), true},
		{"the excess of each batch is added", timingOf(steady, []time.Duration{7 * second},
			[][]time.Duration{{time.Duration(m) + second, time.Duration(m) + 2*second, time.Millisecond}}), 100, int64(10 * second), true},
		{"a slow 101st batch does not count with n = 100", timingOf(steady, []time.Duration{7 * second},
			[][]time.Duration{append(repeat(time.Duration(m), 100), time.Duration(m)+5*second)}), 100, int64(7 * second), true},
		{"a slow 101st batch counts with n = 101", timingOf(steady, []time.Duration{7 * second},
			[][]time.Duration{append(repeat(time.Duration(m), 100), time.Duration(m)+5*second)}), 101, int64(12 * second), true},
		{"a window longer than the batches recorded counts them all", timingOf(steady, []time.Duration{7 * second},
			[][]time.Duration{{time.Duration(m) + second}}), 100, int64(8 * second), true},
		{"the largest retention, not the sum", timingOf(steady, []time.Duration{10 * second, 20 * second, 15 * second},
			[][]time.Duration{{}, {}, {}}), 100, int64(20 * second), true},
		{"the largest is the one the batches slowed, not the longest retention", timingOf(steady, []time.Duration{10 * second, 20 * second},
			[][]time.Duration{{time.Duration(m) + 15*second}, {}}), 100, int64(25 * second), true},
		{"the median, not the mean", timingOf(skewed, []time.Duration{second},
			[][]time.Duration{repeat(50*time.Millisecond, 10)}), 100, int64(second) + 10*(int64(50*time.Millisecond)-ms), true},
		{"no recorded batches", timingOf(steady, []time.Duration{7 * second}, nil), 100, 0, false},
		{"fewer lists than retentions", timingOf(steady, []time.Duration{7 * second, 8 * second}, [][]time.Duration{{}}), 100, 0, false},
		{"more lists than retentions", timingOf(steady, []time.Duration{7 * second}, [][]time.Duration{{}, {}}), 100, 0, false},
		{"no retention", timingOf(steady, nil, nil), 100, 0, false},
		// Round values that stand for the shape of a stall and a settled retention, not
		// for any measurement.
		{"a retention that left the writer slow", timingOf(steady, []time.Duration{5 * second},
			[][]time.Duration{repeat(3*second, 100)}), 100, int64(5*second) + 100*(int64(3*second)-m), true},
		{"a retention that settled", timingOf(steady, []time.Duration{40 * second},
			[][]time.Duration{repeat(20*time.Millisecond, 100)}), 100, int64(40*second) + 100*(int64(20*time.Millisecond)-m), true},
	} {
		got, ok := stallOf(c.t, c.n)
		if ok != c.ok || got != c.want {
			t.Errorf("%s: stallOf = %d, %v; want %d, %v", c.name, got, ok, c.want, c.ok)
		}
	}

	// What the two shapes do to the budget.
	stalled, _ := stallOf(timingOf(steady, []time.Duration{5 * second}, [][]time.Duration{repeat(3*second, 100)}), 100)
	settled, _ := stallOf(timingOf(steady, []time.Duration{40 * second}, [][]time.Duration{repeat(20*time.Millisecond, 100)}), 100)
	budget := DefaultRules().StallBudgetSeconds
	if time.Duration(stalled).Seconds() <= budget {
		t.Errorf("a retention of 5 s followed by 100 batches of 3 s is %v, within %v s", time.Duration(stalled), budget)
	}
	if time.Duration(settled).Seconds() > budget {
		t.Errorf("a retention of 40 s followed by batches of 20 ms is %v, over %v s", time.Duration(settled), budget)
	}
	if n := DefaultRules().PostRetentionBatches; n != 100 {
		t.Errorf("the window of G2 is %d batches, the rules say 100", n)
	}
}

// A value of exactly the budget passes, and a nanosecond more does not.
func TestG2Boundary(t *testing.T) {
	t.Parallel()

	r := DefaultRules()
	budget := time.Duration(r.StallBudgetSeconds * float64(time.Second))
	if _, _, pass := g2Verdict(budget, r); !pass {
		t.Error("a value of exactly the budget does not pass")
	}
	if _, _, pass := g2Verdict(budget+1, r); pass {
		t.Error("a value a nanosecond over the budget passes")
	}
}

// batchSink is an engine that takes a time proportional to the sequence number of a
// batch to write it, so that the batches in a list can be told apart.
type batchSink struct {
	measurable
	seq  uint64
	each time.Duration
}

func (b *batchSink) Write(batch []engine.Record) error {
	time.Sleep(time.Duration(batch[len(batch)-1].Seq) * b.each)
	b.seq = batch[len(batch)-1].Seq
	return nil
}
func (b *batchSink) LastSeq() uint64        { return b.seq }
func (b *batchSink) Retain(time.Time) error { return nil }

// A retention starts its own list of batches and ends the previous one's, and a list
// holds at most the first n batches written after it.
func TestTheBatchesAfterARetentionAreCutAtTheNextRetention(t *testing.T) {
	t.Parallel()

	const each = 3 * time.Millisecond
	var (
		timing Timing
		after  bool
	)
	e := &batchSink{each: each}
	sink := buildSink{e: e, t: &timing, afterRetention: &after, post: &postState{n: 3}}
	// Ten batches of one record, numbered 1 to 10, with a retention after the second and
	// after the fourth.
	for seq := uint64(1); seq <= 10; seq++ {
		if err := sink.Write([]engine.Record{{Seq: seq}}); err != nil {
			t.Fatal(err)
		}
		if seq == 2 || seq == 4 {
			if err := sink.Retain(time.Time{}); err != nil {
				t.Fatal(err)
			}
		}
	}

	if got := len(timing.PostRetention); got != 2 {
		t.Fatalf("%d lists of batches, want one for each of 2 retentions", got)
	}
	for i, c := range []struct{ first, last uint64 }{{3, 4}, {5, 7}} {
		list := timing.PostRetention[i]
		if want := int(c.last - c.first + 1); len(list) != want {
			t.Errorf("retention %d: %d batches, want %d (%d to %d)", i, len(list), want, c.first, c.last)
			continue
		}
		for k, d := range list {
			if seq := c.first + uint64(k); time.Duration(d) < time.Duration(seq)*each {
				t.Errorf("retention %d: batch %d took %v, less than it sleeps", i, seq, time.Duration(d))
			}
		}
		if len(timing.AfterRetention) != 2 || timing.AfterRetention[i] != list[0] {
			t.Errorf("retention %d: the first batch after it is %v in AfterRetention and %v in the list", i, timing.AfterRetention, list[0])
		}
	}
	if timing.Writes.Count != 10 {
		t.Errorf("%d batches timed, want 10", timing.Writes.Count)
	}
	if len(timing.RetainPhases) != 0 {
		t.Errorf("phases were recorded for an engine that says nothing of them: %v", timing.RetainPhases)
	}
}

// A retention with no batch after it has an empty list, and a sink with no state records
// no batches but keeps one list for each retention.
func TestARetentionWithNoBatchAfterItHasAnEmptyList(t *testing.T) {
	t.Parallel()

	var (
		timing Timing
		after  bool
	)
	sink := buildSink{e: &batchSink{}, t: &timing, afterRetention: &after, post: &postState{n: 5}}
	if err := sink.Write([]engine.Record{{Seq: 1}}); err != nil {
		t.Fatal(err)
	}
	if err := sink.Retain(time.Time{}); err != nil {
		t.Fatal(err)
	}
	if len(timing.PostRetention) != 1 || len(timing.PostRetention[0]) != 0 || len(timing.AfterRetention) != 0 {
		t.Errorf("a retention at the end of the stream: %v, %v", timing.PostRetention, timing.AfterRetention)
	}
	if got, ok := stallOf(timing, 5); !ok || got != timing.Retains[0] {
		t.Errorf("stallOf = %d, %v; want the retention alone, %d", got, ok, timing.Retains[0])
	}
}
