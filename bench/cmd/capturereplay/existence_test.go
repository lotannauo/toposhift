package main

import (
	"math"
	"testing"
	"time"
)

func TestClassifyPair(t *testing.T) {
	first := t0
	last := t0.Add(10 * time.Minute)
	at := func(d time.Duration, from time.Time) time.Time { return from.Add(d) }
	margin := 60 * time.Second
	for _, c := range []struct {
		name           string
		t              time.Time
		present, alive bool
		want           verdict
	}{
		{"present and alive", t0.Add(5 * time.Minute), true, true, vAgree},
		{"absent and not alive", t0.Add(20 * time.Minute), false, false, vAgree},
		{"present and not alive", t0.Add(5 * time.Minute), true, false, vFalseAbsent},
		{"absent and alive", t0.Add(20 * time.Minute), false, true, vGhost},
		// The margin: 60 seconds either side of the first and the last presence, inclusive.
		{"at the first presence", first, true, false, vSkipped},
		{"30 s after the first presence", at(30*time.Second, first), true, false, vSkipped},
		{"60 s after the first presence", at(60*time.Second, first), true, false, vSkipped},
		{"61 s after the first presence", at(61*time.Second, first), true, false, vFalseAbsent},
		{"30 s before the first presence", at(-30*time.Second, first), false, true, vSkipped},
		{"61 s before the first presence", at(-61*time.Second, first), false, true, vGhost},
		{"61 s before the last presence", at(-61*time.Second, last), true, false, vFalseAbsent},
		{"60 s before the last presence", at(-60*time.Second, last), true, false, vSkipped},
		{"30 s after the last presence", at(30*time.Second, last), false, true, vSkipped},
		{"60 s after the last presence", at(60*time.Second, last), false, true, vSkipped},
		{"61 s after the last presence", at(61*time.Second, last), false, true, vGhost},
	} {
		if got := classifyPair(c.present, c.alive, c.t, first, last, margin); got != c.want {
			t.Errorf("%s: %d, want %d", c.name, got, c.want)
		}
	}
}

// A small series: two pods over 12 samples 30 seconds apart. "old" is listed from the
// start to the end and is absent from the store from the 4th to the 7th sample; "gone" is
// listed to the 3rd sample and alive in the store to the 9th, and another pod, "young", is
// listed from the 8th sample on and not alive until the 11th.
func smallSeries() (series, [][]bool) {
	n := 12
	times := make([]time.Time, n)
	for i := range times {
		times[i] = t0.Add(time.Duration(i) * 30 * time.Second)
	}
	mk := func(from, to int) []bool {
		b := make([]bool, n)
		for i := from; i <= to; i++ {
			b[i] = true
		}
		return b
	}
	sr := series{
		Subject: "pod", samples: samples{Times: times, Unavail: make([]bool, n)},
		UIDs: []string{"old", "gone", "young"}, Names: []string{"old", "gone", "young"},
		Present: [][]bool{mk(0, 11), mk(0, 3), mk(8, 11)},
		First:   []int{0, 0, 8}, Last: []int{11, 3, 11},
	}
	alive := [][]bool{
		{true, true, true, true, false, false, false, false, true, true, true, true},
		mk(0, 9),
		mk(11, 11),
	}
	return sr, alive
}

func TestAnalyzeSumsPairsPerWindow(t *testing.T) {
	sr, alive := smallSeries()
	// One window, from the 4th sample to the 9th (exclusive), which started at the 5th
	// sample's time less 10 s; "young" is listed from the 8th sample on, so it was
	// created inside the window.
	ws := []window{{Name: "w", Start: t0.Add(100 * time.Second), End: t0.Add(270 * time.Second)}}
	ex := analyze(sr, alive, ws, 60*time.Second)

	w := ex.window("w")
	// Samples 4..8 (times 120 s..240 s) are in the window.
	//  old:   samples 4..7 are false absent (120..210 s): 4 pairs; sample 8 agrees.
	//  gone:  last presence 90 s; samples 4 and 5 (120 s, 150 s) are within 60 s: skipped;
	//         samples 6..8 are ghosts (180, 210, 240 s).
	//  young: first presence 240 s; samples 4 and 5 (120 s, 150 s) agree (absent and not
	//         alive); samples 6, 7 and 8 (180 s, 210 s, 240 s) are within 60 s of its first
	//         presence: skipped.
	if w.FalseAbsent != 4 || w.Ghost != 3 {
		t.Errorf("window: false absent %d, ghost %d; want 4 and 3", w.FalseAbsent, w.Ghost)
	}
	if w.Skipped != 5 { // gone 4 and 5, young 6, 7 and 8
		t.Errorf("window: skipped %d, want 5", w.Skipped)
	}
	if math.Abs(w.AbsentMin-2.0) > 1e-9 || math.Abs(w.LongestAbsentMin-2.0) > 1e-9 || w.AbsentPods != 1 {
		t.Errorf("window: absent %.2f min, longest %.2f, %d pods; want 2.0 (4 samples of 30 s), 2.0 and 1", w.AbsentMin, w.LongestAbsentMin, w.AbsentPods)
	}
	if math.Abs(w.GhostMin-1.5) > 1e-9 || math.Abs(w.LongestGhostMin-1.5) > 1e-9 || w.GhostPods != 1 {
		t.Errorf("window: ghost %.2f min, longest %.2f; want 1.5", w.GhostMin, w.LongestGhostMin)
	}
	// Outside the window.
	st := ex.window(windowSteady)
	//  old: samples 0..3 agree (sample 0 is within the margin of its first presence, as are
	//  1 and 2 (30 s, 60 s): only sample 3 is judged), 9..11 agree except 10 and 11, which are
	//  within the margin of the last presence (the 11th: 330 s; 10th: 300 s); sample 9 agrees.
	if st.FalseAbsent != 0 {
		t.Errorf("steady state false absent %d, want 0", st.FalseAbsent)
	}
	//  gone: samples 9 (270 s) is alive and absent from the truth: a ghost in the steady state.
	//  young: sample 11 is alive and listed, within the margin of its last presence.
	if st.Ghost != 1 {
		t.Errorf("steady state ghosts %d, want 1 (gone, at the 9th sample)", st.Ghost)
	}
	// The verdicts are kept for the other analyses.
	if ex.Verdicts[0][5] != vFalseAbsent || ex.Verdicts[1][7] != vGhost || ex.Verdicts[2][0] != vAgree {
		t.Errorf("verdicts %v %v %v", ex.Verdicts[0][5], ex.Verdicts[1][7], ex.Verdicts[2][0])
	}
	tot := ex.total()
	if tot.FalseAbsent != 4 || tot.Ghost != 4 {
		t.Errorf("total false absent %d, ghost %d", tot.FalseAbsent, tot.Ghost)
	}
}

func TestAnalyzeSeparatesPodsCreatedInsideAWindow(t *testing.T) {
	sr, alive := smallSeries()
	// "young" is listed from sample 8 and alive only at 11; with no margin its pairs at
	// samples 9 and 10 are false absent (8 is its first presence and 11 its last).
	ws := []window{{Name: "w", Start: t0.Add(60 * time.Second), End: t0.Add(400 * time.Second)}}
	// With a margin of zero nothing is skipped but the exact first and last samples.
	ex := analyze(sr, alive, ws, 0)
	w := ex.window("w")
	if w.AbsentNewPods != 1 || math.Abs(w.AbsentNewMin-1.0) > 1e-9 {
		t.Errorf("pods created in the window: %d pods, %.2f min; want 1 pod, 1.0 min (samples 9 and 10)", w.AbsentNewPods, w.AbsentNewMin)
	}
	if w.AbsentPods != 1 { // old
		t.Errorf("older pods absent: %d", w.AbsentPods)
	}
}

func TestGhostLifetime(t *testing.T) {
	times := make([]time.Time, 8)
	for i := range times {
		times[i] = t0.Add(time.Duration(i) * 30 * time.Second)
	}
	f, tr := false, true
	un := make([]bool, 8)
	// Listed to sample 1; alive in the store through sample 4; not alive from sample 5.
	life, lastAlive, censored, ok := ghostLifetime(times, un, []bool{tr, tr, tr, tr, tr, f, f, f}, 1)
	if !ok || censored || life != 3*30*time.Second || !lastAlive.Equal(times[4]) {
		t.Errorf("life %v, last alive %v, censored %v, ok %v; want 90 s from sample 2 to sample 5, last alive at sample 4", life, lastAlive, censored, ok)
	}
	// Gone from the store at once.
	life, lastAlive, censored, ok = ghostLifetime(times, un, []bool{tr, tr, f, f, f, f, f, f}, 1)
	if !ok || censored || life != 0 || !lastAlive.IsZero() {
		t.Errorf("life %v, last alive %v", life, lastAlive)
	}
	// Alive to the end: censored at the last sample.
	life, lastAlive, censored, ok = ghostLifetime(times, un, []bool{tr, tr, tr, tr, tr, tr, tr, tr}, 1)
	if !ok || !censored || life != 5*30*time.Second || !lastAlive.Equal(times[7]) {
		t.Errorf("life %v, last alive %v, censored %v", life, lastAlive, censored)
	}
	// An unavailable sample says nothing.
	un[3] = true
	life, _, _, _ = ghostLifetime(times, un, []bool{tr, tr, tr, f, tr, f, f, f}, 1)
	if life != 3*30*time.Second {
		t.Errorf("life %v with an unavailable sample, want 90 s", life)
	}
	// Listed to the last sample: it did not leave.
	if _, _, _, ok := ghostLifetime(times, un, make([]bool, 8), 7); ok {
		t.Error("a pod listed to the last sample has a ghost lifetime")
	}
}

func TestSummarize(t *testing.T) {
	s := summarize([]float64{3, 1, 2, 10}, 1)
	if s.N != 4 || s.Censored != 1 || s.MinMin != 1 || s.MaxMin != 10 || s.MedianMin != 2.5 {
		t.Errorf("%+v", s)
	}
	if s := summarize(nil, 0); s.N != 0 || s.MaxMin != 0 {
		t.Errorf("%+v", s)
	}
}
