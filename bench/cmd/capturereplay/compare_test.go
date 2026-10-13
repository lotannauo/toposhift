package main

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/store"
)

func fpOf(t *testing.T, r *identity.Resolver, typ catalog.EntityType, key catalog.AttributeKey, v string) identity.Fingerprint {
	t.Helper()
	fp, err := fingerprintOf(r, typ, key, v)
	if err != nil {
		t.Fatal(err)
	}
	return fp
}

// Pod b leaves the truth after the first sample; the store still has it for two more.
func TestGhostsCountsLostDeletesAndLifetimes(t *testing.T) {
	pods, wl := loadPods(t), loadWorkload(t)
	mem, other := twoMemStores(t)
	p, err := newPass("A", false, mem, other, memLookup(other))
	if err != nil {
		t.Fatal(err)
	}
	fpB := fpOf(t, p.resolver, catalog.K8sPod, catalog.K8sPodUID, uidB)
	pa := &podAnswers{Alive: [][]bool{
		{true, true, true, true, true},   // pod a: listed and alive
		{true, true, true, false, false}, // pod b: listed at the first sample only
	}}
	ws := wl.windows()

	// Its objects delete was written and the cluster's was lost; its last assertion was at
	// the first sample.
	p.deletes[fpB] = &deleteInfo{Mask: 1 << srcObjects}
	p.lastAssert[fpB] = pods.Times[0]
	gr, err := ghosts(p, pods, pa, ws, wl)
	if err != nil {
		t.Fatal(err)
	}
	var s5 *ghostWindow
	for _, g := range gr.Windows {
		if g.Window == "S5" {
			s5 = g
		}
	}
	if s5 == nil || s5.Disappeared != 1 || s5.ObjectsDeleted != 1 || s5.ClusterDeleted != 0 || s5.LostObjects != 0 || s5.LostCluster != 1 || s5.NoDelete != 0 || s5.BothDeleted != 0 {
		t.Errorf("S5: %+v", s5)
	}
	if gr.OverBound.Count != 0 {
		t.Errorf("a ghost 30 s old is over the bound: %v", gr.OverBound)
	}

	// With no delete from either producer: its lifetime is counted from the first sample
	// without it (30 s) to the first sample at which the store does not have it (90 s).
	delete(p.deletes, fpB)
	gr, _ = ghosts(p, pods, pa, ws, wl)
	for _, g := range gr.Windows {
		if g.Window == "S5" {
			s5 = g
		}
	}
	if s5.NoDelete != 1 || s5.NoDeleteLife.N != 1 || math.Abs(s5.NoDeleteLife.MaxMin-1.0) > 1e-9 || s5.LostObjects != 1 || s5.LostCluster != 1 {
		t.Errorf("S5 with no delete: %+v", s5)
	}

	// A ghost that outlives the last assertion by more than the TTL and a minute.
	p.lastAssert[fpB] = pods.Times[0].Add(-time.Hour)
	gr, _ = ghosts(p, pods, pa, ws, wl)
	if gr.OverBound.Count != 1 {
		t.Errorf("over bound: %+v, want the one ghost alive an hour after its last assertion", gr.OverBound)
	}

	// The pods the log names but the truth never listed are reported as not found.
	var names []string
	for _, n := range gr.Named {
		names = append(names, n.Window+":"+n.Name)
	}
	if len(gr.Named) != 2+1+3 || !strings.Contains(strings.Join(names, ","), "S5:default/web-1") || !strings.Contains(strings.Join(names, ","), "API restart 3:default/web-6") {
		t.Errorf("named: %v", names)
	}
}

func TestPredictionsUseTheMeasuredU(t *testing.T) {
	pods, wl := loadPods(t), loadWorkload(t)
	ws := wl.windows()
	mkStats := func(maxRun1, minRun2, maxRun2, minRun3 string) []*runStats {
		at := func(s string) time.Time {
			v, err := time.Parse(time.RFC3339, "2026-10-10T"+s+":00Z")
			if err != nil {
				t.Fatal(err)
			}
			return v
		}
		r1, r2, r3 := &runStats{N: 1}, &runStats{N: 2}, &runStats{N: 3}
		r1.PodPulls.add(at("11:00"))
		r1.PodPulls.add(at(maxRun1))
		r2.PodPulls.add(at(minRun2))
		r2.PodPulls.add(at(maxRun2))
		r3.PodPulls.add(at(minRun3))
		return []*runStats{r1, r2, r3}
	}
	// Run 1 stopped at 12:00 and its last pull was at 11:56; run 2's first was at 12:05.
	// Run 2 stopped at 12:40, its last pull at 12:35, and run 3's first was at 12:52.
	stats := mkStats("11:56", "12:05", "12:35", "12:52")
	sr, alive := smallSeries()
	ex := analyze(sr, alive, ws, 60*time.Second)
	p := passForTest(t)
	got := predictions(p, pods, ex, wl, ws, stats)
	if len(got) != 2 {
		t.Fatalf("%d predictions, want 2", len(got))
	}
	s5, early := got[0], got[1]
	if s5.Window != "S5" || math.Abs(s5.UMin-4) > 1e-9 || math.Abs(s5.OutageMin-5) > 1e-9 || s5.RecomputedMin != 0 {
		t.Errorf("S5: %+v; want u 4, outage 5, nothing absent (the pull of 11:56 is good until 12:11)", s5)
	}
	if early.Window != "S12-early" || math.Abs(early.UMin-5) > 1e-9 || math.Abs(early.OutageMin-12) > 1e-9 || math.Abs(early.RecomputedMin-2) > 1e-9 {
		t.Errorf("S12-early: %+v; want u 5, outage 12, absent 2 (good until 12:50, back at 12:52)", early)
	}
	if early.Stated != "0" || early.Within30s {
		t.Errorf("S12-early: %+v; no window of that name was measured, so the longest span is 0, which is 2 minutes from the prediction", early)
	}
}

func TestCadenceTakesIntervalsWithinRuns(t *testing.T) {
	at := func(s int) time.Time { return t0.Add(time.Duration(s) * time.Second) }
	run1 := map[string][]time.Time{"n1": {at(0), at(300), at(600)}, "n2": {at(10), at(400)}}
	// The next run begins hours later; the gap between the runs is not an interval.
	run2 := map[string][]time.Time{"n1": {at(36000), at(36310)}}
	c := cadenceOf([]map[string][]time.Time{run1, run2}, func(uid string) string { return "node-" + uid })
	if c.Intervals != 4 {
		t.Errorf("%d intervals, want 4 (300 and 300 for n1, 390 for n2, 310 for n1 in the second run)", c.Intervals)
	}
	if c.MedianSeconds != 305 {
		t.Errorf("median %.0f, want 305, the middle of 300 300 310 390", c.MedianSeconds)
	}
	if got := c.PerNode["node-n1 (n1)"]; got != 300 {
		t.Errorf("n1 median %.0f, want 300", got)
	}
	if got := c.PerNode["node-n2 (n2)"]; got != 390 {
		t.Errorf("n2 median %.0f, want 390", got)
	}
}

func TestContainerChecksCompareContainersOfSharedPods(t *testing.T) {
	mem, other := twoMemStores(t)
	p, err := newPass("A", false, mem, other, memLookup(other))
	if err != nil {
		t.Fatal(err)
	}
	pod := func(u string) identity.Fingerprint { return fpOf(t, p.resolver, catalog.K8sPod, catalog.K8sPodUID, u) }
	ctr := func(u string) identity.Fingerprint {
		return fpOf(t, p.resolver, catalog.Container, catalog.ContainerID, u)
	}
	p1, p2 := pod("p1"), pod("p2")
	c1, c2, c3 := ctr("c1"), ctr("c2"), ctr("c3")
	set := func(s source, pods []identity.Fingerprint, ctrs map[identity.Fingerprint]identity.Fingerprint) {
		for _, x := range pods {
			p.podsBy[s][x] = struct{}{}
		}
		for c, x := range ctrs {
			p.ctrsBy[s][c] = struct{}{}
			p.ctrPod[s][c] = x
		}
	}
	// Both write pod 1; only the objects translator writes pod 2. c2 is of pod 1 and only
	// the objects translator names it; c3 is of pod 2, which the cluster does not write,
	// so its absence from the cluster's is no disagreement.
	set(srcObjects, []identity.Fingerprint{p1, p2}, map[identity.Fingerprint]identity.Fingerprint{c1: p1, c2: p1, c3: p2})
	set(srcCluster, []identity.Fingerprint{p1}, map[identity.Fingerprint]identity.Fingerprint{c1: p1})
	got := containerChecks(p)
	if got.Both != 1 || got.OnlyObjects != 2 || got.OnlyCluster != 0 {
		t.Errorf("whole sets: %+v", got)
	}
	if got.SharedPods != 1 || got.SharedBoth != 1 || got.SharedOnlyObjects != 1 || got.SharedOnlyCluster != 0 || len(got.Examples) != 1 {
		t.Errorf("shared pods: %+v", got)
	}
	pr := &passReport{Name: "A", Containers: got, Edges: &edgeReport{}, Ghosts: &ghostReport{}}
	if b := blocking(pr); len(b) != 1 || !strings.Contains(b[0], "container fingerprints") {
		t.Errorf("blocking: %v", b)
	}
	pr.Containers.SharedOnlyObjects = 0
	if b := blocking(pr); len(b) != 0 {
		t.Errorf("blocking with matching containers: %v", b)
	}
}

func TestBlockingNamesEachDecidedRule(t *testing.T) {
	pr := &passReport{Name: "B", Edges: &edgeReport{}, Ghosts: &ghostReport{}}
	if got := blocking(pr); len(got) != 0 {
		t.Fatalf("an empty pass is blocking: %v", got)
	}
	pr.Edges.AfterPodGone.add("x")
	pr.Ghosts.OverBound.add("y")
	pr.Properties.Lapsed.add("z")
	pr.Containers.SharedOnlyCluster = 1
	got := blocking(pr)
	if len(got) != 4 {
		t.Fatalf("%d blocking lines, want 4: %v", len(got), got)
	}
	for i, want := range []string{"edge-after-pod", "ghosts outlived", "container fingerprints", "lapse before"} {
		if !strings.Contains(got[i], want) {
			t.Errorf("line %d: %q lacks %q", i, got[i], want)
		}
	}
}

func TestDiffPassesCountsDifferingPairs(t *testing.T) {
	pods := loadPods(t)
	a := &passReport{Name: "A", podWin: []string{"x", "x", "x", "y", "y"}, podAlive: [][]bool{{true, true, true, true, true}, {true, false, false, false, false}}}
	b := &passReport{Name: "B", podWin: a.podWin, podAlive: [][]bool{{true, true, true, false, true}, {true, true, false, false, false}}}
	a.Runs = []passRunReport{{N: 1, Written: 5}}
	b.Runs = []passRunReport{{N: 1, Written: 7}}
	d := diffPasses(a, b, pods)
	// Sample 2 is unavailable and not compared: 2 pods x 4 samples.
	if d.PairsCompared != 8 || d.PairsDiffer != 2 || d.AliveOnlyInA != 1 || d.AliveOnlyInB != 1 || d.ByWindow["y"] != 1 || d.ByWindow["x"] != 1 {
		t.Errorf("%+v", d)
	}
	if len(d.WrittenByRun) != 1 || d.WrittenByRun[0].A != 5 || d.WrittenByRun[0].B != 7 {
		t.Errorf("%+v", d.WrittenByRun)
	}
}

func passForTest(t *testing.T) *pass {
	t.Helper()
	mem, other := twoMemStores(t)
	p, err := newPass("A", false, mem, other, memLookup(other))
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// A pod's own deadline in the store, not the last pull, says when it lapses: the
// coalescer absorbs refreshes, so the store's copy can be up to a whole extension
// interval older than the last pull.
func TestPredictionsAlsoUseTheStoresDeadline(t *testing.T) {
	pods, wl := loadPods(t), loadWorkload(t)
	ws := wl.windows()
	at := func(s string) time.Time {
		v, err := time.Parse(time.RFC3339, "2026-10-10T"+s+"Z")
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	// Run 1 stopped at 12:00:00 and its last pod pull was at 11:56; run 2's first pod pull
	// was at 12:00:45, so pod a, listed at 12:00:00 and after, lived through the outage.
	r1, r2, r3 := &runStats{N: 1}, &runStats{N: 2}, &runStats{N: 3}
	r1.PodPulls.add(at("11:56:00"))
	r2.PodPulls.add(at("12:00:45"))
	r3.PodPulls.add(at("12:52:00"))
	p := startedPass(t)
	fpA := fpOf(t, p.resolver, catalog.K8sPod, catalog.K8sPodUID, uidA)
	// The store's last recorded refresh of pod a was an extension of its run, keyed at 11:30
	// and asserted through 11:46, ten minutes before the last pull: its copy lapses at 12:01,
	// the Through plus the TTL, not the 11:30 it is keyed at plus the TTL.
	p.noteDeadlines([]store.Record{observeEntity(fpA, at("11:30:00"), time.Time{}), observeEntity(fpA, at("11:30:00"), at("11:46:00"))})
	p.snapshot()
	if got, want := p.Runs[0].Deadlines[fpA], at("11:46:00").Add(entityTTL); !got.Equal(want) {
		t.Fatalf("deadline in the snapshot %s, want %s", got, want)
	}
	alive := [][]bool{{true, true, true, false, true}, {false, false, false, false, false}}
	ex := analyze(podSeries(pods), alive, ws, 0)
	got := predictions(p, pods, ex, wl, ws, []*runStats{r1, r2, r3})
	if len(got) == 0 || got[0].Window != "S5" {
		t.Fatalf("predictions: %+v", got)
	}
	s5 := got[0]
	if s5.PodsAtStop != 1 || s5.NoDeadline != 0 {
		t.Fatalf("S5: %+v; want pod a alone alive through the outage, with a deadline", s5)
	}
	// Plain: last pull 11:56 + 15 min = 12:11, after the next pull at 12:00:45: nothing.
	if s5.RecomputedMin != 0 {
		t.Errorf("plain prediction %.2f, want 0", s5.RecomputedMin)
	}
	// Store's copy: 11:46 + 15 min = 12:01, 15 s after the pull: not absent before 12:00:45.
	if s5.CoalescerLongestMin != 0 || s5.CoalescerPodMin != 0 {
		t.Errorf("coalescer-aware prediction %.2f, want 0 for a copy good until 12:01", s5.CoalescerLongestMin)
	}
	// An earlier extension, through 11:40: the copy lapses at 11:55 and the pod is absent
	// from then to 12:00:45. Keyed at 11:30 as well, the plain TTL from the key would say
	// 11:45 and 15.75 minutes.
	p.Runs[0].Deadlines[fpA] = at("11:40:00").Add(entityTTL)
	got = predictions(p, pods, ex, wl, ws, []*runStats{r1, r2, r3})
	if c := got[0].CoalescerLongestMin; math.Abs(c-5.75) > 1e-9 || math.Abs(got[0].CoalescerPodMin-5.75) > 1e-9 {
		t.Errorf("coalescer-aware prediction %.2f, want 5.75", c)
	}
	// Pod a's measured absence in the window is the one sample at 12:01:30 (0.5 min), under
	// the prediction: not flagged. With a deadline after the next pull the prediction is 0 and
	// 0.5 is under 0 + 60 s, still not flagged.
	if got[0].Flagged || got[0].OverCoalescerPods != 0 {
		t.Errorf("flagged: %+v", got[0])
	}
	// The flag itself: a measured absence beyond the prediction plus a minute.
	for _, c := range []struct {
		measured, predicted float64
		want                bool
	}{
		{0, 0, false}, {1, 0, false}, {1.01, 0, true}, {6.5, 5.75, false}, {6.8, 5.75, true}, {3, 5.75, false},
	} {
		if got := overPrediction(c.measured, c.predicted); got != c.want {
			t.Errorf("overPrediction(%v, %v) = %v, want %v", c.measured, c.predicted, got, c.want)
		}
	}
}

// edge-after-pod counts only the pairs outside the margin where the truth no longer lists
// the pod; edge-across-gap counts those where it still does.
func TestEdgeCountsSeparateTheBlockingCaseFromTheInformationalOne(t *testing.T) {
	pods := loadPods(t)
	// Pod a is listed at samples 0, 3 and 4, pod b at sample 0 only.
	alive := [][]bool{{true, false, false, false, true}, {false, false, false, false, false}}
	ex := analyze(podSeries(pods), alive, nil, 60*time.Second)
	pa := &podAnswers{
		Alive: alive,
		Edge:  [][]edgeCode{make([]edgeCode, 5), make([]edgeCode, 5)},
		AnyEdge: [][]bool{
			{false, true, false, true, false}, // a: an edge at sample 1 (not listed) and 3 (listed)
			{true, true, false, true, true},   // b: an edge at 0 (listed), at 1 (30 s after its last listing), at 3 and 4
		},
	}
	er := edges(pods, pa, ex, nil)
	// b at samples 3 and 4 (90 s and 120 s after its listing): blocking. b at sample 1 is
	// within the margin, and b at sample 0 is listed. a at sample 1 is not listed and is
	// 30 s from its first listing: within the margin. a at sample 3 is listed: across a gap.
	if er.AfterPodGone.Count != 2 {
		t.Errorf("edge-after-pod %d, want 2 (pod b at the 4th and 5th samples): %v", er.AfterPodGone.Count, er.AfterPodGone.Examples)
	}
	if er.AcrossGap != 2 {
		t.Errorf("edge-across-gap %d, want 2 (pod b at the 1st sample, pod a at the 4th)", er.AcrossGap)
	}
	pr := &passReport{Name: "A", Edges: er, Ghosts: &ghostReport{}}
	if b := blocking(pr); len(b) != 1 || !strings.Contains(b[0], "edge-after-pod") {
		t.Errorf("blocking: %v", b)
	}
	// Only the informational case and the pairs inside the margin: nothing blocks.
	pa.AnyEdge[1] = []bool{true, true, false, false, false}
	er = edges(pods, pa, ex, nil)
	pr.Edges = er
	if er.AfterPodGone.Count != 0 || er.AcrossGap != 2 || len(blocking(pr)) != 0 {
		t.Errorf("after pod gone %d, across gap %d, blocking %v", er.AfterPodGone.Count, er.AcrossGap, blocking(pr))
	}
}
