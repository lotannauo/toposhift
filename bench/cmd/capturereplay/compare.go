package main

import (
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
)

// slack60 is the allowance on top of a TTL before a ghost counts as having outlived it.
const ghostSlack = 60 * time.Second

// podSeries and nodeSeries show the truth files as series for existence.
func podSeries(pt *podsTruth) series {
	sr := series{Subject: "pod", samples: pt.samples, UIDs: pt.UIDs}
	for _, uid := range pt.UIDs {
		p := pt.Pods[uid]
		sr.Names = append(sr.Names, p.Name)
		sr.Present = append(sr.Present, p.Present)
		sr.First = append(sr.First, p.First)
		sr.Last = append(sr.Last, p.Last)
	}
	return sr
}

func nodeSeries(nt *nodesTruth) series {
	sr := series{Subject: "node", samples: nt.samples, UIDs: nt.UIDs}
	for _, uid := range nt.UIDs {
		n := nt.Nodes[uid]
		sr.Names = append(sr.Names, n.Name)
		sr.Present = append(sr.Present, n.Present)
		sr.First = append(sr.First, n.First)
		sr.Last = append(sr.Last, n.Last)
	}
	return sr
}

// ghostLifetime is how long a UID the truth stopped listing stayed alive in the store.
// last is the index of the last sample that listed it; it is gone from the sample after.
// The lifetime runs from that sample to the first sample at which the store has it not
// alive (unavailable samples say nothing). If it is alive at every sample to the end, the
// lifetime is censored: the time to the last sample. lastAlive is the time of the latest
// sample before the end at which it was alive, and zero if it never was.
func ghostLifetime(times []time.Time, unavail, alive []bool, last int) (life time.Duration, lastAlive time.Time, censored, ok bool) {
	t2 := last + 1
	if t2 >= len(times) {
		return 0, time.Time{}, false, false
	}
	end := times[t2]
	for j := t2; j < len(times); j++ {
		if unavail[j] {
			continue
		}
		if !alive[j] {
			return times[j].Sub(times[t2]), lastAlive, false, true
		}
		lastAlive = times[j]
		end = times[j]
	}
	return end.Sub(times[t2]), lastAlive, true, true
}

// lifetimeSummary is the minutes of a set of lifetimes.
type lifetimeSummary struct {
	N         int     `json:"n"`
	Censored  int     `json:"censored"`
	MinMin    float64 `json:"min_minutes"`
	MedianMin float64 `json:"median_minutes"`
	MaxMin    float64 `json:"max_minutes"`
}

func summarize(mins []float64, censored int) lifetimeSummary {
	ls := lifetimeSummary{N: len(mins), Censored: censored}
	if len(mins) == 0 {
		return ls
	}
	s := slices.Clone(mins)
	sort.Float64s(s)
	ls.MinMin, ls.MaxMin = s[0], s[len(s)-1]
	ls.MedianMin = s[len(s)/2]
	if len(s)%2 == 0 {
		ls.MedianMin = (s[len(s)/2-1] + s[len(s)/2]) / 2
	}
	return ls
}

// ghostWindow is the pods that left the truth in a window, and what the producers did.
type ghostWindow struct {
	Window         string `json:"window"`
	Disappeared    int    `json:"pods_that_left"`
	ObjectsDeleted int    `json:"deleted_by_k8sobjects"`
	ClusterDeleted int    `json:"deleted_by_k8scluster"`
	BothDeleted    int    `json:"deleted_by_both"`
	NoDelete       int    `json:"deleted_by_neither"`
	LostObjects    int    `json:"lost_deletes_k8sobjects"`
	LostCluster    int    `json:"lost_deletes_k8scluster"`
	// NoDeleteLife is the ghost lifetime of the pods neither producer deleted.
	NoDeleteLife lifetimeSummary `json:"ghost_lifetime_of_pods_with_no_delete"`
	// OverBound counts the pods that were alive after their producers' last assertion
	// plus the TTL and 60 seconds.
	OverBound int `json:"ghosts_over_ttl_plus_60s"`

	mins     []float64
	censored int
}

// namedPod is a pod the log names as deleted at a stop or an API restart.
type namedPod struct {
	Window         string    `json:"window"`
	Name           string    `json:"name"`
	UID            string    `json:"uid,omitempty"`
	ObjectsDeleted bool      `json:"deleted_by_k8sobjects"`
	ClusterDeleted bool      `json:"deleted_by_k8scluster"`
	GoneFromTruth  time.Time `json:"first_sample_without_it,omitzero"`
	LastAssertion  time.Time `json:"last_assertion,omitzero"`
	GhostMinutes   float64   `json:"ghost_minutes"`
	Censored       bool      `json:"ghost_still_alive_at_the_end,omitempty"`
	OverBound      bool      `json:"over_ttl_plus_60s"`
}

// ghostReport is the ghost rate of a pass.
type ghostReport struct {
	Windows   []*ghostWindow `json:"windows"`
	Named     []namedPod     `json:"named_pods"`
	OverBound violation      `json:"ghosts_over_ttl_plus_60s"`
}

// ghosts looks at every pod that the truth stops listing before its last sample.
func ghosts(p *pass, pods *podsTruth, pa *podAnswers, ws []window, wl *workload) (*ghostReport, error) {
	gr := &ghostReport{}
	by := map[string]*ghostWindow{}
	get := func(name string) *ghostWindow {
		if g, ok := by[name]; ok {
			return g
		}
		g := &ghostWindow{Window: name}
		by[name] = g
		gr.Windows = append(gr.Windows, g)
		return g
	}
	for _, w := range ws {
		get(w.Name)
	}
	get(windowSteady)
	byUID := map[string]*namedPod{}
	for u, uid := range pods.UIDs {
		pt := pods.Pods[uid]
		life, lastAlive, censored, ok := ghostLifetime(pods.Times, pods.Unavail, pa.Alive[u], pt.Last)
		if !ok {
			continue // listed to the last sample
		}
		fp, err := fingerprintOf(p.resolver, catalog.K8sPod, catalog.K8sPodUID, uid)
		if err != nil {
			return nil, err
		}
		t2 := pods.Times[pt.Last+1]
		g := get(attribute(ws, t2))
		g.Disappeared++
		var mask uint8
		if di := p.deletes[fp]; di != nil {
			mask = di.Mask
		}
		objs, clus := mask&(1<<srcObjects) != 0, mask&(1<<srcCluster) != 0
		switch {
		case objs && clus:
			g.BothDeleted++
		case mask == 0:
			g.NoDelete++
		}
		if objs {
			g.ObjectsDeleted++
		} else {
			g.LostObjects++
		}
		if clus {
			g.ClusterDeleted++
		} else {
			g.LostCluster++
		}
		bound := p.lastAssert[fp].Add(entityTTL + ghostSlack)
		over := !lastAlive.IsZero() && lastAlive.After(bound)
		if over {
			g.OverBound++
			gr.OverBound.add("pod %s (%s) alive at %s, past %s", uid, pt.Name, lastAlive.Format(time.RFC3339), bound.Format(time.RFC3339))
		}
		if mask == 0 {
			g.mins = append(g.mins, life.Minutes())
			if censored {
				g.censored++
			}
		}
		byUID[uid] = &namedPod{
			Name: pt.Name, UID: uid, ObjectsDeleted: objs, ClusterDeleted: clus, GoneFromTruth: t2,
			LastAssertion: p.lastAssert[fp], GhostMinutes: life.Minutes(), Censored: censored, OverBound: over,
		}
	}
	for _, g := range gr.Windows {
		g.NoDeleteLife = summarize(g.mins, g.censored)
	}
	// The pods the log names: of the UIDs that bore a name, the one that left the truth
	// nearest the time of the log line.
	name := func(window, short string, at time.Time) {
		full := "default/" + short
		var pick *namedPod
		for _, uid := range pods.ByName[full] {
			np := byUID[uid]
			if np == nil {
				continue
			}
			if pick == nil || absDur(np.GoneFromTruth.Sub(at)) < absDur(pick.GoneFromTruth.Sub(at)) {
				pick = np
			}
		}
		if pick == nil {
			gr.Named = append(gr.Named, namedPod{Window: window, Name: full})
			return
		}
		np := *pick
		np.Window = window
		gr.Named = append(gr.Named, np)
	}
	for _, s := range wl.Stops {
		for _, n := range s.Pods {
			name(s.Name, n, s.Start)
		}
	}
	for _, a := range wl.API {
		for _, n := range a.Pods {
			name(fmt.Sprintf("API restart %d", a.Ordinal), n, a.Time)
		}
	}
	return gr, nil
}

// edgeWindow counts the edge verdicts of a window.
type edgeWindow struct {
	Window string `json:"window"`
	// Checked: pod listed, beyond the margin, truth names a node that has a UID.
	Checked   int `json:"checked"`
	Exact     int `json:"exact"`
	Missing   int `json:"missing"`
	Wrong     int `json:"wrong_node"`
	WithExtra int `json:"exact_and_a_second_edge"`
	// Unexpected: an edge where the truth names no node.
	Unexpected int `json:"edge_where_truth_names_no_node"`
	// AfterPodGone: pairs outside the margin at which the truth does not list the pod, the
	// store does not have it alive, and an edge of it (scheduled_on, or the part_of edge
	// of a container) is alive. This is the pair the rule edge-after-pod counts.
	AfterPodGone int `json:"edge_after_pod_gone"`
	// AcrossGap: pairs at which the truth lists the pod, the store does not have it alive,
	// and an edge of it is alive. Informational.
	AcrossGap int `json:"edge_across_gap"`
	// OutlivesTruth: pairs beyond the margin at which the truth no longer lists the pod
	// and the store has an edge of it alive.
	OutlivesTruth int `json:"edge_alive_after_pod_left_truth"`
}

// edgeReport is the edges of a pass.
type edgeReport struct {
	Windows []*edgeWindow `json:"windows"`
	// AfterPodGone counts the pairs of the rule edge-after-pod, with examples, and
	// AfterPodGoneLongestMin is the longest unbroken stretch of them, in minutes.
	AfterPodGone           violation `json:"edge_after_pod"`
	AfterPodGoneLongestMin float64   `json:"edge_after_pod_longest_minutes"`
	// AcrossGap counts the pairs of the informational edge-across-gap.
	AcrossGap int `json:"edge_across_gap"`
	// Second are pods that had a second scheduled_on edge in the union.
	Second       violation `json:"second_edge_in_the_union"`
	SecondPods   int       `json:"second_edge_pods"`
	SecondMaxMin float64   `json:"second_edge_longest_minutes"`
}

// edges checks the scheduled_on edges against the nodes of the truth.
func edges(pods *podsTruth, pa *podAnswers, ex *existence, ws []window) *edgeReport {
	er := &edgeReport{}
	by := map[string]*edgeWindow{}
	get := func(name string) *edgeWindow {
		if e, ok := by[name]; ok {
			return e
		}
		e := &edgeWindow{Window: name}
		by[name] = e
		er.Windows = append(er.Windows, e)
		return e
	}
	spacing := pods.spacing()
	secondPods := map[string]struct{}{}
	for u, uid := range pods.UIDs {
		pt := pods.Pods[uid]
		var outRun, secondRun float64
		for s, t := range pods.Times {
			if pods.Unavail[s] {
				outRun, secondRun = 0, 0
				continue
			}
			w := get(attribute(ws, t))
			code := pa.Edge[u][s]
			v := ex.Verdicts[u][s]
			if !pa.Alive[u][s] && pa.AnyEdge[u][s] && pt.Present[s] {
				w.AcrossGap++
				er.AcrossGap++
			}
			if !pa.Alive[u][s] && pa.AnyEdge[u][s] && !pt.Present[s] && v != vSkipped {
				w.AfterPodGone++
				outRun += spacing[s].Minutes()
				er.AfterPodGoneLongestMin = max(er.AfterPodGoneLongestMin, outRun)
				er.AfterPodGone.add("pod %s (%s) at %s", uid, pt.Name, t.Format(time.RFC3339))
			} else {
				outRun = 0
			}
			if code == edgeExactPlus {
				secondRun += spacing[s].Minutes()
			} else {
				secondRun = 0
			}
			if code == edgeExactPlus {
				secondPods[uid] = struct{}{}
				er.SecondMaxMin = max(er.SecondMaxMin, secondRun)
				er.Second.add("pod %s (%s) at %s", uid, pt.Name, t.Format(time.RFC3339))
			}
			if v == vSkipped {
				continue
			}
			if !pt.Present[s] {
				if code != edgeNone && code != edgeMissing {
					w.OutlivesTruth++
				}
				continue
			}
			switch code {
			case edgeNone:
			case edgeUnexpected:
				w.Unexpected++
			case edgeMissing:
				w.Checked++
				w.Missing++
			case edgeExact:
				w.Checked++
				w.Exact++
			case edgeExactPlus:
				w.Checked++
				w.WithExtra++
			case edgeWrong:
				w.Checked++
				w.Wrong++
			}
		}
	}
	er.SecondPods = len(secondPods)
	return er
}

// nodeLine is one node UID against the truth.
type nodeLine struct {
	UID         string    `json:"uid"`
	Name        string    `json:"name"`
	TruthFirst  time.Time `json:"truth_first"`
	TruthLast   time.Time `json:"truth_last"`
	AliveFrom   time.Time `json:"store_alive_from,omitzero"`
	AliveTo     time.Time `json:"store_alive_to,omitzero"`
	FalseAbsent int       `json:"false_absent"`
	Ghost       int       `json:"ghost"`
	AbsentMin   float64   `json:"absent_minutes"`
	GhostMin    float64   `json:"ghost_minutes"`
}

// nodeReport is the nodes against the truth.
type nodeReport struct {
	Nodes []nodeLine `json:"nodes"`
	// Agent2 is the recreation of agent-2: the gap in the truth between the old UID's
	// last sample and the new one's first, and the store's.
	Agent2 *agent2Gap `json:"agent2,omitempty"`
}

type agent2Gap struct {
	OldUID, NewUID string
	TruthGapSec    float64 `json:"truth_gap_seconds"`
	// OverlapSec is how long both UIDs were alive in the store at the same sample times,
	// in seconds (the samples counted by their spacing).
	StoreBothAliveSec float64 `json:"store_both_uids_alive_seconds"`
	// StoreNeitherSec is how long neither was.
	StoreNeitherSec float64 `json:"store_neither_uid_alive_seconds"`
}

func nodes(nt *nodesTruth, na *nodeAnswers, ex *existence, wl *workload) *nodeReport {
	nr := &nodeReport{}
	spacing := nt.spacing()
	idx := map[string]int{}
	for u, uid := range nt.UIDs {
		idx[uid] = u
		n := nt.Nodes[uid]
		line := nodeLine{UID: uid, Name: n.Name, TruthFirst: nt.Times[n.First], TruthLast: nt.Times[n.Last]}
		for s, t := range nt.Times {
			if nt.Unavail[s] {
				continue
			}
			if na.Alive[u][s] {
				if line.AliveFrom.IsZero() {
					line.AliveFrom = t
				}
				line.AliveTo = t
			}
			switch ex.Verdicts[u][s] {
			case vFalseAbsent:
				line.FalseAbsent++
				line.AbsentMin += spacing[s].Minutes()
			case vGhost:
				line.Ghost++
				line.GhostMin += spacing[s].Minutes()
			}
		}
		nr.Nodes = append(nr.Nodes, line)
	}
	oi, ok1 := idx[wl.OldUID]
	ni, ok2 := idx[wl.NewUID]
	if ok1 && ok2 {
		g := &agent2Gap{OldUID: wl.OldUID, NewUID: wl.NewUID}
		g.TruthGapSec = nt.Times[nt.Nodes[wl.NewUID].First].Sub(nt.Times[nt.Nodes[wl.OldUID].Last]).Seconds()
		for s := range nt.Times {
			if nt.Unavail[s] {
				continue
			}
			switch a, b := na.Alive[oi][s], na.Alive[ni][s]; {
			case a && b:
				g.StoreBothAliveSec += spacing[s].Seconds()
			case !a && !b && s >= nt.Nodes[wl.OldUID].First && s <= nt.Nodes[wl.NewUID].Last:
				g.StoreNeitherSec += spacing[s].Seconds()
			}
		}
		nr.Agent2 = g
	}
	return nr
}

// prediction is a stop of the collector against what the expected lapse says.
type prediction struct {
	Window string `json:"window"`
	// Stated is the prediction made before the capture ran.
	Stated string `json:"stated_prediction"`
	// UMin is the minutes from the last pod pull before the stop to the stop, and
	// OutageMin the minutes from the stop to the next run's first pod pull.
	UMin      float64 `json:"u_minutes"`
	OutageMin float64 `json:"outage_minutes"`
	// RecomputedMin is, for a pod that lived through the whole outage, the minutes from
	// its last pull plus the TTL to the next run's first pull, at least zero.
	RecomputedMin    float64 `json:"recomputed_absent_minutes_per_pod"`
	PodsAtStop       int     `json:"pods_alive_through_the_outage"`
	RecomputedPodMin float64 `json:"recomputed_pod_minutes"`
	// CoalescerLongestMin and CoalescerPodMin are the second prediction: from the
	// instant the store's copy of each pod lapses (the last refresh the coalescer
	// recorded, plus the TTL) to the next run's first pull. The coalescer absorbs
	// refreshes for up to one extension interval, 7.5 minutes at a TTL of 15, so this is
	// later than the plain prediction by up to that much.
	CoalescerLongestMin float64 `json:"coalescer_aware_longest_minutes_per_pod"`
	CoalescerPodMin     float64 `json:"coalescer_aware_pod_minutes"`
	NoDeadline          int     `json:"pods_with_no_record_in_the_store"`
	MeasuredPodMin      float64 `json:"measured_pod_minutes"`
	MeasuredLongest     float64 `json:"measured_longest_minutes"`
	// DeltaSec is the longest measured span less the recomputed one, in seconds.
	DeltaSec  float64 `json:"longest_less_recomputed_seconds"`
	Within30s bool    `json:"within_30s"`
	// OverCoalescerPods counts the pods whose measured absence in the window exceeds their
	// coalescer-aware prediction plus 60 seconds; Flagged says there are any. Predictions
	// are informational.
	OverCoalescerPods int  `json:"pods_over_coalescer_aware_plus_60s"`
	Flagged           bool `json:"flagged_over_coalescer_aware_plus_60s"`
	// NewPodMin and NewPodLongest are for the pods created during the outage, which the
	// collector could not have seen.
	NewPodMin     float64 `json:"measured_pod_minutes_of_pods_created_in_the_window"`
	NewPodLongest float64 `json:"measured_longest_of_pods_created_in_the_window"`
}

var statedPredictions = map[string]string{
	"S5": "0", "S12-early": "0", "S12-late": "about 2 min", "S20": "5-10 min",
}

// overPrediction says whether a measured absence exceeds a prediction by more than the
// 60-second margin.
func overPrediction(measuredMin, predictedMin float64) bool {
	return measuredMin > predictedMin+time.Minute.Minutes()
}

// predictions measures each stop window against the expected lapse, with u from the pull
// lines, and against the lapse of the store's own copy, from the pass's deadlines.
func predictions(p *pass, pods *podsTruth, ex *existence, wl *workload, ws []window, stats []*runStats) []prediction {
	var out []prediction
	spacing := pods.spacing()
	for _, s := range wl.Stops {
		var w window
		for _, x := range ws {
			if x.Name == s.Name {
				w = x
			}
		}
		k := w.StopOf
		if k < 1 || k >= len(stats) {
			continue
		}
		last, next := stats[k-1].PodPulls.Max, stats[k].PodPulls.Min
		if last.IsZero() || next.IsZero() {
			continue
		}
		stop := s.Start
		pr := prediction{Window: s.Name, Stated: statedPredictions[s.Name]}
		pr.UMin = stop.Sub(last).Minutes()
		pr.OutageMin = next.Sub(stop).Minutes()
		pr.RecomputedMin = max(0, next.Sub(last.Add(entityTTL)).Minutes())
		var deadlines map[identity.Fingerprint]time.Time
		if k-1 < len(p.Runs) {
			deadlines = p.Runs[k-1].Deadlines
		}
		// Pods in the cluster when the collector stopped and still there when it came back.
		before, after := pods.sampleAtOrBefore(stop), pods.sampleAtOrBefore(next)+1
		win := ex.window(s.Name)
		for u, uid := range pods.UIDs {
			pt := pods.Pods[uid]
			if before < 0 || pt.First > before || pt.Last < after {
				continue
			}
			pr.PodsAtStop++
			fp, err := fingerprintOf(p.resolver, catalog.K8sPod, catalog.K8sPodUID, uid)
			dl, ok := deadlines[fp]
			if err != nil || !ok {
				pr.NoDeadline++
				continue
			}
			predicted := max(0, next.Sub(dl).Minutes())
			pr.CoalescerPodMin += predicted
			pr.CoalescerLongestMin = max(pr.CoalescerLongestMin, predicted)
			if win == nil {
				continue
			}
			var measured float64
			for sIdx, t := range pods.Times {
				if !t.Before(w.Start) && t.Before(w.End) && ex.Verdicts[u][sIdx] == vFalseAbsent {
					measured += spacing[sIdx].Minutes()
				}
			}
			if overPrediction(measured, predicted) {
				pr.OverCoalescerPods++
			}
		}
		pr.Flagged = pr.OverCoalescerPods > 0
		pr.RecomputedPodMin = pr.RecomputedMin * float64(pr.PodsAtStop)
		if win != nil {
			pr.MeasuredPodMin, pr.MeasuredLongest = win.AbsentMin, win.LongestAbsentMin
			pr.NewPodMin, pr.NewPodLongest = win.AbsentNewMin, win.LongestAbsentNewMin
		}
		pr.DeltaSec = (pr.MeasuredLongest - pr.RecomputedMin) * 60
		pr.Within30s = absFloat(pr.DeltaSec) <= 30
		out = append(out, pr)
	}
	return out
}

func absFloat(f float64) float64 {
	if f < 0 {
		return -f
	}
	return f
}

// subjectSets compares the entities the two translators wrote.
type subjectSets struct {
	Kind        string `json:"kind"`
	Both        int    `json:"in_both"`
	OnlyObjects int    `json:"only_k8sobjects"`
	OnlyCluster int    `json:"only_k8scluster"`
}

func compareSets(a, b map[identity.Fingerprint]struct{}) (both, onlyA, onlyB int) {
	for k := range a {
		if _, ok := b[k]; ok {
			both++
		} else {
			onlyA++
		}
	}
	for k := range b {
		if _, ok := a[k]; !ok {
			onlyB++
		}
	}
	return both, onlyA, onlyB
}

// containerCheck is the check that both translators name the same containers.
type containerCheck struct {
	// Whole sets.
	ObjectsContainers int `json:"k8sobjects_containers"`
	ClusterContainers int `json:"k8scluster_containers"`
	Both              int `json:"in_both"`
	OnlyObjects       int `json:"only_k8sobjects"`
	OnlyCluster       int `json:"only_k8scluster"`
	// Of the containers whose pod both translators wrote.
	SharedPods        int `json:"pods_written_by_both"`
	SharedBoth        int `json:"shared_pods_containers_in_both"`
	SharedOnlyObjects int `json:"shared_pods_containers_only_k8sobjects"`
	SharedOnlyCluster int `json:"shared_pods_containers_only_k8scluster"`
	// Examples of a container of a shared pod that only one translator wrote.
	Examples []string `json:"examples,omitempty"`
}

func containerChecks(p *pass) containerCheck {
	var c containerCheck
	c.ObjectsContainers, c.ClusterContainers = len(p.ctrsBy[srcObjects]), len(p.ctrsBy[srcCluster])
	c.Both, c.OnlyObjects, c.OnlyCluster = compareSets(p.ctrsBy[srcObjects], p.ctrsBy[srcCluster])
	shared := map[identity.Fingerprint]struct{}{}
	for fp := range p.podsBy[srcObjects] {
		if _, ok := p.podsBy[srcCluster][fp]; ok {
			shared[fp] = struct{}{}
		}
	}
	c.SharedPods = len(shared)
	// A container's pod is the pod of its part_of edge, from the translator that wrote it.
	restricted := func(src source) map[identity.Fingerprint]struct{} {
		out := map[identity.Fingerprint]struct{}{}
		for ctr := range p.ctrsBy[src] {
			if _, ok := shared[p.ctrPod[src][ctr]]; ok {
				out[ctr] = struct{}{}
			}
		}
		return out
	}
	ro, rc := restricted(srcObjects), restricted(srcCluster)
	c.SharedBoth, c.SharedOnlyObjects, c.SharedOnlyCluster = compareSets(ro, rc)
	for ctr := range ro {
		if _, ok := rc[ctr]; !ok && len(c.Examples) < maxExamples {
			c.Examples = append(c.Examples, "only k8sobjects: "+ctr.String())
		}
	}
	for ctr := range rc {
		if _, ok := ro[ctr]; !ok && len(c.Examples) < 2*maxExamples {
			c.Examples = append(c.Examples, "only k8scluster: "+ctr.String())
		}
	}
	slices.Sort(c.Examples)
	return c
}

// median of a list of durations in seconds, or 0 for none.
func medianSeconds(d []float64) float64 {
	if len(d) == 0 {
		return 0
	}
	s := slices.Clone(d)
	sort.Float64s(s)
	if len(s)%2 == 1 {
		return s[len(s)/2]
	}
	return (s[len(s)/2-1] + s[len(s)/2]) / 2
}

// cadence is the median interval between the times of each key, in seconds, counting
// only intervals inside one run, and the median over all intervals.
type cadence struct {
	MedianSeconds float64            `json:"median_seconds_all_intervals"`
	Intervals     int                `json:"intervals"`
	PerNode       map[string]float64 `json:"median_seconds_per_node"`
}

func cadenceOf(runs []map[string][]time.Time, name func(uid string) string) cadence {
	c := cadence{PerNode: map[string]float64{}}
	per := map[string][]float64{}
	var all []float64
	for _, run := range runs {
		for uid, ts := range run {
			ts = slices.Clone(ts)
			slices.SortFunc(ts, func(a, b time.Time) int { return a.Compare(b) })
			for i := 1; i < len(ts); i++ {
				d := ts[i].Sub(ts[i-1]).Seconds()
				per[uid] = append(per[uid], d)
				all = append(all, d)
			}
		}
	}
	c.Intervals = len(all)
	c.MedianSeconds = medianSeconds(all)
	for uid, d := range per {
		c.PerNode[fmt.Sprintf("%s (%s)", name(uid), shortUID(uid))] = medianSeconds(d)
	}
	return c
}

func shortUID(uid string) string {
	if len(uid) > 8 {
		return uid[:8]
	}
	return uid
}

// waiting is the pods the truth lists with no container id yet (a container that has not
// started): how many, how many never got one, and for how long the others were without.
type waiting struct {
	PodsEverWithoutID int `json:"pods_listed_without_a_container_id"`
	NeverGotOne       int `json:"pods_that_never_got_one"`
	// Seconds are the time a pod was listed without an id, summed over its samples.
	MedianSeconds float64 `json:"median_seconds_without_an_id"`
	MaxSeconds    float64 `json:"max_seconds_without_an_id"`
}

func waitingContainers(pods *podsTruth) waiting {
	var w waiting
	var secs []float64
	spacing := pods.spacing()
	for _, uid := range pods.UIDs {
		pt := pods.Pods[uid]
		var without time.Duration
		empty, withID := 0, 0
		for _, r := range pt.Rows {
			if strings.TrimSpace(r.CIDs) == "" {
				empty++
				without += spacing[r.Sample]
			} else {
				withID++
			}
		}
		if empty == 0 {
			continue
		}
		w.PodsEverWithoutID++
		if withID == 0 {
			w.NeverGotOne++
		}
		secs = append(secs, without.Seconds())
	}
	w.MedianSeconds = medianSeconds(secs)
	for _, s := range secs {
		w.MaxSeconds = max(w.MaxSeconds, s)
	}
	return w
}

// alignment is the median offset between each pod's first watch ADDED and its first truth
// sample, which says how far apart the host's clock and the collector's are.
type alignment struct {
	Pods          int     `json:"pods"`
	MedianSeconds float64 `json:"median_seconds"`
	MinSeconds    float64 `json:"min_seconds"`
	MaxSeconds    float64 `json:"max_seconds"`
}

// clockAlignment measures the offsets between the first ADDED record of each pod created
// while a run was up (at least two minutes in, so it is not the initial list) and the first
// sample that lists it.
func clockAlignment(pods *podsTruth, stats []*runStats, wl *workload) alignment {
	var offs []float64
	for _, rs := range stats {
		var start time.Time
		for _, r := range wl.Runs {
			if r.N == rs.N {
				start = r.Start
			}
		}
		if start.IsZero() {
			continue
		}
		for uid, t := range rs.Added {
			pt := pods.Pods[uid]
			if pt == nil || t.Before(start.Add(2*time.Minute)) {
				continue
			}
			offs = append(offs, t.Sub(pods.Times[pt.First]).Seconds())
		}
	}
	a := alignment{Pods: len(offs), MedianSeconds: medianSeconds(offs)}
	for i, o := range offs {
		if i == 0 || o < a.MinSeconds {
			a.MinSeconds = o
		}
		if i == 0 || o > a.MaxSeconds {
			a.MaxSeconds = o
		}
	}
	return a
}
