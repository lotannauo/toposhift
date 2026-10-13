package main

import (
	"fmt"
	"io"
	"strings"
	"text/tabwriter"

	"github.com/lotannauo/toposhift/internal/coalesce"
)

func heading(w io.Writer, format string, args ...any) {
	fmt.Fprintf(w, "\n== "+format+" ==\n", args...)
}

func counts(m map[string]int) string {
	if len(m) == 0 {
		return "none"
	}
	var parts []string
	for _, k := range sortedKeys(m) {
		parts = append(parts, fmt.Sprintf("%s=%d", k, m[k]))
	}
	return strings.Join(parts, ", ")
}

func counts64(m map[string]int64) string {
	var parts []string
	for _, k := range sortedKeys(m) {
		if m[k] != 0 {
			parts = append(parts, fmt.Sprintf("%s=%d", k, m[k]))
		}
	}
	if len(parts) == 0 {
		return "none"
	}
	return strings.Join(parts, ", ")
}

func ts(t interface{ Format(string) string }) string { return t.Format("15:04:05") }

// writeText prints the report.
func writeText(w io.Writer, r *Report) error {
	fmt.Fprintf(w, "capturereplay: %s\n", r.Capture)
	fmt.Fprintf(w, "replay %s, analysis %s; truth margin %s\n", r.Elapsed, r.AnalysisElapsed, r.Margin)
	fmt.Fprintf(w, "truth: %d pod samples (%d unavailable), %d pod UIDs; %d node samples (%d unavailable), %d node UIDs\n",
		r.Truth.PodSamples, r.Truth.PodUnavailable, r.Truth.PodUIDs, r.Truth.NodeSamples, r.Truth.NodeUnavailable, r.Truth.NodeUIDs)
	fmt.Fprintln(w, "the coalescer is flushed at each listing sample (every 30 s), and in pass B fully at each restart")
	fmt.Fprintf(w, "largest gap between listing samples: %s (flush lead %s)%s\n",
		r.Truth.PodSampleLargestGap, coalesce.FlushLead, map[bool]string{true: ": OVER THE LEAD", false: ""}[r.Truth.FlushGapOverLead])

	heading(w, "BLOCKING (a decided rule was breached)")
	if len(r.Blocking) == 0 {
		fmt.Fprintln(w, "none")
	}
	for _, b := range r.Blocking {
		fmt.Fprintln(w, "BLOCKING:", b)
	}
	if len(r.Blocking) > 0 {
		fmt.Fprintln(w, "A BLOCKING count above 0 holds the M3 release. Informational counts never hold anything.")
	}
	heading(w, "findings")
	if len(r.Findings) == 0 {
		fmt.Fprintln(w, "none")
	}
	for _, f := range r.Findings {
		fmt.Fprintln(w, "-", f)
	}
	heading(w, "self-check failures")
	if len(r.Failures) == 0 {
		fmt.Fprintln(w, "none")
	}
	for _, f := range r.Failures {
		fmt.Fprintln(w, "FAILED:", f)
	}

	heading(w, "1. per run")
	for _, rr := range r.Runs {
		fmt.Fprintf(w, "run %d: %d lines, %d bytes, %d records; sources: %s\n", rr.N, rr.Lines, rr.Bytes, rr.Records, counts(rr.BySource))
		fmt.Fprintf(w, "  watch: %s\n", counts(rr.Watch))
		fmt.Fprintf(w, "  entity events: %s\n", counts(rr.Entity))
		fmt.Fprintf(w, "  observed %s .. %s; entity events %s .. %s\n", rr.Observed.Min.Format("2006-01-02 15:04:05"), rr.Observed.Max.Format("15:04:05"),
			rr.Entities.Min.Format("2006-01-02 15:04:05"), rr.Entities.Max.Format("15:04:05"))
		fmt.Fprintf(w, "  pull lines: %d", len(rr.Pulls))
		for i, pl := range rr.Pulls {
			if i%6 == 0 {
				fmt.Fprint(w, "\n   ")
			}
			fmt.Fprintf(w, " [line %d: %d pods %d nodes %d B @%s]", pl.Line, pl.Pods, pl.Nodes, pl.Bytes, ts(pl.Observed))
		}
		fmt.Fprintln(w)
		if rr.SortErr != "" {
			fmt.Fprintf(w, "  SortRefs: %s\n", rr.SortErr)
		} else {
			fmt.Fprintf(w, "  SortRefs would move %d of %d records (furthest %d places); the replay keeps file order\n", rr.MovedBySort, rr.Records, rr.MaxShift)
		}
		fmt.Fprintf(w, "  container ids: pod objects %s | entity events %s\n", counts(rr.IDShapesObjects), counts(rr.IDShapesCluster))
		fmt.Fprintf(w, "  distinct container ids by shape: pod objects %s | entity events %s\n", counts(rr.DistinctObjects), counts(rr.DistinctCluster))
		fmt.Fprintf(w, "  entity.report.interval: %s\n", counts(rr.ReportIntervals))
		for _, c := range rr.Cross {
			fmt.Fprintf(w, "  uid cross-check, %s: both %d, only in objects records %d, only in entity events %d\n", c.Kind, c.Both, c.OnlyObjects, c.OnlyCluster)
		}
	}

	heading(w, "2. the largest pull")
	if r.Pull != nil {
		p := r.Pull
		fmt.Fprintf(w, "run %d line %d: %d records; JSON %d bytes (%.2f MiB), protobuf %d bytes (%.2f MiB); gzip: JSON %d, protobuf %d; receiver limit %d bytes; fits: %v\n",
			p.Run, p.Line, p.Size.Records, p.Size.JSONBytes, mib(p.Size.JSONBytes), p.Size.ProtoBytes, mib(p.Size.ProtoBytes), p.Size.JSONGzip, p.Size.ProtoGzip, p.Limit, p.Size.Fits)
	}

	for _, p := range r.Passes {
		writePass(w, r, p)
	}

	heading(w, "pass A against pass B")
	d := r.Diff
	fmt.Fprintf(w, "pod sample pairs compared %d, differing %d (alive only in A %d, only in B %d); by window: %s\n", d.PairsCompared, d.PairsDiffer, d.AliveOnlyInA, d.AliveOnlyInB, counts(d.ByWindow))
	for _, x := range d.WrittenByRun {
		fmt.Fprintf(w, "  run %d records written: A %d, B %d\n", x.Run, x.A, x.B)
	}
	for _, e := range d.Examples {
		fmt.Fprintln(w, "  ", e)
	}

	heading(w, "6. cadences")
	for _, c := range []struct {
		n string
		c cadence
	}{{"node watch MODIFIED", r.Cadences.NodeWatchModified}, {"k8scluster node state", r.Cadences.NodeClusterState}} {
		fmt.Fprintf(w, "%s: median %.1f s over %d intervals within runs\n", c.n, c.c.MedianSeconds, c.c.Intervals)
		for _, k := range sortedKeys(c.c.PerNode) {
			fmt.Fprintf(w, "    %s: %.1f s\n", k, c.c.PerNode[k])
		}
	}
	a := r.Alignment
	fmt.Fprintf(w, "\nclock alignment (first watch ADDED less first truth sample, pods created at least 2 min into a run): %d pods, median %.1f s, min %.1f s, max %.1f s\n", a.Pods, a.MedianSeconds, a.MinSeconds, a.MaxSeconds)
	fmt.Fprintf(w, "pods listed without a container id in the truth: %d, of which %d never had one; median time without %.0f s, max %.0f s\n", r.Waiting.PodsEverWithoutID, r.Waiting.NeverGotOne, r.Waiting.MedianSeconds, r.Waiting.MaxSeconds)
	return nil
}

func mib(n int) float64 { return float64(n) / (1 << 20) }

func writePass(w io.Writer, r *Report, p *passReport) {
	heading(w, "PASS %s (%d restarts of the translators and the coalescer)", p.Name, p.Restarts)
	fmt.Fprintf(w, "flush records written: %d\n", p.FlushRecords)
	fmt.Fprintln(w, "\nrecords per run (translated by each translator; written after the coalescer):")
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "run\tbatches\tobjects\tcluster\twritten\tobjects errors\tcluster errors\t")
	for _, x := range p.Runs {
		fmt.Fprintf(tw, "%d\t%d\t%d\t%d\t%d\t%d\t%d\t\n", x.N, x.Batches, x.Objects.Translated, x.Cluster.Translated, x.Written, x.Objects.Errors, x.Cluster.Errors)
	}
	tw.Flush()
	for _, x := range p.Runs {
		fmt.Fprintf(w, "  run %d skipped: objects %s | cluster %s\n", x.N, counts(x.Objects.Skipped), counts(x.Cluster.Skipped))
		fmt.Fprintf(w, "        notes: objects %s | cluster %s\n", counts(x.Objects.Notes), counts(x.Cluster.Notes))
		for _, s := range x.Subjects {
			fmt.Fprintf(w, "        %s subjects: both %d, only objects %d, only cluster %d\n", s.Kind, s.Both, s.OnlyObjects, s.OnlyCluster)
		}
	}
	for _, s := range p.Subjects {
		fmt.Fprintf(w, "  all runs, %s subjects: both %d, only objects %d, only cluster %d\n", s.Kind, s.Both, s.OnlyObjects, s.OnlyCluster)
	}
	fmt.Fprintf(w, "resume, all runs: %s\n", counts64(p.Resume))
	for _, x := range p.Runs {
		fmt.Fprintf(w, "  run %d resume: %s\n", x.N, counts64(x.Resume))
	}
	st := p.Store
	fmt.Fprintf(w, "pebble store: checkpoints written %d, invalidated %d (%s); lookups %d, keys walked at most %d, exhausted %d\n",
		st.CheckpointsWritten, st.CheckpointsInvalid, st.CheckpointPolicy, st.LookupIterators, st.LookupKeysWalkedMax, st.LookupExhausted)
	if st.ResumeFirstReadError != "" {
		fmt.Fprintf(w, "  resume: the first read error: %s\n", st.ResumeFirstReadError)
	}

	heading(w, "3. pods against the truth, pass %s", p.Name)
	writeExistence(w, p.Pods)
	fmt.Fprintln(w, "\npredictions for the collector stops (minutes; per pod unless stated). The plain prediction counts from the last pull plus the TTL;")
	fmt.Fprintln(w, "the coalescer-aware one from the store's own deadline for the pod (the last refresh it recorded plus the TTL). Predictions are informational;")
	fmt.Fprintln(w, "a window is flagged only when a pod's measured absence exceeds its coalescer-aware prediction plus 60 s.")
	tw = tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "window\tstated\tu\toutage\tplain\tpods\tplain pod-min\tcoalescer-aware longest\tcoalescer-aware pod-min\tno record\tmeasured pod-min\tmeasured longest\tlongest-plain (s)\twithin 30 s\tpods over coalescer-aware+60 s\tflagged\tnew pods pod-min\tnew pods longest\t")
	for _, x := range p.Predictions {
		fmt.Fprintf(tw, "%s\t%s\t%.2f\t%.2f\t%.2f\t%d\t%.1f\t%.2f\t%.1f\t%d\t%.1f\t%.2f\t%.0f\t%v\t%d\t%v\t%.1f\t%.2f\t\n",
			x.Window, x.Stated, x.UMin, x.OutageMin, x.RecomputedMin, x.PodsAtStop, x.RecomputedPodMin, x.CoalescerLongestMin, x.CoalescerPodMin, x.NoDeadline,
			x.MeasuredPodMin, x.MeasuredLongest, x.DeltaSec, x.Within30s, x.OverCoalescerPods, x.Flagged, x.NewPodMin, x.NewPodLongest)
	}
	tw.Flush()
	for _, win := range p.Pods.Windows {
		if strings.HasPrefix(win.Name, "API restart") {
			fmt.Fprintf(w, "%s: stated 0; measured pod-minutes %.1f, longest %.2f min (pods created in the window: %.1f, longest %.2f)\n", win.Name, win.AbsentMin, win.LongestAbsentMin, win.AbsentNewMin, win.LongestAbsentNewMin)
		}
	}

	heading(w, "4. ghost rate, pass %s", p.Name)
	tw = tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "window\tleft the truth\tdeleted by objects\tby cluster\tby both\tby neither\tlost objects\tlost cluster\tghost life (no delete): n, median, max min\tcensored\tover TTL+60s\t")
	for _, g := range p.Ghosts.Windows {
		if g.Disappeared == 0 {
			continue
		}
		fmt.Fprintf(tw, "%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d, %.1f, %.1f\t%d\t%d\t\n", g.Window, g.Disappeared, g.ObjectsDeleted, g.ClusterDeleted, g.BothDeleted, g.NoDelete,
			g.LostObjects, g.LostCluster, g.NoDeleteLife.N, g.NoDeleteLife.MedianMin, g.NoDeleteLife.MaxMin, g.NoDeleteLife.Censored, g.OverBound)
	}
	tw.Flush()
	fmt.Fprintln(w, "\npods the log names as deleted at a stop or an API restart:")
	tw = tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "window\tpod\tobjects delete\tcluster delete\tgone from the truth\tlast assertion\tghost min\tover bound\t")
	for _, n := range p.Ghosts.Named {
		fmt.Fprintf(tw, "%s\t%s\t%v\t%v\t%s\t%s\t%.1f\t%v\t\n", n.Window, n.Name, n.ObjectsDeleted, n.ClusterDeleted, ts(n.GoneFromTruth), ts(n.LastAssertion), n.GhostMinutes, n.OverBound)
	}
	tw.Flush()

	heading(w, "5. edges, pass %s", p.Name)
	tw = tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "window\tchecked\texact\tmissing\twrong node\texact + second edge\tedge, truth names no node\tedge-after-pod\tedge-across-gap\tedge alive after the pod left the truth\t")
	for _, e := range p.Edges.Windows {
		if e.Checked+e.Unexpected+e.AfterPodGone+e.AcrossGap+e.OutlivesTruth == 0 {
			continue
		}
		fmt.Fprintf(tw, "%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t\n", e.Window, e.Checked, e.Exact, e.Missing, e.Wrong, e.WithExtra, e.Unexpected, e.AfterPodGone, e.AcrossGap, e.OutlivesTruth)
	}
	tw.Flush()
	fmt.Fprintln(w, edgeAfterPodRule)
	fmt.Fprintf(w, "edge-after-pod count, pass %s: %d (longest %.1f min) %v\n", p.Name, p.Edges.AfterPodGone.Count, p.Edges.AfterPodGoneLongestMin, p.Edges.AfterPodGone.Examples)
	fmt.Fprintln(w, edgeAcrossGapRule)
	fmt.Fprintf(w, "edge-across-gap count, pass %s: %d\n", p.Name, p.Edges.AcrossGap)
	fmt.Fprintf(w, "second scheduled_on edge in the union: %d pairs on %d pods, longest %.1f min %v\n", p.Edges.Second.Count, p.Edges.SecondPods, p.Edges.SecondMaxMin, p.Edges.Second.Examples)

	heading(w, "nodes against the truth, pass %s", p.Name)
	writeExistence(w, p.Nodes)
	for _, n := range p.NodeLines.Nodes {
		fmt.Fprintf(w, "  node %s (%s): truth %s .. %s; store alive %s .. %s; false absent %d (%.1f min), ghost %d (%.1f min)\n",
			n.Name, shortUID(n.UID), n.TruthFirst.Format("15:04:05"), n.TruthLast.Format("15:04:05"), ts(n.AliveFrom), ts(n.AliveTo), n.FalseAbsent, n.AbsentMin, n.Ghost, n.GhostMin)
	}
	if g := p.NodeLines.Agent2; g != nil {
		fmt.Fprintf(w, "  agent-2 recreation: truth gap %.0f s; store: both UIDs alive %.0f s, neither alive %.0f s (sample-weighted)\n", g.TruthGapSec, g.StoreBothAliveSec, g.StoreNeitherSec)
	}

	heading(w, "7. container fingerprints, pass %s", p.Name)
	c := p.Containers
	fmt.Fprintf(w, "k8sobjects %d containers, k8scluster %d; in both %d, only objects %d, only cluster %d\n", c.ObjectsContainers, c.ClusterContainers, c.Both, c.OnlyObjects, c.OnlyCluster)
	fmt.Fprintf(w, "for the %d pods both wrote: in both %d, only objects %d, only cluster %d %v\n", c.SharedPods, c.SharedBoth, c.SharedOnlyObjects, c.SharedOnlyCluster, c.Examples)

	heading(w, "9. properties, pass %s", p.Name)
	fmt.Fprintf(w, "records failing Validate: %d %v\n", p.Properties.Invalid.Count, p.Properties.Invalid.Examples)
	fmt.Fprintf(w, "Observes lapsing before observed time + TTL: %d %v\n", p.Properties.Lapsed.Count, p.Properties.Lapsed.Examples)
	fmt.Fprintf(w, "store disagreements: pods %d, nodes %d\n", p.Disagree.Pods.Count, p.Disagree.Nodes.Count)
	_ = r
}

func writeExistence(w io.Writer, e existenceReport) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "window\tagree\tfalse absent\tghost\tskipped\tabsent pod-min\tlongest\tpods\tnew: pod-min\tlongest\tpods\tghost pod-min\tlongest\tpods\t")
	rows := append(append([]*windowStat{}, e.Windows...), &e.Total)
	for _, x := range rows {
		fmt.Fprintf(tw, "%s\t%d\t%d\t%d\t%d\t%.1f\t%.2f\t%d\t%.1f\t%.2f\t%d\t%.1f\t%.2f\t%d\t\n", x.Name, x.Agree, x.FalseAbsent, x.Ghost, x.Skipped,
			x.AbsentMin, x.LongestAbsentMin, x.AbsentPods, x.AbsentNewMin, x.LongestAbsentNewMin, x.AbsentNewPods, x.GhostMin, x.LongestGhostMin, x.GhostPods)
	}
	tw.Flush()
	for _, sp := range e.LongestAbsent {
		fmt.Fprintf(w, "  longest false absent: %s (%s) %.1f min from %s in %q\n", sp.Name, shortUID(sp.UID), sp.Minutes, sp.From.Format("15:04:05"), sp.Window)
	}
	for _, sp := range e.LongestGhost {
		fmt.Fprintf(w, "  longest ghost: %s (%s) %.1f min from %s in %q\n", sp.Name, shortUID(sp.UID), sp.Minutes, sp.From.Format("15:04:05"), sp.Window)
	}
}

// The two lines that say what the edge counts are, printed as they were decided.
const (
	edgeAfterPodRule  = "BLOCKING edge-after-pod: the count of (T, pod) with T outside the +/-60 s margin where the pod is absent in the truth and not alive in the store, yet its scheduled_on or part_of edge is alive in the store; BLOCKING if the count > 0"
	edgeAcrossGapRule = "edge-across-gap (informational): the count of (T, pod) where the pod is present in the truth but not alive in the store, while its edge is alive; expected from late-resolved edges carrying Through across an outage"
)
