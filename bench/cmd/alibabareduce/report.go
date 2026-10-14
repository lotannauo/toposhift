package main

import (
	"fmt"
	"strings"

	"github.com/lotannauo/toposhift/bench/spike/workload"
)

// The tolerances the fixture's pods-per-node distribution is held to against the
// hour's, written before either is computed: a quantile within this share of the
// hour's, or within two pods when that is larger.
const (
	fixtureTolerance      = 0.25
	fixtureToleranceCount = 2
)

// withinTolerance is whether the fixture's value is within the tolerance of the hour's.
func withinTolerance(fixture, hour int64) bool {
	diff := fixture - hour
	if diff < 0 {
		diff = -diff
	}
	return diff <= fixtureToleranceCount || float64(diff) <= fixtureTolerance*float64(hour)
}

// report is REDUCTION.md: the numbers of the run with their labels and
// censoring. Every number is [M, local]: measured on this machine from the
// archives and informational, never published.
func (r *results) report() string {
	var b strings.Builder
	w := func(format string, args ...any) { fmt.Fprintf(&b, format+"\n", args...) }
	n := func(v int64) string { return fmt.Sprintf("%d [M, local]", v) }
	p := r.params()
	m := r.manifest()
	rt := m.Ratios

	w("# Reduction of one hour of the Alibaba microservices trace (v2022)")
	w("")
	w("Every number is labeled [M, local]: measured here, from the archives, and informational. None is a performance figure, and none is published.")
	w("Everything is one hour, censored at both ends of it; nothing here shows daily patterns, lifetimes longer than an hour or retention parameters.")
	w("")
	w("## Inputs and accounting")
	w("")
	w("Epoch (placing the trace on a calendar only): %s. Hour: %d minutes.", m.Epoch, r.cfg.hourMinutes())
	w("")
	w("| file | rows | used | columns | shifted | timestamp | empty_id | unknown_endpoint | layout |")
	w("|---|---|---|---|---|---|---|---|---|")
	var dirty, total int64
	for _, f := range r.files {
		w("| %s | %d | %d | %d | %d | %d | %d | %d | %s |", f.Name, f.Rows, f.Used, f.skips[skipColumns], f.skips[skipShifted],
			f.skips[skipTimestamp], f.skips[skipEmptyID], f.skips[skipUnknownEndpoint], f.Layout)
		total += f.Rows
		dirty += f.skips[skipColumns] + f.skips[skipShifted] + f.skips[skipTimestamp] + f.skips[skipEmptyID]
	}
	w("")
	w("Rows read equal rows used plus the skips, per file, exactly. `columns`, `shifted`, `timestamp` and `empty_id` together are %d of %d rows [M, local]; `unknown_endpoint` is expected and is not dirt.", dirty, total)
	w("A call endpoint is unknown if `um` or `dm` is `UNKNOWN` or `UNAVAILABLE`, or an instance id is empty, one of those two, or `UNKNOWN_POD_<n>`.")
	w("The response time `rt` is read as a decimal with at most three decimals and summed exactly in thousandths of its unit; its unit is not confirmed.")
	w("")
	w("## Placement")
	w("")
	w("- samples read: %s; used: %s", n(rt.PlacementRows), n(rt.PlacementUsedRows))
	w("- pod-on-node samples after one assertion per instant and producer: %s (duplicates: %s)", n(rt.DedupedEdgeSamples), n(rt.Duplicates))
	w("- maximal intervals at C60 (the raw interval count, one record per interval; reported for comparison only): pods %s, nodes %s, pod-on-node %s",
		n(rt.MaximalIntervals["pods"]), n(rt.MaximalIntervals["nodes"]), n(rt.MaximalIntervals["pod_on_node"]))
	w("- intervals at C300: pods %s, nodes %s, pod-on-node %s", n(rt.IntervalsC300["pods"]), n(rt.IntervalsC300["nodes"]), n(rt.IntervalsC300["pod_on_node"]))
	w("- records stored by the coalescer under the shipped configuration: C60 %s, C300 %s", n(rt.StoredRecordsC60), n(rt.StoredRecordsC300))
	w("  The stored count is the coalescer's, which bounds a run's age at half its TTL on purpose; it is not the maximal-interval count.")
	w("- the intervals are sampled at 60 s (300 s for C300) and censored at both ends: they are not true pod lifetimes.")
	w("")
	w("## Dirt (anomalies relative to this model, kept as the trace said them)")
	w("")
	for _, k := range dirtKinds {
		w("- %s: %s", k, n(rt.DirtCounts[k]))
	}
	w("")
	w("Gaps (samples missed by an (instance, node) that came back within the TTL): %v [M, local]", p.GapHistogram)
	w("")
	w("## Distributions and their censoring")
	w("")
	d := func(name string, x dist, censoring string) {
		w("- %s: n %d, min %d, p50 %d, p90 %d, p99 %d, max %d, mean %.2f [M, local]. Censoring: %s", name, x.N, x.Min, x.P50, x.P90, x.P99, x.Max, x.Mean, censoring)
	}
	d("pods per node", p.PodsPerNode, p.PodsPerNodeCensoring)
	d("instances per service", p.InstancesPerService, p.InstancesPerServiceNote)
	d("placement churn per node per hour", p.ChurnPerNode, p.ChurnCensoring)
	d("instance lifetime (minutes, uncensored intervals only)", p.InstanceLifetime, "only intervals touching neither end; this biases it short")
	d("instance lifetime (minutes, all intervals)", p.InstanceLifetimeAll, "intervals touching an end are cut by it")
	w("- instance intervals: %d, left-censored %d, right-censored %d, both %d, neither %d; censored share %.4f [M, local]",
		p.InstanceCensoring.Intervals, p.InstanceCensoring.Left, p.InstanceCensoring.Right, p.InstanceCensoring.Both, p.InstanceCensoring.Neither, p.InstanceCensoring.CensoredShare)
	w("  More hours are asked for only if more than about 50%% of lifetimes are censored; the share here is %.4f [M, local].", p.InstanceCensoring.CensoredShare)
	d("service out-degree (hour)", p.OutDegreeHour, p.DegreeCensoring)
	d("service in-degree (hour)", p.InDegreeHour, p.DegreeCensoring)
	d("service out-degree (per service and minute)", p.OutDegreePerMin, "per minute, so no edge is cut by the hour")
	d("service in-degree (per service and minute)", p.InDegreePerMin, "per minute, so no edge is cut by the hour")
	w("- edges: %d distinct; seen in only one 3-minute archive: %d (share %.4f) [M, local]. The calls' unit of time is the archive, three minutes.", p.Edges, p.EdgesInOneFile, p.EdgeChurnShare)
	w("- rpc depth (dot-separated parts of rpc_id, count): %v [M, local]", p.DepthDistribution)
	w("")
	w("## Calls")
	w("")
	w("- rows read %s, used %s; rollup rows %s; edge-minutes %s; self-call rows (kept in the rollup, left out of the edges) %s", n(rt.CallRows), n(rt.CallRowsUsed), n(rt.CallRollupRows), n(rt.CallEdgeMinutes), n(rt.SelfCallRows))
	w("- maximal edge intervals %s; stored `depends_on` records %s. The edge stream is provisional: the storage of per-minute rollups is not decided here.", n(rt.CallEdgeIntervals), n(rt.StoredDependsOn))
	w("- distinct traces are counted as 64-bit hashes per rollup cell: exact up to 64-bit collisions.")
	w("- `rt` missing or unparsed among used rows: %s, %s", n(rt.RTMissing), n(rt.RTOdd))
	w("")
	w("## Fixture")
	w("")
	w("The fixture keeps %d nodes by hash of the node token, with every sample of each, for the first %d minutes, and the `depends_on` edges whose both services have a pod on a kept node. It was written (%s records) and replayed into the in-memory store, which agreed with the samples at %s questions.",
		r.cfg.FixtureNodes, r.cfg.FixtureMinutes, n(r.fixture.Records), n(r.asked["fixture"]))
	w("")
	w("Tolerance, fixed before the numbers were computed: each pods-per-node quantile of the fixture is within %.0f%% of the hour's, or within %d pods when that is larger.", fixtureTolerance*100, fixtureToleranceCount)
	fh := histDist(r.fixtureHist)
	for _, q := range []struct {
		name         string
		fixture, all int64
	}{{"p50", fh.P50, p.PodsPerNode.P50}, {"p90", fh.P90, p.PodsPerNode.P90}, {"p99", fh.P99, p.PodsPerNode.P99}} {
		verdict := "within"
		if !withinTolerance(q.fixture, q.all) {
			verdict = "outside"
		}
		w("- pods per node %s: fixture %d, hour %d: %s [M, local]", q.name, q.fixture, q.all, verdict)
	}
	w("The fixture's degree distributions are not compared with the hour's: it keeps the edges among a sample of services, so they are lower by construction.")
	w("")
	w("## Against the synthetic preset")
	w("")
	pre := workload.Cluster(1)
	w("The cluster preset has %d hosts, %d pods (%d per host), %d services, %d dependencies per service and a pod heartbeat every %s [preset values at this commit].",
		pre.Hosts, pre.Pods, pre.Pods/max(pre.Hosts, 1), pre.Services, pre.DependsPerService, pre.PodHeartbeatInterval)
	w("This hour has %d nodes, %d instances (%.1f per node on average over minutes), %d placement services, %d call services, mean out-degree %.2f per service and minute, and a sample every minute [M, local].",
		p.Nodes, p.Instances, p.PodsPerNode.Mean, p.PlacementServices, p.CallServices, p.OutDegreePerMin.Mean)
	w("So the hour would move these defaults, were they to follow it: nodes (x%.0f), pods (x%.1f), pods per node (the busiest node holds %d), services, dependencies per service, and the heartbeat interval (one minute against fifteen). It changes no preset; that is a later lane.",
		float64(p.Nodes)/float64(max(pre.Hosts, 1)), float64(p.Instances)/float64(max(pre.Pods, 1)), p.PodsPerNode.Max)
	w("")
	w("## What this does not claim")
	w("")
	w("- that `nodeid` is a host or a machine (it is placed as a `k8s.node`), that `rt` is in milliseconds, or that an `msname` is a Kubernetes service;")
	w("- that the dirt is error in the trace's systems, or that intervals are true pod lifetimes;")
	w("- that the fixture represents the whole trace beyond the tolerances above;")
	w("- any performance or throughput figure.")
	return b.String()
}
