package main

import (
	"maps"
	"runtime/debug"
	"slices"
	"strings"
)

// histPairs lists a histogram as [value, count] pairs in ascending value order.
func histPairs(h map[int64]int64) [][2]int64 {
	out := make([][2]int64, 0, len(h))
	for _, k := range slices.Sorted(maps.Keys(h)) {
		out = append(out, [2]int64{k, h[k]})
	}
	return out
}

// censorShares counts intervals by which end of the hour they touch.
type censorShares struct {
	Intervals int64 `json:"intervals"`
	Left      int64 `json:"left_censored"`
	Right     int64 `json:"right_censored"`
	Both      int64 `json:"both_censored"`
	Neither   int64 `json:"uncensored"`
	// CensoredShare is the share of intervals touching either end.
	CensoredShare float64 `json:"censored_share"`
}

func shares(ivs []interval, ttl, hour int32) censorShares {
	var c censorShares
	for _, iv := range ivs {
		l, r := censored(iv, ttl, hour)
		c.Intervals++
		switch {
		case l && r:
			c.Both++
		case l:
			c.Left++
		case r:
			c.Right++
		default:
			c.Neither++
		}
	}
	if c.Intervals > 0 {
		c.CensoredShare = float64(c.Intervals-c.Neither) / float64(c.Intervals)
		c.CensoredShare = float64(int64(c.CensoredShare*10000+0.5)) / 10000
	}
	return c
}

type paramsFile struct {
	Basis       string `json:"basis"`
	HourMinutes int32  `json:"hour_minutes"`

	Nodes             int `json:"nodes"`
	Instances         int `json:"instances"`
	PlacementServices int `json:"placement_services"`
	CallServices      int `json:"call_services"`

	PodsPerNode             dist   `json:"pods_per_node"`
	PodsPerNodeCensoring    string `json:"pods_per_node_censoring"`
	InstancesPerService     dist   `json:"instances_per_service"`
	InstancesPerServiceNote string `json:"instances_per_service_censoring"`

	ChurnPerNode          dist         `json:"placement_churn_per_node_hour"`
	ChurnCensoring        string       `json:"placement_churn_censoring"`
	InstanceLifetime      dist         `json:"instance_lifetime_minutes_uncensored"`
	InstanceLifetimeAll   dist         `json:"instance_lifetime_minutes_all_intervals"`
	InstanceCensoring     censorShares `json:"instance_lifetime_censoring"`
	EdgeIntervalCensoring censorShares `json:"pod_on_node_interval_censoring"`
	GapHistogram          [][2]int64   `json:"gap_histogram_missed_samples"`

	OutDegreeHour     dist             `json:"service_out_degree_hour"`
	InDegreeHour      dist             `json:"service_in_degree_hour"`
	OutDegreePerMin   dist             `json:"service_out_degree_per_minute"`
	InDegreePerMin    dist             `json:"service_in_degree_per_minute"`
	DegreeCensoring   string           `json:"degree_censoring"`
	Edges             int              `json:"distinct_edges"`
	EdgesInOneFile    int              `json:"edges_seen_in_only_one_call_archive"`
	EdgeChurnShare    float64          `json:"edge_churn_share"`
	EdgeCensoring     censorShares     `json:"edge_interval_censoring"`
	DepthDistribution [][2]int64       `json:"rpc_depth_distribution"`
	CallsByRPCType    map[string]int64 `json:"calls_by_rpc_type"`
}

const paramsBasis = "One hour of one trace, reduced on a laptop. Local and informational: nothing here is published or " +
	"a property of a cluster beyond this hour. Every distribution is censored at the hour's ends, as each states."

// params fits the workload parameters of the hour, each with its censoring.
func (r *results) params() paramsFile {
	pr, d, cr := r.place, r.ids, r.calls
	hour := r.cfg.hourMinutes()
	p := paramsFile{
		Basis: paramsBasis, HourMinutes: hour, Nodes: d.nodes.size(), Instances: d.pods.size(),
		PlacementServices: d.names.size(), CallServices: d.svcs.size(),
		PodsPerNode:          histDist(pr.perNode),
		PodsPerNodeCensoring: "over every (minute, node) cell of the hour; a node's pods are counted as the samples show, so a pod missing a sample is missing from that cell",
		GapHistogram:         histPairs(pr.gaps),
		DepthDistribution:    histPairs(cr.depth),
		CallsByRPCType:       map[string]int64{},
		OutDegreePerMin:      histDist(cr.fanOut),
		InDegreePerMin:       histDist(cr.fanIn),
		DegreeCensoring:      "degrees count the edges seen in the hour; an edge seen only outside it is absent, and the per-hour degrees are lower bounds",
	}
	for i, t := range rpcTypes {
		p.CallsByRPCType[t] = cr.byRPC[i]
	}

	// Instances per service, by the msname of an instance's first sample.
	perName := map[uint32]int64{}
	for id := 1; id <= d.pods.size(); id++ {
		if n := pr.podFirstName[id]; n != 0 {
			perName[n]++
		}
	}
	counts := make([]int64, 0, len(perName))
	for _, c := range perName {
		counts = append(counts, c)
	}
	p.InstancesPerService = summarize(counts)
	p.InstancesPerServiceNote = "instances seen in the hour, by the msname of their first sample; an instance outside the hour is not counted"

	// Churn of pod-on-node placements per node, at C60.
	c60 := cadenceC60
	edgeIvs := r.edgeIvs[c60.name]
	churn := make(map[uint32]int64, d.nodes.size())
	for id := 1; id <= d.nodes.size(); id++ {
		churn[uint32(id)] = 0
	}
	for _, iv := range edgeIvs {
		_, n := unpair(iv.key)
		l, rt := censored(iv, c60.ttlMinutes(), hour)
		if !l {
			churn[n]++
		}
		if !rt {
			churn[n]++
		}
	}
	cv := make([]int64, 0, len(churn))
	for _, c := range churn {
		cv = append(cv, c)
	}
	p.ChurnPerNode = summarize(cv)
	p.ChurnCensoring = "a start (a placement that begins after the hour's first sample) and an end (one whose deadline is within the hour) each count once; " +
		"intervals touching an end of the hour contribute only their other end"
	p.EdgeIntervalCensoring = shares(edgeIvs, c60.ttlMinutes(), hour)

	// Instance lifetimes, from the pods' existence intervals at C60.
	podIvs := r.podIvs[c60.name]
	var all, free []int64
	for _, iv := range podIvs {
		l := int64(iv.last-iv.start) + 1
		all = append(all, l)
		if lc, rc := censored(iv, c60.ttlMinutes(), hour); !lc && !rc {
			free = append(free, l)
		}
	}
	p.InstanceLifetime = summarize(free)
	p.InstanceLifetimeAll = summarize(all)
	p.InstanceCensoring = shares(podIvs, c60.ttlMinutes(), hour)

	// The service graph of the hour.
	callHour := int32(int64(r.cfg.CallFiles) * r.cfg.CallFileMillis / 60000)
	out, in := map[uint32]int64{}, map[uint32]int64{}
	filesOf := map[uint64]int32{}
	for i, iv := range r.callIvs {
		filesOf[iv.key] += iv.files
		if i > 0 && r.callIvs[i-1].key == iv.key {
			continue
		}
		u, dm := unpair(iv.key)
		out[u]++
		in[dm]++
	}
	ov, iv2 := make([]int64, 0, len(out)), make([]int64, 0, len(in))
	for _, c := range out {
		ov = append(ov, c)
	}
	for _, c := range in {
		iv2 = append(iv2, c)
	}
	p.OutDegreeHour, p.InDegreeHour = summarize(ov), summarize(iv2)
	p.Edges = len(filesOf)
	for _, n := range filesOf {
		if n == 1 {
			p.EdgesInOneFile++
		}
	}
	if p.Edges > 0 {
		p.EdgeChurnShare = float64(int64(float64(p.EdgesInOneFile)/float64(p.Edges)*10000+0.5)) / 10000
	}
	p.EdgeCensoring = shares(r.callIvs, int32(callEdgeTTL/sampleInterval), callHour)
	return p
}

// manifestFile records what a run read, how it was run and what it wrote.
type manifestFile struct {
	Tool               string                      `json:"tool"`
	Commit             string                      `json:"commit"`
	Epoch              string                      `json:"epoch"`
	Layout             string                      `json:"layout_note"`
	Flags              map[string]any              `json:"flags"`
	Cadences           map[string]map[string]int64 `json:"cadences"`
	Coalescer          map[string]string           `json:"coalescer"`
	FlushPeriodSeconds int64                       `json:"flush_period_seconds"`
	Provisional        string                      `json:"provisional"`
	Inputs             []*fileReport               `json:"inputs"`
	Outputs            []sinkResult                `json:"outputs"`
	Tables             map[string]int64            `json:"tables"`
	Ratios             ratios                      `json:"ratios"`
	Checks             map[string]int64            `json:"in_process_store_checks"`
}

type ratios struct {
	PlacementRows      int64            `json:"placement_rows_read"`
	PlacementUsedRows  int64            `json:"placement_rows_used"`
	DedupedEdgeSamples int64            `json:"placement_edge_samples_after_duplicates"`
	MaximalIntervals   map[string]int64 `json:"maximal_intervals_c60"`
	IntervalsC300      map[string]int64 `json:"intervals_c300"`
	StoredRecordsC60   int64            `json:"stored_records_c60"`
	StoredRecordsC300  int64            `json:"stored_records_c300"`
	CallRows           int64            `json:"call_rows_read"`
	CallRowsUsed       int64            `json:"call_rows_used"`
	CallRollupRows     int64            `json:"call_rollup_rows"`
	CallEdgeMinutes    int64            `json:"call_edge_minutes"`
	SelfCallRows       int64            `json:"self_call_rows"`
	CallEdgeIntervals  int64            `json:"call_edge_maximal_intervals"`
	StoredDependsOn    int64            `json:"stored_depends_on_records"`
	RTMissing          int64            `json:"rt_missing_among_used"`
	RTOdd              int64            `json:"rt_unparsed_among_used"`
	Duplicates         int64            `json:"dirt_duplicate_rows"`
	DirtCounts         map[string]int64 `json:"dirt_counts"`
}

func (r *results) manifest() manifestFile {
	cfg := &r.cfg
	m := manifestFile{
		Tool: "alibabareduce", Commit: cfg.Commit, Epoch: cfg.Epoch.Format("2006-01-02T15:04:05Z07:00"),
		Layout: "per-archive timestamp layout and offsets are in the inputs",
		Flags: map[string]any{
			"placement_files": cfg.PlacementFiles, "call_files": cfg.CallFiles, "placement_file_minutes": cfg.PlacementFileMinutes,
			"call_file_millis": cfg.CallFileMillis, "fixture_minutes": cfg.FixtureMinutes, "fixture_nodes": cfg.FixtureNodes,
			"write_c60": cfg.WriteC60, "max_file_bytes": cfg.MaxFileBytes, "max_call_keys": cfg.MaxCallKeys, "max_edges": cfg.MaxEdges,
			"sample_interval_seconds": int64(sampleInterval.Seconds()), "ttl_factor": ttlFactor,
		},
		Cadences:           map[string]map[string]int64{},
		Coalescer:          coalesceConfig().Describe(),
		FlushPeriodSeconds: 30,
		Provisional:        "the depends_on stream is provisional: the storage format of per-minute rollups (l3-rollups) is not decided here",
		Inputs:             r.files, Tables: r.tables, Checks: r.asked,
	}
	for _, c := range []cadenceSpec{cadenceC60, cadenceC300} {
		m.Cadences[c.name] = map[string]int64{"step_minutes": int64(c.step), "ttl_seconds": int64(c.ttl().Seconds())}
	}
	m.Outputs = append(m.Outputs, r.sinks...)
	m.Outputs = append(m.Outputs, r.fixture)

	rt := ratios{
		MaximalIntervals:   map[string]int64{"pods": int64(len(r.podIvs["c60"])), "nodes": int64(len(r.nodeIvs["c60"])), "pod_on_node": int64(len(r.edgeIvs["c60"]))},
		IntervalsC300:      map[string]int64{"pods": int64(len(r.podIvs["c300"])), "nodes": int64(len(r.nodeIvs["c300"])), "pod_on_node": int64(len(r.edgeIvs["c300"]))},
		DedupedEdgeSamples: r.place.used, Duplicates: r.place.dupRows, DirtCounts: maps.Clone(r.place.dirt.counts),
		CallRollupRows: r.calls.rollupRows, CallEdgeMinutes: r.calls.edgeMinutes, SelfCallRows: r.calls.selfCalls,
		CallEdgeIntervals: int64(len(r.callIvs)), RTMissing: r.calls.rtMissing, RTOdd: r.calls.rtOdd,
	}
	for _, f := range r.files {
		if f.Table == placementTable {
			rt.PlacementRows += f.Rows
			rt.PlacementUsedRows += f.Used
		} else {
			rt.CallRows += f.Rows
			rt.CallRowsUsed += f.Used
		}
	}
	for _, s := range r.sinks {
		switch s.Name {
		case "placement_c60":
			rt.StoredRecordsC60 = s.Records
		case "placement_c300":
			rt.StoredRecordsC300 = s.Records
		case "calls":
			rt.StoredDependsOn = s.Records
		}
	}
	m.Ratios = rt
	return m
}

// currentCommit is the revision this binary was built from, if it knows.
func currentCommit() string {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return ""
	}
	var rev, dirty string
	for _, s := range bi.Settings {
		switch s.Key {
		case "vcs.revision":
			rev = s.Value
		case "vcs.modified":
			if s.Value == "true" {
				dirty = "+dirty"
			}
		}
	}
	return strings.TrimSpace(rev + dirty)
}
