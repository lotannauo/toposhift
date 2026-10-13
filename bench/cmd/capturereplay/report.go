package main

import (
	"context"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/lotannauo/toposhift/internal/ingest/resume"
	"github.com/lotannauo/toposhift/internal/store/pebblestore"
)

// Report is everything the replay measured. It is written as JSON, and printed as text.
type Report struct {
	Capture   string    `json:"capture"`
	Generated time.Time `json:"generated"`
	// Elapsed is the time of the replay (reading, translating, writing), and AnalysisElapsed
	// that of the queries and the comparison.
	Elapsed         string `json:"replay_elapsed"`
	AnalysisElapsed string `json:"analysis_elapsed"`
	Margin          string `json:"truth_margin"`

	// Blocking are the breaches of a decided rule. Findings are other things the replay
	// found that a reader should see. Failures are self-checks that failed; the command
	// exits non-zero for them.
	Blocking []string `json:"blocking"`
	Findings []string `json:"findings"`
	Failures []string `json:"self_check_failures"`

	Truth     truthSummary  `json:"truth"`
	Runs      []runReport   `json:"runs"`
	Pull      *pullReport   `json:"largest_pull"`
	Passes    []*passReport `json:"passes"`
	Diff      *passDiff     `json:"pass_a_against_pass_b"`
	Cadences  cadences      `json:"cadences"`
	Alignment alignment     `json:"clock_alignment"`
	Waiting   waiting       `json:"containers_without_an_id_in_the_truth"`
	Windows   []window      `json:"windows"`
}

type truthSummary struct {
	PodSamples      int `json:"pod_samples"`
	PodUIDs         int `json:"pod_uids"`
	PodUnavailable  int `json:"pod_samples_unavailable"`
	NodeSamples     int `json:"node_samples"`
	NodeUIDs        int `json:"node_uids"`
	NodeUnavailable int `json:"node_samples_unavailable"`
	// PodSampleLargestGap is the largest gap between consecutive pod listing times,
	// which are the times the coalescer is flushed at. A gap longer than the flush lead
	// ends the guarantee that a run's stored copy is written before it lapses.
	PodSampleLargestGap string `json:"pod_sample_largest_gap"`
	FlushGapOverLead    bool   `json:"pod_sample_gap_over_flush_lead"`
}

type runReport struct {
	N        int            `json:"run"`
	Lines    int            `json:"lines"`
	Bytes    int64          `json:"bytes"`
	Records  int            `json:"records"`
	BySource map[string]int `json:"records_by_source"`
	Watch    map[string]int `json:"watch_records_by_type_and_kind"`
	Entity   map[string]int `json:"entity_events_by_name_and_type"`
	Pulls    []pullLine     `json:"pull_lines"`
	Observed timeRange      `json:"observed_time_range"`
	Entities timeRange      `json:"entity_event_time_range"`
	// The records SortRefs would move, and how far.
	MovedBySort int    `json:"records_sortrefs_would_move"`
	MaxShift    int    `json:"sortrefs_max_shift"`
	SortErr     string `json:"sortrefs_error,omitempty"`
	// IDShapes are the container id shapes, as the pod objects and the entity events
	// write them.
	IDShapesObjects map[string]int `json:"container_id_shapes_in_pod_objects"`
	IDShapesCluster map[string]int `json:"container_id_shapes_in_entity_events"`
	DistinctObjects map[string]int `json:"distinct_container_ids_in_pod_objects_by_shape"`
	DistinctCluster map[string]int `json:"distinct_container_ids_in_entity_events_by_shape"`
	ReportIntervals map[string]int `json:"entity_report_intervals"`
	// Cross is the UID cross-check of the run: pod and node UIDs of the objects records
	// against the entity events, and the container ids of both.
	Cross []crossLine `json:"uid_cross_check"`
}

type crossLine struct {
	Kind        string `json:"kind"`
	Both        int    `json:"in_both"`
	OnlyObjects int    `json:"only_in_objects_records"`
	OnlyCluster int    `json:"only_in_entity_events"`
}

type pullReport struct {
	Run   int      `json:"run"`
	Line  int      `json:"line"`
	Size  pullSize `json:"size"`
	Limit int      `json:"receiver_limit_bytes"`
}

// passRunReport is what a pass made of one run.
type passRunReport struct {
	N          int           `json:"run"`
	Batches    int           `json:"batches"`
	Objects    sourceCounts  `json:"k8sobjects"`
	Cluster    sourceCounts  `json:"k8scluster"`
	Written    int           `json:"records_written_after_the_coalescer"`
	Subjects   []subjectSets `json:"subjects_written"`
	ErrExample []string      `json:"translate_error_examples,omitempty"`
	// Resume is what the resume step decided in the run, by counter.
	Resume map[string]int64 `json:"resume"`
}

// storeReport is what the pass's Pebble store counted: its checkpoints, under the
// store's default policy, and the lookups of the resume step.
type storeReport struct {
	CheckpointPolicy     string `json:"checkpoint_policy"`
	CheckpointsWritten   int64  `json:"checkpoints_written"`
	CheckpointsInvalid   int64  `json:"checkpoints_invalidated"`
	LookupIterators      int64  `json:"lookup_iterators"`
	LookupKeysWalkedMax  int64  `json:"lookup_keys_walked_max"`
	LookupExhausted      int64  `json:"lookup_exhausted"`
	ResumeFirstReadError string `json:"resume_first_read_error,omitempty"`
}

type sourceCounts struct {
	Translated int            `json:"records_translated"`
	Errors     int            `json:"translate_errors"`
	Skipped    map[string]int `json:"skipped"`
	Notes      map[string]int `json:"notes"`
}

type existenceReport struct {
	Subject       string        `json:"subject"`
	Windows       []*windowStat `json:"windows"`
	Total         windowStat    `json:"total"`
	LongestAbsent []span        `json:"longest_false_absent_spans"`
	LongestGhost  []span        `json:"longest_ghost_spans"`
}

func reportOf(ex *existence) existenceReport {
	return existenceReport{
		Subject: ex.Subject, Windows: ex.Windows, Total: ex.total(),
		LongestAbsent: ex.LongestAbsent, LongestGhost: ex.LongestGhost,
	}
}

type propertyReport struct {
	Invalid violation `json:"records_failing_validate"`
	Lapsed  violation `json:"observes_lapsing_before_observed_plus_ttl"`
}

type passReport struct {
	Name     string `json:"pass"`
	Restarts int    `json:"restarts"`
	// FlushRecords are the records the flushes wrote (informational): at each listing
	// sample, and in pass B at each restart.
	FlushRecords int             `json:"flush_records"`
	Runs         []passRunReport `json:"runs"`
	// Totals of subjects written, across the runs.
	Subjects    []subjectSets   `json:"subjects_written_total"`
	Pods        existenceReport `json:"pods_against_truth"`
	Predictions []prediction    `json:"predictions"`
	Ghosts      *ghostReport    `json:"ghosts"`
	Edges       *edgeReport     `json:"edges"`
	Nodes       existenceReport `json:"nodes_against_truth"`
	NodeLines   *nodeReport     `json:"node_lines"`
	Containers  containerCheck  `json:"container_fingerprints"`
	Properties  propertyReport  `json:"properties"`
	// Resume is what the resume step decided over the pass, by counter, and Store what
	// the Pebble store counted.
	Resume map[string]int64 `json:"resume"`
	Store  storeReport      `json:"pebble_store"`
	// Disagree are the pairs at which the two stores answered differently.
	Disagree struct {
		Pods  violation `json:"pods"`
		Nodes violation `json:"nodes"`
	} `json:"store_disagreements"`

	// podAlive is kept for the comparison of the passes.
	podAlive [][]bool
	podWin   []string
}

// passDiff is how the two passes differ.
type passDiff struct {
	// WrittenByRun is the records written by each pass per run.
	WrittenByRun []struct {
		Run int `json:"run"`
		A   int `json:"pass_a"`
		B   int `json:"pass_b"`
	} `json:"records_written_by_run"`
	PairsCompared int            `json:"pod_sample_pairs_compared"`
	PairsDiffer   int            `json:"pairs_that_differ"`
	AliveOnlyInA  int            `json:"alive_only_in_pass_a"`
	AliveOnlyInB  int            `json:"alive_only_in_pass_b"`
	ByWindow      map[string]int `json:"differing_pairs_by_window"`
	Examples      []string       `json:"examples,omitempty"`
}

type cadences struct {
	NodeWatchModified cadence `json:"node_watch_modified"`
	NodeClusterState  cadence `json:"node_cluster_state_events"`
}

// config says what to run.
type config struct {
	Capture string
	Tmp     string
	Keep    bool
	Margin  time.Duration
	// Progress, if not nil, gets a line for each step.
	Progress io.Writer
}

func (c config) say(format string, args ...any) {
	if c.Progress != nil {
		fmt.Fprintf(c.Progress, format+"\n", args...)
	}
}

// subjectsOf compares what the two translators wrote in a run's sets.
func subjectsOf(pr *passRun) []subjectSets {
	var out []subjectSets
	for k := range numSubjects {
		both, onlyO, onlyC := compareSets(pr.Subjects[srcObjects][k], pr.Subjects[srcCluster][k])
		out = append(out, subjectSets{Kind: subjectNames[k], Both: both, OnlyObjects: onlyO, OnlyCluster: onlyC})
	}
	return out
}

func countsOf(pr *passRun, s source) sourceCounts {
	return sourceCounts{Translated: pr.Translated[s], Errors: pr.Errors[s], Skipped: pr.Skips[s], Notes: pr.Notes[s]}
}

func passRunsOf(p *pass) ([]passRunReport, []subjectSets) {
	var runs []passRunReport
	// The totals across runs are computed over the union of each run's sets, which the
	// passRun keeps by fingerprint.
	union := [numSources][numSubjects]map[any]struct{}{}
	for s := range numSources {
		for k := range numSubjects {
			union[s][k] = map[any]struct{}{}
		}
	}
	for _, pr := range p.Runs {
		runs = append(runs, passRunReport{
			N: pr.N, Batches: pr.Batches, Objects: countsOf(pr, srcObjects), Cluster: countsOf(pr, srcCluster),
			Written: pr.Written, Subjects: subjectsOf(pr), ErrExample: pr.ErrExamples, Resume: pr.Resume.Counters(),
		})
		for s := range numSources {
			for k := range numSubjects {
				for fp := range pr.Subjects[s][k] {
					union[s][k][fp] = struct{}{}
				}
			}
		}
	}
	var tot []subjectSets
	for k := range numSubjects {
		var both, onlyO, onlyC int
		for fp := range union[srcObjects][k] {
			if _, ok := union[srcCluster][k][fp]; ok {
				both++
			} else {
				onlyO++
			}
		}
		for fp := range union[srcCluster][k] {
			if _, ok := union[srcObjects][k][fp]; !ok {
				onlyC++
			}
		}
		tot = append(tot, subjectSets{Kind: subjectNames[k], Both: both, OnlyObjects: onlyO, OnlyCluster: onlyC})
	}
	return runs, tot
}

// analyzePass asks the stores the questions of the truth and builds the pass's report.
func analyzePass(ctx context.Context, p *pass, pods *podsTruth, nodesT *nodesTruth, wl *workload, ws []window, stats []*runStats, margin time.Duration) (*passReport, error) {
	pr := &passReport{Name: p.Name, Restarts: p.Restarts, FlushRecords: p.FlushRecords}
	pr.Runs, pr.Subjects = passRunsOf(p)
	pr.Resume, pr.Store = resumeOf(p)

	pa, err := queryPods(ctx, p, pods, nodesT)
	if err != nil {
		return nil, fmt.Errorf("pass %s: pods: %w", p.Name, err)
	}
	pr.Disagree.Pods = pa.Disagree
	exP := analyze(podSeries(pods), pa.Alive, ws, margin)
	pr.Pods = reportOf(exP)
	pr.Predictions = predictions(p, pods, exP, wl, ws, stats)
	if pr.Ghosts, err = ghosts(p, pods, pa, ws, wl); err != nil {
		return nil, err
	}
	pr.Edges = edges(pods, pa, exP, ws)

	na, err := queryNodes(ctx, p, nodesT)
	if err != nil {
		return nil, fmt.Errorf("pass %s: nodes: %w", p.Name, err)
	}
	pr.Disagree.Nodes = na.Disagree
	exN := analyze(nodeSeries(nodesT), na.Alive, ws, margin)
	pr.Nodes = reportOf(exN)
	pr.NodeLines = nodes(nodesT, na, exN, wl)
	pr.Containers = containerChecks(p)
	pr.Properties = propertyReport{Invalid: p.Invalid, Lapsed: p.Lapsed}

	pr.podAlive = pa.Alive
	pr.podWin = make([]string, len(pods.Times))
	for i, t := range pods.Times {
		pr.podWin[i] = attribute(ws, t)
	}
	return pr, nil
}

// diffPasses compares the pods' existence in the two passes.
func diffPasses(a, b *passReport, pods *podsTruth) *passDiff {
	d := &passDiff{ByWindow: map[string]int{}}
	for i := range a.Runs {
		if i < len(b.Runs) {
			d.WrittenByRun = append(d.WrittenByRun, struct {
				Run int `json:"run"`
				A   int `json:"pass_a"`
				B   int `json:"pass_b"`
			}{a.Runs[i].N, a.Runs[i].Written, b.Runs[i].Written})
		}
	}
	for u := range a.podAlive {
		for s := range a.podAlive[u] {
			if pods.Unavail[s] {
				continue
			}
			d.PairsCompared++
			x, y := a.podAlive[u][s], b.podAlive[u][s]
			if x == y {
				continue
			}
			d.PairsDiffer++
			if x {
				d.AliveOnlyInA++
			} else {
				d.AliveOnlyInB++
			}
			d.ByWindow[a.podWin[s]]++
			if len(d.Examples) < maxExamples {
				pt := pods.Pods[pods.UIDs[u]]
				d.Examples = append(d.Examples, fmt.Sprintf("pod %s (%s) at %s: A %v, B %v", pt.UID, pt.Name, pods.Times[s].Format(time.RFC3339), x, y))
			}
		}
	}
	return d
}

func runReportOf(rs *runStats) runReport {
	rr := runReport{
		N: rs.N, Lines: rs.Lines, Bytes: rs.Bytes, Records: rs.Records, BySource: rs.BySource, Watch: rs.Watch,
		Entity: rs.Entity, Pulls: rs.Pulls, Observed: rs.Observed, Entities: rs.EntityTime,
		MovedBySort: rs.MovedBySort, MaxShift: rs.MaxShift, SortErr: rs.SortErr,
		IDShapesObjects: rs.IDShapes[srcObjects], IDShapesCluster: rs.IDShapes[srcCluster],
		DistinctObjects: map[string]int{}, DistinctCluster: map[string]int{}, ReportIntervals: rs.ReportIntervals,
	}
	for sh, m := range rs.DistinctIDShapes[srcObjects] {
		rr.DistinctObjects[sh] = len(m)
	}
	for sh, m := range rs.DistinctIDShapes[srcCluster] {
		rr.DistinctCluster[sh] = len(m)
	}
	cross := func(kind string, a, b map[string]struct{}) {
		var l crossLine
		l.Kind = kind
		for k := range a {
			if _, ok := b[k]; ok {
				l.Both++
			} else {
				l.OnlyObjects++
			}
		}
		for k := range b {
			if _, ok := a[k]; !ok {
				l.OnlyCluster++
			}
		}
		rr.Cross = append(rr.Cross, l)
	}
	cross("pod uid", rs.PodUIDs[srcObjects], rs.PodUIDs[srcCluster])
	cross("node uid", rs.NodeUIDs[srcObjects], rs.NodeUIDs[srcCluster])
	cross("container id", rs.CtrIDs[srcObjects], rs.CtrIDs[srcCluster])
	return rr
}

// blocking lists the breaches of a decided rule in a pass.
func blocking(pr *passReport) []string {
	var out []string
	if e := pr.Edges; e.AfterPodGone.Count > 0 {
		out = append(out, fmt.Sprintf("pass %s: edge-after-pod: %d (T, pod) pairs outside the margin with the pod absent in the truth and not alive in the store, yet an edge of it alive (longest %.1f min); first: %v",
			pr.Name, e.AfterPodGone.Count, e.AfterPodGoneLongestMin, e.AfterPodGone.Examples))
	}
	if g := pr.Ghosts; g.OverBound.Count > 0 {
		out = append(out, fmt.Sprintf("pass %s: %d ghosts outlived their producers' last assertion plus TTL plus 60 s; first: %v", pr.Name, g.OverBound.Count, g.OverBound.Examples))
	}
	if c := pr.Containers; c.SharedOnlyObjects+c.SharedOnlyCluster > 0 {
		out = append(out, fmt.Sprintf("pass %s: the translators disagree on the container fingerprints of pods both wrote: %d only in k8sobjects, %d only in k8scluster (of %d and %d shared-pod containers)",
			pr.Name, c.SharedOnlyObjects, c.SharedOnlyCluster, c.SharedBoth+c.SharedOnlyObjects, c.SharedBoth+c.SharedOnlyCluster))
	}
	if l := pr.Properties.Lapsed; l.Count > 0 {
		out = append(out, fmt.Sprintf("pass %s: %d Observes lapse before their observed time plus TTL; first: %v", pr.Name, l.Count, l.Examples))
	}
	return out
}

// findings lists what else the replay found that a reader should see.
func findings(pr *passReport) []string {
	var out []string
	if v := pr.Properties.Invalid; v.Count > 0 {
		out = append(out, fmt.Sprintf("pass %s: %d records fail Validate; first: %v", pr.Name, v.Count, v.Examples))
	}
	for _, r := range pr.Runs {
		for _, c := range []struct {
			n string
			s sourceCounts
		}{{"k8sobjects", r.Objects}, {"k8scluster", r.Cluster}} {
			if c.s.Errors > 0 {
				out = append(out, fmt.Sprintf("pass %s run %d: %s could not read %d records; first: %v", pr.Name, r.N, c.n, c.s.Errors, r.ErrExample))
			}
		}
	}
	return out
}

// selfChecks lists the self-checks of a pass that failed.
func selfChecks(pr *passReport) []string {
	var out []string
	if v := pr.Disagree.Pods; v.Count > 0 {
		out = append(out, fmt.Sprintf("pass %s: the memory store and the Pebble store answered differently for %d pod pairs; first: %v", pr.Name, v.Count, v.Examples))
	}
	if v := pr.Disagree.Nodes; v.Count > 0 {
		out = append(out, fmt.Sprintf("pass %s: the memory store and the Pebble store answered differently for %d node pairs; first: %v", pr.Name, v.Count, v.Examples))
	}
	return out
}

// sortedKeys is the keys of a map in order.
func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// resumeOf is the resume step's counts over the pass, and what the pass's Pebble store
// counted (nothing for a pass over a store without a recorder).
func resumeOf(p *pass) (map[string]int64, storeReport) {
	var st resume.Stats
	for _, pr := range p.Runs {
		st.Add(pr.Resume)
	}
	c := pebblestore.DefaultCheckpoints()
	sr := storeReport{CheckpointPolicy: fmt.Sprintf("the store's default: on, %d records between checkpoints (alpha %g), lag %s", c.KMin, c.Alpha, c.Lag)}
	if st.FirstError != nil {
		sr.ResumeFirstReadError = st.FirstError.Error()
	}
	if p.rec != nil {
		sr.CheckpointsWritten = p.rec.sum("checkpoint.written")
		sr.CheckpointsInvalid = p.rec.sum("checkpoint.invalidated")
		sr.LookupIterators = p.rec.sum("lookup.iterators")
		sr.LookupKeysWalkedMax = p.rec.max("lookup.keys_walked")
		sr.LookupExhausted = p.rec.sum("lookup.exhausted")
	}
	return st.Counters(), sr
}
