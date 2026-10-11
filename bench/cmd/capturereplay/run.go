package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/lotannauo/toposhift/internal/coalesce"
)

// capture layout: the run files, and the logs beside them.
const (
	runFiles = 5
)

func runPath(dir string, n int) string {
	return filepath.Join(dir, "out", fmt.Sprintf("run%d.jsonl", n))
}

func logPath(dir, name string) string { return filepath.Join(dir, "logs", name) }

// parseSizes reads sizes.txt: lines of "runN bytes".
func parseSizes(data string) (map[int]int64, error) {
	out := map[int]int64{}
	for _, line := range strings.Split(data, "\n") {
		f := strings.Fields(line)
		if len(f) == 0 {
			continue
		}
		if len(f) != 2 || !strings.HasPrefix(f[0], "run") {
			return nil, fmt.Errorf("sizes: unreadable line %q", line)
		}
		n, err := strconv.Atoi(strings.TrimPrefix(f[0], "run"))
		if err != nil {
			return nil, fmt.Errorf("sizes: %q: %w", line, err)
		}
		b, err := strconv.ParseInt(f[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("sizes: %q: %w", line, err)
		}
		out[n] = b
	}
	return out, nil
}

// checkSizes compares each run file's size with sizes.txt and with the bytes the workload
// log says the collector wrote. A difference means the capture is not the one the
// timeline describes.
func checkSizes(paths []string, sizes map[int]int64, wl *workload) []string {
	var bad []string
	for i, path := range paths {
		n := i + 1
		st, err := os.Stat(path)
		if err != nil {
			bad = append(bad, err.Error())
			continue
		}
		if want, ok := sizes[n]; !ok {
			bad = append(bad, fmt.Sprintf("run%d: sizes.txt has no size", n))
		} else if st.Size() != want {
			bad = append(bad, fmt.Sprintf("run%d.jsonl is %d bytes, sizes.txt says %d", n, st.Size(), want))
		}
		for _, r := range wl.Runs {
			if r.N == n && r.HasStop && r.Bytes != st.Size() {
				bad = append(bad, fmt.Sprintf("run%d.jsonl is %d bytes, the workload log says the collector stopped at %d", n, st.Size(), r.Bytes))
			}
		}
	}
	return bad
}

// checkLines compares the lines of each run file with the workload log's.
func checkLines(files []*runFile, wl *workload) []string {
	var bad []string
	for _, rf := range files {
		for _, r := range wl.Runs {
			if r.N == rf.N && r.HasStop && r.Lines != len(rf.Lines) {
				bad = append(bad, fmt.Sprintf("run%d.jsonl has %d lines, the workload log says the collector wrote %d", rf.N, len(rf.Lines), r.Lines))
			}
		}
	}
	return bad
}

// largestGap is the largest gap between consecutive times, which are in order.
func largestGap(times []time.Time) time.Duration {
	var gap time.Duration
	for i := 1; i < len(times); i++ {
		gap = max(gap, times[i].Sub(times[i-1]))
	}
	return gap
}

func openParse[T any](path string, parse func(*os.File) (T, error)) (T, error) {
	var zero T
	f, err := os.Open(path)
	if err != nil {
		return zero, err
	}
	v, err := parse(f)
	return v, errors.Join(err, f.Close())
}

// build replays the capture and compares it with the truth.
func build(ctx context.Context, cfg config) (_ *Report, err error) {
	say := cfg.say
	wl, err := openParse(logPath(cfg.Capture, "workload.txt"), func(f *os.File) (*workload, error) { return parseWorkload(f) })
	if err != nil {
		return nil, err
	}
	pods, err := openParse(logPath(cfg.Capture, "truth-pods.tsv"), func(f *os.File) (*podsTruth, error) { return parsePods(f) })
	if err != nil {
		return nil, fmt.Errorf("truth-pods.tsv: %w", err)
	}
	nodesT, err := openParse(logPath(cfg.Capture, "truth-nodes.tsv"), func(f *os.File) (*nodesTruth, error) { return parseNodes(f) })
	if err != nil {
		return nil, fmt.Errorf("truth-nodes.tsv: %w", err)
	}
	sizesData, err := os.ReadFile(logPath(cfg.Capture, "sizes.txt"))
	if err != nil {
		return nil, err
	}
	sizes, err := parseSizes(string(sizesData))
	if err != nil {
		return nil, err
	}
	var paths []string
	for n := 1; n <= runFiles; n++ {
		paths = append(paths, runPath(cfg.Capture, n))
	}
	if bad := checkSizes(paths, sizes, wl); len(bad) > 0 {
		return nil, fmt.Errorf("the capture is not the one the logs describe: %s", strings.Join(bad, "; "))
	}
	say("truth: %d pod samples (%d uids), %d node samples (%d uids)", len(pods.Times), len(pods.UIDs), len(nodesT.Times), len(nodesT.UIDs))

	rep, err := replayRuns(ctx, paths, replayOptions{Tmp: cfg.Tmp, Keep: cfg.Keep, Progress: cfg.Progress, FlushTimes: pods.Times})
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, rep.close()) }()

	r := &Report{
		Capture: cfg.Capture, Generated: time.Now().UTC(), Elapsed: rep.Elapsed.Round(time.Second).String(),
		Margin: cfg.Margin.String(),
		Truth: truthSummary{
			PodSamples: len(pods.Times), PodUIDs: len(pods.UIDs), NodeSamples: len(nodesT.Times), NodeUIDs: len(nodesT.UIDs),
		},
	}
	for _, u := range pods.Unavail {
		if u {
			r.Truth.PodUnavailable++
		}
	}
	gap := largestGap(pods.Times)
	r.Truth.PodSampleLargestGap = gap.String()
	if r.Truth.FlushGapOverLead = gap > coalesce.FlushLead; r.Truth.FlushGapOverLead {
		r.Findings = append(r.Findings, fmt.Sprintf("the largest gap between pod listing samples, %s, is more than the flush lead, %s: the coalescer is flushed less often than its guarantee needs", gap, coalesce.FlushLead))
	}
	for _, u := range nodesT.Unavail {
		if u {
			r.Truth.NodeUnavailable++
		}
	}
	r.Failures = append(r.Failures, checkLines(rep.Files, wl)...)
	for _, rs := range rep.Stats {
		r.Runs = append(r.Runs, runReportOf(rs))
	}
	ps, run, line, err := rep.largestPull()
	if err != nil {
		r.Findings = append(r.Findings, "largest pull: "+err.Error())
	} else {
		r.Pull = &pullReport{Run: run, Line: line, Size: ps, Limit: receiverLimit}
	}

	ws := wl.windows()
	r.Windows = ws
	start := time.Now()
	for _, p := range []*pass{rep.A, rep.B} {
		say("analysing pass %s", p.Name)
		pr, err := analyzePass(ctx, p, pods, nodesT, wl, ws, rep.Stats, cfg.Margin)
		if err != nil {
			return nil, err
		}
		r.Passes = append(r.Passes, pr)
		r.Blocking = append(r.Blocking, blocking(pr)...)
		r.Findings = append(r.Findings, findings(pr)...)
		r.Failures = append(r.Failures, selfChecks(pr)...)
	}
	r.Diff = diffPasses(r.Passes[0], r.Passes[1], pods)
	r.AnalysisElapsed = time.Since(start).Round(time.Second).String()

	var modified, state []map[string][]time.Time
	for _, rs := range rep.Stats {
		modified = append(modified, rs.NodeModified)
		state = append(state, rs.NodeState)
	}
	nodeName := func(uid string) string {
		if n := nodesT.Nodes[uid]; n != nil {
			return n.Name
		}
		return "?"
	}
	r.Cadences = cadences{NodeWatchModified: cadenceOf(modified, nodeName), NodeClusterState: cadenceOf(state, nodeName)}
	r.Alignment = clockAlignment(pods, rep.Stats, wl)
	r.Waiting = waitingContainers(pods)
	return r, nil
}
