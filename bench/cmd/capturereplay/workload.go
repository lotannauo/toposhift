package main

import (
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// The workload log is the timeline the capture script kept: when each collector run
// started and stopped, which pods it deleted at each stop and each API restart, and what
// it did to the nodes and to the replica count. Only the lines that begin with a time
// carry an event; the output of the commands between them is not read.

// Windows the analysis attributes samples to.
const (
	windowSteady   = "steady state"
	windowNode     = "node scenario"
	windowFullPull = "400-pod phase"
	windowTeardown = "teardown"
)

// stopSettle is how long after a new collector run starts a stop's window lasts: the TTL
// of a pull, 15 minutes.
const stopSettle = 15 * time.Minute

// apiSettle is how long after an API restart's deletes its window lasts.
const apiSettle = 10 * time.Minute

// agentSettle is how long after agent-2 is back the node scenario lasts.
const agentSettle = 15 * time.Minute

// runSpan is one collector run.
type runSpan struct {
	N        int
	Start    time.Time // the line that says it started
	Stop     time.Time // the line that says it stopped
	T0       int64
	Lines    int
	Bytes    int64
	HasStart bool
	HasStop  bool
}

// stopEvent is one deliberate stop of the collector: S5, S12-early, S12-late, S20.
type stopEvent struct {
	Name    string
	Start   time.Time // its "starts" line, which follows the collector's stop
	Deleted time.Time // the line that says what was deleted
	Pods    []string  // namespace-less names of the pods deleted
}

// apiRestart is one restart of the API server, with the pods deleted as soon as it was
// ready again.
type apiRestart struct {
	Label   string // as the log says it; the log's label is not unique
	Time    time.Time
	Pods    []string
	Ordinal int // 1, 2, 3 in time order
}

// workload is what the log says.
type workload struct {
	Runs        []runSpan
	BaselineB   time.Time
	BaselineEnd time.Time
	Stops       []stopEvent
	API         []apiRestart
	Agent2Stop  time.Time
	Agent2Back  time.Time
	OldUID      string
	NewUID      string
	// FirstScaleUp is the first scale of the deployment above its steady size, in the
	// full-pull phase (to 100), and Scale400 the line that says 400 are running; ScaleDown
	// is the scale back to 40 after it.
	FirstScaleUp time.Time
	Running400   time.Time
	ScaleDown    time.Time
	Teardown     time.Time
	// Named are the pods the log names by a delete or an eviction: name -> time.
	Named map[string]time.Time
}

var (
	reLine     = regexp.MustCompile(`^(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ) (.*)$`)
	reRunStart = regexp.MustCompile(`^collector run (\d+) started, T0=(\d+)`)
	reRunStop  = regexp.MustCompile(`^collector run (\d+) stopped: (\d+) lines, (\d+) bytes`)
	reStop     = regexp.MustCompile(`^(S\d+(?:-\w+)?) starts$`)
	reDeleted  = regexp.MustCompile(`^deleted \d+ web pods \((S\d+(?:-\w+)?)\):(.*)$`)
	reAPI      = regexp.MustCompile(`^(API restart \d+): ready, deleted \d+ pods at once:(.*)$`)
	reAgentOld = regexp.MustCompile(`^agent 2 stopped \(node uid ([0-9a-f-]+)\)`)
	reAgentNew = regexp.MustCompile(`^agent 2 back with uid ([0-9a-f-]+)`)
	reScale    = regexp.MustCompile(`^kubectl scale deployment web --replicas=(\d+)`)
	reDelPod   = regexp.MustCompile(`^kubectl delete pod (\S+)`)
	reEvicted  = regexp.MustCompile(`^evicted (\S+)`)
)

func podNames(list string) []string {
	var out []string
	for _, f := range strings.Fields(list) {
		out = append(out, strings.TrimPrefix(f, "pod/"))
	}
	return out
}

func (w *workload) run(n int) *runSpan {
	for i := range w.Runs {
		if w.Runs[i].N == n {
			return &w.Runs[i]
		}
	}
	w.Runs = append(w.Runs, runSpan{N: n})
	return &w.Runs[len(w.Runs)-1]
}

// parseWorkload reads the log. The log labels every API restart "API restart 3", because
// the script's loop variable was overwritten; the restarts are told apart by the order
// of their lines, and Ordinal is that order.
func parseWorkload(r io.Reader) (*workload, error) {
	w := &workload{Named: map[string]time.Time{}}
	scaledUp := false
	err := lines(r, func(n int, line string) error {
		m := reLine.FindStringSubmatch(line)
		if m == nil {
			return nil
		}
		t, err := time.Parse(time.RFC3339, m[1])
		if err != nil {
			return fmt.Errorf("line %d: %w", n, err)
		}
		t = t.UTC()
		msg := m[2]
		switch {
		case reRunStart.MatchString(msg):
			g := reRunStart.FindStringSubmatch(msg)
			rn, _ := strconv.Atoi(g[1])
			t0, _ := strconv.ParseInt(g[2], 10, 64)
			rs := w.run(rn)
			rs.Start, rs.T0, rs.HasStart = t, t0, true
		case reRunStop.MatchString(msg):
			g := reRunStop.FindStringSubmatch(msg)
			rn, _ := strconv.Atoi(g[1])
			rs := w.run(rn)
			rs.Stop, rs.HasStop = t, true
			rs.Lines, _ = strconv.Atoi(g[2])
			rs.Bytes, _ = strconv.ParseInt(g[3], 10, 64)
		case strings.HasPrefix(msg, "baseline starts"):
			w.BaselineB = t
		case msg == "baseline ends":
			w.BaselineEnd = t
		case reStop.MatchString(msg):
			w.Stops = append(w.Stops, stopEvent{Name: reStop.FindStringSubmatch(msg)[1], Start: t})
		case reDeleted.MatchString(msg):
			g := reDeleted.FindStringSubmatch(msg)
			found := false
			for i := range w.Stops {
				if w.Stops[i].Name == g[1] {
					w.Stops[i].Deleted, w.Stops[i].Pods = t, podNames(g[2])
					found = true
				}
			}
			if !found {
				return fmt.Errorf("line %d: deletes for %s, which has no starts line", n, g[1])
			}
		case reAPI.MatchString(msg):
			g := reAPI.FindStringSubmatch(msg)
			w.API = append(w.API, apiRestart{Label: g[1], Time: t, Pods: podNames(g[2]), Ordinal: len(w.API) + 1})
		case reAgentOld.MatchString(msg):
			w.Agent2Stop, w.OldUID = t, reAgentOld.FindStringSubmatch(msg)[1]
		case reAgentNew.MatchString(msg):
			w.Agent2Back, w.NewUID = t, reAgentNew.FindStringSubmatch(msg)[1]
		case reScale.MatchString(msg):
			size, _ := strconv.Atoi(reScale.FindStringSubmatch(msg)[1])
			switch {
			case size >= 100 && w.FirstScaleUp.IsZero():
				w.FirstScaleUp, scaledUp = t, true
			case size == 40 && scaledUp && w.ScaleDown.IsZero() && !w.Running400.IsZero():
				w.ScaleDown = t
			}
		case msg == "web=400 Running":
			w.Running400 = t
		case strings.HasPrefix(msg, "kubectl delete deployment web"):
			w.Teardown = t
		case reDelPod.MatchString(msg):
			w.Named[reDelPod.FindStringSubmatch(msg)[1]] = t
		case reEvicted.MatchString(msg):
			w.Named[reEvicted.FindStringSubmatch(msg)[1]] = t
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	// Runs in order of their number.
	for i := 1; i < len(w.Runs); i++ {
		for j := i; j > 0 && w.Runs[j].N < w.Runs[j-1].N; j-- {
			w.Runs[j], w.Runs[j-1] = w.Runs[j-1], w.Runs[j]
		}
	}
	return w, nil
}

// window is a stretch of the capture the samples are attributed to.
type window struct {
	Name  string    `json:"name"`
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
	// StopOf is set for the windows of a collector stop: the number of the run that
	// stopped, so that its last pull and the next run's first can be found.
	StopOf int `json:"stop_of_run,omitempty"`
}

// farFuture is the end of a window that lasts to the end of the capture.
var farFuture = time.Date(2200, 1, 1, 0, 0, 0, 0, time.UTC)

// windows lists the windows of the workload. A sample belongs to the one of them that
// started latest among those it falls in (the API restarts' windows overlap), or to the
// steady state if none.
func (w *workload) windows() []window {
	var out []window
	for _, s := range w.Stops {
		end := farFuture
		for _, r := range w.Runs {
			if r.HasStart && r.Start.After(s.Start) {
				end = r.Start.Add(stopSettle)
				break
			}
		}
		// The collector run that stopped just before the starts line.
		stopped := 0
		for _, r := range w.Runs {
			if r.HasStop && !r.Stop.After(s.Start) {
				stopped = r.N
			}
		}
		out = append(out, window{Name: s.Name, Start: s.Start, End: end, StopOf: stopped})
	}
	for _, a := range w.API {
		out = append(out, window{Name: fmt.Sprintf("API restart %d", a.Ordinal), Start: a.Time, End: a.Time.Add(apiSettle)})
	}
	if !w.Agent2Stop.IsZero() && !w.Agent2Back.IsZero() {
		out = append(out, window{Name: windowNode, Start: w.Agent2Stop, End: w.Agent2Back.Add(agentSettle)})
	}
	if !w.FirstScaleUp.IsZero() {
		end := w.Teardown
		if end.IsZero() {
			end = farFuture
		}
		out = append(out, window{Name: windowFullPull, Start: w.FirstScaleUp, End: end})
	}
	if !w.Teardown.IsZero() {
		out = append(out, window{Name: windowTeardown, Start: w.Teardown, End: farFuture})
	}
	return out
}

// attribute names the window a time falls in.
func attribute(ws []window, t time.Time) string {
	best := -1
	for i, wd := range ws {
		if t.Before(wd.Start) || !t.Before(wd.End) {
			continue
		}
		if best < 0 || wd.Start.After(ws[best].Start) {
			best = i
		}
	}
	if best < 0 {
		return windowSteady
	}
	return ws[best].Name
}
