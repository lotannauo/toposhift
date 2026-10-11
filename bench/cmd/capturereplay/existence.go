package main

import (
	"sort"
	"time"
)

// Existence against the truth: for every (sample, UID) pair, does the store agree with
// the cluster about whether the UID exists?

// verdict is how a pair came out.
type verdict uint8

const (
	// vAgree: the store and the truth say the same.
	vAgree verdict = iota
	// vFalseAbsent: the truth lists the UID and the store does not have it alive.
	vFalseAbsent
	// vGhost: the truth does not list the UID and the store has it alive.
	vGhost
	// vSkipped: the pair is within the margin of the UID's first or last presence in the
	// truth, where 30-second samples on the host's clock cannot say.
	vSkipped
)

// withinMargin says whether t is within margin of the first or the last presence.
func withinMargin(t, first, last time.Time, margin time.Duration) bool {
	return absDur(t.Sub(first)) <= margin || absDur(t.Sub(last)) <= margin
}

func absDur(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

// classifyPair classifies one pair. present is whether the truth lists the UID at t,
// alive whether the store has it alive, and first and last are the times of the first
// and the last sample that list it.
func classifyPair(present, alive bool, t, first, last time.Time, margin time.Duration) verdict {
	if withinMargin(t, first, last, margin) {
		return vSkipped
	}
	switch {
	case present && !alive:
		return vFalseAbsent
	case !present && alive:
		return vGhost
	}
	return vAgree
}

// windowStat is what the pairs of one window came to.
type windowStat struct {
	Name  string    `json:"name"`
	Start time.Time `json:"start,omitzero"`
	End   time.Time `json:"end,omitzero"`

	Agree       int `json:"agree"`
	FalseAbsent int `json:"false_absent"`
	Ghost       int `json:"ghost"`
	Skipped     int `json:"skipped_by_margin"`

	// AbsentMin is the minutes, summed over pods, that a pod that was in the cluster
	// when the window began was in the truth and not alive in the store; LongestAbsentMin
	// is the longest unbroken stretch of any one of them, and AbsentPods how many pods had any.
	AbsentMin        float64 `json:"absent_pod_minutes"`
	LongestAbsentMin float64 `json:"longest_absent_minutes"`
	AbsentPods       int     `json:"absent_pods"`
	// The same for pods created after the window began: the collector could not have seen
	// them before its next pull.
	AbsentNewMin        float64 `json:"absent_new_pod_minutes"`
	LongestAbsentNewMin float64 `json:"longest_absent_new_minutes"`
	AbsentNewPods       int     `json:"absent_new_pods"`
	// GhostMin is the pod-minutes the store had alive a UID the truth no longer lists.
	GhostMin        float64 `json:"ghost_pod_minutes"`
	LongestGhostMin float64 `json:"longest_ghost_minutes"`
	GhostPods       int     `json:"ghost_pods"`

	absentSet, absentNewSet, ghostSet map[string]struct{}
}

func newWindowStat(w window) *windowStat {
	return &windowStat{
		Name: w.Name, Start: w.Start, End: w.End,
		absentSet: map[string]struct{}{}, absentNewSet: map[string]struct{}{}, ghostSet: map[string]struct{}{},
	}
}

// span is a stretch of consecutive samples a pair stayed in one verdict.
type span struct {
	UID     string    `json:"uid"`
	Name    string    `json:"name,omitempty"`
	Window  string    `json:"window"`
	From    time.Time `json:"from"`
	Minutes float64   `json:"minutes"`
	New     bool      `json:"created_in_window,omitempty"`
}

// existence is the verdicts of one kind of subject (pods, or nodes) in one pass.
type existence struct {
	Subject string
	Windows []*windowStat
	byName  map[string]*windowStat
	// Longest are the longest false-absent and ghost spans.
	LongestAbsent []span
	LongestGhost  []span
	// Verdicts are by UID index, then by sample.
	Verdicts [][]verdict
}

// seriesOf is a truth file seen as what existence needs of it.
type series struct {
	Subject string
	samples samples
	UIDs    []string
	Names   []string
	Present [][]bool
	First   []int
	Last    []int
}

const maxSpans = 8

// analyze classifies every pair of the series against the stores' answers, and sums
// the pairs per window. alive is indexed like the series.
func analyze(sr series, alive [][]bool, ws []window, margin time.Duration) *existence {
	ex := &existence{Subject: sr.Subject, byName: map[string]*windowStat{}, Verdicts: make([][]verdict, len(sr.UIDs))}
	get := func(name string) *windowStat {
		if w, ok := ex.byName[name]; ok {
			return w
		}
		w := newWindowStat(window{Name: name})
		ex.byName[name] = w
		ex.Windows = append(ex.Windows, w)
		return w
	}
	for _, w := range ws {
		st := newWindowStat(w)
		ex.byName[w.Name] = st
		ex.Windows = append(ex.Windows, st)
	}
	get(windowSteady)
	// A sample's window depends on its time alone.
	winOf := make([]*windowStat, len(sr.samples.Times))
	for i, t := range sr.samples.Times {
		winOf[i] = get(attribute(ws, t))
	}
	spacing := sr.samples.spacing()

	for u, uid := range sr.UIDs {
		verd := make([]verdict, len(sr.samples.Times))
		ex.Verdicts[u] = verd
		firstT, lastT := sr.samples.Times[sr.First[u]], sr.samples.Times[sr.Last[u]]
		var run struct {
			active bool
			kind   verdict
			win    *windowStat
			from   time.Time
			min    float64
			isNew  bool
		}
		flush := func() {
			if !run.active {
				return
			}
			sp := span{UID: uid, Name: sr.Names[u], Window: run.win.Name, From: run.from, Minutes: run.min, New: run.isNew}
			switch run.kind {
			case vFalseAbsent:
				if run.isNew {
					run.win.LongestAbsentNewMin = max(run.win.LongestAbsentNewMin, run.min)
				} else {
					run.win.LongestAbsentMin = max(run.win.LongestAbsentMin, run.min)
				}
				ex.LongestAbsent = keepLongest(ex.LongestAbsent, sp)
			case vGhost:
				run.win.LongestGhostMin = max(run.win.LongestGhostMin, run.min)
				ex.LongestGhost = keepLongest(ex.LongestGhost, sp)
			}
			run.active = false
		}
		for s, t := range sr.samples.Times {
			if sr.samples.Unavail[s] {
				flush()
				continue
			}
			w := winOf[s]
			v := classifyPair(sr.Present[u][s], alive[u][s], t, firstT, lastT, margin)
			verd[s] = v
			isNew := !w.Start.IsZero() && !firstT.Before(w.Start)
			mins := spacing[s].Minutes()
			switch v {
			case vAgree:
				w.Agree++
			case vSkipped:
				w.Skipped++
			case vFalseAbsent:
				w.FalseAbsent++
				if isNew {
					w.AbsentNewMin += mins
					w.absentNewSet[uid] = struct{}{}
				} else {
					w.AbsentMin += mins
					w.absentSet[uid] = struct{}{}
				}
			case vGhost:
				w.Ghost++
				w.GhostMin += mins
				w.ghostSet[uid] = struct{}{}
			}
			if v == vFalseAbsent || v == vGhost {
				if run.active && (run.kind != v || run.win != w || run.isNew != isNew) {
					flush()
				}
				if !run.active {
					run.active, run.kind, run.win, run.from, run.min, run.isNew = true, v, w, t, 0, isNew
				}
				run.min += mins
			} else {
				flush()
			}
		}
		flush()
	}
	for _, w := range ex.Windows {
		w.AbsentPods, w.AbsentNewPods, w.GhostPods = len(w.absentSet), len(w.absentNewSet), len(w.ghostSet)
	}
	return ex
}

// keepLongest keeps the longest few spans, longest first.
func keepLongest(list []span, sp span) []span {
	list = append(list, sp)
	sort.SliceStable(list, func(i, j int) bool { return list[i].Minutes > list[j].Minutes })
	if len(list) > maxSpans {
		list = list[:maxSpans]
	}
	return list
}

// window returns the stats of a window by name, or nil.
func (e *existence) window(name string) *windowStat { return e.byName[name] }

// total sums the windows.
func (e *existence) total() windowStat {
	var t windowStat
	t.Name = "total"
	for _, w := range e.Windows {
		t.Agree += w.Agree
		t.FalseAbsent += w.FalseAbsent
		t.Ghost += w.Ghost
		t.Skipped += w.Skipped
		t.AbsentMin += w.AbsentMin
		t.AbsentNewMin += w.AbsentNewMin
		t.GhostMin += w.GhostMin
		t.LongestAbsentMin = max(t.LongestAbsentMin, w.LongestAbsentMin)
		t.LongestAbsentNewMin = max(t.LongestAbsentNewMin, w.LongestAbsentNewMin)
		t.LongestGhostMin = max(t.LongestGhostMin, w.LongestGhostMin)
		t.AbsentPods += w.AbsentPods
		t.AbsentNewPods += w.AbsentNewPods
		t.GhostPods += w.GhostPods
	}
	return t
}
