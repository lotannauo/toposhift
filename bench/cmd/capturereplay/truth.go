package main

import (
	"bufio"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"
)

// The truth files are what the host saw of the cluster while the capture ran: every
// 30 seconds, the pods and the nodes the API listed. A pod that the API lists
// (Terminating included) is present; deletion requested is not deletion done.

// unavailableMark is what a sampler wrote for a sample taken while the API was down.
const unavailableMark = "unavailable"

// samples are the times of the samples of one truth file, in order, and which of them
// were taken while the API was unavailable (such a sample says nothing).
type samples struct {
	Times   []time.Time
	Unavail []bool
}

// podRow is what one sample said of one pod.
type podRow struct {
	Sample   int
	Node     string
	Phase    string
	Deletion string
	// CIDs are the container ids as listed, separated by spaces; empty if none has one yet.
	CIDs     string
	Restarts string
}

// podTruth is one pod UID's presence over the samples.
type podTruth struct {
	UID  string
	Name string // namespace/name
	// Present is true at the samples whose list held the UID.
	Present []bool
	// Rows are the samples that held it, in order.
	Rows []podRow
	// First and Last are the indexes of the first and last sample that held it.
	First, Last int
}

// podsTruth is truth-pods.tsv.
type podsTruth struct {
	samples
	// Pods is by UID; UIDs is the same keys, in order of first presence.
	Pods map[string]*podTruth
	UIDs []string
	// ByName lists the UIDs that bore a namespace/name, in order of first presence.
	ByName map[string][]string
}

// nodeTruth is one node UID's presence over the node samples.
type nodeTruth struct {
	UID, Name   string
	Present     []bool
	Ready       []string // by sample; empty where absent
	First, Last int
}

// nodesTruth is truth-nodes.tsv.
type nodesTruth struct {
	samples
	Nodes map[string]*nodeTruth
	UIDs  []string
	// UIDAt[name][i] is the UID that bore the name at sample i, or "" if no node did.
	UIDAt map[string][]string
}

// sampleReader numbers the samples of a tab-separated truth file: a line belongs to the
// sample of its first field, and a sample with nothing in it has one line of its time
// alone (or the time and the unavailable mark).
type sampleReader struct {
	s        samples
	lastTime string
	cur      int
}

// next takes the fields of a line, and says whether it opens a new sample, and whether
// the line is only the mark of an empty or unavailable sample.
func (sr *sampleReader) next(f []string, line int) (marker bool, err error) {
	t, err := time.Parse(time.RFC3339, f[0])
	if err != nil {
		return false, fmt.Errorf("line %d: sample time %q: %w", line, f[0], err)
	}
	t = t.UTC()
	if f[0] != sr.lastTime {
		if n := len(sr.s.Times); n > 0 && !t.After(sr.s.Times[n-1]) {
			return false, fmt.Errorf("line %d: sample time %s is not after the sample before it", line, f[0])
		}
		sr.s.Times = append(sr.s.Times, t)
		sr.s.Unavail = append(sr.s.Unavail, false)
		sr.lastTime = f[0]
		sr.cur = len(sr.s.Times) - 1
	}
	switch {
	case len(f) == 1, len(f) == 2 && f[1] == "":
		return true, nil
	case len(f) == 2 && f[1] == unavailableMark:
		sr.s.Unavail[sr.cur] = true
		return true, nil
	}
	return false, nil
}

func lines(r io.Reader, visit func(n int, line string) error) error {
	br := bufio.NewReaderSize(r, 1<<20)
	for n := 1; ; n++ {
		line, err := br.ReadString('\n')
		if line != "" {
			if verr := visit(n, strings.TrimRight(line, "\r\n")); verr != nil {
				return verr
			}
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

// parsePods reads truth-pods.tsv: sample time, uid, namespace/name, node, phase,
// deletionTimestamp, container ids (space separated), restart counts.
func parsePods(r io.Reader) (*podsTruth, error) {
	var sr sampleReader
	pt := &podsTruth{Pods: map[string]*podTruth{}, ByName: map[string][]string{}}
	seen := map[string]int{} // uid -> the sample it was last listed in
	err := lines(r, func(n int, line string) error {
		if line == "" {
			return fmt.Errorf("line %d: empty", n)
		}
		f := strings.Split(line, "\t")
		marker, err := sr.next(f, n)
		if err != nil {
			return err
		}
		if marker {
			return nil
		}
		if len(f) != 8 {
			return fmt.Errorf("line %d: %d fields, want 8", n, len(f))
		}
		uid := f[1]
		if uid == "" {
			return fmt.Errorf("line %d: no uid", n)
		}
		if s, dup := seen[uid]; dup && s == sr.cur {
			return fmt.Errorf("line %d: uid %s listed twice in one sample", n, uid)
		}
		seen[uid] = sr.cur
		p := pt.Pods[uid]
		if p == nil {
			p = &podTruth{UID: uid, Name: f[2], First: sr.cur}
			pt.Pods[uid] = p
			pt.UIDs = append(pt.UIDs, uid)
			pt.ByName[f[2]] = append(pt.ByName[f[2]], uid)
		}
		p.Last = sr.cur
		p.Rows = append(p.Rows, podRow{Sample: sr.cur, Node: f[3], Phase: f[4], Deletion: f[5], CIDs: f[6], Restarts: f[7]})
		return nil
	})
	if err != nil {
		return nil, err
	}
	pt.samples = sr.s
	for _, p := range pt.Pods {
		p.Present = make([]bool, len(pt.Times))
		for _, row := range p.Rows {
			p.Present[row.Sample] = true
		}
	}
	return pt, nil
}

// parseNodes reads truth-nodes.tsv: sample time, uid, name, Ready, the Ready
// condition's lastHeartbeatTime.
func parseNodes(r io.Reader) (*nodesTruth, error) {
	var sr sampleReader
	nt := &nodesTruth{Nodes: map[string]*nodeTruth{}, UIDAt: map[string][]string{}}
	type row struct {
		sample             int
		uid, name, isReady string
	}
	var rows []row
	err := lines(r, func(n int, line string) error {
		if line == "" {
			return fmt.Errorf("line %d: empty", n)
		}
		f := strings.Split(line, "\t")
		marker, err := sr.next(f, n)
		if err != nil {
			return err
		}
		if marker {
			return nil
		}
		if len(f) != 5 {
			return fmt.Errorf("line %d: %d fields, want 5", n, len(f))
		}
		if f[1] == "" {
			return fmt.Errorf("line %d: no uid", n)
		}
		rows = append(rows, row{sr.cur, f[1], f[2], f[3]})
		return nil
	})
	if err != nil {
		return nil, err
	}
	nt.samples = sr.s
	for _, rw := range rows {
		nodeTr := nt.Nodes[rw.uid]
		if nodeTr == nil {
			nodeTr = &nodeTruth{
				UID: rw.uid, Name: rw.name, First: rw.sample,
				Present: make([]bool, len(nt.Times)), Ready: make([]string, len(nt.Times)),
			}
			nt.Nodes[rw.uid] = nodeTr
			nt.UIDs = append(nt.UIDs, rw.uid)
		}
		if nodeTr.Present[rw.sample] {
			return nil, fmt.Errorf("node uid %s listed twice in sample %d", rw.uid, rw.sample)
		}
		nodeTr.Present[rw.sample] = true
		nodeTr.Ready[rw.sample] = rw.isReady
		nodeTr.Last = rw.sample
		at := nt.UIDAt[rw.name]
		if at == nil {
			at = make([]string, len(nt.Times))
			nt.UIDAt[rw.name] = at
		}
		at[rw.sample] = rw.uid
	}
	return nt, nil
}

// sampleAtOrBefore is the index of the latest sample not after t, or -1 if t is before
// them all.
func (s samples) sampleAtOrBefore(t time.Time) int {
	i, _ := slices.BinarySearchFunc(s.Times, t, func(a, b time.Time) int { return a.Compare(b) })
	// BinarySearch gives the first index with Times[i] >= t.
	if i < len(s.Times) && s.Times[i].Equal(t) {
		return i
	}
	return i - 1
}

// nodeUIDAt is the UID of the node that bore name at t, by the latest node sample not
// after t, or "" if the name was borne by none then.
func (nt *nodesTruth) nodeUIDAt(name string, t time.Time) string {
	i := nt.sampleAtOrBefore(t)
	if i < 0 {
		return ""
	}
	at := nt.UIDAt[name]
	if at == nil {
		return ""
	}
	return at[i]
}

// spacing is how long each sample stands for: the time to the next sample, and for the
// last the spacing before it.
func (s samples) spacing() []time.Duration {
	d := make([]time.Duration, len(s.Times))
	for i := range s.Times {
		switch {
		case i+1 < len(s.Times):
			d[i] = s.Times[i+1].Sub(s.Times[i])
		case i > 0:
			d[i] = d[i-1]
		}
	}
	return d
}
