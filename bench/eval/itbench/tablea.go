package itbench

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/lotannauo/toposhift/bench/eval/blastscore"
)

// The strings that make a case co-residency. A case is co-residency when its
// labelled category or fault mechanism says resource exhaustion or interference
// on a pod or a node; every other case is call-graph. The strings are matched
// exactly, so that what fired is printed in table A and can be corrected before
// any fact is loaded.
var (
	coResidencyCategories = []string{"Resource", "Resource Saturation"}
	coResidencyMechanisms = []string{
		"cpu_stress", "memory_stress", "jvm_heap_stress", "disk_stress", "io_stress",
		"network_bandwidth", "noisy_neighbor",
	}
)

// SeedKind is how a case's seed is found, from the kind of the root of the chain.
type SeedKind string

// The seed rules.
const (
	// SeedPod: the service of the pod, and the pod, if one pod of that name is
	// alive at the start of the window.
	SeedPod SeedKind = "pod"
	// SeedWorkload: the service of a Deployment, ReplicaSet, StatefulSet or
	// Service, and every pod it owns or selects.
	SeedWorkload SeedKind = "workload"
	// SeedNode: the node, scored at the pod level only.
	SeedNode SeedKind = "node"
	// SeedMoved: the first propagation target that maps.
	SeedMoved SeedKind = "moved"
	// SeedNone: no root is marked.
	SeedNone SeedKind = "none"
)

func seedKindOf(kind string) SeedKind {
	switch kind {
	case "Pod":
		return SeedPod
	case "Deployment", "ReplicaSet", "StatefulSet", "Service":
		return SeedWorkload
	case "Node":
		return SeedNode
	}
	return SeedMoved
}

// Window is the interval [From, To) a case is scored over. The labels carry no time
// of injection, so From is the earliest time an alert became active, and is
// labelled a proxy; To is the time of the latest alerts_at file, or, when there is
// none, of the alerts_in_alerting_state file, also a proxy.
type Window struct {
	From, To         time.Time
	T0Proxy, T1Proxy bool
	// Why says why there is no window, if there is none.
	Why string
}

// OK reports whether the window can be scored over.
func (w Window) OK() bool { return w.Why == "" }

func windowOf(a Alerts) Window {
	w := Window{From: a.FirstActive, T0Proxy: true}
	switch {
	case !a.LastAt.IsZero():
		w.To = a.LastAt
	case !a.AlertingState.IsZero():
		w.To, w.T1Proxy = a.AlertingState, true
	}
	switch {
	case w.From.IsZero():
		w.Why = "no_start_time"
	case w.To.IsZero():
		w.Why = "no_end_time"
	case !w.From.Before(w.To):
		w.Why = "start_not_before_end"
	}
	return w
}

// ARow is one scenario's row of table A: what the labels and the alerts say,
// before any object or span is read.
type ARow struct {
	ID string
	// N is the scenario number.
	N int
	// Unreadable is set when the ground-truth file could not be read: the error,
	// and no more of the row. Missing says the file is not there.
	Unreadable string
	Missing    bool

	Faults []Fault
	Root   Group
	// HasRoot says exactly one group marks the root.
	HasRoot bool
	Seed    SeedKind
	// Targets are the ids of the groups the chain leads to, in order: the
	// candidates a moved seed takes its replacement from.
	Targets []string
	// Class is the case's class, and ClassRule what fired: a category or a
	// mechanism.
	Class     blastscore.Class
	ClassRule string
	Window    Window
	// Truth are the groups of the chain less the root.
	Truth []Group
	// UnknownGroups counts the ids of the chain that name no group.
	UnknownGroups int

	GT GroundTruth
}

// ReadTableA reads one scenario's labels and alerts, and nothing else.
func ReadTableA(dir string) (ARow, error) {
	id := filepath.Base(dir)
	row := ARow{ID: id}
	if _, err := fmt.Sscanf(id, "Scenario-%d", &row.N); err != nil {
		return ARow{}, fmt.Errorf("itbench: %q is not a scenario directory", id)
	}
	gt, err := ReadGroundTruth(filepath.Join(dir, "ground_truth.yaml"))
	if err != nil {
		switch {
		case errors.Is(err, ErrUnreadable):
			row.Unreadable = err.Error()
			return row, nil
		case os.IsNotExist(err):
			row.Unreadable, row.Missing = "there is no ground_truth.yaml", true
			return row, nil
		}
		return ARow{}, err
	}
	alerts, err := ReadAlerts(dir)
	if err != nil {
		return ARow{}, err
	}
	row.GT = gt
	row.Faults = gt.Fault
	row.Window = windowOf(alerts)
	row.Root, row.HasRoot = gt.Root()
	row.Seed = SeedNone
	if row.HasRoot {
		row.Seed = seedKindOf(row.Root.Kind)
		if row.Seed == SeedMoved {
			row.Targets = gt.Targets()
		}
	}
	row.Class, row.ClassRule = classOf(gt, row.Root, row.HasRoot)
	for _, gid := range gt.Chain() {
		if row.HasRoot && gid == row.Root.ID {
			continue
		}
		g, ok := gt.Group(gid)
		if !ok {
			row.UnknownGroups++
			continue
		}
		row.Truth = append(row.Truth, g)
	}
	return row, nil
}

// classOf decides a case's class from the fault entries that are about the root of
// the chain (those whose entity names the root group, by group id or by name), or
// from all of them if none is.
func classOf(gt GroundTruth, root Group, hasRoot bool) (blastscore.Class, string) {
	entries := gt.Fault
	if hasRoot {
		var about []Fault
		for _, f := range gt.Fault {
			e := f.Entity
			if e.GroupID == root.ID || e.Name == root.ID || (root.Name != "" && e.Name == root.Name) {
				about = append(about, f)
			}
		}
		if len(about) > 0 {
			entries = about
		}
	}
	for _, f := range entries {
		for _, c := range coResidencyCategories {
			if f.Category == c {
				return blastscore.ClassCoResidency, "category=" + c
			}
		}
		for _, m := range coResidencyMechanisms {
			if f.Mechanism == m {
				return blastscore.ClassCoResidency, "fault_mechanism=" + m
			}
		}
	}
	return blastscore.ClassCallGraph, ""
}

// TableA reads the rows of table A for every scenario of a dataset directory, in
// order of scenario number. It reads ground_truth.yaml and the alerts only.
func TableA(dir string) ([]ARow, error) {
	scs, err := Scenarios(dir)
	if err != nil {
		return nil, err
	}
	var out []ARow
	for _, s := range scs {
		r, err := ReadTableA(s)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", filepath.Base(s), err)
		}
		out = append(out, r)
	}
	return out, nil
}

// describe prints a group the way table A lists it: kind, namespace and what names
// its objects.
func (g Group) describe() string {
	var b strings.Builder
	b.WriteString(g.Kind)
	if g.Namespace != "" {
		b.WriteString(" in " + g.Namespace)
	}
	switch {
	case len(g.Filter) > 0:
		fmt.Fprintf(&b, " filter %q", g.Filter)
	case g.Name != "":
		fmt.Fprintf(&b, " name %q", g.Name)
	}
	return b.String()
}

// TableBText prints table B for the scenarios of table A, reading each scenario's
// Kubernetes objects in turn and only those: no span is read.
func TableBText(dir string, rows []ARow) (string, error) {
	var b strings.Builder
	b.WriteString(TableBHeader)
	for _, r := range rows {
		var cl *Cluster
		if r.Unreadable == "" {
			path := filepath.Join(dir, r.ID, ObjectsFile)
			if _, err := os.Stat(path); err == nil {
				c, err := readCluster(path, false)
				if err != nil {
					return "", fmt.Errorf("%s: %w", r.ID, err)
				}
				cl = c
			} else if !os.IsNotExist(err) {
				return "", err
			}
		}
		b.WriteString(FormatTableB(r, cl))
	}
	return b.String(), nil
}
