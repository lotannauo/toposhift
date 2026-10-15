package itbench

import (
	"fmt"
	"slices"
	"strings"
	"time"
)

func stamp(t time.Time, proxy bool) string {
	if t.IsZero() {
		return "-"
	}
	s := t.UTC().Format("2006-01-02T15:04:05Z")
	if proxy {
		s += " (proxy)"
	}
	return s
}

// FormatTableA prints table A: what the labels and the alerts say about each
// scenario, before any object or span is read. It prints names and times, so it
// goes outside the repository.
func FormatTableA(rows []ARow) string {
	var b strings.Builder
	b.WriteString("Table A: from ground_truth.yaml and the alerts only\n")
	for _, r := range rows {
		fmt.Fprintf(&b, "\n%s\n", r.ID)
		if r.Unreadable != "" {
			fmt.Fprintf(&b, "  unreadable: %s\n", oneLine(r.Unreadable))
			continue
		}
		for _, f := range r.Faults {
			fmt.Fprintf(&b, "  fault:    %s %q (group %q), category %q, mechanism %q\n",
				f.Entity.Kind, f.Entity.Name, f.Entity.GroupID, f.Category, f.Mechanism)
		}
		if r.HasRoot {
			fmt.Fprintf(&b, "  root:     %s [%s]\n", r.Root.ID, r.Root.describe())
		} else {
			b.WriteString("  root:     none, or more than one, marked\n")
		}
		seed := string(r.Seed)
		if r.Seed == SeedMoved {
			seed += " (candidates, in order: " + strings.Join(r.Targets, ", ") + ")"
		}
		fmt.Fprintf(&b, "  seed:     %s\n", seed)
		rule := r.ClassRule
		if rule == "" {
			rule = "no resource category or mechanism"
		}
		fmt.Fprintf(&b, "  class:    %s (%s)\n", r.Class, rule)
		if r.Window.OK() {
			fmt.Fprintf(&b, "  window:   [%s, %s)\n", stamp(r.Window.From, r.Window.T0Proxy), stamp(r.Window.To, r.Window.T1Proxy))
		} else {
			fmt.Fprintf(&b, "  window:   none (%s); from %s, to %s\n", r.Window.Why, stamp(r.Window.From, r.Window.T0Proxy), stamp(r.Window.To, r.Window.T1Proxy))
		}
		for _, g := range r.Truth {
			fmt.Fprintf(&b, "  truth:    %s [%s]\n", g.ID, g.describe())
		}
		if r.UnknownGroups > 0 {
			fmt.Fprintf(&b, "  truth:    %d id(s) of the chain name no group\n", r.UnknownGroups)
		}
	}
	return b.String()
}

// TableBHeader starts the text of table B.
const TableBHeader = "Table B: from the Kubernetes objects only\n"

// FormatTableB prints one scenario's part of table B: its groups mapped to
// services and pods through the scenario's Kubernetes objects. cl is nil if the
// scenario has none.
func FormatTableB(r ARow, cl *Cluster) string {
	var b strings.Builder
	fmt.Fprintf(&b, "\n%s\n", r.ID)
	switch {
	case r.Unreadable != "":
		b.WriteString("  unreadable ground truth\n")
		return b.String()
	case cl == nil:
		b.WriteString("  no Kubernetes objects\n")
		return b.String()
	}
	pulls := fmt.Sprintf("%d pull(s), %s .. %s", cl.Pulls, stamp(cl.First, false), stamp(cl.Last, false))
	if cl.Pulls > 1 {
		pulls += fmt.Sprintf(", %s apart", cl.Interval)
	}
	fmt.Fprintf(&b, "  cluster:  %d nodes, %s\n", len(cl.Nodes), pulls)
	if !r.Window.OK() {
		b.WriteString("  no window: not mapped\n")
		return b.String()
	}
	seed := PlanSeed(r, cl, r.Window.From)
	if seed.OK() {
		moved := ""
		if seed.Moved {
			moved = " (moved)"
		}
		switch seed.Kind {
		case SeedNode:
			fmt.Fprintf(&b, "  seed:     node %q from %s\n", seed.Node.Name, seed.Group)
		default:
			fmt.Fprintf(&b, "  seed:     service %q from %s%s, %d pod(s) alive at the start", seed.Service, seed.Group, moved, len(seed.Pods))
			if seed.PodsWhy != "" {
				fmt.Fprintf(&b, " (%s)", seed.PodsWhy)
			}
			b.WriteString("\n")
		}
	} else {
		fmt.Fprintf(&b, "  seed:     none (%s): excluded as an unmappable root\n", seed.Why)
	}
	tr := ChainTruth(r, cl, seed)
	for _, m := range tr.Maps {
		why := m.Why
		if why == "" {
			why = "mapped"
		}
		fmt.Fprintf(&b, "  group:    %s %s via %q, %d object(s), services [%s], %d unmapped (%s)\n",
			m.ID, m.Kind, m.Via, m.Objects, strings.Join(m.Services, ", "), m.Unmapped, why)
	}
	fmt.Fprintf(&b, "  truth:    services [%s], %d pod(s), %d unmappable, %d by label filter, %d not expanded into pods\n",
		strings.Join(tr.Services, ", "), len(tr.Pods), tr.Unmappable, tr.LabelFiltered, tr.NotExpanded)
	kinds := make([]string, 0, len(tr.UnmappableKinds))
	for k := range tr.UnmappableKinds {
		kinds = append(kinds, fmt.Sprintf("%s %d", k, tr.UnmappableKinds[k]))
	}
	slices.Sort(kinds)
	if len(kinds) == 0 {
		kinds = []string{"none"}
	}
	fmt.Fprintf(&b, "  unmappable truth groups by kind: %s\n", strings.Join(kinds, ", "))
	return b.String()
}

func oneLine(s string) string { return strings.Join(strings.Fields(s), " ") }
