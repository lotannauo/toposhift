package itbench

import (
	"slices"
	"time"
)

// mappedKinds are the kinds of group whose objects map to a service, through the
// pods they are, own or select.
func mappedKind(kind string) bool {
	switch kind {
	case "Pod", "Service", "Deployment", "ReplicaSet", "StatefulSet":
		return true
	}
	return false
}

// NoGroup stands for the kind of an id of the chain that names no group.
const NoGroup = "(no group)"

// Why a group maps to nothing.
const (
	WhyKind        = "kind_not_mapped"
	WhyNoObject    = "no_object_matches"
	WhyNoComponent = "no_component_label"
	WhyAmbiguous   = "several_services"
)

// GroupMap is what a group maps to in the cluster: the objects it names, the
// services they are of, and, for a group of pods, the pods.
type GroupMap struct {
	ID   string
	Kind string
	// Via is how the group named its objects: ViaName or ViaFilter.
	Via string
	// Why says why the group maps to nothing, if it does not.
	Why string
	// Objects counts the objects the group names.
	Objects int
	// Services are the service names the objects map to, sorted and without
	// repeats. An object with no component label, or with pods of several, maps
	// to none and is counted in Unmapped.
	Services []string
	Unmapped int
	// Pods are the pods a group of pods names.
	Pods []*Object
}

// Mapped reports whether the group maps to at least one service.
func (m GroupMap) Mapped() bool { return len(m.Services) > 0 }

// MapGroup maps a group to services through the cluster: the objects it names (by
// an exact name, or by the filters the group carries, whole name, as written)
// take their service name from the component label of the pods they are, own or
// select. Nothing is matched by prefix, by a part of a name or by a likeness.
func MapGroup(c *Cluster, g Group) GroupMap {
	m := GroupMap{ID: g.ID, Kind: g.Kind}
	if !mappedKind(g.Kind) {
		m.Why = WhyKind
		return m
	}
	mt := g.Matcher()
	if mt.Why != "" {
		m.Why = mt.Why
		return m
	}
	m.Via = mt.Via
	objs := c.Select(g.Kind, g.Namespace, mt.Match)
	m.Objects = len(objs)
	if len(objs) == 0 {
		m.Why = WhyNoObject
		return m
	}
	set := map[string]bool{}
	for _, o := range objs {
		comps, _ := c.Components(o)
		switch len(comps) {
		case 0:
			m.Unmapped++
		case 1:
			set[comps[0]] = true
		default:
			m.Unmapped++
		}
		if g.Kind == "Pod" {
			m.Pods = append(m.Pods, o)
		}
	}
	for s := range set {
		m.Services = append(m.Services, s)
	}
	slices.Sort(m.Services)
	if len(m.Services) == 0 {
		m.Why = WhyNoComponent
		if m.Unmapped > 0 && m.Objects > 0 {
			// Distinguish an object whose pods name several services.
			for _, o := range objs {
				if comps, _ := c.Components(o); len(comps) > 1 {
					m.Why = WhyAmbiguous
					break
				}
			}
		}
	}
	return m
}

// Seed is what table B finds for a case's seed.
type Seed struct {
	Kind SeedKind
	// Moved says the seed is the first propagation target that maps, not the root.
	Moved bool
	// Group is the group the seed was taken from.
	Group string
	// Service is the seed service, and Pods the seed pods alive at the start of the
	// window (none, if the pod is not unique). PodsWhy says why a pod seed has no
	// pods.
	Service string
	Pods    []*Object
	PodsWhy string
	// Node is the node of a node seed.
	Node *Object
	// Why says why there is no seed: the case is excluded as an unmappable root.
	Why string
}

// OK reports whether a seed was found.
func (s Seed) OK() bool { return s.Why == "" }

// Why there is no seed.
const (
	WhyNoRoot       = "no_single_root"
	WhyRootUnmapped = "root_does_not_map"
	WhyNoTarget     = "no_target_maps"
)

// PlanSeed finds the seed of a case from the root of its chain. A pod root takes the
// service of the pod and the pod itself, if exactly one pod of the group is alive
// at t0; a Deployment, ReplicaSet, StatefulSet or Service root takes its service and
// every pod it owns or selects that is alive at t0; a Node root takes the node. Any
// other root is replaced by the first propagation target that maps to one service,
// by the same rules, and the seed is marked moved. If nothing maps, there is no
// seed.
func PlanSeed(row ARow, c *Cluster, t0 time.Time) Seed {
	if !row.HasRoot {
		return Seed{Kind: SeedNone, Why: WhyNoRoot}
	}
	kind := seedKindOf(row.Root.Kind)
	if kind != SeedMoved {
		s := seedFrom(row.Root, kind, c, t0)
		if s.Why == "" || kind == SeedNode {
			return s
		}
		return Seed{Kind: kind, Group: row.Root.ID, Why: WhyRootUnmapped}
	}
	for _, id := range row.Targets {
		g, ok := row.GT.Group(id)
		if !ok || g.ID == row.Root.ID {
			continue
		}
		k := seedKindOf(g.Kind)
		if k == SeedMoved || k == SeedNode {
			continue
		}
		if s := seedFrom(g, k, c, t0); s.Why == "" {
			s.Moved = true
			return s
		}
	}
	return Seed{Kind: SeedMoved, Group: row.Root.ID, Why: WhyNoTarget}
}

func seedFrom(g Group, kind SeedKind, c *Cluster, t0 time.Time) Seed {
	s := Seed{Kind: kind, Group: g.ID}
	if kind == SeedNode {
		mt := g.Matcher()
		if mt.Why != "" {
			s.Why = mt.Why
			return s
		}
		nodes := c.Select("Node", "", mt.Match)
		if len(nodes) != 1 {
			s.Why = WhyNoObject
			if len(nodes) > 1 {
				s.Why = WhyAmbiguous
			}
			return s
		}
		s.Node = nodes[0]
		return s
	}
	m := MapGroup(c, g)
	if len(m.Services) != 1 {
		s.Why = m.Why
		if s.Why == "" {
			s.Why = WhyAmbiguous
		}
		return s
	}
	s.Service = m.Services[0]
	mt := g.Matcher()
	var pods []*Object
	for _, o := range c.Select(g.Kind, g.Namespace, mt.Match) {
		pods = append(pods, c.pods(o)...)
	}
	var alive []*Object
	seen := map[*Object]bool{}
	for _, p := range pods {
		if !seen[p] && aliveAt(p, t0) {
			seen[p] = true
			alive = append(alive, p)
		}
	}
	if kind == SeedPod && len(alive) != 1 {
		s.PodsWhy = "pod_not_unique_at_window_start"
		return s
	}
	if len(alive) == 0 {
		s.PodsWhy = "no_pod_alive_at_window_start"
		return s
	}
	s.Pods = alive
	return s
}

// Truth is the truth of a case after mapping, before any fact is loaded.
type Truth struct {
	// Services are the service names the chain maps to, less the seed's, as a set.
	Services []string
	// Pods are the pods of the chain's pod groups.
	Pods []*Object
	// Unmappable counts the groups of the chain that map to no service (and
	// the ids that name no group), and LabelFiltered those that mapped through a
	// filter the label carries.
	Unmappable, LabelFiltered int
	// UnmappableKinds counts the unmappable groups by kind; an id of the chain that
	// names no group is under NoGroup.
	UnmappableKinds map[string]int
	// NotExpanded counts the groups that map to services but are not pod groups: they
	// are not expanded into pods for the pod level.
	NotExpanded int
	// Maps are the mapping of every group of the chain, for table B.
	Maps []GroupMap
}

// ChainTruth maps every group of the chain, less the root and the seed's group, to
// services and pods. Truth is a set after mapping: groups that map to one service
// count once, and the seed's own service is not truth. For a node seed there is no
// service-level truth.
func ChainTruth(row ARow, c *Cluster, seed Seed) Truth {
	t := Truth{UnmappableKinds: map[string]int{}}
	svc := map[string]bool{}
	pods := map[*Object]bool{}
	for _, gid := range row.GT.Chain() {
		if row.HasRoot && gid == row.Root.ID || gid == seed.Group {
			continue
		}
		g, ok := row.GT.Group(gid)
		if !ok {
			t.Unmappable++
			t.UnmappableKinds[NoGroup]++
			continue
		}
		m := MapGroup(c, g)
		t.Maps = append(t.Maps, m)
		if !m.Mapped() {
			t.Unmappable++
			t.UnmappableKinds[g.Kind]++
			continue
		}
		if m.Via == ViaFilter {
			t.LabelFiltered++
		}
		if g.Kind == "Pod" {
			for _, p := range m.Pods {
				if !pods[p] {
					pods[p] = true
					t.Pods = append(t.Pods, p)
				}
			}
		} else {
			t.NotExpanded++
		}
		for _, s := range m.Services {
			svc[s] = true
		}
	}
	if seed.Kind == SeedNode {
		svc = map[string]bool{}
	}
	delete(svc, seed.Service)
	for s := range svc {
		t.Services = append(t.Services, s)
	}
	slices.Sort(t.Services)
	return t
}
