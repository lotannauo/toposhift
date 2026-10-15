package bytesgate

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/query/coresidency"
	"github.com/lotannauo/toposhift/internal/query/extract"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/query/subgraph"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/wire"
)

// Scale sizes the fixtures. [Reference] is the scale the bytes gate is run at; a
// smaller one runs the same code on small fixtures, for the tests.
type Scale struct {
	// Seed fixes every hash and every choice, so a scale gives one set of
	// documents.
	Seed uint64
	// Pods is the number of pods on a node when the window opens, in fixtures a,
	// b and c.
	Pods int
	// Hosts is the number of hosts, each with one node, in fixture c.
	Hosts int
	// ReplaceEvery is the period of the churn: one pod in fixtures a, b and c is
	// deleted, and another put in its place, every ReplaceEvery.
	ReplaceEvery time.Duration
	// SmallScope and LargeScope are the elements in the union of fixtures d and
	// e: a pod is two, its entity and its edge to its node.
	SmallScope, LargeScope int
}

// Reference is the scale of the gate: coresidency's reference node, extract's two
// reference cells, and the two sizes of the client-scrub spike.
func Reference() Scale {
	return Scale{
		Seed: 1, Pods: coresidency.BudgetReferencePods, Hosts: 40, ReplaceEvery: 30 * time.Second,
		SmallScope: 2000, LargeScope: 10000,
	}
}

func (s Scale) validate() error {
	switch {
	case s.Pods < 7, s.Hosts < 1, s.ReplaceEvery <= 0, s.ReplaceEvery >= window, s.SmallScope < 2, s.LargeScope < 2:
		return fmt.Errorf("bytesgate: the scale %+v needs at least 7 pods, 1 host, a replacement period inside the window and a scope of 2 elements: %w", s, store.ErrInvalid)
	}
	return nil
}

// Fixture is one document the gate measures.
type Fixture struct {
	// Name is the letter of the fixture, "a" to "e", and Title what it is.
	Name, Title string
	// Build reads the fixture through the adapters, from a store of its own.
	Build func(context.Context) (wire.Document, error)
}

// Fixtures are the five fixtures at a scale, in order.
func Fixtures(s Scale) []Fixture {
	return []Fixture{
		{"a", "hot node", func(ctx context.Context) (wire.Document, error) { return hotNode(ctx, s) }},
		{"b", "anchored reference cell", func(ctx context.Context) (wire.Document, error) { return anchored(ctx, s) }},
		{"c", fmt.Sprintf("exploratory reference cell, default policy, seed pods capped at %d", extract.DefaultPolicy().MaxSeedPods), func(ctx context.Context) (wire.Document, error) { return exploratory(ctx, s) }},
		{"d", fmt.Sprintf("scope, %d elements", s.SmallScope), func(ctx context.Context) (wire.Document, error) {
			return scope(ctx, s, s.SmallScope, 4)
		}},
		{"e", fmt.Sprintf("scope, %d elements", s.LargeScope), func(ctx context.Context) (wire.Document, error) {
			return scope(ctx, s, s.LargeScope, 5)
		}},
	}
}

func win() interval.Window {
	return interval.Window{From: from, To: from.Add(window), AsOf: store.Latest}
}

// document wraps a subgraph in the echo of its query and the policy of this
// build.
func document(kind wire.QueryKind, seeds []identity.Fingerprint, opts map[string]int64, sg subgraph.Subgraph) wire.Document {
	q := wire.Query{Kind: kind, Seeds: seeds, AsOf: store.Latest}
	for name, v := range opts {
		q.Options = append(q.Options, wire.Option{Name: name, Value: v})
	}
	slices.SortFunc(q.Options, func(a, b wire.Option) int { return cmp.Compare(a.Name, b.Name) })
	return wire.Document{Query: q, Policy: wire.CurrentPolicy(), Subgraph: sg}
}

func flag(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

func coOptions(p coresidency.Policy, nodeOnly bool, out map[string]int64) map[string]int64 {
	out["node_only"] = flag(nodeOnly)
	out["max_nodes"], out["max_hosts"] = int64(p.MaxNodes), int64(p.MaxHosts)
	out["max_per_via"], out["max_results"] = int64(p.MaxPerVia), int64(p.MaxResults)
	out["min_overlap_ns"] = int64(p.MinOverlap)
	return out
}

func exOptions(p extract.Policy, nodeOnly, anchored bool) map[string]int64 {
	out := coOptions(p.CoResidency, nodeOnly, map[string]int64{})
	out["anchored"] = flag(anchored)
	out["max_members"], out["max_instances"] = int64(p.MaxMembers), int64(p.MaxInstances)
	out["max_seed_pods"], out["max_pairs"] = int64(p.MaxSeedPods), int64(p.MaxPairs)
	out["ubiquity_num"], out["ubiquity_den"], out["ubiquity_min_nodes"] = int64(p.UbiquityNum), int64(p.UbiquityDen), int64(p.UbiquityMinNodes)
	return out
}

// hotNode is fixture a: one host and one node, Pods pods on the node, replaced at
// the scale's rate, and the co-residents of the first pod, which stays.
func hotNode(ctx context.Context, s Scale) (wire.Document, error) {
	if err := s.validate(); err != nil {
		return wire.Document{}, err
	}
	w, err := newWorld()
	if err != nil {
		return wire.Document{}, err
	}
	defer w.close()
	g, r := newIDs(s.Seed), newRNG(s.Seed)
	h := g.next(catalog.Host)
	n := &nodeRec{node: g.next(catalog.K8sNode)}
	w.obs(entity(h), base())
	w.obs(entity(n.node), base())
	w.obs(runsOn(n.node, h), base())
	for range s.Pods {
		g.placePod(w, n, base(), false, identity.Fingerprint{})
	}
	seed := n.pods[0].pod
	g.replace(w, r, []*nodeRec{n}, s.ReplaceEvery, 1, false)
	if err := w.commit(ctx); err != nil {
		return wire.Document{}, err
	}
	res, err := coresidency.NewFinder(w.st).Find(ctx, seed, win(), coresidency.Options{})
	if err != nil {
		return wire.Document{}, fmt.Errorf("bytesgate: fixture a: %w", err)
	}
	sg, err := subgraph.FromCoResidency(ctx, w.st, res, subgraph.ReadPolicy{})
	if err != nil {
		return wire.Document{}, fmt.Errorf("bytesgate: fixture a: %w", err)
	}
	return document(wire.QueryCoResidents, []identity.Fingerprint{seed}, coOptions(coresidency.DefaultPolicy(), false, map[string]int64{}), sg), nil
}

// cell is the services of an extraction cell: the seed, three callers and three
// callees, and the nodes the pods are on.
type cell struct {
	seed  identity.Fingerprint
	hosts []identity.Fingerprint
	nodes []*nodeRec
}

// newCell writes the services and their dependency edges, and for each of hosts
// hosts one node with Pods pods, each with an instance. The first seven pods of a
// node are those of the seven services, one each; the instance of any other pod
// is mapped to no service.
func newCell(w *world, g *ids, s Scale, hosts int) cell {
	var c cell
	svcs := make([]identity.Fingerprint, 7)
	for i := range svcs {
		svcs[i] = g.next(catalog.Service)
		w.obs(entity(svcs[i]), base())
	}
	c.seed = svcs[0]
	for _, caller := range svcs[1:4] {
		w.obs(dependsOn(caller, c.seed), base())
	}
	for _, callee := range svcs[4:] {
		w.obs(dependsOn(c.seed, callee), base())
	}
	for range hosts {
		h := g.next(catalog.Host)
		n := &nodeRec{node: g.next(catalog.K8sNode)}
		w.obs(entity(h), base())
		w.obs(entity(n.node), base())
		w.obs(runsOn(n.node, h), base())
		for i := range s.Pods {
			var svc identity.Fingerprint
			if i < len(svcs) {
				svc = svcs[i]
			}
			g.placePod(w, n, base(), true, svc)
		}
		c.hosts = append(c.hosts, h)
		c.nodes = append(c.nodes, n)
	}
	return c
}

// anchored is fixture b: one host of the cell, anchored at the middle of the
// window.
func anchored(ctx context.Context, s Scale) (wire.Document, error) {
	if err := s.validate(); err != nil {
		return wire.Document{}, err
	}
	w, err := newWorld()
	if err != nil {
		return wire.Document{}, err
	}
	defer w.close()
	g, r := newIDs(s.Seed+1), newRNG(s.Seed+1)
	c := newCell(w, g, s, 1)
	g.replace(w, r, c.nodes, s.ReplaceEvery, 1, true)
	if err := w.commit(ctx); err != nil {
		return wire.Document{}, err
	}
	seed := extract.Anchored{Service: c.seed, Host: c.hosts[0], At: from.Add(window / 2)}
	res, err := extract.NewExtractor(w.st).Extract(ctx, seed, win(), extract.Options{})
	if err != nil {
		return wire.Document{}, fmt.Errorf("bytesgate: fixture b: %w", err)
	}
	sg, err := subgraph.FromExtraction(ctx, w.st, res, subgraph.ReadPolicy{})
	if err != nil {
		return wire.Document{}, fmt.Errorf("bytesgate: fixture b: %w", err)
	}
	return document(wire.QueryExtraction, []identity.Fingerprint{c.seed, c.hosts[0]}, exOptions(extract.DefaultPolicy(), false, true), sg), nil
}

// exploratory is fixture c: Hosts hosts of the cell, searched from the service
// alone.
func exploratory(ctx context.Context, s Scale) (wire.Document, error) {
	if err := s.validate(); err != nil {
		return wire.Document{}, err
	}
	w, err := newWorld()
	if err != nil {
		return wire.Document{}, err
	}
	defer w.close()
	g, r := newIDs(s.Seed+2), newRNG(s.Seed+2)
	c := newCell(w, g, s, s.Hosts)
	g.replace(w, r, c.nodes, s.ReplaceEvery, 1, true)
	if err := w.commit(ctx); err != nil {
		return wire.Document{}, err
	}
	res, err := extract.NewExtractor(w.st).Extract(ctx, extract.Exploratory{Service: c.seed}, win(), extract.Options{})
	if err != nil {
		return wire.Document{}, fmt.Errorf("bytesgate: fixture c: %w", err)
	}
	sg, err := subgraph.FromExtraction(ctx, w.st, res, subgraph.ReadPolicy{})
	if err != nil {
		return wire.Document{}, fmt.Errorf("bytesgate: fixture c: %w", err)
	}
	return document(wire.QueryExtraction, []identity.Fingerprint{c.seed}, exOptions(extract.DefaultPolicy(), false, false), sg), nil
}

// scopeNodePods is the pods the generator puts on a node in fixtures d and e.
const scopeNodePods = 110

// scope is fixtures d and e: a scope of elements/2 pods, each of which is two
// elements, its entity and its edge to its node. The nodes are the far ends of
// those edges and are boundary entries. Of the pods, 70 in 100 are there for the
// whole window, 15 appear at an instant of it, 10 disappear at one, and 5 go and
// come back, so the union is exactly the elements asked for and there are deltas.
func scope(ctx context.Context, s Scale, elements int, salt uint64) (wire.Document, error) {
	if err := s.validate(); err != nil {
		return wire.Document{}, err
	}
	pods := elements / 2
	w, err := newWorld()
	if err != nil {
		return wire.Document{}, err
	}
	defer w.close()
	g, r := newIDs(s.Seed+salt), newRNG(s.Seed+salt)
	nodes := make([]identity.Fingerprint, (pods+scopeNodePods-1)/scopeNodePods)
	for i := range nodes {
		nodes[i] = g.next(catalog.K8sNode)
		w.obs(entity(nodes[i]), base())
	}
	inWindow := func() time.Time {
		return from.Add(time.Duration(1+r.intn(int(window/time.Second)-2)) * time.Second)
	}
	var scopeFPs []identity.Fingerprint
	for i := range pods {
		pod, n := g.next(catalog.K8sPod), nodes[i%len(nodes)]
		scopeFPs = append(scopeFPs, pod)
		edge := scheduledOn(pod, n)
		begin := base()
		switch k := r.intn(100); {
		case k >= 70 && k < 85:
			begin = inWindow()
		case k >= 85 && k < 95:
			end := inWindow()
			w.obs(entity(pod), base())
			w.obs(edge, base())
			w.del(edge, end)
			w.del(entity(pod), end)
			continue
		case k >= 95:
			a := inWindow()
			b := a.Add(time.Duration(1+r.intn(int(window/time.Second)/4)) * time.Second)
			w.obs(entity(pod), base())
			w.obs(edge, base())
			w.del(edge, a)
			w.del(entity(pod), a)
			w.obs(entity(pod), b)
			w.obs(edge, b)
			continue
		}
		w.obs(entity(pod), begin)
		w.obs(edge, begin)
	}
	if err := w.commit(ctx); err != nil {
		return wire.Document{}, err
	}
	sg, err := subgraph.ReadScope(ctx, w.st, scopeFPs, win(), subgraph.ScopePolicy{MaxScope: pods})
	if err != nil {
		return wire.Document{}, fmt.Errorf("bytesgate: scope of %d elements: %w", elements, err)
	}
	return document(wire.QueryScope, scopeFPs, map[string]int64{"max_scope": int64(pods)}, sg), nil
}
