package workload

import (
	"container/heap"
	"fmt"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// The modeled cluster: its entities, how they are fingerprinted, and how a
// record about them is built and queued.

// cluster is the current state of the modeled Kubernetes cluster.
type cluster struct {
	racks, hosts, nodes, services []identity.Fingerprint
	pods, instances               []identity.Fingerprint
	containers                    [][]identity.Fingerprint
	podNode                       []int  // node index, or -1 when the pod does not exist
	confirmed                     []bool // the kubelet also asserts the pod's current placement
	nodeOutageUntil               []time.Time
	nodePayload, rollupPayload    [][]byte
	depends                       [][2]int

	nodeCount   []int                  // pods on each node
	incarnation []int                  // how many times the pod in each slot has been replaced (FreshIdentities)
	minted      []identity.Fingerprint // every pod, instance and container created (FreshIdentities)
	podPayload  [][]byte               // what the pod heartbeat says about the pod in each slot (PodHeartbeatInterval)
}

func (g *Generator) fingerprint(t catalog.EntityType, attrs ...identity.Attr) identity.Fingerprint {
	id, err := g.res.Resolve(t, attrs)
	if err != nil {
		panic(fmt.Sprintf("workload: building a %s fingerprint: %v", t, err)) // a bug in this package
	}
	return id.Fingerprint()
}

func attr(k catalog.AttributeKey, format string, a ...any) identity.Attr {
	return identity.Attr{Key: k, Value: fmt.Sprintf(format, a...)}
}

func (g *Generator) buildCluster() {
	c := g.cfg
	cl := &g.cl
	for i := range c.Racks {
		cl.racks = append(cl.racks, g.fingerprint(catalog.Rack, attr(catalog.RackID, "rack-%d", i)))
	}
	for i := range c.Hosts {
		cl.hosts = append(cl.hosts, g.fingerprint(catalog.Host, attr(catalog.HostID, "host-%d", i)))
		cl.nodes = append(cl.nodes, g.fingerprint(catalog.K8sNode, attr(catalog.K8sNodeUID, "node-%d", i)))
		cl.nodePayload = append(cl.nodePayload, g.payload())
	}
	cl.nodeOutageUntil = make([]time.Time, c.Hosts)
	for i := range c.Services {
		cl.services = append(cl.services, g.fingerprint(catalog.Service, attr(catalog.ServiceName, "svc-%d", i)))
	}
	cl.nodeCount = make([]int, c.Hosts)
	cl.incarnation = make([]int, c.Pods)
	cl.podPayload = make([][]byte, c.Pods)
	for i := range c.Pods {
		if c.FreshIdentities {
			cl.pods = append(cl.pods, identity.Fingerprint{})
			cl.instances = append(cl.instances, identity.Fingerprint{})
			cl.containers = append(cl.containers, nil)
			g.mint(i)
			continue
		}
		cl.pods = append(cl.pods, g.fingerprint(catalog.K8sPod, attr(catalog.K8sPodUID, "pod-%d", i)))
		cl.instances = append(cl.instances, g.fingerprint(catalog.ServiceInstance,
			attr(catalog.ServiceName, "svc-%d", i%c.Services), attr(catalog.ServiceInstanceID, "pod-%d", i)))
		n := 1 + g.rng.IntN(c.ContainersPerPod)
		var cs []identity.Fingerprint
		for j := range n {
			cs = append(cs, g.fingerprint(catalog.Container, attr(catalog.ContainerID, "ctr-%d-%d", i, j)))
		}
		cl.containers = append(cl.containers, cs)
	}
	cl.confirmed = make([]bool, c.Pods)
	cl.podNode = make([]int, c.Pods)
	for i := range cl.podNode {
		cl.podNode[i] = g.drawNode(-1)
		cl.nodeCount[cl.podNode[i]]++
	}
	for s := range c.Services {
		for d := 1; d <= c.DependsPerService && d < c.Services; d++ {
			cl.depends = append(cl.depends, [2]int{s, (s + d) % c.Services})
			cl.rollupPayload = append(cl.rollupPayload, g.payload())
		}
	}
}

// mint gives the pod slot p a new pod, service instance and containers, with
// identities no earlier pod had (FreshIdentities). The first call for a slot
// makes its first incarnation.
func (g *Generator) mint(p int) {
	cl := &g.cl
	inc := cl.incarnation[p]
	if cl.pods[p] != (identity.Fingerprint{}) {
		inc++
		cl.incarnation[p] = inc
	}
	cl.pods[p] = g.fingerprint(catalog.K8sPod, attr(catalog.K8sPodUID, "pod-%d.%d", p, inc))
	cl.instances[p] = g.fingerprint(catalog.ServiceInstance,
		attr(catalog.ServiceName, "svc-%d", p%g.cfg.Services), attr(catalog.ServiceInstanceID, "pod-%d.%d", p, inc))
	n := 1 + g.rng.IntN(g.cfg.ContainersPerPod)
	cs := make([]identity.Fingerprint, 0, n)
	for j := range n {
		cs = append(cs, g.fingerprint(catalog.Container, attr(catalog.ContainerID, "ctr-%d.%d-%d", p, inc, j)))
	}
	cl.containers[p] = cs
	cl.minted = append(cl.minted, cl.pods[p], cl.instances[p])
	cl.minted = append(cl.minted, cs...)
}

// drawNode chooses a node for a pod by the Zipf skew, never the node avoid (if
// not negative), and, when nodes have a capacity, one with room if there is one.
func (g *Generator) drawNode(avoid int) int {
	node := int(g.nodeZipf.Uint64())
	if node == avoid {
		node = (node + 1) % g.cfg.Hosts
	}
	if g.cfg.MaxPodsPerNode > 0 {
		for range g.cfg.Hosts {
			if node != avoid && g.cl.nodeCount[node] < g.cfg.MaxPodsPerNode {
				break
			}
			node = (node + 1) % g.cfg.Hosts
		}
	}
	return node
}

func (g *Generator) payload() []byte {
	n := g.cfg.PayloadMin
	if g.cfg.PayloadMax > g.cfg.PayloadMin {
		n += g.rng.IntN(g.cfg.PayloadMax - g.cfg.PayloadMin + 1)
	}
	b := make([]byte, n)
	for i := 0; i < n; i += 8 {
		v := g.rng.Uint64()
		for j := 0; j < 8 && i+j < n; j++ {
			b[i+j] = byte(v >> (8 * j))
		}
	}
	return b
}

// layerOf is the churn layer of a relation, as the design assigns them: slow
// placement is L1, workload placement L2, service dependency L3.
func layerOf(rel catalog.RelationType) catalog.Layer {
	switch rel {
	case catalog.LocatedIn, catalog.RunsOn:
		return catalog.L1
	case catalog.DependsOn:
		return catalog.L3
	default:
		return catalog.L2
	}
}

func entityLayer(t catalog.EntityType) catalog.Layer {
	e, ok := catalog.Default().Entity(t)
	if !ok {
		panic("workload: unknown entity type " + string(t))
	}
	return e.Layer()
}

func (g *Generator) entityRecord(at time.Time, p lifecycle.Producer, fp identity.Fingerprint, kind lifecycle.Kind, ttl time.Duration, payload []byte) {
	r := engine.Record{
		Layer: entityLayer(fp.Type()), Subject: engine.EntitySubject(fp), Producer: p, EventTime: at,
		Kind: kind, TTL: ttl,
	}
	if kind == lifecycle.Observe {
		r.Payload = payload
	}
	g.emit(r)
}

func (g *Generator) edgeRecord(at time.Time, p lifecycle.Producer, from, to identity.Fingerprint, rel catalog.RelationType, kind lifecycle.Kind, ttl time.Duration, payload []byte) {
	if ok := mustRelation(rel).Allows(from.Type(), to.Type()); !ok {
		panic(fmt.Sprintf("workload: %s does not allow %s -> %s", rel, from.Type(), to.Type()))
	}
	r := engine.Record{
		Layer: layerOf(rel), Subject: engine.EdgeSubject(from, to, rel), Producer: p, EventTime: at,
		Kind: kind, TTL: ttl,
	}
	if kind == lifecycle.Observe {
		r.Payload = payload
	}
	g.emit(r)
}

func mustRelation(rel catalog.RelationType) catalog.Relation {
	r, ok := catalog.Default().Relation(rel)
	if !ok {
		panic("workload: unknown relation " + string(rel))
	}
	return r
}

// emit queues a record. Its arrival is its event time, or later if it is late.
func (g *Generator) emit(r engine.Record) {
	arrival := r.EventTime
	if g.cfg.LateProbability > 0 && g.rng.Float64() < g.cfg.LateProbability {
		arrival = arrival.Add(time.Duration(g.rng.ExpFloat64() * float64(g.cfg.LateMeanDelay)))
	}
	if g.cfg.BacklogEvery > 0 {
		arrival = g.backlogArrival(r)
	}
	g.tick++
	heap.Push(&g.queue, arrivalItem{arrival: arrival, tick: g.tick, rec: r})
}

func (g *Generator) heartbeatTTL() time.Duration {
	if g.cfg.HeartbeatInterval <= 0 {
		return 0
	}
	return time.Duration(g.cfg.HeartbeatTTLFactor) * g.cfg.HeartbeatInterval
}

func (g *Generator) rollupTTL() time.Duration {
	if g.cfg.RollupInterval <= 0 {
		return 0
	}
	return time.Duration(g.cfg.HeartbeatTTLFactor) * g.cfg.RollupInterval
}

// backlog is one producer's pipeline: when its next episode of delay starts, how
// long the current one lasts and delays records by, and when the last record it
// delivered arrived.
type backlog struct {
	nextStart, until time.Time
	delay            time.Duration
	last             time.Time

	episodes int           // started so far, for the tests of the rate
	delaySum time.Duration // their delays added up
}

// backlogArrival is when r arrives under the backlog model (BacklogEvery). The
// producer's records arrive in the order they happened: a delay never lets a
// later record overtake an earlier one.
func (g *Generator) backlogArrival(r engine.Record) time.Time {
	b := g.backlogs[r.Producer]
	if b == nil {
		b = &backlog{nextStart: g.cfg.Start.Add(g.backlogGap())}
		g.backlogs[r.Producer] = b
	}
	if !r.EventTime.Before(b.until) && !r.EventTime.Before(b.nextStart) {
		b.delay = time.Duration(g.rng.ExpFloat64() * float64(g.cfg.BacklogMeanDelay))
		b.episodes++
		b.delaySum += b.delay
		b.until = r.EventTime.Add(g.cfg.BacklogSpan)
		b.nextStart = b.until.Add(g.backlogGap())
	}
	arrival := r.EventTime
	if r.EventTime.Before(b.until) {
		arrival = arrival.Add(b.delay)
	}
	if arrival.Before(b.last) {
		arrival = b.last
	}
	b.last = arrival
	return arrival
}

func (g *Generator) backlogGap() time.Duration {
	return time.Duration(g.rng.ExpFloat64() * float64(g.cfg.BacklogEvery))
}
